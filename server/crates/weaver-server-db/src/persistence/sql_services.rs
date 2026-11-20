// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#![allow(dead_code)]

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Connection};
use tracing::log::LevelFilter;

use crate::persistence::database_target::DatabaseTarget;

use crate::persistence::StateError;
use crate::persistence::sql_runtime::{SqlRuntime, StoreDatastore, db_err};
use crate::persistence::sqlite_writer::{SqliteWriterGate, new_writer_gate};
use crate::persistence::wal_checkpoint::{WAL_CHECKPOINT_FRAMES, WalCheckpointer};
/// Schema operations supplied by the application when opening a pool.
pub trait MigrationRunner {
    fn sqlite_version(
        &self,
        pool: &sqlx::SqlitePool,
    ) -> impl Future<Output = Result<Option<i64>, StateError>>;
    fn migrate_sqlite(
        &self,
        pool: &sqlx::SqlitePool,
    ) -> impl Future<Output = Result<(), StateError>>;
    fn postgres_version(
        &self,
        pool: &sqlx::PgPool,
    ) -> impl Future<Output = Result<Option<i64>, StateError>>;
    fn migrate_postgres(&self, pool: &sqlx::PgPool)
    -> impl Future<Output = Result<(), StateError>>;
}

const DEFAULT_SQLITE_MAX_CONNECTIONS: u32 = 16;
const MAX_SQLITE_CONNECTIONS_CAP: u32 = 64;
const DEFAULT_POSTGRES_MAX_CONNECTIONS: u32 = 16;
const MAX_POSTGRES_CONNECTIONS_CAP: u32 = 128;
const POSTGRES_WARM_MIN_CONNECTIONS: u32 = 2;
// The pg pool must hold at least this many connections: the migration runner
// pins one connection to hold a session advisory lock while the migration
// steps acquire another from the pool, so a pool of 1 would deadlock at
// startup.
const MIN_POSTGRES_MAX_CONNECTIONS: u32 = 2;
// Only validate (ping) a pooled connection that has been idle at least this
// long — long enough that a proxy or idle-timeout may have dropped it. Hot
// connections skip the round-trip.
const POSTGRES_PING_IDLE_THRESHOLD: Duration = Duration::from_secs(30);
// Default `synchronous_commit` for every weaver Postgres session.
//
// `off` matches the durability posture the SQLite side has always run
// (`PRAGMA synchronous = NORMAL` under WAL): a host crash can lose the last
// instant of commits but never corrupts or reorders state, and everything in
// active state is re-derived on restart by design. Leaving Postgres at its
// server default meant every commit paid a full WAL flush that the SQLite
// side does not — the single largest per-write latency gap between the two
// engines. Asynchronous commit is still fully ordered and atomic; only
// durability of the tail is relaxed.
const DEFAULT_POSTGRES_SYNCHRONOUS_COMMIT: &str = "off";
const SLOW_STATEMENT_WARN_MS: u64 = 1000;

#[derive(Clone)]
pub enum DatabaseServices {
    Sqlite(SqliteServices),
    Postgres(PostgresServices),
}

impl DatabaseServices {
    pub async fn open(
        target: DatabaseTarget,
        migration_mode: impl MigrationRunner,
    ) -> Result<Self, StateError> {
        match target {
            DatabaseTarget::SqlitePath(path) => SqliteServices::new_path(path, migration_mode)
                .await
                .map(Self::Sqlite),
            DatabaseTarget::SqliteUrl(url) => SqliteServices::new_url(url, migration_mode)
                .await
                .map(Self::Sqlite),
            DatabaseTarget::PostgresUrl(url) => PostgresServices::new(url, migration_mode)
                .await
                .map(Self::Postgres),
        }
    }

    pub fn datastore(&self) -> StoreDatastore {
        match self {
            Self::Sqlite(services) => services.datastore(),
            Self::Postgres(services) => services.datastore(),
        }
    }

    // A SQLite file database's checkpoint task ends here, its connection
    // closed; Postgres has none.
    pub fn stop_checkpointer(&self) {
        if let Self::Sqlite(services) = self {
            services.stop_checkpointer();
        }
    }

    // Whether the checkpoint task has ended; `None` without one.
    #[cfg(any(test, feature = "test-support"))]
    pub fn checkpointer_ended(&self) -> Option<bool> {
        match self {
            Self::Sqlite(services) => services
                .checkpointer
                .as_ref()
                .map(|checkpointer| checkpointer.ended()),
            Self::Postgres(_) => None,
        }
    }

    pub fn pre_migration_schema_version(&self) -> Option<i64> {
        match self {
            Self::Sqlite(services) => services.pre_migration_schema_version,
            Self::Postgres(services) => services.pre_migration_schema_version,
        }
    }
}

#[derive(Clone)]
pub struct SqliteServices {
    pool: sqlx::SqlitePool,
    encryption_key: Arc<RwLock<Option<crate::persistence::encryption::EncryptionKey>>>,
    writer_gate: SqliteWriterGate,
    // A file database's checkpoint task; closing the database, or the last
    // handle to drop, runs its shutdown checkpoint and closes its connection.
    checkpointer: Option<Arc<WalCheckpointer>>,
    // Migration ledger maximum as it stood when this handle opened the
    // database, captured before the migration run below could move it.
    // `None` for a database nothing had ever migrated.
    pre_migration_schema_version: Option<i64>,
}

impl SqliteServices {
    pub async fn new_path(
        path: impl AsRef<Path>,
        migration_mode: impl MigrationRunner,
    ) -> Result<Self, StateError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        Self::new_url(
            sqlite_url_with_create(&path.to_string_lossy()),
            migration_mode,
        )
        .await
    }

    pub async fn new_url(
        url: impl AsRef<str>,
        migration_mode: impl MigrationRunner,
    ) -> Result<Self, StateError> {
        Self::open_sqlite(url.as_ref(), migration_mode, WAL_CHECKPOINT_FRAMES).await
    }

    pub async fn open_sqlite(
        url: &str,
        migration_mode: impl MigrationRunner,
        wal_checkpoint_frames: u32,
    ) -> Result<Self, StateError> {
        let db_url = sqlite_url_with_create(url);
        let is_memory = db_url.contains(":memory:");
        let connect_opts = sqlite_connect_options(&db_url, is_memory)?;
        let wal = (!is_memory).then(|| WalCheckpointer::signal(wal_checkpoint_frames));
        let pool_opts = match wal {
            Some(wal) => sqlite_pool_options(is_memory).after_connect(move |connection, _| {
                Box::pin(async move { wal.report_commits(connection).await })
            }),
            None => sqlite_pool_options(is_memory),
        };

        // The run goes over one connection. A connection keeps the schema it
        // last read, and SQLite checks a dropped column against that copy, so
        // a migration that drops a column an earlier step added on another
        // connection would read it as missing.
        //
        // auto_vacuum is set here and only here. Setting it writes page 1, so
        // a connection that sets it takes SQLite's write lock while it opens:
        // behind a long commit the open waits busy_timeout and fails, and a
        // read on that connection fails with it. The setting lives in the
        // database file, so the one migration connection, which opens first
        // and creates the schema, sets it for every connection after it.
        let label = db_url.as_str();
        let open = move |options: SqliteConnectOptions, pool_opts: SqlitePoolOptions| async move {
            pool_opts.connect_with(options).await.map_err(|error| {
                StateError::Database(format!("cannot open SQLite database {label}: {error}"))
            })
        };
        let migration_pool = if is_memory {
            None
        } else {
            Some(
                // The run is alone at startup and has no checkpoint task
                // yet, so it keeps SQLite's own autocheckpoint.
                open(
                    connect_opts
                        .clone()
                        .pragma("auto_vacuum", "INCREMENTAL")
                        .pragma("wal_autocheckpoint", WAL_CHECKPOINT_FRAMES.to_string()),
                    SqlitePoolOptions::new().max_connections(1),
                )
                .await?,
            )
        };
        let pool = open(connect_opts.clone(), pool_opts).await?;
        let runner = migration_pool.as_ref().unwrap_or(&pool);

        // Read before the run, because the run is what changes the answer.
        let pre_migration_schema_version = migration_mode.sqlite_version(runner).await?;
        let migrated = migration_mode.migrate_sqlite(runner).await;
        if let Some(migration_pool) = migration_pool {
            migration_pool.close().await;
        }
        migrated?;

        let checkpointer = match wal {
            Some(wal) => Some(Arc::new(WalCheckpointer::start(wal, connect_opts).await?)),
            None => None,
        };
        Ok(Self {
            pool,
            encryption_key: Arc::new(RwLock::new(None)),
            writer_gate: new_writer_gate(wal),
            checkpointer,
            pre_migration_schema_version,
        })
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn wal_signal(&self) -> Option<&'static crate::persistence::wal_checkpoint::WalSignal> {
        self.writer_gate.wal()
    }

    // Ends the checkpoint task and closes its connection, whoever else
    // still holds a handle; see `WalCheckpointer::stop`.
    pub fn stop_checkpointer(&self) {
        if let Some(checkpointer) = &self.checkpointer {
            checkpointer.stop();
        }
    }

    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    pub fn datastore(&self) -> StoreDatastore {
        StoreDatastore::Sqlite {
            pool: self.pool.clone(),
            writer_gate: self.writer_gate.clone(),
        }
    }

    pub fn encryption_key_state(
        &self,
    ) -> Arc<RwLock<Option<crate::persistence::encryption::EncryptionKey>>> {
        self.encryption_key.clone()
    }

    pub async fn set_encryption_key(
        &self,
        key: crate::persistence::encryption::EncryptionKey,
    ) -> Result<(), StateError> {
        *self
            .encryption_key
            .write()
            .map_err(|_| StateError::Database("encryption key lock poisoned".to_string()))? =
            Some(key);
        Ok(())
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "writer gate: runs under run_serialized_sqlite, which holds the gate"
    )]
    pub async fn vacuum_into(&self, dest_path: &str) -> Result<(), StateError> {
        let dest_path = dest_path.to_string();
        SqlRuntime::run_serialized_sqlite(&self.datastore(), "vacuum_into", move |pool| {
            let dest_path = dest_path.clone();
            async move {
                sqlx::query("VACUUM INTO ?")
                    .bind(dest_path)
                    .execute(&pool)
                    .await
                    .map_err(db_err)?;
                Ok(())
            }
        })
        .await
    }
}

#[derive(Clone)]
pub struct PostgresServices {
    pool: sqlx::PgPool,
    encryption_key: Arc<RwLock<Option<crate::persistence::encryption::EncryptionKey>>>,
    // See [`SqliteServices::pre_migration_schema_version`].
    pre_migration_schema_version: Option<i64>,
}

impl PostgresServices {
    pub async fn new(
        database_url: impl AsRef<str>,
        migration_mode: impl MigrationRunner,
    ) -> Result<Self, StateError> {
        let mut connect_options: PgConnectOptions =
            database_url
                .as_ref()
                .parse()
                .map_err(|error: sqlx::Error| {
                    StateError::Database(format!("invalid PostgreSQL database URL: {error}"))
                })?;
        connect_options = connect_options
            .application_name("weaver")
            .log_slow_statements(
                LevelFilter::Warn,
                Duration::from_millis(SLOW_STATEMENT_WARN_MS),
            );
        // `on` sends nothing: the server default is already `on`, and skipping
        // the startup option keeps weaver usable behind poolers that reject
        // `options` (PgBouncer in transaction mode), making the env var a full
        // escape hatch.
        let synchronous_commit = postgres_synchronous_commit_from_env();
        if synchronous_commit != "on" {
            connect_options =
                connect_options.options([("synchronous_commit", synchronous_commit.as_str())]);
        }

        let max_connections = postgres_max_connections_from_env();
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            // Keep a small warm floor so bursty pipeline writes don't pay
            // TCP+TLS+auth on a cold pool after every idle lull.
            .min_connections(max_connections.min(POSTGRES_WARM_MIN_CONNECTIONS))
            // Don't ping on every acquire — that adds a full round-trip to each
            // statement.
            .test_before_acquire(false)
            // Instead, validate only connections that have been idle long enough
            // to plausibly have been dropped (proxy/idle-timeout/backend
            // recycle). Hot connections skip the round-trip; a failed ping
            // discards the connection so the caller transparently gets a fresh
            // one instead of a hard error on the first use.
            .before_acquire(|conn, meta| {
                Box::pin(async move {
                    if meta.idle_for >= POSTGRES_PING_IDLE_THRESHOLD {
                        conn.ping().await?;
                    }
                    Ok(true)
                })
            })
            .connect_with(connect_options)
            .await
            .map_err(|error| {
                StateError::Database(format!("cannot open PostgreSQL database: {error}"))
            })?;

        // Read before the run, because the run is what changes the answer.
        let pre_migration_schema_version = migration_mode.postgres_version(&pool).await?;
        migration_mode.migrate_postgres(&pool).await?;

        Ok(Self {
            pool,
            encryption_key: Arc::new(RwLock::new(None)),
            pre_migration_schema_version,
        })
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    pub fn datastore(&self) -> StoreDatastore {
        StoreDatastore::Postgres {
            pool: self.pool.clone(),
        }
    }

    pub fn encryption_key_state(
        &self,
    ) -> Arc<RwLock<Option<crate::persistence::encryption::EncryptionKey>>> {
        self.encryption_key.clone()
    }

    pub async fn set_encryption_key(
        &self,
        key: crate::persistence::encryption::EncryptionKey,
    ) -> Result<(), StateError> {
        *self
            .encryption_key
            .write()
            .map_err(|_| StateError::Database("encryption key lock poisoned".to_string()))? =
            Some(key);
        Ok(())
    }
}

pub fn sqlite_url_with_create(path: &str) -> String {
    if path.starts_with("sqlite:") {
        if path.starts_with("sqlite://:memory:") {
            let with_mode = if path.contains("?mode=") {
                path.to_string()
            } else if path.contains('?') {
                format!("{path}&mode=memory")
            } else {
                format!("{path}?mode=memory")
            };

            let with_cache = if with_mode.contains("cache=shared") {
                with_mode
            } else if with_mode.contains('?') {
                format!("{with_mode}&cache=shared")
            } else {
                format!("{with_mode}?cache=shared")
            };

            return with_cache.replace("sqlite://:memory:", "sqlite://file::memory:");
        }

        if path.contains("?mode=") {
            return path.to_string();
        }

        return if path.contains('?') {
            format!("{path}&mode=rwc")
        } else {
            format!("{path}?mode=rwc")
        };
    }

    format!("sqlite://{path}?mode=rwc")
}

// Nothing a file pool connection is opened with writes the database, so
// opening one never needs SQLite's write lock.
pub fn sqlite_connect_options(
    db_url: &str,
    is_memory: bool,
) -> Result<SqliteConnectOptions, StateError> {
    let mut connect_opts: SqliteConnectOptions = db_url.parse().map_err(db_err)?;
    connect_opts = connect_opts
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(10_000))
        .pragma("synchronous", "NORMAL")
        .pragma("mmap_size", "16777216")
        .pragma("temp_store", "MEMORY")
        .log_slow_statements(
            LevelFilter::Warn,
            Duration::from_millis(SLOW_STATEMENT_WARN_MS),
        );
    if is_memory {
        // A memory database has its one connection for life and no
        // migration connection, so that connection sets auto_vacuum.
        connect_opts = connect_opts
            .pragma("auto_vacuum", "INCREMENTAL")
            .pragma("cache_size", "-16000");
    } else {
        // A file database sits on whatever disk the user has, spinning ones
        // included: a 64 MB page cache keeps the hot tables off the disk, a
        // 16 MB WAL between checkpoints turns many small commits into fewer
        // long writes, and the size limit truncates the WAL back after a
        // checkpoint so one burst does not leave it large for good.
        connect_opts = connect_opts
            .journal_mode(SqliteJournalMode::Wal)
            .pragma("cache_size", "-65536")
            // Checkpoints run on their own task; see `wal_checkpoint`.
            .pragma("wal_autocheckpoint", "0")
            .pragma("journal_size_limit", "67108864");
    }
    Ok(connect_opts)
}

// A file pool opens every connection up front and keeps it for the life of
// the process, so no request ever waits on an open: an open reads the
// header and the schema, which on network storage is slow. Sixteen
// connections cost sixteen file handles, and each page cache only grows as
// it is used.
pub fn sqlite_pool_options(is_memory: bool) -> SqlitePoolOptions {
    let max_connections = if is_memory {
        1
    } else {
        sqlite_max_connections_from_env()
    };
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .min_connections(max_connections)
        .idle_timeout(None)
        .max_lifetime(None)
}

pub fn sqlite_max_connections_from_env() -> u32 {
    std::env::var("WEAVER_SQLITE_MAX_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SQLITE_MAX_CONNECTIONS)
        .clamp(1, MAX_SQLITE_CONNECTIONS_CAP)
}

pub fn postgres_max_connections_from_env() -> u32 {
    std::env::var("WEAVER_POSTGRES_MAX_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_MAX_CONNECTIONS)
        .clamp(MIN_POSTGRES_MAX_CONNECTIONS, MAX_POSTGRES_CONNECTIONS_CAP)
}

// Resolves the per-session `synchronous_commit` setting, defaulting to
// [`DEFAULT_POSTGRES_SYNCHRONOUS_COMMIT`]. Values outside PostgreSQL's own
// vocabulary for the setting are rejected (with a warning) rather than passed
// through, because the value lands in the connection's startup options.
pub fn postgres_synchronous_commit_from_env() -> String {
    parse_postgres_synchronous_commit(
        std::env::var("WEAVER_POSTGRES_SYNCHRONOUS_COMMIT")
            .ok()
            .as_deref(),
    )
}

pub fn parse_postgres_synchronous_commit(value: Option<&str>) -> String {
    const ALLOWED: [&str; 5] = ["on", "off", "local", "remote_write", "remote_apply"];
    match value {
        Some(value) => {
            let value = value.trim().to_ascii_lowercase();
            if ALLOWED.contains(&value.as_str()) {
                value
            } else {
                tracing::warn!(
                    value = %value,
                    default = DEFAULT_POSTGRES_SYNCHRONOUS_COMMIT,
                    "ignoring invalid WEAVER_POSTGRES_SYNCHRONOUS_COMMIT; expected one of on/off/local/remote_write/remote_apply"
                );
                DEFAULT_POSTGRES_SYNCHRONOUS_COMMIT.to_string()
            }
        }
        None => DEFAULT_POSTGRES_SYNCHRONOUS_COMMIT.to_string(),
    }
}
