#![allow(dead_code)]

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Connection};
use tracing::log::LevelFilter;

use crate::persistence::database_target::DatabaseTarget;
use crate::persistence::postgres_migrations;
use crate::persistence::sql_runtime::{SqlRuntime, StoreDatastore, db_err};
use crate::persistence::sqlite_writer::{SqliteWriterGate, new_writer_gate};
use crate::persistence::{StateError, postgres_migrations::MigrationStatus};
use crate::schema_migrations::MigrationMode;

const DEFAULT_SQLITE_MAX_CONNECTIONS: u32 = 16;
const MAX_SQLITE_CONNECTIONS_CAP: u32 = 64;
const DEFAULT_POSTGRES_MAX_CONNECTIONS: u32 = 16;
const MAX_POSTGRES_CONNECTIONS_CAP: u32 = 128;
const POSTGRES_WARM_MIN_CONNECTIONS: u32 = 2;
/// The pg pool must hold at least this many connections: the migration runner
/// pins one connection to hold a session advisory lock while the migration
/// steps acquire another from the pool, so a pool of 1 would deadlock at
/// startup.
const MIN_POSTGRES_MAX_CONNECTIONS: u32 = 2;
/// Only validate (ping) a pooled connection that has been idle at least this
/// long — long enough that a proxy or idle-timeout may have dropped it. Hot
/// connections skip the round-trip.
const POSTGRES_PING_IDLE_THRESHOLD: Duration = Duration::from_secs(30);
/// Default `synchronous_commit` for every weaver Postgres session.
///
/// `off` matches the durability posture the SQLite side has always run
/// (`PRAGMA synchronous = NORMAL` under WAL): a host crash can lose the last
/// instant of commits but never corrupts or reorders state, and everything in
/// active state is re-derived on restart by design. Leaving Postgres at its
/// server default meant every commit paid a full WAL flush that the SQLite
/// side does not — the single largest per-write latency gap between the two
/// engines. Asynchronous commit is still fully ordered and atomic; only
/// durability of the tail is relaxed.
const DEFAULT_POSTGRES_SYNCHRONOUS_COMMIT: &str = "off";
const SLOW_STATEMENT_WARN_MS: u64 = 1000;

#[derive(Clone)]
pub(crate) enum DatabaseServices {
    Sqlite(SqliteServices),
    Postgres(PostgresServices),
}

impl DatabaseServices {
    pub(crate) async fn open(
        target: DatabaseTarget,
        migration_mode: MigrationMode,
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

    pub(crate) fn datastore(&self) -> StoreDatastore {
        match self {
            Self::Sqlite(services) => services.datastore(),
            Self::Postgres(services) => services.datastore(),
        }
    }

    pub(crate) fn pre_migration_schema_version(&self) -> Option<i64> {
        match self {
            Self::Sqlite(services) => services.pre_migration_schema_version,
            Self::Postgres(services) => services.pre_migration_schema_version,
        }
    }
}

#[derive(Clone)]
pub(crate) struct SqliteServices {
    pool: sqlx::SqlitePool,
    encryption_key: Arc<RwLock<Option<crate::persistence::encryption::EncryptionKey>>>,
    writer_gate: SqliteWriterGate,
    /// Migration ledger maximum as it stood when this handle opened the
    /// database, captured before the migration run below could move it.
    /// `None` for a database nothing had ever migrated.
    pre_migration_schema_version: Option<i64>,
}

impl SqliteServices {
    pub(crate) async fn new_path(
        path: impl AsRef<Path>,
        migration_mode: MigrationMode,
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

    pub(crate) async fn new_url(
        url: impl AsRef<str>,
        migration_mode: MigrationMode,
    ) -> Result<Self, StateError> {
        let db_url = sqlite_url_with_create(url.as_ref());
        let is_memory = db_url.contains(":memory:");
        let pool_opts = if is_memory {
            SqlitePoolOptions::new()
                .max_connections(1)
                .min_connections(1)
                .idle_timeout(None)
                .max_lifetime(None)
        } else {
            SqlitePoolOptions::new().max_connections(sqlite_max_connections_from_env())
        };

        let mut connect_opts: SqliteConnectOptions = db_url.parse().map_err(db_err)?;
        connect_opts = connect_opts
            .foreign_keys(true)
            .busy_timeout(Duration::from_millis(10_000))
            .pragma("auto_vacuum", "INCREMENTAL")
            .pragma("synchronous", "NORMAL")
            .pragma("cache_size", "-16000")
            .pragma("mmap_size", "16777216")
            .pragma("temp_store", "MEMORY")
            .log_slow_statements(
                LevelFilter::Warn,
                Duration::from_millis(SLOW_STATEMENT_WARN_MS),
            );
        if !is_memory {
            connect_opts = connect_opts.journal_mode(SqliteJournalMode::Wal);
        }

        let pool = pool_opts
            .connect_with(connect_opts)
            .await
            .map_err(|error| {
                StateError::Database(format!("cannot open SQLite database {db_url}: {error}"))
            })?;

        // Read before the run, because the run is what changes the answer.
        let pre_migration_schema_version =
            crate::schema_migrations::max_recorded_migration_version(&pool).await?;
        crate::schema_migrations::run_embedded_migrations(&pool, migration_mode).await?;

        Ok(Self {
            pool,
            encryption_key: Arc::new(RwLock::new(None)),
            writer_gate: new_writer_gate(),
            pre_migration_schema_version,
        })
    }

    pub(crate) fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    pub(crate) fn datastore(&self) -> StoreDatastore {
        StoreDatastore::Sqlite {
            pool: self.pool.clone(),
            writer_gate: self.writer_gate.clone(),
        }
    }

    pub(crate) fn encryption_key_state(
        &self,
    ) -> Arc<RwLock<Option<crate::persistence::encryption::EncryptionKey>>> {
        self.encryption_key.clone()
    }

    pub(crate) async fn set_encryption_key(
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

    pub(crate) async fn vacuum_into(&self, dest_path: &str) -> Result<(), StateError> {
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
pub(crate) struct PostgresServices {
    pool: sqlx::PgPool,
    encryption_key: Arc<RwLock<Option<crate::persistence::encryption::EncryptionKey>>>,
    /// See [`SqliteServices::pre_migration_schema_version`].
    pre_migration_schema_version: Option<i64>,
}

impl PostgresServices {
    pub(crate) async fn new(
        database_url: impl AsRef<str>,
        migration_mode: MigrationMode,
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
        let pre_migration_schema_version =
            postgres_migrations::max_recorded_migration_version(&pool).await?;
        postgres_migrations::run_migrations(&pool, migration_mode).await?;

        Ok(Self {
            pool,
            encryption_key: Arc::new(RwLock::new(None)),
            pre_migration_schema_version,
        })
    }

    pub(crate) fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    pub(crate) fn datastore(&self) -> StoreDatastore {
        StoreDatastore::Postgres {
            pool: self.pool.clone(),
        }
    }

    pub(crate) fn encryption_key_state(
        &self,
    ) -> Arc<RwLock<Option<crate::persistence::encryption::EncryptionKey>>> {
        self.encryption_key.clone()
    }

    pub(crate) async fn set_encryption_key(
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

    pub(crate) async fn list_applied_migrations(&self) -> Result<Vec<MigrationStatus>, StateError> {
        postgres_migrations::list_applied_migrations(&self.pool).await
    }
}

pub(crate) fn sqlite_url_with_create(path: &str) -> String {
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

pub(crate) fn sqlite_max_connections_from_env() -> u32 {
    std::env::var("WEAVER_SQLITE_MAX_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SQLITE_MAX_CONNECTIONS)
        .clamp(1, MAX_SQLITE_CONNECTIONS_CAP)
}

pub(crate) fn postgres_max_connections_from_env() -> u32 {
    std::env::var("WEAVER_POSTGRES_MAX_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_POSTGRES_MAX_CONNECTIONS)
        .clamp(MIN_POSTGRES_MAX_CONNECTIONS, MAX_POSTGRES_CONNECTIONS_CAP)
}

/// Resolves the per-session `synchronous_commit` setting, defaulting to
/// [`DEFAULT_POSTGRES_SYNCHRONOUS_COMMIT`]. Values outside PostgreSQL's own
/// vocabulary for the setting are rejected (with a warning) rather than passed
/// through, because the value lands in the connection's startup options.
pub(crate) fn postgres_synchronous_commit_from_env() -> String {
    parse_postgres_synchronous_commit(
        std::env::var("WEAVER_POSTGRES_SYNCHRONOUS_COMMIT")
            .ok()
            .as_deref(),
    )
}

fn parse_postgres_synchronous_commit(value: Option<&str>) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_url_with_create_adds_rwc_for_paths() {
        assert_eq!(
            sqlite_url_with_create("/tmp/weaver.db"),
            "sqlite:///tmp/weaver.db?mode=rwc"
        );
    }

    #[test]
    fn sqlite_url_with_create_keeps_existing_mode() {
        assert_eq!(
            sqlite_url_with_create("sqlite:///tmp/weaver.db?mode=ro"),
            "sqlite:///tmp/weaver.db?mode=ro"
        );
    }

    #[test]
    fn sqlite_url_with_create_uses_shared_memory_url() {
        assert_eq!(
            sqlite_url_with_create("sqlite://:memory:"),
            "sqlite://file::memory:?mode=memory&cache=shared"
        );
    }

    #[test]
    fn synchronous_commit_defaults_off_to_match_the_sqlite_posture() {
        assert_eq!(parse_postgres_synchronous_commit(None), "off");
    }

    #[test]
    fn synchronous_commit_accepts_postgres_vocabulary_case_insensitively() {
        assert_eq!(parse_postgres_synchronous_commit(Some(" ON ")), "on");
        assert_eq!(parse_postgres_synchronous_commit(Some("local")), "local");
        assert_eq!(
            parse_postgres_synchronous_commit(Some("remote_apply")),
            "remote_apply"
        );
    }

    #[test]
    fn synchronous_commit_rejects_values_postgres_would_not_accept() {
        // The value lands in connection startup options, so anything outside
        // the setting's own vocabulary falls back to the default instead of
        // being passed through.
        assert_eq!(parse_postgres_synchronous_commit(Some("true")), "off");
        assert_eq!(
            parse_postgres_synchronous_commit(Some("off; DROP TABLE jobs")),
            "off"
        );
    }
}
