// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::StateError;
use super::database_target::DatabaseTarget;
use super::executor::DatabaseRuntimeWorker;
use super::sql_runtime::{StoreDatastore, db_err};
use super::sql_services::{DatabaseServices, MigrationRunner};
use std::path::Path;
use std::sync::Arc;
#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::AtomicUsize;
use weaver_server_base::operations::instrumentation::DbRuntimeMetricsSnapshot;

#[derive(Clone)]
pub struct Database {
    target: DatabaseTarget,
    sql_services: DatabaseServices,
    sql_worker: DatabaseRuntimeWorker,
    encryption_key: Option<crate::persistence::encryption::EncryptionKey>,
    // The torrent listen port the environment pins for this run, handed
    // over once at start-up. A database never handed one has none.
    torrent_port_pin: Option<u16>,
    _ephemeral_dir: Option<Arc<tempfile::TempDir>>,
    // How many times the datastore was taken for a statement, read or write.
    #[cfg(any(test, feature = "test-support"))]
    datastore_takes: Arc<AtomicUsize>,
}

impl Database {
    pub fn open_target(
        target: DatabaseTarget,
        migrations: impl MigrationRunner + Send + 'static,
    ) -> Result<Self, StateError> {
        let sql_worker = DatabaseRuntimeWorker::start(&target)?;
        let service_target = target.clone();
        let sql_services = sql_worker
            .block_on_startup_local(std::panic::Location::caller(), move || {
                DatabaseServices::open(service_target, migrations)
            })?;
        Ok(Self {
            target,
            sql_worker,
            sql_services,
            #[cfg(any(test, feature = "test-support"))]
            datastore_takes: Arc::default(),
            encryption_key: None,
            torrent_port_pin: None,
            _ephemeral_dir: None,
        })
    }

    pub fn open_in_memory(
        migrations: impl MigrationRunner + Send + 'static,
    ) -> Result<Self, StateError> {
        let tempdir =
            Arc::new(tempfile::tempdir().map_err(|e| StateError::Database(e.to_string()))?);
        let target = DatabaseTarget::SqlitePath(tempdir.path().join("weaver.db"));
        let mut db = Self::open_target(target, migrations)?;
        db.encryption_key = Some(super::encryption::EncryptionKey::generate());
        db._ephemeral_dir = Some(tempdir);
        Ok(db)
    }

    pub fn services(&self) -> &DatabaseServices {
        &self.sql_services
    }
    pub fn worker(&self) -> &DatabaseRuntimeWorker {
        &self.sql_worker
    }

    // Executor engine, saturation and operation-latency histogram for the
    // database runtime backing this handle.
    //
    // Read at scrape time. Cheap: a handful of `Relaxed` atomic loads plus one
    // `Vec` for the histogram buckets.
    pub fn runtime_metrics_snapshot(&self) -> DbRuntimeMetricsSnapshot {
        self.sql_worker.runtime_metrics().snapshot()
    }

    // What the WAL checkpoint task's passes did; `None` for every engine but
    // a SQLite file database. Read at scrape time: atomic loads plus one
    // `Vec` for the histogram buckets.
    pub fn wal_checkpoint_metrics_snapshot(
        &self,
    ) -> Option<crate::persistence::WalCheckpointMetricsSnapshot> {
        match self.datastore() {
            StoreDatastore::Sqlite { writer_gate, .. } => writer_gate
                .wal()
                .map(crate::persistence::wal_checkpoint::WalSignal::metrics_snapshot),
            StoreDatastore::Postgres { .. } => None,
        }
    }

    pub fn datastore(&self) -> StoreDatastore {
        #[cfg(any(test, feature = "test-support"))]
        self.datastore_takes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.sql_services.datastore()
    }

    /// How many times a statement took the datastore, to read or to write:
    /// what a test reads to know a path touched the database not at all.
    #[cfg(any(test, feature = "test-support"))]
    pub fn datastore_takes(&self) -> usize {
        self.datastore_takes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    // Stable, non-sensitive name of the configured persistence engine.
    pub fn engine_name(&self) -> &'static str {
        self.datastore().engine().as_str()
    }

    // Highest migration version this database's ledger held when this handle
    // opened it, before the migrations that open ran. `None` means nothing had
    // ever migrated it — a database this process created.
    //
    // This is how startup tells a fresh install from an upgrade, and from
    // which release line it is upgrading.
    pub fn pre_migration_schema_version(&self) -> Option<i64> {
        self.sql_services.pre_migration_schema_version()
    }

    pub fn database_target(&self) -> &DatabaseTarget {
        &self.target
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "writer gate: a TRUNCATE checkpoint for backup, outside the gate by ruling, idle at the time"
    )]
    pub fn checkpoint_sqlite(&self) -> Result<(), StateError> {
        let StoreDatastore::Sqlite { pool, .. } = self.datastore() else {
            return Ok(());
        };
        self.run_sql_blocking_local(move || async move {
            let mut connection = pool.acquire().await.map_err(db_err)?;
            sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(&mut *connection)
                .await
                .map_err(db_err)?;
            Ok(())
        })
    }

    // Closes every connection this database opened, the WAL checkpoint
    // task's last, even while another owner, such as the background writer,
    // still holds a handle to the services: a file is then free to be
    // replaced, which Windows refuses while any handle is open.
    pub fn close(self) -> Result<(), StateError> {
        let datastore = self.datastore();
        let services = self.sql_services.clone();
        let worker = self.sql_worker.clone();
        drop(self);
        let closed = worker.block_on(std::panic::Location::caller(), async move {
            match datastore {
                StoreDatastore::Sqlite { pool, .. } => pool.close().await,
                StoreDatastore::Postgres { pool } => pool.close().await,
            }
            Ok(())
        });
        services.stop_checkpointer();
        closed
    }

    #[track_caller]
    pub fn run_sql_blocking<T, Fut>(&self, future: Fut) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: std::future::Future<Output = Result<T, StateError>> + Send + 'static,
    {
        self.sql_worker
            .block_on(std::panic::Location::caller(), future)
    }

    // Run a **pure read**: no writes, and not inside a write transaction.
    //
    // Reads dispatched here run on the multi-threaded read executor instead of
    // queueing behind the single writer. Use [`Self::run_sql_blocking`] for
    // anything that writes, and for reads that must sit inside a write
    // transaction (read-modify-write), where the ordering the single writer
    // provides is the point.
    #[track_caller]
    pub fn run_sql_blocking_read<T, Fut>(&self, future: Fut) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: std::future::Future<Output = Result<T, StateError>> + Send + 'static,
    {
        self.sql_worker
            .block_on_read(std::panic::Location::caller(), future)
    }

    #[track_caller]
    pub fn run_sql_blocking_local<T, Build, Fut>(&self, build: Build) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, StateError>> + 'static,
    {
        self.sql_worker
            .block_on_local(std::panic::Location::caller(), build)
    }

    // Set the encryption key used to protect sensitive fields (passwords).
    pub fn set_encryption_key(&mut self, key: crate::persistence::encryption::EncryptionKey) {
        self.encryption_key = Some(key);
    }

    // Set the torrent listen port the environment pins for this run. The
    // settings load reports it as the listen port and the save keeps the
    // stored one as it is.
    pub fn set_torrent_port_pin(&mut self, pin: Option<u16>) {
        self.torrent_port_pin = pin;
    }

    // The torrent listen port the environment pins, if it pins one.
    pub fn torrent_port_pin(&self) -> Option<u16> {
        self.torrent_port_pin
    }

    // Get a reference to the encryption key, if set.
    pub fn encryption_key(&self) -> Option<&crate::persistence::encryption::EncryptionKey> {
        self.encryption_key.as_ref()
    }

    // Answer a trivial query, to prove the datastore is reachable.
    //
    // Deliberately touches no table: a readiness probe must report on the
    // connection, not on whether some particular schema object exists. This is
    // a blocking call — callers on an async runtime must wrap it in
    // `spawn_blocking`.
    pub fn probe_liveness(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            crate::persistence::sql_runtime::SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 AS alive",
                &[],
            )
            .await?;
            Ok(())
        })
    }

    // The sizes of a SQLite database's files, `None` on another engine. It
    // touches no executor and no connection, only file metadata, so it
    // answers while the database itself is stalled. It blocks on the file
    // system, so it is called off the async runtime. Paths stay here; only
    // sizes leave.
    pub fn sqlite_file_sizes(&self) -> Option<SqliteFileSizes> {
        if !matches!(self.datastore(), StoreDatastore::Sqlite { .. }) {
            return None;
        }
        Some(match self.target.sqlite_path() {
            Ok(Some(path)) => {
                let sibling = |suffix: &str| {
                    let mut name = path.clone().into_os_string();
                    name.push(suffix);
                    std::path::PathBuf::from(name)
                };
                SqliteFileSizes {
                    database_file: SqliteFileSize::of(&path),
                    wal_file: SqliteFileSize::of(&sibling("-wal")),
                    shm_file: SqliteFileSize::of(&sibling("-shm")),
                }
            }
            Ok(None) => SqliteFileSizes {
                database_file: SqliteFileSize::Absent,
                wal_file: SqliteFileSize::Absent,
                shm_file: SqliteFileSize::Absent,
            },
            Err(error) => {
                let error = error.to_string();
                SqliteFileSizes {
                    database_file: SqliteFileSize::Unreadable(error.clone()),
                    wal_file: SqliteFileSize::Unreadable(error.clone()),
                    shm_file: SqliteFileSize::Unreadable(error),
                }
            }
        })
    }

    // How a SQLite database's pages are laid out, and its journal mode.
    // `None` on another engine. The pragmas only read and run on the read
    // executor (no writer gate, no checkpoint), and each value is read on its
    // own, so one that fails leaves the others. It waits behind whatever the
    // read executor is doing, so a caller that must not stall bounds its wait.
    pub fn sqlite_page_facts(&self) -> Option<SqlitePageFacts> {
        let datastore = self.datastore();
        if !matches!(datastore, StoreDatastore::Sqlite { .. }) {
            return None;
        }
        let pragmas = self.run_sql_blocking_read(async move {
            Ok(SqlitePageFacts {
                page_size: sqlite_pragma_u64(&datastore, "PRAGMA page_size").await,
                page_count: sqlite_pragma_u64(&datastore, "PRAGMA page_count").await,
                freelist_count: sqlite_pragma_u64(&datastore, "PRAGMA freelist_count").await,
                journal_mode: sqlite_pragma_journal_mode(&datastore).await,
            })
        });
        Some(pragmas.unwrap_or_else(|error| {
            let error = error.to_string();
            SqlitePageFacts {
                page_size: Err(error.clone()),
                page_count: Err(error.clone()),
                freelist_count: Err(error.clone()),
                journal_mode: Err(error),
            }
        }))
    }
}

// The sizes of a SQLite database's files; see `Database::sqlite_file_sizes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteFileSizes {
    pub database_file: SqliteFileSize,
    pub wal_file: SqliteFileSize,
    pub shm_file: SqliteFileSize,
}

// A SQLite database's page layout; see `Database::sqlite_page_facts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlitePageFacts {
    pub page_size: Result<u64, String>,
    pub page_count: Result<u64, String>,
    pub freelist_count: Result<u64, String>,
    pub journal_mode: Result<String, String>,
}

// The size of one of a SQLite database's files. A `-wal` or `-shm` file is
// absent whenever SQLite has none open, which is not a failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqliteFileSize {
    Present(u64),
    Absent,
    Unreadable(String),
}

impl SqliteFileSize {
    fn of(path: &Path) -> Self {
        match std::fs::metadata(path) {
            Ok(metadata) => Self::Present(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Self::Absent,
            Err(error) => Self::Unreadable(error.to_string()),
        }
    }
}

async fn sqlite_pragma_u64(
    datastore: &StoreDatastore,
    pragma: &'static str,
) -> Result<u64, String> {
    let row = crate::persistence::sql_runtime::SqlRuntime::fetch_optional(
        datastore.read_exec(),
        pragma,
        &[],
    )
    .await
    .map_err(|error| error.to_string())?
    .ok_or_else(|| format!("{pragma} returned no rows"))?;
    row.i64_at(0)
        .map(|value| value.max(0) as u64)
        .map_err(|error| error.to_string())
}

// `PRAGMA journal_mode` with no argument only reads the mode.
async fn sqlite_pragma_journal_mode(datastore: &StoreDatastore) -> Result<String, String> {
    let row = crate::persistence::sql_runtime::SqlRuntime::fetch_optional(
        datastore.read_exec(),
        "PRAGMA journal_mode",
        &[],
    )
    .await
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "PRAGMA journal_mode returned no rows".to_string())?;
    row.text("journal_mode").map_err(|error| error.to_string())
}
