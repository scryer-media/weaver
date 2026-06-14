use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;

use tokio::sync::{Notify, mpsc, oneshot};

use crate::StateError;
use crate::persistence::database_target::DatabaseTarget;
use crate::persistence::sql_runtime::StoreDatastore;
use crate::persistence::sql_services::DatabaseServices;

pub use crate::auth::{ApiKeyRow, AuthCredentials};
pub use crate::history::{HistoryFilter, IntegrationEventRow, JobEvent, JobHistoryRow};
pub use crate::jobs::{
    ActiveFileIdentity, ActiveFileProgress, ActiveJob, ActivePar2File, CommittedSegment,
    ExtractionChunk, RecoveredJob,
};
pub use crate::operations::{
    MetricsHistoryChunkRow, MetricsHistoryQueryData, MetricsHistoryQueryResult,
    RawMetricsHistoryPoint, RollupMetricsHistoryPoint, StableStateExport,
};
pub use crate::rss::{RssFeedRow, RssRuleAction, RssRuleRow, RssSeenItemRow};

const SQLITE_WRITE_QUEUE_CAPACITY: usize = 128;

type DatabaseRuntimeJob = Box<dyn FnOnce(&tokio::runtime::Runtime) + Send + 'static>;

#[derive(Clone)]
struct DatabaseRuntimeWorker {
    tx: std_mpsc::Sender<DatabaseRuntimeJob>,
}

impl DatabaseRuntimeWorker {
    fn start() -> Result<Self, StateError> {
        let (tx, rx) = std_mpsc::channel::<DatabaseRuntimeJob>();
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(1);

        std::thread::Builder::new()
            .name("weaver-db-runtime".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| error.to_string());
                let runtime = match runtime {
                    Ok(runtime) => {
                        let _ = ready_tx.send(Ok(()));
                        runtime
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };

                while let Ok(job) = rx.recv() {
                    job(&runtime);
                }
            })
            .map_err(|error| StateError::Database(error.to_string()))?;

        ready_rx
            .recv()
            .map_err(|_| StateError::Database("database runtime worker stopped".to_string()))?
            .map_err(StateError::Database)?;

        Ok(Self { tx })
    }

    fn block_on<T, Fut>(&self, future: Fut) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: std::future::Future<Output = Result<T, StateError>> + Send + 'static,
    {
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.tx
            .send(Box::new(move |runtime| {
                let _ = reply_tx.send(runtime.block_on(future));
            }))
            .map_err(|_| StateError::Database("database runtime worker stopped".to_string()))?;
        reply_rx
            .recv()
            .map_err(|_| StateError::Database("database runtime worker panicked".to_string()))?
    }

    fn block_on_local<T, Build, Fut>(&self, build: Build) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, StateError>> + 'static,
    {
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.tx
            .send(Box::new(move |runtime| {
                let _ = reply_tx.send(runtime.block_on(build()));
            }))
            .map_err(|_| StateError::Database("database runtime worker stopped".to_string()))?;
        reply_rx
            .recv()
            .map_err(|_| StateError::Database("database runtime worker panicked".to_string()))?
    }
}

fn open_sql_services_blocking(
    sql_worker: &DatabaseRuntimeWorker,
    target: DatabaseTarget,
) -> Result<DatabaseServices, StateError> {
    sql_worker.block_on_local(move || {
        DatabaseServices::open(target, crate::schema_migrations::MigrationMode::Apply)
    })
}

#[derive(Clone)]
pub(crate) struct DatabaseWriterExecutor {
    sql_services: DatabaseServices,
    sql_worker: DatabaseRuntimeWorker,
}

impl DatabaseWriterExecutor {
    fn from_database(db: &Database) -> Self {
        Self {
            sql_services: db.sql_services.clone(),
            sql_worker: db.sql_worker.clone(),
        }
    }

    pub(crate) fn datastore(&self) -> StoreDatastore {
        self.sql_services.datastore()
    }

    pub(crate) fn run_sql_blocking<T, Fut>(&self, future: Fut) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: std::future::Future<Output = Result<T, StateError>> + Send + 'static,
    {
        self.sql_worker.block_on(future)
    }
}

enum DbWriteCommand {
    ArchiveJob {
        job_id: crate::jobs::ids::JobId,
        history: Box<crate::history::JobHistoryRow>,
    },
    InsertJobEvents {
        events: Vec<crate::history::JobEvent>,
    },
    Flush {
        reply: oneshot::Sender<()>,
    },
}

/// SQL-backed persistent store for config, servers, and job history.
#[derive(Clone)]
pub struct Database {
    sql_services: DatabaseServices,
    sql_worker: DatabaseRuntimeWorker,
    writer_tx: mpsc::Sender<DbWriteCommand>,
    pending_archive_retries: Arc<AtomicUsize>,
    pending_archive_notify: Arc<Notify>,
    encryption_key: Option<crate::persistence::encryption::EncryptionKey>,
    _ephemeral_dir: Option<Arc<tempfile::TempDir>>,
}

impl Database {
    /// Open (or create) the database at `path`.
    /// Runs schema migrations and configures SQLite pragmas.
    pub fn open(path: &Path) -> Result<Self, StateError> {
        Self::open_target(DatabaseTarget::SqlitePath(path.to_path_buf()))
    }

    pub(crate) fn open_target(target: DatabaseTarget) -> Result<Self, StateError> {
        let sql_worker = DatabaseRuntimeWorker::start()?;
        let sql_services = open_sql_services_blocking(&sql_worker, target)?;

        let (writer_tx, writer_rx) = mpsc::channel(SQLITE_WRITE_QUEUE_CAPACITY);
        let db = Self {
            sql_services,
            sql_worker,
            writer_tx,
            pending_archive_retries: Arc::new(AtomicUsize::new(0)),
            pending_archive_notify: Arc::new(Notify::new()),
            encryption_key: None,
            _ephemeral_dir: None,
        };
        db.spawn_writer_task(writer_rx);
        Ok(db)
    }

    /// Open an in-memory database (for tests).
    pub fn open_in_memory() -> Result<Self, StateError> {
        let tempdir =
            Arc::new(tempfile::tempdir().map_err(|e| StateError::Database(e.to_string()))?);
        let path = tempdir.path().join("weaver.db");
        let sql_worker = DatabaseRuntimeWorker::start()?;
        let sql_services =
            open_sql_services_blocking(&sql_worker, DatabaseTarget::SqlitePath(path.clone()))?;

        let (writer_tx, writer_rx) = mpsc::channel(SQLITE_WRITE_QUEUE_CAPACITY);
        let db = Self {
            sql_services,
            sql_worker,
            writer_tx,
            pending_archive_retries: Arc::new(AtomicUsize::new(0)),
            pending_archive_notify: Arc::new(Notify::new()),
            encryption_key: None,
            _ephemeral_dir: Some(tempdir),
        };
        db.spawn_writer_task(writer_rx);
        Ok(db)
    }

    pub(crate) fn datastore(&self) -> StoreDatastore {
        self.sql_services.datastore()
    }

    pub(crate) fn run_sql_blocking<T, Fut>(&self, future: Fut) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: std::future::Future<Output = Result<T, StateError>> + Send + 'static,
    {
        self.sql_worker.block_on(future)
    }

    pub(crate) fn run_sql_blocking_local<T, Build, Fut>(
        &self,
        build: Build,
    ) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, StateError>> + 'static,
    {
        self.sql_worker.block_on_local(build)
    }

    /// Set the encryption key used to protect sensitive fields (passwords).
    pub fn set_encryption_key(&mut self, key: crate::persistence::encryption::EncryptionKey) {
        self.encryption_key = Some(key);
    }

    /// Get a reference to the encryption key, if set.
    pub(crate) fn encryption_key(&self) -> Option<&crate::persistence::encryption::EncryptionKey> {
        self.encryption_key.as_ref()
    }

    /// Check if the database has no settings (i.e. fresh / needs migration).
    pub fn is_empty(&self) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let count = crate::persistence::sql_runtime::SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT COUNT(*) AS count FROM settings",
                &[],
            )
            .await?
            .map(|row| row.i64("count"))
            .transpose()?
            .unwrap_or(0);
            Ok(count == 0)
        })
    }

    fn spawn_writer_task(&self, mut rx: mpsc::Receiver<DbWriteCommand>) {
        let writer = DatabaseWriterExecutor::from_database(self);
        let worker = async move {
            while let Some(command) = rx.recv().await {
                match command {
                    DbWriteCommand::ArchiveJob { job_id, history } => {
                        let writer = writer.clone();
                        tokio::task::spawn_blocking(move || {
                            if let Err(error) = writer.archive_job(job_id, history.as_ref()) {
                                tracing::warn!(
                                    job_id = job_id.0,
                                    error = %error,
                                    "failed to archive job on database writer path"
                                );
                            }
                        })
                        .await
                        .ok();
                    }
                    DbWriteCommand::InsertJobEvents { events } => {
                        let writer = writer.clone();
                        tokio::task::spawn_blocking(move || {
                            if let Err(error) = writer.insert_job_events(&events) {
                                tracing::warn!(
                                    count = events.len(),
                                    error = %error,
                                    "failed to persist job events on database writer path"
                                );
                            }
                        })
                        .await
                        .ok();
                    }
                    DbWriteCommand::Flush { reply } => {
                        let _ = reply.send(());
                    }
                }
            }
        };

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(worker);
        } else {
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("database writer runtime should build");
                runtime.block_on(worker);
            });
        }
    }

    pub fn try_queue_archive_job(
        &self,
        job_id: crate::jobs::ids::JobId,
        history: crate::history::JobHistoryRow,
    ) -> Result<(), StateError> {
        let command = DbWriteCommand::ArchiveJob {
            job_id,
            history: Box::new(history),
        };
        match self.writer_tx.try_send(command) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(command)) => {
                let tx = self.writer_tx.clone();
                let pending_archive_retries = self.pending_archive_retries.clone();
                let pending_archive_notify = self.pending_archive_notify.clone();
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    pending_archive_retries.fetch_add(1, Ordering::AcqRel);
                    handle.spawn(async move {
                        let send_result = tx.send(command).await;
                        pending_archive_retries.fetch_sub(1, Ordering::AcqRel);
                        pending_archive_notify.notify_waiters();
                        if let Err(error) = send_result {
                            tracing::warn!(error = %error, "database writer queue closed while retrying archive enqueue");
                        }
                    });
                    Ok(())
                } else {
                    tx.blocking_send(command).map_err(|_| {
                        StateError::Database("database writer queue closed".to_string())
                    })
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(StateError::Database(
                "database writer queue closed".to_string(),
            )),
        }
    }

    pub async fn queue_archive_job(
        &self,
        job_id: crate::jobs::ids::JobId,
        history: crate::history::JobHistoryRow,
    ) -> Result<(), StateError> {
        self.writer_tx
            .send(DbWriteCommand::ArchiveJob {
                job_id,
                history: Box::new(history),
            })
            .await
            .map_err(|_| StateError::Database("database writer queue closed".to_string()))
    }

    pub async fn queue_job_events(
        &self,
        events: Vec<crate::history::JobEvent>,
    ) -> Result<(), StateError> {
        self.writer_tx
            .send(DbWriteCommand::InsertJobEvents { events })
            .await
            .map_err(|_| StateError::Database("database writer queue closed".to_string()))
    }

    pub async fn flush_write_queue(&self) -> Result<(), StateError> {
        self.wait_for_pending_archive_retries().await;
        let (reply, rx) = oneshot::channel();
        self.writer_tx
            .send(DbWriteCommand::Flush { reply })
            .await
            .map_err(|_| StateError::Database("database writer queue closed".to_string()))?;
        rx.await
            .map_err(|_| StateError::Database("database writer flush failed".to_string()))
    }

    async fn wait_for_pending_archive_retries(&self) {
        loop {
            if self.pending_archive_retries.load(Ordering::Acquire) == 0 {
                return;
            }
            let notified = self.pending_archive_notify.notified();
            if self.pending_archive_retries.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Re-encrypt any plaintext passwords in the database.
    ///
    /// On upgrade from a version without encryption, passwords are stored as
    /// plaintext. This reads each one and re-writes it, which triggers the
    /// encrypt-on-write path. Idempotent — already-encrypted values pass through.
    pub fn migrate_plaintext_credentials(&self) -> Result<(), StateError> {
        use crate::persistence::encryption::{is_encrypted, maybe_encrypt};
        use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

        let Some(key) = self.encryption_key() else {
            return Ok(()); // no key set, nothing to do
        };
        let key = key.clone();
        let datastore = self.datastore();

        self.run_sql_blocking(async move {
            let server_rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, password FROM servers WHERE password IS NOT NULL",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| Ok((row.i64("id")?, row.text("password")?)))
            .collect::<Result<Vec<_>, StateError>>()?
            .into_iter()
            .filter(|(_, pw)| !pw.is_empty() && !is_encrypted(pw))
            .collect::<Vec<_>>();

            for (id, plaintext) in &server_rows {
                let val = Some(plaintext.clone());
                if let Some(encrypted) = maybe_encrypt(Some(&key), &val) {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "UPDATE servers SET password = {} WHERE id = {}",
                        &[SqlArg::Text(encrypted), SqlArg::I64(*id)],
                    )
                    .await?;
                }
            }
            if !server_rows.is_empty() {
                tracing::info!(
                    count = server_rows.len(),
                    "encrypted plaintext server passwords"
                );
            }

            let feed_rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, password FROM rss_feeds WHERE password IS NOT NULL",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| Ok((row.i64("id")?, row.text("password")?)))
            .collect::<Result<Vec<_>, StateError>>()?
            .into_iter()
            .filter(|(_, pw)| !pw.is_empty() && !is_encrypted(pw))
            .collect::<Vec<_>>();

            for (id, plaintext) in &feed_rows {
                let val = Some(plaintext.clone());
                if let Some(encrypted) = maybe_encrypt(Some(&key), &val) {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "UPDATE rss_feeds SET password = {} WHERE id = {}",
                        &[SqlArg::Text(encrypted), SqlArg::I64(*id)],
                    )
                    .await?;
                }
            }
            if !feed_rows.is_empty() {
                tracing::info!(
                    count = feed_rows.len(),
                    "encrypted plaintext RSS feed passwords"
                );
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;
