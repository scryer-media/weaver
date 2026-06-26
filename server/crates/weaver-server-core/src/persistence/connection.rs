use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::{Notify, Semaphore, mpsc, oneshot};

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
const JOB_HISTORY_CACHE_CAPACITY: usize = 1024;

type SqliteDatabaseRuntimeJob = Box<dyn FnOnce(&tokio::runtime::Runtime) + Send + 'static>;
enum PostgresDatabaseRuntimeJob {
    Send(Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>),
    Local(Box<dyn FnOnce(&tokio::runtime::Runtime) + Send + 'static>),
}

#[derive(Clone)]
enum DatabaseRuntimeWorker {
    Sqlite(SqliteDatabaseRuntimeWorker),
    Postgres(PostgresDatabaseRuntimeWorker),
}

impl DatabaseRuntimeWorker {
    fn start(target: &DatabaseTarget) -> Result<Self, StateError> {
        match target {
            DatabaseTarget::PostgresUrl(_) => {
                PostgresDatabaseRuntimeWorker::start(postgres_db_concurrency_from_env())
                    .map(Self::Postgres)
            }
            DatabaseTarget::SqlitePath(_) | DatabaseTarget::SqliteUrl(_) => {
                SqliteDatabaseRuntimeWorker::start().map(Self::Sqlite)
            }
        }
    }

    fn block_on<T, Fut>(&self, future: Fut) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + Send + 'static,
    {
        match self {
            Self::Sqlite(worker) => worker.block_on(future),
            Self::Postgres(worker) => worker.block_on(future),
        }
    }

    fn block_on_local<T, Build, Fut>(&self, build: Build) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + 'static,
    {
        match self {
            Self::Sqlite(worker) => worker.block_on_local(build),
            Self::Postgres(_) => Err(StateError::Database(
                "run_sql_blocking_local requires sqlite datastore".to_string(),
            )),
        }
    }

    fn block_on_startup_local<T, Build, Fut>(&self, build: Build) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + 'static,
    {
        match self {
            Self::Sqlite(worker) => worker.block_on_local(build),
            Self::Postgres(worker) => worker.block_on_local(build),
        }
    }
}

#[derive(Clone)]
struct SqliteDatabaseRuntimeWorker {
    tx: std_mpsc::Sender<SqliteDatabaseRuntimeJob>,
}

impl SqliteDatabaseRuntimeWorker {
    fn start() -> Result<Self, StateError> {
        let (tx, rx) = std_mpsc::channel::<SqliteDatabaseRuntimeJob>();
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(1);

        std::thread::Builder::new()
            .name("weaver-sqlite-db-runtime".to_string())
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
        Fut: Future<Output = Result<T, StateError>> + Send + 'static,
    {
        let started = Instant::now();
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.tx
            .send(Box::new(move |runtime| {
                let _ = reply_tx.send(runtime.block_on(future));
            }))
            .map_err(|_| StateError::Database("database runtime worker stopped".to_string()))?;
        let result = reply_rx
            .recv()
            .map_err(|_| StateError::Database("database runtime worker panicked".to_string()))?;
        crate::runtime::perf_probe::record("db.runtime.sqlite.block_on", started.elapsed());
        result
    }

    fn block_on_local<T, Build, Fut>(&self, build: Build) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + 'static,
    {
        let started = Instant::now();
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.tx
            .send(Box::new(move |runtime| {
                let _ = reply_tx.send(runtime.block_on(build()));
            }))
            .map_err(|_| StateError::Database("database runtime worker stopped".to_string()))?;
        let result = reply_rx
            .recv()
            .map_err(|_| StateError::Database("database runtime worker panicked".to_string()))?;
        crate::runtime::perf_probe::record("db.runtime.sqlite.block_on_local", started.elapsed());
        result
    }
}

#[derive(Clone)]
struct PostgresDatabaseRuntimeWorker {
    tx: std_mpsc::SyncSender<PostgresDatabaseRuntimeJob>,
    in_flight: Arc<AtomicUsize>,
    blocked_submissions: Arc<AtomicUsize>,
    concurrency: usize,
}

impl PostgresDatabaseRuntimeWorker {
    fn start(concurrency: usize) -> Result<Self, StateError> {
        let concurrency = concurrency.max(1);
        let (tx, rx) = std_mpsc::sync_channel::<PostgresDatabaseRuntimeJob>(concurrency);
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(1);
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let blocked_submissions = Arc::new(AtomicUsize::new(0));
        let worker_in_flight = in_flight.clone();

        std::thread::Builder::new()
            .name("weaver-postgres-db-dispatch".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name("weaver-postgres-db-runtime")
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
                    match job {
                        PostgresDatabaseRuntimeJob::Local(job) => job(&runtime),
                        PostgresDatabaseRuntimeJob::Send(job) => {
                            let permit = match runtime.block_on(semaphore.clone().acquire_owned()) {
                                Ok(permit) => permit,
                                Err(_) => break,
                            };
                            let in_flight = worker_in_flight.clone();
                            let active = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
                            tracing::trace!(
                                db_executor = "postgres",
                                db_in_flight = active,
                                db_concurrency = concurrency,
                                "starting postgres database runtime job"
                            );
                            runtime.spawn(async move {
                                let _permit = permit;
                                job().await;
                                let remaining =
                                    in_flight.fetch_sub(1, Ordering::AcqRel).saturating_sub(1);
                                tracing::trace!(
                                    db_executor = "postgres",
                                    db_in_flight = remaining,
                                    db_concurrency = concurrency,
                                    "finished postgres database runtime job"
                                );
                            });
                        }
                    }
                }
            })
            .map_err(|error| StateError::Database(error.to_string()))?;

        ready_rx
            .recv()
            .map_err(|_| StateError::Database("database runtime worker stopped".to_string()))?
            .map_err(StateError::Database)?;

        Ok(Self {
            tx,
            in_flight,
            blocked_submissions,
            concurrency,
        })
    }

    fn block_on<T, Fut>(&self, future: Fut) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + Send + 'static,
    {
        let started = Instant::now();
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        let job = PostgresDatabaseRuntimeJob::Send(Box::new(move || {
            Box::pin(async move {
                let _ = reply_tx.send(future.await);
            })
        }));

        match self.tx.try_send(job) {
            Ok(()) => {}
            Err(std_mpsc::TrySendError::Full(job)) => {
                let blocked_started = Instant::now();
                let blocked = self.blocked_submissions.fetch_add(1, Ordering::AcqRel) + 1;
                tracing::debug!(
                    db_executor = "postgres",
                    db_in_flight = self.in_flight.load(Ordering::Acquire),
                    db_concurrency = self.concurrency,
                    db_blocked_submissions = blocked,
                    "postgres database executor queue full; blocking caller"
                );
                self.tx.send(job).map_err(|_| {
                    StateError::Database("database runtime worker stopped".to_string())
                })?;
                crate::runtime::perf_probe::record(
                    "db.runtime.postgres.submit_blocked",
                    blocked_started.elapsed(),
                );
            }
            Err(std_mpsc::TrySendError::Disconnected(_)) => {
                tracing::warn!(
                    db_executor = "postgres",
                    db_in_flight = self.in_flight.load(Ordering::Acquire),
                    db_concurrency = self.concurrency,
                    "postgres database executor rejected job because queue is closed"
                );
                return Err(StateError::Database(
                    "database runtime worker stopped".to_string(),
                ));
            }
        }

        let result = reply_rx
            .recv()
            .map_err(|_| StateError::Database("database runtime worker panicked".to_string()))?;
        crate::runtime::perf_probe::record("db.runtime.postgres.block_on", started.elapsed());
        result
    }

    fn block_on_local<T, Build, Fut>(&self, build: Build) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + 'static,
    {
        let started = Instant::now();
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        let job = PostgresDatabaseRuntimeJob::Local(Box::new(move |runtime| {
            let _ = reply_tx.send(runtime.block_on(build()));
        }));

        match self.tx.try_send(job) {
            Ok(()) => {}
            Err(std_mpsc::TrySendError::Full(job)) => {
                let blocked_started = Instant::now();
                let blocked = self.blocked_submissions.fetch_add(1, Ordering::AcqRel) + 1;
                tracing::debug!(
                    db_executor = "postgres",
                    db_in_flight = self.in_flight.load(Ordering::Acquire),
                    db_concurrency = self.concurrency,
                    db_blocked_submissions = blocked,
                    "postgres database executor queue full; blocking startup caller"
                );
                self.tx.send(job).map_err(|_| {
                    StateError::Database("database runtime worker stopped".to_string())
                })?;
                crate::runtime::perf_probe::record(
                    "db.runtime.postgres.submit_blocked_local",
                    blocked_started.elapsed(),
                );
            }
            Err(std_mpsc::TrySendError::Disconnected(_)) => {
                return Err(StateError::Database(
                    "database runtime worker stopped".to_string(),
                ));
            }
        }

        let result = reply_rx
            .recv()
            .map_err(|_| StateError::Database("database runtime worker panicked".to_string()))?;
        crate::runtime::perf_probe::record("db.runtime.postgres.block_on_local", started.elapsed());
        result
    }
}

fn postgres_db_concurrency_from_env() -> usize {
    let pool_max = crate::persistence::sql_services::postgres_max_connections_from_env() as usize;
    std::env::var("WEAVER_POSTGRES_DB_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(pool_max)
        .clamp(1, pool_max.max(1))
}

fn open_sql_services_blocking(
    sql_worker: &DatabaseRuntimeWorker,
    target: DatabaseTarget,
) -> Result<DatabaseServices, StateError> {
    sql_worker.block_on_startup_local(move || {
        DatabaseServices::open(target, crate::schema_migrations::MigrationMode::Apply)
    })
}

#[derive(Clone)]
pub(crate) struct DatabaseWriterExecutor {
    sql_services: DatabaseServices,
    sql_worker: DatabaseRuntimeWorker,
    job_history_cache: Arc<Mutex<JobHistoryCache>>,
}

impl DatabaseWriterExecutor {
    fn from_database(db: &Database) -> Self {
        Self {
            sql_services: db.sql_services.clone(),
            sql_worker: db.sql_worker.clone(),
            job_history_cache: db.job_history_cache.clone(),
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

    pub(crate) fn cache_job_history(&self, row: JobHistoryRow) {
        self.job_history_cache
            .lock()
            .expect("job history cache poisoned")
            .insert(row);
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

#[derive(Default)]
struct JobHistoryCache {
    rows: BTreeMap<u64, JobHistoryRow>,
    insertion_order: VecDeque<u64>,
}

impl JobHistoryCache {
    fn get(&self, job_id: u64) -> Option<JobHistoryRow> {
        self.rows.get(&job_id).cloned()
    }

    fn insert(&mut self, row: JobHistoryRow) {
        if !self.rows.contains_key(&row.job_id) {
            self.insertion_order.push_back(row.job_id);
        }
        self.rows.insert(row.job_id, row);
        while self.rows.len() > JOB_HISTORY_CACHE_CAPACITY {
            let Some(job_id) = self.insertion_order.pop_front() else {
                break;
            };
            self.rows.remove(&job_id);
        }
    }

    fn remove(&mut self, job_id: u64) {
        self.rows.remove(&job_id);
        self.insertion_order
            .retain(|cached_id| *cached_id != job_id);
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.insertion_order.clear();
    }
}

/// SQL-backed persistent store for config, servers, and job history.
#[derive(Clone)]
pub struct Database {
    sql_services: DatabaseServices,
    sql_worker: DatabaseRuntimeWorker,
    writer_tx: mpsc::Sender<DbWriteCommand>,
    pending_archive_retries: Arc<AtomicUsize>,
    pending_archive_notify: Arc<Notify>,
    job_history_cache: Arc<Mutex<JobHistoryCache>>,
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
        let sql_worker = DatabaseRuntimeWorker::start(&target)?;
        let sql_services = open_sql_services_blocking(&sql_worker, target)?;

        let (writer_tx, writer_rx) = mpsc::channel(SQLITE_WRITE_QUEUE_CAPACITY);
        let db = Self {
            sql_services,
            sql_worker,
            writer_tx,
            pending_archive_retries: Arc::new(AtomicUsize::new(0)),
            pending_archive_notify: Arc::new(Notify::new()),
            job_history_cache: Arc::new(Mutex::new(JobHistoryCache::default())),
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
        let target = DatabaseTarget::SqlitePath(path.clone());
        let sql_worker = DatabaseRuntimeWorker::start(&target)?;
        let sql_services = open_sql_services_blocking(&sql_worker, target)?;

        let (writer_tx, writer_rx) = mpsc::channel(SQLITE_WRITE_QUEUE_CAPACITY);
        let db = Self {
            sql_services,
            sql_worker,
            writer_tx,
            pending_archive_retries: Arc::new(AtomicUsize::new(0)),
            pending_archive_notify: Arc::new(Notify::new()),
            job_history_cache: Arc::new(Mutex::new(JobHistoryCache::default())),
            encryption_key: Some(crate::persistence::encryption::EncryptionKey::generate()),
            _ephemeral_dir: Some(tempdir),
        };
        db.spawn_writer_task(writer_rx);
        Ok(db)
    }

    pub(crate) fn datastore(&self) -> StoreDatastore {
        self.sql_services.datastore()
    }

    pub(crate) fn get_cached_job_history(&self, job_id: u64) -> Option<JobHistoryRow> {
        self.job_history_cache
            .lock()
            .expect("job history cache poisoned")
            .get(job_id)
    }

    pub(crate) fn cache_job_history(&self, row: JobHistoryRow) {
        self.job_history_cache
            .lock()
            .expect("job history cache poisoned")
            .insert(row);
    }

    pub(crate) fn invalidate_job_history_cache(&self, job_id: u64) {
        self.job_history_cache
            .lock()
            .expect("job history cache poisoned")
            .remove(job_id);
    }

    pub(crate) fn clear_job_history_cache(&self) {
        self.job_history_cache
            .lock()
            .expect("job history cache poisoned")
            .clear();
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
                            match writer.archive_job(job_id, history.as_ref()) {
                                Ok(Some(row)) => writer.cache_job_history(row),
                                Ok(None) => {}
                                Err(error) => {
                                    tracing::warn!(
                                        job_id = job_id.0,
                                        error = %error,
                                        "failed to archive job on database writer path"
                                    );
                                }
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
                if let Some(encrypted) =
                    maybe_encrypt(Some(&key), &val).map_err(StateError::Database)?
                {
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
                if let Some(encrypted) =
                    maybe_encrypt(Some(&key), &val).map_err(StateError::Database)?
                {
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
