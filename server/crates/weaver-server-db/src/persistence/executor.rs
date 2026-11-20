// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::persistence::StateError;
use crate::persistence::database_target::DatabaseTarget;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::Instant;
use tokio::sync::Semaphore;
use weaver_server_base::operations::instrumentation::DbRuntimeMetrics;

type SqliteDatabaseRuntimeJob = Box<dyn FnOnce(&tokio::runtime::Runtime) + Send + 'static>;
enum PostgresDatabaseRuntimeJob {
    Send(Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>),
    Local(Box<dyn FnOnce(&tokio::runtime::Runtime) + Send + 'static>),
}

#[derive(Clone)]
pub enum DatabaseRuntimeWorker {
    // SQLite carries two executors on purpose. `writer` is a single thread —
    // SQLite is at its best with one writer, and serialising there is what keeps
    // same-key state transitions ordered. `reads` is a small multi-threaded
    // executor, because in WAL mode readers take a snapshot and neither block
    // the writer nor are blocked by it, so there is no reason for a query to
    // queue behind the download pipeline's writes. The pool already has 16
    // connections, each on its own sqlx worker thread; before this split, 15 of
    // them could never be in flight.
    Sqlite {
        writer: SqliteDatabaseRuntimeWorker,
        reads: SqliteReadRuntimeWorker,
    },
    // PostgreSQL needs none of this: its executor is already multi-threaded with
    // a permit per pooled connection, so reads and writes are concurrent
    // whichever entry point they use. `run_sql_blocking_read` forwards to the
    // same executor rather than adding a second one.
    Postgres(PostgresDatabaseRuntimeWorker),
}

impl DatabaseRuntimeWorker {
    pub fn start(target: &DatabaseTarget) -> Result<Self, StateError> {
        match target {
            DatabaseTarget::PostgresUrl(_) => {
                PostgresDatabaseRuntimeWorker::start(postgres_db_concurrency_from_env())
                    .map(Self::Postgres)
            }
            DatabaseTarget::SqlitePath(_) | DatabaseTarget::SqliteUrl(_) => Ok(Self::Sqlite {
                writer: SqliteDatabaseRuntimeWorker::start()?,
                reads: SqliteReadRuntimeWorker::start(sqlite_read_concurrency_from_env())?,
            }),
        }
    }

    pub fn block_on<T, Fut>(
        &self,
        caller: &'static std::panic::Location<'static>,
        future: Fut,
    ) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + Send + 'static,
    {
        #[cfg(any(test, feature = "test-support"))]
        blocking_call_probe::note(caller);
        #[cfg(any(test, feature = "test-support"))]
        actor_thread::note(caller);
        match self {
            Self::Sqlite { writer, .. } => writer.block_on(caller, future),
            Self::Postgres(worker) => worker.block_on(future),
        }
    }

    pub fn block_on_local<T, Build, Fut>(
        &self,
        caller: &'static std::panic::Location<'static>,
        build: Build,
    ) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + 'static,
    {
        #[cfg(any(test, feature = "test-support"))]
        blocking_call_probe::note(caller);
        #[cfg(any(test, feature = "test-support"))]
        actor_thread::note(caller);
        match self {
            Self::Sqlite { writer, .. } => writer.block_on_local(caller, build),
            Self::Postgres(_) => Err(StateError::Database(
                "run_sql_blocking_local requires sqlite datastore".to_string(),
            )),
        }
    }

    // Dispatch a **pure read** — no writes, and not part of a write transaction.
    pub fn block_on_read<T, Fut>(
        &self,
        caller: &'static std::panic::Location<'static>,
        future: Fut,
    ) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + Send + 'static,
    {
        #[cfg(any(test, feature = "test-support"))]
        blocking_call_probe::note(caller);
        #[cfg(any(test, feature = "test-support"))]
        actor_thread::note(caller);
        match self {
            Self::Sqlite { reads, .. } => reads.block_on(caller, future),
            Self::Postgres(worker) => worker.block_on(future),
        }
    }

    pub fn block_on_startup_local<T, Build, Fut>(
        &self,
        caller: &'static std::panic::Location<'static>,
        build: Build,
    ) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + 'static,
    {
        #[cfg(any(test, feature = "test-support"))]
        blocking_call_probe::note(caller);
        match self {
            Self::Sqlite { writer, .. } => writer.block_on_local(caller, build),
            Self::Postgres(worker) => worker.block_on_local(build),
        }
    }

    // Saturation and latency counters for whichever executor is in use.
    //
    // For SQLite this is the WRITE executor's gauge: the read lane is a
    // separate sqlx pool whose calls never queue behind the writer, so
    // folding it into this single snapshot would blur the one saturation
    // signal that matters (the serialized writer). Exposing the read lane
    // as its own metric is follow-up work on the metrics surface, not
    // something to smuggle into an accessor.
    pub fn runtime_metrics(&self) -> &Arc<DbRuntimeMetrics> {
        match self {
            Self::Sqlite { writer, .. } => &writer.metrics,
            Self::Postgres(worker) => &worker.metrics,
        }
    }
}

// The SQLite executor is a single thread, so *every* database call in the
// process — reads included — is serialized behind whatever is already running
// or queued on it. A caller's wall time is therefore `wait + exec`, and only
// `exec` is that caller's own cost; `wait` is somebody else's work. Reporting
// only the sum (as `db.runtime.sqlite.block_on` did) makes a victim of queueing
// look identical to a slow query, which is what made post-processing finalize
// latency unattributable.
//
// These probes split the two and attribute both to the `#[track_caller]` call
// site, and sample the queue depth seen at submit. All of it is behind
// `WEAVER_PROFILE_HOT_PATHS`; when that is off `record*` returns immediately
// and the only residual cost is the depth counter.
#[derive(Clone)]
pub struct SqliteDatabaseRuntimeWorker {
    tx: std_mpsc::Sender<SqliteDatabaseRuntimeJob>,
    // Executor saturation and operation latency. Not hot-path state: every
    // site that touches it is already blocking on a channel round-trip to the
    // database thread and already reads the clock for the `perf_probe` record
    // alongside it.
    metrics: Arc<DbRuntimeMetrics>,
    // Jobs submitted but not yet picked up by the executor thread. The channel
    // itself is unbounded and exposes no depth, so this is the only queueing
    // signal available.
    queue_depth: Arc<AtomicUsize>,
}

// Bookkeeping shared by both submit paths: count the job in, and hand back the
// enqueue instant so the executor side can split wait from exec.
struct SqliteSubmission {
    queued_at: Instant,
    queue_depth: Arc<AtomicUsize>,
    caller: &'static std::panic::Location<'static>,
}

impl SqliteSubmission {
    fn open(
        worker: &SqliteDatabaseRuntimeWorker,
        caller: &'static std::panic::Location<'static>,
    ) -> Self {
        let depth = worker.queue_depth.fetch_add(1, Ordering::AcqRel) + 1;
        weaver_server_base::runtime::perf_probe::record_value(
            "db.sqlite.queue.depth_at_submit",
            depth as u64,
        );
        Self {
            queued_at: Instant::now(),
            queue_depth: worker.queue_depth.clone(),
            caller,
        }
    }

    // Called on the executor thread the moment the job is picked up.
    fn start(self) -> SqliteExecution {
        let waited = self.queued_at.elapsed();
        self.queue_depth.fetch_sub(1, Ordering::AcqRel);
        // Guarded: building the per-caller label allocates, and this runs on
        // every database call in the process. Nothing here may cost anything
        // when profiling is off.
        if weaver_server_base::runtime::perf_probe::enabled() {
            weaver_server_base::runtime::perf_probe::record("db.sqlite.queue.wait", waited);
            weaver_server_base::runtime::perf_probe::record_owned(
                format!("db.sqlite.caller.{}.wait", caller_label(self.caller)),
                waited,
            );
        }
        SqliteExecution {
            started: Instant::now(),
            caller: self.caller,
        }
    }
}

struct SqliteExecution {
    started: Instant,
    caller: &'static std::panic::Location<'static>,
}

impl Drop for SqliteExecution {
    fn drop(&mut self) {
        if !weaver_server_base::runtime::perf_probe::enabled() {
            return;
        }
        let elapsed = self.started.elapsed();
        weaver_server_base::runtime::perf_probe::record("db.sqlite.queue.exec", elapsed);
        weaver_server_base::runtime::perf_probe::record_owned(
            format!("db.sqlite.caller.{}.exec", caller_label(self.caller)),
            elapsed,
        );
    }
}

// `file:line` of the call site, with the workspace-relative tail only so the
// bucket labels stay stable across checkouts.
fn caller_label(location: &std::panic::Location<'static>) -> String {
    let file = location.file();
    let short = file
        .rfind("/src/")
        .map(|index| &file[index + 5..])
        .unwrap_or(file);
    format!("{short}:{}", location.line())
}

impl SqliteDatabaseRuntimeWorker {
    fn start() -> Result<Self, StateError> {
        let (tx, rx) = std_mpsc::channel::<SqliteDatabaseRuntimeJob>();
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(1);

        std::thread::Builder::new()
            .name("weaver-sqlite-db-runtime".to_string())
            .spawn(move || {
                // sqlx hands a dropped connection back to the pool from a
                // spawned task. A current-thread runtime only runs that task
                // inside the next job's `block_on`, so an idle writer kept its
                // last connection checked out and a one-connection pool timed
                // out on the next read. One worker thread runs it at once.
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name("weaver-sqlite-db-runtime-worker")
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

        Ok(Self {
            tx,
            // The sqlite runtime is a single serialized worker thread.
            metrics: Arc::new(DbRuntimeMetrics::new("sqlite", 1)),
            queue_depth: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn block_on<T, Fut>(
        &self,
        caller: &'static std::panic::Location<'static>,
        future: Fut,
    ) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + Send + 'static,
    {
        let started = Instant::now();
        let _in_flight = self.metrics.note_submission_started();
        let submission = SqliteSubmission::open(self, caller);
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.tx
            .send(Box::new(move |runtime| {
                let _execution = submission.start();
                let _ = reply_tx.send(runtime.block_on(future));
            }))
            .map_err(|_| StateError::Database("database runtime worker stopped".to_string()))?;
        let result = reply_rx
            .recv()
            .map_err(|_| StateError::Database("database runtime worker panicked".to_string()))?;
        let elapsed = started.elapsed();
        weaver_server_base::runtime::perf_probe::record("db.runtime.sqlite.block_on", elapsed);
        self.metrics.note_submission_finished(elapsed);
        result
    }

    fn block_on_local<T, Build, Fut>(
        &self,
        caller: &'static std::panic::Location<'static>,
        build: Build,
    ) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + 'static,
    {
        let started = Instant::now();
        let _in_flight = self.metrics.note_submission_started();
        let submission = SqliteSubmission::open(self, caller);
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.tx
            .send(Box::new(move |runtime| {
                let _execution = submission.start();
                let _ = reply_tx.send(runtime.block_on(build()));
            }))
            .map_err(|_| StateError::Database("database runtime worker stopped".to_string()))?;
        let result = reply_rx
            .recv()
            .map_err(|_| StateError::Database("database runtime worker panicked".to_string()))?;
        let elapsed = started.elapsed();
        weaver_server_base::runtime::perf_probe::record(
            "db.runtime.sqlite.block_on_local",
            elapsed,
        );
        self.metrics.note_submission_finished(elapsed);
        result
    }
}

#[derive(Clone)]
pub struct PostgresDatabaseRuntimeWorker {
    tx: std_mpsc::SyncSender<PostgresDatabaseRuntimeJob>,
    in_flight: Arc<AtomicUsize>,
    blocked_submissions: Arc<AtomicUsize>,
    concurrency: usize,
    // Executor saturation and operation latency. See the sqlite worker for
    // why this is not hot-path state.
    metrics: Arc<DbRuntimeMetrics>,
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
            metrics: Arc::new(DbRuntimeMetrics::new("postgres", concurrency as u64)),
        })
    }

    fn block_on<T, Fut>(&self, future: Fut) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + Send + 'static,
    {
        let started = Instant::now();
        let _in_flight = self.metrics.note_submission_started();
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
                self.metrics.note_submission_blocked();
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
                weaver_server_base::runtime::perf_probe::record(
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
        let elapsed = started.elapsed();
        weaver_server_base::runtime::perf_probe::record("db.runtime.postgres.block_on", elapsed);
        self.metrics.note_submission_finished(elapsed);
        result
    }

    fn block_on_local<T, Build, Fut>(&self, build: Build) -> Result<T, StateError>
    where
        T: Send + 'static,
        Build: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + 'static,
    {
        let started = Instant::now();
        let _in_flight = self.metrics.note_submission_started();
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        let job = PostgresDatabaseRuntimeJob::Local(Box::new(move |runtime| {
            let _ = reply_tx.send(runtime.block_on(build()));
        }));

        match self.tx.try_send(job) {
            Ok(()) => {}
            Err(std_mpsc::TrySendError::Full(job)) => {
                let blocked_started = Instant::now();
                self.metrics.note_submission_blocked();
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
                weaver_server_base::runtime::perf_probe::record(
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
        let elapsed = started.elapsed();
        weaver_server_base::runtime::perf_probe::record(
            "db.runtime.postgres.block_on_local",
            elapsed,
        );
        self.metrics.note_submission_finished(elapsed);
        result
    }
}

// Multi-threaded executor for pure SQLite reads.
//
// WAL gives every reader a consistent snapshot taken at its own BEGIN, and a
// reader neither blocks nor is blocked by the single writer. So reads only need
// an executor that can drive several of them at once; the concurrency limit is
// the pool size, since each in-flight read holds one pooled connection.
//
// Ordering note, deliberately unchanged: a read submitted *after* a synchronous
// write call has returned still observes that write, because the caller blocked
// until the writer committed and the reader's snapshot is taken afterwards.
// Writes queued through `writer_tx` (`try_queue_write` / `try_queue_archive_job`)
// were already asynchronous with respect to readers before this split — that is
// why `flush_write_queue` and the archive `committed` oneshot exist — so callers
// that need read-your-write on those paths must keep using them.
#[derive(Clone)]
pub struct SqliteReadRuntimeWorker {
    tx: std_mpsc::Sender<SqliteReadJob>,
    queue_depth: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
}

type SqliteReadJob = Box<dyn FnOnce(&tokio::runtime::Runtime) + Send + 'static>;

impl SqliteReadRuntimeWorker {
    fn start(concurrency: usize) -> Result<Self, StateError> {
        let concurrency = concurrency.max(1);
        let (tx, rx) = std_mpsc::channel::<SqliteReadJob>();
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(1);

        std::thread::Builder::new()
            .name("weaver-sqlite-read-dispatch".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(concurrency)
                    .thread_name("weaver-sqlite-read")
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
                // Dispatch only: each job spawns onto the multi-threaded runtime
                // and this loop moves straight to the next, so reads overlap
                // instead of queueing behind each other.
                while let Ok(job) = rx.recv() {
                    job(&runtime);
                }
            })
            .map_err(|error| StateError::Database(error.to_string()))?;

        ready_rx
            .recv()
            .map_err(|_| StateError::Database("database read worker stopped".to_string()))?
            .map_err(StateError::Database)?;

        Ok(Self {
            tx,
            queue_depth: Arc::new(AtomicUsize::new(0)),
            in_flight: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn block_on<T, Fut>(
        &self,
        caller: &'static std::panic::Location<'static>,
        future: Fut,
    ) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, StateError>> + Send + 'static,
    {
        let started = Instant::now();
        let queued_at = Instant::now();
        let depth = self.queue_depth.fetch_add(1, Ordering::AcqRel) + 1;
        weaver_server_base::runtime::perf_probe::record_value(
            "db.sqlite.read.depth_at_submit",
            depth as u64,
        );
        let queue_depth = self.queue_depth.clone();
        let in_flight = self.in_flight.clone();
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.tx
            .send(Box::new(move |runtime| {
                runtime.spawn(async move {
                    let waited = queued_at.elapsed();
                    queue_depth.fetch_sub(1, Ordering::AcqRel);
                    let active = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
                    if weaver_server_base::runtime::perf_probe::enabled() {
                        weaver_server_base::runtime::perf_probe::record(
                            "db.sqlite.read.wait",
                            waited,
                        );
                        weaver_server_base::runtime::perf_probe::record_value(
                            "db.sqlite.read.in_flight",
                            active as u64,
                        );
                    }
                    let exec_started = Instant::now();
                    let result = future.await;
                    in_flight.fetch_sub(1, Ordering::AcqRel);
                    if weaver_server_base::runtime::perf_probe::enabled() {
                        let elapsed = exec_started.elapsed();
                        weaver_server_base::runtime::perf_probe::record(
                            "db.sqlite.read.exec",
                            elapsed,
                        );
                        weaver_server_base::runtime::perf_probe::record_owned(
                            format!("db.sqlite.read.caller.{}.exec", caller_label(caller)),
                            elapsed,
                        );
                    }
                    let _ = reply_tx.send(result);
                });
            }))
            .map_err(|_| StateError::Database("database read worker stopped".to_string()))?;
        let result = reply_rx
            .recv()
            .map_err(|_| StateError::Database("database read worker panicked".to_string()))?;
        weaver_server_base::runtime::perf_probe::record(
            "db.runtime.sqlite.block_on_read",
            started.elapsed(),
        );
        result
    }
}

// Read concurrency defaults to the SQLite pool size: every in-flight read holds
// one pooled connection, so more workers than connections only queues inside
// sqlx instead of here.
fn sqlite_read_concurrency_from_env() -> usize {
    let pool_max = crate::persistence::sql_services::sqlite_max_connections_from_env() as usize;
    std::env::var("WEAVER_SQLITE_READ_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(pool_max)
        .clamp(1, pool_max.max(1))
}

pub fn postgres_db_concurrency_from_env() -> usize {
    let pool_max = crate::persistence::sql_services::postgres_max_connections_from_env() as usize;
    std::env::var("WEAVER_POSTGRES_DB_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(pool_max)
        .clamp(1, pool_max.max(1))
}

// The database calls made from the pipeline actor's own thread, counted
// by call site for the tests: a call there stalls every turn behind it.
#[cfg(any(test, feature = "test-support"))]
pub mod actor_thread {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    thread_local! {
        static ON_ACTOR: Cell<bool> = const { Cell::new(false) };
    }

    static CALLS: Mutex<BTreeMap<String, u64>> = Mutex::new(BTreeMap::new());

    pub fn note(caller: &'static std::panic::Location<'static>) {
        if ON_ACTOR.with(Cell::get) {
            *CALLS
                .lock()
                .expect("actor thread counter poisoned")
                .entry(super::caller_label(caller))
                .or_default() += 1;
        }
    }

    pub fn total() -> u64 {
        CALLS
            .lock()
            .expect("actor thread counter poisoned")
            .values()
            .sum()
    }

    // Marks the thread polling the wrapped future as the actor's.
    pub struct OnActor<F>(pub std::pin::Pin<Box<F>>);

    impl<F: std::future::Future> std::future::Future for OnActor<F> {
        type Output = F::Output;

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<F::Output> {
            ON_ACTOR.with(|on| on.set(true));
            let polled = self.0.as_mut().poll(cx);
            ON_ACTOR.with(|on| on.set(false));
            polled
        }
    }
}

// Every blocking database call made on the thread that armed it, by the
// caller's location: a test proves an actor turn reads nothing by arming the
// probe on the actor's thread, running the turn, and taking the list.
#[cfg(any(test, feature = "test-support"))]
pub mod blocking_call_probe {
    use std::cell::RefCell;
    use std::panic::Location;

    thread_local! {
        static CALLS: RefCell<Option<Vec<&'static Location<'static>>>> =
            const { RefCell::new(None) };
    }

    pub fn arm() {
        CALLS.with(|calls| *calls.borrow_mut() = Some(Vec::new()));
    }

    pub fn take() -> Vec<&'static Location<'static>> {
        CALLS.with(|calls| calls.borrow_mut().take().unwrap_or_default())
    }

    pub fn note(caller: &'static Location<'static>) {
        CALLS.with(|calls| {
            if let Some(calls) = calls.borrow_mut().as_mut() {
                calls.push(caller);
            }
        });
    }
}
