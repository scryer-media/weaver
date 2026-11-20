// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::executor::DatabaseRuntimeWorker;
#[cfg(test)]
use super::executor::postgres_db_concurrency_from_env;
#[cfg(test)]
pub(crate) use super::executor::{actor_thread, blocking_call_probe};
use crate::history::cache::{HistoryWriterCacheQueries, JobHistoryCache};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::{Notify, Semaphore, mpsc, oneshot, watch};

use crate::StateError;
use crate::persistence::database_target::DatabaseTarget;
use crate::persistence::sql_runtime::StoreDatastore;
use crate::persistence::sql_services::DatabaseServices;
use weaver_server_db::persistence::connection::Database as SqlDatabase;
pub use weaver_server_db::persistence::connection::{
    SqliteFileSize, SqliteFileSizes, SqlitePageFacts,
};

pub use crate::auth::{ApiKeyRow, AuthCredentials};
pub use crate::history::{HistoryFilter, IntegrationEventRow, JobEvent, JobHistoryRow};
pub use crate::jobs::{ActiveFileIdentity, ActiveJob, RecoveredJob};
pub use crate::operations::{
    MetricsHistoryChunkRow, MetricsHistoryQueryData, MetricsHistoryQueryResult,
    RawMetricsHistoryPoint, RollupMetricsHistoryPoint, StableStateExport,
};
pub use crate::rss::{RssFeedRow, RssRuleAction, RssRuleRow, RssSeenItemRow};

const SQLITE_WRITE_QUEUE_CAPACITY: usize = 128;

#[derive(Clone)]
pub(crate) struct DatabaseWriterExecutor {
    sql_services: DatabaseServices,
    sql_worker: DatabaseRuntimeWorker,
    pub(crate) job_history_cache: Arc<Mutex<JobHistoryCache>>,
    #[cfg(any(test, feature = "test-support"))]
    torrent_history_writes: Arc<AtomicUsize>,
}

impl DatabaseWriterExecutor {
    fn from_database(db: &Database) -> Self {
        Self {
            sql_services: db.connection.services().clone(),
            sql_worker: db.connection.worker().clone(),
            job_history_cache: db.job_history_cache.clone(),
            #[cfg(any(test, feature = "test-support"))]
            torrent_history_writes: db.torrent_history_writes.clone(),
        }
    }

    // One more transaction wrote torrents' jobs' history rows.
    pub(crate) fn count_history_write(&self) {
        #[cfg(any(test, feature = "test-support"))]
        self.torrent_history_writes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn datastore(&self) -> StoreDatastore {
        self.sql_services.datastore()
    }

    #[track_caller]
    pub(crate) fn run_sql_blocking<T, Fut>(&self, future: Fut) -> Result<T, StateError>
    where
        T: Send + 'static,
        Fut: std::future::Future<Output = Result<T, StateError>> + Send + 'static,
    {
        self.sql_worker
            .block_on(std::panic::Location::caller(), future)
    }
}

type WriteOp = Box<dyn FnOnce(&Database) -> Result<(), StateError> + Send + 'static>;

// A writer-queue command plus the instant it was enqueued.
//
// A command's latency is `queue wait + execution`: on SQLite the ordered writer
// takes one command at a time, and on Postgres the wait also covers the
// command's lane. Carrying the enqueue instant is what lets the consumer
// report those two separately per command kind.
struct QueuedWrite {
    queued_at: Instant,
    command: DbWriteCommand,
}

enum DbWriteCommand {
    ArchiveJob {
        job_id: weaver_server_base::jobs::ids::JobId,
        history: Box<crate::history::JobHistoryRow>,
        typed_terminal_cause: Option<crate::jobs::SemanticTerminalCause>,
        failure_kind: Option<crate::history::HistoryFailureKind>,
        // Fired once the archive transaction has committed and the row has
        // been re-cached, so callers can hold a terminal event until the
        // history row is queryable through the read paths.
        committed: Option<oneshot::Sender<()>>,
    },
    // Several jobs' archives as one ordered write. Each archive keeps its own
    // transaction; `committed` fires once every one has run, naming the jobs
    // whose archive failed and why.
    ArchiveJobs {
        jobs: Vec<(
            weaver_server_base::jobs::ids::JobId,
            crate::history::JobHistoryRow,
        )>,
        failure_kind: Option<crate::history::HistoryFailureKind>,
        committed: oneshot::Sender<BTreeMap<weaver_server_base::jobs::ids::JobId, String>>,
    },
    // A torrent's job's history row, kept and tried again for want of room
    // as an NZB job's archive is. `retires` removes the torrent in the same
    // transaction. `kept` fires if the row is kept to be tried again.
    ArchiveTorrent {
        job_id: weaver_server_base::jobs::ids::JobId,
        history: Box<crate::history::JobHistoryRow>,
        retires: bool,
        // The files the torrent's job wrote, listed with its row.
        files: Arc<[crate::torrents::persistence::HistoryFile]>,
        committed: oneshot::Sender<()>,
        kept: oneshot::Sender<()>,
    },
    // Several torrents' jobs' history rows, of distinct jobs, in one
    // transaction. Any the transaction cannot take goes on as its own
    // `ArchiveTorrent`, in order, kept for want of room as that one is.
    ArchiveTorrents {
        archives: Vec<TorrentArchive>,
    },
    InsertJobEvents {
        events: Vec<crate::history::JobEvent>,
    },
    // A generic ordered write. Same-key state transitions land in enqueue
    // order: writes on one lane keep their order and a write with no lane
    // is a barrier. SQLite additionally serializes SQL execution.
    // `op` runs on a
    // `Database` clone the writer task holds, letting it call methods that
    // live on `Database` (e.g. `set_active_job_runtime` / `update_active_job`)
    // without duplicating them onto `DatabaseWriterExecutor`.
    Write {
        label: &'static str,
        // The lane this write extends. Writes on one lane keep
        // their order while other lanes' writes run alongside; `None` makes
        // the write a barrier against every other queued write.
        lane: Option<WriteLane>,
        op: WriteOp,
        // Answers with the write's outcome once it has run. A write that
        // never runs drops it, which its receiver reads as a failure.
        committed: Option<oneshot::Sender<Result<(), super::error::WriteFailed>>>,
    },
    GraphCheckpoint {
        job_id: weaver_server_base::jobs::ids::JobId,
        prepare: Box<dyn FnOnce() -> super::checkpoint_write::CheckpointWrite + Send>,
    },
    Flush {
        reply: oneshot::Sender<()>,
    },
    // The kept archives from place `from` on are tried again, each once, in
    // the order they were kept, at most [`KEPT_ARCHIVE_PASS`] of them.
    RetryKeptArchives {
        from: usize,
    },
}

// What an `ArchiveTorrent` carries: one torrent's job's history row.
struct TorrentArchive {
    job_id: weaver_server_base::jobs::ids::JobId,
    history: Box<crate::history::JobHistoryRow>,
    retires: bool,
    files: Arc<[crate::torrents::persistence::HistoryFile]>,
    committed: oneshot::Sender<()>,
    kept: oneshot::Sender<()>,
}

// What a torrent's history row's waiter hears: written, or kept for room
// (`Database::queue_torrent_history`).
pub(crate) type HistoryWaits = (oneshot::Receiver<()>, oneshot::Receiver<()>);

// The most kept archives one pass tries, so one pass does not hold the
// serial writer for the whole list.
#[cfg(not(test))]
const KEPT_ARCHIVE_PASS: usize = 256;
#[cfg(test)]
const KEPT_ARCHIVE_PASS: usize = 2;

// One job's archive the database had no room for, kept whole: what its
// command held, its `committed` sender with it, and nothing more.
struct KeptArchive {
    job_id: weaver_server_base::jobs::ids::JobId,
    history: Box<crate::history::JobHistoryRow>,
    typed_terminal_cause: Option<crate::jobs::SemanticTerminalCause>,
    failure_kind: Option<crate::history::HistoryFailureKind>,
    committed: Option<oneshot::Sender<()>>,
    of: ArchiveOf,
}

// What an archive writes: an NZB job's move out of the active tables, or a
// torrent's job's history row, with the torrent removed when it `retires`.
// A torrent's lists the files its job wrote, which an archive from history
// removes.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ArchiveOf {
    ActiveJob,
    Torrent {
        retires: bool,
        files: Arc<[crate::torrents::persistence::HistoryFile]>,
    },
}

// What the writer says of the archives it keeps, one job at a time, so
// whoever hears it counts them and never reads the list.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ArchiveNews {
    // The job's archive found no room and is kept.
    Kept(weaver_server_base::jobs::ids::JobId),
    // A kept archive is kept no longer: it was written, or it failed for
    // another reason and is lost as any archive that fails so is.
    LetGo {
        job_id: weaver_server_base::jobs::ids::JobId,
        written: bool,
    },
    // A pass wrote at least one archive and stopped at its cap with more
    // yet to try: the next pass starts at `from`.
    More {
        from: usize,
    },
}

type ArchiveNewsSink = Box<dyn Fn(ArchiveNews) + Send + Sync>;

// The archives kept for want of room, in the order they were kept, and who
// is told of them. Neither is touched by an archive that commits the first
// time.
struct KeptArchives {
    list: Mutex<VecDeque<KeptArchive>>,
    news: Mutex<Option<ArchiveNewsSink>>,
}

impl KeptArchives {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            list: Mutex::new(VecDeque::new()),
            news: Mutex::new(None),
        })
    }

    fn list(&self) -> std::sync::MutexGuard<'_, VecDeque<KeptArchive>> {
        self.list
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn tell(&self, news: ArchiveNews) {
        let sink = self
            .news
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(sink) = sink.as_ref() {
            sink(news);
        }
    }
}

// Whether an archive that failed with `error` is kept to be tried again.
// Only want of room on a SQLite database is: its folder's disk is read from
// here, so room made there is seen. A Postgres server's disk cannot be read
// from here, so an archive it refuses is lost as any failed archive is, and
// start-up recovery archives the job.
fn keeps_archive(target: &DatabaseTarget, error: &StateError) -> bool {
    matches!(error, StateError::OutOfSpace(_)) && !matches!(target, DatabaseTarget::PostgresUrl(_))
}

// Test seam: the errors the next archives answer with instead of writing,
// and how many archives were written.
#[cfg(test)]
pub(crate) static ARCHIVE_REFUSED: Mutex<VecDeque<StateError>> = Mutex::new(VecDeque::new());
#[cfg(test)]
pub(crate) static ARCHIVES_WRITTEN: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

// Test seam: the next pass over the kept archives panics on its thread.
#[cfg(test)]
pub(crate) static ARCHIVE_PASS_PANICS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Archive one job and cache its history row. Runs on the blocking pool.
// `retry` says the archive is a kept one tried again: refused again for want
// of room, it is logged at debug, since its pass says how many are still
// kept in one line.
fn archive_one(
    writer: &DatabaseWriterExecutor,
    archive: &KeptArchive,
    retry: bool,
) -> Result<(), StateError> {
    let KeptArchive {
        job_id,
        history,
        typed_terminal_cause,
        failure_kind,
        of,
        ..
    } = archive;
    let job_id = *job_id;
    #[cfg(test)]
    if let Some(refused) = ARCHIVE_REFUSED.lock().unwrap().pop_front() {
        return Err(refused);
    }
    // Capture the cache generation *before* the archive read so a concurrent
    // history-delete that bumps the generation makes the conditional insert
    // a no-op, rather than resurrecting the just-deleted row.
    let observed_generation = writer.job_history_cache_generation();
    let archived = match of {
        ArchiveOf::ActiveJob => writer.archive_job_with_terminal_cause(
            job_id,
            history,
            *typed_terminal_cause,
            *failure_kind,
        ),
        ArchiveOf::Torrent { retires, files } => writer
            .write_torrent_history(history, *retires, files)
            .map(|()| Some((**history).clone())),
    };
    let archived = archived.inspect_err(|error| {
        if retry && matches!(error, StateError::OutOfSpace(_)) {
            tracing::debug!(
                job_id = job_id.0,
                error = %error,
                "a kept history archive found no room again"
            );
        } else {
            tracing::warn!(
                job_id = job_id.0,
                error = %error,
                "failed to archive job on database writer path"
            );
        }
    })?;
    if let Some(row) = archived {
        writer.cache_job_history_at(row, observed_generation);
    }
    #[cfg(test)]
    ARCHIVES_WRITTEN.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

// Where an ordered write sits relative to the others on the Postgres writer.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum WriteLane {
    // Writes for one job, applied in submission order.
    Job(u64),
    // Job event batches, applied in submission order.
    Events,
    // Server attribution and support facts checkpoints, applied in submission
    // order. They only set `active_jobs.server_attribution` and
    // `active_jobs.support_facts`, which a job's other writes never touch and
    // its archive reads.
    ServerAttribution,
}

impl DbWriteCommand {
    // The lanes this command must wait out, and the lane it then extends.
    // `None` is a barrier: it runs after everything queued before it and
    // before anything queued after it, exactly as on the serial writer.
    fn lanes(&self) -> Option<(&'static [WriteLane], WriteLane)> {
        match self {
            // After every earlier write for its job, after every earlier
            // event batch, since those may carry this job's events, and after
            // every earlier attribution checkpoint, since the archive copies
            // the job's attribution into history.
            Self::ArchiveJob { job_id, .. } => Some((
                &[WriteLane::Events, WriteLane::ServerAttribution],
                WriteLane::Job(job_id.0),
            )),
            // As an NZB job's archive: its job's events and attribution
            // land first.
            Self::ArchiveTorrent { job_id, .. } => Some((
                &[WriteLane::Events, WriteLane::ServerAttribution],
                WriteLane::Job(job_id.0),
            )),
            Self::InsertJobEvents { .. } => Some((&[], WriteLane::Events)),
            Self::GraphCheckpoint { job_id, .. } => Some((&[], WriteLane::Job(job_id.0))),
            Self::Write {
                lane: Some(lane), ..
            } => Some((&[], *lane)),
            // Spans many jobs' lanes, so it is a barrier: every earlier write
            // for any of its jobs (a support facts save among them), every
            // event batch and every attribution checkpoint lands first.
            Self::ArchiveJobs { .. }
            | Self::ArchiveTorrents { .. }
            | Self::Write { lane: None, .. }
            | Self::Flush { .. }
            | Self::RetryKeptArchives { .. } => None,
        }
    }
}

// Starts the profiling scope for one writer command. The per-command labels
// allocate, so nothing is built unless profiling is on.
fn writer_command_scope(
    command: &DbWriteCommand,
    queued_at: Instant,
) -> Option<weaver_server_base::runtime::perf_probe::OwnedScope> {
    weaver_server_base::runtime::perf_probe::enabled().then(|| {
        let label = command.label();
        let waited = queued_at.elapsed();
        weaver_server_base::runtime::perf_probe::record("db.writer_queue.wait", waited);
        weaver_server_base::runtime::perf_probe::record_owned(
            format!("db.writer_queue.{label}.wait"),
            waited,
        );
        weaver_server_base::runtime::perf_probe::owned_scope(format!(
            "db.writer_queue.{label}.exec"
        ))
    })
}

// Runs one writer command to completion. An archive's `committed` signal and a
// flush's reply fire only after everything the command stands for is done.
async fn execute_write_command(
    command: DbWriteCommand,
    writer: &DatabaseWriterExecutor,
    writer_db: &Database,
    permits: &Arc<Semaphore>,
) {
    // Checkpoints acquire a slot per SQL attempt, releasing it while paused.
    // Every other command executes once under one slot.
    let _permit = if matches!(&command, DbWriteCommand::GraphCheckpoint { .. }) {
        None
    } else {
        Some(permits.clone().acquire_owned().await)
    };
    match command {
        DbWriteCommand::ArchiveJob {
            job_id,
            history,
            typed_terminal_cause,
            failure_kind,
            committed,
        } => {
            let blocking = writer.clone();
            let failures = Arc::clone(&writer_db.checkpoint_failures);
            let refused = tokio::task::spawn_blocking(move || {
                let mut archive = KeptArchive {
                    job_id,
                    history,
                    typed_terminal_cause,
                    failure_kind,
                    committed,
                    of: ArchiveOf::ActiveJob,
                };
                archive_kept(&blocking, &failures, &mut archive, false)
                    .err()
                    .map(|error| (archive, error))
            })
            .await
            .ok()
            .flatten();
            // An archive that commits ends here. One refused for want of
            // room is kept whole and its job named to the listener; one
            // that fails any other way drops its sender, which its waiter
            // reads as a lost archive.
            if let Some((archive, error)) = refused
                && keeps_archive(writer_db.database_target(), &error)
            {
                let job_id = archive.job_id;
                writer_db.kept_archives.list().push_back(archive);
                writer_db.kept_archives.tell(ArchiveNews::Kept(job_id));
            }
        }
        DbWriteCommand::ArchiveTorrent {
            job_id,
            history,
            retires,
            files,
            committed,
            kept,
        } => {
            let archive = TorrentArchive {
                job_id,
                history,
                retires,
                files,
                committed,
                kept,
            };
            supersede_kept_torrent(writer_db, job_id);
            archive_torrent(archive, writer, writer_db).await;
        }
        DbWriteCommand::ArchiveTorrents { archives } => {
            // Each job's kept row is let go before its newer one is
            // written, as one at a time lets it go.
            for archive in &archives {
                supersede_kept_torrent(writer_db, archive.job_id);
            }
            let blocking = writer.clone();
            let failures = Arc::clone(&writer_db.checkpoint_failures);
            let refused = tokio::task::spawn_blocking(move || {
                archive_torrents_at_once(&blocking, &failures, archives)
            })
            .await
            .unwrap_or_default();
            for archive in refused {
                archive_torrent(archive, writer, writer_db).await;
            }
        }
        DbWriteCommand::RetryKeptArchives { from } => {
            retry_kept_archives(from, writer, writer_db).await;
        }
        DbWriteCommand::ArchiveJobs {
            jobs,
            failure_kind,
            committed,
        } => {
            let writer = writer.clone();
            let archived_jobs: Vec<_> = jobs.iter().map(|(job_id, _)| *job_id).collect();
            let failed = tokio::task::spawn_blocking(move || {
                let mut failed = BTreeMap::new();
                for (job_id, history) in jobs {
                    // Taken before each archive read, as for one archive.
                    let observed_generation = writer.job_history_cache_generation();
                    match writer.archive_job_with_terminal_cause(
                        job_id,
                        &history,
                        None,
                        failure_kind,
                    ) {
                        Ok(Some(row)) => writer.cache_job_history_at(row, observed_generation),
                        Ok(None) => {}
                        Err(error) => {
                            tracing::warn!(
                                job_id = job_id.0,
                                error = %error,
                                "failed to archive job on database writer path"
                            );
                            failed.insert(job_id, error.to_string());
                        }
                    }
                }
                failed
            })
            .await;
            if let Ok(failed) = failed {
                writer_db.checkpoint_failures.send_modify(|failures| {
                    for job_id in archived_jobs {
                        if !failed.contains_key(&job_id) {
                            failures.remove(&job_id);
                        }
                    }
                });
                let _ = committed.send(failed);
            }
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
        DbWriteCommand::Write {
            label,
            op,
            committed,
            ..
        } => {
            let writer_db = writer_db.clone();
            tokio::task::spawn_blocking(move || {
                // The waiter is told the kind with the message: a write the
                // disk had no room for is not a write that went wrong.
                let outcome =
                    op(&writer_db).map_err(|error| super::error::WriteFailed::from(&error));
                if let Err(error) = &outcome {
                    tracing::warn!(label, error = %error, "failed to run ordered database write");
                }
                if let Some(committed) = committed {
                    let _ = committed.send(outcome);
                }
            })
            .await
            .ok();
        }
        DbWriteCommand::GraphCheckpoint { job_id, prepare } => {
            let mut write = prepare();
            let blocked = write.blocked;
            let failures = Arc::clone(&writer_db.checkpoint_failures);
            write.blocked = Box::new(move |revision, error| {
                failures.send_modify(|failures| {
                    failures.insert(job_id, error.to_owned());
                });
                blocked(revision, error);
            });
            let result = write
                .run(
                    job_id,
                    writer_db,
                    writer_db.checkpoint_shutdown.subscribe(),
                    permits,
                )
                .await;
            writer_db
                .checkpoint_failures
                .send_modify(|failures| match result {
                    Ok(true) => {
                        failures.remove(&job_id);
                    }
                    Ok(false) => {}
                    Err(error) => {
                        failures.insert(job_id, error);
                    }
                });
        }
        DbWriteCommand::Flush { reply } => {
            let _ = reply.send(());
        }
    }
}

// A newer row for the torrent's job takes the place of one kept: the kept
// one is let go unwritten, so it is never written over the newer one when
// room comes.
fn supersede_kept_torrent(writer_db: &Database, job_id: weaver_server_base::jobs::ids::JobId) {
    let superseded: VecDeque<KeptArchive> = {
        let mut list = writer_db.kept_archives.list();
        let (gone, stay) = std::mem::take(&mut *list).into_iter().partition(|archive| {
            archive.job_id == job_id && matches!(archive.of, ArchiveOf::Torrent { .. })
        });
        *list = stay;
        gone
    };
    for archive in superseded {
        writer_db.kept_archives.tell(ArchiveNews::LetGo {
            job_id: archive.job_id,
            written: false,
        });
    }
}

// One torrent's job's history row in its own transaction: written, or,
// refused for want of room, kept, its job named to the listener before its
// waiter hears it is kept. One lost any other way drops both senders.
async fn archive_torrent(
    archive: TorrentArchive,
    writer: &DatabaseWriterExecutor,
    writer_db: &Database,
) {
    let TorrentArchive {
        job_id,
        history,
        retires,
        files,
        committed,
        kept,
    } = archive;
    let blocking = writer.clone();
    let failures = Arc::clone(&writer_db.checkpoint_failures);
    let refused = tokio::task::spawn_blocking(move || {
        let mut archive = KeptArchive {
            job_id,
            history,
            typed_terminal_cause: None,
            failure_kind: None,
            committed: Some(committed),
            of: ArchiveOf::Torrent { retires, files },
        };
        archive_kept(&blocking, &failures, &mut archive, false)
            .err()
            .map(|error| (archive, error))
    })
    .await
    .ok()
    .flatten();
    if let Some((archive, error)) = refused
        && keeps_archive(writer_db.database_target(), &error)
    {
        // Told before its waiter hears it is kept, so whoever counts
        // the kept archives has counted it by then.
        writer_db.kept_archives.list().push_back(archive);
        writer_db.kept_archives.tell(ArchiveNews::Kept(job_id));
        let _ = kept.send(());
    }
}

// The torrents' jobs' history rows in one transaction, each cached and its
// waiter told once it commits, as `archive_kept` does for one. A refused
// transaction hands every archive back, unwritten, to go on alone. Runs on
// the blocking pool.
fn archive_torrents_at_once(
    writer: &DatabaseWriterExecutor,
    failures: &watch::Sender<BTreeMap<weaver_server_base::jobs::ids::JobId, String>>,
    archives: Vec<TorrentArchive>,
) -> Vec<TorrentArchive> {
    // The test seam refuses archives one at a time: with any refusal
    // waiting, each goes on alone to meet it in its turn, unless the
    // batch's own transaction is to be refused first.
    #[cfg(test)]
    if !ARCHIVE_REFUSED.lock().unwrap().is_empty()
        && !crate::torrents::persistence::HISTORIES_REFUSED
            .load(std::sync::atomic::Ordering::SeqCst)
    {
        return archives;
    }
    let observed_generation = writer.job_history_cache_generation();
    let rows = archives
        .iter()
        .map(|archive| {
            (
                (*archive.history).clone(),
                archive.retires,
                Arc::clone(&archive.files),
            )
        })
        .collect();
    if let Err(error) = writer.write_torrent_histories(rows) {
        tracing::debug!(
            torrents = archives.len(),
            error = %error,
            "a batch's torrent history rows did not write at once; each goes on its own"
        );
        return archives;
    }
    failures.send_modify(|failures| {
        for archive in &archives {
            failures.remove(&archive.job_id);
        }
    });
    for archive in archives {
        writer.cache_job_history_at(*archive.history, observed_generation);
        #[cfg(test)]
        ARCHIVES_WRITTEN.fetch_add(1, Ordering::Relaxed);
        let _ = archive.committed.send(());
    }
    Vec::new()
}

// Archive one job, and say it committed through its `committed` sender.
// Runs on the blocking pool.
fn archive_kept(
    writer: &DatabaseWriterExecutor,
    failures: &watch::Sender<BTreeMap<weaver_server_base::jobs::ids::JobId, String>>,
    archive: &mut KeptArchive,
    retry: bool,
) -> Result<(), StateError> {
    archive_one(writer, archive, retry)?;
    // An archived job no longer needs its cancelled checkpoint. Keep the
    // failure until this transaction actually succeeds.
    failures.send_modify(|failures| {
        failures.remove(&archive.job_id);
    });
    if let Some(committed) = archive.committed.take() {
        let _ = committed.send(());
    }
    Ok(())
}

// One pass over the kept archives: those from place `from` on, at most
// [`KEPT_ARCHIVE_PASS`], each tried once, in order, in one run on the
// blocking pool. One refused again stays kept, in its place, and nothing is
// said of it. One written, or failed for another reason, is let go and its
// job named. The archives of the pass are out of the list while it runs;
// nobody reads the list to count them: the listener counts what it is told.
// When the pass wrote at least one and more remain past it, the listener is
// told where the next pass starts; a pass that wrote none says nothing, and
// the rest wait for the next ring.
async fn retry_kept_archives(from: usize, writer: &DatabaseWriterExecutor, writer_db: &Database) {
    let kept = &writer_db.kept_archives;
    let (from, pass): (usize, Vec<KeptArchive>) = {
        let mut list = kept.list();
        let from = from.min(list.len());
        let to = from.saturating_add(KEPT_ARCHIVE_PASS).min(list.len());
        (from, list.drain(from..to).collect())
    };
    if pass.is_empty() {
        return;
    }
    let writer = writer.clone();
    let target = writer_db.database_target().clone();
    let failures = Arc::clone(&writer_db.checkpoint_failures);
    let in_pass: Vec<_> = pass.iter().map(|archive| archive.job_id).collect();
    let ran = tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        if ARCHIVE_PASS_PANICS.swap(false, Ordering::Relaxed) {
            panic!("the pass over the kept archives panics, as the test asked");
        }
        let mut again = Vec::new();
        let mut let_go = Vec::new();
        for mut archive in pass {
            match archive_kept(&writer, &failures, &mut archive, true) {
                Ok(()) => let_go.push((archive.job_id, true)),
                Err(error) if keeps_archive(&target, &error) => again.push(archive),
                Err(_) => let_go.push((archive.job_id, false)),
            }
        }
        (again, let_go)
    })
    .await;
    // A pass whose thread failed took its archives with it: none is put
    // back, so each is said to be let go, unwritten, and none is counted
    // or shown waiting for a retry that cannot come.
    let (again, let_go) = ran.unwrap_or_else(|error| {
        tracing::error!(
            job_ids = ?in_pass.iter().map(|job_id| job_id.0).collect::<Vec<_>>(),
            %error,
            "the pass over the kept history archives failed; these jobs are archived at the next start"
        );
        let lost = in_pass.iter().map(|job_id| (*job_id, false)).collect();
        (Vec::new(), lost)
    });
    tracing::info!(
        written = let_go.iter().filter(|(_, written)| *written).count(),
        failed = let_go.iter().filter(|(_, written)| !*written).count(),
        still_kept = again.len(),
        "kept history archives were tried again"
    );
    let next = from + again.len();
    let more = {
        let mut list = kept.list();
        let after = list.split_off(from);
        list.extend(again);
        list.extend(after);
        list.len() > next
    };
    let wrote = let_go.iter().any(|(_, written)| *written);
    for (job_id, written) in let_go {
        kept.tell(ArchiveNews::LetGo { job_id, written });
    }
    if more && wrote {
        kept.tell(ArchiveNews::More { from: next });
    }
}

// SQLite shares ordered lanes, but only one SQL command executes at a time.
async fn run_serial_writes(
    rx: mpsc::Receiver<QueuedWrite>,
    writer: DatabaseWriterExecutor,
    writer_db: Database,
) {
    run_postgres_write_lanes(rx, writer, writer_db, 1, SQLITE_WRITE_QUEUE_CAPACITY).await;
}

// Ordered writes a Postgres writer runs at once: half the pool, so queued
// writes never take every connection from reads and synchronous writes.
fn postgres_write_lane_concurrency() -> usize {
    (crate::persistence::sql_services::postgres_max_connections_from_env() as usize / 2).max(1)
}

// Commands the Postgres writer holds taken from the queue per write it runs
// at once. Past that the writer stops taking commands, so the bounded queue
// fills and senders see it full, as on the serial writer.
const POSTGRES_WRITES_TAKEN_PER_LANE_SLOT: usize = 4;

// The ordered lane writer, shared by both datastores. SQL execution is
// limited by `concurrency`, while each lane keeps submission order even
// when its checkpoint is paused between attempts:
//
// - a job's writes apply one after another, in the order they were queued;
// - an archive also waits for every event batch and every server attribution
//   checkpoint queued before it;
// - a barrier (`Flush`, or a write with no lane) waits for everything queued
//   before it and holds back everything queued after it;
// - at most `max_taken` commands are taken and unfinished at once; the rest
//   stay in the queue;
// - when the queue closes, every write already taken is finished first.
async fn run_postgres_write_lanes(
    mut rx: mpsc::Receiver<QueuedWrite>,
    writer: DatabaseWriterExecutor,
    writer_db: Database,
    concurrency: usize,
    max_taken: usize,
) {
    let permits = Arc::new(Semaphore::new(concurrency.max(1)));
    let max_taken = max_taken.max(1);
    let mut in_flight = tokio::task::JoinSet::new();
    // The completion signal of the last command taken on each lane.
    let mut tails: HashMap<WriteLane, watch::Receiver<bool>> = HashMap::new();
    let counts = writer_db.writer_counts.clone();
    loop {
        // Bound ownership of taken commands as well as the incoming queue.
        // A paused checkpoint retains its lane until resumed or cancelled.
        while in_flight.len() >= max_taken {
            in_flight.join_next().await;
        }
        let Some(QueuedWrite { queued_at, command }) = rx.recv().await else {
            break;
        };
        while in_flight.try_join_next().is_some() {}
        prune_finished_lane_tails(&mut tails);
        let Some((also_after, lane)) = command.lanes() else {
            counts.taken.fetch_add(1, Ordering::Release);
            let mut command = command;
            let abandoned = if let DbWriteCommand::Flush { reply } = &mut command {
                loop {
                    tokio::select! {
                        biased;
                        _ = reply.closed() => break true,
                        joined = in_flight.join_next() => {
                            if joined.is_none() { break false; }
                        }
                    }
                }
            } else {
                while in_flight.join_next().await.is_some() {}
                false
            };
            if abandoned {
                continue;
            }
            tails.clear();
            let _executed = writer_command_scope(&command, queued_at);
            execute_write_command(command, &writer, &writer_db, &permits).await;
            continue;
        };
        let waits: Vec<watch::Receiver<bool>> = std::iter::once(&lane)
            .chain(also_after)
            .filter_map(|lane| tails.get(lane).cloned())
            .collect();
        if !waits.is_empty() {
            counts.parked.fetch_add(1, Ordering::Relaxed);
        }
        counts.taken.fetch_add(1, Ordering::Release);
        let (done_tx, done_rx) = watch::channel(false);
        tails.insert(lane, done_rx);
        let permits = permits.clone();
        let writer = writer.clone();
        let writer_db = writer_db.clone();
        in_flight.spawn(async move {
            // A predecessor that panicked drops its sender, which ends the
            // wait just as finishing would.
            for mut wait in waits {
                let _ = wait.wait_for(|done| *done).await;
            }
            // Taken only once the predecessors are done, so a write waiting
            // on its lane never holds a permit a predecessor needs.
            let _executed = writer_command_scope(&command, queued_at);
            execute_write_command(command, &writer, &writer_db, &permits).await;
            let _ = done_tx.send(true);
        });
    }
    while in_flight.join_next().await.is_some() {}
}

// What the ordered writer has taken from its queue so far.
#[derive(Default)]
struct WriterCounts {
    // Every command taken.
    taken: AtomicU64,
    // The lane writes among them taken while a write before them on one of
    // their lanes was still unfinished, so they waited for it.
    parked: AtomicU64,
}

// Forget the lanes whose last taken write has finished, so the table holds
// only lanes with unfinished work instead of every job the writer has seen.
// A finished tail has nothing left to wait for, and a write on a lane with no
// tail waits for nothing, so dropping it changes no ordering. A write that
// panicked dropped its sender, which ends its wait just as finishing does.
fn prune_finished_lane_tails(tails: &mut HashMap<WriteLane, watch::Receiver<bool>>) {
    tails.retain(|_, done| !*done.borrow() && done.has_changed().is_ok());
}

impl DbWriteCommand {
    // Stable bucket label. `Write` carries a caller-supplied `&'static str`, so
    // generic ordered writes stay distinguishable from each other.
    fn label(&self) -> &'static str {
        match self {
            Self::ArchiveJob { .. } => "archive_job",
            Self::ArchiveTorrent { .. } => "archive_torrent",
            Self::ArchiveTorrents { .. } => "archive_torrents",
            Self::ArchiveJobs { .. } => "archive_jobs",
            Self::InsertJobEvents { .. } => "insert_job_events",
            Self::Write { label, .. } => label,
            Self::GraphCheckpoint { .. } => "save_graph_checkpoint",
            Self::Flush { .. } => "flush",
            Self::RetryKeptArchives { .. } => "retry_kept_archives",
        }
    }
}

// SQL-backed persistent store for config, servers, and job history.
#[derive(Clone)]
pub struct Database {
    connection: SqlDatabase,
    #[cfg(test)]
    pub(crate) graph_checkpoint_writes: Arc<AtomicUsize>,
    // How many times the rows a torrent's restore and attach read were
    // read: the torrents', their trackers', counters' and history's.
    #[cfg(test)]
    pub(crate) torrent_reads: Arc<AtomicUsize>,
    // How many times a consumer's routing policy was read from its row.
    #[cfg(test)]
    pub(crate) proxy_route_reads: Arc<AtomicUsize>,
    // How many transactions wrote torrents' jobs' history rows.
    #[cfg(any(test, feature = "test-support"))]
    torrent_history_writes: Arc<AtomicUsize>,
    pub(crate) rss_schedule_cache: Arc<crate::rss::ScheduleCache>,
    pub(crate) script_runtime: Arc<crate::post_processing::events::ScriptRuntime>,
    writer_tx: mpsc::Sender<QueuedWrite>,
    checkpoint_shutdown: watch::Sender<bool>,
    checkpoint_failures: Arc<watch::Sender<BTreeMap<weaver_server_base::jobs::ids::JobId, String>>>,
    kept_archives: Arc<KeptArchives>,
    // Count of in-flight background re-sends into the bounded writer queue
    // (archive jobs and generic ordered writes) spawned when `try_send` hit a
    // full queue. `flush_write_queue` waits for this to reach zero before its
    // own `Flush` so a re-send in progress is not skipped at shutdown.
    pending_write_retries: Arc<AtomicUsize>,
    pending_write_notify: Arc<Notify>,
    writer_counts: Arc<WriterCounts>,
    // The writes waiting, in the order they were queued, for room in the
    // full writer queue: some while the re-send empties them into it.
    write_overflow: Arc<Mutex<Option<VecDeque<QueuedWrite>>>>,
    pub(crate) history_delete_wake: Arc<Notify>,
    pub(crate) job_history_cache: Arc<Mutex<JobHistoryCache>>,
    // Every tag and each job's tags, as last read, and their revision.
    pub(crate) tags: Arc<crate::tags::TagCache>,
}

impl std::ops::Deref for Database {
    type Target = SqlDatabase;
    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl Database {
    pub(crate) fn close(self) -> Result<(), StateError> {
        let connection = self.connection.clone();
        drop(self);
        connection.close()
    }

    pub fn set_encryption_key(&mut self, key: crate::persistence::encryption::EncryptionKey) {
        self.connection.set_encryption_key(key);
    }

    // Open (or create) the database at `path`.
    // Runs schema migrations and configures SQLite pragmas.
    pub fn open(path: &Path) -> Result<Self, StateError> {
        Self::open_target(DatabaseTarget::SqlitePath(path.to_path_buf()))
    }

    pub(crate) fn open_target(target: DatabaseTarget) -> Result<Self, StateError> {
        SqlDatabase::open_target(target, crate::schema_migrations::MigrationMode::Apply)
            .map(Self::with_writer)
    }

    pub fn open_in_memory() -> Result<Self, StateError> {
        SqlDatabase::open_in_memory(crate::schema_migrations::MigrationMode::Apply)
            .map(Self::with_writer)
    }

    fn with_writer(connection: SqlDatabase) -> Self {
        let (writer_tx, writer_rx) = mpsc::channel(SQLITE_WRITE_QUEUE_CAPACITY);
        let db = Self {
            connection,
            rss_schedule_cache: Arc::new(crate::rss::ScheduleCache::default()),
            #[cfg(test)]
            graph_checkpoint_writes: Arc::default(),
            #[cfg(test)]
            torrent_reads: Arc::default(),
            #[cfg(test)]
            proxy_route_reads: Arc::default(),
            #[cfg(any(test, feature = "test-support"))]
            torrent_history_writes: Arc::default(),
            script_runtime: Arc::new(crate::post_processing::events::ScriptRuntime::default()),
            writer_tx,
            checkpoint_shutdown: watch::channel(false).0,
            checkpoint_failures: Arc::new(watch::channel(BTreeMap::new()).0),
            kept_archives: KeptArchives::new(),
            pending_write_retries: Arc::new(AtomicUsize::new(0)),
            pending_write_notify: Arc::new(Notify::new()),
            writer_counts: Arc::default(),
            write_overflow: Arc::new(Mutex::new(None)),
            history_delete_wake: Arc::new(Notify::new()),
            job_history_cache: Arc::new(Mutex::new(JobHistoryCache::default())),
            tags: Arc::default(),
        };
        db.spawn_writer_task(writer_rx);
        db
    }

    /// How many transactions wrote torrents' jobs' history rows.
    #[cfg(any(test, feature = "test-support"))]
    pub fn torrent_history_writes(&self) -> usize {
        self.torrent_history_writes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    // Check if the database has no settings (i.e. fresh / needs migration).
    //
    // The install-generation stamp and the last-started-version marker are
    // written the moment a new database is created, before any configuration
    // is imported into it, so they do not make the database hold settings of
    // its own.
    pub fn is_empty(&self) -> Result<bool, StateError> {
        use crate::persistence::sql_runtime::SqlArg;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let count = crate::persistence::sql_runtime::SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT COUNT(*) AS count FROM settings WHERE key NOT IN ({}, {})",
                &[
                    SqlArg::Text(
                        weaver_server_base::security::SETTING_INSTALL_GENERATION.to_string(),
                    ),
                    SqlArg::Text(crate::operations::backup::LAST_VERSION.to_string()),
                ],
            )
            .await?
            .map(|row| row.i64("count"))
            .transpose()?
            .unwrap_or(0);
            Ok(count == 0)
        })
    }

    // Build a `Database` handle for the writer task to run generic `Write`
    // ops against. It shares the sql services/worker/cache but carries a
    // *detached* `writer_tx` (a throwaway channel whose receiver is dropped),
    // so it does NOT hold the real writer channel open. That keeps the
    // all-senders-dropped shutdown contract intact — the real queue still
    // closes when every external `Database` handle drops — while letting
    // `Write` ops call methods that live only on `Database`
    // (`set_active_job_runtime`, `update_active_job`), which touch only
    // `datastore()` / `run_sql_blocking()` and never `writer_tx`.
    fn writer_task_handle(&self) -> Database {
        let (detached_tx, _detached_rx) = mpsc::channel(1);
        Database {
            connection: self.connection.clone(),
            #[cfg(test)]
            graph_checkpoint_writes: self.graph_checkpoint_writes.clone(),
            #[cfg(test)]
            torrent_reads: self.torrent_reads.clone(),
            #[cfg(test)]
            proxy_route_reads: self.proxy_route_reads.clone(),
            #[cfg(any(test, feature = "test-support"))]
            torrent_history_writes: self.torrent_history_writes.clone(),
            writer_tx: detached_tx,
            checkpoint_shutdown: self.checkpoint_shutdown.clone(),
            checkpoint_failures: self.checkpoint_failures.clone(),
            kept_archives: self.kept_archives.clone(),
            pending_write_retries: self.pending_write_retries.clone(),
            pending_write_notify: self.pending_write_notify.clone(),
            writer_counts: self.writer_counts.clone(),
            write_overflow: Arc::new(Mutex::new(None)),
            history_delete_wake: self.history_delete_wake.clone(),
            job_history_cache: self.job_history_cache.clone(),
            tags: self.tags.clone(),
            script_runtime: self.script_runtime.clone(),
            rss_schedule_cache: self.rss_schedule_cache.clone(),
        }
    }

    fn spawn_writer_task(&self, rx: mpsc::Receiver<QueuedWrite>) {
        let writer = DatabaseWriterExecutor::from_database(self);
        // Handle used only to execute generic `Write` ops; carries a detached
        // sender so it never keeps the real writer channel open (see above).
        let writer_db = self.writer_task_handle();
        let worker: Pin<Box<dyn Future<Output = ()> + Send>> =
            if matches!(self.database_target(), DatabaseTarget::PostgresUrl(_)) {
                Box::pin({
                    let concurrency = postgres_write_lane_concurrency();
                    run_postgres_write_lanes(
                        rx,
                        writer,
                        writer_db,
                        concurrency,
                        concurrency * POSTGRES_WRITES_TAKEN_PER_LANE_SLOT,
                    )
                })
            } else {
                Box::pin(run_serial_writes(rx, writer, writer_db))
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
        job_id: weaver_server_base::jobs::ids::JobId,
        history: crate::history::JobHistoryRow,
    ) -> Result<(), StateError> {
        let command = DbWriteCommand::ArchiveJob {
            job_id,
            history: Box::new(history),
            typed_terminal_cause: None,
            failure_kind: None,
            committed: None,
        };
        self.try_send_with_retry(command, "archive")
    }

    // Queue the durable history archive and hand back a signal that fires once
    // that write has committed. Terminal job events are published off this
    // signal, so a subscriber that observes one can immediately read the row
    // back through the history facade instead of racing the write queue.
    pub fn try_queue_archive_job_with_terminal_cause(
        &self,
        job_id: weaver_server_base::jobs::ids::JobId,
        history: crate::history::JobHistoryRow,
        typed_terminal_cause: Option<crate::jobs::SemanticTerminalCause>,
        failure_kind: Option<crate::history::HistoryFailureKind>,
    ) -> Result<oneshot::Receiver<()>, StateError> {
        let (committed_tx, committed_rx) = oneshot::channel();
        self.try_send_with_retry(
            DbWriteCommand::ArchiveJob {
                job_id,
                history: Box::new(history),
                typed_terminal_cause,
                failure_kind,
                committed: Some(committed_tx),
            },
            "archive",
        )?;
        Ok(committed_rx)
    }

    // Queue a torrent's job's history row through the writer's archives:
    // written in its turn, or, refused for want of room, kept and tried
    // again as an NZB job's archive is. `retires` removes the torrent in the
    // same transaction. The first receiver fires once the row is written;
    // the second once it is kept. A row lost any other way drops both.
    pub(crate) fn queue_torrent_history(
        &self,
        job_id: weaver_server_base::jobs::ids::JobId,
        history: crate::history::JobHistoryRow,
        retires: bool,
        files: Vec<crate::torrents::persistence::HistoryFile>,
    ) -> Result<(oneshot::Receiver<()>, oneshot::Receiver<()>), StateError> {
        let (committed, written) = oneshot::channel();
        let (kept, held) = oneshot::channel();
        self.try_send_with_retry(
            DbWriteCommand::ArchiveTorrent {
                job_id,
                history: Box::new(history),
                retires,
                files: files.into(),
                committed,
                kept,
            },
            "archive",
        )?;
        Ok((written, held))
    }

    // `queue_torrent_history` of each of `rows`, of distinct jobs, as one
    // ordered write and, while the database takes them, one transaction:
    // each row's receivers fire as that one's do.
    pub(crate) fn queue_torrent_histories(
        &self,
        rows: Vec<(
            weaver_server_base::jobs::ids::JobId,
            crate::history::JobHistoryRow,
            bool,
            Vec<crate::torrents::persistence::HistoryFile>,
        )>,
    ) -> Result<Vec<HistoryWaits>, StateError> {
        let mut archives = Vec::with_capacity(rows.len());
        let mut waits = Vec::with_capacity(rows.len());
        for (job_id, history, retires, files) in rows {
            let (committed, written) = oneshot::channel();
            let (kept, held) = oneshot::channel();
            archives.push(TorrentArchive {
                job_id,
                history: Box::new(history),
                retires,
                files: files.into(),
                committed,
                kept,
            });
            waits.push((written, held));
        }
        self.try_send_with_retry(DbWriteCommand::ArchiveTorrents { archives }, "archive")?;
        Ok(waits)
    }

    // Name the one listener the writer tells of the archives it keeps. It is
    // called on the writer's task and must not wait.
    pub(crate) fn hear_archive_news(&self, sink: impl Fn(ArchiveNews) + Send + Sync + 'static) {
        *self
            .kept_archives
            .news
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(sink));
    }

    // The jobs whose archive is kept for want of room, in the order kept:
    // for the shutdown warning and for tests. An archive a pass is trying
    // is not among them while the pass runs.
    pub(crate) fn kept_archive_jobs(&self) -> Vec<weaver_server_base::jobs::ids::JobId> {
        let list = self.kept_archives.list();
        list.iter().map(|kept| kept.job_id).collect()
    }

    // Queue one pass over the kept archives from place `from` on, as one
    // ordered write behind the writes already waiting. The caller counts
    // the kept archives and calls this only while it counts any.
    pub(crate) fn retry_kept_archives(&self, from: usize) {
        if let Err(error) =
            self.try_send_with_retry(DbWriteCommand::RetryKeptArchives { from }, "archive")
        {
            tracing::warn!(error = %error, "failed to queue the kept history archives");
        }
    }

    // Queue several jobs' history archives as one ordered write, and hand
    // back a signal that fires once every archive has run, naming the jobs
    // whose archive failed. A dropped signal means the writer closed first.
    pub(crate) fn try_queue_archive_jobs(
        &self,
        jobs: Vec<(
            weaver_server_base::jobs::ids::JobId,
            crate::history::JobHistoryRow,
        )>,
        failure_kind: Option<crate::history::HistoryFailureKind>,
    ) -> Result<oneshot::Receiver<BTreeMap<weaver_server_base::jobs::ids::JobId, String>>, StateError>
    {
        let (committed_tx, committed_rx) = oneshot::channel();
        self.try_send_with_retry(
            DbWriteCommand::ArchiveJobs {
                jobs,
                failure_kind,
                committed: committed_tx,
            },
            "archive",
        )?;
        Ok(committed_rx)
    }

    // Queue a generic ordered write. It runs after every write queued before
    // it and before every write queued after it, so two writes to the same
    // row land in enqueue order — closing the same-key reorder hazard that
    // detached `db_fire_and_forget` tasks have. Backpressure matches
    // [`Self::try_queue_archive_job`]: on a full queue, a background re-send is
    // spawned and tracked by `pending_write_retries` so `flush_write_queue`
    // waits for it. `op` runs against a `Database` handle inside the writer
    // task; use it to call methods that live on `Database`.
    pub fn try_queue_write(
        &self,
        label: &'static str,
        op: impl FnOnce(&Database) -> Result<(), StateError> + Send + 'static,
    ) -> Result<(), StateError> {
        let command = DbWriteCommand::Write {
            label,
            lane: None,
            op: Box::new(op),
            committed: None,
        };
        self.try_send_with_retry(command, label)
    }

    // [`Self::try_queue_write`] for a write that belongs to one job. It keeps
    // its order against that job's other queued writes and its archive.
    // Other jobs can advance while this job's checkpoint awaits resume.
    pub fn try_queue_job_write(
        &self,
        job_id: weaver_server_base::jobs::ids::JobId,
        label: &'static str,
        op: impl FnOnce(&Database) -> Result<(), StateError> + Send + 'static,
    ) -> Result<(), StateError> {
        self.queue_job_write(job_id, label, op, None)
    }

    // [`Self::try_queue_job_write`] whose receiver answers with the write's
    // outcome once it has run. A write that fails, or never runs because the
    // lane closed, answers with an error or not at all.
    pub(crate) fn try_queue_job_write_committed(
        &self,
        job_id: weaver_server_base::jobs::ids::JobId,
        label: &'static str,
        op: impl FnOnce(&Database) -> Result<(), StateError> + Send + 'static,
    ) -> Result<oneshot::Receiver<Result<(), super::error::WriteFailed>>, StateError> {
        let (committed, rx) = oneshot::channel();
        self.queue_job_write(job_id, label, op, Some(committed))?;
        Ok(rx)
    }

    fn queue_job_write(
        &self,
        job_id: weaver_server_base::jobs::ids::JobId,
        label: &'static str,
        op: impl FnOnce(&Database) -> Result<(), StateError> + Send + 'static,
        committed: Option<oneshot::Sender<Result<(), super::error::WriteFailed>>>,
    ) -> Result<(), StateError> {
        let command = DbWriteCommand::Write {
            label,
            lane: Some(WriteLane::Job(job_id.0)),
            op: Box::new(op),
            committed,
        };
        self.try_send_with_retry(command, label)
    }

    // A marker at the end of the job's lane: it answers once every write
    // queued for the job before it has run, each to its commit, as writes
    // on one lane run in order. Queue the markers of several jobs before
    // awaiting any, and their lanes are passed once.
    pub(crate) fn queue_job_writes_marker(
        &self,
        job_id: weaver_server_base::jobs::ids::JobId,
    ) -> Result<oneshot::Receiver<()>, StateError> {
        let (reached, rx) = oneshot::channel();
        self.try_queue_job_write(job_id, "job_writes_committed", move |_| {
            let _ = reached.send(());
            Ok(())
        })?;
        #[cfg(test)]
        JOB_WRITE_WAITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(rx)
    }

    pub(crate) fn try_queue_graph_checkpoint(
        &self,
        job_id: weaver_server_base::jobs::ids::JobId,
        prepare: impl FnOnce() -> super::checkpoint_write::CheckpointWrite + Send + 'static,
    ) -> Result<(), StateError> {
        self.try_send_with_retry(
            DbWriteCommand::GraphCheckpoint {
                job_id,
                prepare: Box::new(prepare),
            },
            "save_graph_checkpoint",
        )
    }

    // A job outside the pipeline let its blocked checkpoint go: the failure
    // no longer stands in a flush's way. Run on the job's lane behind that
    // checkpoint, so it comes after the write that recorded the failure.
    pub(crate) fn forget_checkpoint_failure(&self, job_id: weaver_server_base::jobs::ids::JobId) {
        self.checkpoint_failures.send_modify(|failures| {
            failures.remove(&job_id);
        });
    }

    // Stop retry waits at shutdown without claiming any unwritten delta
    // committed. A database call already running is still awaited.
    pub(crate) fn stop_checkpoint_retries(&self) {
        self.checkpoint_shutdown.send_replace(true);
    }

    // Queue a server attribution or support facts checkpoint. Checkpoints keep their order
    // against each other, and a job's archive waits for the ones queued
    // before it; on Postgres they otherwise run alongside other writes
    // instead of holding every lane back.
    pub fn try_queue_server_attribution_write(
        &self,
        op: impl FnOnce(&Database) -> Result<(), StateError> + Send + 'static,
    ) -> Result<(), StateError> {
        const LABEL: &str = "active_server_attribution";
        let command = DbWriteCommand::Write {
            label: LABEL,
            lane: Some(WriteLane::ServerAttribution),
            op: Box::new(op),
            committed: None,
        };
        self.try_send_with_retry(command, LABEL)
    }

    // Shared enqueue path for ordered writer commands. A write goes straight
    // into the queue only when no write waits ahead of it. Once the queue is
    // full, writes wait in an overflow line that one sender empties into it
    // in order, and a write queued while that line is being emptied joins
    // its tail, even when the queue has room again: no write overtakes one
    // queued before it. On the runtime the sender is a background task; off
    // it, the caller's own thread, which waits for room with the line's lock
    // released, so a runtime worker queueing meanwhile joins the line and
    // never waits. Either sender is tracked by
    // `pending_write_retries`/`pending_write_notify`, so a flush cannot race
    // past it.
    fn try_send_with_retry(
        &self,
        command: DbWriteCommand,
        context: &'static str,
    ) -> Result<(), StateError> {
        let command = self.enqueue_write(command);
        let mut overflow = self
            .write_overflow
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(waiting) = overflow.as_mut() {
            waiting.push_back(command);
            return Ok(());
        }
        let command = match self.writer_tx.try_send(command) {
            Ok(()) => return Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(writer_queue_closed());
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(command)) => command,
        };
        *overflow = Some(VecDeque::from([command]));
        drop(overflow);
        self.pending_write_retries.fetch_add(1, Ordering::AcqRel);
        let tx = self.writer_tx.clone();
        let overflow = Arc::clone(&self.write_overflow);
        let pending_write_retries = self.pending_write_retries.clone();
        let pending_write_notify = self.pending_write_notify.clone();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            let mut sent = Ok(());
            while let Some(next) = next_in_line(&overflow) {
                if tx.blocking_send(next).is_err() {
                    close_line(&overflow, context);
                    sent = Err(writer_queue_closed());
                    break;
                }
            }
            pending_write_retries.fetch_sub(1, Ordering::AcqRel);
            pending_write_notify.notify_waiters();
            return sent;
        };
        handle.spawn(async move {
            while let Some(next) = next_in_line(&overflow) {
                if tx.send(next).await.is_err() {
                    close_line(&overflow, context);
                    break;
                }
            }
            pending_write_retries.fetch_sub(1, Ordering::AcqRel);
            pending_write_notify.notify_waiters();
        });
        Ok(())
    }

    // How many commands wait in the ordered writer queue, the one it is
    // running not counted. Derived from the channel rather than a separate
    // counter so it cannot drift from reality.
    pub(crate) fn queued_writes(&self) -> usize {
        self.writer_tx
            .max_capacity()
            .saturating_sub(self.writer_tx.capacity())
    }

    // How many commands the ordered writer has taken from its queue. A
    // write's `parked` count is settled before its `taken` count shows it.
    #[cfg(test)]
    pub(crate) fn writes_taken(&self) -> u64 {
        self.writer_counts.taken.load(Ordering::Acquire)
    }

    // How many lane writes the ordered writer took while a write before them
    // on their lane was unfinished.
    #[cfg(test)]
    pub(crate) fn writes_parked(&self) -> u64 {
        self.writer_counts.parked.load(Ordering::Relaxed)
    }

    // Stamp a command with its enqueue instant and sample how full the ordered
    // writer queue already is.
    fn enqueue_write(&self, command: DbWriteCommand) -> QueuedWrite {
        let depth = self.queued_writes();
        weaver_server_base::runtime::perf_probe::record_value(
            "db.writer_queue.depth_at_submit",
            depth as u64,
        );
        QueuedWrite {
            queued_at: Instant::now(),
            command,
        }
    }

    // Queued through the same line as every other write, so it never
    // overtakes a write waiting for room in the full queue. It does not wait
    // for room itself.
    pub async fn queue_archive_job(
        &self,
        job_id: weaver_server_base::jobs::ids::JobId,
        history: crate::history::JobHistoryRow,
    ) -> Result<(), StateError> {
        self.try_queue_archive_job(job_id, history)
    }

    // Queued through the same line as every other write, as
    // [`Self::queue_archive_job`] is.
    pub async fn queue_job_events(
        &self,
        events: Vec<crate::history::JobEvent>,
    ) -> Result<(), StateError> {
        self.try_send_with_retry(DbWriteCommand::InsertJobEvents { events }, "job_events")
    }

    pub async fn flush_write_queue(&self) -> Result<(), StateError> {
        // Subscribe before queuing the barrier. A checkpoint that needs
        // explicit resume retains its ordered slot, but a flush must report
        // that unresolved failure instead of waiting behind it forever.
        let mut failures = self.checkpoint_failures.subscribe();
        let flush = async {
            // Drain any in-flight background re-sends (archive jobs and generic
            // writes) into the queue first, so the `Flush` we enqueue below sits
            // behind every write that had already been accepted for the queue.
            self.wait_for_pending_write_retries().await;
            let (reply, rx) = oneshot::channel();
            let command = self.enqueue_write(DbWriteCommand::Flush { reply });
            self.writer_tx
                .send(command)
                .await
                .map_err(|_| StateError::Database("database writer queue closed".to_string()))?;
            rx.await
                .map_err(|_| StateError::Database("database writer flush failed".to_string()))?;
            Ok::<_, StateError>(())
        };
        tokio::pin!(flush);
        let mut flushed = false;
        loop {
            {
                let unresolved = failures.borrow_and_update();
                if !unresolved.is_empty() {
                    return Err(StateError::Database(format!(
                        "unresolved graph checkpoint writes: {}",
                        unresolved
                            .iter()
                            .map(|(job, error)| format!("job {}: {error}", job.0))
                            .collect::<Vec<_>>()
                            .join("; ")
                    )));
                }
            }
            if flushed {
                return Ok(());
            }
            tokio::select! {
                result = &mut flush => { result?; flushed = true; }
                changed = failures.changed() => {
                    changed.map_err(|_| StateError::Database("checkpoint failure reporting closed".into()))?;
                }
            }
        }
    }

    async fn wait_for_pending_write_retries(&self) {
        loop {
            if self.pending_write_retries.load(Ordering::Acquire) == 0 {
                return;
            }
            let notified = self.pending_write_notify.notified();
            if self.pending_write_retries.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

fn writer_queue_closed() -> StateError {
    StateError::Database("database writer queue closed".to_string())
}

// The overflow line's next write, taken under its lock and sent with the
// lock released. The line stays open while its head is sent, so a write
// queued meanwhile waits behind it; it closes in the same turn it is found
// empty, so a write queued after that goes straight to the queue.
fn next_in_line(overflow: &Mutex<Option<VecDeque<QueuedWrite>>>) -> Option<QueuedWrite> {
    let mut line = overflow
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let next = line.as_mut().and_then(VecDeque::pop_front);
    if next.is_none() {
        *line = None;
    }
    next
}

// No writer is left to take what still waits in the overflow line.
fn close_line(overflow: &Mutex<Option<VecDeque<QueuedWrite>>>, context: &'static str) {
    tracing::warn!(
        context,
        "database writer queue closed while retrying enqueue"
    );
    *overflow
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
}

// Each wait for a job's queued writes, counted for the tests.
#[cfg(test)]
pub(crate) static JOB_WRITE_WAITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) mod tests;
