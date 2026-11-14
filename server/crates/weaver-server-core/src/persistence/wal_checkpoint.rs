// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The WAL is checkpointed by one task per database, on its own connection,
// never by the commit that crossed the threshold. SQLite's autocheckpoint
// runs inside the committing statement, so every write behind it in the
// writer gate waited for a copy of up to the whole WAL into the database
// file, with its syncs; on network storage that is seconds. Pool
// connections run with `wal_autocheckpoint = 0` and report the WAL's frame
// count after each commit instead. A commit that leaves a threshold's worth
// of frames past what the last checkpoint saw wakes the task; the task
// itself never takes the gate.
//
// Writes do not wait for that checkpoint. A pass copies pages into the
// database file and syncs it, and on a slow or busy disk that sync takes
// seconds: a write that waited for it held the gate, and every write behind
// it, that long. Writes append to the WAL while the pass runs. A pass that a
// commit overlapped copies what it saw, but the WAL cannot restart until a
// pass has copied every frame, so under steady writes it keeps growing.
// Once a commit that asks for a pass finds the WAL at `WRITE_WAIT_THRESHOLDS`
// thresholds or more, the next write waits, holding the gate, until a pass
// has copied every frame in the WAL. The pass already running does that when
// it read the WAL after the last commit, and the write is released when it
// ends. Otherwise that pass read the WAL before commits it overlapped, which
// it cannot copy, and the write also waits for the next pass. That pass has
// no commit during it and copies only the frames left: the ones written while
// the first pass ran. No shorter wait is sound, since SQLite restarts the WAL
// only once a pass has copied every frame, and only a pass with no commit
// during it can.
//
// Once every frame is copied, the waiting write restarts the WAL, unless a
// read is still open on it with a read mark past 0: SQLite cannot restart
// under such a read, and the write after the read ends does. With no read
// open on an older snapshot the WAL holds at most the wait bound, one
// threshold and one transaction.
//
// A read that is still open keeps the frames
// committed after its snapshot from being copied (one holding read-mark 0
// keeps all of them), and SQLite cannot restart the WAL while any read is
// open on it, so the WAL then grows by every write made while that read
// lasts. Those writes ask for one checkpoint per threshold of new frames, not
// one each. After the read ends, the next request copies the backlog and the
// write after that checkpoint restarts the WAL.

use std::ffi::{c_char, c_int, c_void};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Connection};
use tokio::sync::{Notify, watch};

use crate::operations::instrumentation::{
    AtomicHistogram, DB_OP_DURATION_BOUNDS, HistogramSnapshot,
};
use crate::persistence::StateError;

// SQLite's own default is 1000 frames; 4000 (16 MB at 4 KiB pages) turns
// many small commits into fewer, longer checkpoint writes.
pub(crate) const WAL_CHECKPOINT_FRAMES: u32 = 4000;

// How many thresholds of frames a WAL reaches before a write that finds a
// pass asked for waits for one. The WAL then peaks at this bound, one more
// threshold and one transaction: 12,000 frames of 4,120 bytes (a 4 KiB page
// and its frame header) is about 49 MB, so the peak stays under the 64 MiB
// `journal_size_limit` for any transaction under about 17 MB, and the file
// is reused rather than grown and trimmed each cycle.
pub(crate) const WRITE_WAIT_THRESHOLDS: u32 = 2;

// Statement times come from SQLite's profile trace, which takes the
// difference of two millisecond wall-clock readings: a statement under a
// millisecond reads as 0 or 1 ms, so no bucket finer than a millisecond can
// tell anything apart.
const STATEMENT_DURATION_BOUNDS: &[f64] =
    &[0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0];

unsafe extern "C" {
    // Part of the SQLite library sqlx links; replaces the connection's
    // autocheckpoint hook. Declared here because sqlx exposes the raw handle
    // but not this call.
    fn sqlite3_wal_hook(
        db: *mut c_void,
        callback: Option<
            unsafe extern "C" fn(*mut c_void, *mut c_void, *const c_char, c_int) -> c_int,
        >,
        arg: *mut c_void,
    ) -> *mut c_void;

    // Also part of that library; sqlx installs no trace callback of its own.
    fn sqlite3_trace_v2(
        db: *mut c_void,
        mask: u32,
        callback: Option<unsafe extern "C" fn(u32, *mut c_void, *mut c_void, *mut c_void) -> c_int>,
        context: *mut c_void,
    ) -> c_int;
}

// `SQLITE_TRACE_PROFILE`: called as each statement finishes, with the
// nanoseconds it ran.
const SQLITE_TRACE_PROFILE: u32 = 0x02;

// A pass at or past this is logged.
const SLOW_PASS: Duration = Duration::from_secs(1);

// What the checkpoint task's passes did, written only by the task, once per
// pass. The frame counts are from the latest pass.
struct WalCheckpointMetrics {
    passes: AtomicU64,
    // Passes that ended with frames in the WAL they could not copy.
    passes_left_behind: AtomicU64,
    // Passes SQLite reported as busy.
    busy_passes: AtomicU64,
    duration: AtomicHistogram,
    log_frames: AtomicU64,
    checkpointed_frames: AtomicU64,
    // Frames this pass copied: its checkpointed count less the previous
    // pass's, when the WAL has not restarted between them.
    copied_frames: AtomicU64,
    // The WAL file's length after the pass. It does not shrink when the WAL
    // restarts; `journal_size_limit` trims it.
    wal_bytes: AtomicU64,
}

impl WalCheckpointMetrics {
    const fn new() -> Self {
        Self {
            passes: AtomicU64::new(0),
            passes_left_behind: AtomicU64::new(0),
            busy_passes: AtomicU64::new(0),
            duration: AtomicHistogram::new(DB_OP_DURATION_BOUNDS),
            log_frames: AtomicU64::new(0),
            checkpointed_frames: AtomicU64::new(0),
            copied_frames: AtomicU64::new(0),
            wal_bytes: AtomicU64::new(0),
        }
    }
}

// Read at scrape time: atomic loads plus one `Vec` for the histogram.
#[derive(Debug, Clone, PartialEq)]
pub struct WalCheckpointMetricsSnapshot {
    pub passes: u64,
    pub passes_left_behind: u64,
    pub busy_passes: u64,
    pub restarts: u64,
    pub duration: HistogramSnapshot,
    pub log_frames: u64,
    pub checkpointed_frames: u64,
    pub copied_frames: u64,
    pub wal_bytes: u64,
    // Every statement on a pool connection, reads and writes alike.
    pub statement_duration: HistogramSnapshot,
}

// What commits tell the checkpoint task, and what the task tells writes.
pub(crate) struct WalSignal {
    threshold: u32,
    // WAL frames after the latest commit on any pool connection.
    frames: AtomicU32,
    // Commits that found the WAL's frame count lower than the commit before,
    // which happens only when SQLite restarted the WAL under that write.
    restarts: AtomicU64,
    // The WAL frame count the latest checkpoint saw, in the low 32 bits,
    // tagged in the high 32 bits with `restarts` as it stood before that
    // checkpoint. A count from before a restart is not compared with frames
    // after it.
    last_pass: AtomicU64,
    // Commits that left a threshold of frames past the latest checkpoint.
    requested: AtomicU64,
    // The `requested` count of the latest commit that asked for a checkpoint
    // with the WAL at or past the write-wait bound. Writes wait for a
    // checkpoint started after it, or one that copied every frame.
    write_waits_for: AtomicU64,
    // The highest `write_waits_for` a waiting write saw met by a checkpoint
    // that copied every frame in the WAL.
    write_wait_met: AtomicU64,
    // The frames the latest checkpoint copied, tagged as `last_pass` is.
    backfilled: AtomicU64,
    // The highest `requested` a finished checkpoint started after.
    completed: watch::Sender<u64>,
    wake: Notify,
    stopping: AtomicBool,
    metrics: WalCheckpointMetrics,
    // How long each statement on a pool connection ran, as SQLite times it
    // from its first step to its reset: the statements the slow-statement
    // log reports, and the ones inside a transaction, its commit included.
    statements: AtomicHistogram,
    // A test holds the task back to see a commit cross the threshold with
    // no checkpoint anywhere.
    #[cfg(test)]
    held: watch::Sender<bool>,
    #[cfg(test)]
    runs: watch::Sender<u64>,
    // A test holds a pass after it has taken its target and before it reads
    // the WAL, as a pass that has begun but not yet copied.
    #[cfg(test)]
    pass_held: watch::Sender<bool>,
    #[cfg(test)]
    started: watch::Sender<u64>,
}

impl WalSignal {
    // Leaked on purpose: SQLite keeps a raw pointer to it in every pool
    // connection, and a connection can outlive the services that opened it
    // while the pool closes. One small struct per database opened.
    fn leak(threshold: u32) -> &'static Self {
        Box::leak(Box::new(Self {
            threshold,
            frames: AtomicU32::new(0),
            restarts: AtomicU64::new(0),
            last_pass: AtomicU64::new(0),
            requested: AtomicU64::new(0),
            write_waits_for: AtomicU64::new(0),
            write_wait_met: AtomicU64::new(0),
            backfilled: AtomicU64::new(0),
            completed: watch::channel(0).0,
            wake: Notify::new(),
            stopping: AtomicBool::new(false),
            metrics: WalCheckpointMetrics::new(),
            statements: AtomicHistogram::new(STATEMENT_DURATION_BOUNDS),
            #[cfg(test)]
            held: watch::channel(false).0,
            #[cfg(test)]
            runs: watch::channel(0).0,
            #[cfg(test)]
            pass_held: watch::channel(false).0,
            #[cfg(test)]
            started: watch::channel(0).0,
        }))
    }

    // Called by SQLite on the committing connection, after the commit and
    // outside its write lock. Never skipped: a wake while the task is busy
    // stays stored, and every commit at or past the threshold of frames the
    // latest checkpoint did not see raises `requested`. With no read
    // pinning the WAL that checkpoint copied everything and the WAL has
    // restarted since, so this is the plain frame count against the
    // threshold. With a read pinning it, the checkpoint saw frames it could
    // not copy; counting from them asks again only once another threshold of
    // frames has been written, instead of on every commit.
    fn committed(&self, frames: u32) {
        let previous = self.frames.swap(frames, Ordering::AcqRel);
        let restarts = if frames < previous {
            self.restarts.fetch_add(1, Ordering::AcqRel) + 1
        } else {
            self.restarts.load(Ordering::Acquire)
        };
        let unseen = frames.saturating_sub(self.seen_by_last_pass(restarts));
        if unseen >= self.threshold {
            let requested = self.requested.fetch_add(1, Ordering::AcqRel) + 1;
            // The whole WAL, not what the latest pass left: a pass that a
            // commit overlapped copied what it saw but cannot restart the
            // WAL, so passes alone do not bound it under steady writes. A
            // pass that a waiting write holds the gate for has no commit
            // during it.
            if frames >= self.write_wait_frames() {
                self.write_waits_for.fetch_max(requested, Ordering::AcqRel);
            }
            self.wake.notify_one();
        }
    }

    fn write_wait_frames(&self) -> u32 {
        self.threshold.saturating_mul(WRITE_WAIT_THRESHOLDS)
    }

    // The frames the latest checkpoint saw, or none when the WAL restarted
    // after it. A tag that cannot be matched counts as none, which only asks
    // for a checkpoint sooner.
    fn seen_by_last_pass(&self, restarts: u64) -> u32 {
        tagged_frames(self.last_pass.load(Ordering::Acquire), restarts)
    }

    // Records what a checkpoint saw and copied. `restarts` is read before it
    // began, so a restart during or after it leaves the tag behind and the
    // counts unused.
    fn passed(&self, restarts: u64, log: u32, checkpointed: u32) {
        let tag = (restarts & u64::from(u32::MAX)) << 32;
        self.backfilled
            .store(tag | u64::from(checkpointed), Ordering::Release);
        self.last_pass
            .store(tag | u64::from(log), Ordering::Release);
    }

    // A write that holds the writer gate waits here while a commit that
    // asked for a checkpoint with the WAL past the write-wait bound has no
    // finished checkpoint started after it, and no finished checkpoint has
    // copied every frame now in the WAL. The gate keeps the WAL at the
    // frames read on entry while it waits. Below the bound it does not wait.
    pub(crate) async fn caught_up(&self) {
        let needed = self.write_waits_for.load(Ordering::Acquire);
        let met =
            |done: u64| done >= needed || self.write_wait_met.load(Ordering::Acquire) >= needed;
        if met(*self.completed.borrow()) {
            return;
        }
        let restarts = self.restarts.load(Ordering::Acquire);
        let frames = self.frames.load(Ordering::Acquire);
        // Every pass end notifies `completed`, whether or not it raised it,
        // and records what it copied first.
        let mut completed = self.completed.subscribe();
        let _ = completed
            .wait_for(|done| {
                if met(*done) {
                    return true;
                }
                let copied_all =
                    tagged_frames(self.backfilled.load(Ordering::Acquire), restarts) >= frames;
                if copied_all {
                    self.write_wait_met.fetch_max(needed, Ordering::AcqRel);
                }
                copied_all
            })
            .await;
    }

    // Has every pool connection report its commits and how long each of its
    // statements ran here.
    pub(crate) async fn report_commits(
        &'static self,
        connection: &mut SqliteConnection,
    ) -> Result<(), sqlx::Error> {
        let mut handle = connection.lock_handle().await?;
        let db = handle.as_raw_handle().as_ptr().cast::<c_void>();
        let arg = std::ptr::from_ref(self).cast_mut().cast::<c_void>();
        // SAFETY: `db` is a live connection handle, held locked for the call;
        // `arg` points at a leaked `WalSignal`, valid for the whole process.
        unsafe {
            sqlite3_wal_hook(db, Some(on_commit), arg);
            sqlite3_trace_v2(db, SQLITE_TRACE_PROFILE, Some(on_statement), arg);
        }
        Ok(())
    }

    pub(crate) fn metrics_snapshot(&self) -> WalCheckpointMetricsSnapshot {
        let metrics = &self.metrics;
        WalCheckpointMetricsSnapshot {
            passes: metrics.passes.load(Ordering::Relaxed),
            passes_left_behind: metrics.passes_left_behind.load(Ordering::Relaxed),
            busy_passes: metrics.busy_passes.load(Ordering::Relaxed),
            restarts: self.restarts.load(Ordering::Relaxed),
            duration: metrics.duration.snapshot(),
            log_frames: metrics.log_frames.load(Ordering::Relaxed),
            checkpointed_frames: metrics.checkpointed_frames.load(Ordering::Relaxed),
            copied_frames: metrics.copied_frames.load(Ordering::Relaxed),
            wal_bytes: metrics.wal_bytes.load(Ordering::Relaxed),
            statement_duration: self.statements.snapshot(),
        }
    }

    // Waits until every checkpoint asked for so far has run, whether or not
    // a write would wait for it.
    #[cfg(test)]
    pub(crate) async fn settled(&self) {
        let needed = self.requested.load(Ordering::Acquire);
        let mut completed = self.completed.subscribe();
        let _ = completed.wait_for(|done| *done >= needed).await;
    }

    #[cfg(test)]
    pub(crate) fn frames(&self) -> u32 {
        self.frames.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn hold(&self, held: bool) {
        self.held.send_replace(held);
    }

    #[cfg(test)]
    pub(crate) fn runs(&self) -> u64 {
        *self.runs.borrow()
    }

    #[cfg(test)]
    pub(crate) fn hold_pass(&self, held: bool) {
        self.pass_held.send_replace(held);
    }

    // Passes that have taken their target, whether or not they have run.
    #[cfg(test)]
    pub(crate) fn started(&self) -> u64 {
        *self.started.borrow()
    }

    #[cfg(test)]
    pub(crate) async fn until_started(&self, passes: u64) {
        let mut started = self.started.subscribe();
        let _ = started.wait_for(|started| *started >= passes).await;
    }
}

// A frame count tagged with the restart count it was taken under, or none
// when the WAL has restarted since. A tag that cannot be matched counts as
// none.
fn tagged_frames(tagged: u64, restarts: u64) -> u32 {
    if tagged >> 32 == restarts & u64::from(u32::MAX) {
        u32::try_from(tagged & u64::from(u32::MAX)).unwrap_or(0)
    } else {
        0
    }
}

unsafe extern "C" fn on_commit(
    arg: *mut c_void,
    _db: *mut c_void,
    _name: *const c_char,
    frames: c_int,
) -> c_int {
    // SAFETY: `arg` is the leaked `WalSignal` installed by `report_commits`.
    let signal = unsafe { &*arg.cast::<WalSignal>() };
    signal.committed(u32::try_from(frames).unwrap_or(0));
    0
}

// One histogram observation per statement: atomics only, no allocation.
unsafe extern "C" fn on_statement(
    mask: u32,
    context: *mut c_void,
    _statement: *mut c_void,
    nanos: *mut c_void,
) -> c_int {
    if mask == SQLITE_TRACE_PROFILE && !nanos.is_null() {
        // SAFETY: `context` is the leaked `WalSignal` installed by
        // `report_commits`; for a profile event SQLite passes a pointer to
        // the statement's run time as a 64-bit count of nanoseconds.
        let (signal, nanos) = unsafe { (&*context.cast::<WalSignal>(), *nanos.cast::<i64>()) };
        signal
            .statements
            .observe(Duration::from_nanos(u64::try_from(nanos).unwrap_or(0)));
    }
    0
}

// Owns the checkpoint task's thread. Stopping it, or dropping the last
// handle, runs one more checkpoint, so the WAL is backfilled at shutdown,
// and waits for the task to close its connection and end.
pub(crate) struct WalCheckpointer {
    signal: &'static WalSignal,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl WalCheckpointer {
    pub(crate) fn signal(threshold: u32) -> &'static WalSignal {
        WalSignal::leak(threshold)
    }

    // Opens the task's own connection and starts it. The connection's
    // options must not write on open; see `sqlite_connect_options`.
    pub(crate) async fn start(
        signal: &'static WalSignal,
        options: SqliteConnectOptions,
    ) -> Result<Self, StateError> {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let wal_path = {
            let mut path = options.get_filename().as_os_str().to_owned();
            path.push("-wal");
            PathBuf::from(path)
        };
        let thread = std::thread::Builder::new()
            .name("weaver-wal-checkpoint".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                runtime.block_on(async move {
                    match options.connect().await {
                        Ok(connection) => {
                            let _ = ready_tx.send(Ok(()));
                            run(signal, connection, &wal_path).await;
                        }
                        Err(error) => {
                            let _ = ready_tx.send(Err(error.to_string()));
                        }
                    }
                });
                // No write can wait on a task that is gone.
                signal.completed.send_replace(u64::MAX);
            })
            .map_err(|error| {
                StateError::Database(format!("cannot start the WAL checkpoint task: {error}"))
            })?;
        let ready = ready_rx.await.unwrap_or_else(|_| {
            Err("the WAL checkpoint task stopped before it opened".to_string())
        });
        if let Err(error) = ready {
            let _ = thread.join();
            return Err(StateError::Database(format!(
                "cannot open the WAL checkpoint connection: {error}"
            )));
        }
        Ok(Self {
            signal,
            thread: Mutex::new(Some(thread)),
        })
    }

    // Ends the task through its own shutdown checkpoint and waits for its
    // thread, so its connection is closed when this returns. The database
    // closes through here, not through the last handle: a handle another
    // owner still holds would keep the file open past the close. Once
    // stopped, a later stop or drop has nothing to do.
    pub(crate) fn stop(&self) {
        self.signal.stopping.store(true, Ordering::Release);
        self.signal.wake.notify_one();
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(thread) = thread {
            let _ = thread.join();
        }
    }

    // Whether the task's thread has ended and said so, as it does last,
    // after closing its connection.
    #[cfg(test)]
    pub(crate) fn ended(&self) -> bool {
        self.thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_none_or(JoinHandle::is_finished)
            && *self.signal.completed.borrow() == u64::MAX
    }
}

impl Drop for WalCheckpointer {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn run(
    signal: &'static WalSignal,
    mut connection: SqliteConnection,
    wal_path: &std::path::Path,
) {
    // The previous pass's restart count and checkpointed frames.
    let mut previous: Option<(u64, u32)> = None;
    loop {
        signal.wake.notified().await;
        #[cfg(test)]
        {
            let mut held = signal.held.subscribe();
            let _ = held.wait_for(|held| !*held).await;
        }
        let stopping = signal.stopping.load(Ordering::Acquire);
        let target = signal.requested.load(Ordering::Acquire);
        // A request made between the last wake and the last pass's start
        // leaves a wake behind that the pass already served.
        if !stopping && target <= *signal.completed.borrow() {
            continue;
        }
        #[cfg(test)]
        {
            signal.started.send_modify(|started| *started += 1);
            let mut held = signal.pass_held.subscribe();
            let _ = held.wait_for(|held| !*held).await;
        }
        let restarts = signal.restarts.load(Ordering::Acquire);
        let started = Instant::now();
        // PASSIVE never waits on a reader or a writer and never takes the
        // write lock: it copies what no reader still needs, and the next
        // write restarts the WAL once all of it is copied. Its row is
        // (busy, frames in the WAL, frames copied into the database).
        match sqlx::query_as::<_, (i64, i64, i64)>("PRAGMA wal_checkpoint(PASSIVE)")
            .fetch_one(&mut connection)
            .await
        {
            Ok((busy, log, checkpointed)) => {
                let log = u32::try_from(log).unwrap_or(0);
                let checkpointed = u32::try_from(checkpointed).unwrap_or(0);
                signal.passed(restarts, log, checkpointed);
                let copied = match previous {
                    Some((before, done)) if before == restarts => checkpointed.saturating_sub(done),
                    _ => checkpointed,
                };
                previous = Some((restarts, checkpointed));
                record_pass(
                    signal,
                    wal_path,
                    started.elapsed(),
                    busy != 0,
                    log,
                    checkpointed,
                    copied,
                );
            }
            Err(error) => {
                tracing::warn!(error = %error, "WAL checkpoint failed");
            }
        }
        // Counted before completion wakes anyone, so a writer released by
        // `caught_up` already sees the run that released it. The send below
        // notifies even when it leaves `completed` unchanged, so a waiting
        // write checks every pass's copy.
        #[cfg(test)]
        signal.runs.send_modify(|runs| *runs += 1);
        signal
            .completed
            .send_modify(|done| *done = (*done).max(target));
        if stopping {
            break;
        }
    }
    let _ = connection.close().await;
}

// Records one pass, on the task's own thread. Reads the WAL file's length:
// one `stat` per pass, none on a commit.
fn record_pass(
    signal: &WalSignal,
    wal_path: &std::path::Path,
    elapsed: Duration,
    busy: bool,
    log: u32,
    checkpointed: u32,
    copied: u32,
) {
    let metrics = &signal.metrics;
    metrics.passes.fetch_add(1, Ordering::Relaxed);
    if checkpointed < log {
        metrics.passes_left_behind.fetch_add(1, Ordering::Relaxed);
    }
    if busy {
        metrics.busy_passes.fetch_add(1, Ordering::Relaxed);
    }
    metrics.duration.observe(elapsed);
    metrics.log_frames.store(u64::from(log), Ordering::Relaxed);
    metrics
        .checkpointed_frames
        .store(u64::from(checkpointed), Ordering::Relaxed);
    metrics
        .copied_frames
        .store(u64::from(copied), Ordering::Relaxed);
    if let Ok(file) = std::fs::metadata(wal_path) {
        metrics.wal_bytes.store(file.len(), Ordering::Relaxed);
    }
    if elapsed >= SLOW_PASS {
        tracing::warn!(
            elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            log,
            checkpointed,
            copied,
            "WAL checkpoint pass was slow"
        );
    }
}

#[cfg(test)]
mod tests;
