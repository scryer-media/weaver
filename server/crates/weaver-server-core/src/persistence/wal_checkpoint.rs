// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The WAL is checkpointed by one task per database, on its own connection,
// never by the commit that crossed the threshold. SQLite's autocheckpoint
// runs inside the committing statement, so every write behind it in the
// writer gate waited for a copy of up to the whole WAL into the database
// file, with its syncs; on network storage that is seconds. Pool
// connections run with `wal_autocheckpoint = 0` and report the WAL's frame
// count after each commit instead; a commit that leaves it at or past the
// threshold wakes the task, and the next write to take the gate waits for a
// checkpoint started after that commit; the task itself never takes it. The WAL then restarts under that
// write, so it never holds more than the threshold plus one transaction.

use std::ffi::{c_char, c_int, c_void};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::thread::JoinHandle;

use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Connection};
use tokio::sync::{Notify, watch};

use crate::persistence::StateError;

// SQLite's own default is 1000 frames; 4000 (16 MB at 4 KiB pages) turns
// many small commits into fewer, longer checkpoint writes.
pub(crate) const WAL_CHECKPOINT_FRAMES: u32 = 4000;

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
}

// What commits tell the checkpoint task, and what the task tells writes.
pub(crate) struct WalSignal {
    threshold: u32,
    // WAL frames after the latest commit on any pool connection.
    frames: AtomicU32,
    // Commits that left the WAL at or past the threshold.
    requested: AtomicU64,
    // The highest `requested` a finished checkpoint started after.
    completed: watch::Sender<u64>,
    wake: Notify,
    stopping: AtomicBool,
    // A test holds the task back to see a commit cross the threshold with
    // no checkpoint anywhere.
    #[cfg(test)]
    held: watch::Sender<bool>,
    #[cfg(test)]
    runs: watch::Sender<u64>,
}

impl WalSignal {
    // Leaked on purpose: SQLite keeps a raw pointer to it in every pool
    // connection, and a connection can outlive the services that opened it
    // while the pool closes. One small struct per database opened.
    fn leak(threshold: u32) -> &'static Self {
        Box::leak(Box::new(Self {
            threshold,
            frames: AtomicU32::new(0),
            requested: AtomicU64::new(0),
            completed: watch::channel(0).0,
            wake: Notify::new(),
            stopping: AtomicBool::new(false),
            #[cfg(test)]
            held: watch::channel(false).0,
            #[cfg(test)]
            runs: watch::channel(0).0,
        }))
    }

    // Called by SQLite on the committing connection, after the commit and
    // outside its write lock. Never skipped: a wake while the task is busy
    // stays stored, and every crossing raises `requested`.
    fn committed(&self, frames: u32) {
        self.frames.store(frames, Ordering::Release);
        if frames >= self.threshold {
            self.requested.fetch_add(1, Ordering::AcqRel);
            self.wake.notify_one();
        }
    }

    // A write that holds the writer gate waits here while a commit that
    // crossed the threshold has no finished checkpoint after it.
    pub(crate) async fn caught_up(&self) {
        let needed = self.requested.load(Ordering::Acquire);
        let mut completed = self.completed.subscribe();
        let _ = completed.wait_for(|done| *done >= needed).await;
    }

    // Has every pool connection report its commits here.
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
        }
        Ok(())
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

// Owns the checkpoint task's thread. Dropping the last handle runs one more
// checkpoint, so the WAL is backfilled at shutdown, and waits for it.
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
                            run(signal, connection).await;
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
}

impl Drop for WalCheckpointer {
    fn drop(&mut self) {
        self.signal.stopping.store(true, Ordering::Release);
        self.signal.wake.notify_one();
        let thread = self
            .thread
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(thread) = thread {
            let _ = thread.join();
        }
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: the checkpoint task's own connection runs a PASSIVE checkpoint and never takes the gate"
)]
async fn run(signal: &'static WalSignal, mut connection: SqliteConnection) {
    loop {
        signal.wake.notified().await;
        #[cfg(test)]
        {
            let mut held = signal.held.subscribe();
            let _ = held.wait_for(|held| !*held).await;
        }
        let stopping = signal.stopping.load(Ordering::Acquire);
        let target = signal.requested.load(Ordering::Acquire);
        // PASSIVE never waits on a reader or a writer and never takes the
        // write lock: it copies what no reader still needs, and the next
        // write restarts the WAL once all of it is copied.
        if let Err(error) = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
            .execute(&mut connection)
            .await
        {
            tracing::warn!(error = %error, "WAL checkpoint failed");
        }
        signal
            .completed
            .send_modify(|done| *done = (*done).max(target));
        #[cfg(test)]
        signal.runs.send_modify(|runs| *runs += 1);
        if stopping {
            break;
        }
    }
    let _ = connection.close().await;
}

#[cfg(test)]
mod tests;
