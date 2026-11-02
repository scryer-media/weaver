// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Read handles of leaves whose file is no longer open for writing, shared by
// every job, so a file read again and again, as a seed's are by its peers,
// is opened once rather than at every read (libtorrent `file_pool`).
//
// A handle is known by its leaf's reader key, never by path: the leaf takes
// a fresh key whenever its file may have moved, been replaced or let go of,
// so no handle outlives the file it was opened on. At most `CAPACITY` stay
// open, the least recently read closed first; one not read for `IDLE` is
// closed by a sweep that runs only while any is open.

use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

// qBittorrent's `FilePoolSize` default; libtorrent's own `file_pool_size`
// is 40.
pub(crate) const CAPACITY: usize = 100;
// libtorrent `close_file_interval` on Windows. libtorrent closes nothing for
// idleness elsewhere; here every platform lets an idle handle go, so a
// torrent no peer reads holds no descriptor.
pub(crate) const IDLE: Duration = Duration::from_secs(240);

struct Reader {
    key: u64,
    file: Arc<File>,
    used: Instant,
    // When it was last read, in reads: the least recently read is the one
    // with the lowest, whatever the clock says.
    turn: u64,
}

struct Readers {
    open: Vec<Reader>,
    turns: u64,
    // Whether a sweep is running; set and cleared under the lock, so a
    // handle opened as the last sweep ends starts the next.
    sweeping: bool,
}

// A pool of read handles. Every job reads through `SHARED`; a test makes
// its own, so no other test's reads or clock reach it.
pub(crate) struct ReaderPool(Mutex<Readers>);

pub(crate) static SHARED: ReaderPool = ReaderPool::new();

impl std::fmt::Debug for ReaderPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaderPool").finish_non_exhaustive()
    }
}

static NEXT_KEY: AtomicU64 = AtomicU64::new(1);

impl Readers {
    // The handle under `key`, marked read now.
    fn used(&mut self, key: u64, now: Instant) -> Option<Arc<File>> {
        self.turns += 1;
        let turn = self.turns;
        let reader = self.open.iter_mut().find(|reader| reader.key == key)?;
        reader.used = now;
        reader.turn = turn;
        Some(Arc::clone(&reader.file))
    }
}

// A key no handle is open under, in any pool.
pub(crate) fn key() -> u64 {
    NEXT_KEY.fetch_add(1, Ordering::Relaxed)
}

impl ReaderPool {
    pub(crate) const fn new() -> Self {
        Self(Mutex::new(Readers {
            open: Vec::new(),
            turns: 0,
            sweeping: false,
        }))
    }

    fn readers(&self) -> std::sync::MutexGuard<'_, Readers> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // The handle open under `key`, opened on `path` when none is.
    pub(crate) fn open(&'static self, key: u64, path: &Path) -> io::Result<Arc<File>> {
        let now = Instant::now();
        {
            let mut readers = self.readers();
            if let Some(file) = readers.used(key, now) {
                return Ok(file);
            }
        }
        // Opened outside the lock: another reader's open never waits on it.
        let file = Arc::new(File::open(path)?);
        let mut readers = self.readers();
        if let Some(file) = readers.used(key, now) {
            return Ok(file);
        }
        if readers.open.len() >= CAPACITY
            && let Some(oldest) = (0..readers.open.len()).min_by_key(|&at| readers.open[at].turn)
        {
            readers.open.swap_remove(oldest);
        }
        readers.turns += 1;
        let turn = readers.turns;
        readers.open.push(Reader {
            key,
            file: Arc::clone(&file),
            used: now,
            turn,
        });
        if !readers.sweeping
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            readers.sweeping = true;
            runtime.spawn(self.sweep());
        }
        Ok(file)
    }

    // The handle under `key`, if one is open, is closed once its last read
    // ends.
    pub(crate) fn forget(&self, key: u64) {
        self.readers().open.retain(|reader| reader.key != key);
    }

    #[cfg(test)]
    pub(crate) fn is_open(&self, key: u64) -> bool {
        self.readers().open.iter().any(|reader| reader.key == key)
    }

    // Closes each handle once it has gone `IDLE` unread, sleeping until the
    // first one could, and ends when none is open.
    async fn sweep(&'static self) {
        loop {
            let next = {
                let mut readers = self.readers();
                let now = Instant::now();
                readers
                    .open
                    .retain(|reader| now.duration_since(reader.used) < IDLE);
                let Some(oldest) = readers.open.iter().map(|reader| reader.used).min() else {
                    readers.sweeping = false;
                    return;
                };
                oldest + IDLE
            };
            tokio::time::sleep_until(next).await;
        }
    }
}
