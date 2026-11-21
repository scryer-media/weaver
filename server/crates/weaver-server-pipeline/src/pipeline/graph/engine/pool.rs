// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The decode pool: the memory every job's engine runs reserve before they
// start, one pool for the whole process. It is apart from the holds: a run's
// reservation never takes room the holds keep, and held bytes never keep a
// run waiting, so at worst the process keeps the holds' resident limit and
// this pool resident together.
//
// The pool fits one run decoding at the ceiling the hardware profile grants a
// run: the wider of its memory and its window, and the run's fixed overhead.
// Smaller runs share it. A profile change resizes it; what is reserved
// stands, and runs waiting on it ask again once it grew.
//
// A run the profile lets decode in RAM above that ceiling reserves more
// than the pool holds, apart from it: it is admitted when its window, every
// other such run's, and the whole pool together fit under the machine's
// hard stop. Runs within the pool keep sharing it as they always did, and
// never wait on one above it, so a run above the ceiling never waits on a
// run that may itself be waiting on it. While one waits, others above the
// ceiling that come after it wait behind it.

use std::sync::{Mutex, PoisonError};

use weaver_graph::Budget;

use super::RUN_OVERHEAD;

#[derive(Debug)]
pub struct DecodePool(Mutex<Room>);

// What the pool holds reserved, against its limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Room {
    pub reserved: u64,
    pub limit: u64,
    // What runs above the ceiling hold, apart from the pool.
    pub above: u64,
    // How many times the pool was given room back: a release, or a run
    // that stopped waiting above the ceiling. A run turned away wakes once
    // this moved on.
    pub freed: u64,
    // Runs above the ceiling waiting for room under the hard stop.
    pub whole: u32,
}

impl DecodePool {
    pub fn new(limit: u64) -> Self {
        Self(Mutex::new(Room {
            reserved: 0,
            limit,
            above: 0,
            freed: 0,
            whole: 0,
        }))
    }

    // A pool with no limit, for runs outside a pipeline.
    #[cfg(any(test, feature = "test-support"))]
    pub fn unbounded() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new(u64::MAX))
    }

    // The pool for runs granted `run`.
    pub fn for_run(run: &Budget) -> Self {
        Self::new(limit_for(run))
    }

    // Runs from now on are granted `run`. Reservations already taken stand.
    pub(crate) fn follow(&self, run: &Budget) {
        self.set_limit(limit_for(run));
    }

    pub fn set_limit(&self, limit: u64) {
        self.lock().limit = limit;
    }

    pub fn room(&self) -> Room {
        *self.lock()
    }

    // Take `len` bytes if they fit beside what is reserved.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn reserve(&self, len: u64) -> bool {
        self.take(len).is_ok()
    }

    // Take `len` bytes if they fit beside what is reserved; otherwise the
    // room as it stood when they did not.
    pub(crate) fn take(&self, len: u64) -> Result<(), Room> {
        let mut room = self.lock();
        match room.reserved.checked_add(len) {
            Some(reserved) if reserved <= room.limit => {
                room.reserved = reserved;
                Ok(())
            }
            _ => Err(*room),
        }
    }

    // Whether `len` bytes above the ceiling could ever fit under `stop`
    // beside the whole pool.
    pub fn could_fit_above(&self, len: u64, stop: u64) -> bool {
        self.lock().limit.saturating_add(len) <= stop
    }

    // Take `len` bytes above the ceiling once they, what is held above it
    // already, and the whole pool fit under the hard stop `stop`, and no run
    // that asked before the caller still waits; otherwise the room as it
    // stood. `waiting` is whether the caller is counted among the runs
    // waiting above the ceiling, and is kept so.
    pub fn take_above(&self, len: u64, stop: u64, waiting: &mut bool) -> Result<(), Room> {
        let mut room = self.lock();
        let fits = room.limit.saturating_add(room.above).saturating_add(len) <= stop;
        if fits && (room.whole == 0 || *waiting) {
            room.above += len;
            if std::mem::take(waiting) {
                room.whole -= 1;
            }
            return Ok(());
        }
        if !std::mem::replace(waiting, true) {
            room.whole += 1;
        }
        Err(*room)
    }

    // The caller no longer waits above the ceiling.
    pub fn stop_waiting(&self, waiting: &mut bool) {
        if std::mem::take(waiting) {
            let mut room = self.lock();
            room.whole -= 1;
            room.freed += 1;
        }
    }

    pub fn release(&self, len: u64) {
        if len == 0 {
            return;
        }
        let mut room = self.lock();
        room.reserved = room.reserved.saturating_sub(len);
        room.freed += 1;
    }

    // Give back `len` bytes held above the ceiling.
    pub fn release_above(&self, len: u64) {
        if len == 0 {
            return;
        }
        let mut room = self.lock();
        room.above = room.above.saturating_sub(len);
        room.freed += 1;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Room> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// One run at the ceiling `run` grants, and its overhead. A run granted no
// ceiling may reserve any window it declares, so the pool has no limit.
fn limit_for(run: &Budget) -> u64 {
    run.memory_bytes
        .max(run.window_bytes)
        .map_or(u64::MAX, |ceiling| ceiling.saturating_add(RUN_OVERHEAD))
}
