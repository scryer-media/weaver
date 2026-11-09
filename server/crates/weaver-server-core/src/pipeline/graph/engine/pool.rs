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

use std::sync::{Mutex, PoisonError};

use weaver_graph::Budget;

use super::RUN_OVERHEAD;

#[derive(Debug)]
pub(crate) struct DecodePool(Mutex<Room>);

// What the pool holds reserved, against its limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Room {
    pub(crate) reserved: u64,
    pub(crate) limit: u64,
}

impl DecodePool {
    pub(crate) fn new(limit: u64) -> Self {
        Self(Mutex::new(Room { reserved: 0, limit }))
    }

    // A pool with no limit, for runs outside a pipeline.
    #[cfg(test)]
    pub(crate) fn unbounded() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new(u64::MAX))
    }

    // The pool for runs granted `run`.
    pub(crate) fn for_run(run: &Budget) -> Self {
        Self::new(limit_for(run))
    }

    // Runs from now on are granted `run`. Reservations already taken stand.
    pub(crate) fn follow(&self, run: &Budget) {
        self.set_limit(limit_for(run));
    }

    pub(crate) fn set_limit(&self, limit: u64) {
        self.lock().limit = limit;
    }

    pub(crate) fn room(&self) -> Room {
        *self.lock()
    }

    // Take `len` bytes if they fit beside what is reserved.
    pub(crate) fn reserve(&self, len: u64) -> bool {
        let mut room = self.lock();
        match room.reserved.checked_add(len) {
            Some(reserved) if reserved <= room.limit => {
                room.reserved = reserved;
                true
            }
            _ => false,
        }
    }

    pub(crate) fn release(&self, len: u64) {
        let mut room = self.lock();
        room.reserved = room.reserved.saturating_sub(len);
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

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;

    fn run(memory: Option<u64>, window: Option<u64>) -> Budget {
        Budget {
            memory_bytes: memory,
            window_bytes: window,
            ..Default::default()
        }
    }

    // The pool fits one run at the ceiling the profile grants, the wider of
    // its memory and its window, and the run's overhead. Runs granted no
    // ceiling share a pool with no limit.
    #[test]
    fn the_pool_fits_one_decode_at_the_profiles_ceiling() {
        let room = |budget| DecodePool::for_run(&budget).room().limit;
        assert_eq!(
            room(run(Some(512 * MIB), Some(256 * MIB))),
            512 * MIB + RUN_OVERHEAD
        );
        assert_eq!(room(run(Some(GIB), Some(4 * GIB))), 4 * GIB + RUN_OVERHEAD);
        assert_eq!(room(run(None, None)), u64::MAX);
    }

    // Runs share the pool up to its limit; a release makes room again, and a
    // resize leaves what is reserved where it stands.
    #[test]
    fn reservations_share_the_pool_and_a_resize_keeps_them() {
        let pool = DecodePool::new(10);
        assert!(pool.reserve(6));
        assert!(!pool.reserve(5), "it does not fit beside the first");
        assert!(pool.reserve(4));
        pool.set_limit(3);
        assert_eq!(
            pool.room(),
            Room {
                reserved: 10,
                limit: 3
            }
        );
        pool.release(6);
        pool.release(4);
        assert!(pool.reserve(3));
        assert!(!pool.reserve(1));
        assert!(!pool.reserve(u64::MAX), "an overflow never fits");
    }
}
