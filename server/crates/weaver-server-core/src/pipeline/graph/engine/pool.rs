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
pub(crate) struct DecodePool(Mutex<Room>);

// What the pool holds reserved, against its limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Room {
    pub(crate) reserved: u64,
    pub(crate) limit: u64,
    // What runs above the ceiling hold, apart from the pool.
    pub(crate) above: u64,
    // How many times the pool was given room back: a release, or a run
    // that stopped waiting above the ceiling. A run turned away wakes once
    // this moved on.
    pub(crate) freed: u64,
    // Runs above the ceiling waiting for room under the hard stop.
    pub(crate) whole: u32,
}

impl DecodePool {
    pub(crate) fn new(limit: u64) -> Self {
        Self(Mutex::new(Room {
            reserved: 0,
            limit,
            above: 0,
            freed: 0,
            whole: 0,
        }))
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
    #[cfg(test)]
    pub(crate) fn reserve(&self, len: u64) -> bool {
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
    pub(crate) fn could_fit_above(&self, len: u64, stop: u64) -> bool {
        self.lock().limit.saturating_add(len) <= stop
    }

    // Take `len` bytes above the ceiling once they, what is held above it
    // already, and the whole pool fit under the hard stop `stop`, and no run
    // that asked before the caller still waits; otherwise the room as it
    // stood. `waiting` is whether the caller is counted among the runs
    // waiting above the ceiling, and is kept so.
    pub(crate) fn take_above(&self, len: u64, stop: u64, waiting: &mut bool) -> Result<(), Room> {
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
    pub(crate) fn stop_waiting(&self, waiting: &mut bool) {
        if std::mem::take(waiting) {
            let mut room = self.lock();
            room.whole -= 1;
            room.freed += 1;
        }
    }

    pub(crate) fn release(&self, len: u64) {
        if len == 0 {
            return;
        }
        let mut room = self.lock();
        room.reserved = room.reserved.saturating_sub(len);
        room.freed += 1;
    }

    // Give back `len` bytes held above the ceiling.
    pub(crate) fn release_above(&self, len: u64) {
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
                limit: 3,
                above: 0,
                freed: 0,
                whole: 0,
            }
        );
        pool.release(6);
        pool.release(4);
        assert!(pool.reserve(3));
        assert!(!pool.reserve(1));
        assert!(!pool.reserve(u64::MAX), "an overflow never fits");
    }

    // A reservation above the ceiling starts at once beside the pool's
    // runs while it, the others above it, and the whole pool fit under the
    // hard stop; the pool's runs share the pool beside it as before.
    #[test]
    fn a_reservation_above_the_ceiling_starts_beside_the_pool_under_the_hard_stop() {
        let pool = DecodePool::new(10);
        assert!(pool.reserve(4));
        let mut waiting = false;
        assert_eq!(pool.take_above(25, 60, &mut waiting), Ok(()));
        assert!(!waiting);
        assert!(pool.reserve(6), "the pool is shared beside it as before");
        assert!(!pool.reserve(1));
        assert_eq!(
            pool.room(),
            Room {
                reserved: 10,
                limit: 10,
                above: 25,
                freed: 0,
                whole: 0,
            }
        );
        // A second above the ceiling waits for the first: 10 + 25 + 30 is
        // over 60. It holds no one in the pool back.
        let mut second = false;
        let turned = pool.take_above(30, 60, &mut second).unwrap_err();
        assert!(second);
        assert_eq!(turned.whole, 1);
        pool.release(4);
        assert!(pool.reserve(4), "the pool's runs never wait on it");
        // A third that would fit waits behind the second.
        let mut third = false;
        assert!(pool.take_above(1, 60, &mut third).is_err());
        pool.release_above(25);
        assert!(pool.room().freed > turned.freed, "the release wakes it");
        assert_eq!(pool.take_above(30, 60, &mut second), Ok(()));
        assert!(!second);
        assert_eq!(pool.take_above(1, 60, &mut third), Ok(()));
        assert_eq!(pool.room().above, 31);
        assert_eq!(pool.room().whole, 0);
        pool.release_above(30);
        pool.release_above(1);
        assert_eq!(pool.room().above, 0);
        assert!(pool.could_fit_above(50, 60));
        assert!(!pool.could_fit_above(51, 60), "never beside the whole pool");
    }

    // A run that stops waiting above the ceiling lets the others in again,
    // and wakes those it turned away.
    #[test]
    fn a_run_that_stops_waiting_above_the_ceiling_wakes_the_others() {
        let pool = DecodePool::new(10);
        let mut first = false;
        assert_eq!(pool.take_above(40, 60, &mut first), Ok(()));
        let mut waiting = false;
        assert!(pool.take_above(25, 60, &mut waiting).is_err());
        let mut behind = false;
        let turned = pool.take_above(5, 60, &mut behind).unwrap_err();
        pool.stop_waiting(&mut waiting);
        assert!(!waiting);
        assert!(pool.room().freed > turned.freed);
        assert_eq!(pool.take_above(5, 60, &mut behind), Ok(()));
        pool.stop_waiting(&mut waiting);
        assert_eq!(pool.room().whole, 0, "stopping twice counts once");
    }
}
