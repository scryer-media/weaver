// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What the piece checks ask of the disk and the processor: the check of a
//! torrent's files, and the check of a seed-mode piece the first time a
//! peer asks for it. Counted for the whole process, as the checks of every
//! session read the same disks. Each counter is one relaxed add per read
//! call or per piece.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

gen_stats!(CheckStatsAtomic CheckStatsSnapshot, [
    // Read calls a check made to a storage.
    read_calls u64,
    // The bytes those calls read.
    read_bytes u64,
    // The time spent inside those calls, in nanoseconds.
    read_nanos u64,
    // Read calls that did not start where the read before them on the same
    // file ended. A check of a torrent's files knows the read before; the
    // check of one seed-mode piece does not, and counts a read as one
    // unless it starts its file.
    read_seeks u64,
    // Pieces hashed.
    pieces_hashed u64,
    // The time spent hashing them, in nanoseconds.
    hash_nanos u64,
    // Seed-mode pieces whose check was started.
    seed_checks_started u64,
    // Askers of a seed-mode piece that waited on a check already running
    // and took its verdict. One whose wait ended without a verdict, the
    // check having ended in an error, is not counted.
    seed_checks_joined u64
], []);

/// The process's check counters.
pub static CHECK_STATS: CheckStatsAtomic = CheckStatsAtomic {
    read_calls: AtomicU64::new(0),
    read_bytes: AtomicU64::new(0),
    read_nanos: AtomicU64::new(0),
    read_seeks: AtomicU64::new(0),
    pieces_hashed: AtomicU64::new(0),
    hash_nanos: AtomicU64::new(0),
    seed_checks_started: AtomicU64::new(0),
    seed_checks_joined: AtomicU64::new(0),
};

impl CheckStatsAtomic {
    /// One read call of `bytes` bytes that began at `started`, a seek when
    /// it did not go on from the read before it.
    pub(crate) fn read(&self, bytes: usize, started: Instant, seek: bool) {
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.read_nanos
            .fetch_add(nanos(started.elapsed()), Ordering::Relaxed);
        if seek {
            self.read_seeks.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One piece hashed, in `spent`.
    pub(crate) fn hashed(&self, spent: Duration) {
        self.pieces_hashed.fetch_add(1, Ordering::Relaxed);
        self.hash_nanos.fetch_add(nanos(spent), Ordering::Relaxed);
    }
}

fn nanos(spent: Duration) -> u64 {
    u64::try_from(spent.as_nanos()).unwrap_or(u64::MAX)
}
