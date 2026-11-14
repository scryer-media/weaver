// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::{AtomicU64, Ordering};

// The most of a volume's hard limit kept for the pipeline loop's own
// writes (`Volume::loop_share`).
const LOOP_SHARE: u64 = 4 << 20;

// One volume as the OS presents it: every path with the same device id.
pub(crate) struct Volume {
    // The device id the disk owner holds for the volume's paths; `None`
    // when the OS gave none.
    pub(crate) device: Option<u64>,
    // The mount path or drive root shown to the user.
    pub(crate) label: String,
    // Bytes its writer has written, and the nanoseconds spent inside those
    // write calls. After each call the writer adds the nanoseconds first
    // and the bytes last, with `Release`; the rate sampler loads the bytes
    // first, with `Acquire`, and the nanoseconds after, so it never reads a
    // call's bytes without its time.
    pub(crate) written_bytes: AtomicU64,
    pub(crate) write_nanos: AtomicU64,
    // Write calls its writer has made, and how many of them did not start
    // where the previous write to the same file ended.
    pub(crate) write_calls: AtomicU64,
    pub(crate) write_seeks: AtomicU64,
    // Bytes waiting in memory for its writer.
    pub(crate) buffered_bytes: AtomicU64,
    // Bytes that arrived for the jobs writing here and have not been handed
    // to its writer yet: what is being decoded. Only the pipeline loop
    // changes it.
    pub(crate) arrived_bytes: AtomicU64,
    // What counts against the hard limit (`unwritten_bytes`): the articles
    // leased to lanes for the jobs writing here and not arrived, at the
    // size the post gives them, what arrived and is being decoded, and what
    // waits for the writer. Only `reserve` and `charge` raise it, and only
    // the pipeline loop calls them; the writer lowers it as it writes.
    counted_bytes: AtomicU64,
    // The most `counted_bytes` has been.
    peak_bytes: AtomicU64,
    // Bytes of `counted_bytes` the pipeline loop holds for what it is about
    // to queue in one step: the arrived article it is handing on. Its
    // writes take from here first and are charged only past it. Only the
    // loop reads or changes it.
    pub(crate) loop_credit: AtomicU64,
    // The volume's buffer limits. Nothing is reserved or charged past the
    // hard one (`reserve`, `charge`); the soft limit is a reported figure.
    // One pair for the volume, shared by every stream writing to it.
    // `u64::MAX` until the volume is sized: no limit.
    buffer_soft_bytes: AtomicU64,
    buffer_hard_bytes: AtomicU64,
    // One more than the count of unwritten bytes someone asked to be woken
    // at; zero when no one asked. Whoever wakes them clears it.
    pub(crate) wake_line: AtomicU64,
    // Times its writer ran out of work. A sample across which this did not
    // move is one in which the writer never did.
    pub(crate) idle_turns: AtomicU64,
    // What the volume takes, in bytes per second; zero until measured. A
    // synced probe sets it when the volume is first seen, and samples taken
    // while its writer never ran out of work replace it.
    pub(crate) write_rate: AtomicU64,
    // Bytes per second of wall time its writer wrote over the last window,
    // whether or not the volume was the limit; zero when nothing was written.
    pub(crate) throughput: AtomicU64,
    // What its writer did and what waiting on it cost, for the metrics.
    pub(crate) writer: WriterCounts,
}

// A volume writer's own counts. Each is one relaxed add per event.
#[derive(Default)]
pub(crate) struct WriterCounts {
    // Batches the writer ran, and the operations in them it ran.
    pub(crate) batches: AtomicU64,
    pub(crate) operations: AtomicU64,
    // Times the pipeline loop stood still for the writer, and for how long:
    // in all, and by what it waited for (`LoopWait`).
    pub(crate) loop_waits: AtomicU64,
    pub(crate) loop_wait_nanos: AtomicU64,
    pub(crate) loop_waits_by_kind: [AtomicU64; LoopWait::COUNT],
    pub(crate) loop_wait_nanos_by_kind: [AtomicU64; LoopWait::COUNT],
    // Reads the pipeline loop made itself of a file the writer writes,
    // after its wait, and how long they took, the open included.
    pub(crate) loop_reads: AtomicU64,
    pub(crate) loop_read_nanos: AtomicU64,
    // Times a thread other than the loop (a check, a repair, a replay)
    // stood for the writer in `Barrier::wait_aside`, and for how long.
    pub(crate) aside_waits: AtomicU64,
    pub(crate) aside_wait_nanos: AtomicU64,
    // Operations that came back with an error.
    pub(crate) failed_operations: AtomicU64,
    // Operations not run: their caller gave its files up, or an earlier
    // operation of the same caller failed.
    pub(crate) abandoned_operations: AtomicU64,
    // Writer threads started for the volume, one after another.
    pub(crate) threads: AtomicU64,
    // Times the writer woke to mail after waiting for it: a send that found
    // it parked.
    pub(crate) woken: AtomicU64,
    // Written buffers handed back to the pipeline loop to be freed there,
    // and those the writer freed itself because the way back was full.
    pub(crate) buffers_returned: AtomicU64,
    pub(crate) buffers_freed: AtomicU64,
}

// What the pipeline loop stood still for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoopWait {
    // A read of a file's bytes, after the file's writes before it.
    Read,
    // A file's operations to return: its length, or a delivery of it.
    Settle,
    // The volume's buffered bytes to fall below its limit, while the loop
    // itself produces what is written (a repair's or a replay's output).
    Budget,
}

impl LoopWait {
    pub(crate) const COUNT: usize = 3;
    // In the order of `instrumentation::LOOP_WAIT_KINDS`, which labels them.
    pub(crate) const ALL: [LoopWait; Self::COUNT] = [Self::Read, Self::Settle, Self::Budget];
}

impl WriterCounts {
    // The loop stood still for `nanos` waiting for `kind`. Relaxed adds.
    pub(crate) fn loop_waited(&self, kind: LoopWait, nanos: u64) {
        self.loop_waits.fetch_add(1, Ordering::Relaxed);
        self.loop_wait_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.loop_waits_by_kind[kind as usize].fetch_add(1, Ordering::Relaxed);
        self.loop_wait_nanos_by_kind[kind as usize].fetch_add(nanos, Ordering::Relaxed);
    }

    // The loop read a writer's file itself for `nanos`. Relaxed adds.
    pub(crate) fn loop_read(&self, nanos: u64) {
        self.loop_reads.fetch_add(1, Ordering::Relaxed);
        self.loop_read_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    // A thread aside stood `nanos` for the writer. Relaxed adds.
    pub(crate) fn aside_waited(&self, nanos: u64) {
        self.aside_waits.fetch_add(1, Ordering::Relaxed);
        self.aside_wait_nanos.fetch_add(nanos, Ordering::Relaxed);
    }
}

impl Volume {
    pub(crate) fn new(device: Option<u64>, label: String) -> Self {
        Self {
            device,
            label,
            written_bytes: AtomicU64::new(0),
            write_nanos: AtomicU64::new(0),
            write_calls: AtomicU64::new(0),
            write_seeks: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            arrived_bytes: AtomicU64::new(0),
            counted_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            loop_credit: AtomicU64::new(0),
            buffer_soft_bytes: AtomicU64::new(u64::MAX),
            buffer_hard_bytes: AtomicU64::new(u64::MAX),
            wake_line: AtomicU64::new(0),
            idle_turns: AtomicU64::new(0),
            write_rate: AtomicU64::new(0),
            throughput: AtomicU64::new(0),
            writer: WriterCounts::default(),
        }
    }

    // The bytes counted against the volume's hard limit: leased and not
    // arrived, being decoded, and waiting for its writer.
    pub(crate) fn unwritten_bytes(&self) -> u64 {
        self.counted_bytes.load(Ordering::SeqCst)
    }

    // The most bytes ever counted against the volume at once.
    pub(crate) fn peak_bytes(&self) -> u64 {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    // The part of the hard limit only the pipeline loop's own writes take:
    // what it says again for a repair or a replay, an extraction's output,
    // a delivery. Leases stop below it, so the loop always finds this much
    // once the writer has written what it holds, and never waits on bytes
    // only it can hand on.
    pub(crate) fn loop_share(&self) -> u64 {
        let (_, hard) = self.buffer_limits();
        (hard / 16).clamp(1, LOOP_SHARE)
    }

    // The bytes below which leases are granted: the hard limit less the
    // loop's share. `u64::MAX` when the volume is not sized.
    pub(crate) fn lease_limit(&self) -> u64 {
        let (_, hard) = self.buffer_limits();
        if hard == u64::MAX {
            return u64::MAX;
        }
        hard.saturating_sub(self.loop_share())
    }

    // The bytes a lease may still take.
    pub(crate) fn lease_room(&self) -> u64 {
        self.lease_limit().saturating_sub(self.unwritten_bytes())
    }

    // Reserve `bytes` for an article leased to a lane, if they fit below
    // the lease limit. A volume that counts nothing grants one article
    // whatever its size, so an article larger than the limit is still
    // fetched, alone.
    pub(crate) fn reserve(&self, bytes: u64) -> bool {
        let limit = self.lease_limit();
        let granted =
            self.counted_bytes
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |counted| {
                    (counted == 0 || counted.saturating_add(bytes) <= limit)
                        .then(|| counted.saturating_add(bytes))
                });
        match granted {
            Ok(counted) => {
                self.peak_bytes
                    .fetch_max(counted.saturating_add(bytes), Ordering::Relaxed);
                true
            }
            Err(_) => false,
        }
    }

    // Whether the loop may queue `bytes` more of its own: they fit below
    // the hard limit, or the volume counts nothing, or its writer holds
    // nothing it could write to make room.
    pub(crate) fn fits(&self, bytes: u64) -> bool {
        let (_, hard) = self.buffer_limits();
        let counted = self.unwritten_bytes();
        counted == 0
            || counted.saturating_add(bytes) <= hard
            || self.buffered_bytes.load(Ordering::SeqCst) == 0
    }

    // Count `bytes` the loop puts in: an arrived article, or a write past
    // its credit once it `fits`.
    pub(crate) fn charge(&self, bytes: u64) {
        let counted = self.counted_bytes.fetch_add(bytes, Ordering::SeqCst);
        self.peak_bytes
            .fetch_max(counted.saturating_add(bytes), Ordering::Relaxed);
    }

    // An article reserved at `reserved` bytes arrived at `arrived`: it is
    // counted at what arrived from here, in one step, so the count never
    // dips below what is held while its reservation is given back.
    pub(crate) fn settle(&self, reserved: u64, arrived: u64) {
        if arrived <= reserved {
            self.release(reserved - arrived);
        } else {
            self.charge(arrived - reserved);
        }
    }

    // `bytes` reserved or charged no longer count: an article left its
    // lane, or the writer wrote them.
    pub(crate) fn release(&self, bytes: u64) {
        let before = self.counted_bytes.fetch_sub(bytes, Ordering::SeqCst);
        debug_assert!(before >= bytes, "a volume released more than it counted");
    }

    // Give the volume its soft and hard buffer limits. A limit lowered
    // below what the volume holds stops its lanes until it has drained.
    pub(crate) fn set_buffer_limits(&self, soft_bytes: u64, hard_bytes: u64) {
        self.buffer_soft_bytes.store(soft_bytes, Ordering::Relaxed);
        self.buffer_hard_bytes.store(hard_bytes, Ordering::Relaxed);
    }

    // The volume's soft and hard buffer limits, in that order.
    pub(crate) fn buffer_limits(&self) -> (u64, u64) {
        (
            self.buffer_soft_bytes.load(Ordering::Relaxed),
            self.buffer_hard_bytes.load(Ordering::Relaxed),
        )
    }

    // Count exactly `bytes`, as a test sets a volume's fill.
    #[cfg(test)]
    pub(crate) fn count_for_test(&self, bytes: u64) {
        self.counted_bytes.store(bytes, Ordering::SeqCst);
        self.peak_bytes.fetch_max(bytes, Ordering::Relaxed);
    }

    // The unwritten bytes at which whoever was stopped at the hard limit is
    // woken: an eighth below it, so the loop is not woken per write.
    pub(crate) fn resume_line(&self) -> u64 {
        let (_, hard) = self.buffer_limits();
        hard.saturating_sub((hard / 8).max(1))
    }

    // Take the wake someone asked for, when the unwritten bytes have fallen
    // to their line: true once for each ask, to whoever lowered the bytes
    // there, the volume's writer or the pipeline loop. The caller lowers
    // its count first and asks after.
    pub(crate) fn take_wake(&self) -> bool {
        let ask = self.wake_line.load(Ordering::SeqCst);
        ask != 0
            && self.unwritten_bytes() < ask
            && self
                .wake_line
                .compare_exchange(ask, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sized(hard: u64) -> Volume {
        let volume = Volume::new(Some(1), "/v".into());
        volume.set_buffer_limits(hard, hard);
        volume
    }

    // Leases stop below the loop's share of the hard limit; the loop's own
    // writes take that share, and nothing goes past the limit.
    #[test]
    fn leases_stop_at_the_lease_limit_and_the_loop_keeps_its_share() {
        let volume = sized(1600);
        assert_eq!(volume.loop_share(), 100);
        assert_eq!(volume.lease_limit(), 1500);
        assert!(volume.reserve(1000));
        assert!(volume.reserve(500));
        assert!(!volume.reserve(1), "the lease limit is reached");
        assert_eq!(volume.lease_room(), 0);
        assert_eq!(volume.unwritten_bytes(), 1500);
        // The loop's share is left for the loop.
        volume.buffered_bytes.store(1, Ordering::SeqCst);
        assert!(volume.fits(100));
        assert!(!volume.fits(101));
        volume.charge(100);
        assert_eq!(volume.unwritten_bytes(), 1600);
        assert!(!volume.fits(1));
        volume.release(600);
        assert!(volume.reserve(500));
        assert_eq!(volume.peak_bytes(), 1600);
    }

    // A volume that counts nothing grants one article larger than its
    // whole limit, and only one.
    #[test]
    fn an_empty_volume_grants_one_article_larger_than_its_limit() {
        let volume = sized(1600);
        assert!(volume.reserve(5000));
        assert!(!volume.reserve(1));
        assert_eq!(volume.peak_bytes(), 5000);
        volume.release(5000);
        assert_eq!(volume.unwritten_bytes(), 0);
        assert!(volume.reserve(1500));
    }

    // A volume not sized grants everything.
    #[test]
    fn a_volume_not_sized_grants_everything() {
        let volume = Volume::new(None, "/v".into());
        assert_eq!(volume.lease_limit(), u64::MAX);
        assert!(volume.reserve(u64::MAX / 2));
        assert!(volume.reserve(1 << 40));
    }
}
