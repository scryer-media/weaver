// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::{AtomicU64, Ordering};

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
    // The volume's buffer limits, over the bytes that have arrived and are
    // not written (`unwritten_bytes`). At the hard limit its lanes stop; the
    // soft limit is a reported figure. One pair for
    // the volume, shared by every stream writing to it. `u64::MAX` until
    // the volume is sized: no limit.
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
            buffer_soft_bytes: AtomicU64::new(u64::MAX),
            buffer_hard_bytes: AtomicU64::new(u64::MAX),
            wake_line: AtomicU64::new(0),
            idle_turns: AtomicU64::new(0),
            write_rate: AtomicU64::new(0),
            throughput: AtomicU64::new(0),
            writer: WriterCounts::default(),
        }
    }

    // The bytes that have arrived for the volume and are not written: what
    // is being decoded for its jobs and what waits for its writer. Articles
    // leased to a connection that have not arrived are not in it.
    pub(crate) fn unwritten_bytes(&self) -> u64 {
        self.arrived_bytes
            .load(Ordering::SeqCst)
            .saturating_add(self.buffered_bytes.load(Ordering::SeqCst))
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
