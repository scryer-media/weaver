// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::AtomicU64;

// One volume as the OS presents it: every path with the same device id.
pub(crate) struct Volume {
    // The device id the disk owner holds for the volume's paths; `None`
    // when the OS gave none.
    pub(crate) device: Option<u64>,
    // The mount path or drive root shown to the user.
    pub(crate) label: String,
    // Bytes its writer has written, and the nanoseconds spent inside those
    // write calls.
    pub(crate) written_bytes: AtomicU64,
    pub(crate) write_nanos: AtomicU64,
    // Write calls its writer has made, and how many of them did not start
    // where the previous write to the same file ended.
    pub(crate) write_calls: AtomicU64,
    pub(crate) write_seeks: AtomicU64,
    // Bytes waiting in memory for its writer.
    pub(crate) buffered_bytes: AtomicU64,
    // Times its writer found nothing waiting. A sample across which this
    // did not move is one in which the writer never ran out of work.
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
    // Times the pipeline loop stood still for the writer, and for how long.
    pub(crate) loop_waits: AtomicU64,
    pub(crate) loop_wait_nanos: AtomicU64,
    // Operations that came back with an error.
    pub(crate) failed_operations: AtomicU64,
    // Operations not run: their caller gave its files up, or an earlier
    // operation of the same caller failed.
    pub(crate) abandoned_operations: AtomicU64,
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
            idle_turns: AtomicU64::new(0),
            write_rate: AtomicU64::new(0),
            throughput: AtomicU64::new(0),
            writer: WriterCounts::default(),
        }
    }
}
