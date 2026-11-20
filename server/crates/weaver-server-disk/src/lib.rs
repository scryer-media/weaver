// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#![deny(clippy::await_holding_lock)]

pub use weaver_server_base::{clock, jobs};
pub mod operations {
    pub use weaver_server_base::operations::instrumentation;
    pub mod disk;
    pub mod room_wait;

    pub use disk::{
        Capacity, CapacityDebits, CapacityReader, CapacityReading, DiskProbeError, DiskSpace,
        LateClassification, OnDemandSpace, Reach, StorageCapacity, StorageRoot, VolumeBuffers,
        VolumeWriters, is_out_of_space, is_quota_exceeded,
    };
}
pub mod pipeline {
    pub mod archive {
        pub mod split_reader;
    }
    pub mod graph {
        pub mod sparse;
    }
}
pub mod runtime {
    pub use weaver_server_base::runtime::*;
    pub mod file_cache;
    pub mod fs;
    pub mod system_probe;
    pub use system_probe::{StartupDiskGate, StartupDiskGateOpener};
}
pub mod scratch;
