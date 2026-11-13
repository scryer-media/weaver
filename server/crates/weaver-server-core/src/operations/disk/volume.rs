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
    // Bytes waiting in memory for its writer.
    pub(crate) buffered_bytes: AtomicU64,
    // What the volume takes, in bytes per second inside write calls, over
    // the last window; zero until measured.
    pub(crate) write_rate: AtomicU64,
}

impl Volume {
    pub(crate) fn new(device: Option<u64>, label: String) -> Self {
        Self {
            device,
            label,
            written_bytes: AtomicU64::new(0),
            write_nanos: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            write_rate: AtomicU64::new(0),
        }
    }
}
