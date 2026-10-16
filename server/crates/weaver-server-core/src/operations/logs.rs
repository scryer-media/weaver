// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::runtime::log_buffer::LogRingBuffer;

pub fn snapshot_service_logs(buffer: &LogRingBuffer, limit: i32) -> Vec<String> {
    let clamped = limit.clamp(1, 2000) as usize;
    buffer.snapshot(clamped)
}
