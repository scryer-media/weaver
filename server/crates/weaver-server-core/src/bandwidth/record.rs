// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandwidthUsageMinuteBucket {
    pub bucket_epoch_minute: i64,
    pub payload_bytes: u64,
}
