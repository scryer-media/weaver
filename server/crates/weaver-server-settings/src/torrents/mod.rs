// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod identity;

// The seeding ledger.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TorrentCounters {
    pub uploaded_bytes: u64,
    // Every byte fetched from peers, whether it verified or not.
    pub downloaded_bytes: u64,
    // Time spent running in a session, in seconds.
    pub active_duration_s: u64,
    // The last time a complete copy was in reach: the torrent's own while it
    // ran complete, or a connected seed's.
    pub last_seen_complete_ms: Option<i64>,
    // Time spent complete and seeding, in seconds.
    pub finished_duration_s: u64,
    // The seconds of the current unbroken seeding run: back to zero at any
    // reading that finds the torrent not seeding, and at a restart.
    pub consecutive_seeding_s: u64,
    pub last_activity_ms: Option<i64>,
    // Bytes fetched and thrown away: those of pieces that failed their
    // check, and those of blocks that arrived when already in hand. Their
    // sum is the torrent's wasted bytes.
    pub failed_bytes: u64,
    pub redundant_bytes: u64,
}

impl TorrentCounters {
    // Every byte fetched and thrown away.
    pub fn wasted_bytes(&self) -> u64 {
        self.failed_bytes.saturating_add(self.redundant_bytes)
    }
}
