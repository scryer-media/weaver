// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use serde::Serialize;
pub use weaver_utp::UtpStatsSnapshot;

use crate::{
    session_stats::SessionCountersSnapshot,
    stream_connect::ConnectStatsSnapshot,
    torrent_state::{peers::stats::AggregatePeerStats, stats::Speed},
};

use super::SessionStats;

#[derive(Debug, Serialize)]
pub struct SessionStatsSnapshot {
    pub counters: SessionCountersSnapshot,
    pub download_speed: Speed,
    pub upload_speed: Speed,
    pub peers: AggregatePeerStats,
    pub uptime_seconds: u64,
    pub connections: ConnectStatsSnapshot,
    #[serde(with = "SerializedUtpStatsSnapshot")]
    pub utp: UtpStatsSnapshot,
}

impl From<(&SessionStats, ConnectStatsSnapshot)> for SessionStatsSnapshot {
    fn from((s, c): (&SessionStats, ConnectStatsSnapshot)) -> Self {
        Self {
            download_speed: s.down_speed_estimator.mbps().into(),
            upload_speed: s.up_speed_estimator.mbps().into(),
            counters: s.counters.snapshot(),
            peers: s.peers.snapshot(),
            uptime_seconds: s.startup_time.elapsed().as_secs(),
            connections: c,
            utp: Default::default(),
        }
    }
}

#[derive(Serialize)]
#[serde(remote = "weaver_utp::UtpStatsSnapshot")]
struct SerializedUtpStatsSnapshot {
    delay_samples_above_target: u64,
    delay_samples_below_target: u64,
    packets_lost: u64,
    timeouts: u64,
    invalid_packets: u64,
    redundant_packets: u64,
    syn_flood_dropped: u64,
    syn_rst: u64,
    max_congestion_window_bytes: u64,
}
