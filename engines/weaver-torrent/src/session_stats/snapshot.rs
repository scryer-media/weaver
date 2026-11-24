// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use serde::Serialize;
pub use weaver_utp::UtpStatsSnapshot;
pub use weaver_utp::raw::ext_close_reason::LibTorrentCloseReason as UtpCloseReason;

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
    pub swarm: super::swarm::SwarmSnapshot,
    pub trackers: super::tracker::TrackerSnapshot,
    pub incoming: super::incoming::IncomingSnapshot,
    pub web_seeds: super::web_seed::WebSeedSnapshot,
    pub network: super::network::NetworkSnapshot,
    /// `None` while the session runs no local service discovery.
    pub lsd: Option<super::network::LsdSnapshot>,
    pub limiter: super::network::LimiterSnapshot,
    /// Upload slots held by unchoked peers now.
    pub unchoke_slots_held: usize,
    /// The session's disk calls, blocking permits and peer sockets.
    pub disk: super::disk::DiskStatsSnapshot,
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
            swarm: s.swarm.snapshot(),
            trackers: s.trackers.snapshot(),
            incoming: s.incoming.snapshot(),
            web_seeds: s.web_seeds.snapshot(),
            network: s.network.snapshot(),
            lsd: None,
            limiter: Default::default(),
            unchoke_slots_held: 0,
            disk: Default::default(),
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
    immediate_acks: u64,
    delayed_acks: u64,
    window_update_acks: u64,
    ack_packets_sent: u64,
    data_packets_received: u64,
    data_packets_sent: u64,
    sent_control_packets: u64,
    send_window_exhausted: u64,
    data_retransmissions: u64,
    duplicate_acks_received: u64,
    out_of_order_packets: u64,
    streams_given_up: u64,
    receive_drains: u64,
    streams_marked: u64,
    drain_marks_with_packets_queued: u64,
    acks_with_sack: u64,
    sack_bytes_sent: u64,
    sacks_received: u64,
    sack_bits_received: u64,
    data_packets_with_sack: u64,
    duplicate_acks_sent: u64,
    receive_cuts: u64,
    send_runs: u64,
    send_run_datagrams: u64,
    send_run_errors: u64,
    receive_calls: u64,
    receive_call_datagrams: u64,
    send_batches: u64,
    send_batch_datagrams: u64,
    datagrams_too_long: u64,
    send_batch_errors: u64,
    syns_received: u64,
    syns_repeated: u64,
    syns_queued: u64,
    unmatched_packets: u64,
    accepts: u64,
    unknown_stream_resets: u64,
    resets_rate_limited: u64,
    inactivity_timeouts: u64,
    synack_retransmissions: u64,
    send_errors: u64,
    live_virtual_sockets: u64,
    remote_close_reasons: [u64; UtpCloseReason::BUCKETS],
}
