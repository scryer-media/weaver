// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::{AtomicU64, Ordering};

// One socket's counters: relaxed atomics, bumped where the event happens.
// Gauges (`accepting`, `connecting`, `live_virtual_sockets`) go up and down.
#[derive(Debug, Default)]
pub struct UtpStats {
    pub accepting: AtomicU64,
    pub accepts: AtomicU64,
    pub connecting: AtomicU64,
    pub connection_attempts: AtomicU64,
    pub connection_failures: AtomicU64,
    pub connection_successes: AtomicU64,
    pub consumed_bytes: AtomicU64,
    pub consumed_data_seq_nrs: AtomicU64,
    pub duplicate_acks_received: AtomicU64,
    pub inactivity_timeouts: AtomicU64,
    pub immediate_acks: AtomicU64,
    pub delayed_acks: AtomicU64,
    /// Delay samples at or above the effective congestion target.
    pub delay_samples_above_target: AtomicU64,
    pub delay_samples_below_target: AtomicU64,
    /// Non-probe loss indications passed to congestion handling, before loss gating.
    pub packets_lost: AtomicU64,
    /// Distinct retransmission expiries for outstanding data, probes, or FIN.
    pub timeouts: AtomicU64,
    pub invalid_packets: AtomicU64,
    pub redundant_packets: AtomicU64,
    pub syn_flood_dropped: AtomicU64,
    pub syn_rst: AtomicU64,
    /// ST_RESETs sent for packets that name no stream this socket holds.
    pub unknown_stream_resets: AtomicU64,
    /// ST_RESETs left unsent because the socket's reset budget was spent.
    pub resets_rate_limited: AtomicU64,
    /// Largest per-stream congestion window observed on this socket, in bytes.
    pub max_congestion_window_bytes: AtomicU64,
    pub live_virtual_sockets: AtomicU64,
    pub out_of_order_packets: AtomicU64,
    pub data_retransmissions: AtomicU64,
    pub synack_retransmissions: AtomicU64,
    pub retransmitted_bytes: AtomicU64,
    pub send_count: AtomicU64,
    pub send_errors: AtomicU64,
    pub send_poll_pending: AtomicU64,
    pub send_window_exhausted: AtomicU64,
    pub max_retransmissions_reached: AtomicU64,
    pub sent_bytes: AtomicU64,
    pub sent_control_packets: AtomicU64,
    pub unsent_control_packets: AtomicU64,
    pub incoming_already_acked_data_packets: AtomicU64,
    pub recovery_retransmitted_segments_count: AtomicU64,
    pub recovery_enter_count: AtomicU64,
    pub rto_timeouts_count: AtomicU64,
    pub recovery_transmitted_new_segments_count: AtomicU64,
    pub recovery_rto_during_recovery_count: AtomicU64,
}

/// Relaxed counters for a socket, aggregated across its streams.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct UtpStatsSnapshot {
    pub delay_samples_above_target: u64,
    pub delay_samples_below_target: u64,
    pub packets_lost: u64,
    pub timeouts: u64,
    pub invalid_packets: u64,
    pub redundant_packets: u64,
    pub syn_flood_dropped: u64,
    pub syn_rst: u64,
    pub max_congestion_window_bytes: u64,
}

impl UtpStats {
    pub fn snapshot(&self) -> UtpStatsSnapshot {
        UtpStatsSnapshot {
            delay_samples_above_target: self.delay_samples_above_target.load(Ordering::Relaxed),
            delay_samples_below_target: self.delay_samples_below_target.load(Ordering::Relaxed),
            packets_lost: self.packets_lost.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            invalid_packets: self.invalid_packets.load(Ordering::Relaxed),
            redundant_packets: self.redundant_packets.load(Ordering::Relaxed),
            syn_flood_dropped: self.syn_flood_dropped.load(Ordering::Relaxed),
            syn_rst: self.syn_rst.load(Ordering::Relaxed),
            max_congestion_window_bytes: self.max_congestion_window_bytes.load(Ordering::Relaxed),
        }
    }
}

impl std::ops::AddAssign for UtpStatsSnapshot {
    fn add_assign(&mut self, other: Self) {
        self.delay_samples_above_target = self
            .delay_samples_above_target
            .saturating_add(other.delay_samples_above_target);
        self.delay_samples_below_target = self
            .delay_samples_below_target
            .saturating_add(other.delay_samples_below_target);
        self.packets_lost = self.packets_lost.saturating_add(other.packets_lost);
        self.timeouts = self.timeouts.saturating_add(other.timeouts);
        self.invalid_packets = self.invalid_packets.saturating_add(other.invalid_packets);
        self.redundant_packets = self
            .redundant_packets
            .saturating_add(other.redundant_packets);
        self.syn_flood_dropped = self
            .syn_flood_dropped
            .saturating_add(other.syn_flood_dropped);
        self.syn_rst = self.syn_rst.saturating_add(other.syn_rst);
        self.max_congestion_window_bytes = self
            .max_congestion_window_bytes
            .max(other.max_congestion_window_bytes);
    }
}

pub(crate) trait Counter {
    fn increment(&self, by: u64);
    fn decrement(&self, by: u64);
}

impl Counter for AtomicU64 {
    #[inline]
    fn increment(&self, by: u64) {
        self.fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    fn decrement(&self, by: u64) {
        self.fetch_sub(by, Ordering::Relaxed);
    }
}
