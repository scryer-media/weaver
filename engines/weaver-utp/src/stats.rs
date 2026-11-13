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
    /// Streams ended because their packets went unanswered: a packet
    /// resent the most times allowed, the timeouts in a row used up, or
    /// the FIN never acknowledged.
    pub streams_given_up: AtomicU64,
    /// Reads of the socket that ended with it empty after taking packets
    /// for a stream, each marking that stream's packets as drained.
    pub receive_drains: AtomicU64,
    /// Marks made because the next packet read was for another stream.
    pub stream_switch_marks: AtomicU64,
    /// Drain marks a stream reached with more of its packets already
    /// queued behind the mark.
    pub drain_marks_with_packets_queued: AtomicU64,
    pub sent_bytes: AtomicU64,
    pub sent_control_packets: AtomicU64,
    /// ACKs sent because the reader freed receive window.
    pub window_update_acks: AtomicU64,
    /// Every ACK packet sent on its own (not carried by a data packet).
    pub ack_packets_sent: AtomicU64,
    /// ST_DATA packets received, valid and in window, in order or not.
    pub data_packets_received: AtomicU64,
    /// ST_DATA packets the socket took to send, resends included.
    pub data_packets_sent: AtomicU64,
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
    pub immediate_acks: u64,
    pub delayed_acks: u64,
    pub window_update_acks: u64,
    pub ack_packets_sent: u64,
    pub data_packets_received: u64,
    pub data_packets_sent: u64,
    pub sent_control_packets: u64,
    pub send_window_exhausted: u64,
    pub data_retransmissions: u64,
    pub duplicate_acks_received: u64,
    pub out_of_order_packets: u64,
    pub streams_given_up: u64,
    pub receive_drains: u64,
    pub stream_switch_marks: u64,
    pub drain_marks_with_packets_queued: u64,
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
            immediate_acks: self.immediate_acks.load(Ordering::Relaxed),
            delayed_acks: self.delayed_acks.load(Ordering::Relaxed),
            window_update_acks: self.window_update_acks.load(Ordering::Relaxed),
            ack_packets_sent: self.ack_packets_sent.load(Ordering::Relaxed),
            data_packets_received: self.data_packets_received.load(Ordering::Relaxed),
            data_packets_sent: self.data_packets_sent.load(Ordering::Relaxed),
            sent_control_packets: self.sent_control_packets.load(Ordering::Relaxed),
            send_window_exhausted: self.send_window_exhausted.load(Ordering::Relaxed),
            data_retransmissions: self.data_retransmissions.load(Ordering::Relaxed),
            duplicate_acks_received: self.duplicate_acks_received.load(Ordering::Relaxed),
            out_of_order_packets: self.out_of_order_packets.load(Ordering::Relaxed),
            streams_given_up: self.streams_given_up.load(Ordering::Relaxed),
            receive_drains: self.receive_drains.load(Ordering::Relaxed),
            stream_switch_marks: self.stream_switch_marks.load(Ordering::Relaxed),
            drain_marks_with_packets_queued: self
                .drain_marks_with_packets_queued
                .load(Ordering::Relaxed),
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
        self.immediate_acks = self.immediate_acks.saturating_add(other.immediate_acks);
        self.delayed_acks = self.delayed_acks.saturating_add(other.delayed_acks);
        self.window_update_acks = self
            .window_update_acks
            .saturating_add(other.window_update_acks);
        self.ack_packets_sent = self.ack_packets_sent.saturating_add(other.ack_packets_sent);
        self.data_packets_received = self
            .data_packets_received
            .saturating_add(other.data_packets_received);
        self.data_packets_sent = self
            .data_packets_sent
            .saturating_add(other.data_packets_sent);
        self.sent_control_packets = self
            .sent_control_packets
            .saturating_add(other.sent_control_packets);
        self.send_window_exhausted = self
            .send_window_exhausted
            .saturating_add(other.send_window_exhausted);
        self.data_retransmissions = self
            .data_retransmissions
            .saturating_add(other.data_retransmissions);
        self.duplicate_acks_received = self
            .duplicate_acks_received
            .saturating_add(other.duplicate_acks_received);
        self.out_of_order_packets = self
            .out_of_order_packets
            .saturating_add(other.out_of_order_packets);
        self.streams_given_up = self.streams_given_up.saturating_add(other.streams_given_up);
        self.receive_drains = self.receive_drains.saturating_add(other.receive_drains);
        self.stream_switch_marks = self
            .stream_switch_marks
            .saturating_add(other.stream_switch_marks);
        self.drain_marks_with_packets_queued = self
            .drain_marks_with_packets_queued
            .saturating_add(other.drain_marks_with_packets_queued);
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
