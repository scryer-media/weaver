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
    pub cant_accept_syn: AtomicU64,
    pub sent_control_packets: AtomicU64,
    pub unsent_control_packets: AtomicU64,
    pub incoming_already_acked_data_packets: AtomicU64,
    pub recovery_retransmitted_segments_count: AtomicU64,
    pub recovery_enter_count: AtomicU64,
    pub rto_timeouts_count: AtomicU64,
    pub recovery_transmitted_new_segments_count: AtomicU64,
    pub recovery_rto_during_recovery_count: AtomicU64,
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
