// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

pub mod cubic;
pub mod ledbat;
pub mod tracing;

use std::time::Duration;

use tokio::time::Instant;

use crate::rtte::RttEstimator;

/// A one-way delay sample that came with acknowledged data.
#[derive(Debug, Clone, Copy)]
pub struct DelaySample {
    pub delay: Duration,
    /// Bytes this packet acknowledged, selectively or not.
    pub acked_bytes: usize,
    pub in_flight_before: usize,
    pub in_flight_after: usize,
    /// The largest segment the sender may send next.
    pub segment: usize,
}

#[allow(unused_variables)]
pub trait CongestionController: Send + Sync + core::fmt::Debug {
    /// Returns the number of bytes that can be sent.
    fn window(&self) -> usize;

    fn sshthresh(&self) -> usize;

    fn set_mss(&mut self, mss: usize);

    fn smss(&self) -> usize;

    fn on_recovered(&mut self, new_cwnd_bytes: usize, new_sshthresh: usize);

    /// Increase the window on ACK
    fn on_ack(&mut self, now: Instant, len: usize, rtt: &RttEstimator);

    // NOT fast retransmit
    // flight_size: bytes sent and not yet acknowledged
    fn on_retransmission_timeout(&mut self, now: Instant);

    fn on_enter_recovery(&mut self, now: Instant);

    fn set_remote_window(&mut self, win: usize);

    fn on_delay_sample(&mut self, sample: DelaySample) {}

    /// A timeout passed with nothing of ours in flight.
    fn on_idle_timeout(&mut self) {}
}
