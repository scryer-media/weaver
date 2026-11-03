// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

pub mod cubic;
pub mod tracing;

use tokio::time::Instant;

use crate::rtte::RttEstimator;

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
}
