// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

pub mod ledbat;

use std::time::Duration;

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
