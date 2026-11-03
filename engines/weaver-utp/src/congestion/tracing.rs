// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use tokio::time::Instant;

use crate::{constants::CONGESTION_TRACING_LOG_LEVEL, rtte::RttEstimator};

use super::CongestionController;

pub struct TracingController<I> {
    inner: I,
}

impl<I: std::fmt::Debug> std::fmt::Debug for TracingController<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<I> TracingController<I> {
    pub fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<I> CongestionController for TracingController<I>
where
    I: CongestionController + PartialEq + Copy + 'static,
{
    fn window(&self) -> usize {
        self.inner.window()
    }

    fn on_ack(&mut self, now: Instant, len: usize, rtt: &RttEstimator) {
        log_every_ms_if_changed!(
            500,
            CONGESTION_TRACING_LOG_LEVEL,
            "on_ack",
            self,
            |s| s.inner,
            |s| s.inner.on_ack(now, len, rtt)
        );
    }

    fn on_retransmission_timeout(&mut self, now: Instant) {
        log_if_changed!(
            CONGESTION_TRACING_LOG_LEVEL,
            "on_rto",
            self,
            |s| s.inner,
            |s| s.inner.on_retransmission_timeout(now)
        );
    }

    fn on_enter_recovery(&mut self, now: Instant) {
        log_if_changed!(
            CONGESTION_TRACING_LOG_LEVEL,
            "on_enter_recovery",
            self,
            |s| s.inner,
            |s| s.inner.on_enter_recovery(now)
        );
    }

    fn on_delay_sample(&mut self, sample: super::DelaySample) {
        let before = self.inner.window();
        self.inner.on_delay_sample(sample);
        tracing::event!(
            CONGESTION_TRACING_LOG_LEVEL,
            ?sample,
            before,
            after = self.inner.window(),
            "on_delay_sample"
        );
    }

    fn set_remote_window(&mut self, win: usize) {
        self.inner.set_remote_window(win);
    }

    fn sshthresh(&self) -> usize {
        self.inner.sshthresh()
    }

    fn on_recovered(&mut self, new_cwnd_bytes: usize, new_sshthresh: usize) {
        log_if_changed!(
            CONGESTION_TRACING_LOG_LEVEL,
            "on_recovered",
            self,
            |s| s.inner,
            |s| s.inner.on_recovered(new_cwnd_bytes, new_sshthresh)
        );
    }

    fn smss(&self) -> usize {
        self.inner.smss()
    }

    fn set_mss(&mut self, mss: usize) {
        log_if_changed!(
            CONGESTION_TRACING_LOG_LEVEL,
            "set_mss",
            self,
            |s| s.inner,
            |s| s.inner.set_mss(mss)
        );
    }
}
