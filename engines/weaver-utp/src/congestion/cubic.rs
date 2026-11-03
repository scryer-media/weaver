// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::{f64, time::Duration};

use tokio::time::Instant;

use crate::rtte::RttEstimator;

use super::CongestionController;

// Constants for the Cubic congestion control algorithm.
const BETA_CUBIC: f64 = 0.7;
const C: f64 = 0.4;

#[derive(Clone, Copy)]
pub struct Cubic {
    // All wnd units are in mss, as per CUBIC
    cwnd: f64,
    ssthresh: f64,

    k: f64, // CUBIC: time required to get to w_max
    w_max: f64,
    w_max_last: f64, // fast convergence

    mss: usize,
    last_congestion_event: Instant,

    // Remote window. Limits the window size
    rwnd: f64,
}

impl core::fmt::Debug for Cubic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cwnd={},cwnd_mss={:.2},sshthresh_mss:{:.2}:w_max:{:.2},mss:{}",
            self.window(),
            self.cwnd,
            self.ssthresh,
            self.w_max,
            self.mss
        )
    }
}

impl PartialEq for Cubic {
    fn eq(&self, other: &Self) -> bool {
        self.cwnd == other.cwnd && self.ssthresh == other.ssthresh
    }
}

impl Cubic {
    pub fn new(now: Instant, mss: usize) -> Cubic {
        Cubic {
            cwnd: 2.,
            ssthresh: f64::INFINITY,

            k: 0.,
            w_max: 0.,
            w_max_last: 0.,
            last_congestion_event: now,

            rwnd: 0.,
            mss,
        }
    }
}

impl CongestionController for Cubic {
    fn window(&self) -> usize {
        (self.cwnd.max(2.).min(self.rwnd) * self.mss as f64) as usize
    }

    fn sshthresh(&self) -> usize {
        (self.ssthresh * self.mss as f64) as usize
    }

    fn on_retransmission_timeout(&mut self, _now: Instant) {
        self.ssthresh = (self.cwnd * BETA_CUBIC).max(2.);
        self.w_max = self.cwnd;
        self.cwnd = 1.
    }

    fn on_enter_recovery(&mut self, now: Instant) {
        self.w_max = self.cwnd;
        self.cwnd *= BETA_CUBIC;
        self.ssthresh = self.cwnd.max(2.);

        self.last_congestion_event = now;

        // Fast convergence.
        if self.w_max < self.w_max_last {
            self.w_max_last = self.w_max;
            self.w_max *= (1. + BETA_CUBIC) / 2.;
        } else {
            self.w_max_last = self.w_max;
        }

        self.k = calc_k(self.w_max);
    }

    fn on_recovered(&mut self, new_cwnd_bytes: usize, new_sshthresh: usize) {
        let rec_cwnd = new_cwnd_bytes as f64 / self.mss as f64;
        self.cwnd = rec_cwnd.min(self.rwnd).max(2.);
        self.ssthresh = new_sshthresh as f64 / self.mss as f64;
    }

    fn on_ack(&mut self, now: Instant, len: usize, rtte: &RttEstimator) {
        if len == 0 {
            return;
        }

        if self.cwnd >= self.rwnd {
            return;
        }

        if self.cwnd < self.ssthresh {
            // Slow start
            self.cwnd += len as f64 / self.mss as f64;
        } else {
            // During congestion avoidance, window is computed using CUBIC.
            let t = now - self.last_congestion_event;
            let w_cubic_v = w_cubic(t, self.k, self.w_max);
            let rtt = rtte.roundtrip_time();
            let w_est_v = w_est(t, rtt, self.w_max);

            if w_cubic_v < w_est_v {
                // TCP friendly region
                self.cwnd = w_est_v;
            } else {
                // Concave and convex regions
                self.cwnd += (w_cubic(t + rtt, self.k, self.w_max) - self.cwnd) / self.cwnd;
            }
        }
        self.cwnd = self.cwnd.min(self.rwnd).max(2.);
    }

    fn set_remote_window(&mut self, win: usize) {
        self.rwnd = win as f64 / self.mss as f64
    }

    fn smss(&self) -> usize {
        self.mss
    }

    fn set_mss(&mut self, mss: usize) {
        if self.mss != mss {
            let rescale = self.mss as f64 / mss as f64;
            self.cwnd *= rescale;
            self.ssthresh *= rescale;
            self.w_max *= rescale;
            self.w_max_last *= rescale;
            self.mss = mss;
        }
    }
}

// K is the number of seconds required to get back to w_max.
fn calc_k(w_max_in_mss_units: f64) -> f64 {
    const FACTOR: f64 = (1. - BETA_CUBIC) / C;
    (w_max_in_mss_units * FACTOR).cbrt()
}

fn w_cubic(t: Duration, k: f64, w_max_in_mss_units: f64) -> f64 {
    C * (t.as_secs_f64() - k).powf(3.) + w_max_in_mss_units
}

fn w_est(t: Duration, rtt: Duration, w_max_in_mss_units: f64) -> f64 {
    // [3*(1-beta_cubic)/(1+beta_cubic)]
    const FACTOR: f64 = 3. * (1. - BETA_CUBIC) / (1. + BETA_CUBIC); // 0.52
    w_max_in_mss_units * BETA_CUBIC + FACTOR * (t.as_secs_f64() / rtt.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;
    use tracing::trace;

    use crate::{
        congestion::{
            CongestionController,
            cubic::{BETA_CUBIC, calc_k, w_cubic},
        },
        rtte::RttEstimator,
        test_util::setup_test_logging,
    };

    use super::Cubic;

    #[test]
    fn test_w_cubic_zero() {
        for w_max_mss in 0usize..=1000 {
            let w_max_mss = w_max_mss as f64;
            let k = calc_k(w_max_mss);
            let w = w_cubic(Duration::from_secs(0), k, w_max_mss);
            assert!(
                (w - w_max_mss * BETA_CUBIC).abs() <= 0.01,
                "{w} != {}",
                w_max_mss * BETA_CUBIC
            );
        }
    }

    #[test]
    fn test_cubic_playground() {
        setup_test_logging();
        let mut now = Instant::now();
        let rtt = Duration::from_millis(50);
        let mut rtte = RttEstimator::default();
        let mut cubic = Cubic::new(now, 1500);
        cubic.set_remote_window(1024 * 1024);

        dbg!(&cubic);

        rtte.sample(rtt);

        for _ in 0..50 {
            now += rtt;
            cubic.on_ack(now, 1500, &rtte);
        }
        trace!(?cubic, "cubic after 50 MSS packets");

        now += rtt * 3;
        cubic.on_enter_recovery(now);
        trace!(?cubic, "cubic after triple duplicate acks");
        // Pretend there was a fast retransmit here.

        for _ in 0..500 {
            now += rtt;
            cubic.on_ack(now, 1500, &rtte);
        }

        trace!(?cubic, "cubic after 250 acked packets");

        now += Duration::from_secs(1);
        cubic.on_retransmission_timeout(now);

        trace!(?cubic, "cubic after RTO");

        for _ in 0..50 {
            now += rtt;
            cubic.on_ack(now, 1500, &rtte);
        }

        trace!(?cubic, "cubic after 50 acked packets");
    }
}
