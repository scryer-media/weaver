// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Delay-based congestion control: the window grows while our packets'
// one-way delay sits under the target and shrinks above it, so a uTP
// stream yields to other traffic on the same link before it fills the
// queues. The window is kept in bytes with sixteen fractional bits.

use std::time::Duration;

use tokio::time::Instant;

use super::{CongestionController, DelaySample};
use crate::rtte::RttEstimator;

const FRAC: u32 = 16;
// The window a stream starts with: one Ethernet frame.
const INITIAL_WINDOW: i64 = 1500;

/// LEDBAT's tunables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedbatParams {
    /// The one-way delay the window steers toward.
    pub target_delay: Duration,
    /// Bytes the window grows by per round trip at zero delay.
    pub gain_factor: u32,
    /// The window after a loss, as a percentage of the window before it.
    pub loss_multiplier: u8,
    /// The least time between two loss cuts.
    pub cwnd_reduce_timer: Duration,
}

impl Default for LedbatParams {
    fn default() -> Self {
        Self {
            target_delay: Duration::from_millis(100),
            gain_factor: 3000,
            loss_multiplier: 50,
            cwnd_reduce_timer: Duration::from_millis(100),
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub struct Ledbat {
    params: LedbatParams,
    // Bytes << FRAC.
    cwnd: i64,
    // Bytes; 0 is none yet.
    ssthresh: usize,
    slow_start: bool,
    mss: usize,
    rwnd: usize,
}

impl core::fmt::Debug for Ledbat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cwnd={},ssthresh={},slow_start={},mss={}",
            self.cwnd_bytes(),
            self.ssthresh,
            self.slow_start,
            self.mss
        )
    }
}

impl Ledbat {
    pub fn new(mss: usize, params: LedbatParams) -> Self {
        let mut ledbat = Self {
            params,
            cwnd: INITIAL_WINDOW << FRAC,
            ssthresh: 0,
            slow_start: true,
            mss,
            rwnd: usize::MAX,
        };
        ledbat.floor();
        ledbat
    }

    fn cwnd_bytes(&self) -> usize {
        (self.cwnd >> FRAC) as usize
    }

    fn floor(&mut self) {
        let floor = (self.mss as i64) << FRAC;
        if self.cwnd < floor {
            self.cwnd = floor;
        }
    }

    #[cfg(test)]
    fn is_slow_start(&self) -> bool {
        self.slow_start
    }

    // Cuts the window by the loss multiplier, never under one segment;
    // slow start ends at the cut window.
    pub(crate) fn cut(&mut self) {
        self.cwnd = self.cwnd * i64::from(self.params.loss_multiplier) / 100;
        self.floor();
        if self.slow_start {
            self.ssthresh = self.cwnd_bytes();
            self.slow_start = false;
        }
    }
}

impl CongestionController for Ledbat {
    fn window(&self) -> usize {
        self.cwnd_bytes().min(self.rwnd).max(self.mss)
    }

    fn sshthresh(&self) -> usize {
        self.cwnd_bytes()
    }

    fn set_mss(&mut self, mss: usize) {
        self.mss = mss;
        self.floor();
    }

    fn smss(&self) -> usize {
        self.mss
    }

    // Loss recovery ends at the window the loss left.
    fn on_recovered(&mut self, _new_cwnd_bytes: usize, _new_sshthresh: usize) {}

    // The window moves on delay samples alone.
    fn on_ack(&mut self, _now: Instant, _len: usize, _rtt: &RttEstimator) {}

    fn on_delay_sample(&mut self, sample: DelaySample) {
        if sample.acked_bytes == 0 || sample.in_flight_before == 0 {
            return;
        }
        let target = (self.params.target_delay.as_micros() as i64).max(1);
        let delay = sample.delay.as_micros() as i64;
        let acked = sample.acked_bytes as i64;

        let saturated = (sample.in_flight_after + sample.acked_bytes + sample.segment.max(self.mss))
            as i64
            > self.cwnd >> FRAC;
        let window_factor = (acked << FRAC) / sample.in_flight_before as i64;
        let delay_factor = ((target - delay) << FRAC) / target;

        if delay >= target && self.slow_start {
            self.ssthresh = self.cwnd_bytes() / 2;
            self.slow_start = false;
        }

        let linear = ((window_factor * delay_factor) >> FRAC) * i64::from(self.params.gain_factor);
        let gain = if !saturated {
            0
        } else if self.slow_start {
            let exponential = acked << FRAC;
            if self.ssthresh != 0 && ((self.cwnd + exponential) >> FRAC) as usize > self.ssthresh {
                self.slow_start = false;
                linear
            } else {
                exponential.max(linear)
            }
        } else {
            linear
        };

        self.cwnd = self.cwnd.saturating_add(gain);
        self.floor();
    }

    fn on_retransmission_timeout(&mut self, _now: Instant) {
        self.cwnd = (self.mss as i64) << FRAC;
        self.slow_start = true;
    }

    // An idle direction keeps two thirds of its window, then slow-starts.
    fn on_idle_timeout(&mut self) {
        self.cwnd = self.cwnd * 2 / 3;
        self.floor();
        self.slow_start = true;
    }

    fn on_enter_recovery(&mut self, _now: Instant) {
        self.cut();
    }

    fn set_remote_window(&mut self, win: usize) {
        self.rwnd = win;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MSS: usize = 1000;

    fn ledbat() -> Ledbat {
        let mut l = Ledbat::new(MSS, LedbatParams::default());
        l.set_remote_window(usize::MAX);
        l
    }

    fn sample(delay_ms: u64, acked: usize, before: usize) -> DelaySample {
        DelaySample {
            delay: Duration::from_millis(delay_ms),
            acked_bytes: acked,
            in_flight_before: before,
            in_flight_after: before - acked,
            segment: MSS,
        }
    }

    #[test]
    fn slow_start_grows_by_what_was_acknowledged() {
        let mut l = ledbat();
        let start = l.window();
        // Just under the target the linear gain is a few bytes.
        l.on_delay_sample(sample(99, MSS, start));
        assert_eq!(l.window(), start + MSS);
        assert!(l.is_slow_start());
    }

    #[test]
    fn a_delay_at_the_target_ends_slow_start_at_half_the_window() {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w));
        }
        let before = l.window();
        l.on_delay_sample(sample(100, MSS, before));
        assert!(!l.is_slow_start());
        assert_eq!(l.ssthresh, before / 2);
        // At the target the linear gain is nothing.
        assert_eq!(l.window(), before);
    }

    #[test]
    fn above_the_target_the_window_shrinks() {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w));
        }
        let before = l.window();
        l.on_delay_sample(sample(200, MSS, before));
        // window_factor = 1000/before, delay_factor = -1, gain 3000.
        let expected = before as i64 - 3000 * MSS as i64 / before as i64;
        assert!(
            (l.window() as i64 - expected).abs() <= 2,
            "{l:?} {expected}"
        );
    }

    #[test]
    fn an_unsaturated_window_does_not_move() {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w));
        }
        let before = l.window();
        // Little in flight: the window is not what limits the sender.
        l.on_delay_sample(sample(10, MSS, 2 * MSS));
        assert_eq!(l.window(), before);
        l.on_delay_sample(sample(300, MSS, 2 * MSS));
        assert_eq!(l.window(), before);
    }

    #[test]
    fn the_window_never_falls_under_one_segment() {
        let mut l = ledbat();
        for _ in 0..50 {
            let w = l.window();
            l.on_delay_sample(sample(5_000, w, w));
        }
        assert_eq!(l.window(), MSS);
    }

    #[test]
    fn a_loss_halves_the_window_and_ends_slow_start_there() {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w));
        }
        let before = l.window();
        l.on_enter_recovery(Instant::now());
        assert_eq!(l.window(), before / 2);
        assert!(!l.is_slow_start());
        assert_eq!(l.ssthresh, before / 2);
    }

    #[test]
    fn slow_start_stops_at_the_threshold() {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w));
        }
        l.on_enter_recovery(Instant::now());
        l.on_retransmission_timeout(Instant::now());
        assert!(l.is_slow_start());
        assert_eq!(l.window(), MSS);
        let threshold = l.ssthresh;
        while l.is_slow_start() {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w));
        }
        assert!(l.window() <= threshold + MSS);
    }

    #[test]
    fn an_idle_timeout_keeps_two_thirds_and_slow_starts() {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w));
        }
        l.on_enter_recovery(Instant::now());
        let before = l.window();
        l.on_idle_timeout();
        assert!(l.window().abs_diff(before * 2 / 3) <= 1, "{l:?}");
        assert!(l.is_slow_start());
        for _ in 0..20 {
            l.on_idle_timeout();
        }
        assert_eq!(l.window(), MSS);
    }
}
