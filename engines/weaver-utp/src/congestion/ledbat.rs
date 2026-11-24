// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Delay-based congestion control: the window grows while our packets'
// one-way delay sits under the target and shrinks above it, so a uTP
// stream yields to other traffic on the same link before it fills the
// queues. The window is kept in bytes with sixteen fractional bits.

use std::time::Duration;

use tokio::time::Instant;

use super::DelaySample;
use crate::{
    seq_nr::SeqNr,
    stats::{Counter, UtpStats},
};

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
    // A loss cuts the window only for a packet sent after this one, the
    // next new send when the window was last cut or timed out.
    loss_gate: Option<SeqNr>,
    // Nor before this.
    next_cut: Option<Instant>,
    // After a timer expiry, until the acknowledgement reaches this packet,
    // the highest sent before the expiry, the window grows by at most one
    // segment per ACK. ACKs in that time also report packets that arrived
    // before the expiry; counting them all would let the window, and the
    // resends behind it, leap far past what the path has shown it carries
    // (RFC 3465 section 2.3, from memory: L = 1 SMSS in slow start after a
    // retransmission timeout). libtorrent grows by every byte an ACK
    // removes, SACKed ones included (v2.0.15 utp_stream.cpp:2741-2747,
    // 3290).
    regrow_until: Option<SeqNr>,
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
            loss_gate: None,
            next_cut: None,
            regrow_until: None,
        };
        ledbat.floor();
        ledbat
    }

    pub(crate) fn cwnd_bytes(&self) -> usize {
        (self.cwnd >> FRAC) as usize
    }

    fn floor(&mut self) {
        let floor = (self.mss as i64) << FRAC;
        if self.cwnd < floor {
            self.cwnd = floor;
        }
    }

    // Tests that drive the machinery around the window open it this wide.
    #[cfg(test)]
    pub(crate) fn set_window(&mut self, bytes: usize) {
        self.cwnd = (bytes as i64) << FRAC;
        self.slow_start = false;
    }

    pub(crate) fn is_slow_start(&self) -> bool {
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

    /// Bytes that may be in flight.
    pub fn window(&self) -> usize {
        self.cwnd_bytes().min(self.rwnd).max(self.mss)
    }

    pub fn recovery_window(&self) -> usize {
        self.cwnd_bytes()
    }

    pub fn set_mss(&mut self, mss: usize) {
        self.mss = mss;
        self.floor();
    }

    pub fn on_delay_sample(&mut self, sample: DelaySample, stats: &UtpStats) {
        if sample.acked_bytes == 0 || sample.in_flight_before == 0 {
            return;
        }
        let target = (self.params.target_delay.as_micros() as i64).max(1);
        let delay = sample.delay.as_micros() as i64;
        if delay >= target {
            stats.delay_samples_above_target.increment(1);
        } else {
            stats.delay_samples_below_target.increment(1);
        }
        // An ACK acknowledges no more than was in flight when it came, so
        // one stretched across a rewound or partly SACKed flight does not
        // count as more than a round trip of growth.
        let mut acked = sample.acked_bytes.min(sample.in_flight_before) as i64;
        if self.regrow_until.is_some() {
            acked = acked.min(self.mss as i64);
        }
        let before = self.cwnd;

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

        // The window grows to no more than what was in flight plus the
        // larger of what this ACK acknowledged and one segment.
        let ceiling =
            ((sample.in_flight_before as i64).saturating_add(acked.max(self.mss as i64))) << FRAC;
        self.cwnd = self.cwnd.saturating_add(gain).min(before.max(ceiling));
        self.floor();
    }

    pub fn on_retransmission_timeout(&mut self, _now: Instant, next_seq_nr: SeqNr) {
        self.cwnd = (self.mss as i64) << FRAC;
        self.slow_start = true;
        // What was in flight is counted lost already.
        self.loss_gate = Some(next_seq_nr);
        self.regrow_until = Some(next_seq_nr - 1);
    }

    // An idle direction keeps two thirds of its window, then slow-starts.
    pub fn on_idle_timeout(&mut self) {
        self.cwnd = self.cwnd * 2 / 3;
        self.floor();
        self.slow_start = true;
    }

    // Losses come in bursts: one cut per window of packets, and no more
    // than one per cut timer.
    pub fn on_enter_recovery(&mut self, now: Instant, lost: SeqNr, next_seq_nr: SeqNr) {
        if self.loss_gate.is_some_and(|gate| !lost.is_after(gate)) {
            return;
        }
        if self.next_cut.is_some_and(|next| now <= next) {
            return;
        }
        self.next_cut = Some(now + self.params.cwnd_reduce_timer);
        self.loss_gate = Some(next_seq_nr);
        self.cut();
    }

    pub fn advance_loss_gate(&mut self, acknowledged: SeqNr) {
        if self
            .regrow_until
            .is_some_and(|until| acknowledged.is_at_or_after(until))
        {
            self.regrow_until = None;
        }
        if let Some(gate) = &mut self.loss_gate
            && acknowledged.is_after(*gate)
        {
            *gate = acknowledged;
        }
    }

    pub fn set_remote_window(&mut self, win: usize) {
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
    fn delay_sample_counters_follow_the_target() {
        let mut l = ledbat();
        let stats = UtpStats::default();
        for delay_ms in [20, 100, 200] {
            l.on_delay_sample(sample(delay_ms, MSS, l.window()), &stats);
        }
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.delay_samples_below_target, 1);
        assert_eq!(snapshot.delay_samples_above_target, 2);
    }

    #[test]
    fn slow_start_grows_by_what_was_acknowledged() {
        let mut l = ledbat();
        let start = l.window();
        // Just under the target the linear gain is a few bytes.
        l.on_delay_sample(sample(99, MSS, start), &UtpStats::default());
        assert_eq!(l.window(), start + MSS);
        assert!(l.is_slow_start());
    }

    #[test]
    fn a_delay_at_the_target_ends_slow_start_at_half_the_window() {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w), &UtpStats::default());
        }
        let before = l.window();
        l.on_delay_sample(sample(100, MSS, before), &UtpStats::default());
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
            l.on_delay_sample(sample(10, MSS, w), &UtpStats::default());
        }
        let before = l.window();
        l.on_delay_sample(sample(200, MSS, before), &UtpStats::default());
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
            l.on_delay_sample(sample(10, MSS, w), &UtpStats::default());
        }
        let before = l.window();
        // Little in flight: the window is not what limits the sender.
        l.on_delay_sample(sample(10, MSS, 2 * MSS), &UtpStats::default());
        assert_eq!(l.window(), before);
        l.on_delay_sample(sample(300, MSS, 2 * MSS), &UtpStats::default());
        assert_eq!(l.window(), before);
    }

    #[test]
    fn the_window_never_falls_under_one_segment() {
        let mut l = ledbat();
        for _ in 0..50 {
            let w = l.window();
            l.on_delay_sample(sample(5_000, w, w), &UtpStats::default());
        }
        assert_eq!(l.window(), MSS);
    }

    #[test]
    fn a_loss_halves_the_window_and_ends_slow_start_there() {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w), &UtpStats::default());
        }
        let before = l.window();
        l.on_enter_recovery(Instant::now(), 10.into(), 20.into());
        assert_eq!(l.window(), before / 2);
        assert!(!l.is_slow_start());
        assert_eq!(l.ssthresh, before / 2);
    }

    #[test]
    fn slow_start_stops_at_the_threshold() {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w), &UtpStats::default());
        }
        l.on_enter_recovery(Instant::now(), 10.into(), 20.into());
        l.on_retransmission_timeout(Instant::now(), 20.into());
        assert!(l.is_slow_start());
        assert_eq!(l.window(), MSS);
        let threshold = l.ssthresh;
        while l.is_slow_start() {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w), &UtpStats::default());
        }
        assert!(l.window() <= threshold + MSS);
    }

    #[test]
    fn an_idle_timeout_keeps_two_thirds_and_slow_starts() {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w), &UtpStats::default());
        }
        l.on_enter_recovery(Instant::now(), 10.into(), 20.into());
        let before = l.window();
        l.on_idle_timeout();
        assert!(l.window().abs_diff(before * 2 / 3) <= 1, "{l:?}");
        assert!(l.is_slow_start());
        for _ in 0..20 {
            l.on_idle_timeout();
        }
        assert_eq!(l.window(), MSS);
    }

    fn grown() -> Ledbat {
        let mut l = ledbat();
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w), &UtpStats::default());
        }
        l
    }

    #[test]
    fn losses_from_one_window_cut_it_once() {
        let mut l = grown();
        let now = Instant::now();
        let before = l.window();
        l.on_enter_recovery(now, 10.into(), 20.into());
        // Sent before the first loss was seen, lost after the cut timer.
        l.on_enter_recovery(now + Duration::from_secs(1), 15.into(), 25.into());
        assert_eq!(l.window(), before / 2);
        // Sent after it.
        l.on_enter_recovery(now + Duration::from_secs(2), 21.into(), 30.into());
        assert_eq!(l.window(), before / 4);
    }

    #[test]
    fn no_two_cuts_within_the_cut_timer() {
        let mut l = grown();
        let now = Instant::now();
        let before = l.window();
        l.on_enter_recovery(now, 10.into(), 20.into());
        l.on_enter_recovery(now + Duration::from_millis(100), 21.into(), 30.into());
        assert_eq!(l.window(), before / 2);
        l.on_enter_recovery(now + Duration::from_millis(101), 22.into(), 30.into());
        assert_eq!(l.window(), before / 4);
    }

    #[test]
    fn a_loss_from_before_a_timeout_does_not_cut() {
        let mut l = grown();
        let now = Instant::now();
        l.on_retransmission_timeout(now, 20.into());
        for _ in 0..8 {
            let w = l.window();
            l.on_delay_sample(sample(10, MSS, w), &UtpStats::default());
        }
        let before = l.window();
        l.on_enter_recovery(now + Duration::from_secs(1), 18.into(), 30.into());
        assert_eq!(l.window(), before);
    }

    // After a timeout only the resent segment is in flight. One ACK that
    // also covers fifty segments sent before the timeout grows the window
    // to two segments, not back to fifty-one; each round trip after that
    // doubles it.
    #[test]
    fn a_window_after_a_timeout_ramps_instead_of_snapping_back() {
        let mut l = grown();
        l.on_retransmission_timeout(Instant::now(), 20.into());
        assert_eq!(l.window(), MSS);
        let stretched = DelaySample {
            delay: Duration::from_millis(10),
            acked_bytes: 50 * MSS,
            in_flight_before: MSS,
            in_flight_after: 0,
            segment: MSS,
        };
        l.on_delay_sample(stretched, &UtpStats::default());
        assert_eq!(l.window(), 2 * MSS);
        l.advance_loss_gate(19.into());
        for round in 2..6 {
            let w = l.window();
            l.on_delay_sample(sample(10, w, w), &UtpStats::default());
            assert_eq!(l.window(), (1 << round) * MSS);
        }
    }

    // Until the acknowledgement reaches the highest packet sent before a
    // timeout, an ACK grows the window by one segment however much it
    // reports: one packet more per ACK, not a doubling.
    #[test]
    fn after_a_timeout_each_ack_grows_the_window_by_one_segment_until_the_old_flight_is_acked() {
        let mut l = grown();
        l.on_retransmission_timeout(Instant::now(), 20.into());
        let mut windows = vec![l.window()];
        for acknowledged in [10, 15, 18] {
            let w = l.window();
            l.on_delay_sample(sample(10, w, w), &UtpStats::default());
            l.advance_loss_gate(acknowledged.into());
            windows.push(l.window());
        }
        // The ACK of packet 19 is the last held to one segment.
        let w = l.window();
        l.on_delay_sample(sample(10, w, w), &UtpStats::default());
        l.advance_loss_gate(19.into());
        windows.push(l.window());
        for _ in 0..2 {
            let w = l.window();
            l.on_delay_sample(sample(10, w, w), &UtpStats::default());
            windows.push(l.window());
        }
        assert_eq!(
            windows,
            [1, 2, 3, 4, 5, 10, 20].map(|segments| segments * MSS)
        );
    }

    // One ACK never grows the window past what was in flight plus what it
    // acknowledged.
    #[test]
    fn one_ack_grows_the_window_by_no_more_than_it_acknowledged() {
        let mut l = grown();
        let before = l.window();
        let wide = DelaySample {
            delay: Duration::from_millis(10),
            acked_bytes: 4 * before,
            in_flight_before: before,
            in_flight_after: 0,
            segment: MSS,
        };
        l.on_delay_sample(wide, &UtpStats::default());
        assert_eq!(l.window(), 2 * before);
    }

    #[test]
    fn acknowledged_progress_keeps_the_loss_gate_in_the_live_sequence_range() {
        let mut l = grown();
        let now = Instant::now();
        l.on_enter_recovery(now, 65000.into(), 65500.into());
        let window = l.window();
        for acknowledged in [1000, 30000, 60000] {
            l.advance_loss_gate(acknowledged.into());
        }
        l.on_enter_recovery(now + Duration::from_secs(1), 60001.into(), 61000.into());
        assert_eq!(l.window(), window / 2);
    }
}
