// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Decides when a stream's data goes out; the congestion window still
// decides how much may be in flight.
//
// A token bucket fills at a rate set from the window: twice the window per
// smoothed round trip in slow start, five quarters of it after. ACKs wake
// the stream; what each wake sends is what the bucket holds, so data leaves
// at the window's rate whether an ACK, a timer or the writer woke it. The
// bucket holds what the rate adds in one millisecond, the timer's step, so
// a stream woken no more often than its timer still sends at that rate; it
// holds no less than sixteen packets and no more than half the window, so
// no instant sends the whole window at once.
//
// libtorrent v2.0.15 does not pace: each ACK runs `while (send_pkt());`
// (utp_stream.cpp:3023) and a write does the same (757, 779), so one pass
// sends all the window has room for, in slow start what the ACK freed and
// as much again (3290-3308). A receiver that drains its queue in lumps
// then gets the window in lumps, which the bucket here spreads out.

use std::time::Duration;

use tokio::time::Instant;

// The bucket fills at these multiples of the window per round trip, in
// eighths.
const SLOW_START_GAIN_8THS: u128 = 16;
const AVOIDANCE_GAIN_8THS: u128 = 10;
// The least packets the bucket holds.
pub(crate) const PACE_BURST_PACKETS: usize = 16;
// The coarsest step at which the stream's timer wakes it.
const PACE_TICK: Duration = Duration::from_millis(1);

#[derive(Debug, Default)]
pub(crate) struct Pacer {
    // Bytes that may still go out now.
    budget: usize,
    // When the budget was last brought up to date; none before the first
    // send.
    refilled_at: Option<Instant>,
    // The window, round trip and gain the last refill used.
    window: usize,
    srtt: Duration,
    gain_8ths: u128,
    mss: usize,
}

impl Pacer {
    fn srtt_nanos(&self) -> u128 {
        self.srtt.as_nanos().max(1)
    }

    // The bytes the bucket fills by in `elapsed`.
    fn fill(&self, elapsed: Duration) -> u128 {
        elapsed.as_nanos() * self.gain_8ths * self.window as u128 / (8 * self.srtt_nanos())
    }

    pub(crate) fn capacity(&self) -> usize {
        let tick = usize::try_from(self.fill(PACE_TICK)).unwrap_or(usize::MAX);
        let floor = PACE_BURST_PACKETS.saturating_mul(self.mss);
        tick.min(self.window / 2).max(floor)
    }

    /// Brings the budget up to `now` for a window of `window` bytes per
    /// round trip of `srtt`, faster while the window is in slow start.
    pub fn refill(
        &mut self,
        now: Instant,
        window: usize,
        srtt: Duration,
        mss: usize,
        slow_start: bool,
    ) {
        self.window = window.max(mss);
        self.srtt = srtt;
        self.mss = mss.max(1);
        self.gain_8ths = if slow_start {
            SLOW_START_GAIN_8THS
        } else {
            AVOIDANCE_GAIN_8THS
        };
        let capacity = self.capacity();
        let Some(at) = self.refilled_at else {
            self.budget = capacity;
            self.refilled_at = Some(now);
            return;
        };
        let added = self.fill(now.saturating_duration_since(at));
        if added > 0 {
            let added = usize::try_from(added).unwrap_or(usize::MAX);
            self.budget = self.budget.saturating_add(added).min(capacity);
            self.refilled_at = Some(now);
        } else {
            self.budget = self.budget.min(capacity);
        }
    }

    pub fn cost(&self, payload: usize) -> usize {
        payload.max(self.mss)
    }

    /// Whether a packet of `payload` bytes may go out now.
    pub fn allows(&self, payload: usize) -> bool {
        self.budget >= self.cost(payload)
    }

    /// Takes a sent packet of `payload` bytes out of the budget. A packet
    /// that must go out whatever the budget leaves it empty.
    pub fn charge(&mut self, payload: usize) {
        self.budget = self.budget.saturating_sub(self.cost(payload));
    }

    /// When the budget covers a packet of `payload` bytes.
    pub fn ready_at(&self, payload: usize) -> Option<Instant> {
        let at = self.refilled_at?;
        let short = self.cost(payload).saturating_sub(self.budget) as u128;
        let per = (self.gain_8ths * self.window as u128).max(1);
        let nanos = (short * 8 * self.srtt_nanos()).div_ceil(per);
        let wait = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
        Some(at + wait)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MSS: usize = 1000;

    fn pacer(now: Instant, window: usize, srtt: Duration, slow_start: bool) -> Pacer {
        let mut pacer = Pacer::default();
        pacer.refill(now, window, srtt, MSS, slow_start);
        pacer
    }

    fn drain(pacer: &mut Pacer) -> usize {
        let mut sent = 0;
        while pacer.allows(MSS) {
            pacer.charge(MSS);
            sent += 1;
        }
        sent
    }

    #[test]
    fn a_small_window_holds_the_burst_floor() {
        let now = Instant::now();
        let mut pacer = pacer(now, 10 * MSS, Duration::from_millis(100), true);
        assert_eq!(drain(&mut pacer), PACE_BURST_PACKETS);
    }

    #[test]
    fn slow_start_fills_at_twice_the_window_per_round_trip() {
        let now = Instant::now();
        let srtt = Duration::from_millis(100);
        let mut pacer = pacer(now, 100 * MSS, srtt, true);
        drain(&mut pacer);
        assert!(!pacer.allows(MSS));
        // Two packets per millisecond: the next is ready in half of one.
        assert_eq!(pacer.ready_at(MSS), Some(now + Duration::from_micros(500)));
        pacer.refill(now + Duration::from_millis(5), 100 * MSS, srtt, MSS, true);
        assert_eq!(drain(&mut pacer), 10);
    }

    #[test]
    fn after_slow_start_it_fills_at_five_quarters_of_the_window() {
        let now = Instant::now();
        // A hundred packets a millisecond.
        let srtt = Duration::from_millis(10);
        let mut pacer = pacer(now, 800 * MSS, srtt, false);
        drain(&mut pacer);
        pacer.refill(
            now + Duration::from_micros(400),
            800 * MSS,
            srtt,
            MSS,
            false,
        );
        assert_eq!(drain(&mut pacer), 40);
    }

    #[test]
    fn an_idle_bucket_never_holds_more_than_its_capacity() {
        let now = Instant::now();
        let srtt = Duration::from_millis(100);
        let mut pacer = pacer(now, 100 * MSS, srtt, true);
        pacer.refill(now + Duration::from_secs(60), 100 * MSS, srtt, MSS, true);
        assert_eq!(drain(&mut pacer), PACE_BURST_PACKETS);
    }

    // On a short round trip the bucket holds a millisecond of the rate, so
    // a stream woken once a millisecond keeps it; never more than half the
    // window.
    #[test]
    fn a_fast_path_holds_a_tick_of_the_rate_up_to_half_the_window() {
        let now = Instant::now();
        // 2000 packets per 10 ms in slow start: 400 a millisecond.
        let srtt = Duration::from_millis(10);
        let mut slower = pacer(now, 2000 * MSS, srtt, true);
        slower.refill(now + Duration::from_millis(5), 2000 * MSS, srtt, MSS, true);
        assert_eq!(drain(&mut slower), 400);
        for srtt in [
            Duration::from_millis(1),
            Duration::from_micros(100),
            Duration::ZERO,
        ] {
            let mut fast = pacer(now, 1000 * MSS, srtt, true);
            fast.refill(now + Duration::from_millis(1), 1000 * MSS, srtt, MSS, true);
            assert_eq!(drain(&mut fast), 500, "srtt {srtt:?}");
        }
    }

    #[test]
    fn small_packets_cost_a_segment() {
        let now = Instant::now();
        let mut pacer = pacer(now, 10 * MSS, Duration::from_secs(1), true);
        let mut sent = 0;
        while pacer.allows(1) {
            pacer.charge(1);
            sent += 1;
        }
        assert_eq!(sent, PACE_BURST_PACKETS);
    }
}
