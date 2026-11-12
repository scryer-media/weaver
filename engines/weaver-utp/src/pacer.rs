// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Spreads a stream's data packets across the round trip. The congestion
// window still decides how much may be in flight; the pacer only decides
// when it goes out, so a window that opens all at once does not land in
// the receiver's socket queue as one burst.
//
// A token bucket fills at twice the window per smoothed round trip, so a
// window that doubles each round trip in slow start is never held back. It
// holds at most a small burst: sixteen packets, or one timer tick of the
// rate when that is larger, so a stream woken once per tick still sends at
// the full rate. Each packet costs at least one segment, as a receiver's
// queue counts datagrams, not bytes.

use std::time::Duration;

use tokio::time::Instant;

// The bucket fills at this multiple of window per round trip.
const PACE_GAIN: u128 = 2;
// Packets the bucket holds at least.
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
    // The rate the last refill used, in bytes per round trip, and the
    // round trip itself.
    window: usize,
    srtt: Duration,
    mss: usize,
}

impl Pacer {
    fn srtt_nanos(&self) -> u128 {
        self.srtt.as_nanos().max(1)
    }

    // The bytes the bucket fills by in `elapsed`.
    fn fill(&self, elapsed: Duration) -> u128 {
        elapsed.as_nanos() * PACE_GAIN * self.window as u128 / self.srtt_nanos()
    }

    fn capacity(&self) -> usize {
        let tick = usize::try_from(self.fill(PACE_TICK)).unwrap_or(usize::MAX);
        tick.max(PACE_BURST_PACKETS.saturating_mul(self.mss))
    }

    /// Brings the budget up to `now` for a window of `window` bytes per
    /// round trip of `srtt`.
    pub fn refill(&mut self, now: Instant, window: usize, srtt: Duration, mss: usize) {
        self.window = window.max(mss);
        self.srtt = srtt;
        self.mss = mss.max(1);
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
        let per = PACE_GAIN * self.window as u128;
        let nanos = (short * self.srtt_nanos()).div_ceil(per);
        let wait = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
        Some(at + wait)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MSS: usize = 1000;

    fn pacer(now: Instant, window: usize, srtt: Duration) -> Pacer {
        let mut pacer = Pacer::default();
        pacer.refill(now, window, srtt, MSS);
        pacer
    }

    #[test]
    fn a_fresh_bucket_holds_the_burst_allowance() {
        let now = Instant::now();
        let mut pacer = pacer(now, 1000 * MSS, Duration::from_secs(1));
        let mut sent = 0;
        while pacer.allows(MSS) {
            pacer.charge(MSS);
            sent += 1;
        }
        assert_eq!(sent, PACE_BURST_PACKETS);
    }

    #[test]
    fn the_bucket_fills_at_twice_the_window_per_round_trip() {
        let now = Instant::now();
        let srtt = Duration::from_millis(100);
        let mut pacer = pacer(now, 100 * MSS, srtt);
        pacer.charge(PACE_BURST_PACKETS * MSS);
        assert!(!pacer.allows(MSS));
        // Two packets per millisecond: the next is ready in half of one.
        assert_eq!(pacer.ready_at(MSS), Some(now + Duration::from_micros(500)));
        pacer.refill(now + Duration::from_millis(5), 100 * MSS, srtt, MSS);
        let mut sent = 0;
        while pacer.allows(MSS) {
            pacer.charge(MSS);
            sent += 1;
        }
        assert_eq!(sent, 10);
    }

    #[test]
    fn an_idle_bucket_never_holds_more_than_its_capacity() {
        let now = Instant::now();
        let srtt = Duration::from_millis(100);
        let mut pacer = pacer(now, 100 * MSS, srtt);
        pacer.refill(now + Duration::from_secs(60), 100 * MSS, srtt, MSS);
        let mut sent = 0;
        while pacer.allows(MSS) {
            pacer.charge(MSS);
            sent += 1;
        }
        assert_eq!(sent, PACE_BURST_PACKETS);
    }

    // At a rate past one burst per timer tick the bucket holds a tick of it.
    #[test]
    fn a_fast_path_holds_one_tick_of_its_rate() {
        let now = Instant::now();
        let mut pacer = pacer(now, 1000 * MSS, Duration::from_micros(100));
        let mut sent = 0;
        while pacer.allows(MSS) {
            pacer.charge(MSS);
            sent += 1;
        }
        assert_eq!(sent, 20_000);
    }

    #[test]
    fn small_packets_cost_a_segment() {
        let now = Instant::now();
        let mut pacer = pacer(now, 1000 * MSS, Duration::from_secs(1));
        let mut sent = 0;
        while pacer.allows(1) {
            pacer.charge(1);
            sent += 1;
        }
        assert_eq!(sent, PACE_BURST_PACKETS);
    }
}
