// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::time::Duration;

// The assumed round trip, and the timeout before connection confirmation.
const RTTE_INITIAL_RTT: Duration = Duration::from_millis(300);
const RTTE_INITIAL_TIMEOUT: Duration = Duration::from_secs(3);

const RTTE_MAX_RTO: Duration = Duration::from_secs(60);

// From this many timeouts in a row the timeout is the cap.
const BACKOFF_CAPPED_AT: u32 = 7;

fn duration_abs_diff(dur: Duration, other: Duration) -> Duration {
    if let Some(res) = dur.checked_sub(other) {
        res
    } else {
        other.checked_sub(dur).unwrap()
    }
}

#[derive(Clone, Copy)]
enum RttState {
    Initial,
    Subsequent { srtt: Duration, rttvar: Duration },
}

#[derive(Clone, Copy)]
pub struct RttEstimator {
    state: RttState,
    confirmed: bool,
    // The least timeout after connection confirmation.
    min_timeout: Duration,
    // Timeouts in a row, with no valid packet from the remote between.
    timeouts: u32,

    #[cfg(test)]
    forced_timeout: Option<Duration>,
}

impl std::fmt::Debug for RttEstimator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rtt:{:?},rto:{:?}",
            self.roundtrip_time(),
            self.retransmission_timeout()
        )
    }
}

impl Default for RttEstimator {
    fn default() -> Self {
        Self::new(crate::constants::DEFAULT_MIN_TIMEOUT)
    }
}

impl RttEstimator {
    pub fn new(min_timeout: Duration) -> Self {
        Self {
            state: RttState::Initial,
            confirmed: false,
            min_timeout,
            timeouts: 0,

            #[cfg(test)]
            forced_timeout: None,
        }
    }
}

impl PartialEq for RttEstimator {
    fn eq(&self, other: &Self) -> bool {
        (self.roundtrip_time(), self.retransmission_timeout())
            == (other.roundtrip_time(), other.retransmission_timeout())
    }
}

impl RttEstimator {
    #[cfg(test)]
    pub fn force_timeout(&mut self, duration: Duration) {
        self.forced_timeout = Some(duration);
    }

    pub fn roundtrip_time(&self) -> Duration {
        match self.state {
            RttState::Initial => RTTE_INITIAL_RTT,
            RttState::Subsequent { srtt, .. } => srtt,
        }
    }

    /// Whether a round trip was measured, not assumed.
    pub fn has_sample(&self) -> bool {
        matches!(self.state, RttState::Subsequent { .. })
    }

    /// How long to wait for an ACK before the oldest packet in flight counts
    /// as lost: the mean round trip plus twice its deviation, never under
    /// the floor, a second more for the first timeout in a row and doubling
    /// from there, never over a minute.
    pub fn retransmission_timeout(&self) -> Duration {
        #[cfg(test)]
        if let Some(t) = self.forced_timeout {
            return t;
        }

        if !self.confirmed {
            return RTTE_INITIAL_TIMEOUT;
        }
        if self.timeouts >= BACKOFF_CAPPED_AT {
            return RTTE_MAX_RTO;
        }
        let base = match self.state {
            RttState::Initial => self.min_timeout,
            RttState::Subsequent { srtt, rttvar } => (srtt + rttvar * 2).max(self.min_timeout),
        };
        let backoff = match self.timeouts {
            0 => Duration::ZERO,
            n => Duration::from_secs(1 << (n - 1)),
        };
        (base + backoff).min(RTTE_MAX_RTO)
    }

    pub fn sample(&mut self, new_rtt: Duration) {
        match &mut self.state {
            RttState::Initial => {
                self.state = RttState::Subsequent {
                    srtt: new_rtt,
                    rttvar: new_rtt / 2,
                }
            }
            RttState::Subsequent { srtt, rttvar } => {
                *rttvar = *rttvar * 3 / 4 + duration_abs_diff(*srtt, new_rtt) / 4;
                *srtt = (*srtt * 7 + new_rtt) / 8;
            }
        }
    }

    /// A timeout, which backs the next one off.
    pub fn on_rto_timeout(&mut self) {
        self.timeouts = self.timeouts.saturating_add(1);
    }

    /// Timeouts in a row.
    pub fn timeouts(&self) -> u32 {
        self.timeouts
    }

    pub fn confirm(&mut self) {
        self.confirmed = true;
    }

    /// A valid packet confirms the connection and ends a run of timeouts.
    pub fn on_valid_packet(&mut self) {
        self.confirm();
        self.timeouts = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLOOR: Duration = Duration::from_millis(500);

    #[test]
    fn three_seconds_before_any_round_trip() {
        let r = RttEstimator::new(FLOOR);
        assert_eq!(r.retransmission_timeout(), Duration::from_secs(3));
    }

    #[test]
    fn confirmation_without_a_round_trip_uses_the_floor() {
        let mut r = RttEstimator::new(FLOOR);
        r.sample(Duration::from_millis(20));
        assert_eq!(r.retransmission_timeout(), Duration::from_secs(3));
        let mut r = RttEstimator::new(FLOOR);
        r.on_valid_packet();
        assert_eq!(r.retransmission_timeout(), FLOOR);
        assert_eq!(r.roundtrip_time(), RTTE_INITIAL_RTT);
    }

    #[test]
    fn a_short_round_trip_waits_the_floor() {
        let mut r = RttEstimator::new(FLOOR);
        r.confirm();
        r.sample(Duration::from_millis(20));
        assert_eq!(r.retransmission_timeout(), FLOOR);
    }

    #[test]
    fn a_long_round_trip_waits_it_and_twice_its_deviation() {
        let mut r = RttEstimator::new(FLOOR);
        r.confirm();
        r.sample(Duration::from_millis(400));
        // 400 ms with a deviation of half of it.
        assert_eq!(r.retransmission_timeout(), Duration::from_millis(800));
    }

    #[test]
    fn timeouts_in_a_row_add_seconds_doubling_up_to_a_minute() {
        let mut r = RttEstimator::new(FLOOR);
        r.confirm();
        r.sample(Duration::from_millis(20));
        let mut waits = vec![];
        for _ in 0..8 {
            r.on_rto_timeout();
            waits.push(r.retransmission_timeout().as_millis());
        }
        assert_eq!(waits, [1500, 2500, 4500, 8500, 16500, 32500, 60000, 60000]);
        r.on_valid_packet();
        assert_eq!(r.retransmission_timeout(), FLOOR);
    }
}
