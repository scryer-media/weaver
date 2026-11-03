// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One-way delay as the two ends see it. Every packet carries its send time
// and the gap its sender last saw between our send time and its receive
// time; the lowest gap of the last twenty minutes is the base, and a
// sample's delay is how far it sits above that base.

use std::time::Duration;

use tokio::time::Instant;

const HISTORY_MINUTES: usize = 20;
// A minute with fewer samples than this is too idle to step the base on.
const STEP_MIN_SAMPLES: u16 = 120;
const STEP_EVERY: Duration = Duration::from_secs(60);
// Their base dropping by less than this is clock drift, mirrored on ours.
const MAX_DRIFT_US: u32 = 10_000;
const RECENT_SAMPLES: usize = 3;

// u32 microsecond clocks wrap: `a` is below `b` when the forward distance
// from `a` to `b` is under half the range.
fn wrapping_less(a: u32, b: u32) -> bool {
    a != b && b.wrapping_sub(a) < u32::MAX / 2
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BaseHistory {
    minutes: [u32; HISTORY_MINUTES],
    base: u32,
    at: usize,
    // None until the first sample.
    samples_this_minute: Option<u16>,
}

impl Default for BaseHistory {
    fn default() -> Self {
        Self {
            minutes: [0; HISTORY_MINUTES],
            base: 0,
            at: 0,
            samples_this_minute: None,
        }
    }
}

impl BaseHistory {
    pub(crate) fn base(&self) -> Option<u32> {
        self.samples_this_minute.map(|_| self.base)
    }

    // The sample's height above the base, the base taking the sample when
    // it is lower. `step` moves to a new minute when this one saw enough.
    pub(crate) fn add(&mut self, sample: u32, step: bool) -> u32 {
        let seen = match self.samples_this_minute {
            Some(seen) => seen,
            None => {
                self.minutes = [sample; HISTORY_MINUTES];
                self.base = sample;
                0
            }
        };
        let seen = seen.saturating_add(1).min(u16::MAX - 1);
        self.samples_this_minute = Some(seen);

        if wrapping_less(sample, self.base) {
            self.base = sample;
            self.minutes[self.at] = sample;
        } else if wrapping_less(sample, self.minutes[self.at]) {
            self.minutes[self.at] = sample;
        }
        let height = sample.wrapping_sub(self.base);

        if step && seen > STEP_MIN_SAMPLES {
            self.samples_this_minute = Some(0);
            self.at = (self.at + 1) % HISTORY_MINUTES;
            self.minutes[self.at] = sample;
            self.base = sample;
            for &minute in &self.minutes {
                if wrapping_less(minute, self.base) {
                    self.base = minute;
                }
            }
        }
        height
    }

    // Raises the base by `by`, every minute below it with it.
    pub(crate) fn raise(&mut self, by: u32) {
        if self.samples_this_minute.is_none() {
            return;
        }
        self.base = self.base.wrapping_add(by);
        for minute in &mut self.minutes {
            if wrapping_less(*minute, self.base) {
                *minute = self.base;
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DelayTracker {
    ours: BaseHistory,
    theirs: BaseHistory,
    last_step: Instant,
    // Their receive time minus their send time on the last packet, in our
    // clock: the gap we report back in every header.
    reply_us: u32,
    recent: [u32; RECENT_SAMPLES],
    recent_at: usize,
}

impl DelayTracker {
    pub(crate) fn new(now: Instant, reply_us: u32) -> Self {
        Self {
            ours: BaseHistory::default(),
            theirs: BaseHistory::default(),
            last_step: now,
            reply_us,
            recent: [u32::MAX; RECENT_SAMPLES],
            recent_at: 0,
        }
    }

    pub(crate) fn reply_us(&self) -> u32 {
        self.reply_us
    }

    // Takes a packet's timestamps: `received_us` our clock when it came,
    // `sent_us` theirs when it left, `their_gap_us` the gap they saw on
    // ours. Returns the delay of our packets to them above its base, when
    // the packet carried one.
    pub(crate) fn on_packet(
        &mut self,
        now: Instant,
        received_us: u32,
        sent_us: u32,
        their_gap_us: u32,
    ) -> Option<u32> {
        let step = now.saturating_duration_since(self.last_step) > STEP_EVERY;
        if step {
            self.last_step = now;
        }

        if sent_us != 0 {
            self.reply_us = received_us.wrapping_sub(sent_us);
            let before = self.theirs.base();
            self.theirs.add(self.reply_us, step);
            if let (Some(before), Some(after)) = (before, self.theirs.base()) {
                let dropped = before.wrapping_sub(after);
                if before != 0
                    && dropped != 0
                    && dropped < MAX_DRIFT_US
                    && self.ours.base().is_some()
                {
                    self.ours.raise(dropped);
                }
            }
        }

        // A gap of i32::MAX is a peer that never measured one.
        if their_gap_us == 0 || their_gap_us == i32::MAX as u32 {
            return None;
        }
        let delay = self.ours.add(their_gap_us, step);
        self.recent[self.recent_at] = delay;
        self.recent_at = (self.recent_at + 1) % RECENT_SAMPLES;
        Some(delay)
    }

    // The delay the controller acts on: the lowest of the last three
    // samples, never above the round trip of what this packet acknowledged.
    pub(crate) fn controller_delay(&self, min_rtt: Option<Duration>) -> Duration {
        let mut delay = self.recent.iter().copied().min().unwrap_or(0);
        if let Some(rtt) = min_rtt {
            delay = delay.min(u32::try_from(rtt.as_micros()).unwrap_or(u32::MAX));
        }
        Duration::from_micros(delay.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sample_is_its_height_above_the_lowest() {
        let mut h = BaseHistory::default();
        assert_eq!(h.add(5_000, false), 0);
        assert_eq!(h.add(7_500, false), 2_500);
        assert_eq!(h.add(4_000, false), 0);
        assert_eq!(h.add(6_000, false), 2_000);
        assert_eq!(h.base(), Some(4_000));
    }

    #[test]
    fn heights_survive_the_clock_wrapping() {
        let mut h = BaseHistory::default();
        h.add(u32::MAX - 100, false);
        assert_eq!(h.add(200, false), 301);
    }

    #[test]
    fn the_base_steps_only_after_a_busy_minute() {
        let mut h = BaseHistory::default();
        h.add(1_000, false);
        for _ in 0..50 {
            h.add(9_000, false);
        }
        // An idle minute does not step.
        h.add(9_000, true);
        assert_eq!(h.base(), Some(1_000));
        for _ in 0..STEP_MIN_SAMPLES {
            h.add(9_000, false);
        }
        h.add(9_000, true);
        // The old minute still holds the low sample.
        assert_eq!(h.base(), Some(1_000));
        // Twenty busy minutes later the low sample has aged out.
        for _ in 0..HISTORY_MINUTES {
            for _ in 0..=STEP_MIN_SAMPLES {
                h.add(9_000, false);
            }
            h.add(9_000, true);
        }
        assert_eq!(h.base(), Some(9_000));
    }

    fn tracker() -> (DelayTracker, Instant) {
        let now = Instant::now();
        (DelayTracker::new(now, 0), now)
    }

    #[test]
    fn the_gap_we_report_is_taken_when_the_packet_arrives() {
        let (mut t, now) = tracker();
        t.on_packet(now, 50_000, 20_000, 0);
        assert_eq!(t.reply_us(), 30_000);
    }

    #[test]
    fn a_gap_of_i32_max_is_no_sample() {
        let (mut t, now) = tracker();
        assert_eq!(t.on_packet(now, 1, 1, i32::MAX as u32), None);
        assert_eq!(t.on_packet(now, 1, 1, 0), None);
        assert_eq!(t.on_packet(now, 1, 1, 40_000), Some(0));
        assert_eq!(t.on_packet(now, 1, 1, 45_000), Some(5_000));
    }

    #[test]
    fn their_base_dropping_a_little_raises_ours() {
        let (mut t, now) = tracker();
        t.on_packet(now, 100_000, 10_000, 40_000);
        // Their base drops by 4 ms: our base moves up by the same.
        t.on_packet(now, 100_000, 14_000, 0);
        assert_eq!(t.ours.base(), Some(44_000));
        // A drop of 10 ms or more is not drift; ours stays.
        t.on_packet(now, 100_000, 30_000, 0);
        assert_eq!(t.ours.base(), Some(44_000));
    }

    #[test]
    fn the_controller_takes_the_lowest_recent_delay_under_the_rtt() {
        let (mut t, now) = tracker();
        t.on_packet(now, 1, 1, 10_000);
        t.on_packet(now, 1, 1, 70_000);
        t.on_packet(now, 1, 1, 50_000);
        t.on_packet(now, 1, 1, 90_000);
        // The last three are 60, 40 and 80 ms above the base.
        assert_eq!(t.controller_delay(None), Duration::from_millis(40));
        assert_eq!(
            t.controller_delay(Some(Duration::from_millis(25))),
            Duration::from_millis(25)
        );
    }
}
