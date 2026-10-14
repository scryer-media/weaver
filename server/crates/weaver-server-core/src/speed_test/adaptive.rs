// When a bandwidth run adds a connection, when its warm-up ends and when it
// stops. It is fed the run's counters one tick at a time and keeps no clock
// of its own.

use std::collections::VecDeque;
use std::time::Duration;

use super::overrides::next_leg;

// How often the run is looked at.
pub(crate) const TICK: Duration = Duration::from_millis(200);
// Ticks the ramp averages the rate over.
const RAMP_WINDOW: usize = 5;
// How much an added connection must lift that average for the ramp to go on.
const RAMP_GAIN: f64 = 1.05;
// Running averages that must agree before the run stops.
const STABLE_TICKS: usize = 7;
// How far apart they may lie, as a share of the largest.
const STABLE_BAND: f64 = 0.02;
// The least a run measures for before it may stop on a stable average.
const MIN_MEASURING: Duration = Duration::from_secs(5);
// The most a run measures for.
const MAX_MEASURING: Duration = Duration::from_secs(30);
// The most connections a run opens, unless more legs than this are up.
const MAX_CONNECTIONS: usize = 8;
// Ticks the ramp waits for a new connection's first byte before it judges
// the rate without it.
const FIRST_BYTE_TICKS: usize = 10;

// The run's counters at one tick.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Tick {
    // Since the run started measuring.
    pub at: Duration,
    // Every byte moved so far.
    pub bytes: u64,
    // Open connections that have not moved a byte yet.
    pub waiting: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decision {
    Wait,
    // Open one more connection on this leg.
    Open(usize),
    Stop,
}

#[derive(Debug)]
enum Phase {
    // Waiting for the newest connections to move their first bytes.
    Settling { ticks: usize },
    // Collecting a window of ticks to judge the rate on.
    Judging { from: Tick, ticks: usize },
    // Past the warm-up: the running averages since it ended.
    Measuring { averages: VecDeque<f64> },
    Stopped,
}

#[derive(Debug)]
pub(crate) struct Controller {
    weights: Vec<u8>,
    // Legs a connection may still go on: up, and none of their connections
    // has ended on its own.
    usable: Vec<bool>,
    opened: Vec<u16>,
    cap: usize,
    // The averaged rate the last added connection was judged against.
    baseline: f64,
    phase: Phase,
    warm_up: Option<(Duration, u64)>,
}

impl Controller {
    // `weights` and `up` cover every leg of the route.
    pub(crate) fn new(weights: Vec<u8>, up: Vec<bool>) -> Self {
        let legs_up = up.iter().filter(|up| **up).count();
        Self {
            opened: vec![0; weights.len()],
            weights,
            usable: up,
            cap: MAX_CONNECTIONS.max(legs_up),
            baseline: 0.0,
            phase: Phase::Settling { ticks: 0 },
            warm_up: None,
        }
    }

    // The connections a run opens first: one per leg that is up.
    pub(crate) fn initial(&mut self) -> Vec<usize> {
        let legs: Vec<usize> = (0..self.usable.len())
            .filter(|&leg| self.usable[leg])
            .collect();
        for &leg in &legs {
            self.opened[leg] += 1;
        }
        legs
    }

    // A connection on `leg` ended on its own; no more go on that leg.
    pub(crate) fn ended(&mut self, leg: usize) {
        if let Some(usable) = self.usable.get_mut(leg) {
            *usable = false;
        }
    }

    // When the warm-up ended and the bytes moved by then.
    pub(crate) fn warm_up(&self) -> Option<(Duration, u64)> {
        self.warm_up
    }

    pub(crate) fn tick(&mut self, tick: Tick) -> Decision {
        if matches!(self.phase, Phase::Stopped) {
            return Decision::Stop;
        }
        if tick.at >= MAX_MEASURING {
            self.phase = Phase::Stopped;
            return Decision::Stop;
        }
        match &mut self.phase {
            Phase::Settling { ticks } => {
                *ticks += 1;
                if tick.bytes > 0 && (tick.waiting == 0 || *ticks >= FIRST_BYTE_TICKS) {
                    self.phase = Phase::Judging {
                        from: tick,
                        ticks: 0,
                    };
                }
                Decision::Wait
            }
            Phase::Judging { from, ticks } => {
                *ticks += 1;
                if *ticks < RAMP_WINDOW {
                    return Decision::Wait;
                }
                let rate = rate_between(*from, tick);
                if rate > 0.0 && rate >= self.baseline * RAMP_GAIN {
                    self.baseline = rate;
                    match self.place() {
                        Some(leg) => {
                            self.opened[leg] += 1;
                            self.phase = Phase::Settling { ticks: 0 };
                            Decision::Open(leg)
                        }
                        // At the cap: judge the next window the same way.
                        None => {
                            self.phase = Phase::Judging {
                                from: tick,
                                ticks: 0,
                            };
                            Decision::Wait
                        }
                    }
                } else {
                    self.warm_up = Some((tick.at, tick.bytes));
                    self.phase = Phase::Measuring {
                        averages: VecDeque::with_capacity(STABLE_TICKS),
                    };
                    Decision::Wait
                }
            }
            Phase::Measuring { averages } => {
                let (since, moved) = self.warm_up.unwrap_or_default();
                let span = tick.at.saturating_sub(since).as_secs_f64();
                if span > 0.0 {
                    if averages.len() == STABLE_TICKS {
                        averages.pop_front();
                    }
                    averages.push_back(tick.bytes.saturating_sub(moved) as f64 / span);
                }
                if tick.at >= MIN_MEASURING && averages.len() == STABLE_TICKS && stable(averages) {
                    self.phase = Phase::Stopped;
                    return Decision::Stop;
                }
                Decision::Wait
            }
            Phase::Stopped => Decision::Stop,
        }
    }

    // Where the next connection goes, or `None` at the cap or with no leg
    // left to take one.
    fn place(&self) -> Option<usize> {
        let opened: usize = self.opened.iter().map(|n| usize::from(*n)).sum();
        if opened >= self.cap {
            return None;
        }
        next_leg(&self.weights, &self.usable, &self.opened)
    }
}

fn rate_between(from: Tick, to: Tick) -> f64 {
    let span = to.at.saturating_sub(from.at).as_secs_f64();
    if span > 0.0 {
        to.bytes.saturating_sub(from.bytes) as f64 / span
    } else {
        0.0
    }
}

fn stable(averages: &VecDeque<f64>) -> bool {
    let max = averages.iter().copied().fold(f64::MIN, f64::max);
    let min = averages.iter().copied().fold(f64::MAX, f64::min);
    max - min <= STABLE_BAND * max
}

#[cfg(test)]
#[path = "adaptive_tests.rs"]
mod tests;
