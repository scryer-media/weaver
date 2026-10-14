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
// How far the rate over those ticks may lie from the latest average, as a
// share of it. A mean can settle while the line under it still moves.
const STEADY_BAND: f64 = 0.05;
// The least a run measures for before it may stop on a stable average.
const MIN_MEASURING: Duration = Duration::from_secs(5);
// The most a run measures for.
const MAX_MEASURING: Duration = Duration::from_secs(30);
// How long a run waits for its first byte before it gives up.
const FIRST_BYTE_WAIT: Duration = Duration::from_secs(10);
// The most connections a run opens, unless more legs than this are up.
const MAX_CONNECTIONS: usize = 8;
// Ticks the ramp waits for a new connection's first byte before it judges
// the rate without it.
const FIRST_BYTE_TICKS: usize = 10;
// Connections a leg may lose before no more go on it.
const LEG_LOSSES: u8 = 3;

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
    Settling {
        ticks: usize,
    },
    // Collecting a window of ticks to judge the rate on.
    Judging {
        from: Tick,
        ticks: usize,
    },
    // Past the warm-up: the running averages since it ended, and the last
    // ticks, the warm-up's first, that the recent rate is taken over.
    Measuring {
        averages: VecDeque<f64>,
        recent: VecDeque<Tick>,
    },
    Stopped,
}

#[derive(Debug)]
pub(crate) struct Controller {
    weights: Vec<u8>,
    // Legs a connection may still go on: up, and not retired for the
    // connections they lost.
    usable: Vec<bool>,
    // Connections open on each leg, counting any that took a lost one's
    // place.
    opened: Vec<u16>,
    // Connections each leg has lost.
    losses: Vec<u8>,
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
            losses: vec![0; weights.len()],
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

    // A connection on `leg` ended on its own. `replaceable` when it moved a
    // byte and no quota refused it. True when the run opens another on the
    // same leg in its place; otherwise the leg is retired and its slot freed.
    // One that never moved a byte cannot reach the URL, and a quota that
    // refused one refuses the next.
    pub(crate) fn ended(&mut self, leg: usize, replaceable: bool) -> bool {
        let Some(losses) = self.losses.get_mut(leg) else {
            return false;
        };
        *losses = losses.saturating_add(1);
        if replaceable && self.usable[leg] && *losses < LEG_LOSSES {
            // No judged window straddles the swap.
            if matches!(self.phase, Phase::Settling { .. } | Phase::Judging { .. }) {
                self.phase = Phase::Settling { ticks: 0 };
            }
            return true;
        }
        self.usable[leg] = false;
        self.opened[leg] = self.opened[leg].saturating_sub(1);
        false
    }

    // When the warm-up ended and the bytes moved by then.
    pub(crate) fn warm_up(&self) -> Option<(Duration, u64)> {
        self.warm_up
    }

    pub(crate) fn tick(&mut self, tick: Tick) -> Decision {
        if matches!(self.phase, Phase::Stopped) {
            return Decision::Stop;
        }
        // A source that answers and never sends would otherwise hold the run
        // to its maximum.
        if tick.at >= MAX_MEASURING || (tick.bytes == 0 && tick.at >= FIRST_BYTE_WAIT) {
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
                    if let Some(leg) = self.place() {
                        self.opened[leg] += 1;
                        self.phase = Phase::Settling { ticks: 0 };
                        return Decision::Open(leg);
                    }
                }
                // No gain, or none left to add at the cap or with no leg
                // usable: the ramp ends here.
                self.warm_up = Some((tick.at, tick.bytes));
                let mut recent = VecDeque::with_capacity(STABLE_TICKS + 1);
                recent.push_back(tick);
                self.phase = Phase::Measuring {
                    averages: VecDeque::with_capacity(STABLE_TICKS),
                    recent,
                };
                Decision::Wait
            }
            Phase::Measuring { averages, recent } => {
                let (since, moved) = self.warm_up.unwrap_or_default();
                let span = tick.at.saturating_sub(since).as_secs_f64();
                if span > 0.0 {
                    if averages.len() == STABLE_TICKS {
                        averages.pop_front();
                    }
                    averages.push_back(tick.bytes.saturating_sub(moved) as f64 / span);
                }
                if recent.len() > STABLE_TICKS {
                    recent.pop_front();
                }
                recent.push_back(tick);
                if tick.at >= MIN_MEASURING
                    && averages.len() == STABLE_TICKS
                    && recent.len() > STABLE_TICKS
                    && stable(averages)
                    && steady(rate_between(recent[0], tick), averages[STABLE_TICKS - 1])
                {
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

// The rate over the last ticks holds to the running average.
fn steady(rate: f64, average: f64) -> bool {
    (rate - average).abs() <= STEADY_BAND * average
}

#[cfg(test)]
#[path = "adaptive_tests.rs"]
mod tests;
