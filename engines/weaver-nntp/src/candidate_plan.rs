//! Pure candidate selection shared by address races and proxy-member races.
use crate::address_plan::{
    ADDRESS_REPLAN_INTERVAL, DELIVERY_EVIDENCE_AGE, DELIVERY_MIN_SAMPLES, DELIVERY_MIN_WIRE,
    DELIVERY_REPIN_RATIO, DELIVERY_VERDICT_INTERVAL, FAILED_RACE_HOLDOFF, RaceReason,
    SHADOW_EVERY_CONNECTS, SHADOW_INTERVAL, SHADOW_MIN_PIN_AGE, SUSPECT_AFTER_FAILURES,
};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    hash::Hash,
    io,
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};
use tracing::{debug, info};

pub trait Candidate: Copy + Eq + Hash + Debug + Display + Send + Sync + 'static {
    type Key: Copy + Eq + Hash + Send + Sync;
    fn key(self) -> Self::Key;
}
impl Candidate for SocketAddr {
    type Key = IpAddr;
    fn key(self) -> IpAddr {
        self.ip()
    }
}
impl Candidate for u32 {
    type Key = u32;
    fn key(self) -> u32 {
        self
    }
}

pub enum Attempt<C> {
    Race {
        candidates: Vec<C>,
        reason: RaceReason,
    },
    Dial(Vec<C>),
    Wait(u64),
    Fail(io::ErrorKind, String),
}

#[derive(Clone, Debug)]
pub struct CandidateSnapshot<C> {
    pub candidate: C,
    pub state: &'static str,
    pub connect_time: Option<Duration>,
    pub body_latency: Option<Duration>,
    pub failures: u32,
    pub samples: u32,
    pub bytes_per_second: Option<f64>,
}
#[derive(Clone, Debug)]
pub struct PlanSnapshot<C> {
    pub pinned: Option<C>,
    pub candidates: Vec<CandidateSnapshot<C>>,
    pub races_won: u64,
    pub races_failed: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct AddrStats {
    pub(crate) connect_ewma: Option<Duration>,
    pub(crate) consecutive_failures: u32,
    /// Sessions in a row that connected and then never heard from the
    /// server. A connect does not clear it; only a session that did hear
    /// from the server does.
    pub(crate) setup_failures: u32,
    pub(crate) body_latency_ewma: Option<Duration>,
    pub(crate) delivery: Delivery,
}

impl AddrStats {
    /// The address last refused a connect, or last connected without the
    /// server answering.
    pub(crate) fn failing(self) -> bool {
        self.consecutive_failures > 0 || self.setup_failures > 0
    }
}

/// What one address's connections delivered since the pin was last judged:
/// warm fetches only, each booked as its bytes and its own wire time, so the
/// quotient is what one connection to the address moves per second whatever
/// the number of connections that happened to land there.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Delivery {
    pub(crate) bytes: u64,
    pub(crate) wire: Duration,
    pub(crate) samples: u32,
    pub(crate) last_at: Option<Instant>,
}

impl Delivery {
    pub(crate) fn book(&mut self, bytes: u64, wire: Duration, now: Instant) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.wire = self.wire.saturating_add(wire);
        self.samples = self.samples.saturating_add(1);
        self.last_at = Some(now);
    }

    pub(crate) fn bytes_per_second(self) -> Option<f64> {
        (self.samples > 0 && !self.wire.is_zero())
            .then(|| self.bytes as f64 / self.wire.as_secs_f64())
    }

    /// The rate, once enough fetches and enough wire time stand behind it to
    /// be evidence and while the latest of them is recent enough to still
    /// describe the address.
    pub(crate) fn measured_rate(self, now: Instant) -> Option<f64> {
        let fresh = self
            .last_at
            .is_some_and(|at| now.saturating_duration_since(at) < DELIVERY_EVIDENCE_AGE);
        (fresh && self.samples >= DELIVERY_MIN_SAMPLES && self.wire >= DELIVERY_MIN_WIRE)
            .then(|| self.bytes_per_second())
            .flatten()
    }
}

/// What the delivery booked since the pin was last judged says about it.
enum DeliveryVerdict<C> {
    /// A challenger out-delivered the pin by [`DELIVERY_REPIN_RATIO`].
    Challenged {
        challenger: C,
        challenger_rate: f64,
        pin_rate: f64,
    },
    /// The best measured challenger did not.
    Keep {
        best: C,
        best_rate: f64,
        pin_rate: f64,
    },
    /// The pin or every challenger is short of [`DELIVERY_MIN_SAMPLES`] or
    /// [`DELIVERY_MIN_WIRE`], or its evidence has gone stale.
    Unmeasured,
}

/// What a connect decided, told once the plan's lock is released.
pub(crate) enum Announce<C> {
    Repinned {
        challenger: C,
        challenger_rate: f64,
        pin: C,
        pin_rate: f64,
    },
    Leading {
        challenger: C,
        challenger_rate: f64,
        pin: C,
        pin_rate: f64,
    },
    Kept {
        pin: C,
        pin_rate: f64,
        best: C,
        best_rate: f64,
    },
    Shadow {
        challenger: C,
        pin: C,
    },
}

impl<C: Candidate> Announce<C> {
    pub(crate) fn log(&self, label: &str) {
        match self {
            Announce::Repinned {
                challenger,
                challenger_rate,
                pin,
                pin_rate,
            } => info!(
                server = label,
                reason = RaceReason::Delivery.as_str(),
                address = %challenger,
                previous = %pin,
                bytes_per_second = *challenger_rate as u64,
                previous_bytes_per_second = *pin_rate as u64,
                "pinned the address that delivered faster"
            ),
            Announce::Leading {
                challenger,
                challenger_rate,
                pin,
                pin_rate,
            } => debug!(
                server = label,
                address = %pin,
                bytes_per_second = *pin_rate as u64,
                challenger = %challenger,
                challenger_bytes_per_second = *challenger_rate as u64,
                "a challenger out-delivered the pinned address; confirming at the next verdict"
            ),
            Announce::Kept {
                pin,
                pin_rate,
                best,
                best_rate,
            } => debug!(
                server = label,
                address = %pin,
                bytes_per_second = *pin_rate as u64,
                challenger = %best,
                challenger_bytes_per_second = *best_rate as u64,
                "kept the pinned address on delivery"
            ),
            Announce::Shadow { challenger, pin } => debug!(
                server = label,
                address = %challenger,
                pinned = %pin,
                "pointing a reconnect at a challenger address to measure its delivery"
            ),
        }
    }
}

pub struct CandidatePlan<C: Candidate> {
    pub(crate) candidates: Vec<C>,
    pub(crate) pinned: Option<C>,
    pub(crate) chosen_at: Option<Instant>,
    pub(crate) per_addr: HashMap<C::Key, AddrStats>,
    /// When a reconnect was last pointed at a challenger.
    pub(crate) last_shadow_at: Option<Instant>,
    /// Reconnects handed out since then.
    pub(crate) connects_since_shadow: u32,
    /// The challenger the last verdict found faster than the pin. It takes
    /// the pin only if the next verdict finds the same, so one verdict's
    /// worth of samples from one connection never moves the pin by itself.
    pub(crate) delivery_leader: Option<C>,
    /// When delivery was last judged, since the last race. The next verdict
    /// waits [`DELIVERY_VERDICT_INTERVAL`] from it, or from the pin's choice
    /// when there has been none.
    pub(crate) last_verdict_at: Option<Instant>,
    /// Bumped whenever a race finishes, so a caller waiting on one can tell
    /// that it did.
    pub(crate) generation: u64,
    pub(crate) racing: bool,
    /// A race some event asked for, still to run.
    pub(crate) pending: Option<RaceReason>,
    /// Why the last race found nothing, handed to callers that waited on it.
    pub(crate) last_race_error: Option<(io::ErrorKind, String)>,
    /// When a race last found no address while nothing was pinned. Holds
    /// further races off for [`FAILED_RACE_HOLDOFF`].
    pub(crate) last_race_failed_at: Option<Instant>,
    pub(crate) races_won: u64,
    pub(crate) races_failed: u64,
    pub(crate) repins: [u64; RaceReason::ALL.len()],
}

pub(crate) enum Next<C> {
    Dial(Vec<C>, Option<Announce<C>>),
    Race(RaceReason),
    Wait(u64),
    Fail(io::ErrorKind, String),
}

impl<C: Candidate> CandidatePlan<C> {
    pub(crate) fn next(&mut self, now: Instant) -> Next<C> {
        if self.racing {
            return Next::Wait(self.generation);
        }
        let mut announce = None;
        let reason = match (self.pinned, self.pending) {
            (None, _) if self.failed_race_is_recent(now) => {
                if self.candidates.is_empty() {
                    let (kind, message) = self.last_race_error.clone().unwrap_or((
                        io::ErrorKind::AddrNotAvailable,
                        "no address answered".to_string(),
                    ));
                    return Next::Fail(kind, message);
                }
                None
            }
            (None, _) => Some(RaceReason::Initial),
            (Some(_), Some(pending)) => Some(pending),
            (Some(_), None) if self.pin_is_due(now) => Some(RaceReason::Interval),
            (Some(pin), None) if self.verdict_is_allowed(now) => {
                announce = self.judge_on_delivery(now, pin);
                None
            }
            _ => None,
        };
        match reason {
            Some(reason) => {
                self.racing = true;
                Next::Race(reason)
            }
            None => {
                let (order, shadow) = self.dial_order_for_connect(now);
                Next::Dial(order, announce.or(shadow))
            }
        }
    }

    pub(crate) fn stats(&self, addr: C) -> AddrStats {
        self.per_addr.get(&addr.key()).copied().unwrap_or_default()
    }

    /// A verdict may run: the pin was chosen, or last judged, at least
    /// [`DELIVERY_VERDICT_INTERVAL`] ago.
    pub(crate) fn verdict_is_allowed(&self, now: Instant) -> bool {
        self.last_verdict_at
            .or(self.chosen_at)
            .is_some_and(|at| now.saturating_duration_since(at) >= DELIVERY_VERDICT_INTERVAL)
    }

    /// Judge the pin on the delivery booked since it was last judged, when
    /// there is enough of it. Leaves the pin's age alone, and leaves the
    /// samples to keep gathering when there is not enough.
    pub(crate) fn judge_on_delivery(&mut self, now: Instant, pin: C) -> Option<Announce<C>> {
        let announce = match self.delivery_verdict(now, pin) {
            DeliveryVerdict::Challenged {
                challenger,
                challenger_rate,
                pin_rate,
            } if self.delivery_leader == Some(challenger) => {
                self.pinned = Some(challenger);
                self.delivery_leader = None;
                self.repins[RaceReason::Delivery.index()] += 1;
                Announce::Repinned {
                    challenger,
                    challenger_rate,
                    pin,
                    pin_rate,
                }
            }
            DeliveryVerdict::Challenged {
                challenger,
                challenger_rate,
                pin_rate,
            } => {
                self.delivery_leader = Some(challenger);
                Announce::Leading {
                    challenger,
                    challenger_rate,
                    pin,
                    pin_rate,
                }
            }
            DeliveryVerdict::Keep {
                best,
                best_rate,
                pin_rate,
            } => {
                self.delivery_leader = None;
                Announce::Kept {
                    pin,
                    pin_rate,
                    best,
                    best_rate,
                }
            }
            DeliveryVerdict::Unmeasured => return None,
        };
        self.last_verdict_at = Some(now);
        self.reset_delivery();
        Some(announce)
    }

    /// Compare the pin with the best measured challenger that is not
    /// currently refusing connections: a pin should never move to an address
    /// the next connect would fail on.
    fn delivery_verdict(&self, now: Instant, pin: C) -> DeliveryVerdict<C> {
        let Some(pin_rate) = self.stats(pin).delivery.measured_rate(now) else {
            return DeliveryVerdict::Unmeasured;
        };
        let best = self
            .candidates
            .iter()
            .copied()
            .filter(|addr| *addr != pin)
            .map(|addr| (addr, self.stats(addr)))
            .filter(|(_, stats)| !stats.failing())
            .filter_map(|(addr, stats)| Some((addr, stats.delivery.measured_rate(now)?)))
            .max_by(|left, right| left.1.total_cmp(&right.1));
        match best {
            Some((challenger, challenger_rate))
                if challenger_rate >= pin_rate * DELIVERY_REPIN_RATIO =>
            {
                DeliveryVerdict::Challenged {
                    challenger,
                    challenger_rate,
                    pin_rate,
                }
            }
            Some((best, best_rate)) => DeliveryVerdict::Keep {
                best,
                best_rate,
                pin_rate,
            },
            None => DeliveryVerdict::Unmeasured,
        }
    }

    /// Every verdict, and every race, judges the pin afresh from here.
    pub(crate) fn reset_delivery(&mut self) {
        for stats in self.per_addr.values_mut() {
            stats.delivery = Delivery::default();
        }
    }

    /// The order one connect dials: the usual order, or a challenger ahead of
    /// it when this reconnect is the one to point at a challenger.
    pub(crate) fn dial_order_for_connect(&mut self, now: Instant) -> (Vec<C>, Option<Announce<C>>) {
        let order = self.dial_order();
        self.connects_since_shadow = self.connects_since_shadow.saturating_add(1);
        let Some((challenger, pin)) = self.shadow_candidate(now) else {
            return (order, None);
        };
        self.last_shadow_at = Some(now);
        self.connects_since_shadow = 0;
        let order = std::iter::once(challenger)
            .chain(order.into_iter().filter(|addr| *addr != challenger))
            .collect();
        (order, Some(Announce::Shadow { challenger, pin }))
    }

    /// The challenger this reconnect goes to, and the pin it stands in for,
    /// if it is time for one: the candidate with the fewest fetches booked
    /// among those not yet measured, by the same measure a verdict holds
    /// them to, so a challenger whose connection closed short of it is given
    /// another rather than left unjudged; never one that last refused and never
    /// one that has yet to connect at all — an address that swallows packets
    /// would hold this reconnect for the whole attempt limit. `None` while
    /// the pin is too young, too recently or too often shadowed, not yet
    /// measured itself, or already judged against every challenger.
    pub(crate) fn shadow_candidate(&self, now: Instant) -> Option<(C, C)> {
        let pin = self.pinned?;
        if now.saturating_duration_since(self.chosen_at?) < SHADOW_MIN_PIN_AGE {
            return None;
        }
        if self
            .last_shadow_at
            .is_some_and(|at| now.saturating_duration_since(at) < SHADOW_INTERVAL)
        {
            return None;
        }
        if self.connects_since_shadow < SHADOW_EVERY_CONNECTS {
            return None;
        }
        // The pin must be measured before any challenger is worth a look.
        self.stats(pin).delivery.measured_rate(now)?;
        self.candidates
            .iter()
            .copied()
            .filter(|addr| *addr != pin)
            .map(|addr| (addr, self.stats(addr)))
            .filter(|(_, stats)| {
                !stats.failing()
                    && stats.connect_ewma.is_some()
                    && stats.delivery.measured_rate(now).is_none()
            })
            .min_by_key(|(_, stats)| (stats.delivery.samples, stats.connect_ewma))
            .map(|(addr, _)| (addr, pin))
    }

    pub(crate) fn failed_race_is_recent(&self, now: Instant) -> bool {
        self.last_race_failed_at
            .is_some_and(|at| now.saturating_duration_since(at) < FAILED_RACE_HOLDOFF)
    }

    pub(crate) fn pin_is_due(&self, now: Instant) -> bool {
        self.chosen_at
            .is_none_or(|at| now.saturating_duration_since(at) >= ADDRESS_REPLAN_INTERVAL)
    }

    /// The pin first, then every other candidate: the ones that last
    /// connected before the ones that last failed, faster before slower.
    pub(crate) fn dial_order(&self) -> Vec<C> {
        let mut others: Vec<C> = self
            .candidates
            .iter()
            .copied()
            .filter(|addr| Some(*addr) != self.pinned)
            .collect();
        others.sort_by_key(|addr| {
            let stats = self.per_addr.get(&addr.key()).copied().unwrap_or_default();
            (stats.failing(), stats.connect_ewma.unwrap_or(Duration::MAX))
        });
        self.pinned.into_iter().chain(others).collect()
    }
}

impl<C: Candidate> Default for CandidatePlan<C> {
    fn default() -> Self {
        Self {
            candidates: Vec::new(),
            pinned: None,
            chosen_at: None,
            per_addr: HashMap::new(),
            last_shadow_at: None,
            connects_since_shadow: 0,
            delivery_leader: None,
            last_verdict_at: None,
            generation: 0,
            racing: false,
            pending: None,
            last_race_error: None,
            last_race_failed_at: None,
            races_won: 0,
            races_failed: 0,
            repins: [0; RaceReason::ALL.len()],
        }
    }
}
impl<C: Candidate> CandidatePlan<C> {
    /// Ordered leases do not start a detached race: DNS and HTTP must share a member.
    pub fn lease_order(&self) -> Vec<C> {
        let mut order = self.dial_order();
        order.sort_by_key(|candidate| self.stats(*candidate).failing());
        order
    }
    pub fn lease_succeeded(&mut self, candidate: C, now: Instant) {
        self.setup(candidate, true, now);
        if !self.racing && self.pinned.is_none_or(|pin| self.stats(pin).failing()) {
            self.pinned = Some(candidate);
            self.chosen_at = Some(now);
            self.pending = None;
        }
    }
    pub fn next_attempt(&mut self, now: Instant) -> Attempt<C> {
        match self.next(now) {
            Next::Race(reason) => Attempt::Race {
                candidates: self.candidates.clone(),
                reason,
            },
            Next::Dial(order, _) => Attempt::Dial(order),
            Next::Wait(generation) => Attempt::Wait(generation),
            Next::Fail(kind, message) => Attempt::Fail(kind, message),
        }
    }
    pub fn set_candidates(&mut self, candidates: Vec<C>, _now: Instant) {
        let mut unique = Vec::new();
        for candidate in candidates {
            if !unique.contains(&candidate) {
                unique.push(candidate);
            }
        }
        self.per_addr
            .retain(|key, _| unique.iter().any(|c| c.key() == *key));
        if self.pinned.is_some_and(|pin| !unique.contains(&pin)) {
            self.pinned = None;
        }
        if self.candidates != unique {
            self.pending = Some(RaceReason::Initial);
        }
        self.candidates = unique;
    }
    pub fn replan(&mut self) {
        self.racing = false;
        self.generation = self.generation.wrapping_add(1);
        self.pending = Some(RaceReason::Initial);
    }
    pub fn connected(&mut self, candidate: C, elapsed: Duration, _now: Instant) {
        let stats = self.per_addr.entry(candidate.key()).or_default();
        stats.connect_ewma = Some(ewma(stats.connect_ewma, elapsed));
        stats.consecutive_failures = 0;
    }
    pub fn failed(&mut self, candidate: C, _now: Instant) {
        let stats = self.per_addr.entry(candidate.key()).or_default();
        stats.consecutive_failures = stats.consecutive_failures.saturating_add(1);
        if self.pinned == Some(candidate) && stats.consecutive_failures >= SUSPECT_AFTER_FAILURES {
            self.pending = Some(RaceReason::Suspect);
        }
    }
    pub fn setup(&mut self, candidate: C, reached: bool, _now: Instant) {
        let stats = self.per_addr.entry(candidate.key()).or_default();
        if reached {
            stats.setup_failures = 0;
        } else {
            stats.setup_failures = stats.setup_failures.saturating_add(1);
            if self.pinned == Some(candidate) && stats.setup_failures >= SUSPECT_AFTER_FAILURES {
                self.pending = Some(RaceReason::Suspect);
            }
        }
    }
    pub fn body_latency(&mut self, candidate: C, elapsed: Duration, _now: Instant) {
        if !self.candidates.contains(&candidate) {
            return;
        }
        let stats = self.per_addr.entry(candidate.key()).or_default();
        stats.body_latency_ewma = Some(ewma(stats.body_latency_ewma, elapsed));
    }
    pub fn delivered(&mut self, candidate: C, bytes: u64, wire: Duration, now: Instant) {
        if bytes > 0 && !wire.is_zero() && self.candidates.contains(&candidate) {
            self.per_addr
                .entry(candidate.key())
                .or_default()
                .delivery
                .book(bytes, wire, now);
        }
    }
    pub fn over_limit_cleared(&mut self, _now: Instant) {
        if self.pinned.is_some() && self.pending.is_none() {
            self.pending = Some(RaceReason::OverLimitCleared);
        }
    }
    pub fn race_finished(
        &mut self,
        winner: Result<C, (io::ErrorKind, String)>,
        reason: RaceReason,
        now: Instant,
    ) {
        self.racing = false;
        self.pending = None;
        self.generation = self.generation.wrapping_add(1);
        self.reset_delivery();
        self.delivery_leader = None;
        self.last_verdict_at = None;
        let previous = self.pinned;
        match winner {
            Ok(winner) => {
                self.pinned = Some(winner);
                self.chosen_at = Some(now);
                self.races_won += 1;
                self.last_race_error = None;
                self.last_race_failed_at = None;
                if previous.is_some_and(|prior| prior != winner) {
                    self.repins[reason.index()] += 1;
                }
            }
            Err(error) => {
                self.races_failed += 1;
                self.last_race_error = Some(error);
                if previous.is_some() {
                    self.chosen_at = Some(now);
                } else {
                    self.last_race_failed_at = Some(now);
                }
            }
        }
    }
    pub fn snapshot_at(&self, now: Instant) -> PlanSnapshot<C> {
        let mut snapshot = self.snapshot();
        if self.pinned.is_none() && self.failed_race_is_recent(now) {
            for candidate in &mut snapshot.candidates {
                candidate.state = "HOLDOFF";
            }
        }
        snapshot
    }
    pub fn snapshot(&self) -> PlanSnapshot<C> {
        PlanSnapshot {
            pinned: self.pinned,
            races_won: self.races_won,
            races_failed: self.races_failed,
            candidates: self
                .candidates
                .iter()
                .map(|&candidate| {
                    let stats = self.stats(candidate);
                    CandidateSnapshot {
                        candidate,
                        state: if stats.consecutive_failures.max(stats.setup_failures)
                            >= SUSPECT_AFTER_FAILURES
                        {
                            "SUSPECT"
                        } else if self.racing {
                            "PROBING"
                        } else if self.pinned == Some(candidate) {
                            if stats.delivery.samples >= DELIVERY_MIN_SAMPLES {
                                "PINNED"
                            } else {
                                "LEADER"
                            }
                        } else if self.delivery_leader == Some(candidate) {
                            "CHALLENGER"
                        } else if stats.failing() {
                            "FAILING"
                        } else if stats.connect_ewma.is_none() {
                            "UNMEASURED"
                        } else {
                            "HEALTHY"
                        },
                        connect_time: stats.connect_ewma,
                        body_latency: stats.body_latency_ewma,
                        failures: stats.consecutive_failures.max(stats.setup_failures),
                        samples: stats.delivery.samples,
                        bytes_per_second: stats.delivery.bytes_per_second(),
                    }
                })
                .collect(),
        }
    }
}
fn ewma(previous: Option<Duration>, sample: Duration) -> Duration {
    match previous {
        None => sample,
        Some(previous) => previous.mul_f64(0.75) + sample.mul_f64(0.25),
    }
}

#[cfg(test)]
#[path = "candidate_plan_tests.rs"]
mod tests;
