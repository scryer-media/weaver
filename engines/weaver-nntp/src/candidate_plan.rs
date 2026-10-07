//! Pure candidate selection shared by address races and proxy-member races.
use crate::address_plan::{
    DELIVERY_MIN_SAMPLES, DELIVERY_REPIN_RATIO, RaceReason, SHADOW_EVERY_CONNECTS, SHADOW_RETRIES,
    SUSPECT_AFTER_FAILURES,
};
use crate::plan_timing::timing;
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
    /// Dial the candidates in order. `shadow` names the challenger this
    /// reconnect was pointed at, when it was: the caller reports that
    /// connection's end through [`CandidatePlan::shadow_ended`]. `announce`
    /// is what the plan decided for this connect, for the caller to log once
    /// the plan's lock is released.
    Dial {
        order: Vec<C>,
        shadow: Option<C>,
        announce: Option<Announce<C>>,
    },
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
            .is_some_and(|at| now.saturating_duration_since(at) < timing().delivery_evidence_age);
        (fresh && self.samples >= DELIVERY_MIN_SAMPLES && self.wire >= timing().delivery_min_wire)
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
    /// the timing's `delivery_min_wire`, or its evidence has gone stale.
    Unmeasured,
}

/// What a connect decided, told once the plan's lock is released.
pub enum Announce<C> {
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
    pub fn log(&self, label: &str) {
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
    /// A challenger whose shadow connection ended before it was measured,
    /// owed the next reconnect ahead of the usual pacing.
    pub(crate) shadow_owed: Option<C>,
    /// Shadow connections in a row that ended without one response from
    /// their challenger. Bounds how many reconnects a challenger that
    /// connects but never answers may take ahead of the pacing.
    pub(crate) idle_shadows: u32,
    /// The challenger the last verdict found faster than the pin. It takes
    /// the pin only if the next verdict finds the same, so one verdict's
    /// worth of samples from one connection never moves the pin by itself.
    pub(crate) delivery_leader: Option<C>,
    /// When delivery was last judged, since the last race. The next verdict
    /// waits the timing's `delivery_verdict_interval` from it, or from the pin's choice
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
    /// further races off for the timing's `failed_race_holdoff`.
    pub(crate) last_race_failed_at: Option<Instant>,
    pub(crate) races_won: u64,
    pub(crate) races_failed: u64,
    pub(crate) repins: [u64; RaceReason::ALL.len()],
}

pub(crate) enum Next<C> {
    /// Dial in order; `shadow` is the challenger this reconnect was pointed
    /// at, when it was, whether or not the announce says so.
    Dial {
        order: Vec<C>,
        announce: Option<Announce<C>>,
        shadow: Option<C>,
    },
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
                let (order, shadowed) = self.dial_order_for_connect(now);
                let shadow = match shadowed {
                    Some(Announce::Shadow { challenger, .. }) => Some(challenger),
                    _ => None,
                };
                Next::Dial {
                    order,
                    announce: announce.or(shadowed),
                    shadow,
                }
            }
        }
    }

    pub(crate) fn stats(&self, addr: C) -> AddrStats {
        self.per_addr.get(&addr.key()).copied().unwrap_or_default()
    }

    /// A verdict may run: the pin was chosen, or last judged, at least
    /// the timing's `delivery_verdict_interval` ago.
    pub(crate) fn verdict_is_allowed(&self, now: Instant) -> bool {
        self.last_verdict_at.or(self.chosen_at).is_some_and(|at| {
            now.saturating_duration_since(at) >= timing().delivery_verdict_interval
        })
    }

    /// Judge the pin on the delivery booked since it was last judged, when
    /// there is enough of it. Leaves the pin's age alone, and leaves the
    /// samples to keep gathering when there is not enough.
    pub(crate) fn judge_on_delivery(&mut self, now: Instant, pin: C) -> Option<Announce<C>> {
        let mut promoted = None;
        let announce = match self.delivery_verdict(now, pin) {
            DeliveryVerdict::Challenged {
                challenger,
                challenger_rate,
                pin_rate,
            } if self.delivery_leader == Some(challenger) => {
                self.pinned = Some(challenger);
                promoted = Some(challenger);
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
        self.reset_judged(now, promoted);
        Some(announce)
    }

    /// A verdict judges every measured candidate afresh from here, except a
    /// challenger it just pinned, which was pinned on that measure and keeps
    /// it as the pin's own. A challenger still short of a measure keeps what
    /// it has booked: one that only gets a few fetches per shadow connection
    /// would otherwise lose them to every verdict on another challenger and
    /// never be judged.
    fn reset_judged(&mut self, now: Instant, promoted: Option<C>) {
        let keep = promoted.map(Candidate::key);
        for (key, stats) in &mut self.per_addr {
            if Some(*key) != keep && stats.delivery.measured_rate(now).is_some() {
                stats.delivery = Delivery::default();
            }
        }
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

    /// Every race judges the pin afresh from here.
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
        if self.shadow_owed == Some(challenger) {
            self.shadow_owed = None;
        } else {
            self.idle_shadows = 0;
        }
        let order = std::iter::once(challenger)
            .chain(order.into_iter().filter(|addr| *addr != challenger))
            .collect();
        (order, Some(Announce::Shadow { challenger, pin }))
    }

    /// The challenger this reconnect goes to, and the pin it stands in for,
    /// if it is time for one: among the candidates not yet measured, by the
    /// same measure a verdict holds them to, the one with the most fetches
    /// booked, so one challenger reaches a verdict instead of every
    /// challenger sharing the shadows and none arriving; never one that last
    /// refused and never one that has yet to connect at all — an address that
    /// swallows packets would hold this reconnect for the whole attempt
    /// limit. `None` while the pin is too young, too recently or too often
    /// shadowed, not yet measured itself, or already judged against every
    /// challenger.
    ///
    /// A challenger owed a reconnect, because its last shadow connection
    /// ended before it was measured, skips the pacing: a server that drops
    /// connections early would otherwise end every shadow short of a verdict
    /// and leave the pin unjudged for good.
    pub(crate) fn shadow_candidate(&self, now: Instant) -> Option<(C, C)> {
        let pin = self.pinned?;
        if now.saturating_duration_since(self.chosen_at?) < timing().shadow_min_pin_age {
            return None;
        }
        // The pin must be measured before any challenger is worth a look.
        self.stats(pin).delivery.measured_rate(now)?;
        let unmeasured = |addr: &C| {
            let stats = self.stats(*addr);
            *addr != pin
                && !stats.failing()
                && stats.connect_ewma.is_some()
                && stats.delivery.measured_rate(now).is_none()
        };
        if let Some(owed) = self.shadow_owed.filter(unmeasured) {
            return Some((owed, pin));
        }
        if self
            .last_shadow_at
            .is_some_and(|at| now.saturating_duration_since(at) < timing().shadow_interval)
        {
            return None;
        }
        if self.connects_since_shadow < SHADOW_EVERY_CONNECTS {
            return None;
        }
        self.candidates
            .iter()
            .filter(|addr| unmeasured(addr))
            .map(|addr| (*addr, self.stats(*addr)))
            .max_by_key(|(_, stats)| {
                (
                    stats.delivery.samples,
                    std::cmp::Reverse(stats.connect_ewma),
                )
            })
            .map(|(addr, _)| (addr, pin))
    }

    /// The connection a reconnect pointed at `challenger` has ended after
    /// `responses` answers from it. Ended before the challenger was
    /// measured, it is owed the next reconnect, for as long as its
    /// connections keep answering and up to [`SHADOW_RETRIES`] in a row that
    /// did not; measured, the verdict takes it from here.
    pub fn shadow_ended(&mut self, challenger: C, responses: u64, now: Instant) {
        if !self.candidates.contains(&challenger) {
            if self.shadow_owed == Some(challenger) {
                self.shadow_owed = None;
            }
            return;
        }
        if self.stats(challenger).delivery.measured_rate(now).is_some() {
            self.idle_shadows = 0;
            self.shadow_owed = None;
            return;
        }
        self.idle_shadows = if responses == 0 {
            self.idle_shadows.saturating_add(1)
        } else {
            0
        };
        self.shadow_owed = (self.idle_shadows < SHADOW_RETRIES).then_some(challenger);
    }

    pub(crate) fn failed_race_is_recent(&self, now: Instant) -> bool {
        self.last_race_failed_at
            .is_some_and(|at| now.saturating_duration_since(at) < timing().failed_race_holdoff)
    }

    pub(crate) fn pin_is_due(&self, now: Instant) -> bool {
        self.chosen_at
            .is_none_or(|at| now.saturating_duration_since(at) >= timing().replan_interval)
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
            shadow_owed: None,
            idle_shadows: 0,
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
            Next::Dial {
                order,
                shadow,
                announce,
            } => Attempt::Dial {
                order,
                shadow,
                announce,
            },
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
        if self.shadow_owed.is_some_and(|owed| !unique.contains(&owed)) {
            self.shadow_owed = None;
            self.idle_shadows = 0;
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
    pub fn connected(&mut self, candidate: C, elapsed: Duration, now: Instant) {
        let stats = self.per_addr.entry(candidate.key()).or_default();
        stats.connect_ewma = Some(ewma(stats.connect_ewma, elapsed));
        stats.consecutive_failures = 0;
        // A connect that lands on another candidate while the pin is failing
        // moves the pin there, as a lease does: the dial fell through the pin
        // to this candidate, and the connections it opens are the ones now
        // carrying the work. Waiting for the suspect race would leave the pin
        // on a candidate nothing connects to until the next dial, which may
        // not come while every lane is open elsewhere.
        if !self.racing
            && self
                .pinned
                .is_some_and(|pin| pin != candidate && self.stats(pin).failing())
        {
            self.pinned = Some(candidate);
            self.chosen_at = Some(now);
            if self.pending == Some(RaceReason::Suspect) {
                self.pending = None;
            }
        }
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
    pub fn body_latency(&mut self, candidate: C, elapsed: Duration, now: Instant) {
        if !self.candidates.contains(&candidate) {
            return;
        }
        let stats = self.per_addr.entry(candidate.key()).or_default();
        stats.body_latency_ewma = Some(ewma(stats.body_latency_ewma, elapsed));
        // An answer is booked once the article is processed, which may be
        // after the connection that fetched it has ended and been counted as
        // one that never answered. The answer stands: it ends the idle streak,
        // and a challenger it left unmeasured is owed its next reconnect.
        if self.pinned != Some(candidate) && stats.delivery.measured_rate(now).is_none() {
            self.idle_shadows = 0;
            if self.shadow_owed.is_none() && !stats.failing() {
                self.shadow_owed = Some(candidate);
            }
        }
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
