//! Which of a server's resolved addresses new connections dial.
//!
//! A provider hostname usually resolves to several addresses, and they are
//! rarely equally close. Spreading a server's connections across all of them
//! runs the transfer at the average of their speeds. Instead, the first
//! connect races every resolved address, pins the one that answers first, and
//! every later connect dials the pin.
//!
//! The plan only races again when it has a reason to: the pin keeps refusing
//! connections, the provider just lifted an over-limit holdoff, or the plan
//! has aged past [`ADDRESS_REPLAN_INTERVAL`]. A race runs inside one connect
//! whether or not the server is busy: the winning stream is the connection
//! that connect returns, so a race costs only its losing handshakes, and a
//! race of any outcome restarts the pin's age, so age alone races at most once
//! per interval. Existing connections stay where they are; only connections
//! opened after a repin go to the new address.
//!
//! A connect that finds the pin refusing tries the remaining candidates in
//! order of their measured connect time, so one bad address never fails a
//! connect the server could have served. A race loser that merely timed out
//! is not booked as a failure: it was slower than the winner, not broken.
//!
//! When a race finds no address at all and nothing is pinned, connects dial
//! the known candidates one at a time for [`FAILED_RACE_HOLDOFF`] instead of
//! racing again, so a server that is down does not start a burst of race
//! threads on every reconnect attempt.
//!
//! A race measures handshakes, and the address that shakes hands fastest is
//! not always the one that delivers articles fastest. So while the pin is
//! busy, the plan also measures delivery: every warm article fetch books its
//! bytes and wire time against the address that served it, and now and then a
//! connection the server was reopening anyway is pointed at a challenger
//! instead of the pin — at most one in [`SHADOW_EVERY_CONNECTS`], no sooner
//! than [`SHADOW_INTERVAL`] apart, never an extra connection and never a
//! handshake the server would not have paid for regardless. Once the pin and a
//! challenger have each delivered at least [`DELIVERY_MIN_SAMPLES`] articles
//! over at least [`DELIVERY_MIN_WIRE`] of wire time, the next connect judges
//! that evidence, no sooner than [`DELIVERY_VERDICT_INTERVAL`] after the pin
//! was chosen or last judged: a challenger that delivered
//! [`DELIVERY_REPIN_RATIO`] times the pin's per-connection rate at two
//! verdicts running takes the pin, and otherwise the pin stays, with no race
//! and no losing handshakes either way. Every verdict judges the next one from
//! fresh samples, so a busy server settles a faster address within a few
//! verdicts rather than a few intervals.
//!
//! Verdicts run on their own clock and leave the pin's age alone: neither a
//! verdict nor a pin moved on one restarts it. The pin still races on age,
//! busy or idle, at least once per [`ADDRESS_REPLAN_INTERVAL`], because a race
//! is also where the hostname is resolved again, and a server whose delivery
//! keeps being judged would otherwise never learn of an address the provider
//! added or withdrew. A server that never gathers the evidence — an idle one,
//! or one whose connections never turned over — only ever races.

use crate::candidate_plan::{CandidatePlan, Next};
type PlanState = CandidatePlan<SocketAddr>;
use std::io;
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant};

use tracing::{debug, info};

/// How old a pin may get before the next connect races the server's
/// addresses again. Every race, won or failed, restarts the pin's age, so this
/// is also the most often a server races on age alone, busy or idle. A
/// delivery verdict does not restart it, even one that moves the pin, so a
/// server whose connects keep coming resolves its hostname again at least
/// this often.
pub const ADDRESS_REPLAN_INTERVAL: Duration = Duration::from_mins(10);

/// How long after a race in which no address answered, with nothing pinned,
/// connects dial the known candidates one after another instead of racing.
/// A race dials each candidate on its own thread, each held for up to
/// [`ADDRESS_ATTEMPT_LIMIT`]; against a server that is down, racing on every
/// reconnect attempt would keep those threads blocked for nothing. A
/// minute is long enough to absorb a reconnect loop and short enough that a
/// server coming back is raced, and pinned, soon after.
pub const FAILED_RACE_HOLDOFF: Duration = Duration::from_mins(1);

/// Longest one connect attempt to one address may take, whatever the connect
/// timeout, so an address that swallows packets neither holds a race thread
/// for long nor delays the fall-over to the next candidate.
pub const ADDRESS_ATTEMPT_LIMIT: Duration = Duration::from_secs(15);

/// Consecutive connect failures on the pin that make it suspect. A suspect pin
/// is raced again on the next connect, however young it is.
pub const SUSPECT_AFTER_FAILURES: u32 = 2;

/// Most addresses a race dials at once, each on its own thread for the length
/// of one connect. Far more than a server resolves to; it bounds the threads
/// a resolver answering with an absurd number of addresses could start. Any
/// beyond it are raced in further batches of this many, in the order they
/// resolved.
const MAX_RACE_CANDIDATES: usize = 256;

/// Warm article fetches an address must have served since the pin was last
/// judged before its delivery rate counts as evidence, for the pin and for a
/// challenger alike. A busy server fills this in seconds; an idle one never
/// does, and its pin only ever races on age.
pub const DELIVERY_MIN_SAMPLES: u32 = 16;

/// Wire time an address's booked fetches must add up to, besides
/// [`DELIVERY_MIN_SAMPLES`], before its delivery rate counts as evidence. A
/// handful of small articles finishes within the round trips it costs to ask
/// for them and says more about latency than about throughput; this keeps
/// them from out-voting an address that was moving large ones.
pub const DELIVERY_MIN_WIRE: Duration = Duration::from_secs(10);

/// Least time between two delivery verdicts, and least age of a pin before its
/// first. A busy server measures the pin and a challenger well within it, so
/// it is what paces the two verdicts that move a pin; it also keeps one
/// verdict's samples from being judged again before fresh ones have come in.
pub const DELIVERY_VERDICT_INTERVAL: Duration = Duration::from_mins(1);

/// How much faster, per connection, a challenger must have delivered than the
/// pin to take the pin on that evidence alone. Addresses of one provider are
/// usually within a few percent of each other, and moving the pin for less
/// than this would just chase noise from one address to the next.
pub const DELIVERY_REPIN_RATIO: f64 = 1.15;

/// Youngest a pin may be when a reconnect is first pointed at a challenger.
/// A fresh pin has yet to show what it delivers, and the first connections
/// after a race are the ones a download is waiting on.
pub const SHADOW_MIN_PIN_AGE: Duration = Duration::from_secs(30);

/// Least time between two reconnects pointed at a challenger.
pub const SHADOW_INTERVAL: Duration = Duration::from_secs(30);

/// One reconnect in this many, counted since the last one pointed at a
/// challenger, may be pointed at a challenger; the rest go to the pin. A
/// challenger keeps whatever it is given for the life of that connection, so
/// this bounds the share of a busy server's connections that are off the pin
/// at any time.
pub const SHADOW_EVERY_CONNECTS: u32 = 8;

/// How long booked delivery stays evidence. A server idle for longer than
/// this has samples from a different time, and possibly the pin's from one
/// time and a challenger's from another, so they are not judged.
pub const DELIVERY_EVIDENCE_AGE: Duration = ADDRESS_REPLAN_INTERVAL;

/// Why the pin moved, or a race ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RaceReason {
    /// Nothing was pinned yet.
    Initial,
    /// The pin aged past [`ADDRESS_REPLAN_INTERVAL`].
    Interval,
    /// An over-limit holdoff cleared, so the provider's view of this client
    /// may have changed.
    OverLimitCleared,
    /// The pin failed [`SUSPECT_AFTER_FAILURES`] connects in a row.
    Suspect,
    /// A challenger out-delivered the pin by [`DELIVERY_REPIN_RATIO`]. No
    /// race ran: the pin moved on measured delivery alone.
    Delivery,
}

impl RaceReason {
    pub const ALL: [RaceReason; 5] = [
        RaceReason::Initial,
        RaceReason::Interval,
        RaceReason::OverLimitCleared,
        RaceReason::Suspect,
        RaceReason::Delivery,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            RaceReason::Initial => "initial",
            RaceReason::Interval => "interval",
            RaceReason::OverLimitCleared => "over_limit_cleared",
            RaceReason::Suspect => "suspect",
            RaceReason::Delivery => "delivery",
        }
    }

    pub(crate) fn index(self) -> usize {
        match self {
            RaceReason::Initial => 0,
            RaceReason::Interval => 1,
            RaceReason::OverLimitCleared => 2,
            RaceReason::Suspect => 3,
            RaceReason::Delivery => 4,
        }
    }
}

/// One candidate address as the plan last measured it.
#[derive(Debug, Clone, PartialEq)]
pub struct AddressSnapshot {
    pub address: SocketAddr,
    /// Smoothed TCP connect time, once any connect to it has finished.
    pub connect_time: Option<Duration>,
    /// Smoothed article fetch time on connections to it.
    pub body_latency: Option<Duration>,
    pub consecutive_failures: u32,
    /// Bytes per second of wire time one connection to it delivered, over the
    /// warm fetches booked since the pin was last judged.
    pub delivery_bytes_per_second: Option<f64>,
    /// Warm fetches booked against it since the pin was last judged.
    pub delivery_samples: u32,
}

/// A server's address plan at one moment.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AddressPlanSnapshot {
    pub pinned: Option<SocketAddr>,
    pub addresses: Vec<AddressSnapshot>,
    /// Races that pinned an address.
    pub races_won: u64,
    /// Races in which no address answered.
    pub races_failed: u64,
    /// Pin changes, by the reason of the race or the delivery verdict that
    /// made them.
    pub repins: Vec<(RaceReason, u64)>,
}

/// Opens connections to one address for a plan. The plan decides which
/// address; the dialer only knows how.
pub(crate) trait AddressDialer: Send + Sync + 'static {
    type Stream: Send + 'static;

    fn resolve(&self) -> io::Result<Vec<SocketAddr>>;

    fn dial(&self, addr: SocketAddr) -> io::Result<Self::Stream>;
}

/// Plain TCP to a hostname's resolved addresses.
pub(crate) struct TcpDialer {
    host: String,
    port: u16,
    timeout: Duration,
    egress: crate::egress::SocketEgress,
}

impl TcpDialer {
    pub(crate) fn new(host: &str, port: u16, timeout: Duration) -> Self {
        Self {
            host: host.to_string(),
            port,
            timeout: timeout.min(ADDRESS_ATTEMPT_LIMIT),
            egress: crate::egress::SocketEgress::System,
        }
    }
    pub(crate) fn with_egress(mut self, egress: crate::egress::SocketEgress) -> Self {
        self.egress = egress;
        self
    }
}

impl AddressDialer for TcpDialer {
    type Stream = TcpStream;

    fn resolve(&self) -> io::Result<Vec<SocketAddr>> {
        Ok((self.host.as_str(), self.port)
            .to_socket_addrs()?
            .filter(|a| self.egress.supports_address(a.ip()))
            .collect())
    }

    fn dial(&self, addr: SocketAddr) -> io::Result<TcpStream> {
        self.egress.connect_blocking(addr, self.timeout)
    }
}

/// The address plan a new connection to one server goes through.
#[derive(Clone)]
pub struct AddressRoute {
    pub(crate) plan: Arc<AddressPlan>,
}

impl AddressRoute {
    /// Open a TCP socket to the address this route's plan picks.
    pub(crate) fn connect_tcp(
        &self,
        host: &str,
        port: u16,
        timeout: Duration,
    ) -> io::Result<(TcpStream, SocketAddr)> {
        let dialer = Arc::new(TcpDialer::new(host, port, timeout));
        self.plan.connect(&dialer)
    }

    /// Watch the session being set up on a socket this route opened to
    /// `addr`.
    pub(crate) fn watch_setup(&self, addr: SocketAddr) -> SetupWatch {
        SetupWatch {
            plan: Arc::clone(&self.plan),
            addr,
            reached: false,
        }
    }
}

/// One session's setup on a connected socket, from the connect to the point
/// where the server has answered over it. Dropped before that point, it books
/// a failed setup against the address: a handshake that failed, a greeting
/// that never came, or a setup abandoned because it ran out of time.
pub(crate) struct SetupWatch {
    plan: Arc<AddressPlan>,
    addr: SocketAddr,
    reached: bool,
}

impl SetupWatch {
    /// The server answered over this socket. What it said is the server's
    /// answer, not the address's.
    pub(crate) fn reached_server(mut self) {
        self.reached = true;
        self.plan.record_setup(self.addr, true);
    }
}

impl Drop for SetupWatch {
    fn drop(&mut self) {
        if !self.reached {
            self.plan.record_setup(self.addr, false);
        }
    }
}

/// The address state of one server. See the module docs.
pub struct AddressPlan {
    label: String,
    state: Mutex<PlanState>,
    race_finished: Condvar,
    /// A clock the tests hold still and move by hand, so nothing they assert
    /// depends on how fast the machine runs them.
    #[cfg(test)]
    frozen_clock: Mutex<Option<Instant>>,
}

impl AddressPlan {
    pub(crate) fn new(label: String) -> Self {
        Self {
            label,
            state: Mutex::new(PlanState::default()),
            race_finished: Condvar::new(),
            #[cfg(test)]
            frozen_clock: Mutex::new(None),
        }
    }

    /// Plain data behind the lock, so a panic elsewhere must not stop every
    /// later connect.
    fn state(&self) -> MutexGuard<'_, PlanState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The moment every plan decision is dated by.
    fn now(&self) -> Instant {
        #[cfg(test)]
        if let Some(frozen) = *self
            .frozen_clock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            return frozen;
        }
        Instant::now()
    }

    /// Open a stream through `dialer` to the address this plan picks, racing
    /// the candidates first when a race is due.
    pub(crate) fn connect<D: AddressDialer>(
        self: &Arc<Self>,
        dialer: &Arc<D>,
    ) -> io::Result<(D::Stream, SocketAddr)> {
        loop {
            let now = self.now();
            let next = self.state().next(now);
            match next {
                Next::Dial(order, announce) => {
                    if let Some(announce) = announce {
                        announce.log(&self.label);
                    }
                    return self.dial_in_order(dialer.as_ref(), &order);
                }
                Next::Race(reason) => return self.race(dialer, reason),
                Next::Fail(kind, message) => return Err(io::Error::new(kind, message)),
                Next::Wait(generation) => {
                    let mut state = self.state();
                    while state.racing && state.generation == generation {
                        state = self
                            .race_finished
                            .wait(state)
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                    }
                    if state.pinned.is_none() {
                        let (kind, message) = state.last_race_error.clone().unwrap_or((
                            io::ErrorKind::AddrNotAvailable,
                            "no address answered".to_string(),
                        ));
                        return Err(io::Error::new(kind, message));
                    }
                }
            }
        }
    }

    fn dial_in_order<D: AddressDialer>(
        &self,
        dialer: &D,
        order: &[SocketAddr],
    ) -> io::Result<(D::Stream, SocketAddr)> {
        let mut last_error = None;
        for &addr in order {
            let started = Instant::now();
            match dialer.dial(addr) {
                Ok(stream) => {
                    self.record_connect(addr, Some(started.elapsed()));
                    return Ok((stream, addr));
                }
                Err(error) => {
                    debug!(
                        server = %self.label,
                        address = %addr,
                        error = %error,
                        "connect to planned address failed; trying the next candidate"
                    );
                    self.record_connect(addr, None);
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::AddrNotAvailable, "no candidate address")
        }))
    }

    fn race<D: AddressDialer>(
        self: &Arc<Self>,
        dialer: &Arc<D>,
        reason: RaceReason,
    ) -> io::Result<(D::Stream, SocketAddr)> {
        let mut ticket = RaceTicket {
            plan: self,
            reason,
            done: false,
        };
        let resolved = match dialer.resolve() {
            Ok(addrs) => distinct_addrs(addrs),
            Err(error) => {
                debug!(server = %self.label, error = %error, "address race could not resolve");
                Vec::new()
            }
        };
        let candidates = if resolved.is_empty() {
            // A resolver hiccup must not strand a server whose addresses
            // were fine a moment ago.
            self.state().candidates.clone()
        } else {
            resolved.clone()
        };
        if candidates.is_empty() {
            let error = io::Error::other(weaver_tunnel::pipe::ResolutionFailed);
            ticket.finish(Err(&error), None, Vec::new());
            return Err(error);
        }

        let mut last_error = None;
        let mut slow = Vec::new();
        // The first address to connect whose sessions have not been reaching
        // the server. It wins only if no other address of its batch connects.
        let mut fallback = None;
        // The next batch is dialled only once every address of this one has
        // answered without connecting.
        for batch in candidates.chunks(MAX_RACE_CANDIDATES) {
            let (tx, rx) = mpsc::channel();
            for &addr in batch {
                let tx = tx.clone();
                let dialer = Arc::clone(dialer);
                let plan = Arc::clone(self);
                let spawned = std::thread::Builder::new()
                    .name("nntp-address-race".into())
                    .spawn(move || {
                        let started = Instant::now();
                        let result = dialer.dial(addr);
                        let elapsed = started.elapsed();
                        match &result {
                            Ok(_) => plan.record_connect(addr, Some(elapsed)),
                            // Only slow. Whether that counts against the
                            // address depends on whether anything won, which
                            // the race decides below.
                            Err(error) if is_slow(error) => {}
                            Err(_) => plan.record_connect(addr, None),
                        }
                        // A loser's stream is dropped here once the winner has
                        // stopped listening, which closes it.
                        let _ = tx.send((addr, result, elapsed));
                    });
                if let Err(error) = spawned {
                    last_error = Some(error);
                }
            }
            drop(tx);

            for (addr, result, elapsed) in rx {
                match result {
                    Ok(stream) => {
                        if self.state().stats(addr).setup_failures > 0 {
                            fallback.get_or_insert((stream, addr, elapsed));
                            continue;
                        }
                        // Addresses that timed out lost to this one; they are
                        // not booked as failures.
                        ticket.finish(Ok(addr), Some(elapsed), resolved);
                        return Ok((stream, addr));
                    }
                    Err(error) => {
                        if is_slow(&error) {
                            slow.push(addr);
                        }
                        last_error = Some(error);
                    }
                }
            }
            if let Some((stream, addr, elapsed)) = fallback.take() {
                ticket.finish(Ok(addr), Some(elapsed), resolved);
                return Ok((stream, addr));
            }
        }
        // Nothing answered, so a timeout was a failure after all.
        for addr in slow {
            self.record_connect(addr, None);
        }
        let error = last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::AddrNotAvailable, "no address answered")
        });
        ticket.finish(Err(&error), None, resolved);
        Err(error)
    }

    /// Book one finished connect to `addr`: its time when it connected,
    /// `None` when it failed.
    fn record_connect(&self, addr: SocketAddr, connected_in: Option<Duration>) {
        let now = self.now();
        let mut state = self.state();
        let pending = state.pending;
        match connected_in {
            Some(elapsed) => state.connected(addr, elapsed, now),
            None => state.failed(addr, now),
        }
        let suspect =
            pending != Some(RaceReason::Suspect) && state.pending == Some(RaceReason::Suspect);
        drop(state);
        if suspect {
            tracing::warn!(server = %self.label, address = %addr, failures = SUSPECT_AFTER_FAILURES,
                "pinned address keeps refusing connections; racing the addresses again");
        }
    }

    /// Book one session's setup on a socket connected to `addr`: whether the
    /// server answered over it.
    pub(crate) fn record_setup(&self, addr: SocketAddr, reached: bool) {
        let mut state = self.state();
        let pending = state.pending;
        state.setup(addr, reached, self.now());
        let suspect =
            pending != Some(RaceReason::Suspect) && state.pending == Some(RaceReason::Suspect);
        drop(state);
        if suspect {
            tracing::warn!(server = %self.label, address = %addr, failures = SUSPECT_AFTER_FAILURES,
                "pinned address connects but the server does not answer; racing the addresses again");
        }
    }

    /// Book how long an article fetch took on a connection to `ip`. Ignored
    /// for an address the plan does not know.
    pub(crate) fn record_body_latency(&self, ip: IpAddr, elapsed: Duration) {
        let mut state = self.state();
        let candidate = state
            .candidates
            .iter()
            .find(|addr| addr.ip() == ip)
            .copied();
        if let Some(candidate) = candidate {
            state.body_latency(candidate, elapsed, self.now());
        }
    }

    /// Book one warm article fetch on a connection to `ip`: its decoded bytes
    /// and the wire time they took. Ignored for an address the plan does not
    /// know, and for a fetch that moved nothing.
    ///
    /// The caller keeps a connection's first fetch out of this: it pays for
    /// the tail of the handshake and runs through a congestion window still
    /// opening, and would count against whichever address was connected to
    /// most recently.
    pub(crate) fn record_delivery(&self, ip: IpAddr, bytes: u64, wire: Duration) {
        let mut state = self.state();
        let candidate = state
            .candidates
            .iter()
            .find(|addr| addr.ip() == ip)
            .copied();
        if let Some(candidate) = candidate {
            state.delivered(candidate, bytes, wire, self.now());
        }
    }

    /// The provider accepts new connections again after refusing them. Its
    /// addresses may have been rebalanced meanwhile, so the next connect races
    /// again, busy or not: this is exactly when a fresh look is worth the
    /// losing handshakes.
    pub(crate) fn note_over_limit_cleared(&self) {
        self.state().over_limit_cleared(self.now());
    }

    pub fn snapshot(&self) -> AddressPlanSnapshot {
        let state = self.state();
        AddressPlanSnapshot {
            pinned: state.pinned,
            addresses: state
                .candidates
                .iter()
                .map(|&address| {
                    let stats = state
                        .per_addr
                        .get(&address.ip())
                        .copied()
                        .unwrap_or_default();
                    AddressSnapshot {
                        address,
                        connect_time: stats.connect_ewma,
                        body_latency: stats.body_latency_ewma,
                        consecutive_failures: stats.consecutive_failures.max(stats.setup_failures),
                        delivery_bytes_per_second: stats.delivery.bytes_per_second(),
                        delivery_samples: stats.delivery.samples,
                    }
                })
                .collect(),
            races_won: state.races_won,
            races_failed: state.races_failed,
            repins: RaceReason::ALL
                .iter()
                .map(|reason| (*reason, state.repins[reason.index()]))
                .collect(),
        }
    }

    /// Make the pin look `age` older, as if the clock had moved on.
    #[cfg(test)]
    pub(crate) fn age_pin_by(&self, age: Duration) {
        let mut state = self.state();
        state.chosen_at = state.chosen_at.and_then(|at| at.checked_sub(age));
    }

    /// Make the last failed race look `age` older, as if the clock had moved on.
    #[cfg(test)]
    pub(crate) fn age_failed_race_by(&self, age: Duration) {
        let mut state = self.state();
        state.last_race_failed_at = state.last_race_failed_at.and_then(|at| at.checked_sub(age));
    }

    /// Stop the plan's clock where it stands. From here on only
    /// [`Self::advance`] moves it.
    #[cfg(test)]
    pub(crate) fn freeze_clock(&self) {
        let now = self.now();
        *self.frozen_clock.lock().unwrap() = Some(now);
    }

    /// Move the frozen clock forward.
    #[cfg(test)]
    pub(crate) fn advance(&self, by: Duration) {
        let mut clock = self.frozen_clock.lock().unwrap();
        let frozen = clock.expect("advance needs a frozen clock");
        *clock = Some(frozen + by);
    }
}

/// The right to run a race. Dropping it unfinished (a panic mid-race) still
/// releases the callers waiting on the race.
struct RaceTicket<'a> {
    plan: &'a AddressPlan,
    reason: RaceReason,
    done: bool,
}

impl RaceTicket<'_> {
    fn finish(
        &mut self,
        outcome: std::result::Result<SocketAddr, &io::Error>,
        winner_connect: Option<Duration>,
        resolved: Vec<SocketAddr>,
    ) {
        self.done = true;
        let plan = self.plan;
        let reason = self.reason;
        let mut state = plan.state();
        if !resolved.is_empty() {
            state
                .per_addr
                .retain(|ip, _| resolved.iter().any(|addr| addr.ip() == *ip));
            state.candidates = resolved;
        }
        state.racing = false;
        state.pending = None;
        state.generation = state.generation.wrapping_add(1);
        state.reset_delivery();
        state.delivery_leader = None;
        state.last_verdict_at = None;
        let now = plan.now();
        let previous = state.pinned;
        match outcome {
            Ok(winner) => {
                state.pinned = Some(winner);
                state.chosen_at = Some(now);
                state.races_won += 1;
                state.last_race_error = None;
                state.last_race_failed_at = None;
                if previous.is_some_and(|previous| previous != winner) {
                    state.repins[reason.index()] += 1;
                }
                let previous_body_latency = previous
                    .and_then(|previous| state.per_addr.get(&previous.ip()))
                    .and_then(|stats| stats.body_latency_ewma);
                let candidates = state.candidates.len();
                drop(state);
                if previous == Some(winner) {
                    info!(
                        server = %plan.label,
                        reason = reason.as_str(),
                        address = %winner,
                        connect_ms = winner_connect.map(|d| d.as_millis()),
                        candidates,
                        "address race kept the pinned address"
                    );
                } else {
                    info!(
                        server = %plan.label,
                        reason = reason.as_str(),
                        address = %winner,
                        previous = previous.map(|addr| addr.to_string()),
                        previous_body_latency_ms = previous_body_latency.map(|d| d.as_millis()),
                        connect_ms = winner_connect.map(|d| d.as_millis()),
                        candidates,
                        "address race pinned a new address"
                    );
                }
            }
            Err(error) => {
                state.races_failed += 1;
                state.last_race_error = Some((error.kind(), error.to_string()));
                if previous.is_some() {
                    // Keep the old pin, and do not race again on age alone
                    // straight away.
                    state.chosen_at = Some(now);
                } else {
                    state.last_race_failed_at = Some(now);
                }
                let candidates = state.candidates.len();
                drop(state);
                info!(
                    server = %plan.label,
                    reason = reason.as_str(),
                    error = %error,
                    candidates,
                    kept = previous.map(|addr| addr.to_string()),
                    "address race found no address that answers"
                );
            }
        }
        plan.race_finished.notify_all();
    }
}

impl Drop for RaceTicket<'_> {
    fn drop(&mut self) {
        if !self.done {
            let error = io::Error::other("address race abandoned");
            self.finish(Err(&error), None, Vec::new());
        }
    }
}

/// A connect that ran out of time rather than being turned away.
fn is_slow(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

fn distinct_addrs(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let mut distinct = Vec::with_capacity(addrs.len());
    for addr in addrs {
        if !distinct.contains(&addr) {
            distinct.push(addr);
        }
    }
    distinct
}

#[cfg(test)]
mod tests;
