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
//! handshake the server would not have paid for regardless. When the pin's age
//! comes due and both the pin and a challenger have delivered at least
//! [`DELIVERY_MIN_SAMPLES`] articles, that evidence decides instead of a race:
//! a challenger that delivered [`DELIVERY_REPIN_RATIO`] times the pin's
//! per-connection rate at two verdicts running takes the pin, and otherwise
//! the pin stays, with no losing handshakes paid either way. A pin without
//! that evidence — an idle server, or one whose connections never turned over
//! — races on age as before, and every [`VERDICTS_BETWEEN_RACES`] verdicts
//! the pin races regardless so the hostname is resolved again.

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

/// How old a pin may get before the next connect races the server's
/// addresses again. Every race, won or failed, restarts the pin's age, so this
/// is also the most often a server races on age alone, busy or idle.
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

/// Most addresses a race dials at once. Each gets its own thread for the
/// length of one connect, and a server with more addresses than this is raced
/// in batches of this many, in the order they resolved.
const MAX_RACE_CANDIDATES: usize = 16;

/// Weight of a new sample in the per-address averages.
const EWMA_WEIGHT: f64 = 0.25;

/// Warm article fetches an address must have served since the pin was last
/// judged before its delivery rate counts as evidence, for the pin and for a
/// challenger alike. A busy server fills this in seconds; an idle one never
/// does, and races on age as before.
pub const DELIVERY_MIN_SAMPLES: u32 = 16;

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

/// Consecutive verdicts settled on delivery before the pin's age races anyway.
/// A race is also where the hostname is resolved again, and a busy server
/// whose verdicts keep coming would otherwise never learn of an address the
/// provider added or withdrew. At [`ADDRESS_REPLAN_INTERVAL`] this is about
/// one race an hour.
pub const VERDICTS_BETWEEN_RACES: u32 = 6;

/// How long booked delivery stays evidence. A server idle for longer than
/// this has samples from a different time, and possibly the pin's from one
/// time and a challenger's from another, so it races as if it had none.
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
    /// A challenger out-delivered the aged pin by [`DELIVERY_REPIN_RATIO`]. No
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

    fn index(self) -> usize {
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
}

impl TcpDialer {
    pub(crate) fn new(host: &str, port: u16, timeout: Duration) -> Self {
        Self {
            host: host.to_string(),
            port,
            timeout: timeout.min(ADDRESS_ATTEMPT_LIMIT),
        }
    }
}

impl AddressDialer for TcpDialer {
    type Stream = TcpStream;

    fn resolve(&self) -> io::Result<Vec<SocketAddr>> {
        Ok((self.host.as_str(), self.port).to_socket_addrs()?.collect())
    }

    fn dial(&self, addr: SocketAddr) -> io::Result<TcpStream> {
        TcpStream::connect_timeout(&addr, self.timeout)
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
}

#[derive(Debug, Default, Clone, Copy)]
struct AddrStats {
    connect_ewma: Option<Duration>,
    consecutive_failures: u32,
    body_latency_ewma: Option<Duration>,
    delivery: Delivery,
}

/// What one address's connections delivered since the pin was last judged:
/// warm fetches only, each booked as its bytes and its own wire time, so the
/// quotient is what one connection to the address moves per second whatever
/// the number of connections that happened to land there.
#[derive(Debug, Default, Clone, Copy)]
struct Delivery {
    bytes: u64,
    wire: Duration,
    samples: u32,
    last_at: Option<Instant>,
}

impl Delivery {
    fn book(&mut self, bytes: u64, wire: Duration, now: Instant) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.wire = self.wire.saturating_add(wire);
        self.samples = self.samples.saturating_add(1);
        self.last_at = Some(now);
    }

    fn bytes_per_second(self) -> Option<f64> {
        (self.samples > 0 && !self.wire.is_zero())
            .then(|| self.bytes as f64 / self.wire.as_secs_f64())
    }

    /// The rate, once enough fetches stand behind it to be evidence and while
    /// the latest of them is recent enough to still describe the address.
    fn measured_rate(self, now: Instant) -> Option<f64> {
        let fresh = self
            .last_at
            .is_some_and(|at| now.saturating_duration_since(at) < DELIVERY_EVIDENCE_AGE);
        (fresh && self.samples >= DELIVERY_MIN_SAMPLES)
            .then(|| self.bytes_per_second())
            .flatten()
    }
}

/// What the delivery booked since the pin was last judged says about it.
enum DeliveryVerdict {
    /// A challenger out-delivered the pin by [`DELIVERY_REPIN_RATIO`].
    Challenged {
        challenger: SocketAddr,
        challenger_rate: f64,
        pin_rate: f64,
    },
    /// The best measured challenger did not.
    Keep {
        best: SocketAddr,
        best_rate: f64,
        pin_rate: f64,
    },
    /// The pin or every challenger is short of [`DELIVERY_MIN_SAMPLES`].
    Unmeasured,
}

/// What a connect decided, told once the plan's lock is released.
enum Announce {
    Repinned {
        challenger: SocketAddr,
        challenger_rate: f64,
        pin: SocketAddr,
        pin_rate: f64,
    },
    Leading {
        challenger: SocketAddr,
        challenger_rate: f64,
        pin: SocketAddr,
        pin_rate: f64,
    },
    Kept {
        pin: SocketAddr,
        pin_rate: f64,
        best: SocketAddr,
        best_rate: f64,
    },
    Shadow {
        challenger: SocketAddr,
        pin: SocketAddr,
    },
}

impl Announce {
    fn log(&self, label: &str) {
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

#[derive(Default)]
struct PlanState {
    candidates: Vec<SocketAddr>,
    pinned: Option<SocketAddr>,
    chosen_at: Option<Instant>,
    per_addr: HashMap<IpAddr, AddrStats>,
    /// When a reconnect was last pointed at a challenger.
    last_shadow_at: Option<Instant>,
    /// Reconnects handed out since then.
    connects_since_shadow: u32,
    /// The challenger the last verdict found faster than the pin. It takes
    /// the pin only if the next verdict finds the same, so one verdict's
    /// worth of samples from one connection never moves the pin by itself.
    delivery_leader: Option<SocketAddr>,
    /// Verdicts settled on delivery since the last race.
    verdicts_since_race: u32,
    /// Bumped whenever a race finishes, so a caller waiting on one can tell
    /// that it did.
    generation: u64,
    racing: bool,
    /// A race some event asked for, still to run.
    pending: Option<RaceReason>,
    /// Why the last race found nothing, handed to callers that waited on it.
    last_race_error: Option<(io::ErrorKind, String)>,
    /// When a race last found no address while nothing was pinned. Holds
    /// further races off for [`FAILED_RACE_HOLDOFF`].
    last_race_failed_at: Option<Instant>,
    races_won: u64,
    races_failed: u64,
    repins: [u64; RaceReason::ALL.len()],
}

enum Next {
    Dial(Vec<SocketAddr>, Option<Announce>),
    Race(RaceReason),
    Wait(u64),
    Fail(io::ErrorKind, String),
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
            let error = io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "no address resolved for the server",
            );
            ticket.finish(Err(&error), None, Vec::new());
            return Err(error);
        }

        let mut last_error = None;
        let mut slow = Vec::new();
        // One batch at a time bounds the threads a race holds, and the next
        // batch is dialled only once every address of this one has answered
        // without connecting.
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
        let mut state = self.state();
        let pinned = state.pinned;
        let stats = state.per_addr.entry(addr.ip()).or_default();
        match connected_in {
            Some(elapsed) => {
                stats.connect_ewma = Some(ewma(stats.connect_ewma, elapsed));
                stats.consecutive_failures = 0;
            }
            None => {
                stats.consecutive_failures = stats.consecutive_failures.saturating_add(1);
                if pinned == Some(addr)
                    && stats.consecutive_failures >= SUSPECT_AFTER_FAILURES
                    && state.pending != Some(RaceReason::Suspect)
                {
                    state.pending = Some(RaceReason::Suspect);
                    warn!(
                        server = %self.label,
                        address = %addr,
                        failures = SUSPECT_AFTER_FAILURES,
                        "pinned address keeps refusing connections; racing the addresses again"
                    );
                }
            }
        }
    }

    /// Book how long an article fetch took on a connection to `ip`. Ignored
    /// for an address the plan does not know.
    pub(crate) fn record_body_latency(&self, ip: IpAddr, elapsed: Duration) {
        let mut state = self.state();
        if !state.candidates.iter().any(|addr| addr.ip() == ip) {
            return;
        }
        let stats = state.per_addr.entry(ip).or_default();
        stats.body_latency_ewma = Some(ewma(stats.body_latency_ewma, elapsed));
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
        if bytes == 0 || wire.is_zero() {
            return;
        }
        let now = self.now();
        let mut state = self.state();
        if !state.candidates.iter().any(|addr| addr.ip() == ip) {
            return;
        }
        state
            .per_addr
            .entry(ip)
            .or_default()
            .delivery
            .book(bytes, wire, now);
    }

    /// The provider accepts new connections again after refusing them. Its
    /// addresses may have been rebalanced meanwhile, so the next connect races
    /// again, busy or not: this is exactly when a fresh look is worth the
    /// losing handshakes.
    pub(crate) fn note_over_limit_cleared(&self) {
        let mut state = self.state();
        if state.pinned.is_some() && state.pending.is_none() {
            state.pending = Some(RaceReason::OverLimitCleared);
        }
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
                        consecutive_failures: stats.consecutive_failures,
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

impl PlanState {
    fn next(&mut self, now: Instant) -> Next {
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
            (Some(pin), None) if self.pin_is_due(now) => {
                let (reason, verdict) = self.judge_on_delivery(now, pin);
                announce = verdict;
                reason
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

    fn stats(&self, addr: SocketAddr) -> AddrStats {
        self.per_addr.get(&addr.ip()).copied().unwrap_or_default()
    }

    /// The pin's age is due. Settle it on delivery when there is enough, and
    /// say which race to run when there is not — or when enough verdicts
    /// have gone by that the hostname is owed a fresh resolve.
    fn judge_on_delivery(
        &mut self,
        now: Instant,
        pin: SocketAddr,
    ) -> (Option<RaceReason>, Option<Announce>) {
        if self.verdicts_since_race >= VERDICTS_BETWEEN_RACES {
            return (Some(RaceReason::Interval), None);
        }
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
            DeliveryVerdict::Unmeasured => return (Some(RaceReason::Interval), None),
        };
        self.chosen_at = Some(now);
        self.verdicts_since_race += 1;
        self.reset_delivery();
        (None, Some(announce))
    }

    /// Compare the pin with the best measured challenger that is not
    /// currently refusing connections: a pin should never move to an address
    /// the next connect would fail on.
    fn delivery_verdict(&self, now: Instant, pin: SocketAddr) -> DeliveryVerdict {
        let Some(pin_rate) = self.stats(pin).delivery.measured_rate(now) else {
            return DeliveryVerdict::Unmeasured;
        };
        let best = self
            .candidates
            .iter()
            .copied()
            .filter(|addr| *addr != pin)
            .map(|addr| (addr, self.stats(addr)))
            .filter(|(_, stats)| stats.consecutive_failures == 0)
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
    fn reset_delivery(&mut self) {
        for stats in self.per_addr.values_mut() {
            stats.delivery = Delivery::default();
        }
    }

    /// The order one connect dials: the usual order, or a challenger ahead of
    /// it when this reconnect is the one to point at a challenger.
    fn dial_order_for_connect(&mut self, now: Instant) -> (Vec<SocketAddr>, Option<Announce>) {
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
    /// among those not yet measured, never one that last refused and never
    /// one that has yet to connect at all — an address that swallows packets
    /// would hold this reconnect for the whole attempt limit. `None` while
    /// the pin is too young, too recently or too often shadowed, not yet
    /// measured itself, or already judged against every challenger.
    fn shadow_candidate(&self, now: Instant) -> Option<(SocketAddr, SocketAddr)> {
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
                stats.consecutive_failures == 0
                    && stats.connect_ewma.is_some()
                    && stats.delivery.samples < DELIVERY_MIN_SAMPLES
            })
            .min_by_key(|(_, stats)| (stats.delivery.samples, stats.connect_ewma))
            .map(|(addr, _)| (addr, pin))
    }

    fn failed_race_is_recent(&self, now: Instant) -> bool {
        self.last_race_failed_at
            .is_some_and(|at| now.saturating_duration_since(at) < FAILED_RACE_HOLDOFF)
    }

    fn pin_is_due(&self, now: Instant) -> bool {
        self.chosen_at
            .is_none_or(|at| now.saturating_duration_since(at) >= ADDRESS_REPLAN_INTERVAL)
    }

    /// The pin first, then every other candidate: the ones that last
    /// connected before the ones that last failed, faster before slower.
    fn dial_order(&self) -> Vec<SocketAddr> {
        let mut others: Vec<SocketAddr> = self
            .candidates
            .iter()
            .copied()
            .filter(|addr| Some(*addr) != self.pinned)
            .collect();
        others.sort_by_key(|addr| {
            let stats = self.per_addr.get(&addr.ip()).copied().unwrap_or_default();
            (
                stats.consecutive_failures > 0,
                stats.connect_ewma.unwrap_or(Duration::MAX),
            )
        });
        self.pinned.into_iter().chain(others).collect()
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
        state.verdicts_since_race = 0;
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

fn ewma(previous: Option<Duration>, sample: Duration) -> Duration {
    match previous {
        None => sample,
        Some(previous) => previous.mul_f64(1.0 - EWMA_WEIGHT) + sample.mul_f64(EWMA_WEIGHT),
    }
}

#[cfg(test)]
mod tests;
