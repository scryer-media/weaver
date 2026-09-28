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
//! has aged past [`ADDRESS_REPLAN_INTERVAL`]. Only a failing pin re-races while
//! the server is busy; the other reasons wait until it is idle, because
//! moving busy connections to another address costs more than any address
//! could save.
//!
//! A connect that finds the pin refusing tries the remaining candidates in
//! order of their measured connect time, so one bad address never fails a
//! connect the server could have served.

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

/// How old a pin may get before an idle server races its addresses again.
pub const ADDRESS_REPLAN_INTERVAL: Duration = Duration::from_mins(10);

/// Longest one connect attempt to one address may take, whatever the connect
/// timeout, so an address that swallows packets neither holds a race thread
/// for long nor delays the fall-over to the next candidate.
pub const ADDRESS_ATTEMPT_LIMIT: Duration = Duration::from_secs(15);

/// Consecutive connect failures on the pin that make it suspect. A suspect pin
/// is raced again on the next connect, even while the server is busy.
pub const SUSPECT_AFTER_FAILURES: u32 = 2;

/// Most addresses one race dials. Each gets its own thread for the length of
/// one connect.
const MAX_RACE_CANDIDATES: usize = 16;

/// Weight of a new sample in the per-address averages.
const EWMA_WEIGHT: f64 = 0.25;

/// Why a race ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RaceReason {
    /// Nothing was pinned yet.
    Initial,
    /// The pin aged past [`ADDRESS_REPLAN_INTERVAL`] and the server was idle.
    Interval,
    /// An over-limit holdoff cleared, so the provider's view of this client
    /// may have changed.
    OverLimitCleared,
    /// The pin failed [`SUSPECT_AFTER_FAILURES`] connects in a row.
    Suspect,
}

impl RaceReason {
    pub const ALL: [RaceReason; 4] = [
        RaceReason::Initial,
        RaceReason::Interval,
        RaceReason::OverLimitCleared,
        RaceReason::Suspect,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            RaceReason::Initial => "initial",
            RaceReason::Interval => "interval",
            RaceReason::OverLimitCleared => "over_limit_cleared",
            RaceReason::Suspect => "suspect",
        }
    }

    fn index(self) -> usize {
        match self {
            RaceReason::Initial => 0,
            RaceReason::Interval => 1,
            RaceReason::OverLimitCleared => 2,
            RaceReason::Suspect => 3,
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
    /// Pin changes, by the reason of the race that made them.
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

/// A server's address plan plus what the caller knows about the server's
/// load, which the plan needs to decide whether a due race may run now.
#[derive(Clone)]
pub struct AddressRoute {
    pub(crate) plan: Arc<AddressPlan>,
    pub(crate) server_idle: bool,
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
        self.plan.connect(&dialer, self.server_idle)
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct AddrStats {
    connect_ewma: Option<Duration>,
    consecutive_failures: u32,
    body_latency_ewma: Option<Duration>,
}

#[derive(Default)]
struct PlanState {
    candidates: Vec<SocketAddr>,
    pinned: Option<SocketAddr>,
    chosen_at: Option<Instant>,
    per_addr: HashMap<IpAddr, AddrStats>,
    /// Bumped whenever a race finishes, so a caller waiting on one can tell
    /// that it did.
    generation: u64,
    racing: bool,
    /// A race some event asked for, still to run.
    pending: Option<RaceReason>,
    /// Why the last race found nothing, handed to callers that waited on it.
    last_race_error: Option<(io::ErrorKind, String)>,
    races_won: u64,
    races_failed: u64,
    repins: [u64; RaceReason::ALL.len()],
}

enum Next {
    Dial(Vec<SocketAddr>),
    Race(RaceReason),
    Wait(u64),
}

/// The address state of one server. See the module docs.
pub struct AddressPlan {
    label: String,
    state: Mutex<PlanState>,
    race_finished: Condvar,
}

impl AddressPlan {
    pub(crate) fn new(label: String) -> Self {
        Self {
            label,
            state: Mutex::new(PlanState::default()),
            race_finished: Condvar::new(),
        }
    }

    /// Plain data behind the lock, so a panic elsewhere must not stop every
    /// later connect.
    fn state(&self) -> MutexGuard<'_, PlanState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Open a stream through `dialer` to the address this plan picks, racing
    /// the candidates first when a race is due.
    pub(crate) fn connect<D: AddressDialer>(
        self: &Arc<Self>,
        dialer: &Arc<D>,
        server_idle: bool,
    ) -> io::Result<(D::Stream, SocketAddr)> {
        loop {
            let next = self.state().next(Instant::now(), server_idle);
            match next {
                Next::Dial(order) => return self.dial_in_order(dialer.as_ref(), &order),
                Next::Race(reason) => return self.race(dialer, reason),
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
            Ok(addrs) => distinct_capped(addrs),
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

        let (tx, rx) = mpsc::channel();
        let mut last_error = None;
        for &addr in &candidates {
            let tx = tx.clone();
            let dialer = Arc::clone(dialer);
            let plan = Arc::clone(self);
            let spawned = std::thread::Builder::new()
                .name("nntp-address-race".into())
                .spawn(move || {
                    let started = Instant::now();
                    let result = dialer.dial(addr);
                    let elapsed = started.elapsed();
                    plan.record_connect(addr, result.is_ok().then_some(elapsed));
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
                    ticket.finish(Ok(addr), Some(elapsed), resolved);
                    return Ok((stream, addr));
                }
                Err(error) => last_error = Some(error),
            }
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

    /// The provider accepts new connections again after refusing them. Its
    /// addresses may have been rebalanced meanwhile, so race again once the
    /// server is idle.
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
}

impl PlanState {
    fn next(&mut self, now: Instant, server_idle: bool) -> Next {
        if self.racing {
            return Next::Wait(self.generation);
        }
        let reason = match (self.pinned, self.pending) {
            (None, _) => Some(RaceReason::Initial),
            (Some(_), Some(RaceReason::Suspect)) => Some(RaceReason::Suspect),
            (Some(_), Some(pending)) if server_idle => Some(pending),
            (Some(_), _) if server_idle && self.pin_is_due(now) => Some(RaceReason::Interval),
            _ => None,
        };
        match reason {
            Some(reason) => {
                self.racing = true;
                Next::Race(reason)
            }
            None => Next::Dial(self.dial_order()),
        }
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
        let previous = state.pinned;
        match outcome {
            Ok(winner) => {
                state.pinned = Some(winner);
                state.chosen_at = Some(Instant::now());
                state.races_won += 1;
                state.last_race_error = None;
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
                    state.chosen_at = Some(Instant::now());
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

fn distinct_capped(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let mut distinct = Vec::with_capacity(addrs.len().min(MAX_RACE_CANDIDATES));
    for addr in addrs {
        if distinct.len() == MAX_RACE_CANDIDATES {
            break;
        }
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
