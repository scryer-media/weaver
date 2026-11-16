// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What one DNS server can do: whether it answers in plain, over TLS and
// over HTTPS, and whether it hands back the records a DNSSEC check needs.
// Each is asked through the stage given by the code a lookup asks with, on
// connections, TLS sessions and a validator of the probe's own, so nothing
// a route's naming holds is used or kept. Nothing here is counted in the
// DNS metrics.
use super::{
    Carrier, DnsServer, Exchanged, Failed, SERVER_TIMEOUT, Stage, Wire, WireKind,
    exchange::{Reply, Session, Wires},
    judge::{self, Asking, Judge, as_sent, validating},
    sessions::DnsSessions,
};
use crate::{
    TunnelError, TunnelStream,
    pipe::{DialError, Dialer, Purpose},
    resolution::record::LookupError,
};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Notify, futures::Notified};
use tokio::time::Instant;
use weaver_dnssec::{Name, Query, RecordType};

// The root's SOA: a question every recursive server answers, and one whose
// check needs the root's DNSKEY set, proven by the built-in root anchors.
const SOA: u16 = 6;

// The longest one probe waits for sockets, in all. A test's probes run on a
// session of their own: at most 10 servers (`MAX_DNS_SERVERS` in the
// server's DNS settings), four probes each, so 39 others. A probe holds a
// socket only while its clock runs, and its clock runs out after
// SERVER_TIMEOUT, so the others hold the share for at most 39 x 5 s =
// 195 s in all. A socket that frees wakes every waiter at once, and any of
// them, or a probe that starts later, may take it, so a probe can be passed
// by every other in the test. A DNS connection a naming keeps between
// lookups never holds a socket a probe waits for: the share counts its
// socket as room, and the probe's claim closes it (`SessionSockets::take`).
// The test itself keeps no connection; each probe's go when it ends. One
// still waiting past that is held up by something other than the test.
pub(super) const SOCKET_WAIT: Duration =
    Duration::from_secs(SERVER_TIMEOUT.as_secs() * (10 * 4 - 1));

// What one probe came to: whether it worked, how long it took, and why not
// when it did not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Probed {
    pub supported: bool,
    pub elapsed: Duration,
    pub error: Option<LookupError>,
}

// One server's probes. `plain`, `tls` and `https` ask the server's address
// on that wire, at the server's own port, TLS name or path when it is
// listed on that wire and at the wire's usual one otherwise (53, 853, and
// 443 at /dns-query). `dnssec` asks on the server's own wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerProbe {
    pub plain: Probed,
    pub tls: Probed,
    pub https: Probed,
    pub dnssec: Probed,
}

// Probes `server` through `dialer`, every probe at once. A dial refused at
// capacity waits for a socket instead of failing. Each probe's time limit
// counts only while it has a stream open or dialling: a probe whose every
// query waits for a socket is not counted, and fails on the route's
// account if the waits pass SOCKET_WAIT in all. A DNSSEC check dials
// several streams at once, so while one of them is open or dialling its
// limit runs even though its other queries wait for a socket.
pub async fn probe_server(dialer: &dyn Dialer, server: &DnsServer) -> ServerProbe {
    probe_on(&Slotted(dialer), &DnsSessions::default(), server).await
}

// The probe's carrier: a dial through the stage that, when it is refused at
// capacity, waits for room, then dials again. The refusal may come of the
// route's socket share or of something past it, such as a tunnel's own
// socket count; either wait is left off the probe's clock while none of
// its streams is open or dialling. A dial under way holds a socket, so it
// is counted as one. It must run under `timed`, which bounds its waits.
pub(super) struct Slotted<'a>(pub(super) &'a dyn Dialer);

#[async_trait::async_trait]
impl Carrier for Slotted<'_> {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn TunnelStream>, TunnelError> {
        debug_assert!(
            CLOCK.try_with(|_| ()).is_ok(),
            "a probe's carrier runs under `timed`"
        );
        // The signals refusals and the share have named, the latest of
        // them past about MOST_SIGNALS.
        let mut known: Vec<Arc<Notify>> = Vec::new();
        loop {
            // Armed before the dial, so a socket freed after the dial is
            // refused and before the wait begins is not missed.
            let mut signals = armed(&known);
            let dialling = Counted::new(true);
            let stage = Stage {
                dialer: self.0,
                failure: Mutex::new(None),
            };
            let said = match stage.open(server).await {
                Ok(stream) => {
                    return Ok(Box::new(Opened {
                        stream,
                        _counted: dialling,
                    }));
                }
                Err(said) => said,
            };
            drop(dialling);
            let mut named = match stage.failure.into_inner().expect("dns stage failure") {
                Some(DialError::AtCapacity(freed)) if !freed.is_empty() => freed,
                _ => return Err(said),
            };
            // The share's own signal too, when it is full: it can differ
            // from the one the refusal names.
            if let Err(more) = self.0.room(&Purpose::Dns) {
                named.extend(more);
            }
            let mut fresh: Vec<Arc<Notify>> = Vec::new();
            for signal in named {
                if !armed_already(&known, &signal) && !armed_already(&fresh, &signal) {
                    fresh.push(signal);
                }
            }
            if fresh.is_empty() || known.len() >= MOST_SIGNALS {
                // Everything the refusal names was armed before the dial,
                // or enough signals are known that a new one is not worth a
                // dial at once: wait for those armed, and for the new ones,
                // off the clock. A new one takes the place of the oldest
                // known, so a free of only what the refusal named is seen.
                let mut late = armed(&fresh);
                {
                    let _waiting = Counted::new(false);
                    tokio::select! {
                        () = any(&mut signals) => {}
                        () = any(&mut late) => {}
                    }
                }
                drop((late, signals));
                for signal in fresh {
                    if known.len() >= MOST_SIGNALS {
                        known.remove(0);
                    }
                    known.push(signal);
                }
                continue;
            }
            // The refusal names a signal not armed before the dial, so a
            // free before now may have gone unseen. Arm it and dial again
            // at once: that dial is the check.
            drop(signals);
            known.extend(fresh);
        }
    }
}

// The signals a probe's dial learns before a new one no longer earns a dial
// at once. A dial is refused by the route's share or by something past it
// (a tunnel's own socket count, a session limit); a refusal from a ladder
// of stages can name one of each. A dialer that named a new signal on every
// refusal would otherwise be dialled again without end. Past this many, a
// new signal is waited on with those known, and takes the oldest's place.
const MOST_SIGNALS: usize = 4;

// Whether `signal` is one of `known`.
fn armed_already(known: &[Arc<Notify>], signal: &Arc<Notify>) -> bool {
    known.iter().any(|old| Arc::ptr_eq(old, signal))
}

type Signal<'a> = Pin<Box<Notified<'a>>>;

// A wait on each of `notifies`, armed.
fn armed(notifies: &[Arc<Notify>]) -> Vec<Signal<'_>> {
    let mut signals: Vec<_> = notifies
        .iter()
        .map(|notify| Box::pin(notify.notified()))
        .collect();
    for signal in &mut signals {
        signal.as_mut().enable();
    }
    signals
}

// Until any of the signals fires.
async fn any(signals: &mut [Signal<'_>]) {
    std::future::poll_fn(|cx| {
        if signals
            .iter_mut()
            .any(|signal| signal.as_mut().poll(cx).is_ready())
        {
            return Poll::Ready(());
        }
        Poll::Pending
    })
    .await
}

tokio::task_local! {
    // The clock of the probe being run.
    static CLOCK: Arc<Clock>;
}

#[cfg(test)]
thread_local! {
    // How many probes run on this thread have their clocks stopped now,
    // each waiting for a socket with no stream open or dialling: what a
    // test waits on to know its probes are waiting.
    pub(super) static STOPPED: tokio::sync::watch::Sender<usize> =
        tokio::sync::watch::channel(0).0;
}

// One probe's clock. It stops while the probe waits for a socket and has no
// stream open or dialling, whether the share or something past it refused
// the dial. A probe with a stream open or dialling is being answered, or
// not, by the server, so its clock runs even while another of its queries
// waits, and a server that never replies still runs the probe out of time
// and frees the socket. A stop does not reset the clock: it resumes with
// what is left. `changed` fires each time the clock stops or starts.
#[derive(Default)]
struct Clock {
    stops: Mutex<Stops>,
    changed: Notify,
}

#[derive(Default)]
struct Stops {
    // Time the clock stood stopped, in stops that have ended.
    stopped: Duration,
    // Waits under way, sockets open, and since when the clock stands
    // stopped.
    waiting: usize,
    open: usize,
    since: Option<Instant>,
}

impl Stops {
    // Stops or starts the clock as the waits and sockets now say, and says
    // whether it did either.
    fn settle(&mut self) -> bool {
        let stop = self.waiting > 0 && self.open == 0;
        match self.since {
            None if stop => self.since = Some(Instant::now()),
            Some(since) if !stop => {
                self.stopped += Instant::now() - since;
                self.since = None;
            }
            _ => return false,
        }
        true
    }
}

// Where a probe's clock stands.
struct Reading {
    stopped: bool,
    // Time run, and time stood stopped, since the probe began.
    spent: Duration,
    waited: Duration,
}

impl Clock {
    fn read(&self, started: Instant) -> Reading {
        let now = Instant::now();
        let stops = self.stops.lock().expect("probe clock");
        let standing = stops.since.map_or(Duration::ZERO, |since| now - since);
        let waited = stops.stopped + standing;
        Reading {
            stopped: stops.since.is_some(),
            spent: (now - started).saturating_sub(waited),
            waited,
        }
    }
    // Counts a wait or a socket in or out, by `change`.
    fn count(&self, open: bool, change: impl FnOnce(&mut usize)) {
        let mut stops = self.stops.lock().expect("probe clock");
        change(if open {
            &mut stops.open
        } else {
            &mut stops.waiting
        });
        let changed = stops.settle();
        #[cfg(test)]
        if changed {
            let stopped = stops.since.is_some();
            STOPPED.with(|count| {
                count.send_modify(|count| match stopped {
                    true => *count += 1,
                    false => *count -= 1,
                })
            });
        }
        drop(stops);
        if changed {
            self.changed.notify_waiters();
        }
    }
}

// A wait for a socket, or a socket open, counted on the clock of the probe
// it is made for while it lasts.
struct Counted {
    clock: Option<Arc<Clock>>,
    open: bool,
}

impl Counted {
    fn new(open: bool) -> Self {
        let clock = CLOCK.try_with(Arc::clone).ok();
        if let Some(clock) = &clock {
            clock.count(open, |count| *count += 1);
        }
        Self { clock, open }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        if let Some(clock) = &self.clock {
            clock.count(self.open, |count| *count -= 1);
        }
    }
}

// A stream the probe opened, counted on its clock while it is open.
struct Opened {
    stream: Box<dyn TunnelStream>,
    _counted: Counted,
}

impl tokio::io::AsyncRead for Opened {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for Opened {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

// The server's address on `kind`, as the probe of that wire asks it.
fn on(server: &DnsServer, kind: WireKind) -> DnsServer {
    if server.wire.kind() == kind {
        return server.clone();
    }
    let wire = match kind {
        WireKind::Plain => Wire::Plain,
        WireKind::Tls => Wire::Tls { name: None },
        WireKind::Https => Wire::Https {
            path: "/dns-query".into(),
        },
    };
    DnsServer {
        address: std::net::SocketAddr::new(server.address.ip(), kind.default_port()),
        wire,
    }
}

pub(super) async fn probe_on(
    carrier: &dyn Carrier,
    sessions: &DnsSessions,
    server: &DnsServer,
) -> ServerProbe {
    sessions.ready().await;
    let wires = sessions.wires();
    let (plain, tls, https) = (
        on(server, WireKind::Plain),
        on(server, WireKind::Tls),
        on(server, WireKind::Https),
    );
    let (plain, tls, https, dnssec) = tokio::join!(
        timed(&plain, answers(carrier, wires, &plain)),
        timed(&tls, answers(carrier, wires, &tls)),
        timed(&https, answers(carrier, wires, &https)),
        timed(server, signed(carrier, wires, sessions.dnssec(), server)),
    );
    ServerProbe {
        plain,
        tls,
        https,
        dnssec,
    }
}

// Runs one probe within the time limit a lookup gives one server, and says
// why it failed in the words a lookup would use. Time spent waiting for a
// socket with no stream open or dialling is not counted in that limit or
// reported: it is this machine's, not the server's. Those waits have a
// limit of their own, SOCKET_WAIT, summed over every wait the probe makes,
// and a probe that reaches it fails on the route's account, not the
// server's.
pub(super) async fn timed<E>(
    server: &DnsServer,
    probe: impl std::future::Future<Output = Result<(), E>>,
) -> Probed
where
    Failed: From<E>,
{
    let clock = Arc::new(Clock::default());
    let started = Instant::now();
    let probe = CLOCK.scope(Arc::clone(&clock), probe);
    tokio::pin!(probe);
    let answered = loop {
        // Armed before the clock is read, so a stop or start in between
        // is not missed.
        let changed = clock.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let now = clock.read(started);
        let left = match now.stopped {
            true => SOCKET_WAIT.saturating_sub(now.waited),
            false => SERVER_TIMEOUT.saturating_sub(now.spent),
        };
        if left.is_zero() {
            break Err(now.stopped);
        }
        tokio::select! {
            biased;
            answered = &mut probe => break Ok(answered),
            () = &mut changed => {}
            () = tokio::time::sleep(left) => {}
        }
    };
    let elapsed = clock.read(started).spent;
    let failed = match answered {
        Ok(Ok(())) => {
            return Probed {
                supported: true,
                elapsed,
                error: None,
            };
        }
        Ok(Err(failed)) => Failed::from(failed),
        Err(false) => Failed::TimedOut,
        Err(true) => Failed::NoSocket,
    };
    Probed {
        supported: false,
        elapsed,
        error: Some(failed.error_at(Some(server))),
    }
}

// Whether `server` answers the root's SOA on its wire, asked and read as a
// lookup taking answers as sent asks and reads its queries: through the
// lookup's own loop, exchanges and reader, so a server that would fail a
// lookup fails this probe the same way. Any reply that reads, whatever its
// records, is an answer. The talk's connections are the probe's own, and
// go when it ends.
async fn answers(carrier: &dyn Carrier, wires: &Wires, server: &DnsServer) -> Result<(), Failed> {
    let mut session = Session::new(carrier, wires, server);
    let mut asked = RootSoa::default();
    judge::run(&mut session, &mut false, &mut asked).await;
    match asked.answered {
        Some(answered) => answered.map_err(Failed::from),
        // A talk that ended with the query neither answered nor failed: a
        // lookup counts a family left so as timed out (`ServerSaid::take`).
        None => Err(Failed::TimedOut),
    }
}

// The probe's one question, the root's SOA, as a judge: the query goes out
// once, and its reply is read by the reader a lookup taking answers as sent
// uses.
#[derive(Default)]
struct RootSoa {
    // The id the query went out with, once it has.
    id: Option<u16>,
    answered: Option<Result<(), Exchanged>>,
}

impl RootSoa {
    fn query() -> Query {
        Query::new(Name::root(), RecordType::new(SOA))
    }
}

impl Judge for RootSoa {
    async fn want(&mut self, asking: &mut Asking<'_, '_, '_>) {
        if self.id.is_none() {
            let id = asking.id();
            self.id = Some(id);
            asking.send(0, 0, as_sent::packet(id, &Name::root(), SOA));
        }
    }

    fn supply(&mut self, _: usize, reply: Reply) {
        let id = self.id.expect("a reply comes only to a query sent");
        self.answered =
            Some(reply.and_then(|bytes| as_sent::read(&Self::query(), id, &bytes).map(drop)));
    }

    fn done(&self) -> bool {
        self.answered.is_some()
    }

    fn fail(&mut self, failed: Exchanged) {
        self.answered.get_or_insert(Err(failed));
    }
}

// Whether `server` gives the root's SOA with the signatures and keys that
// prove it from the root anchors: the root's DNSKEY set asked with DO set,
// and checked as a route's lookup under `Validate` checks it.
pub(super) async fn signed(
    carrier: &dyn Carrier,
    wires: &Wires,
    dnssec: &validating::Dnssec,
    server: &DnsServer,
) -> Result<(), Exchanged> {
    let mut session = Session::new(carrier, wires, server);
    let found = validating::validated(&mut session, dnssec, "", SOA, &mut false).await?;
    match found.insecure {
        None => Ok(()),
        // The root anchors prove the root signed, so this only comes of a
        // server whose answer leaves the proof out.
        Some(_) => Err(Exchanged::Stripped(Vec::new())),
    }
}
