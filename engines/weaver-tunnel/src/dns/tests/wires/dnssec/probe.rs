// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A server's probes, against fixture servers that each speak one wire.
use super::*;
use crate::dns::Failed;
use crate::dns::probe::{SOCKET_WAIT, Slotted, probe_on, signed, timed};
use std::time::Duration;

// Fixture servers for one address, one per port: each probe reaches the
// one listening on the port it dials, and a port nobody listens on fails
// the dial.
struct ByPort(Vec<(u16, Servers)>);

#[async_trait::async_trait]
impl Carrier for ByPort {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        match self
            .0
            .iter()
            .find(|(port, _)| *port == server.address.port())
        {
            Some((_, servers)) => servers.open(server).await,
            None => Err(TunnelError::Engine(format!(
                "nothing listens at {}",
                server.address
            ))),
        }
    }
}

impl ByPort {
    fn dials(&self) -> Vec<SocketAddr> {
        self.0
            .iter()
            .flat_map(|(_, servers)| servers.dials())
            .collect()
    }
}

// Sessions of a probe's own that trust `authority` and the fixture's root
// keys.
fn probing(authority: &Authority, resolver: &FakeResolver) -> DnsSessions {
    let sessions = DnsSessions::default();
    sessions.trust(&authority.wires());
    sessions
        .dnssec()
        .anchored(resolver.zones()[0].anchors().unwrap(), NOW);
    sessions
}

// Plain on 53, TLS on `tls_port` with a certificate for `tls_names`, and
// HTTPS over HTTP/2 on 443, all at 192.0.2.53 and answering as `serves`.
fn every_port(
    authority: &Authority,
    resolver: &Arc<FakeResolver>,
    serves: &'static [(&'static str, Serves)],
    tls_port: u16,
    tls_names: &[&str],
) -> ByPort {
    let answer = serving(resolver, serves);
    let plain =
        Servers::new(Speaks::Dot { close_after: None }, answer.clone()).plain_at("192.0.2.53");
    let tls = Servers::new(Speaks::Dot { close_after: None }, answer.clone())
        .at("192.0.2.53", authority.server(tls_names, b"dot"));
    let https = Servers::new(
        Speaks::H2 {
            respond: Arc::new(h2_message),
            goaway_after: None,
            misstep: H2Misstep::None,
        },
        answer,
    )
    .at("192.0.2.53", authority.server(&["192.0.2.53"], b"h2"));
    ByPort(vec![(53, plain), (tls_port, tls), (443, https)])
}

#[tokio::test(start_paused = true)]
async fn a_server_on_every_wire_is_probed_on_each_and_checks_dnssec() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let carrier = every_port(&authority, &resolver, &[], 853, &["192.0.2.53"]);
    let sessions = probing(&authority, &resolver);
    let before = counted();
    let sent = crate::metrics::dnssec_queries();
    let probe = probe_on(&carrier, &sessions, &first()).await;
    for (wire, probed) in [
        ("plain", &probe.plain),
        ("tls", &probe.tls),
        ("https", &probe.https),
        ("dnssec", &probe.dnssec),
    ] {
        assert!(probed.supported, "{wire}: {probed:?}");
        assert_eq!(probed.error, None, "{wire}");
    }
    // Each wire was asked at its own port.
    let dials = carrier.dials();
    for port in [53, 853, 443] {
        assert!(
            dials.iter().any(|dial| dial.port() == port),
            "{port}: {dials:?}"
        );
    }
    // Nothing the probe asked is counted as a lookup's.
    assert_eq!(crate::metrics::dnssec_queries(), sent);
    assert!(since(&before).is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_listed_wire_is_probed_at_its_own_port_and_name() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    // The certificate names only dns.example, so a probe that checked it
    // against the address would fail.
    let carrier = every_port(&authority, &resolver, &[], 8853, &["dns.example"]);
    let sessions = probing(&authority, &resolver);
    let listed = DnsServer {
        address: "192.0.2.53:8853".parse().unwrap(),
        wire: Wire::Tls {
            name: Some("dns.example".into()),
        },
    };
    let probe = probe_on(&carrier, &sessions, &listed).await;
    assert!(probe.tls.supported, "{:?}", probe.tls);
    assert!(probe.plain.supported, "{:?}", probe.plain);
    assert!(probe.https.supported, "{:?}", probe.https);
    // The DNSSEC check rides the listed wire.
    assert!(probe.dnssec.supported, "{:?}", probe.dnssec);
    assert!(!carrier.dials().iter().any(|dial| dial.port() == 853));
}

#[tokio::test(start_paused = true)]
async fn a_plain_server_that_strips_signatures_fails_the_other_probes_in_a_lookups_words() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let answer = serving(&resolver, &[("192.0.2.53", Serves::Stripped)]);
    let carrier = ByPort(vec![(
        53,
        Servers::new(Speaks::Dot { close_after: None }, answer).plain_at("192.0.2.53"),
    )]);
    let sessions = probing(&authority, &resolver);
    let probe = probe_on(&carrier, &sessions, &first()).await;
    assert!(probe.plain.supported, "{:?}", probe.plain);
    for (wire, probed, port) in [("tls", &probe.tls, 853), ("https", &probe.https, 443)] {
        assert!(!probed.supported, "{wire}");
        assert_eq!(
            probed.error,
            Some(failure(
                LookupFailure::NoReply,
                &format!("nothing listens at 192.0.2.53:{port}")
            )),
            "{wire}"
        );
    }
    assert!(!probe.dnssec.supported);
    assert_eq!(
        probe.dnssec.error.as_ref().map(|error| error.failure),
        Some(LookupFailure::DnssecUnavailable)
    );
}

#[tokio::test(start_paused = true)]
async fn a_server_that_never_replies_fails_each_probe_as_timed_out() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let carrier = ByPort(vec![(
        53,
        Servers::new(Speaks::Dot { close_after: None }, serving(&resolver, &[]))
            .silent_at("192.0.2.53"),
    )]);
    let sessions = probing(&authority, &resolver);
    let probe = probe_on(&carrier, &sessions, &first()).await;
    for probed in [&probe.plain, &probe.dnssec] {
        assert!(!probed.supported);
        assert_eq!(
            probed.error.as_ref().map(|error| error.failure),
            Some(LookupFailure::TimedOut)
        );
        assert_eq!(probed.elapsed, SERVER_TIMEOUT);
    }
}

// A stage whose tunnel has `core` DNS sockets, carrying each dial to
// `carrier`. A dial past the share fails at capacity, as a full tunnel
// share fails it, and every stream holds its socket until it closes. A
// dial holds its socket from the start, as a tunnel's does.
struct Shared {
    carrier: Arc<dyn Carrier>,
    sockets: crate::pipe::SessionSockets,
    at_capacity: AtomicUsize,
    // Fires at each dial refused.
    refused: tokio::sync::Notify,
}

impl Shared {
    fn new(carrier: impl Carrier + 'static, core: usize) -> Arc<Self> {
        Arc::new(Self {
            carrier: Arc::new(carrier),
            sockets: crate::pipe::SessionSockets::new(crate::pipe::SocketShares {
                nntp: 0,
                torrent: 0,
                core,
                http: 0,
            }),
            at_capacity: AtomicUsize::new(0),
            refused: tokio::sync::Notify::new(),
        })
    }
    // The share's one socket class for DNS.
    fn take(&self) -> crate::pipe::SocketPermit {
        self.sockets
            .take(crate::pipe::SocketClass::ReserveCore)
            .unwrap()
            .unwrap()
    }
    // Until `count` dials in all were refused.
    async fn refusals(&self, count: usize) {
        loop {
            let refused = self.refused.notified();
            tokio::pin!(refused);
            refused.as_mut().enable();
            if self.at_capacity.load(Ordering::Relaxed) >= count {
                return;
            }
            refused.await;
        }
    }
}

// A stream that holds its tunnel socket.
struct Holding {
    stream: Box<dyn crate::TunnelStream>,
    _socket: Option<crate::pipe::SocketPermit>,
}

impl AsyncRead for Holding {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Holding {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[async_trait::async_trait]
impl Dialer for Shared {
    fn room(&self, purpose: &crate::pipe::Purpose) -> Result<(), Vec<Arc<tokio::sync::Notify>>> {
        self.sockets
            .room(crate::pipe::SocketClass::of(purpose))
            .map_err(|freed| vec![freed])
    }
    async fn dial(&self, target: &Target) -> Result<crate::pipe::Dialed, DialError> {
        let socket = self
            .sockets
            .take(crate::pipe::SocketClass::of(&target.purpose))
            .map_err(|freed| {
                self.at_capacity.fetch_add(1, Ordering::Relaxed);
                self.refused.notify_waiters();
                DialError::AtCapacity(vec![freed])
            })?;
        let server = DnsServer {
            address: SocketAddr::new(target.addresses[0], target.port),
            wire: Wire::Plain,
        };
        let stream = self
            .carrier
            .open(&server)
            .await
            .map_err(|source| DialError::Hop { proxy: 7, source })?;
        Ok(dialed(Box::new(Holding {
            stream,
            _socket: socket,
        })))
    }
    fn budget(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "shared".into()
    }
}

fn dialed(stream: Box<dyn crate::TunnelStream>) -> crate::pipe::Dialed {
    crate::pipe::Dialed {
        stream: crate::pipe::DialedStream::Tunnel(stream),
        outcome: Default::default(),
        path: crate::pipe::DialPath::default(),
        peer: None,
        source: None,
        setup: None,
    }
}

// A carrier whose dials to port 53 never complete, and which has nothing
// on any other port.
struct Hanging;

#[async_trait::async_trait]
impl Carrier for Hanging {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        match server.address.port() {
            53 => std::future::pending().await,
            _ => Err(TunnelError::Engine("nothing listens".into())),
        }
    }
}

// A stage whose share always has room, but whose every dial is refused at
// capacity by something past the share, as a tunnel's own socket count
// refuses it while closing sockets are reclaimed.
#[derive(Default)]
struct Refusing {
    dials: AtomicUsize,
    freed: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl Dialer for Refusing {
    async fn dial(&self, _target: &Target) -> Result<crate::pipe::Dialed, DialError> {
        self.dials.fetch_add(1, Ordering::Relaxed);
        Err(DialError::AtCapacity(vec![self.freed.clone()]))
    }
    fn budget(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "refusing".into()
    }
}

// A stage past a share with room that refuses the first two dials at
// capacity, and frees a socket the moment it refuses the second: after the
// dial is refused and before its caller can begin to wait. The third dial
// opens.
#[derive(Default)]
struct FreesOnRefusal {
    dials: AtomicUsize,
    freed: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl Dialer for FreesOnRefusal {
    async fn dial(&self, _target: &Target) -> Result<crate::pipe::Dialed, DialError> {
        match self.dials.fetch_add(1, Ordering::Relaxed) {
            0 => Err(DialError::AtCapacity(vec![self.freed.clone()])),
            1 => {
                self.freed.notify_waiters();
                Err(DialError::AtCapacity(vec![self.freed.clone()]))
            }
            _ => Ok(dialed(Box::new(tokio::io::duplex(64).0))),
        }
    }
    fn budget(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "frees on refusal".into()
    }
}

// A stage past a share with room that refuses every dial at capacity with
// a new signal each time, and never fires one. It panics past 100 dials, so
// a probe that redials without end fails instead of hanging the run.
#[derive(Default)]
struct FreshSignals {
    dials: AtomicUsize,
}

#[async_trait::async_trait]
impl Dialer for FreshSignals {
    async fn dial(&self, _target: &Target) -> Result<crate::pipe::Dialed, DialError> {
        let dials = self.dials.fetch_add(1, Ordering::Relaxed) + 1;
        assert!(dials <= 100, "dialled {dials} times");
        Err(DialError::AtCapacity(vec![Arc::new(
            tokio::sync::Notify::new(),
        )]))
    }
    fn budget(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "fresh signals".into()
    }
}

// A stage past a share with room that refuses the first five dials at
// capacity, each with a new signal it keeps, and opens the sixth.
#[derive(Default)]
struct NamesFive {
    named: Mutex<Vec<Arc<tokio::sync::Notify>>>,
}

#[async_trait::async_trait]
impl Dialer for NamesFive {
    async fn dial(&self, _target: &Target) -> Result<crate::pipe::Dialed, DialError> {
        let mut named = self.named.lock().unwrap();
        if named.len() == 5 {
            return Ok(dialed(Box::new(tokio::io::duplex(64).0)));
        }
        named.push(Arc::new(tokio::sync::Notify::new()));
        Err(DialError::AtCapacity(vec![named[named.len() - 1].clone()]))
    }
    fn budget(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "names five".into()
    }
}

// A plain server at 192.0.2.53 that opens at once, or after `dial` when
// set, and answers nothing it is asked.
fn plain_server(dial: Option<Duration>) -> Servers {
    let servers = Servers::new(
        Speaks::Dot { close_after: None },
        |_: std::net::IpAddr, _: u16, _: &[u8]| Vec::new(),
    )
    .plain_at("192.0.2.53");
    match dial {
        Some(dial) => servers.delayed(dial),
        None => servers,
    }
}

// Until `count` probes on this thread wait for a socket, their clocks
// stopped.
async fn waiting(count: usize) {
    let mut stopped = crate::dns::probe::STOPPED.with(|stopped| stopped.subscribe());
    stopped
        .wait_for(|stopped| *stopped >= count)
        .await
        .expect("the probes' count lasts the test");
}

// A probe that only opens one stream through `slotted`.
async fn opens(slotted: &Slotted<'_>, server: &DnsServer) -> Result<(), Failed> {
    slotted
        .open(server)
        .await
        .map(drop)
        .map_err(Failed::Unreached)
}

// Every probe of `probe` supported.
fn all_supported(probe: &crate::dns::ServerProbe) {
    for (wire, probed) in [
        ("plain", &probe.plain),
        ("tls", &probe.tls),
        ("https", &probe.https),
        ("dnssec", &probe.dnssec),
    ] {
        assert!(probed.supported, "{wire}: {probed:?}");
    }
}

// Ten servers probed at once, four probes each, through a tunnel with room
// for two DNS sockets: every probe waits for a socket and is really asked,
// and none reports the tunnel's limit as the server's.
#[tokio::test(start_paused = true)]
async fn probes_past_the_tunnels_socket_share_wait_for_a_socket_and_each_is_asked() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let carrier = every_port(&authority, &resolver, &[], 853, &["192.0.2.53"]);
    let sessions = probing(&authority, &resolver);
    let shared = Shared::new(carrier, 2);
    let slotted = crate::dns::probe::Slotted(&*shared);
    let server = first();
    let mut running: Vec<_> = (0..10)
        .map(|_| Some(Box::pin(probe_on(&slotted, &sessions, &server))))
        .collect();
    let mut probes: Vec<Option<_>> = (0..10).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        for (probe, done) in running.iter_mut().zip(probes.iter_mut()) {
            if let Some(future) = probe
                && let Poll::Ready(probed) = future.as_mut().poll(cx)
            {
                *done = Some(probed);
                *probe = None;
            }
        }
        if probes.iter().all(Option::is_some) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    for probe in probes.iter().flatten() {
        all_supported(probe);
    }
    // The share was full while the probes ran, so some of them waited.
    assert!(shared.at_capacity.load(Ordering::Relaxed) > 0);
    assert_eq!(
        shared.sockets.held(crate::pipe::SocketClass::ReserveCore),
        0
    );
}

// A probe's time limit runs only while it is being asked. The test holds
// the one socket for twice the limit while every probe waits for it: each
// is then asked, with its full limit, and is answered.
#[tokio::test(start_paused = true)]
async fn a_wait_for_a_socket_is_not_counted_against_the_probes_time_limit() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let carrier = every_port(&authority, &resolver, &[], 853, &["192.0.2.53"]);
    let sessions = probing(&authority, &resolver);
    let shared = Shared::new(carrier, 1);
    let held = shared.take();
    let slotted = crate::dns::probe::Slotted(&*shared);
    let server = first();
    let (probe, ()) = tokio::join!(probe_on(&slotted, &sessions, &server), async {
        // Each of the four probes found the share full and waits.
        waiting(4).await;
        tokio::time::advance(SERVER_TIMEOUT * 2).await;
        drop(held);
    });
    all_supported(&probe);
}

// A DNSSEC check sends the queries a step needs at once. Run alone with
// one socket, its own queries take turns on it, and the check passes.
#[tokio::test(start_paused = true)]
async fn a_check_whose_queries_go_out_at_once_takes_turns_on_one_socket() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let carrier = every_port(&authority, &resolver, &[], 853, &["192.0.2.53"]);
    let sessions = probing(&authority, &resolver);
    let shared = Shared::new(carrier, 1);
    let slotted = Slotted(&*shared);
    let server = first();
    let probed = timed(
        &server,
        signed(&slotted, sessions.wires(), sessions.dnssec(), &server),
    )
    .await;
    assert!(probed.supported, "{probed:?}");
    // Nothing else ran, so every refusal was of the check's own queries.
    assert!(shared.at_capacity.load(Ordering::Relaxed) > 0);
}

// A dial refused at capacity by something past a share that has room is
// dialled once more, then waits for that refusal's own signal off the
// probe's clock: no spin, the server is never charged with the wait, and
// the probe ends on the route's account at the socket wait limit.
#[tokio::test(start_paused = true)]
async fn a_refusal_from_past_the_share_waits_off_the_clock_and_fails_on_the_routes_account() {
    let refusing = Refusing::default();
    let slotted = Slotted(&refusing);
    let server = first();
    let started = tokio::time::Instant::now();
    let probed = timed(&server, opens(&slotted, &server)).await;
    assert_eq!(refusing.dials.load(Ordering::Relaxed), 2);
    assert_eq!(
        probed.error.map(|error| error.failure),
        Some(LookupFailure::Other)
    );
    assert_eq!(probed.elapsed, Duration::ZERO);
    assert_eq!(started.elapsed(), SOCKET_WAIT);
}

// A dialer that names a new signal on every refusal is dialled at once only
// until four signals are known; then the probe waits on those, off the
// clock, and ends on the route's account at the socket wait limit.
#[tokio::test(start_paused = true)]
async fn a_dialer_naming_a_new_signal_each_refusal_is_dialled_a_bounded_number_of_times() {
    let fresh = FreshSignals::default();
    let slotted = Slotted(&fresh);
    let server = first();
    let started = tokio::time::Instant::now();
    let probed = timed(&server, opens(&slotted, &server)).await;
    // Four dials each learn a signal; the fifth's refusal is waited on.
    assert_eq!(fresh.dials.load(Ordering::Relaxed), 5);
    assert_eq!(
        probed.error.map(|error| error.failure),
        Some(LookupFailure::Other)
    );
    assert_eq!(probed.elapsed, Duration::ZERO);
    assert_eq!(started.elapsed(), SOCKET_WAIT);
}

// Once four signals are known, a refusal that names a fifth is waited on
// with them: a free of only the fifth wakes the probe, whose next dial
// opens, with no time passed.
#[tokio::test(start_paused = true)]
async fn a_free_of_the_newest_signal_past_the_bound_wakes_the_probe() {
    let names = NamesFive::default();
    let slotted = Slotted(&names);
    let server = first();
    let started = tokio::time::Instant::now();
    let (probed, ()) = tokio::join!(timed(&server, opens(&slotted, &server)), async {
        waiting(1).await;
        let named = names.named.lock().unwrap().clone();
        assert_eq!(named.len(), 5);
        named[4].notify_waiters();
    });
    assert!(probed.supported, "{probed:?}");
    assert_eq!(names.named.lock().unwrap().len(), 5);
    assert_eq!(started.elapsed(), Duration::ZERO);
}

// A socket freed after a dial is refused, and before the probe begins to
// wait, is not missed: the wait is on signals armed before that dial, so
// it ends at once and the next dial opens.
#[tokio::test(start_paused = true)]
async fn a_socket_freed_as_a_dial_is_refused_is_not_missed() {
    let frees = FreesOnRefusal::default();
    let slotted = Slotted(&frees);
    let server = first();
    let started = tokio::time::Instant::now();
    let probed = timed(&server, opens(&slotted, &server)).await;
    assert!(probed.supported, "{probed:?}");
    assert_eq!(frees.dials.load(Ordering::Relaxed), 3);
    assert_eq!(started.elapsed(), Duration::ZERO);
}

// A wait stops the probe's clock without resetting it. The probe spends
// 3 s dialling, waits for a socket the test holds, then needs 3 s more: it
// runs out at 5 s of its own time.
#[tokio::test(start_paused = true)]
async fn a_wait_resumes_the_clock_with_only_what_was_left() {
    let shared = Shared::new(plain_server(Some(Duration::from_secs(3))), 1);
    let slotted = Slotted(&*shared);
    let server = first();
    let held = Mutex::new(None);
    let (probed, ()) = tokio::join!(
        timed(&server, async {
            drop(slotted.open(&server).await.map_err(Failed::Unreached)?);
            *held.lock().unwrap() = Some(shared.take());
            opens(&slotted, &server).await
        }),
        async {
            // The second dial was refused, and dialled again armed.
            shared.refusals(2).await;
            tokio::time::advance(SERVER_TIMEOUT * 2).await;
            drop(held.lock().unwrap().take());
        }
    );
    assert_eq!(
        probed.error.map(|error| error.failure),
        Some(LookupFailure::TimedOut)
    );
    assert_eq!(probed.elapsed, SERVER_TIMEOUT);
}

// The socket wait limit is for all of a probe's waits together. The probe
// waits half the limit for the test's socket, opens a stream, then waits
// for a socket nobody frees: it fails on the route's account once the two
// waits make the whole limit.
#[tokio::test(start_paused = true)]
async fn the_socket_wait_limit_counts_every_wait_a_probe_makes() {
    let shared = Shared::new(plain_server(None), 1);
    let held = Mutex::new(Some(shared.take()));
    let slotted = Slotted(&*shared);
    let server = first();
    let started = tokio::time::Instant::now();
    let (probed, ()) = tokio::join!(
        timed(&server, async {
            drop(slotted.open(&server).await.map_err(Failed::Unreached)?);
            *held.lock().unwrap() = Some(shared.take());
            opens(&slotted, &server).await
        }),
        async {
            shared.refusals(2).await;
            tokio::time::advance(SOCKET_WAIT / 2).await;
            drop(held.lock().unwrap().take());
        }
    );
    assert_eq!(
        probed.error.map(|error| error.failure),
        Some(LookupFailure::Other)
    );
    assert_eq!(started.elapsed(), SOCKET_WAIT);
}

// A probe with a stream open is being answered, or not, by the server, so
// its clock runs while its next query waits for a socket.
#[tokio::test(start_paused = true)]
async fn a_probe_with_a_stream_open_runs_its_clock_while_it_waits_for_another() {
    let shared = Shared::new(plain_server(None), 1);
    let slotted = Slotted(&*shared);
    let server = first();
    let probed = timed(&server, async {
        let _open = slotted.open(&server).await.map_err(Failed::Unreached)?;
        opens(&slotted, &server).await
    })
    .await;
    assert!(shared.at_capacity.load(Ordering::Relaxed) > 0);
    assert_eq!(
        probed.error.map(|error| error.failure),
        Some(LookupFailure::TimedOut)
    );
    assert_eq!(probed.elapsed, SERVER_TIMEOUT);
}

// Probes dropped while one dials and the other waits for the socket leave
// nothing of theirs in the share: a later probe gets the socket.
#[tokio::test(start_paused = true)]
async fn probes_dropped_mid_dial_and_mid_wait_leave_the_share_free() {
    let shared = Shared::new(Hanging, 1);
    let slotted = Slotted(&*shared);
    let server = first();
    {
        let one = std::pin::pin!(timed(&server, opens(&slotted, &server)));
        let other = std::pin::pin!(timed(&server, opens(&slotted, &server)));
        tokio::select! {
            biased;
            // Whichever dialled first holds the socket; the other was
            // refused, dialled again armed, and waits.
            () = shared.refusals(2) => {}
            probed = one => panic!("{probed:?}"),
            probed = other => panic!("{probed:?}"),
        }
    }
    assert_eq!(
        shared.sockets.held(crate::pipe::SocketClass::ReserveCore),
        0
    );
    let refused = shared.at_capacity.load(Ordering::Relaxed);
    // Hanging has nothing at 853, so a dial that gets the socket fails at
    // once, in the server's words.
    let later = DnsServer {
        address: "192.0.2.53:853".parse().unwrap(),
        wire: Wire::Tls { name: None },
    };
    let probed = timed(&later, opens(&slotted, &later)).await;
    assert_eq!(shared.at_capacity.load(Ordering::Relaxed), refused);
    assert_eq!(
        probed.error.map(|error| error.failure),
        Some(LookupFailure::NoReply)
    );
}

// A dial under way holds its socket, so a probe with one query dialling
// and another waiting for the socket runs its clock, and a dial that never
// completes ends at the probe's limit.
#[tokio::test(start_paused = true)]
async fn a_dial_under_way_counts_against_the_probes_time_limit() {
    let shared = Shared::new(Hanging, 1);
    let slotted = crate::dns::probe::Slotted(&*shared);
    let server = first();
    let probed = crate::dns::probe::timed(&server, async {
        let (one, other) = tokio::join!(slotted.open(&server), slotted.open(&server));
        one.and(other)
            .map(drop)
            .map_err(crate::dns::Failed::Unreached)
    })
    .await;
    // The second dial was refused, then dialled again armed, and waited.
    assert_eq!(shared.at_capacity.load(Ordering::Relaxed), 2);
    assert_eq!(
        probed.error.map(|error| error.failure),
        Some(LookupFailure::TimedOut)
    );
    assert_eq!(probed.elapsed, SERVER_TIMEOUT);
}

// Probes waiting for a socket end when their leg is revoked: revoking
// closes the stream that holds the socket, the freed socket wakes the
// waits, and the leg refuses their dials. None runs to its limit. Every
// port is silent, so whichever probe dials first holds the socket.
#[tokio::test(start_paused = true)]
async fn probes_waiting_for_a_socket_end_when_the_leg_is_revoked() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let silent = || {
        Servers::new(Speaks::Dot { close_after: None }, serving(&resolver, &[]))
            .silent_at("192.0.2.53")
    };
    let carrier = ByPort(vec![(53, silent()), (853, silent()), (443, silent())]);
    let sessions = probing(&authority, &resolver);
    let shared = Shared::new(carrier, 1);
    let leg = crate::pipe::Revocable::new(shared.clone() as Arc<dyn Dialer>);
    let slotted = Slotted(&leg);
    let server = first();
    let (probe, ()) = tokio::join!(probe_on(&slotted, &sessions, &server), async {
        // One probe holds the socket; each of the other three was refused,
        // dialled again armed, and waits for it.
        shared.refusals(6).await;
        leg.revoke();
    });
    let mut revoked = 0;
    for (wire, probed) in [
        ("plain", &probe.plain),
        ("tls", &probe.tls),
        ("https", &probe.https),
        ("dnssec", &probe.dnssec),
    ] {
        assert!(!probed.supported, "{wire}");
        let error = probed.error.as_ref().unwrap();
        assert!(
            !matches!(
                error.failure,
                LookupFailure::TimedOut | LookupFailure::Other
            ),
            "{wire}: {probed:?}"
        );
        if error.failure == LookupFailure::NoReply && error.detail.contains("revoked") {
            revoked += 1;
        }
    }
    // At least the three that waited were refused by the revoked leg.
    assert!(revoked >= 3, "{probe:?}");
}

// A share that never frees: each probe stops waiting after the socket wait
// limit, and fails as this machine's doing, not the server's.
#[tokio::test(start_paused = true)]
async fn a_share_that_never_frees_fails_each_probe_on_the_routes_account() {
    let shared = Shared::new(Hanging, 1);
    let _held = shared.take();
    let probe = probe_on(
        &crate::dns::probe::Slotted(&*shared),
        &DnsSessions::default(),
        &first(),
    )
    .await;
    for (wire, probed) in [
        ("plain", &probe.plain),
        ("tls", &probe.tls),
        ("https", &probe.https),
        ("dnssec", &probe.dnssec),
    ] {
        assert!(!probed.supported, "{wire}");
        assert_eq!(
            probed.error.as_ref().map(|error| error.failure),
            Some(LookupFailure::Other),
            "{wire}: {probed:?}"
        );
        assert_eq!(probed.elapsed, std::time::Duration::ZERO, "{wire}");
    }
    assert_eq!(
        probe.plain.error.unwrap().detail,
        format!(
            "{}: waited {} s in all for a DNS socket on the route",
            first(),
            SOCKET_WAIT.as_secs()
        )
    );
}
