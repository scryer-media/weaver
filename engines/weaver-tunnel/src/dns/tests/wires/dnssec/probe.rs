// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A server's probes, against fixture servers that each speak one wire.
use super::*;
use crate::dns::Exchanged;
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
    DnsSessions::with_roots(Some(roots(authority.store(), resolver, NOW)))
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
            sockets: crate::pipe::reserve_of(core as u64),
            at_capacity: AtomicUsize::new(0),
            refused: tokio::sync::Notify::new(),
        })
    }
    // The share's one socket class for DNS.
    fn take(&self) -> crate::pipe::SocketPermit {
        self.sockets
            .take(
                crate::pipe::SocketClass::Reserve,
                crate::pipe::dns_stream_bytes(),
            )
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
            .room(
                crate::pipe::SocketClass::of(purpose),
                crate::pipe::dns_stream_bytes(),
            )
            .map_err(|freed| vec![freed.notify()])
    }
    async fn dial(&self, target: &Target) -> Result<crate::pipe::Dialed, DialError> {
        let socket = self
            .sockets
            .take(
                crate::pipe::SocketClass::of(&target.purpose),
                crate::pipe::dns_stream_bytes(),
            )
            .map_err(|refused| {
                self.at_capacity.fetch_add(1, Ordering::Relaxed);
                self.refused.notify_waiters();
                DialError::AtCapacity(refused.into())
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
        Err(DialError::AtCapacity(vec![self.freed.clone()].into()))
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
            0 => Err(DialError::AtCapacity(vec![self.freed.clone()].into())),
            1 => {
                self.freed.notify_waiters();
                Err(DialError::AtCapacity(vec![self.freed.clone()].into()))
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
        Err(DialError::AtCapacity(
            vec![Arc::new(tokio::sync::Notify::new())].into(),
        ))
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
        Err(DialError::AtCapacity(
            vec![named[named.len() - 1].clone()].into(),
        ))
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
async fn opens(slotted: &Slotted<'_>, server: &DnsServer) -> Result<(), Exchanged> {
    slotted
        .open(server)
        .await
        .map(drop)
        .map_err(Exchanged::Unreached)
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
    assert_eq!(shared.sockets.held(crate::pipe::SocketClass::Reserve), 0);
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
            drop(slotted.open(&server).await.map_err(Exchanged::Unreached)?);
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
            drop(slotted.open(&server).await.map_err(Exchanged::Unreached)?);
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
        let _open = slotted.open(&server).await.map_err(Exchanged::Unreached)?;
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
    assert_eq!(shared.sockets.held(crate::pipe::SocketClass::Reserve), 0);
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
            .map_err(crate::dns::Exchanged::Unreached)
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

// How a fixture server changes each reply before it goes out, given the
// query it answers.
type Shape = fn(&[u8], Vec<u8>) -> Vec<u8>;

fn as_made(_: &[u8], reply: Vec<u8>) -> Vec<u8> {
    reply
}

// The reply with its question's type changed: it answers another question
// than the one asked.
fn other_question(query: &[u8], mut reply: Vec<u8>) -> Vec<u8> {
    let (_, end) = question(query);
    reply[end - 4..end - 2].copy_from_slice(&16u16.to_be_bytes());
    reply
}

// The reply with its records taken out: the question asked, answered with
// nothing, NODATA.
fn no_data(query: &[u8], reply: Vec<u8>) -> Vec<u8> {
    let (_, end) = question(query);
    let mut empty = reply[..end].to_vec();
    empty[6..12].fill(0);
    empty
}

// The reply as an answer that follows one alias more than a lookup takes:
// the name asked is an alias of a0., a0. of a1., and so on to a9., ten
// links, and nothing besides.
fn long_chain(query: &[u8], reply: Vec<u8>) -> Vec<u8> {
    const LINKS: u8 = 10;
    let (_, end) = question(query);
    let mut chain = reply[..end].to_vec();
    chain[6..8].copy_from_slice(&u16::from(LINKS).to_be_bytes());
    chain[8..12].fill(0);
    for link in 0..LINKS {
        match link {
            // The question's name, by a pointer to it.
            0 => chain.extend_from_slice(&[0xc0, 12]),
            _ => chain.extend_from_slice(&[2, b'a', b'0' + link - 1, 0]),
        }
        // CNAME, IN, a TTL of 60, and the four bytes of the alias.
        chain.extend_from_slice(&[0, 5, 0, 1, 0, 0, 0, 60, 0, 4]);
        chain.extend_from_slice(&[2, b'a', b'0' + link, 0]);
    }
    chain
}

// The long chain reads as a reply, so only the cap on aliases followed
// turns it away, for the root's SOA as for a host name's address.
#[test]
fn a_long_chain_reads_and_only_the_alias_cap_refuses_it() {
    use crate::dns::judge::as_sent;
    use weaver_dnssec::{Name, Query, RecordType, Reply};
    let host = Name::from_wire(b"\x03www\x07example\x03com\x00").unwrap();
    for (name, qtype) in [(Name::root(), 6), (host, 1)] {
        let query = as_sent::packet(7, &name, qtype);
        let mut reply = query.clone();
        reply[2] |= 0x80;
        let chained = long_chain(&query, reply);
        let asked = Query::new(name, RecordType::new(qtype));
        let read = Reply::read(&asked, &chained).unwrap();
        assert_eq!(read.answer().alias_chain().len(), 11);
        assert!(matches!(
            as_sent::read(&asked, 7, &chained),
            Err(crate::dns::Exchanged::Invalid)
        ));
    }
}

// The reply under an id no query has, and nothing under the query's own.
fn stray_only(_: &[u8], reply: Vec<u8>) -> Vec<u8> {
    stray(reply)
}

fn h2_doing(misstep: H2Misstep) -> Speaks {
    Speaks::H2 {
        respond: Arc::new(h2_message),
        goaway_after: None,
        misstep,
    }
}

fn dot_speaking() -> Speaks {
    Speaks::Dot { close_after: None }
}

// One server at 192.0.2.53 on `kind` alone, at the wire's usual port,
// speaking `speaks` once TLS is up, with a certificate `authority` signed
// for its address, and answering the fixture's signed zones with each reply
// shaped by `shape`; then `adjusted`. With it, the server as a lookup lists
// it on that wire, which is the server the probe of that wire asks. The
// server test of `first()` reaches it on that wire's probe only: every
// other port, the DNSSEC check's among them, has nothing listening.
fn only_on(
    kind: WireKind,
    speaks: Speaks,
    authority: &Authority,
    resolver: &Arc<FakeResolver>,
    shape: Shape,
    adjusted: fn(Servers) -> Servers,
) -> (ByPort, DnsServer) {
    let base = serving(resolver, &[]);
    let alpn: &[u8] = match &speaks {
        Speaks::Dot { .. } => b"dot",
        Speaks::Doh { .. } => b"http/1.1",
        Speaks::H2 { .. } => b"h2",
    };
    let servers = Servers::new(speaks, move |asked, qtype, query: &[u8]| {
        shape(query, base(asked, qtype, query))
    });
    let (servers, listed) = match kind {
        WireKind::Plain => (servers.plain_at("192.0.2.53"), first()),
        WireKind::Tls => (
            servers.at("192.0.2.53", authority.server(&["192.0.2.53"], alpn)),
            dot("192.0.2.53", None),
        ),
        WireKind::Https => (
            servers.at("192.0.2.53", authority.server(&["192.0.2.53"], alpn)),
            doh("192.0.2.53:443"),
        ),
    };
    (
        ByPort(vec![(listed.address.port(), adjusted(servers))]),
        listed,
    )
}

// The probe of `kind`'s wire.
fn of_wire(probe: &crate::dns::ServerProbe, kind: WireKind) -> &crate::dns::Probed {
    match kind {
        WireKind::Plain => &probe.plain,
        WireKind::Tls => &probe.tls,
        WireKind::Https => &probe.https,
    }
}

// What the server test of `first()` says of the wire `kind` through
// `carrier`.
async fn probed_on(
    carrier: &ByPort,
    authority: &Authority,
    resolver: &FakeResolver,
    kind: WireKind,
) -> crate::dns::Probed {
    let probe = probe_on(carrier, &probing(authority, resolver), &first()).await;
    of_wire(&probe, kind).clone()
}

// A reply that answers another question than the one asked fails the
// probe of every wire as an invalid response, as it fails a lookup.
#[tokio::test(start_paused = true)]
async fn a_server_answering_another_question_is_not_supported_on_any_wire() {
    let resolver = tree(KeyAlgorithm::P256);
    for (kind, speaks) in [
        (WireKind::Plain, dot_speaking()),
        (WireKind::Tls, dot_speaking()),
        (WireKind::Https, doh_fixture(plain_reply, false)),
        (WireKind::Https, h2_doing(H2Misstep::None)),
    ] {
        let authority = Authority::new();
        let (carrier, listed) = only_on(
            kind,
            speaks,
            &authority,
            &resolver,
            other_question,
            std::convert::identity,
        );
        let probed = probed_on(&carrier, &authority, &resolver, kind).await;
        assert!(!probed.supported, "{listed}: {probed:?}");
        assert_eq!(
            probed.error,
            Some(failure(
                LookupFailure::Other,
                &format!("{listed}: {}", crate::dns::INVALID)
            )),
            "{listed}"
        );
    }
}

// An HTTP/2 server that refuses the probe's stream once and answers it
// sent again: the probe sends it again on the same connection, as a lookup
// does, and the wire is supported.
#[tokio::test(start_paused = true)]
async fn an_http_2_stream_refused_once_is_sent_again_and_the_wire_is_supported() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let (carrier, _) = only_on(
        WireKind::Https,
        h2_doing(H2Misstep::RefuseFirst),
        &authority,
        &resolver,
        as_made,
        std::convert::identity,
    );
    let probed = probed_on(&carrier, &authority, &resolver, WireKind::Https).await;
    assert!(probed.supported, "{probed:?}");
    let servers = &carrier.0[0].1;
    // Two sends on the one connection: the one refused, and the one
    // answered.
    assert_eq!(servers.dials().len(), 1);
    assert_eq!(servers.log.lock().unwrap().refused, 1);
    assert_eq!(servers.answered(), [1]);
}

// An HTTP/2 server that refuses every stream: the probe's query goes out
// three times on the one connection, and the wire fails as the server
// resetting the query.
#[tokio::test(start_paused = true)]
async fn an_http_2_stream_refused_every_time_fails_as_reset_after_three_sends() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let (carrier, listed) = only_on(
        WireKind::Https,
        h2_doing(H2Misstep::RefuseEvery),
        &authority,
        &resolver,
        as_made,
        std::convert::identity,
    );
    let probed = probed_on(&carrier, &authority, &resolver, WireKind::Https).await;
    assert!(!probed.supported);
    assert_eq!(
        probed.error,
        Some(failure(
            LookupFailure::NoReply,
            &format!("{listed}: the server reset the query")
        ))
    );
    let servers = &carrier.0[0].1;
    assert_eq!(servers.dials().len(), 1);
    assert_eq!(servers.log.lock().unwrap().refused, 3);
}

// A DNS over TLS reply under an id no query has is passed over, as a lookup
// passes over it. Followed by nothing, the probe waits out the server's
// time limit; followed by the right reply, the wire is supported.
#[tokio::test(start_paused = true)]
async fn a_dot_reply_under_an_unknown_id_is_passed_over() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let (carrier, listed) = only_on(
        WireKind::Tls,
        dot_speaking(),
        &authority,
        &resolver,
        stray_only,
        std::convert::identity,
    );
    let probed = probed_on(&carrier, &authority, &resolver, WireKind::Tls).await;
    assert!(!probed.supported);
    assert_eq!(
        probed.error,
        Some(failure(
            LookupFailure::TimedOut,
            &format!("{listed}: no answer within {}s", SERVER_TIMEOUT.as_secs())
        ))
    );
    assert_eq!(probed.elapsed, SERVER_TIMEOUT);

    let (carrier, _) = only_on(
        WireKind::Tls,
        dot_speaking(),
        &authority,
        &resolver,
        as_made,
        Servers::straying,
    );
    let probed = probed_on(&carrier, &authority, &resolver, WireKind::Tls).await;
    assert!(probed.supported, "{probed:?}");
    assert_eq!(carrier.0[0].1.dials().len(), 1);
}

// For each way a server misbehaves, the same fake server is asked by a
// lookup and by the server test of its wire, and both come to the same
// outcome: an answer and no error, or the same error in the same words.
#[tokio::test(start_paused = true)]
async fn the_server_test_and_a_lookup_come_to_the_same_outcome_through_one_server() {
    let resolver = tree(KeyAlgorithm::P256);
    let identity: fn(Servers) -> Servers = std::convert::identity;
    // The wire tested, how the server speaks, how it shapes its reply, and
    // what else it does.
    type Case = (WireKind, fn() -> Speaks, Shape, fn(Servers) -> Servers);
    let cases: [Case; 11] = [
        (WireKind::Plain, dot_speaking, as_made, identity),
        (WireKind::Plain, dot_speaking, no_data, identity),
        (WireKind::Plain, dot_speaking, long_chain, identity),
        (WireKind::Plain, dot_speaking, other_question, identity),
        (WireKind::Tls, dot_speaking, other_question, identity),
        (WireKind::Tls, dot_speaking, stray_only, identity),
        (WireKind::Tls, dot_speaking, as_made, Servers::straying),
        (
            WireKind::Https,
            || doh_fixture(plain_reply, false),
            other_question,
            identity,
        ),
        (
            WireKind::Https,
            || h2_doing(H2Misstep::None),
            other_question,
            identity,
        ),
        (
            WireKind::Https,
            || h2_doing(H2Misstep::RefuseFirst),
            as_made,
            identity,
        ),
        (
            WireKind::Https,
            || h2_doing(H2Misstep::RefuseEvery),
            as_made,
            identity,
        ),
    ];
    for (at, (kind, speaks, shape, adjusted)) in cases.into_iter().enumerate() {
        let authority = Authority::new();
        let (carrier, listed) = only_on(kind, speaks(), &authority, &resolver, shape, adjusted);
        let looked_up = recorded(
            &carrier.0[0].1,
            &authority.wires(),
            std::slice::from_ref(&listed),
            Trust::AsSent,
            "www.example.com",
        )
        .await;
        let probed = probed_on(&carrier, &authority, &resolver, kind).await;
        // The server answered the lookup when it gave addresses, or said
        // the name has none: the probe takes any reply that reads as an
        // answer, whatever its records.
        let answered = match &looked_up.error {
            None => true,
            Some(error) => matches!(
                error.failure,
                LookupFailure::NoAddress | LookupFailure::NoSuchName
            ),
        };
        assert_eq!(
            (probed.supported, probed.error),
            (answered, looked_up.error.filter(|_| !answered)),
            "case {at}: {listed}"
        );
    }
}

// `servers` reached only at `port`, through the stage whose keepers they
// name: a probe's dial to any other port reaches nothing.
struct AtPort<'a>(&'a Servers, u16);

#[async_trait::async_trait]
impl Carrier for AtPort<'_> {
    fn keepers(&self) -> Vec<crate::pipe::KeeperRef> {
        self.0.keepers()
    }
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        if server.address.port() != self.1 {
            return Err(TunnelError::Engine(format!(
                "nothing listens at {}",
                server.address
            )));
        }
        self.0.open(server).await
    }
}

// A server test keeps and reuses nothing, on every encrypted wire: once it
// returns, every connection it opened has ended on the server's side and
// nothing is open, and a lookup by a naming on the same sessions, through a
// stage that could take a kept connection, dials afresh. The clock is
// paused, and an await that finds nothing to run moves it on, past a kept
// connection's idle close: so nothing kept is checked before any await, and
// the clock is checked not to have moved while the connections end.
#[tokio::test(start_paused = true)]
async fn a_server_test_keeps_no_connection_and_a_lookup_after_it_dials_afresh() {
    let resolver = tree(KeyAlgorithm::P256);
    let wires = [
        (
            "dot",
            Speaks::Dot { close_after: None },
            &b"dot"[..],
            dot("192.0.2.53", None),
        ),
        (
            "http/1.1",
            doh_fixture(plain_reply, false),
            b"http/1.1",
            doh("192.0.2.53:443"),
        ),
        (
            "h2",
            Speaks::H2 {
                respond: Arc::new(h2_message),
                goaway_after: None,
                misstep: H2Misstep::None,
            },
            b"h2",
            doh("192.0.2.53:443"),
        ),
    ];
    for (wire, speaks, alpn, listed) in wires {
        let authority = Authority::new();
        let servers = Servers::new(speaks, serving(&resolver, &[]))
            .at("192.0.2.53", authority.server(&["192.0.2.53"], alpn));
        let carrier = AtPort(&servers, listed.address.port());
        let sessions = probing(&authority, &resolver);
        let probe = probe_on(&carrier, &sessions, &listed).await;
        let left = tokio::time::Instant::now();
        assert_eq!(sessions.kept().len(), 0, "{wire}");
        assert!(probe.dnssec.supported, "{wire}: {:?}", probe.dnssec);
        let probed = servers.dials().len();
        assert!(probed > 0, "{wire}");
        servers
            .ended
            .acquire_many(probed as u32)
            .await
            .unwrap()
            .forget();
        assert_eq!(tokio::time::Instant::now(), left, "{wire}");
        assert_eq!(servers.open_now(), 0, "{wire}");
        assert_eq!(sessions.kept().len(), 0, "{wire}");

        let (resolved, _) = resolve_judged(
            &carrier,
            &sessions,
            std::slice::from_ref(&listed),
            Trust::AsSent,
            "www.example.com",
        )
        .await;
        assert!(!resolved.unwrap().addresses.is_empty(), "{wire}");
        assert!(servers.dials().len() > probed, "{wire}");
    }
}
