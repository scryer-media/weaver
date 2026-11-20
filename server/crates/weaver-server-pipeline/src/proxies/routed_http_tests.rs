// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::Database;
use crate::categories::persistence::CategoriesPersistenceQueries;
use crate::proxies::ProxiesDnsPersistenceQueries;
use crate::proxies::ProxiesNetworkPersistenceQueries;
use crate::proxies::ProxiesPersistenceQueries;
use crate::proxies::{LegHealthState, ProxyKind, outbound_only_through};
use axum::Router;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use std::collections::HashMap;
use std::sync::Mutex;
use weaver_tunnel::{TunnelError, TunnelStream};

type Requests = Arc<Mutex<Vec<(String, String)>>>;

// A path whose dials all land on one origin. It resolves names from a fixed
// map, or hands them to the exit when it has none.
struct Mapped {
    names: Option<HashMap<&'static str, IpAddr>>,
    origin: SocketAddr,
    pins: Mutex<Option<(String, Vec<IpAddr>)>>,
    // Each dial's host and the addresses pinned for it.
    dials: Mutex<Vec<(String, Vec<IpAddr>)>>,
}

impl Mapped {
    fn new(origin: SocketAddr, names: Option<&[(&'static str, [u8; 4])]>) -> Arc<Self> {
        Arc::new(Self {
            names: names.map(|names| {
                names
                    .iter()
                    .map(|(name, ip)| (*name, IpAddr::from(*ip)))
                    .collect()
            }),
            origin,
            pins: Mutex::new(None),
            dials: Mutex::new(Vec::new()),
        })
    }
    fn dials(&self) -> Vec<(String, Vec<IpAddr>)> {
        self.dials.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl FetchPath for Mapped {
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        // A stage reports an address named in the URL whoever resolves names.
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(Resolution::Addresses(vec![ip]));
        }
        match &self.names {
            None => Ok(Resolution::Unresolved),
            Some(names) => names
                .get(host)
                .map(|ip| Resolution::Addresses(vec![*ip]))
                .ok_or_else(|| {
                    DialError::Destination(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "no such name",
                    ))
                }),
        }
    }
    fn pin(&self, host: &str, addresses: Vec<IpAddr>) {
        *self.pins.lock().unwrap() = Some((host.to_owned(), addresses));
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(30)
    }
}

#[async_trait::async_trait]
impl TunnelProvider for Mapped {
    async fn dial(&self, host: &str, _: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let pinned = self
            .pins
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(pinned, _)| pinned == host)
            .map(|(_, addresses)| addresses.clone())
            .unwrap_or_default();
        self.dials.lock().unwrap().push((host.to_owned(), pinned));
        let stream = tokio::net::TcpStream::connect(self.origin)
            .await
            .map_err(|e| TunnelError::Engine(e.to_string()))?;
        Ok(Box::new(stream))
    }
    fn describe(&self) -> String {
        "mapped".into()
    }
}

// Logs each request's path and Host header.
async fn origin() -> (SocketAddr, Requests, tokio::task::JoinHandle<()>) {
    let requests: Requests = Arc::default();
    let log = requests.clone();
    let app = Router::new().fallback(get(move |uri: axum::http::Uri, headers: HeaderMap| {
        let log = log.clone();
        async move {
            let host = headers[header::HOST].to_str().unwrap().to_owned();
            log.lock().unwrap().push((uri.path().to_owned(), host));
            let found = |location: &'static str| {
                (StatusCode::FOUND, [(header::LOCATION, location)]).into_response()
            };
            match uri.path() {
                "/to-second" => found("http://second.test/final"),
                "/to-private" => found("http://127.0.0.1/private"),
                _ => "fetched".into_response(),
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, requests, task)
}

fn options(allow_private_network: bool) -> FetchOptions {
    FetchOptions {
        label: "TEST",
        timeout: Duration::from_secs(30),
        redirects: 3,
        user_agent: "weaver-test",
        https_only: false,
        allow_private_network,
    }
}

async fn fetch(
    path: &Arc<Mapped>,
    url: &str,
    allow_private_network: bool,
) -> Result<reqwest::Response, AttemptError> {
    let path: Arc<dyn FetchPath> = path.clone();
    fetch_on(
        &path,
        &url.parse().unwrap(),
        &options(allow_private_network),
        &|request, _, _| request,
    )
    .await
}

fn requests(requests: &Requests) -> Vec<(String, String)> {
    requests.lock().unwrap().clone()
}

fn pair(path: &str, host: &str) -> (String, String) {
    (path.into(), host.into())
}

#[tokio::test]
async fn a_proxy_resolved_path_is_handed_the_name() {
    let (addr, log, task) = origin().await;
    let path = Mapped::new(addr, None);
    let response = fetch(&path, "http://first.test/start", false)
        .await
        .unwrap();
    assert!(response.extensions().get::<RoutedBody>().is_some());
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(path.dials(), [("first.test".to_owned(), Vec::new())]);
    assert_eq!(requests(&log), [pair("/start", "first.test")]);
    task.abort();
}

#[tokio::test]
async fn a_weaver_resolved_path_dials_the_checked_address_under_the_name() {
    let (addr, log, task) = origin().await;
    let path = Mapped::new(addr, Some(&[("first.test", [203, 0, 113, 7])]));
    let response = fetch(&path, "http://first.test/start", false)
        .await
        .unwrap();
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(
        path.dials(),
        [(
            "first.test".to_owned(),
            vec![IpAddr::from([203, 0, 113, 7])]
        )]
    );
    assert_eq!(requests(&log), [pair("/start", "first.test")]);
    task.abort();
}

#[tokio::test]
async fn each_redirect_is_resolved_and_checked_on_the_same_path() {
    let (addr, log, task) = origin().await;
    let path = Mapped::new(
        addr,
        Some(&[
            ("first.test", [203, 0, 113, 7]),
            ("second.test", [203, 0, 113, 8]),
        ]),
    );
    let response = fetch(&path, "http://first.test/to-second", false)
        .await
        .unwrap();
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(
        path.dials(),
        [
            (
                "first.test".to_owned(),
                vec![IpAddr::from([203, 0, 113, 7])]
            ),
            (
                "second.test".to_owned(),
                vec![IpAddr::from([203, 0, 113, 8])]
            ),
        ]
    );
    assert_eq!(
        requests(&log),
        [
            pair("/to-second", "first.test"),
            pair("/final", "second.test")
        ]
    );
    task.abort();
}

#[tokio::test]
async fn a_redirect_to_a_private_address_is_refused_before_it_is_dialed() {
    let (addr, log, task) = origin().await;
    // The exit resolves names, but an address in the URL is still checked.
    let path = Mapped::new(addr, None);
    let error = fetch(&path, "http://first.test/to-private", false)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, AttemptError::Destination(message) if message == "TEST destination address is not allowed"),
        "{error:?}"
    );
    assert_eq!(path.dials(), [("first.test".to_owned(), Vec::new())]);
    assert_eq!(requests(&log), [pair("/to-private", "first.test")]);

    // Allowed, the same redirect is followed.
    let response = fetch(&path, "http://first.test/to-private", true)
        .await
        .unwrap();
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(path.dials().last().unwrap().0, "127.0.0.1");
    task.abort();
}

#[tokio::test]
async fn a_name_resolving_to_a_private_address_is_never_dialed() {
    let (addr, log, task) = origin().await;
    let path = Mapped::new(addr, Some(&[("first.test", [10, 1, 2, 3])]));
    let error = fetch(&path, "http://first.test/start", false)
        .await
        .unwrap_err();
    assert!(matches!(error, AttemptError::Destination(_)), "{error:?}");
    assert!(path.dials().is_empty());
    assert!(requests(&log).is_empty());
    task.abort();
}

#[tokio::test]
async fn the_outbound_route_goes_direct_when_none_is_saved() {
    let (addr, log, task) = origin().await;
    let runtime = ProxyRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let url = format!("http://{addr}/start").parse().unwrap();
    let response = routed_fetch(
        &runtime,
        Consumer::Outbound,
        &url,
        &options(true),
        |request, _, _| request,
    )
    .await
    .unwrap();
    assert!(response.extensions().get::<RoutedBody>().is_some());
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(requests(&log), [pair("/start", &addr.to_string())]);
    runtime.stop_all().await;
    task.abort();
}

// With every proxy failing and no direct fallback, the fetch fails rather
// than leave Weaver directly, though a direct fetch would have worked.
#[tokio::test]
async fn the_outbound_route_fails_closed_when_its_proxies_fail() {
    let (addr, log, task) = origin().await;
    // Refuses every target: it has none mapped.
    let proxy = weaver_tunnel::test_support::proxy::ProxyServerDouble::start(
        weaver_tunnel::transport::TransportKind::Socks5,
        None,
        HashMap::new(),
    )
    .await;
    let runtime = outbound_only_through(ProxyKind::Socks5, proxy.addr.port());
    let url = format!("http://localhost:{}/start", addr.port())
        .parse()
        .unwrap();
    let error = routed_fetch(
        &runtime,
        Consumer::Outbound,
        &url,
        &options(true),
        |request, _, _| request,
    )
    .await
    .unwrap_err();
    assert_eq!(error, "TEST request failed on all permitted routes");
    assert_eq!(
        proxy.targets.lock().unwrap().clone(),
        [("localhost".to_owned(), addr.port())]
    );
    assert!(requests(&log).is_empty());
    runtime.stop_all().await;
    task.abort();
}

// What the Default outbound route's one leg shows: its rungs' states and
// the leg's health.
fn outbound_health(runtime: &ProxyRuntime) -> (Vec<&'static str>, LegHealthState) {
    let route = runtime
        .network
        .route(Consumer::Outbound, 1, Duration::from_secs(30))
        .unwrap();
    let rungs = route.legs.read().unwrap()[0].rung_states();
    let health = route.weighted.allocations().swap_remove(0).health;
    (rungs, health)
}

// A destination that reads each request, then starts an answer and closes
// the connection before its headers end. It counts the requests it read.
async fn dropping() -> (
    SocketAddr,
    Arc<std::sync::atomic::AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = count.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                head.push(stream.read_u8().await.unwrap());
            }
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-")
                .await
                .unwrap();
        }
    });
    (addr, count, task)
}

fn outbound_fetch<'a>(
    runtime: &'a ProxyRuntime,
    url: &'a str,
    options: &'a FetchOptions,
) -> impl std::future::Future<Output = Result<reqwest::Response, String>> + 'a {
    let url: reqwest::Url = url.parse().unwrap();
    async move {
        routed_fetch(
            runtime,
            Consumer::Outbound,
            &url,
            options,
            |request, _, _| request,
        )
        .await
    }
}

// The proxy carries the connection, and the destination drops the request
// before its headers end: that request fails as the destination's, and the
// route is charged nothing. The next fetch on it, to another host, dials
// and is answered.
#[tokio::test]
async fn a_destination_that_drops_a_request_does_not_cool_the_route() {
    let (addr, log, task) = origin().await;
    let (dropping, dropped, dropping_task) = dropping().await;
    let proxy = weaver_tunnel::test_support::proxy::ProxyServerDouble::start(
        weaver_tunnel::transport::TransportKind::Socks5,
        None,
        HashMap::from([
            (("dropping.test".to_owned(), 80), dropping),
            (("origin.test".to_owned(), 80), addr),
        ]),
    )
    .await;
    let runtime = outbound_only_through(ProxyKind::Socks5, proxy.addr.port());
    // Twice: two path failures in a row would cool the leg as well.
    for _ in 0..2 {
        let error = outbound_fetch(&runtime, "http://dropping.test/start", &options(true))
            .await
            .unwrap_err();
        assert_eq!(error, "TEST request failed after its route connected");
        assert_eq!(
            outbound_health(&runtime),
            (vec!["STANDBY"], LegHealthState::Up)
        );
    }
    let response = outbound_fetch(&runtime, "http://origin.test/start", &options(true))
        .await
        .unwrap();
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(
        proxy.targets.lock().unwrap().clone(),
        [
            ("dropping.test".to_owned(), 80),
            ("dropping.test".to_owned(), 80),
            ("origin.test".to_owned(), 80),
        ]
    );
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(requests(&log), [pair("/start", "origin.test")]);
    runtime.stop_all().await;
    dropping_task.abort();
    task.abort();
}

// A proxy that cannot be reached at all is the path's failure: its rung
// cools, and the fetch fails closed.
#[tokio::test]
async fn an_unreachable_proxy_cools_its_rung() {
    let only = StandIn::new(Proxying::Down, &[]);
    let db = with_proxies(&[1]);
    let runtime = standing_in(&db, vec![ladder(100, vec![proxy(1)])], &[(1, &only)]);
    let error = outbound_fetch(&runtime, "http://origin.test/start", &options(true))
        .await
        .unwrap_err();
    assert_eq!(error, "TEST request failed on all permitted routes");
    assert_eq!(outbound_health(&runtime).0, ["COOLDOWN"]);
    assert_eq!(only.hosts(), ["origin.test"]);
    runtime.stop_all().await;
}

// What a stand-in proxy does with a dial through it.
#[derive(Clone, Copy)]
enum Proxying {
    // Carries it to the address.
    To(SocketAddr),
    // Takes it, then resets it with nothing sent back once the request
    // arrives.
    Resets,
    // Cannot be reached.
    Down,
    // Answers TLS with a certificate nothing trusts.
    Untrusted,
    // Answers TLS with a fatal handshake_failure alert: a TLS failure that
    // says nothing of a certificate.
    Alerts,
    // Holds it until the test lets it go, then carries it to the address.
    Held(SocketAddr),
    // Carries it to a destination that reads the request and never answers.
    Stalls,
}

// The requests a stalled destination read, shared by every stand-in that
// carries to it.
#[derive(Default)]
struct Stall {
    asked: std::sync::atomic::AtomicUsize,
    heard: tokio::sync::Notify,
}

impl Stall {
    fn asked(&self) -> usize {
        self.asked.load(std::sync::atomic::Ordering::SeqCst)
    }
}

// A proxy session in place of an HTTP/3 proxy's: what it does with a dial
// depends on the host dialed.
struct StandIn {
    default: Proxying,
    hosts: HashMap<&'static str, Proxying>,
    dials: Mutex<Vec<(String, u16)>>,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
    stall: Arc<Stall>,
}

impl StandIn {
    fn new(default: Proxying, hosts: &[(&'static str, Proxying)]) -> Arc<Self> {
        Self::stalling(default, hosts, &Arc::default())
    }
    // One whose stalled destination is `stall`.
    fn stalling(
        default: Proxying,
        hosts: &[(&'static str, Proxying)],
        stall: &Arc<Stall>,
    ) -> Arc<Self> {
        Arc::new(Self {
            default,
            hosts: hosts.iter().copied().collect(),
            dials: Mutex::default(),
            reached: Default::default(),
            release: Default::default(),
            stall: stall.clone(),
        })
    }
    fn dials(&self) -> Vec<(String, u16)> {
        self.dials.lock().unwrap().clone()
    }
    fn hosts(&self) -> Vec<String> {
        self.dials().into_iter().map(|(host, _)| host).collect()
    }
}

#[async_trait::async_trait]
impl TunnelProvider for StandIn {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.dials.lock().unwrap().push((host.to_owned(), port));
        let connect = |addr: SocketAddr| async move {
            tokio::net::TcpStream::connect(addr)
                .await
                .map(|stream| Box::new(stream) as Box<dyn TunnelStream>)
                .map_err(|e| TunnelError::Engine(e.to_string()))
        };
        match self.hosts.get(host).copied().unwrap_or(self.default) {
            Proxying::To(addr) => connect(addr).await,
            Proxying::Resets => Ok(Box::new(Resetting::default())),
            Proxying::Down => Err(TunnelError::Engine("proxy unreachable".into())),
            Proxying::Untrusted => {
                let (near, far) = tokio::io::duplex(64 * 1024);
                let (acceptor, _, _) = crate::speed_test::fixture::loopback_tls();
                tokio::spawn(async move {
                    let _ = acceptor.accept(far).await;
                });
                Ok(Box::new(near))
            }
            Proxying::Alerts => {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let (near, mut far) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    // The client's hello first, as a server would have it.
                    let mut hello = [0u8; 4096];
                    let _ = far.read(&mut hello).await;
                    // Alert record, TLS 1.2, length 2: fatal, handshake_failure.
                    let _ = far
                        .write_all(&[0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28])
                        .await;
                    let mut sink = Vec::new();
                    let _ = far.read_to_end(&mut sink).await;
                });
                Ok(Box::new(near))
            }
            Proxying::Held(addr) => {
                self.reached.notify_one();
                self.release.notified().await;
                connect(addr).await
            }
            Proxying::Stalls => {
                use tokio::io::AsyncReadExt;
                let (near, mut far) = tokio::io::duplex(64 * 1024);
                let stall = self.stall.clone();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    while !head.ends_with(b"\r\n\r\n") {
                        let Ok(byte) = far.read_u8().await else {
                            return;
                        };
                        head.push(byte);
                    }
                    stall
                        .asked
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    stall.heard.notify_one();
                    // Held open, unanswered, until the client lets it go.
                    let mut rest = Vec::new();
                    let _ = far.read_to_end(&mut rest).await;
                });
                Ok(Box::new(near))
            }
        }
    }
    fn describe(&self) -> String {
        "stand-in".into()
    }
}

// A stream a proxy took and resets, with nothing read from it, once the
// request is written to it.
#[derive(Default)]
struct Resetting {
    asked: bool,
    reader: Option<std::task::Waker>,
}

impl tokio::io::AsyncRead for Resetting {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.asked {
            return std::task::Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()));
        }
        this.reader = Some(cx.waker().clone());
        std::task::Poll::Pending
    }
}

impl tokio::io::AsyncWrite for Resetting {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        this.asked = true;
        if let Some(reader) = this.reader.take() {
            reader.wake();
        }
        std::task::Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

// HTTP/3 proxy `id`. How a name is resolved through it is the route's DNS
// profile's to say.
fn stand_in_profile(id: u32) -> crate::proxies::ProxyProfile {
    crate::proxies::ProxyProfile {
        id,
        name: format!("proxy-{id}"),
        kind: ProxyKind::Http3Connect,
        enabled: true,
        host: "127.0.0.1".into(),
        port: 1,
        dns_profile_id: crate::proxies::SYSTEM_DNS_PROFILE_ID,
        dns_servers: vec![],
        tunnel_addresses: vec![],
        peer_public_key: None,
        mtu: 1280,
        keepalive_seconds: None,
        timeout_seconds: 300,
        host_key_fingerprint: None,
        natpmp: false,
        forwarded_port: None,
        revision: 1,
        secrets: crate::proxies::ProxySecrets {
            username: Some("fixture".into()),
            password: Some("fixture-only".into()),
            ..Default::default()
        },
    }
}

// A leg on the host's egress over `rungs`, with no direct fallback.
fn ladder(weight: u8, rungs: Vec<crate::proxies::Rung>) -> crate::proxies::RouteLeg {
    crate::proxies::RouteLeg {
        egress_id: crate::proxies::SYSTEM_EGRESS_ID,
        weight,
        path: crate::proxies::LegPath::Ladder {
            rungs,
            direct_fallback: false,
        },
    }
}

fn proxy(id: u32) -> crate::proxies::Rung {
    crate::proxies::Rung::Proxy { id }
}

// A route DNS profile. With no `listed` server, the route's proxy is handed
// every name; with one, Weaver asks that server through the proxy.
fn route_dns(db: &Database, listed: Option<SocketAddr>) -> u32 {
    use crate::proxies::{
        DnsProfile, DnsProfileKind, DnsServerList, DnsTransit, DnsTrust, DnsWho, ListedServer,
        ListedWire,
    };
    let server_list_id = listed.map(|address| {
        db.save_dns_server_list(
            DnsServerList {
                id: 0,
                name: "Listed".into(),
                servers: vec![ListedServer {
                    address,
                    wire: ListedWire::Plain,
                }],
            },
            true,
        )
        .unwrap()
        .id
    });
    db.save_dns_profile(
        DnsProfile {
            id: 0,
            name: if listed.is_some() { "Listed" } else { "Proxy" }.into(),
            kind: DnsProfileKind::Route,
            who: if listed.is_some() {
                DnsWho::Weaver
            } else {
                DnsWho::Proxy
            },
            transit: listed.map(|_| DnsTransit::Proxy),
            trust: DnsTrust::AsSent,
            server_list_id,
            system_fallback: false,
        },
        true,
    )
    .unwrap()
    .id
}

// The route DNS profile that hands the route's proxy every name, made the
// first time it is asked for.
fn proxy_names(db: &Database) -> u32 {
    db.list_dns_profiles()
        .unwrap()
        .into_iter()
        .find(|profile| profile.who == crate::proxies::DnsWho::Proxy)
        .map_or_else(|| route_dns(db, None), |profile| profile.id)
}

fn save_outbound(db: &Database, dns: u32, legs: Vec<crate::proxies::RouteLeg>) {
    db.save_proxy_routing_policy(
        Consumer::Outbound,
        &crate::proxies::RoutingPolicy {
            proxy_ids: vec![],
            allow_direct: false,
            legs,
            dns_profile_id: Some(dns),
            ..Default::default()
        },
    )
    .unwrap();
}

// A runtime whose Default outbound takes `legs`, each proxy of `proxies`
// a stand-in, and hands its proxies every name.
fn standing_in(
    db: &Database,
    legs: Vec<crate::proxies::RouteLeg>,
    proxies: &[(u32, &Arc<StandIn>)],
) -> Arc<ProxyRuntime> {
    standing_in_on(db, proxy_names(db), legs, proxies)
}

// The same, with the route's names following DNS profile `dns`.
fn standing_in_on(
    db: &Database,
    dns: u32,
    legs: Vec<crate::proxies::RouteLeg>,
    proxies: &[(u32, &Arc<StandIn>)],
) -> Arc<ProxyRuntime> {
    save_outbound(db, dns, legs);
    let runtime = ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    for (id, stand_in) in proxies {
        runtime
            .network
            .install_fixture(*id, (*stand_in).clone() as Arc<dyn TunnelProvider>);
    }
    runtime
}

fn with_proxies(ids: &[u32]) -> Database {
    let db = Database::open_in_memory().unwrap();
    for id in ids {
        db.save_proxy_profile(&stand_in_profile(*id)).unwrap();
    }
    db
}

// The proxy takes the connection and resets it with nothing sent back: the
// fetch goes on to the next rung, which carries it, and neither rung is
// charged.
#[tokio::test]
async fn a_request_reset_on_one_rung_is_carried_by_the_next() {
    let (addr, log, task) = origin().await;
    let first = StandIn::new(Proxying::Resets, &[]);
    let second = StandIn::new(Proxying::To(addr), &[]);
    let db = with_proxies(&[1, 2]);
    let runtime = standing_in(
        &db,
        vec![ladder(100, vec![proxy(1), proxy(2)])],
        &[(1, &first), (2, &second)],
    );
    let response = outbound_fetch(&runtime, "http://origin.test/start", &options(true))
        .await
        .unwrap();
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(first.dials(), [("origin.test".to_owned(), 80)]);
    assert_eq!(second.dials(), [("origin.test".to_owned(), 80)]);
    assert_eq!(
        outbound_health(&runtime),
        (vec!["STANDBY", "STANDBY"], LegHealthState::Up)
    );
    assert_eq!(requests(&log), [pair("/start", "origin.test")]);
    runtime.stop_all().await;
    task.abort();
}

// One rung, and the proxy resets the request with nothing sent back, over
// plain HTTP or during the TLS handshake: that request fails as the
// destination's, the rung stays on standby and the leg up, and the next
// fetch, to another host, dials and is answered.
#[tokio::test]
async fn a_reset_with_nothing_sent_back_fails_that_request_only() {
    let (addr, log, task) = origin().await;
    let only = StandIn::new(Proxying::To(addr), &[("dropping.test", Proxying::Resets)]);
    let db = with_proxies(&[1]);
    let runtime = standing_in(&db, vec![ladder(100, vec![proxy(1)])], &[(1, &only)]);
    for url in [
        "http://dropping.test/start",
        "http://dropping.test/start",
        "https://dropping.test/start",
    ] {
        let error = outbound_fetch(&runtime, url, &options(true))
            .await
            .unwrap_err();
        assert_eq!(error, "TEST request failed after its route connected");
        assert_eq!(
            outbound_health(&runtime),
            (vec!["STANDBY"], LegHealthState::Up)
        );
    }
    let response = outbound_fetch(&runtime, "http://origin.test/start", &options(true))
        .await
        .unwrap();
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(
        only.dials(),
        [
            ("dropping.test".to_owned(), 80),
            ("dropping.test".to_owned(), 80),
            ("dropping.test".to_owned(), 443),
            ("origin.test".to_owned(), 80),
        ]
    );
    assert_eq!(requests(&log), [pair("/start", "origin.test")]);
    runtime.stop_all().await;
    task.abort();
}

// A redirect's hop reset on one rung: the fetch starts over on the next
// rung, which follows the redirect, and neither rung is charged.
#[tokio::test]
async fn a_redirect_reset_on_one_rung_is_carried_by_the_next() {
    let (addr, log, task) = origin().await;
    let first = StandIn::new(Proxying::To(addr), &[("second.test", Proxying::Resets)]);
    let second = StandIn::new(Proxying::To(addr), &[]);
    let db = with_proxies(&[1, 2]);
    let runtime = standing_in(
        &db,
        vec![ladder(100, vec![proxy(1), proxy(2)])],
        &[(1, &first), (2, &second)],
    );
    let response = outbound_fetch(&runtime, "http://first.test/to-second", &options(true))
        .await
        .unwrap();
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(first.hosts(), ["first.test", "second.test"]);
    assert_eq!(second.hosts(), ["first.test", "second.test"]);
    assert_eq!(
        outbound_health(&runtime),
        (vec!["STANDBY", "STANDBY"], LegHealthState::Up)
    );
    assert_eq!(
        requests(&log),
        [
            pair("/to-second", "first.test"),
            pair("/to-second", "first.test"),
            pair("/final", "second.test"),
        ]
    );
    runtime.stop_all().await;
    task.abort();
}

fn pool_of(db: &Database, members: &[u32]) -> crate::proxies::Rung {
    let pool = db
        .create_proxy_pool(&crate::proxies::ProxyPool {
            id: 0,
            name: "pool".into(),
            kind: ProxyKind::Http3Connect,
            member_ids: members.to_vec(),
            enabled: true,
        })
        .unwrap();
    crate::proxies::Rung::Pool { id: pool.id }
}

// No member of a pool is counted as failing to set a connection up, and the
// pool is not raced again.
fn assert_pool_uncharged(runtime: &ProxyRuntime) {
    let pools = runtime.network.pool_status();
    assert_eq!(pools.len(), 1);
    let plan = &pools[0].2;
    assert_eq!(plan.races_failed, 0);
    for candidate in &plan.candidates {
        assert_eq!(candidate.failures, 0, "member {}", candidate.candidate);
        assert_ne!(candidate.state, "SUSPECT", "member {}", candidate.candidate);
    }
}

// A pool rung of three members, then a proxy, every one carrying the
// request to a destination that drops it: the destination is asked once by
// the pool and once by the proxy, not once by each member. Nothing is
// charged.
#[tokio::test]
async fn a_destination_dropping_a_request_is_asked_once_per_rung() {
    let (dropping, dropped, task) = dropping().await;
    let members: Vec<_> = (0..3)
        .map(|_| StandIn::new(Proxying::To(dropping), &[]))
        .collect();
    let next = StandIn::new(Proxying::To(dropping), &[]);
    let db = with_proxies(&[1, 2, 3, 4]);
    let pool = pool_of(&db, &[1, 2, 3]);
    let runtime = standing_in(
        &db,
        vec![ladder(100, vec![pool, proxy(4)])],
        &[
            (1, &members[0]),
            (2, &members[1]),
            (3, &members[2]),
            (4, &next),
        ],
    );
    let error = outbound_fetch(&runtime, "http://dropping.test/start", &options(true))
        .await
        .unwrap_err();
    assert_eq!(error, "TEST request failed after its route connected");
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 2);
    let pool_dials: usize = members.iter().map(|member| member.dials().len()).sum();
    assert_eq!(pool_dials, 1);
    assert_eq!(next.hosts(), ["dropping.test"]);
    assert_pool_uncharged(&runtime);
    assert_eq!(
        outbound_health(&runtime),
        (vec!["STANDBY", "STANDBY"], LegHealthState::Up)
    );
    runtime.stop_all().await;
    task.abort();
}

// The same, with the rung after the pool reaching a destination that
// answers: the fetch is answered after two requests in all.
#[tokio::test]
async fn a_request_dropped_through_a_pool_is_carried_by_the_next_rung() {
    let (dropping, dropped, dropping_task) = dropping().await;
    let (addr, log, task) = origin().await;
    let members: Vec<_> = (0..3)
        .map(|_| StandIn::new(Proxying::To(dropping), &[]))
        .collect();
    let next = StandIn::new(Proxying::To(addr), &[]);
    let db = with_proxies(&[1, 2, 3, 4]);
    let pool = pool_of(&db, &[1, 2, 3]);
    let runtime = standing_in(
        &db,
        vec![ladder(100, vec![pool, proxy(4)])],
        &[
            (1, &members[0]),
            (2, &members[1]),
            (3, &members[2]),
            (4, &next),
        ],
    );
    let response = outbound_fetch(&runtime, "http://origin.test/start", &options(true))
        .await
        .unwrap();
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(requests(&log), [pair("/start", "origin.test")]);
    assert_pool_uncharged(&runtime);
    assert_eq!(
        outbound_health(&runtime),
        (vec!["STANDBY", "STANDBY"], LegHealthState::Up)
    );
    runtime.stop_all().await;
    dropping_task.abort();
    task.abort();
}

const TV: Consumer = Consumer::Torrent(crate::proxies::TorrentScope::Category(1));

fn save_torrent(db: &Database, scope: crate::proxies::TorrentScope, rung: crate::proxies::Rung) {
    db.save_proxy_routing_policy(
        Consumer::Torrent(scope),
        &crate::proxies::RoutingPolicy {
            legs: vec![ladder(100, vec![rung])],
            dns_profile_id: Some(proxy_names(db)),
            ..Default::default()
        },
    )
    .unwrap();
}

// Two torrent scopes share one leg, a pool of two members. A fetch of one
// scope is held mid-dial on whichever member it leased first while that
// scope's route moves to another proxy. The leg's stage lives on, as the
// other scope still runs it, but the moved scope no longer does: the fetch
// dials no other member of it.
async fn a_leg_changed_mid_dial(fetch: impl AsyncFnOnce(&ProxyRuntime) -> Result<String, String>) {
    use crate::proxies::TorrentScope;
    let (addr, log, task) = origin().await;
    let members = [
        StandIn::new(Proxying::Held(addr), &[]),
        StandIn::new(Proxying::Held(addr), &[]),
    ];
    let moved_to = StandIn::new(Proxying::To(addr), &[]);
    let db = with_proxies(&[1, 2, 3]);
    db.insert_category(&crate::categories::CategoryConfig {
        upload_cap: Default::default(),
        id: 1,
        name: "tv".into(),
        dest_dir: None,
        aliases: String::new(),
        seeding: None,
        torrent_route: true,
        unpack: None,
    })
    .unwrap();
    let pool = pool_of(&db, &[1, 2]);
    save_torrent(&db, TorrentScope::Default, pool.clone());
    save_torrent(&db, TorrentScope::Category(1), pool);
    let runtime = ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    for (id, stand_in) in [(1, &members[0]), (2, &members[1]), (3, &moved_to)] {
        runtime
            .network
            .install_fixture(id, stand_in.clone() as Arc<dyn TunnelProvider>);
    }
    // The default scope's route is live, so the category's takes up its leg.
    runtime
        .network
        .route(
            Consumer::Torrent(TorrentScope::Default),
            1,
            Duration::from_secs(30),
        )
        .unwrap();
    let change = async {
        let first = tokio::select! {
            () = members[0].reached.notified() => 0,
            () = members[1].reached.notified() => 1,
        };
        save_torrent(&db, TorrentScope::Category(1), proxy(3));
        runtime.reload().await.unwrap();
        // Both are let go, so a dial of the other member would not hang.
        members[0].release.notify_one();
        members[1].release.notify_one();
        first
    };
    let (result, first) = tokio::join!(fetch(&runtime), change);
    assert_eq!(
        result.unwrap_err(),
        "TEST request failed on all permitted routes"
    );
    assert_eq!(members[first].hosts(), ["origin.test"]);
    assert!(members[1 - first].dials().is_empty());
    assert!(moved_to.dials().is_empty());
    assert!(requests(&log).is_empty());
    runtime.stop_all().await;
    task.abort();
}

#[tokio::test]
async fn a_fetch_dials_no_more_of_a_leg_changed_under_it() {
    a_leg_changed_mid_dial(async |runtime| {
        let url: reqwest::Url = "http://origin.test/start".parse().unwrap();
        let response =
            routed_fetch(runtime, TV, &url, &options(true), |request, _, _| request).await?;
        Ok(String::from_utf8(read_body(response, 1024).await?).unwrap())
    })
    .await;
}

#[tokio::test]
async fn a_routed_client_dials_no_more_of_a_leg_changed_under_it() {
    a_leg_changed_mid_dial(async |runtime| {
        with_routed_client(
            runtime,
            TV,
            "TEST",
            reqwest::Client::builder,
            |client| async move {
                client
                    .get("http://origin.test/start")
                    .send()
                    .await?
                    .text()
                    .await
            },
        )
        .await?
        .map_err(|error| error.to_string())
    })
    .await;
}

// What the walk makes of an attempt's own time running out: the
// destination's once the attempt's dial went through, so nothing is
// charged and the next attempt is tried; the path's, as a timeout, while it
// had not.
#[test]
fn running_out_of_time_is_the_paths_until_its_dial_went_through() {
    let timed_out = || {
        Err(AttemptError::TimedOut {
            destination: "http://stalled.test:80".into(),
        })
    };
    let verdict = judge(timed_out(), None, true, "TEST");
    assert!(verdict.end.is_none());
    assert!(
        matches!(&verdict.report, Report::Carried(error) if error == "TEST request timed out after its route connected"),
        "{:?}",
        verdict.report
    );
    let verdict = judge(timed_out(), None, false, "TEST");
    assert!(verdict.end.is_none());
    assert!(
        matches!(&verdict.report, Report::Failed(error) if matches!(error.as_ref(), DialError::Timeout { .. })),
        "{:?}",
        verdict.report
    );
}

// A request that fails once it was handed over is the destination's only
// when the attempt's dial went through the path. One whose dial never did,
// as when the bridge's own dial runs out of time, is the path's.
#[test]
fn a_request_sent_without_a_dial_through_is_the_paths() {
    let sent = || {
        Err(AttemptError::Sent {
            destination: "http://dropping.test:80".into(),
            cause: "connection reset".into(),
        })
    };
    let verdict = judge(sent(), None, false, "TEST");
    assert!(verdict.end.is_none());
    assert!(
        matches!(&verdict.report, Report::Failed(error) if error.is_path_evidence()),
        "{:?}",
        verdict.report
    );
    let verdict = judge(sent(), None, true, "TEST");
    assert!(verdict.end.is_none());
    assert!(
        matches!(&verdict.report, Report::Carried(error) if error == "TEST request failed after its route connected"),
        "{:?}",
        verdict.report
    );
}

// Holds a paused clock still while a loopback read or write is in flight:
// tokio does not move it on its own while a blocking task runs, so time
// moves only when the test advances it. Dropping the hold ends the task.
struct ClockHold(#[allow(dead_code)] std::sync::mpsc::Sender<()>);

fn hold_the_clock() -> ClockHold {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || {
        let _ = rx.recv();
    });
    ClockHold(tx)
}

// Once `stall` has read a request, the attempt's time runs out.
async fn run_out_once_asked(stall: &Stall, options: &FetchOptions) {
    stall.heard.notified().await;
    tokio::time::advance(options.timeout).await;
}

// The proxy carries the request to a destination that never answers: the
// attempt's time runs out, the fetch goes on to the next rung, which
// carries it, and neither rung is charged.
#[tokio::test(start_paused = true)]
async fn a_destination_that_never_answers_on_one_rung_is_carried_by_the_next() {
    let clock = hold_the_clock();
    let (addr, log, task) = origin().await;
    let first = StandIn::new(Proxying::Stalls, &[]);
    let second = StandIn::new(Proxying::To(addr), &[]);
    let db = with_proxies(&[1, 2]);
    let runtime = standing_in(
        &db,
        vec![ladder(100, vec![proxy(1), proxy(2)])],
        &[(1, &first), (2, &second)],
    );
    let options = options(true);
    let (response, ()) = tokio::join!(
        outbound_fetch(&runtime, "http://origin.test/start", &options),
        run_out_once_asked(&first.stall, &options),
    );
    assert_eq!(
        read_body(response.unwrap(), 1024).await.unwrap(),
        b"fetched"
    );
    assert_eq!(first.stall.asked(), 1);
    assert_eq!(first.hosts(), ["origin.test"]);
    assert_eq!(second.hosts(), ["origin.test"]);
    assert_eq!(
        outbound_health(&runtime),
        (vec!["STANDBY", "STANDBY"], LegHealthState::Up)
    );
    assert_eq!(requests(&log), [pair("/start", "origin.test")]);
    runtime.stop_all().await;
    task.abort();
    drop(clock);
}

// One rung, and a destination that never answers: that request times out
// as the destination's, the rung stays on standby and the leg up, and the
// next fetch, to another host, dials at once and is answered.
#[tokio::test(start_paused = true)]
async fn a_destination_that_never_answers_fails_that_request_only() {
    let clock = hold_the_clock();
    let (addr, log, task) = origin().await;
    let only = StandIn::new(Proxying::To(addr), &[("stalled.test", Proxying::Stalls)]);
    let db = with_proxies(&[1]);
    let runtime = standing_in(&db, vec![ladder(100, vec![proxy(1)])], &[(1, &only)]);
    let options = options(true);
    let (result, ()) = tokio::join!(
        outbound_fetch(&runtime, "http://stalled.test/start", &options),
        run_out_once_asked(&only.stall, &options),
    );
    assert_eq!(
        result.unwrap_err(),
        "TEST request timed out after its route connected"
    );
    assert_eq!(
        outbound_health(&runtime),
        (vec!["STANDBY"], LegHealthState::Up)
    );
    let response = outbound_fetch(&runtime, "http://origin.test/start", &options)
        .await
        .unwrap();
    assert_eq!(read_body(response, 1024).await.unwrap(), b"fetched");
    assert_eq!(only.stall.asked(), 1);
    assert_eq!(only.hosts(), ["stalled.test", "origin.test"]);
    assert_eq!(requests(&log), [pair("/start", "origin.test")]);
    runtime.stop_all().await;
    task.abort();
    drop(clock);
}

// A pool rung of three members before a destination that never answers:
// the destination is asked once on that rung, not once by each member, and
// no member is charged.
#[tokio::test(start_paused = true)]
async fn a_destination_that_never_answers_is_asked_once_per_rung() {
    let clock = hold_the_clock();
    let stall = Arc::<Stall>::default();
    let members: Vec<_> = (0..3)
        .map(|_| StandIn::stalling(Proxying::Stalls, &[], &stall))
        .collect();
    let db = with_proxies(&[1, 2, 3]);
    let pool = pool_of(&db, &[1, 2, 3]);
    let runtime = standing_in(
        &db,
        vec![ladder(100, vec![pool])],
        &[(1, &members[0]), (2, &members[1]), (3, &members[2])],
    );
    let options = options(true);
    let (result, ()) = tokio::join!(
        outbound_fetch(&runtime, "http://stalled.test/start", &options),
        run_out_once_asked(&stall, &options),
    );
    assert_eq!(
        result.unwrap_err(),
        "TEST request timed out after its route connected"
    );
    assert_eq!(stall.asked(), 1);
    let dials: usize = members.iter().map(|member| member.dials().len()).sum();
    assert_eq!(dials, 1);
    assert_pool_uncharged(&runtime);
    assert_eq!(
        outbound_health(&runtime),
        (vec!["STANDBY"], LegHealthState::Up)
    );
    runtime.stop_all().await;
    drop(clock);
}

// A dial the proxy never completes: the attempt's time runs out before the
// path carried anything, so the rung cools as the path's.
#[tokio::test(start_paused = true)]
async fn a_dial_that_never_completes_cools_its_rung() {
    let clock = hold_the_clock();
    let (addr, _log, task) = origin().await;
    let held = StandIn::new(Proxying::Held(addr), &[]);
    let db = with_proxies(&[1]);
    let runtime = standing_in(&db, vec![ladder(100, vec![proxy(1)])], &[(1, &held)]);
    let options = options(true);
    let (result, ()) = tokio::join!(
        outbound_fetch(&runtime, "http://origin.test/start", &options),
        async {
            held.reached.notified().await;
            tokio::time::advance(options.timeout).await;
        },
    );
    assert_eq!(
        result.unwrap_err(),
        "TEST request failed on all permitted routes"
    );
    assert_eq!(outbound_health(&runtime).0, ["COOLDOWN"]);
    runtime.stop_all().await;
    task.abort();
    drop(clock);
}

// Every WARN event on this thread, as its fields.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for Warnings {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        use std::fmt::Write;
        struct Line(String);
        impl tracing::field::Visit for Line {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let _ = write!(self.0, " {field}={value:?}");
            }
        }
        let mut line = Line(String::new());
        event.record(&mut line);
        self.0.lock().unwrap().push(line.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

// A request reset after its route connected is logged and returned by
// scheme, host and port only: no credentials, path or query.
#[tokio::test]
async fn a_reset_request_names_no_secret() {
    let warnings = Warnings::default();
    let _capture = tracing::subscriber::set_default(warnings.clone());
    let only = StandIn::new(Proxying::Resets, &[]);
    let db = with_proxies(&[1]);
    let runtime = standing_in(&db, vec![ladder(100, vec![proxy(1)])], &[(1, &only)]);
    let error = outbound_fetch(
        &runtime,
        "http://u:p@dropping.test/announce?passkey=SECRET",
        &options(true),
    )
    .await
    .unwrap_err();
    let heard = warnings.0.lock().unwrap().clone();
    let line = heard
        .iter()
        .find(|line| line.contains("request failed after its route connected"))
        .unwrap();
    assert!(line.contains("http://dropping.test:80"), "{line}");
    for said in [line, &error] {
        for secret in ["u:p", "/announce", "passkey", "SECRET"] {
            assert!(!said.contains(secret), "{secret} in {said}");
        }
    }
    runtime.stop_all().await;
}

// A destination whose certificate is not trusted, reached through a proxy,
// is refused as the destination's, whichever path reaches it: the walk ends
// there, and the next rung is never dialed.
#[tokio::test]
async fn tls_verification_failed_through_a_proxy() {
    let first = StandIn::new(Proxying::Untrusted, &[]);
    let second = StandIn::new(Proxying::Untrusted, &[]);
    let db = with_proxies(&[1, 2]);
    let runtime = standing_in(
        &db,
        vec![ladder(100, vec![proxy(1), proxy(2)])],
        &[(1, &first), (2, &second)],
    );
    let error = outbound_fetch(&runtime, "https://origin.test/start", &options(true))
        .await
        .unwrap_err();
    assert_eq!(error, "TEST TLS verification failed");
    assert_eq!(first.hosts(), ["origin.test"]);
    assert!(second.dials().is_empty());
    assert_eq!(
        outbound_health(&runtime),
        (vec!["STANDBY", "STANDBY"], LegHealthState::Up)
    );
    runtime.stop_all().await;
}

// A TLS handshake the destination ends with an alert, not a certificate
// refusal, after the proxy carried the dial: the fetch goes on to the next
// rung, whose destination refuses its certificate, and neither rung is
// charged.
#[tokio::test]
async fn a_tls_failure_that_is_not_the_certificates_goes_to_the_next_rung() {
    let first = StandIn::new(Proxying::Alerts, &[]);
    let second = StandIn::new(Proxying::Untrusted, &[]);
    let db = with_proxies(&[1, 2]);
    let runtime = standing_in(
        &db,
        vec![ladder(100, vec![proxy(1), proxy(2)])],
        &[(1, &first), (2, &second)],
    );
    let error = outbound_fetch(&runtime, "https://origin.test/start", &options(true))
        .await
        .unwrap_err();
    assert_eq!(error, "TEST TLS verification failed");
    assert_eq!(first.dials(), [("origin.test".to_owned(), 443)]);
    assert_eq!(second.dials(), [("origin.test".to_owned(), 443)]);
    assert_eq!(
        outbound_health(&runtime),
        (vec!["STANDBY", "STANDBY"], LegHealthState::Up)
    );
    runtime.stop_all().await;
}

// A rung whose lookup through the proxy fails is the path's: it cools, and
// the destination is never dialed. The route's names are asked of a listed
// server through the proxy, whose dial to it the proxy fails.
#[tokio::test]
async fn a_rung_whose_routed_lookup_fails_cools() {
    let only = StandIn::new(Proxying::Resets, &[("192.0.2.53", Proxying::Down)]);
    let db = with_proxies(&[1]);
    let dns = route_dns(&db, Some("192.0.2.53:53".parse().unwrap()));
    let runtime = standing_in_on(&db, dns, vec![ladder(100, vec![proxy(1)])], &[(1, &only)]);
    let error = outbound_fetch(&runtime, "http://origin.test/start", &options(true))
        .await
        .unwrap_err();
    assert_eq!(error, "TEST request failed on all permitted routes");
    assert_eq!(outbound_health(&runtime).0, ["COOLDOWN"]);
    assert!(only.hosts().iter().all(|host| host == "192.0.2.53"));
    runtime.stop_all().await;
}

// A client a library drives over `path`: it follows redirects itself.
fn client_on(path: &Arc<Mapped>, allow_private_network: bool) -> (reqwest::Client, Arc<Bridge>) {
    let checked = Arc::new(Checked {
        path: path.clone(),
        label: "TEST",
        allow_private_network,
        failure: Default::default(),
    });
    let bridge = bridge_onto(checked, Duration::from_secs(30)).unwrap();
    let client = through_bridge(
        reqwest::Client::builder().redirect(reqwest::redirect::Policy::limited(3)),
        &bridge,
    )
    .unwrap()
    .build()
    .unwrap();
    (client, bridge)
}

#[tokio::test]
async fn a_client_following_redirects_has_each_name_checked_on_its_path() {
    let (addr, log, task) = origin().await;
    let path = Mapped::new(
        addr,
        Some(&[
            ("first.test", [203, 0, 113, 7]),
            ("second.test", [203, 0, 113, 8]),
        ]),
    );
    let (client, _bridge) = client_on(&path, false);
    let response = client
        .get("http://first.test/to-second")
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "fetched");
    assert_eq!(
        path.dials(),
        [
            (
                "first.test".to_owned(),
                vec![IpAddr::from([203, 0, 113, 7])]
            ),
            (
                "second.test".to_owned(),
                vec![IpAddr::from([203, 0, 113, 8])]
            ),
        ]
    );
    assert_eq!(
        requests(&log),
        [
            pair("/to-second", "first.test"),
            pair("/final", "second.test")
        ]
    );
    task.abort();
}

#[tokio::test]
async fn a_client_redirected_to_a_private_address_never_dials_it() {
    let (addr, log, task) = origin().await;
    let path = Mapped::new(addr, None);
    let (client, _bridge) = client_on(&path, false);
    assert!(
        client
            .get("http://first.test/to-private")
            .send()
            .await
            .is_err()
    );
    assert_eq!(path.dials(), [("first.test".to_owned(), Vec::new())]);
    assert_eq!(requests(&log), [pair("/to-private", "first.test")]);
    task.abort();
}

#[tokio::test]
async fn a_routed_client_goes_direct_when_no_route_is_saved() {
    let (addr, log, task) = origin().await;
    let runtime = ProxyRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let body = with_routed_client(
        &runtime,
        Consumer::Outbound,
        "TEST",
        reqwest::Client::builder,
        |client| async move {
            client
                .get(format!("http://{addr}/start"))
                .send()
                .await?
                .text()
                .await
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(body, "fetched");
    assert_eq!(requests(&log), [pair("/start", &addr.to_string())]);
    runtime.stop_all().await;
    task.abort();
}

#[tokio::test]
async fn a_routed_client_fails_closed_when_its_proxies_fail() {
    let (addr, log, task) = origin().await;
    let proxy = weaver_tunnel::test_support::proxy::ProxyServerDouble::start(
        weaver_tunnel::transport::TransportKind::Socks5,
        None,
        HashMap::new(),
    )
    .await;
    let runtime = outbound_only_through(ProxyKind::Socks5, proxy.addr.port());
    let error = with_routed_client(
        &runtime,
        Consumer::Outbound,
        "TEST",
        reqwest::Client::builder,
        |client| async move {
            client
                .get(format!("http://localhost:{}/start", addr.port()))
                .send()
                .await
                .map(drop)
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error, "TEST request failed on all permitted routes");
    assert_eq!(
        proxy.targets.lock().unwrap().clone(),
        [("localhost".to_owned(), addr.port())]
    );
    assert!(requests(&log).is_empty());
    runtime.stop_all().await;
    task.abort();
}
