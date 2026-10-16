// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::Database;
use crate::proxies::{ProxyKind, outbound_only_through};
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
