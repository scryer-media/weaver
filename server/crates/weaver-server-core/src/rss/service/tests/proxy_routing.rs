use super::*;
use crate::proxies::{
    Consumer, ProxyKind, ProxyProfile, ProxyRuntime, ProxySecrets, RouteState, RoutingPolicy,
};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
};
use weaver_tunnel::{
    test_support::{
        SshServerDouble, SshServerOptions, TEST_PEER_ADDRESS, WireGuardTestPeer,
        WireGuardTestPeerOptions, dns::DnsServerDouble, proxy::ProxyServerDouble,
    },
    transport::TransportKind,
};

const PUBLIC: Ipv4Addr = Ipv4Addr::new(93, 184, 216, 34);
const DNS: &str = "192.0.2.53";
type ObservedTargets = Arc<StdMutex<Vec<(String, u16)>>>;
pub(super) struct Fixture {
    _proxy: Option<ProxyServerDouble>,
    _ssh: Option<SshServerDouble>,
    _wg: Option<WireGuardTestPeer>,
    dns: DnsServerDouble,
    profile: ProxyProfile,
    targets: Option<ObservedTargets>,
}
impl Fixture {
    pub(super) async fn start(kind: ProxyKind, destination: SocketAddr) -> Self {
        let dns = DnsServerDouble::start(HashMap::from([
            ("feed.invalid".into(), PUBLIC),
            ("download.invalid".into(), PUBLIC),
        ]))
        .await;
        let mut p = ProxyProfile {
            id: 1,
            name: "RSS proxy".into(),
            kind,
            enabled: true,
            host: "127.0.0.1".into(),
            port: 1,
            dns_servers: vec![DNS.parse().unwrap()],
            tunnel_addresses: vec![],
            peer_public_key: None,
            mtu: 1280,
            keepalive_seconds: None,
            timeout_seconds: 3,
            host_key_fingerprint: None,
            revision: 1,
            secrets: ProxySecrets::default(),
        };
        let mapping = HashMap::from([
            ((DNS.into(), 53), dns.addr),
            ((PUBLIC.to_string(), 80), destination),
        ]);
        let mut standard = None;
        let mut ssh = None;
        let mut wg = None;
        let mut targets = None;
        match kind {
            ProxyKind::HttpConnect | ProxyKind::Socks5 => {
                let proxy = ProxyServerDouble::start(
                    if kind == ProxyKind::HttpConnect {
                        TransportKind::HttpConnect
                    } else {
                        TransportKind::Socks5
                    },
                    None,
                    mapping,
                )
                .await;
                p.port = proxy.addr.port();
                targets = Some(proxy.targets.clone());
                standard = Some(proxy);
            }
            ProxyKind::Ssh => {
                let proxy =
                    SshServerDouble::start_with_destinations(SshServerOptions::default(), mapping)
                        .await;
                p.port = proxy.port();
                p.secrets.username = Some("operator".into());
                p.secrets.private_key =
                    Some(weaver_tunnel::test_support::CLIENT_ED25519_PEM.into());
                ssh = Some(proxy);
            }
            ProxyKind::WireGuard => {
                let proxy = WireGuardTestPeer::start_for_downloads(
                    WireGuardTestPeerOptions {
                        http_port: 80,
                        names: HashMap::from([
                            ("feed.invalid".into(), vec![TEST_PEER_ADDRESS.into()]),
                            ("download.invalid".into(), vec![TEST_PEER_ADDRESS.into()]),
                        ]),
                        ..Default::default()
                    },
                    Some(destination),
                    std::time::Duration::ZERO,
                    true,
                )
                .await;
                let spec = proxy.client_spec("rss");
                use base64::Engine;
                p.port = proxy.endpoint().port();
                p.dns_servers = spec.dns_servers;
                p.tunnel_addresses = vec!["10.63.0.2/32".into()];
                p.peer_public_key =
                    Some(base64::engine::general_purpose::STANDARD.encode(spec.peer_public_key));
                p.secrets.private_key =
                    Some(base64::engine::general_purpose::STANDARD.encode(spec.private_key));
                wg = Some(proxy);
            }
        }
        Self {
            _proxy: standard,
            _ssh: ssh,
            _wg: wg,
            dns,
            profile: p,
            targets,
        }
    }
}

async fn origin() -> (
    SocketAddr,
    Arc<StdMutex<Vec<(String, bool, String)>>>,
    tokio::task::JoinHandle<()>,
) {
    let captured = Arc::new(StdMutex::new(Vec::new()));
    let log = captured.clone();
    let app = Router::new().fallback(get(move |uri: axum::http::Uri, headers: HeaderMap| {
        let log = log.clone();
        async move {
            let host = headers
                .get(header::HOST)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            log.lock().unwrap().push((
                uri.path().to_string(),
                headers.contains_key(header::AUTHORIZATION),
                host,
            ));
            match uri.path() {
                "/redirect" => Redirect::temporary("http://download.invalid/final").into_response(),
                "/feed" => (
                    [(header::CONTENT_TYPE, "application/rss+xml")],
                    sample_rss_feed("routed-guid", "Silver Horizon", "/download.nzb")
                        .replace("http://localhost", "http://download.invalid"),
                )
                    .into_response(),
                "/download.nzb" => sample_nzb_bytes().into_response(),
                "/failure" => (StatusCode::BAD_GATEWAY, "upstream response").into_response(),
                _ => "through selected route".into_response(),
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, captured, task)
}
pub(super) fn service(
    temp: &TempDir,
    fixture: &Fixture,
    url: &str,
    allow_private: bool,
) -> (
    RssService,
    Arc<ProxyRuntime>,
    RssFeedRow,
    Arc<StdMutex<Vec<CapturedSubmission>>>,
) {
    let db = Database::open_in_memory().unwrap();
    db.save_proxy_profile(&fixture.profile).unwrap();
    let feed = basic_auth_feed(url.into());
    db.insert_rss_feed_with_routing(
        &feed,
        Some(&RoutingPolicy {
            proxy_ids: vec![1],
            allow_direct: false,
        }),
    )
    .unwrap();
    let runtime = ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let submissions = Arc::new(StdMutex::new(Vec::new()));
    let mut security = RuntimeSecurityConfig::default();
    security.rss_allow_private_network = allow_private;
    let service = build_service_with_security(temp.path(), db, submissions.clone(), security);
    service.inner.handle.set_proxy_runtime(runtime.clone());
    (service, runtime, feed, submissions)
}

async fn routed_http(kind: ProxyKind) {
    let temp = TempDir::new().unwrap();
    let (addr, captured, task) = origin().await;
    let fixture = Fixture::start(kind, addr).await;
    let (service, runtime, feed, _) = service(
        &temp,
        &fixture,
        "http://feed.invalid/redirect",
        kind == ProxyKind::WireGuard,
    );
    let response = service
        .send_rss_request(&feed, &feed.url, false)
        .await
        .unwrap();
    assert_eq!(
        read_response_with_limit(response, 1024).await.unwrap(),
        b"through selected route"
    );
    let requests = captured.lock().unwrap().clone();
    assert_eq!(
        requests,
        vec![
            ("/redirect".into(), true, "feed.invalid".into()),
            ("/final".into(), false, "download.invalid".into())
        ]
    );
    assert_eq!(
        runtime
            .route(Consumer::Rss(1), Duration::from_secs(30))
            .unwrap()
            .status()
            .selected_proxy_id,
        Some(1)
    );
    if let Some(targets) = &fixture.targets {
        let targets = targets.lock().unwrap();
        assert!(
            targets
                .iter()
                .any(|(host, port)| host == DNS && *port == 53)
        );
        assert!(
            targets
                .iter()
                .any(|(host, port)| host == &PUBLIC.to_string() && *port == 80)
        );
        assert!(
            !targets.iter().any(|(host, _)| host.ends_with(".invalid")),
            "proxy must connect validated IPs, without a second hostname lookup"
        );
    }
    if let Some(ssh) = &fixture._ssh {
        assert!(
            ssh.forwarded_targets()
                .iter()
                .all(|(host, _)| !host.ends_with(".invalid"))
        );
    }
    runtime.stop_all().await;
    task.abort();
}
#[tokio::test]
async fn http_connect_rss_uses_routed_dns_and_validated_addresses() {
    routed_http(ProxyKind::HttpConnect).await;
}
#[tokio::test]
async fn socks5_rss_uses_routed_dns_and_validated_addresses() {
    routed_http(ProxyKind::Socks5).await;
}
#[tokio::test]
async fn ssh_rss_uses_routed_dns_and_validated_addresses() {
    routed_http(ProxyKind::Ssh).await;
}
#[tokio::test]
async fn wireguard_rss_uses_tunnel_dns_and_preserves_host() {
    routed_http(ProxyKind::WireGuard).await;
}

#[tokio::test]
async fn redirect_rebinding_to_private_address_is_rejected_before_connection() {
    let temp = TempDir::new().unwrap();
    let (addr, captured, task) = origin().await;
    let fixture = Fixture::start(ProxyKind::Socks5, addr).await;
    fixture
        .dns
        .names
        .lock()
        .unwrap()
        .insert("download.invalid".into(), Ipv4Addr::LOCALHOST);
    let (service, runtime, feed, _) =
        service(&temp, &fixture, "http://feed.invalid/redirect", false);
    assert!(
        service
            .send_rss_request(&feed, &feed.url, false)
            .await
            .is_err()
    );
    assert_eq!(captured.lock().unwrap().len(), 1);
    assert!(
        !fixture
            .targets
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .iter()
            .any(|(host, _)| host == "127.0.0.1")
    );
    runtime.stop_all().await;
    task.abort();
}

#[tokio::test]
async fn http_status_and_feed_parsing_errors_do_not_rotate_routes() {
    let temp = TempDir::new().unwrap();
    let (addr, _, task) = origin().await;
    let fixture = Fixture::start(ProxyKind::HttpConnect, addr).await;
    let (service, runtime, feed, _) =
        service(&temp, &fixture, "http://feed.invalid/failure", false);
    let response = service
        .send_rss_request(&feed, &feed.url, false)
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    assert!(
        runtime
            .route(Consumer::Rss(1), Duration::from_secs(30))
            .unwrap()
            .status()
            .failures
            .is_empty()
    );
    runtime.stop_all().await;
    task.abort();
}

#[tokio::test]
async fn automatic_and_manual_nzb_downloads_inherit_feed_routing_without_duplicates() {
    let temp = TempDir::new().unwrap();
    let (addr, captured, task) = origin().await;
    let fixture = Fixture::start(ProxyKind::Socks5, addr).await;
    let (service, runtime, feed, submissions) =
        service(&temp, &fixture, "http://feed.invalid/feed", false);
    service
        .inner
        .db
        .insert_rss_rule(&RssRuleRow {
            id: 1,
            feed_id: 1,
            sort_order: 0,
            enabled: true,
            action: RssRuleAction::Accept,
            title_regex: None,
            item_categories: vec![],
            min_size_bytes: None,
            max_size_bytes: None,
            category_override: None,
            metadata: vec![],
        })
        .unwrap();
    let report = service.run_feed_sync(feed.id).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.items_submitted, 1);
    let again = service.run_feed_sync(feed.id).await.unwrap();
    assert_eq!(again.items_submitted, 0);
    assert_eq!(submissions.lock().unwrap().len(), 1);
    assert!(
        captured
            .lock()
            .unwrap()
            .iter()
            .any(|(path, auth, host)| path == "/download.nzb"
                && !auth
                && host == "download.invalid")
    );
    let direct_download = service
        .send_rss_request(&feed, "http://download.invalid/download.nzb", false)
        .await
        .unwrap();
    assert_eq!(
        read_response_with_limit(direct_download, 1024 * 1024)
            .await
            .unwrap(),
        sample_nzb_bytes()
    );
    runtime.stop_all().await;
    task.abort();
}

#[tokio::test]
async fn blocked_feed_never_contacts_live_host_destination() {
    let temp = TempDir::new().unwrap();
    let (addr, captured, task) = origin().await;
    let fixture = Fixture::start(ProxyKind::Socks5, addr).await;
    let (service, runtime, feed, _) =
        service(&temp, &fixture, &format!("http://{addr}/feed"), true);
    service
        .inner
        .db
        .save_proxy_routing_policy(
            Consumer::Rss(1),
            &RoutingPolicy {
                proxy_ids: vec![],
                allow_direct: false,
            },
        )
        .unwrap();
    runtime.reload().await.unwrap();
    assert!(
        service
            .send_rss_request(&feed, &feed.url, false)
            .await
            .is_err()
    );
    assert!(captured.lock().unwrap().is_empty());
    assert!(fixture.dns.queries.lock().unwrap().is_empty());
    assert_eq!(
        runtime
            .route(Consumer::Rss(1), Duration::from_secs(30))
            .unwrap()
            .status()
            .state,
        RouteState::Blocked
    );
    runtime.stop_all().await;
    task.abort();
}
