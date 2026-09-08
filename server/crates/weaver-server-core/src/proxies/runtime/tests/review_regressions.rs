use super::*;
use tokio::io::{AsyncBufReadExt, BufReader};
use weaver_nntp::{NntpClient, NntpConnection, ServerConfig, ServerId, client::NntpClientConfig};

#[derive(Clone, Copy)]
enum Mode {
    Healthy,
    Stall,
    AuthRejected,
    Truncated,
    NoResponse,
}
struct NntpFixture {
    mode: Mode,
}
#[async_trait::async_trait]
impl TunnelProvider for NntpFixture {
    async fn dial(&self, _: &str, _: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        if matches!(self.mode, Mode::Stall) {
            return std::future::pending().await;
        }
        let (client, peer) = tokio::io::duplex(4096);
        let mode = self.mode;
        tokio::spawn(async move {
            let mut peer = BufReader::new(peer);
            peer.get_mut()
                .write_all(b"200 fixture ready\r\n")
                .await
                .unwrap();
            loop {
                let mut command = String::new();
                if peer.read_line(&mut command).await.unwrap_or(0) == 0 {
                    break;
                }
                let (reply, close): (&[u8], bool) = if command.starts_with("AUTHINFO") {
                    if matches!(mode, Mode::AuthRejected) {
                        (b"481 authentication rejected\r\n", true)
                    } else if command.starts_with("AUTHINFO USER") {
                        (b"381 password required\r\n", false)
                    } else {
                        (b"281 authenticated\r\n", false)
                    }
                } else if command.starts_with("CAPABILITIES") {
                    (b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n", false)
                } else if command.starts_with("GROUP") {
                    (b"211 1 1 1 fixture\r\n", false)
                } else if command.starts_with("BODY") {
                    if matches!(mode, Mode::Truncated) {
                        (b"222 1 <fixture> body\r\npartial", true)
                    } else {
                        (b"222 1 <fixture> body\r\npayload\r\n.\r\n", false)
                    }
                } else if command.starts_with("HEAD") {
                    (
                        b"221 1 <fixture> headers\r\nSubject: fixture\r\n.\r\n",
                        false,
                    )
                } else if command.starts_with("ARTICLE") {
                    (
                        b"220 1 <fixture> article\r\nSubject: fixture\r\n\r\npayload\r\n.\r\n",
                        false,
                    )
                } else if command.starts_with("STAT") {
                    if matches!(mode, Mode::NoResponse) {
                        (b"", true)
                    } else {
                        (b"223 1 <fixture> exists\r\n", false)
                    }
                } else if command.starts_with("QUIT") {
                    (b"205 goodbye\r\n", true)
                } else {
                    (b"200 ok\r\n", false)
                };
                if peer.get_mut().write_all(reply).await.is_err() || close {
                    break;
                }
            }
        });
        Ok(Box::new(client))
    }
    fn describe(&self) -> String {
        "NNTP review fixture".into()
    }
}
fn route(modes: &[Mode], timeout: Duration) -> Arc<ConsumerRoute> {
    let runtime = ProxyRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    for (index, mode) in modes.iter().enumerate() {
        let id = index as u32 + 1;
        runtime.profiles.write().unwrap().insert(
            id,
            Arc::new(ProxyHop {
                profile: profile(id),
                provider: Arc::new(NntpFixture { mode: *mode }),
                wireguard: None,
            }),
        );
    }
    runtime
        .draft_route(
            RoutingPolicy {
                proxy_ids: (1..=modes.len() as u32).collect(),
                allow_direct: false,
            },
            timeout,
        )
        .unwrap()
}
fn config(route: &Arc<ConsumerRoute>) -> ServerConfig {
    ServerConfig {
        host: "nntp-review.invalid".into(),
        port: 119,
        tls: false,
        proxy: Some(route.bridge().unwrap()),
        connect_timeout: Duration::from_secs(2),
        command_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

#[tokio::test]
async fn quit_and_auth_rejection_do_not_cool_a_healthy_route() {
    for mode in [Mode::Healthy, Mode::AuthRejected] {
        let route = route(&[mode], Duration::from_secs(1));
        let mut cfg = config(&route);
        if matches!(mode, Mode::AuthRejected) {
            cfg.username = Some("fixture".into());
            cfg.password = Some("fixture".into());
        }
        match NntpConnection::connect(&cfg).await {
            Ok(mut connection) => connection.quit().await.unwrap(),
            Err(error) => assert!(matches!(mode, Mode::AuthRejected), "{error}"),
        }
        tokio::task::yield_now().await;
        assert!(
            route.begin(1).is_some(),
            "expected NNTP closure must not cool its route"
        );
        assert!(route.status().failures.is_empty());
        route.revoke().await;
    }
}

#[tokio::test]
async fn unexpected_nntp_eof_cools_only_the_connection_route_and_retries() {
    let route = route(&[Mode::Truncated, Mode::Healthy], Duration::from_secs(1));
    let mut cfg = NntpClientConfig::single(config(&route), 1);
    cfg.max_retries_per_server = 1;
    let client = NntpClient::new(cfg);
    let body = client.fetch_body("fixture").await.unwrap();
    assert_eq!(body.as_ref(), b"payload\r\n");
    assert!(route.begin(1).is_none());
    assert_eq!(route.status().selected_proxy_id, Some(2));
    client.shutdown().await;
    route.revoke().await;
}

#[tokio::test]
async fn fallback_establishment_preserves_article_and_lane_soft_budgets() {
    // Primary establishment exceeds the article budget. Each operation starts
    // with a fresh route so a previous request cannot hide the timeout gap.
    for operation in 0..6 {
        let route = route(&[Mode::Stall, Mode::Healthy], Duration::from_millis(150));
        let mut cfg = NntpClientConfig::single(config(&route), 1);
        cfg.soft_timeout = Duration::from_millis(50);
        cfg.max_retries_per_server = 0;
        let client = NntpClient::new(cfg);
        match operation {
            0 => {
                client
                    .fetch_body_with_groups("fixture", &["fixture.group".into()])
                    .await
                    .unwrap();
            }
            1 => {
                client.fetch_head("fixture").await.unwrap();
            }
            2 => {
                client.fetch_article("fixture").await.unwrap();
            }
            3 => {
                assert_eq!(client.stat_many(&["fixture"]).await.unwrap(), vec![true]);
            }
            4 => {
                client.acquire_body_lane(ServerId(0), &[]).await.unwrap();
            }
            _ => {
                client
                    .acquire_extra_body_lane(ServerId(0), &[])
                    .await
                    .unwrap();
            }
        }
        assert_eq!(route.status().selected_proxy_id, Some(2));
        client.shutdown().await;
        route.revoke().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_nntp_reports_unexpected_eof_but_not_quit_or_auth_rejection() {
    for mode in [Mode::Healthy, Mode::AuthRejected, Mode::NoResponse] {
        let route = route(&[mode], Duration::from_secs(1));
        let mut cfg = config(&route);
        if matches!(mode, Mode::AuthRejected) {
            cfg.username = Some("fixture".into());
            cfg.password = Some("fixture".into());
        }
        tokio::task::spawn_blocking(move || {
            match weaver_nntp::BlockingNntpConnection::connect_with_ip_policy(&cfg, &[], 0) {
                Ok(mut connection) => {
                    if matches!(mode, Mode::NoResponse) {
                        assert!(
                            connection
                                .send_command(&weaver_nntp::commands::Command::Stat(
                                    weaver_nntp::ArticleId::MessageId("fixture".into()),
                                ))
                                .is_err()
                        );
                    } else {
                        connection.quit().unwrap();
                    }
                }
                Err(error) => assert!(matches!(mode, Mode::AuthRejected), "{error}"),
            }
        })
        .await
        .unwrap();
        assert_eq!(route.begin(1).is_none(), matches!(mode, Mode::NoResponse));
        route.revoke().await;
    }
}

#[tokio::test]
async fn proxy_budget_does_not_extend_waiting_for_pool_capacity() {
    let route = route(&[Mode::Healthy], Duration::from_secs(1));
    let mut cfg = NntpClientConfig::single(config(&route), 1);
    cfg.soft_timeout = Duration::from_millis(50);
    let client = NntpClient::new(cfg);
    let lease = client.acquire_body_lane(ServerId(0), &[]).await.unwrap();
    let start = Instant::now();
    let result = client.acquire_body_lane(ServerId(0), &[]).await;
    assert!(matches!(
        result,
        Err(weaver_nntp::NntpError::AcquireTimeout(_))
    ));
    assert!(start.elapsed() < Duration::from_millis(500));
    drop(lease);
    client.shutdown().await;
    route.revoke().await;
}

#[tokio::test]
async fn bridges_have_distinct_secrets_and_cannot_authenticate_each_other() {
    let first = route(&[Mode::Healthy], Duration::from_secs(1));
    let second = route(&[Mode::Healthy], Duration::from_secs(1));
    let a = first.bridge().unwrap();
    let b = second.bridge().unwrap();
    assert_ne!(a.credentials(), b.credentials());
    let mut socket = tokio::net::TcpStream::connect(b.addr().unwrap())
        .await
        .unwrap();
    assert!(
        weaver_tunnel::transport::socks_connect(
            &mut socket,
            "private.invalid",
            443,
            Some(a.credentials())
        )
        .await
        .is_err()
    );
    assert_eq!(second.status().selected_proxy_id, None);
    first.revoke().await;
    second.revoke().await;
}

#[test]
fn concurrent_save_never_erases_a_committed_ssh_pin() {
    for id in 1..=8 {
        let db = Database::open_in_memory().unwrap();
        let mut profile = profile(id);
        profile.kind = ProxyKind::Ssh;
        profile.secrets.username = Some("fixture".into());
        profile.secrets.password = Some("fixture".into());
        db.save_proxy_profile(&profile).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let pin_db = db.clone();
        let pin_barrier = barrier.clone();
        let pin = std::thread::spawn(move || {
            pin_barrier.wait();
            pin_db.pin_proxy_host_key(id, 1, "SHA256:fixture")
        });
        profile.revision = 2;
        barrier.wait();
        db.save_proxy_profile(&profile).unwrap();
        if pin.join().unwrap().is_ok() {
            assert_eq!(
                db.proxy_host_key(id, 2).unwrap().as_deref(),
                Some("SHA256:fixture")
            );
        }
        assert!(db.pin_proxy_host_key(id, 1, "SHA256:stale").is_err());
    }
}
