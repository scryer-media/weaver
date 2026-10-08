use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn bottom() -> Arc<Egress> {
    Arc::new(Egress {
        id: 4,
        binding: SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
        timeout: Duration::from_secs(30),
    })
}
fn target(address: SocketAddr) -> Target {
    Target {
        host: address.ip().to_string(),
        port: address.port(),
        purpose: Purpose::Probe,
        addresses: Vec::new(),
    }
}

#[tokio::test]
async fn direct_stage_keeps_a_real_socket_and_revokes_only_its_leg() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first = Revocable::new(bottom());
    let second = Revocable::new(bottom());
    let first_stream = first
        .dial(&target(listener.local_addr().unwrap()))
        .await
        .unwrap();
    let (mut first_peer, source) = listener.accept().await.unwrap();
    let mut second_stream = second
        .dial(&target(listener.local_addr().unwrap()))
        .await
        .unwrap();
    let (mut second_peer, _) = listener.accept().await.unwrap();
    assert!(first_stream.stream.tcp().is_some());
    assert_eq!(first_stream.source, Some(source));
    assert_eq!(first_stream.path.egress, 4);
    first.revoke();
    let mut byte = [0];
    assert_eq!(first_peer.read(&mut byte).await.unwrap(), 0);
    second_stream.stream.write_all(b"x").await.unwrap();
    second_peer.read_exact(&mut byte).await.unwrap();
    assert_eq!(byte, *b"x");
    assert!(matches!(
        first.dial(&target(listener.local_addr().unwrap())).await,
        Err(DialError::Skipped(_))
    ));
}

#[tokio::test]
async fn a_direct_dial_uses_the_pinned_addresses_instead_of_resolving() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let egress = bottom();
    let pinned = Target {
        host: "feed.invalid".into(),
        port,
        purpose: Purpose::Probe,
        addresses: vec!["127.0.0.1".parse().unwrap()],
    };
    let mut dialed = egress.dial(&pinned).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    dialed.stream.write_all(b"x").await.unwrap();
    let mut byte = [0];
    peer.read_exact(&mut byte).await.unwrap();
    assert_eq!(byte, *b"x");
    let unpinned = Target {
        host: "feed.invalid".into(),
        port,
        purpose: Purpose::Probe,
        addresses: Vec::new(),
    };
    assert!(egress.dial(&unpinned).await.is_err());
}

/// A stream that, like an SSH channel, closes itself from a task it spawns
/// when dropped.
struct ClosesFromATask(tokio::io::DuplexStream);
impl Drop for ClosesFromATask {
    fn drop(&mut self) {
        tokio::spawn(async {});
    }
}
impl AsyncRead for ClosesFromATask {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
impl AsyncWrite for ClosesFromATask {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn a_dialed_tunnel_stream_dropped_off_the_runtime_closes_inside_it() {
    let (stream, _peer) = tokio::io::duplex(16);
    let outcome = Arc::new(ConnectionOutcome::default());
    let dialed = Dialed {
        stream: DialedStream::Tunnel(Box::new(ClosesFromATask(stream))),
        outcome: outcome.clone(),
        path: DialPath::default(),
        peer: None,
        source: None,
        setup: None,
    };
    let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = closed.clone();
    outcome.on_close(move || flag.store(true, std::sync::atomic::Ordering::SeqCst));
    let observed = dialed.into_observed_stream();
    // A blocking lane thread has no runtime of its own.
    std::thread::spawn(move || drop(observed)).join().unwrap();
    assert!(closed.load(std::sync::atomic::Ordering::SeqCst));
}

async fn connect_proxy(
    expected: String,
    next: SocketAddr,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            header.push(stream.read_u8().await.unwrap());
        }
        assert!(
            String::from_utf8(header)
                .unwrap()
                .starts_with(&format!("CONNECT {expected} HTTP/1.1\r\n"))
        );
        let mut upstream = tokio::net::TcpStream::connect(next).await.unwrap();
        stream
            .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            .await
            .unwrap();
        tokio::io::copy_bidirectional(&mut stream, &mut upstream)
            .await
            .unwrap();
    });
    (address, task)
}

fn hop(id: u32, endpoint: SocketAddr, inner: Arc<dyn Dialer>) -> Arc<TransportHop> {
    Arc::new(TransportHop {
        id,
        spec: TransportProxy {
            kind: crate::transport::TransportKind::HttpConnect,
            host: endpoint.ip().to_string(),
            port: endpoint.port(),
            username: None,
            password: None,
        },
        inner,
        timeout: Duration::from_secs(30),
        failure: Default::default(),
    })
}

#[tokio::test]
async fn chained_connect_hops_preserve_source_path_and_destination_name() {
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (second, second_task) =
        connect_proxy("news.invalid:119".into(), origin.local_addr().unwrap()).await;
    let (first, first_task) = connect_proxy(second.to_string(), second).await;
    let pipe = hop(8, second, hop(7, first, bottom()));
    let mut dialed = pipe
        .dial(&Target {
            host: "news.invalid".into(),
            port: 119,
            purpose: Purpose::Probe,
            addresses: Vec::new(),
        })
        .await
        .unwrap();
    let (mut peer, _) = origin.accept().await.unwrap();
    assert_eq!(dialed.path.proxies, vec![7, 8]);
    assert_eq!(dialed.path.egress, 4);
    assert_eq!(dialed.peer, Some(first));
    assert_eq!(
        dialed.source.unwrap().ip(),
        "127.0.0.1".parse::<IpAddr>().unwrap()
    );
    dialed.stream.write_all(b"article").await.unwrap();
    let mut bytes = [0; 7];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"article");
    peer.shutdown().await.unwrap();
    dialed.stream.shutdown().await.unwrap();
    drop(dialed);
    first_task.await.unwrap();
    second_task.await.unwrap();
}

/// A stage whose streams reach a proxy that has already sent `reply`.
struct Answered {
    reply: Mutex<&'static [u8]>,
    far: Mutex<Vec<tokio::io::DuplexStream>>,
}
#[async_trait::async_trait]
impl Dialer for Answered {
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        let (near, mut far) = tokio::io::duplex(1024);
        let reply = *self.reply.lock().unwrap();
        far.write_all(reply).await.unwrap();
        // The far end stays open so the hop can write its request.
        self.far.lock().unwrap().push(far);
        Ok(Dialed {
            stream: DialedStream::Tunnel(Box::new(near)),
            outcome: Default::default(),
            path: DialPath::default(),
            peer: None,
            source: None,
            setup: None,
        })
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "answered".into()
    }
}

/// Each hop keeps its own last failure, so a ladder names the first hop
/// failing on a rung even though the hop stacked on it failed as well.
#[tokio::test]
async fn a_chain_names_its_first_failing_hop() {
    let endpoint: SocketAddr = "192.0.2.1:8080".parse().unwrap();
    let unreachable = Arc::new(Failure {
        fatal: false,
        calls: Default::default(),
    });
    let chain = hop(8, endpoint, hop(7, endpoint, unreachable));
    let ladder = Fallback::new(vec![chain.clone(), bottom()]);
    assert_eq!(ladder.failing_hops(), [None, None]);
    assert!(matches!(
        chain.dial(&target("192.0.2.9:119".parse().unwrap())).await,
        Err(DialError::Egress(_))
    ));
    let failing = ladder.failing_hops();
    let first = failing[0].as_ref().expect("the chain has a failing hop");
    assert_eq!(
        first,
        &FailingHop {
            proxy: 7,
            reason: "endpoint unreachable: fixture refusal".into(),
        }
    );
    assert_eq!(failing[1], None);
}

/// A proxy that refuses to forward is failing itself, not the stage beneath
/// it, and stops failing as soon as it carries a dial.
#[tokio::test]
async fn a_hop_is_failing_until_it_carries_a_dial() {
    let stage = Arc::new(Answered {
        reply: Mutex::new(b"HTTP/1.1 403 Forbidden\r\n\r\n"),
        far: Default::default(),
    });
    let proxy = hop(8, "192.0.2.1:8080".parse().unwrap(), stage.clone());
    let target = target("192.0.2.9:119".parse().unwrap());
    let Err(DialError::Hop { proxy: 8, source }) = proxy.dial(&target).await else {
        panic!("the proxy refuses to forward");
    };
    // The hop is named beside its reason, so the reason is the cause alone.
    assert_eq!(
        proxy.failing_hop(),
        Some(FailingHop {
            proxy: 8,
            reason: source.to_string(),
        })
    );
    *stage.reply.lock().unwrap() = b"HTTP/1.1 200 Connection established\r\n\r\n";
    assert!(proxy.dial(&target).await.is_ok());
    assert_eq!(proxy.failing_hop(), None);
}

/// A session hop is failing after a path failure of its own. A destination
/// it reached for and could not connect to is not its failure, and carrying
/// a dial clears one.
#[tokio::test]
async fn a_session_hop_is_failing_only_while_its_own_path_fails() {
    use std::sync::atomic::{AtomicU8, Ordering};
    const PATH_DOWN: u8 = 0;
    const DESTINATION_DOWN: u8 = 1;
    const UP: u8 = 2;
    #[derive(Default)]
    struct Provider(AtomicU8);
    #[async_trait::async_trait]
    impl TunnelProvider for Provider {
        async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
            match self.0.load(Ordering::SeqCst) {
                PATH_DOWN => Err(TunnelError::Configuration("session lost".into())),
                DESTINATION_DOWN => Err(TunnelError::Dial {
                    host: host.into(),
                    port,
                    detail: "ConnectFailed".into(),
                }),
                _ => Ok(Box::new(tokio::io::duplex(64).0)),
            }
        }
        fn describe(&self) -> String {
            "scripted fixture".into()
        }
    }
    let provider = Arc::new(Provider::default());
    let hop = SessionHop {
        capacity: Default::default(),
        activity: Default::default(),
        id: 3,
        provider: provider.clone(),
        inner: bottom(),
        transport: None,
        path: DialPath::default(),
        endpoint: Some("127.0.0.1:22".parse().unwrap()),
        timeout: Duration::from_secs(30),
        resolver: None,
        failure: Default::default(),
    };
    let target = target(hop.endpoint.unwrap());
    assert_eq!(hop.failing_hop(), None);
    assert!(matches!(
        hop.dial(&target).await,
        Err(DialError::Hop { proxy: 3, .. })
    ));
    assert_eq!(hop.failing_hop().map(|hop| hop.proxy), Some(3));
    provider.0.store(DESTINATION_DOWN, Ordering::SeqCst);
    assert!(matches!(
        hop.dial(&target).await,
        Err(DialError::Destination(_))
    ));
    assert_eq!(hop.failing_hop(), None);
    provider.0.store(PATH_DOWN, Ordering::SeqCst);
    assert!(hop.dial(&target).await.is_err());
    assert_eq!(hop.failing_hop().map(|hop| hop.proxy), Some(3));
    provider.0.store(UP, Ordering::SeqCst);
    assert!(hop.dial(&target).await.is_ok());
    assert_eq!(hop.failing_hop(), None);
}

struct Failure {
    fatal: bool,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl Dialer for Failure {
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.fatal {
            Err(DialError::Fatal(TunnelError::HostKeyMismatch {
                host: "proxy.invalid".into(),
                port: 22,
                expected: "expected".into(),
                actual: "actual".into(),
            }))
        } else {
            Err(DialError::Egress(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "fixture refusal",
            )))
        }
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "failure".into()
    }
}

#[tokio::test(start_paused = true)]
async fn cooldown_skips_are_not_evidence_and_fatal_stops_fallback() {
    use std::sync::atomic::Ordering;
    let fail = Arc::new(Failure {
        fatal: false,
        calls: Default::default(),
    });
    let ladder = Fallback::new(vec![fail.clone()]);
    let mut target = target("127.0.0.1:119".parse().unwrap());
    target.purpose = Purpose::Nntp { server: 1, leg: 0 };
    assert!(matches!(
        ladder.dial(&target).await,
        Err(DialError::Egress(_))
    ));
    assert!(matches!(
        ladder.dial(&target).await,
        Err(DialError::Skipped(_))
    ));
    assert_eq!(fail.calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(30)).await;
    assert!(matches!(
        ladder.dial(&target).await,
        Err(DialError::Egress(_))
    ));
    let fatal = Arc::new(Failure {
        fatal: true,
        calls: Default::default(),
    });
    let ladder = Fallback::new(vec![fatal, fail.clone()]);
    assert!(matches!(
        ladder.dial(&target).await,
        Err(DialError::Fatal(_))
    ));
    assert_eq!(fail.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn idle_retirement_preserves_channels_shared_by_other_consumers() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct Provider(AtomicUsize);
    #[async_trait::async_trait]
    impl TunnelProvider for Provider {
        async fn retire(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        async fn dial(&self, _: &str, _: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
            Ok(Box::new(tokio::io::duplex(64).0))
        }
        fn describe(&self) -> String {
            "shared fixture".into()
        }
    }
    let provider = Arc::new(Provider::default());
    let hop = SessionHop {
        capacity: Default::default(),
        activity: Default::default(),
        id: 3,
        provider: provider.clone(),
        inner: bottom(),
        transport: None,
        path: DialPath::default(),
        endpoint: Some("127.0.0.1:22".parse().unwrap()),
        timeout: Duration::from_secs(30),
        resolver: None,
        failure: Default::default(),
    };
    let first = hop.dial(&target(hop.endpoint.unwrap())).await.unwrap();
    let second = hop.dial(&target(hop.endpoint.unwrap())).await.unwrap();
    hop.retire().await;
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
    drop(first);
    hop.retire().await;
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
    drop(second);
    hop.retire().await;
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    let reopened = hop.dial(&target(hop.endpoint.unwrap())).await.unwrap();
    hop.retire().await;
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    drop(reopened);
    hop.retire().await;
    assert_eq!(provider.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn only_a_path_failure_retires_an_idle_session() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    #[derive(Default)]
    struct Provider {
        retired: AtomicUsize,
        path_down: AtomicBool,
    }
    #[async_trait::async_trait]
    impl TunnelProvider for Provider {
        async fn retire(&self) {
            self.retired.fetch_add(1, Ordering::SeqCst);
        }
        async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
            Err(if self.path_down.load(Ordering::SeqCst) {
                TunnelError::Configuration("session lost".into())
            } else {
                TunnelError::Dial {
                    host: host.into(),
                    port,
                    detail: "ConnectFailed".into(),
                }
            })
        }
        fn describe(&self) -> String {
            "refusing fixture".into()
        }
    }
    let provider = Arc::new(Provider::default());
    let hop = SessionHop {
        capacity: Default::default(),
        activity: Default::default(),
        id: 3,
        provider: provider.clone(),
        inner: bottom(),
        transport: None,
        path: DialPath::default(),
        endpoint: Some("127.0.0.1:22".parse().unwrap()),
        timeout: Duration::from_secs(30),
        resolver: None,
        failure: Default::default(),
    };
    let target = target(hop.endpoint.unwrap());
    assert!(matches!(
        hop.dial(&target).await,
        Err(DialError::Destination(_))
    ));
    assert_eq!(provider.retired.load(Ordering::SeqCst), 0);
    provider.path_down.store(true, Ordering::SeqCst);
    assert!(matches!(
        hop.dial(&target).await,
        Err(DialError::Hop { .. })
    ));
    assert_eq!(provider.retired.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn destination_failures_do_not_cool_rungs_and_path_cooldown_is_flat() {
    let ladder = Fallback::new(vec![bottom()]);
    for _ in 0..20 {
        ladder.report_rung(
            0,
            Some(&DialError::Destination(io::Error::other("origin failed"))),
        );
        ladder.report_rung(
            0,
            Some(&DialError::Refused(io::Error::other("origin refused"))),
        );
    }
    assert_eq!(ladder.rung_states(), ["STANDBY"]);
    for _ in 0..3 {
        for _ in 0..20 {
            ladder.report_rung(0, Some(&DialError::Egress(io::Error::other("link failed"))));
        }
        assert!(!ladder.allows_rung(0, false));
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(ladder.allows_rung(0, false));
    }
}

#[tokio::test]
async fn fallback_keeps_real_error_before_a_skipped_rung() {
    let failed = Arc::new(Failure {
        fatal: false,
        calls: Default::default(),
    });
    let revoked = Arc::new(Revocable::new(bottom()));
    revoked.revoke();
    let ladder = Fallback::new(vec![failed, revoked]);
    assert!(matches!(
        ladder.dial(&target("127.0.0.1:119".parse().unwrap())).await,
        Err(DialError::Egress(_))
    ));
}

#[test]
fn unresolved_destination_and_bound_route_failures_have_distinct_evidence() {
    let unresolved = DialError::destination(io::Error::new(
        io::ErrorKind::AddrNotAvailable,
        ResolutionFailed,
    ));
    assert!(matches!(unresolved, DialError::Destination(_)));
    assert!(!unresolved.is_path_evidence());
    for kind in [
        io::ErrorKind::NetworkUnreachable,
        io::ErrorKind::HostUnreachable,
    ] {
        for binding in [
            SocketEgress::Interface("fixture-link".into()),
            SocketEgress::SourceAddress("192.0.2.1".parse().unwrap()),
        ] {
            let egress = Egress {
                id: 1,
                binding,
                timeout: Duration::from_secs(1),
            };
            let failure = egress.connect_error(io::Error::from(kind));
            assert!(failure.is_path_evidence());
            assert!(failure.to_string().contains("no route via"));
        }
        let system = Egress {
            id: 0,
            binding: SocketEgress::System,
            timeout: Duration::from_secs(1),
        };
        assert!(
            !system
                .connect_error(io::Error::from(kind))
                .is_path_evidence()
        );
    }
}

#[test]
fn session_budget_counts_probes_and_keeps_live_stream_reservations() {
    let budget = Arc::new(tokio::sync::Semaphore::new(1));
    let live = SessionCapacity::new(budget.clone());
    let probe = SessionCapacity::new(budget.clone());
    let stream = live.acquire().unwrap();
    assert!(matches!(probe.acquire(), Err(DialError::Skipped(_))));
    live.release();
    assert!(matches!(probe.acquire(), Err(DialError::Skipped(_))));
    drop(stream);
    let probe_stream = probe.acquire().unwrap();
    assert_eq!(budget.available_permits(), 0);
    drop(probe_stream);
    probe.release();
    assert_eq!(budget.available_permits(), 1);
}

#[test]
fn both_unique_local_ipv6_prefixes_are_filtered_consistently() {
    for ip in ["fc00::1", "fd00::1"] {
        assert!(!crate::egress::usable_address(ip.parse().unwrap()));
    }
    assert!(crate::egress::usable_address(
        "2001:db8::1".parse().unwrap()
    ));
}

#[test]
fn an_ssh_forwarding_refusal_is_hop_evidence_and_an_unreachable_destination_is_not() {
    let dial = |detail: &str| TunnelError::Dial {
        host: "news.example.test".into(),
        port: 563,
        detail: detail.into(),
    };
    let refused = DialError::hop(3, dial("AdministrativelyProhibited"));
    assert!(
        matches!(&refused, DialError::Hop { proxy: 3, .. }),
        "{refused:?}"
    );
    assert!(refused.is_path_evidence());
    let unreachable = DialError::hop(3, dial("ConnectFailed"));
    assert!(
        matches!(&unreachable, DialError::Destination(_)),
        "{unreachable:?}"
    );
    assert!(!unreachable.is_path_evidence());
}

#[test]
fn cooldown_doubles_from_thirty_seconds_to_five_minutes() {
    let secs: Vec<u64> = (0..8).map(|n| cooldown(n).as_secs()).collect();
    assert_eq!(secs, [30, 30, 60, 120, 240, 300, 300, 300]);
}

#[test]
fn cooldown_from_scales_the_whole_ladder() {
    let initial = Duration::from_secs(3);
    for failures in 0..8 {
        assert_eq!(cooldown_from(initial, failures), cooldown(failures) / 10);
    }
}

/// One WireGuard session carrying another: the carried session reaches the
/// peer behind the first, the session beneath stays up while anything rides
/// on it, and each session holds its own budget permit.
#[tokio::test]
async fn a_wireguard_session_carries_another_and_stays_up_while_it_rides() {
    use crate::test_support::{
        TEST_CLIENT_ADDRESS, TEST_PEER_ADDRESS, TEST_PEER_HTTP_PORT, WireGuardTestPeer,
        WireGuardTestPeerOptions, test_key,
    };
    const CARRIED_PORT: u16 = 51820;
    let far = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        private_key: test_key(31),
        body: "carried through two tunnels".into(),
        ..WireGuardTestPeerOptions::default()
    })
    .await;
    let near = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        udp_forward: Some((CARRIED_PORT, far.endpoint())),
        ..WireGuardTestPeerOptions::default()
    })
    .await;
    let budget = Arc::new(tokio::sync::Semaphore::new(2));
    let session = |id: u32,
                   provider: Arc<crate::WireGuardTunnelProvider>,
                   inner: Arc<dyn Dialer>,
                   proxies: Vec<u32>,
                   endpoint: SocketAddr| {
        Arc::new(SessionHop {
            capacity: SessionCapacity::new(budget.clone()),
            activity: Default::default(),
            id,
            provider: provider.clone(),
            inner,
            transport: None,
            path: DialPath {
                egress: 4,
                proxies,
                ..Default::default()
            },
            endpoint: Some(endpoint),
            timeout: Duration::from_secs(30),
            resolver: Some(provider),
            failure: Default::default(),
        })
    };

    let lower_provider = Arc::new(
        crate::WireGuardTunnelProvider::new(
            near.client_spec("1"),
            Arc::new(crate::NoopTunnelObserver),
        )
        .with_udp_factory(bottom()),
    );
    let lower = session(1, lower_provider, bottom(), vec![], near.endpoint());
    let carrier = lower
        .clone()
        .datagrams()
        .expect("a WireGuard session carries datagrams");
    let carried_endpoint = SocketAddr::new(IpAddr::V4(TEST_PEER_ADDRESS), CARRIED_PORT);
    let upper_provider = Arc::new(
        crate::WireGuardTunnelProvider::new(
            crate::WireGuardSpec {
                endpoint_host: TEST_PEER_ADDRESS.to_string(),
                endpoint_port: CARRIED_PORT,
                ..far.client_spec("2")
            },
            Arc::new(crate::NoopTunnelObserver),
        )
        .with_datagram_transport(carrier),
    );
    let upper = session(2, upper_provider, lower.clone(), vec![1], carried_endpoint);

    // Names beyond the top tunnel resolve on its far side.
    let Resolution::Addresses(addresses) = upper.resolve("origin.tunnel.test").await.unwrap()
    else {
        panic!("the top tunnel resolves names");
    };
    assert!(!addresses.is_empty());
    assert_eq!(far.dns_queries(), vec!["origin.tunnel.test"]);
    assert!(near.dns_queries().is_empty());

    let mut dialed = upper
        .dial(&Target {
            host: TEST_PEER_ADDRESS.to_string(),
            port: TEST_PEER_HTTP_PORT,
            purpose: Purpose::Probe,
            addresses: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(dialed.path.proxies, vec![1, 2]);
    assert_eq!(dialed.path.egress, 4);
    assert_eq!(dialed.peer, Some(carried_endpoint));
    dialed
        .stream
        .write_all(b"GET /carried HTTP/1.1\r\nHost: origin\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    dialed.stream.read_to_end(&mut answer).await.unwrap();
    assert!(String::from_utf8_lossy(&answer).ends_with("carried through two tunnels"));
    assert_eq!(far.requests(), vec!["GET /carried HTTP/1.1"]);
    assert!(near.requests().is_empty());
    assert_eq!(budget.available_permits(), 0, "one permit per tunnel");

    // The near peer relayed only WireGuard, from the lower tunnel's address.
    let forwarded = near.udp_forwarded();
    assert!(!forwarded.is_empty());
    for (source, seen) in &forwarded {
        assert_eq!(source.ip(), IpAddr::V4(TEST_CLIENT_ADDRESS));
        assert_eq!(seen.wireguard_datagrams, seen.datagrams, "{source}");
    }

    drop(dialed);
    // The lower session is busy for as long as the upper tunnel rides on it.
    assert!(!lower.retire_idle().await);
    assert!(upper.retire_idle().await);
    assert!(lower.retire_idle().await);
    assert_eq!(budget.available_permits(), 2);

    // Stopping the upper session leaves the one beneath it alone: that
    // session belongs to whoever built it and may serve other routes.
    upper.shutdown().await;
    let mut direct = lower
        .dial(&Target {
            host: TEST_PEER_ADDRESS.to_string(),
            port: TEST_PEER_HTTP_PORT,
            purpose: Purpose::Probe,
            addresses: Vec::new(),
        })
        .await
        .expect("the session beneath outlives the one it carried");
    direct
        .stream
        .write_all(b"GET /near HTTP/1.1\r\nHost: origin\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    direct.stream.read_to_end(&mut answer).await.unwrap();
    assert!(String::from_utf8_lossy(&answer).starts_with("HTTP/1.1 200"));
    drop(direct);
    lower.shutdown().await;
}

/// A WireGuard session over `inner`, as the runtime builds one, with its
/// own budget.
fn wireguard_session(
    id: u32,
    provider: Arc<dyn TunnelProvider>,
    resolver: Arc<crate::WireGuardTunnelProvider>,
    inner: Arc<dyn Dialer>,
    proxies: Vec<u32>,
    budget: usize,
) -> Arc<SessionHop> {
    Arc::new(SessionHop {
        capacity: SessionCapacity::new(Arc::new(tokio::sync::Semaphore::new(budget))),
        activity: Default::default(),
        id,
        provider,
        inner,
        transport: None,
        path: DialPath {
            egress: 4,
            proxies,
            ..Default::default()
        },
        endpoint: None,
        timeout: Duration::from_secs(30),
        resolver: Some(resolver),
        failure: Default::default(),
    })
}

/// A WireGuard hop stacked on `lower`, aimed at an address inside it.
fn carried_by(lower: &Arc<SessionHop>, spec: crate::WireGuardSpec) -> Arc<SessionHop> {
    let carrier = lower
        .clone()
        .datagrams()
        .expect("a WireGuard session carries datagrams");
    let provider = Arc::new(
        crate::WireGuardTunnelProvider::new(spec, Arc::new(crate::NoopTunnelObserver))
            .with_datagram_transport(carrier),
    );
    wireguard_session(2, provider.clone(), provider, lower.clone(), vec![1], 1)
}

fn probe_target() -> Target {
    Target {
        host: crate::test_support::TEST_PEER_ADDRESS.to_string(),
        port: crate::test_support::TEST_PEER_HTTP_PORT,
        purpose: Purpose::Probe,
        addresses: Vec::new(),
    }
}

/// The session beneath has no budget left: the stacked hop is skipped, as
/// the session beneath would be, and nothing counts against the hop above.
#[tokio::test]
async fn an_exhausted_budget_beneath_a_carried_hop_is_a_skip_not_its_failure() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let lower_provider = Arc::new(
        crate::WireGuardTunnelProvider::new(
            near.client_spec("1"),
            Arc::new(crate::NoopTunnelObserver),
        )
        .with_udp_factory(bottom()),
    );
    let lower = wireguard_session(
        1,
        lower_provider.clone(),
        lower_provider,
        bottom(),
        vec![],
        0,
    );
    let upper = carried_by(&lower, near.client_spec("2"));

    let error = match upper.dial(&probe_target()).await {
        Ok(_) => panic!("no budget beneath, no tunnel above"),
        Err(error) => error,
    };
    assert!(matches!(error, DialError::Skipped(_)), "{error:?}");
    assert!(!error.is_path_evidence());
    // The hop above never took a permit of its own for a tunnel it could
    // not bring up.
    assert_eq!(
        upper.capacity.budget.as_ref().unwrap().available_permits(),
        1
    );
}

/// The session beneath fails its handshake: the failure is that hop's, and
/// the hop stacked on it is not blamed.
#[tokio::test]
async fn a_failed_handshake_beneath_a_carried_hop_is_attributed_to_the_hop_beneath() {
    struct Unreachable;
    #[async_trait::async_trait]
    impl TunnelProvider for Unreachable {
        async fn prepare(&self) -> Result<(), TunnelError> {
            Err(TunnelError::WireGuardConnect {
                host: "vpn.example.test".into(),
                port: 51820,
                detail: "the peer did not complete a handshake".into(),
            })
        }
        async fn dial(&self, _: &str, _: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
            unreachable!("the session beneath never came up")
        }
        fn describe(&self) -> String {
            "unreachable fixture".into()
        }
    }
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let resolver = Arc::new(crate::WireGuardTunnelProvider::new(
        near.client_spec("1"),
        Arc::new(crate::NoopTunnelObserver),
    ));
    let lower = wireguard_session(1, Arc::new(Unreachable), resolver, bottom(), vec![], 1);
    let upper = carried_by(&lower, near.client_spec("2"));

    for error in [
        upper.dial(&probe_target()).await.err().expect("dial fails"),
        upper.prepare().await.expect_err("prepare fails"),
        upper
            .resolve("origin.tunnel.test")
            .await
            .expect_err("resolve fails"),
    ] {
        assert!(
            matches!(error, DialError::Hop { proxy: 1, .. }),
            "{error:?}"
        );
    }
    assert_eq!(upper.failing_hop().map(|hop| hop.proxy), Some(1));
}
