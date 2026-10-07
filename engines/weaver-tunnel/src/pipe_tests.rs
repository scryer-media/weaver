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
