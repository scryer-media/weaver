// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn bottom() -> Arc<Egress> {
    Arc::new(crate::test_support::loopback::egress(
        4,
        Duration::from_secs(30),
    ))
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

// A stream that, like an SSH channel, closes itself from a task it spawns
// when dropped.
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
        let mut upstream = crate::test_support::loopback::connect(next).await.unwrap();
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
    Arc::new(TransportHop::new(
        id,
        TransportProxy {
            kind: crate::transport::TransportKind::HttpConnect,
            host: endpoint.ip().to_string(),
            port: endpoint.port(),
            username: None,
            password: None,
        },
        inner,
        Duration::from_secs(30),
        Naming::Proxy,
        std::sync::Arc::new(crate::pipe::NamesSlot::new(crate::pipe::Names::new(
            crate::pipe::DnsChoice::System,
            None,
        ))),
        Default::default(),
    ))
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

// A stage whose streams reach a proxy that has already sent `reply`.
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

// Each hop keeps its own last failure, so a ladder names the first hop
// failing on a rung even though the hop stacked on it failed as well.
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
            fault: ProxyFault::ConnectionRefused,
        }
    );
    assert_eq!(failing[1], None);
}

// A proxy that refuses to forward is failing itself, not the stage beneath
// it, and stops failing as soon as it carries a dial.
#[tokio::test]
async fn a_hop_is_failing_until_it_carries_a_dial() {
    let stage = Arc::new(Answered {
        reply: Mutex::new(b"HTTP/1.1 403 Forbidden\r\n\r\n"),
        far: Default::default(),
    });
    let proxy = hop(8, "192.0.2.1:8080".parse().unwrap(), stage.clone());
    let target = target("192.0.2.9:119".parse().unwrap());
    assert!(matches!(
        proxy.dial(&target).await,
        Err(DialError::Hop { proxy: 8, .. })
    ));
    // The hop is named beside its fault, so the fault names the destination
    // alone.
    assert_eq!(
        proxy.failing_hop(),
        Some(FailingHop {
            proxy: 8,
            fault: ProxyFault::DestinationForbidden {
                target: "192.0.2.9:119".into()
            },
        })
    );
    *stage.reply.lock().unwrap() = b"HTTP/1.1 200 Connection established\r\n\r\n";
    assert!(proxy.dial(&target).await.is_ok());
    assert_eq!(proxy.failing_hop(), None);
}

// A proxy that reports it cannot reach the next hop's endpoint is working;
// the hop it could not reach is the one failing. The same report for a final
// destination stays on the proxy's own path.
#[tokio::test]
async fn a_chain_blames_the_hop_its_proxy_could_not_reach() {
    let stage = Arc::new(Answered {
        reply: Mutex::new(b"HTTP/1.1 502 Bad Gateway\r\n\r\n"),
        far: Default::default(),
    });
    let first = hop(7, "192.0.2.1:8080".parse().unwrap(), stage.clone());
    let chain = hop(8, "192.0.2.2:8080".parse().unwrap(), first.clone());
    let target = target("192.0.2.9:119".parse().unwrap());
    assert!(matches!(
        chain.dial(&target).await,
        Err(DialError::Hop { proxy: 8, .. })
    ));
    assert_eq!(first.failing_hop(), None);
    assert_eq!(
        chain.failing_hop(),
        Some(FailingHop {
            proxy: 8,
            fault: ProxyFault::DestinationUnreachable {
                target: "192.0.2.2:8080".into(),
                reply: "HTTP 502".into()
            },
        })
    );
    assert!(matches!(
        first.dial(&target).await,
        Err(DialError::Hop { proxy: 7, .. })
    ));
    assert_eq!(
        first.failing_hop(),
        Some(FailingHop {
            proxy: 7,
            fault: ProxyFault::DestinationUnreachable {
                target: "192.0.2.9:119".into(),
                reply: "HTTP 502".into()
            },
        })
    );
    // A proxy that refuses to forward at all is failing itself, whichever
    // destination it was asked for.
    *stage.reply.lock().unwrap() = b"HTTP/1.1 403 Forbidden\r\n\r\n";
    assert!(matches!(
        chain.dial(&target).await,
        Err(DialError::Hop { proxy: 7, .. })
    ));
    assert_eq!(
        chain.failing_hop(),
        Some(FailingHop {
            proxy: 7,
            fault: ProxyFault::DestinationForbidden {
                target: "192.0.2.2:8080".into()
            },
        })
    );
    // Each proxy answers its own request, and no hop is failing any longer.
    *stage.reply.lock().unwrap() =
        b"HTTP/1.1 200 Connection established\r\n\r\nHTTP/1.1 200 Connection established\r\n\r\n";
    assert!(chain.dial(&target).await.is_ok());
    assert_eq!(chain.failing_hop(), None);
}

// A SOCKS proxy says what became of its own connection attempt, so a reply
// that the next hop's endpoint refused names that hop.
#[tokio::test]
async fn a_socks_reply_names_the_hop_that_refused() {
    // The method is accepted, then the request comes back "connection refused".
    let stage = Arc::new(Answered {
        reply: Mutex::new(&[5, 0, 5, 5, 0, 1, 0, 0, 0, 0, 0, 0]),
        far: Default::default(),
    });
    let first = Arc::new(TransportHop::new(
        7,
        TransportProxy {
            kind: crate::transport::TransportKind::Socks5,
            host: "192.0.2.1".into(),
            port: 1080,
            username: None,
            password: None,
        },
        stage,
        Duration::from_secs(30),
        Naming::Proxy,
        std::sync::Arc::new(crate::pipe::NamesSlot::new(crate::pipe::Names::new(
            crate::pipe::DnsChoice::System,
            None,
        ))),
        Default::default(),
    ));
    let chain = hop(8, "192.0.2.2:8080".parse().unwrap(), first.clone());
    let target = target("192.0.2.9:119".parse().unwrap());
    assert!(matches!(
        chain.dial(&target).await,
        Err(DialError::Hop { proxy: 8, .. })
    ));
    assert_eq!(
        chain.failing_hop(),
        Some(FailingHop {
            proxy: 8,
            fault: ProxyFault::DestinationRefused {
                target: "192.0.2.2:8080".into()
            },
        })
    );
    assert!(matches!(
        first.dial(&target).await,
        Err(DialError::Hop { proxy: 7, .. })
    ));
    assert_eq!(
        first.failing_hop(),
        Some(FailingHop {
            proxy: 7,
            fault: ProxyFault::DestinationRefused {
                target: "192.0.2.9:119".into()
            },
        })
    );
}

// A proxy that takes the request for the next hop's endpoint and never
// answers was itself reached, so the wait is charged to the next hop. The
// same silence for a final destination times out this hop's own path.
#[tokio::test(start_paused = true)]
async fn a_silent_proxy_is_charged_to_the_hop_it_was_asked_for() {
    let stage = Arc::new(Answered {
        reply: Mutex::new(b""),
        far: Default::default(),
    });
    let first = hop(7, "192.0.2.1:8080".parse().unwrap(), stage);
    let chain = hop(8, "192.0.2.2:8080".parse().unwrap(), first.clone());
    let target = target("192.0.2.9:119".parse().unwrap());
    assert!(matches!(
        chain.dial(&target).await,
        Err(DialError::Hop { proxy: 8, .. })
    ));
    assert_eq!(first.failing_hop(), None);
    assert_eq!(
        chain.failing_hop(),
        Some(FailingHop {
            proxy: 8,
            fault: ProxyFault::DestinationTimedOut {
                target: "192.0.2.2:8080".into()
            },
        })
    );
    assert!(matches!(
        first.dial(&target).await,
        Err(DialError::Timeout { .. })
    ));
    assert_eq!(first.failing_hop().map(|hop| hop.proxy), Some(7));
}

// A session hop is failing after a path failure of its own. A destination
// it reached for and could not connect to is not its failure, and carrying
// a dial clears one.
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
        sockets: Default::default(),
        capacity: Default::default(),
        activity: Default::default(),
        id: 3,
        provider: provider.clone(),
        inner: bottom(),
        transport: None,
        path: DialPath::default(),
        endpoint: Some("127.0.0.1:22".parse().unwrap()),
        timeout: Duration::from_secs(30),
        naming: Naming::Proxy,
        tunnel_dns: Vec::new(),
        resolver: None,
        forwarding: Default::default(),
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

// A destination that keeps quiet through a session that answered lately is
// the destination's: the hop stays working and its watcher hears nothing.
// Once the session has not answered for `LIVE_WINDOW`, the same wait is the
// hop's. A session that fails outright is the hop's at once.
#[tokio::test(start_paused = true)]
async fn a_wait_on_a_live_session_is_the_destinations() {
    use std::sync::atomic::{AtomicU8, Ordering};
    const UP: u8 = 0;
    const QUIET: u8 = 1;
    const DEADLINE: u8 = 2;
    const BROKEN: u8 = 3;
    #[derive(Default)]
    struct Provider(AtomicU8);
    #[async_trait::async_trait]
    impl TunnelProvider for Provider {
        async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
            match self.0.load(Ordering::SeqCst) {
                UP => Ok(Box::new(tokio::io::duplex(64).0)),
                QUIET => std::future::pending().await,
                DEADLINE => Err(TunnelError::Http3Connect {
                    host: host.into(),
                    port,
                    detail: "HTTP/3 dial deadline exceeded".into(),
                }),
                _ => Err(TunnelError::Configuration("session lost".into())),
            }
        }
        fn describe(&self) -> String {
            "scripted fixture".into()
        }
    }
    let provider = Arc::new(Provider::default());
    let turns = Arc::new(Turns::default());
    let hop = SessionHop {
        sockets: Default::default(),
        capacity: Default::default(),
        activity: Default::default(),
        id: 3,
        provider: provider.clone(),
        inner: bottom(),
        transport: None,
        path: DialPath::default(),
        endpoint: Some("127.0.0.1:22".parse().unwrap()),
        timeout: Duration::from_secs(30),
        naming: Naming::Proxy,
        tunnel_dns: Vec::new(),
        resolver: None,
        forwarding: Default::default(),
        failure: HopFailure::watched(turns.clone()),
    };
    let target = target("192.0.2.9:119".parse().unwrap());
    assert!(hop.dial(&target).await.is_ok());
    assert_eq!(turns.taken(), vec![None]);

    for mode in [QUIET, DEADLINE] {
        provider.0.store(mode, Ordering::SeqCst);
        assert!(matches!(
            hop.dial(&target).await,
            Err(DialError::Destination(_))
        ));
        assert_eq!(hop.failing_hop(), None);
    }
    assert_eq!(turns.taken(), vec![]);

    tokio::time::advance(crate::pipe::LIVE_WINDOW).await;
    provider.0.store(QUIET, Ordering::SeqCst);
    assert!(matches!(
        hop.dial(&target).await,
        Err(DialError::Timeout { .. })
    ));
    assert_eq!(hop.failing_hop().map(|hop| hop.proxy), Some(3));
    assert_eq!(turns.taken().len(), 1);

    provider.0.store(UP, Ordering::SeqCst);
    assert!(hop.dial(&target).await.is_ok());
    provider.0.store(BROKEN, Ordering::SeqCst);
    assert!(matches!(
        hop.dial(&target).await,
        Err(DialError::Hop { proxy: 3, .. })
    ));
    let told = turns.taken();
    assert_eq!(told.len(), 2, "{told:?}");
    assert!(told[1].is_some());
}

// An HTTP proxy that answered lately and then keeps quiet for a
// destination is still working; one quiet past `LIVE_WINDOW` is failing.
// A SOCKS5 proxy that answers its greeting is up, so a quiet CONNECT after
// it is the destination's even on a first dial.
#[tokio::test(start_paused = true)]
async fn a_wait_on_a_live_proxy_is_the_destinations() {
    let stage = Arc::new(Answered {
        reply: Mutex::new(b"HTTP/1.1 200 Connection established\r\n\r\n"),
        far: Default::default(),
    });
    let turns = Arc::new(Turns::default());
    let mut http =
        Arc::into_inner(hop(7, "192.0.2.1:8080".parse().unwrap(), stage.clone())).unwrap();
    http.endpoint_names = std::sync::Arc::new(crate::pipe::NamesSlot::new(
        crate::pipe::Names::new(crate::pipe::DnsChoice::System, None),
    ));
    http.failure = HopFailure::watched(turns.clone());
    let target = target("192.0.2.9:119".parse().unwrap());
    assert!(http.dial(&target).await.is_ok());
    *stage.reply.lock().unwrap() = b"";
    assert!(matches!(
        http.dial(&target).await,
        Err(DialError::Destination(_))
    ));
    assert_eq!(http.failing_hop(), None);
    assert_eq!(turns.taken(), vec![None]);
    tokio::time::advance(crate::pipe::LIVE_WINDOW).await;
    assert!(matches!(
        http.dial(&target).await,
        Err(DialError::Timeout { .. })
    ));
    assert_eq!(http.failing_hop().map(|hop| hop.proxy), Some(7));
    assert_eq!(turns.taken().len(), 1);

    let greeted = Arc::new(Answered {
        reply: Mutex::new(&[5, 0]),
        far: Default::default(),
    });
    let socks = TransportHop::new(
        9,
        TransportProxy {
            kind: crate::transport::TransportKind::Socks5,
            host: "192.0.2.3".into(),
            port: 1080,
            username: None,
            password: None,
        },
        greeted,
        Duration::from_secs(30),
        Naming::Proxy,
        std::sync::Arc::new(crate::pipe::NamesSlot::new(crate::pipe::Names::new(
            crate::pipe::DnsChoice::System,
            None,
        ))),
        Default::default(),
    );
    assert!(matches!(
        socks.dial(&target).await,
        Err(DialError::Destination(_))
    ));
    assert_eq!(socks.failing_hop(), None);
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
    target.purpose = Purpose::Nntp {
        server: 1,
        leg: 0,
        probing: false,
    };
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
        sockets: Default::default(),
        capacity: Default::default(),
        activity: Default::default(),
        id: 3,
        provider: provider.clone(),
        inner: bottom(),
        transport: None,
        path: DialPath::default(),
        endpoint: Some("127.0.0.1:22".parse().unwrap()),
        timeout: Duration::from_secs(30),
        naming: Naming::Proxy,
        tunnel_dns: Vec::new(),
        resolver: None,
        forwarding: Default::default(),
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
        sockets: Default::default(),
        capacity: Default::default(),
        activity: Default::default(),
        id: 3,
        provider: provider.clone(),
        inner: bottom(),
        transport: None,
        path: DialPath::default(),
        endpoint: Some("127.0.0.1:22".parse().unwrap()),
        timeout: Duration::from_secs(30),
        naming: Naming::Proxy,
        tunnel_dns: Vec::new(),
        resolver: None,
        forwarding: Default::default(),
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
async fn saturated_fallback_reports_every_capacity_release_source() {
    use std::sync::atomic::Ordering;
    struct Capacity {
        available: std::sync::atomic::AtomicBool,
        changed: Arc<Notify>,
    }
    #[async_trait::async_trait]
    impl Dialer for Capacity {
        fn has_capacity(&self) -> bool {
            self.available.load(Ordering::Acquire)
        }
        async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
            Err(DialError::AtCapacity(vec![self.changed.clone()].into()))
        }
        fn budget(&self) -> Duration {
            Duration::ZERO
        }
        fn describe(&self) -> String {
            "capacity fixture".into()
        }
    }
    let first = Arc::new(Capacity {
        available: false.into(),
        changed: Arc::new(Notify::new()),
    });
    let second = Arc::new(Capacity {
        available: false.into(),
        changed: Arc::new(Notify::new()),
    });
    let ladder = Fallback::new(vec![first.clone(), second.clone()]);
    assert!(!ladder.has_capacity());
    let Err(DialError::AtCapacity(changes)) =
        ladder.dial(&target("127.0.0.1:119".parse().unwrap())).await
    else {
        panic!("all saturated rungs must report capacity");
    };
    assert_eq!(changes.waits.len(), 2);
    assert!(Arc::ptr_eq(&changes.waits[0], &first.changed));
    assert!(Arc::ptr_eq(&changes.waits[1], &second.changed));
    for rung in [&first, &second] {
        rung.available.store(true, Ordering::Release);
        assert!(ladder.has_capacity());
        rung.available.store(false, Ordering::Release);
        assert!(!ladder.has_capacity());
    }
    first.available.store(true, Ordering::Release);
    ladder.report_rung(0, Some(&DialError::Egress(io::Error::other("path down"))));
    assert!(
        !ladder.has_capacity(),
        "a cooling rung cannot satisfy normal acquisition"
    );
    tokio::time::advance(PATH_COOLDOWN).await;
    assert!(ladder.has_capacity());
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

// A rung with a naming of its own: it resolves every name to `answer`,
// counting the lookups, and keeps the targets it is asked to dial. It fails
// each dial as its path when `fails`, and reaches a stream otherwise.
struct OwnNaming {
    answer: Vec<IpAddr>,
    fails: bool,
    lookups: std::sync::atomic::AtomicUsize,
    dialled: Mutex<Vec<Target>>,
}
impl OwnNaming {
    fn new(answer: &[&str], fails: bool) -> Arc<Self> {
        Arc::new(Self {
            answer: answer.iter().map(|ip| ip.parse().unwrap()).collect(),
            fails,
            lookups: Default::default(),
            dialled: Default::default(),
        })
    }
    fn lookups(&self) -> usize {
        self.lookups.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn dialled(&self) -> Vec<Vec<IpAddr>> {
        self.dialled
            .lock()
            .unwrap()
            .iter()
            .map(|target| target.addresses.clone())
            .collect()
    }
}
#[async_trait::async_trait]
impl Dialer for OwnNaming {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        self.dialled.lock().unwrap().push(target.clone());
        if self.fails {
            return Err(DialError::Egress(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "fixture refusal",
            )));
        }
        let (near, _far) = tokio::io::duplex(64);
        Ok(Dialed {
            stream: DialedStream::Tunnel(Box::new(near)),
            outcome: Default::default(),
            path: DialPath::default(),
            peer: None,
            source: None,
            setup: None,
        })
    }
    async fn resolve(&self, _host: &str) -> Result<Resolution, DialError> {
        self.lookups
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(if self.answer.is_empty() {
            Resolution::Unresolved
        } else {
            Resolution::Addresses(self.answer.clone())
        })
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "own naming".into()
    }
}

// The addresses `ladder` resolves `host` to, pinned on a dial of it.
async fn checked(ladder: &Fallback, host: &str) -> Target {
    let Resolution::Addresses(addresses) = ladder.resolve(host).await.unwrap() else {
        panic!("the answering rung resolves names");
    };
    Target {
        addresses,
        ..named(host, 443)
    }
}

// A name checked under the first rung's naming is not dialled with that
// answer by the rung below it when the first fails: the rung below asks its
// own naming and dials only the checked addresses its answer also holds.
#[tokio::test]
async fn a_fallback_rung_dials_a_checked_name_under_its_own_naming() {
    let first = OwnNaming::new(&["192.0.2.1", "192.0.2.2"], true);
    let second = OwnNaming::new(&["192.0.2.2", "192.0.2.3"], false);
    let ladder = Fallback::new(vec![first.clone(), second.clone()]);
    let target = checked(&ladder, "feed.invalid").await;
    assert_eq!(first.lookups(), 1);
    ladder.dial(&target).await.unwrap();
    assert_eq!(first.lookups(), 1, "the answering rung dials what it gave");
    assert_eq!(second.lookups(), 1, "the rung below asks its own naming");
    assert_eq!(
        second.dialled(),
        [vec!["192.0.2.2".parse::<IpAddr>().unwrap()]]
    );
}

// A rung below whose naming gives none of the checked addresses does not
// carry the dial, and the first rung's failure is the one reported.
#[tokio::test]
async fn a_fallback_rung_whose_naming_disagrees_is_skipped() {
    let first = OwnNaming::new(&["192.0.2.1"], true);
    let second = OwnNaming::new(&["192.0.2.3"], false);
    let ladder = Fallback::new(vec![first.clone(), second.clone()]);
    let target = checked(&ladder, "feed.invalid").await;
    assert!(matches!(
        ladder.dial(&target).await,
        Err(DialError::Egress(_))
    ));
    assert_eq!(second.lookups(), 1);
    assert!(second.dialled().is_empty());
}

// A rung below whose proxy resolves names is handed the name, not the first
// rung's answer.
#[tokio::test]
async fn a_fallback_rung_whose_proxy_resolves_is_handed_the_name() {
    let first = OwnNaming::new(&["192.0.2.1"], true);
    let second = OwnNaming::new(&[], false);
    let ladder = Fallback::new(vec![first.clone(), second.clone()]);
    let target = checked(&ladder, "feed.invalid").await;
    ladder.dial(&target).await.unwrap();
    assert_eq!(second.lookups(), 1);
    assert_eq!(second.dialled(), [Vec::<IpAddr>::new()]);
}

#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "builds egresses only to classify their errors; it never dials or binds"
)]
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
            let egress = Egress::new(1, binding, Duration::from_secs(1));
            let failure = egress.connect_error(io::Error::from(kind));
            assert!(failure.is_path_evidence());
            assert!(failure.to_string().contains("no route via"));
        }
        let system = Egress::new(0, SocketEgress::System, Duration::from_secs(1));
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
    assert!(matches!(probe.acquire(), Err(DialError::SessionBudget)));
    live.release();
    assert!(matches!(probe.acquire(), Err(DialError::SessionBudget)));
    drop(stream);
    let probe_stream = probe.acquire().unwrap();
    assert_eq!(budget.available_permits(), 0);
    drop(probe_stream);
    probe.release();
    assert_eq!(budget.available_permits(), 1);
}

// A full tunnel and an exhausted session budget are refusals for room; every
// other refusal, skipped or not, is not.
#[test]
fn only_a_full_tunnel_or_session_budget_is_a_capacity_refusal() {
    let budget = Arc::new(tokio::sync::Semaphore::new(0));
    let exhausted = SessionCapacity::new(budget).acquire().unwrap_err();
    assert!(
        matches!(exhausted, DialError::SessionBudget),
        "{exhausted:?}"
    );
    assert!(exhausted.is_capacity(), "{exhausted}");
    assert!(DialError::AtCapacity(Default::default()).is_capacity());
    for other in [
        // Only the variant is capacity: the same words in a skip are not.
        DialError::Skipped(SESSION_BUDGET_EXHAUSTED.into()),
        DialError::Skipped("leg revoked".into()),
        DialError::Skipped("all rungs are unavailable".into()),
        DialError::Timeout {
            stage: "network route".into(),
        },
        DialError::Egress(io::Error::other("down")),
        DialError::Refused(io::Error::from(io::ErrorKind::ConnectionRefused)),
    ] {
        assert!(!other.is_capacity(), "{other}");
    }
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

// One WireGuard session carrying another: the carried session reaches the
// peer behind the first, the session beneath stays up while anything rides
// on it, and each session holds its own budget permit.
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
            sockets: Default::default(),
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
            naming: Naming::proxy_servers(
                vec![DnsServer::plain("10.0.0.2".parse().unwrap())],
                Trust::AsSent,
            ),
            tunnel_dns: Vec::new(),
            resolver: Some(provider),
            forwarding: Default::default(),
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
    // The upper tunnel's tasks are cancelled by its retire and let go of the
    // socket they hold in the lower session as they end, which can be after
    // the retire returns: wait for the last rider to leave.
    drop(lower.activity.write().await);
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

// A WireGuard session over `inner`, as the runtime builds one, with its
// own budget.
fn wireguard_session(
    id: u32,
    provider: Arc<dyn TunnelProvider>,
    resolver: Arc<crate::WireGuardTunnelProvider>,
    inner: Arc<dyn Dialer>,
    proxies: Vec<u32>,
    budget: usize,
) -> Arc<SessionHop> {
    Arc::new(SessionHop {
        sockets: Default::default(),
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
        naming: Naming::proxy_servers(
            vec![DnsServer::plain("10.0.0.2".parse().unwrap())],
            Trust::AsSent,
        ),
        tunnel_dns: Vec::new(),
        resolver: Some(resolver),
        forwarding: Default::default(),
        failure: Default::default(),
    })
}

// A WireGuard hop stacked on `lower`, aimed at an address inside it.
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

// The session beneath has no budget left: the stacked hop is skipped, as
// the session beneath would be, and nothing counts against the hop above.
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
    assert!(matches!(error, DialError::SessionBudget), "{error:?}");
    assert!(!error.is_path_evidence());
    // The hop above never took a permit of its own for a tunnel it could
    // not bring up.
    assert_eq!(
        upper.capacity.budget.as_ref().unwrap().available_permits(),
        1
    );
}

// The session beneath fails its handshake: the failure is that hop's, and
// the hop stacked on it is not blamed.
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

// A stage that reaches `to` whatever it is asked for, and keeps what it was
// asked for: an egress that can reach a DNS double behind a public address.
struct Mapped {
    to: SocketAddr,
    asked: Mutex<Vec<(String, u16)>>,
}
#[async_trait::async_trait]
impl Dialer for Mapped {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        self.asked
            .lock()
            .unwrap()
            .push((target.host.clone(), target.port));
        let stream = crate::test_support::loopback::connect(self.to)
            .await
            .map_err(DialError::Destination)?;
        Ok(Dialed {
            stream: DialedStream::Socket(stream),
            outcome: Default::default(),
            path: DialPath::default(),
            peer: None,
            source: None,
            setup: None,
        })
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(30)
    }
    fn describe(&self) -> String {
        "mapped".into()
    }
}

const ROUTED_DNS: &str = "192.0.2.53";
const PUBLIC: &str = "93.184.216.34";

fn named_hop(
    id: u32,
    host: &str,
    port: u16,
    naming: Naming,
    inner: Arc<dyn Dialer>,
) -> Arc<TransportHop> {
    named_hop_on(
        id,
        host,
        port,
        naming,
        inner,
        crate::pipe::DnsChoice::System,
    )
}

// A hop whose own address is named by `endpoint`.
fn named_hop_on(
    id: u32,
    host: &str,
    port: u16,
    naming: Naming,
    inner: Arc<dyn Dialer>,
    endpoint: crate::pipe::DnsChoice,
) -> Arc<TransportHop> {
    Arc::new(TransportHop::new(
        id,
        TransportProxy {
            kind: crate::transport::TransportKind::HttpConnect,
            host: host.into(),
            port,
            username: None,
            password: None,
        },
        inner,
        Duration::from_secs(30),
        naming,
        std::sync::Arc::new(crate::pipe::NamesSlot::new(crate::pipe::Names::new(
            endpoint, None,
        ))),
        Default::default(),
    ))
}

// A proxy name profile asking the servers at ROUTED_DNS.
fn listed_routed_dns() -> crate::pipe::DnsChoice {
    crate::pipe::DnsChoice::Weaver {
        servers: vec![DnsServer::plain(ROUTED_DNS.parse().unwrap())],
        trust: Trust::AsSent,
        transit: crate::pipe::DnsTransit::Egress,
    }
}

fn named(host: &str, port: u16) -> Target {
    Target {
        host: host.into(),
        port,
        purpose: Purpose::Probe,
        addresses: Vec::new(),
    }
}

async fn dns_double(names: &[(&str, &str)]) -> crate::test_support::dns::DnsServerDouble {
    crate::test_support::dns::DnsServerDouble::start(
        names
            .iter()
            .map(|(name, ip)| (name.to_string(), ip.parse().unwrap()))
            .collect(),
    )
    .await
}

async fn connect_double(
    mapping: &[((&str, u16), SocketAddr)],
) -> crate::test_support::proxy::ProxyServerDouble {
    crate::test_support::proxy::ProxyServerDouble::start(
        crate::transport::TransportKind::HttpConnect,
        None,
        mapping
            .iter()
            .map(|((host, port), to)| ((host.to_string(), *port), *to))
            .collect(),
    )
    .await
}

fn asked(targets: &Mutex<Vec<(String, u16)>>) -> Vec<(String, u16)> {
    targets.lock().unwrap().clone()
}

// An inner hop on a proxy name profile has its listed servers asked through
// the hop that carries it, for its own address, and the last hop is handed
// the destination's name.
#[tokio::test]
async fn each_hop_of_a_chain_resolves_the_next_with_its_own_setting() {
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dns = dns_double(&[("second.invalid", "10.9.9.9")]).await;
    let second = connect_double(&[(("news.invalid", 119), origin.local_addr().unwrap())]).await;
    let first = connect_double(&[
        ((ROUTED_DNS, 53), dns.addr),
        (("10.9.9.9", second.addr.port()), second.addr),
    ])
    .await;
    let pipe = named_hop_on(
        8,
        "second.invalid",
        second.addr.port(),
        Naming::Proxy,
        named_hop(
            7,
            &first.addr.ip().to_string(),
            first.addr.port(),
            Naming::Proxy,
            bottom(),
        ),
        listed_routed_dns(),
    );

    let dialed = pipe.dial(&named("news.invalid", 119)).await.unwrap();
    let _accepted = origin.accept().await.unwrap();

    assert_eq!(dialed.path.proxies, vec![7, 8]);
    assert_eq!(
        dns.queries.lock().unwrap().first().unwrap(),
        "second.invalid"
    );
    assert!(
        !dns.queries
            .lock()
            .unwrap()
            .iter()
            .any(|name| name == "news.invalid")
    );
    let through_first = asked(&first.targets);
    assert!(
        through_first.contains(&(ROUTED_DNS.into(), 53)),
        "{through_first:?}"
    );
    assert_eq!(
        through_first.last(),
        Some(&("10.9.9.9".to_string(), second.addr.port()))
    );
    assert!(
        !through_first
            .iter()
            .any(|(host, _)| host.ends_with(".invalid"))
    );
    assert_eq!(
        asked(&second.targets),
        vec![("news.invalid".to_string(), 119)]
    );
}

// The address of a hop on a proxy name profile in the middle of a chain is
// asked of its listed servers through the hop that carries it, never from
// the route's egress, and that hop is asked only for the address. A hop on
// the host's profile beneath it is handed its name by its carrier.
#[tokio::test]
async fn a_mid_chain_hop_resolved_by_weaver_has_weaver_resolve_the_next_hop() {
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dns = dns_double(&[("third.invalid", "10.9.9.9")]).await;
    let egress = Arc::new(Mapped {
        to: dns.addr,
        asked: Mutex::new(Vec::new()),
    });
    let third = connect_double(&[(("news.invalid", 119), origin.local_addr().unwrap())]).await;
    let second = connect_double(&[
        ((ROUTED_DNS, 53), dns.addr),
        (("10.9.9.9", third.addr.port()), third.addr),
    ])
    .await;
    let first = connect_double(&[(("second.invalid", second.addr.port()), second.addr)]).await;
    let pipe = named_hop_on(
        9,
        "third.invalid",
        third.addr.port(),
        Naming::Proxy,
        named_hop(
            8,
            "second.invalid",
            second.addr.port(),
            Naming::Proxy,
            named_hop(
                7,
                &first.addr.ip().to_string(),
                first.addr.port(),
                Naming::Proxy,
                bottom(),
            ),
        ),
        listed_routed_dns(),
    );

    let dialed = pipe.dial(&named("news.invalid", 119)).await.unwrap();
    let _accepted = origin.accept().await.unwrap();

    assert_eq!(dialed.path.proxies, vec![7, 8, 9]);
    assert_eq!(
        dns.queries.lock().unwrap().first().unwrap(),
        "third.invalid"
    );
    assert!(asked(&egress.asked).is_empty(), "the egress is not asked");
    // Each dial through the second hop, its lookup's among them, is carried
    // to it by the first, which is handed its name.
    let through_first = asked(&first.targets);
    assert!(!through_first.is_empty());
    assert!(
        through_first
            .iter()
            .all(|asked| asked == &("second.invalid".to_string(), second.addr.port())),
        "{through_first:?}"
    );
    let through_second = asked(&second.targets);
    assert!(
        through_second.contains(&(ROUTED_DNS.into(), 53)),
        "{through_second:?}"
    );
    assert_eq!(
        through_second.last(),
        Some(&("10.9.9.9".to_string(), third.addr.port()))
    );
    assert_eq!(
        asked(&third.targets),
        vec![("news.invalid".to_string(), 119)]
    );
}

// Weaver asks a hop's servers from the route's egress, so the proxy is
// asked only for the address they answered.
#[tokio::test]
async fn a_hop_resolved_by_weavers_servers_asks_them_from_the_egress() {
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dns = dns_double(&[("news.invalid", PUBLIC)]).await;
    let egress = Arc::new(Mapped {
        to: dns.addr,
        asked: Mutex::new(Vec::new()),
    });
    let proxy = connect_double(&[((PUBLIC, 119), origin.local_addr().unwrap())]).await;
    let pipe = named_hop(
        7,
        &proxy.addr.ip().to_string(),
        proxy.addr.port(),
        Naming::servers(
            vec![DnsServer::plain(ROUTED_DNS.parse().unwrap())],
            Trust::AsSent,
            egress.clone(),
        ),
        bottom(),
    );

    pipe.dial(&named("news.invalid", 119)).await.unwrap();
    let _accepted = origin.accept().await.unwrap();

    assert_eq!(dns.queries.lock().unwrap().first().unwrap(), "news.invalid");
    assert!(
        asked(&egress.asked)
            .iter()
            .all(|asked| asked == &(ROUTED_DNS.to_string(), 53))
    );
    assert_eq!(asked(&proxy.targets), vec![(PUBLIC.to_string(), 119)]);
}

// A recorded dial through a hop resolved by Weaver's servers says those
// servers answered, what they answered and which address the hop reached,
// and a name they do not know says so.
#[tokio::test]
async fn a_recorded_dial_says_weavers_servers_resolved_the_name() {
    use crate::resolution::record::{
        HostResolution, LookupError, LookupFailure, Recording, ResolvedBy,
    };
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dns = dns_double(&[("news.invalid", PUBLIC)]).await;
    let egress = Arc::new(Mapped {
        to: dns.addr,
        asked: Mutex::new(Vec::new()),
    });
    let proxy = connect_double(&[((PUBLIC, 119), origin.local_addr().unwrap())]).await;
    let pipe = named_hop(
        7,
        &proxy.addr.ip().to_string(),
        proxy.addr.port(),
        Naming::servers(
            vec![DnsServer::plain(ROUTED_DNS.parse().unwrap())],
            Trust::AsSent,
            egress,
        ),
        bottom(),
    );

    let recording = Arc::new(Recording::default());
    recording
        .scope(pipe.dial(&named("news.invalid", 119)))
        .await
        .unwrap();
    let _accepted = origin.accept().await.unwrap();
    assert_eq!(
        recording.finish("news.invalid", 119, None),
        Some(HostResolution {
            host: "news.invalid".into(),
            by: ResolvedBy::Servers,
            proxy: None,
            servers: vec![ROUTED_DNS.parse().unwrap()],
            wire: Some(crate::dns::WireKind::Plain),
            answered_by: Some(ROUTED_DNS.parse().unwrap()),
            trust: Some(crate::resolution::record::TrustOutcome::AsSent),
            addresses: vec![PUBLIC.parse().unwrap()],
            cached: false,
            connected: Some(PUBLIC.parse().unwrap()),
            error: None,
        })
    );

    let recording = Arc::new(Recording::default());
    assert!(
        recording
            .scope(pipe.dial(&named("missing.invalid", 119)))
            .await
            .is_err()
    );
    let resolution = recording.finish("missing.invalid", 119, None).unwrap();
    assert_eq!(resolution.by, ResolvedBy::Servers);
    assert!(resolution.addresses.is_empty());
    assert_eq!(resolution.connected, None);
    assert_eq!(
        resolution.error,
        Some(LookupError {
            failure: LookupFailure::NoSuchName,
            detail: format!("{ROUTED_DNS}: NXDOMAIN"),
        })
    );
}

fn lookups(dns: &crate::test_support::dns::DnsServerDouble, name: &str) -> usize {
    dns.queries
        .lock()
        .unwrap()
        .iter()
        .filter(|asked| *asked == name)
        .count()
}

// A hop holds what its servers answered for as long as they said, so a
// second dial within that asks them nothing; an answer for no time at all
// is asked for again. A day's TTL is held for the hour ceiling, past any
// run of this test.
#[tokio::test]
async fn a_hop_holds_its_servers_answer_for_its_ttl() {
    for (ttl, held) in [(86_400, true), (0, false)] {
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dns = dns_double(&[("news.invalid", PUBLIC)]).await;
        dns.ttl.store(ttl, std::sync::atomic::Ordering::Relaxed);
        let egress = Arc::new(Mapped {
            to: dns.addr,
            asked: Mutex::new(Vec::new()),
        });
        let proxy = connect_double(&[((PUBLIC, 119), origin.local_addr().unwrap())]).await;
        let pipe = named_hop(
            7,
            &proxy.addr.ip().to_string(),
            proxy.addr.port(),
            Naming::servers(
                vec![DnsServer::plain(ROUTED_DNS.parse().unwrap())],
                Trust::AsSent,
                egress,
            ),
            bottom(),
        );

        pipe.dial(&named("news.invalid", 119)).await.unwrap();
        let _first = origin.accept().await.unwrap();
        let once = lookups(&dns, "news.invalid");
        assert!(once > 0);
        pipe.dial(&named("news.invalid", 119)).await.unwrap();
        let _second = origin.accept().await.unwrap();

        let expected = if held { once } else { 2 * once };
        assert_eq!(lookups(&dns, "news.invalid"), expected, "TTL {ttl}");
        assert_eq!(asked(&proxy.targets).len(), 2);
    }
}

// An answer whose address the proxy could not reach is dropped, so the next
// dial asks the servers again rather than reusing it.
#[tokio::test]
async fn a_hop_forgets_an_answer_its_proxy_could_not_reach() {
    let dns = dns_double(&[("news.invalid", PUBLIC)]).await;
    dns.ttl.store(86_400, std::sync::atomic::Ordering::Relaxed);
    let egress = Arc::new(Mapped {
        to: dns.addr,
        asked: Mutex::new(Vec::new()),
    });
    // A proxy that reaches nothing.
    let proxy = connect_double(&[]).await;
    let pipe = named_hop(
        7,
        &proxy.addr.ip().to_string(),
        proxy.addr.port(),
        Naming::servers(
            vec![DnsServer::plain(ROUTED_DNS.parse().unwrap())],
            Trust::AsSent,
            egress,
        ),
        bottom(),
    );

    let Err(error) = pipe.dial(&named("news.invalid", 119)).await else {
        panic!("a proxy that reaches nothing connected");
    };
    assert!(
        matches!(&error, DialError::Hop { proxy: 7, source } if crate::transport::is_destination_unreachable(source)),
        "{error:?}"
    );
    let once = lookups(&dns, "news.invalid");
    assert!(once > 0);
    assert!(pipe.dial(&named("news.invalid", 119)).await.is_err());

    assert_eq!(lookups(&dns, "news.invalid"), 2 * once);
}

// A failure shared with callers that waited on the same lookup says what
// the one that asked was told.
#[test]
fn a_shared_dial_failure_keeps_its_kind_and_its_hop() {
    use crate::resolution::Replicate;
    let hop = DialError::Hop {
        proxy: 7,
        source: TunnelError::Engine("proxy refused".into()),
    };
    let DialError::Hop { proxy, source } = hop.replicate() else {
        panic!("a hop failure changed kind");
    };
    assert_eq!(proxy, 7);
    assert_eq!(
        source.to_string(),
        TunnelError::Engine("proxy refused".into()).to_string()
    );
    let refused = DialError::Refused(io::Error::new(io::ErrorKind::ConnectionRefused, "refused"));
    let DialError::Refused(copy) = refused.replicate() else {
        panic!("a refusal changed kind");
    };
    assert_eq!(copy.kind(), io::ErrorKind::ConnectionRefused);
    assert_eq!(copy.to_string(), "refused");
    let unresolved = crate::dns::unresolved().replicate();
    assert!(matches!(&unresolved, DialError::Destination(_)));
    assert_eq!(unresolved.to_string(), crate::dns::unresolved().to_string());
}

// The host's own DNS resolves for a hop set to it, and the proxy is asked
// only for addresses.
#[tokio::test]
async fn a_hop_resolved_by_the_hosts_dns_hands_the_proxy_addresses() {
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let to = origin.local_addr().unwrap();
    let proxy = connect_double(&[(("127.0.0.1", 119), to), (("::1", 119), to)]).await;
    let pipe = named_hop(
        7,
        &proxy.addr.ip().to_string(),
        proxy.addr.port(),
        Naming::Host,
        bottom(),
    );

    pipe.dial(&named("localhost", 119)).await.unwrap();
    let _accepted = origin.accept().await.unwrap();

    let targets = asked(&proxy.targets);
    assert!(!targets.is_empty());
    assert!(
        targets
            .iter()
            .all(|(host, _)| host.parse::<IpAddr>().is_ok()),
        "{targets:?}"
    );
}

// Servers asked through a hop that cannot be reached fail the dial as the
// path failed, not as a name that does not resolve.
#[tokio::test]
async fn servers_asked_through_an_unreachable_hop_fail_as_the_path() {
    let inner = Arc::new(Failure {
        fatal: false,
        calls: Default::default(),
    });
    let pipe = named_hop(
        7,
        "192.0.2.1",
        8080,
        Naming::proxy_servers(
            vec![DnsServer::plain(ROUTED_DNS.parse().unwrap())],
            Trust::AsSent,
        ),
        inner,
    );
    let Err(error) = pipe.dial(&named("news.invalid", 119)).await else {
        panic!("a dial through an unreachable hop succeeded");
    };
    assert!(error.is_path_evidence(), "{error:?}");
    assert!(!matches!(error, DialError::Destination(_)), "{error:?}");
}

// An egress whose DNS server answers each query only after `delay`: `PUBLIC`
// for an A query, nothing for any other.
struct SlowDns {
    delay: Duration,
}
#[async_trait::async_trait]
impl Dialer for SlowDns {
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        let (near, mut far) = tokio::io::duplex(1024);
        let delay = self.delay;
        tokio::spawn(async move {
            let size = far.read_u16().await.unwrap() as usize;
            let mut reply = vec![0; size];
            far.read_exact(&mut reply).await.unwrap();
            tokio::time::sleep(delay).await;
            let qtype = u16::from_be_bytes([reply[size - 4], reply[size - 3]]);
            reply[2..4].copy_from_slice(&0x8180u16.to_be_bytes());
            if qtype == 1 {
                reply[6..8].copy_from_slice(&1u16.to_be_bytes());
                reply.extend_from_slice(&[0xc0, 12, 0, 1, 0, 1, 0, 0, 0, 1, 0, 4]);
                reply.extend_from_slice(&PUBLIC.parse::<std::net::Ipv4Addr>().unwrap().octets());
            }
            far.write_u16(reply.len() as u16).await.unwrap();
            far.write_all(&reply).await.unwrap();
        });
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
        "slow dns".into()
    }
}

// A stage whose proxy answers the request for the next hop only after
// `delay`.
struct Late {
    delay: Duration,
}
#[async_trait::async_trait]
impl Dialer for Late {
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        let (near, mut far) = tokio::io::duplex(1024);
        let delay = self.delay;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            far.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();
            // The far end stays open for the stream the hop hands back.
            std::future::pending::<()>().await;
        });
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
        "late".into()
    }
}

// A lookup has a window of its own: one that takes 8 s of a hop's 30 still
// leaves the proxy the whole 30 s to answer, and the hop's budget counts
// both windows. The naming is the one in scope when the hop dials, which a
// DNS change swaps without rebuilding the hop, so a hop that hands the name
// on today keeps the lookup window too.
#[tokio::test(start_paused = true)]
async fn a_slow_lookup_leaves_the_hop_its_whole_connect_window() {
    let resolving = named_hop(
        7,
        "192.0.2.1",
        8080,
        Naming::servers(
            vec![DnsServer::plain(ROUTED_DNS.parse().unwrap())],
            Trust::AsSent,
            Arc::new(SlowDns {
                delay: Duration::from_secs(4),
            }),
        ),
        Arc::new(Late {
            delay: Duration::from_secs(25),
        }),
    );

    let dialed = resolving.dial(&named("news.invalid", 119)).await;

    assert!(dialed.is_ok(), "{:?}", dialed.err());
    assert_eq!(resolving.budget(), Duration::from_secs(61));
    let handing_on = named_hop(
        8,
        "192.0.2.1",
        8080,
        Naming::Proxy,
        Arc::new(Late {
            delay: Duration::ZERO,
        }),
    );
    assert_eq!(handing_on.budget(), Duration::from_secs(61));
}

// A stage that carries datagrams, as a WireGuard session does.
struct Carrying;
struct NoPeers;
#[async_trait::async_trait]
impl crate::endpoint::DatagramTransport for NoPeers {
    async fn resolve(&self, _: &str) -> Result<Vec<IpAddr>, TunnelError> {
        Ok(Vec::new())
    }
    async fn bind(
        &self,
        _: SocketAddr,
    ) -> Result<Arc<dyn crate::endpoint::DatagramSocket>, TunnelError> {
        Err(TunnelError::Engine("no peers".into()))
    }
}
#[async_trait::async_trait]
impl Dialer for Carrying {
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        Err(DialError::Skipped("carries datagrams only".into()))
    }
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn crate::endpoint::DatagramTransport>> {
        Some(Arc::new(NoPeers))
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "carrying".into()
    }
}

#[test]
fn a_revocable_leg_carries_its_stage_datagrams_until_revoked() {
    let leg = Arc::new(Revocable::new(Arc::new(Carrying)));
    assert!(Arc::clone(&leg).datagrams().is_some());
    assert!(Arc::new(Revocable::new(bottom())).datagrams().is_none());
    leg.revoke();
    assert!(leg.datagrams().is_none());
}

#[tokio::test]
async fn a_ladder_carries_the_datagrams_of_the_rung_a_dial_would_try() {
    let ladder = Arc::new(Fallback::new(vec![Arc::new(Carrying), bottom()]));
    assert!(Arc::clone(&ladder).datagrams().is_some());
    ladder.report_rung(0, Some(&DialError::Egress(io::Error::other("path down"))));
    assert!(ladder.datagrams().is_none());
}

// A WireGuard hop whose tunnel has every socket slot taken.
struct Full(proxy_tunnels::SlotFreed);
#[async_trait::async_trait]
impl Dialer for Full {
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        Err(DialError::hop(
            1,
            TunnelError::AtCapacity {
                open: 256,
                freed: self.0.clone(),
            },
        ))
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "full".into()
    }
}

#[tokio::test]
async fn a_full_tunnel_waits_for_room_without_cooling_its_rung() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut target = target(listener.local_addr().unwrap());
    target.purpose = Purpose::Nntp {
        server: 1,
        leg: 0,
        probing: false,
    };
    let freed = proxy_tunnels::SlotFreed::default();

    // The ladder goes on to the next rung and leaves the full one standing.
    let ladder = Fallback::new(vec![Arc::new(Full(freed.clone())), bottom()]);
    let dialed = ladder.dial(&target).await.unwrap();
    assert_eq!(dialed.path.rung, Some(1));
    assert_eq!(ladder.rung_states()[0], "STANDBY");

    // With nowhere else to go, the dial waits on the tunnel freeing a slot.
    let ladder = Fallback::new(vec![Arc::new(Full(freed.clone()))]);
    let Err(DialError::AtCapacity(waits)) = ladder.dial(&target).await else {
        panic!("a full tunnel is at capacity");
    };
    assert!(Arc::ptr_eq(&waits.waits[0], &freed.notify()));
    assert_eq!(ladder.rung_states(), ["STANDBY"]);
}

// A WireGuard session over the test peer, as the runtime builds one.
fn listening_session(near: &crate::test_support::WireGuardTestPeer) -> Arc<SessionHop> {
    let provider = Arc::new(
        crate::WireGuardTunnelProvider::new(
            near.client_spec("1"),
            Arc::new(crate::NoopTunnelObserver),
        )
        .with_udp_factory(bottom()),
    );
    wireguard_session(1, provider.clone(), provider, bottom(), vec![], 1)
}

// A peer inside the tunnel connects to the session's listener: the
// connection carries bytes both ways, names the session's path, and holds
// the session up, as the listener does while it lives.
#[tokio::test]
async fn a_wireguard_listener_takes_a_peer_connection_inside_its_tunnel() {
    use crate::test_support::{TEST_CLIENT_ADDRESS, TEST_PEER_ADDRESS, WireGuardTestPeer};
    let near = WireGuardTestPeer::start().await;
    let session = listening_session(&near);
    let listener = Arc::clone(&session)
        .inbound()
        .expect("a WireGuard session takes connections")
        .listen(0, true)
        .await
        .unwrap();
    let local = listener.local_addr();
    assert_eq!(local.ip(), IpAddr::V4(TEST_CLIENT_ADDRESS));
    assert_ne!(local.port(), 0);

    let (accepted, peer) = tokio::join!(listener.accept(), near.dial_client(local.port()));
    let mut accepted = accepted.unwrap();
    let mut peer = peer.unwrap();
    assert_eq!(accepted.path.proxies, vec![1]);
    assert_eq!(accepted.path.egress, 4);
    assert_eq!(accepted.source, Some(local));
    assert_eq!(
        accepted.peer.map(|peer| peer.ip()),
        Some(IpAddr::V4(TEST_PEER_ADDRESS))
    );
    peer.write_all(b"handshake").await.unwrap();
    let mut heard = [0u8; 9];
    accepted.stream.read_exact(&mut heard).await.unwrap();
    assert_eq!(&heard, b"handshake");
    accepted.stream.write_all(b"bitfield").await.unwrap();
    let mut answer = [0u8; 8];
    peer.read_exact(&mut answer).await.unwrap();
    assert_eq!(&answer, b"bitfield");

    // The connection and the listener each hold the session; it retires
    // only once both are gone, and the connection's close is reported.
    let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    accepted.outcome.on_close({
        let closed = closed.clone();
        move || closed.store(true, std::sync::atomic::Ordering::SeqCst)
    });
    drop(listener);
    assert!(!session.retire_idle().await);
    drop(accepted);
    assert!(closed.load(std::sync::atomic::Ordering::SeqCst));
    assert!(session.retire_idle().await);
    assert_eq!(
        session
            .capacity
            .budget
            .as_ref()
            .unwrap()
            .available_permits(),
        1
    );
}

// The tunnel is replaced under a listener: its next accept listens again
// on the same port inside the new tunnel, and a peer reaches it there.
#[tokio::test]
async fn a_wireguard_listener_listens_again_inside_a_replaced_tunnel() {
    use crate::test_support::WireGuardTestPeer;
    let near = WireGuardTestPeer::start().await;
    let session = listening_session(&near);
    let listener = Arc::clone(&session)
        .inbound()
        .unwrap()
        .listen(0, true)
        .await
        .unwrap();
    let port = listener.local_addr().port();

    // Retire the tunnel the listener sits in, and bring its replacement up.
    session.provider.retire().await;
    session.prepare().await.unwrap();

    // The accept finds its tunnel stopped and listens in the new one; the
    // peer dials once the new tunnel listens on the port.
    let provider = session.resolver.as_ref().unwrap();
    let (accepted, peer) = tokio::join!(listener.accept(), async {
        while !provider.listens_on(port).await {
            tokio::task::yield_now().await;
        }
        near.dial_client(port).await
    });
    let mut accepted = accepted.unwrap();
    let mut peer = peer.unwrap();
    assert_eq!(listener.local_addr().port(), port);
    assert_eq!(accepted.source.map(|source| source.port()), Some(port));
    peer.write_all(b"again").await.unwrap();
    let mut heard = [0u8; 5];
    accepted.stream.read_exact(&mut heard).await.unwrap();
    assert_eq!(&heard, b"again");
}

// A listener the tunnel has no sockets left for is refused as full, not
// broken: the hop is not marked failing, and the tunnel stays up with the
// listener that filled it.
#[tokio::test]
async fn a_listener_a_full_tunnel_cannot_hold_leaves_the_hop_working() {
    use crate::test_support::WireGuardTestPeer;
    let near = WireGuardTestPeer::start().await;
    let provider = Arc::new(
        crate::WireGuardTunnelProvider::new(
            crate::WireGuardSpec {
                // One listener's backlog fills the smallest tunnel.
                max_sockets: crate::wireguard::MIN_MAX_SOCKETS,
                ..near.client_spec("1")
            },
            Arc::new(crate::NoopTunnelObserver),
        )
        .with_udp_factory(bottom()),
    );
    let session = wireguard_session(1, provider.clone(), provider, bottom(), vec![], 2);
    let inbound = Arc::clone(&session).inbound().unwrap();
    let first = inbound.listen(0, true).await.unwrap();
    let port = first.local_addr().port();

    let Err(refused) = inbound.listen(0, false).await else {
        panic!("a full tunnel takes no second listener");
    };
    assert!(
        matches!(refused, TunnelError::AtCapacity { .. }),
        "{refused:?}"
    );
    assert!(session.failure.of(1).is_none(), "the hop is not failing");
    assert!(session.resolver.as_ref().unwrap().listens_on(port).await);
}

// Only a WireGuard hop at the top of a path takes connections: a chain ends
// in one or it listens nowhere, and an SSH session or a SOCKS or HTTP hop
// takes none, whatever runs beneath it.
#[tokio::test]
async fn only_a_path_ending_in_wireguard_takes_connections() {
    use crate::test_support::WireGuardTestPeer;
    let near = WireGuardTestPeer::start().await;
    let wireguard = listening_session(&near);
    assert!(Arc::clone(&wireguard).inbound().is_some());

    // WireGuard on WireGuard: the top hop listens.
    let stacked = carried_by(&wireguard, near.client_spec("2"));
    assert!(stacked.inbound().is_some());

    // SOCKS and HTTP CONNECT over WireGuard: no listener.
    let socks = Arc::new(TransportHop::new(
        7,
        TransportProxy {
            kind: crate::transport::TransportKind::Socks5,
            host: "192.0.2.1".into(),
            port: 1080,
            username: None,
            password: None,
        },
        wireguard.clone(),
        Duration::from_secs(30),
        Naming::Proxy,
        std::sync::Arc::new(crate::pipe::NamesSlot::new(crate::pipe::Names::new(
            crate::pipe::DnsChoice::System,
            None,
        ))),
        Default::default(),
    ));
    assert!(socks.inbound().is_none());
    let connect = hop(8, "192.0.2.2:8080".parse().unwrap(), wireguard.clone());
    assert!(connect.inbound().is_none());

    // An SSH session over WireGuard: no listener.
    let ssh = Arc::new(SessionHop {
        sockets: Default::default(),
        capacity: SessionCapacity::default(),
        activity: Default::default(),
        id: 9,
        provider: wireguard.provider.clone(),
        inner: wireguard.clone(),
        transport: None,
        path: DialPath::default(),
        endpoint: None,
        timeout: Duration::from_secs(30),
        naming: Naming::Proxy,
        tunnel_dns: Vec::new(),
        resolver: None,
        forwarding: Default::default(),
        failure: Default::default(),
    });
    assert!(ssh.inbound().is_none());

    // A direct egress listens nowhere through a dialer.
    assert!(bottom().inbound().is_none());
}

// A ladder takes the connections of the rung a dial would try, and a
// revoked leg takes none.
#[tokio::test]
async fn a_ladder_listens_on_the_rung_a_dial_would_try_and_a_revoked_leg_nowhere() {
    use crate::test_support::WireGuardTestPeer;
    let near = WireGuardTestPeer::start().await;
    let wireguard: Arc<dyn Dialer> = listening_session(&near);
    let ladder = Arc::new(Fallback::new(vec![wireguard.clone(), bottom()]));
    assert!(Arc::clone(&ladder).inbound().is_some());
    ladder.report_rung(0, Some(&DialError::Egress(io::Error::other("path down"))));
    assert!(Arc::clone(&ladder).inbound().is_none());

    let leg = Arc::new(Revocable::new(wireguard));
    assert!(Arc::clone(&leg).inbound().is_some());
    leg.revoke();
    assert!(leg.inbound().is_none());
}

// `session`, its provider forwarding a port as `forwarding` says.
fn forwarding(session: Arc<SessionHop>, forwarding: PortForwarding) -> Arc<SessionHop> {
    let mut session = Arc::try_unwrap(session)
        .ok()
        .expect("a session nobody holds yet");
    session.forwarding = forwarding;
    Arc::new(session)
}

// A WireGuard hop stacked on `lower` as hop `id` of a chain, aimed at the
// address inside `lower`'s tunnel that relays to `peer`.
fn chained_on(
    lower: &Arc<SessionHop>,
    peer: &crate::test_support::WireGuardTestPeer,
    id: u32,
    proxies: Vec<u32>,
) -> Arc<SessionHop> {
    let carrier = lower
        .clone()
        .datagrams()
        .expect("a WireGuard session carries datagrams");
    let provider = Arc::new(
        crate::WireGuardTunnelProvider::new(
            crate::WireGuardSpec {
                endpoint_host: crate::test_support::TEST_PEER_ADDRESS.to_string(),
                endpoint_port: CHAINED_PORT,
                ..peer.client_spec(&id.to_string())
            },
            Arc::new(crate::NoopTunnelObserver),
        )
        .with_datagram_transport(carrier),
    );
    wireguard_session(id, provider.clone(), provider, lower.clone(), proxies, 1)
}

// The port inside each tunnel that relays the next hop's WireGuard.
const CHAINED_PORT: u16 = 51820;

// Accept one connection from `peer` on `port`, carry bytes both ways, and
// return the accepted connection's path. The peer dials once the current
// tunnel of `hop` holds a listener on `port`, so after a tunnel replacement
// it waits for the listener to bind inside the new tunnel.
async fn accept_from(
    listener: &dyn InboundListener,
    hop: &SessionHop,
    peer: &crate::test_support::WireGuardTestPeer,
    port: u16,
) -> Dialed {
    let provider = hop.resolver.as_ref().expect("a WireGuard hop");
    let (accepted, dialed) = tokio::join!(listener.accept(), async {
        while listener.local_addr().port() != port || !provider.listens_on(port).await {
            tokio::task::yield_now().await;
        }
        peer.dial_client(port).await
    });
    let mut accepted = accepted.unwrap();
    let mut dialed = dialed.unwrap();
    dialed.write_all(b"handshake").await.unwrap();
    let mut heard = [0u8; 9];
    accepted.stream.read_exact(&mut heard).await.unwrap();
    assert_eq!(&heard, b"handshake");
    accepted.stream.write_all(b"bitfield").await.unwrap();
    let mut answer = [0u8; 8];
    dialed.read_exact(&mut answer).await.unwrap();
    assert_eq!(&answer, b"bitfield");
    accepted
}

// A chain of three WireGuard hops, each tunnel carried inside the one
// before. Only the chain's last hop listens: a peer beyond it connects and
// bytes cross both ways, while the outer hops' far sides find nothing on
// that port. The last hop's gateway forwards the port over NAT-PMP asked
// inside the last tunnel, and names resolve there too. Replacing the
// tunnel of any hop brings the listener down and back on the same port.
#[tokio::test]
async fn a_three_hop_wireguard_chain_listens_only_on_its_last_hop() {
    use crate::test_support::{
        TEST_CLIENT_ADDRESS, WireGuardTestPeer, WireGuardTestPeerOptions, test_key,
    };
    const MAPPED: u16 = 47001;
    let gateway = crate::test_support::natpmp::NatPmpGateway::start(MAPPED).await;
    let exit = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        private_key: test_key(43),
        udp_forward: Some((natpmp::NATPMP_PORT, gateway.address())),
        ..WireGuardTestPeerOptions::default()
    })
    .await;
    let middle = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        private_key: test_key(42),
        udp_forward: Some((CHAINED_PORT, exit.endpoint())),
        ..WireGuardTestPeerOptions::default()
    })
    .await;
    let entry = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        private_key: test_key(41),
        udp_forward: Some((CHAINED_PORT, middle.endpoint())),
        ..WireGuardTestPeerOptions::default()
    })
    .await;

    let first = listening_session(&entry);
    let second = chained_on(&first, &middle, 2, vec![1]);
    let last = forwarding(
        chained_on(&second, &exit, 3, vec![1, 2]),
        PortForwarding::NatPmp {
            gateway: gateway_of(TEST_CLIENT_ADDRESS),
        },
    );

    let listener = Arc::clone(&last)
        .inbound()
        .expect("the chain's last hop takes connections")
        .listen(0, true)
        .await
        .unwrap();
    let mut mapping = listener.mapping().expect("its gateway forwards a port");
    mapping
        .wait_for(|mapping| mapping.external_port == Some(MAPPED))
        .await
        .unwrap();

    let accepted = accept_from(&*listener, &last, &exit, MAPPED).await;
    assert_eq!(accepted.path.proxies, vec![1, 2, 3]);
    assert_eq!(accepted.source.map(|source| source.port()), Some(MAPPED));
    drop(accepted);

    // The outer hops' far sides find no listener on that port.
    assert!(entry.dial_client(MAPPED).await.is_err());
    assert!(middle.dial_client(MAPPED).await.is_err());

    // The gateway was asked inside the last tunnel only: the outer hops'
    // relays carried nothing but WireGuard, and the exit's relay carried
    // the NAT-PMP requests from the last tunnel's address.
    assert!(gateway.heard() >= 3);
    for relay in [&entry, &middle] {
        for (source, seen) in relay.udp_forwarded() {
            assert_eq!(seen.wireguard_datagrams, seen.datagrams, "{source}");
        }
    }
    let asked = exit.udp_forwarded();
    assert!(!asked.is_empty());
    for (source, seen) in &asked {
        assert_eq!(source.ip(), IpAddr::V4(TEST_CLIENT_ADDRESS));
        assert_eq!(seen.wireguard_datagrams, 0, "{source}");
    }

    // Datagrams ride the last hop's UDP: names resolve beyond the exit.
    let Resolution::Addresses(_) = last.resolve("origin.tunnel.test").await.unwrap() else {
        panic!("the last hop resolves names");
    };
    assert_eq!(exit.dns_queries(), vec!["origin.tunnel.test"]);
    assert!(entry.dns_queries().is_empty());
    assert!(middle.dns_queries().is_empty());

    // Replace each hop's tunnel in turn, outermost first. Each replacement
    // stops the tunnels stacked on it; the gateway is asked again inside the
    // rebuilt last tunnel, and the listener comes back on the same port.
    // The re-map proves the last hop's current tunnel is the rebuilt one, so
    // the peer dials only once the listener is bound inside it.
    for hop in [&first, &second, &last] {
        mapping.borrow_and_update();
        hop.provider.retire().await;
        mapping.changed().await.unwrap();
        assert_eq!(mapping.borrow().external_port, Some(MAPPED));
        let accepted = accept_from(&*listener, &last, &exit, MAPPED).await;
        assert_eq!(accepted.path.proxies, vec![1, 2, 3]);
        assert_eq!(listener.local_addr().port(), MAPPED);
    }
}

// A provider that always forwards the same port: the listener peers reach
// through it listens on that port, any other listener where it asked.
#[tokio::test]
async fn a_fixed_forwarded_port_is_listened_on_by_the_forwarded_listener_only() {
    use crate::test_support::WireGuardTestPeer;
    const FORWARDED: u16 = 46001;
    let near = WireGuardTestPeer::start().await;
    let session = forwarding(listening_session(&near), PortForwarding::Fixed(FORWARDED));
    let inbound = Arc::clone(&session).inbound().unwrap();

    let forwarded = inbound.listen(0, true).await.unwrap();
    assert_eq!(forwarded.local_addr().port(), FORWARDED);
    assert!(forwarded.mapping().is_none());
    let accepted = accept_from(&*forwarded, &session, &near, FORWARDED).await;
    assert_eq!(accepted.path.proxies, vec![1]);

    let other = inbound.listen(0, false).await.unwrap();
    assert_ne!(other.local_addr().port(), FORWARDED);
    assert_ne!(other.local_addr().port(), 0);
}

// A revoked leg has no route and binds nothing.
#[test]
fn a_revoked_leg_reports_no_route() {
    let leg = Revocable::new(bottom());
    let to: SocketAddr = "127.0.0.1:9".parse().unwrap();
    assert!(matches!(leg.source_toward(to), Some(Ok(_))));
    leg.revoke();
    assert!(matches!(leg.source_toward(to), Some(Err(_))));
    assert!(matches!(
        leg.bind_local_udp("127.0.0.1:0".parse().unwrap()),
        Some(Err(_))
    ));
}

// Gives torrents every byte of the sides and usenet none.
struct TorrentsOnly;

impl SocketDivider for TorrentsOnly {
    fn shares(&self, cap: u64) -> [u64; SIDES] {
        [0, cap]
    }
    fn share_out(&self, cap: u64, _: &[u64], _: &[Option<u64>], _: &[u64]) -> Vec<u64> {
        vec![0, cap]
    }
    fn presses(&self, used: u64, given: u64) -> bool {
        used >= given
    }
}

// Each class holds its own part of a WireGuard tunnel's bytes. A full part
// is at capacity, a dropped stream's bytes stay held while it closes, and
// the reserve still dials.
#[tokio::test]
async fn a_full_socket_share_is_at_capacity_and_leaves_the_others_alone() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let provider = Arc::new(
        crate::WireGuardTunnelProvider::new(
            near.client_spec("1"),
            Arc::new(crate::NoopTunnelObserver),
        )
        .with_udp_factory(bottom()),
    );
    let stream = stream_bytes(None);
    let mtu = near.client_spec("1").mtu;
    let session = Arc::new(SessionHop {
        sockets: Arc::new(SessionSockets::new(
            SocketBudget {
                bytes: overhead_bytes(mtu) + 2 * stream,
                reserve: stream,
                torrent_kept: 0,
                mtu,
            },
            Arc::new(TorrentsOnly),
        )),
        ..Arc::into_inner(wireguard_session(
            1,
            provider.clone(),
            provider,
            bottom(),
            vec![],
            1,
        ))
        .unwrap()
    });
    let target = |purpose| Target {
        purpose,
        ..probe_target()
    };

    assert!(session.has_room(&Purpose::Torrent));
    let peer = session.dial(&target(Purpose::Torrent)).await.unwrap();
    assert!(!session.has_room(&Purpose::Torrent));
    assert_eq!(session.sockets.held(SocketClass::Torrent), stream);
    assert!(matches!(
        session.dial(&target(Purpose::Torrent)).await,
        Err(DialError::AtCapacity(_))
    ));
    // A web seed transfer and the DHT are torrents' too.
    assert!(matches!(
        session.dial(&target(Purpose::TorrentWebSeed)).await,
        Err(DialError::AtCapacity(_))
    ));
    let datagrams = Arc::clone(&session).datagrams().unwrap();
    let far: SocketAddr = "192.0.2.50:6881".parse().unwrap();
    assert!(datagrams.bind(far).await.is_err());
    // The reserve is off the split: with the torrent side full, an announce
    // dials at once.
    let announce = session.dial(&target(Purpose::TorrentHttp)).await.unwrap();
    assert_eq!(session.sockets.held(SocketClass::Reserve), stream);
    assert!(matches!(
        session.dial(&target(Purpose::Feed { feed: 2 })).await,
        Err(DialError::AtCapacity(_))
    ));
    assert!(matches!(
        session
            .dial(&target(Purpose::Nntp {
                server: 1,
                leg: 0,
                probing: false,
            }))
            .await,
        Err(DialError::AtCapacity(_))
    ));

    // A dropped stream's socket stays in the tunnel while it closes, and its
    // bytes with it, counted closing.
    drop(peer);
    assert_eq!(session.sockets.held(SocketClass::Torrent), stream);
    assert_eq!(session.sockets.closing(SocketClass::Torrent), stream);
    assert!(!session.has_room(&Purpose::Torrent));
    drop(announce);
    assert_eq!(session.sockets.closing(SocketClass::Reserve), stream);
}

// A WireGuard session on `near` whose reserve holds one DNS stream and whose
// torrent side holds one peer.
fn dns_session(near: &crate::test_support::WireGuardTestPeer) -> Arc<SessionHop> {
    dns_session_of(near, 1)
}

// As `dns_session`, its reserve holding `streams` DNS streams.
fn dns_session_of(near: &crate::test_support::WireGuardTestPeer, streams: u64) -> Arc<SessionHop> {
    let provider = Arc::new(
        crate::WireGuardTunnelProvider::new(
            near.client_spec("1"),
            Arc::new(crate::NoopTunnelObserver),
        )
        .with_udp_factory(bottom()),
    );
    let mtu = near.client_spec("1").mtu;
    let reserve = streams * super::dns_stream_bytes();
    Arc::new(SessionHop {
        sockets: Arc::new(SessionSockets::new(
            SocketBudget {
                bytes: overhead_bytes(mtu) + reserve + stream_bytes(None),
                reserve,
                torrent_kept: 0,
                mtu,
            },
            Arc::new(TorrentsOnly),
        )),
        ..Arc::into_inner(wireguard_session(
            1,
            provider.clone(),
            provider,
            bottom(),
            vec![],
            1,
        ))
        .unwrap()
    })
}

// Only a DNS dial records the bytes its socket reserved: a connection is
// kept only after one. A peer's dial records nothing and leaves the
// session's keeper unmade.
#[tokio::test]
async fn only_a_dns_dial_records_what_its_socket_reserved() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let session = dns_session(&near);
    let target = |purpose| Target {
        purpose,
        ..probe_target()
    };
    let peer = session.dial(&target(Purpose::Torrent)).await.unwrap();
    assert!(peer.outcome.reservations().is_empty());
    assert!(!session.sockets.keeper().asked_for());

    let dns = session.dial(&target(Purpose::Dns)).await.unwrap();
    let reserved = dns.outcome.reservations();
    assert_eq!(reserved.len(), 1);
    assert_eq!(
        reserved[0].socket.held(),
        (SocketClass::Reserve, super::dns_stream_bytes())
    );
    assert!(reserved[0].keeper.is(&session.sockets.keeper().handle()));
    // Its tunnel socket can be reset, to free it at once when reclaimed.
    assert!(reserved[0].socket.aborts());
    drop(peer);
}

// A DNS dial on a reserve held whole by a kept connection closes it, resets
// its socket and takes its bytes, rather than fail: kept, the connection
// makes nothing fail. With the reserve held by a connection in use, the dial
// is refused at once.
#[tokio::test]
async fn a_dial_on_a_reserve_held_by_a_kept_connection_takes_its_bytes() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let session = dns_session(&near);
    let target = Target {
        purpose: Purpose::Dns,
        ..probe_target()
    };
    let in_use = session.dial(&target).await.unwrap();
    assert!(matches!(
        session.dial(&target).await,
        Err(DialError::AtCapacity(_))
    ));

    // The lookup ends and keeps its connection.
    let reserved = in_use.outcome.reservations().remove(0);
    let link = Arc::new(Mutex::new(Some(in_use)));
    let closing = link.clone();
    let _kept = reserved
        .keeper
        .hold(
            Some(reserved.socket),
            Box::new(move || closing.lock().unwrap().take().is_some()),
        )
        .expect("a live keeper");
    let next = session.dial(&target).await.unwrap();
    assert!(link.lock().unwrap().is_none());
    assert_eq!(session.sockets.keeper().holding(), 0);
    assert_eq!(
        session.sockets.held(SocketClass::Reserve),
        super::dns_stream_bytes()
    );
    drop(next);
}

// Keep `dialed` as a lookup that ends keeps its connection: with the keeper
// of each stage in `rides`, and reclaimable in the share whose keeper its
// socket reserved bytes with. Closing it drops it, unless `in_use` says a
// lookup took it back first.
fn keep_dialed(
    dialed: Dialed,
    rides: &[KeeperRef],
    in_use: &Arc<std::sync::atomic::AtomicBool>,
) -> (Vec<KeptRegistration>, Arc<Mutex<Option<Dialed>>>) {
    let reserved = dialed.outcome.reservations();
    let link = Arc::new(Mutex::new(Some(dialed)));
    let registered = rides
        .iter()
        .map(|keeper| {
            let socket = reserved
                .iter()
                .find(|reserved| reserved.keeper.is(keeper))
                .map(|reserved| reserved.socket.clone());
            let (closing, in_use) = (link.clone(), in_use.clone());
            keeper
                .hold(
                    socket,
                    Box::new(move || {
                        !in_use.load(std::sync::atomic::Ordering::SeqCst)
                            && closing.lock().unwrap().take().is_some()
                    }),
                )
                .expect("a live keeper")
        })
        .collect();
    (registered, link)
}

// A kept DNS connection whose far side has gone silent, closed to make room
// for a claim that does not fit: its socket is reset, so the claim takes its
// bytes at once, with no reply from the far side and no time passing. Closed
// gracefully, the socket would linger until the far side answered its FIN or
// the tunnel's linger ran out, and the claim would wait. The share never
// holds past its budget, and ends where it began.
#[tokio::test]
async fn a_kept_dns_connection_reclaimed_gives_its_bytes_back_at_once() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let session = dns_session(&near);
    let sockets = &session.sockets;
    let one = super::dns_stream_bytes();
    let before = sockets.reserved();
    let within = || assert!(sockets.reserved() <= sockets.limit());
    let dialed = session
        .dial(&Target {
            purpose: Purpose::Dns,
            ..probe_target()
        })
        .await
        .unwrap();
    within();
    let (_kept, link) = keep_dialed(dialed, &[sockets.keeper().handle()], &Default::default());
    assert_eq!(sockets.held(SocketClass::Reserve), one);
    within();

    // The far side goes: nothing will answer the close.
    drop(near);
    tokio::time::pause();
    let asked = tokio::time::Instant::now();
    let permit = sockets
        .take(SocketClass::Reserve, one)
        .expect("room: the kept connection's bytes are back")
        .expect("a budget");
    assert_eq!(tokio::time::Instant::now(), asked);
    assert!(link.lock().unwrap().is_none());
    assert_eq!(sockets.keeper().holding(), 0);
    within();
    assert_eq!(sockets.held(SocketClass::Reserve), one);
    assert_eq!(sockets.closing(SocketClass::Reserve), 0);

    drop(permit);
    assert_eq!(sockets.reserved(), before);
    assert_eq!(sockets.held(SocketClass::Reserve), 0);
    assert_eq!(sockets.closing(SocketClass::Reserve), 0);
}

// A kept DNS connection a lookup takes back as the share reclaims it is in
// use: it is not reset, and still carries its exchange. The next oldest is
// closed in its place, reset, and its bytes are back at once.
#[tokio::test]
async fn a_kept_connection_taken_back_is_never_reset_and_the_next_is_closed() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let session = dns_session_of(&near, 2);
    let sockets = &session.sockets;
    let one = super::dns_stream_bytes();
    let target = Target {
        purpose: Purpose::Dns,
        ..probe_target()
    };
    let older = session.dial(&target).await.unwrap();
    let newer = session.dial(&target).await.unwrap();
    let rides = [sockets.keeper().handle()];
    let taken = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (_older, older) = keep_dialed(older, &rides, &taken);
    let (_newer, newer) = keep_dialed(newer, &rides, &Default::default());

    let permit = sockets
        .take(SocketClass::Reserve, one)
        .expect("room: the kept connection's bytes are back")
        .expect("a budget");
    assert!(newer.lock().unwrap().is_none());
    assert_eq!(sockets.held(SocketClass::Reserve), 2 * one);
    assert_eq!(sockets.closing(SocketClass::Reserve), 0);

    // The connection the lookup took back still works.
    let mut in_use = older.lock().unwrap().take().expect("never closed");
    in_use
        .stream
        .write_all(b"GET /kept HTTP/1.1\r\nHost: origin\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    in_use.stream.read_to_end(&mut answer).await.unwrap();
    assert!(String::from_utf8_lossy(&answer).starts_with("HTTP/1.1 200"));
    assert_eq!(near.requests(), vec!["GET /kept HTTP/1.1"]);
    drop((permit, in_use));
}

// A kept connection on a tunnel stream, held until it is closed.
type FarLink = Arc<Mutex<Option<(crate::WgTcpStream, StreamSocket)>>>;

// A connection `near` opens into `provider`'s tunnel, kept as a DNS
// connection is, on bytes of `sockets`' reserve marked reclaimable: the far
// side's end, its registration, and the kept connection.
async fn kept_from_far(
    near: &crate::test_support::WireGuardTestPeer,
    provider: &crate::WireGuardTunnelProvider,
    sockets: &SessionSockets,
) -> (crate::WgTcpStream, KeptRegistration, FarLink) {
    let mut listener = provider.listen(0, 1).await.unwrap();
    let port = listener.local_addr().port();
    let (accepted, far) = tokio::join!(listener.accept(), near.dial_client(port));
    let (stream, far) = (accepted.unwrap(), far.unwrap());
    let permit = sockets
        .take(SocketClass::Reserve, super::dns_stream_bytes())
        .expect("room")
        .expect("a budget");
    let mut socket = StreamSocket::new(Some(permit));
    let reclaimable = socket
        .reclaimable(SocketClass::Reserve)
        .expect("bytes held");
    socket.hold_in(&stream);
    assert!(socket.aborts() && reclaimable.aborts());
    let link = Arc::new(Mutex::new(Some((stream, socket))));
    let closing = link.clone();
    let kept = sockets
        .keeper()
        .handle()
        .hold(
            Some(reclaimable),
            Box::new(move || closing.lock().unwrap().take().is_some()),
        )
        .expect("a live keeper");
    (far, kept, link)
}

// A connection kept from `near` into `provider`'s tunnel, closed by `close`:
// the far side reads a FIN, and the bytes stay held, closing, until the far
// side ends its side too.
async fn closes_gracefully(
    near: &crate::test_support::WireGuardTestPeer,
    provider: &crate::WireGuardTunnelProvider,
    close: impl FnOnce(&SessionSockets, KeptRegistration, &FarLink),
) {
    let one = super::dns_stream_bytes();
    let sockets = super::reserve_of(1);
    let (mut far, kept, link) = kept_from_far(near, provider, &sockets).await;
    close(&sockets, kept, &link);
    assert!(link.lock().unwrap().is_none());
    assert_eq!(sockets.held(SocketClass::Reserve), one);
    assert_eq!(sockets.closing(SocketClass::Reserve), one);
    let mut rest = Vec::new();
    far.read_to_end(&mut rest)
        .await
        .expect("a FIN, not a reset");
    assert!(rest.is_empty());
    drop(far);
    until_given_back(&sockets, SocketClass::Reserve).await;
    assert_eq!(sockets.closing(SocketClass::Reserve), 0);
}

// A kept connection closed for anything but room ends gracefully: idle past
// its time, as the sweep closes it, or cut, as when its stage retires. The
// far side reads a FIN, not a reset, and the bytes stay held, closing, until
// it ends its side too. One closed for room is reset, and its bytes are back
// at once, though the far side has not closed.
#[tokio::test]
async fn only_a_kept_connection_closed_for_room_is_reset() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let provider = crate::WireGuardTunnelProvider::new(
        near.client_spec("1"),
        Arc::new(crate::NoopTunnelObserver),
    )
    .with_udp_factory(bottom());
    let one = super::dns_stream_bytes();
    // Idle: the sweep drops the connection, and with it the registration.
    closes_gracefully(&near, &provider, |_, kept, link| {
        let idle = link.lock().unwrap().take();
        drop((idle, kept));
    })
    .await;
    // Cut by its stage.
    closes_gracefully(&near, &provider, |sockets, _kept, _| sockets.keeper().cut()).await;

    // Closed for room.
    let sockets = super::reserve_of(1);
    let (far, _kept, link) = kept_from_far(&near, &provider, &sockets).await;
    let permit = sockets
        .take(SocketClass::Reserve, one)
        .expect("room")
        .expect("a budget");
    assert!(link.lock().unwrap().is_none());
    assert_eq!(sockets.held(SocketClass::Reserve), one);
    assert_eq!(sockets.closing(SocketClass::Reserve), 0);
    drop(permit);
    assert_eq!(sockets.held(SocketClass::Reserve), 0);
    drop(far);
}

// Only a stream marked reclaimable, as only a DNS dial marks its socket,
// keeps what resets its tunnel socket: any other stream, an accepted one
// among them, makes none.
#[tokio::test]
async fn only_a_stream_a_kept_connection_may_ride_can_be_reset() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let provider = crate::WireGuardTunnelProvider::new(
        near.client_spec("1"),
        Arc::new(crate::NoopTunnelObserver),
    )
    .with_udp_factory(bottom());
    let sockets = super::reserve_of(1);
    let mut listener = provider.listen(0, 1).await.unwrap();
    let port = listener.local_addr().port();
    let (accepted, far) = tokio::join!(listener.accept(), near.dial_client(port));
    let stream = accepted.unwrap();
    let mut socket = StreamSocket::new(Some(
        sockets
            .take(SocketClass::Reserve, super::dns_stream_bytes())
            .expect("room")
            .expect("a budget"),
    ));
    socket.hold_in(&stream);
    assert!(!socket.aborts());
    drop((stream, socket, far));
}

// A DNS connection through a WireGuard tunnel carried by another holds a
// socket only in the top tunnel's share: the tunnel beneath carries its
// packets on the one datagram socket the top tunnel rides, and records
// nothing. Closed for room, the kept connection resets its own socket alone,
// its bytes are back in the top share at once, and the tunnel beneath and
// its carrier are untouched: the next dial through both reaches the far
// peer.
#[tokio::test]
async fn a_kept_connection_through_two_tunnels_resets_only_its_own_socket() {
    use crate::test_support::{
        TEST_PEER_ADDRESS, TEST_PEER_HTTP_PORT, WireGuardTestPeer, WireGuardTestPeerOptions,
        test_key,
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
    let lower_provider = Arc::new(
        crate::WireGuardTunnelProvider::new(
            near.client_spec("1"),
            Arc::new(crate::NoopTunnelObserver),
        )
        .with_udp_factory(bottom()),
    );
    let mtu = near.client_spec("1").mtu;
    let carrying = 2 * datagram_bytes(mtu);
    let lower = Arc::new(SessionHop {
        sockets: Arc::new(SessionSockets::new(
            SocketBudget {
                bytes: overhead_bytes(mtu) + carrying,
                reserve: carrying,
                torrent_kept: 0,
                mtu,
            },
            Arc::new(TorrentsOnly),
        )),
        ..Arc::into_inner(wireguard_session(
            1,
            lower_provider.clone(),
            lower_provider,
            bottom(),
            vec![],
            1,
        ))
        .unwrap()
    });
    let carrier = lower
        .clone()
        .carrier_datagrams()
        .expect("a WireGuard session carries a tunnel");
    let spec = crate::WireGuardSpec {
        endpoint_host: TEST_PEER_ADDRESS.to_string(),
        endpoint_port: CARRIED_PORT,
        ..far.client_spec("2")
    };
    let upper_provider = Arc::new(
        crate::WireGuardTunnelProvider::new(spec.clone(), Arc::new(crate::NoopTunnelObserver))
            .with_datagram_transport(carrier),
    );
    let one = super::dns_stream_bytes();
    let upper = Arc::new(SessionHop {
        sockets: Arc::new(SessionSockets::new(
            SocketBudget {
                bytes: overhead_bytes(spec.mtu) + one,
                reserve: one,
                torrent_kept: 0,
                mtu: spec.mtu,
            },
            Arc::new(TorrentsOnly),
        )),
        ..Arc::into_inner(wireguard_session(
            2,
            upper_provider.clone(),
            upper_provider,
            lower.clone(),
            vec![1],
            1,
        ))
        .unwrap()
    });
    let target = Target {
        host: TEST_PEER_ADDRESS.to_string(),
        port: TEST_PEER_HTTP_PORT,
        purpose: Purpose::Dns,
        addresses: Vec::new(),
    };
    let dialed = upper.dial(&target).await.unwrap();
    let reserved = dialed.outcome.reservations();
    assert_eq!(reserved.len(), 1);
    assert!(reserved[0].keeper.is(&upper.sockets.keeper().handle()));
    assert!(reserved[0].socket.aborts());
    assert!(!lower.sockets.keeper().asked_for());
    let carried = lower.sockets.held(SocketClass::Reserve);
    assert!(carried > 0);

    // Kept with both stages' keepers, as a naming keeps it.
    let rides = upper.keepers();
    assert!(rides[1].is(&lower.sockets.keeper().handle()));
    let (_kept, link) = keep_dialed(dialed, &rides, &Default::default());
    assert!(lower.sockets.keeper().held().is_empty());
    let permit = upper
        .sockets
        .take(SocketClass::Reserve, one)
        .expect("room")
        .expect("a budget");
    assert!(link.lock().unwrap().is_none());
    assert_eq!(upper.sockets.held(SocketClass::Reserve), one);
    assert_eq!(upper.sockets.closing(SocketClass::Reserve), 0);
    assert_eq!(lower.sockets.held(SocketClass::Reserve), carried);
    assert_eq!(lower.sockets.closing(SocketClass::Reserve), 0);
    drop(permit);

    let mut next = upper.dial(&target).await.unwrap();
    next.stream
        .write_all(b"GET /carried HTTP/1.1\r\nHost: origin\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    next.stream.read_to_end(&mut answer).await.unwrap();
    assert!(String::from_utf8_lossy(&answer).ends_with("carried through two tunnels"));
    drop(next);
}

// A WireGuard session on `near` whose tunnel holds a reserve socket, a
// listener's backlog and two torrent peers, and its tunnel.
fn budgeted_session(
    near: &crate::test_support::WireGuardTestPeer,
) -> (Arc<SessionHop>, Arc<crate::WireGuardTunnelProvider>) {
    let provider = Arc::new(
        crate::WireGuardTunnelProvider::new(
            near.client_spec("1"),
            Arc::new(crate::NoopTunnelObserver),
        )
        .with_udp_factory(bottom()),
    );
    let stream = stream_bytes(None);
    let mtu = near.client_spec("1").mtu;
    let session = Arc::new(SessionHop {
        sockets: Arc::new(SessionSockets::new(
            SocketBudget {
                bytes: overhead_bytes(mtu) + 3 * stream + listener_bytes(),
                reserve: stream,
                torrent_kept: listener_bytes(),
                mtu,
            },
            Arc::new(TorrentsOnly),
        )),
        ..Arc::into_inner(wireguard_session(
            1,
            provider.clone(),
            provider.clone(),
            bottom(),
            vec![],
            1,
        ))
        .unwrap()
    });
    (session, provider)
}

// Until the tunnel holds none of `class`'s bytes: woken as bytes come back,
// on the slot every give-back rings.
async fn until_given_back(sockets: &SessionSockets, class: SocketClass) {
    let past = sockets.budget().expect("a budget").bytes + 1;
    let freed = sockets
        .room(SocketClass::Reserve, past)
        .expect_err("no room past the tunnel's bytes")
        .notify();
    loop {
        let back = freed.notified();
        tokio::pin!(back);
        back.as_mut().enable();
        if sockets.held(class) == 0 {
            return;
        }
        back.await;
    }
}

// A dialled stream's bytes stay held after it is dropped, while its socket
// closes in the tunnel, and come back when the tunnel frees the socket once
// the far side finished the close.
#[tokio::test]
async fn a_dropped_streams_bytes_come_back_when_the_tunnel_frees_its_socket() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let (session, _provider) = budgeted_session(&near);
    let stream = stream_bytes(None);
    let peer = session
        .dial(&Target {
            purpose: Purpose::Torrent,
            ..probe_target()
        })
        .await
        .unwrap();
    assert_eq!(session.sockets.held(SocketClass::Torrent), stream);
    assert_eq!(session.sockets.closing(SocketClass::Torrent), 0);

    // Nothing runs the tunnel between the drop and the reads: the socket is
    // retired, not freed.
    drop(peer);
    assert_eq!(session.sockets.held(SocketClass::Torrent), stream);
    assert_eq!(session.sockets.closing(SocketClass::Torrent), stream);
    assert_eq!(session.sockets.owed(SocketClass::Torrent), 0);

    until_given_back(&session.sockets, SocketClass::Torrent).await;
    assert_eq!(session.sockets.closing(SocketClass::Torrent), 0);
}

// A stream dropped while its far side answers nothing stays closing in the
// tunnel, its bytes held; stopping the tunnel frees the socket and gives them
// back.
#[tokio::test]
async fn a_closing_streams_bytes_come_back_when_its_tunnel_stops() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let (session, provider) = budgeted_session(&near);
    let stream = stream_bytes(None);
    let peer = session
        .dial(&Target {
            purpose: Purpose::Torrent,
            ..probe_target()
        })
        .await
        .unwrap();
    // The far side goes: nothing will finish the close.
    drop(near);
    drop(peer);
    assert_eq!(session.sockets.held(SocketClass::Torrent), stream);
    assert_eq!(session.sockets.closing(SocketClass::Torrent), stream);

    // Retiring the tunnel waits for its engine to end, which stops its
    // stack and frees every socket still closing.
    TunnelProvider::retire(&*provider).await;
    assert_eq!(session.sockets.held(SocketClass::Torrent), 0);
    assert_eq!(session.sockets.closing(SocketClass::Torrent), 0);
}

// A dial the far side refuses holds nothing: its socket was freed before the
// dial returned, and its bytes with it.
#[tokio::test]
async fn a_failed_dial_holds_nothing() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let (session, _provider) = budgeted_session(&near);
    let refused = session
        .dial(&Target {
            port: 9,
            purpose: Purpose::Torrent,
            ..probe_target()
        })
        .await;
    assert!(refused.is_err(), "a closed port took a connection");
    assert_eq!(session.sockets.held(SocketClass::Torrent), 0);
    assert_eq!(session.sockets.closing(SocketClass::Torrent), 0);
}

// An accepted stream's bytes, taken before the accept, are the tunnel's once
// it accepted: held after the stream is dropped, and back when the tunnel
// frees the socket. The listener's backlog comes back as it is dropped.
#[tokio::test]
async fn an_accepted_streams_bytes_come_back_when_the_tunnel_frees_its_socket() {
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let (session, _provider) = budgeted_session(&near);
    let stream = stream_bytes(None);
    let listener = Arc::clone(&session)
        .inbound()
        .expect("a WireGuard session takes connections")
        .listen(0, true)
        .await
        .unwrap();
    assert_eq!(
        session.sockets.held(SocketClass::TorrentSession),
        listener_bytes()
    );
    let port = listener.local_addr().port();
    let (accepted, far) = tokio::join!(listener.accept(), near.dial_client(port));
    let accepted = accepted.unwrap();
    let far = far.unwrap();
    assert_eq!(session.sockets.held(SocketClass::Torrent), stream);

    drop(accepted);
    assert_eq!(session.sockets.held(SocketClass::Torrent), stream);
    assert_eq!(session.sockets.closing(SocketClass::Torrent), stream);
    // The far side closes too, and the close finishes.
    drop(far);
    until_given_back(&session.sockets, SocketClass::Torrent).await;

    drop(listener);
    assert_eq!(session.sockets.held(SocketClass::TorrentSession), 0);
}

// A stage that records the purpose of every dial and fails it, with room in
// every class but the reserve.
#[derive(Default)]
struct RecordsPurpose(Mutex<Vec<Purpose>>);

#[async_trait::async_trait]
impl Dialer for RecordsPurpose {
    fn room(&self, purpose: &Purpose) -> Result<(), Vec<Arc<tokio::sync::Notify>>> {
        if SocketClass::of(purpose) == SocketClass::Reserve {
            Err(Vec::new())
        } else {
            Ok(())
        }
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        self.0.lock().unwrap().push(target.purpose.clone());
        Err(DialError::Egress(std::io::Error::other("recorded")))
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(30)
    }
    fn describe(&self) -> String {
        "records purpose".into()
    }
}

// A connection through a SOCKS or HTTP proxy opens one endpoint connection
// in the tunnel beneath, which counts against that connection's own share:
// usenet against usenet, a TCP peer against torrent peers, however many
// proxies it passes. Whether a dial has room is asked of the tunnel beneath.
#[tokio::test]
async fn a_chained_dial_takes_its_own_share_of_the_tunnel_beneath() {
    let beneath = Arc::new(RecordsPurpose::default());
    let first = hop(7, "192.0.2.1:1080".parse().unwrap(), beneath.clone());
    let chain = hop(8, "192.0.2.2:8080".parse().unwrap(), first);
    let mut nntp = target("192.0.2.9:119".parse().unwrap());
    nntp.purpose = Purpose::Nntp {
        server: 1,
        leg: 0,
        probing: false,
    };
    let peer = Target {
        purpose: Purpose::Torrent,
        ..nntp.clone()
    };
    let _ = chain.dial(&nntp).await;
    let _ = chain.dial(&peer).await;
    let classes: Vec<_> = beneath
        .0
        .lock()
        .unwrap()
        .iter()
        .map(SocketClass::of)
        .collect();
    assert_eq!(classes, [SocketClass::Nntp, SocketClass::Torrent]);
    assert!(chain.has_room(&Purpose::Torrent));
    assert!(!chain.has_room(&Purpose::Feed { feed: 1 }));
}

// A rung whose share has room, or is full and says what frees it.
struct Roomy(Option<Arc<Notify>>);
#[async_trait::async_trait]
impl Dialer for Roomy {
    fn room(&self, _: &Purpose) -> Result<(), Vec<Arc<Notify>>> {
        self.0.clone().map_or(Ok(()), |freed| Err(vec![freed]))
    }
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        Err(DialError::Egress(io::Error::other("not dialled")))
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "roomy".into()
    }
}

// A probe dials the rung still cooling, so its room is that rung's; an
// ordinary dial skips the cooling rung, so its room is the next one's.
#[tokio::test(start_paused = true)]
async fn a_probes_room_is_the_cooling_rung_it_dials() {
    let freed = Arc::new(Notify::new());
    let ladder = Fallback::new(vec![
        Arc::new(Roomy(None)),
        Arc::new(Roomy(Some(freed.clone()))),
    ]);
    ladder.report_rung(0, Some(&DialError::Egress(io::Error::other("path down"))));
    let nntp = |probing| Purpose::Nntp {
        server: 1,
        leg: 0,
        probing,
    };
    let waits = ladder.room(&nntp(false)).unwrap_err();
    assert!(Arc::ptr_eq(&waits[0], &freed));
    assert!(ladder.room(&nntp(true)).is_ok());
}

// An ordinary dial skips a cooling rung, so with the next rung full it waits
// on that rung's share and on the cooling rung's cooldown: the ladder
// raises that signal when the cooldown ends, and the rung has room again.
#[tokio::test(start_paused = true)]
async fn a_dial_that_skipped_a_cooling_rung_wakes_when_its_cooldown_ends() {
    let freed = Arc::new(Notify::new());
    let ladder = Fallback::new(vec![
        Arc::new(Roomy(None)),
        Arc::new(Roomy(Some(freed.clone()))),
    ]);
    ladder.report_rung(0, Some(&DialError::Egress(io::Error::other("path down"))));
    let nntp = Purpose::Nntp {
        server: 1,
        leg: 0,
        probing: false,
    };
    let waits = ladder.room(&nntp).unwrap_err();
    assert!(waits.iter().any(|wait| Arc::ptr_eq(wait, &freed)));
    let cooled: Vec<_> = waits
        .iter()
        .filter(|wait| !Arc::ptr_eq(wait, &freed))
        .cloned()
        .collect();
    assert_eq!(cooled.len(), 1);
    let notified = cooled[0].notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    assert!(ladder.room(&nntp).is_err());
    notified.await;
    assert!(ladder.room(&nntp).is_ok());
    assert_eq!(ladder.rung_states()[0], "FAILING");
}

// A rung that is cooling and full gives both of its signals.
#[tokio::test(start_paused = true)]
async fn a_cooling_rung_that_is_full_gives_its_share_signal_too() {
    let skipped = Arc::new(Notify::new());
    let freed = Arc::new(Notify::new());
    let ladder = Fallback::new(vec![
        Arc::new(Roomy(Some(skipped.clone()))),
        Arc::new(Roomy(Some(freed.clone()))),
    ]);
    ladder.report_rung(0, Some(&DialError::Egress(io::Error::other("path down"))));
    let waits = ladder
        .room(&Purpose::Nntp {
            server: 1,
            leg: 0,
            probing: false,
        })
        .unwrap_err();
    assert_eq!(waits.len(), 3);
    assert!(waits.iter().any(|wait| Arc::ptr_eq(wait, &skipped)));
    assert!(waits.iter().any(|wait| Arc::ptr_eq(wait, &freed)));
}

// Records each turn a watched hop tells of.
#[derive(Default)]
struct Turns(Mutex<Vec<Option<ProxyFault>>>);
impl HopWatcher for Turns {
    fn working(&self) {
        self.0.lock().unwrap().push(None);
    }
    fn failing(&self, fault: &ProxyFault) {
        self.0.lock().unwrap().push(Some(fault.clone()));
    }
}
impl Turns {
    fn taken(&self) -> Vec<Option<ProxyFault>> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

// A watched hop tells only of turns: working once however many dials work,
// failing once however many fail, and nothing for a dial that never tried.
#[test]
fn a_watched_hop_tells_each_turn_once() {
    let turns = Arc::new(Turns::default());
    let failure = HopFailure::watched(turns.clone());
    let closed = || ProxyFault::ConnectionClosed;
    failure.clear();
    failure.clear();
    assert_eq!(turns.taken(), vec![None]);
    failure.record(closed());
    failure.record(closed());
    failure.note(&DialError::Skipped("full".into()), Duration::from_secs(1));
    assert_eq!(turns.taken(), vec![Some(closed())]);
    assert_eq!(failure.of(7).map(|hop| hop.proxy), Some(7));
    failure.clear();
    assert_eq!(turns.taken(), vec![None]);
    assert!(failure.of(7).is_none());

    // A hop never tried that fails tells of that too: its watcher decides
    // what a failure before any success means.
    let turns = Arc::new(Turns::default());
    let failure = HopFailure::watched(turns.clone());
    failure.record(closed());
    assert_eq!(turns.taken(), vec![Some(closed())]);
}

// A proxy that answers turns its hop working; once it is gone, the next
// dial turns it failing, and the dial after says nothing new.
#[tokio::test]
async fn a_watched_proxy_hop_turns_failing_when_its_proxy_goes() {
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_address = origin.local_addr().unwrap();
    let (proxy, proxy_task) = connect_proxy(origin_address.to_string(), origin_address).await;
    let turns = Arc::new(Turns::default());
    let mut hop = Arc::into_inner(hop(7, proxy, bottom())).unwrap();
    hop.endpoint_names = std::sync::Arc::new(crate::pipe::NamesSlot::new(crate::pipe::Names::new(
        crate::pipe::DnsChoice::System,
        None,
    )));
    hop.failure = HopFailure::watched(turns.clone());
    let dialed = hop.dial(&target(origin_address)).await.unwrap();
    let (peer, _) = origin.accept().await.unwrap();
    assert_eq!(turns.taken(), vec![None]);
    drop(peer);
    drop(dialed);
    // The proxy served its one connection and closed its listener.
    proxy_task.await.unwrap();
    for _ in 0..2 {
        assert!(hop.dial(&target(origin_address)).await.is_err());
    }
    let told = turns.taken();
    assert_eq!(told.len(), 1, "{told:?}");
    assert!(told[0].is_some());
}

// A hop that fails for its egress and then for itself tells both turns: a
// fault that is the egress's own and one that is not are not the same kind
// of failing, so the watcher hears the real outage after the egress's.
#[test]
fn a_hop_failing_for_its_egress_and_then_itself_tells_both() {
    let turns = Arc::new(Turns::default());
    let failure = HopFailure::watched(turns.clone());
    let no_route = || ProxyFault::NoRouteVia { via: "eth3".into() };
    failure.clear();
    failure.record(no_route());
    failure.record(no_route());
    failure.record(ProxyFault::ConnectionClosed);
    failure.record(ProxyFault::ConnectionRefused);
    failure.record(no_route());
    assert_eq!(
        turns.taken(),
        vec![
            None,
            Some(no_route()),
            Some(ProxyFault::ConnectionClosed),
            Some(no_route()),
        ]
    );
}

// A SOCKS5 proxy silent at its own greeting is failing however lately it
// answered: only a quiet CONNECT after an answered greeting is the
// destination's.
#[tokio::test(start_paused = true)]
async fn a_socks5_proxy_silent_at_its_greeting_is_failing() {
    let stage = Arc::new(Answered {
        reply: Mutex::new(&[5, 0]),
        far: Default::default(),
    });
    let turns = Arc::new(Turns::default());
    let socks = TransportHop::new(
        9,
        TransportProxy {
            kind: crate::transport::TransportKind::Socks5,
            host: "192.0.2.3".into(),
            port: 1080,
            username: None,
            password: None,
        },
        stage.clone(),
        Duration::from_secs(30),
        Naming::Proxy,
        std::sync::Arc::new(crate::pipe::NamesSlot::new(crate::pipe::Names::new(
            crate::pipe::DnsChoice::System,
            None,
        ))),
        HopFailure::watched(turns.clone()),
    );
    let target = target("192.0.2.9:119".parse().unwrap());
    assert!(matches!(
        socks.dial(&target).await,
        Err(DialError::Destination(_))
    ));
    assert_eq!(socks.failing_hop(), None);
    assert_eq!(turns.taken(), vec![None]);

    *stage.reply.lock().unwrap() = b"";
    assert!(matches!(
        socks.dial(&target).await,
        Err(DialError::Timeout { .. })
    ));
    assert_eq!(socks.failing_hop().map(|hop| hop.proxy), Some(9));
    let told = turns.taken();
    assert_eq!(told.len(), 1, "{told:?}");
    assert!(told[0].is_some());
}

// A direct leg's blocking lookup, the NNTP address plan's, gives what its
// async lookup gives: the host's own DNS on its one shared cache, in the
// host's order, kept to the egress's address family. The async lookup hands
// the same addresses on sorted.
#[tokio::test]
async fn a_direct_legs_blocking_lookup_matches_its_async_lookup() {
    let egress = bottom();
    let first = Arc::new(record::Recording::default());
    let blocking = first
        .scope(async { egress.resolve_blocking("localhost").unwrap() })
        .await;
    let noted = first.finish("localhost", 0, None).unwrap();
    assert_eq!(noted.by, record::ResolvedBy::Host);
    let host_order: Vec<IpAddr> = noted
        .addresses
        .iter()
        .copied()
        .filter(IpAddr::is_ipv4)
        .collect();
    assert!(!blocking.is_empty());
    assert_eq!(blocking, host_order);

    let second = Arc::new(record::Recording::default());
    let Resolution::Addresses(resolved) = second.scope(egress.resolve("localhost")).await.unwrap()
    else {
        panic!("a direct leg resolves every name");
    };
    let mut sorted = blocking.clone();
    sorted.sort();
    assert_eq!(resolved, sorted);
    assert!(
        second.finish("localhost", 0, None).unwrap().cached,
        "the async lookup asks the blocking one's cache"
    );
}

#[test]
fn a_tunnel_resolver_serves_only_plain_servers_on_port_53_taken_as_sent() {
    let ip: IpAddr = "10.0.0.2".parse().unwrap();
    let plain = DnsServer::plain(ip);
    assert!(tunnel_resolver_serves(&[], Trust::AsSent));
    assert!(tunnel_resolver_serves(
        std::slice::from_ref(&plain),
        Trust::AsSent
    ));
    assert!(!tunnel_resolver_serves(
        std::slice::from_ref(&plain),
        Trust::Validate
    ));
    let off_port = DnsServer {
        address: SocketAddr::new(ip, 5353),
        wire: crate::dns::Wire::Plain,
    };
    assert!(!tunnel_resolver_serves(
        &[plain.clone(), off_port],
        Trust::AsSent
    ));
    let tls = DnsServer {
        address: SocketAddr::new(ip, 53),
        wire: crate::dns::Wire::Tls { name: None },
    };
    assert!(!tunnel_resolver_serves(
        &[plain.clone(), tls],
        Trust::AsSent
    ));
    let https = DnsServer {
        address: SocketAddr::new(ip, 443),
        wire: crate::dns::Wire::Https {
            path: "/dns-query".into(),
        },
    };
    assert!(!tunnel_resolver_serves(&[https], Trust::AsSent));
    // The tunnel's resolver asks only the first server it holds, so a list
    // of two is asked by Weaver's client, each in turn.
    let second = DnsServer::plain("10.0.0.3".parse().unwrap());
    assert!(!tunnel_resolver_serves(&[plain, second], Trust::AsSent));
}

// A WireGuard hop whose naming its tunnel's own resolver cannot serve asks
// the servers with Weaver's client, dialled through the hop.
#[tokio::test]
async fn a_wireguard_hop_asks_servers_its_tunnel_cannot_serve_through_itself() {
    #[derive(Default)]
    struct Recorder(Mutex<Vec<(String, u16)>>);
    #[async_trait::async_trait]
    impl TunnelProvider for Recorder {
        async fn prepare(&self) -> Result<(), TunnelError> {
            Ok(())
        }
        async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
            self.0.lock().unwrap().push((host.to_owned(), port));
            Err(TunnelError::Engine(
                "the fixture refuses every stream".into(),
            ))
        }
        fn describe(&self) -> String {
            "recording fixture".into()
        }
    }
    let near = crate::test_support::WireGuardTestPeer::start().await;
    let ip: IpAddr = "10.0.0.2".parse().unwrap();
    for (server, trust) in [
        (
            DnsServer {
                address: SocketAddr::new(ip, 853),
                wire: crate::dns::Wire::Tls { name: None },
            },
            Trust::AsSent,
        ),
        (DnsServer::plain(ip), Trust::Validate),
    ] {
        let resolver = Arc::new(crate::WireGuardTunnelProvider::new(
            near.client_spec("1"),
            Arc::new(crate::NoopTunnelObserver),
        ));
        let recorder = Arc::new(Recorder::default());
        let hop = wireguard_session(1, recorder.clone(), resolver, bottom(), vec![], 1);
        let mut hop = Arc::try_unwrap(hop).ok().expect("one owner");
        hop.naming = Naming::proxy_servers(vec![server.clone()], trust);
        let hop = Arc::new(hop);
        hop.resolve("origin.tunnel.test")
            .await
            .expect_err("the fixture refuses the stream");
        assert_eq!(
            *recorder.0.lock().unwrap(),
            [("10.0.0.2".to_owned(), server.address.port())],
            "{server}, {trust:?}"
        );
    }
}

// A direct leg's egress that names by `dns`, reached through a stage that
// maps the listed server's address to the double.
fn egress_naming_by(dns: &crate::test_support::dns::DnsServerDouble) -> Egress {
    let carrier = Arc::new(Mapped {
        to: dns.addr,
        asked: Mutex::new(Vec::new()),
    });
    crate::test_support::loopback::egress(4, Duration::from_secs(30)).with_naming(Naming::servers(
        vec![DnsServer::plain(ROUTED_DNS.parse().unwrap())],
        Trust::AsSent,
        carrier,
    ))
}

// On a thread handed over to blocking work, a blocking lookup asks the
// egress's listed servers on the runtime the thread belongs to.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_blocking_lookup_on_a_blocking_thread_asks_the_listed_servers() {
    let dns = dns_double(&[("news.invalid", PUBLIC)]).await;
    let egress = Arc::new(egress_naming_by(&dns));
    let found = tokio::task::spawn_blocking({
        let egress = egress.clone();
        move || blocking(|| egress.resolve_blocking("news.invalid"))
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(found, vec![PUBLIC.parse::<IpAddr>().unwrap()]);
    assert!(lookups(&dns, "news.invalid") > 0);
}

// Asked from a runtime worker, where waiting on the runtime would stall it,
// a blocking lookup through listed servers fails at once and asks nobody;
// a debug build stops on it.
#[tokio::test]
#[cfg_attr(
    debug_assertions,
    should_panic(
        expected = "a blocking lookup through listed servers runs only inside `blocking`"
    )
)]
async fn a_blocking_lookup_from_a_runtime_worker_fails_at_once() {
    let dns = dns_double(&[("news.invalid", PUBLIC)]).await;
    let egress = egress_naming_by(&dns);
    let error = egress.resolve_blocking("news.invalid").unwrap_err();
    assert!(error.to_string().contains("may not block"), "{error}");
    assert_eq!(lookups(&dns, "news.invalid"), 0);
}

// Every lookup the host resolver made or answered from its cache.
fn host_lookups() -> u64 {
    use crate::metrics::Resolver;
    let metrics = crate::metrics::snapshot();
    let resolved: u64 = metrics
        .resolutions
        .iter()
        .filter(|(by, ..)| *by == Resolver::Host)
        .map(|(_, ok, failed)| ok + failed)
        .sum();
    let held: u64 = metrics
        .cache
        .iter()
        .filter(|(by, ..)| *by == Resolver::Host)
        .map(|(.., count)| count)
        .sum();
    resolved + held
}

// A SOCKS5 hop at `host`:`port` above `inner`, its own address on the
// host's profile.
fn socks_on_host(id: u32, host: &str, port: u16, inner: Arc<dyn Dialer>) -> Arc<TransportHop> {
    Arc::new(TransportHop::new(
        id,
        TransportProxy {
            kind: crate::transport::TransportKind::Socks5,
            host: host.into(),
            port,
            username: None,
            password: None,
        },
        inner,
        Duration::from_secs(30),
        Naming::Proxy,
        std::sync::Arc::new(crate::pipe::NamesSlot::new(crate::pipe::Names::new(
            crate::pipe::DnsChoice::System,
            None,
        ))),
        Default::default(),
    ))
}

// A proxy on the host's profile whose address is a name: carried by another
// proxy in a chain, the name goes to the proxy carrying it and the host is
// never asked; the same proxy as the only hop is found by the host.
#[tokio::test]
async fn an_inner_proxys_name_goes_to_its_carrier_and_a_first_hops_to_the_host() {
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_port = origin.local_addr().unwrap().port();
    let socks = |mapping: Vec<((String, u16), SocketAddr)>| {
        crate::test_support::proxy::ProxyServerDouble::start(
            crate::transport::TransportKind::Socks5,
            None,
            mapping.into_iter().collect(),
        )
    };
    let b = socks(vec![(
        ("origin.invalid".into(), origin_port),
        origin.local_addr().unwrap(),
    )])
    .await;
    let a = socks(vec![(("b.invalid".into(), b.addr.port()), b.addr)]).await;
    let target = Target {
        host: "origin.invalid".into(),
        port: origin_port,
        purpose: Purpose::Probe,
        addresses: Vec::new(),
    };

    let before = host_lookups();
    let first = socks_on_host(7, &a.addr.ip().to_string(), a.addr.port(), bottom());
    let chain = socks_on_host(8, "b.invalid", b.addr.port(), first);
    let dialed = chain.dial(&target).await.unwrap();
    let _ = origin.accept().await.unwrap();
    assert_eq!(dialed.path.proxies, vec![7, 8]);
    assert_eq!(
        a.targets.lock().unwrap().clone(),
        vec![("b.invalid".to_string(), b.addr.port())]
    );
    assert_eq!(host_lookups(), before);

    let alone = socks_on_host(8, "localhost", b.addr.port(), bottom());
    let dialed = alone.dial(&target).await.unwrap();
    let _ = origin.accept().await.unwrap();
    assert_eq!(dialed.path.proxies, vec![8]);
    assert!(host_lookups() > before);
}

// A tunnel that records the buffers each dial through it asked for.
#[derive(Default)]
struct SizedRecorder(std::sync::Mutex<Vec<Option<crate::wireguard::TcpBuffers>>>);

#[async_trait::async_trait]
impl TunnelProvider for SizedRecorder {
    async fn dial(&self, _: &str, _: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        Ok(Box::new(tokio::io::duplex(64).0))
    }
    async fn dial_sized(
        &self,
        host: &str,
        port: u16,
        _outcome: Arc<ConnectionOutcome>,
        buffers: Option<crate::wireguard::TcpBuffers>,
        _socket: &mut StreamSocket,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.0.lock().unwrap().push(buffers);
        self.dial(host, port).await
    }
    fn describe(&self) -> String {
        "sized recorder".into()
    }
}

fn recording_session(provider: Arc<SizedRecorder>) -> SessionHop {
    SessionHop {
        sockets: Default::default(),
        capacity: Default::default(),
        activity: Default::default(),
        id: 3,
        provider,
        inner: bottom(),
        transport: None,
        path: DialPath::default(),
        endpoint: Some("127.0.0.1:22".parse().unwrap()),
        timeout: Duration::from_secs(30),
        naming: Naming::Proxy,
        tunnel_dns: Vec::new(),
        resolver: None,
        forwarding: Default::default(),
        failure: Default::default(),
    }
}

fn aimed(purpose: Purpose) -> Target {
    Target {
        purpose,
        ..target("192.0.2.9:119".parse().unwrap())
    }
}

// Every dial through a tunnel asks for the buffers its traffic needs: a
// usenet connection or a test of a usenet server the receive buffer the
// profile sets and the command-sized send buffer, everything else the
// tunnel's own sizes. Each profile's receive buffer reaches the next usenet
// dial; a stream dialled before a change keeps what it was dialled with, and
// nothing is dialled again for it.
//
// The receive size is one value for the whole process, so this is the only
// test in the crate that sets it.
#[tokio::test]
async fn a_dial_through_a_tunnel_asks_for_the_buffers_of_its_traffic() {
    use crate::wireguard::TcpBuffers;
    let provider = Arc::new(SizedRecorder::default());
    let hop = recording_session(provider.clone());
    let nntp = Purpose::Nntp {
        server: 1,
        leg: 0,
        probing: false,
    };
    let probe = Purpose::NntpProbe { server: 1, leg: 0 };
    let sized = |receive| Some(TcpBuffers::new(receive, 64 * 1024));

    // Before any profile is in force, a usenet socket asks for 1 MiB.
    let cases = [
        (nntp.clone(), sized(1024 * 1024)),
        (probe.clone(), sized(1024 * 1024)),
        (
            Purpose::ProxyEndpoint {
                proxy: 4,
                carrying: None,
            },
            None,
        ),
        (Purpose::Feed { feed: 2 }, None),
        (Purpose::Outbound, None),
        (Purpose::Torrent, None),
        (Purpose::TorrentHttp, None),
        (Purpose::Probe, None),
        (Purpose::Dns, None),
    ];
    for (purpose, _) in &cases {
        hop.dial(&aimed(purpose.clone())).await.unwrap();
    }
    let asked = std::mem::take(&mut *provider.0.lock().unwrap());
    let expected: Vec<_> = cases.iter().map(|(_, buffers)| *buffers).collect();
    assert_eq!(asked, expected);

    let mut open = Vec::new();
    for receive in [256 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
        set_usenet_receive_bytes(receive);
        open.push(hop.dial(&aimed(nntp.clone())).await.unwrap());
        hop.dial(&aimed(probe.clone())).await.unwrap();
    }
    let asked = provider.0.lock().unwrap().clone();
    assert_eq!(
        asked,
        vec![
            sized(256 * 1024),
            sized(256 * 1024),
            sized(1024 * 1024),
            sized(1024 * 1024),
            sized(4 * 1024 * 1024),
            sized(4 * 1024 * 1024),
        ]
    );
    assert_eq!(USENET_SEND_BYTES, 64 * 1024);
    assert_eq!(open.len(), 3);
}

// A usenet dial on a direct route opens an ordinary socket, sized by the
// system as any other direct dial is: a profile's tunnel sizes never reach
// it.
#[tokio::test]
async fn a_direct_usenet_dial_is_not_sized_by_the_profile() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let egress = bottom();
    let plain = egress.dial(&target(address)).await.unwrap();
    let usenet = egress
        .dial(&Target {
            purpose: Purpose::Nntp {
                server: 1,
                leg: 0,
                probing: false,
            },
            ..target(address)
        })
        .await
        .unwrap();
    let sizes = |dialed: &Dialed| {
        let socket = socket2::SockRef::from(dialed.stream.tcp().expect("a real socket"));
        (
            socket.recv_buffer_size().unwrap(),
            socket.send_buffer_size().unwrap(),
        )
    };
    assert_eq!(sizes(&usenet), sizes(&plain));
}
