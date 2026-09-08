use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use weaver_nntp::{BlockingNntpConnection, NntpConnection, ServerConfig, commands::Command};
use weaver_tunnel::{
    NoopTunnelObserver, SshTunnelProvider, TunnelProvider, TunnelStream, WireGuardTunnelProvider,
    bridge::Bridge,
    test_support::{
        SshServerDouble, SshServerOptions, TEST_PEER_ADDRESS, WireGuardTestPeer,
        WireGuardTestPeerOptions, proxy::ProxyServerDouble, spec_for,
    },
    transport::{TransportKind, TransportProxy},
};

struct Origin {
    addr: SocketAddr,
    ca: PathBuf,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.ca);
    }
}
impl Origin {
    async fn start(implicit: bool) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["provider.invalid".into()]).unwrap();
        let config = tokio_rustls::rustls::ServerConfig::builder_with_provider(Arc::new(
            tokio_rustls::rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer::from(
                cert.signing_key.serialize_der(),
            )
            .into(),
        )
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ca = std::env::temp_dir().join(format!(
            "weaver-proxy-ca-{}-{}.pem",
            std::process::id(),
            addr.port()
        ));
        std::fs::write(&ca, cert.cert.pem()).unwrap();
        let body_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream,_)) = accepted else { break; }; let acceptor = acceptor.clone();
                        let body_attempts = body_attempts.clone();
                        tasks.spawn(async move {
                            let stream: Box<dyn TunnelStream> = Box::new(stream);
                            let mut stream: Box<dyn TunnelStream> = if implicit { match acceptor.accept(stream).await { Ok(tls) => Box::new(tls), Err(_) => return } } else { stream };
                            if stream.write_all(b"200 fixture ready\r\n").await.is_err() { return; }
                            loop {
                                let mut line = Vec::new();
                                while line.len() < 1024 {
                                    let Ok(byte) = stream.read_u8().await else { return; }; line.push(byte); if byte == b'\n' { break; }
                                }
                                let line = String::from_utf8_lossy(&line);
                                let response: &[u8] = if line.starts_with("AUTHINFO USER") { b"381 password required\r\n" }
                                    else if line.starts_with("AUTHINFO PASS") { b"281 authenticated\r\n" }
                                    else if line.starts_with("CAPABILITIES") { b"101 capabilities\r\nVERSION 2\r\nPIPELINING\r\n.\r\n" }
                                    else if line.starts_with("DATE") { b"111 20260907120000\r\n" }
                                    else if line.starts_with("BODY <retry@fixture>") {
                                        if body_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                                            let _ = stream.write_all(b"222 1 <retry@fixture>\r\npartial\r\n").await;
                                            return;
                                        }
                                        b"222 1 <retry@fixture>\r\ncomplete\r\n.\r\n"
                                    }
                                    else if line.starts_with("BODY") { b"430 missing article\r\n" }
                                    else if line.starts_with("QUIT") { let _=stream.write_all(b"205 goodbye\r\n").await; return; }
                                    else if line.starts_with("STARTTLS") {
                                        if stream.write_all(b"382 begin TLS\r\n").await.is_err() { return; }
                                        stream = match acceptor.accept(stream).await { Ok(tls) => Box::new(tls), Err(_) => return }; continue;
                                    } else { b"500 unsupported\r\n" };
                                if stream.write_all(response).await.is_err() { return; }
                            }
                        });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        Self { addr, ca, task }
    }
}

enum Fixture {
    Standard(ProxyServerDouble),
    Ssh(SshServerDouble),
    WireGuard(WireGuardTestPeer),
}
async fn profile(kind: u8, destination: SocketAddr) -> (Fixture, Arc<dyn TunnelProvider>) {
    let observer = Arc::new(NoopTunnelObserver);
    let mapping = HashMap::from([
        (("provider.invalid".into(), destination.port()), destination),
        (("wrong.invalid".into(), destination.port()), destination),
    ]);
    match kind {
        0 | 1 => {
            let kind = if kind == 0 {
                TransportKind::HttpConnect
            } else {
                TransportKind::Socks5
            };
            let proxy =
                ProxyServerDouble::start(kind, Some(("user".into(), "password".into())), mapping)
                    .await;
            let provider = TransportProxy {
                kind,
                host: proxy.addr.ip().to_string(),
                port: proxy.addr.port(),
                username: Some("user".into()),
                password: Some("password".into()),
            };
            (Fixture::Standard(proxy), Arc::new(provider))
        }
        2 => {
            let proxy = SshServerDouble::start(SshServerOptions {
                destinations: mapping,
                ..Default::default()
            })
            .await;
            let spec = spec_for("nntp-test", &proxy.host(), proxy.port());
            (
                Fixture::Ssh(proxy),
                Arc::new(SshTunnelProvider::new(spec, observer)),
            )
        }
        _ => {
            let proxy = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
                tcp_forward: Some(destination),
                http_port: destination.port(),
                names: HashMap::from([
                    ("provider.invalid".into(), vec![TEST_PEER_ADDRESS.into()]),
                    ("wrong.invalid".into(), vec![TEST_PEER_ADDRESS.into()]),
                ]),
                ..Default::default()
            })
            .await;
            let spec = proxy.client_spec("nntp-test");
            (
                Fixture::WireGuard(proxy),
                Arc::new(WireGuardTunnelProvider::new(spec, observer)),
            )
        }
    }
}

async fn exercise(kind: u8, implicit: bool, starttls: bool) {
    let origin = Origin::start(implicit).await;
    let (fixture, provider) = profile(kind, origin.addr).await;
    let bridge = Bridge::start(
        &tokio::runtime::Handle::current(),
        provider.clone(),
        Duration::from_secs(5),
        ("fixture".into(), "bridge-secret".into()),
    )
    .unwrap();
    let config = ServerConfig {
        proxy: Some(bridge.clone()),
        host: "provider.invalid".into(),
        port: origin.addr.port(),
        tls: implicit,
        starttls,
        username: Some("nntp-user".into()),
        password: Some("nntp-pass".into()),
        tls_ca_cert: Some(origin.ca.clone()),
        connect_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let mut client = NntpConnection::connect(&config).await.unwrap();
    assert_eq!(
        client.remote_ip(),
        None,
        "the bridge is not the provider's IP"
    );
    assert_eq!(
        client
            .send_command(&Command::Date)
            .await
            .unwrap()
            .code
            .raw(),
        111
    );
    assert!(client.body_by_id("missing@fixture").await.is_err());
    client.quit().await.unwrap();
    let pooled = weaver_nntp::NntpClient::new(weaver_nntp::client::NntpClientConfig::single(
        config.clone(),
        1,
    ));
    let recovered = pooled.fetch_body("retry@fixture").await.unwrap();
    assert_eq!(
        std::str::from_utf8(&recovered).unwrap().trim(),
        "complete",
        "article retry must discard the partial first response"
    );
    pooled.shutdown().await;
    if !starttls {
        let blocking_config = config.clone();
        tokio::task::spawn_blocking(move || {
            let mut client =
                BlockingNntpConnection::connect_with_ip_policy(&blocking_config, &[], 0).unwrap();
            assert_eq!(client.remote_ip(), None);
            assert_eq!(client.send_command(&Command::Date).unwrap().code.raw(), 111);
            client.quit().unwrap();
        })
        .await
        .unwrap();
    }
    if implicit {
        let mut wrong = config.clone();
        wrong.host = "wrong.invalid".into();
        assert!(
            NntpConnection::connect(&wrong).await.is_err(),
            "hostname checks use the original destination"
        );
        let certificate = weaver_nntp::tls::inspect_tls_name_mismatch_certificate_via(
            &wrong.host,
            wrong.port,
            Some(&origin.ca),
            Some(&bridge),
        )
        .await
        .unwrap();
        assert!(
            certificate.is_some(),
            "certificate inspection follows the proxy"
        );
    }
    match &fixture {
        Fixture::Standard(proxy) => assert!(
            proxy
                .targets
                .lock()
                .unwrap()
                .iter()
                .any(|(host, _)| host == "provider.invalid")
        ),
        Fixture::Ssh(proxy) => assert!(
            proxy
                .forwarded_targets()
                .iter()
                .any(|(host, _)| host == "provider.invalid")
        ),
        Fixture::WireGuard(proxy) => assert!(
            proxy
                .dns_queries()
                .iter()
                .any(|host| host == "provider.invalid")
        ),
    }
    bridge.revoke().await;
    provider.shutdown().await;
    assert!(NntpConnection::connect(&config).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_connect_plain_tls_and_starttls() {
    for (tls, starttls) in [(false, false), (true, false), (false, true)] {
        exercise(0, tls, starttls).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socks5_plain_tls_and_starttls() {
    for (tls, starttls) in [(false, false), (true, false), (false, true)] {
        exercise(1, tls, starttls).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ssh_plain_tls_and_starttls() {
    for (tls, starttls) in [(false, false), (true, false), (false, true)] {
        exercise(2, tls, starttls).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wireguard_plain_tls_and_starttls() {
    for (tls, starttls) in [(false, false), (true, false), (false, true)] {
        exercise(3, tls, starttls).await;
    }
}

/// An opt-in local transport measurement; wall-clock rates are informational.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local throughput measurement"]
async fn local_route_throughput() {
    const BYTES: usize = 16 * 1024 * 1024;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let sender = tokio::spawn(async move {
        let mut children = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                incoming = listener.accept() => {
                    let (mut socket, _) = incoming.unwrap();
                    children.spawn(async move {
                        let chunk = [0x5a; 65536];
                        for _ in 0..BYTES / chunk.len() { socket.write_all(&chunk).await.unwrap(); }
                        socket.shutdown().await.unwrap();
                    });
                }
                _ = children.join_next(), if !children.is_empty() => {}
            }
        }
    });
    async fn consume(stream: &mut dyn TunnelStream) -> f64 {
        let start = std::time::Instant::now();
        let mut count = 0;
        let mut buffer = [0; 65536];
        loop {
            let size = stream.read(&mut buffer).await.unwrap();
            if size == 0 {
                break;
            }
            assert!(buffer[..size].iter().all(|b| *b == 0x5a));
            count += size;
        }
        assert_eq!(count, BYTES);
        BYTES as f64 / 1048576.0 / start.elapsed().as_secs_f64()
    }
    for registered in [false, true] {
        let registry = weaver_nntp::revocation::SocketRegistry::default();
        let mut rates = Vec::new();
        for _ in 0..3 {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let _registration =
                registered.then(|| registry.track(socket2::SockRef::from(&stream)).unwrap());
            rates.push(consume(&mut stream).await);
        }
        rates.sort_by(f64::total_cmp);
        println!(
            "direct registered={registered}: {:.1} MiB/s median, verified {BYTES} bytes per sample",
            rates[1]
        );
    }
    for kind in 0..4 {
        let (_fixture, provider) = profile(kind, addr).await;
        let bridge = Bridge::start(
            &tokio::runtime::Handle::current(),
            provider.clone(),
            Duration::from_secs(10),
            ("fixture".into(), "bridge-secret".into()),
        )
        .unwrap();
        let mut rates = Vec::new();
        for _ in 0..3 {
            let mut stream = tokio::net::TcpStream::connect(bridge.addr().unwrap())
                .await
                .unwrap();
            weaver_tunnel::transport::socks_connect(
                &mut stream,
                "provider.invalid",
                addr.port(),
                Some(bridge.credentials()),
            )
            .await
            .unwrap();
            rates.push(consume(&mut stream).await);
        }
        rates.sort_by(f64::total_cmp);
        println!(
            "{}: {:.1} MiB/s median, verified {BYTES} bytes per sample",
            ["HTTP CONNECT", "SOCKS5", "SSH", "WireGuard"][kind as usize],
            rates[1]
        );
        bridge.revoke().await;
        provider.shutdown().await;
    }
    sender.abort();
    let _ = sender.await;
}
