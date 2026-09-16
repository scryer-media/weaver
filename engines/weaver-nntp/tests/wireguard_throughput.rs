//! Opt-in encrypted download measurements using only loopback fixtures.
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::Instant,
};
use weaver_tunnel::{
    NoopTunnelObserver, TunnelProvider, TunnelStream, WireGuardTunnelProvider,
    bridge::Bridge,
    test_support::{WireGuardTestPeer, WireGuardTestPeerOptions},
};

struct CertificateFile(std::path::PathBuf);
impl Drop for CertificateFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local throughput measurement"]
async fn wireguard_download_throughput() {
    let bytes = std::env::var("WEAVER_THROUGHPUT_MIB")
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(8)
        * 1048576;
    let samples = std::env::var("WEAVER_THROUGHPUT_SAMPLES")
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(3);
    let modes =
        std::env::var("WEAVER_THROUGHPUT_MODES").unwrap_or_else(|_| "relay,direct,s2n".into());
    let connections_list: Vec<usize> = std::env::var("WEAVER_THROUGHPUT_CONNECTIONS")
        .unwrap_or_else(|_| "1,20".into())
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    let cert = rcgen::generate_simple_self_signed(vec!["provider.invalid".into()]).unwrap();
    let config = tokio_rustls::rustls::ServerConfig::builder()
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
    let ca = CertificateFile(std::env::temp_dir().join(format!(
            "weaver-throughput-ca-{}-{}.pem",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    std::fs::write(&ca.0, cert.cert.pem()).unwrap();
    for sample in 1..=samples {
        for mode in modes.split(',') {
            assert!(["relay", "direct", "s2n"].contains(&mode));
            for rtt_ms in [0, 50, 100] {
                for &connections in &connections_list {
                    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = origin.local_addr().unwrap();
                    let encrypted = mode == "s2n";
                    let acceptor = acceptor.clone();
                    let mut fixtures = JoinSet::new();
                    fixtures.spawn(async move {
                        let mut senders = JoinSet::new();
                        loop {
                            tokio::select! {
                                incoming = origin.accept() => {
                                    let (socket, _) = incoming.unwrap();
                                    let acceptor = acceptor.clone();
                                    senders.spawn(async move {
                                        let mut socket: Box<dyn TunnelStream> = if encrypted { Box::new(acceptor.accept(socket).await.unwrap()) } else { Box::new(socket) };
                                        assert_eq!(socket.read_u8().await.unwrap(), 1);
                                        let chunk = [0x5a; 65536];
                                        for _ in 0..bytes / chunk.len() { socket.write_all(&chunk).await.unwrap(); }
                                        socket.shutdown().await.unwrap();
                                    });
                                }
                                result = senders.join_next(), if !senders.is_empty() => { result.unwrap().unwrap(); }
                            }
                        }
                    });
                    let peer = WireGuardTestPeer::start_for_downloads(
                        WireGuardTestPeerOptions {
                            http_port: addr.port(),
                            ..Default::default()
                        },
                        Some(addr),
                        Duration::from_millis(rtt_ms),
                        true,
                    )
                    .await;
                    let provider = Arc::new(WireGuardTunnelProvider::new(
                        peer.client_spec("throughput"),
                        Arc::new(NoopTunnelObserver),
                    ));
                    let bridge = Bridge::start(
                        &tokio::runtime::Handle::current(),
                        provider.clone(),
                        Duration::from_secs(10),
                        ("fixture".into(), "secret".into()),
                    )
                    .unwrap();
                    // Sequential establishment keeps the small peer listen backlog
                    // and TLS handshakes outside the download measurement.
                    let mut streams: Vec<Box<dyn TunnelStream>> = Vec::new();
                    for _ in 0..connections {
                        let stream: Box<dyn TunnelStream> = if mode == "relay" {
                            let mut stream =
                                TcpStream::connect(bridge.addr().unwrap()).await.unwrap();
                            weaver_tunnel::transport::socks_connect(
                                &mut stream,
                                &peer.tunnel_address().to_string(),
                                addr.port(),
                                Some(bridge.credentials()),
                            )
                            .await
                            .unwrap();
                            Box::new(stream)
                        } else {
                            let (stream, _) = bridge
                                .dial(&peer.tunnel_address().to_string(), addr.port())
                                .await
                                .unwrap();
                            if encrypted {
                                let stream = weaver_nntp::tls::upgrade_starttls(
                                    weaver_nntp::tls::NntpTransport::Plain {
                                        inner: stream.into(),
                                        remote_addr: None,
                                    },
                                    "provider.invalid",
                                    Some(&ca.0),
                                    None,
                                    weaver_nntp::TlsCipherPreference::Auto,
                                )
                                .await
                                .unwrap();
                                assert!(
                                    matches!(
                                        stream,
                                        weaver_nntp::tls::NntpTransport::S2nTls { .. }
                                    ),
                                    "run this benchmark with WEAVER_NNTP_TLS_BACKEND=s2n"
                                );
                                Box::new(stream)
                            } else {
                                Box::new(stream)
                            }
                        };
                        streams.push(stream);
                    }
                    let start = Instant::now();
                    let mut readers = JoinSet::new();
                    for mut stream in streams {
                        readers.spawn(async move {
                            stream.write_u8(1).await.unwrap();
                            let mut buffer = [0; 65536];
                            let mut count = 0;
                            while count < bytes {
                                let read = stream.read(&mut buffer).await.unwrap();
                                assert!(read > 0);
                                assert!(buffer[..read].iter().all(|b| *b == 0x5a));
                                count += read;
                            }
                            assert_eq!(count, bytes);
                            assert_eq!(stream.read(&mut buffer).await.unwrap(), 0);
                        });
                    }
                    tokio::time::timeout(Duration::from_secs(120), async {
                        while let Some(result) = readers.join_next().await {
                            result.unwrap();
                        }
                    })
                    .await
                    .unwrap();
                    println!(
                        "WireGuard mode={mode} sample={sample} ACK-delay={rtt_ms}ms connections={connections}: {:.1} Mbps, verified {} MiB",
                        bytes as f64 * connections as f64 * 8.0
                            / start.elapsed().as_secs_f64()
                            / 1e6,
                        bytes * connections / 1048576
                    );
                    bridge.revoke().await;
                    provider.shutdown().await;
                    fixtures.abort_all();
                    while let Some(result) = fixtures.join_next().await {
                        assert!(result.unwrap_err().is_cancelled());
                    }
                }
            }
        }
    }
}
