use super::*;

struct IdleTlsFixture {
    port: u16,
    ca: std::path::PathBuf,
    advance: std::sync::mpsc::Sender<()>,
    ready: std::sync::mpsc::Receiver<()>,
    server: std::thread::JoinHandle<()>,
}

fn idle_tls_fixture() -> IdleTlsFixture {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let ca = unique_ca_path("idle-close-notify");
    std::fs::write(&ca, cert.cert.pem()).unwrap();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    let config = RustlsServerConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert.cert.der().clone()], key)
    .unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let (advance, commands) = std::sync::mpsc::channel();
    let (stages, ready) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().unwrap();
        tcp.set_nodelay(true).unwrap();
        let mut tls = tokio_rustls::rustls::ServerConnection::new(Arc::new(config)).unwrap();
        while tls.is_handshaking() {
            tls.complete_io(&mut tcp).unwrap();
        }
        while tls.wants_write() {
            tls.write_tls(&mut tcp).unwrap();
        }
        stages.send(()).unwrap();
        if commands.recv().is_err() {
            return;
        }
        tls.send_close_notify();
        let mut record = Vec::new();
        tls.write_tls(&mut record).unwrap();
        tcp.write_all(&record[..3]).unwrap();
        stages.send(()).unwrap();
        if commands.recv().is_err() {
            return;
        }
        tcp.write_all(&record[3..]).unwrap();
        stages.send(()).unwrap();
        // Keep TCP open so the client must interpret TLS, not just TCP FIN.
        let _ = commands.recv();
    });
    IdleTlsFixture {
        port,
        ca,
        advance,
        ready,
        server,
    }
}

fn inspect_fragmented_shutdown(mut inspect: impl FnMut() -> bool, fixture: IdleTlsFixture) {
    fixture.ready.recv().unwrap();
    assert!(
        !inspect(),
        "TLS session tickets are not terminal application data"
    );
    fixture.advance.send(()).unwrap();
    fixture.ready.recv().unwrap();
    for _ in 0..5 {
        assert!(!inspect(), "a fragmented record must retain its TLS state");
        std::thread::sleep(Duration::from_millis(2));
    }
    fixture.advance.send(()).unwrap();
    fixture.ready.recv().unwrap();
    while !inspect() {
        std::thread::yield_now();
    }
    fixture.advance.send(()).unwrap();
    fixture.server.join().unwrap();
    std::fs::remove_file(fixture.ca).unwrap();
}

#[test]
fn rustls_idle_inspection_preserves_fragments_and_detects_close_notify() {
    let fixture = idle_tls_fixture();
    let tcp = TcpStream::connect(("127.0.0.1", fixture.port)).unwrap();
    let stream = BlockingManualTlsStream::connect(
        tcp,
        "localhost",
        Some(&fixture.ca),
        None,
        TlsCipherPreference::Auto,
        Duration::from_secs(5),
    )
    .unwrap();
    let mut transport = BlockingTransport::Rustls(Box::new(stream));
    inspect_fragmented_shutdown(|| transport.idle_terminal(), fixture);
}

#[cfg(not(windows))]
#[test]
fn s2n_idle_inspection_preserves_fragments_and_detects_close_notify() {
    let _guard = s2n_test_guard();
    let fixture = idle_tls_fixture();
    let tcp = TcpStream::connect(("127.0.0.1", fixture.port)).unwrap();
    let stream =
        BlockingS2nStream::connect(tcp, "localhost", Some(&fixture.ca), Duration::from_secs(5))
            .unwrap();
    let mut transport = BlockingTransport::S2n(stream);
    inspect_fragmented_shutdown(|| transport.idle_terminal(), fixture);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_rustls_idle_inspection_preserves_fragments_and_detects_close_notify() {
    for manual in [true, false] {
        let fixture = idle_tls_fixture();
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", fixture.port))
            .await
            .unwrap();
        let config =
            crate::tls::build_tls_config(Some(&fixture.ca), TlsCipherPreference::Auto).unwrap();
        let name = crate::tls::make_server_name("localhost").unwrap();
        let mut transport = if manual {
            crate::tls::NntpTransport::ManualTls {
                inner: crate::tls::ManualTlsStream::connect(tcp, config, name)
                    .await
                    .unwrap(),
                remote_addr: None,
            }
        } else {
            crate::tls::NntpTransport::Tls {
                inner: tokio_rustls::TlsConnector::from(config)
                    .connect(name, tcp.into())
                    .await
                    .unwrap(),
                remote_addr: None,
            }
        };
        inspect_fragmented_shutdown(|| transport.idle_terminal(), fixture);
    }
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_s2n_idle_inspection_preserves_fragments_and_detects_close_notify() {
    let fixture = idle_tls_fixture();
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", fixture.port))
        .await
        .unwrap();
    let config = crate::tls::build_s2n_tls_config(Some(&fixture.ca)).unwrap();
    let mut transport = crate::tls::NntpTransport::S2nTls {
        inner: s2n_tls_tokio::TlsConnector::new(config)
            .connect("localhost", tcp.into())
            .await
            .unwrap(),
        remote_addr: None,
    };
    inspect_fragmented_shutdown(|| transport.idle_terminal(), fixture);
}
