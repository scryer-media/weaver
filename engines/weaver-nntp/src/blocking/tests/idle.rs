// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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

// A lane waiting for work may free its read buffer. The idle probe must still
// tell an open session from a closed one rather than read a zero-length
// buffer as the peer going away.
#[test]
fn rustls_idle_inspection_after_the_read_buffer_is_freed_tells_open_from_closed() {
    let fixture = idle_tls_fixture();
    let tcp = TcpStream::connect(("127.0.0.1", fixture.port)).unwrap();
    let mut stream = BlockingManualTlsStream::connect(
        tcp,
        "localhost",
        Some(&fixture.ca),
        None,
        TlsCipherPreference::Auto,
        Duration::from_secs(5),
    )
    .unwrap();
    stream.read_buffer = Vec::new();
    let mut transport = BlockingTransport::Rustls(Box::new(stream));

    fixture.ready.recv().unwrap();
    assert!(!transport.idle_terminal(), "an open session is not closed");
    // The server sends its close_notify in two pieces.
    fixture.advance.send(()).unwrap();
    fixture.ready.recv().unwrap();
    fixture.advance.send(()).unwrap();
    fixture.ready.recv().unwrap();
    while !transport.idle_terminal() {
        std::thread::yield_now();
    }
    fixture.advance.send(()).unwrap();
    fixture.server.join().unwrap();
    std::fs::remove_file(fixture.ca).unwrap();
}

// A TLS connection frees both read buffers between articles and reads the
// next article as before.
#[test]
fn a_rustls_connection_frees_its_read_buffers_between_articles_and_reads_on() {
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![
        ("<first@test>", TestArticle::Body(vec![0x41; 300 * 1024])),
        ("<second@test>", TestArticle::Body(vec![0x42; 300 * 1024])),
    ]);
    let mut conn = connect_with_backend(&config, NntpTlsBackend::ManualRustls);
    conn.select_group("alt.test").unwrap();
    assert_eq!(
        conn.stream_yenc_article("<first@test>")
            .unwrap()
            .into_data(),
        vec![0x41; 300 * 1024]
    );

    conn.release_idle_buffers();
    assert_eq!(conn.read_buf.capacity(), 0);
    let BlockingTransport::Rustls(inner) = &conn.transport else {
        panic!("the connection was made with rustls");
    };
    assert_eq!(inner.read_buffer.capacity(), 0);

    assert_eq!(
        conn.stream_yenc_article("<second@test>")
            .unwrap()
            .into_data(),
        vec![0x42; 300 * 1024]
    );
    conn.quit().unwrap();
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

// A plain server answering BODY with the given bodies in order.
fn spawn_plain_body_server(bodies: Vec<Vec<u8>>) -> (ServerConfig, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let config = ServerConfig {
        // Long enough that a loaded machine never trips it.
        command_timeout: Duration::from_secs(600),
        ..blocking_pipelined_setup_config(listener.local_addr().unwrap().port())
    };
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket.write_all(b"200 ready\r\n").unwrap();
        let mut reader = std::io::BufReader::new(socket.try_clone().unwrap());
        let mut bodies = bodies.into_iter();
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap() == 0 {
                break;
            }
            if line.starts_with("AUTHINFO USER") {
                socket.write_all(b"381 password\r\n").unwrap();
            } else if line.starts_with("AUTHINFO PASS") {
                socket.write_all(b"281 authenticated\r\n").unwrap();
            } else if line.starts_with("GROUP ") {
                socket.write_all(b"211 1 1 1 alt.test\r\n").unwrap();
            } else if line.starts_with("BODY ") {
                let body = yenc_body(&bodies.next().expect("a body for every BODY"));
                socket.write_all(b"222 0 body follows\r\n").unwrap();
                socket.write_all(&body).unwrap();
                socket.write_all(b".\r\n").unwrap();
            } else if line.starts_with("QUIT") {
                socket.write_all(b"205 closing\r\n").unwrap();
                break;
            } else {
                panic!("unexpected test command: {line}");
            }
        }
        assert!(bodies.next().is_none(), "every body was asked for");
    });
    (config, server)
}

// A lane keeps its buffers while a response is owed, frees them once it is
// idle, and the next article reads exactly as before.
#[test]
fn an_idle_plain_lane_frees_its_read_buffers_and_reads_the_next_article() {
    let first = vec![0x61; 300 * 1024];
    let second = vec![0x62; 300 * 1024];
    let (config, server) = spawn_plain_body_server(vec![first.clone(), second.clone()]);
    let mut lane = test_body_lane(&config);

    assert!(matches!(
        lane.ring_issue("<first@test>", 300 * 1024, 1),
        RingIssueOutcome::Issued
    ));
    lane.release_idle_buffers();
    assert!(
        lane.conn.read_buf.capacity() > 0,
        "a lane owed a response keeps its buffer"
    );
    let (trace, _) = lane.ring_read_next().unwrap();
    assert_eq!(ring_payload(&trace), first);

    lane.release_idle_buffers();
    assert_eq!(lane.conn.read_buf.capacity(), 0);
    assert!(!lane.peer_closed(), "freeing the buffers keeps the session");

    assert!(matches!(
        lane.ring_issue("<second@test>", 300 * 1024, 1),
        RingIssueOutcome::Issued
    ));
    let (trace, _) = lane.ring_read_next().unwrap();
    assert_eq!(ring_payload(&trace), second);
    lane.park();
    server.join().unwrap();
}
