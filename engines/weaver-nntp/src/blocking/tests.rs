use super::*;
use std::collections::HashMap;
use std::io::BufRead;
use std::net::TcpListener;
use std::sync::Arc;
#[cfg(not(windows))]
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig as RustlsServerConfig;
use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

enum TestArticle {
    Body(Vec<u8>),
    DelayedInitial {
        data: Vec<u8>,
        delay: Duration,
    },
    Trickle {
        data: Vec<u8>,
        line_delay: Duration,
    },
    Truncated {
        data: Vec<u8>,
        wait_for_next_request: bool,
    },
}

#[cfg(not(windows))]
fn s2n_test_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

#[cfg(not(windows))]
fn assert_s2n_socket_timeout_slice(conn: &BlockingNntpConnection) {
    let BlockingTransport::S2n(stream) = &conn.transport else {
        panic!("expected blocking S2N transport");
    };
    assert_eq!(
        stream.tcp.read_timeout().unwrap(),
        Some(S2N_BLOCKING_IO_SLICE)
    );
    assert_eq!(
        stream.tcp.write_timeout().unwrap(),
        Some(S2N_BLOCKING_IO_SLICE)
    );
}

fn yenc_body(data: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    weaver_yenc::encode(data, &mut body, 128, "owned.bin").unwrap();
    body
}

// Nanosecond timestamps alone collide when TLS tests start in parallel;
// the serial keeps each fixture's CA file unique within the process.
fn unique_ca_path(label: &str) -> std::path::PathBuf {
    static CA_SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let serial = CA_SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "weaver-{label}-{}-{serial}-{nonce}.pem",
        std::process::id()
    ))
}

fn spawn_tls_nntp_server(
    articles: Vec<(&'static str, TestArticle)>,
) -> (
    ServerConfig,
    std::thread::JoinHandle<()>,
    std::path::PathBuf,
) {
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("generate test cert");
    let cert_der = certified_key.cert.der().clone();
    let cert_pem = certified_key.cert.pem();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified_key.signing_key.serialize_der(),
    ));

    let server_provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
    let server_config = RustlsServerConfig::builder_with_provider(Arc::new(server_provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("server TLS config");

    let ca_path = unique_ca_path("blocking-tls-ca");
    std::fs::write(&ca_path, cert_pem).expect("write blocking TLS CA PEM");

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();

    let articles = articles
        .into_iter()
        .map(|(id, article)| (id.to_string(), article))
        .collect::<HashMap<_, _>>();
    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let (socket, _) = listener.accept().await.unwrap();
            let acceptor = TlsAcceptor::from(Arc::new(server_config));
            let tls = acceptor.accept(socket).await.unwrap();
            let mut stream = BufReader::new(tls);
            stream
                .get_mut()
                .write_all(b"200 test server ready\r\n")
                .await
                .unwrap();
            stream.get_mut().flush().await.unwrap();

            let mut line = String::new();
            let mut close_after_next_request = false;
            loop {
                line.clear();
                let Ok(read) = stream.read_line(&mut line).await else {
                    break;
                };
                if read == 0 {
                    break;
                }
                if close_after_next_request {
                    break;
                }
                let command = line.trim_end_matches(['\r', '\n']);
                if command.eq_ignore_ascii_case("CAPABILITIES") {
                    stream
                        .get_mut()
                        .write_all(
                            b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
                        )
                        .await
                        .unwrap();
                } else if command.to_ascii_uppercase().starts_with("GROUP ") {
                    stream
                        .get_mut()
                        .write_all(b"211 1 1 1 alt.test\r\n")
                        .await
                        .unwrap();
                } else if let Some(id) = command.strip_prefix("BODY ") {
                    match articles.get(id.trim()) {
                        Some(TestArticle::Body(data)) => {
                            stream
                                .get_mut()
                                .write_all(
                                    format!("222 0 {} body follows\r\n", id.trim()).as_bytes(),
                                )
                                .await
                                .unwrap();
                            stream.get_mut().write_all(&yenc_body(data)).await.unwrap();
                            stream.get_mut().write_all(b".\r\n").await.unwrap();
                        }
                        Some(TestArticle::DelayedInitial { data, delay }) => {
                            tokio::time::sleep(*delay).await;
                            if stream
                                .get_mut()
                                .write_all(
                                    format!("222 0 {} body follows\r\n", id.trim()).as_bytes(),
                                )
                                .await
                                .is_err()
                                || stream.get_mut().write_all(&yenc_body(data)).await.is_err()
                                || stream.get_mut().write_all(b".\r\n").await.is_err()
                            {
                                return;
                            }
                        }
                        Some(TestArticle::Trickle { data, line_delay }) => {
                            stream
                                .get_mut()
                                .write_all(
                                    format!("222 0 {} body follows\r\n", id.trim()).as_bytes(),
                                )
                                .await
                                .unwrap();
                            let encoded = yenc_body(data);
                            for line in encoded.split_inclusive(|byte| *byte == b'\n') {
                                tokio::time::sleep(*line_delay).await;
                                if stream.get_mut().write_all(line).await.is_err()
                                    || stream.get_mut().flush().await.is_err()
                                {
                                    return;
                                }
                            }
                            if stream.get_mut().write_all(b".\r\n").await.is_err() {
                                return;
                            }
                        }
                        Some(TestArticle::Truncated {
                            data,
                            wait_for_next_request,
                        }) => {
                            stream
                                .get_mut()
                                .write_all(
                                    format!("222 0 {} body follows\r\n", id.trim()).as_bytes(),
                                )
                                .await
                                .unwrap();
                            stream.get_mut().write_all(&yenc_body(data)).await.unwrap();
                            stream.get_mut().flush().await.unwrap();
                            if *wait_for_next_request {
                                close_after_next_request = true;
                            } else {
                                break;
                            }
                        }
                        None => {
                            stream
                                .get_mut()
                                .write_all(b"430 no such article\r\n")
                                .await
                                .unwrap();
                        }
                    }
                } else if command.eq_ignore_ascii_case("QUIT") {
                    stream
                        .get_mut()
                        .write_all(b"205 closing\r\n")
                        .await
                        .unwrap();
                    stream.get_mut().flush().await.unwrap();
                    break;
                } else {
                    stream
                        .get_mut()
                        .write_all(b"500 command not recognized\r\n")
                        .await
                        .unwrap();
                }
                stream.get_mut().flush().await.unwrap();
            }
        });
    });

    let config = ServerConfig {
        host: "localhost".to_string(),
        port,
        tls: true,
        starttls: false,
        username: None,
        password: None,
        connect_timeout: Duration::from_secs(5),
        command_timeout: Duration::from_secs(5),
        buffer_profile: NntpBufferProfile::default(),
        tls_ca_cert: Some(ca_path.clone()),
        tls_name_mismatch_certificate_der: None,
        pipelining: crate::connection::PipeliningCapability::Probe,
        pipelining_depth: None,
        tls_cipher_preference: TlsCipherPreference::Auto,
    };
    (config, handle, ca_path)
}

#[cfg(not(windows))]
fn spawn_partial_tls_record_proxy(
    upstream_port: u16,
    stall: Duration,
) -> (u16, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        let (mut downstream, _) = listener.accept().unwrap();
        let mut upstream = TcpStream::connect(("127.0.0.1", upstream_port)).unwrap();
        let mut request_reader = downstream.try_clone().unwrap();
        let mut request_writer = upstream.try_clone().unwrap();
        let request_forwarder = std::thread::spawn(move || {
            let _ = io::copy(&mut request_reader, &mut request_writer);
        });

        let mut fragmented = false;
        loop {
            let mut header = [0u8; 5];
            if upstream.read_exact(&mut header).is_err() {
                break;
            }
            let record_len = usize::from(u16::from_be_bytes([header[3], header[4]]));
            let mut payload = vec![0u8; record_len];
            if upstream.read_exact(&mut payload).is_err() {
                break;
            }

            if header[0] == 0x17 && record_len >= 8 * 1024 {
                downstream.write_all(&header).unwrap();
                downstream.write_all(&payload[..record_len / 2]).unwrap();
                downstream.flush().unwrap();
                fragmented = true;
                std::thread::sleep(stall);
                break;
            }

            downstream.write_all(&header).unwrap();
            downstream.write_all(&payload).unwrap();
            downstream.flush().unwrap();
        }

        let _ = downstream.shutdown(std::net::Shutdown::Both);
        let _ = upstream.shutdown(std::net::Shutdown::Both);
        let _ = request_forwarder.join();
        assert!(
            fragmented,
            "proxy never observed a large TLS application record"
        );
    });
    (port, handle)
}

fn test_transfer_control(
    stable_id: u32,
    limit_bytes: u64,
) -> Arc<crate::transfer::ServerTransferControl> {
    crate::transfer::ServerTransferRegistry::new().configure(
        StableServerId(stable_id),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes,
                generation: 1,
                retry_at: None,
            }),
        },
    )
}

fn connect_with_backend(config: &ServerConfig, backend: NntpTlsBackend) -> BlockingNntpConnection {
    BlockingNntpConnection::connect_with_ip_policy_with_backend(config, &[], 0, Some(backend), None)
        .unwrap()
}

/// A plain server that answers each AUTHINFO line as it arrives, then
/// answers nothing until every pipelined line has arrived — so a client
/// that pipelined AUTHINFO, or serialized the rest of the setup, fails.
fn spawn_blocking_pipelined_setup_server(
    auth: Vec<(&'static str, &'static [u8])>,
    expected: Vec<&'static str>,
    responses: &'static [u8],
) -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket.write_all(b"200 ready\r\n").unwrap();
        let mut reader = std::io::BufReader::new(socket.try_clone().unwrap());
        for (prefix, response) in auth {
            let mut line = String::new();
            let read = reader.read_line(&mut line);
            assert!(matches!(read, Ok(n) if n > 0), "expected {prefix:?}");
            assert!(
                line.starts_with(prefix),
                "expected {prefix:?} but received {line:?}"
            );
            socket.write_all(response).unwrap();
        }
        let mut lines = Vec::new();
        while lines.len() < expected.len() {
            let mut line = String::new();
            let read = reader.read_line(&mut line);
            assert!(
                matches!(read, Ok(n) if n > 0),
                "client did not pipeline the setup commands; received {lines:?}"
            );
            lines.push(line);
        }
        for (line, prefix) in lines.iter().zip(&expected) {
            assert!(
                line.starts_with(prefix),
                "expected {prefix:?} but received {line:?}"
            );
        }
        socket.write_all(responses).unwrap();
        let mut quit = String::new();
        let _ = reader.read_line(&mut quit);
    });
    (port, handle)
}

fn blocking_pipelined_setup_config(port: u16) -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port,
        tls: false,
        username: Some("user".into()),
        password: Some("pass".into()),
        pipelining: crate::connection::PipeliningCapability::Known(true),
        connect_timeout: Duration::from_secs(1),
        command_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}

/// A plain server that answers probe commands from a fixed spool, recording
/// every command line it received so a test can assert what was asked.
///
/// `stat_reply` and `head_reply` stand in for the status line a server sends
/// for an article it does not hold, so a test can make either command look
/// unimplemented.
fn spawn_blocking_probe_server(
    held: &'static [&'static str],
    stat_reply: &'static [u8],
    head_reply: &'static [u8],
) -> (u16, Arc<Mutex<Vec<String>>>, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&seen);
    let handle = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket.write_all(b"200 ready\r\n").unwrap();
        let mut reader = std::io::BufReader::new(socket.try_clone().unwrap());
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let command = line.trim_end_matches(['\r', '\n']).to_string();
            recorder.lock().unwrap().push(command.clone());
            let upper = command.to_ascii_uppercase();
            let response: Vec<u8> = if upper.starts_with("AUTHINFO USER") {
                b"381 password\r\n".to_vec()
            } else if upper.starts_with("AUTHINFO PASS") {
                b"281 authenticated\r\n".to_vec()
            } else if let Some(id) = upper.strip_prefix("STAT ") {
                if held.contains(&id.trim()) {
                    format!("223 0 {} article exists\r\n", id.trim()).into_bytes()
                } else {
                    stat_reply.to_vec()
                }
            } else if let Some(id) = upper.strip_prefix("HEAD ") {
                if held.contains(&id.trim()) {
                    format!(
                        "221 0 {} headers follow\r\nSubject: probe\r\n.\r\n",
                        id.trim()
                    )
                    .into_bytes()
                } else {
                    head_reply.to_vec()
                }
            } else if upper.starts_with("QUIT") {
                let _ = socket.write_all(b"205 closing\r\n");
                break;
            } else {
                b"500 command not recognized\r\n".to_vec()
            };
            if socket.write_all(&response).is_err() {
                break;
            }
        }
    });
    (port, seen, handle)
}

fn probe_config(port: u16) -> ServerConfig {
    let mut config = blocking_pipelined_setup_config(port);
    config.username = None;
    config.password = None;
    config
}

/// The probe asks STAT for the whole batch and HEAD only for what STAT could
/// not find, because a provider's STAT index can lag its spool.
#[test]
fn a_probe_batch_heads_only_the_articles_stat_missed() {
    const HELD: &[&str] = &["<HELD@SILVER.HORIZON>"];
    let (port, seen, handle) =
        spawn_blocking_probe_server(HELD, b"430 no such article\r\n", b"430 no such article\r\n");
    let config = probe_config(port);
    crate::server_caps::forget(&config.host, config.port);

    let mut conn = BlockingNntpConnection::connect_with_ip_policy(&config, &[], 0).unwrap();
    let verdicts = conn
        .probe_exists(&[
            "<held@silver.horizon>".to_string(),
            "<gone@silver.horizon>".to_string(),
        ])
        .unwrap();
    drop(conn);
    handle.join().unwrap();

    assert_eq!(verdicts, vec![true, false]);
    let seen = seen.lock().unwrap().clone();
    let heads: Vec<&String> = seen
        .iter()
        .filter(|line| line.to_ascii_uppercase().starts_with("HEAD "))
        .collect();
    assert_eq!(
        heads.len(),
        1,
        "only the STAT miss is worth a HEAD; saw {seen:?}"
    );
    assert!(heads[0].contains("gone@silver.horizon"));
}

/// A 500 to STAT means the server does not implement it. The batch is settled
/// with HEAD instead, the fact is remembered, and the connection is untouched.
#[test]
fn a_server_without_stat_is_probed_with_head_and_stays_healthy() {
    const HELD: &[&str] = &["<HELD@SILVER.HORIZON>"];
    let (port, seen, handle) = spawn_blocking_probe_server(
        HELD,
        b"500 command not recognized\r\n",
        b"430 no such article\r\n",
    );
    let config = probe_config(port);
    crate::server_caps::forget(&config.host, config.port);

    let mut conn = BlockingNntpConnection::connect_with_ip_policy(&config, &[], 0).unwrap();
    let verdicts = conn
        .probe_exists(&[
            "<held@silver.horizon>".to_string(),
            "<gone@silver.horizon>".to_string(),
        ])
        .unwrap();
    assert_eq!(verdicts, vec![true, false]);
    assert!(!conn.poisoned, "a missing command is not a broken socket");
    assert!(!crate::server_caps::supports_stat(
        &config.host,
        config.port
    ));

    // The next batch skips STAT entirely.
    let seen_before = seen.lock().unwrap().len();
    let again = conn
        .probe_exists(&["<held@silver.horizon>".to_string()])
        .unwrap();
    drop(conn);
    handle.join().unwrap();

    assert_eq!(again, vec![true]);
    let asked: Vec<String> = seen.lock().unwrap()[seen_before..].to_vec();
    assert!(
        asked
            .iter()
            .all(|line| !line.to_ascii_uppercase().starts_with("STAT ")),
        "a retired command must not be sent again; saw {asked:?}"
    );
    crate::server_caps::forget(&config.host, config.port);
}

/// Where HEAD is the missing command, STAT's verdict is the final one: the
/// batch settles rather than reporting itself unanswerable.
#[test]
fn a_server_without_head_settles_on_the_stat_verdict() {
    const HELD: &[&str] = &["<HELD@SILVER.HORIZON>"];
    let (port, _seen, handle) = spawn_blocking_probe_server(
        HELD,
        b"430 no such article\r\n",
        b"500 command not recognized\r\n",
    );
    let config = probe_config(port);
    crate::server_caps::forget(&config.host, config.port);

    let mut conn = BlockingNntpConnection::connect_with_ip_policy(&config, &[], 0).unwrap();
    let verdicts = conn
        .probe_exists(&[
            "<held@silver.horizon>".to_string(),
            "<gone@silver.horizon>".to_string(),
        ])
        .unwrap();
    drop(conn);
    handle.join().unwrap();

    assert_eq!(verdicts, vec![true, false]);
    assert!(!crate::server_caps::supports_head(
        &config.host,
        config.port
    ));
    crate::server_caps::forget(&config.host, config.port);
}

/// A server that has proven it needs a selected group gets the GROUP in the
/// same write, after the serial AUTHINFO exchange. Nothing else joins it —
/// MODE READER is never sent to anyone.
#[test]
fn blocking_known_pipelining_servers_authenticate_then_get_the_group_in_one_write() {
    let (port, handle) = spawn_blocking_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"281 welcome\r\n"),
        ],
        vec!["GROUP alt.test"],
        b"211 1 1 1 alt.test\r\n",
    );
    crate::server_caps::note_group_required("127.0.0.1", port);

    let mut conn = BlockingNntpConnection::connect_with_ip_policy_for_group(
        &blocking_pipelined_setup_config(port),
        &[],
        0,
        Some("alt.test"),
    )
    .unwrap();

    assert_eq!(conn.current_group(), Some("alt.test"));
    // The lane's own selection short-circuits on the pipelined group.
    conn.select_group("alt.test").unwrap();
    drop(conn);
    handle.join().unwrap();
    crate::server_caps::forget("127.0.0.1", port);
}

/// Session setup is authentication and nothing else, so the first line after
/// the last AUTHINFO answer is the caller's own command.
#[test]
fn blocking_unproven_server_gets_no_mode_reader_and_no_group() {
    let (port, handle) = spawn_blocking_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"281 welcome\r\n"),
        ],
        vec!["BODY <first@example.com>"],
        b"430 no article\r\n",
    );

    let mut conn = BlockingNntpConnection::connect_with_ip_policy_for_group(
        &blocking_pipelined_setup_config(port),
        &[],
        0,
        Some("alt.test"),
    )
    .unwrap();

    assert_eq!(conn.current_group(), None);
    // The scripted server asserts the command it receives, so a 430 here
    // is the proof that BODY was the next line on the wire.
    let response = conn
        .send_command(&Command::Body(ArticleId::MessageId(
            "<first@example.com>".to_string(),
        )))
        .unwrap();
    assert_eq!(response.code.raw(), 430);
    drop(conn);
    handle.join().unwrap();
    crate::server_caps::forget("127.0.0.1", port);
}

/// A 500 to BODY is the server refusing that command and nothing more: the
/// connection stays usable and no future connection changes its behaviour.
#[test]
fn blocking_500_after_setup_leaves_the_connection_alone() {
    let (port, handle) = spawn_blocking_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"281 welcome\r\n"),
        ],
        vec!["BODY <first@example.com>"],
        b"500 unknown command\r\n",
    );

    let mut conn = BlockingNntpConnection::connect_with_ip_policy_for_group(
        &blocking_pipelined_setup_config(port),
        &[],
        0,
        None,
    )
    .unwrap();

    let response = conn
        .send_command(&Command::Body(ArticleId::MessageId(
            "<first@example.com>".to_string(),
        )))
        .unwrap();
    assert_eq!(response.code.raw(), 500);
    assert!(!conn.poisoned, "a refused command is not a broken socket");
    assert!(
        !conn.needs_group_prologue(),
        "a refused BODY says nothing about groups"
    );
    drop(conn);
    handle.join().unwrap();
    crate::server_caps::forget("127.0.0.1", port);
}

/// A 412 for a message-id fetch is the server insisting on a selected
/// group, which RFC 3977 does not require of it.
#[test]
fn blocking_412_after_setup_records_the_group_requirement() {
    let (port, handle) = spawn_blocking_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"281 welcome\r\n"),
        ],
        vec!["BODY <first@example.com>"],
        b"412 no newsgroup selected\r\n",
    );

    let mut conn = BlockingNntpConnection::connect_with_ip_policy_for_group(
        &blocking_pipelined_setup_config(port),
        &[],
        0,
        Some("alt.test"),
    )
    .unwrap();
    assert!(!conn.needs_group_prologue());

    let response = conn
        .send_command(&Command::Body(ArticleId::MessageId(
            "<first@example.com>".to_string(),
        )))
        .unwrap();
    let error = NntpError::from_status(response.code, &response.message);
    assert!(matches!(error, NntpError::NoGroupSelected));
    assert!(is_transient(&error), "discovery must allow a grouped retry");

    assert!(
        conn.needs_group_prologue(),
        "the 412 should have been recorded against the server"
    );
    assert!(conn.poisoned);
    drop(conn);
    handle.join().unwrap();
    crate::server_caps::forget("127.0.0.1", port);
}

#[test]
fn blocking_pipelined_setup_maps_a_rejected_password() {
    let (port, handle) = spawn_blocking_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"481 bad password\r\n"),
        ],
        vec![],
        b"",
    );

    let result = BlockingNntpConnection::connect_with_ip_policy(
        &blocking_pipelined_setup_config(port),
        &[],
        0,
    );

    let Err(error) = result else {
        panic!("expected a rejected password");
    };
    assert!(
        matches!(error, NntpError::AuthenticationFailed),
        "{error:?}"
    );
    handle.join().unwrap();
}

fn tls_lane_reads_single_body_response(backend: NntpTlsBackend) {
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<solo@test>",
        TestArticle::Body(b"single article body".to_vec()),
    )]);
    let mut conn = connect_with_backend(&config, backend);
    conn.select_group("alt.test").unwrap();

    let article = conn.stream_yenc_article("<solo@test>").unwrap();

    assert_eq!(article.into_data(), b"single article body");
    conn.quit().unwrap();
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

/// E12: the blocking article reader had no chunk callback at all, so every
/// caller buffered the whole article and `output_callback_cpu` was
/// permanently zero. Decoded batches must now reach the callback as they
/// are produced, in order, and concatenate to the buffered article.
fn tls_lane_delivers_decoded_batches_to_chunk_callback(backend: NntpTlsBackend) {
    // Two full 512 KiB batches plus a remainder.
    let original: Vec<u8> = (0..(2 * 512 * 1024 + 123))
        .map(|i| (i % 251) as u8)
        .collect();
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<batched@test>",
        TestArticle::Body(original.clone()),
    )]);
    let mut conn = connect_with_backend(&config, backend);
    conn.select_group("alt.test").unwrap();

    let mut delivered: Vec<Vec<u8>> = Vec::new();
    let article = conn
        .stream_yenc_article_with_chunks("<batched@test>", |chunk| {
            delivered.push(chunk.to_vec());
            Ok(())
        })
        .unwrap();

    assert_eq!(delivered.len(), 3, "batch lengths {:?}", {
        delivered.iter().map(Vec::len).collect::<Vec<_>>()
    });
    assert_eq!(delivered[0].len(), 512 * 1024);
    assert_eq!(delivered[1].len(), 512 * 1024);
    assert_eq!(delivered[2].len(), 123);

    let streamed: Vec<u8> = delivered.concat();
    assert_eq!(streamed, original);
    assert_eq!(streamed, article.to_data());
    assert_eq!(article.stats.output_batches as usize, delivered.len());

    conn.quit().unwrap();
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

fn tls_lane_reads_pipelined_body_responses(backend: NntpTlsBackend) {
    let sequential_data = vec![0x11; 512];
    let first_data = vec![0x22; 512];
    let second_data = vec![0x33; 512];
    let reset_data = vec![0x44; 512];
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![
        (
            "<sequential@test>",
            TestArticle::Body(sequential_data.clone()),
        ),
        ("<one@test>", TestArticle::Body(first_data.clone())),
        ("<two@test>", TestArticle::Body(second_data.clone())),
        ("<reset@test>", TestArticle::Body(reset_data.clone())),
    ]);
    let checkpoint_plan = CheckpointPlan::from_sizes([
        std::num::NonZeroU64::new(64).unwrap(),
        std::num::NonZeroU64::new(96).unwrap(),
    ])
    .plan;
    let mut conn = connect_with_backend(&config, backend);
    conn.select_group("alt.test").unwrap();
    conn.set_checkpoint_plan(checkpoint_plan.clone());

    let sequential = conn.stream_yenc_article("<sequential@test>").unwrap();
    let sequential_result = sequential.body.yenc().unwrap();
    assert_eq!(sequential_result.checkpoint_plan, checkpoint_plan);
    assert!(sequential_result.segments.len() > 1);
    assert_eq!(sequential.into_data(), sequential_data);

    conn.write_body_request("<one@test>").unwrap();
    conn.write_body_request("<two@test>").unwrap();
    conn.flush_commands().unwrap();

    let first = conn.stream_next_yenc_article().unwrap();
    let second = conn.stream_next_yenc_article().unwrap();

    let first_result = first.body.yenc().unwrap();
    assert_eq!(first_result.checkpoint_plan, checkpoint_plan);
    assert!(first_result.segments.len() > 1);
    assert_eq!(first.into_data(), first_data);
    let second_result = second.body.yenc().unwrap();
    assert_eq!(second_result.checkpoint_plan, checkpoint_plan);
    assert!(second_result.segments.len() > 1);
    assert_eq!(second.into_data(), second_data);

    conn.set_checkpoint_plan(CheckpointPlan::None);
    conn.write_body_request("<reset@test>").unwrap();
    conn.flush_commands().unwrap();
    let reset = conn.stream_next_yenc_article().unwrap();
    let reset_result = reset.body.yenc().unwrap();
    assert_eq!(reset_result.checkpoint_plan, CheckpointPlan::None);
    assert_eq!(reset_result.segments.len(), 1);
    assert_eq!(reset.into_data(), reset_data);

    assert!(conn.stats().tls_recv_calls > 0);
    assert!(conn.stats().tls_send_calls > 0);
    conn.quit().unwrap();
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

fn tls_lane_reads_large_pipelined_body_responses(backend: NntpTlsBackend) {
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![
        ("<one@test>", TestArticle::Body(vec![b'a'; 128 * 1024])),
        ("<two@test>", TestArticle::Body(vec![b'b'; 128 * 1024])),
    ]);
    let mut conn = connect_with_backend(&config, backend);
    conn.select_group("alt.test").unwrap();
    conn.write_body_request("<one@test>").unwrap();
    conn.write_body_request("<two@test>").unwrap();
    conn.flush_commands().unwrap();

    let first = conn.stream_next_yenc_article().unwrap();
    let second = conn.stream_next_yenc_article().unwrap();

    assert_eq!(first.to_data(), vec![b'a'; 128 * 1024]);
    assert_eq!(second.to_data(), vec![b'b'; 128 * 1024]);
    conn.quit().unwrap();
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

fn tls_lane_reports_missing_article(backend: NntpTlsBackend) {
    let (config, handle, ca_path) = spawn_tls_nntp_server(Vec::new());
    let mut conn = connect_with_backend(&config, backend);
    conn.select_group("alt.test").unwrap();

    let error = conn.stream_yenc_article("<missing@test>").unwrap_err();

    assert!(matches!(
        error,
        FusedYencError::Nntp(NntpError::NoSuchArticle { .. })
            | FusedYencError::Nntp(NntpError::ArticleNotFound)
    ));
    conn.quit().unwrap();
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

fn tls_pipeline_failure_drains_permits_before_any_reuse(backend: NntpTlsBackend) {
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<first@test>",
        TestArticle::Truncated {
            data: b"partial".to_vec(),
            wait_for_next_request: true,
        },
    )]);
    let mut conn = connect_with_backend(&config, backend);
    let control = test_transfer_control(90, 1_000);
    conn.set_transfer_control(Some(control.clone()));
    conn.write_body_request_with_estimate("<first@test>", 400)
        .unwrap();
    conn.write_body_request_with_estimate("<second@test>", 400)
        .unwrap();
    assert_eq!(conn.body_accounting.len(), 2);
    assert_eq!(control.snapshot().quota_reserved_bytes, 800);
    conn.flush_commands().unwrap();

    let error = conn.stream_next_yenc_article().unwrap_err();
    assert!(matches!(
        error,
        FusedYencError::Nntp(NntpError::ServerDisconnectedMidBody)
            | FusedYencError::Nntp(NntpError::Io(_))
            | FusedYencError::Nntp(NntpError::ConnectionClosed)
            | FusedYencError::Nntp(NntpError::TruncatedMultilineBody)
            | FusedYencError::Yenc(_)
    ));
    assert!(conn.poisoned);
    assert!(conn.body_accounting.is_empty());
    assert_eq!(control.snapshot().quota_reserved_bytes, 0);

    // Exercise the same queue as a hypothetical reuse guard: after the
    // failure drain, a fresh reservation must be the only queued permit.
    conn.reserve_body(200).unwrap();
    assert_eq!(conn.body_accounting.len(), 1);
    assert_eq!(control.snapshot().quota_reserved_bytes, 200);
    conn.abort_all_bodies();
    assert_eq!(control.snapshot().quota_reserved_bytes, 0);

    drop(conn);
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

fn tls_lane_reports_truncated_response(backend: NntpTlsBackend) {
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<truncated@test>",
        TestArticle::Truncated {
            data: b"incomplete article".to_vec(),
            wait_for_next_request: false,
        },
    )]);
    let mut conn = connect_with_backend(&config, backend);
    conn.select_group("alt.test").unwrap();

    let error = conn.stream_yenc_article("<truncated@test>").unwrap_err();

    assert!(matches!(
        error,
        FusedYencError::Nntp(NntpError::ConnectionClosed)
            | FusedYencError::Nntp(NntpError::Io(_))
            | FusedYencError::Nntp(NntpError::TruncatedMultilineBody)
            | FusedYencError::Yenc(_)
    ));
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

fn assert_blocking_known_pipelining_skips_capabilities_probe(supports_pipelining: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket.write_all(b"200 ready\r\n").unwrap();
        socket.flush().unwrap();

        // No credentials and a known capability answer: setup has nothing
        // to say, so the server must see no command at all.
        let mut reader = std::io::BufReader::new(socket.try_clone().unwrap());
        socket
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        let mut trailing = String::new();
        match reader.read_line(&mut trailing) {
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) => {}
            Ok(0) => {}
            Ok(_) => panic!("known capability mode sent an unexpected command: {trailing:?}"),
            Err(error) => panic!("unexpected read error: {error}"),
        }
    });

    let config = ServerConfig {
        host: "127.0.0.1".into(),
        port,
        tls: false,
        connect_timeout: Duration::from_secs(1),
        command_timeout: Duration::from_secs(1),
        pipelining: crate::connection::PipeliningCapability::Known(supports_pipelining),
        ..Default::default()
    };
    let conn = BlockingNntpConnection::connect_with_ip_policy(&config, &[], 0).unwrap();
    assert_eq!(
        conn.capabilities().supports_pipelining(),
        supports_pipelining
    );
    drop(conn);
    server.join().unwrap();
}

#[test]
fn blocking_known_pipelining_skips_capabilities_probe() {
    assert_blocking_known_pipelining_skips_capabilities_probe(true);
}

#[test]
fn blocking_known_non_pipelining_skips_capabilities_probe() {
    assert_blocking_known_pipelining_skips_capabilities_probe(false);
}

#[test]
fn blocking_rustls_reads_single_body_response() {
    tls_lane_reads_single_body_response(NntpTlsBackend::ManualRustls);
}

#[test]
fn blocking_rustls_reads_pipelined_body_responses() {
    tls_lane_reads_pipelined_body_responses(NntpTlsBackend::ManualRustls);
}

#[test]
fn blocking_rustls_delivers_decoded_batches_to_chunk_callback() {
    tls_lane_delivers_decoded_batches_to_chunk_callback(NntpTlsBackend::ManualRustls);
}

#[test]
fn blocking_rustls_reads_large_pipelined_body_responses() {
    tls_lane_reads_large_pipelined_body_responses(NntpTlsBackend::ManualRustls);
}

#[test]
fn blocking_rustls_reports_missing_article() {
    tls_lane_reports_missing_article(NntpTlsBackend::ManualRustls);
}

#[test]
fn blocking_rustls_reports_truncated_response() {
    tls_lane_reports_truncated_response(NntpTlsBackend::ManualRustls);
}

#[test]
fn blocking_rustls_remote_trickle_consumes_active_budget() {
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<trickle@test>",
        TestArticle::Trickle {
            data: vec![b'A'; 2_560],
            line_delay: Duration::from_millis(20),
        },
    )]);
    let mut conn = connect_with_backend(&config, NntpTlsBackend::ManualRustls);
    conn.select_group("alt.test").unwrap();
    let mut budget = ActiveTransferBudget::new(Duration::from_millis(75));

    let error = conn
        .stream_yenc_article_with_active_budget("<trickle@test>", 0, &mut budget)
        .unwrap_err();

    assert!(matches!(
        error,
        FusedYencError::Nntp(NntpError::SoftTimeout(_))
    ));
    assert!(conn.poisoned);
    drop(conn);
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

#[test]
fn blocking_rustls_delayed_initial_consumes_active_budget() {
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<delayed@test>",
        TestArticle::DelayedInitial {
            data: vec![b'A'; 128],
            delay: Duration::from_millis(300),
        },
    )]);
    let mut conn = connect_with_backend(&config, NntpTlsBackend::ManualRustls);
    conn.select_group("alt.test").unwrap();
    let mut budget = ActiveTransferBudget::new(Duration::from_millis(75));

    let error = conn
        .stream_yenc_article_with_active_budget("<delayed@test>", 0, &mut budget)
        .unwrap_err();

    assert!(matches!(
        error,
        FusedYencError::Nntp(NntpError::SoftTimeout(_))
    ));
    assert!(conn.poisoned);
    drop(conn);
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

#[test]
fn blocking_rustls_rate_wait_is_excluded_from_active_budget() {
    const BODY_LEN: usize = 2_000;
    const ACTIVE_BUDGET: Duration = Duration::from_millis(500);

    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<rate-wait@test>",
        TestArticle::Body(vec![b'A'; BODY_LEN]),
    )]);
    let mut conn = connect_with_backend(&config, NntpTlsBackend::ManualRustls);
    let control = crate::transfer::ServerTransferRegistry::new().configure(
        StableServerId(104),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 1_000,
            quota: None,
        },
    );
    conn.set_transfer_control(Some(control));
    conn.select_group("alt.test").unwrap();
    let mut budget = ActiveTransferBudget::new(ACTIVE_BUDGET);

    let article = conn
        .stream_yenc_article_with_active_budget("<rate-wait@test>", 0, &mut budget)
        .expect("deliberate rate wait must not exhaust the active budget");

    assert!(
        article.stats.throttle_wait > ACTIVE_BUDGET,
        "test must force a throttle wait longer than the active budget: {:?}",
        article.stats.throttle_wait
    );
    assert_eq!(article.into_data(), vec![b'A'; BODY_LEN]);
    conn.quit().unwrap();
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

#[test]
fn expired_active_budget_does_not_enter_blocking_rate_wait() {
    let decoded = vec![b'A'; 1_200];
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<expired-budget@test>",
        TestArticle::Body(decoded.clone()),
    )]);
    let mut conn = connect_with_backend(&config, NntpTlsBackend::ManualRustls);
    let control = crate::transfer::ServerTransferRegistry::new().configure(
        StableServerId(105),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 1_000,
            quota: None,
        },
    );
    conn.set_transfer_control(Some(control.clone()));
    conn.select_group("alt.test").unwrap();
    conn.reserve_body(0).unwrap();
    let initial = conn
        .send_command(&Command::Body(ArticleId::MessageId(
            "<expired-budget@test>".to_string(),
        )))
        .unwrap();
    let mut buffered_body = yenc_body(&decoded);
    buffered_body.extend_from_slice(b".\r\n");
    conn.read_buf = BytesMut::from(buffered_body.as_slice());
    let mut budget = ActiveTransferBudget::new(Duration::ZERO);

    let error = conn
        .stream_yenc_article_response(initial, Some(&mut budget), |_| Ok(()))
        .unwrap_err();

    assert!(matches!(
        error,
        FusedYencError::Nntp(NntpError::SoftTimeout(_))
    ));
    assert!(conn.poisoned);
    let snapshot = control.snapshot();
    assert!(snapshot.lifetime_body_bytes >= decoded.len() as u64);
    assert_eq!(snapshot.throttle_wait, Duration::ZERO);
    drop(conn);
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

#[test]
fn blocking_rustls_pipeline_failure_drains_transfer_permits() {
    tls_pipeline_failure_drains_permits_before_any_reuse(NntpTlsBackend::ManualRustls);
}

#[cfg(not(windows))]
#[test]
fn blocking_s2n_fd_reads_single_body_response() {
    let _guard = s2n_test_guard();
    tls_lane_reads_single_body_response(NntpTlsBackend::S2n);
}

#[cfg(not(windows))]
#[test]
fn blocking_s2n_fd_reads_pipelined_body_responses() {
    let _guard = s2n_test_guard();
    tls_lane_reads_pipelined_body_responses(NntpTlsBackend::S2n);
}

#[cfg(not(windows))]
#[test]
fn blocking_s2n_delivers_decoded_batches_to_chunk_callback() {
    let _guard = s2n_test_guard();
    tls_lane_delivers_decoded_batches_to_chunk_callback(NntpTlsBackend::S2n);
}

#[cfg(not(windows))]
#[test]
fn blocking_s2n_reads_large_pipelined_body_responses() {
    let _guard = s2n_test_guard();
    tls_lane_reads_large_pipelined_body_responses(NntpTlsBackend::S2n);
}

#[cfg(not(windows))]
#[test]
fn blocking_s2n_fd_reports_missing_article() {
    let _guard = s2n_test_guard();
    tls_lane_reports_missing_article(NntpTlsBackend::S2n);
}

#[cfg(not(windows))]
#[test]
fn blocking_s2n_fd_reports_truncated_response() {
    let _guard = s2n_test_guard();
    tls_lane_reports_truncated_response(NntpTlsBackend::S2n);
}

#[cfg(not(windows))]
#[test]
fn blocking_s2n_remote_trickle_consumes_active_budget() {
    let _guard = s2n_test_guard();
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<trickle@test>",
        TestArticle::Trickle {
            data: vec![b'A'; 2_560],
            line_delay: Duration::from_millis(20),
        },
    )]);
    let mut conn = connect_with_backend(&config, NntpTlsBackend::S2n);
    conn.select_group("alt.test").unwrap();
    let mut budget = ActiveTransferBudget::new(Duration::from_millis(75));

    let error = conn
        .stream_yenc_article_with_active_budget("<trickle@test>", 0, &mut budget)
        .unwrap_err();

    assert!(matches!(
        error,
        FusedYencError::Nntp(NntpError::SoftTimeout(_))
    ));
    assert!(conn.poisoned);
    drop(conn);
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

#[cfg(not(windows))]
#[test]
fn blocking_s2n_delayed_initial_consumes_active_budget() {
    let _guard = s2n_test_guard();
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<delayed@test>",
        TestArticle::DelayedInitial {
            data: vec![b'A'; 128],
            delay: Duration::from_millis(300),
        },
    )]);
    let mut conn = connect_with_backend(&config, NntpTlsBackend::S2n);
    conn.select_group("alt.test").unwrap();
    let mut budget = ActiveTransferBudget::new(Duration::from_millis(75));

    let error = conn
        .stream_yenc_article_with_active_budget("<delayed@test>", 0, &mut budget)
        .unwrap_err();

    assert!(matches!(
        error,
        FusedYencError::Nntp(NntpError::SoftTimeout(_))
    ));
    assert!(conn.poisoned);
    drop(conn);
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

#[cfg(not(windows))]
#[test]
fn blocking_s2n_retries_socket_slices_within_active_budget() {
    let _guard = s2n_test_guard();
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<within-budget@test>",
        TestArticle::DelayedInitial {
            data: vec![b'A'; 128],
            delay: Duration::from_millis(250),
        },
    )]);
    let mut conn = connect_with_backend(&config, NntpTlsBackend::S2n);
    conn.select_group("alt.test").unwrap();
    assert_s2n_socket_timeout_slice(&conn);
    let mut budget = ActiveTransferBudget::new(Duration::from_millis(500));

    let article = conn
        .stream_yenc_article_with_active_budget("<within-budget@test>", 0, &mut budget)
        .expect("socket timeout slices must retry while active budget remains");

    assert_eq!(article.into_data(), vec![b'A'; 128]);
    assert_s2n_socket_timeout_slice(&conn);
    conn.quit().unwrap();
    assert_s2n_socket_timeout_slice(&conn);
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

#[cfg(not(windows))]
#[test]
fn blocking_s2n_partial_tls_record_respects_active_budget() {
    let _guard = s2n_test_guard();
    let (mut config, server_handle, ca_path) = spawn_tls_nntp_server(vec![(
        "<partial-record@test>",
        TestArticle::Body(vec![b'A'; 256 * 1024]),
    )]);
    let (proxy_port, proxy_handle) =
        spawn_partial_tls_record_proxy(config.port, Duration::from_secs(2));
    config.port = proxy_port;

    let mut conn = connect_with_backend(&config, NntpTlsBackend::S2n);
    conn.select_group("alt.test").unwrap();
    let mut budget = ActiveTransferBudget::new(Duration::from_millis(150));
    let started = Instant::now();
    let result =
        conn.stream_yenc_article_with_active_budget("<partial-record@test>", 0, &mut budget);
    let elapsed = started.elapsed();

    drop(conn);
    proxy_handle.join().unwrap();
    let _ = server_handle.join();
    let _ = std::fs::remove_file(ca_path);

    let error = result.expect_err("an incomplete TLS record must not produce an article");
    assert!(matches!(
        error,
        FusedYencError::Nntp(NntpError::SoftTimeout(_))
    ));
    assert!(
        elapsed < Duration::from_secs(1),
        "partial TLS record ignored active budget for {elapsed:?}"
    );
}

const TLS_DRAIN_RECORD_BYTES: usize = 16 * 1024;
const TLS_DRAIN_RECORDS: usize = 8;

fn spawn_raw_tls_drain_server() -> (u16, std::thread::JoinHandle<()>, std::path::PathBuf) {
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("generate test cert");
    let cert_der = certified_key.cert.der().clone();
    let cert_pem = certified_key.cert.pem();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified_key.signing_key.serialize_der(),
    ));

    let server_provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
    let server_config = RustlsServerConfig::builder_with_provider(Arc::new(server_provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("server TLS config");

    let ca_path = unique_ca_path("blocking-rustls-drain-ca");
    std::fs::write(&ca_path, cert_pem).expect("write drain CA PEM");

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();

    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let (socket, _) = listener.accept().await.unwrap();
            let acceptor = TlsAcceptor::from(Arc::new(server_config));
            let mut tls = acceptor.accept(socket).await.unwrap();
            let record = vec![0x5Au8; TLS_DRAIN_RECORD_BYTES];
            for _ in 0..TLS_DRAIN_RECORDS {
                tls.write_all(&record).await.unwrap();
                tls.flush().await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        });
    });

    (port, handle, ca_path)
}

#[test]
#[ignore = "diagnostic read-shape probe; run explicitly with --ignored --nocapture"]
fn rustls_blocking_drain_probe() {
    let (port, handle, ca_path) = spawn_raw_tls_drain_server();
    let tcp = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut stream = BlockingManualTlsStream::connect(
        tcp,
        "localhost",
        Some(&ca_path),
        None,
        TlsCipherPreference::Auto,
        Duration::from_secs(5),
    )
    .expect("blocking rustls connect");

    let payload = TLS_DRAIN_RECORD_BYTES * TLS_DRAIN_RECORDS;
    let mut read_buf = BytesMut::with_capacity(payload);
    let mut total = 0usize;
    let mut aggregate = TransportReadStats::default();
    while total < payload {
        let (n, stats) = stream
            .read_into_buf(&mut read_buf, payload, Duration::from_secs(5))
            .expect("blocking rustls read");
        assert_ne!(n, 0, "TLS stream closed before probe payload");
        total += n;
        aggregate.add(stats);
    }

    handle.join().unwrap();
    let _ = std::fs::remove_file(&ca_path);
    let bytes_per_socket_read = (total as u64)
        .checked_div(aggregate.try_read_calls)
        .unwrap_or(0);
    println!(
        "rustls_blocking_drain_probe total_bytes={total} records={TLS_DRAIN_RECORDS} socket_reads={} tls_record_reads={} bytes_per_socket_read={bytes_per_socket_read}",
        aggregate.try_read_calls, aggregate.tls_read_calls
    );
    assert_eq!(total, payload);
    assert!(
        aggregate.try_read_calls < TLS_DRAIN_RECORDS as u64,
        "buffered manual rustls should consume multiple TLS records per socket read"
    );
}

#[test]
fn blocking_decoder_reads_single_body_response() {
    let mut decoder = FusedYencArticleDecoder::from_body_response(
        parse_response("222 0 <one@test> body follows").unwrap(),
    )
    .unwrap();
    let mut input = BytesMut::new();
    input.extend_from_slice(&yenc_body(b"hello owned lane"));
    input.extend_from_slice(b".\r\n");
    let article = decoder
        .decode_available(&mut input)
        .unwrap()
        .expect("complete article");
    assert_eq!(article.into_data(), b"hello owned lane");
    assert!(input.is_empty());
}

#[test]
fn blocking_decoder_preserves_pipelined_leftover() {
    let mut decoder = FusedYencArticleDecoder::from_body_response(
        parse_response("222 0 <one@test> body follows").unwrap(),
    )
    .unwrap();
    let mut input = BytesMut::new();
    input.extend_from_slice(&yenc_body(b"first"));
    input.extend_from_slice(b".\r\n222 0 <two@test> body follows\r\n");
    let article = decoder
        .decode_available(&mut input)
        .unwrap()
        .expect("complete article");
    assert_eq!(article.into_data(), b"first");
    assert!(std::str::from_utf8(&input).unwrap().starts_with("222 "));
}

/// A lane bound to the test server, with its own permit.
fn test_body_lane(config: &ServerConfig) -> BlockingBodyLane {
    BlockingBodyLane::connect(
        ServerId(0),
        StableServerId(9001),
        None,
        config,
        &[],
        0,
        &["alt.test".to_string()],
        Duration::from_secs(30),
        crate::pool::BlockingConnectionPermit::for_tests(),
    )
    .expect("test lane connects")
}

fn ring_payload(trace: &DecodedBodyTrace) -> Vec<u8> {
    trace
        .result
        .as_ref()
        .expect("the test server answers every article")
        .decoded
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect()
}

/// Responses come back in issue order, and the judging window closes once per
/// depth so a caller that never lets the ring drain still gets the same rung
/// verdicts the batch API produced per batch.
#[test]
fn ring_reads_responses_in_issue_order_and_closes_a_window_per_depth() {
    let payloads = [
        vec![0x11; 512],
        vec![0x22; 512],
        vec![0x33; 512],
        vec![0x44; 512],
    ];
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![
        ("<one@test>", TestArticle::Body(payloads[0].clone())),
        ("<two@test>", TestArticle::Body(payloads[1].clone())),
        ("<three@test>", TestArticle::Body(payloads[2].clone())),
        ("<four@test>", TestArticle::Body(payloads[3].clone())),
    ]);
    let mut lane = test_body_lane(&config);

    assert!(
        lane.ring_read_next().is_none(),
        "an empty ring owes nothing"
    );
    for id in ["<one@test>", "<two@test>", "<three@test>", "<four@test>"] {
        assert!(matches!(
            lane.ring_issue(id, 512, 4),
            RingIssueOutcome::Issued
        ));
    }
    assert_eq!(lane.ring_outstanding(), 4);

    let mut completions = Vec::new();
    for (index, expected) in payloads.iter().enumerate() {
        let (trace, meta) = lane.ring_read_next().expect("a response is owed");
        assert_eq!(
            &ring_payload(&trace),
            expected,
            "response {index} must be the {index}th request's article"
        );
        assert_eq!(lane.ring_outstanding(), 3 - index);
        assert!(meta.batch_clean);
        assert!(!meta.connection_discarded);
        completions.push((meta.batch_complete, meta.batch_response_count));
    }

    assert_eq!(
        completions,
        vec![(false, 0), (false, 0), (false, 0), (true, 4)]
    );
    lane.park();
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

/// The point of the ring: a request goes out while responses are still
/// arriving, so the pipe never empties at a batch boundary. Under the batch
/// API the last response of a batch was read before the next batch's first
/// command was written, costing a round trip every time.
#[test]
fn ring_never_drains_between_batches() {
    let payloads = [
        vec![0x51; 256],
        vec![0x52; 256],
        vec![0x53; 256],
        vec![0x54; 256],
    ];
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![
        ("<a@test>", TestArticle::Body(payloads[0].clone())),
        ("<b@test>", TestArticle::Body(payloads[1].clone())),
        ("<c@test>", TestArticle::Body(payloads[2].clone())),
        ("<d@test>", TestArticle::Body(payloads[3].clone())),
    ]);
    let mut lane = test_body_lane(&config);
    let ids = ["<a@test>", "<b@test>", "<c@test>", "<d@test>"];

    // Fill one pipe of depth 2, then top it back up after every response.
    for id in &ids[..2] {
        assert!(matches!(
            lane.ring_issue(id, 256, 2),
            RingIssueOutcome::Issued
        ));
    }
    let mut read = 0usize;
    let mut window_closes = 0usize;
    for id in &ids[2..] {
        let (trace, meta) = lane.ring_read_next().expect("a response is owed");
        assert_eq!(&ring_payload(&trace), &payloads[read]);
        read += 1;
        if meta.batch_complete {
            window_closes += 1;
        }
        assert_eq!(
            lane.ring_outstanding(),
            1,
            "one request stays on the wire while its predecessor is decoded"
        );
        assert!(matches!(
            lane.ring_issue(id, 256, 2),
            RingIssueOutcome::Issued
        ));
        assert_eq!(lane.ring_outstanding(), 2);
    }
    while let Some((trace, meta)) = lane.ring_read_next() {
        assert_eq!(&ring_payload(&trace), &payloads[read]);
        read += 1;
        if meta.batch_complete {
            window_closes += 1;
        }
    }

    assert_eq!(read, 4);
    assert_eq!(
        window_closes, 2,
        "four responses at depth two are two rung windows"
    );
    lane.park();
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

/// Abandoning the ring must poison the connection: the responses to the
/// commands that were dropped are still in the socket, so it can never go back
/// to the pool.
#[test]
fn ring_abandon_reports_the_dropped_requests_and_poisons_the_lane() {
    let (config, handle, ca_path) = spawn_tls_nntp_server(vec![
        ("<x@test>", TestArticle::Body(vec![0x61; 128])),
        ("<y@test>", TestArticle::Body(vec![0x62; 128])),
    ]);
    let mut lane = test_body_lane(&config);
    for id in ["<x@test>", "<y@test>"] {
        assert!(matches!(
            lane.ring_issue(id, 128, 2),
            RingIssueOutcome::Issued
        ));
    }

    assert_eq!(lane.ring_abandon(), 2);
    assert!(lane.ring_is_closed());
    assert_eq!(lane.ring_outstanding(), 0);
    assert!(matches!(
        lane.ring_issue("<x@test>", 128, 2),
        RingIssueOutcome::Failed(_)
    ));

    lane.park();
    drop(handle);
    let _ = std::fs::remove_file(ca_path);
}
