use super::*;
use crate::test_support::{
    ScriptedStep, read_command_line, spawn_scripted_server as spawn_shared_scripted_server,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct ScriptStep {
    expect_prefix: Option<&'static str>,
    response: &'static [u8],
}

impl ScriptedStep for ScriptStep {
    fn expected_prefix(&self) -> Option<&str> {
        self.expect_prefix
    }

    fn response(&self) -> &[u8] {
        self.response
    }
}

async fn spawn_scripted_server(steps: Vec<ScriptStep>) -> u16 {
    spawn_shared_scripted_server(steps, Duration::ZERO).await
}

async fn spawn_trickling_body_server(line_delay: Duration) -> u16 {
    const LINE: &[u8] = b"kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk\r\n";

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"200 ready\r\n").await.unwrap();

        let capabilities = read_command_line(&mut socket).await;
        assert!(capabilities.starts_with("CAPABILITIES"));
        socket
            .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
            .await
            .unwrap();

        let body = read_command_line(&mut socket).await;
        assert!(body.starts_with("BODY "));
        socket
            .write_all(b"222 1 <trickle@example.com>\r\n=ybegin line=128 size=2560 name=x\r\n")
            .await
            .unwrap();
        socket.flush().await.unwrap();

        for _ in 0..20 {
            tokio::time::sleep(line_delay).await;
            if socket.write_all(LINE).await.is_err() || socket.flush().await.is_err() {
                return;
            }
        }
        let _ = socket.write_all(b"=yend size=2560\r\n.\r\n").await;
        let _ = socket.flush().await;
    });

    port
}

async fn spawn_delayed_body_initial_server(delay: Duration) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"200 ready\r\n").await.unwrap();

        let capabilities = read_command_line(&mut socket).await;
        assert!(capabilities.starts_with("CAPABILITIES"));
        socket
            .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
            .await
            .unwrap();

        let body = read_command_line(&mut socket).await;
        assert!(body.starts_with("BODY "));
        tokio::time::sleep(delay).await;
        let _ = socket
            .write_all(
                b"222 1 <delayed@example.com>\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n",
            )
            .await;
        let _ = socket.flush().await;
    });

    port
}

async fn spawn_delayed_reauth_server(delay: Duration) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"200 ready\r\n").await.unwrap();

        let initial_user = read_command_line(&mut socket).await;
        assert!(initial_user.starts_with("AUTHINFO USER "));
        socket
            .write_all(b"281 authentication accepted\r\n")
            .await
            .unwrap();

        let authenticated_capabilities = read_command_line(&mut socket).await;
        assert!(authenticated_capabilities.starts_with("CAPABILITIES"));
        socket
            .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
            .await
            .unwrap();

        let body = read_command_line(&mut socket).await;
        assert!(body.starts_with("BODY "));
        socket
            .write_all(b"480 authentication required\r\n")
            .await
            .unwrap();

        let user = read_command_line(&mut socket).await;
        assert!(user.starts_with("AUTHINFO USER "));
        tokio::time::sleep(delay).await;
        let _ = socket.write_all(b"381 password required\r\n").await;
        let _ = socket.flush().await;
    });

    port
}

async fn spawn_unterminated_body_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"200 ready\r\n").await.unwrap();

        let capabilities = read_command_line(&mut socket).await;
        assert!(capabilities.starts_with("CAPABILITIES"));
        socket
            .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
            .await
            .unwrap();

        let group = read_command_line(&mut socket).await;
        assert!(group.starts_with("GROUP "));
        socket
            .write_all(b"211 1 1 1 alt.binaries.test\r\n")
            .await
            .unwrap();

        let body = read_command_line(&mut socket).await;
        assert!(body.starts_with("BODY "));
        socket
            .write_all(b"222 1 <unterminated@example.com>\r\n")
            .await
            .unwrap();
        socket.write_all(&vec![b'x'; 64 * 1024]).await.unwrap();
        socket.flush().await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
    });

    port
}

async fn try_read_command_line(socket: &mut TcpStream) -> Option<String> {
    let mut buf = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        let n = socket.read(&mut byte).await.unwrap();
        if n == 0 {
            if buf.is_empty() {
                return None;
            }
            panic!("client closed connection before command completed");
        }
        buf.push(byte[0]);
        if byte[0] == b'\n' {
            return Some(String::from_utf8(buf).unwrap());
        }
    }
}

async fn spawn_probe_confirmation_server(head_response: &'static [u8]) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        loop {
            let accept = tokio::time::timeout(Duration::from_secs(1), listener.accept()).await;
            let Ok(Ok((mut socket, _))) = accept else {
                return;
            };

            tokio::spawn(async move {
                socket.write_all(b"200 ready\r\n").await.unwrap();
                socket.flush().await.unwrap();

                while let Some(line) = try_read_command_line(&mut socket).await {
                    if line.starts_with("MODE READER") {
                        socket
                            .write_all(b"500 MODE READER unsupported\r\n")
                            .await
                            .unwrap();
                        socket.flush().await.unwrap();
                        continue;
                    }

                    if line.starts_with("CAPABILITIES") {
                        socket
                            .write_all(
                                b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
                            )
                            .await
                            .unwrap();
                        socket.flush().await.unwrap();
                        continue;
                    }

                    if line.starts_with("STAT ") {
                        socket.write_all(b"430 No such article\r\n").await.unwrap();
                        socket.flush().await.unwrap();
                        continue;
                    }

                    if line.starts_with("HEAD ") {
                        if !head_response.is_empty() {
                            socket.write_all(head_response).await.unwrap();
                            socket.flush().await.unwrap();
                        }
                        break;
                    }

                    panic!("unexpected command line: {line:?}");
                }
            });
        }
    });

    port
}

/// A server on which STAT finds nothing, and which insists that the HEAD
/// re-check arrive as one pipelined batch: the second HEAD has to be on the
/// wire before the first is answered. Answers the first id as present and the
/// second as missing.
async fn spawn_pipelined_head_recheck_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"200 ready\r\n").await.unwrap();
        socket.flush().await.unwrap();

        while let Some(line) = try_read_command_line(&mut socket).await {
            if line.starts_with("MODE READER") {
                socket
                    .write_all(b"500 MODE READER unsupported\r\n")
                    .await
                    .unwrap();
                socket.flush().await.unwrap();
                continue;
            }
            if line.starts_with("CAPABILITIES") {
                socket
                    .write_all(
                        b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
                    )
                    .await
                    .unwrap();
                socket.flush().await.unwrap();
                continue;
            }
            if line.starts_with("STAT ") {
                socket.write_all(b"430 No such article\r\n").await.unwrap();
                socket.flush().await.unwrap();
                continue;
            }
            if line.starts_with("HEAD <first@example.com>") {
                let second = tokio::time::timeout(
                    Duration::from_millis(250),
                    read_command_line(&mut socket),
                )
                .await
                .expect("the HEAD re-check must send every miss before reading an answer");
                assert!(
                    second.starts_with("HEAD <second@example.com>"),
                    "unexpected second command: {second:?}"
                );
                socket
                    .write_all(
                        b"221 0 <first@example.com> Headers follow\r\nSubject: still here\r\n.\r\n430 No such article\r\n",
                    )
                    .await
                    .unwrap();
                socket.flush().await.unwrap();
                continue;
            }
            panic!("unexpected command line: {line:?}");
        }
    });

    port
}

fn scripted_server(port: u16, group: usize) -> ServerPoolConfig {
    ServerPoolConfig {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: false,
            connect_timeout: Duration::from_secs(1),
            command_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        max_connections: 2,
        group: group as u32,
        ..ServerPoolConfig::default()
    }
}

fn yenc_body_response(message_id: &str, data: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    weaver_yenc::encode(data, &mut encoded, 128, "checkpoint-plan.bin").unwrap();

    let mut response = format!("222 0 {message_id} body follows\r\n").into_bytes();
    response.extend_from_slice(&encoded);
    response.extend_from_slice(b".\r\n");
    response
}

async fn spawn_checkpoint_plan_pipelining_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"200 ready\r\n").await.unwrap();
        socket.flush().await.unwrap();

        let capabilities = read_command_line(&mut socket).await;
        assert!(capabilities.starts_with("CAPABILITIES"));
        socket
            .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n")
            .await
            .unwrap();
        socket.flush().await.unwrap();

        let first = read_command_line(&mut socket).await;
        assert_eq!(first, "BODY <first@checkpoint.test>\r\n");
        let second = tokio::time::timeout(Duration::from_secs(1), read_command_line(&mut socket))
            .await
            .expect("depth-2 lane must issue both BODY commands before a response");
        assert_eq!(second, "BODY <second@checkpoint.test>\r\n");
        socket
            .write_all(&yenc_body_response(
                "<first@checkpoint.test>",
                &(0..512u16).map(|byte| byte as u8).collect::<Vec<_>>(),
            ))
            .await
            .unwrap();
        socket
            .write_all(&yenc_body_response(
                "<second@checkpoint.test>",
                &(0..512u16)
                    .map(|byte| (byte.wrapping_mul(17)) as u8)
                    .collect::<Vec<_>>(),
            ))
            .await
            .unwrap();
        socket.flush().await.unwrap();

        let refill = read_command_line(&mut socket).await;
        assert_eq!(refill, "BODY <refill@checkpoint.test>\r\n");
        socket
            .write_all(&yenc_body_response(
                "<refill@checkpoint.test>",
                &vec![0x5a; 512],
            ))
            .await
            .unwrap();
        socket.flush().await.unwrap();

        let pooled = read_command_line(&mut socket).await;
        assert_eq!(pooled, "BODY <pooled@checkpoint.test>\r\n");
        socket
            .write_all(&yenc_body_response(
                "<pooled@checkpoint.test>",
                &vec![0xa5; 512],
            ))
            .await
            .unwrap();
        socket.flush().await.unwrap();
    });

    port
}

async fn spawn_stat_server(expect_pipelined: bool) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"200 ready\r\n").await.unwrap();
        socket.flush().await.unwrap();

        let first = read_command_line(&mut socket).await;
        assert!(first.starts_with("STAT <first@example.com>"));
        if expect_pipelined {
            let second =
                tokio::time::timeout(Duration::from_millis(250), read_command_line(&mut socket))
                    .await
                    .expect("known pipelining mode must send the next STAT before a response");
            assert!(second.starts_with("STAT <second@example.com>"));
            socket
                .write_all(
                    b"223 1 <first@example.com> article exists\r\n223 2 <second@example.com> article exists\r\n",
                )
                .await
                .unwrap();
        } else {
            socket
                .write_all(b"223 1 <first@example.com> article exists\r\n")
                .await
                .unwrap();
            socket.flush().await.unwrap();
            let second = read_command_line(&mut socket).await;
            assert!(second.starts_with("STAT <second@example.com>"));
            socket
                .write_all(b"223 2 <second@example.com> article exists\r\n")
                .await
                .unwrap();
        }
        socket.flush().await.unwrap();
    });

    port
}

fn scripted_blocking_s2n_server(port: u16, max_connections: usize) -> ServerPoolConfig {
    ServerPoolConfig {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: true,
            tls_ca_cert: Some(std::path::PathBuf::from("/tmp/weaver-test-ca.pem")),
            connect_timeout: Duration::from_secs(1),
            command_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        max_connections,
        group: 0,
        ..ServerPoolConfig::default()
    }
}

#[test]
fn fetch_kind_is_copy() {
    let kind = FetchKind::Body;
    let _copy = kind;
    let _another = kind;
}

#[tokio::test]
async fn body_lane_multi_checkpoint_plan_resets_for_refill_and_pool_reuse() {
    let port = spawn_checkpoint_plan_pipelining_server().await;
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });
    let checkpoint_plan = CheckpointPlan::from_sizes([
        std::num::NonZeroU64::new(64).unwrap(),
        std::num::NonZeroU64::new(96).unwrap(),
    ])
    .plan;

    let mut lane = client.acquire_body_lane(ServerId(0), &[]).await.unwrap();
    assert!(lane.supports_pipelining());
    lane.set_checkpoint_plan(checkpoint_plan.clone());
    let message_ids = [
        "<first@checkpoint.test>".to_string(),
        "<second@checkpoint.test>".to_string(),
    ];
    let mut callbacks = Vec::new();
    let stats = lane
        .fetch_decoded_pipeline(
            &message_ids.iter().map(String::as_str).collect::<Vec<_>>(),
            2,
            |index, trace, meta| {
                callbacks.push((index, trace, meta));
                std::future::ready(())
            },
        )
        .await;

    assert_eq!(stats.offered, 2);
    assert_eq!(stats.requested, 2);
    assert_eq!(stats.completed, 2);
    assert_eq!(callbacks.len(), 2);
    for (expected_index, (index, trace, _)) in callbacks.iter().enumerate() {
        assert_eq!(*index, expected_index);
        let body = trace.result.as_ref().unwrap().body.yenc().unwrap();
        assert_eq!(body.checkpoint_plan, checkpoint_plan);
        assert!(
            body.segments.len() > 1,
            "multi-grid plan must refine segments"
        );
    }

    lane.set_checkpoint_plan(CheckpointPlan::None);
    let refill = lane
        .fetch_decoded_sequential("<refill@checkpoint.test>")
        .await;
    let refill = refill.result.unwrap();
    let refill_body = refill.body.yenc().unwrap();
    assert_eq!(refill_body.checkpoint_plan, CheckpointPlan::None);
    assert_eq!(refill_body.segments.len(), 1);
    lane.park();
    // Returning a pooled connection is spawned on drop; let that task park
    // the sole fixture connection before proving the next lane reuses it.
    tokio::task::yield_now().await;

    let mut reused_lane = client.acquire_body_lane(ServerId(0), &[]).await.unwrap();
    let pooled = reused_lane
        .fetch_decoded_sequential("<pooled@checkpoint.test>")
        .await;
    let pooled = pooled.result.unwrap();
    let pooled_body = pooled.body.yenc().unwrap();
    assert_eq!(pooled_body.checkpoint_plan, CheckpointPlan::None);
    assert_eq!(pooled_body.segments.len(), 1);
}

#[test]
fn blocking_body_lane_candidate_honors_exclusions_and_capacity() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(1, 2), scripted_server(2, 0)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });

    assert!(client.has_blocking_body_lane_candidate(&[]));
    assert!(
        client.has_blocking_body_lane_candidate(&[0]),
        "a plaintext server is as good an owned-lane host as a TLS one"
    );
    assert!(!client.has_blocking_body_lane_candidate(&[0, 1]));

    let saturated = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(1, 0)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });
    assert!(!saturated.has_blocking_body_lane_candidate(&[]));
}

#[test]
fn blocking_body_lane_candidate_survives_temporary_permit_saturation() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(1, 2)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });
    let first = client
        .pool()
        .try_acquire_blocking_permit(ServerId(0))
        .unwrap();
    let second = client
        .pool()
        .try_acquire_blocking_permit(ServerId(0))
        .unwrap();

    assert_eq!(client.pool().server_load(0), (0, 2));
    assert!(
        client.has_blocking_body_lane_candidate(&[]),
        "leased permits can belong to reusable owned-lane caches"
    );

    drop((first, second));
}

#[test]
fn blocking_body_lane_candidate_survives_the_over_limit_holdoff() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(1, 2)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });

    client.pool().note_provider_over_limit(ServerId(0));

    assert!(client.pool().is_over_limit(ServerId(0)));
    assert!(
        client.has_blocking_body_lane_candidate(&[]),
        "a held-off server must keep feeding the owned lanes it already opened"
    );
}

#[test]
fn blocking_body_lane_candidate_keeps_backfill_locked_until_fill_excluded() {
    // The fill server negotiates STARTTLS, which an owned lane cannot do, so
    // the only lane candidate is a backfill server — and that must stay
    // unreachable for ordinary work.
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            ServerPoolConfig {
                server: ServerConfig {
                    starttls: true,
                    ..scripted_server(1, 0).server
                },
                ..scripted_server(1, 0)
            },
            ServerPoolConfig {
                backfill: true,
                ..scripted_blocking_s2n_server(2, 2)
            },
        ],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });

    assert!(
        !client.has_blocking_body_lane_candidate(&[]),
        "locked backfill must not serve ordinary work"
    );
    assert!(
        client.has_blocking_body_lane_candidate(&[0]),
        "excluding the fill tier unlocks backfill"
    );
    assert!(!client.has_blocking_body_lane_candidate(&[0, 1]));
}

#[tokio::test]
async fn group_requirement_discovery_retries_the_decoded_batch_item() {
    let client = multi_server_client(1);
    let mut attempts = Vec::new();
    let mut last_error = None;
    let disposition = client
        .classify_decoded_batch_item(
            0,
            None,
            "<group-required@example.com>",
            DecodedBatchItem {
                elapsed: Duration::ZERO,
                result: Err(DecodedBodyError::Nntp(NntpError::NoGroupSelected)),
            },
            &mut attempts,
            &mut last_error,
        )
        .await;

    assert!(matches!(disposition, DecodedBatchDisposition::Retry));
    assert_eq!(
        attempts[0].outcome,
        FetchAttemptOutcome::GroupSelectionRequired
    );
    assert!(matches!(
        last_error,
        Some(DecodedBodyError::Nntp(NntpError::NoGroupSelected))
    ));
    assert_eq!(
        client.pool().health().lock().await.server(0).failure_count,
        0
    );
}

#[test]
fn transient_errors() {
    assert!(is_transient(&NntpError::NoGroupSelected));
    assert!(is_transient(&NntpError::Timeout));
    assert!(is_transient(&NntpError::ConnectionClosed));
    assert!(is_transient(&NntpError::TruncatedMultilineBody));
    assert!(is_transient(&NntpError::ServerDisconnectedMidBody));
    assert!(is_transient(&NntpError::MalformedMultilineTerminator));
    assert!(is_transient(&NntpError::ServiceUnavailable));
    assert!(is_transient(&NntpError::TooManyConnections));
    assert!(is_transient(&NntpError::PoolExhausted));
    assert!(is_transient(&NntpError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "reset"
    ))));

    // Non-transient errors
    assert!(!is_transient(&NntpError::AuthenticationFailed));
    assert!(!is_transient(&NntpError::AccessDenied));
    assert!(!is_transient(&NntpError::PoolShutdown));
    assert!(!is_transient(&NntpError::ArticleNotFound));
    assert!(!is_transient(&NntpError::MalformedResponse("bad".into())));
}

#[test]
fn connection_errors() {
    assert!(is_connection_error(&NntpError::Timeout));
    assert!(is_connection_error(&NntpError::ConnectionClosed));
    assert!(is_connection_error(&NntpError::TruncatedMultilineBody));
    assert!(is_connection_error(&NntpError::ServerDisconnectedMidBody));
    assert!(is_connection_error(
        &NntpError::MalformedMultilineTerminator
    ));
    assert!(is_connection_error(&NntpError::TooManyConnections));
    assert!(is_connection_error(&NntpError::AccessDenied));
    assert!(!is_connection_error(&NntpError::ArticleNotFound));
    assert!(!is_connection_error(&NntpError::AuthenticationFailed));
}

#[test]
fn malformed_stat_response_is_retryable_transport_fault() {
    let err = NntpError::MalformedResponse("2\u{fffd}3".into());
    assert!(is_retryable_stat_error(&err));
    assert!(should_discard_stat_connection(&err));
    assert_eq!(stat_cooldown_reason(&err), Some(CooldownReason::Transport));
}

#[test]
fn client_config_default_retries() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    assert_eq!(config.max_retries_per_server, 1);
}

#[test]
fn client_config_custom_retries() {
    let mut config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    config.max_retries_per_server = 3;
    assert_eq!(config.max_retries_per_server, 3);

    let client = NntpClient::new(config);
    assert_eq!(client.max_retries_per_server, 3);
}

#[test]
fn from_pool_default_retries() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    let pool = Arc::new(NntpPool::new(PoolConfig {
        servers: config.servers,
        max_idle_age: config.max_idle_age,
        ..PoolConfig::default()
    }));
    let client = NntpClient::from_pool(pool);
    assert_eq!(client.max_retries_per_server, 1);
}

#[test]
fn transient_errors_are_retriable() {
    // Verify that the errors we consider transient would trigger retries
    assert!(is_transient(&NntpError::Timeout));
    assert!(is_transient(&NntpError::ConnectionClosed));
    assert!(is_transient(&NntpError::ServiceUnavailable));
    assert!(is_transient(&NntpError::TooManyConnections));
    assert!(is_transient(&NntpError::PoolExhausted));

    // These should NOT trigger retries
    assert!(!is_transient(&NntpError::ArticleNotFound));
    assert!(!is_transient(&NntpError::AuthenticationFailed));
    assert!(!is_transient(&NntpError::AccessDenied));
    assert!(!is_transient(&NntpError::NoSuchArticle {
        message_id: "<test@example.com>".into(),
    }));
}

#[test]
fn client_config_single() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            port: 563,
            tls: true,
            ..Default::default()
        },
        10,
    );
    assert_eq!(config.servers.len(), 1);
    assert_eq!(config.servers[0].max_connections, 10);
    assert_eq!(config.max_idle_age, Duration::from_secs(300));
}

#[test]
fn client_creation() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    let client = NntpClient::new(config);
    assert_eq!(client.pool().server_count(), 1);
}

#[tokio::test]
async fn infrastructure_admission_failures_do_not_poison_server_health() {
    let client = NntpClient::new(NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    ));

    for error in [
        NntpError::TooManyConnections,
        NntpError::PoolExhausted,
        NntpError::PoolShutdown,
    ] {
        client.record_transient_server_failure(0, &error).await;
    }
    assert_eq!(
        client.pool().health().lock().await.server(0).failure_count,
        0
    );

    client
        .record_transient_server_failure(0, &NntpError::AcquireTimeout(15))
        .await;
    assert_eq!(
        client.pool().health().lock().await.server(0).failure_count,
        0
    );

    client
        .record_transient_server_failure(0, &NntpError::SoftTimeout(15))
        .await;
    assert_eq!(
        client.pool().health().lock().await.server(0).failure_count,
        1
    );
}

fn single_permit_server(port: u16) -> ServerPoolConfig {
    let mut server = scripted_server(port, 0);
    server.max_connections = 1;
    server
}

fn probing_client(server: ServerPoolConfig) -> NntpClient {
    NntpClient::new(NntpClientConfig {
        servers: vec![server],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_millis(200),
    })
}

const READER_CAPABILITIES: &[u8] = b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n";

/// A STAT batch that never got a connection must leave the server alone.
///
/// The deadline expired while the request was still queued behind our own
/// connection semaphore, so nothing ever reached the server. Cooling it
/// down here stalls every download behind a transient cooldown when only
/// one server is configured.
#[tokio::test]
async fn stat_batch_that_never_got_a_connection_leaves_the_server_healthy() {
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: READER_CAPABILITIES,
        },
    ])
    .await;

    let client = probing_client(single_permit_server(port));

    // Occupy the server's only permit, the way an idle owned download lane
    // holds its cached connection after a job's queue drains.
    let held = client.pool().acquire(ServerId(0)).await.unwrap();

    let error = client
        .stat_many(&["<probe@silver.horizon>"])
        .await
        .expect_err("no permit was free, so the batch cannot conclude");
    assert!(matches!(error, NntpError::AcquireTimeout(_)), "{error:?}");

    {
        let health = client.pool().health().lock().await;
        assert_eq!(health.server(0).state(), &ServerState::Healthy);
        assert_eq!(health.server(0).failure_count, 0);
    }
    drop(held);
}

/// A soft timeout on a live connection is still a transport failure: the
/// command went out and the reply never came, so the server is implicated
/// and keeps its cooldown.
#[tokio::test]
async fn stat_batch_that_stalls_mid_command_still_cools_the_server_down() {
    let port = spawn_shared_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: READER_CAPABILITIES,
            },
        ],
        // Keep the socket open but silent so the STAT reply never arrives.
        Duration::from_secs(3),
    )
    .await;

    let client = probing_client(single_permit_server(port));

    let error = client
        .stat_many(&["<probe@silver.horizon>"])
        .await
        .expect_err("the server never answered the STAT");
    assert!(matches!(error, NntpError::SoftTimeout(_)), "{error:?}");

    let health = client.pool().health().lock().await;
    assert!(
        matches!(health.server(0).state(), ServerState::CoolingDown { .. }),
        "{:?}",
        health.server(0).state()
    );
}

#[tokio::test]
async fn all_async_fetch_families_keep_capacity_rejections_out_of_health() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let _ = socket.write_all(b"502 Too Many Connections\r\n").await;
            });
        }
    });
    let client = NntpClient::new(NntpClientConfig::single(
        ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: false,
            ..Default::default()
        },
        64,
    ));

    assert!(client.fetch_body("<test>").await.is_err());
    assert_eq!(
        client.pool().health().lock().await.server(0).failure_count,
        0,
        "BODY"
    );
    assert!(
        client
            .fetch_body_decoded_with_groups("<test>", &[])
            .await
            .is_err()
    );
    assert_eq!(
        client.pool().health().lock().await.server(0).failure_count,
        0,
        "decoded BODY"
    );
    assert!(client.stat_many(&["<test>"]).await.is_err());
    assert_eq!(
        client.pool().health().lock().await.server(0).failure_count,
        0,
        "STAT"
    );
    assert!(client.fetch_head("<test>").await.is_err());
    assert_eq!(
        client.pool().health().lock().await.server(0).failure_count,
        0,
        "HEAD"
    );
    assert!(client.fetch_article("<test>").await.is_err());

    let health = client.pool().health().lock().await;
    assert_eq!(health.server(0).failure_count, 0);
    assert_eq!(health.server(0).consecutive_failures, 0);
    drop(health);
    assert_eq!(client.pool().configured_connections(ServerId(0)), Some(64));
    assert!(client.pool().is_over_limit(ServerId(0)));
    server.abort();
}

#[tokio::test]
async fn blocking_tls_capacity_rejection_parks_connects_without_health_poisoning() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(563, 8)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });

    client.record_blocking_connect_failure(0, &NntpError::TooManyConnections);

    assert_eq!(client.pool().configured_connections(ServerId(0)), Some(8));
    assert!(client.pool().is_over_limit(ServerId(0)));
    let health = client.pool().health().lock().await;
    assert_eq!(health.server(0).failure_count, 0);
    assert_eq!(health.server(0).consecutive_failures, 0);
}

#[tokio::test]
async fn blocking_capacity_holdoff_never_cools_healthy_server() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(563, 2)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });

    client.record_blocking_connect_failure(0, &NntpError::TooManyConnections);
    client.record_blocking_connect_failure(0, &NntpError::TooManyConnections);

    assert_eq!(client.pool().configured_connections(ServerId(0)), Some(2));
    assert!(client.pool().is_over_limit(ServerId(0)));
    let mut health = client.pool().health().lock().await;
    assert_eq!(health.server(0).state(), &ServerState::Healthy);
    assert!(health.is_available(0));
}

#[test]
fn blocking_lane_preserves_provider_capacity_rejection() {
    let error = NntpError::from_status(crate::types::StatusCode::new(502), "Too many connections");
    let error = BlockingBodyLaneAcquireError::from_connect_error(error);

    assert!(
        matches!(
            error,
            BlockingBodyLaneAcquireError::ProviderCapacity(NntpError::TooManyConnections)
        ),
        "unexpected blocking lane error: {error:?}"
    );
}

#[tokio::test]
async fn client_shutdown() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    let client = NntpClient::new(config);
    client.shutdown().await;

    // After shutdown, fetches should fail
    let result = client.fetch_body("<test@example.com>").await;
    assert!(result.is_err());
}

#[test]
fn soft_timeout_is_transient() {
    assert!(is_transient(&NntpError::SoftTimeout(15)));
}

#[test]
fn soft_timeout_is_connection_error() {
    assert!(is_connection_error(&NntpError::SoftTimeout(15)));
}

#[test]
fn transient_failure_recording_counts_soft_timeouts() {
    assert_eq!(
        cooldown_reason(&NntpError::SoftTimeout(15)),
        Some(CooldownReason::Transport)
    );
    assert_eq!(cooldown_reason(&NntpError::TooManyConnections), None);
    assert_eq!(cooldown_reason(&NntpError::PoolExhausted), None);
    assert_eq!(
        cooldown_reason(&NntpError::Timeout),
        Some(CooldownReason::Transport)
    );
}

/// Failing to obtain a connection is local capacity, not a server fault:
/// it is still retryable, but it must never cool the server down, and it
/// carries no connection to discard.
#[test]
fn acquire_timeout_is_capacity_not_transport() {
    assert!(is_transient(&NntpError::AcquireTimeout(15)));
    assert!(is_retryable_stat_error(&NntpError::AcquireTimeout(15)));
    assert!(!is_connection_error(&NntpError::AcquireTimeout(15)));
    assert_eq!(cooldown_reason(&NntpError::AcquireTimeout(15)), None);
    assert_eq!(stat_cooldown_reason(&NntpError::AcquireTimeout(15)), None);
    assert_eq!(
        NntpError::AcquireTimeout(15).to_string(),
        "no connection available within 15s"
    );
}

#[test]
fn soft_timeout_display() {
    let err = NntpError::SoftTimeout(15);
    assert_eq!(err.to_string(), "article fetch soft timeout (15s)");
}

#[test]
fn client_config_default_soft_timeout() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    assert_eq!(config.soft_timeout, Duration::from_secs(15));
}

#[test]
fn client_soft_timeout_propagated() {
    let mut config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    config.soft_timeout = Duration::from_secs(30);
    let client = NntpClient::new(config);
    assert_eq!(client.soft_timeout, Duration::from_secs(30));
}

#[test]
fn from_pool_default_soft_timeout() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    let pool = Arc::new(NntpPool::new(PoolConfig {
        servers: config.servers,
        max_idle_age: config.max_idle_age,
        ..PoolConfig::default()
    }));
    let client = NntpClient::from_pool(pool);
    assert_eq!(client.soft_timeout, Duration::from_secs(15));
}

/// Build a multi-server client for testing server ordering.
fn multi_server_client(server_count: usize) -> NntpClient {
    let servers: Vec<ServerPoolConfig> = (0..server_count)
        .map(|i| ServerPoolConfig {
            server: ServerConfig {
                host: format!("server{i}.example.com"),
                ..Default::default()
            },
            max_connections: 10,
            group: if i < 2 { 0 } else { 1 }, // first 2 in group 0, rest in group 1
            ..ServerPoolConfig::default()
        })
        .collect();

    let pool = NntpPool::new(PoolConfig {
        servers,
        max_idle_age: Duration::from_secs(300),
        ..PoolConfig::default()
    });

    NntpClient {
        pool: Arc::new(pool),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    }
}

#[tokio::test]
async fn build_server_order_respects_priority_groups() {
    let client = multi_server_client(4);

    let order = client.build_server_order(&[]).await;
    // All 4 servers should be present.
    assert_eq!(order.len(), 4);
    // Group 0 servers (0, 1) should come before group 1 servers (2, 3).
    let group0_positions: Vec<usize> = order
        .iter()
        .position(|&x| x == 0)
        .into_iter()
        .chain(order.iter().position(|&x| x == 1))
        .collect();
    let group1_positions: Vec<usize> = order
        .iter()
        .position(|&x| x == 2)
        .into_iter()
        .chain(order.iter().position(|&x| x == 3))
        .collect();

    let max_group0 = *group0_positions.iter().max().unwrap();
    let min_group1 = *group1_positions.iter().min().unwrap();
    assert!(
        max_group0 < min_group1,
        "group 0 servers should all come before group 1 servers"
    );
}

#[tokio::test]
async fn build_server_order_excludes_servers() {
    let client = multi_server_client(4);

    let order = client.build_server_order(&[1, 3]).await;
    assert_eq!(order.len(), 2);
    assert!(!order.contains(&1));
    assert!(!order.contains(&3));
    assert!(order.contains(&0));
    assert!(order.contains(&2));
}

/// Servers: 0 = fill (group 0), 1 = fill (group 1), 2 = backfill (group 0).
fn tiered_client(backfill_flags: &[bool]) -> NntpClient {
    let servers: Vec<ServerPoolConfig> = backfill_flags
        .iter()
        .enumerate()
        .map(|(i, &backfill)| ServerPoolConfig {
            server: ServerConfig {
                host: format!("server{i}.example.com"),
                ..Default::default()
            },
            max_connections: 10,
            group: (i % 2) as u32,
            backfill,
            ..ServerPoolConfig::default()
        })
        .collect();
    let pool = NntpPool::new(PoolConfig {
        servers,
        ..PoolConfig::default()
    });
    NntpClient::from_pool(pool.into())
}

fn quota_selection_client(
    backfill_flags: &[bool],
    blocked: &[(usize, Option<Instant>)],
) -> NntpClient {
    let transfers = crate::transfer::ServerTransferRegistry::new();
    let servers = backfill_flags
        .iter()
        .enumerate()
        .map(|(idx, &backfill)| {
            let stable_id = StableServerId(1_000 + idx as u32);
            let transfer_control = blocked
                .iter()
                .find(|(blocked_idx, _)| *blocked_idx == idx)
                .map(|(_, retry_at)| {
                    transfers.configure(
                        stable_id,
                        crate::transfer::ServerTransferConfig {
                            rate_bytes_per_sec: 0,
                            quota: Some(crate::transfer::QuotaRuntimeConfig {
                                limit_bytes: 0,
                                generation: 1,
                                retry_at: *retry_at,
                            }),
                        },
                    )
                });
            ServerPoolConfig {
                server: ServerConfig {
                    host: format!("selection-{idx}.example.com"),
                    ..Default::default()
                },
                stable_id,
                transfer_control,
                max_connections: 1,
                group: 0,
                backfill,
                ..ServerPoolConfig::default()
            }
        })
        .collect();
    NntpClient::new(NntpClientConfig {
        servers,
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    })
}

#[tokio::test]
async fn estimate_selection_fails_over_large_request_but_keeps_smaller_work_moving() {
    let transfers = crate::transfer::ServerTransferRegistry::new();
    let limited_id = StableServerId(2_000);
    let limited = transfers.configure(
        limited_id,
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 100,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    let mut used = limited.try_reserve(60).unwrap();
    used.record_blocking(60);
    used.finish();
    assert!(!limited.snapshot().quota_blocked);

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            ServerPoolConfig {
                server: ServerConfig {
                    host: "limited.example.com".into(),
                    ..Default::default()
                },
                stable_id: limited_id,
                transfer_control: Some(limited),
                max_connections: 1,
                group: 0,
                ..ServerPoolConfig::default()
            },
            ServerPoolConfig {
                server: ServerConfig {
                    host: "unlimited.example.com".into(),
                    ..Default::default()
                },
                stable_id: StableServerId(2_001),
                max_connections: 1,
                group: 1,
                ..ServerPoolConfig::default()
            },
        ],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });

    let large = client.body_server_selection_with_estimate(&[], 41).await;
    assert_eq!(large.eligible, vec![ServerId(1)]);
    let rejection = large.quota_blocked.unwrap();
    assert_eq!(rejection.stable_server_id, limited_id);
    assert_eq!(rejection.requested_body_bytes, 41);
    assert!(!rejection.snapshot.quota_blocked);
    assert_eq!(
        client
            .server_quota_rejection(ServerId(0), 41)
            .unwrap()
            .requested_body_bytes,
        41
    );
    assert!(client.server_quota_rejection(ServerId(0), 40).is_none());
    assert!(client.server_quota_rejection(ServerId(1), 41).is_none());
    let blocking_large = client
        .try_blocking_body_server_selection_with_estimate(&[], 41)
        .expect("the health state is uncontended in this test");
    assert_eq!(blocking_large.eligible, vec![ServerId(1)]);
    assert_eq!(
        blocking_large.quota_blocked.unwrap().stable_server_id,
        limited_id
    );

    let small = client.body_server_selection_with_estimate(&[], 40).await;
    assert_eq!(small.eligible, vec![ServerId(0), ServerId(1)]);
    assert!(small.quota_blocked.is_none());
    assert!(!client.all_normal_fill_servers_quota_blocked());
}

#[test]
fn global_quota_predicate_requires_every_normal_fill_and_ignores_backfill() {
    let blocked_fill = quota_selection_client(&[false, true], &[(0, None)]);
    assert!(blocked_fill.all_normal_fill_servers_quota_blocked());

    let mixed_fills = quota_selection_client(&[false, false], &[(0, None)]);
    assert!(!mixed_fills.all_normal_fill_servers_quota_blocked());

    let empty_pool = quota_selection_client(&[], &[]);
    assert!(!empty_pool.all_normal_fill_servers_quota_blocked());
}

/// A 430 in the middle of a pipelined batch must not dirty the batch.
///
/// The article-not-found answer is a complete, bodyless response: the
/// reader stays in sync and the socket is reusable. Marking the batch dirty
/// made the caller discard the connection and permanently block the
/// server's pipelining proof, so a provider that legitimately does not
/// carry some articles could never be pipelined.
#[tokio::test]
async fn pipelined_article_not_found_keeps_the_batch_and_connection_clean() {
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 1 <one@miss-batch>\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"430 No such article\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 3 <three@miss-batch>\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n",
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(2),
    });
    let mut lane = client.acquire_body_lane(ServerId(0), &[]).await.unwrap();
    let message_ids = ["<one@miss-batch>", "<two@miss-batch>", "<three@miss-batch>"];
    let mut seen = Vec::new();

    let stats = lane
        .fetch_decoded_pipeline_with_estimates(&message_ids, &[0, 0, 0], 8, |idx, trace, meta| {
            seen.push((idx, trace, meta));
            std::future::ready(())
        })
        .await;

    assert_eq!(stats.requested, 3);
    assert_eq!(stats.completed, 3);
    assert_eq!(stats.unresolved, 0);
    assert!(!stats.connection_discarded);
    assert!(!stats.response_order_mismatch);

    assert_eq!(seen[0].1.attempts[0].outcome, FetchAttemptOutcome::Success);
    assert_eq!(seen[1].1.attempts[0].outcome, FetchAttemptOutcome::NotFound);
    assert_eq!(seen[2].1.attempts[0].outcome, FetchAttemptOutcome::Success);
    assert!(matches!(
        &seen[1].1.result,
        Err(DecodedBodyError::Nntp(NntpError::NoSuchArticle { .. }))
    ));
    // The two surrounding articles still decoded: the miss did not
    // desynchronise the response stream.
    assert!(seen[0].1.result.is_ok());
    assert!(seen[2].1.result.is_ok());

    for (idx, _, meta) in &seen {
        assert!(meta.batch_clean, "item {idx} dirtied the batch on a 430");
        assert!(!meta.connection_discarded, "item {idx} discarded the lane");
    }
    assert!(seen[2].2.batch_complete);
    assert_eq!(seen[2].2.batch_response_count, 3);

    lane.park();
}

#[tokio::test]
async fn quota_stopped_pipeline_reports_every_unissued_item_without_poisoning_lane() {
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 1 <first@quota-tail>\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n",
        },
    ])
    .await;

    let transfers = crate::transfer::ServerTransferRegistry::new();
    let stable_id = crate::transfer::StableServerId(303);
    let control = transfers.configure(
        stable_id,
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 500,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    let mut server = scripted_server(port, 0);
    server.stable_id = stable_id;
    server.transfer_control = Some(control);
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![server],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });
    let mut lane = client.acquire_body_lane(ServerId(0), &[]).await.unwrap();
    let message_ids = [
        String::from("<first@quota-tail>"),
        String::from("<blocked@quota-tail>"),
        String::from("<fits-after-refund@quota-tail>"),
    ];
    let estimates = [400, 200, 50];
    let mut seen = Vec::new();

    let stats = lane
        .fetch_decoded_pipeline_with_estimates(
            &message_ids.iter().map(String::as_str).collect::<Vec<_>>(),
            &estimates,
            4,
            |idx, trace, meta| {
                seen.push((idx, trace, meta));
                std::future::ready(())
            },
        )
        .await;

    assert_eq!(stats.offered, 3);
    assert_eq!(stats.requested, 1);
    assert_eq!(stats.unrequested, 2);
    assert_eq!(stats.completed, 3);
    assert_eq!(stats.unresolved, 0);
    assert_eq!(
        seen.iter().map(|(idx, _, _)| *idx).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(seen[0].1.attempts[0].outcome, FetchAttemptOutcome::Success);
    assert_eq!(
        seen[1].1.attempts[0].outcome,
        FetchAttemptOutcome::QuotaBlocked
    );
    assert_eq!(
        seen[2].1.attempts[0].outcome,
        FetchAttemptOutcome::QuotaUnrequested
    );
    assert!(matches!(
        &seen[2].1.result,
        Err(DecodedBodyError::Nntp(
            NntpError::BodyNotRequestedDueToQuota {
                preceding_rejection,
                requested_body_bytes: 50,
            }
        )) if preceding_rejection.requested_body_bytes == 200
    ));
    assert!(seen[1].2.batch_clean);
    assert!(!seen[1].2.connection_discarded);
    assert!(!seen[1].2.batch_complete);
    assert!(seen[2].2.batch_clean);
    assert!(!seen[2].2.connection_discarded);
    assert!(seen[2].2.batch_complete);
    assert_eq!(seen[2].2.unresolved_count, 0);

    lane.park();
}

#[test]
fn earliest_quota_selection_preserves_first_blocked_registry_revision() {
    let transfers = crate::transfer::ServerTransferRegistry::new();
    let later = Instant::now() + Duration::from_secs(120);
    let earlier = Instant::now() + Duration::from_secs(60);
    let first = transfers.configure(
        StableServerId(3_000),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 0,
                generation: 1,
                retry_at: Some(later),
            }),
        },
    );
    let first_rejection = first.quota_rejection_for(1).unwrap();
    let second = transfers.configure(
        StableServerId(3_001),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 0,
                generation: 1,
                retry_at: Some(earlier),
            }),
        },
    );
    let second_rejection = second.quota_rejection_for(1).unwrap();
    assert_ne!(
        first_rejection.registry_capacity_revision,
        second_rejection.registry_capacity_revision
    );

    let first_revision = first_rejection.registry_capacity_revision;
    let mut selected = None;
    retain_earliest_quota_rejection(&mut selected, first_rejection);
    retain_earliest_quota_rejection(&mut selected, second_rejection);

    let selected = selected.unwrap();
    assert_eq!(selected.stable_server_id, StableServerId(3_001));
    assert_eq!(selected.retry_at, Some(earlier));
    assert_eq!(selected.registry_capacity_revision, first_revision);
}

#[tokio::test]
async fn body_selection_reports_earliest_quota_when_all_fills_are_blocked() {
    let later = Instant::now() + Duration::from_secs(120);
    let earlier = Instant::now() + Duration::from_secs(60);
    let client = quota_selection_client(&[false, false], &[(0, Some(later)), (1, Some(earlier))]);

    let selection = client.body_server_selection(&[]).await;
    assert!(selection.eligible.is_empty());
    let rejection = selection
        .quota_blocked
        .expect("quota block must survive filtering");
    assert_eq!(rejection.stable_server_id, StableServerId(1_001));
    assert_eq!(rejection.retry_at, Some(earlier));
    assert!(client.body_server_order(&[]).await.is_empty());

    let blocking = client
        .try_acquire_blocking_body_lane(&[], &[])
        .err()
        .expect("blocking selection must surface the same quota block");
    assert!(matches!(
        blocking,
        BlockingBodyLaneAcquireError::Other(NntpError::QuotaBlocked(rejection))
            if rejection.stable_server_id == StableServerId(1_001)
                && rejection.retry_at == Some(earlier)
    ));
}

#[tokio::test]
async fn body_selection_keeps_quota_reason_when_other_fill_is_health_unavailable() {
    let retry_at = Instant::now() + Duration::from_secs(60);
    let client = quota_selection_client(&[false, false], &[(0, Some(retry_at))]);
    client
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(1, CooldownReason::Transport);

    let selection = client.body_server_selection(&[]).await;
    assert!(selection.eligible.is_empty());
    assert_eq!(
        selection.quota_blocked.unwrap().stable_server_id,
        StableServerId(1_000)
    );
}

#[tokio::test]
async fn body_selection_never_reports_quota_from_excluded_servers() {
    let first = Instant::now() + Duration::from_secs(30);
    let second = Instant::now() + Duration::from_secs(60);
    let client = quota_selection_client(&[false, false], &[(0, Some(first)), (1, Some(second))]);

    let selection = client.body_server_selection(&[0]).await;
    assert!(selection.eligible.is_empty());
    assert_eq!(
        selection.quota_blocked.unwrap().stable_server_id,
        StableServerId(1_001)
    );

    let selection = client.body_server_selection(&[0, 1]).await;
    assert!(selection.eligible.is_empty());
    assert!(selection.quota_blocked.is_none());
}

#[tokio::test]
async fn quota_blocked_fill_does_not_unlock_or_report_locked_backfill() {
    let retry_at = Instant::now() + Duration::from_secs(60);
    let client = quota_selection_client(&[false, true], &[(0, Some(retry_at))]);

    let selection = client.body_server_selection(&[]).await;
    assert!(selection.eligible.is_empty());
    assert_eq!(
        selection.quota_blocked.unwrap().stable_server_id,
        StableServerId(1_000)
    );

    let selection = client.body_server_selection(&[0]).await;
    assert_eq!(selection.eligible, vec![ServerId(1)]);
    assert!(selection.quota_blocked.is_none());
}

#[tokio::test]
async fn backfill_server_absent_from_ordinary_order() {
    let client = tiered_client(&[false, false, true]);

    let order = client.build_server_order(&[]).await;
    assert_eq!(order.len(), 2);
    assert!(order.contains(&0));
    assert!(order.contains(&1));
    assert!(
        !order.contains(&2),
        "backfill server must not serve ordinary work"
    );
}

#[tokio::test]
async fn backfill_unlocks_only_when_all_active_fill_servers_excluded() {
    let client = tiered_client(&[false, false, true]);

    let order = client.build_server_order(&[0]).await;
    assert_eq!(
        order,
        vec![1],
        "one fill server left: backfill stays locked"
    );

    let order = client.build_server_order(&[0, 1]).await;
    assert_eq!(
        order,
        vec![2],
        "all fill servers excluded: backfill unlocks"
    );
}

#[tokio::test]
async fn cooling_fill_server_keeps_backfill_locked_and_order_empty() {
    let client = tiered_client(&[false, true]);

    client
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(0, CooldownReason::Transport);

    let order = client.build_server_order(&[]).await;
    assert!(
        order.is_empty(),
        "cooling fill tier must wait, not spill onto backfill: {order:?}"
    );
}

/// A `Disabled` fill server never produces the 430 that would put it in
/// `exclude`, so backfill has to unlock on the health state instead.
/// Without this, an article the remaining fill servers do not have is
/// pinned out of backfill for as long as the auth failure lasts.
#[tokio::test]
async fn disabled_fill_server_unlocks_backfill() {
    let client = tiered_client(&[false, false, true]);

    // Server 0: auth failure => Disabled { AuthFailure }.
    client.pool.health().lock().await.record_failure(0, true);

    // Server 1: missed the article, so it is on the exclusion ladder.
    let order = client.build_server_order(&[1]).await;
    assert!(
        order.contains(&2),
        "an auth-disabled fill server must unlock backfill: {order:?}"
    );
    assert!(
        !order.contains(&0),
        "the disabled server itself must not be ordered: {order:?}"
    );
}

/// An outage disable heals on its own; it must wait like a cooldown, not
/// spill the queue onto backfill.
#[tokio::test]
async fn outage_disabled_fill_server_keeps_backfill_locked_with_peers_excluded() {
    let client = tiered_client(&[false, false, true]);
    {
        let mut health = client.pool.health().lock().await;
        for _ in 0..crate::health::HealthConfig::default().disable_threshold {
            health.record_failure(0, false);
        }
        assert!(matches!(
            health.server(0).state(),
            ServerState::Disabled {
                reason: crate::health::DisableReason::ConsecutiveFailures,
                ..
            }
        ));
    }

    let order = client.build_server_order(&[1]).await;
    assert!(
        order.is_empty(),
        "a consecutive-failure disable is an outage, not a config error: {order:?}"
    );
    let selection = client
        .try_blocking_body_server_selection(&[1], 0)
        .expect("the health state is uncontended in this test");
    assert!(
        selection.eligible.is_empty(),
        "owned lane must not spill an outage onto backfill: {:?}",
        selection.eligible
    );
}

#[tokio::test]
async fn cooling_fill_server_keeps_backfill_locked_with_peers_excluded() {
    let client = tiered_client(&[false, false, true]);

    client
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(0, CooldownReason::Transport);

    let order = client.build_server_order(&[1]).await;
    assert!(
        !order.contains(&2),
        "a 5-10s cooldown is a blip, not an outage: {order:?}"
    );
    assert!(
        order.is_empty(),
        "the caller waits for the cooling fill server instead: {order:?}"
    );
}

/// The owned blocking lane runs the same tiering rule off a `try_lock`.
#[tokio::test]
async fn blocking_selection_unlocks_backfill_for_a_disabled_fill_server() {
    let client = tiered_client(&[false, false, true]);

    client.pool.health().lock().await.record_failure(0, true);

    let selection = client
        .try_blocking_body_server_selection(&[1], 0)
        .expect("the health state is uncontended in this test");
    assert_eq!(
        selection.eligible,
        vec![ServerId(2)],
        "owned lane must reach backfill when the fill tier is disabled"
    );

    let cooling = tiered_client(&[false, false, true]);
    cooling
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(0, CooldownReason::Transport);
    let selection = cooling
        .try_blocking_body_server_selection(&[1], 0)
        .expect("the health state is uncontended in this test");
    assert!(
        selection.eligible.is_empty(),
        "owned lane must not spill a cooldown onto backfill: {:?}",
        selection.eligible
    );
}

/// Contention is "ask again", not a tiering verdict: it must requeue on
/// the owned fast path without being mistaken for capacity admission.
#[test]
fn selection_contention_is_requeued_but_is_not_capacity_admission() {
    let contended = BlockingBodyLaneAcquireError::SelectionContended;
    assert!(!contended.is_capacity_admission());
    assert!(contended.should_requeue_owned_work());

    assert!(BlockingBodyLaneAcquireError::LocalCapacity.should_requeue_owned_work());
    assert!(
        BlockingBodyLaneAcquireError::ProviderCapacity(NntpError::TooManyConnections)
            .should_requeue_owned_work()
    );
    assert!(!BlockingBodyLaneAcquireError::NoEligibleServer.should_requeue_owned_work());
    assert!(
        !BlockingBodyLaneAcquireError::Other(NntpError::PoolShutdown).should_requeue_owned_work()
    );
}

/// The dispatcher asks this question before it decides between an owned lane
/// and the async path, and a momentary lock collision is not an answer about
/// servers. Collapsing it into "no candidate" sent the batch to the async
/// path — which then dialled its own connection while the owned lane sat on a
/// warm one.
#[tokio::test]
async fn candidacy_separates_contention_from_having_no_candidate() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(1, 2)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });
    assert_eq!(
        client.blocking_body_lane_candidacy(&[]),
        BlockingBodyLaneCandidacy::Candidate
    );
    assert_eq!(
        client.blocking_body_lane_candidacy(&[0]),
        BlockingBodyLaneCandidacy::None,
        "the only server is excluded, so there is genuinely no candidate"
    );

    let health = client.pool.health().clone();
    let guard = health.lock().await;
    assert_eq!(
        client.blocking_body_lane_candidacy(&[]),
        BlockingBodyLaneCandidacy::Contended,
        "a held health mutex is 'ask again', not 'no candidate'"
    );
    drop(guard);
    assert_eq!(
        client.blocking_body_lane_candidacy(&[]),
        BlockingBodyLaneCandidacy::Candidate
    );
}

/// A held health mutex used to surface as `NoEligibleServer`, which reads
/// as "no server can serve this" and pushed the work off the owned lane.
#[tokio::test]
async fn contended_health_mutex_reports_contention_not_an_empty_tier() {
    let client = tiered_client(&[false]);
    let health = client.pool.health().clone();
    let guard = health.lock().await;

    let Err(error) = client.try_acquire_blocking_body_lane_with_estimate(&[], &[], 0) else {
        panic!("selection cannot rank candidates while health is held");
    };
    assert!(
        matches!(error, BlockingBodyLaneAcquireError::SelectionContended),
        "contention must not be reported as a tiering verdict: {error:?}"
    );
    assert!(error.should_requeue_owned_work());

    drop(guard);
    // With the lock free the same call reaches an ordinary tiering verdict
    // again rather than contention.
    let Err(error) = client.try_acquire_blocking_body_lane_with_estimate(&[], &[0], 0) else {
        panic!("the only server is excluded, so no lane can be acquired");
    };
    assert!(
        matches!(error, BlockingBodyLaneAcquireError::NoEligibleServer),
        "an excluded tier is still an empty tier: {error:?}"
    );
}

#[tokio::test]
async fn all_backfill_config_degrades_to_fill_with_warning() {
    let client = tiered_client(&[true, true]);

    let order = client.build_server_order(&[]).await;
    assert_eq!(order.len(), 2, "all-backfill config is normalized to fill");
}

#[tokio::test]
async fn backfill_server_that_misses_joins_exclusion_ladder() {
    let client = tiered_client(&[false, true]);

    // Fill server missed the article, backfill unlocked…
    let order = client.build_server_order(&[0]).await;
    assert_eq!(order, vec![1]);
    // …and a backfill miss exhausts the order entirely.
    let order = client.build_server_order(&[0, 1]).await;
    assert!(order.is_empty());
}

#[tokio::test]
async fn fetch_body_with_groups_traced_reports_successful_server_idx() {
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("GROUP "),
            response: b"211 1 1 1 alt.binaries.test\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 1 <exists@example.com>\r\npayload\r\n.\r\n",
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let trace = client
        .fetch_body_with_groups_traced("<exists@example.com>", &[String::from("alt.binaries.test")])
        .await;

    assert!(trace.result.is_ok(), "traced fetch should succeed");
    assert_eq!(trace.attempts.len(), 1);
    assert_eq!(trace.attempts[0].server_idx, 0);
    assert_eq!(
        trace.attempts[0].remote_ip,
        Some("127.0.0.1".parse().unwrap())
    );
    assert_eq!(trace.attempts[0].outcome, FetchAttemptOutcome::Success);
}

#[tokio::test]
async fn decoded_quota_rejection_fails_over_to_another_fill_server() {
    let capped_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
    ])
    .await;
    let healthy_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 1 <quota-failover@example.com>\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n",
        },
    ])
    .await;

    let transfers = crate::transfer::ServerTransferRegistry::new();
    let capped_control = transfers.configure(
        crate::transfer::StableServerId(101),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 0,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    let mut capped = scripted_server(capped_port, 0);
    capped.stable_id = crate::transfer::StableServerId(101);
    capped.transfer_control = Some(capped_control);
    let healthy = scripted_server(healthy_port, 1);
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![capped, healthy],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });

    let trace = client
        .fetch_body_decoded_with_groups_traced("<quota-failover@example.com>", &[])
        .await;
    let decoded = trace.result.expect("second fill server should succeed");
    let decoded_bytes: Vec<u8> = decoded
        .decoded
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    assert_eq!(decoded_bytes, b"A");
    assert_eq!(trace.attempts.len(), 2);
    assert_eq!(trace.attempts[0].outcome, FetchAttemptOutcome::QuotaBlocked);
    assert_eq!(trace.attempts[1].outcome, FetchAttemptOutcome::Success);
}

#[tokio::test]
async fn decoded_quota_rejection_does_not_unlock_backfill() {
    let capped_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
    ])
    .await;
    let backfill_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 1 <backfill-locked@example.com>\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n",
        },
    ])
    .await;

    let transfers = crate::transfer::ServerTransferRegistry::new();
    let capped_control = transfers.configure(
        crate::transfer::StableServerId(102),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 0,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    let mut capped = scripted_server(capped_port, 0);
    capped.stable_id = crate::transfer::StableServerId(102);
    capped.transfer_control = Some(capped_control);
    let mut backfill = scripted_server(backfill_port, 0);
    backfill.backfill = true;
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![capped, backfill],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });

    let trace = client
        .fetch_body_decoded_with_groups_traced("<backfill-locked@example.com>", &[])
        .await;
    assert!(matches!(
        trace.result,
        Err(DecodedBodyError::Nntp(NntpError::QuotaBlocked(_)))
    ));
    assert_eq!(trace.attempts.len(), 1);
    assert_eq!(trace.attempts[0].server_idx, 0);
    assert_eq!(trace.attempts[0].outcome, FetchAttemptOutcome::QuotaBlocked);
}

#[tokio::test]
async fn deliberate_rate_wait_does_not_trip_decoded_soft_timeout_or_health() {
    const DECODED_SIZE: usize = 1_200;
    let mut response =
        b"222 1 <rate-wait@example.com>\r\n=ybegin line=128 size=1200 name=x\r\n".to_vec();
    for offset in (0..DECODED_SIZE).step_by(128) {
        let line_len = (DECODED_SIZE - offset).min(128);
        response.extend(std::iter::repeat_n(b'k', line_len));
        response.extend_from_slice(b"\r\n");
    }
    response.extend_from_slice(b"=yend size=1200\r\n.\r\n");
    let response: &'static [u8] = Box::leak(response.into_boxed_slice());
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response,
        },
    ])
    .await;

    let transfers = crate::transfer::ServerTransferRegistry::new();
    let control = transfers.configure(
        crate::transfer::StableServerId(103),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 1_000,
            quota: None,
        },
    );
    let mut server = scripted_server(port, 0);
    server.stable_id = crate::transfer::StableServerId(103);
    server.transfer_control = Some(control.clone());
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![server],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_millis(50),
    });

    let started = Instant::now();
    let trace = client
        .fetch_body_decoded_with_groups_traced("<rate-wait@example.com>", &[])
        .await;
    assert!(started.elapsed() >= Duration::from_millis(100));
    let decoded = trace.result.expect("deliberate wait must not time out");
    let expected = vec![b'A'; DECODED_SIZE];
    let decoded_bytes: Vec<u8> = decoded
        .decoded
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    assert_eq!(decoded_bytes, expected);
    assert_eq!(trace.attempts.len(), 1);
    assert_eq!(trace.attempts[0].outcome, FetchAttemptOutcome::Success);
    assert_eq!(
        *client.pool.health().lock().await.server(0).state(),
        crate::health::ServerState::Healthy
    );
    assert!(control.snapshot().lifetime_body_bytes > 1_000);
}

#[tokio::test]
async fn remote_trickle_consumes_active_budget_and_fails_over() {
    let primary_port = spawn_trickling_body_server(Duration::from_millis(20)).await;
    let mut backup_response =
        b"222 1 <trickle@example.com>\r\n=ybegin line=128 size=128 name=x\r\n".to_vec();
    backup_response.extend(std::iter::repeat_n(b'k', 128));
    backup_response.extend_from_slice(b"\r\n=yend size=128\r\n.\r\n");
    let backup_response: &'static [u8] = Box::leak(backup_response.into_boxed_slice());
    let backup_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: backup_response,
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            scripted_server(primary_port, 0),
            scripted_server(backup_port, 1),
        ],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_millis(75),
    });

    let trace = tokio::time::timeout(
        Duration::from_secs(2),
        client.fetch_body_decoded_with_groups_traced("<trickle@example.com>", &[]),
    )
    .await
    .expect("trickle failover must complete before the watchdog");
    let decoded = trace.result.expect("backup should satisfy the BODY fetch");
    assert_eq!(decoded.decoded.concat(), vec![b'A'; 128]);
    assert_eq!(trace.attempts.len(), 2);
    assert_eq!(
        trace.attempts[0].outcome,
        FetchAttemptOutcome::TransientFailure
    );
    assert!(
        trace.attempts[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("soft timeout"))
    );
    assert_eq!(trace.attempts[1].outcome, FetchAttemptOutcome::Success);
    let health = client.pool.health().lock().await;
    let primary = health.server(0);
    assert!(matches!(
        primary.state(),
        crate::health::ServerState::Healthy
    ));
    assert_eq!(primary.failure_count, 1);
    assert_eq!(primary.consecutive_failures, 1);
}

#[tokio::test]
async fn delayed_body_initial_consumes_active_budget_and_fails_over() {
    let primary_port = spawn_delayed_body_initial_server(Duration::from_millis(300)).await;
    let backup_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 1 <delayed@example.com>\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n",
        },
    ])
    .await;
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            scripted_server(primary_port, 0),
            scripted_server(backup_port, 1),
        ],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_millis(75),
    });

    let trace = tokio::time::timeout(
        Duration::from_secs(2),
        client.fetch_body_decoded_with_groups_traced("<delayed@example.com>", &[]),
    )
    .await
    .expect("initial-response failover must complete before the watchdog");

    assert_eq!(trace.result.unwrap().decoded.concat(), vec![b'A']);
    assert_eq!(trace.attempts.len(), 2);
    assert!(
        trace.attempts[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("soft timeout"))
    );
    assert_eq!(trace.attempts[1].outcome, FetchAttemptOutcome::Success);
}

#[tokio::test]
async fn delayed_reauth_consumes_active_budget_and_fails_over() {
    let primary_port = spawn_delayed_reauth_server(Duration::from_millis(300)).await;
    let backup_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 1 <reauth@example.com>\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n",
        },
    ])
    .await;
    let mut primary = scripted_server(primary_port, 0);
    primary.server.username = Some("user".to_string());
    primary.server.password = Some("pass".to_string());
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![primary, scripted_server(backup_port, 1)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_millis(75),
    });

    let trace = tokio::time::timeout(
        Duration::from_secs(2),
        client.fetch_body_decoded_with_groups_traced("<reauth@example.com>", &[]),
    )
    .await
    .expect("re-auth failover must complete before the watchdog");

    assert_eq!(trace.result.unwrap().decoded.concat(), vec![b'A']);
    assert_eq!(trace.attempts.len(), 2);
    assert!(
        trace.attempts[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("soft timeout"))
    );
    assert_eq!(trace.attempts[1].outcome, FetchAttemptOutcome::Success);
}

#[tokio::test]
async fn raw_timeout_cleanup_preserves_rate_debt_without_waiting() {
    let primary_port = spawn_unterminated_body_server().await;
    let backup_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("GROUP "),
            response: b"211 1 1 1 alt.binaries.test\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 1 <unterminated@example.com>\r\nbackup\r\n.\r\n",
        },
    ])
    .await;

    let transfers = crate::transfer::ServerTransferRegistry::new();
    let control = transfers.configure(
        crate::transfer::StableServerId(105),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 1,
            quota: None,
        },
    );
    let mut primary = scripted_server(primary_port, 0);
    primary.stable_id = crate::transfer::StableServerId(105);
    primary.transfer_control = Some(control.clone());
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![primary, scripted_server(backup_port, 1)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_millis(75),
    });

    let trace = tokio::time::timeout(
        Duration::from_secs(2),
        client.fetch_body_with_groups_traced(
            "<unterminated@example.com>",
            &[String::from("alt.binaries.test")],
        ),
    )
    .await
    .expect("raw timeout cleanup must not wait on the rate limiter");

    assert_eq!(trace.result.unwrap().as_ref(), b"backup\r\n");
    assert_eq!(trace.attempts.len(), 2);
    assert!(
        trace.attempts[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("soft timeout"))
    );
    let snapshot = control.snapshot();
    assert!(snapshot.lifetime_body_bytes >= 64 * 1024);
    assert_eq!(snapshot.throttle_wait, Duration::ZERO);
}

#[tokio::test]
async fn grouped_body_missing_plus_transport_uncertainty_is_not_authoritative_not_found() {
    let primary_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("GROUP "),
            response: b"211 1 1 1 alt.binaries.test\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"430 No such article\r\n",
        },
    ])
    .await;

    let backup_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("GROUP "),
            response: b"211 1 1 1 alt.binaries.test\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"",
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            scripted_server(primary_port, 0),
            scripted_server(backup_port, 1),
        ],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let trace = client
        .fetch_body_with_groups_traced("<maybe@example.com>", &[String::from("alt.binaries.test")])
        .await;

    assert_eq!(trace.attempts.len(), 2);
    assert!(matches!(
        trace.result,
        Err(NntpError::ConnectionClosed) | Err(NntpError::SoftTimeout(_))
    ));
}

#[tokio::test]
async fn extra_body_lane_reports_remote_ip() {
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("GROUP "),
            response: b"211 1 1 1 alt.binaries.test\r\n",
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let lane = client
        .acquire_extra_body_lane(ServerId(0), &[String::from("alt.binaries.test")])
        .await
        .expect("extra BODY lane should acquire");

    assert_eq!(
        lane.remote_ip(),
        Some("127.0.0.1".parse::<IpAddr>().unwrap())
    );
    lane.park();
}

#[tokio::test]
async fn extra_body_lane_excluding_only_ip_fails_before_group_selection() {
    let port = spawn_scripted_server(vec![]).await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let result = client
        .acquire_extra_body_lane_excluding(
            ServerId(0),
            &[String::from("alt.binaries.test")],
            &["127.0.0.1".parse().unwrap()],
        )
        .await;

    assert!(result.is_err(), "excluded only IP should not connect");
}

#[tokio::test]
async fn extra_body_lane_uses_fresh_connection_instead_of_idle_pool() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let accepted_task = accepted.clone();

    tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            accepted_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            socket.write_all(b"200 ready\r\n").await.unwrap();
            socket.flush().await.unwrap();
            let line = read_command_line(&mut socket).await;
            assert!(line.starts_with("CAPABILITIES"));
            socket
                .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                .await
                .unwrap();
            socket.flush().await.unwrap();
            // A body lane fetches by message-id, so no GROUP follows: the
            // connection is ready for the caller's own command here.
        }
    });

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let first = client
        .acquire_body_lane(ServerId(0), &[String::from("alt.binaries.test")])
        .await
        .expect("normal BODY lane should acquire");
    first.park();

    let second = client
        .acquire_extra_body_lane(ServerId(0), &[String::from("alt.binaries.test")])
        .await
        .expect("extra BODY lane should acquire fresh connection");
    second.discard().await;

    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "extra BODY lane must create a fresh connection instead of reusing idle"
    );
}

#[tokio::test]
async fn parked_extra_body_lane_is_not_returned_to_normal_idle_pool() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let accepted_task = accepted.clone();

    tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            accepted_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            socket.write_all(b"200 ready\r\n").await.unwrap();
            socket.flush().await.unwrap();
            let line = read_command_line(&mut socket).await;
            assert!(line.starts_with("CAPABILITIES"));
            socket
                .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                .await
                .unwrap();
            socket.flush().await.unwrap();
            // A body lane fetches by message-id, so no GROUP follows: the
            // connection is ready for the caller's own command here.
        }
    });

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let extra = client
        .acquire_extra_body_lane(ServerId(0), &[String::from("alt.binaries.test")])
        .await
        .expect("extra BODY lane should acquire");
    extra.park();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let normal = client
        .acquire_body_lane(ServerId(0), &[String::from("alt.binaries.test")])
        .await
        .expect("normal BODY lane should not reuse parked extra connection");
    normal.discard().await;

    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "parked over-max BODY lane must close instead of entering normal idle pool"
    );
}

#[tokio::test]
async fn fetch_body_prefer_excluding_falls_back_when_only_server_excluded() {
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("GROUP "),
            response: b"211 1 1 1 alt.binaries.test\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 1 <exists@example.com>\r\npayload\r\n.\r\n",
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let trace = client
        .fetch_body_with_groups_prefer_excluding_traced(
            "<exists@example.com>",
            &[String::from("alt.binaries.test")],
            &[0],
        )
        .await;

    assert!(trace.result.is_ok(), "fallback fetch should succeed");
    assert_eq!(trace.attempts.len(), 1);
    assert_eq!(trace.attempts[0].server_idx, 0);
    assert_eq!(trace.attempts[0].outcome, FetchAttemptOutcome::Success);
}

#[tokio::test]
async fn fetch_body_prefer_excluding_uses_alternate_server_first() {
    let backup_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("GROUP "),
            response: b"211 1 1 1 alt.binaries.test\r\n",
        },
        ScriptStep {
            expect_prefix: Some("BODY "),
            response: b"222 1 <exists@example.com>\r\nbackup\r\n.\r\n",
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(1, 0), scripted_server(backup_port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let trace = client
        .fetch_body_with_groups_prefer_excluding_traced(
            "<exists@example.com>",
            &[String::from("alt.binaries.test")],
            &[0],
        )
        .await;

    assert!(trace.result.is_ok(), "alternate fetch should succeed");
    assert_eq!(trace.attempts.len(), 1);
    assert_eq!(trace.attempts[0].server_idx, 1);
    assert_eq!(trace.attempts[0].outcome, FetchAttemptOutcome::Success);
}

#[tokio::test]
async fn weighted_selection_favours_faster_server() {
    let client = multi_server_client(2);

    // Seed server 0 with 50ms latency and server 1 with 500ms latency.
    {
        let mut health = client.pool.health().lock().await;
        health.record_latency(0, Duration::from_millis(50));
        health.record_latency(1, Duration::from_millis(500));
    }

    // Run 1000 iterations and count how often server 0 is picked first.
    let mut server0_first = 0u32;
    for _ in 0..1000 {
        let ranked = client.rank_servers_in_group(&[0, 1]).await;
        if ranked[0] == 0 {
            server0_first += 1;
        }
    }

    // Server 0 is 10x faster, so it should be picked first significantly
    // more often. With weight=1/score, server 0 weight ≈ 10x server 1 weight,
    // so server 0 should be picked ~90% of the time. Allow some variance.
    assert!(
        server0_first > 700,
        "faster server should be picked first most of the time, but was only first {server0_first}/1000 times"
    );
}

#[tokio::test]
async fn rank_single_server_returns_it() {
    let client = multi_server_client(2);
    let ranked = client.rank_servers_in_group(&[1]).await;
    assert_eq!(ranked, vec![1]);
}

#[tokio::test]
async fn rank_empty_returns_empty() {
    let client = multi_server_client(2);
    let ranked = client.rank_servers_in_group(&[]).await;
    assert!(ranked.is_empty());
}

#[tokio::test]
async fn build_server_order_prefers_healthy_over_degraded() {
    let client = multi_server_client(2);

    {
        let mut health = client.pool.health().lock().await;
        for _ in 0..5 {
            health.record_failure(1, false);
        }
    }

    let order = client.build_server_order(&[]).await;
    assert_eq!(order, vec![0, 1]);
}

#[tokio::test]
async fn build_server_order_keeps_backfill_after_primary_group() {
    let pool = NntpPool::new(PoolConfig {
        servers: vec![
            ServerPoolConfig {
                server: ServerConfig {
                    host: "saturated-primary.example.com".into(),
                    ..Default::default()
                },
                max_connections: 0,
                group: 0,
                ..ServerPoolConfig::default()
            },
            ServerPoolConfig {
                server: ServerConfig {
                    host: "available-backfill.example.com".into(),
                    ..Default::default()
                },
                max_connections: 1,
                group: 1,
                ..ServerPoolConfig::default()
            },
        ],
        max_idle_age: Duration::from_secs(300),
        ..PoolConfig::default()
    });

    let client = NntpClient {
        pool: Arc::new(pool),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    };

    let order = client.build_server_order(&[]).await;
    assert_eq!(order, vec![0, 1]);
}

#[tokio::test]
async fn build_server_order_prefers_ready_servers_within_same_group() {
    let pool = NntpPool::new(PoolConfig {
        servers: vec![
            ServerPoolConfig {
                server: ServerConfig {
                    host: "waiting-primary.example.com".into(),
                    ..Default::default()
                },
                max_connections: 0,
                group: 0,
                ..ServerPoolConfig::default()
            },
            ServerPoolConfig {
                server: ServerConfig {
                    host: "ready-primary.example.com".into(),
                    ..Default::default()
                },
                max_connections: 1,
                group: 0,
                ..ServerPoolConfig::default()
            },
        ],
        max_idle_age: Duration::from_secs(300),
        ..PoolConfig::default()
    });

    let client = NntpClient {
        pool: Arc::new(pool),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    };

    let order = client.build_server_order(&[]).await;
    assert_eq!(order, vec![1, 0]);
}

#[tokio::test]
async fn known_pipelining_pipelines_stats_without_capabilities() {
    let port = spawn_stat_server(true).await;
    let mut server = scripted_server(port, 0);
    server.server.pipelining = crate::connection::PipeliningCapability::Known(true);
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![server],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    assert_eq!(
        client
            .stat_many(&["<first@example.com>", "<second@example.com>"])
            .await
            .unwrap(),
        vec![true, true]
    );
}

#[tokio::test]
async fn known_non_pipelining_serializes_stats_without_capabilities() {
    let port = spawn_stat_server(false).await;
    let mut server = scripted_server(port, 0);
    server.server.pipelining = crate::connection::PipeliningCapability::Known(false);
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![server],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    assert_eq!(
        client
            .stat_many(&["<first@example.com>", "<second@example.com>"])
            .await
            .unwrap(),
        vec![true, true]
    );
}

#[tokio::test]
async fn stat_many_fails_over_after_malformed_pipelined_response_when_article_is_confirmed() {
    let primary_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("STAT "),
            response: b"2\xff3 malformed\r\n",
        },
    ])
    .await;

    let backup_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("STAT "),
            response: b"223 0 <exists@example.com>\r\n",
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            scripted_server(primary_port, 0),
            scripted_server(backup_port, 1),
        ],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let results = client
        .stat_many(&["<exists@example.com>"])
        .await
        .expect("stat_many should fail over");
    assert_eq!(results, vec![true]);
}

#[tokio::test]
async fn stat_many_is_inconclusive_when_malformed_response_precedes_clean_missing() {
    let primary_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("STAT "),
            response: b"2\xff3 malformed\r\n",
        },
    ])
    .await;

    let backup_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("STAT "),
            response: b"430 No such article\r\n",
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            scripted_server(primary_port, 0),
            scripted_server(backup_port, 1),
        ],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let err = client
        .stat_many(&["<missing@example.com>"])
        .await
        .expect_err("malformed STAT plus clean missing must be inconclusive");
    assert!(matches!(err, NntpError::MalformedResponse(_)));
}

#[tokio::test]
async fn confirm_exists_for_probe_recovers_false_stat_with_head_success() {
    let port = spawn_probe_confirmation_server(
        b"221 0 <exists@example.com> Headers follow\r\nSubject: still here\r\n.\r\n",
    )
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let result = client
        .confirm_exists_for_probe(&["<exists@example.com>"])
        .await;
    assert_eq!(
        result,
        ProbeBatchResult {
            exists: vec![true],
            inconclusive: false,
        }
    );
}

#[tokio::test]
async fn confirm_exists_for_probe_marks_head_transport_failure_inconclusive() {
    let port = spawn_probe_confirmation_server(b"").await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let result = client
        .confirm_exists_for_probe(&["<exists@example.com>"])
        .await;
    assert_eq!(
        result,
        ProbeBatchResult {
            exists: vec![false],
            inconclusive: true,
        }
    );
}

#[tokio::test]
async fn confirm_exists_for_probe_marks_malformed_stat_with_clean_missing_inconclusive() {
    let primary_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("STAT "),
            response: b"2\xff3 malformed\r\n",
        },
    ])
    .await;

    let backup_port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("STAT "),
            response: b"430 No such article\r\n",
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            scripted_server(primary_port, 0),
            scripted_server(backup_port, 1),
        ],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let result = client
        .confirm_exists_for_probe(&["<missing@example.com>"])
        .await;
    assert_eq!(
        result,
        ProbeBatchResult {
            exists: vec![false],
            inconclusive: true,
        }
    );
}

fn lane_config(tls: bool, starttls: bool, pinned_ca: bool) -> ServerConfig {
    ServerConfig {
        tls,
        starttls,
        tls_ca_cert: pinned_ca.then(|| std::path::PathBuf::from("/tmp/weaver-test-ca.pem")),
        ..ServerConfig::default()
    }
}

/// Owned lanes are the one download path, so a plaintext server gets one too:
/// its lane is what serves BODY, PAR2 recovery and the existence probe from a
/// single warm connection. Only STARTTLS is left out, because the blocking
/// transport has no in-band upgrade.
#[test]
fn a_plaintext_server_gets_an_owned_lane_and_a_starttls_one_does_not() {
    assert!(supports_blocking_body_lane(&lane_config(
        false, false, false
    )));
    assert!(!supports_blocking_body_lane(&lane_config(true, true, true)));
    assert!(!supports_blocking_body_lane(&lane_config(
        false, true, false
    )));
}

#[test]
fn blocking_tls_lane_eligibility_rejects_plain_and_starttls() {
    use crate::tls::NntpTlsBackend;

    assert!(!blocking_lane_tls_eligible(
        &lane_config(false, false, true),
        NntpTlsBackend::ManualRustls
    ));
    assert!(!blocking_lane_tls_eligible(
        &lane_config(true, true, true),
        NntpTlsBackend::ManualRustls
    ));
}

#[test]
fn blocking_tls_lane_eligibility_rustls_works_without_pinned_ca() {
    use crate::tls::NntpTlsBackend;

    assert!(blocking_lane_tls_eligible(
        &lane_config(true, false, false),
        NntpTlsBackend::ManualRustls
    ));
    assert!(blocking_lane_tls_eligible(
        &lane_config(true, false, true),
        NntpTlsBackend::ManualRustls
    ));
}

#[test]
fn adopted_name_mismatch_certificate_forces_the_rustls_body_lane() {
    let mut config = lane_config(true, false, false);
    config.tls_name_mismatch_certificate_der = Some(vec![0x30, 0x82, 0x01, 0x0a]);

    assert!(supports_blocking_body_lane(&config));
}

#[cfg(not(windows))]
#[test]
fn blocking_tls_lane_eligibility_s2n_requires_pinned_ca() {
    use crate::tls::NntpTlsBackend;

    assert!(!blocking_lane_tls_eligible(
        &lane_config(true, false, false),
        NntpTlsBackend::S2n
    ));
    assert!(blocking_lane_tls_eligible(
        &lane_config(true, false, true),
        NntpTlsBackend::S2n
    ));
}

/// A 501 is a syntax error in the one request, not a server without STAT.
///
/// One message-id the server cannot parse must not retire STAT for the
/// process: every later probe would then run HEAD per article, one round trip
/// each, against a server that pipelines STAT perfectly well.
#[tokio::test]
async fn a_501_to_one_stat_does_not_retire_stat_for_the_server() {
    let port = spawn_scripted_server(vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
        },
        ScriptStep {
            expect_prefix: Some("STAT "),
            response: b"501 Syntax error\r\n",
        },
    ])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let err = client
        .stat_many(&["<odd id@example.com>"])
        .await
        .expect_err("a 501 is a refusal of the request");
    assert!(
        !matches!(err, NntpError::CommandNotRecognized),
        "a 501 must not be read as the command being missing: {err:?}"
    );
    assert!(
        crate::server_caps::supports_stat("127.0.0.1", port),
        "one unparseable id must not retire STAT for the server"
    );
}

/// With every usable server excluded there is nobody left to ask, and that is
/// an answer for the caller — not an inconclusive batch, and not a dial.
#[tokio::test]
async fn a_probe_with_every_server_excluded_has_nobody_to_ask() {
    // The script would fail on any command; the point is that no connection
    // is ever opened to it.
    let port = spawn_scripted_server(vec![ScriptStep {
        expect_prefix: None,
        response: b"200 ready\r\n",
    }])
    .await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let outcome = client
        .confirm_exists_for_probe_excluding(&["<probe@silver.horizon>"], &[0])
        .await;
    assert!(
        outcome.is_none(),
        "excluding the only server leaves nothing to ask: {outcome:?}"
    );
    assert!(
        client.has_available_permit(ServerId(0)),
        "no connection may be opened on an excluded server"
    );
}

/// The HEAD re-check of STAT's misses is one pipelined batch, not one
/// failover fetch per article: N misses cost one round trip per server, and a
/// missing article is exactly the case where every server has to be asked.
#[tokio::test]
async fn the_head_recheck_of_stat_misses_is_one_pipelined_batch() {
    let port = spawn_pipelined_head_recheck_server().await;

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_server(port, 0)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(5),
    });

    let result = client
        .confirm_exists_for_probe(&["<first@example.com>", "<second@example.com>"])
        .await;
    assert_eq!(
        result,
        ProbeBatchResult {
            exists: vec![true, false],
            inconclusive: false,
        },
        "the batch's answers land on the ids they were asked about"
    );
}
