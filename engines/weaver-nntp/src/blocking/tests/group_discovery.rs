use super::*;
use crate::client::{NntpClient, NntpClientConfig};
use crate::pool::ServerPoolConfig;

#[test]
fn concurrent_group_discovery_keeps_the_provider_eligible_for_a_grouped_retry() {
    const LANES: usize = 10;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let mut handlers = Vec::new();
        for _ in 0..=LANES {
            let (mut stream, _) = listener.accept().unwrap();
            handlers.push(std::thread::spawn(move || {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                stream.write_all(b"200 ready\r\n").unwrap();
                let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                let mut selected = false;
                loop {
                    let mut line = String::new();
                    assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                    match line.trim() {
                        "AUTHINFO USER user" => stream.write_all(b"381 password\r\n").unwrap(),
                        "AUTHINFO PASS pass" => stream.write_all(b"281 welcome\r\n").unwrap(),
                        "GROUP alt.test" => {
                            selected = true;
                            stream.write_all(b"211 1 1 1 alt.test\r\n").unwrap();
                        }
                        "BODY <group@test>" => {
                            if selected {
                                stream
                                    .write_all(b"222 0 <group@test> body follows\r\n")
                                    .unwrap();
                                stream.write_all(&yenc_body(b"grouped payload")).unwrap();
                                stream.write_all(b".\r\n").unwrap();
                            } else {
                                stream.write_all(b"412 no newsgroup selected\r\n").unwrap();
                            }
                            break;
                        }
                        command => panic!("unexpected command: {command}"),
                    }
                }
            }));
        }
        for handler in handlers {
            handler.join().unwrap();
        }
    });
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![ServerPoolConfig {
            server: blocking_pipelined_setup_config(port),
            max_connections: LANES,
            ..Default::default()
        }],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(10),
    });
    let groups = vec!["alt.test".to_string()];
    // All sessions exist before the first BODY reveals the group requirement.
    let lanes: Vec<_> = (0..LANES)
        .map(|_| client.try_acquire_blocking_body_lane(&groups, &[]).unwrap())
        .collect();
    for mut lane in lanes {
        let trace = lane.fetch_decoded_sequential("<group@test>");
        assert!(matches!(
            trace.result,
            Err(DecodedBodyError::Nntp(NntpError::NoGroupSelected))
        ));
        client.record_blocking_attempts(&trace.attempts);
    }
    assert_eq!(
        client
            .pool()
            .health()
            .blocking_lock()
            .server(0)
            .failure_count,
        0
    );
    assert!(client.has_blocking_body_lane_candidate(&[]));
    let mut retry = client.try_acquire_blocking_body_lane(&groups, &[]).unwrap();
    let trace = retry.fetch_decoded_sequential("<group@test>");
    assert_eq!(ring_payload(&trace), b"grouped payload");
    client.record_blocking_attempts(&trace.attempts);
    let timeout = retry.trace_item(
        "<group@test>",
        Duration::ZERO,
        Err(DecodedBodyError::Nntp(NntpError::Timeout)),
    );
    drop(retry);
    server.join().unwrap();
    // Real transport failures must still disable an unhealthy provider.
    for _ in 0..LANES {
        client.record_blocking_attempts(&timeout.attempts);
    }
    assert_eq!(
        client
            .pool()
            .health()
            .blocking_lock()
            .server(0)
            .failure_count,
        LANES as u64
    );
    assert!(!client.has_blocking_body_lane_candidate(&[]));
    crate::server_caps::forget("127.0.0.1", port);
}
