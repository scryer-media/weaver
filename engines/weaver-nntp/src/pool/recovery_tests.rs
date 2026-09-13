use super::*;
use crate::client::{NntpClient, NntpClientConfig};
use crate::transfer::ServerTransferRegistry;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_probe_is_shared_fresh_and_single_article_until_reply() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::channel(8);
    let (answer, mut answer_rx) = tokio::sync::mpsc::channel(8);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = socket.into_split();
        writer.write_all(b"200 ready\r\n").await.unwrap();
        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            if line == "CAPABILITIES" {
                writer
                    .write_all(b"101 capabilities\r\nVERSION 2\r\nPIPELINING\r\n.\r\n")
                    .await
                    .unwrap();
            } else {
                assert!(line.starts_with("BODY "));
                seen_tx.send(line).await.unwrap();
                answer_rx.recv().await.unwrap();
                writer.write_all(b"430 unavailable\r\n").await.unwrap();
            }
        }
    });
    let registry = ServerTransferRegistry::new();
    let control = registry.control(StableServerId(7));
    let config = || NntpClientConfig {
        servers: vec![ServerPoolConfig {
            server: ServerConfig {
                host: address.ip().to_string(),
                port: address.port(),
                tls: false,
                ..Default::default()
            },
            stable_id: StableServerId(7),
            transfer_control: Some(Arc::clone(&control)),
            max_connections: 8,
            ..Default::default()
        }],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(2),
    };
    let client = NntpClient::new(config());
    let pool = client.pool();
    for _ in 0..10 {
        pool.health.lock().await.record_failure(0, false);
    }
    assert!(pool.requires_recovery(0));
    pool.recovery_gates[0].expire_now();
    let replacement = NntpClient::new(config());
    assert!(replacement.pool().requires_recovery(0));
    assert!(
        pool.try_acquire_blocking_permit_for_work(ServerId(0), false)
            .is_err()
    );
    let mut lane = client.acquire_body_lane(ServerId(0), &[]).await.unwrap();
    assert!(pool.requires_recovery(0), "handshake is insufficient");
    assert!(
        replacement
            .pool()
            .try_acquire_blocking_permit(ServerId(0))
            .is_err()
    );
    let fetch = tokio::spawn(async move {
        let stats = lane
            .fetch_decoded_pipeline(&["<first@test>", "<second@test>"], 8, |_, _, _| async {})
            .await;
        (lane, stats)
    });
    assert_eq!(seen_rx.recv().await.unwrap(), "BODY <first@test>");
    assert!(seen_rx.try_recv().is_err());
    assert_eq!(pool.socket_budget_snapshot(0).physical, 1);
    assert!(
        replacement
            .pool()
            .try_acquire_blocking_permit(ServerId(0))
            .is_err()
    );
    answer.send(()).await.unwrap();
    let (lane, stats) = fetch.await.unwrap();
    assert_eq!(stats.offered, 1);
    assert!(
        !pool.requires_recovery(0),
        "a valid 430 proves transport recovery"
    );
    assert!(!replacement.pool().requires_recovery(0));
    let peer = replacement
        .pool()
        .try_acquire_blocking_permit(ServerId(0))
        .unwrap();
    drop(peer);
    drop(lane);
    pool.drain_all_idle().await;
    server.await.unwrap();
    assert_eq!(pool.socket_budget_snapshot(0).physical, 0);
}
