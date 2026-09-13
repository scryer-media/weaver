//! Loopback regressions for mixed backend ownership. No provider is contacted.
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use weaver_nntp::transfer::{ServerTransferRegistry, StableServerId};
use weaver_nntp::{NntpPool, PoolConfig, ServerConfig, ServerId, ServerPoolConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_idle_socket_is_closed_before_blocking_slot_is_reassigned() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (closed_tx, mut closed_rx) = tokio::sync::mpsc::channel(3);
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (socket, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = socket.into_split();
            writer.write_all(b"200 ready\r\n").await.unwrap();
            let mut lines = BufReader::new(reader).lines();
            while let Some(line) = lines.next_line().await.unwrap() {
                assert_eq!(line, "CAPABILITIES");
                writer
                    .write_all(b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
                    .await
                    .unwrap();
            }
            closed_tx.send(()).await.unwrap();
        }
    });
    let registry = ServerTransferRegistry::new();
    let control = registry.control(StableServerId(7));
    let config = || PoolConfig {
        servers: vec![ServerPoolConfig {
            server: ServerConfig {
                host: address.ip().to_string(),
                port: address.port(),
                tls: false,
                ..Default::default()
            },
            stable_id: StableServerId(7),
            transfer_control: Some(Arc::clone(&control)),
            max_connections: 1,
            ..Default::default()
        }],
        ..Default::default()
    };
    let old = NntpPool::new(config());
    drop(old.acquire(ServerId(0)).await.unwrap());
    assert_eq!(old.server_load(0), (1, 1));
    assert_eq!(old.socket_budget_snapshot(0).async_idle, 1);
    // The same durable control covers a concurrently alive replacement client.
    let replacement = NntpPool::new(config());
    let permit = replacement
        .try_acquire_blocking_permit(ServerId(0))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), closed_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.socket_budget_snapshot(0).physical, 1);
    assert_eq!(old.socket_budget_snapshot(0).async_idle, 0);
    assert!(old.try_acquire_blocking_permit(ServerId(0)).is_err());
    let server_config = config().servers.remove(0).server;
    let lane = tokio::task::spawn_blocking(move || {
        weaver_nntp::blocking::BlockingBodyLane::connect(
            ServerId(0),
            StableServerId(7),
            None,
            &server_config,
            &[],
            0,
            &[],
            Duration::from_secs(2),
            permit,
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(old.socket_budget_snapshot(0).physical, 1);
    let (recall_tx, recall_rx) = std::sync::mpsc::channel();
    let socket_id = lane.socket_id();
    lane.mark_idle(Arc::new(move |id| {
        recall_tx.send(id).unwrap();
    }));
    let owner = std::thread::spawn(move || {
        assert_eq!(
            recall_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            socket_id
        );
        drop(lane);
    });
    // Reverse backend handoff must wake on both the owned dispatch permit
    // and the physical-slot acknowledgement, without a lost wakeup.
    let async_again =
        tokio::time::timeout(Duration::from_secs(2), replacement.acquire(ServerId(0)))
            .await
            .unwrap()
            .unwrap();
    owner.join().unwrap();
    assert_eq!(old.socket_budget_snapshot(0).physical, 1);
    tokio::time::timeout(Duration::from_secs(2), closed_rx.recv())
        .await
        .unwrap()
        .unwrap();
    drop(async_again);
    replacement.drain_all_idle().await;
    assert_eq!(old.socket_budget_snapshot(0).physical, 0);
    server.await.unwrap();
}
