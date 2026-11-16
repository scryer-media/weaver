// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Loopback regressions for mixed backend ownership. No provider is contacted.
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use weaver_nntp::route_dialer::{AddressPlanned, RouteDialer};
use weaver_nntp::transfer::{ServerTransferRegistry, StableServerId};
use weaver_nntp::{NntpPool, PoolConfig, ServerConfig, ServerId, ServerPoolConfig};

// The direct route out of the System egress to the loopback fixture.
#[expect(
    clippy::disallowed_methods,
    reason = "tests reach their loopback servers on the direct route"
)]
fn direct_route() -> Arc<RouteDialer> {
    let planned = Arc::new(AddressPlanned::new(
        "socket budget fixture".into(),
        Arc::new(weaver_tunnel::pipe::Egress::new(
            0,
            weaver_nntp::egress::SocketEgress::System,
            Duration::from_secs(30),
        )),
    ));
    Arc::new(RouteDialer {
        plan: Some(planned.plan.clone()),
        inner: planned,
        egress_controls: Arc::new(ServerTransferRegistry::new()),
        runtime: tokio::runtime::Handle::current(),
        server: 0,
    })
}

// The two pools share one durable control, as an old and a replacement client
// generation do, so they share one socket budget of one.
fn connect_lane(
    pool: &NntpPool,
    config: ServerConfig,
) -> tokio::task::JoinHandle<weaver_nntp::blocking::BlockingBodyLane> {
    let permit = pool.try_acquire_blocking_permit(ServerId(0)).unwrap();
    tokio::task::spawn_blocking(move || {
        weaver_nntp::blocking::BlockingBodyLane::connect(
            ServerId(0),
            StableServerId(7),
            None,
            &config,
            &[],
            Duration::from_secs(2),
            permit,
        )
        .unwrap()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_lane_socket_is_closed_before_its_slot_is_reassigned() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (closed_tx, mut closed_rx) = tokio::sync::mpsc::channel(2);
    let server = tokio::spawn(async move {
        for _ in 0..2 {
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
    let route = direct_route();
    let config = || PoolConfig {
        servers: vec![ServerPoolConfig {
            stable_id: StableServerId(7),
            transfer_control: Some(Arc::clone(&control)),
            max_connections: 1,
            ..ServerPoolConfig::new(ServerConfig {
                host: address.ip().to_string(),
                port: address.port(),
                tls: false,
                ..ServerConfig::new(Arc::clone(&route))
            })
        }],
        ..Default::default()
    };
    let server_config = || config().servers.remove(0).server;
    let old = NntpPool::new(config());
    let lane = connect_lane(&old, server_config()).await.unwrap();
    assert_eq!(old.socket_budget_snapshot(0).physical, 1);
    // The idle lane's owner closes its exact socket when recalled.
    let held = Arc::new(std::sync::Mutex::new(None));
    let socket_id = lane.socket_id();
    let owner = Arc::clone(&held);
    lane.mark_idle(Arc::new(move |id| {
        assert_eq!(id, socket_id);
        drop(owner.lock().unwrap().take());
    }));
    *held.lock().unwrap() = Some(lane);
    assert_eq!(old.socket_budget_snapshot(0).owned_idle, 1);

    // The replacement generation recalls the idle socket for its own dial,
    // and gets the slot only once that socket has closed.
    let replacement = NntpPool::new(config());
    let lane = connect_lane(&replacement, server_config()).await.unwrap();
    assert!(held.lock().unwrap().is_none());
    closed_rx.recv().await.unwrap();
    assert_eq!(old.socket_budget_snapshot(0).physical, 1);
    assert_eq!(old.socket_budget_snapshot(0).owned_idle, 0);
    assert!(old.try_acquire_blocking_permit(ServerId(0)).is_err());
    drop(lane);
    assert_eq!(old.socket_budget_snapshot(0).physical, 0);
    closed_rx.recv().await.unwrap();
    server.await.unwrap();
}
