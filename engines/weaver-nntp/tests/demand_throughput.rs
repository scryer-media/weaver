// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use weaver_nntp::client::{NntpClient, NntpClientConfig};
use weaver_nntp::route_dialer::{AddressPlanned, RouteDialer};
use weaver_nntp::transfer::ServerTransferRegistry;
use weaver_nntp::{ServerConfig, ServerPoolConfig};

#[expect(
    clippy::disallowed_methods,
    reason = "local throughput fixture uses loopback"
)]
fn route() -> Arc<RouteDialer> {
    let planned = Arc::new(AddressPlanned::new(
        "throughput fixture".into(),
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

// A measurement, not a timing assertion. Run explicitly on both revisions;
// elapsed time never determines whether this test passes. What it asserts is
// structural: in every phase the server holds a payload request on each of
// its connections at once. The fixture answers no payload until that many
// are in flight, so demand that does not spread across every connection
// never completes instead of passing slowly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual local cold/warm throughput comparison"]
async fn primary_only_cold_bootstrap_and_warm_reuse() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    const CONNECTIONS: usize = 8;
    let dials = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&dials);
    let in_flight = Arc::new(AtomicUsize::new(0));
    // The most payload requests in flight at once in the current phase.
    let (peak_tx, peak_rx) = tokio::sync::watch::channel(0usize);
    let peak_tx = Arc::new(peak_tx);
    let server_peak = Arc::clone(&peak_tx);
    let server_in_flight = Arc::clone(&in_flight);
    let server = tokio::spawn(async move {
        let mut sockets = tokio::task::JoinSet::new();
        let mut payload = b"222 1 <payload@example.invalid>\r\n".to_vec();
        for _ in 0..2048 {
            payload.extend_from_slice(&[b'x'; 128]);
            payload.extend_from_slice(b"\r\n");
        }
        payload.extend_from_slice(b".\r\n");
        let payload = Arc::new(payload);
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            count.fetch_add(1, Ordering::SeqCst);
            let payload = Arc::clone(&payload);
            let peak = Arc::clone(&server_peak);
            let mut reached = peak.subscribe();
            let in_flight = Arc::clone(&server_in_flight);
            sockets.spawn(async move {
                let (read, mut write) = socket.into_split();
                write.write_all(b"200 ready\r\n").await.unwrap();
                let mut lines = BufReader::new(read).lines();
                while let Some(command) = lines.next_line().await.unwrap() {
                    let response = if command == "CAPABILITIES" {
                        b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n".as_slice()
                    } else if command.contains("index") {
                        b"222 1 <index@example.invalid>\r\nmetadata\r\n.\r\n".as_slice()
                    } else {
                        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.send_if_modified(|peak| {
                            let raised = now > *peak;
                            *peak = (*peak).max(now);
                            raised
                        });
                        if reached.wait_for(|peak| *peak >= CONNECTIONS).await.is_err() {
                            break;
                        }
                        payload.as_slice()
                    };
                    let written = write.write_all(response).await;
                    if !command.contains("index") && command != "CAPABILITIES" {
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                    }
                    if written.is_err() {
                        break;
                    }
                }
            });
        }
    });
    let mut config = ServerPoolConfig::new(ServerConfig {
        host: "127.0.0.1".into(),
        port,
        tls: false,
        ..ServerConfig::new(route())
    });
    config.max_connections = CONNECTIONS;
    let client = Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![config],
        max_idle_age: Duration::from_mins(5),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(3600),
    }));
    for phase in ["cold", "warm", "warm-repeat"] {
        peak_tx.send_replace(0);
        let before = dials.load(Ordering::SeqCst);
        let start = Instant::now();
        client.fetch_body("<index@example.invalid>").await.unwrap();
        let mut workers = tokio::task::JoinSet::new();
        for worker in 0..CONNECTIONS {
            let client = Arc::clone(&client);
            workers.spawn(async move {
                let mut bytes = 0;
                for article in 0..128 {
                    bytes += client
                        .fetch_body(&format!("<payload-{worker}-{article}@example.invalid>"))
                        .await
                        .unwrap()
                        .len();
                }
                bytes
            });
        }
        let mut bytes = 0;
        while let Some(result) = workers.join_next().await {
            bytes += result.unwrap();
        }
        let elapsed = start.elapsed().as_secs_f64();
        eprintln!(
            "{phase}: bytes={bytes} seconds={elapsed:.6} MiB/s={:.2} new_dials={}",
            bytes as f64 / 1_048_576.0 / elapsed,
            dials.load(Ordering::SeqCst) - before
        );
        assert_eq!(bytes, 1024 * 2048 * 130);
        assert_eq!(
            *peak_rx.borrow(),
            CONNECTIONS,
            "{phase}: every connection carried a payload request at once"
        );
    }
    client.pool().drain_all_idle().await;
    server.abort();
    let _ = server.await;
}
