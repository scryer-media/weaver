// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncBufReadExt, BufReader};

struct Providers {
    client: NntpClient,
    dials: Arc<Vec<AtomicUsize>>,
    commands: Arc<std::sync::Mutex<Vec<(usize, String)>>>,
    servers: tokio::task::JoinSet<()>,
}

impl Providers {
    async fn new() -> Self {
        let dials = Arc::new((0..3).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut servers = tokio::task::JoinSet::new();
        let mut configs = Vec::new();
        for idx in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut config = scripted_server(listener.local_addr().unwrap().port(), idx);
            config.max_connections = 16;
            config.backfill = idx == 2;
            configs.push(config);
            let dials = Arc::clone(&dials);
            let commands = Arc::clone(&commands);
            servers.spawn(async move {
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    let (socket, _) = listener.accept().await.unwrap();
                    dials[idx].fetch_add(1, Ordering::SeqCst);
                    let commands = Arc::clone(&commands);
                    connections.spawn(async move {
                        let (read, mut write) = socket.into_split();
                        write.write_all(b"200 ready\r\n").await.unwrap();
                        let mut lines = BufReader::new(read).lines();
                        while let Some(command) = lines.next_line().await.unwrap() {
                            if command == "CAPABILITIES" {
                                write.write_all(b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n").await.unwrap();
                                continue;
                            }
                            commands.lock().unwrap().push((idx, command.clone()));
                            let missing = idx == 0 && command.contains("fallback");
                            let response: &[u8] = if command.starts_with("GROUP ") {
                                b"211 1 1 1 alt.binaries.test\r\n"
                            } else if missing {
                                b"430 absent\r\n"
                            } else if command.starts_with("STAT ") {
                                b"223 1 <present@example.invalid>\r\n"
                            } else if command.starts_with("HEAD ") {
                                b"221 1 <present@example.invalid>\r\nSubject: fixture\r\n.\r\n"
                            } else {
                                b"222 1 <present@example.invalid>\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n"
                            };
                            if write.write_all(response).await.is_err() { break; }
                        }
                    });
                }
            });
        }
        Self {
            client: NntpClient::new(NntpClientConfig {
                servers: configs,
                max_idle_age: Duration::from_mins(5),
                max_retries_per_server: 0,
                soft_timeout: UNREACHED_TIMEOUT,
            }),
            dials,
            commands,
            servers,
        }
    }

    async fn close(mut self) {
        self.client.pool.drain_all_idle();
        self.servers.abort_all();
        while self.servers.join_next().await.is_some() {}
    }
}

// Commands a server read, leaving out the goodbye a closing lane sends.
fn work_commands(fixture: &Providers) -> Vec<(usize, String)> {
    fixture
        .commands
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, command)| command != "QUIT")
        .cloned()
        .collect()
}

// On the connection downloads use, the lower group serves only the article
// the primary does not have: one lane is opened there for it, the primary's
// own lane carries every other article, and the backfill is never asked.
#[tokio::test]
async fn blocking_fallback_uses_the_lower_group_only_for_the_missing_article() {
    let fixture = Providers::new().await;
    let client = fixture.client.clone();
    tokio::task::spawn_blocking(move || {
        let mut primary = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .expect("the primary opens a lane");
        assert_eq!(primary.server_id(), ServerId(0));
        for id in 0..8 {
            let trace =
                primary.fetch_decoded_sequential(&format!("<primary-{id}@example.invalid>"));
            client.record_blocking_attempts(&trace.attempts);
            trace.result.expect("the primary has its own articles");
        }
        let missing = primary.fetch_decoded_sequential("<fallback@example.invalid>");
        client.record_blocking_attempts(&missing.attempts);
        assert_eq!(missing.attempts.len(), 1);
        assert_eq!(missing.attempts[0].outcome, FetchAttemptOutcome::NotFound);

        let mut lower = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::from([0]))
            .expect("the lower group opens a lane for the missing article");
        assert_eq!(lower.server_id(), ServerId(1));
        let found = lower.fetch_decoded_sequential("<fallback@example.invalid>");
        client.record_blocking_attempts(&found.attempts);
        found.result.expect("the lower group has the article");
        drop(lower);

        let again = client
            .blocking_body_server_order(crate::ServerSet::EMPTY)
            .expect("the health state is uncontended in this test");
        assert_eq!(again.first(), Some(&ServerId(0)));
        let trace = primary.fetch_decoded_sequential("<primary-again@example.invalid>");
        client.record_blocking_attempts(&trace.attempts);
        trace
            .result
            .expect("the next article goes back to the primary");
        drop(primary);
    })
    .await
    .unwrap();

    let commands = work_commands(&fixture);
    assert!(
        commands.iter().all(|(server, _)| *server != 2),
        "{commands:?}"
    );
    let lower: Vec<_> = commands.iter().filter(|(server, _)| *server == 1).collect();
    assert_eq!(lower.len(), 1, "{commands:?}");
    assert!(lower[0].1.contains("fallback"), "{commands:?}");
    assert!(
        commands
            .iter()
            .any(|(server, command)| *server == 0 && command.contains("primary-again")),
        "{commands:?}"
    );
    assert_eq!(fixture.dials[0].load(Ordering::SeqCst), 1);
    assert_eq!(fixture.dials[1].load(Ordering::SeqCst), 1);
    assert_eq!(fixture.dials[2].load(Ordering::SeqCst), 0);
    fixture.close().await;
}

#[tokio::test]
async fn fresh_connect_failures_preserve_established_connection_health() {
    let client = Arc::new(NntpClient::new(NntpClientConfig::single(
        ServerConfig {
            host: "127.0.0.1".into(),
            port: 1,
            tls: false,
            ..Default::default()
        },
        4,
    )));
    let established = client
        .pool
        .try_acquire_blocking_permit(ServerId(0))
        .unwrap();
    established.socket_slot.active();
    let pending: Vec<_> = (0..3)
        .map(|_| {
            client
                .pool
                .try_acquire_blocking_permit(ServerId(0))
                .unwrap()
        })
        .collect();
    tokio::task::spawn_blocking(move || {
        for permit in pending {
            let ticket = Arc::clone(&permit.health_lease);
            drop(permit);
            client.record_blocking_connect_failure(
                0,
                crate::pool::FreshConnectAdmission::Open,
                &NntpError::Tls(tokio_rustls::rustls::Error::General(
                    "fixture TLS failure".into(),
                )),
                &ticket.0,
            );
        }
        assert!(established.health_lease.0.current());
        assert!(client.pool.health().blocking_lock().is_available(0));
        assert_eq!(
            client.pool.health().blocking_lock().server(0).failure_count,
            0
        );
        assert_eq!(client.pool.socket_budget_snapshot(0).physical, 1);
    })
    .await
    .unwrap();
}

// An idle cached socket proves nothing about the server now: when every
// new dial is refused, health still sees each failure and quarantines the
// server at its threshold.
#[tokio::test]
async fn refused_dials_reach_health_past_an_idle_cached_socket() {
    let client = Arc::new(NntpClient::new(NntpClientConfig::single(
        ServerConfig {
            host: "127.0.0.1".into(),
            port: 1,
            tls: false,
            ..Default::default()
        },
        16,
    )));
    let threshold = crate::health::HealthConfig::default().disable_threshold as usize;
    let cached = client
        .pool
        .try_acquire_blocking_permit(ServerId(0))
        .unwrap();
    cached.socket_slot.idle(
        crate::socket_budget::SocketPhase::OwnedIdle,
        Arc::new(|_| {}),
    );
    let pending: Vec<_> = (0..threshold)
        .map(|_| {
            client
                .pool
                .try_acquire_blocking_permit(ServerId(0))
                .unwrap()
        })
        .collect();
    tokio::task::spawn_blocking(move || {
        for permit in pending {
            let ticket = Arc::clone(&permit.health_lease);
            drop(permit);
            client.record_blocking_connect_failure(
                0,
                crate::pool::FreshConnectAdmission::Open,
                &NntpError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionRefused)),
                &ticket.0,
            );
        }
        let mut health = client.pool.health().blocking_lock();
        assert!(
            matches!(
                health.server(0).state(),
                crate::health::ServerState::Disabled {
                    reason: crate::health::DisableReason::ConsecutiveFailures,
                    ..
                }
            ),
            "{:?}",
            health.server(0).state()
        );
        assert!(!health.is_available(0));
        drop(health);
        assert!(!client.pool.is_over_limit(ServerId(0)));
        assert!(!client.pool.provider_policy().servers[0].available);
        drop(cached);
    })
    .await
    .unwrap();
}

// Fill servers that are down are an outage, not a miss: the connection
// downloads use falls to the next fill group while one is up, and with every
// fill server cooling it opens no lane at all rather than the backfill.
#[tokio::test]
async fn blocking_outages_do_not_unlock_the_backfill() {
    let fixture = Providers::new().await;
    fixture
        .client
        .pool
        .health()
        .lock()
        .await
        .set_reenable_clock(Instant::now());
    fixture
        .client
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(0, crate::health::CooldownReason::Transport);
    let client = fixture.client.clone();
    let client = tokio::task::spawn_blocking(move || {
        let mut lane = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .expect("the next fill group opens a lane");
        assert_eq!(lane.server_id(), ServerId(1));
        let trace = lane.fetch_decoded_sequential("<ordinary-outage@example.invalid>");
        client.record_blocking_attempts(&trace.attempts);
        trace
            .result
            .expect("the next fill group serves the article");
        drop(lane);
        client
    })
    .await
    .unwrap();
    assert_eq!(fixture.dials[0].load(Ordering::SeqCst), 0);
    assert_eq!(fixture.dials[1].load(Ordering::SeqCst), 1);

    client
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(1, crate::health::CooldownReason::Transport);
    tokio::task::spawn_blocking(move || {
        let refused = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .err()
            .expect("an outage of every fill server must not open the backfill");
        assert!(
            matches!(refused, BlockingBodyLaneAcquireError::NoEligibleServer),
            "{refused:?}"
        );
        assert!(
            client
                .blocking_body_server_order(crate::ServerSet::EMPTY)
                .expect("the health state is uncontended in this test")
                .is_empty()
        );
    })
    .await
    .unwrap();
    assert_eq!(fixture.dials[2].load(Ordering::SeqCst), 0);
    assert!(
        work_commands(&fixture)
            .iter()
            .all(|(server, _)| *server != 2)
    );
    fixture.close().await;
}
