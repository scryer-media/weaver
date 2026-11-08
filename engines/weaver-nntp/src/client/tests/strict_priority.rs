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
        self.client.pool.drain_all_idle().await;
        self.servers.abort_all();
        while self.servers.join_next().await.is_some() {}
    }
}

#[tokio::test]
async fn selective_fallback_and_mixed_probes_reuse_only_eligible_sockets() {
    let fixture = Providers::new().await;
    // A preexisting paid socket must not turn into permission to use it.
    drop(fixture.client.pool.acquire(ServerId(2)).await.unwrap());
    for id in 0..124 {
        fixture
            .client
            .fetch_body(&format!("<primary-{id}@example.invalid>"))
            .await
            .unwrap();
    }
    fixture
        .client
        .fetch_body("<fallback@example.invalid>")
        .await
        .unwrap();
    fixture
        .client
        .fetch_body("<primary-again@example.invalid>")
        .await
        .unwrap();
    let result = fixture
        .client
        .confirm_exists_for_probe(&[
            "<primary-probe@example.invalid>",
            "<fallback-probe@example.invalid>",
        ])
        .await;
    assert_eq!(result.exists, vec![true, true]);
    assert!(!result.inconclusive);
    let commands = fixture.commands.lock().unwrap().clone();
    assert!(commands.iter().all(|(server, _)| *server != 2));
    assert!(
        commands
            .iter()
            .filter(|(server, _)| *server == 1)
            .all(|(_, command)| command.contains("fallback"))
    );
    assert_eq!(fixture.dials[0].load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.dials[1].load(Ordering::SeqCst),
        1,
        "one fallback does not dial sixteen connections"
    );
    fixture.close().await;
}

#[tokio::test]
async fn obsolete_work_cannot_dial_or_take_a_socket_after_waiting_for_capacity() {
    use std::future::Future;
    use std::sync::atomic::AtomicBool;
    use std::task::Poll;

    let fixture = Providers::new().await;
    let allowed = Arc::new(AtomicBool::new(false));
    let guard = Arc::clone(&allowed);
    let client = fixture
        .client
        .with_work_guard(move |_| guard.load(Ordering::SeqCst));
    let mut deadline = TokioInstant::now() + UNREACHED_TIMEOUT;
    assert!(matches!(
        client
            .acquire_before_deadline(ServerId(0), &mut deadline)
            .await,
        Err(NntpError::WorkObsolete)
    ));
    assert_eq!(fixture.dials[0].load(Ordering::SeqCst), 0);

    allowed.store(true, Ordering::SeqCst);
    let mut held = Vec::new();
    for _ in 0..16 {
        held.push(fixture.client.pool.acquire(ServerId(0)).await.unwrap());
    }
    // Model an owned idle lane whose recall is queued until we release it.
    held.last().unwrap().socket_slot.as_ref().unwrap().idle(
        crate::socket_budget::SocketPhase::OwnedIdle,
        Arc::new(|_| {}),
    );
    let mut waiting = Box::pin(client.acquire_before_deadline(ServerId(0), &mut deadline));
    std::future::poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    allowed.store(false, Ordering::SeqCst);
    drop(held.pop());
    assert!(matches!(waiting.await, Err(NntpError::WorkObsolete)));
    assert_eq!(fixture.dials[0].load(Ordering::SeqCst), 16);
    assert!(fixture.commands.lock().unwrap().is_empty());
    drop(held);
    fixture.close().await;
}

// A guard that turns the work away is neither a wait nor a server fault:
// the article leaves no attempt behind and no server's health moves.
#[tokio::test]
async fn obsolete_work_leaves_no_attempt_and_no_health_mark() {
    let fixture = Providers::new().await;
    let client = fixture.client.with_work_guard(|_| false);
    let trace = client
        .fetch_body_with_groups_prefer_excluding_traced(
            "<obsolete@example.invalid>",
            &["alt.binaries.test".to_string()],
            &[],
        )
        .await;
    assert!(
        matches!(trace.result, Err(NntpError::WorkObsolete)),
        "{:?}",
        trace.result
    );
    assert!(trace.attempts.is_empty(), "{:?}", trace.attempts);
    {
        let health = fixture.client.pool.health().lock().await;
        for idx in 0..3 {
            assert_eq!(health.server(idx).failure_count, 0);
            assert!(health.server(idx).is_available());
        }
    }
    assert!(
        fixture
            .dials
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 0)
    );
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

#[tokio::test]
async fn ordinary_outages_do_not_unlock_a_cached_backfill_socket() {
    let fixture = Providers::new().await;
    fixture
        .client
        .pool
        .health()
        .lock()
        .await
        .set_reenable_clock(Instant::now());
    drop(fixture.client.pool.acquire(ServerId(2)).await.unwrap());
    fixture
        .client
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(0, crate::health::CooldownReason::Transport);
    fixture
        .client
        .fetch_body("<ordinary-outage@example.invalid>")
        .await
        .unwrap();
    assert_eq!(fixture.dials[0].load(Ordering::SeqCst), 0);
    assert_eq!(fixture.dials[1].load(Ordering::SeqCst), 1);
    fixture
        .client
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(1, crate::health::CooldownReason::Transport);
    assert!(
        fixture
            .client
            .fetch_body("<protected@example.invalid>")
            .await
            .is_err()
    );
    let result = fixture
        .client
        .confirm_exists_for_probe(&["<protected@example.invalid>"])
        .await;
    assert!(result.inconclusive);
    assert!(
        fixture
            .commands
            .lock()
            .unwrap()
            .iter()
            .all(|(server, _)| *server != 2)
    );
    fixture.close().await;
}

#[tokio::test]
async fn queued_probe_guard_rechecks_recovery_before_using_a_cached_fallback() {
    let fixture = Providers::new().await;
    let now = Instant::now();
    let pool = Arc::clone(fixture.client.pool());
    {
        let mut health = pool.health().lock().await;
        health.set_reenable_clock(now);
        health.record_cooldown(0, crate::health::CooldownReason::Transport);
    }
    let result = fixture
        .client
        .confirm_exists_for_probe_using(
            &["<queued-probe@example.invalid>"],
            &[],
            |server, batch, guard| {
                let pool = Arc::clone(&pool);
                async move {
                    assert_eq!(server, 1);
                    assert!(guard());
                    pool.health()
                        .lock()
                        .await
                        .set_reenable_clock(now + Duration::from_secs(3600));
                    assert!(
                        !guard(),
                        "a recovered primary invalidates unissued fallback work"
                    );
                    Some(ProbeBatchResult {
                        exists: vec![false; batch.len()],
                        inconclusive: true,
                    })
                }
            },
        )
        .await
        .unwrap();
    assert!(result.inconclusive);
    assert!(fixture.commands.lock().unwrap().is_empty());
    assert!(
        fixture
            .dials
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 0)
    );
    fixture.close().await;
}
