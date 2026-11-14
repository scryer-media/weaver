// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_batch_tries_unsent_articles_before_leaving_the_provider() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::channel(4);
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
                writer.write_all(b"430 unavailable\r\n").await.unwrap();
            }
        }
    });
    let config = NntpClientConfig::single(
        ServerConfig {
            host: address.ip().to_string(),
            port: address.port(),
            tls: false,
            ..Default::default()
        },
        2,
    );
    let client = NntpClient::from_pool(Arc::new(NntpPool::new(PoolConfig {
        servers: config.servers,
        health_config: crate::health::HealthConfig {
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..Default::default()
        },
        ..Default::default()
    })));
    let pool = client.pool();
    for _ in 0..10 {
        pool.health.lock().await.record_failure(0, false);
    }
    pool.recovery_gates[0].expire_now();
    let traces = client
        .fetch_bodies_decoded_with_groups_prefer_excluding_traced(
            &["<first@test>", "<second@test>", "<third@test>"],
            &[],
            crate::ServerSet::EMPTY,
        )
        .await;
    assert!(!pool.requires_recovery(0));
    for (trace, id) in traces.iter().zip(["first", "second", "third"]) {
        assert_eq!(seen_rx.try_recv().unwrap(), format!("BODY <{id}@test>"));
        assert_eq!(trace.attempts.len(), 1);
        assert!(matches!(
            trace.result,
            Err(crate::client::DecodedBodyError::Nntp(
                NntpError::NoSuchArticle { .. }
            ))
        ));
    }
    assert!(seen_rx.try_recv().is_err());
    pool.drain_all_idle().await;
    server.await.unwrap();
}

// A server that has been re-enabled but still owes a recovery probe refuses
// extra connections through the recovery gate, not through the permit
// semaphore. The two must answer differently: this one happens with every
// permit free and every socket idle, and calling it saturation sends the
// reader hunting a permit leak that does not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recovery_gate_refusal_is_not_capacity_saturation() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![ServerPoolConfig {
            server: ServerConfig {
                host: "127.0.0.1".into(),
                port: 1,
                tls: false,
                ..Default::default()
            },
            stable_id: StableServerId(11),
            max_connections: 4,
            ..Default::default()
        }],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });
    let pool = client.pool();
    for _ in 0..10 {
        pool.health.lock().await.record_failure(0, false);
    }
    assert!(pool.requires_recovery(0));

    // Before the backoff expires nothing is admitted at all.
    assert!(matches!(
        pool.try_acquire_blocking_permit_for_work(ServerId(0), true),
        Err(NntpError::ServerRecovering)
    ));

    pool.recovery_gates[0].expire_now();
    // A warm dial is not demanded work and must never spend the one probe.
    assert!(matches!(
        pool.try_acquire_blocking_permit_for_work(ServerId(0), false),
        Err(NntpError::ServerRecovering)
    ));

    let probe = pool
        .try_acquire_blocking_permit_for_work(ServerId(0), true)
        .expect("the re-enabled server admits its one probe");
    assert!(
        probe.health_lease.0.probing(),
        "the admitted connection is the recovery probe"
    );
    // Every permit but the probe's is still free: this refusal is the gate's.
    assert_eq!(pool.server_load(0).0, 3);
    assert!(matches!(
        pool.try_acquire_blocking_permit_for_work(ServerId(0), true),
        Err(NntpError::ServerRecovering)
    ));

    // The probe's success is what ends the quarantine, and ordinary work is
    // admitted again.
    assert!(probe.health_lease.0.complete_recovery());
    assert!(!pool.requires_recovery(0));
    pool.try_acquire_blocking_permit_for_work(ServerId(0), false)
        .expect("a recovered server takes warm dials again");
}

// A server quarantined because it refused the credentials waits like a
// server quarantined by transport failures until it has refused the login
// for its give-up time. From then on the refusal is a verdict for the work,
// not a wait: the acquire answers with the refusal, names the server, and
// does not hand the work back. The same acquire on a server quarantined by
// transport failures still waits on the recovery gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_credentials_quarantine_refuses_where_a_transport_quarantine_waits() {
    use crate::client::BlockingBodyLaneAcquireError;

    let client = || {
        NntpClient::new(NntpClientConfig {
            servers: vec![ServerPoolConfig {
                server: ServerConfig {
                    host: "127.0.0.1".into(),
                    port: 1,
                    tls: false,
                    ..Default::default()
                },
                stable_id: StableServerId(12),
                max_connections: 4,
                ..Default::default()
            }],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(1),
        })
    };

    let refused = client();
    refused.pool().health.lock().await.record_failure(0, true);
    // Within the give-up time the refusal waits, as an outage does.
    assert!(!refused.pool().refuses_credentials(0));
    assert!(matches!(
        refused
            .pool()
            .try_acquire_blocking_permit_for_work(ServerId(0), true),
        Err(NntpError::ServerRecovering)
    ));
    let Err(waiting) = refused.try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY) else {
        panic!("a server refusing credentials opened a lane");
    };
    assert!(
        waiting.server().is_none() && waiting.kind() != "credentials_refused",
        "{waiting:?}"
    );
    refused.pool().set_login_refusal_clock(
        0,
        std::time::Instant::now() + crate::health::LOGIN_REFUSAL_GIVE_UP,
    );
    assert!(refused.pool().refuses_credentials(0));
    assert!(matches!(
        refused
            .pool()
            .try_acquire_blocking_permit_for_work(ServerId(0), true),
        Err(NntpError::AuthenticationRejected)
    ));
    let Err(error) = refused.try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY) else {
        panic!("a server refusing credentials opened a lane");
    };
    assert!(
        matches!(
            error,
            BlockingBodyLaneAcquireError::CredentialsRefused {
                server: ServerId(0),
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(error.kind(), "credentials_refused");
    assert_eq!(error.server(), Some(ServerId(0)));
    assert!(!error.should_requeue_owned_work());
    assert!(
        error
            .nntp_error()
            .is_some_and(NntpError::is_credentials_refusal)
    );
    // Excluded for the article, the server is no answer at all.
    assert!(matches!(
        refused.try_acquire_blocking_body_lane(&[], crate::ServerSet::from([0])),
        Err(BlockingBodyLaneAcquireError::NoEligibleServer)
    ));
    // Once the disable runs out the one probe is due, and the refusal is a
    // wait like any other until the probe answers.
    refused.pool().recovery_gates[0].expire_now();
    assert!(!refused.pool().refuses_credentials(0));
    let probe = refused
        .pool()
        .try_acquire_blocking_permit_for_work(ServerId(0), true)
        .expect("the lapsed refusal admits its probe");
    assert!(matches!(
        refused
            .pool()
            .try_acquire_blocking_permit_for_work(ServerId(0), true),
        Err(NntpError::ServerRecovering)
    ));
    drop(probe);

    let outage = client();
    for _ in 0..10 {
        outage.pool().health.lock().await.record_failure(0, false);
    }
    assert!(outage.pool().requires_recovery(0));
    assert!(!outage.pool().refuses_credentials(0));
    assert!(matches!(
        outage
            .pool()
            .try_acquire_blocking_permit_for_work(ServerId(0), true),
        Err(NntpError::ServerRecovering)
    ));
    let Err(error) = outage.try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY) else {
        panic!("an outage-quarantined server opened a lane");
    };
    assert!(
        error.server().is_none() && error.kind() != "credentials_refused",
        "{error:?}"
    );
}

// A backfill server refusing the login holds nothing back: its refusal is the
// answer at once, with no give-up time, while a fill server refusing the
// same way is waited for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backfill_server_refusing_the_login_is_answered_at_once() {
    let server = |port, backfill| ServerPoolConfig {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: false,
            ..Default::default()
        },
        stable_id: StableServerId(u32::from(port)),
        max_connections: 2,
        backfill,
        ..Default::default()
    };
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![server(1, false), server(2, true)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });
    let pool = client.pool();
    {
        let mut health = pool.health.lock().await;
        health.record_failure(0, true);
        health.record_failure(1, true);
    }
    assert!(
        !pool.refuses_credentials(0),
        "the fill server is waited for"
    );
    assert!(pool.refuses_credentials(1), "the backfill server is not");
}

// A pool rebuilt over the same server (a speed change, a route edit, the
// active toggle of another server) carries its run of login refusals on with
// the same verdict instant: the hour is not restarted by rebuilding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebuilt_pool_keeps_the_login_refusal_run_and_its_verdict_instant() {
    let registry = ServerTransferRegistry::new();
    let control = registry.control(StableServerId(7));
    let client = || {
        NntpClient::new(NntpClientConfig {
            servers: vec![ServerPoolConfig {
                server: ServerConfig {
                    host: "127.0.0.1".into(),
                    port: 1,
                    tls: false,
                    ..Default::default()
                },
                stable_id: StableServerId(7),
                transfer_control: Some(Arc::clone(&control)),
                max_connections: 1,
                ..Default::default()
            }],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(1),
        })
    };
    let first = client();
    first.pool().health.lock().await.record_failure(0, true);
    let verdict_at = control
        .login_refusal_verdict_at()
        .expect("the refusal began a run");
    let rebuilt = client();
    assert_eq!(control.login_refusal_verdict_at(), Some(verdict_at));
    assert_eq!(
        rebuilt.pool().login_refusal_waits(),
        crate::ServerSet::from_iter([0]),
        "the rebuilt pool waits on the server"
    );
    rebuilt
        .pool()
        .set_login_refusal_clock(0, verdict_at - Duration::from_secs(1));
    assert!(
        !rebuilt.pool().login_refusal_waits().is_empty(),
        "up to the same verdict instant"
    );
    rebuilt.pool().set_login_refusal_clock(0, verdict_at);
    assert!(rebuilt.pool().refuses_credentials(0));
    assert!(rebuilt.pool().login_refusal_waits().is_empty());
}

// The pool says when a fill server's run of login refusals becomes the
// verdict, finds it untold at that instant and marks it told once; a
// backfill server, answered at once, has no such instant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pool_tells_a_fill_servers_verdict_once_at_its_instant() {
    let server = |port, backfill| ServerPoolConfig {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: false,
            ..Default::default()
        },
        stable_id: StableServerId(u32::from(port)),
        max_connections: 2,
        backfill,
        ..Default::default()
    };
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![server(1, false), server(2, true)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });
    let pool = client.pool();
    assert_eq!(pool.next_login_refusal_verdict(), None);
    let start = Instant::now();
    pool.set_login_refusal_clock(0, start);
    pool.set_login_refusal_clock(1, start);
    {
        let mut health = pool.health.lock().await;
        health.record_failure(0, true);
        health.record_failure(1, true);
    }
    let verdict_at = start + crate::health::LOGIN_REFUSAL_GIVE_UP;
    assert_eq!(pool.next_login_refusal_verdict(), Some(verdict_at));
    assert!(
        pool.login_refusal_verdicts_untold().is_none(),
        "the run still waits"
    );
    let mut heard = pool.health_changed.subscribe();
    heard.mark_unchanged();
    pool.set_login_refusal_clock(0, verdict_at);
    pool.set_login_refusal_clock(1, verdict_at);
    let untold = pool
        .login_refusal_verdicts_untold()
        .expect("the fill server's verdict");
    assert_eq!(untold.0.len(), 1, "the backfill server has none");
    assert!(!heard.has_changed().unwrap());
    assert!(pool.mark_login_refusal_verdicts_told(&untold));
    assert!(
        heard.has_changed().unwrap(),
        "a watcher of the pool's health hears the verdict"
    );
    assert!(pool.refuses_credentials(0));
    assert!(!pool.mark_login_refusal_verdicts_told(&untold), "told once");
    assert!(pool.login_refusal_verdicts_untold().is_none());
    assert_eq!(pool.next_login_refusal_verdict(), None);
}

// A server's password is edited while the pool built from the old one is
// still in use and its server quarantined for the refusal. Building the
// pool from the new password changes nothing: a dispatch on the old pool
// between then and the new pool coming into use is not handed the probe.
// The new pool is handed it at once when it is put in use.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_built_from_an_edited_login_identity_probes_once_it_is_in_use() {
    let registry = ServerTransferRegistry::new();
    let control = registry.control(StableServerId(7));
    let client = |password: &str| {
        NntpClient::new(NntpClientConfig {
            servers: vec![ServerPoolConfig {
                server: ServerConfig {
                    host: "127.0.0.1".into(),
                    port: 1,
                    tls: false,
                    username: Some("user".into()),
                    password: Some(password.into()),
                    ..Default::default()
                },
                stable_id: StableServerId(7),
                transfer_control: Some(Arc::clone(&control)),
                max_connections: 1,
                ..Default::default()
            }],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(1),
        })
    };
    let old = client("old");
    old.pool().health.lock().await.record_failure(0, true);
    let verdict_at = control.login_refusal_verdict_at();
    assert!(verdict_at.is_some());
    assert!(matches!(
        old.pool().connection_hold(0, true),
        Some(crate::recovery::ConnectionHold::Until(_))
    ));

    let new = client("new");
    assert_eq!(
        control.login_refusal_verdict_at(),
        verdict_at,
        "building the pool ends nothing: the rebuild may yet fail"
    );
    assert!(
        matches!(
            old.pool().connection_hold(0, true),
            Some(crate::recovery::ConnectionHold::Until(_))
        ),
        "the old pool is not handed the probe"
    );
    assert!(
        old.pool().recovery_gates[0].admit(true).is_none(),
        "a dispatch on the old pool opens no connection"
    );

    new.pool().activated();
    assert_eq!(control.login_refusal_verdict_at(), None);
    assert_eq!(
        new.pool().connection_hold(0, true),
        Some(crate::recovery::ConnectionHold::Probe)
    );
    assert!(new.pool().login_refusal_waits().is_empty());
}

// A server refusing the login is switched from fill to backfill: the pool
// put in use with it in its new role ends the run and asks it again at
// once, where a pool put in use with it in the same role keeps the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_put_in_use_with_the_server_switched_to_backfill_ends_its_refusal_run() {
    let registry = ServerTransferRegistry::new();
    let control = registry.control(StableServerId(7));
    let server = |port, backfill, control: Option<&Arc<_>>| ServerPoolConfig {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: false,
            ..Default::default()
        },
        stable_id: StableServerId(u32::from(port)),
        transfer_control: control.map(Arc::clone),
        max_connections: 1,
        backfill,
        ..Default::default()
    };
    let client = |backfill| {
        NntpClient::new(NntpClientConfig {
            servers: vec![server(7, backfill, Some(&control)), server(8, false, None)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(1),
        })
    };
    let fill = client(false);
    fill.pool().health.lock().await.record_failure(0, true);
    let verdict_at = control.login_refusal_verdict_at();
    assert!(verdict_at.is_some());
    let same_role = client(false);
    same_role.pool().activated();
    assert_eq!(control.login_refusal_verdict_at(), verdict_at);
    assert!(matches!(
        same_role.pool().connection_hold(0, true),
        Some(crate::recovery::ConnectionHold::Until(_))
    ));
    let backfill = client(true);
    assert_eq!(control.login_refusal_verdict_at(), verdict_at);
    backfill.pool().activated();
    assert_eq!(control.login_refusal_verdict_at(), None);
    assert!(backfill.pool().login_refusal_waits().is_empty());
    assert_eq!(
        backfill.pool().connection_hold(0, true),
        Some(crate::recovery::ConnectionHold::Probe)
    );
}

// A server configured as backfill is held as fill in effect while every
// active server is backfill. Switching the only fill server off, or on
// again, flips the role in effect without touching the server's own
// configuration: its run of login refusals goes on to the same verdict
// instant, so a schedule that switches other servers cannot keep the
// verdict from arriving. Whether its refusal counts at once follows the
// role in effect in the pool that is asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_change_of_the_role_in_effect_alone_keeps_the_refusal_run() {
    let registry = ServerTransferRegistry::new();
    let control = registry.control(StableServerId(7));
    let server = |port, backfill, control: Option<&Arc<_>>| ServerPoolConfig {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: false,
            ..Default::default()
        },
        stable_id: StableServerId(u32::from(port)),
        transfer_control: control.map(Arc::clone),
        max_connections: 1,
        backfill,
        ..Default::default()
    };
    let client = |with_fill_server: bool| {
        let mut servers = vec![server(7, true, Some(&control))];
        if with_fill_server {
            servers.push(server(8, false, None));
        }
        NntpClient::new(NntpClientConfig {
            servers,
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(1),
        })
    };
    let as_backfill = client(true);
    assert!(as_backfill.pool().is_backfill(0));
    as_backfill
        .pool()
        .health
        .lock()
        .await
        .record_failure(0, true);
    let verdict_at = control.login_refusal_verdict_at();
    assert!(verdict_at.is_some());
    assert!(
        as_backfill.pool().refuses_credentials(0),
        "a backfill server's refusal counts at once"
    );

    let as_fill = client(false);
    as_fill.pool().activated();
    assert!(!as_fill.pool().is_backfill(0), "fill in effect");
    assert_eq!(control.login_refusal_verdict_at(), verdict_at);
    assert!(
        !as_fill.pool().refuses_credentials(0),
        "held as fill, its refusal is waited for"
    );
    assert_eq!(
        as_fill.pool().login_refusal_waits(),
        crate::ServerSet::from_iter([0])
    );

    let as_backfill_again = client(true);
    as_backfill_again.pool().activated();
    assert_eq!(control.login_refusal_verdict_at(), verdict_at);
    assert!(as_backfill_again.pool().refuses_credentials(0));
}

// The label the refusal travels under is the whole point of the variant.
#[test]
fn a_recovery_refusal_reports_its_own_kind() {
    use crate::client::BlockingBodyLaneAcquireError;

    let recovering = BlockingBodyLaneAcquireError::ServerRecovering;
    assert_eq!(recovering.kind(), "server_recovering");
    assert_ne!(
        recovering.kind(),
        BlockingBodyLaneAcquireError::LocalCapacity.kind()
    );
    assert!(
        recovering.should_requeue_owned_work(),
        "the probe clears on its own, so the work waits on the owned lanes"
    );
    assert!(recovering.nntp_error().is_none());
}

// The probe proves the server is up the moment it answers the demanded BODY
// with its 2xx status line. The quarantine lifts while the body is still
// streaming, however long the rest of the transfer then takes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_probe_clears_the_quarantine_on_its_answer_before_the_body_ends() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (finish, mut finish_rx) = tokio::sync::mpsc::channel::<()>(1);
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
                writer
                    .write_all(b"222 1 <probe@test>\r\n=ybegin line=128 size=2 name=x\r\nk\r\n")
                    .await
                    .unwrap();
                writer.flush().await.unwrap();
                finish_rx.recv().await.unwrap();
                writer
                    .write_all(b"k\r\n=yend size=2\r\n.\r\n")
                    .await
                    .unwrap();
            }
        }
    });
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![ServerPoolConfig {
            server: ServerConfig {
                host: address.ip().to_string(),
                port: address.port(),
                tls: false,
                ..Default::default()
            },
            stable_id: StableServerId(8),
            max_connections: 2,
            ..Default::default()
        }],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        // No runner waits this long: the body's hold ends only when the
        // test releases it. `Duration::MAX` itself overflows the lane's
        // acquire deadline.
        soft_timeout: Duration::from_secs(365 * 24 * 60 * 60),
    });
    let pool = client.pool();
    for _ in 0..10 {
        pool.health.lock().await.record_failure(0, false);
    }
    assert!(pool.requires_recovery(0));
    pool.recovery_gates[0].expire_now();
    let mut changes = pool.recovery_gates[0].subscribe();
    let mut lane = client.acquire_body_lane(ServerId(0), &[]).await.unwrap();
    assert!(pool.requires_recovery(0), "handshake is insufficient");
    let fetch = tokio::spawn(async move {
        let mut decoded = None;
        lane.fetch_decoded_pipeline(&["<probe@test>"], 1, |_, trace, _| {
            decoded = Some(trace.result.is_ok());
            async {}
        })
        .await;
        (lane, decoded)
    });

    while pool.requires_recovery(0) {
        changes.changed().await.unwrap();
    }
    assert!(
        !fetch.is_finished(),
        "the quarantine lifted on the answer, before the body ended"
    );
    assert!(
        pool.try_acquire_blocking_permit_for_work(ServerId(0), false)
            .is_ok(),
        "a lifted quarantine admits ordinary work again"
    );

    finish.send(()).await.unwrap();
    let (lane, decoded) = fetch.await.unwrap();
    assert_eq!(decoded, Some(true));
    assert!(!pool.requires_recovery(0));
    assert_eq!(
        *pool.health.lock().await.server(0).state(),
        crate::health::ServerState::Healthy
    );
    drop(lane);
    pool.drain_all_idle().await;
    server.await.unwrap();
}
