use super::*;

fn proxy() -> ProxyProfile {
    ProxyProfile {
        id: 1,
        name: "fixture".into(),
        kind: ProxyKind::Socks5,
        enabled: true,
        host: "proxy.invalid".into(),
        port: 1080,
        dns_servers: vec![],
        tunnel_addresses: vec![],
        peer_public_key: None,
        mtu: 1420,
        keepalive_seconds: None,
        timeout_seconds: 1,
        host_key_fingerprint: None,
        revision: 1,
        secrets: Default::default(),
    }
}

fn pool_policy() -> RoutingPolicy {
    RoutingPolicy {
        legs: vec![RouteLeg {
            egress_id: 0,
            weight: 100,
            path: LegPath::Ladder {
                rungs: vec![Rung::Pool { id: 7 }],
                direct_fallback: false,
            },
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn failed_reload_keeps_routes_pools_and_configuration_together() {
    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut initial = runtime.configuration.read().unwrap().clone();
    for id in 1..=3 {
        initial.consumers.insert(Consumer::Server(id).key());
    }
    initial.profiles.insert(1, proxy());
    initial.pools.insert(
        7,
        ProxyPool {
            id: 7,
            name: "fixture".into(),
            kind: ProxyKind::Socks5,
            member_ids: vec![1],
            enabled: true,
        },
    );
    initial
        .policies
        .insert(Consumer::Server(1).key(), pool_policy());
    runtime.apply_configuration(initial.clone()).unwrap();
    let routes: Vec<_> = (1..=3)
        .map(|id| {
            runtime
                .route(Consumer::Server(id), 5, Duration::from_secs(1))
                .unwrap()
        })
        .collect();
    let stages: Vec<_> = routes
        .iter()
        .map(|route| route.legs.read().unwrap()[0].stage.clone())
        .collect();
    let pool = runtime.pools.lock().unwrap()[&(7, 0)].clone();
    let mut next = initial;
    next.pools.get_mut(&7).unwrap().enabled = false;
    next.consumers.remove(&Consumer::Server(2).key());
    next.policies.insert(
        Consumer::Server(3).key(),
        RoutingPolicy {
            proxy_ids: vec![999],
            allow_direct: false,
            ..Default::default()
        },
    );
    assert!(runtime.apply_configuration(next.clone()).is_err());
    assert_eq!(pool.snapshot().1.len(), 1);
    assert!(runtime.configuration.read().unwrap().pools[&7].enabled);
    for (index, route) in routes.iter().enumerate() {
        assert!(Arc::ptr_eq(
            &stages[index],
            &route.legs.read().unwrap()[0].stage
        ));
        assert!(Arc::ptr_eq(
            route,
            &runtime
                .route(
                    Consumer::Server(index as u32 + 1),
                    5,
                    Duration::from_secs(1)
                )
                .unwrap()
        ));
    }
    next.policies.remove(&Consumer::Server(3).key());
    runtime.apply_configuration(next).unwrap();
    assert!(pool.snapshot().1.is_empty());
    assert!(
        runtime
            .route(Consumer::Server(2), 5, Duration::from_secs(1))
            .is_err()
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn profile_revision_rebuilds_pool_leg_after_trust_failure() {
    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut config = runtime.configuration.read().unwrap().clone();
    config.consumers.insert(Consumer::Server(1).key());
    config.profiles.insert(1, proxy());
    config.pools.insert(
        7,
        ProxyPool {
            id: 7,
            name: "fixture".into(),
            kind: ProxyKind::Socks5,
            member_ids: vec![1],
            enabled: true,
        },
    );
    config
        .policies
        .insert(Consumer::Server(1).key(), pool_policy());
    runtime.apply_configuration(config.clone()).unwrap();
    let route = runtime
        .route(Consumer::Server(1), 5, Duration::from_secs(1))
        .unwrap();
    let old: Arc<dyn Dialer> = route.legs.read().unwrap()[0].stage.clone();
    route.weighted.report_external(
        0,
        &old,
        Some(&DialError::Fatal(weaver_tunnel::TunnelError::Engine(
            "trust rejected".into(),
        ))),
    );
    assert!(matches!(
        route.weighted.allocations()[0].health,
        LegHealthState::Blocked(_)
    ));
    config.profiles.get_mut(&1).unwrap().revision += 1;
    runtime.apply_configuration(config).unwrap();
    assert!(matches!(
        route.weighted.allocations()[0].health,
        LegHealthState::Up
    ));
    let new: Arc<dyn Dialer> = route.legs.read().unwrap()[0].stage.clone();
    assert!(!Arc::ptr_eq(&old, &new));
    runtime.shutdown().await;
}

#[tokio::test]
async fn unused_session_is_kept_until_live_streams_allow_retirement() {
    struct Busy(std::sync::Arc<std::sync::atomic::AtomicBool>);
    #[async_trait::async_trait]
    impl Dialer for Busy {
        async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
            unreachable!()
        }
        async fn retire_idle(&self) -> bool {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn budget(&self) -> Duration {
            Duration::ZERO
        }
        fn describe(&self) -> String {
            "busy fixture".into()
        }
    }
    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let idle = Arc::new(std::sync::atomic::AtomicBool::new(false));
    runtime
        .sessions
        .lock()
        .unwrap()
        .insert("fixture".into(), Arc::new(Busy(idle.clone())));
    runtime.retire_unused_sessions().await;
    assert!(runtime.sessions.lock().unwrap().contains_key("fixture"));
    idle.store(true, std::sync::atomic::Ordering::SeqCst);
    runtime.retire_unused_sessions().await;
    assert!(runtime.sessions.lock().unwrap().is_empty());
    runtime.shutdown().await;
}

#[tokio::test]
async fn probe_of_a_saved_profile_shares_the_live_session() {
    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut config = runtime.configuration.read().unwrap().clone();
    config.profiles.insert(1, proxy());
    runtime.apply_configuration(config.clone()).unwrap();
    let bottom = NetworkRuntime::bottom(&config, SYSTEM_EGRESS_ID, Duration::from_secs(1)).unwrap();
    let live = runtime
        .hop(&config, 1, bottom.clone(), bottom.clone(), &[])
        .unwrap();

    // The probe reuses the session that already holds the budget.
    let isolated = runtime.isolated_probe_runtime();
    let probed = isolated
        .hop(&config, 1, bottom.clone(), bottom.clone(), &[])
        .unwrap();
    assert!(Arc::ptr_eq(&live, &probed));
    isolated.shutdown().await;
    assert!(Arc::ptr_eq(
        runtime.sessions.lock().unwrap().values().next().unwrap(),
        &live
    ));

    // An edited profile is a different session and is built afresh.
    config.profiles.get_mut(&1).unwrap().revision += 1;
    let edited = isolated
        .hop(&config, 1, bottom.clone(), bottom, &[])
        .unwrap();
    assert!(!Arc::ptr_eq(&live, &edited));
    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_feed_leg_holds_its_health_until_its_last_rung_and_fallback_have_failed() {
    let db = Database::open_in_memory().unwrap();
    let runtime = NetworkRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let mut config = runtime.configuration.read().unwrap().clone();
    config.consumers.insert(Consumer::Rss(1).key());
    for (id, host) in [(1, "first.invalid"), (2, "second.invalid")] {
        let mut profile = proxy();
        profile.id = id;
        profile.name = format!("fixture-{id}");
        profile.host = host.into();
        config.profiles.insert(id, profile);
    }
    config.policies.insert(
        Consumer::Rss(1).key(),
        RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: 0,
                weight: 100,
                path: LegPath::Ladder {
                    rungs: vec![Rung::Proxy { id: 1 }, Rung::Proxy { id: 2 }],
                    direct_fallback: true,
                },
            }],
            ..Default::default()
        },
    );
    runtime.apply_configuration(config).unwrap();
    let legacy = ProxyRuntime::new(db, tokio::runtime::Handle::current())
        .unwrap()
        .draft_route(RoutingPolicy::default(), Duration::from_secs(1))
        .unwrap();
    let route = runtime
        .route(Consumer::Rss(1), 1, Duration::from_secs(1))
        .unwrap();
    let health = || route.weighted.allocations()[0].health.clone();
    let refused = |proxy| DialError::Hop {
        proxy,
        source: weaver_tunnel::TunnelError::Engine("refused".into()),
    };
    let unreachable = || DialError::Egress(std::io::Error::other("no route to host"));

    // Both proxy rungs fail; the direct fallback has not run yet.
    let attempts = runtime.feed_attempts(1, legacy.clone()).unwrap();
    assert_eq!(attempts.len(), 3, "two rungs and the direct fallback");
    attempts[0].report(Some(&refused(1)));
    attempts[1].report(Some(&refused(2)));
    assert_eq!(
        health(),
        LegHealthState::Up,
        "two failed rungs must not take the leg down before its fallback runs"
    );
    // The fallback fails too: that is the leg's one failure for this sync.
    attempts[2].report(Some(&unreachable()));
    assert_eq!(health(), LegHealthState::Up);

    // Once the rungs' own cooldowns lapse, the next sync fails the same way;
    // two whole failed syncs cool the leg.
    tokio::time::advance(Duration::from_secs(31)).await;
    let attempts = runtime.feed_attempts(1, legacy).unwrap();
    assert_eq!(attempts.len(), 3);
    attempts[0].report(Some(&refused(1)));
    attempts[1].report(Some(&refused(2)));
    assert_eq!(health(), LegHealthState::Up);
    attempts[2].report(Some(&unreachable()));
    assert!(
        matches!(health(), LegHealthState::Down(_)),
        "{:?}",
        health()
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn configured_legs_of_a_dormant_consumer_report_a_missing_egress() {
    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let consumer = Consumer::Server(3);
    let mut config = runtime.configuration.read().unwrap().clone();
    config.consumers.insert(consumer.key());
    config
        .consumer_labels
        .push((consumer, "news.invalid".into(), 3));
    config.policies.insert(
        consumer.key(),
        RoutingPolicy {
            legs: vec![
                RouteLeg {
                    egress_id: 0,
                    weight: 50,
                    path: LegPath::Direct,
                },
                RouteLeg {
                    egress_id: 99,
                    weight: 50,
                    path: LegPath::Direct,
                },
            ],
            ..Default::default()
        },
    );
    let warning =
        "Restored egress 99 is missing; this leg is Down until its egress is repaired.".to_string();
    config.warnings.insert((consumer.key(), 1), warning.clone());
    runtime.apply_configuration(config).unwrap();

    // The consumer never dialed, yet its configured legs and their egress
    // health are reported.
    let dormant = runtime.dormant_legs();
    let legs: Vec<_> = dormant
        .iter()
        .filter(|leg| leg.consumer == consumer)
        .collect();
    assert_eq!(legs.len(), 2);
    assert_eq!((legs[0].position, legs[0].definition.egress_id), (0, 0));
    assert_eq!(legs[0].health, LegHealthState::Up);
    assert_eq!((legs[1].position, legs[1].definition.egress_id), (1, 99));
    assert_eq!(legs[1].health, LegHealthState::Down(warning.clone()));

    // Once it dials, the live route takes over with the same reason.
    let route = runtime.route(consumer, 3, Duration::from_secs(1)).unwrap();
    assert!(
        runtime
            .dormant_legs()
            .iter()
            .all(|leg| leg.consumer != consumer)
    );
    assert_eq!(
        route.weighted.allocations()[1].health,
        LegHealthState::Down(warning)
    );
    runtime.shutdown().await;
}

fn metered_egress(limit_bytes: u64) -> EgressInterface {
    EgressInterface {
        id: 0,
        name: "Metered link".into(),
        binding: EgressBinding::SourceAddress {
            address: "127.0.0.1".parse().unwrap(),
        },
        enabled: true,
        max_download_speed: 0,
        download_quota: crate::servers::ServerDownloadQuotaConfig {
            enabled: true,
            limit_bytes,
            period: crate::servers::ServerDownloadQuotaPeriod::OneTime,
            ..Default::default()
        },
    }
}

// Spend `bytes` of an egress's allowance the way a finished BODY does.
fn spend(runtime: &NetworkRuntime, egress_id: u32, bytes: usize) {
    let mut permit = runtime
        .egress_controls
        .control(weaver_nntp::transfer::StableServerId(egress_id))
        .try_reserve(bytes as u64)
        .unwrap();
    permit.record_blocking(bytes);
    permit.finish();
}

#[tokio::test]
async fn a_used_up_egress_quota_takes_its_leg_down_and_its_share_moves_to_the_other_leg() {
    let db = Database::open_in_memory().unwrap();
    let metered = db
        .create_egress_interface(&metered_egress(1_000_000))
        .unwrap();
    let policy = Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(db.clone(), &[])
            .unwrap(),
    );
    let runtime = NetworkRuntime::with_quota_policy(
        db,
        tokio::runtime::Handle::current(),
        Some(policy.clone()),
    )
    .unwrap();
    let consumer = Consumer::Server(3);
    let mut config = runtime.configuration.read().unwrap().clone();
    config.consumers.insert(consumer.key());
    config.policies.insert(
        consumer.key(),
        RoutingPolicy {
            legs: vec![
                RouteLeg {
                    egress_id: SYSTEM_EGRESS_ID,
                    weight: 50,
                    path: LegPath::Direct,
                },
                RouteLeg {
                    egress_id: metered.id,
                    weight: 50,
                    path: LegPath::Direct,
                },
            ],
            ..Default::default()
        },
    );
    runtime.apply_configuration(config).unwrap();
    let route = runtime.route(consumer, 4, Duration::from_secs(1)).unwrap();
    assert_ne!(
        route.weighted.allocations()[1].health,
        LegHealthState::Down(QUOTA_REACHED.into())
    );

    // The allowance is spent and the next body is turned away.
    spend(&runtime, metered.id, 900_000);
    assert!(
        runtime
            .egress_controls
            .control(weaver_nntp::transfer::StableServerId(metered.id))
            .try_reserve(200_000)
            .is_err()
    );
    assert_eq!(
        runtime.quota_blocked_egresses(),
        HashSet::from([metered.id])
    );
    runtime.refresh_health();
    let allocations = route.weighted.allocations();
    assert_eq!(
        allocations[1].health,
        LegHealthState::Down(QUOTA_REACHED.into())
    );
    assert_eq!(allocations[1].target, 0);
    assert_eq!(allocations[0].health, LegHealthState::Up);
    assert_eq!(allocations[0].target, 4);
    assert!(policy.egress_snapshot(metered.id).unwrap().blocked);

    // A larger allowance gives the leg back.
    let mut raised = metered.clone();
    raised.download_quota.limit_bytes = 10_000_000;
    policy.reconfigure_egresses(&[raised]).unwrap();
    runtime.refresh_health();
    assert!(runtime.quota_blocked_egresses().is_empty());
    assert_ne!(
        route.weighted.allocations()[1].health,
        LegHealthState::Down(QUOTA_REACHED.into())
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn egress_quota_usage_survives_a_network_recompile_and_a_rebuilt_runtime() {
    let db = Database::open_in_memory().unwrap();
    let metered = db
        .create_egress_interface(&metered_egress(5_000_000))
        .unwrap();
    let policy = Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(db.clone(), &[])
            .unwrap(),
    );
    let runtime = NetworkRuntime::with_quota_policy(
        db.clone(),
        tokio::runtime::Handle::current(),
        Some(policy.clone()),
    )
    .unwrap();
    spend(&runtime, metered.id, 1_234_567);
    assert_eq!(
        policy.egress_snapshot(metered.id).unwrap().used_bytes,
        1_234_567
    );

    // An edit to the egress recompiles the network mid-window.
    let mut edited = metered.clone();
    edited.name = "Metered link, renamed".into();
    edited.max_download_speed = 4_096;
    db.update_egress_interface(&edited).unwrap();
    runtime.reload().await.unwrap();
    let after_reload = policy.egress_snapshot(metered.id).unwrap();
    assert_eq!(after_reload.used_bytes, 1_234_567);
    assert_eq!(after_reload.remaining_bytes, Some(5_000_000 - 1_234_567));
    assert_eq!(
        runtime
            .egress_controls
            .snapshot(weaver_nntp::transfer::StableServerId(metered.id))
            .rate_bytes_per_sec,
        4_096
    );

    // A runtime built afresh over the same policies keeps counting from there.
    runtime.shutdown().await;
    drop(runtime);
    let rebuilt = NetworkRuntime::with_quota_policy(
        db.clone(),
        tokio::runtime::Handle::current(),
        Some(policy.clone()),
    )
    .unwrap();
    spend(&rebuilt, metered.id, 1_000);
    assert_eq!(
        policy.egress_snapshot(metered.id).unwrap().used_bytes,
        1_235_567
    );

    // Persisted usage restores into policies built from scratch.
    policy.flush_usage().unwrap();
    rebuilt.shutdown().await;
    drop(rebuilt);
    let restored = Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(db.clone(), &[])
            .unwrap(),
    );
    let runtime = NetworkRuntime::with_quota_policy(
        db,
        tokio::runtime::Handle::current(),
        Some(restored.clone()),
    )
    .unwrap();
    assert_eq!(
        restored.egress_snapshot(metered.id).unwrap().used_bytes,
        1_235_567
    );
    runtime.shutdown().await;
}

// A ladder whose chain stacks one WireGuard proxy on another, against two
// real peers: the second is reachable only inside the first, by a name only
// the first resolves.
#[tokio::test]
async fn a_wireguard_chain_rides_one_tunnel_inside_another() {
    use weaver_tunnel::test_support::{
        TEST_CLIENT_ADDRESS, TEST_PEER_ADDRESS, TEST_PEER_HTTP_PORT, WireGuardTestPeer,
        WireGuardTestPeerOptions, test_key,
    };
    const FAR_PORT: u16 = 51820;
    let far = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        private_key: test_key(31),
        body: "beyond the second tunnel".into(),
        ..WireGuardTestPeerOptions::default()
    })
    .await;
    let near = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        names: HashMap::from([("far.vpn.test".into(), vec![TEST_PEER_ADDRESS.into()])]),
        udp_forward: Some((FAR_PORT, far.endpoint())),
        ..WireGuardTestPeerOptions::default()
    })
    .await;
    let wireguard = wireguard_profile;

    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let consumer = Consumer::Server(1);
    let mut config = runtime.configuration.read().unwrap().clone();
    config.consumers.insert(consumer.key());
    config.profiles.insert(
        1,
        wireguard(1, &near, "127.0.0.1".into(), near.endpoint().port()),
    );
    config
        .profiles
        .insert(2, wireguard(2, &far, "far.vpn.test".into(), FAR_PORT));
    config.policies.insert(
        consumer.key(),
        RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: 0,
                weight: 100,
                path: LegPath::Ladder {
                    rungs: vec![Rung::Chain { ids: vec![1, 2] }],
                    direct_fallback: false,
                },
            }],
            ..Default::default()
        },
    );
    runtime.apply_configuration(config.clone()).unwrap();
    let route = runtime
        .route(consumer, 1, Duration::from_secs(300))
        .unwrap();
    let stage = route.legs.read().unwrap()[0].stage.clone();

    let mut dialed = stage
        .dial(&Target {
            host: TEST_PEER_ADDRESS.to_string(),
            port: TEST_PEER_HTTP_PORT,
            purpose: weaver_tunnel::pipe::Purpose::Probe,
            addresses: Vec::new(),
        })
        .await
        .unwrap();
    // The top of the chain is the proxy the connection went out through.
    assert_eq!(dialed.path.proxies, vec![1, 2]);
    assert_eq!(dialed.path.egress, 0);
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    dialed
        .stream
        .write_all(b"GET /chain HTTP/1.1\r\nHost: origin\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    dialed.stream.read_to_end(&mut answer).await.unwrap();
    assert!(String::from_utf8_lossy(&answer).ends_with("beyond the second tunnel"));
    assert_eq!(far.requests(), vec!["GET /chain HTTP/1.1"]);
    assert!(near.requests().is_empty());
    // The second tunnel's endpoint name was resolved inside the first.
    assert_eq!(near.dns_queries(), vec!["far.vpn.test"]);
    let forwarded = near.udp_forwarded();
    assert!(!forwarded.is_empty());
    for (source, seen) in &forwarded {
        assert_eq!(source.ip(), std::net::IpAddr::V4(TEST_CLIENT_ADDRESS));
        assert_eq!(seen.wireguard_datagrams, seen.datagrams, "{source}");
    }
    // Two tunnels, two sessions.
    assert_eq!(runtime.sessions.lock().unwrap().len(), 2);
    drop(dialed);

    // WireGuard never rides a stream proxy.
    config.profiles.insert(3, proxy_with_id(3));
    let bottom = NetworkRuntime::bottom(&config, SYSTEM_EGRESS_ID, Duration::from_secs(1)).unwrap();
    let socks = runtime
        .hop(&config, 3, bottom.clone(), bottom.clone(), &[])
        .unwrap();
    let error = match runtime.hop(&config, 2, socks, bottom, &[3]) {
        Ok(_) => panic!("WireGuard over SOCKS5 must be refused"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        "WireGuard must be the first hop on an egress or sit directly on another WireGuard hop"
    );
    runtime.shutdown().await;
}

fn proxy_with_id(id: u32) -> ProxyProfile {
    ProxyProfile { id, ..proxy() }
}

// A WireGuard profile for `peer`, reached at `host:port`.
fn wireguard_profile(
    id: u32,
    peer: &weaver_tunnel::test_support::WireGuardTestPeer,
    host: String,
    port: u16,
) -> ProxyProfile {
    use base64::Engine;
    let spec = peer.client_spec("route");
    let key = |bytes: [u8; 32]| base64::engine::general_purpose::STANDARD.encode(bytes);
    ProxyProfile {
        id,
        name: format!("wg-{id}"),
        kind: ProxyKind::WireGuard,
        host,
        port,
        dns_servers: spec.dns_servers.clone(),
        tunnel_addresses: vec![format!(
            "{}/32",
            weaver_tunnel::test_support::TEST_CLIENT_ADDRESS
        )],
        peer_public_key: Some(key(spec.peer_public_key)),
        mtu: spec.mtu,
        timeout_seconds: 300,
        secrets: ProxySecrets {
            private_key: Some(key(spec.private_key)),
            preshared_key: spec.preshared_key.map(key),
            ..Default::default()
        },
        ..proxy()
    }
}

// The port the near peer relays to the far one, inside its tunnel.
const FAR_PORT: u16 = 51820;

// Two peers, the far one reachable only through the near one, which also
// resolves `far.vpn.test` to it.
async fn stacked_peers() -> (
    weaver_tunnel::test_support::WireGuardTestPeer,
    weaver_tunnel::test_support::WireGuardTestPeer,
) {
    use weaver_tunnel::test_support::{
        TEST_PEER_ADDRESS, WireGuardTestPeer, WireGuardTestPeerOptions, test_key,
    };
    let far = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        private_key: test_key(31),
        body: "beyond the second tunnel".into(),
        ..WireGuardTestPeerOptions::default()
    })
    .await;
    let near = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        names: HashMap::from([("far.vpn.test".into(), vec![TEST_PEER_ADDRESS.into()])]),
        udp_forward: Some((FAR_PORT, far.endpoint())),
        body: "the near peer".into(),
        ..WireGuardTestPeerOptions::default()
    })
    .await;
    (near, far)
}

fn wireguard_ladder(rung: Rung) -> RoutingPolicy {
    RoutingPolicy {
        legs: vec![RouteLeg {
            egress_id: 0,
            weight: 100,
            path: LegPath::Ladder {
                rungs: vec![rung],
                direct_fallback: false,
            },
        }],
        ..Default::default()
    }
}

// The body the test peer's HTTP service at the far end of `stage` answers.
async fn fetch_through(stage: &Arc<dyn Dialer>) -> Result<String, DialError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut dialed = stage
        .dial(&Target {
            host: weaver_tunnel::test_support::TEST_PEER_ADDRESS.to_string(),
            port: weaver_tunnel::test_support::TEST_PEER_HTTP_PORT,
            purpose: weaver_tunnel::pipe::Purpose::Probe,
            addresses: Vec::new(),
        })
        .await?;
    dialed
        .stream
        .write_all(b"GET / HTTP/1.1\r\nHost: origin\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    dialed.stream.read_to_end(&mut answer).await.unwrap();
    Ok(String::from_utf8_lossy(&answer).into_owned())
}

// Testing a draft that stacks WireGuard on a live WireGuard session borrows
// that session; ending the draft must leave it running for the live route.
#[tokio::test]
async fn ending_a_draft_that_stacked_on_a_live_session_leaves_that_session_running() {
    let (near, far) = stacked_peers().await;
    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let consumer = Consumer::Server(1);
    let mut config = runtime.configuration.read().unwrap().clone();
    config.consumers.insert(consumer.key());
    config.profiles.insert(
        1,
        wireguard_profile(1, &near, "127.0.0.1".into(), near.endpoint().port()),
    );
    config.profiles.insert(
        2,
        wireguard_profile(2, &far, "far.vpn.test".into(), FAR_PORT),
    );
    config
        .policies
        .insert(consumer.key(), wireguard_ladder(Rung::Proxy { id: 1 }));
    runtime.apply_configuration(config).unwrap();
    let route = runtime
        .route(consumer, 1, Duration::from_secs(300))
        .unwrap();
    let live: Arc<dyn Dialer> = route.legs.read().unwrap()[0].stage.clone();
    assert!(
        fetch_through(&live)
            .await
            .unwrap()
            .ends_with("the near peer")
    );

    let draft = runtime
        .draft_route(
            consumer,
            wireguard_ladder(Rung::Chain { ids: vec![1, 2] }),
            Duration::from_secs(300),
        )
        .unwrap();
    draft.revoke().await;

    let answer = fetch_through(&live)
        .await
        .expect("the live session outlives the draft that borrowed it");
    assert!(answer.ends_with("the near peer"));
    runtime.shutdown().await;
}

// Bulk replies must fit a 1280-byte carrier, including when four download
// streams are active and the carried endpoint is resolved inside it.
#[tokio::test]
async fn a_wireguard_chain_downloads_four_concurrent_bulk_streams() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use weaver_tunnel::test_support::{
        TEST_CLIENT_ADDRESS, TEST_PEER_ADDRESS, WireGuardTestPeer, WireGuardTestPeerOptions,
        test_key, test_preshared_key,
    };
    const STREAMS: usize = 4;
    const BYTES: usize = 512 * 1024;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let forward = listener.local_addr().unwrap();
    let ready = Arc::new(tokio::sync::Barrier::new(STREAMS));
    let upstream = tokio::spawn(async move {
        let mut replies = tokio::task::JoinSet::new();
        for _ in 0..STREAMS {
            let (mut socket, _) = listener.accept().await.unwrap();
            let ready = ready.clone();
            replies.spawn(async move {
                let mut request = [0u8; 1];
                socket.read_exact(&mut request).await.unwrap();
                assert!(usize::from(request[0]) < STREAMS);
                let body: Vec<_> = (0..BYTES)
                    .map(|offset| (offset % 251) as u8 ^ request[0])
                    .collect();
                ready.wait().await;
                socket.write_all(&body).await.unwrap();
                socket.shutdown().await.unwrap();
            });
        }
        while let Some(reply) = replies.join_next().await {
            reply.unwrap();
        }
    });
    let far = WireGuardTestPeer::start_for_downloads(
        WireGuardTestPeerOptions {
            private_key: test_key(31),
            preshared_key: Some(test_preshared_key()),
            ..Default::default()
        },
        Some(forward),
        Duration::ZERO,
        false,
    )
    .await;
    let near = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        names: HashMap::from([("far.vpn.test".into(), vec![TEST_PEER_ADDRESS.into()])]),
        udp_forward: Some((FAR_PORT, far.endpoint())),
        ..Default::default()
    })
    .await;
    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let consumer = Consumer::Server(1);
    let mut config = runtime.configuration.read().unwrap().clone();
    config.consumers.insert(consumer.key());
    config.profiles.insert(
        1,
        wireguard_profile(1, &near, "127.0.0.1".into(), near.endpoint().port()),
    );
    config.profiles.insert(
        2,
        wireguard_profile(2, &far, "far.vpn.test".into(), FAR_PORT),
    );
    assert_eq!(config.profiles[&1].mtu, 1280);
    assert_eq!(config.profiles[&2].mtu, 1280);
    config.policies.insert(
        consumer.key(),
        wireguard_ladder(Rung::Chain { ids: vec![1, 2] }),
    );
    runtime.apply_configuration(config).unwrap();
    let route = runtime
        .route(consumer, STREAMS as u16, Duration::from_secs(300))
        .unwrap();
    assert!(route.legs.read().unwrap()[0].warning.is_none());
    let stage = route.legs.read().unwrap()[0].stage.clone();
    let mut downloads = tokio::task::JoinSet::new();
    for index in 0..STREAMS {
        let stage = stage.clone();
        let port = far.http_port();
        downloads.spawn(async move {
            let mut dialed = stage
                .dial(&Target {
                    host: TEST_PEER_ADDRESS.to_string(),
                    port,
                    purpose: weaver_tunnel::pipe::Purpose::Nntp { server: 1, leg: 0 },
                    addresses: Vec::new(),
                })
                .await
                .unwrap();
            assert_eq!(dialed.path.proxies, vec![1, 2]);
            assert_eq!(dialed.path.egress, SYSTEM_EGRESS_ID);
            dialed.stream.write_all(&[index as u8]).await.unwrap();
            let mut answer = vec![0; BYTES];
            dialed.stream.read_exact(&mut answer).await.unwrap();
            let expected: Vec<_> = (0..BYTES)
                .map(|offset| (offset % 251) as u8 ^ index as u8)
                .collect();
            assert_eq!(answer, expected, "stream {index}");
        });
    }
    while let Some(download) = downloads.join_next().await {
        download.unwrap();
    }
    upstream.await.unwrap();
    assert_eq!(near.dns_queries(), vec!["far.vpn.test"]);
    assert!(near.requests().is_empty());
    let forwarded = near.udp_forwarded();
    assert!(!forwarded.is_empty());
    for (source, seen) in &forwarded {
        assert_eq!(source.ip(), std::net::IpAddr::V4(TEST_CLIENT_ADDRESS));
        assert_eq!(seen.wireguard_datagrams, seen.datagrams, "{source}");
    }
    assert_eq!(runtime.sessions.lock().unwrap().len(), 2);
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_three_hop_wireguard_chain_sizes_each_tunnel_from_its_carrier() {
    use weaver_tunnel::test_support::{
        TEST_PEER_ADDRESS, WireGuardTestPeer, WireGuardTestPeerOptions, test_key,
    };
    let body = "third tunnel payload".repeat(4096);
    let far = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        private_key: test_key(31),
        body: body.clone(),
        ..Default::default()
    })
    .await;
    let middle = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        private_key: test_key(33),
        names: HashMap::from([("far.vpn.test".into(), vec![TEST_PEER_ADDRESS.into()])]),
        udp_forward: Some((FAR_PORT, far.endpoint())),
        ..Default::default()
    })
    .await;
    let near = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        names: HashMap::from([("middle.vpn.test".into(), vec![TEST_PEER_ADDRESS.into()])]),
        udp_forward: Some((FAR_PORT, middle.endpoint())),
        ..Default::default()
    })
    .await;
    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let consumer = Consumer::Server(1);
    let mut config = runtime.configuration.read().unwrap().clone();
    config.consumers.insert(consumer.key());
    config.profiles.insert(
        1,
        wireguard_profile(1, &near, "127.0.0.1".into(), near.endpoint().port()),
    );
    config.profiles.insert(
        2,
        wireguard_profile(2, &middle, "middle.vpn.test".into(), FAR_PORT),
    );
    config.profiles.insert(
        3,
        wireguard_profile(3, &far, "far.vpn.test".into(), FAR_PORT),
    );
    config.policies.insert(
        consumer.key(),
        wireguard_ladder(Rung::Chain { ids: vec![1, 2, 3] }),
    );
    runtime.apply_configuration(config.clone()).unwrap();
    let route = runtime
        .route(consumer, 1, Duration::from_secs(300))
        .unwrap();
    assert!(route.legs.read().unwrap()[0].warning.is_none());
    let stage: Arc<dyn Dialer> = route.legs.read().unwrap()[0].stage.clone();
    let answer = fetch_through(&stage).await.unwrap();
    assert_eq!(answer.split_once("\r\n\r\n").unwrap().1, body);
    assert_eq!(near.dns_queries(), vec!["middle.vpn.test"]);
    assert_eq!(middle.dns_queries(), vec!["far.vpn.test"]);
    let bottom =
        NetworkRuntime::bottom(&config, SYSTEM_EGRESS_ID, Duration::from_secs(300)).unwrap();
    let ids = [1, 2, 3];
    let mut carrier: Arc<dyn Dialer> = bottom.clone();
    for (index, expected) in [1280, 1216, 1152].into_iter().enumerate() {
        assert_eq!(config.profiles[&ids[index]].mtu, 1280);
        carrier = runtime
            .hop(&config, ids[index], carrier, bottom.clone(), &ids[..index])
            .unwrap();
        assert_eq!(tunnel_mtu(carrier.clone()).await, Some(expected));
    }
    runtime.shutdown().await;
}

// The MTU of the WireGuard tunnel `session` runs, as seen by a socket bound
// inside it.
async fn tunnel_mtu(session: Arc<dyn Dialer>) -> Option<u16> {
    session
        .datagrams()
        .expect("a WireGuard session carries datagrams")
        .bind("192.0.2.9:9".parse().unwrap())
        .await
        .expect("a socket inside the tunnel")
        .link_mtu()
}

// Saving and removing a stacked route preserves its live carrier. The
// carried tunnel fits the carrier after resolving its endpoint.
#[tokio::test]
async fn stacking_on_a_wireguard_session_preserves_its_capacity_and_identity() {
    use weaver_tunnel::test_support::TEST_PEER_ADDRESS;
    for (upper_host, lower_mtu, upper_mtu, effective_upper_mtu) in [
        ("far.vpn.test".to_string(), 1280, 1280, 1216),
        (TEST_PEER_ADDRESS.to_string(), 1280, 1280, 1216),
        ("far.vpn.test".to_string(), 1420, 1280, 1280),
        (TEST_PEER_ADDRESS.to_string(), 1420, 1420, 1360),
    ] {
        let (near, far) = stacked_peers().await;
        let runtime = NetworkRuntime::new(
            Database::open_in_memory().unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let direct = Consumer::Server(1);
        let stacked = Consumer::Server(2);
        let mut config = runtime.configuration.read().unwrap().clone();
        config.consumers.insert(direct.key());
        config.profiles.insert(
            1,
            wireguard_profile(1, &near, "127.0.0.1".into(), near.endpoint().port()),
        );
        config
            .profiles
            .insert(2, wireguard_profile(2, &far, upper_host.clone(), FAR_PORT));
        assert_eq!(config.profiles[&1].mtu, 1280);
        config.profiles.get_mut(&1).unwrap().mtu = lower_mtu;
        config.profiles.get_mut(&2).unwrap().mtu = upper_mtu;
        config
            .policies
            .insert(direct.key(), wireguard_ladder(Rung::Proxy { id: 1 }));
        runtime.apply_configuration(config.clone()).unwrap();
        let route = runtime.route(direct, 1, Duration::from_secs(300)).unwrap();
        // The session the live route uses, as the runtime holds it.
        let session = |runtime: &NetworkRuntime, ids: &[u32], inner: Option<Arc<dyn Dialer>>| {
            let config = runtime.configuration.read().unwrap().clone();
            let bottom =
                NetworkRuntime::bottom(&config, SYSTEM_EGRESS_ID, Duration::from_secs(300))
                    .unwrap();
            let (id, prefix) = ids.split_last().unwrap();
            runtime
                .hop(
                    &config,
                    *id,
                    inner.unwrap_or(bottom.clone()),
                    bottom,
                    prefix,
                )
                .unwrap()
        };
        let alone = session(&runtime, &[1], None);
        assert_eq!(tunnel_mtu(alone.clone()).await, Some(lower_mtu));
        let first_stage = route.legs.read().unwrap()[0].stage.clone();

        config.consumers.insert(stacked.key());
        config.policies.insert(
            stacked.key(),
            wireguard_ladder(Rung::Chain { ids: vec![1, 2] }),
        );
        runtime.apply_configuration(config.clone()).unwrap();
        runtime.retire_unused_sessions().await;
        let stacked_route = runtime.route(stacked, 1, Duration::from_secs(300)).unwrap();
        assert!(stacked_route.legs.read().unwrap()[0].warning.is_none());
        let carrier = session(&runtime, &[1], None);
        assert!(Arc::ptr_eq(&alone, &carrier), "{upper_host}");
        assert!(Arc::ptr_eq(
            &first_stage,
            &route.legs.read().unwrap()[0].stage
        ));
        assert_eq!(
            tunnel_mtu(carrier.clone()).await,
            Some(lower_mtu),
            "{upper_host}"
        );
        let carried = session(&runtime, &[1, 2], Some(carrier.clone()));
        assert_eq!(
            tunnel_mtu(carried).await,
            Some(effective_upper_mtu),
            "{upper_host}"
        );

        config.policies.remove(&stacked.key());
        config.consumers.remove(&stacked.key());
        runtime.apply_configuration(config.clone()).unwrap();
        runtime.retire_unused_sessions().await;
        let again = session(&runtime, &[1], None);
        assert!(Arc::ptr_eq(&carrier, &again), "{upper_host}");
        assert_eq!(
            tunnel_mtu(again.clone()).await,
            Some(lower_mtu),
            "{upper_host}"
        );

        // Editing the carrier itself still rebuilds the session, including
        // an explicit MTU change carried by the profile revision.
        let edited = config.profiles.get_mut(&1).unwrap();
        edited.mtu += 16;
        edited.revision += 1;
        runtime.apply_configuration(config).unwrap();
        let changed = session(&runtime, &[1], None);
        assert!(!Arc::ptr_eq(&again, &changed));
        assert_eq!(tunnel_mtu(changed).await, Some(lower_mtu + 16));
        runtime.shutdown().await;
    }
}
