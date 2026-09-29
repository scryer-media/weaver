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
