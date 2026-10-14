use super::*;
use weaver_tunnel::test_support::{TEST_PEER_ADDRESS, TEST_PEER_HTTP_PORT};

fn socks(id: u32, port: u16) -> ProxyProfile {
    ProxyProfile {
        id,
        host: "127.0.0.1".into(),
        port,
        ..proxy()
    }
}

// A drafted route of one leg on the System egress.
fn draft(path: LegPath) -> Route {
    Route {
        legs: vec![RouteLeg {
            egress_id: SYSTEM_EGRESS_ID,
            weight: 100,
            path,
        }],
        failover: Failover::default(),
    }
}

fn ladder(rungs: Vec<Rung>) -> LegPath {
    LegPath::Ladder {
        rungs,
        direct_fallback: false,
    }
}

fn runtime_with(
    profiles: Vec<ProxyProfile>,
    pools: Vec<ProxyPool>,
    policies: Vec<(Consumer, RoutingPolicy)>,
) -> Arc<NetworkRuntime> {
    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut config = runtime.configuration.read().unwrap().clone();
    config
        .profiles
        .extend(profiles.into_iter().map(|profile| (profile.id, profile)));
    config
        .pools
        .extend(pools.into_iter().map(|pool| (pool.id, pool)));
    for (consumer, policy) in policies {
        config.consumers.insert(consumer.key());
        config.policies.insert(consumer.key(), policy);
    }
    runtime.apply_configuration(config).unwrap();
    runtime
}

async fn refusal(runtime: &NetworkRuntime, path: LegPath, subject: TestSubject) -> NetworkRefusal {
    runtime
        .test_via(&draft(path), subject, None)
        .await
        .unwrap_err()
}

#[test]
fn every_path_through_a_route_is_tried_once() {
    let pools = HashMap::from([(
        7,
        ProxyPool {
            id: 7,
            name: "pool".into(),
            kind: ProxyKind::Socks5,
            member_ids: vec![2, 3],
            enabled: true,
        },
    )]);
    let route = Route {
        legs: vec![
            RouteLeg {
                egress_id: 0,
                weight: 40,
                path: LegPath::Ladder {
                    rungs: vec![
                        Rung::Proxy { id: 1 },
                        Rung::Pool { id: 7 },
                        Rung::Chain { ids: vec![4, 5] },
                    ],
                    direct_fallback: true,
                },
            },
            // The same direct line as the first leg's fallback.
            RouteLeg {
                egress_id: 0,
                weight: 30,
                path: LegPath::Direct,
            },
            RouteLeg {
                egress_id: 1,
                weight: 30,
                path: LegPath::Direct,
            },
        ],
        failover: Failover::default(),
    };
    assert_eq!(
        super::super::test_via::paths(&route, &pools),
        vec![
            (0, 0, vec![1]),
            (0, 0, vec![2]),
            (0, 0, vec![3]),
            (0, 0, vec![4, 5]),
            (0, 0, vec![]),
            (2, 1, vec![]),
        ]
    );
}

#[tokio::test]
async fn the_route_alone_reaches_its_target_and_needs_one() {
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = target.local_addr().unwrap().port();
    let runtime = runtime_with(vec![], vec![], vec![]);
    let route = draft(LegPath::Direct);
    assert_eq!(
        runtime
            .test_via(&route, TestSubject::Route, None)
            .await
            .unwrap_err(),
        NetworkRefusal::TestTargetRequired
    );
    let tests = runtime
        .test_via(&route, TestSubject::Route, Some(("127.0.0.1".into(), port)))
        .await
        .unwrap();
    assert_eq!(tests.len(), 1);
    assert_eq!(tests[0].outcome, Ok(ProxyTestPass::Connected));
    assert_eq!(tests[0].egress_id, SYSTEM_EGRESS_ID);
    assert!(tests[0].carriers.is_empty());
    assert_eq!(tests[0].proxy_id, None);
    assert!(tests[0].source_address.is_some());
    let _accepted = target.accept().await.unwrap();
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_stream_proxy_is_reached_at_its_endpoint_through_each_path() {
    let endpoint = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut disabled = socks(2, 1080);
    disabled.enabled = false;
    let runtime = runtime_with(
        vec![socks(1, endpoint.local_addr().unwrap().port()), disabled],
        vec![],
        vec![],
    );
    let route = draft(LegPath::Ladder {
        rungs: vec![Rung::Proxy { id: 2 }],
        direct_fallback: true,
    });
    let tests = runtime
        .test_via(&route, TestSubject::Proxy(1), None)
        .await
        .unwrap();
    let outcomes: Vec<_> = tests
        .iter()
        .map(|test| {
            (
                test.carriers.clone(),
                test.proxy_id,
                test.outcome.clone(),
                test.failing_proxy,
            )
        })
        .collect();
    assert_eq!(
        outcomes,
        vec![
            // A disabled carrier is named without anything being dialled.
            (vec![2], Some(1), Err(ProxyFault::ProxyUnavailable), Some(2)),
            (vec![], Some(1), Ok(ProxyTestPass::Reachable), None),
        ]
    );
    let _accepted = endpoint.accept().await.unwrap();
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_proxy_reachable_only_through_a_tunnel_is_tested_through_that_tunnel() {
    let (near, far) = stacked_peers().await;
    let runtime = runtime_with(
        vec![
            wireguard_profile(1, &near, "127.0.0.1".into(), near.endpoint().port()),
            wireguard_profile(2, &far, "far.vpn.test".into(), FAR_PORT),
        ],
        vec![],
        vec![],
    );
    let route = draft(ladder(vec![Rung::Proxy { id: 1 }]));
    let tests = runtime
        .test_via(&route, TestSubject::Proxy(2), None)
        .await
        .unwrap();
    assert_eq!(tests.len(), 1);
    assert_eq!(tests[0].carriers, vec![1]);
    assert_eq!(tests[0].proxy_id, Some(2));
    assert_eq!(tests[0].outcome, Ok(ProxyTestPass::Connected));
    // The far endpoint's name was resolved inside the near tunnel.
    assert!(
        near.dns_queries()
            .iter()
            .any(|query| query == "far.vpn.test")
    );

    // Given a target, the far tunnel carries a connection to it.
    let tests = runtime
        .test_via(
            &route,
            TestSubject::Proxy(2),
            Some((TEST_PEER_ADDRESS.to_string(), TEST_PEER_HTTP_PORT)),
        )
        .await
        .unwrap();
    assert_eq!(tests[0].outcome, Ok(ProxyTestPass::Forwarded));
    // The test's sessions ended with it.
    assert!(runtime.sessions.lock().unwrap().is_empty());
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_test_through_a_route_that_cannot_carry_its_proxy_is_refused() {
    let (near, far) = stacked_peers().await;
    let mut profiles = vec![
        wireguard_profile(1, &near, "127.0.0.1".into(), near.endpoint().port()),
        wireguard_profile(2, &far, "127.0.0.1".into(), far.endpoint().port()),
    ];
    profiles.extend((3..=6).chain(10..=26).map(|id| socks(id, 1080)));
    let pool = ProxyPool {
        id: 9,
        name: "pool".into(),
        kind: ProxyKind::Socks5,
        member_ids: (10..=26).collect(),
        enabled: true,
    };
    let runtime = runtime_with(
        profiles,
        vec![pool],
        vec![(Consumer::Server(1), wireguard_ladder(Rung::Proxy { id: 2 }))],
    );
    // A proxy cannot carry itself.
    assert_eq!(
        refusal(
            &runtime,
            ladder(vec![Rung::Proxy { id: 3 }]),
            TestSubject::Proxy(3)
        )
        .await,
        NetworkRefusal::TestSubjectInRoute
    );
    // WireGuard never rides a stream proxy.
    assert_eq!(
        refusal(
            &runtime,
            ladder(vec![Rung::Proxy { id: 3 }]),
            TestSubject::Proxy(1)
        )
        .await,
        NetworkRefusal::RouteChainTransport
    );
    // A chain holds at most three proxies, the one under test among them.
    assert_eq!(
        refusal(
            &runtime,
            ladder(vec![Rung::Chain { ids: vec![3, 4, 5] }]),
            TestSubject::Proxy(6)
        )
        .await,
        NetworkRefusal::RouteChainLength
    );
    // Four paths for each of a pool's seventeen members is too many.
    assert_eq!(
        refusal(
            &runtime,
            ladder((3..=6).map(|id| Rung::Proxy { id }).collect()),
            TestSubject::Pool(9)
        )
        .await,
        NetworkRefusal::TestPathCount {
            limit: MAX_TEST_PATHS
        }
    );
    assert_eq!(
        refusal(&runtime, LegPath::Direct, TestSubject::Pool(99)).await,
        NetworkRefusal::PoolGone
    );
    assert_eq!(
        refusal(&runtime, LegPath::Direct, TestSubject::Proxy(99)).await,
        NetworkRefusal::ProxyNotFound
    );
    // A saved route holds WireGuard proxy 2 directly on the egress, so a
    // test does not reach it through proxy 1 there.
    assert_eq!(
        refusal(
            &runtime,
            ladder(vec![Rung::Proxy { id: 1 }]),
            TestSubject::Proxy(2)
        )
        .await,
        NetworkRefusal::WireGuardPathConflict {
            proxy: 2,
            egress: SYSTEM_EGRESS_ID,
            paths: vec![vec![1, 2], vec![2]],
        }
    );
    // On the saved route's own path it is tested.
    let tests = runtime
        .test_via(&draft(LegPath::Direct), TestSubject::Proxy(2), None)
        .await
        .unwrap();
    assert_eq!(tests[0].outcome, Ok(ProxyTestPass::Connected));
    runtime.shutdown().await;
}
