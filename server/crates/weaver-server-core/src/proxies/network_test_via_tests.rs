use super::*;
use std::net::IpAddr;
use weaver_tunnel::test_support::{
    CLIENT_ED25519_PEM, HOST_KEY_FINGERPRINT, SshServerDouble, SshServerOptions, TEST_PEER_ADDRESS,
    TEST_PEER_HTTP_PORT, dns::DnsServerDouble, proxy::ProxyServerDouble,
};

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

fn ip(text: &str) -> IpAddr {
    text.parse().unwrap()
}

// How the one path of a test through `route` resolved `host`.
async fn resolved(
    runtime: &NetworkRuntime,
    route: &Route,
    subject: TestSubject,
    target: Option<(&str, u16)>,
) -> (Result<ProxyTestPass, ProxyFault>, HostResolution) {
    let mut tests = runtime
        .test_via(
            route,
            subject,
            target.map(|(host, port)| (host.to_owned(), port)),
        )
        .await
        .unwrap();
    assert_eq!(tests.len(), 1);
    let test = tests.remove(0);
    (test.outcome, test.resolution.expect("a resolution"))
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
    let resolution = tests[0].resolution.clone().unwrap();
    assert_eq!(resolution.host, "far.vpn.test");
    assert_eq!(resolution.by, ResolvedBy::ProxyServers);
    assert_eq!(resolution.proxy, Some(1));
    assert_eq!(resolution.servers, near.client_spec("route").dns_servers);
    assert_eq!(resolution.addresses, [IpAddr::V4(TEST_PEER_ADDRESS)]);
    assert_eq!(resolution.connected, Some(IpAddr::V4(TEST_PEER_ADDRESS)));
    assert_eq!(resolution.error, None);

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
    let resolution = tests[0].resolution.clone().unwrap();
    assert_eq!(resolution.by, ResolvedBy::Literal);
    assert_eq!(resolution.connected, Some(IpAddr::V4(TEST_PEER_ADDRESS)));
    // A name the far tunnel's DNS server does not know fails the lookup.
    let (outcome, resolution) = resolved(
        &runtime,
        &route,
        TestSubject::Proxy(2),
        Some(("missing.vpn.test", TEST_PEER_HTTP_PORT)),
    )
    .await;
    assert!(outcome.is_err());
    assert_eq!(resolution.by, ResolvedBy::ProxyServers);
    assert_eq!(resolution.proxy, Some(2));
    assert!(resolution.addresses.is_empty());
    assert_eq!(
        resolution.error.map(|error| error.failure),
        Some(LookupFailure::NoSuchName)
    );
    // The test's sessions ended with it.
    assert!(runtime.sessions.lock().unwrap().is_empty());
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_direct_path_says_how_its_target_was_resolved() {
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = target.local_addr().unwrap().port();
    let runtime = runtime_with(vec![], vec![], vec![]);
    let route = draft(LegPath::Direct);
    // An address is its own answer.
    let (outcome, resolution) = resolved(
        &runtime,
        &route,
        TestSubject::Route,
        Some(("127.0.0.1", port)),
    )
    .await;
    assert_eq!(outcome, Ok(ProxyTestPass::Connected));
    assert_eq!(resolution.by, ResolvedBy::Literal);
    assert_eq!(resolution.addresses, [ip("127.0.0.1")]);
    assert_eq!(resolution.connected, Some(ip("127.0.0.1")));
    let _accepted = target.accept().await.unwrap();
    // A name is looked up with this machine's DNS, and the answer is held
    // for the next dial.
    for held in [false, true] {
        let (outcome, resolution) = resolved(
            &runtime,
            &route,
            TestSubject::Route,
            Some(("localhost", port)),
        )
        .await;
        assert_eq!(outcome, Ok(ProxyTestPass::Connected));
        assert_eq!(resolution.host, "localhost");
        assert_eq!(resolution.by, ResolvedBy::Host);
        assert_eq!(resolution.proxy, None);
        assert!(resolution.addresses.contains(&ip("127.0.0.1")));
        assert_eq!(resolution.connected, Some(ip("127.0.0.1")));
        assert_eq!(resolution.cached, held);
        assert_eq!(resolution.error, None);
        let _accepted = target.accept().await.unwrap();
    }
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_connect_that_fails_after_a_lookup_still_says_what_the_name_resolved_to() {
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    let runtime = runtime_with(vec![], vec![], vec![]);
    let (outcome, resolution) = resolved(
        &runtime,
        &draft(LegPath::Direct),
        TestSubject::Route,
        Some(("localhost", port)),
    )
    .await;
    assert_eq!(outcome, Err(ProxyFault::ConnectionRefused));
    assert_eq!(resolution.by, ResolvedBy::Host);
    assert!(resolution.addresses.contains(&ip("127.0.0.1")));
    assert_eq!(resolution.connected, None);
    assert_eq!(resolution.error, None);
    runtime.shutdown().await;
}

// The DNS server a stream proxy asks through itself.
const ROUTED_DNS: &str = "192.0.2.53";
// What that server resolves `named.invalid` to.
const NAMED: &str = "198.51.100.7";

// A SOCKS5 proxy at `double`, resolving as `resolver` and `dns` say.
fn socks_at(
    id: u32,
    double: &ProxyServerDouble,
    resolver: crate::proxies::Resolver,
    dns: &[&str],
) -> ProxyProfile {
    ProxyProfile {
        id,
        port: double.addr.port(),
        resolver,
        dns_servers: dns.iter().map(|server| ip(server)).collect(),
        // Hop timeouts are not under test here.
        timeout_seconds: 300,
        ..socks(id, 0)
    }
}

#[tokio::test]
async fn a_proxy_test_with_a_target_says_who_resolved_it() {
    use crate::proxies::Resolver as Naming;
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let to = target.local_addr().unwrap();
    let dns = DnsServerDouble::start(HashMap::from([(
        "named.invalid".into(),
        NAMED.parse().unwrap(),
    )]))
    .await;
    let double = ProxyServerDouble::start(
        weaver_tunnel::transport::TransportKind::Socks5,
        None,
        HashMap::from([
            ((ROUTED_DNS.into(), 53), dns.addr),
            ((NAMED.into(), to.port()), to),
            (("named.invalid".into(), to.port()), to),
            (("127.0.0.1".into(), to.port()), to),
        ]),
    )
    .await;
    let runtime = runtime_with(
        vec![
            socks_at(1, &double, Naming::Proxy, &[ROUTED_DNS]),
            socks_at(2, &double, Naming::Proxy, &[]),
            socks_at(3, &double, Naming::Weaver, &[]),
        ],
        vec![],
        vec![],
    );
    let route = draft(LegPath::Direct);
    let named = Some(("named.invalid", to.port()));

    // DNS servers asked through the proxy.
    let (outcome, resolution) = resolved(&runtime, &route, TestSubject::Proxy(1), named).await;
    assert_eq!(outcome, Ok(ProxyTestPass::Forwarded));
    assert_eq!(
        resolution,
        HostResolution {
            host: "named.invalid".into(),
            by: ResolvedBy::ProxyServers,
            proxy: Some(1),
            servers: vec![ip(ROUTED_DNS)],
            addresses: vec![ip(NAMED)],
            cached: false,
            connected: Some(ip(NAMED)),
            error: None,
        }
    );
    let _accepted = target.accept().await.unwrap();

    // The proxy handed the name.
    let (outcome, resolution) = resolved(&runtime, &route, TestSubject::Proxy(2), named).await;
    assert_eq!(outcome, Ok(ProxyTestPass::Forwarded));
    assert_eq!(resolution.by, ResolvedBy::Proxy);
    assert_eq!(resolution.proxy, Some(2));
    assert!(resolution.addresses.is_empty());
    assert_eq!(resolution.connected, None);
    let _accepted = target.accept().await.unwrap();

    // This machine's DNS, with the proxy handed the address.
    let (outcome, resolution) = resolved(
        &runtime,
        &route,
        TestSubject::Proxy(3),
        Some(("localhost", to.port())),
    )
    .await;
    assert_eq!(outcome, Ok(ProxyTestPass::Forwarded));
    assert_eq!(resolution.by, ResolvedBy::Host);
    assert_eq!(resolution.proxy, None);
    assert!(resolution.addresses.contains(&ip("127.0.0.1")));
    assert_eq!(resolution.connected, Some(ip("127.0.0.1")));
    let _accepted = target.accept().await.unwrap();

    // A name the servers do not know fails the lookup, in their words.
    let (outcome, resolution) = resolved(
        &runtime,
        &route,
        TestSubject::Proxy(1),
        Some(("missing.invalid", to.port())),
    )
    .await;
    assert_eq!(outcome, Err(ProxyFault::NameNotResolved));
    assert_eq!(resolution.by, ResolvedBy::ProxyServers);
    assert!(resolution.addresses.is_empty());
    assert_eq!(
        resolution.error,
        Some(LookupError {
            failure: LookupFailure::NoSuchName,
            detail: format!("{ROUTED_DNS}: NXDOMAIN"),
        })
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn an_ssh_test_without_a_target_says_how_its_endpoint_was_resolved() {
    let ssh = SshServerDouble::start(SshServerOptions::default()).await;
    let mut profile = ProxyProfile {
        id: 1,
        kind: ProxyKind::Ssh,
        host: "localhost".into(),
        port: ssh.port(),
        timeout_seconds: 300,
        host_key_fingerprint: Some(HOST_KEY_FINGERPRINT.into()),
        ..proxy()
    };
    profile.secrets.username = Some("operator".into());
    profile.secrets.private_key = Some(CLIENT_ED25519_PEM.into());
    // An SSH test pins the host key it met, so the profile is saved.
    let db = Database::open_in_memory().unwrap();
    db.save_proxy_profile(&profile).unwrap();
    let runtime = NetworkRuntime::new(db, tokio::runtime::Handle::current()).unwrap();
    let (outcome, resolution) = resolved(
        &runtime,
        &draft(LegPath::Direct),
        TestSubject::Proxy(1),
        None,
    )
    .await;
    assert_eq!(outcome, Ok(ProxyTestPass::Connected));
    assert_eq!(resolution.host, "localhost");
    assert_eq!(resolution.by, ResolvedBy::Host);
    assert!(resolution.addresses.contains(&ip("127.0.0.1")));
    assert_eq!(resolution.connected, Some(ip("127.0.0.1")));
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
