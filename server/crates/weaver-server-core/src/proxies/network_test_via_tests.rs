// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::proxies::ProxiesPersistenceQueries;
use crate::proxies::SYSTEM_DNS_PROFILE_ID;
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

// The DNS profile a test names when Weaver asks listed servers, on list
// `LISTED`.
const WEAVER_DNS: u32 = 1;
const LISTED: u32 = 1;
// The DNS profile a test names when the proxy answers.
const PROXY_DNS: u32 = 2;

fn dns_profile(
    id: u32,
    who: crate::proxies::DnsWho,
    transit: Option<crate::proxies::DnsTransit>,
) -> crate::proxies::DnsProfile {
    crate::proxies::DnsProfile {
        kind: crate::proxies::DnsProfileKind::Route,
        id,
        name: format!("DNS {id}"),
        who,
        transit,
        trust: crate::proxies::DnsTrust::AsSent,
        server_list_id: transit.map(|_| LISTED),
        system_fallback: false,
    }
}

// Stored DNS of profile `PROXY_DNS`, answered by the proxy, and profile
// `WEAVER_DNS`, asking the plain servers at `servers` in order through
// `transit`.
fn stored_dns(
    transit: crate::proxies::DnsTransit,
    servers: &[std::net::SocketAddr],
) -> crate::proxies::StoredDns {
    use crate::proxies::DnsWho;
    let list = crate::proxies::DnsServerList {
        id: LISTED,
        name: "Listed".into(),
        servers: servers
            .iter()
            .map(|address| crate::proxies::ListedServer {
                address: *address,
                wire: crate::proxies::ListedWire::Plain,
            })
            .collect(),
    };
    crate::proxies::StoredDns {
        lists: HashMap::from([(LISTED, list)]),
        profiles: HashMap::from([
            (
                WEAVER_DNS,
                dns_profile(WEAVER_DNS, DnsWho::Weaver, Some(transit)),
            ),
            (PROXY_DNS, dns_profile(PROXY_DNS, DnsWho::Proxy, None)),
        ]),
        ..Default::default()
    }
}

// `runtime`'s configuration with `dns` stored, as a save of it loads it.
fn store_dns(runtime: &NetworkRuntime, dns: crate::proxies::StoredDns) {
    let mut config = runtime.configuration.read().unwrap().clone();
    config.dns = Arc::new(dns);
    runtime.apply_configuration(config).unwrap();
}

// How the one path of a test through `route` resolved `host`, naming by
// DNS profile `dns`.
async fn resolved(
    runtime: &NetworkRuntime,
    route: &Route,
    dns: u32,
    subject: TestSubject,
    target: Option<(&str, u16)>,
) -> (Result<ProxyTestPass, ProxyFault>, HostResolution) {
    let mut tests = runtime
        .test_via(
            route,
            dns,
            subject,
            target.map(|(host, port)| (host.to_owned(), port)),
        )
        .await
        .unwrap();
    assert_eq!(tests.len(), 1);
    let test = tests.remove(0);
    let resolution = test.resolution.unwrap_or_else(|| {
        panic!(
            "no resolution for {target:?}: outcome={:?}, failing_proxy={:?}",
            test.outcome, test.failing_proxy
        )
    });
    (test.outcome, resolution)
}

async fn refusal(runtime: &NetworkRuntime, path: LegPath, subject: TestSubject) -> NetworkRefusal {
    runtime
        .test_via(&draft(path), SYSTEM_DNS_PROFILE_ID, subject, None)
        .await
        .unwrap_err()
}

#[tokio::test]
async fn empty_pool_tests_are_refused_before_planning() {
    let runtime = runtime_with(
        vec![],
        vec![ProxyPool {
            id: 7,
            name: "empty".into(),
            kind: ProxyKind::Socks5,
            member_ids: vec![],
            enabled: true,
        }],
        vec![],
    );
    for subject in [TestSubject::Pool(7), TestSubject::Members(7)] {
        for target in [None, Some(("example.test".to_owned(), 443))] {
            let error = runtime
                .test_via(
                    &draft(LegPath::Direct),
                    SYSTEM_DNS_PROFILE_ID,
                    subject,
                    target,
                )
                .await
                .unwrap_err();
            assert_eq!(error, NetworkRefusal::RoutePoolEmpty);
        }
    }
    runtime.shutdown().await;
}

// A drafted route on a DNS profile it cannot answer on is refused by its
// test as its save would refuse it, naming no route: a draft has no holder.
#[tokio::test]
async fn a_test_refuses_a_route_its_dns_profile_cannot_answer_on() {
    use crate::proxies::{DnsTrust, DnsWho, StoredDns};
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    let tunnel = |id: u32, dns_servers: Vec<IpAddr>| ProxyProfile {
        id,
        kind: ProxyKind::WireGuard,
        dns_servers,
        ..socks(id, port)
    };
    let runtime = runtime_with(
        vec![
            socks(1, port),
            tunnel(2, vec![ip("10.0.0.53")]),
            tunnel(3, vec![]),
        ],
        vec![],
        vec![],
    );
    const ASKED: u32 = 3;
    const VALIDATED: u32 = 4;
    const FALLBACK: u32 = 5;
    let profile = |id: u32, trust: DnsTrust, system_fallback: bool| crate::proxies::DnsProfile {
        trust,
        system_fallback,
        ..dns_profile(id, DnsWho::Proxy, None)
    };
    store_dns(
        &runtime,
        StoredDns {
            profiles: HashMap::from([
                (ASKED, profile(ASKED, DnsTrust::AsSent, false)),
                (VALIDATED, profile(VALIDATED, DnsTrust::Validate, false)),
                (FALLBACK, profile(FALLBACK, DnsTrust::Validate, true)),
            ]),
            ..Default::default()
        },
    );
    let refused = |path: LegPath, dns: u32| {
        let runtime = runtime.clone();
        async move {
            runtime
                .test_via(
                    &draft(path),
                    dns,
                    TestSubject::Route,
                    Some(("127.0.0.1".into(), 9)),
                )
                .await
                .err()
        }
    };
    let rung = |id: u32| ladder(vec![Rung::Proxy { id }]);
    // Validating a SOCKS5 proxy's answers, even with the host to fall back
    // to on a leg with no proxy.
    for dns in [VALIDATED, FALLBACK] {
        assert_eq!(
            refused(rung(1), dns).await,
            Some(NetworkRefusal::DnsProfileNeedsWireGuard { routes: vec![] })
        );
    }
    // The proxy answering on a leg with no proxy, and nothing to fall back
    // to; with a fallback, only the validation is refused.
    assert_eq!(
        refused(LegPath::Direct, ASKED).await,
        Some(NetworkRefusal::DnsProfileNeedsProxy { routes: vec![] })
    );
    assert_eq!(
        refused(LegPath::Direct, FALLBACK).await,
        Some(NetworkRefusal::DnsProfileNeedsWireGuard { routes: vec![] })
    );
    // The proxy answering at a tunnel that lists no DNS servers.
    assert_eq!(
        refused(rung(3), ASKED).await,
        Some(NetworkRefusal::DnsProfileNeedsTunnelDns {
            proxies: vec![3],
            routes: vec![]
        })
    );
    // A proxy under test tops each path, so it answers on a direct leg
    // beneath it, and validating its answers is refused when it is not
    // WireGuard. Its closed port fails the dial a test that runs makes.
    let proxied = |dns: u32| {
        let runtime = runtime.clone();
        async move {
            runtime
                .test_via(
                    &draft(LegPath::Direct),
                    dns,
                    TestSubject::Proxy(1),
                    Some(("127.0.0.1".into(), 9)),
                )
                .await
        }
    };
    assert_eq!(
        proxied(VALIDATED).await.err(),
        Some(NetworkRefusal::DnsProfileNeedsWireGuard { routes: vec![] })
    );
    let tests = proxied(ASKED).await.unwrap();
    assert_eq!(tests.len(), 1);
    assert!(tests[0].outcome.is_err(), "{:?}", tests[0].outcome);
    // A tunnel with DNS servers answers, validated or not; the proxy's
    // answers as sent and the system's are taken on any rung, and the
    // system's on a leg with no proxy.
    let config = runtime.configuration.read().unwrap().clone();
    for (path, dns) in [
        (rung(2), VALIDATED),
        (rung(2), ASKED),
        (rung(1), ASKED),
        (rung(1), SYSTEM_DNS_PROFILE_ID),
        (LegPath::Direct, SYSTEM_DNS_PROFILE_ID),
    ] {
        let profile = config.dns.profile(dns).unwrap();
        assert_eq!(
            crate::proxies::dns_persistence::validate_draft_route_dns(
                &draft(path.clone()),
                &profile,
                &config.profiles,
                &config.pools,
                &config.dns,
            ),
            Ok(()),
            "{path:?} on {dns}"
        );
    }
    runtime.shutdown().await;
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
            .test_via(&route, SYSTEM_DNS_PROFILE_ID, TestSubject::Route, None)
            .await
            .unwrap_err(),
        NetworkRefusal::TestTargetRequired
    );
    let tests = runtime
        .test_via(
            &route,
            SYSTEM_DNS_PROFILE_ID,
            TestSubject::Route,
            Some(("127.0.0.1".into(), port)),
        )
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
        .test_via(&route, SYSTEM_DNS_PROFILE_ID, TestSubject::Proxy(1), None)
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

// A probe owns fresh client sessions and shuts them down before returning.
// Give each scenario fresh peers too: a peer retained after its client closes
// can receive a Windows UDP reset and lose its receive loop.
async fn nested_tunnel_fixture() -> (
    Arc<NetworkRuntime>,
    Route,
    [weaver_tunnel::test_support::WireGuardTestPeer; 2],
) {
    let (near, far) = stacked_peers().await;
    let runtime = runtime_with(
        vec![
            wireguard_profile(1, &near, "127.0.0.1".into(), near.endpoint().port()),
            wireguard_profile(2, &far, "far.vpn.test".into(), FAR_PORT),
        ],
        vec![],
        vec![],
    );
    // The tunnels' own servers answer names dialled through them.
    store_dns(&runtime, stored_dns(crate::proxies::DnsTransit::Proxy, &[]));
    let route = draft(ladder(vec![Rung::Proxy { id: 1 }]));
    (runtime, route, [near, far])
}

#[tokio::test]
async fn a_proxy_reachable_only_through_a_tunnel_is_tested_through_that_tunnel() {
    let (runtime, route, [near, _far]) = nested_tunnel_fixture().await;
    let before = weaver_tunnel::metrics::dns_snapshot();
    let tests = runtime
        .test_via(&route, PROXY_DNS, TestSubject::Proxy(2), None)
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
    // The tunnel's own resolver asked its one server in plain; the answer
    // is recorded and counted as Weaver's own client records and counts one.
    use weaver_tunnel::{dns::WireKind, resolution::record::TrustOutcome};
    assert_eq!(resolution.wire, Some(WireKind::Plain));
    assert_eq!(resolution.answered_by, resolution.servers.first().copied());
    assert_eq!(resolution.trust, Some(TrustOutcome::AsSent));
    let after = weaver_tunnel::metrics::dns_snapshot();
    let moved = |counts: &[(TrustOutcome, u64)], then: &[(TrustOutcome, u64)]| {
        counts
            .iter()
            .zip(then)
            .find(|((outcome, _), _)| *outcome == TrustOutcome::AsSent)
            .map(|((_, now), (_, then))| now - then)
    };
    assert_eq!(moved(&after.trust, &before.trust), Some(1));
    let plain = |counts: &[(WireKind, u64)]| {
        counts
            .iter()
            .find(|(wire, _)| *wire == WireKind::Plain)
            .unwrap()
            .1
    };
    assert_eq!(plain(&after.wires) - plain(&before.wires), 1);

    assert!(runtime.sessions.lock().unwrap().is_empty());
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_nested_tunnel_probe_forwards_to_its_target() {
    let (runtime, route, _peers) = nested_tunnel_fixture().await;
    let tests = runtime
        .test_via(
            &route,
            PROXY_DNS,
            TestSubject::Proxy(2),
            Some((TEST_PEER_ADDRESS.to_string(), TEST_PEER_HTTP_PORT)),
        )
        .await
        .unwrap();
    assert_eq!(tests[0].outcome, Ok(ProxyTestPass::Forwarded));
    let resolution = tests[0].resolution.clone().unwrap();
    assert_eq!(resolution.by, ResolvedBy::Literal);
    assert_eq!(resolution.connected, Some(IpAddr::V4(TEST_PEER_ADDRESS)));
    assert!(runtime.sessions.lock().unwrap().is_empty());
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_nested_tunnel_probe_reports_a_missing_target_name() {
    let (runtime, route, _peers) = nested_tunnel_fixture().await;
    let (outcome, resolution) = resolved(
        &runtime,
        &route,
        PROXY_DNS,
        TestSubject::Proxy(2),
        Some(("missing.vpn.test", TEST_PEER_HTTP_PORT)),
    )
    .await;
    assert!(outcome.is_err());
    assert_eq!(resolution.by, ResolvedBy::ProxyServers);
    assert_eq!(resolution.proxy, Some(2));
    assert!(resolution.addresses.is_empty());
    // The tunnel's own resolver says the same of a name that does not
    // exist, a server failure and silence, so no cause is named, and
    // nothing was judged.
    assert_eq!(resolution.trust, None);
    assert_eq!(resolution.answered_by, None);
    let error = resolution.error.unwrap();
    assert_eq!(error.failure, LookupFailure::Other);
    assert!(
        error
            .detail
            .contains("does not say whether the name does not exist"),
        "{error:?}"
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
        SYSTEM_DNS_PROFILE_ID,
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
            SYSTEM_DNS_PROFILE_ID,
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
        SYSTEM_DNS_PROFILE_ID,
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

// A SOCKS5 proxy at `double`.
fn socks_at(id: u32, double: &ProxyServerDouble) -> ProxyProfile {
    ProxyProfile {
        id,
        port: double.addr.port(),
        // Hop timeouts are not under test here.
        timeout_seconds: 300,
        ..socks(id, 0)
    }
}

#[tokio::test]
async fn a_proxy_test_with_a_target_says_who_resolved_it() {
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
            socks_at(1, &double),
            socks_at(2, &double),
            socks_at(3, &double),
        ],
        vec![],
        vec![],
    );
    store_dns(
        &runtime,
        stored_dns(
            crate::proxies::DnsTransit::Proxy,
            &[std::net::SocketAddr::new(ip(ROUTED_DNS), 53)],
        ),
    );
    let route = draft(LegPath::Direct);
    let named = Some(("named.invalid", to.port()));

    // DNS servers asked through the proxy.
    let (outcome, resolution) =
        resolved(&runtime, &route, WEAVER_DNS, TestSubject::Proxy(1), named).await;
    assert_eq!(outcome, Ok(ProxyTestPass::Forwarded));
    assert_eq!(
        resolution,
        HostResolution {
            host: "named.invalid".into(),
            by: ResolvedBy::ProxyServers,
            proxy: Some(1),
            servers: vec![ip(ROUTED_DNS)],
            wire: Some(weaver_tunnel::dns::WireKind::Plain),
            answered_by: Some(ip(ROUTED_DNS)),
            trust: Some(weaver_tunnel::resolution::record::TrustOutcome::AsSent),
            addresses: vec![ip(NAMED)],
            cached: false,
            connected: Some(ip(NAMED)),
            error: None,
        }
    );
    let _accepted = target.accept().await.unwrap();

    // The proxy handed the name.
    let (outcome, resolution) =
        resolved(&runtime, &route, PROXY_DNS, TestSubject::Proxy(2), named).await;
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
        SYSTEM_DNS_PROFILE_ID,
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
        WEAVER_DNS,
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
        natpmp: false,
        forwarded_port: None,
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
        SYSTEM_DNS_PROFILE_ID,
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
        .test_via(
            &draft(LegPath::Direct),
            SYSTEM_DNS_PROFILE_ID,
            TestSubject::Proxy(2),
            None,
        )
        .await
        .unwrap();
    assert_eq!(tests[0].outcome, Ok(ProxyTestPass::Connected));
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_pool_carries_the_target_through_the_member_it_picks() {
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let to = target.local_addr().unwrap();
    let double = ProxyServerDouble::start(
        weaver_tunnel::transport::TransportKind::Socks5,
        None,
        HashMap::from([(("named.invalid".into(), to.port()), to)]),
    )
    .await;
    // An unmapped target gets an explicit SOCKS host-unreachable reply.
    // A closed OS port can time out before Windows reports a refusal.
    let refusing = ProxyServerDouble::start(
        weaver_tunnel::transport::TransportKind::Socks5,
        None,
        HashMap::new(),
    )
    .await;
    let pool = |id, member_ids| ProxyPool {
        id,
        name: "pool".into(),
        kind: ProxyKind::Socks5,
        member_ids,
        enabled: true,
    };
    let runtime = runtime_with(
        vec![socks_at(1, &double), socks_at(2, &refusing)],
        vec![pool(9, vec![2, 1]), pool(8, vec![2])],
        vec![],
    );
    store_dns(&runtime, stored_dns(crate::proxies::DnsTransit::Proxy, &[]));
    let route = draft(LegPath::Direct);
    let named = Some(("named.invalid".to_owned(), to.port()));
    let rows = |tests: &[PathTest]| {
        tests
            .iter()
            .map(|test| (test.pool_id, test.proxy_id, test.outcome.is_ok()))
            .collect::<Vec<_>>()
    };

    // The pool settles on the member that answers, and the lookup shown is
    // that member's.
    let tests = runtime
        .test_via(&route, PROXY_DNS, TestSubject::Pool(9), named.clone())
        .await
        .unwrap();
    assert_eq!(tests.len(), 1);
    assert_eq!(tests[0].outcome, Ok(ProxyTestPass::Forwarded));
    assert_eq!(tests[0].pool_id, Some(9));
    assert_eq!(tests[0].proxy_id, Some(1));
    assert_eq!(tests[0].failing_proxy, None);
    let resolution = tests[0].resolution.clone().unwrap();
    assert_eq!(resolution.host, "named.invalid");
    assert_eq!(resolution.by, ResolvedBy::Proxy);
    assert_eq!(resolution.proxy, Some(1));
    let _accepted = target.accept().await.unwrap();

    // Each member on its own, with or without a target.
    let tests = runtime
        .test_via(&route, PROXY_DNS, TestSubject::Members(9), named.clone())
        .await
        .unwrap();
    assert_eq!(
        rows(&tests),
        [(None, Some(2), false), (None, Some(1), true)]
    );
    let _accepted = target.accept().await.unwrap();
    let tests = runtime
        .test_via(&route, PROXY_DNS, TestSubject::Pool(9), None)
        .await
        .unwrap();
    // Without a target this probes endpoints. The refusing proxy is live,
    // even though it cannot carry the named destination.
    assert_eq!(rows(&tests), [(None, Some(2), true), (None, Some(1), true)]);
    for test in &tests {
        assert_eq!(test.outcome, Ok(ProxyTestPass::Reachable));
    }

    // A pool with no member that can carry the target picks none, and
    // attributes the explicit refusal to the member that returned it.
    let tests = runtime
        .test_via(&route, PROXY_DNS, TestSubject::Pool(8), named)
        .await
        .unwrap();
    assert_eq!(rows(&tests), [(Some(8), None, false)]);
    assert_eq!(
        tests[0].outcome,
        Err(ProxyFault::DestinationUnreachable {
            target: format!("named.invalid:{}", to.port()),
            reply: "SOCKS reply 4".into(),
        })
    );
    assert_eq!(tests[0].failing_proxy, Some(2));
    runtime.shutdown().await;
}

// A torrent route's test runs the test torrent over each path that reached
// the torrent's host, on that path's leg as the route's session would ride
// it: a direct leg on a loopback source address takes peers through its
// own sockets, and its loopback listener announces no port.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_route_runs_the_test_torrent_over_each_path() {
    use crate::speed_test::P2pStep;
    use crate::speed_test::torrent_fixture::{Answer, TorrentFixture};
    use crate::torrents::net::Exposure;

    let fixture = TorrentFixture::start(true).await;
    fixture.answer(Answer {
        peers: vec![fixture.seeder()],
        seeders: 1,
        leechers: 0,
        failure: None,
        connect_in: true,
    });
    let runtime = runtime_with(vec![], vec![], vec![]);
    let mut config = runtime.configuration.read().unwrap().clone();
    config.egresses.insert(
        5,
        EgressInterface {
            id: 5,
            name: "loopback".into(),
            binding: EgressBinding::SourceAddress {
                address: ip("127.0.0.1"),
            },
            ..EgressInterface::system()
        },
    );
    runtime.apply_configuration(config).unwrap();
    let route = Route {
        legs: vec![RouteLeg {
            egress_id: 5,
            weight: 100,
            path: LegPath::Direct,
        }],
        failover: Failover::default(),
    };
    let quiet = crate::settings::TorrentConfig {
        upnp: false,
        dht_bootstrap_nodes: String::new(),
        ..Default::default()
    };
    let tests = runtime
        .test_via_torrent(
            &route,
            SYSTEM_DNS_PROFILE_ID,
            TestSubject::Route,
            None,
            P2pTest {
                source: fixture.source.clone(),
                torrent: quiet,
                ip_filter: Ok(weaver_torrent::IpFilter::default()),
                allow_private_network: true,
                // A clock no one moves: the piece wait ends on the piece.
                clock: crate::speed_test::HandClock::new().clock(),
            },
        )
        .await
        .unwrap();

    assert_eq!(tests.len(), 1);
    assert_eq!(tests[0].outcome, Ok(ProxyTestPass::Connected));
    let report = tests[0].p2p.as_ref().expect("the torrent ran");
    for step in [
        P2pStep::Torrent,
        P2pStep::Announce,
        P2pStep::Handshake,
        P2pStep::Piece,
    ] {
        assert!(report.passed(step), "{step:?} in {report:?}");
    }
    assert!(!report.passed(P2pStep::Inbound), "{report:?}");
    assert_eq!(report.mapped_port, None);
    let exposure: Exposure = report.exposure.expect("the session opened");
    assert_eq!(exposure.resolved_by, Some(ResolvedBy::Host));
    assert_eq!(exposure.announced_port, 0);
    assert!(!exposure.inbound);
    assert!(exposure.udp_trackers && exposure.dht);
    fixture.stop().await;
    runtime.shutdown().await;
}

// A route of one leg on the System egress along `path`, naming by DNS
// profile `WEAVER_DNS`.
fn named_route(path: LegPath) -> RoutingPolicy {
    RoutingPolicy {
        legs: vec![RouteLeg {
            egress_id: SYSTEM_EGRESS_ID,
            weight: 100,
            path,
        }],
        dns_profile_id: Some(WEAVER_DNS),
        ..Default::default()
    }
}

// Profile `WEAVER_DNS` asking the plain servers at `servers` in order, from
// the egress.
fn egress_servers(servers: &[std::net::SocketAddr]) -> crate::proxies::StoredDns {
    stored_dns(crate::proxies::DnsTransit::Egress, servers)
}

// A port nothing listens on: a server there refuses every connection.
async fn refused_port() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

// A direct route whose DNS profile lists servers asks them from the egress,
// says so in the test, and never asks the host: a name only the host knows
// does not resolve.
#[tokio::test]
async fn a_direct_test_on_a_profile_with_servers_asks_them_and_never_the_host() {
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let to = target.local_addr().unwrap();
    let dns = DnsServerDouble::start(HashMap::from([(
        "named.invalid".into(),
        "127.0.0.1".parse().unwrap(),
    )]))
    .await;
    let feed = Consumer::Rss(1);
    let runtime = runtime_with(vec![], vec![], vec![(feed, named_route(LegPath::Direct))]);
    store_dns(&runtime, egress_servers(&[dns.addr]));
    let route = draft(LegPath::Direct);

    let (outcome, resolution) = resolved(
        &runtime,
        &route,
        WEAVER_DNS,
        TestSubject::Route,
        Some(("named.invalid", to.port())),
    )
    .await;
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(
        resolution,
        HostResolution {
            host: "named.invalid".into(),
            by: ResolvedBy::Servers,
            proxy: None,
            servers: vec![ip("127.0.0.1")],
            wire: Some(weaver_tunnel::dns::WireKind::Plain),
            answered_by: Some(ip("127.0.0.1")),
            trust: Some(weaver_tunnel::resolution::record::TrustOutcome::AsSent),
            addresses: vec![ip("127.0.0.1")],
            cached: false,
            connected: Some(ip("127.0.0.1")),
            error: None,
        }
    );
    let _accepted = target.accept().await.unwrap();

    // The host resolves `localhost`; the listed server does not know it.
    let (outcome, resolution) = resolved(
        &runtime,
        &route,
        WEAVER_DNS,
        TestSubject::Route,
        Some(("localhost", to.port())),
    )
    .await;
    assert_eq!(outcome, Err(ProxyFault::NameNotResolved));
    assert_eq!(resolution.by, ResolvedBy::Servers);
    assert!(resolution.addresses.is_empty());
    assert!(
        dns.queries
            .lock()
            .unwrap()
            .iter()
            .any(|name| name == "localhost"),
        "the listed server was asked"
    );

    // A torrent session's own sockets look names up the same way.
    let egress = runtime
        .egress_dialer(feed, SYSTEM_EGRESS_ID, Duration::from_secs(300))
        .unwrap();
    assert!(matches!(
        egress.resolve("named.invalid").await.unwrap(),
        weaver_tunnel::pipe::Resolution::Addresses(addresses) if addresses == [ip("127.0.0.1")]
    ));
    assert!(egress.resolve("localhost").await.is_err());
    assert_eq!(
        crate::torrents::sessions::names_on(&runtime, feed, &LegPath::Direct),
        Some(ResolvedBy::Servers)
    );
    runtime.shutdown().await;
}

// When no listed server answers, the lookup fails: the host is not asked
// in their place.
#[tokio::test]
async fn a_profile_whose_servers_do_not_answer_fails_closed() {
    let refused = refused_port().await;
    let feed = Consumer::Rss(1);
    let runtime = runtime_with(vec![], vec![], vec![(feed, named_route(LegPath::Direct))]);
    store_dns(&runtime, egress_servers(&[refused]));
    let egress = runtime
        .egress_dialer(feed, SYSTEM_EGRESS_ID, Duration::from_secs(300))
        .unwrap();
    assert!(egress.resolve("localhost").await.is_err());
    let dialed = egress
        .dial(&Target {
            host: "localhost".into(),
            port: 9,
            purpose: weaver_tunnel::pipe::Purpose::Probe,
            addresses: Vec::new(),
        })
        .await;
    assert!(dialed.is_err());
    runtime.shutdown().await;
}

// A save of a route's DNS servers keeps the legs, and the next lookup asks
// the new servers. A save that changes nothing keeps what the servers
// answered. Back on the host, the legs are still kept and the host answers.
#[tokio::test]
async fn a_save_of_a_routes_dns_keeps_its_legs_and_the_next_lookup_asks_the_new_setting() {
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let to = target.local_addr().unwrap();
    let names = || HashMap::from([("named.invalid".into(), "127.0.0.1".parse().unwrap())]);
    let first = DnsServerDouble::start(names()).await;
    let second = DnsServerDouble::start(names()).await;
    // Answers are held, so a kept naming asks no server again.
    first.ttl.store(3600, std::sync::atomic::Ordering::Relaxed);
    second.ttl.store(3600, std::sync::atomic::Ordering::Relaxed);
    let feed = Consumer::Rss(1);
    let runtime = runtime_with(vec![], vec![], vec![(feed, named_route(LegPath::Direct))]);
    store_dns(&runtime, egress_servers(&[first.addr]));
    let target_named = |host: &str| Target {
        host: host.into(),
        port: to.port(),
        purpose: weaver_tunnel::pipe::Purpose::Probe,
        addresses: Vec::new(),
    };
    let named = target_named("named.invalid");
    let asked = |dns: &DnsServerDouble| dns.queries.lock().unwrap().len();
    let route = runtime.route(feed, 1, Duration::from_secs(300)).unwrap();
    let stage = || route.legs.read().unwrap()[0].stage.clone();
    let before = stage();
    route.weighted.dial(&named).await.unwrap();
    let _accepted = target.accept().await.unwrap();
    let held = asked(&first);
    assert!(held > 0);

    // The same setting saved again keeps the leg and its answers.
    store_dns(&runtime, egress_servers(&[first.addr]));
    assert!(Arc::ptr_eq(&before, &stage()));
    route.weighted.dial(&named).await.unwrap();
    let _accepted = target.accept().await.unwrap();
    assert_eq!(asked(&first), held);

    // Another server: the leg is kept, and its next lookup asks the new one.
    store_dns(&runtime, egress_servers(&[second.addr]));
    assert!(!before.is_revoked());
    assert!(Arc::ptr_eq(&before, &stage()));
    route.weighted.dial(&named).await.unwrap();
    let _accepted = target.accept().await.unwrap();
    assert_eq!(asked(&first), held);
    assert!(asked(&second) > 0);

    // Back to the host: the leg is kept, and the host answers a name the
    // listed server does not know.
    let mut host = egress_servers(&[second.addr]);
    host.profiles.insert(
        WEAVER_DNS,
        dns_profile(WEAVER_DNS, crate::proxies::DnsWho::System, None),
    );
    store_dns(&runtime, host);
    assert!(!before.is_revoked());
    assert!(Arc::ptr_eq(&before, &stage()));
    route
        .weighted
        .dial(&target_named("localhost"))
        .await
        .unwrap();
    let _accepted = target.accept().await.unwrap();
    assert!(
        !second
            .queries
            .lock()
            .unwrap()
            .iter()
            .any(|name| name == "localhost")
    );
    runtime.shutdown().await;
}

// A proxy whose route asks listed servers from the egress has its
// destinations' names answered by those servers: the host is not asked,
// and every record says the servers answered. Back on the host, the proxy
// asks the host again.
#[tokio::test]
async fn a_proxy_on_a_profile_with_servers_from_the_egress_asks_them_and_never_the_host() {
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let to = target.local_addr().unwrap();
    let dns = DnsServerDouble::start(HashMap::from([(
        "named.invalid".into(),
        "127.0.0.1".parse().unwrap(),
    )]))
    .await;
    let double = ProxyServerDouble::start(
        weaver_tunnel::transport::TransportKind::Socks5,
        None,
        HashMap::from([(("127.0.0.1".into(), to.port()), to)]),
    )
    .await;
    let through = ladder(vec![Rung::Proxy { id: 3 }]);
    let feed = Consumer::Rss(1);
    let runtime = runtime_with(
        vec![socks_at(3, &double)],
        vec![],
        vec![(feed, named_route(through.clone()))],
    );
    store_dns(&runtime, egress_servers(&[dns.addr]));
    let route = draft(LegPath::Direct);

    let (outcome, resolution) = resolved(
        &runtime,
        &route,
        WEAVER_DNS,
        TestSubject::Proxy(3),
        Some(("named.invalid", to.port())),
    )
    .await;
    assert_eq!(outcome, Ok(ProxyTestPass::Forwarded));
    assert_eq!(resolution.by, ResolvedBy::Servers);
    assert_eq!(resolution.servers, vec![ip("127.0.0.1")]);
    assert_eq!(resolution.addresses, vec![ip("127.0.0.1")]);
    assert_eq!(resolution.connected, Some(ip("127.0.0.1")));
    let _accepted = target.accept().await.unwrap();

    // The host knows `localhost`; the listed server does not, and it is the
    // one asked.
    let (outcome, resolution) = resolved(
        &runtime,
        &route,
        WEAVER_DNS,
        TestSubject::Proxy(3),
        Some(("localhost", to.port())),
    )
    .await;
    assert_eq!(outcome, Err(ProxyFault::NameNotResolved));
    assert_eq!(resolution.by, ResolvedBy::Servers);
    assert!(resolution.addresses.is_empty());
    assert!(
        dns.queries
            .lock()
            .unwrap()
            .iter()
            .any(|name| name == "localhost"),
        "the listed server was asked"
    );
    // A torrent route through the proxy shows the same.
    assert_eq!(
        crate::torrents::sessions::names_on(&runtime, feed, &through),
        Some(ResolvedBy::Servers)
    );
    assert_eq!(
        runtime.resolved_by(feed, Some(3)),
        Some(ResolvedBy::Servers)
    );

    // On the host's DNS, the proxy test asks the host.
    let (outcome, resolution) = resolved(
        &runtime,
        &route,
        SYSTEM_DNS_PROFILE_ID,
        TestSubject::Proxy(3),
        Some(("localhost", to.port())),
    )
    .await;
    assert_eq!(outcome, Ok(ProxyTestPass::Forwarded));
    assert_eq!(resolution.by, ResolvedBy::Host);
    assert!(resolution.addresses.contains(&ip("127.0.0.1")));
    let _accepted = target.accept().await.unwrap();
    let mut host = egress_servers(&[dns.addr]);
    host.profiles.insert(
        WEAVER_DNS,
        dns_profile(WEAVER_DNS, crate::proxies::DnsWho::System, None),
    );
    store_dns(&runtime, host);
    assert_eq!(
        crate::torrents::sessions::names_on(&runtime, feed, &through),
        Some(ResolvedBy::Host)
    );
    runtime.shutdown().await;
}

// A profile that finds proxies' own addresses, never a route's names.
const PROXY_NAME_DNS: u32 = 9;

fn with_proxy_name_profile(runtime: &NetworkRuntime) {
    let mut dns = egress_servers(&[]);
    dns.profiles.insert(
        PROXY_NAME_DNS,
        crate::proxies::DnsProfile {
            kind: crate::proxies::DnsProfileKind::ProxyName,
            ..dns_profile(PROXY_NAME_DNS, crate::proxies::DnsWho::System, None)
        },
    );
    store_dns(runtime, dns);
}

const NOT_A_ROUTE_PROFILE: NetworkRefusal = NetworkRefusal::DnsProfileKind {
    expected: crate::proxies::DnsProfileKind::Route,
};

// A draft on a proxy name profile is refused as its save would be.
#[tokio::test]
async fn a_draft_on_a_proxy_name_profile_is_refused_as_its_save_is() {
    let runtime = runtime_with(vec![], vec![], vec![]);
    with_proxy_name_profile(&runtime);
    let policy = RoutingPolicy {
        dns_profile_id: Some(PROXY_NAME_DNS),
        ..named_route(LegPath::Direct)
    };
    assert_eq!(runtime.validate_policy(&policy), Err(NOT_A_ROUTE_PROFILE));
    let refused = runtime
        .draft_route(Consumer::Rss(1), policy, Duration::from_secs(1))
        .err()
        .unwrap();
    assert_eq!(refused, NOT_A_ROUTE_PROFILE.to_string());
    assert!(
        runtime
            .validate_policy(&named_route(LegPath::Direct))
            .is_ok()
    );
    runtime.shutdown().await;
}

// A test through a route on a proxy name profile is refused before any
// path is tried, whether it tests the route or listed servers.
#[tokio::test]
async fn a_test_on_a_proxy_name_profile_is_refused_as_its_save_is() {
    let runtime = runtime_with(vec![], vec![], vec![]);
    with_proxy_name_profile(&runtime);
    let route = draft(LegPath::Direct);
    let refused = runtime
        .test_via(
            &route,
            PROXY_NAME_DNS,
            TestSubject::Route,
            Some(("example.test".into(), 443)),
        )
        .await
        .unwrap_err();
    assert_eq!(refused, NOT_A_ROUTE_PROFILE);
    let refused = runtime
        .dns_server_test(
            &route,
            PROXY_NAME_DNS,
            vec![weaver_tunnel::dns::DnsServer::plain(ip("192.0.2.53"))],
        )
        .err()
        .unwrap();
    assert_eq!(refused, NOT_A_ROUTE_PROFILE);
    runtime.shutdown().await;
}

// A test of listed servers is refused where a save of its route would be,
// before anything is built or dialled: the proxy cannot answer on a leg with
// no proxy, and a WireGuard proxy is reached only by the path a saved route
// holds it on, when the servers are asked through the first path's proxies.
#[tokio::test]
async fn a_dns_server_test_is_refused_where_its_route_could_not_be_saved() {
    let (near, far) = stacked_peers().await;
    let runtime = runtime_with(
        vec![
            wireguard_profile(1, &near, "127.0.0.1".into(), near.endpoint().port()),
            wireguard_profile(2, &far, "127.0.0.1".into(), far.endpoint().port()),
        ],
        vec![],
        vec![(Consumer::Server(1), wireguard_ladder(Rung::Proxy { id: 2 }))],
    );
    store_dns(&runtime, stored_dns(crate::proxies::DnsTransit::Proxy, &[]));
    let test = |path: LegPath, dns: u32| {
        runtime
            .dns_server_test(
                &draft(path),
                dns,
                vec![weaver_tunnel::dns::DnsServer::plain(ip("192.0.2.53"))],
            )
            .err()
    };
    let chained = || ladder(vec![Rung::Chain { ids: vec![1, 2] }]);
    assert_eq!(
        test(LegPath::Direct, PROXY_DNS),
        Some(NetworkRefusal::DnsProfileNeedsProxy { routes: vec![] })
    );
    assert_eq!(
        test(chained(), WEAVER_DNS),
        Some(NetworkRefusal::WireGuardPathConflict {
            proxy: 2,
            egress: SYSTEM_EGRESS_ID,
            paths: vec![vec![1, 2], vec![2]],
        })
    );
    // On the saved route's own path the servers are asked through it, and
    // on the egress nothing is asked through the chain.
    assert_eq!(test(ladder(vec![Rung::Proxy { id: 2 }]), WEAVER_DNS), None);
    assert_eq!(test(LegPath::Direct, WEAVER_DNS), None);
    assert_eq!(test(chained(), SYSTEM_DNS_PROFILE_ID), None);
    runtime.shutdown().await;
}
