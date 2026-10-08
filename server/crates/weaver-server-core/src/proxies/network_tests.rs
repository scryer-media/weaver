use super::*;

fn route(weights: &[u8], failover: Failover) -> Route {
    Route {
        legs: weights
            .iter()
            .map(|&weight| RouteLeg {
                egress_id: 0,
                weight,
                path: LegPath::Direct,
            })
            .collect(),
        failover,
    }
}

fn profile(id: u32, kind: ProxyKind) -> ProxyProfile {
    ProxyProfile {
        id,
        name: format!("proxy-{id}"),
        kind,
        enabled: true,
        host: "proxy.example".into(),
        port: 443,
        dns_servers: vec!["192.0.2.53".parse().unwrap()],
        tunnel_addresses: vec![],
        peer_public_key: None,
        mtu: 1420,
        keepalive_seconds: None,
        timeout_seconds: 30,
        host_key_fingerprint: None,
        revision: 1,
        secrets: Default::default(),
    }
}

#[test]
fn worked_example_and_hold_keep_the_cap() {
    let mut route = route(&[60, 30, 10], Failover::Redistribute);
    assert_eq!(
        route.targets(50, &[true, true, false]).unwrap(),
        [33, 17, 0]
    );
    route.failover = Failover::Hold;
    assert_eq!(
        route.targets(50, &[true, true, false]).unwrap(),
        [30, 15, 0]
    );
    assert_eq!(
        route.targets(50, &[false, false, false]).unwrap(),
        [0, 0, 0]
    );
    route.failover = Failover::Redistribute;
    assert_eq!(
        route.targets(50, &[false, false, false]).unwrap(),
        [0, 0, 0]
    );
}

#[test]
fn largest_remainder_properties_across_weights_caps_and_health() {
    for first in 1..99 {
        for second in 1..100 - first {
            let mut route = route(
                &[first, second, 100 - first - second],
                Failover::Redistribute,
            );
            for cap in [0, 1, 2, 3, 7, 20, 50, 100, u16::MAX] {
                let baseline = route.targets(cap, &[true; 3]).unwrap();
                for mask in 0..8 {
                    let healthy = [mask & 1 != 0, mask & 2 != 0, mask & 4 != 0];
                    route.failover = Failover::Redistribute;
                    let targets = route.targets(cap, &healthy).unwrap();
                    assert_eq!(
                        targets.iter().map(|&n| u32::from(n)).sum::<u32>(),
                        if mask == 0 { 0 } else { u32::from(cap) }
                    );
                    let denominator: u32 = route
                        .legs
                        .iter()
                        .zip(healthy)
                        .filter(|(_, up)| *up)
                        .map(|(leg, _)| u32::from(leg.weight))
                        .sum();
                    for (i, &target) in targets.iter().enumerate() {
                        if !healthy[i] {
                            assert_eq!(target, 0);
                        } else {
                            assert!(
                                u32::from(target)
                                    <= (u32::from(cap) * u32::from(route.legs[i].weight))
                                        .div_ceil(denominator)
                            );
                        }
                    }
                    route.failover = Failover::Hold;
                    let targets = route.targets(cap, &healthy).unwrap();
                    for i in 0..3 {
                        assert_eq!(targets[i], if healthy[i] { baseline[i] } else { 0 });
                    }
                }
            }
        }
    }
}

#[test]
fn ties_are_by_position_and_single_leg_matches_existing_capacity() {
    assert_eq!(
        route(&[50, 50], Failover::Redistribute)
            .targets(1, &[true, true])
            .unwrap(),
        [1, 0]
    );
    assert_eq!(
        route(&[50, 50], Failover::Hold)
            .targets(1, &[false, true])
            .unwrap(),
        [0, 0]
    );
    for mode in [Failover::Redistribute, Failover::Hold] {
        for cap in [0, 1, 50, u16::MAX] {
            assert_eq!(route(&[100], mode).targets(cap, &[true]).unwrap(), [cap]);
        }
    }
}

#[test]
fn malformed_weights_and_ladders_are_not_normalized() {
    for weights in [&[][..], &[0, 100], &[50, 49], &[101], &[1; 9]] {
        assert!(
            route(weights, Failover::Redistribute)
                .validate_shape()
                .is_err()
        );
    }
    let mut route = route(&[100], Failover::Redistribute);
    assert!(route.targets(50, &[]).is_err());
    route.legs[0].path = LegPath::Ladder {
        rungs: vec![],
        direct_fallback: true,
    };
    assert!(route.validate_shape().unwrap_err().contains("rungs"));
    route.legs[0].path = LegPath::Ladder {
        rungs: vec![Rung::Chain { ids: vec![1] }],
        direct_fallback: false,
    };
    assert!(route.validate_shape().unwrap_err().contains("chain"));
}

#[test]
fn legacy_proxy_only_and_blocked_routes_never_gain_direct_fallback() {
    for proxy_ids in [vec![], vec![1, 2]] {
        for allow_direct in [false, true] {
            let legacy = RoutingPolicy {
                proxy_ids: proxy_ids.clone(),
                allow_direct,
                ..Default::default()
            };
            let route = Route::from_legacy(&legacy);
            assert_eq!(route.legacy_policy(), Some(legacy));
            let encoded = serde_json::to_string(&route).unwrap();
            assert_eq!(serde_json::from_str::<Route>(&encoded).unwrap(), route);
        }
    }
}

#[test]
fn legacy_writes_refuse_lossy_routes() {
    let mut route = route(&[100], Failover::Redistribute);
    route.legs[0].egress_id = 1;
    assert!(route.legacy_policy().is_none());
    route.legs[0].egress_id = 0;
    for rung in [Rung::Pool { id: 1 }, Rung::Chain { ids: vec![1, 2] }] {
        route.legs[0].path = LegPath::Ladder {
            rungs: vec![rung],
            direct_fallback: false,
        };
        assert!(route.legacy_policy().is_none());
    }
    assert!(
        super::tests::route(&[50, 50], Failover::Redistribute)
            .legacy_policy()
            .is_none()
    );
}

#[test]
fn reference_validation_catches_overlap_udp_chains_and_rss_dns() {
    let egresses = HashMap::from([(0, EgressInterface::system())]);
    let mut profiles = HashMap::from([
        (1, profile(1, ProxyKind::Socks5)),
        (2, profile(2, ProxyKind::Socks5)),
    ]);
    let pools = HashMap::from([(
        1,
        ProxyPool {
            id: 1,
            name: "Region".into(),
            kind: ProxyKind::Socks5,
            member_ids: vec![1, 2],
            enabled: true,
        },
    )]);
    let mut route = route(&[100], Failover::Redistribute);
    route.legs[0].path = LegPath::Ladder {
        rungs: vec![Rung::Pool { id: 1 }, Rung::Proxy { id: 2 }],
        direct_fallback: false,
    };
    assert!(
        route
            .validate_references(&egresses, &profiles, &pools, false)
            .unwrap_err()
            .contains("twice")
    );
    route.legs[0].path = LegPath::Ladder {
        rungs: vec![Rung::Chain { ids: vec![1, 2] }],
        direct_fallback: false,
    };
    profiles.get_mut(&2).unwrap().kind = ProxyKind::WireGuard;
    assert!(
        route
            .validate_references(&egresses, &profiles, &pools, false)
            .unwrap_err()
            .contains("first")
    );
    profiles.get_mut(&2).unwrap().kind = ProxyKind::Socks5;
    assert!(
        route
            .validate_references(&egresses, &profiles, &pools, true)
            .is_ok()
    );
    profiles.get_mut(&2).unwrap().dns_servers.clear();
    assert!(
        route
            .validate_references(&egresses, &profiles, &pools, true)
            .unwrap_err()
            .contains("DNS")
    );
    route.legs[0].egress_id = 7;
    assert!(
        route
            .validate_references(&egresses, &profiles, &pools, false)
            .unwrap_err()
            .contains("egress")
    );
}

#[test]
fn a_wireguard_hop_may_sit_only_on_the_egress_or_another_wireguard_hop() {
    let egresses = HashMap::from([(0, EgressInterface::system())]);
    let profiles = HashMap::from([
        (1, profile(1, ProxyKind::WireGuard)),
        (2, profile(2, ProxyKind::WireGuard)),
        (3, profile(3, ProxyKind::Ssh)),
        (4, profile(4, ProxyKind::HttpConnect)),
        (5, profile(5, ProxyKind::Http3Connect)),
        (6, profile(6, ProxyKind::WireGuard)),
    ]);
    let pools = HashMap::new();
    let check = |ids: &[u32]| {
        let mut route = route(&[100], Failover::Redistribute);
        route.legs[0].path = LegPath::Ladder {
            rungs: vec![Rung::Chain { ids: ids.to_vec() }],
            direct_fallback: false,
        };
        route.validate_references(&egresses, &profiles, &pools, false)
    };
    for accepted in [
        &[1, 2][..],
        &[1, 2, 3],
        &[1, 2, 4],
        &[1, 2, 6],
        &[5, 3],
        &[1, 3, 4],
    ] {
        assert!(check(accepted).is_ok(), "{accepted:?}");
    }
    // WireGuard never rides a stream proxy, and HTTP/3 rides nothing.
    for refused in [
        &[4, 1][..],
        &[3, 1],
        &[1, 3, 2],
        &[1, 5],
        &[2, 1, 5],
        &[5, 1],
    ] {
        let error = check(refused).unwrap_err();
        assert!(
            error.contains("WireGuard and HTTP/3 must be the first proxy in a chain"),
            "{refused:?}: {error}"
        );
    }
    assert!(check(&[1, 2, 1]).unwrap_err().contains("twice"));
}

#[test]
fn a_stacked_wireguard_hop_is_its_own_instance() {
    let profiles = HashMap::from([
        (1, profile(1, ProxyKind::WireGuard)),
        (2, profile(2, ProxyKind::WireGuard)),
    ]);
    let ladder = |rung: Rung| {
        let mut route = route(&[100], Failover::Redistribute);
        route.legs[0].path = LegPath::Ladder {
            rungs: vec![rung],
            direct_fallback: false,
        };
        route
    };
    // The chain's lower hop is the same tunnel as the first route's; its
    // upper hop rides on it and is a second tunnel apart from the third's.
    let routes = [
        ladder(Rung::Proxy { id: 1 }),
        ladder(Rung::Chain { ids: vec![1, 2] }),
        ladder(Rung::Proxy { id: 2 }),
    ];
    let pools = HashMap::new();
    assert!(validate_instance_budget(&routes, &profiles, &pools, 3).is_ok());
    let error = validate_instance_budget(&routes, &profiles, &pools, 2).unwrap_err();
    assert!(error.contains("require 3 WireGuard instances"), "{error}");
}

fn ladder_on(egress_id: u32, rung: Rung) -> Route {
    let mut route = route(&[100], Failover::Redistribute);
    route.legs[0].egress_id = egress_id;
    route.legs[0].path = LegPath::Ladder {
        rungs: vec![rung],
        direct_fallback: false,
    };
    route
}

#[test]
fn a_wireguard_proxy_reaches_one_egress_by_only_one_path() {
    let profiles: HashMap<_, _> = (1..=3)
        .map(|id| (id, profile(id, ProxyKind::WireGuard)))
        .collect();
    let pools = HashMap::from([(
        7,
        ProxyPool {
            id: 7,
            name: "pool".into(),
            kind: ProxyKind::WireGuard,
            member_ids: vec![2],
            enabled: true,
        },
    )]);
    let check = |routes: &[Route]| validate_wireguard_paths(routes, &profiles, &pools);
    // The lower hop of a chain is the same session as the proxy used
    // directly, so sharing it is fine, as is the same path twice.
    assert!(
        check(&[
            ladder_on(0, Rung::Proxy { id: 1 }),
            ladder_on(0, Rung::Chain { ids: vec![1, 2] }),
            ladder_on(0, Rung::Chain { ids: vec![1, 2, 3] }),
            ladder_on(0, Rung::Chain { ids: vec![1, 2] }),
        ])
        .is_ok()
    );
    // Another egress is another connection to the server.
    assert!(
        check(&[
            ladder_on(0, Rung::Chain { ids: vec![1, 2] }),
            ladder_on(5, Rung::Proxy { id: 2 }),
            ladder_on(6, Rung::Chain { ids: vec![3, 2] }),
        ])
        .is_ok()
    );
    for (routes, paths) in [
        (
            vec![
                ladder_on(0, Rung::Proxy { id: 2 }),
                ladder_on(0, Rung::Chain { ids: vec![1, 2] }),
            ],
            "(proxy 1 → proxy 2 and proxy 2)",
        ),
        (
            vec![
                ladder_on(0, Rung::Chain { ids: vec![1, 2] }),
                ladder_on(0, Rung::Chain { ids: vec![3, 2] }),
            ],
            "(proxy 1 → proxy 2 and proxy 3 → proxy 2)",
        ),
        (
            vec![
                ladder_on(0, Rung::Pool { id: 7 }),
                ladder_on(0, Rung::Chain { ids: vec![1, 2] }),
            ],
            "(proxy 1 → proxy 2 and proxy 2)",
        ),
    ] {
        let error = check(&routes).unwrap_err();
        assert!(
            error.starts_with("WireGuard proxy 2 is used on egress 0 by two different paths"),
            "{error}"
        );
        assert!(error.contains(paths), "{error}");
    }
}

fn wireguard_at(id: u32, host: &str) -> ProxyProfile {
    ProxyProfile {
        host: host.into(),
        mtu: 1280,
        ..profile(id, ProxyKind::WireGuard)
    }
}

/// The session MTUs a set of single-chain routes plans, by session path.
fn planned(chains: &[&[u32]], profiles: &HashMap<u32, ProxyProfile>) -> HashMap<Vec<u32>, u16> {
    let routes: Vec<_> = chains
        .iter()
        .map(|ids| ladder_on(0, Rung::Chain { ids: ids.to_vec() }))
        .collect();
    wireguard_mtu_needs(
        &wireguard_sessions(&routes, profiles, &HashMap::new()),
        profiles,
    )
    .into_iter()
    .map(|((_, path), need)| {
        let mtu = effective_wireguard_mtu(&profiles[path.last().unwrap()], Some(need));
        (path, mtu)
    })
    .collect()
}

/// What the tunnel engine leaves a tunnel carried over a link of `link`.
fn carried(configured: u16, link: u16, endpoint: &ProxyProfile) -> u16 {
    let address = endpoint
        .host
        .parse()
        .unwrap_or_else(|_| "2001:db8::1".parse().unwrap());
    weaver_tunnel::wireguard::carried_mtu(configured, link, address).unwrap()
}

#[test]
fn a_wireguard_tunnel_is_raised_to_carry_a_full_sized_tunnel_on_top() {
    let profiles = HashMap::from([
        (1, wireguard_at(1, "192.0.2.1")),
        (2, wireguard_at(2, "10.8.0.1")),
        (3, wireguard_at(3, "fd00::1")),
        (4, wireguard_at(4, "vpn.example.test")),
        (5, wireguard_at(5, "10.9.0.1")),
        (6, wireguard_at(6, "fd00::2")),
    ]);
    // Nothing stacked: the profile's own MTU.
    assert_eq!(
        planned(&[&[1]], &profiles),
        HashMap::from([(vec![1], 1280)])
    );
    // An IPv4 endpoint above costs 60 bytes, an IPv6 one or a name 80.
    let v4 = planned(&[&[1, 2]], &profiles);
    assert_eq!(v4, HashMap::from([(vec![1], 1340), (vec![1, 2], 1280)]));
    assert_eq!(carried(1280, 1340, &profiles[&2]), 1280);
    assert_eq!(carried(1280, 1344, &profiles[&2]), 1280);
    assert_eq!(carried(1280, 1339, &profiles[&2]), 1264);
    for upper in [3, 4] {
        let planned = planned(&[&[1, upper]], &profiles);
        assert_eq!(planned[&vec![1]], 1360, "{upper}");
        assert_eq!(carried(1280, 1360, &profiles[&upper]), 1280);
        assert_eq!(carried(1280, 1359, &profiles[&upper]), 1264);
    }
    // Three deep: the middle tunnel's own MTU is rounded down to 16 by the
    // one beneath, so that one leaves room for the rounded-up need.
    let v4 = planned(&[&[1, 2, 5]], &profiles);
    assert_eq!(v4[&vec![1]], 1404);
    assert_eq!(v4[&vec![1, 2]], 1340);
    let middle = carried(1340, 1404, &profiles[&2]);
    assert_eq!(middle, 1340);
    assert_eq!(carried(1280, middle, &profiles[&5]), 1280);
    // Three IPv6 hops need 1440, more than the 1420 cap: the bottom stops
    // there and the top runs short, which the route reports.
    let v6 = planned(&[&[1, 3, 6]], &profiles);
    assert_eq!(v6[&vec![1, 3]], 1360);
    assert_eq!(v6[&vec![1]], MAX_RAISED_WIREGUARD_MTU);
    let middle = carried(1360, 1420, &profiles[&3]);
    assert_eq!(middle, 1328);
    assert_eq!(carried(1280, middle, &profiles[&6]), 1248);
    assert_eq!(
        carried_chain_mtu(&[
            (1420, &profiles[&1]),
            (1360, &profiles[&3]),
            (1280, &profiles[&6])
        ]),
        Some(1248)
    );
    // The need of a hop comes from everything stacked on it; the largest wins.
    let shared = planned(&[&[1, 2], &[1, 4]], &profiles);
    assert_eq!(shared[&vec![1]], 1360);
    // A profile already wider than the need keeps its own MTU.
    let wide = HashMap::from([
        (
            1,
            ProxyProfile {
                mtu: 1420,
                ..wireguard_at(1, "192.0.2.1")
            },
        ),
        (2, wireguard_at(2, "10.8.0.1")),
    ]);
    assert_eq!(planned(&[&[1, 2]], &wide)[&vec![1]], 1420);
}

#[test]
fn a_chain_the_capped_uplink_cannot_carry_runs_its_top_short() {
    let profiles = HashMap::from([
        (1, wireguard_at(1, "192.0.2.1")),
        (2, wireguard_at(2, "fd00::1")),
        (3, wireguard_at(3, "fd00::2")),
        (4, wireguard_at(4, "fd00::3")),
    ]);
    let planned = planned(&[&[1, 2, 3, 4]], &profiles);
    assert_eq!(planned[&vec![1]], MAX_RAISED_WIREGUARD_MTU);
    let hops: Vec<_> = [vec![1], vec![1, 2], vec![1, 2, 3], vec![1, 2, 3, 4]]
        .iter()
        .map(|path| (planned[path], &profiles[path.last().unwrap()]))
        .collect();
    let top = carried_chain_mtu(&hops).unwrap();
    assert!(top < CARRIED_WIREGUARD_MTU, "{top}");
    // The same arithmetic as the engine, hop by hop.
    let mut link = planned[&vec![1]];
    for path in [vec![1, 2], vec![1, 2, 3], vec![1, 2, 3, 4]] {
        link = carried(planned[&path], link, &profiles[path.last().unwrap()]);
    }
    assert_eq!(link, top);
}

#[test]
fn system_egress_is_unique_and_immutable() {
    let mut egress = EgressInterface::system();
    assert!(egress.validate().is_ok());
    egress.enabled = false;
    assert!(egress.validate().is_err());
    egress.enabled = true;
    egress.id = 1;
    assert!(egress.validate().is_err());
    egress.binding = EgressBinding::SourceAddress {
        address: "0.0.0.0".parse().unwrap(),
    };
    assert!(egress.validate().is_err());
    egress.binding = EgressBinding::SourceAddress {
        address: "192.0.2.1".parse().unwrap(),
    };
    assert!(egress.validate().is_ok());
}

#[test]
fn stored_policy_without_direct_permission_fails_closed() {
    let policy: RoutingPolicy = serde_json::from_str(r#"{"proxyIds":[1]}"#).unwrap();
    assert!(!policy.allow_direct);
    assert!(matches!(
        policy.route().legs[0].path,
        LegPath::Ladder {
            direct_fallback: false,
            ..
        }
    ));
    assert!(RoutingPolicy::default().allow_direct);
}
