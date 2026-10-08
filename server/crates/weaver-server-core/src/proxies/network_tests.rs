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
