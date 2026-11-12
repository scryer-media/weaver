// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;

use super::*;
use crate::proxies::{Resolver, RouteLeg, RoutingPolicy, TorrentScope};

fn profile(id: u32, kind: ProxyKind) -> ProxyProfile {
    ProxyProfile {
        id,
        name: format!("proxy-{id}"),
        kind,
        enabled: true,
        host: "192.0.2.1".into(),
        port: 51820,
        resolver: Resolver::Proxy,
        dns_servers: vec![],
        tunnel_addresses: vec![],
        peer_public_key: None,
        mtu: 1420,
        keepalive_seconds: None,
        timeout_seconds: 30,
        host_key_fingerprint: None,
        natpmp: false,
        forwarded_port: None,
        revision: 1,
        secrets: Default::default(),
    }
}

fn profiles(wireguard: &[u32], socks: &[u32]) -> HashMap<u32, ProxyProfile> {
    wireguard
        .iter()
        .map(|id| (*id, profile(*id, ProxyKind::WireGuard)))
        .chain(
            socks
                .iter()
                .map(|id| (*id, profile(*id, ProxyKind::Socks5))),
        )
        .collect()
}

fn leg(rung: Rung, weight: u8) -> RouteLeg {
    RouteLeg {
        egress_id: 0,
        weight,
        path: LegPath::Ladder {
            rungs: vec![rung],
            direct_fallback: false,
        },
    }
}

// Legs of equal weight, one per rung.
fn route(rungs: Vec<Rung>, failover: Failover) -> Route {
    let weight = 100 / rungs.len() as u8;
    Route {
        legs: rungs.into_iter().map(|rung| leg(rung, weight)).collect(),
        failover,
    }
}

fn torrent(rung: Rung) -> (Consumer, Route) {
    (
        Consumer::Torrent(TorrentScope::Default),
        route(vec![rung], Failover::Redistribute),
    )
}

fn tunnel(id: u32) -> SessionPath {
    (0, vec![id])
}

// A server whose route spreads over two tunnels asks each for every
// connection under Redistribute, since either leg takes the other's share
// when it fails, and for its own target under Hold. A pool asks each member
// for every connection either way. Each server keeps one more socket on
// each tunnel for replacing a connection.
#[test]
fn usenet_asks_every_connection_unless_its_route_holds() {
    let profiles = profiles(&[1, 2, 3, 4], &[]);
    let pools = HashMap::from([(
        9,
        ProxyPool {
            id: 9,
            name: "pool".into(),
            kind: ProxyKind::WireGuard,
            member_ids: vec![3, 4],
            enabled: true,
        },
    )]);
    let connections = HashMap::from([(1, 20), (2, 30)]);
    let two_legs = |failover| route(vec![Rung::Proxy { id: 1 }, Rung::Proxy { id: 2 }], failover);
    let pooled = route(vec![Rung::Pool { id: 9 }], Failover::Hold);

    let ledger = socket_ledger(
        &[
            (Consumer::Server(1), two_legs(Failover::Redistribute)),
            (Consumer::Server(2), pooled.clone()),
        ],
        &profiles,
        &pools,
        &connections,
        256,
    );
    assert_eq!(ledger[&tunnel(1)].nntp, 21);
    assert_eq!(ledger[&tunnel(2)].nntp, 21);
    assert_eq!(ledger[&tunnel(3)].nntp, 31);
    assert_eq!(ledger[&tunnel(4)].nntp, 31);

    let ledger = socket_ledger(
        &[(Consumer::Server(1), two_legs(Failover::Hold))],
        &profiles,
        &pools,
        &connections,
        256,
    );
    assert_eq!(ledger[&tunnel(1)].nntp, 11);
    assert_eq!(ledger[&tunnel(2)].nntp, 11);

    // Two servers on one tunnel each count their own replacement.
    let ledger = socket_ledger(
        &[
            (
                Consumer::Server(1),
                route(vec![Rung::Proxy { id: 1 }], Failover::Hold),
            ),
            (
                Consumer::Server(2),
                route(vec![Rung::Proxy { id: 1 }], Failover::Hold),
            ),
        ],
        &profiles,
        &pools,
        &connections,
        256,
    );
    assert_eq!(ledger[&tunnel(1)].nntp, 21 + 31);
    // A server that is not active asks nothing.
    let ledger = socket_ledger(
        &[(
            Consumer::Server(3),
            route(vec![Rung::Proxy { id: 1 }], Failover::Hold),
        )],
        &profiles,
        &pools,
        &connections,
        256,
    );
    assert_eq!(ledger[&tunnel(1)].nntp, 0);
}

// A chain of WireGuard hops is a session per hop; the connections open in
// the last one. A SOCKS or HTTP hop, and a route with no WireGuard hop,
// take nothing of any tunnel.
#[test]
fn connections_count_on_the_last_wireguard_hop_and_nothing_else_is_counted() {
    let profiles = profiles(&[1, 2], &[5]);
    let connections = HashMap::from([(1, 10)]);
    let ledger = socket_ledger(
        &[(
            Consumer::Server(1),
            route(vec![Rung::Chain { ids: vec![1, 2, 5] }], Failover::Hold),
        )],
        &profiles,
        &HashMap::new(),
        &connections,
        256,
    );
    assert_eq!(ledger.len(), 2);
    assert_eq!(ledger[&tunnel(1)].nntp, 0);
    assert_eq!(ledger[&(0, vec![1, 2])].nntp, 11);
    assert_eq!(ledger[&(0, vec![1, 2])].proxy, 2);

    let ledger = socket_ledger(
        &[
            (
                Consumer::Server(1),
                route(vec![Rung::Proxy { id: 5 }], Failover::Hold),
            ),
            torrent(Rung::Proxy { id: 5 }),
        ],
        &profiles,
        &HashMap::new(),
        &connections,
        256,
    );
    assert!(ledger.is_empty());
}

// Torrent routes over `sessions` legs that all reach tunnel 1, one session
// each.
fn torrent_sessions(sessions: usize) -> Vec<(Consumer, Route)> {
    let rungs = [
        Rung::Proxy { id: 1 },
        Rung::Chain { ids: vec![1, 5] },
        Rung::Chain { ids: vec![1, 6] },
    ];
    rungs
        .into_iter()
        .take(sessions)
        .zip(0u32..)
        .map(|(rung, category)| {
            (
                Consumer::Torrent(TorrentScope::Category(category)),
                route(vec![rung], Failover::Redistribute),
            )
        })
        .collect()
}

// Torrent routes alone on a tunnel give their TCP peers everything but the
// reserve, at each hardware profile's cap. The reserve keeps each session's
// two listeners, sixteen sockets each, beside its five datagram sockets,
// and hands none of the listeners' sockets to a share.
#[test]
fn torrent_peers_alone_take_all_but_the_reserve() {
    assert_eq!(LISTENING_PER_TORRENT_SESSION, 32);
    assert_eq!(SOCKETS_PER_TORRENT_SESSION, 37);
    for (cap, sessions, reserve, core, http, torrent_peers) in [
        (256, 1, 69, 13, 8, 187),
        (256, 2, 106, 18, 8, 150),
        (512, 1, 93, 13, 16, 419),
        (512, 2, 130, 18, 16, 382),
        (1024, 1, 141, 13, 32, 883),
        (1024, 2, 178, 18, 32, 846),
    ] {
        let ledger = socket_ledger(
            &torrent_sessions(sessions),
            &profiles(&[1], &[5, 6]),
            &HashMap::new(),
            &HashMap::new(),
            cap,
        );
        let tunnel = &ledger[&tunnel(1)];
        assert_eq!(tunnel.torrent_sessions, sessions, "{cap}");
        assert_eq!(tunnel.reserve(), reserve, "{cap} {sessions}");
        let shares = tunnel.shares();
        assert_eq!(
            shares,
            SocketShares {
                nntp: 0,
                torrent: torrent_peers,
                core,
                http,
            },
            "{cap} {sessions}"
        );
        assert_eq!(
            shares.nntp + shares.torrent + shares.core + shares.http + socket_listening(sessions),
            cap - socket_linger(cap),
            "{cap} {sessions}"
        );
    }
}

// Whatever usenet asks, and however many torrent sessions reach a tunnel,
// the shares it hands out and the sockets its torrent sessions' listeners
// hold never come to more than its cap.
#[test]
fn shares_and_listeners_never_exceed_the_cap() {
    for cap in [256, 512, 1024] {
        for sessions in 0..=3 {
            for connections in [0u16, 10, 100, 150, 170, 171, 200, 240, 500, 1000] {
                let mut routes = torrent_sessions(sessions);
                routes.push((
                    Consumer::Server(1),
                    route(vec![Rung::Proxy { id: 1 }], Failover::Redistribute),
                ));
                let ledger = socket_ledger(
                    &routes,
                    &profiles(&[1], &[5, 6]),
                    &HashMap::new(),
                    &HashMap::from([(1, connections)]),
                    cap,
                );
                let tunnel = &ledger[&tunnel(1)];
                let shares = tunnel.shares();
                let listening = socket_listening(tunnel.torrent_sessions);
                let total = shares.nntp + shares.torrent + shares.core + shares.http + listening;
                assert!(
                    total <= cap,
                    "{cap} {sessions} {connections}: {shares:?} and {listening} listening"
                );
                if !tunnel.over_budget() {
                    assert_eq!(total, cap - socket_linger(cap));
                }
            }
        }
    }
}

// Usenet over a WireGuard tunnel then SSH rides the one socket the SSH
// session holds in the tunnel, however many connections and servers use it;
// over SOCKS each connection opens a socket of its own.
#[test]
fn a_carrier_above_a_tunnel_takes_one_reserve_socket_for_all_its_connections() {
    let mut profiles = profiles(&[1], &[3]);
    profiles.insert(2, profile(2, ProxyKind::Ssh));
    let chain = |ids: Vec<u32>| route(vec![Rung::Chain { ids }], Failover::Redistribute);
    let ledger = |routes: &[(Consumer, Route)]| {
        socket_ledger(
            routes,
            &profiles,
            &HashMap::new(),
            &HashMap::from([(1, 50), (2, 30)]),
            256,
        )
    };

    let over_ssh = ledger(&[
        (Consumer::Server(1), chain(vec![1, 2])),
        (Consumer::Server(2), chain(vec![1, 2])),
    ]);
    let carried = &over_ssh[&tunnel(1)];
    assert_eq!((carried.nntp, carried.carriers), (0, 1));
    assert_eq!(carried.shares().core, SOCKET_CORE_BASE + 1);

    let over_socks = ledger(&[(Consumer::Server(1), chain(vec![1, 3]))]);
    let direct = &over_socks[&tunnel(1)];
    assert_eq!((direct.nntp, direct.carriers), (51, 0));
}

// Three torrent routes over three legs through one tunnel each start a
// session whose datagram sockets all bind, beside the lookups and probes
// the tunnel keeps room for, and HTTP keeps its floor.
#[test]
fn three_torrent_sessions_on_one_tunnel_all_bind_their_sockets() {
    let over = |rung| route(vec![rung], Failover::Redistribute);
    let ledger = socket_ledger(
        &[
            (
                Consumer::Torrent(TorrentScope::Default),
                over(Rung::Proxy { id: 1 }),
            ),
            (
                Consumer::Torrent(TorrentScope::Category(1)),
                over(Rung::Chain { ids: vec![1, 5] }),
            ),
            (
                Consumer::Torrent(TorrentScope::Category(2)),
                over(Rung::Chain { ids: vec![1, 6] }),
            ),
        ],
        &profiles(&[1], &[5, 6]),
        &HashMap::new(),
        &HashMap::new(),
        256,
    );
    let tunnel = &ledger[&tunnel(1)];
    assert_eq!(tunnel.torrent_sessions, 3);
    let shares = tunnel.shares();
    assert_eq!(
        shares.core,
        SOCKET_CORE_BASE + 3 * DATAGRAMS_PER_TORRENT_SESSION
    );
    assert_eq!(shares.http, socket_http_floor(256));
    assert_eq!(
        shares.nntp + shares.torrent + shares.core + shares.http,
        256 - socket_linger(256) - 3 * LISTENING_PER_TORRENT_SESSION
    );

    let sockets = weaver_tunnel::pipe::SessionSockets::new(shares);
    let core = weaver_tunnel::pipe::SocketClass::ReserveCore;
    let held: Vec<_> = (0..3 * DATAGRAMS_PER_TORRENT_SESSION + SOCKET_CORE_BASE)
        .map(|_| sockets.take(core).unwrap().unwrap())
        .collect();
    assert_eq!(held.len(), shares.core);
    assert!(!sockets.has_room(core));
}

// Two torrent routes on one leg share one session on its tunnel.
#[test]
fn two_torrent_routes_on_one_leg_share_one_session() {
    let one_leg = socket_ledger(
        &[
            (
                Consumer::Torrent(TorrentScope::Default),
                route(vec![Rung::Proxy { id: 1 }], Failover::Redistribute),
            ),
            (
                Consumer::Torrent(TorrentScope::Category(1)),
                route(vec![Rung::Proxy { id: 1 }], Failover::Redistribute),
            ),
        ],
        &profiles(&[1], &[]),
        &HashMap::new(),
        &HashMap::new(),
        256,
    );
    let shared = &one_leg[&tunnel(1)];
    assert_eq!(shared.torrent_sessions, 1);
    assert_eq!(
        shared.shares().core - SOCKET_CORE_BASE,
        DATAGRAMS_PER_TORRENT_SESSION
    );
}

// Torrent routes over a pool may land on any member, so each member's
// tunnel is charged for them; a member runs one session for every pool
// route on its egress.
#[test]
fn pool_torrent_routes_charge_each_member_one_session() {
    let pools = HashMap::from([(
        9,
        ProxyPool {
            id: 9,
            name: "pool".into(),
            kind: ProxyKind::WireGuard,
            member_ids: vec![1, 2],
            enabled: true,
        },
    )]);
    let ledger = socket_ledger(
        &[
            (
                Consumer::Torrent(TorrentScope::Default),
                route(vec![Rung::Pool { id: 9 }], Failover::Redistribute),
            ),
            (
                Consumer::Torrent(TorrentScope::Category(1)),
                route(vec![Rung::Pool { id: 9 }], Failover::Redistribute),
            ),
        ],
        &profiles(&[1, 2], &[]),
        &pools,
        &HashMap::new(),
        256,
    );
    for member in [1, 2] {
        assert_eq!(ledger[&tunnel(member)].torrent_sessions, 1);
    }
}

// Usenet and torrent peers on one tunnel: the shares add up to the cap and
// the peers keep what usenet and the reserve leave.
#[test]
fn usenet_and_torrent_peers_share_one_tunnel_within_its_cap() {
    for cap in [256, 512] {
        let ledger = socket_ledger(
            &[
                (
                    Consumer::Server(1),
                    route(vec![Rung::Proxy { id: 1 }], Failover::Redistribute),
                ),
                torrent(Rung::Proxy { id: 1 }),
            ],
            &profiles(&[1], &[]),
            &HashMap::new(),
            &HashMap::from([(1, 100)]),
            cap,
        );
        let tunnel = &ledger[&tunnel(1)];
        assert_eq!(tunnel.torrent_sessions, 1);
        assert!(!tunnel.over_budget());
        let shares = tunnel.shares();
        assert_eq!(shares.nntp, 101);
        assert_eq!(
            shares.nntp + shares.torrent + shares.core + shares.http,
            cap - cap / 16 - LISTENING_PER_TORRENT_SESSION,
            "everything but the lingering allowance and the listeners is handed out"
        );
        assert!(shares.torrent >= SOCKET_PEER_FLOOR);
    }
}

// Usenet that asks more than a tunnel holds is refused with the totals; a
// torrent route on it keeps its floor. A tunnel over budget at run time,
// as a lowered hardware profile leaves one, holds usenet to what the
// reserve and the floor leave.
#[test]
fn usenet_over_a_tunnel_is_refused_with_its_totals() {
    let routes = |torrents: bool| {
        let mut routes = vec![(
            Consumer::Server(1),
            route(vec![Rung::Proxy { id: 3 }], Failover::Redistribute),
        )];
        if torrents {
            routes.push(torrent(Rung::Proxy { id: 3 }));
        }
        routes
    };
    let ledger = |torrents, connections, cap| {
        socket_ledger(
            &routes(torrents),
            &profiles(&[3], &[]),
            &HashMap::new(),
            &HashMap::from([(1, connections)]),
            cap,
        )
    };
    let check = |torrents, connections, cap| {
        validate_socket_raise(&HashMap::new(), ledger(torrents, connections, cap))
    };

    assert_eq!(
        check(false, 240, 256),
        Err(NetworkRefusal::WireGuardSocketBudget {
            proxy: 3,
            egress: 0,
            nntp: 241,
            reserve: 32,
            peers: 0,
            cap: 256,
        })
    );
    assert_eq!(check(false, 223, 256), Ok(()));
    assert_eq!(
        check(true, 223, 256),
        Err(NetworkRefusal::WireGuardSocketBudget {
            proxy: 3,
            egress: 0,
            nntp: 224,
            reserve: 69,
            peers: SOCKET_PEER_FLOOR,
            cap: 256,
        })
    );
    // 171 usenet sockets, the 69 reserved and the 16 peers fill the 256.
    assert_eq!(check(true, 170, 256), Ok(()));
    assert!(check(true, 171, 256).is_err());
    assert_eq!(check(true, 240, 512), Ok(()));

    // Against what the servers asked before, only a raise on a tunnel left
    // over budget is refused.
    let asked = |connections| socket_asks(&ledger(true, connections, 256));
    assert!(validate_socket_raise(&asked(240), ledger(true, 240, 256)).is_ok());
    assert!(validate_socket_raise(&asked(240), ledger(true, 230, 256)).is_ok());
    assert!(validate_socket_raise(&asked(168), ledger(true, 170, 256)).is_ok());
    assert_eq!(
        validate_socket_raise(&asked(240), ledger(true, 241, 256)),
        check(true, 241, 256)
    );
    assert!(validate_socket_raise(&asked(223), ledger(true, 223, 256)).is_ok());

    let shares = ledger(true, 240, 256)[&tunnel(3)].shares();
    assert_eq!(shares.nntp, 256 - 69 - SOCKET_PEER_FLOOR);
    assert_eq!(shares.torrent, SOCKET_PEER_FLOOR);
}

// Every stored route key names its consumer again.
#[test]
fn a_route_key_names_its_consumer() {
    for consumer in [
        Consumer::Outbound,
        Consumer::Server(7),
        Consumer::Rss(3),
        Consumer::Torrent(TorrentScope::Default),
        Consumer::Torrent(TorrentScope::Category(4)),
        Consumer::Torrent(TorrentScope::TrackerRule(5)),
    ] {
        assert_eq!(Consumer::from_key(&consumer.key()), Some(consumer));
    }
    assert_eq!(Consumer::from_key("server:x"), None);
}

fn saved_wireguard(id: u32) -> ProxyProfile {
    use base64::Engine;
    let key = |seed: u8| {
        base64::engine::general_purpose::STANDARD
            .encode(weaver_tunnel::test_support::test_key(seed))
    };
    ProxyProfile {
        dns_servers: vec!["10.64.0.1".parse().unwrap()],
        tunnel_addresses: vec!["10.64.0.2/32".into()],
        peer_public_key: Some(key(2)),
        secrets: crate::proxies::ProxySecrets {
            private_key: Some(key(1)),
            ..Default::default()
        },
        ..profile(id, ProxyKind::WireGuard)
    }
}

fn server(id: u32, connections: u16, active: bool) -> crate::servers::ServerConfig {
    crate::servers::ServerConfig {
        id,
        host: "provider.invalid".into(),
        port: 119,
        tls: false,
        username: None,
        password: None,
        connections,
        active,
        supports_pipelining: true,
        pipelining_depth: None,
        tls_name_mismatch_certificate_der: None,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: Default::default(),
        tls_ca_cert: None,
    }
}

fn refused_code(result: Result<(), crate::StateError>) -> Option<&'static str> {
    result
        .err()
        .and_then(|error| error.refusal().map(|refusal| refusal.code))
}

// A server save that would put more usenet on a WireGuard tunnel than it
// holds is refused and changes nothing: adding the server, editing its
// connections, and turning it on.
#[test]
fn usenet_overflow_is_refused_at_every_server_save() {
    let db = crate::Database::open_in_memory().unwrap();
    db.save_proxy_profile(&saved_wireguard(3)).unwrap();
    let through_tunnel = RoutingPolicy {
        legs: vec![leg(Rung::Proxy { id: 3 }, 100)],
        ..Default::default()
    };
    const CODE: Option<&str> = Some("WIREGUARD_SOCKET_BUDGET");

    assert_eq!(
        refused_code(db.insert_server_with_routing(&server(1, 240, true), Some(&through_tunnel))),
        CODE
    );
    assert!(db.list_servers().unwrap().is_empty());
    db.insert_server_with_routing(&server(1, 200, true), Some(&through_tunnel))
        .unwrap();

    assert_eq!(refused_code(db.update_server(&server(1, 240, true))), CODE);
    assert_eq!(db.list_servers().unwrap()[0].connections, 200);

    // An inactive server asks nothing until it is turned on.
    db.insert_server_with_routing(&server(2, 100, false), Some(&through_tunnel))
        .unwrap();
    assert_eq!(refused_code(db.set_server_active(2, true)), CODE);
    assert!(
        !db.list_servers()
            .unwrap()
            .iter()
            .any(|server| server.id == 2 && server.active)
    );
    db.set_server_active(1, false).unwrap();
    db.set_server_active(2, true).unwrap();

    // A rollback restores the activation a failed change replaced, and is
    // never budgeted again.
    assert_eq!(refused_code(db.set_server_active(1, true)), CODE);
    db.restore_server_active(1, true).unwrap();
    assert!(
        db.list_servers()
            .unwrap()
            .iter()
            .all(|server| server.active)
    );
}

// A tunnel over budget, as a torrent route added to a full one leaves it,
// refuses only a save that raises what usenet asks of it. A feed's route
// through it, an edit of its proxy, and a server that never reaches it all
// save, and so does lowering the server that fills it.
#[test]
fn a_tunnel_over_budget_refuses_only_a_raise_of_its_usenet() {
    let db = crate::Database::open_in_memory().unwrap();
    db.save_proxy_profile(&saved_wireguard(3)).unwrap();
    let through_tunnel = RoutingPolicy {
        legs: vec![leg(Rung::Proxy { id: 3 }, 100)],
        ..Default::default()
    };
    // 224 usenet sockets and the 32 reserved fill the 256.
    db.insert_server_with_routing(&server(1, 223, true), Some(&through_tunnel))
        .unwrap();
    db.save_proxy_routing_policy(Consumer::Torrent(TorrentScope::Default), &through_tunnel)
        .unwrap();

    db.save_proxy_routing_policy(Consumer::Rss(7), &through_tunnel)
        .unwrap();
    let mut edited = saved_wireguard(3);
    edited.revision = 2;
    edited.mtu = 1380;
    db.save_proxy_profile(&edited).unwrap();
    db.insert_server(&server(2, 50, true)).unwrap();
    db.update_server(&server(2, 60, true)).unwrap();

    assert_eq!(
        refused_code(db.update_server(&server(1, 224, true))),
        Some("WIREGUARD_SOCKET_BUDGET")
    );
    db.update_server(&server(1, 220, true)).unwrap();
}
