// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;

use super::*;
use crate::proxies::{Resolver, RouteLeg, RoutingPolicy, TorrentScope};
use weaver_tunnel::pipe::{SocketDivider, overhead_bytes, stream_bytes};

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

const MIB: u64 = 1024 * 1024;

// What a tunnel keeps for itself and its torrent sessions, worked through
// once at an MTU of 1420 with usenet's receive buffer before any profile
// (1 MiB): the overhead is 12.8 MiB, the reserve twelve sockets at the
// tunnel's own 2 MiB, and each torrent session 96.8 MiB, its two listeners'
// backlogs of sixteen sockets and its five datagram sockets. A usenet
// connection is 1092 KiB, so 449 fit beside the reserve, 358 beside one
// torrent session too, and 267 beside two.
#[test]
fn a_tunnel_keeps_its_overhead_reserve_and_torrent_sessions() {
    assert_eq!(overhead_bytes(1420), 13_381_632);
    assert_eq!(stream_bytes(None), 2 * MIB + 4096);
    assert_eq!(torrent_session_bytes(1420), 101_451_776);
    assert_eq!(usenet_socket_bytes(), MIB + 68 * 1024);
    for (sessions, fit) in [(0, 449), (1, 358), (2, 267)] {
        let mut routes = torrent_sessions(sessions);
        routes.push((
            Consumer::Server(1),
            route(vec![Rung::Proxy { id: 1 }], Failover::Redistribute),
        ));
        let ledger = socket_ledger(
            &routes,
            &profiles(&[1], &[5, 6]),
            &HashMap::new(),
            &HashMap::from([(1, 1000)]),
        );
        let tunnel = &ledger[&tunnel(1)];
        assert_eq!(tunnel.torrent_sessions, sessions);
        assert_eq!(tunnel.reserve(), 12 * stream_bytes(None));
        assert_eq!(
            tunnel.kept(),
            overhead_bytes(1420) + tunnel.reserve() + sessions as u64 * torrent_session_bytes(1420)
        );
        assert_eq!(tunnel.nntp_held(), fit, "{sessions}");
        let budget = tunnel.budget();
        assert_eq!(budget.bytes, WIREGUARD_INSTANCE_BYTES);
        assert_eq!(budget.reserve, tunnel.reserve());
        assert_eq!(budget.mtu, 1420);
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
        )
    };

    let over_ssh = ledger(&[
        (Consumer::Server(1), chain(vec![1, 2])),
        (Consumer::Server(2), chain(vec![1, 2])),
    ]);
    let carried = &over_ssh[&tunnel(1)];
    assert_eq!((carried.nntp, carried.carriers), (0, 1));
    // The carrier is a datagram socket in the tunnel, and the reserve is
    // charged what the tunnel takes for it, not a stream's bytes.
    assert_eq!(
        carried.reserve(),
        (SOCKET_CORE_BASE + SOCKET_HTTP_FLOOR) as u64 * stream_bytes(None)
            + weaver_tunnel::pipe::datagram_bytes(1420)
    );
    // The floor still fits beside it, with room left for usenet.
    assert!(carried.kept() + usenet_socket_bytes() <= WIREGUARD_INSTANCE_BYTES);
    // As many carriers as one tunnel has servers still leave the floor room.
    let carriers = TunnelSockets {
        carriers: 32,
        ..TunnelSockets::new(0, 1, 1420)
    };
    assert_eq!(
        carriers.reserve(),
        (SOCKET_CORE_BASE + SOCKET_HTTP_FLOOR) as u64 * stream_bytes(None)
            + 32 * weaver_tunnel::pipe::datagram_bytes(1420)
    );
    assert!(carriers.kept() <= WIREGUARD_INSTANCE_BYTES);

    let over_socks = ledger(&[(Consumer::Server(1), chain(vec![1, 3]))]);
    let direct = &over_socks[&tunnel(1)];
    assert_eq!((direct.nntp, direct.carriers), (51, 0));
}

// Three torrent routes over three legs through one tunnel each start a
// session, and the tunnel keeps the bytes of all three.
#[test]
fn three_torrent_sessions_on_one_tunnel_are_all_kept() {
    let ledger = socket_ledger(
        &torrent_sessions(3),
        &profiles(&[1], &[5, 6]),
        &HashMap::new(),
        &HashMap::new(),
    );
    let tunnel = &ledger[&tunnel(1)];
    assert_eq!(tunnel.torrent_sessions, 3);
    assert_eq!(
        tunnel.kept(),
        overhead_bytes(1420) + tunnel.reserve() + 3 * torrent_session_bytes(1420)
    );
    assert!(!tunnel.over_budget());
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
    );
    assert_eq!(one_leg[&tunnel(1)].torrent_sessions, 1);
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
    );
    for member in [1, 2] {
        assert_eq!(ledger[&tunnel(member)].torrent_sessions, 1);
    }
}

// Usenet that asks more than a tunnel holds beside what it keeps is refused
// with the totals in bytes. A torrent route on the tunnel keeps its
// session's bytes, and leaves usenet less.
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
    let ledger = |torrents, connections| {
        socket_ledger(
            &routes(torrents),
            &profiles(&[3], &[]),
            &HashMap::new(),
            &HashMap::from([(1, connections)]),
        )
    };
    let check = |torrents, connections| {
        validate_socket_raise(&HashMap::new(), ledger(torrents, connections))
    };

    // 449 usenet connections, one of them the replacement, fill the tunnel.
    assert_eq!(check(false, 448), Ok(()));
    assert_eq!(
        check(false, 449),
        Err(NetworkRefusal::WireGuardSocketBudget {
            proxy: 3,
            egress: 0,
            nntp: 450,
            nntp_bytes: 450 * usenet_socket_bytes(),
            kept_bytes: overhead_bytes(1420) + 12 * stream_bytes(None),
            limit_bytes: 516 * MIB,
        })
    );
    assert_eq!(
        check(false, 449).unwrap_err().to_string(),
        "usenet servers ask 450 connections of WireGuard proxy 3 on egress 0, 480 MiB, and the tunnel keeps 37 MiB for itself, over the 516 MiB it holds"
    );
    // Beside a torrent session, 358.
    assert_eq!(check(true, 357), Ok(()));
    assert!(check(true, 358).is_err());

    // Against what the servers asked before, only a raise on a tunnel left
    // over budget is refused.
    let asked = |connections| socket_asks(&ledger(true, connections));
    assert!(validate_socket_raise(&asked(400), ledger(true, 400)).is_ok());
    assert!(validate_socket_raise(&asked(400), ledger(true, 380)).is_ok());
    assert!(validate_socket_raise(&asked(350), ledger(true, 357)).is_ok());
    assert_eq!(
        validate_socket_raise(&asked(400), ledger(true, 401)),
        check(true, 401)
    );

    // Over budget, usenet is held to what fits.
    assert_eq!(ledger(true, 400)[&tunnel(3)].nntp_held(), 358);
}

// The tunnel's sides are the job classes, in the protocol table's order.
#[test]
fn the_tunnel_sides_are_the_job_classes() {
    use crate::jobs::Protocol;
    use weaver_tunnel::pipe::{SIDE_CLASSES, SocketClass};
    assert_eq!(SIDE_CLASSES.len(), Protocol::COUNT);
    assert_eq!(SIDE_CLASSES[Protocol::Nzb.index()], SocketClass::Nntp);
    assert_eq!(
        SIDE_CLASSES[Protocol::Torrent.index()],
        SocketClass::Torrent
    );
}

// The bytes divide as the download limit does: the same proportions for the
// same inputs, both pressing at 50/50 and at 70/30, and one alone.
#[test]
fn bytes_divide_in_the_proportions_the_bandwidth_split_gives() {
    use crate::bandwidth::{BandwidthSplit, SplitRates, SplitUse, split_rates};
    let cap = 400 * MIB;
    for (split, used) in [
        (None, [cap, cap]),
        (Some(70), [cap, cap]),
        (Some(70), [cap, 0]),
        (Some(70), [0, cap]),
        (None, [cap / 10, cap]),
    ] {
        let split = split.map(|usenet_pct| BandwidthSplit { usenet_pct });
        let divider = SplitDivider::Fixed(split);
        let shares = divider.shares(cap);
        let bytes = divider.share_out(cap, &shares, &[None, None], &used);
        let rates = split_rates(
            Some(cap),
            split,
            SplitRates::default(),
            SplitUse {
                usenet: used[0],
                torrent: used[1],
            },
        );
        assert_eq!(
            bytes,
            [rates.usenet.unwrap(), rates.torrent.unwrap()],
            "{split:?} {used:?}"
        );
    }
}

// The sides as the bandwidth split's own rule divides them, at `usenet_pct`
// (none for the default), on a tunnel whose reserve holds 4 peer sockets and
// whose sides hold `sides`. The split is given, not read from the one in
// force, so no test depends on another.
fn rule_sockets(usenet_pct: Option<u8>, sides: u64) -> weaver_tunnel::pipe::SessionSockets {
    let peer = stream_bytes(None);
    weaver_tunnel::pipe::SessionSockets::new(
        weaver_tunnel::pipe::SocketBudget {
            bytes: overhead_bytes(1420) + 4 * peer + sides * peer,
            reserve: 4 * peer,
            torrent_kept: 0,
            mtu: 1420,
        },
        std::sync::Arc::new(fixed_split(usenet_pct)),
    )
}

fn fixed_split(usenet_pct: Option<u8>) -> SplitDivider {
    SplitDivider::Fixed(
        usenet_pct.map(|usenet_pct| crate::bandwidth::BandwidthSplit { usenet_pct }),
    )
}

// What each side owes now, usenet first.
fn owed(sockets: &weaver_tunnel::pipe::SessionSockets) -> [u64; 2] {
    use weaver_tunnel::pipe::SocketClass;
    [
        sockets.owed(SocketClass::Nntp),
        sockets.owed(SocketClass::Torrent),
    ]
}

fn take_peer(
    sockets: &weaver_tunnel::pipe::SessionSockets,
    class: weaver_tunnel::pipe::SocketClass,
) -> Option<weaver_tunnel::pipe::SocketPermit> {
    sockets.take(class, stream_bytes(None)).ok().flatten()
}

fn fill_peers(
    sockets: &weaver_tunnel::pipe::SessionSockets,
    class: weaver_tunnel::pipe::SocketClass,
) -> Vec<weaver_tunnel::pipe::SocketPermit> {
    std::iter::from_fn(|| take_peer(sockets, class)).collect()
}

// At 70/30, torrents alone fill a tunnel; usenet presses, and torrents are
// asked for exactly their bytes past 30 percent, once.
#[test]
fn a_70_30_split_gives_back_to_its_proportions() {
    use weaver_tunnel::pipe::SocketClass;
    let peer = stream_bytes(None);
    let sockets = rule_sockets(Some(70), 100);
    let peers = fill_peers(&sockets, SocketClass::Torrent);
    // A lone side holds all but the idle side's probe.
    assert_eq!(peers.len(), 99);
    assert_eq!(sockets.woken(), [false, false]);
    let pressing = sockets.take(SocketClass::Nntp, 2 * peer);
    assert!(pressing.is_err());
    let given = sockets.given(SocketClass::Torrent);
    assert_eq!(given, 30 * peer);
    assert_eq!(sockets.given(SocketClass::Nntp), 70 * peer);
    assert_eq!(owed(&sockets), [0, 99 * peer - given]);
    assert_eq!(sockets.woken(), [false, true]);
    // The torrent hook drops one peer for each socket's worth owed.
    assert_eq!((99 * peer - given).div_ceil(peer), 69);
    assert!(sockets.take(SocketClass::Nntp, 2 * peer).is_err());
    assert_eq!(sockets.woken(), [false, false], "woken twice");
    assert_eq!(owed(&sockets), [0, 99 * peer - given]);
    drop(pressing);
}

// Torrents alone fill the tunnel; usenet presses, and torrents are asked
// once for exactly their bytes past their share. What they close lingers and
// counts toward the ask, so usenet asking again asks nothing more, and once
// the closed streams are back usenet has its share.
#[tokio::test(start_paused = true)]
async fn a_class_over_its_share_gives_back_once_another_presses() {
    use weaver_tunnel::pipe::{SocketClass, TUNNEL_LINGER};
    let peer = stream_bytes(None);
    let sockets = rule_sockets(None, 100);
    let mut peers = fill_peers(&sockets, SocketClass::Torrent);
    assert_eq!(peers.len(), 99, "torrents alone take all but the probe");
    assert_eq!(sockets.woken(), [false, false], "nobody pressed");

    let Err(weaver_tunnel::pipe::Refused {
        freed,
        demand: _usenet,
        ..
    }) = sockets.take(SocketClass::Nntp, 2 * peer)
    else {
        panic!("usenet took bytes torrents hold");
    };
    assert_eq!(owed(&sockets), [0, 49 * peer]);
    assert_eq!(sockets.woken(), [false, true]);
    assert_eq!(sockets.over_share(SocketClass::Torrent), 49 * peer);

    // Torrents close their 49 lowest peers: streams, so they linger.
    for peer in peers.drain(..49) {
        drop(peer.lingering());
    }
    assert_eq!(sockets.over_share(SocketClass::Torrent), 0);
    assert!(
        sockets.take(SocketClass::Nntp, 2 * peer).is_err(),
        "the closed peers still linger"
    );
    assert_eq!(owed(&sockets), [0, 0], "owed while lingering");
    assert_eq!(sockets.woken(), [false, false], "woken while lingering");

    let woken = freed.notify();
    let woken = woken.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    tokio::time::advance(TUNNEL_LINGER).await;
    woken.await;
    let usenet = fill_peers(&sockets, SocketClass::Nntp);
    assert_eq!(usenet.len(), 50, "usenet has its share");
    assert_eq!(owed(&sockets), [0, 0], "owed at its share");
    assert_eq!(sockets.woken(), [false, false], "woken at its share");
    drop(freed);
}

// The reverse: usenet alone fills the tunnel, torrents press, and usenet is
// asked for its bytes past its share. Usenet alone may hold every byte, so a
// server saved to fill the tunnel is held near its share while torrents
// press, never refused.
#[test]
fn usenet_over_its_share_gives_back_when_torrents_press() {
    use weaver_tunnel::pipe::SocketClass;
    let peer = stream_bytes(None);
    let sockets = rule_sockets(None, 100);
    let connections = fill_peers(&sockets, SocketClass::Nntp);
    assert_eq!(connections.len(), 99);
    // One peer fits in the idle side's probe; two press.
    let probe = take_peer(&sockets, SocketClass::Torrent);
    assert!(probe.is_some());
    let pressing = sockets.take(SocketClass::Torrent, peer);
    assert!(pressing.is_err());
    // Torrents hold one of their 50, so usenet owes 49 of its 99.
    assert_eq!(owed(&sockets), [49 * peer, 0]);
    assert_eq!(sockets.woken(), [true, false]);
}

// A class holding past its share is never asked while no other presses:
// its own refused hand-outs, a smaller budget that still holds it, and its
// closes ask nothing.
#[test]
fn nobody_pressing_asks_no_give_back() {
    use weaver_tunnel::pipe::{SocketBudget, SocketClass};
    let peer = stream_bytes(None);
    let sockets = rule_sockets(None, 100);
    let mut peers = fill_peers(&sockets, SocketClass::Torrent);
    for _ in 0..3 {
        assert!(sockets.take(SocketClass::Torrent, peer).is_err());
    }
    sockets.set_budget(SocketBudget {
        bytes: overhead_bytes(1420) + 4 * peer + 99 * peer,
        reserve: 4 * peer,
        torrent_kept: 0,
        mtu: 1420,
    });
    assert!(sockets.take(SocketClass::Torrent, peer).is_err());
    drop(peers.pop());
    assert_eq!(owed(&sockets), [0, 0]);
    assert_eq!(sockets.woken(), [false, false]);
}

// A side stops pressing once its refused hand-outs are gone, not only on
// its next take: usenet's dial refused and its waiter dropped, the ask on
// torrents stands no more, and no give-back is asked on its behalf again.
#[test]
fn a_dropped_waiter_stops_its_side_pressing() {
    use weaver_tunnel::pipe::SocketClass;
    let peer = stream_bytes(None);
    let sockets = rule_sockets(None, 100);
    let mut peers = fill_peers(&sockets, SocketClass::Torrent);
    let refused = sockets.take(SocketClass::Nntp, 2 * peer);
    assert!(refused.is_err());
    assert!(sockets.pressing(SocketClass::Nntp));
    assert!(sockets.owes(SocketClass::Torrent));
    assert_eq!(owed(&sockets), [0, 49 * peer]);
    assert_eq!(sockets.woken(), [false, true]);

    drop(refused);
    assert!(!sockets.pressing(SocketClass::Nntp));
    assert!(
        !sockets.owes(SocketClass::Torrent),
        "the ask stands no more"
    );
    drop(peers.pop());
    assert_eq!(sockets.woken(), [false, false]);
    // Torrents take back what usenet no longer wants.
    peers.extend(fill_peers(&sockets, SocketClass::Torrent));
    assert_eq!(peers.len(), 99);
    assert_eq!(sockets.woken(), [false, false]);
    assert_eq!(owed(&sockets), [0, 0]);
}

// A side alone grows to what the rule gives a lone side through looks at
// its room only, as the torrent dialer looks before it dials TCP.
#[test]
fn a_side_alone_grows_through_looks_at_its_room() {
    use weaver_tunnel::pipe::SocketClass;
    let peer = stream_bytes(None);
    let sockets = rule_sockets(None, 100);
    let mut peers = Vec::new();
    while sockets.has_room(SocketClass::Torrent, peer) {
        peers.push(take_peer(&sockets, SocketClass::Torrent).expect("the room it had"));
    }
    assert_eq!(peers.len(), 99, "all but the idle side's probe");
    assert_eq!(sockets.woken(), [false, false]);
}

// Torrents take 40 peers first, then usenet and torrents each take what
// their looks give them until the sides hold every byte: usenet past half,
// torrents below it, as a tunnel's sides are after usenet grew alone.
fn usenet_past_half(
    sockets: &weaver_tunnel::pipe::SessionSockets,
) -> (
    Vec<weaver_tunnel::pipe::SocketPermit>,
    Vec<weaver_tunnel::pipe::SocketPermit>,
) {
    use weaver_tunnel::pipe::SocketClass;
    let peer = stream_bytes(None);
    let mut peers: Vec<_> = (0..40)
        .map(|_| take_peer(sockets, SocketClass::Torrent).expect("a lone side's room"))
        .collect();
    let mut connections = Vec::new();
    loop {
        let held = peers.len() + connections.len();
        while sockets.has_room(SocketClass::Nntp, peer) {
            connections.push(take_peer(sockets, SocketClass::Nntp).expect("the room it had"));
        }
        while sockets.has_room(SocketClass::Torrent, peer) {
            peers.push(take_peer(sockets, SocketClass::Torrent).expect("the room it had"));
        }
        if peers.len() + connections.len() == held {
            break;
        }
    }
    (peers, connections)
}

// A look at a side's room may share out again what each side is given, but
// never asks a side to give back: with usenet holding past half and torrents
// below theirs, any number of torrent looks leave usenet owing nothing, so
// no busy connection is marked and no idle one closes.
#[test]
fn a_look_alone_never_raises_an_ask() {
    use weaver_tunnel::pipe::SocketClass;
    let sockets = rule_sockets(None, 190);
    let (_peers, connections) = usenet_past_half(&sockets);
    assert!(connections.len() > 95);
    assert_eq!(sockets.woken(), [false, false]);
    for _ in 0..3 {
        assert!(!sockets.has_room(SocketClass::Torrent, stream_bytes(None)));
        assert_eq!(sockets.woken(), [false, false], "a look woke usenet");
        assert!(!sockets.owes(SocketClass::Nntp), "an ask stands for a look");
    }
}

// The reviewed case, in peer sockets: the sides hold 190 at 50/50, torrents
// hold 40 and usenet the rest. Torrents looking alone ask nothing. A torrent
// TCP dial refused only because usenet holds past what it is given waits for
// the room: while its signal is held the ask on usenet stands, usenet closes
// what is asked, and once those bytes come back torrents grow to their share
// and usenet keeps its own.
#[tokio::test(start_paused = true)]
async fn a_waiting_torrent_dial_holds_the_ask_until_both_sides_hold_their_share() {
    use weaver_tunnel::pipe::{SocketClass, TUNNEL_LINGER};
    let peer = stream_bytes(None);
    let sockets = rule_sockets(None, 190);
    let (mut peers, mut connections) = usenet_past_half(&sockets);
    let usenet = connections.len() as u64;
    assert!(usenet > 95);
    assert!(!sockets.has_room(SocketClass::Torrent, peer));
    assert_eq!(
        sockets.woken(),
        [false, false],
        "no churn before a waiter exists"
    );

    // The dial waits: usenet is asked for what it holds past its half.
    let waiting = sockets
        .wait_room(SocketClass::Torrent, peer)
        .expect_err("usenet holds the room");
    assert!(sockets.pressing(SocketClass::Torrent));
    assert_eq!(sockets.woken(), [true, false]);
    let asked = sockets.owed(SocketClass::Nntp);
    assert_eq!(asked, usenet * peer - sockets.given(SocketClass::Nntp));
    assert_eq!(sockets.given(SocketClass::Nntp), 95 * peer);
    assert!(
        sockets.owes(SocketClass::Nntp),
        "the ask stands while it waits"
    );

    // Usenet closes what is asked, and the ask stands until it has.
    let closing = usize::try_from(asked.div_ceil(peer)).unwrap();
    for connection in connections.drain(..closing - 1) {
        drop(connection.lingering());
    }
    assert!(sockets.owes(SocketClass::Nntp), "one more is owed");
    drop(connections.pop().unwrap().lingering());
    assert!(!sockets.owes(SocketClass::Nntp));

    let woken = waiting.freed.notify();
    let woken = woken.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    tokio::time::advance(TUNNEL_LINGER).await;
    woken.await;
    while sockets.has_room(SocketClass::Torrent, peer) {
        peers.push(take_peer(&sockets, SocketClass::Torrent).expect("the room it had"));
    }
    drop(waiting);
    assert_eq!(
        sockets.held(SocketClass::Torrent),
        95 * peer,
        "torrents at their share"
    );
    assert_eq!(
        sockets.held(SocketClass::Nntp),
        95 * peer,
        "usenet at its share"
    );
    assert_eq!(owed(&sockets), [0, 0]);
    assert_eq!(sockets.woken(), [false, false]);
}

// Closes what the give-back hook of `class` would close: one socket at a
// time from the newest, each only while the ask on `class` still stands, as
// a give-back reads `owes` before it closes. Returns how many it closed.
fn give_back(
    sockets: &weaver_tunnel::pipe::SessionSockets,
    class: weaver_tunnel::pipe::SocketClass,
    held: &mut Vec<weaver_tunnel::pipe::SocketPermit>,
) -> usize {
    let mut closed = 0;
    while sockets.owes(class) {
        let Some(permit) = held.pop() else {
            break;
        };
        drop(permit.lingering());
        closed += 1;
    }
    closed
}

// A hand-out of one peer socket of `class` that must be refused.
fn refused(
    sockets: &weaver_tunnel::pipe::SessionSockets,
    class: weaver_tunnel::pipe::SocketClass,
) -> weaver_tunnel::pipe::Refused {
    match sockets.take(class, stream_bytes(None)) {
        Err(refused) => refused,
        Ok(_) => panic!("a full side was handed a socket"),
    }
}

// Bytes of `class` coming back after they linger: waits on the slot that
// rings as they do, with the paused clock moved past the linger.
async fn lingered_back(freed: std::sync::Arc<tokio::sync::Notify>) {
    let back = freed.notified();
    tokio::pin!(back);
    back.as_mut().enable();
    tokio::time::advance(weaver_tunnel::pipe::TUNNEL_LINGER).await;
    back.await;
}

// Takes peer sockets of `class` until one is refused, as a side wanting
// every socket it can get dials: the refusal is returned with its demand.
fn take_until_refused(
    sockets: &weaver_tunnel::pipe::SessionSockets,
    class: weaver_tunnel::pipe::SocketClass,
    held: &mut Vec<weaver_tunnel::pipe::SocketPermit>,
) -> weaver_tunnel::pipe::Refused {
    loop {
        match sockets.take(class, stream_bytes(None)) {
            Ok(permit) => held.push(permit.expect("a session with a budget")),
            Err(refused) => return refused,
        }
    }
}

// The reviewed usenet case, in peer sockets: at 50/50 the sides hold 190,
// usenet holds 40 and torrents grew while it did no more. Usenet then dials
// for more and a dial is refused: the lane parks as capacity and holds the
// refusal's demand through its paced requeue, so the ask on torrents stands
// that long, torrents give back what they hold past their half, and usenet
// grows into it. Both end at their share, and nothing more is asked.
#[tokio::test(start_paused = true)]
async fn a_parked_usenet_lane_holds_its_press_until_both_sides_hold_their_share() {
    use weaver_tunnel::pipe::SocketClass;
    let peer = stream_bytes(None);
    let sockets = rule_sockets(None, 190);
    let mut connections: Vec<_> = (0..40)
        .map(|_| take_peer(&sockets, SocketClass::Nntp).expect("a lone side's room"))
        .collect();
    let mut peers = Vec::new();
    while sockets.has_room(SocketClass::Torrent, peer) {
        peers.push(take_peer(&sockets, SocketClass::Torrent).expect("the room it had"));
    }
    // Usenet not pressing keeps what it holds and a fifth more.
    assert_eq!(peers.len(), 190 - 48);
    assert_eq!(sockets.woken(), [false, false], "nobody pressed");

    // The refused dial: the lane parks, holding the demand it was handed.
    let parked = take_until_refused(&sockets, SocketClass::Nntp, &mut connections);
    assert_eq!(connections.len(), 48);
    let demand = parked.demand.expect("a side refused presses");
    assert_eq!(sockets.woken(), [false, true]);
    let asked = sockets.owed(SocketClass::Torrent);
    assert_eq!(
        asked,
        (peers.len() as u64 - 95) * peer,
        "torrents are asked"
    );
    // The refusal itself is gone; the parked lane's demand is what keeps
    // the ask standing until torrents have given it.
    assert!(sockets.pressing(SocketClass::Nntp));
    assert!(sockets.owes(SocketClass::Torrent), "the ask lapsed");
    assert_eq!(
        give_back(&sockets, SocketClass::Torrent, &mut peers),
        142 - 95
    );
    lingered_back(parked.freed.notify()).await;

    // The lane is fed: it dials again and takes what it was owed.
    while let Some(connection) = take_peer(&sockets, SocketClass::Nntp) {
        connections.push(connection);
    }
    drop(demand);
    assert!(!sockets.pressing(SocketClass::Nntp));
    assert_eq!(connections.len(), 95, "usenet at its share");
    assert_eq!(peers.len(), 95, "torrents at their share");
    assert_eq!(owed(&sockets), [0, 0], "owed at the shares");
    assert_eq!(sockets.woken(), [false, false], "woken at the shares");
}

// At 70/30, usenet alone holds all but the idle side's probe. A torrent dial
// crowded out holds its demand; usenet gives back to its 70 percent and
// torrents grow to their 30. Then the torrents complete and keep 5 peers,
// wanting no more TCP dials, so their demand is dropped: torrents press no
// more, nothing stands on usenet, and usenet grows to every byte torrents do
// not keep, as a side alone may.
#[tokio::test(start_paused = true)]
async fn usenet_grows_back_once_torrents_stop_wanting_tcp() {
    use weaver_tunnel::pipe::SocketClass;
    let peer = stream_bytes(None);
    let sockets = rule_sockets(Some(70), 190);
    let mut connections = Vec::new();
    while sockets.has_room(SocketClass::Nntp, peer) {
        connections.push(take_peer(&sockets, SocketClass::Nntp).expect("the room it had"));
    }
    let mut peers = Vec::new();
    while sockets.has_room(SocketClass::Torrent, peer) {
        peers.push(take_peer(&sockets, SocketClass::Torrent).expect("the probe"));
    }
    let alone = connections.len();
    assert!(!peers.is_empty(), "the idle side's probe");

    let crowded = sockets
        .wait_room(SocketClass::Torrent, peer)
        .expect_err("usenet holds the room");
    let want = crowded
        .demand
        .expect("crowded out, the dial wants the room");
    assert!(sockets.pressing(SocketClass::Torrent));
    assert_eq!(sockets.given(SocketClass::Nntp), 133 * peer);
    assert_eq!(owed(&sockets), [(alone as u64 - 133) * peer, 0]);
    assert_eq!(sockets.woken(), [true, false]);
    assert_eq!(
        give_back(&sockets, SocketClass::Nntp, &mut connections),
        alone - 133
    );
    lingered_back(crowded.freed.notify()).await;
    while sockets.has_room(SocketClass::Torrent, peer) {
        peers.push(take_peer(&sockets, SocketClass::Torrent).expect("the room it had"));
    }
    assert_eq!(peers.len(), 57, "torrents at their 30 percent");
    assert_eq!(connections.len(), 133, "usenet at its 70 percent");

    // The torrents complete: 5 peers stay to seed, and no TCP dial is
    // wanted, so the want held for one goes.
    peers.truncate(5);
    drop(want);
    assert!(!sockets.pressing(SocketClass::Torrent));
    assert!(!sockets.owes(SocketClass::Nntp));
    while sockets.has_room(SocketClass::Nntp, peer) {
        connections.push(take_peer(&sockets, SocketClass::Nntp).expect("the room it had"));
    }
    // Torrents not pressing keep what they hold and a fifth more.
    assert_eq!(
        connections.len(),
        190 - 6,
        "usenet holds every byte torrents do not keep"
    );
    assert_eq!(owed(&sockets), [0, 0], "nothing owed by usenet");
    assert_eq!(sockets.woken(), [false, false], "usenet woken");
    assert_eq!(give_back(&sockets, SocketClass::Nntp, &mut connections), 0);
}

// A side woken to give back that finds nothing to give is marked dry, and is
// not woken again each time the other side's press lapses and returns, as
// each paced redial of a parked lane does. It is woken again once its own
// holdings change and the press returns: what it owes is still the whole
// level, never only what grew since.
#[test]
fn a_dry_side_is_woken_once_until_its_holdings_change() {
    use weaver_tunnel::pipe::SocketClass;
    let peer = stream_bytes(None);
    let sockets = rule_sockets(Some(70), 190);
    let mut peers = fill_peers(&sockets, SocketClass::Torrent);
    let _probe = fill_peers(&sockets, SocketClass::Nntp);
    let alone = peers.len() as u64;

    let first = refused(&sockets, SocketClass::Nntp);
    assert_eq!(sockets.woken(), [false, true], "woken once");
    assert_eq!(owed(&sockets), [0, (alone - 57) * peer]);
    // The answer finds nothing it can close.
    sockets.dry(SocketClass::Torrent);
    drop(first);
    for _ in 0..4 {
        // The press lapses, a torrent look runs, and usenet presses again.
        assert!(!sockets.pressing(SocketClass::Nntp));
        assert!(!sockets.has_room(SocketClass::Torrent, peer));
        let again = refused(&sockets, SocketClass::Nntp);
        assert!(sockets.owes(SocketClass::Torrent), "the debt still stands");
        assert_eq!(sockets.woken(), [false, false], "a dry side woken");
        drop(again);
    }

    // Torrents' holdings change: the next press wakes them for the whole
    // level again.
    drop(peers.pop().unwrap().lingering());
    let again = refused(&sockets, SocketClass::Nntp);
    assert_eq!(sockets.woken(), [false, true], "woken after a change");
    assert_eq!(owed(&sockets), [0, (alone - 57 - 1) * peer]);
    assert_eq!(
        give_back(&sockets, SocketClass::Torrent, &mut peers) as u64,
        alone - 57 - 1
    );
    drop(again);
}

// At 70/30 with both sides wanting every socket they can get, the sides
// converge on their proportions, and from then on nothing more closes: each
// side keeps its demands and keeps being refused, and no ask is raised or
// stands on either.
#[tokio::test(start_paused = true)]
async fn a_70_30_split_converges_and_then_closes_nothing() {
    use weaver_tunnel::pipe::SocketClass;
    let peer = stream_bytes(None);
    let sockets = rule_sockets(Some(70), 190);
    let mut peers = Vec::new();
    while sockets.has_room(SocketClass::Torrent, peer) {
        peers.push(take_peer(&sockets, SocketClass::Torrent).expect("the room it had"));
    }
    let mut connections = Vec::new();
    let parked = take_until_refused(&sockets, SocketClass::Nntp, &mut connections);
    assert_eq!(
        connections.len() + peers.len(),
        190,
        "a lone side and a probe"
    );
    let usenet_demand = parked.demand.expect("a side refused presses");
    let torrents = peers.len();
    assert_eq!(owed(&sockets), [0, (torrents as u64 - 57) * peer]);
    assert_eq!(sockets.woken(), [false, true]);
    assert_eq!(
        give_back(&sockets, SocketClass::Torrent, &mut peers),
        torrents - 57
    );
    lingered_back(parked.freed.notify()).await;
    while let Some(connection) = take_peer(&sockets, SocketClass::Nntp) {
        connections.push(connection);
    }
    assert_eq!(connections.len(), 133, "usenet at its 70 percent");
    assert_eq!(peers.len(), 57, "torrents at their 30 percent");
    assert!(
        sockets
            .wait_room(SocketClass::Torrent, peer)
            .expect_err("the sides are full")
            .demand
            .is_none(),
        "torrents at their share are not crowded out"
    );

    // Converged. Both keep wanting, and nothing more closes.
    let mut closes = 0;
    let mut refusals = Vec::new();
    for _ in 0..20 {
        refusals.push(refused(&sockets, SocketClass::Nntp));
        refusals.push(refused(&sockets, SocketClass::Torrent));
        assert!(!sockets.has_room(SocketClass::Nntp, peer));
        assert!(!sockets.has_room(SocketClass::Torrent, peer));
        assert_eq!(owed(&sockets), [0, 0], "owed after convergence");
        assert_eq!(sockets.woken(), [false, false], "woken after convergence");
        closes += give_back(&sockets, SocketClass::Nntp, &mut connections);
        closes += give_back(&sockets, SocketClass::Torrent, &mut peers);
    }
    assert_eq!(closes, 0, "closes after convergence");
    drop((refusals, usenet_demand));
    assert!(!sockets.pressing(SocketClass::Nntp));
    assert!(!sockets.pressing(SocketClass::Torrent));
    assert_eq!((connections.len(), peers.len()), (133, 57));
}

// A torrent session's listeners and datagram sockets are kept off the split,
// so they all bind beside usenet holding every byte it can, and a give-back
// asks torrents only for peers they can close. At 90/10 with one session and
// at 50/50 with three, torrents press for peers and usenet gives back to their
// share; usenet then presses back and is asked nothing, holding its share.
#[test]
fn torrent_sessions_sockets_are_off_the_split() {
    use weaver_tunnel::pipe::{SocketClass, datagram_bytes, listener_bytes};
    let peer = stream_bytes(None);
    for (usenet_pct, sessions) in [(Some(90), 1), (Some(50), 3)] {
        let tunnel = TunnelSockets {
            torrent_sessions: sessions,
            ..TunnelSockets::new(0, 1, 1420)
        };
        let sockets = weaver_tunnel::pipe::SessionSockets::new(
            tunnel.budget(),
            std::sync::Arc::new(fixed_split(usenet_pct)),
        );
        let usenet = usenet_socket_bytes();
        let mut connections: Vec<_> =
            std::iter::from_fn(|| sockets.take(SocketClass::Nntp, usenet).ok().flatten()).collect();
        let mut fixed = Vec::new();
        for _ in 0..sessions {
            for _ in 0..DATAGRAMS_PER_TORRENT_SESSION {
                fixed.push(sockets.take(SocketClass::TorrentSession, datagram_bytes(1420)));
            }
            for _ in 0..LISTENERS_PER_TORRENT_SESSION {
                fixed.push(sockets.take(SocketClass::TorrentSession, listener_bytes()));
            }
        }
        assert!(
            fixed.iter().all(|socket| matches!(socket, Ok(Some(_)))),
            "{usenet_pct:?}: a session could not bind"
        );
        assert_eq!(sockets.held(SocketClass::Torrent), 0, "{usenet_pct:?}");
        assert_eq!(
            sockets.held(SocketClass::TorrentSession),
            tunnel.torrent_kept()
        );

        // Torrents press for peers, and usenet gives back what is asked.
        let mut pressing = Vec::new();
        let mut peers = Vec::new();
        loop {
            match sockets.take(SocketClass::Torrent, peer) {
                Ok(held) => peers.push(held),
                Err(refused) => {
                    pressing.push(refused);
                    let asked = sockets.owed(SocketClass::Nntp);
                    if asked == 0 {
                        break;
                    }
                    for _ in 0..asked.div_ceil(usenet) {
                        connections.pop();
                    }
                }
            }
        }
        let torrents = sockets.given(SocketClass::Torrent);
        assert!(!peers.is_empty(), "{usenet_pct:?}: torrents got no peer");
        assert!(
            sockets.held(SocketClass::Torrent) + peer > torrents,
            "{usenet_pct:?}"
        );

        // Usenet presses back: torrents hold no more than their share, so
        // nothing is asked, and usenet holds its share to a connection.
        let refused = sockets.take(SocketClass::Nntp, usenet);
        assert!(refused.is_err());
        assert_eq!(
            owed(&sockets),
            [0, 0],
            "{usenet_pct:?}: owed what cannot be given"
        );
        assert!(
            sockets.held(SocketClass::Nntp) + 2 * usenet > sockets.given(SocketClass::Nntp),
            "{usenet_pct:?}: usenet short of its share"
        );
    }
}

// What the tunnel holds never passes its bytes over a long run of random
// opens, closes, lingers and presses, and of budget changes, lowered and
// raised, MTU among them, on the bandwidth split's rule: while it holds past
// a lowered limit, no new hand-out is granted.
#[tokio::test(start_paused = true)]
async fn what_is_held_never_passes_the_limit() {
    use std::time::Duration;
    use weaver_tunnel::pipe::{
        SocketBudget, SocketClass, SocketPermit, datagram_bytes, listener_bytes,
    };
    let peer = stream_bytes(None);
    let budget = |sides: u64, mtu: u16| SocketBudget {
        bytes: overhead_bytes(mtu) + 4 * peer + 2 * listener_bytes() + sides * peer,
        reserve: 4 * peer,
        torrent_kept: 2 * listener_bytes(),
        mtu,
    };
    let sockets = rule_sockets(Some(60), 40);
    sockets.set_budget(budget(40, 1420));
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let classes = [
        SocketClass::Nntp,
        SocketClass::Torrent,
        SocketClass::Reserve,
        SocketClass::TorrentSession,
    ];
    let sizes = [peer, datagram_bytes(1420), listener_bytes(), 3 * peer / 2];
    let mut open: Vec<SocketPermit> = Vec::new();
    let mut pressing = Vec::new();
    let mut lowered = 0;
    for _ in 0..20_000 {
        match next() % 16 {
            0..=8 => {
                let class = classes[(next() % 4) as usize];
                let bytes = sizes[(next() % 4) as usize];
                match sockets.take(class, bytes) {
                    Ok(Some(permit)) => {
                        // Granted only when what is held, this one with it,
                        // is within the limit: never while held is past it.
                        assert!(
                            sockets.reserved() <= sockets.limit(),
                            "handed out past the limit"
                        );
                        open.push(if next() % 2 == 0 {
                            permit.lingering()
                        } else {
                            permit
                        });
                    }
                    Ok(None) => unreachable!("a budgeted tunnel hands out permits"),
                    Err(refused) if next() % 4 == 0 => pressing.push(refused),
                    Err(_) => {}
                }
            }
            9..=11 if !open.is_empty() => {
                let at = (next() % open.len() as u64) as usize;
                drop(open.swap_remove(at));
            }
            12 => pressing.clear(),
            13 => {
                let before = sockets.limit();
                let sides = 20 + next() % 40;
                let mtu = [1280, 1420, 1500][(next() % 3) as usize];
                sockets.set_budget(budget(sides, mtu));
                if sockets.reserved() > sockets.limit() && sockets.limit() < before {
                    lowered += 1;
                }
            }
            _ => tokio::time::advance(Duration::from_millis(next() % 3_000)).await,
        }
        tokio::task::yield_now().await;
        let limit = sockets.budget().expect("a budget");
        assert_eq!(
            sockets.reserved(),
            sockets.held(SocketClass::Nntp)
                + sockets.held(SocketClass::Torrent)
                + sockets.held(SocketClass::Reserve)
                + sockets.held(SocketClass::TorrentSession)
                + overhead_bytes(limit.mtu)
        );
    }
    assert!(
        lowered > 0,
        "no budget change left the tunnel over its limit"
    );
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
        refused_code(db.insert_server_with_routing(&server(1, 500, true), Some(&through_tunnel))),
        CODE
    );
    assert!(db.list_servers().unwrap().is_empty());
    db.insert_server_with_routing(&server(1, 400, true), Some(&through_tunnel))
        .unwrap();

    assert_eq!(refused_code(db.update_server(&server(1, 500, true))), CODE);
    assert_eq!(db.list_servers().unwrap()[0].connections, 400);

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
    // 401 usenet connections fit beside the reserve, not beside a torrent
    // session too.
    db.insert_server_with_routing(&server(1, 400, true), Some(&through_tunnel))
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
        refused_code(db.update_server(&server(1, 401, true))),
        Some("WIREGUARD_SOCKET_BUDGET")
    );
    db.update_server(&server(1, 380, true)).unwrap();
}
