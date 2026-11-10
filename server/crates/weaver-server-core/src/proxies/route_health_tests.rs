// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::time::{Duration, Instant};

use crate::proxies::{ProxySecrets, Resolver, RouteLeg};

fn interface(id: u32) -> EgressInterface {
    EgressInterface {
        id,
        name: format!("eth-{id}"),
        binding: EgressBinding::Interface {
            name: format!("eth{id}"),
        },
        enabled: true,
        download_bps: None,
        upload_bps: None,
        quota: Default::default(),
    }
}

fn wireguard(id: u32, revision: u64) -> ProxyProfile {
    ProxyProfile {
        id,
        name: format!("wg-{id}"),
        kind: ProxyKind::WireGuard,
        enabled: true,
        host: "vpn.invalid".into(),
        port: 51820,
        resolver: Resolver::Weaver,
        dns_servers: vec![],
        tunnel_addresses: vec![],
        peer_public_key: None,
        mtu: 1420,
        keepalive_seconds: None,
        timeout_seconds: 5,
        host_key_fingerprint: None,
        natpmp: false,
        forwarded_port: None,
        revision,
        secrets: ProxySecrets::default(),
    }
}

// A clock the test moves by hand.
struct Clock(Mutex<Instant>);
impl RouteClock for Clock {
    fn now(&self) -> Instant {
        *self.0.lock().unwrap()
    }
}
impl Clock {
    fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += by;
    }
}

fn configured(
    egresses: &[EgressInterface],
    profiles: &[ProxyProfile],
) -> (Arc<RouteHealth>, mpsc::UnboundedReceiver<RouteEvent>) {
    let (health, events, _) = clocked(egresses, profiles, &[], vec![]);
    (health, events)
}

fn clocked(
    egresses: &[EgressInterface],
    profiles: &[ProxyProfile],
    pools: &[ProxyPool],
    routes: Vec<Route>,
) -> (
    Arc<RouteHealth>,
    mpsc::UnboundedReceiver<RouteEvent>,
    Arc<Clock>,
) {
    let clock = Arc::new(Clock(Mutex::new(Instant::now())));
    let health = RouteHealth::with_clock(clock.clone());
    configure_with(&health, egresses, profiles, pools, routes);
    let events = health.subscribe();
    (health, events, clock)
}

fn configure(health: &RouteHealth, egresses: &[EgressInterface], profiles: &[ProxyProfile]) {
    configure_with(health, egresses, profiles, &[], vec![]);
}

fn configure_with(
    health: &RouteHealth,
    egresses: &[EgressInterface],
    profiles: &[ProxyProfile],
    pools: &[ProxyPool],
    routes: Vec<Route>,
) {
    health.configure(
        &egresses.iter().map(|e| (e.id, e.clone())).collect(),
        &profiles.iter().map(|p| (p.id, p.clone())).collect(),
        &pools.iter().map(|p| (p.id, p.clone())).collect(),
        routes,
    );
}

fn pool(id: u32, members: &[u32]) -> ProxyPool {
    ProxyPool {
        id,
        name: format!("pool-{id}"),
        kind: ProxyKind::WireGuard,
        member_ids: members.to_vec(),
        enabled: true,
    }
}

// A saved route of one leg on `egress` over `rungs`.
fn route(egress: u32, rungs: Vec<Rung>) -> Route {
    Route {
        legs: vec![RouteLeg {
            egress_id: egress,
            weight: 1,
            path: LegPath::Ladder {
                rungs,
                direct_fallback: false,
            },
        }],
        failover: Default::default(),
    }
}

fn changes(events: &[RouteEvent]) -> Vec<(RouteChange, RouteKind, &str)> {
    events
        .iter()
        .map(|event| (event.change, event.kind, event.name.as_str()))
        .collect()
}

fn down(reason: &str) -> EgressHealth {
    EgressHealth::Down(reason.into())
}

// Everything told so far. Each turn is told as it is seen, so what was
// told is all there is.
fn told(downs: &mut mpsc::UnboundedReceiver<RouteEvent>) -> Vec<RouteEvent> {
    let mut told = Vec::new();
    while let Ok(down) = downs.try_recv() {
        told.push(down);
    }
    told
}

// An interface seen up and then down is told once, with its egress, kind,
// name and reason. Staying down, or a health check that cannot tell, says
// nothing more; up again is told up, and down again after the quiet period
// is told down again.
#[test]
fn an_interface_up_then_down_is_told_once_per_turn() {
    let (health, mut downs) = configured(&[interface(3)], &[]);
    health.egress(3, &EgressHealth::Up);
    health.egress(3, &down("Interface is down"));
    assert_eq!(
        told(&mut downs),
        vec![RouteEvent {
            change: RouteChange::Down,
            egress_id: 3,
            kind: RouteKind::Interface,
            name: "eth-3".into(),
            reason: "Interface is down".into(),
        }]
    );
    health.egress(3, &down("Interface is down"));
    health.egress(3, &down("Interface is missing"));
    health.egress(3, &EgressHealth::Unknown);
    assert_eq!(told(&mut downs), vec![]);
    health.egress(3, &EgressHealth::Up);
    health.egress(3, &EgressHealth::Unknown);
    assert_eq!(
        told(&mut downs),
        vec![RouteEvent {
            change: RouteChange::Up,
            egress_id: 3,
            kind: RouteKind::Interface,
            name: "eth-3".into(),
            reason: String::new(),
        }]
    );
}

// A route down from the start was never up, so nothing is told until it
// has been up.
#[test]
fn a_route_down_at_the_start_is_not_told() {
    let (health, mut downs) = configured(&[interface(3)], &[wireguard(5, 1)]);
    let hop = health.watcher(3, 5, 1);
    health.egress(3, &down("Interface is missing"));
    hop.failing(&ProxyFault::ConnectionClosed);
    assert_eq!(told(&mut downs), vec![]);
    health.egress(3, &EgressHealth::Up);
    health.egress(3, &down("Interface is down"));
    assert_eq!(told(&mut downs).len(), 1);
}

// A disabled or deleted egress, or one whose binding was edited, is not
// told: the edit forgets that it was up.
#[test]
fn a_disabled_deleted_or_edited_egress_is_not_told() {
    let system = EgressInterface {
        binding: EgressBinding::System,
        ..interface(4)
    };
    let (health, mut downs) = configured(&[interface(3), system], &[]);
    health.egress(3, &EgressHealth::Up);
    let mut edited = interface(3);
    edited.binding = EgressBinding::Interface {
        name: "wlan0".into(),
    };
    configure(&health, &[edited.clone()], &[]);
    health.egress(3, &down("Interface is missing"));
    assert_eq!(told(&mut downs), vec![]);

    health.egress(3, &EgressHealth::Up);
    edited.enabled = false;
    configure(&health, &[edited.clone()], &[]);
    health.egress(3, &down("Disabled"));
    assert_eq!(told(&mut downs), vec![]);

    edited.enabled = true;
    configure(&health, &[edited], &[]);
    health.egress(3, &EgressHealth::Up);
    configure(&health, &[], &[]);
    health.egress(3, &down("Interface is missing"));
    assert_eq!(told(&mut downs), vec![]);
}

// A rename or a rate edit keeps what was seen, and the down names the
// egress as it is now.
#[test]
fn an_egress_renamed_keeps_what_was_seen() {
    let (health, mut downs) = configured(&[interface(3)], &[]);
    health.egress(3, &EgressHealth::Up);
    let renamed = EgressInterface {
        name: "uplink".into(),
        download_bps: Some(1_000),
        ..interface(3)
    };
    configure(&health, &[renamed], &[]);
    health.egress(3, &down("Interface is down"));
    assert_eq!(told(&mut downs)[0].name, "uplink");
}

// A proxy whose hop worked and then failed is told once, as the proxy on
// its egress, with the fault as the reason.
#[test]
fn a_proxy_up_then_down_is_told_once() {
    let (health, mut downs) = configured(&[interface(3)], &[wireguard(5, 1)]);
    let hop = health.watcher(3, 5, 1);
    hop.working();
    hop.failing(&ProxyFault::ConnectionClosed);
    assert_eq!(
        told(&mut downs),
        vec![RouteEvent {
            change: RouteChange::Down,
            egress_id: 3,
            kind: RouteKind::WireGuard,
            name: "wg-5".into(),
            reason: ProxyFault::ConnectionClosed.to_string(),
        }]
    );
    // Another hop of the same proxy on the same egress is the same route.
    let other = health.watcher(3, 5, 1);
    other.failing(&ProxyFault::ConnectionClosed);
    hop.failing(&ProxyFault::ConnectionClosed);
    assert_eq!(told(&mut downs), vec![]);
}

// A proxy the operator disabled, deleted or edited is not told, nor is a
// hop built from its old revision.
#[test]
fn a_disabled_deleted_or_edited_proxy_is_not_told() {
    let (health, mut downs) = configured(&[interface(3)], &[wireguard(5, 1)]);
    let old = health.watcher(3, 5, 1);
    old.working();
    configure(&health, &[interface(3)], &[wireguard(5, 2)]);
    old.failing(&ProxyFault::ConnectionClosed);
    let new = health.watcher(3, 5, 2);
    new.failing(&ProxyFault::ConnectionClosed);
    assert_eq!(told(&mut downs), vec![]);

    new.working();
    let mut disabled = wireguard(5, 2);
    disabled.enabled = false;
    configure(&health, &[interface(3)], &[disabled]);
    new.failing(&ProxyFault::ConnectionClosed);
    assert_eq!(told(&mut downs), vec![]);

    configure(&health, &[interface(3)], &[wireguard(5, 2)]);
    new.working();
    configure(&health, &[interface(3)], &[]);
    new.failing(&ProxyFault::ConnectionClosed);
    assert_eq!(told(&mut downs), vec![]);
}

// A pool with one member down is still up and tells nothing, nor does the
// member, which is used only in the pool. Once every member is down the
// pool is told down once, by its own name; a member back is the pool up.
#[test]
fn a_pool_is_down_only_once_every_member_is() {
    let (health, mut events, _) = clocked(
        &[interface(3)],
        &[wireguard(5, 1), wireguard(6, 1)],
        &[pool(10, &[5, 6])],
        vec![route(3, vec![Rung::Pool { id: 10 }])],
    );
    let first = health.watcher(3, 5, 1);
    let second = health.watcher(3, 6, 1);
    first.working();
    second.working();
    first.failing(&ProxyFault::ConnectionClosed);
    assert_eq!(told(&mut events), vec![]);
    second.failing(&ProxyFault::ConnectionClosed);
    second.failing(&ProxyFault::ConnectionClosed);
    assert_eq!(
        told(&mut events),
        vec![RouteEvent {
            change: RouteChange::Down,
            egress_id: 3,
            kind: RouteKind::Pool,
            name: "pool-10".into(),
            reason: format!("wg-6: {}", ProxyFault::ConnectionClosed),
        }]
    );
    first.working();
    assert_eq!(
        changes(&told(&mut events)),
        vec![(RouteChange::Up, RouteKind::Pool, "pool-10")]
    );
}

// A proxy used alone as well as in a pool on the same egress tells as
// itself; so does each proxy of a chain.
#[test]
fn a_proxy_used_alone_or_in_a_chain_tells_as_itself() {
    let (health, mut events, _) = clocked(
        &[interface(3)],
        &[wireguard(5, 1), wireguard(6, 1), wireguard(7, 1)],
        &[pool(10, &[5, 6])],
        vec![
            route(3, vec![Rung::Pool { id: 10 }]),
            route(3, vec![Rung::Proxy { id: 5 }]),
            route(3, vec![Rung::Chain { ids: vec![6, 7] }]),
        ],
    );
    for proxy in [5, 7] {
        let hop = health.watcher(3, proxy, 1);
        hop.working();
        hop.failing(&ProxyFault::ConnectionClosed);
    }
    assert_eq!(
        changes(&told(&mut events)),
        vec![
            (RouteChange::Down, RouteKind::WireGuard, "wg-5"),
            (RouteChange::Down, RouteKind::WireGuard, "wg-7"),
        ]
    );
}

// After a route is told down, a second down inside the quiet period waits,
// and is told when the period ends if the route is still down.
#[test]
fn the_quiet_period_holds_a_second_down() {
    let (health, mut events, clock) = clocked(&[interface(3)], &[], &[], vec![]);
    health.egress(3, &EgressHealth::Up);
    health.egress(3, &down("Interface is down"));
    health.egress(3, &EgressHealth::Up);
    health.egress(3, &down("Interface is down"));
    assert_eq!(
        changes(&told(&mut events)),
        vec![
            (RouteChange::Down, RouteKind::Interface, "eth-3"),
            (RouteChange::Up, RouteKind::Interface, "eth-3"),
        ]
    );
    let due = health.due().expect("a down waits");
    clock.advance(QUIET_PERIOD - Duration::from_secs(1));
    health.settle();
    assert_eq!(told(&mut events), vec![]);
    clock.advance(Duration::from_secs(1));
    assert_eq!(clock.now(), due);
    health.settle();
    assert_eq!(
        changes(&told(&mut events)),
        vec![(RouteChange::Down, RouteKind::Interface, "eth-3")]
    );
    assert_eq!(health.due(), None);
}

// A route flapping down, up, down, up inside the quiet period is told down
// once and up once. Once it holds, the last told matches it: up stays as
// told, and a flap that ends down is told down when the period ends.
#[test]
fn a_flapping_route_is_told_one_down_and_one_up() {
    let (health, mut events, clock) = clocked(&[interface(3)], &[], &[], vec![]);
    health.egress(3, &EgressHealth::Up);
    for _ in 0..2 {
        health.egress(3, &down("Interface is down"));
        health.egress(3, &EgressHealth::Up);
    }
    let flap = vec![
        (RouteChange::Down, RouteKind::Interface, "eth-3"),
        (RouteChange::Up, RouteKind::Interface, "eth-3"),
    ];
    assert_eq!(changes(&told(&mut events)), flap);
    clock.advance(QUIET_PERIOD);
    health.settle();
    assert_eq!(told(&mut events), vec![]);

    for _ in 0..2 {
        health.egress(3, &down("Interface is down"));
        health.egress(3, &EgressHealth::Up);
    }
    health.egress(3, &down("Interface is down"));
    assert_eq!(changes(&told(&mut events)), flap);
    clock.advance(QUIET_PERIOD);
    health.settle();
    assert_eq!(
        changes(&told(&mut events)),
        vec![(RouteChange::Down, RouteKind::Interface, "eth-3")]
    );
}

// While its egress is down a proxy tells nothing, and a fault that is the
// egress's own tells nothing either. Back up, the proxy starts again as
// never seen up; one told down before is told up when it works again.
#[test]
fn a_proxy_is_silent_while_its_egress_is_down() {
    let (health, mut events, _) = clocked(&[interface(3)], &[wireguard(5, 1)], &[], vec![]);
    let hop = health.watcher(3, 5, 1);
    health.egress(3, &EgressHealth::Up);
    hop.working();
    hop.failing(&ProxyFault::NoRouteVia { via: "eth3".into() });
    hop.failing(&ProxyFault::EgressUnusable {
        detail: "no such device".into(),
    });
    assert_eq!(told(&mut events), vec![]);

    hop.failing(&ProxyFault::ConnectionClosed);
    health.egress(3, &down("Interface is down"));
    hop.working();
    hop.failing(&ProxyFault::ConnectionClosed);
    assert_eq!(
        changes(&told(&mut events)),
        vec![
            (RouteChange::Down, RouteKind::WireGuard, "wg-5"),
            (RouteChange::Down, RouteKind::Interface, "eth-3"),
        ]
    );

    health.egress(3, &EgressHealth::Up);
    hop.failing(&ProxyFault::ConnectionClosed);
    assert_eq!(
        changes(&told(&mut events)),
        vec![(RouteChange::Up, RouteKind::Interface, "eth-3")]
    );
    hop.working();
    assert_eq!(
        changes(&told(&mut events)),
        vec![(RouteChange::Up, RouteKind::WireGuard, "wg-5")]
    );
}
