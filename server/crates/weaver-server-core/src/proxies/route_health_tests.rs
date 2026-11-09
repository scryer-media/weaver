// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::proxies::{ProxySecrets, Resolver};

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

fn configured(
    egresses: &[EgressInterface],
    profiles: &[ProxyProfile],
) -> (Arc<RouteHealth>, mpsc::UnboundedReceiver<RouteDown>) {
    let health = RouteHealth::new();
    configure(&health, egresses, profiles);
    let downs = health.subscribe();
    (health, downs)
}

fn configure(health: &RouteHealth, egresses: &[EgressInterface], profiles: &[ProxyProfile]) {
    health.configure(
        &egresses.iter().map(|e| (e.id, e.clone())).collect(),
        &profiles.iter().map(|p| (p.id, p.clone())).collect(),
    );
}

fn down(reason: &str) -> EgressHealth {
    EgressHealth::Down(reason.into())
}

// Everything told so far. Each turn is told as it is seen, so what was
// told is all there is.
fn told(downs: &mut mpsc::UnboundedReceiver<RouteDown>) -> Vec<RouteDown> {
    let mut told = Vec::new();
    while let Ok(down) = downs.try_recv() {
        told.push(down);
    }
    told
}

// An interface seen up and then down is told once, with its egress, kind,
// name and reason. Staying down, or a health check that cannot tell, says
// nothing more; up and down again is told again.
#[test]
fn an_interface_up_then_down_is_told_once_per_turn() {
    let (health, mut downs) = configured(&[interface(3)], &[]);
    health.egress(3, &EgressHealth::Up);
    health.egress(3, &down("Interface is down"));
    assert_eq!(
        told(&mut downs),
        vec![RouteDown {
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
    health.egress(3, &down("Interface is missing"));
    assert_eq!(told(&mut downs).len(), 1);
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
        vec![RouteDown {
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
