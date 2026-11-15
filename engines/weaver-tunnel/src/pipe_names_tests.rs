// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn proxy(system_fallback: bool) -> Names {
    Names::new(
        DnsChoice::Proxy {
            trust: Trust::AsSent,
            system_fallback,
        },
        None,
    )
}

#[test]
fn a_stage_with_no_proxy_is_answered_by_the_host_when_the_profile_falls_back() {
    assert!(matches!(proxy(true).at_egress(false), Naming::Host));
}

#[test]
fn a_stage_with_no_proxy_fails_its_lookups_when_the_profile_does_not_fall_back() {
    let Naming::Refused(reason) = proxy(false).at_egress(false) else {
        panic!("a proxy-less stage was answered");
    };
    assert!(reason.contains("has no proxy"), "{reason}");
}

#[test]
fn a_proxy_endpoint_is_named_by_the_host_whatever_the_fallback() {
    for system_fallback in [false, true] {
        assert!(matches!(
            proxy(system_fallback).at_egress(true),
            Naming::Host
        ));
        assert!(matches!(
            proxy(system_fallback).for_endpoint().at_egress(false),
            Naming::Host
        ));
    }
}

#[test]
fn a_stage_through_a_proxy_hands_it_the_name_whatever_the_fallback() {
    for system_fallback in [false, true] {
        assert!(matches!(
            proxy(system_fallback).at_hop("hop", None),
            Naming::Proxy
        ));
    }
}

#[test]
fn a_wireguard_hop_that_lists_no_dns_servers_fails_the_lookup_when_the_proxy_answers() {
    for system_fallback in [false, true] {
        let Naming::Refused(reason) = proxy(system_fallback).at_hop("hop", Some(&[])) else {
            panic!("a WireGuard hop with no DNS servers was answered");
        };
        assert_eq!(reason, "the WireGuard proxy lists no DNS servers");
    }
}

#[test]
fn a_wireguard_hop_that_lists_several_dns_servers_asks_every_one() {
    let servers = [
        DnsServer::plain("10.64.0.1".parse().unwrap()),
        DnsServer::plain("10.64.0.2".parse().unwrap()),
    ];
    let Naming::ProxyServers { servers: asked, .. } = proxy(false).at_hop("hop", Some(&servers))
    else {
        panic!("a WireGuard hop's servers were not asked");
    };
    assert_eq!(asked, servers);
}

// An inner proxy's own address: listed servers are asked on the path below
// the hop that carries it, and on the host's profile the carrier resolves it:
// a proxy is handed the name, a WireGuard tunnel's servers are asked through
// it, and one with none fails. A first hop's is the host's.
#[test]
fn an_inner_proxys_address_is_asked_below_its_hop_or_of_the_host() {
    let servers = vec![DnsServer::plain("192.0.2.53".parse().unwrap())];
    let weaver = Names::new(
        DnsChoice::Weaver {
            servers: servers.clone(),
            trust: Trust::AsSent,
            transit: DnsTransit::Egress,
        },
        None,
    );
    let Naming::ProxyServers { servers: asked, .. } = weaver.for_endpoint().at_hop("hop", None)
    else {
        panic!("an inner proxy's listed servers were not asked through the hop beneath");
    };
    assert_eq!(asked, servers);
    let system = Names::new(DnsChoice::System, None).for_endpoint();
    assert!(matches!(system.at_hop("hop", None), Naming::Proxy));
    let tunnel = [DnsServer::plain("10.64.0.1".parse().unwrap())];
    let Naming::ProxyServers { servers: asked, .. } = system.at_hop("hop", Some(&tunnel)) else {
        panic!("a WireGuard carrier's servers were not asked");
    };
    assert_eq!(asked, tunnel);
    assert!(matches!(
        system.at_hop("hop", Some(&[])),
        Naming::Refused(_)
    ));
    assert!(matches!(system.at_egress(false), Naming::Host));
}
