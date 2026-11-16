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

// What a naming came to, in terms the table below states: the resolver,
// the servers it lists, the trust they are judged by, and the held slot
// (the hop's path, or "egress") whose answers and TLS sessions it uses.
#[derive(Debug, PartialEq)]
enum Named {
    Host,
    Proxy,
    Refused(String),
    AtEgress {
        servers: Vec<DnsServer>,
        trust: Trust,
        slot: String,
    },
    ThroughHop {
        servers: Vec<DnsServer>,
        trust: Trust,
        slot: String,
    },
}

const PATH: &str = "hop-path";
const NO_PROXY: &str = "the route's DNS is answered by its proxy, and this stage has no proxy";
const NO_EGRESS: &str = "the route's DNS servers have no egress to be asked from";
const CARRIER_LISTS_NONE: &str = "the WireGuard proxy carrying this proxy lists no DNS servers";
const TUNNEL_LISTS_NONE: &str = "the WireGuard proxy lists no DNS servers";
const ONLY_WIREGUARD_VALIDATES: &str = "only a WireGuard proxy's answers can be validated here";
const REFUSED: &str = "the profile cannot be read";

// The one held slot whose answers and sessions the naming holds.
fn slot_of(
    names: &Names,
    answers: &Arc<ResolutionCache<DialError>>,
    sessions: &Arc<DnsSessions>,
) -> String {
    let held = names.inner.held.lock().expect("held names");
    let slots: Vec<&String> = held
        .iter()
        .filter(|(_, held)| {
            Arc::ptr_eq(&held.answers, answers) && Arc::ptr_eq(&held.sessions, sessions)
        })
        .map(|(path, _)| path)
        .collect();
    assert_eq!(slots.len(), 1, "held in {slots:?}");
    slots[0].clone()
}

fn seen(names: &Names, naming: Naming) -> Named {
    match naming {
        Naming::Host => Named::Host,
        Naming::Proxy => Named::Proxy,
        Naming::Refused(reason) => Named::Refused(reason),
        Naming::Servers {
            servers,
            trust,
            egress,
            answers,
            sessions,
        } => {
            let own = names.inner.egress.as_ref().expect("the names' egress");
            assert!(Arc::ptr_eq(own, &egress), "asked from another egress");
            Named::AtEgress {
                servers,
                trust,
                slot: slot_of(names, &answers, &sessions),
            }
        }
        Naming::ProxyServers {
            servers,
            trust,
            answers,
            sessions,
        } => Named::ThroughHop {
            servers,
            trust,
            slot: slot_of(names, &answers, &sessions),
        },
    }
}

#[derive(Clone, Copy, Debug)]
enum HopKind {
    // A WireGuard tunnel that lists DNS servers.
    Lists,
    // A WireGuard tunnel that lists none.
    ListsNone,
    // Any other proxy.
    NotATunnel,
}

#[derive(Clone, Copy, Debug)]
enum Cell {
    // At the egress, with the caller's endpoint flag.
    Egress { caller: bool },
    // Through a hop of the kind given.
    Hop { tunnel: HopKind },
}

fn listed() -> Vec<DnsServer> {
    vec![
        DnsServer::plain("192.0.2.53".parse().unwrap()),
        DnsServer {
            address: "192.0.2.54:853".parse().unwrap(),
            wire: crate::dns::Wire::Tls { name: None },
        },
    ]
}

fn tunnel_servers() -> Vec<DnsServer> {
    vec![
        DnsServer::plain("10.64.0.1".parse().unwrap()),
        DnsServer::plain("10.64.0.2".parse().unwrap()),
    ]
}

// The truth table, row by row: what each choice gives at each cell, with
// `own` the names' own endpoint flag and `has_egress` whether the names
// have an egress to ask listed servers from.
fn expected(choice: &DnsChoice, cell: Cell, own: bool, has_egress: bool) -> Named {
    let refused = |reason: &str| Named::Refused(reason.to_owned());
    let at_egress = |servers: Vec<DnsServer>, trust: Trust| {
        if has_egress {
            Named::AtEgress {
                servers,
                trust,
                slot: "egress".to_owned(),
            }
        } else {
            refused(NO_EGRESS)
        }
    };
    let through_hop = |servers: Vec<DnsServer>, trust: Trust| Named::ThroughHop {
        servers,
        trust,
        slot: PATH.to_owned(),
    };
    match (choice, cell) {
        (DnsChoice::Refused(reason), _) => refused(reason),
        // At the egress.
        (DnsChoice::System, Cell::Egress { .. }) => Named::Host,
        (DnsChoice::Proxy { .. }, Cell::Egress { caller }) if caller || own => Named::Host,
        (
            DnsChoice::Proxy {
                system_fallback, ..
            },
            Cell::Egress { .. },
        ) => {
            if *system_fallback {
                Named::Host
            } else {
                refused(NO_PROXY)
            }
        }
        (DnsChoice::Weaver { trust, .. }, Cell::Egress { .. }) => at_egress(listed(), *trust),
        // Through a hop.
        (DnsChoice::System, Cell::Hop { .. }) if !own => Named::Host,
        (DnsChoice::System, Cell::Hop { tunnel }) => match tunnel {
            HopKind::Lists => through_hop(tunnel_servers(), Trust::AsSent),
            HopKind::ListsNone => refused(CARRIER_LISTS_NONE),
            HopKind::NotATunnel => Named::Proxy,
        },
        (DnsChoice::Proxy { trust, .. }, Cell::Hop { tunnel }) => match (tunnel, trust) {
            (HopKind::Lists, trust) => through_hop(tunnel_servers(), *trust),
            (HopKind::ListsNone, _) => refused(TUNNEL_LISTS_NONE),
            (HopKind::NotATunnel, Trust::AsSent) => Named::Proxy,
            (HopKind::NotATunnel, Trust::Validate) => refused(ONLY_WIREGUARD_VALIDATES),
        },
        (DnsChoice::Weaver { trust, .. }, Cell::Hop { .. }) if own => through_hop(listed(), *trust),
        (
            DnsChoice::Weaver {
                trust,
                transit: DnsTransit::Egress,
                ..
            },
            Cell::Hop { .. },
        ) => at_egress(listed(), *trust),
        (
            DnsChoice::Weaver {
                trust,
                transit: DnsTransit::Proxy,
                ..
            },
            Cell::Hop { .. },
        ) => through_hop(listed(), *trust),
    }
}

// Every choice, at every stage, for every endpoint flag, with and without
// an egress: the resolver, its servers, its trust, the slot that holds its
// answers, and each refusal's words.
#[test]
fn the_naming_policy_gives_each_cell_of_its_table() {
    let egress: Arc<dyn Dialer> = Arc::new(crate::test_support::loopback::egress(
        4,
        std::time::Duration::from_secs(30),
    ));
    let mut choices = vec![DnsChoice::Refused(REFUSED.into()), DnsChoice::System];
    for trust in [Trust::AsSent, Trust::Validate] {
        for system_fallback in [false, true] {
            choices.push(DnsChoice::Proxy {
                trust,
                system_fallback,
            });
        }
        for transit in [DnsTransit::Egress, DnsTransit::Proxy] {
            choices.push(DnsChoice::Weaver {
                servers: listed(),
                trust,
                transit,
            });
        }
    }
    let mut cells = vec![
        Cell::Egress { caller: false },
        Cell::Egress { caller: true },
    ];
    for tunnel in [HopKind::Lists, HopKind::ListsNone, HopKind::NotATunnel] {
        cells.push(Cell::Hop { tunnel });
    }
    let tunnel = tunnel_servers();
    let mut checked = 0;
    for choice in &choices {
        for &cell in &cells {
            for own in [false, true] {
                for has_egress in [false, true] {
                    let names = Names::new(choice.clone(), has_egress.then(|| egress.clone()));
                    let names = if own { names.for_endpoint() } else { names };
                    let naming = match cell {
                        Cell::Egress { caller } => names.at_egress(caller),
                        Cell::Hop { tunnel: kind } => names.at_hop(
                            PATH,
                            match kind {
                                HopKind::Lists => Some(tunnel.as_slice()),
                                HopKind::ListsNone => Some(&[]),
                                HopKind::NotATunnel => None,
                            },
                        ),
                    };
                    assert_eq!(
                        seen(&names, naming),
                        expected(choice, cell, own, has_egress),
                        "{choice:?} at {cell:?}, own endpoint {own}, egress {has_egress}"
                    );
                    checked += 1;
                }
            }
        }
    }
    assert_eq!(checked, 200);
}
