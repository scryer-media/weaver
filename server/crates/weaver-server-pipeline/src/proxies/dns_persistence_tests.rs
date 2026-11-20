// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::proxies::ProxiesPersistenceQueries;
use crate::proxies::{ListedServer, ListedWire, ProxySecrets, RouteLeg, RoutingPolicy};

fn refused<T: std::fmt::Debug>(result: Result<T, StateError>) -> &'static str {
    match result.unwrap_err() {
        StateError::Refused(refusal) => refusal.code,
        other => panic!("not a refusal: {other}"),
    }
}

fn socks(db: &Database) -> u32 {
    db.save_proxy_profile(&ProxyProfile {
        dns_profile_id: 0,
        id: 0,
        name: "socks".into(),
        kind: ProxyKind::Socks5,
        enabled: true,
        host: "proxy.example".into(),
        port: 1080,
        dns_servers: vec![],
        tunnel_addresses: vec![],
        peer_public_key: None,
        mtu: 1280,
        keepalive_seconds: None,
        timeout_seconds: 30,
        host_key_fingerprint: None,
        natpmp: false,
        forwarded_port: None,
        revision: 1,
        secrets: ProxySecrets::default(),
    })
    .unwrap()
    .id
}

fn proxy_answers(system_fallback: bool) -> DnsProfile {
    DnsProfile {
        kind: crate::proxies::DnsProfileKind::Route,
        id: 0,
        name: "Proxy".into(),
        who: DnsWho::Proxy,
        transit: None,
        trust: DnsTrust::AsSent,
        server_list_id: None,
        system_fallback,
    }
}

// A route through `proxy` that falls back to direct, so one of its rungs
// has no proxy.
fn falls_back(proxy: u32, profile: u32) -> RoutingPolicy {
    RoutingPolicy {
        proxy_ids: vec![],
        allow_direct: false,
        legs: vec![RouteLeg {
            egress_id: 0,
            weight: 100,
            path: LegPath::Ladder {
                rungs: vec![Rung::Proxy { id: proxy }],
                direct_fallback: true,
            },
        }],
        dns_profile_id: Some(profile),
        ..RoutingPolicy::default()
    }
}

#[test]
fn the_proxy_answers_a_stage_with_no_proxy_only_when_the_profile_falls_back() {
    let db = Database::open_in_memory().unwrap();
    let proxy = socks(&db);
    let strict = db.save_dns_profile(proxy_answers(false), true).unwrap();
    let fallback = db.save_dns_profile(proxy_answers(true), true).unwrap();
    assert!(!strict.system_fallback && fallback.system_fallback);

    assert_eq!(
        refused(db.save_proxy_routing_policy(Consumer::Server(1), &falls_back(proxy, strict.id))),
        "DNS_PROFILE_NEEDS_PROXY"
    );
    db.save_proxy_routing_policy(Consumer::Server(1), &falls_back(proxy, fallback.id))
        .unwrap();

    // Turning the fallback off under a route that needs it is refused, and
    // the saved profile keeps it on.
    let off = DnsProfile {
        system_fallback: false,
        ..fallback.clone()
    };
    assert_eq!(
        refused(db.save_dns_profile(off, false)),
        "DNS_PROFILE_NEEDS_PROXY"
    );
    assert_eq!(db.load_dns().unwrap().profile(fallback.id), Some(fallback));
}

#[test]
fn only_a_profile_where_the_proxy_answers_keeps_the_fallback() {
    let db = Database::open_in_memory().unwrap();
    let list = db
        .save_dns_server_list(
            DnsServerList {
                id: 0,
                name: "Listed".into(),
                servers: vec![ListedServer {
                    address: "192.0.2.53:53".parse().unwrap(),
                    wire: ListedWire::Plain,
                }],
            },
            true,
        )
        .unwrap();
    let weaver = db
        .save_dns_profile(
            DnsProfile {
                who: DnsWho::Weaver,
                server_list_id: Some(list.id),
                ..proxy_answers(true)
            },
            true,
        )
        .unwrap();
    assert!(!weaver.system_fallback);
    assert!(
        !db.load_dns()
            .unwrap()
            .profile(weaver.id)
            .unwrap()
            .system_fallback
    );
}

#[test]
fn a_profile_in_use_names_every_stored_route_even_one_whose_key_cannot_be_read() {
    let db = Database::open_in_memory().unwrap();
    let proxy = socks(&db);
    let profile = db.save_dns_profile(proxy_answers(true), true).unwrap();
    db.save_proxy_routing_policy(Consumer::Server(3), &falls_back(proxy, profile.id))
        .unwrap();
    let store = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&store, "unreadable_route", |tx| {
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO proxy_routes (consumer, policy, dns_profile_id) VALUES ({}, {}, {})",
                    &[
                        SqlArg::Text("server:x".into()),
                        SqlArg::Text("{}".into()),
                        SqlArg::I64(i64::from(profile.id)),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();

    let StateError::Refused(refusal) = db.delete_dns_profile(profile.id).unwrap_err() else {
        panic!("not a refusal");
    };
    assert_eq!(refusal.code, "DNS_PROFILE_IN_USE");
    assert_eq!(
        serde_json::Value::Object(refusal.params),
        serde_json::json!({ "routes": ["server:3", "server:x"], "proxies": [] })
    );
    assert_eq!(db.dns_profile(profile.id).unwrap(), Some(profile));
}

#[test]
fn a_profile_is_read_by_id_the_system_profile_included() {
    let db = Database::open_in_memory().unwrap();
    let profile = db.save_dns_profile(proxy_answers(true), true).unwrap();

    assert_eq!(
        db.dns_profile(SYSTEM_DNS_PROFILE_ID).unwrap(),
        Some(DnsProfile::system())
    );
    assert_eq!(db.dns_profile(profile.id).unwrap(), Some(profile.clone()));
    assert_eq!(db.dns_profile(profile.id + 1).unwrap(), None);
}

#[test]
fn the_users_of_a_list_are_only_the_profiles_naming_it() {
    let db = Database::open_in_memory().unwrap();
    let list = |name: &str| {
        db.save_dns_server_list(
            DnsServerList {
                id: 0,
                name: name.into(),
                servers: vec![ListedServer {
                    address: "192.0.2.53:53".parse().unwrap(),
                    wire: ListedWire::Plain,
                }],
            },
            true,
        )
        .unwrap()
        .id
    };
    let (first, second, unused) = (list("First"), list("Second"), list("Unused"));
    for (name, list) in [("Zeta", first), ("Alpha", first), ("Beta", second)] {
        db.save_dns_profile(
            DnsProfile {
                name: name.into(),
                who: DnsWho::Weaver,
                server_list_id: Some(list),
                ..proxy_answers(false)
            },
            true,
        )
        .unwrap();
    }

    let users = db.dns_list_users(&[first, unused, first]).unwrap();

    assert_eq!(
        users,
        HashMap::from([(first, vec!["Alpha".to_string(), "Zeta".to_string()])])
    );
    assert!(db.dns_list_users(&[]).unwrap().is_empty());
}

// A WireGuard proxy at `host` whose tunnel lists `dns_servers`.
fn wireguard(db: &Database, host: &str, dns_servers: &[&str]) -> u32 {
    use base64::Engine;
    let key = |seed: u8| base64::engine::general_purpose::STANDARD.encode([seed; 32]);
    // Each one under an id of its own, after any proxies already saved.
    let id = u32::try_from(db.list_proxy_profiles().unwrap().len()).unwrap() + 1;
    db.save_proxy_profile(&ProxyProfile {
        dns_profile_id: 0,
        id,
        name: format!("wg {host}"),
        kind: ProxyKind::WireGuard,
        enabled: true,
        host: host.into(),
        port: 51820,
        dns_servers: dns_servers.iter().map(|ip| ip.parse().unwrap()).collect(),
        tunnel_addresses: vec!["10.63.0.2/32".into()],
        peer_public_key: Some(key(11)),
        mtu: 1280,
        keepalive_seconds: None,
        timeout_seconds: 30,
        host_key_fingerprint: None,
        natpmp: false,
        forwarded_port: None,
        revision: 1,
        secrets: ProxySecrets {
            private_key: Some(key(1)),
            ..Default::default()
        },
    })
    .unwrap()
    .id
}

// A route through `proxy` alone, on DNS profile `profile`.
fn only_through(proxy: u32, profile: u32) -> RoutingPolicy {
    let mut route = falls_back(proxy, profile);
    route.legs[0].path = LegPath::Ladder {
        rungs: vec![Rung::Proxy { id: proxy }],
        direct_fallback: false,
    };
    route
}

#[test]
fn validating_the_proxys_answers_is_refused_on_any_stage_not_ending_at_wireguard() {
    let db = Database::open_in_memory().unwrap();
    let tunnel = wireguard(&db, "192.0.2.9", &["10.64.0.1"]);
    let validating = db
        .save_dns_profile(
            DnsProfile {
                trust: DnsTrust::Validate,
                ..proxy_answers(true)
            },
            true,
        )
        .unwrap();
    db.save_proxy_routing_policy(Consumer::Server(1), &only_through(tunnel, validating.id))
        .unwrap();
    // A direct fallback is a stage the host answers, fallback flag or not.
    assert_eq!(
        refused(
            db.save_proxy_routing_policy(Consumer::Server(2), &falls_back(tunnel, validating.id))
        ),
        "DNS_PROFILE_NEEDS_WIREGUARD"
    );
    // A direct leg too.
    let direct = RoutingPolicy {
        legs: vec![RouteLeg {
            egress_id: 0,
            weight: 100,
            path: LegPath::Direct,
        }],
        ..only_through(tunnel, validating.id)
    };
    assert_eq!(
        refused(db.save_proxy_routing_policy(Consumer::Server(2), &direct)),
        "DNS_PROFILE_NEEDS_WIREGUARD"
    );
}

#[test]
fn the_proxy_answering_at_a_wireguard_proxy_with_no_dns_servers_is_refused_naming_it() {
    let db = Database::open_in_memory().unwrap();
    let silent = wireguard(&db, "192.0.2.9", &[]);
    let proxy = db.save_dns_profile(proxy_answers(false), true).unwrap();
    let StateError::Refused(refusal) = db
        .save_proxy_routing_policy(Consumer::Server(1), &only_through(silent, proxy.id))
        .unwrap_err()
    else {
        panic!("not a refusal");
    };
    assert_eq!(refusal.code, "DNS_PROFILE_NEEDS_TUNNEL_DNS");
    assert_eq!(
        serde_json::Value::Object(refusal.params),
        serde_json::json!({ "proxies": [silent], "routes": ["server:1"] })
    );

    // With servers listed, the route saves; taking them away is refused on
    // the proxy's save.
    let listing = wireguard(&db, "192.0.2.10", &["10.64.0.1", "10.64.0.2"]);
    db.save_proxy_routing_policy(Consumer::Server(1), &only_through(listing, proxy.id))
        .unwrap();
    let mut emptied = db
        .list_proxy_profiles()
        .unwrap()
        .into_iter()
        .find(|saved| saved.id == listing)
        .unwrap();
    emptied.dns_servers.clear();
    emptied.revision += 1;
    assert_eq!(
        refused(db.save_proxy_profile(&emptied)),
        "DNS_PROFILE_NEEDS_TUNNEL_DNS"
    );
}

// Routes on different DNS profiles through one WireGuard proxy whose
// endpoint is a name all save: the proxy's own profile finds its address,
// so how each route names what it dials does not touch the session.
#[test]
fn routes_on_different_profiles_through_one_named_proxy_all_save() {
    let db = Database::open_in_memory().unwrap();
    let tunnel = wireguard(&db, "vpn.example", &["10.64.0.1"]);
    let list = plain_list(&db, "Listed", &["192.0.2.53:53"]);
    let listed = weaver_on(&db, "Listed", list.id, DnsTrust::AsSent);
    let validating = weaver_on(&db, "Validating", list.id, DnsTrust::Validate);
    let proxy = db.save_dns_profile(proxy_answers(false), true).unwrap();
    for (consumer, profile) in [
        (Consumer::Server(1), SYSTEM_DNS_PROFILE_ID),
        (Consumer::Server(2), proxy.id),
        (Consumer::Rss(1), listed.id),
        (Consumer::Rss(2), validating.id),
    ] {
        db.save_proxy_routing_policy(consumer, &only_through(tunnel, profile))
            .unwrap();
    }
}

// A proxy name profile, on which Weaver asks `list` from the egress.
fn proxy_name_on(db: &Database, list: u32) -> DnsProfile {
    db.save_dns_profile(
        DnsProfile {
            kind: DnsProfileKind::ProxyName,
            name: "Proxy address".into(),
            who: DnsWho::Weaver,
            server_list_id: Some(list),
            transit: Some(crate::proxies::DnsTransit::Egress),
            ..proxy_answers(false)
        },
        true,
    )
    .unwrap()
}

// A route selects System or a route profile, a proxy System or a proxy name
// profile; anything else is refused naming the kind expected. A profile a
// proxy uses is not deleted, and the refusal names the proxy.
#[test]
fn routes_and_proxies_select_only_profiles_of_their_kind() {
    let db = Database::open_in_memory().unwrap();
    let proxy = socks(&db);
    let list = plain_list(&db, "Listed", &["192.0.2.53:53"]);
    let route_profile = weaver_on(&db, "Listed", list.id, DnsTrust::AsSent);
    let name_profile = proxy_name_on(&db, list.id);
    assert_eq!(name_profile.kind, DnsProfileKind::ProxyName);

    let StateError::Refused(refusal) = db
        .save_proxy_routing_policy(Consumer::Server(1), &only_through(proxy, name_profile.id))
        .unwrap_err()
    else {
        panic!("not a refusal");
    };
    assert_eq!(refusal.code, "DNS_PROFILE_KIND");
    assert_eq!(
        serde_json::Value::Object(refusal.params),
        serde_json::json!({ "expected": "ROUTE" })
    );
    db.save_proxy_routing_policy(Consumer::Server(1), &only_through(proxy, route_profile.id))
        .unwrap();

    let mut saved = db.list_proxy_profiles().unwrap().remove(0);
    saved.dns_profile_id = route_profile.id;
    let StateError::Refused(refusal) = db.save_proxy_profile(&saved).unwrap_err() else {
        panic!("not a refusal");
    };
    assert_eq!(refusal.code, "DNS_PROFILE_KIND");
    assert_eq!(
        serde_json::Value::Object(refusal.params),
        serde_json::json!({ "expected": "PROXY_NAME" })
    );
    saved.dns_profile_id = name_profile.id;
    db.save_proxy_profile(&saved).unwrap();
    assert_eq!(
        db.list_proxy_profiles().unwrap()[0].dns_profile_id,
        name_profile.id
    );
    assert_eq!(db.dns_profile_proxy_counts().unwrap()[&name_profile.id], 1);

    // The kind stays as created.
    assert_eq!(
        refused(db.save_dns_profile(
            DnsProfile {
                kind: DnsProfileKind::Route,
                ..name_profile.clone()
            },
            false
        )),
        "DNS_PROFILE_KIND_IMMUTABLE"
    );
    let StateError::Refused(refusal) = db.delete_dns_profile(name_profile.id).unwrap_err() else {
        panic!("not a refusal");
    };
    assert_eq!(refusal.code, "DNS_PROFILE_IN_USE");
    assert_eq!(
        serde_json::Value::Object(refusal.params),
        serde_json::json!({ "routes": [], "proxies": [proxy] })
    );
}

// A proxy name profile is answered by the system or by listed servers asked
// from the egress, never by the proxy and never through it.
#[test]
fn a_proxy_name_profile_never_has_the_proxy_answer() {
    let db = Database::open_in_memory().unwrap();
    let list = plain_list(&db, "Listed", &["192.0.2.53:53"]);
    let name_profile = DnsProfile {
        kind: DnsProfileKind::ProxyName,
        name: "Listed".into(),
        who: DnsWho::Weaver,
        server_list_id: Some(list.id),
        transit: Some(crate::proxies::DnsTransit::Egress),
        ..proxy_answers(false)
    };
    for profile in [
        DnsProfile {
            id: 0,
            ..proxy_answers(false)
        },
        DnsProfile {
            id: 0,
            transit: Some(crate::proxies::DnsTransit::Proxy),
            ..name_profile.clone()
        },
    ] {
        assert_eq!(
            refused(db.save_dns_profile(
                DnsProfile {
                    kind: DnsProfileKind::ProxyName,
                    ..profile
                },
                true
            )),
            "DNS_PROFILE_PROXY_NAME_INVALID"
        );
    }
}

// A list of `addresses`, each asked in plain.
fn plain_list(db: &Database, name: &str, addresses: &[&str]) -> DnsServerList {
    db.save_dns_server_list(
        DnsServerList {
            id: 0,
            name: name.into(),
            servers: addresses
                .iter()
                .map(|address| ListedServer {
                    address: address.parse().unwrap(),
                    wire: ListedWire::Plain,
                })
                .collect(),
        },
        true,
    )
    .unwrap()
}

// A profile asking `list` over the egress, judged by `trust`.
fn weaver_on(db: &Database, name: &str, list: u32, trust: DnsTrust) -> DnsProfile {
    db.save_dns_profile(
        DnsProfile {
            name: name.into(),
            who: DnsWho::Weaver,
            server_list_id: Some(list),
            transit: Some(crate::proxies::DnsTransit::Egress),
            trust,
            ..proxy_answers(false)
        },
        true,
    )
    .unwrap()
}

// Points stored route `consumer` at `profile`, past every save check, as a
// store written by an older build could hold it.
fn store_profile(db: &Database, consumer: &str, profile: u32) {
    let datastore = db.datastore();
    let consumer = consumer.to_string();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&datastore, "store_route_profile", |tx| {
            let consumer = consumer.clone();
            Box::pin(async move {
                tx.execute(
                    "UPDATE proxy_routes SET dns_profile_id = {} WHERE consumer = {}",
                    &[SqlArg::I64(i64::from(profile)), SqlArg::Text(consumer)],
                )
                .await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();
}

// A fault a store holds, which no save lets through, refuses only the saves
// that touch it: a route, profile or list it does not involve still saves.
#[test]
fn a_stored_fault_refuses_only_the_saves_that_touch_it() {
    let db = Database::open_in_memory().unwrap();
    let silent = wireguard(&db, "192.0.2.9", &[]);
    let other = socks(&db);
    let proxy = db.save_dns_profile(proxy_answers(false), true).unwrap();
    let unrelated_list = plain_list(&db, "Unrelated", &["192.0.2.60:53"]);
    let unrelated = weaver_on(&db, "Unrelated", unrelated_list.id, DnsTrust::AsSent);
    db.save_proxy_routing_policy(
        Consumer::Server(1),
        &only_through(silent, SYSTEM_DNS_PROFILE_ID),
    )
    .unwrap();
    // The proxy answering at a WireGuard proxy that lists no DNS servers.
    store_profile(&db, "server:1", proxy.id);

    // An unrelated usenet route, its profile and its list still save.
    db.save_proxy_routing_policy(Consumer::Server(3), &only_through(other, unrelated.id))
        .unwrap();
    db.save_dns_profile(
        DnsProfile {
            name: "Unrelated again".into(),
            ..unrelated.clone()
        },
        false,
    )
    .unwrap();
    db.save_dns_server_list(
        DnsServerList {
            name: "Unrelated again".into(),
            ..unrelated_list
        },
        false,
    )
    .unwrap();

    // A save of the route, its profile or its proxy is refused.
    assert_eq!(
        refused(db.save_proxy_routing_policy(Consumer::Server(1), &only_through(silent, proxy.id))),
        "DNS_PROFILE_NEEDS_TUNNEL_DNS"
    );
    assert_eq!(
        refused(db.save_dns_profile(
            DnsProfile {
                name: "Proxy again".into(),
                ..proxy.clone()
            },
            false
        )),
        "DNS_PROFILE_NEEDS_TUNNEL_DNS"
    );
    let mut tunnel = db
        .list_proxy_profiles()
        .unwrap()
        .into_iter()
        .find(|saved| saved.id == silent)
        .unwrap();
    tunnel.revision += 1;
    tunnel.name = "renamed".into();
    assert_eq!(
        refused(db.save_proxy_profile(&tunnel)),
        "DNS_PROFILE_NEEDS_TUNNEL_DNS"
    );
    // Putting the stored route right saves.
    db.save_proxy_routing_policy(
        Consumer::Server(1),
        &only_through(silent, SYSTEM_DNS_PROFILE_ID),
    )
    .unwrap();
}

// A proxy named by a host name on the host's profile, carried in a chain by a
// WireGuard proxy with no tunnel DNS, has no one to resolve its address: the
// route is refused naming the carrier. At an address, or on a proxy name
// profile, it saves.
#[test]
fn a_named_proxy_carried_by_a_wireguard_proxy_with_no_dns_is_refused_naming_the_carrier() {
    let db = Database::open_in_memory().unwrap();
    let carrier = wireguard(&db, "192.0.2.9", &[]);
    let carried = socks(&db);
    let chain = |profile: u32| {
        let mut route = only_through(carrier, profile);
        route.legs[0].path = LegPath::Ladder {
            rungs: vec![Rung::Chain {
                ids: vec![carrier, carried],
            }],
            direct_fallback: false,
        };
        route
    };
    let StateError::Refused(refusal) = db
        .save_proxy_routing_policy(Consumer::Server(1), &chain(0))
        .unwrap_err()
    else {
        panic!("not a refusal");
    };
    assert_eq!(refusal.code, "DNS_PROFILE_NEEDS_TUNNEL_DNS");
    assert_eq!(
        serde_json::Value::Object(refusal.params),
        serde_json::json!({ "proxies": [carrier], "routes": ["server:1"] })
    );

    let list = plain_list(&db, "Listed", &["192.0.2.53:53"]);
    let name = db
        .save_dns_profile(proxy_name_on(&db, list.id), true)
        .unwrap();
    let mut saved = db
        .list_proxy_profiles()
        .unwrap()
        .into_iter()
        .find(|proxy| proxy.id == carried)
        .unwrap();
    saved.dns_profile_id = name.id;
    db.save_proxy_profile(&saved).unwrap();
    db.save_proxy_routing_policy(Consumer::Server(1), &chain(0))
        .unwrap();

    // Back on the host's profile, at an address, it saves too.
    saved.dns_profile_id = 0;
    saved.host = "192.0.2.80".into();
    saved.revision += 1;
    db.save_proxy_profile(&saved).unwrap();
}

// A read of one kind is System, then the saved profiles of that kind by id;
// a read of every kind is the same profiles in the same order.
#[test]
fn profiles_of_one_kind_are_system_then_that_kind() {
    let db = Database::open_in_memory().unwrap();
    let list = plain_list(&db, "Listed", &["192.0.2.53:53"]);
    let first_name = proxy_name_on(&db, list.id).id;
    let route = weaver_on(&db, "Route", list.id, DnsTrust::AsSent).id;
    let second_name = proxy_name_on(&db, list.id).id;
    let ids = |kind: Option<DnsProfileKind>| {
        db.list_dns_profiles_of(kind)
            .unwrap()
            .into_iter()
            .map(|profile| profile.id)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        ids(None),
        vec![SYSTEM_DNS_PROFILE_ID, first_name, route, second_name]
    );
    assert_eq!(
        db.list_dns_profiles().unwrap(),
        db.list_dns_profiles_of(None).unwrap()
    );
    assert_eq!(
        ids(Some(DnsProfileKind::Route)),
        vec![SYSTEM_DNS_PROFILE_ID, route]
    );
    assert_eq!(
        ids(Some(DnsProfileKind::ProxyName)),
        vec![SYSTEM_DNS_PROFILE_ID, first_name, second_name]
    );
}
