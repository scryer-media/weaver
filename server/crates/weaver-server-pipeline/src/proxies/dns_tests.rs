// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::proxies::{NetworkRefusal as R, ProxyKind, ResolvedBy};

fn list(wires: Vec<ListedWire>) -> DnsServerList {
    DnsServerList {
        id: 1,
        name: "Public".into(),
        servers: wires
            .into_iter()
            .enumerate()
            .map(|(index, wire)| ListedServer {
                address: SocketAddr::new([192, 0, 2, 53 + index as u8].into(), 853),
                wire,
            })
            .collect(),
    }
}

fn plain() -> ListedWire {
    ListedWire::Plain
}

fn tls() -> ListedWire {
    ListedWire::Tls {
        name: Some("dns.example".into()),
    }
}

fn https() -> ListedWire {
    ListedWire::Https {
        path: "/dns-query".into(),
    }
}

fn checked(wires: Vec<ListedWire>) -> Result<(), R> {
    list(wires).validate()
}

fn profile(who: DnsWho, trust: DnsTrust, list: Option<u32>) -> DnsProfile {
    DnsProfile {
        kind: crate::proxies::DnsProfileKind::Route,
        id: 1,
        name: "Profile".into(),
        who,
        transit: None,
        trust,
        server_list_id: list,
        system_fallback: false,
    }
}

// A lookup never goes out in clear once an encrypted server failed it.
#[test]
fn a_plain_server_may_lead_but_never_follow_an_encrypted_one() {
    assert_eq!(
        checked(vec![tls(), plain()]),
        Err(R::ProxyDnsPlainAfterEncrypted)
    );
    assert_eq!(
        checked(vec![https(), plain()]),
        Err(R::ProxyDnsPlainAfterEncrypted)
    );
    assert_eq!(
        checked(vec![plain(), tls(), plain()]),
        Err(R::ProxyDnsPlainAfterEncrypted)
    );
    assert_eq!(checked(vec![plain(), tls()]), Ok(()));
    assert_eq!(checked(vec![plain(), plain()]), Ok(()));
    assert_eq!(checked(vec![tls(), https()]), Ok(()));
}

#[test]
fn a_server_list_holds_one_to_ten_well_formed_servers() {
    assert_eq!(checked(vec![]), Err(R::ProxyDnsServersRequired));
    assert_eq!(checked(vec![plain(); MAX_DNS_SERVERS]), Ok(()));
    assert_eq!(checked(vec![plain(); 10]), Ok(()));
    assert_eq!(checked(vec![plain(); 11]), Err(R::ProxyDnsServerCount));
    // Ten encrypted servers after a plain one are eleven.
    let mut wires = vec![plain()];
    wires.extend(vec![tls(); 10]);
    assert_eq!(checked(wires), Err(R::ProxyDnsServerCount));

    let mut zero = list(vec![plain()]);
    zero.servers[0].address.set_port(0);
    assert_eq!(zero.validate(), Err(R::ProxyDnsServerInvalid));

    let tls_named = |name: &str| ListedWire::Tls {
        name: Some(name.into()),
    };
    assert_eq!(checked(vec![ListedWire::Tls { name: None }]), Ok(()));
    assert_eq!(checked(vec![tls_named("dns.example.")]), Ok(()));
    for bad in [
        "",
        "not a name",
        "-dns.example",
        "dns..example",
        "dns\u{e9}.example",
    ] {
        assert_eq!(
            checked(vec![tls_named(bad)]),
            Err(R::ProxyDnsTlsNameInvalid),
            "{bad:?}"
        );
    }
    assert_eq!(checked(vec![tls_named(&["a"; 127].join("."))]), Ok(()));
    assert_eq!(
        checked(vec![tls_named(&["a"; 128].join("."))]),
        Err(R::ProxyDnsTlsNameInvalid)
    );

    let path = |path: &str| ListedWire::Https { path: path.into() };
    assert_eq!(checked(vec![path("/dns-query?ct=1")]), Ok(()));
    assert_eq!(
        checked(vec![path(&format!("/{}", "a".repeat(2047)))]),
        Ok(())
    );
    for bad in [
        String::new(),
        "dns-query".into(),
        "/dns query".into(),
        "/dns\tquery".into(),
        "/dns-query#top".into(),
        "/d\u{e9}".into(),
        format!("/{}", "a".repeat(2048)),
    ] {
        assert_eq!(
            checked(vec![path(&bad)]),
            Err(R::ProxyDnsUrlInvalid),
            "{bad:?}"
        );
    }
}

#[test]
fn a_list_has_a_name_of_one_to_128_bytes() {
    let mut named = list(vec![plain()]);
    named.name = " ".into();
    assert_eq!(named.validate(), Err(R::DnsListNameLength));
    named.name = "a".repeat(129);
    assert_eq!(named.validate(), Err(R::DnsListNameLength));
    named.name = "a".repeat(128);
    assert_eq!(named.validate(), Ok(()));
    named.name = "  Public  ".into();
    assert_eq!(named.normal().name, "Public");
}

// One address, port and wire, with one TLS name or HTTPS path, is one
// server, and a list holds it once. The same address over another wire,
// port, name or path is another server.
#[test]
fn a_server_is_listed_once_and_another_wire_is_another_server() {
    let at = |port, wire| ListedServer {
        address: SocketAddr::new([192, 0, 2, 53].into(), port),
        wire,
    };
    let name = |name: &str| ListedWire::Tls {
        name: Some(name.into()),
    };
    let path = |path: &str| ListedWire::Https { path: path.into() };
    let checked = |servers: Vec<ListedServer>| {
        DnsServerList {
            servers,
            ..list(vec![])
        }
        .validate()
    };
    let repeated = [
        vec![at(53, plain()), at(53, plain())],
        vec![at(53, plain()), at(853, tls()), at(853, tls())],
        vec![at(853, tls()), at(443, https()), at(853, tls())],
        vec![at(443, https()), at(443, https())],
    ];
    for servers in repeated {
        assert_eq!(checked(servers), Err(R::ProxyDnsServerDuplicate));
    }
    let distinct = [
        vec![at(53, plain()), at(53, tls())],
        vec![at(53, plain()), at(5353, plain())],
        vec![at(853, name("a.example")), at(853, name("b.example"))],
        vec![
            at(853, ListedWire::Tls { name: None }),
            at(853, name("a.example")),
        ],
        vec![at(443, path("/dns-query")), at(443, path("/other"))],
    ];
    for servers in distinct {
        assert_eq!(checked(servers), Ok(()));
    }
}

// A DoH path is one the request can be built with: every path the check
// accepts is an HTTP path and query, and every character a URI refuses
// there is refused here.
#[test]
fn a_doh_path_holds_only_what_an_http_request_carries() {
    let every: String = (b'!'..=b'~').map(char::from).collect();
    let allowed: String = every
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || "-._~!$&'()*+,;=:@/%?".contains(*c))
        .collect();
    for accepted in [
        "/".to_owned(),
        "/dns-query".to_owned(),
        "/dns-query?ct=application/dns-message&x=1".to_owned(),
        format!("/{allowed}"),
        format!("/{}", "a".repeat(MAX_HTTPS_PATH_BYTES - 1)),
    ] {
        assert!(is_https_path(&accepted), "{accepted}");
        let uri: http::Uri = accepted.parse().unwrap();
        assert_eq!(uri.path_and_query().unwrap().as_str(), accepted);
    }
    for refused in "<>`\"{}|\\^[] #".chars() {
        let path = format!("/a{refused}b");
        assert!(!is_https_path(&path), "{path}");
    }
    for byte in every.chars().filter(|c| !allowed.contains(*c)) {
        let path = format!("/a{byte}b");
        assert!(!is_https_path(&path), "{path}");
    }
    assert!(!is_https_path("dns-query"));
    assert!(!is_https_path(""));
    assert!(!is_https_path(&format!(
        "/{}",
        "a".repeat(MAX_HTTPS_PATH_BYTES)
    )));
    assert!(!is_https_path("/a\u{e9}"));
}

// Each answerer takes only the trusts it can keep: the system's answers
// come as sent; the proxy's come as sent, or validated here when a
// WireGuard tunnel's servers are asked; Weaver's servers take either.
#[test]
fn who_answers_decides_the_trust_a_profile_may_take() {
    for (who, trust, ok) in [
        (DnsWho::System, DnsTrust::AsSent, true),
        (DnsWho::System, DnsTrust::Validate, false),
        (DnsWho::Proxy, DnsTrust::AsSent, true),
        (DnsWho::Proxy, DnsTrust::Validate, true),
        (DnsWho::Weaver, DnsTrust::AsSent, true),
        (DnsWho::Weaver, DnsTrust::Validate, true),
    ] {
        let list = (who == DnsWho::Weaver).then_some(1);
        assert_eq!(
            profile(who, trust, list).validate(),
            if ok {
                Ok(())
            } else {
                Err(R::DnsProfileTrustInvalid)
            },
            "{who:?} {trust:?}"
        );
    }
    assert_eq!(
        profile(DnsWho::Weaver, DnsTrust::AsSent, None).validate(),
        Err(R::DnsProfileListRequired)
    );
    let mut named = profile(DnsWho::System, DnsTrust::AsSent, None);
    named.name = String::new();
    assert_eq!(named.validate(), Err(R::DnsProfileNameLength));
    named.name = "a".repeat(129);
    assert_eq!(named.validate(), Err(R::DnsProfileNameLength));
}

// What only Weaver's servers use is dropped for any other answerer, and
// Weaver asks from the egress unless told otherwise.
#[test]
fn a_profile_is_stored_in_one_form() {
    let mut loose = profile(DnsWho::Proxy, DnsTrust::AsSent, Some(3));
    loose.transit = Some(DnsTransit::Proxy);
    let normal = loose.normal();
    assert_eq!(normal.transit, None);
    assert_eq!(normal.server_list_id, None);
    let weaver = profile(DnsWho::Weaver, DnsTrust::Validate, Some(3)).normal();
    assert_eq!(weaver.transit, Some(DnsTransit::Egress));
    assert_eq!(weaver.server_list_id, Some(3));
    assert!(DnsProfile::system().is_built_in());
    assert_eq!(DnsProfile::system().validate(), Ok(()));
}

// A profile reaches the engine as the choice it makes: its list's servers,
// in order and on their wires, and its transit and trust.
#[test]
fn a_profile_reaches_the_engine_with_its_list() {
    let lists = HashMap::from([(1, list(vec![plain(), tls(), https()]))]);
    let mut weaver = profile(DnsWho::Weaver, DnsTrust::Validate, Some(1)).normal();
    weaver.transit = Some(DnsTransit::Proxy);
    let DnsChoice::Weaver {
        servers,
        trust,
        transit,
    } = weaver.choice(&lists)
    else {
        panic!("Weaver's servers answer");
    };
    assert_eq!(trust, Trust::Validate);
    assert_eq!(transit, Transit::Proxy);
    assert_eq!(servers.len(), 3);
    assert_eq!(
        servers[0],
        DnsServer {
            address: "192.0.2.53:853".parse().unwrap(),
            wire: Wire::Plain,
        }
    );
    assert_eq!(
        servers[2].wire,
        Wire::Https {
            path: "/dns-query".into()
        }
    );
    // A list that is gone answers nothing, and never falls back to the host.
    let gone = profile(DnsWho::Weaver, DnsTrust::AsSent, Some(9)).normal();
    assert!(matches!(gone.choice(&lists), DnsChoice::Refused(_)));
    assert_eq!(DnsProfile::system().choice(&lists), DnsChoice::System);
    assert_eq!(
        profile(DnsWho::Proxy, DnsTrust::Validate, None).choice(&lists),
        DnsChoice::Proxy {
            trust: Trust::Validate,
            system_fallback: false,
        }
    );
    let fallback = DnsProfile {
        system_fallback: true,
        ..profile(DnsWho::Proxy, DnsTrust::AsSent, None)
    };
    assert_eq!(
        fallback.choice(&lists),
        DnsChoice::Proxy {
            trust: Trust::AsSent,
            system_fallback: true,
        }
    );
}

// The fallback means something only where the proxy answers; any other
// profile is saved without it.
#[test]
fn only_the_proxy_answering_keeps_the_fallback() {
    for who in [DnsWho::System, DnsWho::Proxy, DnsWho::Weaver] {
        let saved = DnsProfile {
            system_fallback: true,
            ..profile(who, DnsTrust::AsSent, Some(1))
        }
        .normal();
        assert_eq!(saved.system_fallback, who == DnsWho::Proxy, "{who:?}");
    }
}

#[test]
fn a_profile_says_who_resolves_a_destination() {
    let weaver = |transit| DnsProfile {
        transit: Some(transit),
        ..profile(DnsWho::Weaver, DnsTrust::AsSent, Some(1))
    };
    let socks = Some(ProxyKind::Socks5);
    let wireguard = Some(ProxyKind::WireGuard);
    assert_eq!(
        DnsProfile::system().resolved_by(socks, false),
        Some(ResolvedBy::Host)
    );
    let proxy = profile(DnsWho::Proxy, DnsTrust::AsSent, None);
    assert_eq!(proxy.resolved_by(socks, false), Some(ResolvedBy::Proxy));
    assert_eq!(
        proxy.resolved_by(wireguard, true),
        Some(ResolvedBy::ProxyServers)
    );
    // A WireGuard proxy that lists no DNS servers answers nothing.
    assert_eq!(proxy.resolved_by(wireguard, false), None);
    // With no proxy, the proxy answering resolves nothing unless the host
    // may answer there.
    assert_eq!(proxy.resolved_by(None, false), None);
    let fallback = DnsProfile {
        system_fallback: true,
        ..proxy.clone()
    };
    assert_eq!(fallback.resolved_by(None, false), Some(ResolvedBy::Host));
    assert_eq!(fallback.resolved_by(socks, false), Some(ResolvedBy::Proxy));
    assert_eq!(
        weaver(DnsTransit::Egress).resolved_by(socks, false),
        Some(ResolvedBy::Servers)
    );
    assert_eq!(
        weaver(DnsTransit::Proxy).resolved_by(socks, false),
        Some(ResolvedBy::ProxyServers)
    );
    // With no proxy, "via the proxy" asks from the egress.
    assert_eq!(
        weaver(DnsTransit::Proxy).resolved_by(None, false),
        Some(ResolvedBy::Servers)
    );
}

// The stored form reads back as written, says its version, and is refused
// when it is too large, of another version, or holds a list a save would
// refuse.
#[test]
fn stored_servers_read_back_and_refuse_what_they_cannot_hold() {
    let servers = list(vec![plain(), tls(), https()]).servers;
    let stored = stored_servers(&servers).unwrap();
    assert!(stored.starts_with("{\"v\":1,"), "{stored}");
    assert_eq!(read_servers(&stored).unwrap(), servers);
    assert!(read_servers("{\"v\":2,\"servers\":[]}").is_err());
    assert!(read_servers("not json").is_err());
    assert!(read_servers("{\"v\":1,\"servers\":[]}").is_err());
    let large = format!(
        "{{\"v\":1,\"pad\":\"{}\"}}",
        "a".repeat(MAX_STORED_SERVERS_BYTES)
    );
    assert!(read_servers(&large).unwrap_err().contains("over the limit"));
    for who in [DnsWho::System, DnsWho::Proxy, DnsWho::Weaver] {
        assert_eq!(who_of(who_text(who)), Some(who));
    }
    for transit in [DnsTransit::Egress, DnsTransit::Proxy] {
        assert_eq!(transit_of(transit_text(transit)), Some(transit));
    }
    for trust in [DnsTrust::AsSent, DnsTrust::Validate] {
        assert_eq!(trust_of(trust_text(trust)), Some(trust));
    }
    // The trust an unreleased build could store, and this one cannot.
    assert_eq!(trust_of("require_ad"), None);
    assert_eq!(who_of("host"), None);
}
