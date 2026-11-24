// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_core::proxies::ProxiesDnsPersistenceQueries;
use weaver_server_core::proxies::ProxiesPersistenceQueries;
use weaver_server_core::rss::RssQueries;
mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use weaver_server_api::auth::CallerScope;
use weaver_server_core::proxies::ProxyRuntime;

async fn harness() -> TestHarness {
    let h = TestHarness::new().await;
    h.handle.set_proxy_runtime(
        ProxyRuntime::new(h.db.clone(), tokio::runtime::Handle::current()).unwrap(),
    );
    h
}

#[tokio::test]
async fn profile_crud_redacts_secrets_and_preserves_omitted_credentials() {
    let h = harness().await;
    let create = r#"mutation { saveProxyProfile(input: {
        name: "gateway", kind: SOCKS5, enabled: true, host: "127.0.0.1", port: 1080,
        username: "private-user", password: "private-password"
    }) { id kind hasUsername hasPassword } }"#;
    assert_has_errors(&h.execute_as(create, CallerScope::Control).await);
    let result = h.execute(create).await;
    assert_no_errors(&result);
    let data = response_data(&result);
    assert_eq!(data["saveProxyProfile"]["kind"], "SOCKS5");
    assert_eq!(data["saveProxyProfile"]["hasPassword"], true);
    let id = data["saveProxyProfile"]["id"].as_u64().unwrap();
    let result = h
        .execute(&format!(
            r#"mutation {{ saveProxyProfile(id: {id}, input: {{
        name: "renamed", kind: SOCKS5, enabled: false, host: "127.0.0.1", port: 1080
    }}) {{ id enabled hasUsername hasPassword }} }}"#
        ))
        .await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["saveProxyProfile"]["hasPassword"],
        true
    );
    assert_eq!(
        h.db.list_proxy_profiles().unwrap()[0]
            .secrets
            .password
            .as_deref(),
        Some("private-password")
    );
    let result = h
        .execute("{ proxyProfiles { id name host hasPassword hasUsername } }")
        .await;
    assert_no_errors(&result);
    assert!(
        !serde_json::to_string(&response_data(&result))
            .unwrap()
            .contains("private-")
    );
    assert_has_errors(&h.execute("{ proxyProfiles { password } }").await);
    assert_has_errors(
        &h.execute_as("{ proxyProfiles { id } }", CallerScope::Control)
            .await,
    );
    assert_no_errors(
        &h.execute(&format!("mutation {{ deleteProxyProfile(id: {id}) }}"))
            .await,
    );
    assert!(h.db.list_proxy_profiles().unwrap().is_empty());
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

const WIREGUARD_KEYS: &str = r#", tunnelAddresses: ["10.0.0.2/32"],
        peerPublicKey: "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=",
        privateKey: "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=""#;

// A save of a DNS server list holding `servers`, asking for them back.
fn save_dns(id: Option<u64>, servers: &str) -> String {
    let id = id.map_or(String::new(), |id| format!("id: {id}, "));
    format!(
        r#"mutation {{ saveDnsServerList(input: {{ {id}name: "Resolvers", servers: [{servers}] }}) {{ id servers {{ wire address tlsName url }} }} }}"#
    )
}

// The saved list's id, and the list as it came back.
fn saved_dns(result: &async_graphql::Response) -> (u64, serde_json::Value) {
    assert_no_errors(result);
    let data = response_data(result);
    (
        data["saveDnsServerList"]["id"].as_u64().unwrap(),
        data["saveDnsServerList"].clone(),
    )
}

// A WireGuard proxy's tunnel DNS servers are kept by an update that does
// not name them, and any other kind is refused them.
#[tokio::test]
async fn a_wireguard_proxys_dns_servers_are_kept_by_an_update_without_them() {
    let h = harness().await;
    let save = |id: Option<u64>, kind: &str, extra: &str| {
        let id = id.map_or("null".into(), |id| id.to_string());
        format!(
            r#"mutation {{ saveProxyProfile(id: {id}, input: {{
        name: "dns-{kind}", kind: {kind}, enabled: true, host: "127.0.0.1", port: 1080{extra}
    }}) {{ id dnsServers }} }}"#
        )
    };
    let saved = |result: &async_graphql::Response| {
        assert_no_errors(result);
        let data = response_data(result);
        (
            data["saveProxyProfile"]["id"].as_u64().unwrap(),
            data["saveProxyProfile"]["dnsServers"].clone(),
        )
    };
    let with_dns = format!(r#"{WIREGUARD_KEYS}, dnsServers: ["10.0.0.1", "10.0.0.2"]"#);
    let (tunnel, servers) = saved(&h.execute(&save(None, "WIRE_GUARD", &with_dns)).await);
    assert_eq!(servers, serde_json::json!(["10.0.0.1", "10.0.0.2"]));
    let (_, servers) = saved(
        &h.execute(&save(Some(tunnel), "WIRE_GUARD", WIREGUARD_KEYS))
            .await,
    );
    assert_eq!(
        servers,
        serde_json::json!(["10.0.0.1", "10.0.0.2"]),
        "an update without DNS servers keeps the saved ones"
    );
    let refused = h
        .execute(&save(None, "SOCKS5", r#", dnsServers: ["10.0.0.1"]"#))
        .await;
    assert_eq!(
        serde_json::to_value(&refused.errors[0]).unwrap()["extensions"]["code"],
        "PROXY_DNS_SERVER_INVALID"
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// Each wire comes back as the web reads it: the address an IP with its
// port only when it is not the wire's own, and DNS over HTTPS as its URL.
#[tokio::test]
async fn dns_servers_come_back_as_they_were_given() {
    let h = harness().await;
    let servers = r#"
        { wire: PLAIN, address: "192.0.2.53" },
        { wire: PLAIN, address: "192.0.2.54:5353" },
        { wire: TLS, address: "[2001:db8::1]:853", tlsName: " dns.example " },
        { wire: HTTPS, address: "", url: "HTTPS://[2001:db8::2]:8443/dns-query?ct=1" }
    "#;
    let (id, dns) = saved_dns(&h.execute(&save_dns(None, servers)).await);
    assert_eq!(
        dns,
        serde_json::json!({
            "id": id,
            "servers": [
                { "wire": "PLAIN", "address": "192.0.2.53", "tlsName": null, "url": null },
                { "wire": "PLAIN", "address": "192.0.2.54:5353", "tlsName": null, "url": null },
                { "wire": "TLS", "address": "2001:db8::1", "tlsName": "dns.example", "url": null },
                {
                    "wire": "HTTPS", "address": "[2001:db8::2]:8443", "tlsName": null,
                    "url": "https://[2001:db8::2]:8443/dns-query?ct=1"
                },
            ],
        })
    );
    let stored = h.db.list_dns_server_lists().unwrap();
    let stored = stored.iter().find(|list| u64::from(list.id) == id).unwrap();
    assert_eq!(
        serde_json::to_value(&stored.servers).unwrap()[3],
        serde_json::json!({
            "address": "[2001:db8::2]:8443",
            "wire": { "Https": { "path": "/dns-query?ct=1" } }
        })
    );

    let default_port = r#"
        { wire: HTTPS, address: "", url: "https://192.0.2.1/dns-query" },
        { wire: TLS, address: "192.0.2.2", tlsName: "" }
    "#;
    let (_, dns) = saved_dns(&h.execute(&save_dns(Some(id), default_port)).await);
    assert_eq!(dns["servers"][0]["url"], "https://192.0.2.1/dns-query");
    assert_eq!(dns["servers"][0]["address"], "192.0.2.1");
    assert_eq!(dns["servers"][1]["address"], "192.0.2.2");
    assert_eq!(dns["servers"][1]["tlsName"], serde_json::Value::Null);

    // A DNS over HTTPS URL that names a host is dialled at the IP given
    // beside it, at the URL's port, and comes back with both. Without the
    // IP it is refused.
    let named = r#"
        { wire: HTTPS, address: " 2001:db8::3 ", url: "https://dns.example:8443/dns-query" }
    "#;
    let (_, dns) = saved_dns(&h.execute(&save_dns(Some(id), named)).await);
    assert_eq!(
        dns["servers"][0]["url"],
        "https://dns.example:8443/dns-query"
    );
    assert_eq!(dns["servers"][0]["address"], "[2001:db8::3]");
    let stored = h.db.list_dns_server_lists().unwrap();
    let stored = stored.iter().find(|list| u64::from(list.id) == id).unwrap();
    assert_eq!(
        serde_json::to_value(&stored.servers).unwrap()[0],
        serde_json::json!({
            "address": "[2001:db8::3]:8443",
            "wire": { "Https": { "path": "/dns-query", "name": "dns.example" } }
        })
    );
    let refused = h
        .execute(&save_dns(
            Some(id),
            r#"{ wire: HTTPS, address: "", url: "https://dns.example/dns-query" }"#,
        ))
        .await;
    assert_eq!(
        serde_json::to_value(&refused.errors[0]).unwrap()["extensions"]["code"],
        "PROXY_DNS_URL_HOSTNAME"
    );

    // A port is read as a number, leading zeros and all; a bare or bracketed
    // v6 address keeps the wire's port.
    let spellings = r#"
        { wire: PLAIN, address: "192.0.2.53:05353" },
        { wire: PLAIN, address: "[2001:db8::3]:0053" },
        { wire: PLAIN, address: "2001:db8::4" },
        { wire: TLS, address: "[2001:db8::5]" }
    "#;
    let (_, dns) = saved_dns(&h.execute(&save_dns(Some(id), spellings)).await);
    let addresses: Vec<_> = dns["servers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|server| server["address"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        addresses,
        [
            "192.0.2.53:5353",
            "2001:db8::3",
            "2001:db8::4",
            "2001:db8::5"
        ]
    );
    let stored = h.db.list_dns_server_lists().unwrap();
    let stored = stored.iter().find(|list| u64::from(list.id) == id).unwrap();
    assert_eq!(
        stored
            .servers
            .iter()
            .map(|server| server.address.to_string())
            .collect::<Vec<_>>(),
        [
            "192.0.2.53:5353",
            "[2001:db8::3]:53",
            "[2001:db8::4]:53",
            "[2001:db8::5]:853"
        ]
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn a_dns_server_list_no_resolver_could_use_is_refused() {
    let h = harness().await;
    let refused = |result: &async_graphql::Response| {
        assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
        serde_json::to_value(&result.errors[0]).unwrap()["extensions"]["code"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let servers = |list: &str| list.to_owned();
    let plain = r#"{ wire: PLAIN, address: "192.0.2.53" }"#;
    let cases = [
        (servers(""), "PROXY_DNS_SERVERS_REQUIRED"),
        (servers(&[plain; 11].join(", ")), "PROXY_DNS_SERVER_COUNT"),
        (
            servers(r#"{ wire: PLAIN, address: "192.0.2.53:0" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            servers(r#"{ wire: PLAIN, address: "dns.example" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        // Brackets hold a v6 address only, and no address carries a zone,
        // in any form it is written.
        (
            servers(r#"{ wire: PLAIN, address: "[192.0.2.53]" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            servers(r#"{ wire: PLAIN, address: "[192.0.2.53]:53" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            servers(r#"{ wire: TLS, address: "[192.0.2.53]:853" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            servers(r#"{ wire: PLAIN, address: "[fe80::1%3]:53" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            servers(r#"{ wire: PLAIN, address: "[fe80::1%3]" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            servers(r#"{ wire: PLAIN, address: "fe80::1%3" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            servers(r#"{ wire: TLS, address: "[fe80::1%eth0]:853" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            servers(r#"{ wire: TLS, address: "fe80::1%3" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "https://[192.0.2.1]/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "https://[fe80::1%3]/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "https://[fe80::1%253]:443/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(r#"{ wire: TLS, address: "192.0.2.53", tlsName: "not a name" }"#),
            "PROXY_DNS_TLS_NAME_INVALID",
        ),
        (
            servers(&format!(
                r#"{{ wire: TLS, address: "192.0.2.53", tlsName: "{}" }}"#,
                ["a"; 128].join(".")
            )),
            "PROXY_DNS_TLS_NAME_INVALID",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "https://dns.example/dns-query" }"#),
            "PROXY_DNS_URL_HOSTNAME",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "http://192.0.2.1/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "https://192.0.2.1" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "https://user@192.0.2.1/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "https://192.0.2.1/dns-query#top" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "https://192.0.2.1:0/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "https://192.0.2.1/dns query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(r#"{ wire: HTTPS, address: "", url: "https://192.0.2.53/a<b" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(&format!(
                r#"{{ wire: HTTPS, address: "", url: "https://192.0.2.1/{}" }}"#,
                "a".repeat(2048)
            )),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            servers(&format!(
                r#"{{ wire: TLS, address: "192.0.2.53" }}, {plain}"#
            )),
            "PROXY_DNS_PLAIN_AFTER_ENCRYPTED",
        ),
        // One server named twice, once with its port spelled out.
        (
            servers(&format!(
                r#"{plain}, {{ wire: PLAIN, address: "192.0.2.53:53" }}"#
            )),
            "PROXY_DNS_SERVER_DUPLICATE",
        ),
        (
            servers(
                r#"{ wire: TLS, address: "192.0.2.53", tlsName: "dns.example" }, { wire: TLS, address: "192.0.2.53:853", tlsName: "dns.example" }"#,
            ),
            "PROXY_DNS_SERVER_DUPLICATE",
        ),
        (
            servers(
                r#"{ wire: HTTPS, address: "", url: "https://192.0.2.53/dns-query" }, { wire: HTTPS, address: "", url: "https://192.0.2.53:443/dns-query" }"#,
            ),
            "PROXY_DNS_SERVER_DUPLICATE",
        ),
    ];
    for (extra, code) in cases {
        assert_eq!(
            refused(&h.execute(&save_dns(None, &extra)).await),
            code,
            "{extra}"
        );
    }
    assert!(h.db.list_dns_server_lists().unwrap().is_empty());

    // The most servers a list holds are taken.
    let ten = (1..=10)
        .map(|n| format!(r#"{{ wire: PLAIN, address: "192.0.2.{n}" }}"#))
        .collect::<Vec<_>>()
        .join(", ");
    let (_, dns) = saved_dns(&h.execute(&save_dns(None, &ten)).await);
    assert_eq!(dns["servers"].as_array().unwrap().len(), 10);
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// A WireGuard proxy keeps NAT-PMP or a forwarded port, never both; NAT-PMP
// wins. Any other proxy keeps neither, and a forwarded port of 0 is none.
#[tokio::test]
async fn a_wireguard_proxy_keeps_nat_pmp_or_a_forwarded_port() {
    let h = harness().await;
    let save = |kind: &str, forwarding: &str| {
        let wireguard = if kind == "WIRE_GUARD" {
            r#", tunnelAddresses: ["10.2.0.2/32"],
        peerPublicKey: "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=",
        privateKey: "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=", dnsServers: ["10.2.0.1"]"#
        } else {
            ""
        };
        format!(
            r#"mutation {{ saveProxyProfile(input: {{
        name: "forwarding-{kind}", kind: {kind}, enabled: true, host: "127.0.0.1",
        port: 1080{wireguard}{forwarding}
    }}) {{ natpmp forwardedPort }} }}"#
        )
    };
    let saved = |result: &async_graphql::Response| {
        assert_no_errors(result);
        let data = response_data(result);
        (
            data["saveProxyProfile"]["natpmp"].as_bool().unwrap(),
            data["saveProxyProfile"]["forwardedPort"].as_u64(),
        )
    };
    let cases = [
        ("WIRE_GUARD", "", (false, None)),
        ("WIRE_GUARD", ", forwardedPort: 51413", (false, Some(51413))),
        ("WIRE_GUARD", ", forwardedPort: 0", (false, None)),
        ("WIRE_GUARD", ", natpmp: true", (true, None)),
        (
            "WIRE_GUARD",
            ", natpmp: true, forwardedPort: 51413",
            (true, None),
        ),
        (
            "SOCKS5",
            ", natpmp: true, forwardedPort: 51413",
            (false, None),
        ),
    ];
    for (kind, forwarding, expected) in cases {
        assert_eq!(
            saved(&h.execute(&save(kind, forwarding)).await),
            expected,
            "{kind}{forwarding}"
        );
    }
    let stored = h.db.list_proxy_profiles().unwrap();
    assert_eq!(stored[1].forwarded_port, Some(51413));
    assert!(stored[3].natpmp);
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn an_ssh_profile_is_refused_a_password() {
    let h = harness().await;
    let result = h
        .execute(
            r#"mutation { saveProxyProfile(input: {
        name: "seedbox", kind: SSH, enabled: true, host: "127.0.0.1", port: 22,
        username: "operator", password: "fixture-password"
    }) { id } }"#,
        )
        .await;
    assert_has_errors(&result);
    assert!(
        result.errors[0]
            .message
            .contains("SSH requires an Ed25519 private key and takes no password")
    );
    assert!(h.db.list_proxy_profiles().unwrap().is_empty());
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn http3_profiles_round_trip_and_feed_routes_keep_credentials_private() {
    let h = harness().await;
    let result = h
        .execute(
            r#"mutation { saveProxyProfile(input: {
        name: "QUIC gateway", kind: HTTP3_CONNECT, enabled: true,
        host: "proxy.invalid", port: 443,
        username: "fixture-user", password: "fixture-password"
    }) { id kind hasUsername hasPassword } }"#,
        )
        .await;
    assert_no_errors(&result);
    let data = response_data(&result);
    assert_eq!(data["saveProxyProfile"]["kind"], "HTTP3_CONNECT");
    assert_eq!(data["saveProxyProfile"]["hasPassword"], true);
    assert!(!data.to_string().contains("fixture-password"));
    let id = data["saveProxyProfile"]["id"].as_u64().unwrap();
    let feed = h
        .execute(&format!(
            r#"mutation {{ addRssFeed(input: {{
        name: "QUIC feed", url: "https://feed.invalid/rss", enabled: false,
        routing: {{ proxyIds: [{id}], allowDirect: false }}
    }}) {{ routing {{ proxyIds allowDirect }} }} }}"#
        ))
        .await;
    assert_no_errors(&feed);
    assert_eq!(
        response_data(&feed)["addRssFeed"]["routing"]["allowDirect"],
        false
    );
    let result = h
        .execute(&format!(
            r#"mutation {{ saveProxyProfile(id: {id}, input: {{
        name: "renamed", kind: HTTP3_CONNECT, enabled: false,
        host: "proxy.invalid", port: 443
    }}) {{ kind enabled hasUsername hasPassword }} }}"#
        ))
        .await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["saveProxyProfile"]["hasPassword"],
        true
    );
    assert_eq!(
        h.db.list_proxy_profiles().unwrap()[0]
            .secrets
            .password
            .as_deref(),
        Some("fixture-password")
    );
    assert_has_errors(
        &h.execute(&format!("mutation {{ deleteProxyProfile(id: {id}) }}"))
            .await,
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// A test goes through a route drafted for it, answers each path with a code
// the client words, and dials nothing it cannot use: an HTTP/3 proxy is
// tested through a DNS server it resolves with.
#[tokio::test]
async fn a_proxy_test_answers_with_a_code() {
    let h = harness().await;
    let result = h
        .execute(
            r#"mutation { saveProxyProfile(input: {
        name: "QUIC gateway", kind: HTTP3_CONNECT, enabled: true,
        host: "proxy.invalid", port: 443
    }) { id } }"#,
        )
        .await;
    assert_no_errors(&result);
    let id = response_data(&result)["saveProxyProfile"]["id"]
        .as_u64()
        .unwrap();
    let test_via = |subject: &str| {
        format!(
            r#"mutation {{ testVia(
        route: {{ legs: [{{ egressId: 0, weight: 100, path: {{ direct: true }} }}] }}{subject}
    ) {{ leg egressId carrierIds proxyId success code message params failingProxyId }} }}"#
        )
    };
    let refused = |result: &async_graphql::Response| {
        assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
        serde_json::to_value(&result.errors[0]).unwrap()["extensions"]["code"].clone()
    };
    let subject = format!(", subject: {{ proxy: {id} }}");
    assert_has_errors(
        &h.execute_as(&test_via(&subject), CallerScope::Control)
            .await,
    );
    let result = h.execute(&test_via(&subject)).await;
    assert_no_errors(&result);
    let tests = response_data(&result)["testVia"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(tests.len(), 1);
    let test = &tests[0];
    assert_eq!(test["leg"], 0);
    assert_eq!(test["egressId"], 0);
    assert_eq!(test["carrierIds"], serde_json::json!([]));
    assert_eq!(test["proxyId"], id);
    assert_eq!(test["success"], false);
    assert_eq!(test["code"], "TEST_NEEDS_DNS");
    assert_eq!(test["params"], serde_json::json!({}));
    assert!(!test["message"].as_str().unwrap().is_empty());
    assert_eq!(
        refused(&h.execute(&test_via(", subject: { proxy: 999 }")).await),
        "PROXY_NOT_FOUND"
    );
    assert_eq!(
        refused(&h.execute(&test_via("")).await),
        "TEST_TARGET_REQUIRED"
    );
    // A test that names no route is refused.
    assert_eq!(
        refused(
            &h.execute(&format!(
                "mutation {{ testProxyProfile(id: {id}) {{ success }} }}"
            ))
            .await
        ),
        "TEST_NEEDS_ROUTE"
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// Each path says how the host it dialled was resolved, what the name
// resolved to even when the connect then failed, and the codes a failed
// lookup is reported with.
#[tokio::test]
async fn a_test_says_how_its_host_was_resolved() {
    let h = harness().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let test_via = |host: &str| {
        format!(
            r#"mutation {{ testVia(
        route: {{ legs: [{{ egressId: 0, weight: 100, path: {{ direct: true }} }}] }},
        target: {{ host: "{host}", port: {port} }}
    ) {{ success resolution {{
        host resolvedBy proxyId dnsServers addresses cached connectedAddress error {{ code detail }}
    }} }} }}"#
        )
    };
    let result = h.execute(&test_via("127.0.0.1")).await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["testVia"],
        serde_json::json!([{
            "success": true,
            "resolution": {
                "host": "127.0.0.1",
                "resolvedBy": "LITERAL",
                "proxyId": null,
                "dnsServers": [],
                "addresses": ["127.0.0.1"],
                "cached": false,
                "connectedAddress": "127.0.0.1",
                "error": null,
            },
        }])
    );
    let _accepted = listener.accept().await.unwrap();
    drop(listener);
    let result = h.execute(&test_via("localhost")).await;
    assert_no_errors(&result);
    let test = &response_data(&result)["testVia"][0];
    assert_eq!(test["success"], false);
    let resolution = &test["resolution"];
    assert_eq!(resolution["host"], "localhost");
    assert_eq!(resolution["resolvedBy"], "HOST");
    assert!(
        resolution["addresses"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("127.0.0.1")),
        "{resolution}"
    );
    assert_eq!(resolution["connectedAddress"], serde_json::Value::Null);
    assert_eq!(resolution["error"], serde_json::Value::Null);
    let sdl = weaver_server_api::export_schema_sdl();
    let codes = sdl
        .split("enum LookupErrorCode {")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .unwrap()
        .split_whitespace()
        .collect::<Vec<_>>();
    assert_eq!(
        codes,
        [
            "NO_SUCH_NAME",
            "NO_ADDRESS",
            "TIMED_OUT",
            "SERVER_FAILURE",
            "NO_REPLY",
            "BOGUS",
            "DNSSEC_UNAVAILABLE",
            "TLS_FAILED",
            "OTHER"
        ]
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// What the DNS server below resolves `named.invalid` to.
const NAMED: &str = "198.51.100.7";

// A DNS server over TCP on loopback that resolves `named.invalid` to NAMED
// and no other name, marking every reply authenticated when `authentic`.
async fn dns_server(authentic: bool) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ad = if authentic { 0x20 } else { 0 };
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        while let Ok((mut stream, _)) = listener.accept().await {
            connections.spawn(async move {
                while let Ok(size) = stream.read_u16().await {
                    let mut query = vec![0; usize::from(size)];
                    if stream.read_exact(&mut query).await.is_err() {
                        return;
                    }
                    let Some((name, qtype, end)) = question(&query) else {
                        return;
                    };
                    let known = name == "named.invalid";
                    // The header and question alone: no OPT record, and so no
                    // DNSSEC records either.
                    let mut reply = query[..end].to_vec();
                    reply[10..12].fill(0);
                    reply[2] = 0x81;
                    reply[3] = if known { 0x80 } else { 0x83 } | ad;
                    if known && qtype == 1 {
                        reply[6..8].copy_from_slice(&1u16.to_be_bytes());
                        reply.extend_from_slice(&[0xc0, 12, 0, 1, 0, 1, 0, 0, 0, 0, 0, 4]);
                        reply.extend_from_slice(
                            &NAMED.parse::<std::net::Ipv4Addr>().unwrap().octets(),
                        );
                    }
                    let length = u16::try_from(reply.len()).unwrap();
                    if stream.write_u16(length).await.is_err()
                        || stream.write_all(&reply).await.is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    (addr, task)
}

// The name and type a DNS query asks, and where its question ends.
fn question(packet: &[u8]) -> Option<(String, u16, usize)> {
    let mut at = 12;
    let mut labels = Vec::new();
    loop {
        let length = usize::from(*packet.get(at)?);
        at += 1;
        if length == 0 {
            break;
        }
        labels.push(std::str::from_utf8(packet.get(at..at + length)?).ok()?);
        at += length;
    }
    let qtype = u16::from_be_bytes(packet.get(at..at + 4)?[..2].try_into().ok()?);
    Some((labels.join("."), qtype, at + 4))
}

// A test says which wire its listed servers were asked over and how their
// answer was judged, as the lookup it made recorded them: a TLS server that
// could not be reached is still the TLS wire, and an answer Weaver
// validates fails closed when it carries no signatures.
#[tokio::test]
async fn a_test_says_the_wire_and_trust_of_its_dns_servers() {
    let h = harness().await;
    // The proxy is closed: each lookup runs before its dial, and is what
    // the test reports.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = closed.local_addr().unwrap().port();
    drop(closed);
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tls_port = closed.local_addr().unwrap().port();
    drop(closed);
    let (marked, marked_task) = dns_server(true).await;

    let resolution = |name: &'static str, trust: &'static str, server: String| {
        let h = &h;
        async move {
            let result = h
                .execute(&format!(
                    r#"mutation {{ saveProxyProfile(input: {{
                name: "{name}", kind: SOCKS5, enabled: true,
                host: "127.0.0.1", port: {proxy_port}
            }}) {{ id }} saveDnsServerList(input: {{ name: "{name}", servers: [{server}] }}) {{ id }} }}"#
                ))
                .await;
            assert_no_errors(&result);
            let id = response_data(&result)["saveProxyProfile"]["id"]
                .as_u64()
                .unwrap();
            let list = response_data(&result)["saveDnsServerList"]["id"]
                .as_u64()
                .unwrap();
            let result = h
                .execute(&format!(
                    r#"mutation {{ saveDnsProfile(input: {{
                name: "{name}", kind: ROUTE, who: WEAVER, transit: EGRESS, trust: {trust}, serverListId: {list}
            }}) {{ id }} }}"#
                ))
                .await;
            assert_no_errors(&result);
            let profile = response_data(&result)["saveDnsProfile"]["id"]
                .as_u64()
                .unwrap();
            let result = h
                .execute(&format!(
                    r#"mutation {{ testVia(
                route: {{ legs: [{{ egressId: 0, weight: 100, path: {{ direct: true }} }}], dnsProfileId: {profile} }},
                subject: {{ proxy: {id} }}, target: {{ host: "named.invalid", port: 443 }}
            ) {{ success resolution {{
                resolvedBy dnsServers addresses wire trust error {{ code }}
            }} }} }}"#
                ))
                .await;
            assert_no_errors(&result);
            let test = &response_data(&result)["testVia"][0];
            assert_eq!(test["success"], false, "{test}");
            test["resolution"].clone()
        }
    };

    let tls = resolution(
        "Encrypted",
        "AS_SENT",
        format!(r#"{{ wire: TLS, address: "127.0.0.1:{tls_port}", tlsName: "dns.example" }}"#),
    )
    .await;
    assert_eq!(tls["resolvedBy"], "SERVERS", "{tls}");
    assert_eq!(tls["dnsServers"], serde_json::json!(["127.0.0.1"]));
    assert_eq!(tls["wire"], "TLS");
    // No answer came, so none was judged: the record claims no trust.
    assert!(tls["trust"].is_null(), "{tls}");
    assert_eq!(tls["addresses"], serde_json::json!([]));
    assert!(!tls["error"].is_null(), "{tls}");

    // Validating here checks the signatures itself, against the root's
    // keys. A server that sends none, even one marking its answer
    // authenticated, fails closed: nothing resolves, and the lookup is
    // refused as failing the check.
    let validated = resolution(
        "Validated",
        "VALIDATE",
        format!(r#"{{ wire: PLAIN, address: "{marked}" }}"#),
    )
    .await;
    assert_eq!(validated["resolvedBy"], "SERVERS", "{validated}");
    assert_eq!(validated["addresses"], serde_json::json!([]));
    assert_eq!(validated["wire"], "PLAIN");
    assert_eq!(validated["trust"], "BOGUS");
    assert_eq!(
        validated["error"],
        serde_json::json!({ "code": "DNSSEC_UNAVAILABLE" })
    );
    let sdl = weaver_server_api::export_schema_sdl();
    let outcomes = sdl
        .split("enum LookupTrust {")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .unwrap()
        .split_whitespace()
        .collect::<Vec<_>>();
    assert_eq!(outcomes, ["AS_SENT", "VALIDATED", "UNSIGNED", "BOGUS"]);
    marked_task.abort();
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// A drafted route that its save would refuse is refused by its test the
// same way, before anything is dialled: only a WireGuard proxy's answers can
// be validated, so validating a SOCKS5 proxy's is refused.
#[tokio::test]
async fn a_test_refuses_a_route_its_save_would_refuse_before_dialling() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let h = harness().await;
    let code = |result: &async_graphql::Response| {
        assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
        let error = serde_json::to_value(&result.errors[0]).unwrap();
        (
            error["extensions"]["code"].as_str().unwrap().to_owned(),
            error["extensions"]["params"].clone(),
        )
    };
    // The proxy counts each connection it is dialled with, and closes it
    // before the dial can go further. A dial waits on its reply, so a test
    // that dialled it has been counted by the time the test answers.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let dials = Arc::new(AtomicUsize::new(0));
    let counted = dials.clone();
    let proxy_task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            counted.fetch_add(1, Ordering::SeqCst);
            drop(stream);
        }
    });
    let result = h
        .execute(&format!(
            r#"mutation {{ saveProxyProfile(input: {{
        name: "gateway", kind: SOCKS5, enabled: true, host: "127.0.0.1", port: {proxy_port}
    }}) {{ id }} saveDnsProfile(input: {{
        name: "validated", kind: ROUTE, who: PROXY, trust: VALIDATE
    }}) {{ id }} }}"#
        ))
        .await;
    assert_no_errors(&result);
    let proxy = response_data(&result)["saveProxyProfile"]["id"]
        .as_u64()
        .unwrap();
    let profile = response_data(&result)["saveDnsProfile"]["id"]
        .as_u64()
        .unwrap();
    let route = |profile: u64| {
        format!(
            "{{ legs: [{{ egressId: 0, weight: 100, path: {{ ladder: {{ rungs: [{{ proxy: {proxy} }}], directFallback: false }} }} }}], dnsProfileId: {profile} }}"
        )
    };
    let test_via = |profile: u64| {
        format!(
            r#"mutation {{ testVia(route: {}, target: {{ host: "127.0.0.1", port: 9 }}) {{ success code }} }}"#,
            route(profile)
        )
    };

    // The save is refused, and names the route it refuses.
    let (saved, params) = code(
        &h.execute(&format!(
            "mutation {{ saveNetworkRoute(kind: OUTBOUND, id: 0, input: {}) {{ consumer }} }}",
            route(profile)
        ))
        .await,
    );
    assert_eq!(saved, "DNS_PROFILE_NEEDS_WIREGUARD");
    assert_eq!(params["routes"].as_array().unwrap().len(), 1, "{params}");

    // The test is refused with the same code, naming no route: a draft has
    // no holder. Nothing was dialled.
    assert_eq!(
        code(&h.execute(&test_via(profile)).await),
        (
            "DNS_PROFILE_NEEDS_WIREGUARD".to_owned(),
            serde_json::json!({ "routes": [] })
        )
    );
    assert_eq!(dials.load(Ordering::SeqCst), 0);

    // The same route on the host's names is tested, and dials the proxy.
    let result = h.execute(&test_via(0)).await;
    assert_no_errors(&result);
    assert_eq!(response_data(&result)["testVia"][0]["success"], false);
    assert!(dials.load(Ordering::SeqCst) > 0);
    proxy_task.abort();
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// A test of listed servers through a drafted route that its save would
// refuse is refused the same way, before any server is dialled: the proxy
// cannot answer on a leg with no proxy.
#[tokio::test]
async fn a_dns_server_test_refuses_a_route_its_save_would_refuse_before_dialling() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let h = harness().await;
    let code = |result: &async_graphql::Response| {
        assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
        let error = serde_json::to_value(&result.errors[0]).unwrap();
        (
            error["extensions"]["code"].as_str().unwrap().to_owned(),
            error["extensions"]["params"].clone(),
        )
    };
    // The server counts each connection a probe opens to it, and closes it
    // before it is answered. A probe waits on its answer, so a test that
    // asked it has been counted by the time the test answers.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = listener.local_addr().unwrap();
    let dials = Arc::new(AtomicUsize::new(0));
    let counted = dials.clone();
    let server_task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            counted.fetch_add(1, Ordering::SeqCst);
            drop(stream);
        }
    });
    let result = h
        .execute(
            r#"mutation { saveDnsProfile(input: {
        name: "proxy answers", kind: ROUTE, who: PROXY, trust: AS_SENT
    }) { id } }"#,
        )
        .await;
    assert_no_errors(&result);
    let profile = response_data(&result)["saveDnsProfile"]["id"]
        .as_u64()
        .unwrap();
    let route = |profile: u64| {
        format!(
            "{{ legs: [{{ egressId: 0, weight: 100, path: {{ direct: true }} }}], dnsProfileId: {profile} }}"
        )
    };
    let test = |profile: u64| {
        format!(
            r#"mutation {{ testDnsServers(route: {}, servers: [{{ wire: PLAIN, address: "{server}" }}]) {{ index }} }}"#,
            route(profile)
        )
    };

    // The save is refused, and names the route it refuses.
    let (saved, params) = code(
        &h.execute(&format!(
            "mutation {{ saveNetworkRoute(kind: OUTBOUND, id: 0, input: {}) {{ consumer }} }}",
            route(profile)
        ))
        .await,
    );
    assert_eq!(saved, "DNS_PROFILE_NEEDS_PROXY");
    assert_eq!(params["routes"].as_array().unwrap().len(), 1, "{params}");

    // The test is refused with the same code, naming no route: a draft has
    // no holder. Nothing was dialled.
    assert_eq!(
        code(&h.execute(&test(profile)).await),
        (
            "DNS_PROFILE_NEEDS_PROXY".to_owned(),
            serde_json::json!({ "routes": [] })
        )
    );
    assert_eq!(dials.load(Ordering::SeqCst), 0);

    // The same route on the host's names asks the server from the egress.
    let result = h.execute(&test(0)).await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["testDnsServers"],
        serde_json::json!([{ "index": 0 }])
    );
    assert!(dials.load(Ordering::SeqCst) > 0);
    server_task.abort();
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// A pool under test carries a target through the member it picks, and is
// tried member by member without one; the old pool test stays member by
// member.
#[tokio::test]
async fn a_pool_test_names_the_pool_and_the_member_it_picked() {
    let h = harness().await;
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    let mut ids = Vec::new();
    for name in ["Primary", "Secondary"] {
        let result = h
            .execute(&format!(
                r#"mutation {{ saveProxyProfile(input: {{
            name: "{name}", kind: SOCKS5, enabled: true,
            host: "127.0.0.1", port: {port}
        }}) {{ id }} }}"#
            ))
            .await;
        assert_no_errors(&result);
        ids.push(
            response_data(&result)["saveProxyProfile"]["id"]
                .as_u64()
                .unwrap(),
        );
    }
    let result = h
        .execute(&format!(
            r#"mutation {{ createProxyPool(input: {{
        name: "Exits", kind: SOCKS5, memberIds: [{}, {}]
    }}) {{ id }} }}"#,
            ids[0], ids[1]
        ))
        .await;
    assert_no_errors(&result);
    let pool = response_data(&result)["createProxyPool"]["id"]
        .as_u64()
        .unwrap();
    let test_via = |target: &str| {
        format!(
            r#"mutation {{ testVia(
        route: {{ legs: [{{ egressId: 0, weight: 100, path: {{ direct: true }} }}] }},
        subject: {{ pool: {pool} }}{target}
    ) {{ poolId proxyId success }} }}"#
        )
    };
    let result = h
        .execute(&test_via(&format!(
            r#", target: {{ host: "127.0.0.1", port: {port} }}"#
        )))
        .await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["testVia"],
        serde_json::json!([{ "poolId": pool, "proxyId": null, "success": false }])
    );
    let result = h.execute(&test_via("")).await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["testVia"],
        serde_json::json!([
            { "poolId": null, "proxyId": ids[0], "success": false },
            { "poolId": null, "proxyId": ids[1], "success": false },
        ])
    );
    let result = h
        .execute(&format!(
            r#"mutation {{ testProxyPool(id: {pool}, egressId: 0, host: "127.0.0.1", port: {port}) {{
        proxyId success
    }} }}"#
        ))
        .await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["testProxyPool"],
        serde_json::json!([
            { "proxyId": ids[0], "success": false },
            { "proxyId": ids[1], "success": false },
        ])
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn feed_policy_is_atomic_preserved_and_blocks_referenced_profile_deletion() {
    let h = harness().await;
    let result = h
        .execute(
            r#"mutation { saveProxyProfile(input: {
        name: "gateway", kind: HTTP_CONNECT, enabled: true, host: "127.0.0.1", port: 8080
    }) { id } }"#,
        )
        .await;
    assert_no_errors(&result);
    let proxy = response_data(&result)["saveProxyProfile"]["id"]
        .as_u64()
        .unwrap();
    let result = h
        .execute(&format!(
            r#"mutation {{ addRssFeed(input: {{
        name: "routed", url: "https://feed.invalid/rss", enabled: false,
        routing: {{ proxyIds: [{proxy}], allowDirect: false }}
    }}) {{ id routing {{ proxyIds allowDirect }} }} }}"#
        ))
        .await;
    assert_no_errors(&result);
    let feed = response_data(&result)["addRssFeed"]["id"].as_u64().unwrap();
    let route = h
        .handle
        .proxy_runtime()
        .unwrap()
        .route(
            weaver_server_core::proxies::Consumer::Rss(feed as u32),
            std::time::Duration::from_secs(30),
        )
        .unwrap();
    assert_has_errors(
        &h.execute(&format!("mutation {{ deleteProxyProfile(id: {proxy}) }}"))
            .await,
    );
    let result = h
        .execute(&format!(
            r#"mutation {{ updateRssFeed(id: {feed}, input: {{
        name: "renamed", url: "https://feed.invalid/rss", enabled: false
    }}) {{ routing {{ proxyIds allowDirect }} }} }}"#
        ))
        .await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["updateRssFeed"]["routing"]["allowDirect"],
        false
    );
    for ids in [format!("{proxy}, {proxy}"), "999".to_string()] {
        assert_has_errors(
            &h.execute(&format!(
                r#"mutation {{ updateRssFeed(id: {feed}, input: {{
            name: "must not save", url: "https://feed.invalid/rss", enabled: false,
            routing: {{ proxyIds: [{ids}], allowDirect: true }}
        }}) {{ id }} }}"#
            ))
            .await,
        );
    }
    assert_eq!(h.db.list_rss_feeds().unwrap()[0].name, "renamed");
    let result = h
        .execute(&format!(
            r#"mutation {{ updateRssFeed(id: {feed}, input: {{
        name: "blocked", url: "https://feed.invalid/rss", enabled: false,
        routing: {{ proxyIds: [], allowDirect: false }}
    }}) {{ routingStatus {{ state }} }} }}"#
        ))
        .await;
    assert_no_errors(&result);
    assert!(route.is_revoked());
    assert_eq!(
        response_data(&result)["updateRssFeed"]["routingStatus"]["state"],
        "BLOCKED"
    );
    assert_no_errors(
        &h.execute(&format!("mutation {{ deleteProxyProfile(id: {proxy}) }}"))
            .await,
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// A proxy save rebuilds the usenet connections only when a server's route
// goes through the proxy, and a save that changes nothing rebuilds nothing,
// not even the routes.
#[tokio::test]
async fn a_proxy_save_rebuilds_usenet_connections_only_for_a_proxy_a_server_routes_through() {
    let h = harness().await;
    let generation = || h.handle.nntp_runtime_activation().map(|a| a.generation);
    let reloads = h.handle.proxy_runtime().unwrap().network.reloads();
    let save = |id: Option<u64>, port: u16| {
        let id = id.map_or("null".into(), |id| id.to_string());
        format!(
            r#"mutation {{ saveProxyProfile(id: {id}, input: {{
        name: "gateway", kind: SOCKS5, enabled: true, host: "127.0.0.1", port: {port}
    }}) {{ id }} }}"#
        )
    };
    let created = h.execute(&save(None, 1080)).await;
    assert_no_errors(&created);
    let id = response_data(&created)["saveProxyProfile"]["id"]
        .as_u64()
        .unwrap();

    // No server routes through the proxy: its change reloads the routes and
    // leaves the usenet connections alone.
    let before = generation();
    let reloaded = *reloads.borrow();
    assert_no_errors(&h.execute(&save(Some(id), 1081)).await);
    assert_eq!(generation(), before);
    assert!(*reloads.borrow() > reloaded);

    let added = h
        .execute(&format!(
            r#"mutation {{addServer(input:{{host:"news.fixture.invalid",port:119,tls:false,connections:2,active:false,routing:{{proxyIds:[{id}],allowDirect:false}}}}){{id}}}}"#
        ))
        .await;
    assert_no_errors(&added);

    // A server routes through it: its change rebuilds them.
    let before = generation();
    assert_no_errors(&h.execute(&save(Some(id), 1082)).await);
    assert!(generation() > before, "{:?} after {before:?}", generation());

    // The same settings again: nothing is rebuilt or reloaded.
    let before = generation();
    let reloaded = *reloads.borrow();
    assert_no_errors(&h.execute(&save(Some(id), 1082)).await);
    assert_eq!(generation(), before);
    assert_eq!(*reloads.borrow(), reloaded);
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// A proxy selects the DNS profile that finds its own address: System or a
// proxy name profile, never a route profile. A profile's kind is named when
// it is created and kept, and a profile a proxy uses is not deleted.
#[tokio::test]
async fn a_proxy_selects_a_proxy_name_profile_for_its_own_address() {
    let h = harness().await;
    let code = |result: &async_graphql::Response| {
        assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
        let error = serde_json::to_value(&result.errors[0]).unwrap();
        (
            error["extensions"]["code"].as_str().unwrap().to_owned(),
            error["extensions"]["params"].clone(),
        )
    };
    let result = h
        .execute(
            r#"mutation { saveDnsServerList(input: { name: "Resolvers", servers: [{ wire: PLAIN, address: "192.0.2.53" }] }) { id } }"#,
        )
        .await;
    assert_no_errors(&result);
    let list = response_data(&result)["saveDnsServerList"]["id"]
        .as_u64()
        .unwrap();
    let profile = |kind: &str| {
        format!(
            r#"mutation {{ saveDnsProfile(input: {{
        name: "listed", {kind} who: WEAVER, transit: EGRESS, trust: AS_SENT, serverListId: {list}
    }}) {{ id kind }} }}"#
        )
    };
    assert_eq!(
        code(&h.execute(&profile("")).await).0,
        "DNS_PROFILE_KIND_REQUIRED"
    );
    let saved = |result: &async_graphql::Response| {
        assert_no_errors(result);
        let data = response_data(result);
        assert!(data["saveDnsProfile"]["kind"].is_string());
        data["saveDnsProfile"]["id"].as_u64().unwrap()
    };
    let route = saved(&h.execute(&profile("kind: ROUTE,")).await);
    let name = saved(&h.execute(&profile("kind: PROXY_NAME,")).await);
    let refused = h
        .execute(&format!(
            r#"mutation {{ saveDnsProfile(input: {{
        id: {name}, name: "listed", kind: ROUTE, who: WEAVER, transit: EGRESS, trust: AS_SENT, serverListId: {list}
    }}) {{ id }} }}"#
        ))
        .await;
    assert_eq!(code(&refused).0, "DNS_PROFILE_KIND_IMMUTABLE");
    let refused = h
        .execute(
            r#"mutation { saveDnsProfile(input: {
        name: "via proxy", kind: PROXY_NAME, who: PROXY, trust: AS_SENT
    }) { id } }"#,
        )
        .await;
    assert_eq!(code(&refused).0, "DNS_PROFILE_PROXY_NAME_INVALID");

    // A drafted route on a proxy name profile is refused before any server
    // is asked, as its save would be.
    let refused = h
        .execute(&format!(
            r#"mutation {{ testDnsServers(
                route: {{ legs: [{{ egressId: 0, weight: 100, path: {{ direct: true }} }}], dnsProfileId: {name} }},
                servers: [{{ wire: PLAIN, address: "192.0.2.53" }}]
            ) {{ index }} }}"#
        ))
        .await;
    assert_eq!(
        code(&refused),
        (
            "DNS_PROFILE_KIND".to_owned(),
            serde_json::json!({ "expected": "ROUTE" })
        )
    );

    let save = |id: Option<u64>, dns: &str| {
        let id = id.map_or("null".into(), |id| id.to_string());
        format!(
            r#"mutation {{ saveProxyProfile(id: {id}, input: {{
        name: "gateway", kind: SOCKS5, enabled: true, host: "proxy.example.test", port: 1080{dns}
    }}) {{ id dnsProfileId }} }}"#
        )
    };
    let result = h.execute(&save(None, "")).await;
    assert_no_errors(&result);
    let proxy = response_data(&result)["saveProxyProfile"]["id"]
        .as_u64()
        .unwrap();
    assert_eq!(
        response_data(&result)["saveProxyProfile"]["dnsProfileId"],
        0
    );
    let (refusal, params) = code(
        &h.execute(&save(Some(proxy), &format!(", dnsProfileId: {route}")))
            .await,
    );
    assert_eq!(refusal, "DNS_PROFILE_KIND");
    assert_eq!(params, serde_json::json!({ "expected": "PROXY_NAME" }));
    let result = h
        .execute(&save(Some(proxy), &format!(", dnsProfileId: {name}")))
        .await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["saveProxyProfile"]["dnsProfileId"],
        name
    );
    // An update that names no profile keeps the saved one.
    let result = h.execute(&save(Some(proxy), "")).await;
    assert_no_errors(&result);
    assert_eq!(
        response_data(&result)["saveProxyProfile"]["dnsProfileId"],
        name
    );

    let result = h
        .execute("{ dnsProfiles { id kind usedByProxies usedByRoutes } }")
        .await;
    assert_no_errors(&result);
    let profiles = response_data(&result)["dnsProfiles"].clone();
    let used = |id: u64| {
        profiles
            .as_array()
            .unwrap()
            .iter()
            .find(|profile| profile["id"] == id)
            .unwrap()
            .clone()
    };
    assert_eq!(used(0)["kind"], "ROUTE");
    assert_eq!(used(name)["kind"], "PROXY_NAME");
    assert_eq!(used(name)["usedByProxies"], 1);
    assert_eq!(used(route)["usedByProxies"], 0);

    let (refusal, params) = code(
        &h.execute(&format!("mutation {{ deleteDnsProfile(id: {name}) }}"))
            .await,
    );
    assert_eq!(refusal, "DNS_PROFILE_IN_USE");
    assert_eq!(
        params,
        serde_json::json!({ "routes": [], "proxies": [proxy] })
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// `dnsProfiles(kind:)` gives what something of that kind may select: System,
// then the saved profiles of that kind by id. Without a kind it gives every
// profile in the same order.
#[tokio::test]
async fn dns_profiles_of_one_kind_are_system_then_that_kind() {
    let h = harness().await;
    let result = h
        .execute(
            r#"mutation { saveDnsServerList(input: { name: "Resolvers", servers: [{ wire: PLAIN, address: "192.0.2.53" }] }) { id } }"#,
        )
        .await;
    assert_no_errors(&result);
    let list = response_data(&result)["saveDnsServerList"]["id"]
        .as_u64()
        .unwrap();
    let mut saved = Vec::new();
    // Interleaved, so an order by kind and an order by id differ.
    for kind in ["PROXY_NAME", "ROUTE", "PROXY_NAME", "ROUTE"] {
        let result = h
            .execute(&format!(
                r#"mutation {{ saveDnsProfile(input: {{
            name: "listed", kind: {kind}, who: WEAVER, transit: EGRESS, trust: AS_SENT, serverListId: {list}
        }}) {{ id }} }}"#
            ))
            .await;
        assert_no_errors(&result);
        saved.push((
            response_data(&result)["saveDnsProfile"]["id"]
                .as_u64()
                .unwrap(),
            kind,
        ));
    }
    let read = |query: &'static str| {
        let h = &h;
        async move {
            let result = h.execute(query).await;
            assert_no_errors(&result);
            response_data(&result)["dnsProfiles"]
                .as_array()
                .unwrap()
                .iter()
                .map(|profile| {
                    (
                        profile["id"].as_u64().unwrap(),
                        profile["kind"].as_str().unwrap().to_owned(),
                    )
                })
                .collect::<Vec<_>>()
        }
    };
    let expected = |kind: Option<&str>| {
        std::iter::once((0, "ROUTE".to_owned()))
            .chain(
                saved
                    .iter()
                    .filter(|(_, saved)| kind.is_none_or(|kind| kind == *saved))
                    .map(|(id, kind)| (*id, (*kind).to_owned())),
            )
            .collect::<Vec<_>>()
    };
    assert_eq!(read("{ dnsProfiles { id kind } }").await, expected(None));
    assert_eq!(
        read("{ dnsProfiles(kind: ROUTE) { id kind } }").await,
        expected(Some("ROUTE"))
    );
    assert_eq!(
        read("{ dnsProfiles(kind: PROXY_NAME) { id kind } }").await,
        expected(Some("PROXY_NAME"))
    );
    // One read of each kind in one request, as the DNS page sends it.
    let result = h
        .execute(
            "{ names: dnsProfiles(kind: PROXY_NAME) { id usedByProxies } routes: dnsProfiles(kind: ROUTE) { id usedByRoutes } }",
        )
        .await;
    assert_no_errors(&result);
    let ids = |key: &str| {
        response_data(&result)[key]
            .as_array()
            .unwrap()
            .iter()
            .map(|profile| profile["id"].as_u64().unwrap())
            .collect::<Vec<_>>()
    };
    let ids_of = |kind: &str| {
        expected(Some(kind))
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids("names"), ids_of("PROXY_NAME"));
    assert_eq!(ids("routes"), ids_of("ROUTE"));
}
