// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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
        username: "private-user", password: "private-password", dns: { who: SERVERS, path: THROUGH_PROXY, trust: AS_SENT, servers: [{ wire: PLAIN, address: "192.0.2.53" }] }
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
        name: "renamed", kind: SOCKS5, enabled: false, host: "127.0.0.1", port: 1080,
        dns: {{ who: SERVERS, path: THROUGH_PROXY, trust: AS_SENT, servers: [{{ wire: PLAIN, address: "192.0.2.53" }}] }}
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

// A save of a `kind` proxy with `extra` fields, asking for its DNS back.
fn save_dns(id: Option<u64>, kind: &str, extra: &str) -> String {
    let id = id.map_or("null".into(), |id| id.to_string());
    format!(
        r#"mutation {{ saveProxyProfile(id: {id}, input: {{
        name: "dns-{kind}", kind: {kind}, enabled: true, host: "127.0.0.1", port: 1080{extra}
    }}) {{ id dns {{ who path trust servers {{ wire address tlsName url }} }} }} }}"#
    )
}

// The saved proxy's id and DNS.
fn saved_dns(result: &async_graphql::Response) -> (u64, serde_json::Value) {
    assert_no_errors(result);
    let data = response_data(result);
    (
        data["saveProxyProfile"]["id"].as_u64().unwrap(),
        data["saveProxyProfile"]["dns"].clone(),
    )
}

#[tokio::test]
async fn a_save_without_dns_keeps_how_the_proxy_resolves() {
    let h = harness().await;
    let who = |dns: &serde_json::Value| dns["who"].as_str().unwrap().to_owned();
    let (socks, dns) = saved_dns(&h.execute(&save_dns(None, "SOCKS5", "")).await);
    assert_eq!(
        dns,
        serde_json::json!({ "who": "PROXY", "path": "THROUGH_PROXY", "trust": "AS_SENT", "servers": [] })
    );
    let (_, dns) = saved_dns(
        &h.execute(&save_dns(None, "WIRE_GUARD", WIREGUARD_KEYS))
            .await,
    );
    assert_eq!(who(&dns), "HOST");
    let with_dns = format!(
        r#"{WIREGUARD_KEYS}, dns: {{ who: SERVERS, path: THROUGH_PROXY, trust: AS_SENT, servers: [{{ wire: PLAIN, address: "10.0.0.1" }}] }}"#
    );
    let (_, dns) = saved_dns(&h.execute(&save_dns(None, "WIRE_GUARD", &with_dns)).await);
    assert_eq!(who(&dns), "SERVERS");

    let host = r#", dns: { who: HOST, path: DIRECT, trust: REQUIRE_AD, servers: [] }"#;
    let (_, dns) = saved_dns(&h.execute(&save_dns(Some(socks), "SOCKS5", host)).await);
    assert_eq!(
        dns,
        serde_json::json!({ "who": "HOST", "path": "THROUGH_PROXY", "trust": "AS_SENT", "servers": [] }),
        "who answers alone is kept, in its normal form"
    );
    let host = r#", dns: { who: HOST, path: DIRECT, trust: VALIDATE, servers: [] }"#;
    let (_, dns) = saved_dns(&h.execute(&save_dns(Some(socks), "SOCKS5", host)).await);
    assert_eq!(
        dns,
        serde_json::json!({ "who": "HOST", "path": "THROUGH_PROXY", "trust": "AS_SENT", "servers": [] }),
        "validating is kept only while servers answer"
    );
    let (_, dns) = saved_dns(&h.execute(&save_dns(Some(socks), "SOCKS5", "")).await);
    assert_eq!(
        who(&dns),
        "HOST",
        "an update without DNS keeps the saved one"
    );
    let validated = r#", dns: { who: SERVERS, path: DIRECT, trust: VALIDATE, servers: [{ wire: TLS, address: "192.0.2.53" }] }"#;
    let (_, dns) = saved_dns(&h.execute(&save_dns(Some(socks), "SOCKS5", validated)).await);
    assert_eq!(dns["trust"], "VALIDATE");
    assert_eq!(dns["who"], "SERVERS");
    let (_, dns) = saved_dns(&h.execute(&save_dns(Some(socks), "SOCKS5", "")).await);
    assert_eq!(
        dns["trust"], "VALIDATE",
        "an update without DNS keeps validating"
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

// Each wire comes back as the web reads it: the address an IP with its
// port only when it is not the wire's own, and DNS over HTTPS as its URL.
#[tokio::test]
async fn dns_servers_come_back_as_they_were_given() {
    let h = harness().await;
    let servers = r#", dns: { who: SERVERS, path: DIRECT, trust: REQUIRE_AD, servers: [
        { wire: PLAIN, address: "192.0.2.53" },
        { wire: PLAIN, address: "192.0.2.54:5353" },
        { wire: TLS, address: "[2001:db8::1]:853", tlsName: " dns.example " },
        { wire: HTTPS, address: "", url: "HTTPS://[2001:db8::2]:8443/dns-query?ct=1" }
    ] }"#;
    let (id, dns) = saved_dns(&h.execute(&save_dns(None, "SOCKS5", servers)).await);
    assert_eq!(
        dns,
        serde_json::json!({
            "who": "SERVERS", "path": "DIRECT", "trust": "REQUIRE_AD",
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
    let stored = h.db.list_proxy_profiles().unwrap();
    let stored = stored.iter().find(|p| u64::from(p.id) == id).unwrap();
    assert_eq!(
        serde_json::to_value(&stored.dns).unwrap()["servers"][3],
        serde_json::json!({
            "address": "[2001:db8::2]:8443",
            "wire": { "Https": { "path": "/dns-query?ct=1" } }
        })
    );

    let default_port = r#", dns: { who: SERVERS, path: THROUGH_PROXY, trust: AS_SENT, servers: [
        { wire: HTTPS, address: "", url: "https://192.0.2.1/dns-query" },
        { wire: TLS, address: "192.0.2.2", tlsName: "" }
    ] }"#;
    let (_, dns) = saved_dns(&h.execute(&save_dns(Some(id), "SOCKS5", default_port)).await);
    assert_eq!(dns["servers"][0]["url"], "https://192.0.2.1/dns-query");
    assert_eq!(dns["servers"][0]["address"], "192.0.2.1");
    assert_eq!(dns["servers"][1]["address"], "192.0.2.2");
    assert_eq!(dns["servers"][1]["tlsName"], serde_json::Value::Null);

    // A port is read as a number, leading zeros and all; a bare or bracketed
    // v6 address keeps the wire's port.
    let spellings = r#", dns: { who: SERVERS, path: THROUGH_PROXY, trust: AS_SENT, servers: [
        { wire: PLAIN, address: "192.0.2.53:05353" },
        { wire: PLAIN, address: "[2001:db8::3]:0053" },
        { wire: PLAIN, address: "2001:db8::4" },
        { wire: TLS, address: "[2001:db8::5]" }
    ] }"#;
    let (_, dns) = saved_dns(&h.execute(&save_dns(Some(id), "SOCKS5", spellings)).await);
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
    let stored = h.db.list_proxy_profiles().unwrap();
    let stored = stored.iter().find(|p| u64::from(p.id) == id).unwrap();
    assert_eq!(
        stored
            .dns
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
async fn a_dns_setting_no_proxy_could_use_is_refused() {
    let h = harness().await;
    let refused = |result: &async_graphql::Response| {
        assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
        serde_json::to_value(&result.errors[0]).unwrap()["extensions"]["code"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let servers = |list: &str| {
        format!(
            r#", dns: {{ who: SERVERS, path: THROUGH_PROXY, trust: AS_SENT, servers: [{list}] }}"#
        )
    };
    let plain = r#"{ wire: PLAIN, address: "192.0.2.53" }"#;
    let cases = [
        ("SOCKS5", servers(""), "PROXY_DNS_SERVERS_REQUIRED"),
        (
            "SOCKS5",
            r#", dns: { who: SERVERS, path: DIRECT, trust: VALIDATE, servers: [] }"#.to_owned(),
            "PROXY_DNS_SERVERS_REQUIRED",
        ),
        (
            "SOCKS5",
            servers(&[plain; 11].join(", ")),
            "PROXY_DNS_SERVER_COUNT",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: PLAIN, address: "192.0.2.53:0" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: PLAIN, address: "dns.example" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        // Brackets hold a v6 address only, and no address carries a zone,
        // in any form it is written.
        (
            "SOCKS5",
            servers(r#"{ wire: PLAIN, address: "[192.0.2.53]" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: PLAIN, address: "[192.0.2.53]:53" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: TLS, address: "[192.0.2.53]:853" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: PLAIN, address: "[fe80::1%3]:53" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: PLAIN, address: "[fe80::1%3]" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: PLAIN, address: "fe80::1%3" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: TLS, address: "[fe80::1%eth0]:853" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: TLS, address: "fe80::1%3" }"#),
            "PROXY_DNS_SERVER_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "https://[192.0.2.1]/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "https://[fe80::1%3]/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "https://[fe80::1%253]:443/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: TLS, address: "192.0.2.53", tlsName: "not a name" }"#),
            "PROXY_DNS_TLS_NAME_INVALID",
        ),
        (
            "SOCKS5",
            servers(&format!(
                r#"{{ wire: TLS, address: "192.0.2.53", tlsName: "{}" }}"#,
                ["a"; 128].join(".")
            )),
            "PROXY_DNS_TLS_NAME_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "https://dns.example/dns-query" }"#),
            "PROXY_DNS_URL_HOSTNAME",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "http://192.0.2.1/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "https://192.0.2.1" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "https://user@192.0.2.1/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "https://192.0.2.1/dns-query#top" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "https://192.0.2.1:0/dns-query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "https://192.0.2.1/dns query" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(r#"{ wire: HTTPS, address: "", url: "https://192.0.2.53/a<b" }"#),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(&format!(
                r#"{{ wire: HTTPS, address: "", url: "https://192.0.2.1/{}" }}"#,
                "a".repeat(2048)
            )),
            "PROXY_DNS_URL_INVALID",
        ),
        (
            "SOCKS5",
            servers(&format!(
                r#"{{ wire: TLS, address: "192.0.2.53" }}, {plain}"#
            )),
            "PROXY_DNS_PLAIN_AFTER_ENCRYPTED",
        ),
        // One server named twice, once with its port spelled out.
        (
            "SOCKS5",
            servers(&format!(
                r#"{plain}, {{ wire: PLAIN, address: "192.0.2.53:53" }}"#
            )),
            "PROXY_DNS_SERVER_DUPLICATE",
        ),
        (
            "SOCKS5",
            servers(
                r#"{ wire: TLS, address: "192.0.2.53", tlsName: "dns.example" }, { wire: TLS, address: "192.0.2.53:853", tlsName: "dns.example" }"#,
            ),
            "PROXY_DNS_SERVER_DUPLICATE",
        ),
        (
            "SOCKS5",
            servers(
                r#"{ wire: HTTPS, address: "", url: "https://192.0.2.53/dns-query" }, { wire: HTTPS, address: "", url: "https://192.0.2.53:443/dns-query" }"#,
            ),
            "PROXY_DNS_SERVER_DUPLICATE",
        ),
        (
            "WIRE_GUARD",
            format!(
                r#"{WIREGUARD_KEYS}, dns: {{ who: PROXY, path: THROUGH_PROXY, trust: AS_SENT, servers: [] }}"#
            ),
            "PROXY_WIREGUARD_NEEDS_DNS_SERVERS",
        ),
    ];
    for (kind, extra, code) in cases {
        assert_eq!(
            refused(&h.execute(&save_dns(None, kind, &extra)).await),
            code,
            "{extra}"
        );
    }
    assert!(h.db.list_proxy_profiles().unwrap().is_empty());

    // The most servers a proxy may ask, and a plain server asked directly
    // with answers that must be authenticated, are taken.
    let ten = format!(
        r#", dns: {{ who: SERVERS, path: DIRECT, trust: REQUIRE_AD, servers: [{}] }}"#,
        (1..=10)
            .map(|n| format!(r#"{{ wire: PLAIN, address: "192.0.2.{n}" }}"#))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let (_, dns) = saved_dns(&h.execute(&save_dns(None, "SOCKS5", &ten)).await);
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
        privateKey: "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=", dns: { who: SERVERS, path: THROUGH_PROXY, trust: AS_SENT, servers: [{ wire: PLAIN, address: "10.2.0.1" }] }"#
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
        host: "proxy.invalid", port: 443, dns: { who: SERVERS, path: THROUGH_PROXY, trust: AS_SENT, servers: [{ wire: PLAIN, address: "192.0.2.53" }] },
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
        host: "proxy.invalid", port: 443, dns: {{ who: SERVERS, path: THROUGH_PROXY, trust: AS_SENT, servers: [{{ wire: PLAIN, address: "192.0.2.53" }}] }}
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
            "NOT_AUTHENTICATED",
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
// could not be reached is still the TLS wire, and servers that must
// authenticate are taken when they do and refused when they do not.
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
    let (unmarked, unmarked_task) = dns_server(false).await;

    let resolution = |name: &'static str, trust: &'static str, server: String| {
        let h = &h;
        async move {
            let result = h
                .execute(&format!(
                    r#"mutation {{ saveProxyProfile(input: {{
                name: "{name}", kind: SOCKS5, enabled: true,
                host: "127.0.0.1", port: {proxy_port},
                dns: {{ who: SERVERS, path: DIRECT, trust: {trust}, servers: [{server}] }}
            }}) {{ id }} }}"#
                ))
                .await;
            assert_no_errors(&result);
            let id = response_data(&result)["saveProxyProfile"]["id"]
                .as_u64()
                .unwrap();
            let result = h
                .execute(&format!(
                    r#"mutation {{ testVia(
                route: {{ legs: [{{ egressId: 0, weight: 100, path: {{ direct: true }} }}] }},
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

    let authenticated = resolution(
        "Marked",
        "REQUIRE_AD",
        format!(r#"{{ wire: PLAIN, address: "{marked}" }}"#),
    )
    .await;
    assert_eq!(
        authenticated,
        serde_json::json!({
            "resolvedBy": "SERVERS",
            "dnsServers": ["127.0.0.1"],
            "addresses": [NAMED],
            "wire": "PLAIN",
            "trust": "AUTHENTICATED",
            "error": null,
        })
    );

    let refused = resolution(
        "Unmarked",
        "REQUIRE_AD",
        format!(r#"{{ wire: PLAIN, address: "{unmarked}" }}"#),
    )
    .await;
    assert_eq!(
        refused,
        serde_json::json!({
            "resolvedBy": "SERVERS",
            "dnsServers": ["127.0.0.1"],
            "addresses": [],
            "wire": "PLAIN",
            "trust": "REFUSED",
            "error": { "code": "NOT_AUTHENTICATED" },
        })
    );

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
    assert_eq!(
        outcomes,
        [
            "AS_SENT",
            "AUTHENTICATED",
            "REFUSED",
            "VALIDATED",
            "UNSIGNED",
            "BOGUS"
        ]
    );
    marked_task.abort();
    unmarked_task.abort();
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
        name: "gateway", kind: HTTP_CONNECT, enabled: true, host: "127.0.0.1", port: 8080,
        dns: { who: SERVERS, path: THROUGH_PROXY, trust: AS_SENT, servers: [{ wire: PLAIN, address: "192.0.2.53" }] }
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
