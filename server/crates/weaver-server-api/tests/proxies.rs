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
        name: "gateway", kind: SOCKS5, resolver: PROXY, enabled: true, host: "127.0.0.1", port: 1080,
        username: "private-user", password: "private-password", dnsServers: ["192.0.2.53"]
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
        name: "renamed", kind: SOCKS5, resolver: PROXY, enabled: false, host: "127.0.0.1", port: 1080,
        dnsServers: ["192.0.2.53"]
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

#[tokio::test]
async fn a_save_without_a_resolver_keeps_how_the_proxy_resolved() {
    let h = harness().await;
    let save = |id: Option<u64>, kind: &str, extra: &str| {
        let id = id.map_or("null".into(), |id| id.to_string());
        format!(
            r#"mutation {{ saveProxyProfile(id: {id}, input: {{
        name: "older-client-{kind}", kind: {kind}, enabled: true, host: "127.0.0.1", port: 1080{extra}
    }}) {{ id resolver }} }}"#
        )
    };
    let saved = |result: &async_graphql::Response| {
        assert_no_errors(result);
        let data = response_data(result);
        (
            data["saveProxyProfile"]["id"].as_u64().unwrap(),
            data["saveProxyProfile"]["resolver"]
                .as_str()
                .unwrap()
                .to_owned(),
        )
    };
    let (socks, resolver) = saved(&h.execute(&save(None, "SOCKS5", "")).await);
    assert_eq!(resolver, "PROXY");
    let wireguard = r#", tunnelAddresses: ["10.0.0.2/32"],
        peerPublicKey: "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=",
        privateKey: "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=""#;
    let (_, resolver) = saved(&h.execute(&save(None, "WIRE_GUARD", wireguard)).await);
    assert_eq!(resolver, "WEAVER");
    let with_dns = format!(r#"{wireguard}, dnsServers: ["10.0.0.1"]"#);
    let (_, resolver) = saved(&h.execute(&save(None, "WIRE_GUARD", &with_dns)).await);
    assert_eq!(resolver, "PROXY");

    let named = h
        .execute(&format!(
            r#"mutation {{ saveProxyProfile(id: {socks}, input: {{
        name: "older-client-SOCKS5", kind: SOCKS5, resolver: WEAVER, enabled: true, host: "127.0.0.1", port: 1080
    }}) {{ resolver }} }}"#
        ))
        .await;
    assert_no_errors(&named);
    let (_, resolver) = saved(&h.execute(&save(Some(socks), "SOCKS5", "")).await);
    assert_eq!(
        resolver, "WEAVER",
        "an update without a resolver keeps the saved one"
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn an_ssh_profile_is_refused_a_password() {
    let h = harness().await;
    let result = h
        .execute(
            r#"mutation { saveProxyProfile(input: {
        name: "seedbox", kind: SSH, resolver: PROXY, enabled: true, host: "127.0.0.1", port: 22,
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
        name: "QUIC gateway", kind: HTTP3_CONNECT, resolver: PROXY, enabled: true,
        host: "proxy.invalid", port: 443, dnsServers: ["192.0.2.53"],
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
        name: "renamed", kind: HTTP3_CONNECT, resolver: PROXY, enabled: false,
        host: "proxy.invalid", port: 443, dnsServers: ["192.0.2.53"]
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
        name: "QUIC gateway", kind: HTTP3_CONNECT, resolver: PROXY, enabled: true,
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
    let tests = response_data(&result)["testVia"].as_array().unwrap().clone();
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
            "OTHER"
        ]
    );
    h.handle.proxy_runtime().unwrap().stop_all().await;
}

#[tokio::test]
async fn feed_policy_is_atomic_preserved_and_blocks_referenced_profile_deletion() {
    let h = harness().await;
    let result = h
        .execute(
            r#"mutation { saveProxyProfile(input: {
        name: "gateway", kind: HTTP_CONNECT, resolver: PROXY, enabled: true, host: "127.0.0.1", port: 8080,
        dnsServers: ["192.0.2.53"]
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
