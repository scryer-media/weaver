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
        name: "renamed", kind: SOCKS5, enabled: false, host: "127.0.0.1", port: 1080,
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
async fn feed_policy_is_atomic_preserved_and_blocks_referenced_profile_deletion() {
    let h = harness().await;
    let result = h
        .execute(
            r#"mutation { saveProxyProfile(input: {
        name: "gateway", kind: HTTP_CONNECT, enabled: true, host: "127.0.0.1", port: 8080,
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
