//! Contract coverage for the additive combined network-access query surface.

mod common;

use async_graphql::Request;
use axum::http::{HeaderMap, HeaderValue};
use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use weaver_server_api::auth::types::NetworkRequestSecurityContext;
use weaver_server_api::auth::{CallerIdentity, CallerScope};
use weaver_server_core::security::{
    AUTHENTICATED_POLICY_REVISION, RuntimeSecurityConfig, SETTING_HTTP_BIND_ADDRESS,
    SETTING_SECURITY_POLICY_REVISION, SETTING_TRUSTED_NETWORKS, parse_ip_or_cidr,
};

const NETWORK_ACCESS_QUERY: &str = r#"query {
    networkAccess {
        authenticatedAccess legacyCompatibility
        trustedNetworks trustedProxies trustedNetworksSource editable envPinned
        rememberedPolicyValid
        currentClient {
            available peer resolvedClient forwardingHeadersIgnored rememberedClientAllowed
        }
        bindAddress { address storedAddress source editable restartRequired }
    }
}"#;

#[tokio::test]
async fn network_access_combines_effective_policy_and_listener_state() {
    let mut security = RuntimeSecurityConfig::default();
    security.set_trusted_cidrs(vec![parse_ip_or_cidr("198.51.100.0/24").unwrap()]);
    security.trusted_proxies = vec![parse_ip_or_cidr("10.0.0.5").unwrap()];
    let harness = TestHarness::new_with_security(security).await;
    let response = harness.execute(NETWORK_ACCESS_QUERY).await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let access = &data["networkAccess"];
    assert_eq!(access["authenticatedAccess"], false);
    assert_eq!(access["legacyCompatibility"], true);
    assert_eq!(
        access["trustedNetworks"],
        serde_json::json!(["198.51.100.0/24"])
    );
    assert_eq!(access["trustedProxies"], serde_json::json!(["10.0.0.5/32"]));
    assert_eq!(access["trustedNetworksSource"], "STORED");
    assert_eq!(access["editable"], true);
    assert_eq!(access["rememberedPolicyValid"], true);
    assert_eq!(access["currentClient"]["available"], false);
    assert!(access["currentClient"]["rememberedClientAllowed"].is_null());
    assert_eq!(access["bindAddress"]["address"], "127.0.0.1");
}

#[tokio::test]
async fn preview_is_non_persistent_and_canonicalizes_the_entire_draft() {
    let harness = TestHarness::new().await;
    let response = harness
        .execute(
            r#"query {
                previewNetworkAccess(input: {
                    trustedNetworks: ["198.51.100.42/24", "2001:db8::1/64"]
                    bindAddress: " 0.0.0.0 "
                }) {
                    trustedNetworks bindAddress restartRequired currentClientAllowed
                }
            }"#,
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let preview = &data["previewNetworkAccess"];
    assert_eq!(
        preview["trustedNetworks"],
        serde_json::json!(["198.51.100.0/24", "2001:db8::/64"])
    );
    assert_eq!(preview["bindAddress"], "0.0.0.0");
    assert_eq!(preview["restartRequired"], true);
    assert!(preview["currentClientAllowed"].is_null());

    let current = harness.execute(NETWORK_ACCESS_QUERY).await;
    assert_no_errors(&current);
    let data = response_data(&current);
    assert!(
        data["networkAccess"]["trustedNetworks"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(data["networkAccess"]["bindAddress"]["storedAddress"].is_null());
}

#[tokio::test]
async fn invalid_or_duplicate_draft_returns_a_structured_error_without_partial_result() {
    let harness = TestHarness::new().await;
    let response = harness
        .execute(
            r#"query {
                previewNetworkAccess(input: {
                    trustedNetworks: ["198.51.100.42/24", "198.51.100.0/24"]
                    bindAddress: "not-an-ip"
                }) { trustedNetworks }
            }"#,
        )
        .await;
    assert_has_errors(&response);
    assert_eq!(
        response.errors[0]
            .extensions
            .as_ref()
            .unwrap()
            .get("code")
            .unwrap(),
        &async_graphql::Value::String("DUPLICATE_TRUSTED_NETWORK".into())
    );
    let data = response_data(&response);
    assert!(data["previewNetworkAccess"].is_null());

    let response = harness
        .execute(
            r#"query {
                previewNetworkAccess(input: {
                    trustedNetworks: ["not-a-cidr"]
                    bindAddress: "127.0.0.1"
                }) { trustedNetworks }
            }"#,
        )
        .await;
    assert_has_errors(&response);
    assert_eq!(
        response.errors[0]
            .extensions
            .as_ref()
            .unwrap()
            .get("code")
            .unwrap(),
        &async_graphql::Value::String("INVALID_TRUSTED_NETWORKS".into())
    );

    let response = harness
        .execute(
            r#"query {
                previewNetworkAccess(input: {
                    trustedNetworks: []
                    bindAddress: "not-an-ip"
                }) { bindAddress }
            }"#,
        )
        .await;
    assert_has_errors(&response);
    assert_eq!(
        response.errors[0]
            .extensions
            .as_ref()
            .unwrap()
            .get("code")
            .unwrap(),
        &async_graphql::Value::String("INVALID_BIND_ADDRESS".into())
    );
}

#[tokio::test]
async fn request_context_reports_the_resolved_forwarded_client_and_draft_admission() {
    let mut security = RuntimeSecurityConfig::default();
    security.set_trusted_cidrs(vec![parse_ip_or_cidr("198.51.100.0/24").unwrap()]);
    security.trusted_proxies = vec![parse_ip_or_cidr("10.0.0.5").unwrap()];
    let harness = TestHarness::new_with_security(security).await;
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-for", HeaderValue::from_static("198.51.100.44"));
    let response = harness
        .schema
        .execute(
            Request::new(
                r#"query {
                    networkAccess {
                        currentClient { available peer resolvedClient forwardingHeadersIgnored rememberedClientAllowed }
                    }
                    previewNetworkAccess(input: { trustedNetworks: ["203.0.113.0/24"] }) {
                        currentClientAllowed
                    }
                }"#,
            )
            .data(CallerScope::Local)
            .data(CallerIdentity::Local([7; 32]))
            .data(NetworkRequestSecurityContext {
                peer: Some("10.0.0.5:443".parse().unwrap()),
                headers,
            }),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(data["networkAccess"]["currentClient"]["available"], true);
    assert_eq!(data["networkAccess"]["currentClient"]["peer"], "10.0.0.5");
    assert_eq!(
        data["networkAccess"]["currentClient"]["resolvedClient"],
        "198.51.100.44"
    );
    assert_eq!(
        data["networkAccess"]["currentClient"]["forwardingHeadersIgnored"],
        false
    );
    assert_eq!(
        data["networkAccess"]["currentClient"]["rememberedClientAllowed"],
        true
    );
    assert_eq!(data["previewNetworkAccess"]["currentClientAllowed"], false);
}

#[tokio::test]
async fn network_queries_remain_admin_only() {
    let harness = TestHarness::new().await;
    for document in [
        NETWORK_ACCESS_QUERY,
        r#"query { previewNetworkAccess(input: { trustedNetworks: [] }) { trustedNetworks } }"#,
    ] {
        let response = harness.execute_as(document, CallerScope::Read).await;
        assert_has_errors(&response);
        assert_eq!(
            response.errors[0]
                .extensions
                .as_ref()
                .unwrap()
                .get("code")
                .unwrap(),
            &async_graphql::Value::String("FORBIDDEN".into())
        );
    }
}

#[tokio::test]
async fn proxy_preview_canonicalizes_bare_addresses_and_rejects_invalid_drafts() {
    let harness = TestHarness::new().await;
    let response = harness.execute(r#"query { previewNetworkAccess(input: { trustedNetworks: [], trustedProxies: ["127.0.0.1"] }) { trustedProxies } }"#).await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["previewNetworkAccess"]["trustedProxies"],
        serde_json::json!(["127.0.0.1/32"])
    );
    let response = harness.execute(r#"query { previewNetworkAccess(input: { trustedNetworks: ["203.0.113.0/24"], trustedProxies: ["invalid"] }) { trustedProxies } }"#).await;
    assert_has_errors(&response);
    assert!(
        harness
            .db
            .get_setting(SETTING_TRUSTED_NETWORKS)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn authenticated_update_is_fresh_admin_only_and_commits_the_policy_together() {
    let security = RuntimeSecurityConfig::default();
    security.apply_stored_access_policy_revision(None, Some(AUTHENTICATED_POLICY_REVISION), true);
    let harness = TestHarness::new_with_security(security).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    harness
        .db
        .create_browser_session(&weaver_server_core::auth::BrowserSession {
            token_hash: "07".repeat(32),
            csrf_verifier: "test-verifier".into(),
            origin: "https://test".into(),
            client_ip: None,
            remembered: false,
            created_at: now,
            expires_at: now + 3600,
            revoked_at: None,
        })
        .unwrap();
    let document = r#"mutation {
        updateNetworkAccess(input: {
            trustedNetworks: ["203.0.113.42/24"]
            trustedProxies: ["10.0.0.5"]
            bindAddress: "0.0.0.0"
        }) {
            authenticatedAccess legacyCompatibility trustedNetworks
            bindAddress { storedAddress restartRequired }
        }
    }"#;

    let denied = harness
        .schema
        .execute(
            Request::new(document)
                .data(CallerScope::Admin)
                .data(CallerIdentity::Local([7; 32])),
        )
        .await;
    assert_has_errors(&denied);
    assert_eq!(
        denied.errors[0]
            .extensions
            .as_ref()
            .unwrap()
            .get("code")
            .unwrap(),
        &async_graphql::Value::String("REAUTH_REQUIRED".into())
    );

    for scope in [CallerScope::Read, CallerScope::Control] {
        let denied = harness
            .schema
            .execute(
                Request::new(document)
                    .data(scope)
                    .data(CallerIdentity::Local([7; 32])),
            )
            .await;
        assert_has_errors(&denied);
        assert_eq!(
            denied.errors[0]
                .extensions
                .as_ref()
                .unwrap()
                .get("code")
                .unwrap(),
            &async_graphql::Value::String("FORBIDDEN".into())
        );
    }

    let response = harness
        .schema
        .execute(
            Request::new(document)
                .data(CallerScope::Admin)
                .data(CallerIdentity::Jwt([7; 32])),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(data["updateNetworkAccess"]["authenticatedAccess"], true);
    assert_eq!(data["updateNetworkAccess"]["legacyCompatibility"], false);
    assert_eq!(
        data["updateNetworkAccess"]["trustedNetworks"],
        serde_json::json!(["203.0.113.0/24"])
    );
    assert_eq!(
        harness.db.get_setting(SETTING_TRUSTED_NETWORKS).unwrap(),
        Some("[\"203.0.113.0/24\"]".to_string())
    );
    assert_eq!(
        harness
            .db
            .get_setting(weaver_server_core::security::SETTING_TRUSTED_PROXIES)
            .unwrap(),
        Some("[\"10.0.0.5/32\"]".into())
    );
    assert_eq!(
        harness.db.get_setting(SETTING_HTTP_BIND_ADDRESS).unwrap(),
        Some("0.0.0.0".to_string())
    );
    assert_eq!(
        harness
            .db
            .get_setting(SETTING_SECURITY_POLICY_REVISION)
            .unwrap(),
        Some(AUTHENTICATED_POLICY_REVISION.to_string())
    );

    let legacy = harness
        .schema
        .execute(
            Request::new(r#"mutation { setAccessPolicy(mode: "no_login") }"#)
                .data(CallerScope::Admin)
                .data(CallerIdentity::Jwt([7; 32])),
        )
        .await;
    assert_has_errors(&legacy);
    assert_eq!(
        legacy.errors[0]
            .extensions
            .as_ref()
            .unwrap()
            .get("code")
            .unwrap(),
        &async_graphql::Value::String("AUTHENTICATED_NETWORK_ACCESS".into())
    );
}
