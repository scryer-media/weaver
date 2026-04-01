mod common;

use async_graphql::Value;
use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use weaver_api::auth::CallerScope;

#[tokio::test]
async fn create_control_api_key() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                createApiKey(name: "test", scope: CONTROL) {
                    rawKey
                    key { id name scope createdAt }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let result = &data["createApiKey"];
    assert!(!result["rawKey"].as_str().unwrap().is_empty());
    assert!(result["key"]["id"].as_i64().is_some());
    assert_eq!(result["key"]["name"].as_str().unwrap(), "test");
    assert_eq!(result["key"]["scope"].as_str().unwrap(), "CONTROL");
    assert!(result["key"]["createdAt"].as_f64().unwrap() > 1_000_000_000_000.0);
}

#[tokio::test]
async fn create_admin_api_key() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                createApiKey(name: "admin-key", scope: ADMIN) {
                    rawKey
                    key { id name scope }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let result = &data["createApiKey"];
    assert!(!result["rawKey"].as_str().unwrap().is_empty());
    assert_eq!(result["key"]["name"].as_str().unwrap(), "admin-key");
    assert_eq!(result["key"]["scope"].as_str().unwrap(), "ADMIN");
}

#[tokio::test]
async fn list_api_keys_hides_raw_key() {
    let h = TestHarness::new().await;

    // Create a key first.
    let resp = h
        .execute(
            r#"mutation {
                createApiKey(name: "listed", scope: CONTROL) {
                    rawKey
                    key { id }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);

    // List keys — rawKey is not a field on ApiKey.
    let resp = h
        .execute(r#"{ apiKeys { id name scope createdAt lastUsedAt } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let keys = data["apiKeys"].as_array().unwrap();
    assert_eq!(keys.len(), 1);
    let key = &keys[0];
    assert!(key["id"].as_i64().is_some());
    assert_eq!(key["name"].as_str().unwrap(), "listed");
    assert_eq!(key["scope"].as_str().unwrap(), "CONTROL");
    assert!(key["createdAt"].as_f64().unwrap() > 1_000_000_000_000.0);
    assert!(key["lastUsedAt"].is_null());
}

#[tokio::test]
async fn delete_api_key() {
    let h = TestHarness::new().await;

    // Create a key.
    let resp = h
        .execute(
            r#"mutation {
                createApiKey(name: "doomed", scope: CONTROL) {
                    key { id }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let id = data["createApiKey"]["key"]["id"].as_i64().unwrap();

    // Delete it.
    let resp = h
        .execute(&format!(
            r#"mutation {{ deleteApiKey(id: {id}) {{ id }} }}"#
        ))
        .await;
    assert_no_errors(&resp);

    // List should be empty.
    let resp = h.execute(r#"{ apiKeys { id } }"#).await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let keys = data["apiKeys"].as_array().unwrap();
    assert!(keys.is_empty());
}

#[tokio::test]
async fn delete_nonexistent_api_key() {
    let h = TestHarness::new().await;

    // Deleting a nonexistent key should be idempotent (no error).
    let resp = h
        .execute(r#"mutation { deleteApiKey(id: 999) { id } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let remaining = data["deleteApiKey"].as_array().unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn enable_login() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { enableLogin(username: "admin", password: "pass") }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["enableLogin"].as_bool().unwrap());
    let creds = h.db.get_auth_credentials().unwrap().unwrap();
    assert_eq!(creds.username, "admin");
    assert!(creds.password_hash.starts_with("$argon2id$"));
    let cached = h.auth_cache.snapshot().unwrap();
    assert_eq!(cached.username, "admin");
    assert_eq!(cached.password_hash, creds.password_hash);
}

#[tokio::test]
async fn enable_login_empty_username() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { enableLogin(username: "", password: "p") }"#)
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn enable_login_empty_password() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { enableLogin(username: "admin", password: "") }"#)
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn login_status_when_enabled() {
    let h = TestHarness::new().await;

    // Enable login first.
    let resp = h
        .execute(r#"mutation { enableLogin(username: "admin", password: "pass") }"#)
        .await;
    assert_no_errors(&resp);

    // Check status.
    let resp = h
        .execute(r#"{ adminLoginStatus { enabled username } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let status = &data["adminLoginStatus"];
    assert!(status["enabled"].as_bool().unwrap());
    assert_eq!(status["username"].as_str().unwrap(), "admin");
}

#[tokio::test]
async fn login_status_when_disabled() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"{ adminLoginStatus { enabled username } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let status = &data["adminLoginStatus"];
    assert!(!status["enabled"].as_bool().unwrap());
}

#[tokio::test]
async fn disable_login() {
    let h = TestHarness::new().await;

    // Enable first.
    let resp = h
        .execute(r#"mutation { enableLogin(username: "admin", password: "pass") }"#)
        .await;
    assert_no_errors(&resp);

    // Disable.
    let resp = h.execute(r#"mutation { disableLogin }"#).await;
    assert_no_errors(&resp);

    // Verify disabled.
    let resp = h.execute(r#"{ adminLoginStatus { enabled } }"#).await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(!data["adminLoginStatus"]["enabled"].as_bool().unwrap());
    assert!(h.db.get_auth_credentials().unwrap().is_none());
    assert!(h.auth_cache.snapshot().is_none());
}

#[tokio::test]
async fn change_password_correct() {
    let h = TestHarness::new().await;

    // Enable login.
    let resp = h
        .execute(r#"mutation { enableLogin(username: "admin", password: "pass") }"#)
        .await;
    assert_no_errors(&resp);
    let old_hash = h.db.get_auth_credentials().unwrap().unwrap().password_hash;

    // Change password.
    let resp = h
        .execute(r#"mutation { changePassword(currentPassword: "pass", newPassword: "new") }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["changePassword"].as_bool().unwrap());
    let creds = h.db.get_auth_credentials().unwrap().unwrap();
    assert!(creds.password_hash.starts_with("$argon2id$"));
    assert_ne!(creds.password_hash, old_hash);
    let cached = h.auth_cache.snapshot().unwrap();
    assert_eq!(cached.password_hash, creds.password_hash);
}

#[tokio::test]
async fn change_password_wrong_current() {
    let h = TestHarness::new().await;

    // Enable login.
    let resp = h
        .execute(r#"mutation { enableLogin(username: "admin", password: "pass") }"#)
        .await;
    assert_no_errors(&resp);

    // Try with wrong current password.
    let resp = h
        .execute(r#"mutation { changePassword(currentPassword: "wrong", newPassword: "new") }"#)
        .await;
    assert_has_errors(&resp);
    let err_msg = resp.errors[0].message.to_lowercase();
    assert!(
        err_msg.contains("incorrect"),
        "expected 'incorrect' in error: {err_msg}"
    );
}

#[tokio::test]
async fn change_password_empty_new() {
    let h = TestHarness::new().await;

    // Enable login.
    let resp = h
        .execute(r#"mutation { enableLogin(username: "admin", password: "pass") }"#)
        .await;
    assert_no_errors(&resp);

    // Try with empty new password.
    let resp = h
        .execute(r#"mutation { changePassword(currentPassword: "pass", newPassword: "") }"#)
        .await;
    assert_has_errors(&resp);
    let err_msg = resp.errors[0].message.to_lowercase();
    assert!(
        err_msg.contains("must not be empty"),
        "expected 'must not be empty' in error: {err_msg}"
    );
}

#[tokio::test]
async fn change_password_when_not_enabled() {
    let h = TestHarness::new().await;

    // Try changing password without enabling login.
    let resp = h
        .execute(r#"mutation { changePassword(currentPassword: "pass", newPassword: "new") }"#)
        .await;
    assert_has_errors(&resp);
    let err_msg = resp.errors[0].message.to_lowercase();
    assert!(
        err_msg.contains("login is not enabled"),
        "expected 'login is not enabled' in error: {err_msg}"
    );
}

#[tokio::test]
async fn read_scope_can_query_public_facade() {
    let h = TestHarness::new().await;
    let resp = h
        .execute_as(
            r#"{ queueSummary { totalItems } systemStatus { version } globalQueueState { isPaused } }"#,
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&resp);
}

#[tokio::test]
async fn read_scope_cannot_submit() {
    let h = TestHarness::new().await;
    let nzb_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        common::make_test_nzb("read-scope-submit").as_bytes(),
    );

    let resp = h
        .execute_as(
            &format!(
                r#"mutation {{
                    submitNzb(input: {{ nzbBase64: "{nzb_b64}" }}) {{
                        accepted
                    }}
                }}"#
            ),
            CallerScope::Read,
        )
        .await;

    assert_has_errors(&resp);
    assert_eq!(
        resp.errors[0]
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.get("code"))
            .and_then(|value| match value {
                Value::String(code) => Some(code.as_str()),
                _ => None,
            }),
        Some("FORBIDDEN")
    );
}

#[tokio::test]
async fn control_scope_can_submit() {
    let h = TestHarness::new().await;
    let nzb_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        common::make_test_nzb("control-scope-submit").as_bytes(),
    );

    let resp = h
        .execute_as(
            &format!(
                r#"mutation {{
                    submitNzb(input: {{ nzbBase64: "{nzb_b64}", clientRequestId: "scope-1" }}) {{
                        accepted
                        clientRequestId
                        item {{ id state }}
                    }}
                }}"#
            ),
            CallerScope::Control,
        )
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["submitNzb"]["accepted"].as_bool().unwrap());
    assert_eq!(
        data["submitNzb"]["clientRequestId"].as_str().unwrap(),
        "scope-1"
    );
    assert_eq!(
        data["submitNzb"]["item"]["state"].as_str().unwrap(),
        "QUEUED"
    );
}
