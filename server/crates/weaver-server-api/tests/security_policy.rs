//! The browser-admission and bind-address surface: who may read or change it,
//! and which combinations the server refuses outright rather than storing a
//! setting the next start will reject.

mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use weaver_server_api::auth::CallerScope;
use weaver_server_core::security::{BindAddressSource, RuntimeSecurityConfig};

const ACCESS_POLICY_QUERY: &str = r#"query {
    accessPolicy { mode trustedNetworks editable envPinned configured strictSecurity }
}"#;

const HTTP_BIND_ADDRESS_QUERY: &str = r#"query {
    httpBindAddress { address storedAddress source editable exposedWithoutLogin restartRequired }
}"#;

const SET_ACCESS_POLICY: &str = r#"mutation { setAccessPolicy(mode: "login_required") }"#;

const SET_BIND_ADDRESS: &str = r#"mutation { setHttpBindAddress(address: "127.0.0.1") }"#;

/// `RuntimeSecurityConfig` keeps its trusted-network list private behind a
/// shared lock, so `..default()` update syntax is unavailable outside that
/// crate; the public fields are assigned instead.
fn strict_security() -> RuntimeSecurityConfig {
    let mut security = RuntimeSecurityConfig::default();
    security.strict_security = true;
    security
}

fn trust_env_pinned() -> RuntimeSecurityConfig {
    let mut security = RuntimeSecurityConfig::default();
    security.trust_env_pinned = true;
    security
}

fn bind_env_pinned() -> RuntimeSecurityConfig {
    let mut security = RuntimeSecurityConfig::default();
    security.http_bind_address = "0.0.0.0".parse().expect("test bind address is valid");
    security.bind_address_source = BindAddressSource::Environment;
    security
}

fn error_messages(response: &async_graphql::Response) -> String {
    response
        .errors
        .iter()
        .map(|error| error.message.clone())
        .collect::<Vec<_>>()
        .join("; ")
}

async fn assert_admin_only(harness: &TestHarness, document: &str) {
    for scope in [CallerScope::Read, CallerScope::Control] {
        let response = harness.execute_as(document, scope).await;
        assert!(
            response
                .errors
                .iter()
                .any(|error| error.message.contains("admin scope required")),
            "{scope:?} was not refused by AdminGuard for {document}: {:?}",
            response.errors
        );
    }
    for scope in [CallerScope::Admin, CallerScope::Local] {
        let response = harness.execute_as(document, scope).await;
        assert_no_errors(&response);
    }
}

#[tokio::test]
async fn the_security_surface_is_admin_only() {
    // Nothing else pins these: a Read-scoped *arr key must not be able to read
    // the trust list, and a Control-scoped one must not be able to widen it.
    let harness = TestHarness::new().await;
    assert_admin_only(&harness, ACCESS_POLICY_QUERY).await;
    assert_admin_only(&harness, HTTP_BIND_ADDRESS_QUERY).await;
    assert_admin_only(&harness, SET_ACCESS_POLICY).await;
    assert_admin_only(&harness, SET_BIND_ADDRESS).await;
}

#[tokio::test]
async fn an_environment_pin_refuses_the_edit_and_names_the_variable() {
    // Refused rather than stored-and-ignored: the operator who pinned it in
    // their deployment must be told, not left to discover it after a restart.
    let harness = TestHarness::new_with_security(trust_env_pinned()).await;

    // And the policy reads back read-only, which is what keeps the upgrade
    // wizard away from an environment-managed deployment entirely.
    let response = harness.execute(ACCESS_POLICY_QUERY).await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(data["accessPolicy"]["editable"], false);
    assert_eq!(data["accessPolicy"]["envPinned"], true);
    assert_eq!(data["accessPolicy"]["mode"], "env");

    let response = harness
        .execute(r#"mutation { setAccessPolicy(mode: "login_except_local") }"#)
        .await;
    assert_has_errors(&response);
    assert!(
        error_messages(&response).contains("WEAVER_TRUSTED_CIDRS"),
        "policy refusal must name the variable: {}",
        error_messages(&response)
    );

    let harness = TestHarness::new_with_security(bind_env_pinned()).await;
    let response = harness
        .execute(r#"mutation { setHttpBindAddress(address: "127.0.0.1") }"#)
        .await;
    assert_has_errors(&response);
    assert!(
        error_messages(&response).contains("WEAVER_HTTP_BIND_ADDRESS"),
        "bind refusal must name the variable: {}",
        error_messages(&response)
    );
}

#[tokio::test]
async fn strict_security_refuses_trusting_modes_and_exposed_binds() {
    let harness = TestHarness::new_with_security(strict_security()).await;

    let response = harness
        .execute(r#"mutation { setAccessPolicy(mode: "login_except_local") }"#)
        .await;
    assert_has_errors(&response);

    // A time bomb otherwise: the value stores fine and the next boot refuses
    // to start on it.
    let response = harness
        .execute(r#"mutation { setHttpBindAddress(address: "0.0.0.0") }"#)
        .await;
    assert_has_errors(&response);

    // The S7 pin: the IPv4-mapped spelling of loopback is still this machine,
    // so strict security has nothing to refuse.
    let response = harness
        .execute(r#"mutation { setHttpBindAddress(address: "::ffff:127.0.0.1") }"#)
        .await;
    assert_no_errors(&response);
}

#[tokio::test]
async fn no_login_is_refused_while_a_login_exists() {
    let harness = TestHarness::new().await;
    let response = harness
        .execute(r#"mutation { enableLogin(username: "admin", password: "pass") }"#)
        .await;
    assert_no_errors(&response);

    // Switching to no-login while a password is stored would strand a
    // credential that silently stops mattering; the order is forced instead.
    let response = harness
        .execute(r#"mutation { setAccessPolicy(mode: "no_login") }"#)
        .await;
    assert_has_errors(&response);
    assert!(
        error_messages(&response).contains("disable login"),
        "refusal must say what to do first: {}",
        error_messages(&response)
    );
}

#[tokio::test]
async fn a_stored_policy_applies_live_and_marks_the_install_configured() {
    let harness = TestHarness::new().await;

    // The upgrade trigger: a fresh database has no stored mode, so the query's
    // "login_required" is a default rather than a decision.
    let response = harness.execute(ACCESS_POLICY_QUERY).await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(data["accessPolicy"]["configured"], false);
    assert_eq!(data["accessPolicy"]["mode"], "login_required");
    assert_eq!(data["accessPolicy"]["editable"], true);
    assert_eq!(data["accessPolicy"]["strictSecurity"], false);

    let response = harness
        .execute(
            r#"mutation {
                setAccessPolicy(mode: "login_except_local", trustedNetworks: ["10.1.0.0/16"])
            }"#,
        )
        .await;
    assert_no_errors(&response);

    let response = harness.execute(ACCESS_POLICY_QUERY).await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(data["accessPolicy"]["configured"], true);
    assert_eq!(data["accessPolicy"]["mode"], "login_except_local");
    assert_eq!(data["accessPolicy"]["trustedNetworks"][0], "10.1.0.0/16");

    // Live, not next-restart: the shared list every router clone reads.
    assert!(
        harness
            .security
            .is_trusted_peer(Some("10.1.4.4:49152".parse().unwrap()))
    );
    assert!(
        !harness
            .security
            .is_trusted_peer(Some("192.168.1.20:49152".parse().unwrap()))
    );
}

#[tokio::test]
async fn keeping_the_current_setup_is_what_stops_the_wizard_returning() {
    // The upgrader's one-click exit stores the mode it was already defaulting
    // to. `configured` must still flip — reading `mode` alone would see no
    // change and ask again on every start.
    let harness = TestHarness::new().await;
    let data = response_data(&harness.execute(ACCESS_POLICY_QUERY).await);
    assert_eq!(data["accessPolicy"]["mode"], "login_required");
    assert_eq!(data["accessPolicy"]["configured"], false);

    let response = harness
        .execute(r#"mutation { setAccessPolicy(mode: "login_required") }"#)
        .await;
    assert_no_errors(&response);

    let data = response_data(&harness.execute(ACCESS_POLICY_QUERY).await);
    assert_eq!(data["accessPolicy"]["mode"], "login_required");
    assert_eq!(data["accessPolicy"]["configured"], true);
    // Login-required trusts nothing, which is the pre-upgrade behaviour.
    assert!(harness.security.trusted_cidrs().is_empty());
}
