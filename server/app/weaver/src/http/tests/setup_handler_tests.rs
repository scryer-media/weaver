use super::*;
use weaver_server_core::security::{
    BindAddressSource, RuntimeSecurityConfig, SETTING_ACCESS_MODE, SETTING_HTTP_BIND_ADDRESS,
    SETTING_TRUSTED_NETWORKS,
};

fn setup_test_router(
    db: Database,
    auth_cache: LoginAuthCache,
    security: RuntimeSecurityConfig,
    peer: SocketAddr,
) -> Router {
    Router::new()
        .route("/api/auth/setup", post(auth::setup_handler))
        .layer(axum::extract::connect_info::MockConnectInfo(peer))
        .layer(Extension(db))
        .layer(Extension(security))
        .layer(Extension(auth_cache))
        .layer(Extension(SessionToken(Arc::new(
            "browser-session-token".to_string(),
        ))))
}

fn peer(value: &str) -> SocketAddr {
    value.parse().expect("test peer address is valid")
}

/// `RuntimeSecurityConfig` keeps its trusted-network list private behind a
/// shared lock, so `..default()` update syntax is unavailable outside that
/// crate; the public fields are assigned instead.
fn env_pinned_bind(address: &str) -> RuntimeSecurityConfig {
    let mut security = RuntimeSecurityConfig::default();
    security.http_bind_address = address.parse().expect("test bind address is valid");
    security.bind_address_source = BindAddressSource::Environment;
    security
}

fn loopback_peer() -> SocketAddr {
    peer("127.0.0.1:49152")
}

struct SetupOutcome {
    status: StatusCode,
    payload: serde_json::Value,
    cookies: Vec<String>,
}

async fn post_setup(app: Router, body: serde_json::Value) -> SetupOutcome {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/setup")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let cookies = response
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .map(str::to_string)
        .collect();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    SetupOutcome {
        status,
        payload,
        cookies,
    }
}

fn setting(db: &Database, key: &str) -> Option<String> {
    db.get_setting(key).expect("settings table is readable")
}

/// No access mode, credentials, or bind address stored — a fresh install.
fn assert_nothing_written(db: &Database) {
    assert_eq!(setting(db, SETTING_ACCESS_MODE), None);
    assert_eq!(setting(db, SETTING_TRUSTED_NETWORKS), None);
    assert_eq!(setting(db, SETTING_HTTP_BIND_ADDRESS), None);
    assert!(db.get_auth_credentials().unwrap().is_none());
}

#[tokio::test]
async fn setup_stores_the_mode_and_signs_the_wizard_browser_in() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let app = setup_test_router(
        db.clone(),
        auth_cache.clone(),
        RuntimeSecurityConfig::default(),
        loopback_peer(),
    );

    let outcome = post_setup(
        app,
        serde_json::json!({
            "mode": "login_required",
            "username": "admin",
            "password": test_password(),
        }),
    )
    .await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert_eq!(outcome.payload["ok"], true);
    assert_eq!(outcome.payload["restartRequiredForBind"], false);
    assert_eq!(
        setting(&db, SETTING_ACCESS_MODE).as_deref(),
        Some("login_required")
    );
    assert_eq!(
        db.get_auth_credentials()
            .unwrap()
            .map(|creds| creds.username),
        Some("admin".to_string())
    );
    // Completing setup lands in the app, not at a login form.
    assert!(
        outcome
            .cookies
            .iter()
            .any(|cookie| cookie.starts_with("weaver_jwt="))
    );
    assert_eq!(
        auth_cache.snapshot().map(|creds| creds.username),
        Some("admin".to_string())
    );
}

#[tokio::test]
async fn setup_is_refused_once_credentials_exist() {
    let db = Database::open_in_memory().unwrap();
    db.set_auth_credentials("admin", &hash_password(&test_password()).unwrap())
        .unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
    let app = setup_test_router(
        db.clone(),
        auth_cache,
        RuntimeSecurityConfig::default(),
        loopback_peer(),
    );

    let outcome = post_setup(
        app,
        serde_json::json!({ "mode": "no_login", "bindAddress": "0.0.0.0" }),
    )
    .await;

    assert_eq!(outcome.status, StatusCode::CONFLICT);
    assert_eq!(setting(&db, SETTING_ACCESS_MODE), None);
    assert_eq!(setting(&db, SETTING_HTTP_BIND_ADDRESS), None);
}

#[tokio::test]
async fn setup_refuses_a_remote_peer_but_admits_mapped_loopback() {
    let db = Database::open_in_memory().unwrap();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        RuntimeSecurityConfig::default(),
        peer("203.0.113.9:49152"),
    );

    let outcome = post_setup(
        app,
        serde_json::json!({
            "mode": "login_required",
            "username": "admin",
            "password": test_password(),
        }),
    )
    .await;
    assert_eq!(outcome.status, StatusCode::FORBIDDEN);
    assert_nothing_written(&db);

    // The S7 pin: a dual-stack listener reports the machine's own browser
    // as ::ffff:127.0.0.1, which must be admitted as loopback.
    let db = Database::open_in_memory().unwrap();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        RuntimeSecurityConfig::default(),
        peer("[::ffff:127.0.0.1]:49152"),
    );

    let outcome = post_setup(
        app,
        serde_json::json!({
            "mode": "login_required",
            "username": "admin",
            "password": test_password(),
        }),
    )
    .await;
    assert_eq!(outcome.status, StatusCode::OK);
    assert_eq!(
        setting(&db, SETTING_ACCESS_MODE).as_deref(),
        Some("login_required")
    );
}

#[tokio::test]
async fn credentials_are_required_by_login_modes_and_refused_by_no_login() {
    for mode in ["login_required", "login_except_local"] {
        let db = Database::open_in_memory().unwrap();
        let app = setup_test_router(
            db.clone(),
            LoginAuthCache::default(),
            RuntimeSecurityConfig::default(),
            loopback_peer(),
        );

        let outcome = post_setup(app, serde_json::json!({ "mode": mode })).await;
        assert_eq!(outcome.status, StatusCode::BAD_REQUEST, "{mode}");
        assert_nothing_written(&db);
    }

    // A password must never be silently collected and then ignored.
    let db = Database::open_in_memory().unwrap();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        RuntimeSecurityConfig::default(),
        loopback_peer(),
    );
    let outcome = post_setup(
        app,
        serde_json::json!({
            "mode": "no_login",
            "username": "admin",
            "password": test_password(),
        }),
    )
    .await;
    assert_eq!(outcome.status, StatusCode::BAD_REQUEST);
    assert_nothing_written(&db);
}

#[tokio::test]
async fn no_login_trusts_loopback_immediately_in_every_clone() {
    let db = Database::open_in_memory().unwrap();
    let security = RuntimeSecurityConfig::default();
    // The clone stands in for the router layers already holding a copy:
    // the next request must be admitted without a restart.
    let already_cloned = security.clone();
    assert!(!already_cloned.is_trusted_peer(Some(loopback_peer())));

    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        security,
        loopback_peer(),
    );
    let outcome = post_setup(app, serde_json::json!({ "mode": "no_login" })).await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert!(already_cloned.is_trusted_peer(Some(loopback_peer())));
    assert!(!already_cloned.is_trusted_peer(Some(peer("192.168.1.20:49152"))));
    assert!(db.get_auth_credentials().unwrap().is_none());
}

#[tokio::test]
async fn finishing_setup_marks_the_install_configured_in_every_clone() {
    // Without this the wizard is its own trap: a no-login install stores
    // no credentials and trusts only loopback, so from the router clone
    // serving the LAN browser the instance still looks fresh and the
    // wizard comes back on the next page load — with an endpoint that
    // refuses that browser every time.
    let db = Database::open_in_memory().unwrap();
    let security = RuntimeSecurityConfig::default();
    let already_cloned = security.clone();
    assert!(!already_cloned.security_configured());

    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        security,
        loopback_peer(),
    );
    let outcome = post_setup(app, serde_json::json!({ "mode": "no_login" })).await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert!(already_cloned.security_configured());
}

#[tokio::test]
async fn the_bind_address_is_validated_before_it_is_stored() {
    let db = Database::open_in_memory().unwrap();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        RuntimeSecurityConfig::default(),
        loopback_peer(),
    );
    let outcome = post_setup(
        app,
        serde_json::json!({
            "mode": "login_required",
            "username": "admin",
            "password": test_password(),
            "bindAddress": "not-an-address",
        }),
    )
    .await;
    assert_eq!(outcome.status, StatusCode::BAD_REQUEST);
    assert_nothing_written(&db);

    let db = Database::open_in_memory().unwrap();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        RuntimeSecurityConfig::default(),
        loopback_peer(),
    );
    let outcome = post_setup(
        app,
        serde_json::json!({
            "mode": "login_required",
            "username": "admin",
            "password": test_password(),
            "bindAddress": "0.0.0.0",
        }),
    )
    .await;
    assert_eq!(outcome.status, StatusCode::OK);
    assert_eq!(
        setting(&db, SETTING_HTTP_BIND_ADDRESS).as_deref(),
        Some("0.0.0.0")
    );
    assert_eq!(outcome.payload["restartRequiredForBind"], true);
}

#[tokio::test]
async fn an_env_pinned_bind_is_reported_ignored_rather_than_stored() {
    let db = Database::open_in_memory().unwrap();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        env_pinned_bind("0.0.0.0"),
        loopback_peer(),
    );

    let outcome = post_setup(
        app,
        serde_json::json!({
            "mode": "login_required",
            "username": "admin",
            "password": test_password(),
            "bindAddress": "192.0.2.10",
        }),
    )
    .await;

    assert_eq!(outcome.status, StatusCode::OK);
    // Storing a value the process will never read is a lie waiting to be
    // discovered; the wizard is told instead.
    assert_eq!(setting(&db, SETTING_HTTP_BIND_ADDRESS), None);
    assert_eq!(outcome.payload["bindIgnoredBecauseEnvPinned"], true);
    assert_eq!(outcome.payload["restartRequiredForBind"], false);
}

#[tokio::test]
async fn an_invalid_trusted_network_list_is_refused_whole() {
    let db = Database::open_in_memory().unwrap();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        RuntimeSecurityConfig::default(),
        loopback_peer(),
    );

    let outcome = post_setup(
        app,
        serde_json::json!({
            "mode": "login_except_local",
            "username": "admin",
            "password": test_password(),
            "trustedNetworks": ["192.168.1.0/24", "not-a-cidr"],
        }),
    )
    .await;

    // All-or-nothing: one bad entry must not admit a partial trust list.
    assert_eq!(outcome.status, StatusCode::BAD_REQUEST);
    assert_nothing_written(&db);
}

#[tokio::test]
async fn an_env_pinned_trust_list_is_never_overridden_by_the_wizard() {
    // WEAVER_TRUSTED_CIDRS pins the browser-access policy exactly as the
    // bind variable pins the address: the wizard may still create the
    // credentials — the half the environment did not answer — but its
    // policy answer is neither stored nor applied live, and no-login
    // (which would be a total no-op) is refused outright.
    let pinned = || {
        let mut security = RuntimeSecurityConfig::default();
        security.trust_env_pinned = true;
        security.set_trusted_cidrs(vec!["10.0.0.0/8".parse().unwrap()]);
        security
    };

    let db = Database::open_in_memory().unwrap();
    let security = pinned();
    let live_clone = security.clone();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        security,
        loopback_peer(),
    );
    let outcome = post_setup(
        app,
        serde_json::json!({
            "mode": "login_except_local",
            "username": "admin",
            "password": test_password(),
            "trustedNetworks": ["192.168.0.0/16"],
        }),
    )
    .await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert_eq!(outcome.payload["accessPolicyIgnoredBecauseEnvPinned"], true);
    // Credentials landed; the policy did not.
    assert!(db.get_auth_credentials().unwrap().is_some());
    assert_eq!(setting(&db, SETTING_ACCESS_MODE), None);
    assert_eq!(setting(&db, SETTING_TRUSTED_NETWORKS), None);
    // The live list is still the environment's, not the wizard's.
    assert!(live_clone.is_trusted_peer(Some(peer("10.1.2.3:49152"))));
    assert!(!live_clone.is_trusted_peer(Some(peer("192.168.1.20:49152"))));

    let db = Database::open_in_memory().unwrap();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        pinned(),
        loopback_peer(),
    );
    let outcome = post_setup(app, serde_json::json!({ "mode": "no_login" })).await;
    assert_eq!(outcome.status, StatusCode::BAD_REQUEST);
    assert_nothing_written(&db);
}

#[tokio::test]
async fn a_no_login_setup_hands_its_own_browser_the_trusted_session_cookie() {
    let db = Database::open_in_memory().unwrap();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        RuntimeSecurityConfig::default(),
        loopback_peer(),
    );

    let outcome = post_setup(app, serde_json::json!({ "mode": "no_login" })).await;

    assert_eq!(outcome.status, StatusCode::OK);
    // This page does not reload when the operator restarts from it, so the
    // cookie the next page load would set has to arrive here.
    assert!(
        outcome
            .cookies
            .iter()
            .any(|cookie| cookie.starts_with("weaver_session=")),
        "{:?}",
        outcome.cookies
    );
    assert!(
        !outcome
            .cookies
            .iter()
            .any(|cookie| cookie.starts_with("weaver_jwt=")),
    );
}

#[tokio::test]
async fn setup_reports_whether_the_wizard_may_offer_a_restart() {
    let db = Database::open_in_memory().unwrap();
    let app = setup_test_router(
        db.clone(),
        LoginAuthCache::default(),
        RuntimeSecurityConfig::default(),
        loopback_peer(),
    );

    let outcome = post_setup(
        app,
        serde_json::json!({ "mode": "no_login", "bindAddress": "0.0.0.0" }),
    )
    .await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert_eq!(outcome.payload["restartRequiredForBind"], true);
    // The rule is the deployment's, so the answer depends on where the
    // test runs; the contract is that the wizard is told either way.
    assert!(outcome.payload["restartSupported"].is_boolean());
    if outcome.payload["restartSupported"] == false {
        assert!(outcome.payload["restartUnsupportedReason"].is_string());
    }
}
