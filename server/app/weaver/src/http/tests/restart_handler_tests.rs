use super::*;
use weaver_server_core::runtime::restart::{RestartCapability, RestartController};

/// Long enough to cover the handler's response grace, so "nothing was
/// requested" is a settled fact rather than a race.
const NOT_REQUESTED_WINDOW: std::time::Duration = std::time::Duration::from_millis(750);

fn restart_test_router(controller: RestartController, api_key_cache: ApiKeyCache) -> Router {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let session_token = SessionToken(Arc::new("browser-session-token".to_string()));
    let security = weaver_server_core::security::RuntimeSecurityConfig::default();
    security.set_trusted_cidrs(vec!["127.0.0.0/8".parse().unwrap()]);
    let request_auth = RequestAuthContext {
        db: db.clone(),
        auth_cache: auth_cache.clone(),
        api_key_cache: api_key_cache.clone(),
        session_token: session_token.clone(),
        security: Arc::new(security.clone()),
    };
    let peer_addr: SocketAddr = "127.0.0.1:49152".parse().unwrap();

    Router::new()
        .route("/api/system/restart", post(system::restart_handler))
        // The peer the trusted-browser rule is judged on, in the shape
        // `into_make_service_with_connect_info` produces.
        .layer(Extension(axum::extract::ConnectInfo(peer_addr)))
        .layer(Extension(controller))
        .layer(Extension(request_auth))
        .layer(Extension(db))
        .layer(Extension(auth_cache))
        .layer(Extension(api_key_cache))
        .layer(Extension(security))
        .layer(Extension(session_token))
}

async fn post_restart(app: Router, header: (&str, &str)) -> (StatusCode, serde_json::Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/restart")
                .header(header.0, header.1)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

#[tokio::test]
async fn an_admin_key_and_a_trusted_browser_may_restart() {
    let controller = RestartController::with_capability_source(RestartCapability::supported);
    let app = restart_test_router(controller.clone(), api_key_cache("admin-key", "admin"));

    let (status, payload) = post_restart(app.clone(), ("x-api-key", "admin-key")).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(payload["ok"], true);

    // The response has to reach the browser before the process goes away,
    // so the request is made after a short grace period.
    tokio::time::timeout(std::time::Duration::from_secs(5), controller.requested())
        .await
        .expect("an accepted restart reaches the serve loop");

    let (status, _) = post_restart(
        app,
        (
            header::COOKIE.as_str(),
            "weaver_session=browser-session-token",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
}

#[tokio::test]
async fn read_and_control_keys_and_anonymous_callers_may_not() {
    let controller = RestartController::with_capability_source(RestartCapability::supported);

    for (raw_key, scope, expected) in [
        ("read-key", "read", StatusCode::FORBIDDEN),
        ("control-key", "control", StatusCode::FORBIDDEN),
    ] {
        let app = restart_test_router(controller.clone(), api_key_cache(raw_key, scope));
        let (status, _) = post_restart(app, ("x-api-key", raw_key)).await;
        assert_eq!(status, expected, "{scope}");
    }

    let app = restart_test_router(controller.clone(), ApiKeyCache::default());
    let (status, _) = post_restart(app, ("x-api-key", "unknown-key")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    assert!(
        tokio::time::timeout(NOT_REQUESTED_WINDOW, controller.requested())
            .await
            .is_err(),
        "a refused caller must not reach the serve loop"
    );
}

#[tokio::test]
async fn a_deployment_that_must_not_exit_is_refused_before_anything_happens() {
    let controller = RestartController::with_capability_source(|| {
        RestartCapability::unsupported(
            "Weaver is running in a Docker container, where the container runtime decides \
             restarts. Restart the container instead.",
        )
    });
    let app = restart_test_router(controller.clone(), api_key_cache("admin-key", "admin"));

    let (status, payload) = post_restart(app, ("x-api-key", "admin-key")).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        payload["error"]
            .as_str()
            .expect("the refusal explains itself")
            .contains("container")
    );
    assert!(
        tokio::time::timeout(NOT_REQUESTED_WINDOW, controller.requested())
            .await
            .is_err(),
        "a container must never be asked to exit"
    );
}
