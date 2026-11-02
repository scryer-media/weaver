// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The GraphQL endpoint's refusals before the schema runs: each keeps its
// status and carries a stable code a machine client can match.

use super::super::graphql::refusal;
use super::*;
use weaver_server_core::security::RuntimeSecurityConfig;

const VALID_KEY: &str = "fixture-graphql-key";

// A schema with no runtime data behind it. Refused requests never reach it,
// and `__typename` resolves without any.
fn bare_schema() -> weaver_server_api::WeaverSchema {
    async_graphql::Schema::build(
        weaver_server_api::QueryRoot::default(),
        weaver_server_api::MutationRoot::default(),
        weaver_server_api::SubscriptionRoot::default(),
    )
    .finish()
}

fn graphql_router(db: Database, security: Arc<RuntimeSecurityConfig>) -> Router {
    Router::new()
        .route("/graphql", post(super::super::graphql::graphql_handler))
        .layer(Extension(bare_schema()))
        .layer(Extension(RequestAuthContext {
            db,
            auth_cache: LoginAuthCache::default(),
            api_key_cache: ApiKeyCache::default(),
            session_token: SessionToken(Arc::new("fixture-process-token".to_string())),
            security,
        }))
}

fn db_with_key() -> Database {
    let db = Database::open_in_memory().unwrap();
    db.insert_api_key("fixture-graphql", &hash_api_key(VALID_KEY), "admin")
        .unwrap();
    db
}

fn graphql_request(headers: &[(header::HeaderName, &str)]) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header(header::CONTENT_TYPE, "application/json");
    for (name, value) in headers {
        builder = builder.header(name, *value);
    }
    builder
        .body(Body::from(r#"{"query":"{ __typename }"}"#))
        .unwrap()
}

async fn send(app: Router, request: Request<Body>) -> (StatusCode, serde_json::Value) {
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload = serde_json::from_slice(&body)
        .unwrap_or_else(|error| panic!("body is not JSON ({error}): {body:?}"));
    (status, payload)
}

#[test]
fn refusal_codes_are_distinct_screaming_snake() {
    let mut seen = std::collections::BTreeSet::new();
    for code in refusal::ALL {
        assert!(seen.insert(*code), "duplicate code {code}");
        assert!(
            !code.is_empty()
                && code
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'),
            "{code} is not SCREAMING_SNAKE"
        );
    }
}

#[tokio::test]
async fn wrong_bearer_key_is_refused_with_invalid_api_key_code() {
    let app = graphql_router(db_with_key(), Arc::new(RuntimeSecurityConfig::default()));
    let (status, payload) = send(
        app,
        graphql_request(&[(header::AUTHORIZATION, "Bearer fixture-wrong-key")]),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{payload}");
    assert_eq!(payload["code"], "INVALID_API_KEY", "{payload}");
    assert!(payload["error"].is_string(), "{payload}");
}

#[tokio::test]
async fn wrong_x_api_key_is_refused_with_invalid_api_key_code() {
    let app = graphql_router(db_with_key(), Arc::new(RuntimeSecurityConfig::default()));
    let (status, payload) = send(
        app,
        graphql_request(&[(
            header::HeaderName::from_static("x-api-key"),
            "fixture-wrong-key",
        )]),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{payload}");
    assert_eq!(payload["code"], refusal::INVALID_API_KEY, "{payload}");
}

#[tokio::test]
async fn missing_credentials_are_refused_with_authentication_required_code() {
    let app = graphql_router(db_with_key(), Arc::new(RuntimeSecurityConfig::default()));
    let (status, payload) = send(app, graphql_request(&[])).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{payload}");
    assert_eq!(payload["code"], "AUTHENTICATION_REQUIRED", "{payload}");
    assert!(payload["error"].is_string(), "{payload}");
}

#[tokio::test]
async fn browser_session_without_csrf_is_refused_with_browser_verification_code() {
    let db = db_with_key();
    let security = Arc::new(RuntimeSecurityConfig::default());
    security.apply_stored_access_policy_revision(None, None, false);
    assert!(security.authenticated_access_mode());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let browser_token = "fixture-graphql-browser";
    db.create_browser_session(&weaver_server_core::auth::BrowserSession {
        token_hash: hash_api_key(browser_token)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        csrf_verifier: "fixture-csrf".to_string(),
        origin: "http://localhost".to_string(),
        client_ip: None,
        remembered: false,
        created_at: now,
        expires_at: now + 3_600,
        revoked_at: None,
    })
    .unwrap();
    let app = graphql_router(db, security);
    let (status, payload) = send(
        app,
        graphql_request(&[
            (header::COOKIE, "weaver_session=fixture-graphql-browser"),
            (header::ORIGIN, "http://localhost"),
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{payload}");
    assert_eq!(
        payload["code"], "BROWSER_VERIFICATION_REQUIRED",
        "{payload}"
    );
}

#[tokio::test]
async fn valid_bearer_key_reaches_the_schema() {
    let app = graphql_router(db_with_key(), Arc::new(RuntimeSecurityConfig::default()));
    let (status, payload) = send(
        app,
        graphql_request(&[(header::AUTHORIZATION, &format!("Bearer {VALID_KEY}"))]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{payload}");
    assert_eq!(payload["data"]["__typename"], "QueryRoot", "{payload}");
}
