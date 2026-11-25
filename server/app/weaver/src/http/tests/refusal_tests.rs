// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The `/api/*` handlers' refusals before they do any work: each keeps its
// status and carries the same stable code the GraphQL endpoint uses.

use super::super::graphql::refusal;
use super::*;

const READ_KEY: &str = "fixture-refusal-read-key";
const ADMIN_KEY: &str = "fixture-refusal-admin-key";

fn request_auth(api_key_cache: ApiKeyCache) -> RequestAuthContext {
    RequestAuthContext {
        db: Database::open_in_memory().unwrap(),
        auth_cache: LoginAuthCache::default(),
        api_key_cache,
        session_token: SessionToken(Arc::new("fixture-process-token".to_string())),
        security: Arc::new(weaver_server_core::security::RuntimeSecurityConfig::default()),
    }
}

fn request(method: &str, uri: &str, key: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(key) = key {
        builder = builder.header("x-api-key", key);
    }
    builder.body(Body::from("{}")).unwrap()
}

async fn refusal_of(response: Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body)
        .unwrap_or_else(|error| panic!("body is not JSON ({error}): {body:?}"));
    assert!(payload["error"].is_string(), "{payload}");
    (status, payload)
}

async fn send(app: Router, request: Request<Body>) -> (StatusCode, serde_json::Value) {
    refusal_of(app.oneshot(request).await.unwrap()).await
}

#[tokio::test]
async fn job_downloads_refuse_with_a_code() {
    let app = job_nzb_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle().handle,
    );

    let (status, payload) = send(app.clone(), request("GET", "/api/jobs/1/nzb", None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(payload["code"], refusal::AUTHENTICATION_REQUIRED);

    let output_file = Request::builder()
        .method("POST")
        .uri("/api/jobs/1/output-file")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("x-api-key", "fixture-wrong-key")
        .body(Body::from("path=fixture.mkv"))
        .unwrap();
    let (status, payload) = send(app, output_file).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(payload["code"], refusal::INVALID_API_KEY);
}

#[tokio::test]
async fn auth_actions_refuse_with_a_code() {
    let peer_addr: SocketAddr = "127.0.0.1:49152".parse().unwrap();
    let app = Router::new()
        .route(
            "/api/auth/verify-password",
            post(auth::verify_password_handler),
        )
        .route("/api/auth/signout-all", post(auth::sign_out_all_handler))
        .layer(axum::extract::connect_info::MockConnectInfo(peer_addr))
        .layer(Extension(auth::LoginRateLimiter::default()))
        .layer(Extension(request_auth(api_key_cache(READ_KEY, "read"))));

    let verify = Request::builder()
        .method("POST")
        .uri("/api/auth/verify-password")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::json!({ "password": test_password() }).to_string(),
        ))
        .unwrap();
    let (status, payload) = send(app.clone(), verify).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(payload["code"], refusal::AUTHENTICATION_REQUIRED);

    let (status, payload) = send(
        app.clone(),
        request("POST", "/api/auth/signout-all", Some("fixture-wrong-key")),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(payload["code"], refusal::INVALID_API_KEY);

    let (status, payload) = send(
        app,
        request("POST", "/api/auth/signout-all", Some(READ_KEY)),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(payload["code"], refusal::FORBIDDEN);
}

#[tokio::test]
async fn backup_reads_refuse_with_a_code() {
    let config = test_config();
    let handle = test_scheduler_handle().handle;
    let db = Database::open_in_memory().unwrap();
    let rss = weaver_server_api::RssService::new(handle.clone(), config.clone(), db.clone());
    let service = weaver_server_api::BackupService::new(
        handle,
        config,
        db.clone(),
        rss,
        std::env::temp_dir(),
    );
    let keys = api_key_cache(READ_KEY, "read");
    keys.upsert(ApiKeyAuthRow {
        key_hash: hash_api_key(ADMIN_KEY),
        id: 43,
        scope: "admin".to_string(),
    });
    let app = Router::new()
        .route("/api/backup/status", get(backup::backup_status_handler))
        .route(
            "/api/backup/stored/{filename}",
            get(backup::backup_download_handler),
        )
        .layer(Extension(service))
        .layer(Extension(request_auth(keys)));

    let (status, payload) = send(app.clone(), request("GET", "/api/backup/status", None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(payload["code"], refusal::AUTHENTICATION_REQUIRED);

    let (status, payload) = send(
        app.clone(),
        request("GET", "/api/backup/status", Some(READ_KEY)),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(payload["code"], refusal::FORBIDDEN);

    // An administrator without the one-time download token is refused too.
    let (status, payload) = send(
        app,
        request(
            "GET",
            "/api/backup/stored/fixture.weaverbak",
            Some(ADMIN_KEY),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(payload["code"], refusal::FORBIDDEN);
}

#[tokio::test]
async fn diagnostics_refuse_with_a_code() {
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("fixture-wrong-key"));
    let refused = diagnostics::admin_caller(&request_auth(ApiKeyCache::default()), None, &headers)
        .await
        .err()
        .expect("an unknown key is refused");
    let (status, payload) = refusal_of(refused).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(payload["code"], refusal::INVALID_API_KEY);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static(READ_KEY));
    let refused = diagnostics::admin_caller(
        &request_auth(api_key_cache(READ_KEY, "read")),
        None,
        &headers,
    )
    .await
    .err()
    .expect("a read key is refused");
    let (status, payload) = refusal_of(refused).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(payload["code"], refusal::FORBIDDEN);
}
