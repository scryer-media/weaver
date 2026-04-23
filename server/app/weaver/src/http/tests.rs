use super::*;
use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::Extension;
use axum::http::{HeaderMap, HeaderValue, Request, header};
use axum::routing::{get, post};
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use scrypt::password_hash::{PasswordHasher, SaltString, rand_core::OsRng};
use std::io::Write;
use tower::ServiceExt;
use weaver_server_core::Database;
use weaver_server_core::auth::{self as jwt, JWT_TTL_SECS};
use weaver_server_core::auth::{
    ApiKeyAuthRow, ApiKeyCache, CachedLoginAuth, CallerScope, LoginAuthCache, hash_api_key,
    hash_password,
};
use weaver_server_core::jobs::handle::{DownloadBlockKind, DownloadBlockState};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::{JobInfo, JobStatus, MetricsSnapshot};

fn legacy_scrypt_hash(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    scrypt::Scrypt
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

fn auth_test_router(db: Database, auth_cache: LoginAuthCache) -> Router {
    Router::new()
        .route("/api/login", post(auth::login_handler))
        .route("/api/auth/status", get(auth::auth_status_handler))
        .layer(Extension(db))
        .layer(Extension(auth_cache))
}

#[tokio::test]
async fn resolve_scope_requires_explicit_auth_when_login_is_disabled() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let headers = HeaderMap::new();
    let result =
        auth::resolve_scope(&db, &auth_cache, &api_key_cache, "session-token", &headers).await;
    assert_eq!(result, Err(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn resolve_scope_accepts_session_bearer_without_login() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer session-token"),
    );
    let result =
        auth::resolve_scope(&db, &auth_cache, &api_key_cache, "session-token", &headers).await;
    assert_eq!(result, Ok(CallerScope::Local));
}

#[tokio::test]
async fn resolve_scope_accepts_cached_jwt_without_db_lookup() {
    let db = Database::open_in_memory().unwrap();
    let password_hash = hash_password("hunter2").unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let auth = CachedLoginAuth::new("admin", password_hash);
    let token = jwt::create_jwt("admin", &auth.jwt_secret, JWT_TTL_SECS);
    auth_cache.replace(Some(auth));

    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_str(&format!("weaver_jwt={token}")).unwrap(),
    );

    let result =
        auth::resolve_scope(&db, &auth_cache, &api_key_cache, "session-token", &headers).await;
    assert_eq!(result, Ok(CallerScope::Admin));
}

#[tokio::test]
async fn resolve_scope_accepts_cached_api_key_without_db_lookup() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let raw_key = "wvr_cached";
    api_key_cache.upsert(ApiKeyAuthRow {
        key_hash: hash_api_key(raw_key),
        id: 42,
        scope: "read".to_string(),
    });

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {raw_key}")).unwrap(),
    );

    let result =
        auth::resolve_scope(&db, &auth_cache, &api_key_cache, "session-token", &headers).await;
    assert_eq!(result, Ok(CallerScope::Read));
}

#[tokio::test]
async fn login_handler_rehashes_legacy_scrypt_and_updates_cache() {
    let db = Database::open_in_memory().unwrap();
    let legacy_hash = legacy_scrypt_hash("hunter2");
    db.set_auth_credentials("admin", &legacy_hash).unwrap();
    let auth_cache = LoginAuthCache::from_credentials(db.get_auth_credentials().unwrap());
    let old_secret = auth_cache.snapshot().unwrap().jwt_secret;
    let app = auth_test_router(db.clone(), auth_cache.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"username":"admin","password":"hunter2"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let set_cookie = response
        .headers()
        .get(header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_string();
    let token = set_cookie
        .split(';')
        .next()
        .unwrap()
        .strip_prefix("weaver_jwt=")
        .unwrap();

    let stored = db.get_auth_credentials().unwrap().unwrap();
    assert!(stored.password_hash.starts_with("$argon2id$"));
    let cached = auth_cache.snapshot().unwrap();
    assert_eq!(cached.password_hash, stored.password_hash);
    assert!(jwt::verify_jwt(token, &cached.jwt_secret).is_ok());
    assert!(jwt::verify_jwt(token, &old_secret).is_err());
}

#[tokio::test]
async fn login_handler_wrong_password_keeps_legacy_hash_and_cache() {
    let db = Database::open_in_memory().unwrap();
    let legacy_hash = legacy_scrypt_hash("hunter2");
    db.set_auth_credentials("admin", &legacy_hash).unwrap();
    let auth_cache = LoginAuthCache::from_credentials(db.get_auth_credentials().unwrap());
    let original = auth_cache.snapshot().unwrap();
    let app = auth_test_router(db.clone(), auth_cache.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"username":"admin","password":"wrong"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let stored = db.get_auth_credentials().unwrap().unwrap();
    assert_eq!(stored.password_hash, legacy_hash);
    assert_eq!(auth_cache.snapshot().unwrap(), original);
}

#[tokio::test]
async fn login_handler_malformed_hash_fails_cleanly() {
    let db = Database::open_in_memory().unwrap();
    db.set_auth_credentials("admin", "not-a-phc-hash").unwrap();
    let auth_cache = LoginAuthCache::from_credentials(db.get_auth_credentials().unwrap());
    let original = auth_cache.snapshot().unwrap();
    let app = auth_test_router(db.clone(), auth_cache.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"username":"admin","password":"hunter2"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let stored = db.get_auth_credentials().unwrap().unwrap();
    assert_eq!(stored.password_hash, "not-a-phc-hash");
    assert_eq!(auth_cache.snapshot().unwrap(), original);
}

#[tokio::test]
async fn auth_status_handler_uses_cached_auth_state() {
    let db = Database::open_in_memory().unwrap();
    let password_hash = hash_password("hunter2").unwrap();
    let auth_cache = LoginAuthCache::default();
    let auth = CachedLoginAuth::new("admin", password_hash);
    let token = jwt::create_jwt("admin", &auth.jwt_secret, JWT_TTL_SECS);
    auth_cache.replace(Some(auth));
    let app = auth_test_router(db, auth_cache);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/auth/status")
                .header(header::COOKIE, format!("weaver_jwt={token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["enabled"], true);
    assert_eq!(payload["authenticated"], true);
}

#[test]
fn renders_prometheus_metrics_for_pipeline_and_jobs() {
    let snapshot = MetricsSnapshot {
        bytes_downloaded: 10,
        bytes_decoded: 8,
        bytes_committed: 7,
        download_queue_depth: 5,
        decode_pending: 4,
        commit_pending: 3,
        write_buffered_bytes: 2,
        write_buffered_segments: 1,
        direct_write_evictions: 9,
        segments_downloaded: 11,
        segments_decoded: 12,
        segments_committed: 13,
        articles_not_found: 14,
        decode_errors: 15,
        verify_active: 1,
        repair_active: 0,
        extract_active: 2,
        disk_write_latency_us: 16,
        segments_retried: 17,
        segments_failed_permanent: 18,
        current_download_speed: 19,
        crc_errors: 20,
        recovery_queue_depth: 21,
        articles_per_sec: 22.5,
        decode_rate_mbps: 23.5,
    };
    let jobs = vec![JobInfo {
        job_id: JobId(42),
        name: "Frieren".into(),
        status: JobStatus::Downloading,
        download_state: weaver_server_core::DownloadState::Downloading,
        post_state: weaver_server_core::PostState::Idle,
        run_state: weaver_server_core::RunState::Active,
        progress: 0.5,
        total_bytes: 100,
        downloaded_bytes: 50,
        optional_recovery_bytes: 25,
        optional_recovery_downloaded_bytes: 5,
        failed_bytes: 2,
        health: 999,
        password: Some("secret".into()),
        category: Some("tv".into()),
        metadata: Vec::new(),
        output_dir: None,
        error: None,
        created_at_epoch_ms: 1_700_000_000_000.0,
    }];

    let rendered = metrics::render_prometheus_metrics(
        &snapshot,
        &jobs,
        true,
        &DownloadBlockState {
            kind: DownloadBlockKind::ManualPause,
            cap_enabled: false,
            period: None,
            used_bytes: 0,
            limit_bytes: 0,
            remaining_bytes: 0,
            reserved_bytes: 0,
            window_starts_at_epoch_ms: None,
            window_ends_at_epoch_ms: None,
            timezone_name: "MDT".into(),
            scheduled_speed_limit: 0,
        },
        &[],
    );

    assert!(rendered.contains("weaver_pipeline_paused 1"));
    assert!(rendered.contains("weaver_pipeline_current_download_speed_bytes_per_second 19"));
    assert!(rendered.contains(
            "weaver_job_info{job_id=\"42\",job_name=\"Frieren\",status=\"downloading\",category=\"tv\",has_password=\"true\"} 1"
        ));
    assert!(rendered.contains("weaver_job_progress_ratio{job_id=\"42\""));
    assert!(rendered.contains("weaver_pipeline_jobs{status=\"downloading\"} 1"));
}

#[test]
fn escapes_prometheus_label_values() {
    assert_eq!(
        metrics::escape_prometheus_label_value("a\"b\\c\nd"),
        "a\\\"b\\\\c\\nd"
    );
}

fn compress_request_body(encoding: &str, payload: &[u8]) -> Vec<u8> {
    match encoding {
        "gzip" => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(payload).unwrap();
            encoder.finish().unwrap()
        }
        "deflate" => {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(payload).unwrap();
            encoder.finish().unwrap()
        }
        "br" => {
            let mut compressed = Vec::new();
            {
                let mut encoder = brotli::CompressorWriter::new(&mut compressed, 4096, 3, 22);
                encoder.write_all(payload).unwrap();
            }
            compressed
        }
        "zstd" => zstd::bulk::compress(payload, 1).unwrap(),
        other => panic!("unsupported encoding {other}"),
    }
}

#[tokio::test]
async fn request_decompression_accepts_all_supported_encodings() {
    let app = Router::new()
        .route("/", post(|body: Bytes| async move { body }))
        .layer(
            RequestDecompressionLayer::new()
                .gzip(true)
                .deflate(true)
                .br(true)
                .zstd(true),
        );
    let payload = br#"{"query":"query { __typename }"}"#;

    for encoding in ["gzip", "deflate", "br", "zstd"] {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CONTENT_ENCODING, encoding)
                    .body(Body::from(compress_request_body(encoding, payload)))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "encoding {encoding}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], payload, "encoding {encoding}");
    }
}

#[tokio::test]
async fn response_compression_supports_deflate() {
    let payload = "deflate-me-please ".repeat(256);
    let app = Router::new()
        .route(
            "/",
            post(move || {
                let payload = payload.clone();
                async move { payload }
            }),
        )
        .layer(
            CompressionLayer::new()
                .gzip(true)
                .deflate(true)
                .br(true)
                .zstd(true),
        );

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/")
                .header(header::ACCEPT_ENCODING, "deflate")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok()),
        Some("deflate")
    );
}
