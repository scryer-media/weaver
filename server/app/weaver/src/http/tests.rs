use super::*;
use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::Extension;
use axum::http::{HeaderMap, HeaderValue, Request, header};
use axum::routing::{get, post};
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};
use tower::ServiceExt;
use weaver_server_core::Database;
use weaver_server_core::auth::{self as jwt, JWT_TTL_SECS};
use weaver_server_core::auth::{
    ApiKeyAuthRow, ApiKeyCache, CachedLoginAuth, CallerScope, LoginAuthCache, hash_api_key,
    hash_password,
};
use weaver_server_core::jobs::handle::{DownloadBlockKind, DownloadBlockState};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::operations::metrics::PipelineMetrics;
use weaver_server_core::settings::model::{Config, SharedConfig};
use weaver_server_core::{
    JobInfo, JobSpec, JobStatus, MetricsSnapshot, SchedulerCommand, SchedulerError,
    SharedPipelineState,
};

fn auth_test_router(db: Database, auth_cache: LoginAuthCache) -> Router {
    let peer_addr: SocketAddr = "127.0.0.1:49152".parse().unwrap();
    Router::new()
        .route("/api/login", post(auth::login_handler))
        .route("/api/auth/status", get(auth::auth_status_handler))
        .layer(axum::extract::connect_info::MockConnectInfo(peer_addr))
        .layer(Extension(db))
        .layer(Extension(
            weaver_server_core::security::RuntimeSecurityConfig::default(),
        ))
        .layer(Extension(auth::LoginRateLimiter::default()))
        .layer(Extension(auth_cache))
}

fn job_nzb_test_router(db: Database, handle: SchedulerHandle) -> Router {
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let session_token = SessionToken(Arc::new("session-token".to_string()));
    let request_auth = RequestAuthContext {
        db: db.clone(),
        auth_cache: auth_cache.clone(),
        api_key_cache: api_key_cache.clone(),
        session_token: session_token.clone(),
    };

    Router::new()
        .route(
            "/api/jobs/{job_id}/nzb",
            get(jobs::job_nzb_download_handler),
        )
        .route(
            "/api/jobs/{job_id}/output-file",
            post(jobs::job_output_file_download_handler),
        )
        .layer(super::compression_layer())
        .layer(Extension(handle))
        .layer(Extension(db))
        .layer(Extension(auth_cache))
        .layer(Extension(api_key_cache))
        .layer(Extension(request_auth))
        .layer(Extension(session_token))
}

fn minimal_nzb(name: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-seg1@test.com</segment></segments>
  </file>
</nzb>"#
    )
}

fn test_scheduler_handle() -> SchedulerHandle {
    let (cmd_tx, _cmd_rx) = mpsc::channel(1);
    let (event_tx, _) = broadcast::channel(1);
    let shared_state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    SchedulerHandle::new(cmd_tx, event_tx, shared_state)
}

fn test_config() -> SharedConfig {
    Arc::new(RwLock::new(Config {
        data_dir: "/tmp/weaver".to_string(),
        intermediate_dir: None,
        complete_dir: None,
        buffer_pool: None,
        tuner: None,
        servers: vec![],
        categories: vec![],
        retry: None,
        max_download_speed: None,
        cleanup_after_extract: None,
        isp_bandwidth_cap: None,
        diagnostic_upload_url: None,
        config_path: None,
    }))
}

fn nzbget_test_router(
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
    api_key_cache: ApiKeyCache,
) -> Router {
    let auth_cache = LoginAuthCache::default();
    let session_token = SessionToken(Arc::new("session-token".to_string()));
    let context = nzbget::NzbgetFacadeContext::new(
        db,
        handle,
        config,
        auth_cache,
        api_key_cache,
        session_token,
    );

    Router::new()
        .route("/jsonrpc", post(nzbget::jsonrpc_handler))
        .layer(Extension(context))
}

fn api_key_cache(raw_key: &str, scope: &str) -> ApiKeyCache {
    let cache = ApiKeyCache::default();
    cache.upsert(ApiKeyAuthRow {
        key_hash: hash_api_key(raw_key),
        id: 42,
        scope: scope.to_string(),
    });
    cache
}

fn scheduler_handle_with_mock_commands(jobs: Vec<JobInfo>) -> SchedulerHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::channel(16);
    let (event_tx, _) = broadcast::channel(16);
    let shared_state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
    let state = shared_state.clone();
    tokio::spawn(async move {
        while let Some(command) = cmd_rx.recv().await {
            match command {
                SchedulerCommand::AddJob {
                    job_id,
                    spec,
                    reply,
                    ..
                } => {
                    let mut jobs = state.list_jobs();
                    jobs.push(job_info_from_spec(job_id, spec));
                    state.publish_jobs(jobs);
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::PauseJob { job_id, reply } => {
                    let result = update_mock_job(&state, job_id, |job| {
                        job.status = JobStatus::Paused;
                        job.download_state = weaver_server_core::DownloadState::Queued;
                    });
                    let _ = reply.send(result);
                }
                SchedulerCommand::CancelJob { job_id, reply } => {
                    let mut jobs = state.list_jobs();
                    let original_len = jobs.len();
                    jobs.retain(|job| job.job_id != job_id);
                    let result = if jobs.len() == original_len {
                        Err(SchedulerError::JobNotFound(job_id))
                    } else {
                        state.publish_jobs(jobs);
                        Ok(())
                    };
                    let _ = reply.send(result);
                }
                SchedulerCommand::DeleteHistory { reply, .. }
                | SchedulerCommand::RedownloadJob { reply, .. } => {
                    let _ = reply.send(Ok(()));
                }
                _ => {}
            }
        }
    });
    SchedulerHandle::new(cmd_tx, event_tx, shared_state)
}

fn update_mock_job(
    state: &SharedPipelineState,
    job_id: JobId,
    update: impl FnOnce(&mut JobInfo),
) -> Result<(), SchedulerError> {
    let mut jobs = state.list_jobs();
    let Some(job) = jobs.iter_mut().find(|job| job.job_id == job_id) else {
        return Err(SchedulerError::JobNotFound(job_id));
    };
    update(job);
    state.publish_jobs(jobs);
    Ok(())
}

fn job_info_from_spec(job_id: JobId, spec: JobSpec) -> JobInfo {
    JobInfo {
        job_id,
        job_hash: None,
        name: spec.name,
        status: JobStatus::Queued,
        download_state: weaver_server_core::DownloadState::Queued,
        post_state: weaver_server_core::PostState::Idle,
        run_state: weaver_server_core::RunState::Active,
        progress: 0.0,
        total_bytes: spec.total_bytes,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        password: spec.password,
        category: spec.category,
        metadata: spec.metadata,
        output_dir: None,
        error: None,
        created_at_epoch_ms: 1_700_000_000_000.0,
    }
}

async fn post_nzbget(
    app: Router,
    request: serde_json::Value,
    auth_value: &str,
) -> (StatusCode, serde_json::Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jsonrpc")
                .header(header::AUTHORIZATION, auth_value)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(request.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload = serde_json::from_slice(&body).unwrap();
    (status, payload)
}

fn basic_auth(password: &str) -> String {
    use base64::Engine as _;

    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("arr:{password}"))
    )
}

fn nzbget_test_job(
    job_id: u64,
    status: JobStatus,
    download_state: weaver_server_core::DownloadState,
    total_bytes: u64,
    downloaded_bytes: u64,
    metadata: Vec<(String, String)>,
) -> JobInfo {
    JobInfo {
        job_id: JobId(job_id),
        job_hash: None,
        name: "Friends.S05.720p.BluRay.DD5.1.x264-NTb".into(),
        status,
        download_state,
        post_state: weaver_server_core::PostState::Idle,
        run_state: weaver_server_core::RunState::Active,
        progress: if total_bytes == 0 {
            0.0
        } else {
            downloaded_bytes as f64 / total_bytes as f64
        },
        total_bytes,
        downloaded_bytes,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        password: None,
        category: Some("tv".into()),
        metadata,
        output_dir: Some("/downloads/tv/Friends".into()),
        error: None,
        created_at_epoch_ms: 1_700_000_000_000.0,
    }
}

#[tokio::test]
async fn nzbget_version_uses_jsonrpc_11_envelope_and_echoes_id() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "version",
            "params": [],
            "id": "arr-version"
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["version"], "1.1");
    assert_eq!(payload["id"], "arr-version");
    assert_eq!(payload["result"], "16.0-weaver");
}

#[tokio::test]
async fn nzbget_unknown_method_returns_nzbget_error_envelope() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "loadlog",
            "params": [],
            "id": 12
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["version"], "1.1");
    assert_eq!(payload["id"], 12);
    assert_eq!(payload["error"]["name"], "JSONRPCError");
    assert_eq!(payload["error"]["code"], 1);
}

#[tokio::test]
async fn nzbget_rbac_allows_read_keys_and_rejects_read_key_mutations() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        api_key_cache("read-key", "read"),
    );

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "status",
            "params": [],
            "id": "read-ok"
        }),
        "Bearer read-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(payload.get("result").is_some());

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "append",
            "params": [],
            "id": "read-denied"
        }),
        "Bearer read-key",
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(payload["error"]["code"], 401);
}

#[tokio::test]
async fn nzbget_auth_accepts_basic_password_as_weaver_token() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "version",
            "params": [],
            "id": "basic"
        }),
        &basic_auth("session-token"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], "16.0-weaver");
}

#[tokio::test]
async fn nzbget_auth_rejects_missing_and_invalid_basic_auth() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );
    let request = serde_json::json!({
        "method": "version",
        "params": [],
        "id": "auth"
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jsonrpc")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(request.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let (status, payload) = post_nzbget(app, request, "Basic not-base64").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(payload["error"]["code"], 401);
}

#[tokio::test]
async fn nzbget_append_accepts_arr_v16_base64_payload_and_preserves_drone() {
    use base64::Engine as _;

    let db = Database::open_in_memory().unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        db,
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );
    let nzb_b64 = base64::engine::general_purpose::STANDARD
        .encode(minimal_nzb("Friends.S05.720p.BluRay.DD5.1.x264-NTb"));

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "append",
            "params": [
                "Friends.S05.720p.BluRay.DD5.1.x264-NTb.nzb",
                nzb_b64,
                "tv",
                50,
                false,
                false,
                "",
                0,
                "all",
                ["drone", "sonarrdroneid"]
            ],
            "id": "append"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(payload["result"].as_u64().unwrap() >= 10_000);

    let jobs = handle.list_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].category.as_deref(), Some("tv"));
    assert!(jobs[0].metadata.iter().any(|(key, value)| {
        key == weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY && value == "sonarrdroneid"
    }));
    assert!(
        jobs[0].metadata.iter().any(|(key, value)| key
            == weaver_server_api::PRIORITY_ATTRIBUTE_KEY
            && value == "HIGH")
    );
}

#[tokio::test]
async fn nzbget_append_rejects_private_url_payloads_for_prowlarr_shape() {
    let db = Database::open_in_memory().unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        db,
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "append",
            "params": [
                "",
                "http://127.0.0.1:9/download.nzb",
                "Prowlarr",
                0,
                false,
                false,
                "",
                0,
                "all",
                ["drone", "prowlarrdroneid"]
            ],
            "id": "append-url"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["error"]["code"], 2);
    assert!(
        payload["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not allowed")
    );
    assert!(handle.list_jobs().is_empty());
}

#[tokio::test]
async fn nzbget_status_and_listgroups_support_sonarr_progress_queries() {
    let job = nzbget_test_job(
        42,
        JobStatus::Downloading,
        weaver_server_core::DownloadState::Downloading,
        6_000_000_000,
        1_500_000_000,
        vec![
            (
                weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                "drone-progress".to_string(),
            ),
            (
                weaver_server_api::PRIORITY_ATTRIBUTE_KEY.to_string(),
                "HIGH".to_string(),
            ),
            ("drone".to_string(), "spoofed-drone".to_string()),
        ],
    );
    let metrics = PipelineMetrics::new();
    let (cmd_tx, _cmd_rx) = mpsc::channel(1);
    let (event_tx, _) = broadcast::channel(1);
    let shared_state = SharedPipelineState::new(metrics.clone(), vec![job]);
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    metrics
        .bytes_downloaded
        .store(1_048_576, std::sync::atomic::Ordering::Relaxed);
    shared_state.refresh_metrics_snapshot();
    let handle = SchedulerHandle::new(cmd_tx, event_tx, shared_state);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle,
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, status_payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "status",
            "params": [],
            "id": "status"
        }),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(status_payload["result"]["RemainingSizeHi"], 1);
    assert_eq!(status_payload["result"]["DownloadPaused"], false);
    assert!(
        status_payload["result"]["DownloadRate"].as_u64().unwrap() > 0,
        "status should use the speed-bearing metrics snapshot"
    );

    let (status, groups_payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "listgroups",
            "params": [],
            "id": "listgroups"
        }),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let group = &groups_payload["result"][0];
    assert_eq!(group["NZBID"], 42);
    assert_eq!(group["NZBName"], "Friends.S05.720p.BluRay.DD5.1.x264-NTb");
    assert_eq!(group["FileSizeHi"], 1);
    assert_eq!(group["RemainingSizeHi"], 1);
    assert_eq!(group["PausedSizeLo"], 0);
    assert_eq!(group["ActiveDownloads"], 1);
    assert_eq!(group["Status"], "DOWNLOADING");
    let parameters = group["Parameters"].as_array().unwrap();
    let drone_parameters = parameters
        .iter()
        .filter(|parameter| parameter["Name"] == "drone")
        .collect::<Vec<_>>();
    assert_eq!(drone_parameters.len(), 1);
    assert_eq!(drone_parameters[0]["Value"], "drone-progress");
}

#[tokio::test]
async fn nzbget_history_returns_arr_status_fields_and_drone_parameter() {
    let db = Database::open_in_memory().unwrap();
    let metadata = serde_json::to_string(&vec![(
        weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
        "drone-history".to_string(),
    )])
    .unwrap();
    db.insert_job_history(&weaver_server_core::JobHistoryRow {
        job_id: 100,
        job_hash: None,
        name: "Complete.Release".into(),
        status: "complete".into(),
        error_message: None,
        total_bytes: 123,
        downloaded_bytes: 123,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: Some("tv".into()),
        output_dir: Some("/downloads/tv/Complete.Release".into()),
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_100,
        metadata: Some(metadata),
        last_diagnostic_id: None,
        last_diagnostic_uploaded_at_epoch_ms: None,
    })
    .unwrap();
    db.insert_job_history(&weaver_server_core::JobHistoryRow {
        job_id: 101,
        job_hash: None,
        name: "Failed.Release".into(),
        status: "failed".into(),
        error_message: Some("article failures".into()),
        total_bytes: 456,
        downloaded_bytes: 100,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 356,
        health: 100,
        category: Some("tv".into()),
        output_dir: Some("/downloads/tv/Failed.Release".into()),
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_200,
        metadata: None,
        last_diagnostic_id: None,
        last_diagnostic_uploaded_at_epoch_ms: None,
    })
    .unwrap();
    let app = nzbget_test_router(
        db,
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "history",
            "params": [],
            "id": "history"
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let items = payload["result"].as_array().unwrap();
    let complete = items.iter().find(|item| item["ID"] == 100).unwrap();
    let failed = items.iter().find(|item| item["ID"] == 101).unwrap();
    assert_eq!(complete["ParStatus"], "SUCCESS");
    assert_eq!(complete["UnpackStatus"], "SUCCESS");
    assert_eq!(complete["Parameters"][0]["Name"], "drone");
    assert_eq!(complete["Parameters"][0]["Value"], "drone-history");
    assert_eq!(failed["ParStatus"], "FAILURE");
    assert_eq!(failed["Message"], "article failures");
}

#[tokio::test]
async fn nzbget_config_exposes_real_categories_and_keep_history() {
    let config = test_config();
    {
        let mut config_write = config.write().await;
        config_write
            .categories
            .push(weaver_server_core::categories::CategoryConfig {
                id: 1,
                name: "tv".into(),
                dest_dir: Some("/media/tv".into()),
                aliases: "series,shows".into(),
            });
    }
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        config,
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "config",
            "params": [],
            "id": "config"
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let entries = payload["result"].as_array().unwrap();
    let value_for = |name: &str| {
        entries
            .iter()
            .find(|entry| entry["Name"] == name)
            .and_then(|entry| entry["Value"].as_str())
            .unwrap()
    };
    assert_eq!(value_for("KeepHistory"), "7");
    assert_eq!(value_for("Category1.Name"), "tv");
    assert_eq!(value_for("Category1.DestDir"), "/media/tv");
    assert_eq!(value_for("Category1.Aliases"), "series,shows");
}

#[tokio::test]
async fn nzbget_editqueue_maps_arr_actions() {
    let job = nzbget_test_job(
        77,
        JobStatus::Queued,
        weaver_server_core::DownloadState::Queued,
        100,
        0,
        vec![],
    );
    let handle = scheduler_handle_with_mock_commands(vec![job]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupFinalDelete", 0, "", 77],
            "id": "delete"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    assert!(handle.list_jobs().is_empty());

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["HistoryDelete", 0, "", 77],
            "id": "history-delete"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["HistoryRedownload", 0, "", 77],
            "id": "history-redownload"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "editqueue",
            "params": ["UnsupportedAction", 0, "", 77],
            "id": "unsupported"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["error"]["code"], 3);
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
async fn resolve_scope_accepts_session_cookie_without_login() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_static("weaver_session=session-token"),
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
    let auth = CachedLoginAuth::new("admin", password_hash, jwt::generate_jwt_secret());
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
async fn login_handler_rejects_legacy_scrypt_hash() {
    let db = Database::open_in_memory().unwrap();
    let legacy_hash =
        "$scrypt$ln=16,r=8,p=1$MDAwMDAwMDAwMDAwMDAwMA$MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA"
            .to_string();
    db.set_auth_credentials("admin", &legacy_hash).unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
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
    assert_eq!(stored.password_hash, legacy_hash);
    assert_eq!(auth_cache.snapshot().unwrap().password_hash, legacy_hash);
}

#[tokio::test]
async fn login_handler_wrong_password_keeps_argon2_hash_and_cache() {
    let db = Database::open_in_memory().unwrap();
    let argon2_hash = hash_password("hunter2").unwrap();
    db.set_auth_credentials("admin", &argon2_hash).unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
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
    assert_eq!(stored.password_hash, argon2_hash);
    assert_eq!(auth_cache.snapshot().unwrap(), original);
}

#[tokio::test]
async fn login_handler_wrong_username_with_valid_password_is_unauthorized() {
    let db = Database::open_in_memory().unwrap();
    let argon2_hash = hash_password("hunter2").unwrap();
    db.set_auth_credentials("admin", &argon2_hash).unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
    let original = auth_cache.snapshot().unwrap();
    let app = auth_test_router(db.clone(), auth_cache.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"not-admin","password":"hunter2"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let stored = db.get_auth_credentials().unwrap().unwrap();
    assert_eq!(stored.password_hash, argon2_hash);
    assert_eq!(auth_cache.snapshot().unwrap(), original);
}

#[tokio::test]
async fn login_handler_rate_limits_repeated_failures() {
    let db = Database::open_in_memory().unwrap();
    let argon2_hash = hash_password("hunter2").unwrap();
    db.set_auth_credentials("admin", &argon2_hash).unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
    let app = auth_test_router(db, auth_cache);

    for _ in 0..auth::LOGIN_MAX_FAILURES {
        let response = app
            .clone()
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
    }

    let throttled_wrong = app
        .clone()
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
    assert_eq!(throttled_wrong.status(), StatusCode::TOO_MANY_REQUESTS);

    let throttled_correct = app
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
    assert_eq!(throttled_correct.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn login_handler_malformed_hash_fails_cleanly() {
    let db = Database::open_in_memory().unwrap();
    db.set_auth_credentials("admin", "not-a-phc-hash").unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
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
    let auth = CachedLoginAuth::new("admin", password_hash, jwt::generate_jwt_secret());
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

#[tokio::test]
async fn job_nzb_download_handler_returns_uncompressed_history_nzb() {
    let db = Database::open_in_memory().unwrap();
    let handle = test_scheduler_handle();
    let xml = minimal_nzb("Friends.S05.720p.BluRay.DD5.1.x264-NTb");
    let nzb_zstd = weaver_server_core::ingest::compress_nzb_bytes(xml.as_bytes()).unwrap();
    db.create_active_job(&weaver_server_core::ActiveJob {
        job_id: JobId(10_000),
        nzb_hash: weaver_server_core::ingest::hash_persisted_nzb_bytes(&nzb_zstd),
        nzb_path: std::path::PathBuf::from("Friends.S05.720p.BluRay.DD5.1.x264-NTb.nzb"),
        nzb_zstd,
        output_dir: std::path::PathBuf::from("/tmp/weaver-http-test"),
        created_at: 1_700_000_000,
        category: Some("tv".to_string()),
        metadata: vec![],
    })
    .unwrap();
    db.archive_job(
        JobId(10_000),
        &weaver_server_core::JobHistoryRow {
            job_id: 10_000,
            job_hash: None,
            name: "Friends".to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 123,
            downloaded_bytes: 123,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: Some("tv".to_string()),
            output_dir: None,
            nzb_path: Some("Friends.S05.720p.BluRay.DD5.1.x264-NTb.nzb".to_string()),
            created_at: 1_700_000_000,
            completed_at: 1_700_000_100,
            metadata: Some(
                serde_json::to_string(&vec![(
                    weaver_server_core::ingest::ORIGINAL_TITLE_METADATA_KEY.to_string(),
                    "Friends.S05.720p.BluRay.DD5.1.x264-NTb".to_string(),
                )])
                .unwrap(),
            ),
            last_diagnostic_id: None,
            last_diagnostic_uploaded_at_epoch_ms: None,
        },
    )
    .unwrap();
    let app = job_nzb_test_router(db, handle);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/jobs/10000/nzb")
                .header(header::AUTHORIZATION, "Bearer session-token")
                .header(header::ACCEPT_ENCODING, "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-nzb")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok()),
        Some("attachment; filename=\"Friends.S05.720p.BluRay.DD5.1.x264-NTb.nzb\"")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(xml.len().to_string().as_str())
    );
    assert!(response.headers().get(header::CONTENT_ENCODING).is_none());

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body, Bytes::from(xml));
}

#[tokio::test]
async fn job_output_file_download_handler_streams_history_file() {
    let db = Database::open_in_memory().unwrap();
    let handle = test_scheduler_handle();
    let temp_dir = tempfile::tempdir().unwrap();
    let output_dir = temp_dir.path().join("job-output");
    std::fs::create_dir_all(&output_dir).unwrap();
    let file_path = output_dir.join("episode-01.mkv");
    std::fs::write(&file_path, b"video-bytes").unwrap();
    db.insert_job_history(&weaver_server_core::JobHistoryRow {
        job_id: 10_001,
        job_hash: None,
        name: "Friends".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 123,
        downloaded_bytes: 123,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: Some("tv".to_string()),
        output_dir: Some(output_dir.display().to_string()),
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_100,
        metadata: None,
        last_diagnostic_id: None,
        last_diagnostic_uploaded_at_epoch_ms: None,
    })
    .unwrap();
    let app = job_nzb_test_router(db, handle);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/jobs/10001/output-file")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::ACCEPT_ENCODING, "gzip")
                .body(Body::from(format!(
                    "path={}&token=session-token",
                    file_path.display()
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok()),
        Some("attachment; filename=\"episode-01.mkv\"")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("11")
    );
    assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body, Bytes::from_static(b"video-bytes"));
}

#[test]
fn renders_prometheus_metrics_for_pipeline_and_jobs() {
    let snapshot = MetricsSnapshot {
        bytes_downloaded: 10,
        bytes_decoded: 8,
        bytes_committed: 7,
        download_queue_depth: 5,
        active_downloads: 6,
        active_decodes: 2,
        decode_pending: 4,
        decode_pending_bytes: 4096,
        decode_active_bytes: 2048,
        commit_pending: 3,
        write_buffered_bytes: 2,
        write_buffered_segments: 1,
        direct_write_evictions: 9,
        decode_pressure_soft_limit_bytes: 100,
        decode_pressure_hard_limit_bytes: 200,
        write_pressure_soft_limit_bytes: 300,
        write_pressure_hard_limit_bytes: 400,
        download_pressure_state: weaver_server_core::DownloadPressureState::Soft,
        download_pressure_reason: weaver_server_core::DownloadPressureReason::Decode,
        download_pressure_stalls_total: 24,
        download_pressure_stall_duration_ms: 1500,
        download_pressure_current_stall_ms: 250,
        hot_dispatch_job_id: 42,
        hot_dispatch_mode: weaver_server_core::DispatchShareMode::Shared,
        hot_dispatch_underfill_ms: 2500,
        hot_dispatch_lent_connections: 2,
        hot_dispatch_warmup_complete: true,
        hot_dispatch_last_spillover_decision:
            weaver_server_core::SpilloverDecision::AllowedUnderfill,
        hot_dispatch_spillover_blocked_warmup_total: 29,
        hot_dispatch_spillover_blocked_pressure_total: 30,
        hot_dispatch_spillover_blocked_near_cap_total: 31,
        hot_dispatch_spillover_blocked_hot_can_use_capacity_total: 32,
        hot_dispatch_spillover_allowed_underfill_total: 33,
        hot_dispatch_spillover_reclaimed_total: 34,
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
        download_failures_article_not_found: 24,
        download_failures_capacity_unavailable: 25,
        download_failures_transient: 26,
        download_failures_auth: 27,
        download_failures_permanent: 28,
        current_download_speed: 19,
        crc_errors: 20,
        recovery_queue_depth: 21,
        articles_per_sec: 22.5,
        decode_rate_mbps: 23.5,
    };
    let jobs = vec![JobInfo {
        job_id: JobId(42),
        job_hash: None,
        name: "Silver Horizon".into(),
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
    assert!(rendered.contains("weaver_pipeline_active_downloads 6"));
    assert!(rendered.contains("weaver_pipeline_decode_pending_bytes 4096"));
    assert!(rendered.contains("weaver_pipeline_download_pressure_state{state=\"soft\"} 1"));
    assert!(rendered.contains("weaver_pipeline_download_pressure_reason{reason=\"decode\"} 1"));
    assert!(rendered.contains("weaver_pipeline_download_observed_limiter{limiter=\"gated\"} 1"));
    assert!(rendered.contains("weaver_pipeline_download_pressure_stalls_total 24"));
    assert!(rendered.contains("weaver_pipeline_download_pressure_stall_duration_seconds 1.5"));
    assert!(rendered.contains("weaver_pipeline_hot_dispatch_job_id 42"));
    assert!(rendered.contains("weaver_pipeline_hot_dispatch_mode{mode=\"shared\"} 1"));
    assert!(rendered.contains("weaver_pipeline_hot_dispatch_underfill_milliseconds 2500"));
    assert!(rendered.contains("weaver_pipeline_hot_dispatch_lent_connections 2"));
    assert!(rendered.contains("weaver_pipeline_hot_dispatch_warmup_complete 1"));
    assert!(rendered.contains(
        "weaver_pipeline_hot_dispatch_last_spillover_decision{decision=\"allowed_underfill\"} 1"
    ));
    assert!(rendered.contains(
        "weaver_pipeline_hot_dispatch_spillover_decisions_total{decision=\"allowed_underfill\"} 33"
    ));
    assert!(
        rendered.contains("weaver_pipeline_download_failures_total{kind=\"article_not_found\"} 24")
    );
    assert!(
        rendered
            .contains("weaver_pipeline_download_failures_total{kind=\"capacity_unavailable\"} 25")
    );
    assert!(rendered.contains("weaver_pipeline_download_failures_total{kind=\"transient\"} 26"));
    assert!(rendered.contains("weaver_pipeline_download_failures_total{kind=\"auth\"} 27"));
    assert!(rendered.contains("weaver_pipeline_download_failures_total{kind=\"permanent\"} 28"));
    assert!(rendered.contains(
            "weaver_job_info{job_id=\"42\",job_name=\"Silver Horizon\",status=\"downloading\",category=\"tv\",has_password=\"true\"} 1"
        ));
    assert!(rendered.contains("weaver_job_progress_ratio{job_id=\"42\""));
    assert!(rendered.contains("weaver_pipeline_jobs{status=\"downloading\"} 1"));
}

#[test]
fn renders_prometheus_download_observed_limiter_states() {
    let mut snapshot = MetricsSnapshot {
        bytes_downloaded: 0,
        bytes_decoded: 0,
        bytes_committed: 0,
        download_queue_depth: 10,
        active_downloads: 20,
        active_decodes: 0,
        decode_pending: 0,
        decode_pending_bytes: 0,
        decode_active_bytes: 0,
        commit_pending: 0,
        write_buffered_bytes: 0,
        write_buffered_segments: 0,
        direct_write_evictions: 0,
        decode_pressure_soft_limit_bytes: 100,
        decode_pressure_hard_limit_bytes: 200,
        write_pressure_soft_limit_bytes: 100,
        write_pressure_hard_limit_bytes: 200,
        download_pressure_state: weaver_server_core::DownloadPressureState::Clear,
        download_pressure_reason: weaver_server_core::DownloadPressureReason::None,
        download_pressure_stalls_total: 0,
        download_pressure_stall_duration_ms: 0,
        download_pressure_current_stall_ms: 0,
        hot_dispatch_job_id: 0,
        hot_dispatch_mode: weaver_server_core::DispatchShareMode::Exclusive,
        hot_dispatch_underfill_ms: 0,
        hot_dispatch_lent_connections: 0,
        hot_dispatch_warmup_complete: false,
        hot_dispatch_last_spillover_decision: weaver_server_core::SpilloverDecision::None,
        hot_dispatch_spillover_blocked_warmup_total: 0,
        hot_dispatch_spillover_blocked_pressure_total: 0,
        hot_dispatch_spillover_blocked_near_cap_total: 0,
        hot_dispatch_spillover_blocked_hot_can_use_capacity_total: 0,
        hot_dispatch_spillover_allowed_underfill_total: 0,
        hot_dispatch_spillover_reclaimed_total: 0,
        segments_downloaded: 0,
        segments_decoded: 0,
        segments_committed: 0,
        articles_not_found: 0,
        decode_errors: 0,
        verify_active: 0,
        repair_active: 0,
        extract_active: 0,
        disk_write_latency_us: 0,
        segments_retried: 0,
        segments_failed_permanent: 0,
        download_failures_article_not_found: 0,
        download_failures_capacity_unavailable: 0,
        download_failures_transient: 0,
        download_failures_auth: 0,
        download_failures_permanent: 0,
        current_download_speed: 0,
        crc_errors: 0,
        recovery_queue_depth: 0,
        articles_per_sec: 0.0,
        decode_rate_mbps: 0.0,
    };
    let unblocked = DownloadBlockState {
        kind: DownloadBlockKind::None,
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
    };
    let server_health = vec![metrics::ServerHealthInfo {
        label: "news.example:563".into(),
        state: "healthy",
        success_count: 0,
        failure_count: 0,
        consecutive_failures: 0,
        latency_ms: 0.0,
        connections_available: 0,
        connections_max: 20,
        premature_deaths: 0,
    }];

    let rendered =
        metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &server_health);
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"network_limited\"} 1")
    );

    snapshot.decode_pending_bytes = 128 * 1024 * 1024;
    snapshot.current_download_speed = 30 * 1024 * 1024;
    snapshot.decode_rate_mbps = 5.0;
    let rendered =
        metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &server_health);
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"decode_lagging\"} 1")
    );
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"network_limited\"} 0")
    );

    snapshot.decode_pending_bytes = 64 * 1024 * 1024;
    snapshot.decode_active_bytes = 8 * 1024 * 1024;
    snapshot.current_download_speed = 4 * 1024 * 1024;
    snapshot.decode_rate_mbps = 5.0;
    let rendered =
        metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &server_health);
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"decode_lagging\"} 1")
    );

    snapshot.decode_pending_bytes = 0;
    snapshot.decode_active_bytes = 0;
    snapshot.current_download_speed = 0;
    snapshot.decode_rate_mbps = 0.0;
    snapshot.download_pressure_state = weaver_server_core::DownloadPressureState::Soft;
    snapshot.download_pressure_reason = weaver_server_core::DownloadPressureReason::Write;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[]);
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"pressure_limited\"} 1")
    );

    snapshot.download_pressure_state = weaver_server_core::DownloadPressureState::Clear;
    snapshot.download_pressure_reason = weaver_server_core::DownloadPressureReason::None;
    snapshot.download_queue_depth = 0;
    snapshot.active_downloads = 0;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[]);
    assert!(rendered.contains("weaver_pipeline_download_observed_limiter{limiter=\"idle\"} 1"));

    snapshot.download_queue_depth = 242;
    snapshot.recovery_queue_depth = 242;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[]);
    assert!(rendered.contains("weaver_pipeline_download_observed_limiter{limiter=\"idle\"} 1"));
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"dispatch_limited\"} 0")
    );

    snapshot.download_queue_depth = 0;
    snapshot.recovery_queue_depth = 0;
    snapshot.download_pressure_state = weaver_server_core::DownloadPressureState::Soft;
    snapshot.download_pressure_reason = weaver_server_core::DownloadPressureReason::Write;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[]);
    assert!(rendered.contains("weaver_pipeline_download_observed_limiter{limiter=\"idle\"} 1"));
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
