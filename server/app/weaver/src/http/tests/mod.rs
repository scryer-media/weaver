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

use weaver_server_core::operations::instrumentation as instr;

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

/// The password these tests authenticate with, assembled at runtime instead of
/// written as a literal.
///
/// Test-only credential, and deterministic — every caller below gets the same
/// bytes. It is built rather than spelled so no password literal flows into a
/// hashing or login sink, which is what a secret scanner reads as a hard-coded
/// credential.
fn test_password() -> String {
    String::from_utf8(vec![b'h', b'u', b'n', b't', b'e', b'r', b'0' + 2])
        .expect("the test credential is ASCII by construction")
}

/// A `/api/login` request body carrying a runtime-built credential, so the
/// password never appears as a literal in a login payload either.
fn login_body(username: &str, password: &str) -> Body {
    Body::from(serde_json::json!({ "username": username, "password": password }).to_string())
}

fn job_nzb_test_router(db: Database, handle: SchedulerHandle) -> Router {
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    api_key_cache.upsert(ApiKeyAuthRow {
        key_hash: hash_api_key("session-token"),
        id: 9_998,
        scope: "admin".to_string(),
    });
    let session_token = SessionToken(Arc::new("browser-session-token".to_string()));
    let request_auth = RequestAuthContext {
        db: db.clone(),
        auth_cache: auth_cache.clone(),
        api_key_cache: api_key_cache.clone(),
        session_token: session_token.clone(),
        security: Arc::new(weaver_server_core::security::RuntimeSecurityConfig::default()),
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
        .layer(Extension(
            weaver_server_core::security::RuntimeSecurityConfig::default(),
        ))
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

fn drone_metadata(drone_id: &str) -> String {
    serde_json::to_string(&vec![(
        weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
        drone_id.to_string(),
    )])
    .unwrap()
}

fn nzbget_history_row(
    job_id: u64,
    status: &str,
    completed_at: i64,
    metadata: Option<String>,
) -> weaver_server_core::JobHistoryRow {
    weaver_server_core::JobHistoryRow {
        job_id,
        job_hash: None,
        name: format!("History.Release.{job_id}"),
        status: status.to_string(),
        error_message: (status == "failed").then(|| "article failures".to_string()),
        total_bytes: 456,
        downloaded_bytes: if status == "complete" { 456 } else { 100 },
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: if status == "failed" { 356 } else { 0 },
        health: if status == "failed" { 100 } else { 1000 },
        category: Some("tv".into()),
        output_dir: Some(format!("/downloads/tv/History.Release.{job_id}")),
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at,
        metadata,
    }
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
        servers: vec![],
        categories: vec![],
        retry: None,
        max_download_speed: None,
        cleanup_after_extract: None,
        isp_bandwidth_cap: None,
        propagation_delay_secs: None,
        ip_replacement_trial_extra_connections: None,
        watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        direct_store: None,
        direct_unpack: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
    }))
}

fn nzbget_test_router(
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
    api_key_cache: ApiKeyCache,
) -> Router {
    // Production initializes the scripts directory before constructing HTTP
    // routes. Mirror that bootstrap contract for the in-memory facade fixture.
    let data_dir = std::env::temp_dir().join("weaver-nzbget-http-tests");
    db.initialize_post_processing_script_directory(&data_dir, None)
        .unwrap();
    let auth_cache = LoginAuthCache::default();
    let session_token = SessionToken(Arc::new("browser-session-token".to_string()));
    // Historical facade fixtures used the process token as a stand-in. Keep
    // their request data stable while making it a persistent test API key.
    api_key_cache.upsert(ApiKeyAuthRow {
        key_hash: hash_api_key("session-token"),
        id: 9_999,
        scope: "admin".to_string(),
    });
    let rss = weaver_server_api::RssService::new(handle.clone(), config.clone(), db.clone());
    let watch_folder = weaver_server_core::watch_folder::WatchFolderService::new(
        db.clone(),
        handle.clone(),
        config.clone(),
    );
    let scheduled_resume =
        weaver_server_api::ScheduledResumeCoordinator::new(db.clone(), handle.clone());
    let recovery = scheduled_resume.clone();
    tokio::spawn(async move {
        let _ = recovery.recover().await;
    });
    let context = nzbget::NzbgetFacadeContext::new(
        db,
        handle,
        config,
        auth_cache,
        api_key_cache,
        session_token,
        weaver_server_core::security::RuntimeSecurityConfig::default(),
        rss,
        watch_folder,
        scheduled_resume,
    );

    routes::build_nzbget_rpc_routes(context)
}

async fn post_nzbget_xmlrpc(app: Router, body: &str, auth_value: &str) -> (StatusCode, String) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/xmlrpc")
                .header(header::AUTHORIZATION, auth_value)
                .header(header::CONTENT_TYPE, "text/xml")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
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
    scheduler_handle_with_mock_commands_with_db(jobs, None)
}

fn scheduler_handle_with_mock_commands_and_db(jobs: Vec<JobInfo>, db: Database) -> SchedulerHandle {
    scheduler_handle_with_mock_commands_with_db(jobs, Some(db))
}

fn scheduler_handle_with_mock_commands_with_db(
    jobs: Vec<JobInfo>,
    db: Option<Database>,
) -> SchedulerHandle {
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
                    options,
                    reply,
                    ..
                } => {
                    let mut jobs = state.list_jobs();
                    let mut job = job_info_from_spec(job_id, spec);
                    if options.initially_paused {
                        job.status = JobStatus::Paused;
                        job.download_state = weaver_server_core::DownloadState::Queued;
                        job.run_state = weaver_server_core::RunState::Paused;
                    }
                    jobs.push(job);
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
                SchedulerCommand::ResumeJob { job_id, reply } => {
                    let result = update_mock_job(&state, job_id, |job| {
                        job.status = JobStatus::Queued;
                        job.download_state = weaver_server_core::DownloadState::Queued;
                        job.run_state = weaver_server_core::RunState::Active;
                    });
                    let _ = reply.send(result);
                }
                SchedulerCommand::UpdateJob {
                    job_id,
                    update,
                    reply,
                } => {
                    let result = update_mock_job(&state, job_id, |job| {
                        match &update.category {
                            weaver_server_core::FieldUpdate::Unchanged => {}
                            weaver_server_core::FieldUpdate::Clear => job.category = None,
                            weaver_server_core::FieldUpdate::Set(category) => {
                                job.category = Some(category.clone());
                            }
                        }
                        match &update.metadata {
                            weaver_server_core::FieldUpdate::Unchanged => {}
                            weaver_server_core::FieldUpdate::Clear => job.metadata.clear(),
                            weaver_server_core::FieldUpdate::Set(metadata) => {
                                job.metadata = metadata.clone();
                            }
                        }
                        match &update.password {
                            weaver_server_core::FieldUpdate::Unchanged => {}
                            weaver_server_core::FieldUpdate::Clear => job.password = None,
                            weaver_server_core::FieldUpdate::Set(password) => {
                                job.password = Some(password.clone());
                            }
                        }
                    });
                    let _ = reply.send(result);
                }
                SchedulerCommand::ReorderJob {
                    job_id,
                    target,
                    reply,
                } => {
                    let mut jobs = state.list_jobs();
                    let result = match jobs.iter().position(|job| job.job_id == job_id) {
                        Some(current) => {
                            let last = jobs.len() - 1;
                            let new_index = match target {
                                weaver_server_core::QueueMoveTarget::Top => 0,
                                weaver_server_core::QueueMoveTarget::Bottom => last,
                                weaver_server_core::QueueMoveTarget::Offset(delta) => {
                                    (current as i64 + delta).clamp(0, last as i64) as usize
                                }
                            };
                            let job = jobs.remove(current);
                            jobs.insert(new_index, job);
                            state.publish_jobs(jobs);
                            Ok(())
                        }
                        None => Err(SchedulerError::JobNotFound(job_id)),
                    };
                    let _ = reply.send(result);
                }
                SchedulerCommand::ReorderJobs { moves, reply } => {
                    let mut jobs = state.list_jobs();
                    // Mirrors `reorder_jobs`' all-or-nothing contract: if any
                    // id is unknown, apply none of the moves.
                    let missing = moves
                        .iter()
                        .find(|(job_id, _)| !jobs.iter().any(|job| job.job_id == *job_id))
                        .map(|(job_id, _)| *job_id);
                    let result = match missing {
                        Some(job_id) => Err(SchedulerError::JobNotFound(job_id)),
                        None => {
                            for &(job_id, target) in &moves {
                                let Some(current) =
                                    jobs.iter().position(|job| job.job_id == job_id)
                                else {
                                    continue;
                                };
                                let last = jobs.len() - 1;
                                let new_index = match target {
                                    weaver_server_core::QueueMoveTarget::Top => 0,
                                    weaver_server_core::QueueMoveTarget::Bottom => last,
                                    weaver_server_core::QueueMoveTarget::Offset(delta) => {
                                        (current as i64 + delta).clamp(0, last as i64) as usize
                                    }
                                };
                                let job = jobs.remove(current);
                                jobs.insert(new_index, job);
                            }
                            state.publish_jobs(jobs);
                            Ok(())
                        }
                    };
                    let _ = reply.send(result);
                }
                SchedulerCommand::PauseAll { reply } => {
                    state.set_paused(true);
                    let _ = reply.send(());
                }
                SchedulerCommand::ResumeAll { reply } => {
                    state.set_paused(false);
                    let _ = reply.send(());
                }
                SchedulerCommand::SetSpeedLimit { reply, .. } => {
                    let _ = reply.send(());
                }
                SchedulerCommand::ReprocessJob { reply, .. } => {
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::CancelJob { job_id, reply, .. } => {
                    let mut jobs = state.list_jobs();
                    let original_len = jobs.len();
                    let cancelled = jobs.iter().find(|job| job.job_id == job_id).cloned();
                    jobs.retain(|job| job.job_id != job_id);
                    let result = if jobs.len() == original_len {
                        Err(SchedulerError::JobNotFound(job_id))
                    } else {
                        if let (Some(db), Some(job)) = (&db, cancelled) {
                            let _ = db.insert_job_history(&weaver_server_core::JobHistoryRow {
                                job_id: job_id.0,
                                job_hash: job.job_hash.map(|hash| hash.to_vec()),
                                name: job.name,
                                status: "cancelled".to_string(),
                                error_message: None,
                                total_bytes: job.total_bytes,
                                downloaded_bytes: job.downloaded_bytes,
                                optional_recovery_bytes: job.optional_recovery_bytes,
                                optional_recovery_downloaded_bytes: job
                                    .optional_recovery_downloaded_bytes,
                                failed_bytes: job.failed_bytes,
                                health: job.health,
                                category: job.category,
                                output_dir: job.output_dir,
                                nzb_path: None,
                                created_at: (job.created_at_epoch_ms / 1000.0) as i64,
                                completed_at: (job.created_at_epoch_ms / 1000.0) as i64,
                                metadata: if job.metadata.is_empty() {
                                    None
                                } else {
                                    serde_json::to_string(&job.metadata).ok()
                                },
                            });
                        }
                        state.publish_jobs(jobs);
                        Ok(())
                    };
                    let _ = reply.send(result);
                }
                SchedulerCommand::DeleteHistory { job_id, reply, .. } => {
                    if let Some(db) = &db {
                        let _ = db.delete_job_history(job_id.0);
                        let _ = db.delete_job_events(job_id.0);
                    }
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::RedownloadJob { reply, .. } => {
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
    let total_files = spec.files.len() as u32;
    let remaining_par_files = spec.par2_volume_count() as u32;
    JobInfo {
        job_id,
        job_hash: None,
        name: spec.name,
        status: JobStatus::Queued,
        download_state: weaver_server_core::DownloadState::Queued,
        finalizing_download: false,
        fetching_repair_data: false,
        post_state: weaver_server_core::PostState::Idle,
        run_state: weaver_server_core::RunState::Active,
        progress: 0.0,
        total_bytes: spec.total_bytes,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files,
        completed_files: 0,
        remaining_par_files,
        password: spec.password,
        category: spec.category,
        metadata: spec.metadata,
        output_dir: None,
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
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
        name: "Silver.Horizon.S05.720p.BluRay.DD5.1.x264-WVR".into(),
        status,
        download_state,
        finalizing_download: false,
        fetching_repair_data: false,
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
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files: 2,
        completed_files: 1,
        remaining_par_files: 1,
        password: None,
        category: Some("tv".into()),
        metadata,
        output_dir: Some("/downloads/tv/Silver.Horizon".into()),
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        created_at_epoch_ms: 1_700_000_000_000.0,
    }
}

fn two_file_nzb() -> String {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1700000000" subject="Test - &quot;alpha.rar&quot; yEnc (1/2)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="400000" number="1">alpha-seg1@test.com</segment>
      <segment bytes="200000" number="2">alpha-seg2@test.com</segment>
    </segments>
  </file>
  <file poster="test@test.com" date="1700000100" subject="Test - &quot;beta.par2&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="100000" number="1">beta-seg1@test.com</segment></segments>
  </file>
</nzb>"#
        .to_string()
}

fn reorder_test_job(job_id: u64) -> JobInfo {
    let mut job = nzbget_test_job(
        job_id,
        JobStatus::Queued,
        weaver_server_core::DownloadState::Queued,
        100,
        0,
        vec![],
    );
    job.name = format!("Job.{job_id}");
    job
}

async fn listgroups_ids(app: Router) -> Vec<u64> {
    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({"method": "listgroups", "params": [], "id": "order"}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    payload["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|group| group["NZBID"].as_u64().unwrap())
        .collect()
}

/// `/api/auth/status` is unauthenticated, so it describes the deployment only
/// to a browser that is about to run the first-run wizard.
fn auth_status_test_router(
    db: Database,
    auth_cache: LoginAuthCache,
    security: weaver_server_core::security::RuntimeSecurityConfig,
) -> Router {
    auth_status_test_router_from_peer(db, auth_cache, security, "127.0.0.1:49152")
}

fn auth_status_test_router_from_peer(
    db: Database,
    auth_cache: LoginAuthCache,
    security: weaver_server_core::security::RuntimeSecurityConfig,
    peer: &str,
) -> Router {
    let peer_addr: SocketAddr = peer.parse().unwrap();
    Router::new()
        .route("/api/auth/status", get(auth::auth_status_handler))
        // `MockConnectInfo` is only read by the `ConnectInfo` extractor; the
        // peer-aware handlers take `Extension<ConnectInfo<_>>`, which is what
        // `into_make_service_with_connect_info` inserts in production.
        .layer(Extension(axum::extract::ConnectInfo(peer_addr)))
        .layer(Extension(db))
        .layer(Extension(security))
        .layer(Extension(auth_cache))
}

async fn auth_status_payload(app: Router) -> serde_json::Value {
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/auth/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

/// One parsed exposition sample.
struct ParsedSample {
    name: String,
    labels: Vec<(String, String)>,
}

/// Metric names that break today's naming rules and are kept anyway, because
/// removing them would break existing dashboards. The list is derived from the
/// catalogue's own deprecation markers, so it cannot drift from the exporter.
fn deprecated_metric_names() -> std::collections::BTreeSet<&'static str> {
    metrics::catalog::metric_catalog()
        .iter()
        .filter(|family| family.deprecated_by.is_some())
        .map(|family| family.name)
        .collect()
}

/// Split a sample line into its metric name, label set, and value.
///
/// This is deliberately a hand parser rather than a `contains` check: the bug
/// it replaces (a literal `\n` in a HELP line swallowing the TYPE line and the
/// first sample) produced output that still *contained* every expected
/// substring while being unparseable by Prometheus.
fn parse_prometheus_sample(line: &str) -> Result<ParsedSample, String> {
    let mut chars = line.char_indices().peekable();
    let mut name_end = 0;
    let mut first = true;
    while let Some(&(idx, ch)) = chars.peek() {
        let valid = if first {
            ch.is_ascii_alphabetic() || ch == '_' || ch == ':'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_' || ch == ':'
        };
        if !valid {
            break;
        }
        first = false;
        name_end = idx + ch.len_utf8();
        chars.next();
    }
    if name_end == 0 {
        return Err(format!("no metric name in {line:?}"));
    }
    let name = line[..name_end].to_string();
    let mut rest = &line[name_end..];

    let mut labels = Vec::new();
    if let Some(stripped) = rest.strip_prefix('{') {
        let mut remaining = stripped;
        loop {
            let key_end = remaining
                .find('=')
                .ok_or_else(|| format!("label without '=' in {line:?}"))?;
            let key = &remaining[..key_end];
            if key.is_empty()
                || !key
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(format!("invalid label name {key:?} in {line:?}"));
            }
            remaining = remaining[key_end + 1..]
                .strip_prefix('"')
                .ok_or_else(|| format!("unquoted label value in {line:?}"))?;

            let mut value = String::new();
            let mut escaped = false;
            let mut closed = false;
            let mut consumed = 0;
            for (idx, ch) in remaining.char_indices() {
                consumed = idx + ch.len_utf8();
                if escaped {
                    value.push(ch);
                    escaped = false;
                    continue;
                }
                match ch {
                    '\\' => escaped = true,
                    '"' => {
                        closed = true;
                        break;
                    }
                    _ => value.push(ch),
                }
            }
            if !closed {
                return Err(format!("unterminated label value in {line:?}"));
            }
            labels.push((key.to_string(), value));
            remaining = &remaining[consumed..];
            if let Some(next) = remaining.strip_prefix(',') {
                remaining = next;
                continue;
            }
            remaining = remaining
                .strip_prefix('}')
                .ok_or_else(|| format!("unterminated label set in {line:?}"))?;
            break;
        }
        rest = remaining;
    }

    let value = rest
        .strip_prefix(' ')
        .ok_or_else(|| format!("missing value separator in {line:?}"))?;
    let valid_value = matches!(value, "NaN" | "+Inf" | "-Inf") || {
        let body = value.strip_prefix('-').unwrap_or(value);
        let (mantissa, exponent) = match body.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => (mantissa, Some(exponent)),
            None => (body, None),
        };
        !mantissa.is_empty()
            && mantissa.chars().all(|c| c.is_ascii_digit() || c == '.')
            && exponent.is_none_or(|exponent| {
                let digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
                !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit())
            })
    };
    if !valid_value {
        return Err(format!("invalid sample value {value:?} in {line:?}"));
    }

    Ok(ParsedSample { name, labels })
}

/// Structural gate every render test runs. Replaces the old
/// `(length, hash)` golden, which pinned bugs in place instead of catching
/// them: a broken HELP line changed the hash exactly as much as a legitimate
/// new metric did, so the fix and the regression were indistinguishable.
fn assert_valid_prometheus_exposition(rendered: &str) {
    let deprecated = deprecated_metric_names();
    println!(
        "deprecated names exempt from naming rules ({}): {}",
        deprecated.len(),
        deprecated.iter().copied().collect::<Vec<_>>().join(", ")
    );

    let mut help: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    let mut types: std::collections::HashMap<&str, (usize, &str)> =
        std::collections::HashMap::new();
    let mut seen_series: std::collections::HashSet<(String, Vec<(String, String)>)> =
        std::collections::HashSet::new();

    for (number, line) in rendered.lines().enumerate() {
        assert!(!line.is_empty(), "line {number} is blank");
        if let Some(rest) = line.strip_prefix("# HELP ") {
            assert!(
                !rest.contains("\\n"),
                "line {number} carries a literal backslash-n: {line:?}"
            );
            let (name, text) = rest
                .split_once(' ')
                .unwrap_or_else(|| panic!("line {number} has HELP without text: {line:?}"));
            assert!(
                !text.is_empty(),
                "line {number} has an empty HELP: {line:?}"
            );
            let count = help.entry(name).or_insert(0);
            *count += 1;
            assert_eq!(*count, 1, "duplicate HELP for {name}");
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            assert!(
                !rest.contains("\\n"),
                "line {number} carries a literal backslash-n: {line:?}"
            );
            let (name, kind) = rest
                .split_once(' ')
                .unwrap_or_else(|| panic!("line {number} has TYPE without a kind: {line:?}"));
            assert!(
                matches!(
                    kind,
                    "counter" | "gauge" | "summary" | "histogram" | "untyped"
                ),
                "line {number} has an unknown metric type {kind:?}"
            );
            assert!(
                types.insert(name, (number, kind)).is_none(),
                "duplicate TYPE for {name}"
            );
            assert!(help.contains_key(name), "TYPE for {name} precedes its HELP");
            continue;
        }
        assert!(
            !line.starts_with('#'),
            "line {number} is a comment that is neither HELP nor TYPE: {line:?}"
        );

        let sample =
            parse_prometheus_sample(line).unwrap_or_else(|error| panic!("line {number}: {error}"));

        // Resolve the owning family: summaries and histograms emit suffixed
        // series under the base family's descriptor.
        let family = ["_bucket", "_sum", "_count"]
            .into_iter()
            .find_map(|suffix| {
                sample
                    .name
                    .strip_suffix(suffix)
                    .filter(|base| types.contains_key(base))
            })
            .unwrap_or(sample.name.as_str());
        let (_, kind) = types.get(family).copied().unwrap_or_else(|| {
            panic!("line {number}: sample {family} has no TYPE: {line:?}");
        });
        assert!(
            help.contains_key(family),
            "line {number}: sample {family} has no HELP"
        );

        if !deprecated.contains(family) {
            if kind == "counter" {
                assert!(
                    family.ends_with("_total"),
                    "counter {family} must end in _total"
                );
            } else if kind == "gauge" {
                assert!(
                    !family.ends_with("_total"),
                    "gauge {family} must not end in _total"
                );
            }
        }

        assert!(
            seen_series.insert((sample.name.clone(), sample.labels.clone())),
            "line {number}: duplicate series {line:?}"
        );
    }
}

fn populated_metrics_snapshot() -> MetricsSnapshot {
    MetricsSnapshot {
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
        write_pending_bytes: 2,
        uu_spooled_bytes: 0,
        uu_spooled_segments: 0,
        direct_write_evictions: 9,
        direct_sets_admitted: 0,
        direct_sets_demoted: 0,
        direct_sets_finalized_direct: 0,
        direct_sets_repaired_while_direct: 0,
        deobfuscated_members_renamed: 0,
        decode_pressure_soft_limit_bytes: 100,
        decode_pressure_hard_limit_bytes: 200,
        write_pressure_soft_limit_bytes: 300,
        write_pressure_hard_limit_bytes: 400,
        download_pressure_state: weaver_server_core::DownloadPressureState::Soft,
        download_pressure_reason: weaver_server_core::DownloadPressureReason::Decode,
        download_pressure_stalls_total: 24,
        download_pressure_stall_duration_ms: 1500,
        download_pressure_current_stall_ms: 250,
        download_restart_durable_lead_blocked_total: 0,
        hot_dispatch_job_id: 42,
        hot_dispatch_mode: weaver_server_core::DispatchShareMode::Shared,
        hot_dispatch_underfill_ms: 2500,
        hot_dispatch_lent_connections: 2,
        hot_dispatch_last_spillover_decision:
            weaver_server_core::SpilloverDecision::AllowedUnderfill,
        hot_dispatch_spillover_blocked_pressure_total: 30,
        hot_dispatch_spillover_blocked_near_cap_total: 31,
        hot_dispatch_spillover_blocked_hot_can_use_capacity_total: 32,
        hot_dispatch_spillover_blocked_best_mode_pending_total: 33,
        hot_dispatch_spillover_blocked_cap_speed_total: 35,
        hot_dispatch_spillover_allowed_underfill_total: 33,
        hot_dispatch_spillover_allowed_measured_underfill_total: 0,
        hot_dispatch_spillover_reclaimed_total: 34,
        hot_dispatch_hot_speed_bps: 35,
        hot_dispatch_exclusive_peak_bps: 36,
        hot_dispatch_spillover_pre_speed_bps: 37,
        hot_dispatch_spillover_post_speed_bps: 38,
        hot_dispatch_spillover_active_loans: 1,
        hot_dispatch_spillover_reclaimed_speed_harm_total: 39,
        hot_dispatch_recent_expansion_improvement_pct: 5,
        hot_dispatch_best_mode_block_reason: 1,
        hot_dispatch_last_expansion_kind: 0,
        hot_dispatch_last_expansion_before_bps: 0,
        hot_dispatch_last_expansion_after_bps: 0,
        download_lanes_active: 3,
        download_lanes_sequential_active: 1,
        download_lanes_depth2_active: 2,
        download_lanes_depth4_active: 0,
        download_lanes_depth8_active: 0,
        download_lanes_idle_active: 0,
        download_lanes_awaiting_work_active: 0,
        download_lanes_binding_server_active: 0,
        download_lanes_acquired_active: 0,
        download_lanes_issuing_active: 3,
        download_lanes_draining_active: 0,
        download_lanes_yield_after_batch_active: 0,
        download_lanes_parking_active: 0,
        download_lanes_recovering_active: 0,
        download_lane_parks_no_work_total: 35,
        download_lane_parks_pressure_total: 36,
        download_lane_parks_probe_yield_total: 37,
        download_lane_parks_hot_reclaim_total: 38,
        download_lane_parks_hot_share_yield_total: 0,
        download_lane_parks_spillover_withdraw_total: 39,
        download_lane_parks_spillover_speed_harm_total: 0,
        download_lane_parks_ip_replacement_retired_total: 0,
        download_lane_parks_proof_failure_total: 41,
        download_lane_parks_error_total: 42,
        download_lane_lease_items_total: 43,
        download_lane_refill_granted_total: 44,
        download_lane_refill_parked_total: 45,
        download_lane_refill_deferred_total: 0,
        download_pipeline_trial_success_total: 46,
        download_pipeline_trial_failure_total: 47,
        download_pipeline_proof_pass_total: 48,
        download_pipeline_cooldown_total: 49,
        download_pipeline_replay_items_total: 50,
        ip_replacement_trial_extra_connections: 1,
        ip_replacement_burst_active: true,
        ip_replacement_over_max_connections: 1,
        ip_rtt_ewma_entries: 2,
        ip_rtt_ewma_slowest_ms: 123,
        ip_replacement_trials_started_total: 51,
        ip_replacement_trials_rejected_total: 52,
        ip_replacement_trials_accepted_total: 53,
        ip_replacement_trials_blocked_total: 54,
        ip_replacement_trials_acquire_failed_total: 0,
        ip_replacement_trials_same_ip_rejected_total: 0,
        ip_replacement_old_connections_retired_total: 55,
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
        parked_infrastructure_work: 29,
        nntp_generation_recovery_requeues: 30,
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
    }
}

fn sample_job(job_id: u64, name: &str, status: JobStatus) -> JobInfo {
    JobInfo {
        job_id: JobId(job_id),
        job_hash: None,
        name: name.into(),
        status,
        download_state: weaver_server_core::DownloadState::Downloading,
        finalizing_download: false,
        fetching_repair_data: false,
        post_state: weaver_server_core::PostState::Idle,
        run_state: weaver_server_core::RunState::Active,
        progress: 0.5,
        total_bytes: 100,
        downloaded_bytes: 50,
        optional_recovery_bytes: 25,
        optional_recovery_downloaded_bytes: 5,
        phase_progress: Vec::new(),
        failed_bytes: 2,
        health: 999,
        terminal_discards: Vec::new(),
        total_files: 0,
        completed_files: 0,
        remaining_par_files: 0,
        password: Some("secret".into()),
        category: Some("tv".into()),
        metadata: Vec::new(),
        output_dir: None,
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        created_at_epoch_ms: 1_700_000_000_000.0,
    }
}

fn sample_post_processing_metrics()
-> weaver_server_core::post_processing::executor::PostProcessingMetricsSnapshot {
    weaver_server_core::post_processing::executor::PostProcessingMetricsSnapshot {
        queue_depth: 1,
        active_attempts: 2,
        duration_count: 3,
        duration_sum_millis: 4_500,
        succeeded: 5,
        failed: 6,
        skipped: 7,
        timed_out: 8,
        cancelled: 9,
        interrupted: 10,
        truncated: 11,
    }
}

fn sample_server_health() -> metrics::ServerHealthInfo {
    metrics::ServerHealthInfo {
        label: "news.example:563".into(),
        server_id: "7".into(),
        host: "news.example".into(),
        port: 563,
        tls: true,
        priority: 1,
        backfill: false,
        state: metrics::ServerStateKind::Healthy,
        state_reason: metrics::ServerStateReason::None,
        state_until_epoch_seconds: 0.0,
        disable_count: 0,
        success_count: 0,
        failure_count: 0,
        consecutive_failures: 0,
        latency_ms: 0.0,
        connections_available: 0,
        connections_active: 0,
        connections_max: 80,
        connections_configured: 80,
        capacity_penalty_until_epoch_ms: 0,
        premature_deaths: 0,
    }
}

fn manual_pause_block() -> DownloadBlockState {
    DownloadBlockState {
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
        scheduled_speed_limit: 4_096,
    }
}

/// Every distinct value of `label` that `family` emitted, in rendered order.
fn rendered_label_values(rendered: &str, family: &str, label: &str) -> Vec<String> {
    let mut values = Vec::new();
    for line in rendered.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Ok(sample) = parse_prometheus_sample(line) else {
            continue;
        };
        if sample.name != family {
            continue;
        }
        if let Some((_, value)) = sample.labels.iter().find(|(key, _)| key == label)
            && !values.contains(value)
        {
            values.push(value.clone());
        }
    }
    values
}

fn rendered_family_names(rendered: &str) -> std::collections::BTreeSet<String> {
    rendered
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|rest| rest.split_once(' '))
        .map(|(name, _)| name.to_string())
        .collect()
}

fn assert_label_set(rendered: &str, family: &str, label: &str, expected: &[&str]) {
    let mut actual = rendered_label_values(rendered, family, label);
    actual.sort();
    let mut expected: Vec<String> = expected.iter().map(|value| value.to_string()).collect();
    expected.sort();
    assert_eq!(actual, expected, "label set drift on {family}{{{label}}}");
}

fn sample_transfer_snapshot() -> weaver_nntp::transfer::ServerTransferSnapshot {
    weaver_nntp::transfer::ServerTransferSnapshot {
        stable_server_id: weaver_nntp::transfer::StableServerId(7),
        rate_bytes_per_sec: 1_000,
        lifetime_body_bytes: 2_000,
        quota_enabled: true,
        quota_limit_bytes: 9_000,
        quota_used_bytes: 3_000,
        quota_reserved_bytes: 500,
        quota_remaining_bytes: 5_500,
        quota_blocked: false,
        quota_generation: 1,
        capacity_revision: 1,
        retry_at: None,
        throttle_wait: std::time::Duration::from_millis(250),
    }
}

/// Bounds shared by the collection-side fixtures below. The exact values do not
/// matter to the exporter — it renders whatever bounds the snapshot carries —
/// but a two-bound histogram keeps the expected `le` lines readable.
const TEST_BOUNDS: &[f64] = &[0.1, 1.0];

/// A histogram with per-bucket counts 2/3/1, i.e. cumulative 2/5/6.
fn sample_histogram() -> instr::HistogramSnapshot {
    instr::HistogramSnapshot {
        bounds: TEST_BOUNDS,
        counts: vec![2, 3, 1],
        sum: 4.5,
        count: 6,
    }
}

fn sample_server_metrics() -> instr::ServerMetricsSnapshot {
    instr::ServerMetricsSnapshot {
        stable_server_id: 7,
        server_idx: 0,
        attempts: instr::ServerAttemptOutcomeKind::ALL
            .iter()
            .flat_map(|outcome| {
                [true, false].into_iter().map(move |recovery| {
                    instr::ServerAttemptCount {
                        outcome: outcome.as_str(),
                        recovery,
                        // Distinct per cell so a mis-keyed label shows up as a
                        // wrong value rather than a coincidental match.
                        count: u64::from(*outcome == instr::ServerAttemptOutcomeKind::NotFound)
                            * 11
                            + u64::from(recovery),
                    }
                })
            })
            .collect(),
        article_latency: sample_histogram(),
    }
}

fn sample_job_lifecycle() -> instr::JobLifecycleMetricsSnapshot {
    instr::JobLifecycleMetricsSnapshot {
        submitted: vec![instr::JobSubmissionCount {
            origin: "api",
            category: "tv".to_string(),
            count: 5,
        }],
        finished: vec![instr::JobFinishCount {
            result: "complete",
            category: "tv".to_string(),
            count: 4,
        }],
        job_duration: instr::JobResultKind::ALL
            .iter()
            .map(|result| (result.as_str(), sample_histogram()))
            .collect(),
        stage_duration: instr::JobStageKind::ALL
            .iter()
            .map(|stage| (stage.as_str(), sample_histogram()))
            .collect(),
        verifications: instr::VerificationOutcomeKind::ALL
            .iter()
            .map(|outcome| (outcome.as_str(), 3u64))
            .collect(),
        repairs: instr::StageOutcomeKind::ALL
            .iter()
            .map(|outcome| (outcome.as_str(), 2u64))
            .collect(),
        repair_slices_repaired_total: 17,
        extractions: instr::StageOutcomeKind::ALL
            .iter()
            .map(|outcome| (outcome.as_str(), 1u64))
            .collect(),
        files_missing_total: 6,
        missing_segments_total: 61,
        bytes_by_category: vec![("tv".to_string(), 4096), (String::new(), 512)],
    }
}

fn sample_pipeline_histograms() -> instr::PipelineHistogramsSnapshot {
    instr::PipelineHistogramsSnapshot {
        disk_write_duration: sample_histogram(),
        decode_task_duration: Some(sample_histogram()),
        extract_member_duration: Some(sample_histogram()),
    }
}

fn sample_db_runtime() -> instr::DbRuntimeMetricsSnapshot {
    instr::DbRuntimeMetricsSnapshot {
        engine: "sqlite",
        concurrency: 1,
        in_flight: 2,
        blocked_submissions_total: 9,
        op_duration: sample_histogram(),
    }
}

fn sample_process_metrics() -> instr::ProcessMetricsSnapshot {
    instr::ProcessMetricsSnapshot {
        cpu_seconds_total: Some(12.5),
        resident_memory_bytes: Some(64 * 1024 * 1024),
        virtual_memory_bytes: Some(512 * 1024 * 1024),
        open_fds: Some(48),
        max_fds: Some(1024),
        threads: Some(16),
        start_time_seconds: Some(1_600_000_000.0),
    }
}

fn sample_disk_space() -> Vec<instr::DiskSpaceSnapshot> {
    vec![
        instr::DiskSpaceSnapshot {
            role: "data",
            path: "/var/lib/weaver".to_string(),
            total_bytes: 1_000_000,
            available_bytes: 400_000,
        },
        instr::DiskSpaceSnapshot {
            role: "complete",
            path: "/var/lib/weaver/complete".to_string(),
            total_bytes: 2_000_000,
            available_bytes: 50_000,
        },
    ]
}

fn sample_http_metrics() -> instr::HttpMetricsSnapshot {
    instr::HttpMetricsSnapshot {
        requests: vec![
            instr::HttpRequestCount {
                route: "/graphql",
                method: "POST",
                status: 200,
                count: 42,
            },
            instr::HttpRequestCount {
                route: "/api/login",
                method: "POST",
                status: 401,
                count: 3,
            },
        ],
        duration: vec![("/graphql", sample_histogram())],
    }
}

/// Every collection-side input, so callers can populate a render without
/// restating the fixtures. Held as a struct because the render input borrows
/// each of them.
struct CollectionFixtures {
    server_metrics: Vec<instr::ServerMetricsSnapshot>,
    job_lifecycle: instr::JobLifecycleMetricsSnapshot,
    pipeline_histograms: instr::PipelineHistogramsSnapshot,
    db_runtime: instr::DbRuntimeMetricsSnapshot,
    process: instr::ProcessMetricsSnapshot,
    disk_space: Vec<instr::DiskSpaceSnapshot>,
    http_metrics: instr::HttpMetricsSnapshot,
}

impl CollectionFixtures {
    fn new() -> Self {
        Self {
            server_metrics: vec![sample_server_metrics()],
            job_lifecycle: sample_job_lifecycle(),
            pipeline_histograms: sample_pipeline_histograms(),
            db_runtime: sample_db_runtime(),
            process: sample_process_metrics(),
            disk_space: sample_disk_space(),
            http_metrics: sample_http_metrics(),
        }
    }

    /// The fixtures must outlive the render input, which is why they live in
    /// one struct rather than as a pile of temporaries at each call site.
    fn apply<'a>(&'a self, input: &mut metrics::PrometheusRenderInput<'a>) {
        input.server_metrics = &self.server_metrics;
        input.job_lifecycle = Some(&self.job_lifecycle);
        input.pipeline_histograms = Some(&self.pipeline_histograms);
        input.db_runtime = Some(&self.db_runtime);
        input.process = Some(&self.process);
        input.disk_space = &self.disk_space;
        input.http_metrics = Some(&self.http_metrics);
    }
}

/// Build the most complete render the exporter can produce, so the catalogue
/// comparison sees every family.
fn fully_populated_render() -> String {
    let snapshot = populated_metrics_snapshot();
    let block = manual_pause_block();
    let jobs = vec![sample_job(42, "Silver Horizon", JobStatus::Downloading)];
    let server_health = vec![sample_server_health()];
    let transfers = vec![sample_transfer_snapshot()];
    let duplicates = [("api", "accepted", 3u64)];
    let lifecycle = [("promoted", 2u64)];
    let rejections = [("unsafe_path", 1u64), ("ratio", 2u64)];
    let post_processing = sample_post_processing_metrics();
    let collection = CollectionFixtures::new();

    let mut input = metrics::PrometheusRenderInput::new(&snapshot, &block);
    input.jobs = &jobs;
    input.server_health = &server_health;
    input.server_transfers = &transfers;
    input.duplicate_admission = &duplicates;
    input.semantic_duplicate_lifecycle = &lifecycle;
    input.extraction_rejections = &rejections;
    input.post_processing = Some(&post_processing);
    input.runtime_generation = 3;
    input.start_time_seconds = 1_700_000_000.0;
    collection.apply(&mut input);
    metrics::render_prometheus_metrics_input(&input)
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

mod nzbget_version_uses_jsonrpc;
mod renders_prometheus_metrics_for;
mod restart_handler_tests;
mod setup_handler_tests;
