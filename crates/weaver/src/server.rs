use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_graphql::Data;
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::Json;
use axum::Router;
use axum::extract::{Extension, Multipart, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::{get, post};
use rust_embed::Embed;
use serde::Deserialize;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::decompression::RequestDecompressionLayer;
use tracing::info;

use weaver_api::auth::{CallerScope, generate_api_key, hash_api_key};
use weaver_api::{BackupService, BackupStatus, CategoryRemapInput, RestoreOptions, WeaverSchema};
use weaver_scheduler::handle::{DownloadBlockKind, DownloadBlockState};
use weaver_scheduler::{JobInfo, JobStatus, MetricsSnapshot, SchedulerHandle};
use weaver_state::Database;

#[derive(Embed)]
#[folder = "../../apps/weaver-web/dist/"]
struct FrontendAssets;

#[derive(Clone)]
struct BaseUrl(Arc<String>);

#[derive(Clone)]
struct SessionToken(Arc<String>);

/// Resolve the caller scope from an optional API key header.
async fn resolve_scope(
    db: &Database,
    session_token: &str,
    api_key_header: Option<&axum::http::HeaderValue>,
) -> Result<CallerScope, StatusCode> {
    let Some(hdr) = api_key_header else {
        return Err(StatusCode::UNAUTHORIZED);
    };
    let raw_key = hdr.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    if raw_key == session_token {
        return Ok(CallerScope::Local);
    }
    let key_hash = hash_api_key(raw_key);
    let db = db.clone();
    let db2 = db.clone();
    let row = tokio::task::spawn_blocking(move || db.lookup_api_key(&key_hash))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match row {
        Some(row) => {
            // Fire-and-forget last_used_at update.
            let id = row.id;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            tokio::task::spawn_blocking(move || {
                let _ = db2.touch_api_key_last_used(id, now);
            });
            match row.scope.as_str() {
                "admin" => Ok(CallerScope::Admin),
                _ => Ok(CallerScope::Integration),
            }
        }
        None => Err(StatusCode::UNAUTHORIZED),
    }
}

async fn graphql_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(db): Extension<Database>,
    Extension(SessionToken(session_token)): Extension<SessionToken>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Result<GraphQLResponse, StatusCode> {
    let scope = resolve_scope(&db, &session_token, headers.get("x-api-key")).await?;
    let mut request = req.into_inner();
    request = request.data(scope);
    Ok(schema.execute(request).await.into())
}

#[derive(Debug, Deserialize)]
struct BackupExportRequest {
    password: Option<String>,
}

async fn require_admin(
    db: &Database,
    session_token: &str,
    api_key_header: Option<&axum::http::HeaderValue>,
) -> Result<(), StatusCode> {
    let scope = resolve_scope(db, session_token, api_key_header).await?;
    if scope.is_admin() {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

async fn backup_status_handler(
    Extension(db): Extension<Database>,
    Extension(backup): Extension<BackupService>,
    Extension(SessionToken(session_token)): Extension<SessionToken>,
    headers: HeaderMap,
) -> Result<Json<BackupStatus>, StatusCode> {
    require_admin(&db, &session_token, headers.get("x-api-key")).await?;
    let status = backup
        .status()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(status))
}

async fn backup_export_handler(
    Extension(db): Extension<Database>,
    Extension(backup): Extension<BackupService>,
    Extension(SessionToken(session_token)): Extension<SessionToken>,
    headers: HeaderMap,
    Json(body): Json<BackupExportRequest>,
) -> Response {
    if let Err(status) = require_admin(&db, &session_token, headers.get("x-api-key")).await {
        return status.into_response();
    }
    match backup.create_backup(body.password).await {
        Ok(artifact) => (
            [
                (header::CONTENT_TYPE, "application/octet-stream".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", artifact.filename),
                ),
            ],
            artifact.bytes,
        )
            .into_response(),
        Err(error) => error_response(error.status_code(), &error.to_string()),
    }
}

async fn backup_inspect_handler(
    Extension(db): Extension<Database>,
    Extension(backup): Extension<BackupService>,
    Extension(SessionToken(session_token)): Extension<SessionToken>,
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    if let Err(status) = require_admin(&db, &session_token, headers.get("x-api-key")).await {
        return status.into_response();
    }
    match parse_backup_upload(multipart).await {
        Ok(upload) => match backup
            .inspect_backup(&upload.file_path, upload.password)
            .await
        {
            Ok(result) => Json(result).into_response(),
            Err(error) => error_response(error.status_code(), &error.to_string()),
        },
        Err((status, message)) => error_response(status, &message),
    }
}

async fn backup_restore_handler(
    Extension(db): Extension<Database>,
    Extension(backup): Extension<BackupService>,
    Extension(SessionToken(session_token)): Extension<SessionToken>,
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    if let Err(status) = require_admin(&db, &session_token, headers.get("x-api-key")).await {
        return status.into_response();
    }
    match parse_backup_upload(multipart).await {
        Ok(upload) => {
            let options = RestoreOptions {
                data_dir: upload.data_dir.unwrap_or_default(),
                intermediate_dir: upload.intermediate_dir,
                complete_dir: upload.complete_dir,
                category_remaps: upload.category_remaps,
            };
            if options.data_dir.trim().is_empty() {
                return error_response(StatusCode::BAD_REQUEST, "data_dir is required");
            }
            match backup
                .restore_backup(&upload.file_path, upload.password, options)
                .await
            {
                Ok(report) => Json(report).into_response(),
                Err(error) => error_response(error.status_code(), &error.to_string()),
            }
        }
        Err((status, message)) => error_response(status, &message),
    }
}

struct ParsedBackupUpload {
    _temp_dir: tempfile::TempDir,
    file_path: PathBuf,
    password: Option<String>,
    data_dir: Option<String>,
    intermediate_dir: Option<String>,
    complete_dir: Option<String>,
    category_remaps: Vec<CategoryRemapInput>,
}

async fn parse_backup_upload(
    mut multipart: Multipart,
) -> Result<ParsedBackupUpload, (StatusCode, String)> {
    use tokio::io::AsyncWriteExt;

    let temp_dir = tempfile::tempdir().map_err(internal_upload_err)?;
    let file_path = temp_dir.path().join("upload.bin");
    let mut saw_file = false;
    let mut password = None;
    let mut data_dir = None;
    let mut intermediate_dir = None;
    let mut complete_dir = None;
    let mut category_remaps = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
    {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "file" => {
                saw_file = true;
                let mut dest = tokio::fs::File::create(&file_path)
                    .await
                    .map_err(internal_upload_err)?;
                let mut field = field;
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
                {
                    dest.write_all(&chunk).await.map_err(internal_upload_err)?;
                }
                dest.flush().await.map_err(internal_upload_err)?;
            }
            "password" => {
                password = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?,
                );
            }
            "data_dir" => {
                data_dir = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?,
                );
            }
            "intermediate_dir" => {
                intermediate_dir = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?,
                );
            }
            "complete_dir" => {
                complete_dir = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?,
                );
            }
            "category_remaps" => {
                let raw = field
                    .text()
                    .await
                    .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
                category_remaps = serde_json::from_str(&raw)
                    .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
            }
            _ => {}
        }
    }

    if !saw_file {
        return Err((StatusCode::BAD_REQUEST, "file is required".into()));
    }

    Ok(ParsedBackupUpload {
        _temp_dir: temp_dir,
        file_path,
        password,
        data_dir,
        intermediate_dir,
        complete_dir,
        category_remaps,
    })
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

fn internal_upload_err(e: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

async fn ws_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(db): Extension<Database>,
    Extension(SessionToken(session_token)): Extension<SessionToken>,
    protocol: GraphQLProtocol,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    // Browsers cannot send custom HTTP headers on WebSocket upgrade requests,
    // so we skip auth here and rely on `connection_init` payload instead.
    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |stream| {
            let ws = GraphQLWebSocket::new(stream, schema, protocol).on_connection_init(
                move |payload: serde_json::Value| async move {
                    let Some(key) = payload.get("api_key").and_then(|v| v.as_str()) else {
                        return Err(async_graphql::Error::new(
                            "Missing api_key in connection_init",
                        ));
                    };
                    // Check session token first.
                    if key == session_token.as_str() {
                        let mut data = Data::default();
                        data.insert(CallerScope::Local);
                        return Ok(data);
                    }
                    let key_hash = hash_api_key(key);
                    let row = db.lookup_api_key(&key_hash).map_err(|e| {
                        async_graphql::Error::new(format!("auth lookup failed: {e}"))
                    })?;
                    match row {
                        Some(row) => {
                            let scope = match row.scope.as_str() {
                                "admin" => CallerScope::Admin,
                                _ => CallerScope::Integration,
                            };
                            let mut data = Data::default();
                            data.insert(scope);
                            Ok(data)
                        }
                        None => Err(async_graphql::Error::new("Invalid API key")),
                    }
                },
            );
            ws.serve()
        })
}

async fn metrics_handler(Extension(handle): Extension<SchedulerHandle>) -> impl IntoResponse {
    let snapshot = handle.get_metrics();
    let jobs = handle.list_jobs();
    let download_block = handle.get_download_block();
    let body = render_prometheus_metrics(
        &snapshot,
        &jobs,
        handle.is_globally_paused(),
        &download_block,
    );
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8".to_string(),
        )],
        body,
    )
}

/// Rewrite `index.html` to inject the session token and (optionally) base URL.
///
/// Always injects `window.__WEAVER_SESSION__` so the frontend can authenticate.
/// When `base_url` is non-empty (e.g. "/weaver"), also:
/// 1. Replaces `<base href="/">` with `<base href="/weaver/">`
/// 2. Injects `window.__WEAVER_BASE__` so the frontend knows its prefix
fn rewrite_index_html(raw: &[u8], base_url: &str, session_token: &str) -> Vec<u8> {
    let html = String::from_utf8_lossy(raw);
    let html = if base_url.is_empty() {
        html.into_owned()
    } else {
        let html = html.replace(
            "<base href=\"/\" />",
            &format!("<base href=\"{base_url}/\" />"),
        );
        html.replace(
            "</head>",
            &format!(
                "<script>window.__WEAVER_BASE__={}</script>\n  </head>",
                serde_json::to_string(base_url).unwrap_or_default()
            ),
        )
    };
    let html = html.replace(
        "</head>",
        &format!(
            "<script>window.__WEAVER_SESSION__={}</script>\n  </head>",
            serde_json::to_string(session_token).unwrap_or_default()
        ),
    );
    html.into_bytes()
}

fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("gzip"))
}

async fn static_handler(
    uri: Uri,
    headers: HeaderMap,
    Extension(BaseUrl(base_url)): Extension<BaseUrl>,
    Extension(SessionToken(session_token)): Extension<SessionToken>,
) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Try the exact path first, then fall back to index.html for SPA routing.
    if let Some(file) = FrontendAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();

        // Serve pre-compressed .gz variant if client accepts gzip.
        if accepts_gzip(&headers) {
            let gz_path = format!("{path}.gz");
            if let Some(gz_file) = FrontendAssets::get(&gz_path) {
                return (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, mime.as_ref().to_string()),
                        (header::CONTENT_ENCODING, "gzip".to_string()),
                    ],
                    gz_file.data,
                )
                    .into_response();
            }
        }

        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, mime.as_ref().to_string())],
            file.data,
        )
            .into_response()
    } else if let Some(index) = FrontendAssets::get("index.html") {
        let mime = mime_guess::from_path("index.html").first_or_octet_stream();
        let body = rewrite_index_html(&index.data, &base_url, &session_token);
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, mime.as_ref().to_string())],
            body,
        )
            .into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

pub async fn run_server(
    schema: WeaverSchema,
    handle: SchedulerHandle,
    db: Database,
    backup: BackupService,
    addr: SocketAddr,
    base_url: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let base_url_ext = BaseUrl(Arc::new(base_url.clone()));
    let session_token = SessionToken(Arc::new(generate_api_key()));

    let inner = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/graphql", post(graphql_handler))
        .route("/graphql/ws", get(ws_handler))
        .route("/api/backup/status", get(backup_status_handler))
        .route("/api/backup/export", post(backup_export_handler))
        .route("/api/backup/inspect", post(backup_inspect_handler))
        .route("/api/backup/restore", post(backup_restore_handler))
        .route("/", get(static_handler))
        .fallback(get(static_handler))
        .layer(Extension(handle))
        .layer(Extension(schema))
        .layer(Extension(backup))
        .layer(Extension(db))
        .layer(Extension(base_url_ext))
        .layer(Extension(session_token));

    let app = if base_url.is_empty() {
        inner
    } else {
        // The inner router's `.route("/", ...)` is hoisted as an exact match for
        // `{base_url}` by axum's nest, but `{base_url}/` (trailing slash) falls
        // through. Add an explicit redirect so both paths work.
        let bare = base_url.clone();
        Router::new()
            .route(
                &format!("{base_url}/"),
                get(move || async move { axum::response::Redirect::permanent(&bare) }),
            )
            .nest(&base_url, inner)
    };

    let app = app
        .layer(CompressionLayer::new().gzip(true).br(true).zstd(true))
        .layer(
            RequestDecompressionLayer::new()
                .gzip(true)
                .br(true)
                .zstd(true),
        )
        .layer(CorsLayer::permissive());

    info!(%addr, base_url = if base_url.is_empty() { "/" } else { &base_url }, "starting HTTP server");
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        format!("failed to bind to {addr}: {e} — is another process using this port?")
    })?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn render_prometheus_metrics(
    snapshot: &MetricsSnapshot,
    jobs: &[JobInfo],
    pipeline_paused: bool,
    download_block: &DownloadBlockState,
) -> String {
    let mut out = String::with_capacity(16 * 1024);
    out.push_str("# HELP weaver_build_info Static build information.\n");
    out.push_str("# TYPE weaver_build_info gauge\n");
    append_labeled_metric(
        &mut out,
        "weaver_build_info",
        &[("version", env!("CARGO_PKG_VERSION"))],
        1,
    );

    out.push_str("# HELP weaver_pipeline_paused Whether the entire pipeline is globally paused.\n");
    out.push_str("# TYPE weaver_pipeline_paused gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_paused",
        if pipeline_paused { 1 } else { 0 },
    );

    out.push_str("# HELP weaver_pipeline_download_gate Current global download gate by reason.\n");
    out.push_str("# TYPE weaver_pipeline_download_gate gauge\n");
    for reason in ["none", "manual_pause", "isp_cap"] {
        let active = match (reason, download_block.kind) {
            ("none", DownloadBlockKind::None) => 1,
            ("manual_pause", DownloadBlockKind::ManualPause) => 1,
            ("isp_cap", DownloadBlockKind::IspCap) => 1,
            _ => 0,
        };
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_gate",
            &[("reason", reason)],
            active,
        );
    }

    out.push_str(
        "# HELP weaver_bandwidth_cap_enabled Whether the ISP bandwidth cap policy is enabled.\n",
    );
    out.push_str("# TYPE weaver_bandwidth_cap_enabled gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_enabled",
        if download_block.cap_enabled { 1 } else { 0 },
    );

    out.push_str(
        "# HELP weaver_bandwidth_cap_used_bytes Current ISP bandwidth cap usage in bytes.\n",
    );
    out.push_str("# TYPE weaver_bandwidth_cap_used_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_used_bytes",
        download_block.used_bytes,
    );

    out.push_str(
        "# HELP weaver_bandwidth_cap_limit_bytes Configured ISP bandwidth cap limit in bytes.\n",
    );
    out.push_str("# TYPE weaver_bandwidth_cap_limit_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_limit_bytes",
        download_block.limit_bytes,
    );

    out.push_str("# HELP weaver_bandwidth_cap_remaining_bytes Remaining ISP bandwidth cap bytes in the active window.\n");
    out.push_str("# TYPE weaver_bandwidth_cap_remaining_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_remaining_bytes",
        download_block.remaining_bytes,
    );

    out.push_str("# HELP weaver_bandwidth_cap_reserved_bytes Bytes conservatively reserved for in-flight downloads against the active cap window.\n");
    out.push_str("# TYPE weaver_bandwidth_cap_reserved_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_reserved_bytes",
        download_block.reserved_bytes,
    );

    out.push_str("# HELP weaver_bandwidth_cap_window_end_seconds Active ISP bandwidth cap window end as a unix timestamp.\n");
    out.push_str("# TYPE weaver_bandwidth_cap_window_end_seconds gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_window_end_seconds",
        download_block
            .window_ends_at_epoch_ms
            .map(|value| (value / 1000.0) as u64)
            .unwrap_or(0),
    );

    out.push_str("# HELP weaver_pipeline_jobs Number of active jobs by status.\n");
    out.push_str("# TYPE weaver_pipeline_jobs gauge\n");
    for status in all_job_statuses() {
        let count = jobs
            .iter()
            .filter(|job| job_status_label(&job.status) == status)
            .count();
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_jobs",
            &[("status", status)],
            count,
        );
    }

    out.push_str(
        "# HELP weaver_pipeline_bytes_downloaded_total Total bytes downloaded by the pipeline.\n",
    );
    out.push_str("# TYPE weaver_pipeline_bytes_downloaded_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_bytes_downloaded_total",
        snapshot.bytes_downloaded,
    );

    out.push_str(
        "# HELP weaver_pipeline_bytes_decoded_total Total bytes decoded by the pipeline.\n",
    );
    out.push_str("# TYPE weaver_pipeline_bytes_decoded_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_bytes_decoded_total",
        snapshot.bytes_decoded,
    );

    out.push_str("# HELP weaver_pipeline_bytes_committed_total Total bytes committed to disk by the pipeline.\n");
    out.push_str("# TYPE weaver_pipeline_bytes_committed_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_bytes_committed_total",
        snapshot.bytes_committed,
    );

    out.push_str("# HELP weaver_pipeline_segments_downloaded_total Total segments downloaded.\n");
    out.push_str("# TYPE weaver_pipeline_segments_downloaded_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_segments_downloaded_total",
        snapshot.segments_downloaded,
    );

    out.push_str("# HELP weaver_pipeline_segments_decoded_total Total segments decoded.\n");
    out.push_str("# TYPE weaver_pipeline_segments_decoded_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_segments_decoded_total",
        snapshot.segments_decoded,
    );

    out.push_str("# HELP weaver_pipeline_segments_committed_total Total segments committed.\n");
    out.push_str("# TYPE weaver_pipeline_segments_committed_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_segments_committed_total",
        snapshot.segments_committed,
    );

    out.push_str("# HELP weaver_pipeline_segments_retried_total Total segments retried.\n");
    out.push_str("# TYPE weaver_pipeline_segments_retried_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_segments_retried_total",
        snapshot.segments_retried,
    );

    out.push_str("# HELP weaver_pipeline_segments_failed_permanent_total Total segments permanently failed.\n");
    out.push_str("# TYPE weaver_pipeline_segments_failed_permanent_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_segments_failed_permanent_total",
        snapshot.segments_failed_permanent,
    );

    out.push_str("# HELP weaver_pipeline_articles_not_found_total Total articles not found.\n");
    out.push_str("# TYPE weaver_pipeline_articles_not_found_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_articles_not_found_total",
        snapshot.articles_not_found,
    );

    out.push_str("# HELP weaver_pipeline_decode_errors_total Total decode errors.\n");
    out.push_str("# TYPE weaver_pipeline_decode_errors_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_decode_errors_total",
        snapshot.decode_errors,
    );

    out.push_str("# HELP weaver_pipeline_crc_errors_total Total CRC errors.\n");
    out.push_str("# TYPE weaver_pipeline_crc_errors_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_crc_errors_total",
        snapshot.crc_errors,
    );

    out.push_str("# HELP weaver_pipeline_download_queue_depth Download queue depth.\n");
    out.push_str("# TYPE weaver_pipeline_download_queue_depth gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_download_queue_depth",
        snapshot.download_queue_depth,
    );

    out.push_str("# HELP weaver_pipeline_decode_pending Decode pending queue depth.\n");
    out.push_str("# TYPE weaver_pipeline_decode_pending gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_decode_pending",
        snapshot.decode_pending,
    );

    out.push_str("# HELP weaver_pipeline_commit_pending Commit pending queue depth.\n");
    out.push_str("# TYPE weaver_pipeline_commit_pending gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_commit_pending",
        snapshot.commit_pending,
    );

    out.push_str("# HELP weaver_pipeline_recovery_queue_depth Recovery queue depth.\n");
    out.push_str("# TYPE weaver_pipeline_recovery_queue_depth gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_recovery_queue_depth",
        snapshot.recovery_queue_depth,
    );

    out.push_str("# HELP weaver_pipeline_write_buffered_bytes Buffered write bytes.\n");
    out.push_str("# TYPE weaver_pipeline_write_buffered_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_write_buffered_bytes",
        snapshot.write_buffered_bytes,
    );

    out.push_str("# HELP weaver_pipeline_write_buffered_segments Buffered write segments.\n");
    out.push_str("# TYPE weaver_pipeline_write_buffered_segments gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_write_buffered_segments",
        snapshot.write_buffered_segments,
    );

    out.push_str("# HELP weaver_pipeline_direct_write_evictions_total Direct write evictions.\n");
    out.push_str("# TYPE weaver_pipeline_direct_write_evictions_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_direct_write_evictions_total",
        snapshot.direct_write_evictions,
    );

    out.push_str("# HELP weaver_pipeline_verify_active Active verification workers.\n");
    out.push_str("# TYPE weaver_pipeline_verify_active gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_verify_active",
        snapshot.verify_active,
    );

    out.push_str("# HELP weaver_pipeline_repair_active Active repair workers.\n");
    out.push_str("# TYPE weaver_pipeline_repair_active gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_repair_active",
        snapshot.repair_active,
    );

    out.push_str("# HELP weaver_pipeline_extract_active Active extraction workers.\n");
    out.push_str("# TYPE weaver_pipeline_extract_active gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_extract_active",
        snapshot.extract_active,
    );

    out.push_str("# HELP weaver_pipeline_disk_write_latency_microseconds Disk write latency in microseconds.\n");
    out.push_str("# TYPE weaver_pipeline_disk_write_latency_microseconds gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_disk_write_latency_microseconds",
        snapshot.disk_write_latency_us,
    );

    out.push_str("# HELP weaver_pipeline_current_download_speed_bytes_per_second Current download speed in bytes per second.\n");
    out.push_str("# TYPE weaver_pipeline_current_download_speed_bytes_per_second gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_current_download_speed_bytes_per_second",
        snapshot.current_download_speed,
    );

    out.push_str(
        "# HELP weaver_pipeline_articles_per_second Current effective article throughput.\n",
    );
    out.push_str("# TYPE weaver_pipeline_articles_per_second gauge\n");
    append_metric_f64(
        &mut out,
        "weaver_pipeline_articles_per_second",
        snapshot.articles_per_sec,
    );

    out.push_str("# HELP weaver_pipeline_decode_rate_mebibytes_per_second Current decode throughput in MiB per second.\n");
    out.push_str("# TYPE weaver_pipeline_decode_rate_mebibytes_per_second gauge\n");
    append_metric_f64(
        &mut out,
        "weaver_pipeline_decode_rate_mebibytes_per_second",
        snapshot.decode_rate_mbps,
    );

    out.push_str("# HELP weaver_job_info Static information for active jobs.\n");
    out.push_str("# TYPE weaver_job_info gauge\n");
    out.push_str("# HELP weaver_job_progress_ratio Fractional job progress from 0 to 1.\n");
    out.push_str("# TYPE weaver_job_progress_ratio gauge\n");
    out.push_str("# HELP weaver_job_total_bytes Expected total bytes for the job.\n");
    out.push_str("# TYPE weaver_job_total_bytes gauge\n");
    out.push_str("# HELP weaver_job_downloaded_bytes Downloaded bytes for the job.\n");
    out.push_str("# TYPE weaver_job_downloaded_bytes gauge\n");
    out.push_str("# HELP weaver_job_optional_recovery_bytes Optional recovery bytes available for the job.\n");
    out.push_str("# TYPE weaver_job_optional_recovery_bytes gauge\n");
    out.push_str("# HELP weaver_job_optional_recovery_downloaded_bytes Optional recovery bytes downloaded for the job.\n");
    out.push_str("# TYPE weaver_job_optional_recovery_downloaded_bytes gauge\n");
    out.push_str("# HELP weaver_job_failed_bytes Permanently failed bytes for the job.\n");
    out.push_str("# TYPE weaver_job_failed_bytes gauge\n");
    out.push_str("# HELP weaver_job_health_per_mille Job health in per-mille.\n");
    out.push_str("# TYPE weaver_job_health_per_mille gauge\n");
    out.push_str("# HELP weaver_job_created_at_seconds Unix creation timestamp for the job.\n");
    out.push_str("# TYPE weaver_job_created_at_seconds gauge\n");

    for job in jobs {
        append_job_metric(&mut out, "weaver_job_info", job, 1);
        append_job_metric_f64(&mut out, "weaver_job_progress_ratio", job, job.progress);
        append_job_metric(&mut out, "weaver_job_total_bytes", job, job.total_bytes);
        append_job_metric(
            &mut out,
            "weaver_job_downloaded_bytes",
            job,
            job.downloaded_bytes,
        );
        append_job_metric(
            &mut out,
            "weaver_job_optional_recovery_bytes",
            job,
            job.optional_recovery_bytes,
        );
        append_job_metric(
            &mut out,
            "weaver_job_optional_recovery_downloaded_bytes",
            job,
            job.optional_recovery_downloaded_bytes,
        );
        append_job_metric(&mut out, "weaver_job_failed_bytes", job, job.failed_bytes);
        append_job_metric(&mut out, "weaver_job_health_per_mille", job, job.health);
        append_job_metric_f64(
            &mut out,
            "weaver_job_created_at_seconds",
            job,
            job.created_at_epoch_ms / 1000.0,
        );
    }

    out
}

fn append_metric<T: std::fmt::Display>(out: &mut String, name: &str, value: T) {
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_metric_f64(out: &mut String, name: &str, value: f64) {
    out.push_str(name);
    out.push(' ');
    out.push_str(&format_prometheus_f64(value));
    out.push('\n');
}

fn append_labeled_metric<T: std::fmt::Display>(
    out: &mut String,
    name: &str,
    labels: &[(&str, &str)],
    value: T,
) {
    out.push_str(name);
    append_labels(out, labels);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_job_metric<T: std::fmt::Display>(out: &mut String, name: &str, job: &JobInfo, value: T) {
    out.push_str(name);
    append_job_labels(out, job);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_job_metric_f64(out: &mut String, name: &str, job: &JobInfo, value: f64) {
    out.push_str(name);
    append_job_labels(out, job);
    out.push(' ');
    out.push_str(&format_prometheus_f64(value));
    out.push('\n');
}

fn append_labels(out: &mut String, labels: &[(&str, &str)]) {
    if labels.is_empty() {
        return;
    }
    out.push('{');
    for (idx, (key, value)) in labels.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(key);
        out.push_str("=\"");
        out.push_str(&escape_prometheus_label_value(value));
        out.push('"');
    }
    out.push('}');
}

fn append_job_labels(out: &mut String, job: &JobInfo) {
    append_labels(
        out,
        &[
            ("job_id", &job.job_id.0.to_string()),
            ("job_name", &job.name),
            ("status", job_status_label(&job.status)),
            ("category", job.category.as_deref().unwrap_or("")),
            (
                "has_password",
                if job.password.is_some() {
                    "true"
                } else {
                    "false"
                },
            ),
        ],
    );
}

fn escape_prometheus_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn format_prometheus_f64(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else if value.is_nan() {
        "NaN".to_string()
    } else if value.is_sign_negative() {
        "-Inf".to_string()
    } else {
        "+Inf".to_string()
    }
}

fn all_job_statuses() -> [&'static str; 9] {
    [
        "queued",
        "downloading",
        "checking",
        "verifying",
        "repairing",
        "extracting",
        "complete",
        "failed",
        "paused",
    ]
}

fn job_status_label(status: &JobStatus) -> &'static str {
    match status {
        JobStatus::Queued => "queued",
        JobStatus::Downloading => "downloading",
        JobStatus::Checking => "checking",
        JobStatus::Verifying => "verifying",
        JobStatus::Repairing => "repairing",
        JobStatus::Extracting => "extracting",
        JobStatus::Complete => "complete",
        JobStatus::Failed { .. } => "failed",
        JobStatus::Paused => "paused",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_core::id::JobId;

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

        let rendered = render_prometheus_metrics(
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
            },
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
            escape_prometheus_label_value("a\"b\\c\nd"),
            "a\\\"b\\\\c\\nd"
        );
    }
}
