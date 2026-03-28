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

use weaver_api::auth::{
    CallerScope, generate_api_key, hash_api_key, hash_password, needs_rehash, verify_password,
};
use weaver_api::jwt::{self, JWT_TTL_SECS};
use weaver_api::{BackupService, BackupStatus, CategoryRemapInput, RestoreOptions, WeaverSchema};
use weaver_nntp::pool::NntpPool;
use weaver_scheduler::handle::{DownloadBlockKind, DownloadBlockState};
use weaver_scheduler::{JobInfo, JobStatus, MetricsSnapshot, SchedulerHandle};
use weaver_state::Database;

/// Shared handle to the NNTP connection pool for per-server metrics.
#[derive(Clone)]
struct NntpPoolHandle(Arc<NntpPool>);

#[derive(Embed)]
#[folder = "../../apps/weaver-web/dist/"]
struct FrontendAssets;

#[derive(Clone)]
struct BaseUrl(Arc<String>);

#[derive(Clone)]
struct SessionToken(Arc<String>);

/// Extract the `weaver_jwt` cookie value from request headers.
fn extract_jwt_cookie(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(';'))
        .map(str::trim)
        .find_map(|cookie| cookie.strip_prefix("weaver_jwt=").map(|v| v.to_string()))
}

/// Check if login auth is enabled and return the JWT secret if so.
async fn jwt_secret_if_auth_enabled(db: &Database) -> Option<[u8; 32]> {
    let db = db.clone();
    tokio::task::spawn_blocking(move || {
        db.get_auth_credentials()
            .ok()
            .flatten()
            .map(|creds| jwt::derive_jwt_secret(&creds.password_hash))
    })
    .await
    .ok()
    .flatten()
}

/// Resolve the caller scope from API key header, JWT cookie, or session token.
async fn resolve_scope(
    db: &Database,
    session_token: &str,
    headers: &HeaderMap,
) -> Result<CallerScope, StatusCode> {
    let api_key_header = headers.get("x-api-key");

    // 1. API key header (session token or stored key).
    if let Some(hdr) = api_key_header {
        let raw_key = hdr.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
        if raw_key == session_token {
            return Ok(CallerScope::Local);
        }
        let key_hash = hash_api_key(raw_key);
        let db_clone = db.clone();
        let db_touch = db.clone();
        let row = tokio::task::spawn_blocking(move || db_clone.lookup_api_key(&key_hash))
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if let Some(row) = row {
            let id = row.id;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            tokio::task::spawn_blocking(move || {
                let _ = db_touch.touch_api_key_last_used(id, now);
            });
            return match row.scope.as_str() {
                "admin" => Ok(CallerScope::Admin),
                _ => Ok(CallerScope::Integration),
            };
        }
    }

    // 2. JWT cookie (when login auth is enabled).
    if let Some(token) = extract_jwt_cookie(headers)
        && let Some(secret) = jwt_secret_if_auth_enabled(db).await
        && jwt::verify_jwt(&token, &secret).is_ok()
    {
        return Ok(CallerScope::Admin);
    }

    // 3. No auth enabled → open access (backward compat).
    if jwt_secret_if_auth_enabled(db).await.is_none() {
        // Auth is not configured — allow open access with session token.
        if api_key_header.is_some() {
            // They sent a key but it didn't match anything.
            return Err(StatusCode::UNAUTHORIZED);
        }
        return Ok(CallerScope::Local);
    }

    Err(StatusCode::UNAUTHORIZED)
}

async fn graphql_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(db): Extension<Database>,
    Extension(SessionToken(session_token)): Extension<SessionToken>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Result<GraphQLResponse, StatusCode> {
    let scope = resolve_scope(&db, &session_token, &headers).await?;
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
    headers: &HeaderMap,
) -> Result<(), StatusCode> {
    let scope = resolve_scope(db, session_token, headers).await?;
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
    require_admin(&db, &session_token, &headers).await?;
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
    if let Err(status) = require_admin(&db, &session_token, &headers).await {
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
    if let Err(status) = require_admin(&db, &session_token, &headers).await {
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
    if let Err(status) = require_admin(&db, &session_token, &headers).await {
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
    headers: HeaderMap,
    protocol: GraphQLProtocol,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    // Pre-resolve scope from cookies on the upgrade request. Browsers
    // automatically send cookies on WebSocket upgrade, so JWT auth works
    // without needing api_key in connection_init.
    let upgrade_scope = resolve_scope(&db, &session_token, &headers).await.ok();

    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |stream| {
            let ws = GraphQLWebSocket::new(stream, schema, protocol).on_connection_init(
                move |payload: serde_json::Value| async move {
                    // If the upgrade request was already authenticated (via cookie),
                    // use that scope directly.
                    if let Some(scope) = upgrade_scope {
                        let mut data = Data::default();
                        data.insert(scope);
                        return Ok(data);
                    }

                    // Fall back to connection_init payload (api_key field).
                    let Some(key) = payload.get("api_key").and_then(|v| v.as_str()) else {
                        return Err(async_graphql::Error::new(
                            "Missing api_key in connection_init",
                        ));
                    };
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

async fn metrics_handler(
    Extension(handle): Extension<SchedulerHandle>,
    Extension(NntpPoolHandle(pool)): Extension<NntpPoolHandle>,
) -> impl IntoResponse {
    let snapshot = handle.get_metrics();
    let jobs = handle.list_jobs();
    let download_block = handle.get_download_block();
    let server_health = collect_server_health(&pool).await;
    let body = render_prometheus_metrics(
        &snapshot,
        &jobs,
        handle.is_globally_paused(),
        &download_block,
        &server_health,
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
    Extension(db): Extension<Database>,
) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Try the exact path first, then fall back to index.html for SPA routing.
    if let Some(file) = FrontendAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();

        // Hashed assets (Vite output in assets/) are immutable — cache for 1 year.
        // Everything else (sw.js, manifest, etc.) gets no-cache.
        let cache_control = if path.starts_with("assets/") {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        };

        // Serve pre-compressed .gz variant if client accepts gzip.
        if accepts_gzip(&headers) {
            let gz_path = format!("{path}.gz");
            if let Some(gz_file) = FrontendAssets::get(&gz_path) {
                return (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, mime.as_ref().to_string()),
                        (header::CONTENT_ENCODING, "gzip".to_string()),
                        (header::CACHE_CONTROL, cache_control.to_string()),
                    ],
                    gz_file.data,
                )
                    .into_response();
            }
        }

        (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, mime.as_ref().to_string()),
                (header::CACHE_CONTROL, cache_control.to_string()),
            ],
            file.data,
        )
            .into_response()
    } else {
        // SPA fallback: serve index.html (or login page if auth is enabled).
        // When auth is enabled and the user doesn't have a valid JWT cookie,
        // serve the standalone login page instead of the React app.
        if let Some(secret) = jwt_secret_if_auth_enabled(&db).await {
            let has_valid_jwt = extract_jwt_cookie(&headers)
                .is_some_and(|token| jwt::verify_jwt(&token, &secret).is_ok());
            if !has_valid_jwt {
                return login_page_response();
            }
        }

        if let Some(index) = FrontendAssets::get("index.html") {
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
}

// ── Login / Logout / Auth Status ─────────────────────────────────────────

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

async fn login_handler(
    Extension(db): Extension<Database>,
    Json(body): Json<LoginRequest>,
) -> Response {
    let db_clone = db.clone();
    let creds = match tokio::task::spawn_blocking(move || db_clone.get_auth_credentials())
        .await
        .ok()
        .and_then(|r| r.ok())
        .flatten()
    {
        Some(c) => c,
        None => {
            return error_response(StatusCode::BAD_REQUEST, "login is not enabled");
        }
    };

    if body.username != creds.username {
        return error_response(StatusCode::UNAUTHORIZED, "invalid credentials");
    }

    let hash = creds.password_hash.clone();
    let pw = body.password.clone();
    let valid = tokio::task::spawn_blocking(move || verify_password(&pw, &hash))
        .await
        .unwrap_or(false);

    if !valid {
        return error_response(StatusCode::UNAUTHORIZED, "invalid credentials");
    }

    // Transparent rehash: upgrade legacy scrypt hashes to argon2id on successful login.
    let password_hash = if needs_rehash(&creds.password_hash) {
        let pw = body.password.clone();
        let db_rehash = db.clone();
        let username = creds.username.clone();
        if let Ok(new_hash) = tokio::task::spawn_blocking(move || {
            hash_password(&pw).and_then(|h| {
                db_rehash
                    .set_auth_credentials(&username, &h)
                    .map_err(|e| format!("db update failed: {e}"))?;
                Ok(h)
            })
        })
        .await
        .unwrap_or(Err("task failed".into()))
        {
            new_hash
        } else {
            creds.password_hash.clone()
        }
    } else {
        creds.password_hash.clone()
    };

    let secret = jwt::derive_jwt_secret(&password_hash);
    let token = jwt::create_jwt(&creds.username, &secret, JWT_TTL_SECS);
    let cookie =
        format!("weaver_jwt={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={JWT_TTL_SECS}");

    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie)],
        Json(serde_json::json!({ "ok": true })),
    )
        .into_response()
}

async fn logout_handler() -> Response {
    let cookie = "weaver_jwt=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0";
    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie.to_string())],
        Json(serde_json::json!({ "ok": true })),
    )
        .into_response()
}

async fn auth_status_handler(
    Extension(db): Extension<Database>,
    headers: HeaderMap,
) -> Json<serde_json::Value> {
    let db_clone = db.clone();
    let creds = tokio::task::spawn_blocking(move || db_clone.get_auth_credentials())
        .await
        .ok()
        .and_then(|r| r.ok())
        .flatten();

    let enabled = creds.is_some();
    let authenticated = if let Some(creds) = creds {
        if let Some(token) = extract_jwt_cookie(&headers) {
            let secret = jwt::derive_jwt_secret(&creds.password_hash);
            jwt::verify_jwt(&token, &secret).is_ok()
        } else {
            false
        }
    } else {
        true // auth not enabled → everyone is "authenticated"
    };

    Json(serde_json::json!({
        "enabled": enabled,
        "authenticated": authenticated,
    }))
}

// ── Standalone Login Page ────────────────────────────────────────────────

const LOGIN_PAGE_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Weaver - Login</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background:#0a0e1a;color:#e2e8f0;display:flex;align-items:center;
    justify-content:center;min-height:100vh}
  .card{background:#111827;border:1px solid rgba(255,255,255,.08);
    border-radius:20px;padding:40px;width:100%;max-width:380px;
    box-shadow:0 20px 60px rgba(0,0,0,.4)}
  h1{font-size:1.5rem;font-weight:600;margin-bottom:8px;letter-spacing:-.02em}
  .subtitle{font-size:.8rem;color:#64748b;text-transform:uppercase;
    letter-spacing:.2em;margin-bottom:32px}
  label{display:block;font-size:.85rem;color:#94a3b8;margin-bottom:6px}
  input{width:100%;padding:10px 14px;border:1px solid rgba(255,255,255,.1);
    border-radius:10px;background:#0f172a;color:#e2e8f0;font-size:.95rem;
    margin-bottom:16px;outline:none;transition:border .2s}
  input:focus{border-color:#6366f1}
  button{width:100%;padding:11px;border:none;border-radius:10px;
    background:#6366f1;color:#fff;font-size:.95rem;font-weight:500;
    cursor:pointer;transition:background .2s}
  button:hover{background:#4f46e5}
  button:disabled{opacity:.5;cursor:not-allowed}
  .error{color:#f87171;font-size:.85rem;margin-bottom:12px;display:none}
  .forgot{margin-top:16px;text-align:center}
  .forgot a{color:#6366f1;font-size:.85rem;text-decoration:none;cursor:pointer}
  .forgot a:hover{text-decoration:underline}
  .reset-help{display:none;margin-top:12px;padding:12px;border-radius:10px;
    background:#0f172a;border:1px solid rgba(255,255,255,.08);font-size:.8rem;
    color:#94a3b8;line-height:1.5}
  .reset-help code{background:#1e293b;padding:2px 6px;border-radius:4px;
    color:#e2e8f0;font-size:.8rem}
</style>
</head>
<body>
<div class="card">
  <h1>Weaver</h1>
  <div class="subtitle">Sign In</div>
  <form id="form">
    <label for="username">Username</label>
    <input id="username" name="username" type="text" autocomplete="username" required autofocus/>
    <label for="password">Password</label>
    <input id="password" name="password" type="password" autocomplete="current-password" required/>
    <div class="error" id="error"></div>
    <button type="submit" id="btn">Sign In</button>
  </form>
  <div class="forgot">
    <a id="forgot-link">Forgot password?</a>
    <div class="reset-help" id="reset-help">
      Stop weaver, then restart with:<br/><br/>
      <code>WEAVER_RESET_LOGIN=1</code><br/><br/>
      This disables login protection on startup. You can then set a new password in Settings &rarr; Security.
      <br/><br/>
      <strong>Docker:</strong><br/>
      <code>docker run -e WEAVER_RESET_LOGIN=1 ...</code><br/><br/>
      <strong>Bare metal:</strong><br/>
      <code>WEAVER_RESET_LOGIN=1 weaver serve</code>
    </div>
  </div>
</div>
<script>
const form=document.getElementById("form"),
  err=document.getElementById("error"),
  btn=document.getElementById("btn");
form.addEventListener("submit",async e=>{
  e.preventDefault();
  err.style.display="none";
  btn.disabled=true;
  btn.textContent="Signing in\u2026";
  try{
    const r=await fetch("/api/login",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({
        username:form.username.value,
        password:form.password.value
      })
    });
    if(r.ok){window.location.href="/";return}
    const d=await r.json().catch(()=>({}));
    err.textContent=d.error||"Login failed";
    err.style.display="block";
  }catch{
    err.textContent="Connection error";
    err.style.display="block";
  }
  btn.disabled=false;
  btn.textContent="Sign In";
});
document.getElementById("forgot-link").addEventListener("click",()=>{
  const el=document.getElementById("reset-help");
  el.style.display=el.style.display==="block"?"none":"block";
});
</script>
</body>
</html>"#;

fn login_page_response() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8".to_string())],
        LOGIN_PAGE_HTML,
    )
        .into_response()
}

pub async fn run_server(
    schema: WeaverSchema,
    handle: SchedulerHandle,
    db: Database,
    backup: BackupService,
    nntp_pool: Arc<NntpPool>,
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
        .route("/api/login", post(login_handler))
        .route("/api/logout", post(logout_handler))
        .route("/api/auth/status", get(auth_status_handler))
        .route("/", get(static_handler))
        .fallback(get(static_handler))
        .layer(Extension(handle))
        .layer(Extension(schema))
        .layer(Extension(backup))
        .layer(Extension(db))
        .layer(Extension(NntpPoolHandle(nntp_pool)))
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

struct ServerHealthInfo {
    label: String,
    state: &'static str,
    success_count: u64,
    failure_count: u64,
    consecutive_failures: u32,
    latency_ms: f64,
    connections_available: usize,
    connections_max: usize,
    premature_deaths: usize,
}

async fn collect_server_health(pool: &NntpPool) -> Vec<ServerHealthInfo> {
    let configs = pool.server_configs();
    // Build labels and read load outside the health lock.
    let pre: Vec<(String, usize, usize)> = configs
        .iter()
        .enumerate()
        .map(|(idx, cfg)| {
            let (avail, max) = pool.server_load(idx);
            (format!("{}:{}", cfg.host, cfg.port), avail, max)
        })
        .collect();

    // Hold the health lock only for field reads — no allocations inside.
    let health = pool.health().lock().await;
    pre.into_iter()
        .enumerate()
        .map(|(idx, (label, avail, max))| {
            let srv = health.server(idx);
            ServerHealthInfo {
                label,
                state: match srv.state() {
                    weaver_nntp::ServerState::Healthy => "healthy",
                    weaver_nntp::ServerState::Degraded { .. } => "degraded",
                    weaver_nntp::ServerState::Disabled { .. } => "disabled",
                },
                success_count: srv.success_count,
                failure_count: srv.failure_count,
                consecutive_failures: srv.consecutive_failures,
                latency_ms: health.latency_ms(idx),
                connections_available: avail,
                connections_max: max,
                premature_deaths: health.recent_premature_deaths(idx),
            }
        })
        .collect()
}

fn render_prometheus_metrics(
    snapshot: &MetricsSnapshot,
    jobs: &[JobInfo],
    pipeline_paused: bool,
    download_block: &DownloadBlockState,
    server_health: &[ServerHealthInfo],
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

    // Per-server NNTP health metrics.
    if !server_health.is_empty() {
        out.push_str("# HELP weaver_server_state Server health state (1=healthy, 0=disabled).\n");
        out.push_str("# TYPE weaver_server_state gauge\n");
        out.push_str(
            "# HELP weaver_server_success_total Total successful operations per server.\n",
        );
        out.push_str("# TYPE weaver_server_success_total counter\n");
        out.push_str("# HELP weaver_server_failure_total Total failed operations per server.\n");
        out.push_str("# TYPE weaver_server_failure_total counter\n");
        out.push_str("# HELP weaver_server_consecutive_failures Current run of consecutive failures per server.\n");
        out.push_str("# TYPE weaver_server_consecutive_failures gauge\n");
        out.push_str("# HELP weaver_server_latency_ms EWMA latency in milliseconds per server.\n");
        out.push_str("# TYPE weaver_server_latency_ms gauge\n");
        out.push_str(
            "# HELP weaver_server_connections_available Available connection permits per server.\n",
        );
        out.push_str("# TYPE weaver_server_connections_available gauge\n");
        out.push_str("# HELP weaver_server_connections_max Maximum connections per server.\n");
        out.push_str("# TYPE weaver_server_connections_max gauge\n");
        out.push_str(
            "# HELP weaver_server_premature_deaths Recent connections that died before 60s age.\n",
        );
        out.push_str("# TYPE weaver_server_premature_deaths gauge\n");

        for srv in server_health {
            let labels: &[(&str, &str)] = &[("server", &srv.label)];
            append_labeled_metric(
                &mut out,
                "weaver_server_state",
                labels,
                if srv.state == "healthy" { 1 } else { 0 },
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_success_total",
                labels,
                srv.success_count,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_failure_total",
                labels,
                srv.failure_count,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_consecutive_failures",
                labels,
                srv.consecutive_failures,
            );
            append_labeled_metric_f64(&mut out, "weaver_server_latency_ms", labels, srv.latency_ms);
            append_labeled_metric(
                &mut out,
                "weaver_server_connections_available",
                labels,
                srv.connections_available,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_connections_max",
                labels,
                srv.connections_max,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_premature_deaths",
                labels,
                srv.premature_deaths,
            );
        }
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

fn append_labeled_metric_f64(out: &mut String, name: &str, labels: &[(&str, &str)], value: f64) {
    out.push_str(name);
    append_labels(out, labels);
    out.push(' ');
    out.push_str(&format_prometheus_f64(value));
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

fn all_job_statuses() -> [&'static str; 11] {
    [
        "queued",
        "downloading",
        "checking",
        "verifying",
        "queued_repair",
        "repairing",
        "queued_extract",
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
        JobStatus::QueuedRepair => "queued_repair",
        JobStatus::Repairing => "repairing",
        JobStatus::QueuedExtract => "queued_extract",
        JobStatus::Extracting => "extracting",
        JobStatus::Moving => "moving",
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
            escape_prometheus_label_value("a\"b\\c\nd"),
            "a\\\"b\\\\c\\nd"
        );
    }
}
