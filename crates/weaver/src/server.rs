use std::net::SocketAddr;
use std::path::PathBuf;

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

use weaver_api::auth::{CallerScope, hash_api_key};
use weaver_api::{BackupService, BackupStatus, CategoryRemapInput, RestoreOptions, WeaverSchema};
use weaver_state::Database;

#[derive(Embed)]
#[folder = "../../apps/weaver-web/dist/"]
struct FrontendAssets;

/// Resolve the caller scope from an optional API key header.
async fn resolve_scope(
    db: &Database,
    api_key_header: Option<&axum::http::HeaderValue>,
) -> Result<CallerScope, StatusCode> {
    let Some(hdr) = api_key_header else {
        return Ok(CallerScope::Local);
    };
    let raw_key = hdr.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
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
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Result<GraphQLResponse, StatusCode> {
    let scope = resolve_scope(&db, headers.get("x-api-key")).await?;
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
    api_key_header: Option<&axum::http::HeaderValue>,
) -> Result<(), StatusCode> {
    let scope = resolve_scope(db, api_key_header).await?;
    if scope.is_admin() {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

async fn backup_status_handler(
    Extension(db): Extension<Database>,
    Extension(backup): Extension<BackupService>,
    headers: HeaderMap,
) -> Result<Json<BackupStatus>, StatusCode> {
    require_admin(&db, headers.get("x-api-key")).await?;
    let status = backup
        .status()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(status))
}

async fn backup_export_handler(
    Extension(db): Extension<Database>,
    Extension(backup): Extension<BackupService>,
    headers: HeaderMap,
    Json(body): Json<BackupExportRequest>,
) -> Response {
    if let Err(status) = require_admin(&db, headers.get("x-api-key")).await {
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
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    if let Err(status) = require_admin(&db, headers.get("x-api-key")).await {
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
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    if let Err(status) = require_admin(&db, headers.get("x-api-key")).await {
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
    headers: HeaderMap,
    protocol: GraphQLProtocol,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, StatusCode> {
    // Check X-Api-Key header on the WS upgrade request.
    let scope = resolve_scope(&db, headers.get("x-api-key")).await?;

    let mut data = Data::default();
    data.insert(scope);

    let resp = ws
        .protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |stream| {
            let ws = GraphQLWebSocket::new(stream, schema, protocol)
                .with_data(data)
                .on_connection_init(move |payload: serde_json::Value| async move {
                    // Allow connection_init payload to carry an api_key too
                    // for WS clients that can't set custom headers.
                    if let Some(key) = payload.get("api_key").and_then(|v| v.as_str()) {
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
                    } else {
                        Ok(Data::default())
                    }
                });
            ws.serve()
        });
    Ok(resp)
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Try the exact path first, then fall back to index.html for SPA routing.
    if let Some(file) = FrontendAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, mime.as_ref().to_string())],
            file.data,
        )
            .into_response()
    } else if let Some(index) = FrontendAssets::get("index.html") {
        let mime = mime_guess::from_path("index.html").first_or_octet_stream();
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, mime.as_ref().to_string())],
            index.data,
        )
            .into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

pub async fn run_server(
    schema: WeaverSchema,
    db: Database,
    backup: BackupService,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        .route("/graphql", post(graphql_handler))
        .route("/graphql/ws", get(ws_handler))
        .route("/admin/backup/status", get(backup_status_handler))
        .route("/admin/backup/export", post(backup_export_handler))
        .route("/admin/backup/inspect", post(backup_inspect_handler))
        .route("/admin/backup/restore", post(backup_restore_handler))
        .fallback(get(static_handler))
        .layer(Extension(schema))
        .layer(Extension(backup))
        .layer(Extension(db))
        .layer(CompressionLayer::new())
        .layer(RequestDecompressionLayer::new())
        .layer(CorsLayer::permissive());

    info!(%addr, "starting HTTP server");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
