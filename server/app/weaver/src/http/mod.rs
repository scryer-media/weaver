mod assets;
mod auth;
mod backup;
mod graphql;
mod jobs;
mod metrics;
mod routes;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::decompression::RequestDecompressionLayer;
use tracing::info;

use weaver_server_api::{BackupService, WeaverSchema};
use weaver_server_core::Database;
use weaver_server_core::SchedulerHandle;
use weaver_server_core::auth::{ApiKeyCache, LoginAuthCache};

pub(crate) use self::metrics::PrometheusMetricsExporter;

#[derive(Clone)]
struct SessionToken(Arc<String>);

#[derive(Clone)]
struct RequestAuthContext {
    db: Database,
    auth_cache: LoginAuthCache,
    api_key_cache: ApiKeyCache,
    session_token: SessionToken,
}

pub struct ServerRuntime {
    pub schema: WeaverSchema,
    pub handle: SchedulerHandle,
    pub db: Database,
    pub auth_cache: LoginAuthCache,
    pub api_key_cache: ApiKeyCache,
    pub backup: BackupService,
    pub metrics_exporter: PrometheusMetricsExporter,
    pub base_url: String,
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

fn internal_upload_err(e: impl std::fmt::Display) -> (axum::http::StatusCode, String) {
    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

pub async fn run_server(
    runtime: ServerRuntime,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let base_url = runtime.base_url.clone();
    let app = routes::build_router(runtime)
        .layer(
            CompressionLayer::new()
                .gzip(true)
                .deflate(true)
                .br(true)
                .zstd(true),
        )
        .layer(
            RequestDecompressionLayer::new()
                .gzip(true)
                .deflate(true)
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

#[cfg(test)]
mod tests;
