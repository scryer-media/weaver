mod assets;
mod auth;
mod backup;
mod graphql;
mod health;
mod jobs;
mod metrics;
mod nzbget;
mod request_metrics;
mod routes;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::http::{HeaderValue, Method, Response as HttpResponse, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tower_http::compression::{
    CompressionLayer,
    predicate::{DefaultPredicate, Predicate},
};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::decompression::RequestDecompressionLayer;
use tracing::info;

use weaver_server_api::{BackupService, RssService, WeaverSchema};
use weaver_server_core::Database;
use weaver_server_core::SchedulerHandle;
use weaver_server_core::auth::{ApiKeyCache, LoginAuthCache};
use weaver_server_core::operations::disk::DiskSpaceCollector;
use weaver_server_core::operations::instrumentation::{DiskSpaceSnapshot, HttpMetricsSnapshot};
use weaver_server_core::security::RuntimeSecurityConfig;
use weaver_server_core::settings::model::SharedConfig;

pub(crate) use self::metrics::PrometheusMetricsExporter;
pub(crate) use self::request_metrics::HttpMetricsHandle;

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
    pub scheduled_resume: weaver_server_api::ScheduledResumeCoordinator,
    pub db: Database,
    pub auth_cache: LoginAuthCache,
    pub api_key_cache: ApiKeyCache,
    pub backup: BackupService,
    pub rss: RssService,
    pub watch_folder: weaver_server_core::watch_folder::WatchFolderService,
    pub metrics_exporter: PrometheusMetricsExporter,
    pub config: SharedConfig,
    pub base_url: String,
    pub security: RuntimeSecurityConfig,
    /// TTL-cached free-space sampler for the configured directory roles.
    /// Constructed at wiring time from the resolved data/intermediate/complete
    /// directories; read by the exporter at scrape time.
    pub(crate) disk_space: Arc<DiskSpaceCollector>,
    /// Per-route HTTP request counters and latency, written by the
    /// `request_metrics` middleware and read by the exporter.
    pub(crate) http_metrics: HttpMetricsHandle,
}

/// How long a sampled free-space reading is served from cache before the
/// collector stats the filesystems again. A scrape interval is typically
/// 15–60 s, and free space does not move meaningfully faster than this.
pub(crate) const DISK_SPACE_SAMPLE_TTL: Duration = Duration::from_secs(30);

impl ServerRuntime {
    /// Free/total capacity for the configured directory roles, TTL-cached.
    ///
    /// `build_router` consumes the runtime, so the exporter reads the same
    /// collector through the `Extension<Arc<DiskSpaceCollector>>` the router
    /// installs; this accessor is the equivalent for anything still holding the
    /// runtime itself.
    #[allow(dead_code, reason = "read by the Prometheus exporter")]
    pub(crate) fn disk_space_snapshot(&self) -> Vec<DiskSpaceSnapshot> {
        self.disk_space.sample(DISK_SPACE_SAMPLE_TTL)
    }

    /// Per-route HTTP request counters and latency. Also reachable from a
    /// handler as `Extension<HttpMetricsHandle>`.
    #[allow(dead_code, reason = "read by the Prometheus exporter")]
    pub(crate) fn http_metrics_snapshot(&self) -> HttpMetricsSnapshot {
        self.http_metrics.snapshot()
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

#[derive(Clone, Copy, Debug, Default)]
struct NotForAttachment;

impl Predicate for NotForAttachment {
    fn should_compress<B>(&self, response: &HttpResponse<B>) -> bool {
        !response
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.trim_start().starts_with("attachment"))
    }
}

fn compression_layer() -> CompressionLayer<impl Predicate> {
    CompressionLayer::new()
        .gzip(true)
        .deflate(true)
        .br(true)
        .zstd(true)
        .compress_when(DefaultPredicate::new().and(NotForAttachment))
}

fn internal_upload_err(e: impl std::fmt::Display) -> (axum::http::StatusCode, String) {
    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

fn cors_layer(
    security: &RuntimeSecurityConfig,
) -> Result<CorsLayer, Box<dyn std::error::Error + Send + Sync>> {
    if security.cors_allowed_origins.is_empty() {
        return Ok(CorsLayer::new());
    }

    let origins = security
        .cors_allowed_origins
        .iter()
        .map(|origin| HeaderValue::from_str(origin))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("x-api-key"),
        ])
        .allow_credentials(true))
}

pub async fn run_server(
    runtime: ServerRuntime,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let base_url = runtime.base_url.clone();
    let cors = cors_layer(&runtime.security)?;
    let host_security = runtime.security.clone();
    let app = routes::build_router(runtime)
        .layer(compression_layer())
        .layer(
            RequestDecompressionLayer::new()
                .gzip(true)
                .deflate(true)
                .br(true)
                .zstd(true),
        )
        .layer(cors);
    let app = routes::with_http_host_validation(app, host_security);

    info!(%addr, base_url = if base_url.is_empty() { "/" } else { &base_url }, "starting HTTP server");
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        format!("failed to bind to {addr}: {e} — is another process using this port?")
    })?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
