mod assets;
mod auth;
mod backup;
mod diagnostics;
mod graphql;
mod health;
mod jobs;
mod metrics;
mod nzbget;
mod request_metrics;
mod routes;
mod setup_code;
mod system;
mod upgrade_splash;

use std::sync::Arc;

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
use weaver_server_core::operations::disk::StorageCapacity;
use weaver_server_core::operations::instrumentation::{DiskSpaceSnapshot, HttpMetricsSnapshot};
use weaver_server_core::security::RuntimeSecurityConfig;
use weaver_server_core::settings::model::SharedConfig;

pub(crate) use self::metrics::PrometheusMetricsExporter;
pub(crate) use self::request_metrics::HttpMetricsHandle;
pub(crate) use self::upgrade_splash::UpgradeSplash;

#[derive(Clone)]
struct SessionToken(Arc<String>);

#[derive(Clone)]
struct RequestAuthContext {
    db: Database,
    auth_cache: LoginAuthCache,
    api_key_cache: ApiKeyCache,
    session_token: SessionToken,
    security: Arc<RuntimeSecurityConfig>,
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
    /// Handle the restart endpoint pulls to ask the serve loop to tear down
    /// and start again.
    pub restart: weaver_server_core::runtime::restart::RestartController,
    /// The pipeline's background free-space samplers for the data,
    /// intermediate and complete roots. The exporter and the NZBGet status
    /// read their cached readings; nothing here stats a filesystem.
    pub(crate) disk_space: Arc<StorageCapacity>,
    /// Per-route HTTP request counters and latency, written by the
    /// `request_metrics` middleware and read by the exporter.
    pub(crate) http_metrics: HttpMetricsHandle,
}

impl ServerRuntime {
    /// Free/total capacity for the configured directory roles, from the
    /// samplers' last readings.
    ///
    /// `build_router` consumes the runtime, so the exporter reads the same
    /// samplers through the `Extension<Arc<StorageCapacity>>` the router
    /// installs; this accessor is the equivalent for anything still holding the
    /// runtime itself.
    #[allow(dead_code, reason = "read by the Prometheus exporter")]
    pub(crate) fn disk_space_snapshot(&self) -> Vec<DiskSpaceSnapshot> {
        self.disk_space.snapshots()
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
    base_url: &str,
) -> Result<CorsLayer, Box<dyn std::error::Error + Send + Sync>> {
    let origins = security
        .cors_allowed_origins
        .iter()
        .map(|origin| HeaderValue::from_str(origin))
        .collect::<Result<Vec<_>, _>>()?;
    let credential_origins = origins.clone();
    let rpc_paths = [format!("{base_url}/jsonrpc"), format!("{base_url}/xmlrpc")];

    Ok(CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, request| {
            origins.contains(origin)
                || (rpc_paths.iter().any(|path| path == request.uri.path())
                    && browser_extension_origin(origin))
        }))
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("x-api-key"),
        ])
        // Extension RPC clients supply an explicit API key. Browser cookies
        // remain confined to the operator's configured web origins.
        .allow_credentials(tower_http::cors::AllowCredentials::predicate(
            move |origin, _| credential_origins.contains(origin),
        )))
}

fn browser_extension_origin(origin: &HeaderValue) -> bool {
    let Some(url) = origin
        .to_str()
        .ok()
        .and_then(|origin| reqwest::Url::parse(origin).ok())
    else {
        return false;
    };
    matches!(
        url.scheme(),
        "chrome-extension" | "moz-extension" | "safari-web-extension"
    ) && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.port().is_none()
        && url.path().is_empty()
        && url.query().is_none()
        && url.fragment().is_none()
}

/// Runs the HTTP server on a listener the caller already bound. Binding
/// happens in `serve.rs` so an unbindable configured address can fall back to
/// loopback (and be reported) before the security snapshot is captured by the
/// GraphQL schema — the never-brick rule for stored network settings.
pub async fn run_server(
    runtime: ServerRuntime,
    listener: tokio::net::TcpListener,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let base_url = runtime.base_url.clone();
    let cors = cors_layer(&runtime.security, &base_url)?;
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
    let app = routes::with_response_hardening(app);

    let addr = listener.local_addr()?;
    info!(%addr, base_url = if base_url.is_empty() { "/" } else { &base_url }, "starting HTTP server");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
