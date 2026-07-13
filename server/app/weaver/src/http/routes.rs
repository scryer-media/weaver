use std::sync::Arc;

use axum::Router;
use axum::extract::{Extension, Request};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use tower_http::limit::RequestBodyLimitLayer;

use weaver_server_core::auth::generate_api_key;

pub(super) fn build_router(runtime: super::ServerRuntime) -> Router {
    let super::ServerRuntime {
        schema,
        handle,
        db,
        auth_cache,
        api_key_cache,
        backup,
        rss,
        watch_folder,
        metrics_exporter,
        config,
        base_url,
        security,
    } = runtime;
    let base_url_ext = super::assets::BaseUrl(Arc::new(base_url.clone()));
    let session_token = super::SessionToken(Arc::new(generate_api_key()));
    let login_limiter = super::auth::LoginRateLimiter::default();
    let backup_upload_limit =
        usize::try_from(security.backup_upload_limit_bytes).unwrap_or(usize::MAX);
    let request_auth = super::RequestAuthContext {
        db: db.clone(),
        auth_cache: auth_cache.clone(),
        api_key_cache: api_key_cache.clone(),
        session_token: session_token.clone(),
    };
    let nzbget_context = super::nzbget::NzbgetFacadeContext::new(
        db.clone(),
        handle.clone(),
        config,
        auth_cache.clone(),
        api_key_cache.clone(),
        session_token.clone(),
        rss,
        watch_folder,
    );
    let backup_upload_routes = Router::new()
        .route("/inspect", post(super::backup::backup_inspect_handler))
        .route("/restore", post(super::backup::backup_restore_handler))
        .route_layer(RequestBodyLimitLayer::new(backup_upload_limit));

    // NZBGet-compat RPC bodies carry whole NZBs as base64 (+33%) plus envelope
    // overhead; axum's 2 MiB default would reject ordinary season packs.
    let rpc_body_limit = usize::try_from(
        security
            .nzb_upload_limit_bytes
            .saturating_mul(3)
            .saturating_div(2),
    )
    .unwrap_or(usize::MAX);
    // `jsonrpc_handler`/`xmlrpc_handler` take `body: Bytes`, which axum fully
    // buffers (up to `rpc_body_limit`, as large as 384 MiB) as soon as that
    // extractor runs -- before the handler body itself gets a chance to
    // check auth. Without a gate here, an unauthenticated flood of requests
    // could each buffer up to the limit concurrently and exhaust memory.
    // `from_fn` runs as a Tower middleware wrapping the whole inner service,
    // so it executes before axum dispatches to the route's handler and its
    // extractors -- i.e. before any buffering happens:
    //   (a) no credential header at all -> reject with 401 immediately
    //       without calling `next`. A trivial unauth flood (no header) never
    //       buffers a body, and needs no DB lookup.
    //   (b) header present -> acquire a permit from a shared semaphore
    //       (capacity 6) held across the whole request, so at most 6
    //       requests buffer bodies concurrently.
    // The presence check must accept EVERY credential form the facade's real
    // authenticator (`resolve_caller`) honors, or it would 401 valid callers
    // before the handler runs: `authorization`/`x-authorization` (Basic, used
    // by Sonarr/Radarr/nzb360), `x-api-key`, and the `weaver_jwt`/
    // `weaver_session` cookies.
    // Residual: a client that sends a *present but bogus* credential still
    // takes a buffering slot before its credentials are actually checked,
    // but it is bounded to 6x `rpc_body_limit` total instead of unbounded --
    // an acceptable tradeoff to avoid a DB lookup on every single request.
    let rpc_buffer_gate = Arc::new(tokio::sync::Semaphore::new(6));
    let nzbget_rpc_routes = Router::new()
        .route("/jsonrpc", post(super::nzbget::jsonrpc_handler))
        .route("/xmlrpc", post(super::nzbget::xmlrpc_handler))
        .route_layer(axum::extract::DefaultBodyLimit::max(rpc_body_limit))
        .layer(middleware::from_fn(move |req: Request, next: Next| {
            let rpc_buffer_gate = Arc::clone(&rpc_buffer_gate);
            async move {
                let headers = req.headers();
                let has_credential = headers.contains_key(header::AUTHORIZATION)
                    || headers.contains_key("x-authorization")
                    || headers.contains_key("x-api-key")
                    || headers.contains_key(header::COOKIE);
                if !has_credential {
                    return StatusCode::UNAUTHORIZED.into_response();
                }
                let Ok(_permit) = rpc_buffer_gate.acquire().await else {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                };
                next.run(req).await
            }
        }));

    let inner = Router::new()
        .route("/metrics", get(super::metrics::metrics_handler))
        .merge(nzbget_rpc_routes)
        .route("/graphql", post(super::graphql::graphql_handler))
        .route("/graphql/ws", get(super::graphql::ws_handler))
        .route(
            "/api/jobs/{job_id}/nzb",
            get(super::jobs::job_nzb_download_handler),
        )
        .route(
            "/api/jobs/{job_id}/output-file",
            post(super::jobs::job_output_file_download_handler),
        )
        .route(
            "/api/backup/status",
            get(super::backup::backup_status_handler),
        )
        .route(
            "/api/backup/export",
            post(super::backup::backup_export_handler),
        )
        .nest("/api/backup", backup_upload_routes)
        .route("/api/login", post(super::auth::login_handler))
        .route("/api/logout", post(super::auth::logout_handler))
        .route("/api/auth/status", get(super::auth::auth_status_handler))
        .route("/", get(super::assets::static_handler))
        .fallback(get(super::assets::static_handler))
        .layer(Extension(handle))
        .layer(Extension(schema))
        .layer(Extension(backup))
        .layer(Extension(db))
        .layer(Extension(auth_cache))
        .layer(Extension(login_limiter))
        .layer(Extension(api_key_cache))
        .layer(Extension(nzbget_context))
        .layer(Extension(request_auth))
        .layer(Extension(metrics_exporter))
        .layer(Extension(base_url_ext))
        .layer(Extension(security))
        .layer(Extension(session_token));

    if base_url.is_empty() {
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
    }
}
