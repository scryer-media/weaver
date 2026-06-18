use std::sync::Arc;

use axum::Router;
use axum::extract::Extension;
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
        metrics_exporter,
        config,
        base_url,
        security,
    } = runtime;
    let base_url_ext = super::assets::BaseUrl(Arc::new(base_url.clone()));
    let session_token = super::SessionToken(Arc::new(generate_api_key()));
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
    );
    let backup_upload_routes = Router::new()
        .route("/inspect", post(super::backup::backup_inspect_handler))
        .route("/restore", post(super::backup::backup_restore_handler))
        .route_layer(RequestBodyLimitLayer::new(backup_upload_limit));

    let inner = Router::new()
        .route("/metrics", get(super::metrics::metrics_handler))
        .route("/jsonrpc", post(super::nzbget::jsonrpc_handler))
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
