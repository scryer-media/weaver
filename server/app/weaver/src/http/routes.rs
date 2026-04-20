use std::sync::Arc;

use axum::Router;
use axum::extract::Extension;
use axum::routing::{get, post};

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
        base_url,
    } = runtime;
    let base_url_ext = super::assets::BaseUrl(Arc::new(base_url.clone()));
    let session_token = super::SessionToken(Arc::new(generate_api_key()));
    let request_auth = super::RequestAuthContext {
        db: db.clone(),
        auth_cache: auth_cache.clone(),
        api_key_cache: api_key_cache.clone(),
        session_token: session_token.clone(),
    };

    let inner = Router::new()
        .route("/metrics", get(super::metrics::metrics_handler))
        .route("/graphql", post(super::graphql::graphql_handler))
        .route("/graphql/ws", get(super::graphql::ws_handler))
        .route(
            "/api/backup/status",
            get(super::backup::backup_status_handler),
        )
        .route(
            "/api/backup/export",
            post(super::backup::backup_export_handler),
        )
        .route(
            "/api/backup/inspect",
            post(super::backup::backup_inspect_handler),
        )
        .route(
            "/api/backup/restore",
            post(super::backup::backup_restore_handler),
        )
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
        .layer(Extension(request_auth))
        .layer(Extension(metrics_exporter))
        .layer(Extension(base_url_ext))
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
