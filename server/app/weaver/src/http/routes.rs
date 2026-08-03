use std::sync::Arc;

use axum::Router;
use axum::extract::{Extension, Request};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use tower_http::limit::RequestBodyLimitLayer;

use weaver_server_core::auth::generate_api_key;
use weaver_server_core::security::{HttpAuthority, RuntimeSecurityConfig};

pub(super) const NZBGET_RPC_BODY_LIMIT_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostRejection {
    BadRequest,
}

fn request_authority(req: &Request) -> Result<HttpAuthority, HostRejection> {
    let uri_authority = req
        .uri()
        .authority()
        .map(|authority| HttpAuthority::parse(authority.as_str()))
        .transpose()
        .map_err(|_| HostRejection::BadRequest)?;

    let mut host_values = req.headers().get_all(header::HOST).iter();
    let host_authority = host_values
        .next()
        .map(|value| {
            value
                .to_str()
                .map_err(|_| HostRejection::BadRequest)
                .and_then(|value| {
                    HttpAuthority::parse(value).map_err(|_| HostRejection::BadRequest)
                })
        })
        .transpose()?;
    if host_values.next().is_some() {
        return Err(HostRejection::BadRequest);
    }

    match (uri_authority, host_authority) {
        (Some(uri), Some(host)) if !uri.matches(&host) => Err(HostRejection::BadRequest),
        (Some(authority), _) | (_, Some(authority)) => Ok(authority),
        (None, None) => Err(HostRejection::BadRequest),
    }
}

async fn enforce_http_host(security: &RuntimeSecurityConfig, req: Request, next: Next) -> Response {
    match request_authority(&req) {
        Ok(authority) if security.is_http_authority_allowed(&authority) => next.run(req).await,
        Ok(_) => (
            StatusCode::MISDIRECTED_REQUEST,
            "request Host is not allowed",
        )
            .into_response(),
        Err(HostRejection::BadRequest) => {
            (StatusCode::BAD_REQUEST, "invalid Host header").into_response()
        }
    }
}

pub(super) fn build_router(runtime: super::ServerRuntime) -> Router {
    let super::ServerRuntime {
        schema,
        handle,
        scheduled_resume,
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
    let backup_request_limit = backup_upload_limit
        .saturating_add(super::backup::BACKUP_MULTIPART_ENVELOPE_ALLOWANCE_BYTES);
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
        scheduled_resume,
    );
    let backup_upload_routes = Router::new()
        .route("/inspect", post(super::backup::backup_inspect_handler))
        .route("/restore", post(super::backup::backup_restore_handler))
        .route_layer(RequestBodyLimitLayer::new(backup_request_limit));

    let nzbget_rpc_routes = build_nzbget_rpc_routes(nzbget_context);

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

pub(super) fn with_http_host_validation(router: Router, security: RuntimeSecurityConfig) -> Router {
    let host_security = Arc::new(security);
    router.layer(middleware::from_fn(move |req, next| {
        let security = Arc::clone(&host_security);
        async move { enforce_http_host(&security, req, next).await }
    }))
}

pub(super) fn build_nzbget_rpc_routes(
    nzbget_context: super::nzbget::NzbgetFacadeContext,
) -> Router {
    // 32 MiB of JSON/XML envelope carries roughly 24 MiB of decoded base64 NZB
    // data, comfortably above observed real-world payloads while keeping the
    // fully-buffered RPC surface bounded independently of the core upload cap.
    let rpc_buffer_gate = Arc::new(tokio::sync::Semaphore::new(6));
    let nzbget_auth_context = nzbget_context.clone();
    Router::new()
        .route("/jsonrpc", post(super::nzbget::jsonrpc_handler))
        .route("/xmlrpc", post(super::nzbget::xmlrpc_handler))
        .route_layer(axum::extract::DefaultBodyLimit::max(
            NZBGET_RPC_BODY_LIMIT_BYTES,
        ))
        .layer(middleware::from_fn(move |mut req: Request, next: Next| {
            let rpc_buffer_gate = Arc::clone(&rpc_buffer_gate);
            let nzbget_auth_context = nzbget_auth_context.clone();
            async move {
                let scope = match super::nzbget::resolve_scope_for_facade(
                    &nzbget_auth_context,
                    req.headers(),
                )
                .await
                {
                    Ok(scope) => scope,
                    Err(status) => {
                        return super::nzbget::authentication_error_response(
                            req.uri().path(),
                            status,
                        );
                    }
                };
                req.extensions_mut()
                    .insert(super::nzbget::NzbgetCallerScope(scope));
                let Ok(_permit) = rpc_buffer_gate.acquire().await else {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                };
                next.run(req).await
            }
        }))
        .layer(Extension(nzbget_context))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::body::Body;
    use axum::http::{Method, Request};
    use tower::ServiceExt;

    use super::*;

    fn guarded_router(security: RuntimeSecurityConfig, hits: Arc<AtomicUsize>) -> Router {
        let router = Router::new().fallback(move || {
            let hits = Arc::clone(&hits);
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                let mut response = StatusCode::OK.into_response();
                response.headers_mut().insert(
                    header::SET_COOKIE,
                    "weaver_session=local-admin"
                        .parse()
                        .expect("valid test cookie"),
                );
                response
            }
        });
        with_http_host_validation(router, security)
    }

    #[tokio::test]
    async fn disallowed_host_is_rejected_before_every_route_surface() {
        let hits = Arc::new(AtomicUsize::new(0));
        let app = guarded_router(RuntimeSecurityConfig::default(), Arc::clone(&hits));

        for (method, path) in [
            (Method::GET, "/"),
            (Method::GET, "/weaver/"),
            (Method::POST, "/graphql"),
            (Method::GET, "/graphql/ws"),
            (Method::POST, "/jsonrpc"),
            (Method::POST, "/api/backup/restore"),
            (Method::GET, "/metrics"),
        ] {
            let request = Request::builder()
                .method(method)
                .uri(path)
                .header(header::HOST, "attacker.example.test")
                .body(Body::from(vec![0u8; 16 * 1024]))
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();

            assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST, "{path}");
            assert!(
                response.headers().get(header::SET_COOKIE).is_none(),
                "{path}"
            );
        }

        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn host_guard_accepts_local_and_configured_authorities() {
        let security = RuntimeSecurityConfig {
            http_allowed_hosts: vec![HttpAuthority::parse("weaver.internal:8443").unwrap()],
            ..RuntimeSecurityConfig::default()
        };
        let hits = Arc::new(AtomicUsize::new(0));
        let app = guarded_router(security, Arc::clone(&hits));

        for host in [
            "localhost:9090",
            "127.0.0.1:9090",
            "[::1]:9090",
            "WEAVER.INTERNAL.:8443",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/")
                        .header(header::HOST, host)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{host}");
        }

        assert_eq!(hits.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn host_guard_rejects_missing_duplicate_malformed_and_conflicting_authorities() {
        let app = guarded_router(
            RuntimeSecurityConfig::default(),
            Arc::new(AtomicUsize::new(0)),
        );

        let missing = Request::builder().uri("/").body(Body::empty()).unwrap();
        assert_eq!(
            app.clone().oneshot(missing).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );

        let malformed = Request::builder()
            .uri("/")
            .header(header::HOST, "https://localhost")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(malformed).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );

        let mut duplicate = Request::builder()
            .uri("/")
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .unwrap();
        duplicate
            .headers_mut()
            .append(header::HOST, "localhost".parse().unwrap());
        assert_eq!(
            app.clone().oneshot(duplicate).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );

        let conflicting = Request::builder()
            .uri("http://localhost/")
            .header(header::HOST, "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(conflicting).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );

        let forwarded_host = Request::builder()
            .uri("/")
            .header(header::HOST, "attacker.example.test")
            .header("x-forwarded-host", "localhost")
            .header("forwarded", "host=localhost")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(forwarded_host).await.unwrap().status(),
            StatusCode::MISDIRECTED_REQUEST
        );
    }
}
