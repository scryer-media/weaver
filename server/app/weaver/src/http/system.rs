use std::net::SocketAddr;
use std::time::Duration;

use axum::Json;
use axum::extract::{ConnectInfo, Extension};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use weaver_server_api::auth::CallerIdentity;
use weaver_server_core::runtime::restart::RestartController;

/// How long the accepted response is given to reach the browser before the
/// process starts tearing itself down. The page polls for the server coming
/// back, so it has to learn the restart was accepted before it stops answering.
const RESTART_RESPONSE_GRACE: Duration = Duration::from_millis(500);

/// Restart Weaver. REST rather than GraphQL because this is a process-lifecycle
/// action and the first-run wizard — one of its two callers — is mounted
/// outside the GraphQL client.
pub(super) async fn restart_handler(
    Extension(request_auth): Extension<super::RequestAuthContext>,
    Extension(restart): Extension<RestartController>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
) -> Response {
    let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
    let caller = match super::auth::resolve_caller(
        &request_auth.db,
        &request_auth.auth_cache,
        &request_auth.api_key_cache,
        request_auth.session_token.0.as_str(),
        &request_auth.security,
        super::auth::BrowserSessionPolicy::TrustedPeer(peer),
        &headers,
    )
    .await
    {
        Ok(caller) => caller,
        Err(status) => return status.into_response(),
    };
    // Taking the server away from everyone else is an administrator's
    // decision: a read or control key drives downloads, not the process.
    if !caller.scope.is_admin() {
        return super::error_response(StatusCode::FORBIDDEN, "admin scope required");
    }

    // Settled here as well as in the UI: a deployment that must not exit —
    // a container without a restart policy would leave the operator with
    // nothing — is refused no matter who asks.
    let capability = restart.capability();
    if !capability.supported {
        let reason = capability
            .reason
            .unwrap_or_else(|| "this deployment cannot restart Weaver".to_string());
        tracing::warn!(reason, "refused a restart request");
        return super::error_response(StatusCode::CONFLICT, &reason);
    }

    tracing::info!(
        caller = caller_kind(&caller.identity),
        "restart requested through the API"
    );
    tokio::spawn(async move {
        tokio::time::sleep(RESTART_RESPONSE_GRACE).await;
        restart.request_restart();
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "ok": true, "restarting": true })),
    )
        .into_response()
}

fn caller_kind(identity: &CallerIdentity) -> &'static str {
    match identity {
        CallerIdentity::Local(_) => "local",
        CallerIdentity::Jwt(_) => "login",
        CallerIdentity::ApiKey(_) => "api_key",
    }
}
