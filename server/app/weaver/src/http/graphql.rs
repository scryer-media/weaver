use async_graphql::Data;
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::extract::{ConnectInfo, Extension, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use std::net::SocketAddr;

use weaver_server_api::WeaverSchema;
use weaver_server_api::auth::CallerIdentity;
use weaver_server_core::auth::hash_api_key;

fn connection_init_api_key(
    payload: &serde_json::Value,
) -> Result<Option<&str>, async_graphql::Error> {
    let authorization = match payload.get("authorization") {
        Some(value) => Some(
            value
                .as_str()
                .and_then(|value| value.strip_prefix("Bearer "))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| async_graphql::Error::new("Invalid authorization"))?,
        ),
        None => None,
    };
    let api_key = match payload.get("api_key") {
        Some(value) => Some(
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| async_graphql::Error::new("Invalid api_key"))?,
        ),
        None => None,
    };
    match (authorization, api_key) {
        (Some(authorization), Some(api_key)) if authorization != api_key => {
            Err(async_graphql::Error::new("Conflicting API key credentials"))
        }
        (Some(key), _) | (_, Some(key)) => Ok(Some(key)),
        (None, None) => Ok(None),
    }
}

pub(super) async fn graphql_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Result<GraphQLResponse, StatusCode> {
    let resolved = super::auth::resolve_caller(
        &request_auth.db,
        &request_auth.auth_cache,
        &request_auth.api_key_cache,
        request_auth.session_token.0.as_str(),
        &request_auth.security,
        super::auth::BrowserSessionPolicy::TrustedPeer(
            peer.map(|Extension(ConnectInfo(peer))| peer),
        ),
        &headers,
    )
    .await?;
    let mut request = req.into_inner();
    request = request.data(resolved.scope).data(resolved.identity);
    Ok(schema.execute(request).await.into())
}

pub(super) async fn ws_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    protocol: GraphQLProtocol,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    // Pre-resolve scope from cookies on the upgrade request. Browsers
    // automatically send cookies on WebSocket upgrade, so JWT auth works
    // without needing api_key in connection_init.
    let upgrade_caller = super::auth::resolve_caller(
        &request_auth.db,
        &request_auth.auth_cache,
        &request_auth.api_key_cache,
        request_auth.session_token.0.as_str(),
        &request_auth.security,
        super::auth::BrowserSessionPolicy::TrustedPeer(
            peer.map(|Extension(ConnectInfo(peer))| peer),
        ),
        &headers,
    )
    .await
    .ok();

    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |stream| {
            let request_auth = request_auth.clone();
            let ws = GraphQLWebSocket::new(stream, schema, protocol).on_connection_init(
                move |payload: serde_json::Value| async move {
                    // An explicit connection_init credential always wins over
                    // upgrade cookies, and is always a persistent API key.
                    if let Some(key) = connection_init_api_key(&payload)? {
                        let key_hash = hash_api_key(key);
                        let row = super::auth::lookup_api_key_auth(
                            &request_auth.db,
                            &request_auth.api_key_cache,
                            key_hash,
                        )
                        .await
                        .map_err(|status| {
                            async_graphql::Error::new(format!("auth lookup failed: {status}"))
                        })?
                        .ok_or_else(|| async_graphql::Error::new("Invalid API key"))?;
                        super::auth::queue_touch_api_key_last_used(&request_auth.db, row.id);
                        let mut data = Data::default();
                        data.insert(super::auth::caller_scope_from_api_key_scope(&row.scope));
                        data.insert(CallerIdentity::ApiKey(row.key_hash));
                        return Ok(data);
                    }

                    if let Some(caller) = upgrade_caller.clone() {
                        let mut data = Data::default();
                        data.insert(caller.scope);
                        data.insert(caller.identity);
                        return Ok(data);
                    }

                    Err(async_graphql::Error::new(
                        "Missing authorization or api_key in connection_init",
                    ))
                },
            );
            ws.serve()
        })
}
