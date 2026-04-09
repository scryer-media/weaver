use async_graphql::Data;
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::extract::{Extension, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use weaver_server_api::WeaverSchema;
use weaver_server_core::Database;
use weaver_server_core::auth::{CallerScope, LoginAuthCache, hash_api_key};

pub(super) async fn graphql_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Result<GraphQLResponse, StatusCode> {
    let scope = super::auth::resolve_scope(&db, &auth_cache, &session_token, &headers).await?;
    let mut request = req.into_inner();
    request = request.data(scope);
    Ok(schema.execute(request).await.into())
}

pub(super) async fn ws_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    headers: HeaderMap,
    protocol: GraphQLProtocol,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    // Pre-resolve scope from cookies on the upgrade request. Browsers
    // automatically send cookies on WebSocket upgrade, so JWT auth works
    // without needing api_key in connection_init.
    let upgrade_scope = super::auth::resolve_scope(&db, &auth_cache, &session_token, &headers)
        .await
        .ok();

    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |stream| {
            let ws = GraphQLWebSocket::new(stream, schema, protocol).on_connection_init(
                move |payload: serde_json::Value| async move {
                    // If the upgrade request was already authenticated (via cookie),
                    // use that scope directly.
                    if let Some(scope) = upgrade_scope {
                        let mut data = Data::default();
                        data.insert(scope);
                        return Ok(data);
                    }

                    // Fall back to connection_init payload.
                    let presented = payload
                        .get("authorization")
                        .and_then(|value| value.as_str())
                        .and_then(|value| value.strip_prefix("Bearer ").map(str::trim))
                        .or_else(|| payload.get("api_key").and_then(|value| value.as_str()));
                    let Some(key) = presented else {
                        return Err(async_graphql::Error::new(
                            "Missing authorization or api_key in connection_init",
                        ));
                    };
                    if key == session_token.as_str() {
                        let mut data = Data::default();
                        data.insert(CallerScope::Local);
                        return Ok(data);
                    }
                    let key_hash = hash_api_key(key);
                    let row = db.lookup_api_key(&key_hash).map_err(|error| {
                        async_graphql::Error::new(format!("auth lookup failed: {error}"))
                    })?;
                    match row {
                        Some(row) => {
                            let scope = match row.scope.as_str() {
                                "admin" => CallerScope::Admin,
                                "read" => CallerScope::Read,
                                "control" | "integration" => CallerScope::Control,
                                _ => CallerScope::Control,
                            };
                            let mut data = Data::default();
                            data.insert(scope);
                            Ok(data)
                        }
                        None => Err(async_graphql::Error::new("Invalid API key")),
                    }
                },
            );
            ws.serve()
        })
}
