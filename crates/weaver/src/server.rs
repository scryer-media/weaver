use std::net::SocketAddr;

use async_graphql::Data;
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::extract::{Extension, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use rust_embed::Embed;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::decompression::RequestDecompressionLayer;
use tracing::info;

use weaver_api::WeaverSchema;
use weaver_api::auth::{CallerScope, hash_api_key};
use weaver_state::Database;

#[derive(Embed)]
#[folder = "../../apps/weaver-web/dist/"]
struct FrontendAssets;

/// Resolve the caller scope from an optional API key header.
async fn resolve_scope(
    db: &Database,
    api_key_header: Option<&axum::http::HeaderValue>,
) -> Result<CallerScope, StatusCode> {
    let Some(hdr) = api_key_header else {
        return Ok(CallerScope::Local);
    };
    let raw_key = hdr.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    let key_hash = hash_api_key(raw_key);
    let db = db.clone();
    let db2 = db.clone();
    let row = tokio::task::spawn_blocking(move || db.lookup_api_key(&key_hash))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match row {
        Some(row) => {
            // Fire-and-forget last_used_at update.
            let id = row.id;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            tokio::task::spawn_blocking(move || {
                let _ = db2.touch_api_key_last_used(id, now);
            });
            match row.scope.as_str() {
                "admin" => Ok(CallerScope::Admin),
                _ => Ok(CallerScope::Integration),
            }
        }
        None => Err(StatusCode::UNAUTHORIZED),
    }
}

async fn graphql_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(db): Extension<Database>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Result<GraphQLResponse, StatusCode> {
    let scope = resolve_scope(&db, headers.get("x-api-key")).await?;
    let mut request = req.into_inner();
    request = request.data(scope);
    Ok(schema.execute(request).await.into())
}

async fn ws_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(db): Extension<Database>,
    headers: HeaderMap,
    protocol: GraphQLProtocol,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, StatusCode> {
    // Check X-Api-Key header on the WS upgrade request.
    let scope = resolve_scope(&db, headers.get("x-api-key")).await?;

    let mut data = Data::default();
    data.insert(scope);

    let resp = ws
        .protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |stream| {
            let ws = GraphQLWebSocket::new(stream, schema, protocol)
                .with_data(data)
                .on_connection_init(move |payload: serde_json::Value| async move {
                    // Allow connection_init payload to carry an api_key too
                    // for WS clients that can't set custom headers.
                    if let Some(key) = payload.get("api_key").and_then(|v| v.as_str()) {
                        let key_hash = hash_api_key(key);
                        let row = db.lookup_api_key(&key_hash).map_err(|e| {
                            async_graphql::Error::new(format!("auth lookup failed: {e}"))
                        })?;
                        match row {
                            Some(row) => {
                                let scope = match row.scope.as_str() {
                                    "admin" => CallerScope::Admin,
                                    _ => CallerScope::Integration,
                                };
                                let mut data = Data::default();
                                data.insert(scope);
                                Ok(data)
                            }
                            None => Err(async_graphql::Error::new("Invalid API key")),
                        }
                    } else {
                        Ok(Data::default())
                    }
                });
            ws.serve()
        });
    Ok(resp)
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Try the exact path first, then fall back to index.html for SPA routing.
    if let Some(file) = FrontendAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        (StatusCode::OK, [(header::CONTENT_TYPE, mime.as_ref().to_string())], file.data).into_response()
    } else if let Some(index) = FrontendAssets::get("index.html") {
        let mime = mime_guess::from_path("index.html").first_or_octet_stream();
        (StatusCode::OK, [(header::CONTENT_TYPE, mime.as_ref().to_string())], index.data).into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

pub async fn run_server(
    schema: WeaverSchema,
    db: Database,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        .route("/graphql", post(graphql_handler))
        .route("/graphql/ws", get(ws_handler))
        .fallback(get(static_handler))
        .layer(Extension(schema))
        .layer(Extension(db))
        .layer(CompressionLayer::new())
        .layer(RequestDecompressionLayer::new())
        .layer(CorsLayer::permissive());

    info!(%addr, "starting HTTP server");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
