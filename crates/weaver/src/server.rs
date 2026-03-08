use std::net::SocketAddr;

use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::extract::Extension;
use axum::http::{StatusCode, Uri, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use rust_embed::Embed;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::decompression::RequestDecompressionLayer;
use tracing::info;

use weaver_api::WeaverSchema;

#[derive(Embed)]
#[folder = "../../apps/weaver-web/dist/"]
struct FrontendAssets;

async fn graphql_handler(
    Extension(schema): Extension<WeaverSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
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
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        .route("/graphql", post(graphql_handler))
        .route_service("/graphql/ws", GraphQLSubscription::new(schema.clone()))
        .fallback(get(static_handler))
        .layer(Extension(schema))
        .layer(CompressionLayer::new())
        .layer(RequestDecompressionLayer::new())
        .layer(CorsLayer::permissive());

    info!(%addr, "starting HTTP server");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
