// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The built-in MCP endpoint: Streamable HTTP, JSON responses only, no
// server-side sessions.
//
// Every request is admitted here before rmcp sees it: the endpoint must be
// turned on (it is by default; Settings > Security, or `WEAVER_MCP_ENABLED`),
// the browser Origin must be one the GraphQL socket would accept, and the
// caller must present an API key. Tools then run operations from the operator skills' catalog
// through the same GraphQL schema `/graphql` serves, with that key's scope,
// so guards and refusals match `/graphql` exactly. Nothing here dials,
// binds, listens or resolves a name.
//
// rmcp's Host allow-list is off: the router's own Host check
// (`WEAVER_HTTP_ALLOWED_HOSTS`) has already run, and LAN and reverse-proxy
// deployments reach Weaver under any name it admits. The Origin check does
// not stop DNS rebinding: a rebound page sends an Origin equal to the Host
// it reached, and that passes. What stops it is the API key: a browser page
// cannot attach a bearer token it does not hold. A Host allow-list must come
// back before any caller other than an API key is admitted here.

mod catalog;
mod confirm;
mod schema_index;
mod server;
mod tools;

#[cfg(test)]
mod operation_contract;
#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{ConnectInfo, Extension};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use weaver_server_api::WeaverSchema;
use weaver_server_api::auth::CallerIdentity;
use weaver_server_core::auth::CallerScope;

use super::graphql::refusal;
use catalog::Catalog;
use server::WeaverMcpServer;

// MCP requests are small JSON-RPC envelopes.
pub(crate) const MCP_MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024;

// The caller admission resolved, stored on the request for the tools.
#[derive(Clone, Debug)]
pub(crate) struct McpCaller {
    pub scope: CallerScope,
    // The API key's digest, as `CallerIdentity::ApiKey` carries it.
    pub key: [u8; 32],
    pub peer: Option<SocketAddr>,
}

#[derive(Clone)]
pub(crate) struct McpState {
    service: Arc<StreamableHttpService<WeaverMcpServer, NeverSessionManager>>,
}

impl McpState {
    pub(crate) fn new(schema: WeaverSchema) -> Self {
        // The catalog is parsed once, here at start-up, not on a request.
        Self::with_catalog(schema, &catalog::CATALOG)
    }

    pub(crate) fn with_catalog(schema: WeaverSchema, catalog: &'static Catalog) -> Self {
        let confirmations = Arc::new(confirm::Confirmations::default());
        // Stateless mode answers every request, `initialize` included, on its
        // own; nothing is kept between requests. Host checks are off; see the
        // module notes for why that is safe only while every caller is an
        // API key.
        let config = StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true)
            .with_sse_keep_alive(None)
            .disable_allowed_hosts()
            .with_max_request_body_bytes(MCP_MAX_REQUEST_BODY_BYTES);
        let service = StreamableHttpService::new(
            move || {
                Ok(WeaverMcpServer::new(
                    schema.clone(),
                    catalog,
                    confirmations.clone(),
                ))
            },
            Arc::new(NeverSessionManager::default()),
            config,
        );
        Self {
            service: Arc::new(service),
        }
    }
}

// The caller is resolved from the request's headers before any of its body
// is read, so a request that does not authenticate costs no body read.
pub(super) async fn mcp_handler(
    Extension(state): Extension<McpState>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    request: Request<Body>,
) -> Response {
    // The operator's switch comes first: a turned-off endpoint does no other
    // work and reads nothing of the request. In-memory state, no query.
    if !request_auth.security.mcp_enabled() {
        return refusal_response(
            StatusCode::FORBIDDEN,
            refusal::MCP_DISABLED,
            "the Weaver MCP endpoint is turned off in Settings > Security",
        );
    }
    let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
    let headers = request.headers();
    if !super::graphql::upgrade_origin_allowed(
        super::graphql::OriginSurface::Mcp,
        &request_auth,
        peer,
        headers,
    ) {
        return refusal_response(
            StatusCode::FORBIDDEN,
            refusal::MCP_ORIGIN_NOT_ALLOWED,
            "this Origin may not call the Weaver MCP endpoint",
        );
    }
    let resolved = match super::auth::resolve_caller(
        &request_auth.db,
        &request_auth.auth_cache,
        &request_auth.api_key_cache,
        request_auth.session_token.0.as_str(),
        &request_auth.security,
        super::auth::BrowserSessionPolicy::TrustedPeer(peer),
        headers,
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(status) => return caller_refusal(status, headers),
    };
    let CallerIdentity::ApiKey(key) = resolved.identity else {
        return api_key_required(false);
    };
    let caller = McpCaller {
        scope: resolved.scope,
        key,
        peer,
    };

    let (mut parts, body) = request.into_parts();
    let bytes = match read_body(&parts.headers, body, MCP_MAX_REQUEST_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(BodyRefusal::TooLarge) => {
            return refusal_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                refusal::MCP_REQUEST_TOO_LARGE,
                "the MCP request body is too large",
            );
        }
        Err(BodyRefusal::Unreadable) => {
            return refusal_response(
                StatusCode::BAD_REQUEST,
                refusal::MCP_REQUEST_BODY_UNREADABLE,
                "the MCP request body could not be read",
            );
        }
    };
    parts.extensions.insert(caller);
    let request = Request::from_parts(parts, Body::from(bytes));

    let mut response = state.service.handle(request).await.map(Body::new);
    // Tool results belong to one API key.
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

enum BodyRefusal {
    TooLarge,
    Unreadable,
}

// Reads at most `limit` bytes of `body`. A declared length past the limit
// is refused before any read, and a stream stops at the first frame that
// passes it, so an oversized body is never buffered whole.
async fn read_body(
    headers: &HeaderMap,
    body: Body,
    limit: usize,
) -> Result<axum::body::Bytes, BodyRefusal> {
    use async_graphql::futures_util::StreamExt as _;
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared.is_some_and(|length| length > limit as u64) {
        return Err(BodyRefusal::TooLarge);
    }
    let mut stream = body.into_data_stream();
    let mut buffer = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BodyRefusal::Unreadable)?;
        if buffer.len() + chunk.len() > limit {
            return Err(BodyRefusal::TooLarge);
        }
        buffer.extend_from_slice(&chunk);
    }
    Ok(buffer.into())
}

// `resolve_caller`'s refusal as an MCP refusal. It answers 401 both for no
// credential and for a bad one, so the two are told apart by whether a key
// header was sent, as `/graphql` does. A 403 is a browser session refused
// for its origin: not an API key either way.
fn caller_refusal(status: StatusCode, headers: &HeaderMap) -> Response {
    match status {
        StatusCode::UNAUTHORIZED => api_key_required(
            headers.contains_key(header::AUTHORIZATION) || headers.contains_key("x-api-key"),
        ),
        StatusCode::FORBIDDEN => api_key_required(false),
        other => other.into_response(),
    }
}

// RFC 6750: a bare challenge when no API key was offered, an error code when
// the offered credential is malformed or not accepted.
fn api_key_required(offered: bool) -> Response {
    let (code, message, challenge) = if offered {
        (
            refusal::MCP_API_KEY_INVALID,
            "the API key is invalid or no longer authorized",
            "Bearer error=\"invalid_token\"",
        )
    } else {
        (
            refusal::MCP_API_KEY_REQUIRED,
            "the Weaver MCP endpoint accepts only an API key, as a bearer token or X-API-Key",
            "Bearer",
        )
    };
    let mut response = refusal_response(StatusCode::UNAUTHORIZED, code, message);
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static(challenge),
    );
    response
}

fn refusal_response(status: StatusCode, code: &str, message: &str) -> Response {
    let mut response = super::coded_error_response(status, code, message);
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
