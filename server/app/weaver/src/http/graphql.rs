// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Data, Executor, futures_util::StreamExt};
use async_graphql_axum::GraphQLResponse;
use axum::extract::{
    ConnectInfo, Extension, WebSocketUpgrade,
    ws::{CloseFrame, Message, WebSocket},
};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use std::{net::SocketAddr, str::FromStr, time::Duration};
use tokio::sync::watch;
use weaver_server_core::security::HttpAuthority;

use weaver_server_api::WeaverSchema;
use weaver_server_core::auth::CallerScope;

const WEBSOCKET_AUTH_RECHECK_INTERVAL: Duration = Duration::from_secs(1);

/// The most subscriptions one GraphQL socket may hold live at once. A
/// subscribe past it is refused for that operation alone, with an `error`
/// message for its id; the socket and its other subscriptions stay up. A
/// subscription frees its place when its stream ends, when the client stops
/// it, or when the socket closes.
const MAX_LIVE_SUBSCRIPTIONS_PER_SOCKET: usize = 32;

/// The most GraphQL sockets one caller may hold open at once, counted per
/// credential: one API key, one login or browser session, or the one local
/// session every trusted-network browser shares when login is off. A socket
/// past it is closed with 4429 after `connection_init`, which is where its
/// credential becomes known.
const MAX_SOCKETS_PER_CALLER: usize = 8;

// Close code for a socket refused by a count limit.
const CLOSE_TOO_MANY: u16 = 4429;

fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// Open GraphQL sockets per caller. An entry is removed when its count reaches
// zero, so the map holds only callers with a socket open.
static OPEN_SOCKETS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<weaver_server_api::auth::CallerIdentity, usize>>,
> = std::sync::LazyLock::new(Default::default);

// One open socket's place in its caller's count, given back when dropped.
struct SocketPlace {
    identity: weaver_server_api::auth::CallerIdentity,
}

impl SocketPlace {
    fn claim(identity: &weaver_server_api::auth::CallerIdentity) -> Option<Self> {
        let mut open = lock(&OPEN_SOCKETS);
        let count = open.entry(identity.clone()).or_default();
        if *count >= MAX_SOCKETS_PER_CALLER {
            return None;
        }
        *count += 1;
        Some(Self {
            identity: identity.clone(),
        })
    }
}

impl Drop for SocketPlace {
    fn drop(&mut self) {
        let mut open = lock(&OPEN_SOCKETS);
        if let Some(count) = open.get_mut(&self.identity) {
            *count -= 1;
            if *count == 0 {
                open.remove(&self.identity);
            }
        }
    }
}

// The live subscriptions of one socket, by operation id. Each start of an id
// gets a new generation, so a stream replaced under a reused id cannot free
// the place of the one that replaced it.
#[derive(Default)]
struct LiveSubscriptions {
    ids: std::collections::HashMap<String, u64>,
    next_generation: u64,
}

// A live subscription's place on its socket. It travels in the request's
// data, which the subscription's stream holds for as long as it runs, so the
// place is given back whenever the stream is dropped: it ended by itself, it
// failed, the client stopped it, or the socket went away.
struct SubscriptionPlace {
    live: std::sync::Arc<std::sync::Mutex<LiveSubscriptions>>,
    id: String,
    generation: u64,
}

impl Drop for SubscriptionPlace {
    fn drop(&mut self) {
        let mut live = lock(&self.live);
        if live.ids.get(&self.id) == Some(&self.generation) {
            live.ids.remove(&self.id);
        }
    }
}

// Whether `request` runs a subscription. A request that does not parse, or
// does not name one operation, is left to the schema to refuse.
fn starts_subscription(request: &mut async_graphql::Request) -> bool {
    let name = request.operation_name.clone();
    let Ok(document) = request.parsed_query() else {
        return false;
    };
    let mut operations = document.operations.iter();
    let operation = match name.as_deref() {
        Some(name) => operations
            .find(|(candidate, _)| candidate.is_some_and(|candidate| candidate.as_str() == name)),
        None => match (operations.next(), operations.next()) {
            (Some(only), None) => Some(only),
            _ => None,
        },
    };
    operation.is_some_and(|(_, operation)| {
        operation.node.ty == async_graphql::parser::types::OperationType::Subscription
    })
}

// Counts a subscribe against the socket's live subscriptions. `false` when the
// socket is full and the operation must be refused.
fn admit_operation(
    live: &std::sync::Arc<std::sync::Mutex<LiveSubscriptions>>,
    id: &str,
    request: &mut async_graphql::Request,
) -> bool {
    let subscription = starts_subscription(request);
    let mut state = lock(live);
    if !subscription {
        // A query under a live id replaces that subscription's stream.
        state.ids.remove(id);
        return true;
    }
    if !state.ids.contains_key(id) && state.ids.len() >= MAX_LIVE_SUBSCRIPTIONS_PER_SOCKET {
        return false;
    }
    state.next_generation += 1;
    let generation = state.next_generation;
    state.ids.insert(id.to_owned(), generation);
    request.data.insert(SubscriptionPlace {
        live: live.clone(),
        id: id.to_owned(),
        generation,
    });
    true
}

// The protocol's `error` message refusing operation `id`.
fn too_many_subscriptions(protocol: async_graphql::http::WebSocketProtocols, id: &str) -> String {
    let error = serde_json::json!({
        "message": format!(
            "Too many live subscriptions on this connection (limit {MAX_LIVE_SUBSCRIPTIONS_PER_SOCKET})"
        ),
    });
    let payload = match protocol {
        async_graphql::http::WebSocketProtocols::GraphQLWS => serde_json::json!([error]),
        async_graphql::http::WebSocketProtocols::SubscriptionsTransportWS => error,
    };
    serde_json::json!({ "type": "error", "id": id, "payload": payload }).to_string()
}

#[derive(Clone)]
enum WebSocketCredential {
    Browser(HeaderMap),
    ApiKey(HeaderMap),
}

#[derive(Clone)]
struct WebSocketAuthorization {
    credential: WebSocketCredential,
    scope: CallerScope,
    identity: weaver_server_api::auth::CallerIdentity,
}

impl WebSocketAuthorization {
    async fn from_connection_init(
        request_auth: &super::RequestAuthContext,
        peer: Option<SocketAddr>,
        headers: &HeaderMap,
        payload: &serde_json::Value,
    ) -> Result<Self, async_graphql::Error> {
        let credential = if let Some(key) = connection_init_api_key(payload)? {
            // Credentials sent in connection_init are machine credentials.
            // Remove upgrade credentials so the payload cannot accidentally
            // inherit a browser session or a different API key.
            let mut api_headers = headers.clone();
            api_headers.remove(header::AUTHORIZATION);
            api_headers.remove("x-api-key");
            api_headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {key}"))
                    .map_err(|_| async_graphql::Error::new("Invalid authorization"))?,
            );
            WebSocketCredential::ApiKey(api_headers)
        } else {
            let mut browser_headers = headers.clone();
            if request_auth.security.authenticated_access_mode() {
                let csrf = payload
                    .get("csrf")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| async_graphql::Error::new("CSRF token required"))?;
                browser_headers.insert(
                    "x-weaver-csrf",
                    HeaderValue::from_str(csrf)
                        .map_err(|_| async_graphql::Error::new("Invalid CSRF token"))?,
                );
            }
            WebSocketCredential::Browser(browser_headers)
        };

        let caller = Self::resolve(request_auth, peer, &credential)
            .await
            .map_err(|_| async_graphql::Error::new("Browser session rejected"))?;
        Ok(Self {
            credential,
            scope: caller.scope,
            identity: caller.identity,
        })
    }

    async fn remains_active(
        &self,
        request_auth: &super::RequestAuthContext,
        peer: Option<SocketAddr>,
    ) -> bool {
        let Ok(caller) = Self::resolve(request_auth, peer, &self.credential).await else {
            return false;
        };
        caller.scope == self.scope && caller.identity == self.identity
    }

    async fn resolve(
        request_auth: &super::RequestAuthContext,
        peer: Option<SocketAddr>,
        credential: &WebSocketCredential,
    ) -> Result<super::auth::ResolvedCaller, StatusCode> {
        let headers = match credential {
            WebSocketCredential::Browser(headers) => {
                if request_auth.security.authenticated_access_mode() {
                    super::auth::validate_browser_csrf(
                        &request_auth.db,
                        &request_auth.security,
                        headers,
                    )
                    .await?;
                }
                headers
            }
            WebSocketCredential::ApiKey(headers) => headers,
        };
        super::auth::resolve_caller(
            &request_auth.db,
            &request_auth.auth_cache,
            &request_auth.api_key_cache,
            request_auth.session_token.0.as_str(),
            &request_auth.security,
            super::auth::BrowserSessionPolicy::TrustedPeer(peer),
            headers,
        )
        .await
    }

    fn data(&self, peer: Option<SocketAddr>, headers: &HeaderMap) -> Data {
        let mut data = Data::default();
        data.insert(self.scope);
        data.insert(self.identity.clone());
        data.insert(
            weaver_server_api::auth::types::NetworkRequestSecurityContext {
                peer,
                headers: headers.clone(),
            },
        );
        data
    }
}

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

// Stable codes the GraphQL endpoint sends as `code` beside `error` when it
// refuses a request before the schema runs. Machine clients match the code,
// never the sentence or the HTTP status alone. A released code is API: the
// sentence may be reworded, but a code is never renamed, reused or
// repurposed.
pub(super) mod refusal {
    // 401: an API key was presented (`Authorization: Bearer` or `X-API-Key`)
    // and refused: malformed, conflicting between the two headers, unknown,
    // or revoked.
    pub(in crate::http) const INVALID_API_KEY: &str = "INVALID_API_KEY";
    // 401: no API key was presented and no login or browser session the
    // endpoint accepts came with the request.
    pub(in crate::http) const AUTHENTICATION_REQUIRED: &str = "AUTHENTICATION_REQUIRED";
    // 403: a browser session came with the request but failed its origin or
    // CSRF binding.
    pub(in crate::http) const BROWSER_VERIFICATION_REQUIRED: &str = "BROWSER_VERIFICATION_REQUIRED";
    // 413: an authenticated request's body is larger than the endpoint takes,
    // by its declared length or by the bytes that arrived.
    pub(in crate::http) const REQUEST_TOO_LARGE: &str = "REQUEST_TOO_LARGE";

    #[cfg(test)]
    pub(in crate::http) const ALL: &[&str] = &[
        INVALID_API_KEY,
        AUTHENTICATION_REQUIRED,
        BROWSER_VERIFICATION_REQUIRED,
        REQUEST_TOO_LARGE,
    ];
}

// The body for an authentication or browser-binding refusal. Only the body
// changes: the status is the one the auth check chose, and any status that is
// not a refusal (a storage failure, say) keeps its bare response.
fn auth_refusal_response(status: StatusCode, headers: &HeaderMap) -> axum::response::Response {
    match status {
        StatusCode::UNAUTHORIZED
            if headers.contains_key(header::AUTHORIZATION) || headers.contains_key("x-api-key") =>
        {
            super::coded_error_response(status, refusal::INVALID_API_KEY, "invalid API key")
        }
        StatusCode::UNAUTHORIZED => super::coded_error_response(
            status,
            refusal::AUTHENTICATION_REQUIRED,
            "authentication required",
        ),
        StatusCode::FORBIDDEN => super::coded_error_response(
            status,
            refusal::BROWSER_VERIFICATION_REQUIRED,
            "browser verification required",
        ),
        other => other.into_response(),
    }
}

// Room in a request body for the JSON or multipart framing around its largest
// value.
const BODY_FRAMING_BYTES: u64 = 1024 * 1024;

// The largest request body the endpoint reads. An NZB at the upload limit has
// to fit either way it can arrive: as base64 text inside the JSON (four
// characters for every three bytes), or as a multipart file next to the
// operation.
pub(super) fn body_limit(security: &weaver_server_core::security::RuntimeSecurityConfig) -> u64 {
    security
        .nzb_upload_limit_bytes
        .div_ceil(3)
        .saturating_mul(4)
        .saturating_add(BODY_FRAMING_BYTES)
}

// The GraphQL request in `body`, read up to `limit` bytes. A declared length
// over the limit is refused before any of the body is read; a body that
// passes the limit as it arrives is cut off there.
async fn read_request(
    headers: &HeaderMap,
    body: axum::body::Body,
    limit: u64,
) -> Result<async_graphql::Request, axum::response::Response> {
    use async_graphql::futures_util::TryStreamExt;

    let too_large = || {
        super::coded_error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            refusal::REQUEST_TOO_LARGE,
            &format!("request body exceeds {limit} bytes"),
        )
    };
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared.is_some_and(|declared| declared > limit) {
        return Err(too_large());
    }
    let overflowed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut read = 0u64;
    let flag = overflowed.clone();
    let reader = body
        .into_data_stream()
        .map_err(std::io::Error::other)
        .and_then(move |chunk| {
            read = read.saturating_add(chunk.len() as u64);
            let past = read > limit;
            if past {
                flag.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            async move {
                if past {
                    Err(std::io::Error::other("request body too large"))
                } else {
                    Ok(chunk)
                }
            }
        })
        .into_async_read();
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    // Uploaded files are held in memory, so the body limit is also the
    // bound on what one request's files can hold.
    let parsed = async_graphql::http::receive_batch_body(
        content_type,
        reader,
        async_graphql::http::MultipartOptions::default(),
    )
    .await
    .and_then(async_graphql::BatchRequest::into_single);
    match parsed {
        Ok(request) => Ok(request),
        Err(_) if overflowed.load(std::sync::atomic::Ordering::Relaxed) => Err(too_large()),
        Err(error) => Err(async_graphql_axum::rejection::GraphQLRejection(error).into_response()),
    }
}

// The caller is resolved from the request's headers before any of its body
// is read, so a request that does not authenticate costs no body read.
pub(super) async fn graphql_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    req: axum::extract::Request,
) -> Result<GraphQLResponse, axum::response::Response> {
    let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
    let (parts, body) = req.into_parts();
    let headers = parts.headers;
    let resolved = match super::auth::resolve_script_run(&request_auth.db, &headers).await {
        Some(resolved) => resolved,
        None => super::auth::resolve_caller(
            &request_auth.db,
            &request_auth.auth_cache,
            &request_auth.api_key_cache,
            request_auth.session_token.0.as_str(),
            &request_auth.security,
            super::auth::BrowserSessionPolicy::TrustedPeer(peer),
            &headers,
        )
        .await
        .map_err(|status| auth_refusal_response(status, &headers))?,
    };
    if !matches!(
        resolved.identity,
        weaver_server_api::auth::CallerIdentity::ApiKey(_)
            | weaver_server_api::auth::CallerIdentity::ScriptRun(_)
    ) {
        super::auth::validate_browser_csrf(&request_auth.db, &request_auth.security, &headers)
            .await
            .map_err(|status| auth_refusal_response(status, &headers))?;
    }
    let mut request = read_request(&headers, body, body_limit(&request_auth.security)).await?;
    request = request
        .data(resolved.scope)
        .data(resolved.identity)
        .data(weaver_server_api::auth::types::NetworkRequestSecurityContext { peer, headers });
    Ok(schema.execute(request).await.into())
}

// Whether the page that opened this socket is this application.
//
// Browsers always send `Origin` on a socket upgrade and page scripts cannot
// forge it; a request without one is a machine client, which carries its own
// credential rather than riding on a browser's cookies.
fn upgrade_origin_allowed(
    auth: &super::RequestAuthContext,
    peer: Option<SocketAddr>,
    headers: &HeaderMap,
) -> bool {
    let mut origins = headers.get_all(header::ORIGIN).iter();
    let Some(origin) = origins.next() else {
        return true;
    };
    if origins.next().is_some() {
        return false;
    }
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    let request_host = headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
        .and_then(|host| HttpAuthority::parse(host).ok());
    let allowed =
        auth.security
            .is_websocket_origin_allowed(origin, request_host.as_ref(), peer, headers);
    if !allowed {
        tracing::warn!(
            origin,
            "refused a GraphQL socket opened from another origin; name this \
             server's public host in WEAVER_HTTP_ALLOWED_HOSTS if a proxy rewrites Host"
        );
    }
    allowed
}

// Everything that can take a credential away, subscribed before the upgrade
// is authenticated so no change can slip between the check and the watch.
struct RevocationWatch {
    login: watch::Receiver<()>,
    api_keys: watch::Receiver<()>,
    trust: watch::Receiver<()>,
}

impl RevocationWatch {
    fn subscribe(auth: &super::RequestAuthContext) -> Self {
        Self {
            login: auth.auth_cache.subscribe(),
            api_keys: auth.api_key_cache.subscribe(),
            trust: auth.security.subscribe_trust_changes(),
        }
    }

    async fn changed(&mut self) {
        tokio::select! {
            _ = self.login.changed() => {}
            _ = self.api_keys.changed() => {}
            _ = self.trust.changed() => {}
        }
    }
}

pub(super) async fn ws_handler<E: Executor>(
    Extension(schema): Extension<E>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<axum::response::Response, StatusCode> {
    let protocol = websocket_protocol(&headers)?;
    let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
    // Authenticated sessions bind an exact Origin and CSRF token at init.
    // Legacy browser sessions still need the upgrade Host/origin defense.
    if !request_auth.security.authenticated_access_mode()
        && !upgrade_origin_allowed(&request_auth, peer, &headers)
    {
        return Err(StatusCode::FORBIDDEN);
    }
    let changes = RevocationWatch::subscribe(&request_auth);
    Ok(ws
        .max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .protocols(async_graphql::http::ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| async move {
            serve_authenticated_websocket(
                socket,
                schema,
                request_auth,
                peer,
                headers,
                protocol,
                changes,
            )
            .await;
        })
        .into_response())
}

fn websocket_protocol(
    headers: &HeaderMap,
) -> Result<async_graphql::http::WebSocketProtocols, StatusCode> {
    headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .and_then(|protocols| {
            protocols.split(',').find_map(|protocol| {
                async_graphql::http::WebSocketProtocols::from_str(protocol.trim()).ok()
            })
        })
        .ok_or(StatusCode::BAD_REQUEST)
}

async fn serve_authenticated_websocket<E: Executor>(
    mut socket: WebSocket,
    schema: E,
    request_auth: super::RequestAuthContext,
    peer: Option<SocketAddr>,
    headers: HeaderMap,
    protocol: async_graphql::http::WebSocketProtocols,
    mut changes: RevocationWatch,
) {
    let initial = match tokio::time::timeout(
        Duration::from_secs(10),
        receive_graphql_message(&mut socket, &mut None),
    )
    .await
    {
        Ok(initial) => initial,
        Err(_) => {
            let _ = socket
                .send(Message::Close(Some(CloseFrame {
                    code: 4408,
                    reason: "Connection initialization timeout".into(),
                })))
                .await;
            return;
        }
    };
    let Some(initial) = initial else {
        return;
    };
    let Ok(initial) = initial else {
        close_unauthorized(&mut socket).await;
        return;
    };
    let async_graphql::http::ClientMessage::ConnectionInit { ref payload } = initial else {
        close_unauthorized(&mut socket).await;
        return;
    };
    let authorization = match WebSocketAuthorization::from_connection_init(
        &request_auth,
        peer,
        &headers,
        &payload.clone().unwrap_or_default(),
    )
    .await
    {
        Ok(authorization) => authorization,
        Err(_) => {
            if request_auth.security.authenticated_access_mode() {
                close_unauthorized(&mut socket).await;
            } else {
                // Preserve the legacy protocol's connection-init rejection.
                let _ = socket
                    .send(Message::Close(Some(CloseFrame {
                        code: 1002,
                        reason: "Invalid authorization".into(),
                    })))
                    .await;
            }
            return;
        }
    };
    // Held while the socket is served; every way out of this function gives
    // the place back.
    let Some(place) = SocketPlace::claim(&authorization.identity) else {
        let _ = socket
            .send(Message::Close(Some(CloseFrame {
                code: CLOSE_TOO_MANY,
                reason: format!(
                    "Too many open connections for this credential (limit {MAX_SOCKETS_PER_CALLER})"
                )
                .into(),
            })))
            .await;
        return;
    };
    let mut place = Some(place);
    let live = std::sync::Arc::new(std::sync::Mutex::new(LiveSubscriptions::default()));

    let (input_sender, input_receiver) = tokio::sync::mpsc::channel(32);
    let input = async_graphql::futures_util::stream::unfold(input_receiver, |mut receiver| async {
        receiver.recv().await.map(|message| (message, receiver))
    });
    let mut graphql = Box::pin(
        async_graphql::http::WebSocket::from_message_stream(schema, input, protocol)
            .connection_data(authorization.data(peer, &headers)),
    );
    if input_sender.try_send(Ok(initial)).is_err() {
        return;
    }

    let mut recheck = tokio::time::interval(WEBSOCKET_AUTH_RECHECK_INTERVAL);
    recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Consume the immediate tick; authentication has just completed above.
    recheck.tick().await;

    loop {
        tokio::select! {
            _ = changes.changed() => {
                if !authorization.remains_active(&request_auth, peer).await {
                    close_unauthorized(&mut socket).await;
                    return;
                }
            }
            _ = recheck.tick() => {
                if !authorization.remains_active(&request_auth, peer).await {
                    close_unauthorized(&mut socket).await;
                    return;
                }
            }
            message = receive_graphql_message(&mut socket, &mut place) => {
                let Some(mut message) = message else {
                    return;
                };
                if !authorization.remains_active(&request_auth, peer).await {
                    close_unauthorized(&mut socket).await;
                    return;
                }
                match &mut message {
                    Ok(async_graphql::http::ClientMessage::Start { id, payload }) => {
                        if !admit_operation(&live, id, payload) {
                            let refusal = too_many_subscriptions(protocol, id);
                            if socket.send(Message::Text(refusal.into())).await.is_err() {
                                return;
                            }
                            continue;
                        }
                    }
                    // The place is free from the moment the client stops
                    // the subscription, not when the protocol handler next
                    // runs, so a stop followed by a subscribe always fits.
                    Ok(async_graphql::http::ClientMessage::Stop { id }) => {
                        lock(&live).ids.remove(id.as_str());
                    }
                    _ => {}
                }
                if input_sender.try_send(message).is_err() {
                    let _ = socket.send(Message::Close(Some(CloseFrame {
                        code: 4429, reason: "Too many pending operations".into(),
                    }))).await;
                    return;
                }
            }
            response = graphql.next() => {
                let Some(response) = response else {
                    return;
                };
                if !authorization.remains_active(&request_auth, peer).await {
                    close_unauthorized(&mut socket).await;
                    return;
                }
                match response {
                    async_graphql::http::WsMessage::Text(text) => {
                        if socket.send(Message::Text(text.into())).await.is_err() {
                            return;
                        }
                    }
                    async_graphql::http::WsMessage::Close(code, reason) => {
                        let _ = socket.send(Message::Close(Some(CloseFrame {
                            code,
                            reason: reason.into(),
                        }))).await;
                        return;
                    }
                }
            }
        }
    }
}

// `place` is given back before a client's close is answered, so a client that
// has seen its socket closed can open the next one at once.
async fn receive_graphql_message(
    socket: &mut WebSocket,
    place: &mut Option<SocketPlace>,
) -> Option<serde_json::Result<async_graphql::http::ClientMessage>> {
    loop {
        let message = socket.recv().await?.ok()?;
        match message {
            Message::Text(text) => return Some(serde_json::from_str(&text)),
            Message::Binary(bytes) => return Some(serde_json::from_slice(&bytes)),
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).await.ok()?;
            }
            Message::Pong(_) => {}
            Message::Close(frame) => {
                drop(place.take());
                let _ = socket.send(Message::Close(frame)).await;
                return None;
            }
        }
    }
}

async fn close_unauthorized(socket: &mut WebSocket) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: 4403,
            reason: "Forbidden".into(),
        })))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use weaver_server_core::Database;
    use weaver_server_core::auth::{ApiKeyCache, BrowserSession, LoginAuthCache, hash_api_key};
    use weaver_server_core::security::RuntimeSecurityConfig;

    fn epoch_seconds() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn hash_to_hex(value: &str) -> String {
        hash_api_key(value)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn browser_headers(token: &str, csrf: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            format!("weaver_session={token}").parse().unwrap(),
        );
        headers.insert(header::ORIGIN, "https://media.test".parse().unwrap());
        headers.insert("x-weaver-csrf", csrf.parse().unwrap());
        headers
    }

    fn request_auth(
        db: Database,
        security: RuntimeSecurityConfig,
    ) -> super::super::RequestAuthContext {
        super::super::RequestAuthContext {
            db,
            auth_cache: LoginAuthCache::default(),
            api_key_cache: ApiKeyCache::default(),
            session_token: super::super::SessionToken(Arc::new("process-token".into())),
            security: Arc::new(security),
        }
    }

    fn browser_session(token: &str, csrf: &str, expires_at: i64) -> BrowserSession {
        BrowserSession {
            token_hash: hash_to_hex(token),
            csrf_verifier: hash_to_hex(csrf),
            origin: "https://media.test".into(),
            client_ip: None,
            remembered: false,
            created_at: epoch_seconds(),
            expires_at,
            revoked_at: None,
        }
    }

    #[tokio::test]
    async fn already_open_browser_subscription_loses_access_after_revocation() {
        let db = Database::open_in_memory().unwrap();
        let security = RuntimeSecurityConfig::default();
        security.apply_stored_access_policy_revision(None, None, false);
        let token = "subscription-token";
        let csrf = "subscription-csrf";
        db.create_browser_session(&browser_session(token, csrf, epoch_seconds() + 60))
            .unwrap();
        let request_auth = request_auth(db.clone(), security);
        let peer = Some("127.0.0.1:49152".parse().unwrap());
        let authorization = WebSocketAuthorization::from_connection_init(
            &request_auth,
            peer,
            &browser_headers(token, csrf),
            &serde_json::json!({ "csrf": csrf }),
        )
        .await
        .unwrap();

        assert!(authorization.remains_active(&request_auth, peer).await);
        db.revoke_browser_session(&hash_to_hex(token), epoch_seconds())
            .unwrap();
        assert!(
            !authorization.remains_active(&request_auth, peer).await,
            "the live transport check must close an existing subscription after revocation"
        );
    }

    #[tokio::test]
    async fn already_open_browser_subscription_loses_access_after_expiry() {
        let db = Database::open_in_memory().unwrap();
        let security = RuntimeSecurityConfig::default();
        security.apply_stored_access_policy_revision(None, None, false);
        let token = "expired-subscription-token";
        let csrf = "expired-subscription-csrf";
        let expires_at = epoch_seconds() + 3600;
        db.create_browser_session(&browser_session(token, csrf, expires_at))
            .unwrap();
        let request_auth = request_auth(db.clone(), security);
        let peer = Some("127.0.0.1:49152".parse().unwrap());
        let authorization = WebSocketAuthorization::from_connection_init(
            &request_auth,
            peer,
            &browser_headers(token, csrf),
            &serde_json::json!({ "csrf": csrf }),
        )
        .await
        .unwrap();
        assert!(
            authorization.remains_active(&request_auth, peer).await,
            "the subscription must be open before its expiry can close it"
        );

        // Expire the session under the open subscription by moving its expiry
        // into the past, not by waiting for the clock to reach it.
        db.set_browser_session_expiry(&hash_to_hex(token), epoch_seconds() - 1)
            .unwrap();
        assert!(
            !authorization.remains_active(&request_auth, peer).await,
            "the live transport check must close an existing subscription after expiry"
        );
    }
}
