use async_graphql::{Data, futures_util::StreamExt};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::{
    ConnectInfo, Extension, WebSocketUpgrade,
    ws::{CloseFrame, Message, WebSocket},
};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use std::{net::SocketAddr, str::FromStr, time::Duration};

use weaver_server_api::WeaverSchema;
use weaver_server_core::auth::CallerScope;

const WEBSOCKET_AUTH_RECHECK_INTERVAL: Duration = Duration::from_secs(1);

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

pub(super) async fn graphql_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Result<GraphQLResponse, StatusCode> {
    let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
    let resolved = super::auth::resolve_caller(
        &request_auth.db,
        &request_auth.auth_cache,
        &request_auth.api_key_cache,
        request_auth.session_token.0.as_str(),
        &request_auth.security,
        super::auth::BrowserSessionPolicy::TrustedPeer(peer),
        &headers,
    )
    .await?;
    if !matches!(
        resolved.identity,
        weaver_server_api::auth::CallerIdentity::ApiKey(_)
    ) {
        super::auth::validate_browser_csrf(&request_auth.db, &request_auth.security, &headers)
            .await?;
    }
    let mut request = req.into_inner();
    request = request
        .data(resolved.scope)
        .data(resolved.identity)
        .data(weaver_server_api::auth::types::NetworkRequestSecurityContext { peer, headers });
    Ok(schema.execute(request).await.into())
}

pub(super) async fn ws_handler(
    Extension(schema): Extension<WeaverSchema>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<axum::response::Response, StatusCode> {
    let protocol = websocket_protocol(&headers)?;
    let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
    Ok(ws
        .max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .protocols(async_graphql::http::ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| async move {
            serve_authenticated_websocket(socket, schema, request_auth, peer, headers, protocol)
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

async fn serve_authenticated_websocket(
    mut socket: WebSocket,
    schema: WeaverSchema,
    request_auth: super::RequestAuthContext,
    peer: Option<SocketAddr>,
    headers: HeaderMap,
    protocol: async_graphql::http::WebSocketProtocols,
) {
    let initial = match tokio::time::timeout(
        Duration::from_secs(10),
        receive_graphql_message(&mut socket),
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
            close_unauthorized(&mut socket).await;
            return;
        }
    };

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
            _ = recheck.tick() => {
                if !authorization.remains_active(&request_auth, peer).await {
                    close_unauthorized(&mut socket).await;
                    return;
                }
            }
            message = receive_graphql_message(&mut socket) => {
                let Some(message) = message else {
                    return;
                };
                if !authorization.remains_active(&request_auth, peer).await {
                    close_unauthorized(&mut socket).await;
                    return;
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

async fn receive_graphql_message(
    socket: &mut WebSocket,
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
            reason: "Unauthorized".into(),
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
        db.create_browser_session(&browser_session(token, csrf, epoch_seconds() + 1))
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

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            !authorization.remains_active(&request_auth, peer).await,
            "the live transport check must close an existing subscription after expiry"
        );
    }
}
