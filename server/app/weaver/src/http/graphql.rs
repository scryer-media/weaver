use async_graphql::{Data, Executor};
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::extract::ws::{CloseFrame, Message};
use axum::extract::{ConnectInfo, Extension, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::watch;

use weaver_server_api::WeaverSchema;
use weaver_server_api::auth::{CallerIdentity, CallerScope};
use weaver_server_core::auth::hash_api_key;
use weaver_server_core::security::HttpAuthority;

/// How often an open socket re-checks its credential with no change signal.
/// This is what retires a login token that simply reached its expiry.
const SOCKET_RECHECK_INTERVAL: Duration = Duration::from_secs(60);

/// graphql-transport-ws `Forbidden`: the credential stopped authorizing this
/// connection.
const CLOSE_FORBIDDEN: u16 = 4403;
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

/// Whether the page that opened this socket is this application.
///
/// Browsers always send `Origin` on a socket upgrade and page scripts cannot
/// forge it; a request without one is a machine client, which carries its own
/// credential rather than riding on a browser's cookies.
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

/// The credential an open socket was admitted with.
#[derive(Clone)]
enum SocketGrant {
    /// A key from `connection_init`, which outranks the upgrade request.
    ApiKey {
        key_hash: [u8; 32],
        scope: CallerScope,
    },
    /// What the upgrade request carried: a key header, a login cookie, or a
    /// trusted browser session.
    Upgrade(super::auth::ResolvedCaller),
}

/// Everything that can take a credential away, subscribed before the upgrade
/// is authenticated so no change can slip between the check and the watch.
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

    fn mark_seen(&mut self) {
        self.login.mark_unchanged();
        self.api_keys.mark_unchanged();
        self.trust.mark_unchanged();
    }
}

/// Whether `grant` still authorizes exactly what it did when it was admitted.
async fn grant_still_holds(
    auth: &super::RequestAuthContext,
    peer: Option<SocketAddr>,
    headers: &HeaderMap,
    grant: &SocketGrant,
) -> bool {
    match grant {
        SocketGrant::ApiKey { key_hash, scope } => matches!(
            super::auth::lookup_api_key_auth(&auth.db, &auth.api_key_cache, *key_hash).await,
            Ok(Some(row)) if super::auth::caller_scope_from_api_key_scope(&row.scope) == *scope
        ),
        SocketGrant::Upgrade(admitted) => matches!(
            super::auth::resolve_caller(
                &auth.db,
                &auth.auth_cache,
                &auth.api_key_cache,
                auth.session_token.0.as_str(),
                &auth.security,
                super::auth::BrowserSessionPolicy::TrustedPeer(peer),
                headers,
            )
            .await,
            Ok(current) if current.identity == admitted.identity && current.scope == admitted.scope
        ),
    }
}

/// Resolves once the socket's credential no longer authorizes it. A socket
/// that has not finished `connection_init` holds nothing to revoke.
async fn revoked(
    auth: super::RequestAuthContext,
    peer: Option<SocketAddr>,
    headers: HeaderMap,
    mut changes: RevocationWatch,
    mut grant: watch::Receiver<Option<SocketGrant>>,
) {
    loop {
        changes.mark_seen();
        let current = grant.borrow_and_update().clone();
        if let Some(current) = current
            && !grant_still_holds(&auth, peer, &headers, &current).await
        {
            return;
        }
        tokio::select! {
            _ = changes.login.changed() => {}
            _ = changes.api_keys.changed() => {}
            _ = changes.trust.changed() => {}
            _ = grant.changed() => {}
            () = tokio::time::sleep(SOCKET_RECHECK_INTERVAL) => {}
        }
    }
}

/// Generic over the executor so a socket's admission and revocation do not
/// depend on which schema it serves.
pub(super) async fn ws_handler<E: Executor>(
    Extension(schema): Extension<E>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    protocol: GraphQLProtocol,
    ws: WebSocketUpgrade,
) -> Response {
    let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
    if !upgrade_origin_allowed(&request_auth, peer, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let changes = RevocationWatch::subscribe(&request_auth);
    // Pre-resolve scope from cookies on the upgrade request. Browsers
    // automatically send cookies on WebSocket upgrade, so JWT auth works
    // without needing api_key in connection_init.
    let upgrade_caller = super::auth::resolve_caller(
        &request_auth.db,
        &request_auth.auth_cache,
        &request_auth.api_key_cache,
        request_auth.session_token.0.as_str(),
        &request_auth.security,
        super::auth::BrowserSessionPolicy::TrustedPeer(peer),
        &headers,
    )
    .await
    .ok();

    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |mut socket| async move {
            let (grant_tx, grant_rx) = watch::channel(None);
            let init_auth = request_auth.clone();
            let lost_grant = {
                let serve = GraphQLWebSocket::new(&mut socket, schema, protocol)
                    .on_connection_init(move |payload: serde_json::Value| async move {
                        let request_auth = init_auth;
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
                            let scope = super::auth::caller_scope_from_api_key_scope(&row.scope);
                            grant_tx.send_replace(Some(SocketGrant::ApiKey {
                                key_hash: row.key_hash,
                                scope,
                            }));
                            let mut data = Data::default();
                            data.insert(scope);
                            data.insert(CallerIdentity::ApiKey(row.key_hash));
                            return Ok(data);
                        }

                        if let Some(caller) = upgrade_caller {
                            let mut data = Data::default();
                            data.insert(caller.scope);
                            data.insert(caller.identity.clone());
                            grant_tx.send_replace(Some(SocketGrant::Upgrade(caller)));
                            return Ok(data);
                        }

                        Err(async_graphql::Error::new(
                            "Missing authorization or api_key in connection_init",
                        ))
                    })
                    .serve();
                tokio::select! {
                    () = serve => false,
                    () = revoked(request_auth, peer, headers, changes, grant_rx) => true,
                }
            };
            // Requests re-authenticate on every call; a socket would otherwise
            // keep a deleted key's or a rotated login's access until it drops.
            if lost_grant {
                let _ = socket
                    .send(Message::Close(Some(CloseFrame {
                        code: CLOSE_FORBIDDEN,
                        reason: "Forbidden".into(),
                    })))
                    .await;
            }
        })
        .into_response()
}
