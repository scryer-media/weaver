//! GraphQL sockets over a live connection: who may open one, and that an open
//! socket loses its access the moment the credential it was admitted with does.

use super::*;
use async_graphql::futures_util::{SinkExt, StreamExt};
use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema};
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as SocketError, Message};
use weaver_server_core::security::RuntimeSecurityConfig;

struct Query;

#[Object]
impl Query {
    async fn ready(&self) -> bool {
        true
    }
}

type TestSchema = Schema<Query, EmptyMutation, EmptySubscription>;

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const USERNAME: &str = "admin";
const SESSION_TOKEN: &str = "browser-session-token";
const SOCKET_WAIT: Duration = Duration::from_secs(5);

struct SocketServer {
    addr: SocketAddr,
    db: Database,
    auth_cache: LoginAuthCache,
    api_key_cache: ApiKeyCache,
    security: Arc<RuntimeSecurityConfig>,
}

impl SocketServer {
    async fn start(db: Database, auth_cache: LoginAuthCache) -> Self {
        let api_key_cache = ApiKeyCache::default();
        let security = Arc::new(RuntimeSecurityConfig::default());
        let app = Router::new()
            .route(
                "/graphql/ws",
                get(super::super::graphql::ws_handler::<TestSchema>),
            )
            .layer(Extension(Schema::new(
                Query,
                EmptyMutation,
                EmptySubscription,
            )))
            .layer(Extension(RequestAuthContext {
                db: db.clone(),
                auth_cache: auth_cache.clone(),
                api_key_cache: api_key_cache.clone(),
                session_token: SessionToken(Arc::new(SESSION_TOKEN.to_string())),
                security: Arc::clone(&security),
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        Self {
            addr,
            db,
            auth_cache,
            api_key_cache,
            security,
        }
    }

    async fn open(&self, headers: &[(header::HeaderName, String)]) -> Result<Client, SocketError> {
        let mut request = format!("ws://{}/graphql/ws", self.addr)
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("graphql-transport-ws"),
        );
        for (name, value) in headers {
            request
                .headers_mut()
                .insert(name.clone(), HeaderValue::from_str(value).unwrap());
        }
        tokio_tungstenite::connect_async(request)
            .await
            .map(|(client, _)| client)
    }

    /// A socket through `connection_init`, proven to answer a query.
    async fn admitted(
        &self,
        headers: &[(header::HeaderName, String)],
        init: serde_json::Value,
    ) -> Client {
        let mut client = self.open(headers).await.expect("upgrade accepted");
        send(
            &mut client,
            serde_json::json!({ "type": "connection_init", "payload": init }),
        )
        .await;
        assert_eq!(receive(&mut client).await["type"], "connection_ack");
        assert_answers(&mut client, "first").await;
        client
    }
}

async fn send(client: &mut Client, message: serde_json::Value) {
    client
        .send(Message::Text(message.to_string().into()))
        .await
        .unwrap();
}

async fn receive(client: &mut Client) -> serde_json::Value {
    loop {
        let message = tokio::time::timeout(SOCKET_WAIT, client.next())
            .await
            .expect("socket answered in time")
            .expect("socket still open")
            .unwrap();
        match message {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("expected a protocol message, got {other:?}"),
        }
    }
}

async fn assert_answers(client: &mut Client, id: &str) {
    send(
        client,
        serde_json::json!({ "id": id, "type": "subscribe", "payload": { "query": "{ ready }" } }),
    )
    .await;
    let next = receive(client).await;
    assert_eq!(next["type"], "next", "{next}");
    assert_eq!(next["payload"]["data"]["ready"], true, "{next}");
    assert_eq!(receive(client).await["type"], "complete");
}

/// The socket is closed with `4403 Forbidden` without the client doing anything.
async fn assert_closed_forbidden(client: &mut Client) {
    let message = tokio::time::timeout(SOCKET_WAIT, client.next())
        .await
        .expect("socket was closed in time")
        .expect("a close frame precedes the end of the stream")
        .unwrap();
    let Message::Close(Some(frame)) = message else {
        panic!("expected a close frame, got {message:?}");
    };
    assert_eq!(frame.code, CloseCode::Library(4403));
    assert_eq!(frame.reason.as_str(), "Forbidden");
}

fn login_enabled(db: &Database) -> LoginAuthCache {
    let hash = jwt::hash_password(&test_password()).unwrap();
    db.set_auth_credentials(USERNAME, &hash).unwrap();
    let secret = db.get_or_create_jwt_signing_secret().unwrap();
    LoginAuthCache::from_credentials(db.get_auth_credentials().unwrap(), secret)
}

fn jwt_cookie(auth_cache: &LoginAuthCache) -> (header::HeaderName, String) {
    let auth = auth_cache.snapshot().expect("login is enabled");
    let token = jwt::create_jwt(&auth.username, &auth.jwt_secret, JWT_TTL_SECS);
    (header::COOKIE, format!("weaver_jwt={token}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn changing_the_password_closes_sockets_opened_with_the_old_login() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = login_enabled(&db);
    let server = SocketServer::start(db, auth_cache).await;
    let cookie = jwt_cookie(&server.auth_cache);
    let mut client = server
        .admitted(std::slice::from_ref(&cookie), serde_json::json!({}))
        .await;

    // What `changePassword` does once the current password checks out.
    let hash = jwt::hash_password("a-different-long-password").unwrap();
    server.db.set_auth_credentials(USERNAME, &hash).unwrap();
    let secret = server.db.rotate_jwt_signing_secret().unwrap();
    server
        .auth_cache
        .replace(Some(CachedLoginAuth::new(USERNAME, hash, secret)));

    assert_closed_forbidden(&mut client).await;
    // The old cookie cannot come back in through a new socket either.
    let mut retry = server.open(&[cookie]).await.expect("upgrade accepted");
    send(
        &mut retry,
        serde_json::json!({ "type": "connection_init", "payload": {} }),
    )
    .await;
    let refused = tokio::time::timeout(SOCKET_WAIT, retry.next())
        .await
        .expect("refused in time")
        .expect("a close frame precedes the end of the stream")
        .unwrap();
    let Message::Close(Some(frame)) = refused else {
        panic!("expected the init to be refused, got {refused:?}");
    };
    assert_eq!(frame.code, CloseCode::Protocol);
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_an_api_key_closes_only_the_sockets_that_key_opened() {
    let db = Database::open_in_memory().unwrap();
    let server = SocketServer::start(db, LoginAuthCache::default()).await;
    let kept_key = "kept-integration-key";
    let deleted_key = "deleted-integration-key";
    server
        .db
        .insert_api_key("kept", &hash_api_key(kept_key), "control")
        .unwrap();
    let deleted_id = server
        .db
        .insert_api_key("deleted", &hash_api_key(deleted_key), "control")
        .unwrap();

    let mut kept_by_init = server
        .admitted(&[], serde_json::json!({ "api_key": kept_key }))
        .await;
    let mut deleted_by_init = server
        .admitted(&[], serde_json::json!({ "api_key": deleted_key }))
        .await;
    let mut deleted_by_header = server
        .admitted(
            &[(
                header::HeaderName::from_static("x-api-key"),
                deleted_key.to_string(),
            )],
            serde_json::json!({}),
        )
        .await;

    // What `deleteApiKey` does.
    assert!(server.db.delete_api_key(deleted_id).unwrap());
    server.api_key_cache.remove_by_id(deleted_id);

    assert_closed_forbidden(&mut deleted_by_init).await;
    assert_closed_forbidden(&mut deleted_by_header).await;
    assert_answers(&mut kept_by_init, "after-delete").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn narrowing_trusted_networks_closes_local_browser_sockets() {
    let db = Database::open_in_memory().unwrap();
    let server = SocketServer::start(db, LoginAuthCache::default()).await;
    server
        .security
        .set_trusted_cidrs(vec!["127.0.0.0/8".parse().unwrap()]);
    let mut client = server
        .admitted(
            &[(header::COOKIE, format!("weaver_session={SESSION_TOKEN}"))],
            serde_json::json!({}),
        )
        .await;

    // An unrelated change to the same list keeps the socket.
    server.security.set_trusted_cidrs(vec![
        "127.0.0.0/8".parse().unwrap(),
        "10.0.0.0/8".parse().unwrap(),
    ]);
    assert_answers(&mut client, "still-trusted").await;

    server
        .security
        .set_trusted_cidrs(vec!["10.0.0.0/8".parse().unwrap()]);
    assert_closed_forbidden(&mut client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sockets_are_refused_to_pages_from_other_origins() {
    let db = Database::open_in_memory().unwrap();
    let server = SocketServer::start(db, LoginAuthCache::default()).await;
    let key = "origin-check-key";
    server
        .db
        .insert_api_key("origin", &hash_api_key(key), "read")
        .unwrap();
    let init = serde_json::json!({ "api_key": key });

    for origin in [
        "http://attacker.example.test".to_string(),
        format!("http://127.0.0.1:{}", server.addr.port() + 1),
        "null".to_string(),
    ] {
        match server.open(&[(header::ORIGIN, origin.clone())]).await {
            Err(SocketError::Http(response)) => {
                assert_eq!(response.status(), StatusCode::FORBIDDEN, "{origin}");
            }
            other => panic!("{origin} was not refused: {:?}", other.map(|_| ())),
        }
    }

    // The application's own page, and a machine client that sends no Origin.
    server
        .admitted(
            &[(header::ORIGIN, format!("http://{}", server.addr))],
            init.clone(),
        )
        .await;
    server.admitted(&[], init).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_browser_socket_keeps_origin_binding_and_closes_after_logout() {
    let db = Database::open_in_memory().unwrap();
    let server = SocketServer::start(db, LoginAuthCache::default()).await;
    server
        .security
        .apply_stored_access_policy_revision(None, None, false);
    let token = "authenticated-socket-test-token";
    let csrf = "authenticated-socket-test-csrf";
    let hex_hash = |value: &str| {
        jwt::hash_api_key(value)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let origin = "https://media.example.test";
    server
        .db
        .create_browser_session(&weaver_server_core::auth::BrowserSession {
            token_hash: hex_hash(token),
            csrf_verifier: hex_hash(csrf),
            origin: origin.into(),
            client_ip: None,
            remembered: false,
            created_at: now,
            expires_at: now + 60,
            revoked_at: None,
        })
        .unwrap();
    let headers = [
        (header::COOKIE, format!("weaver_session={token}")),
        (header::ORIGIN, origin.into()),
    ];
    // A proxy may rewrite Host; the persisted Origin plus CSRF proof governs this mode.
    let mut client = server
        .admitted(&headers, serde_json::json!({"csrf": csrf}))
        .await;
    server
        .db
        .revoke_browser_session(&hex_hash(token), now)
        .unwrap();
    assert_closed_forbidden(&mut client).await;
}
