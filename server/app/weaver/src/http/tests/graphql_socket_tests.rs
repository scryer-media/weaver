// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// GraphQL sockets over a live connection: who may open one, and that an open
// socket loses its access the moment the credential it was admitted with does.

use super::*;
use async_graphql::futures_util::{SinkExt, Stream, StreamExt, stream};
use async_graphql::{Context, EmptyMutation, Object, Schema, Subscription};
use std::collections::BTreeSet;
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

// The values a test sends to every live `ticks` subscription.
struct Ticks(broadcast::Sender<i32>);

struct TestSubscription;

#[Subscription]
impl TestSubscription {
    // 0 at once, then each value the test sends. With `until`, the stream ends
    // by itself when that value arrives, without sending it.
    async fn ticks(&self, ctx: &Context<'_>, until: Option<i32>) -> impl Stream<Item = i32> {
        let receiver = ctx.data_unchecked::<Ticks>().0.subscribe();
        stream::once(async { 0 }).chain(stream::unfold(receiver, move |mut receiver| async move {
            match receiver.recv().await {
                Ok(value) if Some(value) != until => Some((value, receiver)),
                _ => None,
            }
        }))
    }
}

type TestSchema = Schema<Query, EmptyMutation, TestSubscription>;

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const USERNAME: &str = "admin";
const SESSION_TOKEN: &str = "browser-session-token";

struct SocketServer {
    addr: SocketAddr,
    db: Database,
    auth_cache: LoginAuthCache,
    api_key_cache: ApiKeyCache,
    security: Arc<RuntimeSecurityConfig>,
    ticks: broadcast::Sender<i32>,
}

impl SocketServer {
    async fn start(db: Database, auth_cache: LoginAuthCache) -> Self {
        let api_key_cache = ApiKeyCache::default();
        let security = Arc::new(RuntimeSecurityConfig::default());
        let (ticks, _) = broadcast::channel(64);
        let app = Router::new()
            .route(
                "/graphql/ws",
                get(super::super::graphql::ws_handler::<TestSchema>),
            )
            .layer(Extension(
                Schema::build(Query, EmptyMutation, TestSubscription)
                    .data(Ticks(ticks.clone()))
                    .finish(),
            ))
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
            ticks,
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

    // A socket through `connection_init`, proven to answer a query.
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
        let message = client.next().await.expect("socket still open").unwrap();
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

// The socket is closed with `4403 Forbidden` without the client doing anything.
async fn assert_closed_forbidden(client: &mut Client) {
    let message = client
        .next()
        .await
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
    let refused = retry
        .next()
        .await
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

async fn subscribe(client: &mut Client, id: &str, query: &str) {
    send(
        client,
        serde_json::json!({ "id": id, "type": "subscribe", "payload": { "query": query } }),
    )
    .await;
}

// The next message for operation `id`, passing over messages for the others.
async fn receive_for(client: &mut Client, id: &str) -> serde_json::Value {
    loop {
        let message = receive(client).await;
        if message["id"] == id {
            return message;
        }
    }
}

// The ids of the next `count` messages, each a `next` carrying `value`.
async fn receive_ticks(client: &mut Client, count: usize, value: i32) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for _ in 0..count {
        let message = receive(client).await;
        assert_eq!(message["type"], "next", "{message}");
        assert_eq!(message["payload"]["data"]["ticks"], value, "{message}");
        ids.insert(message["id"].as_str().unwrap().to_string());
    }
    ids
}

fn assert_refused_as_full(message: &serde_json::Value, id: &str) {
    assert_eq!(message["type"], "error", "{message}");
    assert_eq!(message["id"], id, "{message}");
    assert_eq!(
        message["payload"][0]["message"],
        "Too many live subscriptions on this connection (limit 32)",
        "{message}"
    );
}

async fn read_key_socket(server: &SocketServer, key: &str) -> Client {
    server
        .admitted(&[], serde_json::json!({ "api_key": key }))
        .await
}

fn insert_read_key(server: &SocketServer, key: &str) {
    server
        .db
        .insert_api_key(key, &hash_api_key(key), "read")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_socket_holds_at_most_32_live_subscriptions_and_a_stop_frees_one() {
    let server = SocketServer::start(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
    )
    .await;
    insert_read_key(&server, "subscription-cap-key");
    let mut client = read_key_socket(&server, "subscription-cap-key").await;

    let ids: BTreeSet<String> = (0..32).map(|index| format!("live-{index}")).collect();
    for id in &ids {
        subscribe(&mut client, id, "subscription { ticks }").await;
    }
    assert_eq!(receive_ticks(&mut client, 32, 0).await, ids);

    subscribe(&mut client, "over", "subscription { ticks }").await;
    assert_refused_as_full(&receive(&mut client).await, "over");

    // The refusal left the socket and the 32 others running.
    server.ticks.send(1).unwrap();
    assert_eq!(receive_ticks(&mut client, 32, 1).await, ids);
    assert_answers(&mut client, "query-while-full").await;

    // A stopped subscription gives its place to the next subscribe.
    send(
        &mut client,
        serde_json::json!({ "id": "live-0", "type": "complete" }),
    )
    .await;
    subscribe(&mut client, "after-stop", "subscription { ticks }").await;
    let started = receive_for(&mut client, "after-stop").await;
    assert_eq!(started["type"], "next", "{started}");
    assert_eq!(started["payload"]["data"]["ticks"], 0, "{started}");

    subscribe(&mut client, "over-again", "subscription { ticks }").await;
    assert_refused_as_full(&receive_for(&mut client, "over-again").await, "over-again");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_that_ends_by_itself_frees_its_place() {
    let server = SocketServer::start(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
    )
    .await;
    insert_read_key(&server, "subscription-end-key");
    let mut client = read_key_socket(&server, "subscription-end-key").await;

    for index in 0..31 {
        subscribe(
            &mut client,
            &format!("live-{index}"),
            "subscription { ticks }",
        )
        .await;
    }
    subscribe(&mut client, "ending", "subscription { ticks(until: 99) }").await;
    receive_ticks(&mut client, 32, 0).await;
    subscribe(&mut client, "over", "subscription { ticks }").await;
    assert_refused_as_full(&receive(&mut client).await, "over");

    server.ticks.send(99).unwrap();
    assert_eq!(receive_for(&mut client, "ending").await["type"], "complete");

    subscribe(&mut client, "after-end", "subscription { ticks }").await;
    let started = receive_for(&mut client, "after-end").await;
    assert_eq!(started["type"], "next", "{started}");
    subscribe(&mut client, "over-again", "subscription { ticks }").await;
    assert_refused_as_full(&receive_for(&mut client, "over-again").await, "over-again");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_credential_holds_at_most_8_open_sockets() {
    let server = SocketServer::start(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
    )
    .await;
    insert_read_key(&server, "socket-cap-key");
    insert_read_key(&server, "socket-cap-other-key");

    let mut open = Vec::new();
    for _ in 0..8 {
        open.push(read_key_socket(&server, "socket-cap-key").await);
    }

    let mut ninth = server.open(&[]).await.expect("upgrade accepted");
    send(
        &mut ninth,
        serde_json::json!({ "type": "connection_init", "payload": { "api_key": "socket-cap-key" } }),
    )
    .await;
    let refused = ninth
        .next()
        .await
        .expect("a close frame precedes the end of the stream")
        .unwrap();
    let Message::Close(Some(frame)) = refused else {
        panic!("expected the ninth socket to be closed, got {refused:?}");
    };
    assert_eq!(frame.code, CloseCode::Library(4429));
    assert_eq!(
        frame.reason.as_str(),
        "Too many open connections for this credential (limit 8)"
    );

    // Another credential is counted on its own.
    read_key_socket(&server, "socket-cap-other-key").await;

    // A closed socket gives its place back; the eight still open keep working.
    let mut closed = open.pop().unwrap();
    closed.close(None).await.unwrap();
    while closed.next().await.is_some() {}
    open.push(read_key_socket(&server, "socket-cap-key").await);
    for client in &mut open {
        assert_answers(client, "still-open").await;
    }
}
