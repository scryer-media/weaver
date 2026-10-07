//! SSH and WireGuard endpoints for the Weaver advanced-networking e2e flows.
//!
//! The doubles come from proxy-tunnels' `test-support` feature at the exact
//! revision Weaver pins, so the protocol on the far side of every tunnel is the
//! one Weaver's own engine tests run against. Those doubles bind loopback
//! only. Each one is published on a fixed port of this container through a
//! relay that this binary owns, and the relays are what make the doubles
//! controllable from a test: an endpoint can be taken down (listener closed,
//! live sessions cut), brought back, or switched to a double presenting a
//! different host key, and every session it carries is recorded with the
//! client address it came from.
//!
//! The control API listens on 8095:
//!
//! - `GET /` the endpoints, SSH evidence, WireGuard evidence and credentials.
//! - `GET /events?after=N` events with a sequence above `N`.
//! - `POST /` `{endpoint, up}` | `{endpoint, cut: true}` |
//!   `{endpoint: "ssh-switch", hostKey: "primary" | "other"}`.

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use proxy_tunnels::test_support::{
    CLIENT_ED25519_PEM, CLIENT_ED25519_PEM_PASSPHRASE, CLIENT_ED25519_PEM_WITH_PASSPHRASE,
    HOST_ED25519_PEM, HOST_KEY_FINGERPRINT, OTHER_HOST_ED25519_PEM, OTHER_HOST_KEY_FINGERPRINT,
    SshServerDouble, SshServerOptions, TEST_CLIENT_ADDRESS, TEST_PEER_ADDRESS, WireGuardTestPeer,
    WireGuardTestPeerOptions, test_client_private_key, test_preshared_key,
};
use proxy_tunnels::{NoopTunnelObserver, TunnelProvider, WireGuardTunnelProvider};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::watch;
use tokio::task::JoinSet;

/// Events kept for watermark reads. Older ones are dropped in one block so a
/// long run cannot grow without bound; sequences never repeat.
const EVENT_LIMIT: usize = 50_000;

#[derive(Default)]
struct Log {
    sequence: u64,
    events: Vec<Value>,
}

impl Log {
    fn record(&mut self, kind: &str, fields: Value) {
        if self.events.len() >= EVENT_LIMIT {
            self.events.drain(..EVENT_LIMIT / 5);
        }
        self.sequence += 1;
        let mut event = json!({ "sequence": self.sequence, "kind": kind });
        if let (Some(event), Value::Object(fields)) = (event.as_object_mut(), fields) {
            event.extend(fields);
        }
        self.events.push(event);
    }

    fn after(&self, sequence: u64) -> Vec<Value> {
        self.events
            .iter()
            .filter(|event| {
                event["sequence"]
                    .as_u64()
                    .is_some_and(|value| value > sequence)
            })
            .cloned()
            .collect()
    }
}

type SharedLog = Arc<Mutex<Log>>;

fn record(log: &SharedLog, kind: &str, fields: Value) {
    log.lock().expect("event log").record(kind, fields);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Transport {
    Tcp,
    Udp,
}

/// One published port and the double behind it.
struct Endpoint {
    name: String,
    transport: Transport,
    bind: SocketAddr,
    /// The port actually bound; equals `bind`'s port unless that was 0.
    port: AtomicU64,
    up: watch::Sender<bool>,
    cut: watch::Sender<u64>,
    upstream: Mutex<String>,
    active: AtomicI64,
}

impl Endpoint {
    fn new(name: &str, transport: Transport, bind: SocketAddr, upstream: String) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_string(),
            transport,
            bind,
            port: AtomicU64::new(u64::from(bind.port())),
            up: watch::Sender::new(true),
            cut: watch::Sender::new(0),
            upstream: Mutex::new(upstream),
            active: AtomicI64::new(0),
        })
    }

    fn bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind.ip(), self.port.load(Ordering::SeqCst) as u16)
    }

    fn upstream(&self) -> String {
        self.upstream.lock().expect("upstream").clone()
    }

    fn set_up(&self, up: bool) {
        self.up.send_replace(up);
    }

    fn cut(&self) {
        self.cut.send_modify(|generation| *generation += 1);
    }

    fn summary(&self) -> Value {
        json!({
            "transport": match self.transport { Transport::Tcp => "tcp", Transport::Udp => "udp" },
            "port": self.port.load(Ordering::SeqCst),
            "up": *self.up.borrow(),
            "active": self.active.load(Ordering::SeqCst),
            "upstream": self.upstream(),
        })
    }
}

/// Counts a session in `active` and records its end however the task ends,
/// including when a cut or a down aborts it.
struct SessionGuard {
    log: SharedLog,
    endpoint: Arc<Endpoint>,
    connection: u64,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.endpoint.active.fetch_sub(1, Ordering::SeqCst);
        record(
            &self.log,
            "disconnect",
            json!({ "endpoint": self.endpoint.name, "connection": self.connection }),
        );
    }
}

static CONNECTIONS: AtomicU64 = AtomicU64::new(0);

async fn relay_tcp(
    endpoint: Arc<Endpoint>,
    log: SharedLog,
    mut client: TcpStream,
    peer: SocketAddr,
) {
    let connection = CONNECTIONS.fetch_add(1, Ordering::SeqCst) + 1;
    let upstream = endpoint.upstream();
    endpoint.active.fetch_add(1, Ordering::SeqCst);
    record(
        &log,
        "connect",
        json!({
            "endpoint": endpoint.name, "connection": connection,
            "client": peer.ip().to_string(), "upstream": upstream,
        }),
    );
    let _guard = SessionGuard {
        log: Arc::clone(&log),
        endpoint: Arc::clone(&endpoint),
        connection,
    };
    match TcpStream::connect(&upstream).await {
        Ok(mut server) => {
            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
        }
        Err(error) => record(
            &log,
            "upstream-failed",
            json!({ "endpoint": endpoint.name, "connection": connection, "error": error.to_string() }),
        ),
    }
}

/// Serve a TCP endpoint: listen while up, close the listener and every live
/// session while down, and rebind the same port when it comes back.
async fn serve_tcp(
    endpoint: Arc<Endpoint>,
    log: SharedLog,
    ready: Option<tokio::sync::oneshot::Sender<u16>>,
) {
    let mut ready = ready;
    let mut up = endpoint.up.subscribe();
    loop {
        while !*up.borrow_and_update() {
            if up.changed().await.is_err() {
                return;
            }
        }
        let listener = TcpListener::bind(endpoint.bind_addr())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "bind {} on {}: {error}",
                    endpoint.name,
                    endpoint.bind_addr()
                )
            });
        let port = listener.local_addr().expect("bound address").port();
        endpoint.port.store(u64::from(port), Ordering::SeqCst);
        if let Some(ready) = ready.take() {
            let _ = ready.send(port);
        }
        record(
            &log,
            "endpoint-up",
            json!({ "endpoint": endpoint.name, "port": port }),
        );
        let mut cut = endpoint.cut.subscribe();
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    if let Ok((stream, peer)) = accepted {
                        sessions.spawn(relay_tcp(Arc::clone(&endpoint), Arc::clone(&log), stream, peer));
                    }
                }
                changed = up.changed() => {
                    if changed.is_err() || !*up.borrow_and_update() { break; }
                }
                changed = cut.changed() => {
                    if changed.is_err() { break; }
                    record(&log, "cut", json!({ "endpoint": endpoint.name, "sessions": sessions.len() }));
                    sessions.abort_all();
                }
                Some(_) = sessions.join_next(), if !sessions.is_empty() => {}
            }
        }
        drop(listener);
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
        record(&log, "endpoint-down", json!({ "endpoint": endpoint.name }));
    }
}

/// Serve a UDP endpoint: one loopback socket per client address, so the
/// double sees each client as its own peer. Down closes the public socket,
/// which is what an unreachable WireGuard endpoint looks like to a client.
async fn serve_udp(
    endpoint: Arc<Endpoint>,
    log: SharedLog,
    ready: Option<tokio::sync::oneshot::Sender<u16>>,
) {
    let mut ready = ready;
    let mut up = endpoint.up.subscribe();
    loop {
        while !*up.borrow_and_update() {
            if up.changed().await.is_err() {
                return;
            }
        }
        let socket = Arc::new(UdpSocket::bind(endpoint.bind_addr()).await.unwrap_or_else(
            |error| {
                panic!(
                    "bind {} on {}: {error}",
                    endpoint.name,
                    endpoint.bind_addr()
                )
            },
        ));
        let port = socket.local_addr().expect("bound address").port();
        endpoint.port.store(u64::from(port), Ordering::SeqCst);
        if let Some(ready) = ready.take() {
            let _ = ready.send(port);
        }
        record(
            &log,
            "endpoint-up",
            json!({ "endpoint": endpoint.name, "port": port }),
        );
        let mut cut = endpoint.cut.subscribe();
        let mut flows: HashMap<SocketAddr, Arc<UdpSocket>> = HashMap::new();
        let mut returns = JoinSet::new();
        let mut buffer = vec![0u8; 65_535];
        loop {
            tokio::select! {
                received = socket.recv_from(&mut buffer) => {
                    let Ok((length, peer)) = received else { continue };
                    let flow = match flows.get(&peer) {
                        Some(flow) => Arc::clone(flow),
                        None => {
                            let Ok(flow) = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await else { continue };
                            if flow.connect(endpoint.upstream()).await.is_err() { continue; }
                            let flow = Arc::new(flow);
                            record(&log, "connect", json!({
                                "endpoint": endpoint.name, "client": peer.ip().to_string(),
                                "clientPort": peer.port(), "upstream": endpoint.upstream(),
                            }));
                            endpoint.active.fetch_add(1, Ordering::SeqCst);
                            returns.spawn(udp_return(Arc::clone(&flow), Arc::clone(&socket), peer));
                            flows.insert(peer, Arc::clone(&flow));
                            flow
                        }
                    };
                    let _ = flow.send(&buffer[..length]).await;
                }
                changed = up.changed() => {
                    if changed.is_err() || !*up.borrow_and_update() { break; }
                }
                changed = cut.changed() => {
                    if changed.is_err() { break; }
                    record(&log, "cut", json!({ "endpoint": endpoint.name, "sessions": flows.len() }));
                    endpoint.active.fetch_sub(flows.len() as i64, Ordering::SeqCst);
                    flows.clear();
                    returns.abort_all();
                }
                Some(_) = returns.join_next(), if !returns.is_empty() => {}
            }
        }
        endpoint
            .active
            .fetch_sub(flows.len() as i64, Ordering::SeqCst);
        drop(flows);
        returns.abort_all();
        while returns.join_next().await.is_some() {}
        drop(socket);
        record(&log, "endpoint-down", json!({ "endpoint": endpoint.name }));
    }
}

async fn udp_return(flow: Arc<UdpSocket>, public: Arc<UdpSocket>, peer: SocketAddr) {
    let mut buffer = vec![0u8; 65_535];
    while let Ok(length) = flow.recv(&mut buffer).await {
        let _ = public.send_to(&buffer[..length], peer).await;
    }
}

/// Start an endpoint and return the port it bound, once it is listening.
async fn start_endpoint(endpoint: &Arc<Endpoint>, log: &SharedLog) -> u16 {
    let (ready, bound) = tokio::sync::oneshot::channel();
    match endpoint.transport {
        Transport::Tcp => tokio::spawn(serve_tcp(
            Arc::clone(endpoint),
            Arc::clone(log),
            Some(ready),
        )),
        Transport::Udp => tokio::spawn(serve_udp(
            Arc::clone(endpoint),
            Arc::clone(log),
            Some(ready),
        )),
    };
    bound.await.expect("endpoint reports its port")
}

struct SshDouble {
    name: &'static str,
    double: SshServerDouble,
}

struct WireGuardDouble {
    name: &'static str,
    peer: WireGuardTestPeer,
    preshared: bool,
}

struct Fixture {
    log: SharedLog,
    endpoints: BTreeMap<String, Arc<Endpoint>>,
    ssh: Vec<SshDouble>,
    switch_targets: HashMap<&'static str, String>,
    wireguard: Vec<WireGuardDouble>,
}

/// Fixed public ports. Toxiproxy fronts the SSH ones as ssh1..ssh3.
const SSH_ENDPOINTS: &[(&str, u16)] = &[
    ("ssh1", 2221),
    ("ssh2", 2222),
    ("ssh3", 2223),
    ("ssh-switch", 2224),
    ("ssh-refuse", 2225),
];
const WIREGUARD_ENDPOINTS: &[(&str, u16)] = &[("wg1", 51821), ("wg2", 51822), ("wg-rss", 51823)];

/// A name every WireGuard peer answers and no test asks for. The fixture
/// resolves it through a real tunnel before it publishes the peer, so a
/// client can never reach a peer whose resolver is not yet serving, and the
/// query it leaves in the peer's evidence cannot satisfy a test's assertion.
const READY_NAME: &str = "ready.tunnel-fixture.test";

/// Resolve [`READY_NAME`] through a tunnel to `peer` until its DNS server
/// answers with the peer's own address. Each attempt is bounded by the
/// client's request timeout; the wait ends on the answer, never on time, and
/// a peer that never answers keeps the fixture from reporting ready.
async fn dns_serving(peer: &WireGuardTestPeer, name: &str) -> u32 {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let client = WireGuardTunnelProvider::new(
            peer.client_spec(&format!("tunnel-fixture-ready-{name}")),
            Arc::new(NoopTunnelObserver),
        );
        let answer = client.resolve_host(READY_NAME).await;
        client.shutdown().await;
        match answer {
            Ok(addresses) if addresses.contains(&IpAddr::V4(TEST_PEER_ADDRESS)) => return attempts,
            Ok(addresses) => {
                eprintln!("{name}: readiness lookup answered {addresses:?}, not the peer address")
            }
            Err(error) => eprintln!("{name}: readiness lookup attempt {attempts} failed: {error}"),
        }
    }
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

async fn build_fixture(public_ip: IpAddr, nntp: String, http: String, log: SharedLog) -> Fixture {
    let mut ssh = Vec::new();
    for name in ["ssh1", "ssh2", "ssh3"] {
        ssh.push(SshDouble {
            name,
            double: SshServerDouble::start(SshServerOptions::default()).await,
        });
    }
    let primary = SshServerDouble::start(SshServerOptions {
        host_key_pem: HOST_ED25519_PEM,
        ..SshServerOptions::default()
    })
    .await;
    let other = SshServerDouble::start(SshServerOptions {
        host_key_pem: OTHER_HOST_ED25519_PEM,
        ..SshServerOptions::default()
    })
    .await;
    let mut switch_targets = HashMap::new();
    switch_targets.insert("primary", primary.addr().to_string());
    switch_targets.insert("other", other.addr().to_string());
    ssh.push(SshDouble {
        name: "ssh-switch-primary",
        double: primary,
    });
    ssh.push(SshDouble {
        name: "ssh-switch-other",
        double: other,
    });
    ssh.push(SshDouble {
        name: "ssh-refuse",
        double: SshServerDouble::start(SshServerOptions {
            refuse_forwarding: true,
            ..SshServerOptions::default()
        })
        .await,
    });

    let mut endpoints = BTreeMap::new();
    for (name, port) in SSH_ENDPOINTS {
        let upstream = match *name {
            "ssh-switch" => switch_targets["primary"].clone(),
            _ => ssh
                .iter()
                .find(|double| double.name == *name)
                .expect("ssh double")
                .double
                .addr()
                .to_string(),
        };
        let endpoint = Endpoint::new(
            name,
            Transport::Tcp,
            SocketAddr::new(public_ip, *port),
            upstream,
        );
        start_endpoint(&endpoint, &log).await;
        endpoints.insert(name.to_string(), endpoint);
    }

    // The WireGuard peers hand inbound TCP to a loopback relay, because the
    // peer only forwards to loopback; the relay reaches the real service.
    let nntp_relay = Endpoint::new(
        "wg-nntp-forward",
        Transport::Tcp,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        nntp,
    );
    let nntp_port = start_endpoint(&nntp_relay, &log).await;
    let http_relay = Endpoint::new(
        "wg-http-forward",
        Transport::Tcp,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        http,
    );
    let http_port = start_endpoint(&http_relay, &log).await;
    endpoints.insert(nntp_relay.name.clone(), nntp_relay);
    endpoints.insert(http_relay.name.clone(), http_relay);

    let mut wireguard = Vec::new();
    for (name, port) in WIREGUARD_ENDPOINTS {
        let tunnel_peer = vec![IpAddr::V4(TEST_PEER_ADDRESS)];
        let (names, http_port_inside, forward, preshared) = match *name {
            "wg1" => (vec!["nntp.proxy.test"], 119, nntp_port, false),
            "wg2" => (vec!["nntp.proxy.test"], 119, nntp_port, true),
            _ => (
                vec!["rss.proxy.test", "download.proxy.test"],
                8089,
                http_port,
                true,
            ),
        };
        let options = WireGuardTestPeerOptions {
            names: names
                .into_iter()
                .chain([READY_NAME])
                .map(|host| (host.to_string(), tunnel_peer.clone()))
                .collect(),
            http_port: http_port_inside,
            preshared_key: preshared.then(test_preshared_key),
            ..WireGuardTestPeerOptions::default()
        };
        let peer = WireGuardTestPeer::start_for_downloads(
            options,
            Some(SocketAddr::from((Ipv4Addr::LOCALHOST, forward))),
            Duration::ZERO,
            false,
        )
        .await;
        let attempts = dns_serving(&peer, name).await;
        record(
            &log,
            "wireguard-dns-ready",
            json!({ "endpoint": name, "attempts": attempts }),
        );
        let endpoint = Endpoint::new(
            name,
            Transport::Udp,
            SocketAddr::new(public_ip, *port),
            peer.endpoint().to_string(),
        );
        start_endpoint(&endpoint, &log).await;
        endpoints.insert(name.to_string(), endpoint);
        wireguard.push(WireGuardDouble {
            name,
            peer,
            preshared,
        });
    }

    Fixture {
        log,
        endpoints,
        ssh,
        switch_targets,
        wireguard,
    }
}

type AppState = Arc<Fixture>;

async fn snapshot(State(fixture): State<AppState>) -> Json<Value> {
    let endpoints: serde_json::Map<String, Value> = fixture
        .endpoints
        .iter()
        .map(|(name, endpoint)| (name.clone(), endpoint.summary()))
        .collect();
    let ssh: serde_json::Map<String, Value> = fixture
        .ssh
        .iter()
        .map(|double| {
            (
                double.name.to_string(),
                json!({
                    "forwarded": double.double.forwarded_targets(),
                    "acceptedAuth": double.double.accepted_auth(),
                }),
            )
        })
        .collect();
    let mut wireguard = serde_json::Map::new();
    for double in &fixture.wireguard {
        wireguard.insert(
            double.name.to_string(),
            json!({
                "peerPublicKey": STANDARD.encode(double.peer.public_key()),
                "presharedKey": double.preshared.then(|| STANDARD.encode(test_preshared_key())),
                "clientPrivateKey": STANDARD.encode(test_client_private_key()),
                "clientAddress": format!("{TEST_CLIENT_ADDRESS}/32"),
                "dnsServer": TEST_PEER_ADDRESS.to_string(),
                "dnsQueries": double.peer.dns_queries(),
                "requests": double.peer.requests(),
                "clientRxBytes": double.peer.client_rx_bytes().await,
            }),
        );
    }
    let defaults = SshServerOptions::default();
    let sequence = fixture.log.lock().expect("event log").sequence;
    Json(json!({
        "sequence": sequence,
        "endpoints": endpoints,
        "ssh": ssh,
        "sshSwitch": fixture.switch_targets.iter()
            .find(|(_, target)| **target == fixture.endpoints["ssh-switch"].upstream())
            .map(|(key, _)| *key),
        "hostKeys": { "primary": HOST_KEY_FINGERPRINT, "other": OTHER_HOST_KEY_FINGERPRINT },
        "sshClient": {
            "username": defaults.username,
            "password": defaults.password,
            "privateKey": CLIENT_ED25519_PEM,
            "privateKeyWithPassphrase": CLIENT_ED25519_PEM_WITH_PASSPHRASE,
            "passphrase": CLIENT_ED25519_PEM_PASSPHRASE,
        },
        "wireguard": wireguard,
    }))
}

#[derive(Deserialize)]
struct After {
    after: Option<u64>,
}

async fn events(State(fixture): State<AppState>, Query(query): Query<After>) -> Json<Value> {
    let log = fixture.log.lock().expect("event log");
    Json(json!({ "sequence": log.sequence, "events": log.after(query.after.unwrap_or(0)) }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Command {
    endpoint: String,
    up: Option<bool>,
    cut: Option<bool>,
    host_key: Option<String>,
}

fn apply(fixture: &Fixture, command: Command) -> Result<(), String> {
    let endpoint = fixture
        .endpoints
        .get(&command.endpoint)
        .ok_or_else(|| format!("unknown endpoint {}", command.endpoint))?;
    if let Some(key) = command.host_key.as_deref() {
        if command.endpoint != "ssh-switch" {
            return Err("hostKey applies only to ssh-switch".to_string());
        }
        let target = fixture
            .switch_targets
            .get(key)
            .ok_or_else(|| format!("unknown host key {key}"))?;
        *endpoint.upstream.lock().expect("upstream") = target.clone();
        record(
            &fixture.log,
            "host-key",
            json!({ "endpoint": endpoint.name, "hostKey": key }),
        );
        // Sessions already open keep the old key; new ones must see the new one.
        endpoint.cut();
    }
    if command.cut == Some(true) {
        endpoint.cut();
    }
    if let Some(up) = command.up {
        endpoint.set_up(up);
    }
    Ok(())
}

async fn control(
    State(fixture): State<AppState>,
    Json(command): Json<Command>,
) -> (StatusCode, Json<Value>) {
    match apply(&fixture, command) {
        Ok(()) => (StatusCode::OK, Json(json!({}))),
        Err(error) => (StatusCode::BAD_REQUEST, Json(json!({ "error": error }))),
    }
}

fn router(fixture: AppState) -> Router {
    Router::new()
        .route("/", get(snapshot).post(control))
        .route("/events", get(events))
        .with_state(fixture)
}

#[tokio::main]
async fn main() {
    let public_ip: IpAddr = env_or("TUNNEL_FIXTURE_BIND", "0.0.0.0")
        .parse()
        .expect("TUNNEL_FIXTURE_BIND is an IP address");
    let nntp = env_or("TUNNEL_FIXTURE_NNTP", "nntp:119");
    let http = env_or("TUNNEL_FIXTURE_HTTP", "proxy-fixture:8089");
    let control_port: u16 = env_or("TUNNEL_FIXTURE_CONTROL_PORT", "8095")
        .parse()
        .expect("control port");
    let log: SharedLog = Arc::default();
    let fixture = Arc::new(build_fixture(public_ip, nntp, http, log).await);
    let listener = TcpListener::bind((public_ip, control_port))
        .await
        .expect("bind control");
    println!("tunnel fixture ready");
    axum::serve(listener, router(fixture))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("control server");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn tcp_echo() -> SocketAddr {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let (mut reader, mut writer) = stream.split();
                    let _ = tokio::io::copy(&mut reader, &mut writer).await;
                });
            }
        });
        addr
    }

    async fn round_trip(port: u16, payload: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
        stream.write_all(payload).await?;
        let mut buffer = vec![0u8; payload.len()];
        stream.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    async fn wait_for(log: &SharedLog, kind: &str, after: u64) -> Value {
        let mut notify = tokio::time::interval(Duration::from_millis(5));
        loop {
            notify.tick().await;
            if let Some(event) = log
                .lock()
                .unwrap()
                .after(after)
                .into_iter()
                .find(|event| event["kind"] == kind)
            {
                return event;
            }
        }
    }

    #[test]
    fn log_reads_only_events_after_the_watermark() {
        let mut log = Log::default();
        log.record("one", json!({ "endpoint": "a" }));
        log.record("two", json!({}));
        assert_eq!(log.after(0).len(), 2);
        let after = log.after(1);
        assert_eq!(after.len(), 1);
        assert_eq!(after[0]["kind"], "two");
        assert_eq!(after[0]["sequence"], 2);
        assert!(log.after(2).is_empty());
    }

    #[test]
    fn log_drops_old_events_without_reusing_sequences() {
        let mut log = Log::default();
        for _ in 0..EVENT_LIMIT + 1 {
            log.record("tick", json!({}));
        }
        assert!(log.events.len() <= EVENT_LIMIT);
        assert_eq!(log.sequence, (EVENT_LIMIT + 1) as u64);
        assert_eq!(
            log.events.last().unwrap()["sequence"],
            (EVENT_LIMIT + 1) as u64
        );
    }

    #[tokio::test]
    async fn tcp_endpoint_relays_records_and_goes_down_and_up_on_the_same_port() {
        let log: SharedLog = Arc::default();
        let upstream = tcp_echo().await;
        let endpoint = Endpoint::new(
            "ssh1",
            Transport::Tcp,
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            upstream.to_string(),
        );
        let port = start_endpoint(&endpoint, &log).await;
        assert_eq!(round_trip(port, b"hello").await.unwrap(), b"hello");
        let connect = wait_for(&log, "connect", 0).await;
        assert_eq!(connect["client"], "127.0.0.1");
        assert_eq!(connect["endpoint"], "ssh1");
        wait_for(&log, "disconnect", 0).await;

        let mark = log.lock().unwrap().sequence;
        endpoint.set_up(false);
        wait_for(&log, "endpoint-down", mark).await;
        assert!(
            TcpStream::connect((Ipv4Addr::LOCALHOST, port))
                .await
                .is_err()
        );

        let mark = log.lock().unwrap().sequence;
        endpoint.set_up(true);
        let up = wait_for(&log, "endpoint-up", mark).await;
        assert_eq!(up["port"], u64::from(port));
        assert_eq!(round_trip(port, b"again").await.unwrap(), b"again");
    }

    #[tokio::test]
    async fn cut_ends_live_sessions_and_keeps_listening() {
        let log: SharedLog = Arc::default();
        let upstream = tcp_echo().await;
        let endpoint = Endpoint::new(
            "ssh2",
            Transport::Tcp,
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            upstream.to_string(),
        );
        let port = start_endpoint(&endpoint, &log).await;
        let mut live = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap();
        live.write_all(b"x").await.unwrap();
        let mut byte = [0u8; 1];
        live.read_exact(&mut byte).await.unwrap();
        let mark = log.lock().unwrap().sequence;
        endpoint.cut();
        wait_for(&log, "disconnect", mark).await;
        let mut rest = Vec::new();
        assert_eq!(live.read_to_end(&mut rest).await.unwrap_or(0), 0);
        assert_eq!(endpoint.active.load(Ordering::SeqCst), 0);
        assert_eq!(round_trip(port, b"still").await.unwrap(), b"still");
    }

    #[tokio::test]
    async fn udp_endpoint_gives_each_client_its_own_flow() {
        let log: SharedLog = Arc::default();
        let echo = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let echo_addr = echo.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buffer = [0u8; 1500];
            while let Ok((length, peer)) = echo.recv_from(&mut buffer).await {
                let _ = echo.send_to(&buffer[..length], peer).await;
            }
        });
        let endpoint = Endpoint::new(
            "wg1",
            Transport::Udp,
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            echo_addr.to_string(),
        );
        let port = start_endpoint(&endpoint, &log).await;
        for payload in [b"first".as_slice(), b"second".as_slice()] {
            let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            client.connect((Ipv4Addr::LOCALHOST, port)).await.unwrap();
            client.send(payload).await.unwrap();
            let mut buffer = [0u8; 64];
            let length = client.recv(&mut buffer).await.unwrap();
            assert_eq!(&buffer[..length], payload);
        }
        let connects = log
            .lock()
            .unwrap()
            .after(0)
            .into_iter()
            .filter(|event| event["kind"] == "connect")
            .count();
        assert_eq!(connects, 2);
        assert_eq!(endpoint.active.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_peer_is_published_only_once_its_resolver_answers_through_a_tunnel() {
        let peer = WireGuardTestPeer::start_for_downloads(
            WireGuardTestPeerOptions {
                names: [(READY_NAME.to_string(), vec![IpAddr::V4(TEST_PEER_ADDRESS)])]
                    .into_iter()
                    .collect(),
                preshared_key: Some(test_preshared_key()),
                ..WireGuardTestPeerOptions::default()
            },
            None,
            Duration::ZERO,
            false,
        )
        .await;
        assert!(dns_serving(&peer, "wg-test").await >= 1);
        assert_eq!(peer.dns_queries(), vec![READY_NAME.to_string()]);
    }

    #[tokio::test]
    async fn control_rejects_unknown_endpoints_and_misplaced_host_keys() {
        let log: SharedLog = Arc::default();
        let endpoint = Endpoint::new(
            "ssh1",
            Transport::Tcp,
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            "127.0.0.1:9".to_string(),
        );
        let mut endpoints = BTreeMap::new();
        endpoints.insert("ssh1".to_string(), endpoint);
        let fixture = Fixture {
            log,
            endpoints,
            ssh: Vec::new(),
            switch_targets: HashMap::new(),
            wireguard: Vec::new(),
        };
        let command = |endpoint: &str, host_key: Option<&str>| Command {
            endpoint: endpoint.to_string(),
            up: None,
            cut: None,
            host_key: host_key.map(str::to_string),
        };
        assert!(
            apply(&fixture, command("nope", None))
                .unwrap_err()
                .contains("unknown endpoint")
        );
        assert!(
            apply(&fixture, command("ssh1", Some("other")))
                .unwrap_err()
                .contains("only to ssh-switch")
        );
        assert!(
            apply(
                &fixture,
                Command {
                    up: Some(false),
                    ..command("ssh1", None)
                }
            )
            .is_ok()
        );
        assert!(!*fixture.endpoints["ssh1"].up.borrow());
    }
}
