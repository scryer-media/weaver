// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::test_support::{
    ScriptedStep, read_command_line, spawn_scripted_server as spawn_shared_scripted_server,
};
use crate::transfer::StableServerId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod strict_priority;

struct ScriptStep {
    expect_prefix: Option<&'static str>,
    response: &'static [u8],
}

impl ScriptedStep for ScriptStep {
    fn expected_prefix(&self) -> Option<&str> {
        self.expect_prefix
    }

    fn response(&self) -> &[u8] {
        self.response
    }
}

async fn spawn_scripted_server(steps: Vec<ScriptStep>) -> u16 {
    spawn_shared_scripted_server(steps, Duration::ZERO).await
}

// Answers a BODY with nothing at all, holding the session open until the
// client gives up on it.
async fn spawn_delayed_body_initial_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"200 ready\r\n").await.unwrap();

        let capabilities = read_command_line(&mut socket).await;
        assert!(capabilities.starts_with("CAPABILITIES"));
        socket
            .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
            .await
            .unwrap();

        let body = read_command_line(&mut socket).await;
        assert!(body.starts_with("BODY "));
        let _ = socket.read_to_end(&mut Vec::new()).await;
    });

    port
}

// Asks for re-authentication mid-session, then never answers the AUTHINFO,
// holding the session open until the client gives up on it.
async fn spawn_delayed_reauth_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"200 ready\r\n").await.unwrap();

        let initial_user = read_command_line(&mut socket).await;
        assert!(initial_user.starts_with("AUTHINFO USER "));
        socket
            .write_all(b"281 authentication accepted\r\n")
            .await
            .unwrap();

        let authenticated_capabilities = read_command_line(&mut socket).await;
        assert!(authenticated_capabilities.starts_with("CAPABILITIES"));
        socket
            .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
            .await
            .unwrap();

        let body = read_command_line(&mut socket).await;
        assert!(body.starts_with("BODY "));
        socket
            .write_all(b"480 authentication required\r\n")
            .await
            .unwrap();

        let user = read_command_line(&mut socket).await;
        assert!(user.starts_with("AUTHINFO USER "));
        let _ = socket.read_to_end(&mut Vec::new()).await;
    });

    port
}

// Longer than any test runs. A script answers, closes or deliberately goes
// silent, so a connection's own timeouts only decide a test that sets them
// itself, never a slow runner.
const UNREACHED_TIMEOUT: Duration = Duration::from_secs(3600);

fn scripted_server(port: u16, group: usize) -> ServerPoolConfig {
    ServerPoolConfig {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: false,
            connect_timeout: UNREACHED_TIMEOUT,
            command_timeout: UNREACHED_TIMEOUT,
            ..Default::default()
        },
        max_connections: 2,
        group: group as u32,
        ..ServerPoolConfig::default()
    }
}

fn scripted_blocking_s2n_server(port: u16, max_connections: usize) -> ServerPoolConfig {
    ServerPoolConfig {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: true,
            tls_ca_cert: Some(std::path::PathBuf::from("/tmp/weaver-test-ca.pem")),
            connect_timeout: Duration::from_secs(1),
            command_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        max_connections,
        group: 0,
        ..ServerPoolConfig::default()
    }
}

#[test]
fn blocking_body_lane_candidate_honors_exclusions_and_capacity() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(1, 2), scripted_server(2, 0)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });

    assert!(client.has_blocking_body_lane_candidate(crate::ServerSet::EMPTY));
    assert!(
        client.has_blocking_body_lane_candidate(crate::ServerSet::from([0])),
        "a plaintext server is as good an owned-lane host as a TLS one"
    );
    assert!(!client.has_blocking_body_lane_candidate(crate::ServerSet::from([0, 1])));

    let saturated = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(1, 0)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });
    assert!(!saturated.has_blocking_body_lane_candidate(crate::ServerSet::EMPTY));
}

#[test]
fn blocking_body_lane_candidate_survives_temporary_permit_saturation() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(1, 2)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });
    let first = client
        .pool()
        .try_acquire_blocking_permit(ServerId(0))
        .unwrap();
    let second = client
        .pool()
        .try_acquire_blocking_permit(ServerId(0))
        .unwrap();

    assert_eq!(client.pool().server_load(0), (0, 2));
    assert!(
        client.has_blocking_body_lane_candidate(crate::ServerSet::EMPTY),
        "leased permits can belong to reusable owned-lane caches"
    );

    drop((first, second));
}

#[test]
fn blocking_body_lane_candidate_survives_the_over_limit_holdoff() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(1, 2)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });

    client.pool().note_provider_over_limit(ServerId(0));

    assert!(client.pool().is_over_limit(ServerId(0)));
    assert!(
        client.has_blocking_body_lane_candidate(crate::ServerSet::EMPTY),
        "a held-off server must keep feeding the owned lanes it already opened"
    );
}

#[test]
fn blocking_body_lane_candidate_keeps_backfill_locked_until_fill_excluded() {
    // The fill server is allowed no connections at all, so the only lane
    // candidate is a backfill server — and that must stay unreachable for
    // ordinary work.
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            ServerPoolConfig {
                max_connections: 0,
                ..scripted_server(1, 0)
            },
            ServerPoolConfig {
                backfill: true,
                ..scripted_blocking_s2n_server(2, 2)
            },
        ],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });

    assert!(
        !client.has_blocking_body_lane_candidate(crate::ServerSet::EMPTY),
        "locked backfill must not serve ordinary work"
    );
    assert!(
        client.has_blocking_body_lane_candidate(crate::ServerSet::from([0])),
        "excluding the fill tier unlocks backfill"
    );
    assert!(!client.has_blocking_body_lane_candidate(crate::ServerSet::from([0, 1])));
}

#[test]
fn client_config_single() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            port: 563,
            tls: true,
            ..Default::default()
        },
        10,
    );
    assert_eq!(config.servers.len(), 1);
    assert_eq!(config.servers[0].max_connections, 10);
    assert_eq!(config.max_idle_age, Duration::from_secs(300));
}

#[test]
fn client_creation() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    let client = NntpClient::new(config);
    assert_eq!(client.pool().server_count(), 1);
}

#[test]
fn infrastructure_admission_failures_do_not_poison_server_health() {
    let client = NntpClient::new(NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    ));

    let permit = client
        .pool()
        .try_acquire_blocking_permit(ServerId(0))
        .unwrap();
    let ticket = &permit.health_lease.0;
    for error in [
        NntpError::PoolExhausted,
        NntpError::PoolShutdown,
        NntpError::AcquireTimeout(15),
    ] {
        client.record_blocking_connect_failure(
            0,
            crate::pool::FreshConnectAdmission::Open,
            &error,
            ticket,
        );
    }
    assert_eq!(
        client
            .pool()
            .health()
            .blocking_lock()
            .server(0)
            .failure_count,
        0
    );

    client.record_blocking_connect_failure(
        0,
        crate::pool::FreshConnectAdmission::Open,
        &NntpError::SoftTimeout(15),
        ticket,
    );
    assert_eq!(
        client
            .pool()
            .health()
            .blocking_lock()
            .server(0)
            .failure_count,
        1
    );
}

const READER_CAPABILITIES: &[u8] = b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n";

// A dial the provider declined is paced by the holdoff alone. A lane asked
// for on that server inside the window learns it is over its limit, not
// that it is recovering, and nothing dials it again.
#[tokio::test]
async fn declined_dial_holds_blocking_lanes_on_the_holdoff() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let accepted_by_server = Arc::clone(&accepted);
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            accepted_by_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::spawn(async move {
                let _ = socket.write_all(b"502 Too Many Connections\r\n").await;
            });
        }
    });
    let client = NntpClient::new(NntpClientConfig::single(
        ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: false,
            ..Default::default()
        },
        4,
    ));

    let client = tokio::task::spawn_blocking(move || {
        let Err(declined) = client.try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
        else {
            panic!("the provider declines the dial");
        };
        assert!(
            matches!(
                declined,
                BlockingBodyLaneAcquireError::ProviderCapacity(NntpError::TooManyConnections)
            ),
            "{declined:?}"
        );
        assert!(client.pool().is_over_limit(ServerId(0)));
        for _ in 0..4 {
            let Err(error) = client.try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            else {
                panic!("a held-off server must not open a lane");
            };
            assert!(
                matches!(
                    error,
                    BlockingBodyLaneAcquireError::ProviderCapacity(
                        NntpError::ServerOverLimit { .. }
                    )
                ),
                "{error:?}"
            );
        }
        client
    })
    .await
    .unwrap();
    assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        client.pool().health().lock().await.server(0).failure_count,
        0
    );
    server.abort();
}

#[tokio::test]
async fn blocking_tls_capacity_rejection_parks_connects_without_health_poisoning() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(563, 8)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });

    client.record_blocking_connect_failure(
        0,
        crate::pool::FreshConnectAdmission::Open,
        &NntpError::TooManyConnections,
        &crate::pool::BlockingConnectionPermit::for_tests()
            .health_lease
            .0,
    );

    assert_eq!(client.pool().configured_connections(ServerId(0)), Some(8));
    assert!(client.pool().is_over_limit(ServerId(0)));
    let health = client.pool().health().lock().await;
    assert_eq!(health.server(0).failure_count, 0);
    assert_eq!(health.server(0).consecutive_failures, 0);
}

#[tokio::test]
async fn blocking_capacity_holdoff_never_cools_healthy_server() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(563, 2)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });

    client.record_blocking_connect_failure(
        0,
        crate::pool::FreshConnectAdmission::Open,
        &NntpError::TooManyConnections,
        &crate::pool::BlockingConnectionPermit::for_tests()
            .health_lease
            .0,
    );
    client.record_blocking_connect_failure(
        0,
        crate::pool::FreshConnectAdmission::Open,
        &NntpError::TooManyConnections,
        &crate::pool::BlockingConnectionPermit::for_tests()
            .health_lease
            .0,
    );

    assert_eq!(client.pool().configured_connections(ServerId(0)), Some(2));
    assert!(client.pool().is_over_limit(ServerId(0)));
    let mut health = client.pool().health().lock().await;
    assert_eq!(health.server(0).state(), &ServerState::Healthy);
    assert!(health.is_available(0));
}

// A provider that turns a dial away with 502 parks fresh lanes on that
// server only: the next lane meets the holdoff without a socket, the lanes
// already open keep fetching, the configured limit stands, and a peer server
// is untouched.
#[test]
fn blocking_provider_rejection_parks_fresh_lanes_for_that_server_only() {
    use std::io::{BufRead, BufReader, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_by_server = Arc::clone(&accepted);
    std::thread::spawn(move || {
        for socket in listener.incoming() {
            let Ok(mut socket) = socket else { return };
            let slot = accepted_by_server.fetch_add(1, Ordering::SeqCst);
            std::thread::spawn(move || {
                if slot >= 2 {
                    let _ = socket.write_all(b"502 Too Many Connections\r\n");
                    return;
                }
                socket.write_all(b"200 test server ready\r\n").unwrap();
                let reader = BufReader::new(socket.try_clone().unwrap());
                for line in reader.lines() {
                    let Ok(line) = line else { return };
                    let reply: &[u8] = if line == "CAPABILITIES" {
                        READER_CAPABILITIES
                    } else if line.starts_with("BODY ") {
                        b"222 1 <test> body follows\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n"
                    } else if line == "QUIT" {
                        let _ = socket.write_all(b"205 closing\r\n");
                        return;
                    } else {
                        b"500 unsupported\r\n"
                    };
                    if socket.write_all(reply).is_err() {
                        return;
                    }
                }
            });
        }
    });

    let mut capped = scripted_server(port, 0);
    capped.max_connections = 8;
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            capped,
            ServerPoolConfig {
                server: ServerConfig {
                    host: "backup.example.com".into(),
                    ..Default::default()
                },
                max_connections: 4,
                group: 0,
                ..ServerPoolConfig::default()
            },
        ],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: UNREACHED_TIMEOUT,
    });
    // The peer is excluded throughout, so nothing ever dials it.
    let only_capped = ServerSet::from([1]);

    let mut open: Vec<_> = (0..2)
        .map(|_| {
            client
                .try_acquire_blocking_body_lane(&[], only_capped)
                .expect("the provider admits its first two connections")
        })
        .collect();
    let refused = client
        .try_acquire_blocking_body_lane(&[], only_capped)
        .err()
        .expect("the third connection is refused");
    assert!(
        matches!(
            refused,
            BlockingBodyLaneAcquireError::ProviderCapacity(NntpError::TooManyConnections)
        ),
        "{refused:?}"
    );
    assert!(client.pool().is_over_limit(ServerId(0)));
    let accepted_after_rejection = accepted.load(Ordering::SeqCst);
    assert_eq!(accepted_after_rejection, 3);

    let held = client
        .try_acquire_blocking_body_lane(&[], only_capped)
        .err()
        .expect("a held-off server opens no lane");
    assert!(
        matches!(
            held,
            BlockingBodyLaneAcquireError::ProviderCapacity(NntpError::ServerOverLimit { .. })
        ),
        "{held:?}"
    );
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        accepted_after_rejection,
        "a held-off server must not open another socket"
    );
    // The provider refused connections, not the configuration.
    assert_eq!(client.pool().configured_connections(ServerId(0)), Some(8));
    assert_eq!(client.pool().server_load(0), (6, 8));

    // Lanes already open are untouched by the holdoff.
    for lane in &mut open {
        let trace = lane.fetch_decoded_sequential("<test>");
        client.record_blocking_attempts(&trace.attempts);
        let decoded = trace.result.expect("an open lane keeps fetching");
        let bytes: Vec<u8> = decoded
            .decoded
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect();
        assert_eq!(bytes, b"A");
    }
    assert_eq!(accepted.load(Ordering::SeqCst), accepted_after_rejection);
    assert_eq!(
        client
            .pool()
            .health()
            .blocking_lock()
            .server(0)
            .failure_count,
        0
    );

    // A peer server that never rejected anything is unaffected.
    assert!(!client.pool().is_over_limit(ServerId(1)));
    assert_eq!(client.pool().server_load(1), (4, 4));
    drop(open);
}

#[test]
fn blocking_lane_preserves_provider_capacity_rejection() {
    let error = NntpError::from_status(crate::types::StatusCode::new(502), "Too many connections");
    let error = BlockingBodyLaneAcquireError::from_connect_error(ServerId(0), error);

    assert!(
        matches!(
            error,
            BlockingBodyLaneAcquireError::ProviderCapacity(NntpError::TooManyConnections)
        ),
        "unexpected blocking lane error: {error:?}"
    );
}

// A route that refuses every dial with `refusal`: the tunnel behind the
// server is full, or its session budget spent. Nothing reaches a server.
struct FullRoute(fn() -> weaver_tunnel::pipe::DialError);

#[async_trait::async_trait]
impl weaver_tunnel::pipe::Dialer for FullRoute {
    async fn dial(
        &self,
        _: &weaver_tunnel::pipe::Target,
    ) -> std::result::Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
        Err((self.0)())
    }
    fn budget(&self) -> Duration {
        UNREACHED_TIMEOUT
    }
    fn describe(&self) -> String {
        "full route fixture".into()
    }
}

// A client of one server behind a route that refuses with `refusal`. The
// blocking dial runs on a runtime of its own, as the download lanes' does.
fn client_behind_full_route(
    runtime: &tokio::runtime::Runtime,
    refusal: fn() -> weaver_tunnel::pipe::DialError,
) -> NntpClient {
    let dialer = Arc::new(crate::route_dialer::RouteDialer {
        inner: Arc::new(FullRoute(refusal)),
        plan: None,
        egress_controls: Arc::new(crate::transfer::ServerTransferRegistry::new()),
        runtime: runtime.handle().clone(),
        server: 1,
    });
    let mut server = scripted_server(119, 0);
    server.server.dialer = dialer;
    NntpClient::new(NntpClientConfig {
        servers: vec![server],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    })
}

// A full tunnel, or a spent WireGuard session budget, is room the route lacks,
// not a fault of the server: the lane is refused for capacity, the work is
// handed back, and the server keeps a clean health record and every permit.
#[test]
fn a_full_route_refuses_the_lane_for_capacity_and_spares_the_server() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let refusals: [fn() -> weaver_tunnel::pipe::DialError; 2] = [
        || weaver_tunnel::pipe::DialError::AtCapacity(Default::default()),
        || weaver_tunnel::pipe::DialError::SessionBudget,
    ];
    for refusal in refusals {
        let client = client_behind_full_route(&runtime, refusal);
        let (free_before, configured) = client.pool().server_load(0);
        let Err(error) =
            client.try_acquire_blocking_body_lane_with_estimate(&[], crate::ServerSet::EMPTY, 0)
        else {
            panic!("a full route opens no lane");
        };
        assert!(
            matches!(&error, BlockingBodyLaneAcquireError::RouteCapacity(inner) if inner.is_route_capacity()),
            "{error:?}"
        );
        assert!(error.is_capacity_admission());
        assert!(error.should_requeue_owned_work());
        assert!(error.is_route_capacity());
        assert_eq!(error.kind(), "route_capacity");
        assert!(error.server().is_none());
        let health = client.pool().health().blocking_lock();
        assert_eq!(health.server(0).failure_count, 0);
        assert!(matches!(health.server(0).state(), ServerState::Healthy));
        drop(health);
        assert_eq!(client.pool().server_load(0), (free_before, configured));
        assert!(!client.pool().is_over_limit(ServerId(0)));
    }
}

// Every other route refusal keeps its class: a leg that is down is not room
// the route lacks.
#[test]
fn a_route_fault_is_not_route_capacity() {
    let fault = NntpError::Route(Arc::new(weaver_tunnel::pipe::DialError::Egress(
        std::io::Error::other("leg down"),
    )));
    assert!(!fault.is_route_capacity());
    let error = BlockingBodyLaneAcquireError::from_connect_error(ServerId(0), fault);
    assert!(
        matches!(error, BlockingBodyLaneAcquireError::Other(_)),
        "{error:?}"
    );
    assert!(!error.should_requeue_owned_work());
    let skipped = NntpError::Route(Arc::new(weaver_tunnel::pipe::DialError::Skipped(
        "leg revoked".into(),
    )));
    assert!(matches!(
        BlockingBodyLaneAcquireError::from_connect_error(ServerId(0), skipped),
        BlockingBodyLaneAcquireError::Other(_)
    ));
}

// A client of two servers, each behind a route that refuses with its own
// refusal.
fn client_behind_two_routes(
    runtime: &tokio::runtime::Runtime,
    refusals: [fn() -> weaver_tunnel::pipe::DialError; 2],
) -> NntpClient {
    let servers = refusals
        .into_iter()
        .enumerate()
        .map(|(index, refusal)| {
            let mut server = scripted_server(119 + index as u16, 0);
            server.server.dialer = Arc::new(crate::route_dialer::RouteDialer {
                inner: Arc::new(FullRoute(refusal)),
                plan: None,
                egress_controls: Arc::new(crate::transfer::ServerTransferRegistry::new()),
                runtime: runtime.handle().clone(),
                server: index as u32 + 1,
            });
            server
        })
        .collect();
    NntpClient::new(NntpClientConfig {
        servers,
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    })
}

// A server that failed for real is the answer over another whose route is
// full, whichever is asked first, as a failed rung outranks a full one in a
// route's ladder. With every route full, the answer is the full route.
#[test]
fn a_real_fault_outranks_a_full_route_and_two_full_routes_wait() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let full: fn() -> weaver_tunnel::pipe::DialError =
        || weaver_tunnel::pipe::DialError::AtCapacity(Default::default());
    let refused: fn() -> weaver_tunnel::pipe::DialError = || {
        weaver_tunnel::pipe::DialError::Refused(std::io::Error::from(
            std::io::ErrorKind::ConnectionRefused,
        ))
    };
    for refusals in [[full, refused], [refused, full]] {
        let client = client_behind_two_routes(&runtime, refusals);
        let Err(error) =
            client.try_acquire_blocking_body_lane_with_estimate(&[], crate::ServerSet::EMPTY, 0)
        else {
            panic!("no route opens a lane");
        };
        assert!(
            matches!(
                &error,
                BlockingBodyLaneAcquireError::Other(NntpError::Route(route))
                    if matches!(route.as_ref(), weaver_tunnel::pipe::DialError::Refused(_))
            ),
            "{error:?}"
        );
        assert!(!error.should_requeue_owned_work());
    }
    let client = client_behind_two_routes(&runtime, [full, full]);
    let Err(error) =
        client.try_acquire_blocking_body_lane_with_estimate(&[], crate::ServerSet::EMPTY, 0)
    else {
        panic!("no route opens a lane");
    };
    assert!(error.is_route_capacity(), "{error:?}");
    assert!(error.should_requeue_owned_work());
}

// Gives the sides nothing: every usenet socket is refused, and presses.
struct NoRoom;

impl weaver_tunnel::pipe::SocketDivider for NoRoom {
    fn shares(&self, _: u64) -> [u64; weaver_tunnel::pipe::SIDES] {
        [0; weaver_tunnel::pipe::SIDES]
    }

    fn share_out(&self, _: u64, shares: &[u64], _: &[Option<u64>], _: &[u64]) -> Vec<u64> {
        shares.to_vec()
    }

    fn presses(&self, used: u64, given: u64) -> bool {
        used >= given
    }
}

fn full_tunnel() -> Arc<weaver_tunnel::pipe::SessionSockets> {
    const MTU: u16 = 1420;
    Arc::new(weaver_tunnel::pipe::SessionSockets::new(
        weaver_tunnel::pipe::SocketBudget {
            bytes: weaver_tunnel::pipe::overhead_bytes(MTU),
            reserve: 0,
            torrent_kept: 0,
            mtu: MTU,
        },
        Arc::new(NoRoom),
    ))
}

// A route whose tunnel has no room: each dial is refused by the tunnel's
// ledger, and the refusal carries the press it leaves.
struct LedgerRoute(Arc<weaver_tunnel::pipe::SessionSockets>);

#[async_trait::async_trait]
impl weaver_tunnel::pipe::Dialer for LedgerRoute {
    async fn dial(
        &self,
        _: &weaver_tunnel::pipe::Target,
    ) -> std::result::Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
        let refused = match self.0.take(
            weaver_tunnel::pipe::SocketClass::Nntp,
            weaver_tunnel::pipe::stream_bytes(None),
        ) {
            Err(refused) => refused,
            Ok(_) => panic!("a tunnel with no room hands out no socket"),
        };
        Err(weaver_tunnel::pipe::DialError::AtCapacity(refused.into()))
    }
    fn budget(&self) -> Duration {
        UNREACHED_TIMEOUT
    }
    fn describe(&self) -> String {
        "ledger route fixture".into()
    }
}

// A client of two servers, each behind its own full tunnel.
fn client_behind_two_tunnels(
    runtime: &tokio::runtime::Runtime,
    tunnels: &[Arc<weaver_tunnel::pipe::SessionSockets>; 2],
) -> NntpClient {
    let servers = tunnels
        .iter()
        .enumerate()
        .map(|(index, tunnel)| {
            let mut server = scripted_server(119 + index as u16, 0);
            server.server.dialer = Arc::new(crate::route_dialer::RouteDialer {
                inner: Arc::new(LedgerRoute(Arc::clone(tunnel))),
                plan: None,
                egress_controls: Arc::new(crate::transfer::ServerTransferRegistry::new()),
                runtime: runtime.handle().clone(),
                server: index as u32 + 1,
            });
            server
        })
        .collect();
    NntpClient::new(NntpClientConfig {
        servers,
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    })
}

// Every tunnel that refused a lane keeps its press while the work waits, not
// only the last one asked, and whichever error answers. The press ends when
// the work drops its demand.
#[test]
fn every_tunnel_that_refused_a_lane_keeps_its_press() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let usenet = weaver_tunnel::pipe::SocketClass::Nntp;

    // Both tunnels full: the route's error answers, and both press.
    let tunnels = [full_tunnel(), full_tunnel()];
    let client = client_behind_two_tunnels(&runtime, &tunnels);
    let mut route_demand = Vec::new();
    let Err(error) = client.try_acquire_blocking_body_lane_pressing(
        &[],
        crate::ServerSet::EMPTY,
        0,
        &mut route_demand,
    ) else {
        panic!("no tunnel opens a lane");
    };
    assert!(error.is_route_capacity(), "{error:?}");
    assert!(tunnels.iter().all(|tunnel| tunnel.pressing(usenet)));
    drop(error);
    assert!(
        tunnels.iter().all(|tunnel| tunnel.pressing(usenet)),
        "the work's demand holds both presses"
    );
    drop(route_demand);
    assert!(tunnels.iter().all(|tunnel| !tunnel.pressing(usenet)));

    // A's tunnel full and B at its provider's limit: the provider's error
    // answers, as before, and A's tunnel still presses.
    let tunnels = [full_tunnel(), full_tunnel()];
    let client = client_behind_two_tunnels(&runtime, &tunnels);
    client.pool().note_provider_over_limit(ServerId(1));
    let mut route_demand = Vec::new();
    let Err(error) = client.try_acquire_blocking_body_lane_pressing(
        &[],
        crate::ServerSet::EMPTY,
        0,
        &mut route_demand,
    ) else {
        panic!("no server opens a lane");
    };
    assert!(
        matches!(error, BlockingBodyLaneAcquireError::ProviderCapacity(_)),
        "{error:?}"
    );
    drop(error);
    assert!(tunnels[0].pressing(usenet), "A's refusal still presses");
    assert!(!tunnels[1].pressing(usenet), "B was never dialled");
    drop(route_demand);
    assert!(!tunnels[0].pressing(usenet));
}

#[test]
fn transient_failure_recording_counts_soft_timeouts() {
    assert_eq!(
        cooldown_reason(&NntpError::SoftTimeout(15)),
        Some(CooldownReason::Transport)
    );
    assert_eq!(cooldown_reason(&NntpError::TooManyConnections), None);
    assert_eq!(cooldown_reason(&NntpError::PoolExhausted), None);
    assert_eq!(
        cooldown_reason(&NntpError::Timeout),
        Some(CooldownReason::Transport)
    );
}

// Failing to obtain a connection is local capacity, not a server fault:
// it must never cool the server down.
#[test]
fn acquire_timeout_is_capacity_not_transport() {
    assert_eq!(cooldown_reason(&NntpError::AcquireTimeout(15)), None);
    assert_eq!(
        NntpError::AcquireTimeout(15).to_string(),
        "no connection available within 15s"
    );
    // Zero is not a duration: it is the pool declining to wait at all.
    assert_eq!(
        NntpError::AcquireTimeout(0).to_string(),
        "no connection available; did not wait"
    );
}

#[test]
fn soft_timeout_display() {
    let err = NntpError::SoftTimeout(15);
    assert_eq!(err.to_string(), "article fetch soft timeout (15s)");
}

#[test]
fn client_config_default_soft_timeout() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    assert_eq!(config.soft_timeout, Duration::from_secs(15));
}

#[test]
fn client_soft_timeout_propagated() {
    let mut config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    config.soft_timeout = Duration::from_secs(30);
    let client = NntpClient::new(config);
    assert_eq!(client.soft_timeout, Duration::from_secs(30));
}

#[test]
fn from_pool_default_soft_timeout() {
    let config = NntpClientConfig::single(
        ServerConfig {
            host: "news.example.com".into(),
            ..Default::default()
        },
        5,
    );
    let pool = Arc::new(NntpPool::new(PoolConfig {
        servers: config.servers,
        max_idle_age: config.max_idle_age,
        ..PoolConfig::default()
    }));
    let client = NntpClient::from_pool(pool);
    assert_eq!(client.soft_timeout, Duration::from_secs(15));
}

// Build a multi-server client for testing server ordering.
fn multi_server_client(server_count: usize) -> NntpClient {
    let servers: Vec<ServerPoolConfig> = (0..server_count)
        .map(|i| ServerPoolConfig {
            server: ServerConfig {
                host: format!("server{i}.example.com"),
                ..Default::default()
            },
            max_connections: 10,
            group: if i < 2 { 0 } else { 1 }, // first 2 in group 0, rest in group 1
            ..ServerPoolConfig::default()
        })
        .collect();

    let pool = NntpPool::new(PoolConfig {
        servers,
        max_idle_age: Duration::from_secs(300),
        ..PoolConfig::default()
    });

    NntpClient {
        pool: Arc::new(pool),
        soft_timeout: Duration::from_secs(15),
    }
}

#[tokio::test]
async fn build_server_order_respects_priority_groups() {
    let client = multi_server_client(4);

    let order = client.build_server_order(crate::ServerSet::EMPTY).await;
    // All 4 servers should be present.
    assert_eq!(order.len(), 4);
    // Group 0 servers (0, 1) should come before group 1 servers (2, 3).
    let group0_positions: Vec<usize> = order
        .iter()
        .position(|&x| x == 0)
        .into_iter()
        .chain(order.iter().position(|&x| x == 1))
        .collect();
    let group1_positions: Vec<usize> = order
        .iter()
        .position(|&x| x == 2)
        .into_iter()
        .chain(order.iter().position(|&x| x == 3))
        .collect();

    let max_group0 = *group0_positions.iter().max().unwrap();
    let min_group1 = *group1_positions.iter().min().unwrap();
    assert!(
        max_group0 < min_group1,
        "group 0 servers should all come before group 1 servers"
    );
}

#[tokio::test]
async fn build_server_order_excludes_servers() {
    let client = multi_server_client(4);

    let order = client
        .build_server_order(crate::ServerSet::from([1, 3]))
        .await;
    assert_eq!(order.len(), 2);
    assert!(!order.contains(&1));
    assert!(!order.contains(&3));
    assert!(order.contains(&0));
    assert!(order.contains(&2));
}

// Servers: 0 = fill (group 0), 1 = fill (group 1), 2 = backfill (group 0).
fn tiered_client(backfill_flags: &[bool]) -> NntpClient {
    let servers: Vec<ServerPoolConfig> = backfill_flags
        .iter()
        .enumerate()
        .map(|(i, &backfill)| ServerPoolConfig {
            server: ServerConfig {
                host: format!("server{i}.example.com"),
                ..Default::default()
            },
            max_connections: 10,
            group: (i % 2) as u32,
            backfill,
            ..ServerPoolConfig::default()
        })
        .collect();
    let pool = NntpPool::new(PoolConfig {
        servers,
        ..PoolConfig::default()
    });
    NntpClient::from_pool(pool.into())
}

fn quota_selection_client(
    backfill_flags: &[bool],
    blocked: &[(usize, Option<Instant>)],
) -> NntpClient {
    let transfers = crate::transfer::ServerTransferRegistry::new();
    let servers = backfill_flags
        .iter()
        .enumerate()
        .map(|(idx, &backfill)| {
            let stable_id = StableServerId(1_000 + idx as u32);
            let transfer_control = blocked
                .iter()
                .find(|(blocked_idx, _)| *blocked_idx == idx)
                .map(|(_, retry_at)| {
                    transfers.configure(
                        stable_id,
                        crate::transfer::ServerTransferConfig {
                            closed: false,
                            rate_bytes_per_sec: 0,
                            quota: Some(crate::transfer::QuotaRuntimeConfig {
                                limit_bytes: 0,
                                generation: 1,
                                retry_at: *retry_at,
                            }),
                        },
                    )
                });
            ServerPoolConfig {
                server: ServerConfig {
                    host: format!("selection-{idx}.example.com"),
                    ..Default::default()
                },
                stable_id,
                transfer_control,
                max_connections: 1,
                group: 0,
                backfill,
                ..ServerPoolConfig::default()
            }
        })
        .collect();
    NntpClient::new(NntpClientConfig {
        servers,
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: UNREACHED_TIMEOUT,
    })
}

#[tokio::test]
async fn estimate_selection_fails_over_large_request_but_keeps_smaller_work_moving() {
    let transfers = crate::transfer::ServerTransferRegistry::new();
    let limited_id = StableServerId(2_000);
    let limited = transfers.configure(
        limited_id,
        crate::transfer::ServerTransferConfig {
            closed: false,
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 100,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    let mut used = limited.try_reserve(60).unwrap();
    used.record_blocking(60);
    used.finish();
    assert!(!limited.snapshot().quota_blocked);

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            ServerPoolConfig {
                server: ServerConfig {
                    host: "limited.example.com".into(),
                    ..Default::default()
                },
                stable_id: limited_id,
                transfer_control: Some(limited),
                max_connections: 1,
                group: 0,
                ..ServerPoolConfig::default()
            },
            ServerPoolConfig {
                server: ServerConfig {
                    host: "unlimited.example.com".into(),
                    ..Default::default()
                },
                stable_id: StableServerId(2_001),
                max_connections: 1,
                group: 1,
                ..ServerPoolConfig::default()
            },
        ],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: UNREACHED_TIMEOUT,
    });

    let large = client
        .body_server_selection_with_estimate(crate::ServerSet::EMPTY, 41)
        .await;
    assert_eq!(large.eligible, vec![ServerId(1)]);
    let rejection = large.quota_blocked.unwrap();
    assert_eq!(rejection.stable_server_id, limited_id);
    assert_eq!(rejection.requested_body_bytes, 41);
    assert!(!rejection.snapshot.quota_blocked);
    assert_eq!(
        client
            .server_quota_rejection(ServerId(0), 41)
            .unwrap()
            .requested_body_bytes,
        41
    );
    assert!(client.server_quota_rejection(ServerId(0), 40).is_none());
    assert!(client.server_quota_rejection(ServerId(1), 41).is_none());
    let blocking_large = client
        .try_blocking_body_server_selection_with_estimate(crate::ServerSet::EMPTY, 41)
        .expect("the health state is uncontended in this test");
    assert_eq!(blocking_large.eligible, vec![ServerId(1)]);
    assert_eq!(
        blocking_large.quota_blocked.unwrap().stable_server_id,
        limited_id
    );

    let small = client
        .body_server_selection_with_estimate(crate::ServerSet::EMPTY, 40)
        .await;
    assert_eq!(small.eligible, vec![ServerId(0), ServerId(1)]);
    assert!(small.quota_blocked.is_none());
    assert!(!client.all_normal_fill_servers_quota_blocked());
}

#[test]
fn global_quota_predicate_requires_every_normal_fill_and_ignores_backfill() {
    let blocked_fill = quota_selection_client(&[false, true], &[(0, None)]);
    assert!(blocked_fill.all_normal_fill_servers_quota_blocked());

    let mixed_fills = quota_selection_client(&[false, false], &[(0, None)]);
    assert!(!mixed_fills.all_normal_fill_servers_quota_blocked());

    let empty_pool = quota_selection_client(&[], &[]);
    assert!(!empty_pool.all_normal_fill_servers_quota_blocked());
}

#[test]
fn earliest_quota_selection_preserves_first_blocked_registry_revision() {
    let transfers = crate::transfer::ServerTransferRegistry::new();
    let later = Instant::now() + Duration::from_secs(120);
    let earlier = Instant::now() + Duration::from_secs(60);
    let first = transfers.configure(
        StableServerId(3_000),
        crate::transfer::ServerTransferConfig {
            closed: false,
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 0,
                generation: 1,
                retry_at: Some(later),
            }),
        },
    );
    let first_rejection = first.quota_rejection_for(1).unwrap();
    let second = transfers.configure(
        StableServerId(3_001),
        crate::transfer::ServerTransferConfig {
            closed: false,
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 0,
                generation: 1,
                retry_at: Some(earlier),
            }),
        },
    );
    let second_rejection = second.quota_rejection_for(1).unwrap();
    assert_ne!(
        first_rejection.registry_capacity_revision,
        second_rejection.registry_capacity_revision
    );

    let first_revision = first_rejection.registry_capacity_revision;
    let mut selected = None;
    retain_earliest_quota_rejection(&mut selected, first_rejection);
    retain_earliest_quota_rejection(&mut selected, second_rejection);

    let selected = selected.unwrap();
    assert_eq!(selected.stable_server_id, StableServerId(3_001));
    assert_eq!(selected.retry_at, Some(earlier));
    assert_eq!(selected.registry_capacity_revision, first_revision);
}

#[tokio::test]
async fn body_selection_reports_earliest_quota_when_all_fills_are_blocked() {
    let later = Instant::now() + Duration::from_secs(120);
    let earlier = Instant::now() + Duration::from_secs(60);
    let client = quota_selection_client(&[false, false], &[(0, Some(later)), (1, Some(earlier))]);

    let selection = client.body_server_selection(crate::ServerSet::EMPTY).await;
    assert!(selection.eligible.is_empty());
    let rejection = selection
        .quota_blocked
        .expect("quota block must survive filtering");
    assert_eq!(rejection.stable_server_id, StableServerId(1_001));
    assert_eq!(rejection.retry_at, Some(earlier));
    assert!(
        client
            .body_server_order(crate::ServerSet::EMPTY)
            .await
            .is_empty()
    );

    let blocking = client
        .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
        .err()
        .expect("blocking selection must surface the same quota block");
    assert!(matches!(
        blocking,
        BlockingBodyLaneAcquireError::Other(NntpError::QuotaBlocked(rejection))
            if rejection.stable_server_id == StableServerId(1_001)
                && rejection.retry_at == Some(earlier)
    ));
}

#[tokio::test]
async fn body_selection_keeps_quota_reason_when_other_fill_is_health_unavailable() {
    let retry_at = Instant::now() + Duration::from_secs(60);
    let client = quota_selection_client(&[false, false], &[(0, Some(retry_at))]);
    client
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(1, CooldownReason::Transport);

    let selection = client.body_server_selection(crate::ServerSet::EMPTY).await;
    assert!(selection.eligible.is_empty());
    assert_eq!(
        selection.quota_blocked.unwrap().stable_server_id,
        StableServerId(1_000)
    );
}

#[tokio::test]
async fn body_selection_never_reports_quota_from_excluded_servers() {
    let first = Instant::now() + Duration::from_secs(30);
    let second = Instant::now() + Duration::from_secs(60);
    let client = quota_selection_client(&[false, false], &[(0, Some(first)), (1, Some(second))]);

    let selection = client
        .body_server_selection(crate::ServerSet::from([0]))
        .await;
    assert!(selection.eligible.is_empty());
    assert_eq!(
        selection.quota_blocked.unwrap().stable_server_id,
        StableServerId(1_001)
    );

    let selection = client
        .body_server_selection(crate::ServerSet::from([0, 1]))
        .await;
    assert!(selection.eligible.is_empty());
    assert!(selection.quota_blocked.is_none());
}

#[tokio::test]
async fn quota_blocked_fill_does_not_unlock_or_report_locked_backfill() {
    let retry_at = Instant::now() + Duration::from_secs(60);
    let client = quota_selection_client(&[false, true], &[(0, Some(retry_at))]);

    let selection = client.body_server_selection(crate::ServerSet::EMPTY).await;
    assert!(selection.eligible.is_empty());
    assert_eq!(
        selection.quota_blocked.unwrap().stable_server_id,
        StableServerId(1_000)
    );

    let selection = client
        .body_server_selection(crate::ServerSet::from([0]))
        .await;
    assert_eq!(selection.eligible, vec![ServerId(1)]);
    assert!(selection.quota_blocked.is_none());
}

// A local listener a test must never dial. Nothing ever accepts on it, so
// `never_dialled` reads the kernel's backlog without waiting.
fn undialled_listener() -> (std::net::TcpListener, u16) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

fn never_dialled(listener: &std::net::TcpListener) -> bool {
    matches!(
        listener.accept(),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    )
}

// The setup a scripted server greets a reader session with.
fn greeted_reader(mut rest: Vec<ScriptStep>) -> Vec<ScriptStep> {
    let mut steps = vec![
        ScriptStep {
            expect_prefix: None,
            response: b"200 ready\r\n",
        },
        ScriptStep {
            expect_prefix: Some("CAPABILITIES"),
            response: READER_CAPABILITIES,
        },
    ];
    steps.append(&mut rest);
    steps
}

// The connection downloads use skips a fill server whose quota is spent and
// opens on the next fill server, which serves the article. The capped server
// is never dialled.
#[tokio::test]
async fn blocking_quota_rejection_fails_over_to_another_fill_server() {
    let (capped_listener, capped_port) = undialled_listener();
    let healthy_port = spawn_scripted_server(greeted_reader(vec![ScriptStep {
        expect_prefix: Some("BODY "),
        response: b"222 1 <quota-failover@example.com>\r\n=ybegin line=128 size=1 name=x\r\nk\r\n=yend size=1\r\n.\r\n",
    }]))
    .await;
    let transfers = crate::transfer::ServerTransferRegistry::new();
    let capped_control = transfers.configure(
        StableServerId(111),
        crate::transfer::ServerTransferConfig {
            closed: false,
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 0,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    let mut capped = scripted_server(capped_port, 0);
    capped.stable_id = StableServerId(111);
    capped.transfer_control = Some(capped_control);
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![capped, scripted_server(healthy_port, 1)],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: UNREACHED_TIMEOUT,
    });

    let trace = tokio::task::spawn_blocking(move || {
        let mut lane = client
            .try_acquire_blocking_body_lane(&[], ServerSet::EMPTY)
            .expect("the second fill server opens a lane");
        assert_eq!(lane.server_id(), ServerId(1));
        let trace = lane.fetch_decoded_sequential("<quota-failover@example.com>");
        client.record_blocking_attempts(&trace.attempts);
        trace
    })
    .await
    .unwrap();

    let decoded = trace.result.expect("second fill server should succeed");
    let decoded_bytes: Vec<u8> = decoded
        .decoded
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    assert_eq!(decoded_bytes, b"A");
    assert_eq!(trace.attempts.len(), 1);
    assert_eq!(trace.attempts[0].server_idx, 1);
    assert_eq!(trace.attempts[0].outcome, FetchAttemptOutcome::Success);
    assert!(never_dialled(&capped_listener));
}

// A fill server out of quota is no reason to open the backfill: the
// connection downloads use reports that quota and opens no lane, and the
// backfill is offered only once the fill server is excluded for the article.
#[test]
fn blocking_quota_blocked_fill_does_not_unlock_or_report_locked_backfill() {
    let retry_at = Instant::now() + Duration::from_secs(60);
    let client = quota_selection_client(&[false, true], &[(0, Some(retry_at))]);

    let refused = client
        .try_acquire_blocking_body_lane(&[], ServerSet::EMPTY)
        .err()
        .expect("a quota-blocked fill server must not open the backfill");
    assert!(
        matches!(
            &refused,
            BlockingBodyLaneAcquireError::Other(NntpError::QuotaBlocked(rejection))
                if rejection.stable_server_id == StableServerId(1_000)
        ),
        "{refused:?}"
    );
    let selection = client
        .try_blocking_body_server_selection_with_estimate(ServerSet::EMPTY, 0)
        .expect("the health state is uncontended in this test");
    assert!(selection.eligible.is_empty());

    let selection = client
        .try_blocking_body_server_selection_with_estimate(ServerSet::from([0]), 0)
        .expect("the health state is uncontended in this test");
    assert_eq!(selection.eligible, vec![ServerId(1)]);
    assert!(selection.quota_blocked.is_none());
}

// A primary that goes silent spends the lane's active budget and fails the
// article on that server alone. One soft timeout leaves the primary first in
// line, and the healthy lower group stays behind it: nothing dials it. The
// silence comes either before the body's status line or in the login a
// mid-session 480 asks for.
#[tokio::test]
async fn blocking_soft_timeout_on_the_primary_does_not_unlock_a_healthy_lower_group() {
    for reauth in [false, true] {
        let primary_port = if reauth {
            spawn_delayed_reauth_server().await
        } else {
            spawn_delayed_body_initial_server().await
        };
        let (backup_listener, backup_port) = undialled_listener();
        let mut primary = scripted_server(primary_port, 0);
        if reauth {
            primary.server.username = Some("user".to_string());
            primary.server.password = Some("pass".to_string());
        }
        let client = NntpClient::new(NntpClientConfig {
            servers: vec![primary, scripted_server(backup_port, 1)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_millis(75),
        });

        let (client, trace) = tokio::task::spawn_blocking(move || {
            let mut lane = client
                .try_acquire_blocking_body_lane(&[], ServerSet::EMPTY)
                .expect("the primary opens a lane");
            assert_eq!(lane.server_id(), ServerId(0));
            let trace = lane.fetch_decoded_sequential("<delayed@example.com>");
            drop(lane);
            client.record_blocking_attempts(&trace.attempts);
            (client, trace)
        })
        .await
        .unwrap();

        assert!(
            matches!(
                trace.result,
                Err(DecodedBodyError::Nntp(NntpError::SoftTimeout(_)))
            ),
            "reauth={reauth}: {:?}",
            trace.result.as_ref().err()
        );
        assert_eq!(trace.attempts.len(), 1, "reauth={reauth}");
        assert_eq!(trace.attempts[0].server_idx, 0, "reauth={reauth}");
        assert!(
            trace.attempts[0]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("soft timeout")),
            "reauth={reauth}: {:?}",
            trace.attempts[0].error
        );
        let order = client
            .blocking_body_server_order(ServerSet::EMPTY)
            .expect("the health state is uncontended in this test");
        assert_eq!(order.first(), Some(&ServerId(0)), "reauth={reauth}");
        assert!(
            matches!(
                client.pool().provider_policy().eligibility(
                    1,
                    ServerSet::EMPTY,
                    ServerSet::EMPTY,
                    1
                ),
                crate::provider_policy::ArticleEligibility::HigherPriority(_)
            ),
            "reauth={reauth}"
        );
        assert!(never_dialled(&backup_listener), "reauth={reauth}");
    }
}

// The pipeline asks this on every article it takes, so it reads what each
// quota published and never takes a quota's lock; the answer is the one the
// quotas' snapshots give.
#[tokio::test]
async fn all_fill_quota_blocked_reads_published_state_without_locking() {
    let client = quota_selection_client(&[false, false, true], &[(0, None), (1, None)]);
    let controls: Vec<_> = (0..2)
        .map(|idx| client.pool.server_transfer_control(ServerId(idx)).unwrap())
        .collect();
    let locks_before: Vec<u64> = controls
        .iter()
        .map(|control| control.snapshot_lock_count())
        .collect();

    assert!(client.all_normal_fill_servers_quota_blocked());

    let locks_after: Vec<u64> = controls
        .iter()
        .map(|control| control.snapshot_lock_count())
        .collect();
    assert_eq!(locks_after, locks_before, "no quota lock per article");
    assert!(
        controls
            .iter()
            .all(|control| control.snapshot().quota_blocked)
    );

    // One fill with room left: not every fill is blocked.
    let client = quota_selection_client(&[false, false], &[(0, None)]);
    assert!(!client.all_normal_fill_servers_quota_blocked());
}

// One server per entry: no transfer control, a control with no quota, or a
// quota of the given limit.
fn quota_policy_client(quotas: &[Option<Option<u64>>]) -> NntpClient {
    let transfers = crate::transfer::ServerTransferRegistry::new();
    let servers = quotas
        .iter()
        .enumerate()
        .map(|(idx, quota)| ServerPoolConfig {
            server: ServerConfig {
                host: format!("policy-{idx}.example.com"),
                ..Default::default()
            },
            stable_id: StableServerId(3_000 + idx as u32),
            transfer_control: quota.map(|limit| {
                transfers.configure(
                    StableServerId(3_000 + idx as u32),
                    crate::transfer::ServerTransferConfig {
                        rate_bytes_per_sec: 0,
                        quota: limit.map(|limit_bytes| crate::transfer::QuotaRuntimeConfig {
                            limit_bytes,
                            generation: 1,
                            retry_at: None,
                        }),
                        closed: false,
                    },
                )
            }),
            max_connections: 1,
            group: 0,
            ..ServerPoolConfig::default()
        })
        .collect();
    NntpClient::new(NntpClientConfig {
        servers,
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: UNREACHED_TIMEOUT,
    })
}

// The pipeline samples the policy on every article it takes. A server with
// no quota is read without its transfer state's lock; one with a quota, room
// left or none, still reads its snapshot. Either way the policy is the one
// reading every server's snapshot gives, and a quota taken away stops the
// lock with it.
#[tokio::test]
async fn provider_policy_takes_no_transfer_lock_for_a_server_without_a_quota() {
    let snapshot_locks = |client: &NntpClient, count: usize| -> Vec<Option<u64>> {
        (0..count)
            .map(|idx| {
                client
                    .pool
                    .server_transfer_control(ServerId(idx))
                    .map(|control| control.snapshot_lock_count())
            })
            .collect()
    };
    let remaining = |policy: &crate::provider_policy::ProviderPolicy| -> Vec<u64> {
        policy
            .servers
            .iter()
            .map(|state| state.quota_remaining)
            .collect()
    };

    // No quota anywhere: no lock at all.
    let client = quota_policy_client(&[None, Some(None)]);
    let before = snapshot_locks(&client, 2);
    let policy = client.pool.provider_policy();
    assert_eq!(snapshot_locks(&client, 2), before);
    assert_eq!(remaining(&policy), vec![u64::MAX, u64::MAX]);
    assert_eq!(policy, client.pool.provider_policy_by_snapshot());

    // A quota with room and one exhausted each take their lock once.
    let client = quota_policy_client(&[None, Some(None), Some(Some(1_000)), Some(Some(0))]);
    let before = snapshot_locks(&client, 4);
    let policy = client.pool.provider_policy();
    assert_eq!(
        snapshot_locks(&client, 4),
        vec![
            None,
            before[1],
            before[2].map(|n| n + 1),
            before[3].map(|n| n + 1)
        ]
    );
    assert_eq!(remaining(&policy), vec![u64::MAX, u64::MAX, 1_000, 0]);
    assert_eq!(policy, client.pool.provider_policy_by_snapshot());

    // The quota with room taken away: no lock for it any more.
    client
        .pool
        .server_transfer_control(ServerId(2))
        .unwrap()
        .update_config(crate::transfer::ServerTransferConfig::default());
    let before = snapshot_locks(&client, 4);
    let policy = client.pool.provider_policy();
    assert_eq!(
        snapshot_locks(&client, 4),
        vec![None, before[1], before[2], before[3].map(|n| n + 1)]
    );
    assert_eq!(remaining(&policy), vec![u64::MAX, u64::MAX, u64::MAX, 0]);
    assert_eq!(policy, client.pool.provider_policy_by_snapshot());
}

#[tokio::test]
async fn backfill_server_absent_from_ordinary_order() {
    let client = tiered_client(&[false, false, true]);

    let order = client.build_server_order(crate::ServerSet::EMPTY).await;
    assert_eq!(order.len(), 2);
    assert!(order.contains(&0));
    assert!(order.contains(&1));
    assert!(
        !order.contains(&2),
        "backfill server must not serve ordinary work"
    );
}

#[tokio::test]
async fn backfill_unlocks_only_when_all_active_fill_servers_excluded() {
    let client = tiered_client(&[false, false, true]);

    let order = client.build_server_order(crate::ServerSet::from([0])).await;
    assert_eq!(
        order,
        vec![1],
        "one fill server left: backfill stays locked"
    );

    let order = client
        .build_server_order(crate::ServerSet::from([0, 1]))
        .await;
    assert_eq!(
        order,
        vec![2],
        "all fill servers excluded: backfill unlocks"
    );
}

#[tokio::test]
async fn cooling_fill_server_keeps_backfill_locked_and_order_empty() {
    let client = tiered_client(&[false, true]);

    client
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(0, CooldownReason::Transport);

    let order = client.build_server_order(crate::ServerSet::EMPTY).await;
    assert!(
        order.is_empty(),
        "cooling fill tier must wait, not spill onto backfill: {order:?}"
    );
}

// A fill server refusing this pool's credentials is not one that lacks the
// article: backfill stays locked, and the article waits for the fill tier.
#[tokio::test]
async fn credentials_refusing_fill_server_keeps_backfill_locked() {
    let client = tiered_client(&[false, false, true]);

    // Server 0: auth failure => Disabled { AuthFailure }.
    client.pool.health().lock().await.record_failure(0, true);

    // Server 1: missed the article, so it is on the exclusion ladder.
    let order = client.build_server_order(crate::ServerSet::from([1])).await;
    assert!(
        order.is_empty(),
        "a fill server refusing credentials must not unlock backfill: {order:?}"
    );
}

// An outage disable heals on its own; it must wait like a cooldown, not
// spill the queue onto backfill.
#[tokio::test]
async fn outage_disabled_fill_server_keeps_backfill_locked_with_peers_excluded() {
    let client = tiered_client(&[false, false, true]);
    {
        let mut health = client.pool.health().lock().await;
        for _ in 0..crate::health::HealthConfig::default().disable_threshold {
            health.record_failure(0, false);
        }
        assert!(matches!(
            health.server(0).state(),
            ServerState::Disabled {
                reason: crate::health::DisableReason::ConsecutiveFailures,
                ..
            }
        ));
    }

    let order = client.build_server_order(crate::ServerSet::from([1])).await;
    assert!(
        order.is_empty(),
        "a consecutive-failure disable is an outage, not a config error: {order:?}"
    );
    let selection = client
        .try_blocking_body_server_selection(crate::ServerSet::from([1]), 0, QuotaCheck::Dispatch)
        .expect("the health state is uncontended in this test");
    assert!(
        selection.eligible.is_empty(),
        "owned lane must not spill an outage onto backfill: {:?}",
        selection.eligible
    );
}

#[tokio::test]
async fn cooling_fill_server_keeps_backfill_locked_with_peers_excluded() {
    let client = tiered_client(&[false, false, true]);

    client
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(0, CooldownReason::Transport);

    let order = client.build_server_order(crate::ServerSet::from([1])).await;
    assert!(
        !order.contains(&2),
        "a 5-10s cooldown is a blip, not an outage: {order:?}"
    );
    assert!(
        order.is_empty(),
        "the caller waits for the cooling fill server instead: {order:?}"
    );
}

// The owned blocking lane runs the same tiering rule: a fill server refusing
// credentials or cooling down never opens the backfill tier.
#[tokio::test]
async fn blocking_selection_keeps_backfill_locked_for_an_unavailable_fill_server() {
    let client = tiered_client(&[false, false, true]);

    client.pool.health().lock().await.record_failure(0, true);

    let selection = client
        .try_blocking_body_server_selection(crate::ServerSet::from([1]), 0, QuotaCheck::Dispatch)
        .expect("the health state is uncontended in this test");
    assert!(
        selection.eligible.is_empty(),
        "owned lane must not spill a credentials refusal onto backfill: {:?}",
        selection.eligible
    );

    let cooling = tiered_client(&[false, false, true]);
    cooling
        .pool
        .health()
        .lock()
        .await
        .record_cooldown(0, CooldownReason::Transport);
    let selection = cooling
        .try_blocking_body_server_selection(crate::ServerSet::from([1]), 0, QuotaCheck::Dispatch)
        .expect("the health state is uncontended in this test");
    assert!(
        selection.eligible.is_empty(),
        "owned lane must not spill a cooldown onto backfill: {:?}",
        selection.eligible
    );
}

// Contention is "ask again", not a tiering verdict: it must requeue on
// the owned fast path without being mistaken for capacity admission.
#[test]
fn selection_contention_is_requeued_but_is_not_capacity_admission() {
    let contended = BlockingBodyLaneAcquireError::SelectionContended;
    assert!(!contended.is_capacity_admission());
    assert!(contended.should_requeue_owned_work());

    assert!(BlockingBodyLaneAcquireError::LocalCapacity.should_requeue_owned_work());
    assert!(
        BlockingBodyLaneAcquireError::ProviderCapacity(NntpError::TooManyConnections)
            .should_requeue_owned_work()
    );
    assert!(!BlockingBodyLaneAcquireError::NoEligibleServer.should_requeue_owned_work());
    assert!(
        !BlockingBodyLaneAcquireError::Other(NntpError::PoolShutdown).should_requeue_owned_work()
    );
}

// The dispatcher asks this question before it opens a lane, and a
// momentary lock collision is not an answer about servers: contention is
// asked again, while having no candidate is a verdict.
#[tokio::test]
async fn candidacy_separates_contention_from_having_no_candidate() {
    let client = NntpClient::new(NntpClientConfig {
        servers: vec![scripted_blocking_s2n_server(1, 2)],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });
    assert_eq!(
        client.blocking_body_lane_candidacy(crate::ServerSet::EMPTY),
        BlockingBodyLaneCandidacy::Candidate
    );
    assert_eq!(
        client.blocking_body_lane_candidacy(crate::ServerSet::from([0])),
        BlockingBodyLaneCandidacy::None,
        "the only server is excluded, so there is genuinely no candidate"
    );

    let health = client.pool.health().clone();
    let guard = health.lock().await;
    assert_eq!(
        client.blocking_body_lane_candidacy(crate::ServerSet::EMPTY),
        BlockingBodyLaneCandidacy::Contended,
        "a held health mutex is 'ask again', not 'no candidate'"
    );
    drop(guard);
    assert_eq!(
        client.blocking_body_lane_candidacy(crate::ServerSet::EMPTY),
        BlockingBodyLaneCandidacy::Candidate
    );
}

// A held health mutex used to surface as `NoEligibleServer`, which reads
// as "no server can serve this" and pushed the work off the owned lane.
#[tokio::test]
async fn contended_health_mutex_reports_contention_not_an_empty_tier() {
    let client = tiered_client(&[false]);
    let health = client.pool.health().clone();
    let guard = health.lock().await;

    let Err(error) =
        client.try_acquire_blocking_body_lane_with_estimate(&[], crate::ServerSet::EMPTY, 0)
    else {
        panic!("selection cannot rank candidates while health is held");
    };
    assert!(
        matches!(error, BlockingBodyLaneAcquireError::SelectionContended),
        "contention must not be reported as a tiering verdict: {error:?}"
    );
    assert!(error.should_requeue_owned_work());

    drop(guard);
    // With the lock free the same call reaches an ordinary tiering verdict
    // again rather than contention.
    let Err(error) =
        client.try_acquire_blocking_body_lane_with_estimate(&[], crate::ServerSet::from([0]), 0)
    else {
        panic!("the only server is excluded, so no lane can be acquired");
    };
    assert!(
        matches!(error, BlockingBodyLaneAcquireError::NoEligibleServer),
        "an excluded tier is still an empty tier: {error:?}"
    );
}

#[tokio::test]
async fn all_backfill_config_degrades_to_fill_with_warning() {
    let client = tiered_client(&[true, true]);

    let order = client.build_server_order(crate::ServerSet::EMPTY).await;
    assert_eq!(order.len(), 2, "all-backfill config is normalized to fill");
}

#[tokio::test]
async fn backfill_server_that_misses_joins_exclusion_ladder() {
    let client = tiered_client(&[false, true]);

    // Fill server missed the article, backfill unlocked…
    let order = client.build_server_order(crate::ServerSet::from([0])).await;
    assert_eq!(order, vec![1]);
    // …and a backfill miss exhausts the order entirely.
    let order = client
        .build_server_order(crate::ServerSet::from([0, 1]))
        .await;
    assert!(order.is_empty());
}

#[tokio::test]
async fn weighted_selection_favours_faster_server() {
    let client = multi_server_client(2);

    // Seed server 0 with 50ms latency and server 1 with 500ms latency.
    {
        let mut health = client.pool.health().lock().await;
        health.record_latency(0, Duration::from_millis(50));
        health.record_latency(1, Duration::from_millis(500));
    }

    // Run 1000 iterations and count how often server 0 is picked first.
    let mut server0_first = 0u32;
    for _ in 0..1000 {
        let ranked = client.rank_servers_in_group(&[0, 1]).await;
        if ranked[0] == 0 {
            server0_first += 1;
        }
    }

    // Server 0 is 10x faster, so it should be picked first significantly
    // more often. With weight=1/score, server 0 weight ≈ 10x server 1 weight,
    // so server 0 should be picked ~90% of the time. Allow some variance.
    assert!(
        server0_first > 700,
        "faster server should be picked first most of the time, but was only first {server0_first}/1000 times"
    );
}

#[tokio::test]
async fn rank_single_server_returns_it() {
    let client = multi_server_client(2);
    let ranked = client.rank_servers_in_group(&[1]).await;
    assert_eq!(ranked, vec![1]);
}

#[tokio::test]
async fn rank_empty_returns_empty() {
    let client = multi_server_client(2);
    let ranked = client.rank_servers_in_group(&[]).await;
    assert!(ranked.is_empty());
}

#[tokio::test]
async fn build_server_order_prefers_healthy_over_degraded() {
    let client = multi_server_client(2);

    {
        let mut health = client.pool.health().lock().await;
        for _ in 0..5 {
            health.record_failure(1, false);
        }
    }

    let order = client.build_server_order(crate::ServerSet::EMPTY).await;
    assert_eq!(order, vec![0, 1]);
}

#[tokio::test]
async fn build_server_order_keeps_backfill_after_primary_group() {
    let pool = NntpPool::new(PoolConfig {
        servers: vec![
            ServerPoolConfig {
                server: ServerConfig {
                    host: "saturated-primary.example.com".into(),
                    ..Default::default()
                },
                max_connections: 0,
                group: 0,
                ..ServerPoolConfig::default()
            },
            ServerPoolConfig {
                server: ServerConfig {
                    host: "available-backfill.example.com".into(),
                    ..Default::default()
                },
                max_connections: 1,
                group: 1,
                ..ServerPoolConfig::default()
            },
        ],
        max_idle_age: Duration::from_secs(300),
        ..PoolConfig::default()
    });

    let client = NntpClient {
        pool: Arc::new(pool),
        soft_timeout: Duration::from_secs(15),
    };

    let order = client.build_server_order(crate::ServerSet::EMPTY).await;
    assert_eq!(order, vec![0, 1]);
}

#[tokio::test]
async fn build_server_order_prefers_ready_servers_within_same_group() {
    let pool = NntpPool::new(PoolConfig {
        servers: vec![
            ServerPoolConfig {
                server: ServerConfig {
                    host: "waiting-primary.example.com".into(),
                    ..Default::default()
                },
                max_connections: 0,
                group: 0,
                ..ServerPoolConfig::default()
            },
            ServerPoolConfig {
                server: ServerConfig {
                    host: "ready-primary.example.com".into(),
                    ..Default::default()
                },
                max_connections: 1,
                group: 0,
                ..ServerPoolConfig::default()
            },
        ],
        max_idle_age: Duration::from_secs(300),
        ..PoolConfig::default()
    });

    let client = NntpClient {
        pool: Arc::new(pool),
        soft_timeout: Duration::from_secs(15),
    };

    let order = client.build_server_order(crate::ServerSet::EMPTY).await;
    assert_eq!(order, vec![1, 0]);
}

fn lane_config(tls: bool, starttls: bool, pinned_ca: bool) -> ServerConfig {
    ServerConfig {
        tls,
        starttls,
        tls_ca_cert: pinned_ca.then(|| std::path::PathBuf::from("/tmp/weaver-test-ca.pem")),
        ..ServerConfig::default()
    }
}

// Owned lanes are the one download engine, so every server gets one: there is
// no second path left for a config to fall to. Plaintext, implicit TLS with
// and without a pinned CA, STARTTLS, and an adopted name-mismatch certificate
// are all lane-served.
#[test]
fn every_server_arrangement_gets_an_owned_lane() {
    for tls in [false, true] {
        for starttls in [false, true] {
            for pinned_ca in [false, true] {
                assert!(
                    supports_blocking_body_lane(&lane_config(tls, starttls, pinned_ca)),
                    "tls={tls} starttls={starttls} pinned_ca={pinned_ca}"
                );
            }
        }
    }
}

#[test]
fn adopted_name_mismatch_certificate_still_gets_an_owned_lane() {
    let mut config = lane_config(true, false, false);
    config.tls_name_mismatch_certificate_der = Some(vec![0x30, 0x82, 0x01, 0x0a]);

    assert!(supports_blocking_body_lane(&config));
}

// The owned-lane candidacy probe asks whether any server could take work at
// all. It is not a dispatch, so it must leave a server's blocked signal
// exactly as the last real dispatch left it — the zero-byte question it
// asks fits any server with a byte of headroom.
#[test]
fn candidacy_probe_leaves_the_quota_blocked_signal_alone() {
    let transfers = crate::transfer::ServerTransferRegistry::new();
    let limited_id = StableServerId(2_100);
    let limited = transfers.configure(
        limited_id,
        crate::transfer::ServerTransferConfig {
            closed: false,
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 100,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    let mut used = limited.try_reserve(60).unwrap();
    used.record_blocking(60);
    used.finish();
    assert!(!limited.snapshot().quota_blocked);

    let client = NntpClient::new(NntpClientConfig {
        servers: vec![
            ServerPoolConfig {
                server: ServerConfig {
                    host: "limited.example.com".into(),
                    ..Default::default()
                },
                stable_id: limited_id,
                transfer_control: Some(Arc::clone(&limited)),
                max_connections: 1,
                group: 0,
                ..ServerPoolConfig::default()
            },
            ServerPoolConfig {
                server: ServerConfig {
                    host: "unlimited.example.com".into(),
                    ..Default::default()
                },
                stable_id: StableServerId(2_101),
                max_connections: 1,
                group: 1,
                ..ServerPoolConfig::default()
            },
        ],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 0,
        soft_timeout: UNREACHED_TIMEOUT,
    });

    // A dispatch that skips the limited server for headroom is the server
    // turning work away, and the signal latches.
    let selection = client
        .try_blocking_body_server_selection_with_estimate(crate::ServerSet::EMPTY, 41)
        .expect("the health state is uncontended in this test");
    assert_eq!(selection.eligible, vec![ServerId(1)]);
    assert!(limited.snapshot().quota_blocked);

    // The probe still sees the limited server as a candidate (zero bytes fit)
    // and the latch stands.
    let candidacy = client.blocking_body_lane_candidacy(crate::ServerSet::EMPTY);
    assert!(
        !matches!(candidacy, BlockingBodyLaneCandidacy::Contended),
        "the health state is uncontended in this test"
    );
    assert!(
        limited.snapshot().quota_blocked,
        "a candidacy question must not clear the blocked signal a dispatch latched"
    );
    assert!(client.server_quota_rejection(ServerId(0), 41).is_some());
    assert!(limited.snapshot().quota_blocked);
}
