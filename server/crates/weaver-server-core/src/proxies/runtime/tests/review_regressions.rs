// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use tokio::io::{AsyncBufReadExt, BufReader};
use weaver_nntp::{NntpClient, NntpConnection, ServerConfig, ServerId, client::NntpClientConfig};
use weaver_tunnel::bridge::ConnectionOutcome;
use weaver_tunnel::pipe::{DialError, Dialed, DialedStream, Dialer, Target};

// How an NNTP fixture behaves once dialed.
#[derive(Clone, Copy)]
enum Mode {
    Healthy,
    AuthRejected,
    Truncated,
    NoResponse,
    // The first rung's connect window runs out before the next rung answers,
    // as a route's ladder does when its primary stalls.
    StalledRung,
}

// What the network runtime hands NNTP for a server's route: one dial per
// connection, each with the outcome its leg listens to. Every dial takes the
// next mode, the last repeating, and every reported stream failure is
// counted.
struct FixtureDialer {
    modes: Mutex<VecDeque<Mode>>,
    failures: Arc<AtomicUsize>,
    budget: Duration,
}
#[async_trait::async_trait]
impl Dialer for FixtureDialer {
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        let mode = {
            let mut modes = self.modes.lock().unwrap();
            if modes.len() > 1 {
                modes.pop_front().unwrap()
            } else {
                *modes.front().unwrap()
            }
        };
        if matches!(mode, Mode::StalledRung) {
            let _ = tokio::time::timeout(STALLED_RUNG, std::future::pending::<()>()).await;
        }
        let outcome = Arc::new(ConnectionOutcome::default());
        let failures = self.failures.clone();
        outcome.on_failure(move || {
            failures.fetch_add(1, Ordering::SeqCst);
        });
        Ok(Dialed {
            stream: DialedStream::Tunnel(Box::new(nntp_fixture(mode))),
            outcome,
            path: Default::default(),
            peer: None,
            source: None,
            setup: None,
        })
    }
    fn budget(&self) -> Duration {
        self.budget
    }
    fn describe(&self) -> String {
        "NNTP review fixture".into()
    }
}

fn nntp_fixture(mode: Mode) -> tokio::io::DuplexStream {
    let (client, peer) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut peer = BufReader::new(peer);
        peer.get_mut()
            .write_all(b"200 fixture ready\r\n")
            .await
            .unwrap();
        loop {
            let mut command = String::new();
            if peer.read_line(&mut command).await.unwrap_or(0) == 0 {
                break;
            }
            let (reply, close): (&[u8], bool) = if command.starts_with("AUTHINFO") {
                if matches!(mode, Mode::AuthRejected) {
                    (b"481 authentication rejected\r\n", true)
                } else if command.starts_with("AUTHINFO USER") {
                    (b"381 password required\r\n", false)
                } else {
                    (b"281 authenticated\r\n", false)
                }
            } else if command.starts_with("CAPABILITIES") {
                (b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n", false)
            } else if command.starts_with("GROUP") {
                (b"211 1 1 1 fixture\r\n", false)
            } else if command.starts_with("BODY") {
                if matches!(mode, Mode::Truncated) {
                    (b"222 1 <fixture> body\r\npartial", true)
                } else {
                    (b"222 1 <fixture> body\r\npayload\r\n.\r\n", false)
                }
            } else if command.starts_with("HEAD") {
                (
                    b"221 1 <fixture> headers\r\nSubject: fixture\r\n.\r\n",
                    false,
                )
            } else if command.starts_with("ARTICLE") {
                (
                    b"220 1 <fixture> article\r\nSubject: fixture\r\n\r\npayload\r\n.\r\n",
                    false,
                )
            } else if command.starts_with("STAT") {
                if matches!(mode, Mode::NoResponse) {
                    (b"", true)
                } else {
                    (b"223 1 <fixture> exists\r\n", false)
                }
            } else if command.starts_with("QUIT") {
                (b"205 goodbye\r\n", true)
            } else {
                (b"200 ok\r\n", false)
            };
            if peer.get_mut().write_all(reply).await.is_err() || close {
                break;
            }
        }
    });
    client
}

// Route and NNTP timeouts that a test does not exercise, set out of reach of a
// slow runner so only the behavior under test can decide the outcome.
const NOT_UNDER_TEST: Duration = Duration::from_secs(3600);
// The connect window of a stalled rung.
const STALLED_RUNG: Duration = Duration::from_millis(150);

// A server reached through `modes`, and the count of stream failures its
// connections report to their route.
fn config(modes: &[Mode], budget: Duration) -> (ServerConfig, Arc<AtomicUsize>) {
    let failures = Arc::new(AtomicUsize::new(0));
    let dialer = Arc::new(weaver_nntp::route_dialer::RouteDialer {
        inner: Arc::new(FixtureDialer {
            modes: Mutex::new(modes.iter().copied().collect()),
            failures: failures.clone(),
            budget,
        }),
        plan: None,
        egress_controls: Arc::new(weaver_nntp::transfer::ServerTransferRegistry::with_scope(
            weaver_nntp::transfer::TransferScope::Egress,
        )),
        runtime: tokio::runtime::Handle::current(),
        server: 0,
    });
    let config = ServerConfig {
        host: "nntp-review.invalid".into(),
        port: 119,
        tls: false,
        dialer,
        connect_timeout: NOT_UNDER_TEST,
        command_timeout: NOT_UNDER_TEST,
        ..Default::default()
    };
    (config, failures)
}

#[tokio::test]
async fn quit_and_auth_rejection_do_not_fail_the_route_stream() {
    for mode in [Mode::Healthy, Mode::AuthRejected] {
        let (mut cfg, failures) = config(&[mode], NOT_UNDER_TEST);
        if matches!(mode, Mode::AuthRejected) {
            cfg.username = Some("fixture".into());
            cfg.password = Some("fixture".into());
        }
        match NntpConnection::connect(&cfg).await {
            Ok(mut connection) => connection.quit().await.unwrap(),
            Err(error) => assert!(matches!(mode, Mode::AuthRejected), "{error}"),
        }
        tokio::task::yield_now().await;
        assert_eq!(
            failures.load(Ordering::SeqCst),
            0,
            "expected NNTP closure must not fail its route"
        );
    }
}

#[tokio::test]
async fn unexpected_nntp_eof_fails_only_that_connection_and_retries() {
    let (cfg, failures) = config(&[Mode::Truncated, Mode::Healthy], NOT_UNDER_TEST);
    let mut cfg = NntpClientConfig::single(cfg, 1);
    cfg.max_retries_per_server = 1;
    let client = NntpClient::new(cfg);
    let body = client.fetch_body("fixture").await.unwrap();
    assert_eq!(body.as_ref(), b"payload\r\n");
    assert_eq!(failures.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

// The paused clock lets only the stalled rung's connect window move time, so
// a loaded runner cannot spend the operation's soft budget.
#[tokio::test(start_paused = true)]
async fn fallback_establishment_preserves_the_article_soft_budget() {
    // Establishment exceeds the article budget, and the fetch still lands.
    let (cfg, failures) = config(&[Mode::StalledRung], STALLED_RUNG * 3);
    let mut cfg = NntpClientConfig::single(cfg, 1);
    cfg.soft_timeout = Duration::from_millis(50);
    cfg.max_retries_per_server = 0;
    let client = NntpClient::new(cfg);
    client.fetch_body("fixture").await.unwrap();
    assert_eq!(failures.load(Ordering::SeqCst), 0);
    client.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_nntp_reports_unexpected_eof_but_not_quit_or_auth_rejection() {
    for mode in [Mode::Healthy, Mode::AuthRejected, Mode::NoResponse] {
        let (mut cfg, failures) = config(&[mode], NOT_UNDER_TEST);
        if matches!(mode, Mode::AuthRejected) {
            cfg.username = Some("fixture".into());
            cfg.password = Some("fixture".into());
        }
        tokio::task::spawn_blocking(move || {
            match weaver_nntp::BlockingNntpConnection::connect(&cfg) {
                Ok(mut connection) => {
                    if matches!(mode, Mode::NoResponse) {
                        assert!(
                            connection
                                .send_command(&weaver_nntp::commands::Command::Stat(
                                    weaver_nntp::ArticleId::MessageId("fixture".into()),
                                ))
                                .is_err()
                        );
                    } else {
                        connection.quit().unwrap();
                    }
                }
                Err(error) => assert!(matches!(mode, Mode::AuthRejected), "{error}"),
            }
        })
        .await
        .unwrap();
        assert_eq!(
            failures.load(Ordering::SeqCst),
            usize::from(matches!(mode, Mode::NoResponse))
        );
    }
}

#[tokio::test]
async fn proxy_budget_does_not_extend_waiting_for_pool_capacity() {
    let (cfg, _) = config(&[Mode::Healthy], NOT_UNDER_TEST);
    let mut cfg = NntpClientConfig::single(cfg, 1);
    cfg.soft_timeout = Duration::from_millis(50);
    let client = NntpClient::new(cfg);
    let held = client.pool().acquire(ServerId(0)).await.unwrap();
    // The route budget is an hour, so a pool wait that borrowed it would hang
    // until the runner ends the test; only the 50 ms soft budget can end this
    // wait with an acquire timeout.
    let result = client.fetch_body("fixture").await;
    assert!(matches!(
        result,
        Err(weaver_nntp::NntpError::AcquireTimeout(_))
    ));
    drop(held);
    client.shutdown().await;
}

#[test]
fn concurrent_save_never_erases_a_committed_ssh_pin() {
    for id in 1..=8 {
        let db = Database::open_in_memory().unwrap();
        let mut profile = profile(id);
        profile.kind = ProxyKind::Ssh;
        profile.secrets.username = Some("fixture".into());
        profile.secrets.private_key = Some(weaver_tunnel::test_support::CLIENT_ED25519_PEM.into());
        db.save_proxy_profile(&profile).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let pin_db = db.clone();
        let pin_barrier = barrier.clone();
        let pin = std::thread::spawn(move || {
            pin_barrier.wait();
            pin_db.pin_proxy_host_key(id, 1, "SHA256:fixture")
        });
        profile.revision = 2;
        barrier.wait();
        db.save_proxy_profile(&profile).unwrap();
        if pin.join().unwrap().is_ok() {
            assert_eq!(
                db.proxy_host_key(id, 2).unwrap().as_deref(),
                Some("SHA256:fixture")
            );
        }
        assert!(db.pin_proxy_host_key(id, 1, "SHA256:stale").is_err());
    }
}
