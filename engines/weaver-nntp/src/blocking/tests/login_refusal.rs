// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A 480 at a BODY on a connection with no login configured is the server
// asking for one. The lane logs in on that socket with an empty name and
// password. A server that takes them serves the article; one that does not
// refuses the account, and health hears that the way it hears a refused
// login: the server is waited for its give-up time, through every recovery
// probe, before the refusal is the verdict.

use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::client::{FetchAttemptOutcome, NntpClient, NntpClientConfig};
use crate::health::{DisableReason, LOGIN_REFUSAL_GIVE_UP, ServerState};
use crate::pool::ServerPoolConfig;

// How the server answers a login. An empty reply closes the connection in
// place of an answer.
#[derive(Clone, Copy)]
struct Login {
    user: &'static [u8],
    pass: &'static [u8],
}

// Takes the empty login at its first step: no login is needed.
const NEEDS_NONE: Login = Login {
    user: b"281 no login needed\r\n",
    pass: b"",
};
// Asks for the password and takes the empty one.
const TAKES_EMPTY: Login = Login {
    user: b"381 password\r\n",
    pass: b"281 ok\r\n",
};
// Wants an account: the empty password is refused.
const REFUSES: Login = Login {
    user: b"381 password\r\n",
    pass: b"481 authentication failed\r\n",
};
// Knows no AUTHINFO at all.
const UNKNOWN: Login = Login {
    user: b"500 command not recognized\r\n",
    pass: b"",
};
// Hangs up rather than answer the login.
const HANGS_UP: Login = Login {
    user: b"",
    pass: b"",
};

// A news server that greets every connection and asks for no login at the
// dial. Each BODY is answered with 480 until the connection has logged in,
// and with an article once it has if `serves_after_login` holds; with 480
// again if it does not. Once `serve` is set, every BODY is answered with an
// article. Each connection is answered on its own thread until the client
// hangs up; the accept loop outlives the test. Records every command line.
fn spawn_server(
    login: Login,
    serves_after_login: bool,
    serve: Arc<AtomicBool>,
) -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&seen);
    std::thread::spawn(move || {
        for socket in listener.incoming().flatten() {
            let serve = Arc::clone(&serve);
            let recorder = Arc::clone(&recorder);
            std::thread::spawn(move || {
                answer(socket, login, serves_after_login, &serve, &recorder)
            });
        }
    });
    (port, seen)
}

fn answer(
    mut socket: std::net::TcpStream,
    login: Login,
    serves_after_login: bool,
    serve: &AtomicBool,
    seen: &Mutex<Vec<String>>,
) {
    if socket.write_all(b"200 ready\r\n").is_err() {
        return;
    }
    let mut reader = std::io::BufReader::new(socket.try_clone().unwrap());
    let mut logged_in = false;
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        let command = line.trim_end_matches(['\r', '\n']).to_string();
        seen.lock().unwrap().push(command.clone());
        let upper = command.to_ascii_uppercase();
        let login_reply = |reply: &'static [u8]| (!reply.is_empty()).then(|| reply.to_vec());
        let reply = if upper.starts_with("BODY") {
            if serve.load(Ordering::Acquire) || (logged_in && serves_after_login) {
                let mut reply = b"222 0 body follows\r\n".to_vec();
                reply.extend_from_slice(&yenc_body(b"served at last"));
                reply.extend_from_slice(b".\r\n");
                Some(reply)
            } else {
                Some(b"480 authentication required\r\n".to_vec())
            }
        } else if upper.starts_with("AUTHINFO USER") {
            logged_in |= login.user.starts_with(b"281");
            login_reply(login.user)
        } else if upper.starts_with("AUTHINFO PASS") {
            logged_in |= login.pass.starts_with(b"281");
            login_reply(login.pass)
        } else if upper == "CAPABILITIES" {
            Some(b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n".to_vec())
        } else if upper == "QUIT" {
            let _ = socket.write_all(b"205 closing\r\n");
            return;
        } else {
            Some(b"500 command not recognized\r\n".to_vec())
        };
        let Some(reply) = reply else {
            let _ = socket.shutdown(std::net::Shutdown::Both);
            return;
        };
        if socket.write_all(&reply).is_err() {
            return;
        }
    }
}

fn anonymous_client(port: u16) -> NntpClient {
    let mut server = reauth_config(port);
    server.username = None;
    server.password = None;
    NntpClient::new(NntpClientConfig {
        servers: vec![ServerPoolConfig {
            server,
            max_connections: 2,
            ..Default::default()
        }],
        max_idle_age: Duration::from_secs(30),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(30),
    })
}

// BODY `<1@t>` on a fresh lane, sequential or pipelined, each answer told
// to health as the owned lane tells it. Pipelined, two requests go out and
// both answers are read; the login a 480 asks for goes out after the last.
// The last answer is returned.
fn fetch_and_record(client: &NntpClient, pipelined: bool) -> DecodedBodyTrace {
    let mut lane = client
        .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
        .expect("the anonymous dial is taken");
    let trace = if pipelined {
        issue_all(&mut lane, &["<1@t>", "<2@t>"]);
        let (head, _) = lane.ring_read_next().expect("the head request is answered");
        client.record_blocking_attempts(&head.attempts);
        lane.ring_read_next()
            .expect("the second request is answered")
            .0
    } else {
        lane.fetch_decoded_sequential("<1@t>")
    };
    client.record_blocking_attempts(&trace.attempts);
    drop(lane);
    trace
}

// The answer is the server refusing the account, told to health as a
// refused login.
fn refused(trace: &DecodedBodyTrace) -> bool {
    matches!(&trace.result, Err(DecodedBodyError::Nntp(error)) if error.is_credentials_refusal())
        && matches!(
            trace.attempts.as_slice(),
            [attempt] if attempt.outcome == FetchAttemptOutcome::AuthenticationFailure
        )
}

// Health's re-enable is read past the disable the last refusal set, and the
// quarantine's deadline is now: the one probe is due.
fn make_probe_due(client: &NntpClient) {
    client
        .pool()
        .health()
        .blocking_lock()
        .set_reenable_clock(std::time::Instant::now() + Duration::from_mins(10));
    client.pool().expire_quarantine_for_test(0);
}

// The commands starting with `prefix`, in the order seen.
fn sent<'a>(seen: &'a [String], prefix: &str) -> Vec<&'a str> {
    seen.iter()
        .filter(|line| line.to_ascii_uppercase().starts_with(prefix))
        .map(String::as_str)
        .collect()
}

// The server takes the empty login, at its first step or at the password,
// and the article arrives on the same connection. The 480 spends no try and
// health hears nothing against the server.
#[test]
fn an_anonymous_480_logs_in_with_an_empty_account_and_the_article_arrives() {
    for (login, steps) in [(NEEDS_NONE, 1), (TAKES_EMPTY, 2)] {
        for pipelined in [false, true] {
            let (port, seen) = spawn_server(login, true, Arc::new(AtomicBool::new(false)));
            let client = anonymous_client(port);
            let mut lane = client
                .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
                .expect("the anonymous dial is taken");
            let trace = if pipelined {
                issue_all(&mut lane, &["<1@t>"]);
                let (asked, meta) = lane.ring_read_next().unwrap();
                assert!(is_session_expired(&asked), "{asked:?}");
                assert!(!meta.connection_discarded);
                client.record_blocking_attempts(&asked.attempts);
                issue_all(&mut lane, &["<1@t>"]);
                lane.ring_read_next().unwrap().0
            } else {
                lane.fetch_decoded_sequential("<1@t>")
            };
            client.record_blocking_attempts(&trace.attempts);
            assert_eq!(
                ring_payload(&trace),
                b"served at last",
                "pipelined {pipelined}, {steps} steps"
            );
            assert!(lane.is_healthy());
            drop(lane);
            assert!(!client.pool().requires_recovery(0));
            assert!(client.pool().login_refusal_waits().is_empty());
            let seen = seen.lock().unwrap().clone();
            let mut login = vec!["AUTHINFO USER "];
            if steps == 2 {
                login.push("AUTHINFO PASS ");
            }
            assert_eq!(sent(&seen, "AUTHINFO"), login, "saw {seen:?}");
            crate::server_caps::forget("127.0.0.1", port);
        }
    }
}

// The server takes the empty login and answers the very next BODY with 480
// again: it refuses the account it took. The lane logs in once, and health
// hears a refused login.
#[test]
fn an_anonymous_480_right_after_the_login_it_asked_for_is_a_refusal() {
    for pipelined in [false, true] {
        let (port, seen) = spawn_server(TAKES_EMPTY, false, Arc::new(AtomicBool::new(false)));
        let client = anonymous_client(port);
        let mut lane = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .expect("the anonymous dial is taken");
        let trace = if pipelined {
            issue_all(&mut lane, &["<1@t>"]);
            let (asked, _) = lane.ring_read_next().unwrap();
            assert!(is_session_expired(&asked), "{asked:?}");
            client.record_blocking_attempts(&asked.attempts);
            issue_all(&mut lane, &["<1@t>"]);
            lane.ring_read_next().unwrap().0
        } else {
            lane.fetch_decoded_sequential("<1@t>")
        };
        client.record_blocking_attempts(&trace.attempts);
        drop(lane);
        assert!(refused(&trace), "pipelined {pipelined}: {trace:?}");
        assert_eq!(
            client.pool().login_refusal_waits(),
            crate::ServerSet::from([0])
        );
        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            sent(&seen, "AUTHINFO USER").len(),
            1,
            "pipelined {pipelined}: one login, not one per 480; saw {seen:?}"
        );
        crate::server_caps::forget("127.0.0.1", port);
    }
}

// On both the sequential and the pipelined BODY, a login the server does
// not take, whether refused, unknown to it or hung up on, leaves the server
// held as refusing the login: quarantined for it, waited for inside its
// give-up time, the refusal past it.
#[test]
fn an_anonymous_login_not_taken_is_heard_by_health_as_a_refused_login() {
    for (login, name) in [
        (REFUSES, "refused"),
        (UNKNOWN, "unknown"),
        (HANGS_UP, "hung up"),
    ] {
        for pipelined in [false, true] {
            let case = format!("{name}, pipelined {pipelined}");
            let (port, _) = spawn_server(login, true, Arc::new(AtomicBool::new(false)));
            let client = anonymous_client(port);
            let pool = client.pool();
            let start = std::time::Instant::now();
            pool.set_login_refusal_clock(0, start);

            let trace = fetch_and_record(&client, pipelined);
            assert!(
                matches!(
                    pool.health().blocking_lock().server(0).state(),
                    ServerState::Disabled {
                        reason: DisableReason::AuthFailure,
                        ..
                    }
                ),
                "{case}: {:?}",
                pool.health().blocking_lock().server(0).state()
            );
            assert!(refused(&trace), "{case}: {trace:?}");
            assert!(pool.requires_recovery(0));
            assert_eq!(
                pool.login_refusal_waits(),
                crate::ServerSet::from([0]),
                "{case}: the refusal waits, as an outage does"
            );
            assert!(!pool.refuses_credentials(0));
            assert_eq!(
                pool.next_login_refusal_verdict(),
                Some(start + LOGIN_REFUSAL_GIVE_UP)
            );
            pool.set_login_refusal_clock(0, start + LOGIN_REFUSAL_GIVE_UP);
            assert!(
                pool.refuses_credentials(0),
                "{case}: past the give-up time the refusal is the verdict"
            );
            crate::server_caps::forget("127.0.0.1", port);
        }
    }
}

// The server keeps taking the anonymous dial and keeps refusing the login
// its 480 asks for. Each recovery probe dials with no login and is let in;
// neither the dial nor the refusal after it ends or restarts the run. Inside
// the give-up time the refusal is a wait at every probe, and the probe
// refused at the give-up time is the verdict. A BODY answered at a later
// probe is the server recovered, and the run is over.
#[test]
fn an_anonymous_server_refusing_the_login_reaches_the_verdict_through_its_probes() {
    let serve = Arc::new(AtomicBool::new(false));
    let (port, _) = spawn_server(REFUSES, true, Arc::clone(&serve));
    let client = anonymous_client(port);
    let pool = client.pool();
    let start = std::time::Instant::now();
    let give_up = LOGIN_REFUSAL_GIVE_UP;
    let verdict_at = start + give_up;
    pool.set_login_refusal_clock(0, start);
    assert!(refused(&fetch_and_record(&client, false)));
    assert_eq!(pool.next_login_refusal_verdict(), Some(verdict_at));

    for into_the_run in [give_up / 4, give_up / 2, give_up - Duration::from_secs(60)] {
        pool.set_login_refusal_clock(0, start + into_the_run);
        make_probe_due(&client);
        let mut probe = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .expect("the due probe dials with no login and is let in");
        assert!(pool.requires_recovery(0), "a dial taken is no recovery");
        assert_eq!(
            pool.next_login_refusal_verdict(),
            Some(verdict_at),
            "the dial leaves the run where it was"
        );
        let trace = probe.fetch_decoded_sequential("<1@t>");
        client.record_blocking_attempts(&trace.attempts);
        drop(probe);
        assert!(refused(&trace), "{trace:?}");
        assert!(!pool.refuses_credentials(0), "inside the give-up time");
        assert_eq!(pool.login_refusal_waits(), crate::ServerSet::from([0]));
        assert_eq!(
            pool.next_login_refusal_verdict(),
            Some(verdict_at),
            "the refused probe carries the run on"
        );
    }

    pool.set_login_refusal_clock(0, verdict_at);
    make_probe_due(&client);
    assert!(!pool.refuses_credentials(0), "the probe is due");
    assert!(refused(&fetch_and_record(&client, false)));
    assert!(pool.refuses_credentials(0), "refused at the give-up time");
    assert!(pool.login_refusal_verdicts_untold().is_some());
    assert!(pool.login_refusal_waits().is_empty());

    // The server takes the account after all: the BODY answered at the next
    // probe ends the quarantine and the run.
    serve.store(true, Ordering::Release);
    make_probe_due(&client);
    let trace = fetch_and_record(&client, false);
    assert_eq!(ring_payload(&trace), b"served at last");
    assert!(!pool.requires_recovery(0));
    assert!(!pool.refuses_credentials(0));
    assert!(pool.login_refusal_waits().is_empty());
    assert_eq!(pool.next_login_refusal_verdict(), None);
    crate::server_caps::forget("127.0.0.1", port);
}
