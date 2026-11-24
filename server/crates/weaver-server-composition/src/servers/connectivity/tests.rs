// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn hostname_mismatch_error_explains_the_operator_next_step() {
    let error = weaver_nntp::NntpError::Io(std::io::Error::other(
        "invalid peer certificate: certificate not valid for name configured.example",
    ));

    assert_eq!(
        user_facing_connection_error(&error),
        "We could not verify that this certificate belongs to the hostname you entered. Check the hostname with your provider, or review the presented certificate if one is available."
    );
}

#[test]
fn plaintext_on_a_tls_port_error_explains_the_operator_next_step() {
    let error = weaver_nntp::NntpError::Io(std::io::Error::other(
        "received corrupt message of type InvalidContentType",
    ));

    assert_eq!(
        user_facing_connection_error(&error),
        "The configured port doesn't seem to accept TLS. Try another port or check your server's connection guidance."
    );
}

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

fn test_server(port: u16, tls: bool, login: Option<(&str, &str)>) -> ServerConfig {
    ServerConfig {
        id: 1,
        host: "127.0.0.1".into(),
        port,
        tls,
        username: login.map(|(user, _)| user.to_string()),
        password: login.map(|(_, pass)| pass.to_string()),
        connections: 1,
        active: true,
        supports_pipelining: false,
        pipelining_depth: None,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: crate::servers::ServerDownloadQuotaConfig::default(),
        tls_ca_cert: None,
        tls_name_mismatch_certificate_der: None,
    }
}

// A plaintext server that greets, answers each command line with
// `reply(line)`, and ends the session after QUIT or a reply ending in
// a close. Returns the port and every command line it read.
fn spawn_plain_server(
    reply: fn(&str) -> &'static str,
) -> (u16, std::thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket.write_all(b"200 test server ready\r\n").unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        let mut seen = Vec::new();
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            let command = line.trim_end().to_string();
            let answer = reply(&command);
            seen.push(command);
            if socket.write_all(answer.as_bytes()).is_err() || answer.starts_with("205") {
                break;
            }
            if answer.starts_with("481") {
                break;
            }
        }
        seen
    });
    (port, handle)
}

#[tokio::test]
async fn the_server_test_reports_capabilities_and_distance_on_a_plain_server() {
    let (port, server) = spawn_plain_server(|command| match command {
        "CAPABILITIES" => "101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
        "QUIT" => "205 closing\r\n",
        _ => "500 command not recognized\r\n",
    });
    let result = probe_server_connection(
        &test_server(port, false, None),
        weaver_nntp::route_dialer::RouteDialer::direct_for_tests(),
    )
    .await;

    assert!(result.success, "{}", result.message);
    assert_eq!(result.message, "Connected successfully");
    assert!(result.latency_ms.is_some());
    assert!(result.first_byte_latency_ms.is_some());
    assert!(result.first_byte_latency_band.is_some());
    assert!(result.supports_pipelining);
    assert_eq!(result.tls_cipher_suite, None);
    assert_eq!(result.tls_honors_client_cipher_order, None);
    assert_eq!(result.adoptable_tls_name_mismatch_certificate_der, None);
    assert_eq!(server.join().unwrap(), ["CAPABILITIES", "QUIT"]);
}

#[tokio::test]
async fn the_server_test_reports_a_refused_login() {
    let (port, server) = spawn_plain_server(|command| {
        if command.starts_with("AUTHINFO USER ") {
            "381 password required\r\n"
        } else if command.starts_with("AUTHINFO PASS ") {
            "481 Authentication failed\r\n"
        } else {
            "500 command not recognized\r\n"
        }
    });
    let result = probe_server_connection(
        &test_server(port, false, Some(("reader", "wrong"))),
        weaver_nntp::route_dialer::RouteDialer::direct_for_tests(),
    )
    .await;

    assert!(!result.success);
    assert_eq!(result.message, "authentication failed (481)");
    assert_eq!(result.latency_ms, None);
    assert!(!result.supports_pipelining);
    assert_eq!(
        server.join().unwrap(),
        ["AUTHINFO USER reader", "AUTHINFO PASS wrong"]
    );
}

#[tokio::test]
async fn the_server_test_reports_a_refused_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let result = probe_server_connection(
        &test_server(port, false, None),
        weaver_nntp::route_dialer::RouteDialer::direct_for_tests(),
    )
    .await;

    assert!(!result.success);
    assert!(result.message.starts_with("route: "), "{}", result.message);
    assert_eq!(result.latency_ms, None);
}

// A TLS test against a port that answers in plaintext names the port, not
// the handshake. The failed test also looks for a presented certificate,
// which dials again, so the server answers every connection the same way.
#[tokio::test]
async fn the_server_test_explains_a_port_that_does_not_speak_tls() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for socket in listener.incoming() {
            let Ok(mut socket) = socket else { break };
            let _ = socket.write_all(b"200 test server ready\r\n");
            // Hold the socket until the client hangs up, so the greeting
            // is read rather than lost to a reset.
            let _ = std::io::copy(&mut socket, &mut std::io::sink());
        }
    });

    let result = probe_server_connection(
        &test_server(port, true, None),
        weaver_nntp::route_dialer::RouteDialer::direct_for_tests(),
    )
    .await;

    assert!(!result.success);
    assert_eq!(
        result.message,
        "The configured port doesn't seem to accept TLS. Try another port or check your server's connection guidance."
    );
    assert_eq!(result.tls_cipher_suite, None);
    assert_eq!(result.adoptable_tls_name_mismatch_certificate_der, None);
}
