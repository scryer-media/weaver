// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;

use crate::servers::ServerConfig;

// How long a server connection may take to dial, for the pool's routes and
// for the connectivity probe alike.
pub(crate) const SERVER_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct ServerConnectivityResult {
    pub success: bool,
    pub message: String,
    pub latency_ms: Option<u64>,
    // Command-to-status-line round trip on the established session, which is
    // what BODY pipelining actually has to hide. Taken from the CAPABILITIES
    // exchange setup already sends; no extra command is issued for it. `latency_ms` above is the
    // whole connect — TCP, TLS and authentication — and is far larger.
    pub first_byte_latency_ms: Option<u64>,
    // "good", "moderate" or "slow" for `first_byte_latency_ms`. Descriptive
    // only: nothing the operator has to act on follows from it.
    pub first_byte_latency_band: Option<String>,
    pub supports_pipelining: bool,
    pub adoptable_tls_name_mismatch_certificate_der: Option<Vec<u8>>,
    // IANA name of the TLS suite negotiated with weaver's CPU-preferred
    // cipher family offered first; `None` for plaintext or failed probes.
    pub tls_cipher_suite: Option<String>,
    // Whether the negotiated suite is the family weaver offered first, i.e.
    // the server follows client order. `None` for plaintext or failed probes.
    pub tls_honors_client_cipher_order: Option<bool>,
}

impl ServerConnectivityResult {
    pub fn failed(message: String) -> Self {
        Self {
            success: false,
            message,
            latency_ms: None,
            first_byte_latency_ms: None,
            first_byte_latency_band: None,
            supports_pipelining: false,
            adoptable_tls_name_mismatch_certificate_der: None,
            tls_cipher_suite: None,
            tls_honors_client_cipher_order: None,
        }
    }
}

// Probe a server over each leg of its drafted route, in leg order, then
// revoke the draft. Nothing live changes.
pub async fn probe_server_route(
    config: &ServerConfig,
    route: &crate::proxies::DraftNetworkRoute,
) -> Result<Vec<ServerConnectivityResult>, String> {
    let mut results = Vec::new();
    for position in 0..route.leg_count() {
        match route.nntp_dialer_for_leg(position, config.id) {
            Ok(dialer) => results.push(probe_server_connection(config, dialer).await),
            Err(error) => {
                route.revoke().await;
                return Err(error);
            }
        }
    }
    route.revoke().await;
    Ok(results)
}

// Probe a server that is not saved yet over the route its policy drafts: the
// saved route for its id, or the default one. The first leg that answers
// speaks for the server.
pub async fn probe_unsaved_server(
    proxies: &crate::proxies::ProxyRuntime,
    config: &ServerConfig,
) -> ServerConnectivityResult {
    let consumer = crate::proxies::Consumer::Server(config.id);
    let probed = match proxies.network.draft_route(
        consumer,
        proxies.policy(consumer),
        SERVER_CONNECT_TIMEOUT,
    ) {
        Ok(route) => probe_server_route(config, &route).await,
        Err(error) => Err(error),
    };
    match probed {
        Ok(mut results) => match results.iter().position(|result| result.success) {
            Some(answered) => results.swap_remove(answered),
            None if results.is_empty() => {
                ServerConnectivityResult::failed("route has no legs to test".into())
            }
            None => results.swap_remove(0),
        },
        Err(message) => ServerConnectivityResult::failed(message),
    }
}

pub async fn probe_server_connection(
    config: &ServerConfig,
    dialer: std::sync::Arc<weaver_nntp::route_dialer::RouteDialer>,
) -> ServerConnectivityResult {
    // Every check happens on the one real connection. Its handshake verifies
    // the certificate against the hostname, so a mismatch fails there, before
    // any greeting or credentials are exchanged; only then is the presented
    // certificate captured for the server form to offer. Nothing else dials:
    // on a distant server each extra handshake or command is a full round
    // trip, and a seeded server's probe must finish inside a fixed deadline.
    let nntp_config = weaver_nntp::ServerConfig {
        host: config.host.clone(),
        port: config.port,
        tls: config.tls,
        username: config.username.clone(),
        password: config.password.clone(),
        tls_ca_cert: config.tls_ca_cert.clone(),
        tls_name_mismatch_certificate_der: config.tls_name_mismatch_certificate_der.clone(),
        pipelining: weaver_nntp::PipeliningCapability::Probe,
        connect_timeout: SERVER_CONNECT_TIMEOUT,
        ..weaver_nntp::ServerConfig::new(dialer)
    };
    match dial_blocking(&nntp_config).await {
        Ok(dialed) => {
            let latency = dialed.connect_latency.as_millis() as u64;
            let pipelining = dialed.supports_pipelining;
            let tls_cipher_suite = dialed.tls_cipher_suite;
            // Distance comes from the CAPABILITIES exchange setup already made.
            let first_byte_latency = dialed.capabilities_round_trip;
            // The handshake already answered this: the server either picked the
            // family this client offered first or overrode it.
            let tls_honors_client_cipher_order = tls_cipher_suite
                .as_deref()
                .map(|suite| nntp_config.tls_cipher_preference.leads_with(suite));
            ServerConnectivityResult {
                success: true,
                message: "Connected successfully".to_string(),
                latency_ms: Some(latency),
                first_byte_latency_ms: first_byte_latency.map(|latency| latency.as_millis() as u64),
                first_byte_latency_band: first_byte_latency
                    .map(|latency| latency_band_label(latency).to_string()),
                supports_pipelining: pipelining,
                adoptable_tls_name_mismatch_certificate_der: None,
                tls_cipher_suite,
                tls_honors_client_cipher_order,
            }
        }
        Err(error) => {
            let adoptable_tls_name_mismatch_certificate_der = if config.tls {
                weaver_nntp::tls::inspect_tls_name_mismatch_certificate_via(
                    &config.host,
                    config.port,
                    config.tls_ca_cert.as_deref(),
                    &nntp_config.dialer,
                )
                .await
                .ok()
                .flatten()
            } else {
                None
            };
            let message = if adoptable_tls_name_mismatch_certificate_der.is_some() {
                "We reached the server securely, but its certificate belongs to a different hostname. Review the certificate below only if you recognise this provider.".to_string()
            } else {
                user_facing_connection_error(&error)
            };
            ServerConnectivityResult {
                success: false,
                message,
                latency_ms: None,
                first_byte_latency_ms: None,
                first_byte_latency_band: None,
                supports_pipelining: false,
                adoptable_tls_name_mismatch_certificate_der,
                tls_cipher_suite: None,
                tls_honors_client_cipher_order: None,
            }
        }
    }
}

// What the server test reads off its connection before it quits.
struct DialedServer {
    connect_latency: Duration,
    supports_pipelining: bool,
    tls_cipher_suite: Option<String>,
    capabilities_round_trip: Option<Duration>,
}

// Dial the connection downloads use, on a worker thread so the runtime is
// never blocked, read what the test reports, and quit. The whole setup keeps
// one deadline, the connect timeout plus the route's own budget; a dial still
// running past it ends on its socket timeouts and quits on its own thread.
async fn dial_blocking(
    config: &weaver_nntp::ServerConfig,
) -> Result<DialedServer, weaver_nntp::NntpError> {
    let deadline = config.connect_timeout.max(MIN_CONNECT_TIMEOUT) + config.dialer.inner.budget();
    let config = config.clone();
    let dial = tokio::task::spawn_blocking(move || {
        let start = std::time::Instant::now();
        let mut conn = weaver_nntp::BlockingNntpConnection::connect(&config)?;
        let dialed = DialedServer {
            connect_latency: start.elapsed(),
            supports_pipelining: conn.capabilities().supports_pipelining(),
            tls_cipher_suite: conn.negotiated_cipher_suite(),
            capabilities_round_trip: conn.capabilities_round_trip(),
        };
        let _ = conn.quit();
        Ok(dialed)
    });
    match tokio::time::timeout(deadline, dial).await {
        Ok(Ok(result)) => result,
        Ok(Err(join)) => Err(weaver_nntp::NntpError::Io(std::io::Error::other(join))),
        Err(_) => Err(weaver_nntp::NntpError::Timeout),
    }
}

// The floor the connection setup clamps its timeout to.
const MIN_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

// Descriptive label for a first-byte latency, on the same thresholds the
// download depth explorer uses.
fn latency_band_label(latency: Duration) -> &'static str {
    crate::pipeline::download::transport::LatencyBand::from_latency(latency).label()
}

fn user_facing_connection_error(error: &weaver_nntp::NntpError) -> String {
    let diagnostic = error.to_string();
    if diagnostic.contains("certificate") && diagnostic.contains("not valid for name") {
        "We could not verify that this certificate belongs to the hostname you entered. Check the hostname with your provider, or review the presented certificate if one is available."
            .to_string()
    } else if diagnostic.contains("received corrupt message of type InvalidContentType") {
        "The configured port doesn't seem to accept TLS. Try another port or check your server's connection guidance."
            .to_string()
    } else {
        diagnostic
    }
}

#[cfg(test)]
mod tests {
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
}
