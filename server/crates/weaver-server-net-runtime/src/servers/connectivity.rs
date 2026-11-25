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

    // Whether the probe failed because the server's certificate names another
    // host: the handshake was refused for that alone, and the certificate it
    // presented is held for the operator to review.
    pub fn certificate_names_another_host(&self) -> bool {
        !self.success && self.adoptable_tls_name_mismatch_certificate_der.is_some()
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
        Err(error) => Err(error.to_string()),
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
    crate::servers::latency_band::LatencyBand::from_latency(latency).label()
}

pub fn user_facing_connection_error(error: &weaver_nntp::NntpError) -> String {
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
