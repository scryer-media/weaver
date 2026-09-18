use std::time::Duration;

use crate::servers::ServerConfig;

#[derive(Debug, Clone)]
pub struct ServerConnectivityResult {
    pub success: bool,
    pub message: String,
    pub latency_ms: Option<u64>,
    /// Command-to-status-line round trip on the established session, which is
    /// what BODY pipelining actually has to hide. Taken from the CAPABILITIES
    /// exchange setup already sends; no extra command is issued for it. `latency_ms` above is the
    /// whole connect — TCP, TLS and authentication — and is far larger.
    pub first_byte_latency_ms: Option<u64>,
    /// "good", "moderate" or "slow" for `first_byte_latency_ms`. Descriptive
    /// only: nothing the operator has to act on follows from it.
    pub first_byte_latency_band: Option<String>,
    pub supports_pipelining: bool,
    pub adoptable_tls_name_mismatch_certificate_der: Option<Vec<u8>>,
    /// IANA name of the TLS suite negotiated with weaver's CPU-preferred
    /// cipher family offered first; `None` for plaintext or failed probes.
    pub tls_cipher_suite: Option<String>,
    /// Whether the negotiated suite is the family weaver offered first, i.e.
    /// the server follows client order. `None` for plaintext or failed probes.
    pub tls_honors_client_cipher_order: Option<bool>,
}

pub async fn probe_server_connection(config: &ServerConfig) -> ServerConnectivityResult {
    probe_server_connection_with_proxy(config, None).await
}

pub async fn probe_server_connection_with_proxy(
    config: &ServerConfig,
    proxy: Option<std::sync::Arc<weaver_tunnel::bridge::Bridge>>,
) -> ServerConnectivityResult {
    // Every check happens on the one real connection. Its handshake verifies
    // the certificate against the hostname, so a mismatch fails there, before
    // any greeting or credentials are exchanged; only then is the presented
    // certificate captured for the server form to offer. Nothing else dials:
    // on a distant server each extra handshake or command is a full round
    // trip, and a seeded server's probe must finish inside a fixed deadline.
    let nntp_config = weaver_nntp::ServerConfig {
        proxy: proxy.clone(),
        host: config.host.clone(),
        port: config.port,
        tls: config.tls,
        username: config.username.clone(),
        password: config.password.clone(),
        tls_ca_cert: config.tls_ca_cert.clone(),
        tls_name_mismatch_certificate_der: config.tls_name_mismatch_certificate_der.clone(),
        pipelining: weaver_nntp::PipeliningCapability::Probe,
        ..Default::default()
    };
    let start = std::time::Instant::now();
    match weaver_nntp::NntpConnection::connect(&nntp_config).await {
        Ok(mut conn) => {
            let latency = start.elapsed().as_millis() as u64;
            let pipelining = conn.capabilities().supports_pipelining();
            let tls_cipher_suite = conn.negotiated_cipher_suite();
            // Distance comes from the CAPABILITIES exchange setup already made.
            let first_byte_latency = conn.capabilities_round_trip();
            let _ = conn.quit().await;
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
                    proxy.as_ref(),
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

/// Descriptive label for a first-byte latency, on the same thresholds the
/// download depth explorer uses.
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
}
