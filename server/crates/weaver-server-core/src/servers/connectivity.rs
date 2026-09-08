use std::time::Duration;

use crate::servers::ServerConfig;

#[derive(Debug, Clone)]
pub struct ServerConnectivityResult {
    pub success: bool,
    pub message: String,
    pub latency_ms: Option<u64>,
    /// Command-to-status-line round trip on the established session, which is
    /// what BODY pipelining actually has to hide. `latency_ms` above is the
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
    /// Whether a second handshake offering the opposite family first
    /// landed on a different suite, i.e. the server follows client order.
    /// `None` when the second handshake could not be completed.
    pub tls_honors_client_cipher_order: Option<bool>,
}

pub async fn probe_server_connection(config: &ServerConfig) -> ServerConnectivityResult {
    probe_server_connection_with_proxy(config, None).await
}

pub async fn probe_server_connection_with_proxy(
    config: &ServerConfig,
    proxy: Option<std::sync::Arc<weaver_tunnel::bridge::Bridge>>,
) -> ServerConnectivityResult {
    // Inspect an unadopted TLS server before the ordinary NNTP probe. A trusted
    // hostname mismatch stops here, before any greeting or credentials are
    // exchanged, and the first handshake supplies the exact candidate shown in
    // the server form.
    if config.tls
        && config.tls_name_mismatch_certificate_der.is_none()
        && let Ok(Some(certificate_der)) =
            weaver_nntp::tls::inspect_tls_name_mismatch_certificate_via(
                &config.host,
                config.port,
                config.tls_ca_cert.as_deref(),
                proxy.as_ref(),
            )
            .await
    {
        return ServerConnectivityResult {
            success: false,
            message: "We reached the server securely, but its certificate belongs to a different hostname. Review the certificate below only if you recognise this provider.".to_string(),
            latency_ms: None,
            first_byte_latency_ms: None,
            first_byte_latency_band: None,
            supports_pipelining: false,
            adoptable_tls_name_mismatch_certificate_der: Some(certificate_der),
            tls_cipher_suite: None,
            tls_honors_client_cipher_order: None,
        };
    }

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
            let first_byte_latency = measure_first_byte_latency(&mut conn).await;
            let _ = conn.quit().await;
            let tls_honors_client_cipher_order = match tls_cipher_suite.as_deref() {
                Some(suite) => probe_cipher_order_honoured(&nntp_config, suite, pipelining).await,
                None => None,
            };
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
            ServerConnectivityResult {
                success: false,
                message: user_facing_connection_error(&error),
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

/// Time the command-to-status-line round trip on a session that is already
/// open, using the cheapest single-line command there is. This is the distance
/// figure the download lanes work against, so the operator sees the same number
/// the depth explorer does rather than a connect time dominated by TLS.
///
/// The best of a few tries is taken: a scheduler hiccup or a coalesced ACK can
/// only inflate a sample, never shorten one below the real round trip.
async fn measure_first_byte_latency(conn: &mut weaver_nntp::NntpConnection) -> Option<Duration> {
    const PROBES: usize = 3;
    let mut best: Option<Duration> = None;
    for _ in 0..PROBES {
        let started = std::time::Instant::now();
        if conn.ping().await.is_err() {
            break;
        }
        let sample = started.elapsed();
        best = Some(best.map_or(sample, |best: Duration| best.min(sample)));
    }
    best
}

/// Descriptive label for a first-byte latency, on the same thresholds the
/// download depth explorer uses.
fn latency_band_label(latency: Duration) -> &'static str {
    crate::pipeline::download::transport::LatencyBand::from_latency(latency).label()
}

/// Reconnect once offering the opposite AEAD family first. A server that
/// follows the client's order then lands on a different suite; a server
/// with a fixed order of its own answers with the same suite again. The
/// answer is recorded so the operator can see whether the CPU-derived
/// preference actually decides which suite carries this server's traffic.
async fn probe_cipher_order_honoured(
    base: &weaver_nntp::ServerConfig,
    negotiated: &str,
    supports_pipelining: bool,
) -> Option<bool> {
    let config = weaver_nntp::ServerConfig {
        tls_cipher_preference: weaver_nntp::TlsCipherPreference::opposing(negotiated),
        pipelining: weaver_nntp::PipeliningCapability::Known(supports_pipelining),
        ..base.clone()
    };
    let mut conn = weaver_nntp::NntpConnection::connect(&config).await.ok()?;
    let opposing = conn.negotiated_cipher_suite();
    let _ = conn.quit().await;
    opposing.map(|suite| suite != negotiated)
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
