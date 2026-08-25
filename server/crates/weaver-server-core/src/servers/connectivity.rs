use crate::servers::ServerConfig;

#[derive(Debug, Clone)]
pub struct ServerConnectivityResult {
    pub success: bool,
    pub message: String,
    pub latency_ms: Option<u64>,
    pub supports_pipelining: bool,
    pub adoptable_tls_name_mismatch_certificate_der: Option<Vec<u8>>,
}

pub async fn probe_server_connection(config: &ServerConfig) -> ServerConnectivityResult {
    let nntp_config = weaver_nntp::ServerConfig {
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
            let _ = conn.quit().await;
            ServerConnectivityResult {
                success: true,
                message: "Connected successfully".to_string(),
                latency_ms: Some(latency),
                supports_pipelining: pipelining,
                adoptable_tls_name_mismatch_certificate_der: None,
            }
        }
        Err(error) => {
            let adoptable_tls_name_mismatch_certificate_der = if config.tls {
                weaver_nntp::tls::inspect_tls_name_mismatch_certificate(
                    &config.host,
                    config.port,
                    config.tls_ca_cert.as_deref(),
                )
                .await
                .ok()
                .flatten()
            } else {
                None
            };
            ServerConnectivityResult {
                success: false,
                message: format!("{error}"),
                latency_ms: None,
                supports_pipelining: false,
                adoptable_tls_name_mismatch_certificate_der,
            }
        }
    }
}
