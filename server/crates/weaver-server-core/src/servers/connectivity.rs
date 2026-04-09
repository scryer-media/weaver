use crate::servers::ServerConfig;

#[derive(Debug, Clone)]
pub struct ServerConnectivityResult {
    pub success: bool,
    pub message: String,
    pub latency_ms: Option<u64>,
    pub supports_pipelining: bool,
}

pub async fn probe_server_connection(config: &ServerConfig) -> ServerConnectivityResult {
    let nntp_config = weaver_nntp::ServerConfig {
        host: config.host.clone(),
        port: config.port,
        tls: config.tls,
        username: config.username.clone(),
        password: config.password.clone(),
        tls_ca_cert: config.tls_ca_cert.clone(),
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
            }
        }
        Err(error) => ServerConnectivityResult {
            success: false,
            message: format!("{error}"),
            latency_ms: None,
            supports_pipelining: false,
        },
    }
}
