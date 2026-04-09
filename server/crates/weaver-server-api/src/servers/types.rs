use async_graphql::{InputObject, SimpleObject};

#[derive(Debug, Clone, SimpleObject)]
pub struct Server {
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub connections: u16,
    pub active: bool,
    pub supports_pipelining: bool,
    pub priority: u32,
    pub tls_ca_cert: Option<String>,
}

impl From<&weaver_server_core::servers::ServerConfig> for Server {
    fn from(s: &weaver_server_core::servers::ServerConfig) -> Self {
        Self {
            id: s.id,
            host: s.host.clone(),
            port: s.port,
            tls: s.tls,
            username: s.username.clone(),
            connections: s.connections,
            active: s.active,
            supports_pipelining: s.supports_pipelining,
            priority: s.priority,
            tls_ca_cert: s.tls_ca_cert.as_ref().map(|p| p.display().to_string()),
        }
    }
}

#[derive(Debug, InputObject)]
pub struct ServerInput {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connections: u16,
    #[graphql(default = true)]
    pub active: bool,
    #[graphql(default = 0)]
    pub priority: u16,
    pub tls_ca_cert: Option<String>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TestConnectionResult {
    pub success: bool,
    pub message: String,
    pub latency_ms: Option<u64>,
    pub supports_pipelining: bool,
}

impl From<weaver_server_core::servers::ServerConnectivityResult> for TestConnectionResult {
    fn from(result: weaver_server_core::servers::ServerConnectivityResult) -> Self {
        Self {
            success: result.success,
            message: result.message,
            latency_ms: result.latency_ms,
            supports_pipelining: result.supports_pipelining,
        }
    }
}
