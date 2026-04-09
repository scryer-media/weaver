use crate::servers::ServerConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServerRecord {
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connections: u16,
    pub active: bool,
    pub supports_pipelining: bool,
    pub priority: u32,
    pub tls_ca_cert: Option<String>,
}

impl ServerRecord {
    pub(crate) fn into_config(self) -> ServerConfig {
        ServerConfig {
            id: self.id,
            host: self.host,
            port: self.port,
            tls: self.tls,
            username: self.username,
            password: self.password,
            connections: self.connections,
            active: self.active,
            supports_pipelining: self.supports_pipelining,
            priority: self.priority,
            tls_ca_cert: self.tls_ca_cert.map(std::path::PathBuf::from),
        }
    }

    pub(crate) fn from_config(server: &ServerConfig) -> Self {
        Self {
            id: server.id,
            host: server.host.clone(),
            port: server.port,
            tls: server.tls,
            username: server.username.clone(),
            password: server.password.clone(),
            connections: server.connections,
            active: server.active,
            supports_pipelining: server.supports_pipelining,
            priority: server.priority,
            tls_ca_cert: server
                .tls_ca_cert
                .as_ref()
                .map(|path| path.display().to_string()),
        }
    }
}
