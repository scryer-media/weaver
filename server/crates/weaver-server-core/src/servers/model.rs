use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Stable identifier for CRUD operations.
    #[serde(default)]
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connections: u16,
    /// Whether this server is enabled. Defaults to true.
    #[serde(default = "default_true")]
    pub active: bool,
    /// Whether the server supports NNTP command pipelining (RFC 4644).
    /// Auto-detected when the server is added or tested.
    #[serde(default)]
    pub supports_pipelining: bool,
    /// Priority group (0 = primary, 1+ = backfill). Lower values tried first.
    #[serde(default)]
    pub priority: u32,
    /// Optional path to a PEM-encoded CA certificate to trust for TLS
    /// connections to this server (e.g. self-signed or internal CAs).
    #[serde(default)]
    pub tls_ca_cert: Option<std::path::PathBuf>,
}

fn default_true() -> bool {
    true
}
