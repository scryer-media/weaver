use serde::{Deserialize, Serialize};
use std::{collections::HashSet, net::IpAddr, time::Duration};
use weaver_tunnel::{TunnelError, TunnelSpec, WireGuardSpec, parse_key};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProxyKind {
    HttpConnect,
    Socks5,
    Ssh,
    WireGuard,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ProxySecrets {
    pub username: Option<String>,
    pub password: Option<String>,
    pub private_key: Option<String>,
    pub passphrase: Option<String>,
    pub preshared_key: Option<String>,
}
impl std::fmt::Debug for ProxySecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProxySecrets([REDACTED])")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyProfile {
    pub id: u32,
    pub name: String,
    pub kind: ProxyKind,
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub dns_servers: Vec<IpAddr>,
    pub tunnel_addresses: Vec<String>,
    pub peer_public_key: Option<String>,
    pub mtu: u16,
    pub keepalive_seconds: Option<u16>,
    pub timeout_seconds: u32,
    pub host_key_fingerprint: Option<String>,
    pub revision: u64,
    #[serde(skip)]
    pub secrets: ProxySecrets,
}

impl ProxyProfile {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(u64::from(self.timeout_seconds))
    }
    pub fn validate(&self) -> Result<(), TunnelError> {
        let invalid = |s: &str| TunnelError::Configuration(s.into());
        if self.name.trim().is_empty() || self.name.len() > 128 {
            return Err(invalid("proxy name must contain 1–128 characters"));
        }
        if self.host.is_empty()
            || self.host.len() > 253
            || self.host.bytes().any(|b| {
                b.is_ascii_whitespace() || b.is_ascii_control() || matches!(b, b'/' | b'@' | b'\\')
            })
            || self.port == 0
        {
            return Err(invalid("proxy endpoint must have a valid host and port"));
        }
        if !(1..=300).contains(&self.timeout_seconds) {
            return Err(invalid("proxy timeout must be between 1 and 300 seconds"));
        }
        if self.dns_servers.len() > 4 || self.tunnel_addresses.len() > 8 {
            return Err(invalid("too many DNS servers or tunnel addresses"));
        }
        for secret in [
            &self.secrets.username,
            &self.secrets.password,
            &self.secrets.private_key,
            &self.secrets.passphrase,
            &self.secrets.preshared_key,
        ]
        .into_iter()
        .flatten()
        {
            if secret.len() > 16384 {
                return Err(invalid("proxy credential is too large"));
            }
        }
        match self.kind {
            ProxyKind::WireGuard => self.wireguard_spec()?.validate()?,
            ProxyKind::Ssh => {
                if self.secrets.username.as_ref().is_none_or(|v| v.is_empty()) {
                    return Err(invalid("SSH requires a username"));
                }
                if let Some(key) = &self.secrets.private_key {
                    weaver_tunnel::validate_private_key(key, self.secrets.passphrase.as_deref())?;
                } else if self.secrets.password.is_none() {
                    return Err(invalid("SSH requires a password or Ed25519 private key"));
                }
            }
            ProxyKind::Socks5 => {
                if self
                    .secrets
                    .username
                    .as_ref()
                    .is_some_and(|v| v.is_empty() || v.len() > 255)
                    || self
                        .secrets
                        .password
                        .as_ref()
                        .is_some_and(|v| v.len() > 255)
                {
                    return Err(invalid("SOCKS5 credentials must fit in 255 bytes"));
                }
            }
            ProxyKind::HttpConnect => {
                if self
                    .secrets
                    .username
                    .as_ref()
                    .is_some_and(|v| v.contains(':'))
                {
                    return Err(invalid("HTTP proxy username cannot contain a colon"));
                }
            }
        }
        Ok(())
    }
    pub fn ssh_spec(&self) -> TunnelSpec {
        TunnelSpec {
            proxy_config_id: self.id.to_string(),
            proxy_name: self.name.clone(),
            revision: self.revision.to_string(),
            host: self.host.clone(),
            port: self.port,
            username: self.secrets.username.clone().unwrap_or_default(),
            password: self.secrets.password.clone(),
            private_key_pem: self.secrets.private_key.clone(),
            private_key_passphrase: self.secrets.passphrase.clone(),
            pinned_host_key: self.host_key_fingerprint.clone(),
            request_timeout: self.timeout(),
        }
    }
    pub fn wireguard_spec(&self) -> Result<WireGuardSpec, TunnelError> {
        Ok(WireGuardSpec {
            proxy_config_id: self.id.to_string(),
            proxy_name: self.name.clone(),
            revision: self.revision.to_string(),
            endpoint_host: self.host.clone(),
            endpoint_port: self.port,
            private_key: parse_key(self.secrets.private_key.as_deref().unwrap_or(""))?,
            peer_public_key: parse_key(self.peer_public_key.as_deref().unwrap_or(""))?,
            preshared_key: self
                .secrets
                .preshared_key
                .as_deref()
                .map(parse_key)
                .transpose()?,
            addresses: self
                .tunnel_addresses
                .iter()
                .map(|s| s.parse())
                .collect::<Result<_, _>>()?,
            dns_servers: self.dns_servers.clone(),
            mtu: self.mtu,
            persistent_keepalive: self.keepalive_seconds.filter(|v| *v != 0),
            request_timeout: self.timeout(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutingPolicy {
    pub proxy_ids: Vec<u32>,
    pub allow_direct: bool,
}
impl Default for RoutingPolicy {
    fn default() -> Self {
        Self {
            proxy_ids: Vec::new(),
            allow_direct: true,
        }
    }
}
impl RoutingPolicy {
    pub fn is_direct(&self) -> bool {
        self.proxy_ids.is_empty() && self.allow_direct
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.proxy_ids.len() > 8 {
            return Err("a route supports at most eight proxies".into());
        }
        let unique: HashSet<_> = self.proxy_ids.iter().collect();
        if unique.len() != self.proxy_ids.len() {
            return Err("a proxy cannot appear twice in a ladder".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Consumer {
    Server(u32),
    Rss(u32),
}
impl Consumer {
    pub fn key(self) -> String {
        match self {
            Self::Server(id) => format!("server:{id}"),
            Self::Rss(id) => format!("rss:{id}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteState {
    Idle,
    Proxy,
    Direct,
    Blocked,
}
#[derive(Debug, Clone)]
pub struct RoutingStatus {
    pub state: RouteState,
    pub selected_proxy_id: Option<u32>,
    pub failures: Vec<(u32, String)>,
}
impl Default for RoutingStatus {
    fn default() -> Self {
        Self {
            state: RouteState::Idle,
            selected_proxy_id: None,
            failures: Vec::new(),
        }
    }
}
