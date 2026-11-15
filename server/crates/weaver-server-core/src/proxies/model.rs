// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use weaver_tunnel::{
    Http3ProxyCredentials, Http3TunnelProvider, Http3TunnelSpec, TunnelError, TunnelSpec,
    WireGuardSpec,
    dns::{DnsServer, Trust, Wire},
    parse_key,
    pipe::{Egress, Naming, PortForwarding, gateway_of},
};
// Which wire a server is asked over, as a lookup's record names it.
pub use weaver_tunnel::dns::WireKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProxyKind {
    HttpConnect,
    Http3Connect,
    Socks5,
    Ssh,
    WireGuard,
}

// How a name dialled through a proxy is resolved: the next hop's endpoint,
// or the destination when the proxy is the last hop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyDns {
    pub who: DnsWho,
    // Where the listed servers are asked from. Only Servers asks them.
    pub path: DnsPath,
    // Asked in order. Only Servers has any.
    pub servers: Vec<ProxyDnsServer>,
    // How the listed servers' answers are judged. Only Servers asks them.
    pub trust: DnsTrust,
}

// Who answers: the host's own DNS, the proxy, or the listed servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DnsWho {
    Host,
    Proxy,
    Servers,
}

// Where the listed servers are asked from: the route's egress, or through
// the proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DnsPath {
    Direct,
    ThroughProxy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DnsTrust {
    AsSent,
    // An answer the server did not mark authenticated is refused.
    RequireAd,
    // Weaver checks the DNSSEC signatures itself: an unsigned name resolves,
    // one that fails the check does not.
    Validate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyDnsServer {
    pub address: SocketAddr,
    pub wire: ProxyDnsWire,
}

// How a server is asked: in the clear, over TLS (its certificate checked
// against `name`, or its address without one), or over HTTPS at `path`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProxyDnsWire {
    Plain,
    Tls { name: Option<String> },
    Https { path: String },
}

// The most servers one DNS setting lists: a proxy's, or an egress's.
pub const MAX_DNS_SERVERS: usize = 10;
// The most tunnel addresses a WireGuard proxy takes.
pub const MAX_TUNNEL_ADDRESSES: usize = 8;
const MAX_TLS_NAME_BYTES: usize = 253;
const MAX_HTTPS_PATH_BYTES: usize = 2048;

// How a name dialled straight out of an egress, with no proxy above it, is
// resolved: by the host's own DNS, or by the listed servers asked from the
// egress. Every direct leg, direct fallback and first proxy endpoint on the
// egress resolves this way.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressDns {
    pub who: DnsWho,
    // Asked in order. Only Servers has any.
    pub servers: Vec<ProxyDnsServer>,
    // How the listed servers' answers are judged. Only Servers asks them.
    pub trust: DnsTrust,
}

impl Default for EgressDns {
    fn default() -> Self {
        Self::host()
    }
}

impl EgressDns {
    // The host answers.
    pub fn host() -> Self {
        Self {
            who: DnsWho::Host,
            servers: Vec::new(),
            trust: DnsTrust::AsSent,
        }
    }
    pub fn is_host(&self) -> bool {
        self.who == DnsWho::Host
    }
    // The setting as it is saved: what only listed servers use is dropped
    // when the host answers, so two settings that behave the same are stored
    // the same.
    pub fn normal(self) -> Self {
        match self.who {
            DnsWho::Host => Self::host(),
            DnsWho::Proxy | DnsWho::Servers => self,
        }
    }
    pub fn validate(&self) -> Result<(), super::NetworkRefusal> {
        match self.who {
            DnsWho::Host => Ok(()),
            // No proxy sits above a direct dial to hand the name to.
            DnsWho::Proxy => Err(super::NetworkRefusal::EgressDnsWhoInvalid),
            DnsWho::Servers => validate_servers(&self.servers),
        }
    }
    // The listed servers in the engine's terms, and how their answers are
    // judged; None when the host answers.
    pub fn engine(&self) -> Option<(Vec<DnsServer>, Trust)> {
        (self.who == DnsWho::Servers).then(|| {
            (
                self.servers.iter().map(ProxyDnsServer::engine).collect(),
                self.trust.engine(),
            )
        })
    }
    // Who resolves a name dialled straight out of the egress.
    pub fn resolved_by(&self) -> super::ResolvedBy {
        match self.who {
            DnsWho::Servers => super::ResolvedBy::Servers,
            DnsWho::Host | DnsWho::Proxy => super::ResolvedBy::Host,
        }
    }
}

impl DnsTrust {
    pub fn engine(self) -> Trust {
        match self {
            Self::AsSent => Trust::AsSent,
            Self::RequireAd => Trust::RequireAd,
            Self::Validate => Trust::Validate,
        }
    }
}

// What every DNS setting checks of the servers it lists: at least one, at
// most `MAX_DNS_SERVERS`, no plain server after an encrypted one, and each
// server well formed.
fn validate_servers(servers: &[ProxyDnsServer]) -> Result<(), super::NetworkRefusal> {
    use super::NetworkRefusal as Refusal;
    if servers.is_empty() {
        return Err(Refusal::ProxyDnsServersRequired);
    }
    if servers.len() > MAX_DNS_SERVERS {
        return Err(Refusal::ProxyDnsServerCount);
    }
    // A name asked of an encrypted server that failed must not then go out
    // in clear: a plain server may only come first.
    let first_encrypted = servers
        .iter()
        .position(|server| server.wire != ProxyDnsWire::Plain);
    if let Some(first) = first_encrypted
        && servers[first..]
            .iter()
            .any(|server| server.wire == ProxyDnsWire::Plain)
    {
        return Err(Refusal::ProxyDnsPlainAfterEncrypted);
    }
    servers.iter().try_for_each(ProxyDnsServer::validate)?;
    // The same address, port and wire, with the same TLS name or HTTPS path,
    // is one server; asking it twice only repeats a failure. The list holds
    // at most ten, so comparing every pair costs nothing.
    if servers
        .iter()
        .enumerate()
        .any(|(at, server)| servers[..at].contains(server))
    {
        return Err(Refusal::ProxyDnsServerDuplicate);
    }
    Ok(())
}

impl ProxyDns {
    // The host answers.
    pub fn host() -> Self {
        Self {
            who: DnsWho::Host,
            path: DnsPath::ThroughProxy,
            servers: Vec::new(),
            trust: DnsTrust::AsSent,
        }
    }
    // The proxy is handed the name.
    pub fn proxy() -> Self {
        Self {
            who: DnsWho::Proxy,
            ..Self::host()
        }
    }
    // These servers, asked plain on port 53 from `path`, their answers taken
    // as sent.
    pub fn plain(path: DnsPath, ips: impl IntoIterator<Item = IpAddr>) -> Self {
        Self {
            who: DnsWho::Servers,
            path,
            servers: ips
                .into_iter()
                .map(|ip| ProxyDnsServer {
                    address: SocketAddr::new(ip, WireKind::Plain.default_port()),
                    wire: ProxyDnsWire::Plain,
                })
                .collect(),
            trust: DnsTrust::AsSent,
        }
    }
    // The settings as they are saved: what only listed servers use is
    // dropped when no servers answer, so two settings that behave the same
    // are stored the same.
    pub fn normal(self) -> Self {
        match self.who {
            DnsWho::Host => Self::host(),
            DnsWho::Proxy => Self::proxy(),
            DnsWho::Servers => self,
        }
    }
    fn engine_servers(&self) -> Vec<DnsServer> {
        self.servers.iter().map(ProxyDnsServer::engine).collect()
    }
    fn engine_trust(&self) -> Trust {
        self.trust.engine()
    }
    fn validate(&self, kind: ProxyKind) -> Result<(), super::NetworkRefusal> {
        use super::NetworkRefusal as Refusal;
        match self.who {
            // A WireGuard peer is handed packets, never a name.
            DnsWho::Proxy if kind == ProxyKind::WireGuard => {
                Err(Refusal::ProxyWireGuardNeedsDnsServers)
            }
            DnsWho::Host | DnsWho::Proxy => Ok(()),
            DnsWho::Servers => validate_servers(&self.servers),
        }
    }
}

impl ProxyDnsServer {
    pub fn engine(&self) -> DnsServer {
        DnsServer {
            address: self.address,
            wire: match &self.wire {
                ProxyDnsWire::Plain => Wire::Plain,
                ProxyDnsWire::Tls { name } => Wire::Tls { name: name.clone() },
                ProxyDnsWire::Https { path } => Wire::Https { path: path.clone() },
            },
        }
    }
    fn validate(&self) -> Result<(), super::NetworkRefusal> {
        use super::NetworkRefusal as Refusal;
        if self.address.port() == 0 {
            return Err(Refusal::ProxyDnsServerInvalid);
        }
        match &self.wire {
            ProxyDnsWire::Plain | ProxyDnsWire::Tls { name: None } => Ok(()),
            ProxyDnsWire::Tls { name: Some(name) } if is_tls_name(name) => Ok(()),
            ProxyDnsWire::Tls { .. } => Err(Refusal::ProxyDnsTlsNameInvalid),
            ProxyDnsWire::Https { path } if is_https_path(path) => Ok(()),
            ProxyDnsWire::Https { .. } => Err(Refusal::ProxyDnsUrlInvalid),
        }
    }
}

// A name a certificate is issued for: labels of letters, digits, hyphens
// and underscores, at most 63 bytes each and not starting or ending with a
// hyphen, with one optional trailing dot, 253 bytes in all.
pub fn is_tls_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_TLS_NAME_BYTES {
        return false;
    }
    let name = name.strip_suffix('.').unwrap_or(name);
    !name.is_empty()
        && name.split('.').all(|label| {
            let bytes = label.as_bytes();
            (1..=63).contains(&bytes.len())
                && bytes
                    .iter()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
                && bytes.first() != Some(&b'-')
                && bytes.last() != Some(&b'-')
        })
}

// The path a DNS over HTTPS query is posted to, its query string included:
// from a leading '/', only letters, digits and the characters a URI path or
// query carries as written (unreserved, sub-delims, ':', '@', '/', '%' and
// '?'), 2048 bytes at most. A request can always be built for it.
pub fn is_https_path(path: &str) -> bool {
    path.starts_with('/')
        && path.len() <= MAX_HTTPS_PATH_BYTES
        && path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@/%?".contains(&b))
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    pub dns: ProxyDns,
    pub tunnel_addresses: Vec<String>,
    pub peer_public_key: Option<String>,
    pub mtu: u16,
    pub keepalive_seconds: Option<u16>,
    pub timeout_seconds: u32,
    pub host_key_fingerprint: Option<String>,
    pub revision: u64,
    // WireGuard: the gateway at the tunnel's far end forwards a port of its
    // choosing over NAT-PMP.
    #[serde(default)]
    pub natpmp: bool,
    // WireGuard: the port the provider forwards to the tunnel, always the
    // same one. Unused with NAT-PMP.
    #[serde(default)]
    pub forwarded_port: Option<u16>,
    #[serde(skip)]
    pub secrets: ProxySecrets,
}

impl ProxyProfile {
    /// The revision a save of `self` over `existing` stores. Everything built
    /// from a profile is keyed by its revision, so a save that changes no
    /// field keeps it, and its tunnels, sessions and legs stay as they are.
    /// `None` once the revisions are exhausted.
    pub fn saved_revision(&self, existing: Option<&ProxyProfile>) -> Option<u64> {
        let Some(existing) = existing else {
            return Some(1);
        };
        let settings = |profile: &ProxyProfile| {
            serde_json::to_value(ProxyProfile {
                revision: 0,
                ..profile.clone()
            })
            .ok()
        };
        if self.secrets == existing.secrets
            && settings(self).is_some()
            && settings(self) == settings(existing)
        {
            return Some(existing.revision);
        }
        existing.revision.checked_add(1)
    }
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(u64::from(self.timeout_seconds))
    }
    // How a hop through this proxy resolves names, Weaver's servers asked
    // from `egress`. The host answering means the egress's own naming: on an
    // egress that lists servers those servers answer, sent on its binding,
    // from the one naming the egress holds; on any other, the host's DNS.
    pub fn naming(&self, egress: Arc<Egress>) -> Naming {
        let dns = &self.dns;
        match (dns.who, dns.path) {
            (DnsWho::Host, _) => egress.naming().cloned().unwrap_or(Naming::Host),
            (DnsWho::Proxy, _) => Naming::Proxy,
            (DnsWho::Servers, DnsPath::Direct) => {
                Naming::servers(dns.engine_servers(), dns.engine_trust(), egress)
            }
            (DnsWho::Servers, DnsPath::ThroughProxy) => {
                Naming::proxy_servers(dns.engine_servers(), dns.engine_trust())
            }
        }
    }
    // Who resolves a destination's name dialled through this proxy as the
    // last hop on an egress whose DNS is `egress`, as `naming` resolves it:
    // the host answering is the egress's servers when it lists them.
    pub fn resolved_on(&self, egress: &EgressDns) -> super::ResolvedBy {
        match self.resolved_by() {
            super::ResolvedBy::Host => egress.resolved_by(),
            by => by,
        }
    }
    // Who resolves a destination's name dialled through this proxy as the
    // last hop on an egress that asks the host, as `naming` resolves it.
    pub fn resolved_by(&self) -> super::ResolvedBy {
        use super::ResolvedBy;
        match (self.dns.who, self.dns.path) {
            (DnsWho::Host, _) => ResolvedBy::Host,
            (DnsWho::Proxy, _) => ResolvedBy::Proxy,
            (DnsWho::Servers, DnsPath::Direct) => ResolvedBy::Servers,
            (DnsWho::Servers, DnsPath::ThroughProxy) => ResolvedBy::ProxyServers,
        }
    }
    // The DNS servers asked through the proxy itself.
    pub fn proxy_dns_servers(&self) -> &[ProxyDnsServer] {
        match (self.dns.who, self.dns.path) {
            (DnsWho::Servers, DnsPath::ThroughProxy) => &self.dns.servers,
            _ => &[],
        }
    }
    // The servers the WireGuard tunnel's own resolver may ask, plain on port
    // 53 from inside it. Only a list that asks nothing more of its servers
    // qualifies: one encrypted server, another port or an answer that must be
    // authenticated or validated here leaves the tunnel resolver with no server at all, so no
    // name can reach a listed server in clear by that path.
    fn wireguard_resolver_ips(&self) -> Vec<IpAddr> {
        let servers = self.proxy_dns_servers();
        let plain_only = self.dns.trust == DnsTrust::AsSent
            && servers
                .iter()
                .all(|server| server.engine() == DnsServer::plain(server.address.ip()));
        if !plain_only {
            return Vec::new();
        }
        servers.iter().map(|server| server.address.ip()).collect()
    }
    // What a save checks of the profile's own settings, apart from its
    // secrets: what can be judged of a profile whose secrets are not in hand.
    pub fn validate_config(&self) -> Result<(), super::NetworkRefusal> {
        use super::NetworkRefusal as Refusal;
        if self.name.trim().is_empty() || self.name.len() > 128 {
            return Err(Refusal::ProxyNameLength);
        }
        if self.host.is_empty()
            || self.host.len() > 253
            || self.host.bytes().any(|b| {
                b.is_ascii_whitespace() || b.is_ascii_control() || matches!(b, b'/' | b'@' | b'\\')
            })
            || self.port == 0
        {
            return Err(Refusal::ProxyEndpointInvalid);
        }
        if !(1..=300).contains(&self.timeout_seconds) {
            return Err(Refusal::ProxyTimeoutRange);
        }
        if self.tunnel_addresses.len() > MAX_TUNNEL_ADDRESSES {
            return Err(Refusal::ProxyAddressCount);
        }
        self.dns.validate(self.kind)
    }
    // Everything a save checks: the profile's own settings, then those that
    // need its secrets, such as a WireGuard or SSH key.
    pub fn validate(&self) -> Result<(), super::NetworkRefusal> {
        use super::NetworkRefusal as Refusal;
        self.validate_config()?;
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
                return Err(Refusal::ProxyCredentialTooLarge);
            }
        }
        match self.kind {
            ProxyKind::WireGuard => self.wireguard_spec()?.validate()?,
            ProxyKind::Ssh => {
                if self.secrets.username.as_ref().is_none_or(|v| v.is_empty()) {
                    return Err(Refusal::ProxySshUsernameRequired);
                }
                if let Some(key) = &self.secrets.private_key {
                    weaver_tunnel::validate_private_key(key, self.secrets.passphrase.as_deref())?;
                } else {
                    return Err(Refusal::ProxySshKeyRequired);
                }
            }
            ProxyKind::Socks5 => {
                // A username is what carries the password on the wire, so a
                // password alone would be dropped and the proxy dialled
                // unauthenticated.
                if self.secrets.password.is_some() && self.secrets.username.is_none() {
                    return Err(Refusal::ProxySocks5PasswordNeedsUsername);
                }
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
                    return Err(Refusal::ProxySocks5CredentialLength);
                }
            }
            ProxyKind::Http3Connect => {
                // Construction validates authority, credentials and timeout;
                // sessions are lazy, so saving a profile does not use networking.
                if self.secrets.username.is_none() && self.secrets.password.is_some() {
                    return Err(Refusal::ProxyHttp3PasswordNeedsUsername);
                }
                Http3TunnelProvider::new(self.http3_spec()?)?;
            }
            ProxyKind::HttpConnect => {
                if self
                    .secrets
                    .username
                    .as_ref()
                    .is_some_and(|v| v.contains(':'))
                {
                    return Err(Refusal::ProxyHttpUsernameColon);
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
            private_key_pem: self.secrets.private_key.clone().unwrap_or_default(),
            private_key_passphrase: self.secrets.passphrase.clone(),
            pinned_host_key: self.host_key_fingerprint.clone(),
            request_timeout: self.timeout(),
        }
    }
    pub fn http3_spec(&self) -> Result<Http3TunnelSpec, TunnelError> {
        if self.secrets.username.is_none() && self.secrets.password.is_some() {
            return Err(TunnelError::Configuration(
                super::NetworkRefusal::ProxyHttp3PasswordNeedsUsername.to_string(),
            ));
        }
        Ok(Http3TunnelSpec {
            proxy_config_id: self.id.to_string(),
            revision: self.revision.to_string(),
            host: self.host.clone(),
            port: self.port,
            credentials: self
                .secrets
                .username
                .as_ref()
                .map(|username| {
                    Http3ProxyCredentials::new(
                        username.clone(),
                        self.secrets.password.clone().unwrap_or_default(),
                    )
                })
                .transpose()?,
            request_timeout: self.timeout(),
        })
    }
    // How the provider at this WireGuard proxy's far end forwards a port to
    // the tunnel. Its NAT-PMP gateway is host 1 of the network of the
    // tunnel's first IPv4 address; a tunnel without one has no gateway to ask.
    pub fn port_forwarding(&self) -> PortForwarding {
        if self.kind != ProxyKind::WireGuard {
            return PortForwarding::None;
        }
        if self.natpmp {
            return self
                .tunnel_addresses
                .iter()
                .filter_map(|address| address.parse::<weaver_tunnel::IpCidr>().ok())
                .find_map(|cidr| match cidr.address {
                    IpAddr::V4(address) => Some(PortForwarding::NatPmp {
                        gateway: gateway_of(address),
                    }),
                    IpAddr::V6(_) => None,
                })
                .unwrap_or(PortForwarding::None);
        }
        self.forwarded_port
            .filter(|port| *port != 0)
            .map_or(PortForwarding::None, PortForwarding::Fixed)
    }
    pub fn wireguard_spec(&self) -> Result<WireGuardSpec, TunnelError> {
        self.wireguard_spec_capped(super::wireguard_socket_cap())
    }
    // The tunnel this profile describes, holding at most `max_sockets`
    // sockets open.
    pub fn wireguard_spec_capped(&self, max_sockets: usize) -> Result<WireGuardSpec, TunnelError> {
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
            dns_servers: self.wireguard_resolver_ips(),
            mtu: self.mtu,
            persistent_keepalive: self.keepalive_seconds.filter(|v| *v != 0),
            request_timeout: self.timeout(),
            max_sockets,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutingPolicy {
    #[serde(default)]
    pub proxy_ids: Vec<u32>,
    #[serde(default)]
    pub allow_direct: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub legs: Vec<super::RouteLeg>,
    #[serde(default)]
    pub failover: super::Failover,
}
impl Default for RoutingPolicy {
    fn default() -> Self {
        Self {
            proxy_ids: Vec::new(),
            allow_direct: true,
            legs: Vec::new(),
            failover: super::Failover::Redistribute,
        }
    }
}
impl RoutingPolicy {
    pub fn route(&self) -> super::Route {
        if self.legs.is_empty() {
            super::Route::from_legacy(self)
        } else {
            super::Route {
                legs: self.legs.clone(),
                failover: self.failover,
            }
        }
    }

    pub fn from_route(
        route: super::Route,
        pools: &std::collections::HashMap<u32, super::ProxyPool>,
    ) -> Self {
        let mut proxy_ids = Vec::new();
        let allow_direct = match route.legs.first().map(|leg| &leg.path) {
            Some(super::LegPath::Direct) => true,
            Some(super::LegPath::Ladder {
                rungs,
                direct_fallback,
            }) => {
                for rung in rungs {
                    match rung {
                        super::Rung::Proxy { id } => proxy_ids.push(*id),
                        super::Rung::Chain { ids } => proxy_ids.extend(ids),
                        super::Rung::Pool { id } => {
                            if let Some(pool) = pools.get(id) {
                                proxy_ids.extend(&pool.member_ids);
                            }
                        }
                    }
                }
                *direct_fallback
            }
            None => false,
        };
        Self {
            proxy_ids,
            allow_direct,
            legs: route.legs,
            failover: route.failover,
        }
    }

    pub fn is_direct(&self) -> bool {
        if self.legs.is_empty() {
            self.proxy_ids.is_empty() && self.allow_direct
        } else {
            matches!(
                self.legs.as_slice(),
                [super::RouteLeg {
                    egress_id: 0,
                    path: super::LegPath::Direct,
                    ..
                }]
            )
        }
    }
    pub fn validate(&self) -> Result<(), super::NetworkRefusal> {
        if !self.legs.is_empty() {
            return self.route().validate_shape();
        }
        if self.proxy_ids.len() > 8 {
            return Err(super::NetworkRefusal::RoutingProxyCount);
        }
        let unique: HashSet<_> = self.proxy_ids.iter().collect();
        if unique.len() != self.proxy_ids.len() {
            return Err(super::NetworkRefusal::RoutingProxyRepeated);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Consumer {
    Server(u32),
    Rss(u32),
    // Default outbound: everything Weaver fetches over HTTP that is not a
    // server, a feed or a speed test. It always exists and has cap 1.
    Outbound,
    // Torrent peers and trackers.
    Torrent(TorrentScope),
}
impl Consumer {
    pub fn key(self) -> String {
        match self {
            Self::Server(id) => format!("server:{id}"),
            Self::Rss(id) => format!("rss:{id}"),
            Self::Outbound => "outbound".into(),
            Self::Torrent(TorrentScope::Default) => "torrent:default".into(),
            Self::Torrent(TorrentScope::Category(id)) => format!("torrent:category:{id}"),
            Self::Torrent(TorrentScope::TrackerRule(id)) => format!("torrent:tracker:{id}"),
        }
    }

    // Torrent scopes whose routes resolve to one leg share one session, so
    // they share the leg's stage: it lives while any of them routes over it.
    pub fn shares_legs(self) -> bool {
        matches!(self, Self::Torrent(_))
    }

    // A torrent's peers and trackers see one address for the whole run: its
    // route is one leg with one exit. A direct leg is its egress; a ladder
    // is one rung, a proxy, a chain ending at one proxy or a pool whose
    // torrents each stay on one member, and never falls back to the egress.
    // With one leg there is nothing to fail over to, so the route's failover
    // means nothing here.
    pub fn validate_route(self, route: &super::Route) -> Result<(), super::NetworkRefusal> {
        use super::{NetworkRefusal, TorrentRouteFault};
        if !matches!(self, Self::Torrent(_)) {
            return Ok(());
        }
        TorrentRouteFault::validate(route).map_err(NetworkRefusal::TorrentRouteExit)
    }
}

// Which torrents a torrent consumer carries: every torrent whose tracker
// rule and category have no route of their own, one category's, or one
// tracker rule's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TorrentScope {
    Default,
    Category(u32),
    TrackerRule(u32),
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
    pub failures: Vec<(u32, super::ProxyFault)>,
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

#[cfg(test)]
mod dns_tests {
    use super::*;
    use crate::proxies::NetworkRefusal as R;

    fn socks(dns: ProxyDns) -> ProxyProfile {
        ProxyProfile {
            id: 1,
            name: "proxy".into(),
            kind: ProxyKind::Socks5,
            enabled: true,
            host: "192.0.2.1".into(),
            port: 1080,
            dns,
            tunnel_addresses: vec![],
            peer_public_key: None,
            mtu: 1420,
            keepalive_seconds: None,
            timeout_seconds: 30,
            host_key_fingerprint: None,
            revision: 0,
            natpmp: false,
            forwarded_port: None,
            secrets: ProxySecrets::default(),
        }
    }

    fn servers(path: DnsPath, trust: DnsTrust, wires: Vec<ProxyDnsWire>) -> ProxyDns {
        ProxyDns {
            who: DnsWho::Servers,
            path,
            servers: wires
                .into_iter()
                .enumerate()
                .map(|(index, wire)| ProxyDnsServer {
                    address: SocketAddr::new([192, 0, 2, 53 + index as u8].into(), 853),
                    wire,
                })
                .collect(),
            trust,
        }
    }

    fn plain() -> ProxyDnsWire {
        ProxyDnsWire::Plain
    }

    fn tls() -> ProxyDnsWire {
        ProxyDnsWire::Tls {
            name: Some("dns.example".into()),
        }
    }

    fn https() -> ProxyDnsWire {
        ProxyDnsWire::Https {
            path: "/dns-query".into(),
        }
    }

    fn checked(wires: Vec<ProxyDnsWire>) -> Result<(), R> {
        socks(servers(DnsPath::ThroughProxy, DnsTrust::AsSent, wires)).validate()
    }

    // A lookup never goes out in clear once an encrypted server failed it.
    #[test]
    fn a_plain_server_may_lead_but_never_follow_an_encrypted_one() {
        assert_eq!(
            checked(vec![tls(), plain()]),
            Err(R::ProxyDnsPlainAfterEncrypted)
        );
        assert_eq!(
            checked(vec![https(), plain()]),
            Err(R::ProxyDnsPlainAfterEncrypted)
        );
        assert_eq!(
            checked(vec![plain(), tls(), plain()]),
            Err(R::ProxyDnsPlainAfterEncrypted)
        );
        assert_eq!(checked(vec![plain(), tls()]), Ok(()));
        assert_eq!(checked(vec![plain(), plain()]), Ok(()));
        assert_eq!(checked(vec![tls(), https()]), Ok(()));
    }

    #[test]
    fn a_server_list_is_bounded_and_each_server_well_formed() {
        assert_eq!(checked(vec![]), Err(R::ProxyDnsServersRequired));
        assert_eq!(checked(vec![plain(); MAX_DNS_SERVERS]), Ok(()));
        assert_eq!(checked(vec![plain(); 10]), Ok(()));
        assert_eq!(checked(vec![plain(); 11]), Err(R::ProxyDnsServerCount));
        // Ten encrypted servers after a plain one are eleven.
        let mut wires = vec![plain()];
        wires.extend(vec![tls(); 10]);
        assert_eq!(checked(wires), Err(R::ProxyDnsServerCount));

        let mut zero = servers(DnsPath::ThroughProxy, DnsTrust::AsSent, vec![plain()]);
        zero.servers[0].address.set_port(0);
        assert_eq!(socks(zero).validate(), Err(R::ProxyDnsServerInvalid));

        let tls_named = |name: &str| ProxyDnsWire::Tls {
            name: Some(name.into()),
        };
        assert_eq!(checked(vec![ProxyDnsWire::Tls { name: None }]), Ok(()));
        assert_eq!(checked(vec![tls_named("dns.example.")]), Ok(()));
        for bad in [
            "",
            "not a name",
            "-dns.example",
            "dns..example",
            "dns\u{e9}.example",
        ] {
            assert_eq!(
                checked(vec![tls_named(bad)]),
                Err(R::ProxyDnsTlsNameInvalid),
                "{bad:?}"
            );
        }
        assert_eq!(checked(vec![tls_named(&["a"; 127].join("."))]), Ok(()));
        assert_eq!(
            checked(vec![tls_named(&["a"; 128].join("."))]),
            Err(R::ProxyDnsTlsNameInvalid)
        );

        let path = |path: &str| ProxyDnsWire::Https { path: path.into() };
        assert_eq!(checked(vec![path("/dns-query?ct=1")]), Ok(()));
        assert_eq!(
            checked(vec![path(&format!("/{}", "a".repeat(2047)))]),
            Ok(())
        );
        for bad in [
            String::new(),
            "dns-query".into(),
            "/dns query".into(),
            "/dns\tquery".into(),
            "/dns-query#top".into(),
            "/d\u{e9}".into(),
            format!("/{}", "a".repeat(2048)),
        ] {
            assert_eq!(
                checked(vec![path(&bad)]),
                Err(R::ProxyDnsUrlInvalid),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn who_answers_decides_what_a_wireguard_proxy_may_save() {
        let wireguard = |dns: &ProxyDns| dns.validate(ProxyKind::WireGuard);
        assert_eq!(
            wireguard(&ProxyDns::proxy()),
            Err(R::ProxyWireGuardNeedsDnsServers)
        );
        assert_eq!(wireguard(&ProxyDns::host()), Ok(()));
        // Servers asked from the egress are taken from a WireGuard proxy, and
        // a plain server asked there may still require authenticated answers.
        let direct = servers(DnsPath::Direct, DnsTrust::RequireAd, vec![plain()]);
        assert_eq!(wireguard(&direct), Ok(()));
        assert_eq!(socks(direct).validate(), Ok(()));
    }

    #[test]
    fn a_saved_dns_says_who_resolves_and_what_the_tunnel_asks() {
        let through = servers(DnsPath::ThroughProxy, DnsTrust::AsSent, vec![plain()]);
        let direct = servers(DnsPath::Direct, DnsTrust::AsSent, vec![plain()]);
        assert_eq!(
            socks(ProxyDns::host()).resolved_by(),
            super::super::ResolvedBy::Host
        );
        assert_eq!(
            socks(ProxyDns::proxy()).resolved_by(),
            super::super::ResolvedBy::Proxy
        );
        assert_eq!(
            socks(direct.clone()).resolved_by(),
            super::super::ResolvedBy::Servers
        );
        assert_eq!(
            socks(through.clone()).resolved_by(),
            super::super::ResolvedBy::ProxyServers
        );
        assert_eq!(
            socks(through.clone()).proxy_dns_servers(),
            &through.servers[..]
        );
        assert!(socks(direct).proxy_dns_servers().is_empty());
    }

    #[test]
    fn a_dns_that_names_no_servers_is_stored_in_one_form() {
        let loose = ProxyDns {
            who: DnsWho::Host,
            path: DnsPath::Direct,
            servers: vec![ProxyDnsServer {
                address: "192.0.2.53:53".parse().unwrap(),
                wire: plain(),
            }],
            trust: DnsTrust::RequireAd,
        };
        assert_eq!(loose.clone().normal(), ProxyDns::host());
        let kept = servers(DnsPath::Direct, DnsTrust::RequireAd, vec![tls()]);
        assert_eq!(kept.clone().normal(), kept);
        let validated = ProxyDns {
            trust: DnsTrust::Validate,
            ..loose
        };
        assert_eq!(validated.normal(), ProxyDns::host());
        let kept = servers(DnsPath::ThroughProxy, DnsTrust::Validate, vec![plain()]);
        assert_eq!(kept.clone().normal(), kept);
    }

    // Validating here asks listed servers like the other trusts do: it needs
    // one, and the engine is told to validate.
    #[test]
    fn validating_here_needs_listed_servers_and_reaches_the_engine() {
        let none = servers(DnsPath::Direct, DnsTrust::Validate, Vec::new());
        assert_eq!(
            none.validate(ProxyKind::Socks5),
            Err(R::ProxyDnsServersRequired)
        );
        let listed = servers(DnsPath::Direct, DnsTrust::Validate, vec![plain()]);
        assert_eq!(listed.validate(ProxyKind::Socks5), Ok(()));
        assert_eq!(listed.validate(ProxyKind::WireGuard), Ok(()));
        assert_eq!(listed.engine_trust(), Trust::Validate);
        let stored = serde_json::to_value(&listed).unwrap();
        assert_eq!(stored["trust"], "Validate");
        assert_eq!(serde_json::from_value::<ProxyDns>(stored).unwrap(), listed);
    }

    // A WireGuard profile whose DNS servers are `listed`, each at its own
    // address and port, asked through the tunnel with `trust`.
    fn wireguard(trust: DnsTrust, listed: Vec<(u16, ProxyDnsWire)>) -> ProxyProfile {
        let mut profile = socks(ProxyDns {
            who: DnsWho::Servers,
            path: DnsPath::ThroughProxy,
            servers: listed
                .into_iter()
                .enumerate()
                .map(|(index, (port, wire))| ProxyDnsServer {
                    address: SocketAddr::new([10, 64, 0, 1 + index as u8].into(), port),
                    wire,
                })
                .collect(),
            trust,
        });
        profile.kind = ProxyKind::WireGuard;
        profile.port = 51820;
        profile.tunnel_addresses = vec!["10.63.0.2/32".into()];
        profile.peer_public_key = Some("AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=".into());
        profile.secrets.private_key = Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".into());
        profile
    }

    fn tunnel_resolver(profile: &ProxyProfile) -> Vec<IpAddr> {
        profile.validate().unwrap();
        profile.wireguard_spec_capped(8).unwrap().dns_servers
    }

    #[test]
    fn the_tunnel_resolver_gets_a_list_only_of_plain_servers_on_port_53() {
        let plain = wireguard(DnsTrust::AsSent, vec![(53, plain()), (53, plain())]);
        assert_eq!(
            tunnel_resolver(&plain),
            vec![IpAddr::from([10, 64, 0, 1]), IpAddr::from([10, 64, 0, 2])]
        );
    }

    #[test]
    fn the_tunnel_resolver_gets_no_server_a_name_could_reach_in_clear() {
        let cases = [
            ("tls", wireguard(DnsTrust::AsSent, vec![(853, tls())])),
            ("https", wireguard(DnsTrust::AsSent, vec![(443, https())])),
            ("tls on 53", wireguard(DnsTrust::AsSent, vec![(53, tls())])),
            (
                "plain then tls",
                wireguard(DnsTrust::AsSent, vec![(53, plain()), (853, tls())]),
            ),
            (
                "plain then https",
                wireguard(DnsTrust::AsSent, vec![(53, plain()), (443, https())]),
            ),
            (
                "plain off 53",
                wireguard(DnsTrust::AsSent, vec![(5353, plain())]),
            ),
            (
                "plain, one off 53",
                wireguard(DnsTrust::AsSent, vec![(53, plain()), (5353, plain())]),
            ),
            (
                "require ad",
                wireguard(DnsTrust::RequireAd, vec![(53, plain())]),
            ),
            (
                "validate",
                wireguard(DnsTrust::Validate, vec![(53, plain()), (53, plain())]),
            ),
        ];
        for (case, profile) in cases {
            assert!(tunnel_resolver(&profile).is_empty(), "{case}");
        }
    }

    // A DoH path is one the request can be built with: every path the check
    // accepts is an HTTP path and query, and every character a URI refuses
    // there is refused here.
    #[test]
    fn a_doh_path_holds_only_what_an_http_request_carries() {
        let every: String = (b'!'..=b'~').map(char::from).collect();
        let allowed: String = every
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || "-._~!$&'()*+,;=:@/%?".contains(*c))
            .collect();
        for accepted in [
            "/".to_owned(),
            "/dns-query".to_owned(),
            "/dns-query?ct=application/dns-message&x=1".to_owned(),
            format!("/{allowed}"),
            format!("/{}", "a".repeat(MAX_HTTPS_PATH_BYTES - 1)),
        ] {
            assert!(is_https_path(&accepted), "{accepted}");
            let uri: http::Uri = accepted.parse().unwrap();
            assert_eq!(uri.path_and_query().unwrap().as_str(), accepted);
        }
        for refused in "<>`\"{}|\\^[] #".chars() {
            let path = format!("/a{refused}b");
            assert!(!is_https_path(&path), "{path}");
        }
        for byte in every.chars().filter(|c| !allowed.contains(*c)) {
            let path = format!("/a{byte}b");
            assert!(!is_https_path(&path), "{path}");
        }
        assert!(!is_https_path("dns-query"));
        assert!(!is_https_path(""));
        assert!(!is_https_path(&format!(
            "/{}",
            "a".repeat(MAX_HTTPS_PATH_BYTES)
        )));
        assert!(!is_https_path("/a\u{e9}"));
    }

    fn egress_servers(trust: DnsTrust, wires: Vec<ProxyDnsWire>) -> EgressDns {
        let ProxyDns { servers, .. } = servers(DnsPath::Direct, trust, wires);
        EgressDns {
            who: DnsWho::Servers,
            servers,
            trust,
        }
    }

    // An egress's DNS keeps every rule a proxy's listed servers keep, and
    // has no proxy to hand a name to.
    #[test]
    fn an_egress_dns_keeps_the_server_rules_and_has_no_proxy() {
        assert_eq!(EgressDns::host().validate(), Ok(()));
        assert_eq!(EgressDns::default(), EgressDns::host());
        let proxy = EgressDns {
            who: DnsWho::Proxy,
            ..EgressDns::host()
        };
        assert_eq!(proxy.validate(), Err(R::EgressDnsWhoInvalid));
        let checked = |wires| egress_servers(DnsTrust::AsSent, wires).validate();
        assert_eq!(checked(vec![]), Err(R::ProxyDnsServersRequired));
        assert_eq!(checked(vec![plain(); 10]), Ok(()));
        assert_eq!(checked(vec![plain(); 11]), Err(R::ProxyDnsServerCount));
        assert_eq!(
            checked(vec![tls(), plain()]),
            Err(R::ProxyDnsPlainAfterEncrypted)
        );
        assert_eq!(checked(vec![plain(), tls(), https()]), Ok(()));
        let mut zero = egress_servers(DnsTrust::AsSent, vec![plain()]);
        zero.servers[0].address.set_port(0);
        assert_eq!(zero.validate(), Err(R::ProxyDnsServerInvalid));
        assert_eq!(
            checked(vec![ProxyDnsWire::Tls {
                name: Some("not a name".into())
            }]),
            Err(R::ProxyDnsTlsNameInvalid)
        );
        assert_eq!(
            checked(vec![ProxyDnsWire::Https {
                path: "dns-query".into()
            }]),
            Err(R::ProxyDnsUrlInvalid)
        );
    }

    // One address, port and wire, with one TLS name or HTTPS path, is one
    // server, and a proxy or an egress lists it once. The same address over
    // another wire, port, name or path is another server.
    #[test]
    fn a_server_is_listed_once_and_another_wire_is_another_server() {
        let at = |port, wire| ProxyDnsServer {
            address: SocketAddr::new([192, 0, 2, 53].into(), port),
            wire,
        };
        let name = |name: &str| ProxyDnsWire::Tls {
            name: Some(name.into()),
        };
        let path = |path: &str| ProxyDnsWire::Https { path: path.into() };
        let proxy = |list: Vec<ProxyDnsServer>| {
            socks(ProxyDns {
                servers: list,
                ..servers(DnsPath::ThroughProxy, DnsTrust::AsSent, vec![])
            })
            .validate()
        };
        let egress = |list: Vec<ProxyDnsServer>| {
            EgressDns {
                servers: list,
                ..egress_servers(DnsTrust::AsSent, vec![])
            }
            .validate()
        };
        let repeated = [
            vec![at(53, plain()), at(53, plain())],
            vec![at(53, plain()), at(853, tls()), at(853, tls())],
            vec![at(853, tls()), at(443, https()), at(853, tls())],
            vec![at(443, https()), at(443, https())],
        ];
        for list in repeated {
            assert_eq!(proxy(list.clone()), Err(R::ProxyDnsServerDuplicate));
            assert_eq!(egress(list), Err(R::ProxyDnsServerDuplicate));
        }
        let distinct = [
            vec![at(53, plain()), at(53, tls())],
            vec![at(53, plain()), at(5353, plain())],
            vec![at(853, name("a.example")), at(853, name("b.example"))],
            vec![
                at(853, ProxyDnsWire::Tls { name: None }),
                at(853, name("a.example")),
            ],
            vec![at(443, path("/dns-query")), at(443, path("/other"))],
        ];
        for list in distinct {
            assert_eq!(proxy(list.clone()), Ok(()));
            assert_eq!(egress(list), Ok(()));
        }
    }

    #[test]
    fn an_egress_dns_that_names_no_servers_is_stored_as_the_host() {
        let loose = EgressDns {
            who: DnsWho::Host,
            ..egress_servers(DnsTrust::Validate, vec![tls()])
        };
        assert_eq!(loose.clone().normal(), EgressDns::host());
        assert!(loose.normal().is_host());
        let kept = egress_servers(DnsTrust::RequireAd, vec![plain(), https()]);
        assert_eq!(kept.clone().normal(), kept);
        assert_eq!(kept.resolved_by(), super::super::ResolvedBy::Servers);
        assert_eq!(
            EgressDns::host().resolved_by(),
            super::super::ResolvedBy::Host
        );
        let (servers, trust) = kept.engine().unwrap();
        assert_eq!(trust, Trust::RequireAd);
        assert_eq!(servers.len(), 2);
        assert_eq!(
            servers[1].wire,
            Wire::Https {
                path: "/dns-query".into()
            }
        );
        assert!(EgressDns::host().engine().is_none());
        // The stored form reads back as written.
        let json = serde_json::to_string(&kept).unwrap();
        assert_eq!(serde_json::from_str::<EgressDns>(&json).unwrap(), kept);
    }
}
