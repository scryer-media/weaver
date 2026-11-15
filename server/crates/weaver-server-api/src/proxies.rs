// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::{
    auth::AdminGuard,
    observability::{refusal_error, spawn_blocking_db},
};
use async_graphql::{
    ComplexObject, Context, Enum, InputObject, MaybeUndefined, Object, Result, SimpleObject,
};
use std::{sync::Arc, time::Duration};
use weaver_server_core::{
    Database, SchedulerHandle,
    proxies::{self, Consumer, NetworkRefusal, ProxyProfile, ProxyRuntime, RoutingPolicy},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "ProxyKind")]
pub enum ProxyKind {
    HttpConnect,
    #[graphql(name = "HTTP3_CONNECT")]
    Http3Connect,
    #[graphql(name = "SOCKS5")]
    Socks5,
    Ssh,
    WireGuard,
}
impl From<ProxyKind> for proxies::ProxyKind {
    fn from(v: ProxyKind) -> Self {
        match v {
            ProxyKind::HttpConnect => Self::HttpConnect,
            ProxyKind::Http3Connect => Self::Http3Connect,
            ProxyKind::Socks5 => Self::Socks5,
            ProxyKind::Ssh => Self::Ssh,
            ProxyKind::WireGuard => Self::WireGuard,
        }
    }
}
impl From<proxies::ProxyKind> for ProxyKind {
    fn from(v: proxies::ProxyKind) -> Self {
        match v {
            proxies::ProxyKind::HttpConnect => Self::HttpConnect,
            proxies::ProxyKind::Http3Connect => Self::Http3Connect,
            proxies::ProxyKind::Socks5 => Self::Socks5,
            proxies::ProxyKind::Ssh => Self::Ssh,
            proxies::ProxyKind::WireGuard => Self::WireGuard,
        }
    }
}

/// Who answers a name dialled through a proxy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "ProxyDnsWho")]
pub enum ProxyDnsWho {
    /// The host's own DNS.
    Host,
    /// The proxy is handed the name.
    Proxy,
    /// The listed servers.
    Servers,
}
/// Where the listed servers are asked from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "ProxyDnsPath")]
pub enum ProxyDnsPath {
    /// From the route's egress, outside the proxy.
    Direct,
    ThroughProxy,
}
/// How the listed servers' answers are judged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "ProxyDnsTrust")]
pub enum ProxyDnsTrust {
    AsSent,
    /// An answer the server did not mark authenticated is refused.
    RequireAd,
    /// Weaver checks the DNSSEC signatures itself: an unsigned name
    /// resolves, one that fails the check does not.
    Validate,
}
/// How a query travels to one server.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "ProxyDnsWire")]
pub enum ProxyDnsWire {
    Plain,
    Tls,
    Https,
}
impl From<proxies::WireKind> for ProxyDnsWire {
    fn from(v: proxies::WireKind) -> Self {
        match v {
            proxies::WireKind::Plain => Self::Plain,
            proxies::WireKind::Tls => Self::Tls,
            proxies::WireKind::Https => Self::Https,
        }
    }
}

/// A proxy's DNS. Path, trust and servers mean something only when the
/// servers answer.
#[derive(Clone, PartialEq, SimpleObject)]
#[graphql(name = "ProxyDns")]
pub struct ProxyDnsGql {
    pub who: ProxyDnsWho,
    pub path: ProxyDnsPath,
    pub trust: ProxyDnsTrust,
    pub servers: Vec<ProxyDnsServerGql>,
}
#[derive(Clone, Debug, PartialEq, SimpleObject)]
#[graphql(name = "ProxyDnsServer")]
pub struct ProxyDnsServerGql {
    pub wire: ProxyDnsWire,
    /// The server's IP, with its port when that is not the wire's own.
    pub address: String,
    /// TLS: the name its certificate is checked against.
    pub tls_name: Option<String>,
    /// HTTPS: the URL queries are posted to.
    pub url: Option<String>,
}
impl From<proxies::DnsWho> for ProxyDnsWho {
    fn from(who: proxies::DnsWho) -> Self {
        match who {
            proxies::DnsWho::Host => Self::Host,
            proxies::DnsWho::Proxy => Self::Proxy,
            proxies::DnsWho::Servers => Self::Servers,
        }
    }
}
impl From<ProxyDnsWho> for proxies::DnsWho {
    fn from(who: ProxyDnsWho) -> Self {
        match who {
            ProxyDnsWho::Host => Self::Host,
            ProxyDnsWho::Proxy => Self::Proxy,
            ProxyDnsWho::Servers => Self::Servers,
        }
    }
}
impl From<proxies::DnsTrust> for ProxyDnsTrust {
    fn from(trust: proxies::DnsTrust) -> Self {
        match trust {
            proxies::DnsTrust::AsSent => Self::AsSent,
            proxies::DnsTrust::RequireAd => Self::RequireAd,
            proxies::DnsTrust::Validate => Self::Validate,
        }
    }
}
impl From<ProxyDnsTrust> for proxies::DnsTrust {
    fn from(trust: ProxyDnsTrust) -> Self {
        match trust {
            ProxyDnsTrust::AsSent => Self::AsSent,
            ProxyDnsTrust::RequireAd => Self::RequireAd,
            ProxyDnsTrust::Validate => Self::Validate,
        }
    }
}
impl From<proxies::ProxyDns> for ProxyDnsGql {
    fn from(dns: proxies::ProxyDns) -> Self {
        Self {
            who: dns.who.into(),
            path: match dns.path {
                proxies::DnsPath::Direct => ProxyDnsPath::Direct,
                proxies::DnsPath::ThroughProxy => ProxyDnsPath::ThroughProxy,
            },
            trust: dns.trust.into(),
            servers: dns.servers.iter().map(ProxyDnsServerGql::from).collect(),
        }
    }
}

/// How a name dialled straight out of an egress, with no proxy above it,
/// is resolved: by the host, or by the listed servers asked from the
/// egress. Trust and servers mean something only when the servers answer.
#[derive(Clone, Debug, PartialEq, SimpleObject)]
#[graphql(name = "EgressDns")]
pub struct EgressDnsGql {
    pub who: ProxyDnsWho,
    pub trust: ProxyDnsTrust,
    pub servers: Vec<ProxyDnsServerGql>,
}
impl From<&proxies::EgressDns> for EgressDnsGql {
    fn from(dns: &proxies::EgressDns) -> Self {
        Self {
            who: dns.who.into(),
            trust: dns.trust.into(),
            servers: dns.servers.iter().map(ProxyDnsServerGql::from).collect(),
        }
    }
}
/// An egress's DNS as a save sends it. Who is HOST or SERVERS; trust and
/// servers are kept only when the servers answer.
#[derive(InputObject)]
pub struct EgressDnsInput {
    pub who: ProxyDnsWho,
    pub trust: ProxyDnsTrust,
    pub servers: Vec<ProxyDnsServerInput>,
}
impl EgressDnsInput {
    pub(crate) fn dns(self) -> Result<proxies::EgressDns> {
        let servers = self
            .servers
            .into_iter()
            .map(ProxyDnsServerInput::server)
            .collect::<std::result::Result<_, _>>()
            .map_err(refusal_error)?;
        Ok(proxies::EgressDns {
            who: self.who.into(),
            servers,
            trust: self.trust.into(),
        }
        .normal())
    }
}
impl From<&proxies::ProxyDnsServer> for ProxyDnsServerGql {
    fn from(server: &proxies::ProxyDnsServer) -> Self {
        let engine = server.engine();
        let address = engine.to_string();
        let (wire, tls_name, url) = match &server.wire {
            proxies::ProxyDnsWire::Plain => (ProxyDnsWire::Plain, None, None),
            proxies::ProxyDnsWire::Tls { name } => (ProxyDnsWire::Tls, name.clone(), None),
            proxies::ProxyDnsWire::Https { path } => {
                let host = match server.address.ip() {
                    std::net::IpAddr::V4(ip) => ip.to_string(),
                    std::net::IpAddr::V6(ip) => format!("[{ip}]"),
                };
                let port = server.address.port();
                let port = if port == proxies::WireKind::Https.default_port() {
                    String::new()
                } else {
                    format!(":{port}")
                };
                (
                    ProxyDnsWire::Https,
                    None,
                    Some(format!("https://{host}{port}{path}")),
                )
            }
        };
        Self {
            wire,
            address,
            tls_name,
            url,
        }
    }
}

/// A proxy's DNS as a save sends it. Path, trust and servers are kept only
/// when the servers answer.
#[derive(InputObject)]
pub struct ProxyDnsInput {
    pub who: ProxyDnsWho,
    pub path: ProxyDnsPath,
    pub trust: ProxyDnsTrust,
    pub servers: Vec<ProxyDnsServerInput>,
}
#[derive(InputObject)]
pub struct ProxyDnsServerInput {
    pub wire: ProxyDnsWire,
    /// PLAIN and TLS: the server's IP, with a port when it is not the
    /// wire's own (`192.0.2.53:5353`, `[2001:db8::53]:853`). HTTPS: unused.
    pub address: String,
    /// TLS: the name the certificate is checked against; absent checks it
    /// against the address.
    pub tls_name: Option<String>,
    /// HTTPS: the URL queries are posted to, `https://` and an IP.
    pub url: Option<String>,
}
impl ProxyDnsInput {
    fn dns(self) -> Result<proxies::ProxyDns> {
        let servers = self
            .servers
            .into_iter()
            .map(ProxyDnsServerInput::server)
            .collect::<std::result::Result<_, _>>()
            .map_err(refusal_error)?;
        Ok(proxies::ProxyDns {
            who: self.who.into(),
            path: match self.path {
                ProxyDnsPath::Direct => proxies::DnsPath::Direct,
                ProxyDnsPath::ThroughProxy => proxies::DnsPath::ThroughProxy,
            },
            servers,
            trust: self.trust.into(),
        }
        .normal())
    }
}
impl ProxyDnsServerInput {
    fn server(self) -> std::result::Result<proxies::ProxyDnsServer, NetworkRefusal> {
        let wire = match self.wire {
            ProxyDnsWire::Plain => proxies::ProxyDnsWire::Plain,
            ProxyDnsWire::Tls => proxies::ProxyDnsWire::Tls {
                name: self
                    .tls_name
                    .map(|name| name.trim().to_owned())
                    .filter(|name| !name.is_empty()),
            },
            ProxyDnsWire::Https => {
                let (address, path) = doh_url(self.url.as_deref().unwrap_or("").trim())?;
                return Ok(proxies::ProxyDnsServer {
                    address,
                    wire: proxies::ProxyDnsWire::Https { path },
                });
            }
        };
        let default_port = match self.wire {
            ProxyDnsWire::Tls => proxies::WireKind::Tls,
            _ => proxies::WireKind::Plain,
        }
        .default_port();
        Ok(proxies::ProxyDnsServer {
            address: server_address(self.address.trim(), default_port)
                .ok_or(NetworkRefusal::ProxyDnsServerInvalid)?,
            wire,
        })
    }
}

// An IP with an optional port; `default_port` without one. Brackets hold a
// v6 address only, and no address carries a zone: a server is asked at an
// address that means the same on every interface.
fn server_address(text: &str, default_port: u16) -> Option<std::net::SocketAddr> {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    let (ip, port): (IpAddr, &str) = match text.strip_prefix('[') {
        Some(bracketed) => {
            let (ip, after) = bracketed.split_once(']')?;
            (IpAddr::V6(ip.parse::<Ipv6Addr>().ok()?), after)
        }
        // A bare v6 address has no room for a port.
        None if text.matches(':').count() > 1 => (IpAddr::V6(text.parse::<Ipv6Addr>().ok()?), ""),
        None => match text.split_once(':') {
            Some((ip, _)) => (IpAddr::V4(ip.parse::<Ipv4Addr>().ok()?), &text[ip.len()..]),
            None => (IpAddr::V4(text.parse::<Ipv4Addr>().ok()?), ""),
        },
    };
    let port = match port {
        "" => default_port,
        port => port
            .strip_prefix(':')
            .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))?
            .parse()
            .ok()?,
    };
    Some(SocketAddr::new(ip, port))
}

// A DNS over HTTPS URL as the server's address and the path queries are
// posted to: `https://`, an IP literal host (a v6 one in brackets), an
// optional port, and a path from '/', its query string kept. Nothing else:
// no user, no fragment, and no host name. The host is read as written, not
// as a browser reads it, so `1.2.3` or `0x7f.1` is no address.
fn doh_url(url: &str) -> std::result::Result<(std::net::SocketAddr, String), NetworkRefusal> {
    use NetworkRefusal::{ProxyDnsUrlHostname, ProxyDnsUrlInvalid};
    let scheme = url.get(..8).ok_or(ProxyDnsUrlInvalid)?;
    if !scheme.eq_ignore_ascii_case("https://") || url.contains('#') {
        return Err(ProxyDnsUrlInvalid);
    }
    let rest = &url[8..];
    let slash = rest.find('/').ok_or(ProxyDnsUrlInvalid)?;
    let (authority, path) = rest.split_at(slash);
    if authority.contains(['@', '?']) {
        return Err(ProxyDnsUrlInvalid);
    }
    let (host, port) = match authority.strip_prefix('[') {
        Some(bracketed) => {
            let (host, after) = bracketed.split_once(']').ok_or(ProxyDnsUrlInvalid)?;
            let ip: std::net::Ipv6Addr = host.parse().map_err(|_| ProxyDnsUrlInvalid)?;
            (std::net::IpAddr::V6(ip), after)
        }
        None => {
            let (host, port) = match authority.split_once(':') {
                Some((host, _)) => (host, &authority[host.len()..]),
                None => (authority, ""),
            };
            let ip: std::net::Ipv4Addr = match host.parse() {
                Ok(ip) => ip,
                // A name, as against something in no address form at all.
                Err(_) if host.bytes().any(|b| b.is_ascii_alphabetic()) => {
                    return Err(ProxyDnsUrlHostname);
                }
                Err(_) => return Err(ProxyDnsUrlInvalid),
            };
            (std::net::IpAddr::V4(ip), port)
        }
    };
    let port = match port {
        "" => proxies::WireKind::Https.default_port(),
        port => port
            .strip_prefix(':')
            .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|digits| digits.parse().ok())
            .filter(|port| *port != 0)
            .ok_or(ProxyDnsUrlInvalid)?,
    };
    Ok((std::net::SocketAddr::new(host, port), path.to_owned()))
}

#[derive(Clone, Debug, InputObject)]
pub struct RoutingPolicyInput {
    pub proxy_ids: Vec<u32>,
    pub allow_direct: bool,
}
impl From<RoutingPolicyInput> for RoutingPolicy {
    fn from(v: RoutingPolicyInput) -> Self {
        Self {
            proxy_ids: v.proxy_ids,
            allow_direct: v.allow_direct,
            ..Default::default()
        }
    }
}
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "RoutingPolicy")]
pub struct RoutingPolicyGql {
    pub proxy_ids: Vec<u32>,
    pub allow_direct: bool,
}
impl From<RoutingPolicy> for RoutingPolicyGql {
    fn from(v: RoutingPolicy) -> Self {
        Self {
            proxy_ids: v.proxy_ids,
            allow_direct: v.allow_direct,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum RoutingState {
    Idle,
    Proxy,
    Direct,
    Blocked,
}
#[derive(SimpleObject)]
pub struct RoutingFailure {
    pub proxy_id: u32,
    // The failure in English, for a client with no sentence for its code.
    pub message: String,
    pub code: String,
    // The values the client's sentence for the code may name.
    pub params: async_graphql::Json<serde_json::Map<String, serde_json::Value>>,
}
impl RoutingFailure {
    fn new(proxy_id: u32, fault: &proxies::ProxyFault) -> Self {
        Self {
            proxy_id,
            message: fault.to_string(),
            code: fault.code().into(),
            params: async_graphql::Json(proxies::fault_params(fault)),
        }
    }
}
#[derive(SimpleObject)]
pub struct RoutingStatus {
    pub legs: Vec<crate::networking::NetworkLegFlow>,
    pub state: RoutingState,
    pub selected_proxy_id: Option<u32>,
    pub failures: Vec<RoutingFailure>,
}

#[derive(Clone, PartialEq, SimpleObject)]
#[graphql(name = "ProxyProfile")]
pub struct ProxyProfileGql {
    pub id: u32,
    pub name: String,
    pub kind: ProxyKind,
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub dns: ProxyDnsGql,
    pub tunnel_addresses: Vec<String>,
    pub peer_public_key: Option<String>,
    pub tunnel_public_key: Option<String>,
    pub mtu: u16,
    pub keepalive_seconds: Option<u16>,
    pub timeout_seconds: u32,
    pub host_key_fingerprint: Option<String>,
    /// WireGuard: the gateway forwards a port over NAT-PMP.
    pub natpmp: bool,
    /// WireGuard: the port the provider forwards, always the same one.
    pub forwarded_port: Option<u16>,
    pub has_username: bool,
    pub has_password: bool,
    pub has_private_key: bool,
    pub has_passphrase: bool,
    pub has_preshared_key: bool,
}
impl From<ProxyProfile> for ProxyProfileGql {
    fn from(p: ProxyProfile) -> Self {
        let tunnel_public_key = if p.kind == proxies::ProxyKind::WireGuard {
            p.wireguard_spec().ok().map(|s| s.public_key())
        } else {
            None
        };
        Self {
            id: p.id,
            name: p.name,
            kind: p.kind.into(),
            enabled: p.enabled,
            host: p.host,
            port: p.port,
            dns: p.dns.into(),
            tunnel_addresses: p.tunnel_addresses,
            peer_public_key: p.peer_public_key,
            tunnel_public_key,
            mtu: p.mtu,
            keepalive_seconds: p.keepalive_seconds,
            timeout_seconds: p.timeout_seconds,
            host_key_fingerprint: p.host_key_fingerprint,
            natpmp: p.natpmp,
            forwarded_port: p.forwarded_port,
            has_username: p.secrets.username.is_some(),
            has_password: p.secrets.password.is_some(),
            has_private_key: p.secrets.private_key.is_some(),
            has_passphrase: p.secrets.passphrase.is_some(),
            has_preshared_key: p.secrets.preshared_key.is_some(),
        }
    }
}

#[derive(InputObject)]
pub struct ProxyProfileInput {
    pub name: String,
    pub kind: ProxyKind,
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    // Omitted: a saved proxy keeps its own, and a new one resolves as a
    // proxy saved without one always has.
    pub dns: Option<ProxyDnsInput>,
    #[graphql(default)]
    pub tunnel_addresses: Vec<String>,
    pub peer_public_key: Option<String>,
    #[graphql(default = 1280)]
    pub mtu: u16,
    pub keepalive_seconds: Option<u16>,
    #[graphql(default = 30)]
    pub timeout_seconds: u32,
    #[graphql(default)]
    pub natpmp: bool,
    pub forwarded_port: Option<u16>,
    pub username: MaybeUndefined<String>,
    pub password: MaybeUndefined<String>,
    pub private_key: MaybeUndefined<String>,
    pub passphrase: MaybeUndefined<String>,
    pub preshared_key: MaybeUndefined<String>,
}
fn secret_update(target: &mut Option<String>, value: MaybeUndefined<String>) {
    match value {
        MaybeUndefined::Undefined => {}
        MaybeUndefined::Null => *target = None,
        MaybeUndefined::Value(value) => *target = (!value.is_empty()).then_some(value),
    }
}
impl ProxyProfileInput {
    fn profile(self, id: u32, existing: Option<&ProxyProfile>) -> Result<ProxyProfile> {
        if existing.is_some_and(|p| p.kind != self.kind.into()) {
            return Err(refusal_error(NetworkRefusal::ProxyKindImmutable));
        }
        if matches!(self.kind, ProxyKind::Ssh)
            && matches!(&self.password, MaybeUndefined::Value(password) if !password.is_empty())
        {
            return Err(refusal_error(NetworkRefusal::ProxySshPasswordUnsupported));
        }
        let mut secrets = existing.map(|p| p.secrets.clone()).unwrap_or_default();
        secret_update(&mut secrets.username, self.username);
        secret_update(&mut secrets.password, self.password);
        secret_update(&mut secrets.private_key, self.private_key);
        secret_update(&mut secrets.passphrase, self.passphrase);
        secret_update(&mut secrets.preshared_key, self.preshared_key);
        let dns = match (self.dns, existing) {
            (Some(dns), _) => dns.dns()?,
            (None, Some(existing)) => existing.dns.clone(),
            // A WireGuard tunnel cannot take a name, so the host resolves
            // for it; every other proxy resolves for itself.
            (None, None) if self.kind == ProxyKind::WireGuard => proxies::ProxyDns::host(),
            (None, None) => proxies::ProxyDns::proxy(),
        };
        let mut profile = ProxyProfile {
            id,
            name: self.name.trim().into(),
            kind: self.kind.into(),
            enabled: self.enabled,
            host: self.host.trim().trim_matches(['[', ']']).into(),
            port: self.port,
            dns,
            tunnel_addresses: self.tunnel_addresses,
            peer_public_key: self.peer_public_key,
            mtu: self.mtu,
            keepalive_seconds: self.keepalive_seconds,
            timeout_seconds: self.timeout_seconds,
            host_key_fingerprint: existing.and_then(|p| p.host_key_fingerprint.clone()),
            // Only a WireGuard tunnel takes a forwarded port; with NAT-PMP the
            // gateway picks it.
            natpmp: self.kind == ProxyKind::WireGuard && self.natpmp,
            forwarded_port: self
                .forwarded_port
                .filter(|port| *port != 0 && self.kind == ProxyKind::WireGuard && !self.natpmp),
            revision: 0,
            secrets,
        };
        profile.revision = profile
            .saved_revision(existing)
            .ok_or("proxy revision exhausted")?;
        profile.validate().map_err(refusal_error)?;
        Ok(profile)
    }
}

pub(crate) fn runtime(ctx: &Context<'_>) -> Result<Arc<ProxyRuntime>> {
    ctx.data::<SchedulerHandle>()?
        .proxy_runtime()
        .ok_or_else(|| "proxy runtime unavailable".into())
}
pub(crate) fn draft_route(
    ctx: &Context<'_>,
    consumer: Consumer,
    input: Option<RoutingPolicy>,
) -> Result<Arc<proxies::DraftNetworkRoute>> {
    let runtime = runtime(ctx)?;
    let policy = input.unwrap_or_else(|| runtime.policy(consumer));
    // A route the operator may not save is refused by its code; a draft that
    // cannot be built for any other reason is reported as it stands.
    runtime
        .network
        .validate_policy(&policy)
        .map_err(refusal_error)?;
    runtime
        .network
        .draft_route(consumer, policy, Duration::from_secs(30))
        .map_err(async_graphql::Error::new)
}
pub(crate) fn refresh<'a>(
    ctx: &'a Context<'_>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.data::<SchedulerHandle>()?;
        if let Some(runtime) = handle.proxy_runtime() {
            runtime.reload().await.map_err(async_graphql::Error::new)?;
        }
        Ok(())
    })
}
// Take a saved change to proxy `profile`: the routes reload, and the usenet
// servers' connections are rebuilt only when a server's route goes through
// the proxy. A proxy no server route names feeds no usenet connection.
fn refresh_profile<'a>(
    ctx: &'a Context<'_>,
    profile: u32,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        refresh(ctx).await?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let feeds_servers = handle
            .proxy_runtime()
            .is_some_and(|runtime| runtime.network.feeds_servers(profile));
        if feeds_servers && handle.server_transfer_policy().is_some() {
            weaver_server_core::runtime::rebuild_nntp_from_config(ctx.data()?, handle)
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        }
        Ok(())
    })
}

#[derive(Default)]
pub(crate) struct ProxiesQuery;
#[Object]
impl ProxiesQuery {
    #[graphql(guard = "AdminGuard")]
    async fn proxy_profiles(&self, ctx: &Context<'_>) -> Result<Vec<ProxyProfileGql>> {
        let db = ctx.data::<Database>()?.clone();
        Ok(
            spawn_blocking_db("proxies.list", move || db.list_proxy_profiles())
                .await?
                .into_iter()
                .map(Into::into)
                .collect(),
        )
    }
}

#[derive(SimpleObject)]
pub struct ProxyTestResult {
    pub success: bool,
    pub code: String,
    // The outcome in English, for a client with no sentence for its code.
    pub message: String,
    // The values the client's sentence for the code may name.
    pub params: async_graphql::Json<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Default)]
pub(crate) struct ProxiesMutation;
#[Object]
impl ProxiesMutation {
    #[graphql(guard = "AdminGuard")]
    async fn save_proxy_profile(
        &self,
        ctx: &Context<'_>,
        id: Option<u32>,
        input: ProxyProfileInput,
    ) -> Result<ProxyProfileGql> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let read_db = db.clone();
        let profiles =
            spawn_blocking_db("proxies.load", move || read_db.list_proxy_profiles()).await?;
        let existing = id
            .map(|id| {
                profiles
                    .iter()
                    .find(|p| p.id == id)
                    .ok_or_else(|| refusal_error(NetworkRefusal::ProxyNotFound))
            })
            .transpose()?;
        if id.is_none() && profiles.len() >= 128 {
            return Err(refusal_error(NetworkRefusal::ProxyLimitReached));
        }
        let id = id.unwrap_or(
            profiles
                .iter()
                .map(|p| p.id)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or("proxy IDs exhausted")?,
        );
        let profile = input.profile(id, existing)?;
        // A save that changes nothing keeps the revision, and everything
        // built from the profile is keyed by it: nothing is rebuilt.
        let unchanged = existing.is_some_and(|existing| existing.revision == profile.revision);
        let saved = profile.clone();
        let profile =
            spawn_blocking_db("proxies.save", move || db.save_proxy_profile(&saved)).await?;
        if !unchanged {
            refresh_profile(ctx, id).await?;
        }
        Ok(profile.into())
    }
    #[graphql(guard = "AdminGuard")]
    async fn delete_proxy_profile(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_db("proxies.delete", move || db.delete_proxy_profile(id)).await?;
        refresh_profile(ctx, id).await?;
        Ok(true)
    }
    #[graphql(guard = "AdminGuard")]
    async fn reset_proxy_host_key(&self, ctx: &Context<'_>, id: u32) -> Result<ProxyProfileGql> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let read_db = db.clone();
        let mut profile = spawn_blocking_db("proxies.load", move || read_db.list_proxy_profiles())
            .await?
            .into_iter()
            .find(|p| p.id == id)
            .ok_or_else(|| refusal_error(NetworkRefusal::ProxyNotFound))?;
        if profile.kind != proxies::ProxyKind::Ssh {
            return Err(refusal_error(NetworkRefusal::ProxyHostKeyNotSsh));
        }
        profile.host_key_fingerprint = None;
        profile.revision = profile
            .revision
            .checked_add(1)
            .ok_or("proxy revision exhausted")?;
        let saved = profile.clone();
        let profile = spawn_blocking_db("proxies.reset_trust", move || {
            db.reset_proxy_host_key(&saved)
        })
        .await?;
        refresh_profile(ctx, id).await?;
        Ok(profile.into())
    }
    // A test names the route it goes through; this one names none, so it is
    // refused.
    #[graphql(
        guard = "AdminGuard",
        deprecation = "Use testVia with the route the proxy is tested through."
    )]
    async fn test_proxy_profile(&self, id: u32) -> Result<ProxyTestResult> {
        let _ = id;
        Err(refusal_error(NetworkRefusal::TestNeedsRoute))
    }
}

async fn policy_for(ctx: &Context<'_>, consumer: Consumer) -> Result<RoutingPolicyGql> {
    let db = ctx.data::<Database>()?.clone();
    Ok(
        spawn_blocking_db("proxies.routing", move || db.proxy_routing_policy(consumer))
            .await?
            .into(),
    )
}
fn status_for(ctx: &Context<'_>, consumer: Consumer) -> Result<RoutingStatus> {
    let legs = ctx
        .data::<SchedulerHandle>()?
        .proxy_runtime()
        .map(|runtime| {
            crate::networking::flow(&runtime)
                .legs
                .into_iter()
                .filter(|leg| leg.consumer == consumer.key())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let status = match ctx.data::<SchedulerHandle>()?.proxy_runtime() {
        Some(runtime) => runtime
            .route(consumer, Duration::from_secs(30))
            .map_err(async_graphql::Error::new)?
            .status(),
        None => proxies::RoutingStatus::default(),
    };
    let active = legs.iter().find(|leg| leg.state == "UP");
    let selected_proxy_id = active.map_or(status.selected_proxy_id, |leg| leg.selected_proxy_id);
    let state = if let Some(leg) = active {
        if leg.selected_proxy_id.is_some() {
            RoutingState::Proxy
        } else {
            RoutingState::Direct
        }
    } else if !legs.is_empty()
        && legs
            .iter()
            .all(|leg| matches!(leg.state.as_str(), "DOWN" | "BLOCKED"))
    {
        RoutingState::Blocked
    } else {
        match status.state {
            proxies::RouteState::Idle => RoutingState::Idle,
            proxies::RouteState::Proxy => RoutingState::Proxy,
            proxies::RouteState::Direct => RoutingState::Direct,
            proxies::RouteState::Blocked => RoutingState::Blocked,
        }
    };
    Ok(RoutingStatus {
        legs,
        state,
        selected_proxy_id,
        failures: status
            .failures
            .into_iter()
            .map(|(proxy_id, fault)| RoutingFailure::new(proxy_id, &fault))
            .collect(),
    })
}
#[ComplexObject]
impl crate::servers::types::Server {
    async fn route(&self, ctx: &Context<'_>) -> Result<crate::networking::RouteGql> {
        let db = ctx.data::<Database>()?.clone();
        let id = self.id;
        Ok(spawn_blocking_db("network.consumer_route", move || {
            db.proxy_routing_policy(Consumer::Server(id))
        })
        .await?
        .into())
    }
    #[graphql(deprecation = "Use route for the full route; routing is the first-leg projection.")]
    async fn routing(&self, ctx: &Context<'_>) -> Result<RoutingPolicyGql> {
        policy_for(ctx, Consumer::Server(self.id)).await
    }
    async fn routing_status(&self, ctx: &Context<'_>) -> Result<RoutingStatus> {
        status_for(ctx, Consumer::Server(self.id))
    }
}
#[ComplexObject]
impl crate::rss::types::RssFeed {
    async fn route(&self, ctx: &Context<'_>) -> Result<crate::networking::RouteGql> {
        let db = ctx.data::<Database>()?.clone();
        let id = self.id;
        Ok(spawn_blocking_db("network.consumer_route", move || {
            db.proxy_routing_policy(Consumer::Rss(id))
        })
        .await?
        .into())
    }
    #[graphql(deprecation = "Use route for the full route; routing is the first-leg projection.")]
    async fn routing(&self, ctx: &Context<'_>) -> Result<RoutingPolicyGql> {
        policy_for(ctx, Consumer::Rss(self.id)).await
    }
    async fn routing_status(&self, ctx: &Context<'_>) -> Result<RoutingStatus> {
        status_for(ctx, Consumer::Rss(self.id))
    }
}

#[ComplexObject]
impl crate::servers::types::ServerDetails {
    async fn route(&self, ctx: &Context<'_>) -> Result<crate::networking::RouteGql> {
        let db = ctx.data::<Database>()?.clone();
        let id = self.id;
        Ok(spawn_blocking_db("network.consumer_route", move || {
            db.proxy_routing_policy(Consumer::Server(id))
        })
        .await?
        .into())
    }
    #[graphql(deprecation = "Use route for the full route; routing is the first-leg projection.")]
    async fn routing(&self, ctx: &Context<'_>) -> Result<RoutingPolicyGql> {
        policy_for(ctx, Consumer::Server(self.id)).await
    }
    async fn routing_status(&self, ctx: &Context<'_>) -> Result<RoutingStatus> {
        status_for(ctx, Consumer::Server(self.id))
    }
}
