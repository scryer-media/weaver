// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use serde::{Deserialize, Serialize};
use std::{collections::HashSet, net::IpAddr, time::Duration};
use weaver_tunnel::{
    Http3ProxyCredentials, Http3TunnelProvider, Http3TunnelSpec, TunnelError, TunnelSpec,
    WireGuardSpec,
    dns::DnsServer,
    parse_key,
    pipe::{PortForwarding, gateway_of},
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

// The most tunnel addresses a WireGuard proxy takes.
pub const MAX_TUNNEL_ADDRESSES: usize = 8;

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
    // WireGuard: the DNS servers the tunnel's configuration lists. A route
    // whose DNS profile has the proxy answer asks them through the tunnel.
    // Other kinds keep none.
    #[serde(default)]
    pub dns_servers: Vec<IpAddr>,
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
    // The DNS profile that finds this proxy's own address when it is a
    // name: System, or a proxy name profile. Stored in its own column, so it
    // is no part of the settings a revision covers: pointing a proxy at
    // another profile rebuilds nothing, and the next lookup of its address
    // uses the new profile.
    #[serde(skip)]
    pub dns_profile_id: u32,
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
    // The tunnel's DNS servers in the engine's terms: each asked in plain
    // on port 53 from inside the tunnel.
    pub fn tunnel_dns(&self) -> Vec<DnsServer> {
        if self.kind != ProxyKind::WireGuard {
            return Vec::new();
        }
        self.dns_servers
            .iter()
            .copied()
            .map(DnsServer::plain)
            .collect()
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
        if self.dns_servers.len() > super::MAX_DNS_SERVERS {
            return Err(Refusal::ProxyDnsServerCount);
        }
        if self
            .dns_servers
            .iter()
            .enumerate()
            .any(|(at, ip)| self.dns_servers[..at].contains(ip))
        {
            return Err(Refusal::ProxyDnsServerDuplicate);
        }
        if self.kind != ProxyKind::WireGuard && !self.dns_servers.is_empty() {
            return Err(Refusal::ProxyDnsServerInvalid);
        }
        Ok(())
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
    // The tunnel's persistent keepalive, in seconds: the one saved, the
    // tunnel's default when none is, and none when it is saved as 0.
    pub fn keepalive(&self) -> Option<u16> {
        Some(
            self.keepalive_seconds
                .unwrap_or(weaver_tunnel::DEFAULT_WIREGUARD_KEEPALIVE),
        )
        .filter(|seconds| *seconds != 0)
    }
    // The tunnel this profile describes. Its sockets are held to its bytes
    // (`TunnelSockets::budget`), not to a count, so the library's own count
    // is set as high as it goes.
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
            // The tunnel's own resolver asks only the first server it is
            // given, so it is given one only when the tunnel lists exactly
            // one; a longer list is asked by Weaver's own client in turn.
            dns_servers: match self.dns_servers.as_slice() {
                [only] => vec![*only],
                _ => Vec::new(),
            },
            mtu: self.mtu,
            persistent_keepalive: self.keepalive(),
            request_timeout: self.timeout(),
            max_sockets: weaver_tunnel::wireguard::MAX_MAX_SOCKETS,
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
    // The DNS profile the route's names follow, stored in its own column
    // beside the policy. None on a save keeps the stored profile, or takes
    // the System profile for a new route; a loaded route always has one.
    #[serde(skip)]
    pub dns_profile_id: Option<u32>,
}
impl Default for RoutingPolicy {
    fn default() -> Self {
        Self {
            proxy_ids: Vec::new(),
            allow_direct: true,
            legs: Vec::new(),
            failover: super::Failover::Redistribute,
            dns_profile_id: None,
        }
    }
}
impl RoutingPolicy {
    // The DNS profile the route's names follow.
    pub fn dns_profile(&self) -> u32 {
        self.dns_profile_id.unwrap_or(super::SYSTEM_DNS_PROFILE_ID)
    }
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
            dns_profile_id: None,
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

impl Consumer {
    // The consumer a stored route key names.
    pub fn from_key(key: &str) -> Option<Self> {
        use super::TorrentScope;
        let id = |rest: &str| rest.parse().ok();
        if key == "outbound" {
            return Some(Self::Outbound);
        }
        if key == "torrent:default" {
            return Some(Self::Torrent(TorrentScope::Default));
        }
        if let Some(rest) = key.strip_prefix("torrent:category:") {
            return id(rest).map(|id| Self::Torrent(TorrentScope::Category(id)));
        }
        if let Some(rest) = key.strip_prefix("torrent:tracker:") {
            return id(rest).map(|id| Self::Torrent(TorrentScope::TrackerRule(id)));
        }
        if let Some(rest) = key.strip_prefix("server:") {
            return id(rest).map(Self::Server);
        }
        key.strip_prefix("rss:").and_then(id).map(Self::Rss)
    }
}
