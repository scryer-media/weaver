// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The saved DNS presets: server lists, which name public servers once, and
// profiles, which say who answers a name. A route profile answers the names
// a route dials; a proxy name profile answers a proxy's own address. A route
// selects one route profile and a proxy one proxy name profile, the built-in
// System profile being either; editing a profile changes everything that
// selects it.
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr};
use weaver_tunnel::{
    dns::{DnsServer, Trust, Wire},
    pipe::{DnsChoice, DnsTransit as Transit},
};

use super::NetworkRefusal as Refusal;

// The built-in profile every route without a choice uses: the host's own
// resolver, as an untouched install always had.
pub const SYSTEM_DNS_PROFILE_ID: u32 = 0;
// The most servers one list holds.
pub const MAX_DNS_SERVERS: usize = 10;
// The most lists and profiles a Weaver keeps.
pub const MAX_DNS_LISTS: usize = 128;
pub const MAX_DNS_PROFILES: usize = 128;
const MAX_DNS_NAME_BYTES: usize = 128;
const MAX_TLS_NAME_BYTES: usize = 253;
const MAX_HTTPS_PATH_BYTES: usize = 2048;

// One server of a list, at `address`, asked over `wire`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedServer {
    pub address: SocketAddr,
    pub wire: ListedWire,
}

// How a server is asked: in the clear, over TLS (its certificate checked
// against `name`, or its address without one), or over HTTPS at `path`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ListedWire {
    Plain,
    Tls { name: Option<String> },
    Https { path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsServerList {
    pub id: u32,
    pub name: String,
    // Asked in order.
    pub servers: Vec<ListedServer>,
}

// Who answers the names a route dials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DnsWho {
    // The host's own resolver.
    System,
    // The leg's active proxy: handed the name, or for WireGuard the
    // tunnel's own DNS servers asked through it.
    Proxy,
    // The profile's listed servers, asked by Weaver.
    Weaver,
}

// Where Weaver asks its listed servers from: the leg's egress, or the last
// hop of the leg's active proxy. A leg or rung with no proxy asks from its
// egress either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DnsTransit {
    Egress,
    Proxy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DnsTrust {
    AsSent,
    // Weaver checks the DNSSEC signatures itself: an unsigned name resolves,
    // one that fails the check does not.
    Validate,
}

// What a profile answers for, set when it is created and never changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DnsProfileKind {
    // The names a route dials.
    Route,
    // A proxy's own address, when it is a name: the host, or listed servers
    // asked from the egress, never a proxy.
    ProxyName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsProfile {
    pub id: u32,
    pub name: String,
    pub kind: DnsProfileKind,
    pub who: DnsWho,
    // Weaver only.
    pub transit: Option<DnsTransit>,
    pub trust: DnsTrust,
    // Weaver only.
    pub server_list_id: Option<u32>,
    // The proxy only: a leg or rung with no proxy is answered by the host
    // when set, and fails its lookups when not.
    pub system_fallback: bool,
}

impl DnsProfile {
    // The built-in profile: the host answers.
    pub fn system() -> Self {
        Self {
            id: SYSTEM_DNS_PROFILE_ID,
            name: "System".into(),
            kind: DnsProfileKind::Route,
            who: DnsWho::System,
            transit: None,
            trust: DnsTrust::AsSent,
            server_list_id: None,
            system_fallback: false,
        }
    }
    pub fn is_built_in(&self) -> bool {
        self.id == SYSTEM_DNS_PROFILE_ID
    }
    // Whether something that takes profiles of `kind` may select this one:
    // a profile of that kind, or the built-in System profile.
    pub fn selectable_as(&self, kind: DnsProfileKind) -> bool {
        self.is_built_in() || self.kind == kind
    }
    // Refused, naming `kind`, unless it is selectable as `kind`.
    pub fn select_as(&self, kind: DnsProfileKind) -> Result<(), Refusal> {
        if self.selectable_as(kind) {
            Ok(())
        } else {
            Err(Refusal::DnsProfileKind { expected: kind })
        }
    }
    // The profile as it is saved: what only Weaver's servers use is dropped
    // for any other answerer, and Weaver asks from the egress unless told
    // otherwise, so two profiles that behave the same are stored the same.
    pub fn normal(mut self) -> Self {
        self.name = self.name.trim().into();
        if self.who != DnsWho::Proxy {
            self.system_fallback = false;
        }
        if self.who == DnsWho::Weaver {
            self.transit.get_or_insert(DnsTransit::Egress);
        }
        if self.who != DnsWho::Weaver {
            self.transit = None;
            self.server_list_id = None;
        }
        self
    }
    pub fn validate(&self) -> Result<(), Refusal> {
        if self.name.trim().is_empty() || self.name.len() > MAX_DNS_NAME_BYTES {
            return Err(Refusal::DnsProfileNameLength);
        }
        match (self.who, self.trust) {
            (DnsWho::System, DnsTrust::AsSent)
            | (DnsWho::Proxy, DnsTrust::AsSent | DnsTrust::Validate)
            | (DnsWho::Weaver, _) => {}
            _ => return Err(Refusal::DnsProfileTrustInvalid),
        }
        if self.who == DnsWho::Weaver && self.server_list_id.is_none() {
            return Err(Refusal::DnsProfileListRequired);
        }
        // A proxy's address is found by the host or by listed servers asked
        // from the egress: no proxy is there yet to answer or carry it.
        if self.kind == DnsProfileKind::ProxyName
            && (self.who == DnsWho::Proxy || self.transit == Some(DnsTransit::Proxy))
        {
            return Err(Refusal::DnsProfileProxyNameInvalid);
        }
        Ok(())
    }
    // What the engine is told to do with every name a route on this profile
    // dials, its servers taken from `lists`. A profile whose list is gone
    // resolves nothing.
    pub fn choice(&self, lists: &HashMap<u32, DnsServerList>) -> DnsChoice {
        match self.who {
            DnsWho::System => DnsChoice::System,
            DnsWho::Proxy => DnsChoice::Proxy {
                trust: self.trust.engine(),
                system_fallback: self.system_fallback,
            },
            DnsWho::Weaver => {
                let Some(list) = self.server_list_id.and_then(|id| lists.get(&id)) else {
                    return DnsChoice::Refused(format!(
                        "DNS profile {} names a server list that does not exist",
                        self.name
                    ));
                };
                DnsChoice::Weaver {
                    servers: list.servers.iter().map(ListedServer::engine).collect(),
                    trust: self.trust.engine(),
                    transit: match self.transit.unwrap_or(DnsTransit::Egress) {
                        DnsTransit::Egress => Transit::Egress,
                        DnsTransit::Proxy => Transit::Proxy,
                    },
                }
            }
        }
    }
    // Who resolves a destination's name dialled through a proxy of kind
    // `proxy` as the last hop, or straight out of the egress when there is
    // none; `tunnel_dns` says whether a WireGuard proxy lists DNS servers of
    // its tunnel. None when nothing would: the proxy answering with no proxy
    // and no fallback to the host, or at a WireGuard proxy that lists none.
    pub fn resolved_by(
        &self,
        proxy: Option<super::ProxyKind>,
        tunnel_dns: bool,
    ) -> Option<super::ResolvedBy> {
        use super::{ProxyKind, ResolvedBy};
        Some(match (self.who, self.transit, proxy) {
            (DnsWho::System, ..) => ResolvedBy::Host,
            (DnsWho::Proxy, _, None) if self.system_fallback => ResolvedBy::Host,
            (DnsWho::Proxy, _, None) => return None,
            (DnsWho::Proxy, _, Some(ProxyKind::WireGuard)) if tunnel_dns => {
                ResolvedBy::ProxyServers
            }
            (DnsWho::Proxy, _, Some(ProxyKind::WireGuard)) => return None,
            (DnsWho::Proxy, ..) => ResolvedBy::Proxy,
            (DnsWho::Weaver, Some(DnsTransit::Proxy), Some(_)) => ResolvedBy::ProxyServers,
            (DnsWho::Weaver, ..) => ResolvedBy::Servers,
        })
    }
}

impl DnsServerList {
    pub fn normal(mut self) -> Self {
        self.name = self.name.trim().into();
        self
    }
    pub fn validate(&self) -> Result<(), Refusal> {
        if self.name.trim().is_empty() || self.name.len() > MAX_DNS_NAME_BYTES {
            return Err(Refusal::DnsListNameLength);
        }
        validate_servers(&self.servers)
    }
}

impl DnsTrust {
    pub fn engine(self) -> Trust {
        match self {
            Self::AsSent => Trust::AsSent,
            Self::Validate => Trust::Validate,
        }
    }
}

// What a list checks of its servers: at least one, at most
// `MAX_DNS_SERVERS`, no plain server after an encrypted one, each server
// well formed, and none twice.
pub fn validate_servers(servers: &[ListedServer]) -> Result<(), Refusal> {
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
        .position(|server| server.wire != ListedWire::Plain);
    if let Some(first) = first_encrypted
        && servers[first..]
            .iter()
            .any(|server| server.wire == ListedWire::Plain)
    {
        return Err(Refusal::ProxyDnsPlainAfterEncrypted);
    }
    servers.iter().try_for_each(ListedServer::validate)?;
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

impl ListedServer {
    pub fn engine(&self) -> DnsServer {
        DnsServer {
            address: self.address,
            wire: match &self.wire {
                ListedWire::Plain => Wire::Plain,
                ListedWire::Tls { name } => Wire::Tls { name: name.clone() },
                ListedWire::Https { path } => Wire::Https { path: path.clone() },
            },
        }
    }
    // What one server checks of itself: a port, and a TLS name or HTTPS
    // path that is well formed.
    pub fn validate(&self) -> Result<(), Refusal> {
        if self.address.port() == 0 {
            return Err(Refusal::ProxyDnsServerInvalid);
        }
        match &self.wire {
            ListedWire::Plain | ListedWire::Tls { name: None } => Ok(()),
            ListedWire::Tls { name: Some(name) } if is_tls_name(name) => Ok(()),
            ListedWire::Tls { .. } => Err(Refusal::ProxyDnsTlsNameInvalid),
            ListedWire::Https { path } if is_https_path(path) => Ok(()),
            ListedWire::Https { .. } => Err(Refusal::ProxyDnsUrlInvalid),
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

// The stored form of a list's servers: a version tag, so a later form can
// be told from this one, and the servers in order.
#[derive(Serialize, Deserialize)]
struct StoredServers {
    v: u32,
    servers: Vec<ListedServer>,
}
const STORED_SERVERS_VERSION: u32 = 1;
// The most a stored server list may hold: ten servers with the longest
// HTTPS path fit in a little over 20 KiB.
pub const MAX_STORED_SERVERS_BYTES: usize = 64 * 1024;

pub fn stored_servers(servers: &[ListedServer]) -> Result<String, String> {
    serde_json::to_string(&StoredServers {
        v: STORED_SERVERS_VERSION,
        servers: servers.to_vec(),
    })
    .map_err(|error| error.to_string())
}

// The servers a stored list holds, or why they cannot be read.
pub fn read_servers(stored: &str) -> Result<Vec<ListedServer>, String> {
    if stored.len() > MAX_STORED_SERVERS_BYTES {
        return Err(format!("{} bytes, over the limit", stored.len()));
    }
    let read: StoredServers = serde_json::from_str(stored).map_err(|error| error.to_string())?;
    if read.v != STORED_SERVERS_VERSION {
        return Err(format!("unknown version {}", read.v));
    }
    validate_servers(&read.servers).map_err(|refusal| refusal.to_string())?;
    Ok(read.servers)
}

pub fn kind_text(kind: DnsProfileKind) -> &'static str {
    match kind {
        DnsProfileKind::Route => "route",
        DnsProfileKind::ProxyName => "proxy_name",
    }
}
pub fn kind_of(text: &str) -> Option<DnsProfileKind> {
    Some(match text {
        "route" => DnsProfileKind::Route,
        "proxy_name" => DnsProfileKind::ProxyName,
        _ => return None,
    })
}
pub fn who_text(who: DnsWho) -> &'static str {
    match who {
        DnsWho::System => "system",
        DnsWho::Proxy => "proxy",
        DnsWho::Weaver => "weaver",
    }
}
pub fn who_of(text: &str) -> Option<DnsWho> {
    Some(match text {
        "system" => DnsWho::System,
        "proxy" => DnsWho::Proxy,
        "weaver" => DnsWho::Weaver,
        _ => return None,
    })
}
pub fn transit_text(transit: DnsTransit) -> &'static str {
    match transit {
        DnsTransit::Egress => "egress",
        DnsTransit::Proxy => "proxy",
    }
}
pub fn transit_of(text: &str) -> Option<DnsTransit> {
    Some(match text {
        "egress" => DnsTransit::Egress,
        "proxy" => DnsTransit::Proxy,
        _ => return None,
    })
}
pub fn trust_text(trust: DnsTrust) -> &'static str {
    match trust {
        DnsTrust::AsSent => "as_sent",
        DnsTrust::Validate => "validate",
    }
}
pub fn trust_of(text: &str) -> Option<DnsTrust> {
    Some(match text {
        "as_sent" => DnsTrust::AsSent,
        "validate" => DnsTrust::Validate,
        _ => return None,
    })
}

#[cfg(test)]
#[path = "dns_tests.rs"]
mod tests;
