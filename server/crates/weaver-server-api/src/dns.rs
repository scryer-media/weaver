// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The DNS section of networking: server lists, which name public servers
// once, and profiles, which say who answers the names a route dials, or a
// proxy's own address. A route selects one route profile by id, a proxy one
// proxy name profile; the built-in System profile, id 0, is either.
use crate::{
    auth::AdminGuard,
    networking::{LookupError, RouteInput},
    observability::{refusal_error, spawn_blocking_db},
    proxies::{refresh, runtime},
};
use async_graphql::{ComplexObject, Context, Enum, InputObject, Object, Result, SimpleObject};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::OnceCell;
use weaver_server_core::proxies::ProxiesDnsPersistenceQueries;
use weaver_server_core::{Database, proxies as core, proxies::NetworkRefusal};

/// What a profile answers for, set when it is created.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum DnsProfileKind {
    /// The names a route dials. Routes select it.
    Route,
    /// A proxy's own address. Proxies select it; it is answered by the
    /// system or by listed servers asked from the egress.
    ProxyName,
}
impl From<core::DnsProfileKind> for DnsProfileKind {
    fn from(v: core::DnsProfileKind) -> Self {
        match v {
            core::DnsProfileKind::Route => Self::Route,
            core::DnsProfileKind::ProxyName => Self::ProxyName,
        }
    }
}
impl From<DnsProfileKind> for core::DnsProfileKind {
    fn from(v: DnsProfileKind) -> Self {
        match v {
            DnsProfileKind::Route => Self::Route,
            DnsProfileKind::ProxyName => Self::ProxyName,
        }
    }
}

/// Who answers the names a route dials.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum DnsWho {
    /// The host's own resolver.
    System,
    /// The leg's active proxy: handed the name, or for WireGuard the
    /// tunnel's own DNS servers asked through it.
    Proxy,
    /// The profile's listed servers, asked by Weaver.
    Weaver,
}
/// Where Weaver asks its listed servers from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum DnsTransit {
    /// The leg's egress.
    Egress,
    /// The last hop of the leg's active proxy. A leg with no proxy asks from
    /// its egress.
    Proxy,
}
/// How answers are judged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum DnsTrust {
    AsSent,
    /// Weaver checks the DNSSEC signatures itself: an unsigned name
    /// resolves, one that fails the check does not.
    Validate,
}
/// How a query travels to one server.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum DnsWire {
    Plain,
    Tls,
    Https,
}

impl From<core::WireKind> for DnsWire {
    fn from(v: core::WireKind) -> Self {
        match v {
            core::WireKind::Plain => Self::Plain,
            core::WireKind::Tls => Self::Tls,
            core::WireKind::Https => Self::Https,
        }
    }
}
impl From<core::DnsWho> for DnsWho {
    fn from(v: core::DnsWho) -> Self {
        match v {
            core::DnsWho::System => Self::System,
            core::DnsWho::Proxy => Self::Proxy,
            core::DnsWho::Weaver => Self::Weaver,
        }
    }
}
impl From<DnsWho> for core::DnsWho {
    fn from(v: DnsWho) -> Self {
        match v {
            DnsWho::System => Self::System,
            DnsWho::Proxy => Self::Proxy,
            DnsWho::Weaver => Self::Weaver,
        }
    }
}
impl From<core::DnsTransit> for DnsTransit {
    fn from(v: core::DnsTransit) -> Self {
        match v {
            core::DnsTransit::Egress => Self::Egress,
            core::DnsTransit::Proxy => Self::Proxy,
        }
    }
}
impl From<DnsTransit> for core::DnsTransit {
    fn from(v: DnsTransit) -> Self {
        match v {
            DnsTransit::Egress => Self::Egress,
            DnsTransit::Proxy => Self::Proxy,
        }
    }
}
impl From<core::DnsTrust> for DnsTrust {
    fn from(v: core::DnsTrust) -> Self {
        match v {
            core::DnsTrust::AsSent => Self::AsSent,
            core::DnsTrust::Validate => Self::Validate,
        }
    }
}
impl From<DnsTrust> for core::DnsTrust {
    fn from(v: DnsTrust) -> Self {
        match v {
            DnsTrust::AsSent => Self::AsSent,
            DnsTrust::Validate => Self::Validate,
        }
    }
}

/// One server of a list.
#[derive(Clone, Debug, PartialEq, SimpleObject)]
pub struct DnsServer {
    pub wire: DnsWire,
    /// The server's IP, with its port when that is not the wire's own. For
    /// HTTPS whose URL names a host, the IP dialled, with no port.
    pub address: String,
    /// TLS: the name its certificate is checked against.
    pub tls_name: Option<String>,
    /// HTTPS: the URL queries are posted to.
    pub url: Option<String>,
}
impl From<&core::ListedServer> for DnsServer {
    fn from(server: &core::ListedServer) -> Self {
        let mut address = server.engine().to_string();
        let (wire, tls_name, url) = match &server.wire {
            core::ListedWire::Plain => (DnsWire::Plain, None, None),
            core::ListedWire::Tls { name } => (DnsWire::Tls, name.clone(), None),
            core::ListedWire::Https { path, name } => {
                let ip = match server.address.ip() {
                    std::net::IpAddr::V4(ip) => ip.to_string(),
                    std::net::IpAddr::V6(ip) => format!("[{ip}]"),
                };
                let host = name.clone().unwrap_or_else(|| ip.clone());
                // A server named by its host is dialled at the IP given
                // beside its URL, and reads back with it.
                if name.is_some() {
                    address = ip;
                }
                let port = server.address.port();
                let port = if port == core::WireKind::Https.default_port() {
                    String::new()
                } else {
                    format!(":{port}")
                };
                (
                    DnsWire::Https,
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

/// One server as a save or a test sends it.
#[derive(InputObject)]
pub struct DnsServerInput {
    pub wire: DnsWire,
    /// PLAIN and TLS: the server's IP, with a port when it is not the
    /// wire's own (`192.0.2.53:5353`, `[2001:db8::53]:853`). HTTPS: the IP
    /// dialled when the URL names a host (`192.0.2.53`, `[2001:db8::53]`),
    /// with no port, which the URL gives; unused when the URL's host is an
    /// IP. The host name is never looked up.
    #[graphql(default)]
    pub address: String,
    /// TLS: the name the certificate is checked against; absent checks it
    /// against the address.
    pub tls_name: Option<String>,
    /// HTTPS: the URL queries are posted to, `https://` and an IP or a host
    /// name. A host name is the name the certificate is checked against and
    /// the request's host; the server is dialled at `address`.
    pub url: Option<String>,
}
impl DnsServerInput {
    fn server(self) -> std::result::Result<core::ListedServer, NetworkRefusal> {
        let wire = match self.wire {
            DnsWire::Plain => core::ListedWire::Plain,
            DnsWire::Tls => core::ListedWire::Tls {
                name: self
                    .tls_name
                    .map(|name| name.trim().to_owned())
                    .filter(|name| !name.is_empty()),
            },
            DnsWire::Https => {
                let (host, port, path) = doh_url(self.url.as_deref().unwrap_or("").trim())?;
                let (ip, name) = match host {
                    DohHost::Ip(ip) => (ip, None),
                    // A name is never looked up: the IP to dial is given
                    // beside it, with no port of its own.
                    DohHost::Name(name) => (
                        bare_ip(self.address.trim()).ok_or(NetworkRefusal::ProxyDnsUrlHostname)?,
                        Some(name),
                    ),
                };
                return Ok(core::ListedServer {
                    address: std::net::SocketAddr::new(ip, port),
                    wire: core::ListedWire::Https { path, name },
                });
            }
        };
        let default_port = match self.wire {
            DnsWire::Tls => core::WireKind::Tls,
            _ => core::WireKind::Plain,
        }
        .default_port();
        Ok(core::ListedServer {
            address: server_address(self.address.trim(), default_port)
                .ok_or(NetworkRefusal::ProxyDnsServerInvalid)?,
            wire,
        })
    }
}

// An IP alone, with no port: a v6 one bare or in brackets, and with no
// zone.
fn bare_ip(text: &str) -> Option<std::net::IpAddr> {
    match text.strip_prefix('[') {
        Some(bracketed) => bracketed
            .strip_suffix(']')?
            .parse::<std::net::Ipv6Addr>()
            .ok()
            .map(std::net::IpAddr::V6),
        None => text.parse().ok(),
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

// The host of a DNS over HTTPS URL: an IP literal, or a name that follows
// the TLS name rules.
#[derive(Debug, PartialEq)]
enum DohHost {
    Ip(std::net::IpAddr),
    Name(String),
}

// A DNS over HTTPS URL as the server's host, port and the path queries are
// posted to: `https://`, an IP literal host (a v6 one in brackets) or a
// host name, an optional port, and a path from '/', its query string kept.
// Nothing else: no user and no fragment. The host is read as written, not
// as a browser reads it, so `1.2.3` is no address, and `0x7f.1`, which
// holds a letter, is read as a name.
fn doh_url(url: &str) -> std::result::Result<(DohHost, u16, String), NetworkRefusal> {
    use NetworkRefusal::ProxyDnsUrlInvalid;
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
            (DohHost::Ip(std::net::IpAddr::V6(ip)), after)
        }
        None => {
            let (host, port) = match authority.split_once(':') {
                Some((host, _)) => (host, &authority[host.len()..]),
                None => (authority, ""),
            };
            let host = match host.parse::<std::net::Ipv4Addr>() {
                Ok(ip) => DohHost::Ip(std::net::IpAddr::V4(ip)),
                // A name, as against something in no address form at all.
                Err(_)
                    if host.bytes().any(|b| b.is_ascii_alphabetic()) && core::is_tls_name(host) =>
                {
                    DohHost::Name(host.to_owned())
                }
                Err(_) => return Err(ProxyDnsUrlInvalid),
            };
            (host, port)
        }
    };
    let port = match port {
        "" => core::WireKind::Https.default_port(),
        port => port
            .strip_prefix(':')
            .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|digits| digits.parse().ok())
            .filter(|port| *port != 0)
            .ok_or(ProxyDnsUrlInvalid)?,
    };
    Ok((host, port, path.to_owned()))
}

// The profiles that use each list, loaded once for every list one request
// returns, and only when a list's `usedByProfiles` is asked for.
#[derive(Clone)]
struct ListUsers {
    ids: Arc<Vec<u32>>,
    loaded: Arc<OnceCell<HashMap<u32, Vec<String>>>>,
}

/// A saved list of servers, asked in order.
#[derive(Clone, SimpleObject)]
#[graphql(complex)]
pub struct DnsServerList {
    pub id: u32,
    pub name: String,
    pub servers: Vec<DnsServer>,
    #[graphql(skip)]
    users: ListUsers,
}
#[ComplexObject]
impl DnsServerList {
    /// The names of the profiles that use this list, in name order.
    async fn used_by_profiles(&self, ctx: &Context<'_>) -> Result<Vec<String>> {
        let users = self
            .users
            .loaded
            .get_or_try_init(|| async {
                let db = ctx.data::<Database>()?.clone();
                let ids = self.users.ids.clone();
                spawn_blocking_db("dns.list_users", move || db.dns_list_users(&ids)).await
            })
            .await?;
        Ok(users.get(&self.id).cloned().unwrap_or_default())
    }
}
fn lists(saved: Vec<core::DnsServerList>) -> Vec<DnsServerList> {
    let users = ListUsers {
        ids: Arc::new(saved.iter().map(|list| list.id).collect()),
        loaded: Arc::default(),
    };
    saved
        .into_iter()
        .map(|list| DnsServerList {
            id: list.id,
            name: list.name,
            servers: list.servers.iter().map(DnsServer::from).collect(),
            users: users.clone(),
        })
        .collect()
}

// How many routes, and how many proxies, use each profile, each counted once
// for every profile one request returns, and only when a profile's
// `usedByRoutes` or `usedByProxies` is asked for.
#[derive(Clone, Default)]
struct RouteCounts {
    routes: Arc<OnceCell<HashMap<u32, u32>>>,
    proxies: Arc<OnceCell<HashMap<u32, u32>>>,
}

/// Who answers the names a route on this profile dials, and how.
#[derive(Clone, SimpleObject)]
#[graphql(complex)]
pub struct DnsProfile {
    pub id: u32,
    pub name: String,
    /// What the profile answers for. System is ROUTE, and proxies may
    /// select it too.
    pub kind: DnsProfileKind,
    /// The built-in System profile, which cannot be changed or deleted.
    pub built_in: bool,
    pub who: DnsWho,
    /// WEAVER only.
    pub transit: Option<DnsTransit>,
    pub trust: DnsTrust,
    /// WEAVER only.
    pub server_list_id: Option<u32>,
    /// PROXY only: a leg or rung with no proxy is answered by the system
    /// resolver when set, and fails its lookups when not.
    pub system_fallback: bool,
    #[graphql(skip)]
    counts: RouteCounts,
}
#[ComplexObject]
impl DnsProfile {
    /// How many routes select this profile. Routes never saved use System.
    async fn used_by_routes(&self, ctx: &Context<'_>) -> Result<u32> {
        let counts = self
            .counts
            .routes
            .get_or_try_init(|| async {
                let db = ctx.data::<Database>()?.clone();
                spawn_blocking_db("dns.profile_routes", move || db.dns_profile_route_counts()).await
            })
            .await?;
        Ok(counts.get(&self.id).copied().unwrap_or(0))
    }
    /// How many proxies find their own address with this profile.
    async fn used_by_proxies(&self, ctx: &Context<'_>) -> Result<u32> {
        let counts = self
            .counts
            .proxies
            .get_or_try_init(|| async {
                let db = ctx.data::<Database>()?.clone();
                spawn_blocking_db("dns.profile_proxies", move || db.dns_profile_proxy_counts())
                    .await
            })
            .await?;
        Ok(counts.get(&self.id).copied().unwrap_or(0))
    }
}
fn profile(saved: core::DnsProfile, counts: RouteCounts) -> DnsProfile {
    DnsProfile {
        id: saved.id,
        built_in: saved.is_built_in(),
        kind: saved.kind.into(),
        name: saved.name,
        who: saved.who.into(),
        transit: saved.transit.map(Into::into),
        trust: saved.trust.into(),
        server_list_id: saved.server_list_id,
        system_fallback: saved.system_fallback,
        counts,
    }
}

#[derive(InputObject)]
pub struct DnsServerListInput {
    /// Absent creates a list.
    pub id: Option<u32>,
    pub name: String,
    pub servers: Vec<DnsServerInput>,
}

#[derive(InputObject)]
pub struct DnsProfileInput {
    /// Absent creates a profile.
    pub id: Option<u32>,
    pub name: String,
    /// Required on create. On update absent keeps the saved kind, and a
    /// different one is refused (DNS_PROFILE_KIND_IMMUTABLE).
    pub kind: Option<DnsProfileKind>,
    pub who: DnsWho,
    /// WEAVER only; absent asks from the egress.
    pub transit: Option<DnsTransit>,
    pub trust: DnsTrust,
    /// WEAVER only.
    pub server_list_id: Option<u32>,
    /// PROXY only. Absent is false on create and keeps the saved value on
    /// update.
    pub system_fallback: Option<bool>,
}

/// What one probe of a server came to.
#[derive(SimpleObject)]
pub struct DnsProbe {
    pub supported: bool,
    /// How long the probe took.
    pub millis: u64,
    /// Why it failed, in the codes a lookup fails with.
    pub error: Option<LookupError>,
}
impl From<core::DnsProbed> for DnsProbe {
    fn from(probed: core::DnsProbed) -> Self {
        Self {
            supported: probed.supported,
            millis: u64::try_from(probed.elapsed.as_millis()).unwrap_or(u64::MAX),
            error: probed.error.map(|error| LookupError {
                code: error.failure.into(),
                detail: error.detail,
            }),
        }
    }
}

/// What one server of a test supports.
#[derive(SimpleObject)]
pub struct DnsServerCapability {
    /// The server's position in the test's `servers`, from 0.
    pub index: u32,
    /// The server's IP, with its port when that is not its wire's own.
    pub address: String,
    /// Plain DNS on port 53, or on the server's own port when it is listed
    /// plain.
    pub plain: DnsProbe,
    /// DNS over TLS on port 853, or the server's own port and TLS name when
    /// it is listed on TLS. Without a name the certificate is checked
    /// against the address.
    pub tls: DnsProbe,
    /// DNS over HTTPS on port 443 at /dns-query, or the server's own URL
    /// when it is listed on HTTPS.
    pub https: DnsProbe,
    /// The root's SOA, asked on the server's own wire and checked from the
    /// built-in root anchors: the server returns what a DNSSEC check needs.
    pub dnssec: DnsProbe,
}

#[derive(Default)]
pub(crate) struct DnsQuery;
#[Object]
impl DnsQuery {
    /// Every saved server list, in id order.
    #[graphql(guard = "AdminGuard")]
    async fn dns_server_lists(&self, ctx: &Context<'_>) -> Result<Vec<DnsServerList>> {
        let db = ctx.data::<Database>()?.clone();
        Ok(lists(
            spawn_blocking_db("dns.lists", move || db.list_dns_server_lists()).await?,
        ))
    }
    /// Every profile, the built-in System profile first, then by id. With
    /// `kind`, the profiles something of that kind may select: System, then
    /// the saved profiles of that kind.
    #[graphql(guard = "AdminGuard")]
    async fn dns_profiles(
        &self,
        ctx: &Context<'_>,
        kind: Option<DnsProfileKind>,
    ) -> Result<Vec<DnsProfile>> {
        let db = ctx.data::<Database>()?.clone();
        let counts = RouteCounts::default();
        let kind = kind.map(core::DnsProfileKind::from);
        Ok(
            spawn_blocking_db("dns.profiles", move || db.list_dns_profiles_of(kind))
                .await?
                .into_iter()
                .map(|saved| profile(saved, counts.clone()))
                .collect(),
        )
    }
}

#[derive(Default)]
pub(crate) struct DnsMutation;
#[Object]
impl DnsMutation {
    /// Save a server list: a new one without an id. Every route using it
    /// takes the change; no connection is dropped and saved answers are
    /// forgotten.
    #[graphql(guard = "AdminGuard")]
    async fn save_dns_server_list(
        &self,
        ctx: &Context<'_>,
        input: DnsServerListInput,
    ) -> Result<DnsServerList> {
        let servers = input
            .servers
            .into_iter()
            .map(DnsServerInput::server)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(refusal_error)?;
        let list = core::DnsServerList {
            id: input.id.unwrap_or(0),
            name: input.name,
            servers,
        };
        let create = input.id.is_none();
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let saved = spawn_blocking_db("dns.save_list", move || {
            db.save_dns_server_list(list, create)
        })
        .await?;
        refresh(ctx).await?;
        Ok(lists(vec![saved]).remove(0))
    }
    /// Delete a server list, refused while a profile uses it.
    #[graphql(guard = "AdminGuard")]
    async fn delete_dns_server_list(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_db("dns.delete_list", move || db.delete_dns_server_list(id)).await?;
        refresh(ctx).await?;
        Ok(true)
    }
    /// Save a profile: a new one without an id. Every route using it takes
    /// the change; no connection is dropped and saved answers are
    /// forgotten.
    #[graphql(guard = "AdminGuard")]
    async fn save_dns_profile(
        &self,
        ctx: &Context<'_>,
        input: DnsProfileInput,
    ) -> Result<DnsProfile> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let create = input.id.is_none();
        // Absent on update keeps what is saved.
        let saved = match input.id {
            Some(id) if input.system_fallback.is_none() || input.kind.is_none() => {
                let read_db = db.clone();
                Some(
                    spawn_blocking_db("dns.profile", move || read_db.dns_profile(id))
                        .await?
                        .ok_or_else(|| refusal_error(NetworkRefusal::DnsProfileNotFound))?,
                )
            }
            _ => None,
        };
        let system_fallback = input
            .system_fallback
            .or(saved.as_ref().map(|saved| saved.system_fallback))
            .unwrap_or(false);
        let kind = match (input.kind, &saved) {
            (Some(kind), _) => kind.into(),
            (None, Some(saved)) => saved.kind,
            // Only a create reads nothing saved.
            (None, None) => {
                return Err(refusal_error(NetworkRefusal::DnsProfileKindRequired));
            }
        };
        let draft = core::DnsProfile {
            id: input.id.unwrap_or(0),
            name: input.name,
            kind,
            who: input.who.into(),
            transit: input.transit.map(Into::into),
            trust: input.trust.into(),
            server_list_id: input.server_list_id,
            system_fallback,
        };
        let saved = spawn_blocking_db("dns.save_profile", move || {
            db.save_dns_profile(draft, create)
        })
        .await?;
        refresh(ctx).await?;
        Ok(profile(saved, RouteCounts::default()))
    }
    /// Delete a profile, refused for System and while a route or a proxy
    /// uses it.
    #[graphql(guard = "AdminGuard")]
    async fn delete_dns_profile(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_db("dns.delete_profile", move || db.delete_dns_profile(id)).await?;
        refresh(ctx).await?;
        Ok(true)
    }
    /// What each server supports, probed through the drafted `route` from
    /// where its DNS profile would ask listed servers: the first path's
    /// egress, or its proxies when the profile asks through the proxy. One
    /// to ten servers, each well formed; nothing is saved. Every probe runs
    /// in a runtime of the test's own, with nothing a route holds.
    #[graphql(guard = "AdminGuard")]
    async fn test_dns_servers(
        &self,
        ctx: &Context<'_>,
        route: RouteInput,
        servers: Vec<DnsServerInput>,
    ) -> Result<Vec<DnsServerCapability>> {
        if servers.is_empty() {
            return Err(refusal_error(NetworkRefusal::ProxyDnsServersRequired));
        }
        if servers.len() > core::MAX_DNS_SERVERS {
            return Err(refusal_error(NetworkRefusal::ProxyDnsServerCount));
        }
        // Each server is checked as a list's are, and the first that fails
        // is named by its position in `servers`.
        let servers = servers
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                input
                    .server()
                    .and_then(|server| server.validate().map(|()| server))
                    .map_err(|refusal| {
                        let mut refusal = weaver_server_core::Refusal::from(refusal);
                        refusal
                            .params
                            .insert("index".into(), serde_json::json!(index));
                        refusal_error(refusal)
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let dns_profile = route.dns_profile_id.unwrap_or(core::SYSTEM_DNS_PROFILE_ID);
        let route = route.route()?;
        let runtime = runtime(ctx)?;
        // The test is built against the saved configuration while no save
        // runs, and probes after, so a slow server holds no save back.
        let test = {
            let _guard = runtime.mutations.lock().await;
            runtime
                .network
                .dns_server_test(
                    &route,
                    dns_profile,
                    servers.iter().map(core::ListedServer::engine).collect(),
                )
                .map_err(refusal_error)?
        };
        let probes = test.run().await;
        Ok(servers
            .iter()
            .zip(probes)
            .enumerate()
            .map(|(index, (server, probe))| DnsServerCapability {
                index: index as u32,
                address: server.engine().to_string(),
                plain: probe.plain.into(),
                tls: probe.tls.into(),
                https: probe.https.into(),
                dnssec: probe.dnssec.into(),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_server_reads_back_as_it_was_written() {
        let input = DnsServerInput {
            wire: DnsWire::Https,
            address: String::new(),
            tls_name: None,
            url: Some("https://[2001:db8::53]:8443/dns-query?x=1".into()),
        };
        let server = input.server().unwrap();
        assert_eq!(
            DnsServer::from(&server).url.as_deref(),
            Some("https://[2001:db8::53]:8443/dns-query?x=1")
        );
        let plain = DnsServerInput {
            wire: DnsWire::Plain,
            address: "192.0.2.53:5353".into(),
            tls_name: None,
            url: None,
        }
        .server()
        .unwrap();
        assert_eq!(DnsServer::from(&plain).address, "192.0.2.53:5353");
    }

    fn doh(url: &str, address: &str) -> std::result::Result<core::ListedServer, NetworkRefusal> {
        DnsServerInput {
            wire: DnsWire::Https,
            address: address.into(),
            tls_name: None,
            url: Some(url.into()),
        }
        .server()
    }

    #[test]
    fn a_doh_url_with_a_host_name_needs_the_ip_beside_it() {
        assert_eq!(
            doh_url("https://dns.example/dns-query"),
            Ok((
                DohHost::Name("dns.example".into()),
                443,
                "/dns-query".into()
            ))
        );
        assert_eq!(
            doh_url("https://192.0.2.53"),
            Err(NetworkRefusal::ProxyDnsUrlInvalid)
        );
        for bad in [
            "https://dns..example/q",
            "https://-dns.example/q",
            "https://dns_ex ample/q",
        ] {
            assert_eq!(
                doh_url(bad),
                Err(NetworkRefusal::ProxyDnsUrlInvalid),
                "{bad}"
            );
        }
        // No address, or one that is not a bare IP, is refused as such.
        for address in [
            "",
            " ",
            "dns.example",
            "192.0.2.53:443",
            "[2001:db8::53]:443",
            "1.2.3",
            "fe80::1%en0",
        ] {
            assert_eq!(
                doh("https://dns.example/dns-query", address),
                Err(NetworkRefusal::ProxyDnsUrlHostname),
                "{address:?}"
            );
        }
        // The IP is dialled at the URL's port, and the name kept.
        assert_eq!(
            doh("https://dns.example:8443/dns-query", "[2001:db8::53]"),
            Ok(core::ListedServer {
                address: "[2001:db8::53]:8443".parse().unwrap(),
                wire: core::ListedWire::Https {
                    path: "/dns-query".into(),
                    name: Some("dns.example".into()),
                },
            })
        );
        assert_eq!(
            doh("https://dns.example/dns-query", " 2001:db8::53 ")
                .unwrap()
                .address,
            "[2001:db8::53]:443".parse().unwrap()
        );
        // An IP host needs no address, and one sent beside it is unused.
        assert_eq!(
            doh("https://192.0.2.53/dns-query", "198.51.100.1"),
            Ok(core::ListedServer {
                address: "192.0.2.53:443".parse().unwrap(),
                wire: core::ListedWire::Https {
                    path: "/dns-query".into(),
                    name: None,
                },
            })
        );
    }

    #[test]
    fn a_doh_server_named_by_its_host_reads_back_as_it_was_written() {
        for (url, address, read) in [
            ("https://dns.example/dns-query", "192.0.2.53", "192.0.2.53"),
            (
                "https://dns.example:8443/q?x=1",
                "[2001:db8::53]",
                "[2001:db8::53]",
            ),
            (
                "https://dns.example/dns-query",
                "2001:db8::53",
                "[2001:db8::53]",
            ),
        ] {
            let server = doh(url, address).unwrap();
            let shown = DnsServer::from(&server);
            assert_eq!(shown.url.as_deref(), Some(url));
            assert_eq!(shown.address, read);
            assert_eq!(shown.wire, DnsWire::Https);
            // What reads back saves back the same.
            assert_eq!(doh(&shown.url.unwrap(), &shown.address), Ok(server));
        }
        // An IP host reads back as before.
        let shown = DnsServer::from(&doh("https://192.0.2.53:8443/dns-query", "").unwrap());
        assert_eq!(
            shown.url.as_deref(),
            Some("https://192.0.2.53:8443/dns-query")
        );
        assert_eq!(shown.address, "192.0.2.53:8443");
    }
}
