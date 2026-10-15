use crate::{
    auth::AdminGuard,
    observability::{refusal_error, spawn_blocking_db},
    proxies::ProxyKind,
};
use async_graphql::{
    Context, Enum, InputObject, Object, OneofObject, Result, SimpleObject, Subscription,
};
use std::{sync::Arc, time::Duration};
use weaver_server_core::{
    Database,
    proxies::{self as core, NetworkRefusal, ProxyRuntime},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Enum)]
pub enum RouteFailover {
    #[default]
    Redistribute,
    Hold,
}
impl From<RouteFailover> for core::Failover {
    fn from(value: RouteFailover) -> Self {
        match value {
            RouteFailover::Redistribute => Self::Redistribute,
            RouteFailover::Hold => Self::Hold,
        }
    }
}
impl From<core::Failover> for RouteFailover {
    fn from(value: core::Failover) -> Self {
        match value {
            core::Failover::Redistribute => Self::Redistribute,
            core::Failover::Hold => Self::Hold,
        }
    }
}

#[derive(Clone, Debug, OneofObject)]
pub enum RouteRungInput {
    Proxy(u32),
    Pool(u32),
    Chain(Vec<u32>),
}
impl From<RouteRungInput> for core::Rung {
    fn from(value: RouteRungInput) -> Self {
        match value {
            RouteRungInput::Proxy(id) => Self::Proxy { id },
            RouteRungInput::Pool(id) => Self::Pool { id },
            RouteRungInput::Chain(ids) => Self::Chain { ids },
        }
    }
}
#[derive(Clone, Debug, InputObject)]
pub struct RouteLadderInput {
    pub rungs: Vec<RouteRungInput>,
    pub direct_fallback: bool,
}
#[derive(Clone, Debug, OneofObject)]
pub enum LegPathInput {
    Direct(bool),
    Ladder(RouteLadderInput),
}
#[derive(Clone, Debug, InputObject)]
pub struct RouteLegInput {
    pub egress_id: u32,
    pub weight: u8,
    pub path: LegPathInput,
}
impl TryFrom<RouteLegInput> for core::RouteLeg {
    type Error = async_graphql::Error;
    fn try_from(value: RouteLegInput) -> Result<Self> {
        let path = match value.path {
            LegPathInput::Direct(true) => core::LegPath::Direct,
            LegPathInput::Direct(false) => {
                return Err(refusal_error(NetworkRefusal::RouteDirectPathFalse));
            }
            LegPathInput::Ladder(ladder) => core::LegPath::Ladder {
                rungs: ladder.rungs.into_iter().map(Into::into).collect(),
                direct_fallback: ladder.direct_fallback,
            },
        };
        Ok(Self {
            egress_id: value.egress_id,
            weight: value.weight,
            path,
        })
    }
}
#[derive(Clone, Debug, InputObject)]
pub struct RouteInput {
    pub legs: Vec<RouteLegInput>,
    #[graphql(default)]
    pub failover: RouteFailover,
}
#[derive(Clone, PartialEq, SimpleObject)]
#[graphql(name = "Route")]
pub struct RouteGql {
    pub legs: Vec<RouteLegGql>,
    pub failover: RouteFailover,
}
impl From<core::RoutingPolicy> for RouteGql {
    fn from(policy: core::RoutingPolicy) -> Self {
        let route = policy.route();
        Self {
            legs: route.legs.into_iter().map(Into::into).collect(),
            failover: route.failover.into(),
        }
    }
}
pub(crate) fn selected_policy(
    routing: Option<crate::proxies::RoutingPolicyInput>,
    route: Option<RouteInput>,
) -> Result<Option<core::RoutingPolicy>> {
    match (routing, route) {
        (Some(_), Some(_)) => Err(refusal_error(NetworkRefusal::RouteAndRoutingBoth)),
        (Some(legacy), None) => Ok(Some(legacy.into())),
        (None, Some(input)) => {
            let route = input.route()?;
            Ok(Some(core::RoutingPolicy {
                legs: route.legs,
                failover: route.failover,
                ..Default::default()
            }))
        }
        (None, None) => Ok(None),
    }
}
impl RouteInput {
    pub fn route(self) -> Result<core::Route> {
        let route = core::Route {
            legs: self
                .legs
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_>>()?,
            failover: self.failover.into(),
        };
        route.validate_shape().map_err(refusal_error)?;
        Ok(route)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum RungKind {
    Proxy,
    Pool,
    Chain,
}
#[derive(Clone, Debug, PartialEq, SimpleObject)]
#[graphql(name = "RouteRung")]
pub struct RouteRungGql {
    pub kind: RungKind,
    pub proxy_id: Option<u32>,
    pub pool_id: Option<u32>,
    pub chain_ids: Vec<u32>,
}
impl From<core::Rung> for RouteRungGql {
    fn from(r: core::Rung) -> Self {
        match r {
            core::Rung::Proxy { id } => Self {
                kind: RungKind::Proxy,
                proxy_id: Some(id),
                pool_id: None,
                chain_ids: vec![],
            },
            core::Rung::Pool { id } => Self {
                kind: RungKind::Pool,
                proxy_id: None,
                pool_id: Some(id),
                chain_ids: vec![],
            },
            core::Rung::Chain { ids } => Self {
                kind: RungKind::Chain,
                proxy_id: None,
                pool_id: None,
                chain_ids: ids,
            },
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum LegPathKind {
    Direct,
    Ladder,
}
#[derive(Clone, Debug, PartialEq, SimpleObject)]
#[graphql(name = "LegPath")]
pub struct LegPathGql {
    pub kind: LegPathKind,
    pub rungs: Vec<RouteRungGql>,
    pub direct_fallback: bool,
}
#[derive(Clone, Debug, PartialEq, SimpleObject)]
#[graphql(name = "RouteLeg")]
pub struct RouteLegGql {
    pub egress_id: u32,
    pub weight: u8,
    pub path: LegPathGql,
}
impl From<core::RouteLeg> for RouteLegGql {
    fn from(leg: core::RouteLeg) -> Self {
        let path = match leg.path {
            core::LegPath::Direct => LegPathGql {
                kind: LegPathKind::Direct,
                rungs: vec![],
                direct_fallback: false,
            },
            core::LegPath::Ladder {
                rungs,
                direct_fallback,
            } => LegPathGql {
                kind: LegPathKind::Ladder,
                rungs: rungs.into_iter().map(Into::into).collect(),
                direct_fallback,
            },
        };
        Self {
            egress_id: leg.egress_id,
            weight: leg.weight,
            path,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum EgressBindingKind {
    System,
    Interface,
    SourceAddress,
}
#[derive(InputObject)]
pub struct EgressInterfaceInput {
    pub name: String,
    pub binding_kind: EgressBindingKind,
    pub interface_name: Option<String>,
    pub source_address: Option<String>,
    #[graphql(default = true)]
    pub enabled: bool,
    #[graphql(default)]
    pub max_download_speed: u64,
    /// The download allowance on this egress. Omitted on an update, the
    /// saved quota stays as it is; omitted on a create, there is none.
    pub download_quota: Option<crate::servers::types::ServerDownloadQuotaInput>,
}
impl EgressInterfaceInput {
    fn egress(
        self,
        id: u32,
        saved_quota: weaver_server_core::servers::ServerDownloadQuotaConfig,
    ) -> Result<core::EgressInterface> {
        // The egress checks its own quota below, so a refusal names an egress.
        let download_quota = match self.download_quota {
            Some(quota) => quota.config(),
            None => saved_quota,
        };
        let binding = match self.binding_kind {
            EgressBindingKind::System => core::EgressBinding::System,
            EgressBindingKind::Interface => {
                if cfg!(windows) {
                    return Err(refusal_error(
                        NetworkRefusal::EgressInterfaceBindingUnsupported,
                    ));
                }
                core::EgressBinding::Interface {
                    name: self.interface_name.ok_or_else(|| {
                        refusal_error(NetworkRefusal::EgressInterfaceNameRequired)
                    })?,
                }
            }
            EgressBindingKind::SourceAddress => core::EgressBinding::SourceAddress {
                address: self
                    .source_address
                    .ok_or_else(|| refusal_error(NetworkRefusal::EgressSourceAddressRequired))?
                    .parse()
                    .map_err(|_| refusal_error(NetworkRefusal::EgressSourceAddressInvalid))?,
            },
        };
        let egress = core::EgressInterface {
            id,
            name: self.name,
            binding,
            enabled: self.enabled,
            max_download_speed: self.max_download_speed,
            download_quota,
        };
        egress.validate().map_err(refusal_error)?;
        Ok(egress)
    }
}
#[derive(Clone, Debug, PartialEq, SimpleObject)]
#[graphql(name = "EgressInterface")]
pub struct EgressInterfaceGql {
    pub id: u32,
    pub name: String,
    pub binding_kind: EgressBindingKind,
    pub interface_name: Option<String>,
    pub source_address: Option<String>,
    pub addresses: Vec<String>,
    pub enabled: bool,
    pub max_download_speed: u64,
    /// The download allowance and its live usage.
    pub download_quota: crate::servers::types::ServerDownloadQuota,
    /// The live usage of the download allowance, or null while this Weaver
    /// is not tracking it.
    pub download_quota_usage: Option<crate::servers::types::DownloadQuotaUsage>,
    pub health: String,
    pub reason: Option<String>,
}
fn egress_gql(
    e: core::EgressInterface,
    interfaces: &core::InterfaceSnapshot,
    network: &core::NetworkRuntime,
) -> EgressInterfaceGql {
    let usage = network.egress_quota_usage(e.id);
    let (health, reason) = match interfaces.health(&e, None) {
        core::EgressHealth::Down(reason) => ("DOWN", Some(reason)),
        _ if usage.as_ref().is_some_and(|usage| usage.blocked) => {
            ("DOWN", Some(core::QUOTA_REACHED.to_owned()))
        }
        core::EgressHealth::Up => ("UP", None),
        core::EgressHealth::Unknown => ("UNKNOWN", interfaces.error.clone()),
    };
    let download_quota =
        crate::servers::types::ServerDownloadQuota::from_config(&e.download_quota, usage.as_ref());
    let download_quota_usage = usage.as_ref().map(Into::into);
    let addresses = interfaces
        .interfaces
        .iter()
        .filter(|interface| interface.up)
        .flat_map(|interface| {
            interface.addresses.iter().filter(|address| {
                // Loopback can never be the source of an outbound connection.
                address.usable()
                    && !address.address.is_loopback()
                    && match &e.binding {
                        core::EgressBinding::System => true,
                        core::EgressBinding::Interface { name } => name == &interface.name,
                        core::EgressBinding::SourceAddress { address: source } => {
                            source == &address.address
                        }
                    }
            })
        })
        .map(|address| address.address.to_string())
        .collect();
    let (binding_kind, interface_name, source_address) = match e.binding {
        core::EgressBinding::System => (EgressBindingKind::System, None, None),
        core::EgressBinding::Interface { name } => (EgressBindingKind::Interface, Some(name), None),
        core::EgressBinding::SourceAddress { address } => (
            EgressBindingKind::SourceAddress,
            None,
            Some(address.to_string()),
        ),
    };
    EgressInterfaceGql {
        id: e.id,
        name: e.name,
        binding_kind,
        interface_name,
        source_address,
        addresses,
        enabled: e.enabled,
        max_download_speed: e.max_download_speed,
        download_quota,
        download_quota_usage,
        health: health.into(),
        reason,
    }
}
#[derive(SimpleObject)]
pub struct DiscoveredNetworkInterface {
    pub name: String,
    pub index: Option<u32>,
    pub up: bool,
    pub addresses: Vec<String>,
}
#[derive(SimpleObject)]
pub struct PlatformNetworking {
    pub platform: String,
    pub egress_binding_kinds: Vec<EgressBindingKind>,
    pub source_address_hint: String,
    pub container: bool,
    pub bridge_network_suspected: bool,
    pub max_wireguard_instances: usize,
    pub notes: Vec<String>,
}
#[derive(SimpleObject)]
pub struct EgressTestResult {
    pub success: bool,
    pub message: String,
    pub source_address: Option<String>,
    pub connect_millis: Option<f64>,
    pub proxy_id: Option<u32>,
}
impl From<core::PathTest> for EgressTestResult {
    fn from(test: core::PathTest) -> Self {
        match test.outcome {
            Ok(_) => Self {
                success: true,
                message: "Connection succeeded".into(),
                source_address: test.source_address.map(|s| s.to_string()),
                connect_millis: Some(test.elapsed.as_secs_f64() * 1000.0),
                proxy_id: test.proxy_id,
            },
            Err(fault) => Self {
                success: false,
                message: fault.to_string(),
                source_address: None,
                connect_millis: None,
                proxy_id: test.proxy_id,
            },
        }
    }
}
// What a test through a drafted route stacks on top of it.
#[derive(Clone, Copy, Debug, OneofObject)]
pub enum TestSubjectInput {
    Proxy(u32),
    Pool(u32),
}
#[derive(Clone, Debug, InputObject)]
pub struct TestTargetInput {
    pub host: String,
    pub port: u16,
}
// One path through a drafted route, and the proxy tested on top of it.
#[derive(SimpleObject)]
pub struct PathTestResult {
    pub leg: u32,
    pub egress_id: u32,
    pub carrier_ids: Vec<u32>,
    // The proxy under test, or the member the pool under test picked.
    pub proxy_id: Option<u32>,
    // The pool under test, when it carried the target.
    pub pool_id: Option<u32>,
    pub success: bool,
    pub code: String,
    // The outcome in English, for a client with no sentence for its code.
    pub message: String,
    // The values the client's sentence for the code may name.
    pub params: async_graphql::Json<serde_json::Map<String, serde_json::Value>>,
    pub failing_proxy_id: Option<u32>,
    pub source_address: Option<String>,
    pub connect_millis: f64,
    // How the host the path's final dial named was resolved.
    pub resolution: Option<HostResolution>,
}
// Who turned a test's host into addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum ResolvedBy {
    // The host is an address.
    Literal,
    // The operating system's resolver.
    Host,
    // Weaver asked DNS servers from the route's egress.
    Servers,
    // The proxy was handed the name.
    Proxy,
    // Weaver asked DNS servers through the proxy.
    ProxyServers,
}
impl From<core::ResolvedBy> for ResolvedBy {
    fn from(value: core::ResolvedBy) -> Self {
        match value {
            core::ResolvedBy::Literal => Self::Literal,
            core::ResolvedBy::Host => Self::Host,
            core::ResolvedBy::Servers => Self::Servers,
            core::ResolvedBy::Proxy => Self::Proxy,
            core::ResolvedBy::ProxyServers => Self::ProxyServers,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum LookupErrorCode {
    NoSuchName,
    NoAddress,
    TimedOut,
    ServerFailure,
    NoReply,
    Other,
}
impl From<core::LookupFailure> for LookupErrorCode {
    fn from(value: core::LookupFailure) -> Self {
        match value {
            core::LookupFailure::NoSuchName => Self::NoSuchName,
            core::LookupFailure::NoAddress => Self::NoAddress,
            core::LookupFailure::TimedOut => Self::TimedOut,
            core::LookupFailure::ServerFailure => Self::ServerFailure,
            core::LookupFailure::NoReply => Self::NoReply,
            core::LookupFailure::Other => Self::Other,
        }
    }
}
#[derive(SimpleObject)]
pub struct LookupError {
    pub code: LookupErrorCode,
    // The resolver's own words.
    pub detail: String,
}
#[derive(SimpleObject)]
pub struct HostResolution {
    pub host: String,
    pub resolved_by: ResolvedBy,
    // The proxy that resolved it, or that Weaver asked through.
    pub proxy_id: Option<u32>,
    pub dns_servers: Vec<String>,
    pub addresses: Vec<String>,
    // The answer was held from an earlier lookup.
    pub cached: bool,
    pub connected_address: Option<String>,
    pub error: Option<LookupError>,
}
impl From<core::HostResolution> for HostResolution {
    fn from(value: core::HostResolution) -> Self {
        Self {
            host: value.host,
            resolved_by: value.by.into(),
            proxy_id: value.proxy,
            dns_servers: value.servers.iter().map(ToString::to_string).collect(),
            addresses: value.addresses.iter().map(ToString::to_string).collect(),
            cached: value.cached,
            connected_address: value.connected.map(|ip| ip.to_string()),
            error: value.error.map(|error| LookupError {
                code: error.failure.into(),
                detail: error.detail,
            }),
        }
    }
}
impl From<core::PathTest> for PathTestResult {
    fn from(test: core::PathTest) -> Self {
        let (success, code, message, params) = match &test.outcome {
            Ok(pass) => (true, pass.code(), pass.to_string(), serde_json::Map::new()),
            Err(fault) => (
                false,
                fault.code(),
                fault.to_string(),
                core::fault_params(fault),
            ),
        };
        Self {
            leg: u32::try_from(test.leg).unwrap_or(u32::MAX),
            egress_id: test.egress_id,
            carrier_ids: test.carriers,
            proxy_id: test.proxy_id,
            pool_id: test.pool_id,
            success,
            code: code.into(),
            message,
            params: async_graphql::Json(params),
            failing_proxy_id: test.failing_proxy,
            source_address: test.source_address.map(|s| s.to_string()),
            connect_millis: test.elapsed.as_secs_f64() * 1000.0,
            resolution: test.resolution.map(Into::into),
        }
    }
}
// A route of one direct leg on an egress, for the tests that name only it.
fn direct_on(egress_id: u32) -> core::Route {
    core::Route {
        legs: vec![core::RouteLeg {
            egress_id,
            weight: 100,
            path: core::LegPath::Direct,
        }],
        failover: core::Failover::default(),
    }
}
async fn test_via(
    runtime: &ProxyRuntime,
    route: &core::Route,
    subject: core::TestSubject,
    target: Option<(String, u16)>,
) -> Result<Vec<core::PathTest>> {
    let _guard = runtime.mutations.lock().await;
    let test: std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<Vec<core::PathTest>, NetworkRefusal>,
                > + Send
                + '_,
        >,
    > = Box::pin(runtime.network.test_via(route, subject, target));
    test.await.map_err(refusal_error)
}
#[derive(Clone, Debug, InputObject)]
pub struct ProxyPoolInput {
    pub name: String,
    pub kind: ProxyKind,
    pub member_ids: Vec<u32>,
    #[graphql(default = true)]
    pub enabled: bool,
}
impl ProxyPoolInput {
    fn pool(self, id: u32) -> core::ProxyPool {
        core::ProxyPool {
            id,
            name: self.name,
            kind: self.kind.into(),
            member_ids: self.member_ids,
            enabled: self.enabled,
        }
    }
}
#[derive(Clone, Debug, PartialEq, SimpleObject)]
#[graphql(name = "ProxyPool")]
pub struct ProxyPoolGql {
    pub id: u32,
    pub name: String,
    pub kind: ProxyKind,
    pub member_ids: Vec<u32>,
    pub enabled: bool,
}
impl From<core::ProxyPool> for ProxyPoolGql {
    fn from(p: core::ProxyPool) -> Self {
        Self {
            id: p.id,
            name: p.name,
            kind: p.kind.into(),
            member_ids: p.member_ids,
            enabled: p.enabled,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum NetworkConsumerKind {
    Server,
    Rss,
    // Default outbound. There is one, so its id is ignored.
    Outbound,
}
impl NetworkConsumerKind {
    pub(crate) fn consumer(self, id: u32) -> core::Consumer {
        match self {
            Self::Server => core::Consumer::Server(id),
            Self::Rss => core::Consumer::Rss(id),
            Self::Outbound => core::Consumer::Outbound,
        }
    }
}
#[derive(SimpleObject)]
pub struct NetworkRoute {
    pub consumer: String,
    pub legs: Vec<RouteLegGql>,
    pub failover: RouteFailover,
}
#[derive(Clone, PartialEq, SimpleObject)]
pub struct NetworkLegFlow {
    pub consumer: String,
    pub position: i32,
    pub egress_id: u32,
    pub weight: u8,
    pub target: u16,
    pub open: u16,
    pub opening: u16,
    pub state: String,
    pub reason: Option<String>,
    pub path: LegPathGql,
    pub pinned_address: Option<String>,
    pub source_address: Option<String>,
    pub bytes_per_second: u64,
    pub selected_rung: Option<usize>,
    pub selected_proxy_id: Option<u32>,
    pub rung_states: Vec<String>,
    pub failing_hops: Vec<NetworkFailingHop>,
}
/// The first proxy hop on one of a leg's ladder rungs known to be failing.
/// Hops past it on a chain were not reached, so nothing is known of them.
#[derive(Clone, PartialEq, SimpleObject)]
pub struct NetworkFailingHop {
    pub rung: usize,
    pub proxy_id: u32,
    // The failure in English, for a client with no sentence for its code.
    pub reason: String,
    pub code: String,
    // The values the client's sentence for the code may name.
    pub params: async_graphql::Json<serde_json::Map<String, serde_json::Value>>,
}
#[derive(Clone, PartialEq, SimpleObject)]
pub struct NetworkConsumerFlow {
    pub key: String,
    pub id: u32,
    pub name: String,
    pub kind: NetworkConsumerKind,
    pub cap: u16,
    pub route: RouteGql,
}
#[derive(Clone, PartialEq, SimpleObject)]
pub struct NetworkFlow {
    pub consumers: Vec<NetworkConsumerFlow>,
    pub legs: Vec<NetworkLegFlow>,
    pub pools: Vec<NetworkPoolFlow>,
    pub egresses: Vec<EgressInterfaceGql>,
    pub proxies: Vec<crate::proxies::ProxyProfileGql>,
    pub proxy_pools: Vec<ProxyPoolGql>,
    pub sampled_at: f64,
}
#[derive(Clone, PartialEq, SimpleObject)]
pub struct NetworkPoolFlow {
    pub pool_id: u32,
    pub egress_id: u32,
    pub pinned_member: Option<u32>,
    pub members: Vec<NetworkPoolMemberFlow>,
}
#[derive(Clone, PartialEq, SimpleObject)]
pub struct NetworkPoolMemberFlow {
    pub id: u32,
    pub state: String,
    pub open: u32,
    pub opening: u32,
    pub warmed: bool,
    pub blocked: Option<String>,
    pub handshake_ms: Option<f64>,
    pub connect_ms: Option<f64>,
    // The delivery rate the pool judges the member by, per connection.
    pub bytes_per_second: Option<f64>,
    pub samples: u32,
    pub failures: u32,
    // What the member's open connections carry now, all together.
    pub live_bytes_per_second: u64,
}
pub(crate) fn flow(runtime: &ProxyRuntime) -> NetworkFlow {
    let mut legs = Vec::new();
    for route in runtime.network.live_routes() {
        let definitions = route.legs.read().expect("route legs");
        for allocation in route.weighted.allocations() {
            let Some(leg) = definitions.get(allocation.position) else {
                continue;
            };
            let rung_states: Vec<String> =
                leg.rung_states().into_iter().map(str::to_owned).collect();
            // A leg counts two failed dials before it cools, but its ladder
            // cools each rung on the first. With no connection open and every
            // rung cooling the leg has no path it may dial until one lapses,
            // so it is down even while its own failure count reads healthy.
            let ladder_cooling = allocation.open == 0
                && allocation.opening == 0
                && !rung_states.is_empty()
                && rung_states.iter().all(|state| state == "COOLDOWN");
            let (state, reason) = match allocation.health {
                core::LegHealthState::Up if ladder_cooling => {
                    ("DOWN", Some("every rung is cooling down".to_owned()))
                }
                core::LegHealthState::Up => ("UP", None),
                core::LegHealthState::Probing => ("PROBING", None),
                core::LegHealthState::Down(reason) => ("DOWN", Some(reason)),
                core::LegHealthState::Blocked(reason) => ("BLOCKED", Some(reason)),
            };
            legs.push(NetworkLegFlow {
                consumer: route.consumer.key(),
                position: allocation.position as i32,
                egress_id: leg.definition.egress_id,
                weight: leg.definition.weight,
                target: allocation.target,
                open: allocation.open,
                opening: allocation.opening,
                state: if state == "UP" && allocation.open == 0 && allocation.opening == 0 {
                    "IDLE"
                } else {
                    state
                }
                .into(),
                reason: reason.or_else(|| leg.warning.clone()),
                path: RouteLegGql::from(leg.definition.clone()).path,
                pinned_address: leg
                    .address_plan
                    .as_ref()
                    .and_then(|p| p.snapshot().pinned)
                    .map(|p| p.to_string()),
                source_address: allocation.source.map(|source| source.to_string()),
                bytes_per_second: allocation.bytes_per_second,
                selected_rung: allocation.path.as_ref().and_then(|path| path.rung),
                selected_proxy_id: allocation
                    .path
                    .as_ref()
                    .and_then(|path| path.proxies.last().copied()),
                rung_states,
                failing_hops: leg
                    .failing_hops()
                    .into_iter()
                    .enumerate()
                    .filter_map(|(rung, hop)| {
                        hop.map(|hop| NetworkFailingHop {
                            rung,
                            proxy_id: hop.proxy,
                            reason: hop.fault.to_string(),
                            code: hop.fault.code().into(),
                            params: async_graphql::Json(core::fault_params(&hop.fault)),
                        })
                    })
                    .collect(),
            });
        }
    }
    // Routes configured for consumers that have not dialed yet, such as an
    // inactive server, are part of the picture too: the operator configured
    // them, and an egress they depend on may already be missing.
    for leg in runtime.network.dormant_legs() {
        let (state, reason) = match leg.health {
            core::LegHealthState::Up | core::LegHealthState::Probing => ("IDLE", None),
            core::LegHealthState::Down(reason) => ("DOWN", Some(reason)),
            core::LegHealthState::Blocked(reason) => ("BLOCKED", Some(reason)),
        };
        legs.push(NetworkLegFlow {
            consumer: leg.consumer.key(),
            position: leg.position as i32,
            egress_id: leg.definition.egress_id,
            weight: leg.definition.weight,
            target: 0,
            open: 0,
            opening: 0,
            state: state.into(),
            reason,
            path: RouteLegGql::from(leg.definition).path,
            pinned_address: None,
            source_address: None,
            bytes_per_second: 0,
            selected_rung: None,
            selected_proxy_id: None,
            rung_states: Vec::new(),
            failing_hops: Vec::new(),
        });
    }
    let mut pools: Vec<_> = runtime
        .network
        .pool_status()
        .into_iter()
        .map(|(pool_id, egress_id, plan, members)| NetworkPoolFlow {
            pool_id,
            egress_id,
            pinned_member: plan.pinned,
            members: members
                .into_iter()
                .map(|m| {
                    let evidence = plan.candidates.iter().find(|c| c.candidate == m.id);
                    NetworkPoolMemberFlow {
                        id: m.id,
                        state: if m.blocked.is_some() {
                            "BLOCKED"
                        } else if m.opening > 0 {
                            "PROBING"
                        } else {
                            evidence.map_or("UNMEASURED", |candidate| candidate.state)
                        }
                        .into(),
                        open: m.open,
                        opening: m.opening,
                        warmed: m.warmed,
                        blocked: m.blocked,
                        handshake_ms: m.session_handshake.map(|d| d.as_secs_f64() * 1000.0),
                        connect_ms: evidence
                            .and_then(|c| c.connect_time)
                            .map(|d| d.as_secs_f64() * 1000.0),
                        bytes_per_second: evidence.and_then(|c| c.bytes_per_second),
                        samples: evidence.map_or(0, |c| c.samples),
                        failures: evidence.map_or(0, |c| c.failures),
                        live_bytes_per_second: m.bytes_per_second,
                    }
                })
                .collect(),
        })
        .collect();
    pools.sort_by_key(|p| (p.pool_id, p.egress_id));
    legs.sort_by_key(|leg| (leg.consumer.clone(), leg.position));
    let (mut egresses, mut profiles, mut definitions) = runtime.network.configuration_snapshot();
    egresses.sort_by_key(|e| e.id);
    profiles.sort_by_key(|p| p.id);
    definitions.sort_by_key(|p| p.id);
    for pool in &mut pools {
        if let Some(definition) = definitions.iter().find(|p| p.id == pool.pool_id) {
            for id in &definition.member_ids {
                if !pool.members.iter().any(|member| member.id == *id) {
                    pool.members.push(NetworkPoolMemberFlow {
                        id: *id,
                        state: if profiles.iter().any(|p| p.id == *id && p.enabled) {
                            "UNMEASURED"
                        } else {
                            "DISABLED"
                        }
                        .into(),
                        open: 0,
                        opening: 0,
                        warmed: false,
                        blocked: None,
                        handshake_ms: None,
                        connect_ms: None,
                        bytes_per_second: None,
                        samples: 0,
                        failures: 0,
                        live_bytes_per_second: 0,
                    });
                }
            }
            pool.members.sort_by_key(|member| member.id);
        }
    }
    let interfaces = runtime.network.interfaces();
    let mut consumers: Vec<_> = runtime
        .network
        .consumer_snapshot()
        .into_iter()
        .map(|(consumer, name, cap, route)| {
            let (kind, id) = match consumer {
                core::Consumer::Server(id) => (NetworkConsumerKind::Server, id),
                core::Consumer::Rss(id) => (NetworkConsumerKind::Rss, id),
                core::Consumer::Outbound => (NetworkConsumerKind::Outbound, 0),
            };
            NetworkConsumerFlow {
                key: consumer.key(),
                id,
                name,
                kind,
                cap,
                route: route.into(),
            }
        })
        .collect();
    consumers.sort_by(|a, b| a.key.cmp(&b.key));
    NetworkFlow {
        consumers,
        legs,
        pools,
        egresses: egresses
            .into_iter()
            .map(|e| egress_gql(e, &interfaces, &runtime.network))
            .collect(),
        proxies: profiles.into_iter().map(Into::into).collect(),
        proxy_pools: definitions.into_iter().map(Into::into).collect(),
        sampled_at: chrono::Utc::now().timestamp_millis() as f64 / 1000.0,
    }
}
fn runtime(ctx: &Context<'_>) -> Result<Arc<ProxyRuntime>> {
    crate::proxies::runtime(ctx)
}

#[derive(Default)]
pub struct NetworkingQuery;
#[Object]
impl NetworkingQuery {
    #[graphql(guard = "AdminGuard")]
    async fn egress_interfaces(&self, ctx: &Context<'_>) -> Result<Vec<EgressInterfaceGql>> {
        let runtime = runtime(ctx)?;
        let snapshot = runtime.network.interfaces();
        let db = ctx.data::<Database>()?.clone();
        Ok(
            spawn_blocking_db("network.egresses", move || db.list_egress_interfaces())
                .await?
                .into_iter()
                .map(|e| egress_gql(e, &snapshot, &runtime.network))
                .collect(),
        )
    }
    #[graphql(guard = "AdminGuard")]
    async fn discover_network_interfaces(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Vec<DiscoveredNetworkInterface>> {
        Ok(runtime(ctx)?
            .network
            .interfaces()
            .interfaces
            .into_iter()
            .map(|i| DiscoveredNetworkInterface {
                name: i.name,
                index: i.index,
                up: i.up,
                addresses: i
                    .addresses
                    .into_iter()
                    .filter(|a| a.usable())
                    .map(|a| a.address.to_string())
                    .collect(),
            })
            .collect())
    }
    #[graphql(guard = "AdminGuard")]
    async fn platform_networking(&self, ctx: &Context<'_>) -> Result<PlatformNetworking> {
        let container = std::path::Path::new("/.dockerenv").exists()
            || std::path::Path::new("/run/.containerenv").exists();
        let interfaces = runtime(ctx)?.network.interfaces();
        let visible: Vec<_> = interfaces
            .interfaces
            .iter()
            .filter(|interface| {
                interface
                    .addresses
                    .iter()
                    .any(|address| !address.address.is_loopback())
            })
            .collect();
        let bridge_network_suspected = container
            && matches!(visible.as_slice(), [interface]
            if interface.name == "eth0" && interface.addresses.iter().any(|address|
                matches!(address.address, std::net::IpAddr::V4(ip) if ip.is_private())));
        let mut notes = vec!["Direct destinations and proxy endpoints use the system DNS resolver; selecting an egress does not bind those DNS lookups.".into()];
        if bridge_network_suspected {
            notes.push("Only container interfaces are visible. Use host networking for host-link selection, or attach multiple container networks.".into());
        }
        if cfg!(target_os = "linux") {
            notes.push("Interface binding may require CAP_NET_RAW. Grant only that capability when interface binding is needed; NET_ADMIN is not required. Containers retain it only with WEAVER_RETAIN_NET_RAW=true.".into());
        }
        Ok(PlatformNetworking {platform:std::env::consts::OS.into(),container,bridge_network_suspected,max_wireguard_instances:core::max_wireguard_instances(),notes,egress_binding_kinds:if cfg!(windows){vec![EgressBindingKind::System,EgressBindingKind::SourceAddress]}else{vec![EgressBindingKind::System,EgressBindingKind::Interface,EgressBindingKind::SourceAddress]},source_address_hint:if cfg!(windows){"A source-bound adapter needs a default gateway."}else if cfg!(target_os="linux"){"Linux routes by destination; bind the interface, or add a source policy rule."}else{"A source address does not select a route; use an interface binding or a scoped route."}.into()})
    }
    #[graphql(guard = "AdminGuard")]
    async fn proxy_pools(&self, ctx: &Context<'_>) -> Result<Vec<ProxyPoolGql>> {
        let db = ctx.data::<Database>()?.clone();
        Ok(
            spawn_blocking_db("network.pools", move || db.list_proxy_pools())
                .await?
                .into_iter()
                .map(Into::into)
                .collect(),
        )
    }
    #[graphql(guard = "AdminGuard")]
    async fn network_routes(&self, ctx: &Context<'_>) -> Result<Vec<NetworkRoute>> {
        let db = ctx.data::<Database>()?.clone();
        Ok(
            spawn_blocking_db("network.routes", move || db.list_proxy_routes())
                .await?
                .into_iter()
                .map(|(consumer, p)| {
                    let r = p.route();
                    NetworkRoute {
                        consumer,
                        legs: r.legs.into_iter().map(Into::into).collect(),
                        failover: r.failover.into(),
                    }
                })
                .collect(),
        )
    }
    #[graphql(guard = "AdminGuard")]
    async fn network_flow(&self, ctx: &Context<'_>) -> Result<NetworkFlow> {
        Ok(flow(runtime(ctx)?.as_ref()))
    }
}

#[derive(Default)]
pub struct NetworkingMutation;
#[Object]
impl NetworkingMutation {
    /// Reset an egress quota baseline without clearing lifetime usage.
    #[graphql(guard = "AdminGuard")]
    async fn reset_egress_download_quota_usage(
        &self,
        ctx: &Context<'_>,
        id: u32,
    ) -> Result<EgressInterfaceGql> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let policy = ctx.data::<Arc<weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry>>()?.clone();
        let db = ctx.data::<Database>()?.clone();
        let saved = spawn_blocking_db("network.reset_egress_usage", move || {
            let saved = db
                .list_egress_interfaces()?
                .into_iter()
                .find(|egress| egress.id == id)
                .ok_or(NetworkRefusal::EgressNotFound)?;
            policy.reset_egress_usage(id)?;
            Ok::<_, weaver_server_core::StateError>(saved)
        })
        .await?;
        runtime.network.refresh_health();
        Ok(egress_gql(
            saved,
            &runtime.network.interfaces(),
            &runtime.network,
        ))
    }

    #[graphql(guard = "AdminGuard")]
    async fn create_egress_interface(
        &self,
        ctx: &Context<'_>,
        input: EgressInterfaceInput,
    ) -> Result<EgressInterfaceGql> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let egress = input.egress(1, Default::default())?;
        let saved = spawn_blocking_db("network.create_egress", move || {
            db.create_egress_interface(&egress)
        })
        .await?;
        runtime.reload().await.map_err(async_graphql::Error::new)?;
        Ok(egress_gql(
            saved,
            &runtime.network.interfaces(),
            &runtime.network,
        ))
    }
    #[graphql(guard = "AdminGuard")]
    async fn update_egress_interface(
        &self,
        ctx: &Context<'_>,
        id: u32,
        input: EgressInterfaceInput,
    ) -> Result<EgressInterfaceGql> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let saved_quota = if input.download_quota.is_some() {
            Default::default()
        } else {
            let db = db.clone();
            spawn_blocking_db("network.update_egress.saved_quota", move || {
                db.list_egress_interfaces()
            })
            .await?
            .into_iter()
            .find(|egress| egress.id == id)
            .map(|egress| egress.download_quota)
            .unwrap_or_default()
        };
        let egress = input.egress(id, saved_quota)?;
        let saved = spawn_blocking_db("network.update_egress", move || {
            db.update_egress_interface(&egress)
        })
        .await?;
        runtime.reload().await.map_err(async_graphql::Error::new)?;
        Ok(egress_gql(
            saved,
            &runtime.network.interfaces(),
            &runtime.network,
        ))
    }
    #[graphql(guard = "AdminGuard")]
    async fn delete_egress_interface(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        // Serialize with schedule edits, so an earlier save cannot republish
        // rules for this deleted egress.
        let mut schedules_guard =
            match ctx.data_opt::<weaver_server_core::bandwidth::schedule::SharedSchedules>() {
                Some(schedules) => Some(schedules.write().await),
                None => None,
            };
        {
            let db = db.clone();
            spawn_blocking_db("network.delete_egress", move || {
                db.delete_egress_interface(id)
            })
            .await?;
        }
        // The delete took this egress's rules and speed limits out of what is
        // saved; publish that.
        if let Some(schedules) = schedules_guard.as_mut() {
            **schedules = spawn_blocking_db("network.delete_egress.schedules", move || {
                db.list_schedules()
            })
            .await?;
        }
        drop(schedules_guard);
        runtime.reload().await.map_err(async_graphql::Error::new)?;
        Ok(true)
    }
    #[graphql(guard = "AdminGuard")]
    async fn create_proxy_pool(
        &self,
        ctx: &Context<'_>,
        input: ProxyPoolInput,
    ) -> Result<ProxyPoolGql> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let pool = input.pool(1);
        let saved =
            spawn_blocking_db("network.create_pool", move || db.create_proxy_pool(&pool)).await?;
        runtime.reload().await.map_err(async_graphql::Error::new)?;
        Ok(saved.into())
    }
    #[graphql(guard = "AdminGuard")]
    async fn update_proxy_pool(
        &self,
        ctx: &Context<'_>,
        id: u32,
        input: ProxyPoolInput,
    ) -> Result<ProxyPoolGql> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let pool = input.pool(id);
        let saved =
            spawn_blocking_db("network.update_pool", move || db.update_proxy_pool(&pool)).await?;
        runtime.reload().await.map_err(async_graphql::Error::new)?;
        Ok(saved.into())
    }
    #[graphql(guard = "AdminGuard")]
    async fn delete_proxy_pool(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_db("network.delete_pool", move || db.delete_proxy_pool(id)).await?;
        runtime.reload().await.map_err(async_graphql::Error::new)?;
        Ok(true)
    }
    #[graphql(guard = "AdminGuard")]
    async fn save_network_route(
        &self,
        ctx: &Context<'_>,
        kind: NetworkConsumerKind,
        id: u32,
        input: RouteInput,
    ) -> Result<NetworkRoute> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let route = input.route()?;
        let consumer = kind.consumer(id);
        let policy = core::RoutingPolicy {
            legs: route.legs.clone(),
            failover: route.failover,
            ..Default::default()
        };
        spawn_blocking_db("network.save_route", move || {
            let exists = match consumer {
                core::Consumer::Server(id) => {
                    db.list_servers()?.iter().any(|server| server.id == id)
                }
                core::Consumer::Rss(id) => db.get_rss_feed(id)?.is_some(),
                core::Consumer::Outbound => true,
            };
            if !exists {
                return Err(NetworkRefusal::RouteConsumerGone.into());
            }
            db.save_proxy_routing_policy(consumer, &policy)
        })
        .await?;
        runtime.reload().await.map_err(async_graphql::Error::new)?;
        Ok(NetworkRoute {
            consumer: consumer.key(),
            legs: route.legs.into_iter().map(Into::into).collect(),
            failover: route.failover.into(),
        })
    }
    // Tests every path through a drafted route, with a proxy or a pool on
    // top of it. A pool carries the target through the member it picks;
    // without a target each member is tried on its own. Nothing is dialled
    // outside the route.
    #[graphql(guard = "AdminGuard")]
    async fn test_via(
        &self,
        ctx: &Context<'_>,
        route: RouteInput,
        subject: Option<TestSubjectInput>,
        target: Option<TestTargetInput>,
    ) -> Result<Vec<PathTestResult>> {
        let runtime = runtime(ctx)?;
        let route = route.route()?;
        let subject = match subject {
            None => core::TestSubject::Route,
            Some(TestSubjectInput::Proxy(id)) => core::TestSubject::Proxy(id),
            Some(TestSubjectInput::Pool(id)) => core::TestSubject::Pool(id),
        };
        let target = target.map(|target| (target.host, target.port));
        Ok(test_via(&runtime, &route, subject, target)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }
    #[graphql(
        guard = "AdminGuard",
        deprecation = "Use testVia with a direct route on the egress."
    )]
    async fn test_egress_interface(
        &self,
        ctx: &Context<'_>,
        id: u32,
        proxy_id: Option<u32>,
        host: String,
        port: u16,
    ) -> Result<EgressTestResult> {
        let runtime = runtime(ctx)?;
        let subject = proxy_id.map_or(core::TestSubject::Route, core::TestSubject::Proxy);
        test_via(&runtime, &direct_on(id), subject, Some((host, port)))
            .await?
            .into_iter()
            .next()
            .map(Into::into)
            .ok_or_else(|| "the test tried no path".into())
    }
    #[graphql(
        guard = "AdminGuard",
        deprecation = "Use testVia with a direct route on the egress."
    )]
    async fn test_proxy_pool(
        &self,
        ctx: &Context<'_>,
        id: u32,
        egress_id: u32,
        host: Option<String>,
        port: Option<u16>,
    ) -> Result<Vec<EgressTestResult>> {
        let runtime = runtime(ctx)?;
        let db = ctx.data::<Database>()?.clone();
        let pool = spawn_blocking_db("network.test_pool", move || db.list_proxy_pools())
            .await?
            .into_iter()
            .find(|pool| pool.id == id)
            .ok_or("proxy pool does not exist")?;
        if matches!(
            pool.kind,
            core::ProxyKind::Socks5 | core::ProxyKind::HttpConnect
        ) && (host.is_none() || port.is_none())
        {
            return Err("host and port are required for a SOCKS5 or HTTP CONNECT pool test".into());
        }
        let target = host.zip(port);
        Ok(test_via(
            &runtime,
            &direct_on(egress_id),
            core::TestSubject::Members(id),
            target,
        )
        .await?
        .into_iter()
        .map(Into::into)
        .collect())
    }
}

#[derive(Default)]
pub struct NetworkingSubscription;
#[Subscription]
impl NetworkingSubscription {
    #[graphql(guard = "AdminGuard")]
    async fn network_flow(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl tokio_stream::Stream<Item = NetworkFlow> + use<>> {
        let runtime = runtime(ctx)?;
        Ok(async_stream::stream! {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last: Option<(NetworkFlow, tokio::time::Instant)> = None;
            loop {
                interval.tick().await;
                let next = flow(&runtime);
                let now = tokio::time::Instant::now();
                if let Some((previous, sent_at)) = &last
                    && same_flow(previous, &next)
                    && now.duration_since(*sent_at) < NETWORK_FLOW_HEARTBEAT
                {
                    continue;
                }
                last = Some((next.clone(), now));
                yield next;
            }
        })
    }
}

// The longest a `networkFlow` client waits for a frame while nothing
// changes; it still learns the stream is alive.
const NETWORK_FLOW_HEARTBEAT: Duration = Duration::from_secs(10);

// Whether two flows differ only in when they were sampled.
fn same_flow(a: &NetworkFlow, b: &NetworkFlow) -> bool {
    a.consumers == b.consumers
        && a.legs == b.legs
        && a.pools == b.pools
        && a.egresses == b.egresses
        && a.proxies == b.proxies
        && a.proxy_pools == b.proxy_pools
}
