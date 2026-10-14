use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
};

use serde::{Deserialize, Serialize};

use super::{NetworkRefusal, ProxyKind, ProxyProfile, RoutingPolicy};

pub const SYSTEM_EGRESS_ID: u32 = 0;
pub const MAX_ROUTE_LEGS: usize = 8;
pub const MAX_LADDER_RUNGS: usize = 8;
pub const WIREGUARD_INSTANCE_BYTES: u64 = 516 * 1024 * 1024;

pub fn wireguard_instance_limit(available_memory: u64) -> usize {
    (available_memory / 4 / WIREGUARD_INSTANCE_BYTES).clamp(1, 8) as usize
}

pub fn max_wireguard_instances() -> usize {
    // Keep the process's admission limit stable across unrelated memory pressure.
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        let memory = crate::runtime::system_probe::detect_memory();
        wireguard_instance_limit(
            memory
                .available_bytes
                .min(memory.cgroup_limit.unwrap_or(u64::MAX)),
        )
    })
}

pub(super) fn wireguard_session_budget() -> std::sync::Arc<tokio::sync::Semaphore> {
    static BUDGET: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    BUDGET
        .get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(max_wireguard_instances())))
        .clone()
}

// Every WireGuard session `routes` need, named by its egress and the hops
// up to and including it. A WireGuard hop stacked on another is its own
// session, apart from the same proxy directly on the egress.
pub fn wireguard_sessions(
    routes: &[Route],
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
) -> HashSet<(u32, Vec<u32>)> {
    let mut sessions = HashSet::new();
    for route in routes {
        for leg in &route.legs {
            if let LegPath::Ladder { rungs, .. } = &leg.path {
                for rung in rungs {
                    let ids = match rung {
                        Rung::Proxy { id } => std::slice::from_ref(id),
                        Rung::Chain { ids } => ids.as_slice(),
                        Rung::Pool { id } => pools
                            .get(id)
                            .filter(|pool| pool.enabled)
                            .map_or(&[][..], |pool| pool.member_ids.as_slice()),
                    };
                    let chained = matches!(rung, Rung::Chain { .. });
                    for (position, id) in ids.iter().enumerate() {
                        if profiles.get(id).is_some_and(|profile| {
                            profile.enabled && profile.kind == ProxyKind::WireGuard
                        }) {
                            let path = if chained {
                                ids[..=position].to_vec()
                            } else {
                                vec![*id]
                            };
                            sessions.insert((leg.egress_id, path));
                        }
                    }
                }
            }
        }
    }
    sessions
}

pub fn validate_instance_budget(
    routes: &[Route],
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
    limit: usize,
) -> Result<(), NetworkRefusal> {
    let required = wireguard_sessions(routes, profiles, pools).len();
    if required > limit {
        return Err(NetworkRefusal::WireGuardInstanceLimit { required, limit });
    }
    Ok(())
}

// A WireGuard server keeps one endpoint per peer key, so one profile may
// run only one session per egress: used directly and also stacked on
// another WireGuard proxy, or stacked on two different ones, its two
// sessions would keep taking the endpoint from each other.
pub fn validate_wireguard_paths(
    routes: &[Route],
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
) -> Result<(), NetworkRefusal> {
    let mut paths: HashMap<(u32, u32), Vec<Vec<u32>>> = HashMap::new();
    for (egress, path) in wireguard_sessions(routes, profiles, pools) {
        let id = *path.last().expect("a session path names its own hop");
        paths.entry((egress, id)).or_default().push(path);
    }
    let mut conflicts: Vec<_> = paths
        .into_iter()
        .filter(|(_, paths)| paths.len() > 1)
        .collect();
    conflicts.sort();
    if let Some(((egress, proxy), mut paths)) = conflicts.into_iter().next() {
        paths.sort();
        return Err(NetworkRefusal::WireGuardPathConflict {
            proxy,
            egress,
            paths,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum EgressBinding {
    System,
    Interface { name: String },
    SourceAddress { address: IpAddr },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EgressInterface {
    pub id: u32,
    pub name: String,
    pub binding: EgressBinding,
    pub enabled: bool,
    pub max_download_speed: u64,
    // Raw BODY bytes this egress may carry, counted across every server
    // routed over it.
    #[serde(default)]
    pub download_quota: crate::servers::ServerDownloadQuotaConfig,
}

impl EgressInterface {
    pub fn system() -> Self {
        Self {
            id: SYSTEM_EGRESS_ID,
            name: "System".into(),
            binding: EgressBinding::System,
            enabled: true,
            max_download_speed: 0,
            download_quota: crate::servers::ServerDownloadQuotaConfig::default(),
        }
    }

    pub fn validate(&self) -> Result<(), NetworkRefusal> {
        if self.name.trim().is_empty() || self.name.chars().count() > 128 {
            return Err(NetworkRefusal::EgressNameLength);
        }
        if let Some(fault) = self.download_quota.fault() {
            return Err(NetworkRefusal::EgressQuota(fault));
        }
        if self.id == SYSTEM_EGRESS_ID {
            if self.binding != EgressBinding::System || self.name != "System" || !self.enabled {
                return Err(NetworkRefusal::EgressSystemImmutable);
            }
        } else if self.binding == EgressBinding::System {
            return Err(NetworkRefusal::EgressSystemBindingReserved);
        }
        match &self.binding {
            EgressBinding::Interface { name } if name.is_empty() || name.contains('\0') => {
                Err(NetworkRefusal::EgressInterfaceNameInvalid)
            }
            EgressBinding::SourceAddress { address }
                if address.is_unspecified() || address.is_multicast() =>
            {
                Err(NetworkRefusal::EgressSourceAddressNotUnicast)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Failover {
    #[default]
    Redistribute,
    Hold,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Rung {
    Proxy { id: u32 },
    Pool { id: u32 },
    Chain { ids: Vec<u32> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LegPath {
    Direct,
    Ladder {
        rungs: Vec<Rung>,
        #[serde(rename = "directFallback")]
        direct_fallback: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteLeg {
    pub egress_id: u32,
    pub weight: u8,
    pub path: LegPath,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    pub legs: Vec<RouteLeg>,
    #[serde(default)]
    pub failover: Failover,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyPool {
    pub id: u32,
    pub name: String,
    pub kind: ProxyKind,
    pub member_ids: Vec<u32>,
    pub enabled: bool,
}

impl ProxyPool {
    pub fn validate(&self, profiles: &HashMap<u32, ProxyProfile>) -> Result<(), NetworkRefusal> {
        if self.name.trim().is_empty() || self.name.chars().count() > 128 {
            return Err(NetworkRefusal::PoolNameLength);
        }
        // A pool may be saved empty and filled later; only a route needs a member.
        if self.member_ids.len() > 16 {
            return Err(NetworkRefusal::PoolMemberCount);
        }
        let mut seen = HashSet::new();
        for id in &self.member_ids {
            if !seen.insert(id) {
                return Err(NetworkRefusal::PoolProxyRepeated);
            }
            let profile = profiles.get(id).ok_or(NetworkRefusal::PoolProxyMissing)?;
            if profile.kind != self.kind {
                return Err(NetworkRefusal::PoolMemberKindMismatch);
            }
        }
        Ok(())
    }
}

impl Route {
    // Old rows, including v1 restores, keep their exact direct-fallback policy.
    pub fn from_legacy(policy: &RoutingPolicy) -> Self {
        let path = if policy.proxy_ids.is_empty() && policy.allow_direct {
            LegPath::Direct
        } else {
            LegPath::Ladder {
                rungs: policy
                    .proxy_ids
                    .iter()
                    .map(|&id| Rung::Proxy { id })
                    .collect(),
                direct_fallback: policy.allow_direct,
            }
        };
        Self {
            legs: vec![RouteLeg {
                egress_id: SYSTEM_EGRESS_ID,
                weight: 100,
                path,
            }],
            failover: Failover::Redistribute,
        }
    }

    // Legacy writes may only round-trip routes without dropping path information.
    pub fn legacy_policy(&self) -> Option<RoutingPolicy> {
        let [leg] = self.legs.as_slice() else {
            return None;
        };
        if leg.egress_id != SYSTEM_EGRESS_ID || leg.weight != 100 {
            return None;
        }
        match &leg.path {
            LegPath::Direct => Some(RoutingPolicy::default()),
            LegPath::Ladder {
                rungs,
                direct_fallback,
            } => Some(RoutingPolicy {
                proxy_ids: rungs
                    .iter()
                    .map(|rung| match rung {
                        Rung::Proxy { id } => Some(*id),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?,
                allow_direct: *direct_fallback,
                ..Default::default()
            }),
        }
    }

    pub fn validate_shape(&self) -> Result<(), NetworkRefusal> {
        if !(1..=MAX_ROUTE_LEGS).contains(&self.legs.len()) {
            return Err(NetworkRefusal::RouteLegCount);
        }
        if self.legs.iter().any(|leg| !(1..=100).contains(&leg.weight)) {
            return Err(NetworkRefusal::RouteLegWeightRange);
        }
        if self
            .legs
            .iter()
            .map(|leg| u16::from(leg.weight))
            .sum::<u16>()
            != 100
        {
            return Err(NetworkRefusal::RouteLegWeightSum);
        }
        for leg in &self.legs {
            if let LegPath::Ladder { rungs, .. } = &leg.path {
                if !(1..=MAX_LADDER_RUNGS).contains(&rungs.len()) {
                    return Err(NetworkRefusal::RouteLadderRungCount);
                }
                for rung in rungs {
                    if let Rung::Chain { ids } = rung
                        && !(2..=3).contains(&ids.len())
                    {
                        return Err(NetworkRefusal::RouteChainLength);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn validate_references(
        &self,
        egresses: &HashMap<u32, EgressInterface>,
        profiles: &HashMap<u32, ProxyProfile>,
        pools: &HashMap<u32, ProxyPool>,
    ) -> Result<(), NetworkRefusal> {
        self.validate_shape()?;
        for leg in &self.legs {
            let egress = egresses
                .get(&leg.egress_id)
                .ok_or(NetworkRefusal::RouteEgressMissing)?;
            if !egress.enabled {
                return Err(NetworkRefusal::RouteEgressDisabled);
            }
            let LegPath::Ladder { rungs, .. } = &leg.path else {
                continue;
            };
            let mut seen = HashSet::new();
            for rung in rungs {
                let ids = match rung {
                    Rung::Proxy { id } => std::slice::from_ref(id),
                    Rung::Pool { id } => {
                        let pool = pools.get(id).ok_or(NetworkRefusal::RoutePoolMissing)?;
                        pool.validate(profiles)?;
                        if pool.member_ids.is_empty() {
                            return Err(NetworkRefusal::RoutePoolEmpty);
                        }
                        &pool.member_ids
                    }
                    Rung::Chain { ids } => ids,
                };
                for (position, id) in ids.iter().enumerate() {
                    let profile = profiles.get(id).ok_or(NetworkRefusal::RouteProxyMissing)?;
                    if !seen.insert(id) {
                        return Err(NetworkRefusal::RouteProxyRepeated);
                    }
                    // Only a WireGuard hop carries the UDP that WireGuard
                    // and HTTP/3 travel over, and only WireGuard can use it.
                    let carried = position > 0
                        && profile.kind == ProxyKind::WireGuard
                        && profiles
                            .get(&ids[position - 1])
                            .is_some_and(|beneath| beneath.kind == ProxyKind::WireGuard);
                    if matches!(rung, Rung::Chain { .. })
                        && position > 0
                        && matches!(profile.kind, ProxyKind::WireGuard | ProxyKind::Http3Connect)
                        && !carried
                    {
                        return Err(NetworkRefusal::RouteChainTransport);
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_allocation(&self) -> Result<(), NetworkRefusal> {
        if self
            .legacy_policy()
            .is_some_and(|p| p.proxy_ids.is_empty() && !p.allow_direct)
        {
            return Ok(());
        }
        self.validate_shape()
    }

    // Integer largest-remainder allocation; position breaks all ties.
    // HOLD parks a down leg's original allocation, including its rounding share.
    pub fn targets(&self, cap: u16, healthy: &[bool]) -> Result<Vec<u16>, String> {
        self.validate_allocation()?;
        if healthy.len() != self.legs.len() {
            return Err("health must describe every route leg".into());
        }
        let eligible = |i: usize| self.failover == Failover::Hold || healthy[i];
        let denominator: u32 = self
            .legs
            .iter()
            .enumerate()
            .filter(|(i, _)| eligible(*i))
            .map(|(_, leg)| u32::from(leg.weight))
            .sum();
        let mut targets = vec![0; self.legs.len()];
        if denominator == 0 {
            return Ok(targets);
        }
        let mut remainders = Vec::with_capacity(self.legs.len());
        for (i, leg) in self.legs.iter().enumerate().filter(|(i, _)| eligible(*i)) {
            let product = u32::from(cap) * u32::from(leg.weight);
            targets[i] = (product / denominator) as u16;
            remainders.push((i, product % denominator));
        }
        remainders.sort_by_key(|&(i, remainder)| (std::cmp::Reverse(remainder), i));
        let assigned: u16 = targets.iter().sum();
        for &(i, _) in remainders.iter().take(usize::from(cap - assigned)) {
            targets[i] += 1;
        }
        for (i, target) in targets.iter_mut().enumerate() {
            if !healthy[i] {
                *target = 0;
            }
        }
        Ok(targets)
    }
}

#[cfg(test)]
#[path = "network_tests.rs"]
mod tests;
