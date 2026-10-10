// Network routing metrics for the Prometheus exporter.
//
// Leg dial outcomes are counted per egress on the dial path with a relaxed
// atomic add. The registry entry for an egress is created once, the first
// time a leg on it dials; every later dial takes the read lock and adds.
// Everything else here is read from the network runtime at scrape time.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use weaver_tunnel::pipe::DialError;

// The proxy-hop, DNS and ladder counters the tunnel engine keeps, re-exported
// so the exporter reads them without depending on the engine itself.
pub use weaver_tunnel::metrics as tunnel;

use super::{EgressHealth, LegHealthState, NetworkRuntime, ProxyKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegDialResult {
    Success,
    Failed,
    Blocked,
    Ignored,
}

impl LegDialResult {
    pub const ALL: [Self; 4] = [Self::Success, Self::Failed, Self::Blocked, Self::Ignored];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Ignored => "ignored",
        }
    }

    // A failure that is no evidence about the path (a capacity refusal or a
    // skipped dial) neither counts toward the leg's cooldown nor is a path
    // failure here.
    pub fn of_error(error: &DialError) -> Self {
        if !error.is_path_evidence() {
            Self::Ignored
        } else if matches!(error, DialError::Fatal(_)) {
            Self::Blocked
        } else {
            Self::Failed
        }
    }
}

#[derive(Default)]
struct EgressSlots {
    dials: [AtomicU64; 4],
    cooldowns: AtomicU64,
}

static EGRESSES: LazyLock<RwLock<HashMap<u32, Arc<EgressSlots>>>> = LazyLock::new(Default::default);

fn egress_slots(egress_id: u32) -> Arc<EgressSlots> {
    if let Some(slots) = EGRESSES
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&egress_id)
    {
        return slots.clone();
    }
    EGRESSES
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .entry(egress_id)
        .or_default()
        .clone()
}

pub(crate) fn record_leg_dial(egress_id: u32, result: LegDialResult) {
    egress_slots(egress_id).dials[result as usize].fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_leg_cooldown(egress_id: u32) {
    egress_slots(egress_id)
        .cooldowns
        .fetch_add(1, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegStateLabel {
    Up,
    Down,
    Probing,
    Blocked,
}

impl LegStateLabel {
    pub const ALL: [Self; 4] = [Self::Up, Self::Down, Self::Probing, Self::Blocked];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
            Self::Probing => "probing",
            Self::Blocked => "blocked",
        }
    }

    pub fn of(health: &LegHealthState) -> Self {
        match health {
            LegHealthState::Up => Self::Up,
            LegHealthState::Down(_) => Self::Down,
            LegHealthState::Probing => Self::Probing,
            LegHealthState::Blocked(_) => Self::Blocked,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EgressHealthLabel {
    Up,
    Down,
    Unknown,
}

impl EgressHealthLabel {
    pub const ALL: [Self; 3] = [Self::Up, Self::Down, Self::Unknown];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
            Self::Unknown => "unknown",
        }
    }

    pub fn of(health: &EgressHealth) -> Self {
        match health {
            EgressHealth::Up => Self::Up,
            EgressHealth::Down(_) => Self::Down,
            EgressHealth::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RungStateLabel {
    Standby,
    Failing,
    Cooldown,
}

impl RungStateLabel {
    pub const ALL: [Self; 3] = [Self::Standby, Self::Failing, Self::Cooldown];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standby => "standby",
            Self::Failing => "failing",
            Self::Cooldown => "cooldown",
        }
    }

    fn of(state: &str) -> Self {
        match state {
            "COOLDOWN" => Self::Cooldown,
            "FAILING" => Self::Failing,
            _ => Self::Standby,
        }
    }
}

pub const PROXY_KINDS: [ProxyKind; 5] = [
    ProxyKind::HttpConnect,
    ProxyKind::Http3Connect,
    ProxyKind::Socks5,
    ProxyKind::Ssh,
    ProxyKind::WireGuard,
];

pub fn proxy_kind_label(kind: ProxyKind) -> &'static str {
    match kind {
        ProxyKind::HttpConnect => "http_connect",
        ProxyKind::Http3Connect => "http3_connect",
        ProxyKind::Socks5 => "socks5",
        ProxyKind::Ssh => "ssh",
        ProxyKind::WireGuard => "wireguard",
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegMetrics {
    // `server:N` or `rss:N`: the operator-configured consumer.
    pub consumer: String,
    pub position: usize,
    pub egress_id: u32,
    pub live: bool,
    pub state: LegStateLabel,
    pub state_since_epoch_ms: u64,
    pub target: u64,
    pub open: u64,
    pub opening: u64,
    pub bytes_per_second: u64,
    // The ladder rung the leg's last connection used, when it used one.
    pub rung: Option<usize>,
    pub rungs: Vec<RungStateLabel>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EgressMetrics {
    pub egress_id: u32,
    pub enabled: bool,
    pub health: EgressHealthLabel,
    pub health_since_epoch_ms: u64,
    pub dials: [(LegDialResult, u64); 4],
    pub cooldowns: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PoolMemberMetrics {
    pub member_id: u32,
    pub open: u64,
    pub opening: u64,
    pub blocked: bool,
    pub warmed: bool,
    pub session_handshake_seconds: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PoolMetrics {
    pub pool_id: u32,
    pub egress_id: u32,
    pub races_won: u64,
    pub races_failed: u64,
    pub members: Vec<PoolMemberMetrics>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NetworkMetricsSnapshot {
    pub legs: Vec<LegMetrics>,
    pub egresses: Vec<EgressMetrics>,
    pub pools: Vec<PoolMetrics>,
    // (kind, enabled, configured profiles) for every kind and both states.
    pub proxies: Vec<(ProxyKind, bool, u64)>,
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

// When each leg and egress state was first seen by a scrape. The state is
// only observed at scrape time, so a change is dated to the scrape that saw
// it, never earlier.
#[derive(Default)]
struct Transitions {
    legs: HashMap<(String, usize), (LegStateLabel, u64)>,
    egresses: HashMap<u32, (EgressHealthLabel, u64)>,
}

static TRANSITIONS: LazyLock<Mutex<Transitions>> = LazyLock::new(Default::default);

fn since<K: Eq + std::hash::Hash, S: PartialEq + Copy>(
    map: &mut HashMap<K, (S, u64)>,
    key: K,
    state: S,
    now: u64,
) -> u64 {
    let entry = map.entry(key).or_insert((state, now));
    if entry.0 != state {
        *entry = (state, now);
    }
    entry.1
}

fn egress_counters(egress_id: u32) -> ([(LegDialResult, u64); 4], u64) {
    let slots = EGRESSES
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&egress_id)
        .cloned();
    let dials = LegDialResult::ALL.map(|result| {
        (
            result,
            slots
                .as_ref()
                .map_or(0, |s| s.dials[result as usize].load(Ordering::Relaxed)),
        )
    });
    let cooldowns = slots
        .as_ref()
        .map_or(0, |s| s.cooldowns.load(Ordering::Relaxed));
    (dials, cooldowns)
}

pub fn snapshot(runtime: &NetworkRuntime) -> NetworkMetricsSnapshot {
    let now = now_epoch_ms();
    let mut legs = Vec::new();
    for route in runtime.live_routes() {
        let consumer = route.consumer.key();
        let allocations = route.weighted.allocations();
        let definitions = route.legs.read().expect("route legs");
        for (position, definition) in definitions.iter().enumerate() {
            let allocation = allocations.iter().find(|a| a.position == position);
            let state = allocation.map_or(LegStateLabel::Up, |a| LegStateLabel::of(&a.health));
            legs.push(LegMetrics {
                consumer: consumer.clone(),
                position,
                egress_id: definition.definition.egress_id,
                live: true,
                state,
                state_since_epoch_ms: 0,
                target: allocation.map_or(0, |a| u64::from(a.target)),
                open: allocation.map_or(0, |a| u64::from(a.open)),
                opening: allocation.map_or(0, |a| u64::from(a.opening)),
                bytes_per_second: allocation.map_or(0, |a| a.bytes_per_second),
                rung: allocation
                    .and_then(|a| a.path.as_ref())
                    .and_then(|p| p.rung),
                rungs: definition
                    .rung_states()
                    .into_iter()
                    .map(RungStateLabel::of)
                    .collect(),
            });
        }
    }
    for dormant in runtime.dormant_legs() {
        legs.push(LegMetrics {
            consumer: dormant.consumer.key(),
            position: dormant.position,
            egress_id: dormant.definition.egress_id,
            live: false,
            state: LegStateLabel::of(&dormant.health),
            state_since_epoch_ms: 0,
            target: 0,
            open: 0,
            opening: 0,
            bytes_per_second: 0,
            rung: None,
            rungs: Vec::new(),
        });
    }
    legs.sort_by(|a, b| (&a.consumer, a.position).cmp(&(&b.consumer, b.position)));

    let (egress_config, profiles, _) = runtime.configuration_snapshot();
    let interfaces = runtime.interfaces();
    let mut egresses: Vec<EgressMetrics> = egress_config
        .iter()
        .map(|egress| {
            let (dials, cooldowns) = egress_counters(egress.id);
            EgressMetrics {
                egress_id: egress.id,
                enabled: egress.enabled,
                health: EgressHealthLabel::of(&interfaces.health(egress, None)),
                health_since_epoch_ms: 0,
                dials,
                cooldowns,
            }
        })
        .collect();
    egresses.sort_by_key(|e| e.egress_id);

    {
        let mut transitions = TRANSITIONS.lock().unwrap_or_else(|e| e.into_inner());
        for leg in &mut legs {
            leg.state_since_epoch_ms = since(
                &mut transitions.legs,
                (leg.consumer.clone(), leg.position),
                leg.state,
                now,
            );
        }
        for egress in &mut egresses {
            egress.health_since_epoch_ms = since(
                &mut transitions.egresses,
                egress.egress_id,
                egress.health,
                now,
            );
        }
        // Forget legs and egresses that are no longer configured, so the map
        // never outgrows the configuration.
        let live_legs: std::collections::HashSet<_> = legs
            .iter()
            .map(|l| (l.consumer.clone(), l.position))
            .collect();
        transitions.legs.retain(|key, _| live_legs.contains(key));
        let live_egresses: std::collections::HashSet<_> =
            egresses.iter().map(|e| e.egress_id).collect();
        transitions
            .egresses
            .retain(|id, _| live_egresses.contains(id));
    }

    let mut pools: Vec<PoolMetrics> = runtime
        .pool_status()
        .into_iter()
        .map(|(pool_id, egress_id, plan, members)| PoolMetrics {
            pool_id,
            egress_id,
            races_won: plan.races_won,
            races_failed: plan.races_failed,
            members: members
                .into_iter()
                .map(|m| PoolMemberMetrics {
                    member_id: m.id,
                    open: u64::from(m.open),
                    opening: u64::from(m.opening),
                    blocked: m.blocked.is_some(),
                    warmed: m.warmed,
                    session_handshake_seconds: m.session_handshake.map(|d| d.as_secs_f64()),
                })
                .collect(),
        })
        .collect();
    pools.sort_by_key(|p| (p.pool_id, p.egress_id));
    for pool in &mut pools {
        pool.members.sort_by_key(|m| m.member_id);
    }

    let mut proxies = Vec::with_capacity(PROXY_KINDS.len() * 2);
    for kind in PROXY_KINDS {
        for enabled in [true, false] {
            let count = profiles
                .iter()
                .filter(|p| p.kind == kind && p.enabled == enabled)
                .count() as u64;
            proxies.push((kind, enabled, count));
        }
    }

    NetworkMetricsSnapshot {
        legs,
        egresses,
        pools,
        proxies,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leg_dials_count_per_egress_and_result() {
        // A private egress id keeps this test independent of any other test
        // that dials through the shared registry.
        let egress = 0xfeed_0001;
        record_leg_dial(egress, LegDialResult::Success);
        record_leg_dial(egress, LegDialResult::Success);
        record_leg_dial(egress, LegDialResult::Blocked);
        record_leg_cooldown(egress);
        let (dials, cooldowns) = egress_counters(egress);
        assert_eq!(
            dials,
            [
                (LegDialResult::Success, 2),
                (LegDialResult::Failed, 0),
                (LegDialResult::Blocked, 1),
                (LegDialResult::Ignored, 0),
            ]
        );
        assert_eq!(cooldowns, 1);
    }

    #[test]
    fn state_changes_are_dated_to_the_scrape_that_saw_them() {
        let mut map = HashMap::new();
        assert_eq!(since(&mut map, 1u32, LegStateLabel::Up, 10), 10);
        assert_eq!(since(&mut map, 1u32, LegStateLabel::Up, 20), 10);
        assert_eq!(since(&mut map, 1u32, LegStateLabel::Down, 30), 30);
        assert_eq!(since(&mut map, 1u32, LegStateLabel::Down, 40), 30);
    }
}
