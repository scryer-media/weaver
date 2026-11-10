// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Whether each configured route was last seen up, and the routes that went
// down or came back. A route is an egress bound to an interface or a source
// address, a proxy on an egress, or a pool on an egress. An egress turns as
// the interface check finds it; a proxy turns as its hops' dials go; a pool
// is up while any of its members is up, and down once every member is down.
//
// A proxy used on an egress only as a pool member tells nothing of its own:
// its pool does. A chain is not a route of its own: each of its proxies
// tells as itself. While an egress is down, nothing on it tells, and once it
// is down every proxy and pool on it starts again as never seen up.
//
// A route goes down only after it was seen up. Once one is told down, no
// other down is told for it until `QUIET_PERIOD` has passed: a down inside
// it waits, and is told when the period ends if the route is still down. A
// route told down is told up when it is seen up again. A route the operator
// disabled, deleted or edited is forgotten, and starts again as never seen.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, mpsc};
use weaver_tunnel::pipe::HopWatcher;

use super::{
    EgressBinding, EgressHealth, EgressInterface, LegPath, ProxyFault, ProxyKind, ProxyPool,
    ProxyProfile, Route, Rung,
};

// How long after a route is told down before another down is told for it.
pub const QUIET_PERIOD: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteKind {
    Interface,
    SourceAddress,
    HttpConnect,
    Http3Connect,
    Socks5,
    Ssh,
    WireGuard,
    Pool,
}
impl RouteKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interface => "interface",
            Self::SourceAddress => "source_address",
            Self::HttpConnect => "http_connect",
            Self::Http3Connect => "http3_connect",
            Self::Socks5 => "socks5",
            Self::Ssh => "ssh",
            Self::WireGuard => "wireguard",
            Self::Pool => "pool",
        }
    }
    fn of_proxy(kind: ProxyKind) -> Self {
        match kind {
            ProxyKind::HttpConnect => Self::HttpConnect,
            ProxyKind::Http3Connect => Self::Http3Connect,
            ProxyKind::Socks5 => Self::Socks5,
            ProxyKind::Ssh => Self::Ssh,
            ProxyKind::WireGuard => Self::WireGuard,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteChange {
    Down,
    Up,
}

// A route that went down or came back. A proxy's or a pool's route names
// the egress it is dialled from. A route that came back has no reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteEvent {
    pub change: RouteChange,
    pub egress_id: u32,
    pub kind: RouteKind,
    pub name: String,
    pub reason: String,
}

// Where route health takes the time from.
pub trait RouteClock: Send + Sync {
    fn now(&self) -> Instant;
}
struct SystemClock;
impl RouteClock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RouteKey {
    Egress(u32),
    Proxy { egress: u32, proxy: u32 },
    Pool { egress: u32, pool: u32 },
}
impl RouteKey {
    fn egress(self) -> u32 {
        match self {
            Self::Egress(egress) | Self::Proxy { egress, .. } | Self::Pool { egress, .. } => egress,
        }
    }
}

#[derive(Default)]
struct Seen {
    // Whether the route was last seen up. None: never seen.
    up: Option<bool>,
    // What was last told of it.
    told: Option<RouteChange>,
    // When it was last told down.
    down_at: Option<Instant>,
    // A down held by the quiet period.
    waiting: Option<RouteEvent>,
}

#[derive(Default)]
struct State {
    egresses: HashMap<u32, EgressInterface>,
    profiles: HashMap<u32, ProxyProfile>,
    pools: HashMap<u32, ProxyPool>,
    // Each egress and proxy used other than as a pool member.
    alone: HashSet<(u32, u32)>,
    // Each egress and enabled pool used on it.
    pooled: HashSet<(u32, u32)>,
    seen: HashMap<RouteKey, Seen>,
    listeners: Vec<mpsc::UnboundedSender<RouteEvent>>,
}

fn tell(listeners: &mut Vec<mpsc::UnboundedSender<RouteEvent>>, event: RouteEvent) {
    listeners.retain(|listener| listener.send(event.clone()).is_ok());
}

impl State {
    fn egress(&self, id: u32) -> Option<&EgressInterface> {
        self.egresses.get(&id).filter(|egress| egress.enabled)
    }
    fn egress_down(&self, id: u32) -> bool {
        self.seen
            .get(&RouteKey::Egress(id))
            .is_some_and(|seen| seen.up == Some(false))
    }
    // Record `key` as `event` found it, and tell what turned. Returns true
    // when a down now waits for the quiet period to end.
    fn turn(&mut self, key: RouteKey, event: RouteEvent, now: Instant) -> bool {
        let up = event.change == RouteChange::Up;
        let seen = self.seen.entry(key).or_default();
        let before = seen.up.replace(up);
        if up {
            seen.waiting = None;
            if seen.told == Some(RouteChange::Down) {
                seen.told = Some(RouteChange::Up);
                tell(&mut self.listeners, event);
            }
            return false;
        }
        if before != Some(true) || seen.told == Some(RouteChange::Down) {
            return false;
        }
        if seen.down_at.is_some_and(|at| now < at + QUIET_PERIOD) {
            seen.waiting = Some(event);
            return true;
        }
        seen.told = Some(RouteChange::Down);
        seen.down_at = Some(now);
        tell(&mut self.listeners, event);
        false
    }
    // Whether proxy `proxy` on egress `egress` tells as itself: anywhere but
    // only inside the pools used there.
    fn tells_alone(&self, egress: u32, proxy: u32) -> bool {
        self.alone.contains(&(egress, proxy))
            || !self.pooled.iter().any(|&(on, pool)| {
                on == egress
                    && self
                        .pools
                        .get(&pool)
                        .is_some_and(|pool| pool.member_ids.contains(&proxy))
            })
    }
    // The enabled members of `pool`.
    fn members<'a>(&'a self, pool: &'a ProxyPool) -> impl Iterator<Item = u32> + 'a {
        pool.member_ids
            .iter()
            .copied()
            .filter(|id| self.profiles.get(id).is_some_and(|profile| profile.enabled))
    }
    // Each pool used on `egress` with `proxy` in it, as its members are now:
    // up while any member is up, down once every member is down.
    fn turn_pools(&mut self, egress: u32, proxy: u32, fault: Option<&str>, now: Instant) -> bool {
        let mut turns = Vec::new();
        for &(on, id) in &self.pooled {
            let Some(pool) = self.pools.get(&id) else {
                continue;
            };
            if on != egress || !pool.member_ids.contains(&proxy) {
                continue;
            }
            let ups: Vec<_> = self
                .members(pool)
                .map(|member| {
                    self.seen
                        .get(&RouteKey::Proxy {
                            egress,
                            proxy: member,
                        })
                        .and_then(|seen| seen.up)
                })
                .collect();
            let change = if ups.contains(&Some(true)) {
                RouteChange::Up
            } else if !ups.is_empty() && ups.iter().all(|up| *up == Some(false)) {
                RouteChange::Down
            } else {
                continue;
            };
            let reason = match change {
                RouteChange::Down => fault.unwrap_or_default().to_string(),
                RouteChange::Up => String::new(),
            };
            turns.push((
                RouteKey::Pool { egress, pool: id },
                RouteEvent {
                    change,
                    egress_id: egress,
                    kind: RouteKind::Pool,
                    name: pool.name.clone(),
                    reason,
                },
            ));
        }
        let mut waits = false;
        for (key, event) in turns {
            waits |= self.turn(key, event, now);
        }
        waits
    }
}

// The same route as far as its health goes: an edit to anything else, such
// as a rate, a quota or a name, keeps what was seen of it.
fn same_egress(before: Option<&EgressInterface>, after: Option<&EgressInterface>) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => {
            before.enabled && after.enabled && before.binding == after.binding
        }
        _ => false,
    }
}
fn same_profile(before: Option<&ProxyProfile>, after: Option<&ProxyProfile>) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => {
            before.enabled && after.enabled && before.revision == after.revision
        }
        _ => false,
    }
}
fn same_pool(before: Option<&ProxyPool>, after: Option<&ProxyPool>) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => {
            before.enabled && after.enabled && before.member_ids == after.member_ids
        }
        _ => false,
    }
}

pub struct RouteHealth {
    state: Mutex<State>,
    clock: Arc<dyn RouteClock>,
    // Woken when a down starts to wait, so the follower can sleep to it.
    rearm: Arc<Notify>,
}
impl RouteHealth {
    pub fn new() -> Arc<Self> {
        Self::with_clock(Arc::new(SystemClock))
    }
    pub fn with_clock(clock: Arc<dyn RouteClock>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::default(),
            clock,
            rearm: Arc::default(),
        })
    }
    // Each route that goes down or comes back from here on. Ends once this
    // is dropped.
    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<RouteEvent> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.state
            .lock()
            .expect("route health")
            .listeners
            .push(sender);
        receiver
    }
    // Woken each time a down starts to wait for the quiet period.
    pub fn rearmed(&self) -> Arc<Notify> {
        self.rearm.clone()
    }
    // When the first waiting down is due, if one waits.
    pub fn due(&self) -> Option<Instant> {
        self.state
            .lock()
            .expect("route health")
            .seen
            .values()
            .filter(|seen| seen.waiting.is_some())
            .filter_map(|seen| seen.down_at)
            .map(|at| at + QUIET_PERIOD)
            .min()
    }
    // Tell each waiting down whose quiet period has ended, if its route is
    // still down.
    pub fn settle(&self) {
        let now = self.clock.now();
        let mut state = self.state.lock().expect("route health");
        let State {
            seen, listeners, ..
        } = &mut *state;
        for seen in seen.values_mut() {
            if !seen.down_at.is_some_and(|at| at + QUIET_PERIOD <= now) {
                continue;
            }
            let Some(event) = seen.waiting.take() else {
                continue;
            };
            if seen.up == Some(false) && seen.told != Some(RouteChange::Down) {
                seen.told = Some(RouteChange::Down);
                seen.down_at = Some(now);
                tell(listeners, event);
            }
        }
    }
    // Take the configured egresses, proxies, pools and the saved routes
    // that use them, forgetting what was seen of each route the change
    // disabled, deleted or edited.
    pub(super) fn configure(
        &self,
        egresses: &HashMap<u32, EgressInterface>,
        profiles: &HashMap<u32, ProxyProfile>,
        pools: &HashMap<u32, ProxyPool>,
        routes: impl IntoIterator<Item = Route>,
    ) {
        let mut alone = HashSet::new();
        let mut pooled = HashSet::new();
        for leg in routes.into_iter().flat_map(|route| route.legs) {
            let LegPath::Ladder { rungs, .. } = leg.path else {
                continue;
            };
            for rung in &rungs {
                match rung {
                    Rung::Proxy { id } => {
                        alone.insert((leg.egress_id, *id));
                    }
                    Rung::Chain { ids } => {
                        alone.extend(ids.iter().map(|id| (leg.egress_id, *id)));
                    }
                    Rung::Pool { id } => {
                        if pools.get(id).is_some_and(|pool| pool.enabled) {
                            pooled.insert((leg.egress_id, *id));
                        }
                    }
                }
            }
        }
        let mut state = self.state.lock().expect("route health");
        let State {
            egresses: before_egresses,
            profiles: before_profiles,
            pools: before_pools,
            seen,
            ..
        } = &mut *state;
        seen.retain(|key, _| {
            let egress = key.egress();
            same_egress(before_egresses.get(&egress), egresses.get(&egress))
                && match *key {
                    RouteKey::Egress(_) => true,
                    RouteKey::Proxy { proxy, .. } => {
                        same_profile(before_profiles.get(&proxy), profiles.get(&proxy))
                    }
                    RouteKey::Pool { pool, .. } => {
                        same_pool(before_pools.get(&pool), pools.get(&pool))
                    }
                }
        });
        state.egresses = egresses.clone();
        state.profiles = profiles.clone();
        state.pools = pools.clone();
        state.alone = alone;
        state.pooled = pooled;
    }
    // Egress `id` as the interface check found it. An egress on the
    // system's own routing is always up, and one whose health is unknown
    // keeps what was last seen. An egress going down takes every proxy and
    // pool on it back to never seen.
    pub(super) fn egress(&self, id: u32, health: &EgressHealth) {
        let now = self.clock.now();
        let mut state = self.state.lock().expect("route health");
        let Some(egress) = state.egress(id) else {
            return;
        };
        let kind = match egress.binding {
            EgressBinding::System => return,
            EgressBinding::Interface { .. } => RouteKind::Interface,
            EgressBinding::SourceAddress { .. } => RouteKind::SourceAddress,
        };
        let (change, reason) = match health {
            EgressHealth::Up => (RouteChange::Up, String::new()),
            EgressHealth::Down(reason) => (RouteChange::Down, reason.clone()),
            EgressHealth::Unknown => return,
        };
        let event = RouteEvent {
            change,
            egress_id: id,
            kind,
            name: egress.name.clone(),
            reason,
        };
        if change == RouteChange::Down && !state.egress_down(id) {
            for (key, seen) in &mut state.seen {
                if !matches!(key, RouteKey::Egress(_)) && key.egress() == id {
                    seen.up = None;
                    seen.waiting = None;
                }
            }
        }
        let waits = state.turn(RouteKey::Egress(id), event, now);
        drop(state);
        if waits {
            self.rearm.notify_one();
        }
    }
    // Proxy `proxy` at `revision` on egress `egress` works, or fails with
    // `fault`. A hop built from another revision, for a proxy or egress no
    // longer enabled, or on an egress that is down, says nothing; nor does a
    // fault that is the egress's own.
    fn proxy(&self, egress: u32, proxy: u32, revision: u64, fault: Option<&ProxyFault>) {
        if matches!(
            fault,
            Some(ProxyFault::NoRouteVia { .. } | ProxyFault::EgressUnusable { .. })
        ) {
            return;
        }
        let now = self.clock.now();
        let mut state = self.state.lock().expect("route health");
        if state.egress(egress).is_none() || state.egress_down(egress) {
            return;
        }
        let Some(profile) = state
            .profiles
            .get(&proxy)
            .filter(|profile| profile.enabled && profile.revision == revision)
        else {
            return;
        };
        let reason = fault.map(ToString::to_string);
        let event = RouteEvent {
            change: match fault {
                Some(_) => RouteChange::Down,
                None => RouteChange::Up,
            },
            egress_id: egress,
            kind: RouteKind::of_proxy(profile.kind),
            name: profile.name.clone(),
            reason: reason.clone().unwrap_or_default(),
        };
        let key = RouteKey::Proxy { egress, proxy };
        let mut waits = false;
        if state.tells_alone(egress, proxy) {
            waits |= state.turn(key, event, now);
        } else {
            state.seen.entry(key).or_default().up = Some(fault.is_none());
        }
        let reason = reason.map(|reason| format!("{}: {reason}", profile_name(&state, proxy)));
        waits |= state.turn_pools(egress, proxy, reason.as_deref(), now);
        drop(state);
        if waits {
            self.rearm.notify_one();
        }
    }
    // What a hop for proxy `proxy` at `revision` on egress `egress` tells.
    pub(super) fn watcher(
        self: &Arc<Self>,
        egress: u32,
        proxy: u32,
        revision: u64,
    ) -> Arc<dyn HopWatcher> {
        Arc::new(ProxyWatcher {
            health: Arc::downgrade(self),
            egress,
            proxy,
            revision,
        })
    }
}

fn profile_name(state: &State, proxy: u32) -> String {
    state
        .profiles
        .get(&proxy)
        .map(|profile| profile.name.clone())
        .unwrap_or_default()
}

struct ProxyWatcher {
    health: Weak<RouteHealth>,
    egress: u32,
    proxy: u32,
    revision: u64,
}
impl HopWatcher for ProxyWatcher {
    fn working(&self) {
        if let Some(health) = self.health.upgrade() {
            health.proxy(self.egress, self.proxy, self.revision, None);
        }
    }
    fn failing(&self, fault: &ProxyFault) {
        if let Some(health) = self.health.upgrade() {
            health.proxy(self.egress, self.proxy, self.revision, Some(fault));
        }
    }
}

#[cfg(test)]
#[path = "route_health_tests.rs"]
mod tests;
