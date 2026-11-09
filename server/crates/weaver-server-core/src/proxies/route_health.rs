// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Whether each configured route was last seen up, and the routes that went
// down. A route is an egress bound to an interface or a source address, or
// a proxy on an egress. An egress turns as the interface check finds it; a
// proxy turns as its hops' dials go. A route goes down only after it was
// seen up, once per turn: one never seen up, or already down, raises
// nothing. A route the operator disabled, deleted or edited is forgotten,
// and starts again as never seen up.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::mpsc;
use weaver_tunnel::pipe::HopWatcher;

use super::{EgressBinding, EgressHealth, EgressInterface, ProxyFault, ProxyKind, ProxyProfile};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteKind {
    Interface,
    SourceAddress,
    HttpConnect,
    Http3Connect,
    Socks5,
    Ssh,
    WireGuard,
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

// A route that went from up to down. A proxy's route names the egress it
// is dialled from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDown {
    pub egress_id: u32,
    pub kind: RouteKind,
    pub name: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RouteKey {
    Egress(u32),
    Proxy { egress: u32, proxy: u32 },
}

#[derive(Default)]
struct State {
    egresses: HashMap<u32, EgressInterface>,
    profiles: HashMap<u32, ProxyProfile>,
    // Whether each route was last seen up. A route not here was never seen.
    seen: HashMap<RouteKey, bool>,
    listeners: Vec<mpsc::UnboundedSender<RouteDown>>,
}
impl State {
    fn egress(&self, id: u32) -> Option<&EgressInterface> {
        self.egresses.get(&id).filter(|egress| egress.enabled)
    }
    // Record `key` as up, or as down with `down`, and tell the listeners of
    // a route that was up and is now down.
    fn turn(&mut self, key: RouteKey, down: Option<RouteDown>) {
        let was_up = self.seen.insert(key, down.is_none()) == Some(true);
        if let Some(down) = down.filter(|_| was_up) {
            self.listeners
                .retain(|listener| listener.send(down.clone()).is_ok());
        }
    }
}

// The same route as far as its health goes: an edit to anything else, such
// as a rate or a quota, keeps what was seen of it.
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

#[derive(Default)]
pub struct RouteHealth {
    state: Mutex<State>,
}
impl RouteHealth {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }
    // Each route that goes down from here on. Ends once this is dropped.
    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<RouteDown> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.state
            .lock()
            .expect("route health")
            .listeners
            .push(sender);
        receiver
    }
    // Take the configured egresses and proxies, forgetting what was seen
    // of each route the change disabled, deleted or edited.
    pub(super) fn configure(
        &self,
        egresses: &HashMap<u32, EgressInterface>,
        profiles: &HashMap<u32, ProxyProfile>,
    ) {
        let mut state = self.state.lock().expect("route health");
        let State {
            egresses: before_egresses,
            profiles: before_profiles,
            seen,
            ..
        } = &mut *state;
        seen.retain(|key, _| match *key {
            RouteKey::Egress(id) => same_egress(before_egresses.get(&id), egresses.get(&id)),
            RouteKey::Proxy { egress, proxy } => {
                same_egress(before_egresses.get(&egress), egresses.get(&egress))
                    && same_profile(before_profiles.get(&proxy), profiles.get(&proxy))
            }
        });
        state.egresses = egresses.clone();
        state.profiles = profiles.clone();
    }
    // Egress `id` as the interface check found it. An egress on the
    // system's own routing is always up, and one whose health is unknown
    // keeps what was last seen.
    pub(super) fn egress(&self, id: u32, health: &EgressHealth) {
        let mut state = self.state.lock().expect("route health");
        let Some(egress) = state.egress(id) else {
            return;
        };
        let kind = match egress.binding {
            EgressBinding::System => return,
            EgressBinding::Interface { .. } => RouteKind::Interface,
            EgressBinding::SourceAddress { .. } => RouteKind::SourceAddress,
        };
        let down = match health {
            EgressHealth::Up => None,
            EgressHealth::Down(reason) => Some(RouteDown {
                egress_id: id,
                kind,
                name: egress.name.clone(),
                reason: reason.clone(),
            }),
            EgressHealth::Unknown => return,
        };
        state.turn(RouteKey::Egress(id), down);
    }
    // Proxy `proxy` at `revision` on egress `egress` works, or fails with
    // `fault`. A hop built from another revision, or for a proxy or egress
    // no longer enabled, says nothing.
    fn proxy(&self, egress: u32, proxy: u32, revision: u64, fault: Option<&ProxyFault>) {
        let mut state = self.state.lock().expect("route health");
        if state.egress(egress).is_none() {
            return;
        }
        let Some(profile) = state
            .profiles
            .get(&proxy)
            .filter(|profile| profile.enabled && profile.revision == revision)
        else {
            return;
        };
        let down = fault.map(|fault| RouteDown {
            egress_id: egress,
            kind: RouteKind::of_proxy(profile.kind),
            name: profile.name.clone(),
            reason: fault.to_string(),
        });
        state.turn(RouteKey::Proxy { egress, proxy }, down);
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
