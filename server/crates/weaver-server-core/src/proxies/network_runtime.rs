// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use weaver_nntp::route_dialer::{AddressPlanned, RouteDialer};
use weaver_tunnel::endpoint_names::EndpointNames;
use weaver_tunnel::{
    Http3TunnelProvider, SshTunnelProvider, TunnelProvider, WireGuardTunnelProvider,
    egress::SocketEgress,
    pipe::{
        DialError, DialPath, Dialed, Dialer, DnsChoice, Egress, Fallback, InnerTransport, Names,
        NamesSlot, Naming, Revocable, SessionHop, SessionSockets, SocketBudget, Target,
        TransportHop,
    },
    transport::{TransportKind, TransportProxy},
};

use super::*;
use crate::Database;

#[path = "network_fetch.rs"]
mod fetch;
pub use fetch::FetchAttempt;
#[path = "network_draft.rs"]
mod draft;
pub use draft::DraftNetworkRoute;
#[path = "network_torrent.rs"]
mod torrent;
pub use torrent::{
    EgressLimitChanges, LegTenancy, LegTenant, SessionOpening, SessionTraffic, TorrentLeg, pool_of,
};
#[path = "network_test_via.rs"]
mod test_via;
pub use test_via::{
    DnsProbed, DnsServerProbe, DnsServerTest, HostResolution, LookupError, LookupFailure,
    MAX_TEST_PATHS, P2pTest, PathTest, ResolvedBy, TestSubject, TrustOutcome,
};

// The health reason of an egress whose download quota is used up.
pub const QUOTA_REACHED: &str = "Quota reached";

// How long a listed DNS server is given to accept a connection. The lookup
// as a whole stays inside the dial or resolve that asked for it.
const DNS_SERVER_DIAL_TIMEOUT: Duration = Duration::from_secs(30);

// What a stage resolves when no route's DNS profile is in scope. Every dial
// of a route carries its profile, so nothing reaches this but a mistake, and
// a mistake fails rather than asking the host.
const UNSCOPED_NAMES: &str = "no route's DNS profile is in scope for this lookup";

#[derive(Clone)]
struct Configuration {
    consumers: std::collections::HashSet<String>,
    consumer_labels: Vec<(Consumer, String, u16)>,
    egresses: HashMap<u32, EgressInterface>,
    profiles: HashMap<u32, ProxyProfile>,
    pools: HashMap<u32, ProxyPool>,
    policies: HashMap<String, RoutingPolicy>,
    warnings: HashMap<(String, usize), String>,
    // How the routes share each WireGuard session's bytes.
    socket_ledger: HashMap<SessionPath, TunnelSockets>,
    // The saved DNS server lists and profiles.
    dns: Arc<StoredDns>,
}
impl Configuration {
    fn load(db: &Database) -> Result<Self, String> {
        let mut configuration = Self {
            warnings: HashMap::new(),
            socket_ledger: HashMap::new(),
            consumer_labels: ProxyRuntime::consumer_labels(db).map_err(|e| e.to_string())?,
            consumers: ProxyRuntime::load_consumers(db)
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(Consumer::key)
                .collect(),
            egresses: db
                .list_egress_interfaces()
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|e| (e.id, e))
                .collect(),
            profiles: db
                .list_proxy_profiles()
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|p| (p.id, p))
                .collect(),
            pools: db
                .list_proxy_pools()
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|p| (p.id, p))
                .collect(),
            policies: db
                .list_proxy_routes()
                .map_err(|e| e.to_string())?
                .into_iter()
                .collect(),
            dns: Arc::new(db.load_dns().map_err(|e| e.to_string())?),
        };
        let connections: HashMap<u32, u16> = db
            .list_servers()
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|server| server.active)
            .map(|server| (server.id, server.connections))
            .collect();
        configuration.socket_ledger = configuration.ledger(&connections);
        let mut missing = Vec::new();
        for (consumer, policy) in &configuration.policies {
            // A route whose DNS profile cannot be used resolves nothing, and
            // each of its legs says why.
            let dns = match configuration.dns_choice(policy.dns_profile()) {
                DnsChoice::Refused(reason) => Some(reason),
                _ => None,
            };
            for (position, leg) in policy.legs.iter().enumerate() {
                if !configuration.egresses.contains_key(&leg.egress_id) {
                    missing.push((
                        (consumer.clone(), position),
                        format!("Restored egress {} is missing; this leg is Down until its egress is repaired.", leg.egress_id),
                    ));
                }
                if let Some(reason) = &dns {
                    missing.push((
                        (consumer.clone(), position),
                        format!("The route's DNS cannot be used: {reason}. Nothing this leg dials by name is resolved until it is repaired."),
                    ));
                }
            }
        }
        for (slot, warning) in missing {
            configuration.warn(slot, warning);
        }
        configuration.warn_over_budget();
        Ok(configuration)
    }
    // Profile `id`, refused as a route's save refuses it unless a route may
    // name by it.
    pub(super) fn route_profile(&self, id: u32) -> Result<super::DnsProfile, NetworkRefusal> {
        let profile = self
            .dns
            .profile(id)
            .ok_or(NetworkRefusal::DnsProfileNotFound)?;
        profile.select_as(super::DnsProfileKind::Route)?;
        Ok(profile)
    }
    // The DNS profile `consumer`'s route names.
    fn dns_profile_of(&self, consumer: &str) -> u32 {
        self.policies
            .get(consumer)
            .map_or(super::SYSTEM_DNS_PROFILE_ID, RoutingPolicy::dns_profile)
    }
    // The DNS profile a torrent leg on egress `egress_id` and `path` names
    // its session's own lookups by: that of the first torrent route on it by
    // consumer key, System when none stored is on it.
    fn torrent_leg_profile(&self, egress_id: u32, path: &LegPath) -> u32 {
        self.policies
            .iter()
            .filter(|(key, _)| key.starts_with("torrent:"))
            .filter(|(_, policy)| {
                policy
                    .route()
                    .legs
                    .first()
                    .is_some_and(|leg| leg.egress_id == egress_id && leg.path == *path)
            })
            .min_by(|(a, _), (b, _)| a.cmp(b))
            .map_or(super::SYSTEM_DNS_PROFILE_ID, |(_, policy)| {
                policy.dns_profile()
            })
    }
    // Who answers the names a route on DNS profile `id` dials. A profile
    // that is gone, or whose server list cannot be read, answers nothing.
    fn dns_choice(&self, id: u32) -> DnsChoice {
        self.dns
            .choice(id)
            .unwrap_or_else(|| DnsChoice::Refused(format!("DNS profile {id} does not exist")))
    }
    // Hand route health the routes of every consumer that still exists.
    fn configure_health(&self, health: &RouteHealth) {
        health.configure(
            &self.egresses,
            &self.profiles,
            &self.pools,
            self.policies
                .iter()
                .filter(|(key, _)| self.consumers.contains(*key))
                .map(|(_, policy)| policy.route()),
        );
    }
    // Add `warning` to the leg's, after any it already carries.
    fn warn(&mut self, slot: (String, usize), warning: String) {
        self.warnings
            .entry(slot)
            .and_modify(|held| {
                held.push(' ');
                held.push_str(&warning);
            })
            .or_insert(warning);
    }
    // The WireGuard sessions under each leg of a ladder.
    fn leg_sessions(&self) -> Vec<((String, usize), SessionPath)> {
        let mut sessions = Vec::new();
        for (key, policy) in &self.policies {
            for (position, leg) in policy.route().legs.iter().enumerate() {
                let LegPath::Ladder { rungs, .. } = &leg.path else {
                    continue;
                };
                for rung in rungs {
                    for path in rung_carriers(rung, &self.profiles, &self.pools) {
                        sessions.push(((key.clone(), position), (leg.egress_id, path)));
                    }
                }
            }
        }
        sessions.sort();
        sessions.dedup();
        sessions
    }
    // A leg over a tunnel its usenet servers ask more of than it holds, as
    // a hardware profile with larger usenet buffers or a torrent route added
    // to a full tunnel leaves one, says so with the totals and what usenet
    // is held to.
    fn warn_over_budget(&mut self) {
        let mut over = Vec::new();
        for (slot, path) in self.leg_sessions() {
            let Some(tunnel) = self.socket_ledger.get(&path).filter(|t| t.over_budget()) else {
                continue;
            };
            over.push((slot, tunnel.over_budget_warning()));
        }
        for (slot, warning) in over {
            self.warn(slot, warning);
        }
    }
    fn ledger(&self, connections: &HashMap<u32, u16>) -> HashMap<SessionPath, TunnelSockets> {
        let routes: Vec<_> = self
            .policies
            .iter()
            .filter_map(|(key, policy)| Consumer::from_key(key).map(|c| (c, policy.route())))
            .collect();
        socket_ledger(&routes, &self.profiles, &self.pools, connections)
    }
    // The bytes a WireGuard session holds its sockets to. A session no
    // route counts on, such as one a probe opens, keeps the reserve and
    // divides the rest by the split.
    fn socket_budget(&self, path: &SessionPath) -> SocketBudget {
        self.socket_ledger
            .get(path)
            .cloned()
            .unwrap_or_else(|| {
                let proxy = path.1.last().copied().unwrap_or_default();
                TunnelSockets::new(path.0, proxy, tunnel_mtu(&self.profiles, proxy))
            })
            .budget()
    }
    // The torrent sessions over each WireGuard session, by their route keys:
    // those of every torrent leg whose rungs carry it, keyed as
    // `torrents::sessions::leg_key` and `member_key` key them.
    fn torrent_routes(&self) -> HashMap<SessionPath, Vec<TorrentRouteKey>> {
        let mut routes: HashMap<SessionPath, Vec<TorrentRouteKey>> = HashMap::new();
        for (key, policy) in &self.policies {
            if !matches!(Consumer::from_key(key), Some(Consumer::Torrent(_))) {
                continue;
            }
            for leg in &policy.route().legs {
                let LegPath::Ladder { rungs, .. } = &leg.path else {
                    continue;
                };
                let (egress, binding) = self
                    .egresses
                    .get(&leg.egress_id)
                    .map_or((leg.egress_id, EgressBinding::System), |egress| {
                        (egress.id, egress.binding.clone())
                    });
                for rung in rungs {
                    let pooled = matches!(rung, Rung::Pool { .. });
                    for path in rung_carriers(rung, &self.profiles, &self.pools) {
                        let route = if pooled {
                            let member = *path.last().expect("a session has a proxy");
                            TorrentRouteKey::member(egress, &binding, member)
                        } else {
                            TorrentRouteKey::of(egress, &binding, &leg.path)
                        };
                        let held = routes.entry((leg.egress_id, path)).or_default();
                        if !held.contains(&route) {
                            held.push(route);
                        }
                    }
                }
            }
        }
        routes
    }
    // The health a leg's egress contributes before any dial evidence. A
    // missing egress is Down under the warning recorded for that leg, so
    // the flow names which egress vanished instead of a generic removal.
    //
    // An egress whose download quota is used up is Down too, so routes move
    // their connections to the legs that can still download.
    fn egress_health(
        &self,
        consumer: &str,
        position: usize,
        egress_id: u32,
        snapshot: &InterfaceSnapshot,
        quota_blocked: &HashSet<u32>,
    ) -> EgressHealth {
        match self.egresses.get(&egress_id) {
            Some(egress) => match snapshot.health(egress, None) {
                EgressHealth::Down(reason) => EgressHealth::Down(reason),
                _ if quota_blocked.contains(&egress_id) => {
                    EgressHealth::Down(QUOTA_REACHED.to_owned())
                }
                health => health,
            },
            None => EgressHealth::Down(self.missing_egress_warning(consumer, position, egress_id)),
        }
    }
    fn missing_egress_warning(&self, consumer: &str, position: usize, egress_id: u32) -> String {
        self.warnings
            .get(&(consumer.to_owned(), position))
            .cloned()
            .unwrap_or_else(|| {
                format!(
                    "Egress {egress_id} is missing; this leg is Down until its egress is repaired."
                )
            })
    }
}

// A leg of a configured route whose consumer has not asked for a dialer,
// such as an inactive server. It carries no connections, but the route the
// operator configured and the health of its egress are still facts the
// flow reports.
pub struct DormantLeg {
    pub consumer: Consumer,
    pub position: usize,
    pub definition: RouteLeg,
    pub health: LegHealthState,
}

#[derive(Clone)]
pub struct LiveLeg {
    pub definition: RouteLeg,
    pub warning: Option<String>,
    pub stage: Arc<Revocable>,
    pub address_plan: Option<Arc<AddressPlanned>>,
    ladder: Option<Arc<Fallback>>,
    signature: String,
}
impl LiveLeg {
    // Ride `other`'s stage, which dials the same leg.
    fn share(&mut self, other: &LiveLeg) {
        self.stage = other.stage.clone();
        self.address_plan = other.address_plan.clone();
        self.ladder = other.ladder.clone();
    }
    pub fn rung_states(&self) -> Vec<&'static str> {
        self.ladder
            .as_ref()
            .map_or_else(Vec::new, |ladder| ladder.rung_states())
    }
    // For each rung, the first proxy hop on it known to be failing.
    pub fn failing_hops(&self) -> Vec<Option<weaver_tunnel::pipe::FailingHop>> {
        self.ladder
            .as_ref()
            .map_or_else(Vec::new, |ladder| ladder.failing_hops())
    }
}
pub struct LiveNetworkRoute {
    pub consumer: Consumer,
    pub weighted: Arc<Weighted>,
    pub legs: RwLock<Vec<LiveLeg>>,
    cap: std::sync::atomic::AtomicU16,
    timeout: Duration,
}

type StagedPoolMembers = HashMap<PoolKey, Vec<(u32, Arc<dyn Dialer>)>>;
// The pools running now, by pool and egress.
// A WireGuard session's path, its bytes, and the task that answers its
// asks to give bytes back, ended with the entry.
pub(super) type SessionSocketMap =
    Arc<Mutex<HashMap<String, (SessionPath, Arc<SessionSockets>, GiveBackTask)>>>;
// Running pools by pool and egress.
pub(super) type PoolKey = (u32, u32);
pub(super) type RunningPools = Arc<Mutex<HashMap<PoolKey, Arc<PoolStage>>>>;

// The owner of the slot a proxy's hop looks its own address up with: this
// and the proxy's id. The slot holds the names of the DNS profile the proxy
// selects, never a route's.
const PROXY_NAME_OWNER: &str = "proxy-name:";

fn proxy_name_owner(proxy: u32) -> String {
    format!("{PROXY_NAME_OWNER}{proxy}")
}

// The owner of the slot a torrent leg dials with: this and the leg's path.
const TORRENT_LEG_OWNER: &str = "torrent-leg:";

// The names of one DNS profile on one egress, and the setting they answer
// for. They hold that setting's answers and DNSSEC state.
#[derive(Clone)]
struct NamedEgress {
    choice: DnsChoice,
    binding: EgressBinding,
    names: Names,
}

// The names every route dials with, kept across reloads: a save that leaves
// a profile and its egress alone keeps what was answered, and one that
// changes either puts fresh names in the slots, which every leg and session
// built on them reads at its next dial. Nothing is rebuilt for it.
#[derive(Default)]
pub(super) struct RouteNames {
    // By DNS profile and egress.
    named: HashMap<(u32, u32), NamedEgress>,
    // The slot the legs of a route on an egress dial with, by owner and
    // egress. A route's own legs are owned by the route. A torrent leg is
    // shared by every torrent route on it, whatever its profile, and is
    // owned by its path: it names as the first of those routes by key does.
    // Each torrent route's own lookups name by a slot the route owns.
    slots: HashMap<(String, u32), Arc<NamesSlot>>,
}
pub(super) type SharedRouteNames = Arc<Mutex<RouteNames>>;

impl RouteNames {
    // The names of profile `profile` on `egress` as `config` sets them: the
    // ones held when the setting and the egress's binding are unchanged,
    // otherwise fresh ones, which hold no answer.
    fn names(&mut self, config: &Configuration, profile: u32, egress: &EgressInterface) -> Names {
        let choice = config.dns_choice(profile);
        match self.named.get(&(profile, egress.id)) {
            Some(held) if held.choice == choice && held.binding == egress.binding => {
                held.names.clone()
            }
            _ => {
                let carrier: Arc<dyn Dialer> = Arc::new(NetworkRuntime::socket_egress(
                    egress,
                    DNS_SERVER_DIAL_TIMEOUT,
                ));
                let names = Names::new(choice.clone(), Some(carrier));
                self.named.insert(
                    (profile, egress.id),
                    NamedEgress {
                        choice,
                        binding: egress.binding.clone(),
                        names: names.clone(),
                    },
                );
                names
            }
        }
    }
    // The owner of the slot `consumer`'s leg on `path` dials with.
    fn owner(consumer: Consumer, path: &LegPath) -> String {
        if consumer.shares_legs() {
            format!(
                "{TORRENT_LEG_OWNER}{}",
                serde_json::to_string(path).unwrap_or_default()
            )
        } else {
            consumer.key()
        }
    }
    // Puts in every slot still held the names `config` gives it, and lets go
    // of what nothing holds. Names that did not change stay as they are.
    fn refresh(&mut self, config: &Configuration) {
        self.slots.retain(|_, slot| Arc::strong_count(slot) > 1);
        let slots: Vec<_> = self
            .slots
            .iter()
            .map(|((owner, egress), slot)| (owner.clone(), *egress, slot.clone()))
            .collect();
        let mut used = HashSet::new();
        for (owner, egress, slot) in slots {
            let Some(egress) = config.egresses.get(&egress) else {
                continue;
            };
            let profile = if let Some(path) = owner.strip_prefix(TORRENT_LEG_OWNER) {
                match serde_json::from_str(path) {
                    Ok(path) => config.torrent_leg_profile(egress.id, &path),
                    Err(_) => continue,
                }
            } else if let Some(id) = owner.strip_prefix(PROXY_NAME_OWNER) {
                // A proxy that is gone leaves its slot as it is: nothing
                // builds a hop on it again.
                match id.parse().ok().and_then(|id| config.profiles.get(&id)) {
                    Some(proxy) => proxy.dns_profile_id,
                    None => continue,
                }
            } else {
                config.dns_profile_of(&owner)
            };
            used.insert((profile, egress.id));
            slot.set(self.names(config, profile, egress));
        }
        self.named.retain(|key, _| used.contains(key));
    }
}

type TorrentRouteKey = crate::torrents::sessions::RouteKey;

// Who answers a WireGuard session's asks to give bytes back: the job
// classes, seated once the scheduler is up, and the torrent sessions over
// each WireGuard session as the configuration in force routes them.
#[derive(Default)]
pub(super) struct GiveBacks {
    classes: std::sync::OnceLock<crate::jobs::JobClasses>,
    torrent_routes: Mutex<HashMap<SessionPath, Vec<TorrentRouteKey>>>,
}

impl GiveBacks {
    // Wake the class on `side` of the session at `path`, whose bytes are
    // `sockets`, to give back what it owes there.
    fn ask(&self, path: &SessionPath, sockets: &Arc<SessionSockets>, side: usize) {
        let Some(class) = self
            .classes
            .get()
            .and_then(|classes| classes.get(crate::jobs::Protocol::ALL[side]))
        else {
            return;
        };
        let torrent_routes = self
            .torrent_routes
            .lock()
            .expect("give-back routes")
            .get(path)
            .cloned()
            .unwrap_or_default();
        let owed = Arc::downgrade(sockets);
        let dry = Arc::downgrade(sockets);
        let class_of = weaver_tunnel::pipe::SIDE_CLASSES[side];
        class.give_back(crate::jobs::TunnelGiveBack {
            egress: path.0,
            hops: path.1.clone(),
            torrent_routes,
            owed: crate::jobs::Owed::new(
                move || owed.upgrade().map_or(0, |sockets| sockets.owed(class_of)),
                move || {
                    if let Some(sockets) = dry.upgrade() {
                        sockets.dry(class_of);
                    }
                },
            ),
        });
    }
}

// Waits on a WireGuard session's wakes to give bytes back and hands each to
// the class it names. A wake, not a poll: it runs only when the session
// wakes a side. Aborted when its session's entry goes.
pub(super) struct GiveBackTask(tokio::task::JoinHandle<()>);

impl Drop for GiveBackTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl GiveBackTask {
    fn spawn(
        handle: &tokio::runtime::Handle,
        sockets: &Arc<SessionSockets>,
        path: SessionPath,
        give_backs: Arc<GiveBacks>,
    ) -> Self {
        let asked = sockets.give_back_asked();
        let sockets = Arc::downgrade(sockets);
        Self(handle.spawn(async move {
            let Some(asked) = asked else {
                return;
            };
            loop {
                let notified = asked.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let Some(sockets) = sockets.upgrade() else {
                    return;
                };
                let woken = sockets.woken();
                for (side, woken) in woken.into_iter().enumerate() {
                    if woken {
                        give_backs.ask(&path, &sockets, side);
                    }
                }
                drop(sockets);
                notified.await;
            }
        }))
    }
}

pub struct NetworkRuntime {
    pub egress_controls: Arc<weaver_nntp::transfer::ServerTransferRegistry>,
    // The long-lived download policies. With one, the egress controls are
    // its egress registry, so counters outlive this runtime's rebuilds and
    // every egress quota is enforced.
    quota_policy: Option<Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>>,
    #[cfg(test)]
    fixture_providers: Mutex<HashMap<u32, Arc<dyn TunnelProvider>>>,
    db: Database,
    handle: tokio::runtime::Handle,
    configuration: RwLock<Configuration>,
    sessions: Mutex<HashMap<String, Arc<dyn Dialer>>>,
    // Each WireGuard session's socket shares, by session key. Shared with
    // the staging runtime so a reload re-budgets the sessions it kept.
    session_sockets: SessionSocketMap,
    // Live sessions a probe runtime may use but does not own. A probe of a
    // saved profile shares the session already holding its WireGuard permit
    // instead of competing with it for the budget, and never shuts it down.
    borrowed_sessions: HashMap<String, Arc<dyn Dialer>>,
    // Shared with the torrent legs that hold a pool member, which look a
    // member up again in whichever pool on their egress still has it.
    pools: RunningPools,
    routes: Mutex<HashMap<String, Arc<LiveNetworkRoute>>>,
    interfaces: Arc<InterfaceMonitor>,
    compiling: bool,
    pool_updates: Mutex<StagedPoolMembers>,
    poll: Mutex<Option<tokio::task::JoinHandle<()>>>,
    quota_watch: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // Who answers the WireGuard sessions' asks to give bytes back. Shared
    // with the staging runtime.
    give_backs: Arc<GiveBacks>,
    // What the last health publish was computed from. The periodic tick
    // republishes only when one of these moved; an idle daemon otherwise
    // recomputes nothing.
    health_inputs: Mutex<Option<(InterfaceSnapshot, HashSet<u32>)>>,
    // The torrents sessions run on the legs, each counted on its leg while
    // its tenancy lives.
    torrent_legs: Mutex<Vec<std::sync::Weak<torrent::LegTenancy>>>,
    // What the saved routes' egresses and hops last showed of their health.
    // A probe of a draft has none: its hops are no saved route's.
    route_health: Option<Arc<RouteHealth>>,
    // A probe runtime's: its WireGuard sessions are probes, which ask
    // through a live session that holds their identity instead of bringing
    // up a second tunnel with the same keys.
    probing: bool,
    // Counts the reloads that took a saved configuration, so whoever waits
    // on a route to come up tries it again at once.
    reloads: tokio::sync::watch::Sender<u64>,
    // The names the routes dial with. Shared with the staging runtime, which
    // adds the slots of new legs; a reload's commit refreshes them all.
    route_names: SharedRouteNames,
}

impl NetworkRuntime {
    pub fn new(db: Database, handle: tokio::runtime::Handle) -> Result<Arc<Self>, String> {
        Self::with_quota_policy(db, handle, None)
    }
    pub fn with_quota_policy(
        db: Database,
        handle: tokio::runtime::Handle,
        quota_policy: Option<Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>>,
    ) -> Result<Arc<Self>, String> {
        let configuration = Configuration::load(&db)?;
        let route_health = RouteHealth::new();
        configuration.configure_health(&route_health);
        let interfaces = Arc::new(InterfaceMonitor::start(
            Arc::new(SystemInterfaceSource),
            &handle,
        ));
        let egress_controls = match &quota_policy {
            Some(policy) => {
                policy
                    .reconfigure_egresses(
                        &configuration.egresses.values().cloned().collect::<Vec<_>>(),
                    )
                    .map_err(|e| e.to_string())?;
                policy.egress_transfer_registry()
            }
            None => {
                let controls = Arc::new(weaver_nntp::transfer::ServerTransferRegistry::with_scope(
                    weaver_nntp::transfer::TransferScope::Egress,
                ));
                configure_egress_rates(&controls, &configuration);
                controls
            }
        };
        let quota_changes = quota_policy.as_ref().map(|policy| {
            (
                policy.subscribe_changes(),
                policy.subscribe_capacity_changes(),
            )
        });
        let runtime = Arc::new(Self {
            egress_controls,
            quota_policy,
            #[cfg(test)]
            fixture_providers: Mutex::new(HashMap::new()),
            db,
            handle: handle.clone(),
            configuration: RwLock::new(configuration),
            sessions: Mutex::new(HashMap::new()),
            session_sockets: Arc::default(),
            borrowed_sessions: HashMap::new(),
            pools: Arc::default(),
            routes: Mutex::new(HashMap::new()),
            interfaces,
            compiling: false,
            pool_updates: Mutex::new(HashMap::new()),
            poll: Mutex::new(None),
            quota_watch: Mutex::new(None),
            give_backs: Arc::default(),
            health_inputs: Mutex::new(None),
            torrent_legs: Mutex::default(),
            route_health: Some(route_health),
            probing: false,
            reloads: tokio::sync::watch::Sender::new(0),
            route_names: Arc::default(),
        });
        let weak = Arc::downgrade(&runtime);
        *runtime.poll.lock().expect("network health task") = Some(handle.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let Some(runtime) = weak.upgrade() else {
                    break;
                };
                runtime.refresh_health_if_changed();
                runtime.retire_unused_sessions().await;
            }
        }));
        if let Some((policy_changes, capacity_changes)) = quota_changes {
            let task = handle.spawn(Self::follow_quota_state(
                Arc::downgrade(&runtime),
                policy_changes,
                capacity_changes,
            ));
            *runtime.quota_watch.lock().expect("network quota task") = Some(task);
        }
        Ok(runtime)
    }
    // Seat the job classes that answer the WireGuard sessions' asks to give
    // bytes back. The first seated stays.
    pub fn attach_classes(&self, classes: crate::jobs::JobClasses) {
        let _ = self.give_backs.classes.set(classes);
    }
    // Re-publish leg health whenever an egress runs out of quota or gets
    // quota back, so routes move off it or back onto it straight away.
    async fn follow_quota_state(
        runtime: std::sync::Weak<Self>,
        mut policy_changes: tokio::sync::watch::Receiver<u64>,
        mut capacity_changes: tokio::sync::watch::Receiver<u64>,
    ) {
        let mut published = HashSet::new();
        loop {
            {
                let Some(runtime) = runtime.upgrade() else {
                    break;
                };
                let blocked = runtime.quota_blocked_egresses();
                if blocked != published {
                    runtime.refresh_health();
                    published = blocked;
                }
            }
            tokio::select! {
                changed = policy_changes.changed() => if changed.is_err() { break },
                changed = capacity_changes.changed() => if changed.is_err() { break },
            }
        }
    }
    // Egresses whose download quota turns work away.
    pub fn quota_blocked_egresses(&self) -> HashSet<u32> {
        self.egress_controls
            .snapshots()
            .into_iter()
            .filter(|snapshot| snapshot.quota_blocked)
            .map(|snapshot| snapshot.stable_server_id.0)
            .collect()
    }
    // Whether `consumer`'s route still gives connections to a leg on an
    // egress whose download quota has room. A route that gives none cannot
    // dial anything until a quota resets.
    pub fn has_leg_in_quota(&self, consumer: Consumer) -> bool {
        let quota_blocked = self.quota_blocked_egresses();
        let config = self.configuration.read().expect("network configuration");
        let route = config
            .policies
            .get(&consumer.key())
            .cloned()
            .unwrap_or_default()
            .route();
        let cap = self
            .routes
            .lock()
            .expect("network routes")
            .get(&consumer.key())
            .map(|live| live.cap.load(std::sync::atomic::Ordering::Acquire));
        route_has_leg_in_quota(&route, cap, &quota_blocked)
    }
    // The live download-quota usage of one egress, when this runtime
    // enforces download policies.
    pub fn egress_quota_usage(
        &self,
        egress_id: u32,
    ) -> Option<crate::servers::transfer_policy::ServerDownloadQuotaSnapshot> {
        self.quota_policy.as_ref()?.egress_snapshot(egress_id)
    }
    // An egress's usage in both directions, when this runtime enforces
    // download policies.
    pub fn egress_usage(
        &self,
        egress_id: u32,
    ) -> Option<crate::servers::transfer_policy::EgressQuotaUsage> {
        self.quota_policy.as_ref()?.egress_usage(egress_id)
    }
    pub fn interfaces(&self) -> InterfaceSnapshot {
        self.interfaces.snapshot()
    }
    /// Rings as the host's interfaces change as multicast sees them.
    pub fn interface_changes(&self) -> tokio::sync::watch::Receiver<MulticastInterfaces> {
        self.interfaces.changes()
    }
    // The saved routes' health, which tells of each route that goes down.
    pub fn route_health(&self) -> Option<Arc<RouteHealth>> {
        self.route_health.clone()
    }
    pub fn egress_configuration(&self, id: u32) -> Option<EgressInterface> {
        self.configuration
            .read()
            .expect("network configuration")
            .egresses
            .get(&id)
            .cloned()
    }
    /// Who resolves a destination's name `consumer`'s route dials through
    /// proxy `id` as the last hop, or straight out of its egress with none,
    /// when the proxy is configured.
    pub fn resolved_by(&self, consumer: Consumer, id: Option<u32>) -> Option<ResolvedBy> {
        let config = self.configuration.read().expect("network configuration");
        let exit = match id {
            Some(id) => Some(config.profiles.get(&id)?),
            None => None,
        };
        let profile = config.dns.profile(config.dns_profile_of(&consumer.key()))?;
        profile.resolved_by(
            exit.map(|proxy| proxy.kind),
            exit.is_some_and(|proxy| !proxy.dns_servers.is_empty()),
        )
    }
    /// The DNS profile `consumer`'s route names.
    pub fn dns_profile_of(&self, consumer: Consumer) -> u32 {
        self.configuration
            .read()
            .expect("network configuration")
            .dns_profile_of(&consumer.key())
    }
    /// Egress `id` alone, with no proxy above it: what a direct leg of
    /// `consumer`'s route dials out of with its own sockets, looking names up
    /// the way the route's DNS profile says.
    pub fn egress_dialer(
        &self,
        consumer: Consumer,
        id: u32,
        timeout: Duration,
    ) -> Result<Arc<dyn Dialer>, String> {
        let config = self.configuration.read().expect("network configuration");
        let bottom = Self::bottom(&config, id, timeout)?;
        let names = self.leg_names(&config, consumer, id, &LegPath::Direct, None);
        Ok(Arc::new(Revocable::named(bottom, names)))
    }
    // The slot `consumer`'s leg on egress `egress_id` and `path` dials with:
    // the names of the route's DNS profile there, or of `profile` when one is
    // given. A torrent leg names as its owner route does.
    fn leg_names(
        &self,
        config: &Configuration,
        consumer: Consumer,
        egress_id: u32,
        path: &LegPath,
        profile: Option<u32>,
    ) -> Arc<NamesSlot> {
        let profile = profile.unwrap_or_else(|| {
            if consumer.shares_legs() {
                config.torrent_leg_profile(egress_id, path)
            } else {
                config.dns_profile_of(&consumer.key())
            }
        });
        self.names_slot(
            config,
            RouteNames::owner(consumer, path),
            profile,
            egress_id,
        )
    }
    /// The slot a torrent route's own lookups on egress `egress_id` name
    /// by: its trackers' and web seeds'. It holds the names of the route's
    /// DNS profile there, and a save puts the new ones in it.
    pub fn torrent_names(&self, scope: TorrentScope, egress_id: u32) -> Arc<NamesSlot> {
        let config = self.configuration.read().expect("network configuration");
        let consumer = Consumer::Torrent(scope).key();
        let profile = config.dns_profile_of(&consumer);
        self.names_slot(&config, consumer, profile, egress_id)
    }
    // The slot `owner`'s dials on egress `egress_id` use, holding the names
    // of DNS profile `profile` there.
    fn names_slot(
        &self,
        config: &Configuration,
        owner: String,
        profile: u32,
        egress_id: u32,
    ) -> Arc<NamesSlot> {
        let mut held = self.route_names.lock().expect("route names");
        let Some(egress) = config.egresses.get(&egress_id) else {
            return Arc::new(NamesSlot::new(Names::new(
                DnsChoice::Refused(format!("egress {egress_id} does not exist")),
                None,
            )));
        };
        let names = held.names(config, profile, egress);
        let key = (owner, egress_id);
        match held.slots.get(&key) {
            Some(slot) => {
                // A reload's commit puts the new names in every slot at once.
                if !self.compiling {
                    slot.set(names);
                }
                slot.clone()
            }
            None => {
                let slot = Arc::new(NamesSlot::new(names));
                held.slots.insert(key, slot.clone());
                slot
            }
        }
    }
    pub fn configuration_snapshot(
        &self,
    ) -> (Vec<EgressInterface>, Vec<ProxyProfile>, Vec<ProxyPool>) {
        let config = self.configuration.read().expect("network configuration");
        (
            config.egresses.values().cloned().collect(),
            config.profiles.values().cloned().collect(),
            config.pools.values().cloned().collect(),
        )
    }
    pub fn consumer_snapshot(&self) -> Vec<(Consumer, String, u16, RoutingPolicy)> {
        let config = self.configuration.read().expect("network configuration");
        config
            .consumer_labels
            .iter()
            .map(|(consumer, name, cap)| {
                (
                    *consumer,
                    name.clone(),
                    *cap,
                    config
                        .policies
                        .get(&consumer.key())
                        .cloned()
                        .unwrap_or_default(),
                )
            })
            .collect()
    }
    pub fn live_routes(&self) -> Vec<Arc<LiveNetworkRoute>> {
        self.routes
            .lock()
            .expect("network routes")
            .values()
            .cloned()
            .collect()
    }
    // Legs of every configured route without a live counterpart.
    pub fn dormant_legs(&self) -> Vec<DormantLeg> {
        let snapshot = self.interfaces();
        let quota_blocked = self.quota_blocked_egresses();
        let config = self.configuration.read().expect("network configuration");
        let routes = self.routes.lock().expect("network routes");
        let mut legs = Vec::new();
        for (consumer, _, _) in &config.consumer_labels {
            let key = consumer.key();
            if routes.contains_key(&key) {
                continue;
            }
            let definition = config
                .policies
                .get(&key)
                .cloned()
                .unwrap_or_default()
                .route();
            for (position, leg) in definition.legs.into_iter().enumerate() {
                let health = LegHealthState::of_egress(config.egress_health(
                    &key,
                    position,
                    leg.egress_id,
                    &snapshot,
                    &quota_blocked,
                ));
                legs.push(DormantLeg {
                    consumer: *consumer,
                    position,
                    definition: leg,
                    health,
                });
            }
        }
        legs
    }
    // Each WireGuard path, the bytes its sessions hold now (their overhead
    // included) and their limits, summed: one atomic read each. Two sessions
    // on one path, as an edited profile's old and new while the old drains,
    // are one path's figures, never two series of the same labels.
    pub fn tunnel_bytes(&self) -> Vec<(SessionPath, u64, u64)> {
        let mut paths: std::collections::BTreeMap<SessionPath, (u64, u64)> =
            std::collections::BTreeMap::new();
        for (path, sockets, _) in self
            .session_sockets
            .lock()
            .expect("session sockets")
            .values()
        {
            let limit = sockets.limit();
            if limit == 0 {
                continue;
            }
            let summed = paths.entry(path.clone()).or_default();
            summed.0 += sockets.reserved();
            summed.1 += limit;
        }
        paths
            .into_iter()
            .map(|(path, (reserved, limit))| (path, reserved, limit))
            .collect()
    }
    pub fn pool_status(
        &self,
    ) -> Vec<(
        u32,
        u32,
        weaver_nntp::candidate_plan::PlanSnapshot<u32>,
        Vec<PoolMemberStatus>,
    )> {
        self.pools
            .lock()
            .expect("network pools")
            .iter()
            .map(|(&(pool, egress), stage)| {
                let (plan, members) = stage.snapshot();
                (pool, egress, plan, members)
            })
            .collect()
    }
    // [`refresh_health`](Self::refresh_health) only when the interfaces or
    // the quota-blocked set moved since the last publish. Configuration
    // changes publish through the reload path, which always refreshes.
    pub fn refresh_health_if_changed(&self) {
        let snapshot = self.interfaces();
        let quota_blocked = self.quota_blocked_egresses();
        let unchanged = self
            .health_inputs
            .lock()
            .expect("network health inputs")
            .as_ref()
            .is_some_and(|(last_snapshot, last_blocked)| {
                *last_snapshot == snapshot && *last_blocked == quota_blocked
            });
        if !unchanged {
            self.refresh_health_from(snapshot, quota_blocked);
        }
    }

    // Publish every live leg's egress health, quota included.
    pub fn refresh_health(&self) {
        self.refresh_health_from(self.interfaces(), self.quota_blocked_egresses());
    }

    fn refresh_health_from(&self, snapshot: InterfaceSnapshot, quota_blocked: HashSet<u32>) {
        *self.health_inputs.lock().expect("network health inputs") =
            Some((snapshot.clone(), quota_blocked.clone()));
        let config = self.configuration.read().expect("network configuration");
        // An egress out of quota is not down here: its quota events say so.
        if let Some(health) = &self.route_health {
            for egress in config.egresses.values() {
                health.egress(egress.id, &snapshot.health(egress, None));
            }
        }
        for route in self.routes.lock().expect("network routes").values() {
            for (position, leg) in route.legs.read().expect("route legs").iter().enumerate() {
                let health = config.egress_health(
                    &route.consumer.key(),
                    position,
                    leg.definition.egress_id,
                    &snapshot,
                    &quota_blocked,
                );
                route.weighted.set_egress_health(position, health);
            }
        }
    }
    // Egress `id` as a route dials out of it. It names what it dials by the
    // DNS profile of the route dialling.
    fn bottom(config: &Configuration, id: u32, timeout: Duration) -> Result<Arc<Egress>, String> {
        let egress = config.egresses.get(&id).ok_or("egress does not exist")?;
        Ok(Arc::new(Self::socket_egress(egress, timeout)))
    }
    // The saved egress's sockets, naming what they dial by the DNS profile
    // of the route dialling, and nothing outside one.
    fn socket_egress(egress: &EgressInterface, timeout: Duration) -> Egress {
        let binding = match &egress.binding {
            EgressBinding::System => SocketEgress::System,
            EgressBinding::Interface { name } => SocketEgress::Interface(name.clone()),
            EgressBinding::SourceAddress { address } => SocketEgress::SourceAddress(*address),
        };
        #[expect(
            clippy::disallowed_methods,
            reason = "the runtime builds a route's egress from the saved egress"
        )]
        let socket = Egress::new(egress.id, binding, timeout);
        socket.with_naming(Naming::Refused(UNSCOPED_NAMES.into()))
    }
    // Proxy `id` above `inner`, for a route on DNS profile `dns`. A session
    // it opens names its own endpoint, when it looks one up, by the names of
    // the route dialling it, and from its own tasks by that profile's names,
    // which a reload swaps in place.
    fn hop(
        &self,
        config: &Configuration,
        id: u32,
        inner: Arc<dyn Dialer>,
        bottom: Arc<Egress>,
        prefix: &[u32],
    ) -> Result<Arc<dyn Dialer>, String> {
        let profile = config.profiles.get(&id).ok_or("proxy does not exist")?;
        if !profile.enabled {
            return Ok(Arc::new(Unavailable(format!("proxy {id} is disabled"))));
        }
        // A canonical hop has the profile's timeout, independent of its first
        // consumer, and names what it dials by the route dialling.
        #[expect(
            clippy::disallowed_methods,
            reason = "a hop's egress is the leg's, with the profile's timeout"
        )]
        let socket = Egress::new(bottom.id(), bottom.binding().clone(), profile.timeout());
        let bottom = Arc::new(socket.with_naming(Naming::Refused(UNSCOPED_NAMES.into())));
        let inner: Arc<dyn Dialer> = if prefix.is_empty() {
            bottom.clone()
        } else {
            inner
        };
        let mut ids = prefix.to_vec();
        ids.push(id);
        // A session is rebuilt when any hop up to it changes. Adding a
        // tunnel above it leaves the carrier's configuration and key alone.
        let revisions: Vec<_> = ids
            .iter()
            .map(|id| (*id, config.profiles.get(id).map(|p| p.revision)))
            .collect();
        let key = format!("{}:{:?}:{revisions:?}", bottom.id(), bottom.binding());
        let mut sessions = self.sessions.lock().expect("network sessions");
        if let Some(stage) = sessions
            .get(&key)
            .or_else(|| self.borrowed_sessions.get(&key))
        {
            return Ok(stage.clone());
        }
        let observer = Arc::new(super::runtime::Observer {
            db: self.db.clone(),
            id,
            revision: profile.revision,
        });
        // One session serves every route that rides it. Its own address is
        // looked up with the DNS profile the proxy selects, from a slot of
        // its own that a save swaps in place, so pointing the proxy at
        // another profile or editing that profile rebuilds nothing.
        let names = self.names_slot(
            config,
            proxy_name_owner(id),
            profile.dns_profile_id,
            bottom.id(),
        );
        let mut transport = None;
        let mut resolver = None;
        // The hop tells the saved route's health how its dials go, as one
        // hop of its proxy: the same proxy alone and atop a chain are two.
        let failure = || match &self.route_health {
            Some(health) => weaver_tunnel::pipe::HopFailure::watched(health.watcher(
                bottom.id(),
                prefix,
                id,
                profile.revision,
            )),
            None => Default::default(),
        };
        let stage: Arc<dyn Dialer> = match profile.kind {
            ProxyKind::Socks5 | ProxyKind::HttpConnect => Arc::new(TransportHop::new(
                id,
                TransportProxy {
                    kind: if profile.kind == ProxyKind::Socks5 {
                        TransportKind::Socks5
                    } else {
                        TransportKind::HttpConnect
                    },
                    host: profile.host.clone(),
                    port: profile.port,
                    username: profile.secrets.username.clone(),
                    password: profile.secrets.password.clone(),
                },
                inner,
                profile.timeout(),
                Naming::Refused(UNSCOPED_NAMES.into()),
                names.clone(),
                failure(),
            )),
            _ => {
                let provider: Arc<dyn TunnelProvider> = match profile.kind {
                    ProxyKind::Ssh => {
                        let input = Arc::new(InnerTransport {
                            inner: inner.clone(),
                            proxy: id,
                            connected: Mutex::new(None),
                            names: names.clone(),
                        });
                        transport = Some(input.clone());
                        Arc::new(
                            SshTunnelProvider::new(profile.ssh_spec(), observer)
                                .with_transport(input),
                        )
                    }
                    ProxyKind::WireGuard => {
                        // The carrier's capacity stays at its configured MTU.
                        // The provider sizes a carried tunnel to fit after
                        // resolving its endpoint through that carrier.
                        let spec = profile.wireguard_spec().map_err(|e| e.to_string())?;
                        let provider = WireGuardTunnelProvider::new(spec, observer);
                        let provider = match prefix.last() {
                            None => provider
                                .with_udp_factory(bottom.clone())
                                .with_endpoint_resolver(Arc::new(EndpointNames(
                                    bottom.clone(),
                                    names.clone(),
                                ))),
                            // A WireGuard tunnel rides only on another
                            // WireGuard tunnel.
                            Some(beneath) => {
                                if config.profiles.get(beneath).map(|p| p.kind)
                                    != Some(ProxyKind::WireGuard)
                                {
                                    return Err("WireGuard must be the first hop on an egress or sit directly on another WireGuard hop".into());
                                }
                                let Some(carrier) = inner.clone().carrier_datagrams() else {
                                    return Ok(Arc::new(Unavailable(format!(
                                        "proxy {beneath} cannot carry proxy {id}: {}",
                                        inner.describe()
                                    ))));
                                };
                                // Its endpoint is asked through the tunnel
                                // beneath, with the proxy's own names.
                                provider.with_datagram_transport(Arc::new(
                                    weaver_tunnel::endpoint_names::EndpointDatagrams(
                                        carrier,
                                        names.clone(),
                                    ),
                                ))
                            }
                        };
                        let provider = Arc::new(provider);
                        resolver = Some(provider.clone());
                        provider
                    }
                    ProxyKind::Http3Connect => {
                        if !prefix.is_empty() {
                            return Err("HTTP/3 must be the first hop on an egress".into());
                        }
                        Arc::new(
                            Http3TunnelProvider::new(
                                profile.http3_spec().map_err(|e| e.to_string())?,
                            )
                            .map_err(|e| e.to_string())?
                            .with_udp_factory(bottom.clone())
                            .with_endpoint_resolver(Arc::new(EndpointNames(
                                bottom.clone(),
                                names.clone(),
                            ))),
                        )
                    }
                    _ => unreachable!(),
                };
                #[cfg(test)]
                let provider = self
                    .fixture_providers
                    .lock()
                    .expect("fixture providers")
                    .get(&id)
                    .cloned()
                    .unwrap_or(provider);
                let sockets = if profile.kind == ProxyKind::WireGuard {
                    let path = (bottom.id(), ids.clone());
                    let sockets = Arc::new(SessionSockets::new(
                        config.socket_budget(&path),
                        Arc::new(super::SplitDivider::InForce),
                    ));
                    let task = GiveBackTask::spawn(
                        &self.handle,
                        &sockets,
                        path.clone(),
                        self.give_backs.clone(),
                    );
                    self.session_sockets
                        .lock()
                        .expect("session sockets")
                        .insert(key.clone(), (path, sockets.clone(), task));
                    sockets
                } else {
                    Default::default()
                };
                Arc::new(SessionHop {
                    capacity: if profile.kind == ProxyKind::WireGuard {
                        let capacity =
                            weaver_tunnel::pipe::SessionCapacity::new(wireguard_session_budget());
                        if self.probing {
                            capacity.probing()
                        } else {
                            capacity
                        }
                    } else {
                        Default::default()
                    },
                    sockets,
                    activity: Default::default(),
                    id,
                    provider,
                    inner,
                    transport,
                    path: DialPath {
                        egress: bottom.id(),
                        proxies: prefix.to_vec(),
                        ..Default::default()
                    },
                    endpoint: profile
                        .host
                        .parse()
                        .ok()
                        .map(|ip| std::net::SocketAddr::new(ip, profile.port)),
                    timeout: profile.timeout(),
                    naming: Naming::Refused(UNSCOPED_NAMES.into()),
                    tunnel_dns: profile.tunnel_dns(),
                    resolver,
                    forwarding: profile.port_forwarding(),
                    failure: failure(),
                })
            }
        };
        sessions.insert(key, stage.clone());
        Ok(stage)
    }
    // Pool `id` on `bottom`'s egress, marked as one a consumer dials whole when `whole` is: only such a pool
    // opens its members ahead of a dial.
    fn pool(
        &self,
        config: &Configuration,
        id: u32,
        bottom: Arc<Egress>,
        whole: bool,
    ) -> Result<Arc<dyn Dialer>, String> {
        let pool = config.pools.get(&id).ok_or("proxy pool does not exist")?;
        let key = (id, bottom.id());
        if !pool.enabled {
            if let Some(stage) = self.pools.lock().expect("network pools").get(&key) {
                if self.compiling {
                    self.pool_updates
                        .lock()
                        .expect("staged pools")
                        .insert(key, Vec::new());
                } else {
                    stage.set_members(Vec::new());
                }
            }
            return Ok(Arc::new(Unavailable(format!("pool {id} is disabled"))));
        }
        let members = pool
            .member_ids
            .iter()
            .filter(|id| config.profiles.get(id).is_some_and(|p| p.enabled))
            .map(|id| {
                Ok((
                    *id,
                    self.hop(config, *id, bottom.clone(), bottom.clone(), &[])?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut pools = self.pools.lock().expect("network pools");
        let stage = if let Some(stage) = pools.get(&key) {
            if whole {
                stage.serve_whole();
            }
            if self.compiling {
                self.pool_updates
                    .lock()
                    .expect("staged pools")
                    .insert(key, members);
            } else {
                let revision = stage.revision();
                stage.set_members(members);
                if stage.revision() != revision || stage.unwarmed() {
                    stage.prewarm();
                }
            }
            stage.clone()
        } else {
            let stage = PoolStage::new(id, members, self.handle.clone());
            if whole {
                stage.serve_whole();
            }
            pools.insert(key, stage.clone());
            if !self.compiling {
                stage.prewarm();
            }
            stage
        };
        Ok(stage)
    }
    // Leg `position` of `consumer`'s route, naming what it dials by DNS
    // profile `profile` when one is given, else as its route does.
    fn leg(
        &self,
        config: &Configuration,
        consumer: Consumer,
        position: usize,
        leg: &RouteLeg,
        timeout: Duration,
        profile: Option<u32>,
    ) -> Result<LiveLeg, String> {
        if !config.egresses.contains_key(&leg.egress_id) {
            let warning = config.missing_egress_warning(&consumer.key(), position, leg.egress_id);
            return Ok(LiveLeg {
                definition: leg.clone(),
                warning: Some(warning.clone()),
                stage: Arc::new(Revocable::new(Arc::new(Unavailable(warning)))),
                address_plan: None,
                ladder: None,
                signature: format!("missing:{}", leg.egress_id),
            });
        }
        let bottom = Self::bottom(config, leg.egress_id, timeout)?;
        let names = self.leg_names(config, consumer, leg.egress_id, &leg.path, profile);
        let mut address_plan = None;
        let mut ladder = None;
        let mut versions = Vec::new();
        let mut pool_versions = Vec::new();
        let stage: Arc<dyn Dialer> = match &leg.path {
            LegPath::Direct => {
                if matches!(consumer, Consumer::Server(_)) {
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "a server's direct leg is its route"
                    )]
                    let plan = Arc::new(AddressPlanned::new(
                        format!("{} leg {}", consumer.key(), position),
                        bottom.clone(),
                    ));
                    address_plan = Some(plan.clone());
                    plan
                } else {
                    bottom.clone()
                }
            }
            LegPath::Ladder {
                rungs,
                direct_fallback,
            } => {
                let mut stages = Vec::<Arc<dyn Dialer>>::new();
                for rung in rungs {
                    stages.push(match rung {
                        Rung::Proxy { id } => {
                            versions.push((*id, config.profiles.get(id).map(|p| p.revision)));
                            self.hop(config, *id, bottom.clone(), bottom.clone(), &[])?
                        }
                        Rung::Pool { id } => {
                            let pool = config.pools.get(id).ok_or("proxy pool does not exist")?;
                            // An edit to the pool's members reaches, through
                            // the pool's stage, only the members it touches,
                            // so it keeps the leg: every other member keeps
                            // its sessions and connections, and a block the
                            // leg holds is lifted on reload once a member is
                            // unblocked.
                            pool_versions.push((pool.id, pool.enabled));
                            // A torrent session holds one member of it;
                            // every other consumer dials the pool whole.
                            let whole = !matches!(consumer, Consumer::Torrent(_));
                            self.pool(config, *id, bottom.clone(), whole)?
                        }
                        Rung::Chain { ids } => {
                            let mut stage: Arc<dyn Dialer> = bottom.clone();
                            for (index, id) in ids.iter().enumerate() {
                                versions.push((*id, config.profiles.get(id).map(|p| p.revision)));
                                stage =
                                    self.hop(config, *id, stage, bottom.clone(), &ids[..index])?;
                            }
                            stage
                        }
                    });
                }
                if *direct_fallback {
                    stages.push(bottom.clone());
                }
                let fallback = Arc::new(Fallback::with_rung_cooldown(
                    stages,
                    weaver_nntp::plan_timing::timing().rung_cooldown,
                ));
                ladder = Some(fallback.clone());
                fallback
            }
        };
        let signature = format!(
            "{}:{:?}:{}:{versions:?}:{pool_versions:?}",
            leg.egress_id,
            bottom.binding(),
            serde_json::to_string(&leg.path).map_err(|e| e.to_string())?
        );
        // A DNS change rebuilds no leg: the leg reads its names from the slot
        // at each dial, and a torrent leg is shared whatever its routes'
        // profiles.
        Ok(LiveLeg {
            definition: leg.clone(),
            warning: config.warnings.get(&(consumer.key(), position)).cloned(),
            stage: Arc::new(Revocable::named(stage, names)),
            address_plan,
            ladder,
            signature,
        })
    }
    pub fn route(
        &self,
        consumer: Consumer,
        cap: u16,
        timeout: Duration,
    ) -> Result<Arc<LiveNetworkRoute>, String> {
        let config = self.configuration.read().expect("network configuration");
        let mut routes = self.routes.lock().expect("network routes");
        if !config.consumers.contains(&consumer.key()) {
            return Err("routing consumer no longer exists".into());
        }
        let definition = config
            .policies
            .get(&consumer.key())
            .cloned()
            .unwrap_or_default()
            .route();
        // A route its consumer may not run on gives no path at all.
        consumer
            .validate_route(&definition)
            .map_err(|refusal| refusal.to_string())?;
        if let Some(route) = routes.get(&consumer.key()) {
            route.weighted.reweight(definition, cap)?;
            route.cap.store(cap, std::sync::atomic::Ordering::Release);
            return Ok(route.clone());
        }
        let mut legs = definition
            .legs
            .iter()
            .enumerate()
            .map(|(position, leg)| self.leg(&config, consumer, position, leg, timeout, None))
            .collect::<Result<Vec<_>, _>>()?;
        if consumer.shares_legs() {
            let live: Vec<_> = routes.values().cloned().collect();
            for leg in &mut legs {
                if let Some(shared) = shared_leg(&live, &leg.signature) {
                    leg.share(&shared);
                }
            }
        }
        let weighted = Weighted::new(
            definition,
            legs.iter()
                .map(|leg| leg.stage.clone() as Arc<dyn Dialer>)
                .collect(),
            cap,
            &self.handle,
        )?;
        let snapshot = self.interfaces();
        let quota_blocked = self.quota_blocked_egresses();
        for (position, leg) in legs.iter().enumerate() {
            weighted.set_egress_health(
                position,
                config.egress_health(
                    &consumer.key(),
                    position,
                    leg.definition.egress_id,
                    &snapshot,
                    &quota_blocked,
                ),
            );
        }
        let route = Arc::new(LiveNetworkRoute {
            consumer,
            weighted,
            legs: RwLock::new(legs),
            cap: std::sync::atomic::AtomicU16::new(cap),
            timeout,
        });
        routes.insert(consumer.key(), route.clone());
        Ok(route)
    }
    pub fn nntp_dialer(
        &self,
        id: u32,
        cap: u16,
        timeout: Duration,
    ) -> Result<Arc<RouteDialer>, String> {
        let route = self.route(Consumer::Server(id), cap, timeout)?;
        let plan = route
            .legs
            .read()
            .expect("network route legs")
            .iter()
            .find_map(|leg| leg.address_plan.as_ref().map(|plan| plan.plan.clone()));
        Ok(Arc::new(RouteDialer {
            inner: route.weighted.clone(),
            plan,
            egress_controls: self.egress_controls.clone(),
            runtime: self.handle.clone(),
            server: id,
        }))
    }
    pub async fn reload(&self) -> Result<(), String> {
        let db = self.db.clone();
        let policy = self.quota_policy.clone();
        let next = tokio::task::spawn_blocking(move || {
            let next = Configuration::load(&db)?;
            if let Some(policy) = policy {
                policy
                    .reconfigure_egresses(&next.egresses.values().cloned().collect::<Vec<_>>())
                    .map_err(|e| e.to_string())?;
            }
            Ok::<_, String>(next)
        })
        .await
        .map_err(|e| e.to_string())??;
        self.apply_configuration(next)?;
        self.retire_unused_sessions().await;
        self.refresh_health();
        self.reloads.send_modify(|reloads| *reloads += 1);
        Ok(())
    }
    /// Rings each time a reload takes the saved configuration: a profile,
    /// pool or route saved, enabled or disabled.
    pub fn reloads(&self) -> tokio::sync::watch::Receiver<u64> {
        self.reloads.subscribe()
    }
    /// Whether proxy `profile` carries a usenet server's route as loaded: a
    /// leg of it names the proxy, a chain through it, or a pool it is a
    /// member of.
    pub fn feeds_servers(&self, profile: u32) -> bool {
        let config = self.configuration.read().expect("network configuration");
        let names = |rung: &Rung| match rung {
            Rung::Proxy { id } => *id == profile,
            Rung::Chain { ids } => ids.contains(&profile),
            Rung::Pool { id } => config
                .pools
                .get(id)
                .is_some_and(|pool| pool.member_ids.contains(&profile)),
        };
        config
            .consumer_labels
            .iter()
            .filter(|(consumer, ..)| matches!(consumer, Consumer::Server(_)))
            .filter_map(|(consumer, ..)| config.policies.get(&consumer.key()))
            .any(|policy| {
                policy.route().legs.iter().any(|leg| match &leg.path {
                    LegPath::Direct => false,
                    LegPath::Ladder { rungs, .. } => rungs.iter().any(names),
                })
            })
    }
    fn apply_configuration(&self, next: Configuration) -> Result<(), String> {
        // Route lookups must see configuration and compiled paths from the same reload.
        let mut configuration = self.configuration.write().expect("network configuration");
        let mut staging = Self {
            db: self.db.clone(),
            handle: self.handle.clone(),
            configuration: RwLock::new(next.clone()),
            sessions: Mutex::new(self.sessions.lock().expect("network sessions").clone()),
            session_sockets: self.session_sockets.clone(),
            borrowed_sessions: HashMap::new(),
            pools: Arc::new(Mutex::new(
                self.pools.lock().expect("network pools").clone(),
            )),
            routes: Mutex::new(HashMap::new()),
            interfaces: self.interfaces.clone(),
            compiling: true,
            pool_updates: Mutex::new(HashMap::new()),
            poll: Mutex::new(None),
            quota_watch: Mutex::new(None),
            give_backs: self.give_backs.clone(),
            health_inputs: Mutex::new(None),
            torrent_legs: Mutex::default(),
            route_health: self.route_health.clone(),
            probing: self.probing,
            reloads: tokio::sync::watch::Sender::new(0),
            route_names: self.route_names.clone(),
            egress_controls: self.egress_controls.clone(),
            quota_policy: None,
            #[cfg(test)]
            fixture_providers: Mutex::new(
                self.fixture_providers
                    .lock()
                    .expect("fixture providers")
                    .clone(),
            ),
        };
        let mut updates = Vec::new();
        let mut removed = Vec::new();
        let mut routes = self.live_routes();
        routes.sort_by_key(|route| route.consumer.key());
        // Legs a sharing consumer took up this reload, by signature.
        let mut shared: HashMap<String, LiveLeg> = HashMap::new();
        for route in routes {
            let definition = next
                .policies
                .get(&route.consumer.key())
                .cloned()
                .unwrap_or_default()
                .route();
            if !next.consumers.contains(&route.consumer.key())
                || route.consumer.validate_route(&definition).is_err()
            {
                removed.push(route);
                continue;
            }
            let mut fresh = definition
                .legs
                .iter()
                .enumerate()
                .map(|(position, leg)| {
                    staging.leg(&next, route.consumer, position, leg, route.timeout, None)
                })
                .collect::<Result<Vec<_>, _>>()?;
            {
                let old = route.legs.read().expect("route legs");
                for (position, new) in fresh.iter_mut().enumerate() {
                    if let Some(prior) = old
                        .get(position)
                        .filter(|prior| prior.signature == new.signature)
                    {
                        new.share(prior);
                    }
                }
            }
            if route.consumer.shares_legs() {
                for new in &mut fresh {
                    match shared.get(&new.signature) {
                        Some(leg) => new.share(leg),
                        None => {
                            shared.insert(new.signature.clone(), new.clone());
                        }
                    }
                }
            }
            let allocation = Weighted::prepare_update(
                definition,
                fresh
                    .iter()
                    .map(|leg| leg.stage.clone() as Arc<dyn Dialer>)
                    .collect(),
                route.cap.load(std::sync::atomic::Ordering::Acquire),
            )?;
            updates.push((route, fresh, allocation));
        }
        // Everything below commits already validated objects; preparation above never
        // changes live membership, allocations, sessions, or revocation state.
        let prior_revisions: HashMap<PoolKey, u64> = self
            .pools
            .lock()
            .expect("network pools")
            .iter()
            .map(|(key, stage)| (*key, stage.revision()))
            .collect();
        *self.sessions.lock().expect("network sessions") =
            std::mem::take(staging.sessions.get_mut().expect("staged sessions"));
        self.rebudget_sessions(&next);
        *self.pools.lock().expect("network pools") =
            std::mem::take(&mut *staging.pools.lock().expect("staged pools"));
        // Pools whose members this reload changed.
        let mut changed_pools = HashSet::new();
        for (key, members) in std::mem::take(staging.pool_updates.get_mut().expect("staged pools"))
        {
            let pool = self.pools.lock().expect("network pools")[&key].clone();
            let revision = pool.revision();
            pool.set_members(members);
            if pool.revision() != revision {
                changed_pools.insert(key);
            }
        }
        let snapshot = self.interfaces();
        let quota_blocked = self.quota_blocked_egresses();
        // A stage goes once no route holds it.
        let mut retired: Vec<Arc<Revocable>> = Vec::new();
        let mut held: Vec<Arc<Revocable>> = Vec::new();
        for (route, fresh, allocation) in updates {
            let mut old = route.legs.write().expect("route legs");
            route.weighted.apply_update(allocation);
            for (position, leg) in fresh.iter().enumerate() {
                route.weighted.set_egress_health(
                    position,
                    next.egress_health(
                        &route.consumer.key(),
                        position,
                        leg.definition.egress_id,
                        &snapshot,
                        &quota_blocked,
                    ),
                );
            }
            // A pool leg is blocked only when every member of its pool was.
            // A member edit kept the leg, so its block goes once the pool
            // holds a member that is not blocked.
            for (position, leg) in fresh.iter().enumerate() {
                let unblocked = pool_keys(&leg.definition).any(|key| {
                    changed_pools.contains(&key)
                        && self
                            .pools
                            .lock()
                            .expect("network pools")
                            .get(&key)
                            .is_some_and(|pool| !pool.all_blocked())
                });
                if unblocked {
                    let stage: Arc<dyn Dialer> = leg.stage.clone();
                    route.weighted.unblock(position, &stage);
                }
            }
            retired.extend(old.iter().map(|leg| leg.stage.clone()));
            held.extend(fresh.iter().map(|leg| leg.stage.clone()));
            *old = fresh;
        }
        for route in removed {
            retired.extend(
                route
                    .legs
                    .read()
                    .expect("route legs")
                    .iter()
                    .map(|leg| leg.stage.clone()),
            );
            self.routes
                .lock()
                .expect("network routes")
                .remove(&route.consumer.key());
        }
        for stage in retired {
            if !held.iter().any(|kept| Arc::ptr_eq(kept, &stage)) {
                stage.revoke();
            }
        }
        // The policy registry already applied rates and quotas on reload.
        if self.quota_policy.is_none() {
            configure_egress_rates(&self.egress_controls, &next);
        }
        // A pool cache owns its member sessions. Drop unused plans before
        // pruning sessions so deleted routes do not retain tunnel buffers.
        let retired_pools = {
            let mut pools = self.pools.lock().expect("network pools");
            let keys: Vec<_> = pools
                .iter()
                .filter(|(_, stage)| Arc::strong_count(stage) == 1)
                .map(|(key, _)| *key)
                .collect();
            keys.into_iter()
                .filter_map(|key| pools.remove(&key))
                .collect::<Vec<_>>()
        };
        for pool in retired_pools {
            pool.set_members(Vec::new());
        }
        if let Some(health) = &self.route_health {
            next.configure_health(health);
        }
        // Every leg kept, and every session on it, names by its route's DNS
        // profile as saved now. A profile, list or route change only swaps
        // the names in the slots; what was answered under a setting that
        // changed goes with the names it was held in.
        self.route_names.lock().expect("route names").refresh(&next);
        *configuration = next;
        // Only new or re-membered pools warm up, and a pool first dialled
        // whole; an unchanged idle member must not handshake again on every
        // reload. A pool only torrent sessions hold members of warms none.
        for (key, pool) in self.pools.lock().expect("network pools").iter() {
            if prior_revisions.get(key) != Some(&pool.revision()) || pool.unwarmed() {
                pool.prewarm();
            }
        }
        Ok(())
    }
    // Hold every live WireGuard session to the bytes `config` gives it,
    // forget the sessions no longer live, and route their asks to give
    // bytes back to the torrent sessions `config` runs over each.
    fn rebudget_sessions(&self, config: &Configuration) {
        *self
            .give_backs
            .torrent_routes
            .lock()
            .expect("give-back routes") = config.torrent_routes();
        let sessions = self.sessions.lock().expect("network sessions");
        let mut shares = self.session_sockets.lock().expect("session sockets");
        shares.retain(|key, _| sessions.contains_key(key));
        for (path, sockets, _) in shares.values() {
            sockets.set_budget(config.socket_budget(path));
        }
    }
    async fn retire_unused_sessions(&self) {
        loop {
            let candidates: Vec<_> = self
                .sessions
                .lock()
                .expect("network sessions")
                .iter()
                .filter(|(_, stage)| Arc::strong_count(stage) == 1)
                .map(|(key, stage)| (key.clone(), stage.clone()))
                .collect();
            let mut removed = false;
            for (key, stage) in candidates {
                if !stage.retire_idle().await {
                    continue;
                }
                let mut sessions = self.sessions.lock().expect("network sessions");
                if sessions
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current, &stage))
                    && Arc::strong_count(&stage) == 2
                {
                    sessions.remove(&key);
                    removed = true;
                }
            }
            if !removed {
                break;
            }
        }
        let sessions = self.sessions.lock().expect("network sessions");
        self.session_sockets
            .lock()
            .expect("session sockets")
            .retain(|key, _| sessions.contains_key(key));
    }
    pub async fn shutdown(&self) {
        if let Some(task) = self.poll.lock().expect("network health task").take() {
            task.abort();
        }
        if let Some(task) = self.quota_watch.lock().expect("network quota task").take() {
            task.abort();
        }
        for route in self.live_routes() {
            for leg in route.legs.read().expect("route legs").iter() {
                leg.stage.revoke();
            }
        }
        let pools: Vec<_> = self
            .pools
            .lock()
            .expect("network pools")
            .values()
            .cloned()
            .collect();
        for pool in pools {
            pool.shutdown().await;
        }
        let sessions: Vec<_> = self
            .sessions
            .lock()
            .expect("network sessions")
            .values()
            .cloned()
            .collect();
        for session in sessions {
            session.shutdown().await;
        }
    }
}
impl Drop for NetworkRuntime {
    fn drop(&mut self) {
        if let Some(task) = self.poll.get_mut().expect("network health task").take() {
            task.abort();
        }
        if let Some(task) = self
            .quota_watch
            .get_mut()
            .expect("network quota task")
            .take()
        {
            task.abort();
        }
    }
}

// Download rates alone, for a runtime without download policies.
// The leg with `signature` that a sharing consumer's live route dials.
fn shared_leg(routes: &[Arc<LiveNetworkRoute>], signature: &str) -> Option<LiveLeg> {
    routes
        .iter()
        .filter(|route| route.consumer.shares_legs())
        .find_map(|route| {
            route
                .legs
                .read()
                .expect("route legs")
                .iter()
                .find(|leg| leg.signature == signature)
                .cloned()
        })
}
// The running pools `leg` dials through, by pool and egress.
fn pool_keys(leg: &RouteLeg) -> impl Iterator<Item = PoolKey> + '_ {
    let rungs = match &leg.path {
        LegPath::Ladder { rungs, .. } => rungs.as_slice(),
        LegPath::Direct => &[],
    };
    rungs.iter().filter_map(move |rung| match rung {
        Rung::Pool { id } => Some((*id, leg.egress_id)),
        _ => None,
    })
}

fn configure_egress_rates(
    controls: &weaver_nntp::transfer::ServerTransferRegistry,
    configuration: &Configuration,
) {
    for egress in configuration.egresses.values() {
        controls.configure(
            weaver_nntp::transfer::StableServerId(egress.id),
            crate::servers::transfer_policy::rate_config(egress.download_bps, None),
        );
    }
}

// A leg counts only while its egress has quota left and the route still
// allocates it connections. HOLD keeps an out-of-quota leg's share parked
// instead of handing it on, so with a small cap the legs that have quota
// can be left with none. Without a live route there is no cap to allocate,
// and any leg with quota counts.
fn route_has_leg_in_quota(route: &Route, cap: Option<u16>, quota_blocked: &HashSet<u32>) -> bool {
    let in_quota: Vec<bool> = route
        .legs
        .iter()
        .map(|leg| !quota_blocked.contains(&leg.egress_id))
        .collect();
    let Some(cap) = cap else {
        return in_quota.contains(&true);
    };
    match route.targets(cap, &in_quota) {
        Ok(targets) => targets
            .iter()
            .zip(&in_quota)
            .any(|(target, in_quota)| *in_quota && *target > 0),
        Err(_) => in_quota.contains(&true),
    }
}

#[cfg(test)]
impl NetworkRuntime {
    pub(crate) fn install_fixture(&self, id: u32, provider: Arc<dyn TunnelProvider>) {
        assert!(self.sessions.lock().unwrap().is_empty());
        self.fixture_providers.lock().unwrap().insert(id, provider);
    }
}

struct Unavailable(String);
#[async_trait::async_trait]
impl Dialer for Unavailable {
    async fn prepare(&self) -> Result<(), DialError> {
        Err(DialError::Skipped(self.0.clone()))
    }
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        Err(DialError::Skipped(self.0.clone()))
    }
    fn budget(&self) -> Duration {
        Duration::ZERO
    }
    fn describe(&self) -> String {
        self.0.clone()
    }
}

#[cfg(test)]
#[path = "network_runtime_tests.rs"]
mod tests;
