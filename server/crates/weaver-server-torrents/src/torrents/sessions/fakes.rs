// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::proxies::ProxiesPersistenceQueries;

use std::io;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use weaver_torrent::StreamListener;
use weaver_torrent::network::{BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket};

use super::*;
use crate::proxies::{EgressBinding, LegPath, SYSTEM_EGRESS_ID};
use crate::proxies::{RouteLeg, Rung, TorrentScope};
use crate::torrents::net::NoDatagrams;

// A leg that reaches no one: no peer is dialed, nothing listens, no UDP.
pub struct Nowhere;

impl SocketFactory for Nowhere {
    fn connect_tcp<'a>(
        &'a self,
        _: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async { Err(io::Error::from(io::ErrorKind::Unsupported)) })
    }

    fn listen_tcp<'a>(
        &'a self,
        _: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async { Err(io::Error::from(io::ErrorKind::Unsupported)) })
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move { Ok(Arc::new(NoDatagrams::new(addr)) as Arc<dyn DatagramSocket>) })
    }
}

pub fn ladder(proxy: u32) -> LegPath {
    LegPath::Ladder {
        rungs: vec![Rung::Proxy { id: proxy }],
        direct_fallback: false,
    }
}

pub fn pool_ladder(pool: u32) -> LegPath {
    LegPath::Ladder {
        rungs: vec![Rung::Pool { id: pool }],
        direct_fallback: false,
    }
}

// Routes whose legs are ladders that reach no one, unless a leg of another
// kind is given. Each scope maps to a proxy id, so scopes with the same id
// share a route, or to a pool and its members; every leg can be set down,
// one proxy's out of quota, one pool member down, a leg can refuse to open,
// or wait to open until the route answers.
#[derive(Default)]
pub struct FakeRoutes {
    pub proxies: Mutex<BTreeMap<String, u32>>,
    // The scopes routed through a pool: the pool, and its members in order.
    pub pools: Mutex<BTreeMap<String, (u32, Vec<u32>)>>,
    // The pool members that are down.
    pub members_down: Mutex<BTreeSet<u32>>,
    pub down: Mutex<Option<String>>,
    // The routes whose legs are out of quota.
    pub spent: Mutex<BTreeSet<RouteKey>>,
    pub refuse: Mutex<Option<String>>,
    // While true, the route has not answered: a leg asked to open waits.
    pub unanswered: tokio::sync::watch::Sender<bool>,
    // The legs asked to open, answered or not.
    pub asked: tokio::sync::watch::Sender<usize>,
    pub leg: Mutex<Option<(LegKind, Arc<dyn SocketFactory>)>>,
    // The peer limit each leg was opened for, in order.
    pub peers: Mutex<Vec<usize>>,
    pub opened: AtomicUsize,
    // The blocklist a fetch hands back, and the fetches made.
    pub blocklist: Mutex<Option<String>>,
    pub fetched: tokio::sync::watch::Sender<usize>,
    pub blocklist_unanswered: tokio::sync::watch::Sender<bool>,
    // The network whose route gives each leg opened, when one is set: the
    // leg is its route's, and is revoked as the route changes.
    pub network: Mutex<Option<Arc<NetworkRuntime>>>,
    // The egress each scope's route runs on; the system egress when unset.
    pub egress_of: Mutex<BTreeMap<String, u32>>,
    // What each egress allows its sessions, unlimited when unset, and the
    // signals rung as it changes.
    pub egress_rates: Mutex<BTreeMap<u32, crate::bandwidth::ClassRates>>,
    pub egress_policy: tokio::sync::watch::Sender<u64>,
    pub egress_capacity: tokio::sync::watch::Sender<u64>,
    // The legs it opens run no local service discovery, so a session on
    // the host's own sockets sends nothing past loopback.
    pub no_local_discovery: std::sync::atomic::AtomicBool,
    // The legs it opens on the network carry the session's sockets and
    // names through the network's leg, as the network's own routes do.
    pub through_legs: std::sync::atomic::AtomicBool,
    // Rung as the routes take a saved configuration again.
    pub reloads: tokio::sync::watch::Sender<u64>,
    // Called as each leg is asked to open, before it dials.
    pub on_open: OnOpen,
}

pub type OnOpen = Mutex<Option<Box<dyn Fn() + Send + Sync>>>;

// The route of a scope mapped to `proxy`.
pub fn proxy_key(proxy: u32) -> RouteKey {
    RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &ladder(proxy))
}

// The session key of pool member `member` on the system egress, whichever
// pool holds it.
pub fn pool_member_key(member: u32) -> RouteKey {
    RouteKey::member(SYSTEM_EGRESS_ID, &EgressBinding::System, member)
}

pub fn scope_name(scope: TorrentScope) -> String {
    Consumer::Torrent(scope).key()
}

impl FakeRoutes {
    pub fn route(&self, scope: TorrentScope, proxy: u32) {
        self.pools.lock().unwrap().remove(&scope_name(scope));
        self.proxies
            .lock()
            .unwrap()
            .insert(scope_name(scope), proxy);
    }

    // `scope` runs through `pool`, whose members are `members`.
    pub fn route_pool(&self, scope: TorrentScope, pool: u32, members: &[u32]) {
        self.proxies.lock().unwrap().remove(&scope_name(scope));
        self.pools
            .lock()
            .unwrap()
            .insert(scope_name(scope), (pool, members.to_vec()));
    }

    // Pool member `member` is down, or up again.
    pub fn set_member_down(&self, member: u32, down: bool) {
        let mut members = self.members_down.lock().unwrap();
        if down {
            members.insert(member);
        } else {
            members.remove(&member);
        }
    }

    // Why pool member `member` of `pool`, on `key`, carries nothing, when it
    // does not: set down here, or down in the network's pool when a network
    // is set.
    pub fn member_down(&self, pool: u32, member: u32, key: &RouteKey) -> Option<String> {
        self.key_down(key)
            .or_else(|| {
                self.members_down
                    .lock()
                    .unwrap()
                    .contains(&member)
                    .then(|| format!("proxy {member} failed"))
            })
            .or_else(|| {
                let network = self.network.lock().unwrap().clone()?;
                network
                    .pool_members_down(pool, SYSTEM_EGRESS_ID)
                    .remove(&member)
            })
    }

    pub fn set_down(&self, reason: Option<&str>) {
        *self.down.lock().unwrap() = reason.map(str::to_owned);
    }

    // The leg through `proxy` is out of quota.
    pub fn spend(&self, proxy: u32) {
        self.spent.lock().unwrap().insert(proxy_key(proxy));
    }

    // Why the leg of `key` may carry nothing, when it may not.
    pub fn key_down(&self, key: &RouteKey) -> Option<String> {
        self.down.lock().unwrap().clone().or_else(|| {
            self.spent
                .lock()
                .unwrap()
                .contains(key)
                .then(|| "quota reached".to_owned())
        })
    }

    pub fn set_refused(&self, error: Option<&str>) {
        *self.refuse.lock().unwrap() = error.map(str::to_owned);
    }

    // `scope`'s route runs on egress `egress_id`.
    pub fn route_on_egress(&self, scope: TorrentScope, egress_id: u32) {
        self.egress_of
            .lock()
            .unwrap()
            .insert(scope_name(scope), egress_id);
    }

    // Egress `egress_id` allows `rates` from now, and says so.
    pub fn set_egress_rates(&self, egress_id: u32, rates: crate::bandwidth::ClassRates) {
        self.egress_rates.lock().unwrap().insert(egress_id, rates);
        self.egress_policy.send_modify(|revision| *revision += 1);
    }

    pub fn set_leg(&self, kind: LegKind, factory: Arc<dyn SocketFactory>) {
        *self.leg.lock().unwrap() = Some((kind, factory));
    }
}

#[async_trait::async_trait]
impl Routes for FakeRoutes {
    fn plan(&self, scope: TorrentScope) -> RoutePlan {
        let pool = self.pools.lock().unwrap().get(&scope_name(scope)).cloned();
        let proxy = self
            .proxies
            .lock()
            .unwrap()
            .get(&scope_name(scope))
            .copied()
            .unwrap_or(0);
        let (key, members) = match pool {
            None => (proxy_key(proxy), Vec::new()),
            Some((pool, members)) => (
                RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &pool_ladder(pool)),
                members
                    .into_iter()
                    .map(|id| {
                        let key = pool_member_key(id);
                        MemberPlan {
                            id,
                            down: self.member_down(pool, id, &key).map(RouteFailure::leg_down),
                            key,
                            names: Some(ResolvedBy::Proxy),
                        }
                    })
                    .collect(),
            ),
        };
        let down = if members.is_empty() {
            self.key_down(&key)
        } else {
            members
                .iter()
                .all(|member| member.down.is_some())
                .then(|| "every member is down".to_owned())
        };
        let kind = self
            .leg
            .lock()
            .unwrap()
            .as_ref()
            .map_or(LegKind::Ladder, |(kind, _)| kind.clone());
        let egress_id = self
            .egress_of
            .lock()
            .unwrap()
            .get(&scope_name(scope))
            .copied()
            .unwrap_or(SYSTEM_EGRESS_ID);
        RoutePlan {
            scope,
            policy: RoutingPolicy::default(),
            egress_id,
            names: Some(match kind {
                LegKind::Ladder => ResolvedBy::Proxy,
                _ => ResolvedBy::Host,
            }),
            kind,
            down: down.map(RouteFailure::leg_down),
            key,
            members,
            member: None,
        }
    }

    fn leg_down(&self, plan: &RoutePlan) -> Option<RouteFailure> {
        let pool = self
            .pools
            .lock()
            .unwrap()
            .get(&scope_name(plan.scope))
            .map(|(pool, _)| *pool);
        match plan.member.zip(pool) {
            Some((member, pool)) => self.member_down(pool, member, &plan.key),
            None => self.key_down(&plan.key),
        }
        .map(RouteFailure::leg_down)
    }

    async fn open(&self, plan: &RoutePlan, peers: usize) -> Result<OpenLeg, String> {
        if let Some(on_open) = self.on_open.lock().unwrap().as_ref() {
            on_open();
        }
        let mut unanswered = self.unanswered.subscribe();
        self.asked.send_modify(|asked| *asked += 1);
        let _ = unanswered.wait_for(|unanswered| !unanswered).await;
        self.opened.fetch_add(1, Ordering::SeqCst);
        self.peers.lock().unwrap().push(peers);
        if let Some(error) = self.refuse.lock().unwrap().clone() {
            return Err(error);
        }
        let factory = self.leg.lock().unwrap().as_ref().map_or_else(
            || Arc::new(Nowhere) as Arc<dyn SocketFactory>,
            |(_, factory)| Arc::clone(factory),
        );
        let network = self.network.lock().unwrap().clone();
        let leg = network
            .map(|network| match plan.member {
                Some(member) => network.torrent_member_leg(
                    plan.scope,
                    TORRENT_LEG,
                    member,
                    8,
                    Duration::from_secs(30),
                ),
                None => network.torrent_leg(plan.scope, TORRENT_LEG, 8, Duration::from_secs(30)),
            })
            .transpose()?;
        if self.through_legs.load(Ordering::SeqCst)
            && let Some(leg) = leg
        {
            let bridge = match plan.kind {
                LegKind::Ladder => Some(leg.bridge()?),
                _ => None,
            };
            return OpenLeg::on(plan.kind.clone(), leg.dialer(), bridge, Some(leg));
        }
        let names: Arc<dyn Resolver> = Arc::new(weaver_torrent::network::NoNames);
        let http = tracker_client(
            &LegKind::System,
            crate::torrents::net::system_dialer(),
            None,
            None,
        )?;
        Ok(OpenLeg {
            kind: plan.kind.clone(),
            shape: {
                let mut shape = plan.kind.shape(None);
                if self.no_local_discovery.load(Ordering::SeqCst) {
                    shape.own_sockets = false;
                }
                shape
            },
            factory,
            naming: plain_naming(&names, &http),
            names,
            http,
            leg,
            tunnels: None,
        })
    }

    // Through the legs, each scope's torrents name by its route's profile.
    fn scope_names(&self, plan: &RoutePlan) -> Option<Arc<NamesSlot>> {
        if !self.through_legs.load(Ordering::SeqCst) {
            return None;
        }
        let network = self.network.lock().unwrap().clone()?;
        Some(network.torrent_names(plan.scope, plan.egress_id))
    }

    fn join(&self, plan: &RoutePlan, _: usize) {
        if let Some(network) = self.network.lock().unwrap().clone() {
            network
                .start_torrent_route(plan.scope, 8, Duration::from_secs(30))
                .unwrap();
        }
    }

    // As the network's routes hold it, when a network is set.
    fn opening(&self, plan: &RoutePlan) -> Option<Arc<crate::proxies::LegTenancy>> {
        let network = self.network.lock().unwrap().clone()?;
        Some(network.tenant_torrent(
            Consumer::Torrent(plan.scope),
            TORRENT_LEG,
            crate::proxies::SessionOpening,
        ))
    }

    fn egress_rates(&self, egress_id: u32) -> crate::bandwidth::ClassRates {
        self.egress_rates
            .lock()
            .unwrap()
            .get(&egress_id)
            .copied()
            .unwrap_or_default()
    }

    fn egress_limit_changes(&self) -> Option<EgressLimitChanges> {
        Some(EgressLimitChanges::new(
            self.egress_policy.subscribe(),
            self.egress_capacity.subscribe(),
            self.egress_capacity.subscribe(),
        ))
    }

    fn reloads(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.reloads.subscribe())
    }

    async fn fetch_blocklist(&self, url: &reqwest::Url) -> Result<IpRanges, String> {
        let mut unanswered = self.blocklist_unanswered.subscribe();
        self.fetched.send_modify(|fetched| *fetched += 1);
        let _ = unanswered.wait_for(|unanswered| !unanswered).await;
        let list = self
            .blocklist
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| format!("{url} is unreachable"))?;
        IpRanges::create_from_stream(&mut std::io::Cursor::new(list))
            .await
            .map_err(|error| error.to_string())
    }
}

pub fn env(root: &Path) -> SessionEnv {
    SessionEnv {
        root: root.to_path_buf(),
    }
}

// The torrent settings with the host choosing each listen port, so a test
// takes no fixed port.
pub fn any_port() -> TorrentConfig {
    TorrentConfig {
        base_port: 0,
        ..TorrentConfig::default()
    }
}

// A proxy at 127.0.0.1:`port`, as saved under id 1.
pub fn loopback_proxy(port: u16) -> crate::proxies::ProxyProfile {
    crate::proxies::ProxyProfile {
        dns_profile_id: 0,
        id: 1,
        name: "tunnel".into(),
        kind: crate::proxies::ProxyKind::Socks5,
        enabled: true,
        host: "127.0.0.1".into(),
        port,
        dns_servers: vec![],
        tunnel_addresses: vec![],
        peer_public_key: None,
        mtu: 1280,
        keepalive_seconds: None,
        timeout_seconds: 30,
        host_key_fingerprint: None,
        natpmp: false,
        forwarded_port: None,
        revision: 1,
        secrets: crate::proxies::ProxySecrets::default(),
    }
}

// The default scope's route on a network whose one leg is a ladder through
// the proxy of id 1, the legs `routes` opens being that route's.
pub fn on_network(routes: &FakeRoutes) -> (Database, Arc<NetworkRuntime>) {
    let db = Database::open_in_memory().unwrap();
    db.save_proxy_profile(&loopback_proxy(1)).unwrap();
    db.save_proxy_routing_policy(
        Consumer::Torrent(TorrentScope::Default),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: SYSTEM_EGRESS_ID,
                weight: 100,
                path: ladder(1),
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let network = NetworkRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    routes.route(TorrentScope::Default, 1);
    *routes.network.lock().unwrap() = Some(Arc::clone(&network));
    (db, network)
}

// Save the proxy's profile again, at another port, and reload the network:
// the route's leg is rebuilt behind the same proxy id.
pub async fn save_again(db: &Database, network: &NetworkRuntime) {
    let saved = db.list_proxy_profiles().unwrap().remove(0);
    db.save_proxy_profile(&crate::proxies::ProxyProfile {
        port: saved.port + 1,
        revision: saved.revision + 1,
        ..saved
    })
    .unwrap();
    network.reload().await.unwrap();
}

pub struct Quiet(pub SocketAddr);

impl StreamListener for Quiet {
    fn local_addr(&self) -> SocketAddr {
        self.0
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(std::future::pending())
    }
}

// A leg whose sockets reach no one and bind nothing on the host: its
// listener holds the address asked for, so a session on it shows the port
// the settings give without taking that port.
pub struct Unbound;

impl SocketFactory for Unbound {
    fn connect_tcp<'a>(
        &'a self,
        _: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async { Err(io::Error::from(io::ErrorKind::Unsupported)) })
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async move { Ok(Box::new(Quiet(addr)) as Box<dyn StreamListener>) })
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move { Ok(Arc::new(NoDatagrams::new(addr)) as Arc<dyn DatagramSocket>) })
    }
}

// Routes on a loopback leg that listens through `Unbound`.
pub fn unbound_leg(routes: &FakeRoutes) {
    routes.set_leg(
        LegKind::SourceAddress("127.0.0.1".parse().unwrap()),
        Arc::new(Unbound),
    );
}

// A leg's naming that names every torrent as the session does.
pub fn plain_naming(names: &Arc<dyn Resolver>, http: &Arc<dyn HttpClients>) -> Arc<LegNaming> {
    Arc::new(LegNaming::new(
        LegKind::System,
        crate::torrents::net::system_dialer(),
        None,
        Arc::clone(names),
        Arc::clone(http),
    ))
}
