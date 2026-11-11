// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod outgoing_utp;
mod pool_members;
mod system_utp;

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use weaver_torrent::StreamListener;
use weaver_torrent::network::{BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket};

use super::*;
use crate::proxies::{RouteLeg, Rung, TorrentScope};
use crate::torrents::identity::IDENTITY;
use crate::torrents::net::NoDatagrams;

// A leg that reaches no one: no peer is dialed, nothing listens, no UDP.
pub(crate) struct Nowhere;

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

fn ladder(proxy: u32) -> LegPath {
    LegPath::Ladder {
        rungs: vec![Rung::Proxy { id: proxy }],
        direct_fallback: false,
    }
}

fn pool_ladder(pool: u32) -> LegPath {
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
pub(crate) struct FakeRoutes {
    pub(crate) proxies: Mutex<BTreeMap<String, u32>>,
    // The scopes routed through a pool: the pool, and its members in order.
    pub(crate) pools: Mutex<BTreeMap<String, (u32, Vec<u32>)>>,
    // The pool members that are down.
    pub(crate) members_down: Mutex<BTreeSet<u32>>,
    pub(crate) down: Mutex<Option<String>>,
    // The routes whose legs are out of quota.
    pub(crate) spent: Mutex<BTreeSet<RouteKey>>,
    pub(crate) refuse: Mutex<Option<String>>,
    // While true, the route has not answered: a leg asked to open waits.
    pub(crate) unanswered: tokio::sync::watch::Sender<bool>,
    // The legs asked to open, answered or not.
    pub(crate) asked: tokio::sync::watch::Sender<usize>,
    pub(crate) leg: Mutex<Option<(LegKind, Arc<dyn SocketFactory>)>>,
    // The peer limit each leg was opened for, in order.
    pub(crate) peers: Mutex<Vec<usize>>,
    pub(crate) opened: AtomicUsize,
    // The blocklist a fetch hands back, and the fetches made.
    pub(crate) blocklist: Mutex<Option<String>>,
    pub(crate) fetched: tokio::sync::watch::Sender<usize>,
    pub(crate) blocklist_unanswered: tokio::sync::watch::Sender<bool>,
    // The network whose route gives each leg opened, when one is set: the
    // leg is its route's, and is revoked as the route changes.
    pub(crate) network: Mutex<Option<Arc<NetworkRuntime>>>,
    // The egress each scope's route runs on; the system egress when unset.
    pub(crate) egress_of: Mutex<BTreeMap<String, u32>>,
    // What each egress allows its sessions, unlimited when unset, and the
    // signals rung as it changes.
    pub(crate) egress_rates: Mutex<BTreeMap<u32, crate::bandwidth::TorrentRates>>,
    pub(crate) egress_policy: tokio::sync::watch::Sender<u64>,
    pub(crate) egress_capacity: tokio::sync::watch::Sender<u64>,
    // The legs it opens run no local service discovery, so a session on
    // the host's own sockets sends nothing past loopback.
    pub(crate) no_local_discovery: std::sync::atomic::AtomicBool,
}

// The route of a scope mapped to `proxy`.
fn proxy_key(proxy: u32) -> RouteKey {
    RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &ladder(proxy))
}

// The session key of pool member `member` on the system egress, whichever
// pool holds it.
pub(crate) fn pool_member_key(member: u32) -> RouteKey {
    RouteKey::member(SYSTEM_EGRESS_ID, &EgressBinding::System, member)
}

fn scope_name(scope: TorrentScope) -> String {
    Consumer::Torrent(scope).key()
}

impl FakeRoutes {
    pub(crate) fn route(&self, scope: TorrentScope, proxy: u32) {
        self.pools.lock().unwrap().remove(&scope_name(scope));
        self.proxies
            .lock()
            .unwrap()
            .insert(scope_name(scope), proxy);
    }

    // `scope` runs through `pool`, whose members are `members`.
    pub(crate) fn route_pool(&self, scope: TorrentScope, pool: u32, members: &[u32]) {
        self.proxies.lock().unwrap().remove(&scope_name(scope));
        self.pools
            .lock()
            .unwrap()
            .insert(scope_name(scope), (pool, members.to_vec()));
    }

    // Pool member `member` is down, or up again.
    pub(crate) fn set_member_down(&self, member: u32, down: bool) {
        let mut members = self.members_down.lock().unwrap();
        if down {
            members.insert(member);
        } else {
            members.remove(&member);
        }
    }

    // Why pool member `member`, on `key`, carries nothing, when it does not.
    fn member_down(&self, member: u32, key: &RouteKey) -> Option<String> {
        self.key_down(key).or_else(|| {
            self.members_down
                .lock()
                .unwrap()
                .contains(&member)
                .then(|| format!("proxy {member} failed"))
        })
    }

    pub(crate) fn set_down(&self, reason: Option<&str>) {
        *self.down.lock().unwrap() = reason.map(str::to_owned);
    }

    // The leg through `proxy` is out of quota.
    pub(crate) fn spend(&self, proxy: u32) {
        self.spent.lock().unwrap().insert(proxy_key(proxy));
    }

    // Why the leg of `key` may carry nothing, when it may not.
    fn key_down(&self, key: &RouteKey) -> Option<String> {
        self.down.lock().unwrap().clone().or_else(|| {
            self.spent
                .lock()
                .unwrap()
                .contains(key)
                .then(|| "quota reached".to_owned())
        })
    }

    pub(crate) fn set_refused(&self, error: Option<&str>) {
        *self.refuse.lock().unwrap() = error.map(str::to_owned);
    }

    // `scope`'s route runs on egress `egress_id`.
    pub(crate) fn route_on_egress(&self, scope: TorrentScope, egress_id: u32) {
        self.egress_of
            .lock()
            .unwrap()
            .insert(scope_name(scope), egress_id);
    }

    // Egress `egress_id` allows `rates` from now, and says so.
    pub(crate) fn set_egress_rates(&self, egress_id: u32, rates: crate::bandwidth::TorrentRates) {
        self.egress_rates.lock().unwrap().insert(egress_id, rates);
        self.egress_policy.send_modify(|revision| *revision += 1);
    }

    pub(crate) fn set_leg(&self, kind: LegKind, factory: Arc<dyn SocketFactory>) {
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
                            down: self.member_down(id, &key).map(RouteFailure::leg_down),
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
        match plan.member {
            Some(member) => self.member_down(member, &plan.key),
            None => self.key_down(&plan.key),
        }
        .map(RouteFailure::leg_down)
    }

    async fn open(&self, plan: &RoutePlan, peers: usize) -> Result<OpenLeg, String> {
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
            names: Arc::new(weaver_torrent::network::NoNames),
            http: tracker_client(
                &LegKind::System,
                crate::torrents::net::system_dialer(),
                None,
            )?,
            leg,
        })
    }

    fn join(&self, plan: &RoutePlan, _: usize) {
        if let Some(network) = self.network.lock().unwrap().clone() {
            network
                .start_torrent_route(plan.scope, 8, Duration::from_secs(30))
                .unwrap();
        }
    }

    fn egress_rates(&self, egress_id: u32) -> crate::bandwidth::TorrentRates {
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

pub(crate) fn env(root: &Path) -> SessionEnv {
    SessionEnv {
        root: root.to_path_buf(),
    }
}

// The torrent settings with the host choosing each listen port, so a test
// takes no fixed port.
fn any_port() -> TorrentConfig {
    TorrentConfig {
        base_port: 0,
        ..TorrentConfig::default()
    }
}

// A proxy at 127.0.0.1:`port`, as saved under id 1.
pub(crate) fn loopback_proxy(port: u16) -> crate::proxies::ProxyProfile {
    crate::proxies::ProxyProfile {
        id: 1,
        name: "tunnel".into(),
        kind: crate::proxies::ProxyKind::Socks5,
        enabled: true,
        host: "127.0.0.1".into(),
        port,
        resolver: crate::proxies::Resolver::Proxy,
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
pub(crate) fn on_network(routes: &FakeRoutes) -> (Database, Arc<NetworkRuntime>) {
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
pub(crate) async fn save_again(db: &Database, network: &NetworkRuntime) {
    let saved = db.list_proxy_profiles().unwrap().remove(0);
    db.save_proxy_profile(&crate::proxies::ProxyProfile {
        port: saved.port + 1,
        revision: saved.revision + 1,
        ..saved
    })
    .unwrap();
    network.reload().await.unwrap();
}

// A profile saved again behind the same proxy, under a route's open
// session, revokes the leg the session opened on. The route's key names the
// proxy, not its settings, so it stands; the session no longer reads open,
// is outdated, and the route opens again on the leg the network gives it
// now.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_whose_leg_is_revoked_opens_again_on_the_new_leg() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network) = on_network(&routes);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(any_port());
    let plan = routes.plan(TorrentScope::Default);
    let stage = |slots: &BTreeMap<RouteKey, Slot>| match slots.get(&plan.key) {
        Some(Slot::Open { leg: Some(leg), .. }) => Arc::clone(&leg.stage),
        _ => panic!("the route is not open on a leg"),
    };

    let old = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    let old_stage = stage(&*sessions.slots.lock().await);
    assert_eq!(sessions.view(&plan.key).await.unwrap().held, None);

    save_again(&db, &network).await;
    assert!(old_stage.is_revoked());
    sessions.revoked().await;
    let view = sessions.view(&plan.key).await.unwrap();
    assert_eq!(view.held.as_ref(), Some(&REVOKED));
    assert_eq!(
        view.held.as_ref().unwrap().reason,
        TorrentRouteReason::LegRevoked
    );
    assert_eq!((view.port, view.listen), (None, None));
    assert!(view.external.is_empty());
    assert_eq!(
        sessions.outdated().await,
        BTreeSet::from([plan.key.clone()])
    );

    sessions.drop_route(&plan.key).await;
    let new = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(!Arc::ptr_eq(&old, &new));
    let new_stage = stage(&*sessions.slots.lock().await);
    assert!(!Arc::ptr_eq(&old_stage, &new_stage));
    assert!(!new_stage.is_revoked());
    assert_eq!(sessions.view(&plan.key).await.unwrap().held, None);
    assert!(sessions.outdated().await.is_empty());
    sessions.stop_all().await;
    network.shutdown().await;
}

// A session whose leg was revoked is not handed out before the follower
// drops it: the route opens again on the leg the network gives it now, and
// the revoked session stops.
#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_session_is_not_handed_out_before_its_route_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network) = on_network(&routes);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(any_port());
    let plan = routes.plan(TorrentScope::Default);
    let old = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();

    save_again(&db, &network).await;
    let new = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(!Arc::ptr_eq(&old, &new));
    old.cancellation_token().cancelled().await;
    assert_eq!(sessions.view(&plan.key).await.unwrap().held, None);
    assert!(sessions.outdated().await.is_empty());
    let running = sessions.running().await;
    assert!(Arc::ptr_eq(&running[&plan.key], &new));
    sessions.stop_all().await;
    network.shutdown().await;
}

#[test]
fn route_keys_follow_the_leg_not_the_scope() {
    let address: IpAddr = "192.0.2.4".parse().unwrap();
    let source = EgressBinding::SourceAddress { address };
    assert_eq!(
        RouteKey::of(3, &source, &LegPath::Direct),
        RouteKey::of(3, &source, &LegPath::Direct)
    );
    assert_ne!(
        RouteKey::of(3, &source, &LegPath::Direct),
        RouteKey::of(4, &source, &LegPath::Direct)
    );
    assert_ne!(
        RouteKey::of(0, &EgressBinding::System, &ladder(1)),
        RouteKey::of(0, &EgressBinding::System, &ladder(2))
    );
    assert_ne!(
        RouteKey::of(0, &EgressBinding::System, &LegPath::Direct),
        RouteKey::of(0, &EgressBinding::System, &ladder(1))
    );
}

#[test]
fn port_slots_are_kept_and_reused_once_free() {
    let db = Database::open_in_memory().unwrap();
    let first = RouteKey::of(0, &EgressBinding::System, &ladder(1));
    let second = RouteKey::of(0, &EgressBinding::System, &ladder(2));
    let third = RouteKey::of(0, &EgressBinding::System, &ladder(3));
    assert_eq!(port_slot(&db, &first).unwrap(), 0);
    assert_eq!(port_slot(&db, &second).unwrap(), 1);
    assert_eq!(port_slot(&db, &first).unwrap(), 0);
    release_slots(&db, |key| key == first.as_str()).unwrap();
    assert_eq!(port_slot(&db, &third).unwrap(), 0);
    assert_eq!(port_slot(&db, &second).unwrap(), 1);
}

// A route keeps its port while it exists, its session dropped or not, and
// gives it back once it is gone.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_keeps_its_port_until_it_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.route(TorrentScope::Default, 1);
    routes.route(TorrentScope::Category(5), 2);
    let db = Database::open_in_memory().unwrap();
    let sessions = Sessions::new(routes.clone(), env(dir.path()), db.clone());
    sessions.set_config(any_port());
    let first = routes.plan(TorrentScope::Default);
    let second = routes.plan(TorrentScope::Category(5));
    sessions
        .session(&first, &IDENTITY.identity())
        .await
        .unwrap();
    sessions
        .session(&second, &IDENTITY.identity())
        .await
        .unwrap();
    let port = |key: &RouteKey| port_slot(&db, key).unwrap();
    let (one, two) = (port(&first.key), port(&second.key));
    assert_ne!(one, two);

    sessions.drop_route(&first.key).await;
    sessions
        .forget_routes([first.key.clone(), second.key.clone()].into())
        .await;
    sessions
        .session(&first, &IDENTITY.identity())
        .await
        .unwrap();
    assert_eq!(sessions.view(&first.key).await.unwrap().port, Some(one));

    sessions.drop_route(&first.key).await;
    sessions.forget_routes([second.key.clone()].into()).await;
    let third = RouteKey::of(0, &EgressBinding::System, &ladder(9));
    assert_eq!(port(&third), one);
    assert_eq!(port(&second.key), two);
    sessions.stop_all().await;
}

#[test]
fn a_route_without_a_slot_listens_on_the_first_slots_port() {
    let db = Database::open_in_memory().unwrap();
    let first = RouteKey::of(0, &EgressBinding::System, &ladder(1));
    let second = RouteKey::of(0, &EgressBinding::System, &ladder(2));
    let torrent = TorrentConfig::default();
    assert_eq!(listen_port(&db, &torrent, &second).unwrap(), Some(6881));
    port_slot(&db, &first).unwrap();
    port_slot(&db, &second).unwrap();
    assert_eq!(listen_port(&db, &torrent, &second).unwrap(), Some(6882));
    let last = TorrentConfig {
        base_port: u16::MAX,
        ..torrent
    };
    assert_eq!(listen_port(&db, &last, &second).unwrap(), None);
}

// A listening route's session announces the configured announce port.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_announces_the_configured_port() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.route(TorrentScope::Default, 1);
    loopback_leg(&routes);
    let db = Database::open_in_memory().unwrap();
    let sessions = Sessions::new(routes.clone(), env(dir.path()), db.clone());
    sessions.set_config(TorrentConfig {
        announce_port: Some(51_413),
        ..any_port()
    });
    let plan = routes.plan(TorrentScope::Default);

    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert_eq!(session.announce_port(), Some(51_413));
    sessions.stop_all().await;
}

// A loopback leg whose session listens on 127.0.0.1.
fn loopback_leg(routes: &FakeRoutes) {
    let kind = LegKind::SourceAddress("127.0.0.1".parse().unwrap());
    let loopback = Arc::new(weaver_tunnel::pipe::Egress::new(
        4,
        weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
        std::time::Duration::MAX,
    ));
    routes.set_leg(kind.clone(), Arc::new(LegSockets::new(loopback, &kind)));
}

// An open route's view carries the address its session bound, and each
// gateway's last report on its mappings as it arrives: a failure with its
// reason, a success with the port and the gateway's address.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_shows_where_it_listens_and_how_its_ports_are_mapped() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    loopback_leg(&routes);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(any_port());
    let plan = routes.plan(TorrentScope::Default);
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();

    let view = sessions.view(&plan.key).await.unwrap();
    let listen = view.listen.expect("an open loopback session listens");
    assert_eq!(listen.ip(), IpAddr::from([127, 0, 0, 1]));
    assert_ne!(listen.port(), 0);
    assert_eq!(view.listen, session.listen_addr());
    assert_eq!(view.utp, session.listens_utp());
    assert!(view.mappings.is_empty());
    assert_eq!(view.held, None);

    let mappings = match sessions.slots.lock().await.get(&plan.key) {
        Some(Slot::Open { mappings, .. }) => mappings.clone(),
        _ => panic!("the route is open"),
    };
    let mut reported = mappings.0.subscribe();
    let events = mappings.follow();
    let failed = PortMapped {
        transport: PortmapTransport::Upnp,
        protocol: PortmapProtocol::Tcp,
        local_port: listen.port(),
        external_port: None,
        external_ip: None,
        error: Some("ConflictInMappingEntry".into()),
    };
    let mapped = PortMapped {
        transport: PortmapTransport::NatPmp,
        protocol: PortmapProtocol::Tcp,
        local_port: listen.port(),
        external_port: Some(40_001),
        external_ip: Some(IpAddr::from([203, 0, 113, 7])),
        error: None,
    };
    events.send(failed.clone()).unwrap();
    events.send(mapped.clone()).unwrap();
    reported
        .wait_for(|mappings| mappings.len() == 2)
        .await
        .unwrap();
    let view = sessions.view(&plan.key).await.unwrap();
    assert_eq!(view.mappings, [failed.clone(), mapped.clone()]);

    // A later report on a mapping stands in for the one before it.
    let renewed = PortMapped {
        external_port: Some(40_002),
        error: None,
        ..failed
    };
    events.send(renewed.clone()).unwrap();
    reported
        .wait_for(|mappings| mappings.first() == Some(&renewed))
        .await
        .unwrap();
    assert_eq!(
        sessions.view(&plan.key).await.unwrap().mappings,
        [renewed.clone(), mapped.clone()]
    );

    // The SSL listener's mapping by the same mapper is a mapping of its own.
    let ssl = PortMapped {
        local_port: listen.port().wrapping_add(1),
        external_port: Some(40_003),
        ..mapped.clone()
    };
    events.send(ssl.clone()).unwrap();
    reported
        .wait_for(|mappings| mappings.len() == 3)
        .await
        .unwrap();
    assert_eq!(
        sessions.view(&plan.key).await.unwrap().mappings,
        [renewed, mapped, ssl]
    );
    sessions.stop_all().await;
}

// A leg that listens, binding nothing on the host, but binds no datagram
// socket: a session on it gets past its listener and fails to build at its
// DHT.
struct NoUdp;

impl SocketFactory for NoUdp {
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

    fn bind_udp<'a>(&'a self, _: SocketAddr) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async { Err(io::Error::from(io::ErrorKind::Unsupported)) })
    }
}

// Each step of opening a session that fails holds its route closed with the
// reason of that step.
#[tokio::test(flavor = "multi_thread")]
async fn session_setup_failures_keep_the_cause_that_failed() {
    for expected in [
        TorrentRouteReason::LegOpenFailed,
        TorrentRouteReason::StateDirectoryFailed,
        TorrentRouteReason::ListenPortUnavailable,
        TorrentRouteReason::SessionStateUnreadable,
        TorrentRouteReason::SessionBuildFailed,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let routes = Arc::new(FakeRoutes::default());
        let db = Database::open_in_memory().unwrap();
        let root = dir.path().join("state");
        let sessions = Sessions::new(routes.clone(), env(&root), db.clone());
        match expected {
            TorrentRouteReason::LegOpenFailed => routes.set_refused(Some("the leg failed to open")),
            TorrentRouteReason::StateDirectoryFailed => {
                std::fs::write(&root, "not a directory").unwrap()
            }
            TorrentRouteReason::ListenPortUnavailable => {
                let first = RouteKey::of(9, &EgressBinding::System, &LegPath::Direct);
                assert_eq!(port_slot(&db, &first).unwrap(), 0);
                sessions.set_config(TorrentConfig {
                    base_port: u16::MAX,
                    ..Default::default()
                });
            }
            TorrentRouteReason::SessionStateUnreadable => {
                use crate::persistence::sql_runtime::SqlRuntime;
                let store = db.datastore();
                db.run_sql_blocking(async move {
                    SqlRuntime::run_in_transaction(&store, "missing_settings_fixture", |tx| {
                        Box::pin(async move {
                            tx.execute("DROP TABLE settings", &[]).await?;
                            Ok(())
                        })
                    })
                    .await
                })
                .unwrap();
            }
            TorrentRouteReason::SessionBuildFailed => routes.set_leg(
                LegKind::SourceAddress("127.0.0.1".parse().unwrap()),
                Arc::new(NoUdp),
            ),
            _ => unreachable!(),
        }
        let plan = routes.plan(TorrentScope::Default);
        let Err(SessionError::Build(failure)) = sessions.session(&plan, &IDENTITY.identity()).await
        else {
            panic!("failed setup opened a session: {expected:?}");
        };
        assert_eq!(failure.reason, expected, "{failure:?}");
        assert_eq!(
            sessions.view(&plan.key).await.unwrap().held.unwrap().reason,
            expected
        );
        sessions.stop_all().await;
    }
}

// A route whose session could not listen holds closed with the reason, and
// shows no address and no mapping.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_that_could_not_listen_shows_why() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let kind = LegKind::SourceAddress("127.0.0.1".parse().unwrap());
    routes.set_leg(kind, Arc::new(Nowhere));
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    let plan = routes.plan(TorrentScope::Default);
    let Err(SessionError::Build(error)) = sessions.session(&plan, &IDENTITY.identity()).await
    else {
        panic!("a leg that cannot listen builds no session");
    };

    let view = sessions.view(&plan.key).await.unwrap();
    assert_eq!(
        view.held
            .as_ref()
            .and_then(|failure| failure.detail.as_deref()),
        error.detail.as_deref()
    );
    assert_eq!(error.reason, TorrentRouteReason::ListenPortBindFailed);
    assert!(error.message().contains("listen"), "{error:?}");
    assert_eq!(view.listen, None);
    assert!(view.mappings.is_empty());
    assert!(view.external.is_empty());
    sessions.stop_all().await;
}

// A session that cannot be opened is logged with its cause once, however
// often its route is asked to open for that same cause; a different cause is
// logged again.
#[tokio::test(flavor = "current_thread")]
async fn a_session_that_cannot_open_is_logged_once_for_each_cause() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.set_leg(
        LegKind::SourceAddress("127.0.0.1".parse().unwrap()),
        Arc::new(NoUdp),
    );
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    let (lines, _guard) = crate::pipeline::graph::lines::Lines::capture_at(
        "weaver_server_core::torrents",
        tracing::Level::WARN,
    );
    let plan = routes.plan(TorrentScope::Default);
    let mut first = None;
    for _ in 0..3 {
        let Err(SessionError::Build(failure)) = sessions.session(&plan, &IDENTITY.identity()).await
        else {
            panic!("a leg with no datagram socket builds no session");
        };
        first.get_or_insert(failure);
    }
    let first = first.unwrap();
    assert_eq!(first.reason, TorrentRouteReason::SessionBuildFailed);
    let detail = first.detail.clone().expect("the build failure has a cause");
    let heard = lines.of("a route's session could not be opened");
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(heard[0].contains(&detail), "{heard:?}");

    routes.set_refused(Some("the leg failed to open"));
    let Err(SessionError::Build(second)) = sessions.session(&plan, &IDENTITY.identity()).await
    else {
        panic!("a refused leg builds no session");
    };
    assert_eq!(second.reason, TorrentRouteReason::LegOpenFailed);
    let heard = lines.of("a route's session could not be opened");
    assert_eq!(heard.len(), 2, "{heard:?}");
    sessions.stop_all().await;
}

// A route whose session is being built reads as opening at once: its build
// waits on its leg, and no reader of the sessions waits with it. A second
// opener of the route waits for that build and is handed its session; one
// leg is opened.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_reads_as_opening_while_its_leg_has_not_answered() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let sessions = Arc::new(Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    ));
    sessions.set_config(any_port());
    let plan = routes.plan(TorrentScope::Default);
    routes.unanswered.send_replace(true);
    let mut asked = routes.asked.subscribe();
    let opening = |sessions: Arc<Sessions>, plan: RoutePlan| {
        tokio::spawn(async move { sessions.session(&plan, &IDENTITY.identity()).await })
    };
    let first = opening(Arc::clone(&sessions), plan.clone());
    asked.wait_for(|asked| *asked == 1).await.unwrap();

    let views = polled(std::pin::pin!(sessions.views()))
        .expect("the sessions read while a route's leg has not answered");
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].held, Some(OPENING));
    let view = polled(std::pin::pin!(sessions.view(&plan.key))).unwrap();
    assert_eq!(
        view.unwrap().held.map(|held| held.reason),
        Some(TorrentRouteReason::Opening)
    );
    let read = Mutex::new(Vec::new());
    assert_eq!(
        polled(std::pin::pin!(sessions.read_into(&read))),
        Some(()),
        "the exporter reads while a route's leg has not answered"
    );
    assert_eq!(read.lock().unwrap().len(), 1);

    let second = opening(Arc::clone(&sessions), plan.clone());
    routes.unanswered.send_replace(false);
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(routes.opened.load(Ordering::SeqCst), 1);
    assert_eq!(sessions.view(&plan.key).await.unwrap().held, None);
    sessions.stop_all().await;
}

// A route whose session is being built when torrents are turned off holds
// no session: the one built stops, and its opener is told why.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_built_after_torrents_were_turned_off_is_not_kept() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let sessions = Arc::new(Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    ));
    sessions.set_config(any_port());
    let plan = routes.plan(TorrentScope::Default);
    routes.unanswered.send_replace(true);
    let mut asked = routes.asked.subscribe();
    let opener = {
        let (sessions, plan) = (Arc::clone(&sessions), plan.clone());
        tokio::spawn(async move { sessions.session(&plan, &IDENTITY.identity()).await })
    };
    asked.wait_for(|asked| *asked == 1).await.unwrap();
    sessions.turn(false).await;
    routes.unanswered.send_replace(false);

    assert_eq!(
        opener.await.unwrap().err(),
        Some(SessionError::RouteDown(
            TorrentRouteReason::TorrentsDisabled.into()
        ))
    );
    assert!(sessions.views().await.is_empty());
    assert!(sessions.running().await.is_empty());
    sessions.stop_all().await;
}

// The SSL port chosen while the settings name none is chosen once, above
// the listen ports and below the ephemeral range, and kept.
#[test]
fn the_chosen_ssl_port_is_kept() {
    let db = Database::open_in_memory().unwrap();
    let chosen = chosen_ssl_port(&db).unwrap();
    assert!((30_000..50_000).contains(&chosen));
    assert_eq!(chosen_ssl_port(&db).unwrap(), chosen);
}

// A route's session takes SSL torrents' peers on a listener of its own
// while the settings turn it on, and has none while they do not.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_listens_for_ssl_peers_while_turned_on() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.route(TorrentScope::Default, 1);
    loopback_leg(&routes);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(any_port());
    let plan = routes.plan(TorrentScope::Default);

    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(session.listen_addr().is_some());
    assert_eq!(session.ssl_listen_addr(), None);

    // Port 0 asks the host for one, so the test takes no fixed port.
    sessions.set_config(TorrentConfig {
        ssl_enabled: true,
        ssl_port: Some(0),
        ..any_port()
    });
    sessions.settled().await;
    sessions.drop_route(&plan.key).await;
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    let ssl = session.ssl_listen_addr().expect("an SSL listener");
    assert_ne!(ssl.port(), 0);
    assert_ne!(Some(ssl), session.listen_addr());
    sessions.stop_all().await;
}

// Connections per address take effect on the open session, as libtorrent's
// `apply_settings` does: the same session, not one opened again.
#[tokio::test(flavor = "multi_thread")]
async fn connections_per_address_reach_the_open_session_live() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.route(TorrentScope::Default, 1);
    loopback_leg(&routes);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(any_port());
    let plan = routes.plan(TorrentScope::Default);
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(!session.allows_multiple_connections_per_ip());

    sessions.set_config(TorrentConfig {
        allow_multiple_connections_per_ip: true,
        ..any_port()
    });
    sessions.settled().await;
    assert!(session.allows_multiple_connections_per_ip());
    assert!(sessions.outdated().await.is_empty());
    let again = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(Arc::ptr_eq(&session, &again));

    sessions.set_config(any_port());
    sessions.settled().await;
    assert!(!session.allows_multiple_connections_per_ip());
    sessions.stop_all().await;
}

// The next announce the tracker on `listener` hears, unanswered, and its
// request line.
async fn heard_announce(listener: &tokio::net::TcpListener) -> (tokio::net::TcpStream, String) {
    use tokio::io::AsyncReadExt;

    let (mut stream, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    while !request.windows(4).any(|end| end == b"\r\n\r\n") {
        let read = stream.read(&mut buf).await.unwrap();
        assert!(read > 0, "the announce ended before its request was read");
        request.extend_from_slice(&buf[..read]);
    }
    let line = String::from_utf8_lossy(&request)
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned();
    (stream, line)
}

// The tracker answers `stream`'s announce with no peers.
async fn answer(mut stream: tokio::net::TcpStream) {
    use tokio::io::AsyncWriteExt;

    let body = b"d8:intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(&[head.as_bytes(), body].concat())
        .await
        .unwrap();
}

// A one-piece torrent that announces to `url`.
fn announcing(url: &str) -> Vec<u8> {
    let mut metainfo = format!("d8:announce{}:{url}4:info", url.len()).into_bytes();
    metainfo.extend_from_slice(b"d6:lengthi16384e4:name5:a.bin12:piece lengthi16384e6:pieces20:");
    metainfo.extend_from_slice(&[7; 20]);
    metainfo.extend_from_slice(b"ee");
    metainfo
}

// Options announcing to `url` as a tracker the operator named: one on
// loopback the torrent named would never be asked.
fn operators(url: &str) -> weaver_torrent::AddTorrentOptions {
    weaver_torrent::AddTorrentOptions {
        tracker_tiers: Some(vec![vec![weaver_torrent::tracker::Tracker::operator(
            url.parse().unwrap(),
        )]]),
        ..Default::default()
    }
}

// Two loopback ports free as this is called.
fn two_free_ports() -> (u16, u16) {
    let first = TcpListener::bind("127.0.0.1:0").unwrap();
    let second = TcpListener::bind("127.0.0.1:0").unwrap();
    (
        first.local_addr().unwrap().port(),
        second.local_addr().unwrap().port(),
    )
}

// A base port change opens the route on its new port at once: its last
// session stops off the open path, its tracker still to hear `stopped`, and
// each step of that stop logs.
#[tokio::test]
async fn a_base_port_change_listens_on_the_new_port_while_the_old_stopped_announce_waits() {
    let (lines, _guard) =
        crate::pipeline::graph::lines::Lines::capture_at("weaver", tracing::Level::INFO);
    let tracker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", tracker.local_addr().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.route(TorrentScope::Default, 1);
    loopback_leg(&routes);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    let (first, second) = two_free_ports();
    // A day: the stopped announce waits on the tracker, not on its timeout.
    let on = |base_port| TorrentConfig {
        base_port,
        stop_tracker_timeout_secs: 86_400,
        ..TorrentConfig::default()
    };
    sessions.set_config(on(first));
    let plan = routes.plan(TorrentScope::Default);
    let old = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert_eq!(old.listen_addr().map(|addr| addr.port()), Some(first));

    // The torrent announces, and leaves its session: the tracker hears
    // `stopped`, and does not answer.
    let added = old
        .add_torrent(
            weaver_torrent::AddTorrent::from_bytes(announcing(&url)),
            Some(operators(&url)),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    let (started, line) = heard_announce(&tracker).await;
    assert!(line.contains("event=started"), "{line}");
    answer(started).await;
    // Only a tracker that took `started` is sent `stopped`.
    added
        .tracker_status()
        .expect("a live run with trackers")
        .subscribe()
        .wait_for(|announces| {
            announces[0]
                .last
                .as_ref()
                .is_some_and(|last| last.result.is_ok())
        })
        .await
        .unwrap();
    old.delete(weaver_torrent::api::TorrentIdOrHash::Id(added.id()), false)
        .await
        .unwrap();
    let (stopped, line) = heard_announce(&tracker).await;
    assert!(line.contains("event=stopped"), "{line}");

    sessions.set_config(on(second));
    sessions.settled().await;
    assert_eq!(
        sessions.outdated().await,
        BTreeSet::from([plan.key.clone()])
    );
    sessions.drop_route(&plan.key).await;
    let new = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert_eq!(new.listen_addr().map(|addr| addr.port()), Some(second));
    // The old session still waits on its tracker.
    assert!(!old.cancellation_token().is_cancelled());

    answer(stopped).await;
    sessions.stop_all().await;
    let route = format!(" route={:?}", plan.key.as_str());
    let heard = |message: &str| lines.of(message);
    let closed_off =
        heard("a route's session closes off the open path; the route may open again at once");
    assert_eq!(closed_off.len(), 1, "{closed_off:?}");
    assert!(closed_off[0].contains(&route), "{closed_off:?}");
    // The old session and, at the end, the new one.
    for message in ["stopping a route's session", "a route's session stopped"] {
        let heard = heard(message);
        assert_eq!(heard.len(), 2, "{message}: {heard:?}");
    }
    for message in [
        "stopping the session: closing its listeners, DHT and local discovery",
        "stopping the session: pausing its torrents",
        "stopping the session: waiting on its trackers' stopped announces",
        "stopping the session: closing its trackers and removing its port mappings",
        "the session stopped",
    ] {
        let heard = heard(message);
        assert_eq!(heard.len(), 2, "{message}: {heard:?}");
    }
}

// A mapping session asks the gateway for its own listen port, whatever
// announce port is set; one that does not map asks for none.
#[test]
fn a_session_maps_its_listen_port_when_it_maps() {
    let upnp = TorrentConfig {
        upnp: true,
        announce_port: Some(51_413),
        ..TorrentConfig::default()
    };
    let system = LegKind::System.shape(None);
    assert_eq!(maps(&upnp, system, 6881), Some(6881));
    let own = TorrentConfig {
        announce_port: None,
        ..upnp.clone()
    };
    assert_eq!(maps(&own, system, 6881), Some(6881));
    let off = TorrentConfig {
        upnp: false,
        ..upnp.clone()
    };
    assert_eq!(maps(&off, system, 6881), None);
    // A leg held to a source address or a device maps as the host's does.
    let address = LegKind::SourceAddress("192.0.2.4".parse().unwrap()).shape(None);
    assert_eq!(maps(&upnp, address, 6881), Some(6881));
    let device = LegKind::Interface("en1".into()).shape(None);
    assert_eq!(maps(&upnp, device, 6882), Some(6882));
    assert_eq!(maps(&off, device, 6881), None);
    // A ladder leg maps nothing, however it listens.
    let mut ladder = LegKind::Ladder.shape(None);
    ladder.listen = true;
    assert_eq!(maps(&upnp, ladder, 6881), None);
}

// `future` polled once: its output, if it is ready.
fn polled<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> Option<F::Output> {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match future.poll(&mut cx) {
        std::task::Poll::Ready(output) => Some(output),
        std::task::Poll::Pending => None,
    }
}

// A gateway knows a mapping by its external port alone: a new session that
// maps the port a stopping one still has mapped waits for that one to
// remove it, and one that maps another port does not wait.
#[tokio::test]
async fn a_new_session_waits_on_a_stopping_one_only_for_a_port_it_shares() {
    let (lines, _guard) =
        crate::pipeline::graph::lines::Lines::capture_at("weaver", tracing::Level::INFO);
    let routes = FakeRoutes::default();
    let plan = routes.plan(TorrentScope::Default);
    let closing = Closing::default();
    let (stopped, waiting) = tokio::sync::watch::channel(false);
    // Listening on 6881 and 6882, it maps 51413.
    closing.lock().push(Stopping {
        route: plan.key.clone(),
        ports: vec![6881, 6882, 51_413],
        stopped: waiting,
    });

    let elsewhere = std::pin::pin!(closing.released(&plan, [6883, 51_414].into_iter()));
    assert_eq!(polled(elsewhere), Some(()));
    assert!(
        lines
            .of("a route's session waits on a stopping session to release its port")
            .is_empty()
    );

    let mut shared = std::pin::pin!(closing.released(&plan, [6883, 51_413].into_iter()));
    assert_eq!(polled(shared.as_mut()), None);
    assert_eq!(
        lines
            .of("a route's session waits on a stopping session to release its port")
            .len(),
        1
    );
    stopped.send_replace(true);
    shared.await;
    assert_eq!(lines.of("the stopping session released the port").len(), 1);
}

#[test]
fn a_host_without_ipv6_listens_on_every_ipv4_address() {
    assert_eq!(
        unspecified(|_| true),
        IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)
    );
    assert_eq!(
        unspecified(|ip| ip.is_ipv4()),
        IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
    );
}

fn count(count: u32) -> TorrentLimit {
    TorrentLimit::Count(std::num::NonZeroU32::new(count).unwrap())
}

fn speed(speed: u32) -> std::num::NonZeroU32 {
    std::num::NonZeroU32::new(speed).unwrap()
}

// A session opens under the settings, and an open one takes the limits as
// they change (libtorrent `session_impl::apply_settings`).
#[tokio::test(flavor = "multi_thread")]
async fn the_torrent_settings_reach_the_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(TorrentConfig {
        connections_limit: count(321),
        unchoke_slots_limit: TorrentLimit::Unlimited,
        connection_speed: speed(7),
        ..TorrentConfig::default()
    });
    let plan = routes.plan(TorrentScope::Default);
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert_eq!(
        session.limits(),
        SessionLimits {
            connections_limit: 321,
            unchoke_slots_limit: usize::MAX,
            connection_speed: speed(7),
        }
    );

    sessions.set_config(TorrentConfig {
        connections_limit: TorrentLimit::Unlimited,
        unchoke_slots_limit: count(3),
        connection_speed: speed(40),
        ..TorrentConfig::default()
    });
    assert_eq!(
        session.limits(),
        SessionLimits {
            connections_limit: super::connections_within(
                crate::runtime::resource_limits::open_file_limit()
            ),
            unchoke_slots_limit: 3,
            connection_speed: speed(40),
        }
    );
    assert!(Arc::ptr_eq(
        &session,
        &sessions.session(&plan, &IDENTITY.identity()).await.unwrap()
    ));
    sessions.stop_all().await;
}

// A route whose session would have no way to reach a peer holds closed:
// uTP alone over a leg that does not listen.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_with_no_transport_holds_closed() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(TorrentConfig {
        peer_protocol: crate::settings::TorrentPeerProtocol::Utp,
        ..TorrentConfig::default()
    });
    let plan = routes.plan(TorrentScope::Default);
    assert!(matches!(
        sessions.session(&plan, &IDENTITY.identity()).await,
        Err(SessionError::Build(_))
    ));
    let held = sessions.view(&plan.key).await.unwrap().held.unwrap();
    assert!(
        held.message().contains("no way to connect to peers"),
        "{held:?}"
    );
    sessions.stop_all().await;
}

// A route whose blocklist does not load holds closed rather than open
// without the filter asked for.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_whose_blocklist_does_not_load_holds_closed() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    let missing = dir.path().join("missing.p2p");
    sessions.set_config(TorrentConfig {
        blocklist_enabled: true,
        blocklist: Some(missing.display().to_string()),
        ..TorrentConfig::default()
    });
    let plan = routes.plan(TorrentScope::Default);
    assert!(matches!(
        sessions.session(&plan, &IDENTITY.identity()).await,
        Err(SessionError::Build(_))
    ));
    let held = sessions.view(&plan.key).await.unwrap().held.unwrap();
    assert_eq!(held.reason, TorrentRouteReason::BlocklistFailed);
    assert!(
        held.message().contains("the blocklist did not load"),
        "{held:?}"
    );

    // Once it is there, the route opens on its next session.
    std::fs::write(&missing, "test:10.0.0.1-10.0.0.1\n").unwrap();
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(session.ip_filter().blocked.has("10.0.0.1".parse().unwrap()));
    sessions.stop_all().await;
}

// The banned addresses, the privileged ports and whether trackers are
// filtered reach the open sessions at once, with the blocklist once it has
// loaded (libtorrent `session_impl::set_ip_filter`, qBittorrent
// `SessionImpl::processBannedIPs`); settings that change none of them keep
// the one list.
#[tokio::test(flavor = "multi_thread")]
async fn the_ip_filter_settings_reach_the_open_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    *routes.blocklist.lock().unwrap() = Some("test:192.0.2.1-192.0.2.1\n".to_owned());
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(TorrentConfig::default());
    let plan = routes.plan(TorrentScope::Default);
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    let (banned, listed): (IpAddr, IpAddr) =
        ("10.0.0.7".parse().unwrap(), "192.0.2.1".parse().unwrap());
    // qBittorrent's defaults: nothing blocked, no port refused, trackers
    // left alone.
    let filter = session.ip_filter();
    assert!(!filter.blocked.has(banned));
    assert!(!filter.no_privileged_ports);
    assert!(!filter.trackers);

    let bans = TorrentConfig {
        banned_ips: vec!["10.0.0.0 - 10.0.0.255".to_owned()],
        block_privileged_ports: true,
        filter_trackers: true,
        ..TorrentConfig::default()
    };
    sessions.set_config(bans.clone());
    sessions.settled().await;
    let filter = session.ip_filter();
    assert!(filter.blocked.has(banned));
    assert!(!filter.blocked.has(listed));
    assert!(filter.no_privileged_ports);
    assert!(filter.trackers);

    let listing = TorrentConfig {
        blocklist_enabled: true,
        blocklist: Some("https://lists.example/level1.p2p".to_owned()),
        ..bans
    };
    sessions.set_config(listing.clone());
    sessions.settled().await;
    let filter = session.ip_filter();
    assert!(filter.blocked.has(banned));
    assert!(filter.blocked.has(listed));

    // Other settings keep the list the session holds.
    sessions.set_config(TorrentConfig {
        connection_speed: speed(9),
        ..listing
    });
    sessions.settled().await;
    assert!(Arc::ptr_eq(&session.ip_filter().blocked, &filter.blocked));
    assert_eq!(*routes.fetched.borrow(), 1);

    sessions.set_config(TorrentConfig::default());
    sessions.settled().await;
    let filter = session.ip_filter();
    assert!(!filter.blocked.has(banned));
    assert!(!filter.blocked.has(listed));
    assert!(!filter.no_privileged_ports);
    assert!(Arc::ptr_eq(
        &session,
        &sessions.session(&plan, &IDENTITY.identity()).await.unwrap()
    ));
    sessions.stop_all().await;
}

// A blocklist named while sessions are open that does not load holds them
// closed, as it holds closed a route opening under it; once it loads, the
// route opens again under it.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocklist_that_does_not_load_holds_the_open_sessions_closed() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    let plan = routes.plan(TorrentScope::Default);
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();

    sessions.set_config(TorrentConfig {
        blocklist_enabled: true,
        blocklist: Some("https://lists.example/level1.p2p".to_owned()),
        ..TorrentConfig::default()
    });
    sessions.settled().await;
    let held = sessions.view(&plan.key).await.unwrap().held.unwrap();
    assert_eq!(held.reason, TorrentRouteReason::BlocklistFailed);
    assert!(
        held.message().contains("the blocklist did not load"),
        "{held:?}"
    );
    session.cancellation_token().cancelled().await;

    *routes.blocklist.lock().unwrap() = Some("test:192.0.2.1-192.0.2.1\n".to_owned());
    let reopened = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(!Arc::ptr_eq(&session, &reopened));
    assert!(
        reopened
            .ip_filter()
            .blocked
            .has("192.0.2.1".parse().unwrap())
    );
    sessions.stop_all().await;
}

// The blocklist is loaded once for the settings that name it, and every
// route's session shares it; settings naming another load that one.
#[tokio::test(flavor = "multi_thread")]
async fn every_route_shares_one_blocklist_load() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.route(TorrentScope::Default, 1);
    routes.route(TorrentScope::Category(5), 2);
    *routes.blocklist.lock().unwrap() = Some("test:10.0.0.1-10.0.0.1\n".to_owned());
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    let named = TorrentConfig {
        blocklist_enabled: true,
        blocklist: Some("https://lists.example/level1.p2p".to_owned()),
        ..TorrentConfig::default()
    };
    sessions.set_config(named.clone());
    let first = routes.plan(TorrentScope::Default);
    let second = routes.plan(TorrentScope::Category(5));
    let a = sessions
        .session(&first, &IDENTITY.identity())
        .await
        .unwrap();
    let b = sessions
        .session(&second, &IDENTITY.identity())
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&a, &b));
    assert!(Arc::ptr_eq(&a.ip_filter().blocked, &b.ip_filter().blocked));
    assert!(a.ip_filter().blocked.has("10.0.0.1".parse().unwrap()));
    assert_eq!(*routes.fetched.borrow(), 1);

    sessions.drop_route(&first.key).await;
    sessions.set_config(TorrentConfig {
        blocklist: Some("https://lists.example/level2.p2p".to_owned()),
        ..named
    });
    sessions
        .session(&first, &IDENTITY.identity())
        .await
        .unwrap();
    assert_eq!(*routes.fetched.borrow(), 2);
    sessions.stop_all().await;
}

#[test]
fn the_built_session_options_carry_the_configured_values() {
    let dir = tempfile::tempdir().unwrap();
    let torrent = TorrentConfig {
        connections_limit: count(450),
        unchoke_slots_limit: count(12),
        connection_speed: speed(55),
        allow_multiple_connections_per_ip: true,
        dht_bootstrap_nodes: " router.example:6881, 192.0.2.9:25401 ,".to_owned(),
        stop_tracker_timeout_secs: 0,
        ..TorrentConfig::default()
    };
    let filter = IpFilter {
        blocked: Arc::new(IpRanges::default()),
        no_privileged_ports: true,
        trackers: false,
    };
    let options = session_options(
        &leg(LegKind::System),
        6881,
        dir.path(),
        75,
        &torrent,
        filter.clone(),
        &IDENTITY.identity(),
    );
    assert_eq!(options.connections_limit, 450);
    assert_eq!(options.unchoke_slots_limit, 12);
    assert_eq!(options.connection_speed.get(), 55);
    assert!(options.allow_multiple_connections_per_ip);
    assert_eq!(options.peer_limit, Some(75));
    assert_eq!(
        options.dht.as_ref().unwrap().bootstrap_addrs.as_deref(),
        Some(
            [
                "router.example:6881".to_owned(),
                "192.0.2.9:25401".to_owned()
            ]
            .as_slice()
        )
    );
    assert_eq!(options.ip_filter, filter);
    assert_eq!(options.tracker_stopped_timeout, Some(Duration::ZERO));

    // The defaults are libtorrent's, and peers go over TCP alone.
    let defaults = session_options(
        &leg(LegKind::System),
        6881,
        dir.path(),
        75,
        &TorrentConfig::default(),
        IpFilter::default(),
        &IDENTITY.identity(),
    );
    assert_eq!(defaults.connections_limit, 200);
    assert_eq!(defaults.unchoke_slots_limit, 8);
    assert_eq!(defaults.connection_speed.get(), 30);
    assert!(!defaults.allow_multiple_connections_per_ip);
    // The engine's own routers, as when it was told none.
    assert_eq!(
        defaults.dht.as_ref().unwrap().bootstrap_addrs.as_deref(),
        Some(
            weaver_torrent::dht::DHT_BOOTSTRAP
                .iter()
                .map(|node| (*node).to_owned())
                .collect::<Vec<_>>()
                .as_slice()
        )
    );
    assert_eq!(defaults.ip_filter.blocked.len(), 0);
    assert_eq!(
        defaults.tracker_stopped_timeout,
        Some(Duration::from_secs(5))
    );
    assert!(matches!(
        defaults.listen.as_ref().unwrap().mode,
        ListenerMode::TcpOnly
    ));
    assert!(defaults.connect.as_ref().unwrap().enable_tcp);
}

#[test]
fn the_peer_protocol_picks_the_transports_the_leg_carries() {
    use crate::settings::TorrentPeerProtocol;
    let dir = tempfile::tempdir().unwrap();
    let tunnel = leg(LegKind::Ladder);
    let built = |leg: &OpenLeg, protocol| {
        let torrent = TorrentConfig {
            peer_protocol: protocol,
            ..TorrentConfig::default()
        };
        let options = session_options(
            leg,
            6881,
            dir.path(),
            100,
            &torrent,
            IpFilter::default(),
            &IDENTITY.identity(),
        );
        let connect = options.connect.unwrap();
        (
            options.listen.map(|listen| listen.mode),
            connect.enable_tcp,
            connect.enable_utp,
        )
    };
    let system = leg(LegKind::System);
    assert!(matches!(
        built(&system, TorrentPeerProtocol::Tcp),
        (Some(ListenerMode::TcpOnly), true, Some(false))
    ));
    assert!(matches!(
        built(&system, TorrentPeerProtocol::Utp),
        (Some(ListenerMode::UtpOnly), false, Some(true))
    ));
    assert!(matches!(
        built(&system, TorrentPeerProtocol::Both),
        (Some(ListenerMode::TcpAndUtp), true, Some(true))
    ));
    // Incoming policy is independent of the transports a leg dials over.
    let mut datagrams = leg(LegKind::Ladder);
    datagrams.shape.datagrams = true;
    datagrams.shape.utp = true;
    for ladder in [&tunnel, &datagrams] {
        assert!(!ladder.shape.listen);
        let (listen, tcp, utp) = built(ladder, TorrentPeerProtocol::Both);
        assert!(listen.is_none() && tcp);
        assert_eq!(utp, Some(ladder.shape.utp));
        let (listen, tcp, utp) = built(ladder, TorrentPeerProtocol::Utp);
        assert!(listen.is_none() && !tcp);
        assert_eq!(utp, Some(ladder.shape.utp));
    }
}

// A ladder leg that listens inside its tunnel takes peers over TCP alone,
// as the tunnel carries no uTP, and announces the port it listens on, or the
// announce port when one is set; it still maps none.
#[test]
fn a_listening_ladder_takes_tcp_peers_and_announces_its_port() {
    use crate::settings::TorrentPeerProtocol;
    let dir = tempfile::tempdir().unwrap();
    let mut tunnel = leg(LegKind::Ladder);
    tunnel.shape.listen = true;
    assert!(!tunnel.shape.utp);
    let built = |protocol, announce_port| {
        let torrent = TorrentConfig {
            peer_protocol: protocol,
            announce_port,
            upnp: true,
            ..TorrentConfig::default()
        };
        session_options(
            &tunnel,
            6885,
            dir.path(),
            100,
            &torrent,
            IpFilter::default(),
            &IDENTITY.identity(),
        )
    };
    for protocol in [TorrentPeerProtocol::Tcp, TorrentPeerProtocol::Both] {
        let options = built(protocol, None);
        let listen = options.listen.expect("a listening ladder listens");
        assert!(matches!(listen.mode, ListenerMode::TcpOnly));
        assert_eq!(listen.listen_addr.port(), 6885);
        assert_eq!(listen.announce_port, None);
        assert!(!listen.enable_upnp_port_forwarding);
        assert!(options.connect.unwrap().enable_tcp);
    }
    let listen = built(TorrentPeerProtocol::Tcp, Some(51413)).listen.unwrap();
    assert_eq!(listen.announce_port, Some(51413));
    // Asked for uTP alone, it has no transport the tunnel carries.
    let options = built(TorrentPeerProtocol::Utp, None);
    assert!(options.listen.is_none());
    assert!(!options.connect.unwrap().enable_tcp);
}

fn leg(kind: LegKind) -> OpenLeg {
    OpenLeg {
        shape: kind.shape(None),
        kind,
        factory: Arc::new(weaver_torrent::network::KernelSockets::new(None).unwrap()),
        names: Arc::new(weaver_torrent::network::NoNames),
        http: tracker_client(
            &LegKind::System,
            crate::torrents::net::system_dialer(),
            None,
        )
        .unwrap(),
        leg: None,
    }
}

#[test]
fn session_options_follow_the_leg() {
    let dir = tempfile::tempdir().unwrap();
    let torrent = TorrentConfig {
        upnp: true,
        announce_port: Some(51413),
        ..TorrentConfig::default()
    };
    let client = crate::settings::model::Client::QBittorrent.name();
    let opener = client.identity();

    let system = session_options(
        &leg(LegKind::System),
        6881,
        dir.path(),
        100,
        &torrent,
        IpFilter::default(),
        &opener,
    );
    assert!(system.dht.is_some());
    let listen = system.listen.as_ref().unwrap();
    assert!(listen.enable_upnp_port_forwarding);
    assert_eq!(listen.listen_addr.port(), 6881);
    assert_eq!(listen.announce_port, Some(51413));
    assert!(!system.disable_local_service_discovery);
    // The session names the client of the torrent that opens it, under a
    // peer id of its own.
    let identity = system.identity.as_ref().unwrap();
    assert_eq!(&identity.peer_id.0[..8], client.prefix());
    assert_ne!(identity.peer_id, opener.peer_id);
    assert_eq!(identity.user_agent, client.user_agent());
    assert_eq!(identity.version, client.version());
    assert!(system.http_client.is_some());

    let interface = session_options(
        &leg(LegKind::Interface("en1".into())),
        6881,
        dir.path(),
        100,
        &torrent,
        IpFilter::default(),
        &IDENTITY.identity(),
    );
    assert!(interface.dht.is_some());
    assert_eq!(interface.bind_device_name.as_deref(), Some("en1"));
    assert!(interface.listen.unwrap().enable_upnp_port_forwarding);

    let address: IpAddr = "192.0.2.4".parse().unwrap();
    let source = session_options(
        &leg(LegKind::SourceAddress(address)),
        6882,
        dir.path(),
        100,
        &TorrentConfig::default(),
        IpFilter::default(),
        &IDENTITY.identity(),
    );
    assert!(source.dht.is_some());
    let listen = source.listen.unwrap();
    assert_eq!(listen.listen_addr, SocketAddr::new(address, 6882));
    // The port-mapping setting is on by default.
    assert!(listen.enable_upnp_port_forwarding);
    assert_eq!(source.bind_device_name, None);
    assert!(source.disable_local_service_discovery);

    let ladder = session_options(
        &leg(LegKind::Ladder),
        6883,
        dir.path(),
        100,
        &TorrentConfig::default(),
        IpFilter::default(),
        &IDENTITY.identity(),
    );
    assert!(ladder.dht.is_none());
    assert!(ladder.listen.is_none());
    assert!(ladder.disable_local_service_discovery);
    assert_eq!(ladder.peer_limit, Some(100));

    // A ladder whose tunnel carries datagrams runs DHT, and still neither
    // listens nor maps a port.
    let mut tunnel = leg(LegKind::Ladder);
    tunnel.shape.dht = true;
    tunnel.shape.datagrams = true;
    let tunnel = session_options(
        &tunnel,
        6884,
        dir.path(),
        100,
        &TorrentConfig::default(),
        IpFilter::default(),
        &IDENTITY.identity(),
    );
    assert_eq!(tunnel.dht.as_ref().and_then(|dht| dht.port), Some(6884));
    assert!(tunnel.listen.is_none());
    assert!(tunnel.disable_local_service_discovery);
}

#[test]
fn a_peer_may_stay_quiet_for_two_minutes_on_any_leg() {
    let dir = tempfile::tempdir().unwrap();
    for kind in [LegKind::System, LegKind::Ladder] {
        let options = session_options(
            &leg(kind),
            6881,
            dir.path(),
            100,
            &TorrentConfig::default(),
            IpFilter::default(),
            &IDENTITY.identity(),
        );
        let peer = options.connect.unwrap().peer_opts.unwrap();
        assert_eq!(peer.read_write_timeout, Some(Duration::from_secs(120)));
        assert_eq!(peer.connect_timeout, None);
        assert_eq!(peer.keep_alive_interval, None);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn scopes_on_one_route_share_a_session() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.route(TorrentScope::Default, 1);
    routes.route(TorrentScope::Category(5), 1);
    routes.route(TorrentScope::Category(6), 2);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );

    let default = routes.plan(TorrentScope::Default);
    let five = routes.plan(TorrentScope::Category(5));
    let six = routes.plan(TorrentScope::Category(6));
    assert_eq!(default.key, five.key);
    assert_ne!(default.key, six.key);

    let a = sessions
        .session(&default, &IDENTITY.identity())
        .await
        .unwrap();
    let b = sessions.session(&five, &IDENTITY.identity()).await.unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    sessions.session(&six, &IDENTITY.identity()).await.unwrap();
    assert_eq!(sessions.views().await.len(), 2);
    assert_eq!(routes.opened.load(Ordering::SeqCst), 2);
    assert!(dir.path().join(default.key.as_str()).is_dir());
    sessions.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ninth_route_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    for id in 0..MAX_SESSIONS as u32 {
        routes.route(TorrentScope::Category(id), id + 1);
        let plan = routes.plan(TorrentScope::Category(id));
        sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    }
    routes.route(TorrentScope::Category(99), 99);
    let ninth = routes.plan(TorrentScope::Category(99));
    assert_eq!(
        sessions.session(&ninth, &IDENTITY.identity()).await.err(),
        Some(SessionError::Cap)
    );

    let first = routes.plan(TorrentScope::Category(0));
    sessions.drop_route(&first.key).await;
    sessions
        .session(&ninth, &IDENTITY.identity())
        .await
        .unwrap();
    sessions.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_route_that_goes_down_holds_closed_until_it_is_back() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    let plan = routes.plan(TorrentScope::Default);
    let before = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();

    sessions
        .hold(&plan.key, RouteFailure::leg_down("link down".into()))
        .await;
    before.cancellation_token().cancelled().await;
    let view = sessions.view(&plan.key).await.unwrap();
    assert_eq!(
        view.held
            .as_ref()
            .and_then(|failure| failure.detail.as_deref()),
        Some("link down")
    );
    assert_eq!(view.port, None);

    routes.set_down(Some("link down"));
    let down = routes.plan(TorrentScope::Default);
    assert_eq!(
        sessions.session(&down, &IDENTITY.identity()).await.err(),
        Some(SessionError::RouteDown(RouteFailure::leg_down(
            "link down".into()
        )))
    );

    routes.set_down(None);
    let up = routes.plan(TorrentScope::Default);
    let after = sessions.session(&up, &IDENTITY.identity()).await.unwrap();
    assert!(!Arc::ptr_eq(&before, &after));
    assert_eq!(sessions.view(&plan.key).await.unwrap().held, None);
    assert_eq!(routes.opened.load(Ordering::SeqCst), 2);
    sessions.stop_all().await;
}

// The torrents' rate is laid over every open session, live, and a session
// that opens later takes its share at once.
#[tokio::test(flavor = "multi_thread")]
async fn the_torrent_rate_is_shared_across_the_open_sessions() {
    use weaver_torrent::limits::LimitKind;

    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.route(TorrentScope::Default, 1);
    routes.route(TorrentScope::Category(6), 2);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    let rate = |session: &Session| match session.ratelimits.get_download_bps() {
        LimitKind::Unlimited => None,
        LimitKind::Closed => Some(0),
        LimitKind::Bps(bps) => Some(bps.get()),
    };

    let one = sessions
        .session(&routes.plan(TorrentScope::Default), &IDENTITY.identity())
        .await
        .unwrap();
    assert_eq!(rate(&one), None);
    sessions.rates().set(crate::bandwidth::TorrentRates {
        download: Some(1_000_000),
        upload: None,
    });
    assert_eq!(rate(&one), Some(1_000_000));

    let two = sessions
        .session(
            &routes.plan(TorrentScope::Category(6)),
            &IDENTITY.identity(),
        )
        .await
        .unwrap();
    assert_eq!((rate(&one), rate(&two)), (Some(500_000), Some(500_000)));
    assert_eq!(sessions.rates().sample(), 0);

    // Nothing closes the sessions.
    sessions.rates().set(crate::bandwidth::TorrentRates {
        download: Some(0),
        upload: None,
    });
    assert_eq!((rate(&one), rate(&two)), (Some(0), Some(0)));
    sessions.rates().set(crate::bandwidth::TorrentRates {
        download: None,
        upload: None,
    });
    assert_eq!((rate(&one), rate(&two)), (None, None));
    sessions.stop_all().await;
}

// Two VPN legs, each its own proxy on the system egress.
// The peer limit is each torrent's: a route opened beside others gets all
// of it.
#[tokio::test(flavor = "multi_thread")]
async fn every_session_gets_the_whole_peer_limit() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.route(TorrentScope::Default, 1);
    routes.route(TorrentScope::Category(5), 2);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(TorrentConfig {
        max_peers_total: 400,
        ..TorrentConfig::default()
    });
    for scope in [TorrentScope::Default, TorrentScope::Category(5)] {
        sessions
            .session(&routes.plan(scope), &IDENTITY.identity())
            .await
            .unwrap();
    }
    assert_eq!(*routes.peers.lock().unwrap(), [400, 400]);
}

// A leg whose DHT socket, the one bound on the route's port, logs when it is
// bound and when it goes. It listens on nothing that accepts and resolves no
// name, so the session reaches no one.
struct LoggedDht {
    port: u16,
    log: Arc<Mutex<Vec<&'static str>>>,
}

struct Logged {
    inner: tokio::net::UdpSocket,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Drop for Logged {
    fn drop(&mut self) {
        self.log.lock().unwrap().push("dropped");
    }
}

impl DatagramSocket for Logged {
    fn bind_addr(&self) -> SocketAddr {
        self.inner.bind_addr()
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        DatagramSocket::recv_from(&self.inner, buf)
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        DatagramSocket::send_to(&self.inner, buf, target)
    }

    fn poll_send_to(
        &self,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> std::task::Poll<io::Result<usize>> {
        DatagramSocket::poll_send_to(&self.inner, cx, buf, target)
    }
}

struct Quiet(SocketAddr);

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

impl SocketFactory for LoggedDht {
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
        Box::pin(async move {
            if addr.port() != self.port {
                return Ok(Arc::new(NoDatagrams::new(addr)) as Arc<dyn DatagramSocket>);
            }
            let inner = tokio::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
            self.log.lock().unwrap().push("bound");
            Ok(Arc::new(Logged {
                inner,
                log: Arc::clone(&self.log),
            }) as Arc<dyn DatagramSocket>)
        })
    }
}

// A route closed and opened again binds its DHT socket only once the closed
// session's is gone, so the new one can take the same port.
#[tokio::test(flavor = "multi_thread")]
async fn a_closed_route_lets_go_of_its_dht_socket_before_it_opens_again() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let base_port = 40_000;
    let log = Arc::new(Mutex::new(Vec::new()));
    let kind = LegKind::SourceAddress("127.0.0.1".parse().unwrap());
    routes.set_leg(
        kind,
        Arc::new(LoggedDht {
            port: base_port,
            log: Arc::clone(&log),
        }),
    );
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(TorrentConfig {
        base_port,
        ..any_port()
    });
    let plan = routes.plan(TorrentScope::Default);

    let before = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(before.get_dht().is_some());
    drop(before);
    sessions.drop_route(&plan.key).await;
    let after = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(after.get_dht().is_some());
    assert_eq!(*log.lock().unwrap(), ["bound", "dropped", "bound"]);
    sessions.stop_all().await;
    assert_eq!(
        *log.lock().unwrap(),
        ["bound", "dropped", "bound", "dropped"]
    );
}

// A leg whose sockets reach no one and bind nothing on the host: its
// listener holds the address asked for, so a session on it shows the port
// the settings give without taking that port.
pub(crate) struct Unbound;

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
pub(crate) fn unbound_leg(routes: &FakeRoutes) {
    routes.set_leg(
        LegKind::SourceAddress("127.0.0.1".parse().unwrap()),
        Arc::new(Unbound),
    );
}

fn unbound_sessions(dir: &Path, torrent: TorrentConfig) -> (Arc<FakeRoutes>, Sessions) {
    let routes = Arc::new(FakeRoutes::default());
    unbound_leg(&routes);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(torrent);
    (routes, sessions)
}

// A listen port given while a route is open is the port it opens on again:
// the open session is outdated until it does, and its next session listens
// there.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_listen_port_reopens_the_route_on_it() {
    let dir = tempfile::tempdir().unwrap();
    let first = TorrentConfig {
        base_port: 40_000,
        ..TorrentConfig::default()
    };
    let (routes, sessions) = unbound_sessions(dir.path(), first.clone());
    let plan = routes.plan(TorrentScope::Default);
    let before = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    let view = sessions.view(&plan.key).await.unwrap();
    assert_eq!(view.listen.map(|listen| listen.port()), Some(40_000));
    assert!(sessions.outdated().await.is_empty());

    sessions.set_config(TorrentConfig {
        base_port: 41_000,
        ..first
    });
    assert_eq!(
        sessions.outdated().await,
        BTreeSet::from([plan.key.clone()])
    );
    sessions.drop_route(&plan.key).await;
    let after = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(!Arc::ptr_eq(&before, &after));
    let view = sessions.view(&plan.key).await.unwrap();
    assert_eq!(view.port, Some(41_000));
    assert_eq!(view.listen.map(|listen| listen.port()), Some(41_000));
    assert_eq!(view.listen, after.listen_addr());
    assert!(sessions.outdated().await.is_empty());
    sessions.stop_all().await;
}

// An announce port given while a route is open is the one its next session
// announces.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_announce_port_reaches_the_next_announce() {
    let dir = tempfile::tempdir().unwrap();
    let first = TorrentConfig {
        announce_port: Some(51_413),
        ..TorrentConfig::default()
    };
    let (routes, sessions) = unbound_sessions(dir.path(), first.clone());
    let plan = routes.plan(TorrentScope::Default);
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert_eq!(session.announce_port(), Some(51_413));

    sessions.set_config(TorrentConfig {
        announce_port: Some(51_414),
        ..first
    });
    assert_eq!(
        sessions.outdated().await,
        BTreeSet::from([plan.key.clone()])
    );
    sessions.drop_route(&plan.key).await;
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert_eq!(session.announce_port(), Some(51_414));

    sessions.set_config(TorrentConfig::default());
    assert_eq!(
        sessions.outdated().await,
        BTreeSet::from([plan.key.clone()])
    );
    sessions.drop_route(&plan.key).await;
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert_eq!(session.announce_port(), None);
    sessions.stop_all().await;
}

// Settings that change nothing a session opens with leave it open.
#[tokio::test(flavor = "multi_thread")]
async fn other_settings_leave_the_session_open() {
    let dir = tempfile::tempdir().unwrap();
    let (routes, sessions) = unbound_sessions(dir.path(), TorrentConfig::default());
    let plan = routes.plan(TorrentScope::Default);
    let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();

    sessions.set_config(TorrentConfig {
        connection_speed: speed(9),
        max_peers_total: 12,
        ..TorrentConfig::default()
    });
    sessions.settled().await;
    assert!(sessions.outdated().await.is_empty());
    assert!(Arc::ptr_eq(
        &session,
        &sessions.session(&plan, &IDENTITY.identity()).await.unwrap()
    ));
    assert!(!session.cancellation_token().is_cancelled());
    sessions.stop_all().await;
}

// Port mapping is what a session opens with on a leg that maps its port,
// and nothing on one that does not.
#[test]
fn port_mapping_is_what_a_mapping_leg_opens_with() {
    let dir = tempfile::tempdir().unwrap();
    let env = env(dir.path());
    let off = TorrentConfig {
        upnp: false,
        ..TorrentConfig::default()
    };
    let on = TorrentConfig {
        upnp: true,
        ..off.clone()
    };
    let opening =
        |torrent: &TorrentConfig, kind: LegKind| Opening::of(&env, torrent, kind.shape(None));
    assert!(opening(&on, LegKind::System).upnp);
    assert!(!opening(&off, LegKind::System).upnp);
    assert_ne!(
        opening(&off, LegKind::System),
        opening(&on, LegKind::System)
    );
    for kind in [
        LegKind::SourceAddress("127.0.0.1".parse().unwrap()),
        LegKind::Interface("en1".into()),
    ] {
        assert!(opening(&on, kind.clone()).upnp, "{kind:?}");
        assert!(!opening(&off, kind.clone()).upnp, "{kind:?}");
    }
    assert_eq!(
        opening(&off, LegKind::Ladder),
        opening(&on, LegKind::Ladder)
    );
}

// The peer protocol is what a session opens with: a route open with one
// is outdated once the settings give another, and its next session listens
// with the new one.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_peer_protocol_reopens_the_route_with_it() {
    use crate::settings::TorrentPeerProtocol;

    let dir = tempfile::tempdir().unwrap();
    let both = TorrentConfig {
        peer_protocol: TorrentPeerProtocol::Both,
        ..any_port()
    };
    let routes = Arc::new(FakeRoutes::default());
    routes
        .no_local_discovery
        .store(true, std::sync::atomic::Ordering::SeqCst);
    loopback_leg(&routes);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(both.clone());
    let plan = routes.plan(TorrentScope::Default);
    sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(sessions.view(&plan.key).await.unwrap().utp);
    assert!(sessions.outdated().await.is_empty());

    sessions.set_config(TorrentConfig {
        peer_protocol: TorrentPeerProtocol::Tcp,
        ..both
    });
    assert_eq!(
        sessions.outdated().await,
        BTreeSet::from([plan.key.clone()])
    );
    sessions.drop_route(&plan.key).await;
    sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(!sessions.view(&plan.key).await.unwrap().utp);
    assert!(sessions.outdated().await.is_empty());
    sessions.stop_all().await;
}

// A session directory given while a route is open is where its next
// session keeps its state.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_session_directory_reopens_the_route_there() {
    let dir = tempfile::tempdir().unwrap();
    let (routes, sessions) = unbound_sessions(dir.path(), TorrentConfig::default());
    let plan = routes.plan(TorrentScope::Default);
    sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(dir.path().join(plan.key.as_str()).is_dir());

    let elsewhere = dir.path().join("elsewhere");
    sessions.set_config(TorrentConfig {
        session_dir: Some(elsewhere.clone()),
        ..TorrentConfig::default()
    });
    assert_eq!(
        sessions.outdated().await,
        BTreeSet::from([plan.key.clone()])
    );
    sessions.drop_route(&plan.key).await;
    sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    assert!(elsewhere.join(plan.key.as_str()).is_dir());
    assert!(sessions.outdated().await.is_empty());
    sessions.stop_all().await;
}

// A torrent's trackers hear `stopped` only from a session that may still
// send: one dropped with its leg standing and its route up says it, one
// held for its route going down, whose leg was revoked under it, or whose
// leg is out of quota does not.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_sends_stopped_only_while_its_leg_stands_and_its_route_is_up() {
    let tracker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", tracker.local_addr().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network) = on_network(&routes);
    loopback_leg(&routes);
    let sessions = Sessions::new(
        routes.clone(),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    // A day: a stopped announce waits on the tracker, not on its timeout.
    sessions.set_config(TorrentConfig {
        stop_tracker_timeout_secs: 86_400,
        ..any_port()
    });
    let plan = routes.plan(TorrentScope::Default);
    // The route's session, running a torrent its tracker took `started` from.
    let announced = || async {
        let session = sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
        // Each session in turn writes the one file.
        let added = session
            .add_torrent(
                weaver_torrent::AddTorrent::from_bytes(announcing(&url)),
                Some(weaver_torrent::AddTorrentOptions {
                    overwrite: true,
                    ..operators(&url)
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        let (started, line) = heard_announce(&tracker).await;
        assert!(line.contains("event=started"), "{line}");
        answer(started).await;
        added
            .tracker_status()
            .expect("a live run with trackers")
            .subscribe()
            .wait_for(|announces| {
                announces[0]
                    .last
                    .as_ref()
                    .is_some_and(|last| last.result.is_ok())
            })
            .await
            .unwrap();
    };

    // Dropped, its leg standing and its route up: `stopped`.
    announced().await;
    sessions.drop_route(&plan.key).await;
    let (stopped, line) = heard_announce(&tracker).await;
    assert!(line.contains("event=stopped"), "{line}");
    answer(stopped).await;
    sessions.closing.all_stopped().await;

    // Held, its route down: nothing. The next request the tracker hears is
    // the next session's `started`.
    announced().await;
    sessions
        .hold(&plan.key, RouteFailure::leg_down("down".into()))
        .await;
    sessions.closing.all_stopped().await;

    // Its leg revoked under it: nothing, from its own sockets either.
    announced().await;
    save_again(&db, &network).await;
    assert_eq!(
        sessions.outdated().await,
        BTreeSet::from([plan.key.clone()])
    );
    sessions.drop_route(&plan.key).await;
    sessions.closing.all_stopped().await;

    // Its leg out of quota: nothing, though the leg stands and the session
    // is not outdated.
    announced().await;
    routes.spend(1);
    assert!(sessions.outdated().await.is_empty());
    sessions.drop_route(&plan.key).await;
    sessions.closing.all_stopped().await;

    let tracker = tracker.into_std().unwrap();
    tracker.set_nonblocking(true).unwrap();
    let asked = tracker.accept();
    assert!(
        matches!(&asked, Err(error) if error.kind() == io::ErrorKind::WouldBlock),
        "the tracker heard more: {asked:?}"
    );
    sessions.stop_all().await;
    network.shutdown().await;
}

// The sockets a leg's factory opened, counted, opened by that factory.
struct Counted {
    sockets: Arc<dyn SocketFactory>,
    listened: AtomicUsize,
    bound: AtomicUsize,
}

impl SocketFactory for Counted {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        self.sockets.connect_tcp(addr)
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.listened.fetch_add(1, Ordering::SeqCst);
        self.sockets.listen_tcp(addr)
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.bound.fetch_add(1, Ordering::SeqCst);
        self.sockets.bind_udp(addr)
    }
}

// A session on a leg of the host's own sockets, the System leg or one bound
// to an interface, opens its listener, its DHT socket and its UDP trackers'
// socket through its leg's factory, as a session on every other leg does:
// the engine opens no socket of its own.
#[tokio::test(flavor = "multi_thread")]
async fn an_own_socket_legs_session_opens_its_sockets_through_the_leg() {
    use crate::torrents::net::{LOOPBACK_DEVICE, system_dialer};

    let torrent = TorrentConfig {
        upnp: false,
        dht_bootstrap_nodes: String::new(),
        peer_protocol: crate::settings::TorrentPeerProtocol::Tcp,
        ..TorrentConfig::default()
    };
    for kind in [LegKind::System, LegKind::Interface(LOOPBACK_DEVICE.into())] {
        let dir = tempfile::tempdir().unwrap();
        let mut leg = OpenLeg::on(kind.clone(), system_dialer(), None, None).unwrap();
        let counted = Arc::new(Counted {
            sockets: Arc::clone(&leg.factory),
            listened: AtomicUsize::new(0),
            bound: AtomicUsize::new(0),
        });
        leg.factory = counted.clone();
        let options = session_options(
            &leg,
            0,
            dir.path(),
            8,
            &torrent,
            IpFilter::default(),
            &IDENTITY.identity(),
        );
        assert!(options.dht.is_some(), "{kind:?}");
        let session = Session::new_with_opts(dir.path().join("output"), options)
            .await
            .unwrap();
        // One listener; one socket for the DHT, one for the UDP trackers.
        assert_eq!(counted.listened.load(Ordering::SeqCst), 1, "{kind:?}");
        assert_eq!(counted.bound.load(Ordering::SeqCst), 2, "{kind:?}");
        session.stop().await;
    }
}

// A session on the host's own sockets looks its names up as every other leg
// does, through its leg's dialer: the lookup lands in the resolution record
// of the task that asked, made by the host's DNS.
#[tokio::test(flavor = "multi_thread")]
async fn a_system_leg_session_looks_names_up_on_the_record() {
    use weaver_torrent::network::Resolved;
    use weaver_tunnel::resolution::record::{Recording, ResolvedBy};

    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes = crate::torrents::sessions::NetworkRoutes::new(proxies, Duration::from_secs(30));
    let plan = routes.plan(TorrentScope::Default);
    assert_eq!(plan.kind, LegKind::System);
    let leg = routes.open(&plan, 8).await.unwrap();
    let options = session_options(
        &leg,
        0,
        dir.path(),
        8,
        &TorrentConfig::default(),
        IpFilter::default(),
        &IDENTITY.identity(),
    );
    let names = options.resolver.expect("the session resolves on its leg");
    assert!(Arc::ptr_eq(&names, &leg.names));

    let recording = Arc::new(Recording::default());
    let resolved = recording
        .scope(names.resolve("localhost", 6969))
        .await
        .unwrap();
    let Resolved::Addresses(found) = resolved else {
        panic!("the host's DNS handed the name on");
    };
    let noted = recording
        .finish("localhost", 6969, None)
        .expect("the lookup was recorded");
    assert_eq!(noted.by, ResolvedBy::Host);
    // The leg's egress hands its addresses on in its own order.
    assert_eq!(
        noted.addresses.iter().copied().collect::<BTreeSet<_>>(),
        found.iter().map(SocketAddr::ip).collect::<BTreeSet<_>>()
    );
}

// A route says who resolves its sessions' names: the host's DNS on the
// System leg, the exit proxy's naming on a ladder, here a proxy handed the
// names.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_plans_who_resolves_its_names() {
    let db = Database::open_in_memory().unwrap();
    db.save_proxy_profile(&loopback_proxy(1)).unwrap();
    let category = TorrentScope::Category(3);
    db.save_proxy_routing_policy(
        Consumer::Torrent(category),
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
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes = crate::torrents::sessions::NetworkRoutes::new(proxies, Duration::from_secs(30));
    let direct = routes.plan(TorrentScope::Default);
    assert_eq!(direct.kind, LegKind::System);
    assert_eq!(direct.names, Some(ResolvedBy::Host));
    let laddered = routes.plan(category);
    assert_eq!(laddered.kind, LegKind::Ladder);
    assert_eq!(laddered.names, Some(ResolvedBy::Proxy));
}

// No limit leaves twenty files and a fifth of the rest to files, never
// fewer than five connections (libtorrent `session_impl`).
#[test]
fn unlimited_connections_leave_room_for_files() {
    assert_eq!(super::connections_within(1_024), 803);
    assert_eq!(super::connections_within(256), 188);
    assert_eq!(super::connections_within(10), 5);
}

// A session that counts the times it is asked to join local service
// discovery again, and fails each one when told to.
#[derive(Default)]
struct Rejoining {
    asked: AtomicUsize,
    fails: bool,
}

impl LocalDiscovery for Rejoining {
    fn rejoin_local_discovery(&self) -> anyhow::Result<()> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        if self.fails {
            anyhow::bail!("no interface to join on");
        }
        Ok(())
    }
}

// Only a session on its own sockets runs local service discovery, as its
// options say, and only such a session is asked to join again; one whose
// join fails leaves the others to theirs.
#[tokio::test]
async fn only_sessions_that_run_local_discovery_are_asked_to_rejoin() {
    let address: IpAddr = "192.0.2.4".parse().unwrap();
    let kinds = [
        (LegKind::Interface("en1".into()), true),
        (LegKind::System, false),
        (LegKind::SourceAddress(address), false),
        (LegKind::Ladder, false),
    ];
    let sessions: Vec<_> = kinds
        .iter()
        .map(|(_, fails)| {
            Arc::new(Rejoining {
                fails: *fails,
                ..Rejoining::default()
            })
        })
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let open = kinds
        .iter()
        .zip(&sessions)
        .zip(1..)
        .map(|(((kind, _), session), proxy)| {
            let shape = kind.shape(None);
            let options = session_options(
                &leg(kind.clone()),
                6881,
                dir.path(),
                8,
                &TorrentConfig::default(),
                IpFilter::default(),
                &IDENTITY.identity(),
            );
            assert_eq!(
                runs_local_discovery(shape),
                !options.disable_local_service_discovery,
                "{kind:?}"
            );
            (proxy_key(proxy), shape, Arc::clone(session))
        })
        .collect();
    rejoin_local_discovery(open).await;
    let asked: Vec<_> = sessions
        .iter()
        .map(|session| session.asked.load(Ordering::SeqCst))
        .collect();
    assert_eq!(asked, [1, 1, 0, 0], "interface, system, source, ladder");
}

// Changes that ring before the follower runs make one rejoin, not one
// each: three sets, then the sender gone, and the follower runs once.
#[tokio::test]
async fn several_interface_changes_before_the_follower_runs_make_one_rejoin() {
    let (changes, follower) = tokio::sync::watch::channel(0u32);
    for set in 1..=3 {
        changes.send_replace(set);
    }
    drop(changes);
    let seen = Mutex::new(Vec::new());
    each_change(follower.clone(), || {
        seen.lock().unwrap().push(*follower.borrow());
        std::future::ready(())
    })
    .await;
    assert_eq!(*seen.lock().unwrap(), [3], "one run, on the last set");
}
