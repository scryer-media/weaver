// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use librqbit::StreamListener;
use librqbit::network::{BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket};

use super::*;
use crate::proxies::{Rung, TorrentScope};
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

// Routes whose legs are ladders that reach no one, unless a leg of another
// kind is given. Each scope maps to a proxy id, so scopes with the same id
// share a route; a leg can be set down, or refuse to open.
#[derive(Default)]
pub(crate) struct FakeRoutes {
    pub(crate) proxies: Mutex<BTreeMap<String, u32>>,
    pub(crate) down: Mutex<Option<String>>,
    pub(crate) refuse: Mutex<Option<String>>,
    pub(crate) leg: Mutex<Option<(LegKind, Arc<dyn SocketFactory>)>>,
    pub(crate) opened: AtomicUsize,
}

fn scope_name(scope: TorrentScope) -> String {
    Consumer::Torrent(scope).key()
}

impl FakeRoutes {
    pub(crate) fn route(&self, scope: TorrentScope, proxy: u32) {
        self.proxies
            .lock()
            .unwrap()
            .insert(scope_name(scope), proxy);
    }

    pub(crate) fn set_down(&self, reason: Option<&str>) {
        *self.down.lock().unwrap() = reason.map(str::to_owned);
    }

    pub(crate) fn set_refused(&self, error: Option<&str>) {
        *self.refuse.lock().unwrap() = error.map(str::to_owned);
    }

    pub(crate) fn set_leg(&self, kind: LegKind, factory: Arc<dyn SocketFactory>) {
        *self.leg.lock().unwrap() = Some((kind, factory));
    }
}

#[async_trait::async_trait]
impl Routes for FakeRoutes {
    fn plan(&self, scope: TorrentScope) -> Result<RoutePlan, String> {
        let proxy = self
            .proxies
            .lock()
            .unwrap()
            .get(&scope_name(scope))
            .copied()
            .unwrap_or(0);
        let path = ladder(proxy);
        Ok(RoutePlan {
            scope,
            policy: RoutingPolicy::default(),
            egress_id: SYSTEM_EGRESS_ID,
            kind: self
                .leg
                .lock()
                .unwrap()
                .as_ref()
                .map_or(LegKind::Ladder, |(kind, _)| kind.clone()),
            key: RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &path),
            down: self.down.lock().unwrap().clone(),
        })
    }

    async fn open(&self, plan: &RoutePlan) -> Result<OpenLeg, String> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = self.refuse.lock().unwrap().clone() {
            return Err(error);
        }
        let factory = self.leg.lock().unwrap().as_ref().map_or_else(
            || Arc::new(Nowhere) as Arc<dyn SocketFactory>,
            |(_, factory)| Arc::clone(factory),
        );
        Ok(OpenLeg {
            kind: plan.kind.clone(),
            factory: Some(factory),
            http: tracker_client(&LegKind::System, None, &IDENTITY)?,
            leg: None,
        })
    }
}

pub(crate) fn env(root: &Path) -> SessionEnv {
    SessionEnv {
        root: root.to_path_buf(),
        base_port: 0,
        announce_port: None,
        upnp: false,
        max_peers_total: 400,
        identity: IDENTITY,
    }
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
    let first = routes.plan(TorrentScope::Default).unwrap();
    let second = routes.plan(TorrentScope::Category(5)).unwrap();
    sessions.session(&first).await.unwrap();
    sessions.session(&second).await.unwrap();
    let port = |key: &RouteKey| port_slot(&db, key).unwrap();
    let (one, two) = (port(&first.key), port(&second.key));
    assert_ne!(one, two);

    sessions.drop_route(&first.key).await;
    sessions
        .forget_routes([first.key.clone(), second.key.clone()].into())
        .await;
    sessions.session(&first).await.unwrap();
    assert_eq!(sessions.view(&first.key).await.unwrap().port, Some(one));

    sessions.drop_route(&first.key).await;
    sessions.forget_routes([second.key.clone()].into()).await;
    let third = RouteKey::of(0, &EgressBinding::System, &ladder(9));
    assert_eq!(port(&third), one);
    assert_eq!(port(&second.key), two);
    sessions.stop_all().await;
}

#[test]
fn mapped_ports_are_kept_until_the_route_is_gone() {
    let db = Database::open_in_memory().unwrap();
    let first = RouteKey::of(0, &EgressBinding::System, &ladder(1));
    let second = RouteKey::of(0, &EgressBinding::System, &ladder(2));
    assert_eq!(mapped_port(&db, &first).unwrap(), None);
    set_mapped_port(&db, &first, 40_001).unwrap();
    set_mapped_port(&db, &second, 40_002).unwrap();
    set_mapped_port(&db, &first, 40_003).unwrap();
    assert_eq!(mapped_port(&db, &first).unwrap(), Some(40_003));
    release_mapped(&db, |key| key == first.as_str()).unwrap();
    assert_eq!(mapped_port(&db, &first).unwrap(), None);
    assert_eq!(mapped_port(&db, &second).unwrap(), Some(40_002));
}

#[test]
fn a_route_without_a_slot_listens_on_the_first_slots_port() {
    let db = Database::open_in_memory().unwrap();
    let first = RouteKey::of(0, &EgressBinding::System, &ladder(1));
    let second = RouteKey::of(0, &EgressBinding::System, &ladder(2));
    assert_eq!(listen_port(&db, 6881, &second).unwrap(), 6881);
    port_slot(&db, &first).unwrap();
    port_slot(&db, &second).unwrap();
    assert_eq!(listen_port(&db, 6881, &second).unwrap(), 6882);
}

// A port a gateway mapped to a route is what its session announces, over
// the configured announce port, from the next time the session opens.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_announces_the_port_mapped_to_it() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.route(TorrentScope::Default, 1);
    let kind = LegKind::SourceAddress("127.0.0.1".parse().unwrap());
    let loopback = Arc::new(weaver_tunnel::pipe::Egress::new(
        4,
        weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
        std::time::Duration::MAX,
    ));
    routes.set_leg(kind.clone(), Arc::new(LegSockets::new(loopback, &kind)));
    let db = Database::open_in_memory().unwrap();
    let mut env = env(dir.path());
    env.announce_port = Some(51_413);
    let sessions = Sessions::new(routes.clone(), env, db.clone());
    let plan = routes.plan(TorrentScope::Default).unwrap();

    let session = sessions.session(&plan).await.unwrap();
    assert_eq!(session.announce_port(), Some(51_413));
    set_mapped_port(&db, &plan.key, 40_001).unwrap();
    sessions.drop_route(&plan.key).await;
    let session = sessions.session(&plan).await.unwrap();
    assert_eq!(session.announce_port(), Some(40_001));

    sessions.forget_routes(BTreeSet::new()).await;
    assert_eq!(mapped_port(&db, &plan.key).unwrap(), None);
    sessions.stop_all().await;
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

#[tokio::test(flavor = "multi_thread")]
async fn the_encryption_setting_reaches_the_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let sessions = Sessions::new(
        Arc::new(FakeRoutes::default()),
        env(dir.path()),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_encryption(crate::settings::TorrentEncryption::Require);
    assert_eq!(
        sessions.encryption(),
        crate::settings::TorrentEncryption::Require
    );
}

fn leg(kind: LegKind) -> OpenLeg {
    OpenLeg {
        kind,
        factory: None,
        http: tracker_client(&LegKind::System, None, &IDENTITY).unwrap(),
        leg: None,
    }
}

#[test]
fn session_options_follow_the_leg() {
    let dir = tempfile::tempdir().unwrap();
    let mut env = env(dir.path());
    env.upnp = true;
    env.announce_port = Some(51413);

    let system = session_options(
        &env,
        &leg(LegKind::System),
        6881,
        dir.path(),
        100,
        Default::default(),
    );
    assert!(system.dht.is_some());
    let listen = system.listen.as_ref().unwrap();
    assert!(listen.enable_upnp_port_forwarding);
    assert_eq!(listen.listen_addr.port(), 6881);
    assert_eq!(listen.announce_port, Some(51413));
    assert!(!system.disable_local_service_discovery);
    assert_eq!(&system.peer_id.unwrap().0[..8], IDENTITY.prefix());
    assert_eq!(
        system.client_name_and_version.as_deref(),
        Some(IDENTITY.user_agent())
    );
    assert!(system.http_client.is_some());
    assert_eq!(system.connect.as_ref().unwrap().proxy_url, None);

    let interface = session_options(
        &env,
        &leg(LegKind::Interface("en1".into())),
        6881,
        dir.path(),
        100,
        Default::default(),
    );
    assert!(interface.dht.is_some());
    assert_eq!(interface.bind_device_name.as_deref(), Some("en1"));
    assert!(!interface.listen.unwrap().enable_upnp_port_forwarding);

    let address: IpAddr = "192.0.2.4".parse().unwrap();
    let source = session_options(
        &env,
        &leg(LegKind::SourceAddress(address)),
        6882,
        dir.path(),
        100,
        Default::default(),
    );
    assert!(source.dht.is_some());
    assert_eq!(
        source.listen.unwrap().listen_addr,
        SocketAddr::new(address, 6882)
    );
    assert!(source.disable_local_service_discovery);

    let ladder = session_options(
        &env,
        &leg(LegKind::Ladder),
        6883,
        dir.path(),
        100,
        Default::default(),
    );
    assert!(ladder.dht.is_none());
    assert!(ladder.listen.is_none());
    assert!(ladder.disable_local_service_discovery);
    assert_eq!(ladder.peer_limit, Some(100));
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

    let default = routes.plan(TorrentScope::Default).unwrap();
    let five = routes.plan(TorrentScope::Category(5)).unwrap();
    let six = routes.plan(TorrentScope::Category(6)).unwrap();
    assert_eq!(default.key, five.key);
    assert_ne!(default.key, six.key);

    let a = sessions.session(&default).await.unwrap();
    let b = sessions.session(&five).await.unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    sessions.session(&six).await.unwrap();
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
        let plan = routes.plan(TorrentScope::Category(id)).unwrap();
        sessions.session(&plan).await.unwrap();
    }
    routes.route(TorrentScope::Category(99), 99);
    let ninth = routes.plan(TorrentScope::Category(99)).unwrap();
    assert_eq!(
        sessions.session(&ninth).await.err(),
        Some(SessionError::Cap)
    );

    let first = routes.plan(TorrentScope::Category(0)).unwrap();
    sessions.drop_route(&first.key).await;
    sessions.session(&ninth).await.unwrap();
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
    let plan = routes.plan(TorrentScope::Default).unwrap();
    let before = sessions.session(&plan).await.unwrap();

    sessions.hold(&plan.key, "link down".into()).await;
    assert!(before.cancellation_token().is_cancelled());
    let view = sessions.view(&plan.key).await.unwrap();
    assert_eq!(view.held.as_deref(), Some("link down"));
    assert_eq!(view.port, None);

    routes.set_down(Some("link down"));
    let down = routes.plan(TorrentScope::Default).unwrap();
    assert_eq!(
        sessions.session(&down).await.err(),
        Some(SessionError::RouteDown("link down".into()))
    );

    routes.set_down(None);
    let up = routes.plan(TorrentScope::Default).unwrap();
    let after = sessions.session(&up).await.unwrap();
    assert!(!Arc::ptr_eq(&before, &after));
    assert_eq!(sessions.view(&plan.key).await.unwrap().held, None);
    assert_eq!(routes.opened.load(Ordering::SeqCst), 2);
    sessions.stop_all().await;
}

// The torrents' rate is laid over every open session, live, and a session
// that opens later takes its share at once.
#[tokio::test(flavor = "multi_thread")]
async fn the_torrent_rate_is_shared_across_the_open_sessions() {
    use librqbit::limits::LimitKind;

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
        .session(&routes.plan(TorrentScope::Default).unwrap())
        .await
        .unwrap();
    assert_eq!(rate(&one), None);
    sessions.rates().set(crate::bandwidth::TorrentRates {
        download: Some(1_000_000),
        upload: None,
    });
    assert_eq!(rate(&one), Some(1_000_000));

    let two = sessions
        .session(&routes.plan(TorrentScope::Category(6)).unwrap())
        .await
        .unwrap();
    assert_eq!((rate(&one), rate(&two)), (Some(500_000), Some(500_000)));
    assert_eq!(sessions.rates().download_bps(), 0);

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
