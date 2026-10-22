// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! One torrent session per distinct route. Categories whose torrents resolve
//! to the same leg share its session; a session lives while its leg is up and
//! holds closed while it is down.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
pub use weaver_torrent::IpRanges;
use weaver_torrent::dht::DhtPersistenceConfig;
use weaver_torrent::{
    ConnectionOptions, DhtSessionConfig, HttpClients, IpFilter, ListenerMode, ListenerOptions,
    PeerConnectionOptions, Session, SessionLimits, SessionOptions, SocketFactory,
};

use crate::StateError;
use crate::persistence::Database;
use crate::proxies::{
    Consumer, EgressBinding, EgressHealth, EgressInterface, FetchOptions, LegPath, NetworkRuntime,
    ProxyRuntime, RouteLeg, RoutingPolicy, SYSTEM_EGRESS_ID, TorrentLeg, TorrentScope,
    routed_fetch,
};
use crate::settings::{BlocklistSource, TorrentConfig, TorrentEncryption, TorrentLimit};
use crate::torrents::identity::ClientIdentity;
use crate::torrents::net::{LegKind, LegSockets, Shape, tracker_client};
use crate::torrents::rate::SessionRates;

/// The most sessions that run at once.
pub const MAX_SESSIONS: usize = 8;

/// How long a peer may send nothing before it is dropped. A seed that
/// unchokes and then sends only the odd HAVE is still a seed.
pub const PEER_TIMEOUT: Duration = Duration::from_secs(120);

const SLOTS_KEY: &str = "torrent.session_slots";
const MAPPED_KEY: &str = "torrent.mapped_ports";

/// A resolved route's identity: the leg it dials over.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RouteKey(String);

impl RouteKey {
    pub fn of(egress_id: u32, binding: &EgressBinding, path: &LegPath) -> Self {
        let encoded =
            serde_json::to_vec(&(egress_id, binding, path)).expect("route legs always encode");
        Self(hex::encode(&Sha1::digest(&encoded)[..8]))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The egress a leg names: its configured one, the system egress, or none.
fn leg_egress(network: &NetworkRuntime, egress_id: u32) -> Option<EgressInterface> {
    network
        .egress_configuration(egress_id)
        .or_else(|| (egress_id == SYSTEM_EGRESS_ID).then(EgressInterface::system))
}

/// The key of the session a route through `egress_id` and `path` opens.
pub fn leg_key(network: &NetworkRuntime, egress_id: u32, path: &LegPath) -> RouteKey {
    match leg_egress(network, egress_id) {
        Some(egress) => RouteKey::of(egress.id, &egress.binding, path),
        None => RouteKey::of(egress_id, &EgressBinding::System, path),
    }
}

/// The leg a route runs on: the first that is up, else the first, with why
/// it is down. `down` says why a leg is down, when it is.
pub fn first_up(
    legs: &[RouteLeg],
    down: impl Fn(&RouteLeg) -> Option<String>,
) -> Option<(usize, Option<String>)> {
    let first = down(legs.first()?);
    if first.is_none() {
        return Some((0, None));
    }
    Some(
        (1..legs.len())
            .find(|position| down(&legs[*position]).is_none())
            .map_or((0, first), |position| (position, None)),
    )
}

/// A torrent scope's route as it stands: the leg it runs on, and whether that
/// leg is up.
#[derive(Clone, Debug)]
pub struct RoutePlan {
    pub scope: TorrentScope,
    pub policy: RoutingPolicy,
    /// The route's leg it runs on, by position.
    pub leg: usize,
    pub egress_id: u32,
    pub kind: LegKind,
    pub key: RouteKey,
    /// Why the leg is down, when it is.
    pub down: Option<String>,
}

/// A leg made ready for a session.
pub struct OpenLeg {
    pub kind: LegKind,
    pub shape: Shape,
    pub factory: Option<Arc<dyn SocketFactory>>,
    /// The session's HTTP clients, for its trackers and web seeds.
    pub http: Arc<dyn HttpClients>,
    /// The route's leg the factory dials through, kept while the session
    /// lives.
    pub leg: Option<Arc<TorrentLeg>>,
}

/// Where routes come from.
#[async_trait::async_trait]
pub trait Routes: Send + Sync {
    fn plan(&self, scope: TorrentScope) -> Result<RoutePlan, String>;
    /// Open `plan`'s leg for a session of at most `peers` peers.
    async fn open(&self, plan: &RoutePlan, peers: usize) -> Result<OpenLeg, String>;
    /// The blocklist at `url`, which every route's session shares.
    async fn fetch_blocklist(&self, url: &reqwest::Url) -> Result<IpRanges, String>;
}

/// Routes as the network runtime resolves them.
pub struct NetworkRoutes {
    db: Database,
    proxies: Arc<ProxyRuntime>,
    timeout: Duration,
    identity: ClientIdentity,
}

impl NetworkRoutes {
    pub fn new(
        db: Database,
        proxies: Arc<ProxyRuntime>,
        timeout: Duration,
        identity: ClientIdentity,
    ) -> Self {
        Self {
            db,
            proxies,
            timeout,
            identity,
        }
    }

    fn network(&self) -> &NetworkRuntime {
        &self.proxies.network
    }
}

#[async_trait::async_trait]
impl Routes for NetworkRoutes {
    fn plan(&self, scope: TorrentScope) -> Result<RoutePlan, String> {
        let consumer = Consumer::Torrent(scope);
        let policy = self
            .db
            .proxy_routing_policy(consumer)
            .map_err(|error| error.to_string())?;
        let route = policy.route();
        consumer
            .validate_route(&route)
            .map_err(|refusal| refusal.to_string())?;
        let interfaces = self.network().interfaces();
        let quota_blocked = self.network().quota_blocked_egresses();
        let (position, down) = first_up(&route.legs, |leg| {
            let Some(egress) = leg_egress(self.network(), leg.egress_id) else {
                return Some(format!("egress {} is missing", leg.egress_id));
            };
            match interfaces.health(&egress, None) {
                EgressHealth::Down(reason) => Some(reason),
                _ if quota_blocked.contains(&egress.id) => Some("quota reached".into()),
                _ => None,
            }
        })
        .ok_or("the route has no leg")?;
        let leg = &route.legs[position];
        let egress = leg_egress(self.network(), leg.egress_id);
        Ok(RoutePlan {
            scope,
            leg: position,
            egress_id: egress.as_ref().map_or(leg.egress_id, |egress| egress.id),
            kind: egress.as_ref().map_or(LegKind::System, |egress| {
                LegKind::of(&egress.binding, &leg.path)
            }),
            key: leg_key(self.network(), leg.egress_id, &leg.path),
            policy,
            down,
        })
    }

    async fn open(&self, plan: &RoutePlan, peers: usize) -> Result<OpenLeg, String> {
        if plan.kind.own_sockets() {
            return Ok(OpenLeg {
                kind: plan.kind.clone(),
                shape: plan.kind.shape(None),
                factory: None,
                http: tracker_client(&plan.kind, None, &self.identity)?,
                leg: None,
            });
        }
        let peers = u16::try_from(peers).unwrap_or(u16::MAX);
        let leg = self
            .network()
            .torrent_leg(plan.scope, plan.leg, peers, self.timeout)?;
        let bridge = match plan.kind {
            LegKind::Ladder => Some(leg.bridge()?),
            _ => None,
        };
        let http = tracker_client(&plan.kind, bridge, &self.identity)?;
        let dialer = leg.dialer();
        Ok(OpenLeg {
            shape: plan.kind.shape(Some(&dialer)),
            factory: Some(Arc::new(LegSockets::new(dialer, &plan.kind))),
            kind: plan.kind.clone(),
            http,
            leg: Some(leg),
        })
    }

    // Fetched once for every route, so on Default outbound, which carries
    // what Weaver fetches that no one consumer does; bounded like a fetched
    // torrent.
    async fn fetch_blocklist(&self, url: &reqwest::Url) -> Result<IpRanges, String> {
        let options = FetchOptions {
            label: "blocklist",
            timeout: Duration::from_secs(60),
            redirects: 10,
            user_agent: self.identity.user_agent(),
            https_only: false,
            allow_private_network: self.proxies.allow_private_network(),
        };
        let response = routed_fetch(
            &self.proxies,
            Consumer::Outbound,
            url,
            &options,
            |request, _, _| request,
        )
        .await?;
        if !response.status().is_success() {
            return Err(format!("HTTP {}", response.status()));
        }
        let limit = crate::security::RuntimeSecurityConfig::from_env_or_default_for_tests()
            .nzb_decompressed_limit_bytes;
        let body = crate::proxies::read_body(response, limit).await?;
        IpRanges::create_from_stream(&mut std::io::Cursor::new(body))
            .await
            .map_err(|error| format!("{error:#}"))
    }
}

/// What every session shares.
#[derive(Clone, Debug)]
pub struct SessionEnv {
    /// Each session keeps its state in a directory of its own under here.
    pub root: PathBuf,
    pub base_port: u16,
    pub announce_port: Option<u16>,
    pub upnp: bool,
    pub identity: ClientIdentity,
}

impl SessionEnv {
    /// The sessions the settings ask for, under the data directory unless
    /// they name a directory of their own.
    pub fn of(config: &crate::settings::Config) -> Self {
        let torrent = &config.torrent;
        Self {
            root: torrent
                .session_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from(&config.data_dir).join("torrents")),
            base_port: torrent.base_port,
            announce_port: torrent.announce_port,
            upnp: torrent.upnp,
            identity: crate::torrents::identity::IDENTITY,
        }
    }
}

/// Every address of the host, as one IP: IPv6's when it can be bound, else
/// IPv4's.
pub fn unspecified(binds: impl Fn(IpAddr) -> bool) -> IpAddr {
    let v6 = IpAddr::V6(Ipv6Addr::UNSPECIFIED);
    if binds(v6) {
        v6
    } else {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    }
}

fn binds(ip: IpAddr) -> bool {
    TcpListener::bind(SocketAddr::new(ip, 0)).is_ok()
}

/// How the session treats peer encryption.
fn apply_encryption(options: &mut SessionOptions, policy: TorrentEncryption) {
    options.encryption = encryption_policy(policy);
}

/// The engine's policy for `policy`, a session's or a torrent's.
pub(crate) const fn encryption_policy(
    policy: TorrentEncryption,
) -> weaver_torrent::EncryptionPolicy {
    match policy {
        TorrentEncryption::Prefer => weaver_torrent::EncryptionPolicy::Prefer,
        TorrentEncryption::Require => weaver_torrent::EncryptionPolicy::Require,
    }
}

/// How a session takes and reaches peers: the transports `torrent` asks
/// for. uTP rides the listener's socket, so a session that does not listen
/// has none; one left with neither fails to build, and its route holds
/// closed.
fn transports(torrent: &TorrentConfig) -> (ListenerMode, bool) {
    let protocol = torrent.peer_protocol;
    let mode = match (protocol.tcp(), protocol.utp()) {
        (true, true) => ListenerMode::TcpAndUtp,
        (false, true) => ListenerMode::UtpOnly,
        _ => ListenerMode::TcpOnly,
    };
    (mode, protocol.tcp())
}

/// The limits `torrent` holds a session to, no limit as libtorrent counts
/// it: as many connections as files may be open
/// (`session_impl::update_connections_limit`), and every peer unchoked
/// (`session_impl::preemptive_unchoke`).
pub fn session_limits(torrent: &TorrentConfig) -> SessionLimits {
    let count = |limit: TorrentLimit, unlimited: usize| {
        limit
            .count()
            .map_or(unlimited, |count| count.get() as usize)
    };
    SessionLimits {
        connections_limit: count(
            torrent.connections_limit,
            crate::runtime::resource_limits::open_file_limit(),
        ),
        unchoke_slots_limit: count(torrent.unchoke_slots_limit, usize::MAX),
        connection_speed: torrent.connection_speed,
    }
}

/// The options of a session on `leg`, listening on `port` when it listens,
/// under the torrent settings `torrent` and the IP filter made of them.
pub fn session_options(
    env: &SessionEnv,
    leg: &OpenLeg,
    port: u16,
    dir: &Path,
    peers: usize,
    torrent: &TorrentConfig,
    ip_filter: IpFilter,
) -> SessionOptions {
    let shape = leg.shape;
    let listen_ip = leg.kind.listen_ip().unwrap_or_else(|| unspecified(binds));
    let (mode, enable_tcp) = transports(torrent);
    let limits = session_limits(torrent);
    let mut options = SessionOptions {
        dht: shape.dht.then(|| DhtSessionConfig {
            bootstrap_addrs: Some(torrent.dht_bootstrap_nodes()),
            port: Some(port),
            persistence: Some(DhtPersistenceConfig {
                dump_interval: None,
                config_filename: dir.join("dht.json"),
            }),
        }),
        bind_device_name: leg.kind.bind_device().map(str::to_owned),
        disable_trackers: false,
        fastresume: false,
        persistence: None,
        identity: Some(env.identity.identity()),
        listen: shape.listen.then(|| ListenerOptions {
            mode,
            listen_addr: SocketAddr::new(listen_ip, port),
            enable_upnp_port_forwarding: shape.upnp && env.upnp,
            announce_port: env.announce_port,
            ..ListenerOptions::default()
        }),
        // Peers are dialed by the session's own sockets or by the leg's
        // factory, never by a proxy URL of the session's.
        connect: Some(ConnectionOptions {
            enable_tcp,
            peer_opts: Some(PeerConnectionOptions {
                read_write_timeout: Some(PEER_TIMEOUT),
                ..PeerConnectionOptions::default()
            }),
        }),
        peer_limit: Some(peers.max(1)),
        connections_limit: limits.connections_limit,
        unchoke_slots_limit: limits.unchoke_slots_limit,
        connection_speed: limits.connection_speed,
        allow_multiple_connections_per_ip: torrent.allow_multiple_connections_per_ip,
        tracker_stopped_timeout: Some(Duration::from_secs(
            torrent.stop_tracker_timeout_secs.into(),
        )),
        ip_filter,
        disable_local_service_discovery: !shape.own_sockets,
        socket_factory: leg.factory.clone(),
        http_client: Some(leg.http.clone()),
        ..SessionOptions::default()
    };
    apply_encryption(&mut options, torrent.encryption);
    options
}

/// Why a session could not be had.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    #[error("at most {MAX_SESSIONS} torrent routes can be in use at once")]
    Cap,
    #[error("route down: {0}")]
    RouteDown(String),
    #[error("{0}")]
    Build(String),
    #[error("torrents are off")]
    Off,
}

enum Slot {
    Open {
        session: Arc<Session>,
        leg: Option<Arc<TorrentLeg>>,
        port: u16,
        kind: LegKind,
    },
    Held {
        reason: String,
        kind: LegKind,
    },
}

/// A session as consumers read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionView {
    pub key: RouteKey,
    pub kind: LegKind,
    /// The listen port, while open.
    pub port: Option<u16>,
    /// Why it holds closed.
    pub held: Option<String>,
}

/// The live sessions, by route.
pub struct Sessions {
    routes: Arc<dyn Routes>,
    env: SessionEnv,
    db: Database,
    slots: Arc<tokio::sync::Mutex<BTreeMap<RouteKey, Slot>>>,
    // Read and written under the slots' lock.
    on: AtomicBool,
    // The torrent settings, and the open sessions that take them as they
    // change.
    config: Arc<Mutex<Configured>>,
    // Held while the blocklist the settings name is loaded, once for every
    // session.
    loading: Arc<tokio::sync::Mutex<()>>,
    // The last settings given whose IP filter the open sessions hold, or
    // whose blocklist did not load, by count.
    applied: Arc<tokio::sync::watch::Sender<u64>>,
    // Held while the stored port slots are read and written.
    port_slots: Arc<Mutex<()>>,
    // The torrents' download rate, laid over every open session.
    rates: SessionRates,
}

impl Sessions {
    pub fn new(routes: Arc<dyn Routes>, env: SessionEnv, db: Database) -> Self {
        Self {
            routes,
            env,
            db,
            slots: Arc::default(),
            on: AtomicBool::new(true),
            config: Arc::default(),
            loading: Arc::default(),
            applied: Arc::new(tokio::sync::watch::Sender::new(0)),
            port_slots: Arc::default(),
            rates: SessionRates::default(),
        }
    }

    /// The torrents' download rate across the open sessions.
    pub fn rates(&self) -> &SessionRates {
        &self.rates
    }

    /// The torrent settings. The open sessions take the limits and the IP
    /// filter at once (libtorrent `session_impl::set_ip_filter`), the filter
    /// once the blocklist it names has loaded; a blocklist that does not
    /// load holds them closed. The transports, the DHT's bootstrap nodes,
    /// the stopped announce timeout and connections per address are a
    /// session's from when it opens, so each takes them when its route next
    /// opens.
    pub fn set_config(&self, config: TorrentConfig) {
        let mut configured = self.configured();
        configured.torrent = config;
        configured.given += 1;
        let given = configured.given;
        if configured.lay_over() {
            drop(configured);
            self.applied
                .send_modify(|applied| *applied = given.max(*applied));
            return;
        }
        drop(configured);
        tokio::spawn(refilter(
            Arc::clone(&self.routes),
            Arc::clone(&self.config),
            Arc::clone(&self.slots),
            Arc::clone(&self.loading),
            Arc::clone(&self.applied),
            given,
        ));
    }

    /// Settles once the open sessions hold the IP filter of the settings
    /// last given, or are held closed as its blocklist did not load.
    pub async fn settled(&self) {
        let given = self.configured().given;
        let mut applied = self.applied.subscribe();
        let _ = applied.wait_for(|applied| *applied >= given).await;
    }

    pub fn config(&self) -> TorrentConfig {
        self.configured().torrent.clone()
    }

    fn configured(&self) -> std::sync::MutexGuard<'_, Configured> {
        lock(&self.config)
    }

    // The IP filter `torrent` asks for, its blocklist loaded when it names
    // another than the one loaded; a list that does not load fails the
    // session.
    async fn ip_filter(&self, torrent: &TorrentConfig) -> Result<IpFilter, String> {
        let _loading = self.loading.lock().await;
        load_blocklist(&*self.routes, &self.config, torrent).await?;
        lock(&self.config)
            .filter_of(torrent)
            .ok_or_else(|| "the blocklist did not load".to_owned())
    }

    // The route's port slot, read or taken off the async threads, and the
    // port a gateway mapped to it.
    async fn port_slot(&self, key: &RouteKey) -> Result<(u16, Option<u16>), String> {
        let (db, key, table) = (self.db.clone(), key.clone(), Arc::clone(&self.port_slots));
        tokio::task::spawn_blocking(move || {
            let _table = table.lock().unwrap_or_else(PoisonError::into_inner);
            Ok::<_, StateError>((port_slot(&db, &key)?, mapped_port(&db, &key)?))
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())
    }

    /// Give back the port slot of every route not in `existing`: a route
    /// keeps its port while it exists, open or idle.
    pub async fn forget_routes(&self, existing: BTreeSet<RouteKey>) {
        let (db, table) = (self.db.clone(), Arc::clone(&self.port_slots));
        let released = tokio::task::spawn_blocking(move || {
            let _table = table.lock().unwrap_or_else(PoisonError::into_inner);
            let gone = |key: &str| !existing.iter().any(|kept| kept.as_str() == key);
            release_slots(&db, gone)?;
            release_mapped(&db, gone)
        })
        .await;
        match released {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "could not release torrent port slots");
            }
            Err(error) => {
                tracing::warn!(error = %error, "could not release torrent port slots");
            }
        }
    }

    /// Whether sessions may open.
    pub fn on(&self) -> bool {
        self.on.load(Ordering::Acquire)
    }

    /// Let sessions open, or close every one and open none until turned on.
    pub async fn turn(&self, on: bool) {
        let mut slots = self.slots.lock().await;
        self.on.store(on, Ordering::Release);
        if !on {
            close_all(std::mem::take(&mut *slots)).await;
        }
    }

    pub fn routes(&self) -> &Arc<dyn Routes> {
        &self.routes
    }

    /// Whether `plan`'s route carries DHT. Its open session says; a route
    /// with none open yet has no dialer to ask, so a ladder leg says no.
    pub async fn carries_dht(&self, plan: &RoutePlan) -> bool {
        match self.slots.lock().await.get(&plan.key) {
            Some(Slot::Open { session, .. }) => session.get_dht().is_some(),
            _ => plan.kind.shape(None).dht,
        }
    }

    /// The open session of `plan`'s route, built when it is not open yet. A
    /// route that is down holds closed.
    pub async fn session(&self, plan: &RoutePlan) -> Result<Arc<Session>, SessionError> {
        if let Some(Slot::Open { session, .. }) = self.slots.lock().await.get(&plan.key) {
            return Ok(Arc::clone(session));
        }
        let port_slot = self.port_slot(&plan.key).await;
        let mut slots = self.slots.lock().await;
        if !self.on() {
            return Err(SessionError::Off);
        }
        if let Some(Slot::Open { session, .. }) = slots.get(&plan.key) {
            return Ok(Arc::clone(session));
        }
        if !slots.contains_key(&plan.key) && slots.len() >= MAX_SESSIONS {
            return Err(SessionError::Cap);
        }
        if let Some(reason) = &plan.down {
            slots.insert(
                plan.key.clone(),
                Slot::Held {
                    reason: reason.clone(),
                    kind: plan.kind.clone(),
                },
            );
            return Err(SessionError::RouteDown(reason.clone()));
        }
        // The peer limit is each torrent's, whatever else is open.
        let peers = self.config().max_peers_total as usize;
        match self.build(plan, port_slot, peers).await {
            Ok((session, leg, port)) => {
                self.rates.add(&session);
                self.configured().add(&session);
                slots.insert(
                    plan.key.clone(),
                    Slot::Open {
                        session: Arc::clone(&session),
                        leg,
                        port,
                        kind: plan.kind.clone(),
                    },
                );
                Ok(session)
            }
            Err(error) => {
                slots.insert(
                    plan.key.clone(),
                    Slot::Held {
                        reason: error.clone(),
                        kind: plan.kind.clone(),
                    },
                );
                Err(SessionError::Build(error))
            }
        }
    }

    async fn build(
        &self,
        plan: &RoutePlan,
        slot: Result<(u16, Option<u16>), String>,
        peers: usize,
    ) -> Result<(Arc<Session>, Option<Arc<TorrentLeg>>, u16), String> {
        let (slot, mapped) = slot?;
        let port = self
            .env
            .base_port
            .checked_add(slot)
            .ok_or("no listen port is left for this route")?;
        let dir = self.env.root.join(plan.key.as_str());
        std::fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
        let leg = self.routes.open(plan, peers).await?;
        // A port a gateway mapped to the route is the one it announces.
        let env = SessionEnv {
            announce_port: mapped.or(self.env.announce_port),
            ..self.env.clone()
        };
        let torrent = self.config();
        let ip_filter = self.ip_filter(&torrent).await?;
        let options = session_options(&env, &leg, port, &dir, peers, &torrent, ip_filter);
        Session::new_with_opts(dir.join("output"), options)
            .await
            .map(|session| (session, leg.leg, port))
            .map_err(|error| format!("{error:#}"))
    }

    /// Close `key`'s session and hold it closed for `reason`.
    pub async fn hold(&self, key: &RouteKey, reason: String) {
        let mut slots = self.slots.lock().await;
        let Some(slot) = slots.remove(key) else {
            return;
        };
        let kind = match slot {
            Slot::Open {
                session, leg, kind, ..
            } => {
                close(session, leg).await;
                kind
            }
            Slot::Held { kind, .. } => kind,
        };
        slots.insert(key.clone(), Slot::Held { reason, kind });
    }

    /// Drop `key`'s session, open or held. The route keeps its port slot.
    pub async fn drop_route(&self, key: &RouteKey) {
        let mut slots = self.slots.lock().await;
        if let Some(Slot::Open { session, leg, .. }) = slots.remove(key) {
            close(session, leg).await;
        }
    }

    pub async fn view(&self, key: &RouteKey) -> Option<SessionView> {
        let slots = self.slots.lock().await;
        slots.get(key).map(|slot| view(key, slot))
    }

    pub async fn views(&self) -> Vec<SessionView> {
        let slots = self.slots.lock().await;
        slots.iter().map(|(key, slot)| view(key, slot)).collect()
    }

    pub async fn stop_all(&self) {
        let mut slots = self.slots.lock().await;
        close_all(std::mem::take(&mut *slots)).await;
    }
}

fn lock(config: &Mutex<Configured>) -> std::sync::MutexGuard<'_, Configured> {
    config.lock().unwrap_or_else(PoisonError::into_inner)
}

// The torrent settings, and the sessions open under them.
#[derive(Default)]
struct Configured {
    torrent: TorrentConfig,
    open: Vec<Weak<Session>>,
    // Settings given, counted.
    given: u64,
    // The blocklist last loaded, and where from.
    loaded: Option<(BlocklistSource, Arc<IpRanges>)>,
    // The addresses last blocked, and the blocklist and banned entries they
    // were made of: settings that change neither keep the one list, so the
    // sessions are not filtered again.
    blocked: Option<(Option<Arc<IpRanges>>, Vec<String>, Arc<IpRanges>)>,
}

impl Configured {
    // `session` opened, under settings that may have changed while it did.
    fn add(&mut self, session: &Arc<Session>) {
        self.open.push(Arc::downgrade(session));
        self.lay_over();
    }

    // The IP filter `torrent` asks for: the blocklist it names with its
    // banned addresses added (qBittorrent `SessionImpl::processBannedIPs`),
    // once that blocklist is the one loaded.
    fn filter_of(&mut self, torrent: &TorrentConfig) -> Option<IpFilter> {
        let list = match torrent.blocklist_source() {
            None => None,
            Some(source) => match &self.loaded {
                Some((from, list)) if *from == source => Some(Arc::clone(list)),
                _ => return None,
            },
        };
        let blocked = match &self.blocked {
            Some((of, banned, blocked))
                if *banned == torrent.banned_ips
                    && match (of, &list) {
                        (Some(of), Some(list)) => Arc::ptr_eq(of, list),
                        (of, list) => of.is_none() && list.is_none(),
                    } =>
            {
                Arc::clone(blocked)
            }
            _ => {
                let bans = IpRanges::of_inclusive(torrent.banned_ranges());
                let blocked = match &list {
                    Some(list) if torrent.banned_ips.is_empty() => Arc::clone(list),
                    Some(list) => Arc::new(IpRanges::union([&**list, &bans])),
                    None => Arc::new(bans),
                };
                self.blocked = Some((list, torrent.banned_ips.clone(), Arc::clone(&blocked)));
                blocked
            }
        };
        Some(IpFilter {
            blocked,
            no_privileged_ports: torrent.block_privileged_ports,
            trackers: torrent.filter_trackers,
        })
    }

    // Every open session holds to the settings' limits and, once the
    // blocklist they name is loaded, to their IP filter; whether it is.
    fn lay_over(&mut self) -> bool {
        let limits = session_limits(&self.torrent);
        let torrent = self.torrent.clone();
        let filter = self.filter_of(&torrent);
        self.open.retain(|session| {
            session.upgrade().is_some_and(|session| {
                let open = !session.cancellation_token().is_cancelled();
                if open && session.limits() != limits {
                    session.set_limits(limits);
                }
                if let Some(filter) = filter.as_ref().filter(|_| open) {
                    session.set_ip_filter(filter.clone());
                }
                open
            })
        });
        filter.is_some()
    }
}

// Load the blocklist `torrent` names into `config`, unless it is the one
// loaded. Called under the loading lock.
async fn load_blocklist(
    routes: &dyn Routes,
    config: &Mutex<Configured>,
    torrent: &TorrentConfig,
) -> Result<(), String> {
    let Some(source) = torrent.blocklist_source() else {
        return Ok(());
    };
    if lock(config)
        .loaded
        .as_ref()
        .is_some_and(|(from, _)| *from == source)
    {
        return Ok(());
    }
    let list = match &source {
        BlocklistSource::Http(url) => routes.fetch_blocklist(url).await,
        BlocklistSource::File(path) => IpRanges::load_from_file(path)
            .await
            .map_err(|error| format!("{error:#}")),
    }
    .map_err(|error| format!("the blocklist did not load: {error}"))?;
    lock(config).loaded = Some((source, Arc::new(list)));
    Ok(())
}

// Settings `given` name a blocklist not loaded: load it, then lay the filter
// of the settings as they stand over the open sessions. A list that does not
// load holds them closed, as it would fail them opening, unless newer
// settings have been given since.
async fn refilter(
    routes: Arc<dyn Routes>,
    config: Arc<Mutex<Configured>>,
    slots: Arc<tokio::sync::Mutex<BTreeMap<RouteKey, Slot>>>,
    loading: Arc<tokio::sync::Mutex<()>>,
    applied: Arc<tokio::sync::watch::Sender<u64>>,
    given: u64,
) {
    let loaded = {
        let _loading = loading.lock().await;
        let torrent = lock(&config).torrent.clone();
        load_blocklist(&*routes, &config, &torrent).await
    };
    match loaded {
        Ok(()) => {
            lock(&config).lay_over();
        }
        Err(reason) if lock(&config).given == given => {
            let mut slots = slots.lock().await;
            let open: Vec<RouteKey> = slots
                .iter()
                .filter(|(_, slot)| matches!(slot, Slot::Open { .. }))
                .map(|(key, _)| key.clone())
                .collect();
            for key in open {
                if let Some(Slot::Open {
                    session, leg, kind, ..
                }) = slots.remove(&key)
                {
                    close(session, leg).await;
                    let reason = reason.clone();
                    slots.insert(key, Slot::Held { reason, kind });
                }
            }
        }
        Err(_) => {}
    }
    applied.send_modify(|applied| *applied = given.max(*applied));
}

async fn close_all(slots: BTreeMap<RouteKey, Slot>) {
    let mut closing = tokio::task::JoinSet::new();
    for slot in slots.into_values() {
        if let Slot::Open { session, leg, .. } = slot {
            closing.spawn(close(session, leg));
        }
    }
    closing.join_all().await;
}

fn view(key: &RouteKey, slot: &Slot) -> SessionView {
    match slot {
        Slot::Open { port, kind, .. } => SessionView {
            key: key.clone(),
            kind: kind.clone(),
            port: Some(*port),
            held: None,
        },
        Slot::Held { reason, kind } => SessionView {
            key: key.clone(),
            kind: kind.clone(),
            port: None,
            held: Some(reason.clone()),
        },
    }
}

// Stop a session; its leg's bridge stops with the leg. The leg's stage is
// the route's, and stays. Held under the slots' lock until the session holds
// no sockets, so the route's next session finds its port free.
async fn close(session: Arc<Session>, leg: Option<Arc<TorrentLeg>>) {
    session.stop().await;
    drop(session);
    drop(leg);
}

fn load_slots(db: &Database) -> Result<BTreeMap<String, u16>, StateError> {
    Ok(db
        .get_setting(SLOTS_KEY)?
        .and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default())
}

fn save_slots(db: &Database, slots: &BTreeMap<String, u16>) -> Result<(), StateError> {
    let value =
        serde_json::to_string(slots).map_err(|error| StateError::Database(error.to_string()))?;
    db.set_setting(SLOTS_KEY, &value)
}

/// The port the route's session listens on: its slot's, or the first
/// slot's while it has none.
pub fn listen_port(db: &Database, base_port: u16, key: &RouteKey) -> Result<u16, StateError> {
    let slot = load_slots(db)?.get(key.as_str()).copied().unwrap_or(0);
    Ok(base_port.saturating_add(slot))
}

/// The route's port slot, kept across restarts: the lowest free one the
/// first time the route opens.
pub fn port_slot(db: &Database, key: &RouteKey) -> Result<u16, StateError> {
    let mut slots = load_slots(db)?;
    if let Some(slot) = slots.get(key.as_str()) {
        return Ok(*slot);
    }
    let slot = (0u16..)
        .find(|slot| !slots.values().any(|taken| taken == slot))
        .expect("fewer slots than ports");
    slots.insert(key.as_str().to_owned(), slot);
    save_slots(db, &slots)?;
    Ok(slot)
}

fn load_mapped(db: &Database) -> Result<BTreeMap<String, u16>, StateError> {
    Ok(db
        .get_setting(MAPPED_KEY)?
        .and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default())
}

fn save_mapped(db: &Database, mapped: &BTreeMap<String, u16>) -> Result<(), StateError> {
    let value =
        serde_json::to_string(mapped).map_err(|error| StateError::Database(error.to_string()))?;
    db.set_setting(MAPPED_KEY, &value)
}

/// The port a tunnel's gateway mapped to the route, kept across restarts.
pub fn mapped_port(db: &Database, key: &RouteKey) -> Result<Option<u16>, StateError> {
    Ok(load_mapped(db)?.get(key.as_str()).copied())
}

/// Keep the port a tunnel's gateway mapped to the route: the route's
/// session announces it from the next time it opens.
pub fn set_mapped_port(db: &Database, key: &RouteKey, port: u16) -> Result<(), StateError> {
    let mut mapped = load_mapped(db)?;
    if mapped.insert(key.as_str().to_owned(), port) != Some(port) {
        save_mapped(db, &mapped)?;
    }
    Ok(())
}

// Forget the mapped ports of the routes `gone` names.
fn release_mapped(db: &Database, gone: impl Fn(&str) -> bool) -> Result<(), StateError> {
    let mut mapped = load_mapped(db)?;
    let before = mapped.len();
    mapped.retain(|key, _| !gone(key));
    if mapped.len() != before {
        save_mapped(db, &mapped)?;
    }
    Ok(())
}

// Free the slots of the routes `gone` names.
fn release_slots(db: &Database, gone: impl Fn(&str) -> bool) -> Result<(), StateError> {
    let mut slots = load_slots(db)?;
    let before = slots.len();
    slots.retain(|key, _| !gone(key));
    if slots.len() != before {
        save_slots(db, &slots)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
