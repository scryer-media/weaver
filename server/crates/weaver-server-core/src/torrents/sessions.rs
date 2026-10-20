// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! One torrent session per distinct route. Categories whose torrents resolve
//! to the same leg share its session; a session lives while its leg is up and
//! holds closed while it is down.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use librqbit::dht::DhtPersistenceConfig;
use librqbit::{
    ConnectionOptions, DhtSessionConfig, ListenerMode, ListenerOptions, Session, SessionOptions,
    SocketFactory,
};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::StateError;
use crate::persistence::Database;
use crate::proxies::{
    Consumer, EgressBinding, EgressHealth, EgressInterface, LegPath, NetworkRuntime, RoutingPolicy,
    SYSTEM_EGRESS_ID, TorrentLeg, TorrentScope,
};
use crate::settings::TorrentEncryption;
use crate::torrents::identity::ClientIdentity;
use crate::torrents::net::{LegKind, LegSockets, tracker_client};
use crate::torrents::rate::SessionRates;

/// The most sessions that run at once.
pub const MAX_SESSIONS: usize = 8;

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

/// A torrent scope's route as it stands: its one leg, and whether that leg
/// is up.
#[derive(Clone, Debug)]
pub struct RoutePlan {
    pub scope: TorrentScope,
    pub policy: RoutingPolicy,
    pub egress_id: u32,
    pub kind: LegKind,
    pub key: RouteKey,
    /// Why the leg is down, when it is.
    pub down: Option<String>,
}

/// A leg made ready for a session.
pub struct OpenLeg {
    pub kind: LegKind,
    pub factory: Option<Arc<dyn SocketFactory>>,
    pub http: reqwest::Client,
    /// The route's leg the factory dials through, kept while the session
    /// lives.
    pub leg: Option<Arc<TorrentLeg>>,
}

/// Where routes come from.
#[async_trait::async_trait]
pub trait Routes: Send + Sync {
    fn plan(&self, scope: TorrentScope) -> Result<RoutePlan, String>;
    async fn open(&self, plan: &RoutePlan) -> Result<OpenLeg, String>;
}

/// Routes as the network runtime resolves them.
pub struct NetworkRoutes {
    db: Database,
    network: Arc<NetworkRuntime>,
    timeout: Duration,
    identity: ClientIdentity,
}

impl NetworkRoutes {
    pub fn new(
        db: Database,
        network: Arc<NetworkRuntime>,
        timeout: Duration,
        identity: ClientIdentity,
    ) -> Self {
        Self {
            db,
            network,
            timeout,
            identity,
        }
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
        let leg = route.legs.first().ok_or("the route has no leg")?.clone();
        let key = leg_key(&self.network, leg.egress_id, &leg.path);
        let Some(egress) = leg_egress(&self.network, leg.egress_id) else {
            return Ok(RoutePlan {
                scope,
                policy,
                egress_id: leg.egress_id,
                kind: LegKind::System,
                key,
                down: Some(format!("egress {} is missing", leg.egress_id)),
            });
        };
        let down = match self.network.interfaces().health(&egress, None) {
            EgressHealth::Down(reason) => Some(reason),
            _ if self.network.quota_blocked_egresses().contains(&egress.id) => {
                Some("quota reached".into())
            }
            _ => None,
        };
        Ok(RoutePlan {
            scope,
            egress_id: egress.id,
            kind: LegKind::of(&egress.binding, &leg.path),
            key,
            policy,
            down,
        })
    }

    async fn open(&self, plan: &RoutePlan) -> Result<OpenLeg, String> {
        if plan.kind.shape().own_sockets {
            return Ok(OpenLeg {
                kind: plan.kind.clone(),
                factory: None,
                http: tracker_client(&plan.kind, None, &self.identity)?,
                leg: None,
            });
        }
        let leg = self.network.torrent_leg(plan.scope, self.timeout)?;
        let bridge = match plan.kind {
            LegKind::Ladder => Some(leg.bridge()?),
            _ => None,
        };
        let http = tracker_client(&plan.kind, bridge.as_deref(), &self.identity)?;
        Ok(OpenLeg {
            factory: Some(Arc::new(LegSockets::new(leg.stage.clone(), &plan.kind))),
            kind: plan.kind.clone(),
            http,
            leg: Some(leg),
        })
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
    pub max_peers_total: u32,
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
            max_peers_total: torrent.max_peers_total,
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
    options.encryption = match policy {
        TorrentEncryption::Prefer => librqbit::EncryptionPolicy::Prefer,
        TorrentEncryption::Require => librqbit::EncryptionPolicy::Require,
    };
}

/// The options of a session on `leg`, listening on `port` when it listens.
pub fn session_options(
    env: &SessionEnv,
    leg: &OpenLeg,
    port: u16,
    dir: &Path,
    peers: usize,
    encryption: TorrentEncryption,
) -> SessionOptions {
    let shape = leg.kind.shape();
    let listen_ip = leg.kind.listen_ip().unwrap_or_else(|| unspecified(binds));
    let mut options = SessionOptions {
        dht: shape.dht.then(|| DhtSessionConfig {
            bootstrap_addrs: None,
            port: Some(port),
            persistence: Some(DhtPersistenceConfig {
                dump_interval: None,
                config_filename: Some(dir.join("dht.json")),
            }),
        }),
        bind_device_name: leg.kind.bind_device().map(str::to_owned),
        disable_trackers: false,
        fastresume: false,
        persistence: None,
        peer_id: Some(env.identity.peer_id()),
        listen: shape.listen.then(|| ListenerOptions {
            mode: ListenerMode::TcpOnly,
            listen_addr: SocketAddr::new(listen_ip, port),
            enable_upnp_port_forwarding: shape.upnp && env.upnp,
            announce_port: env.announce_port,
            ..ListenerOptions::default()
        }),
        // Peers are dialed by the session's own sockets or by the leg's
        // factory, never by a proxy URL of the session's.
        connect: Some(ConnectionOptions {
            proxy_url: None,
            enable_tcp: true,
            peer_opts: None,
        }),
        peer_limit: Some(peers.max(1)),
        disable_local_service_discovery: !shape.own_sockets,
        client_name_and_version: Some(env.identity.user_agent().to_owned()),
        socket_factory: leg.factory.clone(),
        http_client: Some(leg.http.clone()),
        ..SessionOptions::default()
    };
    apply_encryption(&mut options, encryption);
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
    slots: tokio::sync::Mutex<BTreeMap<RouteKey, Slot>>,
    // Read and written under the slots' lock.
    on: AtomicBool,
    encryption: Mutex<TorrentEncryption>,
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
            slots: tokio::sync::Mutex::new(BTreeMap::new()),
            on: AtomicBool::new(true),
            encryption: Mutex::new(TorrentEncryption::default()),
            port_slots: Arc::default(),
            rates: SessionRates::default(),
        }
    }

    /// The torrents' download rate across the open sessions.
    pub fn rates(&self) -> &SessionRates {
        &self.rates
    }

    /// The encryption sessions opened from now on ask of their peers.
    pub fn set_encryption(&self, encryption: TorrentEncryption) {
        *self
            .encryption
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = encryption;
    }

    pub fn encryption(&self) -> TorrentEncryption {
        *self
            .encryption
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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
            close_all(std::mem::take(&mut *slots));
        }
    }

    pub fn routes(&self) -> &Arc<dyn Routes> {
        &self.routes
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
        let open = slots
            .values()
            .filter(|slot| matches!(slot, Slot::Open { .. }))
            .count();
        let peers = self.env.max_peers_total as usize / (open + 1);
        match self.build(plan, port_slot, peers).await {
            Ok((session, leg, port)) => {
                self.rates.add(&session);
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
        let leg = self.routes.open(plan).await?;
        // A port a gateway mapped to the route is the one it announces.
        let env = SessionEnv {
            announce_port: mapped.or(self.env.announce_port),
            ..self.env.clone()
        };
        let options = session_options(&env, &leg, port, &dir, peers, self.encryption());
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
                close(session, leg);
                kind
            }
            Slot::Held { kind, .. } => kind,
        };
        slots.insert(key.clone(), Slot::Held { reason, kind });
    }

    /// Drop `key`'s session, open or held. The route keeps its port slot.
    pub async fn drop_route(&self, key: &RouteKey) {
        let slot = self.slots.lock().await.remove(key);
        if let Some(Slot::Open { session, leg, .. }) = slot {
            close(session, leg);
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
        close_all(std::mem::take(&mut *self.slots.lock().await));
    }
}

fn close_all(slots: BTreeMap<RouteKey, Slot>) {
    for slot in slots.into_values() {
        if let Slot::Open { session, leg, .. } = slot {
            close(session, leg);
        }
    }
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
// the route's, and stays.
fn close(session: Arc<Session>, leg: Option<Arc<TorrentLeg>>) {
    session.cancellation_token().cancel();
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
