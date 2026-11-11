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
use weaver_torrent::network::{KernelSockets, Resolver};
use weaver_torrent::upnp::PortMapEvents;
pub use weaver_torrent::upnp::{PortMapped, PortmapProtocol, PortmapTransport};
use weaver_torrent::{
    ConnectionOptions, DhtSessionConfig, HttpClients, Identity, IpFilter, ListenFailed,
    ListenerMode, ListenerOptions, PeerConnectionOptions, Session, SessionLimits, SessionOptions,
    SharedLimits, SocketFactory,
};
use weaver_tunnel::pipe::Dialer;

use crate::StateError;
use crate::bandwidth::TorrentRates;
use crate::persistence::Database;
use crate::proxies::{
    Consumer, EgressBinding, EgressHealth, EgressInterface, EgressLimitChanges, FetchOptions,
    LegPath, MulticastInterfaces, NetworkRuntime, ProxyRuntime, ResolvedBy, Route, RoutingPolicy,
    Rung, SYSTEM_EGRESS_ID, TorrentLeg, TorrentScope, pool_of, routed_fetch,
};
use crate::settings::{BlocklistSource, TorrentConfig, TorrentEncryption, TorrentLimit};
use crate::torrents::identity::IDENTITY;
use crate::torrents::net::{Exposure, LegKind, LegNames, LegSockets, Shape, tracker_client};
use crate::torrents::rate::SessionRates;

/// The most sessions that run at once.
pub const MAX_SESSIONS: usize = 8;

/// How long a peer may send nothing before it is dropped. A seed that
/// unchokes and then sends only the odd HAVE is still a seed.
pub const PEER_TIMEOUT: Duration = Duration::from_secs(120);

const SLOTS_KEY: &str = "torrent.session_slots";
const CHOSEN_SSL_PORT_KEY: &str = "torrent.chosen_ssl_port";

/// Why a torrent route has no open session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TorrentRouteReason {
    NotAskedToOpen,
    LegDown,
    LegMissing,
    LegRevoked,
    QuotaSilenced,
    TorrentsDisabled,
    RouteBroken(crate::proxies::TorrentRouteFault),
    LegOpenFailed,
    StateDirectoryFailed,
    ListenPortUnavailable,
    ListenPortBindFailed,
    SessionStateUnreadable,
    SessionBuildFailed,
    BlocklistFailed,
    /// Every session there can be is in use by other routes.
    SessionLimit,
    /// Its session is being opened now.
    Opening,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteFailure {
    pub reason: TorrentRouteReason,
    /// The underlying diagnostic for logs and protocol status, never a localized route label.
    pub detail: Option<String>,
}

impl From<TorrentRouteReason> for RouteFailure {
    fn from(reason: TorrentRouteReason) -> Self {
        Self {
            reason,
            detail: None,
        }
    }
}

impl RouteFailure {
    pub fn with_detail(reason: TorrentRouteReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: Some(detail.into()),
        }
    }

    pub fn leg_down(detail: String) -> Self {
        Self::with_detail(TorrentRouteReason::LegDown, detail)
    }

    /// Text for protocols that carry a status sentence rather than a reason code.
    pub fn message(&self) -> String {
        if let Some(detail) = &self.detail {
            return detail.clone();
        }
        match self.reason {
            TorrentRouteReason::NotAskedToOpen => "not opened: nothing has asked it to open",
            TorrentRouteReason::LegDown => "the route's leg is down",
            TorrentRouteReason::LegMissing => "the route's egress is missing",
            TorrentRouteReason::LegRevoked => "the route's leg was revoked",
            TorrentRouteReason::QuotaSilenced => "quota reached",
            TorrentRouteReason::TorrentsDisabled => crate::torrents::status::OFF,
            TorrentRouteReason::RouteBroken(fault) => fault.message(),
            TorrentRouteReason::LegOpenFailed => "the route's leg could not be opened",
            TorrentRouteReason::StateDirectoryFailed => {
                "the session's state directory could not be created"
            }
            TorrentRouteReason::ListenPortUnavailable => {
                "no listen port is available for this route"
            }
            TorrentRouteReason::ListenPortBindFailed => "the listen port could not be bound",
            TorrentRouteReason::SessionStateUnreadable => {
                "the session's stored ports could not be read"
            }
            TorrentRouteReason::SessionBuildFailed => "the session could not be opened",
            TorrentRouteReason::BlocklistFailed => "the blocklist could not be loaded",
            TorrentRouteReason::SessionLimit => "every session is in use by other routes",
            TorrentRouteReason::Opening => "the route's session is opening",
        }
        .to_owned()
    }
}

/// A resolved route's identity: the leg it dials over.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RouteKey(String);

impl RouteKey {
    pub fn of(egress_id: u32, binding: &EgressBinding, path: &LegPath) -> Self {
        let encoded =
            serde_json::to_vec(&(egress_id, binding, path)).expect("route legs always encode");
        Self(hex::encode(&Sha1::digest(&encoded)[..8]))
    }

    /// The key of the session a pool route through `egress_id` runs on its
    /// member, proxy `member`: one proxy on one egress runs one session,
    /// whichever pools and routes hold it.
    pub fn member(egress_id: u32, binding: &EgressBinding, member: u32) -> Self {
        let encoded = serde_json::to_vec(&("member", egress_id, binding, member))
            .expect("route legs always encode");
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

/// How a session rides a leg through `egress_id` and `path`.
pub fn leg_kind(network: &NetworkRuntime, egress_id: u32, path: &LegPath) -> LegKind {
    leg_egress(network, egress_id)
        .map_or(LegKind::System, |egress| LegKind::of(&egress.binding, path))
}

/// Why a leg on egress `egress_id` may carry nothing now, when it may not:
/// its egress is down, gone, or out of quota.
pub fn leg_unavailable(network: &NetworkRuntime, egress_id: u32) -> Option<RouteFailure> {
    let Some(egress) = leg_egress(network, egress_id) else {
        return Some(TorrentRouteReason::LegMissing.into());
    };
    match network.interfaces().health(&egress, None) {
        EgressHealth::Down(reason) => Some(RouteFailure::leg_down(reason)),
        _ if network.quota_blocked_egresses().contains(&egress.id) => {
            Some(TorrentRouteReason::QuotaSilenced.into())
        }
        _ => None,
    }
}

/// Who resolves the names a session on `path` looks up: the host's DNS on a
/// direct leg, the exit proxy's naming on a ladder, none on a pool exit, whose
/// sessions each name through their own member, or a proxy that is not
/// configured.
pub fn names_on(network: &NetworkRuntime, path: &LegPath) -> Option<ResolvedBy> {
    let exit = match path {
        LegPath::Direct => return Some(ResolvedBy::Host),
        LegPath::Ladder { rungs, .. } => match rungs.last()? {
            Rung::Proxy { id } => *id,
            Rung::Chain { ids } => *ids.last()?,
            Rung::Pool { .. } => return None,
        },
    };
    network.resolved_by(exit)
}

/// The key of the session a route through `egress_id` and `path` opens.
pub fn leg_key(network: &NetworkRuntime, egress_id: u32, path: &LegPath) -> RouteKey {
    match leg_egress(network, egress_id) {
        Some(egress) => RouteKey::of(egress.id, &egress.binding, path),
        None => RouteKey::of(egress_id, &EgressBinding::System, path),
    }
}

/// The key of the session a pool route through `egress_id` runs on proxy
/// `member`.
pub fn member_key(network: &NetworkRuntime, egress_id: u32, member: u32) -> RouteKey {
    match leg_egress(network, egress_id) {
        Some(egress) => RouteKey::member(egress.id, &egress.binding, member),
        None => RouteKey::member(egress_id, &EgressBinding::System, member),
    }
}

/// The key of a route no torrent may run on, as stored: its own, so holding
/// it holds no session another route's leg runs.
fn refused_key(route: &Route) -> RouteKey {
    let encoded = serde_json::to_vec(&("refused", route)).expect("routes always encode");
    RouteKey(hex::encode(&Sha1::digest(&encoded)[..8]))
}

/// The position of a torrent route's one leg.
pub const TORRENT_LEG: usize = 0;

/// A torrent scope's route as it stands: its one leg, and whether that leg is
/// up. A route a torrent may not run on is down, for the refusal. A route
/// whose one rung is a pool runs a session on each member, and each torrent
/// on one of them: the plan of the route lists its members, and the plan a
/// torrent runs on is its member's.
#[derive(Clone, Debug)]
pub struct RoutePlan {
    pub scope: TorrentScope,
    pub policy: RoutingPolicy,
    pub egress_id: u32,
    pub kind: LegKind,
    pub key: RouteKey,
    /// Why the leg is down, when it is: on a pool route, while no member is
    /// up.
    pub down: Option<RouteFailure>,
    /// Who resolves the names its sessions look up: the host's DNS on a
    /// direct leg, the exit proxy's naming on a ladder, none when the route
    /// names no proxy that is configured.
    pub names: Option<ResolvedBy>,
    /// The members of its pool that may carry a session, in the pool's
    /// order; none when its rung is no pool.
    pub members: Vec<MemberPlan>,
    /// The member this plan's session runs on, on a member's plan.
    pub member: Option<u32>,
}

/// A pool member a route's torrents may run on: its own session's key, why
/// it carries nothing now when it does not, and who resolves its names.
#[derive(Clone, Debug)]
pub struct MemberPlan {
    pub id: u32,
    pub key: RouteKey,
    pub down: Option<RouteFailure>,
    pub names: Option<ResolvedBy>,
}

impl RoutePlan {
    /// The plan a torrent pinned to member `pinned` runs on, and the member
    /// it is pinned to from here. A torrent stays on its member while that
    /// member is up; one with none, or whose member failed while another is
    /// up, goes to the up member with the fewest torrents by `load`, the
    /// lowest id of those. Its member down with none other up, it holds on
    /// its member. A route with no pool pins nothing.
    pub fn place(
        &self,
        pinned: Option<u32>,
        load: impl Fn(&RouteKey) -> usize,
    ) -> (RoutePlan, Option<u32>) {
        let pinned = pinned.and_then(|id| self.members.iter().find(|member| member.id == id));
        let up = |member: &&MemberPlan| member.down.is_none();
        let member = pinned.filter(up).or_else(|| {
            self.members
                .iter()
                .filter(up)
                .min_by_key(|member| (load(&member.key), member.id))
                .or(pinned)
        });
        match member {
            Some(member) => (self.on(member), Some(member.id)),
            None => (self.clone(), None),
        }
    }

    // The plan of `member`'s session.
    fn on(&self, member: &MemberPlan) -> RoutePlan {
        RoutePlan {
            key: member.key.clone(),
            down: member.down.clone(),
            names: member.names,
            member: Some(member.id),
            ..self.clone()
        }
    }

    /// Who resolves the names of a torrent pinned to member `pinned`: its
    /// member's naming on a pool route, the route's otherwise.
    pub fn names_for(&self, pinned: Option<u32>) -> Option<ResolvedBy> {
        match pinned.and_then(|id| self.members.iter().find(|member| member.id == id)) {
            Some(member) => member.names,
            None => self.names,
        }
    }

    /// The keys of every session the route runs: its members' on a pool
    /// route, else its own.
    pub fn keys(&self) -> impl Iterator<Item = &RouteKey> {
        let own = self.members.is_empty().then_some(&self.key);
        own.into_iter()
            .chain(self.members.iter().map(|member| &member.key))
    }
}

/// A leg made ready for a session.
pub struct OpenLeg {
    pub kind: LegKind,
    pub shape: Shape,
    /// Every socket the session opens: the kernel's on an own-socket leg's
    /// device, else the leg's dialer's.
    pub factory: Arc<dyn SocketFactory>,
    /// Every name the session looks up, on whichever leg: the leg's
    /// dialer's naming, which records each lookup as every other dial does.
    pub names: Arc<dyn Resolver>,
    /// The session's HTTP clients, for its trackers and web seeds.
    pub http: Arc<dyn HttpClients>,
    /// The route's leg the factory dials through, kept while the session
    /// lives.
    pub leg: Option<Arc<TorrentLeg>>,
}

impl OpenLeg {
    /// A leg of `kind` over `dialer`, as every session's leg is opened. A
    /// session on an own-socket leg opens the kernel's sockets on the leg's
    /// device and looks its names up through `dialer`; on any other leg its
    /// sockets and names come from `dialer` too, and a ladder's HTTP rides
    /// `bridge`.
    pub fn on(
        kind: LegKind,
        dialer: Arc<dyn Dialer>,
        bridge: Option<crate::proxies::PathBridge>,
        leg: Option<Arc<TorrentLeg>>,
    ) -> Result<Self, String> {
        let factory: Arc<dyn SocketFactory> = if kind.own_sockets() {
            Arc::new(KernelSockets::new(kind.bind_device()).map_err(|error| format!("{error:#}"))?)
        } else {
            Arc::new(LegSockets::new(Arc::clone(&dialer), &kind))
        };
        Ok(Self {
            shape: kind.shape(Some(&dialer)),
            factory,
            http: tracker_client(&kind, Arc::clone(&dialer), bridge)?,
            names: Arc::new(LegNames(dialer)),
            kind,
            leg,
        })
    }
}

/// Where routes come from.
#[async_trait::async_trait]
pub trait Routes: Send + Sync {
    fn plan(&self, scope: TorrentScope) -> RoutePlan;
    /// Why `plan`'s leg may carry nothing now, when it may not: its egress
    /// is down, gone, or out of quota. A route plans down for the same.
    fn leg_down(&self, plan: &RoutePlan) -> Option<RouteFailure>;
    /// Open `plan`'s leg for a session of at most `peers` peers.
    async fn open(&self, plan: &RoutePlan, peers: usize) -> Result<OpenLeg, String>;
    /// `plan`'s scope runs on a member session another scope opened: start
    /// the scope's own route, so the session lives on in its pool after the
    /// opener's route leaves the member.
    fn join(&self, _plan: &RoutePlan, _peers: usize) {}
    /// The blocklist at `url`, which every route's session shares.
    async fn fetch_blocklist(&self, url: &reqwest::Url) -> Result<IpRanges, String>;
    /// What egress `egress_id` lets its sessions move at now: its own caps
    /// under its quota state. No limit by default.
    fn egress_rates(&self, _egress_id: u32) -> TorrentRates {
        TorrentRates::default()
    }
    /// What rings as an egress's caps or quota state may change; none when
    /// no egress limits torrents.
    fn egress_limit_changes(&self) -> Option<EgressLimitChanges> {
        None
    }
    /// What rings as the host's interfaces change as multicast sees them;
    /// none when nothing watches them.
    fn interface_changes(&self) -> Option<tokio::sync::watch::Receiver<MulticastInterfaces>> {
        None
    }
}

/// Routes as the network runtime resolves them.
pub struct NetworkRoutes {
    proxies: Arc<ProxyRuntime>,
    timeout: Duration,
}

impl NetworkRoutes {
    pub fn new(proxies: Arc<ProxyRuntime>, timeout: Duration) -> Self {
        Self { proxies, timeout }
    }

    fn network(&self) -> &NetworkRuntime {
        &self.proxies.network
    }
}

#[async_trait::async_trait]
impl Routes for NetworkRoutes {
    fn plan(&self, scope: TorrentScope) -> RoutePlan {
        // The runtime holds every saved policy, loaded when it is built and
        // again on every reload after a save.
        let policy = self.proxies.policy(Consumer::Torrent(scope));
        let route = policy.route();
        let leg = route.legs.first().expect("a policy's route has a leg");
        let egress = leg_egress(self.network(), leg.egress_id);
        let egress_id = egress.as_ref().map_or(leg.egress_id, |egress| egress.id);
        let mut members = Vec::new();
        let (key, down) = match crate::proxies::TorrentRouteFault::validate(&route) {
            Err(fault) => (
                refused_key(&route),
                Some(TorrentRouteReason::RouteBroken(fault).into()),
            ),
            Ok(()) => {
                let unavailable = leg_unavailable(self.network(), leg.egress_id);
                let down = match pool_of(&leg.path) {
                    None => unavailable,
                    Some(pool) => {
                        members = self
                            .network()
                            .pool_members(pool)
                            .into_iter()
                            .map(|id| MemberPlan {
                                id,
                                key: member_key(self.network(), leg.egress_id, id),
                                down: unavailable.clone().or_else(|| {
                                    self.network()
                                        .pool_member_down(pool, egress_id, id)
                                        .map(RouteFailure::leg_down)
                                }),
                                names: self.network().resolved_by(id),
                            })
                            .collect();
                        // Down while no member is up: for the first member's
                        // reason, or for the pool having none.
                        members.iter().all(|member| member.down.is_some()).then(|| {
                            members
                                .first()
                                .and_then(|member| member.down.clone())
                                .unwrap_or_else(|| {
                                    RouteFailure::leg_down(format!(
                                        "pool {pool} has no member that is enabled"
                                    ))
                                })
                        })
                    }
                };
                (leg_key(self.network(), leg.egress_id, &leg.path), down)
            }
        };
        RoutePlan {
            scope,
            egress_id,
            kind: leg_kind(self.network(), leg.egress_id, &leg.path),
            key,
            names: names_on(self.network(), &leg.path),
            policy,
            down,
            members,
            member: None,
        }
    }

    // A pool member's session may be another scope's too: whether its
    // member carries anything is its leg's to say, not one scope's pool.
    fn leg_down(&self, plan: &RoutePlan) -> Option<RouteFailure> {
        leg_unavailable(self.network(), plan.egress_id)
    }

    async fn open(&self, plan: &RoutePlan, peers: usize) -> Result<OpenLeg, String> {
        if plan.kind.own_sockets() {
            let egress = self.network().egress_dialer(plan.egress_id, self.timeout)?;
            return OpenLeg::on(plan.kind.clone(), egress, None, None);
        }
        let peers = u16::try_from(peers).unwrap_or(u16::MAX);
        let leg = match plan.member {
            Some(member) => self.network().torrent_member_leg(
                plan.scope,
                TORRENT_LEG,
                member,
                peers,
                self.timeout,
            )?,
            None => self
                .network()
                .torrent_leg(plan.scope, TORRENT_LEG, peers, self.timeout)?,
        };
        let bridge = match plan.kind {
            LegKind::Ladder => Some(leg.bridge()?),
            _ => None,
        };
        OpenLeg::on(plan.kind.clone(), leg.dialer(), bridge, Some(leg))
    }

    fn join(&self, plan: &RoutePlan, peers: usize) {
        let peers = u16::try_from(peers).unwrap_or(u16::MAX);
        if let Err(error) = self
            .network()
            .start_torrent_route(plan.scope, peers, self.timeout)
        {
            tracing::debug!(
                route = plan.key.as_str(),
                error,
                "a joining scope's route did not start"
            );
        }
    }

    fn egress_rates(&self, egress_id: u32) -> TorrentRates {
        self.network().torrent_egress_rates(egress_id)
    }

    fn egress_limit_changes(&self) -> Option<EgressLimitChanges> {
        self.network().egress_limit_changes()
    }

    fn interface_changes(&self) -> Option<tokio::sync::watch::Receiver<MulticastInterfaces>> {
        Some(self.network().interface_changes())
    }

    // Fetched once for every route, so on Default outbound, which carries
    // what Weaver fetches that no one consumer does; bounded like a fetched
    // torrent. No torrent's traffic, so Weaver names itself.
    async fn fetch_blocklist(&self, url: &reqwest::Url) -> Result<IpRanges, String> {
        let options = FetchOptions {
            label: "blocklist",
            timeout: Duration::from_secs(60),
            redirects: 10,
            user_agent: IDENTITY.user_agent(),
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

/// What every session shares while the process runs. The rest of what a
/// session opens with is the torrent settings', read as they stand.
#[derive(Clone, Debug)]
pub struct SessionEnv {
    /// Each session keeps its state in a directory of its own under here,
    /// unless the settings name another.
    pub root: PathBuf,
}

impl SessionEnv {
    /// The sessions of a server, under its data directory.
    pub fn of(config: &crate::settings::Config) -> Self {
        Self {
            root: PathBuf::from(&config.data_dir).join("torrents"),
        }
    }

    /// Where the sessions keep their state under `torrent`.
    fn root_of(&self, torrent: &TorrentConfig) -> PathBuf {
        torrent
            .session_dir
            .clone()
            .unwrap_or_else(|| self.root.clone())
    }
}

/// What a session on a leg of `shape` takes from the settings when it opens
/// and cannot take while open: where it keeps its state, the port it binds,
/// the port it announces, whether it maps its port and the transports it
/// listens on and dials with. Settings that now give another re-open it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Opening {
    shape: Shape,
    root: PathBuf,
    base_port: Option<u16>,
    announce_port: Option<u16>,
    upnp: bool,
    // TCP and uTP as `transports` gives them for the leg: a protocol the
    // leg cannot carry changes nothing.
    tcp: bool,
    utp: bool,
}

/// The external port a session on a leg of `shape`, listening on `port`
/// under `torrent`, asks a gateway to map, when it maps at all: its own
/// listen port, whatever announce port is set (libtorrent
/// `session_impl::map_port`, session_impl.cpp:2375-2390).
fn maps(torrent: &TorrentConfig, shape: Shape, port: u16) -> Option<u16> {
    (shape.listen && shape.upnp && torrent.upnp).then_some(port)
}

impl Opening {
    fn of(env: &SessionEnv, torrent: &TorrentConfig, shape: Shape) -> Self {
        let (_, tcp, utp) = transports(torrent, shape);
        Self {
            tcp,
            utp,
            shape,
            root: env.root_of(torrent),
            base_port: (shape.listen || shape.dht).then_some(torrent.base_port),
            announce_port: torrent.announce_port.filter(|_| shape.listen),
            upnp: shape.listen && shape.upnp && torrent.upnp,
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

/// How a session on a leg of `shape` takes and reaches peers: the
/// transports `torrent` asks for that the leg carries, and whether it listens
/// at all. Outgoing uTP uses the leg's datagram route even when it has no
/// incoming listener. A session left with neither outgoing transport fails
/// to build, and its route holds closed.
fn transports(torrent: &TorrentConfig, shape: Shape) -> (Option<ListenerMode>, bool, bool) {
    let protocol = torrent.peer_protocol;
    let utp = protocol.utp() && shape.utp;
    let mode = match (protocol.tcp(), utp) {
        (true, true) => Some(ListenerMode::TcpAndUtp),
        (false, true) => Some(ListenerMode::UtpOnly),
        (true, false) => Some(ListenerMode::TcpOnly),
        (false, false) => None,
    };
    (mode.filter(|_| shape.listen), protocol.tcp(), utp)
}

/// The limits `torrent` holds a session to, no limit as libtorrent counts
/// it: connections within the files that may be open, less twenty kept for
/// files and four in five of the rest (libtorrent `session_impl` sets
/// `connections_limit` to `max(5, (max_files - 20) * 8 / 10)`), so peers
/// never take the descriptors files and the database need, and every peer
/// unchoked (`session_impl::preemptive_unchoke`).
pub fn session_limits(torrent: &TorrentConfig) -> SessionLimits {
    let count = |limit: TorrentLimit, unlimited: usize| {
        limit
            .count()
            .map_or(unlimited, |count| count.get() as usize)
    };
    SessionLimits {
        connections_limit: count(
            torrent.connections_limit,
            connections_within(crate::runtime::resource_limits::open_file_limit()),
        ),
        unchoke_slots_limit: count(torrent.unchoke_slots_limit, usize::MAX),
        connection_speed: torrent.connection_speed,
    }
}

// The connections `files` open files leave room for (libtorrent
// `session_impl`: `max(5, (max_files - 20) * 8 / 10)`).
pub(crate) fn connections_within(files: usize) -> usize {
    (files.saturating_sub(20) * 8 / 10).max(5)
}

/// The options of a session on `leg`, listening on `port` when it listens,
/// under the torrent settings `torrent` and the IP filter made of them, that
/// names itself as `opener`'s client where it names a client for all its
/// torrents: its DHT node and its gateway mappings.
pub fn session_options(
    leg: &OpenLeg,
    port: u16,
    dir: &Path,
    peers: usize,
    torrent: &TorrentConfig,
    ip_filter: IpFilter,
    opener: &Identity,
) -> SessionOptions {
    let shape = leg.shape;
    let listen_ip = leg.kind.listen_ip().unwrap_or_else(|| unspecified(binds));
    let (mode, enable_tcp, enable_utp) = transports(torrent, shape);
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
        identity: Some(opener.same_client()),
        listen: mode.map(|mode| ListenerOptions {
            mode,
            listen_addr: SocketAddr::new(listen_ip, port),
            enable_upnp_port_forwarding: shape.upnp && torrent.upnp,
            announce_port: torrent.announce_port,
            ..ListenerOptions::default()
        }),
        // Peers are dialed by the session's own sockets or by the leg's
        // factory, never by a proxy URL of the session's.
        connect: Some(ConnectionOptions {
            enable_tcp,
            enable_utp: Some(enable_utp),
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
        disable_local_service_discovery: !runs_local_discovery(shape),
        resolver: Some(Arc::clone(&leg.names)),
        http_client: Some(leg.http.clone()),
        ..SessionOptions::new(Arc::clone(&leg.factory))
    };
    apply_encryption(&mut options, torrent.encryption);
    options
}

/// Whether a session on a leg of `shape` runs local service discovery: only
/// one whose sockets are its own. Its groups are on the local network,
/// which a proxy or a tunnel does not reach.
pub(crate) const fn runs_local_discovery(shape: Shape) -> bool {
    shape.own_sockets
}

/// A session whose local service discovery can join its groups again.
pub(crate) trait LocalDiscovery: Send + Sync + 'static {
    fn rejoin_local_discovery(&self) -> anyhow::Result<()>;
}

impl LocalDiscovery for Session {
    fn rejoin_local_discovery(&self) -> anyhow::Result<()> {
        Session::rejoin_local_discovery(self)
    }
}

/// Joins local service discovery's groups again on the interfaces up now,
/// for each of the `open` sessions, by route and leg shape, that runs it;
/// the others are left alone. The joins run together off the async
/// threads. One that fails is logged, and that session keeps the groups it
/// joined before.
pub(crate) async fn rejoin_local_discovery<S: LocalDiscovery>(
    open: Vec<(RouteKey, Shape, Arc<S>)>,
) {
    let rejoining: Vec<_> = open
        .into_iter()
        .filter(|(_, shape, _)| runs_local_discovery(*shape))
        .map(|(key, _, session)| (key, session))
        .collect();
    if rejoining.is_empty() {
        return;
    }
    let joined = tokio::task::spawn_blocking(move || {
        for (key, session) in rejoining {
            match session.rejoin_local_discovery() {
                Ok(()) => tracing::debug!(
                    route = key.as_str(),
                    "local service discovery joined again on the interfaces up now"
                ),
                Err(error) => tracing::warn!(
                    route = key.as_str(),
                    error = format!("{error:#}"),
                    "local service discovery could not join again; it keeps the interfaces it had"
                ),
            }
        }
    })
    .await;
    if let Err(error) = joined {
        tracing::warn!(%error, "local service discovery's rejoin did not finish");
    }
}

/// Runs `run` once for each time `changes` rings, until its sender goes:
/// every change that rang since the last run is taken by the next one.
pub(crate) async fn each_change<T, F, Run>(mut changes: tokio::sync::watch::Receiver<T>, mut run: F)
where
    F: FnMut() -> Run,
    Run: std::future::Future<Output = ()>,
{
    while changes.changed().await.is_ok() {
        changes.borrow_and_update();
        run().await;
    }
}

/// Why a session could not be had.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    #[error("at most {MAX_SESSIONS} torrent routes can be in use at once")]
    Cap,
    #[error("route down: {}", .0.message())]
    RouteDown(RouteFailure),
    #[error("{}", .0.message())]
    Build(RouteFailure),
}

/// Why a session whose leg was revoked under it holds closed.
const REVOKED: RouteFailure = RouteFailure {
    reason: TorrentRouteReason::LegRevoked,
    detail: None,
};

enum Slot {
    Open {
        session: Arc<Session>,
        leg: Option<Arc<TorrentLeg>>,
        port: u16,
        // The external port its session asks a gateway to map, when it maps.
        maps: Option<u16>,
        // The route plan it opened under, whose leg it runs on.
        plan: RoutePlan,
        mappings: PortMappings,
        opening: Box<Opening>,
    },
    Held {
        reason: RouteFailure,
        kind: LegKind,
    },
    // Its session is being built, off the slots' lock: `done` closes once
    // that build ends, whether it opened or not. `last` is why the route
    // held closed before, which a build that fails for the same says once.
    Opening {
        kind: LegKind,
        done: tokio::sync::watch::Receiver<()>,
        last: Option<RouteFailure>,
    },
}

/// Why a route whose session is being built has none yet.
const OPENING: RouteFailure = RouteFailure {
    reason: TorrentRouteReason::Opening,
    detail: None,
};

impl Slot {
    // Whether the slot's session is being built now: a build whose opener
    // went away before it ended left the slot to the next one.
    fn building(&self) -> bool {
        matches!(self, Self::Opening { done, .. } if done.has_changed().is_ok())
    }

    // The slot's session, while it runs.
    fn running(&self) -> Option<&Arc<Session>> {
        match self {
            Self::Open { session, .. } if self.held().is_none() => Some(session),
            _ => None,
        }
    }

    // The kind of leg the slot's route runs on.
    fn kind(&self) -> &LegKind {
        match self {
            Self::Open { plan, .. } => &plan.kind,
            Self::Held { kind, .. } | Self::Opening { kind, .. } => kind,
        }
    }

    // Why the slot's session does not run, when it does not: it is being
    // built, it is held closed, the leg it opened on has been revoked under
    // it, or the pool member that leg holds went from under it or is
    // blocked.
    fn held(&self) -> Option<RouteFailure> {
        match self {
            Self::Held { reason, .. } => Some(reason.clone()),
            Self::Opening { .. } if self.building() => Some(OPENING),
            Self::Opening { last, .. } => Some(
                last.clone()
                    .unwrap_or_else(|| TorrentRouteReason::NotAskedToOpen.into()),
            ),
            Self::Open { leg: Some(leg), .. } => match leg.member_down() {
                Some(reason) if leg.stage.is_revoked() => Some(RouteFailure::with_detail(
                    TorrentRouteReason::LegRevoked,
                    reason,
                )),
                Some(reason) => Some(RouteFailure::leg_down(reason)),
                None => leg.stage.is_revoked().then_some(REVOKED),
            },
            Self::Open { .. } => None,
        }
    }
}

/// The last report of each of a session's port mappings, one for each
/// mapper, protocol and listen port, kept as the mappers make them.
#[derive(Clone)]
struct PortMappings(Arc<tokio::sync::watch::Sender<Vec<PortMapped>>>);

impl Default for PortMappings {
    fn default() -> Self {
        Self(Arc::new(tokio::sync::watch::Sender::new(Vec::new())))
    }
}

impl PortMappings {
    fn record(&self, event: PortMapped) {
        self.0.send_modify(|mappings| {
            match mappings.iter_mut().find(|mapping| {
                mapping.transport == event.transport
                    && mapping.protocol == event.protocol
                    && mapping.local_port == event.local_port
            }) {
                Some(mapping) => *mapping = event,
                None => mappings.push(event),
            }
        });
    }

    fn get(&self) -> Vec<PortMapped> {
        self.0.borrow().clone()
    }

    // Where a session sends its mappers' reports: each is kept as it
    // arrives, until the session lets go of the sender.
    fn follow(&self) -> PortMapEvents {
        let (events, mut reports) = tokio::sync::mpsc::unbounded_channel();
        let mappings = self.clone();
        tokio::spawn(async move {
            while let Some(event) = reports.recv().await {
                mappings.record(event);
            }
        });
        events
    }
}

/// A session as consumers read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionView {
    pub key: RouteKey,
    pub kind: LegKind,
    /// The listen port, while open.
    pub port: Option<u16>,
    /// The address the session listens on, while open and listening: TCP's,
    /// and uTP's when `utp`.
    pub listen: Option<SocketAddr>,
    pub utp: bool,
    /// The last report of each of its port mappings.
    pub mappings: Vec<PortMapped>,
    /// Its address as the internet sees it, of each family it knows one for.
    pub external: Vec<IpAddr>,
    /// What it shows trackers and peers, while open.
    pub exposure: Option<Exposure>,
    /// Why it holds closed.
    pub held: Option<RouteFailure>,
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
    // The sessions closing off the open path, each until it has stopped.
    closing: Closing,
    // Wakes the follower of the routes when the leg under an open session
    // is revoked.
    revoked: Arc<tokio::sync::Notify>,
    // Rung by every session as an announce table or a metadata fetch's
    // phase changes.
    trackers_changed: Arc<tokio::sync::Notify>,
    // Counts the times the egresses' limits were put in force.
    egress_limits_applied: tokio::sync::watch::Sender<u64>,
    // The connection budget and check turns every session shares, made
    // with the first session: the process has one descriptor limit and
    // one disk however many routes it runs.
    shared_limits: std::sync::OnceLock<SharedLimits>,
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
            closing: Closing::default(),
            revoked: Arc::default(),
            trackers_changed: Arc::default(),
            egress_limits_applied: tokio::sync::watch::Sender::new(0),
            shared_limits: std::sync::OnceLock::new(),
        }
    }

    /// Resolves once the leg under an open session has been revoked since
    /// last asked: that session is `outdated` until its route opens again.
    pub async fn revoked(&self) {
        self.revoked.notified().await;
    }

    /// Resolves once an announce table of an open session, or the phase of
    /// a metadata fetch on one, has changed since last asked.
    pub async fn trackers_changed(&self) {
        self.trackers_changed.notified().await;
    }

    // Once `leg` is revoked under `session`, or the pool member it holds
    // goes from under it, the session may send no more, and the follower
    // wakes; unless the session stops first.
    fn watch(&self, key: &RouteKey, leg: &TorrentLeg, session: &Arc<Session>) {
        let (key, gone) = (key.clone(), leg.gone());
        let stopped = session.cancellation_token().clone();
        let session = Arc::downgrade(session);
        let revoked = Arc::clone(&self.revoked);
        tokio::spawn(async move {
            tokio::select! {
                detail = gone => {
                    let reason = detail.map_or(REVOKED, |detail| {
                        RouteFailure::with_detail(TorrentRouteReason::LegRevoked, detail)
                    });
                    tracing::info!(
                        route = key.as_str(),
                        reason = %reason.message(),
                        "a route's leg was revoked under its open session"
                    );
                    if let Some(session) = session.upgrade() {
                        withhold(&key, &session, &reason);
                    }
                    revoked.notify_one();
                }
                () = stopped.cancelled() => {}
            }
        });
    }

    /// The torrents' download rate across the open sessions.
    pub fn rates(&self) -> &SessionRates {
        &self.rates
    }

    /// Put in force what each egress an open session runs on allows now.
    /// Run whenever an egress's caps or quota state may have changed: the
    /// open sessions take it live, with no restart.
    pub fn apply_egress_limits(&self) {
        self.rates
            .set_egresses(|egress_id| self.routes.egress_rates(egress_id));
        self.egress_limits_applied
            .send_modify(|applied| *applied = applied.wrapping_add(1));
    }

    /// Counts the times the egresses' limits were put in force.
    pub fn egress_limits_applied(&self) -> tokio::sync::watch::Receiver<u64> {
        self.egress_limits_applied.subscribe()
    }

    /// The torrent settings. The open sessions take the limits, the
    /// encryption, connections per address (libtorrent
    /// `allow_multiple_connections_per_ip`, read afresh at each judgment)
    /// and the IP filter at once (libtorrent `session_impl::set_ip_filter`),
    /// the filter once the blocklist it names has loaded; a blocklist that
    /// does not load holds them closed. The transports, the DHT's bootstrap
    /// nodes and the stopped announce timeout are a session's from when it
    /// opens, so each takes them when its route next opens. A session whose
    /// ports, port mapping or directory they change is `outdated` until it
    /// opens again.
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
            self.closing.clone(),
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
    pub(crate) async fn ip_filter(&self, torrent: &TorrentConfig) -> Result<IpFilter, String> {
        let _loading = self.loading.lock().await;
        load_blocklist(&*self.routes, &self.config, torrent).await?;
        lock(&self.config)
            .filter_of(torrent)
            .ok_or_else(|| "the blocklist did not load".to_owned())
    }

    // The first route's SSL port, the settings' or the one chosen for them,
    // read or chosen off the async threads.
    async fn ssl_base_port(&self, configured: Option<u16>) -> Result<u16, String> {
        if let Some(port) = configured {
            return Ok(port);
        }
        let (db, table) = (self.db.clone(), Arc::clone(&self.port_slots));
        tokio::task::spawn_blocking(move || {
            let _table = table.lock().unwrap_or_else(PoisonError::into_inner);
            chosen_ssl_port(&db)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())
    }

    // The route's port slot, read or taken off the async threads.
    async fn port_slot(&self, key: &RouteKey) -> Result<u16, String> {
        let (db, key, table) = (self.db.clone(), key.clone(), Arc::clone(&self.port_slots));
        tokio::task::spawn_blocking(move || {
            let _table = table.lock().unwrap_or_else(PoisonError::into_inner);
            port_slot(&db, &key)
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
            release_slots(&db, gone)
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

    /// Why no route may open, while none may: torrents are off.
    pub fn off(&self) -> Option<RouteFailure> {
        (!self.on()).then(|| TorrentRouteReason::TorrentsDisabled.into())
    }

    /// Let sessions open, or close every one and open none until turned on.
    pub async fn turn(&self, on: bool) {
        {
            let mut slots = self.slots.lock().await;
            self.on.store(on, Ordering::Release);
            if on {
                return;
            }
            self.close_all(std::mem::take(&mut *slots));
        }
        self.closing.all_stopped().await;
    }

    // Close every session of `slots` off the open path: each stops with no
    // lock held, as its trackers hear `stopped`, and holds its ports until
    // it has.
    fn close_all(&self, slots: BTreeMap<RouteKey, Slot>) {
        for (key, slot) in slots {
            if let Slot::Open {
                session, leg, maps, ..
            } = slot
            {
                let ports = held_ports(&session, maps);
                self.closing.stop(&key, session, leg, ports);
            }
        }
    }

    pub fn routes(&self) -> &Arc<dyn Routes> {
        &self.routes
    }

    /// Whether `plan`'s route carries DHT. Its open session says; a route
    /// with none open yet has no dialer to ask, so a ladder leg says no.
    pub async fn carries_dht(&self, plan: &RoutePlan) -> bool {
        match self
            .slots
            .lock()
            .await
            .get(&plan.key)
            .and_then(Slot::running)
        {
            Some(session) => session.get_dht().is_some(),
            None => plan.kind.shape(None).dht,
        }
    }

    /// The open session of `plan`'s route, built when it is not open yet. A
    /// route that is down holds closed. A session built here names itself as
    /// the client of `opener`, the torrent that opens it, wherever it names a
    /// client for all its torrents; it keeps that client while it is open.
    pub async fn session(
        &self,
        plan: &RoutePlan,
        opener: &Identity,
    ) -> Result<Arc<Session>, SessionError> {
        let running = self
            .slots
            .lock()
            .await
            .get(&plan.key)
            .and_then(Slot::running)
            .cloned();
        if let Some(session) = running {
            self.joined(plan);
            return Ok(session);
        }
        let port_slot = self.port_slot(&plan.key).await;
        // The slot is marked as opening and the lock let go while the
        // session is built: a build waits on its leg, which may take as long
        // as a tunnel's handshake, and no reader of the sessions waits on
        // that. An opener that finds the route opening waits for that build
        // and takes what it left.
        let (building, last) = loop {
            let mut slots = self.slots.lock().await;
            if let Some(off) = self.off() {
                return Err(SessionError::RouteDown(off));
            }
            if let Some(session) = slots.get(&plan.key).and_then(Slot::running) {
                self.joined(plan);
                return Ok(Arc::clone(session));
            }
            if let Some(Slot::Opening { done, .. }) = slots.get(&plan.key)
                && done.has_changed().is_ok()
            {
                let mut done = done.clone();
                drop(slots);
                let _ = done.changed().await;
                continue;
            }
            if !slots.contains_key(&plan.key) && slots.len() >= MAX_SESSIONS {
                return Err(SessionError::Cap);
            }
            // A session whose leg was revoked under it stops before its route
            // opens again.
            let last = match slots.remove(&plan.key) {
                Some(Slot::Open {
                    session, leg, maps, ..
                }) => {
                    self.closing.close(&plan.key, session, leg, maps);
                    None
                }
                Some(Slot::Held { reason, .. }) => Some(reason),
                Some(Slot::Opening { last, .. }) => last,
                None => None,
            };
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
            let (building, done) = tokio::sync::watch::channel(());
            slots.insert(
                plan.key.clone(),
                Slot::Opening {
                    kind: plan.kind.clone(),
                    done,
                    last: last.clone(),
                },
            );
            break (building, last);
        };
        // The peer limit is each torrent's, whatever else is open.
        let peers = self.config().max_peers_total as usize;
        let mappings = PortMappings::default();
        let built = self.build(plan, opener, port_slot, peers, &mappings).await;
        let mut slots = self.slots.lock().await;
        // The slot may have changed while the session was built: torrents
        // turned off, the route held or dropped. What stands now wins, and
        // a session built for a slot no longer this build's stops unused.
        let ours = matches!(
            slots.get(&plan.key),
            Some(Slot::Opening { done, .. }) if done.same_channel(&building.subscribe())
        );
        if !ours {
            let now = self.off().or_else(|| slots.get(&plan.key).and_then(Slot::held));
            if let Ok((session, leg, _, maps, _)) = built {
                self.closing.close(&plan.key, session, leg, maps);
            }
            return Err(SessionError::RouteDown(now.unwrap_or(REVOKED)));
        }
        match built {
            Ok((session, leg, port, maps, opening)) => {
                self.rates.add(
                    &session,
                    plan.egress_id,
                    self.routes.egress_rates(plan.egress_id),
                );
                self.configured().add(&session);
                if let Some(leg) = &leg {
                    self.watch(&plan.key, leg, &session);
                }
                tracing::info!(route = plan.key.as_str(), port, "a route's session opened");
                slots.insert(
                    plan.key.clone(),
                    Slot::Open {
                        session: Arc::clone(&session),
                        leg,
                        port,
                        maps,
                        plan: plan.clone(),
                        mappings,
                        opening: Box::new(opening),
                    },
                );
                Ok(session)
            }
            Err(error) => {
                // Logged once for each failure that differs from the one
                // the route was held for before: a route retried for the
                // same cause says nothing more.
                if last.as_ref() != Some(&error) {
                    tracing::warn!(
                        route = plan.key.as_str(),
                        reason = %error.message(),
                        "a route's session could not be opened"
                    );
                }
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

    // A member session handed to `plan`'s scope, which may not be the scope
    // that opened it: the scope's own route starts, if it has not, so the
    // session outlives the opener's route while this scope's pool holds the
    // member.
    fn joined(&self, plan: &RoutePlan) {
        if plan.member.is_some() {
            self.routes
                .join(plan, self.config().max_peers_total as usize);
        }
    }

    async fn build(
        &self,
        plan: &RoutePlan,
        opener: &Identity,
        slot: Result<u16, String>,
        peers: usize,
        mappings: &PortMappings,
    ) -> Result<
        (
            Arc<Session>,
            Option<Arc<TorrentLeg>>,
            u16,
            Option<u16>,
            Opening,
        ),
        RouteFailure,
    > {
        let slot = slot.map_err(|detail| {
            RouteFailure::with_detail(TorrentRouteReason::SessionStateUnreadable, detail)
        })?;
        let torrent = self.config();
        let port = slot_port(&torrent, slot)
            .ok_or_else(|| RouteFailure::from(TorrentRouteReason::ListenPortUnavailable))?;
        let dir = self.env.root_of(&torrent).join(plan.key.as_str());
        std::fs::create_dir_all(&dir).map_err(|error| {
            RouteFailure::with_detail(TorrentRouteReason::StateDirectoryFailed, error.to_string())
        })?;
        let leg = self.routes.open(plan, peers).await.map_err(|detail| {
            RouteFailure::with_detail(TorrentRouteReason::LegOpenFailed, detail)
        })?;
        let opening = Opening::of(&self.env, &torrent, leg.shape);
        let ip_filter = self.ip_filter(&torrent).await.map_err(|detail| {
            RouteFailure::with_detail(TorrentRouteReason::BlocklistFailed, detail)
        })?;
        let ssl_port = if torrent.ssl_enabled {
            let base = self
                .ssl_base_port(torrent.ssl_port)
                .await
                .map_err(|detail| {
                    RouteFailure::with_detail(TorrentRouteReason::SessionStateUnreadable, detail)
                })?;
            let port = base
                .checked_add(slot)
                .ok_or_else(|| RouteFailure::from(TorrentRouteReason::ListenPortUnavailable))?;
            Some(port)
        } else {
            None
        };
        // A session that closed off the open path may still hold one of the
        // ports this one takes, or still have a gateway's mapping of the
        // external port this one maps to remove: only then does it wait, for
        // that session.
        let maps = maps(&torrent, leg.shape, port);
        self.closing
            .released(plan, [Some(port), ssl_port, maps].into_iter().flatten())
            .await;
        let mut options = session_options(&leg, port, &dir, peers, &torrent, ip_filter, opener);
        options.port_map_events = Some(mappings.follow());
        options.trackers_changed = Some(Arc::clone(&self.trackers_changed));
        options.shared_limits = Some(
            self.shared_limits
                .get_or_init(|| {
                    SharedLimits::new(
                        session_limits(&torrent),
                        options.connections_slack,
                        options.concurrent_init_limit.unwrap_or(1),
                    )
                })
                .clone(),
        );
        // SSL torrents' peers come to a port of the route's own beside its
        // peer port.
        if let Some(listen) = options.listen.as_mut() {
            listen.ssl_listen_port = ssl_port;
        }
        Session::new_with_opts(dir.join("output"), options)
            .await
            .map(|session| (session, leg.leg, port, maps, opening))
            .map_err(|error| {
                let reason = if error.downcast_ref::<ListenFailed>().is_some() {
                    TorrentRouteReason::ListenPortBindFailed
                } else {
                    TorrentRouteReason::SessionBuildFailed
                };
                RouteFailure::with_detail(reason, format!("{error:#}"))
            })
    }

    /// The routes whose open session would open otherwise now: its leg was
    /// revoked under it, or the settings as they stand would bind or
    /// announce another port, map it or not, or keep its state elsewhere.
    /// Each opens again once it is dropped.
    /// A session whose leg may not send, revoked, down or out of quota, may
    /// send no more from here on, even before the watch on its leg wakes:
    /// nothing taken off it after this sends its trackers `stopped`.
    pub async fn outdated(&self) -> BTreeSet<RouteKey> {
        let torrent = self.config();
        let slots = self.slots.lock().await;
        slots
            .iter()
            .filter_map(|(key, slot)| {
                let Slot::Open {
                    session, opening, ..
                } = slot
                else {
                    return None;
                };
                if let Some(reason) = self.silenced(slot) {
                    withhold(key, session, &reason);
                }
                (slot.held().is_some()
                    || **opening != Opening::of(&self.env, &torrent, opening.shape))
                .then(|| key.clone())
            })
            .collect()
    }

    // Why `slot`'s session may send nothing more, when it may not: its leg
    // was revoked under it, or is down or out of quota now.
    fn silenced(&self, slot: &Slot) -> Option<RouteFailure> {
        match slot {
            Slot::Open { plan, .. } => slot.held().or_else(|| self.routes.leg_down(plan)),
            Slot::Held { .. } | Slot::Opening { .. } => slot.held(),
        }
    }

    /// The session each route runs now: a torrent attached to another has
    /// lost it, and moves to this one.
    pub async fn running(&self) -> BTreeMap<RouteKey, Arc<Session>> {
        let slots = self.slots.lock().await;
        slots
            .iter()
            .filter_map(|(key, slot)| Some((key.clone(), Arc::clone(slot.running()?))))
            .collect()
    }

    /// Joins local service discovery's groups again on every running
    /// session that runs it, on the interfaces up now.
    pub async fn rejoin_local_discovery(&self) {
        let open: Vec<_> = {
            let slots = self.slots.lock().await;
            slots
                .iter()
                .filter_map(|(key, slot)| {
                    let session = slot.running()?;
                    let Slot::Open { opening, .. } = slot else {
                        return None;
                    };
                    Some((key.clone(), opening.shape, Arc::clone(session)))
                })
                .collect()
        };
        rejoin_local_discovery(open).await;
    }

    /// Joins local service discovery again each time `changes` rings, until
    /// its sender goes. Changes that ring while a rejoin runs are taken by
    /// one more, on the interfaces up then.
    pub async fn follow_interfaces(
        &self,
        changes: tokio::sync::watch::Receiver<MulticastInterfaces>,
    ) {
        each_change(changes, || self.rejoin_local_discovery()).await;
    }

    /// Close `key`'s session and hold it closed for `reason`, its route
    /// down: its trackers hear no `stopped` from it.
    pub async fn hold(&self, key: &RouteKey, reason: RouteFailure) {
        let mut slots = self.slots.lock().await;
        let Some(slot) = slots.remove(key) else {
            return;
        };
        let kind = match slot {
            Slot::Open {
                session,
                leg,
                maps,
                plan,
                ..
            } => {
                withhold(key, &session, &reason);
                self.closing.close(key, session, leg, maps);
                plan.kind
            }
            Slot::Held { kind, .. } | Slot::Opening { kind, .. } => kind,
        };
        slots.insert(key.clone(), Slot::Held { reason, kind });
    }

    /// Drop `key`'s session, open or held. The route keeps its port slot,
    /// and may open again at once: the session stops off the open path.
    pub async fn drop_route(&self, key: &RouteKey) {
        let mut slots = self.slots.lock().await;
        if let Some(Slot::Open {
            session, leg, maps, ..
        }) = slots.remove(key)
        {
            self.closing.close(key, session, leg, maps);
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

    /// Each route's session as the exporter reads it: from its counters
    /// while it runs, and as held while it does not. Kept in `read` under
    /// the sessions' lock, so of two readings the later kept is the later
    /// taken.
    pub(crate) async fn read_into(
        &self,
        read: &Mutex<Vec<crate::torrents::metrics::SessionSeries>>,
    ) {
        use crate::torrents::metrics::SessionSeries;
        let slots = self.slots.lock().await;
        let sessions = slots
            .iter()
            .map(|(key, slot)| match (slot, slot.held()) {
                (Slot::Open { session, port, .. }, None) => {
                    SessionSeries::open(key.as_str(), session, *port)
                }
                _ => SessionSeries::held(key.as_str()),
            })
            .collect();
        *read.lock().unwrap_or_else(PoisonError::into_inner) = sessions;
    }

    pub async fn stop_all(&self) {
        let slots = std::mem::take(&mut *self.slots.lock().await);
        self.close_all(slots);
        self.closing.all_stopped().await;
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
    blocked: Option<Blocked>,
}

// The blocklist and banned entries a blocked list was made of, and the list.
type Blocked = (Option<Arc<IpRanges>>, Vec<String>, Arc<IpRanges>);

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
                let blocked = blocked_by(torrent, list.as_ref());
                self.blocked = Some((list, torrent.banned_ips.clone(), Arc::clone(&blocked)));
                blocked
            }
        };
        Some(filter_over(torrent, blocked))
    }

    // Every open session holds to the settings' limits, encryption and
    // connections per address and, once the blocklist they name is loaded,
    // to their IP filter; whether it is.
    fn lay_over(&mut self) -> bool {
        let limits = session_limits(&self.torrent);
        let encryption = encryption_policy(self.torrent.encryption);
        let torrent = self.torrent.clone();
        let filter = self.filter_of(&torrent);
        self.open.retain(|session| {
            session.upgrade().is_some_and(|session| {
                let open = !session.cancellation_token().is_cancelled();
                if open && session.limits() != limits {
                    session.set_limits(limits);
                }
                if open {
                    session.set_encryption(encryption);
                    session.set_allow_multiple_connections_per_ip(
                        torrent.allow_multiple_connections_per_ip,
                    );
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

// The addresses `torrent` blocks: `list`, the blocklist it names, with its
// banned addresses added.
fn blocked_by(torrent: &TorrentConfig, list: Option<&Arc<IpRanges>>) -> Arc<IpRanges> {
    let bans = IpRanges::of_inclusive(torrent.banned_ranges());
    match list {
        Some(list) if torrent.banned_ips.is_empty() => Arc::clone(list),
        Some(list) => Arc::new(IpRanges::union([&**list, &bans])),
        None => Arc::new(bans),
    }
}

fn filter_over(torrent: &TorrentConfig, blocked: Arc<IpRanges>) -> IpFilter {
    IpFilter {
        blocked,
        no_privileged_ports: torrent.block_privileged_ports,
        trackers: torrent.filter_trackers,
    }
}

/// The IP filter `torrent` asks for while it names no blocklist; none while
/// it names one, which only the sessions' loader loads.
pub fn unlisted_filter(torrent: &TorrentConfig) -> Option<IpFilter> {
    torrent
        .blocklist_source()
        .is_none()
        .then(|| filter_over(torrent, blocked_by(torrent, None)))
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
    closing: Closing,
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
                    session,
                    leg,
                    maps,
                    plan,
                    ..
                }) = slots.remove(&key)
                {
                    closing.close(&key, session, leg, maps);
                    let reason = RouteFailure::with_detail(
                        TorrentRouteReason::BlocklistFailed,
                        reason.clone(),
                    );
                    slots.insert(
                        key,
                        Slot::Held {
                            reason,
                            kind: plan.kind,
                        },
                    );
                }
            }
        }
        Err(_) => {}
    }
    applied.send_modify(|applied| *applied = given.max(*applied));
}

fn view(key: &RouteKey, slot: &Slot) -> SessionView {
    match (slot, slot.held()) {
        (
            Slot::Open {
                session,
                port,
                mappings,
                plan,
                opening,
                ..
            },
            None,
        ) => {
            let mappings = mappings.get();
            SessionView {
                key: key.clone(),
                kind: slot.kind().clone(),
                port: Some(*port),
                listen: session.listen_addr(),
                utp: session.listens_utp(),
                exposure: Some(Exposure::of(session, opening.shape, plan.names, &mappings)),
                mappings,
                external: session.external_ips(),
                held: None,
            }
        }
        (_, held) => SessionView {
            key: key.clone(),
            kind: slot.kind().clone(),
            port: None,
            listen: None,
            utp: false,
            mappings: Vec::new(),
            external: Vec::new(),
            exposure: None,
            held,
        },
    }
}

// The session may send no more: its leg was revoked under it, or its route
// is down. From here on its trackers hear no `stopped` from it, whatever
// stops it or takes a torrent off it.
fn withhold(key: &RouteKey, session: &Session, reason: &RouteFailure) {
    tracing::info!(
        route = key.as_str(),
        reason = ?reason.reason,
        detail = ?reason.detail,
        "a route's session sends its trackers no stopped announce"
    );
    session.withhold_stopped();
}

// Stop a session; its leg's bridge stops with the leg. The leg's stage is
// the route's, and stays. The session's own steps log as it takes them.
async fn close(key: &RouteKey, session: Arc<Session>, leg: Option<Arc<TorrentLeg>>) {
    tracing::info!(route = key.as_str(), "stopping a route's session");
    session.stop().await;
    drop(session);
    drop(leg);
    tracing::info!(route = key.as_str(), "a route's session stopped");
}

/// The sessions stopping off the open path: a route opens again while its
/// last session's trackers hear `stopped` and its port mappings are removed.
/// Each holds its listen ports until it has stopped.
#[derive(Clone, Default)]
struct Closing(Arc<Mutex<Vec<Stopping>>>);

// The ports `session` listens on, and the external port whose mapping it
// removes as it stops, `maps`: a gateway knows a mapping by that port alone.
fn held_ports(session: &Session, maps: Option<u16>) -> Vec<u16> {
    [session.listen_addr(), session.ssl_listen_addr()]
        .into_iter()
        .flatten()
        .map(|addr| addr.port())
        .chain(maps)
        .collect()
}

struct Stopping {
    route: RouteKey,
    ports: Vec<u16>,
    stopped: tokio::sync::watch::Receiver<bool>,
}

impl Closing {
    // Stop `session`, which maps `maps` when it maps, in the background.
    fn close(
        &self,
        key: &RouteKey,
        session: Arc<Session>,
        leg: Option<Arc<TorrentLeg>>,
        maps: Option<u16>,
    ) {
        let ports = held_ports(&session, maps);
        tracing::info!(
            route = key.as_str(),
            ?ports,
            "a route's session closes off the open path; the route may open again at once"
        );
        self.stop(key, session, leg, ports);
    }

    // Stop `session`, which holds `ports`, in the background: each of them
    // is held until it has stopped.
    fn stop(
        &self,
        key: &RouteKey,
        session: Arc<Session>,
        leg: Option<Arc<TorrentLeg>>,
        ports: Vec<u16>,
    ) {
        let (stopped, waiting) = tokio::sync::watch::channel(false);
        self.lock().push(Stopping {
            route: key.clone(),
            ports,
            stopped: waiting,
        });
        let closing = self.clone();
        let key = key.clone();
        tokio::spawn(async move {
            close(&key, session, leg).await;
            stopped.send_replace(true);
            closing
                .lock()
                .retain(|stopping| !*stopping.stopped.borrow());
        });
    }

    // Resolves once no session stopping holds any of `ports`.
    async fn released(&self, plan: &RoutePlan, ports: impl Iterator<Item = u16>) {
        let ports: Vec<u16> = ports.filter(|port| *port != 0).collect();
        let holding: Vec<_> = self
            .lock()
            .iter()
            .filter(|stopping| stopping.ports.iter().any(|port| ports.contains(port)))
            .map(|stopping| (stopping.route.clone(), stopping.stopped.clone()))
            .collect();
        for (holder, mut stopped) in holding {
            tracing::info!(
                route = plan.key.as_str(),
                holder = holder.as_str(),
                ?ports,
                "a route's session waits on a stopping session to release its port"
            );
            let _ = stopped.wait_for(|stopped| *stopped).await;
            tracing::info!(
                route = plan.key.as_str(),
                holder = holder.as_str(),
                "the stopping session released the port"
            );
        }
    }

    // Resolves once every session stopping has stopped.
    async fn all_stopped(&self) {
        let waiting: Vec<_> = self
            .lock()
            .iter()
            .map(|stopping| stopping.stopped.clone())
            .collect();
        for mut stopped in waiting {
            let _ = stopped.wait_for(|stopped| *stopped).await;
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Stopping>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
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

/// The port a route on port slot `slot` listens on under `torrent`, while
/// one is left.
pub fn slot_port(torrent: &TorrentConfig, slot: u16) -> Option<u16> {
    torrent.base_port.checked_add(slot)
}

/// The port the route's session listens on under `torrent`: its slot's, or
/// the first slot's while it has none.
pub fn listen_port(
    db: &Database,
    torrent: &TorrentConfig,
    key: &RouteKey,
) -> Result<Option<u16>, StateError> {
    let slot = load_slots(db)?.get(key.as_str()).copied().unwrap_or(0);
    Ok(slot_port(torrent, slot))
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

/// Where a listen port is drawn when the settings name none, as qBittorrent
/// draws its own on the first start (`SessionImpl`, a `Port` of -1): above
/// the privileged ports and below the ephemeral range. The SSL ports are
/// drawn from the range above, so no route's slot of one reaches a slot of
/// the other.
pub const LISTEN_PORTS: std::ops::Range<u16> = 10_000..30_000;
const SSL_PORTS: std::ops::Range<u16> = 30_000..50_000;

/// A port from `ports`, at random.
pub fn random_port(ports: std::ops::Range<u16>) -> Result<u16, StateError> {
    let mut bytes = [0u8; 2];
    getrandom::fill(&mut bytes).map_err(|error| StateError::Database(error.to_string()))?;
    Ok(ports.start + u16::from_le_bytes(bytes) % ports.len() as u16)
}

/// The SSL port chosen while the settings name none: at random the first
/// time, from [`SSL_PORTS`], then kept, so peers find it again.
pub fn chosen_ssl_port(db: &Database) -> Result<u16, StateError> {
    if let Some(port) = db
        .get_setting(CHOSEN_SSL_PORT_KEY)?
        .and_then(|value| value.parse().ok())
    {
        return Ok(port);
    }
    let port = random_port(SSL_PORTS)?;
    db.set_setting(CHOSEN_SSL_PORT_KEY, &port.to_string())?;
    Ok(port)
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
