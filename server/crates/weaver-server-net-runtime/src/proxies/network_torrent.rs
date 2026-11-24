// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::PoisonError;

use super::*;
use weaver_tunnel::{
    TunnelError, TunnelStream,
    bridge::{Bridge, ConnectionOutcome},
    endpoint::DatagramTransport,
    pipe::{Dialed, Inbound, InboundListener, Purpose, Resolution},
};

/// A torrent session's leg: a leg of its consumer's live route. The stage
/// belongs to the route, which revokes it when the leg changes; the bridge
/// an HTTP client rides belongs to this leg, and stops with it. On a pool
/// leg the session holds one member through a stage of its own, which the
/// route's revoking leaves alone: the member is the proxy on the leg's
/// egress, whichever route's pool placed it there, and the stage is revoked
/// once no pool on that egress holds that proxy as it was.
pub struct TorrentLeg {
    pub stage: Arc<Revocable>,
    member: Option<Arc<PinnedMember>>,
    // The route's stage the leg's connections count on.
    counted: std::sync::Weak<Revocable>,
    route: Arc<super::weighted::Weighted>,
    handle: tokio::runtime::Handle,
    bridge: Mutex<Option<PathBridge>>,
    // The bridge the leg's web seed transfers ride, apart from its trackers'.
    seeds: Mutex<Option<PathBridge>>,
    // The bridges a torrent route's own lookups ride, one for each route
    // on the leg that names apart from it and each purpose it dials for,
    // stopped with the leg.
    scoped: Mutex<Vec<PathBridge>>,
}
impl TorrentLeg {
    /// The leg's dialer, whose connections the route counts as its own.
    pub fn dialer(&self) -> Arc<dyn Dialer> {
        Arc::new(Carried {
            stage: self.stage.clone(),
            counted: self.counted.clone(),
            route: self.route.clone(),
        })
    }
    // The bridge onto the leg's stage its trackers and metainfo ride, started
    // the first time it is asked for. Their dials take the tunnel's reserve.
    pub fn bridge(&self) -> Result<PathBridge, String> {
        self.started(&self.bridge, Purpose::TorrentHttp)
    }
    // The bridge onto the leg's stage its web seed transfers ride, started
    // the first time it is asked for. Their dials take the torrent side, so
    // a long web seed transfer never holds what announces are kept.
    pub fn seed_bridge(&self) -> Result<PathBridge, String> {
        self.started(&self.seeds, Purpose::TorrentWebSeed)
    }
    fn started(
        &self,
        slot: &Mutex<Option<PathBridge>>,
        purpose: Purpose,
    ) -> Result<PathBridge, String> {
        // A panic while starting the bridge left it unstarted: start it.
        let mut bridge = slot.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(bridge) = bridge.as_ref() {
            return Ok(bridge.clone());
        }
        let started = self.start_bridge(self.dialer(), purpose)?;
        *bridge = Some(started.clone());
        Ok(started)
    }
    /// A bridge of its own onto `dialer`, the leg's dialer under a torrent
    /// route's names: what that route's HTTP for `purpose` rides (its
    /// trackers and metainfo, or its web seed transfers), its checked
    /// addresses pinned apart from every other route's. It stops with the
    /// leg, or once nothing holds what it hands back nor any client built
    /// on it.
    pub fn bridge_on(
        self: &Arc<Self>,
        dialer: Arc<dyn Dialer>,
        purpose: Purpose,
    ) -> Result<PathBridge, String> {
        let started = self.start_bridge(dialer, purpose)?;
        self.scoped
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(started.clone());
        let hold = BridgeHold {
            leg: Arc::downgrade(self),
            bridge: started.clone(),
        };
        Ok(PathBridge {
            keep: Some(Arc::new(hold)),
            ..started
        })
    }
    /// How many of its own bridges from `bridge_on` the leg runs.
    #[cfg(any(test, feature = "test-support"))]
    pub fn own_bridges(&self) -> usize {
        self.scoped
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
    // Stop `bridge`, one of the leg's own from `bridge_on`, and let go of
    // it.
    fn release_bridge(&self, bridge: &PathBridge) {
        let mut scoped = self.scoped.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(at) = scoped
            .iter()
            .position(|held| Arc::ptr_eq(&held.bridge, &bridge.bridge))
        else {
            return;
        };
        let released = scoped.swap_remove(at);
        drop(scoped);
        self.handle.spawn(async move { released.revoke().await });
    }
    fn start_bridge(
        &self,
        dialer: Arc<dyn Dialer>,
        purpose: Purpose,
    ) -> Result<PathBridge, String> {
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|_| "could not generate bridge credentials")?;
        let path = Arc::new(TorrentProvider::new(dialer, purpose));
        Ok(PathBridge {
            bridge: Bridge::start(
                &self.handle,
                path.clone(),
                self.stage.budget(),
                ("weaver".into(), hex::encode(secret)),
            )
            .map_err(|e| e.to_string())?,
            path,
            keep: None,
        })
    }
    /// Why the pool member the leg holds carries nothing now, when it does
    /// not: no pool on its egress holds it as it was, which revokes the leg,
    /// or it is blocked. None on a leg that holds no member.
    pub fn member_down(&self) -> Option<String> {
        let member = self.member.as_ref()?;
        match held(&self.stage, member) {
            Ok(pin) => pin.blocked(),
            Err(gone) => Some(gone),
        }
    }
    /// The first proxy hop on the leg's path whose own last attempt failed,
    /// and what it came to: on a pool leg, its member's.
    pub fn failing_hop(&self) -> Option<weaver_tunnel::pipe::FailingHop> {
        self.stage.failing_hop()
    }
    /// A session on the leg opened, or could not for a hop of its path:
    /// on a pool leg, its member's setup evidence. Nothing on any other leg.
    pub fn opened(&self, reached: bool) {
        if let Some(member) = &self.member
            && let Ok(pin) = member.current()
        {
            pin.opened(reached);
        }
    }
    /// Resolves once the leg can carry nothing more: its stage revoked, with
    /// why when its member went from under it.
    pub fn gone(&self) -> impl std::future::Future<Output = Option<String>> + Send + 'static {
        let stage = self.stage.clone();
        let member = self.member.clone();
        async move {
            let Some(member) = member else {
                stage.cancelled().await;
                return None;
            };
            loop {
                if stage.is_revoked() {
                    return member.gone.get().cloned();
                }
                let pin = match held(&stage, &member) {
                    Ok(pin) => pin,
                    Err(gone) => return Some(gone),
                };
                tokio::select! {
                    () = stage.cancelled() => {}
                    () = pin.changed() => {}
                }
            }
        }
    }
}
// The member `member` holds now, or why it holds none, `stage` revoked once
// it holds none.
fn held(stage: &Revocable, member: &PinnedMember) -> Result<Arc<MemberPin>, String> {
    member.current().inspect_err(|_| stage.revoke())
}
impl Drop for TorrentLeg {
    fn drop(&mut self) {
        let scoped = std::mem::take(
            self.scoped
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner),
        );
        let mut own = Vec::with_capacity(2);
        for slot in [&mut self.bridge, &mut self.seeds] {
            own.extend(
                slot.get_mut()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take(),
            );
        }
        for bridge in own.into_iter().chain(scoped) {
            self.handle.spawn(async move { bridge.revoke().await });
        }
    }
}
// The pool member a session on a pool leg goes through: the one it was
// placed on, or, once that member's pool lets it go, the same proxy through
// the same session in another pool on the leg's egress. Once no pool there
// holds it as it was, it is gone for good.
struct PinnedMember {
    pools: RunningPools,
    egress_id: u32,
    pin: Mutex<Arc<MemberPin>>,
    gone: std::sync::OnceLock<String>,
}
impl PinnedMember {
    fn pin(&self) -> Arc<MemberPin> {
        self.pin
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
    // The member held now, moved to another pool on the egress when its own
    // let it go; or why there is none.
    fn current(&self) -> Result<Arc<MemberPin>, String> {
        if let Some(gone) = self.gone.get() {
            return Err(gone.clone());
        }
        let pin = self.pin();
        let Some(stale) = pin.stale() else {
            return Ok(pin);
        };
        let moved = self
            .pools
            .lock()
            .expect("network pools")
            .iter()
            .filter(|((_, egress_id), _)| *egress_id == self.egress_id)
            .find_map(|(_, pool)| pool.pin_same(&pin));
        match moved {
            Some(moved) => {
                *self.pin.lock().unwrap_or_else(PoisonError::into_inner) = moved.clone();
                Ok(moved)
            }
            None => Err(self.gone.get_or_init(|| stale).clone()),
        }
    }
    fn dialing(&self) -> Result<Arc<MemberPin>, DialError> {
        self.current().map_err(DialError::Skipped)
    }
}
#[async_trait::async_trait]
impl Dialer for PinnedMember {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        self.dialing()?.dial(target).await
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.dialing()?.resolve(host).await
    }
    fn offers(&self) -> weaver_tunnel::pipe::Transports {
        self.pin().offers()
    }
    fn room(&self, purpose: &Purpose) -> Result<(), Vec<Arc<tokio::sync::Notify>>> {
        self.current().map_err(|_| Vec::new())?.room(purpose)
    }
    fn wait_room(&self, purpose: &Purpose) -> Result<(), weaver_tunnel::pipe::Capacity> {
        self.current()
            .map_err(|_| weaver_tunnel::pipe::Capacity::default())?
            .wait_room(purpose)
    }
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        self.current().ok()?.datagrams()
    }
    fn inbound(self: Arc<Self>) -> Option<Arc<dyn Inbound>> {
        self.current().ok()?.inbound()
    }
    fn bind_local_udp(
        &self,
        local: std::net::SocketAddr,
    ) -> Option<std::io::Result<std::net::UdpSocket>> {
        match self.current() {
            Ok(pin) => pin.bind_local_udp(local),
            Err(gone) => Some(Err(std::io::Error::other(gone))),
        }
    }
    fn source_toward(&self, to: std::net::SocketAddr) -> Option<std::io::Result<std::net::IpAddr>> {
        match self.current() {
            Ok(pin) => pin.source_toward(to),
            Err(gone) => Some(Err(std::io::Error::other(gone))),
        }
    }
    fn ipv6_sources(&self) -> Option<std::io::Result<weaver_tunnel::egress::Ipv6Sources>> {
        match self.current() {
            Ok(pin) => pin.ipv6_sources(),
            Err(gone) => Some(Err(std::io::Error::other(gone))),
        }
    }
    fn failing_hop(&self) -> Option<weaver_tunnel::pipe::FailingHop> {
        self.pin().failing_hop()
    }
    fn budget(&self) -> Duration {
        self.pin().budget()
    }
    fn describe(&self) -> String {
        self.pin().describe()
    }
}
// A session's connections on its leg of the route. The session keeps its
// own peer limit, so they pass the route's allocation by, but the route
// counts them, and their bytes, as those of the leg its stage is when each
// is dialled: a reload that moves the stage to another position moves its
// counts with it. On a pool leg they go through the session's own stage and
// count on the route's.
struct Carried {
    stage: Arc<Revocable>,
    counted: std::sync::Weak<Revocable>,
    route: Arc<super::weighted::Weighted>,
}
impl Carried {
    fn count(&self, dialed: &mut Dialed) {
        if let Some(stage) = self.counted.upgrade() {
            self.route.carry(&*stage, dialed);
        }
    }
}
#[async_trait::async_trait]
impl Dialer for Carried {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let mut dialed = self.stage.dial(target).await?;
        self.count(&mut dialed);
        Ok(dialed)
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.stage.resolve(host).await
    }
    fn offers(&self) -> weaver_tunnel::pipe::Transports {
        self.stage.offers()
    }
    fn room(&self, purpose: &Purpose) -> Result<(), Vec<Arc<tokio::sync::Notify>>> {
        self.stage.room(purpose)
    }
    fn wait_room(&self, purpose: &Purpose) -> Result<(), weaver_tunnel::pipe::Capacity> {
        self.stage.wait_room(purpose)
    }
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        Arc::clone(&self.stage).datagrams()
    }
    fn inbound(self: Arc<Self>) -> Option<Arc<dyn Inbound>> {
        let inbound = Arc::clone(&self.stage).inbound()?;
        Some(Arc::new(CarriedInbound {
            inbound,
            carried: self,
        }))
    }
    fn bind_local_udp(
        &self,
        local: std::net::SocketAddr,
    ) -> Option<std::io::Result<std::net::UdpSocket>> {
        self.stage.bind_local_udp(local)
    }
    fn source_toward(&self, to: std::net::SocketAddr) -> Option<std::io::Result<std::net::IpAddr>> {
        self.stage.source_toward(to)
    }
    fn ipv6_sources(&self) -> Option<std::io::Result<weaver_tunnel::egress::Ipv6Sources>> {
        self.stage.ipv6_sources()
    }
    fn budget(&self) -> Duration {
        self.stage.budget()
    }
    fn describe(&self) -> String {
        self.stage.describe()
    }
}
// Connections arriving on the leg, which the route counts as the session's
// dialled ones are.
struct CarriedInbound {
    inbound: Arc<dyn Inbound>,
    carried: Arc<Carried>,
}
#[async_trait::async_trait]
impl Inbound for CarriedInbound {
    async fn listen(
        &self,
        port: u16,
        forwarded: bool,
    ) -> Result<Box<dyn InboundListener>, TunnelError> {
        Ok(Box::new(CarriedListener {
            listener: self.inbound.listen(port, forwarded).await?,
            carried: self.carried.clone(),
        }))
    }
    fn forwarding(&self) -> weaver_tunnel::pipe::PortForwarding {
        self.inbound.forwarding()
    }
    // Datagrams pass the route's counts by, as the leg's other ones do.
    async fn bind_datagrams(
        &self,
        port: u16,
    ) -> Result<Arc<dyn weaver_tunnel::endpoint::DatagramSocket>, TunnelError> {
        self.inbound.bind_datagrams(port).await
    }
}
struct CarriedListener {
    listener: Box<dyn InboundListener>,
    carried: Arc<Carried>,
}
#[async_trait::async_trait]
impl InboundListener for CarriedListener {
    fn local_addr(&self) -> std::net::SocketAddr {
        self.listener.local_addr()
    }
    fn mapping(&self) -> Option<tokio::sync::watch::Receiver<weaver_tunnel::pipe::PortMapping>> {
        self.listener.mapping()
    }
    async fn accept(&self) -> Result<Dialed, TunnelError> {
        let mut accepted = self.listener.accept().await?;
        self.carried.count(&mut accepted);
        Ok(accepted)
    }
}
// The leg under a torrent bridge, what its dials are for, and the addresses
// a client checked for each name it hands the bridge.
pub struct TorrentProvider {
    leg: Arc<dyn Dialer>,
    purpose: Purpose,
    pinned: Mutex<HashMap<String, Vec<IpAddr>>>,
}
impl TorrentProvider {
    pub fn new(leg: Arc<dyn Dialer>, purpose: Purpose) -> Self {
        Self {
            leg,
            purpose,
            pinned: Mutex::default(),
        }
    }
    // What a dial of `host` is held to: the address it is, or what a client
    // checked for the name; none when the leg's exit resolves it.
    fn addresses(&self, host: &str) -> Vec<IpAddr> {
        if let Ok(address) = host.trim_matches(['[', ']']).parse() {
            return vec![address];
        }
        self.pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(host)
            .cloned()
            .unwrap_or_default()
    }
}
#[async_trait::async_trait]
impl FetchPath for TorrentProvider {
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.leg.resolve(host).await
    }
    fn pin(&self, host: &str, addresses: Vec<IpAddr>) {
        self.pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(host.to_owned(), addresses);
    }
    fn budget(&self) -> Duration {
        self.leg.budget()
    }
}
#[async_trait::async_trait]
impl TunnelProvider for TorrentProvider {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.dial_observed(host, port, Arc::default()).await
    }
    async fn dial_observed(
        &self,
        host: &str,
        port: u16,
        outcome: Arc<ConnectionOutcome>,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        // A client hands the bridge a name with the addresses it checked
        // pinned, which every rung of the leg keeps to; a name with none
        // pinned, the leg's exit resolves.
        let dialed = self
            .leg
            .dial(&Target {
                host: host.into(),
                port,
                purpose: self.purpose.clone(),
                addresses: self.addresses(host),
            })
            .await
            .map_err(|e| match e {
                DialError::Fatal(e) => e,
                e => TunnelError::Engine(e.to_string()),
            })?;
        let upstream = dialed.outcome.clone();
        outcome.on_failure(move || upstream.failed());
        let upstream = dialed.outcome.clone();
        outcome.on_close(move || upstream.closed());
        Ok(dialed.into_observed_stream())
    }
    fn describe(&self) -> String {
        "torrent route".into()
    }
}
/// What torrent sessions hold on a leg: `open`, their peers' established
/// connections, whatever sockets carry them; `opening` and
/// `bytes_per_second`, the peers still in their handshake and what they
/// receive on sockets of their own, which no route carries;
/// `sessions_opening`, the sessions being built on the leg now, each
/// waiting on it, through a tunnel's handshake among the rest.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionTraffic {
    pub open: u64,
    pub opening: u64,
    pub bytes_per_second: u64,
    pub sessions_opening: u64,
}

/// A session being built on a leg, for as long as its tenancy lives.
pub struct SessionOpening;

impl LegTenant for SessionOpening {
    fn traffic(&self) -> SessionTraffic {
        SessionTraffic {
            sessions_opening: 1,
            ..SessionTraffic::default()
        }
    }
}

impl SessionTraffic {
    /// The leg's connections, `(open, opening)`, from what its route
    /// counts: the open ones are the sessions' established peer
    /// connections, and every other connection the route carries (a peer
    /// still in its handshake, a tracker's or a web seed's request) is
    /// still being set up, as is every peer in its handshake on the
    /// sessions' own sockets.
    pub fn connections(&self, open: u64, opening: u64) -> (u64, u64) {
        (
            self.open,
            opening + self.opening + open.saturating_sub(self.open),
        )
    }
}

/// Rings as an egress's caps or quota state may have changed: on a change
/// to the egresses' policies, to their quota counters' capacity, or to
/// whether a download quota turns work away.
pub struct EgressLimitChanges {
    policy: tokio::sync::watch::Receiver<u64>,
    capacity: tokio::sync::watch::Receiver<u64>,
    quota: tokio::sync::watch::Receiver<u64>,
}

impl EgressLimitChanges {
    pub fn new(
        policy: tokio::sync::watch::Receiver<u64>,
        capacity: tokio::sync::watch::Receiver<u64>,
        quota: tokio::sync::watch::Receiver<u64>,
    ) -> Self {
        Self {
            policy,
            capacity,
            quota,
        }
    }

    /// Resolves once any rings since last asked; false once one is gone.
    pub async fn changed(&mut self) -> bool {
        tokio::select! {
            changed = self.policy.changed() => changed.is_ok(),
            changed = self.capacity.changed() => changed.is_ok(),
            changed = self.quota.changed() => changed.is_ok(),
        }
    }
}

/// A torrent a session runs on a leg, as it holds it now.
pub trait LegTenant: Send + Sync {
    /// What the torrent holds on its leg at this instant.
    fn traffic(&self) -> SessionTraffic;
}

/// A torrent's place on the leg its session runs it on: the leg counts what
/// the torrent holds while this lives, and nothing of it once it is dropped.
pub struct LegTenancy {
    leg: (String, usize),
    tenant: Box<dyn LegTenant>,
}

impl LegTenancy {
    /// The leg the torrent holds its place on: its consumer's key and its
    /// position on that consumer's route.
    pub fn leg(&self) -> &(String, usize) {
        &self.leg
    }
}

impl NetworkRuntime {
    /// Count bytes a torrent session moved over `egress_id` as every byte
    /// through an egress is counted: those received in its download total,
    /// those sent in its upload count.
    pub fn record_torrent_bytes(&self, egress_id: u32, received: u64, sent: u64) {
        if let Some(control) = self
            .egress_controls
            .get(weaver_nntp::transfer::StableServerId(egress_id))
        {
            control.record_received_bytes(received);
        }
        if sent > 0
            && let Some(policy) = &self.quota_policy
        {
            policy.record_egress_upload(egress_id, sent);
        }
    }

    /// `tenant` runs on the leg at `position` of `consumer`'s route, and
    /// counts there for as long as the tenancy returned lives.
    pub fn tenant_torrent(
        &self,
        consumer: Consumer,
        position: usize,
        tenant: impl LegTenant + 'static,
    ) -> Arc<LegTenancy> {
        let tenancy = Arc::new(LegTenancy {
            leg: (consumer.key(), position),
            tenant: Box::new(tenant),
        });
        self.torrent_legs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::downgrade(&tenancy));
        tenancy
    }

    /// What torrent sessions hold now on the legs they run on, by consumer
    /// key and leg position: each tenant's figures as it reads them now.
    pub fn torrent_traffic(&self) -> HashMap<(String, usize), SessionTraffic> {
        let mut legs = HashMap::<_, SessionTraffic>::new();
        let mut tenancies = self
            .torrent_legs
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        tenancies.retain(|tenancy| {
            let Some(tenancy) = tenancy.upgrade() else {
                return false;
            };
            let now = tenancy.tenant.traffic();
            let traffic = legs.entry(tenancy.leg.clone()).or_default();
            traffic.open += now.open;
            traffic.opening += now.opening;
            traffic.bytes_per_second += now.bytes_per_second;
            traffic.sessions_opening += now.sessions_opening;
            true
        });
        legs
    }

    /// What `egress_id` lets torrent sessions move at now, each direction:
    /// its own rate cap under its quota state. Uploads past the soft cap
    /// trickle, and past the hard cap move nothing; downloads move nothing
    /// once the download quota is used. The global limits are not in it.
    /// An egress with no policy adds no limit.
    pub fn torrent_egress_rates(
        &self,
        egress_id: u32,
    ) -> weaver_server_net::bandwidth::limits::ClassRates {
        let Some(policy) = &self.quota_policy else {
            return weaver_server_net::bandwidth::limits::ClassRates::default();
        };
        weaver_server_net::bandwidth::limits::ClassRates {
            download: policy.egress_rate(
                egress_id,
                weaver_server_net::bandwidth::limits::Direction::Download,
                None,
                None,
            ),
            upload: policy.egress_rate(
                egress_id,
                weaver_server_net::bandwidth::limits::Direction::Upload,
                None,
                None,
            ),
        }
    }

    /// What rings as any egress's caps or quota state may change: a saved
    /// egress, a quota crossing a cap, a download quota used up, a reset, or
    /// a new period. None without a policy, as then no egress limits
    /// torrents.
    pub fn egress_limit_changes(&self) -> Option<EgressLimitChanges> {
        let policy = self.quota_policy.as_ref()?;
        Some(EgressLimitChanges::new(
            policy.subscribe_changes(),
            policy.subscribe_capacity_changes(),
            policy.subscribe_quota_changes(),
        ))
    }

    /// Bytes ever uploaded over `egress_id`, as its upload count holds them.
    pub fn egress_upload_bytes(&self, egress_id: u32) -> u64 {
        self.quota_policy
            .as_ref()
            .and_then(|policy| policy.egress_upload_bytes(egress_id))
            .unwrap_or(0)
    }

    /// The leg a torrent scope's session runs on: the scope's live route's
    /// leg at `position`, which allows it `peers` connections.
    pub fn torrent_leg(
        &self,
        scope: TorrentScope,
        position: usize,
        peers: u16,
        timeout: Duration,
    ) -> Result<Arc<TorrentLeg>, String> {
        self.torrent_leg_on(scope, position, None, peers, timeout)
    }

    /// The leg a torrent scope's session on pool member `member` runs on:
    /// the scope's live route's leg at `position`, whose one rung is a pool,
    /// held to that member for the session's life. The session outlives the
    /// scope's route: another scope whose pool holds the same proxy on the
    /// same egress runs on it too.
    pub fn torrent_member_leg(
        &self,
        scope: TorrentScope,
        position: usize,
        member: u32,
        peers: u16,
        timeout: Duration,
    ) -> Result<Arc<TorrentLeg>, String> {
        self.torrent_leg_on(scope, position, Some(member), peers, timeout)
    }

    /// Start a torrent scope's live route, and so its pool, when it is not
    /// running yet: a scope that joins a member session another scope opened
    /// opens no leg of its own, yet the session may live on in its pool once
    /// the opener's route leaves the member. A running route is left as it is.
    pub fn start_torrent_route(
        &self,
        scope: TorrentScope,
        peers: u16,
        timeout: Duration,
    ) -> Result<(), String> {
        let consumer = Consumer::Torrent(scope);
        if self
            .routes
            .lock()
            .expect("network routes")
            .contains_key(&consumer.key())
        {
            return Ok(());
        }
        self.route(consumer, peers, timeout).map(drop)
    }

    fn torrent_leg_on(
        &self,
        scope: TorrentScope,
        position: usize,
        member: Option<u32>,
        peers: u16,
        timeout: Duration,
    ) -> Result<Arc<TorrentLeg>, String> {
        let route = self.route(Consumer::Torrent(scope), peers, timeout)?;
        let (stage, definition) = {
            let legs = route.legs.read().expect("network route legs");
            let leg = legs.get(position).ok_or("the route has no such leg")?;
            (leg.stage.clone(), leg.definition.clone())
        };
        let counted = Arc::downgrade(&stage);
        let names = member_names(&stage);
        let (stage, member) = match member {
            None => (stage, None),
            Some(member) => {
                let pool = pool_of(&definition.path).ok_or("the leg has no pool")?;
                let key = (pool, definition.egress_id);
                let stage = self
                    .pools
                    .lock()
                    .expect("network pools")
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| format!("pool {pool} is not running"))?;
                let pin = stage.pin_member(member).ok_or_else(|| {
                    stage
                        .member_down(member)
                        .unwrap_or_else(|| format!("proxy {member} cannot be used in pool {pool}"))
                })?;
                let member = Arc::new(PinnedMember {
                    pools: self.pools.clone(),
                    egress_id: definition.egress_id,
                    pin: Mutex::new(pin),
                    gone: std::sync::OnceLock::new(),
                });
                // The member names what it dials by the leg's DNS profile.
                (
                    Arc::new(Revocable::named(member.clone() as Arc<dyn Dialer>, names)),
                    Some(member),
                )
            }
        };
        Ok(Arc::new(TorrentLeg {
            stage,
            member,
            counted,
            route: route.weighted.clone(),
            handle: self.handle.clone(),
            bridge: Mutex::new(None),
            seeds: Mutex::new(None),
            scoped: Mutex::new(Vec::new()),
        }))
    }

    /// The members of pool `pool` that may carry a session, in the pool's
    /// order: none while the pool is disabled or gone, and no proxy that is
    /// disabled, as the pool's own stage takes them.
    pub fn pool_members(&self, pool: u32) -> Vec<u32> {
        let config = self.configuration.read().expect("network configuration");
        let Some(pool) = config.pools.get(&pool).filter(|pool| pool.enabled) else {
            return Vec::new();
        };
        pool.member_ids
            .iter()
            .copied()
            .filter(|id| config.profiles.get(id).is_some_and(|p| p.enabled))
            .collect()
    }

    /// Why `path` can carry nothing as saved, when it cannot: every rung of
    /// its ladder is a proxy that is disabled or gone, a chain with one such
    /// hop, or a pool with no member that is enabled, and nothing falls back
    /// to direct. The first rung's reason is given. A direct path, or a
    /// ladder with one rung that may carry, has none.
    pub fn path_unusable(&self, path: &LegPath) -> Option<String> {
        let LegPath::Ladder {
            rungs,
            direct_fallback: false,
        } = path
        else {
            return None;
        };
        let config = self.configuration.read().expect("network configuration");
        let proxy = |id: u32| match config.profiles.get(&id) {
            None => Some(format!("proxy {id} does not exist")),
            Some(profile) if !profile.enabled => Some(format!("proxy {id} is disabled")),
            Some(_) => None,
        };
        let rung = |rung: &Rung| match rung {
            Rung::Proxy { id } => proxy(*id),
            Rung::Chain { ids } => ids.iter().find_map(|id| proxy(*id)),
            Rung::Pool { id } => match config.pools.get(id) {
                None => Some(format!("pool {id} does not exist")),
                Some(pool) if !pool.enabled => Some(format!("pool {id} is disabled")),
                Some(pool) => (!pool
                    .member_ids
                    .iter()
                    .any(|member| proxy(*member).is_none()))
                .then(|| format!("pool {id} has no member that is enabled")),
            },
        };
        let mut reasons = rungs.iter().map(rung);
        let first = reasons.next()??;
        reasons.all(|reason| reason.is_some()).then_some(first)
    }

    /// Why each member of pool `pool` on egress `egress_id` that carries
    /// nothing now does not, by id, as `PoolStage::members_down` says. A pool
    /// no route has started yet holds every member up.
    pub fn pool_members_down(&self, pool: u32, egress_id: u32) -> HashMap<u32, String> {
        let key = (pool, egress_id);
        let stage = self.pools.lock().expect("network pools").get(&key).cloned();
        stage.map(|stage| stage.members_down()).unwrap_or_default()
    }
}

// The names a pool member on `stage` dials by: the stage's, or, on a stage
// with none, names that refuse every lookup, so the host never answers.
pub fn member_names(stage: &Revocable) -> Arc<weaver_tunnel::pipe::NamesSlot> {
    stage.names_slot().unwrap_or_else(|| {
        Arc::new(weaver_tunnel::pipe::NamesSlot::new(
            weaver_tunnel::pipe::Names::new(
                weaver_tunnel::pipe::DnsChoice::Refused(
                    "the pool's leg names by no DNS profile".into(),
                ),
                None,
            ),
        ))
    })
}

// What keeps one of a leg's own bridges up: when the last holder of it
// goes, the bridge is stopped and let go, unless the leg has gone first and
// stopped it itself.
struct BridgeHold {
    leg: std::sync::Weak<TorrentLeg>,
    bridge: PathBridge,
}

impl Drop for BridgeHold {
    fn drop(&mut self) {
        if let Some(leg) = self.leg.upgrade() {
            leg.release_bridge(&self.bridge);
        }
    }
}

/// The pool a leg's one rung is, when it is one.
pub fn pool_of(path: &LegPath) -> Option<u32> {
    match path {
        LegPath::Ladder { rungs, .. } => match rungs.as_slice() {
            [Rung::Pool { id }] => Some(*id),
            _ => None,
        },
        LegPath::Direct => None,
    }
}
