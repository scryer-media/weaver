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
    // The bridge onto the leg's stage, started the first time it is asked for.
    pub fn bridge(&self) -> Result<PathBridge, String> {
        // A panic while starting the bridge left it unstarted: start it.
        let mut bridge = self.bridge.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(bridge) = bridge.as_ref() {
            return Ok(bridge.clone());
        }
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|_| "could not generate bridge credentials")?;
        let path = Arc::new(TorrentProvider::new(self.dialer()));
        let started = PathBridge {
            bridge: Bridge::start(
                &self.handle,
                path.clone(),
                self.stage.budget(),
                ("weaver".into(), hex::encode(secret)),
            )
            .map_err(|e| e.to_string())?,
            path,
        };
        *bridge = Some(started.clone());
        Ok(started)
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
        if let Some(bridge) = self
            .bridge
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
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
// The leg under a torrent bridge, and the addresses a client checked for
// each name it hands the bridge.
struct TorrentProvider {
    leg: Arc<dyn Dialer>,
    pinned: Mutex<HashMap<String, Vec<IpAddr>>>,
}
impl TorrentProvider {
    fn new(leg: Arc<dyn Dialer>) -> Self {
        Self {
            leg,
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
                purpose: Purpose::TorrentHttp,
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
/// receive on sockets of their own, which no route carries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionTraffic {
    pub open: u64,
    pub opening: u64,
    pub bytes_per_second: u64,
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
            true
        });
        legs
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
        let (stage, member) = match member {
            None => (stage, None),
            Some(member) => {
                let pool = pool_of(&definition.path).ok_or("the leg has no pool")?;
                let stage = self
                    .pools
                    .lock()
                    .expect("network pools")
                    .get(&(pool, definition.egress_id))
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
                (
                    Arc::new(Revocable::new(member.clone() as Arc<dyn Dialer>)),
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

    /// Why member `member` of pool `pool` on egress `egress_id` carries
    /// nothing now, when it does not: it was dropped from the pool, or is
    /// blocked. A pool no route has started yet holds every member up.
    pub fn pool_member_down(&self, pool: u32, egress_id: u32, member: u32) -> Option<String> {
        self.pools
            .lock()
            .expect("network pools")
            .get(&(pool, egress_id))?
            .member_down(member)
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, SocketAddr};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use weaver_torrent::network::{Resolved, Resolver};
    use weaver_torrent::{ClientShape, HttpClients, HttpTarget};
    use weaver_tunnel::pipe::DialedStream;
    use weaver_tunnel::resolution::record::{Recording, ResolvedBy};

    use super::*;
    use crate::torrents::net::{LegKind, LegNames, tracker_client};

    // A leg whose naming answers `answer` for every name, or hands names on
    // to its exit, and whose exit is the loopback tracker at `exit`.
    struct Leg {
        answer: Option<IpAddr>,
        exit: SocketAddr,
        asked: Mutex<Vec<String>>,
        dialed: Mutex<Vec<Target>>,
    }

    #[async_trait::async_trait]
    impl Dialer for Leg {
        async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
            self.dialed.lock().unwrap().push(target.clone());
            let stream = tokio::net::TcpStream::connect(self.exit)
                .await
                .map_err(|e| DialError::Skipped(e.to_string()))?;
            Ok(Dialed {
                stream: DialedStream::Socket(stream),
                outcome: Arc::default(),
                path: Default::default(),
                peer: None,
                source: None,
                setup: None,
            })
        }
        async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
            self.asked.lock().unwrap().push(host.into());
            Ok(self
                .answer
                .map_or(Resolution::Unresolved, |ip| Resolution::Addresses(vec![ip])))
        }
        fn budget(&self) -> Duration {
            Duration::from_secs(60)
        }
        fn describe(&self) -> String {
            "leg".into()
        }
    }

    fn host_lookups() -> u64 {
        let metrics = weaver_tunnel::metrics::snapshot();
        let resolved: u64 = metrics
            .resolutions
            .iter()
            .filter(|(by, ..)| *by == weaver_tunnel::metrics::Resolver::Host)
            .map(|(_, ok, failed)| ok + failed)
            .sum();
        let held: u64 = metrics
            .cache
            .iter()
            .filter(|(by, ..)| *by == weaver_tunnel::metrics::Resolver::Host)
            .map(|(.., count)| count)
            .sum();
        resolved + held
    }

    // A tracker that answers one announce.
    async fn tracker() -> (SocketAddr, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut buf = [0u8; 1024];
            while !head.ends_with(b"\r\n\r\n") {
                let read = stream.read(&mut buf).await.unwrap();
                assert!(read > 0, "the request ended early");
                head.extend_from_slice(&buf[..read]);
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
            String::from_utf8(head).unwrap()
        });
        (addr, served)
    }

    // An announce to a tracker by name on a ladder leg, as the session makes
    // it: the leg resolves the name by its naming, and the client is held to
    // what that gave. Returns what the leg was asked and what it dialled.
    async fn announce(answer: Option<IpAddr>) -> (Vec<String>, Vec<Target>) {
        let (exit, served) = tracker().await;
        let leg = Arc::new(Leg {
            answer,
            exit,
            asked: Mutex::default(),
            dialed: Mutex::default(),
        });
        let path = Arc::new(TorrentProvider::new(leg.clone()));
        let bridge = PathBridge {
            bridge: Bridge::start(
                &tokio::runtime::Handle::current(),
                path.clone(),
                leg.budget(),
                ("weaver".into(), "bridge".into()),
            )
            .unwrap(),
            path,
        };
        let port = exit.port();
        let addrs: Vec<IpAddr> = match LegNames(leg.clone())
            .resolve("tracker.test", port)
            .await
            .unwrap()
        {
            Resolved::Addresses(found) => found.iter().map(SocketAddr::ip).collect(),
            Resolved::AtExit => Vec::new(),
        };
        let client = tracker_client(&LegKind::Ladder, leg.clone(), Some(bridge.clone()))
            .unwrap()
            .client(
                Some(HttpTarget {
                    host: "tracker.test",
                    port,
                    addrs: &addrs,
                }),
                &ClientShape::FETCH,
            )
            .unwrap();
        let response = client
            .get(format!("http://tracker.test:{port}/announce"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let head = served.await.unwrap();
        assert!(
            head.to_ascii_lowercase()
                .contains(&format!("host: tracker.test:{port}\r\n")),
            "{head}"
        );
        bridge.revoke().await;
        let asked = std::mem::take(&mut *leg.asked.lock().unwrap());
        let dialed = std::mem::take(&mut *leg.dialed.lock().unwrap());
        (asked, dialed)
    }

    // A ladder rung that answers every name with `answer`, and either
    // refuses every dial or reaches the loopback tracker at `exit`.
    struct OwnNaming {
        answer: Vec<IpAddr>,
        exit: Option<SocketAddr>,
        asked: Mutex<Vec<String>>,
        dialed: Mutex<Vec<Target>>,
    }

    impl OwnNaming {
        fn new(answer: &[[u8; 4]], exit: Option<SocketAddr>) -> Arc<Self> {
            Arc::new(Self {
                answer: answer.iter().copied().map(IpAddr::from).collect(),
                exit,
                asked: Mutex::default(),
                dialed: Mutex::default(),
            })
        }
    }

    #[async_trait::async_trait]
    impl Dialer for OwnNaming {
        async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
            self.dialed.lock().unwrap().push(target.clone());
            let Some(exit) = self.exit else {
                return Err(DialError::Egress(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "fixture refusal",
                )));
            };
            let stream = tokio::net::TcpStream::connect(exit)
                .await
                .map_err(|e| DialError::Skipped(e.to_string()))?;
            Ok(Dialed {
                stream: DialedStream::Socket(stream),
                outcome: Arc::default(),
                path: Default::default(),
                peer: None,
                source: None,
                setup: None,
            })
        }
        async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
            self.asked.lock().unwrap().push(host.into());
            Ok(Resolution::Addresses(self.answer.clone()))
        }
        fn budget(&self) -> Duration {
            Duration::from_secs(60)
        }
        fn describe(&self) -> String {
            "own naming".into()
        }
    }

    // A tracker announced to by name through the bridge of a two-rung
    // ladder whose first rung refuses: the name reaches the ladder with the
    // first rung's checked answer pinned, and the second rung asks its own
    // naming and dials only the address both answers hold.
    #[tokio::test]
    async fn a_lower_rung_under_the_bridge_dials_only_what_its_own_naming_shares() {
        let (exit, served) = tracker().await;
        let first = OwnNaming::new(&[[127, 0, 0, 1], [127, 0, 0, 2]], None);
        let second = OwnNaming::new(&[[127, 0, 0, 2], [127, 0, 0, 3]], Some(exit));
        let ladder: Arc<dyn Dialer> = Arc::new(weaver_tunnel::pipe::Fallback::new(vec![
            first.clone(),
            second.clone(),
        ]));
        let path = Arc::new(TorrentProvider::new(ladder.clone()));
        let bridge = PathBridge {
            bridge: Bridge::start(
                &tokio::runtime::Handle::current(),
                path.clone(),
                ladder.budget(),
                ("weaver".into(), "bridge".into()),
            )
            .unwrap(),
            path,
        };
        let port = exit.port();
        let Resolved::Addresses(found) = LegNames(ladder.clone())
            .resolve("tracker.test", port)
            .await
            .unwrap()
        else {
            panic!("the first rung resolves names");
        };
        let addrs: Vec<IpAddr> = found.iter().map(SocketAddr::ip).collect();
        let client = tracker_client(&LegKind::Ladder, ladder, Some(bridge.clone()))
            .unwrap()
            .client(
                Some(HttpTarget {
                    host: "tracker.test",
                    port,
                    addrs: &addrs,
                }),
                &ClientShape::FETCH,
            )
            .unwrap();
        let response = client
            .get(format!("http://tracker.test:{port}/announce"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        served.await.unwrap();
        bridge.revoke().await;

        let [refused] = &first.dialed.lock().unwrap()[..] else {
            panic!("the first rung dials once");
        };
        assert_eq!(refused.host, "tracker.test");
        assert_eq!(refused.addresses, addrs);
        assert_eq!(*first.asked.lock().unwrap(), ["tracker.test"]);
        assert_eq!(*second.asked.lock().unwrap(), ["tracker.test"]);
        let [carried] = &second.dialed.lock().unwrap()[..] else {
            panic!("the second rung dials once");
        };
        assert_eq!(carried.host, "tracker.test");
        assert_eq!(carried.addresses, [IpAddr::from([127, 0, 0, 2])]);
    }

    // Host, Servers and Proxy-servers naming each answer the leg's lookup
    // with addresses: the session checks those and pins them, and the
    // bridge is handed the name, which the leg dials at the checked address.
    // The host's resolver is never asked.
    #[tokio::test]
    async fn a_name_the_leg_resolves_is_dialled_at_the_checked_address() {
        let before = host_lookups();
        for naming in ["host", "servers", "proxy servers"] {
            let checked = IpAddr::from([127, 0, 0, 1]);
            let (asked, dialed) = announce(Some(checked)).await;
            assert_eq!(asked, ["tracker.test"], "{naming}");
            let [target] = &dialed[..] else {
                panic!("{naming}: dialled {dialed:?}");
            };
            assert_eq!(target.addresses, [checked], "{naming}");
            assert_eq!(target.host, "tracker.test", "{naming}");
            assert!(matches!(target.purpose, Purpose::TorrentHttp), "{naming}");
        }
        assert_eq!(host_lookups(), before);
    }

    // Proxy naming hands the name on: the leg is asked once, no address is
    // pinned, the bridge is handed the name, and no host lookup is made.
    #[tokio::test]
    async fn a_name_the_proxy_resolves_is_handed_through_with_no_lookup() {
        let before = host_lookups();
        let (asked, dialed) = announce(None).await;
        assert_eq!(asked, ["tracker.test"]);
        let [target] = &dialed[..] else {
            panic!("dialled {dialed:?}");
        };
        assert_eq!(target.host, "tracker.test");
        assert!(target.addresses.is_empty(), "{:?}", target.addresses);
        assert_eq!(host_lookups(), before);
    }

    // A runtime whose default torrent scope rides a ladder through a SOCKS5
    // proxy named by `resolver`, asking no DNS servers of its own. The proxy
    // is never dialled: only names are looked up on the leg.
    fn ladder_runtime(resolver: crate::proxies::Resolver) -> (Arc<NetworkRuntime>, Configuration) {
        let runtime = NetworkRuntime::new(
            crate::Database::open_in_memory().unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let consumer = Consumer::Torrent(TorrentScope::Default);
        let mut config = runtime.configuration.read().unwrap().clone();
        config.profiles.insert(
            1,
            ProxyProfile {
                id: 1,
                name: "socks".into(),
                kind: ProxyKind::Socks5,
                enabled: true,
                host: "127.0.0.1".into(),
                port: 1,
                resolver,
                dns_servers: vec![],
                tunnel_addresses: vec![],
                peer_public_key: None,
                mtu: 1420,
                keepalive_seconds: None,
                timeout_seconds: 1,
                host_key_fingerprint: None,
                natpmp: false,
                forwarded_port: None,
                revision: 1,
                secrets: Default::default(),
            },
        );
        config.consumers.insert(consumer.key());
        config.policies.insert(
            consumer.key(),
            RoutingPolicy {
                legs: vec![RouteLeg {
                    egress_id: SYSTEM_EGRESS_ID,
                    weight: 100,
                    path: LegPath::Ladder {
                        rungs: vec![Rung::Proxy { id: 1 }],
                        direct_fallback: false,
                    },
                }],
                ..Default::default()
            },
        );
        runtime.apply_configuration(config.clone()).unwrap();
        (runtime, config)
    }

    // What an announce to `host` on a leg does before it sends: the name
    // looked up on the leg, recorded, and the client held to what it gave.
    async fn reach_on(
        names: &LegNames,
        clients: &dyn HttpClients,
        host: &str,
        port: u16,
    ) -> (
        Option<weaver_tunnel::resolution::record::HostResolution>,
        Vec<IpAddr>,
    ) {
        let recording = Arc::new(Recording::default());
        let addrs: Vec<IpAddr> = match recording.scope(names.resolve(host, port)).await.unwrap() {
            Resolved::Addresses(found) => found.iter().map(SocketAddr::ip).collect(),
            Resolved::AtExit => Vec::new(),
        };
        clients
            .client(
                Some(HttpTarget {
                    host,
                    port,
                    addrs: &addrs,
                }),
                &ClientShape::of(weaver_torrent::ClientUse::Tracker { title_case: false }),
            )
            .unwrap();
        (recording.finish(host, port, None), addrs)
    }

    // Two announces to one tracker host on one leg whose naming is Weaver's:
    // the first looks the name up, the second is answered by what the leg's
    // naming holds, and both ride the one client built for that host.
    #[tokio::test]
    async fn two_announces_to_one_host_on_a_leg_resolve_once_and_share_a_client() {
        let (runtime, _) = ladder_runtime(crate::proxies::Resolver::Weaver);
        let leg = runtime
            .torrent_leg(TorrentScope::Default, 0, 8, Duration::from_secs(30))
            .unwrap();
        let names = LegNames(leg.dialer());
        let clients = crate::torrents::net::LegClients::new(
            || Ok(reqwest::Client::builder().no_proxy()),
            Some(leg.bridge().unwrap()),
            LegKind::Ladder,
            leg.dialer(),
        );
        let (first, first_addrs) = reach_on(&names, &clients, "localhost", 6969).await;
        let (second, second_addrs) = reach_on(&names, &clients, "localhost", 6969).await;
        let (first, second) = (first.unwrap(), second.unwrap());
        assert_eq!(first.by, ResolvedBy::Host);
        assert!(!first.cached, "the first announce looks the name up");
        assert!(second.cached, "the second is answered from the held answer");
        assert!(!first_addrs.is_empty());
        assert_eq!(first_addrs, second_addrs);
        assert_eq!(clients.built(), 1);
        runtime.shutdown().await;
    }

    // A proxy's resolver edited from Weaver to Proxy revokes the leg the
    // session opened on: the session's names and clients go with it, and
    // the leg the route gives now hands the name to the proxy, reusing
    // neither the answer the old naming held nor its clients. The host's
    // resolver is not asked on the proxied leg.
    #[tokio::test]
    async fn a_resolver_edit_from_weaver_to_proxy_reuses_no_answer_or_client() {
        let (runtime, mut config) = ladder_runtime(crate::proxies::Resolver::Weaver);
        let timeout = Duration::from_secs(30);
        let old = runtime
            .torrent_leg(TorrentScope::Default, 0, 8, timeout)
            .unwrap();
        let old_clients = crate::torrents::net::LegClients::new(
            || Ok(reqwest::Client::builder().no_proxy()),
            Some(old.bridge().unwrap()),
            LegKind::Ladder,
            old.dialer(),
        );
        let (held, _) = reach_on(&LegNames(old.dialer()), &old_clients, "localhost", 6969).await;
        assert_eq!(held.unwrap().by, ResolvedBy::Host);

        let profile = config.profiles.get_mut(&1).unwrap();
        profile.resolver = crate::proxies::Resolver::Proxy;
        profile.revision += 1;
        runtime.apply_configuration(config).unwrap();
        assert!(old.stage.is_revoked());

        let new = runtime
            .torrent_leg(TorrentScope::Default, 0, 8, timeout)
            .unwrap();
        assert!(!Arc::ptr_eq(&old.stage, &new.stage));
        let new_clients = crate::torrents::net::LegClients::new(
            || Ok(reqwest::Client::builder().no_proxy()),
            Some(new.bridge().unwrap()),
            LegKind::Ladder,
            new.dialer(),
        );
        let before = host_lookups();
        let (handed, addrs) =
            reach_on(&LegNames(new.dialer()), &new_clients, "localhost", 6969).await;
        let handed = handed.unwrap();
        assert_eq!(handed.by, ResolvedBy::Proxy);
        assert!(!handed.cached);
        assert!(addrs.is_empty(), "{addrs:?}");
        assert_eq!(new_clients.built(), 1);
        assert_eq!(host_lookups(), before);
        runtime.shutdown().await;
    }

    // A dialer that resolves with the host's resolver, as a Host-named leg
    // does.
    struct HostNamed;

    #[async_trait::async_trait]
    impl Dialer for HostNamed {
        async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
            Err(DialError::Skipped("names only".into()))
        }
        async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
            weaver_tunnel::resolution::host_lookup(host)
                .await
                .map(Resolution::Addresses)
                .map_err(|e| DialError::Skipped(e.to_string()))
        }
        fn budget(&self) -> Duration {
            Duration::from_secs(60)
        }
        fn describe(&self) -> String {
            "host".into()
        }
    }

    // A name the session looks up on its leg, a UDP tracker's or a web
    // seed's alike, lands in the resolution record of the task that asked.
    #[tokio::test]
    async fn a_session_lookup_lands_in_the_resolution_record() {
        let recording = Arc::new(Recording::default());
        let resolved = recording
            .scope(LegNames(Arc::new(HostNamed)).resolve("localhost", 6969))
            .await
            .unwrap();
        let Resolved::Addresses(found) = resolved else {
            panic!("the host resolver handed the name on");
        };
        let noted = recording
            .finish("localhost", 6969, None)
            .expect("the lookup was recorded");
        assert_eq!(noted.by, ResolvedBy::Host);
        assert_eq!(
            noted.addresses,
            found.iter().map(SocketAddr::ip).collect::<Vec<_>>()
        );
    }
}
