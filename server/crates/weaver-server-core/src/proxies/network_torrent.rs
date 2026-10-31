// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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
/// an HTTP client rides belongs to this leg, and stops with it.
pub struct TorrentLeg {
    pub stage: Arc<Revocable>,
    route: Arc<super::weighted::Weighted>,
    handle: tokio::runtime::Handle,
    bridge: Mutex<Option<Arc<Bridge>>>,
}
impl TorrentLeg {
    /// The leg's dialer, whose connections the route counts as its own.
    pub fn dialer(&self) -> Arc<dyn Dialer> {
        Arc::new(Carried {
            stage: self.stage.clone(),
            route: self.route.clone(),
        })
    }
    // The bridge onto the leg's stage, started the first time it is asked for.
    pub fn bridge(&self) -> Result<Arc<Bridge>, String> {
        // A panic while starting the bridge left it unstarted: start it.
        let mut bridge = self.bridge.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(bridge) = bridge.as_ref() {
            return Ok(bridge.clone());
        }
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|_| "could not generate bridge credentials")?;
        let started = Bridge::start(
            &self.handle,
            Arc::new(TorrentProvider(self.dialer())),
            self.stage.budget(),
            ("weaver".into(), hex::encode(secret)),
        )
        .map_err(|e| e.to_string())?;
        *bridge = Some(started.clone());
        Ok(started)
    }
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
// A session's connections on its leg of the route. The session keeps its
// own peer limit, so they pass the route's allocation by, but the route
// counts them, and their bytes, as those of the leg its stage is when each
// is dialled: a reload that moves the stage to another position moves its
// counts with it.
struct Carried {
    stage: Arc<Revocable>,
    route: Arc<super::weighted::Weighted>,
}
#[async_trait::async_trait]
impl Dialer for Carried {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let mut dialed = self.stage.dial(target).await?;
        self.route.carry(&*self.stage, &mut dialed);
        Ok(dialed)
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.stage.resolve(host).await
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
        self.carried
            .route
            .carry(&*self.carried.stage, &mut accepted);
        Ok(accepted)
    }
}
struct TorrentProvider(Arc<dyn Dialer>);
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
        // A client hands the bridge the address it checked, which the leg
        // dials as it is; a name it hands on, the leg resolves by its naming.
        let dialed = self
            .0
            .dial(&Target {
                host: host.into(),
                port,
                purpose: Purpose::Torrent,
                addresses: host.trim_matches(['[', ']']).parse().into_iter().collect(),
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
        let route = self.route(Consumer::Torrent(scope), peers, timeout)?;
        let stage = route
            .legs
            .read()
            .expect("network route legs")
            .get(position)
            .ok_or("the route has no such leg")?
            .stage
            .clone();
        Ok(Arc::new(TorrentLeg {
            stage,
            route: route.weighted.clone(),
            handle: self.handle.clone(),
            bridge: Mutex::new(None),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, SocketAddr};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use weaver_torrent::HttpTarget;
    use weaver_torrent::network::{Resolved, Resolver};
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
        let bridge = Bridge::start(
            &tokio::runtime::Handle::current(),
            Arc::new(TorrentProvider(leg.clone())),
            leg.budget(),
            ("weaver".into(), "bridge".into()),
        )
        .unwrap();
        let port = exit.port();
        let addrs: Vec<IpAddr> = match LegNames(leg.clone())
            .resolve("tracker.test", port)
            .await
            .unwrap()
        {
            Resolved::Addresses(found) => found.iter().map(SocketAddr::ip).collect(),
            Resolved::AtExit => Vec::new(),
        };
        let client = tracker_client(&LegKind::Ladder, Some(bridge.clone()))
            .unwrap()
            .client(Some(HttpTarget {
                host: "tracker.test",
                port,
                addrs: &addrs,
            }))
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

    // Host, Servers and Proxy-servers naming each answer the leg's lookup
    // with addresses: the session checks those, and the bridge is handed
    // the checked address, which the leg dials as it is. The host's
    // resolver is never asked.
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
            assert_eq!(target.host, checked.to_string(), "{naming}");
            assert!(matches!(target.purpose, Purpose::Torrent), "{naming}");
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
