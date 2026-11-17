// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A session's IPv6 DHT runs only on a route with an IPv6 address
//! libtorrent would give a node, as libtorrent starts a DHT node only on a
//! listen socket that is not `local_network` (session_impl.cpp:2206-2211,
//! 288-345, v2.0.15), and stops it as the address goes (2159-2163). On a
//! proxied route it runs while the proxy carries IPv6, by operator ruling
//! 2026-11-17; libtorrent runs none there (2078-2090).

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use tempfile::TempDir;

use crate::{
    DhtSessionConfig, Ipv6RouteChange, Ipv6Source, Ipv6Sources, Session, SessionOptions,
    bencode::ByteBufOwned,
    dht::{Ipv6Phase, bprotocol},
    network::{
        BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, KernelUdp, SocketFactory,
        StreamListener,
    },
};

/// What a proxy does with a datagram toward an IPv6 address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Proxy {
    /// Relays it.
    Carries,
    /// Refuses it: the send fails, as a tunnel stage that cannot send IPv6
    /// fails it.
    Refuses,
    /// Takes it and drops it, as a SOCKS5 relay drops a datagram it cannot
    /// relay, reporting nothing.
    Silent,
}

/// A route whose IPv4 traffic leaves from loopback, and whose IPv6 traffic
/// toward the internet leaves from `v6`, or finds no route when it is none;
/// `listed`, when set, is the route's IPv6 addresses as its interfaces list
/// them. With `proxy`, the route is a proxy's: it tells nothing of where
/// its traffic leaves from, and its datagram sockets carry IPv6 as the proxy
/// does, counting each IPv6 datagram handed to it in `v6_sent`. Its datagram
/// sockets are the kernel's, on `[::]`, so the DHT reaches both loopbacks.
struct Route {
    v6: Mutex<Option<IpAddr>>,
    listed: Mutex<Option<Ipv6Sources>>,
    proxy: Option<Arc<Mutex<Proxy>>>,
    v6_sent: Arc<tokio::sync::watch::Sender<usize>>,
}

impl Route {
    fn new(v6: Option<IpAddr>) -> Arc<Self> {
        Arc::new(Self {
            v6: Mutex::new(v6),
            listed: Mutex::new(None),
            proxy: None,
            v6_sent: Arc::new(tokio::sync::watch::Sender::new(0)),
        })
    }

    fn listing(listed: Ipv6Sources) -> Arc<Self> {
        Arc::new(Self {
            v6: Mutex::new(None),
            listed: Mutex::new(Some(listed)),
            proxy: None,
            v6_sent: Arc::new(tokio::sync::watch::Sender::new(0)),
        })
    }

    fn proxied(proxy: Proxy) -> Arc<Self> {
        Arc::new(Self {
            v6: Mutex::new(None),
            listed: Mutex::new(None),
            proxy: Some(Arc::new(Mutex::new(proxy))),
            v6_sent: Arc::new(tokio::sync::watch::Sender::new(0)),
        })
    }

    fn set_v6(&self, v6: Option<IpAddr>) {
        *self.v6.lock().unwrap() = v6;
    }

    fn set_proxy(&self, proxy: Proxy) {
        *self
            .proxy
            .as_ref()
            .expect("a proxied route")
            .lock()
            .unwrap() = proxy;
    }

    fn v6_sent(&self) -> usize {
        *self.v6_sent.borrow()
    }

    async fn until_v6_sent(&self, at_least: usize) {
        let _ = self
            .v6_sent
            .subscribe()
            .wait_for(|sent| *sent >= at_least)
            .await;
    }
}

/// A datagram socket of a proxied route.
struct Proxied {
    inner: Arc<dyn DatagramSocket>,
    proxy: Arc<Mutex<Proxy>>,
    v6_sent: Arc<tokio::sync::watch::Sender<usize>>,
}

impl Proxied {
    // `None` sends `target` on; otherwise what the send gives.
    fn carry(&self, len: usize, target: SocketAddr) -> Option<io::Result<usize>> {
        if target.ip().to_canonical().is_ipv4() {
            return None;
        }
        self.v6_sent.send_modify(|sent| *sent += 1);
        match *self.proxy.lock().unwrap() {
            Proxy::Carries => None,
            Proxy::Refuses => Some(Err(io::ErrorKind::NetworkUnreachable.into())),
            Proxy::Silent => Some(Ok(len)),
        }
    }
}

impl DatagramSocket for Proxied {
    fn bind_addr(&self) -> SocketAddr {
        self.inner.bind_addr()
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        self.inner.recv_from(buf)
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        match self.carry(buf.len(), target) {
            Some(sent) => Box::pin(async move { sent }),
            None => self.inner.send_to(buf, target),
        }
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        match self.carry(buf.len(), target) {
            Some(sent) => Poll::Ready(sent),
            None => self.inner.poll_send_to(cx, buf, target),
        }
    }
}

impl SocketFactory for Route {
    fn connect_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
    }

    fn listen_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async {
            let inner = crate::test_support::sockets()
                .bind_udp((Ipv6Addr::UNSPECIFIED, 0).into())
                .await?;
            Ok(match &self.proxy {
                None => inner,
                Some(proxy) => Arc::new(Proxied {
                    inner,
                    proxy: Arc::clone(proxy),
                    v6_sent: Arc::clone(&self.v6_sent),
                }) as Arc<dyn DatagramSocket>,
            })
        })
    }

    fn ipv6_sources(&self) -> Option<io::Result<Ipv6Sources>> {
        if self.proxy.is_some() {
            return None;
        }
        if let Some(listed) = self.listed.lock().unwrap().clone() {
            return Some(Ok(listed));
        }
        let from = self.v6.lock().unwrap().and_then(|ip| match ip {
            IpAddr::V6(ip) => Some(ip),
            IpAddr::V4(_) => None,
        });
        Some(Ok(Ipv6Sources {
            addresses: from
                .map(|ip| Ipv6Source {
                    ip,
                    loopback_interface: false,
                    point_to_point: false,
                    internet_route: true,
                })
                .into_iter()
                .collect(),
            any_internet_route: true,
        }))
    }

    fn source_toward(&self, to: SocketAddr) -> Option<io::Result<IpAddr>> {
        if self.proxy.is_some() {
            return None;
        }
        if to.is_ipv4() {
            return Some(Ok(Ipv4Addr::LOCALHOST.into()));
        }
        Some(
            self.v6
                .lock()
                .unwrap()
                .ok_or_else(|| io::ErrorKind::NetworkUnreachable.into()),
        )
    }
}

/// A node that only listens: a bootstrap router of one family.
fn router(at: IpAddr) -> Arc<KernelUdp> {
    crate::test_support::sockets()
        .bind_kernel_udp(SocketAddr::new(at, 0))
        .expect("binding a loopback datagram socket")
}

async fn session(route: &Arc<Route>, routers: [&Arc<KernelUdp>; 2]) -> (TempDir, Arc<Session>) {
    let dir = TempDir::with_prefix("test_dht_ipv6_route").unwrap();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: Some(DhtSessionConfig {
                bootstrap_addrs: Some(routers.iter().map(|r| r.bind_addr().to_string()).collect()),
                port: None,
                persistence: None,
            }),
            persistence: None,
            disable_local_service_discovery: true,
            socket_factory: Arc::clone(route) as Arc<dyn SocketFactory>,
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    (dir, session)
}

/// The next query of `kind` the DHT at `port` sends to `router`.
async fn next_query(router: &KernelUdp, port: u16, kind: &[u8]) -> Vec<u8> {
    let mut buf = [0u8; 2048];
    loop {
        let (len, from) = router.recv_from(&mut buf).await.unwrap();
        let query = &buf[..len];
        if from.port() == port && query.windows(kind.len()).any(|w| w == kind) {
            return query.to_vec();
        }
    }
}

/// A ping from `router` to the DHT at `to`, under transaction id `tid`.
async fn ping(router: &KernelUdp, to: SocketAddr, tid: &[u8; 2]) {
    let mut ping = b"d1:ad2:id20:".to_vec();
    ping.extend_from_slice(&[7u8; 20]);
    ping.extend_from_slice(b"e1:q4:ping1:t2:");
    ping.extend_from_slice(tid);
    ping.extend_from_slice(b"1:y1:qe");
    router.send_to(&ping, to).await.unwrap();
}

/// Waits for the DHT at `port` to answer `router`'s query `tid`.
async fn answer(router: &KernelUdp, port: u16, tid: &[u8; 2]) {
    let marker = [b"1:t2:".as_slice(), tid].concat();
    let mut buf = [0u8; 2048];
    loop {
        let (len, from) = router.recv_from(&mut buf).await.unwrap();
        let reply = &buf[..len];
        if from.port() == port
            && reply.windows(marker.len()).any(|w| w == marker)
            && reply.windows(6).any(|w| w == b"1:y1:r")
        {
            return;
        }
    }
}

/// What `router` has received from anyone, without waiting.
fn waiting(router: &KernelUdp) -> Vec<(Vec<u8>, SocketAddr)> {
    let mut buf = [0u8; 2048];
    let mut got = Vec::new();
    loop {
        match router.try_recv_from(&mut buf) {
            Ok((len, from)) => got.push((buf[..len].to_vec(), from)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return got,
            Err(e) => panic!("reading the router: {e}"),
        }
    }
}

// On a route with no IPv6 route at all, or only a link-local IPv6 address,
// the IPv6 node holds no table, sends nothing and answers nothing, while
// the IPv4 node bootstraps; once the route's IPv6 traffic leaves from a
// global address, the interfaces' change starts the IPv6 node, which asks
// its router.
#[tokio::test]
async fn an_ipv6_dht_waits_for_an_address_the_internet_reaches() {
    for v6 in [None, Some("fe80::1".parse::<IpAddr>().unwrap())] {
        let route = Route::new(v6);
        let router_v4 = router(Ipv4Addr::LOCALHOST.into());
        let router_v6 = router(Ipv6Addr::LOCALHOST.into());
        let (_dir, session) = session(&route, [&router_v4, &router_v6]).await;
        let dht = session.get_dht().expect("the session runs a DHT").clone();
        let port = dht.listen_addr().port();

        // The IPv4 node bootstraps from its router.
        next_query(&router_v4, port, b"9:find_node").await;
        assert!(!dht.runs_ipv6(), "{v6:?}");

        // A ping from the IPv6 router, then one from the IPv4 router: the
        // DHT reads them in that order, so once the IPv4 one is answered,
        // any answer to the IPv6 one has been sent.
        ping(&router_v6, (Ipv6Addr::LOCALHOST, port).into(), b"v6").await;
        ping(&router_v4, (Ipv4Addr::LOCALHOST, port).into(), b"v4").await;
        answer(&router_v4, port, b"v4").await;
        assert_eq!(waiting(&router_v6), Vec::new(), "{v6:?}");
        dht.with_routing_tables(|_, v6_table| {
            assert_eq!(v6_table.len(), 0, "{v6:?}");
            assert_eq!(v6_table.entries(), 0, "{v6:?}");
        });

        // Nothing changed for IPv6: the node keeps waiting.
        assert_eq!(session.follow_ipv6_route(), None);
        assert!(!dht.runs_ipv6());

        // A global address on the route's internet route starts it.
        route.set_v6(Some("2001:db8::5".parse().unwrap()));
        assert_eq!(session.follow_ipv6_route(), Some(Ipv6RouteChange::Started));
        assert!(dht.runs_ipv6());
        next_query(&router_v6, port, b"9:find_node").await;
        // Started once.
        assert_eq!(session.follow_ipv6_route(), None);
        session.cancellation_token().cancel();
    }
}

// A route whose IPv6 traffic leaves from a global address runs both nodes
// from the start, each asking its own router.
#[tokio::test]
async fn an_ipv6_dht_runs_on_a_route_with_a_global_address() {
    let route = Route::new(Some("2001:db8::5".parse().unwrap()));
    let router_v4 = router(Ipv4Addr::LOCALHOST.into());
    let router_v6 = router(Ipv6Addr::LOCALHOST.into());
    let (_dir, session) = session(&route, [&router_v4, &router_v6]).await;
    let dht = session.get_dht().expect("the session runs a DHT").clone();
    let port = dht.listen_addr().port();
    assert!(dht.runs_ipv6());
    next_query(&router_v4, port, b"9:find_node").await;
    next_query(&router_v6, port, b"9:find_node").await;
    session.cancellation_token().cancel();
}

/// The transaction id of the query `query`.
fn tid_of(query: &[u8]) -> Vec<u8> {
    bprotocol::deserialize_message::<ByteBufOwned>(query)
        .expect("a query the DHT sent")
        .transaction_id
        .as_ref()
        .to_vec()
}

/// A response to the query `query` from the node `id`, naming `named`, an
/// IPv6 node, when given.
fn response(query: &[u8], id: [u8; 20], named: Option<([u8; 20], SocketAddr)>) -> Vec<u8> {
    let tid = tid_of(query);
    let mut r = b"d1:rd2:id20:".to_vec();
    r.extend_from_slice(&id);
    if let Some((named_id, SocketAddr::V6(at))) = named {
        r.extend_from_slice(b"6:nodes638:");
        r.extend_from_slice(&named_id);
        r.extend_from_slice(&at.ip().octets());
        r.extend_from_slice(&at.port().to_be_bytes());
    }
    r.extend_from_slice(format!("e1:t{}:", tid.len()).as_bytes());
    r.extend_from_slice(&tid);
    r.extend_from_slice(b"1:y1:re");
    r
}

/// Answers the next find_node the DHT at `port` sends `node`, as the node
/// `id`, naming `named`.
async fn answer_find_node(
    node: &KernelUdp,
    port: u16,
    id: [u8; 20],
    named: Option<([u8; 20], SocketAddr)>,
) {
    let query = next_query(node, port, b"9:find_node").await;
    let dht = SocketAddr::new(
        if node.bind_addr().is_ipv4() {
            Ipv4Addr::LOCALHOST.into()
        } else {
            Ipv6Addr::LOCALHOST.into()
        },
        port,
    );
    node.send_to(&response(&query, id, named), dht)
        .await
        .unwrap();
}

/// Pings the DHT at `port` from the IPv4 `router` and waits for its answer:
/// every datagram the DHT handed its socket before it read the ping has been
/// sent.
async fn settled(router_v4: &KernelUdp, port: u16, tid: &[u8; 2]) {
    ping(router_v4, (Ipv4Addr::LOCALHOST, port).into(), tid).await;
    answer(router_v4, port, tid).await;
}

// Item 1. A host with a global IPv6 address and no IPv6 default route: the
// route's IPv6 traffic toward the internet finds no route, yet its
// interface holds 2001:db8::7. libtorrent opens a listen socket on it that
// is not `local_network` (the address is global, session_impl.cpp:323-330),
// and a DHT node on it (2206-2211, v2.0.15); so does Weaver: its IPv6 router
// receives a find_node. A unique local address on an interface with no
// internet route, beside an internet route of IPv4, is local in libtorrent
// and gives no node: its router receives nothing.
#[tokio::test]
async fn an_ipv6_dht_runs_from_a_global_address_with_no_default_route() {
    let listed = |ip: &str| Ipv6Sources {
        addresses: vec![Ipv6Source {
            ip: ip.parse().unwrap(),
            loopback_interface: false,
            point_to_point: false,
            internet_route: false,
        }],
        any_internet_route: true,
    };

    let route = Route::listing(listed("2001:db8::7"));
    assert_eq!(
        route
            .source_toward((Ipv6Addr::LOCALHOST, 9).into())
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::NetworkUnreachable,
    );
    let router_v4 = router(Ipv4Addr::LOCALHOST.into());
    let router_v6 = router(Ipv6Addr::LOCALHOST.into());
    let (_dir, session, dht, port) = opened(&route, [&router_v4, &router_v6]).await;
    assert!(dht.runs_ipv6());
    next_query(&router_v6, port, b"9:find_node").await;
    session.cancellation_token().cancel();

    let route = Route::listing(listed("fd00::7"));
    let router_v4 = router(Ipv4Addr::LOCALHOST.into());
    let router_v6 = router(Ipv6Addr::LOCALHOST.into());
    let (_dir, session, dht, port) = opened(&route, [&router_v4, &router_v6]).await;
    assert_eq!(dht.ipv6_phase(), Ipv6Phase::Waits);
    next_query(&router_v4, port, b"9:find_node").await;
    settled(&router_v4, port, b"s1").await;
    assert_eq!(waiting(&router_v6), Vec::new());
    session.cancellation_token().cancel();
}

// Item 3. The route's last global IPv6 address goes: on the interfaces'
// change the IPv6 node stops, as libtorrent deletes the node of the listen
// socket it closes (session_impl.cpp:2159-2163, v2.0.15). It sends nothing:
// neither its router nor the node in its table receives a datagram, and a
// ping from either goes unanswered. Its table is kept. The address comes
// back: the node runs again from that table, and asks the node it holds,
// which only the table knows of.
#[tokio::test]
async fn an_ipv6_dht_stops_as_its_address_goes_and_resumes_from_its_table() {
    let route = Route::new(Some("2001:db8::5".parse().unwrap()));
    let router_v4 = router(Ipv4Addr::LOCALHOST.into());
    let router_v6 = router(Ipv6Addr::LOCALHOST.into());
    let node_v6 = router(Ipv6Addr::LOCALHOST.into());
    let (_dir, session) = session(&route, [&router_v4, &router_v6]).await;
    let dht = session.get_dht().expect("the session runs a DHT").clone();
    let port = dht.listen_addr().port();
    assert!(dht.runs_ipv6());

    // The router names the node, which answers and enters the table.
    let node_id = [0x22; 20];
    answer_find_node(
        &router_v6,
        port,
        [0x11; 20],
        Some((node_id, node_v6.bind_addr())),
    )
    .await;
    answer_find_node(&node_v6, port, node_id, None).await;
    let node_at = node_v6.bind_addr();
    dht.until_v6_table(|table| table.iter().any(|n| n.addr() == node_at))
        .await;

    route.set_v6(None);
    assert_eq!(session.follow_ipv6_route(), Some(Ipv6RouteChange::Stopped));
    assert_eq!(dht.ipv6_phase(), Ipv6Phase::Stopped);
    assert!(!dht.runs_ipv6());
    // Stopped once.
    assert_eq!(session.follow_ipv6_route(), None);
    // What either had been sent until now.
    waiting(&router_v6);
    waiting(&node_v6);

    // A ping from each IPv6 node, then one from the IPv4 router: once the
    // IPv4 one is answered, any datagram to the IPv6 ones has been sent.
    ping(&node_v6, (Ipv6Addr::LOCALHOST, port).into(), b"n6").await;
    ping(&router_v6, (Ipv6Addr::LOCALHOST, port).into(), b"r6").await;
    settled(&router_v4, port, b"s1").await;
    assert_eq!(waiting(&router_v6), Vec::new());
    assert_eq!(waiting(&node_v6), Vec::new());
    dht.with_routing_tables(|_, v6_table| {
        assert!(
            v6_table.iter().any(|n| n.addr() == node_at),
            "the table is kept"
        );
    });

    // The address comes back: the node asks the node its table holds.
    route.set_v6(Some("2001:db8::5".parse().unwrap()));
    assert_eq!(session.follow_ipv6_route(), Some(Ipv6RouteChange::Started));
    assert_eq!(dht.ipv6_phase(), Ipv6Phase::Runs { proving: false });
    next_query(&node_v6, port, b"9:find_node").await;
    session.cancellation_token().cancel();
}

async fn opened(
    route: &Arc<Route>,
    routers: [&Arc<KernelUdp>; 2],
) -> (TempDir, Arc<Session>, crate::dht::Dht, u16) {
    let (dir, session) = session(route, routers).await;
    let dht = session.get_dht().expect("the session runs a DHT").clone();
    let port = dht.listen_addr().port();
    (dir, session, dht, port)
}

// Item 2, a proxy that carries IPv6. By operator ruling 2026-11-17 a
// proxied route keeps the IPv6 DHT when the proxy carries IPv6; libtorrent
// runs none (session_impl.cpp:2078-2090, v2.0.15). The node starts proving
// its proxy; the router receives its find_node through the proxy, answers,
// and the node runs on as any other.
#[tokio::test]
async fn a_proxied_ipv6_dht_runs_while_its_proxy_carries_ipv6() {
    let route = Route::proxied(Proxy::Carries);
    let router_v4 = router(Ipv4Addr::LOCALHOST.into());
    let router_v6 = router(Ipv6Addr::LOCALHOST.into());
    let (_dir, session, dht, port) = opened(&route, [&router_v4, &router_v6]).await;
    assert_eq!(dht.ipv6_phase(), Ipv6Phase::Runs { proving: true });
    answer_find_node(&router_v6, port, [0x11; 20], None).await;
    dht.until_ipv6_phase(Ipv6Phase::Runs { proving: false })
        .await;
    // An interface change leaves a proxied route's node alone.
    assert_eq!(session.follow_ipv6_route(), None);
    assert!(dht.runs_ipv6());
    session.cancellation_token().cancel();
}

// Item 2, a proxy that refuses IPv6 targets, or takes them and drops them.
// The node's first bootstrap hears no IPv6 node while the route carries
// IPv4, and the node goes idle: past many self-refresh and table-refresh
// intervals the proxy is handed no further IPv6 datagram, the IPv6 router
// receives none, and an IPv6 ping goes unanswered. An interface change does
// not wake it. The route's proxy then carries IPv6, which opens a new
// session on the route (a route or proxy change moves its torrents to a
// new session): that session's node tries again, and its router receives
// its find_node. The clock is paused, so the timeouts pass without waiting.
#[tokio::test(start_paused = true)]
async fn a_proxied_ipv6_dht_idles_when_its_proxy_carries_no_ipv6() {
    for proxy in [Proxy::Refuses, Proxy::Silent] {
        let route = Route::proxied(proxy);
        let router_v4 = router(Ipv4Addr::LOCALHOST.into());
        let router_v6 = router(Ipv6Addr::LOCALHOST.into());
        let (_dir, session, dht, port) = opened(&route, [&router_v4, &router_v6]).await;
        // The bootstrap's find_node is handed to the proxy, and an IPv4
        // datagram is read.
        route.until_v6_sent(1).await;
        settled(&router_v4, port, b"s1").await;
        dht.until_ipv6_phase(Ipv6Phase::Idle).await;
        let sent = route.v6_sent();

        // An interface change leaves it idle.
        assert_eq!(session.follow_ipv6_route(), None, "{proxy:?}");
        // Three self-refresh intervals of ten minutes, each many table
        // refreshes.
        for _ in 0..3 {
            tokio::time::advance(std::time::Duration::from_secs(10 * 60)).await;
            settled(&router_v4, port, b"s2").await;
        }
        ping(&router_v6, (Ipv6Addr::LOCALHOST, port).into(), b"r6").await;
        settled(&router_v4, port, b"s3").await;
        assert_eq!(route.v6_sent(), sent, "{proxy:?}");
        assert_eq!(waiting(&router_v6), Vec::new(), "{proxy:?}");
        assert_eq!(dht.ipv6_phase(), Ipv6Phase::Idle, "{proxy:?}");
        session.stop().await;

        // The proxy now carries IPv6: a new session on the route tries again.
        route.set_proxy(Proxy::Carries);
        let (_dir, session, dht, port) = opened(&route, [&router_v4, &router_v6]).await;
        assert_eq!(dht.ipv6_phase(), Ipv6Phase::Runs { proving: true });
        next_query(&router_v6, port, b"9:find_node").await;
        session.stop().await;
    }
}
