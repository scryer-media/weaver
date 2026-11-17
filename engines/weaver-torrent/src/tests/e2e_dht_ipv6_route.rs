// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A session's IPv6 DHT runs only on a route with an IPv6 address the
//! internet reaches, as libtorrent starts a DHT node only on a listen socket
//! that is not `local_network` (session_impl.cpp:2206-2211, 288-345,
//! v2.0.15).

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex},
};

use tempfile::TempDir;

use crate::{
    DhtSessionConfig, Session, SessionOptions,
    network::{
        BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, KernelUdp, SocketFactory,
        StreamListener,
    },
};

/// A route whose IPv4 traffic leaves from loopback, and whose IPv6 traffic
/// toward the internet leaves from `v6`, or finds no route when it is none.
/// Its datagram sockets are the kernel's, on `[::]`, so the DHT reaches both
/// loopbacks.
struct Route {
    v6: Mutex<Option<IpAddr>>,
}

impl Route {
    fn new(v6: Option<IpAddr>) -> Arc<Self> {
        Arc::new(Self { v6: Mutex::new(v6) })
    }

    fn set_v6(&self, v6: Option<IpAddr>) {
        *self.v6.lock().unwrap() = v6;
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
            crate::test_support::sockets()
                .bind_udp((Ipv6Addr::UNSPECIFIED, 0).into())
                .await
        })
    }

    fn source_toward(&self, to: SocketAddr) -> Option<io::Result<IpAddr>> {
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
                bootstrap_addrs: Some(
                    routers
                        .iter()
                        .map(|r| r.bind_addr().to_string())
                        .collect(),
                ),
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
        assert!(!session.follow_ipv6_route());
        assert!(!dht.runs_ipv6());

        // A global address on the route's internet route starts it.
        route.set_v6(Some("2001:db8::5".parse().unwrap()));
        assert!(session.follow_ipv6_route());
        assert!(dht.runs_ipv6());
        next_query(&router_v6, port, b"9:find_node").await;
        // Started once.
        assert!(!session.follow_ipv6_route());
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
