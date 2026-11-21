// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::core::net::{ClientShape, Egress, HttpClients, HttpTarget, NamedBy, Resolver};
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::tests::{
    announce_of, answer_with_a_peer, http_listener, identity, next_announce, no_vote, op,
    protocols, query_param, reach_all, reach_but, read_announce, respond, start, the_peer, v1_hash,
};
use super::{
    AnnounceOk, StoppedAnnounces, ToldPorts, Tracker, TrackerComms, TrackerSetup, TrackerTask,
    UdpTrackerResolveResult, udp_tracker_to_socket_addrs,
};
use crate::tracker::tracker_comms_udp::{ACTION_CONNECT, CONNECTION_ID_MAGIC, UdpTrackerClient};

/// Resolves every name to `at`'s address.
pub(super) struct FixedResolver {
    pub(super) at: std::net::IpAddr,
}

impl Resolver for FixedResolver {
    fn resolve<'a>(
        &'a self,
        _host: &'a str,
        port: u16,
    ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
        Box::pin(async move {
            Ok(crate::core::net::Resolved::Addresses(vec![
                (self.at, port).into(),
            ]))
        })
    }
}

/// The error the announce table records for `url`, once it has one.
async fn refusal_of(
    status: &super::TrackerStatus,
    peers: &mut (impl futures::Stream<Item = SocketAddr> + Unpin),
    url: &Url,
) -> String {
    let mut rx = status.subscribe();
    tokio::select! {
        _ = peers.next() => panic!("a refused tracker gave peers"),
        r = rx.wait_for(|a| announce_of(a, url).last.is_some()) => r.unwrap(),
    };
    announce_of(&status.announces(), url)
        .last
        .clone()
        .unwrap()
        .result
        .unwrap_err()
        .message
}

// An e2e build asks a loopback tracker at any path.
#[cfg(not(feature = "e2e-local-trackers"))]
#[tokio::test]
async fn a_tracker_url_that_could_forge_a_request_is_never_asked() {
    // A URL whose query names an announce argument is refused, and so is
    // a loopback tracker the operator named at a path other than
    // `/announce` with no `/`, `\` or `%` after it, whatever name it is
    // reached by. Each is refused before any request; the next tier's
    // tracker is asked.
    let (refused, at) = http_listener().await;
    let port = at.port().unwrap();
    let named = |s: &str| Url::parse(&s.replace("PORT", &port.to_string())).unwrap();
    let forged = [
        named("http://127.0.0.1:PORT/announce?passkey=k&INFO_HASH=x"),
        named("http://127.0.0.1:PORT/x?y=1"),
        named("http://[::1]:PORT/api"),
        named("http://tracker.invalid:PORT/announce/../admin"),
        named("http://127.0.0.1:PORT/admin/announce"),
        named("http://127.0.0.1:PORT/abcd/announce"),
        named("http://tracker.invalid:PORT/announce/abcd"),
        named("http://tracker.invalid:PORT/announce%2F..%2Fdelete"),
    ];
    let (ok, ok_url) = http_listener().await;
    let mut tiers: Vec<Vec<Url>> = forged.iter().map(|u| vec![u.clone()]).collect();
    tiers.push(vec![ok_url]);

    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let resolver = Arc::new(FixedResolver {
        at: Ipv4Addr::LOCALHOST.into(),
    });
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, Some(resolver));
    let stopped = StoppedAnnounces::default();
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(tiers),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });

    for url in &forged {
        let e = refusal_of(&status, &mut peers, url).await;
        assert!(e.contains("SSRF"), "{url}: {e}");
    }
    let q = answer_with_a_peer(
        &ok,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    // The forged trackers' host heard nothing.
    assert!(refused.accept().now_or_never().is_none());

    drop(peers);
    let q = next_announce(&ok).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    stopped.wait_idle().await;
    token.cancel();
}

/// Answers an HTTP request with a redirect to `location`.
async fn redirect(conn: tokio::net::TcpStream, location: &str) {
    let response = format!(
        "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    let mut response = response.as_bytes();
    while !response.is_empty() {
        conn.writable().await.unwrap();
        match conn.try_write(response) {
            Ok(n) => response = &response[n..],
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{e}"),
        }
    }
}

// An e2e build asks a loopback tracker at any path.
#[cfg(not(feature = "e2e-local-trackers"))]
#[tokio::test]
async fn a_redirect_to_a_loopback_path_no_tracker_answers_is_never_fetched() {
    // libtorrent http_connection::on_read restarts the GET at the
    // redirect's URL, whose endpoints on_filter holds to the same rule
    // before connect: the moved request is refused before it is sent.
    let (tracker, url) = http_listener().await;
    let (moved, moved_url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let serve = async {
        let (conn, _) = read_announce(&tracker).await;
        redirect(conn, moved_url.join("/admin").unwrap().as_str()).await;
        std::future::pending::<()>().await
    };
    let e = tokio::select! {
        _ = serve => unreachable!(),
        e = refusal_of(&status, &mut peers, &url) => e,
    };
    assert!(e.contains("SSRF"), "{e}");
    // The moved host heard nothing.
    assert!(moved.accept().now_or_never().is_none());
    drop(peers);
    stopped.wait_idle().await;
    token.cancel();
}

// A client looks no name up: it reaches a host only at the addresses it
// was built for.
struct NoNames;

impl reqwest::dns::Resolve for NoNames {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let name = name.as_str().to_owned();
        Box::pin(async move { Err(format!("{name}: no lookups in tests").into()) })
    }
}

/// The session's clients, each looking no name up; records every target
/// a client is built for.
#[derive(Default)]
struct Pinned {
    built: Mutex<Vec<(String, u16, Vec<IpAddr>)>>,
}

impl HttpClients for Pinned {
    fn client(
        &self,
        target: Option<HttpTarget<'_>>,
        shape: &ClientShape,
    ) -> anyhow::Result<reqwest::Client> {
        if let Some(t) = &target {
            self.built
                .lock()
                .push((t.host.to_owned(), t.port, t.addrs.to_vec()));
        }
        let builder = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(Arc::new(NoNames));
        crate::test_support::client(builder, target, shape)
    }
}

#[tokio::test]
async fn every_hop_of_a_tracker_get_goes_only_to_the_addresses_checked_for_it() {
    // Each hop's addresses are checked before its client is built, and
    // the client tries only those left; a redirect starts over at its
    // own URL. The names are known to the engine's resolver only: a
    // client that looked one up itself would reach nothing.
    let (tracker, at) = http_listener().await;
    let (moved, moved_at) = http_listener().await;
    let (port, moved_port) = (at.port().unwrap(), moved_at.port().unwrap());
    let url = Url::parse(&format!("http://tracker.invalid:{port}/announce")).unwrap();
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let resolver = Arc::new(FixedResolver {
        at: Ipv4Addr::LOCALHOST.into(),
    });
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, Some(resolver));
    let clients = Arc::new(Pinned::default());
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![url]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            clients.clone(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: StoppedAnnounces::default(),
        counts: Default::default(),
    })
    .peers;

    let (conn, q) = tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = read_announce(&tracker) => r,
    };
    redirect(
        conn,
        &format!("http://moved.invalid:{moved_port}/announce?{q}"),
    )
    .await;
    let q = answer_with_a_peer(
        &moved,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    let loopback = vec![IpAddr::from(Ipv4Addr::LOCALHOST)];
    assert_eq!(
        *clients.built.lock(),
        vec![
            ("tracker.invalid".to_owned(), port, loopback.clone()),
            ("moved.invalid".to_owned(), moved_port, loopback),
        ]
    );
    drop(peers);
    token.cancel();
}

/// The session's clients on a network where every address is the
/// tracker at `at`: a client built for any address reaches it there.
/// Records the addresses clients were built for. An address in `routes`
/// is reached as it says; any other through a proxy.
pub(super) struct Wired {
    at: SocketAddr,
    built: Mutex<Vec<IpAddr>>,
    routes: Vec<(IpAddr, Egress)>,
}

impl Wired {
    pub(super) fn new(at: &Url) -> Arc<Self> {
        Self::routing(at, Vec::new())
    }

    fn routing(at: &Url, routes: Vec<(IpAddr, Egress)>) -> Arc<Self> {
        Arc::new(Self {
            at: SocketAddr::new(at.host_str().unwrap().parse().unwrap(), at.port().unwrap()),
            built: Mutex::default(),
            routes,
        })
    }
}

impl HttpClients for Wired {
    fn client(
        &self,
        target: Option<HttpTarget<'_>>,
        shape: &ClientShape,
    ) -> anyhow::Result<reqwest::Client> {
        let at = [self.at.ip()];
        let target = target.map(|t| {
            self.built.lock().extend(t.addrs);
            HttpTarget { addrs: &at, ..t }
        });
        let builder = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(Arc::new(NoNames));
        crate::test_support::client(builder, target, shape)
    }

    fn egress(&self, to: SocketAddr) -> Egress {
        self.routes
            .iter()
            .find(|(ip, _)| *ip == to.ip())
            .map_or(Egress::Proxied, |(_, egress)| *egress)
    }
}

/// A resolver that knows `names` and no other.
struct Names(Vec<(&'static str, IpAddr)>);

impl Resolver for Names {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
        let found: Vec<SocketAddr> = self
            .0
            .iter()
            .filter(|(name, _)| *name == host)
            .map(|(_, ip)| SocketAddr::new(*ip, port))
            .collect();
        Box::pin(async move {
            if found.is_empty() {
                Err(std::io::ErrorKind::NotFound.into())
            } else {
                Ok(crate::core::net::Resolved::Addresses(found))
            }
        })
    }
}

/// Starts announcing to `tiers` through `clients`, names looked up by
/// `names`.
fn start_through(
    tiers: Vec<Vec<Tracker>>,
    clients: Arc<Wired>,
    names: Names,
    token: &CancellationToken,
    stopped: &StoppedAnnounces,
) -> TrackerTask {
    let udp_sock = crate::test_support::loopback_udp_now();
    let udp_client =
        UdpTrackerClient::new_with_socket(token.clone(), udp_sock, Some(Arc::new(names)));
    TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers,
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(clients, reach_all(), udp_client, Default::default(), false),
        stopped: stopped.clone(),
        counts: Default::default(),
    })
}

/// A tracker named by both families, and the session's own address in
/// each.
fn dual_stack() -> (Names, [IpAddr; 2], [IpAddr; 2]) {
    let tracker: [IpAddr; 2] = [
        Ipv4Addr::new(203, 0, 113, 7).into(),
        "2001:db8::7".parse().unwrap(),
    ];
    let own: [IpAddr; 2] = [
        Ipv4Addr::new(198, 51, 100, 1).into(),
        "2001:db8::1".parse().unwrap(),
    ];
    let names = Names(vec![
        ("dual.invalid", tracker[0]),
        ("dual.invalid", tracker[1]),
    ]);
    (names, tracker, own)
}

#[tokio::test]
async fn a_tracker_both_families_route_to_is_announced_to_once_in_each() {
    // libtorrent announces from each listen socket that can route to
    // the tracker (torrent.cpp `on_filter`): with an IPv4 and an IPv6
    // route, two announces, each held to its family, and the peers of
    // both answers kept.
    let (tracker, at) = http_listener().await;
    let url = Url::parse(&format!(
        "http://dual.invalid:{}/announce",
        at.port().unwrap()
    ))
    .unwrap();
    let (names, addrs, own) = dual_stack();
    let clients = Wired::routing(
        &at,
        vec![
            (addrs[0], Egress::From(own[0])),
            (addrs[1], Egress::From(own[1])),
        ],
    );
    let token = CancellationToken::new();
    let stopped = StoppedAnnounces::default();
    let TrackerTask {
        mut peers,
        status: _status,
    } = start_through(
        vec![vec![Tracker::operator(url)]],
        clients.clone(),
        names,
        &token,
        &stopped,
    );

    // The peers of neither answer come before both are in.
    let mut queries = Vec::new();
    for body in [
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aBe",
    ] {
        let (conn, query) = tokio::select! {
            _ = peers.next() => panic!("a peer came before both announces were answered"),
            r = read_announce(&tracker) => r,
        };
        respond(conn, body).await;
        queries.push(query);
    }
    let [q1, q2] = <[String; 2]>::try_from(queries).unwrap();
    assert_eq!(query_param(&q1, "event"), Some("started"));
    assert_eq!(q1, q2);
    let other_peer: SocketAddr = (Ipv4Addr::LOCALHOST, 0x1a42).into();
    let mut got = vec![peers.next().await.unwrap(), peers.next().await.unwrap()];
    got.sort();
    assert_eq!(got, [the_peer(), other_peer]);
    let mut built = clients.built.lock().clone();
    built.sort();
    assert_eq!(built, addrs);
    drop(peers);
    token.cancel();
}

#[tokio::test]
async fn a_tracker_reached_through_a_proxy_is_announced_to_once() {
    // Through a proxy libtorrent has one listen socket and announces
    // once; the proxy picks the family.
    let (tracker, at) = http_listener().await;
    let url = Url::parse(&format!(
        "http://dual.invalid:{}/announce",
        at.port().unwrap()
    ))
    .unwrap();
    let (names, addrs, _) = dual_stack();
    let clients = Wired::new(&at);
    let token = CancellationToken::new();
    let stopped = StoppedAnnounces::default();
    let TrackerTask {
        mut peers,
        status: _status,
    } = start_through(
        vec![vec![Tracker::operator(url)]],
        clients.clone(),
        names,
        &token,
        &stopped,
    );

    answer_with_a_peer(
        &tracker,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(*clients.built.lock(), addrs);
    drop(peers);
    token.cancel();
}

#[tokio::test]
async fn a_family_with_no_route_to_the_tracker_is_not_announced_in() {
    // libtorrent skips a listen socket that cannot route to the
    // tracker: with no IPv6 route, one announce, held to IPv4.
    let (tracker, at) = http_listener().await;
    let url = Url::parse(&format!(
        "http://dual.invalid:{}/announce",
        at.port().unwrap()
    ))
    .unwrap();
    let (names, addrs, own) = dual_stack();
    let clients = Wired::routing(
        &at,
        vec![
            (addrs[0], Egress::From(own[0])),
            (addrs[1], Egress::NoRoute),
        ],
    );
    let token = CancellationToken::new();
    let stopped = StoppedAnnounces::default();
    let TrackerTask {
        mut peers,
        status: _status,
    } = start_through(
        vec![vec![Tracker::operator(url)]],
        clients.clone(),
        names,
        &token,
        &stopped,
    );

    answer_with_a_peer(
        &tracker,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(*clients.built.lock(), [addrs[0]]);
    drop(peers);
    token.cancel();
}

// The e2e carveout asks a torrent's tracker at these local addresses.
#[cfg(not(feature = "e2e-local-trackers"))]
#[tokio::test]
async fn a_tracker_the_torrent_named_is_refused_at_a_private_address() {
    // A tracker that came with the torrent, at a loopback, private,
    // link-local or unique-local address, by IP or by name, at a
    // tracker's path or any other, over HTTP or UDP, is refused before
    // any client is built for it or any datagram sent; the operator's
    // own tracker on loopback, in the next tier, is asked.
    let (tracker, at) = http_listener().await;
    // Another tracker on loopback at /announce: one URL is one tracker,
    // so the torrent's may not share the operator's.
    let (_theirs, their_at) = http_listener().await;
    let port = at.port().unwrap();
    let named = |s: &str| Url::parse(&s.replace("PORT", &port.to_string())).unwrap();
    let refused = [
        their_at.clone(),
        named("http://ten.invalid:PORT/announce"),
        named("http://lan.invalid:PORT/admin"),
        named("http://link.invalid:PORT/announce"),
        named("http://ula.invalid:PORT/scrape"),
        named("udp://ten.invalid:PORT/announce"),
        named("udp://192.168.1.1:PORT/announce"),
    ];
    let names = Names(vec![
        ("ten.invalid", Ipv4Addr::new(10, 0, 0, 1).into()),
        ("lan.invalid", Ipv4Addr::new(192, 168, 1, 1).into()),
        ("link.invalid", Ipv4Addr::new(169, 254, 1, 1).into()),
        ("ula.invalid", "fd00::1".parse().unwrap()),
    ]);
    let mut tiers: Vec<Vec<Tracker>> = refused
        .iter()
        .map(|u| vec![Tracker::torrent(u.clone())])
        .collect();
    tiers.push(vec![Tracker::operator(at.clone())]);
    let clients = Wired::new(&at);
    let token = CancellationToken::new();
    let stopped = StoppedAnnounces::default();
    let TrackerTask { mut peers, status } =
        start_through(tiers, clients.clone(), names, &token, &stopped);

    for url in &refused {
        let e = refusal_of(&status, &mut peers, url).await;
        assert!(e.contains("not public"), "{url}: {e}");
    }
    let q = answer_with_a_peer(
        &tracker,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    assert_eq!(*clients.built.lock(), [IpAddr::from(Ipv4Addr::LOCALHOST)]);

    drop(peers);
    let q = next_announce(&tracker).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    stopped.wait_idle().await;
    token.cancel();
}

#[cfg(feature = "e2e-local-trackers")]
#[tokio::test]
async fn an_e2e_build_asks_a_tracker_the_torrent_named_at_a_local_address() {
    // In an e2e build a tracker that came with the torrent is asked at
    // any address, link-local too, at any path.
    let (tracker, at) = http_listener().await;
    let port = at.port().unwrap();
    let asked = Url::parse(&format!("http://link.invalid:{port}/0123abcd/announce")).unwrap();
    let names = Names(vec![("link.invalid", Ipv4Addr::new(169, 254, 1, 1).into())]);
    let tiers = vec![vec![Tracker::torrent(asked)]];
    let clients = Wired::new(&at);
    let token = CancellationToken::new();
    let stopped = StoppedAnnounces::default();
    let TrackerTask {
        mut peers,
        status: _status,
    } = start_through(tiers, clients.clone(), names, &token, &stopped);

    let q = answer_with_a_peer(
        &tracker,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    assert_eq!(
        *clients.built.lock(),
        [IpAddr::from(Ipv4Addr::new(169, 254, 1, 1))]
    );

    drop(peers);
    let q = next_announce(&tracker).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    stopped.wait_idle().await;
    token.cancel();
}

// The e2e carveout asks a torrent's tracker at these local addresses.
#[cfg(not(feature = "e2e-local-trackers"))]
#[tokio::test]
async fn a_udp_tracker_is_resolved_to_the_addresses_its_naming_allows() {
    // A UDP tracker that came with the torrent has no address to be
    // asked at among private ones; the operator's is asked there. A
    // `udp://` URL hands an address over as written, and it is not
    // looked up.
    let names = Names(vec![("ten.invalid", Ipv4Addr::new(10, 0, 0, 1).into())]);
    for (host, at) in [
        (url::Host::Domain("ten.invalid"), Ipv4Addr::new(10, 0, 0, 1)),
        (
            url::Host::Ipv4(Ipv4Addr::new(192, 168, 1, 1)),
            Ipv4Addr::new(192, 168, 1, 1),
        ),
        (
            url::Host::Domain("192.168.1.1"),
            Ipv4Addr::new(192, 168, 1, 1),
        ),
    ] {
        let refused =
            udp_tracker_to_socket_addrs(host.clone(), 6969, &names, &reach_all(), NamedBy::Torrent)
                .await
                .unwrap_err();
        assert!(
            refused.downcast_ref::<super::NotPublic>().is_some(),
            "{refused:#}"
        );
        let asked =
            udp_tracker_to_socket_addrs(host, 6969, &names, &reach_all(), NamedBy::Operator)
                .await
                .unwrap();
        assert!(
            matches!(asked, UdpTrackerResolveResult::One(a) if a == (at, 6969).into()),
            "{asked:?}"
        );
    }
}

// The e2e carveout asks a torrent's tracker at these local addresses.
#[cfg(not(feature = "e2e-local-trackers"))]
#[tokio::test]
async fn a_public_tracker_may_not_move_an_announce_to_a_private_address() {
    // A tracker reached at public addresses only that redirects to a
    // private one is refused at the new hop, whoever named it: what it
    // moves is held to public addresses. No client is built for the
    // private hop.
    for named_by in [NamedBy::Torrent, NamedBy::Operator] {
        let (tracker, at) = http_listener().await;
        let port = at.port().unwrap();
        let url = Url::parse(&format!("http://public.invalid:{port}/announce")).unwrap();
        let names = Names(vec![
            ("public.invalid", Ipv4Addr::new(8, 8, 8, 8).into()),
            ("private.invalid", Ipv4Addr::new(10, 0, 0, 1).into()),
        ]);
        let clients = Wired::new(&at);
        let token = CancellationToken::new();
        let stopped = StoppedAnnounces::default();
        let TrackerTask { mut peers, status } = start_through(
            vec![vec![Tracker {
                url: url.clone(),
                named_by,
            }]],
            clients.clone(),
            names,
            &token,
            &stopped,
        );
        let serve = async {
            let (conn, q) = read_announce(&tracker).await;
            redirect(conn, &format!("http://private.invalid:{port}/announce?{q}")).await;
            std::future::pending::<()>().await
        };
        let e = tokio::select! {
            _ = serve => unreachable!(),
            e = refusal_of(&status, &mut peers, &url) => e,
        };
        assert!(e.contains("not public"), "{named_by:?}: {e}");
        assert_eq!(
            *clients.built.lock(),
            [IpAddr::from(Ipv4Addr::new(8, 8, 8, 8))],
            "{named_by:?}"
        );
        drop(peers);
        stopped.wait_idle().await;
        token.cancel();
    }
}

#[cfg(not(feature = "e2e-local-trackers"))]
#[tokio::test]
async fn a_tracker_answering_at_its_local_address_may_move_an_announce_to_a_private_one() {
    // The operator's tracker is found at a public and a local address
    // and answers at the local one: its redirect is not held to public
    // addresses, as one found at local addresses only is not.
    let (tracker, at) = http_listener().await;
    let port = at.port().unwrap();
    let url = Url::parse(&format!("http://mixed.invalid:{port}/announce")).unwrap();
    let public = IpAddr::from(Ipv4Addr::new(8, 8, 8, 8));
    let private = IpAddr::from(Ipv4Addr::new(10, 0, 0, 1));
    let names = Names(vec![
        ("mixed.invalid", public),
        ("mixed.invalid", Ipv4Addr::LOCALHOST.into()),
        ("private.invalid", private),
    ]);
    let clients = Wired::new(&at);
    let token = CancellationToken::new();
    let stopped = StoppedAnnounces::default();
    let TrackerTask { mut peers, .. } = start_through(
        vec![vec![Tracker {
            url,
            named_by: NamedBy::Operator,
        }]],
        clients.clone(),
        names,
        &token,
        &stopped,
    );
    let (conn, q) = tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = read_announce(&tracker) => r,
    };
    redirect(conn, &format!("http://private.invalid:{port}/announce?{q}")).await;
    answer_with_a_peer(
        &tracker,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(
        *clients.built.lock(),
        [public, Ipv4Addr::LOCALHOST.into(), private]
    );
    drop(peers);
    stopped.wait_idle().await;
    token.cancel();
}

// The network resolves the operator's tracker at its exit, so where it
// answered is not known here: its redirect to a local name is held as one
// from a tracker that came with the torrent, and refused before any
// client is built for it.
#[cfg(not(feature = "e2e-local-trackers"))]
#[tokio::test]
async fn an_operators_tracker_resolved_at_the_exit_may_not_move_an_announce_to_a_local_name() {
    struct AtExit;
    impl Resolver for AtExit {
        fn resolve<'a>(
            &'a self,
            _: &'a str,
            _: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
            Box::pin(async { Ok(crate::core::net::Resolved::AtExit) })
        }
    }
    let (tracker, at) = http_listener().await;
    let port = at.port().unwrap();
    let url = Url::parse(&format!("http://exit.invalid:{port}/announce")).unwrap();
    let clients = Wired::new(&at);
    let token = CancellationToken::new();
    let stopped = StoppedAnnounces::default();
    let udp_client = UdpTrackerClient::new_with_socket(
        token.clone(),
        crate::test_support::loopback_udp_now(),
        Some(Arc::new(AtExit)),
    );
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: vec![vec![Tracker {
            url: url.clone(),
            named_by: NamedBy::Operator,
        }]],
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            clients.clone(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });
    let serve = async {
        let (conn, q) = read_announce(&tracker).await;
        redirect(conn, &format!("http://nas.lan:{port}/announce?{q}")).await;
        std::future::pending::<()>().await
    };
    let e = tokio::select! {
        _ = serve => unreachable!(),
        e = refusal_of(&status, &mut peers, &url) => e,
    };
    assert!(e.contains("local one"), "{e}");
    // The first hop's client was built, with no address known; none for
    // the local name.
    assert!(clients.built.lock().is_empty());
    drop(peers);
    stopped.wait_idle().await;
    token.cancel();
}

struct CountingResolver {
    calls: AtomicUsize,
}

impl Resolver for CountingResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
        Box::pin(async move {
            assert_eq!(host, "tracker.invalid");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::core::net::Resolved::Addresses(vec![
                (Ipv4Addr::LOCALHOST, port).into(),
            ]))
        })
    }
}

#[tokio::test]
async fn test_udp_tracker_hostname_through_custom_resolver() {
    const CONNECTION_ID: u64 = 0x1122_3344_5566_7788;

    let tracker = crate::test_support::loopback_udp().await;
    let tracker_addr = tracker.bind_addr();

    let client_sock = crate::test_support::loopback_udp().await;
    let client_addr = client_sock.bind_addr();

    let resolver = Arc::new(CountingResolver {
        calls: AtomicUsize::new(0),
    });
    let token = CancellationToken::new();
    let client =
        UdpTrackerClient::new_with_socket(token.clone(), client_sock, Some(resolver.clone()));

    let url = Url::parse(&format!(
        "udp://tracker.invalid:{}/announce",
        tracker_addr.port()
    ))
    .unwrap();
    let resolved = udp_tracker_to_socket_addrs(
        url.host().unwrap(),
        url.port().unwrap(),
        client.resolver(),
        &reach_all(),
        NamedBy::Operator,
    )
    .await
    .unwrap();
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    let addr = match resolved {
        UdpTrackerResolveResult::One(addr) => addr,
        other => panic!("expected one address, got {other:?}"),
    };
    assert_eq!(addr, tracker_addr);

    let fake_tracker = async {
        let mut buf = [0u8; 1024];
        let (len, from): (usize, SocketAddr) = tracker.recv_from(&mut buf).await.unwrap();
        assert_eq!(from, client_addr);
        assert_eq!(len, 16);
        assert_eq!(buf[..8], CONNECTION_ID_MAGIC.to_be_bytes());
        assert_eq!(buf[8..12], ACTION_CONNECT.to_be_bytes());
        let tid = &buf[12..16];

        let mut reply = Vec::new();
        reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
        reply.extend_from_slice(tid);
        reply.extend_from_slice(&CONNECTION_ID.to_be_bytes());
        tracker.send_to(&reply, from).await.unwrap();
    };

    let (connection_id, ()) = tokio::join!(client.get_connection_id(addr), fake_tracker);
    assert_eq!(connection_id.unwrap(), CONNECTION_ID);
    token.cancel();
}

#[tokio::test]
async fn a_udp_answer_has_libtorrents_min_interval() {
    // libtorrent udp_tracker_connection::on_announce_response: BEP 15
    // carries no min interval, so it is 60 s.
    let tracker = crate::test_support::loopback_udp().await;
    let url = Url::parse(&format!("udp://{}/announce", tracker.bind_addr())).unwrap();
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    // Answers every connect and announce, the stopped one included.
    let serve = tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        loop {
            let (len, from) = tracker.recv_from(&mut buf).await.unwrap();
            let tid = &buf[12..16];
            let mut reply = Vec::new();
            if len == 16 {
                reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
                reply.extend_from_slice(tid);
                reply.extend_from_slice(&1u64.to_be_bytes());
            } else {
                reply.extend_from_slice(&1u32.to_be_bytes()); // announce
                reply.extend_from_slice(tid);
                reply.extend_from_slice(&1800u32.to_be_bytes()); // interval
                reply.extend_from_slice(&5u32.to_be_bytes()); // leechers
                reply.extend_from_slice(&3u32.to_be_bytes()); // seeders
            }
            tracker.send_to(&reply, from).await.unwrap();
        }
    });

    let mut rx = status.subscribe();
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = rx.wait_for(|announces| announces.iter().all(|a| a.last.is_some())) => {
            r.unwrap();
        }
    }
    let announces = status.announces();
    assert_eq!(
        announce_of(&announces, &url).last.as_ref().unwrap().result,
        Ok(AnnounceOk {
            interval: Duration::from_secs(1800),
            min_interval: Duration::from_secs(60),
            seeders: Some(3),
            leechers: Some(5),
            downloaded: None,
            peers: Vec::new(),
            warning: None,
        })
    );

    drop(peers);
    stopped.wait_idle().await;
    serve.abort();
    token.cancel();
}

/// Resolves each name to its own addresses, in the order given, and an
/// address written out (a UDP URL's host is a name to `url`) to itself,
/// as the host's resolver does.
struct Named(Vec<(&'static str, Vec<IpAddr>)>);

impl Resolver for Named {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
        Box::pin(async move {
            if let Ok(ip) = host.parse::<IpAddr>() {
                return Ok(crate::core::net::Resolved::Addresses(vec![
                    (ip, port).into(),
                ]));
            }
            let (_, addrs) = self
                .0
                .iter()
                .find(|(name, _)| *name == host)
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::NotFound, host.to_owned())
                })?;
            Ok(crate::core::net::Resolved::Addresses(
                addrs.iter().map(|ip| (*ip, port).into()).collect(),
            ))
        })
    }
}

// A datagram goes to an address: a UDP tracker whose name the network
// resolves at its exit is not asked.
#[tokio::test]
async fn a_udp_tracker_resolved_at_the_exit_is_not_asked() {
    struct AtExit(AtomicUsize);
    impl Resolver for AtExit {
        fn resolve<'a>(
            &'a self,
            host: &'a str,
            _: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
            assert_eq!(host, "tracker.invalid");
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(crate::core::net::Resolved::AtExit) })
        }
    }
    let resolver = AtExit(AtomicUsize::new(0));
    let found = udp_tracker_to_socket_addrs(
        url::Host::Domain("tracker.invalid"),
        6969,
        &resolver,
        &reach_all(),
        NamedBy::Operator,
    )
    .await;
    assert!(found.is_err(), "{found:?}");
    assert_eq!(resolver.0.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_tracker_with_no_address_the_session_may_reach_is_never_asked() {
    // libtorrent http_tracker_connection::on_filter and
    // udp_tracker_connection::name_lookup drop every endpoint the listen
    // socket cannot route (an IPv4-only session's IPv6 ones), then every
    // one the ip_filter blocks; with none left the announce fails,
    // announce_skipped or banned_by_ip_filter, before anything is sent.
    let (http, http_at) = http_listener().await;
    let udp = crate::test_support::loopback_kernel_udp();
    let (hp, up) = (
        http_at.port().unwrap(),
        crate::network::DatagramSocket::bind_addr(&*udp).port(),
    );
    let url = |s: &str| {
        Url::parse(
            &s.replace("HP", &hp.to_string())
                .replace("UP", &up.to_string()),
        )
        .unwrap()
    };
    let banned = "banned by the IP filter";
    let family = "of a family the session does not connect over";
    let refused = [
        (url("http://127.0.0.1:HP/announce"), banned),
        (url("udp://127.0.0.1:UP/announce"), banned),
        (url("http://tracker.invalid:HP/announce"), banned),
        (url("udp://tracker.invalid:UP/announce"), banned),
        (url("http://[::1]:HP/announce"), family),
        (url("udp://[::1]:UP/announce"), family),
    ];
    let loopback = IpAddr::from(Ipv4Addr::LOCALHOST);
    let resolver = Arc::new(Named(vec![(
        "tracker.invalid",
        vec![Ipv6Addr::LOCALHOST.into(), loopback],
    )]));
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, Some(resolver));
    let clients = Arc::new(Pinned::default());
    let stopped = StoppedAnnounces::default();
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(refused.iter().map(|(u, _)| vec![u.clone()]).collect()),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            clients.clone(),
            reach_but(vec![loopback], true),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });

    for (url, why) in &refused {
        let e = refusal_of(&status, &mut peers, url).await;
        assert!(e.contains(why), "{url}: {e}");
    }
    // No client was built, and neither tracker heard anything.
    assert!(clients.built.lock().is_empty());
    assert!(http.accept().now_or_never().is_none());
    let mut buf = [0u8; 64];
    assert_eq!(
        udp.try_recv_from(&mut buf).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    drop(peers);
    stopped.wait_idle().await;
    token.cancel();
}

#[tokio::test]
async fn every_hop_of_a_tracker_get_goes_only_to_addresses_the_session_may_reach() {
    // libtorrent http_tracker_connection::on_filter, on the first
    // resolve and on each redirect's: the endpoints of a family the
    // listen socket cannot route and those the ip_filter blocks are
    // dropped, connect tries those left, and a hop with none left fails
    // the announce banned_by_ip_filter before it is sent.
    let (tracker, at) = http_listener().await;
    let port = at.port().unwrap();
    let url = Url::parse(&format!("http://tracker.invalid:{port}/announce")).unwrap();
    let blocked = IpAddr::from(Ipv4Addr::new(127, 0, 0, 3));
    let loopback = IpAddr::from(Ipv4Addr::LOCALHOST);
    let resolver = Arc::new(Named(vec![
        (
            "tracker.invalid",
            vec![Ipv6Addr::LOCALHOST.into(), blocked, loopback],
        ),
        ("moved.invalid", vec![blocked]),
    ]));
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, Some(resolver));
    let clients = Arc::new(Pinned::default());
    let stopped = StoppedAnnounces::default();
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![url.clone()]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            clients.clone(),
            reach_but(vec![blocked], true),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });

    let serve = async {
        let (conn, q) = read_announce(&tracker).await;
        redirect(conn, &format!("http://moved.invalid:{port}/announce?{q}")).await;
        std::future::pending::<()>().await
    };
    let e = tokio::select! {
        _ = serve => unreachable!(),
        e = refusal_of(&status, &mut peers, &url) => e,
    };
    assert!(e.contains("banned by the IP filter"), "{e}");
    // The tracker was reached at the one address left; the moved host
    // never had a client built for it.
    assert_eq!(
        *clients.built.lock(),
        vec![("tracker.invalid".to_owned(), port, vec![loopback])]
    );
    drop(peers);
    stopped.wait_idle().await;
    token.cancel();
}

#[tokio::test(start_paused = true)]
async fn a_udp_tracker_is_asked_only_at_addresses_the_session_may_reach() {
    // libtorrent udp_tracker_connection::name_lookup keeps the endpoints
    // the listen socket can route and the ip_filter lets through, in the
    // order found, and asks from those.
    let blocked = IpAddr::from(Ipv4Addr::new(127, 0, 0, 3));
    let resolver = Named(vec![(
        "tracker.invalid",
        vec![
            Ipv6Addr::LOCALHOST.into(),
            blocked,
            Ipv4Addr::LOCALHOST.into(),
            Ipv4Addr::new(127, 0, 0, 4).into(),
        ],
    )]);
    let host = url::Host::Domain("tracker.invalid");
    let v4 = (Ipv4Addr::LOCALHOST, 6969).into();
    let v6 = (Ipv6Addr::LOCALHOST, 6969).into();

    let ipv4_only = reach_but(vec![blocked], true);
    let found =
        udp_tracker_to_socket_addrs(host.clone(), 6969, &resolver, &ipv4_only, NamedBy::Operator)
            .await
            .unwrap();
    assert!(
        matches!(found, UdpTrackerResolveResult::One(a) if a == v4),
        "{found:?}"
    );

    let both = reach_but(vec![blocked], false);
    let found = udp_tracker_to_socket_addrs(host, 6969, &resolver, &both, NamedBy::Operator)
        .await
        .unwrap();
    assert!(
        matches!(found, UdpTrackerResolveResult::Two(a, b) if SocketAddr::from(a) == v4 && SocketAddr::from(b) == v6),
        "{found:?}"
    );
}

/// A resolver that finds every name at loopback and tells each name it
/// was asked for.
struct Asked(tokio::sync::watch::Sender<Vec<String>>);

impl Resolver for Asked {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
        self.0.send_modify(|asked| asked.push(host.to_owned()));
        Box::pin(async move {
            Ok(crate::core::net::Resolved::Addresses(vec![
                (Ipv4Addr::LOCALHOST, port).into(),
            ]))
        })
    }
}

// One torrent's HTTP and UDP trackers have their names looked up by one
// resolver, the session's: there is one path for tracker names.
#[tokio::test]
async fn a_torrents_http_and_udp_tracker_names_go_through_one_resolver() {
    let (_http, at) = http_listener().await;
    let udp = crate::test_support::loopback_udp().await;
    let named = |s: String| Url::parse(&s).unwrap();
    let http_url = named(format!(
        "http://http.invalid:{}/announce",
        at.port().unwrap()
    ));
    let udp_url = named(format!(
        "udp://udp.invalid:{}/announce",
        udp.bind_addr().port()
    ));
    let (asked_tx, mut asked) = tokio::sync::watch::channel(Vec::new());
    let token = CancellationToken::new();
    let udp_client = UdpTrackerClient::new_with_socket(
        token.clone(),
        crate::test_support::loopback_udp().await,
        Some(Arc::new(Asked(asked_tx))),
    );
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![http_url], vec![udp_url]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: StoppedAnnounces::default(),
        counts: Default::default(),
    });
    tokio::select! {
        _ = peers.next() => panic!("no tracker answers"),
        r = asked.wait_for(|names| {
            names.iter().any(|n| n == "http.invalid") && names.iter().any(|n| n == "udp.invalid")
        }) => { r.unwrap(); }
    }
    assert!(
        asked
            .borrow()
            .iter()
            .all(|n| n == "http.invalid" || n == "udp.invalid")
    );
    drop((peers, status));
    token.cancel();
}

/// What a tracker is sent, byte for byte. Each test compares the request a
/// tracker reads with fixed text; a change of these bytes is a change a
/// tracker sees.
mod golden {
    use super::super::tests::{ManualStats, live, read_request};
    use super::super::{TorrentStatsProvider, TrackerCommsStats};
    use super::*;
    use crate::Identity;
    use crate::tracker::{AnnounceShape, qbittorrent};

    /// Starts announcing to `tiers` as `identity`, for the torrent `stats`
    /// counts, through `clients`, names looked up by `names`.
    fn start_counted(
        identity: Identity,
        tiers: Vec<Vec<Tracker>>,
        stats: Box<dyn TorrentStatsProvider>,
        clients: Arc<dyn HttpClients>,
        names: Option<Arc<dyn Resolver>>,
        token: &CancellationToken,
        stopped: &StoppedAnnounces,
    ) -> TrackerTask {
        let udp_sock = crate::test_support::loopback_udp_now();
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, names);
        TrackerComms::start_with_tiers(TrackerSetup {
            info_hashes: v1_hash(),
            identity,
            tiers,
            stats,
            force_interval: None,
            listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
            vote: no_vote(),
            protocols: protocols(clients, reach_all(), udp_client, Default::default(), false),
            stopped: stopped.clone(),
            counts: Default::default(),
        })
    }

    /// A private torrent's counts: distinct, so a field sent in the
    /// place of another shows.
    fn private(downloaded: u64, left: u64, completed_during_run: bool) -> TrackerCommsStats {
        TrackerCommsStats {
            uploaded_bytes: 3000,
            private: true,
            ..live(downloaded, left, completed_during_run)
        }
    }

    fn counted(stats: TrackerCommsStats) -> Arc<ManualStats> {
        Arc::new(ManualStats {
            stats: parking_lot::Mutex::new(stats),
            completed: tokio::sync::watch::channel(false).0,
        })
    }

    /// The tests' identity, its announces in `shape`.
    fn shaped(shape: &'static AnnounceShape) -> Identity {
        Identity {
            shape,
            ..identity()
        }
    }

    /// A private tracker at `dual.invalid`, named in both families, each
    /// routed from a public address of the session's own, so each event
    /// goes out twice, once a family. The own addresses are never
    /// dialled: [`Wired`] clients reach every tracker at its loopback
    /// listener.
    fn dual_private(at: &Url) -> (Url, Arc<Wired>, Names) {
        let url = Url::parse(&format!(
            "http://dual.invalid:{}/announce/0123passkey?pk=s3cret",
            at.port().unwrap()
        ))
        .unwrap();
        let (names, addrs, _) = dual_stack();
        let own: [IpAddr; 2] = [Ipv4Addr::new(1, 2, 3, 4).into(), "2a0a::1".parse().unwrap()];
        let clients = Wired::routing(
            at,
            vec![
                (addrs[0], Egress::From(own[0])),
                (addrs[1], Egress::From(own[1])),
            ],
        );
        (url, clients, names)
    }

    /// The next two requests, the two families' announces of one event,
    /// answered with `bodies`; their heads with the listener's port masked.
    async fn both_families(
        listener: &tokio::net::TcpListener,
        port: u16,
        bodies: [&str; 2],
    ) -> [String; 2] {
        let mut heads = Vec::new();
        let mut conns = Vec::new();
        for _ in 0..2 {
            let (conn, head) = read_request(listener).await;
            conns.push(conn);
            heads.push(head.replace(&format!(":{port}"), ":PORT"));
        }
        for (conn, body) in conns.into_iter().zip(bodies) {
            respond(conn, body).await;
        }
        <[String; 2]>::try_from(heads).unwrap()
    }

    /// A Weaver-shaped announce's request head, `query` following the
    /// hash and peer id. Weaver's shape lists no own address.
    fn weaver(query: &str) -> String {
        format!(
            "GET /announce/0123passkey?\
             info_hash=%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01\
             &peer_id=%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02\
             {query}&pk=s3cret HTTP/1.1\r\n\
             user-agent: Test/1.0\r\n\
             accept: */*\r\n\
             accept-encoding: zstd,gzip,deflate,br\r\n\
             host: dual.invalid:PORT\r\n\r\n"
        )
    }

    /// A qBittorrent-shaped announce's request head, `query` following the
    /// hash and peer id; reqwest adds the `Accept`, last.
    fn qbit(query: &str) -> String {
        format!(
            "GET /announce/0123passkey?pk=s3cret\
             &info_hash=%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01%01\
             &peer_id=%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02%02\
             {query} HTTP/1.1\r\n\
             Host: dual.invalid:PORT\r\n\
             User-Agent: Test/1.0\r\n\
             Accept-Encoding: gzip\r\n\
             Connection: close\r\n\
             Accept: */*\r\n\r\n"
        )
    }

    const NAMES_A_PEER: &str = "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe";
    const NAMES_ANOTHER: &str = "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aBe";
    const NAMES_A_PEER_AND_AN_ID: &str =
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aA10:tracker id3:t-1e";
    const NAMES_AN_ID: &str = "d8:intervali1800e5:peers0:10:tracker id3:t-1e";
    const NO_PEERS: &str = "d8:intervali1800e5:peers0:e";

    /// A private torrent's `started` then `stopped` in `shape`, on a route
    /// with a public address in each family; the heads they were sent as.
    async fn started_then_stopped(shape: &'static AnnounceShape) -> [[String; 2]; 2] {
        let (listener, at) = http_listener().await;
        let port = at.port().unwrap();
        let (url, clients, names) = dual_private(&at);
        let stats = counted(private(5000, 7000, false));
        let token = CancellationToken::new();
        let stopped = StoppedAnnounces::default();
        let TrackerTask {
            mut peers,
            status: _status,
        } = start_counted(
            shaped(shape),
            vec![vec![Tracker::operator(url)]],
            Box::new(stats.clone()),
            clients,
            Some(Arc::new(names)),
            &token,
            &stopped,
        );
        let started = tokio::select! {
            _ = peers.next() => panic!("a peer came before the tracker was asked"),
            heads = both_families(&listener, port, [NAMES_A_PEER, NO_PEERS]) => heads,
        };
        // The answer is taken in: the tracker is owed `stopped`.
        assert_eq!(peers.next().await, Some(the_peer()));
        drop(peers);
        let stopped_heads = both_families(&listener, port, [NO_PEERS, NO_PEERS]).await;
        stopped.wait_idle().await;
        token.cancel();
        [started, stopped_heads]
    }

    // A private torrent's `started`, then `stopped` after the answered
    // `started`. qBittorrent's shape lists the public address each family
    // leaves from as `ipv4` and `ipv6`; Weaver's lists none.
    #[tokio::test]
    async fn a_private_trackers_started_announce_keeps_its_bytes() {
        let [started, stopped] = started_then_stopped(&AnnounceShape::WEAVER).await;
        let want = weaver(
            "&event=started&port=6881&uploaded=3000&downloaded=5000&left=7000&corrupt=0\
             &redundant=0&compact=1&no_peer_id=0&numwant=200&key=24301",
        );
        assert_eq!(started, [want.clone(), want]);
        let want = weaver(
            "&event=stopped&port=6881&uploaded=3000&downloaded=5000&left=7000&corrupt=0\
             &redundant=0&compact=1&no_peer_id=0&numwant=0&key=24301",
        );
        assert_eq!(stopped, [want.clone(), want]);

        let [started, stopped] = started_then_stopped(&qbittorrent::SHAPE).await;
        let want = qbit(
            "&port=6881&uploaded=3000&downloaded=5000&left=7000&corrupt=0&key=00005EED\
             &event=started&numwant=200&compact=1&no_peer_id=1&redundant=0\
             &ipv4=1.2.3.4&ipv6=2a0a%3a%3a1",
        );
        assert_eq!(started, [want.clone(), want]);
        let want = qbit(
            "&port=6881&uploaded=3000&downloaded=5000&left=7000&corrupt=0&key=00005EED\
             &event=stopped&numwant=0&compact=1&no_peer_id=1&redundant=0\
             &ipv4=1.2.3.4&ipv6=2a0a%3a%3a1",
        );
        assert_eq!(stopped, [want.clone(), want]);
    }

    /// A private torrent in `shape` whose tracker answers `started` with a
    /// tracker id, then completes during the run; the heads of its
    /// `started`, `completed` and `stopped`.
    async fn an_id_then_completed(shape: &'static AnnounceShape) -> [[String; 2]; 3] {
        let (listener, at) = http_listener().await;
        let port = at.port().unwrap();
        let (url, clients, names) = dual_private(&at);
        let stats = counted(private(5000, 7000, false));
        let token = CancellationToken::new();
        let stopped = StoppedAnnounces::default();
        let TrackerTask {
            mut peers,
            status: _status,
        } = start_counted(
            shaped(shape),
            vec![vec![Tracker::operator(url)]],
            Box::new(stats.clone()),
            clients,
            Some(Arc::new(names)),
            &token,
            &stopped,
        );
        let started = tokio::select! {
            _ = peers.next() => panic!("a peer came before the tracker was asked"),
            heads = both_families(
                &listener,
                port,
                [NAMES_A_PEER_AND_AN_ID, NAMES_AN_ID],
            ) => heads,
        };
        assert_eq!(peers.next().await, Some(the_peer()));

        *stats.stats.lock() = private(12000, 0, true);
        stats.completed.send_replace(true);
        // The task runs while its peers are asked for; the peer the
        // answer names shows it was taken in before the run ends, so the
        // tracker is owed `stopped`.
        let (completed, peer) = tokio::join!(
            both_families(&listener, port, [NAMES_ANOTHER, NO_PEERS]),
            peers.next()
        );
        let other: SocketAddr = (Ipv4Addr::LOCALHOST, 0x1a42).into();
        assert_eq!(peer, Some(other));

        drop(peers);
        let stopped_heads = both_families(&listener, port, [NO_PEERS, NO_PEERS]).await;
        stopped.wait_idle().await;
        token.cancel();
        [started, completed, stopped_heads]
    }

    // A tracker id a tracker answered with is sent back on every later
    // announce: `completed`, once earned, and `stopped`.
    #[tokio::test]
    async fn a_trackers_id_and_an_earned_completed_keep_their_bytes() {
        let [started, completed, stopped] = an_id_then_completed(&AnnounceShape::WEAVER).await;
        let want = weaver(
            "&event=started&port=6881&uploaded=3000&downloaded=5000&left=7000&corrupt=0\
             &redundant=0&compact=1&no_peer_id=0&numwant=200&key=24301",
        );
        assert_eq!(started, [want.clone(), want]);
        let want = weaver(
            "&event=completed&port=6881&uploaded=3000&downloaded=12000&left=0&corrupt=0\
             &redundant=0&compact=1&no_peer_id=0&numwant=200&key=24301&trackerid=t-1",
        );
        assert_eq!(completed, [want.clone(), want]);
        let want = weaver(
            "&event=stopped&port=6881&uploaded=3000&downloaded=12000&left=0&corrupt=0\
             &redundant=0&compact=1&no_peer_id=0&numwant=0&key=24301&trackerid=t-1",
        );
        assert_eq!(stopped, [want.clone(), want]);

        let [started, completed, stopped] = an_id_then_completed(&qbittorrent::SHAPE).await;
        let want = qbit(
            "&port=6881&uploaded=3000&downloaded=5000&left=7000&corrupt=0&key=00005EED\
             &event=started&numwant=200&compact=1&no_peer_id=1&redundant=0\
             &ipv4=1.2.3.4&ipv6=2a0a%3a%3a1",
        );
        assert_eq!(started, [want.clone(), want]);
        let want = qbit(
            "&port=6881&uploaded=3000&downloaded=12000&left=0&corrupt=0&key=00005EED\
             &event=completed&numwant=200&compact=1&no_peer_id=1&redundant=0&trackerid=t-1\
             &ipv4=1.2.3.4&ipv6=2a0a%3a%3a1",
        );
        assert_eq!(completed, [want.clone(), want]);
        let want = qbit(
            "&port=6881&uploaded=3000&downloaded=12000&left=0&corrupt=0&key=00005EED\
             &event=stopped&numwant=0&compact=1&no_peer_id=1&redundant=0&trackerid=t-1\
             &ipv4=1.2.3.4&ipv6=2a0a%3a%3a1",
        );
        assert_eq!(stopped, [want.clone(), want]);
    }

    /// The UDP announce of `event` (BEP 15) the tests' torrent sends, its
    /// connection and transaction ids zeroed, then the URL data option
    /// (BEP 41) for `/0123passkey?pk=s3cret`.
    fn udp_announce(event: u32, downloaded: u64, left: u64, num_want: i32) -> Vec<u8> {
        let mut d = Vec::new();
        d.extend_from_slice(&[0; 8]); // connection id, masked
        d.extend_from_slice(&1u32.to_be_bytes()); // action: announce
        d.extend_from_slice(&[0; 4]); // transaction id, masked
        d.extend_from_slice(&[1; 20]); // info hash
        d.extend_from_slice(&[2; 20]); // peer id
        d.extend_from_slice(&downloaded.to_be_bytes());
        d.extend_from_slice(&left.to_be_bytes());
        d.extend_from_slice(&3000u64.to_be_bytes()); // uploaded
        d.extend_from_slice(&event.to_be_bytes());
        d.extend_from_slice(&0u32.to_be_bytes()); // ip: the sender's
        d.extend_from_slice(&0x5eedu32.to_be_bytes()); // key
        d.extend_from_slice(&num_want.to_be_bytes());
        d.extend_from_slice(&6881u16.to_be_bytes()); // port
        let data = b"/0123passkey?pk=s3cret";
        d.extend_from_slice(&[2, u8::try_from(data.len()).unwrap()]);
        d.extend_from_slice(data);
        d
    }

    // The connect and announce datagrams a UDP tracker reads, `started`
    // then `stopped`, every field compared; only the connection id and
    // transaction id, which the exchange sets, are masked.
    #[tokio::test]
    async fn a_udp_trackers_started_and_stopped_keep_their_bytes() {
        let tracker = crate::test_support::loopback_udp().await;
        let url = Url::parse(&format!(
            "udp://{}/0123passkey?pk=s3cret",
            tracker.bind_addr()
        ))
        .unwrap();
        let (heard_tx, mut heard) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        // Answers every connect and announce, the stopped one included.
        let serve = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                let (len, from) = tracker.recv_from(&mut buf).await.unwrap();
                let tid = buf[12..16].to_vec();
                let mut reply = Vec::new();
                if len == 16 {
                    reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
                    reply.extend_from_slice(&tid);
                    reply.extend_from_slice(&0x1122_3344_5566_7788u64.to_be_bytes());
                } else {
                    reply.extend_from_slice(&1u32.to_be_bytes()); // announce
                    reply.extend_from_slice(&tid);
                    reply.extend_from_slice(&1800u32.to_be_bytes()); // interval
                    reply.extend_from_slice(&0u32.to_be_bytes()); // leechers
                    reply.extend_from_slice(&0u32.to_be_bytes()); // seeders
                }
                heard_tx.send(buf[..len].to_vec()).unwrap();
                tracker.send_to(&reply, from).await.unwrap();
            }
        });
        let stats = counted(private(5000, 7000, false));
        let token = CancellationToken::new();
        let stopped = StoppedAnnounces::default();
        let TrackerTask { mut peers, status } = start_counted(
            identity(),
            vec![vec![Tracker::operator(url)]],
            Box::new(stats.clone()),
            crate::test_support::loopback(),
            None,
            &token,
            &stopped,
        );
        let mut rx = status.subscribe();
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = rx.wait_for(|a| a.iter().all(|a| a.last.is_some())) => { r.unwrap(); }
        }
        drop(peers);
        stopped.wait_idle().await;
        serve.abort();
        token.cancel();

        let mut connect = Vec::new();
        connect.extend_from_slice(&CONNECTION_ID_MAGIC.to_be_bytes());
        connect.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
        connect.extend_from_slice(&[0; 4]); // transaction id, masked
        let mut announces = Vec::new();
        while let Ok(mut datagram) = heard.try_recv() {
            datagram[12..16].fill(0);
            if datagram.len() == 16 {
                assert_eq!(datagram, connect);
                continue;
            }
            assert_eq!(datagram[..8], 0x1122_3344_5566_7788u64.to_be_bytes());
            datagram[..8].fill(0);
            announces.push(datagram);
        }
        assert_eq!(
            announces,
            [
                udp_announce(2, 5000, 7000, 200),
                udp_announce(3, 5000, 7000, 0)
            ]
        );
    }
}
