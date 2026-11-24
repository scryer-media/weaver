// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use crate::bencode::ByteBufOwned;
use crate::core::net::DatagramSocket;
use std::sync::Arc;

use super::walk_tests::{drive, queried_at, walk_index, walk_node};
use super::{DhtConfig, DhtState};
use crate::dht::{
    Dht,
    bprotocol::{self, MessageKind},
};

async fn start_dht(bootstrap_addr: SocketAddr) -> Dht {
    DhtState::with_config(DhtConfig {
        listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
        sockets: Some(crate::test_support::socket_factory()),
        bootstrap_addrs: Some(vec![bootstrap_addr.to_string()]),
        ..Default::default()
    })
    .await
    .unwrap()
}

async fn assert_answers_ping(dht: &Dht) {
    let client = crate::test_support::loopback_udp().await;
    client
        .send_to(
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
            dht.listen_addr(),
        )
        .await
        .unwrap();
    let mut buf = [0u8; 1024];
    let len = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client.recv_from(&mut buf).await {
                Ok((len, addr)) if addr == dht.listen_addr() => return len,
                _ => continue,
            }
        }
    })
    .await
    .expect("the DHT stopped answering");
    let response = bprotocol::deserialize_message::<ByteBufOwned>(&buf[..len]).unwrap();
    assert!(matches!(response.kind, MessageKind::Response(_)));
}

// On Windows, an ICMP port unreachable that comes back for an earlier send_to makes the next
// recv_from on the same UDP socket fail with WSAECONNRESET (os error 10054).
#[tokio::test]
async fn test_dht_answers_after_icmp_port_unreachable() {
    // Nothing listens on this port once the socket is dropped.
    let closed = crate::test_support::loopback_udp().await.bind_addr();
    let dht = start_dht(closed).await;

    // Let the bootstrap query reach the closed port and the ICMP error come back.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_answers_ping(&dht).await;
}

// On Windows, a datagram longer than the receive buffer makes recv_from fail with
// WSAEMSGSIZE (os error 10040).
#[tokio::test]
async fn test_dht_answers_after_oversized_datagram() {
    // Stays open, so the bootstrap query gets neither an answer nor an ICMP error.
    let silent = crate::test_support::loopback_udp().await;
    let dht = start_dht(silent.bind_addr()).await;

    // Some platforms refuse to send a datagram this large, which is fine.
    let _ = silent.send_to(&[0u8; 20_000], dht.listen_addr()).await;
    assert_answers_ping(&dht).await;
}

#[tokio::test]
async fn test_dht_answers_ping_through_custom_socket() {
    // Stays open, so the bootstrap query gets neither an answer nor an ICMP error.
    let silent = crate::test_support::loopback_udp().await;
    let custom = crate::test_support::loopback_udp().await;
    let custom_addr = custom.bind_addr();

    let dht = DhtState::with_config(DhtConfig {
        bootstrap_addrs: Some(vec![silent.bind_addr().to_string()]),
        socket: Some(custom),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(dht.listen_addr(), custom_addr);

    let client = crate::test_support::loopback_udp().await;
    client
        .send_to(
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
            custom_addr,
        )
        .await
        .unwrap();
    let mut buf = [0u8; 1024];
    let len = loop {
        let (len, addr) = client.recv_from(&mut buf).await.unwrap();
        if addr == custom_addr {
            break len;
        }
    };
    let response = bprotocol::deserialize_message::<ByteBufOwned>(&buf[..len]).unwrap();
    assert!(matches!(response.kind, MessageKind::Response(_)));
    assert_eq!(response.transaction_id.as_ref(), b"aa");
    dht.cancellation_token().cancel();
}

#[tokio::test]
async fn test_dht_bootstrap_lookup_through_custom_resolver() {
    use crate::core::net::{BoxFuture, Resolver};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct CountingResolver {
        target: SocketAddr,
        calls: AtomicUsize,
    }

    impl Resolver for CountingResolver {
        fn resolve<'a>(
            &'a self,
            host: &'a str,
            port: u16,
        ) -> BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
            Box::pin(async move {
                assert_eq!(host, "bootstrap.invalid");
                assert_eq!(port, self.target.port());
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(crate::core::net::Resolved::Addresses(vec![self.target]))
            })
        }
    }

    let bootstrap = crate::test_support::loopback_udp().await;
    let bootstrap_addr = bootstrap.bind_addr();
    let resolver = Arc::new(CountingResolver {
        target: bootstrap_addr,
        calls: AtomicUsize::new(0),
    });

    let dht = DhtState::with_config(DhtConfig {
        listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
        sockets: Some(crate::test_support::socket_factory()),
        bootstrap_addrs: Some(vec![format!("bootstrap.invalid:{}", bootstrap_addr.port())]),
        resolver: Some(resolver.clone()),
        ..Default::default()
    })
    .await
    .unwrap();

    // The only way the DHT learns this address is through the resolver.
    let mut buf = [0u8; 1024];
    let len = loop {
        let (len, addr) = bootstrap.recv_from(&mut buf).await.unwrap();
        if addr == dht.listen_addr() {
            break len;
        }
    };
    let request = bprotocol::deserialize_message::<ByteBufOwned>(&buf[..len]).unwrap();
    assert!(matches!(request.kind, MessageKind::FindNodeRequest(_)));
    assert!(resolver.calls.load(Ordering::SeqCst) >= 1);
    dht.cancellation_token().cancel();
}

// With no bootstrap host the DHT still runs: it answers, a node a peer
// names enters its table, and a torrent's search, asked for while the
// table was empty, goes to that node.
#[tokio::test]
async fn test_a_dht_with_no_bootstrap_host_runs_from_the_nodes_peers_name() {
    use crate::core::hash_id::Id20;
    use futures::StreamExt;

    let dht = DhtState::with_config(DhtConfig {
        listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
        sockets: Some(crate::test_support::socket_factory()),
        bootstrap_addrs: Some(Vec::new()),
        ..Default::default()
    })
    .await
    .unwrap();
    let info_hash = Id20::new([0x22; 20]);
    let mut peers = dht.get_peers(info_hash, None);
    assert!(futures::poll!(peers.next()).is_pending());

    // A node a peer named (BEP 5 PORT).
    let node = crate::test_support::loopback_udp().await;
    dht.add_node(node.bind_addr());
    let ping = recv_message_from(&node, dht.listen_addr()).await;
    assert!(matches!(ping.kind, MessageKind::PingRequest(_)));
    let mut answer = Vec::new();
    bprotocol::serialize_message(
        &mut answer,
        ping.transaction_id,
        None,
        None,
        MessageKind::<ByteBufOwned>::Response(bprotocol::Response {
            id: Id20::new([0x33; 20]),
            ..Default::default()
        }),
    )
    .unwrap();
    node.send_to(&answer, dht.listen_addr()).await.unwrap();

    // The table's refresh of the node is a get_peers too, for an id of
    // its own: the search is the one for `info_hash`.
    loop {
        let m = recv_message_from(&node, dht.listen_addr()).await;
        if let MessageKind::GetPeersRequest(r) = m.kind
            && r.info_hash == info_hash
        {
            break;
        }
    }
    assert_eq!(dht.stats().routing_table_size, 1);
    dht.cancellation_token().cancel();
}

// Start a DHT on loopback whose one bootstrap node is `bootstrap`, a socket the
// test holds: whatever the DHT sends lands there and nowhere else.
async fn start_dht_with(bootstrap: &Arc<dyn DatagramSocket>, config: DhtConfig) -> Dht {
    DhtState::with_config(DhtConfig {
        listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
        sockets: Some(crate::test_support::socket_factory()),
        bootstrap_addrs: Some(vec![bootstrap.bind_addr().to_string()]),
        ..config
    })
    .await
    .unwrap()
}

async fn recv_message_from(
    sock: &Arc<dyn DatagramSocket>,
    from: SocketAddr,
) -> bprotocol::Message<ByteBufOwned> {
    let mut buf = [0u8; 2048];
    loop {
        let (len, addr) = sock.recv_from(&mut buf).await.unwrap();
        if addr == from {
            return bprotocol::deserialize_message::<ByteBufOwned>(&buf[..len]).unwrap();
        }
    }
}

// The "v" on the DHT's own bootstrap query and on its answer to our ping.
async fn versions_sent(client_version: Option<Vec<u8>>) -> [Option<Vec<u8>>; 2] {
    let bootstrap = crate::test_support::loopback_udp().await;
    let dht = start_dht_with(
        &bootstrap,
        DhtConfig {
            client_version,
            ..Default::default()
        },
    )
    .await;
    let query = recv_message_from(&bootstrap, dht.listen_addr()).await;

    bootstrap
        .send_to(
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
            dht.listen_addr(),
        )
        .await
        .unwrap();
    let answer = loop {
        let m = recv_message_from(&bootstrap, dht.listen_addr()).await;
        if m.transaction_id.as_ref() == b"aa" {
            break m;
        }
    };
    assert!(matches!(answer.kind, MessageKind::Response(_)));
    dht.cancellation_token().cancel();
    [query, answer].map(|m| m.version.map(|v| v.as_ref().to_vec()))
}

#[tokio::test]
async fn test_client_version_is_sent_only_when_configured() {
    assert_eq!(versions_sent(None).await, [None, None]);
    let v = b"XY\x01\x02".to_vec();
    assert_eq!(
        versions_sent(Some(v.clone())).await,
        [Some(v.clone()), Some(v)]
    );
}

#[tokio::test]
async fn test_node_id_carries_no_fingerprint() {
    use crate::core::hash_id::Id20;
    use crate::core::peer_id::generate_azereus_style;
    use crate::dht::routing_table::RoutingTable;

    let bootstrap = crate::test_support::loopback_udp().await;
    let weaver = generate_azereus_style(*b"WV", crate::crate_version!());

    // By default: an entirely random id, not Weaver's fingerprint.
    let dht = start_dht_with(&bootstrap, DhtConfig::default()).await;
    assert!(!dht.stats().id.0.starts_with(&weaver.0[..8]));
    dht.cancellation_token().cancel();

    // A restored id is kept, with its table.
    let kept = Id20::new([7u8; 20]);
    let dht = start_dht_with(
        &bootstrap,
        DhtConfig {
            peer_id: Some(kept),
            routing_table: Some(RoutingTable::new(kept, None)),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(dht.stats().id, kept);
    dht.cancellation_token().cancel();

    // One that reads as a BEP 20 peer id is replaced, and so is the table
    // built around it.
    let mut fingerprinted = [9u8; 20];
    fingerprinted[..8].copy_from_slice(b"-XY1234-");
    let fingerprinted = Id20::new(fingerprinted);
    let dht = start_dht_with(
        &bootstrap,
        DhtConfig {
            peer_id: Some(fingerprinted),
            routing_table: Some(RoutingTable::new(fingerprinted, None)),
            ..Default::default()
        },
    )
    .await;
    let id = dht.stats().id;
    assert!(!id.0.starts_with(b"-XY1234-"));
    dht.with_routing_tables(|v4, v6| {
        assert_eq!(v4.id(), id);
        assert_eq!(v6.id(), id);
    });
    dht.cancellation_token().cancel();
}

// A DHT whose outgoing datagrams land in the returned receiver instead of on
// a socket: the test sees every request sent, and nothing is ever answered
// unless the test answers it.
pub(super) fn fixture_dht() -> (
    std::sync::Arc<DhtState>,
    tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>,
) {
    let (dht, rx) = fixture_state();
    (std::sync::Arc::new(dht), rx)
}

pub(super) fn fixture_state() -> (
    DhtState,
    tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let id = crate::core::hash_id::Id20::new([0x11; 20]);
    let dht = DhtState::new_internal(
        id,
        tx,
        None,
        None,
        (Ipv4Addr::LOCALHOST, 0).into(),
        tokio_util::sync::CancellationToken::new(),
        None,
    );
    dht.started_for_test();
    (dht, rx)
}

// `fixture_dht`, before its bootstrap names are looked up: nothing is
// started.
pub(super) fn unstarted_fixture_dht() -> (
    std::sync::Arc<DhtState>,
    tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let dht = DhtState::new_internal(
        crate::core::hash_id::Id20::new([0x11; 20]),
        tx,
        None,
        None,
        (Ipv4Addr::LOCALHOST, 0).into(),
        tokio_util::sync::CancellationToken::new(),
        None,
    );
    (std::sync::Arc::new(dht), rx)
}

pub(super) fn get_peers_search(
    dht: &std::sync::Arc<DhtState>,
) -> super::RecursiveRequest<super::RecursiveRequestCallbacksGetPeers> {
    super::RecursiveRequest::get_peers(
        dht.clone(),
        crate::core::hash_id::Id20::new([0x22; 20]),
        None,
        tokio::sync::mpsc::unbounded_channel().0,
    )
}

// `n` nodes, each with its own id and address, none of which ever answers.
fn silent_nodes(n: u8) -> Vec<(Option<crate::core::hash_id::Id20>, SocketAddr)> {
    (0..n)
        .map(|i| {
            (
                Some(crate::core::hash_id::Id20::new([i; 20])),
                SocketAddr::from(([10, 0, 0, i], 6881)),
            )
        })
        .collect()
}

// Node i, at its own /24 (see `walk_node`), answers at once naming the
// eight nodes just closer to the target than itself, unless it is a
// third one, which never answers. However many closer nodes the answers
// name, the walk never has more than SEARCH_BRANCHING queries in flight
// while no query is late (no time passes but in jumps of
// RESPONSE_TIMEOUT, past every short timeout): libtorrent with
// `dht_aggressive_lookups` off, traversal_algorithm.cpp:519-523,
// v2.0.15. It asks each node at most once, and ends.
#[tokio::test(start_paused = true)]
async fn test_search_keeps_at_most_branching_requests_in_flight_and_ends() {
    use super::SEARCH_BRANCHING;
    use crate::dht::RESPONSE_TIMEOUT;
    use std::{collections::HashSet, task::Poll};

    let (dht, mut sent) = fixture_dht();
    let req = get_peers_search(&dht);
    let silent = |i: u8| i.is_multiple_of(3);
    let given = (60..65u8).map(|i| {
        let (id, addr) = walk_node(i);
        (Some(id), addr)
    });
    let mut search = std::pin::pin!(req.search(true, given));

    let mut asked = HashSet::new();
    let mut most_in_flight = 0;
    let outcome = loop {
        let polled = futures::poll!(search.as_mut());
        let in_flight = dht.get_stats().outstanding_requests;
        assert!(in_flight <= SEARCH_BRANCHING, "{in_flight} in flight");
        most_in_flight = most_in_flight.max(in_flight);
        if let Poll::Ready(outcome) = polled {
            break outcome.unwrap();
        }
        let mut answered = false;
        while let Ok(request) = sent.try_recv() {
            let i = walk_index(request.addr);
            assert!(asked.insert(i), "{i} asked twice");
            if !silent(i) {
                let closer: Vec<_> = (i.saturating_sub(8)..i)
                    .map(|j| {
                        let (id, addr) = walk_node(j);
                        let SocketAddr::V4(addr) = addr else {
                            unreachable!()
                        };
                        (id, addr)
                    })
                    .collect();
                reply(&dht, &request, walk_node(i).0, &closer, &[]);
                answered = true;
            }
        }
        if !answered {
            // The queries in flight run out their time.
            tokio::time::advance(RESPONSE_TIMEOUT).await;
        }
    };

    assert_eq!(most_in_flight, SEARCH_BRANCHING);
    // It walked down to the nodes nearest the target.
    assert!(asked.contains(&0) && asked.contains(&1));
    // 38 asked: 25 answered, 12 silent ones failed, and one silent one,
    // farther than the eight closest that answered, was withdrawn as
    // the walk ended.
    assert_eq!(asked.len(), 38);
    assert_eq!((outcome.answered, outcome.failed), (25, 12));
    assert_eq!(asked.iter().filter(|&&i| silent(i)).count(), 13);
    assert_eq!(dht.get_stats().outstanding_requests, 1);
}

// The walk starts from five far nodes. One answers at once, naming eight
// nodes closer than all five; the other four stay silent, in flight. No
// time passes, so no query is late: the walk asks one more node, the
// closest, in the room the answer left, not five.
#[tokio::test(start_paused = true)]
async fn test_an_answer_naming_closer_nodes_keeps_branching_in_flight() {
    use super::SEARCH_BRANCHING;
    use std::task::Poll;

    let (dht, mut sent) = fixture_dht();
    let req = get_peers_search(&dht);
    let far: Vec<_> = (20..25u8)
        .map(|i| {
            let (id, addr) = walk_node(i);
            (Some(id), addr)
        })
        .collect();
    let mut search = std::pin::pin!(req.search(true, far.into_iter()));
    assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));
    let first: Vec<_> = std::iter::from_fn(|| sent.try_recv().ok()).collect();
    assert_eq!(first.len(), SEARCH_BRANCHING);

    let closer: Vec<_> = (0..8u8)
        .map(|i| {
            let (id, addr) = walk_node(i);
            let SocketAddr::V4(addr) = addr else {
                unreachable!()
            };
            (id, addr)
        })
        .collect();
    let i = walk_index(first[0].addr);
    reply(&dht, &first[0], walk_node(i).0, &closer, &[]);
    assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));

    let more: Vec<_> = std::iter::from_fn(|| sent.try_recv().ok())
        .map(|request| walk_index(request.addr))
        .collect();
    assert_eq!(more, vec![0]);
    assert_eq!(dht.get_stats().outstanding_requests, SEARCH_BRANCHING);
}

// A node `d` from the target [0x22; 20], at `addr`.
pub(super) fn node_at(
    d: u32,
    addr: [u8; 4],
) -> (crate::core::hash_id::Id20, std::net::SocketAddrV4) {
    let mut id = [0x22; 20];
    for (b, x) in id[16..].iter_mut().zip(d.to_be_bytes()) {
        *b ^= x;
    }
    (
        crate::core::hash_id::Id20::new(id),
        std::net::SocketAddrV4::new(addr.into(), 6881),
    )
}

fn given(
    nodes: impl IntoIterator<Item = (crate::core::hash_id::Id20, std::net::SocketAddrV4)>,
) -> impl Iterator<Item = (Option<crate::core::hash_id::Id20>, SocketAddr)> {
    nodes
        .into_iter()
        .map(|(id, addr)| (Some(id), SocketAddr::V4(addr)))
}

// A node answers naming 50 nodes of one /24, as one could that wants us
// to send there: the walk asks one of them, the first named
// (libtorrent `dht_restrict_search_ips`, traversal_algorithm.cpp:
// 191-226, v2.0.15).
#[tokio::test(start_paused = true)]
async fn test_an_answer_naming_many_nodes_of_one_network_adds_one_of_them() {
    use crate::dht::RESPONSE_TIMEOUT;
    use std::task::Poll;

    let (dht, mut sent) = fixture_dht();
    let req = get_peers_search(&dht);
    let first = node_at(1000, [10, 0, 0, 1]);
    let hostile: Vec<_> = (0..50u8)
        .map(|i| node_at(u32::from(i) + 1, [10, 5, 0, i + 1]))
        .collect();
    let mut search = std::pin::pin!(req.search(true, given([first])));
    let mut asked = Vec::new();
    let outcome = loop {
        let polled = futures::poll!(search.as_mut());
        let mut answered = false;
        while let Ok(request) = sent.try_recv() {
            asked.push(request.addr);
            if request.addr == SocketAddr::V4(first.1) {
                reply(&dht, &request, first.0, &hostile, &[]);
                answered = true;
            }
        }
        if let Poll::Ready(outcome) = polled {
            break outcome.unwrap();
        }
        if !answered {
            tokio::time::advance(RESPONSE_TIMEOUT).await;
        }
    };
    assert_eq!(
        asked,
        vec![SocketAddr::V4(first.1), SocketAddr::V4(hostile[0].1)]
    );
    assert_eq!((outcome.answered, outcome.failed), (1, 1));
}

// A chain of nodes, each answering with a token and naming one node
// closer than itself, as long as the list of candidates the walk keeps.
// The answer to the hundredth query names eight nodes closer to the
// target than any asked. They are asked all the same: the walk has no
// count of queries to spend, and ends as libtorrent's does, once the
// eight closest it knows have answered (traversal_algorithm.cpp:519-523,
// v2.0.15). It announces to those eight.
#[tokio::test(start_paused = true)]
async fn test_closer_nodes_named_by_the_hundredth_answer_are_asked() {
    use super::SEARCH_CANDIDATES;
    use std::{collections::BTreeSet, task::Poll};

    let (dht, mut sent) = fixture_dht();
    let info_hash = crate::core::hash_id::Id20::new([0x22; 20]);
    let req = super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        Some(6881),
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let chain = |n: u32| {
        let [_, _, a, b] = n.to_be_bytes();
        node_at(1_000_000 - n, [10, a, b, 1])
    };
    let index = |addr: SocketAddr| match addr.ip() {
        std::net::IpAddr::V4(a) => u32::from(a.octets()[1]) << 8 | u32::from(a.octets()[2]),
        std::net::IpAddr::V6(_) => unreachable!(),
    };
    let last = u32::try_from(SEARCH_CANDIDATES).unwrap() - 1;
    let closer: Vec<_> = (1..=8u8)
        .map(|i| node_at(u32::from(i), [10, 200, i, 1]))
        .collect();
    let mut search = std::pin::pin!(req.search(true, given([chain(0)])));
    let mut asked = 0;
    let outcome = loop {
        if let Poll::Ready(outcome) = futures::poll!(search.as_mut()) {
            break outcome.unwrap();
        }
        let mut answered = false;
        while let Ok(request) = sent.try_recv() {
            asked += 1;
            let n = index(request.addr);
            if let Some(c) = closer.iter().find(|c| SocketAddr::V4(c.1) == request.addr) {
                answer(&dht, &request, c.0, Some(b"t"), &[]);
            } else if n == last {
                answer(&dht, &request, chain(n).0, Some(b"t"), &closer);
            } else {
                answer(&dht, &request, chain(n).0, Some(b"t"), &[chain(n + 1)]);
            }
            answered = true;
        }
        if !answered {
            tokio::time::advance(Duration::from_millis(100)).await;
        }
    };
    assert_eq!(asked, SEARCH_CANDIDATES + closer.len());
    assert_eq!(
        (outcome.answered, outcome.failed),
        (SEARCH_CANDIDATES + closer.len(), 0)
    );

    let mut announced = BTreeSet::new();
    while let Ok(request) = sent.try_recv() {
        assert!(matches!(request.message.kind, MessageKind::AnnouncePeer(_)));
        announced.insert(request.addr);
    }
    assert_eq!(
        announced,
        closer.iter().map(|c| SocketAddr::V4(c.1)).collect()
    );
}

// The closest node's address has its send window kept full, by traffic
// that is not the walk's. Its query is deferred, and RESPONSE_TIMEOUT
// after that it fails for the walk, which then ends: the eight others
// answered at once.
#[tokio::test(start_paused = true)]
async fn test_an_address_whose_window_stays_full_does_not_hold_a_walk_open() {
    use crate::dht::{RESPONSE_TIMEOUT, send_windows::SEND_LIMIT};
    use std::task::Poll;

    let (dht, mut sent) = fixture_dht();
    let full = walk_node(0).1;
    let fill = || {
        for _ in 0..SEND_LIMIT {
            dht.send_guard.record(full.ip());
        }
    };
    let req = get_peers_search(&dht);
    let start = tokio::time::Instant::now();
    let mut search = std::pin::pin!(req.search(
        true,
        (0..9).map(|i| {
            let (id, addr) = walk_node(i);
            (Some(id), addr)
        })
    ));
    let outcome = loop {
        fill();
        let polled = futures::poll!(search.as_mut());
        let mut answered = false;
        while let Ok(request) = sent.try_recv() {
            assert_ne!(request.addr, full, "sent past a full window");
            let i = walk_index(request.addr);
            reply(&dht, &request, walk_node(i).0, &[], &[]);
            answered = true;
        }
        if let Poll::Ready(outcome) = polled {
            break outcome.unwrap();
        }
        if !answered {
            tokio::time::advance(Duration::from_secs(1)).await;
        }
    };
    assert_eq!((outcome.answered, outcome.failed), (8, 0));
    assert_eq!(tokio::time::Instant::now() - start, RESPONSE_TIMEOUT);
    assert_eq!(dht.send_guard.windows.lock().lined_up(full.ip()), 0);
}

// A walk keeps 100 candidates. The first of the five it asks answers
// naming 100 nodes closer than all of them, so the other four are
// pushed off while they wait on the rate limiter, one query a second:
// they are never sent. The walk ends with the queries it no longer
// needs still waiting, and those are never sent either.
#[tokio::test(start_paused = true)]
async fn test_a_query_for_a_candidate_pushed_off_is_never_sent() {
    use super::{RateLimiter, TokioClock};
    use crate::dht::RESPONSE_TIMEOUT;
    use std::{num::NonZeroU32, task::Poll};

    let (mut state, mut sent) = fixture_state();
    state.rate_limiter = RateLimiter::direct_with_clock(
        governor::Quota::per_second(NonZeroU32::new(1).unwrap()),
        TokioClock {
            start: tokio::time::Instant::now(),
        },
    );
    let dht = std::sync::Arc::new(state);
    let far = |i: u8| node_at(0x1000 + u32::from(i), [10, 1, i, 1]);
    let near: Vec<_> = (0..100u8)
        .map(|j| node_at(0x100 + u32::from(j), [10, 2, j, 1]))
        .collect();
    let req = get_peers_search(&dht);
    let mut search = std::pin::pin!(req.search(true, given((0..100).map(far))));
    let mut asked = Vec::new();
    let outcome = loop {
        let polled = futures::poll!(search.as_mut());
        let mut answered = false;
        while let Ok(request) = sent.try_recv() {
            asked.push(request.addr);
            if request.addr == SocketAddr::V4(far(0).1) {
                reply(&dht, &request, far(0).0, &near, &[]);
            } else {
                let n = near
                    .iter()
                    .find(|n| SocketAddr::V4(n.1) == request.addr)
                    .expect("only far node 0 and the near ones are asked");
                reply(&dht, &request, n.0, &[], &[]);
            }
            answered = true;
        }
        if let Poll::Ready(outcome) = polled {
            break outcome.unwrap();
        }
        if !answered {
            tokio::time::advance(Duration::from_secs(1)).await;
        }
    };
    // The walk ended once the eight closest answered.
    assert_eq!(outcome.answered, 9);
    let mut want: Vec<_> = std::iter::once(far(0).1)
        .chain(near[..8].iter().map(|n| n.1))
        .map(SocketAddr::V4)
        .collect();
    want.sort();
    asked.sort();
    assert_eq!(asked, want);
    for _ in 0..2 {
        tokio::time::advance(RESPONSE_TIMEOUT).await;
    }
    assert!(sent.try_recv().is_err(), "a withdrawn query was sent");
    assert_eq!(dht.get_stats().outstanding_requests, 0);
}

// A query is sent to a node that is then pushed off the walk's list; a
// node named later at the same address, under another id, is asked in
// turn. The answer to the first query, which comes then, is not the
// second one's: the walk waits for that until it fails. The answer
// still reaches the table.
#[tokio::test(start_paused = true)]
async fn test_an_answer_to_a_withdrawn_query_is_not_credited_to_a_new_candidate_at_its_address() {
    use crate::dht::RESPONSE_TIMEOUT;
    use std::task::Poll;

    let (dht, mut sent) = fixture_dht();
    let far = |i: u8| node_at(0x1000 + u32::from(i), [10, 1, i, 1]);
    let near: Vec<_> = (0..100u8)
        .map(|j| node_at(0x100 + u32::from(j), [10, 2, j, 1]))
        .collect();
    // At far node 0's address, closer than all.
    let again = (node_at(0x10, [0; 4]).0, far(0).1);
    let req = get_peers_search(&dht);
    let mut search = std::pin::pin!(req.search(true, given((0..100).map(far))));
    let drain = |sent: &mut tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>| {
        std::iter::from_fn(|| sent.try_recv().ok()).collect::<Vec<_>>()
    };
    let addrs = |requests: &[super::WorkerSendRequest]| {
        let mut addrs: Vec<_> = requests.iter().map(|r| r.addr).collect();
        addrs.sort();
        addrs
    };
    let v4 = |nodes: &[(crate::core::hash_id::Id20, std::net::SocketAddrV4)]| {
        let mut addrs: Vec<_> = nodes.iter().map(|n| SocketAddr::V4(n.1)).collect();
        addrs.sort();
        addrs
    };

    assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));
    let first = drain(&mut sent);
    assert_eq!(addrs(&first), v4(&(0..5).map(far).collect::<Vec<_>>()));
    let old = first
        .iter()
        .find(|r| r.addr == SocketAddr::V4(far(0).1))
        .unwrap();
    let one = first
        .iter()
        .find(|r| r.addr == SocketAddr::V4(far(1).1))
        .unwrap();
    // Far nodes 0 and 2-4 are pushed off with their queries in flight.
    reply(&dht, one, far(1).0, &near, &[]);
    assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));

    let second = drain(&mut sent);
    assert_eq!(addrs(&second), v4(&near[..5]));
    for request in &second {
        let n = near
            .iter()
            .find(|n| SocketAddr::V4(n.1) == request.addr)
            .unwrap();
        let named: &[_] = if n == &near[0] { &[again] } else { &[] };
        reply(&dht, request, n.0, named, &[]);
    }
    assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));

    let third = drain(&mut sent);
    let mut want = v4(&near[5..9]);
    want.push(SocketAddr::V4(again.1));
    want.sort();
    assert_eq!(addrs(&third), want);
    // The first query to that address is answered now, while the
    // table's bucket for these ids still has room.
    reply(&dht, old, far(0).0, &[], &[]);
    assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));
    assert!(
        dht.routing_table_v4
            .read()
            .iter()
            .any(|n| n.id() == far(0).0)
    );
    for request in third.iter().filter(|r| r.addr != SocketAddr::V4(again.1)) {
        let n = near
            .iter()
            .find(|n| SocketAddr::V4(n.1) == request.addr)
            .unwrap();
        reply(&dht, request, n.0, &[], &[]);
    }
    assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));

    tokio::time::advance(RESPONSE_TIMEOUT).await;
    let Poll::Ready(outcome) = futures::poll!(search.as_mut()) else {
        panic!("the walk did not end as the new candidate failed");
    };
    let outcome = outcome.unwrap();
    // Far node 1 and near nodes 0-8 answered; the new candidate failed.
    assert_eq!((outcome.answered, outcome.failed), (10, 1));
}

// Two of the ten nodes are still in flight when the walk ends: node 8,
// which answers after it ended, and node 9, heard of into the table
// before, which never does. Node 8's answer still enters it in the
// table (libtorrent rpc_manager.cpp:276-389, 413-417, v2.0.15). Node 9
// is not failed for the table and is no longer marked as asked as its
// query times out, so the table asks it again at its next tick. The two have ids
// near our own, in the bucket that has room once the eight closest
// have filled theirs.
#[tokio::test(start_paused = true)]
async fn test_a_withdrawn_query_still_feeds_the_table() {
    use super::{REFRESH_SPACING, SHORT_TIMEOUT};
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;

    let (dht, mut sent) = fixture_dht();
    let id_of = |i: u8| {
        if i < 8 {
            walk_node(i).0
        } else {
            let mut id = dht.id.0;
            id[19] ^= i;
            Id20::new(id)
        }
    };
    let addr9 = walk_node(9).1;
    assert!(dht.routing_table_v4.write().heard_about(id_of(9), addr9));
    let slow = Duration::from_secs(2);
    let held = std::sync::Mutex::new(None);
    let req = get_peers_search(&dht);
    let start = tokio::time::Instant::now();
    let given: Vec<_> = (0..10).map(|i| (Some(id_of(i)), walk_node(i).1)).collect();
    let search = tokio::spawn(async move { req.search(true, given.into_iter()).await });
    let (outcome, log) = drive(
        &mut sent,
        search,
        |i| match i {
            0..5 => Some(slow),
            5..9 => Some(Duration::ZERO),
            _ => None,
        },
        |request| {
            let i = walk_index(request.addr);
            if i == 8 {
                *held.lock().unwrap() = Some(request.message.transaction_id.clone());
            } else {
                reply(&dht, request, walk_node(i).0, &[], &[]);
            }
        },
    )
    .await;
    assert_eq!(outcome.unwrap().answered, 8);
    assert_eq!(tokio::time::Instant::now() - start, slow);
    assert_eq!(dht.get_stats().outstanding_requests, 2);
    assert_eq!(queried_at(&log, SHORT_TIMEOUT), (5..10).collect::<Vec<_>>());

    let refresher = tokio::spawn(dht.clone().refresh_table(true));
    let entered = dht.routing_table_v4.read().entered();
    let mut rang = std::pin::pin!(entered.notified());
    rang.as_mut().enable();
    dht.on_received_message(
        bprotocol::Message {
            transaction_id: held.lock().unwrap().take().unwrap(),
            version: None,
            ip: None,
            kind: MessageKind::Response(bprotocol::Response {
                id: id_of(8),
                ..Default::default()
            }),
        },
        walk_node(8).1,
    )
    .unwrap();
    rang.await;
    assert!(
        dht.routing_table_v4
            .read()
            .iter()
            .any(|n| n.id() == id_of(8))
    );

    // The table refreshes its nodes one per tick from `slow` on; node 9
    // is asked at the first tick after its query timed out.
    let request = loop {
        let request = sent.recv().await.unwrap();
        if request.addr == addr9 {
            break request;
        }
        assert!((0..9).any(|i| walk_node(i).1 == request.addr));
    };
    assert!(matches!(
        request.message.kind,
        MessageKind::GetPeersRequest(_)
    ));
    let asked = tokio::time::Instant::now() - start;
    assert!(asked > SHORT_TIMEOUT + RESPONSE_TIMEOUT);
    assert_eq!(asked, slow + 3 * REFRESH_SPACING);
    assert!(asked - REFRESH_SPACING < SHORT_TIMEOUT + RESPONSE_TIMEOUT);
    refresher.abort();
}

#[tokio::test(start_paused = true)]
async fn test_cancelling_a_search_withdraws_its_requests_waiting_on_the_limiter() {
    use super::SEARCH_BRANCHING;
    use crate::dht::RESPONSE_TIMEOUT;
    use std::task::Poll;

    let (dht, mut sent) = fixture_dht();
    // Nothing more goes out until the limiter refills, and the clock stands
    // still until the test moves it.
    while dht.rate_limiter.check().is_ok() {}
    let req = get_peers_search(&dht);
    let mut search = Box::pin(req.search(true, silent_nodes(8).into_iter()));

    assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));
    assert_eq!(dht.get_stats().outstanding_requests, SEARCH_BRANCHING);
    assert!(sent.try_recv().is_err(), "sent past an empty limiter");

    drop(search);
    assert_eq!(dht.get_stats().outstanding_requests, 0);
    // The limiter refills; nobody is left waiting to send.
    tokio::time::advance(RESPONSE_TIMEOUT).await;
    assert!(
        sent.try_recv().is_err(),
        "a cancelled search sent a request"
    );
}

// A DHT whose rate limiter lets one query out a minute and has spent it.
fn fixture_dht_limited() -> (
    Arc<DhtState>,
    tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>,
) {
    use super::{RateLimiter, TokioClock};
    use std::num::NonZeroU32;

    let (mut state, sent) = fixture_state();
    state.rate_limiter = RateLimiter::direct_with_clock(
        governor::Quota::per_minute(NonZeroU32::new(1).unwrap()),
        TokioClock {
            start: tokio::time::Instant::now(),
        },
    );
    assert!(state.rate_limiter.check().is_ok());
    (Arc::new(state), sent)
}

// A query the rate limiter holds goes out once the limiter has room, a
// minute later here, and waits RESPONSE_TIMEOUT for its answer from then,
// not from when it was made.
#[tokio::test(start_paused = true)]
async fn test_a_query_the_limiter_holds_waits_its_timeout_from_its_send() {
    use crate::dht::RESPONSE_TIMEOUT;

    let (dht, mut sent) = fixture_dht_limited();
    let addr = SocketAddr::from(([10, 0, 1, 1], 6881));
    let start = tokio::time::Instant::now();
    let ask = tokio::spawn({
        let dht = dht.clone();
        async move { dht.request(super::Request::Ping, addr).await }
    });
    let ping = sent.recv().await.unwrap();
    assert_eq!(ping.addr, addr);
    let held = tokio::time::Instant::now() - start;
    assert_eq!(held, Duration::from_secs(60));
    assert!(matches!(
        ask.await.unwrap(),
        Err(crate::dht::Error::ResponseTimeout(_))
    ));
    assert_eq!(tokio::time::Instant::now() - start, held + RESPONSE_TIMEOUT);
    assert_eq!(dht.get_stats().outstanding_requests, 0);
}

// A query of ours is counted as it goes out and as it ends; a node's
// query is counted by what it asks; a datagram that is no message is
// counted as ignored.
#[tokio::test(start_paused = true)]
async fn test_the_node_counts_its_queries_those_it_answers_and_what_it_ignores() {
    use crate::session_stats::dht::{DhtIgnored, DhtQuery, DhtQueryEnd, DhtSnapshot};

    let (state, mut sent) = fixture_state();
    let dht = Arc::new(state);
    assert_eq!(dht.counted(), DhtSnapshot::default());
    let addr = SocketAddr::from(([10, 0, 1, 1], 6881));

    // Our ping goes out and is never answered.
    let ask = tokio::spawn({
        let dht = dht.clone();
        async move { dht.request(super::Request::Ping, addr).await }
    });
    let ping = sent.recv().await.unwrap();
    assert!(matches!(ping.message.kind, MessageKind::PingRequest(_)));
    assert!(matches!(
        ask.await.unwrap(),
        Err(crate::dht::Error::ResponseTimeout(_))
    ));

    // A node pings us, and is answered.
    let query = dht
        .read_datagram(
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
            addr,
        )
        .unwrap();
    dht.on_received_message(query, addr).unwrap();
    let answer = sent.recv().await.unwrap();
    assert!(matches!(answer.message.kind, MessageKind::Response(_)));

    // A datagram that is no message.
    assert!(dht.read_datagram(&[b'x'; 40], addr).is_none());

    // A reply whose transaction id is not one of ours in shape.
    let reply = dht
        .read_datagram(b"d1:rd2:id20:abcdefghij0123456789e1:t3:abc1:y1:re", addr)
        .unwrap();
    assert!(matches!(
        dht.on_received_message(reply, addr),
        Err(crate::dht::Error::BadTransactionId)
    ));

    let mut counted = DhtSnapshot::default();
    counted.sent[DhtQuery::Ping as usize] = 1;
    counted.ended[DhtQuery::Ping as usize][DhtQueryEnd::Timeout as usize] = 1;
    counted.received[DhtQuery::Ping as usize] = 1;
    counted.ignored[DhtIgnored::Malformed as usize] = 2;
    assert_eq!(dht.counted(), counted);
}

fn errors_of(dht: &DhtState, id: crate::core::hash_id::Id20) -> usize {
    let table = dht.routing_table_v4.read();
    table
        .iter()
        .find(|n| n.id() == id)
        .unwrap()
        .errors_in_a_row()
}

// A node of `dht`'s IPv4 table that has answered, and the refresh that
// pings it.
fn refreshing_one_node(dht: &DhtState) -> super::Refresh {
    let id = crate::core::hash_id::Id20::new([0x21; 20]);
    let addr = SocketAddr::from(([10, 0, 1, 1], 6881));
    let mut table = dht.routing_table_v4.write();
    table.add_node(id, addr);
    assert!(table.mark_response(&id, std::time::Instant::now()));
    super::Refresh {
        id,
        addr,
        target: id,
        ping: true,
    }
}

// A refresh the rate limiter still holds when it is dropped never went
// out: the node is not failed for it.
#[tokio::test(start_paused = true)]
async fn test_a_refresh_dropped_before_it_is_sent_fails_no_table_node() {
    let (dht, mut sent) = fixture_dht_limited();
    let next = refreshing_one_node(&dht);
    {
        let mut ask = std::pin::pin!(dht.ask_refresh(true, next));
        while dht.get_stats().outstanding_requests == 0 {
            assert!(futures::poll!(ask.as_mut()).is_pending());
        }
    }
    assert_eq!(dht.get_stats().outstanding_requests, 0);
    assert!(sent.try_recv().is_err(), "sent past an empty limiter");
    assert_eq!(errors_of(&dht, next.id), 0);
}

// A refresh sent and never answered fails its node once at SHORT_TIMEOUT
// and once more at RESPONSE_TIMEOUT, both counted from the send. The node
// has answered before and its bucket holds no replacement, so it stays.
#[tokio::test(start_paused = true)]
async fn test_a_refresh_not_answered_fails_its_node_at_both_timeouts() {
    use super::SHORT_TIMEOUT;
    use crate::dht::RESPONSE_TIMEOUT;

    let (dht, mut sent) = fixture_dht();
    let next = refreshing_one_node(&dht);
    let mut ask = std::pin::pin!(dht.ask_refresh(true, next));
    let ping = loop {
        assert!(futures::poll!(ask.as_mut()).is_pending());
        if let Ok(ping) = sent.try_recv() {
            break ping;
        }
    };
    assert!(matches!(ping.message.kind, MessageKind::PingRequest(_)));
    assert_eq!(errors_of(&dht, next.id), 0);
    tokio::time::advance(SHORT_TIMEOUT).await;
    while errors_of(&dht, next.id) == 0 {
        assert!(futures::poll!(ask.as_mut()).is_pending());
    }
    assert_eq!(errors_of(&dht, next.id), 1);
    tokio::time::advance(RESPONSE_TIMEOUT - SHORT_TIMEOUT).await;
    while futures::poll!(ask.as_mut()).is_pending() {}
    assert_eq!(errors_of(&dht, next.id), 2);
    assert_eq!(dht.get_stats().outstanding_requests, 0);
}

// A refresh answered with an error fails its node once; one answered
// late, past SHORT_TIMEOUT, forgives the failure the short timeout gave.
#[tokio::test(start_paused = true)]
async fn test_a_refresh_refused_fails_once_and_a_late_answer_forgives() {
    use super::SHORT_TIMEOUT;

    let (dht, mut sent) = fixture_dht();
    let next = refreshing_one_node(&dht);
    {
        let mut ask = std::pin::pin!(dht.ask_refresh(true, next));
        let ping = loop {
            assert!(futures::poll!(ask.as_mut()).is_pending());
            if let Ok(ping) = sent.try_recv() {
                break ping;
            }
        };
        refuse(&dht, &ping);
        while futures::poll!(ask.as_mut()).is_pending() {}
    }
    assert_eq!(errors_of(&dht, next.id), 1);

    let mut ask = std::pin::pin!(dht.ask_refresh(true, next));
    let ping = loop {
        assert!(futures::poll!(ask.as_mut()).is_pending());
        if let Ok(ping) = sent.try_recv() {
            break ping;
        }
    };
    tokio::time::advance(SHORT_TIMEOUT).await;
    while errors_of(&dht, next.id) == 1 {
        assert!(futures::poll!(ask.as_mut()).is_pending());
    }
    assert_eq!(errors_of(&dht, next.id), 2);
    reply(&dht, &ping, next.id, &[], &[]);
    while futures::poll!(ask.as_mut()).is_pending() {}
    assert_eq!(errors_of(&dht, next.id), 0);
}

// A node of the table, which has answered, is the one candidate of a
// search the rate limiter holds, and the search is dropped before the
// query goes out: the table counts no failure, and the node is still one
// we can count on.
#[tokio::test(start_paused = true)]
async fn test_a_query_dropped_before_it_is_sent_fails_no_table_node() {
    let (dht, mut sent) = fixture_dht_limited();
    let id = crate::core::hash_id::Id20::new([0x21; 20]);
    let addr = SocketAddr::from(([10, 0, 1, 1], 6881));
    {
        let mut table = dht.routing_table_v4.write();
        table.add_node(id, addr);
        assert!(table.mark_response(&id, std::time::Instant::now()));
    }
    {
        let req = get_peers_search(&dht);
        let mut search = std::pin::pin!(req.search(true, std::iter::once((Some(id), addr))));
        while dht.get_stats().outstanding_requests == 0 {
            assert!(futures::poll!(search.as_mut()).is_pending());
        }
    }
    assert_eq!(dht.get_stats().outstanding_requests, 0);
    assert!(sent.try_recv().is_err(), "sent past an empty limiter");
    let table = dht.routing_table_v4.read();
    let node = table.iter().find(|n| n.id() == id).unwrap();
    assert_eq!(node.errors_in_a_row(), 0);
    assert!(node.is_confirmed(std::time::Instant::now()));
}

// A search's one candidate, a node of the table that has answered
// before, stays silent: the table counts one failure at SHORT_TIMEOUT
// and a second at RESPONSE_TIMEOUT, both from the send.
#[tokio::test(start_paused = true)]
async fn test_a_silent_search_candidate_fails_at_both_timeouts() {
    use super::SHORT_TIMEOUT;
    use crate::dht::RESPONSE_TIMEOUT;

    let (dht, mut sent) = fixture_dht();
    let id = crate::core::hash_id::Id20::new([0x21; 20]);
    let addr = SocketAddr::from(([10, 0, 1, 1], 6881));
    {
        let mut table = dht.routing_table_v4.write();
        table.add_node(id, addr);
        assert!(table.mark_response(&id, std::time::Instant::now()));
    }
    let req = get_peers_search(&dht);
    let mut search = std::pin::pin!(req.search(true, std::iter::once((Some(id), addr))));
    loop {
        assert!(futures::poll!(search.as_mut()).is_pending());
        if sent.try_recv().is_ok() {
            break;
        }
    }
    assert_eq!(errors_of(&dht, id), 0);
    tokio::time::advance(SHORT_TIMEOUT).await;
    while errors_of(&dht, id) == 0 {
        assert!(futures::poll!(search.as_mut()).is_pending());
    }
    tokio::time::advance(RESPONSE_TIMEOUT - SHORT_TIMEOUT).await;
    let outcome = loop {
        if let std::task::Poll::Ready(outcome) = futures::poll!(search.as_mut()) {
            break outcome.unwrap();
        }
    };
    assert_eq!((outcome.answered, outcome.failed), (0, 1));
    assert_eq!(errors_of(&dht, id), 2);
}

#[tokio::test(start_paused = true)]
async fn test_a_source_over_the_query_limit_is_ignored_until_its_block_lapses() {
    use crate::dht::query_limiter::{BLOCK, QUERY_LIMIT, WINDOW};
    use tokio::sync::mpsc::error::TryRecvError;

    let (dht, mut sent) = fixture_dht();
    let flooder = SocketAddr::from(([10, 0, 0, 1], 6881));
    let quiet = SocketAddr::from(([10, 0, 0, 2], 6881));
    // `from` pings us; whether it was answered.
    let mut ping = |from: SocketAddr| {
        if let Some(message) = dht.read_datagram(PING, from) {
            dht.on_received_message(message, from).unwrap();
        }
        match sent.try_recv() {
            Ok(reply) => {
                assert_eq!(reply.addr, from);
                assert!(matches!(reply.message.kind, MessageKind::Response(_)));
                true
            }
            Err(TryRecvError::Empty) => false,
            Err(e) => panic!("{e}"),
        }
    };

    // Every query short of the limit is answered; the one that reaches it,
    // and every one after, is not.
    for _ in 1..QUERY_LIMIT {
        assert!(ping(flooder));
    }
    assert!(!ping(flooder));
    assert!(!ping(flooder));

    // A source under the limit is answered meanwhile.
    assert!(ping(quiet));

    // The block outlasts the window, and the queries sent during it do
    // not lengthen it.
    tokio::time::advance(WINDOW).await;
    assert!(!ping(flooder));
    assert!(ping(quiet));
    tokio::time::advance(BLOCK - WINDOW - Duration::from_secs(1)).await;
    assert!(!ping(flooder));

    // Once it lapses the source is answered again.
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(ping(flooder));
}

const PING: &[u8] = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";

// An answer to `request`, as the bytes a node sends.
fn answer_datagram(request: &super::WorkerSendRequest) -> Vec<u8> {
    let mut buf = Vec::new();
    bprotocol::serialize_message(
        &mut buf,
        request.message.transaction_id.clone(),
        None,
        None,
        MessageKind::Response(bprotocol::Response {
            id: crate::core::hash_id::Id20::new([0x33; 20]),
            ..Default::default()
        }),
    )
    .unwrap();
    buf
}

// libtorrent dht_tracker.cpp:538-548, v2.0.15: the source is counted
// before the datagram is parsed, so datagrams that do not parse count.
#[tokio::test(start_paused = true)]
async fn test_a_source_is_counted_for_datagrams_that_do_not_parse() {
    use crate::dht::query_limiter::QUERY_LIMIT;

    let (dht, mut sent) = fixture_dht();
    let mut ping = |from: SocketAddr| {
        if let Some(message) = dht.read_datagram(PING, from) {
            dht.on_received_message(message, from).unwrap();
        }
        sent.try_recv().is_ok()
    };
    // A dictionary, but no KRPC message.
    let junk = b"d1:wi1e1:xi2e1:yi3e1:zi4ee";
    let flooder = SocketAddr::from(([10, 0, 0, 1], 6881));
    let under = SocketAddr::from(([10, 0, 0, 2], 6881));
    for _ in 1..QUERY_LIMIT {
        assert!(dht.read_datagram(junk, flooder).is_none());
    }
    for _ in 2..QUERY_LIMIT {
        assert!(dht.read_datagram(junk, under).is_none());
    }
    // The datagram that reaches the limit is not read; one short of it is.
    assert!(!ping(flooder));
    assert!(ping(under));

    // A datagram that is not shaped like a dictionary is not counted.
    let stranger = SocketAddr::from(([10, 0, 0, 3], 6881));
    for _ in 0..2 * QUERY_LIMIT {
        assert!(
            dht.read_datagram(b"not bencode, not counted", stranger)
                .is_none()
        );
        assert!(dht.read_datagram(b"d1:xi1ee", stranger).is_none());
    }
    assert!(ping(stranger));
}

// A node that answers every query we send it, as fast as our send window
// lets us ask one node, keeps being heard: its answers are counted, and
// they stay under the limit.
#[tokio::test(start_paused = true)]
async fn test_a_node_answering_our_queries_is_never_ignored() {
    use crate::dht::{
        query_limiter::{QUERY_LIMIT, WINDOW},
        send_windows::SEND_LIMIT,
    };
    use std::task::Poll;

    let (dht, mut sent) = fixture_dht();
    let node = SocketAddr::from(([10, 0, 0, 1], 6881));
    // SEND_LIMIT a window, evenly spaced, for ten windows. The spacing is
    // rounded up, so no window holds more than SEND_LIMIT.
    let per_window = u32::try_from(SEND_LIMIT).unwrap();
    assert!(per_window < QUERY_LIMIT);
    let step = WINDOW / per_window + Duration::from_nanos(1);
    for _ in 0..10 * per_window {
        let mut asked = std::pin::pin!(dht.request(super::Request::Ping, node));
        assert!(matches!(futures::poll!(asked.as_mut()), Poll::Pending));
        let request = sent.try_recv().unwrap();
        let message = dht
            .read_datagram(&answer_datagram(&request), node)
            .expect("an answer was not read");
        dht.on_received_message(message, node).unwrap();
        assert!(matches!(
            futures::poll!(asked.as_mut()),
            Poll::Ready(Ok(super::ResponseOrError::Response(_)))
        ));
        tokio::time::advance(step).await;
    }
}

// Answers `request`, a get_peers a search sent, as the node `id`, with
// `token`, naming `nodes`.
pub(super) fn answer(
    dht: &std::sync::Arc<DhtState>,
    request: &super::WorkerSendRequest,
    id: crate::core::hash_id::Id20,
    token: Option<&[u8]>,
    nodes: &[(crate::core::hash_id::Id20, std::net::SocketAddrV4)],
) {
    assert!(matches!(
        request.message.kind,
        MessageKind::GetPeersRequest(_)
    ));
    let nodes = (!nodes.is_empty()).then(|| {
        bprotocol::CompactNodeInfoOwned::new_from_iter(
            nodes.iter().map(|&(id, addr)| bprotocol::Node { id, addr }),
        )
    });
    dht.on_received_message(
        bprotocol::Message {
            transaction_id: request.message.transaction_id.clone(),
            version: None,
            ip: None,
            kind: MessageKind::Response(bprotocol::Response {
                id,
                nodes,
                token: token.map(ByteBufOwned::from),
                ..Default::default()
            }),
        },
        request.addr,
    )
    .unwrap();
}

// libtorrent `torrent::start_announcing` / `session_impl::prioritize_dht`
// and `dht_announce_interval`: a torrent searches as it starts, then every
// fifteen minutes, and every search ends in an announce.
#[tokio::test(start_paused = true)]
async fn test_peers_are_searched_and_announced_every_fifteen_minutes() {
    assert!(super::announce_slot(&Id20::new([0x22; 20])) < DHT_ANNOUNCE_INTERVAL);
    assert!(super::announce_slot(&Id20::new([0xff; 20])) < DHT_ANNOUNCE_INTERVAL);
    use crate::core::hash_id::Id20;
    use crate::dht::DHT_ANNOUNCE_INTERVAL;
    use std::collections::BTreeMap;

    let (dht, mut sent) = fixture_dht();
    for i in 0..8u8 {
        dht.routing_table_v4
            .write()
            .add_node(Id20::new([i; 20]), SocketAddr::from(([10, 0, i, 1], 6881)));
    }
    let req = std::sync::Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        Id20::new([0x22; 20]),
        Some(6881),
        tokio::sync::mpsc::unbounded_channel().0,
    ));
    let start = tokio::time::Instant::now();
    let task = req.request_peers_forever(true);

    // Requests sent at each moment, until the first past half an hour.
    let mut searched = BTreeMap::<Duration, usize>::new();
    let mut announced = BTreeMap::<Duration, usize>::new();
    loop {
        let request = sent.recv().await.unwrap();
        let at = tokio::time::Instant::now() - start;
        if at > 2 * DHT_ANNOUNCE_INTERVAL {
            break;
        }
        match &request.message.kind {
            MessageKind::GetPeersRequest(_) => {
                *searched.entry(at).or_default() += 1;
                answer(&dht, &request, Id20::new([0x33; 20]), Some(b"t"), &[]);
            }
            MessageKind::AnnouncePeer(_) => *announced.entry(at).or_default() += 1,
            kind => panic!("unexpected {kind:?}"),
        }
    }
    task.abort();

    // At start, at the torrent's slot, and an interval after that.
    let slot = super::announce_slot(&Id20::new([0x22; 20]));
    let at: BTreeMap<Duration, usize> = [Duration::ZERO, slot, slot + DHT_ANNOUNCE_INTERVAL]
        .into_iter()
        .map(|at| (at, 8))
        .collect();
    assert_eq!(searched, at);
    assert_eq!(announced, at);
}

// A table of `nodes` nodes at 10.0.i.1, each one that has answered when
// `answered`, one only heard of or restored from an earlier run when not.
pub(super) fn add_table_nodes(dht: &DhtState, nodes: u8, answered: bool) {
    use crate::core::hash_id::Id20;
    let mut table = dht.routing_table_v4.write();
    for i in 0..nodes {
        let id = Id20::new([i + 1; 20]);
        table.add_node(id, SocketAddr::from(([10, 0, i, 1], 6881)));
        if answered {
            table.mark_response(&id, std::time::Instant::now());
        }
    }
}

// A search that starts from fewer than three nodes that have answered
// also asks the routers of its family; one with three does not (libtorrent
// `traversal_algorithm::start`, `add_router_entries`, over a table that
// holds only nodes that answered). With no node at all, the routers alone
// start it, at once; three nodes that never answered do not keep it from
// them.
#[tokio::test(start_paused = true)]
async fn test_a_search_short_of_nodes_also_asks_the_routers() {
    use crate::core::hash_id::Id20;
    use std::collections::HashSet;

    let router: SocketAddr = ([192, 0, 2, 1], 6881).into();
    let router_v6: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
    let node = |i: u8| SocketAddr::from(([10, 0, i, 1], 6881));
    for (nodes, answered, routers_asked) in [
        (0, true, true),
        (2, true, true),
        (3, true, false),
        (3, false, true),
    ] {
        let (dht, mut sent) = fixture_dht();
        dht.routers.write().extend([router, router_v6]);
        add_table_nodes(&dht, nodes, answered);
        let req = std::sync::Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            Id20::new([0x22; 20]),
            Some(6881),
            tokio::sync::mpsc::unbounded_channel().0,
        ));
        let start = tokio::time::Instant::now();
        let task = req.request_peers_forever(true);
        let mut want: HashSet<SocketAddr> = (0..nodes).map(node).collect();
        if routers_asked {
            want.insert(router);
        }
        let mut asked = HashSet::new();
        while asked.len() < want.len() {
            asked.insert(sent.recv().await.unwrap().addr);
            assert_eq!(tokio::time::Instant::now(), start, "{nodes} nodes");
        }
        assert!(sent.try_recv().is_err(), "{nodes} nodes");
        assert_eq!(asked, want, "{nodes} nodes");
        assert_eq!(tokio::time::Instant::now(), start, "{nodes} nodes");
        task.abort();
    }
}

// `traversal_algorithm::start` runs for a refresh too: a walk for the
// table, a find_node from the table's nodes nearest its target, that
// finds fewer than three nodes that
// have answered also asks the routers of its family; one that finds three
// does not.
#[tokio::test(start_paused = true)]
async fn test_a_bucket_refresh_short_of_nodes_also_asks_the_routers() {
    use crate::core::hash_id::Id20;
    use std::collections::HashSet;

    let router: SocketAddr = ([192, 0, 2, 1], 6881).into();
    let router_v6: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
    let node = |i: u8| SocketAddr::from(([10, 0, i, 1], 6881));
    for (nodes, answered, routers_asked) in [
        (0, true, true),
        (2, true, true),
        (3, true, false),
        (3, false, true),
    ] {
        let (dht, mut sent) = fixture_dht();
        dht.routers.write().extend([router, router_v6]);
        add_table_nodes(&dht, nodes, answered);
        let mut refresh = std::pin::pin!(super::RecursiveRequest::find_node_for_routing_table(
            dht.clone(),
            Id20::new([0x22; 20]),
            true,
            std::iter::empty(),
        ));
        assert!(futures::poll!(refresh.as_mut()).is_pending());

        let mut want: HashSet<SocketAddr> = (0..nodes).map(node).collect();
        if routers_asked {
            want.insert(router);
        }
        let mut asked = HashSet::new();
        while let Ok(request) = sent.try_recv() {
            assert!(matches!(
                request.message.kind,
                MessageKind::FindNodeRequest(_)
            ));
            assert!(asked.insert(request.addr), "{} asked twice", request.addr);
        }
        assert_eq!(asked, want, "{nodes} nodes");
    }
}

// libtorrent routing_table.cpp:816-822, 1085-1099: a node a search is
// told of takes a place in the table once it answers, never on being
// asked; one that never answers never enters it.
#[tokio::test(start_paused = true)]
async fn test_a_node_enters_the_table_only_once_it_answers() {
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;
    use std::task::Poll;

    let (dht, mut sent) = fixture_dht();
    let answers = (
        Id20::new([0x21; 20]),
        SocketAddr::from(([10, 0, 1, 1], 6881)),
    );
    let silent = (
        Id20::new([0x23; 20]),
        SocketAddr::from(([10, 0, 2, 1], 6881)),
    );
    let in_table = |id: Id20| dht.routing_table_v4.read().iter().any(|n| n.id() == id);
    let target = Id20::new([0x22; 20]);
    let req = super::RecursiveRequest {
        info_hash: target,
        request: super::Request::FindNode(target),
        dht: dht.clone(),
        peer_tx: tokio::sync::mpsc::unbounded_channel().0,
        callbacks: super::RecursiveRequestCallbacksFindNodes {},
    };
    let mut refresh = std::pin::pin!(req.search(
        true,
        [(Some(answers.0), answers.1), (Some(silent.0), silent.1)].into_iter(),
    ));
    assert!(futures::poll!(refresh.as_mut()).is_pending());
    let mut asked = Vec::new();
    while let Ok(request) = sent.try_recv() {
        asked.push(request);
    }
    assert_eq!(asked.len(), 2);
    assert!(
        !in_table(answers.0) && !in_table(silent.0),
        "asked is not answered"
    );

    let request = asked.iter().find(|r| r.addr == answers.1).unwrap();
    dht.on_received_message(
        bprotocol::Message {
            transaction_id: request.message.transaction_id.clone(),
            version: None,
            ip: None,
            kind: MessageKind::Response(bprotocol::Response {
                id: answers.0,
                ..Default::default()
            }),
        },
        answers.1,
    )
    .unwrap();
    let outcome = loop {
        if let Poll::Ready(outcome) = futures::poll!(refresh.as_mut()) {
            break outcome;
        }
        assert!(in_table(answers.0) || sent.try_recv().is_err());
        tokio::time::advance(RESPONSE_TIMEOUT).await;
    };
    let _ = outcome;
    assert!(
        in_table(answers.0),
        "the node that answered is in the table"
    );
    assert!(!in_table(silent.0), "the silent node entered the table");
}

// A router name's addresses join the routers (libtorrent
// `dht_tracker::add_router_node`), and the next search for our own id
// asks every router of the family, the ones the name gave among them,
// each once (`traversal_algorithm::start` and `add_router_entries`).
#[tokio::test(start_paused = true)]
async fn test_a_bootstrap_from_the_routers_asks_each_router_once() {
    use crate::core::net::{BoxFuture, Resolver};
    use std::sync::Arc;

    struct Names(Vec<SocketAddr>);

    impl Resolver for Names {
        fn resolve<'a>(
            &'a self,
            host: &'a str,
            _port: u16,
        ) -> BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
            Box::pin(async move {
                assert_eq!(host, "router.invalid");
                Ok(crate::core::net::Resolved::Addresses(self.0.clone()))
            })
        }
    }

    // Known from earlier names: one this name gives again, one it does
    // not.
    let known: SocketAddr = ([192, 0, 2, 1], 6881).into();
    let other: SocketAddr = ([192, 0, 2, 3], 6881).into();
    let new: SocketAddr = ([192, 0, 2, 2], 6881).into();
    let (dht, mut sent) = fixture_dht();
    dht.routers.write().extend([known, other]);
    let socket = crate::test_support::loopback_udp().await;
    let worker = super::DhtWorker {
        socket,
        resolver: Arc::new(Names(vec![new, known])),
        dht: dht.clone(),
    };
    worker
        .resolve_router(
            "router.invalid:6881",
            &std::sync::atomic::AtomicBool::new(false),
            &std::sync::atomic::AtomicUsize::new(1),
        )
        .await;
    let mut refresh = std::pin::pin!(dht.clone().refresh_self(true));
    assert!(futures::poll!(refresh.as_mut()).is_pending());

    let mut asked = Vec::new();
    while let Ok(request) = sent.try_recv() {
        assert!(matches!(
            request.message.kind,
            MessageKind::FindNodeRequest(_)
        ));
        asked.push(request.addr);
    }
    asked.sort();
    assert_eq!(asked, vec![known, new, other]);
}

// A search due while the routing table is empty and no router is known
// starts as the first node enters it, not at a later look at the table.
#[tokio::test(start_paused = true)]
async fn test_a_search_starts_as_the_first_node_enters_the_table() {
    use crate::core::hash_id::Id20;

    let (dht, mut sent) = fixture_dht();
    let req = std::sync::Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        Id20::new([0x22; 20]),
        Some(6881),
        tokio::sync::mpsc::unbounded_channel().0,
    ));
    let task = req.request_peers_forever(true);
    tokio::time::advance(Duration::from_millis(10_500)).await;
    assert!(sent.try_recv().is_err());

    let entered = tokio::time::Instant::now();
    dht.routing_table_v4
        .write()
        .add_node(Id20::new([1; 20]), SocketAddr::from(([10, 0, 1, 1], 6881)));
    let request = sent.recv().await.unwrap();
    assert!(matches!(
        request.message.kind,
        MessageKind::GetPeersRequest(_)
    ));
    assert_eq!(tokio::time::Instant::now(), entered);
    task.abort();
}

// libtorrent `find_data::done` / `announce_fun`: nothing is announced while
// the search runs; at its end, the closest nodes that had answered with
// a token by then, up to k, are announced to, with their token.
#[tokio::test(start_paused = true)]
async fn test_a_search_announces_to_the_closest_nodes_that_answered_with_a_token_when_it_ends() {
    use crate::core::hash_id::Id20;
    use std::{collections::BTreeMap, net::IpAddr, task::Poll};

    let (dht, mut sent) = fixture_dht();
    let info_hash = Id20::new([0x22; 20]);
    // Node i is i + 1 from the info-hash, at 10.0.i.1.
    let node = |i: u8| {
        let mut id = [0x22; 20];
        id[19] ^= i + 1;
        (Id20::new(id), SocketAddr::from(([10, 0, i, 1], 6881)))
    };
    let index = |addr: SocketAddr| match addr.ip() {
        IpAddr::V4(a) => a.octets()[2],
        IpAddr::V6(_) => unreachable!(),
    };
    // Nodes 1 and 3 answer without a token.
    let token = |i: u8| (i != 1 && i != 3).then_some([i]);
    let req = super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        Some(6881),
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let mut search = std::pin::pin!(req.search(
        true,
        (0..14).map(|i| {
            let (id, addr) = node(i);
            (Some(id), addr)
        })
    ));

    let mut asked = Vec::new();
    loop {
        if let Poll::Ready(outcome) = futures::poll!(search.as_mut()) {
            outcome.unwrap();
            break;
        }
        // Only get_peers go out while it runs.
        while let Ok(request) = sent.try_recv() {
            let i = index(request.addr);
            asked.push(i);
            answer(
                &dht,
                &request,
                node(i).0,
                token(i).as_ref().map(|t| &t[..]),
                &[],
            );
        }
    }
    asked.sort();
    // The search ends as the eight closest have answered, before it
    // sends to 10 and 11, which it would have asked next.
    assert_eq!(asked, (0..10).collect::<Vec<_>>());

    let mut announced = BTreeMap::new();
    while let Ok(request) = sent.try_recv() {
        let MessageKind::AnnouncePeer(a) = request.message.kind else {
            panic!("not an announce: {:?}", request.message.kind);
        };
        assert_eq!((a.info_hash, a.port), (info_hash, 6881));
        announced.insert(index(request.addr), a.token.as_ref().to_vec());
    }
    // It announces as it ends, to the token holders among the nodes that
    // had answered by then: 8 and 9 answer after the eight closest have,
    // and the search no longer waits for them (libtorrent
    // `find_data::done`, find_data.cpp:139-194, v2.0.15).
    assert_eq!(
        announced,
        [0, 2, 4, 5, 6, 7]
            .into_iter()
            .map(|i| (i, vec![i]))
            .collect()
    );
}

// A remote node naming a local address, or anyone naming an address
// nothing can be sent to, is not followed; a local node naming a local
// address is (libtorrent `aux::is_global`, `aux::is_local`, and the rule
// `ut_pex.cpp` applies to PEX peers).
#[tokio::test(start_paused = true)]
async fn test_a_search_asks_no_node_named_at_an_unreachable_or_foreign_local_address() {
    use crate::core::hash_id::Id20;
    use std::{
        collections::HashSet,
        net::{Ipv4Addr, SocketAddrV4},
        task::Poll,
    };

    let (dht, mut sent) = fixture_dht();
    let id = |n: u8| Id20::new([n; 20]);
    let v4 = |ip: [u8; 4], port: u16| SocketAddrV4::new(Ipv4Addr::from(ip), port);
    let public = SocketAddr::from(([5, 6, 7, 8], 6881));
    let local = SocketAddr::from(([10, 2, 0, 1], 6881));
    let named_by_public: Vec<_> = [
        v4([127, 0, 0, 1], 6881),
        v4([10, 1, 0, 1], 6881),
        v4([192, 168, 1, 1], 6881),
        v4([169, 254, 1, 1], 6881),
        v4([100, 64, 0, 1], 6881),
        v4([224, 0, 0, 1], 6881),
        v4([0, 1, 2, 3], 6881),
        v4([240, 0, 0, 1], 6881),
        v4([255, 255, 255, 255], 6881),
        v4([5, 6, 9, 9], 0),
        v4([5, 6, 10, 10], 6881),
    ]
    .into_iter()
    .zip(0x40..)
    .map(|(addr, n)| (id(n), addr))
    .collect();
    let named_by_local = [(id(0x60), v4([10, 3, 0, 1], 6881))];

    let req = get_peers_search(&dht);
    let mut search = std::pin::pin!(req.search(
        true,
        [(Some(id(0x30)), public), (Some(id(0x31)), local)].into_iter()
    ));
    let mut asked = HashSet::new();
    loop {
        if let Poll::Ready(outcome) = futures::poll!(search.as_mut()) {
            outcome.unwrap();
            break;
        }
        while let Ok(request) = sent.try_recv() {
            asked.insert(request.addr);
            let nodes: &[_] = match request.addr {
                a if a == public => &named_by_public,
                a if a == local => &named_by_local,
                _ => &[],
            };
            answer(&dht, &request, id(0x50), None, nodes);
        }
    }

    assert_eq!(
        asked,
        HashSet::from([
            public,
            local,
            SocketAddr::from(([5, 6, 10, 10], 6881)),
            SocketAddr::from(([10, 3, 0, 1], 6881)),
        ])
    );
}

// A DHT whose shared socket closes under it, as a session's uTP socket
// does when the session closes, ends quietly: no error is logged.
#[tokio::test]
async fn a_dht_whose_socket_closes_ends_without_an_error() {
    use crate::core::net::BoxFuture;

    // Sends go nowhere; a read waits until `closed` is told, then says
    // the socket has closed.
    struct Closing {
        closed: tokio::sync::Notify,
    }
    impl DatagramSocket for Closing {
        fn bind_addr(&self) -> SocketAddr {
            (Ipv4Addr::LOCALHOST, 6881).into()
        }
        fn recv_from<'a>(
            &'a self,
            _: &'a mut [u8],
        ) -> BoxFuture<'a, std::io::Result<(usize, SocketAddr)>> {
            Box::pin(async move {
                self.closed.notified().await;
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "the uTP socket the DHT shares has closed",
                ))
            })
        }
        fn send_to<'a>(
            &'a self,
            buf: &'a [u8],
            _: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<usize>> {
            Box::pin(async move { Ok(buf.len()) })
        }
        fn poll_send_to(
            &self,
            _: &mut std::task::Context<'_>,
            buf: &[u8],
            _: SocketAddr,
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Ok(buf.len()))
        }
    }

    #[derive(Clone, Default)]
    struct Lines(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Lines {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let lines = Lines::default();
    let written = lines.clone();
    let _capture = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new("weaver_torrent=error"))
            .with_ansi(false)
            .with_writer(move || written.clone())
            .finish(),
    );

    let socket = Arc::new(Closing {
        closed: tokio::sync::Notify::new(),
    });
    let tasks = tokio_util::task::TaskTracker::new();
    let dht = DhtState::with_config(DhtConfig {
        bootstrap_addrs: Some(Vec::new()),
        socket: Some(socket.clone()),
        tasks: Some(tasks.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    socket.closed.notify_one();
    tasks.close();
    tasks.wait().await;

    assert!(!dht.cancellation_token().is_cancelled());
    let logged = String::from_utf8(lines.0.lock().unwrap().clone()).unwrap();
    assert_eq!(logged, "");
}

// Reply to `request` as the node `id`, naming `nodes` and `values`.
pub(super) fn reply(
    dht: &std::sync::Arc<DhtState>,
    request: &super::WorkerSendRequest,
    id: crate::core::hash_id::Id20,
    nodes: &[(crate::core::hash_id::Id20, std::net::SocketAddrV4)],
    values: &[SocketAddr],
) {
    let nodes = (!nodes.is_empty()).then(|| {
        bprotocol::CompactNodeInfoOwned::new_from_iter(
            nodes.iter().map(|&(id, addr)| bprotocol::Node { id, addr }),
        )
    });
    let values = (!values.is_empty()).then(|| values.iter().map(|&v| v.into()).collect());
    dht.on_received_message(
        bprotocol::Message {
            transaction_id: request.message.transaction_id.clone(),
            version: None,
            ip: None,
            kind: MessageKind::Response(bprotocol::Response {
                id,
                nodes,
                values,
                ..Default::default()
            }),
        },
        request.addr,
    )
    .unwrap();
}

// Reply to `request` with a KRPC error.
pub(super) fn refuse(dht: &std::sync::Arc<DhtState>, request: &super::WorkerSendRequest) {
    dht.on_received_message(
        bprotocol::Message {
            transaction_id: request.message.transaction_id.clone(),
            version: None,
            ip: None,
            kind: MessageKind::Error(bprotocol::ErrorDescription {
                code: 202,
                description: ByteBufOwned::from(&b"Server Error"[..]),
            }),
        },
        request.addr,
    )
    .unwrap();
}
