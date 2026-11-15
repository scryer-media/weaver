// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{net::SocketAddr, time::Duration};

use crate::bencode::ByteBufOwned;
use std::sync::Arc;

use super::tests::{
    add_table_nodes, answer, fixture_dht, get_peers_search, reply, unstarted_fixture_dht,
};
use crate::dht::bprotocol::MessageKind;

// The two routers of `cold_start`, and the node and peer the second one
// leads to.
const ROUTER_A: ([u8; 4], u16) = ([10, 200, 0, 1], 6881);
const ROUTER_B: ([u8; 4], u16) = ([10, 200, 0, 2], 6881);
const NODE_N: ([u8; 4], u16) = ([10, 0, 1, 1], 6881);
const PEER_P: ([u8; 4], u16) = ([10, 9, 9, 9], 51413);

// A fresh DHT's start on a fake network. Its bootstrap names are
// `a.invalid` and `b.invalid`: A's resolves at once, B's `b_late` in, on
// the paused clock. Router A never answers, or, when
// `a_answers_first_only`, answers the first query it gets and no other.
// Router B answers every query: a get_peers naming the node N, which
// answers a get_peers with the peer P. The self-lookup and one torrent's
// search run beside the bootstrap, as a session starts them together.
// Returns the queries sent until P is found, each with when and what,
// and when P was found.
async fn cold_start(
    b_late: Duration,
    a_answers_first_only: bool,
) -> (Vec<(Duration, SocketAddr, &'static str)>, Duration) {
    use crate::core::hash_id::Id20;
    use crate::core::net::{BoxFuture, Resolved, Resolver};
    use std::sync::Arc;

    struct Names {
        start: tokio::time::Instant,
        b_late: Duration,
    }

    impl Resolver for Names {
        fn resolve<'a>(
            &'a self,
            host: &'a str,
            _port: u16,
        ) -> BoxFuture<'a, std::io::Result<Resolved>> {
            Box::pin(async move {
                match host {
                    "a.invalid" => Ok(Resolved::Addresses(vec![ROUTER_A.into()])),
                    "b.invalid" => {
                        tokio::time::sleep_until(self.start + self.b_late).await;
                        Ok(Resolved::Addresses(vec![ROUTER_B.into()]))
                    }
                    host => panic!("unexpected name {host}"),
                }
            })
        }
    }

    let (a, b, n) = (
        SocketAddr::from(ROUTER_A),
        SocketAddr::from(ROUTER_B),
        SocketAddr::from(NODE_N),
    );
    let peer = SocketAddr::from(PEER_P);
    let start = tokio::time::Instant::now();
    let (dht, mut sent) = unstarted_fixture_dht();
    let worker = Arc::new(super::DhtWorker {
        socket: crate::test_support::loopback_udp().await,
        resolver: Arc::new(Names { start, b_late }),
        dht: dht.clone(),
    });
    let bootstrap = tokio::spawn({
        let worker = worker.clone();
        async move {
            worker
                .bootstrap(&["a.invalid:6881".into(), "b.invalid:6881".into()])
                .await
        }
    });
    // The torrent's search is started ahead of the self-lookup: the order
    // its queries go out in is the DHT's, not the order the two began in.
    let (peer_tx, mut peers) = tokio::sync::mpsc::unbounded_channel();
    let search = Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        Id20::new([0x22; 20]),
        None,
        peer_tx,
    ))
    .request_peers_forever(true);
    let refresh = tokio::spawn(dht.clone().refresh_self(true));

    let mut queries = Vec::new();
    let mut a_answered = false;
    let found_at = loop {
        let request = tokio::select! {
            biased;
            found = peers.recv() => {
                assert_eq!(found, Some(peer));
                break tokio::time::Instant::now() - start;
            }
            request = sent.recv() => request.unwrap(),
        };
        let kind = match request.message.kind {
            MessageKind::FindNodeRequest(_) => "find_node",
            MessageKind::GetPeersRequest(_) => "get_peers",
            _ => "other",
        };
        queries.push((tokio::time::Instant::now() - start, request.addr, kind));
        match (request.addr, kind) {
            (addr, _) if addr == a => {
                if a_answers_first_only && !a_answered {
                    a_answered = true;
                    reply(&dht, &request, Id20::new([0xa0; 20]), &[], &[]);
                }
            }
            (addr, "find_node") if addr == b => {
                reply(&dht, &request, Id20::new([0xb0; 20]), &[], &[]);
            }
            (addr, "get_peers") if addr == b => {
                let named = (
                    Id20::new([0x23; 20]),
                    std::net::SocketAddrV4::new(NODE_N.0.into(), NODE_N.1),
                );
                reply(&dht, &request, Id20::new([0xb0; 20]), &[named], &[]);
            }
            (addr, "get_peers") if addr == n => {
                reply(&dht, &request, Id20::new([0x23; 20]), &[], &[peer]);
            }
            _ => {}
        }
    };
    search.abort();
    refresh.abort();
    bootstrap.abort();
    (queries, found_at)
}

// The queries in `queries` sent to `addr`, by kind, in order.
fn kinds_to(
    queries: &[(Duration, SocketAddr, &'static str)],
    addr: SocketAddr,
) -> Vec<&'static str> {
    queries
        .iter()
        .filter(|q| q.1 == addr)
        .map(|q| q.2)
        .collect()
}

// A router name that resolves after another: nothing is asked until
// both have resolved (libtorrent `session_impl::start_dht` waits for
// every router lookup), and then the bootstrap and the torrent's first
// search each ask both routers, the bootstrap first. Router A never
// answers; the torrent's search reaches P through router B at once,
// with no timeout and no second search.
#[tokio::test(start_paused = true)]
async fn test_a_router_resolving_late_joins_the_first_search() {
    let late = Duration::from_millis(3_100);
    let (queries, found_at) = cold_start(late, false).await;
    let (a, b) = (SocketAddr::from(ROUTER_A), SocketAddr::from(ROUTER_B));

    assert_eq!(
        queries[0].0, late,
        "asked before every router name was looked up"
    );
    let first: Vec<_> = queries[..4].iter().map(|q| (q.1, q.2)).collect();
    let mut bootstrap: Vec<_> = first[..2].to_vec();
    bootstrap.sort();
    let mut search: Vec<_> = first[2..].to_vec();
    search.sort();
    assert_eq!(bootstrap, vec![(a, "find_node"), (b, "find_node")]);
    assert_eq!(search, vec![(a, "get_peers"), (b, "get_peers")]);
    assert_eq!(found_at, late);
    assert_eq!(kinds_to(&queries, a), vec!["find_node", "get_peers"]);
}

// Two queries reaching one router at once, of which the router answers
// only the first: the bootstrap's goes first, every time, so it is the
// one answered (libtorrent's `start_dht` sends the bootstrap before any
// torrent's search). The torrent's search, unanswered by that router,
// reaches P through the other router at once.
#[tokio::test(start_paused = true)]
async fn test_the_bootstrap_reaches_a_router_before_a_torrents_search() {
    let (queries, found_at) = cold_start(Duration::ZERO, true).await;
    let (a, b) = (SocketAddr::from(ROUTER_A), SocketAddr::from(ROUTER_B));

    assert_eq!(kinds_to(&queries, a), vec!["find_node", "get_peers"]);
    assert_eq!(kinds_to(&queries, b)[..2], ["find_node", "get_peers"]);
    let first_search = queries.iter().position(|q| q.2 == "get_peers").unwrap();
    assert!(
        queries[..first_search]
            .iter()
            .filter(|q| q.2 == "find_node")
            .count()
            == 2,
        "the torrent searched before the bootstrap asked both routers"
    );
    assert_eq!(found_at, Duration::ZERO);
}

// The most datagrams sent at `times` (in order) inside any WINDOW.
pub(super) fn most_in_any_window(times: &[tokio::time::Instant]) -> usize {
    use crate::dht::query_limiter::WINDOW;
    let mut first = 0;
    let mut most = 0;
    for (last, &at) in times.iter().enumerate() {
        while times[first] + WINDOW <= at {
            first += 1;
        }
        most = most.max(last + 1 - first);
    }
    most
}

// `torrents` torrents added at once on a cold table of two nodes that
// have answered, so every search also asks the two routers: four
// addresses every search shares. Each answers every get_peers at once,
// with a token, naming no node. Returns when each address was sent a
// datagram, up to the time every torrent's first search has announced.
async fn shared_nodes_campaign(
    torrents: u32,
) -> std::collections::HashMap<std::net::IpAddr, Vec<tokio::time::Instant>> {
    use crate::core::hash_id::Id20;
    use std::collections::{HashMap, HashSet};
    use std::net::IpAddr;

    let (dht, mut sent) = fixture_dht();
    dht.routers.write().extend([
        SocketAddr::from(([192, 0, 2, 1], 6881)),
        SocketAddr::from(([192, 0, 2, 2], 6881)),
    ]);
    add_table_nodes(&dht, 2, true);
    // The id each address answers with: a table node's own.
    let id_of = |addr: SocketAddr| match addr.ip() {
        IpAddr::V4(a) if a.octets()[0] == 10 => Id20::new([a.octets()[2] + 1; 20]),
        _ => Id20::new([0x77; 20]),
    };
    let tasks: Vec<_> = (0..torrents)
        .map(|i| {
            let mut hash = [0x55; 20];
            hash[..4].copy_from_slice(&i.to_be_bytes());
            Arc::new(super::RecursiveRequest::get_peers(
                dht.clone(),
                Id20::new(hash),
                Some(6881),
                tokio::sync::mpsc::unbounded_channel().0,
            ))
            .request_peers_forever(true)
        })
        .collect();
    let mut sends = HashMap::<IpAddr, Vec<tokio::time::Instant>>::new();
    let mut announced = HashSet::new();
    while announced.len() < torrents as usize {
        let request = sent.recv().await.unwrap();
        let now = tokio::time::Instant::now();
        sends.entry(request.addr.ip()).or_default().push(now);
        match &request.message.kind {
            MessageKind::GetPeersRequest(_) => {
                answer(&dht, &request, id_of(request.addr), Some(b"t"), &[]);
            }
            MessageKind::AnnouncePeer(a) => {
                announced.insert(a.info_hash);
            }
            kind => panic!("unexpected {kind:?}"),
        }
    }
    for task in tasks {
        task.abort();
    }
    sends
}

// However many torrents search at once from the same few nodes and
// routers, none of them is sent the datagrams in any 10 seconds at which
// a libtorrent node ignores us (dos_blocker.cpp:52-99, v2.0.15), and
// every torrent's first search still ends.
#[tokio::test(start_paused = true)]
async fn test_torrents_searching_from_the_same_nodes_never_flood_one() {
    use crate::dht::{query_limiter::QUERY_LIMIT, send_windows::SEND_LIMIT};
    for torrents in [100, 1000] {
        let sends = shared_nodes_campaign(torrents).await;
        assert_eq!(sends.len(), 4, "{torrents} torrents");
        for (ip, times) in sends {
            let most = most_in_any_window(&times);
            assert!(
                most <= SEND_LIMIT && most < QUERY_LIMIT as usize,
                "{torrents} torrents: {ip} was sent {most} in one window"
            );
        }
    }
}

// Searches whose nodes differ are not held back by a node whose window
// is full; a search that wants that node asks its others at once, and
// that one as its window opens.
#[tokio::test(start_paused = true)]
async fn test_a_full_window_holds_back_only_its_own_address() {
    use crate::core::hash_id::Id20;
    use crate::dht::{query_limiter::WINDOW, send_windows::SEND_LIMIT};
    use futures::{FutureExt, StreamExt};

    let (dht, mut sent) = fixture_dht();
    let full = SocketAddr::from(([10, 9, 9, 9], 6881));
    for _ in 0..SEND_LIMIT {
        dht.send_guard.record(full.ip());
    }
    let start = tokio::time::Instant::now();
    let nodes = |search: u8, with_full: bool| {
        (0..8u8)
            .map(move |i| {
                let addr = if with_full && i == 0 {
                    full
                } else {
                    SocketAddr::from(([10, 1, search, i], 6881))
                };
                (Some(Id20::new([search * 8 + i + 1; 20])), addr)
            })
            .collect::<Vec<_>>()
    };
    let reqs = [get_peers_search(&dht), get_peers_search(&dht)];
    let mut searches = [(0u8, false), (1, true)]
        .into_iter()
        .zip(&reqs)
        .map(|((search, with_full), req)| {
            req.search(true, nodes(search, with_full).into_iter())
                .map(move |outcome| (search, outcome))
        })
        .collect::<futures::stream::FuturesUnordered<_>>();
    let mut asked = Vec::new();
    let mut ended = Vec::new();
    loop {
        tokio::select! {
            done = searches.next() => match done {
                Some((search, outcome)) => {
                    assert_eq!(outcome.unwrap().answered, 8);
                    ended.push((search, tokio::time::Instant::now() - start));
                }
                None => break,
            },
            request = sent.recv() => {
                let request = request.unwrap();
                asked.push((request.addr, tokio::time::Instant::now() - start));
                let id = match &request.message.kind {
                    MessageKind::GetPeersRequest(_) if request.addr == full => Id20::new([9; 20]),
                    MessageKind::GetPeersRequest(_) => match request.addr.ip() {
                        std::net::IpAddr::V4(a) => Id20::new([a.octets()[2] * 8 + a.octets()[3] + 1; 20]),
                        std::net::IpAddr::V6(_) => unreachable!(),
                    },
                    kind => panic!("unexpected {kind:?}"),
                };
                answer(&dht, &request, id, None, &[]);
            }
        }
    }
    // Every other node is asked at once; the full one as its window opens.
    assert_eq!(asked.len(), 16);
    for (addr, at) in &asked {
        let want = if *addr == full {
            WINDOW
        } else {
            Duration::ZERO
        };
        assert_eq!(*at, want, "{addr}");
    }
    assert_eq!(asked.last().unwrap().0, full);
    ended.sort();
    assert_eq!(ended, vec![(0, Duration::ZERO), (1, WINDOW)]);
}

// A node whose window is full, farther than the k closest that answer,
// is never asked: the search ends once they have, and the node leaves
// its address's line.
#[tokio::test(start_paused = true)]
async fn test_a_search_ends_without_waiting_for_a_deferred_node_it_no_longer_needs() {
    use crate::core::hash_id::Id20;
    use crate::dht::send_windows::SEND_LIMIT;
    use std::task::Poll;

    let (dht, mut sent) = fixture_dht();
    // Node i is i + 1 from the info-hash; the farthest, 8, is full.
    let node = |i: u8| {
        let mut id = [0x22; 20];
        id[19] ^= i + 1;
        (Id20::new(id), SocketAddr::from(([10, 0, i, 1], 6881)))
    };
    let full = node(8).1;
    for _ in 0..SEND_LIMIT {
        dht.send_guard.record(full.ip());
    }
    let req = get_peers_search(&dht);
    let start = tokio::time::Instant::now();
    let mut search = std::pin::pin!(req.search(
        true,
        (0..9).map(|i| {
            let (id, addr) = node(i);
            (Some(id), addr)
        })
    ));
    let outcome = loop {
        if let Poll::Ready(outcome) = futures::poll!(search.as_mut()) {
            break outcome.unwrap();
        }
        let mut answered = false;
        while let Ok(request) = sent.try_recv() {
            assert_ne!(request.addr, full, "the deferred node was asked");
            let i = match request.addr.ip() {
                std::net::IpAddr::V4(a) => a.octets()[2],
                std::net::IpAddr::V6(_) => unreachable!(),
            };
            answer(&dht, &request, node(i).0, None, &[]);
            answered = true;
        }
        assert!(answered, "the search waited with nothing to answer");
    };
    assert_eq!(outcome.answered, 8);
    assert_eq!(tokio::time::Instant::now(), start);
    assert_eq!(dht.send_guard.windows.lock().lined_up(full.ip()), 0);
}

// Node i of a walk test: i + 1 from the target [0x22; 20], at 10.0.i.1.
pub(super) fn walk_node(i: u8) -> (crate::core::hash_id::Id20, SocketAddr) {
    let mut id = [0x22; 20];
    id[19] ^= i + 1;
    (
        crate::core::hash_id::Id20::new(id),
        SocketAddr::from(([10, 0, i, 1], 6881)),
    )
}

pub(super) fn walk_index(addr: SocketAddr) -> u8 {
    match addr.ip() {
        std::net::IpAddr::V4(a) => a.octets()[2],
        std::net::IpAddr::V6(_) => unreachable!(),
    }
}

// What `drive` saw sent: to which node, whether it was an announce, and
// when, from the start.
type Sent = Vec<(u8, bool, Duration)>;

// Runs `search` to its end, answering each query `delay` says to,
// through `respond`, that long after it was sent; `delay` None leaves it
// unanswered. Returns the search's result and every datagram sent.
pub(super) async fn drive<T: Send + 'static>(
    sent: &mut tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>,
    mut search: tokio::task::JoinHandle<T>,
    delay: impl Fn(u8) -> Option<Duration>,
    respond: impl Fn(&super::WorkerSendRequest),
) -> (T, Sent) {
    use std::collections::BTreeMap;
    let start = tokio::time::Instant::now();
    let mut log = Vec::new();
    let mut later = BTreeMap::new();
    let mut seq = 0u64;
    loop {
        let due = later.keys().next().map(|&(at, _)| at);
        tokio::select! {
            biased;
            () = super::sleep_until_some(due) => {
                let (_, request) = later.pop_first().unwrap();
                respond(&request);
            }
            request = sent.recv() => {
                let request = request.unwrap();
                let i = walk_index(request.addr);
                let announce = matches!(request.message.kind, MessageKind::AnnouncePeer(_));
                let now = tokio::time::Instant::now();
                log.push((i, announce, now - start));
                match delay(i) {
                    Some(_) if announce => {}
                    Some(d) if d.is_zero() => respond(&request),
                    Some(d) => {
                        seq += 1;
                        later.insert((now + d, seq), request);
                    }
                    None => {}
                }
            }
            done = &mut search => return (done.unwrap(), log),
        }
    }
}

// The queries of `log` sent at `at`, by node.
pub(super) fn queried_at(log: &Sent, at: Duration) -> Vec<u8> {
    let mut nodes: Vec<_> = log
        .iter()
        .filter(|&&(_, announce, when)| !announce && when == at)
        .map(|&(i, _, _)| i)
        .collect();
    nodes.sort();
    nodes
}

// The five closest nodes never answer. A query unanswered SHORT_TIMEOUT
// after it was sent lets the walk ask one more node, so the live nodes
// are asked, and their peers found, at SHORT_TIMEOUT, not as the five
// run out RESPONSE_TIMEOUT. The walk still waits for the five, closer
// than every node that answered, until they fail (libtorrent
// traversal_algorithm.cpp:353-408, 492-580, v2.0.15). Each node is asked
// once: 13 queries, as many as without the short timeout.
#[tokio::test(start_paused = true)]
async fn test_a_walk_whose_five_closest_are_dead_reaches_live_nodes_after_the_short_timeout() {
    use super::{SHORT_TIMEOUT, SearchOutcome};
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;

    let (dht, mut sent) = fixture_dht();
    let peer = SocketAddr::from(([10, 9, 9, 9], 51413));
    let (peer_tx, mut peers) = tokio::sync::mpsc::unbounded_channel();
    let req = super::RecursiveRequest::get_peers(dht.clone(), Id20::new([0x22; 20]), None, peer_tx);
    let start = tokio::time::Instant::now();
    let search = tokio::spawn(async move {
        let outcome = req
            .search(
                true,
                (0..13).map(|i| {
                    let (id, addr) = walk_node(i);
                    (Some(id), addr)
                }),
            )
            .await;
        (outcome, tokio::time::Instant::now())
    });
    let ((outcome, ended), log) = drive(
        &mut sent,
        search,
        |i| (i >= 5).then_some(Duration::ZERO),
        |request| {
            reply(
                &dht,
                request,
                walk_node(walk_index(request.addr)).0,
                &[],
                &[peer],
            )
        },
    )
    .await;

    assert_eq!(queried_at(&log, Duration::ZERO), (0..5).collect::<Vec<_>>());
    assert_eq!(queried_at(&log, SHORT_TIMEOUT), (5..13).collect::<Vec<_>>());
    assert_eq!(log.len(), 13);
    assert_eq!(peers.try_recv().unwrap(), peer);
    assert_eq!(ended - start, RESPONSE_TIMEOUT);
    assert_eq!(
        outcome.unwrap(),
        SearchOutcome {
            answered: 8,
            failed: 5,
            nodes: 8
        }
    );
    assert_eq!(dht.get_stats().outstanding_requests, 0);
}

// The eight closest nodes answer 2 s after they are asked, the twelve
// farther ones at once. At SHORT_TIMEOUT the five asked first have not
// answered, so the walk may keep five more in flight: it asks the other
// three slow ones and farther fast ones, a fast one again as each
// answers, until eight have answered: nine fast ones in all. It ends as
// the slow ones, the closest, have all answered. 17 queries: without
// the short timeout the walk asked the slow ones five at a time and
// sent 12, the 5 more being farther nodes asked in the room the five
// late queries opened; a node is never asked twice.
#[tokio::test(start_paused = true)]
async fn test_a_walk_asks_farther_nodes_only_in_the_room_its_late_queries_open() {
    use super::{SHORT_TIMEOUT, SearchOutcome};

    let (dht, mut sent) = fixture_dht();
    let slow = Duration::from_secs(2);
    let req = get_peers_search(&dht);
    let start = tokio::time::Instant::now();
    let search = tokio::spawn(async move {
        let outcome = req
            .search(
                true,
                (0..20).map(|i| {
                    let (id, addr) = walk_node(i);
                    (Some(id), addr)
                }),
            )
            .await;
        (outcome, tokio::time::Instant::now())
    });
    let ((outcome, ended), log) = drive(
        &mut sent,
        search,
        |i| Some(if i < 8 { slow } else { Duration::ZERO }),
        |request| {
            reply(
                &dht,
                request,
                walk_node(walk_index(request.addr)).0,
                &[],
                &[],
            )
        },
    )
    .await;

    assert_eq!(queried_at(&log, Duration::ZERO), (0..5).collect::<Vec<_>>());
    assert_eq!(queried_at(&log, SHORT_TIMEOUT), (5..17).collect::<Vec<_>>());
    assert_eq!(log.len(), 17);
    assert_eq!(ended - start, SHORT_TIMEOUT + slow);
    assert_eq!(
        outcome.unwrap(),
        SearchOutcome {
            answered: 17,
            failed: 0,
            nodes: 17
        }
    );
}

// The five closest nodes answer 2 s after they are asked, the next three
// at once, and the two farthest never. At SHORT_TIMEOUT the walk asks the
// other eight. Once the eight closest have answered, at 2 s, the walk
// ends without waiting for the two farther ones it still has in flight,
// and announces to the eight then, as libtorrent's `find_data::done`
// does (traversal_algorithm.cpp:492-580, find_data.cpp:139-194,
// v2.0.15). The two are withdrawn: they wait for their answers apart
// from the walk (see `test_a_withdrawn_query_still_feeds_the_table`).
#[tokio::test(start_paused = true)]
async fn test_a_walk_ends_once_the_k_closest_answer_with_farther_queries_in_flight() {
    use super::SHORT_TIMEOUT;
    use crate::core::hash_id::Id20;

    let (dht, mut sent) = fixture_dht();
    let slow = Duration::from_secs(2);
    let req = super::RecursiveRequest::get_peers(
        dht.clone(),
        Id20::new([0x22; 20]),
        Some(6881),
        tokio::sync::mpsc::unbounded_channel().0,
    );
    let start = tokio::time::Instant::now();
    let search = tokio::spawn(async move {
        let outcome = req
            .search(
                true,
                (0..10).map(|i| {
                    let (id, addr) = walk_node(i);
                    (Some(id), addr)
                }),
            )
            .await;
        (outcome, tokio::time::Instant::now())
    });
    let ((outcome, ended), log) = drive(
        &mut sent,
        search,
        |i| match i {
            0..5 => Some(slow),
            5..8 => Some(Duration::ZERO),
            _ => None,
        },
        |request| {
            let i = walk_index(request.addr);
            answer(&dht, request, walk_node(i).0, Some(&[i]), &[]);
        },
    )
    .await;

    assert_eq!(queried_at(&log, Duration::ZERO), (0..5).collect::<Vec<_>>());
    assert_eq!(queried_at(&log, SHORT_TIMEOUT), (5..10).collect::<Vec<_>>());
    assert_eq!(ended - start, slow);
    let outcome = outcome.unwrap();
    assert_eq!((outcome.answered, outcome.failed), (8, 0));
    let mut announced: Vec<_> = log
        .iter()
        .filter(|&&(_, announce, _)| announce)
        .map(|&(i, _, at)| (i, at))
        .collect();
    announced.sort();
    assert_eq!(announced, (0..8).map(|i| (i, slow)).collect::<Vec<_>>());
    assert_eq!(dht.get_stats().outstanding_requests, 2);
}

// A node that answers after SHORT_TIMEOUT, here a find_node of a
// routing-table walk at 5 s, is not failed: its answer is used as any
// other, so the node it names is asked then, and enters the table as
// it answers.
#[tokio::test(start_paused = true)]
async fn test_a_late_answer_is_still_used() {
    use super::{RecursiveRequest, SearchOutcome};
    use crate::core::hash_id::Id20;

    let (dht, mut sent) = fixture_dht();
    let late = Duration::from_secs(5);
    let start = tokio::time::Instant::now();
    let search = tokio::spawn(RecursiveRequest::find_node_for_routing_table(
        dht.clone(),
        Id20::new([0x22; 20]),
        true,
        std::iter::once(walk_node(0).1),
    ));
    let named = |i: u8| {
        let (id, addr) = walk_node(i);
        let SocketAddr::V4(addr) = addr else {
            unreachable!()
        };
        (id, addr)
    };
    let (outcome, log) = drive(
        &mut sent,
        search,
        |i| Some(if i == 0 { late } else { Duration::ZERO }),
        |request| {
            let i = walk_index(request.addr);
            let nodes: Vec<_> = (i == 0).then(|| named(1)).into_iter().collect();
            reply(&dht, request, walk_node(i).0, &nodes, &[]);
        },
    )
    .await;

    assert_eq!(log, vec![(0, false, Duration::ZERO), (1, false, late)]);
    assert_eq!(
        outcome.unwrap(),
        SearchOutcome {
            answered: 2,
            failed: 0,
            nodes: 1
        }
    );
    assert_eq!(tokio::time::Instant::now() - start, late);
    // The node the late answer named answered and entered the table; the
    // first was given by address only, as a router is.
    assert_eq!(dht.routing_table_v4.read().len(), 1);
}

// Queries lined up for a full window go as it opens, one as each datagram
// sent leaves it, in the order they were lined up.
#[tokio::test(start_paused = true)]
async fn test_deferred_queries_go_in_order_as_their_window_opens() {
    use crate::core::hash_id::Id20;
    use crate::dht::{query_limiter::WINDOW, send_windows::SEND_LIMIT};
    use std::task::Poll;

    let (dht, mut sent) = fixture_dht();
    let node = SocketAddr::from(([10, 0, 0, 1], 6881));
    let start = tokio::time::Instant::now();
    let step = Duration::from_millis(100);
    for _ in 0..SEND_LIMIT {
        dht.send_guard.record(node.ip());
        tokio::time::advance(step).await;
    }
    let queries = [1u8, 2, 3]
        .map(|i| Box::pin(dht.request(super::Request::FindNode(Id20::new([i; 20])), node)));
    let mut queries = queries;
    for query in &mut queries {
        assert!(matches!(futures::poll!(query.as_mut()), Poll::Pending));
    }
    assert!(sent.try_recv().is_err(), "sent into a full window");
    assert_eq!(dht.send_guard.windows.lock().lined_up(node.ip()), 3);
    let mut all = futures::future::join_all(queries);
    let mut went = Vec::new();
    while went.len() < 3 {
        tokio::select! {
            _ = &mut all => unreachable!("the queries are not answered"),
            request = sent.recv() => {
                let request = request.unwrap();
                let MessageKind::FindNodeRequest(f) = request.message.kind else {
                    panic!("not a find_node");
                };
                went.push((f.target, tokio::time::Instant::now() - start));
            }
        }
    }
    assert_eq!(
        went,
        (0..3u8)
            .map(|i| (Id20::new([i + 1; 20]), WINDOW + step * u32::from(i)))
            .collect::<Vec<_>>()
    );
}

// An announce_peer counts in its node's window, and one past a full
// window waits for it to open.
#[tokio::test(start_paused = true)]
async fn test_an_announce_is_counted_and_waits_for_room() {
    use crate::core::hash_id::Id20;
    use crate::dht::{query_limiter::WINDOW, send_windows::SEND_LIMIT};
    use futures::StreamExt;

    let (dht, mut sent) = fixture_dht();
    let node = SocketAddr::from(([10, 0, 0, 1], 6881));
    let start = tokio::time::Instant::now();
    let mut announces = (0..=SEND_LIMIT)
        .map(|_| {
            dht.send_unanswered(
                super::Request::Announce {
                    info_hash: Id20::new([0x22; 20]),
                    token: ByteBufOwned::from(&b"t"[..]),
                    port: 6881,
                },
                node,
            )
        })
        .collect::<futures::stream::FuturesUnordered<_>>();
    let mut times = Vec::new();
    loop {
        tokio::select! {
            done = announces.next() => if done.is_none() { break },
            request = sent.recv() => {
                let request = request.unwrap();
                assert!(matches!(request.message.kind, MessageKind::AnnouncePeer(_)));
                times.push(tokio::time::Instant::now());
            }
        }
    }
    while let Ok(request) = sent.try_recv() {
        assert!(matches!(request.message.kind, MessageKind::AnnouncePeer(_)));
        times.push(tokio::time::Instant::now());
    }
    assert_eq!(times.len(), SEND_LIMIT + 1);
    assert_eq!(most_in_any_window(&times), SEND_LIMIT);
    // The last waited for the first to leave the window.
    assert_eq!(*times.last().unwrap(), times[0] + WINDOW);
    assert!(times[0] - start < WINDOW);
}

// Once nothing is sent, the windows close and the map empties; the sweep
// then rests until the next datagram.
#[tokio::test(start_paused = true)]
async fn test_the_send_windows_empty_once_traffic_stops() {
    use crate::dht::query_limiter::WINDOW;
    use std::task::Poll;

    let (dht, _sent) = fixture_dht();
    let guard = dht.send_guard.clone();
    let mut sweep = std::pin::pin!(guard.sweep());
    assert!(matches!(futures::poll!(sweep.as_mut()), Poll::Pending));
    for i in 1..=3u8 {
        dht.send_guard
            .record(SocketAddr::from(([10, 0, 0, i], 6881)).ip());
    }
    // Woken by the first datagram.
    assert!(matches!(futures::poll!(sweep.as_mut()), Poll::Pending));
    assert_eq!(dht.send_guard.windows.lock().len(), 3);
    tokio::time::advance(WINDOW).await;
    assert!(matches!(futures::poll!(sweep.as_mut()), Poll::Pending));
    assert_eq!(dht.send_guard.windows.lock().len(), 0);
    // Resting until a datagram is sent, whatever time passes.
    tokio::time::advance(10 * WINDOW).await;
    assert!(matches!(futures::poll!(sweep.as_mut()), Poll::Pending));
    assert_eq!(dht.send_guard.windows.lock().len(), 0);
    assert!(
        dht.send_guard.windows.lock().record(
            SocketAddr::from(([10, 0, 0, 1], 6881)).ip(),
            tokio::time::Instant::now()
        ),
        "the sweep was not resting"
    );
}
