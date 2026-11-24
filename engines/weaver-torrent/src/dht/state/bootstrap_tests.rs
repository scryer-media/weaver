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

use super::tests::{
    add_table_nodes, answer, fixture_dht, fixture_state, get_peers_search, node_at, refuse, reply,
};
use super::walk_tests::most_in_any_window;
use super::{DhtConfig, DhtState};
use crate::dht::bprotocol::{self, MessageKind};

// On the same network as the nodes the tests have it name: a node named
// at a local address is asked only when a local node names it.
const ROUTER: ([u8; 4], u16) = ([10, 200, 0, 1], 6881);

// A first bootstrap whose router answers while the nodes it names do not,
// as on a route that carries a datagram now and then before it settles,
// does not leave the table empty for good: the table is still shallow,
// so the routers are asked again a backoff later, and once the nodes
// answer they fill the table; the round after that starts from them.
// libtorrent bootstraps again from `node::tick` while the table is under
// 4 deep (node.cpp:647-662, v2.0.15).
#[tokio::test(start_paused = true)]
async fn test_a_bootstrap_that_got_no_answers_fills_the_table_once_the_path_comes_up() {
    use super::BOOTSTRAP_RETRY_MIN;
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;
    use std::collections::BTreeSet;

    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let named: Vec<(Id20, std::net::SocketAddrV4)> = (1..=4u8)
        .map(|i| {
            (
                Id20::new([0x80 + i; 20]),
                std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
            )
        })
        .collect();
    let named_addrs: BTreeSet<SocketAddr> = named.iter().map(|n| n.1.into()).collect();
    let start = tokio::time::Instant::now();
    let task = tokio::spawn(dht.clone().refresh_self(true));

    // Round one: the router answers, the nodes it names never do.
    let request = sent.recv().await.unwrap();
    assert_eq!(request.addr, router);
    assert!(matches!(
        request.message.kind,
        MessageKind::FindNodeRequest(_)
    ));
    reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
    let mut asked = BTreeSet::new();
    for _ in 0..named.len() {
        asked.insert(sent.recv().await.unwrap().addr);
    }
    assert_eq!(asked, named_addrs);
    assert_eq!(dht.routing_table_v4.read().len(), 0);

    // Round two, once those time out and a backoff after: the router
    // again; now the nodes answer.
    let request = sent.recv().await.unwrap();
    assert_eq!(request.addr, router);
    assert_eq!(
        tokio::time::Instant::now() - start,
        RESPONSE_TIMEOUT + BOOTSTRAP_RETRY_MIN
    );
    reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
    for _ in 0..named.len() {
        let request = sent.recv().await.unwrap();
        let &(id, _) = named
            .iter()
            .find(|n| SocketAddr::from(n.1) == request.addr)
            .unwrap();
        reply(&dht, &request, id, &[], &[]);
    }

    // Round three starts from the four nodes that answered, not the router.
    let mut asked = BTreeSet::new();
    for _ in 0..named.len() {
        asked.insert(sent.recv().await.unwrap().addr);
    }
    assert_eq!(asked, named_addrs);
    assert_eq!(dht.routing_table_v4.read().len(), 4);
    task.abort();
}

// A torrent's search asked for while the table is empty and no router is
// known starts as soon as a router becomes known, as its name resolves.
#[tokio::test(start_paused = true)]
async fn test_a_search_starts_as_the_first_router_becomes_known() {
    use crate::core::hash_id::Id20;

    let (dht, mut sent) = fixture_dht();
    let req = std::sync::Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        Id20::new([0x22; 20]),
        Some(6881),
        tokio::sync::mpsc::unbounded_channel().0,
    ));
    let task = req.request_peers_forever(true);
    tokio::time::advance(Duration::from_millis(8_400)).await;
    assert!(sent.try_recv().is_err());

    let known = tokio::time::Instant::now();
    dht.add_routers(&[SocketAddr::from(ROUTER)]);
    let request = sent.recv().await.unwrap();
    assert!(matches!(
        request.message.kind,
        MessageKind::GetPeersRequest(_)
    ));
    assert_eq!(request.addr, SocketAddr::from(ROUTER));
    assert_eq!(tokio::time::Instant::now(), known);
    task.abort();
}

// Our id with bit `bit` flipped, and `n` in its last byte: a node of the
// bucket `bit` buckets from the farthest.
fn id_in_bucket(own: crate::core::hash_id::Id20, bit: u8, n: u8) -> crate::core::hash_id::Id20 {
    let mut id = own.0;
    id[usize::from(bit / 8)] ^= 0x80 >> (bit % 8);
    id[19] ^= n + 1;
    crate::core::hash_id::Id20::new(id)
}

// A table `buckets` buckets past the farthest, each a full bucket of
// nodes that answered (128, 64, 32, 16, then 8: see
// `routing_table::bucket_limit`), each node in its own /24.
fn fill_deep_table(dht: &DhtState, buckets: u8) -> Vec<(crate::core::hash_id::Id20, SocketAddr)> {
    let mut table = dht.routing_table_v4.write();
    let mut nodes = Vec::new();
    for bit in 0..=buckets {
        let full = crate::dht::routing_table::bucket_limit(usize::from(bit));
        for n in 0..u8::try_from(full).unwrap() {
            let id = id_in_bucket(dht.id, bit, n);
            let addr = SocketAddr::from(([10, bit + 1, n + 1, 1], 6881));
            table.add_node(id, addr);
            table.mark_response(&id, std::time::Instant::now());
            nodes.push((id, addr));
        }
    }
    nodes
}

// libtorrent `routing_table::depth` (routing_table.cpp:368-391, v2.0.15):
// past the farthest bucket, the buckets in a row with half a bucket of
// nodes that answered. Nodes only heard of do not count.
#[test]
fn test_table_depth_counts_buckets_of_nodes_that_answered() {
    let (dht, _sent) = fixture_state();
    assert_eq!(
        dht.routing_table_v4.read().depth(std::time::Instant::now()),
        0
    );
    fill_deep_table(&dht, 5);
    assert_eq!(
        dht.routing_table_v4.read().depth(std::time::Instant::now()),
        5
    );

    let (dht, _sent) = fixture_state();
    {
        let mut table = dht.routing_table_v4.write();
        for bit in 0..=5 {
            let full = crate::dht::routing_table::bucket_limit(usize::from(bit));
            for n in 0..u8::try_from(full).unwrap() {
                table.add_node(
                    id_in_bucket(dht.id, bit, n),
                    SocketAddr::from(([10, bit + 1, n + 1, 1], 6881)),
                );
            }
        }
    }
    assert_eq!(
        dht.routing_table_v4.read().depth(std::time::Instant::now()),
        0
    );
}

// A deep table searches for our own id once every SELF_REFRESH_INTERVAL
// and not from the routers. When its nodes later stop answering, the
// next round has no node to count on and asks the routers again
// (node.cpp:647-662: a table under 4 deep bootstraps again).
#[tokio::test(start_paused = true)]
async fn test_a_table_that_empties_later_bootstraps_again() {
    use super::SELF_REFRESH_INTERVAL;
    use crate::dht::RESPONSE_TIMEOUT;

    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let nodes = fill_deep_table(&dht, 5);
    let deepened = dht.growth(true).grew.notified();
    let mut deepened = std::pin::pin!(deepened);
    deepened.as_mut().enable();
    let start = tokio::time::Instant::now();
    let task = tokio::spawn(dht.clone().refresh_self(true));

    // Round one: the eight nodes nearest our id, and they answer; the
    // round ends with the table deep.
    for _ in 0..8 {
        let request = sent.recv().await.unwrap();
        assert_ne!(request.addr, router);
        let &(id, _) = nodes.iter().find(|n| n.1 == request.addr).unwrap();
        reply(&dht, &request, id, &[], &[]);
    }
    deepened.await;

    // Every node stops answering.
    {
        let mut table = dht.routing_table_v4.write();
        for (id, addr) in &nodes {
            table.mark_outgoing_request(id, std::time::Instant::now());
            table.mark_error(id, *addr);
        }
    }

    // Round two, SELF_REFRESH_INTERVAL later, asks the router too.
    let first = sent.recv().await.unwrap();
    assert_eq!(tokio::time::Instant::now() - start, SELF_REFRESH_INTERVAL);
    let mut request = first;
    while request.addr != router {
        request = sent.recv().await.unwrap();
        assert!(
            tokio::time::Instant::now() - start <= SELF_REFRESH_INTERVAL + RESPONSE_TIMEOUT,
            "round two ended without asking the router"
        );
    }
    task.abort();
}

// An error reply is no answer (libtorrent rpc_manager.cpp:316-350): the
// node that sent it counts as failed and does not enter the table.
#[tokio::test(start_paused = true)]
async fn test_a_node_that_replies_with_an_error_does_not_enter_the_table() {
    use crate::core::hash_id::Id20;

    let (dht, mut sent) = fixture_dht();
    let node = (
        Id20::new([0x21; 20]),
        SocketAddr::from(([10, 0, 1, 1], 6881)),
    );
    let target = dht.id;
    let req = super::RecursiveRequest {
        info_hash: target,
        request: super::Request::FindNode(target),
        dht: dht.clone(),
        peer_tx: tokio::sync::mpsc::unbounded_channel().0,
        callbacks: super::RecursiveRequestCallbacksFindNodes {},
    };
    let search =
        tokio::spawn(async move { req.search(true, [(Some(node.0), node.1)].into_iter()).await });
    let request = sent.recv().await.unwrap();
    refuse(&dht, &request);
    let outcome = search.await.unwrap().unwrap();
    assert_eq!(
        outcome,
        super::SearchOutcome {
            answered: 0,
            failed: 1,
            nodes: 0
        }
    );
    assert_eq!(dht.routing_table_v4.read().len(), 0);
}

// A router that refuses the bootstrap does not end it: the table is still
// empty, so the router is asked again a backoff later.
#[tokio::test(start_paused = true)]
async fn test_a_bootstrap_refused_by_its_router_is_made_again() {
    use super::BOOTSTRAP_RETRY_MIN;

    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let start = tokio::time::Instant::now();
    let task = tokio::spawn(dht.clone().refresh_self(true));
    let request = sent.recv().await.unwrap();
    refuse(&dht, &request);
    let request = sent.recv().await.unwrap();
    assert_eq!(request.addr, router);
    assert_eq!(tokio::time::Instant::now() - start, BOOTSTRAP_RETRY_MIN);
    task.abort();
}

// A find_node reply that also names peers, as some nodes send, is walked
// past: the nodes it names are asked next, and the search goes on.
#[tokio::test(start_paused = true)]
async fn test_a_find_node_reply_naming_peers_does_not_end_the_search() {
    use crate::core::hash_id::Id20;

    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let task = tokio::spawn(dht.clone().refresh_self(true));
    let request = sent.recv().await.unwrap();
    assert_eq!(request.addr, router);
    let named = (
        Id20::new([0x81; 20]),
        std::net::SocketAddrV4::new([10, 0, 1, 1].into(), 6881),
    );
    reply(
        &dht,
        &request,
        Id20::new([0x99; 20]),
        &[named],
        &[SocketAddr::from(([10, 9, 9, 9], 51413))],
    );
    let request = sent.recv().await.unwrap();
    assert_eq!(request.addr, SocketAddr::from(named.1));
    task.abort();
}

// A route that carries nothing costs one router query a round: rounds
// back off from BOOTSTRAP_RETRY_MIN, doubling to SELF_REFRESH_INTERVAL
// apart, each after the last one's queries time out.
#[tokio::test(start_paused = true)]
async fn test_a_dead_route_backs_off_and_sends_a_bounded_number_of_datagrams() {
    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let start = tokio::time::Instant::now();
    let task = tokio::spawn(dht.clone().refresh_self(true));
    let hour = Duration::from_secs(3600);
    let mut at = Vec::new();
    loop {
        let request = sent.recv().await.unwrap();
        assert_eq!(request.addr, router);
        let elapsed = tokio::time::Instant::now() - start;
        if elapsed > hour {
            break;
        }
        at.push(elapsed.as_secs());
    }
    task.abort();
    assert_eq!(
        at,
        [0, 20, 45, 80, 135, 230, 405, 740, 1355, 1970, 2585, 3200]
    );
}

// A table restored from an earlier run holds nodes that have not answered
// this run: they do not keep a search from the routers. libtorrent seeds
// its bootstrap with its saved nodes and adds the routers
// (dht_tracker.cpp:184-191, v2.0.15).
#[tokio::test(start_paused = true)]
async fn test_a_restored_table_of_stale_nodes_does_not_keep_the_routers_out() {
    use crate::core::hash_id::Id20;
    use crate::dht::routing_table::RoutingTable;

    let (saved, _sent) = fixture_state();
    add_table_nodes(&saved, 5, true);
    let json = serde_json::to_string(&*saved.routing_table_v4.read()).unwrap();
    let restored: RoutingTable = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.len(), 5);

    let router = SocketAddr::from(ROUTER);
    for refresh in [true, false] {
        let (tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
        let dht = std::sync::Arc::new(DhtState::new_internal(
            saved.id,
            tx,
            Some(restored.clone()),
            None,
            (Ipv4Addr::LOCALHOST, 0).into(),
            tokio_util::sync::CancellationToken::new(),
            None,
        ));
        dht.started_for_test();
        dht.add_routers(&[router]);
        let task = if refresh {
            tokio::spawn(dht.clone().refresh_self(true)).abort_handle()
        } else {
            let req = std::sync::Arc::new(super::RecursiveRequest::get_peers(
                dht.clone(),
                Id20::new([0x22; 20]),
                Some(6881),
                tokio::sync::mpsc::unbounded_channel().0,
            ));
            req.request_peers_forever(true).abort_handle()
        };
        // The router is asked in the first search, after the nodes ahead
        // of it fail.
        let start = tokio::time::Instant::now();
        loop {
            let request = sent.recv().await.unwrap();
            if request.addr == router {
                break;
            }
            assert!(
                tokio::time::Instant::now() - start <= crate::dht::RESPONSE_TIMEOUT,
                "the first search ended without asking the router"
            );
        }
        task.abort();
    }
}

// A route whose routers become known 3.1 s in, as their names resolve
// through a tunnel: the torrent's first search asks the router and gets
// only a timeout. The self-refresh that starts with it learns four nodes
// from the router; two answer and enter the table, two never answer and
// are asked again each round; the table stays under 4 deep. The torrent
// searches again EARLY_SEARCH_MIN after its first search ended, from the
// nodes that answered, long before its turn, and finds peers.
#[tokio::test(start_paused = true)]
async fn test_a_short_search_searches_again_once_nodes_that_answer_enter_the_table() {
    use super::{EARLY_SEARCH_MIN, SHALLOW_DEPTH};
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;
    use std::collections::BTreeMap;

    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    let named: Vec<(Id20, std::net::SocketAddrV4)> = (1..=4u8)
        .map(|i| {
            (
                Id20::new([0x80 + i; 20]),
                std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
            )
        })
        .collect();
    // Nodes 1 and 2 answer; 3 and 4 never do, nor does the router to a
    // get_peers.
    let answering = |addr: SocketAddr| {
        named[..2]
            .iter()
            .find(|n| SocketAddr::from(n.1) == addr)
            .map(|n| n.0)
    };
    let peer = SocketAddr::from(([10, 9, 9, 9], 51413));
    let info_hash = Id20::new([0x22; 20]);
    let slot = super::announce_slot(&info_hash);
    let (peer_tx, mut peers) = tokio::sync::mpsc::unbounded_channel();
    let req = Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        None,
        peer_tx,
    ));
    let start = tokio::time::Instant::now();
    let refresh = tokio::spawn(dht.clone().refresh_self(true));
    let task = req.request_peers_forever(true);

    let known = Duration::from_millis(3_100);
    tokio::time::sleep(known).await;
    dht.add_routers(&[router]);
    // Both ask the router before anything answers.
    let mut first = Vec::new();
    while first.len() < 2 {
        let request = sent.recv().await.unwrap();
        assert_eq!(request.addr, router);
        first.push(request);
    }

    let mut searched = BTreeMap::<Duration, Vec<SocketAddr>>::new();
    let mut first = first.into_iter();
    let (found, found_at) = loop {
        let request = match first.next() {
            Some(request) => request,
            None => tokio::select! {
                biased;
                found = peers.recv() => {
                    break (found.unwrap(), tokio::time::Instant::now() - start);
                }
                request = sent.recv() => request.unwrap(),
            },
        };
        match request.message.kind {
            MessageKind::FindNodeRequest(_) if request.addr == router => {
                reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
            }
            MessageKind::FindNodeRequest(_) => {
                if let Some(id) = answering(request.addr) {
                    reply(&dht, &request, id, &[], &[]);
                }
            }
            MessageKind::GetPeersRequest(_) => {
                let at = tokio::time::Instant::now() - start;
                searched.entry(at).or_default().push(request.addr);
                if let Some(id) = answering(request.addr) {
                    reply(&dht, &request, id, &[], &[peer]);
                }
            }
            ref kind => panic!("unexpected {kind:?}"),
        }
    };
    task.abort();
    refresh.abort();

    let again = known + RESPONSE_TIMEOUT + EARLY_SEARCH_MIN;
    assert!(again < slot);
    assert_eq!(found, peer);
    assert_eq!(found_at, again);
    assert_eq!(
        searched.keys().copied().collect::<Vec<_>>(),
        vec![known, again]
    );
    assert_eq!(searched[&known], vec![router]);
    let table = dht.routing_table_v4.read();
    assert_eq!(table.len(), 2);
    assert!(table.depth(std::time::Instant::now()) < SHALLOW_DEPTH);
}

// A route that carries nothing: the torrent searches as it starts, at
// its slot and an interval after each search, each time asking the
// router once and waiting out its timeout; the table never grows, so it
// never searches early. Its first hour costs five get_peers, and the
// self-refresh beside it twelve find_node.
#[tokio::test(start_paused = true)]
async fn test_a_torrent_on_a_dead_route_searches_only_at_its_turns() {
    use crate::core::hash_id::Id20;
    use crate::dht::{DHT_ANNOUNCE_INTERVAL, RESPONSE_TIMEOUT};

    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let info_hash = Id20::new([0x22; 20]);
    let req = Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        None,
        tokio::sync::mpsc::unbounded_channel().0,
    ));
    let start = tokio::time::Instant::now();
    let refresh = tokio::spawn(dht.clone().refresh_self(true));
    let task = req.request_peers_forever(true);
    let hour = Duration::from_secs(3600);
    let (mut searched, mut refreshed) = (Vec::new(), Vec::new());
    loop {
        let request = sent.recv().await.unwrap();
        let at = tokio::time::Instant::now() - start;
        if at > hour {
            break;
        }
        assert_eq!(request.addr, router);
        match request.message.kind {
            MessageKind::GetPeersRequest(_) => searched.push(at),
            MessageKind::FindNodeRequest(_) => refreshed.push(at.as_secs()),
            ref kind => panic!("unexpected {kind:?}"),
        }
    }
    task.abort();
    refresh.abort();

    // Each due an interval (the slot, first) after the last search ended.
    let slot = super::announce_slot(&info_hash);
    let want: Vec<Duration> = (0..5u32)
        .map(|n| match n {
            0 => Duration::ZERO,
            n => n * RESPONSE_TIMEOUT + slot + (n - 1) * DHT_ANNOUNCE_INTERVAL,
        })
        .collect();
    assert!(*want.last().unwrap() <= hour);
    assert_eq!(searched, want);
    assert_eq!(
        refreshed,
        [0, 20, 45, 80, 135, 230, 405, 740, 1355, 1970, 2585, 3200]
    );
}

// Twenty torrents whose searches all fell short at once: the first
// growth of the table lets EARLY_SEARCHES_PER_GROWTH of them search
// again, the others wait for the next growth, not for one of those
// searches to end.
#[tokio::test(start_paused = true)]
async fn test_many_short_searches_search_again_a_batch_per_growth() {
    use super::{EARLY_SEARCH_MIN, EARLY_SEARCHES_PER_GROWTH};
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;
    use std::collections::BTreeMap;

    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let node = |i: u8| {
        (
            Id20::new([0x80 + i; 20]),
            std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
        )
    };
    let id_of = |addr: SocketAddr| {
        (1..=2u8)
            .map(node)
            .find(|n| SocketAddr::from(n.1) == addr)
            .map(|n| n.0)
    };
    // The router names node 1 from 20 s, node 2 too at 55 s, the fourth
    // round after it: the table grows at those two moments only.
    let first_growth = Duration::from_secs(20);
    let second_growth = Duration::from_secs(55);
    let start = tokio::time::Instant::now();
    let refresh = tokio::spawn(dht.clone().refresh_self(true));
    // Their slots are far past the end of the test.
    let torrents: Vec<_> = (0..20u8)
        .map(|i| {
            let info_hash = Id20::new([0x80 + i; 20]);
            assert!(super::announce_slot(&info_hash) > Duration::from_secs(300));
            Arc::new(super::RecursiveRequest::get_peers(
                dht.clone(),
                info_hash,
                None,
                tokio::sync::mpsc::unbounded_channel().0,
            ))
            .request_peers_forever(true)
        })
        .collect();

    // Searches started at each moment, each counted by its query to the
    // router, until the torrents that searched again first may do so
    // once more.
    let next_round = EARLY_SEARCH_MIN + 2 * EARLY_SEARCH_MIN;
    let mut searched = BTreeMap::<Duration, usize>::new();
    loop {
        let request = sent.recv().await.unwrap();
        let at = tokio::time::Instant::now() - start;
        if at >= next_round {
            break;
        }
        match request.message.kind {
            MessageKind::FindNodeRequest(_) if request.addr == router => {
                let named: Vec<_> = match at {
                    at if at < first_growth => continue,
                    at if at == second_growth => vec![node(1), node(2)],
                    _ => vec![node(1)],
                };
                reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
            }
            MessageKind::GetPeersRequest(_) if request.addr == router => {
                *searched.entry(at).or_default() += 1;
                reply(&dht, &request, Id20::new([0x99; 20]), &[], &[]);
            }
            MessageKind::FindNodeRequest(_) | MessageKind::GetPeersRequest(_) => {
                reply(&dht, &request, id_of(request.addr).unwrap(), &[], &[]);
            }
            ref kind => panic!("unexpected {kind:?}"),
        }
    }
    for task in torrents {
        task.abort();
    }
    refresh.abort();

    // The first round of the refresh times out; the second, at 20 s,
    // grows the table.
    assert_eq!(first_growth, RESPONSE_TIMEOUT + super::BOOTSTRAP_RETRY_MIN);
    // Each moment's searches start as the query budget lets them in,
    // two tokens each (the search let in and its query to the router):
    // all within 100 ms of it.
    let moments = [Duration::ZERO, EARLY_SEARCH_MIN, second_growth];
    let mut by_moment = BTreeMap::<Duration, usize>::new();
    for (at, n) in &searched {
        let moment = *moments.iter().rev().find(|m| *m <= at).unwrap();
        assert!(*at - moment < Duration::from_millis(100), "{searched:?}");
        *by_moment.entry(moment).or_default() += n;
    }
    assert_eq!(
        by_moment,
        BTreeMap::from([
            (Duration::ZERO, 20),
            (EARLY_SEARCH_MIN, EARLY_SEARCHES_PER_GROWTH),
            (second_growth, 20 - EARLY_SEARCHES_PER_GROWTH),
        ])
    );
}

// A self-refresh round that walks for longer than EARLY_SEARCH_MIN: the
// router answers it ROUTER_DELAY late, naming two nodes near our id that
// answer at once and 98 farther ones, each in its own /24, that never
// answer, so the round asks them five at a time and one more for each
// late one, and waits on each for RESPONSE_TIMEOUT. The torrent's first
// search, asked only of the router, is refused at once. The two nodes
// enter the table as the router answers, and the torrent searches again
// EARLY_SEARCH_MIN after its first search ended, from them, while the
// round is still waiting on silent nodes.
#[tokio::test(start_paused = true)]
async fn test_a_short_search_searches_again_before_the_round_that_grew_the_table_ends() {
    use super::EARLY_SEARCH_MIN;
    use crate::core::hash_id::Id20;
    use std::collections::BTreeMap;

    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let answering: Vec<(Id20, std::net::SocketAddrV4)> = (1..=2u8)
        .map(|i| {
            let mut id = dht.id.0;
            id[19] ^= i;
            (
                Id20::new(id),
                std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
            )
        })
        .collect();
    let is_silent = |addr: SocketAddr| match addr.ip() {
        std::net::IpAddr::V4(a) => a.octets()[..2] == [10, 1],
        std::net::IpAddr::V6(_) => false,
    };
    let silent: Vec<(Id20, std::net::SocketAddrV4)> = (1..=98u8)
        .map(|i| {
            let mut id = [0xc0; 20];
            id[19] = i;
            (
                Id20::new(id),
                std::net::SocketAddrV4::new([10, 1, i, 1].into(), 6881),
            )
        })
        .collect();
    const ROUTER_DELAY: Duration = Duration::from_secs(5);
    let named: Vec<_> = answering.iter().chain(&silent).copied().collect();
    let id_of = |addr: SocketAddr| {
        answering
            .iter()
            .find(|n| SocketAddr::from(n.1) == addr)
            .map(|n| n.0)
    };
    let peer = SocketAddr::from(([10, 9, 9, 9], 51413));
    let info_hash = Id20::new([0x22; 20]);
    assert!(super::announce_slot(&info_hash) > EARLY_SEARCH_MIN);
    let (peer_tx, mut peers) = tokio::sync::mpsc::unbounded_channel();
    let req = Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        None,
        peer_tx,
    ));
    let start = tokio::time::Instant::now();
    let refresh = tokio::spawn(dht.clone().refresh_self(true));
    let task = req.request_peers_forever(true);

    // Nothing answers until both have asked the router: the torrent's
    // search has started before the table grows.
    let mut first = Vec::new();
    while first.len() < 2 {
        let request = sent.recv().await.unwrap();
        assert_eq!(request.addr, router);
        first.push(request);
    }
    let mut first = first.into_iter();
    let mut searched = BTreeMap::<Duration, usize>::new();
    let mut silent_asked = 0;
    // The router's answer to the round, until ROUTER_DELAY.
    let mut held = None;
    let (found, found_at) = loop {
        let request = match first.next() {
            Some(request) => request,
            None => tokio::select! {
                biased;
                found = peers.recv() => {
                    break (found.unwrap(), tokio::time::Instant::now() - start);
                }
                () = super::sleep_until_some(held.as_ref().map(|_| start + ROUTER_DELAY)) => {
                    let request = held.take().unwrap();
                    reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
                    continue;
                }
                request = sent.recv() => request.unwrap(),
            },
        };
        match request.message.kind {
            MessageKind::FindNodeRequest(_) if request.addr == router => {
                assert!(held.replace(request).is_none());
            }
            MessageKind::FindNodeRequest(_) => match id_of(request.addr) {
                Some(id) => reply(&dht, &request, id, &[], &[]),
                None => silent_asked += 1,
            },
            MessageKind::GetPeersRequest(_) => {
                if request.addr == router {
                    *searched
                        .entry(tokio::time::Instant::now() - start)
                        .or_default() += 1;
                    refuse(&dht, &request);
                } else {
                    let id = id_of(request.addr).unwrap();
                    reply(&dht, &request, id, &[], &[peer]);
                }
            }
            ref kind => panic!("unexpected {kind:?}"),
        }
    };
    task.abort();
    // The round is still waiting on silent nodes: it has not ended.
    let waiting = dht
        .inflight_by_transaction_id
        .iter()
        .filter(|e| is_silent(e.key().1))
        .count();
    refresh.abort();

    assert_eq!(found, peer);
    assert_eq!(found_at, EARLY_SEARCH_MIN);
    assert_eq!(
        searched,
        BTreeMap::from([(Duration::ZERO, 1), (EARLY_SEARCH_MIN, 1)])
    );
    assert!(waiting > 0);
    assert!(silent_asked > 0);
    assert_eq!(dht.routing_table_v4.read().len(), answering.len());
}

// The routers of a fresh DHT as they were met on a public swarm: one
// never answers a get_peers, though it names nodes to a find_node; the
// other names one node that never answers, eight times over, to
// anything. The torrent's first search, made before any node answered,
// has only the routers and that silent node to ask, and ends short as
// their timeouts run out. The bootstrap has by then put
// SEARCH_MIN_SEEDS nodes that answer in the table, so the search is made
// again at once, from them, not EARLY_SEARCH_MIN later, and finds the
// peer; the round that seeded the table is still asking silent nodes.
#[tokio::test(start_paused = true)]
async fn test_a_first_search_made_from_the_routers_alone_searches_again_once_the_table_is_seeded() {
    use super::{EARLY_SEARCH_MIN, SEARCH_MIN_SEEDS};
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;
    use std::collections::BTreeMap;

    let seeds_u8 = u8::try_from(SEARCH_MIN_SEEDS).unwrap();
    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    let echo = SocketAddr::from(([10, 200, 0, 2], 6881));
    dht.add_routers(&[router, echo]);
    let node = |id: [u8; 20], net: u8, i: u8| {
        (
            Id20::new(id),
            std::net::SocketAddrV4::new([10, net, i, 1].into(), 6881),
        )
    };
    // The nodes that answer are near our own id, so the round that
    // looks for it asks them first.
    let answering: Vec<_> = (1..=seeds_u8)
        .map(|i| {
            let mut id = dht.id.0;
            id[19] ^= i;
            node(id, 0, i)
        })
        .collect();
    let silent: Vec<_> = (1..=40u8)
        .map(|i| {
            let mut id = [0xc0; 20];
            id[19] = i;
            node(id, 1, i)
        })
        .collect();
    let echoed = vec![node([0xa0; 20], 2, 1); 8];
    let named: Vec<_> = answering.iter().chain(&silent).copied().collect();
    let id_of = |addr: SocketAddr| {
        answering
            .iter()
            .find(|n| SocketAddr::from(n.1) == addr)
            .map(|n| n.0)
    };
    let peer = SocketAddr::from(([10, 9, 9, 9], 51413));
    let info_hash = Id20::new([0x22; 20]);
    assert!(super::announce_slot(&info_hash) > RESPONSE_TIMEOUT + EARLY_SEARCH_MIN);
    let (peer_tx, mut peers) = tokio::sync::mpsc::unbounded_channel();
    let req = Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        None,
        peer_tx,
    ));
    let start = tokio::time::Instant::now();
    let refresh = tokio::spawn(dht.clone().refresh_self(true));
    let task = req.request_peers_forever(true);

    // Nothing answers until the round and the search have both asked
    // both routers.
    let mut first = Vec::new();
    while first.len() < 4 {
        let request = sent.recv().await.unwrap();
        assert!(request.addr == router || request.addr == echo);
        first.push(request);
    }
    let mut first = first.into_iter();
    // Each get_peers by when it was sent and to whom.
    let mut searched = BTreeMap::<Duration, Vec<SocketAddr>>::new();
    let mut silent_asked = 0;
    let (found, found_at) = loop {
        let request = match first.next() {
            Some(request) => request,
            None => tokio::select! {
                biased;
                found = peers.recv() => {
                    break (found.unwrap(), tokio::time::Instant::now() - start);
                }
                request = sent.recv() => request.unwrap(),
            },
        };
        match request.message.kind {
            MessageKind::FindNodeRequest(_) if request.addr == echo => {
                reply(&dht, &request, Id20::new([0x98; 20]), &echoed, &[]);
            }
            MessageKind::FindNodeRequest(_) if request.addr == router => {
                reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
            }
            MessageKind::FindNodeRequest(_) => match id_of(request.addr) {
                Some(id) => reply(&dht, &request, id, &[], &[]),
                None => silent_asked += 1,
            },
            MessageKind::GetPeersRequest(_) => {
                searched
                    .entry(tokio::time::Instant::now() - start)
                    .or_default()
                    .push(request.addr);
                if request.addr == echo {
                    reply(&dht, &request, Id20::new([0x98; 20]), &echoed, &[]);
                } else if let Some(id) = id_of(request.addr) {
                    reply(&dht, &request, id, &[], &[peer]);
                }
            }
            ref kind => panic!("unexpected {kind:?}"),
        }
    };
    task.abort();
    // The round that seeded the table is still waiting on silent nodes.
    let waiting = dht
        .inflight_by_transaction_id
        .iter()
        .filter(|e| silent.iter().any(|n| SocketAddr::from(n.1) == e.key().1))
        .count();
    assert!(waiting > 0);
    assert!(silent_asked > 0);
    refresh.abort();

    assert_eq!(found, peer);
    assert_eq!(found_at, RESPONSE_TIMEOUT);
    let mut at_start = searched[&Duration::ZERO].clone();
    at_start.sort();
    let mut leaned = vec![router, echo, SocketAddr::from(echoed[0].1)];
    leaned.sort();
    assert_eq!(at_start, leaned);
    // Made again from the table alone: it no longer leans on the routers.
    let mut again = searched[&RESPONSE_TIMEOUT].clone();
    again.sort();
    let mut seeds: Vec<_> = answering.iter().map(|n| SocketAddr::from(n.1)).collect();
    seeds.sort();
    assert_eq!(again, seeds);
    assert_eq!(searched.len(), 2);
}

// After a restart the table holds SEARCH_MIN_SEEDS nodes from the last
// run, none of which has answered yet, and a router that never does.
// The first search leans on the router, and the nodes answer it, which
// seeds the table without any node entering it. The search ends short
// as the router's query times out, and is made again at once, from the
// nodes alone.
#[tokio::test(start_paused = true)]
async fn test_a_restored_table_that_becomes_seeded_without_growing_searches_again_at_once() {
    use super::{EARLY_SEARCH_MIN, SEARCH_MIN_SEEDS};
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;

    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let restored: Vec<_> = (1..=u8::try_from(SEARCH_MIN_SEEDS).unwrap())
        .map(|i| node_at(u32::from(i), [10, 0, i, 1]))
        .collect();
    {
        let mut table = dht.routing_table_v4.write();
        for (id, addr) in &restored {
            table.add_node(*id, SocketAddr::V4(*addr));
        }
        assert!(!table.is_seeded());
    }
    let growth = {
        let dht = dht.clone();
        tokio::spawn(async move { dht.watch_growth(true).await })
    };
    let info_hash = Id20::new([0x22; 20]);
    assert!(super::announce_slot(&info_hash) > RESPONSE_TIMEOUT + EARLY_SEARCH_MIN);
    let req = Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        None,
        tokio::sync::mpsc::unbounded_channel().0,
    ));
    let start = tokio::time::Instant::now();
    let task = req.request_peers_forever(true);

    let mut searched = Vec::new();
    while searched.len() < 2 * SEARCH_MIN_SEEDS + 1 {
        let request = sent.recv().await.unwrap();
        assert!(matches!(
            request.message.kind,
            MessageKind::GetPeersRequest(_)
        ));
        searched.push((tokio::time::Instant::now() - start, request.addr));
        if let Some((id, _)) = restored
            .iter()
            .find(|n| SocketAddr::V4(n.1) == request.addr)
        {
            reply(&dht, &request, *id, &[], &[]);
        }
    }
    task.abort();
    growth.abort();
    assert!(dht.routing_table_v4.read().is_seeded());
    let at = |when: Duration| {
        let mut addrs: Vec<_> = searched
            .iter()
            .filter(|s| s.0 == when)
            .map(|s| s.1)
            .collect();
        addrs.sort();
        addrs
    };
    let mut nodes: Vec<_> = restored.iter().map(|n| SocketAddr::V4(n.1)).collect();
    nodes.sort();
    let mut leaned = nodes.clone();
    leaned.push(router);
    leaned.sort();
    assert_eq!(at(Duration::ZERO), leaned);
    assert_eq!(at(RESPONSE_TIMEOUT), nodes);
}

// 200 torrents start on a restored table of SEARCH_MIN_SEEDS nodes and a
// router, none of which answers. Every first search leans on the router,
// ends short and waits to be made again once the table is seeded; the
// three nodes, which never answered, leave the table at their first
// failure. Then the three nodes answer and enter it again, which seeds
// the table. Inside every limit:
// - that growth lets EARLY_SEARCHES_PER_GROWTH torrents search again, and
//   no more come before the next growth;
// - the searches walking at once and the queries outstanding stay inside
//   what the query budget allows (see `DhtState::admit_walk`): with 20 a
//   second, no more than 20 + 20 * 30 = 620 walking, and no more than
//   20 + 20 * 15 = 320 sent and in flight, with the four of the search
//   let in last still waiting to be sent; the second searches, at most
//   EARLY_SEARCHES_PER_GROWTH, have three or four each;
// - every query waits on the global rate limiter, here 20 a second with
//   a burst of 20, so by any time t no more than 20 + t / 50 ms went;
// - no address is sent more than SEND_LIMIT in any WINDOW.
#[tokio::test(start_paused = true)]
async fn test_torrents_waiting_for_a_seeded_table_search_again_inside_every_limit() {
    use super::{EARLY_SEARCHES_PER_GROWTH, RateLimiter, SEARCH_MIN_SEEDS, TokioClock};
    use crate::core::hash_id::Id20;
    use crate::dht::send_windows::SEND_LIMIT;
    use std::{
        collections::{BTreeSet, HashMap},
        num::NonZeroU32,
    };

    const TORRENTS: u16 = 200;
    let (mut state, mut sent) = fixture_state();
    let start = tokio::time::Instant::now();
    let per_second = 20;
    state.rate_limiter = RateLimiter::direct_with_clock(
        governor::Quota::per_second(NonZeroU32::new(per_second).unwrap()),
        TokioClock { start },
    );
    let dht = Arc::new(state);
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let restored: Vec<_> = (1..=u8::try_from(SEARCH_MIN_SEEDS).unwrap())
        .map(|i| node_at(u32::from(i), [10, 0, i, 1]))
        .collect();
    {
        let mut table = dht.routing_table_v4.write();
        for (id, addr) in &restored {
            table.add_node(*id, SocketAddr::V4(*addr));
        }
        assert!(!table.is_seeded());
    }
    let growth = {
        let dht = dht.clone();
        tokio::spawn(async move { dht.watch_growth(true).await })
    };
    let tasks: Vec<_> = (0..TORRENTS)
        .map(|i| {
            let mut hash = [0xf0; 20];
            hash[1..3].copy_from_slice(&i.to_be_bytes());
            let info_hash = Id20::new(hash);
            // The torrent's own turn is far past this test.
            assert!(super::announce_slot(&info_hash) > Duration::from_secs(600));
            Arc::new(super::RecursiveRequest::get_peers(
                dht.clone(),
                info_hash,
                None,
                tokio::sync::mpsc::unbounded_channel().0,
            ))
            .request_peers_forever(true)
        })
        .collect();
    let record = |request: super::WorkerSendRequest| {
        let MessageKind::GetPeersRequest(g) = request.message.kind else {
            panic!("not a get_peers: {:?}", request.message.kind);
        };
        (tokio::time::Instant::now(), request.addr, g.info_hash)
    };
    let walks_ended = || {
        dht.get_stats().outstanding_requests == 0
            && dht
                .walk_admission
                .open
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
            && dht.walk_admission.door.available_permits() == 1
    };
    let open = || {
        dht.walk_admission
            .open
            .load(std::sync::atomic::Ordering::Acquire)
    };
    let mut most_open = 0;

    // The first searches: the first ones ask the three nodes and the
    // router. The three, never answered, fail at their first short
    // timeout and leave the table, so the later ones ask the router
    // alone, until the router's send window is full: a search whose
    // router waits on it past RESPONSE_TIMEOUT ends with nothing sent.
    // The phase is over once every torrent's search was let in and
    // ended: none waits at the door and none walks.
    let mut first = Vec::new();
    let mut most_in_flight = 0;
    // Each query is taken as it is sent, so its time is when it went.
    loop {
        tokio::select! {
            biased;
            Some(request) = sent.recv() => first.push(record(request)),
            () = tokio::time::sleep(Duration::from_millis(50)) => {
                if walks_ended() {
                    break;
                }
            }
        }
        most_in_flight = most_in_flight.max(dht.get_stats().outstanding_requests);
        most_open = most_open.max(open());
    }
    // The budget held them, not a count: far more than
    // EARLY_SEARCHES_PER_GROWTH walked at once.
    assert!(most_open > EARLY_SEARCHES_PER_GROWTH, "{most_open}");
    assert!(most_open <= 20 + 20 * 30, "{most_open}");
    assert!(most_in_flight <= 20 + 20 * 15 + 4, "{most_in_flight}");
    assert_eq!(dht.routing_table_v4.read().len(), 0);

    // The three nodes answer, and enter the table again: it is seeded,
    // which is a growth.
    let seeded_at = tokio::time::Instant::now();
    {
        let mut table = dht.routing_table_v4.write();
        for (id, addr) in &restored {
            table.add_node(*id, SocketAddr::V4(*addr));
            assert!(table.mark_response(id, std::time::Instant::now()));
        }
        assert!(table.is_seeded());
    }
    let mut again = Vec::new();
    let mut most_in_flight = 0;
    while tokio::time::Instant::now() < seeded_at + Duration::from_secs(60) {
        tokio::time::advance(Duration::from_millis(50)).await;
        while let Ok(request) = sent.try_recv() {
            again.push(record(request));
        }
        most_in_flight = most_in_flight.max(dht.get_stats().outstanding_requests);
    }
    assert!(walks_ended());
    growth.abort();
    for task in tasks {
        task.abort();
    }

    // EARLY_SEARCHES_PER_GROWTH torrents searched again, each from the
    // three nodes: alone for the ones let in at the growth, and with the
    // router for the ones let in after the three, silent again, failed
    // their first queries and the table no longer counted on them.
    let mut by_torrent: HashMap<Id20, BTreeSet<SocketAddr>> = HashMap::new();
    for (_, addr, info_hash) in &again {
        assert!(by_torrent.entry(*info_hash).or_default().insert(*addr));
    }
    assert_eq!(by_torrent.len(), EARLY_SEARCHES_PER_GROWTH);
    let nodes: BTreeSet<_> = restored.iter().map(|n| SocketAddr::V4(n.1)).collect();
    let with_router: BTreeSet<_> = nodes.iter().copied().chain([router]).collect();
    assert!(
        by_torrent
            .values()
            .all(|asked| *asked == nodes || *asked == with_router)
    );
    let at_growth: BTreeSet<_> = again
        .iter()
        .filter(|s| s.0 == again[0].0)
        .map(|s| s.2)
        .collect();
    assert!(!at_growth.is_empty());
    assert!(at_growth.iter().all(|t| by_torrent[t] == nodes));
    assert!(most_in_flight <= 4 * EARLY_SEARCHES_PER_GROWTH);

    // The global rate limiter, over both phases: every query sent and
    // every search let in spent a token, and each torrent seen sending
    // in a phase was let in once in it before its first query. It held
    // them back: some went the moment its bucket allowed.
    let all = first
        .iter()
        .map(|s| (0, s))
        .chain(again.iter().map(|s| (1, s)));
    let mut let_in = BTreeSet::new();
    let mut held = 0;
    let mut times = Vec::new();
    for (k, (phase, (at, addr, info_hash))) in all.enumerate() {
        let_in.insert((phase, *info_hash));
        let spent = (k + 1 + let_in.len()) as u128;
        let tokens = per_second as u128 + (*at - start).as_millis() / (1000 / per_second as u128);
        assert!(spent <= tokens, "query {k} at {:?}", *at - start);
        held += usize::from(spent == tokens);
        times.push((*at, *addr));
    }
    assert!(held > 0);
    // Each address's send window.
    for addr in nodes.iter().chain([&router]) {
        let times: Vec<_> = times.iter().filter(|s| s.1 == *addr).map(|s| s.0).collect();
        assert!(most_in_any_window(&times) <= SEND_LIMIT);
    }
}

// A torrent's peer search, for `info_hash`, that never yields peers.
fn peer_search(
    dht: &Arc<DhtState>,
    info_hash: crate::core::hash_id::Id20,
) -> tokio::task::JoinHandle<()> {
    Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        None,
        tokio::sync::mpsc::unbounded_channel().0,
    ))
    .request_peers_forever(true)
}

fn walking(dht: &DhtState) -> usize {
    dht.walk_admission
        .open
        .load(std::sync::atomic::Ordering::Acquire)
}

// A lone torrent added to an idle DHT walks at once: its first query
// goes out the moment it asks for peers.
#[tokio::test(start_paused = true)]
async fn test_a_lone_torrent_walks_at_once() {
    let (dht, mut sent) = fixture_dht();
    let (id, addr) = node_at(1, [10, 0, 1, 1]);
    dht.routing_table_v4
        .write()
        .add_node(id, SocketAddr::V4(addr));
    let start = tokio::time::Instant::now();
    let task = peer_search(&dht, crate::core::hash_id::Id20::new([0xf0; 20]));
    let request = sent.recv().await.unwrap();
    assert_eq!(request.addr, SocketAddr::V4(addr));
    assert_eq!(tokio::time::Instant::now(), start);
    assert_eq!(walking(&dht), 1);
    task.abort();
}

// Sixteen torrents walk with the send window of every node they would
// ask full: each waits on the windows, with no query on the rate
// limiter. A seventeenth, whose nodes have room, is let in and sends at
// once: a walk that can only wait holds no one back.
#[tokio::test(start_paused = true)]
async fn test_walks_that_can_only_wait_hold_no_one_back() {
    use crate::dht::send_windows::SEND_LIMIT;

    let (dht, mut sent) = fixture_dht();
    // Two groups of eight nodes, far apart: each torrent's eight closest
    // are one group.
    let a = |j: u8| node_at(u32::from(j), [10, 1, j, 1]);
    let b = |j: u8| node_at(0x8000_0000 | u32::from(j), [10, 2, j, 1]);
    {
        let mut table = dht.routing_table_v4.write();
        for j in 0..8 {
            for (id, addr) in [a(j), b(j)] {
                table.add_node(id, SocketAddr::V4(addr));
            }
        }
        assert_eq!(table.len(), 16);
    }
    for j in 0..8 {
        for _ in 0..SEND_LIMIT {
            dht.send_guard.record(std::net::IpAddr::V4(*a(j).1.ip()));
        }
    }
    let start = tokio::time::Instant::now();
    let mut tasks: Vec<_> = (0..16)
        .map(|i| peer_search(&dht, node_at(1000 + i, [0; 4]).0))
        .collect();
    while walking(&dht) < 16 {
        tokio::task::yield_now().await;
    }
    assert!(sent.try_recv().is_err());
    tasks.push(peer_search(&dht, node_at(0x8000_0000 | 1000, [0; 4]).0));
    let request = sent.recv().await.unwrap();
    assert_eq!(tokio::time::Instant::now(), start);
    assert!((0..8).any(|j| request.addr == SocketAddr::V4(b(j).1)));
    assert_eq!(walking(&dht), 17);
    for task in tasks {
        task.abort();
    }
}

// Three torrents ask for peers in turn, with the rate limiter at 5 a
// second and a burst of 5, on a table of five nodes that answer every
// query naming nothing: each walk asks the five and ends as they
// answer. The first is let in and sends four at once; its fifth waits
// on the limiter, so the second and third wait too. Each is let in only
// once every query of the walks before it was sent, and in the order
// they asked; every search let in and every query sent spends a token.
#[tokio::test(start_paused = true)]
async fn test_walks_are_let_in_in_order_once_the_open_ones_have_sent() {
    use super::{RateLimiter, TokioClock};
    use std::{collections::HashMap, num::NonZeroU32};

    let (mut state, mut sent) = fixture_state();
    let start = tokio::time::Instant::now();
    state.rate_limiter = RateLimiter::direct_with_clock(
        governor::Quota::per_second(NonZeroU32::new(5).unwrap()),
        TokioClock { start },
    );
    let dht = Arc::new(state);
    let nodes: HashMap<SocketAddr, crate::core::hash_id::Id20> = (0..5u8)
        .map(|j| {
            let (id, addr) = node_at(u32::from(j), [10, 1, j, 1]);
            (SocketAddr::V4(addr), id)
        })
        .collect();
    {
        let mut table = dht.routing_table_v4.write();
        for (addr, id) in &nodes {
            table.add_node(*id, *addr);
        }
    }
    let hashes: Vec<_> = (0..3).map(|i| node_at(1000 + i, [0; 4]).0).collect();
    let tasks: Vec<_> = hashes.iter().map(|h| peer_search(&dht, *h)).collect();
    let mut sends = Vec::new();
    while sends.len() < 15 {
        let request = sent.recv().await.unwrap();
        let MessageKind::GetPeersRequest(g) = &request.message.kind else {
            panic!("not a get_peers: {:?}", request.message.kind);
        };
        sends.push((tokio::time::Instant::now(), g.info_hash));
        reply(&dht, &request, nodes[&request.addr], &[], &[]);
    }
    let order: Vec<_> = sends.iter().map(|s| s.1).collect();
    let expected: Vec<_> = hashes.iter().flat_map(|h| [*h; 5]).collect();
    assert_eq!(order, expected);
    for (k, (at, _)) in sends.iter().enumerate() {
        let spent = (k + 1 + k / 5 + 1) as u128;
        assert!(spent <= 5 + (*at - start).as_millis() / 200, "query {k}");
    }
    for task in tasks {
        task.abort();
    }
}

// 10,000 torrents ask for peers at once, with the default query budget
// (250 a second, 25 to start with) and one router that never answers.
// The router's send window fills, so most searches have nothing to
// send and end RESPONSE_TIMEOUT after they were let in. Every search let
// in spends a token, so by any time t no more than 25 + 250 t were, and
// at no time do more walk, or more queries wait for an answer, than the
// budget allows: 250 + 250 * 30 = 7750 walking, 250 + 250 * 15 = 4000 in
// flight. A torrent not let in yet holds no search state: its
// candidates are made as it is let in. Every torrent is let in, and
// once the last search ends none walks.
#[tokio::test(start_paused = true)]
async fn test_ten_thousand_torrents_walk_inside_the_query_budget() {
    use crate::dht::send_windows::SEND_LIMIT;

    const TORRENTS: u32 = 10_000;
    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let start = tokio::time::Instant::now();
    let tasks: Vec<_> = (0..TORRENTS)
        .map(|i| {
            let mut hash = [0xf0; 20];
            hash[1..4].copy_from_slice(&i.to_be_bytes()[1..]);
            let info_hash = crate::core::hash_id::Id20::new(hash);
            // The torrent's own turn is far past this test.
            assert!(super::announce_slot(&info_hash) > Duration::from_secs(600));
            peer_search(&dht, info_hash)
        })
        .collect();
    let mut most_walking = 0;
    let mut sends = Vec::new();
    loop {
        tokio::time::advance(Duration::from_millis(100)).await;
        while let Ok(request) = sent.try_recv() {
            assert_eq!(request.addr, router);
            sends.push(tokio::time::Instant::now());
        }
        let t = (tokio::time::Instant::now() - start).as_millis();
        let now_walking = walking(&dht);
        most_walking = most_walking.max(now_walking);
        assert!(
            now_walking as u128 <= 25 + 250 * t / 1000,
            "{now_walking} at {t} ms"
        );
        assert!(now_walking <= 250 + 250 * 30, "{now_walking}");
        assert!(dht.get_stats().outstanding_requests <= 250 + 250 * 15);
        if now_walking == 0 && dht.walk_admission.door.available_permits() == 1 {
            break;
        }
    }
    assert!(most_walking > 1000, "{most_walking}");
    assert!(most_in_any_window(&sends) <= SEND_LIMIT);
    for task in tasks {
        task.abort();
    }
}

// Only the first search is made again once the table is seeded: the
// routers name SEARCH_MIN_SEEDS nodes that answer a find_node but never
// a get_peers, and a fourth that answers from the second round on. The
// first search times out on the routers and is made again at once from
// the three; that one times out on them too, and the next search waits
// EARLY_SEARCH_MIN after it, as any short search does.
#[tokio::test(start_paused = true)]
async fn test_a_search_made_again_once_the_table_is_seeded_that_falls_short_waits_early_search_min()
{
    use super::{EARLY_SEARCH_MIN, SEARCH_MIN_SEEDS};
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;
    use std::collections::BTreeSet;

    let seeds_u8 = u8::try_from(SEARCH_MIN_SEEDS).unwrap();
    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let node = |i: u8| {
        let mut id = [0x40; 20];
        id[19] = i;
        (
            Id20::new(id),
            std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
        )
    };
    let id_of = |addr: SocketAddr| {
        (1..=seeds_u8 + 1)
            .map(node)
            .find(|n| SocketAddr::from(n.1) == addr)
            .map(|n| n.0)
    };
    let info_hash = Id20::new([0x22; 20]);
    let ends = 2 * RESPONSE_TIMEOUT + EARLY_SEARCH_MIN;
    assert!(super::announce_slot(&info_hash) > ends);
    let req = Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        None,
        tokio::sync::mpsc::unbounded_channel().0,
    ));
    let start = tokio::time::Instant::now();
    let refresh = tokio::spawn(dht.clone().refresh_self(true));
    let task = req.request_peers_forever(true);

    let mut first = Vec::new();
    while first.len() < 2 {
        let request = sent.recv().await.unwrap();
        assert_eq!(request.addr, router);
        first.push(request);
    }
    let mut first = first.into_iter();
    let mut searched = BTreeSet::<Duration>::new();
    loop {
        let request = match first.next() {
            Some(request) => request,
            None => sent.recv().await.unwrap(),
        };
        let at = tokio::time::Instant::now() - start;
        match request.message.kind {
            MessageKind::FindNodeRequest(_) if request.addr == router => {
                let named: Vec<_> = (1..=seeds_u8).map(node).collect();
                reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
            }
            // From the round after the first search ended, the nodes
            // name a fourth: the table grows while the search made
            // again is walking.
            MessageKind::FindNodeRequest(_) => {
                let named = if at > RESPONSE_TIMEOUT {
                    vec![node(seeds_u8 + 1)]
                } else {
                    Vec::new()
                };
                reply(&dht, &request, id_of(request.addr).unwrap(), &named, &[]);
            }
            MessageKind::GetPeersRequest(_) => {
                searched.insert(at);
                if at >= ends {
                    break;
                }
            }
            ref kind => panic!("unexpected {kind:?}"),
        }
    }
    task.abort();
    refresh.abort();

    assert_eq!(
        searched,
        BTreeSet::from([Duration::ZERO, RESPONSE_TIMEOUT, ends])
    );
}

// Many nodes entering at once are one growth, and the nodes that enter
// in the GROWTH_SPACING after it one more as it ends: the early turns
// are topped up to EARLY_SEARCHES_PER_GROWTH each time, never added up.
// A table nothing enters does not grow.
#[tokio::test(start_paused = true)]
async fn test_nodes_entering_grow_the_table_at_most_once_a_spacing() {
    use super::{EARLY_SEARCHES_PER_GROWTH, GROWTH_SPACING};
    use std::sync::atomic::Ordering;

    let (dht, _sent) = fixture_dht();
    let growth = dht.growth(true);
    let mut watch = std::pin::pin!(dht.watch_growth(true));
    assert!(futures::poll!(watch.as_mut()).is_pending());
    let start = tokio::time::Instant::now();

    // Waits for the next growth while the watch runs; when it came.
    macro_rules! next_growth {
        () => {{
            let grew = growth.grew.notified();
            let mut grew = std::pin::pin!(grew);
            grew.as_mut().enable();
            tokio::select! {
                never = watch.as_mut() => match never {},
                () = grew => tokio::time::Instant::now() - start,
            }
        }};
    }
    macro_rules! run_until {
        ($at:expr) => {
            tokio::select! {
                never = watch.as_mut() => match never {},
                () = tokio::time::sleep_until(start + $at) => {}
            }
        };
    }
    let enter = |bit: u8, n: u8| {
        let id = id_in_bucket(dht.id, bit, n);
        let mut table = dht.routing_table_v4.write();
        table.add_node(id, SocketAddr::from(([10, bit + 1, n + 1, 1], 6881)));
        table.mark_response(&id, std::time::Instant::now());
    };

    // The six farthest buckets' nodes enter at once.
    let at = {
        let grew = growth.grew.notified();
        let mut grew = std::pin::pin!(grew);
        grew.as_mut().enable();
        fill_deep_table(&dht, 5);
        tokio::select! {
            never = watch.as_mut() => match never {},
            () = grew => tokio::time::Instant::now() - start,
        }
    };
    assert_eq!(
        dht.routing_table_v4.read().entries(),
        128 + 64 + 32 + 16 + 8 + 8
    );
    assert_eq!(at, Duration::ZERO);
    assert_eq!(growth.generation.load(Ordering::Acquire), 1);
    assert_eq!(growth.turns.available_permits(), EARLY_SEARCHES_PER_GROWTH);

    // More enter one and two seconds later: one growth between them,
    // as the spacing ends.
    for (second, bit) in [(1, 6), (2, 7)] {
        run_until!(Duration::from_secs(second));
        for n in 0..4 {
            enter(bit, n);
        }
    }
    assert_eq!(
        dht.routing_table_v4.read().entries(),
        128 + 64 + 32 + 16 + 8 + 8 + 4 + 4
    );
    assert_eq!(growth.generation.load(Ordering::Acquire), 1);
    assert_eq!(next_growth!(), GROWTH_SPACING);
    assert_eq!(growth.generation.load(Ordering::Acquire), 2);
    assert_eq!(growth.turns.available_permits(), EARLY_SEARCHES_PER_GROWTH);

    // Nothing enters for a minute: no growth.
    run_until!(Duration::from_secs(60));
    assert_eq!(growth.generation.load(Ordering::Acquire), 2);

    // The next node to enter is a growth at once.
    enter(8, 0);
    assert_eq!(next_growth!(), Duration::from_secs(60));
    assert_eq!(growth.generation.load(Ordering::Acquire), 3);
}

// A short search whose table grows before every early search still
// waits EARLY_SEARCH_MIN after its first, doubling, and searches at its
// turn: the table's growth never brings it sooner.
#[tokio::test(start_paused = true)]
async fn test_a_short_search_backs_off_however_often_the_table_grows() {
    use super::EARLY_SEARCH_MIN;
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;
    use std::sync::atomic::Ordering;

    let (dht, mut sent) = fixture_dht();
    dht.add_routers(&[SocketAddr::from(ROUTER)]);
    let info_hash = Id20::new([0xf0; 20]);
    let slot = super::announce_slot(&info_hash);
    let req = Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        None,
        tokio::sync::mpsc::unbounded_channel().0,
    ));
    let mut watch = std::pin::pin!(dht.watch_growth(true));
    assert!(futures::poll!(watch.as_mut()).is_pending());
    let start = tokio::time::Instant::now();
    let task = req.request_peers_forever(true);

    // Nothing answers, so each search lasts RESPONSE_TIMEOUT; each asks
    // at most five, all as it starts. As each starts, a node that
    // answered enters the table.
    let due = RESPONSE_TIMEOUT + slot;
    let mut searched = Vec::new();
    loop {
        let request = tokio::select! {
            never = watch.as_mut() => match never {},
            request = sent.recv() => request.unwrap(),
        };
        assert!(matches!(
            request.message.kind,
            MessageKind::GetPeersRequest(_)
        ));
        let at = tokio::time::Instant::now() - start;
        if searched.last() == Some(&at) {
            continue;
        }
        searched.push(at);
        if at >= due {
            break;
        }
        let n = u8::try_from(searched.len()).unwrap();
        let id = id_in_bucket(dht.id, n, 0);
        let mut table = dht.routing_table_v4.write();
        table.add_node(id, SocketAddr::from(([10, n, 1, 1], 6881)));
        table.mark_response(&id, std::time::Instant::now());
    }
    task.abort();

    let mut want = vec![Duration::ZERO];
    let (mut end, mut early) = (RESPONSE_TIMEOUT, EARLY_SEARCH_MIN);
    while end + early < due {
        want.push(end + early);
        end += early + RESPONSE_TIMEOUT;
        early *= 2;
    }
    want.push(due);
    // Four early searches: 30, 60, 120 and 240 s after the one before.
    assert_eq!(want.len(), 6);
    assert_eq!(searched, want);
    assert!(dht.growth(true).generation.load(Ordering::Acquire) >= 5);
}

// A search that reached k nodes is not made again ahead of its turn,
// however the table grows after it.
#[tokio::test(start_paused = true)]
async fn test_a_search_that_was_not_short_waits_for_its_turn() {
    use crate::core::hash_id::Id20;
    use std::collections::BTreeMap;

    let (dht, mut sent) = fixture_dht();
    add_table_nodes(&dht, 8, true);
    let info_hash = Id20::new([0x22; 20]);
    let slot = super::announce_slot(&info_hash);
    let req = Arc::new(super::RecursiveRequest::get_peers(
        dht.clone(),
        info_hash,
        None,
        tokio::sync::mpsc::unbounded_channel().0,
    ));
    let start = tokio::time::Instant::now();
    let task = req.request_peers_forever(true);
    let mut searched = BTreeMap::<Duration, usize>::new();
    let mut grown = false;
    loop {
        let request = tokio::select! {
            request = sent.recv() => request.unwrap(),
            () = tokio::time::sleep_until(start + Duration::from_secs(40)), if !grown => {
                grown = true;
                dht.grow(true);
                continue;
            }
        };
        let at = tokio::time::Instant::now() - start;
        *searched.entry(at).or_default() += 1;
        if at >= slot {
            break;
        }
        answer(&dht, &request, Id20::new([0x33; 20]), None, &[]);
    }
    task.abort();
    assert!(grown);
    assert_eq!(searched, BTreeMap::from([(Duration::ZERO, 8), (slot, 1)]));
}

// A search is short while fewer than k nodes have answered it: the
// routers that answered it do not count.
#[tokio::test(start_paused = true)]
async fn test_routers_that_answer_do_not_make_a_search_complete() {
    use crate::core::hash_id::Id20;

    let (dht, mut sent) = fixture_dht();
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);
    let given = (
        Id20::new([0x21; 20]),
        SocketAddr::from(([10, 0, 1, 1], 6881)),
    );
    let named: Vec<(Id20, std::net::SocketAddrV4)> = (2..8u8)
        .map(|i| {
            (
                Id20::new([0x20 + i; 20]),
                std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
            )
        })
        .collect();
    let req = get_peers_search(&dht);
    let search = tokio::spawn(async move {
        req.search(true, [(Some(given.0), given.1)].into_iter())
            .await
    });
    for _ in 0..2 + named.len() {
        let request = sent.recv().await.unwrap();
        if request.addr == router {
            reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
        } else if request.addr == given.1 {
            reply(&dht, &request, given.0, &[], &[]);
        } else {
            let &(id, _) = named
                .iter()
                .find(|n| SocketAddr::from(n.1) == request.addr)
                .unwrap();
            reply(&dht, &request, id, &[], &[]);
        }
    }
    let outcome = search.await.unwrap().unwrap();
    assert_eq!(
        outcome,
        super::SearchOutcome {
            answered: 8,
            failed: 0,
            nodes: 7
        }
    );
    assert!(outcome.is_short());
}

// A node that answers and takes the place of a bad one, in a full bucket
// that cannot split, leaves the table as many nodes as before.
#[test]
fn test_a_node_taking_a_bad_ones_place_leaves_the_count_as_it_was() {
    use crate::dht::routing_table::InsertResult;

    let (dht, _sent) = fixture_state();
    // The farthest bucket full, and the next one, which holds our id.
    fill_deep_table(&dht, 1);
    let mut table = dht.routing_table_v4.write();
    let bad = id_in_bucket(dht.id, 0, 3);
    table.mark_outgoing_request(&bad, std::time::Instant::now());
    let addr = table.iter().find(|n| n.id() == bad).unwrap().addr();
    table.mark_error(&bad, addr);
    let full = 128 + 64;
    assert_eq!(table.len(), full);
    let new = id_in_bucket(dht.id, 0, 200);
    assert!(matches!(
        table.add_node(new, SocketAddr::from(([10, 1, 201, 1], 6881))),
        InsertResult::ReplacedBad(_)
    ));
    assert_eq!(table.iter().count(), full);
    assert_eq!(table.len(), full);
}

// A datagram too long for the buffer, which a socket carried through a
// tunnel reports as InvalidData, costs that datagram only: the DHT reads
// on and answers the next query.
#[tokio::test]
async fn test_a_datagram_that_does_not_fit_costs_only_that_datagram() {
    let (socket, incoming, mut sent) = queued_socket();
    let tasks = tokio_util::task::TaskTracker::new();
    let dht = DhtState::with_config(DhtConfig {
        bootstrap_addrs: Some(Vec::new()),
        socket: Some(socket),
        tasks: Some(tasks.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    tasks.close();

    let from = SocketAddr::from(([10, 0, 0, 1], 6881));
    incoming
        .send(Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the datagram did not fit",
        )))
        .unwrap();
    incoming
        .send(Ok((
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe".to_vec(),
            from,
        )))
        .unwrap();
    let answer = tokio::select! {
        answer = sent.recv() => answer.unwrap(),
        () = tasks.wait() => panic!("the DHT ended"),
    };
    let answer = bprotocol::deserialize_message::<ByteBufOwned>(&answer).unwrap();
    assert!(matches!(answer.kind, MessageKind::Response(_)));
    assert_eq!(answer.transaction_id.as_ref(), b"aa");
    dht.cancellation_token().cancel();
}

type Incoming = std::io::Result<(Vec<u8>, SocketAddr)>;

// A socket that reads what is sent into the returned sender, and sends
// into the returned receiver.
fn queued_socket() -> (
    Arc<dyn DatagramSocket>,
    tokio::sync::mpsc::UnboundedSender<Incoming>,
    tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
) {
    use crate::core::net::BoxFuture;

    struct Queued {
        incoming: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Incoming>>,
        sent: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    }
    impl DatagramSocket for Queued {
        fn bind_addr(&self) -> SocketAddr {
            (Ipv4Addr::LOCALHOST, 6881).into()
        }
        fn recv_from<'a>(
            &'a self,
            buf: &'a mut [u8],
        ) -> BoxFuture<'a, std::io::Result<(usize, SocketAddr)>> {
            Box::pin(async move {
                match self.incoming.lock().await.recv().await {
                    Some(Ok((datagram, from))) => {
                        buf[..datagram.len()].copy_from_slice(&datagram);
                        Ok((datagram.len(), from))
                    }
                    Some(Err(e)) => Err(e),
                    None => std::future::pending().await,
                }
            })
        }
        fn send_to<'a>(
            &'a self,
            buf: &'a [u8],
            _: SocketAddr,
        ) -> BoxFuture<'a, std::io::Result<usize>> {
            Box::pin(async move {
                let _ = self.sent.send(buf.to_vec());
                Ok(buf.len())
            })
        }
        fn poll_send_to(
            &self,
            _: &mut std::task::Context<'_>,
            buf: &[u8],
            _: SocketAddr,
        ) -> std::task::Poll<std::io::Result<usize>> {
            let _ = self.sent.send(buf.to_vec());
            std::task::Poll::Ready(Ok(buf.len()))
        }
    }

    let (incoming, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
    let (sent_tx, sent) = tokio::sync::mpsc::unbounded_channel();
    let socket = Arc::new(Queued {
        incoming: tokio::sync::Mutex::new(incoming_rx),
        sent: sent_tx,
    });
    (socket, incoming, sent)
}

// A dual-stack socket reports an IPv4 node's reply as coming from
// `[::ffff:a.b.c.d]`: it answers the request sent to `a.b.c.d`, and the
// node enters the IPv4 table, not the IPv6 one.
#[tokio::test]
async fn test_a_reply_from_a_mapped_source_answers_the_ipv4_request() {
    use crate::core::hash_id::Id20;

    let (socket, incoming, mut sent) = queued_socket();
    let tasks = tokio_util::task::TaskTracker::new();
    let dht = DhtState::with_config(DhtConfig {
        bootstrap_addrs: Some(Vec::new()),
        socket: Some(socket),
        tasks: Some(tasks.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    tasks.close();

    let node = SocketAddr::from(([10, 0, 0, 2], 6881));
    let id = Id20::new([0x44; 20]);
    let entered = dht.with_routing_tables(|v4, _| v4.entered());
    let mut entered = std::pin::pin!(entered.notified());
    entered.as_mut().enable();
    dht.add_node(node);
    let ping = tokio::select! {
        ping = sent.recv() => ping.unwrap(),
        () = tasks.wait() => panic!("the DHT ended"),
    };
    let ping = bprotocol::deserialize_message::<ByteBufOwned>(&ping).unwrap();
    assert!(matches!(ping.kind, MessageKind::PingRequest(_)));
    let mut reply = Vec::new();
    bprotocol::serialize_message(
        &mut reply,
        ping.transaction_id,
        None,
        None,
        MessageKind::Response(bprotocol::Response {
            id,
            ..Default::default()
        }),
    )
    .unwrap();
    incoming
        .send(Ok((reply, "[::ffff:10.0.0.2]:6881".parse().unwrap())))
        .unwrap();
    tokio::select! {
        () = entered => {}
        () = tasks.wait() => panic!("the DHT ended"),
    }
    dht.with_routing_tables(|v4, v6| {
        assert!(v4.iter().any(|n| n.id() == id && n.addr() == node));
        assert_eq!(v6.len(), 0);
    });
    dht.cancellation_token().cancel();
}

// Every reply that answers a request feeds the table, a get_peers one
// as a find_node one does (libtorrent rpc_manager.cpp:389, v2.0.15).
#[tokio::test(start_paused = true)]
async fn test_a_get_peers_reply_enters_the_table() {
    use crate::core::hash_id::Id20;

    let (dht, mut sent) = fixture_dht();
    let node = (
        Id20::new([0x21; 20]),
        SocketAddr::from(([10, 0, 1, 1], 6881)),
    );
    let req = get_peers_search(&dht);
    let mut search = std::pin::pin!(req.search(true, std::iter::once((Some(node.0), node.1))));
    let request = tokio::select! {
        request = sent.recv() => request.unwrap(),
        _ = search.as_mut() => panic!("the search ended unasked"),
    };
    assert_eq!(request.addr, node.1);
    answer(&dht, &request, node.0, Some(b"t"), &[]);
    let outcome = search.await.unwrap();
    assert_eq!(outcome.answered, 1);
    let table = dht.routing_table_v4.read();
    assert_eq!(table.len(), 1);
    assert!(
        table
            .iter()
            .any(|n| n.id() == node.0 && n.is_confirmed(std::time::Instant::now()))
    );
}

// An IPv6 search is never sent to an IPv4 address written as IPv6 that
// a node names in `nodes6`, and neither table hears of it; the IPv6 node
// named beside it is asked.
#[tokio::test(start_paused = true)]
async fn test_a_mapped_address_in_nodes6_is_never_asked() {
    use crate::core::hash_id::Id20;
    use std::net::SocketAddrV6;

    let (dht, mut sent) = fixture_dht();
    let first = (
        Id20::new([0x21; 20]),
        "[2001:db8:1::1]:6881".parse::<SocketAddr>().unwrap(),
    );
    let mapped: SocketAddrV6 = "[::ffff:5.6.7.8]:6881".parse().unwrap();
    let v6: SocketAddrV6 = "[2001:db8:2::1]:6881".parse().unwrap();
    let req = get_peers_search(&dht);
    let mut search = std::pin::pin!(req.search(false, std::iter::once((Some(first.0), first.1))));
    let request = tokio::select! {
        request = sent.recv() => request.unwrap(),
        _ = search.as_mut() => panic!("the search ended unasked"),
    };
    assert_eq!(request.addr, first.1);
    dht.on_received_message(
        bprotocol::Message {
            transaction_id: request.message.transaction_id.clone(),
            version: None,
            ip: None,
            kind: MessageKind::Response(bprotocol::Response {
                id: first.0,
                nodes6: Some(bprotocol::CompactNodeInfoOwned::new_from_iter(
                    [
                        bprotocol::Node {
                            id: Id20::new([0x22; 20]),
                            addr: mapped,
                        },
                        bprotocol::Node {
                            id: Id20::new([0x23; 20]),
                            addr: v6,
                        },
                    ]
                    .into_iter(),
                )),
                ..Default::default()
            }),
        },
        request.addr,
    )
    .unwrap();
    // The IPv6 node is asked; nothing else is, and the search ends as
    // it fails to answer.
    let mut asked = Vec::new();
    let outcome = loop {
        tokio::select! {
            request = sent.recv() => asked.push(request.unwrap().addr),
            outcome = search.as_mut() => break outcome.unwrap(),
        }
    };
    assert_eq!(asked, vec![SocketAddr::V6(v6)]);
    assert_eq!(outcome.answered, 1);
    dht.with_routing_tables(|v4, v6_table| {
        assert_eq!(v4.len() + v4.heard_of(), 0);
        assert!(
            v6_table
                .iter()
                .all(|n| !crate::dht::routing_table::is_v4_mapped(n.addr()))
        );
        assert_eq!(v6_table.heard_of(), 1);
    });
}

// A query from a node the table does not hold makes it heard of
// (libtorrent node.cpp:831-832, v2.0.15); it does not enter the table.
#[tokio::test(start_paused = true)]
async fn test_a_query_from_an_unknown_node_is_heard_of() {
    use crate::core::hash_id::Id20;

    let (dht, mut sent) = fixture_dht();
    dht.on_received_message(
        bprotocol::Message {
            transaction_id: ByteBufOwned::from(&b"aa"[..]),
            version: None,
            ip: None,
            kind: MessageKind::PingRequest(bprotocol::PingRequest {
                id: Id20::new([0x21; 20]),
            }),
        },
        SocketAddr::from(([10, 0, 1, 1], 6881)),
    )
    .unwrap();
    assert!(sent.recv().await.is_some());
    let table = dht.routing_table_v4.read();
    assert_eq!(table.len(), 0);
    assert_eq!(table.heard_of(), 1);
}

// libtorrent `node::tick` (node.cpp:647-716, v2.0.15): nodes heard of
// are asked into the table one per REFRESH_SPACING, the first at
// once, with a get_peers into their bucket. One that answers enters,
// and the nodes it names are heard of and asked in turn; once none is
// left, the tick refreshes the table's nodes. With an empty table and
// nothing heard of, nothing is sent.
#[tokio::test(start_paused = true)]
async fn test_nodes_heard_of_are_asked_one_per_tick() {
    use super::REFRESH_SPACING;
    use std::net::SocketAddrV4;

    let (dht, mut sent) = fixture_dht();
    let task = tokio::spawn(dht.clone().refresh_table(true));
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert!(sent.try_recv().is_err(), "sent with nothing heard of");

    let start = tokio::time::Instant::now();
    let a = (
        id_in_bucket(dht.id, 0, 0),
        SocketAddrV4::new([10, 1, 1, 1].into(), 6881),
    );
    let b = (
        id_in_bucket(dht.id, 1, 0),
        SocketAddrV4::new([10, 2, 1, 1].into(), 6881),
    );
    let c = (
        id_in_bucket(dht.id, 2, 0),
        SocketAddrV4::new([10, 3, 1, 1].into(), 6881),
    );
    {
        let mut table = dht.routing_table_v4.write();
        table.heard_about(a.0, a.1.into());
        table.heard_about(b.0, b.1.into());
    }
    let first = sent.recv().await.unwrap();
    assert_eq!(tokio::time::Instant::now() - start, Duration::ZERO);
    assert_eq!(first.addr, SocketAddr::V4(a.1));
    answer(&dht, &first, a.0, None, &[c]);

    let second = sent.recv().await.unwrap();
    assert_eq!(tokio::time::Instant::now() - start, REFRESH_SPACING);
    assert_eq!(second.addr, SocketAddr::V4(b.1));
    assert!(matches!(
        second.message.kind,
        MessageKind::GetPeersRequest(_)
    ));
    {
        let table = dht.routing_table_v4.read();
        assert_eq!(table.len(), 1);
        assert!(table.iter().any(|n| n.id() == a.0));
    }

    let third = sent.recv().await.unwrap();
    assert_eq!(tokio::time::Instant::now() - start, 2 * REFRESH_SPACING);
    assert_eq!(third.addr, SocketAddr::V4(c.1));

    // b and c never answer. With no node heard of left to ask, the next
    // tick refreshes the node of the table, a.
    let fourth = sent.recv().await.unwrap();
    assert_eq!(tokio::time::Instant::now() - start, 3 * REFRESH_SPACING);
    assert_eq!(fourth.addr, SocketAddr::V4(a.1));
    assert!(matches!(
        fourth.message.kind,
        MessageKind::GetPeersRequest(_)
    ));
    assert_eq!(dht.routing_table_v4.read().len(), 1);
    task.abort();
}

// A restart whose saved nodes are all stale: the routers, known only
// once the first round is under way, are asked in the round right after
// it, and the nodes they name that answer enter the table.
#[tokio::test(start_paused = true)]
async fn test_a_restart_from_stale_nodes_asks_the_routers_that_resolve_later() {
    use crate::core::hash_id::Id20;
    use crate::dht::RESPONSE_TIMEOUT;
    use crate::dht::routing_table::RoutingTable;
    use std::net::SocketAddrV4;

    let (saved, _sent) = fixture_state();
    add_table_nodes(&saved, 2, true);
    let json = serde_json::to_string(&*saved.routing_table_v4.read()).unwrap();
    let restored: RoutingTable = serde_json::from_str(&json).unwrap();
    let stale: Vec<SocketAddr> = restored.iter().map(|n| n.addr()).collect();
    assert_eq!(stale.len(), 2);

    let (tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
    let dht = std::sync::Arc::new(DhtState::new_internal(
        saved.id,
        tx,
        Some(restored),
        None,
        (Ipv4Addr::LOCALHOST, 0).into(),
        tokio_util::sync::CancellationToken::new(),
        None,
    ));
    dht.started_for_test();
    let start = tokio::time::Instant::now();
    let task = tokio::spawn(dht.clone().refresh_self(true));

    // Round one asks the saved nodes, which never answer; the router
    // becomes known while it waits on them.
    for _ in 0..2 {
        let request = sent.recv().await.unwrap();
        assert!(stale.contains(&request.addr));
    }
    let router = SocketAddr::from(ROUTER);
    dht.add_routers(&[router]);

    // Round two starts as round one ends and asks the router.
    let mut request = sent.recv().await.unwrap();
    while request.addr != router {
        assert!(stale.contains(&request.addr));
        request = sent.recv().await.unwrap();
    }
    assert_eq!(tokio::time::Instant::now() - start, RESPONSE_TIMEOUT);

    let named = [
        (
            Id20::new([0x31; 20]),
            SocketAddrV4::new([10, 0, 5, 1].into(), 6881),
        ),
        (
            Id20::new([0x32; 20]),
            SocketAddrV4::new([10, 0, 6, 1].into(), 6881),
        ),
    ];
    reply(&dht, &request, Id20::new([0x30; 20]), &named, &[]);
    let entered = dht.routing_table_v4.read().entered();
    let mut answered = 0;
    while answered < named.len() {
        let request = sent.recv().await.unwrap();
        if let Some(&(id, _)) = named.iter().find(|n| SocketAddr::V4(n.1) == request.addr) {
            let mut next = std::pin::pin!(entered.notified());
            next.as_mut().enable();
            reply(&dht, &request, id, &[], &[]);
            next.await;
            answered += 1;
        }
    }
    let table = dht.routing_table_v4.read();
    for (id, addr) in named {
        assert!(
            table
                .iter()
                .any(|n| n.id() == id && n.addr() == SocketAddr::V4(addr))
        );
    }
    drop(table);
    task.abort();
}
