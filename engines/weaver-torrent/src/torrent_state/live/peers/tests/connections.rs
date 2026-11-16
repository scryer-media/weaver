// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::*;

// Each connection's PEX message from the one shared map is one the
// map built afresh for it gives: over a seeded run of peers going live
// (connecting to us, or we to them over TCP or uTP), telling us where
// they listen, closing, leaving, their handshakes encrypted or not and
// their pieces whole or not, the shared map equals a fresh walk at
// every round and is shared by the round's connections; and every
// connection, local or not, with its own `told`, gets the peers the
// fresh map adds and drops for it, at most 50 of each, local peers
// hidden from one that is not local. Where more than 50 are new, the
// 50 sent are those the map's order gives first, as with a map built
// afresh, so the test checks they are 50 of the right ones.
#[tokio::test(start_paused = true)]
async fn each_pex_message_from_the_shared_map_is_the_one_a_fresh_map_gives() {
    use crate::torrent_state::live::pex::{MAX_SENT_PEX_PEERS, pex_diff};
    use holepunch::is_local;
    let host = |i: u64| -> SocketAddr {
        let i = u8::try_from(i % 120).unwrap();
        if i % 3 == 0 {
            SocketAddr::from(([10, 2, 0, i], 6881))
        } else {
            SocketAddr::from(([203, 0, 113, i], 6881))
        }
    };
    let to: Vec<SocketAddr> = vec![
        "192.168.1.1:6881".parse().unwrap(),
        "192.168.1.2:6881".parse().unwrap(),
        "10.9.0.1:6881".parse().unwrap(),
        "198.51.100.1:6881".parse().unwrap(),
        "198.51.100.2:6881".parse().unwrap(),
        "198.51.100.3:6881".parse().unwrap(),
    ];
    let (mut capped, mut uncapped, mut hidden, mut dropped_seen) = (0, 0, 0, 0);
    for seed in 0..8u64 {
        let mut next = xorshift(0xd1b5_4a32_d192_ed03 ^ seed);
        let peers = make_peer_states();
        let mut told: Vec<HashSet<SocketAddr>> = vec![HashSet::new(); to.len()];
        let mut keep_rx = Vec::new();
        let mut keep = Vec::new();
        let mut now = Instant::now();
        // A burst first, so the first round has more than 50 to add.
        for i in 0..90 {
            if let Ok(rx) = connected_to_us(&peers, host(i)) {
                peers.with_peer_mut(host(i).into(), "test", |peer| {
                    peer.listens_at(host(i), &peers)
                });
                keep_rx.push(rx);
            }
        }
        for step in 0..600 {
            let r = next();
            let a = host(r >> 8);
            match r % 8 {
                0 => {
                    if let Ok(rx) = connected_to_us(&peers, a) {
                        keep_rx.push(rx);
                        if r & 16 != 0 {
                            peers
                                .with_peer_mut(a.into(), "test", |peer| peer.listens_at(a, &peers));
                        }
                    }
                }
                1 => {
                    peers.add_peer(a, PeerSourceFlag::Tracker);
                    if let Some((handle, rx, tx)) = peers.connect_next(now) {
                        let kind = if r & 16 != 0 {
                            ConnectionKind::Utp
                        } else {
                            ConnectionKind::Tcp
                        };
                        peers.with_peer_mut(handle.into(), "test", |peer| {
                            peer.connecting_to_live(Id20::default(), &peers, kind);
                        });
                        keep.push((rx, tx));
                    }
                }
                2 => {
                    peers.with_peer_mut(a.into(), "test", |peer| {
                        if peer.connected_out().is_some() {
                            peers.connection_closed(peer, now, r & 16 != 0, false);
                        }
                    });
                }
                3 => {
                    peers.drop_peer(a.into());
                }
                4 => peers.handshake_encryption(a.into(), handshake(r & 16 != 0)),
                5 => {
                    let bitfield = if r & 16 != 0 {
                        full_bitfield()
                    } else {
                        empty_bitfield()
                    };
                    peers.update_bitfield(a.into(), bitfield);
                }
                _ => {
                    let mut shared: Option<PexListed> = None;
                    for (to, told) in to.iter().zip(&mut told) {
                        let listed = peers.pex_listed();
                        let fresh = peers.pex_listed_walked();
                        assert_eq!(*listed, fresh, "seed {seed} step {step}");
                        if let Some(shared) = &shared {
                            assert!(Arc::ptr_eq(shared, &listed), "seed {seed} step {step}");
                        }
                        shared = Some(listed.clone());

                        let before = told.clone();
                        let shown = |addr: &SocketAddr| is_local(to.ip()) || !is_local(addr.ip());
                        let may_add: HashMap<SocketAddr, u8> = fresh
                            .iter()
                            .filter(|(addr, _)| shown(addr) && !before.contains(*addr))
                            .map(|(&addr, &flags)| (addr, flags))
                            .collect();
                        let may_drop: HashSet<SocketAddr> = before
                            .iter()
                            .filter(|addr| !fresh.contains_key(*addr))
                            .copied()
                            .collect();
                        hidden += fresh.keys().filter(|addr| !shown(addr)).count();
                        let (added, dropped) = pex_diff(&listed, *to, told).unwrap_or_default();
                        assert_eq!(added.len(), may_add.len().min(MAX_SENT_PEX_PEERS));
                        assert_eq!(dropped.len(), may_drop.len().min(MAX_SENT_PEX_PEERS));
                        let added_at: HashSet<SocketAddr> = added.iter().map(|p| p.addr).collect();
                        assert_eq!(added_at.len(), added.len());
                        for p in &added {
                            assert_eq!(may_add.get(&p.addr), Some(&p.flags), "{seed} {step}");
                        }
                        for addr in &dropped {
                            assert!(may_drop.contains(addr), "seed {seed} step {step}");
                        }
                        let mut expected_told = before;
                        for addr in &dropped {
                            expected_told.remove(addr);
                        }
                        expected_told.extend(added_at);
                        assert_eq!(*told, expected_told, "seed {seed} step {step}");
                        if may_add.len() > MAX_SENT_PEX_PEERS {
                            capped += 1;
                        } else {
                            uncapped += 1;
                        }
                        dropped_seen += dropped.len();
                    }
                    now += MIN_RECONNECT_TIME;
                }
            }
        }
    }
    assert!(capped > 0, "no round had more than 50 to add");
    assert!(uncapped > 0, "no round had 50 or fewer to add");
    assert!(hidden > 0, "no round hid a local peer");
    assert!(dropped_seen > 0, "no round dropped a peer");
}

// libtorrent `torrent::get_peer_list_state`: a paused torrent's list
// holds `max_paused_peerlist_size`; those that go are the ones
// `erase_peers` forgets first.
#[tokio::test(start_paused = true)]
async fn a_paused_list_keeps_the_peers_that_rank_highest() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    // A full list: a live peer, queued peers that leave the paused
    // list room for 100 more, 150 failed once, and the rest failed out.
    let queued_from = MAX_PEERLIST_SIZE - MAX_PAUSED_PEERLIST_SIZE + 100;
    let failed_from = queued_from - 150;
    let most_failed = (0..failed_from).map(peer_addr).collect::<Vec<_>>();
    let failed = (failed_from..queued_from)
        .map(peer_addr)
        .collect::<Vec<_>>();
    let queued = (queued_from..MAX_PEERLIST_SIZE - 1)
        .map(peer_addr)
        .collect::<Vec<_>>();
    let live = peer_addr(MAX_PEERLIST_SIZE - 1);
    for &addr in &most_failed {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
        fail_out(&peers, addr, t0);
    }
    for &addr in &failed {
        failed_once(&peers, addr, t0);
    }
    for &addr in &queued {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
    }
    let _live_rx = connected_to_us(&peers, live).unwrap();
    assert_eq!(peers.listed(), MAX_PEERLIST_SIZE);

    let list = peers.idle_list();
    assert_eq!(list.peers.len(), MAX_PAUSED_PEERLIST_SIZE);
    let kept = list
        .peers
        .iter()
        .filter_map(|p| p.addr.addr())
        .collect::<HashSet<_>>();
    assert!(queued.iter().all(|addr| kept.contains(addr)));
    assert!(kept.contains(&live));
    assert!(most_failed.iter().all(|addr| !kept.contains(addr)));
    assert_eq!(
        failed.iter().filter(|addr| kept.contains(addr)).count(),
        MAX_PAUSED_PEERLIST_SIZE - queued.len() - 1
    );

    let next = PeerStates::founded_on(
        list,
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    assert_eq!(next.listed(), MAX_PAUSED_PEERLIST_SIZE);
}

#[test]
fn a_connected_peer_is_never_forgotten() {
    let peers = make_peer_states();
    let mut channels = Vec::new();
    for i in 0..MAX_PEERLIST_SIZE - 1 {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(
                peer_addr(i),
                Id20::default(),
                tx,
                ConnectionKind::Tcp,
                OUR_PORT,
            )
            .unwrap();
        channels.push(rx);
    }
    let candidate = peer_addr(MAX_PEERLIST_SIZE - 1);
    peers.add_peer(candidate, PeerSourceFlag::Tracker);

    // An incoming peer holding a slot pushes out the one idle peer.
    let first = peer_addr(MAX_PEERLIST_SIZE);
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    peers
        .add_incoming(first, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
        .unwrap();
    assert_eq!(state_of(&peers, candidate), None);
    assert_eq!(state_of(&peers, first), Some("live"));

    // Then the list holds connected peers only, and nobody gets in.
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let second = peer_addr(MAX_PEERLIST_SIZE + 1);
    assert_eq!(
        peers
            .add_incoming(second, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
            .unwrap_err(),
        IncomingRefused::ListFull
    );
    peers.add_peer(second, PeerSourceFlag::Tracker);
    assert_eq!(state_of(&peers, second), None);
    assert_eq!(peers.states.len(), MAX_PEERLIST_SIZE);
    assert_eq!(peers.stats().live as usize, MAX_PEERLIST_SIZE);
}

// libtorrent `torrent::peers_erased` tells `smart_ban_plugin::on_erase_peers`
// when entries leave the peer list, not when a connection closes; and
// `on_read_failed_block` records nothing of a peer the list does not
// know.
#[test]
fn the_smart_ban_records_of_a_peer_leave_the_list_with_it_and_outlive_its_connection() {
    use crate::core::{
        constants::CHUNK_SIZE,
        lengths::{ChunkInfo, Lengths},
    };
    use crate::piece_tracker::Senders;
    use crate::torrent_state::live::smart_ban::read_back;

    let lengths = Lengths::new(u64::from(CHUNK_SIZE) * 4, CHUNK_SIZE * 4).unwrap();
    let piece = lengths.validate_piece_index(0).unwrap();
    let peers = make_peer_states();
    let t0 = Instant::now();
    let (closed, mut closed_rx) = add_live_peer(&peers, "10.1.0.1:6881");
    let (erased, dropped, stranger) = (peer_addr(1), peer_addr(2), peer_addr(3));
    for addr in [erased, dropped] {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
    }
    let senders: Senders = [closed, erased, dropped, stranger]
        .map(PeerHandle::from)
        .map(Some)
        .into_iter()
        .collect();
    let zeros = |_, _: &ChunkInfo, buf: &mut [u8]| {
        buf.fill(0);
        Ok(())
    };
    let senders_recorded = |records: Vec<SentBlock>| {
        records
            .iter()
            .map(SentBlock::sender)
            .collect::<BTreeSet<_>>()
    };

    let failed = read_back(&lengths, piece, &senders, zeros);
    assert!(peers.piece_failed(piece, failed).is_empty());

    // The connection closes: the peer stays on the list.
    peers
        .with_peer_mut(closed.into(), "test", |peer| {
            peer.take_state(&peers);
            peers.connection_closed(peer, t0, false, false);
        })
        .unwrap();
    while closed_rx.try_recv().is_ok() {}
    // One peer is dropped, another pushed out of the full list.
    peers.drop_peer(dropped.into()).unwrap();
    fail_out(&peers, erased, t0);
    for i in 4.. {
        if peers.listed() == MAX_PEERLIST_SIZE {
            break;
        }
        peers.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
    }
    peers.add_peer(peer_addr(MAX_PEERLIST_SIZE + 4), PeerSourceFlag::Tracker);
    assert_eq!(state_of(&peers, erased), None);

    let failed = read_back(&lengths, piece, &senders, zeros);
    assert!(peers.piece_failed(piece, failed).is_empty());
    assert_eq!(
        senders_recorded(peers.piece_passed(piece)),
        BTreeSet::from([closed.into()])
    );
}

// libtorrent's smart-ban plugin lives with the torrent: its
// `m_block_hashes` cross `torrent::do_pause`, which only disconnects,
// and the next failure of the piece names the sender that sent other
// bytes before the pause.
#[test]
fn a_smart_ban_record_made_before_a_pause_names_its_sender_after_it() {
    use crate::core::{
        constants::CHUNK_SIZE,
        lengths::{ChunkInfo, Lengths},
    };
    use crate::piece_tracker::Senders;
    use crate::torrent_state::live::smart_ban::read_back;

    let lengths = Lengths::new(u64::from(CHUNK_SIZE) * 4, CHUNK_SIZE * 4).unwrap();
    let piece = lengths.validate_piece_index(0).unwrap();
    let peers = make_peer_states();
    let sender = peer_addr(1);
    peers.add_peer(sender, PeerSourceFlag::Tracker);
    let senders: Senders = [sender.into(); 4].map(Some).into_iter().collect();
    let filled = |byte: u8| {
        move |_, _: &ChunkInfo, buf: &mut [u8]| {
            buf.fill(byte);
            Ok(())
        }
    };
    let failed = read_back(&lengths, piece, &senders, filled(0));
    assert!(peers.piece_failed(piece, failed).is_empty());

    let next = PeerStates::founded_on(
        peers.idle_list(),
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    let failed = read_back(&lengths, piece, &senders, filled(1));
    assert_eq!(
        next.piece_failed(piece, failed),
        BTreeSet::from([sender.into()])
    );
}

// libtorrent `torrent::do_pause` disconnects every peer and keeps
// `torrent::m_peer_list`: the next run's list knows the banned
// addresses and each peer's trust and failures, and a peer we were
// connecting to is a connect candidate again.
#[test]
fn the_next_run_is_founded_on_the_list_a_pause_leaves() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    let banned = peer_addr(1);
    peers.ban(banned.ip());
    let failed = peer_addr(2);
    peers.add_peer(failed, PeerSourceFlag::Tracker);
    fail_at(&peers, failed, t0);
    peers
        .with_peer_mut(failed.into(), "test", |p| p.piece_failed(false, &peers))
        .unwrap();
    let connecting = peer_addr(3);
    peers.add_peer(connecting, PeerSourceFlag::Tracker);
    assert_eq!(
        peers.connect_next(t0).map(|(addr, ..)| addr),
        Some(connecting)
    );

    let next = PeerStates::founded_on(
        peers.idle_list(),
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    assert!(next.is_banned(banned.ip()));
    next.add_peer(banned, PeerSourceFlag::Tracker);
    assert_eq!(state_of(&next, banned), None);
    let standing = |list: &PeerStates| {
        list.with_peer(failed.into(), |p| (p.trust(), p.reconnect_at()))
            .unwrap()
    };
    assert_eq!(standing(&next), standing(&peers));
    assert!(
        next.with_peer(failed.into(), |p| p.trust().on_parole())
            .unwrap()
    );
    assert_eq!(state_of(&next, connecting), Some("queued"));
    assert_eq!(state_of(&peers, connecting), Some("connecting"));
    assert_eq!(next.stats().queued, 2);
    assert_eq!(next.stats().connecting, 0);
}

// A peer counts as connecting from the moment its dial begins until the
// dial ends, in the run's counts and the session's alike: each dial
// that fails leaves both, and a run that ends leaves none behind.
#[test]
fn a_dial_counts_as_connecting_until_it_ends() {
    let session: Arc<AggregatePeerStatsAtomic> = Default::default();
    let peers = PeerStates::founded_on(
        PeerList::default(),
        session.clone(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    let t0 = Instant::now();
    let dialled = [peer_addr(1), peer_addr(2)];
    for addr in dialled {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
    }
    let connecting = || (peers.stats().connecting, session.snapshot().connecting);
    assert_eq!(connecting(), (0, 0));
    for _ in dialled {
        assert!(peers.connect_next(t0).is_some());
    }
    assert_eq!(connecting(), (2, 2));

    peers
        .with_peer_mut(dialled[0].into(), "test", |peer| {
            peers.connection_closed(peer, t0, true, false);
        })
        .unwrap();
    assert_eq!(connecting(), (1, 1));

    drop(peers);
    assert_eq!(session.snapshot().connecting, 0);
}

// A pause ends the run, not the peer: a peer the pause disconnects
// starts no wait to reconnect, so the next run dials it at once, and a
// peer already idle keeps the wait it had.
#[tokio::test(start_paused = true)]
async fn a_pause_starts_no_reconnect_wait_for_the_peers_it_disconnects() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    let idle = peer_addr(1);
    peers.add_peer(idle, PeerSourceFlag::Tracker);
    fail_at(&peers, idle, t0);
    let connecting = peer_addr(2);
    peers.add_peer(connecting, PeerSourceFlag::Tracker);
    assert_eq!(
        peers.connect_next(t0).map(|(addr, ..)| addr),
        Some(connecting)
    );
    let live = peer_addr(3);
    let _live_rx = connected_to_us(&peers, live).unwrap();
    let idle_wait = peers.with_peer(idle.into(), |p| p.reconnect_at()).unwrap();

    let paused_at = t0 + Duration::from_secs(10);
    peers.close_all(paused_at);
    let next = PeerStates::founded_on(
        peers.idle_list(),
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    let reconnect_at = |addr| next.with_peer(addr, |p| p.reconnect_at()).unwrap();
    assert_eq!(reconnect_at(connecting.into()), Some(paused_at));
    assert_eq!(reconnect_at(live.into()), Some(paused_at));
    assert_eq!(reconnect_at(idle.into()), idle_wait);
    assert_eq!(idle_wait, Some(t0 + MIN_RECONNECT_TIME * 2));
}

// A pause of a finished torrent lets every peer it disconnects go
// without the wait to reconnect, seed and leecher alike. A run that
// resumes finished does not connect to the seeds.
#[tokio::test(start_paused = true)]
async fn a_pause_of_a_finished_torrent_starts_no_wait_for_its_peers() {
    let peers = make_peer_states();
    let seed = peer_addr(1);
    let leecher = peer_addr(2);
    let t0 = Instant::now();
    for addr in [seed, leecher] {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
        peers
            .with_peer_mut(addr.into(), "test", |peer| {
                peer.connect_if_due(t0, &peers).unwrap();
                assert!(
                    peer.connecting_to_live(Id20::default(), &peers, ConnectionKind::Tcp)
                        .is_some()
                );
            })
            .unwrap();
    }
    peers.update_bitfield(seed.into(), full_bitfield()).unwrap();
    peers
        .update_bitfield(leecher.into(), empty_bitfield())
        .unwrap();

    let paused_at = t0 + Duration::from_secs(10);
    peers.close_all(paused_at);
    let next = PeerStates::founded_on(
        peers.idle_list(),
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    let reconnect_at = |addr| next.with_peer(addr, |p| p.reconnect_at()).unwrap();
    assert_eq!(reconnect_at(seed.into()), Some(paused_at));
    assert_eq!(reconnect_at(leecher.into()), Some(paused_at));

    for mut peer in next.states.iter_mut() {
        peer.value_mut().finished(true, &next);
    }
    assert_eq!(state_of(&next, seed), Some("not needed"));
    assert_eq!(state_of(&next, leecher), Some("queued"));
}

// libtorrent `peer_list::set_seed`, as the pause closes a seed's
// connection: the list the pause leaves knows the peer only uploads,
// so a finished torrent does not connect to it again
// (`is_connect_candidate`).
#[tokio::test(start_paused = true)]
async fn a_seed_live_at_the_pause_stays_upload_only() {
    let peers = make_peer_states();
    let (seed, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
    peers.update_bitfield(seed.into(), full_bitfield()).unwrap();
    assert_eq!(peers.with_peer(seed.into(), |p| p.needed(true)), Some(true));

    peers.close_all(Instant::now());
    let next = PeerStates::founded_on(
        peers.idle_list(),
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    assert_eq!(next.with_peer(seed.into(), |p| p.needed(true)), Some(false));
    assert_eq!(next.with_peer(seed.into(), |p| p.needed(false)), Some(true));
}

// libtorrent `peer_list::is_connect_candidate`, `(p.seed ||
// p.upload_only) && m_finished`: a complete torrent whose list holds only
// seeds opens no connection, however a seed comes to be idle: brought
// into a new run, named again, or let go and wanted again. Once we want
// pieces again, the seeds are candidates again.
#[tokio::test(start_paused = true)]
async fn a_finished_torrent_with_only_seeds_on_its_list_opens_no_connection() {
    let peers = make_peer_states();
    let (one, two) = (peer_addr(1), peer_addr(2));
    let t0 = Instant::now();
    for seed in [one, two] {
        peers.add_peer(seed, PeerSourceFlag::Tracker);
        peers
            .with_peer_mut(seed.into(), "test", |peer| {
                peer.connect_if_due(t0, &peers).unwrap();
                assert!(
                    peer.connecting_to_live(Id20::default(), &peers, ConnectionKind::Tcp)
                        .is_some()
                );
            })
            .unwrap();
        peers.update_bitfield(seed.into(), full_bitfield()).unwrap();
    }
    peers.close_all(t0);

    let next = PeerStates::founded_on(
        peers.idle_list(),
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    next.set_finished(true);
    let now = Instant::now();
    let quiet = |next: &PeerStates| {
        assert_eq!(state_of(next, one), Some("not needed"));
        assert_eq!(state_of(next, two), Some("not needed"));
        assert_eq!(next.next_candidate(now), NextCandidate::None);
        assert!(next.connect_next(now).is_none());
        assert_eq!(next.stats().connecting, 0);
    };
    quiet(&next);

    next.add_peer(one, PeerSourceFlag::Tracker);
    next.add_peer(two, PeerSourceFlag::Pex);
    quiet(&next);
    next.requeue_not_needed();
    quiet(&next);

    next.set_finished(false);
    assert_eq!(state_of(&next, one), Some("queued"));
    assert_eq!(state_of(&next, two), Some("queued"));
    assert!(next.connect_next(now).is_some());
}

fn at_port(addr: SocketAddr, port: u16) -> SocketAddr {
    SocketAddr::new(addr.ip(), port)
}

// libtorrent `peer_list::apply_ip_filter`: a refused peer with no
// connection is erased and counted out; one with a connection is asked
// to close and is erased once it has; a web seed is on no list.
#[test]
fn a_refused_peer_leaves_the_list_once_it_has_no_connection() {
    let peers = make_peer_states();
    let (idle, other) = (peer_addr(1), peer_addr(2));
    peers.add_peer(idle, PeerSourceFlag::Tracker);
    peers.add_peer(other, PeerSourceFlag::Tracker);
    let (live, mut live_rx) = add_live_peer(&peers, "10.0.0.3:50000");
    // A web seed is at no address, so the filter is never asked of it.
    let seed = PeerHandle::WebSeed(1);
    peers.admit_web_seed(seed).unwrap();
    let refused = |addr: SocketAddr| addr == idle || addr == live;
    peers.refuse(refused);
    assert_eq!(state_of(&peers, idle), None);
    assert_eq!(state_of(&peers, other), Some("queued"));
    assert_eq!(state_of(&peers, live), Some("live"));
    assert!(matches!(
        live_rx.try_recv(),
        Ok(WriterRequest::Disconnect(Ok(())))
    ));
    assert_eq!(
        peers.with_peer(seed, |peer| peer.get_state().name()),
        Some("connecting")
    );

    // Its connection closed, the refused peer goes too.
    peers
        .with_peer_mut(live.into(), "test", |peer| {
            peer.take_state(&peers);
            peers.connection_closed(peer, Instant::now(), false, false);
        })
        .unwrap();
    peers.refuse_at(live.into(), refused);
    assert_eq!(state_of(&peers, live), None);
    peers.refuse_at(other.into(), refused);
    assert_eq!(state_of(&peers, other), Some("queued"));
    assert_eq!(peers.stats().live, 0);
}

// A private torrent's list, once a tracker answers, keeps the peers a
// tracker or the operator named, whoever else named them too; the
// DHT's, local discovery's, PEX's and incoming ones leave, an idle one
// at once, a connected one once its connection, asked to close, has
// closed. It happens once.
#[test]
fn a_trackers_answer_drops_the_peers_no_tracker_or_operator_named() {
    let operator = peer_addr(1);
    let peers = PeerStates::founded_on(
        PeerList::resumed([operator], []).taking_trackers_alone(false),
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    let (tracker, dht, lsd, pex, both) = (
        peer_addr(2),
        peer_addr(3),
        peer_addr(4),
        peer_addr(5),
        peer_addr(6),
    );
    peers.add_peer(tracker, PeerSourceFlag::Tracker);
    peers.add_peer(dht, PeerSourceFlag::Dht);
    peers.add_peer(lsd, PeerSourceFlag::Lsd);
    peers.add_peer(pex, PeerSourceFlag::Pex);
    peers.add_peer(both, PeerSourceFlag::Dht);
    peers.add_peer(both, PeerSourceFlag::Tracker);
    let (incoming, mut incoming_rx) = add_live_peer(&peers, "10.0.1.1:50000");
    assert!(peers.owes_untracked());

    peers.drop_untracked();
    assert!(!peers.owes_untracked());
    for gone in [dht, lsd, pex] {
        assert_eq!(state_of(&peers, gone), None, "{gone}");
    }
    for kept in [operator, tracker, both] {
        assert_eq!(state_of(&peers, kept), Some("queued"), "{kept}");
    }
    assert_eq!(state_of(&peers, incoming), Some("live"));
    assert!(matches!(
        incoming_rx.try_recv(),
        Ok(WriterRequest::Disconnect(Ok(())))
    ));
    // Neither the next run's list nor a restart's keeps it meanwhile.
    assert!(!peers.idle_list().addrs().contains(&incoming));
    assert!(!peers.resume_peers().peers.contains(&incoming));

    // Its connection closed, the incoming peer goes too.
    peers
        .with_peer_mut(incoming.into(), "test", |peer| {
            peer.take_state(&peers);
            peers.connection_closed(peer, Instant::now(), false, false);
        })
        .unwrap();
    peers.untracked_closed(incoming.into());
    assert_eq!(state_of(&peers, incoming), None);

    // Once only: a peer the DHT names later is not dropped by a second
    // answer.
    peers.add_peer(dht, PeerSourceFlag::Dht);
    peers.drop_untracked();
    assert_eq!(state_of(&peers, dht), Some("queued"));
    assert!(!peers.idle_list().owes_untracked());
}

// A connected peer asked to close because no tracker named it stays
// when a tracker names it before the connection has closed.
#[test]
fn a_peer_a_tracker_names_while_it_closes_stays() {
    let peers = PeerStates::founded_on(
        PeerList::default().taking_trackers_alone(false),
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    let (incoming, _incoming_rx) = add_live_peer(&peers, "10.0.1.1:50000");
    peers.drop_untracked();
    peers.add_peer(incoming, PeerSourceFlag::Tracker);
    peers
        .with_peer_mut(incoming.into(), "test", |peer| {
            peer.take_state(&peers);
            peers.connection_closed(peer, Instant::now(), false, false);
        })
        .unwrap();
    peers.untracked_closed(incoming.into());
    assert!(state_of(&peers, incoming).is_some());
}

// The list a private magnet's fetch leaves: when a tracker has answered
// already, the peers neither a tracker nor the operator named leave it
// now; when none has, all stay, owed to leave at the first answer. The
// owed drop crosses a pause.
#[test]
fn a_fetched_list_takes_trackers_alone_now_or_at_the_first_answer() {
    let fetched = || {
        let peers =
            PeerStates::founded_on(PeerList::resumed([peer_addr(1)], []), Default::default(), 0);
        peers.add_peer(peer_addr(2), PeerSourceFlag::Tracker);
        peers.add_peer(peer_addr(3), PeerSourceFlag::Dht);
        peers.add_peer(peer_addr(4), PeerSourceFlag::Lsd);
        peers.idle_list()
    };
    let mut now = fetched().taking_trackers_alone(true).addrs();
    now.sort();
    assert_eq!(now, [peer_addr(1), peer_addr(2)]);
    assert!(!fetched().taking_trackers_alone(true).owes_untracked());

    let later = fetched().taking_trackers_alone(false);
    assert_eq!(later.addrs().len(), 4);
    let paused = PeerStates::founded_on(later, Default::default(), 0).idle_list();
    assert!(paused.owes_untracked());
    let next = PeerStates::founded_on(paused, Default::default(), 0);
    next.drop_untracked();
    assert_eq!(state_of(&next, peer_addr(3)), None);
    assert_eq!(state_of(&next, peer_addr(2)), Some("queued"));
}

#[test]
fn a_banned_address_is_disconnected_and_never_admitted_again_on_any_port() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    let banned = peer_addr(1);
    let (live, mut live_rx) = add_live_peer(&peers, &at_port(banned, 50000).to_string());
    peers.add_peer(banned, PeerSourceFlag::Tracker);
    let other = peer_addr(2);
    peers.add_peer(other, PeerSourceFlag::Tracker);

    peers.ban(live.ip());

    // The connection is closed; the candidate at the address is one no
    // longer; a peer at another address is untouched.
    assert!(matches!(
        live_rx.try_recv(),
        Ok(WriterRequest::Disconnect(Err(_)))
    ));
    assert_eq!(state_of(&peers, banned), Some("dead"));
    assert_eq!(state_of(&peers, other), Some("queued"));

    // The closed connection leaves the peer Dead, needed or not.
    peers
        .with_peer_mut(live.into(), "test", |peer| {
            peer.take_state(&peers);
            peers.connection_closed(peer, t0, true, false);
        })
        .unwrap();
    assert_eq!(state_of(&peers, live), Some("dead"));

    // Named again or connecting from any port, the address is turned away.
    peers.add_peer(banned, PeerSourceFlag::Tracker);
    peers.add_peer(at_port(banned, 6882), PeerSourceFlag::Tracker);
    peers.add_refused_incoming(at_port(banned, 6883), ConnectionKind::Utp);
    assert_eq!(state_of(&peers, banned), Some("dead"));
    assert_eq!(state_of(&peers, at_port(banned, 6882)), None);
    assert_eq!(state_of(&peers, at_port(banned, 6883)), None);
    for addr in [live, at_port(banned, 50001)] {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        assert_eq!(
            peers
                .add_incoming(addr, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
                .unwrap_err(),
            IncomingRefused::Banned
        );
    }
    peers.requeue_not_needed();
    let much_later = t0 + Duration::from_secs(365 * 24 * 3600);
    assert_eq!(
        peers.connect_next(much_later).map(|(addr, ..)| addr),
        Some(other)
    );
    assert!(peers.connect_next(much_later).is_none());
}

#[test]
fn an_incoming_peer_refused_a_slot_is_kept() {
    let peers = make_peer_states();
    let t0 = Instant::now();

    // A uTP peer connects from where it listens: it is queued to be
    // connected back.
    let utp = peer_addr(1);
    peers.add_refused_incoming(utp, ConnectionKind::Utp);
    assert_eq!(state_of(&peers, utp), Some("queued"));
    assert_eq!(peers.next_candidate(t0), NextCandidate::Due);
    let incoming = peers.with_peer(utp.into(), |peer| {
        peer.stats
            .counters
            .incoming_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    });
    assert_eq!(incoming, Some(1));

    // A known peer that connects is named again, as incoming, which
    // forgives no failure: a dropped one stays dropped.
    let known = peer_addr(2);
    peers.add_peer(known, PeerSourceFlag::Tracker);
    fail_out(&peers, known, t0);
    assert_eq!(state_of(&peers, known), Some("dead"));
    peers.add_refused_incoming(known, ConnectionKind::Tcp);
    assert_eq!(state_of(&peers, known), Some("dead"));

    // An unknown TCP peer connects from an ephemeral port: there is no
    // address to connect back to.
    let tcp = peer_addr(3);
    peers.add_refused_incoming(tcp, ConnectionKind::Tcp);
    assert_eq!(state_of(&peers, tcp), None);
}

// libtorrent `bt_peer_connection::on_holepunch`, `connect`: the peer is
// added or found and connected to at once (`torrent::connect_to_peer`),
// over uTP, whatever its failures, unless it is connected or banned;
// holepunch mode ends with the connection.
#[test]
fn a_holepunched_peer_is_connected_to_next_whatever_its_failures() {
    use crate::stream_connect::Transports;
    let peers = make_peer_states();
    let t0 = Instant::now();
    let fresh = peer_addr(1);
    let failed = peer_addr(2);
    peers.add_peer(fresh, PeerSourceFlag::Tracker);
    peers.add_peer(failed, PeerSourceFlag::Tracker);
    let last = fail_out(&peers, failed, t0);
    assert_eq!(state_of(&peers, failed), Some("dead"));

    peers.holepunch(failed);
    assert_eq!(state_of(&peers, failed), Some("queued"));
    assert_eq!(peers.next_candidate(last), NextCandidate::Due);
    assert_eq!(
        peers.connect_next(last).map(|(addr, ..)| addr),
        Some(failed)
    );
    assert_eq!(state_of(&peers, failed), Some("connecting"));

    // The holepunched connection goes over uTP alone, and fails too:
    // the peer is out again, and may be dialed over TCP once more.
    peers
        .with_peer_mut(failed.into(), "test", |peer| {
            assert!(peer.holepunched());
            assert_eq!(peer.transports(), Transports::Utp);
            peers.connection_closed(peer, last, true, false);
            assert!(!peer.holepunched());
            assert_eq!(peer.transports(), Transports::Any);
        })
        .unwrap();
    assert_eq!(state_of(&peers, failed), Some("dead"));

    // A peer we do not know joins, due now.
    let unknown = peer_addr(3);
    peers.holepunch(unknown);
    assert_eq!(state_of(&peers, unknown), Some("queued"));
    assert_eq!(
        peers.connect_next(last).map(|(addr, ..)| addr),
        Some(unknown)
    );

    // A connected peer, or a banned one, is left as it is.
    let (live, _rx) = add_live_peer(&peers, "10.0.1.1:6881");
    peers.holepunch(live);
    assert_eq!(state_of(&peers, live), Some("live"));
    let banned = peer_addr(4);
    peers.ban(banned.ip());
    peers.holepunch(banned);
    assert_eq!(state_of(&peers, banned), None);
}

// libtorrent `ut_pex_plugin` keeps what a connection's PEX named, the
// added less the dropped, at most `max_pex_peers` of each family;
// `torrent::find_introducer` asks a connected holepunch peer whose PEX
// named the address.
#[test]
fn the_introducer_is_a_holepunch_peer_whose_pex_named_the_address() {
    let named_by =
        |addrs: Vec<SocketAddr>| addrs.into_iter().map(|addr| PexPeerInfo { flags: 0, addr });
    let peers = make_peer_states();
    let target = peer_addr(1);
    let (named, _named_rx) = add_live_peer(&peers, "10.0.1.1:6881");
    let (speaks, _speaks_rx) = add_live_peer(&peers, "10.0.1.2:6881");
    peers.with_live_mut(named.into(), "test", |live| {
        live.pex_named(named_by(vec![target]), std::iter::empty())
    });
    assert!(peers.introducer(target).is_none());

    peers.with_live_mut(speaks.into(), "test", |live| {
        live.supports_holepunch = true;
        live.pex_named(named_by(vec![target]), std::iter::empty());
    });
    assert!(peers.introducer(target).is_some());
    peers.with_live_mut(speaks.into(), "test", |live| {
        live.pex_named(std::iter::empty(), [target].into_iter())
    });
    assert!(peers.introducer(target).is_none());

    let v4 = (0..300).map(peer_addr);
    let v6 = (0..300).map(|i| SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, i], 6881)));
    peers.with_live_mut(speaks.into(), "test", |live| {
        // Each family takes peers while it has room; the news is what it took.
        let news = live.pex_named(named_by(v4.chain(v6).collect()), std::iter::empty());
        assert_eq!(news.len(), 2 * MAX_PEX_PEERS);
        let again = live.pex_named(named_by(vec![peer_addr(0)]), std::iter::empty());
        assert!(again.is_empty());
        let kept = |v4| {
            (0..300)
                .filter(|&i| {
                    let addr = if v4 {
                        peer_addr(i)
                    } else {
                        SocketAddr::from((
                            [
                                0x2001,
                                0xdb8,
                                0,
                                0,
                                0,
                                0,
                                0,
                                u16::try_from(i & 0xffff).unwrap(),
                            ],
                            6881,
                        ))
                    };
                    live.was_introduced(addr)
                })
                .count()
        };
        assert_eq!((kept(true), kept(false)), (MAX_PEX_PEERS, MAX_PEX_PEERS));
    });
}

fn incoming(
    peers: &PeerStates,
    addr: SocketAddr,
    our_port: u16,
) -> Result<PeerRx, IncomingRefused> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    peers
        .add_incoming(addr, Id20::default(), tx, ConnectionKind::Tcp, our_port)
        .map(|_| rx)
}

fn told_to_close(rx: &mut PeerRx) -> bool {
    matches!(rx.try_recv(), Ok(WriterRequest::Disconnect(Ok(()))))
}

// libtorrent `peer_list::new_connection`: a peer we are connecting to
// connects to us too. Both ends keep the connection made by the end
// with the lower listen port; the other is closed.
#[test]
fn a_connection_from_a_peer_we_dial_is_settled_by_listen_port() {
    let peers = make_peer_states();
    let dialed = peer_addr(1);
    peers.add_peer(dialed, PeerSourceFlag::Tracker);
    let (_, mut dialing, _) = peers.connect_next(Instant::now()).unwrap();

    // Our port is the lower: our connection stays, theirs is refused.
    let from = at_port(dialed, 50000);
    assert_eq!(
        incoming(&peers, from, 6000).unwrap_err(),
        IncomingRefused::AlreadyActive
    );
    assert!(dialing.try_recv().is_err());

    // Theirs is the lower: their connection stays, ours is closed.
    assert!(incoming(&peers, from, 7000).is_ok());
    assert!(told_to_close(&mut dialing));
}

// libtorrent `peer_list::new_connection`: when a peer connected to us
// already, a second connection from its address is dropped.
#[test]
fn a_second_connection_from_an_address_that_reached_us_is_refused() {
    let peers = make_peer_states();
    let mut first = incoming(&peers, at_port(peer_addr(1), 50000), OUR_PORT).unwrap();
    assert_eq!(
        incoming(&peers, at_port(peer_addr(1), 50001), OUR_PORT).unwrap_err(),
        IncomingRefused::AlreadyActive
    );
    assert!(first.try_recv().is_err());
    assert!(incoming(&peers, at_port(peer_addr(2), 50001), OUR_PORT).is_ok());
}

// libtorrent's peer list holds one entry per address, and one with a
// connection is no connect candidate: a peer at an address a connection
// is at waits for it to close.
#[test]
fn a_peer_at_a_connected_address_is_dialed_once_the_connection_closes() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    let listening = peer_addr(1);
    peers.add_peer(listening, PeerSourceFlag::Tracker);
    peers.add_peer(peer_addr(2), PeerSourceFlag::Tracker);
    let from = at_port(listening, 50000);
    let _rx = incoming(&peers, from, OUR_PORT).unwrap();
    assert_eq!(peers.connect_candidates(), 1);
    assert_eq!(
        peers.connect_next(t0).map(|(addr, ..)| addr),
        Some(peer_addr(2))
    );
    assert!(peers.connect_next(t0).is_none());
    assert_eq!(peers.next_candidate(t0), NextCandidate::None);

    peers
        .with_peer_mut(from.into(), "test", |peer| {
            peer.take_state(&peers);
            peers.connection_closed(peer, t0, false, false);
        })
        .unwrap();
    assert_eq!(peers.connect_candidates(), 1);
    assert_eq!(
        peers.connect_next(t0).map(|(addr, ..)| addr),
        Some(listening)
    );
}

// libtorrent reads `allow_multiple_connections_per_ip` into each
// `torrent::get_peer_list_state`, so turning it on lets a peer the gate
// held back be dialed at once, the connection at its address still
// open; the connect loop is woken to look again.
#[test]
fn turning_multiple_connections_per_ip_on_dials_the_held_peer_at_once() {
    use futures::FutureExt;

    let allow = Arc::new(AtomicBool::new(false));
    let peers = make_peer_states().allowing_multiple_connections_per_ip(Arc::clone(&allow));
    let t0 = Instant::now();
    let a = peer_addr(1);
    let held = at_port(a, 6882);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    peers.add_peer(held, PeerSourceFlag::Tracker);
    let (first, ..) = peers.connect_next(t0).unwrap();
    assert!(first == a || first == held);
    assert!(peers.connect_next(t0).is_none());
    // Drain the wake the dial left.
    let _ = peers.candidates_changed().now_or_never();

    allow.store(true, Ordering::Relaxed);
    peers.connections_per_ip_changed();
    assert!(peers.candidates_changed().now_or_never().is_some());
    let (second, ..) = peers.connect_next(t0).unwrap();
    assert_ne!(second, first);
    assert_eq!(second.ip(), a.ip());
}

// libtorrent `allow_multiple_connections_per_ip`: when it is on, peers
// at one address each have a connection.
#[test]
fn a_list_allowing_multiple_connections_per_ip_dials_each_peer() {
    let peers =
        make_peer_states().allowing_multiple_connections_per_ip(Arc::new(AtomicBool::new(true)));
    let t0 = Instant::now();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    peers.add_peer(at_port(a, 6882), PeerSourceFlag::Tracker);
    let _rx = incoming(&peers, at_port(a, 50000), OUR_PORT).unwrap();
    assert!(incoming(&peers, at_port(a, 50001), OUR_PORT).is_ok());
    assert!(peers.connect_next(t0).is_some());
    assert!(peers.connect_next(t0).is_some());
}

// libtorrent `torrent::disconnect_peers` with `compare_disconnect_peer`:
// a peer we are not interested in goes first, then one that is no seed,
// then one on parole, then the one that sent least per second connected.
#[test]
fn turnover_closes_the_connections_worth_least_first() {
    let peers = make_peer_states();
    let now = Instant::now();
    let mut rxs = Vec::new();
    let mut addrs = Vec::new();
    for i in 1..=5 {
        let (addr, rx) = add_live_peer(&peers, &format!("10.0.0.{i}:6881"));
        addrs.push(addr);
        rxs.push(rx);
    }
    let [seed, uninteresting, fast, slow, on_parole] = addrs[..] else {
        unreachable!()
    };
    for addr in [seed, fast, slow, on_parole] {
        peers.with_live_mut(addr.into(), "test", |live| live.interest(true, now));
    }
    peers.update_bitfield(seed.into(), full_bitfield()).unwrap();
    for (addr, fetched) in [(fast, 1000), (slow, 10), (on_parole, 1000)] {
        peers.with_peer(addr.into(), |peer| {
            peer.stats
                .counters
                .fetched_bytes
                .fetch_add(fetched, Ordering::Relaxed)
        });
    }
    peers.with_peer_mut(on_parole.into(), "test", |peer| {
        peer.piece_failed(false, &peers)
    });

    let order = [uninteresting, on_parole, slow, fast, seed];
    for n in 1..=order.len() {
        peers.disconnect_lowest(n, now);
        for (addr, rx) in addrs.iter().zip(rxs.iter_mut()) {
            assert_eq!(
                told_to_close(rx),
                order[..n].contains(addr),
                "{addr} of {n}"
            );
        }
    }
}

/// How an outgoing connection in [`attempt_at`] ends.
#[derive(Clone, Copy)]
enum Ends {
    /// Its handshake completes, and we close it later.
    Completed,
    /// Its handshake does not complete: the peer refuses it.
    Failed,
    /// We close it before its handshake completes.
    Closed,
}

// The handshake a connection completed: RC4-encrypted when `encrypted`.
fn handshake(encrypted: bool) -> Encryption {
    if encrypted {
        Encryption::Rc4
    } else {
        Encryption::None
    }
}

// Connects to `addr`, due at `at`, where encryption is preferred: asks
// which handshake the connection tries, which ends as `ends` says, at
// `at`. Returns whether it tried the encrypted handshake.
fn attempt_at(peers: &PeerStates, addr: SocketAddr, at: Instant, ends: Ends) -> bool {
    peers
        .with_peer_mut(addr.into(), "test", |peer| {
            peer.connect_if_due(at, peers).expect("a due candidate");
            let encrypted = peer.encrypt_outgoing();
            if let Ends::Completed = ends {
                peer.connecting_to_live(Id20::default(), peers, ConnectionKind::Tcp);
                peer.handshake_encryption(handshake(encrypted), peers);
            }
            peers.connection_closed(peer, at, matches!(ends, Ends::Failed), false);
            encrypted
        })
        .unwrap()
}

// When the peer is due again after a connection closed at `at`.
fn due_after(peers: &PeerStates, at: Instant) -> Instant {
    match peers.next_candidate(at) {
        NextCandidate::Due => at,
        NextCandidate::At(due) => due,
        NextCandidate::None => panic!("no candidate"),
    }
}

// libtorrent `bt_peer_connection::on_connected` and
// `peer_connection::fast_reconnect`: an encrypted handshake that fails
// leaves `pe_support` pointing at plaintext, and the next connection
// tries it at once, not held by the wait to reconnect. A plaintext one
// that fails waits as any failure does, and points back at encryption.
#[tokio::test(start_paused = true)]
async fn a_failed_encrypted_handshake_is_followed_at_once_by_a_plaintext_one() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    let t0 = Instant::now();

    assert!(attempt_at(&peers, a, t0, Ends::Failed));
    assert_eq!(peers.next_candidate(t0), NextCandidate::Due);
    assert!(!attempt_at(&peers, a, t0, Ends::Failed));
    assert_eq!(
        peers.next_candidate(t0),
        NextCandidate::At(t0 + reconnect_wait(2))
    );
    assert!(attempt_at(
        &peers,
        a,
        t0 + reconnect_wait(2),
        Ends::Completed
    ));
}

// libtorrent `peer_list::compare_peer` puts the fewest failures first,
// then the connection that closed longest ago, a peer never connected
// to first of all. An encrypted handshake the peer closes counts no
// failure (`peer_connection::disconnect` at `normal`; see
// `crate::Error::is_failure`) and its close is dated back
// (`peer_connection::fast_reconnect`), so in a session past its first
// 180 seconds the plaintext connection that follows comes before every
// candidate that failed, behind only those never connected to, and is
// made. One the peer answers with no verification constant counts
// (`peer_error`) and goes behind every candidate that did not fail.
// Peers that tie on all that, all named by a tracker, go by BEP 40 rank.
#[tokio::test(start_paused = true)]
async fn an_encrypted_handshake_the_peer_closed_is_retried_plain_before_failed_candidates() {
    use crate::mse::MseError;
    let peers = make_peer_states();
    let t0 = Instant::now() + Duration::from_secs(3600);
    let fresh: Vec<_> = (10..13).map(peer_addr).collect();
    let failed: Vec<_> = (20..30).map(peer_addr).collect();
    let (closed, unsynced) = (peer_addr(1), peer_addr(2));
    for &addr in &failed {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
        fail_at(&peers, addr, t0 - Duration::from_secs(600));
    }
    for &addr in fresh.iter().chain([&closed, &unsynced]) {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
    }
    let ended = |addr, error: crate::Error| {
        peers
            .with_peer_mut(addr, "test", |peer| {
                peer.connect_if_due(t0, &peers).expect("a due candidate");
                assert!(peer.encrypt_outgoing());
                peers.connection_closed(peer, t0, error.is_failure(), false);
            })
            .unwrap()
    };
    ended(
        closed.into(),
        MseError::Closed {
            stage: "reading the peer's public key",
            source: None,
        }
        .into(),
    );
    ended(
        unsynced.into(),
        MseError::NotSynchronised {
            stage: "looking for the peer's verification constant",
        }
        .into(),
    );

    let mut order = Vec::new();
    while let Some((addr, _, _)) = peers.connect_next(t0) {
        order.push(addr);
    }
    let expected: Vec<_> = by_peer_rank(&peers, fresh)
        .into_iter()
        .chain([closed])
        .chain(by_peer_rank(&peers, failed))
        .chain([unsynced])
        .collect();
    assert_eq!(order, expected);

    peers
        .with_peer_mut(closed.into(), "test", |peer| {
            assert!(!peer.encrypt_outgoing());
            peer.connecting_to_live(Id20::default(), &peers, ConnectionKind::Tcp);
            peer.handshake_encryption(Encryption::None, &peers);
        })
        .unwrap();
    assert_eq!(state_of(&peers, closed), Some("live"));
}

// Every candidate due at `at`, in the order the list hands them out.
fn connect_order_at(peers: &PeerStates, at: Instant) -> Vec<SocketAddr> {
    std::iter::from_fn(|| peers.connect_next(at).map(|(addr, ..)| addr)).collect()
}

// `addrs` as candidates that tie on failures, locality, last close and
// seeding are handed out: highest source rank, then highest BEP 40 rank.
fn by_source_then_peer_rank(
    peers: &PeerStates,
    mut addrs: Vec<(SocketAddr, PeerSourceFlag)>,
) -> Vec<SocketAddr> {
    addrs.sort_by_key(|(addr, source)| {
        (
            Reverse(connect_order::source_rank((*source).into())),
            Reverse(peers.connect_order.now().peer_rank(*addr)),
            *addr,
        )
    });
    addrs.into_iter().map(|(addr, _)| addr).collect()
}

// The peers of the connect-order cases: a peer named by the DHT whose
// encrypted handshake we closed at `closed_at` (its plaintext retry is
// due at once, `peer_connection::fast_reconnect`), and never-tried
// peers named by a tracker, by PEX, and by the DHT. Returns the retry
// peer and the never-tried ones with their sources.
fn retry_among_never_tried(
    peers: &PeerStates,
    closed_at: Instant,
) -> (SocketAddr, Vec<(SocketAddr, PeerSourceFlag)>) {
    let retry = peer_addr(1);
    peers.add_peer(retry, PeerSourceFlag::Dht);
    assert!(attempt_at(peers, retry, closed_at, Ends::Closed));
    let never_tried: Vec<_> = (10..12)
        .map(|i| (peer_addr(i), PeerSourceFlag::Tracker))
        .chain((20..22).map(|i| (peer_addr(i), PeerSourceFlag::Pex)))
        .chain((30..38).map(|i| (peer_addr(i), PeerSourceFlag::Dht)))
        .collect();
    for &(addr, source) in &never_tried {
        peers.add_peer(addr, source);
    }
    (retry, never_tried)
}

// libtorrent `peer_connection::fast_reconnect` dates the close of an
// encrypted handshake back 180 seconds, to 0 while the session is
// younger than that, which is a never-tried peer's `last_connected`:
// `peer_list::compare_peer` then ties the retry with every never-tried
// peer and goes on to source rank, then BEP 40 rank. Here the retry,
// named by the DHT, follows the tracker's peers, comes before PEX's,
// and falls among the DHT's by rank, behind some and ahead of others.
#[tokio::test(start_paused = true)]
async fn in_a_young_session_a_plain_retry_ranks_with_never_tried_peers_by_source_then_peer_rank() {
    let start = Instant::now();
    let peers = peer_states_started_at(start);
    let t0 = start + Duration::from_secs(179);
    let (retry, never_tried) = retry_among_never_tried(&peers, t0);

    let order = connect_order_at(&peers, t0);
    let mut all = never_tried.clone();
    all.push((retry, PeerSourceFlag::Dht));
    assert_eq!(order, by_source_then_peer_rank(&peers, all));

    let at = order.iter().position(|a| *a == retry).unwrap();
    let dht: Vec<_> = order[at - 1..=at + 1]
        .iter()
        .map(|a| never_tried.iter().find(|(n, _)| n == a).map(|(_, s)| *s))
        .collect();
    assert_eq!(
        dht,
        [Some(PeerSourceFlag::Dht), None, Some(PeerSourceFlag::Dht)],
        "the retry falls between two of the DHT's peers: {order:?}"
    );
    assert!(
        at > 2,
        "behind the tracker's peers and at least one DHT one"
    );
}

// Once the session is 180 seconds old the date-back leaves the retry a
// close of its own, after every never-tried peer's 0: those all go
// first, whatever named them.
#[tokio::test(start_paused = true)]
async fn in_a_session_past_180_seconds_never_tried_peers_go_before_a_plain_retry() {
    let start = Instant::now();
    let peers = peer_states_started_at(start);
    let t0 = start + Duration::from_secs(180);
    let (retry, never_tried) = retry_among_never_tried(&peers, t0);

    let order = connect_order_at(&peers, t0);
    let mut expected = by_source_then_peer_rank(&peers, never_tried);
    expected.push(retry);
    assert_eq!(order, expected);
}

// `peer_list::compare_peer` puts the fewest failures first, before
// locality, source or rank: a tracker's peer that failed once goes
// behind a never-tried peer PEX named.
#[tokio::test(start_paused = true)]
async fn fewer_failures_go_first_whatever_the_source() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    let (failed, fresh) = (peer_addr(1), peer_addr(2));
    peers.add_peer(failed, PeerSourceFlag::Tracker);
    fail_at(&peers, failed, t0);
    peers.add_peer(fresh, PeerSourceFlag::Pex);

    let due = t0 + reconnect_wait(1);
    assert_eq!(connect_order_at(&peers, due), [fresh, failed]);
}

// `peer_list::compare_peer`: with failures equal, a peer on a local
// network goes first, before source rank.
#[tokio::test(start_paused = true)]
async fn a_local_peer_goes_before_a_global_one_whatever_the_source() {
    let peers = make_peer_states();
    let global: SocketAddr = "203.0.113.5:6881".parse().unwrap();
    let local = peer_addr(1);
    peers.add_peer(global, PeerSourceFlag::Tracker);
    peers.add_peer(local, PeerSourceFlag::Pex);

    assert_eq!(connect_order_at(&peers, Instant::now()), [local, global]);
}

// `peer_list::compare_peer`: while we are finished, a peer PEX flagged
// a seed (`torrent_peer::maybe_upload_only`) goes behind one it did
// not, ahead of source and BEP 40 rank; while we are not, the flag
// counts nothing. A connection to the peer clears the flag
// (`peer_list::set_connection`).
#[tokio::test(start_paused = true)]
async fn while_finished_a_peer_flagged_a_seed_goes_behind_one_not_flagged() {
    use crate::peer_protocol::extended::ut_pex::PEX_SEED;
    let t0 = Instant::now();
    let setup = |finished: bool| {
        let peers = peer_states_started_at(t0);
        let pair = by_peer_rank(&peers, vec![peer_addr(1), peer_addr(2)]);
        let (higher, lower) = (pair[0], pair[1]);
        for addr in [higher, lower] {
            peers.add_peer(addr, PeerSourceFlag::Pex);
        }
        peers.pex_flags(higher, PEX_SEED);
        peers.set_finished(finished);
        (peers, higher, lower)
    };

    let (peers, higher, lower) = setup(true);
    assert_eq!(connect_order_at(&peers, t0), [lower, higher]);

    let (peers, higher, lower) = setup(false);
    assert_eq!(connect_order_at(&peers, t0), [higher, lower]);

    // Connected once, the flag is gone: the higher rank goes first
    // again, both having closed at the same second.
    let (peers, higher, lower) = setup(true);
    for addr in [higher, lower] {
        assert!(attempt_at(&peers, addr, t0, Ends::Completed));
    }
    let due = t0 + MIN_RECONNECT_TIME;
    assert_eq!(connect_order_at(&peers, due), [higher, lower]);
}

// libtorrent `peer_list::update_peer` sets `maybe_upload_only` only
// while there is no connection to the peer.
#[tokio::test(start_paused = true)]
async fn a_seed_flag_for_a_connected_peer_is_ignored() {
    use crate::peer_protocol::extended::ut_pex::PEX_SEED;
    let t0 = Instant::now();
    let peers = peer_states_started_at(t0);
    let pair = by_peer_rank(&peers, vec![peer_addr(1), peer_addr(2)]);
    let (higher, lower) = (pair[0], pair[1]);
    for addr in [higher, lower] {
        peers.add_peer(addr, PeerSourceFlag::Pex);
    }
    peers.set_finished(true);
    peers
        .with_peer_mut(higher.into(), "test", |peer| {
            peer.connect_if_due(t0, &peers).expect("a due candidate");
            peer.pex_flags(PEX_SEED, &peers);
            peers.connection_closed(peer, t0, false, true);
        })
        .unwrap();
    peers
        .with_peer_mut(lower.into(), "test", |peer| {
            peer.connect_if_due(t0, &peers).expect("a due candidate");
            peers.connection_closed(peer, t0, false, true);
        })
        .unwrap();

    let due = t0 + MIN_RECONNECT_TIME;
    assert_eq!(connect_order_at(&peers, due), [higher, lower]);
}

// libtorrent `bt_peer_connection::on_receive_impl` and
// `init_bt_handshake` set `pe_support` to the method that completed:
// the next connection tries it again.
#[tokio::test(start_paused = true)]
async fn a_peer_is_tried_again_with_the_handshake_it_last_completed() {
    let peers = make_peer_states();
    let (plain, encrypted) = (peer_addr(1), peer_addr(2));
    let t0 = Instant::now();

    peers.add_peer(plain, PeerSourceFlag::Tracker);
    assert!(attempt_at(&peers, plain, t0, Ends::Failed));
    assert!(!attempt_at(&peers, plain, t0, Ends::Completed));
    let t1 = due_after(&peers, t0);
    assert!(t1 > t0);
    assert!(!attempt_at(&peers, plain, t1, Ends::Completed));

    peers.add_peer(encrypted, PeerSourceFlag::Tracker);
    assert!(attempt_at(&peers, encrypted, t1, Ends::Completed));
    assert!(attempt_at(
        &peers,
        encrypted,
        t1 + MIN_RECONNECT_TIME,
        Ends::Completed
    ));
}

// libtorrent `peer_connection::fast_reconnect` takes effect while
// `torrent_peer::fast_reconnects` is under two: a third encrypted
// handshake that does not complete waits like any other close.
#[tokio::test(start_paused = true)]
async fn only_two_connections_follow_an_encrypted_handshake_at_once() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    let mut at = Instant::now();
    for at_once in [true, true, false] {
        assert!(attempt_at(&peers, a, at, Ends::Closed));
        let due = due_after(&peers, at);
        assert_eq!(due == at, at_once, "{due:?} after {at:?}");
        at = due;
        // The plaintext one in between, closed too, waits.
        assert!(!attempt_at(&peers, a, at, Ends::Closed));
        assert_eq!(due_after(&peers, at), at + MIN_RECONNECT_TIME);
        at += MIN_RECONNECT_TIME;
    }
}

// libtorrent `bt_peer_connection::on_connected` reads and flips
// `pe_support` under `pe_enabled` alone: where encryption is required
// nothing asks, so a failed connection neither toggles the peer's
// method nor is followed at once.
#[tokio::test(start_paused = true)]
async fn a_connection_that_asks_nothing_toggles_nothing() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    let t0 = Instant::now();
    fail_at(&peers, a, t0);
    fail_at(&peers, a, t0 + reconnect_wait(1));
    assert_eq!(
        peers.next_candidate(t0 + reconnect_wait(1)),
        NextCandidate::At(t0 + reconnect_wait(1) + reconnect_wait(2))
    );
    let tried = peers.with_peer_mut(a.into(), "test", |peer| peer.encrypt_outgoing());
    assert_eq!(tried, Some(true));
}

// libtorrent `peer_list::insert_peer` sets `pe_support` on a PEX
// `pex_encryption` flag, and never clears it on a naming without one:
// a peer remembered as plaintext is tried encrypted again.
#[tokio::test(start_paused = true)]
async fn a_pex_naming_with_the_encryption_flag_points_a_peer_back_at_encryption() {
    use crate::peer_protocol::extended::ut_pex::{PEX_ENCRYPTION, PEX_SEED};
    let peers = make_peer_states();
    let (named, unflagged) = (peer_addr(1), peer_addr(2));
    let t0 = Instant::now();
    for addr in [named, unflagged] {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
        assert!(attempt_at(&peers, addr, t0, Ends::Failed));
        assert!(!attempt_at(&peers, addr, t0, Ends::Completed));
    }

    peers.pex_flags(named, PEX_ENCRYPTION);
    peers.pex_flags(unflagged, PEX_SEED);
    let t1 = due_after(&peers, t0);
    assert!(attempt_at(&peers, named, t1, Ends::Completed));
    assert!(!attempt_at(&peers, unflagged, t1, Ends::Completed));
}

// libtorrent `ut_pex_plugin::tick` flags `pex_encryption` for a peer
// whose connection is encrypted (`bt_peer_connection::supports_encryption`).
#[tokio::test(start_paused = true)]
async fn our_pex_flags_a_peer_whose_connection_is_encrypted() {
    use crate::peer_protocol::extended::ut_pex::PEX_ENCRYPTION;
    let peers = make_peer_states();
    let (encrypted, plain) = (peer_addr(1), peer_addr(2));
    let now = Instant::now();
    for (addr, encrypt) in [(encrypted, true), (plain, false)] {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
        peers.with_peer_mut(addr.into(), "test", |peer| {
            peer.connect_if_due(now, &peers).expect("a due candidate");
            peer.connecting_to_live(Id20::default(), &peers, ConnectionKind::Tcp);
            peer.handshake_encryption(handshake(encrypt), &peers);
        });
    }

    let listed = peers.pex_listed();
    assert_eq!(listed[&encrypted] & PEX_ENCRYPTION, PEX_ENCRYPTION);
    assert_eq!(listed[&plain] & PEX_ENCRYPTION, 0);
}

// libtorrent `bt_peer_connection::init_bt_handshake` marks a connection
// encrypted whichever side opened it; both writes of `pe_support`
// (`init_bt_handshake`, `on_receive_impl`) are `is_outgoing()`, so a
// handshake the peer opened leaves which one ours try alone.
#[tokio::test(start_paused = true)]
async fn a_handshake_the_peer_opened_leaves_which_one_ours_try_alone() {
    use crate::peer_protocol::extended::ut_pex::PEX_ENCRYPTION;
    let peers = make_peer_states();
    let mut ends = Vec::new();
    for (addr, encrypted, flag) in [
        ("10.0.0.1:6881", true, PEX_ENCRYPTION),
        ("10.0.0.2:6881", false, 0),
    ] {
        let (addr, rx) = add_live_peer(&peers, addr);
        ends.push(rx);
        peers.handshake_encryption(addr.into(), handshake(encrypted));
        let advertised = peers.with_live(addr.into(), |live| live.advertised_pex_flags());
        assert_eq!(advertised.unwrap() & PEX_ENCRYPTION, flag);

        peers.with_peer_mut(addr.into(), "test", |peer| {
            peers.connection_closed(peer, Instant::now(), false, false)
        });
        assert!(peers.encrypt_outgoing(addr.into()), "{addr}");
    }
}

// libtorrent `torrent::write_resume_data`: the saved list holds the
// peers we can connect to that did not fail their last connection and
// sent no bad data, those we were connected to first; a peer at a
// banned address is saved as its ban.
#[test]
fn the_saved_peers_are_the_connectable_trusted_ones_those_connected_to_first() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    let connecting = peer_addr(1);
    peers.add_peer(connecting, PeerSourceFlag::Tracker);
    let (_, _connecting_rx, _connecting_tx) = peers.connect_next(t0).unwrap();
    let never = peer_addr(2);
    peers.add_peer(never, PeerSourceFlag::Tracker);
    let failed = peer_addr(3);
    peers.add_peer(failed, PeerSourceFlag::Tracker);
    fail_at(&peers, failed, t0);
    let distrusted = peer_addr(4);
    peers.add_peer(distrusted, PeerSourceFlag::Tracker);
    peers
        .with_peer_mut(distrusted.into(), "test", |p| p.piece_failed(false, &peers))
        .unwrap();
    // Connected to us: we know no address to connect to it at.
    let (_, _incoming_rx) = add_live_peer(&peers, "10.0.0.5:50000");
    let banned = peer_addr(6);
    peers.add_peer(banned, PeerSourceFlag::Tracker);
    peers.ban(banned.ip());

    assert_eq!(
        peers.resume_peers(),
        ResumePeers {
            peers: vec![connecting, never],
            banned: vec![banned.ip()],
        }
    );
    // The list a pause leaves saves the same.
    assert_eq!(peers.idle_list().resume_peers().banned, vec![banned.ip()]);
}

// libtorrent `torrent::write_resume_data`: every peer we were connected
// to is saved, and those we never were fill the list to a hundred.
#[test]
fn peers_never_connected_to_fill_the_saved_list_to_a_hundred() {
    let peers = make_peer_states();
    let connecting = peer_addr(0);
    peers.add_peer(connecting, PeerSourceFlag::Tracker);
    let (_, _rx, _tx) = peers.connect_next(Instant::now()).unwrap();
    for i in 1..=150 {
        peers.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
    }
    let saved = peers.resume_peers().peers;
    assert_eq!(saved.len(), RESUME_PEERS);
    assert_eq!(saved[0], connecting);
    let distinct: HashSet<_> = saved.iter().collect();
    assert_eq!(distinct.len(), RESUME_PEERS);
}

// libtorrent `torrent::read_resume_data` bans each saved address with
// `peer_list::ban_peer`: the list a restart is founded on refuses a peer
// there on any port, named or connecting to us.
#[test]
fn a_saved_ban_refuses_the_peer() {
    let banned = peer_addr(1);
    let peers = PeerStates::founded_on(
        PeerList::resumed([], [banned.ip()]),
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    );
    assert!(peers.is_banned(banned.ip()));
    peers.add_peer(banned, PeerSourceFlag::ResumeData);
    assert_eq!(state_of(&peers, banned), None);
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    assert_eq!(
        peers
            .add_incoming(
                at_port(banned, 50000),
                Id20::default(),
                tx,
                ConnectionKind::Tcp,
                OUR_PORT
            )
            .err(),
        Some(IncomingRefused::Banned)
    );
    assert_eq!(peers.resume_peers().banned, vec![banned.ip()]);
}
