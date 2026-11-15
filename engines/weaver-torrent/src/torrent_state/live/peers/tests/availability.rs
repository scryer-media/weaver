// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::*;

#[test]
fn full_bitfield_counts_as_seeder_and_is_uncounted_on_disconnect() {
    let peers = make_peer_states();
    let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
    assert_eq!(live_seeders(&peers), (0, 0));

    peers.update_bitfield(addr, empty_bitfield()).unwrap();
    assert_eq!(live_seeders(&peers), (0, 0));

    peers.update_bitfield(addr, full_bitfield()).unwrap();
    assert_eq!(live_seeders(&peers), (1, 1));

    // Repeated updates must not double-count.
    peers.update_bitfield(addr, full_bitfield()).unwrap();
    assert_eq!(live_seeders(&peers), (1, 1));

    peers.drop_peer(addr).unwrap();
    assert_eq!(live_seeders(&peers), (0, 0));
}

#[test]
fn bitfield_completed_piece_by_piece_counts_as_seeder() {
    let peers = make_peer_states();
    let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
    peers.update_bitfield(addr, empty_bitfield()).unwrap();

    for piece in 0..TOTAL_PIECES {
        peers
            .peer_has(
                addr,
                self::piece(
                    u32::try_from(TOTAL_PIECES).unwrap(),
                    u32::try_from(piece).unwrap(),
                ),
            )
            .unwrap();
        let expected = u32::from(piece == TOTAL_PIECES - 1);
        assert_eq!(live_seeders(&peers), (expected, expected));
    }
}

#[test]
fn seeder_is_uncounted_when_peer_leaves_live_state() {
    let peers = make_peer_states();
    let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
    peers.update_bitfield(addr, full_bitfield()).unwrap();
    assert_eq!(live_seeders(&peers), (1, 1));

    peers
        .with_peer_mut(addr, "test", |peer| peer.set_not_needed(&peers))
        .unwrap();
    assert_eq!(live_seeders(&peers), (0, 0));
}

#[test]
fn seeders_are_counted_per_peer() {
    let peers = make_peer_states();
    let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
    let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");

    peers.update_bitfield(a, full_bitfield()).unwrap();
    peers.update_bitfield(b, empty_bitfield()).unwrap();
    assert_eq!(live_seeders(&peers), (1, 1));

    peers.update_bitfield(b, full_bitfield()).unwrap();
    assert_eq!(live_seeders(&peers), (2, 2));

    peers.drop_peer(a).unwrap();
    assert_eq!(live_seeders(&peers), (1, 1));
}

#[test]
fn a_steal_empties_the_victims_queue_of_the_stolen_piece() {
    use crate::core::constants::CHUNK_SIZE;
    use crate::core::lengths::Lengths;
    use crate::peer_protocol::Request;

    let lengths = Lengths::new(CHUNK_SIZE as u64 * 8, CHUNK_SIZE * 4).unwrap();
    let stolen = lengths.validate_piece_index(0).unwrap();
    let kept = lengths.validate_piece_index(1).unwrap();

    let peers = make_peer_states();
    let (victim, mut victim_rx) = add_live_peer(&peers, "127.0.0.1:1234");
    let (thief, _thief_rx) = add_live_peer(&peers, "127.0.0.2:1235");

    peers
        .with_live_mut(victim, "test", |live| {
            for chunk in lengths.iter_chunk_infos(stolen).take(2) {
                assert!(live.add_inflight_request(chunk));
            }
            for chunk in lengths.iter_chunk_infos(kept).take(1) {
                assert!(live.add_inflight_request(chunk));
            }
        })
        .unwrap();

    peers.on_steal(victim, thief, stolen);

    let queued = peers
        .with_live(victim, |live| {
            live.inflight_requests().copied().collect::<Vec<_>>()
        })
        .unwrap();
    assert!(
        queued.iter().all(|req| req.piece_index == kept),
        "{queued:?}"
    );
    assert_eq!(queued.len(), 1);

    let mut cancelled = Vec::new();
    while let Ok(msg) = victim_rx.try_recv() {
        match msg {
            WriterRequest::Message(Message::Cancel(Request { index, begin, .. })) => {
                cancelled.push((index, begin))
            }
            _ => panic!("unexpected writer request"),
        }
    }
    cancelled.sort();
    assert_eq!(cancelled, vec![(0, 0), (0, CHUNK_SIZE)]);
}

fn bitfield_of(pieces: &[usize]) -> BF {
    let mut bf = empty_bitfield();
    for &p in pieces {
        bf.set(p, true);
    }
    bf
}

fn availability(peers: &PeerStates) -> Vec<u32> {
    peers.availability.counts()
}

#[test]
fn availability_counts_every_live_peers_bitfield() {
    let peers = make_peer_states();
    let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
    let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");
    assert_eq!(availability(&peers), [0; TOTAL_PIECES]);

    peers.update_bitfield(a, full_bitfield()).unwrap();
    peers.update_bitfield(b, bitfield_of(&[0, 3])).unwrap();
    assert_eq!(availability(&peers), [2, 1, 1, 2, 1, 1, 1, 1]);
}

#[test]
fn a_have_adds_the_piece_once() {
    let peers = make_peer_states();
    let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
    peers.update_bitfield(a, bitfield_of(&[0])).unwrap();

    peers
        .peer_has(a, piece(u32::try_from(TOTAL_PIECES).unwrap(), 5))
        .unwrap();
    assert_eq!(availability(&peers), [1, 0, 0, 0, 0, 1, 0, 0]);

    // libtorrent incoming_have: a HAVE for a piece the peer has is ignored.
    peers
        .peer_has(a, piece(u32::try_from(TOTAL_PIECES).unwrap(), 5))
        .unwrap();
    peers
        .peer_has(a, piece(u32::try_from(TOTAL_PIECES).unwrap(), 0))
        .unwrap();
    assert_eq!(availability(&peers), [1, 0, 0, 0, 0, 1, 0, 0]);
}

#[test]
fn a_have_says_whether_it_told_us_anything() {
    let peers = make_peer_states();
    let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
    let all_but_last: Vec<usize> = (0..TOTAL_PIECES - 1).collect();
    peers
        .update_bitfield(a, bitfield_of(&all_but_last))
        .unwrap();

    let have = |index: usize| {
        peers.peer_has(
            a,
            piece(
                u32::try_from(TOTAL_PIECES).unwrap(),
                u32::try_from(index).unwrap(),
            ),
        )
    };
    assert_eq!(have(0), Some(Have::Known));
    assert_eq!(have(TOTAL_PIECES - 1), Some(Have::Completed));
    assert_eq!(have(TOTAL_PIECES - 1), Some(Have::Known));

    let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");
    peers.update_bitfield(b, bitfield_of(&[])).unwrap();
    assert_eq!(
        peers.peer_has(b, piece(u32::try_from(TOTAL_PIECES).unwrap(), 3)),
        Some(Have::Gained)
    );
}

#[test]
fn a_seed_counts_once_and_a_dont_have_breaks_it_into_pieces() {
    let peers = make_peer_states();
    let total = u32::try_from(TOTAL_PIECES).unwrap();
    let ranked = |peers: &PeerStates| {
        (0..total)
            .map(|i| peers.availability.ranked(piece(total, i)))
            .collect::<Vec<_>>()
    };
    let all = crate::chunk_tracker::Moved {
        pieces: Vec::new(),
        all: true,
    };
    let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
    let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");
    let (c, _rx_c) = add_live_peer(&peers, "127.0.0.3:1236");
    let (d, _rx_d) = add_live_peer(&peers, "127.0.0.4:1237");
    peers.update_bitfield(c, bitfield_of(&[2])).unwrap();
    let all_but_last: Vec<usize> = (0..TOTAL_PIECES - 1).collect();
    peers
        .update_bitfield(b, bitfield_of(&all_but_last))
        .unwrap();
    peers.availability.take_moved();

    // The first seed adds one to every piece's rank: the picker lays its
    // order out anew.
    peers.update_bitfield(a, full_bitfield()).unwrap();
    assert_eq!(peers.availability.take_moved(), all);
    assert_eq!(availability(&peers), [2, 2, 3, 2, 2, 2, 2, 1]);
    assert_eq!(ranked(&peers), [2, 2, 3, 2, 2, 2, 2, 1]);

    // A peer that gains its last piece leaves the counts for the seed.
    assert_eq!(
        peers.peer_has(b, piece(total, total - 1)),
        Some(Have::Completed)
    );
    assert_eq!(availability(&peers), [2, 2, 3, 2, 2, 2, 2, 2]);
    assert_eq!(ranked(&peers), [1, 1, 2, 1, 1, 1, 1, 1]);
    peers.availability.take_moved();

    // Another seed moves no piece.
    peers.update_bitfield(d, full_bitfield()).unwrap();
    assert_eq!(peers.availability.take_moved(), Default::default());
    assert_eq!(availability(&peers), [3, 3, 4, 3, 3, 3, 3, 3]);

    // A seed's DONT_HAVE counts the pieces it keeps one by one.
    peers.peer_lost(a, piece(total, 3)).unwrap();
    assert_eq!(availability(&peers), [3, 3, 4, 2, 3, 3, 3, 3]);
    assert_eq!(ranked(&peers), [2, 2, 3, 1, 2, 2, 2, 2]);
    peers.availability.take_moved();

    // Only the last seed to leave moves the order.
    peers.drop_peer(b).unwrap();
    assert_eq!(peers.availability.take_moved(), Default::default());
    peers.drop_peer(d).unwrap();
    assert_eq!(peers.availability.take_moved(), all);
    assert_eq!(availability(&peers), [1, 1, 2, 0, 1, 1, 1, 1]);
    assert_eq!(ranked(&peers), [1, 1, 2, 0, 1, 1, 1, 1]);
}

#[test]
fn a_second_bitfield_replaces_the_first() {
    let peers = make_peer_states();
    let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
    peers.update_bitfield(a, bitfield_of(&[0, 1])).unwrap();

    peers.update_bitfield(a, bitfield_of(&[1, 2])).unwrap();
    assert_eq!(availability(&peers), [0, 1, 1, 0, 0, 0, 0, 0]);
}

#[test]
fn a_closed_connection_takes_its_bitfield_back_once() {
    let peers = make_peer_states();
    let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
    let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");
    peers.update_bitfield(a, bitfield_of(&[0, 1])).unwrap();
    peers.update_bitfield(b, bitfield_of(&[1])).unwrap();
    peers
        .peer_has(a, piece(u32::try_from(TOTAL_PIECES).unwrap(), 7))
        .unwrap();

    // As the peer's task does when the connection ends: take the live
    // state, then settle the peer.
    peers
        .with_peer_mut(a, "test", |peer| {
            peer.take_state(&peers);
            peers.connection_closed(peer, Instant::now(), false, false);
        })
        .unwrap();
    assert_eq!(availability(&peers), [0, 1, 0, 0, 0, 0, 0, 0]);

    peers
        .with_peer_mut(a, "test", |peer| {
            peers.connection_closed(peer, Instant::now(), false, false)
        })
        .unwrap();
    assert_eq!(availability(&peers), [0, 1, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn a_peer_let_go_or_dropped_takes_its_bitfield_back() {
    let peers = make_peer_states();
    let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
    let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");
    peers.update_bitfield(a, full_bitfield()).unwrap();
    peers.update_bitfield(b, bitfield_of(&[4])).unwrap();

    peers
        .with_peer_mut(a, "test", |peer| peer.set_not_needed(&peers))
        .unwrap();
    assert_eq!(availability(&peers), [0, 0, 0, 0, 1, 0, 0, 0]);

    peers.drop_peer(b).unwrap();
    assert_eq!(availability(&peers), [0; TOTAL_PIECES]);
}

fn haves_queued(rx: &mut PeerRx) -> Vec<u32> {
    let mut haves = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        match msg {
            WriterRequest::Message(Message::Have(piece)) => haves.push(piece),
            _ => panic!("unexpected writer request"),
        }
    }
    haves
}

fn piece(total_pieces: u32, index: u32) -> ValidPieceIndex {
    crate::core::lengths::Lengths::new(total_pieces as u64, 1)
        .unwrap()
        .validate_piece_index(index)
        .unwrap()
}

#[test]
fn a_completed_piece_is_announced_to_every_live_peer_that_lacks_it() {
    let peers = make_peer_states();
    let (_silent, mut silent_rx) = add_live_peer(&peers, "127.0.0.1:1234");
    let (seeder, mut seeder_rx) = add_live_peer(&peers, "127.0.0.2:1235");
    let (leecher, mut leecher_rx) = add_live_peer(&peers, "127.0.0.3:1236");
    peers.update_bitfield(seeder, full_bitfield()).unwrap();
    peers.update_bitfield(leecher, empty_bitfield()).unwrap();

    peers.announce_have(piece(u32::try_from(TOTAL_PIECES).unwrap(), 3));

    // The peer that sent no bitfield has nothing, so it is told.
    assert_eq!(haves_queued(&mut silent_rx), vec![3]);
    assert_eq!(haves_queued(&mut leecher_rx), vec![3]);
    assert_eq!(haves_queued(&mut seeder_rx), Vec::<u32>::new());
}

#[test]
fn a_peer_that_falls_behind_still_learns_every_completed_piece() {
    // Far more pieces than a 128-entry bounded channel holds, all completed
    // before the peer's writer reads a single one.
    const PIECES: u32 = 1000;
    let peers = make_peer_states();
    let (_slow, mut slow_rx) = add_live_peer(&peers, "127.0.0.1:1234");

    for index in 0..PIECES {
        peers.announce_have(piece(PIECES, index));
    }

    assert_eq!(haves_queued(&mut slow_rx), (0..PIECES).collect::<Vec<_>>());
}

// One connection holds each peer id: a second is refused or takes it
// as libtorrent's rule says (`bt_peer_connection::on_receive`,
// "read_peer_id"), the id is let go once the connection holding it
// closes, and our own id is held by none.
#[test]
fn a_peer_id_is_held_by_one_connection_until_it_closes() {
    let peers = make_peer_states();
    let ours = Id20::new([0x80; 20]);
    let below = Id20::new([0x10; 20]);
    let live = |addr: &str, id: Id20| {
        let addr: SocketAddr = addr.parse().unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(addr, id, tx, ConnectionKind::Tcp, OUR_PORT)
            .unwrap();
        (addr, rx)
    };
    let (a, _rx_a) = live("192.0.2.1:6881", below);
    let (b, _rx_b) = live("[2001:db8::1]:6881", below);
    let (c, _rx_c) = live("192.0.2.3:6881", below);

    assert_eq!(
        peers.claim_peer_id(a, below, false, ours),
        PeerIdClaim::Held
    );
    assert_eq!(
        peers.claim_peer_id(a, below, false, ours),
        PeerIdClaim::Held
    );
    // Incoming, from a client below our id: the newcomer goes.
    assert_eq!(
        peers.claim_peer_id(b, below, false, ours),
        PeerIdClaim::Refused
    );
    // One we made, to a client below our id: the one before it goes.
    assert_eq!(
        peers.claim_peer_id(c, below, true, ours),
        PeerIdClaim::TakenFrom(a)
    );
    // Above our id, the other way round.
    let above = Id20::new([0xf0; 20]);
    let (d, _rx_d) = live("192.0.2.4:6881", above);
    let (e, _rx_e) = live("[2001:db8::4]:6881", above);
    let (f, _rx_f) = live("192.0.2.6:6881", above);
    assert_eq!(peers.claim_peer_id(d, above, true, ours), PeerIdClaim::Held);
    assert_eq!(
        peers.claim_peer_id(e, above, true, ours),
        PeerIdClaim::Refused
    );
    assert_eq!(
        peers.claim_peer_id(f, above, false, ours),
        PeerIdClaim::TakenFrom(d)
    );

    // The connection at `a` closes, which held no id `below` by then;
    // `c`, which held it, closes and lets it go.
    for closed in [a, c] {
        peers
            .with_peer_mut(closed, "test", |peer| {
                peer.take_state(&peers);
                peers.connection_closed(peer, Instant::now(), false, false);
            })
            .unwrap();
    }
    assert_eq!(
        peers.claim_peer_id(b, below, false, ours),
        PeerIdClaim::Held
    );
    assert_eq!(peers.peer_ids.lock().get(&above), Some(&f));

    // Our own id is the self connection, never held.
    assert_eq!(peers.claim_peer_id(b, ours, false, ours), PeerIdClaim::Held);
    assert!(!peers.peer_ids.lock().contains_key(&ours));
}
