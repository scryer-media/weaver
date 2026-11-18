// SPDX-License-Identifier: GPL-3.0-or-later

//! The slots of the pieces in flight: held only in flight, ruling on a
//! block as the tracker would without the torrent's lock, keeping a piece
//! with a block being written from a steal, and taken in the lock order.

use std::sync::Arc;

use parking_lot::RwLock;

use super::golden::{CHUNKS, PIECES, chunk_of, peer, piece_of, tracker};
use super::*;
use crate::core::constants::CHUNK_SIZE;

type Ask = AcquireRequest<
    'static,
    std::iter::Empty<ValidPieceIndex>,
    fn(ValidPieceIndex) -> bool,
    fn(ValidPieceIndex) -> u32,
    fn(ValidPieceIndex) -> bool,
>;

// A request from `who`, which has every piece, held pieces `avg` long.
fn ask(who: PeerHandle, avg: Option<Duration>) -> Ask {
    AcquireRequest {
        peer: who,
        peer_avg_time: avg,
        priority_pieces: std::iter::empty(),
        peer_has_piece: |_| true,
        availability: |_| 1,
        moved: Moved {
            pieces: Vec::new(),
            all: true,
        },
        num_peers: 8,
        outstanding_requests: 0,
        options: PickOptions::default(),
        can_steal: |_| true,
    }
}

fn mark_slow(t: &mut PieceTracker, c: &ChunkInfo, sender: PeerHandle) -> ChunkMarkingResult {
    let data = vec![0u8; CHUNK_SIZE as usize];
    t.mark_chunk_downloaded(
        &Piece::from_data(c.piece_index.get(), c.offset, &data[..]),
        sender,
    )
    .unwrap()
}

#[test]
fn a_piece_holds_a_slot_from_its_grant_until_it_leaves_flight_and_none_otherwise() {
    let mut t = tracker(3);
    let (a, b) = (peer(1), peer(2));
    let first = piece_of(&t, 0);
    let second = piece_of(&t, 1);
    t.reserve_piece(first, a);
    t.reserve_piece(second, b);
    assert_eq!(t.slots().held(), 2);
    // A piece no peer owns holds none, whatever arrives for it.
    let queued = chunk_of(&t, 2, 0);
    assert!(t.slots().arrive(&queued, a, |_, _| true).is_none());
    assert_eq!(t.slots().held(), 2);

    // Given back, it holds none; nor does one out for its check.
    assert_eq!(t.release_pieces_owned_by(a), 1);
    assert_eq!(t.slots().held(), 1);
    t.take_inflight(second);
    assert_eq!(t.slots().held(), 0);

    // Nor any piece once the torrent pauses.
    t.reserve_piece(first, a);
    let slots = t.slots().clone();
    let _ = t.into_chunks();
    assert_eq!(slots.held(), 0);
}

#[test]
fn a_block_of_a_piece_in_flight_is_ruled_on_and_marked_under_its_lock_as_the_tracker_would() {
    let mut t = tracker(3);
    let (owner, other) = (peer(1), peer(2));
    let piece = piece_of(&t, 0);
    t.reserve_piece(piece, owner);
    let slots = t.slots().clone();
    let blocks: Vec<ChunkInfo> = (0..CHUNKS).map(|i| chunk_of(&t, 0, i)).collect();
    let block = |i: u32| blocks[i as usize];

    // The owner's first block takes the piece's room; later ones take none.
    let mut asked = Vec::new();
    let arrival = slots.arrive(&block(0), owner, |p, owned| {
        asked.push((p.get(), owned));
        true
    });
    assert_eq!(arrival, Some(Arrival::Write { cancel: vec![] }));
    // Another copy while it is written is redundant.
    assert_eq!(
        slots.arrive(&block(0), other, |_, _| panic!("no room asked")),
        Some(Arrival::Discard)
    );
    // Another peer's block of it cancels the owner's request.
    assert_eq!(
        slots.arrive(&block(1), other, |_, _| panic!("no room asked")),
        Some(Arrival::Write {
            cancel: vec![owner]
        })
    );
    assert_eq!(asked, vec![(0, true)]);
    // While a block is being written, the piece is started.
    assert!(t.is_started(piece));

    // Marked, not the last: under the piece's lock alone.
    assert!(slots.mark(&block(0), owner));
    assert!(slots.mark(&block(1), other));
    assert_eq!(t.chunks().received_chunks(piece), 2);
    // A block already received goes to the tracker.
    assert!(!slots.mark(&block(1), other));
    // A failed write lets a later copy in.
    assert_eq!(
        slots.arrive(&block(2), owner, |_, _| true),
        Some(Arrival::Write { cancel: vec![] })
    );
    assert!(slots.write_failed(&block(2)));
    assert_eq!(
        slots.arrive(&block(2), owner, |_, _| true),
        Some(Arrival::Write { cancel: vec![] })
    );
    assert!(slots.mark(&block(2), owner));

    // The last block is the tracker's to mark: it takes the piece out of
    // flight for its check, with who sent each block.
    assert_eq!(
        slots.arrive(&block(3), owner, |_, _| true),
        Some(Arrival::Write { cancel: vec![] })
    );
    assert!(!slots.mark(&block(3), owner));
    assert!(matches!(
        mark_slow(&mut t, &block(3), owner),
        ChunkMarkingResult::Completed
    ));
    let (time, senders) = t.take_inflight(piece);
    assert!(time.is_some());
    assert_eq!(
        senders.blocks(),
        [Some(owner), Some(other), Some(owner), Some(owner)]
    );
    assert_eq!(slots.held(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_piece_with_a_block_being_written_is_not_taken_over() {
    let mut t = tracker(5);
    let (slow, fast) = (peer(1), peer(2));
    // Every piece in flight with the slow peer, so the end game is on; the
    // first held longest.
    t.reserve_piece(piece_of(&t, 0), slow);
    tokio::time::advance(Duration::from_secs(1)).await;
    for i in 1..PIECES {
        t.reserve_piece(piece_of(&t, i), slow);
    }
    // Each piece has a block received, and the first a block being written.
    for i in 0..PIECES {
        let c = chunk_of(&t, i, 0);
        assert!(matches!(
            t.slots().arrive(&c, slow, |_, _| true),
            Some(Arrival::Write { .. })
        ));
        assert!(t.slots().mark(&c, slow));
    }
    let busy = chunk_of(&t, 0, 1);
    assert!(matches!(
        t.slots().arrive(&busy, slow, |_, _| true),
        Some(Arrival::Write { .. })
    ));
    tokio::time::advance(Duration::from_secs(10)).await;

    // Held longest, past the fast peer's piece time, the first stays while
    // its block is written: the fast peer doubles a block instead.
    let avg = Some(Duration::from_millis(1));
    let doubled = t.acquire_piece(ask(fast, avg));
    assert!(
        matches!(doubled, AcquireResult::EndGame { .. }),
        "{doubled:?}"
    );
    assert!(t.is_owned_by(piece_of(&t, 0), slow));
    // Written, it goes over, and its slot names its new owner.
    assert!(t.slots().mark(&busy, slow));
    match t.acquire_piece(ask(fast, avg)) {
        AcquireResult::Stolen { piece, from_peer } => {
            assert_eq!((piece.get(), from_peer), (0, slow));
        }
        other => panic!("expected the first piece taken over, got {other:?}"),
    }
    assert_eq!(
        t.slots().arrive(&chunk_of(&t, 0, 2), slow, |_, _| true),
        Some(Arrival::Write { cancel: vec![fast] })
    );
}

// Peers write their pieces' blocks on their own threads, the last of each
// under the torrent's lock, while another thread picks, doubles, snubs and
// looks under it: every block is marked once, every piece completes once,
// and no slot is left.
#[test]
fn blocks_marked_under_their_piece_locks_alongside_the_torrent_lock_all_land_once() {
    const ROUNDS: usize = 200;
    let t = Arc::new(RwLock::new(tracker(11)));
    let peers: Vec<PeerHandle> = (1..=PIECES)
        .map(|i| peer(u8::try_from(i).expect("few pieces")))
        .collect();
    {
        let mut g = t.write();
        for (i, p) in (0..PIECES).zip(&peers) {
            let piece = piece_of(&g, i);
            g.reserve_piece(piece, *p);
        }
    }
    let slots = t.read().slots().clone();
    let completed = std::thread::scope(|scope| {
        let looker = scope.spawn(|| {
            for round in 0..ROUNDS {
                let mut g = t.write();
                let who = peer(200);
                // In the end game: doubles a block, or finds none free.
                let _ = g.acquire_piece(ask(who, None));
                for p in &peers {
                    let _ = g.release_piece_blocked_by(*p, |_| round % 2 == 0);
                }
                g.release_pieces_owned_by(who);
                drop(g);
                std::thread::yield_now();
            }
        });
        let writers: Vec<_> = (0..PIECES)
            .zip(&peers)
            .map(|(i, p)| {
                let (t, slots) = (&t, &slots);
                scope.spawn(move || {
                    let mut completed = 0;
                    for chunk in 0..CHUNKS {
                        let c = chunk_of(&t.read(), i, chunk);
                        let arrival = match slots.arrive(&c, *p, |_, _| true) {
                            Some(arrival) => arrival,
                            None => t.write().admitted_block_arrived(&c, *p, |_, _| true),
                        };
                        if !matches!(arrival, Arrival::Write { .. }) {
                            continue;
                        }
                        if slots.mark(&c, *p) {
                            continue;
                        }
                        let mut g = t.write();
                        if matches!(mark_slow(&mut g, &c, *p), ChunkMarkingResult::Completed) {
                            g.take_inflight(c.piece_index);
                            completed += 1;
                        }
                    }
                    completed
                })
            })
            .collect();
        looker.join().unwrap();
        writers.into_iter().map(|w| w.join().unwrap()).sum::<u32>()
    });
    let g = t.read();
    assert_eq!(completed, PIECES);
    for i in 0..PIECES {
        assert_eq!(g.chunks().received_chunks(piece_of(&g, i)), CHUNKS as usize);
    }
    assert_eq!(slots.held(), 0);
    assert!(g.writing.is_empty());
}

#[test]
#[should_panic(expected = "a second piece lock is taken only under the torrent's write lock")]
fn a_second_piece_lock_outside_the_torrent_write_lock_is_refused() {
    let t = tracker(1);
    let _first = t.slots().lock(piece_of(&t, 0));
    let _second = t.slots().lock(piece_of(&t, 1));
}

#[test]
#[should_panic(expected = "the torrent's lock is taken before a piece's, never under one")]
fn the_torrent_lock_under_a_piece_lock_is_refused() {
    let t = tracker(1);
    let _piece = t.slots().lock(piece_of(&t, 0));
    torrent_lock_taken();
}
