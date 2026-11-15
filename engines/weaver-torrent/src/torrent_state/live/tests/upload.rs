// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

#[tokio::test]
async fn a_cancel_takes_an_unsent_block_off_the_upload_queue() {
    let h = Harness::seeding("a_cancel_drops_an_upload").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);

    for chunk in [0, 1] {
        peer.on_received_message(Message::Request(request(0, chunk)))
            .await
            .unwrap();
    }
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, 0));

    // The first block is with the writer, unsent: cancelled, it is not
    // sent, and the writer is handed the next.
    peer.on_received_message(Message::Cancel(request(0, 0)))
        .await
        .unwrap();
    assert!(!peer.claim_upload(&h.chunk(0, 0)));
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, CHUNK_SIZE));
    assert!(peer.claim_upload(&h.chunk(0, 1)));

    // A block cancelled before its turn never reaches the writer: the next
    // one does.
    peer.on_received_message(Message::Request(request(1, 0)))
        .await
        .unwrap();
    peer.on_received_message(Message::Cancel(request(1, 0)))
        .await
        .unwrap();
    peer.on_received_message(Message::Request(request(1, 1)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Block(1, CHUNK_SIZE));
}

#[tokio::test]
async fn a_cancel_of_an_unsent_block_is_answered_with_a_reject_on_a_fast_connection() {
    let h = Harness::seeding("a_fast_cancel_is_rejected").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    h.speak_fast(&peer);
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);

    peer.on_received_message(Message::Request(request(0, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, 0));
    peer.on_received_message(Message::Cancel(request(0, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Reject(0, 0));
    assert!(!peer.claim_upload(&h.chunk(0, 0)));

    // A block already sent stays sent, and its cancel has no answer.
    peer.on_received_message(Message::Request(request(0, 1)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, CHUNK_SIZE));
    assert!(peer.claim_upload(&h.chunk(0, 1)));
    peer.on_received_message(Message::Cancel(request(0, 1)))
        .await
        .unwrap();
    assert!(rx.is_empty());
}

// The writer's send buffer is the one of the connection that went live with
// it, bound by the handshake and by nothing later: a writer whose
// connection has not gone live takes no block, whoever is live at its
// address.
#[tokio::test]
async fn a_writer_holds_the_send_buffer_its_handshake_bound() {
    let h = Harness::seeding_bytes("upload_ledger_bound", 4).await;
    let (mut peer, mut rx) = h.peer("127.0.0.1:6881");
    let addr = peer.addr;
    let live_ledger = || {
        h.live
            .peers
            .with_live(addr, peer::LivePeerState::upload_ledger)
            .unwrap()
    };
    peer.upload = Default::default();
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    peer.on_received_message(Message::Request(request(0, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, 0));

    // A peer is live at the address and its send buffer holds the block,
    // and the writer has no send buffer to take it from.
    assert!(peer.upload.get().is_none());
    assert!(!peer.claim_upload(&h.chunk(0, 0)));

    peer.on_handshake(handshake(true), ConnectionKind::Tcp, false)
        .unwrap();
    assert!(Arc::ptr_eq(peer.upload.get().unwrap(), &live_ledger()));
    assert!(peer.claim_upload(&h.chunk(0, 0)));
    peer.upload_done(&h.chunk(0, 0));
}

// libtorrent `peer_connection::fill_send_buffer`: each peer is served from
// its own send buffer, held to its watermark, so however many blocks one
// peer asked for, the next peer waits no more than that watermark of it.
#[tokio::test]
async fn a_deep_upload_queue_on_one_peer_never_holds_up_the_next() {
    // 32 pieces of four blocks, all had.
    let h = Harness::seeding_bytes("upload_turns", 4).await;
    let (deep, mut deep_rx) = h.peer("127.0.0.1:6881");
    let (light, mut light_rx) = h.peer("127.0.0.2:6881");
    let (deep, light) = (&deep, &light);
    for (peer, rx) in [(deep, &mut deep_rx), (light, &mut light_rx)] {
        peer.on_received_message(Message::Interested).await.unwrap();
        assert_eq!(next_sent(rx).await, Sent::Unchoke);
    }

    // The deep peer asks for 100 blocks, then the light peer for one.
    let asked: Vec<(u32, u32)> = (0..100)
        .map(|n| (n / CHUNKS_PER_PIECE, n % CHUNKS_PER_PIECE))
        .collect();
    for &(index, chunk) in &asked {
        deep.on_received_message(Message::Request(request(index, chunk)))
            .await
            .unwrap();
    }
    light
        .on_received_message(Message::Request(request(31, 0)))
        .await
        .unwrap();

    // One round fills each peer's send buffer to the low watermark, as an
    // idle peer has no upload rate: the light peer's block goes out
    // alongside the deep peer's first eight, and the deep peer gets no ninth
    // until its writer has sent one.
    let handed = usize::try_from(peer::SEND_BUFFER_LOW_WATERMARK / u64::from(CHUNK_SIZE)).unwrap();
    assert_eq!(next_sent(&mut light_rx).await, Sent::Block(31, 0));
    for &(index, chunk) in &asked[..handed] {
        assert_eq!(
            next_sent(&mut deep_rx).await,
            Sent::Block(index, chunk * CHUNK_SIZE)
        );
    }
    assert!(deep_rx.is_empty());

    // The writer sends each in turn, and the rest follow in the order asked.
    for (n, &(index, chunk)) in asked.iter().enumerate() {
        if n >= handed {
            assert_eq!(
                next_sent(&mut deep_rx).await,
                Sent::Block(index, chunk * CHUNK_SIZE)
            );
        }
        assert!(deep.claim_upload(&h.chunk(index, chunk)));
        deep.upload_done(&h.chunk(index, chunk));
    }
    assert!(deep_rx.is_empty());
}

#[tokio::test]
async fn a_fast_peer_we_choke_is_sent_a_reject_for_every_block_it_will_not_get() {
    let h = Harness::seeding_with_slots("a_fast_choke_rejects", 1).await;
    tokio::time::pause();
    let (held, mut held_rx) = h.peer("127.0.0.1:6881");
    let (other, mut other_rx) = h.peer("127.0.0.2:6881");
    h.speak_fast(&held);
    let (held, other) = (&held, &other);

    held.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut held_rx).await, Sent::Unchoke);
    for chunk in [0, 1] {
        held.on_received_message(Message::Request(request(0, chunk)))
            .await
            .unwrap();
    }
    // Both are with the writer, unsent.
    assert_eq!(next_sent(&mut held_rx).await, Sent::Block(0, 0));
    assert_eq!(next_sent(&mut held_rx).await, Sent::Block(0, CHUNK_SIZE));

    // The slot passes to the other peer. The Choke goes first, then a Reject
    // for each unsent block.
    other
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut other_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut held_rx).await, Sent::Choke);
    let rejects = BTreeSet::from([next_sent(&mut held_rx).await, next_sent(&mut held_rx).await]);
    assert_eq!(
        rejects,
        BTreeSet::from([Sent::Reject(0, 0), Sent::Reject(0, CHUNK_SIZE)])
    );
    assert!(!held.claim_upload(&h.chunk(0, 0)));
    assert!(!held.claim_upload(&h.chunk(0, 1)));

    // What the choked peer asks for now is rejected at once, and within the
    // grace the connection stays.
    held.on_received_message(Message::Request(request(1, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut held_rx).await, Sent::Reject(1, 0));
    assert!(held_rx.is_empty());

    // A cancel for a block already rejected has no answer.
    held.on_received_message(Message::Cancel(request(0, 0)))
        .await
        .unwrap();
    assert!(held_rx.is_empty());
}

#[tokio::test]
async fn a_fast_peer_that_chokes_us_keeps_our_requests_until_it_rejects_one() {
    let h = Harness::new("a_fast_choke_keeps_requests").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    h.speak_fast(&peer);
    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(peer.task_peer_chunk_requester());

    peer.on_i_am_unchoked();
    let taken = h.request_every_chunk(&peer, &mut requester, &mut rx).await;
    let other = 1 - taken;
    drop(requester);

    // The peer answers every request with its block or a Reject, so the
    // Choke leaves them all in flight, and a block still counts.
    (&peer).on_received_message(Message::Choke).await.unwrap();
    assert_eq!(h.inflight_count(&peer), every_chunk().len() - 1);
    assert!(h.queued_pieces().is_empty());
    h.deliver(&peer, (taken, CHUNK_SIZE)).await;
    assert_eq!(h.missing_chunks(taken), vec![2, 3]);

    // One Reject gives back both pieces and every request, the delivered
    // chunks kept.
    (&peer)
        .on_received_message(Message::Reject(request(taken, 2)))
        .await
        .unwrap();
    assert_eq!(h.inflight_count(&peer), 0);
    assert_eq!(h.queued_pieces(), vec![0, 1]);
    assert_eq!(h.missing_chunks(taken), vec![2, 3]);

    // The Rejects for the rest answer requests we no longer wait for.
    let mut rest = chunks(other, 0..CHUNKS_PER_PIECE);
    rest.insert((taken, 3 * CHUNK_SIZE));
    for (index, begin) in rest {
        (&peer)
            .on_received_message(Message::Reject(request(index, begin / CHUNK_SIZE)))
            .await
            .unwrap();
    }
    // One more answers nothing we asked for.
    assert!(
        (&peer)
            .on_received_message(Message::Reject(request(other, 0)))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_reject_for_a_block_we_never_asked_for_ends_the_connection() {
    let h = Harness::new("a_reject_never_requested").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    h.speak_fast(&peer);
    let peer = &peer;
    assert!(
        peer.on_received_message(Message::Reject(request(0, 0)))
            .await
            .is_err()
    );
    let past_the_end = Request {
        index: PIECES,
        begin: 0,
        length: CHUNK_SIZE,
    };
    assert!(
        peer.on_received_message(Message::Reject(past_the_end))
            .await
            .is_err()
    );
}

/// A seed with one upload slot, a fast peer that has piece 1, and a second
/// peer. Two pieces are fewer than the set's five, so the fast peer's
/// allowed fast set is every piece it lacks: piece 0.
async fn allowed_fast_seed(name: &str) -> (Harness, (PeerHandler, PeerRx), (PeerHandler, PeerRx)) {
    let h = Harness::seeding_with_slots(name, 1).await;
    tokio::time::pause();
    let fast = h.peer("127.0.0.1:6881");
    let other = h.peer("127.0.0.2:6881");
    h.speak_fast(&fast.0);
    fast.0.on_bitfield(&[0x40]).unwrap();
    (h, fast, other)
}

#[tokio::test]
async fn a_fast_peer_we_choke_is_still_served_its_allowed_fast_pieces() {
    let (h, (fast, mut fast_rx), (other, mut other_rx)) =
        allowed_fast_seed("allowed_fast_served").await;
    let (fast, other) = (&fast, &other);

    // The set goes out on the first Interested, and not again.
    fast.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut fast_rx).await, Sent::AllowedFast(0));
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Unchoke);
    fast.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Unchoke);
    assert!(fast_rx.is_empty());

    for index in [0, 1] {
        fast.on_received_message(Message::Request(request(index, 0)))
            .await
            .unwrap();
    }
    // Both blocks are with the writer, unsent.
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Block(0, 0));
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Block(1, 0));

    // The slot passes to the other peer: the Choke rejects the unsent block
    // of piece 1 and keeps piece 0's.
    other
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut other_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Choke);
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Reject(1, 0));
    assert!(fast_rx.is_empty());
    assert!(fast.claim_upload(&h.chunk(0, 0)));
    assert!(!fast.claim_upload(&h.chunk(1, 0)));

    // Choked, the peer is served piece 0 and refused piece 1.
    fast.on_received_message(Message::Request(request(0, 1)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Block(0, CHUNK_SIZE));
    fast.on_received_message(Message::Request(request(1, 1)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Reject(1, CHUNK_SIZE));
    assert!(fast_rx.is_empty());
}

#[tokio::test]
async fn a_choked_peer_may_ask_for_an_allowed_fast_piece_three_times_over() {
    let (h, (fast, mut fast_rx), (other, mut other_rx)) =
        allowed_fast_seed("allowed_fast_three_times").await;
    let (fast, other) = (&fast, &other);
    fast.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut fast_rx).await, Sent::AllowedFast(0));
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Unchoke);
    other
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut other_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Choke);

    for asked in 0..3 * CHUNKS_PER_PIECE {
        let chunk = asked % CHUNKS_PER_PIECE;
        fast.on_received_message(Message::Request(request(0, chunk)))
            .await
            .unwrap();
        assert_eq!(
            next_sent(&mut fast_rx).await,
            Sent::Block(0, chunk * CHUNK_SIZE)
        );
        // The writer sends it, so the peer may ask for it again.
        assert!(fast.claim_upload(&h.chunk(0, chunk)));
        fast.upload_done(&h.chunk(0, chunk));
    }
    assert!(
        fast.on_received_message(Message::Request(request(0, 0)))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn allowed_fast_from_a_peer_names_only_pieces_we_lack() {
    // Piece 0 is had.
    let h = Harness::build(
        "allowed_fast_ingest",
        0x80,
        choker::DEFAULT_UNCHOKE_SLOTS_LIMIT,
    )
    .await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    h.speak_fast(&peer);
    let peer = &peer;
    for index in [0, PIECES, 1, 1] {
        peer.on_received_message(Message::AllowedFast(index))
            .await
            .unwrap();
    }
    let allowed = h
        .peers
        .with_live(peer.addr, |live| live.allowed_fast_from_peer.clone())
        .unwrap();
    assert_eq!(allowed, vec![h.piece(1)]);
}

#[tokio::test]
async fn a_peer_that_chokes_us_is_asked_only_for_its_allowed_fast_pieces() {
    let h = Harness::new("allowed_fast_requested").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    h.speak_fast(&peer);
    tokio::time::pause();
    (&peer)
        .on_received_message(Message::AllowedFast(1))
        .await
        .unwrap();
    let mut requester: Task<'_> = Box::pin(peer.task_peer_chunk_requester());

    assert_eq!(
        next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await,
        chunks(1, 0..CHUNKS_PER_PIECE)
    );
    assert!(rx.is_empty());
    assert_eq!(h.queued_pieces(), vec![0]);

    // Unchoked, and each block that arrives deepens the window by one in
    // slow start: the rest is asked for, two at a time.
    peer.on_i_am_unchoked();
    h.deliver(&peer, (1, 0)).await;
    assert_eq!(
        next_requests(&mut requester, &mut rx, 2).await,
        chunks(0, 0..2)
    );
    h.deliver(&peer, (1, CHUNK_SIZE)).await;
    assert_eq!(
        next_requests(&mut requester, &mut rx, 2).await,
        chunks(0, 2..4)
    );
}

#[tokio::test]
async fn a_piece_we_may_not_finish_while_choked_goes_back_and_an_allowed_fast_one_follows() {
    let h = Harness::new("allowed_fast_after_choke").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    h.speak_fast(&peer);
    peer.lock_flow_control("test").request_window = 1;
    let mut requester: Task<'_> = Box::pin(peer.task_peer_chunk_requester());

    peer.on_i_am_unchoked();
    let first = next_requests(&mut requester, &mut rx, 1).await;
    let (taken, _) = *first.first().unwrap();
    assert_eq!(first, chunks(taken, [0]));
    let other = 1 - taken;

    // Choked mid piece, with the other piece allowed fast: the rest of the
    // first piece may not be asked for, so it goes back, and the other
    // follows.
    (&peer).on_received_message(Message::Choke).await.unwrap();
    (&peer)
        .on_received_message(Message::AllowedFast(other))
        .await
        .unwrap();
    assert_eq!(
        next_requests(&mut requester, &mut rx, 1).await,
        chunks(other, [0])
    );
    assert_eq!(h.queued_pieces(), vec![taken]);
    assert_eq!(h.inflight_count(&peer), 1);
}

#[tokio::test]
async fn a_suggestion_names_only_a_piece_we_lack() {
    // Piece 0 is had.
    let h = Harness::build("suggest_ingest", 0x80, choker::DEFAULT_UNCHOKE_SLOTS_LIMIT).await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    h.speak_fast(&peer);
    let peer = &peer;
    for index in [0, PIECES, 1] {
        peer.on_received_message(Message::Suggest(index))
            .await
            .unwrap();
    }
    let suggested = h
        .peers
        .with_live(peer.addr, |live| live.suggested.clone())
        .unwrap();
    assert_eq!(suggested, vec![h.piece(1)]);
}

#[tokio::test]
async fn the_piece_a_peer_suggests_is_asked_for_first_until_it_rejects_it() {
    for suggested in 0..PIECES {
        let h = Harness::new("suggest_requested").await;
        let (peer, mut rx) = h.peer("127.0.0.1:6881");
        h.speak_fast(&peer);
        (&peer)
            .on_received_message(Message::Suggest(suggested))
            .await
            .unwrap();
        let mut requester: Task<'_> = Box::pin(peer.task_peer_chunk_requester());

        // Fewer pieces are had than rarest first needs: without the
        // suggestion the first piece is a random one.
        peer.on_i_am_unchoked();
        assert_eq!(
            next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await,
            chunks(suggested, 0..CHUNKS_PER_PIECE)
        );
        drop(requester);

        (&peer)
            .on_received_message(Message::Reject(request(suggested, 0)))
            .await
            .unwrap();
        let left = h
            .peers
            .with_live(peer.addr, |live| live.suggested.clone())
            .unwrap();
        assert!(left.is_empty());
    }
}

impl Harness {
    /// Whether we choke the peer.
    fn choked(&self, peer: &PeerHandler) -> bool {
        self.peers
            .with_live(peer.addr, |live| live.is_choked())
            .unwrap()
    }
}

#[tokio::test]
async fn only_as_many_peers_as_there_are_upload_slots_are_unchoked() {
    let h = Harness::seeding("only_the_slots_are_unchoked").await;
    tokio::time::pause();
    let slots = choker::DEFAULT_UNCHOKE_SLOTS_LIMIT;
    let mut peers: Vec<_> = (1..=slots + 2)
        .map(|i| h.peer(&format!("127.0.0.{i}:6881")))
        .collect();
    for (peer, _) in &peers {
        peer.on_received_message(Message::Interested).await.unwrap();
    }

    // The first peers take the free slots; the rest wait, choked.
    let (held, waiting) = peers.split_at_mut(slots);
    for (_, rx) in held.iter_mut() {
        assert_eq!(next_sent(rx).await, Sent::Unchoke);
    }
    for (peer, rx) in waiting.iter_mut() {
        assert!(rx.is_empty());
        assert!(h.choked(peer));
    }

    // At the first round one waiting peer takes the optimistic slot, and the
    // round keeps the regular slots, one fewer, for the peers that held one.
    // None has moved a byte and all were unchoked at once, so the last by
    // address gives its slot up.
    assert_eq!(next_sent(&mut held[slots - 1].1).await, Sent::Choke);
    for (_, rx) in held.iter_mut() {
        assert!(rx.is_empty());
    }
    let mut unchoked_waiting = 0;
    for (_, rx) in waiting.iter_mut() {
        if !rx.is_empty() {
            assert_eq!(next_sent(rx).await, Sent::Unchoke);
            unchoked_waiting += 1;
        }
    }
    assert_eq!(unchoked_waiting, 1);
    let unchoked = peers.iter().filter(|(peer, _)| !h.choked(peer)).count();
    assert_eq!(unchoked, slots);
}

#[tokio::test]
async fn a_peer_that_asks_without_saying_it_is_interested_is_taken_to_be() {
    let h = Harness::seeding("a_request_is_interest").await;
    tokio::time::pause();
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;

    peer.on_received_message(Message::Request(request(0, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, 0));
}

#[tokio::test]
async fn an_unchoked_peer_that_says_it_is_interested_again_is_told_again() {
    let h = Harness::seeding("interested_again").await;
    tokio::time::pause();
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;

    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    assert!(!h.choked(peer));
}

#[tokio::test]
async fn a_peer_that_sends_us_more_takes_the_slot_of_one_that_sends_less() {
    // Two regular slots and the optimistic one.
    let h = Harness::seeding_with_slots("the_faster_peer_takes_a_slot", 3).await;
    tokio::time::pause();
    let (slow, mut slow_rx) = h.peer("127.0.0.1:6881");
    let (steady, mut steady_rx) = h.peer("127.0.0.2:6881");
    let (optimistic, mut optimistic_rx) = h.peer("127.0.0.3:6881");
    let (fast, mut fast_rx) = h.peer("127.0.0.4:6881");
    for (peer, rx) in [
        (&slow, &mut slow_rx),
        (&steady, &mut steady_rx),
        (&optimistic, &mut optimistic_rx),
    ] {
        peer.on_received_message(Message::Interested).await.unwrap();
        assert_eq!(next_sent(rx).await, Sent::Unchoke);
    }

    // The first round keeps two of the three, none having sent a byte, and
    // the third comes back on the optimistic slot.
    assert_eq!(next_sent(&mut optimistic_rx).await, Sent::Choke);
    assert_eq!(next_sent(&mut optimistic_rx).await, Sent::Unchoke);

    (&fast)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert!(fast_rx.is_empty());

    // Over the round the fast peer sends us the most, the slow one the least:
    // the fast one takes the slow one's slot.
    for (peer, pieces) in [(&slow, 1u64), (&steady, 2), (&fast, 3)] {
        peer.counters
            .fetched_bytes
            .fetch_add(pieces * u64::from(PIECE_LENGTH), Ordering::Relaxed);
    }
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut slow_rx).await, Sent::Choke);
    assert!(steady_rx.is_empty());
    assert!(!h.choked(&steady));
    assert!(optimistic_rx.is_empty());
    assert!(!h.choked(&optimistic));
}

#[tokio::test]
async fn the_optimistic_slot_passes_every_30_s_to_a_peer_that_never_held_it() {
    // The one slot is the optimistic one.
    let h = Harness::seeding_with_slots("the_optimistic_slot_rotates", 1).await;
    tokio::time::pause();
    let (first, mut first_rx) = h.peer("127.0.0.1:6881");
    let (new, mut new_rx) = h.peer("127.0.0.2:6881");

    // The first peer takes the free slot, loses it at the first round, as
    // there is no regular slot, and gets it back as the optimistic one.
    (&first)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut first_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut first_rx).await, Sent::Choke);
    assert_eq!(next_sent(&mut first_rx).await, Sent::Unchoke);
    let first_held_it = tokio::time::Instant::now();

    // A new peer waits for the slot, then takes it from the first peer, which
    // held it last, after the interval.
    (&new)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert!(new_rx.is_empty());
    assert_eq!(next_sent(&mut new_rx).await, Sent::Unchoke);
    assert_eq!(
        tokio::time::Instant::now(),
        first_held_it + choker::OPTIMISTIC_UNCHOKE_INTERVAL
    );
    assert_eq!(next_sent(&mut first_rx).await, Sent::Choke);
    assert!(h.choked(&first));
}

// libtorrent `peer_connection::get_peer_info`: the listing shows the
// handshake-only encryption, the peer's unchoke, our optimistic slot, and
// who named the peer.
#[tokio::test]
async fn the_peers_listing_shows_encryption_unchoke_and_the_optimistic_slot() {
    // The one slot is the optimistic one.
    let h = Harness::seeding_with_slots("peers_listing_flags", 1).await;
    tokio::time::pause();
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    h.live
        .peers
        .handshake_encryption(peer.addr, crate::mse::Encryption::HeaderOnly);
    (&peer).on_received_message(Message::Unchoke).await.unwrap();
    (&peer)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    // It takes the free slot, loses it at the first round, as there is no
    // regular slot, and gets it back as the optimistic one.
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut rx).await, Sent::Choke);
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);

    let [info] = h.live.peer_infos().try_into().unwrap();
    assert_eq!(info.addr, peer.addr);
    assert_eq!(info.encryption, crate::mse::Encryption::HeaderOnly);
    assert!(!info.outgoing);
    assert!(!info.flags.remote_choked && info.flags.remote_interested);
    assert!(!info.flags.choked && info.flags.optimistic_unchoke);
    assert_eq!(
        info.sources.iter().collect::<Vec<_>>(),
        [PeerSourceFlag::Incoming]
    );
}

// libtorrent `session_impl::preemptive_unchoke`,
// `recalculate_optimistic_unchoke_slots`: the upload slots are the
// session's. A peer of one torrent finds no free slot while a peer of another
// holds the only one, and the optimistic slot passes between the torrents.
#[tokio::test]
async fn a_peer_of_one_torrent_waits_for_the_slot_a_peer_of_another_holds() {
    // The one slot of the session is the optimistic one.
    let a = Harness::seeding_with_slots("one_slot_a", 1).await;
    let b = a.sibling("one_slot_b", 0xc0).await;
    tokio::time::pause();
    let (first, mut first_rx) = a.peer("127.0.0.1:6881");
    let (other, mut other_rx) = b.peer("127.0.0.2:6881");

    (&first)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut first_rx).await, Sent::Unchoke);
    (&other)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert!(other_rx.is_empty());
    assert!(b.choked(&other));

    // At the first round the optimistic slot goes to the peer that waited
    // for it, and the first peer, holding no regular slot, is choked.
    assert_eq!(next_sent(&mut other_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut first_rx).await, Sent::Choke);
    let other_held_it = tokio::time::Instant::now();

    // The first peer never held the optimistic slot: it takes it next.
    assert_eq!(next_sent(&mut first_rx).await, Sent::Unchoke);
    assert_eq!(
        tokio::time::Instant::now(),
        other_held_it + choker::OPTIMISTIC_UNCHOKE_INTERVAL
    );
    assert_eq!(next_sent(&mut other_rx).await, Sent::Choke);
    assert!(b.choked(&other));
}

// libtorrent `session_impl::preemptive_unchoke` counts the slots held,
// `num_peers_up_unchoked`, and a stopped torrent's connections give theirs
// up as they go (`torrent::remove_peer`). An interested peer of another
// torrent takes the freed slot at once, not at the next round.
#[tokio::test]
async fn a_stopped_torrents_peer_gives_up_its_slot_at_once() {
    let a = Harness::seeding_with_slots("stopped_slot_a", 1).await;
    let b = a.sibling("stopped_slot_b", 0xc0).await;
    let (held, mut held_rx) = b.peer("127.0.0.2:6881");
    (&held)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut held_rx).await, Sent::Unchoke);

    let session = &a._session;
    let handle = session.get(b.shared.id.into()).unwrap();
    session.pause(&handle).await.unwrap();

    tokio::time::pause();
    let stopped = tokio::time::Instant::now();
    let (peer, mut rx) = a.peer("127.0.0.1:6881");
    (&peer)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    assert_eq!(tokio::time::Instant::now(), stopped);
}

// libtorrent `session_impl::recalculate_unchoke_slots`: a round ranks the
// peers of every torrent of the session together.
#[tokio::test]
async fn a_peer_of_another_torrent_that_sends_us_more_takes_the_slot_of_one_that_sends_less() {
    // Two regular slots and the optimistic one, over both torrents.
    let a = Harness::seeding_with_slots("ranked_together_a", 3).await;
    let b = a.sibling("ranked_together_b", 0xc0).await;
    tokio::time::pause();
    let (slow, mut slow_rx) = a.peer("127.0.0.1:6881");
    let (steady, mut steady_rx) = a.peer("127.0.0.2:6881");
    let (optimistic, mut optimistic_rx) = a.peer("127.0.0.3:6881");
    let (fast, mut fast_rx) = b.peer("127.0.0.4:6881");
    for (peer, rx) in [
        (&slow, &mut slow_rx),
        (&steady, &mut steady_rx),
        (&optimistic, &mut optimistic_rx),
    ] {
        peer.on_received_message(Message::Interested).await.unwrap();
        assert_eq!(next_sent(rx).await, Sent::Unchoke);
    }

    // The first round keeps two of the three, none having sent a byte, and
    // the third comes back on the optimistic slot.
    assert_eq!(next_sent(&mut optimistic_rx).await, Sent::Choke);
    assert_eq!(next_sent(&mut optimistic_rx).await, Sent::Unchoke);

    // The other torrent has no peer unchoked, and still no slot is free.
    (&fast)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert!(fast_rx.is_empty());

    // Over the round the other torrent's peer sends us the most, the slow
    // one the least: it takes the slow one's slot.
    for (peer, pieces) in [(&slow, 1u64), (&steady, 2), (&fast, 3)] {
        peer.counters
            .fetched_bytes
            .fetch_add(pieces * u64::from(PIECE_LENGTH), Ordering::Relaxed);
    }
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut slow_rx).await, Sent::Choke);
    assert!(steady_rx.is_empty());
    assert!(!a.choked(&steady));
    assert!(optimistic_rx.is_empty());
    assert!(!a.choked(&optimistic));
}

#[tokio::test]
async fn a_choked_peer_loses_its_unsent_blocks_and_its_requests_are_ignored() {
    let h = Harness::seeding_with_slots("a_choke_drops_uploads", 1).await;
    tokio::time::pause();
    let (held, mut held_rx) = h.peer("127.0.0.1:6881");
    let (other, mut other_rx) = h.peer("127.0.0.2:6881");
    let (held, other) = (&held, &other);

    held.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut held_rx).await, Sent::Unchoke);
    for chunk in [0, 1] {
        held.on_received_message(Message::Request(request(0, chunk)))
            .await
            .unwrap();
    }
    assert_eq!(next_sent(&mut held_rx).await, Sent::Block(0, 0));
    assert_eq!(next_sent(&mut held_rx).await, Sent::Block(0, CHUNK_SIZE));

    // The one slot is the optimistic one: at the first round it passes to
    // the other peer, which never held it.
    other
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut other_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut held_rx).await, Sent::Choke);

    // Both blocks were with the writer, unsent: neither goes out.
    assert!(!held.claim_upload(&h.chunk(0, 0)));
    assert!(!held.claim_upload(&h.chunk(0, 1)));

    // What the choked peer asks for now, as if on its way before the Choke,
    // is not queued, so never sent.
    held.on_received_message(Message::Request(request(1, 0)))
        .await
        .unwrap();
    assert!(!held.claim_upload(&h.chunk(1, 0)));
    assert!(held_rx.is_empty());

    // A request past the grace after the Choke ends the connection.
    tokio::time::advance(choker::CHOKED_REQUEST_GRACE + Duration::from_millis(1)).await;
    assert!(
        held.on_received_message(Message::Request(request(1, 1)))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_peer_that_is_not_interested_is_choked_and_its_slot_handed_on_at_once() {
    // The one slot is the optimistic one.
    let h = Harness::seeding_with_slots("not_interested_chokes", 1).await;
    tokio::time::pause();
    let (leaving, mut leaving_rx) = h.peer("127.0.0.1:6881");
    let (waiting, mut waiting_rx) = h.peer("127.0.0.2:6881");
    let leaving = &leaving;

    // The peer takes the free slot, loses it at the first round, and gets it
    // back as the optimistic one.
    leaving
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut leaving_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut leaving_rx).await, Sent::Choke);
    assert_eq!(next_sent(&mut leaving_rx).await, Sent::Unchoke);
    (&waiting)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert!(waiting_rx.is_empty());
    leaving
        .on_received_message(Message::Request(request(0, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut leaving_rx).await, Sent::Block(0, 0));

    // The peer is choked as it says it is not interested, and its unsent
    // block does not go out.
    let said_so = tokio::time::Instant::now();
    leaving
        .on_received_message(Message::NotInterested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut leaving_rx).await, Sent::Choke);
    assert!(!leaving.claim_upload(&h.chunk(0, 0)));

    // The slot it gave up passes to the waiting peer at once.
    assert_eq!(next_sent(&mut waiting_rx).await, Sent::Unchoke);
    assert_eq!(tokio::time::Instant::now(), said_so);
    assert!(leaving_rx.is_empty());
}

// libtorrent `peer_connection::incoming_request`: a request is served when it
// names any block inside a piece we have, no longer than a block.
#[tokio::test]
async fn a_request_for_any_block_inside_a_had_piece_is_served() {
    let h = Harness::seeding("any_block_is_served").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);

    let unaligned = Request {
        index: 0,
        begin: 1,
        length: 100,
    };
    let tail = Request {
        index: 1,
        begin: PIECE_LENGTH - 10,
        length: 10,
    };
    for r in [unaligned, tail] {
        peer.on_received_message(Message::Request(r)).await.unwrap();
    }
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, 1));
    let unaligned = h.lengths.requested_block(h.piece(0), 1, 100).unwrap();
    assert!(peer.claim_upload(&unaligned));
    assert_eq!(next_sent(&mut rx).await, Sent::Block(1, PIECE_LENGTH - 10));
}

// libtorrent `peer_connection::incoming_request`: past
// `max_allowed_in_request_queue` blocks asked for and not sent, a request is
// rejected on a fast connection and the connection stays;
// `bt_peer_connection::write_extensions` advertises the limit as `reqq`.
#[tokio::test]
async fn a_request_past_the_queue_limit_is_rejected_and_reqq_advertises_the_limit() {
    let h = Harness::seeding("request_queue_limit").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    h.speak_fast(&peer);
    let peer = &peer;
    let mut hs = ExtendedHandshake::new();
    peer.update_my_extended_handshake(&mut hs).unwrap();
    assert_eq!(hs.reqq, Some(MAX_ALLOWED_IN_REQUEST_QUEUE));
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);

    // Distinct one-byte blocks of piece 0, every one a block a peer may ask
    // for; the writer takes none, so all stay unsent.
    let byte = |begin| Request {
        index: 0,
        begin,
        length: 1,
    };
    for begin in 0..MAX_ALLOWED_IN_REQUEST_QUEUE {
        peer.on_received_message(Message::Request(byte(begin)))
            .await
            .unwrap();
    }
    // A block asked for again is queued once and goes out once.
    peer.on_received_message(Message::Request(byte(0)))
        .await
        .unwrap();
    // The queue is full: the next is rejected, and the peer kept.
    let past = byte(MAX_ALLOWED_IN_REQUEST_QUEUE);
    peer.on_received_message(Message::Request(past))
        .await
        .unwrap();
    let past_chunk = h
        .lengths
        .requested_block(h.piece(0), past.begin, past.length)
        .unwrap();
    assert!(!peer.claim_upload(&past_chunk));

    // A cancel makes room, and the same request is queued now.
    peer.on_received_message(Message::Cancel(byte(1)))
        .await
        .unwrap();
    peer.on_received_message(Message::Request(past))
        .await
        .unwrap();

    // Everything queued for the writer up to that last block, the writer
    // taking each in turn: each block once, the first Reject the one past
    // the limit.
    let mut blocks = Vec::new();
    let mut rejects = Vec::new();
    loop {
        match next_sent(&mut rx).await {
            Sent::Block(0, begin) => {
                let block = h.lengths.requested_block(h.piece(0), begin, 1).unwrap();
                assert!(peer.claim_upload(&block));
                blocks.push(begin);
                if begin == past.begin {
                    break;
                }
            }
            Sent::Reject(0, begin) => rejects.push(begin),
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(rejects, [past.begin, 1]);
    assert_eq!(blocks.iter().filter(|&&b| b == 0).count(), 1);
    assert_eq!(blocks.last(), Some(&past.begin));
}

/// A request for what the torrent of [`Harness::build`] with only piece 0
/// had cannot serve: the request at position `n`, cycling through each way
/// a request can be wrong.
fn unservable(n: u32) -> Request {
    let (index, begin, length) = match n % 5 {
        0 => (1, 0, CHUNK_SIZE),       // a piece we do not have
        1 => (0, 0, CHUNK_SIZE + 1),   // longer than a block
        2 => (0, 0, 0),                // empty
        3 => (0, PIECE_LENGTH - 1, 2), // past the end of the piece
        _ => (PIECES, 0, CHUNK_SIZE),  // no such piece
    };
    Request {
        index,
        begin,
        length,
    }
}

#[tokio::test]
async fn a_request_we_cannot_serve_is_dropped_and_the_peer_kept() {
    let h = Harness::build(
        "unservable_is_dropped",
        0x80,
        choker::DEFAULT_UNCHOKE_SLOTS_LIMIT,
    )
    .await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);

    for n in 0..5 {
        peer.on_received_message(Message::Request(unservable(n)))
            .await
            .unwrap();
    }
    assert!(rx.is_empty());

    peer.on_received_message(Message::Request(request(0, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, 0));
}

// libtorrent `peer_connection::incoming_request`: on a fast connection each
// request we cannot serve is answered with a Reject (`write_reject_request`),
// once, and the peer kept.
#[tokio::test]
async fn a_request_we_cannot_serve_is_rejected_on_a_fast_connection() {
    let h = Harness::build(
        "unservable_is_rejected",
        0x80,
        choker::DEFAULT_UNCHOKE_SLOTS_LIMIT,
    )
    .await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    h.speak_fast(peer);

    for n in 0..5 {
        let asked = unservable(n);
        peer.on_received_message(Message::Request(asked))
            .await
            .unwrap();
        match rx.try_recv() {
            Ok(WriterRequest::Answer(crate::peer_connection::Answer::Reject(r))) => {
                assert_eq!(
                    (r.index, r.begin, r.length),
                    (asked.index, asked.begin, asked.length)
                );
            }
            other => panic!("expected a Reject for {asked:?}, got {other:?}"),
        }
        assert!(rx.is_empty());
    }

    peer.on_received_message(Message::Request(request(0, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, 0));
}

impl Harness {
    /// The peer sends `count` requests we cannot serve, each kept.
    async fn ask_unservable(&self, peer: &PeerHandler, count: u32) {
        for n in 0..count {
            peer.on_received_message(Message::Request(unservable(n)))
                .await
                .unwrap();
        }
    }
}

// libtorrent `peer_connection::incoming_request`: a peer that is neither
// interested nor unchoked is told it is choked every tenth request we cannot
// serve, and disconnected at a tenth past 300 while it does not choke us.
#[tokio::test]
async fn a_choked_peer_asking_for_what_we_cannot_serve_is_reminded_then_disconnected() {
    let h = Harness::build(
        "unservable_reminded",
        0x80,
        choker::DEFAULT_UNCHOKE_SLOTS_LIMIT,
    )
    .await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    assert!(h.choked(peer));

    for _ in 0..31 {
        h.ask_unservable(peer, 9).await;
        assert!(rx.is_empty());
        h.ask_unservable(peer, 1).await;
        assert_eq!(next_sent(&mut rx).await, Sent::Choke);
    }

    // At 310 the peer chokes us, so it was reminded again; once it unchokes
    // us, the next tenth ends the connection.
    peer.on_received_message(Message::Unchoke).await.unwrap();
    h.ask_unservable(peer, 9).await;
    assert!(
        peer.on_received_message(Message::Request(unservable(0)))
            .await
            .is_err()
    );
}

// libtorrent `peer_connection::send_choke`: choking the peer starts its count
// of requests we cannot serve again.
#[tokio::test]
async fn choking_a_peer_starts_its_count_of_unservable_requests_again() {
    let h = Harness::build(
        "unservable_count_resets",
        0x80,
        choker::DEFAULT_UNCHOKE_SLOTS_LIMIT,
    )
    .await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    h.ask_unservable(peer, 5).await;

    peer.on_received_message(Message::NotInterested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Choke);

    h.ask_unservable(peer, 9).await;
    assert!(rx.is_empty());
    h.ask_unservable(peer, 1).await;
    assert_eq!(next_sent(&mut rx).await, Sent::Choke);
}

/// The pieces we told the peer we have since last asked, draining its
/// queue.
fn told_have(rx: &mut PeerRx) -> Vec<u32> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|r| match r {
            WriterRequest::Message(Message::Have(i)) => Some(i),
            _ => None,
        })
        .collect()
}

// libtorrent `bt_peer_connection::write_bitfield` under super-seeding
// sends no bitfield and tells the peer of two pieces from
// `torrent::get_piece_to_super_seed`: ones it lacks, as rare as any, and
// handed to no peer, itself included, while one such is left.
#[tokio::test]
async fn a_super_seed_sends_no_bitfield_and_hands_each_peer_two_of_the_rarest_pieces() {
    let h = Harness::super_seeding("super_seed_rarest", 4, 0xf0).await;
    let (s, mut s_rx) = h.peer_with("127.0.0.1:6881", &[0]).await;
    assert!(!(&s).should_send_bitfield());
    let handed = told_have(&mut s_rx);
    assert_eq!(handed.len(), 2);
    assert!(handed.iter().all(|p| [1, 2, 3].contains(p)));
    assert_ne!(handed[0], handed[1]);

    // Of the pieces held by none, one is not handed; then piece 0, held by
    // one peer, before the pieces handed.
    let rest = (1..4).find(|p| !handed.contains(p)).unwrap();
    let (_p, mut p_rx) = h.peer_with("127.0.0.2:6882", &[]).await;
    assert_eq!(told_have(&mut p_rx), [rest, 0]);

    // A peer that lacks nothing is handed nothing.
    let (_seed, mut seed_rx) = h.peer_with("127.0.0.3:6883", &[0, 1, 2, 3]).await;
    assert_eq!(told_have(&mut seed_rx), [] as [u32; 0]);
}

// libtorrent `peer_connection::incoming_request`: while super-seeding, a
// request for a piece other than the two handed out is refused.
#[tokio::test]
async fn a_super_seeded_peer_is_served_only_the_pieces_it_was_handed() {
    let h = Harness::super_seeding("super_seed_serves_handed", 3, 0xe0).await;
    let (peer, mut rx) = h.peer_with("127.0.0.1:6881", &[]).await;
    let handed = told_have(&mut rx);
    let other = (0..3).find(|p| !handed.contains(p)).unwrap();
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);

    peer.on_received_message(Message::Request(request(other, 0)))
        .await
        .unwrap();
    assert!(rx.is_empty());
    for piece in handed {
        peer.on_received_message(Message::Request(request(piece, 0)))
            .await
            .unwrap();
        assert_eq!(next_sent(&mut rx).await, Sent::Block(piece, 0));
        assert!(peer.claim_upload(&h.chunk(piece, 0)));
    }
}

// libtorrent `peer_connection::incoming_request`: while super-seeding, a fast
// peer's request for a piece it was not handed is answered with a Reject.
#[tokio::test]
async fn a_super_seeded_fast_peer_is_rejected_a_piece_it_was_not_handed() {
    let h = Harness::super_seeding("super_seed_rejects", 3, 0xe0).await;
    let (peer, mut rx) = h.peer_with("127.0.0.1:6881", &[]).await;
    let handed = told_have(&mut rx);
    let other = (0..3).find(|p| !handed.contains(p)).unwrap();
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    h.speak_fast(peer);

    peer.on_received_message(Message::Request(request(other, 0)))
        .await
        .unwrap();
    assert_eq!(rx.try_recv().ok().map(sent), Some(Sent::Reject(other, 0)));
    assert!(rx.is_empty());
}

// libtorrent `peer_connection::incoming_have` without
// `strict_super_seeding`: a peer that reports a piece it was handed is
// handed the next in its place; a piece it got elsewhere hands it nothing.
#[tokio::test]
async fn a_peer_that_reports_its_handed_piece_is_handed_the_next() {
    let h = Harness::super_seeding("super_seed_next", 4, 0xf0).await;
    let (a, mut a_rx) = h.peer_with("127.0.0.1:6881", &[]).await;
    let handed = told_have(&mut a_rx);
    let others: Vec<u32> = (0..4).filter(|p| !handed.contains(p)).collect();
    let a = &a;

    a.on_received_message(Message::Have(others[0]))
        .await
        .unwrap();
    assert_eq!(told_have(&mut a_rx), [] as [u32; 0]);

    // It lacks the other handed piece and others[1], handed to no peer.
    a.on_received_message(Message::Have(handed[0]))
        .await
        .unwrap();
    assert_eq!(told_have(&mut a_rx), [others[1]]);
    assert!(h.peers.super_seeded(a.addr, h.piece(others[1])));
    assert!(h.peers.super_seeded(a.addr, h.piece(handed[1])));
    assert!(!h.peers.super_seeded(a.addr, h.piece(handed[0])));
}

// libtorrent `peer_connection::second_tick`: a peer not interested for
// more than 10 s is handed another piece, which may interest it, in place
// of the older of its two.
#[tokio::test]
async fn an_uninterested_peer_is_handed_another_piece_after_ten_seconds() {
    let h = Harness::super_seeding("super_seed_uninterested", 4, 0xf0).await;
    tokio::time::pause();
    let start = tokio::time::Instant::now();
    let (peer, mut rx) = h.new_peer("127.0.0.1:6881");
    (&peer)
        .on_received_message(Message::HaveNone)
        .await
        .unwrap();
    let handed = told_have(&mut rx);
    assert_eq!(handed.len(), 2);

    let mut idle: Task<'_> = Box::pin(peer.time_out_idle());
    let WriterRequest::Message(Message::Have(next)) = next_queued(&mut idle, &mut rx).await else {
        panic!("expected a HAVE");
    };
    assert!(tokio::time::Instant::now() - start > Duration::from_secs(10));
    assert!(!handed.contains(&next));
    assert!(h.peers.super_seeded(peer.addr, h.piece(next)));
    assert!(h.peers.super_seeded(peer.addr, h.piece(handed[1])));
    assert!(!h.peers.super_seeded(peer.addr, h.piece(handed[0])));
}

// libtorrent `peer_connection::send_allowed_set` sends nothing while
// super-seeding: the peer learns of no piece but those handed.
#[tokio::test]
async fn a_super_seed_sends_no_allowed_fast_set() {
    let h = Harness::super_seeding("super_seed_no_allowed_fast", 2, 0xc0).await;
    let (peer, mut rx) = h.peer_with("127.0.0.1:6881", &[]).await;
    h.speak_fast(&peer);
    assert_eq!(told_have(&mut rx).len(), 2);
    (&peer)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    assert!(rx.is_empty());
}

// libtorrent `torrent::files_checked` turns super-seeding off for a torrent
// that is not a seed.
#[tokio::test]
async fn a_torrent_that_is_not_a_seed_does_not_super_seed() {
    let h = Harness::super_seeding("super_seed_not_seed", 2, 0x80).await;
    let (peer, mut rx) = h.peer_with("127.0.0.1:6881", &[]).await;
    assert!((&peer).should_send_bitfield());
    assert_eq!(told_have(&mut rx), [] as [u32; 0]);
}
