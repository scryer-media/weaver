// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

// libtorrent `peer_connection::update_desired_queue_size`: four requests
// before any block arrives (`m_desired_queue_size`), one more for each block
// in slow start (`incoming_piece`); once slow start is over (`snub_peer`,
// `second_tick`), the blocks the peer delivers in `request_queue_time` at
// its pace, between two and the most outstanding; one while it is snubbed.
#[tokio::test]
async fn the_request_depth_follows_the_pace_the_peer_delivers_at() {
    let h = Harness::new("request_depth_follows_pace").await;
    let (slow, mut slow_rx) = h.peer("127.0.0.1:6881");
    let (fast, mut fast_rx) = h.peer("127.0.0.2:6881");
    tokio::time::pause();
    let depth = |peer: &PeerHandler| {
        let max = peer.lock_flow_control("test").request_window;
        h.peers
            .with_live(peer.addr, |live| live.desired_queue_size(max))
            .unwrap()
    };

    // Before any block: four, so each peer is asked for one piece.
    let mut slow_requester: Task<'_> = Box::pin(slow.request_chunks());
    slow.on_i_am_unchoked();
    let (slow_piece, _) = *next_requests(&mut slow_requester, &mut slow_rx, 4)
        .await
        .first()
        .unwrap();
    assert_eq!(depth(&slow), 4);
    assert!(!slow.can_send_request());
    let mut fast_requester: Task<'_> = Box::pin(fast.request_chunks());
    fast.on_i_am_unchoked();
    let (fast_piece, _) = *next_requests(&mut fast_requester, &mut fast_rx, 4)
        .await
        .first()
        .unwrap();

    // A block in slow start: one more.
    h.deliver(&fast, (fast_piece, 0)).await;
    assert_eq!(depth(&fast), 5);

    // Snubbed: one, and slow start is over. A block the moment it was asked
    // for lifts the snub: every request we allow, and no more than the
    // peer's `reqq`.
    h.peers
        .with_live_mut(fast.addr, "test", |live| live.snub())
        .unwrap();
    assert_eq!(depth(&fast), 1);
    h.deliver(&fast, (fast_piece, CHUNK_SIZE)).await;
    assert_eq!(depth(&fast), DEFAULT_PEER_REQUEST_WINDOW);
    let mut hs = ExtendedHandshake::new();
    hs.reqq = Some(5);
    (&fast).on_extended_handshake(&hs).unwrap();
    assert_eq!(depth(&fast), 5);

    // Snubbed: one.
    h.peers
        .with_live_mut(fast.addr, "test", |live| live.snub())
        .unwrap();
    assert_eq!(depth(&fast), 1);

    // A block after a second, in slow start: one more.
    tokio::time::advance(Duration::from_secs(1)).await;
    h.deliver(&slow, (slow_piece, 0)).await;
    assert_eq!(depth(&slow), 5);

    // None in the second after: slow start ends with it. A block every two
    // seconds: one and a half in three, so the least, two.
    tokio::time::advance(Duration::from_secs(3)).await;
    h.deliver(&slow, (slow_piece, CHUNK_SIZE)).await;
    assert_eq!(depth(&slow), 2);
}

#[tokio::test]
async fn a_choke_returns_every_outstanding_request_and_unchoke_asks_again() {
    let h = Harness::new("a_choke_returns_requests").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(peer.task_peer_chunk_requester());

    peer.on_i_am_unchoked();
    let taken = h.request_every_chunk(&peer, &mut requester, &mut rx).await;
    let other = 1 - taken;

    peer.on_i_am_choked().unwrap();

    // Nothing is in flight to the peer, and both pieces are back in the
    // queue for any peer, the delivered chunk kept.
    assert_eq!(h.inflight_count(&peer), 0);
    assert_eq!(h.queued_pieces(), vec![0, 1]);
    assert_eq!(h.missing_chunks(taken), vec![1, 2, 3]);
    assert_eq!(h.missing_chunks(other), vec![0, 1, 2, 3]);

    // A block the peer had already sent is let through, and kept: we lack
    // it.
    h.deliver(&peer, (taken, CHUNK_SIZE)).await;
    assert_eq!(h.missing_chunks(taken), vec![2, 3]);

    peer.on_i_am_unchoked();
    let mut again = chunks(taken, 2..CHUNKS_PER_PIECE);
    again.extend(chunks(other, 0..CHUNKS_PER_PIECE));
    assert_eq!(
        next_requests(&mut requester, &mut rx, again.len()).await,
        again
    );
}

impl Harness {
    /// Polls `task` until a piece goes back to the queue.
    async fn until_released(&self, task: &mut Task<'_>) {
        let released = self.new_pieces_notify.notified();
        tokio::select! {
            biased;
            () = released => {}
            res = task.as_mut() => panic!("the peer task ended: {res:?}"),
        }
    }

    /// Whether `peer` owns piece `index`.
    fn owns(&self, peer: &PeerHandler, index: u32) -> bool {
        self.live
            .lock_read("test")
            .get_pieces()
            .unwrap()
            .is_owned_by(self.piece(index), peer.addr)
    }
}

/// The cancels queued for the peer, draining its queue.
fn cancels_sent(rx: &mut PeerRx) -> BTreeSet<(u32, u32)> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|r| match r {
            WriterRequest::Message(Message::Cancel(r)) => Some((r.index, r.begin)),
            _ => None,
        })
        .collect()
}

// libtorrent `peer_connection::snub_peer`: a peer that goes its request
// timeout without a block is snubbed, and a request whose piece has no free
// block left times out: the piece is free for other peers, while the
// request stays in the download queue.
#[tokio::test]
async fn an_overdue_piece_is_free_again_at_the_timeout_and_its_requests_stay() {
    let h = Harness::new("a_request_times_out").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(peer.request_chunks());

    // No block has come from the peer yet: it is asked for one piece.
    peer.on_i_am_unchoked();
    let asked = next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await;
    let (piece, _) = *asked.first().unwrap();
    assert_eq!(asked, chunks(piece, 0..CHUNKS_PER_PIECE));
    drop(requester);

    // The first block takes 10 s, so the peer's timeout is 12 s from it.
    tokio::time::advance(Duration::from_secs(10)).await;
    h.deliver(&peer, (piece, 0)).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    assert_eq!(
        h.peers
            .with_live(peer.addr, |live| live.request_deadline())
            .unwrap(),
        Some(deadline)
    );

    // Nothing more arrives: once the deadline passes, and not before, the
    // peer is snubbed and its piece, which waits on it alone, goes back to
    // the queue with the delivered chunk. No request is cancelled.
    let mut watcher: Task<'_> = Box::pin(peer.time_out_requests());
    h.until_released(&mut watcher).await;
    assert!(tokio::time::Instant::now() >= deadline);
    assert!(h.snubbed(&peer));
    assert_eq!(h.queued_pieces(), vec![0, 1]);
    assert_eq!(h.missing_chunks(piece), vec![1, 2, 3]);
    assert_eq!(h.missing_chunks(1 - piece), vec![0, 1, 2, 3]);
    assert_eq!(h.inflight_count(&peer), CHUNKS_PER_PIECE as usize - 1);
    assert!(rx.is_empty());
}

// A request asks the limits for nothing: a block is paid for as it crosses
// the socket (libtorrent `peer_connection::send_block_requests` asks no
// bandwidth; `setup_receive` reads within the quota). Requests that time out
// or are rejected leave the torrent's and the session's budgets untouched,
// and a cap of one block a second holds back none of them.
#[tokio::test]
async fn a_rejected_or_timed_out_request_leaves_the_budget_untouched() {
    let h = Harness::new("requests_spend_no_budget").await;
    let cap = crate::limits::LimitsConfig {
        upload_bps: LimitKind::Unlimited,
        download_bps: LimitKind::Bps(std::num::NonZeroU32::new(CHUNK_SIZE).unwrap()),
    };
    h.shared.options.ratelimits.set_config(cap);
    h._session.ratelimits.set_config(cap);
    let granted = [
        h.shared.options.ratelimits.watch_download_granted(),
        h._session.ratelimits.watch_download_granted(),
    ];
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(peer.request_chunks());
    peer.on_i_am_unchoked();
    let asked = next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await;
    drop(requester);

    // The requests time out: the peer is snubbed and its piece goes back.
    let mut watcher: Task<'_> = Box::pin(peer.time_out_requests());
    h.until_released(&mut watcher).await;
    drop(watcher);
    assert!(h.snubbed(&peer));
    // And the peer rejects them.
    for (index, begin) in asked {
        (&peer)
            .on_received_message(Message::Reject(request(index, begin / CHUNK_SIZE)))
            .await
            .unwrap();
    }
    assert_eq!(h.inflight_count(&peer), 0);
    for granted in &granted {
        assert_eq!(*granted.borrow(), 0);
    }
}

// libtorrent `peer_connection::second_tick` snubs a peer, or times out its
// pieces, only while it reads the peer's socket (`bw_network`) with quota
// held: a peer whose reads wait on the download limits has no deadline, and
// once they resume its timeouts count from then.
#[tokio::test]
async fn a_peer_waiting_on_the_download_limits_is_not_timed_out() {
    let h = Harness::new("quota_wait_is_not_a_timeout").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(peer.request_chunks());
    peer.on_i_am_unchoked();
    next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await;
    drop(requester);
    let deadline = || {
        h.peers
            .with_live(peer.addr, |live| live.request_deadline())
            .unwrap()
    };
    let timeout = deadline().unwrap() - tokio::time::Instant::now();

    h.shared
        .options
        .ratelimits
        .set_download_bps(LimitKind::Closed);
    let handler = &peer;
    let mut read = std::pin::pin!(handler.request_bandwidth(Channel::Download, 100));
    assert_eq!((&mut read).now_or_never(), None);
    assert_eq!(deadline(), None);
    tokio::time::advance(Duration::from_secs(600)).await;
    assert_eq!(deadline(), None);

    h.shared
        .options
        .ratelimits
        .set_download_bps(LimitKind::Unlimited);
    assert_eq!((&mut read).now_or_never(), Some(100));
    assert_eq!(deadline(), Some(tokio::time::Instant::now() + timeout));
}

#[tokio::test]
async fn a_late_block_for_a_released_piece_completes_its_chunk() {
    // libtorrent `peer_connection::incoming_piece`: a block of a request
    // that timed out is written when no other copy came first, and the
    // other peer's request for it is cancelled (`torrent::cancel_block`).
    let h = Harness::new("a_late_block_is_kept").await;
    let (slow, mut slow_rx) = h.peer("127.0.0.1:6881");
    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(slow.request_chunks());
    slow.on_i_am_unchoked();
    let asked = next_requests(&mut requester, &mut slow_rx, CHUNKS_PER_PIECE as usize).await;
    let (piece, _) = *asked.first().unwrap();
    drop(requester);
    let mut watcher: Task<'_> = Box::pin(slow.time_out_requests());
    h.until_released(&mut watcher).await;
    drop(watcher);

    // Another peer takes the piece and asks for all of it.
    let (other, mut other_rx) = h.peer_with("127.0.0.2:6881", &[piece]).await;
    let mut requester: Task<'_> = Box::pin(other.request_chunks());
    other.on_i_am_unchoked();
    assert_eq!(
        next_requests(&mut requester, &mut other_rx, CHUNKS_PER_PIECE as usize).await,
        chunks(piece, 0..CHUNKS_PER_PIECE)
    );
    drop(requester);
    assert!(h.owns(&other, piece));

    // The slow peer's block arrives late: it is kept, and the other peer
    // need not send it.
    h.deliver(&slow, (piece, CHUNK_SIZE)).await;
    assert_eq!(h.missing_chunks(piece), vec![0, 2, 3]);
    assert_eq!(cancels_sent(&mut other_rx), chunks(piece, 1..2));

    // So are the rest: the piece is complete, and passes its check.
    for chunk in [0, 2, 3] {
        h.deliver(&slow, (piece, chunk * CHUNK_SIZE)).await;
    }
    assert_eq!(cancels_sent(&mut other_rx), chunks(piece, [0, 2, 3]));
    assert!(
        h.lock_read("test")
            .get_chunks()
            .unwrap()
            .is_piece_have(h.piece(piece))
    );
    assert_eq!(h.inflight_count(&slow), 0);
    assert_eq!(h.inflight_count(&other), 0);
}

#[tokio::test]
async fn a_snubbed_peer_keeps_the_requests_of_pieces_that_still_have_free_blocks() {
    // libtorrent `peer_connection::snub_peer`: while its piece has a free
    // block, a request does not hold it up, and does not time out.
    let h = Harness::new("a_snub_keeps_free_pieces").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    peer.on_i_am_unchoked();
    let piece = peer.acquire_next_piece().unwrap().piece().unwrap().get();
    let ask = |blocks: std::ops::Range<u32>| {
        for block in blocks {
            h.peers
                .with_live_mut(peer.addr, "test", |live| {
                    live.add_inflight_request(h.chunk(piece, block))
                })
                .unwrap();
        }
    };

    // Two blocks of its piece are asked of it, two are free: the snub
    // leaves it the piece and the requests.
    ask(0..2);
    peer.on_requests_timed_out().unwrap();
    assert!(h.snubbed(&peer));
    assert!(h.owns(&peer, piece));
    assert_eq!(h.inflight_count(&peer), 2);

    // Every block is asked of it: the piece waits on it alone, and the
    // next snub frees it. The requests stay, and none is cancelled.
    ask(2..CHUNKS_PER_PIECE);
    peer.on_requests_timed_out().unwrap();
    assert!(!h.owns(&peer, piece));
    assert!(h.queued_pieces().contains(&piece));
    assert_eq!(h.inflight_count(&peer), CHUNKS_PER_PIECE as usize);
    assert!(cancels_sent(&mut rx).is_empty());
}

#[tokio::test]
async fn a_snub_gives_back_one_piece_the_newest_that_waits_on_the_peer_alone() {
    // libtorrent `peer_connection::snub_peer` times out one request a snub,
    // the last of the download queue, and `second_tick` snubs again each
    // second the peer stays overdue.
    let h = Harness::new("a_snub_gives_back_one_piece").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    tokio::time::pause();
    peer.on_i_am_unchoked();
    let take_and_ask = || {
        let piece = peer.acquire_next_piece().unwrap().piece().unwrap().get();
        for block in 0..CHUNKS_PER_PIECE {
            h.peers
                .with_live_mut(peer.addr, "test", |live| {
                    live.add_inflight_request(h.chunk(piece, block))
                })
                .unwrap();
        }
        piece
    };
    let older = take_and_ask();
    tokio::time::advance(Duration::from_secs(1)).await;
    let newer = take_and_ask();

    peer.on_requests_timed_out().unwrap();
    assert!(!h.owns(&peer, newer));
    assert!(h.owns(&peer, older));
    assert!(h.queued_pieces().contains(&newer));

    peer.on_requests_timed_out().unwrap();
    assert!(!h.owns(&peer, older));
    assert!(h.queued_pieces().contains(&older));

    // Every request stays outstanding, and none is cancelled.
    assert_eq!(h.inflight_count(&peer), 2 * CHUNKS_PER_PIECE as usize);
    assert!(cancels_sent(&mut rx).is_empty());
}

#[tokio::test]
async fn a_parole_peer_is_not_snubbed_of_its_requests() {
    // libtorrent `peer_connection::snub_peer` returns on parole before it
    // times out any request: a paroled peer was handed only a piece no one
    // else had started. It is marked snubbed first all the same.
    let h = Harness::new("a_parole_peer_keeps_its_requests").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    h.peers
        .with_peer_mut(peer.addr, "test", |p| p.piece_failed(false, &h.peers))
        .unwrap();
    peer.on_i_am_unchoked();
    let piece = peer.acquire_next_piece().unwrap().piece().unwrap().get();
    for block in 0..CHUNKS_PER_PIECE {
        h.peers
            .with_live_mut(peer.addr, "test", |live| {
                live.add_inflight_request(h.chunk(piece, block))
            })
            .unwrap();
    }

    peer.on_requests_timed_out().unwrap();
    assert!(h.snubbed(&peer));
    assert!(h.owns(&peer, piece));
    assert!(!h.queued_pieces().contains(&piece));
    assert_eq!(h.inflight_count(&peer), CHUNKS_PER_PIECE as usize);
    assert!(cancels_sent(&mut rx).is_empty());
}

#[tokio::test]
async fn a_block_that_arrives_after_its_cancel_restarts_the_wait_for_the_next() {
    // libtorrent peer_connection::incoming_piece, for a block it had
    // stopped wanting: the block's time is sampled and, with requests still
    // outstanding, the request timer starts again; the snub stays.
    let h = Harness::new("a_late_block_restarts_the_wait").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(peer.request_chunks());
    peer.on_i_am_unchoked();
    let asked = next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await;
    let (piece, _) = *asked.first().unwrap();
    drop(requester);

    tokio::time::advance(Duration::from_secs(10)).await;
    h.deliver(&peer, (piece, 0)).await;
    let deadline = |h: &Harness| {
        h.peers
            .with_live(peer.addr, |live| {
                (live.request_deadline(), live.request_timeout())
            })
            .unwrap()
    };
    let (first, _) = deadline(&h);
    let cancelled = h.chunk(piece, 1);
    h.peers
        .with_live_mut(peer.addr, "test", |live| {
            live.cancel_inflight_request(&cancelled);
            live.snub();
        })
        .unwrap();

    // The cancelled block arrives anyway, 5 s on.
    tokio::time::advance(Duration::from_secs(5)).await;
    h.deliver(&peer, (piece, CHUNK_SIZE)).await;
    // Its timeout, 27.5 s, passes the 20 s piece timeout, which comes
    // first (libtorrent `settings_pack::piece_timeout`).
    let (moved, timeout) = deadline(&h);
    let piece_timeout = Duration::from_secs(20);
    assert!(timeout > piece_timeout);
    assert_eq!(moved, Some(tokio::time::Instant::now() + piece_timeout));
    assert!(moved > first);
    assert!(h.snubbed(&peer));
    assert_eq!(h.inflight_count(&peer), CHUNKS_PER_PIECE as usize - 2);
}

impl Harness {
    /// Whether the peer stats snapshot shows the peer at `peer` snubbed.
    fn snubbed(&self, peer: &PeerHandler) -> bool {
        self.per_peer_stats_snapshot(Default::default()).peers[&peer.addr.to_string()].snubbed
    }
}

#[tokio::test]
async fn a_timed_out_peer_keeps_its_requests_and_is_asked_for_no_more() {
    let h = Harness::new("a_timed_out_peer_is_snubbed").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    tokio::time::pause();
    let mut task: Task<'_> = Box::pin(peer.task_peer_chunk_requester());

    // No block has come from the peer yet: it is asked for one piece.
    peer.on_i_am_unchoked();
    let first = next_requests(&mut task, &mut rx, CHUNKS_PER_PIECE as usize).await;
    assert!(!h.snubbed(&peer));

    let (piece, _) = *first.first().unwrap();
    let other = 1 - piece;

    // No block arrives: the peer is snubbed, and its piece, which waits on
    // it alone, goes back to the queue. Its four requests stay outstanding,
    // past the one a snubbed peer may hold, so it is asked for no more.
    h.until_released(&mut task).await;
    assert!(h.snubbed(&peer));
    assert_eq!(h.queued_pieces(), vec![0, 1]);
    assert_eq!(h.inflight_count(&peer), CHUNKS_PER_PIECE as usize);
    assert!(!peer.can_send_request());

    // A block comes past the request timeout: it is kept, and the peer
    // stays snubbed (libtorrent `incoming_piece` unsnubs only within the
    // request timeout). The next comes at once, and lifts the snub.
    let timeout = h
        .peers
        .with_live(peer.addr, |live| live.request_timeout())
        .unwrap();
    tokio::time::advance(timeout).await;
    h.deliver(&peer, (piece, 0)).await;
    assert!(h.snubbed(&peer));
    assert_eq!(h.missing_chunks(piece), vec![1, 2, 3]);
    h.deliver(&peer, (piece, CHUNK_SIZE)).await;
    assert!(!h.snubbed(&peer));

    // The rest complete the piece, and the requester asks for the other
    // piece, as deep as the peer's pace allows.
    for chunk in 2..CHUNKS_PER_PIECE {
        h.deliver(&peer, (piece, chunk * CHUNK_SIZE)).await;
    }
    assert!(
        h.lock_read("test")
            .get_chunks()
            .unwrap()
            .is_piece_have(h.piece(piece))
    );
    let depth = h
        .peers
        .with_live(peer.addr, |live| live.desired_queue_size(usize::MAX))
        .unwrap();
    assert_eq!(
        next_requests(&mut task, &mut rx, depth).await,
        chunks(other, 0..u32::try_from(depth).unwrap())
    );
}

// libtorrent `request_a_block` passes over a block already in the peer's
// queues (request_blocks.cpp:238-256), and `snub_peer` asks for a new block
// before it times one out so as not to pick that block again
// (peer_connection.cpp:5217-5233). A slow peer whose piece went back at a
// snub, and that then catches up, is not handed that piece again while its
// requests for it stand: the piece stays free for other peers, the peer is
// asked for the other piece, and no block is asked of it twice.
#[tokio::test]
async fn a_peer_is_not_handed_back_a_piece_it_gave_up_while_its_requests_for_it_stand() {
    let h = Harness::new("a_given_up_piece_is_not_taken_back").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    tokio::time::pause();
    let mut task: Task<'_> = Box::pin(peer.task_peer_chunk_requester());
    peer.on_i_am_unchoked();
    let first = next_requests(&mut task, &mut rx, CHUNKS_PER_PIECE as usize).await;
    let (piece, _) = *first.first().unwrap();
    let other = 1 - piece;

    // No block arrives: the peer is snubbed and its piece goes back, its
    // four requests outstanding still.
    h.until_released(&mut task).await;
    assert_eq!(h.queued_pieces(), vec![0, 1]);

    // The peer catches up: a block past its timeout, then two at once, the
    // first of which lifts the snub. One of its requests for the piece
    // stands.
    let timeout = h
        .peers
        .with_live(peer.addr, |live| live.request_timeout())
        .unwrap();
    tokio::time::advance(timeout).await;
    for chunk in 0..3 {
        h.deliver(&peer, (piece, chunk * CHUNK_SIZE)).await;
    }
    assert!(!h.snubbed(&peer));
    assert_eq!(h.inflight_count(&peer), 1);

    // Its next pick is the other piece: the one it gave up, the started
    // one, has nothing left to ask it, and stays queued for other peers.
    // The requester asks for it, and asks for no block of the given-up
    // piece again.
    let depth = h
        .peers
        .with_live(peer.addr, |live| live.desired_queue_size(usize::MAX))
        .unwrap();
    let room = u32::try_from(depth - 1).unwrap().min(CHUNKS_PER_PIECE);
    assert!(room > 0);
    assert_eq!(
        next_requests(&mut task, &mut rx, room as usize).await,
        chunks(other, 0..room)
    );
    drop(task);
    assert!(h.owns(&peer, other));
    assert!(!h.owns(&peer, piece));
    assert!(h.queued_pieces().contains(&piece));
    assert_eq!(h.stats.asked_already.load(Ordering::Relaxed), 0);

    // Its standing request completes the piece, every byte used.
    h.deliver(&peer, (piece, 3 * CHUNK_SIZE)).await;
    assert!(
        h.lock_read("test")
            .get_chunks()
            .unwrap()
            .is_piece_have(h.piece(piece))
    );
    assert_eq!(h.get_redundant_bytes(), 0);
}

#[tokio::test]
async fn a_snubbed_peer_takes_no_piece_from_another_peer() {
    let h = Harness::new("a_snubbed_peer_does_not_steal").await;
    tokio::time::pause();
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    owner.on_i_am_unchoked();
    assert!(owner.acquire_next_piece().unwrap().piece().is_some());
    assert!(owner.acquire_next_piece().unwrap().piece().is_some());
    assert_eq!(h.queued_pieces(), Vec::<u32>::new());

    // Two peers that take a second a piece; the owner has held both pieces
    // far longer than that.
    let (snubbed, _snubbed_rx) = h.peer("127.0.0.2:6881");
    let (fast, _fast_rx) = h.peer("127.0.0.3:6881");
    for peer in [&snubbed, &fast] {
        peer.counters
            .on_piece_completed(PIECE_LENGTH.into(), Duration::from_secs(1));
        peer.on_i_am_unchoked();
    }
    h.peers
        .with_live_mut(snubbed.addr, "test", |live| live.snub())
        .unwrap();
    tokio::time::advance(Duration::from_secs(100)).await;

    // In the end game it may double a block, but both pieces stay the
    // owner's.
    snubbed.acquire_next_piece().unwrap().piece();
    let owned = |p| {
        h.live
            .lock_read("test")
            .get_pieces()
            .unwrap()
            .is_owned_by(h.piece(p), owner.addr)
    };
    assert!(owned(0) && owned(1));
    assert!(fast.acquire_next_piece().unwrap().piece().is_some());
}

/// A memory storage that gives back room each time it takes a piece, and
/// rings whoever last waited on room when it does.
#[derive(Clone, Default)]
struct RoomStorageFactory {
    inner: MemoryStorageFactory,
    room: Arc<Mutex<Option<Arc<Notify>>>>,
}

struct RoomStorage {
    inner: Box<dyn TorrentStorage>,
    room: Arc<Mutex<Option<Arc<Notify>>>>,
}

impl StorageFactory for RoomStorageFactory {
    type Storage = RoomStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<RoomStorage> {
        Ok(RoomStorage {
            inner: Box::new(self.inner.create(shared, metadata)?),
            room: self.room.clone(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

impl TorrentStorage for RoomStorage {
    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.inner.init(shared, metadata)
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.inner.pread_exact(file_id, offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.inner.pwrite_all(file_id, offset, buf)
    }

    fn remove_file(&self, file_id: usize, filename: &std::path::Path) -> anyhow::Result<()> {
        self.inner.remove_file(file_id, filename)
    }

    fn remove_directory_if_empty(&self, path: &std::path::Path) -> anyhow::Result<()> {
        self.inner.remove_directory_if_empty(path)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.inner.ensure_file_length(file_id, length)
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(RoomStorage {
            inner: self.inner.take()?,
            room: self.room.clone(),
        }))
    }

    fn reserve_piece(&self, _piece: ValidPieceIndex, room: &Arc<Notify>) -> bool {
        *self.room.lock() = Some(Arc::clone(room));
        true
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.inner.on_piece_completed(piece)?;
        if let Some(room) = &*self.room.lock() {
            room.notify_waiters();
        }
        Ok(())
    }
}

/// A requester polled by hand, and whether it was woken since it was last
/// polled.
struct Polled<'a> {
    task: Task<'a>,
    waker: Arc<WakeCount>,
}

/// Counts a requester's wakes, and marks it woken.
#[derive(Default)]
struct WakeCount {
    woken: AtomicBool,
    wakes: AtomicUsize,
}

impl std::task::Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.woken.store(true, Ordering::SeqCst);
    }
}

/// Polls each requester that was woken until none is: every one waits on
/// something again.
fn settle(requesters: &mut [Polled<'_>]) {
    while let Some(r) = requesters
        .iter_mut()
        .find(|r| r.waker.woken.swap(false, Ordering::SeqCst))
    {
        let waker = std::task::Waker::from(r.waker.clone());
        let poll = r
            .task
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker));
        assert!(poll.is_pending(), "a requester ended: {poll:?}");
    }
}

// The requester wakes only for what it can act on: a block delivered or
// cancelled frees one of its request slots, an unchoke opens them, a piece
// may become one it can ask for, and the torrent finishing ends its wants.
// A piece the storage takes, and the room it gives back, is none of these
// for a requester that was not refused room. Eight peers download eight
// pieces, one block at a time, each requester settling before the next
// block; the first peer never answers, so the end game fetches its piece
// from the others.
#[tokio::test]
async fn a_requester_in_the_end_game_wakes_only_for_what_it_can_act_on() {
    const PEERS: usize = 8;
    const PIECES_HERE: u32 = 8;
    let add = AddTorrentOptions {
        storage_factory: Some(RoomStorageFactory::default().clone_box()),
        ..Default::default()
    };
    let h = Harness::build_added(
        "end_game_wakes",
        PIECES_HERE,
        0x00,
        isolated_session_opts(false),
        add,
    )
    .await;
    tokio::time::pause();

    let (peers, mut rxs): (Vec<_>, Vec<_>) = (0..PEERS)
        .map(|i| h.peer(&format!("127.0.0.{}:6881", i + 1)))
        .unzip();
    let mut requesters: Vec<Polled<'_>> = peers
        .iter()
        .map(|peer| {
            peer.on_i_am_unchoked();
            let waker = Arc::new(WakeCount::default());
            waker.woken.store(true, Ordering::SeqCst);
            Polled {
                task: Box::pin(peer.request_chunks()),
                waker,
            }
        })
        .collect();
    let wakes = |requesters: &[Polled<'_>]| {
        requesters
            .iter()
            .map(|r| r.waker.wakes.load(Ordering::SeqCst))
            .sum::<usize>()
    };

    // Every request but the first peer's is answered, in the order asked,
    // unless cancelled first.
    let mut pending = std::collections::VecDeque::new();
    let mut asked = BTreeSet::new();
    let mut doubled = BTreeSet::new();
    let mut delivered = 0;
    let mut cancelled = 0;
    while !(0..PIECES_HERE).all(|p| h.have(p)) {
        settle(&mut requesters);
        for (i, rx) in rxs.iter_mut().enumerate() {
            while let Ok(sent) = rx.try_recv() {
                match sent {
                    WriterRequest::Message(Message::Request(r)) => {
                        if !asked.insert((r.index, r.begin)) {
                            doubled.insert((r.index, r.begin));
                        }
                        if i != 0 {
                            pending.push_back((i, (r.index, r.begin)));
                        }
                    }
                    WriterRequest::Message(Message::Cancel(r)) => {
                        cancelled += 1;
                        pending.retain(|&(j, b)| (j, b) != (i, (r.index, r.begin)));
                    }
                    _ => {}
                }
            }
        }
        let (i, block) = pending
            .pop_front()
            .expect("the requesters wait while no request is outstanding");
        h.deliver(&peers[i], block).await;
        delivered += 1;
    }
    let before_finish = wakes(&requesters);
    settle(&mut requesters);

    // The first peer's piece came from another peer: its blocks were asked
    // twice.
    assert!(!doubled.is_empty());
    let wakes = wakes(&requesters);
    // Each block delivered or cancelled frees a slot of one requester, each
    // is unchoked once, each piece may become one to ask for, and each
    // hears once that the end game began and once that the torrent finished.
    let actionable = delivered + cancelled + PEERS + PIECES_HERE as usize + PEERS + PEERS;
    assert!(
        wakes <= actionable,
        "{wakes} wakes ({before_finish} before the finish) for {actionable} actionable events \
         ({delivered} delivered, {cancelled} cancelled)"
    );
}

/// A seeder played by the test: it sends the blocks asked of it in the
/// order asked, one each [`PlayedSeeder::block_time`]. The first
/// [`SEND_AHEAD`] of its queue are already on their way, so a Cancel stops
/// only those behind them.
struct PlayedSeeder {
    queue: std::collections::VecDeque<(u32, u32)>,
    slow: Duration,
    sent: u32,
    next_block_at: Option<tokio::time::Instant>,
}

impl PlayedSeeder {
    /// A millisecond a block, but `slow` for every other [`PHASE`] blocks
    /// it sends: a peer whose pace swings, as on a public swarm.
    fn block_time(&self) -> Duration {
        if (self.sent / PHASE) % 2 == 1 {
            self.slow
        } else {
            Duration::from_millis(1)
        }
    }
}

const PHASE: u32 = 32;
const SEND_AHEAD: usize = 4;
const WINDOW: usize = 16;

// A block is asked of a second peer only in the end game, once every queued
// piece has a block received: before it no block is asked twice and none
// arrives that is had already, and in it a block arrives only as often as it
// was asked for, so each copy wasted answers a request beyond the first for
// its block. One leecher and six seeders with every piece, over 256 pieces,
// at most 16 requests out to each; the first seeder sends a block a
// millisecond, the others swing between that and 40 to 200 ms. The clock is
// paused and moved to each block's arrival.
#[tokio::test]
async fn only_the_end_game_fetches_a_block_already_had() {
    const SEEDERS: usize = 6;
    const PIECES_HERE: u32 = 256;
    let out = TempDir::with_prefix("waste").unwrap();
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
        .await
        .unwrap();
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        peer_opts: Some(crate::PeerConnectionOptions {
            max_request_window: Some(WINDOW),
            ..Default::default()
        }),
        ..Default::default()
    };
    let h = Harness::added_to(
        session,
        out,
        "waste",
        PIECES_HERE,
        &[0x00; PIECES_HERE as usize / 8],
        add,
    )
    .await;
    tokio::time::pause();

    let (peers, mut rxs): (Vec<_>, Vec<_>) = (0..SEEDERS)
        .map(|i| h.peer(&format!("127.0.0.{}:6881", i + 1)))
        .unzip();
    let mut seeders: Vec<PlayedSeeder> = (0..SEEDERS)
        .map(|i| PlayedSeeder {
            queue: Default::default(),
            slow: Duration::from_millis(40 * i as u64).max(Duration::from_millis(1)),
            sent: 0,
            next_block_at: None,
        })
        .collect();
    let mut requesters: Vec<Polled<'_>> = peers
        .iter()
        .map(|peer| {
            peer.on_i_am_unchoked();
            let waker = Arc::new(WakeCount::default());
            waker.woken.store(true, Ordering::SeqCst);
            Polled {
                task: Box::pin(peer.request_chunks()),
                waker,
            }
        })
        .collect();
    let end_game = || {
        h.live
            .lock_read("test")
            .get_pieces()
            .unwrap()
            .chunks()
            .every_queued_piece_started()
    };

    let mut asked = BTreeSet::new();
    let mut asked_again = 0;
    let mut asked_again_before_end_game = 0;
    let mut wasted_before_end_game = 0;
    while !(0..PIECES_HERE).all(|p| h.have(p)) {
        settle(&mut requesters);
        let now = tokio::time::Instant::now();
        let in_end_game = end_game();
        for (seeder, rx) in seeders.iter_mut().zip(&mut rxs) {
            while let Ok(sent) = rx.try_recv() {
                match sent {
                    WriterRequest::Message(Message::Request(r)) => {
                        if !asked.insert((r.index, r.begin)) {
                            asked_again += 1;
                            if !in_end_game {
                                asked_again_before_end_game += 1;
                            }
                        }
                        if seeder.queue.is_empty() {
                            seeder.next_block_at = Some(now + seeder.block_time());
                        }
                        seeder.queue.push_back((r.index, r.begin));
                    }
                    WriterRequest::Message(Message::Cancel(r)) => {
                        let at = seeder.queue.iter().position(|b| *b == (r.index, r.begin));
                        if let Some(at) = at.filter(|at| *at >= SEND_AHEAD) {
                            seeder.queue.remove(at);
                        }
                    }
                    _ => {}
                }
            }
        }
        let (i, at) = seeders
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.next_block_at.map(|at| (i, at)))
            .min_by_key(|(_, at)| *at)
            .expect("the requesters wait while no request is outstanding");
        tokio::time::advance(at - now).await;
        let seeder = &mut seeders[i];
        let block = seeder.queue.pop_front().unwrap();
        seeder.sent += 1;
        seeder.next_block_at = (!seeder.queue.is_empty()).then(|| at + seeder.block_time());
        let wasted = h.get_redundant_bytes();
        h.deliver(&peers[i], block).await;
        if !in_end_game {
            wasted_before_end_game += h.get_redundant_bytes() - wasted;
        }
    }
    settle(&mut requesters);

    let wasted = h.get_redundant_bytes();
    assert_eq!(
        (asked_again_before_end_game, wasted_before_end_game),
        (0, 0),
        "blocks asked twice, and bytes wasted, before the end game"
    );
    // The seeders send only blocks asked of them, once a request, and the
    // first copy of each block is kept.
    assert!(
        wasted <= asked_again * u64::from(CHUNK_SIZE),
        "{wasted} B wasted, {asked_again} requests for a block asked before"
    );
}
