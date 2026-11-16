// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

#[tokio::test]
async fn the_padding_bits_of_a_bitfield_are_cleared_at_ingest() {
    let h = Harness::new("bitfield_padding_cleared").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;

    // Piece 1 and every padding bit.
    peer.on_received_message(Message::Bitfield(ByteBuf(&[0b0111_1111])))
        .await
        .unwrap();
    let bitfield = h
        .peers
        .with_live(peer.addr, |live| live.bitfield.clone())
        .unwrap();
    assert_eq!(bitfield.as_raw_slice(), [0b0100_0000]);
    assert_eq!(h.peers.availability.counts(), [0, 1]);
}

#[tokio::test]
async fn a_bitfield_that_is_not_one_bit_per_piece_ends_the_connection() {
    let h = Harness::new("bitfield_wrong_length").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;

    for payload in [&[][..], &[0b1100_0000, 0][..]] {
        assert!(
            peer.on_received_message(Message::Bitfield(ByteBuf(payload)))
                .await
                .is_err()
        );
    }
    assert_eq!(h.peers.availability.counts(), [1, 1]);
}

#[tokio::test]
async fn a_have_at_or_past_the_piece_count_ends_the_connection() {
    let h = Harness::new("have_past_piece_count").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    peer.on_received_message(Message::Bitfield(ByteBuf(&[0])))
        .await
        .unwrap();

    peer.on_received_message(Message::Have(1)).await.unwrap();
    assert_eq!(h.peers.availability.counts(), [0, 1]);
    for have in [PIECES, 7, u32::MAX] {
        assert!(peer.on_received_message(Message::Have(have)).await.is_err());
    }
    assert_eq!(h.peers.availability.counts(), [0, 1]);
}

impl Harness {
    /// A live torrent of eight four-chunk pieces, none of them had, and no
    /// peers.
    async fn eight_pieces(name: &str) -> Self {
        let add = AddTorrentOptions {
            storage_factory: Some(MemoryStorageFactory::default().clone_box()),
            ..Default::default()
        };
        Self::build_added(name, 8, 0x00, isolated_session_opts(false), add).await
    }

    /// The pieces `peer` has, as its bitfield's bytes.
    fn pieces_of(&self, peer: &PeerHandler) -> Vec<u8> {
        self.peers
            .with_live(peer.addr, |live| live.bitfield.as_raw_slice().to_vec())
            .unwrap()
    }
}

// libtorrent `peer_connection::incoming_have`, `incoming_request` and
// `incoming_piece` begin with `incoming_have_none` when no bitfield came:
// it was omitted, which is the same as Have None. The peer's pieces are
// known, as none, from its first message on, and what waits on them runs.
#[tokio::test]
async fn a_first_message_that_is_not_a_bitfield_makes_the_peers_pieces_known_as_none() {
    let h = Harness::new("first_message_not_a_bitfield").await;
    for (n, first) in [Message::Interested, Message::Unchoke, Message::KeepAlive]
        .into_iter()
        .enumerate()
    {
        let (peer, _rx) = h.new_peer(&format!("127.0.0.{}:6881", n + 1));
        assert!(
            futures::FutureExt::now_or_never(peer.wait_for_bitfield()).is_none(),
            "known before the first message"
        );
        (&peer).on_received_message(first).await.unwrap();
        assert_eq!(h.pieces_of(&peer), [0]);
        assert!(futures::FutureExt::now_or_never(peer.wait_for_bitfield()).is_some());
    }
    assert_eq!(h.peers.availability.counts(), [0, 0]);
}

// libtorrent `incoming_interested` leaves the pieces at none until a HAVE
// (`incoming_have`): the requester of a peer whose first message is
// Interested runs, and asks for a piece the peer comes to have.
#[tokio::test]
async fn a_peer_whose_first_message_is_interested_is_asked_once_it_has_a_wanted_piece() {
    let h = Harness::new("first_message_interested").await;
    tokio::time::pause();
    let (peer, mut rx) = h.new_peer("127.0.0.1:6881");
    (&peer)
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    peer.on_i_am_unchoked();
    let mut requester: Task<'_> = Box::pin(peer.request_chunks());
    assert!(waiting(&mut requester, &mut rx));
    assert!(!h.interesting(&peer));

    (&peer).on_received_message(Message::Have(1)).await.unwrap();
    assert!(matches!(
        next_queued(&mut requester, &mut rx).await,
        WriterRequest::Message(Message::Interested)
    ));
    assert_eq!(
        next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await,
        chunks(1, 0..CHUNKS_PER_PIECE)
    );
}

// libtorrent `incoming_have` takes the omitted bitfield as Have None
// before it sets the piece: a first HAVE of piece 7 leaves the peer with
// piece 7 and nothing else.
#[tokio::test]
async fn a_peer_whose_first_message_is_a_have_has_that_piece_alone() {
    let h = Harness::eight_pieces("first_message_have").await;
    let (peer, _rx) = h.new_peer("127.0.0.1:6881");
    (&peer).on_received_message(Message::Have(7)).await.unwrap();
    assert_eq!(h.pieces_of(&peer), [0b0000_0001]);
    assert_eq!(h.peers.availability.counts(), [0, 0, 0, 0, 0, 0, 0, 1]);
    assert!(futures::FutureExt::now_or_never(peer.wait_for_bitfield()).is_some());
}

// libtorrent `peer_connection::incoming_bitfield` once the pieces are
// known (`m_bitfield_received`): the pieces the peer was taken to have
// leave the availability (`torrent::peer_lost`) and the bitfield replaces
// them.
#[tokio::test]
async fn a_bitfield_after_a_first_have_replaces_the_pieces_it_made_known() {
    let h = Harness::eight_pieces("bitfield_after_first_have").await;
    let (peer, _rx) = h.new_peer("127.0.0.1:6881");
    (&peer).on_received_message(Message::Have(7)).await.unwrap();

    (&peer)
        .on_received_message(Message::Bitfield(ByteBuf(&[0b0001_0000])))
        .await
        .unwrap();
    assert_eq!(h.pieces_of(&peer), [0b0001_0000]);
    assert_eq!(h.peers.availability.counts(), [0, 0, 0, 1, 0, 0, 0, 0]);
}

/// What we open the connection to `peer` with, as it goes on the wire.
fn opening(peer: &PeerHandler) -> Option<Vec<u8>> {
    let peer = &peer;
    peer.should_send_bitfield().then(|| {
        let mut buf = vec![0u8; 64];
        let len = peer.serialize_bitfield_message_to_buf(&mut buf).unwrap();
        buf.truncate(len);
        buf
    })
}

fn wire(msg: Message<'_>) -> Vec<u8> {
    let mut buf = vec![0u8; 64];
    let len = msg.serialize(&mut buf, &Default::default).unwrap();
    buf.truncate(len);
    buf
}

#[tokio::test]
async fn have_all_and_have_none_are_bitfields_of_every_piece_and_of_none() {
    let h = Harness::new("have_all_and_have_none").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    h.speak_fast(&peer);
    let peer = &peer;
    let bitfield = || {
        h.peers
            .with_live(peer.addr, |live| (live.bitfield.clone(), live.seeder))
            .unwrap()
    };

    peer.on_received_message(Message::HaveNone).await.unwrap();
    let (none, seeder) = bitfield();
    assert_eq!(none.as_raw_slice(), [0]);
    assert!(!seeder);
    assert_eq!(h.peers.availability.counts(), [0, 0]);

    // The padding bits stay clear.
    peer.on_received_message(Message::HaveAll).await.unwrap();
    let (all, seeder) = bitfield();
    assert_eq!(all.as_raw_slice(), [0b1100_0000]);
    assert!(seeder);
    assert_eq!(h.peers.availability.counts(), [1, 1]);
}

fn dont_have(index: u32) -> Message<'static> {
    Message::Extended(ExtendedMessage::DontHave(index))
}

// libtorrent `peer_connection::incoming_dont_have` into `torrent::peer_lost`:
// a DONT_HAVE takes the piece out of the peer's pieces and the
// availability; one for a piece the peer lacks, or past the last piece,
// changes nothing.
#[tokio::test]
async fn a_dont_have_after_a_have_leaves_availability_as_before_the_have() {
    let h = Harness::new("dont_have_after_have").await;
    let (peer, _rx) = h.new_peer("127.0.0.1:6881");
    let peer = &peer;
    let before = h.peers.availability.counts();
    peer.on_received_message(Message::Have(1)).await.unwrap();
    assert_eq!(h.peers.availability.counts(), [0, 1]);
    peer.on_received_message(dont_have(1)).await.unwrap();
    assert_eq!(h.peers.availability.counts(), before);
    for index in [1, 0, 2, u32::MAX] {
        peer.on_received_message(dont_have(index)).await.unwrap();
    }
    assert_eq!(h.peers.availability.counts(), before);
    let pieces = h.peers.with_live(peer.addr, |live| live.bitfield.clone());
    assert!(pieces.unwrap().not_any());
}

// libtorrent `peer_connection::incoming_dont_have`: a seed that loses a
// piece is a seed no longer (`torrent::set_seed`), by the same flag a HAVE
// sets.
#[tokio::test]
async fn a_seeder_that_sends_dont_have_is_no_seeder() {
    let h = Harness::new("dont_have_seeder").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    let seeder = || h.peers.with_live(peer.addr, |live| live.seeder).unwrap();
    assert!(seeder());
    peer.on_received_message(dont_have(0)).await.unwrap();
    assert!(!seeder());
    assert_eq!(h.peers.availability.counts(), [0, 1]);
    peer.on_received_message(Message::Have(0)).await.unwrap();
    assert!(seeder());
}

// libtorrent `bt_peer_connection::write_extensions` names lt_donthave in
// `m`.
#[tokio::test]
async fn the_extended_handshake_offers_lt_donthave() {
    let h = Harness::new("offers_lt_donthave").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    assert_eq!(
        h.advertised(&peer).lt_donthave,
        Some(extended::MY_EXTENDED_LT_DONTHAVE)
    );
}

#[tokio::test]
async fn a_fast_connection_opens_with_have_all_have_none_or_a_bitfield() {
    let seeding = Harness::seeding("opens_seeding").await;
    let half = Harness::build("opens_half", 0x80, choker::DEFAULT_UNCHOKE_SLOTS_LIMIT).await;
    let empty = Harness::new("opens_empty").await;
    for (h, fast_opening, plain_opening) in [
        (
            &seeding,
            wire(Message::HaveAll),
            Some(wire(Message::Bitfield(ByteBuf(&[0xc0])))),
        ),
        (
            &half,
            wire(Message::Bitfield(ByteBuf(&[0x80]))),
            Some(wire(Message::Bitfield(ByteBuf(&[0x80])))),
        ),
        (&empty, wire(Message::HaveNone), None),
    ] {
        let (plain, _plain_rx) = h.peer("127.0.0.1:6881");
        let (fast, _fast_rx) = h.peer("127.0.0.2:6881");
        h.speak_fast(&fast);
        assert_eq!(opening(&fast), Some(fast_opening));
        assert_eq!(opening(&plain), plain_opening);
    }
}

// libtorrent `torrent::update_piece_priorities` has every peer update its
// interest (`peer_connection::update_interest`), and
// `peer_connection::disconnect_if_redundant` lets a seed go once we only
// upload. Neither waits on a timer, and the peer hears NotInterested
// before the connection closes.
#[tokio::test]
async fn a_torrent_that_stops_wanting_says_so_at_once_and_lets_a_seed_go() {
    let h = Harness::build("stops_wanting", 0x80, choker::DEFAULT_UNCHOKE_SLOTS_LIMIT).await;
    let (peer, mut rx) = h.new_peer("127.0.0.1:6881");
    peer.on_bitfield(&bitfield_of(&[1])).unwrap();
    tokio::time::pause();
    let start = tokio::time::Instant::now();
    let mut requester: Task<'_> = Box::pin(peer.request_chunks());

    // We want piece 1, which the peer has; it chokes us, so we wait.
    assert!(matches!(
        next_queued(&mut requester, &mut rx).await,
        WriterRequest::Message(Message::Interested)
    ));
    assert!(waiting(&mut requester, &mut rx));

    h.set_piece_priorities([(h.piece(1), PiecePriority::Skip)])
        .unwrap();
    assert!(matches!(
        next_queued(&mut requester, &mut rx).await,
        WriterRequest::Message(Message::NotInterested)
    ));
    assert!(waiting(&mut requester, &mut rx));

    // The peer becomes a seed: the connection is to close once what was
    // queued before has gone out, and the requester waits for that.
    peer.on_have(0).unwrap();
    assert!(matches!(
        next_queued(&mut requester, &mut rx).await,
        WriterRequest::Disconnect(Ok(()))
    ));
    assert!(waiting(&mut requester, &mut rx));
    assert_eq!(tokio::time::Instant::now(), start);
}

// A seed let go because the torrent wanted nothing of it is no failed
// peer: once a priority change makes a piece wanted again it is dialled at
// once, not a reconnect time after the close. A seed whose connection
// failed while nothing was wanted still waits as its failure has it wait.
#[tokio::test]
async fn a_seed_let_go_while_nothing_was_wanted_is_dialled_once_a_piece_is_wanted() {
    let h = Harness::new("redial_let_go").await;
    h.set_piece_priorities([
        (h.piece(0), PiecePriority::Skip),
        (h.piece(1), PiecePriority::Skip),
    ])
    .unwrap();
    tokio::time::pause();
    let listens = |peer: &PeerHandler| {
        let mut hs = ExtendedHandshake::new();
        hs.p = Some(u32::from(peer.addr.port()));
        peer.on_extended_handshake(&hs).unwrap();
    };

    let (seed, mut seed_rx) = h.peer("127.0.0.1:6881");
    listens(&seed);
    let mut requester: Task<'_> = Box::pin(seed.request_chunks());
    loop {
        match next_queued(&mut requester, &mut seed_rx).await {
            WriterRequest::Disconnect(Ok(())) => break,
            WriterRequest::Message(_) => {}
            other => panic!("unexpected writer request {other:?}"),
        }
    }
    drop(requester);
    seed.on_peer_died(Some(crate::Error::Disconnect)).unwrap();

    let (broken, _broken_rx) = h.peer("127.0.0.2:6881");
    listens(&broken);
    broken
        .on_peer_died(Some(crate::Error::TestDisconnect))
        .unwrap();

    // Both had every piece, so both are let go while we only upload.
    for addr in ["127.0.0.1:6881", "127.0.0.2:6881"] {
        assert_eq!(
            h.peers.with_peer(sock(addr), |p| p.get_state().name()),
            Some("not needed")
        );
    }
    let now = tokio::time::Instant::now();
    assert!(h.peers.connect_next(now).is_none());
    h.set_piece_priorities([(h.piece(1), PiecePriority::Normal)])
        .unwrap();
    assert_eq!(
        h.peers.connect_next(now).map(|(addr, ..)| addr),
        Some(sock("127.0.0.1:6881"))
    );
    assert!(h.peers.connect_next(now).is_none());
    assert_eq!(
        h.peers
            .with_peer(sock("127.0.0.2:6881"), |p| p.reconnect_at())
            .unwrap(),
        Some(now + peer::MIN_RECONNECT_TIME * 2)
    );
}

// libtorrent `peer_connection::send_allowed_set` sends nothing to a peer
// that only uploads (`peer_connection::upload_only`).
#[tokio::test]
async fn a_peer_that_only_uploads_is_sent_no_allowed_fast_set() {
    let h = Harness::seeding("upload_only_no_allowed_fast").await;
    let (peer, mut rx) = h.new_peer("127.0.0.1:6881");
    h.speak_fast(&peer);
    let peer = &peer;
    peer.on_received_message(Message::HaveNone).await.unwrap();
    peer.on_received_message(Message::Extended(ExtendedMessage::UploadOnly(true)))
        .await
        .unwrap();
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    assert!(rx.is_empty());
}

// libtorrent `peer_connection::disconnect_if_redundant`: once we are
// finished, a peer that only uploads goes, whether its handshake or its
// upload_only message said so or it has every piece; a peer that may
// download from us stays.
#[tokio::test]
async fn a_peer_that_only_uploads_goes_once_we_are_finished() {
    let h = Harness::build(
        "upload_only_finished",
        0x80,
        choker::DEFAULT_UNCHOKE_SLOTS_LIMIT,
    )
    .await;
    let (said, mut said_rx) = h.new_peer("127.0.0.1:6881");
    let (shook, mut shook_rx) = h.new_peer("127.0.0.2:6881");
    let (leech, mut leech_rx) = h.new_peer("127.0.0.3:6881");
    for peer in [&said, &shook, &leech] {
        peer.on_received_message(Message::Bitfield(ByteBuf(&bitfield_of(&[1]))))
            .await
            .unwrap();
    }
    let (seed, mut seed_rx) = h.peer("127.0.0.4:6881");
    (&said)
        .on_received_message(Message::Extended(ExtendedMessage::UploadOnly(true)))
        .await
        .unwrap();
    // It names no extension, so it is not told we only upload.
    let mut hs = ExtendedHandshake::new();
    hs.m = Default::default();
    hs.upload_only = Some(1);
    (&shook).on_extended_handshake(&hs).unwrap();

    tokio::time::pause();
    let mut said_task: Task<'_> = Box::pin(said.request_chunks());
    let mut shook_task: Task<'_> = Box::pin(shook.request_chunks());
    let mut leech_task: Task<'_> = Box::pin(leech.request_chunks());
    let mut seed_task: Task<'_> = Box::pin(seed.request_chunks());
    let mut peers = [
        (&mut said_task, &mut said_rx, true),
        (&mut shook_task, &mut shook_rx, true),
        (&mut leech_task, &mut leech_rx, false),
        (&mut seed_task, &mut seed_rx, true),
    ];

    // Each has piece 1, which we want: they stay.
    for (task, rx, _) in &mut peers {
        assert!(matches!(
            next_queued(task, rx).await,
            WriterRequest::Message(Message::Interested)
        ));
        assert!(waiting(task, rx));
    }

    h.set_piece_priorities([(h.piece(1), PiecePriority::Skip)])
        .unwrap();
    for (task, rx, goes) in &mut peers {
        assert!(matches!(
            next_queued(task, rx).await,
            WriterRequest::Message(Message::NotInterested)
        ));
        if *goes {
            assert!(matches!(
                next_queued(task, rx).await,
                WriterRequest::Disconnect(Ok(()))
            ));
        }
        assert!(waiting(task, rx));
    }
}

// libtorrent `peer_connection::disconnect_if_redundant`: a peer that only
// uploads goes once it has nothing we want, though we are not finished.
#[tokio::test]
async fn a_peer_that_only_uploads_goes_once_it_has_nothing_we_want() {
    let h = Harness::new("upload_only_uninteresting").await;
    let (peer, mut rx) = h.new_peer("127.0.0.1:6881");
    (&peer)
        .on_received_message(Message::Bitfield(ByteBuf(&bitfield_of(&[1]))))
        .await
        .unwrap();
    (&peer)
        .on_received_message(Message::Extended(ExtendedMessage::UploadOnly(true)))
        .await
        .unwrap();
    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(peer.request_chunks());
    assert!(matches!(
        next_queued(&mut requester, &mut rx).await,
        WriterRequest::Message(Message::Interested)
    ));
    assert!(waiting(&mut requester, &mut rx));

    // We still want piece 0, which it lacks.
    h.set_piece_priorities([(h.piece(1), PiecePriority::Skip)])
        .unwrap();
    assert!(matches!(
        next_queued(&mut requester, &mut rx).await,
        WriterRequest::Message(Message::NotInterested)
    ));
    assert!(matches!(
        next_queued(&mut requester, &mut rx).await,
        WriterRequest::Disconnect(Ok(()))
    ));
    assert!(!h.is_finished());
}

impl Harness {
    /// Whether we last told `peer` we are interested.
    fn interesting(&self, peer: &PeerHandler) -> bool {
        self.peers
            .with_live(peer.addr, |live| live.is_interesting())
            .unwrap()
    }
}

// libtorrent `peer_connection::incoming_have_none` and `do_update_interest`:
// we are interested only in a peer that has a piece we want and lack, so
// neither a peer with no piece nor one with only pieces we skip hears
// Interested.
#[tokio::test]
async fn a_peer_with_nothing_we_want_is_never_told_interested() {
    let h = Harness::new("nothing_we_want").await;
    h.set_piece_priorities([(h.piece(1), PiecePriority::Skip)])
        .unwrap();
    let (none, mut none_rx) = h.new_peer("127.0.0.1:6881");
    (&none)
        .on_received_message(Message::HaveNone)
        .await
        .unwrap();
    let (skipped, mut skipped_rx) = h.new_peer("127.0.0.2:6881");
    skipped.on_bitfield(&bitfield_of(&[1])).unwrap();
    tokio::time::pause();
    for (peer, rx) in [(&none, &mut none_rx), (&skipped, &mut skipped_rx)] {
        peer.on_i_am_unchoked();
        let mut requester: Task<'_> = Box::pin(peer.request_chunks());
        assert!(waiting(&mut requester, rx));
        assert!(!h.interesting(peer));
    }
}

// libtorrent `peer_connection::incoming_have`: a HAVE of a piece we want
// and lack makes the peer interesting at once.
#[tokio::test]
async fn a_have_of_a_wanted_piece_makes_the_peer_interesting() {
    let h = Harness::new("have_makes_interesting").await;
    let (peer, mut rx) = h.new_peer("127.0.0.1:6881");
    (&peer)
        .on_received_message(Message::HaveNone)
        .await
        .unwrap();
    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(peer.request_chunks());
    assert!(waiting(&mut requester, &mut rx));

    (&peer).on_received_message(Message::Have(1)).await.unwrap();
    assert!(matches!(
        next_queued(&mut requester, &mut rx).await,
        WriterRequest::Message(Message::Interested)
    ));
    assert!(h.interesting(&peer));
}

// libtorrent `torrent::piece_passed`: once we have a piece, each peer we
// were interested in that has it updates its interest, and one with
// nothing else we want hears NotInterested.
#[tokio::test]
async fn a_peer_whose_pieces_we_come_to_have_stops_being_interesting() {
    let h = Harness::new("pieces_we_come_to_have").await;
    let (peer, mut rx) = h.new_peer("127.0.0.1:6881");
    peer.on_bitfield(&bitfield_of(&[0])).unwrap();
    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(peer.request_chunks());
    peer.on_i_am_unchoked();
    assert_eq!(
        next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await,
        chunks(0, 0..CHUNKS_PER_PIECE)
    );
    assert!(h.interesting(&peer));

    for chunk in 0..CHUNKS_PER_PIECE {
        h.deliver(&peer, (0, chunk * CHUNK_SIZE)).await;
    }
    assert!(
        h.lock_read("test")
            .get_chunks()
            .unwrap()
            .is_piece_have(h.piece(0))
    );
    assert!(matches!(
        next_message(&mut requester, &mut rx).await,
        Message::NotInterested
    ));
    assert!(!h.interesting(&peer));
    assert!(waiting(&mut requester, &mut rx));
}

// libtorrent `peer_connection::second_tick`: a connection neither side is
// interested in ends after the inactivity timeout when connections run
// short, while the torrent still downloads.
#[tokio::test]
async fn a_downloading_torrent_parts_from_a_peer_with_nothing_it_wants() {
    // Five connections in all: the torrent's connections run short.
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        peer_limit: Some(5),
        ..Default::default()
    };
    let h = Harness::build_added(
        "no_interest_downloading",
        PIECES,
        0x00,
        isolated_session_opts(false),
        add,
    )
    .await;
    tokio::time::pause();
    let (peer, mut rx) = h.new_peer("127.0.0.1:6881");
    (&peer)
        .on_received_message(Message::HaveNone)
        .await
        .unwrap();
    let start = tokio::time::Instant::now();

    peer.task_peer_chunk_requester().await.unwrap();
    assert!(tokio::time::Instant::now().duration_since(start) > peer::INACTIVITY_TIMEOUT);
    assert!(rx.try_recv().is_err());
}

// libtorrent `torrent::update_piece_priorities`: a piece wanted again makes
// the peers that have it interesting at once, and they are asked for it.
#[tokio::test]
async fn a_finished_torrent_asks_for_a_piece_the_moment_it_is_wanted_again() {
    let h = Harness::build("wanted_again", 0x80, choker::DEFAULT_UNCHOKE_SLOTS_LIMIT).await;
    h.set_piece_priorities([(h.piece(1), PiecePriority::Skip)])
        .unwrap();
    let (peer, mut rx) = h.new_peer("127.0.0.1:6881");
    peer.on_bitfield(&bitfield_of(&[1])).unwrap();
    peer.on_i_am_unchoked();
    tokio::time::pause();
    let start = tokio::time::Instant::now();
    let mut requester: Task<'_> = Box::pin(peer.request_chunks());
    assert!(waiting(&mut requester, &mut rx));

    h.set_piece_priorities([(h.piece(1), PiecePriority::Normal)])
        .unwrap();
    assert!(matches!(
        next_queued(&mut requester, &mut rx).await,
        WriterRequest::Message(Message::Interested)
    ));
    assert_eq!(
        next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await,
        chunks(1, 0..CHUNKS_PER_PIECE)
    );
    assert_eq!(tokio::time::Instant::now(), start);
}

// libtorrent `bt_peer_connection::write_extensions` sends `p`, our listen
// port, on a connection we made; `on_extended_handshake` takes `p` from a
// peer, at the address it connected from (`peer_list::update_peer_port`),
// and never the peer's word on its own address.
#[tokio::test]
async fn the_extended_handshake_carries_listen_ports_and_no_claimed_address() {
    let opts = SessionOptions {
        listen: Some(crate::ListenerOptions {
            mode: crate::ListenerMode::TcpOnly,
            listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            announce_port: Some(51413),
            ..Default::default()
        }),
        ..isolated_session_opts(false)
    };
    let h = Harness::build_on(
        "extended_handshake_ports",
        0x00,
        opts,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let (incoming, _incoming_rx) = h.peer("127.0.0.1:50000");
    let (mut outgoing, _outgoing_rx) = h.new_peer("127.0.0.2:6881");
    outgoing.incoming = false;

    let ours = |peer: &PeerHandler| {
        let mut hs = ExtendedHandshake::new();
        peer.update_my_extended_handshake(&mut hs).unwrap();
        hs.p
    };
    // The listen port, not the announce port set for trackers and the DHT.
    let port = h._session.listen_addr().map(|addr| u32::from(addr.port()));
    assert_eq!(ours(&incoming), None);
    assert_ne!(port, Some(51413));
    assert_eq!(ours(&outgoing), port);

    let theirs = ExtendedHandshake {
        p: Some(7000),
        ipv4: Some(std::net::Ipv4Addr::new(203, 0, 113, 9).into()),
        ipv6: Some(std::net::Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 9).into()),
        ..Default::default()
    };
    (&incoming).on_extended_handshake(&theirs).unwrap();
    let listen: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    assert_eq!(
        h.peers.with_peer(incoming.addr, |p| p.outgoing_address),
        Some(Some(listen))
    );
    assert_eq!(
        h.peers.pex_listed().keys().copied().collect::<Vec<_>>(),
        [listen]
    );
}

// libtorrent `bt_peer_connection::write_extensions` takes `p` from
// `session_impl::listen_port(ssl, local_endpoint().address())`
// (bt_peer_connection.cpp:2459-2463, session_impl.cpp:5859-5873): the
// external port of the listen socket of the connection's family. A port PCP
// got over IPv6 goes to IPv6 peers only, and one NAT-PMP got over IPv4 to
// IPv4 peers only.
#[tokio::test]
async fn the_extended_handshake_carries_the_port_mapped_for_the_peers_family() {
    use crate::upnp::{PortMapped, PortmapProtocol, PortmapTransport};
    let opts = SessionOptions {
        listen: Some(crate::ListenerOptions {
            mode: crate::ListenerMode::TcpOnly,
            listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            announce_port: Some(51413),
            ..Default::default()
        }),
        ..isolated_session_opts(false)
    };
    let h = Harness::build_on(
        "extended_handshake_family_ports",
        0x00,
        opts,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let outgoing = |addr: &str| {
        let (mut peer, rx) = h.new_peer(addr);
        peer.incoming = false;
        (peer, rx)
    };
    let (v4, _v4_rx) = outgoing("127.0.0.2:6881");
    let (mapped_v4, _mapped_v4_rx) = outgoing("[::ffff:127.0.0.3]:6881");
    let (v6, _v6_rx) = outgoing("[2001:db8::2]:6881");
    let ours = |peer: &PeerHandler| {
        let mut hs = ExtendedHandshake::new();
        peer.update_my_extended_handshake(&mut hs).unwrap();
        hs.p
    };
    let listen = h._session.listen_addr().map(|addr| u32::from(addr.port()));
    let mapped = |transport, external_port| PortMapped {
        transport,
        protocol: PortmapProtocol::Tcp,
        local_port: h._session.listen_addr().unwrap().port(),
        external_port: Some(external_port),
        external_ip: None,
        error: None,
    };

    h._session
        .port_mapped(&mapped(PortmapTransport::PcpIpv6, 2002));
    assert_eq!(
        (ours(&v4), ours(&mapped_v4), ours(&v6)),
        (listen, listen, Some(2002))
    );
    h._session
        .port_mapped(&mapped(PortmapTransport::NatPmp, 2000));
    assert_eq!(
        (ours(&v4), ours(&mapped_v4), ours(&v6)),
        (Some(2000), Some(2000), Some(2002))
    );
}

// libtorrent `bt_peer_connection::on_extended_handshake`: a peer's `yourip`
// is its vote on our external address (`session_impl::set_external_address`),
// one address to a family.
#[tokio::test(start_paused = true)]
async fn a_peers_yourip_is_its_vote_on_the_sessions_external_address() {
    let h = Harness::new("yourip_votes").await;
    let ip = |s: &str| s.parse::<std::net::IpAddr>().unwrap();
    let says = |s: &str| ExtendedHandshake {
        yourip: Some(ip(s).into()),
        ..Default::default()
    };
    let (first, _first_rx) = h.peer("198.51.100.1:6881");
    let (second, _second_rx) = h.peer("198.51.100.2:6881");
    let (third, _third_rx) = h.peer("198.51.100.3:6881");
    let (v6, _v6_rx) = h.peer("[2001:db8::1]:6881");
    // A loopback address, or one of the other family, is no vote.
    (&first).on_extended_handshake(&says("127.0.0.1")).unwrap();
    (&first)
        .on_extended_handshake(&says("2001:db8::5"))
        .unwrap();
    assert_eq!(h._session.external_ips.get(), [] as [std::net::IpAddr; 0]);
    (&first)
        .on_extended_handshake(&says("203.0.113.5"))
        .unwrap();
    (&second)
        .on_extended_handshake(&says("203.0.113.9"))
        .unwrap();
    (&third)
        .on_extended_handshake(&says("203.0.113.5"))
        .unwrap();
    (&v6).on_extended_handshake(&says("2001:db8::5")).unwrap();
    assert_eq!(
        h._session.external_ips.get(),
        [ip("203.0.113.5"), ip("2001:db8::5")]
    );
}

// libtorrent `bt_peer_connection::write_extensions` sends `complete_ago`,
// the seconds since the torrent last saw a complete copy, -1 for never; a
// seed sees its own (`torrent::time_since_complete`).
#[tokio::test(start_paused = true)]
async fn the_extended_handshake_says_how_long_ago_we_saw_a_complete_copy() {
    let ours = |peer: &PeerHandler| {
        let mut hs = ExtendedHandshake::new();
        peer.update_my_extended_handshake(&mut hs).unwrap();
        hs.complete_ago
    };
    let h = Harness::new("complete_ago_never").await;
    let (peer, _rx) = h.new_peer("127.0.0.1:6881");
    assert_eq!(h.complete_ago(), None);
    assert_eq!(ours(&peer), Some(-1));

    let seed = Harness::seeding("complete_ago_seed").await;
    let (peer, _rx) = seed.new_peer("127.0.0.1:6881");
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(ours(&peer), Some(0));
}

// libtorrent `bt_peer_connection::on_extended_handshake` takes a peer's
// `complete_ago` (`peer_connection::set_last_seen_complete`), a negative
// one none, and `torrent::second_tick` keeps the latest of ours and the
// peers' as the swarm's (`m_swarm_last_seen_complete`), for status only:
// what we send is our own sighting (`torrent::time_since_complete`).
#[tokio::test(start_paused = true)]
async fn a_peers_complete_ago_is_the_swarms_status_never_what_we_send() {
    let h = Harness::new("complete_ago_theirs").await;
    let (first, _first_rx) = h.new_peer("127.0.0.1:6881");
    let (second, _second_rx) = h.new_peer("127.0.0.2:6881");
    let says = |ago| ExtendedHandshake {
        complete_ago: Some(ago),
        ..Default::default()
    };
    let ours = || {
        let mut hs = ExtendedHandshake::new();
        (&first).update_my_extended_handshake(&mut hs).unwrap();
        hs.complete_ago
    };
    (&first).on_extended_handshake(&says(-1)).unwrap();
    assert_eq!(h.swarm_complete_ago(), None);
    (&first).on_extended_handshake(&says(30)).unwrap();
    (&second).on_extended_handshake(&says(100)).unwrap();
    assert_eq!(h.swarm_complete_ago(), Some(Duration::from_secs(30)));
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(h.swarm_complete_ago(), Some(Duration::from_secs(40)));
    assert_eq!(h.complete_ago(), None);
    assert_eq!(ours(), Some(-1));

    // A seed's own copy is the swarm's latest too.
    let seed = Harness::seeding("complete_ago_theirs_seed").await;
    let (peer, _rx) = seed.new_peer("127.0.0.1:6881");
    (&peer).on_extended_handshake(&says(30)).unwrap();
    assert_eq!(seed.swarm_complete_ago(), Some(Duration::ZERO));
}

// libtorrent `peer_connection::on_receive_data` and `incoming_have`: a seed
// that has sent us data is a complete copy seen each time it sends; one
// that has sent none is only its word.
#[tokio::test(start_paused = true)]
async fn a_seed_that_sent_us_data_is_a_complete_copy_seen() {
    let h = Harness::new("complete_ago_seed_peer").await;
    let (seed, mut rx) = h.peer("127.0.0.1:6881");
    (&seed)
        .on_received_message(Message::KeepAlive)
        .await
        .unwrap();
    assert_eq!(h.complete_ago(), None);

    let mut requester: Task<'_> = Box::pin(seed.request_chunks());
    seed.on_i_am_unchoked();
    let (index, begin) = *next_requests(&mut requester, &mut rx, 4)
        .await
        .first()
        .unwrap();
    let start = (index * PIECE_LENGTH + begin) as usize;
    let block = &h.content[start..start + CHUNK_SIZE as usize];
    (&seed)
        .on_received_message(Message::Piece(Piece::from_data(index, begin, block)))
        .await
        .unwrap();
    assert_eq!(h.complete_ago(), Some(Duration::ZERO));
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(h.complete_ago(), Some(Duration::from_secs(10)));
    (&seed)
        .on_received_message(Message::KeepAlive)
        .await
        .unwrap();
    assert_eq!(h.complete_ago(), Some(Duration::ZERO));
}

// libtorrent `piece_picker::inc_refcount` and `dec_refcount`: a HAVE and a
// peer leaving move the piece in the pick order before the next pick.
#[tokio::test]
async fn a_have_and_a_peer_leaving_move_a_piece_in_the_pick_order() {
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        ..Default::default()
    };
    // Eight pieces, the first four had: the rarest goes first.
    let h = Harness::build_added(
        "have_and_leaving_move_pieces",
        8,
        0xf0,
        isolated_session_opts(false),
        add,
    )
    .await;
    let (a, _a_rx) = h.peer("127.0.0.1:6881");
    let (g, _g_rx) = h.peer("127.0.0.2:6881");
    let (k, _k_rx) = h.peer("127.0.0.3:6881");
    let _b = h.peer_with("127.0.0.4:6881", &[5, 6, 7]).await;
    let _c = h.peer_with("127.0.0.5:6881", &[6, 7]).await;
    let (d, _d_rx) = h.peer_with("127.0.0.6:6881", &[7]).await;
    let (j, _j_rx) = h.peer_with("127.0.0.7:6881", &[7]).await;
    for peer in [&a, &g, &k] {
        peer.on_i_am_unchoked();
    }
    let pick = |peer: &PeerHandler| peer.acquire_next_piece().unwrap().piece().map(|p| p.get());

    // Three peers have piece 4, four 5, five 6, seven 7.
    assert_eq!(pick(&a), Some(4));

    // Two HAVEs of 5: six peers have it, one more than 6.
    let mut later = Vec::new();
    for addr in ["127.0.0.8:6881", "127.0.0.9:6881"] {
        let (peer, rx) = h.peer_with(addr, &[]).await;
        (&peer).on_received_message(Message::Have(5)).await.unwrap();
        later.push((peer, rx));
    }
    assert_eq!(pick(&g), Some(6));

    // Two of the seven with 7 go: five have it, one fewer than 5.
    d.on_peer_died(None).unwrap();
    j.on_peer_died(None).unwrap();
    assert_eq!(pick(&k), Some(7));
}
