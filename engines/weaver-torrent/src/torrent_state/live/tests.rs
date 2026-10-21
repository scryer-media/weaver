// SPDX-License-Identifier: GPL-3.0-or-later

//! A live torrent and one peer driven by hand: the test plays the remote
//! peer, calling the handler as the connection would, and reads what the
//! handler queues for the wire.

use std::{collections::BTreeSet, future::Future, pin::Pin, sync::atomic::AtomicU32};

use tempfile::TempDir;

use super::*;
use crate::{
    AddTorrent, AddTorrentOptions, Session, SessionOptions,
    core::constants::CHUNK_SIZE,
    peer_protocol::extended,
    storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage},
    tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
    type_aliases::BF,
};

const CHUNKS_PER_PIECE: u32 = 4;
const PIECE_LENGTH: u32 = CHUNK_SIZE * CHUNKS_PER_PIECE;
const PIECES: u32 = 2;

struct Harness {
    live: Arc<TorrentStateLive>,
    content: Vec<u8>,
    _session: Arc<Session>,
    _torrent: Option<TestTorrent>,
    _out: TempDir,
}

impl Harness {
    /// A live torrent of two four-chunk pieces, none of them had, and no peers.
    async fn new(name: &str) -> Self {
        Self::build(name, 0x00, choker::DEFAULT_UNCHOKE_SLOTS_LIMIT).await
    }

    /// A live torrent of two four-chunk pieces, both had, and no peers.
    async fn seeding(name: &str) -> Self {
        Self::seeding_with_slots(name, choker::DEFAULT_UNCHOKE_SLOTS_LIMIT).await
    }

    /// As [`Harness::seeding`], uploading to `slots` peers at once.
    async fn seeding_with_slots(name: &str, slots: usize) -> Self {
        Self::build(name, 0xc0, slots).await
    }

    // `have` is the bitfield of the pieces had, `slots` the upload slots.
    async fn build(name: &str, have: u8, slots: usize) -> Self {
        let opts = SessionOptions {
            unchoke_slots_limit: slots,
            ..isolated_session_opts(false)
        };
        Self::build_on(
            name,
            have,
            opts,
            MemoryStorageFactory::default().clone_box(),
        )
        .await
    }

    /// As [`Harness::build`], in a session of `opts`, on the storage
    /// `storage` makes.
    async fn build_on(
        name: &str,
        have: u8,
        opts: SessionOptions,
        storage: BoxStorageFactory,
    ) -> Self {
        let add = AddTorrentOptions {
            storage_factory: Some(storage),
            ..Default::default()
        };
        Self::build_added(name, PIECES, have, opts, add).await
    }

    /// A live torrent of `pieces` four-chunk pieces, `have` the bitfield
    /// of those had, added with `add`.
    async fn build_added(
        name: &str,
        pieces: u32,
        have: u8,
        opts: SessionOptions,
        add: AddTorrentOptions,
    ) -> Self {
        let out = TempDir::with_prefix(name).unwrap();
        let session = Session::new_with_opts(out.path().into(), opts)
            .await
            .unwrap();
        Self::added_to(session, out, name, pieces, have, add).await
    }

    /// Another live torrent of the harness's session, of the harness's
    /// pieces, `have` the bitfield of those had.
    async fn sibling(&self, name: &str, have: u8) -> Self {
        let add = AddTorrentOptions {
            storage_factory: Some(MemoryStorageFactory::default().clone_box()),
            ..Default::default()
        };
        let out = TempDir::with_prefix(name).unwrap();
        Self::added_to(self._session.clone(), out, name, PIECES, have, add).await
    }

    /// A live torrent of `pieces` four-chunk pieces, `have` the bitfield of
    /// those had, added to `session` with `add`.
    async fn added_to(
        session: Arc<Session>,
        out: TempDir,
        name: &str,
        pieces: u32,
        have: u8,
        add: AddTorrentOptions,
    ) -> Self {
        let torrent = TestTorrent::new(name, (pieces * PIECE_LENGTH) as usize, PIECE_LENGTH)
            .await
            .unwrap();
        let have = BF::from_boxed_slice(vec![have].into_boxed_slice());
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent.torrent.clone()),
                Some(AddTorrentOptions {
                    initial_have: Some(have),
                    ..add
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();
        Self {
            live: handle.live().unwrap(),
            content: torrent.content.clone(),
            _session: session,
            _torrent: Some(torrent),
            _out: out,
        }
    }

    /// A live peer at `addr` with every piece, and the queue of what we send it.
    fn peer(&self, addr: &str) -> (PeerHandler, PeerRx) {
        self.peer_speaking(addr, false)
    }

    /// A live peer at `addr` with every piece, that speaks v2 when
    /// `speaks_v2` holds, and the queue of what we send it.
    fn peer_speaking(&self, addr: &str, speaks_v2: bool) -> (PeerHandler, PeerRx) {
        let (handler, rx) = self.new_peer(addr);
        handler.speaks_v2.store(speaks_v2, Ordering::Relaxed);
        handler
            .first_message_received
            .store(true, Ordering::Relaxed);
        let mut every_piece = peer::empty_bitfield(self.lengths.total_pieces());
        every_piece[..self.lengths.total_pieces() as usize].fill(true);
        handler.on_bitfield(every_piece.as_raw_slice()).unwrap();
        (handler, rx)
    }

    /// A peer at `addr` that just went live and has sent nothing yet, and
    /// the queue of what we send it.
    fn new_peer(&self, addr: &str) -> (PeerHandler, PeerRx) {
        let addr: SocketAddr = addr.parse().unwrap();
        let (tx, rx) = unbounded_channel();
        let counters = self
            .live
            .peers
            .add_incoming(addr, Id20::default(), tx.clone(), ConnectionKind::Tcp, 6881)
            .unwrap();
        let handler = PeerHandler {
            addr,
            incoming: true,
            on_bitfield_notify: Default::default(),
            speaks_v2: AtomicBool::new(false),
            sent_dht_port: AtomicBool::new(false),
            flow_control: Mutex::new(PeerFlowControl::new(self.live.max_request_window())),
            state: self.live.clone(),
            tx,
            counters,
            first_message_received: AtomicBool::new(false),
            cancel_token: self.live.cancellation_token.child_token(),
            client_name_and_version: String::new(),
        };
        (handler, rx)
    }

    fn piece(&self, index: u32) -> ValidPieceIndex {
        self.lengths.validate_piece_index(index).unwrap()
    }

    /// Chunk `chunk` of piece `index`.
    fn chunk(&self, index: u32, chunk: u32) -> ChunkInfo {
        self.lengths
            .chunk_info_from_received_data(self.piece(index), chunk * CHUNK_SIZE, CHUNK_SIZE)
            .unwrap()
    }

    fn missing_chunks(&self, index: u32) -> Vec<u32> {
        self.live
            .lock_read("test")
            .get_pieces()
            .unwrap()
            .missing_chunks(self.piece(index))
            .map(|c| c.chunk_index)
            .collect()
    }

    /// The pieces no peer owns and any peer may take.
    fn queued_pieces(&self) -> Vec<u32> {
        let g = self.live.lock_read("test");
        let pieces = g.get_pieces().unwrap();
        let queued: Vec<u32> = pieces
            .chunks()
            .iter_queued_pieces()
            .map(|p| p.get())
            .collect();
        for p in &queued {
            assert!(pieces.get_inflight(self.piece(*p)).is_none());
        }
        queued
    }

    fn inflight_count(&self, peer: &PeerHandler) -> usize {
        self.live
            .peers
            .with_live(peer.addr, |live| live.requested_inflight_count())
            .unwrap()
    }

    /// The peer delivers the chunk `request` asked for.
    async fn deliver(&self, peer: &PeerHandler, request: (u32, u32)) {
        let (index, begin) = request;
        let start = (index * PIECE_LENGTH + begin) as usize;
        let block = &self.content[start..start + CHUNK_SIZE as usize];
        peer.on_received_piece(Piece::from_data(index, begin, block))
            .await
            .unwrap();
    }
}

impl std::ops::Deref for Harness {
    type Target = TorrentStateLive;

    fn deref(&self) -> &TorrentStateLive {
        &self.live
    }
}

type Task<'a> = Pin<Box<dyn Future<Output = crate::Result<()>> + 'a>>;

/// The next message `task` queues for the peer, polling it until it does. An
/// Interested is skipped: the requester sends one before it requests.
async fn next_message(task: &mut Task<'_>, rx: &mut PeerRx) -> Message<'static> {
    loop {
        let sent = tokio::select! {
            biased;
            sent = rx.recv() => sent.expect("the peer's writer queue closed"),
            res = task.as_mut() => panic!("the peer task ended: {res:?}"),
        };
        match sent {
            WriterRequest::Message(Message::Interested) => {}
            WriterRequest::Message(msg) => return msg,
            other => panic!("unexpected writer request {other:?}"),
        }
    }
}

/// The next `n` requests the requester sends, as (piece, offset).
async fn next_requests(task: &mut Task<'_>, rx: &mut PeerRx, n: usize) -> BTreeSet<(u32, u32)> {
    let mut requests = BTreeSet::new();
    while requests.len() < n {
        match next_message(task, rx).await {
            Message::Request(r) => assert!(requests.insert((r.index, r.begin))),
            other => panic!("expected a request, got {other:?}"),
        }
    }
    requests
}

fn chunks(index: u32, chunks: impl IntoIterator<Item = u32>) -> BTreeSet<(u32, u32)> {
    chunks
        .into_iter()
        .map(|c| (index, c * CHUNK_SIZE))
        .collect()
}

fn every_chunk() -> BTreeSet<(u32, u32)> {
    (0..PIECES)
        .flat_map(|p| chunks(p, 0..CHUNKS_PER_PIECE))
        .collect()
}

impl Harness {
    /// The unchoked `peer` is asked for every chunk, on the paused clock:
    /// the first piece's four at the depth of a peer no block has come from
    /// yet; then the first of them arrives, at once, which makes room for the
    /// rest. Returns the piece asked for first.
    async fn request_every_chunk(
        &self,
        peer: &PeerHandler,
        task: &mut Task<'_>,
        rx: &mut PeerRx,
    ) -> u32 {
        let first = next_requests(task, rx, CHUNKS_PER_PIECE as usize).await;
        let (piece, _) = *first.first().unwrap();
        assert_eq!(first, chunks(piece, 0..CHUNKS_PER_PIECE));
        self.deliver(peer, (piece, 0)).await;
        let other = 1 - piece;
        assert_eq!(
            next_requests(task, rx, CHUNKS_PER_PIECE as usize).await,
            chunks(other, 0..CHUNKS_PER_PIECE)
        );
        piece
    }
}

// libtorrent `peer_connection::update_desired_queue_size`: four requests
// before any block arrives (`m_desired_queue_size`), then the blocks the
// peer delivers in `request_queue_time` at its pace, between two and the
// most outstanding; one while it is snubbed.
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

    // A block the moment it was asked for: every request we allow, and no
    // more than the peer's `reqq`.
    h.deliver(&fast, (fast_piece, 0)).await;
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

    // A block a second: three seconds' worth.
    tokio::time::advance(Duration::from_secs(1)).await;
    h.deliver(&slow, (slow_piece, 0)).await;
    assert_eq!(depth(&slow), 3);

    // A block every two seconds: one and a half, so the least, two.
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

    // A block the peer had already sent is let through, and not counted.
    h.deliver(&peer, (taken, CHUNK_SIZE)).await;
    assert_eq!(h.missing_chunks(taken), vec![1, 2, 3]);

    peer.on_i_am_unchoked();
    let mut again = chunks(taken, 1..CHUNKS_PER_PIECE);
    again.extend(chunks(other, 0..CHUNKS_PER_PIECE));
    assert_eq!(
        next_requests(&mut requester, &mut rx, again.len()).await,
        again
    );
}

/// The next `n` cancels `task` sends, as (piece, offset).
async fn next_cancels(task: &mut Task<'_>, rx: &mut PeerRx, n: usize) -> BTreeSet<(u32, u32)> {
    let mut cancels = BTreeSet::new();
    while cancels.len() < n {
        match next_message(task, rx).await {
            Message::Cancel(r) => assert!(cancels.insert((r.index, r.begin))),
            other => panic!("expected a cancel, got {other:?}"),
        }
    }
    cancels
}

#[tokio::test]
async fn an_undelivered_request_is_free_again_after_the_timeout_and_a_delivered_one_is_kept() {
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

    // Nothing more arrives: the watcher cancels every undelivered request
    // once the deadline passes, and not before.
    let mut watcher: Task<'_> = Box::pin(peer.time_out_requests());
    let undelivered = chunks(piece, 1..CHUNKS_PER_PIECE);
    assert_eq!(
        next_cancels(&mut watcher, &mut rx, undelivered.len()).await,
        undelivered
    );
    assert!(tokio::time::Instant::now() >= deadline);

    // The undelivered chunks are free for any peer; the delivered one stays had.
    assert_eq!(h.inflight_count(&peer), 0);
    assert_eq!(h.queued_pieces(), vec![0, 1]);
    assert_eq!(h.missing_chunks(piece), vec![1, 2, 3]);
    assert_eq!(h.missing_chunks(1 - piece), vec![0, 1, 2, 3]);
}

impl Harness {
    /// Whether the peer stats snapshot shows the peer at `peer` snubbed.
    fn snubbed(&self, peer: &PeerHandler) -> bool {
        self.per_peer_stats_snapshot(Default::default()).peers[&peer.addr.to_string()].snubbed
    }
}

#[tokio::test]
async fn a_timed_out_peer_holds_one_request_until_a_block_arrives() {
    let h = Harness::new("a_timed_out_peer_is_snubbed").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    tokio::time::pause();
    let mut task: Task<'_> = Box::pin(peer.task_peer_chunk_requester());

    // No block has come from the peer yet: it is asked for one piece.
    peer.on_i_am_unchoked();
    let first = next_requests(&mut task, &mut rx, CHUNKS_PER_PIECE as usize).await;
    assert!(!h.snubbed(&peer));

    // No block arrives: every request times out and the peer is snubbed.
    assert_eq!(next_cancels(&mut task, &mut rx, first.len()).await, first);
    assert!(h.snubbed(&peer));

    // Snubbed, the peer is asked for one chunk and may hold no more.
    let asked = next_requests(&mut task, &mut rx, 1).await;
    assert_eq!(h.inflight_count(&peer), 1);
    assert!(!peer.can_send_request());

    // The block it was asked for arrives at once: the snub is lifted and
    // the requester fills the deeper window with the rest.
    let delivered = *asked.first().unwrap();
    h.deliver(&peer, delivered).await;
    assert!(!h.snubbed(&peer));
    assert!(peer.can_send_request());
    let mut rest = every_chunk();
    rest.remove(&delivered);
    assert_eq!(next_requests(&mut task, &mut rx, rest.len()).await, rest);
}

#[tokio::test]
async fn a_snubbed_peer_takes_no_piece_from_another_peer() {
    let h = Harness::new("a_snubbed_peer_does_not_steal").await;
    tokio::time::pause();
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    owner.on_i_am_unchoked();
    assert!(owner.acquire_next_piece().unwrap().is_some());
    assert!(owner.acquire_next_piece().unwrap().is_some());
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
    snubbed.acquire_next_piece().unwrap();
    let owned = |p| {
        h.live
            .lock_read("test")
            .get_pieces()
            .unwrap()
            .is_owned_by(h.piece(p), owner.addr)
    };
    assert!(owned(0) && owned(1));
    assert!(fast.acquire_next_piece().unwrap().is_some());
}

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
    /// The peer set the fast bit in its handshake.
    fn speak_fast(&self, peer: &PeerHandler) {
        self.peers
            .with_live_mut(peer.addr, "test", |live| live.fast = true)
            .unwrap();
    }
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

/// What the upload side queues for a peer's writer.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Sent {
    Choke,
    Unchoke,
    /// The block at (piece, offset), handed to the writer to send.
    Block(u32, u32),
    /// A Reject for the block at (piece, offset).
    Reject(u32, u32),
    /// An Allowed Fast for the piece.
    AllowedFast(u32),
}

fn sent(request: WriterRequest) -> Sent {
    match request {
        WriterRequest::Message(Message::Choke) => Sent::Choke,
        WriterRequest::Message(Message::Unchoke) => Sent::Unchoke,
        WriterRequest::Message(Message::Reject(r)) => Sent::Reject(r.index, r.begin),
        WriterRequest::Message(Message::AllowedFast(piece)) => Sent::AllowedFast(piece),
        WriterRequest::ReadChunkRequest(c) => Sent::Block(c.piece_index.get(), c.offset),
        other => panic!("unexpected writer request {other:?}"),
    }
}

/// The next thing queued for the peer's writer, waiting until there is one.
async fn next_sent(rx: &mut PeerRx) -> Sent {
    sent(rx.recv().await.expect("the peer's writer queue closed"))
}

/// The request for chunk `chunk` of piece `index`.
fn request(index: u32, chunk: u32) -> Request {
    Request {
        index,
        begin: chunk * CHUNK_SIZE,
        length: CHUNK_SIZE,
    }
}

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
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, CHUNK_SIZE));

    // Both blocks are with the writer, unsent: the cancelled one is not sent.
    peer.on_received_message(Message::Cancel(request(0, 0)))
        .await
        .unwrap();
    assert!(!peer.claim_upload(&h.chunk(0, 0)));
    assert!(peer.claim_upload(&h.chunk(0, 1)));

    // A block cancelled before the rate limits let it through never reaches
    // the writer: the next one does.
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

    // Unchoked, and a block arrives at once, which deepens the window: the
    // rest is asked for.
    peer.on_i_am_unchoked();
    h.deliver(&peer, (1, 0)).await;
    assert_eq!(
        next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await,
        chunks(0, 0..CHUNKS_PER_PIECE)
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

impl Harness {
    /// The peer takes piece `index`, acquiring pieces until it has it.
    fn take(&self, peer: &PeerHandler, index: u32) {
        peer.on_i_am_unchoked();
        while peer
            .acquire_next_piece()
            .unwrap()
            .expect("the piece is not free")
            .get()
            != index
        {}
    }

    /// The peer is asked for, and sends, chunk `chunk` of piece `index`,
    /// with its bytes flipped when `corrupt` holds.
    async fn send(
        &self,
        peer: &PeerHandler,
        (index, chunk): (u32, u32),
        corrupt: bool,
    ) -> anyhow::Result<()> {
        let info = self.chunk(index, chunk);
        self.peers
            .with_live_mut(peer.addr, "test", |live| live.add_inflight_request(info))
            .unwrap();
        let start = (index * PIECE_LENGTH + chunk * CHUNK_SIZE) as usize;
        let mut block = self.content[start..start + CHUNK_SIZE as usize].to_vec();
        if corrupt {
            block[0] ^= 0xff;
        }
        peer.on_received_piece(Piece::from_data(index, chunk * CHUNK_SIZE, &block[..]))
            .await
    }

    fn banned(&self, peer: &PeerHandler) -> bool {
        self.peers.is_banned(peer.addr.ip())
    }

    /// Whether a peer connecting from `addr` is turned away.
    fn refused(&self, addr: &str) -> bool {
        let (tx, _rx) = unbounded_channel();
        self.peers
            .add_incoming(
                addr.parse().unwrap(),
                Id20::default(),
                tx,
                ConnectionKind::Tcp,
                6881,
            )
            .is_err()
    }

    /// The trust points, hash failures and parole of the peer.
    fn trust(&self, peer: &PeerHandler) -> (i8, u8, bool) {
        let trust = self.peers.with_peer(peer.addr, |p| p.trust()).unwrap();
        (trust.points(), trust.hashfails(), trust.on_parole())
    }

    /// `bad` sends the first half of piece 0, its second block corrupt, and
    /// chokes us; `good` sends the rest, and the piece fails.
    async fn fail_shared(&self, bad: &PeerHandler, good: &PeerHandler) {
        self.take(bad, 0);
        self.send(bad, (0, 0), false).await.unwrap();
        self.send(bad, (0, 1), true).await.unwrap();
        bad.on_i_am_choked().unwrap();
        self.take(good, 0);
        self.send(good, (0, 2), false).await.unwrap();
        self.send(good, (0, 3), false).await.unwrap();
        assert_eq!(self.missing_chunks(0), vec![0, 1, 2, 3]);
    }
}

/// Whether the peer's writer was told to close the connection.
fn told_to_disconnect(rx: &mut PeerRx) -> bool {
    std::iter::from_fn(|| rx.try_recv().ok())
        .any(|r| matches!(r, WriterRequest::Disconnect(Err(_))))
}

#[tokio::test]
async fn the_sender_of_a_bad_block_is_banned_once_the_piece_passes_and_the_others_are_not() {
    let h = Harness::new("smart_ban_two_senders").await;
    tokio::time::pause();
    let (bad, mut bad_rx) = h.peer("127.0.0.1:6881");
    let (good, mut good_rx) = h.peer("127.0.0.2:6881");
    let (third, mut third_rx) = h.peer("127.0.0.3:6881");

    // The piece the two shared failed: it is downloaded again, and each
    // loses two points and goes on parole, but neither is banned, nor
    // disconnected, as either may have sent the bad block.
    h.fail_shared(&bad, &good).await;
    for peer in [&bad, &good] {
        assert_eq!(h.trust(peer), (-2, 1, true));
        assert!(!h.banned(peer));
    }
    assert!(!told_to_disconnect(&mut bad_rx) && !told_to_disconnect(&mut good_rx));

    // A third peer sends the whole piece and it passes: the block the bad
    // peer sent differs from the one now had.
    h.take(&third, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&third, (0, chunk), false).await.unwrap();
    }
    assert!(h.missing_chunks(0).is_empty());
    assert!(h.banned(&bad));
    assert!(told_to_disconnect(&mut bad_rx));
    assert!(!h.banned(&good) && !told_to_disconnect(&mut good_rx));
    assert!(!h.banned(&third) && !told_to_disconnect(&mut third_rx));
    assert_eq!(h.trust(&third), (1, 0, false));

    // The bad peer is turned away when it connects again.
    assert!(h.refused("127.0.0.1:50000"));
    // The records went with the pass.
    assert!(
        h.lock_write("test")
            .smart_ban
            .piece_passed(h.piece(0))
            .is_empty()
    );
}

#[tokio::test]
async fn the_sole_sender_of_a_failed_piece_is_banned_at_once() {
    let h = Harness::new("sole_sender_banned").await;
    tokio::time::pause();
    let (peer, mut rx) = h.peer("127.0.0.1:6881");

    h.take(&peer, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&peer, (0, chunk), chunk == 2).await.unwrap();
    }

    assert_eq!(h.missing_chunks(0), vec![0, 1, 2, 3]);
    assert_eq!(h.trust(&peer), (-2, 1, true));
    assert!(h.banned(&peer));
    assert!(told_to_disconnect(&mut rx));
    assert!(h.refused("127.0.0.1:50000"));
    assert!(!h.refused("127.0.0.2:6881"));
}

// libtorrent torrent::piece_failed adds the piece's size to the failed
// bytes; peer_connection::incoming_piece adds a block it has no use for to
// the redundant bytes. Both reach the tracker as `corrupt` and `redundant`.
#[tokio::test]
async fn a_failed_piece_counts_as_corrupt_and_a_block_already_had_as_redundant() {
    let h = Harness::new("corrupt_and_redundant_bytes").await;
    tokio::time::pause();
    let (peer, _rx) = h.peer("127.0.0.1:6881");

    h.take(&peer, 0);
    h.send(&peer, (0, 0), false).await.unwrap();
    assert_eq!((h.get_failed_bytes(), h.get_redundant_bytes()), (0, 0));
    h.send(&peer, (0, 0), false).await.unwrap();
    assert_eq!(
        (h.get_failed_bytes(), h.get_redundant_bytes()),
        (0, u64::from(CHUNK_SIZE))
    );

    for chunk in 1..CHUNKS_PER_PIECE {
        h.send(&peer, (0, chunk), chunk == 2).await.unwrap();
    }
    assert_eq!(h.missing_chunks(0), vec![0, 1, 2, 3]);
    assert_eq!(
        (h.get_failed_bytes(), h.get_redundant_bytes()),
        (u64::from(PIECE_LENGTH), u64::from(CHUNK_SIZE))
    );
}

#[tokio::test]
async fn a_peer_in_four_failed_pieces_is_banned_and_a_passed_piece_earns_a_point() {
    let h = Harness::new("trust_floor_ban").await;
    tokio::time::pause();
    let (a, _a_rx) = h.peer("127.0.0.1:6881");
    let (b, _b_rx) = h.peer("127.0.0.2:6881");
    let (c, _c_rx) = h.peer("127.0.0.3:6881");

    // A passed piece: b earns a point.
    h.take(&b, 1);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&b, (1, chunk), false).await.unwrap();
    }
    assert_eq!(h.trust(&b), (1, 0, false));
    // b chokes us, so no piece it took on the way stays its own.
    b.on_i_am_choked().unwrap();

    // a shares four failed pieces, two with b and two with c. Each failure
    // costs both senders two points; a reaches the floor at the fourth and
    // is banned, the others, in two each, are not.
    for (failures, other) in [(1, &b), (2, &c), (3, &b)] {
        h.fail_shared(&a, other).await;
        assert_eq!(h.trust(&a), (-2 * failures, failures as u8, true));
        assert!(!h.banned(&a));
    }
    h.fail_shared(&a, &c).await;
    assert_eq!(h.trust(&a), (peer::Trust::MIN_POINTS, 4, true));
    assert!(h.banned(&a));
    assert_eq!(h.trust(&b), (-3, 2, true));
    assert_eq!(h.trust(&c), (-4, 2, true));
    assert!(!h.banned(&b) && !h.banned(&c));
}

mod v2 {
    //! The fetch of a v2 torrent's piece layers from peers that speak v2,
    //! and the answers a seed gives them (libtorrent `hash_picker`,
    //! `bt_peer_connection::on_hashes`, `on_hash_reject`,
    //! `on_hash_request`).

    use super::*;
    use crate::{
        core::{
            Id32,
            merkle::{self, PieceTree},
            v2_test_torrent::{TestTorrent as V2TestTorrent, file_hashes, payload},
        },
        peer_protocol::{HashRequest, Hashes},
        torrent_state::live::hash_fetch::Pick,
    };

    const PIECE: u64 = 16384;
    /// Two runs of the layer: 512 pieces, then 88 asked for as 128.
    const PIECES: u32 = 600;

    /// A v2 torrent without its piece layers: a file of [`PIECES`] pieces
    /// and one of a single piece.
    fn layerless() -> V2TestTorrent {
        let mut torrent = V2TestTorrent::new(
            vec![
                ("a", payload(PIECES as usize * PIECE as usize, 1)),
                ("b", payload(100, 2)),
            ],
            PIECE,
        );
        torrent.layers = false;
        torrent
    }

    impl Harness {
        /// `torrent` live, none of its pieces had, and no peers.
        async fn v2(name: &str, torrent: &V2TestTorrent) -> Self {
            let out = TempDir::with_prefix(name).unwrap();
            let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
                .await
                .unwrap();
            let pieces: usize = torrent
                .files
                .iter()
                .map(|(_, data)| data.len().div_ceil(PIECE as usize))
                .sum();
            let have = BF::from_boxed_slice(vec![0u8; pieces.div_ceil(8)].into_boxed_slice());
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(torrent.torrent_bytes()),
                    Some(AddTorrentOptions {
                        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                        initial_have: Some(have),
                        ..Default::default()
                    }),
                )
                .await
                .unwrap()
                .into_handle()
                .unwrap();
            handle.wait_until_initialized().await.unwrap();
            Self {
                live: handle.live().unwrap(),
                content: torrent.v1_payload(),
                _session: session,
                _torrent: None,
                _out: out,
            }
        }

        fn expected(&self, piece: u32) -> Option<Id32> {
            self.metadata.v2.as_ref().unwrap().expected(piece)
        }
    }

    /// The tree over the piece layer of file "a".
    fn tree(torrent: &V2TestTorrent) -> PieceTree {
        let layer = file_hashes(&torrent.files[0].1, PIECE).1.unwrap();
        PieceTree::from_piece_layer(layer, 1)
    }

    /// The answer a peer holding the layers gives to `request`.
    fn answer(torrent: &V2TestTorrent, request: HashRequest) -> Hashes<ByteBufOwned> {
        let hashes = tree(torrent)
            .hashes(
                request.base,
                request.index.into(),
                request.count.into(),
                request.proof_layers,
            )
            .unwrap();
        Hashes::new(request, &hashes)
    }

    async fn next_hash_request(task: &mut Task<'_>, rx: &mut PeerRx) -> HashRequest {
        match next_message(task, rx).await {
            Message::HashRequest(r) => r,
            other => panic!("expected a hash request, got {other:?}"),
        }
    }

    fn shape(r: HashRequest) -> (u32, u32, u32, u32) {
        (r.base, r.index, r.count, r.proof_layers)
    }

    #[tokio::test]
    async fn a_v2_piece_waits_for_its_layer_run_which_a_v2_peer_proves() {
        let torrent = layerless();
        let h = Harness::v2("a_v2_piece_waits_for_its_layer", &torrent).await;
        let (peer, mut rx) = h.peer_speaking("127.0.0.1:6881", true);
        peer.on_i_am_unchoked();

        // Only the one-piece file, which its root proves, may be fetched.
        assert_eq!(
            peer.acquire_next_piece().unwrap().map(|p| p.get()),
            Some(PIECES)
        );
        assert_eq!(peer.acquire_next_piece().unwrap(), None);

        // The peer is asked for both runs of the layer, from the piece
        // layer, each proven to the root (libtorrent
        // `hash_picker::pick_hashes`).
        let mut fetch: Task<'_> = Box::pin(peer.fetch_layers());
        let first = next_hash_request(&mut fetch, &mut rx).await;
        let second = next_hash_request(&mut fetch, &mut rx).await;
        assert_eq!(first.root, tree(&torrent).root());
        assert_eq!(shape(first), (0, 0, 512, 9));
        assert_eq!(shape(second), (0, 512, 128, 9));

        // The second run proven: its pieces may be fetched, the first's not.
        peer.on_hashes(answer(&torrent, second).as_borrowed())
            .unwrap();
        let piece = peer.acquire_next_piece().unwrap().unwrap().get();
        assert!((512..PIECES).contains(&piece), "{piece}");
        assert_eq!(h.expected(0), None);

        // Both proven: the torrent file carries the piece layers, as one
        // that came with them (libtorrent `write_torrent_file`).
        peer.on_hashes(answer(&torrent, first).as_borrowed())
            .unwrap();
        let tree = tree(&torrent);
        assert!((0..PIECES).all(|p| h.expected(p) == Some(tree.pieces()[p as usize])));
        let mut whole = layerless();
        whole.layers = true;
        assert_eq!(h.metadata.torrent_bytes(), whole.torrent_bytes());
        assert!(matches!(
            h.layer_fetch.pick(
                peer.addr,
                |_| true,
                Duration::ZERO,
                tokio::time::Instant::now()
            ),
            Pick::Done
        ));
    }

    #[tokio::test]
    async fn hashes_that_prove_nothing_drop_the_peer_and_the_run_goes_to_another() {
        let torrent = layerless();
        let h = Harness::v2("hashes_that_prove_nothing", &torrent).await;
        let (bad, mut bad_rx) = h.peer_speaking("127.0.0.1:6881", true);
        let mut bad_fetch: Task<'_> = Box::pin(bad.fetch_layers());
        let first = next_hash_request(&mut bad_fetch, &mut bad_rx).await;
        let _second = next_hash_request(&mut bad_fetch, &mut bad_rx).await;

        // test_hash_picker.cpp `add_bad_hashes`: the last hash flipped.
        let mut hashes = answer(&torrent, first).hashes();
        hashes.last_mut().unwrap().0[0] ^= 1;
        assert!(
            bad.on_hashes(Hashes::new(first, &hashes).as_borrowed())
                .is_err()
        );
        assert_eq!(h.expected(0), None);

        // The run is free for the next peer; the other stays with the first.
        let (good, mut good_rx) = h.peer_speaking("127.0.0.2:6881", true);
        let mut good_fetch: Task<'_> = Box::pin(good.fetch_layers());
        assert_eq!(
            next_hash_request(&mut good_fetch, &mut good_rx).await,
            first
        );
        good.on_hashes(answer(&torrent, first).as_borrowed())
            .unwrap();
        assert_eq!(h.expected(0), Some(tree(&torrent).pieces()[0]));
    }

    #[tokio::test]
    async fn a_rejected_run_goes_to_another_peer_and_an_unasked_answer_is_ignored() {
        let torrent = layerless();
        let h = Harness::v2("a_rejected_run", &torrent).await;
        let (first_peer, mut first_rx) = h.peer_speaking("127.0.0.1:6881", true);
        let mut first_fetch: Task<'_> = Box::pin(first_peer.fetch_layers());
        let first = next_hash_request(&mut first_fetch, &mut first_rx).await;
        let second = next_hash_request(&mut first_fetch, &mut first_rx).await;

        // An answer from a peer that was not asked is ignored (libtorrent
        // `bt_peer_connection::on_hashes`).
        let (other, mut other_rx) = h.peer_speaking("127.0.0.2:6881", true);
        other
            .on_hashes(answer(&torrent, first).as_borrowed())
            .unwrap();
        assert_eq!(h.expected(0), None);

        // A reject frees the run for the other peer at once (libtorrent
        // `hash_picker::hashes_rejected`), and the peer that rejected it is
        // not asked again.
        first_peer.on_hash_reject(first);
        let mut other_fetch: Task<'_> = Box::pin(other.fetch_layers());
        assert_eq!(
            next_hash_request(&mut other_fetch, &mut other_rx).await,
            first
        );
        assert!(matches!(
            h.layer_fetch.pick(
                first_peer.addr,
                |_| true,
                Duration::from_secs(60),
                tokio::time::Instant::now()
            ),
            Pick::Wait(_)
        ));
        other
            .on_hashes(answer(&torrent, first).as_borrowed())
            .unwrap();
        first_peer
            .on_hashes(answer(&torrent, second).as_borrowed())
            .unwrap();
        assert!((0..PIECES).all(|p| h.expected(p).is_some()));
    }

    #[tokio::test]
    async fn an_unanswered_run_goes_to_another_peer_at_the_request_timeout() {
        let torrent = layerless();
        let h = Harness::v2("an_unanswered_run", &torrent).await;
        tokio::time::pause();
        let start = tokio::time::Instant::now();
        let (slow, mut slow_rx) = h.peer_speaking("127.0.0.1:6881", true);
        let mut slow_fetch: Task<'_> = Box::pin(slow.fetch_layers());
        let first = next_hash_request(&mut slow_fetch, &mut slow_rx).await;
        let second = next_hash_request(&mut slow_fetch, &mut slow_rx).await;

        // The slow peer has delivered nothing, so its timeout is the
        // longest; the other peer is asked for both runs once it passes.
        let (other, mut other_rx) = h.peer_speaking("127.0.0.2:6881", true);
        let mut other_fetch: Task<'_> = Box::pin(other.fetch_layers());
        assert_eq!(
            next_hash_request(&mut other_fetch, &mut other_rx).await,
            first
        );
        assert_eq!(
            next_hash_request(&mut other_fetch, &mut other_rx).await,
            second
        );
        let timeout = h
            .peers
            .with_live(slow.addr, |live| live.request_timeout())
            .unwrap();
        assert_eq!(timeout, Duration::from_secs(60));
        assert!(tokio::time::Instant::now() >= start + timeout);

        // A late answer from the slow peer still counts.
        slow.on_hashes(answer(&torrent, first).as_borrowed())
            .unwrap();
        assert!(h.expected(0).is_some());
    }

    #[tokio::test]
    async fn a_seed_answers_hash_requests_from_its_layers_and_rejects_the_rest() {
        let mut torrent = layerless();
        torrent.layers = true;
        let h = Harness::v2("a_seed_answers_hash_requests", &torrent).await;
        let (peer, mut rx) = h.peer_speaking("127.0.0.1:6881", true);
        let tree = tree(&torrent);
        let ask = |root, base, index, count, proof_layers| HashRequest {
            root,
            base,
            index,
            count,
            proof_layers,
        };

        // The second run as libtorrent's `hash_picker::pick_hashes` asks
        // for it: 88 pieces padded to 512 nodes, and the uncle that proves
        // the root (libtorrent
        // `bt_peer_connection::on_hash_request`).
        let request = ask(tree.root(), 0, 512, 512, 9);
        peer.on_hash_request(request).unwrap();
        let Ok(WriterRequest::Hashes(answer)) = rx.try_recv() else {
            panic!("expected hashes");
        };
        assert_eq!(answer.request, request);
        let hashes = answer.hashes();
        let (nodes, uncles) = hashes.split_at(512);
        let pad = merkle::pad_hash(1, 1);
        let real = (PIECES - 512) as usize;
        assert_eq!(&nodes[..real], &tree.pieces()[512..]);
        assert!(nodes[real..].iter().all(|n| *n == pad));
        assert_eq!(uncles.len(), 1);
        assert_eq!(merkle::proven_root(nodes, 512, pad, uncles), tree.root());

        // A malformed request, one for a root of no file, and one for the
        // one-piece file, which has no layer: each is rejected.
        let one_piece = file_hashes(&torrent.files[1].1, PIECE).0.unwrap();
        for bad in [
            ask(tree.root(), 0, 0, 0, 0),
            ask(tree.root(), 0, 0, 1, 10),
            ask(tree.pieces()[0], 0, 0, 1, 0),
            ask(one_piece, 0, 0, 1, 0),
        ] {
            peer.on_hash_request(bad).unwrap();
            assert!(
                matches!(rx.try_recv(), Ok(WriterRequest::Message(Message::HashReject(r))) if r == bad),
                "{bad:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_hash_request_for_a_v1_torrent_drops_the_peer() {
        let h = Harness::new("a_hash_request_for_a_v1_torrent").await;
        let (peer, _rx) = h.peer_speaking("127.0.0.1:6881", true);
        let request = HashRequest {
            root: Id32::default(),
            base: 0,
            index: 0,
            count: 1,
            proof_layers: 0,
        };
        assert!(peer.on_hash_request(request).is_err());
    }
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
    // for; the writer sends none, so all stay queued.
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

    // Everything queued for the writer up to that last block: each block
    // once, the first Reject the one past the limit.
    let mut blocks = Vec::new();
    let mut rejects = Vec::new();
    loop {
        match next_sent(&mut rx).await {
            Sent::Block(0, begin) => {
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

/// A memory storage that refuses the next `refusals` pieces it is handed.
#[derive(Clone, Default)]
struct RefusingStorageFactory {
    inner: MemoryStorageFactory,
    refusals: Arc<AtomicU32>,
}

struct RefusingStorage {
    inner: Box<dyn TorrentStorage>,
    refusals: Arc<AtomicU32>,
}

impl StorageFactory for RefusingStorageFactory {
    type Storage = RefusingStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<RefusingStorage> {
        Ok(RefusingStorage {
            inner: Box::new(self.inner.create(shared, metadata)?),
            refusals: self.refusals.clone(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

impl TorrentStorage for RefusingStorage {
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
        Ok(Box::new(RefusingStorage {
            inner: self.inner.take()?,
            refusals: self.refusals.clone(),
        }))
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let refuse = self
            .refusals
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        anyhow::ensure!(!refuse, "no room for piece {piece:?}");
        self.inner.on_piece_completed(piece)
    }
}

impl Harness {
    fn have(&self, index: u32) -> bool {
        self.live
            .lock_read("test")
            .get_chunks()
            .unwrap()
            .get_have_pieces()
            .as_slice()[index as usize]
    }
}

// libtorrent: the write completes before the hash job, and only then
// `torrent::piece_passed`; on a disk error `torrent::on_piece_verified`
// neither credits nor blames the senders.
#[tokio::test]
async fn a_piece_the_storage_refuses_is_not_had_and_is_downloaded_again() {
    let storage = RefusingStorageFactory::default();
    storage.refusals.store(1, Ordering::SeqCst);
    let h = Harness::build_on(
        "storage_refuses",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    tokio::time::pause();
    let (peer, _rx) = h.peer("127.0.0.1:6881");

    h.take(&peer, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&peer, (0, chunk), false).await.unwrap();
    }
    assert!(!h.have(0));
    assert_eq!(h.missing_chunks(0), vec![0, 1, 2, 3]);
    assert!(h.queued_pieces().contains(&0));
    assert_eq!(h.trust(&peer), (0, 0, false));
    assert!(storage.inner.log.completed_pieces().is_empty());

    // Taken the second time, it is ours.
    h.take(&peer, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&peer, (0, chunk), false).await.unwrap();
    }
    assert!(h.have(0));
    assert_eq!(h.trust(&peer), (1, 0, false));
    assert_eq!(storage.inner.log.completed_pieces(), vec![0]);
}

// A piece is counted into every file with bytes in it, an empty file among
// them or not (libtorrent `torrent::we_have` updates every file the piece
// maps to).
#[test]
fn a_piece_is_counted_into_every_file_past_an_empty_one() {
    let file = |name: &str, piece_range: std::ops::Range<u32>| FileInfo {
        relative_filename: name.into(),
        offset_in_torrent: 0,
        piece_range,
        attrs: Default::default(),
        len: 0,
    };
    let files = [
        file("a", 0..1),
        file("empty at the start of piece 1", 1..1),
        file("b", 1..2),
        file("empty inside piece 1", 1..1),
        file("c", 1..3),
        file("d", 3..4),
    ];
    let names = |piece| -> Vec<usize> { files_in_piece(&files, piece).map(|(i, _)| i).collect() };
    assert_eq!(names(0), vec![0]);
    assert_eq!(names(1), vec![2, 3, 4]);
    assert_eq!(names(2), vec![4]);
    assert_eq!(names(3), vec![5]);
}

/// A PEX message adding `added` and dropping `dropped`.
fn pex(added: &[&str], dropped: &[&str]) -> UtPex<ByteBufOwned> {
    let addrs = |a: &[&str]| -> Vec<SocketAddr> { a.iter().map(|a| a.parse().unwrap()).collect() };
    let (added, dropped) = (addrs(added), addrs(dropped));
    let added = added.iter().map(|&addr| PexPeerInfo { flags: 0, addr });
    UtPex::from_peers(added, dropped.iter().copied())
}

impl Harness {
    /// Whether the peer at `addr` is in the torrent's peer list.
    fn knows(&self, addr: &str) -> bool {
        self.peers
            .with_peer(addr.parse().unwrap(), |_| ())
            .is_some()
    }

    /// The extension ids we advertise to `peer`.
    fn advertised(&self, peer: &PeerHandler) -> extended::PeerExtendedMessageIds {
        let mut hs = ExtendedHandshake::new();
        peer.update_my_extended_handshake(&mut hs).unwrap();
        hs.m
    }
}

// libtorrent `ut_pex_peer_plugin::on_extended`: the added peers are
// candidates, the dropped ones are only forgotten by the sender.
#[tokio::test]
async fn a_pex_message_adds_its_added_peers_and_not_its_dropped_ones() {
    let h = Harness::new("pex_added_and_dropped").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    assert!(h.advertised(&peer).ut_pex.is_some());

    let msg = pex(&["10.0.0.1:6881", "[2001:db8::1]:6881"], &["10.0.0.2:6881"]);
    (&peer)
        .on_received_message(Message::Extended(ExtendedMessage::UtPex(msg.as_borrowed())))
        .await
        .unwrap();
    assert!(h.knows("10.0.0.1:6881"));
    assert!(h.knows("[2001:db8::1]:6881"));
    assert!(!h.knows("10.0.0.2:6881"));
}

impl PeerHandler {
    /// The peer sends us `msg`.
    async fn send_pex(&self, msg: &UtPex<ByteBufOwned>) -> anyhow::Result<()> {
        self.on_received_message(Message::Extended(ExtendedMessage::UtPex(msg.as_borrowed())))
            .await
    }
}

// libtorrent `ut_pex_peer_plugin::on_extended`: a connection's PEX adds at
// most `max_pex_peers` of each family to our list, and a peer on a local
// network only when the connection is on one too.
#[tokio::test]
async fn a_pex_message_adds_at_most_max_pex_peers_and_a_local_peer_only_from_a_local_one() {
    let h = Harness::new("pex_limits").await;
    let (remote, _remote_rx) = h.peer("198.51.100.1:6881");
    let public: Vec<String> = (0..60).map(|i| format!("203.0.113.{i}:6881")).collect();
    let mut added = vec!["192.168.1.1:6881", "10.0.0.1:6881"];
    added.extend(public.iter().map(String::as_str));
    remote.send_pex(&pex(&added, &[])).await.unwrap();
    assert!(!h.knows("192.168.1.1:6881"));
    assert!(!h.knows("10.0.0.1:6881"));
    let known = public.iter().filter(|a| h.knows(a)).count();
    assert_eq!(known, peer::MAX_PEX_PEERS);

    let (local, _local_rx) = h.peer("192.168.1.2:6881");
    local
        .send_pex(&pex(&["192.168.1.1:6881"], &[]))
        .await
        .unwrap();
    assert!(h.knows("192.168.1.1:6881"));
}

// libtorrent `ut_pex_peer_plugin::on_extended`, `m_last_pex`: a peer may
// send six PEX messages a minute; the seventh ends the connection.
#[tokio::test]
async fn a_seventh_pex_message_within_a_minute_ends_the_connection() {
    let h = Harness::new("pex_flood").await;
    tokio::time::pause();
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let msg = pex(&[], &[]);
    for _ in 0..6 {
        peer.send_pex(&msg).await.unwrap();
    }
    tokio::time::advance(Duration::from_secs(59)).await;
    assert!(peer.send_pex(&msg).await.is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    peer.send_pex(&msg).await.unwrap();
}

// libtorrent `ut_pex_peer_plugin::on_extended`: a PEX message past 500 KiB
// ends the connection (`pex_message_too_large`), unread.
#[tokio::test]
async fn a_pex_message_past_500_kib_ends_the_connection() {
    let h = Harness::new("pex_too_large").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    // An IPv4 address and its flags byte: seven bytes a peer.
    let past = MAX_PEX_MESSAGE_LEN / 7 + 1;
    let named = |n: usize| {
        let added = (0..n).map(|i| PexPeerInfo {
            flags: 0,
            addr: SocketAddr::from(([10, (i >> 16) as u8, (i >> 8) as u8, i as u8], 6881)),
        });
        UtPex::from_peers(added, std::iter::empty())
    };
    assert!(peer.send_pex(&named(past)).await.is_err());
    assert!(!h.knows("10.0.0.0:6881"));
    peer.send_pex(&named(past - 1)).await.unwrap();
    assert!(h.knows("10.0.0.0:6881"));
}

// libtorrent `session::add_extension`: a session without the ut_pex plugin
// does not advertise it, and takes no peers from it.
#[tokio::test]
async fn a_session_without_pex_does_not_advertise_it_nor_take_peers_from_it() {
    let opts = SessionOptions {
        disable_pex: true,
        ..isolated_session_opts(false)
    };
    let h = Harness::build_on(
        "pex_disabled",
        0x00,
        opts,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let advertised = h.advertised(&peer);
    assert!(advertised.ut_pex.is_none());
    assert!(advertised.ut_metadata.is_some());

    let msg = pex(&["10.0.0.1:6881"], &[]);
    (&peer)
        .on_received_message(Message::Extended(ExtendedMessage::UtPex(msg.as_borrowed())))
        .await
        .unwrap();
    assert!(!h.knows("10.0.0.1:6881"));
}

/// A handshake from a peer, with the DHT bit when `dht` holds.
fn handshake(dht: bool) -> Handshake {
    Handshake::new(Id20::default(), Id20::new([9; 20])).offering_dht(dht)
}

/// A DHT node on 127.0.0.1 that knows no other node.
async fn local_dht(silent: SocketAddr) -> crate::dht::Dht {
    crate::dht::DhtState::with_config(crate::dht::DhtConfig {
        listen_addr: Some((std::net::Ipv4Addr::LOCALHOST, 0).into()),
        bootstrap_addrs: Some(vec![silent.to_string()]),
        ..Default::default()
    })
    .await
    .unwrap()
}

// libtorrent `bt_peer_connection::write_handshake` sets the DHT bit only
// when the session runs a DHT node; `write_dht_port` sends nothing without
// one.
#[tokio::test]
async fn a_session_without_dht_sets_no_dht_bit_and_sends_no_port() {
    let h = Harness::new("dht_port_no_dht").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    assert_eq!((&peer).dht_port(), None);
    (&peer)
        .on_handshake(handshake(true), ConnectionKind::Tcp, false)
        .unwrap();
    (&peer)
        .on_received_message(Message::Port(6881))
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());
}

// libtorrent `peer_connection::incoming_dht_port` adds the named node on
// each PORT; here a connection's repeat of the port it named last adds
// nothing, and another port is a node to add.
#[tokio::test]
async fn a_repeated_dht_port_names_no_new_node() {
    let h = Harness::new("dht_port_repeat").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    let port = |port| peer.on_received_message(Message::Port(port));
    let news = |port| {
        h.peers
            .with_live_mut(peer.addr, "test", |live| live.dht_node_named(port))
            .unwrap()
    };
    port(6881).await.unwrap();
    port(6881).await.unwrap();
    assert!(!news(6881));
    port(6882).await.unwrap();
    assert!(!news(6882));
    assert!(news(6881));
}

// libtorrent `bt_peer_connection::write_dht_port` after the handshake to a
// peer with the DHT bit, `on_dht_port` answering a PORT from a peer
// without it, once per connection; `peer_connection::incoming_dht_port`
// and `node::add_node`: the named node is pinged and, on its reply, joins
// the routing table.
#[tokio::test]
async fn our_dht_port_goes_once_to_a_dht_peer_and_its_port_adds_its_node() {
    let silent = tokio::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let silent = silent.local_addr().unwrap();
    let opts = SessionOptions {
        dht: Some(crate::DhtSessionConfig {
            bootstrap_addrs: Some(vec![silent.to_string()]),
            ..Default::default()
        }),
        ..isolated_session_opts(false)
    };
    let h = Harness::build_on(
        "dht_port",
        0x00,
        opts,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let ours = h._session.get_dht().unwrap().clone();
    let our_port = ours.listen_addr().port();

    let (bit, mut bit_rx) = h.peer("127.0.0.2:6881");
    assert_eq!((&bit).dht_port(), Some(our_port));
    (&bit)
        .on_handshake(handshake(true), ConnectionKind::Tcp, false)
        .unwrap();
    assert!(matches!(
        bit_rx.try_recv(),
        Ok(WriterRequest::Message(Message::Port(p))) if p == our_port
    ));

    let (no_bit, mut no_bit_rx) = h.peer("127.0.0.1:6882");
    (&no_bit)
        .on_handshake(handshake(false), ConnectionKind::Tcp, false)
        .unwrap();
    assert!(no_bit_rx.try_recv().is_err());

    let theirs = local_dht(silent).await;
    let their_id = theirs.stats().id;
    (&no_bit)
        .on_received_message(Message::Port(theirs.listen_addr().port()))
        .await
        .unwrap();
    assert!(matches!(
        no_bit_rx.try_recv(),
        Ok(WriterRequest::Message(Message::Port(p))) if p == our_port
    ));
    (&no_bit)
        .on_received_message(Message::Port(theirs.listen_addr().port()))
        .await
        .unwrap();
    assert!(no_bit_rx.try_recv().is_err());

    while !ours.with_routing_tables(|v4, _| v4.iter().any(|n| n.id() == their_id)) {
        tokio::task::yield_now().await;
    }
    theirs.cancellation_token().cancel();
}

impl Harness {
    /// A torrent of `pieces` pieces (at most 8), with `have` the bitfield
    /// of those had, added to super-seed.
    async fn super_seeding(name: &str, pieces: u32, have: u8) -> Self {
        let add = AddTorrentOptions {
            storage_factory: Some(MemoryStorageFactory::default().clone_box()),
            super_seeding: true,
            ..Default::default()
        };
        Self::build_added(name, pieces, have, isolated_session_opts(false), add).await
    }

    /// A peer at `addr` whose first message is a bitfield of `pieces`, or
    /// a keep-alive when it has none.
    async fn peer_with(&self, addr: &str, pieces: &[u32]) -> (PeerHandler, PeerRx) {
        let (peer, rx) = self.new_peer(addr);
        let mut bits = peer::empty_bitfield(self.lengths.total_pieces());
        for &p in pieces {
            bits.set(p as usize, true);
        }
        let first = if pieces.is_empty() {
            Message::KeepAlive
        } else {
            Message::Bitfield(ByteBuf(bits.as_raw_slice()))
        };
        (&peer).on_received_message(first).await.unwrap();
        (peer, rx)
    }
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
// sends no bitfield and tells the peer of a piece from
// `torrent::get_piece_to_super_seed`: one it lacks, as rare as any, and
// handed to no other peer while one such is left.
#[tokio::test]
async fn a_super_seed_sends_no_bitfield_and_hands_each_peer_the_rarest_piece_none_holds() {
    let h = Harness::super_seeding("super_seed_rarest", 3, 0xe0).await;
    let (s, mut s_rx) = h.peer_with("127.0.0.1:6881", &[0]).await;
    assert!(!(&s).should_send_bitfield());
    assert_eq!(told_have(&mut s_rx), [1]);

    // Piece 0 is held by one peer, piece 1 handed to one: piece 2.
    let (_p, mut p_rx) = h.peer_with("127.0.0.2:6882", &[]).await;
    assert_eq!(told_have(&mut p_rx), [2]);

    // Every piece is held or handed: one that is not handed comes first.
    let (_q, mut q_rx) = h.peer_with("127.0.0.3:6883", &[]).await;
    assert_eq!(told_have(&mut q_rx), [0]);

    // A peer that lacks nothing is handed nothing.
    let (_seed, mut seed_rx) = h.peer_with("127.0.0.4:6884", &[0, 1, 2]).await;
    assert_eq!(told_have(&mut seed_rx), [] as [u32; 0]);
}

// libtorrent `peer_connection::incoming_request`: while super-seeding, a
// request for a piece other than the one handed out is refused.
#[tokio::test]
async fn a_super_seeded_peer_is_served_only_the_piece_it_was_handed() {
    let h = Harness::super_seeding("super_seed_serves_handed", 2, 0xc0).await;
    let (peer, mut rx) = h.peer_with("127.0.0.1:6881", &[]).await;
    assert_eq!(told_have(&mut rx), [0]);
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);

    peer.on_received_message(Message::Request(request(1, 0)))
        .await
        .unwrap();
    assert!(rx.is_empty());
    peer.on_received_message(Message::Request(request(0, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, 0));
}

// libtorrent `peer_connection::incoming_have` with `strict_super_seeding`
// (BEP 16): a peer's own HAVE for its piece hands it nothing; once another
// peer reports the piece, every peer handed it that has it is handed the
// next.
#[tokio::test]
async fn the_next_piece_goes_out_once_another_peer_reports_the_handed_one() {
    let h = Harness::super_seeding("super_seed_spread", 3, 0xe0).await;
    let (a, mut a_rx) = h.peer_with("127.0.0.1:6881", &[]).await;
    let (b, mut b_rx) = h.peer_with("127.0.0.2:6882", &[]).await;
    assert_eq!(told_have(&mut a_rx), [0]);
    assert_eq!(told_have(&mut b_rx), [1]);

    (&a).on_received_message(Message::Have(0)).await.unwrap();
    assert_eq!(told_have(&mut a_rx), [] as [u32; 0]);

    (&b).on_received_message(Message::Have(0)).await.unwrap();
    assert_eq!(told_have(&mut a_rx), [2]);
    assert_eq!(told_have(&mut b_rx), [] as [u32; 0]);
}

// libtorrent `peer_connection::incoming_have` with `strict_super_seeding`:
// with one peer, its own HAVE is the only report there can be.
#[tokio::test]
async fn a_lone_peer_is_handed_the_next_piece_on_its_own_have() {
    let h = Harness::super_seeding("super_seed_alone", 2, 0xc0).await;
    let (a, mut a_rx) = h.peer_with("127.0.0.1:6881", &[]).await;
    assert_eq!(told_have(&mut a_rx), [0]);
    (&a).on_received_message(Message::Have(0)).await.unwrap();
    assert_eq!(told_have(&mut a_rx), [1]);
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

impl Harness {
    /// A live peer at `addr` whose extended handshake names ut_holepunch
    /// when `holepunch` holds, and the queue of what we send it.
    fn holepunch_peer(&self, addr: &str, holepunch: bool) -> (PeerHandler, PeerRx) {
        let (peer, rx) = self.peer(addr);
        let mut hs = ExtendedHandshake::new();
        hs.m.ut_holepunch = holepunch.then_some(9);
        (&peer).on_extended_handshake(&hs).unwrap();
        (peer, rx)
    }
}

/// The holepunch messages we queued for the peer since last asked, draining
/// its queue.
fn holepunch_sent(rx: &mut PeerRx) -> Vec<UtHolepunch> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|r| match r {
            WriterRequest::Message(Message::Extended(ExtendedMessage::UtHolepunch(m))) => Some(m),
            _ => None,
        })
        .collect()
}

async fn send_holepunch(peer: &PeerHandler, msg: UtHolepunch) {
    peer.on_received_message(Message::Extended(ExtendedMessage::UtHolepunch(msg)))
        .await
        .unwrap();
}

fn sock(addr: &str) -> SocketAddr {
    addr.parse().unwrap()
}

/// An address on 127.0.0.1 nothing listens on, so a connection to it fails.
fn closed_port() -> SocketAddr {
    std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
}

// libtorrent `bt_peer_connection::write_extensions` names ut_holepunch on
// every connection.
#[tokio::test]
async fn every_connection_advertises_ut_holepunch() {
    let h = Harness::new("holepunch_advertised").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    assert_eq!(
        h.advertised(&peer).ut_holepunch,
        Some(MY_EXTENDED_UT_HOLEPUNCH)
    );
}

// libtorrent `bt_peer_connection::on_holepunch`, `rendezvous`: the sender
// is told to connect to the target, the target to the sender.
#[tokio::test]
async fn a_rendezvous_tells_each_peer_to_connect_to_the_other() {
    let h = Harness::new("holepunch_rendezvous").await;
    let (a, mut a_rx) = h.holepunch_peer("203.0.113.1:6881", true);
    let (_b, mut b_rx) = h.holepunch_peer("203.0.113.2:6881", true);
    send_holepunch(&a, UtHolepunch::Rendezvous(sock("203.0.113.2:6881"))).await;
    assert_eq!(
        holepunch_sent(&mut a_rx),
        [UtHolepunch::Connect(sock("203.0.113.2:6881"))]
    );
    assert_eq!(
        holepunch_sent(&mut b_rx),
        [UtHolepunch::Connect(sock("203.0.113.1:6881"))]
    );
}

// libtorrent `bt_peer_connection::on_holepunch`, `rendezvous`: not
// connected, no support and no self are errors back to the sender; a local
// peer and one that is not are not introduced; and a peer that did not
// advertise the extension is not heard.
#[tokio::test]
async fn a_rendezvous_that_cannot_be_made_is_refused_or_ignored() {
    use ut_holepunch::error::{NO_SELF, NO_SUPPORT, NOT_CONNECTED};
    let h = Harness::new("holepunch_refused").await;
    let (a, mut a_rx) = h.holepunch_peer("203.0.113.1:6881", true);
    let (_mute, mut mute_rx) = h.holepunch_peer("203.0.113.2:6881", false);
    let (_local, mut local_rx) = h.holepunch_peer("192.168.1.2:6881", true);
    let (deaf, mut deaf_rx) = h.holepunch_peer("203.0.113.3:6881", false);

    for (target, code) in [
        ("203.0.113.9:6881", NOT_CONNECTED),
        ("203.0.113.2:6881", NO_SUPPORT),
        ("203.0.113.1:6881", NO_SELF),
    ] {
        send_holepunch(&a, UtHolepunch::Rendezvous(sock(target))).await;
        assert_eq!(
            holepunch_sent(&mut a_rx),
            [UtHolepunch::Error(sock(target), code)]
        );
    }
    send_holepunch(&a, UtHolepunch::Rendezvous(sock("192.168.1.2:6881"))).await;
    assert_eq!(holepunch_sent(&mut a_rx), []);
    assert_eq!(holepunch_sent(&mut local_rx), []);
    assert_eq!(holepunch_sent(&mut mute_rx), []);

    send_holepunch(&deaf, UtHolepunch::Rendezvous(sock("203.0.113.1:6881"))).await;
    assert_eq!(holepunch_sent(&mut deaf_rx), []);
    assert_eq!(holepunch_sent(&mut a_rx), []);
}

// libtorrent `bt_peer_connection::on_holepunch`, `connect`: we connect to
// the peer over uTP, so a session without uTP takes no connect; a local
// address is taken only from a local peer.
#[tokio::test]
async fn a_connect_admits_the_peer_only_over_utp_and_a_local_one_only_from_a_local_peer() {
    let target = closed_port();
    let without = Harness::new("holepunch_connect_no_utp").await;
    let (from, _rx) = without.holepunch_peer("127.0.0.1:6881", true);
    send_holepunch(&from, UtHolepunch::Connect(target)).await;
    assert!(!without.knows(&target.to_string()));

    let opts = SessionOptions {
        listen: Some(crate::ListenerOptions {
            mode: crate::ListenerMode::UtpOnly,
            listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            ..Default::default()
        }),
        ..isolated_session_opts(false)
    };
    let with = Harness::build_on(
        "holepunch_connect_utp",
        0x00,
        opts,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let (remote, _remote_rx) = with.holepunch_peer("203.0.113.1:6881", true);
    send_holepunch(&remote, UtHolepunch::Connect(target)).await;
    assert!(!with.knows(&target.to_string()));

    let (from, _rx) = with.holepunch_peer("127.0.0.1:6881", true);
    send_holepunch(&from, UtHolepunch::Connect(target)).await;
    assert!(with.knows(&target.to_string()));
}

/// A PEX message naming `addr` with `flags`.
fn pex_flagged(addr: SocketAddr, flags: u8) -> Vec<u8> {
    let SocketAddr::V4(v4) = addr else {
        panic!("an IPv4 address")
    };
    let mut b = b"d5:added6:".to_vec();
    b.extend_from_slice(&v4.ip().octets());
    b.extend_from_slice(&v4.port().to_be_bytes());
    b.extend_from_slice(b"7:added.f1:");
    b.push(flags);
    b.push(b'e');
    b
}

// libtorrent `peer_connection::connect_failed`: a failed connection to a
// peer PEX flagged as speaking the holepunch extension sends a rendezvous
// for it to `torrent::find_introducer`, a holepunch peer whose PEX named it.
#[tokio::test]
async fn a_failed_connection_asks_the_peer_whose_pex_named_it_to_introduce_us() {
    let h = Harness::new("holepunch_introducer").await;
    // On another host than the target: no connection to its address stands
    // in the way of the connect.
    let (introducer, mut rx) = h.holepunch_peer("127.0.0.2:6881", true);
    let target = closed_port();
    let msg = pex_flagged(target, extended::ut_pex::PEX_HOLEPUNCH);
    let msg = crate::bencode::from_bytes::<UtPex<ByteBuf>>(&msg).unwrap();
    // The PEX is taken in before the connect loop runs: the test runs on
    // one thread and yields only below.
    (&introducer)
        .on_received_message(Message::Extended(ExtendedMessage::UtPex(msg)))
        .await
        .unwrap();
    assert!(h.knows(&target.to_string()));

    // The connect loop connects to the target, which refuses.
    let sent = loop {
        match rx.recv().await.unwrap() {
            WriterRequest::Message(Message::Extended(ExtendedMessage::UtHolepunch(m))) => break m,
            _ => continue,
        }
    };
    assert_eq!(sent, UtHolepunch::Rendezvous(target));
}

// libtorrent `ut_pex_plugin::tick`, `send_peer`: our PEX message lists each
// connected peer whose listening address we know, where it listens, flagged
// a seed, connected over uTP, speaking the holepunch extension, as it is to
// us; the next one drops a peer gone. A peer not on a local network is not
// told of one that is (it would drop it, `ut_pex_peer_plugin::on_extended`).
#[tokio::test]
async fn our_pex_message_lists_each_connected_peer_where_it_listens_flagged_for_what_it_is_to_us() {
    use extended::ut_pex::{PEX_HOLEPUNCH, PEX_SEED, PEX_UTP};
    let h = Harness::new("pex_flags_sent").await;
    let listening_on_6881 = |peer: &PeerHandler, holepunch: bool| {
        let mut hs = ExtendedHandshake::new();
        hs.p = Some(6881);
        if holepunch {
            hs.m.ut_holepunch = Some(9);
        }
        peer.on_extended_handshake(&hs).unwrap();
    };
    let utp = |addr: &str| {
        let (tx, rx) = unbounded_channel();
        h.peers
            .add_incoming(sock(addr), Id20::default(), tx, ConnectionKind::Utp, 6881)
            .unwrap();
        rx
    };
    let (seed, _seed_rx) = h.peer("203.0.113.1:50001");
    listening_on_6881(&seed, false);
    let (plain, _plain_rx) = h.new_peer("203.0.113.2:50002");
    listening_on_6881(&plain, false);
    let _utp_rx = utp("203.0.113.3:6881");
    let (holepunch, _holepunch_rx) = h.new_peer("203.0.113.4:50004");
    listening_on_6881(&holepunch, true);
    // Never says where it listens.
    let (_silent, _silent_rx) = h.new_peer("203.0.113.5:50005");
    let _local_rx = utp("192.168.1.3:6881");

    let sorted = |msg: UtPex<ByteBufOwned>| {
        let mut wire = Vec::new();
        crate::bencode::bencode_serialize_to_writer(&msg.as_borrowed(), &mut wire).unwrap();
        let msg = crate::bencode::from_bytes::<UtPex<ByteBuf>>(&wire).unwrap();
        let mut added: Vec<_> = msg.added_peers().map(|p| (p.addr, p.flags)).collect();
        let mut dropped: Vec<_> = msg.dropped_peers().map(|p| p.addr).collect();
        added.sort();
        dropped.sort();
        (added, dropped)
    };

    let remote = sock("198.51.100.1:6881");
    let mut told = HashSet::new();
    let (added, dropped) = sorted(h.next_pex_message(remote, &mut told).unwrap());
    assert_eq!(
        added,
        [
            (sock("203.0.113.1:6881"), PEX_SEED),
            (sock("203.0.113.2:6881"), 0),
            (sock("203.0.113.3:6881"), PEX_UTP),
            (sock("203.0.113.4:6881"), PEX_HOLEPUNCH),
        ]
    );
    assert!(dropped.is_empty());
    assert!(h.next_pex_message(remote, &mut told).is_none());

    let mut local_told = HashSet::new();
    let (added, _) = sorted(
        h.next_pex_message(sock("192.168.1.9:6881"), &mut local_told)
            .unwrap(),
    );
    assert_eq!(added.len(), 5);
    assert!(added.contains(&(sock("192.168.1.3:6881"), PEX_UTP)));

    h.peers.drop_peer(plain.addr);
    let (added, dropped) = sorted(h.next_pex_message(remote, &mut told).unwrap());
    assert!(added.is_empty());
    assert_eq!(dropped, [sock("203.0.113.2:6881")]);
}

// libtorrent `torrent::do_pause` only disconnects: `torrent::m_peer_list`,
// with each `torrent_peer`'s `banned` and `trust_points`, is the list the
// resumed torrent works from.
#[tokio::test]
async fn a_pause_keeps_the_bans_and_the_trust_of_the_peer_list() {
    let h = Harness::new("pause_keeps_peer_list").await;
    let banned = sock("203.0.113.1:6881");
    let (peer, _rx) = h.peer("203.0.113.2:6881");
    h.peers.ban(banned);
    h.peers
        .with_peer_mut(peer.addr, "test", |p| p.piece_failed(false))
        .unwrap();
    let trust = h.peers.with_peer(peer.addr, |p| p.trust()).unwrap();
    assert_ne!(trust, Default::default());

    let session = &h._session;
    let handle = session.get(h.shared.id.into()).unwrap();
    session.pause(&handle).await.unwrap();
    session.unpause(&handle).await.unwrap();
    let resumed = handle.live().unwrap();
    assert!(!Arc::ptr_eq(&resumed, &h.live));
    assert!(resumed.peers.is_banned(banned.ip()));
    assert_eq!(
        resumed.peers.with_peer(peer.addr, |p| p.trust()),
        Some(trust)
    );
}

#[tokio::test]
async fn a_seed_ends_a_connection_whose_unchoked_peer_asks_for_nothing_for_a_minute() {
    let h = Harness::seeding("no_request").await;
    tokio::time::pause();
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);

    // Forty seconds on the peer asks for a block, and the minute runs again
    // from when the block is taken to be sent.
    tokio::time::advance(Duration::from_secs(40)).await;
    peer.on_received_message(Message::Request(request(0, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Block(0, 0));
    let chunk = h.chunk(0, 0);
    let sent = tokio::time::Instant::now();
    let claimed = h
        .peers
        .with_live_mut(peer.addr, "send", |live| live.claim_upload(&chunk));
    assert_eq!(claimed, Some(true));

    peer.time_out_idle().await.unwrap();
    assert!(tokio::time::Instant::now().duration_since(sent) > peer::NO_REQUEST_TIMEOUT);
}

#[tokio::test]
async fn peers_that_want_nothing_of_each_other_part_after_ten_minutes_when_connections_run_short() {
    // Five connections in all: the torrent's connections run short.
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        peer_limit: Some(5),
        ..Default::default()
    };
    let h = Harness::build_added(
        "no_interest",
        PIECES,
        0xc0,
        isolated_session_opts(false),
        add,
    )
    .await;
    tokio::time::pause();
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let peer = &peer;

    // Five minutes on the peer is interested a while; the ten minutes run
    // from when it no longer is.
    tokio::time::advance(Duration::from_secs(300)).await;
    peer.on_received_message(Message::Interested).await.unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Unchoke);
    peer.on_received_message(Message::NotInterested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut rx).await, Sent::Choke);
    let uninterested = tokio::time::Instant::now();

    peer.time_out_idle().await.unwrap();
    assert!(tokio::time::Instant::now().duration_since(uninterested) > peer::INACTIVITY_TIMEOUT);
}

// libtorrent `bt_peer_connection::write_extensions` says upload_only in the
// extended handshake of a torrent that only uploads, and `torrent::finished`
// tells each peer whose handshake named upload_only once it does
// (`torrent::send_upload_only`).
#[tokio::test]
async fn a_finished_torrent_tells_the_peers_that_take_upload_only() {
    let h = Harness::new("upload_only").await;
    let (seed, mut seed_rx) = h.peer("127.0.0.1:6881");
    let (takes, mut takes_rx) = h.peer_with("127.0.0.2:6881", &[]).await;
    let (deaf, mut deaf_rx) = h.peer_with("127.0.0.3:6881", &[]).await;
    let mut names_it = ExtendedHandshake::new();
    names_it.m.upload_only = Some(3);
    (&takes).on_extended_handshake(&names_it).unwrap();
    (&deaf)
        .on_extended_handshake(&ExtendedHandshake::new())
        .unwrap();
    let says = |peer: &PeerHandler| {
        let mut hs = ExtendedHandshake::new();
        peer.update_my_extended_handshake(&mut hs).unwrap();
        hs.upload_only
    };
    assert_eq!(says(&takes), None);

    let mut requester: Task<'_> = Box::pin(seed.task_peer_chunk_requester());
    seed.on_i_am_unchoked();
    let requests = next_requests(&mut requester, &mut seed_rx, every_chunk().len()).await;
    drop(requester);
    for request in requests {
        h.deliver(&seed, request).await;
    }

    let upload_only = |sent: &WriterRequest| match sent {
        WriterRequest::Message(Message::Extended(ExtendedMessage::UploadOnly(enabled))) => {
            Some(*enabled)
        }
        _ => None,
    };
    loop {
        let sent = takes_rx
            .recv()
            .await
            .expect("the peer's writer queue closed");
        if let Some(enabled) = upload_only(&sent) {
            assert!(enabled);
            break;
        }
    }
    // Every peer was told at once, so the one that named no id would have
    // been sent it by now.
    while let Ok(sent) = deaf_rx.try_recv() {
        assert_eq!(upload_only(&sent), None);
    }
    assert_eq!(says(&takes), Some(1));
}

// libtorrent `peer_list::connection_closed`: only a connection that failed
// counts against the peer; one we asked to close did not fail.
#[tokio::test]
async fn a_connection_we_asked_to_close_counts_no_failure() {
    let h = Harness::new("asked_to_close").await;
    let mut failcounts = Vec::new();
    for (addr, error) in [
        ("127.0.0.1:6881", crate::Error::Disconnect),
        ("127.0.0.2:6881", crate::Error::TestDisconnect),
    ] {
        let (peer, _rx) = h.peer(addr);
        let counters = peer.counters.clone();
        peer.on_peer_died(Some(error)).unwrap();
        let failcount = h
            .peers
            .with_peer(sock(addr), |p| p.connect_rank().1)
            .unwrap();
        failcounts.push((failcount, counters.errors.load(Ordering::Relaxed)));
    }
    assert_eq!(failcounts, [(0, 0), (1, 1)]);
}

/// A torrent of at most `peer_limit` connections in a session of at most
/// `connections_limit`.
async fn turnover_harness(name: &str, peer_limit: usize, connections_limit: usize) -> Harness {
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        peer_limit: Some(peer_limit),
        ..Default::default()
    };
    let opts = SessionOptions {
        connections_limit,
        ..isolated_session_opts(false)
    };
    Harness::build_added(name, PIECES, 0x00, opts, add).await
}

// libtorrent `session_impl::second_tick`: below the session's cutoff, a
// torrent holding 90% of its own connections turns 4% of them over, at
// least one, and no more than it has connect candidates to replace them;
// the one it would keep least goes (`torrent::disconnect_peers`).
#[tokio::test]
async fn a_torrent_at_its_connection_cutoff_turns_its_least_worth_peer_over() {
    let h = turnover_harness("turnover_torrent", 10, 200).await;
    let _held = h.peer_semaphore.clone().try_acquire_many_owned(9).unwrap();
    let (kept, mut kept_rx) = h.peer("127.0.0.1:6881");
    let (_least, mut least_rx) = h.peer("127.0.0.2:6881");
    h.peers.with_live_mut(kept.addr, "test", |live| {
        live.interest(true, tokio::time::Instant::now())
    });
    let closed = |rx: &mut PeerRx| {
        std::iter::from_fn(|| rx.try_recv().ok())
            .any(|r| matches!(r, WriterRequest::Disconnect(Ok(()))))
    };

    // Nobody to take its place: nobody goes.
    h._session.turn_peers_over();
    assert!(!closed(&mut least_rx));

    h.peers.add_peer(sock("203.0.113.1:6881"));
    h._session.turn_peers_over();
    assert!(closed(&mut least_rx));
    assert!(!closed(&mut kept_rx));
}

// libtorrent `session_impl::second_tick`: when the session holds 90% of its
// connections, the torrent with the most turns peers over, however far
// below its own cap.
#[tokio::test]
async fn a_session_at_its_connection_cutoff_turns_over_the_busiest_torrents_peers() {
    let h = turnover_harness("turnover_session", 128, 10).await;
    let _held: Vec<_> = (0..9)
        .map(|_| h._session.connection_slots.try_open().unwrap())
        .collect();
    let (_peer, mut rx) = h.peer("127.0.0.1:6881");
    h.peers.add_peer(sock("203.0.113.1:6881"));

    h._session.turn_peers_over();
    assert!(
        std::iter::from_fn(|| rx.try_recv().ok())
            .any(|r| matches!(r, WriterRequest::Disconnect(Ok(()))))
    );
}
