// SPDX-License-Identifier: GPL-3.0-or-later

//! A live torrent and one peer driven by hand: the test plays the remote
//! peer, calling the handler as the connection would, and reads what the
//! handler queues for the wire.

use std::{collections::BTreeSet, future::Future, pin::Pin};

use tempfile::TempDir;

use super::*;
use crate::{
    AddTorrent, AddTorrentOptions, Session, SessionOptions,
    core::constants::CHUNK_SIZE,
    storage::StorageFactory,
    tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
};

const CHUNKS_PER_PIECE: u32 = 4;
const PIECE_LENGTH: u32 = CHUNK_SIZE * CHUNKS_PER_PIECE;
const PIECES: u32 = 2;

struct Harness {
    live: Arc<TorrentStateLive>,
    content: Vec<u8>,
    _session: Arc<Session>,
    _torrent: TestTorrent,
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
        let torrent = TestTorrent::new(name, (PIECES * PIECE_LENGTH) as usize, PIECE_LENGTH)
            .await
            .unwrap();
        let out = TempDir::with_prefix(name).unwrap();
        let opts = SessionOptions {
            unchoke_slots_limit: slots,
            ..isolated_session_opts(false)
        };
        let session = Session::new_with_opts(out.path().into(), opts)
            .await
            .unwrap();
        let have = BF::from_boxed_slice(vec![have].into_boxed_slice());
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent.torrent.clone()),
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
            content: torrent.content.clone(),
            _session: session,
            _torrent: torrent,
            _out: out,
        }
    }

    /// A live peer at `addr` with every piece, and the queue of what we send it.
    fn peer(&self, addr: &str) -> (PeerHandler, PeerRx) {
        let addr: SocketAddr = addr.parse().unwrap();
        let (tx, rx) = unbounded_channel();
        let counters = self
            .live
            .peers
            .add_incoming(addr, Id20::default(), tx.clone(), ConnectionKind::Tcp)
            .unwrap();
        let handler = PeerHandler {
            addr,
            incoming: true,
            on_bitfield_notify: Default::default(),
            flow_control: Mutex::new(PeerFlowControl::new(self.live.max_request_window())),
            state: self.live.clone(),
            tx,
            counters,
            first_message_received: AtomicBool::new(true),
            cancel_token: self.live.cancellation_token.child_token(),
            client_name_and_version: String::new(),
        };
        handler
            .on_bitfield(ByteBufOwned::from(vec![0xc0u8]))
            .unwrap();
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
            .iter_queued_pieces(&g.file_priorities, &self.live.metadata.file_infos)
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

#[tokio::test]
async fn a_choke_returns_every_outstanding_request_and_unchoke_asks_again() {
    let h = Harness::new("a_choke_returns_requests").await;
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    let mut requester: Task<'_> = Box::pin(peer.task_peer_chunk_requester());

    peer.on_i_am_unchoked();
    assert_eq!(
        next_requests(&mut requester, &mut rx, every_chunk().len()).await,
        every_chunk()
    );
    h.deliver(&peer, (0, 0)).await;

    peer.on_i_am_choked().unwrap();

    // Nothing is in flight to the peer, and both pieces are back in the
    // queue for any peer, the delivered chunk kept.
    assert_eq!(h.inflight_count(&peer), 0);
    assert_eq!(h.queued_pieces(), vec![0, 1]);
    assert_eq!(h.missing_chunks(0), vec![1, 2, 3]);
    assert_eq!(h.missing_chunks(1), vec![0, 1, 2, 3]);

    // A block the peer had already sent is let through, and not counted.
    h.deliver(&peer, (0, CHUNK_SIZE)).await;
    assert_eq!(h.missing_chunks(0), vec![1, 2, 3]);

    peer.on_i_am_unchoked();
    let mut again = chunks(0, 1..CHUNKS_PER_PIECE);
    again.extend(chunks(1, 0..CHUNKS_PER_PIECE));
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

    peer.on_i_am_unchoked();
    assert_eq!(
        next_requests(&mut requester, &mut rx, every_chunk().len()).await,
        every_chunk()
    );
    drop(requester);

    // The first block takes 10 s, so the peer's timeout is 12 s from it.
    tokio::time::advance(Duration::from_secs(10)).await;
    h.deliver(&peer, (0, 0)).await;
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
    let mut undelivered = every_chunk();
    undelivered.remove(&(0, 0));
    assert_eq!(
        next_cancels(&mut watcher, &mut rx, undelivered.len()).await,
        undelivered
    );
    assert!(tokio::time::Instant::now() >= deadline);

    // The undelivered chunks are free for any peer; the delivered one stays had.
    assert_eq!(h.inflight_count(&peer), 0);
    assert_eq!(h.queued_pieces(), vec![0, 1]);
    assert_eq!(h.missing_chunks(0), vec![1, 2, 3]);
    assert_eq!(h.missing_chunks(1), vec![0, 1, 2, 3]);
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

    peer.on_i_am_unchoked();
    assert_eq!(
        next_requests(&mut task, &mut rx, every_chunk().len()).await,
        every_chunk()
    );
    assert!(!h.snubbed(&peer));

    // No block arrives: every request times out and the peer is snubbed.
    assert_eq!(
        next_cancels(&mut task, &mut rx, every_chunk().len()).await,
        every_chunk()
    );
    assert!(h.snubbed(&peer));

    // Snubbed, the peer is asked for one chunk and may hold no more.
    let asked = next_requests(&mut task, &mut rx, 1).await;
    assert_eq!(h.inflight_count(&peer), 1);
    assert!(!peer.can_send_request());

    // The block it was asked for arrives: the snub is lifted and the
    // requester fills the window with the rest.
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

    assert_eq!(snubbed.acquire_next_piece().unwrap(), None);
    assert!(fast.acquire_next_piece().unwrap().is_some());
}

/// What the upload side queues for a peer's writer.
#[derive(Debug, PartialEq, Eq)]
enum Sent {
    Choke,
    Unchoke,
    /// The block at (piece, offset), handed to the writer to send.
    Block(u32, u32),
}

fn sent(request: WriterRequest) -> Sent {
    match request {
        WriterRequest::Message(Message::Choke) => Sent::Choke,
        WriterRequest::Message(Message::Unchoke) => Sent::Unchoke,
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

    // At the round nobody has moved a byte: the peers that waited, never
    // unchoked, take slots from peers that held one, and no more are held.
    for (_, rx) in waiting.iter_mut() {
        assert_eq!(next_sent(rx).await, Sent::Unchoke);
    }
    let mut choked = 0;
    for (_, rx) in held.iter_mut() {
        if !rx.is_empty() {
            assert_eq!(next_sent(rx).await, Sent::Choke);
            choked += 1;
        }
    }
    assert_eq!(choked, waiting.len());
    let unchoked = peers.iter().filter(|(peer, _)| !h.choked(peer)).count();
    assert_eq!(unchoked, slots);
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

    // The other peer sends us more over the round and takes the one slot.
    other
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    other
        .counters
        .fetched_bytes
        .fetch_add(PIECE_LENGTH.into(), Ordering::Relaxed);
    assert_eq!(next_sent(&mut other_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut held_rx).await, Sent::Choke);

    // Both blocks were with the writer, unsent: neither goes out.
    assert!(!held.claim_upload(&h.chunk(0, 0)));
    assert!(!held.claim_upload(&h.chunk(0, 1)));

    // What the choked peer asks for now is not queued, so never sent.
    held.on_received_message(Message::Request(request(1, 0)))
        .await
        .unwrap();
    assert!(!held.claim_upload(&h.chunk(1, 0)));
    assert!(held_rx.is_empty());
}
