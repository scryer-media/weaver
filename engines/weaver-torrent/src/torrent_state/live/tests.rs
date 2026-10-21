// SPDX-License-Identifier: GPL-3.0-or-later

//! A live torrent and one peer driven by hand: the test plays the remote
//! peer, calling the handler as the connection would, and reads what the
//! handler queues for the wire.

use std::{collections::BTreeSet, future::Future, pin::Pin};

use tempfile::TempDir;

use super::*;
use crate::{
    AddTorrent, AddTorrentOptions, Session,
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
    _torrent: Option<TestTorrent>,
    _out: TempDir,
}

impl Harness {
    /// A live torrent of two four-chunk pieces, none of them had, and no peers.
    async fn new(name: &str) -> Self {
        let torrent = TestTorrent::new(name, (PIECES * PIECE_LENGTH) as usize, PIECE_LENGTH)
            .await
            .unwrap();
        let out = TempDir::with_prefix(name).unwrap();
        let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
            .await
            .unwrap();
        let have = BF::from_boxed_slice(vec![0u8; 1].into_boxed_slice());
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
            speaks_v2: AtomicBool::new(speaks_v2),
            flow_control: Mutex::new(PeerFlowControl::new(self.live.max_request_window())),
            state: self.live.clone(),
            tx,
            counters,
            first_message_received: AtomicBool::new(true),
            cancel_token: self.live.cancellation_token.child_token(),
            client_name_and_version: String::new(),
        };
        let mut every_piece = make_piece_bitfield(&self.lengths);
        every_piece[..self.lengths.total_pieces() as usize].fill(true);
        handler
            .on_bitfield(ByteBufOwned::from(every_piece.as_raw_slice().to_vec()))
            .unwrap();
        (handler, rx)
    }

    fn piece(&self, index: u32) -> ValidPieceIndex {
        self.lengths.validate_piece_index(index).unwrap()
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
