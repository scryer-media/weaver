// SPDX-License-Identifier: GPL-3.0-or-later

//! A live torrent and one peer driven by hand: the test plays the remote
//! peer, calling the handler as the connection would, and reads what the
//! handler queues for the wire.

use std::{
    collections::BTreeSet,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU32, AtomicUsize},
};

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
        Self::added_to(session, out, name, pieces, &[have], add).await
    }

    /// Another live torrent of the harness's session, of the harness's
    /// pieces, `have` the bitfield of those had.
    async fn sibling(&self, name: &str, have: u8) -> Self {
        let add = AddTorrentOptions {
            storage_factory: Some(MemoryStorageFactory::default().clone_box()),
            ..Default::default()
        };
        let out = TempDir::with_prefix(name).unwrap();
        Self::added_to(self._session.clone(), out, name, PIECES, &[have], add).await
    }

    /// A live torrent of `bytes` eight times four-chunk pieces, all had, and
    /// no peers.
    async fn seeding_bytes(name: &str, bytes: usize) -> Self {
        let add = AddTorrentOptions {
            storage_factory: Some(MemoryStorageFactory::default().clone_box()),
            ..Default::default()
        };
        let out = TempDir::with_prefix(name).unwrap();
        let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
            .await
            .unwrap();
        let pieces = 8 * bytes as u32;
        Self::added_to(session, out, name, pieces, &vec![0xff; bytes], add).await
    }

    /// A live torrent of `pieces` four-chunk pieces, `have` the bitfield of
    /// those had, added to `session` with `add`.
    async fn added_to(
        session: Arc<Session>,
        out: TempDir,
        name: &str,
        pieces: u32,
        have: &[u8],
        add: AddTorrentOptions,
    ) -> Self {
        let torrent = TestTorrent::new(name, (pieces * PIECE_LENGTH) as usize, PIECE_LENGTH)
            .await
            .unwrap();
        let have = BF::from_boxed_slice(have.into());
        Self::adding(session, out, torrent, crate::Checkpoint::pieces(have), add).await
    }

    /// `torrent`, live, started from `checkpoint`, added to `session` with
    /// `add`.
    async fn adding(
        session: Arc<Session>,
        out: TempDir,
        torrent: TestTorrent,
        checkpoint: crate::Checkpoint,
        add: AddTorrentOptions,
    ) -> Self {
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent.torrent.clone()),
                Some(AddTorrentOptions {
                    checkpoint: Some(checkpoint),
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

    /// The torrent's handle in its session.
    fn handle(&self) -> crate::torrent_state::ManagedTorrentHandle {
        self._session
            .get(crate::api::TorrentIdOrHash::Id(self.live.shared.id))
            .unwrap()
    }

    /// The torrent taken out of its session, its files left, and added
    /// again with `add`, started from `checkpoint`, as a restart does.
    async fn restarted(self, checkpoint: crate::Checkpoint, add: AddTorrentOptions) -> Self {
        let Self {
            live,
            _session: session,
            _torrent: torrent,
            _out: out,
            ..
        } = self;
        let id = live.shared.id;
        drop(live);
        session
            .delete(crate::api::TorrentIdOrHash::Id(id), false)
            .await
            .unwrap();
        Self::adding(session, out, torrent.unwrap(), checkpoint, add).await
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
    /// yet; then the peer is snubbed, which ends its slow start, and the
    /// first of them arrives at once, which lifts the snub at a pace that
    /// makes room for the rest. Returns the piece asked for first.
    async fn request_every_chunk(
        &self,
        peer: &PeerHandler,
        task: &mut Task<'_>,
        rx: &mut PeerRx,
    ) -> u32 {
        let first = next_requests(task, rx, CHUNKS_PER_PIECE as usize).await;
        let (piece, _) = *first.first().unwrap();
        assert_eq!(first, chunks(piece, 0..CHUNKS_PER_PIECE));
        self.peers
            .with_live_mut(peer.addr, "test", |live| live.snub())
            .unwrap();
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
    let mut take_and_ask = || {
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
        .with_peer_mut(peer.addr, "test", |p| p.piece_failed(false))
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
        chunks(other, 0..depth as u32)
    );
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

// libtorrent `peer_connection::fill_send_buffer`: each peer is served from
// its own send buffer, held to its watermark, so however many blocks one
// peer asked for, the next peer waits one block of it.
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

    // One round hands each peer one block: the light peer's goes out
    // alongside the deep peer's first, and the deep peer gets no second
    // until its writer takes the first.
    assert_eq!(next_sent(&mut light_rx).await, Sent::Block(31, 0));
    assert_eq!(next_sent(&mut deep_rx).await, Sent::Block(0, 0));
    assert!(deep_rx.is_empty());

    // The writer takes each in turn, and the rest follow in the order asked.
    for (n, &(index, chunk)) in asked.iter().enumerate() {
        if n > 0 {
            assert_eq!(
                next_sent(&mut deep_rx).await,
                Sent::Block(index, chunk * CHUNK_SIZE)
            );
        }
        assert!(deep.claim_upload(&h.chunk(index, chunk)));
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
    // The first is with the writer, the second waits for it.
    assert_eq!(next_sent(&mut held_rx).await, Sent::Block(0, 0));

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
    // Piece 0's block is with the writer, piece 1's waits for it.
    assert_eq!(next_sent(&mut fast_rx).await, Sent::Block(0, 0));

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

    // The one slot is the optimistic one: at the first round it passes to
    // the other peer, which never held it.
    other
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut other_rx).await, Sent::Unchoke);
    assert_eq!(next_sent(&mut held_rx).await, Sent::Choke);

    // One block was with the writer and one waited for it, both unsent:
    // neither goes out.
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
            .piece()
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
    assert!(h.peers.piece_passed(h.piece(0)).is_empty());
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
    let (d, _d_rx) = h.peer("127.0.0.4:6881");
    let (e, _e_rx) = h.peer("127.0.0.5:6881");

    // A passed piece: b earns a point.
    h.take(&b, 1);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&b, (1, chunk), false).await.unwrap();
    }
    assert_eq!(h.trust(&b), (1, 0, false));
    // b chokes us, so no piece it took on the way stays its own.
    b.on_i_am_choked().unwrap();

    // a shares four failed pieces, each with a peer that is not on parole
    // (a peer on parole is handed no piece another peer sent part of). Each
    // failure costs both senders two points; a reaches the floor at the
    // fourth and is banned, the others, in one each, are not.
    for (failures, other) in [(1, &b), (2, &c), (3, &d)] {
        h.fail_shared(&a, other).await;
        assert_eq!(h.trust(&a), (-2 * failures, failures as u8, true));
        assert!(!h.banned(&a));
    }
    h.fail_shared(&a, &e).await;
    assert_eq!(h.trust(&a), (peer::Trust::MIN_POINTS, 4, true));
    assert!(h.banned(&a));
    assert_eq!(h.trust(&b), (-1, 1, true));
    for other in [&c, &d, &e] {
        assert_eq!(h.trust(other), (-2, 1, true));
    }
    assert!(!h.banned(&b) && !h.banned(&c) && !h.banned(&d) && !h.banned(&e));
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
                .map(|(_, data)| data.len().div_ceil(torrent.piece_length as usize))
                .sum();
            let have = BF::from_boxed_slice(vec![0u8; pieces.div_ceil(8)].into_boxed_slice());
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(torrent.torrent_bytes()),
                    Some(AddTorrentOptions {
                        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                        checkpoint: Some(crate::Checkpoint::pieces(have)),
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
            peer.acquire_next_piece().unwrap().piece().map(|p| p.get()),
            Some(PIECES)
        );
        assert_eq!(peer.acquire_next_piece().unwrap().piece(), None);

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
        let piece = peer.acquire_next_piece().unwrap().piece().unwrap().get();
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
            Pick::Wait(None)
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

    /// A v2 torrent, a hybrid when `hybrid` holds, of one file of two
    /// four-block pieces, with its layers.
    fn two_pieces(hybrid: bool) -> V2TestTorrent {
        let mut torrent = V2TestTorrent::new(
            vec![("a", payload(2 * PIECE_LENGTH as usize, 3))],
            u64::from(PIECE_LENGTH),
        );
        torrent.hybrid = hybrid;
        torrent
    }

    /// The leaves under piece `index` of `torrent`'s file, as the
    /// answer to the request for them carries them.
    fn leaves(torrent: &V2TestTorrent, index: u32) -> Vec<Id32> {
        let start = (index * PIECE_LENGTH) as usize;
        torrent.files[0].1[start..start + PIECE_LENGTH as usize]
            .chunks(CHUNK_SIZE as usize)
            .map(merkle::hash_block)
            .collect()
    }

    // libtorrent torrent::piece_failed asks for the block hashes of a v2
    // piece whose bad blocks are not known (verify_block_hashes); once they
    // come, hash_picker::add_hashes names the blocks that differ, and
    // torrent::piece_failed penalises their senders alone, as known bad
    // (penalize_peers). Weaver blames no one before.
    async fn the_leaves_of_a_failed_v2_piece_blame_only_its_bad_blocks_sender(hybrid: bool) {
        let torrent = two_pieces(hybrid);
        let h = Harness::v2("leaves_blame_bad_blocks", &torrent).await;
        tokio::time::pause();
        let (bad, mut bad_rx) = h.peer("127.0.0.1:6881");
        let (good, mut good_rx) = h.peer("127.0.0.2:6881");

        // `bad` sends the first half, its second block corrupt, `good` the
        // second: the piece fails, is downloaded again, and no one is
        // blamed yet.
        h.fail_shared(&bad, &good).await;
        for peer in [&bad, &good] {
            assert_eq!(h.trust(peer), (0, 0, false));
            assert!(!h.banned(peer));
        }
        assert_eq!(
            h.get_failed_bytes(),
            u64::from(PIECE_LENGTH),
            "the whole piece counts as corrupt"
        );

        // A peer that speaks v2 is asked for the piece's leaves, proven to
        // its node of the tree.
        let (v2_peer, mut v2_rx) = h.peer_speaking("127.0.0.3:6881", true);
        let mut fetch: Task<'_> = Box::pin(v2_peer.fetch_layers());
        let request = next_hash_request(&mut fetch, &mut v2_rx).await;
        assert_eq!(
            request.root,
            file_hashes(&torrent.files[0].1, PIECE_LENGTH.into())
                .0
                .unwrap()
        );
        assert_eq!(shape(request), (0, 0, CHUNKS_PER_PIECE, 1));

        // Leaves that do not prove the piece drop the peer, and blame no
        // one.
        let mut wrong = leaves(&torrent, 0);
        wrong[1] = merkle::hash_block(b"wrong");
        assert!(
            v2_peer
                .on_hashes(Hashes::new(request, &wrong).as_borrowed())
                .is_err()
        );
        assert_eq!(h.trust(&bad), (0, 0, false));
        h.layer_fetch.forget(v2_peer.addr);

        // The leaves come: the sender of the block that differs is
        // penalised and banned; the other keeps its standing.
        let (other, mut other_rx) = h.peer_speaking("127.0.0.4:6881", true);
        let mut other_fetch: Task<'_> = Box::pin(other.fetch_layers());
        assert_eq!(
            next_hash_request(&mut other_fetch, &mut other_rx).await,
            request
        );
        other
            .on_hashes(Hashes::new(request, &leaves(&torrent, 0)).as_borrowed())
            .unwrap();
        assert_eq!(h.trust(&bad), (-2, 1, true));
        assert!(h.banned(&bad) && told_to_disconnect(&mut bad_rx));
        assert_eq!(h.trust(&good), (0, 0, false));
        assert!(!h.banned(&good) && !told_to_disconnect(&mut good_rx));

        // The leaves are wanted no more.
        assert!(matches!(
            h.layer_fetch.pick(
                other.addr,
                |_| true,
                Duration::ZERO,
                tokio::time::Instant::now()
            ),
            Pick::Wait(None)
        ));
    }

    #[tokio::test]
    async fn the_leaves_of_a_failed_v2_piece_blame_only_the_sender_of_its_bad_block() {
        the_leaves_of_a_failed_v2_piece_blame_only_its_bad_blocks_sender(false).await;
    }

    #[tokio::test]
    async fn the_leaves_of_a_failed_hybrid_piece_blame_only_the_sender_of_its_bad_block() {
        the_leaves_of_a_failed_v2_piece_blame_only_its_bad_blocks_sender(true).await;
    }

    // No peer gave the leaves before the piece passed: the good copy names
    // the sender of the bad block, as the smart ban does, and the leaves
    // are wanted no more.
    #[tokio::test]
    async fn a_failed_v2_piece_that_passes_before_its_leaves_come_blames_by_the_good_copy() {
        let torrent = two_pieces(false);
        let h = Harness::v2("passes_before_leaves", &torrent).await;
        tokio::time::pause();
        let (bad, _bad_rx) = h.peer("127.0.0.1:6881");
        let (good, _good_rx) = h.peer("127.0.0.2:6881");
        let (third, _third_rx) = h.peer("127.0.0.3:6881");
        h.fail_shared(&bad, &good).await;

        h.take(&third, 0);
        for chunk in 0..CHUNKS_PER_PIECE {
            h.send(&third, (0, chunk), false).await.unwrap();
        }
        assert!(h.missing_chunks(0).is_empty());
        assert!(h.banned(&bad));
        assert!(!h.banned(&good) && !h.banned(&third));
        assert_eq!(h.trust(&good), (0, 0, false));
        let (v2_peer, _rx) = h.peer_speaking("127.0.0.4:6881", true);
        assert!(matches!(
            h.layer_fetch.pick(
                v2_peer.addr,
                |_| true,
                Duration::ZERO,
                tokio::time::Instant::now()
            ),
            Pick::Wait(None)
        ));
    }

    // libtorrent torrent::piece_failed: a piece one peer sent all of names
    // that peer, with no block hashes to wait for.
    #[tokio::test]
    async fn the_sole_sender_of_a_failed_v2_piece_is_banned_at_once() {
        let torrent = two_pieces(false);
        let h = Harness::v2("v2_sole_sender", &torrent).await;
        tokio::time::pause();
        let (peer, mut rx) = h.peer("127.0.0.1:6881");
        h.take(&peer, 0);
        for chunk in 0..CHUNKS_PER_PIECE {
            h.send(&peer, (0, chunk), chunk == 2).await.unwrap();
        }
        assert_eq!(h.missing_chunks(0), vec![0, 1, 2, 3]);
        assert_eq!(h.trust(&peer), (-2, 1, true));
        assert!(h.banned(&peer) && told_to_disconnect(&mut rx));
        let (v2_peer, _rx) = h.peer_speaking("127.0.0.2:6881", true);
        assert!(matches!(
            h.layer_fetch.pick(
                v2_peer.addr,
                |_| true,
                Duration::ZERO,
                tokio::time::Instant::now()
            ),
            Pick::Wait(None)
        ));
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

/// A memory storage that refuses the next `refusals` pieces it is handed,
/// then the next `no_room` for want of space, and the next `no_room_writes`
/// block writes for want of space.
#[derive(Clone, Default)]
struct RefusingStorageFactory {
    inner: MemoryStorageFactory,
    refusals: Arc<AtomicU32>,
    no_room: Arc<AtomicU32>,
    no_room_writes: Arc<AtomicU32>,
}

struct RefusingStorage {
    inner: Box<dyn TorrentStorage>,
    refusals: Arc<AtomicU32>,
    no_room: Arc<AtomicU32>,
    no_room_writes: Arc<AtomicU32>,
}

/// Whether `count` was above zero, taking one off it when it was.
fn take_one(count: &AtomicU32) -> bool {
    count
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
}

fn no_room() -> anyhow::Error {
    anyhow::Error::from(std::io::Error::from(std::io::ErrorKind::StorageFull)).context("writing")
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
            no_room: self.no_room.clone(),
            no_room_writes: self.no_room_writes.clone(),
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
        if take_one(&self.no_room_writes) {
            return Err(no_room());
        }
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
            no_room: self.no_room.clone(),
            no_room_writes: self.no_room_writes.clone(),
        }))
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        anyhow::ensure!(!take_one(&self.refusals), "refused piece {piece:?}");
        if take_one(&self.no_room) {
            return Err(no_room());
        }
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

impl Harness {
    /// A peer at `addr` with the pieces `pieces`, whose handshake names
    /// upload_only.
    async fn peer_taking_upload_only(&self, addr: &str, pieces: &[u32]) -> (PeerHandler, PeerRx) {
        let (peer, rx) = self.peer_with(addr, pieces).await;
        let mut names_it = ExtendedHandshake::new();
        names_it.m.upload_only = Some(3);
        (&peer).on_extended_handshake(&names_it).unwrap();
        (peer, rx)
    }
}

/// The messages queued for the peer, draining its queue.
fn messages_sent(rx: &mut PeerRx) -> Vec<Message<'static>> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|r| match r {
            WriterRequest::Message(msg) => Some(msg),
            _ => None,
        })
        .collect()
}

// libtorrent `torrent::handle_disk_error`: a write that finds no room puts
// the torrent in upload mode, where `torrent::set_upload_mode` cancels every
// request (`peer_connection::cancel_all_requests`) and tells the peers it
// only uploads (`send_upload_only`); `do_update_interest` is interested in
// no peer and `send_block_requests` asks for nothing, while a request is
// served as ever (`incoming_request` does not read upload mode). Once it
// lasted `optimistic_disk_retry`, `torrent::second_tick` leaves it, and the
// torrent asks again.
#[tokio::test]
async fn a_full_disk_puts_the_torrent_in_upload_mode_and_it_keeps_seeding() {
    let storage = RefusingStorageFactory::default();
    storage.no_room.store(1, Ordering::SeqCst);
    let add = AddTorrentOptions {
        storage_factory: Some(storage.clone_box()),
        ..Default::default()
    };
    // Three pieces, the last had.
    let h = Harness::build_added("full_disk", 3, 0x20, isolated_session_opts(false), add).await;
    tokio::time::pause();
    let (fetcher, _fetcher_rx) = h.peer_with("127.0.0.1:6881", &[0]).await;
    let (asked, mut asked_rx) = h.peer_taking_upload_only("127.0.0.2:6881", &[1]).await;
    let (leecher, mut leecher_rx) = h.peer_with("127.0.0.3:6881", &[]).await;
    let leecher = &leecher;

    // `asked` is asked for all of piece 1.
    let mut requester: Task<'_> = Box::pin(asked.request_chunks());
    asked.on_i_am_unchoked();
    assert_eq!(
        next_requests(&mut requester, &mut asked_rx, CHUNKS_PER_PIECE as usize).await,
        chunks(1, 0..CHUNKS_PER_PIECE)
    );

    // The fetcher sends piece 0, which the storage has no room for.
    h.take(&fetcher, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&fetcher, (0, chunk), false).await.unwrap();
    }
    let entered = tokio::time::Instant::now();
    assert!(h.upload_mode());
    assert!(!h.have(0));
    assert_eq!(h.queued_pieces(), vec![0, 1]);
    assert_eq!(h.inflight_count(&asked), 0);
    let mut told = messages_sent(&mut asked_rx);
    assert!(matches!(
        told.pop(),
        Some(Message::Extended(ExtendedMessage::UploadOnly(true)))
    ));
    let cancelled: BTreeSet<(u32, u32)> = told
        .into_iter()
        .map(|msg| match msg {
            Message::Cancel(r) => (r.index, r.begin),
            other => panic!("expected a cancel, got {other:?}"),
        })
        .collect();
    assert_eq!(cancelled, chunks(1, 0..CHUNKS_PER_PIECE));
    assert_eq!(asked.acquire_next_piece().unwrap().piece(), None);

    // The piece it has is served.
    leecher
        .on_received_message(Message::Interested)
        .await
        .unwrap();
    assert_eq!(next_sent(&mut leecher_rx).await, Sent::Unchoke);
    leecher
        .on_received_message(Message::Request(request(2, 0)))
        .await
        .unwrap();
    assert_eq!(next_sent(&mut leecher_rx).await, Sent::Block(2, 0));

    // The requester finds us interested in nothing; once upload mode ends,
    // the peers are told, and the requester is interested and asks again.
    assert!(matches!(
        next_message(&mut requester, &mut asked_rx).await,
        Message::NotInterested
    ));
    assert!(matches!(
        next_message(&mut requester, &mut asked_rx).await,
        Message::Extended(ExtendedMessage::UploadOnly(false))
    ));
    assert!(tokio::time::Instant::now().duration_since(entered) >= OPTIMISTIC_DISK_RETRY);
    assert!(!h.upload_mode());
    // Two at first: telling the peer we were not interested ended its slow
    // start, and no block came to pace it by
    // (`peer_connection::send_not_interested`).
    assert_eq!(
        next_requests(&mut requester, &mut asked_rx, 2).await,
        chunks(1, 0..2)
    );
}

// libtorrent `peer_connection::on_disk_write_complete`: a block whose write
// failed is no longer being written (`piece_picker::write_failed`), and one
// that found no room puts the torrent in upload mode
// (`torrent::handle_disk_error`); a later copy of the block is written.
#[tokio::test]
async fn a_block_write_that_finds_no_room_is_fetched_again_after_upload_mode() {
    let storage = RefusingStorageFactory::default();
    storage.no_room_writes.store(1, Ordering::SeqCst);
    let h = Harness::build_on(
        "full_disk_write",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    tokio::time::pause();
    let (peer, _rx) = h.peer("127.0.0.1:6881");

    h.take(&peer, 0);
    h.send(&peer, (0, 0), false).await.unwrap();
    assert!(h.upload_mode());
    assert_eq!(h.missing_chunks(0), vec![0, 1, 2, 3]);
    assert_eq!(h.queued_pieces(), vec![0, 1]);

    h.wait_for_upload_mode(false).await;
    h.take(&peer, 0);
    h.send(&peer, (0, 0), false).await.unwrap();
    assert_eq!(h.missing_chunks(0), vec![1, 2, 3]);
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

// A torrent added to take peers from its trackers alone, though its
// metainfo does not mark it private, neither advertises peer exchange nor
// takes peers from it, as a private torrent does not.
#[tokio::test]
async fn a_torrent_that_takes_peers_from_trackers_only_does_not_speak_pex() {
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        trackers_only: true,
        ..Default::default()
    };
    let h = Harness::build_added(
        "pex_trackers_only",
        PIECES,
        0x00,
        isolated_session_opts(false),
        add,
    )
    .await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let advertised = h.advertised(&peer);
    assert!(advertised.ut_pex.is_none());

    peer.send_pex(&pex(&["10.0.0.1:6881"], &[])).await.unwrap();
    assert!(!h.knows("10.0.0.1:6881"));
}

/// A handshake from a peer, with the DHT bit when `dht` holds.
fn handshake(dht: bool) -> Handshake {
    Handshake::new(Id20::default(), Id20::new([9; 20])).offering_dht(dht)
}

type Datagram = (Vec<u8>, SocketAddr);

/// A datagram socket the test holds the other end of: what the session
/// sends through it comes out of the test's receiver, with the address it
/// went to, and what the test sends is what the session receives.
struct HeldDatagrams {
    addr: SocketAddr,
    sent: tokio::sync::mpsc::UnboundedSender<Datagram>,
    arriving: parking_lot::Mutex<tokio::sync::mpsc::UnboundedReceiver<Datagram>>,
}

impl HeldDatagrams {
    fn send(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        self.sent.send((buf.to_vec(), target)).ok();
        Ok(buf.len())
    }
}

impl crate::network::DatagramSocket for HeldDatagrams {
    fn bind_addr(&self) -> SocketAddr {
        self.addr
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> crate::network::BoxFuture<'a, std::io::Result<(usize, SocketAddr)>> {
        Box::pin(std::future::poll_fn(move |cx| {
            match std::task::ready!(self.arriving.lock().poll_recv(cx)) {
                Some((datagram, from)) => {
                    buf[..datagram.len()].copy_from_slice(&datagram);
                    std::task::Poll::Ready(Ok((datagram.len(), from)))
                }
                // The test is done with it: nothing more arrives.
                None => std::task::Poll::Pending,
            }
        }))
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<usize>> {
        Box::pin(std::future::ready(self.send(buf, target)))
    }

    fn poll_send_to(
        &self,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(self.send(buf, target))
    }
}

/// The test's ends of a [`HeldDatagrams`].
struct HeldEnds {
    addr: SocketAddr,
    sent: tokio::sync::mpsc::UnboundedReceiver<Datagram>,
    arriving: tokio::sync::mpsc::UnboundedSender<Datagram>,
}

/// A socket factory whose every datagram socket is a [`HeldDatagrams`], at
/// an address of its own on 127.0.0.1, and which makes no connection.
#[derive(Default)]
struct HeldUdp {
    held: parking_lot::Mutex<Vec<HeldEnds>>,
}

impl HeldUdp {
    /// The test's ends of the socket bound at `addr`.
    fn take(&self, addr: SocketAddr) -> HeldEnds {
        let mut held = self.held.lock();
        let at = held.iter().position(|h| h.addr == addr).unwrap();
        held.swap_remove(at)
    }
}

impl crate::network::SocketFactory for HeldUdp {
    fn connect_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<(
            crate::network::BoxAsyncReadVectored,
            crate::network::BoxAsyncWrite,
        )>,
    > {
        Box::pin(async { Err(std::io::ErrorKind::Unsupported.into()) })
    }

    fn listen_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        Box::pin(async { Err(std::io::ErrorKind::Unsupported.into()) })
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>>
    {
        let (sent, sent_rx) = tokio::sync::mpsc::unbounded_channel();
        let (arriving_tx, arriving) = tokio::sync::mpsc::unbounded_channel();
        let mut held = self.held.lock();
        let addr = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 50000 + held.len() as u16));
        held.push(HeldEnds {
            addr,
            sent: sent_rx,
            arriving: arriving_tx,
        });
        let socket: Arc<dyn crate::network::DatagramSocket> = Arc::new(HeldDatagrams {
            addr,
            sent,
            arriving: parking_lot::Mutex::new(arriving),
        });
        Box::pin(std::future::ready(Ok(socket)))
    }
}

/// The value under `key` in the bencoded dict `v`, as bytes.
fn dict_bytes<'a>(
    v: &'a crate::bencode::BencodeValue<crate::bencode::ByteBufOwned>,
    key: &str,
) -> &'a [u8] {
    use crate::bencode::{BencodeValue, ByteBufOwned};
    match v {
        BencodeValue::Dict(d) => match &d[&ByteBufOwned::from(key.as_bytes())] {
            BencodeValue::Bytes(b) => b.as_ref(),
            other => panic!("{key} is not bytes: {other:?}"),
        },
        _ => panic!("not a dict: {v:?}"),
    }
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
// the routing table. The DHT runs over a socket the test holds, with no
// bootstrap host, so the ping is the one datagram it sends and the test
// writes the node's answer.
#[tokio::test]
async fn our_dht_port_goes_once_to_a_dht_peer_and_its_port_adds_its_node() {
    let udp = Arc::new(HeldUdp::default());
    let opts = SessionOptions {
        dht: Some(crate::DhtSessionConfig {
            bootstrap_addrs: Some(Vec::new()),
            ..Default::default()
        }),
        socket_factory: Some(udp.clone()),
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
    let mut node_side = udp.take(ours.listen_addr());

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

    // The node the peer names, on the peer's address.
    let node = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 6890));
    let their_id = Id20::new([0x33; 20]);
    (&no_bit)
        .on_received_message(Message::Port(node.port()))
        .await
        .unwrap();
    assert!(matches!(
        no_bit_rx.try_recv(),
        Ok(WriterRequest::Message(Message::Port(p))) if p == our_port
    ));
    (&no_bit)
        .on_received_message(Message::Port(node.port()))
        .await
        .unwrap();
    assert!(no_bit_rx.try_recv().is_err());

    let (ping, to) = node_side.sent.recv().await.unwrap();
    assert_eq!(to, node);
    let ping = crate::bencode::dyn_from_bytes::<crate::bencode::ByteBufOwned>(&ping).unwrap();
    assert_eq!(dict_bytes(&ping, "y"), b"q");
    assert_eq!(dict_bytes(&ping, "q"), b"ping");
    let tid = dict_bytes(&ping, "t");

    let entered = ours.with_routing_tables(|v4, _| v4.entered());
    let mut entry = std::pin::pin!(entered.notified());
    entry.as_mut().enable();
    let mut answer = b"d1:rd2:id20:".to_vec();
    answer.extend_from_slice(&their_id.0);
    answer.extend_from_slice(format!("e1:t{}:", tid.len()).as_bytes());
    answer.extend_from_slice(tid);
    answer.extend_from_slice(b"1:y1:re");
    node_side.arriving.send((answer, node)).unwrap();
    entry.await;
    assert!(ours.with_routing_tables(|v4, _| {
        v4.iter().any(|n| n.id() == their_id && n.addr() == node)
    }));
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
    h.peers.ban(banned.ip());
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
    let mut names_none = ExtendedHandshake::new();
    names_none.m.upload_only = None;
    (&deaf).on_extended_handshake(&names_none).unwrap();

    let says = |peer: &PeerHandler| {
        let mut hs = ExtendedHandshake::new();
        peer.update_my_extended_handshake(&mut hs).unwrap();
        hs.upload_only
    };
    assert_eq!(says(&takes), None);

    tokio::time::pause();
    let mut requester: Task<'_> = Box::pin(seed.task_peer_chunk_requester());
    seed.on_i_am_unchoked();
    let first = h
        .request_every_chunk(&seed, &mut requester, &mut seed_rx)
        .await;
    drop(requester);
    let rest = chunks(first, 1..CHUNKS_PER_PIECE)
        .into_iter()
        .chain(chunks(1 - first, 0..CHUNKS_PER_PIECE));
    for request in rest {
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

// libtorrent `peer_list::is_connect_candidate`: once we only upload, a peer
// whose connection showed it to be a seed is no connect candidate, and any
// other still is (`settings_pack::seeding_outgoing_connections`, on by
// default).
#[tokio::test]
async fn a_finished_torrent_still_dials_a_leecher_and_not_a_seed() {
    let h = Harness::seeding("finished_dials").await;
    let listens = |peer: &PeerHandler| {
        let mut hs = ExtendedHandshake::new();
        hs.p = Some(u32::from(peer.addr.port()));
        peer.on_extended_handshake(&hs).unwrap();
    };
    let (leecher, _leecher_rx) = h.new_peer("127.0.0.1:6881");
    leecher.on_bitfield(&bitfield_of(&[])).unwrap();
    listens(&leecher);
    let (seed, _seed_rx) = h.peer("127.0.0.2:6881");
    listens(&seed);
    leecher.on_peer_died(None).unwrap();
    seed.on_peer_died(None).unwrap();

    let due = tokio::time::Instant::now() + peer::MIN_RECONNECT_TIME;
    assert_eq!(
        h.peers.connect_next(due).map(|(addr, ..)| addr),
        Some(sock("127.0.0.1:6881"))
    );
    assert!(h.peers.connect_next(due).is_none());
}

// libtorrent `session_impl::set_ip_filter` and `update_privileged_ports`
// run `torrent::ip_filter_updated` and `privileged_port_updated`
// (`peer_list::apply_ip_filter`, `apply_port_filter`): an idle peer the new
// filter refuses leaves the list at once, a connected one has its connection
// closed and leaves once it has; from then on `torrent::add_peer` takes no
// peer the filter refuses or that is on a privileged port.
#[tokio::test]
async fn a_changed_ip_filter_takes_the_peers_it_refuses_off_the_list() {
    let h = Harness::new("refilter").await;
    let (connected, mut connected_rx) = h.peer("127.0.0.1:6881");
    for addr in ["203.0.113.1:6881", "203.0.113.2:80", "203.0.113.3:6881"] {
        h.add_peer(sock(addr), PeerSourceFlag::Tracker);
    }
    let listed = |addr: &str| h.peers.with_peer(sock(addr), |_| ()).is_some();

    h._session.set_ip_filter(crate::IpFilter {
        blocked: Arc::new(crate::IpRanges::new(
            [
                "127.0.0.1".parse().unwrap().."127.0.0.2".parse().unwrap(),
                "203.0.113.1".parse().unwrap().."203.0.113.2".parse().unwrap(),
            ],
            [],
        )),
        no_privileged_ports: true,
        trackers: true,
    });
    assert!(!listed("203.0.113.1:6881"));
    assert!(!listed("203.0.113.2:80"));
    assert!(listed("203.0.113.3:6881"));
    assert!(
        std::iter::from_fn(|| connected_rx.try_recv().ok())
            .any(|r| matches!(r, WriterRequest::Disconnect(Ok(()))))
    );
    assert!(listed("127.0.0.1:6881"));
    connected.on_peer_died(None).unwrap();
    assert!(!listed("127.0.0.1:6881"));

    for addr in ["203.0.113.1:6881", "203.0.113.4:443", "203.0.113.4:6881"] {
        h.add_peer(sock(addr), PeerSourceFlag::Tracker);
    }
    assert!(!listed("203.0.113.1:6881"));
    assert!(!listed("203.0.113.4:443"));
    assert!(listed("203.0.113.4:6881"));
}

// libtorrent `peer_list::compare_peer`: among candidates that failed as
// often, one on a local network is dialed first.
#[tokio::test]
async fn a_local_peer_is_dialed_before_a_global_one() {
    let h = Harness::new("local_first").await;
    h.peers
        .add_peer(sock("1.1.1.1:6881"), PeerSourceFlag::Tracker);
    h.peers
        .add_peer(sock("192.168.1.1:6881"), PeerSourceFlag::Tracker);
    let now = tokio::time::Instant::now();
    let dialed: Vec<_> =
        std::iter::from_fn(|| h.peers.connect_next(now).map(|(addr, ..)| addr)).collect();
    assert_eq!(dialed, [sock("192.168.1.1:6881"), sock("1.1.1.1:6881")]);
}

/// Every dial is reported to the test with when it began, and none is
/// answered; a datagram socket is bound on loopback, as for [`DialedTcp`].
struct UnansweredDials(tokio::sync::mpsc::UnboundedSender<tokio::time::Instant>);

impl crate::network::SocketFactory for UnansweredDials {
    fn connect_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<(
            crate::network::BoxAsyncReadVectored,
            crate::network::BoxAsyncWrite,
        )>,
    > {
        Box::pin(async move {
            let _ = self.0.send(tokio::time::Instant::now());
            std::future::pending().await
        })
    }

    fn listen_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        Box::pin(async { Err(std::io::ErrorKind::Unsupported.into()) })
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>>
    {
        Box::pin(async {
            let socket = tokio::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
            Ok(Arc::new(socket) as Arc<dyn crate::network::DatagramSocket>)
        })
    }
}

// libtorrent `peer_connection::second_tick`: a connect is given up after
// `peer_connect_timeout` and 3 s more for each failure in a row; the peer
// is dialed again (failcount + 1) reconnect times after that
// (`peer_list::find_connect_candidates`).
#[tokio::test(start_paused = true)]
async fn the_connect_timeout_grows_by_three_seconds_per_failure() {
    let (tx, mut dials) = unbounded_channel();
    let opts = SessionOptions {
        socket_factory: Some(Arc::new(UnansweredDials(tx))),
        ..isolated_session_opts(false)
    };
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        initial_peers: Some(vec![sock("127.0.0.1:6881")]),
        ..Default::default()
    };
    let _h = Harness::build_added("connect_timeout_grows", PIECES, 0x00, opts, add).await;
    let mut at = Vec::new();
    for _ in 0..3 {
        at.push(dials.recv().await.unwrap());
    }
    let timeout = crate::peer_connection::DEFAULT_CONNECT_TIMEOUT;
    assert_eq!(at[1] - at[0], timeout + peer::MIN_RECONNECT_TIME * 2);
    assert_eq!(
        at[2] - at[1],
        timeout + Duration::from_secs(3) + peer::MIN_RECONNECT_TIME * 3
    );
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

    h.peers
        .add_peer(sock("203.0.113.1:6881"), PeerSourceFlag::Tracker);
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
    h.peers
        .add_peer(sock("203.0.113.1:6881"), PeerSourceFlag::Tracker);

    h._session.turn_peers_over();
    assert!(
        std::iter::from_fn(|| rx.try_recv().ok())
            .any(|r| matches!(r, WriterRequest::Disconnect(Ok(()))))
    );
}

impl Harness {
    /// The place the connection at `addr`, whose writer queue is `tx`, holds
    /// under the torrent's cap and the session's, as its task would.
    fn hold(&self, addr: SocketAddr, tx: &PeerTx) -> Held {
        let permit = self.peer_semaphore.clone().try_acquire_owned().unwrap();
        let slot = self._session.connection_slots.try_open().unwrap();
        self.live.hold_place(addr, tx, permit, slot)
    }

    /// A connection from `addr` the listener took and checked, for this
    /// torrent, with its place under the session's cap; and our end of it.
    fn incoming(
        &self,
        addr: &str,
    ) -> (
        CheckedIncomingConnection,
        crate::connection_slots::ConnectionSlot,
        tokio::io::DuplexStream,
    ) {
        let (ours, theirs) = tokio::io::duplex(1 << 16);
        let (reader, writer) = crate::peer_info_reader::tests::halves(ours);
        let checked = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: sock(addr),
            reader,
            writer,
            read_buf: crate::read_buf::ReadBuf::new(),
            handshake: Handshake::new(self.shared.info_hash, Id20::new([7; 20])),
            encryption: crate::mse::Encryption::None,
            meter: Default::default(),
        };
        let slot = self._session.connection_slots.try_accept().unwrap();
        (checked, slot, theirs)
    }

    fn holds_place(&self, addr: &str) -> bool {
        self.places.lock().contains_key(&sock(addr))
    }
}

/// Whether the connection whose writer queue `rx` reads was asked to close.
fn asked_to_close(rx: &mut PeerRx) -> bool {
    std::iter::from_fn(|| rx.try_recv().ok())
        .any(|r| matches!(r, WriterRequest::Disconnect(Ok(()))))
}

// libtorrent `torrent::attach_peer`: at the torrent's cap, while more than
// one in ten of its most connections are still connecting, the connect begun
// longest ago gives way to an incoming connection.
#[tokio::test]
async fn an_incoming_connection_at_the_cap_replaces_the_oldest_connect_while_many_connect() {
    let h = turnover_harness("displace_connect", 10, 200).await;
    tokio::time::pause();
    let mut held = Vec::new();
    let mut connects = Vec::new();
    for addr in ["203.0.113.1:6881", "203.0.113.2:6881"] {
        h.peers.add_peer(sock(addr), PeerSourceFlag::Tracker);
        let (addr, rx, tx) = h.peers.connect_next(tokio::time::Instant::now()).unwrap();
        held.push(h.hold(addr, &tx));
        connects.push((addr, rx));
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    let mut live = Vec::new();
    for i in 1..=8 {
        let (peer, rx) = h.new_peer(&format!("198.51.100.{i}:6881"));
        held.push(h.hold(peer.addr, &peer.tx));
        live.push(rx);
    }

    let (checked, slot, _theirs) = h.incoming("192.0.2.1:6881");
    let added = h.live.add_incoming_peer(checked, slot).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));
    let [(oldest, oldest_rx), (newer, newer_rx)] = &mut connects[..] else {
        unreachable!()
    };
    assert_eq!(*oldest, sock("203.0.113.1:6881"));
    assert!(asked_to_close(oldest_rx));
    assert!(!asked_to_close(newer_rx));
    assert!(live.iter_mut().all(|rx| !asked_to_close(rx)));
    assert!(!h.holds_place("203.0.113.1:6881"));
    assert!(h.places.lock().contains_key(newer));
    assert!(h.holds_place("192.0.2.1:6881"));
    assert_eq!(h.connections(), (10, 10));
}

// libtorrent `torrent::attach_peer`, `find_lowest_ranking_peer`: at the
// torrent's cap, with few connecting, an incoming connection replaces the
// connection that ranks lowest when it ranks higher. Here a connection on
// parole ranks lowest; the newcomer, never seen, is on none.
#[tokio::test]
async fn an_incoming_connection_at_the_cap_replaces_a_connection_that_ranks_lower() {
    let h = turnover_harness("displace_lowest", 2, 200).await;
    tokio::time::pause();
    let (on_parole, mut on_parole_rx) = h.new_peer("203.0.113.1:6881");
    h.peers
        .with_peer_mut(on_parole.addr, "test", |p| p.piece_failed(false))
        .unwrap();
    let (kept, mut kept_rx) = h.new_peer("203.0.113.2:6881");
    let _held = [
        h.hold(on_parole.addr, &on_parole.tx),
        h.hold(kept.addr, &kept.tx),
    ];

    let (checked, slot, _theirs) = h.incoming("192.0.2.1:6881");
    let added = h.live.add_incoming_peer(checked, slot).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));
    assert!(asked_to_close(&mut on_parole_rx));
    assert!(!asked_to_close(&mut kept_rx));
    assert!(!h.holds_place("203.0.113.1:6881"));
    assert!(h.holds_place("192.0.2.1:6881"));
    assert_eq!(h.connections(), (2, 2));
}

// libtorrent `torrent::attach_peer`: an incoming connection that ranks no
// higher than the lowest-ranked connection of a torrent at its cap is
// refused, and every connection stays.
#[tokio::test]
async fn an_incoming_connection_at_the_cap_that_ranks_no_higher_is_refused() {
    let h = turnover_harness("displace_none", 2, 200).await;
    tokio::time::pause();
    let (a, mut a_rx) = h.new_peer("203.0.113.1:6881");
    let (b, mut b_rx) = h.new_peer("203.0.113.2:6881");
    let _held = [h.hold(a.addr, &a.tx), h.hold(b.addr, &b.tx)];

    let (checked, slot, _theirs) = h.incoming("192.0.2.1:6881");
    let added = h.live.add_incoming_peer(checked, slot).unwrap();
    assert!(matches!(
        added,
        AddIncomingPeerResult::ConcurrencyLimitReached
    ));
    assert!(!asked_to_close(&mut a_rx));
    assert!(!asked_to_close(&mut b_rx));
    assert!(!h.holds_place("192.0.2.1:6881"));
    assert_eq!(h.connections(), (2, 2));
}

// libtorrent `bt_peer_connection::init_bt_handshake` marks a connection
// encrypted whichever side opened it, and `ut_pex_plugin::tick` flags a peer
// by it (`supports_encryption()`): an incoming connection the listener took
// encrypted is advertised encrypted, a plaintext one is not.
#[tokio::test]
async fn an_incoming_encrypted_connection_is_advertised_encrypted_in_our_pex() {
    use crate::peer_protocol::extended::ut_pex::PEX_ENCRYPTION;
    use tokio::io::AsyncReadExt;
    let h = turnover_harness("incoming_pex_encryption", 10, 200).await;
    tokio::time::pause();
    let mut ends = Vec::new();
    for (addr, encrypted) in [("192.0.2.1:6881", true), ("192.0.2.2:6881", false)] {
        let (mut checked, slot, mut theirs) = h.incoming(addr);
        checked.encryption = if encrypted {
            crate::mse::Encryption::Rc4
        } else {
            crate::mse::Encryption::None
        };
        let added = h.live.add_incoming_peer(checked, slot).unwrap();
        assert!(matches!(added, AddIncomingPeerResult::Added));
        // Our handshake and the first byte after it, which is written once
        // the connection has told the torrent how the peer greeted.
        let mut written = [0; 69];
        theirs.read_exact(&mut written).await.unwrap();
        ends.push(theirs);
    }

    let flags = |addr| {
        h.peers
            .with_live(sock(addr), |live| live.advertised_pex_flags())
            .unwrap()
    };
    assert_eq!(flags("192.0.2.1:6881") & PEX_ENCRYPTION, PEX_ENCRYPTION);
    assert_eq!(flags("192.0.2.2:6881") & PEX_ENCRYPTION, 0);
}

/// Every dial reaches the test as [`Dialed`](crate::peer_info_reader::tests::Dialed)
/// does; a datagram socket, which only the idle UDP tracker client holds,
/// is bound on loopback.
struct DialedTcp(crate::peer_info_reader::tests::Dialed);

impl crate::network::SocketFactory for DialedTcp {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<(
            crate::network::BoxAsyncReadVectored,
            crate::network::BoxAsyncWrite,
        )>,
    > {
        self.0.connect_tcp(addr)
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        self.0.listen_tcp(addr)
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>>
    {
        Box::pin(async {
            let socket = tokio::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
            Ok(Arc::new(socket) as Arc<dyn crate::network::DatagramSocket>)
        })
    }
}

/// A torrent with no piece, of a session whose connections take
/// `peer_opts`, that dials one peer; the far end of that dial, read past
/// our handshake.
async fn dialing_with(
    name: &str,
    peer_opts: crate::PeerConnectionOptions,
) -> (Harness, tokio::io::DuplexStream) {
    use tokio::io::AsyncReadExt;
    let (tx, mut dialed) = unbounded_channel();
    let opts = SessionOptions {
        socket_factory: Some(Arc::new(DialedTcp(crate::peer_info_reader::tests::Dialed(
            tx,
        )))),
        connect: Some(crate::stream_connect::ConnectionOptions {
            enable_tcp: true,
            peer_opts: Some(peer_opts),
        }),
        ..isolated_session_opts(false)
    };
    let add = AddTorrentOptions {
        storage_factory: Some(MemoryStorageFactory::default().clone_box()),
        initial_peers: Some(vec!["127.0.0.1:6881".parse().unwrap()]),
        ..Default::default()
    };
    let h = Harness::build_added(name, PIECES, 0x00, opts, add).await;
    let (_, mut peer) = dialed.recv().await.unwrap();
    let mut ours = [0; 68];
    peer.read_exact(&mut ours).await.unwrap();
    (h, peer)
}

// libtorrent `peer_connection::second_tick`: a torrent's connection still
// in its handshake ends at the session's handshake timeout, whatever the
// peer timeout.
#[tokio::test(start_paused = true)]
async fn a_torrent_connection_ends_an_unanswered_handshake_at_the_session_timeout() {
    use tokio::io::AsyncReadExt;
    let handshake_timeout = Duration::from_secs(3);
    let (_h, mut peer) = dialing_with(
        "session_handshake_timeout",
        crate::PeerConnectionOptions {
            read_write_timeout: Some(Duration::from_secs(120)),
            handshake_timeout: Some(handshake_timeout),
            ..Default::default()
        },
    )
    .await;
    let start = tokio::time::Instant::now();
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    assert_eq!(start.elapsed(), handshake_timeout);
}

// libtorrent `peer_connection::keep_alive`: a torrent's connection sends a
// keepalive at the session's keepalive interval.
#[tokio::test(start_paused = true)]
async fn a_torrent_connection_sends_keepalives_at_the_session_interval() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let keep_alive = Duration::from_secs(7);
    let (h, mut peer) = dialing_with(
        "session_keep_alive",
        crate::PeerConnectionOptions {
            read_write_timeout: Some(Duration::from_secs(120)),
            keep_alive_interval: Some(keep_alive),
            ..Default::default()
        },
    )
    .await;
    let mut theirs = [0; 68];
    let len =
        Handshake::new(h.shared.info_hash, Id20::new([2; 20])).serialize_unchecked_len(&mut theirs);
    peer.write_all(&theirs[..len]).await.unwrap();
    // Whatever goes out first, the keepalive is the first empty frame.
    let start = tokio::time::Instant::now();
    loop {
        let frame = peer.read_u32().await.unwrap();
        if frame == 0 {
            break;
        }
        let mut body = vec![0; frame as usize];
        peer.read_exact(&mut body).await.unwrap();
    }
    assert_eq!(start.elapsed(), keep_alive);
}

/// The next thing `task` queues for the peer, polling it until it does.
async fn next_queued(task: &mut Task<'_>, rx: &mut PeerRx) -> WriterRequest {
    tokio::select! {
        biased;
        sent = rx.recv() => sent.expect("the peer's writer queue closed"),
        res = task.as_mut() => panic!("the peer task ended: {res:?}"),
    }
}

/// Whether `task` is waiting, having queued nothing more for the peer.
fn waiting(task: &mut Task<'_>, rx: &mut PeerRx) -> bool {
    futures::FutureExt::now_or_never(task.as_mut()).is_none() && rx.is_empty()
}

/// A bitfield of `pieces` alone.
fn bitfield_of(pieces: &[u32]) -> Vec<u8> {
    let mut bf = peer::empty_bitfield(PIECES);
    for p in pieces {
        bf.set(*p as usize, true);
    }
    bf.as_raw_slice().to_vec()
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
    assert_eq!(ours(&incoming), None);
    assert_eq!(ours(&outgoing), Some(51413));

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
        h.peers.pex_listed().into_keys().collect::<Vec<_>>(),
        [listen]
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

// wait_until_completed takes its wait before it checks: a finish rung
// between the check and the await wakes it, where a wait taken after the
// ring sleeps on (tokio `Notify::notify_waiters` wakes only the waits taken
// by then). The window lies inside one poll, so the ring is put there by
// hand on the torrent's own notify.
#[tokio::test(start_paused = true)]
async fn a_finish_between_the_check_and_the_wait_is_not_lost() {
    use futures::FutureExt;
    let h = Harness::new("finish-between").await;
    let mut taken = std::pin::pin!(h.finished_notify.notified());
    taken.as_mut().enable();
    assert!(!h.is_finished());
    h.finished_notify.notify_waiters();
    let after = h.finished_notify.notified();
    assert!(taken.now_or_never().is_some());
    assert!(after.now_or_never().is_none());
}

impl Harness {
    /// A hybrid torrent of `files` live, each file padded to whole pieces,
    /// none of its pieces had, and no peers.
    async fn hybrid(name: &str, files: Vec<(&str, Vec<u8>)>) -> Self {
        use crate::core::v2_test_torrent::TestTorrent as V2TestTorrent;

        let mut torrent = V2TestTorrent::new(files, u64::from(PIECE_LENGTH));
        torrent.hybrid = true;
        let out = TempDir::with_prefix(name).unwrap();
        let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
            .await
            .unwrap();
        let pieces = torrent.v1_payload().len() / PIECE_LENGTH as usize;
        let have = BF::from_boxed_slice(vec![0u8; pieces.div_ceil(8)].into_boxed_slice());
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent.torrent_bytes()),
                Some(AddTorrentOptions {
                    storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                    checkpoint: Some(crate::Checkpoint::pieces(have)),
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

    /// A web seed of the torrent, connected and live with every piece of
    /// the files it serves, the queue of what we send it, and its link,
    /// whose hold keeps the torrent from connecting to the seed itself.
    fn web_seed(&self) -> (PeerHandler, PeerRx, web_seed::SeedLink) {
        use crate::core::torrent_metainfo::{WebSeed, WebSeedKind};

        let seeds = &self.shared.web_seeds;
        assert!(seeds.add(WebSeed {
            url: "http://192.0.2.1/s/".into(),
            kind: WebSeedKind::Url,
        }));
        // The torrent's metainfo names no seed: this one is the first.
        let link = seeds.link(0).unwrap();
        let addr = link.handle();
        let (rx, tx) = self.peers.admit_web_seed(addr).unwrap();
        let counters = self
            .peers
            .with_peer(addr, |p| p.stats.counters.clone())
            .unwrap();
        let handler = PeerHandler {
            addr,
            incoming: false,
            on_bitfield_notify: Default::default(),
            speaks_v2: AtomicBool::new(false),
            sent_dht_port: AtomicBool::new(false),
            flow_control: Mutex::new(PeerFlowControl::new(self.live.max_request_window())),
            state: self.live.clone(),
            tx,
            counters,
            first_message_received: AtomicBool::new(true),
            cancel_token: self.live.cancellation_token.child_token(),
            client_name_and_version: String::new(),
        };
        (&handler)
            .on_handshake(handshake(false), ConnectionKind::Tcp, false)
            .unwrap();
        let serves = self.web_layout.bitfield(&Default::default());
        handler.on_bitfield(serves.as_raw_slice()).unwrap();
        (handler, rx, link)
    }

    /// The pieces the peer has, one bool each.
    fn has(&self, peer: &PeerHandler) -> Vec<bool> {
        let total = self.lengths.total_pieces() as usize;
        self.peers
            .with_live(peer.addr, |live| {
                live.bitfield[..total].iter().by_vals().collect()
            })
            .unwrap()
    }
}

// libtorrent web_peer_connection::received_invalid_data: a web seed that
// sent the whole of a piece lying in one file holds another copy of that
// file, so it no longer has the file's pieces, and is not banned while it
// has others (torrent::penalize_peers bans a known bad peer only when its
// connection allows it). A pad file is no part of what the seed serves.
#[tokio::test]
async fn a_web_seed_that_fails_a_piece_loses_its_file_and_serves_the_rest() {
    use crate::core::v2_test_torrent::payload;

    // "a" fills piece 0 and starts piece 1, which a pad file ends; "b" is
    // piece 2.
    let h = Harness::hybrid(
        "web_seed_loses_a_file",
        vec![
            ("a", payload(PIECE_LENGTH as usize + 1000, 1)),
            ("b", payload(PIECE_LENGTH as usize, 2)),
        ],
    )
    .await;
    tokio::time::pause();
    let (seed, mut rx, _link) = h.web_seed();
    assert_eq!(h.has(&seed), [true, true, true]);

    h.take(&seed, 1);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&seed, (1, chunk), chunk == 0).await.unwrap();
    }

    assert_eq!(h.missing_chunks(1), vec![0, 1, 2, 3]);
    assert_eq!(h.trust(&seed), (-2, 1, true));
    assert!(!h.banned(&seed) && !told_to_disconnect(&mut rx));
    assert_eq!(h.has(&seed), [false, false, true]);
    assert_eq!(h.shared.web_seeds.list().len(), 1);

    // It goes on serving "b", and is asked for nothing of "a": what is
    // left to take of it is piece 2, unless taking piece 1 took it first.
    let left: Vec<u32> = std::iter::from_fn(|| seed.acquire_next_piece().unwrap().piece())
        .map(|p| p.get())
        .collect();
    assert!(left.iter().all(|&p| p == 2), "{left:?}");
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&seed, (2, chunk), false).await.unwrap();
    }
    assert!(h.missing_chunks(2).is_empty());
    assert_eq!(h.trust(&seed), (-1, 1, false));
}

// libtorrent web_peer_connection::received_invalid_data: the seed of a
// single-file torrent that sends a bad piece has nothing left to serve,
// and is banned as any peer that sent a whole bad piece is.
#[tokio::test]
async fn the_web_seed_of_a_single_file_torrent_that_fails_a_piece_is_banned() {
    let h = Harness::new("web_seed_single_file_banned").await;
    tokio::time::pause();
    let (seed, mut rx, link) = h.web_seed();
    assert_eq!(h.has(&seed), [true, true]);

    h.take(&seed, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&seed, (0, chunk), chunk == 2).await.unwrap();
    }

    assert_eq!(h.missing_chunks(0), vec![0, 1, 2, 3]);
    assert!(h.banned(&seed));
    assert!(told_to_disconnect(&mut rx));
    drop(link);
    let due = h.shared.web_seeds.due(
        tokio::time::Instant::now(),
        |handle| h.peers.is_banned(handle.ip()),
        |lacks| h.web_layout.bitfield(lacks).any(),
    );
    assert!(due.is_empty());
}

// libtorrent `torrent::write_resume_data` records the finished blocks of each
// downloading piece (`unfinished_pieces`), and `torrent::read_resume_data`
// marks them finished again (`piece_picker::mark_as_finished`): a restarted
// torrent asks for none of them, and the piece's hash is checked once the
// rest arrive.
#[tokio::test]
async fn a_checkpoint_keeps_the_written_blocks_of_a_piece_across_a_restart() {
    let storage = MemoryStorageFactory::default();
    let add = || AddTorrentOptions {
        storage_factory: Some(storage.clone_box()),
        ..Default::default()
    };
    // Piece 1 had: piece 0 is the one fetched.
    let h = Harness::build_added(
        "checkpoint_keeps_written_blocks",
        PIECES,
        0x40,
        isolated_session_opts(false),
        add(),
    )
    .await;
    tokio::time::pause();
    {
        let (peer, mut rx) = h.peer("127.0.0.1:6881");
        let mut requester: Task<'_> = Box::pin(peer.task_peer_chunk_requester());
        peer.on_i_am_unchoked();
        assert_eq!(
            next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await,
            chunks(0, 0..CHUNKS_PER_PIECE)
        );
        h.deliver(&peer, (0, 0)).await;
        h.deliver(&peer, (0, CHUNK_SIZE)).await;
    }

    let checkpoint = h.handle().checkpoint().unwrap();
    assert_eq!(checkpoint.have.as_raw_slice(), &[0x40]);
    let blocks = checkpoint.blocks.as_ref().unwrap();
    assert_eq!(
        blocks.iter_ones().collect::<Vec<_>>(),
        [0, 1, 4, 5, 6, 7],
        "the two blocks written of piece 0, and every block of piece 1"
    );
    assert_eq!(storage.log.written_pieces(), [0, 0]);

    let h = h.restarted(checkpoint, add()).await;
    assert_eq!(h.missing_chunks(0), [2, 3]);
    let (peer, mut rx) = h.peer("127.0.0.2:6881");
    let mut requester: Task<'_> = Box::pin(peer.task_peer_chunk_requester());
    peer.on_i_am_unchoked();
    assert_eq!(
        next_requests(&mut requester, &mut rx, 2).await,
        chunks(0, 2..CHUNKS_PER_PIECE)
    );
    assert_eq!(h.inflight_count(&peer), 2);
    h.deliver(&peer, (0, 2 * CHUNK_SIZE)).await;
    h.deliver(&peer, (0, 3 * CHUNK_SIZE)).await;

    assert!(
        h.lock_read("test")
            .get_chunks()
            .unwrap()
            .is_piece_have(h.piece(0))
    );
    assert_eq!(storage.log.completed_pieces(), [0]);
    assert_eq!(
        storage.log.written_pieces(),
        [0, 0, 0, 0],
        "no block was fetched twice"
    );
}

// libtorrent `torrent::write_resume_data` saves the peers and the bans, and
// `torrent::read_resume_data` takes them back: each peer through
// `torrent::add_peer` as `peer_info::resume_data`, each ban through
// `peer_list::ban_peer`. A restarted torrent gets its saved peers as its
// initial peers and its saved bans as its banned addresses.
#[tokio::test]
async fn saved_peers_and_bans_come_back_as_initial_peers_and_bans() {
    use futures::StreamExt;

    let h = Harness::seeding("saved_peers_come_back").await;
    // Connected to us over uTP: we can connect back to where it is.
    let connectable: SocketAddr = "127.0.0.3:6881".parse().unwrap();
    let (tx, _rx) = unbounded_channel();
    h.peers
        .add_incoming(connectable, Id20::default(), tx, ConnectionKind::Utp, 6881)
        .unwrap();
    let banned: SocketAddr = "127.0.0.4:6881".parse().unwrap();
    h.peers.ban(banned.ip());
    let saved = h.handle().resume_peers().unwrap();
    assert_eq!(
        saved,
        crate::ResumePeers {
            peers: vec![connectable],
            banned: vec![banned.ip()],
        }
    );

    // Restarted paused, so no peer is connected to.
    let session = h._session.clone();
    let bytes = h._torrent.as_ref().unwrap().torrent.clone();
    session
        .delete(crate::api::TorrentIdOrHash::Id(h.shared.id), false)
        .await
        .unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(bytes),
            Some(AddTorrentOptions {
                paused: true,
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                checkpoint: Some(crate::Checkpoint::pieces(BF::from_boxed_slice(
                    vec![0xc0].into_boxed_slice(),
                ))),
                initial_peers: Some(saved.peers.clone()),
                banned_peers: saved.banned.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    let named: Vec<_> = session
        .make_peer_rx_managed_torrent(&handle, false)
        .unwrap()
        .take(saved.peers.len())
        .collect()
        .await;
    assert_eq!(named, [(connectable, PeerSourceFlag::ResumeData)]);
    // Saved again before it runs, the torrent keeps both.
    assert_eq!(handle.resume_peers().unwrap(), saved);
    let paused_list = handle.with_state(|s| match s {
        crate::ManagedTorrentState::Paused(p) => p.peers.resume_peers(),
        _ => panic!("the restarted torrent is not paused"),
    });
    assert_eq!(paused_list, saved);
}

// libtorrent keeps a torrent's peer list in the torrent itself through every
// state, so `torrent::write_resume_data` reads it whenever it is asked. The
// list stays with a torrent being checked and moves on only with the change
// of state: a save between the end of the check and that change reads the
// list, not an empty one.
#[tokio::test]
async fn a_checked_torrent_keeps_its_peer_list_until_its_state_changes() {
    let h = Harness::seeding("peers_through_the_check").await;
    let session = h._session.clone();
    let bytes = h._torrent.as_ref().unwrap().torrent.clone();
    session
        .delete(crate::api::TorrentIdOrHash::Id(h.shared.id), false)
        .await
        .unwrap();
    let saved = crate::ResumePeers {
        peers: vec!["127.0.0.3:6881".parse().unwrap()],
        banned: vec!["127.0.0.4".parse().unwrap()],
    };
    // Every check waits here, so the session's own check of the torrent
    // does not start; the test runs the check itself.
    let semaphore = session.concurrent_initialize_semaphore.clone();
    let permits = u32::try_from(semaphore.available_permits()).unwrap();
    let _held = semaphore.acquire_many_owned(permits).await.unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(bytes),
            Some(AddTorrentOptions {
                paused: true,
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                checkpoint: Some(crate::Checkpoint::pieces(BF::from_boxed_slice(
                    vec![0xc0].into_boxed_slice(),
                ))),
                initial_peers: Some(saved.peers.clone()),
                banned_peers: saved.banned.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    let init = handle.with_state(|s| match s {
        crate::ManagedTorrentState::Initializing(init) => init.clone(),
        _ => panic!("the added torrent is not being checked"),
    });

    let paused = init.check().await.unwrap();
    assert_eq!(handle.resume_peers().unwrap(), saved);
    drop(paused);
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
