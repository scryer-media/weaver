// SPDX-License-Identifier: GPL-3.0-or-later

//! Room for a piece is taken by its first block, not by its grant: the
//! owner's first block takes the whole piece's, any other peer's one
//! block's, and a connection the storage has no room for stops reading and
//! gives back the pieces it started.

use super::*;
use crate::session_stats::swarm::RequestEvent;
use tokio::time::Instant;

/// What room a piece holds in [`BudgetStorage`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Room {
    Whole,
    Block,
}

#[derive(Default)]
struct Budget {
    // The most pieces holding whole-piece room at once.
    cap: usize,
    pieces: std::collections::HashMap<u32, Room>,
    peak: usize,
    // Whole-piece room asked for, granted or not.
    asked: usize,
    room: Option<Arc<Notify>>,
}

impl Budget {
    fn whole(&self) -> usize {
        self.pieces.values().filter(|r| **r == Room::Whole).count()
    }

    fn ring(&self) {
        if let Some(room) = &self.room {
            room.notify_waiters();
        }
    }
}

/// A memory storage with room for `cap` whole pieces, and room for any
/// number of pieces written block by block: a piece given back gives its
/// whole-piece room back, as one written out to its files does.
#[derive(Clone)]
struct BudgetStorageFactory {
    inner: MemoryStorageFactory,
    budget: Arc<Mutex<Budget>>,
}

impl BudgetStorageFactory {
    fn with_room_for(cap: usize) -> Self {
        Self {
            inner: MemoryStorageFactory::default(),
            budget: Arc::new(Mutex::new(Budget {
                cap,
                ..Budget::default()
            })),
        }
    }

    fn room_of(&self, piece: u32) -> Option<Room> {
        self.budget.lock().pieces.get(&piece).copied()
    }
}

struct BudgetStorage {
    inner: Box<dyn TorrentStorage>,
    budget: Arc<Mutex<Budget>>,
}

impl StorageFactory for BudgetStorageFactory {
    type Storage = BudgetStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<BudgetStorage> {
        Ok(BudgetStorage {
            inner: Box::new(self.inner.create(shared, metadata)?),
            budget: self.budget.clone(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

impl TorrentStorage for BudgetStorage {
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
        let piece = u32::try_from(offset / u64::from(PIECE_LENGTH))?;
        anyhow::ensure!(
            buf.is_empty() || self.budget.lock().pieces.contains_key(&piece),
            "piece {piece} is written while the session holds no room for it"
        );
        self.inner.pwrite_all(file_id, offset, buf)
    }

    fn writes_to_memory(&self) -> bool {
        true
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
        Ok(Box::new(BudgetStorage {
            inner: self.inner.take()?,
            budget: self.budget.clone(),
        }))
    }

    fn reserve_piece(&self, piece: ValidPieceIndex, room: &Arc<Notify>) -> bool {
        let mut budget = self.budget.lock();
        budget.room = Some(Arc::clone(room));
        if budget.pieces.contains_key(&piece.get()) {
            return true;
        }
        budget.asked += 1;
        if budget.whole() == budget.cap {
            return false;
        }
        budget.pieces.insert(piece.get(), Room::Whole);
        budget.peak = budget.peak.max(budget.whole());
        true
    }

    fn reserve_unowned_piece(&self, piece: ValidPieceIndex, room: &Arc<Notify>) -> bool {
        let mut budget = self.budget.lock();
        budget.room = Some(Arc::clone(room));
        budget.pieces.entry(piece.get()).or_insert(Room::Block);
        true
    }

    fn on_piece_requeued(&self, piece: ValidPieceIndex) {
        let mut budget = self.budget.lock();
        if let Some(room) = budget.pieces.get_mut(&piece.get())
            && *room == Room::Whole
        {
            *room = Room::Block;
            budget.ring();
        }
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        {
            let mut budget = self.budget.lock();
            budget.pieces.remove(&piece.get());
            budget.ring();
        }
        self.inner.on_piece_completed(piece)
    }

    fn on_piece_hash_failed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        {
            let mut budget = self.budget.lock();
            budget.pieces.remove(&piece.get());
            budget.ring();
        }
        self.inner.on_piece_hash_failed(piece)
    }
}

async fn budgeted(name: &str, cap: usize) -> (Harness, BudgetStorageFactory) {
    let storage = BudgetStorageFactory::with_room_for(cap);
    let h = Harness::build_on(
        name,
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    tokio::time::pause();
    (h, storage)
}

impl Harness {
    fn room_waits(&self) -> u64 {
        self.session_stats.swarm.snapshot().request_events[RequestEvent::RoomWait as usize]
    }

    fn redundant(&self) -> u64 {
        self.stats.redundant_bytes.load(Ordering::Relaxed)
    }

    /// Whether `peer` owns piece `index`.
    fn holds(&self, peer: &PeerHandler, index: u32) -> bool {
        self.lock_read("test")
            .get_pieces()
            .unwrap()
            .is_owned_by(self.piece(index), peer.addr)
    }

    /// The unchoked peer is handed the next free piece.
    fn grab(&self, peer: &PeerHandler) -> u32 {
        peer.on_i_am_unchoked();
        peer.acquire_next_piece()
            .unwrap()
            .piece()
            .expect("no piece is free")
            .get()
    }

    /// The peer is asked for chunk `chunk` of piece `index`.
    fn ask(&self, peer: &PeerHandler, index: u32, chunk: u32) {
        let info = self.chunk(index, chunk);
        self.peers
            .with_live_mut(peer.addr, "test", |live| live.add_inflight_request(info))
            .unwrap();
    }

    /// The peer sends chunk `chunk` of piece `index`, asked for already.
    async fn answer(&self, peer: &PeerHandler, index: u32, chunk: u32) {
        let start = (index * PIECE_LENGTH + chunk * CHUNK_SIZE) as usize;
        peer.on_received_piece(Piece::from_data(
            index,
            chunk * CHUNK_SIZE,
            &self.content[start..start + CHUNK_SIZE as usize],
        ))
        .await
        .unwrap();
    }

    /// Resolves once no piece waits for its check or for its write.
    async fn drained(&self) {
        let spawner = &self.live.shared.spawner;
        spawner.hash().idle().await;
        spawner.disk().idle().await;
    }

    fn deadline(&self, peer: &PeerHandler) -> Option<Instant> {
        self.peers
            .with_live(peer.addr, |live| live.request_deadline())
            .unwrap()
    }
}

// A grant takes no room: a peer handed every piece holds none until a
// block arrives, and then holds the room of that block's piece alone.
#[tokio::test]
async fn counted_room_follows_arriving_pieces_not_grants() {
    let (h, storage) = budgeted("room_follows_arrivals", 1).await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    h.take_every(&peer);
    assert_eq!(storage.budget.lock().asked, 0);
    assert!(storage.budget.lock().pieces.is_empty());

    h.send(&peer, (1, 0), false).await.unwrap();
    assert_eq!(storage.room_of(1), Some(Room::Whole));
    assert_eq!(storage.budget.lock().pieces.len(), 1);
    // Its later blocks ask for nothing.
    h.send(&peer, (1, 1), false).await.unwrap();
    assert_eq!(storage.budget.lock().asked, 1);
}

// An owner that sends each piece's blocks in order finishes every piece
// with room for one, never waiting.
#[tokio::test]
async fn an_in_order_owner_completes_under_room_for_one_piece() {
    let (h, storage) = budgeted("room_in_order", 1).await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    h.take_every(&peer);
    for piece in 0..PIECES {
        for chunk in 0..CHUNKS_PER_PIECE {
            h.send(&peer, (piece, chunk), false).await.unwrap();
        }
        h.drained().await;
    }
    assert!((0..PIECES).all(|p| h.have(p)));
    assert_eq!(storage.budget.lock().peak, 1);
    assert_eq!(h.room_waits(), 0);
    assert_eq!(h.redundant(), 0);
}

// The jam: the owner of P and Q sends P's first block, which takes the one
// room, then Q's. It stops for room and gives P back, which frees P's
// room: Q's block lands, its requests for P still stand, and P's blocks it
// still sends are written block by block. Both pieces complete.
#[tokio::test]
async fn an_out_of_order_owner_gives_back_its_started_piece_and_goes_on() {
    let (h, storage) = budgeted("room_jam", 1).await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    h.take_every(&peer);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.ask(&peer, 0, chunk);
        h.ask(&peer, 1, chunk);
    }
    h.answer(&peer, 0, 0).await;
    assert_eq!(storage.room_of(0), Some(Room::Whole));

    h.answer(&peer, 1, 0).await;
    assert_eq!(h.room_waits(), 1);
    assert!(!h.holds(&peer, 0), "the started piece went back");
    assert!(h.holds(&peer, 1));
    assert_eq!(storage.room_of(0), Some(Room::Block));
    assert_eq!(storage.room_of(1), Some(Room::Whole));
    // Its requests for P stand.
    assert_eq!(h.inflight_count(&peer), 6);
    // Reading went on, and so does the clock on what it still owes.
    assert!(h.deadline(&peer).is_some());

    for chunk in 1..CHUNKS_PER_PIECE {
        h.answer(&peer, 0, chunk).await;
        h.answer(&peer, 1, chunk).await;
    }
    h.drained().await;
    assert!(h.have(0) && h.have(1));
    assert_eq!(storage.budget.lock().peak, 1);
    assert_eq!(h.room_waits(), 1);
    assert_eq!(h.redundant(), 0);
}

// An owner leaves mid-piece: the piece goes back and gives back its whole
// room, which the next piece's first block takes.
#[tokio::test]
async fn an_owner_that_leaves_mid_piece_frees_its_room() {
    let (h, storage) = budgeted("room_owner_leaves", 1).await;
    let (gone, _gone_rx) = h.peer("127.0.0.1:6881");
    let (next, _next_rx) = h.peer("127.0.0.2:6881");
    let left = h.grab(&gone);
    let other = h.grab(&next);
    h.send(&gone, (left, 0), false).await.unwrap();
    assert_eq!(storage.room_of(left), Some(Room::Whole));
    gone.on_i_am_choked().unwrap();
    assert_eq!(storage.room_of(left), Some(Room::Block));

    h.send(&next, (other, 0), false).await.unwrap();
    assert_eq!(storage.room_of(other), Some(Room::Whole));
    assert_eq!(h.room_waits(), 0);

    // The piece given back is finished by another peer, block by block.
    assert_eq!(h.grab(&next), left);
    for chunk in 1..CHUNKS_PER_PIECE {
        h.send(&next, (left, chunk), false).await.unwrap();
        h.send(&next, (other, chunk), false).await.unwrap();
    }
    h.drained().await;
    assert!(h.have(left) && h.have(other));
    assert_eq!(h.room_waits(), 0);
}

// A block of a piece no peer owns takes one block's room, not the piece's.
#[tokio::test]
async fn an_unowned_pieces_first_block_takes_block_room() {
    let (h, storage) = budgeted("room_unowned", 1).await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    h.send(&peer, (1, 1), false).await.unwrap();
    assert_eq!(storage.room_of(1), Some(Room::Block));
    assert_eq!(storage.budget.lock().asked, 0);
    assert_eq!(h.missing_chunks(1), vec![0, 2, 3]);
}

// A piece given back is asked of its next owner too, while the peer that
// gave it back still owes its blocks: each block counts once, and the copy
// that comes second counts as redundant, its bytes and no more.
#[tokio::test]
async fn a_given_back_block_that_arrives_twice_counts_once_redundant() {
    let (h, storage) = budgeted("room_twice", 1).await;
    let (slow, _slow_rx) = h.peer("127.0.0.1:6881");
    let (fast, _fast_rx) = h.peer("127.0.0.2:6881");
    h.take_every(&slow);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.ask(&slow, 0, chunk);
    }
    h.ask(&slow, 1, 0);
    h.answer(&slow, 0, 0).await;
    h.answer(&slow, 1, 0).await;
    assert!(!h.holds(&slow, 0));
    assert_eq!(storage.room_of(0), Some(Room::Block));

    // The next owner fetches the rest of P first.
    assert_eq!(h.grab(&fast), 0);
    for chunk in 1..CHUNKS_PER_PIECE {
        h.send(&fast, (0, chunk), false).await.unwrap();
    }
    h.drained().await;
    assert!(h.have(0));
    assert_eq!(h.redundant(), 0);
    let fetched = |peer: &PeerHandler| peer.counters.fetched_bytes.load(Ordering::Relaxed);
    let (slow_before, fast_before) = (fetched(&slow), fetched(&fast));

    // Then the blocks the first still owed arrive.
    for chunk in 1..CHUNKS_PER_PIECE {
        h.answer(&slow, 0, chunk).await;
    }
    assert_eq!(h.redundant(), u64::from(3 * CHUNK_SIZE));
    assert_eq!(fetched(&slow) - slow_before, u64::from(3 * CHUNK_SIZE));
    assert_eq!(fetched(&fast), fast_before);
    assert_eq!(h.inflight_count(&slow), 0);
}

// A connection stopped for room is not held to its timeouts: across an
// hour on the paused clock it has no request deadline, and is neither
// snubbed nor dropped. It goes on once the room it waits for comes back.
#[tokio::test]
async fn a_peer_stopped_for_room_is_not_timed_out_across_a_long_wait() {
    let (h, storage) = budgeted("room_long_wait", 1).await;
    let (waiter, _waiter_rx) = h.peer("127.0.0.1:6881");
    let (holder, _holder_rx) = h.peer("127.0.0.2:6881");
    let wanted = h.grab(&waiter);
    let held = h.grab(&holder);
    h.send(&holder, (held, 0), false).await.unwrap();
    h.ask(&waiter, wanted, 0);
    h.ask(&waiter, wanted, 1);

    let mut stalled = std::pin::pin!(h.answer(&waiter, wanted, 0));
    tokio::select! {
        biased;
        () = &mut stalled => panic!("the block landed without room"),
        () = tokio::task::yield_now() => {}
    }
    assert_eq!(h.room_waits(), 1);
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert_eq!(h.deadline(&waiter), None);
    let snubbed =
        h.per_peer_stats_snapshot(Default::default()).peers[&waiter.addr.to_string()].snubbed;
    assert!(!snubbed);
    // The block it holds was answered; the one it still owes stands.
    assert_eq!(h.inflight_count(&waiter), 1);

    // The holder finishes its piece, and the waiter's block lands.
    for chunk in 1..CHUNKS_PER_PIECE {
        h.send(&holder, (held, chunk), false).await.unwrap();
    }
    stalled.await;
    assert_eq!(storage.room_of(wanted), Some(Room::Whole));
    assert!(h.deadline(&waiter).is_some_and(|at| at > Instant::now()));
}
