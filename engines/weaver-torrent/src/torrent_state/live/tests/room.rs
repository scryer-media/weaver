// SPDX-License-Identifier: GPL-3.0-or-later

//! Room for a piece is taken by its first block, not by its grant: the
//! owner's first block takes the whole piece's; a piece any other peer
//! starts, or one given back, holds none, each of its blocks holding one
//! block's for its write alone; and a connection the storage has no room for
//! stops reading and gives back the pieces it started.

use super::*;
use crate::session_stats::swarm::{PieceCheck, RequestEvent};
use tokio::time::Instant;

/// What room a piece holds in [`BudgetStorage`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Room {
    Whole,
    Block,
}

#[derive(Default)]
struct Budget {
    // The most pieces holding whole-piece room at once: the budget is this
    // many pieces' bytes, which whole pieces and blocks being written share.
    cap: usize,
    pieces: std::collections::HashMap<u32, Room>,
    peak: usize,
    // The bytes the blocks being written hold, and block room refused.
    writing: u64,
    refused_blocks: usize,
    // Rung, with a permit kept, at each refusal of room.
    refused: Arc<Notify>,
    // Whole-piece room asked for, granted or not.
    asked: usize,
    room: Option<Arc<Notify>>,
    // A piece whose blocks fail to reach its files once it is given back,
    // and whether they did.
    lose: Option<u32>,
    lost: bool,
    // Pieces given back whose room is not back yet: the storage gives it
    // back aside, later, as a real one does once their blocks are written.
    defer_spills: bool,
    deferred: Vec<u32>,
}

impl Budget {
    fn whole(&self) -> usize {
        self.pieces.values().filter(|r| **r == Room::Whole).count()
    }

    fn used(&self) -> u64 {
        self.whole() as u64 * u64::from(PIECE_LENGTH) + self.writing
    }

    fn fits(&self, bytes: u64) -> bool {
        self.used() + bytes <= self.cap as u64 * u64::from(PIECE_LENGTH)
    }

    fn ring(&self) {
        if let Some(room) = &self.room {
            room.notify_waiters();
        }
    }

    // A piece given back gives back its whole-piece room.
    fn spill(&mut self, piece: u32) {
        if let Some(room) = self.pieces.get_mut(&piece)
            && *room == Room::Whole
        {
            *room = Room::Block;
            if self.lose == Some(piece) {
                self.lost = true;
            }
            self.ring();
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

    /// The bytes held now, by whole pieces and blocks being written.
    fn used(&self) -> u64 {
        self.budget.lock().used()
    }

    /// The pieces given back meanwhile give their whole-piece room back,
    /// as the storage's work aside does once their blocks are written.
    fn spill_deferred(&self) {
        let mut budget = self.budget.lock();
        for piece in std::mem::take(&mut budget.deferred) {
            budget.spill(piece);
        }
    }

    /// Resolves at the next refusal of room, or at once for one since the
    /// last.
    async fn refused(&self) {
        let refused = self.budget.lock().refused.clone();
        refused.notified().await;
    }
}

/// Block room given back as its write returns.
struct BlockWrite(Arc<Mutex<Budget>>);

impl crate::storage::GivesRoomBack for BlockWrite {
    fn give_back(&self, bytes: u64) {
        let mut budget = self.0.lock();
        budget.writing -= bytes;
        budget.ring();
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
        let room = self.budget.lock().pieces.get(&piece).copied();
        anyhow::ensure!(
            buf.is_empty() || room.is_some(),
            "piece {piece} is written while the session holds no room for it"
        );
        // A piece written block by block goes to its files, as a disk
        // write does: never from a uTP thread.
        if room == Some(Room::Block) {
            crate::spawn_utils::no_block_on_utp_thread();
        }
        self.inner.pwrite_all(file_id, offset, buf)
    }

    fn writes_to_memory(&self) -> bool {
        true
    }

    // A piece holding block room is written block by block, off the
    // connection's task.
    fn writes_piece_to_memory(&self, piece: ValidPieceIndex) -> bool {
        self.budget.lock().pieces.get(&piece.get()) != Some(&Room::Block)
    }

    fn pwrite_held(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [std::io::IoSlice<'_>; 2],
    ) -> anyhow::Result<bool> {
        let piece = u32::try_from(offset / u64::from(PIECE_LENGTH))?;
        if self.budget.lock().pieces.get(&piece) == Some(&Room::Block) {
            return Ok(false);
        }
        self.pwrite_all_vectored(file_id, offset, bufs)?;
        Ok(true)
    }

    fn hold_block(&self, bytes: u64, room: &Arc<Notify>) -> crate::storage::BlockRoom {
        let mut budget = self.budget.lock();
        budget.room = Some(Arc::clone(room));
        if !budget.fits(bytes) {
            budget.refused_blocks += 1;
            budget.refused.notify_one();
            return crate::storage::BlockRoom::Refused;
        }
        budget.writing += bytes;
        crate::storage::BlockRoom::Held(crate::storage::HeldRoom::new(
            bytes,
            Arc::new(BlockWrite(Arc::clone(&self.budget))),
        ))
    }

    fn room_for(&self, bytes: u64, room: &Arc<Notify>) -> bool {
        let mut budget = self.budget.lock();
        budget.room = Some(Arc::clone(room));
        budget.fits(bytes)
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
        if !budget.fits(u64::from(PIECE_LENGTH)) {
            budget.refused.notify_one();
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
        if budget.defer_spills {
            budget.deferred.push(piece.get());
            return;
        }
        budget.spill(piece.get());
    }

    fn piece_written(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let budget = self.budget.lock();
        if budget.lost && budget.lose == Some(piece.get()) {
            anyhow::bail!(std::io::Error::other("the disk failed a write"));
        }
        Ok(())
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
// room: Q's block lands, and its requests for P still stand. P's next
// block, which holds a block's room for its write, finds none past Q's: the
// owner stops again and gives Q back. Each block then holds room for its
// write alone, and both pieces complete.
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

    h.answer(&peer, 0, 1).await;
    assert_eq!(h.room_waits(), 2);
    assert!(!h.holds(&peer, 1), "the second started piece went back");
    assert_eq!(storage.room_of(1), Some(Room::Block));
    for chunk in 1..CHUNKS_PER_PIECE {
        if chunk > 1 {
            h.answer(&peer, 0, chunk).await;
        }
        h.answer(&peer, 1, chunk).await;
    }
    h.drained().await;
    assert!(h.have(0) && h.have(1));
    assert_eq!(storage.budget.lock().peak, 1);
    assert_eq!(h.room_waits(), 2);
    assert_eq!(h.redundant(), 0);
    assert_eq!(storage.used(), 0, "every block's room came back");
}

// An owner leaves mid-piece: the piece goes back and gives back its whole
// room, which the next piece's first block takes. The piece given back is
// finished block by block, each block holding its own room: the peer that
// finishes it, past the room its own piece holds, stops and gives that
// piece back too.
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
    assert_eq!(storage.used(), 0);

    h.send(&next, (other, 0), false).await.unwrap();
    assert_eq!(storage.room_of(other), Some(Room::Whole));
    assert_eq!(h.room_waits(), 0);

    assert_eq!(h.grab(&next), left);
    for chunk in 1..CHUNKS_PER_PIECE {
        h.send(&next, (left, chunk), false).await.unwrap();
        h.send(&next, (other, chunk), false).await.unwrap();
    }
    h.drained().await;
    assert!(h.have(left) && h.have(other));
    assert_eq!(h.room_waits(), 1);
    assert_eq!(storage.used(), 0);
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
    h.drained().await;
    assert_eq!(
        storage.used(),
        0,
        "the block's room came back with its write"
    );
}

// A block a disk thread lands for a piece its peer owns, with no room for
// the whole piece: a piece given back still holds its room, its blocks being
// written aside. The block starts its piece as one written block by block,
// in the room its own write holds, and is written: it is not dropped and
// counted redundant, and only the blocks still missing are fetched.
#[tokio::test]
async fn a_block_on_a_disk_thread_without_room_for_its_piece_is_written() {
    let (h, storage) = budgeted("room_disk_thread", 1).await;
    storage.budget.lock().defer_spills = true;
    let (gone, _gone_rx) = h.peer("127.0.0.1:6881");
    let (owner, _owner_rx) = h.peer("127.0.0.2:6881");
    let left = h.grab(&gone);
    let wanted = h.grab(&owner);
    h.send(&gone, (left, 0), false).await.unwrap();
    gone.on_i_am_choked().unwrap();
    // Given back, and its room not back yet.
    assert_eq!(storage.room_of(left), Some(Room::Whole));
    assert!(h.holds(&owner, wanted));

    let info = h.chunk(wanted, 1);
    let start = (wanted * PIECE_LENGTH + CHUNK_SIZE) as usize;
    let block = Piece::from_data(
        wanted,
        CHUNK_SIZE,
        &h.content[start..start + CHUNK_SIZE as usize],
    );
    let landed = land_block(&h.live, owner.addr, &block, &info).unwrap();
    assert!(landed.is_none());
    assert_eq!(storage.room_of(wanted), Some(Room::Block));
    assert_eq!(h.missing_chunks(wanted), vec![0, 2, 3]);
    assert_eq!(h.redundant(), 0);

    storage.spill_deferred();
    assert_eq!(storage.room_of(left), Some(Room::Block));
    assert_eq!(storage.used(), 0);
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
        h.ask(&slow, 1, chunk);
    }
    h.answer(&slow, 0, 0).await;
    h.answer(&slow, 1, 0).await;
    assert!(!h.holds(&slow, 0));
    assert_eq!(storage.room_of(0), Some(Room::Block));
    // The first finishes Q, which holds the room.
    for chunk in 1..CHUNKS_PER_PIECE {
        h.answer(&slow, 1, chunk).await;
    }
    h.drained().await;
    assert!(h.have(1));

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
    assert_eq!(h.room_waits(), 1);
    assert_eq!(storage.used(), 0);
}

// A waiter whose first block has no room, behind a holder's piece: stalled
// once the storage has refused it.
async fn stalled_behind_a_holder<'a>(
    h: &'a Harness,
    storage: &BudgetStorageFactory,
    waiter: &'a PeerHandler,
    holder: &PeerHandler,
) -> (u32, u32, Pin<Box<dyn Future<Output = ()> + 'a>>) {
    let wanted = h.grab(waiter);
    let held = h.grab(holder);
    h.send(holder, (held, 0), false).await.unwrap();
    h.ask(waiter, wanted, 0);
    h.ask(waiter, wanted, 1);
    let mut stalled: Pin<Box<dyn Future<Output = ()> + 'a>> = Box::pin(h.answer(waiter, wanted, 0));
    tokio::select! {
        () = &mut stalled => panic!("the block landed without room"),
        () = storage.refused() => {}
    }
    (wanted, held, stalled)
}

// A connection stopped for room is not held to its timeouts: across an
// hour on the paused clock it has no request deadline, and is neither
// snubbed nor dropped. It goes on once the room it waits for comes back.
#[tokio::test]
async fn a_peer_stopped_for_room_is_not_timed_out_across_a_long_wait() {
    let (h, storage) = budgeted("room_long_wait", 1).await;
    let (waiter, _waiter_rx) = h.peer("127.0.0.1:6881");
    let (holder, _holder_rx) = h.peer("127.0.0.2:6881");
    let (wanted, held, stalled) = stalled_behind_a_holder(&h, &storage, &waiter, &holder).await;
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

// A pause ends a connection's wait for room: the block it holds is
// dropped, to be fetched again in a later run, and the pause goes through.
#[tokio::test]
async fn a_pause_ends_a_wait_for_room() {
    let (h, storage) = budgeted("room_pause", 1).await;
    let (waiter, _waiter_rx) = h.peer("127.0.0.1:6881");
    let (holder, _holder_rx) = h.peer("127.0.0.2:6881");
    let (wanted, _, stalled) = stalled_behind_a_holder(&h, &storage, &waiter, &holder).await;
    let live = h.live.clone();
    let pausing = tokio::task::spawn_blocking(move || live.pause().map(drop));
    stalled.await;
    pausing.await.unwrap().unwrap();
    assert_eq!(storage.room_of(wanted), None, "the waiter took no room");
    assert_eq!(h.room_waits(), 1);
}

// A piece given back whose blocks fail to reach its files is refused by the
// storage, not failed: no sender is struck, no byte counts as corrupt, and
// its room comes back. The piece is fetched again.
#[tokio::test]
async fn a_given_back_piece_whose_writes_fail_is_refused_not_failed() {
    let (h, storage) = budgeted("room_spill_fails", 1).await;
    storage.budget.lock().lose = Some(0);
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    h.take_every(&peer);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.ask(&peer, 0, chunk);
        h.ask(&peer, 1, chunk);
    }
    h.answer(&peer, 0, 0).await;
    h.answer(&peer, 1, 0).await;
    assert_eq!(storage.room_of(0), Some(Room::Block));
    for chunk in 1..CHUNKS_PER_PIECE {
        h.answer(&peer, 0, chunk).await;
        h.answer(&peer, 1, chunk).await;
    }
    h.drained().await;
    assert!(!h.have(0) && h.have(1));
    assert_eq!(h.missing_chunks(0), vec![0, 1, 2, 3]);
    let (_, hashfails, parole) = h.trust(&peer);
    assert_eq!((hashfails, parole), (0, false));
    assert!(!h.banned(&peer));
    assert_eq!(h.get_failed_bytes(), 0);
    let checks = h.session_stats.swarm.snapshot().piece_checks;
    assert_eq!(checks[PieceCheck::StorageRefused as usize], 1);
    assert_eq!(checks[PieceCheck::Failed as usize], 0);
    assert_eq!(storage.used(), 0);
    assert_eq!(storage.room_of(0), None);
}

// A piece given back after a corrupt block, finished block by block by
// another peer, fails once: both senders are struck, the whole piece counts
// as corrupt once, and its room comes back. A third peer then sends it
// whole, and the one whose block differed is banned.
#[tokio::test]
async fn a_given_back_piece_that_fails_its_check_strikes_its_senders_once() {
    let (h, storage) = budgeted("room_spill_corrupt", 1).await;
    let (bad, _bad_rx) = h.peer("127.0.0.1:6881");
    let (good, _good_rx) = h.peer("127.0.0.2:6881");
    let (third, _third_rx) = h.peer("127.0.0.3:6881");
    h.fail_shared(&bad, &good).await;
    h.drained().await;
    for peer in [&bad, &good] {
        assert_eq!(h.trust(peer), (-2, 1, true));
    }
    assert_eq!(h.get_failed_bytes(), u64::from(PIECE_LENGTH));
    assert_eq!(storage.used(), 0);
    assert_eq!(storage.room_of(0), None);

    h.take(&third, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&third, (0, chunk), false).await.unwrap();
    }
    h.drained().await;
    assert!(h.have(0));
    assert!(h.banned(&bad));
    assert!(!h.banned(&good) && !h.banned(&third));
    assert_eq!(storage.used(), 0);
}

// A piece given back and taken by a slow peer, which sends one block, is
// taken over by a fast one in the end game: it finishes the piece block by
// block, each block holding room for its write alone.
#[tokio::test]
async fn a_given_back_piece_taken_over_by_another_peer_completes() {
    let (h, storage) = budgeted("room_spill_stolen", 1).await;
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    let (slow, _slow_rx) = h.peer("127.0.0.2:6881");
    let (fast, _fast_rx) = h.peer("127.0.0.3:6881");
    h.take(&owner, 0);
    h.send(&owner, (0, 0), false).await.unwrap();
    owner.on_i_am_choked().unwrap();
    assert_eq!(storage.room_of(0), Some(Room::Block));

    assert_eq!(h.grab(&slow), 0, "the started piece is handed out first");
    h.send(&slow, (0, 1), false).await.unwrap();
    fast.counters
        .on_piece_completed(PIECE_LENGTH.into(), Duration::from_secs(1));
    tokio::time::advance(Duration::from_secs(100)).await;
    assert_eq!(h.grab(&fast), 1);
    assert_eq!(h.grab(&fast), 0, "the slow peer's piece is taken over");
    assert!(h.holds(&fast, 0) && !h.holds(&slow, 0));

    for chunk in [2, 3] {
        h.send(&fast, (0, chunk), false).await.unwrap();
    }
    h.drained().await;
    assert!(h.have(0));
    assert_eq!(h.redundant(), 0);
    assert_eq!(storage.used(), 0);
}

/// A runtime handle on a uTP thread, and what keeps the thread up.
async fn utp_thread() -> (tokio::runtime::Handle, impl Sized) {
    let factory = crate::test_support::socket_factory();
    let bind = |shards| {
        crate::listen::bind_utp(
            factory.as_ref(),
            (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            weaver_utp::SocketOpts {
                shards,
                ..Default::default()
            },
            crate::udp_options::STREAM_WINDOW,
            None,
        )
    };
    let server = bind(Some(weaver_utp::Shards::new("room-utp", 1)))
        .await
        .unwrap();
    let client = bind(None).await.unwrap();
    let (accepted, connected) = tokio::join!(server.accept(), client.connect(server.bind_addr()));
    let accepted = accepted.unwrap();
    let home = accepted.home().cloned().expect("a stream on a uTP thread");
    (home, (accepted, connected, server, client))
}

// The blocks of a piece written block by block arrive on a uTP thread, as
// a message read in place and as a block in its own buffer: neither is
// written there, as every stream of the thread would wait on the disk.
#[tokio::test(flavor = "multi_thread")]
async fn a_given_back_pieces_blocks_from_a_utp_thread_are_written_off_it() {
    let storage = BudgetStorageFactory::with_room_for(1);
    let h = Harness::build_on(
        "room_utp",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    h.take(&owner, 0);
    h.send(&owner, (0, 0), false).await.unwrap();
    owner.on_i_am_choked().unwrap();
    assert_eq!(storage.room_of(0), Some(Room::Block));

    let (peer, _peer_rx) = h.peer("127.0.0.2:6881");
    for chunk in [1, 2] {
        h.ask(&peer, 0, chunk);
    }
    let len = CHUNK_SIZE as usize;
    let at = |chunk: usize| chunk * len;
    let read_in_place = h.content[at(1)..at(2)].to_vec();
    let mut buf = h.live.shared.spawner.pool().take();
    buf.as_mut_slice()[..len].copy_from_slice(&h.content[at(2)..at(3)]);
    let own_buffer = crate::disk::Block::new(0, 2 * CHUNK_SIZE, len, buf);

    let (home, _kept) = utp_thread().await;
    let peer = home
        .spawn(async move {
            assert!(weaver_utp::on_shard_thread());
            peer.on_received_piece(Piece::from_data(0, CHUNK_SIZE, &read_in_place[..]))
                .await
                .unwrap();
            (&peer).on_received_block(own_buffer).await.unwrap();
            peer
        })
        .await
        .expect("no block was written on the uTP thread");
    h.send(&peer, (0, 3), false).await.unwrap();
    h.drained().await;
    assert!(h.have(0));
    assert_eq!(storage.used(), 0);
}
