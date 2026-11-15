// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

/// A memory storage that holds room for a piece from the moment it is
/// handed out until it verifies or fails its hash, and refuses a write of a
/// piece it holds no room for: the shape of a storage under a resident
/// budget.
#[derive(Clone, Default)]
struct HeldStorageFactory {
    inner: MemoryStorageFactory,
    held: Arc<Mutex<std::collections::HashSet<u32>>>,
    // Holds the one piece check it is armed for.
    gate: Arc<Gate>,
    // The thread of each piece check, in order.
    checked_on: Arc<Mutex<Vec<std::thread::ThreadId>>>,
}

struct HeldStorage {
    inner: Box<dyn TorrentStorage>,
    held: Arc<Mutex<std::collections::HashSet<u32>>>,
    gate: Arc<Gate>,
    checked_on: Arc<Mutex<Vec<std::thread::ThreadId>>>,
}

impl StorageFactory for HeldStorageFactory {
    type Storage = HeldStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<HeldStorage> {
        Ok(HeldStorage {
            inner: Box::new(self.inner.create(shared, metadata)?),
            held: self.held.clone(),
            gate: self.gate.clone(),
            checked_on: self.checked_on.clone(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

impl TorrentStorage for HeldStorage {
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
            buf.is_empty() || self.held.lock().contains(&piece),
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
        Ok(Box::new(HeldStorage {
            inner: self.inner.take()?,
            held: self.held.clone(),
            gate: self.gate.clone(),
            checked_on: self.checked_on.clone(),
        }))
    }

    // The piece is read back by its check, which is held here when the gate
    // is armed.
    fn hash_held_piece(
        &self,
        _piece_index: ValidPieceIndex,
        _update: &mut dyn FnMut(&[u8]),
    ) -> bool {
        self.checked_on.lock().push(std::thread::current().id());
        self.gate.hold();
        false
    }

    fn reserve_piece(&self, piece: ValidPieceIndex, _room: &Arc<Notify>) -> bool {
        self.held.lock().insert(piece.get());
        true
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.held.lock().remove(&piece.get());
        self.inner.on_piece_completed(piece)
    }

    fn on_piece_hash_failed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.held.lock().remove(&piece.get());
        self.inner.on_piece_hash_failed(piece)
    }
}

// A piece fails its hash while another peer still owes a block of it: that
// block is data like any other, written into room the piece is granted
// afresh, never a write the storage refuses. The torrent keeps going and the
// piece, fetched again, verifies.
#[tokio::test]
async fn a_block_owed_for_a_piece_that_failed_its_hash_is_written_into_room_granted_again() {
    let storage = HeldStorageFactory::default();
    let h = Harness::build_on(
        "block_after_hash_failure",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    tokio::time::pause();
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    let (late, _late_rx) = h.peer("127.0.0.2:6881");

    // The late peer is asked for the piece's last block, then the piece goes
    // back to the queue with the request still out.
    let last = CHUNKS_PER_PIECE - 1;
    h.take(&late, 0);
    assert!(
        h.peers
            .with_live_mut(late.addr, "test", |live| live
                .add_inflight_request(h.chunk(0, last)))
            .unwrap()
    );
    h.lock_write("test")
        .get_pieces_mut()
        .unwrap()
        .release_pieces_owned_by(late.addr);

    // Another peer fetches the piece whole, and it fails its hash.
    h.take(&owner, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&owner, (0, chunk), chunk == 0).await.unwrap();
    }
    h.settled().await;
    assert_eq!(h.missing_chunks(0), vec![0, 1, 2, 3]);
    assert!(!storage.held.lock().contains(&0));
    // The piece waits no more, and its connection was told so.
    assert_eq!(h.live.shared.spawner.checks().spans(), 0);
    assert_eq!(owner.disk.outstanding(), 0);

    // The owed block arrives.
    let start = (last * CHUNK_SIZE) as usize;
    late.on_received_piece(Piece::from_data(
        0,
        last * CHUNK_SIZE,
        &h.content[start..start + CHUNK_SIZE as usize],
    ))
    .await
    .unwrap();
    assert!(h.lock_read("test").fatal_errors_tx.is_some());
    assert_eq!(h.missing_chunks(0), vec![0, 1, 2]);

    h.take(&late, 0);
    for chunk in 0..last {
        h.send(&late, (0, chunk), false).await.unwrap();
    }
    h.settled().await;
    assert!(h.have(0));
    assert!(!storage.held.lock().contains(&0));
}

// A block the storage copies into memory lands with every blocking permit
// taken, on the task that read it, and so does its piece's last: the piece
// is checked on the hashing thread and written on the torrent's turn of the
// write threads, neither of which is a blocking permit's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_piece_copied_into_memory_is_landed_checked_and_written_with_no_blocking_permit() {
    let storage = HeldStorageFactory::default();
    let h = Harness::build_on(
        "memory_block_inline",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    h.take(&owner, 0);
    let semaphore = h.live.shared.spawner.semaphore();
    let permits = u32::try_from(semaphore.available_permits()).unwrap();
    let _taken = semaphore.acquire_many_owned(permits).await.unwrap();

    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&owner, (0, chunk), false).await.unwrap();
    }
    assert!(h.missing_chunks(0).is_empty());
    h.settled().await;
    assert!(h.have(0));
}

// The check of a piece whose last block landed belongs to the torrent, not
// to the connection that sent the block: the connection ends while the check
// is held on the hashing thread, and the piece is checked, written and had
// all the same (libtorrent `torrent::verify_piece` hands the hash job to the
// disk thread bound to the torrent, `shared_from_this()`, and
// `torrent::on_piece_verified` takes its verdict whatever became of the
// sender). Here piece 1 is had, so the torrent completes once piece 0 is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_piece_whose_connection_ends_before_its_check_is_checked_all_the_same() {
    let storage = HeldStorageFactory::default();
    let h = Harness::build_on(
        "check_outlives_connection",
        0x40,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    let (owner, owner_rx) = h.peer("127.0.0.1:6881");
    h.take(&owner, 0);
    let (mut entered, release) = storage.gate.arm();
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&owner, (0, chunk), false).await.unwrap();
    }
    entered.recv().await.unwrap();
    assert!(h.missing_chunks(0).is_empty());
    assert!(!h.have(0));
    drop(owner);
    drop(owner_rx);

    release.send(()).unwrap();
    h.handle().wait_until_completed().await.unwrap();
    assert!(h.have(0));
}

// The check of piece 0 is held on the hashing thread when the blocks of
// piece 1 come. The connection takes them all the same, every one landing
// as it is read: both pieces wait whole in the storage's memory, counted,
// piece 1 behind piece 0 for the one hashing thread. Released, the pieces
// are checked and written in that order, both are had, the torrent finishes
// once, and nothing is left waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_receives_the_next_piece_while_the_last_is_being_hashed() {
    use futures::FutureExt;
    let storage = HeldStorageFactory::default();
    let h = Harness::build_on(
        "next_piece_while_hashing",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    // Both pieces, in whichever order they are picked.
    owner.on_i_am_unchoked();
    for _ in 0..PIECES {
        assert!(owner.acquire_next_piece().unwrap().piece().is_some());
    }
    let spawner = &h.live.shared.spawner;
    let mut finished = std::pin::pin!(h.live.finished_notify.notified());
    finished.as_mut().enable();

    let (mut entered, release) = storage.gate.arm();
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&owner, (0, chunk), false).await.unwrap();
    }
    entered.recv().await.unwrap();
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&owner, (1, chunk), false).await.unwrap();
    }
    assert!(h.missing_chunks(1).is_empty());
    assert!(!h.have(0) && !h.have(1));
    // One check runs and one waits for the thread; no second thread is
    // started for it.
    assert_eq!(spawner.hash().at_work(), (1, 1));
    assert_eq!(spawner.hash().wakes(), 1);
    assert_eq!(spawner.checks().spans(), 2);
    assert_eq!(spawner.checks().queued_bytes(), u64::from(2 * PIECE_LENGTH));
    let waiting = spawner.disk_snapshot();
    assert_eq!(waiting.check_pieces_waiting, 2);
    assert_eq!(waiting.check_bytes_waiting, u64::from(2 * PIECE_LENGTH));
    assert_eq!(owner.disk.outstanding(), 2);
    assert!(!owner.disk.blocked());
    assert!(finished.as_mut().now_or_never().is_none());

    release.send(()).unwrap();
    h.settled().await;
    assert!(h.have(0) && h.have(1));
    assert!(finished.now_or_never().is_some());
    assert!(storage.held.lock().is_empty());
    assert_eq!(spawner.checks().spans(), 0);
    assert_eq!(spawner.checks().queued_bytes(), 0);
    assert_eq!(owner.disk.outstanding(), 0);
    assert_eq!(h.live.stats_snapshot().downloaded_and_checked_pieces, 2);
    assert!(spawner.disk_snapshot().hash_busy_nanos > 0);
    // The torrent finished once: nobody who waits from here on is rung.
    let again = h.live.finished_notify.notified();
    assert!(again.now_or_never().is_none());
}

// A live peer of `h` over `kind` that has every piece.
fn peer_with_every_piece(h: &Harness, kind: ConnectionKind) -> (PeerHandler, PeerRx) {
    let (peer, rx) = h.new_peer_over("127.0.0.1:6881", kind);
    peer.first_message_received.store(true, Ordering::Relaxed);
    let mut every_piece = peer::empty_bitfield(h.lengths.total_pieces());
    every_piece[..h.lengths.total_pieces() as usize].fill(true);
    peer.on_bitfield(every_piece.as_raw_slice()).unwrap();
    (peer, rx)
}

// Two torrents of one session: the first's piece comes from a peer over TCP,
// the second's from a peer over uTP. The session has one hashing thread:
// with it held on the first torrent's piece, the second's waits for it, and
// no second thread is started. Released, both are checked on that one
// thread, written and had.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pieces_of_two_torrents_over_tcp_and_utp_are_checked_by_the_sessions_one_thread() {
    let over_tcp = HeldStorageFactory::default();
    let over_utp = HeldStorageFactory::default();
    let a = Harness::build_on(
        "one_hasher_tcp",
        0x00,
        isolated_session_opts(false),
        over_tcp.clone_box(),
    )
    .await;
    let add = AddTorrentOptions {
        storage_factory: Some(over_utp.clone_box()),
        ..Default::default()
    };
    let out = TempDir::with_prefix("one_hasher_utp").unwrap();
    let b = Harness::added_to(
        a._session.clone(),
        out,
        "one_hasher_utp",
        PIECES,
        &[0x00],
        add,
    )
    .await;
    let spawner = &a.live.shared.spawner;
    assert!(spawner.hash().is(b.live.shared.spawner.hash()));

    let (tcp, _tcp_rx) = peer_with_every_piece(&a, ConnectionKind::Tcp);
    let (utp, _utp_rx) = peer_with_every_piece(&b, ConnectionKind::Utp);
    a.take(&tcp, 0);
    b.take(&utp, 0);

    let (mut entered, release) = over_tcp.gate.arm();
    for chunk in 0..CHUNKS_PER_PIECE {
        a.send(&tcp, (0, chunk), false).await.unwrap();
    }
    entered.recv().await.unwrap();
    for chunk in 0..CHUNKS_PER_PIECE {
        b.send(&utp, (0, chunk), false).await.unwrap();
    }
    // The one thread is on the first torrent's piece; the second's waits
    // for it.
    assert_eq!(spawner.hash().at_work(), (1, 1));
    assert_eq!(spawner.hash().wakes(), 1);
    assert_eq!(spawner.disk_snapshot().hash_jobs_queued, 1);
    assert!(over_utp.checked_on.lock().is_empty());
    assert!(!a.have(0) && !b.have(0));

    release.send(()).unwrap();
    a.settled().await;
    assert!(a.have(0) && b.have(0));
    assert_eq!(spawner.hash().wakes(), 1);
    assert_eq!(spawner.disk_snapshot().hash_jobs_queued, 0);
    let (on_tcp, on_utp) = (over_tcp.checked_on.lock(), over_utp.checked_on.lock());
    assert_eq!((on_tcp.len(), on_utp.len()), (1, 1));
    assert_eq!(on_tcp[0], on_utp[0]);
    assert!(spawner.disk_snapshot().hash_busy_nanos > 0);
}

// Two sessions, a torrent on each. Each session has a hashing thread of its
// own: with the first's held on its piece, the second's piece is checked,
// written and had, on another thread. The first session ends; the second
// goes on checking its pieces.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_session_checks_its_pieces_on_its_own_thread_and_goes_on_when_another_ends() {
    let first = HeldStorageFactory::default();
    let second = HeldStorageFactory::default();
    let a = Harness::build_on(
        "two_hashers_first",
        0x00,
        isolated_session_opts(false),
        first.clone_box(),
    )
    .await;
    let b = Harness::build_on(
        "two_hashers_second",
        0x00,
        isolated_session_opts(false),
        second.clone_box(),
    )
    .await;
    let (a_spawner, b_spawner) = (a.live.shared.spawner.clone(), b.live.shared.spawner.clone());
    assert!(!a_spawner.hash().is(b_spawner.hash()));

    let (a_peer, _a_rx) = a.peer("127.0.0.1:6881");
    let (b_peer, _b_rx) = b.peer("127.0.0.1:6881");
    a.take(&a_peer, 0);
    b.take_every(&b_peer);

    let (mut entered, release) = first.gate.arm();
    for chunk in 0..CHUNKS_PER_PIECE {
        a.send(&a_peer, (0, chunk), false).await.unwrap();
    }
    entered.recv().await.unwrap();
    for chunk in 0..CHUNKS_PER_PIECE {
        b.send(&b_peer, (0, chunk), false).await.unwrap();
    }
    // The second session's piece does not wait for the first's thread.
    b.settled().await;
    assert!(b.have(0) && !a.have(0));
    assert_eq!(a_spawner.hash().at_work(), (1, 0));
    assert_ne!(first.checked_on.lock()[0], second.checked_on.lock()[0]);

    release.send(()).unwrap();
    a.settled().await;
    assert!(a.have(0));
    // The first session ends, its torrent and its queues with it.
    a._session.stop().await;
    drop((a_peer, _a_rx, a_spawner, a));

    for chunk in 0..CHUNKS_PER_PIECE {
        b.send(&b_peer, (1, chunk), false).await.unwrap();
    }
    b.settled().await;
    assert!(b.have(1));
    assert_eq!(second.checked_on.lock().len(), 2);
}

// A connection whose whole pieces fill the count of those waiting to its
// high mark, the hashing thread held on the first, stops reading; once they
// are checked and written it reads again. Below the mark it reads on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_stops_reading_at_the_high_mark_of_pieces_waiting_and_reads_again() {
    let storage = HeldStorageFactory::default();
    let add = AddTorrentOptions {
        storage_factory: Some(storage.clone_box()),
        ..Default::default()
    };
    let out = TempDir::with_prefix("check_back_pressure").unwrap();
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
        .await
        .unwrap();
    let checks = session.spawner.checks().clone();
    let high = checks.max_use() / 2 + checks.max_use() / 4;
    let pieces_to_high = u32::try_from(high).unwrap().div_ceil(CHUNKS_PER_PIECE);
    let pieces = pieces_to_high + 2;
    let none = vec![0u8; pieces.div_ceil(8) as usize];
    let h = Harness::added_to(session, out, "check_back_pressure", pieces, &none, add).await;
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");

    owner.on_i_am_unchoked();
    let mut taken = Vec::new();
    while let Some(piece) = owner.acquire_next_piece().unwrap().piece() {
        taken.push(piece.get());
    }
    taken.truncate(pieces_to_high as usize);
    assert_eq!(taken.len(), pieces_to_high as usize);

    let (mut entered, release) = storage.gate.arm();
    for (sent, &index) in taken.iter().enumerate() {
        for chunk in 0..CHUNKS_PER_PIECE {
            h.send(&owner, (index, chunk), false).await.unwrap();
        }
        if sent == 0 {
            entered.recv().await.unwrap();
        }
        assert_eq!(
            owner.disk.blocked(),
            sent + 1 == taken.len(),
            "after {sent} pieces"
        );
    }
    assert_eq!(checks.spans(), u64::from(pieces_to_high));
    assert!(checks.in_use() >= high);
    // The blocks themselves wait nowhere: the pool that counts them is empty.
    assert_eq!(h.live.shared.spawner.pool().in_use(), 0);
    let handler = &owner;
    let mut room = Box::pin(handler.disk_room());
    assert!(futures::poll!(room.as_mut()).is_pending());

    release.send(()).unwrap();
    room.await;
    assert!(!owner.disk.blocked());
    h.settled().await;
    assert_eq!(checks.in_use(), 0);
    assert_eq!(checks.spans(), 0);
    assert_eq!(owner.disk.outstanding(), 0);
    assert!(taken.iter().all(|&index| h.have(index)));
}

/// A memory storage that writes as a file storage does (off the network
/// threads, through the disk queue), counts the bytes it writes, and holds the one
/// write it is armed for until released.
#[derive(Clone, Default)]
struct GatedStorageFactory {
    inner: MemoryStorageFactory,
    gate: Arc<Gate>,
}

#[derive(Default)]
struct Gate {
    armed: AtomicBool,
    // Bytes written.
    written: AtomicUsize,
    entered: Mutex<Option<tokio::sync::mpsc::UnboundedSender<()>>>,
    release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

impl Gate {
    /// Arms the gate: the next write is held until the returned sender is
    /// sent to, and the returned receiver hears when it is held.
    fn arm(
        &self,
    ) -> (
        tokio::sync::mpsc::UnboundedReceiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered_tx, entered) = tokio::sync::mpsc::unbounded_channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        *self.entered.lock() = Some(entered_tx);
        *self.release.lock() = Some(release_rx);
        self.armed.store(true, Ordering::SeqCst);
        (entered, release)
    }

    /// Waits here until released, when armed.
    fn hold(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            let entered = self.entered.lock().take().unwrap();
            let release = self.release.lock().take().unwrap();
            entered.send(()).unwrap();
            release.recv().unwrap();
        }
    }
}

struct GatedStorage {
    inner: Box<dyn TorrentStorage>,
    gate: Arc<Gate>,
}

impl StorageFactory for GatedStorageFactory {
    type Storage = GatedStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<GatedStorage> {
        Ok(GatedStorage {
            inner: Box::new(self.inner.create(shared, metadata)?),
            gate: self.gate.clone(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

impl TorrentStorage for GatedStorage {
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
        if self.gate.armed.swap(false, Ordering::SeqCst) {
            let entered = self.gate.entered.lock().take().unwrap();
            let release = self.gate.release.lock().take().unwrap();
            entered.send(()).unwrap();
            release.recv().unwrap();
        }
        self.gate.written.fetch_add(buf.len(), Ordering::SeqCst);
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
        Ok(Box::new(GatedStorage {
            inner: self.inner.take()?,
            gate: self.gate.clone(),
        }))
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.inner.on_piece_completed(piece)
    }
}

impl Harness {
    /// Resolves once no piece waits for its check or for its write.
    async fn settled(&self) {
        let spawner = &self.live.shared.spawner;
        spawner.hash().idle().await;
        spawner.disk().idle().await;
    }

    /// The torrent is paused on a thread of its own, as a pause waits for
    /// the write and the check under way. Resolves once the run is told to
    /// end, with the pause to wait for.
    async fn pausing(&self) -> tokio::task::JoinHandle<TorrentStatePaused> {
        let live = self.live.clone();
        let pausing = tokio::task::spawn_blocking(move || live.pause().unwrap());
        self.live.cancellation_token.cancelled().await;
        pausing
    }

    /// The peer is asked for chunk `chunk` of piece `index`, and it arrives
    /// in a pool buffer, as a connection receives it, to be queued for the
    /// disk.
    async fn send_block(&self, peer: &PeerHandler, (index, chunk): (u32, u32)) {
        let info = self.chunk(index, chunk);
        self.peers
            .with_live_mut(peer.addr, "test", |live| live.add_inflight_request(info))
            .unwrap();
        let start = (index * PIECE_LENGTH + chunk * CHUNK_SIZE) as usize;
        let len = CHUNK_SIZE as usize;
        let mut buf = self.live.shared.spawner.pool().take();
        buf.as_mut_slice()[..len].copy_from_slice(&self.content[start..start + len]);
        peer.on_received_block(Block::new(index, chunk * CHUNK_SIZE, len, buf))
            .await
            .unwrap();
    }
}

// The connection that brought the last block of piece 0 ends while that
// block is being written on a disk thread. The torrent, not the connection,
// owns the write's completion (libtorrent `on_disk_write_complete` runs
// whatever became of the peer): the block lands, the piece is checked, and
// the torrent completes. The buffer goes back to the pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_last_block_whose_connection_ends_mid_write_is_landed_and_checked() {
    let storage = GatedStorageFactory::default();
    // Piece 1 is had: the torrent completes once piece 0 is.
    let h = Harness::build_on(
        "last_block_cancel",
        0x40,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    let pool = h.live.shared.spawner.pool().clone();
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    h.take(&owner, 0);
    let last = CHUNKS_PER_PIECE - 1;
    for chunk in 0..last {
        h.send(&owner, (0, chunk), false).await.unwrap();
    }
    assert_eq!(h.missing_chunks(0), vec![last]);

    let (mut entered, release) = storage.gate.arm();
    h.send_block(&owner, (0, last)).await;
    entered.recv().await.unwrap();
    assert_eq!(pool.in_use(), 1);
    // The connection ends mid-write.
    owner.on_peer_died(None).unwrap();
    release.send(()).unwrap();

    h.handle().wait_until_completed().await.unwrap();
    assert!(h.have(0));
    h.live.shared.spawner.disk().idle().await;
    assert_eq!(pool.in_use(), 0);
}

// The torrent pauses with one block being written and one queued behind
// it. The write under way finishes, and the run does not end before it
// does; the queued block is not written, its buffer goes back, and nothing
// is left in the pool's count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pause_with_blocks_queued_writes_none_after_it_and_returns_their_buffers() {
    let storage = GatedStorageFactory::default();
    let h = Harness::build_on(
        "pause_with_blocks_queued",
        0x40,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    let pool = h.live.shared.spawner.pool().clone();
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    h.take(&owner, 0);

    let (mut entered, release) = storage.gate.arm();
    h.send_block(&owner, (0, 0)).await;
    entered.recv().await.unwrap();
    // One disk thread is busy with the held write: the next block waits in
    // the queue behind it.
    h.send_block(&owner, (0, 1)).await;
    assert_eq!(pool.in_use(), 2);

    let pausing = h.pausing().await;
    // The pause waits for the write under way.
    assert!(!pausing.is_finished());
    let run = h.live.shared.ending_run.lock().clone().unwrap();
    let mut ended = Box::pin(run.wait());
    assert!(futures::poll!(ended.as_mut()).is_pending());

    release.send(()).unwrap();
    let _paused = pausing.await.unwrap();
    ended.await;
    h.live.shared.spawner.disk().idle().await;
    // The held write went through; the queued one never did.
    assert_eq!(
        storage.gate.written.load(Ordering::SeqCst),
        CHUNK_SIZE as usize
    );
    assert_eq!(pool.in_use(), 0);
    assert_eq!(owner.disk.outstanding(), 0);
}

// A connection whose blocks fill the pool to its high mark, one write of
// its own already outstanding, stops reading; once its writes are done it
// reads again (libtorrent `peer_connection::incoming_piece` sets `bw_disk`,
// `on_disk_write_complete` clears it). The disk thread woke once for the
// batch, not once per block.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_stops_reading_at_the_pools_high_mark_and_reads_again_once_written() {
    let storage = GatedStorageFactory::default();
    let add = AddTorrentOptions {
        storage_factory: Some(storage.clone_box()),
        ..Default::default()
    };
    let out = TempDir::with_prefix("disk_back_pressure").unwrap();
    let opts = SessionOptions {
        // One disk thread: every block queued behind the held one waits.
        runtime_worker_threads: Some(1),
        ..isolated_session_opts(false)
    };
    let session = Session::new_with_opts(out.path().into(), opts)
        .await
        .unwrap();
    let h = Harness::added_to(session, out, "disk_back_pressure", 16, &[0, 0], add).await;
    let pool = h.live.shared.spawner.pool().clone();
    let high = pool.max_use() / 2 + pool.max_use() / 4;
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");

    // The connection takes every piece.
    owner.on_i_am_unchoked();
    let mut pieces = Vec::new();
    while let Some(piece) = owner.acquire_next_piece().unwrap().piece() {
        pieces.push(piece.get());
    }
    let blocks: Vec<(u32, u32)> = pieces
        .iter()
        .flat_map(|&index| (0..CHUNKS_PER_PIECE).map(move |chunk| (index, chunk)))
        .take(high)
        .collect();
    assert_eq!(blocks.len(), high);

    let (mut entered, release) = storage.gate.arm();
    for (sent, &block) in blocks.iter().enumerate() {
        h.send_block(&owner, block).await;
        if sent == 0 {
            entered.recv().await.unwrap();
        }
        // Below the high mark the connection reads on.
        assert_eq!(
            owner.disk.blocked(),
            sent + 1 == high,
            "after {sent} blocks"
        );
    }
    assert_eq!(pool.in_use(), high);
    let handler = &owner;
    let mut room = Box::pin(handler.disk_room());
    assert!(futures::poll!(room.as_mut()).is_pending());

    release.send(()).unwrap();
    room.await;
    assert!(!owner.disk.blocked());
    h.live.shared.spawner.disk().idle().await;
    assert_eq!(pool.in_use(), 0);
    // One wake: the thread that wrote the held block took every block
    // queued behind it as one batch.
    assert_eq!(h.live.shared.spawner.disk().wakes(), 1);
}

/// A memory storage, writing as a file storage does, whose one armed read
/// (the check of a piece reading it back) is held until released, and whose
/// next read errors when told to.
#[derive(Clone, Default)]
struct ReadGatedStorageFactory {
    inner: MemoryStorageFactory,
    gate: Arc<Gate>,
    fail_read: Arc<AtomicBool>,
}

struct ReadGatedStorage {
    inner: Box<dyn TorrentStorage>,
    gate: Arc<Gate>,
    fail_read: Arc<AtomicBool>,
}

impl StorageFactory for ReadGatedStorageFactory {
    type Storage = ReadGatedStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<ReadGatedStorage> {
        Ok(ReadGatedStorage {
            inner: Box::new(self.inner.create(shared, metadata)?),
            gate: self.gate.clone(),
            fail_read: self.fail_read.clone(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

impl TorrentStorage for ReadGatedStorage {
    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.inner.init(shared, metadata)
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        if self.gate.armed.swap(false, Ordering::SeqCst) {
            let entered = self.gate.entered.lock().take().unwrap();
            let release = self.gate.release.lock().take().unwrap();
            entered.send(()).unwrap();
            release.recv().unwrap();
        }
        anyhow::ensure!(
            !self.fail_read.swap(false, Ordering::SeqCst),
            "the read failed"
        );
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
        Ok(Box::new(ReadGatedStorage {
            inner: self.inner.take()?,
            gate: self.gate.clone(),
            fail_read: self.fail_read.clone(),
        }))
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.inner.on_piece_completed(piece)
    }
}

// Every block of piece 0 is written to its files on its connection's turn,
// and the read of its check errors: the check gives no verdict. The
// connection, which waited for the check's end, is ended for the error, and
// the torrent ends in error, as libtorrent's does for a hash job's error
// (`torrent::on_piece_verified`, `handle_disk_error`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_check_that_errors_ends_its_connection_and_the_torrent_in_error() {
    let storage = ReadGatedStorageFactory::default();
    let h = Harness::build_on(
        "check_errors",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    let (owner, mut owner_rx) = h.peer("127.0.0.1:6881");
    h.take(&owner, 0);
    let last = CHUNKS_PER_PIECE - 1;
    for chunk in 0..last {
        h.send(&owner, (0, chunk), false).await.unwrap();
    }
    storage.fail_read.store(true, Ordering::SeqCst);
    // The connection is told of the error as its own end, not by the wait.
    h.send(&owner, (0, last), false).await.unwrap();
    assert!(!storage.fail_read.load(Ordering::SeqCst), "the check read");
    let ended = loop {
        match owner_rx
            .recv()
            .await
            .expect("the peer's writer queue closed")
        {
            WriterRequest::Disconnect(ended) => break ended,
            _ => continue,
        }
    };
    assert!(ended.is_err());
    // No check passed, so piece 0 was never had. Its run's pieces are not
    // read here: the error stops the torrent on a task of its own, which
    // may already have ended the run and dropped them.
    let checks = h.live.session_stats.swarm.snapshot().piece_checks;
    assert_eq!(
        checks[crate::session_stats::swarm::PieceCheck::Passed as usize],
        0
    );
    let handle = h.handle();
    let error = loop {
        let changed = handle.next_state_change();
        let error = handle.with_state(|s| match s {
            crate::ManagedTorrentState::Error(e) => Some(format!("{e:#}")),
            _ => None,
        });
        if let Some(error) = error {
            break error;
        }
        changed.await;
    };
    assert!(error.contains("the read failed"), "{error}");
}

// Every block of both pieces is written; the last write of each takes the
// piece out of flight and queues its check on the hashing thread. The
// torrent pauses while the check of piece 0 reads the piece back, with the
// check of piece 1 queued behind it. The pause waits for the check under
// way, whose piece is had in the run that ends; the queued check never
// reads, and its piece goes back to the queue with no blocks, to be fetched
// again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pause_waits_for_the_check_under_way_and_drops_the_one_queued() {
    let storage = ReadGatedStorageFactory::default();
    let h = Harness::build_on(
        "pause_mid_check",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    h.take_every(&owner);
    let (mut entered, release) = storage.gate.arm();
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send_block(&owner, (0, chunk)).await;
    }
    // The check of piece 0 is reading it back.
    entered.recv().await.unwrap();
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send_block(&owner, (1, chunk)).await;
    }
    // Every block of piece 1 is written and its check waits for the thread.
    h.live.shared.spawner.disk().idle().await;
    assert_eq!(h.live.shared.spawner.stats().snapshot().hash_jobs_queued, 1);

    let pausing = h.pausing().await;
    assert!(!pausing.is_finished(), "the pause waits for the check");
    release.send(()).unwrap();
    let paused = pausing.await.unwrap();
    let run = h.live.shared.ending_run.lock().clone().unwrap();
    run.wait().await;

    let chunks = &paused.chunk_tracker;
    assert!(chunks.is_piece_have(h.piece(0)), "the check under way");
    assert!(!chunks.is_piece_have(h.piece(1)), "the check never ran");
    assert_eq!(
        chunks.received_chunks(h.piece(1)),
        0,
        "piece 1 kept its blocks as received and is not queued: stuck (queued: {:?})",
        chunks
            .iter_queued_pieces()
            .map(|p| p.get())
            .collect::<Vec<_>>()
    );
}

// The same pause where the storage holds its pieces in memory and keeps
// their room from one run to the next. The check of piece 0 is under way
// and ends as the run does: its piece is not written, for the run has ended
// by its turn of the write threads. The check of piece 1 never runs. Both
// pieces go back to the queue, and nothing of the run that ended touches the
// storage once it is handed on: the next run, granted the room again, lands
// its blocks in it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_that_ends_leaves_the_rooms_of_its_waiting_pieces_to_the_next() {
    let storage = HeldStorageFactory::default();
    let h = Harness::build_on(
        "pause_mid_memory_check",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    h.take_every(&owner);
    let (mut entered, release) = storage.gate.arm();
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&owner, (0, chunk), false).await.unwrap();
    }
    // The hashing thread holds piece 0's check.
    entered.recv().await.unwrap();
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&owner, (1, chunk), false).await.unwrap();
    }
    assert_eq!(h.live.shared.spawner.stats().snapshot().hash_jobs_queued, 1);

    let pausing = h.pausing().await;
    assert!(!pausing.is_finished(), "the pause waits for the check");
    release.send(()).unwrap();
    let paused = pausing.await.unwrap();
    // Handed on: the storage is as the run that ended left it.
    let held = storage.held.lock().clone();
    assert_eq!(held, [0, 1].into_iter().collect());

    // The next run is handed both pieces and granted room for them.
    let room = Arc::new(Notify::new());
    for index in 0..2 {
        let piece = h.piece(index);
        assert!(!paused.chunk_tracker.is_piece_have(piece));
        assert_eq!(paused.chunk_tracker.received_chunks(piece), 0);
        assert!(paused.files.reserve_piece(piece, &room));
    }
    // Every job of the run that ended is over.
    let run = h.live.shared.ending_run.lock().clone().unwrap();
    run.wait().await;
    h.settled().await;
    assert_eq!(storage.checked_on.lock().len(), 1, "the queued check ran");
    assert_eq!(*storage.held.lock(), held, "a job of the run that ended");
    // The next run's first block of each piece lands in the room granted.
    for index in 0..2 {
        let start = (index * PIECE_LENGTH) as usize;
        paused
            .files
            .pwrite_all(
                0,
                start as u64,
                &h.content[start..start + CHUNK_SIZE as usize],
            )
            .expect("the next run's block was refused: its room was taken");
    }
}

// The last block of piece 0 lands and takes the piece out of flight. What
// came of it is dropped before its check is queued, as a panic later in the
// same batch drops it (a later block's marking, or an earlier job's
// completion). The piece's check owns it from when it left flight, so the
// drop puts it back in the queue with no blocks: not stuck until a pause.
#[tokio::test]
async fn a_landed_piece_dropped_before_its_check_goes_back_to_the_queue() {
    let h = Harness::new("landed_dropped").await;
    let (owner, _owner_rx) = h.peer("127.0.0.1:6881");
    h.take(&owner, 0);
    let last = CHUNKS_PER_PIECE - 1;
    for chunk in 0..last {
        h.send(&owner, (0, chunk), false).await.unwrap();
    }
    let info = h.chunk(0, last);
    h.peers
        .with_live_mut(owner.addr, "test", |live| live.add_inflight_request(info))
        .unwrap();
    let chunk = owner
        .received_block(0, last * CHUNK_SIZE, CHUNK_SIZE as usize)
        .unwrap();
    let start = (last * CHUNK_SIZE) as usize;
    let block = h.content[start..start + CHUNK_SIZE as usize].to_vec();
    let landed = land_block(
        &h.live,
        owner.addr,
        &Piece::from_data(0, last * CHUNK_SIZE, &block[..]),
        &chunk,
    )
    .unwrap();
    assert!(landed.is_some(), "the last block completes piece 0");
    assert!(
        !h.queued_pieces().contains(&0),
        "out of the queue for its check"
    );

    drop(landed);
    assert!(h.queued_pieces().contains(&0), "piece 0 is stuck");
    assert_eq!(
        h.missing_chunks(0),
        (0..CHUNKS_PER_PIECE).collect::<Vec<_>>()
    );
}

// The check of piece 0 is reading the piece back when the last block of
// piece 1 is written. The blocks of piece 1 are written all the same; its
// check waits for the one hashing thread, and both pieces are had once the
// first check is let go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_piece_is_written_while_another_of_the_torrent_is_checked_and_checked_after_it() {
    let storage = ReadGatedStorageFactory::default();
    let h = Harness::build_on(
        "two_checks",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    // Both pieces, in whichever order they are picked.
    peer.on_i_am_unchoked();
    for _ in 0..PIECES {
        assert!(peer.acquire_next_piece().unwrap().piece().is_some());
    }
    let spawner = &h.live.shared.spawner;
    let (mut entered, release) = storage.gate.arm();
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send_block(&peer, (0, chunk)).await;
    }
    entered.recv().await.unwrap();
    assert_eq!(spawner.hash().wakes(), 1);

    for chunk in 0..CHUNKS_PER_PIECE {
        h.send_block(&peer, (1, chunk)).await;
    }
    // Every write is done, so the check of piece 1 is queued.
    spawner.disk().idle().await;
    assert!(h.missing_chunks(1).is_empty());
    assert_eq!(spawner.hash().at_work(), (1, 1));
    assert_eq!(spawner.hash().wakes(), 1);
    assert!(!h.have(0) && !h.have(1));

    release.send(()).unwrap();
    spawner.hash().idle().await;
    assert!(h.have(0) && h.have(1));
}

// An outgoing connection moves to its stream's runtime when it was opened
// on any other, a different uTP thread among them, and stays when it was
// opened there or has no stream thread.
#[test]
fn a_connection_moves_unless_it_runs_on_its_streams_runtime() {
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    };
    let (first, second) = (runtime(), runtime());
    let (first_home, second_home) = (first.handle().clone(), second.handle().clone());
    first.block_on(async {
        assert!(!runs_elsewhere(None));
        assert!(!runs_elsewhere(Some(&first_home)));
        assert!(runs_elsewhere(Some(&second_home)));
    });
    assert!(runs_elsewhere(Some(&first_home)));
}
