// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

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

// libtorrent `peer_connection::start_receive_piece` queues a block never
// asked for as not wanted, and `incoming_piece` writes it unless the piece
// picker already holds it, counting it redundant then; the peer is kept
// either way. A block the piece lacks is used; the same block again is
// redundant.
#[tokio::test]
async fn a_block_never_asked_for_is_used_when_lacked_and_redundant_when_held() {
    let h = Harness::new("unasked_block").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let block = &h.content[..CHUNK_SIZE as usize];

    peer.on_received_piece(Piece::from_data(0, 0, block))
        .await
        .unwrap();
    assert!(!h.missing_chunks(0).contains(&0));
    assert_eq!(h.get_redundant_bytes(), 0);
    assert_eq!(
        h.stats.fetched_bytes.load(Ordering::Relaxed),
        u64::from(CHUNK_SIZE)
    );

    peer.on_received_piece(Piece::from_data(0, 0, block))
        .await
        .unwrap();
    assert_eq!(h.get_redundant_bytes(), u64::from(CHUNK_SIZE));
    assert!(h.peers.with_live(peer.addr, |_| ()).is_some());
}

// A connection that ends part way through a block drops the bytes of it
// that arrived: they count as redundant, and none as downloaded.
#[tokio::test]
async fn a_block_cut_short_by_the_connection_ending_counts_as_redundant() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const RECEIVED: usize = 1000;

    let h = Harness::new("partial_block_redundant").await;
    let (checked, slot, mut theirs) = h.incoming("192.0.2.1:6881");
    let added = h.live.add_incoming_peer(checked, slot, None).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));

    let mut frame = (9 + CHUNK_SIZE).to_be_bytes().to_vec();
    frame.push(7);
    frame.extend(0u32.to_be_bytes());
    frame.extend(0u32.to_be_bytes());
    frame.extend(vec![0xab; RECEIVED]);
    theirs.write_all(&frame).await.unwrap();
    theirs.shutdown().await.unwrap();
    // Our side closes once the connection has ended.
    let mut written = Vec::new();
    theirs.read_to_end(&mut written).await.unwrap();

    assert_eq!(h.get_redundant_bytes(), RECEIVED as u64);
    assert_eq!(h.stats.fetched_bytes.load(Ordering::Relaxed), 0);
}

// libtorrent `torrent::pause` closes each connection, and
// `peer_connection::disconnect` counts the part of a block that arrived as
// redundant, before the torrent stops: the bytes count in what the torrent
// moved, once, though the connection's task ends after the pause.
#[tokio::test]
async fn a_block_cut_short_by_a_pause_counts_as_redundant_once() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const RECEIVED: usize = 1000;

    let h = Harness::new("partial_block_pause").await;
    let (ours, mut theirs) = tokio::io::duplex(1 << 16);
    let (reader, writer, mut taken) = crate::peer_info_reader::tests::watched_halves(ours);
    let checked = CheckedIncomingConnection {
        kind: ConnectionKind::Tcp,
        addr: sock("192.0.2.1:6881"),
        reader,
        writer: writer.into(),
        read_buf: crate::read_buf::ReadBuf::new(),
        handshake: Handshake::new(h.shared.info_hash, Id20::new([7; 20])),
        encryption: crate::mse::Encryption::None,
        meter: Default::default(),
    };
    let slot = h._session.connection_slots.try_accept().unwrap();
    let added = h.live.add_incoming_peer(checked, slot, None).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));

    let mut frame = (9 + CHUNK_SIZE).to_be_bytes().to_vec();
    frame.push(7);
    frame.extend(0u32.to_be_bytes());
    frame.extend(0u32.to_be_bytes());
    frame.extend(vec![0xab; RECEIVED]);
    theirs.write_all(&frame).await.unwrap();
    taken.wait_for(|n| *n == frame.len()).await.unwrap();

    let _paused = h.live.pause().unwrap();
    // Our side closes once the connection's task has ended.
    let mut written = Vec::new();
    theirs.read_to_end(&mut written).await.unwrap();

    assert_eq!(h.shared.moved.lock().redundant_bytes, RECEIVED as u64);
    assert_eq!(h.get_redundant_bytes(), 0);
}

// libtorrent `torrent::do_pause` disconnects every connection, each counting
// what it drops (`peer_connection::disconnect`), before `stop_announcing`
// builds `stopped`. The run's end names its tasks before anything ends, and
// once they have ended the cut block is counted: what a stopped announce
// waits for.
#[tokio::test]
async fn the_end_of_a_run_waits_for_its_connections_to_count_the_block_they_were_cut_in() {
    use tokio::io::AsyncWriteExt;
    const RECEIVED: usize = 1000;

    let h = Harness::new("partial_block_ending_run").await;
    let (ours, mut theirs) = tokio::io::duplex(1 << 16);
    let (reader, writer, mut taken) = crate::peer_info_reader::tests::watched_halves(ours);
    let checked = CheckedIncomingConnection {
        kind: ConnectionKind::Tcp,
        addr: sock("192.0.2.1:6881"),
        reader,
        writer: writer.into(),
        read_buf: crate::read_buf::ReadBuf::new(),
        handshake: Handshake::new(h.shared.info_hash, Id20::new([7; 20])),
        encryption: crate::mse::Encryption::None,
        meter: Default::default(),
    };
    let slot = h._session.connection_slots.try_accept().unwrap();
    let added = h.live.add_incoming_peer(checked, slot, None).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));

    let mut frame = (9 + CHUNK_SIZE).to_be_bytes().to_vec();
    frame.push(7);
    frame.extend(0u32.to_be_bytes());
    frame.extend(0u32.to_be_bytes());
    frame.extend(vec![0xab; RECEIVED]);
    theirs.write_all(&frame).await.unwrap();
    taken.wait_for(|n| *n == frame.len()).await.unwrap();

    assert!(h.shared.ending_run.lock().is_none());
    let _paused = h.live.pause().unwrap();
    let ending = h.shared.ending_run.lock().clone().expect("the run's tasks");
    ending.wait().await;
    assert_eq!(h.shared.moved.lock().redundant_bytes, RECEIVED as u64);
    drop(theirs);
}

// libtorrent tells a tracker the payload received less the corrupt and
// redundant bytes (`torrent::announce_with_tracker`), not the pieces that
// passed their check: a torrent paused part way through a piece announces
// the blocks of it that it took, and neither the copy it already had nor
// the block a closing connection cut short.
#[tokio::test]
async fn a_torrent_paused_mid_piece_announces_the_payload_it_received() {
    use tokio::io::AsyncWriteExt;
    const RECEIVED: usize = 1000;

    let h = Harness::new("announced_mid_piece").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let chunk = CHUNK_SIZE as usize;
    for (begin, block) in [(0, 0..chunk), (CHUNK_SIZE, chunk..2 * chunk), (0, 0..chunk)] {
        peer.on_received_piece(Piece::from_data(0, begin, &h.content[block]))
            .await
            .unwrap();
    }

    let (ours, mut theirs) = tokio::io::duplex(1 << 16);
    let (reader, writer, mut taken) = crate::peer_info_reader::tests::watched_halves(ours);
    let checked = CheckedIncomingConnection {
        kind: ConnectionKind::Tcp,
        addr: sock("192.0.2.1:6881"),
        reader,
        writer: writer.into(),
        read_buf: crate::read_buf::ReadBuf::new(),
        handshake: Handshake::new(h.shared.info_hash, Id20::new([7; 20])),
        encryption: crate::mse::Encryption::None,
        meter: Default::default(),
    };
    let slot = h._session.connection_slots.try_accept().unwrap();
    let added = h.live.add_incoming_peer(checked, slot, None).unwrap();
    assert!(matches!(added, AddIncomingPeerResult::Added));
    let mut frame = (9 + CHUNK_SIZE).to_be_bytes().to_vec();
    frame.push(7);
    frame.extend(0u32.to_be_bytes());
    frame.extend((2 * CHUNK_SIZE).to_be_bytes());
    frame.extend(vec![0xab; RECEIVED]);
    theirs.write_all(&frame).await.unwrap();
    taken.wait_for(|n| *n == frame.len()).await.unwrap();

    let _paused = h.live.pause().unwrap();
    let ending = h.shared.ending_run.lock().clone().expect("the run's tasks");
    ending.wait().await;
    let totals = *h.shared.moved.lock();
    assert_eq!(totals.downloaded_bytes, 0);
    assert_eq!(totals.fetched_bytes, 3 * u64::from(CHUNK_SIZE));
    assert_eq!(
        totals.redundant_bytes,
        u64::from(CHUNK_SIZE) + RECEIVED as u64
    );
    assert_eq!(totals.announced_downloaded(), 2 * u64::from(CHUNK_SIZE));
    drop(theirs);
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

/// A memory storage that refuses the next `refusals` pieces it is handed,
/// then the next `no_room` for want of space, the next `read_only` as a
/// read-only file system does, and the next `no_room_writes` block writes
/// for want of space.
#[derive(Clone, Default)]
struct RefusingStorageFactory {
    inner: MemoryStorageFactory,
    refusals: Arc<AtomicU32>,
    no_room: Arc<AtomicU32>,
    read_only: Arc<AtomicU32>,
    no_room_writes: Arc<AtomicU32>,
}

struct RefusingStorage {
    inner: Box<dyn TorrentStorage>,
    refusals: Arc<AtomicU32>,
    no_room: Arc<AtomicU32>,
    read_only: Arc<AtomicU32>,
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
            read_only: self.read_only.clone(),
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
            read_only: self.read_only.clone(),
            no_room_writes: self.no_room_writes.clone(),
        }))
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        anyhow::ensure!(!take_one(&self.refusals), "refused piece {piece:?}");
        if take_one(&self.no_room) {
            return Err(no_room());
        }
        if take_one(&self.read_only) {
            return Err(anyhow::Error::from(std::io::Error::from(
                std::io::ErrorKind::ReadOnlyFilesystem,
            ))
            .context("writing"));
        }
        self.inner.on_piece_completed(piece)
    }
}

// A torrent added with `owner_wakes_full_disk` whose write is refused for
// any cause, a read-only file system here, waits for its owner, which
// fails the torrent: no retry spell of its own runs for a write its owner
// refuses.
#[tokio::test]
async fn an_owner_decides_a_write_refused_for_no_want_of_room() {
    let storage = RefusingStorageFactory::default();
    storage.read_only.store(1, Ordering::SeqCst);
    let add = AddTorrentOptions {
        storage_factory: Some(storage.clone_box()),
        owner_wakes_full_disk: true,
        ..Default::default()
    };
    let h = Harness::build_added(
        "owner_read_only",
        3,
        0x20,
        isolated_session_opts(false),
        add,
    )
    .await;
    tokio::time::pause();
    let (fetcher, _fetcher_rx) = h.peer_with("127.0.0.1:6881", &[0]).await;

    h.take(&fetcher, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&fetcher, (0, chunk), false).await.unwrap();
    }
    assert!(h.upload_mode());
    h.retry_declined
        .subscribe()
        .wait_for(|seen| *seen >= 1)
        .await
        .unwrap();
    tokio::time::advance(OPTIMISTIC_DISK_RETRY * 2).await;
    assert!(h.upload_mode());
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

// A torrent added with `owner_wakes_full_disk` whose write found no room
// stays in upload mode past `optimistic_disk_retry`, serving what it has,
// until its owner says room was made; then it asks again. Weaver's own: its
// server watches the disk the torrent writes to.
#[tokio::test]
async fn a_full_disk_its_owner_wakes_lasts_until_the_owner_ends_it() {
    let storage = RefusingStorageFactory::default();
    storage.no_room.store(1, Ordering::SeqCst);
    let add = AddTorrentOptions {
        storage_factory: Some(storage.clone_box()),
        owner_wakes_full_disk: true,
        ..Default::default()
    };
    // Three pieces, the last had.
    let h = Harness::build_added(
        "owner_full_disk",
        3,
        0x20,
        isolated_session_opts(false),
        add,
    )
    .await;
    tokio::time::pause();
    let (fetcher, _fetcher_rx) = h.peer_with("127.0.0.1:6881", &[0]).await;
    let (_asked, mut asked_rx) = h.peer_taking_upload_only("127.0.0.2:6881", &[1]).await;
    let (leecher, mut leecher_rx) = h.peer_with("127.0.0.3:6881", &[]).await;
    let leecher = &leecher;

    h.take(&fetcher, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&fetcher, (0, chunk), false).await.unwrap();
    }
    assert!(h.upload_mode());
    assert!(matches!(
        messages_sent(&mut asked_rx).pop(),
        Some(Message::Extended(ExtendedMessage::UploadOnly(true)))
    ));

    // The retry task saw the mode and left it to the owner: it waits for
    // the mode to change, so no time passing ends it. Twice the retry
    // passes and it stays in upload mode.
    h.retry_declined
        .subscribe()
        .wait_for(|seen| *seen >= 1)
        .await
        .unwrap();
    tokio::time::advance(OPTIMISTIC_DISK_RETRY * 2).await;
    assert!(h.upload_mode());
    assert!(messages_sent(&mut asked_rx).is_empty());

    // The piece it has is served all the while.
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

    // The owner ends it: the peers are told, and piece 0 is fetched again.
    h.leave_upload_mode();
    assert!(!h.upload_mode());
    assert!(matches!(
        messages_sent(&mut asked_rx).pop(),
        Some(Message::Extended(ExtendedMessage::UploadOnly(false)))
    ));
    h.take(&fetcher, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&fetcher, (0, chunk), false).await.unwrap();
    }
    assert!(h.have(0));
    assert!(!h.upload_mode());
}

// A refusal made before its owner left upload mode, and handled after,
// enters nothing: the wake stands, and the torrent fetches. One made after
// the leave enters upload mode as any refusal does. The order is driven by
// hand: the write's count is taken, the owner leaves, then the refusal is
// handled.
#[tokio::test]
async fn a_refusal_made_before_its_owner_left_upload_mode_does_not_enter_it_again() {
    let storage = RefusingStorageFactory::default();
    let add = AddTorrentOptions {
        storage_factory: Some(storage.clone_box()),
        owner_wakes_full_disk: true,
        ..Default::default()
    };
    let h = Harness::build_added("late_refusal", 3, 0x20, isolated_session_opts(false), add).await;
    let no_room = || anyhow::Error::from(std::io::Error::from(std::io::ErrorKind::StorageFull));

    // Pieces A and B are written at the same count; A's refusal enters
    // upload mode.
    let at = h.upload_epoch();
    assert!(h.handle_disk_error(&no_room(), at));
    assert!(h.upload_mode());
    // Room was made: the owner wakes it.
    h.leave_upload_mode();
    assert!(!h.upload_mode());
    // B's refusal lands now, from before the wake: it is still a refusal,
    // but it does not undo the wake.
    assert!(h.handle_disk_error(&no_room(), at));
    assert!(
        !h.upload_mode(),
        "a refusal from before the wake enters nothing"
    );
    // A write made after the wake that finds no room enters it again.
    assert!(h.handle_disk_error(&no_room(), h.upload_epoch()));
    assert!(h.upload_mode());
}

// libtorrent `torrent::set_upload_mode`: leaving upload mode clears every
// peer's `last_connected` to force a fast reconnect. A seed let go as of no
// use while we only uploaded waits a reconnect time; once upload mode ends
// the connect loop wakes and dials it at once, the clock not moved.
#[tokio::test]
async fn a_seed_let_go_in_upload_mode_is_dialled_at_once_when_it_ends() {
    let h = Harness::new("upload_mode_redial").await;
    tokio::time::pause();
    let (seed, mut seed_rx) = h.peer("127.0.0.1:6881");
    let mut hs = ExtendedHandshake::new();
    hs.p = Some(u32::from(seed.addr.addr().unwrap().port()));
    (&seed).on_extended_handshake(&hs).unwrap();
    h.enter_upload_mode(false, h.upload_epoch());
    assert!(h.upload_mode());

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

    let now = tokio::time::Instant::now();
    assert_eq!(
        h.peers
            .with_peer(sock("127.0.0.1:6881").into(), |p| p.reconnect_at())
            .unwrap(),
        Some(now + peer::MIN_RECONNECT_TIME)
    );
    assert!(h.peers.connect_next(now).is_none());
    let mut candidate = std::pin::pin!(h.peers.wait_for_candidate());
    assert!(candidate.as_mut().now_or_never().is_none());

    h.leave_upload_mode();
    assert!(candidate.as_mut().now_or_never().is_some());
    assert_eq!(tokio::time::Instant::now(), now);
    assert_eq!(
        h.peers.connect_next(now).map(|(addr, ..)| addr),
        Some(sock("127.0.0.1:6881"))
    );
}

// A seed let go as of no use while we only uploaded, its close heard only
// after upload mode was left: the leave found it still connected, so its
// close is due again at once, the clock not moved.
#[tokio::test]
async fn a_seed_let_go_in_upload_mode_and_closed_after_it_ends_is_dialled_at_once() {
    let h = Harness::new("upload_mode_late_close").await;
    tokio::time::pause();
    let (seed, mut seed_rx) = h.peer("127.0.0.1:6881");
    let mut hs = ExtendedHandshake::new();
    hs.p = Some(u32::from(seed.addr.addr().unwrap().port()));
    (&seed).on_extended_handshake(&hs).unwrap();
    h.enter_upload_mode(false, h.upload_epoch());

    let mut requester: Task<'_> = Box::pin(seed.request_chunks());
    loop {
        match next_queued(&mut requester, &mut seed_rx).await {
            WriterRequest::Disconnect(Ok(())) => break,
            WriterRequest::Message(_) => {}
            other => panic!("unexpected writer request {other:?}"),
        }
    }
    drop(requester);
    h.leave_upload_mode();
    seed.on_peer_died(Some(crate::Error::Disconnect)).unwrap();

    let now = tokio::time::Instant::now();
    assert_eq!(
        h.peers
            .with_peer(sock("127.0.0.1:6881").into(), |p| p.reconnect_at())
            .unwrap(),
        None
    );
    assert_eq!(
        h.peers.connect_next(now).map(|(addr, ..)| addr),
        Some(sock("127.0.0.1:6881"))
    );
}

// A piece is counted into every file with bytes in it, an empty file among
// them or not (libtorrent `torrent::we_have` updates every file the piece
// maps to).
#[test]
fn a_piece_is_counted_into_every_file_past_an_empty_one() {
    let lengths = Lengths::new(64, 16).unwrap();
    let files = files_laid_out(&lengths, &[16, 0, 8, 0, 30, 10]);
    let ranges: Vec<_> = files.iter().map(|f| f.piece_range.clone()).collect();
    assert_eq!(ranges, [0..1, 1..1, 1..2, 1..1, 1..4, 3..4]);
    let names = |piece| -> Vec<usize> {
        let piece = lengths.validate_piece_index(piece).unwrap();
        files_in_piece(&files, &lengths, piece)
            .map(|(i, _)| i)
            .collect()
    };
    assert_eq!(names(0), vec![0]);
    assert_eq!(names(1), vec![2, 3, 4]);
    assert_eq!(names(2), vec![4]);
    assert_eq!(names(3), vec![4, 5]);
}

/// Files of the lengths given, one after the other from the torrent's first
/// byte.
fn files_laid_out(lengths: &Lengths, lens: &[u64]) -> Vec<FileInfo> {
    let mut offset = 0;
    lens.iter()
        .enumerate()
        .map(|(i, &len)| {
            let file = FileInfo {
                relative_filename: format!("f{i}").into(),
                offset_in_torrent: offset,
                piece_range: lengths.iter_pieces_within_offset(offset, len),
                attrs: Default::default(),
                len,
            };
            offset += len;
            file
        })
        .collect()
}

// The files of a piece are found by halving. Over layouts of many files,
// empty ones, ones smaller than a piece and ones of many pieces among them,
// every piece has the files a walk from the first file finds.
#[test]
fn the_files_of_a_piece_found_by_halving_are_those_a_walk_finds() {
    fn walked(file_infos: &[FileInfo], piece: u32) -> Vec<usize> {
        file_infos
            .iter()
            .enumerate()
            .skip_while(|(_, fi)| fi.piece_range.end <= piece)
            .take_while(|(_, fi)| fi.piece_range.start <= piece)
            .map(|(i, _)| i)
            .collect()
    }
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        seed >> 33
    };
    for layout in 0..200 {
        let piece_length = [16, 64, 1024][layout % 3];
        let lens: Vec<u64> = (0..1 + next() % 60)
            .map(|_| match next() % 5 {
                0 => 0,
                1 => 1 + next() % u64::from(piece_length),
                2 => u64::from(piece_length),
                3 => u64::from(piece_length) * (1 + next() % 4),
                _ => next() % (u64::from(piece_length) * 6),
            })
            .collect();
        let total: u64 = lens.iter().sum();
        if total == 0 {
            continue;
        }
        let lengths = Lengths::new(total, piece_length).unwrap();
        let files = files_laid_out(&lengths, &lens);
        for piece in lengths.iter_piece_infos() {
            let piece = piece.piece_index;
            let halved: Vec<usize> = files_in_piece(&files, &lengths, piece)
                .map(|(i, _)| i)
                .collect();
            assert_eq!(
                halved,
                walked(&files, piece.get()),
                "piece {piece} of {lens:?} in pieces of {piece_length}"
            );
        }
    }
}

impl Harness {
    /// A live torrent of two files of one and a half pieces each, so piece
    /// 1 holds the end of the first and the start of the second, `have` the
    /// bitfield of the pieces had, on `storage`.
    async fn two_files(name: &str, have: u8, storage: &MemoryStorageFactory) -> Self {
        let size = (PIECE_LENGTH + PIECE_LENGTH / 2) as usize;
        let dir =
            crate::tests::test_util::create_default_random_dir_with_torrents(2, size, Some(name));
        let torrent = crate::create_torrent(
            dir.path(),
            crate::CreateTorrentOptions {
                piece_length: Some(PIECE_LENGTH),
                ..Default::default()
            },
            &crate::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .unwrap()
        .as_bytes()
        .unwrap();
        let torrent = TestTorrent {
            dir,
            torrent,
            content: Vec::new(),
        };
        let out = TempDir::with_prefix(name).unwrap();
        let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
            .await
            .unwrap();
        let add = AddTorrentOptions {
            storage_factory: Some(storage.clone_box()),
            ..Default::default()
        };
        let have = BF::from_boxed_slice(vec![have].into());
        let mut h = Self::adding(session, out, torrent, crate::Checkpoint::pieces(have), add).await;
        // The torrent's bytes in its files' order.
        let dir = h._torrent.as_ref().unwrap().dir.path().to_owned();
        let files = &h.live.metadata.file_infos;
        assert_eq!(files.len(), 2);
        h.content = files
            .iter()
            .flat_map(|file| {
                std::fs::read(dir.join(file.relative_filename.file_name().unwrap())).unwrap()
            })
            .collect();
        assert_eq!(h.content.len(), 2 * size);
        h
    }

    /// Every chunk of piece `index` sent by `peer`.
    async fn complete(&self, peer: &PeerHandler, index: u32) {
        // Pieces an earlier take acquired on the way are free again.
        self.lock_write("test")
            .get_pieces_mut()
            .unwrap()
            .release_pieces_owned_by(peer.addr);
        self.take(peer, index);
        for chunk in 0..CHUNKS_PER_PIECE {
            self.send(peer, (index, chunk), false).await.unwrap();
        }
        assert!(self.have(index));
    }
}

// The piece that makes a file whole tells the storage so, once; a piece
// that makes no file whole tells it nothing (libtorrent
// `file_progress::update` posts `file_completed_alert` as a file's
// progress reaches its size).
#[tokio::test]
async fn the_piece_that_makes_a_file_whole_tells_the_storage_once() {
    let storage = MemoryStorageFactory::default();
    let h = Harness::two_files("file_whole_once", 0x00, &storage).await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");

    h.complete(&peer, 0).await;
    assert_eq!(storage.log.completed_files(), Vec::<usize>::new());
    h.complete(&peer, 1).await;
    assert_eq!(storage.log.completed_files(), vec![0]);
    h.complete(&peer, 2).await;
    assert_eq!(storage.log.completed_files(), vec![0, 1]);
}

// A piece that ends one file and starts the next makes both whole at once
// when it is the last either lacked: both are told, in the files' order.
#[tokio::test]
async fn a_piece_that_makes_two_files_whole_tells_both() {
    let storage = MemoryStorageFactory::default();
    let h = Harness::two_files("two_files_whole", 0x00, &storage).await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");

    h.complete(&peer, 0).await;
    h.complete(&peer, 2).await;
    assert_eq!(storage.log.completed_files(), Vec::<usize>::new());
    h.complete(&peer, 1).await;
    assert_eq!(storage.log.completed_files(), vec![0, 1]);
}

// A file whole when the torrent starts, from what it had, is never told:
// only the file a piece of this run makes whole is.
#[tokio::test]
async fn a_file_whole_at_the_start_is_never_told() {
    let storage = MemoryStorageFactory::default();
    let h = Harness::two_files("file_whole_at_start", 0xc0, &storage).await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");

    h.complete(&peer, 2).await;
    assert_eq!(storage.log.completed_files(), vec![1]);
}
