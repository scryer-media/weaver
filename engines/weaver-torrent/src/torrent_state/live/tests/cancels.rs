// SPDX-License-Identifier: GPL-3.0-or-later

//! A block asked of two peers: the copy that arrives first is written, and
//! the other peer it was asked of is told to cancel before the write, as
//! libtorrent `peer_connection::incoming_piece` calls `mark_as_writing`
//! and then `torrent::cancel_block` as it hands the block to its disk
//! threads. The storage reads what each watched peer was sent the moment
//! it writes a block, so the order is that of the peer's own queue.

use super::*;

/// What a watched peer's queue held, or the storage did, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    /// A peer was sent a Cancel for the block at (piece, offset).
    Cancel(&'static str, u32, u32),
    /// Any other message a peer was sent.
    Other(&'static str),
    /// The storage wrote the block at (piece, offset).
    Write(u32, u32),
}

#[derive(Default)]
struct Watch {
    peers: Vec<(&'static str, PeerRx)>,
    seen: Vec<Seen>,
}

impl Watch {
    // What each watched peer was sent since the last look.
    fn drain(&mut self) {
        let Self { peers, seen } = self;
        for (name, rx) in peers.iter_mut() {
            while let Ok(sent) = rx.try_recv() {
                seen.push(match sent {
                    WriterRequest::Message(Message::Cancel(r)) => {
                        Seen::Cancel(name, r.index, r.begin)
                    }
                    _ => Seen::Other(name),
                });
            }
        }
    }
}

/// A memory storage that, as it writes a block, first reads what the
/// watched peers were sent.
#[derive(Clone, Default)]
struct WatchedFactory {
    inner: MemoryStorageFactory,
    watch: Arc<Mutex<Watch>>,
}

struct Watched {
    inner: Box<dyn TorrentStorage>,
    watch: Arc<Mutex<Watch>>,
}

impl StorageFactory for WatchedFactory {
    type Storage = Watched;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<Watched> {
        Ok(Watched {
            inner: Box::new(self.inner.create(shared, metadata)?),
            watch: self.watch.clone(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

impl TorrentStorage for Watched {
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
        if !buf.is_empty() {
            let mut watch = self.watch.lock();
            watch.drain();
            let piece = u32::try_from(offset / u64::from(PIECE_LENGTH))?;
            let begin = u32::try_from(offset % u64::from(PIECE_LENGTH))?;
            watch.seen.push(Seen::Write(piece, begin));
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
        Ok(Box::new(Watched {
            inner: self.inner.take()?,
            watch: self.watch.clone(),
        }))
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.inner.on_piece_completed(piece)
    }
}

impl Harness {
    // The peer adds the request for `chunk` to its own, as its requester
    // does when it asks for it.
    fn asks(&self, peer: &PeerHandler, chunk: ChunkInfo) {
        assert_eq!(
            self.peers
                .with_live_mut(peer.addr, "test", |live| live.add_inflight_request(chunk)),
            Some(true)
        );
    }

    fn owned_by(&self, index: u32, peer: &PeerHandler) -> bool {
        self.live
            .lock_read("test")
            .get_pieces()
            .unwrap()
            .is_owned_by(self.piece(index), peer.addr)
    }
}

#[tokio::test]
async fn a_block_asked_of_two_peers_is_cancelled_with_the_other_before_it_is_written() {
    let storage = WatchedFactory::default();
    let h = Harness::build_on(
        "cancel_before_write",
        0x00,
        isolated_session_opts(false),
        storage.clone_box(),
    )
    .await;
    tokio::time::pause();
    let (owner, owner_rx) = h.peer("127.0.0.1:6881");
    let (doubler, doubler_rx) = h.peer("127.0.0.2:6881");
    // Each peer takes one of the two pieces: none is left queued, so the
    // end game is on.
    owner.on_i_am_unchoked();
    doubler.on_i_am_unchoked();
    let owned = owner.acquire_next_piece().unwrap().piece().unwrap().get();
    let other = doubler.acquire_next_piece().unwrap().piece().unwrap().get();
    assert_ne!(owned, other);
    assert!(h.owned_by(owned, &owner));
    assert!(h.owned_by(other, &doubler));

    // With nothing outstanding and no piece time to take a piece over by,
    // the doubler is handed one block of the owner's piece to ask for too.
    assert_eq!(
        doubler
            .acquire_next_piece()
            .unwrap()
            .piece()
            .map(|p| p.get()),
        Some(owned)
    );
    let doubled: Vec<ChunkInfo> = h
        .lock_read("test")
        .get_pieces()
        .unwrap()
        .chunks_to_request(h.piece(owned), doubler.addr)
        .collect();
    let [doubled] = doubled[..] else {
        panic!("one block doubled, got {doubled:?}");
    };
    // Both peers asked for it, and the owner for one more the doubler sends
    // unasked.
    let unasked = h.chunk(owned, (doubled.chunk_index + 1) % CHUNKS_PER_PIECE);
    h.asks(&owner, doubled);
    h.asks(&owner, unasked);
    h.asks(&doubler, doubled);
    {
        let mut watch = storage.watch.lock();
        watch.peers.push(("owner", owner_rx));
        watch.peers.push(("doubler", doubler_rx));
        watch.drain();
        watch.seen.clear();
    }

    // The owner's copy comes first: the doubler is told to cancel, and then
    // the block is written.
    h.deliver(&owner, (owned, doubled.offset)).await;
    // The doubler's copy, sent before the cancel reached it, is let through
    // as late and counted redundant, once.
    let redundant = h.get_redundant_bytes();
    h.deliver(&doubler, (owned, doubled.offset)).await;
    assert_eq!(h.get_redundant_bytes() - redundant, u64::from(CHUNK_SIZE));
    // A block the owner asked for and another peer sent unasked: the owner
    // is told to cancel, then it is written.
    h.deliver(&doubler, (owned, unasked.offset)).await;

    let mut watch = storage.watch.lock();
    watch.drain();
    assert_eq!(
        watch.seen,
        vec![
            Seen::Cancel("doubler", owned, doubled.offset),
            Seen::Write(owned, doubled.offset),
            Seen::Cancel("owner", owned, unasked.offset),
            Seen::Write(owned, unasked.offset),
        ]
    );
    drop(watch);
    // Neither peer is asked to cancel what it sent, and each is waiting for
    // nothing it was told to cancel.
    assert!(
        !h.peers
            .with_live(owner.addr, |live| live.has_inflight_request(&unasked))
            .unwrap()
    );
    assert!(
        !h.peers
            .with_live(doubler.addr, |live| live.has_inflight_request(&doubled))
            .unwrap()
    );
}
