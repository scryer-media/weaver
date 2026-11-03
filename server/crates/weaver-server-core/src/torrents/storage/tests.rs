// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Notify, watch};
use weaver_graph::NodeId;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant, HoldsNamespace};
use weaver_torrent::core::lengths::{Lengths, ValidPieceIndex};
use weaver_torrent::spawn_utils::BlockingSpawner;
use weaver_torrent::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerMode, ListenerOptions, Session,
    SessionOptions, create_torrent, torrent_from_bytes,
};

use super::{PieceStorage, PieceStorageFactory, Pieces};
use crate::torrents::files::Files;
use crate::torrents::intake::Holds;
use crate::torrents::layout::{PieceFile, PieceLayout, layout_of};
use crate::torrents::relocate::Existing;
use crate::torrents::service::{delete_written, move_listed};
use crate::torrents::store::{Kept, TorrentStore};
use weaver_torrent::storage::{
    BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage,
};

#[cfg(unix)]
mod bench;

fn accountant(resident_limit: u64) -> Arc<HoldsAccountant> {
    Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    }))
}

const NAMESPACE: HoldsNamespace = HoldsNamespace {
    scope: 1,
    node: NodeId::ROOT,
};

fn holds(accountant: &Arc<HoldsAccountant>) -> Holds {
    Holds {
        accountant: Arc::clone(accountant),
        namespace: NAMESPACE,
    }
}

fn unbounded() -> Holds {
    holds(&accountant(u64::MAX))
}

fn file(name: &str, length: u64) -> PieceFile {
    PieceFile {
        name: name.to_owned(),
        length,
        padding: false,
    }
}

// Bytes no format takes for its own.
fn noise(length: usize, seed: u8) -> Vec<u8> {
    (0..length)
        .map(|at| ((at as u32).wrapping_mul(2_654_435_761) >> 13) as u8 ^ seed | 0x80)
        .collect()
}

// The store of a torrent of plain files under `dir`, with what it kept.
fn store_in(layout: &Arc<PieceLayout>, dir: &Path, kept: Option<&Kept>) -> Arc<TorrentStore> {
    let files = Files::new(layout, dir.to_path_buf()).unwrap();
    Arc::new(TorrentStore::new(Arc::clone(layout), files, kept, None))
}

struct Shim {
    store: Arc<TorrentStore>,
    storage: PieceStorage,
    layout: Arc<PieceLayout>,
    dir: tempfile::TempDir,
    room: Arc<Notify>,
}

fn shim(piece_length: u64, files: Vec<PieceFile>) -> Shim {
    let layout = Arc::new(PieceLayout::new(piece_length, files).unwrap());
    let dir = tempfile::tempdir().unwrap();
    let store = store_in(&layout, dir.path(), None);
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&store) as Arc<dyn Pieces>,
        unbounded(),
    );
    Shim {
        store,
        storage,
        layout,
        dir,
        room: Arc::default(),
    }
}

impl Shim {
    fn index(&self, piece: u32) -> ValidPieceIndex {
        Lengths::new(
            self.layout.files().map(|file| file.length).sum(),
            self.layout.piece_length() as u32,
        )
        .unwrap()
        .try_validate_piece_index(piece)
        .unwrap()
    }

    // The piece's bytes written as the session writes them: granted room,
    // then file by file.
    fn write(&self, piece: u32, torrent: &[u8]) {
        assert!(self.storage.reserve_piece(self.index(piece), &self.room));
        let range = self.layout.piece_range(piece);
        for span in self.layout.spans(piece) {
            let at = (range.start + span.at) as usize;
            self.storage
                .pwrite_all(
                    span.file,
                    span.range.start,
                    &torrent[at..at + span.range.len() as usize],
                )
                .unwrap();
        }
    }

    // The piece read back as the session reads it to check or to seed it.
    fn read(&self, piece: u32) -> Vec<u8> {
        let mut out = Vec::new();
        for span in self.layout.spans(piece) {
            let mut buf = vec![0xee; span.range.len() as usize];
            self.storage
                .pread_exact(span.file, span.range.start, &mut buf)
                .unwrap();
            out.extend(buf);
        }
        out
    }

    fn complete(&self, piece: u32, torrent: &[u8]) {
        self.write(piece, torrent);
        self.storage.on_piece_completed(self.index(piece)).unwrap();
    }
}

fn three_files() -> (Shim, Vec<u8>) {
    let shim = shim(
        1024,
        vec![file("a.bin", 1000), file("b.bin", 2500), file("c.bin", 700)],
    );
    let torrent = noise(4200, 3);
    (shim, torrent)
}

#[test]
fn a_piece_in_flight_is_read_back_from_memory() {
    let (shim, torrent) = three_files();
    shim.write(0, &torrent);
    // Before it verifies the piece is the session's alone: the store has
    // none of it, and a read of it comes from the buffer.
    assert_eq!(shim.store.count(), 0);
    assert!(!shim.dir.path().join("a.bin").exists());
    assert_eq!(shim.read(0), torrent[..1024]);
    // A piece with nothing written is on no disk.
    let mut buf = [0; 10];
    assert!(shim.storage.pread_exact(1, 100, &mut buf).is_err());
    // A read across two pieces is read from the files, and piece 1 is not
    // there.
    assert!(shim.storage.pread_exact(1, 20, &mut buf).is_err());
    // Nor does a write cross a piece.
    assert!(shim.storage.pwrite_all(1, 20, &[0; 10]).is_err());
    // An empty read past the end of a file is nothing to do.
    shim.storage.pread_exact(0, 1000, &mut []).unwrap();
}

// libtorrent `disk_io_thread::async_write`: a verified piece is written to
// the torrent's listed files under its directory, and is had.
#[test]
fn a_verified_piece_is_written_to_the_torrents_files() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    assert_eq!(shim.store.count(), 5);
    assert_eq!(shim.store.file_have(), [1000, 2500, 700]);
    let dir = shim.dir.path();
    assert_eq!(std::fs::read(dir.join("a.bin")).unwrap(), torrent[..1000]);
    assert_eq!(
        std::fs::read(dir.join("b.bin")).unwrap(),
        torrent[1000..3500]
    );
    assert_eq!(std::fs::read(dir.join("c.bin")).unwrap(), torrent[3500..]);
    // A piece with nothing written did not verify.
    assert!(shim.storage.on_piece_completed(shim.index(0)).is_err());
}

// The bitfield the session starts from is the pieces the store kept, in
// the wire's bit order.
#[test]
fn a_kept_store_gives_the_session_the_pieces_it_has() {
    let (shim, torrent) = three_files();
    for piece in [0, 1, 2, 4] {
        shim.complete(piece, &torrent);
    }
    let kept = shim.store.kept();
    assert!(!kept.checking);
    let again = store_in(&shim.layout, shim.dir.path(), Some(&kept));
    let have = again.initial_have(false).unwrap();
    assert_eq!(have.len(), 5);
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 1, 2, 4]);
    // Piece 0 is the high bit of the first byte.
    assert_eq!(have.as_raw_slice()[0], 0b1110_1000);
    assert_eq!(again.file_have(), shim.store.file_have());
    // A store with none of files that were placed vouches for nothing: the
    // session checks them.
    let placed = store_in(&shim.layout, shim.dir.path(), None);
    assert!(placed.initial_have(true).is_none());
    assert_eq!(placed.initial_have(false).unwrap().count_ones(), 0);
}

// libtorrent `torrent::force_recheck`: once a check of every piece began, a
// run stopped before it gave every piece its verdict has only what it found,
// and checks every piece again.
#[test]
fn a_check_stopped_part_way_is_kept_to_check_again() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    shim.storage.on_check_everything().unwrap();
    assert_eq!(shim.store.count(), 0);
    shim.storage.on_piece_checked(shim.index(0)).unwrap();
    shim.storage.on_piece_hash_failed(shim.index(1)).unwrap();
    assert_eq!(shim.store.count(), 1);
    let kept = shim.store.kept();
    assert!(kept.checking);
    let again = store_in(&shim.layout, shim.dir.path(), Some(&kept));
    assert!(again.initial_have(false).is_none());
    // It has what the check found, and checks every piece again.
    assert_eq!(again.count(), 1);
    assert!(again.has(0));
    for piece in 2..5 {
        shim.storage.on_piece_checked(shim.index(piece)).unwrap();
    }
    let kept = shim.store.kept();
    assert!(!kept.checking);
    let have = store_in(&shim.layout, shim.dir.path(), Some(&kept))
        .initial_have(false)
        .unwrap();
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 2, 3, 4]);
}

#[test]
fn a_verified_piece_seeds_from_the_files_it_was_written_to() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    for piece in 0..5 {
        let range = shim.layout.piece_range(piece);
        assert_eq!(
            shim.read(piece),
            torrent[range.start as usize..range.end as usize],
            "piece {piece}"
        );
    }
}

// libtorrent `torrent::on_piece_hashed` has only the pieces that pass: a
// piece that then fails its hash is had no longer, reads as the file on
// disk holds it, as libtorrent's storage reads whatever the bitfield says,
// and fetched again is had again.
#[test]
fn a_failed_piece_is_had_no_longer_until_fetched_again() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    shim.storage.on_piece_hash_failed(shim.index(2)).unwrap();
    assert!(!shim.store.has(2));
    assert_eq!(shim.store.count(), 4);
    assert_eq!(shim.store.file_have(), [1000, 2500 - 1024, 700]);
    let range = shim.layout.piece_range(2);
    assert_eq!(
        shim.read(2),
        torrent[range.start as usize..range.end as usize]
    );
    shim.complete(2, &torrent);
    assert_eq!(shim.store.count(), 5);
}

// libtorrent `storage_utils::delete_files`: deleting a torrent's files
// removes them, here with what unpacking put beside them, and then each
// directory that leaves empty. A file the operator put beside them stays,
// and so does its directory until it is empty.
#[test]
fn deleting_a_torrents_files_removes_them_and_its_output_and_nothing_else() {
    let shim = shim(1024, vec![file("sub/a.bin", 1000), file("b.bin", 500)]);
    let torrent = noise(1500, 4);
    for piece in 0..2 {
        shim.complete(piece, &torrent);
    }
    let dir = shim.dir.path().to_path_buf();
    std::fs::create_dir_all(dir.join("out")).unwrap();
    std::fs::write(dir.join("out/member.txt"), b"unpacked").unwrap();
    std::fs::write(dir.join("notes.txt"), b"mine").unwrap();
    shim.store.forget();

    let mut written = shim.store.paths();
    written.push(dir.join("out/member.txt"));
    delete_written(&dir, &written);
    let left: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(left, ["notes.txt"]);

    std::fs::remove_file(dir.join("notes.txt")).unwrap();
    delete_written(&dir, &written);
    assert!(!dir.exists());
}

// libtorrent `torrent::on_piece_hashed`: a piece the check found on disk is
// had (`we_have`) and nothing is written. Each piece adopted from where the
// files were placed is had, and the file keeps the bytes it had; one its
// hold refuses, seed mode having ended, is not.
#[test]
fn an_adopted_piece_is_had_and_not_written_back() {
    let shim = shim(1024, vec![file("a.bin", 3000)]);
    let on_disk = noise(3000, 5);
    std::fs::write(shim.dir.path().join("a.bin"), &on_disk).unwrap();
    assert!(shim.store.adopt(0, |_| None).is_none());
    assert!(!shim.store.has(0));
    for piece in 0..shim.layout.piece_count() {
        shim.store
            .adopt(piece, |take| Some(take()))
            .unwrap()
            .unwrap();
        assert!(shim.store.has(piece), "piece {piece}");
    }
    assert_eq!(shim.store.count(), 3);
    assert_eq!(
        std::fs::read(shim.dir.path().join("a.bin")).unwrap(),
        on_disk
    );
}

// libtorrent `torrent::move_storage`: a torrent's files move from where they
// sit to the new save path, and the torrent goes on from there. Files move
// part-written, a file not yet begun has nothing to move, a piece had is
// served from the new place, and the pieces still to come are written
// there.
#[test]
fn files_move_part_written_and_the_torrent_goes_on_from_there() {
    let shim = shim(
        1024,
        vec![
            file("a.bin", 1000),
            file("sub/b.bin", 2500),
            file("c.bin", 700),
        ],
    );
    let torrent = noise(4200, 7);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let placed = shim.dir.path().to_path_buf();
    let moved_dir = tempfile::tempdir().unwrap();
    let moved = moved_dir.path().join("moved");
    assert!(placed.join("sub/b.bin").exists());
    assert!(!placed.join("c.bin").exists());
    std::fs::create_dir_all(&moved).unwrap();

    let (to, kept) = move_listed(shim.store.files(), &[], moved.clone(), Existing::Keep).unwrap();
    assert_eq!((to, kept), (moved.clone(), false));
    assert!(!placed.join("a.bin").exists());
    assert!(!placed.join("sub").exists());
    assert_eq!(std::fs::read(moved.join("a.bin")).unwrap(), torrent[..1000]);
    assert_eq!(shim.read(0), torrent[..1024]);
    assert_eq!(shim.store.files().path(2), Some(moved.join("c.bin")));

    for piece in 2..5 {
        shim.complete(piece, &torrent);
    }
    assert_eq!(std::fs::read(moved.join("a.bin")).unwrap(), torrent[..1000]);
    assert_eq!(
        std::fs::read(moved.join("sub/b.bin")).unwrap(),
        torrent[1000..3500]
    );
    assert_eq!(std::fs::read(moved.join("c.bin")).unwrap(), torrent[3500..]);
    assert_eq!(std::fs::read_dir(&placed).unwrap().count(), 0);
}

// libtorrent `move_flags_t::dont_replace`: a file already at the new place
// stays, the torrent's own copy of it stays where it was, the other files
// move, and the store reads every file from the new place; the move tells
// that it kept one.
#[test]
fn a_file_already_where_the_files_go_is_kept() {
    let shim = shim(1024, vec![file("a.bin", 1000), file("b.bin", 1000)]);
    let torrent = noise(2000, 8);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let placed = shim.dir.path().to_path_buf();
    let moved_dir = tempfile::tempdir().unwrap();
    let moved = moved_dir.path().to_path_buf();
    std::fs::write(moved.join("b.bin"), b"theirs").unwrap();

    let (to, kept) = move_listed(shim.store.files(), &[], moved.clone(), Existing::Keep).unwrap();
    assert_eq!((to, kept), (moved.clone(), true));
    assert_eq!(std::fs::read(moved.join("a.bin")).unwrap(), torrent[..1000]);
    assert!(!placed.join("a.bin").exists());
    assert_eq!(std::fs::read(moved.join("b.bin")).unwrap(), b"theirs");
    assert_eq!(
        std::fs::read(placed.join("b.bin")).unwrap(),
        torrent[1000..]
    );
    assert_eq!(
        [0, 1].map(|file| shim.store.files().path(file)),
        [Some(moved.join("a.bin")), Some(moved.join("b.bin"))]
    );
}

// A file is read through one shared handle, which never outlives a move of
// the files: once they move, a read opens the file where it is now, not
// the one the handle was opened on (libtorrent `file_pool`, which
// `storage::move_storage` clears).
#[test]
fn a_file_is_read_through_one_handle_that_does_not_follow_a_move() {
    let shim = shim(1024, vec![file("a.bin", 1000), file("b.bin", 1000)]);
    let torrent = noise(2000, 9);
    shim.complete(0, &torrent);
    shim.complete(1, &torrent);
    let mut buf = vec![0; 1000];
    shim.store.files().read(0, 0, &mut buf).unwrap();
    assert_eq!(buf, torrent[..1000]);
    shim.store.files().read(0, 0, &mut buf).unwrap();
    assert_eq!(buf, torrent[..1000]);

    let moved_dir = tempfile::tempdir().unwrap();
    let moved = moved_dir.path().to_path_buf();
    move_listed(shim.store.files(), &[], moved.clone(), Existing::Keep).unwrap();
    // Another file now stands where a.bin went: a handle kept from before
    // the move would still read the old one.
    std::fs::remove_file(moved.join("a.bin")).unwrap();
    std::fs::write(moved.join("a.bin"), vec![0x5a; 1000]).unwrap();
    shim.store.files().read(0, 0, &mut buf).unwrap();
    assert_eq!(buf, vec![0x5a; 1000]);
}

// The torrent's store, telling the test when every piece is had.
struct Watched {
    store: Arc<TorrentStore>,
    whole: watch::Sender<bool>,
}

impl Watched {
    fn tell(&self) {
        self.whole
            .send_replace(self.store.count() == self.store.layout().piece_count());
    }
}

impl Pieces for Watched {
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        self.store.verified(piece, held)?;
        self.tell();
        Ok(())
    }

    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        let held = self.store.failed(piece)?;
        self.tell();
        Ok(held)
    }

    fn checking(&self) -> anyhow::Result<bool> {
        let had = self.store.checking()?;
        self.tell();
        Ok(had)
    }

    fn found(&self, piece: u32) -> anyhow::Result<bool> {
        let found = self.store.found(piece)?;
        self.tell();
        Ok(found)
    }

    fn read(&self, file: usize, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.store.read(file, offset, out)
    }

    fn grow(&self, file: usize, length: u64) -> std::io::Result<()> {
        self.store.grow(file, length)
    }
}

// A session on loopback alone: no DHT, no trackers, no discovery, and
// `blocking` threads for writing and checking pieces.
async fn session(dir: &Path, blocking: Option<usize>) -> Arc<Session> {
    Session::new_with_opts(
        dir.to_path_buf(),
        SessionOptions {
            runtime_worker_threads: blocking,
            dht: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            persistence: None,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

// A torrent of three files, seeded by a session of its own.
struct Seeded {
    originals: [(&'static str, Vec<u8>); 3],
    bytes: bytes::Bytes,
    layout: Arc<PieceLayout>,
    seeder: Arc<Session>,
    peer: SocketAddr,
    _dir: tempfile::TempDir,
}

async fn seeded() -> Seeded {
    let dir = tempfile::tempdir().unwrap();
    let set = dir.path().join("set");
    std::fs::create_dir(&set).unwrap();
    let originals = [
        ("a.bin", noise(1000, 1)),
        ("b.bin", noise(2500, 2)),
        ("c.bin", noise(700, 3)),
    ];
    for (name, bytes) in &originals {
        std::fs::write(set.join(name), bytes).unwrap();
    }
    let created = create_torrent(
        &set,
        CreateTorrentOptions {
            name: None,
            trackers: Vec::new(),
            piece_length: Some(1024),
            ssl_cert: None,
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();
    let bytes = created.as_bytes().unwrap();
    let metainfo = torrent_from_bytes(&bytes).unwrap();
    let info = metainfo.info.data.validate().unwrap();
    let layout = Arc::new(layout_of(&info).unwrap());
    assert!(layout.piece_count() > 3);

    let seeder = session(&dir.path().join("session"), None).await;
    let seeding = seeder
        .add_torrent(
            AddTorrent::from_bytes(bytes.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(set.to_string_lossy().into_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    seeding.wait_until_completed().await.unwrap();
    let peer = seeder.listen_addr().unwrap();
    Seeded {
        originals,
        bytes,
        layout,
        seeder,
        peer,
        _dir: dir,
    }
}

// The torrent's store over the layout under `dir`, with the side the
// storage hands pieces to.
fn watched_store(layout: &Arc<PieceLayout>, dir: &Path) -> (Arc<Watched>, watch::Receiver<bool>) {
    let (whole, in_hand) = watch::channel(false);
    (
        Arc::new(Watched {
            store: store_in(layout, dir, None),
            whole,
        }),
        in_hand,
    )
}

// The seeded torrent fetched from its seeder alone, into `factory`.
async fn fetch(
    seeded: &Seeded,
    dir: &Path,
    blocking: Option<usize>,
    factory: BoxStorageFactory,
) -> Arc<Session> {
    let leecher = session(&dir.join("session"), blocking).await;
    let _fetching = leecher
        .add_torrent(
            AddTorrent::from_bytes(seeded.bytes.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(dir.join("unused").to_string_lossy().into_owned()),
                overwrite: true,
                initial_peers: Some(vec![seeded.peer]),
                storage_factory: Some(factory),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    leecher
}

// One session seeds a torrent from its files; the other fetches it from
// that peer alone into its own files through the piece storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_fetched_from_a_loopback_peer_lands_in_its_files_and_seeds_from_them() {
    let seeded = seeded().await;
    let Seeded {
        originals, layout, ..
    } = &seeded;
    let fetch_dir = tempfile::tempdir().unwrap();
    let (watched, mut in_hand) = watched_store(layout, &fetch_dir.path().join("job"));
    let factory = PieceStorageFactory::new(
        Arc::clone(layout),
        Arc::clone(&watched) as Arc<dyn Pieces>,
        unbounded(),
    );
    let leecher = fetch(&seeded, fetch_dir.path(), None, factory.boxed_factory()).await;
    in_hand.wait_for(|whole| *whole).await.unwrap();

    let dir = fetch_dir.path().join("job");
    let mut torrent = Vec::new();
    for (name, original) in originals {
        assert_eq!(&std::fs::read(dir.join(name)).unwrap(), original, "{name}");
    }
    for index in 0..layout.files().count() {
        let name = &layout.files().nth(index).unwrap().name;
        let (_, original) = originals.iter().find(|(file, _)| file == name).unwrap();
        torrent.extend_from_slice(original);
    }
    let posted = PieceStorage::new(Arc::clone(layout), watched as Arc<dyn Pieces>, unbounded());
    for piece in 0..layout.piece_count() {
        let range = layout.piece_range(piece);
        let mut read = Vec::new();
        for span in layout.spans(piece) {
            let mut buf = vec![0xee; span.range.len() as usize];
            posted
                .pread_exact(span.file, span.range.start, &mut buf)
                .unwrap();
            read.extend(buf);
        }
        assert_eq!(
            read,
            torrent[range.start as usize..range.end as usize],
            "piece {piece}"
        );
    }
    leecher.stop().await;
    seeded.seeder.stop().await;
}

// The piece storage as the session sees it, with the first verified piece
// held back until the test lets it go, and every refusal of room counted.
#[derive(Clone)]
struct Gated {
    storage: PieceStorage,
    gate: Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
    refused: Arc<watch::Sender<usize>>,
    // The pieces granted room, in order.
    granted: Arc<std::sync::Mutex<Vec<u32>>>,
}

impl TorrentStorage for Gated {
    fn init(
        &mut self,
        shared: &weaver_torrent::ManagedTorrentShared,
        metadata: &weaver_torrent::TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.storage.init(shared, metadata)
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.storage.pread_exact(file_id, offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.storage.pwrite_all(file_id, offset, buf)
    }

    fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
        self.storage.remove_file(file_id, filename)
    }

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
        self.storage.remove_directory_if_empty(path)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.storage.ensure_file_length(file_id, length)
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(self.clone()))
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.recv().unwrap();
        }
        self.storage.on_piece_completed(piece)
    }

    fn on_piece_hash_failed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.storage.on_piece_hash_failed(piece)
    }

    fn on_check_everything(&self) -> anyhow::Result<()> {
        self.storage.on_check_everything()
    }

    fn on_piece_checked(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.storage.on_piece_checked(piece)
    }

    fn reserve_piece(&self, piece: ValidPieceIndex, room: &Arc<Notify>) -> bool {
        let granted = self.storage.reserve_piece(piece, room);
        if granted {
            self.granted.lock().unwrap().push(piece.get());
        } else {
            self.refused.send_modify(|refused| *refused += 1);
        }
        granted
    }
}

#[derive(Clone)]
struct GatedFactory {
    factory: PieceStorageFactory,
    gate: Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
    refused: Arc<watch::Sender<usize>>,
    // The pieces granted room, in order.
    granted: Arc<std::sync::Mutex<Vec<u32>>>,
}

impl StorageFactory for GatedFactory {
    type Storage = Gated;

    fn create(
        &self,
        shared: &weaver_torrent::ManagedTorrentShared,
        metadata: &weaver_torrent::TorrentMetadata,
    ) -> anyhow::Result<Gated> {
        Ok(Gated {
            storage: self.factory.create(shared, metadata)?,
            gate: Arc::clone(&self.gate),
            refused: Arc::clone(&self.refused),
            granted: Arc::clone(&self.granted),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

// With room for one piece and one thread for writing and checking, the
// session is granted a piece, and while that piece is held at its handover
// with the one thread, the next is refused: the session waits without a
// thread of its own. Once the piece lets its room go the session is rung
// and fetches the rest a piece at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_at_its_budget_waits_for_room_and_resumes_when_a_piece_lets_go() {
    let seeded = seeded().await;
    let layout = &seeded.layout;
    let fetch_dir = tempfile::tempdir().unwrap();
    let (watched, mut in_hand) = watched_store(layout, &fetch_dir.path().join("job"));
    let budget = accountant(layout.piece_length());
    let (go, gate) = std::sync::mpsc::channel();
    let (refused, mut refusals) = watch::channel(0);
    let granted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let factory = GatedFactory {
        factory: PieceStorageFactory::new(
            Arc::clone(layout),
            Arc::clone(&watched) as Arc<dyn Pieces>,
            holds(&budget),
        ),
        gate: Arc::new(std::sync::Mutex::new(Some(gate))),
        refused: Arc::new(refused),
        granted: Arc::clone(&granted),
    };
    let leecher = fetch(&seeded, fetch_dir.path(), Some(1), factory.boxed()).await;

    refusals.wait_for(|refused| *refused > 0).await.unwrap();
    assert_eq!(watched.store.count(), 0);
    // The one piece granted is resident, whichever the picker started from.
    let first = granted.lock().unwrap()[0];
    assert_eq!(
        budget.namespace_usage(NAMESPACE).resident,
        layout.piece_range(first).len()
    );
    go.send(()).unwrap();

    in_hand.wait_for(|whole| *whole).await.unwrap();
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    leecher.stop().await;
    seeded.seeder.stop().await;
}

// A piece that fails its hash is dropped with the room it held, the session
// waiting for that room is rung, the store is told, and the storage going
// gives back the rest.
#[tokio::test]
async fn a_piece_that_fails_its_hash_lets_go_of_its_bytes_and_its_room() {
    let layout = Arc::new(PieceLayout::new(8, vec![file("a", 16)]).unwrap());
    let budget = accountant(8);
    let recorder = Arc::new(Recorder::default());
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&recorder) as Arc<dyn Pieces>,
        holds(&budget),
    );
    let lengths = Lengths::new(16, 8).unwrap();
    let [first, second] = [0, 1].map(|piece| lengths.try_validate_piece_index(piece).unwrap());
    let room = Arc::new(Notify::new());
    assert!(storage.reserve_piece(first, &room));
    storage.pwrite_all(0, 0, b"badbytes").unwrap();
    // The budget holds one piece, and nothing is written without room.
    assert!(!storage.reserve_piece(second, &room));
    assert!(storage.pwrite_all(0, 8, b"x").is_err());

    let rung = room.notified();
    storage.on_piece_hash_failed(first).unwrap();
    rung.await;
    assert_eq!(*recorder.failed.lock().unwrap(), [0]);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    assert!(storage.pread_exact(0, 0, &mut [0; 8]).is_err());

    assert!(storage.reserve_piece(second, &room));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 8);
    drop(storage);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
}

// The store's side that keeps what it is handed, reads the piece back
// through the storage while it is being handed over, and the pieces it was
// told failed. It has nothing on disk.
#[derive(Default)]
struct Recorder {
    storage: std::sync::OnceLock<PieceStorage>,
    handed: std::sync::Mutex<Vec<(u32, Bytes)>>,
    read_while_handed: std::sync::Mutex<Vec<u8>>,
    failed: std::sync::Mutex<Vec<u32>>,
}

impl Pieces for Recorder {
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        let storage = self.storage.get().unwrap();
        let mut read = self.read_while_handed.lock().unwrap();
        for (file, length) in [(0, 5), (1, 3)] {
            let mut buf = vec![0; length];
            storage.pread_exact(file, 0, &mut buf)?;
            read.extend(buf);
        }
        self.handed.lock().unwrap().push((piece, held.clone()));
        Ok(())
    }

    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        self.failed.lock().unwrap().push(piece);
        Ok(false)
    }

    fn checking(&self) -> anyhow::Result<bool> {
        Ok(false)
    }

    fn found(&self, _: u32) -> anyhow::Result<bool> {
        Ok(false)
    }

    fn read(&self, _: usize, _: u64, _: &mut [u8]) -> std::io::Result<()> {
        Err(std::io::ErrorKind::NotFound.into())
    }

    fn grow(&self, _: usize, _: u64) -> std::io::Result<()> {
        Ok(())
    }
}

// A verified piece goes to the store as the one buffer its blocks landed
// in and its hash was taken over: the bytes hashed and the bytes handed are
// the same memory, so nothing is copied between the check and the disk,
// and the piece reads from memory until the store has it.
#[test]
fn a_verified_piece_goes_to_the_store_as_the_buffer_it_was_hashed_in() {
    let layout = Arc::new(PieceLayout::new(8, vec![file("a", 5), file("b", 3)]).unwrap());
    let recorder = Arc::new(Recorder::default());
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&recorder) as Arc<dyn Pieces>,
        unbounded(),
    );
    recorder.storage.set(storage.clone()).ok().unwrap();
    let index = Lengths::new(8, 8)
        .unwrap()
        .try_validate_piece_index(0)
        .unwrap();
    assert!(storage.reserve_piece(index, &Arc::default()));
    storage.pwrite_all(0, 0, b"aaaaa").unwrap();
    storage.pwrite_all(1, 0, b"bbb").unwrap();
    let mut hashed = Vec::new();
    assert!(storage.hash_held_piece(index, &mut |bytes| hashed.push(bytes.as_ptr() as usize)));
    storage.on_piece_completed(index).unwrap();

    assert_eq!(*recorder.read_while_handed.lock().unwrap(), b"aaaaabbb");
    let handed = recorder.handed.lock().unwrap();
    assert_eq!(handed.len(), 1);
    let (piece, held) = &handed[0];
    assert_eq!((*piece, &held[..]), (0, &b"aaaaabbb"[..]));
    assert_eq!(hashed, [held.as_ptr() as usize]);
    // Once handed over, the piece is no longer in memory.
    assert!(storage.pread_exact(0, 0, &mut [0; 5]).is_err());
}

// A piece handed on to another peer is written into the room it already
// holds: a steal is never asked for room, and a piece its peer gave back is
// granted again without more. So no write the session makes after a grant
// finds the piece without room.
#[test]
fn a_piece_handed_on_is_written_into_the_room_it_holds() {
    let layout = Arc::new(PieceLayout::new(8, vec![file("a", 5), file("b", 3)]).unwrap());
    let budget = accountant(8);
    let recorder = Arc::new(Recorder::default());
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&recorder) as Arc<dyn Pieces>,
        holds(&budget),
    );
    recorder.storage.set(storage.clone()).ok().unwrap();
    let piece = Lengths::new(8, 8)
        .unwrap()
        .try_validate_piece_index(0)
        .unwrap();
    let room = Arc::new(Notify::new());
    assert!(storage.reserve_piece(piece, &room));
    // The first peer writes half, then the piece is stolen: the thief
    // writes the rest with nothing asked.
    storage.pwrite_all(0, 0, b"abcde").unwrap();
    storage.pwrite_all(1, 0, b"fgh").unwrap();
    // The thief leaves and the piece is queued again: the next peer is
    // granted it with the budget already full, and rewrites it.
    assert!(storage.reserve_piece(piece, &room));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 8);
    storage.pwrite_all(0, 0, b"ab").unwrap();
    storage.on_piece_completed(piece).unwrap();
    let handed = recorder.handed.lock().unwrap();
    assert_eq!(&handed[0].1[..], b"abcdefgh");
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
}

// A whole piece is hashed where its blocks landed: the bytes handed are the
// piece as a read back would find it, across the files it spans, and a
// piece not in memory hands nothing, so the session reads it instead.
#[test]
fn a_piece_in_memory_is_handed_whole_for_hashing_and_one_not_is_not() {
    let shim = shim(1024, vec![file("a.bin", 1000), file("b.bin", 1500)]);
    let torrent = noise(2500, 10);
    assert!(
        !shim
            .storage
            .hash_held_piece(shim.index(0), &mut |_| panic!("nothing held"))
    );
    shim.write(0, &torrent);
    let mut handed = Vec::new();
    assert!(
        shim.storage
            .hash_held_piece(shim.index(0), &mut |bytes| handed.extend_from_slice(bytes))
    );
    assert_eq!(handed, torrent[..1024]);
    assert_eq!(handed, shim.read(0));
    shim.storage.on_piece_completed(shim.index(0)).unwrap();
    assert!(
        !shim
            .storage
            .hash_held_piece(shim.index(0), &mut |_| panic!("piece left memory"))
    );
    assert!(shim.storage.pwrite_all(0, 0, &torrent[..16]).is_err());
}

// A torrent of plain files has no tee: nothing of it reaches the graph. Each
// piece is hashed once, from the buffer its blocks landed in, and written
// once, to its files; the store reads nothing of the files to have it.
#[test]
fn a_plain_torrent_is_hashed_once_and_written_once_and_never_reaches_the_graph() {
    let (shim, torrent) = three_files();
    assert!(crate::torrents::tee::Plan::of(&shim.layout).is_none());
    assert!(shim.store.tee().is_none());
    let counted = Arc::new(Counted {
        store: Arc::clone(&shim.store),
        verified: Default::default(),
        reads: Default::default(),
    });
    let storage = PieceStorage::new(
        Arc::clone(&shim.layout),
        Arc::clone(&counted) as Arc<dyn Pieces>,
        unbounded(),
    );
    let mut hashed = 0;
    for piece in 0..5 {
        let index = shim.index(piece);
        assert!(storage.reserve_piece(index, &shim.room));
        let range = shim.layout.piece_range(piece);
        for span in shim.layout.spans(piece) {
            let at = (range.start + span.at) as usize;
            storage
                .pwrite_all(
                    span.file,
                    span.range.start,
                    &torrent[at..at + span.range.len() as usize],
                )
                .unwrap();
        }
        assert!(storage.hash_held_piece(index, &mut |bytes| hashed += bytes.len()));
        storage.on_piece_completed(index).unwrap();
    }
    assert_eq!(hashed, torrent.len());
    assert_eq!(*counted.verified.lock().unwrap(), torrent.len());
    assert_eq!(*counted.reads.lock().unwrap(), 0);
    let on_disk: u64 = ["a.bin", "b.bin", "c.bin"]
        .iter()
        .map(|name| std::fs::metadata(shim.dir.path().join(name)).unwrap().len())
        .sum();
    assert_eq!(on_disk, torrent.len() as u64);
}

// The store, with the bytes handed to it and the reads of its files
// counted.
struct Counted {
    store: Arc<TorrentStore>,
    verified: std::sync::Mutex<usize>,
    reads: std::sync::Mutex<usize>,
}

impl Pieces for Counted {
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        *self.verified.lock().unwrap() += held.len();
        self.store.verified(piece, held)
    }

    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        self.store.failed(piece)
    }

    fn checking(&self) -> anyhow::Result<bool> {
        self.store.checking()
    }

    fn found(&self, piece: u32) -> anyhow::Result<bool> {
        self.store.found(piece)
    }

    fn read(&self, file: usize, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        *self.reads.lock().unwrap() += 1;
        self.store.read(file, offset, out)
    }

    fn grow(&self, file: usize, length: u64) -> std::io::Result<()> {
        self.store.grow(file, length)
    }
}
