// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

use tokio::sync::{Notify, watch};
use weaver_graph::holds::{HoldBudgets, HoldsAccountant, HoldsNamespace};
use weaver_graph::{ByteRange, MemberId, NodeId, VolumeIndex};
use weaver_torrent::core::lengths::{Lengths, ValidPieceIndex};
use weaver_torrent::spawn_utils::BlockingSpawner;
use weaver_torrent::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerMode, ListenerOptions, Session,
    SessionOptions, create_torrent, torrent_from_bytes,
};

use super::{PieceStorage, PieceStorageFactory, Pieces, TorrentJob, initial_have, layout_of};
use crate::operations::disk::CapacityReader;
use crate::pipeline::graph::engine::RunMail;
use crate::pipeline::graph::job::{GraphJob, Storage};
use crate::pipeline::graph::limits::Caps;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::root::pieces::{PieceFile, PieceLayout, Verified};
use crate::pipeline::graph::status::RepairSlots;
use crate::torrents::intake::Holds;
use weaver_torrent::storage::{
    BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage,
};

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

fn storage(dir: &Path, mail: &RunMail) -> Storage {
    Storage {
        working_dir: dir.to_path_buf(),
        accountant: accountant(u64::MAX),
        free_disk: CapacityReader::unknown(),
        slots: 1,
        mail: mail.sender(),
        passwords: Vec::new(),
        caps: Caps::unbounded(),
        nesting: u32::MAX,
        unpack: true,
    }
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

struct Shim {
    job: Arc<TorrentJob>,
    storage: PieceStorage,
    layout: Arc<PieceLayout>,
    mail: RunMail,
    dir: tempfile::TempDir,
    room: Arc<Notify>,
}

fn shim(piece_length: u64, files: Vec<PieceFile>) -> Shim {
    let layout = Arc::new(PieceLayout::new(piece_length, files).unwrap());
    let dir = tempfile::tempdir().unwrap();
    let mail = RunMail::default();
    let job = Arc::new(TorrentJob::new(
        GraphJob::for_torrent(1, Arc::clone(&layout), storage(dir.path(), &mail)),
        Arc::default(),
        Arc::default(),
        1,
    ));
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&job) as Arc<dyn Pieces>,
        unbounded(),
    );
    Shim {
        job,
        storage,
        layout,
        mail,
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

    // Hear the job's runs until it is done.
    async fn settle(&mut self) {
        while self.job.with(|job| job.phase()) != RootPhase::Done {
            let said = self.mail.recv().await.unwrap();
            self.job
                .with(|job| job.run_said(said, &mut RepairSlots::default(), 1))
                .unwrap();
        }
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
    // Before it verifies the piece is the session's alone: the job has
    // none of it, and a read of it comes from the buffer.
    assert!(shim.job.with(|job| job.have().not_any()));
    assert!(!shim.dir.path().join("a.bin").exists());
    assert_eq!(shim.read(0), torrent[..1024]);
    // A piece with nothing written and nothing posted is not in hand.
    let mut buf = [0; 10];
    assert!(shim.storage.pread_exact(1, 100, &mut buf).is_err());
    // A read across two pieces is read from what was posted, and piece 1
    // is not.
    assert!(shim.storage.pread_exact(1, 20, &mut buf).is_err());
    // Nor does a write cross a piece.
    assert!(shim.storage.pwrite_all(1, 20, &[0; 10]).is_err());
    // An empty read past the end of a file is nothing to do.
    shim.storage.pread_exact(0, 1000, &mut []).unwrap();
}

#[test]
fn a_verified_piece_goes_to_the_job_as_its_files_shares() {
    let (shim, torrent) = three_files();
    for piece in 0..5 {
        shim.complete(piece, &torrent);
    }
    assert!(shim.job.with(|job| job.have().all()));
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

// The bitfield the session starts from is the restored job's pieces, in
// the wire's bit order.
#[test]
fn a_restored_job_gives_the_session_the_pieces_it_has() {
    let (shim, torrent) = three_files();
    for piece in [0, 1, 2, 4] {
        shim.complete(piece, &torrent);
    }
    let rows = crate::pipeline::graph::checkpoint::rows(&shim.job.with(|job| job.snapshot()));
    let snapshot = crate::pipeline::graph::checkpoint::snapshot(&rows).unwrap();
    let restored = GraphJob::restore_torrent(
        &snapshot,
        Arc::clone(&shim.layout),
        1,
        storage(shim.dir.path(), &shim.mail),
        1,
    )
    .unwrap();
    let have = initial_have(&restored.job).unwrap();
    assert_eq!(have.len(), 5);
    // Piece 4 came before piece 3, which carries the front of c's file: it
    // is in c's file all the same, so the restart has it.
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 1, 2, 4]);
    // Piece 0 is the high bit of the first byte.
    assert_eq!(have.as_raw_slice()[0], 0b1110_1000);
}

#[test]
fn a_verified_piece_seeds_from_the_files_it_was_posted_to() {
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

// The pieces of a RAR store archive: the job unpacks its member, keeps the
// one copy of the bytes, and seeds the archive's pieces back from it.
#[tokio::test]
async fn a_rar_store_archive_in_pieces_delivers_its_member_and_seeds_from_it() {
    let archive = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let member = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/originals/small.txt"
    ));
    let mut shim = shim(128, vec![file("set.rar", archive.len() as u64)]);
    let pieces = shim.layout.piece_count();
    assert!(pieces > 2);
    for piece in 0..pieces {
        shim.complete(piece, archive);
    }
    shim.settle().await;
    assert_eq!(
        std::fs::read(shim.dir.path().join("small.txt")).unwrap(),
        member
    );
    // The archive's bytes are not kept beside the member: the archive is
    // read back through it.
    assert!(!shim.dir.path().join("set.rar").exists());
    for piece in 0..pieces {
        let range = shim.layout.piece_range(piece);
        assert_eq!(
            shim.read(piece),
            archive[range.start as usize..range.end as usize],
            "piece {piece}"
        );
    }
}

// libtorrent `torrent::on_piece_hashed` has only the pieces that pass: a
// piece of a RAR store archive the job unpacked that then fails its hash is
// in hand no longer, the job steps with the member open again, and the
// piece fetched again settles the job with the member whole.
#[tokio::test]
async fn a_failed_piece_under_an_archive_reopens_it_until_fetched_again() {
    let archive = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let member = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/originals/small.txt"
    ));
    let mut shim = shim(128, vec![file("set.rar", archive.len() as u64)]);
    let pieces = shim.layout.piece_count();
    assert!(pieces > 2);
    for piece in 0..pieces {
        shim.complete(piece, archive);
    }
    shim.settle().await;

    let piece = pieces / 2;
    shim.storage
        .on_piece_hash_failed(shim.index(piece))
        .unwrap();
    shim.job.with(|job| {
        assert!(!job.have()[piece as usize]);
        assert_eq!(job.have().count_ones() as u32, pieces - 1);
        assert!(!job.namespace().get(MemberId(0)).unwrap().closed);
        assert!(job.phase() != RootPhase::Done);
    });
    let range = shim.layout.piece_range(piece);
    assert!(
        shim.storage
            .pread_exact(0, range.start, &mut [0; 8])
            .is_err()
    );

    shim.complete(piece, archive);
    shim.settle().await;
    assert!(shim.job.with(|job| job.have().all()));
    assert_eq!(
        std::fs::read(shim.dir.path().join("small.txt")).unwrap(),
        member
    );
    for piece in 0..pieces {
        let range = shim.layout.piece_range(piece);
        assert_eq!(
            shim.read(piece),
            archive[range.start as usize..range.end as usize],
            "piece {piece}"
        );
    }
}

// The job's side of the storage, telling the test when every piece is in.
struct Watched {
    job: Arc<TorrentJob>,
    whole: watch::Sender<bool>,
}

impl Pieces for Watched {
    fn verified(&self, shares: Vec<Verified>) -> anyhow::Result<()> {
        self.job.verified(shares)?;
        self.whole
            .send_replace(self.job.with(|job| job.have().all()));
        Ok(())
    }

    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        let held = self.job.failed(piece)?;
        self.whole
            .send_replace(self.job.with(|job| job.have().all()));
        Ok(held)
    }

    fn checking(&self) -> anyhow::Result<bool> {
        let had = self.job.checking()?;
        self.whole
            .send_replace(self.job.with(|job| job.have().all()));
        Ok(had)
    }

    fn found(&self, piece: u32) -> anyhow::Result<bool> {
        let found = self.job.found(piece)?;
        self.whole
            .send_replace(self.job.with(|job| job.have().all()));
        Ok(found)
    }

    fn posted(&self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.job.posted(volume, range, into)
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

// A graph job over the layout, with the side the storage hands pieces to.
fn watched_job(layout: &Arc<PieceLayout>, dir: &Path) -> (Arc<Watched>, watch::Receiver<bool>) {
    let mail = RunMail::default();
    let job = Arc::new(TorrentJob::new(
        GraphJob::for_torrent(1, Arc::clone(layout), storage(dir, &mail)),
        Arc::default(),
        Arc::default(),
        1,
    ));
    let (whole, in_hand) = watch::channel(false);
    (Arc::new(Watched { job, whole }), in_hand)
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
// that peer alone into a graph job through the piece storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_fetched_from_a_loopback_peer_lands_in_the_job_and_seeds_from_it() {
    let seeded = seeded().await;
    let Seeded {
        originals, layout, ..
    } = &seeded;
    let fetch_dir = tempfile::tempdir().unwrap();
    let (watched, mut in_hand) = watched_job(layout, &fetch_dir.path().join("job"));
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
    let posted = PieceStorage::new(Arc::clone(&layout), watched as Arc<dyn Pieces>, unbounded());
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
    let (watched, mut in_hand) = watched_job(layout, &fetch_dir.path().join("job"));
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
    assert!(watched.job.with(|job| job.have().not_any()));
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
// waiting for that room is rung, the job is told, and the storage going
// gives back the rest.
#[tokio::test]
async fn a_piece_that_fails_its_hash_lets_go_of_its_bytes_and_its_room() {
    let layout = Arc::new(PieceLayout::new(8, vec![file("a", 16)]).unwrap());
    let budget = accountant(8);
    let kept = Arc::new(Kept::default());
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&kept) as Arc<dyn Pieces>,
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
    assert_eq!(*kept.failed.lock().unwrap(), [0]);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    assert!(storage.pread_exact(0, 0, &mut [0; 8]).is_err());

    assert!(storage.reserve_piece(second, &room));
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 8);
    drop(storage);
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
}

// The job's side that keeps what it is handed, reads the piece back
// through the storage while it is being handed over, and the pieces it was
// told failed.
#[derive(Default)]
struct Kept {
    storage: std::sync::OnceLock<PieceStorage>,
    shares: std::sync::Mutex<Vec<Verified>>,
    read_while_handed: std::sync::Mutex<Vec<u8>>,
    failed: std::sync::Mutex<Vec<u32>>,
}

impl Pieces for Kept {
    fn verified(&self, shares: Vec<Verified>) -> anyhow::Result<()> {
        let storage = self.storage.get().unwrap();
        let mut read = self.read_while_handed.lock().unwrap();
        for (file, length) in [(0, 5), (1, 3)] {
            let mut buf = vec![0; length];
            storage.pread_exact(file, 0, &mut buf)?;
            read.extend(buf);
        }
        self.shares.lock().unwrap().extend(shares);
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

    fn posted(&self, _: VolumeIndex, _: ByteRange, _: &mut Vec<u8>) -> bool {
        false
    }
}

// A verified piece's shares are views of the one buffer it was written
// into, and the piece reads from memory until the job has them.
#[test]
fn a_verified_piece_goes_to_the_job_without_a_copy() {
    let layout = Arc::new(PieceLayout::new(8, vec![file("a", 5), file("b", 3)]).unwrap());
    let kept = Arc::new(Kept::default());
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&kept) as Arc<dyn Pieces>,
        unbounded(),
    );
    kept.storage.set(storage.clone()).ok().unwrap();
    let index = Lengths::new(8, 8)
        .unwrap()
        .try_validate_piece_index(0)
        .unwrap();
    assert!(storage.reserve_piece(index, &Arc::default()));
    storage.pwrite_all(0, 0, b"aaaaa").unwrap();
    storage.pwrite_all(1, 0, b"bbb").unwrap();
    storage.on_piece_completed(index).unwrap();

    assert_eq!(*kept.read_while_handed.lock().unwrap(), b"aaaaabbb");
    let shares = kept.shares.lock().unwrap();
    assert_eq!(
        (&shares[0].bytes[..], &shares[1].bytes[..]),
        (&b"aaaaa"[..], &b"bbb"[..])
    );
    assert_eq!(
        shares[0].bytes.as_ptr().wrapping_add(5),
        shares[1].bytes.as_ptr()
    );
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
    let kept = Arc::new(Kept::default());
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        Arc::clone(&kept) as Arc<dyn Pieces>,
        holds(&budget),
    );
    kept.storage.set(storage.clone()).ok().unwrap();
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
    let shares = kept.shares.lock().unwrap();
    assert_eq!(
        (&shares[0].bytes[..], &shares[1].bytes[..]),
        (&b"abcde"[..], &b"fgh"[..])
    );
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
}
