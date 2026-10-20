// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

use librqbit::spawn_utils::BlockingSpawner;
use librqbit::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerMode, ListenerOptions, Session,
    SessionOptions, create_torrent, torrent_from_bytes,
};
use librqbit_core::lengths::{Lengths, ValidPieceIndex};
use tokio::sync::watch;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{ByteRange, VolumeIndex};

use super::{PieceStorage, PieceStorageFactory, Pieces, TorrentJob, initial_have, layout_of};
use crate::operations::disk::CapacityReader;
use crate::pipeline::graph::engine::RunMail;
use crate::pipeline::graph::job::{GraphJob, Storage};
use crate::pipeline::graph::limits::Caps;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::root::pieces::{PieceFile, PieceLayout, Verified};
use crate::pipeline::graph::status::RepairSlots;
use librqbit::storage::TorrentStorage;

fn storage(dir: &Path, mail: &RunMail) -> Storage {
    Storage {
        working_dir: dir.to_path_buf(),
        accountant: Arc::new(HoldsAccountant::new(HoldBudgets {
            resident_limit: u64::MAX,
            scratch_total: u64::MAX,
            scratch_ceiling: u64::MAX,
            disk_reserve: 0,
        })),
        free_disk: CapacityReader::unknown(),
        slots: 1,
        mail: mail.sender(),
        passwords: Vec::new(),
        caps: Caps::unbounded(),
        nesting: u32::MAX,
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
    let storage = PieceStorage::new(Arc::clone(&layout), Arc::clone(&job) as Arc<dyn Pieces>);
    Shim {
        job,
        storage,
        layout,
        mail,
        dir,
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

    // The piece's bytes written as the session writes them: file by file.
    fn write(&self, piece: u32, torrent: &[u8]) {
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
    let have = initial_have(&restored.job);
    assert_eq!(have.len(), 5);
    // Piece 4 waited in memory behind piece 3, so the checkpoint left it out
    // and it is fetched again.
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 1, 2]);
    // Piece 0 is the high bit of the first byte.
    assert_eq!(have.as_raw_slice()[0], 0b1110_0000);
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

    fn posted(&self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.job.posted(volume, range, into)
    }
}

// A session on loopback alone: no DHT, no trackers, no discovery.
async fn session(dir: &Path) -> Arc<Session> {
    Session::new_with_opts(
        dir.to_path_buf(),
        SessionOptions {
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

// One session seeds a torrent from its files; the other fetches it from
// that peer alone into a graph job through the piece storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_fetched_from_a_loopback_peer_lands_in_the_job_and_seeds_from_it() {
    let seeds = tempfile::tempdir().unwrap();
    let set = seeds.path().join("set");
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

    let seeder = session(&seeds.path().join("session")).await;
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

    let fetch = tempfile::tempdir().unwrap();
    let mail = RunMail::default();
    let job = Arc::new(TorrentJob::new(
        GraphJob::for_torrent(
            1,
            Arc::clone(&layout),
            storage(&fetch.path().join("job"), &mail),
        ),
        Arc::default(),
        Arc::default(),
        1,
    ));
    let (whole, mut in_hand) = watch::channel(false);
    let watched = Arc::new(Watched {
        job: Arc::clone(&job),
        whole,
    });
    let factory =
        PieceStorageFactory::new(Arc::clone(&layout), Arc::clone(&watched) as Arc<dyn Pieces>);
    let leecher = session(&fetch.path().join("session")).await;
    let _fetching = leecher
        .add_torrent(
            AddTorrent::from_bytes(bytes),
            Some(AddTorrentOptions {
                output_folder: Some(fetch.path().join("unused").to_string_lossy().into_owned()),
                overwrite: true,
                initial_peers: Some(vec![peer]),
                storage_factory: Some(factory.boxed_factory()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    in_hand.wait_for(|whole| *whole).await.unwrap();

    let dir = fetch.path().join("job");
    let mut torrent = Vec::new();
    for (name, original) in &originals {
        assert_eq!(&std::fs::read(dir.join(name)).unwrap(), original, "{name}");
    }
    for index in 0..layout.files().count() {
        let name = &layout.files().nth(index).unwrap().name;
        let (_, original) = originals.iter().find(|(file, _)| file == name).unwrap();
        torrent.extend_from_slice(original);
    }
    let posted = PieceStorage::new(Arc::clone(&layout), watched as Arc<dyn Pieces>);
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
    seeder.stop().await;
}
