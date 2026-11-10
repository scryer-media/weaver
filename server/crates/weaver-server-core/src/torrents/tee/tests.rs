// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::VolumeIndex;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};

use super::{Parts, Plan, Tee};
use crate::jobs::ids::JobId;
use crate::operations::disk::CapacityReader;
use crate::pipeline::graph::engine::{Outside, RunMail};
use crate::pipeline::graph::job::Storage;
use crate::pipeline::graph::limits::Caps;
use crate::pipeline::graph::status::RepairSlots;
use crate::torrents::files::Files;
use crate::torrents::layout::{PieceFile, PieceLayout};
use crate::torrents::storage::Pieces;
use crate::torrents::store::{Kept, TorrentStore};

const ARCHIVE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/rar5/rar5_store.rar"
));

fn file(name: &str, length: u64) -> PieceFile {
    PieceFile {
        name: name.to_owned(),
        length,
        padding: false,
    }
}

// A tee over the layout's archive files, writing under `work`, with the
// mail its runs speak on.
fn tee(layout: &Arc<PieceLayout>, work: &Path, mail: &RunMail) -> Arc<Tee> {
    tee_sharing(layout, work, mail, Arc::default())
}

// The same, its repair slots the ones `repairs` names, as every torrent's
// tee shares them.
fn tee_sharing(
    layout: &Arc<PieceLayout>,
    work: &Path,
    mail: &RunMail,
    repairs: Arc<std::sync::Mutex<RepairSlots>>,
) -> Arc<Tee> {
    Tee::fresh(
        parts(layout, work, mail, repairs),
        storage(work, mail, CapacityReader::unknown()),
    )
}

// What a tee over the layout's archive files is made of.
fn parts(
    layout: &Arc<PieceLayout>,
    work: &Path,
    mail: &RunMail,
    repairs: Arc<std::sync::Mutex<RepairSlots>>,
) -> Parts {
    Parts {
        job_id: JobId(1),
        layout: Arc::clone(layout),
        plan: Plan::of(layout).unwrap(),
        work: work.to_path_buf(),
        repairs,
        signal: Arc::default(),
        outside: Arc::new(Outside::default()),
        limit: 1,
        db: crate::Database::open_in_memory().unwrap(),
        mail: mail.written(),
    }
}

// Where a tee's job writes, reading the free disk through `free_disk`.
fn storage(work: &Path, mail: &RunMail, free_disk: CapacityReader) -> Storage {
    Storage {
        working_dir: work.to_path_buf(),
        output: None,
        decode: crate::pipeline::graph::engine::DecodePool::unbounded(),
        data_dir: None,
        window_ledger: Default::default(),
        accountant: Arc::new(HoldsAccountant::new(HoldBudgets {
            resident_limit: u64::MAX,
            scratch_total: u64::MAX,
            scratch_ceiling: u64::MAX,
            disk_reserve: 0,
        })),
        free_disk,
        slots: 1,
        mail: mail.sender(),
        passwords: Vec::new(),
        caps: Caps::unbounded(),
        nesting: u32::MAX,
        hardware: Default::default(),
        metrics: None,
    }
}

// The piece as the torrent hashed it.
fn piece(layout: &PieceLayout, torrent: &[u8], piece: u32) -> Bytes {
    let range = layout.piece_range(piece);
    Bytes::copy_from_slice(&torrent[range.start as usize..range.end as usize])
}

fn settled(tee: &Tee, piece: u32) -> bool {
    !tee.wants(piece)
}

// Only a torrent with a container volume among its files has a tee, and
// only its archive files, the volumes with the recovery files beside them,
// go to it: a video and its notes never do.
#[test]
fn only_archive_files_go_to_the_graph() {
    let plain = PieceLayout::new(
        1024,
        vec![
            file("film.mkv", 4000),
            file("film.nfo", 100),
            file("film.par2", 50),
        ],
    )
    .unwrap();
    assert!(Plan::of(&plain).is_none());
    let archive = PieceLayout::new(
        1024,
        vec![
            file("set.part1.rar", 4000),
            file("set.part2.rar", 1000),
            file("set.nfo", 100),
            file("set.par2", 50),
            file("set.vol00+01.par2", 300),
            file("sample.mkv", 900),
        ],
    )
    .unwrap();
    let plan = Plan::of(&archive).unwrap();
    assert_eq!(
        (0..6).map(|file| plan.volume(file)).collect::<Vec<_>>(),
        [
            Some(VolumeIndex(0)),
            Some(VolumeIndex(1)),
            None,
            Some(VolumeIndex(2)),
            Some(VolumeIndex(3)),
            None,
        ]
    );
}

// A piece's share of an archive file goes to the job as a view of the
// buffer the piece was hashed in: the same memory, at the share's place in
// the piece, vouched for by the piece check and so not checked again. A
// share of a plain file in the same piece does not go at all.
#[tokio::test]
async fn an_archive_share_goes_to_the_job_as_a_view_of_the_hashed_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let mail = RunMail::default();
    let layout =
        Arc::new(PieceLayout::new(64, vec![file("set.rar", 100), file("set.nfo", 50)]).unwrap());
    let tee = tee(&layout, dir.path(), &mail);
    let torrent: Vec<u8> = (0..150).map(|at| at as u8).collect();
    let held = piece(&layout, &torrent, 1);
    let shares = tee.with(|job| tee.unsettled(job, 1));
    assert_eq!(shares.len(), 1);
    let decoded = tee.decoded(&shares[0], &held);
    assert_eq!(decoded.volume, VolumeIndex(0));
    assert_eq!(decoded.ordinal, 1);
    assert_eq!(decoded.offset, 64);
    assert_eq!(decoded.bytes.as_ptr(), held.as_ptr());
    assert_eq!(&decoded.bytes[..], &torrent[64..100]);
    assert!(decoded.intact && decoded.positioned && decoded.segments.is_empty());
    assert_eq!(
        (decoded.length, decoded.declared_end),
        (Some(100), Some(100))
    );

    assert!(tee.deliver(1, &held));
    assert!(settled(&tee, 1));
    tee.forget();
}

// One torrent's commit never waits on another's: while the repair slots
// every torrent's tee shares are held, as by another torrent's step, a
// piece of this torrent is still written to its job and taken, and the
// lend after is left to the settle, which lends it once they are free.
#[tokio::test]
async fn a_commit_takes_its_piece_while_another_torrent_holds_the_repair_slots() {
    let dir = tempfile::tempdir().unwrap();
    let mail = RunMail::default();
    let repairs: Arc<std::sync::Mutex<RepairSlots>> = Arc::default();
    let layout =
        Arc::new(PieceLayout::new(128, vec![file("set.rar", ARCHIVE.len() as u64)]).unwrap());
    let other = tee_sharing(&layout, &dir.path().join("a"), &mail, Arc::clone(&repairs));
    let this = tee_sharing(&layout, &dir.path().join("b"), &mail, Arc::clone(&repairs));

    let held = repairs.lock().unwrap();
    assert!(this.deliver(0, &piece(&layout, ARCHIVE, 0)));
    assert!(settled(&this, 0));
    drop(held);
    this.lend_handed().unwrap();
    assert!(!this.owes_lend.load(std::sync::atomic::Ordering::Acquire));
    this.forget();
    other.forget();
}

// A crash between the torrent's record of its pieces and the job's: the
// torrent has every piece on disk and says so, and the job lacks one of
// them. The store starts from the pieces it kept, so the session fetches
// and checks nothing, and that one piece goes to the job from the files,
// once.
#[tokio::test]
async fn a_piece_the_job_lacks_is_replayed_from_the_files_once() {
    let dir = tempfile::tempdir().unwrap();
    let mail = RunMail::default();
    let layout =
        Arc::new(PieceLayout::new(128, vec![file("set.rar", ARCHIVE.len() as u64)]).unwrap());
    let pieces = layout.piece_count();
    let lacked = pieces / 2;
    std::fs::create_dir_all(dir.path().join("files")).unwrap();
    std::fs::write(dir.path().join("files/set.rar"), ARCHIVE).unwrap();
    let tee = tee(&layout, &dir.path().join("work"), &mail);
    for index in (0..pieces).filter(|index| *index != lacked) {
        assert!(tee.deliver(index, &piece(&layout, ARCHIVE, index)));
    }
    let mut bits = bitvec::vec::BitVec::<u8, bitvec::order::Msb0>::repeat(true, pieces as usize);
    bits.set_uninitialized(false);
    let kept = Kept {
        bits: bits.into_vec(),
        checking: false,
    };

    let files = Files::new(&layout, dir.path().join("files")).unwrap();
    let store = TorrentStore::new(
        Arc::clone(&layout),
        files,
        Some(&kept),
        Some(Arc::clone(&tee)),
    );
    assert_eq!(
        store.initial_have(false).unwrap().count_ones(),
        pieces as usize
    );
    assert!(!settled(&tee, lacked));
    assert_eq!(store.replay_had().taken, 1);
    assert!((0..pieces).all(|index| settled(&tee, index)));
    assert_eq!(store.replay_had().taken, 0);
    store.forget();
}

// The reverse: the job took a piece the torrent then lost, its hash failing
// on a check of the files. The torrent has it no longer and keeps that, so
// the session fetches it again; the copy fetched is written to the files
// and goes to the job no second time.
#[tokio::test]
async fn a_piece_the_torrent_lost_is_fetched_again_and_not_delivered_twice() {
    let dir = tempfile::tempdir().unwrap();
    let mail = RunMail::default();
    let layout =
        Arc::new(PieceLayout::new(128, vec![file("set.rar", ARCHIVE.len() as u64)]).unwrap());
    let pieces = layout.piece_count();
    let lost = pieces / 2;
    let tee = tee(&layout, &dir.path().join("work"), &mail);
    let files = Files::new(&layout, dir.path().join("files")).unwrap();
    let store = TorrentStore::new(Arc::clone(&layout), files, None, Some(Arc::clone(&tee)));
    for index in 0..pieces {
        store
            .verified(index, &piece(&layout, ARCHIVE, index))
            .unwrap();
    }
    assert!((0..pieces).all(|index| settled(&tee, index)));

    assert!(store.failed(lost).unwrap());
    let kept = store.kept();
    let files = Files::new(&layout, dir.path().join("files")).unwrap();
    let again = TorrentStore::new(Arc::clone(&layout), files, Some(&kept), None);
    let have = again.initial_have(false).unwrap();
    assert!(!have[lost as usize]);
    assert_eq!(have.count_ones(), pieces as usize - 1);

    let fetched = piece(&layout, ARCHIVE, lost);
    assert!(!tee.deliver(lost, &fetched));
    store.verified(lost, &fetched).unwrap();
    assert!(store.has(lost));
    assert_eq!(
        std::fs::read(dir.path().join("files/set.rar")).unwrap(),
        ARCHIVE
    );
    store.forget();
}

// A restored tee's nodes retry the torrent's current list of passwords from
// the top: the place each checkpoint kept goes back to the first candidate.
#[test]
fn a_restored_tee_retries_its_passwords_from_the_top() {
    let mut offered = std::collections::BTreeMap::from([
        (weaver_graph::NodeId(1), 3),
        (weaver_graph::NodeId(2), 0),
    ]);
    super::retry_from_top(&mut offered);
    assert_eq!(
        offered.into_iter().collect::<Vec<_>>(),
        [(weaver_graph::NodeId(1), 0), (weaver_graph::NodeId(2), 0)]
    );
}

const LZ: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/rar5/rar5_multifile_lz.rar"
));

// A tee over a compressed archive whose free disk reads panic once
// `armed`, as a failing probe of the disk would, with the trap that
// records this thread's panics.
fn armed_tee(
    dir: &Path,
    mail: &RunMail,
) -> (
    Arc<PieceLayout>,
    Arc<Tee>,
    Arc<std::sync::atomic::AtomicBool>,
    Arc<crate::runtime::panic_trap::Trap>,
) {
    let layout = Arc::new(PieceLayout::new(64, vec![file("set.rar", LZ.len() as u64)]).unwrap());
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let armed = Arc::clone(&armed);
        CapacityReader::from_fn(move || {
            assert!(
                !armed.load(std::sync::atomic::Ordering::Acquire),
                "the disk could not be read"
            );
            crate::operations::disk::Capacity::Unknown
        })
    };
    let work = dir.join("work");
    std::fs::create_dir_all(&work).unwrap();
    let tee = Tee::fresh(
        parts(&layout, &work, mail, Arc::default()),
        storage(&work, mail, reader),
    );
    let trap = Arc::new(crate::runtime::panic_trap::Trap::new(
        &dir.join("marks"),
        "test",
    ));
    (layout, tee, armed, trap)
}

// The one marker the trap wrote, naming the tee's job.
fn marked(dir: &Path) -> crate::runtime::panic_trap::Mark {
    let mut marks = crate::runtime::panic_trap::read_marks(&dir.join("marks"));
    assert_eq!(marks.len(), 1);
    let mark = marks.pop().unwrap();
    assert_eq!(mark.job, Some(JobId(1)));
    mark
}

// A panic while a piece goes to the job leaves the torrent's marker.
#[tokio::test]
async fn a_panic_delivering_a_piece_leaves_the_torrents_marker() {
    let dir = tempfile::tempdir().unwrap();
    let mail = RunMail::default();
    let (layout, tee, armed, trap) = armed_tee(dir.path(), &mail);
    assert!(tee.deliver(0, &piece(&layout, LZ, 0)));
    armed.store(true, std::sync::atomic::Ordering::Release);
    let caught = crate::runtime::panic_trap::trapped(
        &trap,
        std::panic::AssertUnwindSafe(|| tee.deliver(1, &piece(&layout, LZ, 1))),
    );
    assert!(caught.is_err());
    assert_eq!(marked(dir.path()).message, "the disk could not be read");
}

// A panic while the job takes a word its runs said leaves the torrent's
// marker.
#[tokio::test]
async fn a_panic_taking_a_word_leaves_the_torrents_marker() {
    let dir = tempfile::tempdir().unwrap();
    let mut mail = RunMail::default();
    let (layout, tee, armed, trap) = armed_tee(dir.path(), &mail);
    for index in 0..layout.piece_count() {
        tee.deliver(index, &piece(&layout, LZ, index));
    }
    armed.store(true, std::sync::atomic::Ordering::Release);
    loop {
        let said = mail.recv().await.unwrap();
        let caught = crate::runtime::panic_trap::trapped(
            &trap,
            std::panic::AssertUnwindSafe(|| {
                tee.take(super::Word::Said(said, 1), (false, false));
            }),
        );
        if caught.is_err() {
            break;
        }
    }
    assert_eq!(marked(dir.path()).message, "the disk could not be read");
}
