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
        disk: None,
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
        crate::torrents::writes::Writes::for_test(),
    );
    assert_eq!(
        store.initial_have(false).unwrap().count_ones(),
        pieces as usize
    );
    assert!(!settled(&tee, lacked));
    assert_eq!(store.replay_had().taken, 1);
    assert!((0..pieces).all(|index| settled(&tee, index)));
    assert_eq!(store.replay_had().taken, 0);
    store.forget().await;
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
    let store = TorrentStore::new(
        Arc::clone(&layout),
        files,
        None,
        Some(Arc::clone(&tee)),
        crate::torrents::writes::Writes::for_test(),
    );
    for index in 0..pieces {
        store
            .verified(index, &piece(&layout, ARCHIVE, index))
            .unwrap();
    }
    assert!((0..pieces).all(|index| settled(&tee, index)));

    assert!(store.failed(lost).unwrap());
    let kept = store.kept();
    let files = Files::new(&layout, dir.path().join("files")).unwrap();
    let again = TorrentStore::new(
        Arc::clone(&layout),
        files,
        Some(&kept),
        None,
        crate::torrents::writes::Writes::for_test(),
    );
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
    store.forget().await;
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

const SECRET: &str = "tee-loan-secret";

// One ZipCrypto member under `SECRET`, its sizes in a data descriptor
// after it: its keystream runs over the ciphertext it holds as each piece
// comes, work lent off the loop.
fn zipcrypto() -> Vec<u8> {
    use std::io::Write;
    use zip::unstable::write::FileOptionsExt;
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .with_deprecated_encryption(SECRET.as_bytes())
        .unwrap();
    let mut writer = zip::ZipWriter::new_stream(Vec::new());
    writer.start_file("feature.mkv", options).unwrap();
    let body: Vec<u8> = (0..40 * 1024 + 77u32)
        .map(|at| (at.wrapping_mul(61) >> 3) as u8)
        .collect();
    writer.write_all(&body).unwrap();
    writer.finish().unwrap().into_inner()
}

// While an object of the job is out for its work, no checkpoint is taken,
// forced or not: its facts as it left hold the work undone. The checkpoint
// is owed, which keeps any more work from going out. The step that takes
// the object back takes it, work goes out again, and a stop's checkpoint,
// which waits for the object, is taken.
#[tokio::test]
async fn no_checkpoint_is_taken_while_an_object_is_out_for_its_work() {
    let dir = tempfile::tempdir().unwrap();
    let mail = RunMail::default();
    let archive = zipcrypto();
    let layout =
        Arc::new(PieceLayout::new(1024, vec![file("lantern.zip", archive.len() as u64)]).unwrap());
    let mut storage = storage(dir.path(), &mail, CapacityReader::unknown());
    storage.passwords = vec![crate::jobs::ArchivePasswordCandidate::new(
        crate::jobs::ArchivePasswordSource::Tracker,
        SECRET.to_owned(),
    )];
    let tee = Tee::fresh(parts(&layout, dir.path(), &mail, Arc::default()), storage);
    tee.with(|job| job.hold_loans());
    // Every piece but the first leaves its step unread: only the job's
    // first checkpoint is due at once.
    for index in 0..layout.piece_count() {
        assert!(tee.deliver(index, &piece(&layout, &archive, index)));
    }
    assert!(
        tee.with(|job| job.working()),
        "an object is out for its work"
    );
    let mut loans = tee.with(|job| job.held_loans());
    assert_eq!(loans.len(), 1, "one object out for its work");
    let stamp = tee.with(|job| job.pending().stamp());

    tee.checkpoint(false).unwrap();
    tee.checkpoint(true).unwrap();
    assert_eq!(
        tee.with(|job| job.pending().stamp()),
        stamp,
        "nothing taken while out"
    );
    assert!(
        tee.with(|job| job.owes_checkpoint()),
        "the checkpoint is owed"
    );

    let stop = tokio::task::spawn_blocking({
        let tee = Arc::clone(&tee);
        move || tee.checkpoint_stopped()
    });
    let said = tee.with(|job| job.worked_loan(loans.pop().unwrap()));
    tee.take(super::Word::Said(said, 1), (false, false));
    assert!(!tee.with(|job| job.working()), "the object is back");
    assert!(
        tee.with(|job| job.pending().stamp()) > stamp,
        "the step that took it back took the checkpoint"
    );
    assert!(
        !tee.with(|job| job.owes_checkpoint()),
        "work goes out again"
    );
    stop.await.unwrap().unwrap();
    assert!(!tee.with(|job| job.owes_checkpoint()), "the stop took it");
    tee.forget();
}

const LZ: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/rar5/rar5_multifile_lz.rar"
));

// A tee over a compressed archive whose free disk reads panic once
// `armed`, as a failing probe of the disk would, with the trap that
// records this thread's panics. The disk is all but full: a KiB above a
// one-byte reserve. What the job may write is then promised a little at a
// time, so a write its runs make once their pieces are in reads the disk
// again, as one that fits what was promised before would not.
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
    let read_at = std::time::Instant::now();
    let reader = {
        let armed = Arc::clone(&armed);
        CapacityReader::from_fn(move || {
            assert!(
                !armed.load(std::sync::atomic::Ordering::Acquire),
                "the disk could not be read"
            );
            crate::operations::disk::Capacity::Known(crate::operations::disk::CapacityReading {
                available_bytes: 1 + 1024,
                total_bytes: 1 << 40,
                sampled_at: read_at,
                stale: false,
            })
        })
    };
    let work = dir.join("work");
    std::fs::create_dir_all(&work).unwrap();
    let mut storage = storage(&work, mail, reader);
    let mut limits = crate::pipeline::ExtractionLimits::unbounded();
    limits.min_free_bytes = 1;
    storage.caps = Caps::new(
        Arc::new(limits),
        crate::pipeline::graph::limits::known_policy(
            crate::post_processing::model::PostProcessingSettings {
                unacceptable_extensions: Vec::new(),
                ..Default::default()
            },
        ),
        Vec::new(),
    );
    let tee = Tee::fresh(parts(&layout, &work, mail, Arc::default()), storage);
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

// A torrent's unpacking that finds no room over the reserve waits as a job
// of the pipeline's does: it takes a ticket from the disk's ledger and is
// listed among the jobs outside the pipeline that wait, which the
// pipeline's reading wakes in ticket order. While the disk reads no room it
// stays waiting; once a reading shows room it no longer waits, its runs go
// on, and it unpacks whole.
#[tokio::test]
async fn a_torrent_unpack_held_for_room_is_woken_when_room_returns() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let mut mail = RunMail::default();
    let layout = Arc::new(PieceLayout::new(64, vec![file("set.rar", LZ.len() as u64)]).unwrap());
    let room = Arc::new(AtomicU64::new(0));
    let reader = {
        let room = Arc::clone(&room);
        CapacityReader::from_fn(move || {
            crate::operations::disk::Capacity::Known(crate::operations::disk::CapacityReading {
                available_bytes: 1 + room.load(Ordering::Acquire),
                total_bytes: 1 << 40,
                sampled_at: std::time::Instant::now(),
                stale: false,
            })
        })
    };
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let mut storage = storage(&work, &mail, reader);
    let mut limits = crate::pipeline::ExtractionLimits::unbounded();
    limits.min_free_bytes = 1;
    storage.caps = Caps::new(
        Arc::new(limits),
        crate::pipeline::graph::limits::known_policy(
            crate::post_processing::model::PostProcessingSettings {
                unacceptable_extensions: Vec::new(),
                ..Default::default()
            },
        ),
        Vec::new(),
    );
    let tee = Tee::fresh(parts(&layout, &work, &mail, Arc::default()), storage);
    let outside = Arc::clone(&tee.outside);
    assert!(!outside.waits_for_disk());

    for index in 0..layout.piece_count() {
        tee.deliver(index, &piece(&layout, LZ, index));
    }
    while !tee.with(|job| job.disk_held()) {
        let said = mail.recv().await.unwrap();
        tee.take(super::Word::Said(said, 1), (false, false));
    }
    assert!(outside.waits_for_disk(), "the job waits for room");
    assert_eq!(
        outside.disk_waits_began(),
        vec![1],
        "heard once, as it began"
    );
    assert!(outside.disk_waits_began().is_empty());
    let waiters = outside.disk_waiters();
    assert_eq!(waiters.len(), 1);
    let (ticket, scope, hears) = &waiters[0];
    assert_eq!(*scope, 1);
    assert_eq!(Some(*ticket), tee.with(|job| job.disk_ticket()));
    // The pipeline is given its wait and folder, as its own job's, to say
    // as the wait begins.
    let wait = outside
        .disk_wait_of(1)
        .expect("the waiting job gives its wait");
    assert_eq!(Some(wait.clone()), tee.with(|job| job.disk_wait()));
    assert_eq!(wait.0.reserve, 1);
    assert_eq!(hears.disk_wait(), Some(wait));

    // A reading with no room keeps it waiting.
    assert!(!hears.wake_disk());
    assert!(outside.waits_for_disk());

    // Room returns: the reading wakes it, and its runs go on.
    room.store(1 << 30, Ordering::Release);
    assert!(hears.wake_disk(), "a reading with room wakes it");
    assert!(!outside.waits_for_disk());
    assert!(outside.disk_waiters().is_empty());
    assert_eq!(hears.disk_wait(), None, "woken, it waits for nothing");
    assert!(!tee.with(|job| job.disk_held()));
    while !tee.done() {
        let said = mail.recv().await.unwrap();
        tee.take(super::Word::Said(said, 1), (false, false));
    }
    assert!(!outside.waits_for_disk());
    tee.forget();
}

// Operator ruling, 2026-11-16 (PR 129 question 10): an unpacked file whose
// name is taken is placed under a numbered name, never dropped and never
// over the file there.
mod numbered_outputs {
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};

    use super::super::plan_outputs;

    fn plan(work: &Path, dir: &Path, kept: &[&str]) -> super::super::Outputs {
        plan_outputs(
            work,
            dir,
            &HashSet::new(),
            &HashSet::new(),
            kept.iter().map(|name| (*name).to_owned()).collect(),
        )
    }

    fn folders() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let (work, dir) = (root.path().join("work"), root.path().join("dir"));
        std::fs::create_dir_all(&work).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        (root, work, dir)
    }

    // An output's own name and a numbered form of another's never meet:
    // "movie (1).mkv" made by the archive keeps its name, and "movie.mkv",
    // whose name a file holds, is planned past it.
    #[test]
    fn a_numbered_form_is_never_planned_over_another_outputs_name() {
        let (_root, work, dir) = folders();
        std::fs::write(dir.join("movie.mkv"), b"there").unwrap();
        std::fs::write(work.join("movie.mkv"), b"unpacked").unwrap();
        std::fs::write(work.join("movie (1).mkv"), b"also unpacked").unwrap();
        let outputs = plan(&work, &dir, &[]);
        let mut names = outputs.names.clone();
        names.sort();
        assert_eq!(names, ["movie (1).mkv", "movie (2).mkv"]);
        let placed = outputs.place(&work);
        assert!(placed.failed.is_none());
        assert_eq!(std::fs::read(dir.join("movie.mkv")).unwrap(), b"there");
        assert_eq!(
            std::fs::read(dir.join("movie (1).mkv")).unwrap(),
            b"also unpacked"
        );
        assert_eq!(
            std::fs::read(dir.join("movie (2).mkv")).unwrap(),
            b"unpacked"
        );
    }

    // A file that comes to hold the planned name before placing is passed
    // over, untouched, for the next form, and placing says the name it used.
    #[test]
    fn a_name_taken_after_planning_is_passed_over_at_placing() {
        let (_root, work, dir) = folders();
        std::fs::write(dir.join("movie.mkv"), b"there").unwrap();
        std::fs::write(work.join("movie.mkv"), b"unpacked").unwrap();
        let outputs = plan(&work, &dir, &[]);
        assert_eq!(outputs.names, ["movie (1).mkv"]);
        std::fs::write(dir.join("movie (1).mkv"), b"came since").unwrap();
        let placed = outputs.place(&work);
        assert!(placed.failed.is_none());
        assert_eq!(placed.names, ["movie (2).mkv"]);
        assert_eq!(std::fs::read(dir.join("movie.mkv")).unwrap(), b"there");
        assert_eq!(
            std::fs::read(dir.join("movie (1).mkv")).unwrap(),
            b"came since"
        );
        assert_eq!(
            std::fs::read(dir.join("movie (2).mkv")).unwrap(),
            b"unpacked"
        );
    }

    // A restart between planning and placing: the plan's names were kept on
    // the row, and the first placing claimed its name (an empty file) and
    // stopped there. The next plan reads the row and takes the same name, and
    // its placing fills that claim: no second numbered copy. A restart before
    // the claim was made plans the same name too.
    #[test]
    fn a_restart_between_planning_and_placing_places_no_second_numbered_copy() {
        let (_root, work, dir) = folders();
        std::fs::write(dir.join("movie.mkv"), b"there").unwrap();
        std::fs::write(work.join("movie.mkv"), b"unpacked").unwrap();
        let first = plan(&work, &dir, &[]);
        assert_eq!(first.names, ["movie (1).mkv"]);
        // Kept on the row, nothing claimed yet.
        assert_eq!(plan(&work, &dir, &["movie (1).mkv"]).names, first.names);
        // Claimed, as placing claims it, and cut short there.
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join("movie (1).mkv"))
            .unwrap();
        let again = plan(&work, &dir, &["movie (1).mkv"]);
        assert_eq!(again.names, first.names);
        let placed = again.place(&work);
        assert!(placed.failed.is_none());
        assert_eq!(placed.names, ["movie (1).mkv"]);
        assert_eq!(std::fs::read(dir.join("movie.mkv")).unwrap(), b"there");
        assert_eq!(
            std::fs::read(dir.join("movie (1).mkv")).unwrap(),
            b"unpacked"
        );
        assert!(!dir.join("movie (2).mkv").exists());
        // An empty file not on the row is not this torrent's: passed over.
        std::fs::write(work.join("movie.mkv"), b"unpacked").unwrap();
        std::fs::write(dir.join("movie (2).mkv"), b"").unwrap();
        assert_eq!(
            plan(&work, &dir, &["movie (1).mkv"]).names,
            ["movie (3).mkv"]
        );
    }

    // Thousands of outputs whose names are all taken each take their own
    // first number: no output walks past another's forms.
    #[test]
    fn thousands_of_taken_names_each_take_their_first_number() {
        let (_root, work, dir) = folders();
        let count = 2_000;
        for i in 0..count {
            std::fs::write(dir.join(format!("e{i}.mkv")), b"there").unwrap();
            std::fs::write(work.join(format!("e{i}.mkv")), b"unpacked").unwrap();
        }
        let outputs = plan(&work, &dir, &[]);
        let names: HashSet<String> = outputs.names.iter().cloned().collect();
        let wanted: HashSet<String> = (0..count).map(|i| format!("e{i} (1).mkv")).collect();
        assert_eq!(names, wanted);
    }
}
