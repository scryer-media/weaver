// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, mpsc};

use super::JobDisk;
use crate::operations::disk::volume::Volume;
use crate::operations::disk::writer::{Hold, Report, VolumeWriters};
use crate::pipeline::graph::checkpoint::Pending;
use crate::pipeline::graph::job::{GraphJob, RootFile};
use crate::pipeline::graph::leaf::Leaf;
use crate::pipeline::graph::root::tests::{decoded, file, job_through, job_with};
use crate::pipeline::graph::status::RepairSlots;

// A plain file's article, 8 KiB of bytes its file and ordinal say.
const PIECE: u32 = 8 * 1024;
const PIECES: u32 = 6;

fn piece(volume: u32, ordinal: u32) -> Vec<u8> {
    (0..PIECE)
        .map(|at| (at as u8).wrapping_mul(31) ^ (volume as u8 * 16 + ordinal as u8))
        .collect()
}

fn files() -> Vec<RootFile> {
    (0..3)
        .map(|index| file(&format!("part{index}.mkv"), &[PIECE; PIECES as usize]))
        .collect()
}

fn arrive(job: &mut GraphJob, volume: u32, ordinal: u32) {
    job.intake(
        decoded(
            volume,
            ordinal,
            u64::from(ordinal * PIECE),
            &piece(volume, ordinal),
            Some(u64::from(PIECES * PIECE)),
        ),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
}

// Five of the six articles of file `volume` arrive, in order.
fn fill(job: &mut GraphJob, volume: u32) {
    for ordinal in 0..PIECES - 1 {
        arrive(job, volume, ordinal);
    }
}

// The order the articles of the seeded jobs arrive in: out of order within
// a file and across files, one article of each file left out.
fn seeded() -> Vec<(u32, u32)> {
    let mut order = Vec::new();
    for ordinal in [3, 0, 4, 1] {
        for volume in [2, 0, 1] {
            order.push((volume, ordinal));
        }
    }
    order
}

struct Writers {
    writers: VolumeWriters,
    reports: mpsc::Receiver<Report>,
    volume: Arc<Volume>,
}

fn writers() -> Writers {
    let (send, reports) = mpsc::channel();
    let send = Mutex::new(send);
    Writers {
        writers: VolumeWriters::new(move |report| {
            let _ = send.lock().unwrap().send(report);
        }),
        reports,
        volume: Arc::new(Volume::new(None, "test".to_owned())),
    }
}

impl Writers {
    fn disk(&self) -> JobDisk {
        JobDisk::new(self.writers.submitter(&self.volume, 1), None)
    }

    fn hold(&self) -> Hold {
        self.writers.hold(&self.volume)
    }

    fn buffered(&self) -> u64 {
        self.volume.buffered_bytes.load(Ordering::Relaxed)
    }
}

// Every file under `root`, by its path below it.
fn on_disk(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, at: &Path, into: &mut BTreeMap<String, Vec<u8>>) {
        for entry in std::fs::read_dir(at).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, into);
            } else {
                into.insert(
                    path.strip_prefix(root).unwrap().display().to_string(),
                    std::fs::read(&path).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

// The same seeded job written by itself and through a volume's writer ends
// with the same files, the same articles on disk and the same checkpoint
// rows.
#[test]
fn a_job_written_through_a_volume_matches_one_that_writes_itself() {
    let (mut inline, inline_dir, _) = job_with(&files(), Vec::new());
    let through = writers();
    let disk = through.disk();
    let (mut queued, queued_dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    for (volume, ordinal) in seeded() {
        arrive(&mut inline, volume, ordinal);
        arrive(&mut queued, volume, ordinal);
    }
    disk.barrier().wait();
    assert!(disk.barrier().passed());
    assert_eq!(through.buffered(), 0);
    assert_eq!(on_disk(queued_dir.path()), on_disk(inline_dir.path()));
    assert_eq!(queued.on_disk(), inline.on_disk());
    let rows = |job: &GraphJob| {
        let pending = Pending::default();
        assert!(pending.put(job, Vec::new()));
        pending.take().0.unwrap()
    };
    assert_eq!(rows(&queued), rows(&inline));
    let mut failures = 0;
    while let Ok(report) = through.reports.try_recv() {
        failures += report.failures.len();
    }
    assert_eq!(failures, 0);
}

// The bytes a checkpoint spares a restore are said only once the disk
// bears its change out: while the volume holds the job's writes the floor
// stays where the last borne-out checkpoint left it.
#[test]
fn settled_bytes_are_said_once_the_disk_bears_them_out() {
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    let (mut job, _dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    for (volume, ordinal) in seeded() {
        arrive(&mut job, volume, ordinal);
    }
    let pending = Pending::default();
    assert!(!pending.put(&job, Vec::new()), "nothing may be written yet");
    assert!(job.settled_bytes() > 0);
    assert_eq!(pending.settled_bytes(), 0);
    let barrier = disk.barrier();
    hold.open();
    barrier.wait();
    assert!(pending.take().0.is_some());
    assert_eq!(pending.settled_bytes(), job.settled_bytes());
}

// A checkpoint taken while a volume still holds the job's writes records
// none of them: nothing reaches the database's writer until every write
// submitted before it has returned.
#[test]
fn a_checkpoint_taken_while_writes_are_held_records_none() {
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    let (mut job, _dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    for (volume, ordinal) in seeded() {
        arrive(&mut job, volume, ordinal);
    }
    assert!(through.buffered() > 0);
    let pending = Pending::default();
    assert!(!pending.put(&job, Vec::new()), "nothing may be written yet");
    assert!(pending.gated());
    assert!(pending.take().0.is_none());
    // One write through is still not all of them.
    hold.wait_held();
    hold.release(1);
    hold.wait_held();
    assert!(pending.take().0.is_none());
    let barrier = disk.barrier();
    hold.open();
    barrier.wait();
    assert!(pending.take().0.is_some());
    assert!(!pending.gated());
}

// A job that keeps writing still gets its checkpoints out: the first held
// change goes once its own writes returned, though later writes are held.
#[test]
fn a_held_change_goes_once_its_own_writes_returned() {
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    let (mut job, _dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    fill(&mut job, 0);
    assert!(through.buffered() > 0);
    let pending = Pending::default();
    assert!(!pending.put(&job, Vec::new()));
    let first = disk.barrier();
    hold.wait_held();
    let waiting = through.buffered();
    fill(&mut job, 1);
    assert!(through.buffered() > waiting);
    assert!(!pending.put(&job, Vec::new()));
    // Let through exactly what the first change waited for.
    while !first.returned() {
        hold.release(1);
        hold.wait_held();
    }
    assert!(first.passed());
    assert!(!disk.barrier().passed(), "the later writes are still held");
    assert!(pending.take().0.is_some(), "the first change is released");
    assert!(pending.gated(), "the second still waits");
    hold.open();
    disk.barrier().wait();
    assert!(pending.take().0.is_some());
}

// A read of a leaf's bytes waits for the writes of them: whichever of the
// reader and the release runs first, it reads what was written.
#[test]
fn a_read_waits_for_the_writes_of_its_range() {
    let dir = tempfile::tempdir().unwrap();
    let through = writers();
    let hold = through.hold();
    let to = through.writers.submitter(&through.volume, 1);
    let mut leaf = Leaf::at(dir.path(), "a.bin").through(Some(&to));
    leaf.write(0, b"lantern", None).unwrap();
    leaf.write(7, b"light", None).unwrap();
    assert!(leaf.settled().is_some_and(|barrier| !barrier.passed()));
    hold.wait_held();
    assert!(!leaf.path().exists(), "nothing is on disk yet");
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let mut read = Vec::new();
            leaf.read(0, 12, &mut read).unwrap();
            read
        });
        hold.open();
        assert_eq!(reader.join().unwrap(), b"lanternlight");
    });
}

// A file's move to the name it wants follows its last write.
#[test]
fn a_move_follows_the_last_write_of_its_file() {
    let dir = tempfile::tempdir().unwrap();
    let through = writers();
    let hold = through.hold();
    let to = through.writers.submitter(&through.volume, 1);
    let mut leaf = Leaf::at(dir.path(), "from.bin").through(Some(&to));
    leaf.write(0, b"lantern", None).unwrap();
    leaf.write(7, b"light", None).unwrap();
    leaf.intend("to.bin");
    leaf.move_on().unwrap();
    let moved = to.barrier();
    // Both writes go through; the move is still held.
    hold.wait_held();
    hold.release(2);
    hold.wait_held();
    assert_eq!(
        std::fs::read(dir.path().join("from.bin")).unwrap(),
        b"lanternlight"
    );
    assert!(!dir.path().join("to.bin").exists());
    assert!(!moved.returned());
    hold.open();
    moved.wait();
    assert!(moved.passed());
    assert!(!dir.path().join("from.bin").exists());
    assert_eq!(
        std::fs::read(dir.path().join("to.bin")).unwrap(),
        b"lanternlight"
    );
}

// A job that is stopped with writes held, and keeps its files, is kept
// until the last of them returned, and every one of them is written.
#[test]
fn a_stopped_job_is_kept_until_its_last_write_returned() {
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    let (mut job, dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    let (mut inline, inline_dir, _) = job_with(&files(), Vec::new());
    for (volume, ordinal) in seeded() {
        arrive(&mut job, volume, ordinal);
        arrive(&mut inline, volume, ordinal);
    }
    assert!(job.holds_retirement());
    assert!(!job.awaits_mail());
    let barrier = disk.barrier();
    hold.open();
    barrier.wait();
    assert!(!job.holds_retirement());
    assert_eq!(on_disk(dir.path()), on_disk(inline_dir.path()));
}

// A job that gives its files up with writes held: none of them is written
// after, no bytes stay counted as waiting, and the job is kept until the
// volume's writer is done with it.
#[test]
fn nothing_is_written_once_a_job_gives_its_files_up() {
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    let (mut job, dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    for (volume, ordinal) in seeded() {
        arrive(&mut job, volume, ordinal);
    }
    let pending = Pending::default();
    assert!(!pending.put(&job, Vec::new()));
    hold.wait_held();
    let before = on_disk(dir.path());
    job.abandon_disk();
    assert!(job.holds_retirement());
    let barrier = disk.barrier();
    hold.open();
    barrier.wait();
    assert!(!job.holds_retirement());
    assert_eq!(on_disk(dir.path()), before, "nothing was written after");
    assert_eq!(through.buffered(), 0);
    assert!(
        pending.take().0.is_none(),
        "no checkpoint records the writes that were given up"
    );
    // Nothing of the job reaches the disk when it goes, either.
    drop(job);
    through.writers.settle();
    assert_eq!(on_disk(dir.path()), before);
}

// A write the volume fails comes back with its batch under the job's key,
// and no checkpoint that waited for it is ever released.
#[test]
fn a_failed_write_comes_back_under_the_jobs_key() {
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    let (mut job, _dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    fill(&mut job, 0);
    assert!(through.buffered() > 0);
    let pending = Pending::default();
    assert!(!pending.put(&job, Vec::new()));
    hold.wait_held();
    hold.fail_next(std::io::Error::other("the disk is gone"));
    let barrier = disk.barrier();
    hold.open();
    barrier.wait();
    let mut failures = Vec::new();
    while failures.is_empty() {
        failures.extend(through.reports.recv().unwrap().failures);
    }
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].key, 1);
    assert_eq!(failures[0].error.to_string(), "the disk is gone");
    assert!(disk.failed());
    assert!(!disk.barrier().passed());
    assert!(pending.take().0.is_none());
    assert_eq!(through.buffered(), 0);
}

// The volume holds the budget while its writer holds that many bytes, and
// no longer once they are written.
#[test]
fn a_volume_is_over_its_budget_only_while_the_bytes_wait() {
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    let (mut job, _dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    fill(&mut job, 0);
    let budget = through.buffered() + 1;
    assert!(budget > 1);
    assert!(!disk.over(budget));
    fill(&mut job, 1);
    assert!(disk.over(budget));
    let barrier = disk.barrier();
    hold.open();
    barrier.wait();
    assert!(!disk.over(budget));
}

// A job whose delivered files lie on another volume than its working
// directory waits for both writers: one volume putting its part down
// leaves the job's barrier standing until the other has, and a failure on
// either stands against the job.
#[test]
fn a_job_on_two_volumes_waits_for_both() {
    let through = writers();
    let other = Arc::new(Volume::new(Some(2), "other".to_owned()));
    let dir = tempfile::tempdir().unwrap();
    let (working_dir, output_dir) = (dir.path().join("working"), dir.path().join("output"));
    std::fs::create_dir_all(&working_dir).unwrap();
    std::fs::create_dir_all(&output_dir).unwrap();
    let disk = JobDisk::new(
        through.writers.submitter(&through.volume, 1),
        Some((output_dir.clone(), through.writers.submitter(&other, 1))),
    );
    let (hold, other_hold) = (through.hold(), through.writers.hold(&other));
    for root in [&working_dir, &output_dir] {
        let path = root.join("lantern.bin");
        let to = path.clone();
        disk.under(&path)
            .run(None, path, move || std::fs::write(&to, b"lantern"));
    }
    assert!(Arc::ptr_eq(disk.under(&output_dir.join("lantern.bin")).volume(), &other));
    let barrier = disk.barrier();
    assert!(!disk.quiet());
    hold.open();
    disk.working().barrier().wait();
    assert!(working_dir.join("lantern.bin").exists());
    assert!(!barrier.returned(), "the other volume still holds its part");
    assert!(!disk.quiet());
    assert!(!output_dir.join("lantern.bin").exists());
    other_hold.fail_next(std::io::Error::other("the other disk is gone"));
    other_hold.open();
    barrier.wait();
    assert!(disk.quiet());
    assert!(barrier.returned());
    assert!(!barrier.passed(), "a failure on either volume stands against the job");
    let failures: Vec<_> = through
        .reports
        .iter()
        .flat_map(|report| report.failures)
        .take(1)
        .collect();
    assert_eq!(failures[0].key, 1);
    assert_eq!(failures[0].error.to_string(), "the other disk is gone");
    assert!(!output_dir.join("lantern.bin").exists());
}
