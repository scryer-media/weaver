// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, mpsc};
use weaver_server_disk::operations::disk::volume::LoopSite;

use super::JobDisk;
use crate::pipeline::graph::checkpoint::Pending;
use crate::pipeline::graph::disk::VolumeLeases;
use crate::pipeline::graph::job::{GraphJob, RootFile};
use crate::pipeline::graph::leaf::Leaf;
use crate::pipeline::graph::root::tests::{decoded, file, job_through, job_with};
use crate::pipeline::graph::status::RepairSlots;
use weaver_server_disk::operations::disk::volume::{Reservation, Volume};
use weaver_server_disk::operations::disk::writer::{Hold, Report, VolumeWriters};

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
        writers: VolumeWriters::reporting(move |report| {
            let _ = send.lock().unwrap().send(report);
        }),
        reports,
        volume: Arc::new(Volume::new(None, "test".to_owned())),
    }
}

impl Writers {
    fn disk(&self) -> JobDisk {
        JobDisk::new(self.writers.test_submitter(&self.volume, 1), None)
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
    disk.barrier().wait(LoopSite::LeafWrite);
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
    assert!(job.in_hand_bytes() > 0);
    assert_eq!(pending.in_hand_bytes(), 0);
    let barrier = disk.barrier();
    hold.open();
    barrier.wait(LoopSite::LeafWrite);
    assert!(pending.take().0.is_some());
    assert_eq!(pending.in_hand_bytes(), job.in_hand_bytes());
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
    barrier.wait(LoopSite::LeafWrite);
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
    disk.barrier().wait(LoopSite::LeafWrite);
    assert!(pending.take().0.is_some());
}

// A read of a leaf's bytes waits for the writes of them: whichever of the
// reader and the release runs first, it reads what was written.
#[test]
fn a_read_waits_for_the_writes_of_its_range() {
    let dir = tempfile::tempdir().unwrap();
    let through = writers();
    let hold = through.hold();
    let to = through.writers.test_submitter(&through.volume, 1);
    let mut leaf = Leaf::at(dir.path(), "a.bin").through(Some(&to));
    leaf.write(LoopSite::LeafWrite, 0, b"lantern", None)
        .unwrap();
    leaf.write(LoopSite::LeafWrite, 7, b"light", None).unwrap();
    assert!(leaf.settled().is_some_and(|barrier| !barrier.passed()));
    hold.wait_held();
    assert!(!leaf.path().exists(), "nothing is on disk yet");
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let mut read = Vec::new();
            leaf.read(LoopSite::LeafWrite, 0, 12, &mut read).unwrap();
            read
        });
        hold.open();
        assert_eq!(reader.join().unwrap(), b"lanternlight");
    });
}

// The loop reads a file its writer holds open through one handle of its
// own, as a leaf that writes its own file reads through that file. The
// handle goes before the close and the release are submitted, a read of
// the closed file opens it afresh, and every read is counted on the
// volume.
#[test]
fn the_loop_reads_an_open_file_through_one_handle_until_it_closes() {
    let dir = tempfile::tempdir().unwrap();
    let through = writers();
    let to = through.writers.test_submitter(&through.volume, 1);
    let reads = || through.volume.writer.loop_reads.load(Ordering::Relaxed);
    let mut leaf = Leaf::at(dir.path(), "a.bin").through(Some(&to));
    leaf.write(LoopSite::LeafWrite, 0, b"lanternlight", None)
        .unwrap();
    assert!(!leaf.holds_reader(), "nothing was read yet");

    let mut read = Vec::new();
    leaf.read(LoopSite::LeafWrite, 0, 7, &mut read).unwrap();
    assert!(leaf.holds_reader());
    leaf.read(LoopSite::LeafWrite, 7, 5, &mut read).unwrap();
    assert_eq!(read, b"lanternlight");
    assert_eq!(reads(), 2);

    leaf.close(LoopSite::LeafWrite, 12).unwrap();
    assert!(!leaf.holds_reader(), "the handle goes before the close");
    read.clear();
    leaf.read(LoopSite::LeafWrite, 0, 12, &mut read).unwrap();
    assert_eq!(read, b"lanternlight");
    assert!(!leaf.holds_reader(), "a closed file is not held");
    assert_eq!(reads(), 3);

    leaf.write(LoopSite::LeafWrite, 12, b"!", None).unwrap();
    read.clear();
    leaf.read(LoopSite::LeafWrite, 12, 1, &mut read).unwrap();
    assert_eq!(read, b"!");
    assert!(leaf.holds_reader());
    leaf.release_handle();
    assert!(!leaf.holds_reader(), "the handle goes before the release");
    to.barrier().wait_aside();
    assert_eq!(std::fs::read(leaf.path()).unwrap(), b"lanternlight!");
}

// A file whose later write failed is shorter on disk than what was sent to
// it. Asked for its length, the leaf says the write failed: it does not
// answer the short length as its own.
#[test]
fn the_length_of_a_file_whose_write_failed_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let through = writers();
    let hold = through.hold();
    let to = through.writers.test_submitter(&through.volume, 1);
    let mut leaf = Leaf::at(dir.path(), "a.bin").through(Some(&to));
    leaf.write(LoopSite::LeafWrite, 0, b"lantern", None)
        .unwrap();
    leaf.write(LoopSite::LeafWrite, 7, b"light", None).unwrap();
    hold.wait_held();
    // The first write goes down; the second fails.
    hold.release(1);
    hold.wait_held();
    hold.fail_next(std::io::Error::other("no room"));
    hold.open();

    let error = leaf.len(LoopSite::LeafWrite).unwrap_err();

    assert!(error.to_string().contains("failed"), "{error}");
    assert_eq!(std::fs::metadata(leaf.path()).unwrap().len(), 7);
}

// A file's move to the name it wants follows its last write.
#[test]
fn a_move_follows_the_last_write_of_its_file() {
    let dir = tempfile::tempdir().unwrap();
    let through = writers();
    let hold = through.hold();
    let to = through.writers.test_submitter(&through.volume, 1);
    let mut leaf = Leaf::at(dir.path(), "from.bin").through(Some(&to));
    leaf.write(LoopSite::LeafWrite, 0, b"lantern", None)
        .unwrap();
    leaf.write(LoopSite::LeafWrite, 7, b"light", None).unwrap();
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
    moved.wait(LoopSite::LeafWrite);
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
    barrier.wait(LoopSite::LeafWrite);
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
    barrier.wait(LoopSite::LeafWrite);
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
    barrier.wait(LoopSite::LeafWrite);
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

// The volume is stopped once what its writer holds reaches the limit leases
// stop at, and no longer once those bytes are written.
#[test]
fn a_volume_is_at_its_limit_only_while_the_bytes_wait() {
    use crate::pipeline::download::volume_pace::VolumeIntake;
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    // A volume not sized yet holds nothing back.
    assert_eq!(disk.intake(), VolumeIntake::Open);
    let (mut job, _dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    fill(&mut job, 0);
    let held = through.buffered();
    assert!(held > 1);
    assert_eq!(through.volume.unwritten_bytes(), held);
    through
        .volume
        .set_buffer_limits(2 * held + 16, 2 * held + 16);
    assert_eq!(disk.intake(), VolumeIntake::Open);
    through.volume.set_buffer_limits(held + 1, held + 1);
    assert!(through.volume.lease_limit() <= held);
    assert_eq!(disk.intake(), VolumeIntake::Stopped);
    let barrier = disk.barrier();
    hold.open();
    barrier.wait(LoopSite::LeafWrite);
    assert_eq!(through.volume.unwritten_bytes(), 0);
    assert_eq!(disk.intake(), VolumeIntake::Open);
}

// What arrived for the volume's jobs and is still being decoded counts
// against its limit with what waits for its writer: stopped at the lease
// limit, open below it. A limit lowered below what the volume counts reads
// as stopped.
#[test]
fn a_volume_counts_what_is_being_decoded_with_what_waits_for_its_writer() {
    use crate::pipeline::download::volume_pace::VolumeIntake;
    let through = writers();
    let disk = through.disk();
    through.volume.set_buffer_limits(700, 1000);
    assert_eq!(through.volume.lease_limit(), 938);
    let mut held = Reservation::none(Arc::clone(&through.volume));
    held.count(400);
    assert_eq!(disk.intake(), VolumeIntake::Open);
    held.count(300);
    assert_eq!(through.volume.unwritten_bytes(), 700);
    assert_eq!(disk.intake(), VolumeIntake::Open);
    held.count(300);
    assert_eq!(disk.intake(), VolumeIntake::Stopped);
    held.give_back(600);
    assert_eq!(disk.intake(), VolumeIntake::Open);
    through.volume.set_buffer_limits(280, 400);
    assert_eq!(disk.intake(), VolumeIntake::Stopped);
    held.give_back(400);
    assert_eq!(through.volume.unwritten_bytes(), 0);
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
        through.writers.test_submitter(&through.volume, 1),
        Some((
            output_dir.clone(),
            through.writers.test_submitter(&other, 1),
        )),
    );
    let (hold, other_hold) = (through.hold(), through.writers.hold(&other));
    for root in [&working_dir, &output_dir] {
        let path = root.join("lantern.bin");
        let to = path.clone();
        disk.under(&path)
            .run(None, path, move || std::fs::write(&to, b"lantern"));
    }
    assert!(Arc::ptr_eq(
        disk.under(&output_dir.join("lantern.bin")).volume(),
        &other
    ));
    let barrier = disk.barrier();
    assert!(!disk.quiet());
    hold.open();
    disk.working().barrier().wait(LoopSite::LeafWrite);
    assert!(working_dir.join("lantern.bin").exists());
    assert!(!barrier.returned(), "the other volume still holds its part");
    assert!(!disk.quiet());
    assert!(!output_dir.join("lantern.bin").exists());
    other_hold.fail_next(std::io::Error::other("the other disk is gone"));
    other_hold.open();
    barrier.wait(LoopSite::LeafWrite);
    assert!(disk.quiet());
    assert!(barrier.returned());
    assert!(
        !barrier.passed(),
        "a failure on either volume stands against the job"
    );
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

// A volume is probed once, the first time a job is given its writer: the
// probe is queued on that volume's thread, a second job on the volume
// queues none, and a second volume gets its own.
#[test]
fn a_volume_is_probed_once_when_a_job_first_writes_to_it() {
    let dir = tempfile::tempdir().unwrap();
    let (working_dir, output_dir) = (dir.path().join("working"), dir.path().join("output"));
    std::fs::create_dir_all(&output_dir).unwrap();
    let through = writers();
    let other = Arc::new(Volume::new(Some(2), "other".to_owned()));
    let (said, probed) = mpsc::channel();
    let said = Mutex::new(said);
    let probes = super::VolumeProbes::with(move |volume, dir| {
        let said = said.lock().unwrap().clone();
        Box::new(move || {
            let _ = said.send((volume.device, dir));
            Ok(())
        })
    });
    // The job's directory is not made yet: the probe is given the nearest
    // one above it that is.
    probes.first_seen(
        &through.writers,
        through.writers.test_owner(),
        &through.volume,
        &working_dir.join("one"),
    );
    assert_eq!(probed.recv().unwrap(), (None, dir.path().to_path_buf()));
    probes.first_seen(
        &through.writers,
        through.writers.test_owner(),
        &through.volume,
        &working_dir.join("two"),
    );
    // The job's directory is there: the probe is given the one above it.
    probes.first_seen(
        &through.writers,
        through.writers.test_owner(),
        &other,
        &output_dir,
    );
    assert_eq!(probed.recv().unwrap(), (Some(2), dir.path().to_path_buf()));
    probes.first_seen(
        &through.writers,
        through.writers.test_owner(),
        &other,
        &output_dir,
    );
    // Every sender left is in a probe still queued: none is.
    drop(probes);
    assert!(probed.recv().is_err(), "no volume is probed twice");
}

// The probe writes beside a job's folder, never in it, for every folder a
// job writes to: a working directory under its plain or its suffixed name,
// made or not yet, under a root made or not yet, and a staging directory
// under a destination's `.weaver-staging`.
#[test]
fn the_probe_writes_outside_every_job_folder() {
    use crate::jobs::working_dir::{claim_working_dir, compute_working_dir, make_job_staging_dir};
    use weaver_server_base::jobs::ids::JobId;
    let dir = tempfile::tempdir().unwrap();
    let intermediate = dir.path().join("intermediate");
    let complete = dir.path().join("complete");
    let beside = |job_dir: &Path, expected: &Path| {
        let probed = super::probed_dir(job_dir).unwrap();
        assert_eq!(probed, expected, "for {}", job_dir.display());
        assert!(!probed.starts_with(job_dir), "not in {}", job_dir.display());
    };

    // Not made yet, nor its root: the nearest directory there.
    beside(
        &compute_working_dir(&intermediate, JobId(1), "lantern"),
        dir.path(),
    );
    let plain = claim_working_dir(&intermediate, JobId(1), "lantern").unwrap();
    beside(&plain.path, &intermediate);
    let suffixed = claim_working_dir(&intermediate, JobId(2), "lantern").unwrap();
    assert_ne!(suffixed.path, plain.path);
    beside(&suffixed.path, &intermediate);
    beside(
        &compute_working_dir(&intermediate, JobId(3), "lantern later"),
        &intermediate,
    );

    std::fs::create_dir(&complete).unwrap();
    let staging = complete
        .join("tv")
        .join(crate::jobs::working_dir::STAGING_DIR_NAME)
        .join("4");
    beside(&staging, &complete);
    make_job_staging_dir(&staging, JobId(4)).unwrap();
    beside(&staging, staging.parent().unwrap());

    // Where the intermediate root is the complete root, the jobs' folders
    // are still its children.
    let shared = claim_working_dir(&complete, JobId(5), "lantern").unwrap();
    beside(&shared.path, &complete);
}

// On a machine that will start no thread for a volume, a job's first
// operation fails its key at once instead of waiting for ever: the failure
// is reported under the job's key, the barrier returns without passing,
// and nothing is held in memory for a thread that will never come.
#[test]
fn a_job_fails_at_once_where_no_writer_thread_can_start() {
    let (send, reports) = mpsc::channel();
    let send = Mutex::new(send);
    let writers = VolumeWriters::threadless(move |report: Report| {
        let _ = send.lock().unwrap().send(report);
    });
    let volume = Arc::new(Volume::new(None, "test".to_owned()));
    let disk = JobDisk::new(writers.test_submitter(&volume, 1), None);
    let (mut job, dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    for (at, ordinal) in seeded() {
        arrive(&mut job, at, ordinal);
    }
    let barrier = disk.barrier();
    assert!(barrier.returned());
    assert!(!barrier.passed());
    assert!(disk.failed());
    let failures: Vec<_> = reports
        .try_iter()
        .flat_map(|report| report.failures)
        .collect();
    assert!(!failures.is_empty());
    assert!(failures.iter().all(|failure| failure.key == 1));
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    assert!(on_disk(dir.path()).is_empty());
}

// A name is free only when no file sits at it, and a file made through a
// volume's writer sits there only once the writer has run: the name asked
// for waits for what was submitted, and so never lands on a file whose
// making is still queued.
#[test]
fn a_free_name_is_not_one_a_queued_file_will_take() {
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    let dir = tempfile::tempdir().unwrap();
    let given = crate::pipeline::graph::envelope::Given::default();
    // The first file's making is queued behind the hold: the disk does not
    // show it, and nothing waits for it to.
    let first = given.free(Some(disk.working()), dir.path(), "kept");
    assert_eq!(first, "kept");
    let to = dir.path().join(&first);
    disk.working()
        .run(None, to.clone(), move || std::fs::write(&to, b"kept"));
    assert!(!dir.path().join("kept").exists());
    assert_eq!(
        given.free(Some(disk.working()), dir.path(), "kept"),
        "kept-1"
    );
    hold.open();
    disk.barrier().wait_aside();
    // The disk says it all again once the writer is idle: the first file
    // is there, and the second name, which no file was made at, is free.
    assert_eq!(
        given.free(Some(disk.working()), dir.path(), "kept"),
        "kept-1"
    );
    assert_eq!(
        given.free(Some(disk.working()), dir.path(), "other"),
        "other"
    );
    let waits = &disk.working().volume().writer.loop_waits;
    assert_eq!(waits.load(std::sync::atomic::Ordering::Relaxed), 0);
}

// A delivery looks at its barrier twice: whether the operations behind it
// returned, and whether it passed. Operations that return on the volume's
// thread between the two looks leave the barrier passed or the job told:
// the delivery is never left stepped back with no mail asked for.
#[test]
fn operations_that_return_between_a_deliverys_two_looks_do_not_strand_it() {
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    let (mut job, _dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    for (volume, ordinal) in seeded() {
        arrive(&mut job, volume, ordinal);
    }
    let barrier = disk.barrier();
    assert!(!barrier.returned());
    let (mail, mut heard) = tokio::sync::mpsc::unbounded_channel();
    let returning = barrier.clone();
    super::between_looks::set(move || {
        hold.open();
        returning.wait(LoopSite::LeafWrite);
    });
    let passed = barrier.passed_or_tell(crate::JobId(1), &mail);
    assert!(barrier.passed());
    assert!(
        passed || heard.try_recv().is_ok(),
        "the delivery stepped back and no mail will start it again"
    );
}

// A delivery whose operations are still out steps back and hears once they
// returned; one whose operations all returned goes on and hears nothing.
#[test]
fn a_delivery_behind_held_operations_is_told_when_they_return() {
    let through = writers();
    let hold = through.hold();
    let disk = through.disk();
    let (mut job, _dir, _) = job_through(&files(), Vec::new(), |_| Some(disk.clone()));
    for (volume, ordinal) in seeded() {
        arrive(&mut job, volume, ordinal);
    }
    let barrier = disk.barrier();
    let (mail, mut heard) = tokio::sync::mpsc::unbounded_channel();
    assert!(!barrier.passed_or_tell(crate::JobId(1), &mail));
    assert!(heard.try_recv().is_err());
    hold.open();
    barrier.wait(LoopSite::LeafWrite);
    let said = heard.blocking_recv().expect("the job is told");
    assert_eq!(said.scope, 1);
    assert!(matches!(
        said.heard,
        crate::pipeline::graph::engine::Heard::DiskReturned
    ));
    assert!(barrier.passed_or_tell(crate::JobId(1), &mail));
    assert!(heard.try_recv().is_err());
}

// A handle given back is given back once. A delivery that asks again while
// it waits for the volume's writer queues nothing behind what it waits for,
// so the wait ends when the writer has run what was queued. A write after
// the release opens the file again, and the next release follows it.
#[test]
fn a_released_file_is_released_again_only_after_another_operation() {
    let dir = tempfile::tempdir().unwrap();
    let through = writers();
    let hold = through.hold();
    let to = through.writers.test_submitter(&through.volume, 1);
    let mut leaf = Leaf::at(dir.path(), "a.bin").through(Some(&to));
    leaf.write(LoopSite::LeafWrite, 0, b"lantern", None)
        .unwrap();
    leaf.release_handle();
    let released = to.barrier();
    leaf.release_handle();
    leaf.release_handle();
    // The write and the one release go through; anything queued behind
    // them is still held.
    hold.wait_held();
    hold.release(2);
    released.wait(LoopSite::LeafWrite);
    assert!(
        to.barrier().returned(),
        "nothing was queued behind the release"
    );

    leaf.write(LoopSite::LeafWrite, 7, b"light", None).unwrap();
    leaf.release_handle();
    let again = to.barrier();
    assert!(
        !again.returned(),
        "the file was opened again and is released"
    );
    hold.open();
    again.wait(LoopSite::LeafWrite);
    assert!(again.passed());
    assert_eq!(std::fs::read(leaf.path()).unwrap(), b"lanternlight");
}

// A fold asks nothing of the disk the loop would stand still for. A check
// that read the file whole folds what it read while a later operation of
// the file is still held; a restore's fold, asked while one is out, folds
// nothing and does not wait; once it returned the same fold takes. The
// loop never waited for the volume.
#[test]
fn a_fold_never_waits_for_the_file_s_operations() {
    use weaver_graph::Algorithm;
    use weaver_server_disk::operations::disk::volume::LoopWait;

    let dir = tempfile::tempdir().unwrap();
    let through = writers();
    let hold = through.hold();
    let to = through.writers.test_submitter(&through.volume, 1);
    let mut leaf = Leaf::at(dir.path(), "a.bin").through(Some(&to));
    leaf.write(LoopSite::LeafWrite, 0, b"lantern", None)
        .unwrap();
    leaf.release_handle();
    hold.wait_held();
    hold.release(1);
    let written = to.barrier();
    leaf.write(LoopSite::LeafWrite, 7, b"light", None).unwrap();
    let digest = Algorithm::Crc32.hash(b"lantern");

    leaf.fold_read(7, digest);
    assert_eq!(leaf.folded(7, Algorithm::Crc32), Some(digest.value));
    leaf.fold_passed(7, digest);
    assert_eq!(
        leaf.folded(7, Algorithm::Crc32),
        None,
        "the write is still out"
    );
    leaf.fold_vouched([(weaver_graph::ByteRange::new(0, 7), digest)]);
    assert_eq!(leaf.folded(7, Algorithm::Crc32), None);
    let waited = || {
        through.volume.writer.loop_waits_by_kind[LoopWait::Settle as usize].load(Ordering::Relaxed)
    };
    assert_eq!(waited(), 0, "nothing stood still for the held operation");

    hold.open();
    written.wait_aside();
    to.barrier().wait_aside();
    leaf.fold_passed(7, digest);
    assert_eq!(leaf.folded(7, Algorithm::Crc32), Some(digest.value));
    assert_eq!(waited(), 0);
}

// The floor a checkpoint says is in the units a run counts its downloaded
// bytes in: the decoded bytes in hand, not the post's encoded sizes, which
// run about 3% above them.
#[test]
fn a_checkpoint_s_floor_counts_decoded_bytes() {
    let encoded = PIECE + PIECE / 32;
    let (mut job, _dir, _) = job_with(
        &[file("part0.mkv", &[encoded; PIECES as usize])],
        Vec::new(),
    );
    for ordinal in 0..PIECES {
        arrive(&mut job, 0, ordinal);
    }
    let pending = Pending::default();
    assert!(pending.put(&job, Vec::new()));
    assert!(pending.take().0.is_some());
    assert_eq!(job.settled_bytes(), u64::from(PIECES * encoded));
    assert_eq!(pending.in_hand_bytes(), u64::from(PIECES * PIECE));
    assert_eq!(pending.in_hand_bytes(), job.in_hand_bytes());
}
