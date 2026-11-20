// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The namespace writer's files written through a volume's own writer
// thread, as outside tests, on a real directory: what the disk shows while
// the thread is held, and once it goes on. Every removal, move and
// directory the ledger decides runs on that thread, after the checkpoint
// that allows it has committed and after what was submitted before it.

use std::path::Path;
use std::sync::mpsc;

use super::*;
use crate::pipeline::graph::disk::JobDisk;
use weaver_server_disk::operations::disk::volume::Volume;
use weaver_server_disk::operations::disk::writer::{Failure, Hold, Report, VolumeWriters};

// One volume's writer thread, and every failure it reports.
struct Disk {
    writers: VolumeWriters,
    volume: Arc<Volume>,
    reports: mpsc::Receiver<Report>,
}

impl Disk {
    fn new() -> Self {
        let (send, reports) = mpsc::channel();
        let send = Mutex::new(send);
        Self {
            writers: VolumeWriters::reporting(move |report| {
                let _ = send.lock().unwrap().send(report);
            }),
            volume: Arc::new(Volume::new(None, "test".to_owned())),
            reports,
        }
    }

    // The job's files are written through the volume's thread from here.
    fn attach(&self, job: &mut Job) {
        job.writer.move_disk_for_test(JobDisk::new(
            self.writers.test_submitter(&self.volume, 1),
            None,
        ));
    }

    fn hold(&self) -> Hold {
        self.writers.hold(&self.volume)
    }

    // Every failure reported so far.
    fn failures(&self) -> Vec<Failure> {
        self.reports
            .try_iter()
            .flat_map(|report| report.failures)
            .collect()
    }
}

// Wait until everything the job submitted has returned.
fn settle(job: &Job) {
    job.writer
        .disk()
        .expect("writes through")
        .barrier()
        .wait_aside();
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

// The file of a discarded member is removed on the volume's thread, after
// its own write, and only once the checkpoint that stops naming it has
// committed. The leaf knows its file is there from asking for it to be
// made, before the thread has made it.
#[test]
fn a_discarded_file_goes_on_the_writer_only_after_its_checkpoint_commits() {
    let disk = Disk::new();
    let mut job = Job::new(PLENTY);
    disk.attach(&mut job);
    let path = job.dir.path().join("lantern.bin");
    let hold = disk.hold();
    let member = job.member("lantern.bin", Some(7));
    job.send(member, 0, b"lantern", Provenance::Fresh);
    hold.wait_held();
    assert!(!path.exists(), "the file's making is still queued");
    let leaf = job
        .writer
        .files_mut(NodeId::ROOT)
        .leaves
        .remove(&member)
        .expect("leaf");
    assert!(leaf.on_disk(), "the leaf knows its file from making it");
    job.writer.discard(leaf).expect("discard");
    let stamp = job.writer.committed + 1;
    job.writer.recorded(stamp);

    // The write goes down; the checkpoint has not committed, so the file
    // stays where the database still names it.
    hold.open();
    settle(&job);
    assert_eq!(read(&path), b"lantern");

    // The commit leaves the removal with the held thread.
    let hold = disk.hold();
    job.writer
        .commit_moves(&mut job.tree, stamp)
        .expect("commit");
    hold.wait_held();
    assert_eq!(read(&path), b"lantern");
    hold.open();
    settle(&job);
    assert!(!path.exists());
    assert!(disk.failures().is_empty());
}

// A file that cannot be made is told by the volume's thread once it tried,
// not by the write that asked for it: the write is taken, the failure comes
// back under the job's key as a file not made, and nothing lands.
#[test]
fn a_file_that_cannot_be_made_fails_on_the_writer_not_in_the_send() {
    let disk = Disk::new();
    let mut job = Job::new(PLENTY);
    disk.attach(&mut job);
    // A stray file where the member's directory has to go.
    std::fs::write(job.dir.path().join("boxed"), b"stray").unwrap();
    let hold = disk.hold();
    let member = job.member("boxed/lantern.bin", Some(7));
    job.send(member, 0, b"lantern", Provenance::Fresh);
    hold.wait_held();
    assert!(disk.failures().is_empty(), "nothing has been tried yet");
    assert!(job.writer.files[&NodeId::ROOT].leaves[&member].on_disk());
    hold.open();
    settle(&job);
    // The report follows the operation's return: wait for it.
    let mut failures = Vec::new();
    while failures.is_empty() {
        failures.extend(disk.reports.recv().unwrap().failures);
    }
    failures.extend(disk.failures());
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0].key, 1);
    assert!(failures[0].creating, "{failures:?}");
    assert!(job.writer.disk().unwrap().failed());
    assert_eq!(read(&job.dir.path().join("boxed")), b"stray");
}

// A renamed file moves on the volume's thread after its write and only
// once the checkpoint that records the name has committed. A newer member
// given the name it left waits off it, and its own move to that name runs
// behind the first: neither lands on the other.
#[test]
fn a_rename_moves_on_the_writer_after_its_commit_and_its_old_name_waits() {
    let disk = Disk::new();
    let mut job = Job::new(PLENTY);
    disk.attach(&mut job);
    let dir = job.dir.path().to_path_buf();
    let member = job.member("3f9a.bin", Some(5));
    job.send(member, 0, b"alpha", Provenance::Fresh);
    let mut build = job.build();
    job.writer
        .rename(&mut job.tree, &mut build, NodeId::ROOT, member, "show.mkv")
        .expect("rename");
    let stamp = job.writer.committed + 1;
    job.writer.recorded(stamp);
    // Recorded, not committed: the file stays where it sat.
    settle(&job);
    assert_eq!(read(&dir.join("3f9a.bin")), b"alpha");
    assert!(!dir.join("show.mkv").exists());

    let hold = disk.hold();
    assert!(
        job.writer
            .commit_moves(&mut job.tree, stamp)
            .expect("commit")
    );
    let next = job.member("3f9a.bin", Some(5));
    assert_eq!(job.root().get(next).unwrap().output_name, "3f9a (1).bin");
    job.send(next, 0, b"bravo", Provenance::Fresh);
    job.commit();
    assert_eq!(job.root().get(next).unwrap().output_name, "3f9a.bin");
    // Every move is decided and none has run.
    hold.wait_held();
    assert_eq!(read(&dir.join("3f9a.bin")), b"alpha");
    assert!(!dir.join("show.mkv").exists());
    assert!(!dir.join("3f9a (1).bin").exists());

    hold.open();
    settle(&job);
    assert_eq!(read(&dir.join("show.mkv")), b"alpha");
    assert_eq!(read(&dir.join("3f9a.bin")), b"bravo");
    assert!(!dir.join("3f9a (1).bin").exists());
    assert!(disk.failures().is_empty());
}

// A leaf that never made a file leaves nothing to remove: a newer file at
// its path stays once the checkpoint commits, whether the newer one was
// still queued behind the held thread when the old leaf went or was
// already on disk.
#[test]
fn a_discarded_leaf_with_no_file_takes_no_newer_file_through_the_writer() {
    let disk = Disk::new();
    let mut job = Job::new(PLENTY);
    disk.attach(&mut job);
    let dir = job.dir.path().to_path_buf();
    let to = job.writer.disk_under(&dir);

    // The newer file is still queued.
    let queued = crate::pipeline::graph::envelope::envelope_name(NodeId(2), 1);
    let hold = disk.hold();
    job.writer
        .discard(Leaf::at(&dir, &queued).through(to.as_ref()))
        .expect("discard");
    let mut newer = Leaf::at(&dir, &queued).through(to.as_ref());
    newer.write(0, b"next", None).unwrap();
    job.commit();
    hold.wait_held();
    hold.open();
    settle(&job);
    assert_eq!(read(&dir.join(&queued)), b"next");

    // The newer file is on disk first.
    let written = crate::pipeline::graph::envelope::envelope_name(NodeId(3), 1);
    let mut first = Leaf::at(&dir, &written).through(to.as_ref());
    first.write(0, b"next", None).unwrap();
    settle(&job);
    job.writer
        .discard(Leaf::at(&dir, &written).through(to.as_ref()))
        .expect("discard");
    job.commit();
    settle(&job);
    assert_eq!(read(&dir.join(&written)), b"next");
    drop((newer, first));
    settle(&job);
    assert!(disk.failures().is_empty());
}

// Two archives hold one directory, and the lower one made it and a file in
// it first: the directory is shared, made on the writer by both, and
// neither moves.
#[test]
fn a_shared_directory_is_made_on_the_writer_and_never_moves() {
    let disk = Disk::new();
    let mut job = Job::new(PLENTY);
    disk.attach(&mut job);
    let (higher, lower) = two_stores(&mut job);
    let mut emits = directory(1, "folder/");
    emits.extend(store_file(2, "folder/b.bin", b"data"));
    store_says(&mut job, lower, emits);
    store_says(&mut job, higher, directory(1, "folder/"));
    job.commit();
    settle(&job);
    for node in [higher, lower] {
        let namespace = job.tree.namespace(node).unwrap();
        assert_eq!(namespace.get(MemberId(1)).unwrap().output_name, "folder/");
    }
    assert_eq!(read(&job.dir.path().join("folder/b.bin")), b"data");
    assert!(!job.dir.path().join("folder (1)").exists());
    assert!(disk.failures().is_empty());
}

// One archive holds a file "folder", another "folder/b.bin". The file
// moves aside and the other moves in under the directory, both on the
// writer after the commit, in the order that keeps a file and a directory
// off one path.
#[test]
fn a_file_and_a_path_through_its_name_move_apart_on_the_writer() {
    let disk = Disk::new();
    let mut job = Job::new(PLENTY);
    disk.attach(&mut job);
    let dir = job.dir.path().to_path_buf();
    let (higher, lower) = two_stores(&mut job);
    store_says(&mut job, higher, store_file(1, "folder", b"data"));
    store_says(&mut job, lower, store_file(1, "folder/b.bin", b"hi"));
    settle(&job);
    assert_eq!(read(&dir.join("folder")), b"data");

    let hold = disk.hold();
    job.commit();
    hold.wait_held();
    assert_eq!(read(&dir.join("folder")), b"data", "nothing moved yet");
    assert!(!dir.join("folder (1)").exists());
    hold.open();
    settle(&job);
    let file = job
        .tree
        .namespace(higher)
        .unwrap()
        .get(MemberId(1))
        .unwrap();
    assert_eq!(file.output_name, "folder (1)");
    assert_eq!(read(&dir.join("folder (1)")), b"data");
    assert_eq!(read(&dir.join("folder/b.bin")), b"hi");
    assert!(disk.failures().is_empty());
}

// A directory named over a file already written waits for the file to
// move: the file moves aside on the writer before the directory is put at
// its name, and what goes in the directory lands in it.
#[test]
fn a_directory_named_over_a_written_file_follows_its_move_on_the_writer() {
    let disk = Disk::new();
    let mut job = Job::new(PLENTY);
    disk.attach(&mut job);
    let dir = job.dir.path().to_path_buf();
    let (higher, lower) = two_stores(&mut job);
    store_says(&mut job, lower, store_file(1, "folder", b"data"));
    let mut emits = directory(1, "folder/");
    emits.extend(store_file(2, "folder/b.bin", b"hi"));
    store_says(&mut job, higher, emits);
    settle(&job);
    assert_eq!(read(&dir.join("folder")), b"data");

    let hold = disk.hold();
    job.commit();
    hold.wait_held();
    assert_eq!(read(&dir.join("folder")), b"data", "nothing moved yet");
    hold.open();
    settle(&job);
    let file = job.tree.namespace(lower).unwrap().get(MemberId(1)).unwrap();
    assert_eq!(file.output_name, "folder (1)");
    assert_eq!(read(&dir.join("folder (1)")), b"data");
    assert!(dir.join("folder").is_dir());
    assert_eq!(read(&dir.join("folder/b.bin")), b"hi");
    assert!(disk.failures().is_empty());
}

// A retracted directory is removed as a directory on the writer once the
// checkpoint that stops naming it commits, and so is each directory above
// it left empty. One another archive still holds, or with something still
// in it, stays.
#[test]
fn a_retracted_directory_and_its_empty_parents_go_on_the_writer() {
    let disk = Disk::new();
    let mut job = Job::new(PLENTY);
    disk.attach(&mut job);
    let dir = job.dir.path().to_path_buf();
    let (higher, lower) = two_stores(&mut job);
    let mut emits = directory(1, "alone/");
    emits.extend(directory(2, "shared/"));
    emits.extend(directory(3, "full/"));
    emits.extend(store_file(4, "full/kept.bin", b"kept"));
    emits.extend(directory(5, "deep/inner/"));
    store_says(&mut job, lower, emits);
    store_says(&mut job, higher, directory(1, "shared/"));
    job.commit();
    settle(&job);
    for made in ["alone", "shared", "full", "deep/inner"] {
        assert!(dir.join(made).is_dir(), "{made}");
    }

    store_says(
        &mut job,
        lower,
        [1, 2, 3, 5]
            .map(|member| Emit::Retract(MemberId(member)))
            .to_vec(),
    );
    let hold = disk.hold();
    job.commit();
    hold.wait_held();
    assert!(
        dir.join("alone").is_dir(),
        "not removed ahead of the writer"
    );
    assert!(dir.join("deep/inner").is_dir());
    hold.open();
    settle(&job);
    assert!(!dir.join("alone").exists());
    assert!(!dir.join("deep").exists(), "the parent it left empty goes");
    assert!(dir.join("shared").is_dir());
    assert_eq!(read(&dir.join("full/kept.bin")), b"kept");
    assert!(disk.failures().is_empty());
}
