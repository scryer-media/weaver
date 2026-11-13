// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Mutex;

use super::*;

fn open(path: &Path, _length: Option<u64>) -> io::Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

fn create(path: &Path) -> Option<Create> {
    Some(Create {
        path: path.to_path_buf(),
        length: None,
        open,
    })
}

fn writers() -> (VolumeWriters, mpsc::Receiver<Report>) {
    let (send, reports) = mpsc::channel();
    let send = Mutex::new(send);
    let writers = VolumeWriters::new(move |report| {
        let _ = send.lock().unwrap().send(report);
    });
    (writers, reports)
}

fn volume(label: &str) -> Arc<Volume> {
    Arc::new(Volume::new(None, label.to_owned()))
}

#[test]
fn a_barrier_passes_once_every_write_before_it_returned() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 7);
    job.write(1, 0, Bytes::from_static(b"lan"), create(&path));
    job.write(1, 3, Bytes::from_static(b"tern"), None);
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 7);
    let barrier = job.barrier();
    hold.wait_held();
    assert!(!barrier.passed());
    assert!(!path.exists(), "a held write made no file");
    hold.release(1);
    hold.wait_held();
    assert!(!barrier.passed(), "one write of two is still held");
    assert_eq!(std::fs::read(&path).unwrap(), b"lan");
    hold.open();
    barrier.wait();
    assert!(barrier.passed());
    assert_eq!(std::fs::read(&path).unwrap(), b"lantern");
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    let mut failures = 0;
    while volume.written_bytes.load(Ordering::Relaxed) < 7 {
        failures += reports.recv().unwrap().failures.len();
    }
    assert_eq!(failures, 0);
    assert_eq!(volume.written_bytes.load(Ordering::Relaxed), 7);
}

// The order a held batch ran in, by what each write left in the file's
// length: each write is one byte at its offset, and a probe after each
// records how far the file reached.
#[test]
fn a_batch_writes_each_file_by_offset_and_keeps_overlaps_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let (far, near, twice) = (
        dir.path().join("far"),
        dir.path().join("near"),
        dir.path().join("twice"),
    );
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    // One operation is taken and held; the rest queue behind it as a batch.
    job.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    job.write(1, 0, Bytes::from_static(b"0"), create(&far));
    job.write(1, 8, Bytes::from_static(b"8"), None);
    job.write(2, 4, Bytes::from_static(b"4"), create(&near));
    job.write(1, 4, Bytes::from_static(b"4"), None);
    job.write(3, 0, Bytes::from_static(b"old"), create(&twice));
    job.write(3, 1, Bytes::from_static(b"NEW"), None);
    job.write(3, 0, Bytes::from_static(b"x"), None);
    let barrier = job.barrier();
    // The held operation, then the first write of the batch that follows:
    // file 1 goes first, the write that opens it and then lowest offset
    // first.
    hold.release(2);
    hold.wait_held();
    assert_eq!(std::fs::metadata(&far).unwrap().len(), 1);
    assert!(!near.exists(), "one file at a time");
    hold.release(1);
    hold.wait_held();
    assert_eq!(std::fs::metadata(&far).unwrap().len(), 5);
    hold.release(1);
    hold.wait_held();
    assert_eq!(std::fs::metadata(&far).unwrap().len(), 9);
    assert!(!near.exists(), "one file at a time");
    hold.open();
    barrier.wait();
    assert_eq!(std::fs::read(&near).unwrap(), b"\0\0\0\x004");
    assert_eq!(std::fs::read(&twice).unwrap(), b"xNEW");
}

#[test]
fn a_close_and_a_rename_follow_the_writes_before_them() {
    let dir = tempfile::tempdir().unwrap();
    let (from, to) = (dir.path().join("from"), dir.path().join("to"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    job.write(1, 4, Bytes::from_static(b"tail"), create(&from));
    job.close(
        1,
        6,
        Create {
            path: from.clone(),
            length: None,
            open,
        },
    );
    job.rename(Some(1), from.clone(), to.clone());
    job.write(1, 0, Bytes::from_static(b"head"), create(&to));
    let barrier = job.barrier();
    hold.open();
    barrier.wait();
    assert!(!from.exists());
    assert_eq!(std::fs::read(&to).unwrap(), b"headta");
}

#[test]
fn a_failed_write_comes_back_with_its_batch_and_stops_its_key_only() {
    let dir = tempfile::tempdir().unwrap();
    let (bad, good, other) = (
        dir.path().join("bad"),
        dir.path().join("good"),
        dir.path().join("other"),
    );
    let (writers, reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let failing = writers.submitter(&volume, 1);
    let healthy = writers.submitter(&volume, 2);
    failing.write(1, 0, Bytes::from_static(b"lost"), create(&bad));
    failing.write(2, 0, Bytes::from_static(b"never"), create(&good));
    healthy.write(3, 0, Bytes::from_static(b"kept"), create(&other));
    let (failed, kept) = (failing.barrier(), healthy.barrier());
    hold.wait_held();
    hold.fail_next(io::Error::other("no room"));
    hold.open();
    kept.wait();
    let mut failures = Vec::new();
    while failures.is_empty() {
        failures.extend(reports.recv().unwrap().failures);
    }
    assert_eq!(failures.len(), 1);
    assert_eq!((failures[0].key, failures[0].file), (1, Some(1)));
    assert!(failing.failed() && !healthy.failed());
    assert!(failed.returned() && !failed.passed());
    assert!(kept.passed());
    assert!(!good.exists(), "nothing of a failed key is written after");
    assert_eq!(std::fs::read(&other).unwrap(), b"kept");
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
}

#[test]
fn nothing_of_an_abandoned_key_is_written_after() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"one"), create(&path));
    job.write(1, 3, Bytes::from_static(b"two"), None);
    job.rename(Some(1), path.clone(), dir.path().join("b"));
    hold.wait_held();
    hold.release(1);
    hold.wait_held();
    let gone = job.abandon();
    hold.open();
    gone.wait();
    assert_eq!(std::fs::read(&path).unwrap(), b"one");
    assert!(!dir.path().join("b").exists());
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
}

#[test]
fn a_held_volume_does_not_hold_another() {
    let dir = tempfile::tempdir().unwrap();
    let (stuck, free) = (dir.path().join("stuck"), dir.path().join("free"));
    let (writers, _reports) = writers();
    let (slow, fast) = (volume("slow"), volume("fast"));
    let hold = writers.hold(&slow);
    let on_slow = writers.submitter(&slow, 1);
    let on_fast = writers.submitter(&fast, 1);
    on_slow.write(1, 0, Bytes::from_static(b"stuck"), create(&stuck));
    on_fast.write(2, 0, Bytes::from_static(b"free"), create(&free));
    on_fast.barrier().wait();
    assert_eq!(std::fs::read(&free).unwrap(), b"free");
    hold.wait_held();
    assert!(!on_slow.barrier().passed());
    assert_eq!(slow.buffered_bytes.load(Ordering::Relaxed), 5);
    assert_eq!(fast.buffered_bytes.load(Ordering::Relaxed), 0);
    hold.open();
    writers.settle();
    assert_eq!(std::fs::read(&stuck).unwrap(), b"stuck");
}

#[test]
fn a_read_follows_the_writes_submitted_before_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"lantern"), create(&path));
    assert_eq!(job.read(path, 3, 4).unwrap(), b"tern");
}
