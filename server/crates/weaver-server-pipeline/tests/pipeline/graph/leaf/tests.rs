// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use weaver_server_disk::operations::disk::volume::LoopSite;

#[test]
fn moving_a_missing_owned_file_keeps_the_old_name_and_intent() {
    let dir = tempfile::tempdir().unwrap();
    let mut leaf = Leaf::at(dir.path(), "source.bin");
    leaf.write(LoopSite::LeafWrite, 0, b"source", None, false)
        .unwrap();
    leaf.close(LoopSite::LeafWrite, 6, false).unwrap();
    std::fs::remove_file(leaf.path()).unwrap();
    leaf.intend("destination.bin");

    assert!(leaf.move_on().is_err());
    assert_eq!(leaf.name(), "source.bin");
    assert_eq!(leaf.wants(), Some("destination.bin"));
    assert!(!dir.path().join("destination.bin").exists());
}

#[test]
fn moving_an_unwritten_leaf_never_takes_someone_elses_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut leaf = Leaf::at(dir.path(), "source.bin");
    std::fs::write(leaf.path(), b"another owner").unwrap();
    leaf.intend("destination.bin");

    leaf.move_on().unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("source.bin")).unwrap(),
        b"another owner"
    );
    assert!(!dir.path().join("destination.bin").exists());
    assert_eq!(leaf.name(), "destination.bin");
}

#[test]
fn removing_an_unwritten_leaf_preserves_another_owners_file() {
    let dir = tempfile::tempdir().unwrap();
    let leaf = Leaf::at(dir.path(), "source.bin");
    std::fs::write(leaf.path(), b"another owner").unwrap();
    leaf.remove().unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("source.bin")).unwrap(),
        b"another owner"
    );
}

#[cfg(unix)]
#[test]
fn truncation_propagates_inspection_errors() {
    let dir = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink("loop", dir.path().join("loop")).unwrap();
    let mut leaf = Leaf::at(dir.path(), "loop");
    assert!(leaf.truncate(false).is_err());
}

// A name the file system will not take is told apart from a full disk
// and every other error, and nothing is made for it.
#[test]
fn a_refused_name_is_told_apart_from_other_write_errors() {
    let dir = tempfile::tempdir().unwrap();
    let name = format!("{}.mkv", "a".repeat(300));
    let mut leaf = Leaf::at(dir.path(), &name);
    let error = leaf
        .write(LoopSite::LeafWrite, 0, b"lantern", None, false)
        .unwrap_err();
    assert!(error.names_unusable(), "{error}");
    assert!(!leaf.on_disk());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

    let mut full = Leaf::at(dir.path(), "full.bin");
    counting::fill_after(Some(0));
    let error = full
        .write(LoopSite::LeafWrite, 0, b"lantern", None, false)
        .unwrap_err();
    counting::fill_after(None);
    assert!(!error.names_unusable(), "{error}");
}

// A new file starts at its declared length exactly where its
// filesystem makes holes, and is as long as its bytes anywhere else. The
// length is never space: the unwritten tail takes none. Either way
// close cuts the file to its final length.
#[test]
fn a_write_does_not_preallocate_the_declared_unwritten_tail() {
    let dir = tempfile::tempdir().unwrap();
    let sized = weaver_server_disk::pipeline::graph::sparse::sizes_files_in(dir.path());
    let mut leaf = Leaf::at(dir.path(), "payload.bin");
    leaf.write(LoopSite::LeafWrite, 0, b"a", Some(1 << 30), false)
        .unwrap();
    let length = leaf.len(LoopSite::LeafWrite).unwrap();
    assert_eq!(length, if sized { 1 << 30 } else { 1 });
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let taken = std::fs::metadata(leaf.path()).unwrap().blocks() * 512;
        assert!(taken < 1 << 30, "{taken} bytes taken for one written");
    }
    leaf.write(LoopSite::LeafWrite, 1, b"b", None, false)
        .unwrap();
    leaf.close(LoopSite::LeafWrite, 2, false).unwrap();
    assert_eq!(std::fs::read(leaf.path()).unwrap(), b"ab");
}

#[cfg(unix)]
#[test]
fn restoring_a_leaf_propagates_inspection_errors() {
    let dir = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink("loop", dir.path().join("loop")).unwrap();
    assert!(Leaf::found(dir.path(), "loop").is_err());
    assert!(!Leaf::found(dir.path(), "missing").unwrap().on_disk());
}

fn bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|at| (at as u8).wrapping_mul(31) ^ seed)
        .collect()
}

fn file_crc(leaf: &Leaf, length: u64) -> u64 {
    let mut all = Vec::new();
    leaf.read(LoopSite::LeafWrite, 0, index(length), &mut all)
        .unwrap();
    Algorithm::Crc32.hash(&all).value
}

fn folding(leaf: &mut Leaf, offset: u64, bytes: Vec<u8>) {
    let bytes = Evidenced::from(bytes);
    leaf.write_folding(
        LoopSite::LeafWrite,
        offset,
        &bytes,
        Algorithm::Crc32,
        None,
        false,
    )
    .unwrap();
}

// A write over bytes already written, a close that cuts the file short
// and a restore each leave the fold answering for the file, so the
// check at close never reads the member back.
#[test]
fn the_fold_answers_for_the_file_through_rewrites_cuts_and_restores() {
    let dir = tempfile::tempdir().unwrap();
    let mut leaf = Leaf::at(dir.path(), "lantern.bin");
    folding(&mut leaf, 0, bytes(4096, 1));
    folding(&mut leaf, 6000, bytes(2192, 2));
    // Over the end of one run, the gap and the start of the next.
    folding(&mut leaf, 3000, bytes(4000, 3));
    // Inside one run, with other bytes, and with the same bytes.
    folding(&mut leaf, 100, bytes(50, 4));
    folding(&mut leaf, 100, bytes(50, 4));
    let crc = Algorithm::Crc32;
    assert_eq!(leaf.folded(8192, crc), Some(file_crc(&leaf, 8192)));
    leaf.close(LoopSite::LeafWrite, 7000, false).unwrap();
    assert_eq!(leaf.folded(7000, crc), Some(file_crc(&leaf, 7000)));

    // A restore folds what is vouched for, run by run, reading nothing.
    let digest = |from: u64, to: u64| {
        let mut bytes = Vec::new();
        leaf.read(LoopSite::LeafWrite, from, (to - from) as usize, &mut bytes)
            .unwrap();
        crc.hash(&bytes)
    };
    let vouched = [
        (ByteRange::new(0, 3000), digest(0, 3000)),
        (ByteRange::new(3000, 7000), digest(3000, 7000)),
    ];
    let mut restored = Leaf::at(dir.path(), "lantern.bin");
    assert_eq!(restored.folded(7000, crc), None);
    let hashed = weaver_graph::evidence::counting::hashed();
    let (read, written) = counting::so_far();
    restored.fold_vouched(vouched);
    assert_eq!(weaver_graph::evidence::counting::hashed(), hashed);
    assert_eq!(counting::so_far(), (read, written));
    assert_eq!(restored.folded(7000, crc), Some(file_crc(&leaf, 7000)));
    // A run nothing vouches for leaves the fold short.
    let mut short = Leaf::at(dir.path(), "lantern.bin");
    short.fold_vouched([vouched[1]]);
    assert_eq!(short.folded(7000, crc), None);
    // A file cut short since: a run past its length folds nothing, and
    // a write over it later reads nothing that is not there.
    let first = file_crc(&leaf, 3000);
    std::fs::OpenOptions::new()
        .write(true)
        .open(leaf.path())
        .unwrap()
        .set_len(5000)
        .unwrap();
    let mut cut = Leaf::at(dir.path(), "lantern.bin");
    cut.fold_vouched(vouched);
    assert_eq!(cut.folded(3000, crc), Some(first));
    assert_eq!(cut.folded(7000, crc), None);
    folding(&mut cut, 3000, bytes(4000, 3));
    assert_eq!(cut.folded(7000, crc), Some(file_crc(&cut, 7000)));
}

struct Through {
    writers: weaver_server_disk::operations::disk::writer::VolumeWriters,
    volume: Arc<weaver_server_disk::operations::disk::volume::Volume>,
    to: Arc<Submitter>,
}

fn through() -> Through {
    let writers = weaver_server_disk::operations::disk::writer::VolumeWriters::reporting(|_| {});
    let volume = Arc::new(weaver_server_disk::operations::disk::volume::Volume::new(
        None,
        "test".to_owned(),
    ));
    let to = writers.test_submitter(&volume, 1);
    Through {
        writers,
        volume,
        to,
    }
}

impl Through {
    // How many times, and for how long in all, the loop stood for the
    // writer before a read.
    fn read_waits(&self) -> u64 {
        use weaver_server_disk::operations::disk::volume::LoopWait;
        self.volume.writer.loop_waits_by_kind[LoopWait::Read as usize]
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

// A read through a volume's writer waits only for the writes that put
// its bytes there, never behind a later write elsewhere in the file. A
// repair's slices, a replay and a delivery said again each read one
// piece and then write it: while the writer is held on the write of one
// piece, the read back of the next is made at once, the loop goes on
// with another file, and once the writer is let through the file and
// its fold hold every write. The loop stood for the writer not once.
#[test]
fn a_read_back_does_not_wait_for_a_later_write_of_other_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let through = through();
    let mut leaf = Leaf::at(dir.path(), "lantern.bin").through(Some(&through.to));
    let mut other = Leaf::at(dir.path(), "other.bin").through(Some(&through.to));
    folding(&mut leaf, 0, bytes(4096, 1));
    folding(&mut leaf, 4096, bytes(4096, 2));
    other
        .write(LoopSite::LeafWrite, 0, &bytes(4096, 7), None, false)
        .unwrap();
    through.writers.settle();
    let hold = through.writers.hold(&through.volume);
    // Each rewrite reads back what it replaces, folded with a digest,
    // and the second reads behind the first, which the writer holds.
    folding(&mut leaf, 4096, bytes(4096, 3));
    folding(&mut leaf, 0, bytes(4096, 4));
    hold.wait_held();
    let held = through.to.barrier();
    assert!(held.test_held() && !held.returned(), "the rewrites are out");
    // Another file's work goes on meanwhile, its close and a read below
    // the length it closed at included.
    other
        .write(LoopSite::LeafWrite, 4096, &bytes(4096, 8), None, false)
        .unwrap();
    other.close(LoopSite::LeafWrite, 8192, false).unwrap();
    let mut read = Vec::new();
    other.read(LoopSite::LeafWrite, 0, 4096, &mut read).unwrap();
    assert_eq!(read, bytes(4096, 7));
    assert!(!held.returned(), "nothing was let through");
    assert_eq!(through.read_waits(), 0);
    hold.open();
    through.writers.settle();
    let mut all = Vec::new();
    leaf.read(LoopSite::LeafWrite, 0, 8192, &mut all).unwrap();
    assert_eq!(&all[..4096], &bytes(4096, 4)[..]);
    assert_eq!(&all[4096..], &bytes(4096, 3)[..]);
    let crc = Algorithm::Crc32;
    assert_eq!(leaf.folded(8192, crc), Some(crc.hash(&all).value));
    assert_eq!(
        std::fs::read(other.path()).unwrap(),
        [bytes(4096, 7), bytes(4096, 8)].concat()
    );
    assert_eq!(through.read_waits(), 0);
}

// A file handed from one leaf to another keeps what the second writes.
// An archive keeps its stream in a file it starts over and writes the
// head of; refused, it hands that file to its member, whose leaf writes
// the rest and closes it. With both leaves' operations in one batch the
// writer would run the closing leaf first and the first leaf's start
// over last, leaving the head alone. `kept` says which leaf wrote.
fn handed_over(handed: impl FnOnce(&Path, &Arc<Submitter>) -> Leaf) -> (Vec<u8>, Vec<u8>) {
    let dir = tempfile::tempdir().unwrap();
    let through = through();
    let all = bytes(67_584, 5);
    let length = all.len() as u64;
    let mut other = Leaf::at(dir.path(), "other.bin").through(Some(&through.to));
    let hold = through.writers.hold(&through.volume);
    other
        .write(LoopSite::LeafWrite, 0, &bytes(16, 1), None, false)
        .unwrap();
    hold.wait_held();
    let mut first = Leaf::at(dir.path(), "bundle.tar").through(Some(&through.to));
    first.truncate(false).unwrap();
    first
        .write(LoopSite::LeafWrite, 0, &all[..512], None, false)
        .unwrap();
    let mut second = handed(dir.path(), &through.to);
    second
        .write(LoopSite::LeafWrite, 512, &all[512..], Some(length), false)
        .unwrap();
    second.close(LoopSite::LeafWrite, length, false).unwrap();
    first.release_handle();
    hold.open();
    through.writers.settle();
    (std::fs::read(second.path()).unwrap(), all)
}

#[test]
fn a_file_handed_to_another_leaf_keeps_what_that_leaf_writes() {
    let (kept, all) = handed_over(|dir, to| Leaf::handed(dir, "bundle.tar", false, Some(to), 0));
    assert_eq!(kept.len(), all.len());
    assert!(kept == all, "every byte of the stream is kept");
}

// Without the operation between them the first leaf's start over runs
// after the second's close: this is the order `handed` exists to stop.
#[test]
fn two_leaves_over_one_file_are_not_ordered_by_the_writer() {
    let (kept, all) = handed_over(|dir, to| Leaf::at(dir, "bundle.tar").through(Some(to)));
    assert_eq!(kept.len(), 512);
    assert!(kept != all);
}

// A read of bytes whose write is still out waits for that write, and
// reads what it put there; so does a read after anything that shapes
// the file as a whole, a start over here.
#[test]
fn a_read_of_bytes_still_out_waits_for_their_write() {
    let dir = tempfile::tempdir().unwrap();
    let through = through();
    let mut leaf = Leaf::at(dir.path(), "lantern.bin").through(Some(&through.to));
    leaf.write(LoopSite::LeafWrite, 0, &bytes(4096, 1), None, false)
        .unwrap();
    leaf.write(LoopSite::LeafWrite, 4096, &bytes(4096, 2), None, false)
        .unwrap();
    through.writers.settle();
    let hold = through.writers.hold(&through.volume);
    leaf.write(LoopSite::LeafWrite, 1000, &bytes(100, 3), None, false)
        .unwrap();
    let barrier = through.to.barrier();
    hold.wait_held();
    std::thread::scope(|scope| {
        let read = scope.spawn(|| {
            let mut read = Vec::new();
            leaf.read(LoopSite::LeafWrite, 900, 300, &mut read).unwrap();
            read
        });
        barrier.wait_waiters(1);
        hold.open();
        let read = read.join().unwrap();
        assert_eq!(&read[..100], &bytes(4096, 1)[900..1000]);
        assert_eq!(&read[100..200], &bytes(100, 3)[..]);
        assert_eq!(&read[200..], &bytes(4096, 1)[1100..1200]);
    });
    assert_eq!(through.read_waits(), 1);
    through.writers.settle();
    let hold = through.writers.hold(&through.volume);
    leaf.truncate(false).unwrap();
    leaf.write(LoopSite::LeafWrite, 0, &bytes(10, 4), None, false)
        .unwrap();
    let barrier = through.to.barrier();
    hold.wait_held();
    std::thread::scope(|scope| {
        // Bytes from before the start over are not read: the read waits
        // for it, and for the write after it.
        let read = scope.spawn(|| {
            let mut read = Vec::new();
            leaf.read(LoopSite::LeafWrite, 0, 10, &mut read).unwrap();
            read
        });
        barrier.wait_waiters(1);
        hold.open();
        assert_eq!(read.join().unwrap(), bytes(10, 4));
    });
    assert_eq!(through.read_waits(), 2);
}

// What a read waits for: the operation that shaped the file, the writes
// it overlaps, and a close only when it reaches past the closed length.
#[test]
fn a_read_waits_for_what_put_its_bytes_there() {
    let through = through();
    let mut queued = Queued::new(&through.to, 1);
    queued.shape(1);
    queued.wrote(2, 0, 100);
    queued.wrote(3, 100, 200);
    assert_eq!(queued.read_after(0, 50), 2);
    assert_eq!(queued.read_after(150, 160), 3);
    assert_eq!(queued.read_after(50, 150), 3);
    assert_eq!(queued.read_after(300, 400), 1);
    queued.close(4, 150);
    assert_eq!(queued.read_after(0, 50), 2);
    assert_eq!(queued.read_after(100, 150), 3);
    assert_eq!(queued.read_after(100, 151), 4);
    queued.close(5, 300);
    assert_eq!(queued.read_after(140, 200), 5, "cut at 150 before");
    queued.shape(6);
    assert_eq!(queued.read_after(0, 50), 6);
    assert_eq!(queued.read_after(0, 1000), 6);
}

// Runs either side of 4 GiB, in a sparse file that writes a few
// KiB: a write across the line, a rewrite inside it, and a close that
// cuts the run just past it each keep the run's digest equal to the
// bytes the file holds.
#[test]
fn a_fold_across_four_gib_keeps_its_digest() {
    const LINE: u64 = 1 << 32;
    let dir = tempfile::tempdir().unwrap();
    let mut leaf = Leaf::at(dir.path(), "lantern.bin");
    let crc = Algorithm::Crc32;
    let held = |leaf: &Leaf, from: u64, to: u64| {
        let mut bytes = Vec::new();
        leaf.read(LoopSite::LeafWrite, from, index(to - from), &mut bytes)
            .unwrap();
        bytes
    };
    let run = |leaf: &Leaf| {
        assert_eq!(leaf.written.len(), 1, "{:?}", leaf.written);
        let (start, run) = leaf.written.iter().next().unwrap();
        (*start, run.end, run.digest.map(|digest| digest.value))
    };
    let first = bytes(8192, 5);
    folding(&mut leaf, LINE - 4096, first.clone());
    assert_eq!(held(&leaf, LINE - 4096, LINE + 4096), first);
    let (start, end, digest) = run(&leaf);
    assert_eq!((start, end), (LINE - 4096, LINE + 4096));
    assert_eq!(digest, Some(crc.hash(&first).value));

    folding(&mut leaf, LINE - 50, bytes(100, 6));
    let now = held(&leaf, LINE - 4096, LINE + 4096);
    assert_eq!(&now[4046..4146], &bytes(100, 6)[..]);
    assert_eq!(run(&leaf).2, Some(crc.hash(&now).value));

    leaf.close(LoopSite::LeafWrite, LINE + 10, false).unwrap();
    assert_eq!(leaf.len(LoopSite::LeafWrite).unwrap(), LINE + 10);
    let kept = held(&leaf, LINE - 4096, LINE + 10);
    let (start, end, digest) = run(&leaf);
    assert_eq!((start, end), (LINE - 4096, LINE + 10));
    assert_eq!(digest, Some(crc.hash(&kept).value));
}
