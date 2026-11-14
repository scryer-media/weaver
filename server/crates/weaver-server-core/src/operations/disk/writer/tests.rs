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

fn count(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
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
    drop((job, hold));
    writers.wait_ended(&volume);
    assert_eq!(volume.written_bytes.load(Ordering::Relaxed), 7);
    assert!(reports.try_recv().is_err(), "a clean batch says nothing");
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
fn a_close_and_a_move_follow_the_writes_before_them() {
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
    let (moved_from, moved_to) = (from.clone(), to.clone());
    job.run(Some(1), from.clone(), move || {
        std::fs::rename(&moved_from, &moved_to)
    });
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
    assert_eq!(failures[0].path, bad, "the failure names its file");
    assert!(failing.failed() && !healthy.failed());
    assert!(failed.returned() && !failed.passed());
    assert!(kept.passed());
    assert!(!good.exists(), "nothing of a failed key is written after");
    assert_eq!(std::fs::read(&other).unwrap(), b"kept");
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
}

// A forced failure of a write to a file already open, which carries no
// path of its own, names the path the file was opened at.
#[test]
fn a_forced_failure_of_a_later_write_names_the_open_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 4);
    job.write(1, 0, Bytes::from_static(b"head"), create(&path));
    job.write(1, 4, Bytes::from_static(b"tail"), None);
    let barrier = job.barrier();
    hold.wait_held();
    hold.release(1);
    hold.wait_held();
    hold.fail_next(io::Error::other("no room"));
    hold.open();
    barrier.wait();
    let report = reports.recv().unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!((report.failures[0].key, report.failures[0].file), (4, Some(1)));
    assert_eq!(report.failures[0].path, path);
    assert_eq!(std::fs::read(&path).unwrap(), b"head");
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
    let (moved_from, moved_to) = (path.clone(), dir.path().join("b"));
    job.run(Some(1), path.clone(), move || {
        std::fs::rename(&moved_from, &moved_to)
    });
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

#[test]
fn a_keys_barrier_and_read_pass_while_another_keys_operation_is_held() {
    let dir = tempfile::tempdir().unwrap();
    let (stuck, free) = (dir.path().join("stuck"), dir.path().join("free"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let slow = writers.submitter(&volume, 1);
    let quick = writers.submitter(&volume, 2);
    quick.write(0, 0, Bytes::from_static(b"lantern"), create(&free));
    let passed = quick.barrier();
    hold.wait_held();
    hold.release(1);
    slow.write(0, 0, Bytes::from_static(b"stuck"), create(&stuck));
    let held = slow.barrier();
    // The thread stands before the other key's write and does not run it.
    hold.wait_held();
    assert!(passed.passed());
    passed.wait();
    assert_eq!(quick.read(free, 3, 4).unwrap(), b"tern");
    assert!(!held.returned());
    assert!(!stuck.exists());
    hold.open();
    held.wait();
    assert_eq!(std::fs::read(&stuck).unwrap(), b"stuck");
}

#[test]
fn writes_queued_behind_a_held_volume_run_as_one_batch_by_offset() {
    let dir = tempfile::tempdir().unwrap();
    let (first, second) = (dir.path().join("first"), dir.path().join("second"));
    let (writers, reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    // Each file is opened by its first four bytes; the rest are submitted
    // last part first, the two files turn and turn about.
    job.write(1, 0, Bytes::from(vec![0; 4]), create(&first));
    job.write(2, 0, Bytes::from(vec![0; 4]), create(&second));
    for part in (1..32u8).rev() {
        for file in [1, 2] {
            job.write(file, u64::from(part) * 4, Bytes::from(vec![part; 4]), None);
        }
    }
    drop(job);
    hold.open();
    drop(hold);
    writers.wait_ended(&volume);
    let whole: Vec<u8> = (0..32u8).flat_map(|part| [part; 4]).collect();
    assert_eq!(std::fs::read(&first).unwrap(), whole);
    assert_eq!(std::fs::read(&second).unwrap(), whole);
    let counts = &volume.writer;
    // The held operation, then everything that queued behind it.
    assert_eq!(count(&counts.batches), 2);
    assert_eq!(count(&counts.operations), 65);
    assert_eq!(count(&volume.write_calls), 64);
    // Each write began where the one before it on its file ended.
    assert_eq!(count(&volume.write_seeks), 0);
    assert_eq!(count(&volume.idle_turns), 1);
    assert_eq!(count(&counts.threads), 1);
    assert!(reports.try_recv().is_err(), "a clean batch says nothing");
}

#[test]
fn a_wake_asked_for_comes_once_when_the_line_is_crossed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"four"), create(&path));
    job.write(1, 4, Bytes::from_static(b"more"), None);
    job.write(1, 8, Bytes::from_static(b"last"), None);
    assert!(!writers.wake_below(&volume, 12), "already there: no wake");
    assert!(writers.wake_below(&volume, 4));
    assert!(writers.wake_below(&volume, 4), "asked twice, woken once");
    hold.wait_held();
    hold.release(1);
    hold.wait_held();
    assert_eq!(count(&volume.buffered_bytes), 8);
    assert!(reports.try_recv().is_err(), "eight wait, above the line");
    hold.release(1);
    hold.wait_held();
    let report = reports.recv().unwrap();
    assert!(report.below && report.failures.is_empty());
    hold.open();
    drop((job, hold));
    writers.wait_ended(&volume);
    assert_eq!(count(&volume.buffered_bytes), 0);
    assert!(reports.try_recv().is_err(), "one wake for one ask");
}

#[test]
fn a_volume_keeps_one_thread_until_it_ends_and_settle_waits_for_it() {
    let dir = tempfile::tempdir().unwrap();
    let (old, new, later) = (
        dir.path().join("old"),
        dir.path().join("new"),
        dir.path().join("later"),
    );
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let gone = writers.submitter(&volume, 1);
    gone.write(1, 0, Bytes::from_static(b"old"), create(&old));
    drop(gone);
    hold.wait_held();
    // No caller holds the key, and its write is still seen: a settle that
    // gives up at once finds it and the close behind it.
    // The clock and span are chosen beforehand, as a pipeline chooses its own.
    let bound = SettleBound {
        clock: Arc::new(Leaping(AtomicU64::new(0))),
        span: Duration::from_secs(30),
    };
    let left = writers.settle_until(&*bound.clock, bound.end());
    assert_eq!(
        left,
        [Unsettled {
            label: "one".to_owned(),
            operations: 2,
            bytes: 3,
        }]
    );
    let next = writers.submitter(&volume, 2);
    next.write(1, 0, Bytes::from_static(b"new"), create(&new));
    hold.open();
    writers.settle();
    assert_eq!(std::fs::read(&old).unwrap(), b"old");
    assert_eq!(std::fs::read(&new).unwrap(), b"new");
    drop((next, hold));
    writers.wait_ended(&volume);
    assert_eq!(count(&volume.writer.threads), 1);
    // Only a volume whose thread has ended is given another.
    let after = writers.submitter(&volume, 3);
    after.write(1, 0, Bytes::from_static(b"later"), create(&later));
    after.barrier().wait();
    assert_eq!(std::fs::read(&later).unwrap(), b"later");
    assert_eq!(count(&volume.writer.threads), 2);
}

#[test]
fn two_keys_number_their_files_apart() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let (six, seven) = (writers.submitter(&volume, 6), writers.submitter(&volume, 7));
    six.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    // One batch holds both keys' file 0.
    six.write(0, 0, Bytes::from_static(b"AA"), create(&a));
    seven.write(0, 0, Bytes::from_static(b"BB"), create(&b));
    six.write(0, 2, Bytes::from_static(b"AA"), None);
    hold.open();
    six.barrier().wait();
    seven.barrier().wait();
    // And so do two batches.
    seven.write(0, 2, Bytes::from_static(b"BB"), None);
    seven.barrier().wait();
    assert_eq!(std::fs::read(&a).unwrap(), b"AAAA");
    assert_eq!(std::fs::read(&b).unwrap(), b"BBBB");
}

#[test]
fn a_panicking_operation_fails_its_key_and_the_thread_goes_on() {
    let dir = tempfile::tempdir().unwrap();
    let (lost, kept) = (dir.path().join("lost"), dir.path().join("kept"));
    let (writers, reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let broken = writers.submitter(&volume, 1);
    broken.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    broken.write(1, 0, Bytes::from_static(b"first"), create(&lost));
    broken.run(None, lost.clone(), || panic!("an operation that panics"));
    broken.write(1, 5, Bytes::from_static(b"never"), None);
    let failed = broken.barrier();
    hold.open();
    failed.wait();
    assert!(failed.returned() && !failed.passed());
    assert!(broken.failed());
    assert_eq!(count(&volume.buffered_bytes), 0);
    assert_eq!(std::fs::read(&lost).unwrap(), b"first");
    let report = reports.recv().unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].key, 1);
    assert_eq!(count(&volume.writer.failed_operations), 1);
    let healthy = writers.submitter(&volume, 2);
    healthy.write(1, 0, Bytes::from_static(b"kept"), create(&kept));
    let kept_barrier = healthy.barrier();
    kept_barrier.wait();
    assert!(kept_barrier.passed());
    assert_eq!(std::fs::read(&kept).unwrap(), b"kept");
    assert_eq!(count(&volume.writer.threads), 1);
}

#[test]
fn a_bounded_settle_gives_up_on_a_volume_that_holds_writes_at_its_end() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"stuck"), create(&path));
    hold.wait_held();
    let clock = HandClock::default();
    let span = Duration::from_secs(30);
    let over = AtomicBool::new(false);
    let left = std::thread::scope(|scope| {
        let settling = scope.spawn(|| {
            let left = writers.settle_until(&clock, span);
            over.store(true, Ordering::SeqCst);
            left
        });
        // One look before the settle waits.
        clock.wait_looks(1);
        clock.move_to(Duration::from_secs(29));
        writers.look_again();
        clock.wait_looks(2);
        assert!(!over.load(Ordering::SeqCst), "the span has not passed");
        clock.move_to(span);
        writers.look_again();
        settling.join().unwrap()
    });
    assert_eq!(
        left,
        [Unsettled {
            label: "one".to_owned(),
            operations: 1,
            bytes: 5,
        }]
    );
    // What was left is still written once the volume moves.
    hold.open();
    writers.settle();
    assert_eq!(std::fs::read(&path).unwrap(), b"stuck");
}

// Settles given one end share one span. A write that returns just before
// the end does not move it, and a settle begun after it gives up at once.
#[test]
fn settles_that_share_an_end_give_up_at_it_whatever_returned_meanwhile() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"first"), create(&a));
    job.write(2, 0, Bytes::from_static(b"stuck"), create(&b));
    hold.wait_held();
    let clock = HandClock::default();
    let end = Duration::from_secs(30);
    let left = std::thread::scope(|scope| {
        let settling = scope.spawn(|| writers.settle_until(&clock, end));
        // One look before the settle waits.
        clock.wait_looks(1);
        // A second before the end one write goes through.
        clock.move_to(Duration::from_secs(29));
        hold.release(1);
        hold.wait_held();
        clock.move_to(end);
        writers.look_again();
        settling.join().unwrap()
    });
    assert_eq!(left.len(), 1, "{left:?}");
    assert_eq!(left[0].label, "one");
    assert!(left[0].operations > 0, "{left:?}");
    // Nothing of the span is left for the settle that follows.
    assert_eq!(writers.settle_until(&clock, end), left);
    // What was left is still written once the volume moves.
    hold.open();
    writers.settle();
    assert_eq!(std::fs::read(&a).unwrap(), b"first");
    assert_eq!(std::fs::read(&b).unwrap(), b"stuck");
}

#[test]
fn the_counters_hold_what_a_scripted_run_did() {
    let dir = tempfile::tempdir().unwrap();
    let (a, x, z) = (dir.path().join("a"), dir.path().join("x"), dir.path().join("z"));
    let (writers, reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let failing = writers.submitter(&volume, 0);
    let ordered = writers.submitter(&volume, 1);
    let seeking = writers.submitter(&volume, 2);
    let given_up = writers.submitter(&volume, 3);
    failing.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    hold.fail_next(io::Error::other("no room"));
    // Three writes that leave no gap once ordered.
    ordered.write(1, 0, Bytes::from_static(b"aaaa"), create(&a));
    ordered.write(1, 8, Bytes::from_static(b"cccc"), None);
    ordered.write(1, 4, Bytes::from_static(b"bbbb"), None);
    // Two with a gap between them.
    seeking.write(1, 0, Bytes::from_static(b"xx"), create(&x));
    seeking.write(1, 10, Bytes::from_static(b"yy"), None);
    // Two that are given up before they run.
    given_up.write(1, 0, Bytes::from_static(b"zz"), create(&z));
    given_up.write(1, 2, Bytes::from_static(b"zz"), None);
    given_up.abandon();
    drop((failing, ordered, seeking, given_up));
    hold.open();
    drop(hold);
    writers.wait_ended(&volume);
    assert_eq!(std::fs::read(&a).unwrap(), b"aaaabbbbcccc");
    assert_eq!(std::fs::read(&x).unwrap(), b"xx\0\0\0\0\0\0\0\0yy");
    assert!(!z.exists());
    let counts = &volume.writer;
    assert_eq!(count(&counts.batches), 2);
    assert_eq!(count(&counts.operations), 6);
    assert_eq!(count(&counts.failed_operations), 1);
    assert_eq!(count(&counts.abandoned_operations), 2);
    assert_eq!(count(&volume.write_calls), 5);
    assert_eq!(count(&volume.write_seeks), 1);
    assert_eq!(count(&volume.written_bytes), 16);
    assert_eq!(count(&volume.idle_turns), 1);
    assert_eq!(count(&counts.threads), 1);
    assert_eq!(count(&volume.buffered_bytes), 0);
    let report = reports.recv().unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].key, 0);
    assert!(reports.try_recv().is_err(), "one report, for the one failure");
}

// A volume no thread can be started for is asked for one once, however many
// callers come while it is held. Each of them fails at once and hears of it
// once, nothing waits for what they sent, and a thread is asked for again
// only by the caller that comes after all of them have let go.
#[test]
fn a_volume_with_no_thread_is_asked_for_one_again_only_once_it_was_let_go() {
    let dir = tempfile::tempdir().unwrap();
    let (first_path, second_path) = (dir.path().join("first"), dir.path().join("second"));
    let (send, reports) = mpsc::channel();
    let send = Mutex::new(send);
    let writers = VolumeWriters::threadless(move |report| {
        let _ = send.lock().unwrap().send(report);
    });
    let volume = volume("one");
    let starts = || count(&writers.registry.starts);

    let first = writers.submitter(&volume, 1);
    let second = writers.submitter(&volume, 2);
    let own = writers.volume_submitter(&volume);
    assert_eq!(starts(), 1, "one ask for the three of them");

    first.write(1, 0, Bytes::from_static(b"lost"), create(&first_path));
    first.write(1, 4, Bytes::from_static(b"too"), None);
    second.write(1, 0, Bytes::from_static(b"lost"), create(&second_path));
    own.run(None, dir.path().to_path_buf(), || Ok(()));
    for job in [&first, &second, &own] {
        let barrier = job.barrier();
        assert!(barrier.returned() && !barrier.passed());
    }
    assert!(first.failed() && second.failed());
    // Each key heard once, of the first thing it sent.
    let mut failures: Vec<(u64, PathBuf)> = reports
        .try_iter()
        .flat_map(|report| report.failures)
        .map(|failure| (failure.key, failure.path))
        .collect();
    failures.sort();
    assert_eq!(
        failures,
        [
            (1, first_path.clone()),
            (2, second_path.clone()),
            (VOLUME_KEY, dir.path().to_path_buf()),
        ]
    );
    assert_eq!(count(&volume.buffered_bytes), 0);
    assert!(!first_path.exists() && !second_path.exists());
    // Nothing is waited for, with or without a span.
    writers.settle();
    assert!(
        writers
            .settle_until(&HandClock::default(), Duration::from_secs(30))
            .is_empty()
    );
    assert_eq!(starts(), 1);
    assert_eq!(count(&volume.writer.threads), 0);

    // One caller lets go and another comes: the volume is still held.
    drop(first);
    let third = writers.submitter(&volume, 3);
    assert_eq!(starts(), 1);

    // All let go: the next caller asks again, once.
    drop((second, own, third));
    writers.wait_ended(&volume);
    let fourth = writers.submitter(&volume, 4);
    let fifth = writers.submitter(&volume, 5);
    assert_eq!(starts(), 2);
    drop((fourth, fifth));
}

// A thread that produces what is written waits on the volume's budget: it
// stands still while more than its line waits for the writer, and goes on
// as soon as the writer brings the bytes to the line.
#[test]
fn a_producer_waits_until_the_bytes_waiting_fall_to_its_line() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"four"), create(&path));
    job.write(1, 4, Bytes::from_static(b"more"), None);
    job.write(1, 8, Bytes::from_static(b"last"), None);
    // Twelve wait: a line of twelve or more is already met.
    job.wait_below(12);
    hold.wait_held();
    let through = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let producer = scope.spawn(|| {
            job.wait_below(4);
            through.store(true, Ordering::SeqCst);
        });
        hold.wait_below_waiters(1);
        hold.release(1);
        hold.wait_held();
        assert_eq!(count(&volume.buffered_bytes), 8);
        assert!(!through.load(Ordering::SeqCst), "eight wait, above the line");
        hold.release(1);
        producer.join().unwrap();
    });
    assert!(through.load(Ordering::SeqCst));
    assert!(count(&volume.buffered_bytes) <= 4);
    // The wait was the producer's, not the pipeline loop's.
    assert_eq!(count(&volume.writer.loop_waits), 0);
    hold.open();
    job.barrier().wait_aside();
    assert_eq!(std::fs::read(&path).unwrap(), b"fourmorelast");
}

// The wait ends when the key stops, whatever still waits for the writer: by
// a failure of one of its operations, or by its caller giving it up.
#[test]
fn a_producers_wait_ends_when_its_key_fails_or_is_given_up() {
    let dir = tempfile::tempdir().unwrap();
    let (failing_path, given_path, other_path) = (
        dir.path().join("failing"),
        dir.path().join("given"),
        dir.path().join("other"),
    );
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let failing = writers.submitter(&volume, 1);
    let given_up = writers.submitter(&volume, 2);
    let other = writers.submitter(&volume, 3);
    failing.write(1, 0, Bytes::from_static(b"lost"), create(&failing_path));
    hold.wait_held();
    given_up.write(1, 0, Bytes::from_static(b"gone"), create(&given_path));
    other.write(1, 0, Bytes::from_static(b"kept"), create(&other_path));
    std::thread::scope(|scope| {
        let after_failure = scope.spawn(|| failing.wait_below(0));
        let after_giving_up = scope.spawn(|| given_up.wait_below(0));
        hold.wait_below_waiters(2);
        // The first write fails. The other keys' bytes still wait.
        hold.fail_next(io::Error::other("no room"));
        hold.release(1);
        after_failure.join().unwrap();
        hold.wait_held();
        assert_eq!(count(&volume.buffered_bytes), 8);
        assert!(failing.barrier().failed());
        assert!(!given_up.barrier().failed() && !after_giving_up.is_finished());
        let gone = given_up.abandon();
        after_giving_up.join().unwrap();
        assert!(gone.failed());
    });
    assert!(!other.barrier().failed());
    hold.open();
    other.barrier().wait_aside();
    assert_eq!(std::fs::read(&other_path).unwrap(), b"kept");
    assert!(!given_path.exists());
}

// A volume with no writer thread holds no producer: nothing will be written.
#[test]
fn a_producer_does_not_wait_on_a_volume_with_no_thread() {
    let dir = tempfile::tempdir().unwrap();
    let writers = VolumeWriters::threadless(|_| {});
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    // Bytes counted as waiting by something else do not hold it either.
    volume.buffered_bytes.store(64, Ordering::SeqCst);
    job.wait_below(0);
    job.write(1, 0, Bytes::from_static(b"lost"), create(&dir.path().join("a")));
    job.wait_below(0);
    assert!(job.barrier().failed());
}

// A refusal still to be heard is not a failure of the key.
#[test]
fn a_refused_name_is_not_a_failed_barrier() {
    fn refuse(_: &Path, _: Option<u64>) -> io::Result<File> {
        Err(io::ErrorKind::InvalidFilename.into())
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    job.write(
        1,
        0,
        Bytes::from_static(b"name"),
        Some(Create {
            path: path.clone(),
            length: None,
            open: refuse,
        }),
    );
    // The later writes of the refused file are dropped without a word.
    job.write(1, 4, Bytes::from_static(b"more"), None);
    let barrier = job.barrier();
    barrier.wait_aside();
    assert!(!barrier.failed() && !barrier.passed());
    let report = reports.recv().unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(report.failures[0].creating);
    job.heard_refusal();
    assert!(barrier.passed());
    assert!(reports.try_recv().is_err(), "the refusal was the only word");
}

// A write that reaches the writer for a file that is not open, with no way
// to open it and no refusal on record, fails its key: it is not dropped.
#[test]
fn a_write_to_a_file_that_is_not_open_fails_its_key() {
    let dir = tempfile::tempdir().unwrap();
    let (path, after) = (dir.path().join("a"), dir.path().join("after"));
    let (writers, reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"head"), create(&path));
    job.release(1);
    // The handle is closed, and this write brings no way to open it again.
    job.write(1, 4, Bytes::from_static(b"tail"), None);
    job.write(2, 0, Bytes::from_static(b"never"), create(&after));
    let barrier = job.barrier();
    barrier.wait_aside();
    assert!(barrier.failed());
    // The report follows the batch the failure was in.
    let failures = reports.recv().unwrap().failures;
    assert_eq!(failures.len(), 1);
    assert_eq!((failures[0].key, failures[0].file), (1, Some(1)));
    assert!(!failures[0].creating);
    assert_eq!(std::fs::read(&path).unwrap(), b"head");
    assert!(!after.exists(), "nothing of a failed key is written after");
    assert_eq!(count(&volume.buffered_bytes), 0);
}
