// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Mutex;

use super::*;

// Opens the file with nothing said of its length: every close sets it.
fn open(path: &Path, _length: Option<u64>) -> io::Result<Opened> {
    Ok(Opened {
        file: open_file(path)?,
        length: None,
        steps: OpenSteps::default(),
    })
}

fn open_file(path: &Path) -> io::Result<File> {
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

// A written buffer is let go on the writer as its write returns, so its
// memory goes back while it is warm: once the write is waited on, the
// caller's own share is the only one left.
#[test]
fn a_written_buffer_is_let_go_on_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 7);
    let bytes = Bytes::from(b"lantern".to_vec());
    job.write(1, 0, bytes.clone(), create(&path));
    job.barrier().wait();
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    assert!(bytes.is_unique(), "the writer holds no share once written");
    assert_eq!(std::fs::read(&path).unwrap(), b"lantern");
}

// Opens a file a second after the writer's clock, as a slow open would.
fn open_slowly(path: &Path, length: Option<u64>) -> io::Result<Opened> {
    step_clock(Duration::from_secs(1));
    open(path, length)
}

// A volume's write time is the time inside its write calls: an open between
// two writes of one batch is not counted. Every read of the stepped clock
// moves it a millisecond, and each open a second.
#[test]
fn a_batch_counts_only_the_time_inside_its_write_calls() {
    let dir = tempfile::tempdir().unwrap();
    let (first, second) = (dir.path().join("first"), dir.path().join("second"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    let slow = |path: &Path| {
        Some(Create {
            path: path.to_path_buf(),
            length: None,
            open: open_slowly,
        })
    };
    job.write(1, 0, Bytes::from_static(b"lan"), slow(&first));
    job.write(1, 3, Bytes::from_static(b"tern"), None);
    job.write(2, 0, Bytes::from_static(b"wick"), slow(&second));
    let barrier = job.barrier();
    hold.open();
    barrier.wait();
    assert_eq!(std::fs::read(&first).unwrap(), b"lantern");
    assert_eq!(std::fs::read(&second).unwrap(), b"wick");
    assert_eq!(count(&volume.write_calls), 3);
    assert_eq!(
        count(&volume.write_nanos),
        3 * Duration::from_millis(1).as_nanos() as u64,
        "a millisecond for each call, none of either open's second"
    );
}

// A write chained from the last one's return does not count what ran on the
// writer between them past a few loads and stores: here a caller's code
// that takes a second, run as the first write returns.
#[test]
fn a_callers_code_between_two_writes_is_not_write_time() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    // The open starts the stepped clock.
    let slow = Some(Create {
        path: path.clone(),
        length: None,
        open: open_slowly,
    });
    job.write(1, 0, Bytes::from_static(b"lan"), slow);
    job.barrier().then(|| step_clock(Duration::from_secs(1)));
    job.write(1, 3, Bytes::from_static(b"tern"), None);
    let barrier = job.barrier();
    hold.open();
    barrier.wait();
    // A batch is counted after its operations return.
    hold.wait_parked();
    assert_eq!(std::fs::read(&path).unwrap(), b"lantern");
    assert_eq!(count(&volume.write_calls), 2);
    assert_eq!(count(&volume.writer.batches), 2, "both writes in one batch");
    assert_eq!(
        count(&volume.write_nanos),
        2 * Duration::from_millis(1).as_nanos() as u64,
        "a millisecond for each call, none of the code's second"
    );
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

// A reader on a thread of its own, a repair or a check, waits for the bytes
// and the length of what it reads, not for the writer to drop the handle a
// release let go: that drop is slow on some systems for a large file just
// written, and a reader opens its own handle. The key is still told its
// operations returned only after the drop, so a settle, a checkpoint and a
// delivery wait for it as before.
#[test]
fn a_reader_does_not_wait_behind_the_drop_of_a_closed_files_handle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    hold.hold_drops(true);
    hold.open();
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"head"), create(&path));
    job.close(
        1,
        6,
        Create {
            path: path.clone(),
            length: None,
            open,
        },
    );
    job.release(1);
    // The release ran; the writer stands before dropping its handle.
    hold.wait_dropping();
    let barrier = job.barrier();
    assert!(!barrier.returned() && !barrier.passed());
    let finished = std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            barrier.wait_aside();
            (barrier.unwritten(), std::fs::read(&path).unwrap())
        });
        // The reader is done, or it stands on the key behind the drop.
        while !reader.is_finished() && barrier.key.waiters.load(Ordering::SeqCst) == 0 {
            std::thread::yield_now();
        }
        let finished = reader.is_finished();
        hold.hold_drops(false);
        let (unwritten, bytes) = reader.join().unwrap();
        assert!(!unwritten);
        assert_eq!(bytes, b"head\0\0");
        finished
    });
    assert!(finished, "the reader waited behind the drop of a handle");
    barrier.wait();
    assert!(barrier.passed());
    assert_eq!(count(&volume.writer.aside_waits), 0);
}

// A key that let a file go waits behind its own handle's drop only: once
// the batch runs nothing more of it, the handle is dropped and the key
// returned, and the volume's settle has gone past it, while the rest of
// the batch, another key's writes, is still to run.
#[test]
fn a_closed_handle_is_dropped_once_the_batch_runs_nothing_more_of_its_key() {
    let dir = tempfile::tempdir().unwrap();
    let (closed_path, other_path) = (dir.path().join("closed"), dir.path().join("other"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let closing = writers.submitter(&volume, 1);
    let other = writers.submitter(&volume, 2);
    // The thread stands in a batch of its own before what follows comes.
    other.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    closing.write(1, 0, Bytes::from_static(b"head"), create(&closed_path));
    closing.close(
        1,
        4,
        Create {
            path: closed_path.clone(),
            length: None,
            open,
        },
    );
    closing.release(1);
    other.write(1, 0, Bytes::from_static(b"more"), create(&other_path));
    other.write(1, 4, Bytes::from_static(b"tail"), None);
    let state = &writers.lane(&volume).state;
    hold.release(1);
    hold.wait_held();
    assert_eq!(state.returned.load(Ordering::SeqCst), 1);
    // The closing key's write, close and release run; the other key's are
    // next.
    hold.release(3);
    hold.wait_held();
    let closed = closing.barrier();
    assert!(
        closed.returned() && closed.passed(),
        "the key waited for the batch"
    );
    assert!(!other.barrier().returned());
    assert_eq!(
        state.returned.load(Ordering::SeqCst),
        4,
        "the settle went past it"
    );
    assert!(closing.barrier().key.held().letting_go.is_empty());
    hold.open();
    other.barrier().wait();
    assert_eq!(std::fs::read(&closed_path).unwrap(), b"head");
    assert_eq!(std::fs::read(&other_path).unwrap(), b"moretail");
}

// A caller's code of a key that failed is not run, and is dropped with the
// key's handles already dropped, as if it had run: its caller may move or
// remove the key's files at once on a thread of its own.
#[test]
fn a_stopped_keys_code_is_dropped_after_the_handles_the_key_let_go() {
    let dir = tempfile::tempdir().unwrap();
    let (closed_path, failing_path) = (dir.path().join("closed"), dir.path().join("failing"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    hold.hold_drops(true);
    let job = writers.submitter(&volume, 1);
    let plug = writers.submitter(&volume, 2);
    plug.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    job.write(1, 0, Bytes::from_static(b"head"), create(&closed_path));
    job.close(
        1,
        4,
        Create {
            path: closed_path.clone(),
            length: None,
            open,
        },
    );
    job.release(1);
    job.write(2, 0, Bytes::from_static(b"lost"), create(&failing_path));
    let ran = Arc::new(AtomicBool::new(false));
    let marked = Arc::clone(&ran);
    job.run(None, dir.path().to_path_buf(), move || {
        marked.store(true, Ordering::SeqCst);
        Ok(())
    });
    hold.release(1);
    hold.wait_held();
    // The write, the close and the release run; the next write fails the
    // key.
    hold.release(3);
    hold.wait_held();
    assert_eq!(job.barrier().key.held().letting_go.len(), 1);
    hold.fail_next(io::Error::other("no room"));
    hold.release(1);
    hold.wait_held();
    // The code is dropped unrun, and with it the handle the key let go:
    // the thread goes on to the next batch without standing to drop it.
    plug.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.release(1);
    assert!(!hold.wait_held_or_dropping(), "a handle outlived the code");
    let barrier = job.barrier();
    assert!(barrier.failed() && barrier.returned());
    assert!(!ran.load(Ordering::SeqCst));
    assert!(barrier.key.held().letting_go.is_empty());
    hold.hold_drops(false);
    hold.open();
    plug.barrier().wait();
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
    assert_eq!(
        (report.failures[0].key, report.failures[0].file),
        (4, Some(1))
    );
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

// The loop reads a file after that file's own writes, not after every
// write its key queued on the volume: a repair's read of one file does not
// stand behind a stalled write of another file of the same job. The slow
// disk run's loop stood 66.5 s in 289 waits, most of them while repairs read
// back with the buffer near empty.
#[test]
fn a_read_after_a_files_own_writes_passes_while_its_keys_write_of_another_file_is_held() {
    let dir = tempfile::tempdir().unwrap();
    let (read, stuck) = (dir.path().join("read"), dir.path().join("stuck"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    let wrote = job.write(1, 0, Bytes::from_static(b"lantern"), create(&read));
    hold.wait_held();
    hold.release(1);
    job.write(2, 0, Bytes::from_static(b"stuck"), create(&stuck));
    // The thread stands before the key's write of the other file.
    hold.wait_held();
    assert_eq!(job.read_after(wrote, read.clone(), 3, 4).unwrap(), b"tern");
    assert!(!stuck.exists());
    assert_eq!(count(&volume.writer.loop_waits), 0, "the read did not wait");
    hold.open();
    writers.settle();
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
    let (a, x, z) = (
        dir.path().join("a"),
        dir.path().join("x"),
        dir.path().join("z"),
    );
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
    assert!(
        reports.try_recv().is_err(),
        "one report, for the one failure"
    );
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
        assert!(
            !through.load(Ordering::SeqCst),
            "eight wait, above the line"
        );
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

// Stand until `count` threads wait on the barrier's key.
fn key_waiters(barrier: &Barrier, count: usize) {
    while barrier.key.waiters.load(Ordering::SeqCst) < count {
        std::thread::yield_now();
    }
}

// A wait made off the pipeline loop is not a loop wait: neither the wait of
// a thread of its own on a barrier, nor a settle.
#[test]
fn a_wait_off_the_loop_is_not_counted_as_a_loop_wait() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"lantern"), create(&path));
    let barrier = job.barrier();
    hold.wait_held();
    std::thread::scope(|scope| {
        let aside = scope.spawn(|| barrier.wait_aside());
        let settle = scope.spawn(|| writers.settle());
        // Both stand on the held write before it is let through.
        key_waiters(&barrier, 1);
        while hold.0.state.waiters.load(Ordering::SeqCst) < 1 {
            std::thread::yield_now();
        }
        assert!(!barrier.returned());
        hold.open();
        aside.join().unwrap();
        settle.join().unwrap();
    });
    assert!(barrier.passed());
    assert_eq!(count(&volume.writer.loop_waits), 0);
    assert_eq!(count(&volume.writer.loop_wait_nanos), 0);
    // The thread aside stood once, and is counted apart; a wait on a
    // barrier already passed stands for nothing and counts nothing.
    assert_eq!(count(&volume.writer.aside_waits), 1);
    assert!(count(&volume.writer.aside_wait_nanos) > 0);
    barrier.wait_aside();
    assert_eq!(count(&volume.writer.aside_waits), 1);
}

// The loop's own wait on a barrier, and its read behind queued writes, are
// each one loop wait.
#[test]
fn the_loops_wait_and_read_are_each_counted_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"lantern"), create(&path));
    let barrier = job.barrier();
    hold.wait_held();
    std::thread::scope(|scope| {
        let waits = scope.spawn(|| barrier.wait());
        key_waiters(&barrier, 1);
        hold.release(1);
        waits.join().unwrap();
    });
    assert_eq!(count(&volume.writer.loop_waits), 1);
    // A wait that finds everything returned is no wait.
    barrier.wait();
    assert_eq!(job.read(path.clone(), 3, 4).unwrap(), b"tern");
    assert_eq!(count(&volume.writer.loop_waits), 1);
    job.write(1, 7, Bytes::from_static(b"s"), None);
    let after = job.barrier();
    hold.wait_held();
    std::thread::scope(|scope| {
        let reads = scope.spawn(|| job.read(path.clone(), 0, 8).unwrap());
        key_waiters(&after, 1);
        hold.open();
        assert_eq!(reads.join().unwrap(), b"lanterns");
    });
    assert_eq!(count(&volume.writer.loop_waits), 2);
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

// Once the owner is stopping, no thread but the loop stands still for a
// volume: a producer over its line goes on, and a reader waiting for its
// operations finds them still out. Neither waits again after. What the
// volume holds is still written once its thread moves.
#[test]
fn a_stop_ends_the_waits_of_producers_and_readers_on_a_volume_that_stands_still() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let (volume, later) = (volume("one"), volume("two"));
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"held"), create(&path));
    hold.wait_held();
    let barrier = job.barrier();
    std::thread::scope(|scope| {
        let producer = scope.spawn(|| job.wait_below(0));
        let reader = scope.spawn(|| barrier.wait_aside());
        hold.wait_below_waiters(1);
        while barrier.key.waiters.load(Ordering::SeqCst) == 0 {
            std::thread::yield_now();
        }
        writers.stop_waits();
        producer.join().unwrap();
        reader.join().unwrap();
    });
    assert!(!barrier.returned() && !barrier.failed());
    assert_eq!(count(&volume.buffered_bytes), 4);
    // A wait begun after the stop does not stand still either, on this
    // volume or on one first used after it.
    job.wait_below(0);
    barrier.wait_aside();
    let later_hold = writers.hold(&later);
    let other = writers.submitter(&later, 2);
    other.write(
        1,
        0,
        Bytes::from_static(b"more"),
        create(&dir.path().join("b")),
    );
    other.wait_below(0);
    other.barrier().wait_aside();
    assert!(!other.barrier().returned());
    assert_eq!(count(&volume.writer.loop_waits), 0);
    later_hold.open();
    hold.open();
    writers.settle();
    assert!(barrier.returned() && !barrier.failed());
    assert_eq!(std::fs::read(&path).unwrap(), b"held");
}

// The loop's own wait on a volume's limit is not a producer's: the stop
// that sends a producer on leaves the loop standing, so what the loop says
// again while the owner stops is still held to the limit. The wait ends at
// the end the stop gives its volumes, with the bytes still over, and the
// key says it was left there.
#[test]
fn the_loops_wait_on_a_limit_outlasts_the_stop_and_ends_at_the_stops_end() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"held"), create(&path));
    hold.wait_held();
    // The four bytes held fill the volume: one more does not fit.
    volume.set_buffer_limits(4, 4);
    let clock = Arc::new(HandClock::default());
    let end = Duration::from_secs(30);
    writers.end_loop_waits_at(Arc::clone(&clock) as Arc<dyn Clock + Send>, end);
    std::thread::scope(|scope| {
        let producer = scope.spawn(|| job.wait_below(0));
        let looped = scope.spawn(|| job.loop_wait_room(1));
        hold.wait_below_waiters(2);
        // The loop's wait read the clock once before it stood still.
        clock.wait_looks(1);
        writers.stop_waits();
        producer.join().unwrap();
        // Woken by the stop, the loop's wait read the clock again and
        // stands: it did not go on as the producer did.
        clock.wait_looks(2);
        assert!(!job.left());
        clock.move_to(end);
        writers.look_again();
        assert!(!looped.join().unwrap(), "the wait was given up at the end");
    });
    assert!(job.left());
    assert_eq!(count(&volume.buffered_bytes), 4);
    // Past the end the loop does not stand still again, and queues nothing.
    assert!(!job.loop_wait_room(1));
    hold.open();
    writers.settle();
    assert_eq!(std::fs::read(&path).unwrap(), b"held");
    // With room the loop's wait says so, whatever the clock reads.
    assert!(job.loop_wait_room(1));
}

// The loop's read of a file whose writes sit on a stuck volume does not
// stand for good once the owner is stopping: it ends at the end the stop
// gives its volumes, with an error and the key left, and nothing of the key
// failed. A later wait of the loop on the same writes does not stand at
// all, and says they are unwritten. The bytes are written when the volume
// gets to them.
#[test]
fn the_loops_read_of_a_stuck_volume_ends_at_the_stops_end() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    let wrote = job.write(1, 0, Bytes::from_static(b"held"), create(&path));
    hold.wait_held();
    let barrier = job.barrier_at(wrote);
    let clock = Arc::new(HandClock::default());
    let end = Duration::from_secs(30);
    writers.end_loop_waits_at(Arc::clone(&clock) as Arc<dyn Clock + Send>, end);
    std::thread::scope(|scope| {
        let read = scope.spawn(|| job.read_after(wrote, path.clone(), 0, 4));
        // The read looked at the clock once, short of the end, and stands
        // under the key's lock, which the wake below takes.
        clock.wait_looks(1);
        // The stop that sends producers on is not what ends it.
        writers.stop_waits();
        clock.move_to(end);
        barrier.look_again();
        let error = read.join().unwrap().unwrap_err();
        assert!(error.to_string().contains("still out"), "{error}");
    });
    // The wait is counted as a read, the kind the metrics label it.
    let kind = |kind: LoopWait| count(&volume.writer.loop_waits_by_kind[kind as usize]);
    assert_eq!(
        (
            kind(LoopWait::Read),
            kind(LoopWait::Settle),
            kind(LoopWait::Budget)
        ),
        (1, 0, 0)
    );
    assert!(job.left());
    assert!(!barrier.failed(), "nothing of the key failed");
    // Past the end the loop does not stand again.
    barrier.wait();
    assert!(barrier.unwritten());
    assert!(job.read_after(wrote, path.clone(), 0, 4).is_err());
    hold.open();
    writers.settle();
    assert_eq!(std::fs::read(&path).unwrap(), b"held");
    assert!(!barrier.unwritten());
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
    job.write(
        1,
        0,
        Bytes::from_static(b"lost"),
        create(&dir.path().join("a")),
    );
    job.wait_below(0);
    assert!(job.barrier().failed());
}

// An operation its caller makes itself when the volume drops it is no
// failure on a volume with no thread: the key does not fail, nothing is
// reported under it, and a barrier behind it passes.
#[test]
fn an_operation_its_caller_makes_itself_is_no_failure_on_a_volume_with_no_thread() {
    let reported = Arc::new(AtomicU64::new(0));
    let count_reports = Arc::clone(&reported);
    let writers = VolumeWriters::threadless(move |report| {
        count_reports.fetch_add(report.failures.len() as u64, Ordering::SeqCst);
    });
    let volume = volume("one");
    let job = writers.submitter(&volume, 7);
    let (answer, answered) = mpsc::sync_channel(1);
    job.ask(PathBuf::from("delivered"), move || {
        let _ = answer.send(());
    });
    // Dropped unrun: the caller hears nothing and makes it itself.
    assert!(answered.recv().is_err());
    let barrier = job.barrier();
    assert!(barrier.passed() && !barrier.failed());
    assert_eq!(count(&reported), 0);
    assert_eq!(count(&volume.writer.failed_operations), 0);
    // Anything else of the key still fails as it did.
    job.run(None, PathBuf::from("other"), || Ok(()));
    assert!(job.barrier().failed());
    assert_eq!(count(&reported), 1);
}

// A refusal still to be heard is not a failure of the key.
#[test]
fn a_refused_name_is_not_a_failed_barrier() {
    fn refuse(_: &Path, _: Option<u64>) -> io::Result<Opened> {
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

// What a settle of `job`'s volume would still wait for, each time a failure
// is reported under key 1: taken inside the report.
type Outstanding = Arc<Mutex<Vec<u64>>>;

fn reporting_what_is_outstanding(
    make: fn(Box<dyn Fn(Report) + Send + Sync>) -> VolumeWriters,
) -> (VolumeWriters, Arc<Mutex<Option<Arc<Lane>>>>, Outstanding) {
    let lane: Arc<Mutex<Option<Arc<Lane>>>> = Arc::default();
    let seen = Outstanding::default();
    let (at, into) = (Arc::clone(&lane), Arc::clone(&seen));
    let writers = make(Box::new(move |report: Report| {
        if report.failures.is_empty() {
            return;
        }
        let lane = at.lock().unwrap();
        let state = &lane.as_ref().expect("the volume is known").state;
        into.lock()
            .unwrap()
            .push(state.sent.load(Ordering::SeqCst) - state.returned.load(Ordering::SeqCst));
    }));
    (writers, lane, seen)
}

// A settle does not end ahead of the report of a failure in what it waited
// for: when the failure of a batch's last operation is reported, the volume
// has not yet said that operation returned.
#[test]
fn a_batchs_failures_are_reported_before_a_settle_of_it_can_end() {
    let dir = tempfile::tempdir().unwrap();
    let (writers, lane, seen) = reporting_what_is_outstanding(VolumeWriters::new);
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    *lane.lock().unwrap() = Some(Arc::clone(&job.lane));
    job.write(
        1,
        0,
        Bytes::from_static(b"kept"),
        create(&dir.path().join("a")),
    );
    job.write(1, 4, Bytes::from_static(b"lost"), None);
    hold.wait_held();
    hold.release(1);
    hold.wait_held();
    hold.fail_next(io::Error::other("no room"));
    hold.open();

    writers.settle();

    // The report was made, and found the failed write still outstanding.
    assert_eq!(*seen.lock().unwrap(), [1]);
    assert!(job.failed());
}

// The same on a volume with no thread, where the submit itself reports.
#[test]
fn a_failure_on_a_volume_with_no_thread_is_reported_before_a_settle_can_end() {
    let dir = tempfile::tempdir().unwrap();
    let (writers, lane, seen) = reporting_what_is_outstanding(VolumeWriters::threadless);
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    *lane.lock().unwrap() = Some(Arc::clone(&job.lane));

    job.write(
        1,
        0,
        Bytes::from_static(b"lost"),
        create(&dir.path().join("a")),
    );

    writers.settle();
    assert_eq!(*seen.lock().unwrap(), [1]);
    assert!(job.failed());
}

// The volume's own work is left as a job's disk leaves it: through a
// submitter of the volume's own that is let go at once, which queues a key
// to forget behind the work. A volume a job holds and has not written to
// keeps the work: neither that forgetting nor a handle closed is work that
// starts it. It runs once the job's write is down and nothing is queued. A
// settle does not wait for it. A volume no caller holds runs it at once.
#[test]
fn the_volumes_own_work_runs_only_when_nothing_is_queued() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let (volume, other) = (volume("one"), volume("two"));
    // Watches the volume's thread and holds nothing of it back.
    let watch = writers.hold(&volume);
    watch.open();
    let job = writers.submitter(&volume, 1);
    let (began, begun) = mpsc::channel();
    let (go, gone) = mpsc::channel::<()>();
    let (ended, end) = mpsc::channel();
    let read = path.clone();
    let own = move || {
        // What the job's file holds as the work begins.
        began.send(std::fs::read(&read).ok()).unwrap();
        // Stands until the test lets it go: a write behind it would too.
        let _ = gone.recv();
        ended.send(()).unwrap();
    };

    // Left with a volume that is held and has written nothing. The thread
    // has taken the work and the forgetting, and stands: not run.
    writers.volume_submitter(&volume).when_idle(own);
    watch.wait_parked();
    assert!(
        begun.try_recv().is_err(),
        "not ahead of the job's first write"
    );
    // The job's write comes down first. Out of work, the writer runs it.
    job.write(1, 0, Bytes::from_static(b"first"), create(&path));
    assert_eq!(begun.recv().unwrap().as_deref(), Some(&b"first"[..]));
    // A settle does not wait for it.
    writers.settle();
    assert!(end.try_recv().is_err());
    go.send(()).unwrap();
    end.recv().unwrap();

    // A handle closed is no work either: work left after the job's write
    // waits for the next one.
    let (again, ran_again) = mpsc::channel();
    writers
        .volume_submitter(&volume)
        .when_idle(move || again.send(()).unwrap());
    job.release(1);
    watch.wait_parked();
    assert!(ran_again.try_recv().is_err(), "a handle closed is not work");
    job.write(1, 5, Bytes::from_static(b"later"), create(&path));
    ran_again.recv().unwrap();
    job.barrier().wait();
    assert_eq!(std::fs::read(&path).unwrap(), b"firstlater");

    // On a volume no caller holds it runs at once.
    let (ran, run) = mpsc::channel();
    writers
        .volume_submitter(&other)
        .when_idle(move || ran.send(()).unwrap());
    run.recv().unwrap();
}

// The volume's own work is dropped unrun once the owner is stopping: left
// while the job's write is held before the disk, it would run as that write
// came down, and the stop rung meanwhile drops it instead.
#[test]
fn the_volumes_own_work_is_dropped_at_a_stop() {
    let dir = tempfile::tempdir().unwrap();
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(
        1,
        0,
        Bytes::from_static(b"kept"),
        create(&dir.path().join("a")),
    );
    // The thread stands before the write: nothing after it is looked at
    // until the test lets it through.
    hold.wait_held();
    let (ran, run) = mpsc::channel::<()>();
    writers
        .volume_submitter(&volume)
        .when_idle(move || ran.send(()).unwrap());
    writers.stop_waits();
    hold.open();
    job.barrier().wait();
    assert_eq!(std::fs::read(dir.path().join("a")).unwrap(), b"kept");
    drop(job);
    drop(hold);
    writers.wait_ended(&volume);
    assert!(run.recv().is_err(), "dropped, not run");
}

// A write's bytes, call and time reach the volume's counters as the call
// returns, while the rest of its batch is still to run: the live rate is
// read from them, and a batch on a slow disk can take seconds.
#[test]
fn a_write_is_counted_as_its_call_returns_not_at_the_end_of_its_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    job.write(1, 0, Bytes::from_static(b"lan"), create(&path));
    job.write(1, 3, Bytes::from_static(b"tern"), None);
    // The held operation, then the first write of the batch behind it.
    hold.release(2);
    hold.wait_held();
    assert_eq!(count(&volume.written_bytes), 3);
    assert_eq!(count(&volume.write_calls), 1);
    assert_eq!(count(&volume.writer.batches), 1, "the batch has not ended");
    hold.open();
    job.barrier().wait();
    assert_eq!(count(&volume.written_bytes), 7);
    assert_eq!(count(&volume.write_calls), 2);
    assert_eq!(
        count(&volume.write_seeks),
        0,
        "the second write follows the first"
    );
    drop((job, hold));
    writers.wait_ended(&volume);
}

// A batch takes queued writes up to `BATCH_BYTES`; what is queued behind
// them runs in the next batch.
#[test]
fn a_batch_stops_taking_writes_at_its_byte_cap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    let big = BATCH_BYTES as usize;
    job.write(1, 0, Bytes::from(vec![7u8; big]), create(&path));
    job.write(1, big as u64, Bytes::from_static(b"end"), None);
    hold.open();
    job.barrier().wait();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), big as u64 + 3);
    // A batch is counted after its operations return: wait for the thread
    // to stand with nothing queued.
    hold.wait_parked();
    // The held run alone, the write that reached the cap, the one after.
    assert_eq!(count(&volume.writer.batches), 3);
    drop((job, hold));
    writers.wait_ended(&volume);
}

// The loop's writes count against the volume's hard limit before they are
// queued, in pieces of at most its share (100 of 1600 here). With the volume
// full the next write stands still for the writer, queues nothing
// meanwhile, and goes once the writer has written one piece of what it
// holds; the count never passes the limit.
#[test]
fn a_loop_write_on_a_full_volume_waits_for_room_and_never_counts_past_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    volume.set_buffer_limits(1600, 1600);
    assert_eq!(volume.loop_share(), 100);
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from(vec![1u8; 1000]), create(&path));
    hold.wait_held();
    job.write(1, 1000, Bytes::from(vec![2u8; 600]), None);
    assert_eq!(volume.unwritten_bytes(), 1600);
    std::thread::scope(|scope| {
        let next = scope.spawn(|| job.write(1, 1600, Bytes::from(vec![3u8; 40]), None));
        hold.wait_below_waiters(1);
        assert_eq!(volume.unwritten_bytes(), 1600, "nothing more was queued");
        assert_eq!(count(&volume.buffered_bytes), 1600);
        // The first piece written leaves room for the third write.
        hold.release(1);
        next.join().unwrap();
    });
    assert_eq!(volume.unwritten_bytes(), 1540);
    hold.open();
    writers.settle();
    assert_eq!(volume.unwritten_bytes(), 0);
    assert_eq!(volume.peak_bytes(), 1600);
    let written = std::fs::read(&path).unwrap();
    assert_eq!(written.len(), 1640);
    assert!(written[..1000].iter().all(|byte| *byte == 1));
    assert!(written[1000..1600].iter().all(|byte| *byte == 2));
    assert!(written[1600..].iter().all(|byte| *byte == 3));
}

// What the loop holds as credit, the arrived article it hands on, is
// already counted: a write within it adds nothing to the count. Past it a
// write longer than the loop's share goes in pieces of that share, each
// counted as it fits.
#[test]
fn a_loop_write_takes_its_credit_first_and_goes_in_pieces_past_its_share() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    volume.set_buffer_limits(64, 64);
    assert_eq!(volume.loop_share(), 4);
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    volume.charge(10);
    volume.loop_credit.store(10, Ordering::Relaxed);
    job.write(1, 0, Bytes::from(vec![1u8; 10]), create(&path));
    assert_eq!(volume.unwritten_bytes(), 10, "the credit paid for it");
    assert_eq!(count(&volume.loop_credit), 0);
    hold.wait_held();
    // Two of credit, then eighteen past it: pieces of 6, 4, 4, 4 and 2.
    volume.charge(2);
    volume.loop_credit.store(2, Ordering::Relaxed);
    job.write(1, 10, Bytes::from(vec![2u8; 20]), None);
    assert_eq!(volume.unwritten_bytes(), 30);
    assert_eq!(count(&volume.writer.operations), 0);
    hold.open();
    // A settle passes once the last piece returned, which can be before
    // the batch adds its operations to the count; parked, it has.
    hold.wait_parked();
    assert_eq!(
        count(&volume.writer.operations),
        6,
        "one write and five pieces"
    );
    assert_eq!(volume.unwritten_bytes(), 0);
    let written = std::fs::read(&path).unwrap();
    assert_eq!(written.len(), 30);
    assert!(written[10..].iter().all(|byte| *byte == 2));
}

// A loop write whose wait for room is given up at the end of a stop queues
// nothing more, and what of it the loop's credit paid for no longer counts:
// once the writer has written what it holds, the volume counts nothing.
#[test]
fn a_loop_write_given_up_at_the_stops_end_leaves_nothing_counted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    volume.set_buffer_limits(64, 64);
    assert_eq!(volume.loop_share(), 4);
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from(vec![1u8; 60]), create(&path));
    assert_eq!(volume.unwritten_bytes(), 60);
    hold.wait_held();
    let clock = Arc::new(HandClock::default());
    let end = Duration::from_secs(30);
    writers.end_loop_waits_at(Arc::clone(&clock) as Arc<dyn Clock + Send>, end);
    writers.stop_waits();
    clock.move_to(end);
    // An arrived article of two, then eight past it: the first piece is
    // two of credit and four charged, and four more do not fit.
    volume.charge(2);
    volume.loop_credit.store(2, Ordering::Relaxed);
    job.write(1, 60, Bytes::from(vec![2u8; 10]), None);
    assert!(job.left());
    assert_eq!(
        volume.unwritten_bytes(),
        60,
        "the credit's two are given back"
    );
    hold.open();
    writers.settle();
    assert_eq!(volume.unwritten_bytes(), 0);
    assert_eq!(std::fs::read(&path).unwrap().len(), 60);
}

// Opens a new file sized to its declared length, as a file system that makes
// holes is given it, and says so.
fn open_sized(path: &Path, length: Option<u64>) -> io::Result<Opened> {
    let file = open_file(path)?;
    let length = length.unwrap_or(0);
    file.set_len(length)?;
    Ok(Opened {
        file,
        length: Some(length),
        steps: OpenSteps::default(),
    })
}

// Opens a new file that grows as it is written, and says it is empty.
fn open_grown(path: &Path, _length: Option<u64>) -> io::Result<Opened> {
    Ok(Opened {
        file: open_file(path)?,
        length: Some(0),
        steps: OpenSteps::default(),
    })
}

fn made_by(path: &Path, length: u64, open: OpenFn) -> Create {
    Create {
        path: path.to_path_buf(),
        length: Some(length),
        open,
    }
}

// A close sets a length only where the file is not at it already, and every
// file ends at its close's length: sized and written to its end, sized and
// written short of it, grown to it, grown short of it, and opened with
// nothing said of its length.
#[test]
fn a_close_sets_the_length_only_where_the_file_is_not_at_it() {
    let dir = tempfile::tempdir().unwrap();
    let (writers, _reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    // File, opener, declared length, bytes written from the start, close.
    let files: [(&str, OpenFn, u64, &'static [u8], u64); 5] = [
        ("sized-whole", open_sized, 8, b"lanterns", 8),
        ("sized-short", open_sized, 10, b"lantern", 7),
        ("grown-whole", open_grown, 6, b"wicker", 6),
        ("grown-short", open_grown, 6, b"wick", 6),
        ("unsaid", open, 6, b"embers", 6),
    ];
    for (at, &(name, opener, declared, bytes, length)) in files.iter().enumerate() {
        let path = dir.path().join(name);
        let file = at as u64 + 1;
        job.write(
            file,
            0,
            Bytes::from_static(bytes),
            Some(made_by(&path, declared, opener)),
        );
        job.close(file, length, made_by(&path, length, opener));
        job.release(file);
    }
    let barrier = job.barrier();
    barrier.wait();
    assert!(barrier.passed());
    for (name, _, _, bytes, length) in files {
        let read = std::fs::read(dir.path().join(name)).unwrap();
        let mut expected = bytes.to_vec();
        expected.resize(length as usize, 0);
        assert_eq!(read, expected, "{name}");
    }
    let writer = &volume.writer;
    assert_eq!(
        count(&writer.length_sets_skipped),
        2,
        "the sized file written whole and the grown one"
    );
    assert_eq!(
        count(&writer.length_sets),
        3,
        "the short sized file, the one grown short and the one unsaid"
    );
    assert_eq!(count(&writer.opens), 5);
    assert_eq!(count(&writer.closes), 5);
}

// The opener the graph gives the writer, on whatever file system holds the
// test, told to size every new file: a file written to its declared end is
// not set again at its close, and one written short is cut to its length.
#[test]
fn a_file_sized_when_made_is_set_at_close_only_when_short() {
    let _sized = crate::pipeline::graph::sparse::sizing::always();
    let (skipped, sets) = close_through_the_graphs_opener();
    assert_eq!((skipped, sets), (1, 1));
}

// The same opener told to size no file: each grows as it is written, and the
// one whose close is its last byte is not set again.
#[test]
fn a_file_grown_by_its_writes_is_set_at_close_only_when_short() {
    let _grown = crate::pipeline::graph::sparse::sizing::never();
    let (skipped, sets) = close_through_the_graphs_opener();
    assert_eq!((skipped, sets), (1, 1));
}

fn graphs_opener(path: &Path, length: Option<u64>) -> io::Result<Opened> {
    use crate::pipeline::graph::sparse::{PlatformMarker, create_sized_known};
    create_sized_known(path, &PlatformMarker, length)
}

// Two files declared at eight bytes: one written whole and closed at eight,
// one written to five and closed there. Closes that set nothing, and length
// sets made.
fn close_through_the_graphs_opener() -> (u64, u64) {
    let dir = tempfile::tempdir().unwrap();
    let (whole, short) = (dir.path().join("whole"), dir.path().join("short"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    job.write(
        1,
        0,
        Bytes::from_static(b"lanterns"),
        Some(made_by(&whole, 8, graphs_opener)),
    );
    job.close(1, 8, made_by(&whole, 8, graphs_opener));
    job.write(
        2,
        0,
        Bytes::from_static(b"embers"),
        Some(made_by(&short, 8, graphs_opener)),
    );
    job.close(2, 5, made_by(&short, 5, graphs_opener));
    let barrier = job.barrier();
    barrier.wait();
    assert!(barrier.passed());
    assert_eq!(std::fs::read(&whole).unwrap(), b"lanterns");
    assert_eq!(std::fs::read(&short).unwrap(), b"ember");
    (
        count(&volume.writer.length_sets_skipped),
        count(&volume.writer.length_sets),
    )
}

// A truncate is a length set, and the file it leaves is at nothing: a close
// at nothing after it sets no length again.
#[test]
fn a_truncated_file_is_known_to_be_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"lantern"), create(&path));
    job.truncate(1, made_by(&path, 0, open));
    job.close(1, 0, made_by(&path, 0, open));
    let barrier = job.barrier();
    barrier.wait();
    assert!(barrier.passed());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    assert_eq!(count(&volume.writer.length_sets), 1);
    assert_eq!(count(&volume.writer.length_sets_skipped), 1);
}

// The writer's opens, length sets and handle drops are counted with the time
// inside them, on the stepped clock: each open here takes a second, each
// other read of the clock a millisecond.
#[test]
fn opens_length_sets_and_drops_are_timed() {
    let dir = tempfile::tempdir().unwrap();
    let (first, second) = (dir.path().join("first"), dir.path().join("second"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let job = writers.submitter(&volume, 1);
    job.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    job.write(
        1,
        0,
        Bytes::from_static(b"lan"),
        Some(made_by(&first, 3, open_slowly)),
    );
    job.close(1, 7, made_by(&first, 7, open_slowly));
    job.write(
        2,
        0,
        Bytes::from_static(b"wick"),
        Some(made_by(&second, 4, open_slowly)),
    );
    job.close(2, 4, made_by(&second, 4, open_slowly));
    job.release(1);
    job.release(2);
    let barrier = job.barrier();
    hold.open();
    barrier.wait();
    assert!(barrier.passed());
    let writer = &volume.writer;
    let second_ns = Duration::from_secs(1).as_nanos() as u64;
    let milli_ns = Duration::from_millis(1).as_nanos() as u64;
    assert_eq!(count(&writer.opens), 2);
    assert!(count(&writer.open_nanos) >= 2 * second_ns);
    // The test's opener says nothing of the length: both closes set it.
    assert_eq!(count(&writer.length_sets), 2);
    assert_eq!(count(&writer.length_set_nanos), 2 * milli_ns);
    assert_eq!(count(&writer.length_sets_skipped), 0);
    // Both handles go in one drop, once the batch runs nothing more of the
    // key.
    assert_eq!(count(&writer.closes), 2);
    assert_eq!(count(&writer.close_nanos), milli_ns);
    assert_eq!(std::fs::read(&first).unwrap(), b"lan\0\0\0\0");
}

// A reader of a closed file is told its bytes and length are placed before
// another key's file in the same batch is opened, even when that file's
// write came to the writer first.
#[test]
fn a_closed_file_is_placed_before_another_files_open() {
    let dir = tempfile::tempdir().unwrap();
    let (closed_path, other_path) = (dir.path().join("closed"), dir.path().join("other"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let closing = writers.submitter(&volume, 1);
    let other = writers.submitter(&volume, 2);
    // The thread stands in a batch of its own before what follows comes.
    other.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    other.write(1, 0, Bytes::from_static(b"more"), create(&other_path));
    closing.write(1, 0, Bytes::from_static(b"head"), create(&closed_path));
    closing.close(
        1,
        4,
        Create {
            path: closed_path.clone(),
            length: None,
            open,
        },
    );
    hold.release(1);
    hold.wait_held();
    // Two operations of the batch run: the closed file's write and close.
    hold.release(2);
    hold.wait_held();
    let closed = closing.barrier();
    assert!(!closed.unwritten(), "the closed file is placed");
    assert!(!other_path.exists(), "the other file is not opened yet");
    assert_eq!(std::fs::read(&closed_path).unwrap(), b"head");
    hold.open();
    other.barrier().wait();
    assert_eq!(std::fs::read(&other_path).unwrap(), b"more");
}

// A write to a file already open is placed before another key's file in the
// same batch is made, even when the make came to the writer first.
#[test]
fn an_open_files_write_is_placed_before_another_files_make() {
    let dir = tempfile::tempdir().unwrap();
    let (open_path, made_path) = (dir.path().join("open"), dir.path().join("made"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let hold = writers.hold(&volume);
    let writing = writers.submitter(&volume, 1);
    let making = writers.submitter(&volume, 2);
    // The file is opened in a batch of its own.
    writing.write(1, 0, Bytes::from_static(b"head"), create(&open_path));
    hold.wait_held();
    hold.release(1);
    // The thread stands in a batch of its own before what follows comes.
    making.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    making.write(1, 0, Bytes::from_static(b"more"), create(&made_path));
    writing.write(1, 4, Bytes::from_static(b"tail"), None);
    hold.release(1);
    hold.wait_held();
    // One operation of the batch runs: the open file's write.
    hold.release(1);
    hold.wait_held();
    let written = writing.barrier();
    assert!(!written.unwritten(), "the open file's write is placed");
    assert!(!made_path.exists(), "the other file is not made yet");
    hold.open();
    making.barrier().wait();
    written.wait();
    assert_eq!(std::fs::read(&made_path).unwrap(), b"more");
    assert_eq!(std::fs::read(&open_path).unwrap(), b"headtail");
}

// A file is closed when its download ends and again when its tree closes,
// then written and closed again by a repair: its handle is kept through all
// of it, and the file is opened once. Only a release lets the handle go.
#[test]
fn a_closed_file_written_and_closed_again_is_opened_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    let (writers, _reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"head"), create(&path));
    job.close(1, 8, made_by(&path, 8, open));
    job.close(1, 8, made_by(&path, 8, open));
    job.write(1, 4, Bytes::from_static(b"tail"), create(&path));
    job.close(1, 8, made_by(&path, 8, open));
    job.truncate(1, made_by(&path, 0, open));
    job.write(1, 0, Bytes::from_static(b"lanterns"), create(&path));
    job.close(1, 8, made_by(&path, 8, open));
    job.barrier().wait();
    assert!(job.barrier().passed());
    assert_eq!(std::fs::read(&path).unwrap(), b"lanterns");
    assert_eq!(count(&volume.writer.opens), 1);
    assert_eq!(count(&volume.writer.closes), 0);
    job.release(1);
    job.barrier().wait();
    assert_eq!(count(&volume.writer.closes), 1);
    assert!(job.barrier().key.held().open.is_empty());
}

// A caller's code that moves or removes a closed file runs with the file's
// handle already dropped, and the next write opens the file again.
#[test]
fn a_move_and_a_removal_drop_a_closed_files_handle_before_they_run() {
    let dir = tempfile::tempdir().unwrap();
    let (from, to) = (dir.path().join("from"), dir.path().join("to"));
    let (writers, _reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    job.write(1, 0, Bytes::from_static(b"head"), create(&from));
    job.close(1, 4, made_by(&from, 4, open));
    let (seen, moved_from, moved_to) = (Arc::clone(&volume), from.clone(), to.clone());
    let dropped = Arc::new(AtomicU64::new(u64::MAX));
    let told = Arc::clone(&dropped);
    job.run(Some(1), from.clone(), move || {
        told.store(count(&seen.writer.closes), Ordering::SeqCst);
        std::fs::rename(&moved_from, &moved_to)
    });
    job.barrier().wait();
    assert!(job.barrier().passed());
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "the move ran on a held handle"
    );
    job.write(1, 4, Bytes::from_static(b"tail"), create(&to));
    job.close(1, 8, made_by(&to, 8, open));
    let (seen, removed) = (Arc::clone(&volume), to.clone());
    let told = Arc::clone(&dropped);
    job.run(Some(1), to.clone(), move || {
        told.store(count(&seen.writer.closes), Ordering::SeqCst);
        std::fs::remove_file(&removed)
    });
    job.barrier().wait();
    assert!(job.barrier().passed());
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        2,
        "the removal ran on a held handle"
    );
    assert!(!to.exists());
    assert_eq!(count(&volume.writer.opens), 2);
}

// A key's end lets go of every handle it kept.
#[test]
fn a_keys_end_drops_its_closed_files_handles() {
    let dir = tempfile::tempdir().unwrap();
    let (writers, _reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    for file in 1..=3u64 {
        let path = dir.path().join(file.to_string());
        job.write(file, 0, Bytes::from_static(b"head"), create(&path));
        job.close(file, 4, made_by(&path, 4, open));
    }
    job.barrier().wait();
    assert_eq!(count(&volume.writer.closes), 0);
    drop(job);
    writers.wait_ended(&volume);
    assert_eq!(count(&volume.writer.closes), 3);
}

// A key keeps the handles of at most `KEPT_CLOSED` closed files: past it the
// one closed longest ago is let go, and only that file is opened again by
// its next write.
#[test]
fn a_key_keeps_a_bounded_number_of_closed_files_handles() {
    let dir = tempfile::tempdir().unwrap();
    let (writers, _reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    let kept = super::KEPT_CLOSED as u64;
    let path = |file: u64| dir.path().join(file.to_string());
    for file in 1..=kept + 1 {
        job.write(file, 0, Bytes::from_static(b"head"), create(&path(file)));
        job.close(file, 4, made_by(&path(file), 4, open));
    }
    job.barrier().wait();
    assert!(job.barrier().passed());
    assert_eq!(count(&volume.writer.opens), kept + 1);
    assert_eq!(count(&volume.writer.closes), 1);
    assert_eq!(job.barrier().key.held().open.len() as u64, kept);
    assert!(!job.barrier().key.held().open.contains_key(&1));
    job.write(
        kept + 1,
        4,
        Bytes::from_static(b"tail"),
        create(&path(kept + 1)),
    );
    job.barrier().wait();
    assert_eq!(
        count(&volume.writer.opens),
        kept + 1,
        "the newest stayed open"
    );
    job.write(1, 4, Bytes::from_static(b"tail"), create(&path(1)));
    job.barrier().wait();
    assert_eq!(
        count(&volume.writer.opens),
        kept + 2,
        "the oldest was let go"
    );
    assert_eq!(std::fs::read(path(1)).unwrap(), b"headtail");
}

// A kept closed file written again is in use, not closed: it leaves the
// closed files, so the bound passes it by and lets go of the next oldest,
// and the file is not opened again.
#[test]
fn a_closed_file_written_again_leaves_the_closed_files() {
    let dir = tempfile::tempdir().unwrap();
    let (writers, _reports) = writers();
    let volume = volume("one");
    let job = writers.submitter(&volume, 1);
    let kept = super::KEPT_CLOSED as u64;
    let path = |file: u64| dir.path().join(file.to_string());
    let close = |file: u64| {
        job.write(file, 0, Bytes::from_static(b"head"), create(&path(file)));
        job.close(file, 4, made_by(&path(file), 4, open));
    };
    // The closed files are full, and the oldest is written again.
    for file in 1..=kept {
        close(file);
    }
    job.barrier().wait();
    // The thread stands in a batch of its own before what follows comes,
    // so the write and the two closes after it are one batch, which runs
    // the closes first.
    let hold = writers.hold(&volume);
    job.run(None, dir.path().to_path_buf(), || Ok(()));
    hold.wait_held();
    job.write(1, 4, Bytes::from_static(b"tail"), create(&path(1)));
    // Two more closed: the first fills the place the written file left,
    // the second passes the bound.
    close(kept + 1);
    close(kept + 2);
    hold.open();
    job.barrier().wait();
    assert!(job.barrier().passed());
    assert_eq!(
        count(&volume.writer.opens),
        kept + 2,
        "the written file stayed open"
    );
    assert_eq!(count(&volume.writer.closes), 1);
    let barrier = job.barrier();
    let held = barrier.key.held();
    assert!(held.open.contains_key(&1), "the file in use was let go");
    assert!(!held.open.contains_key(&2), "the next oldest stayed");
    assert_eq!(held.closed.len() as u64, kept);
    assert!(!held.closed.contains(&1));
    drop(held);
    // Closed again, it goes in at the newest end.
    job.close(1, 8, made_by(&path(1), 8, open));
    job.barrier().wait();
    assert_eq!(count(&volume.writer.closes), 2);
    let barrier = job.barrier();
    let held = barrier.key.held();
    assert_eq!(held.closed.back(), Some(&1));
    assert!(!held.open.contains_key(&3), "the oldest closed went");
    drop(held);
    assert_eq!(std::fs::read(path(1)).unwrap(), b"headtail");
}
