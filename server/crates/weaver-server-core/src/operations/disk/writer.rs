// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One writer thread per volume. A caller hands it file operations and goes
// on: it never waits for the disk. The thread takes everything queued for
// its volume, writes one file at a time in offset order, and says once per
// batch what came back, with the error of anything that failed.
//
// A caller writes under a key of its own choosing. Everything it submitted
// under the key before a given moment is behind a `Barrier`, which passes
// once every one of those operations has returned. The thread starts with
// the first caller of its volume and ends when the last one lets go.
//
// Nothing here syncs: an operation has returned when its system call has.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, Weak, mpsc};
use std::time::Instant;

use bytes::Bytes;

use super::volume::Volume;

// How a file that is not open is opened, made when it is not there, at the
// length it is declared to reach when one is given.
pub(crate) type OpenFn = fn(&Path, Option<u64>) -> io::Result<File>;

// A file to open before an operation that needs it open.
pub(crate) struct Create {
    pub(crate) path: PathBuf,
    pub(crate) length: Option<u64>,
    pub(crate) open: OpenFn,
}

// What one batch came to: every operation of it has returned.
pub(crate) struct Report {
    pub(crate) failures: Vec<Failure>,
}

// An operation that failed, under the key and for the file it was given.
#[derive(Debug)]
pub(crate) struct Failure {
    pub(crate) key: u64,
    pub(crate) file: Option<u64>,
    pub(crate) path: PathBuf,
    pub(crate) error: io::Error,
    // The file could not be opened or made, so nothing of it was written.
    pub(crate) creating: bool,
}

type ReportFn = dyn Fn(Report) + Send + Sync;
type Then = Box<dyn FnOnce() + Send>;

// The writer threads, one per volume in use.
pub(crate) struct VolumeWriters {
    lanes: Mutex<Vec<(usize, Weak<Lane>)>>,
    report: Arc<ReportFn>,
    // No threads: each operation runs as it is submitted, on the thread
    // that submits it.
    #[cfg(test)]
    inline: bool,
}

impl VolumeWriters {
    // `report` is called on a writer thread once per batch.
    pub(crate) fn new(report: impl Fn(Report) + Send + Sync + 'static) -> Self {
        Self {
            lanes: Mutex::default(),
            report: Arc::new(report),
            #[cfg(test)]
            inline: false,
        }
    }

    // Writers with no threads, for a test that reads the disk right after
    // it writes: each operation runs as it is submitted, on the thread
    // that submits it, through everything else a volume's thread does.
    #[cfg(test)]
    pub(crate) fn inline(report: impl Fn(Report) + Send + Sync + 'static) -> Self {
        Self {
            inline: true,
            ..Self::new(report)
        }
    }

    // A caller's way to write to `volume` under `key`. The volume's thread
    // starts here when no caller holds it.
    pub(crate) fn submitter(&self, volume: &Arc<Volume>, key: u64) -> Arc<Submitter> {
        Arc::new(Submitter {
            lane: self.lane(volume),
            key: Arc::new(Key {
                id: key,
                failed: AtomicBool::new(false),
                abandoned: AtomicBool::new(false),
                unheard: AtomicU32::new(0),
            }),
            last: AtomicU64::new(0),
        })
    }

    fn lane(&self, volume: &Arc<Volume>) -> Arc<Lane> {
        let at = Arc::as_ptr(volume) as usize;
        let mut lanes = self.lanes.lock().unwrap_or_else(PoisonError::into_inner);
        lanes.retain(|(_, lane)| lane.strong_count() > 0);
        lanes
            .iter()
            .find(|(volume, _)| *volume == at)
            .and_then(|(_, lane)| lane.upgrade())
            .unwrap_or_else(|| {
                #[cfg(test)]
                let inline = self.inline;
                #[cfg(not(test))]
                let inline = false;
                let lane = Lane::start(Arc::clone(volume), Arc::clone(&self.report), inline);
                lanes.push((at, Arc::downgrade(&lane)));
                lane
            })
    }

    // Wait until everything submitted to every volume so far has returned.
    pub(crate) fn settle(&self) {
        let lanes: Vec<Arc<Lane>> = self
            .lanes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(|(_, lane)| lane.upgrade())
            .collect();
        for lane in lanes {
            let seq = lane.send.lock().unwrap_or_else(PoisonError::into_inner).0;
            lane.state.wait(seq);
        }
    }

    // Hold `volume`'s thread before each operation until the test lets it
    // through.
    #[cfg(test)]
    pub(crate) fn hold(&self, volume: &Arc<Volume>) -> Hold {
        let lane = self.lane(volume);
        let hold = Hold(Arc::clone(&lane.state), lane);
        let mut gate = hold.0.gate.lock().unwrap_or_else(PoisonError::into_inner);
        gate.held = true;
        drop(gate);
        hold
    }
}

struct Key {
    id: u64,
    // An operation under the key failed: the ones after it are not run.
    failed: AtomicBool,
    // The caller gave the key up: the ones still queued are not run.
    abandoned: AtomicBool,
    // Refusals the caller has yet to hear and answer; no barrier of the key
    // passes while there are any.
    unheard: AtomicU32,
}

// One caller's key on one volume.
pub(crate) struct Submitter {
    lane: Arc<Lane>,
    key: Arc<Key>,
    // The last operation submitted under the key.
    last: AtomicU64,
}

impl Submitter {
    pub(crate) fn volume(&self) -> &Arc<Volume> {
        &self.lane.state.volume
    }

    // Write `bytes` at `offset` of `file`, opening it through `create` when
    // it is not open. The bytes are handed over, not copied. A write to a
    // file that is neither open nor given a way to open is dropped: the
    // open that failed has said so.
    pub(crate) fn write(&self, file: u64, offset: u64, bytes: Bytes, create: Option<Create>) -> u64 {
        self.lane
            .state
            .volume
            .buffered_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.submit(Op::Write {
            file,
            offset,
            bytes,
            create: create.map(Box::new),
        })
    }

    // Cut `file` to nothing, when it is open or `create.path` is there.
    pub(crate) fn truncate(&self, file: u64, create: Create) -> u64 {
        self.submit(Op::Truncate { file, create })
    }

    // `file` takes its final `length` and its handle is closed.
    pub(crate) fn close(&self, file: u64, length: u64, create: Create) -> u64 {
        self.submit(Op::Close {
            file,
            length,
            create,
        })
    }

    // Close the handle of `file`, changing nothing of it.
    pub(crate) fn release(&self, file: u64) -> u64 {
        self.submit(Op::Release { file })
    }

    // Run `run` on the writer thread after everything submitted before it
    // and before everything after: a rename, a link, a removal or a
    // directory made. The handle of `file`, when one is named, is closed
    // first. `path` names the failure when `run` fails.
    pub(crate) fn run(
        &self,
        file: Option<u64>,
        path: PathBuf,
        run: impl FnOnce() -> io::Result<()> + Send + 'static,
    ) -> u64 {
        self.submit(Op::Run {
            file,
            path,
            run: Box::new(run),
        })
    }

    // Move `from` to `to`, where nothing may be, once every earlier
    // operation has returned.
    pub(crate) fn rename(&self, file: Option<u64>, from: PathBuf, to: PathBuf) -> u64 {
        let path = from.clone();
        self.run(file, path, move || {
            crate::runtime::fs::rename_path_exclusive(&from, &to)
        })
    }

    // Give `from` the second name `to`, once every earlier operation has
    // returned.
    pub(crate) fn hard_link(&self, from: PathBuf, to: PathBuf) -> u64 {
        let path = from.clone();
        self.run(None, path, move || std::fs::hard_link(&from, &to))
    }

    // Read `len` bytes at `offset` of the file at `path`, after everything
    // submitted before. The caller waits for the answer.
    pub(crate) fn read(&self, path: PathBuf, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.submit(Op::Read {
            path,
            offset,
            len,
            reply,
        });
        answer
            .recv()
            .unwrap_or_else(|_| Err(io::Error::other("the volume's writer is gone")))
    }

    // Everything submitted under the key so far.
    pub(crate) fn barrier(&self) -> Barrier {
        self.barrier_at(self.last.load(Ordering::Acquire))
    }

    // Everything submitted to the volume through the operation `seq`, which
    // a submission returned.
    pub(crate) fn barrier_at(&self, seq: u64) -> Barrier {
        Barrier {
            lane: Arc::clone(&self.lane.state),
            key: Arc::clone(&self.key),
            seq,
        }
    }

    // Whether an operation under the key failed.
    #[cfg(test)]
    pub(crate) fn failed(&self) -> bool {
        self.key.failed.load(Ordering::Acquire)
    }

    // The caller has heard a refusal out and answered it.
    pub(crate) fn heard_refusal(&self) {
        self.key.unheard.fetch_sub(1, Ordering::AcqRel);
    }

    // The caller is done with the key: what is still queued under it is not
    // run, and every handle it left open is closed. The barrier taken after
    // this passes once nothing of the key can touch the disk again.
    pub(crate) fn abandon(&self) -> Barrier {
        self.key.abandoned.store(true, Ordering::Release);
        self.submit(Op::Forget);
        self.barrier()
    }

    fn submit(&self, op: Op) -> u64 {
        let mut send = self
            .lane
            .send
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        send.0 += 1;
        let seq = send.0;
        self.last.store(seq, Ordering::Release);
        let sent = send.1.send(Queued {
            seq,
            key: Arc::clone(&self.key),
            op,
        });
        drop(send);
        if let Err(mpsc::SendError(unsent)) = sent {
            // The thread is gone, so nothing will be written: the bytes
            // stop waiting and the barrier does not hold anyone.
            if let Op::Write { bytes, .. } = &unsent.op {
                self.lane
                    .state
                    .volume
                    .buffered_bytes
                    .fetch_sub(bytes.len() as u64, Ordering::Relaxed);
            }
            self.key.failed.store(true, Ordering::Release);
            self.lane.state.returned(seq);
        }
        if let Some(inline) = &self.lane.inline {
            let mut inline = inline.lock().unwrap_or_else(PoisonError::into_inner);
            let (thread, queued) = &mut *inline;
            while let Ok(first) = queued.try_recv() {
                thread.batch(first, queued);
            }
        }
        seq
    }
}

impl Drop for Submitter {
    fn drop(&mut self) {
        // What is queued under the key still runs; the handles it left
        // open close behind it.
        if !self.key.abandoned.load(Ordering::Acquire) {
            self.submit(Op::Forget);
        }
    }
}

// Everything submitted before a moment: passed once all of it has returned.
#[derive(Clone)]
pub(crate) struct Barrier {
    lane: Arc<LaneState>,
    key: Arc<Key>,
    seq: u64,
}

impl Barrier {
    // Whether everything behind the barrier returned, none of the key's
    // operations failed or was given up, and no refusal waits to be heard.
    pub(crate) fn passed(&self) -> bool {
        self.lane.returned.load(Ordering::SeqCst) >= self.seq
            && !self.key.failed.load(Ordering::Acquire)
            && !self.key.abandoned.load(Ordering::Acquire)
            && self.key.unheard.load(Ordering::Acquire) == 0
    }

    // Whether everything behind the barrier returned, failed or not.
    pub(crate) fn returned(&self) -> bool {
        self.lane.returned.load(Ordering::SeqCst) >= self.seq
    }

    // Wait until everything behind the barrier has returned, failed or not.
    // For a thread that may wait on the disk.
    pub(crate) fn wait(&self) {
        self.lane.wait(self.seq);
    }

    // Whether a test holds the volume's thread.
    #[cfg(test)]
    pub(crate) fn test_held(&self) -> bool {
        self.lane
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .held
    }

    // Run `run` once everything behind the barrier has returned, failed or
    // not: here and now when it already has, and otherwise on the volume's
    // thread, which it must not hold up.
    pub(crate) fn then(&self, run: impl FnOnce() + Send + 'static) {
        self.lane.then(self.seq, Box::new(run));
    }
}

impl std::fmt::Debug for Barrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Barrier").field("seq", &self.seq).finish()
    }
}

impl std::fmt::Debug for Submitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Submitter")
            .field("key", &self.key.id)
            .finish()
    }
}

struct Lane {
    state: Arc<LaneState>,
    // The number of the last operation sent, and the way to the thread.
    // Numbering and sending are one step, so the thread sees every number
    // in order.
    send: Mutex<(u64, mpsc::Sender<Queued>)>,
    // A lane with no thread: what a thread would hold, run by whoever
    // submits.
    inline: Option<Mutex<(Thread, mpsc::Receiver<Queued>)>>,
}

struct LaneState {
    volume: Arc<Volume>,
    // Every operation through this number has returned.
    returned: AtomicU64,
    waiters: AtomicUsize,
    waiting: Mutex<()>,
    woke: Condvar,
    // What to run once the operations through a number have returned, and
    // how many of them there are.
    asked: AtomicUsize,
    then: Mutex<Vec<(u64, Then)>>,
    #[cfg(test)]
    gate: Mutex<Gate>,
    #[cfg(test)]
    gate_moved: Condvar,
}

impl LaneState {
    fn returned(&self, seq: u64) {
        self.returned.fetch_max(seq, Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) > 0 {
            let _held = self.waiting.lock().unwrap_or_else(PoisonError::into_inner);
            self.woke.notify_all();
        }
        if self.asked.load(Ordering::SeqCst) > 0 {
            self.run_due();
        }
    }

    // Run everything asked for whose operations have returned, outside the
    // lock.
    fn run_due(&self) {
        let returned = self.returned.load(Ordering::SeqCst);
        let mut due = Vec::new();
        {
            let mut then = self.then.lock().unwrap_or_else(PoisonError::into_inner);
            let mut at = 0;
            while at < then.len() {
                if then[at].0 <= returned {
                    due.push(then.swap_remove(at).1);
                } else {
                    at += 1;
                }
            }
            self.asked.store(then.len(), Ordering::SeqCst);
        }
        for run in due {
            run();
        }
    }

    fn then(&self, seq: u64, run: Then) {
        if self.returned.load(Ordering::SeqCst) >= seq {
            run();
            return;
        }
        {
            let mut then = self.then.lock().unwrap_or_else(PoisonError::into_inner);
            then.push((seq, run));
            self.asked.store(then.len(), Ordering::SeqCst);
        }
        // The operations may have returned between the look and the ask.
        if self.returned.load(Ordering::SeqCst) >= seq {
            self.run_due();
        }
    }

    fn wait(&self, seq: u64) {
        if self.returned.load(Ordering::SeqCst) >= seq {
            return;
        }
        let mut held = self.waiting.lock().unwrap_or_else(PoisonError::into_inner);
        self.waiters.fetch_add(1, Ordering::SeqCst);
        while self.returned.load(Ordering::SeqCst) < seq {
            held = self.woke.wait(held).unwrap_or_else(PoisonError::into_inner);
        }
        self.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Lane {
    fn start(volume: Arc<Volume>, report: Arc<ReportFn>, inline: bool) -> Arc<Self> {
        let (send, queued) = mpsc::channel();
        let state = Arc::new(LaneState {
            volume,
            returned: AtomicU64::new(0),
            waiters: AtomicUsize::new(0),
            waiting: Mutex::new(()),
            woke: Condvar::new(),
            asked: AtomicUsize::new(0),
            then: Mutex::default(),
            #[cfg(test)]
            gate: Mutex::default(),
            #[cfg(test)]
            gate_moved: Condvar::new(),
        });
        if inline {
            return Arc::new(Self {
                state: Arc::clone(&state),
                send: Mutex::new((0, send)),
                inline: Some(Mutex::new((Thread::new(state, report), queued))),
            });
        }
        let lane = Arc::new(Self {
            state: Arc::clone(&state),
            send: Mutex::new((0, send)),
            inline: None,
        });
        let name = format!("volume-writer {}", state.volume.label);
        let thread_state = Arc::clone(&state);
        let spawned = std::thread::Builder::new()
            .name(name)
            .spawn(move || Thread::new(thread_state, report).run(queued));
        if let Err(error) = spawned {
            // With no thread the receiver is gone, and every submission
            // fails its key at once instead of waiting forever.
            tracing::error!(%error, "could not start a volume's writer thread");
        }
        lane
    }
}

struct Queued {
    seq: u64,
    key: Arc<Key>,
    op: Op,
}

enum Op {
    Write {
        file: u64,
        offset: u64,
        bytes: Bytes,
        create: Option<Box<Create>>,
    },
    Truncate {
        file: u64,
        create: Create,
    },
    Close {
        file: u64,
        length: u64,
        create: Create,
    },
    Release {
        file: u64,
    },
    Run {
        file: Option<u64>,
        path: PathBuf,
        run: Box<dyn FnOnce() -> io::Result<()> + Send>,
    },
    Read {
        path: PathBuf,
        offset: u64,
        len: usize,
        reply: mpsc::SyncSender<io::Result<Vec<u8>>>,
    },
    // Close every handle the key left open.
    Forget,
}

impl Op {
    fn file(&self) -> Option<u64> {
        match self {
            Op::Write { file, .. }
            | Op::Truncate { file, .. }
            | Op::Close { file, .. }
            | Op::Release { file } => Some(*file),
            Op::Run { .. } | Op::Read { .. } | Op::Forget => None,
        }
    }
}

struct Open {
    file: File,
    // Where it was opened, to name a failure.
    path: PathBuf,
    key: u64,
}

// Where an operation of a batch runs: batches part at every operation that
// may touch more than its own file, a file's operations stay together, and
// within a file its writes run by offset between the operations that must
// follow or precede them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Place {
    part: u32,
    file: u32,
    run: u32,
    offset: u64,
    at: u32,
}

struct Thread {
    state: Arc<LaneState>,
    report: Arc<ReportFn>,
    open: HashMap<u64, Open>,
    // A file whose open was refused: its later writes are dropped.
    refused: std::collections::HashSet<u64>,
    batch: Vec<Option<Queued>>,
    order: Vec<Place>,
    done: Vec<bool>,
    files: HashMap<u64, (u32, u32, u32)>,
}

impl Thread {
    fn new(state: Arc<LaneState>, report: Arc<ReportFn>) -> Self {
        Self {
            state,
            report,
            open: HashMap::new(),
            refused: Default::default(),
            batch: Vec::new(),
            order: Vec::new(),
            done: Vec::new(),
            files: HashMap::new(),
        }
    }

    fn run(mut self, queued: mpsc::Receiver<Queued>) {
        while let Ok(first) = queued.recv() {
            self.batch(first, &queued);
        }
    }

    // Run `first` and everything queued behind it as one batch.
    fn batch(&mut self, first: Queued, queued: &mpsc::Receiver<Queued>) {
        {
            let base = first.seq;
            self.batch.push(Some(first));
            while let Ok(more) = queued.try_recv() {
                self.batch.push(Some(more));
            }
            self.arrange();
            let mut failures = Vec::new();
            let (mut written, mut nanos) = (0u64, 0u64);
            self.done.clear();
            self.done.resize(self.batch.len(), false);
            let mut returned = 0;
            for index in 0..self.order.len() {
                let at = self.order[index].at as usize;
                let queued = self.batch[at].take().expect("each operation runs once");
                #[cfg(test)]
                let forced = self.pass_gate();
                #[cfg(not(test))]
                let forced = None;
                self.perform(queued, forced, &mut failures, &mut written, &mut nanos);
                self.done[at] = true;
                let before = returned;
                while returned < self.done.len() && self.done[returned] {
                    returned += 1;
                }
                if returned > before {
                    self.state.returned(base + returned as u64 - 1);
                }
            }
            self.batch.clear();
            let volume = &self.state.volume;
            volume.written_bytes.fetch_add(written, Ordering::Relaxed);
            volume.write_nanos.fetch_add(nanos, Ordering::Relaxed);
            (self.report)(Report { failures });
        }
    }

    // Order the batch: see `Place`. Overlapping writes of one file between
    // two of its other operations keep the order they were submitted in.
    fn arrange(&mut self) {
        self.order.clear();
        self.files.clear();
        let mut part = 0u32;
        let mut rank = 0u32;
        for (at, queued) in self.batch.iter().enumerate() {
            let queued = queued.as_ref().expect("a batch not yet run");
            let at = at as u32;
            let Some(file) = queued.op.file() else {
                part += 1;
                self.order.push(Place {
                    part,
                    file: 0,
                    run: 0,
                    offset: 0,
                    at,
                });
                part += 1;
                continue;
            };
            let entry = self.files.entry(file).or_insert((u32::MAX, 0, 0));
            if entry.0 != part {
                rank += 1;
                *entry = (part, rank, 0);
            }
            // A write that opens the file runs before the writes after it,
            // which carry no way to open it.
            let alone = !matches!(&queued.op, Op::Write { create: None, .. });
            let offset = match &queued.op {
                Op::Write { offset, .. } => *offset,
                _ => 0,
            };
            if alone {
                entry.2 += 1;
            }
            self.order.push(Place {
                part,
                file: entry.1,
                run: entry.2,
                offset,
                at,
            });
            if alone {
                entry.2 += 1;
            }
        }
        if self.order.len() < 2 {
            return;
        }
        self.order.sort_unstable();
        let mut start = 0;
        while start < self.order.len() {
            let head = self.order[start];
            let mut end = start + 1;
            while end < self.order.len()
                && (self.order[end].part, self.order[end].file, self.order[end].run)
                    == (head.part, head.file, head.run)
            {
                end += 1;
            }
            let mut reached = 0u64;
            let mut overlaps = false;
            for place in &self.order[start..end] {
                if let Some(Queued {
                    op: Op::Write { offset, bytes, .. },
                    ..
                }) = &self.batch[place.at as usize]
                {
                    overlaps |= *offset < reached;
                    reached = reached.max(offset + bytes.len() as u64);
                }
            }
            if overlaps {
                self.order[start..end].sort_unstable_by_key(|place| place.at);
            }
            start = end;
        }
    }

    fn perform(
        &mut self,
        queued: Queued,
        forced: Option<io::Error>,
        failures: &mut Vec<Failure>,
        written: &mut u64,
        nanos: &mut u64,
    ) {
        let Queued { key, op, .. } = queued;
        let waiting = match &op {
            Op::Write { bytes, .. } => bytes.len() as u64,
            _ => 0,
        };
        let skip = key.abandoned.load(Ordering::Acquire) || key.failed.load(Ordering::Acquire);
        let file = op.file();
        let mut creating = false;
        let mut failed_at = PathBuf::new();
        let result = match op {
            Op::Forget => {
                self.open.retain(|_, open| open.key != key.id);
                Ok(())
            }
            Op::Release { file } => {
                self.open.remove(&file);
                Ok(())
            }
            Op::Read {
                path,
                offset,
                len,
                reply,
            } => {
                let read = if skip {
                    Err(io::Error::other("the file's writes were given up"))
                } else {
                    read_at(&path, offset, len)
                };
                let _ = reply.send(read);
                Ok(())
            }
            _ if skip => {
                if let Some(file) = file {
                    self.open.remove(&file);
                }
                Ok(())
            }
            _ if forced.is_some() => Err(forced.expect("checked above")),
            Op::Write {
                file,
                offset,
                bytes,
                create,
            } => {
                if let Some(create) = &create {
                    failed_at.clone_from(&create.path);
                }
                match self.opened(file, key.id, create.as_deref(), &mut creating) {
                    Ok(Some(open)) => {
                        let started = Instant::now();
                        let wrote = write_all_at(&open.file, offset, &bytes);
                        if wrote.is_err() {
                            failed_at.clone_from(&open.path);
                        }
                        *nanos += started.elapsed().as_nanos() as u64;
                        if wrote.is_ok() {
                            *written += bytes.len() as u64;
                        }
                        wrote
                    }
                    Ok(None) => Ok(()),
                    Err(error) => Err(error),
                }
            }
            Op::Truncate { file, create } => {
                failed_at.clone_from(&create.path);
                if !self.open.contains_key(&file) && !create.path.try_exists().unwrap_or(true) {
                    Ok(())
                } else {
                    self.opened(file, key.id, Some(&create), &mut creating)
                        .and_then(|open| open.map_or(Ok(()), |open| open.file.set_len(0)))
                }
            }
            Op::Close {
                file,
                length,
                create,
            } => {
                failed_at.clone_from(&create.path);
                let closed = self
                    .opened(file, key.id, Some(&create), &mut creating)
                    .and_then(|open| open.map_or(Ok(()), |open| open.file.set_len(length)));
                self.open.remove(&file);
                closed
            }
            Op::Run { file, path, run } => {
                if let Some(file) = file {
                    self.open.remove(&file);
                }
                failed_at = path;
                run()
            }
        };
        if waiting > 0 {
            self.state
                .volume
                .buffered_bytes
                .fetch_sub(waiting, Ordering::Relaxed);
        }
        if let Err(error) = result {
            // A name the file system will not take is a refusal of that one
            // file: the caller decides what follows. Anything else fails
            // the key, and what is queued behind it under the key is not
            // run.
            let refusal = creating && error.kind() == io::ErrorKind::InvalidFilename;
            if refusal {
                if let Some(file) = file {
                    self.refused.insert(file);
                }
                key.unheard.fetch_add(1, Ordering::AcqRel);
            } else {
                key.failed.store(true, Ordering::Release);
            }
            failures.push(Failure {
                key: key.id,
                file,
                path: failed_at,
                error,
                creating,
            });
        }
    }

    // The open handle of `file`, opened through `create` when it is not
    // open. `None` for a file with no handle and no way to open one.
    fn opened(
        &mut self,
        file: u64,
        key: u64,
        create: Option<&Create>,
        creating: &mut bool,
    ) -> io::Result<Option<&Open>> {
        if !self.open.contains_key(&file) {
            let Some(create) = create else {
                return Ok(None);
            };
            if self.refused.contains(&file) {
                return Ok(None);
            }
            *creating = true;
            let opened = (create.open)(&create.path, create.length)?;
            *creating = false;
            self.open.insert(
                file,
                Open {
                    file: opened,
                    path: create.path.clone(),
                    key,
                },
            );
        }
        Ok(self.open.get(&file))
    }

    #[cfg(test)]
    fn pass_gate(&self) -> Option<io::Error> {
        let mut gate = self
            .state
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while gate.held && gate.permits == 0 {
            gate.waiting = true;
            self.state.gate_moved.notify_all();
            gate = self
                .state
                .gate_moved
                .wait(gate)
                .unwrap_or_else(PoisonError::into_inner);
        }
        gate.waiting = false;
        if gate.held {
            gate.permits -= 1;
        }
        gate.fail_next.take()
    }
}

fn read_at(path: &Path, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let mut bytes = vec![0; len];
    let mut filled = 0;
    while filled < len {
        #[cfg(unix)]
        let read = std::os::unix::fs::FileExt::read_at(
            &file,
            &mut bytes[filled..],
            offset + filled as u64,
        )?;
        #[cfg(windows)]
        let read = std::os::windows::fs::FileExt::seek_read(
            &file,
            &mut bytes[filled..],
            offset + filled as u64,
        )?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        filled += read;
    }
    Ok(bytes)
}

fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
    }
    #[cfg(windows)]
    {
        let mut written = 0;
        while written < bytes.len() {
            let wrote = std::os::windows::fs::FileExt::seek_write(
                file,
                &bytes[written..],
                offset + written as u64,
            )?;
            if wrote == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            written += wrote;
        }
        Ok(())
    }
}

#[cfg(test)]
#[derive(Default)]
struct Gate {
    held: bool,
    permits: usize,
    // The thread stands before an operation with no permit for it.
    waiting: bool,
    fail_next: Option<io::Error>,
}

// A test's hold on a volume's thread.
#[cfg(test)]
pub(crate) struct Hold(Arc<LaneState>, #[allow(dead_code)] Arc<Lane>);

#[cfg(test)]
impl Hold {
    fn gate(&self) -> std::sync::MutexGuard<'_, Gate> {
        self.0.gate.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Let the next `count` operations through.
    pub(crate) fn release(&self, count: usize) {
        self.gate().permits += count;
        self.0.gate_moved.notify_all();
    }

    // Let everything through from now on.
    pub(crate) fn open(&self) {
        self.gate().held = false;
        self.0.gate_moved.notify_all();
    }

    // The next operation let through fails with `error` instead of running.
    pub(crate) fn fail_next(&self, error: io::Error) {
        self.gate().fail_next = Some(error);
    }

    // Wait until the thread stands before an operation it may not run yet.
    pub(crate) fn wait_held(&self) {
        let mut gate = self.gate();
        while !(gate.waiting && gate.permits == 0) {
            gate = self
                .0
                .gate_moved
                .wait(gate)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

#[cfg(test)]
mod tests;
