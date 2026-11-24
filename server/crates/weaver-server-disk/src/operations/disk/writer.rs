// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One writer thread per volume. A caller hands it file operations and goes
// on: it never waits for the disk. The thread takes everything queued for
// its volume and writes one file at a time in offset order. It speaks only
// when there is something to hear: an operation failed, or the bytes waiting
// for it fell to a line someone asked to be woken at.
//
// A caller writes under a key of its own choosing, and its file numbers are
// its own. Everything it submitted under the key before a given moment is
// behind a `Barrier`, which passes once every one of those operations has
// returned, whatever other keys still have queued. The thread starts with
// the first caller of its volume and ends when the last one has let go and
// nothing is queued; until it has ended, every caller of the volume gets
// that same thread.
//
// Each caller is an owner (`DiskOwner`): it hears of its keys' failures
// and of the wakes it asked for. An owner's queuing thread is the one
// thread that queues its writes and must not stand still for a disk
// longer than the volume's limit makes it; its waits are counted apart
// from those of any other thread.
//
// Nothing here syncs: an operation has returned when its system call has.
// On a share over the network, on Linux, the writer only starts the
// write-out of each file every `WRITE_OUT_STRIDE` bytes written to it, so
// the OS cannot pile up minutes of unwritten pages for a close to stand
// behind (`write_out_due`): flow, not durability.

use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, Weak, mpsc};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::clock::{Clock, SystemClock};

use super::volume::{LoopSite, LoopWait, OWNERS, Reservation, Volume, WriterCounts};
use super::volume_rate::RateSignal;

mod credit;
pub use credit::{Credit, Paying, Stand};

// How a file that is not open is opened, made when it is not there, at the
// length it is declared to reach when one is given, and under the folder it
// must not leave when one is given (`Create::within`).
pub(crate) type OpenFn = fn(&Path, Option<u64>, Option<&Path>) -> io::Result<Opened>;

// A file an `OpenFn` opened, and the length it left the file at when the
// opener knows it without another call to the file system; `None` when it
// does not. The writer keeps that length as it writes, and a close at the
// length the file already has sets nothing.
pub struct Opened {
    pub file: File,
    pub length: Option<u64>,
    pub steps: OpenSteps,
}

// The time an opener spent in each step of an open, for the steps it ran
// and timed; `None` for a step it did not run. The create includes the
// folders made for the file; the size is the look at the file before its
// length is set at creation, and the set.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenSteps {
    pub create: Option<Duration>,
    pub(crate) mark: Option<Duration>,
    pub(crate) size: Option<Duration>,
}

impl OpenSteps {
    fn nanos(step: Option<Duration>) -> Option<u64> {
        step.map(|took| took.as_nanos() as u64)
    }
}

// An open that took longer than this is logged with its steps.
const SLOW_OPEN: Duration = Duration::from_millis(100);

// A file to open before an operation that needs it open.
pub struct Create {
    pub path: PathBuf,
    pub length: Option<u64>,
    pub open: OpenFn,
    // The folder `path` lies under and must resolve inside, links and all,
    // for an opener that holds it there (`super::beneath`); None for one
    // whose path is its own.
    pub within: Option<Arc<Path>>,
}

// Whoever writes through the volumes' writers: a caller obtains its
// `Submitter` with its owner, and hears through it what its writes have to
// say. Called on a writer thread, or on the submitting thread for a volume
// that has none; neither call may wait.
pub trait DiskOwner: Send + Sync {
    // One of this owner's operations failed. A batch's failures are told one
    // after another, and `room` follows them once.
    fn failed(&self, failure: Failure);
    // The volume may have room this owner waits for: its unwritten bytes
    // fell to the line the owner asked about (`Submitter::wake_below`), or
    // operations of the owner failed, which frees what they held.
    fn room(&self);
}

// An owner as the writers know it: the owner itself, its slot among the
// owners of one set of writers, and the one bit its asks for a wake are
// marked with on a volume. Made once for each owner
// (`VolumeWriters::owner`); cloning it is cheap.
#[derive(Clone)]
pub struct Owner {
    owner: Arc<dyn DiskOwner>,
    slot: usize,
    bit: u64,
}

impl Owner {
    // Move `bytes` of `from` ahead of the writes the owner is about to
    // queue to its volume: those writes take them first, and are counted
    // only past them (`Submitter::write`). The bytes stay counted. The
    // owner prepays and writes from one thread, and settles each
    // prepayment (`Prepaid::settle`) before any it made earlier on the
    // volume: a credit's (`Credit::prepay`) may be made and settled inside
    // this one's step, as a step's writes of held bytes are.
    pub fn prepay(&self, from: &mut Reservation, bytes: u64) -> Prepaid {
        from.hand_over(bytes);
        let volume = Arc::clone(from.volume());
        let before = prepay_on(&volume, self.slot, bytes);
        Prepaid {
            volume,
            slot: self.slot,
            bytes,
            before,
        }
    }
}

// The one owner of an owner's prepaid slot on a volume, for `Owner::prepay`
// and `Credit::prepay` alike. Prepayments on one slot nest: each adds its
// bytes on top of what is ahead already, and is settled before any made
// earlier, and the writes take the latest first. Returns what was ahead
// before this one, which its settle is judged against.
pub(super) fn prepay_on(volume: &Volume, slot: usize, bytes: u64) -> u64 {
    let slot = &volume.prepaid[slot];
    let before = slot.load(Ordering::Relaxed);
    slot.store(before + bytes, Ordering::Relaxed);
    before
}

// The prepayment of `bytes` made on top of `before` is settled: what of it
// the writes did not take leaves the slot, and is returned, still counted
// for the caller to give back or keep. What was ahead before it stays.
pub(super) fn settle_on(volume: &Volume, slot: usize, bytes: u64, before: u64) -> u64 {
    let slot = &volume.prepaid[slot];
    let now = slot.load(Ordering::Relaxed);
    // The writes took this prepayment first.
    let taken = (before + bytes).saturating_sub(now).min(bytes);
    let left = bytes - taken;
    slot.store(now - left, Ordering::Relaxed);
    left
}

// What an owner prepaid on a volume (`Owner::prepay`), until it settles.
#[must_use = "what was prepaid stays counted until it is settled"]
pub struct Prepaid {
    volume: Arc<Volume>,
    slot: usize,
    bytes: u64,
    before: u64,
}

impl Prepaid {
    pub fn volume(&self) -> &Arc<Volume> {
        &self.volume
    }

    // The writes are queued: what of the prepaid bytes they did not take
    // no longer counts. The bytes given back; the caller takes the wake
    // they may have reached (`VolumeWriters::take_wake`). A prepayment
    // made and settled inside this one's step keeps its own bytes apart.
    pub fn settle(&self) -> u64 {
        let left = settle_on(&self.volume, self.slot, self.bytes, self.before);
        self.volume.release(left);
        left
    }
}

// The owners known to a set of writers, each in the slot of its bit. Set
// once each; read without a lock.
struct Owners {
    known: [OnceLock<Arc<dyn DiskOwner>>; OWNERS],
    next: AtomicUsize,
}

impl Default for Owners {
    fn default() -> Self {
        Self {
            known: std::array::from_fn(|_| OnceLock::new()),
            next: AtomicUsize::new(0),
        }
    }
}

impl Owners {
    // An owner is a download service (the pipeline, the torrents, and a
    // third that downloads over HTTP), made known once for the process:
    // never one per job, torrent or session, which key their writes
    // instead. A wake asked is one bit of a word, so there are at most
    // `OWNERS`.
    fn add(&self, owner: Arc<dyn DiskOwner>) -> Owner {
        let at = self.next.fetch_add(1, Ordering::Relaxed);
        assert!(
            at < OWNERS,
            "at most {OWNERS} owners write through one set of writers"
        );
        let _ = self.known[at].set(Arc::clone(&owner));
        Owner {
            owner,
            slot: at,
            bit: 1 << at,
        }
    }

    // Tell every owner whose bit is in `asked` that the volume has room.
    // Nothing when no wake was taken.
    #[inline]
    fn room(&self, asked: u64) {
        if asked != 0 {
            self.wake(asked);
        }
    }

    #[cold]
    fn wake(&self, asked: u64) {
        break_span();
        let mut left = asked;
        while left != 0 {
            let at = left.trailing_zeros() as usize;
            left &= left - 1;
            if let Some(owner) = self.known[at].get() {
                owner.room();
            }
        }
    }
}

// What a volume's writer had to say in a test, as one report: the failures
// of a batch, or that the bytes waiting fell to a line asked about.
#[cfg(any(test, feature = "test-support"))]
pub struct Report {
    pub failures: Vec<Failure>,
    #[cfg(test)]
    pub(crate) below: bool,
}

// An owner for a test that hears its writers as reports: the failures told
// are gathered and sent with the `room` that follows them.
#[cfg(any(test, feature = "test-support"))]
struct Reporting {
    failures: Mutex<Vec<Failure>>,
    report: Box<dyn Fn(Report) + Send + Sync>,
}

#[cfg(any(test, feature = "test-support"))]
impl DiskOwner for Reporting {
    fn failed(&self, failure: Failure) {
        self.failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(failure);
    }

    fn room(&self) {
        let failures =
            std::mem::take(&mut *self.failures.lock().unwrap_or_else(PoisonError::into_inner));
        (self.report)(Report {
            #[cfg(test)]
            below: failures.is_empty(),
            failures,
        });
    }
}

// An operation that failed, under the key and for the file it was given.
#[derive(Debug)]
pub struct Failure {
    pub key: u64,
    pub file: Option<u64>,
    pub path: PathBuf,
    pub error: io::Error,
    // The file could not be opened or made, so nothing of it was written.
    pub creating: bool,
}

// What a bounded settle left on a volume it gave up on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsettled {
    pub label: String,
    pub operations: u64,
    pub bytes: u64,
}

// The clock a bounded settle goes by and the span it has, chosen by
// whoever will ask for the settle: the machine's clock outside tests, and
// in a test one it moves by hand.
#[derive(Clone)]
pub struct SettleBound {
    pub clock: Arc<dyn Clock>,
    pub span: Duration,
}

impl SettleBound {
    // What the clock reads when the span, begun now, is over. A span too
    // long to add to the reading saturates, as a `Duration` would: it ends
    // at the latest moment the clock holds within it, centuries away, so
    // the settle never gives a volume up.
    pub fn end(&self) -> Instant {
        let now = self.clock.now();
        let mut span = self.span;
        loop {
            if let Some(end) = now.checked_add(span) {
                return end;
            }
            // Halved until it fits; a zero span always does.
            span /= 2;
        }
    }

    // `span` by the machine's own clock.
    pub fn wall(span: Duration) -> Self {
        Self {
            clock: Arc::new(SystemClock),
            span,
        }
    }
}

// Where the queuing thread's own waits for a volume end once the owner is
// stopping: the clock and the reading at which the stop gives a volume up,
// the same one its settles go by. Until it is set the queuing thread waits as long
// as the volume takes.
#[derive(Default)]
struct WaitEnd {
    set: AtomicBool,
    at: Mutex<Option<(Arc<dyn Clock>, Instant)>>,
}

impl WaitEnd {
    // How long the queuing thread may still wait and whether the clock runs by
    // itself, once the end is set. One load before then.
    fn more(&self) -> Option<(Duration, bool)> {
        if !self.set.load(Ordering::SeqCst) {
            return None;
        }
        let at = self.at.lock().unwrap_or_else(PoisonError::into_inner);
        let (clock, end) = at.as_ref()?;
        Some((end.saturating_duration_since(clock.now()), clock.runs()))
    }

    // Wait on `woke` while `over` says so and the end has not passed.
    // Whether `over` came to say no: false when the wait was given up at
    // the end. Only the queuing thread waits here and only the queuing thread sets the end, so
    // setting it wakes nobody.
    fn wait<'a>(
        &self,
        woke: &Condvar,
        mut held: MutexGuard<'a, ()>,
        over: impl Fn() -> bool,
    ) -> (MutexGuard<'a, ()>, bool) {
        loop {
            if !over() {
                return (held, true);
            }
            held = match self.more() {
                Some((more, _)) if more.is_zero() => return (held, false),
                Some((more, true)) => {
                    woke.wait_timeout(held, more)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
                None | Some((_, false)) => woke.wait(held).unwrap_or_else(PoisonError::into_inner),
            };
        }
    }
}

type Then = Box<dyn FnOnce() + Send>;
type Lanes = Vec<(usize, Arc<Lane>)>;
// Rung after a batch that wrote, so an idle rate sampler looks again; set
// once, by whoever samples the rates.
type Rates = OnceLock<Arc<RateSignal>>;

/// The writer threads, one per volume in use. Built once for the process
/// and shared by everything that writes downloads to disk.
pub struct VolumeWriters {
    registry: Arc<Registry>,
    #[cfg(any(test, feature = "test-support"))]
    mode: Mode,
    // The owner a test's writers report to (`VolumeWriters::reporting`).
    #[cfg(any(test, feature = "test-support"))]
    test_owner: Option<Owner>,
}

impl Default for VolumeWriters {
    fn default() -> Self {
        Self::new()
    }
}

// Every volume with a writer, by the address of its record. A volume stays
// here until its thread has ended, whether or not a caller still holds it.
// One whose thread could not be started stays until its last caller has let
// go: see `VolumeWriters::lane`.
#[derive(Default)]
struct Registry {
    lanes: Mutex<Lanes>,
    // Set once the owner is stopping: no thread but the queuing thread
    // stands still for a volume from then on. Every lane holds it.
    stopped: Arc<AtomicBool>,
    // Where the queuing thread's own waits end once the owner is stopping. Every
    // lane holds it.
    wait_end: Arc<WaitEnd>,
    // The owners writing through these writers. Every lane holds it.
    owners: Arc<Owners>,
    rates: Rates,
    #[cfg(any(test, feature = "test-support"))]
    ended: Condvar,
    // Times a thread was asked for.
    #[cfg(test)]
    starts: AtomicU64,
}

impl Registry {
    fn lock(&self) -> MutexGuard<'_, Lanes> {
        self.lanes.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// How a test's writers run.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Threads,
    // No threads: each operation runs as it is submitted, on the thread
    // that submits it.
    Inline,
    // No thread can be started.
    NoThread,
}

impl VolumeWriters {
    /// Writers with no volume in use yet: each volume's thread starts with
    /// its first caller.
    pub fn new() -> Self {
        Self {
            registry: Arc::default(),
            #[cfg(any(test, feature = "test-support"))]
            mode: Mode::Threads,
            #[cfg(any(test, feature = "test-support"))]
            test_owner: None,
        }
    }

    // Ring `rates` after every batch that wrote, from now on. Set once;
    // a second call changes nothing.
    pub fn ring(&self, rates: Arc<RateSignal>) {
        let _ = self.registry.rates.set(rates);
    }

    // Make `owner` known to the writers. Each owner is made known once and
    // keeps what this returns; at most `OWNERS` are.
    pub fn owner(&self, owner: Arc<dyn DiskOwner>) -> Owner {
        self.registry.owners.add(owner)
    }

    // Writers that report to `report`, for a test, with a thread per
    // volume. A test's submitters write as that owner
    // (`VolumeWriters::test_submitter`).
    #[cfg(any(test, feature = "test-support"))]
    pub fn reporting(report: impl Fn(Report) + Send + Sync + 'static) -> Self {
        let mut writers = Self::new();
        writers.test_owner = Some(writers.owner(Arc::new(Reporting {
            failures: Mutex::default(),
            report: Box::new(report),
        })));
        writers
    }

    // Writers on a machine that will not start another thread.
    #[cfg(any(test, feature = "test-support"))]
    pub fn threadless(report: impl Fn(Report) + Send + Sync + 'static) -> Self {
        Self {
            mode: Mode::NoThread,
            ..Self::reporting(report)
        }
    }

    // Writers with no threads, for a test that reads the disk right after
    // it writes: each operation runs as it is submitted, on the thread
    // that submits it, through everything else a volume's thread does.
    #[cfg(any(test, feature = "test-support"))]
    pub fn inline(report: impl Fn(Report) + Send + Sync + 'static) -> Self {
        Self {
            mode: Mode::Inline,
            ..Self::reporting(report)
        }
    }

    // The same writers, with no threads, for a test whose caller makes its
    // own owner.
    #[cfg(any(test, feature = "test-support"))]
    pub fn unthreaded() -> Self {
        Self {
            mode: Mode::Inline,
            ..Self::new()
        }
    }

    // The owner a test's writers report to.
    #[cfg(any(test, feature = "test-support"))]
    pub fn test_owner(&self) -> &Owner {
        self.test_owner
            .as_ref()
            .expect("writers made to report to a test")
    }

    // A test's way to write to `volume` under `key`, as its owner.
    #[cfg(any(test, feature = "test-support"))]
    pub fn test_submitter(&self, volume: &Arc<Volume>, key: u64) -> Arc<Submitter> {
        self.submitter(volume, key, self.test_owner())
    }

    // A test's way to queue a volume's own work, as its owner.
    #[cfg(test)]
    pub(crate) fn test_volume_submitter(&self, volume: &Arc<Volume>) -> Arc<Submitter> {
        self.volume_submitter(volume, self.test_owner())
    }

    // A caller's way to write to `volume` under `key`, as `owner`, which
    // hears what its operations have to say. The volume's thread starts
    // here when it has none; one that has not ended is used again.
    pub fn submitter(&self, volume: &Arc<Volume>, key: u64, owner: &Owner) -> Arc<Submitter> {
        debug_assert_ne!(key, VOLUME_KEY, "the volume's own key is no caller's");
        self.submitter_under(volume, key, owner)
    }

    // The way to queue a volume's own work on its writer, such as the probe
    // of its write rate, under the one key no caller has, for `owner`.
    pub fn volume_submitter(&self, volume: &Arc<Volume>, owner: &Owner) -> Arc<Submitter> {
        self.submitter_under(volume, VOLUME_KEY, owner)
    }

    fn submitter_under(&self, volume: &Arc<Volume>, key: u64, owner: &Owner) -> Arc<Submitter> {
        let lane = self.lane(volume);
        let send = Mutex::new(lane.send.clone());
        Arc::new(Submitter {
            lane,
            key: Arc::new(Key::new(key, owner.clone())),
            send,
        })
    }

    // The volume's lane, held for one more caller.
    fn lane(&self, volume: &Arc<Volume>) -> Arc<Lane> {
        let at = Arc::as_ptr(volume) as usize;
        // The thread decides to end under this lock, so it cannot go while
        // a caller is being counted in.
        let mut lanes = self.registry.lock();
        if let Some((_, lane)) = lanes.iter().find(|(volume, _)| *volume == at) {
            lane.state.users.fetch_add(1, Ordering::SeqCst);
            return Arc::clone(lane);
        }
        #[cfg(any(test, feature = "test-support"))]
        let (inline, threadless) = (self.mode == Mode::Inline, self.mode == Mode::NoThread);
        #[cfg(not(any(test, feature = "test-support")))]
        let (inline, threadless) = (false, false);
        #[cfg(test)]
        self.registry.starts.fetch_add(1, Ordering::Relaxed);
        let lane = Lane::start(Arc::clone(volume), &self.registry, inline, threadless);
        lane.state.users.fetch_add(1, Ordering::SeqCst);
        // A lane whose thread could not be started is kept as well, so the
        // callers that come while it is held share it and fail at once: no
        // thread is asked for again on their behalf. It leaves when the last
        // of them lets go, and only the caller after that asks for a thread
        // again: one ask each time the volume has been let go by everyone.
        lanes.push((at, Arc::clone(&lane)));
        lane
    }

    // Take the wake asked of `volume`, when its unwritten bytes have
    // fallen to the line asked (`Volume::take_wake`), for a caller that
    // lowered them itself: every other owner that asked hears `room`, and
    // whether `owner` asked comes back for the caller to act on itself.
    // One atomic step when no wake is due.
    pub fn take_wake(&self, volume: &Volume, owner: &Owner) -> bool {
        let asked = volume.take_wake();
        self.registry.owners.room(asked & !owner.bit);
        asked & owner.bit != 0
    }

    // Wait until everything submitted to every volume so far has returned,
    // whether or not its caller still holds the volume. A settle is the
    // wait of a stop, made off the queuing thread: it is not counted as
    // one of its waits.
    #[cfg(any(test, feature = "test-support"))]
    pub fn settle(&self) {
        self.settle_each(None);
    }

    // The same wait, given up on every volume that has not returned
    // everything once `clock` reads `end`, however much returned meanwhile.
    // Settles that share one `end` share one span between them: a later
    // one has what the earlier ones left of it. What each volume given up
    // on still held comes back; nothing does when everything returned.
    pub fn settle_until(&self, clock: &dyn Clock, end: Instant) -> Vec<Unsettled> {
        self.settle_each(Some((end, clock)))
    }

    fn settle_each(&self, bound: Option<(Instant, &dyn Clock)>) -> Vec<Unsettled> {
        let lanes: Vec<(Arc<Lane>, u64)> = self
            .registry
            .lock()
            .iter()
            .map(|(_, lane)| (Arc::clone(lane), lane.state.sent.load(Ordering::SeqCst)))
            .collect();
        let mut left = Vec::new();
        for (lane, sent) in lanes {
            let state = &lane.state;
            if state.returned.load(Ordering::SeqCst) >= sent {
                continue;
            }
            let mut held = state.waiting.lock().unwrap_or_else(PoisonError::into_inner);
            state.waiters.fetch_add(1, Ordering::SeqCst);
            // A test that waits for the settle to stand here is told.
            #[cfg(any(test, feature = "test-support"))]
            state.woke.notify_all();
            while state.returned.load(Ordering::SeqCst) < sent {
                let Some((end, clock)) = bound else {
                    held = state
                        .woke
                        .wait(held)
                        .unwrap_or_else(PoisonError::into_inner);
                    continue;
                };
                // How long the volume is still waited for.
                let more = end.saturating_duration_since(clock.now());
                if more.is_zero() {
                    let done = state.done.load(Ordering::SeqCst);
                    left.push(Unsettled {
                        label: state.volume.label.clone(),
                        operations: state.sent.load(Ordering::SeqCst).saturating_sub(done),
                        bytes: state.volume.buffered_bytes.load(Ordering::Relaxed),
                    });
                    break;
                }
                held = if clock.runs() {
                    state
                        .woke
                        .wait_timeout(held, more)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                } else {
                    state
                        .woke
                        .wait(held)
                        .unwrap_or_else(PoisonError::into_inner)
                };
            }
            state.waiters.fetch_sub(1, Ordering::SeqCst);
            drop(held);
        }
        left
    }

    // The owner has begun to stop, and gives a volume up once `clock` reads
    // `end`: the end its settles share. From now on the queuing thread's own
    // waits for a volume (`Barrier::wait`, `Submitter::wait_for`,
    // `Submitter::wait_room`) end there too, with what they waited
    // for still out; the key they waited on says so (`Submitter::gave_up`).
    // Called by the queuing thread, which stands in none of those waits then.
    pub fn end_waits_at(&self, clock: Arc<dyn Clock>, end: Instant) {
        let wait_end = &self.registry.wait_end;
        *wait_end.at.lock().unwrap_or_else(PoisonError::into_inner) = Some((clock, end));
        wait_end.set.store(true, Ordering::SeqCst);
    }

    // The owner is stopping. Every thread that stands still for a volume
    // without being the queuing thread goes on, and none stands still again:
    // a producer in `Submitter::wait_below`, and a reader in
    // `Barrier::wait_aside`, which finds its operations still out. What a
    // volume holds is still written when its thread gets to it.
    //
    // The queuing thread's own waits do not end here. The queuing thread still keeps
    // a volume's limit when it says a repair's output again
    // (`Submitter::wait_room`), and still waits for what it reads
    // back; those waits end at `end_waits_at`'s end.
    pub fn stop_waits(&self) {
        self.registry.stopped.store(true, Ordering::SeqCst);
        let lanes: Vec<Arc<Lane>> = self
            .registry
            .lock()
            .iter()
            .map(|(_, lane)| Arc::clone(lane))
            .collect();
        for lane in lanes {
            let state = &lane.state;
            {
                let _held = state
                    .volume
                    .below_waiting
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                state.volume.below.notify_all();
            }
            let keys = state
                .aside
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            for key in keys {
                let _held = key.waiting.lock().unwrap_or_else(PoisonError::into_inner);
                key.woke.notify_all();
            }
        }
    }

    // Have a bounded settle, and a wait of the queuing thread on a volume's limit,
    // look at its clock again, after a test moved it.
    #[cfg(any(test, feature = "test-support"))]
    pub fn look_again(&self) {
        let lanes: Vec<Arc<Lane>> = self
            .registry
            .lock()
            .iter()
            .map(|(_, lane)| Arc::clone(lane))
            .collect();
        for lane in lanes {
            let _held = lane
                .state
                .waiting
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            lane.state.woke.notify_all();
            let _held = lane
                .state
                .volume
                .below_waiting
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            lane.state.volume.below.notify_all();
        }
    }

    // Wait until `volume`'s thread has ended.
    #[cfg(any(test, feature = "test-support"))]
    pub fn wait_ended(&self, volume: &Arc<Volume>) {
        let at = Arc::as_ptr(volume) as usize;
        let mut lanes = self.registry.lock();
        while lanes.iter().any(|(volume, _)| *volume == at) {
            lanes = self
                .registry
                .ended
                .wait(lanes)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    // Hold `volume`'s thread before each operation until the test lets it
    // through. The hold keeps the thread as a caller does.
    #[cfg(any(test, feature = "test-support"))]
    pub fn hold(&self, volume: &Arc<Volume>) -> Hold {
        let hold = Hold(self.lane(volume));
        hold.gate().held = true;
        hold
    }
}

// The key a volume's own work is queued under: reserved here, and handed
// out only by `volume_submitter`. A caller's key is its own number. The
// callers today number their keys from ten thousand up, one at a time, and
// keep them in signed 64-bit columns, so none reaches this.
const VOLUME_KEY: u64 = u64::MAX;

// No entry of a batch: the end of a key's chain through it.
const NONE: u32 = u32::MAX;

// The writes one batch takes before the rest wait for the next: about
// half a second of the slowest disk measured (57 MB/s), so a caller's
// operation never waits behind more than that of other writes.
const BATCH_BYTES: u64 = 32 * 1024 * 1024;

struct Key {
    id: u64,
    // Who hears of the key's failures, and whose asks for a wake it makes.
    owner: Owner,
    // An operation under the key failed: the ones after it are not run.
    failed: AtomicBool,
    // The caller gave the key up: the ones still queued are not run.
    abandoned: AtomicBool,
    // A wait of the queuing thread on the key was given up at the end of a
    // stop, with what it waited for still out.
    gave_up: AtomicBool,
    // Bytes counted on the volume that the queuing thread carries for the
    // key's output it holds (`Credit`): what a producing thread's grant
    // under the key is judged beside (`Volume::grant`). Changed by the
    // queuing thread only.
    carried: AtomicU64,
    // Refusals the caller has yet to hear and answer; no barrier of the key
    // passes while there are any.
    unheard: AtomicU32,
    // The number of the last operation submitted under the key, and the
    // number through which every one of them has returned.
    submitted: AtomicU64,
    returned: AtomicU64,
    // The number through which every operation's bytes and length are on
    // disk: each write call, truncate, length set and caller's code has
    // returned. Only the drop of a handle the key let go may still be out.
    // A reader on a thread of its own opens its own handle and needs no
    // more; delivery, checkpoints and settles still go by `returned`.
    placed: AtomicU64,
    waiters: AtomicUsize,
    waiting: Mutex<()>,
    woke: Condvar,
    // What to run once the key's operations through a number have returned,
    // lowest number first, and that lowest number: `u64::MAX` for none.
    asked: AtomicU64,
    then: Mutex<BinaryHeap<Asked>>,
    // The key's files as its volume's thread holds them. Only that thread
    // takes this lock.
    held: Mutex<Held>,
    // The key's place in the batch being run, kept by the volume's thread
    // alone: the batch it was last seen in, and the first of its operations
    // there that has not returned and the last of them.
    turn: AtomicU64,
    head: AtomicU32,
    tail: AtomicU32,
    // The batch in which the key let a handle go that is not dropped yet,
    // and the entry of the batch that let the first go, kept by the
    // volume's thread alone. From that entry on the key is told its
    // operations returned only once its handles are dropped, which is done
    // as soon as the batch runs nothing more of the key.
    letting: AtomicU64,
    held_entry: AtomicU32,
}

struct Asked {
    seq: u64,
    run: Then,
}

impl PartialEq for Asked {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq
    }
}

impl Eq for Asked {}

impl PartialOrd for Asked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Asked {
    // The lowest number is the greatest, so the heap gives it first.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.seq.cmp(&self.seq)
    }
}

// A key's files on its volume, by the numbers the key gave them.
#[derive(Default)]
struct Held {
    open: HashMap<u64, Open>,
    // A file whose open was refused: its later writes are dropped.
    refused: HashSet<u64>,
    // Handles a release, a forget or the bound on closed files let go in the
    // batch being run, dropped once the batch runs nothing more of the key,
    // or before a caller's code of the key runs.
    letting_go: Vec<Open>,
    // Files a close ended whose handles are kept, oldest close first, at
    // most `KEPT_CLOSED`: a later write, close or truncate of the file uses
    // the handle instead of opening the file again, which on some systems
    // stands behind what the file system is writing out of it.
    closed: VecDeque<u64>,
}

// The handles a key keeps of files a close ended. Past it the oldest is let
// go: a file closed longest ago is the one least likely to be written again.
const KEPT_CLOSED: usize = 64;

impl Key {
    fn new(id: u64, owner: Owner) -> Self {
        Self {
            id,
            owner,
            failed: AtomicBool::new(false),
            abandoned: AtomicBool::new(false),
            gave_up: AtomicBool::new(false),
            carried: AtomicU64::new(0),
            unheard: AtomicU32::new(0),
            submitted: AtomicU64::new(0),
            returned: AtomicU64::new(0),
            placed: AtomicU64::new(0),
            waiters: AtomicUsize::new(0),
            waiting: Mutex::new(()),
            woke: Condvar::new(),
            asked: AtomicU64::new(u64::MAX),
            then: Mutex::default(),
            held: Mutex::default(),
            turn: AtomicU64::new(0),
            head: AtomicU32::new(NONE),
            tail: AtomicU32::new(NONE),
            letting: AtomicU64::new(0),
            held_entry: AtomicU32::new(NONE),
        }
    }

    fn held(&self) -> MutexGuard<'_, Held> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn has_returned(&self, seq: u64) -> bool {
        self.returned.load(Ordering::SeqCst) >= seq
    }

    // Whether the key's operations are no longer run.
    fn stopped(&self) -> bool {
        self.abandoned.load(Ordering::Acquire) || self.failed.load(Ordering::Acquire)
    }

    fn has_placed(&self, seq: u64) -> bool {
        self.placed.load(Ordering::SeqCst) >= seq
    }

    // Every operation of the key through `seq` has returned.
    fn returned(&self, seq: u64) {
        // Placed first: nothing that has returned is still out for a reader.
        self.placed.fetch_max(seq, Ordering::SeqCst);
        self.returned.fetch_max(seq, Ordering::SeqCst);
        self.wake();
        if self.asked.load(Ordering::SeqCst) <= seq {
            self.run_due();
        }
    }

    // The bytes and lengths of the key's operations through `seq` are on
    // disk; handles they let go may still be open (`placed`).
    fn placed(&self, seq: u64) {
        self.placed.fetch_max(seq, Ordering::SeqCst);
        self.wake();
    }

    fn wake(&self) {
        if self.waiters.load(Ordering::SeqCst) > 0 {
            break_span();
            let _held = self.waiting.lock().unwrap_or_else(PoisonError::into_inner);
            self.woke.notify_all();
        }
    }

    // Run everything asked for whose operations have returned, outside the
    // lock.
    fn run_due(&self) {
        let returned = self.returned.load(Ordering::SeqCst);
        let mut due = Vec::new();
        {
            let mut then = self.then.lock().unwrap_or_else(PoisonError::into_inner);
            while then.peek().is_some_and(|asked| asked.seq <= returned) {
                due.extend(then.pop().map(|asked| asked.run));
            }
            let next = then.peek().map_or(u64::MAX, |asked| asked.seq);
            self.asked.store(next, Ordering::SeqCst);
        }
        if !due.is_empty() {
            break_span();
        }
        for run in due {
            // One that panics does not cost the others their turn.
            let _ = catch_unwind(AssertUnwindSafe(run));
        }
    }

    fn then(&self, seq: u64, run: Then) {
        if self.has_returned(seq) {
            run();
            return;
        }
        {
            let mut then = self.then.lock().unwrap_or_else(PoisonError::into_inner);
            then.push(Asked { seq, run });
            let next = then.peek().map_or(u64::MAX, |asked| asked.seq);
            self.asked.store(next, Ordering::SeqCst);
        }
        // The operations may have returned between the look and the ask.
        if self.has_returned(seq) {
            self.run_due();
        }
    }

    // Wait until the key's operations through `seq` have returned, or
    // until `stopped` is set when one is given. Whoever sets it wakes the
    // wait under the key's lock.
    // The queuing thread's wait for the key's operations through `seq`:
    // until they returned, or until the end of a stop passes with them
    // still out, which the key then says of itself. Whether they returned.
    fn wait_to_end(&self, seq: u64, end: &WaitEnd) -> bool {
        if self.has_returned(seq) {
            return true;
        }
        let held = self.waiting.lock().unwrap_or_else(PoisonError::into_inner);
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let (held, returned) = end.wait(&self.woke, held, || !self.has_returned(seq));
        self.waiters.fetch_sub(1, Ordering::SeqCst);
        drop(held);
        if !returned {
            self.gave_up.store(true, Ordering::SeqCst);
        }
        returned
    }

    // A reader's wait: until the key's operations through `seq` are placed,
    // or until `stopped` is set when one is given.
    fn wait_placed(&self, seq: u64, stopped: Option<&AtomicBool>) {
        let over = || {
            self.has_placed(seq) || stopped.is_some_and(|stopped| stopped.load(Ordering::SeqCst))
        };
        if over() {
            return;
        }
        let mut held = self.waiting.lock().unwrap_or_else(PoisonError::into_inner);
        self.waiters.fetch_add(1, Ordering::SeqCst);
        while !over() {
            held = self.woke.wait(held).unwrap_or_else(PoisonError::into_inner);
        }
        self.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

// One caller's key on one volume. Its operations are numbered from one in
// the order it submits them, and run in that order between themselves.
pub struct Submitter {
    lane: Arc<Lane>,
    key: Arc<Key>,
    // The caller's own way to the thread. Numbering and sending are one
    // step under it, so the thread sees the key's numbers in order; no
    // other key takes this lock.
    send: Mutex<mpsc::Sender<Mail>>,
}

impl Submitter {
    pub fn volume(&self) -> &Arc<Volume> {
        &self.lane.state.volume
    }

    // Have the key's owner hear `room`, once, when the bytes counted on the
    // volume and not written (`Volume::unwritten_bytes`) are `bytes` or
    // fewer. False when they already are: nothing was asked and no wake
    // comes. One line is kept per volume, the highest asked
    // (`Volume::ask_wake`). The writer takes the wake as it writes; a
    // caller that lowers the count itself takes it with
    // `VolumeWriters::take_wake`.
    pub fn wake_below(&self, bytes: u64) -> bool {
        self.lane.state.volume.ask_wake(bytes, self.key.owner.bit)
    }

    // Write `bytes` at `offset` of `file`, opening it through `create` when
    // it is not open. The bytes are handed over, not copied. A write to a
    // file whose name was refused is dropped: the refusal has said so. A
    // write to any other file that is neither open nor given a way to open
    // fails the key.
    //
    // What is queued counts against the volume's hard limit before it is
    // queued: first out of `from`, a reservation on this volume, whose
    // bytes move to the write and are given back by the writer once it is
    // written; past that, counted once it fits (`Volume::fits`), the
    // caller standing still for the writer until then. A write longer than
    // `Volume::write_piece` is queued in pieces of that size, each counted
    // as it fits. When that wait is given up at the end of a stop, the rest
    // of the write is not queued (`Submitter::gave_up`).
    pub fn write_from(
        &self,
        site: LoopSite,
        from: &mut Reservation,
        file: u64,
        offset: u64,
        bytes: Bytes,
        create: Option<Create>,
    ) -> u64 {
        debug_assert!(
            Arc::ptr_eq(from.volume(), &self.lane.state.volume),
            "a write is paid on its own volume"
        );
        let paid = (bytes.len() as u64).min(from.bytes());
        from.hand_over(paid);
        self.queue_paid(site, file, offset, bytes, create, paid)
    }

    // `write_from` for the queuing thread, paid out of what its owner
    // prepaid (`Owner::prepay`) instead of a reservation in hand: the one
    // thread that prepays and writes needs no reservation passed along.
    pub fn write(
        &self,
        site: LoopSite,
        file: u64,
        offset: u64,
        bytes: Bytes,
        create: Option<Create>,
    ) -> u64 {
        let prepaid = &self.lane.state.volume.prepaid[self.key.owner.slot];
        let credit = prepaid.load(Ordering::Relaxed);
        let paid = (bytes.len() as u64).min(credit);
        prepaid.store(credit - paid, Ordering::Relaxed);
        self.queue_paid(site, file, offset, bytes, create, paid)
    }

    // Queue the write, `paid` of its bytes already counted on the volume.
    #[inline]
    fn queue_paid(
        &self,
        site: LoopSite,
        file: u64,
        offset: u64,
        bytes: Bytes,
        create: Option<Create>,
        paid: u64,
    ) -> u64 {
        let len = bytes.len() as u64;
        if paid >= len {
            self.lane
                .state
                .volume
                .buffered_bytes
                .fetch_add(len, Ordering::Relaxed);
            return self.submit(Op::Write {
                file,
                offset,
                bytes,
                create: create.map(Box::new),
            });
        }
        self.write_charged(site, file, offset, bytes, create, paid)
    }

    // A write past what was paid: `credit` of the bytes are paid.
    #[cold]
    fn write_charged(
        &self,
        site: LoopSite,
        file: u64,
        mut offset: u64,
        mut bytes: Bytes,
        mut create: Option<Create>,
        mut credit: u64,
    ) -> u64 {
        let volume = &self.lane.state.volume;
        let share = volume.write_piece();
        loop {
            let len = bytes.len() as u64;
            let piece = len.min(credit.saturating_add(share));
            let charged = piece - credit.min(piece);
            credit -= credit.min(piece);
            if charged > 0 {
                if !self.wait_room(site, charged) {
                    // The end of a stop passed with the volume still full:
                    // nothing more is queued, and the key says it gave up.
                    // What of the piece was prepaid is not queued either,
                    // and no longer counts.
                    volume.release(piece - charged);
                    return self.key.submitted.load(Ordering::Acquire);
                }
                volume.charge(charged);
            }
            let rest = bytes.split_off(piece as usize);
            volume.buffered_bytes.fetch_add(piece, Ordering::Relaxed);
            let seq = self.submit(Op::Write {
                file,
                offset,
                bytes,
                create: create.take().map(Box::new),
            });
            if rest.is_empty() {
                return seq;
            }
            offset += piece;
            bytes = rest;
        }
    }

    // Cut `file` to nothing, when it is open or `create.path` is there.
    pub fn truncate(&self, file: u64, create: Create) -> u64 {
        self.submit(Op::Truncate { file, create })
    }

    // `file` takes its final `length` and its handle is closed.
    pub fn close(&self, file: u64, length: u64, create: Create) -> u64 {
        self.submit(Op::Close {
            file,
            length,
            create,
        })
    }

    // Close the handle of `file`, changing nothing of it.
    pub fn release(&self, file: u64) -> u64 {
        self.submit(Op::Release { file })
    }

    // Run `run` on the writer thread after everything submitted before it
    // and before everything after: a rename, a link, a removal or a
    // directory made. The handle of `file`, when one is named, is closed
    // first. `path` names the failure when `run` fails.
    pub fn run(
        &self,
        file: Option<u64>,
        path: PathBuf,
        run: impl FnOnce() -> io::Result<()> + Send + 'static,
    ) -> u64 {
        self.submit(Op::Run {
            file,
            path,
            run: Box::new(run),
            own: false,
        })
    }

    // Run `run` on the writer thread as `run` does, for a caller that
    // judges what it did by itself and does it itself when it is dropped
    // unrun: `run` hands its result to the caller, and a volume with no
    // thread to run it on is no failure of the key.
    pub fn ask(&self, path: PathBuf, run: impl FnOnce() + Send + 'static) -> u64 {
        self.submit(Op::Run {
            file: None,
            path,
            run: Box::new(move || {
                run();
                Ok(())
            }),
            own: true,
        })
    }

    // Run `run` on the writer thread when the volume has nothing queued:
    // the first time its writer runs out of work, or at once when no caller
    // holds the volume. It is no operation of any key: no barrier waits for
    // it, a settle does not, and nothing queued before or after it stands
    // behind it. What arrives while it runs waits for it. It is dropped
    // unrun when the owner is stopping or the volume has no thread.
    pub fn when_idle(&self, run: impl FnOnce() + Send + 'static) {
        let send = self.send.lock().unwrap_or_else(PoisonError::into_inner);
        let _ = send.send(Mail::Idle(Box::new(run)));
        drop(send);
        self.lane.run_inline();
    }

    // Stand still until `bytes` or fewer wait in memory for the volume's
    // writer: the wait of a thread that produces what is written, such as
    // an extraction, on the volume's budget. Returns at once when no more
    // than that wait, and whatever waits once the key has stopped, by a
    // failure or by being given up, or the volume has no writer thread:
    // nothing more of the key will be written then. Whatever waits, too,
    // once the owner is stopping (`VolumeWriters::stop_waits`): the thread
    // is being ended and produces nothing more. The writer wakes it as the
    // bytes fall; nothing is polled.
    //
    // Never for the queuing thread, which must not stand still for a disk:
    // the queuing thread asks `VolumeWriters::wake_below` and goes on, or, where it
    // queues what is written itself, waits in `wait_room`. The
    // wait is not counted against the queuing thread.
    pub fn wait_below(&self, bytes: u64) {
        let state = &self.lane.state;
        let over = || {
            state.volume.buffered_bytes.load(Ordering::SeqCst) > bytes
                && !self.key.failed.load(Ordering::SeqCst)
                && !self.key.abandoned.load(Ordering::SeqCst)
                && self.lane.threadless.is_none()
                && !state.stopped.load(Ordering::SeqCst)
        };
        if !over() {
            return;
        }
        // Counted in before the look that decides to wait, and the look is
        // made with the lock held: the writer lowers the bytes first and
        // looks for waiters after, so it either finds this one and wakes
        // it under the lock, or this look finds the bytes already lowered.
        state.volume.below_waiters.fetch_add(1, Ordering::SeqCst);
        let mut held = state
            .volume
            .below_waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        #[cfg(any(test, feature = "test-support"))]
        state.volume.below.notify_all();
        while over() {
            held = state
                .volume
                .below
                .wait(held)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.volume.below_waiters.fetch_sub(1, Ordering::SeqCst);
        drop(held);
    }

    // A producing thread's wait to count `bytes` it is about to hand the
    // queuing thread (`Credit::reserve`): it stands still here, on its own
    // thread, until they are counted below the volume's hard limit
    // (`Reservation::reserve`), and the reservation goes with the
    // bytes. Nothing is reserved once the key stopped, by a failure or by
    // being given up, or the volume has no writer thread: nothing more of
    // the key is written then. `stand` says what a stop does to the wait:
    // it ends it, or the wait keeps the volume's limit until the end of the
    // stop, as the queuing thread's own waits do (`wait_room`), and the key
    // says it gave up when that end passed with the bytes still over. The
    // writer, and whoever gives counted bytes back, wake it; nothing is
    // polled. Counted with the waits made aside, never the queuing
    // thread's. The grant is judged beside what the queuing threads carry
    // on the volume (`Volume::grant`).
    fn reserve_ahead(&self, bytes: u64, stand: Stand) -> Reservation {
        let state = &self.lane.state;
        let held = std::cell::RefCell::new(Reservation::none(Arc::clone(&state.volume)));
        let over = || !self.ends_reserve(stand) && !held.borrow_mut().reserve(bytes, 0);
        if !over() {
            return held.into_inner();
        }
        let began = Instant::now();
        // Counted in before the look made under the lock, as in
        // `wait_below`.
        state.volume.below_waiters.fetch_add(1, Ordering::SeqCst);
        let mut lock = state
            .volume
            .below_waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        #[cfg(any(test, feature = "test-support"))]
        state.volume.below.notify_all();
        let below = match stand {
            Stand::UntilStop => {
                while over() {
                    lock = state
                        .volume
                        .below
                        .wait(lock)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                true
            }
            Stand::UntilStopsEnd => {
                let below;
                (lock, below) = state.wait_end.wait(&state.volume.below, lock, over);
                below
            }
        };
        state.volume.below_waiters.fetch_sub(1, Ordering::SeqCst);
        drop(lock);
        state
            .volume
            .writer
            .aside_waited(began.elapsed().as_nanos() as u64);
        if !below {
            self.key.gave_up.store(true, Ordering::SeqCst);
        }
        held.into_inner()
    }

    // `reserve_ahead` without standing: `None` when the volume refuses the
    // bytes now. Whatever ends a stand ends this too, with nothing counted.
    fn try_reserve_ahead(&self, bytes: u64, stand: Stand) -> Option<Reservation> {
        let mut held = Reservation::none(Arc::clone(&self.lane.state.volume));
        (self.ends_reserve(stand) || held.reserve(bytes, 0)).then_some(held)
    }

    // Whether a producing thread's wait for room under the key ends with
    // nothing counted: the key stopped, by a failure or by being given up,
    // the volume has no writer thread, or the owner is stopping and `stand`
    // ends the wait at that.
    fn ends_reserve(&self, stand: Stand) -> bool {
        self.key.failed.load(Ordering::SeqCst)
            || self.key.abandoned.load(Ordering::SeqCst)
            || self.lane.threadless.is_some()
            || (stand == Stand::UntilStop && self.lane.state.stopped.load(Ordering::SeqCst))
    }

    // The bytes the queuing thread carries on the volume under the key
    // changed by `more` and `less` (`Credit`).
    fn carried(&self, more: u64, less: u64) {
        let carried = &self.key.carried;
        carried.fetch_add(more, Ordering::SeqCst);
        let before = carried
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |carried| {
                Some(carried.saturating_sub(less))
            })
            .unwrap_or_else(|carried| carried);
        debug_assert!(before >= less, "a key uncarried more than it carried");
        let volume = &self.lane.state.volume;
        volume.carry(more);
        volume.uncarry(less);
    }

    // The queuing thread's wait to queue `bytes` of its own: it stands
    // still until they fit below the volume's hard limit (`Volume::fits`),
    // counted as a budget wait. A stop does not end it: the queuing thread keeps the
    // volume's limit for as long as it queues. It ends when the bytes fit,
    // the key stopped, or the end of a stop passed with them still over,
    // which the key then says of itself. Whether the queuing thread may queue more:
    // false only at that end.
    pub fn wait_room(&self, site: LoopSite, bytes: u64) -> bool {
        let state = &self.lane.state;
        let over = || {
            !state.volume.fits(bytes)
                && !self.key.failed.load(Ordering::SeqCst)
                && !self.key.abandoned.load(Ordering::SeqCst)
                && self.lane.threadless.is_none()
        };
        if !over() {
            return true;
        }
        let began = Instant::now();
        // Counted in before the look made under the lock, as in
        // `wait_below`.
        state.volume.below_waiters.fetch_add(1, Ordering::SeqCst);
        let held = state
            .volume
            .below_waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        #[cfg(any(test, feature = "test-support"))]
        state.volume.below.notify_all();
        let (held, below) = state.wait_end.wait(&state.volume.below, held, over);
        state.volume.below_waiters.fetch_sub(1, Ordering::SeqCst);
        drop(held);
        state.waited(LoopWait::Budget, site, began);
        if !below {
            self.key.gave_up.store(true, Ordering::SeqCst);
        }
        below
    }

    // Whether a wait of the queuing thread on the key was given up at the
    // end of a stop: something of the key is still out, and what the queuing thread
    // did after it is borne out by nothing on disk.
    pub fn gave_up(&self) -> bool {
        self.key.gave_up.load(Ordering::SeqCst)
    }

    // Whether the owner is stopping (`VolumeWriters::stop_waits`): a wait
    // aside on the volume ends at once from here, its operations still out.
    pub fn stopping(&self) -> bool {
        self.lane.state.stopped.load(Ordering::SeqCst)
    }

    // Read `len` bytes at `offset` of the file at `path`, after everything
    // the key submitted before. The caller waits for the answer. Only the
    // tests read this way: the queuing thread waits for the file's own
    // writes (`wait_for`) and reads it itself, never behind every file of
    // its key. A thread that is not the queuing thread waits with
    // `Barrier::wait_aside` and reads the file itself.
    #[cfg(test)]
    pub(crate) fn read(&self, path: PathBuf, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.read_after(
            self.key.submitted.load(Ordering::Acquire),
            path,
            offset,
            len,
        )
    }

    // The same read, after the key's operations through `seq` only: the
    // number the last submission for the file returned. The read is made
    // here, by the caller, as soon as those have returned: it waits for no
    // other key, queued or running.
    #[cfg(test)]
    pub(crate) fn read_after(
        &self,
        seq: u64,
        path: PathBuf,
        offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        self.wait_for(LoopSite::ReadOn, seq)?;
        read_at(&path, offset, len)
    }

    // The wait before such a read alone: the key's operations through `seq`
    // have returned once this does, and the caller reads the file itself,
    // through a handle it already holds when it has one.
    pub fn wait_for(&self, site: LoopSite, seq: u64) -> io::Result<()> {
        if !self.key.has_returned(seq) {
            let began = Instant::now();
            let returned = self.key.wait_to_end(seq, &self.lane.state.wait_end);
            self.lane.state.waited(LoopWait::Read, site, began);
            if !returned {
                return Err(io::Error::other(
                    "the owner is stopping and the file's writes are still out",
                ));
            }
        }
        if self.key.stopped() {
            return Err(io::Error::other("the file's writes were given up"));
        }
        Ok(())
    }

    // A read of the queuing thread's own, made after `wait_for`, counted on the
    // volume: how many and how long, the open of the file included.
    pub fn loop_read(&self, began: Instant) {
        self.lane.state.loop_read(began);
    }

    // Everything submitted under the key so far.
    pub fn barrier(&self) -> Barrier {
        self.barrier_at(self.key.submitted.load(Ordering::Acquire))
    }

    // Everything submitted under the key through the operation `seq`, which
    // a submission returned.
    pub fn barrier_at(&self, seq: u64) -> Barrier {
        Barrier {
            lane: Arc::clone(&self.lane.state),
            key: Arc::clone(&self.key),
            seq,
        }
    }

    // Whether an operation under the key failed.
    #[cfg(any(test, feature = "test-support"))]
    pub fn failed(&self) -> bool {
        self.key.failed.load(Ordering::Acquire)
    }

    // The caller has heard a refusal out and answered it.
    pub fn heard_refusal(&self) {
        self.key.unheard.fetch_sub(1, Ordering::AcqRel);
    }

    // The caller is done with the key: what is still queued under it is not
    // run, and every handle it left open is closed. The barrier taken after
    // this passes once nothing of the key can touch the disk again.
    pub fn abandon(&self) -> Barrier {
        self.key.abandoned.store(true, Ordering::SeqCst);
        self.lane.state.look_below();
        self.submit(Op::Forget);
        self.barrier()
    }

    fn submit(&self, op: Op) -> u64 {
        let state = &self.lane.state;
        let send = self.send.lock().unwrap_or_else(PoisonError::into_inner);
        let seq = self.key.submitted.load(Ordering::Relaxed) + 1;
        self.key.submitted.store(seq, Ordering::Release);
        state.sent.fetch_add(1, Ordering::Relaxed);
        let sent = send.send(Mail::Op(Queued {
            seq,
            key: Arc::clone(&self.key),
            op,
        }));
        drop(send);
        if let Err(mpsc::SendError(Mail::Op(unsent))) = sent {
            // The volume has no thread, so nothing will be written: the
            // bytes stop waiting and the barrier does not hold anyone.
            if let Op::Write { bytes, .. } = &unsent.op {
                unbuffer(state, bytes.len() as u64);
            }
            state.done.fetch_add(1, Ordering::Relaxed);
            // The caller makes this one itself: nothing failed.
            if matches!(unsent.op, Op::Run { own: true, .. }) {
                state.returned(state.sent.load(Ordering::SeqCst));
                self.key.returned(seq);
                return seq;
            }
            let counts = &state.volume.writer;
            counts.failed_operations.fetch_add(1, Ordering::Relaxed);
            let first = !self.key.failed.swap(true, Ordering::AcqRel);
            self.key.returned(seq);
            // The caller hears of it once, as it hears of any failure.
            if first
                && !self.key.abandoned.load(Ordering::Acquire)
                && !matches!(unsent.op, Op::Forget | Op::Release { .. })
            {
                let owner = &self.key.owner.owner;
                owner.failed(Failure {
                    key: self.key.id,
                    file: unsent.op.file(),
                    path: unsent.op.path(),
                    error: io::Error::other("the volume's writer thread is gone"),
                    creating: false,
                });
                owner.room();
            }
            // Nothing sent to the volume will run, so all of it has
            // returned: a settle does not wait for it. Said after the
            // failure is reported, so a settle never ends ahead of it.
            state.returned(state.sent.load(Ordering::SeqCst));
        }
        self.lane.run_inline();
        seq
    }
}

impl Drop for Submitter {
    fn drop(&mut self) {
        // What is queued under the key still runs, unless it was given up;
        // the handles it left open close behind it.
        self.submit(Op::Forget);
        self.lane.leave();
    }
}

// Everything a key submitted before a moment: passed once all of it has
// returned, whatever other keys still have queued on the volume.
#[derive(Clone)]
pub struct Barrier {
    lane: Arc<LaneState>,
    key: Arc<Key>,
    seq: u64,
}

impl Barrier {
    // Whether everything behind the barrier returned, none of the key's
    // operations failed or was given up, and no refusal waits to be heard.
    pub fn passed(&self) -> bool {
        self.key.has_returned(self.seq)
            && !self.key.stopped()
            && self.key.unheard.load(Ordering::Acquire) == 0
    }

    // Whether an operation of the key failed or the key was given up:
    // nothing more of it is written. A refusal still to be heard is not a
    // failure.
    pub fn failed(&self) -> bool {
        self.key.stopped()
    }

    // Whether everything behind the barrier returned, failed or not.
    pub fn returned(&self) -> bool {
        self.key.has_returned(self.seq)
    }

    // Wait until everything behind the barrier has returned, failed or not.
    // For the queuing thread only: a wait that stops here is counted as one
    // of its waits on the volume. Every other thread uses `wait_aside`. Once
    // the owner is stopping the wait also ends at the end of the stop
    // (`VolumeWriters::end_waits_at`) with the operations still out:
    // the caller asks `unwritten`.
    pub fn wait(&self, site: LoopSite) {
        if self.returned() {
            return;
        }
        let began = Instant::now();
        self.key.wait_to_end(self.seq, &self.lane.wait_end);
        self.lane.waited(LoopWait::Settle, site, began);
    }

    // What a caller asks after `wait` or `wait_aside`: the operations are
    // not all on disk, because one failed, the key was given up, or the
    // wait ended at a stop with some still out. Nothing may be read back as if they were.
    // Asked of what is placed: a handle the writer has yet to drop does not
    // keep bytes off disk.
    pub fn unwritten(&self) -> bool {
        self.failed() || !self.key.has_placed(self.seq)
    }

    // The same wait for any thread that is not the queuing thread: a check,
    // a repair, a replay or an extraction on a thread of its own. It is
    // counted apart (`aside_waits`), so the queuing thread's wait counters
    // say only what it stood for.
    // It also ends once the owner is stopping
    // (`VolumeWriters::stop_waits`), with the operations still out: the
    // caller asks `unwritten` and reads nothing back when they are.
    // It waits until the operations are placed, not returned: a reader opens
    // its own handle, so the writer's drop of the handles a close or a
    // release let go is not waited for. On some systems that drop is slow
    // for a large file just written.
    pub fn wait_aside(&self) {
        if self.key.has_placed(self.seq) {
            return;
        }
        let began = Instant::now();
        let aside = || {
            self.lane
                .aside
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        };
        aside().push(Arc::clone(&self.key));
        self.key.wait_placed(self.seq, Some(&self.lane.stopped));
        let mut aside = aside();
        if let Some(at) = aside.iter().position(|key| Arc::ptr_eq(key, &self.key)) {
            aside.swap_remove(at);
        }
        drop(aside);
        // Counted apart from the queuing thread's waits: what a check, a repair or a
        // replay stood for the writer.
        self.lane
            .volume
            .writer
            .aside_waited(began.elapsed().as_nanos() as u64);
    }

    // Have a wait of the queuing thread on the barrier's key look at its clock
    // again, after a test moved it.
    #[cfg(test)]
    pub(crate) fn look_again(&self) {
        let _held = self
            .key
            .waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.key.woke.notify_all();
    }

    // Wait until `count` threads stand in a wait on the barrier's key.
    #[cfg(any(test, feature = "test-support"))]
    pub fn wait_waiters(&self, count: usize) {
        while self.key.waiters.load(Ordering::SeqCst) < count {
            std::thread::yield_now();
        }
    }

    // Whether a test holds the volume's thread.
    #[cfg(any(test, feature = "test-support"))]
    pub fn test_held(&self) -> bool {
        self.lane
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .held
    }

    // Run `run` once everything behind the barrier has returned, failed or
    // not: here and now when it already has, and otherwise on the volume's
    // thread, which it must not hold up.
    pub fn then(&self, run: impl FnOnce() + Send + 'static) {
        self.key.then(self.seq, Box::new(run));
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
    // The way to the thread; each caller takes a copy of its own.
    send: mpsc::Sender<Mail>,
    // A lane with no thread: what a thread would hold, run by whoever
    // submits.
    inline: Option<Mutex<(Thread, mpsc::Receiver<Mail>)>>,
    // Set on a lane whose thread could not be started: where it is kept
    // until its last caller lets go.
    threadless: Option<Weak<Registry>>,
}

struct LaneState {
    volume: Arc<Volume>,
    // Callers that hold the volume. The thread ends only at none.
    users: AtomicUsize,
    // Operations submitted to the volume, and operations that returned.
    sent: AtomicU64,
    done: AtomicU64,
    // Of the operations in the order they reached the thread, every one
    // through this many has returned. Only a settle waits on it.
    returned: AtomicU64,
    waiters: AtomicUsize,
    waiting: Mutex<()>,
    woke: Condvar,
    // Threads standing for room on the volume wait on the volume itself
    // (`Volume::look_below`), so whoever lowers its count wakes them.
    // The owner is stopping: see `VolumeWriters::stop_waits`.
    stopped: Arc<AtomicBool>,
    // Where the queuing thread's own waits end once the owner is stopping.
    wait_end: Arc<WaitEnd>,
    // The keys threads stand in `Barrier::wait_aside` for, so a stop can
    // wake them.
    aside: Mutex<Vec<Arc<Key>>>,
    // Who hears a volume's wakes, by the bit each asked under.
    owners: Arc<Owners>,
    #[cfg(any(test, feature = "test-support"))]
    gate: Mutex<Gate>,
    #[cfg(any(test, feature = "test-support"))]
    gate_moved: Condvar,
}

impl LaneState {
    // The queuing thread stood still for the writer since `began`. Called
    // only from the waits the queuing thread itself makes.
    fn waited(&self, kind: LoopWait, site: LoopSite, began: Instant) {
        self.volume
            .writer
            .loop_waited(kind, site, began.elapsed().as_nanos() as u64);
    }

    fn loop_read(&self, began: Instant) {
        self.volume
            .writer
            .loop_read(began.elapsed().as_nanos() as u64);
    }

    // Have every thread standing for room on the volume look again: a key
    // stopped, or what a requester carries changed (`Volume::look_below`;
    // a lower count wakes them by itself). One load when no thread waits.
    fn look_below(&self) {
        if self.volume.below_waited() {
            break_span();
            self.volume.look_below();
        }
    }

    fn returned(&self, through: u64) {
        self.returned.fetch_max(through, Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) > 0 {
            break_span();
            let _held = self.waiting.lock().unwrap_or_else(PoisonError::into_inner);
            self.woke.notify_all();
        }
    }
}

// `bytes` no longer wait for the volume's writer. Whoever asked to be woken
// at a line the count has now reached is woken, once.
fn unbuffer(state: &LaneState, bytes: u64) {
    if bytes == 0 {
        return;
    }
    let volume = &state.volume;
    volume.buffered_bytes.fetch_sub(bytes, Ordering::SeqCst);
    // The release wakes whoever stands for room.
    if volume.below_waited() {
        break_span();
    }
    volume.release(bytes);
    state.owners.room(volume.take_wake());
}

impl Lane {
    // The lane, with its thread started. When no thread could be started
    // nothing will run what is sent to the lane: every submission fails its
    // key at once.
    fn start(
        volume: Arc<Volume>,
        registry: &Arc<Registry>,
        inline: bool,
        threadless: bool,
    ) -> Arc<Self> {
        // Taken once, at the lane's start: the owner rings the rates before
        // any caller asks for a lane (`VolumeWriters::ring`).
        let rates = registry.rates.get().cloned();
        let owners = Arc::clone(&registry.owners);
        let stopped = Arc::clone(&registry.stopped);
        let wait_end = Arc::clone(&registry.wait_end);
        let registry = Arc::downgrade(registry);
        let (send, queued) = mpsc::channel();
        let state = Arc::new(LaneState {
            volume,
            users: AtomicUsize::new(0),
            sent: AtomicU64::new(0),
            done: AtomicU64::new(0),
            returned: AtomicU64::new(0),
            waiters: AtomicUsize::new(0),
            waiting: Mutex::new(()),
            woke: Condvar::new(),
            stopped,
            wait_end,
            aside: Mutex::default(),
            owners,
            #[cfg(any(test, feature = "test-support"))]
            gate: Mutex::default(),
            #[cfg(any(test, feature = "test-support"))]
            gate_moved: Condvar::new(),
        });
        let thread = Thread::new(Arc::clone(&state), rates, registry.clone());
        if inline {
            return Arc::new(Self {
                state,
                send,
                inline: Some(Mutex::new((thread, queued))),
                threadless: None,
            });
        }
        let name = format!("volume-writer {}", state.volume.label);
        let run = move || thread.run(queued);
        let spawned = if threadless {
            drop(run);
            Err(io::Error::other("no thread to be had"))
        } else {
            std::thread::Builder::new().name(name).spawn(run).map(drop)
        };
        if let Err(error) = &spawned {
            // With no thread the receiver is gone, and every submission
            // fails its key at once instead of waiting forever.
            tracing::error!(
                %error,
                volume = %state.volume.label,
                "could not start a volume's writer thread; what is written to the volume fails until it is let go and asked for again"
            );
        }
        Arc::new(Self {
            state,
            send,
            inline: None,
            threadless: spawned.is_err().then_some(registry),
        })
    }

    // A caller lets the volume go. The thread is woken to see whether it
    // was the last. A lane with no thread leaves the registry with its last
    // caller, decided under the registry's lock as a thread's end is, so
    // none takes the lane as it goes.
    fn leave(&self) {
        self.state.users.fetch_sub(1, Ordering::SeqCst);
        let _ = self.send.send(Mail::Left);
        self.run_inline();
        let Some(registry) = self.threadless.as_ref().and_then(Weak::upgrade) else {
            return;
        };
        let mut lanes = registry.lock();
        if self.state.users.load(Ordering::SeqCst) == 0 {
            lanes.retain(|(_, lane)| !Arc::ptr_eq(&lane.state, &self.state));
            #[cfg(any(test, feature = "test-support"))]
            registry.ended.notify_all();
        }
    }

    // On a lane with no thread, run what was sent.
    fn run_inline(&self) {
        let Some(inline) = &self.inline else {
            return;
        };
        let mut inline = inline.lock().unwrap_or_else(PoisonError::into_inner);
        let (thread, queued) = &mut *inline;
        while let Ok(mail) = queued.try_recv() {
            match mail {
                Mail::Op(first) => {
                    if thread.batch(first, queued) {
                        thread.worked = true;
                    }
                }
                Mail::Idle(run) => thread.idle.push(run),
                Mail::Left => {}
            }
        }
        while thread.run_idle() {}
    }
}

enum Mail {
    Op(Queued),
    // Work of the volume's own, for when its writer has nothing queued.
    Idle(Idle),
    // A caller let the volume go.
    Left,
}

type Idle = Box<dyn FnOnce() + Send>;

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
        // The caller judges the result and makes the operation itself
        // when the volume drops it unrun.
        own: bool,
    },
    // Close every handle the key left open and forget its files.
    Forget,
}

impl Op {
    // The bytes the operation writes: none but for a write.
    fn write_len(&self) -> u64 {
        match self {
            Op::Write { bytes, .. } => bytes.len() as u64,
            _ => 0,
        }
    }

    fn file(&self) -> Option<u64> {
        match self {
            Op::Write { file, .. }
            | Op::Truncate { file, .. }
            | Op::Close { file, .. }
            | Op::Release { file } => Some(*file),
            Op::Run { .. } | Op::Forget => None,
        }
    }

    // The path the operation is about, to name a failure.
    fn path(&self) -> PathBuf {
        match self {
            Op::Write { create, .. } => create
                .as_ref()
                .map(|create| create.path.clone())
                .unwrap_or_default(),
            Op::Truncate { create, .. } | Op::Close { create, .. } => create.path.clone(),
            Op::Run { path, .. } => path.clone(),
            Op::Release { .. } | Op::Forget => PathBuf::new(),
        }
    }
}

struct Open {
    file: File,
    // Where it was opened, to name a failure.
    path: PathBuf,
    // Where the last write to it ended; `None` before its first.
    end: Option<u64>,
    // The file's length as this handle left it: what the opener said, grown
    // by every write past it and set by every length set. `None` when the
    // opener did not say, or a write or a length set failed. Only this
    // writer changes the length of a file while it holds it open.
    length: Option<u64>,
    // The file is in the key's closed files, its handle kept since a close.
    closed: bool,
    // Bytes written through this handle since its last write-out, on a
    // volume that starts them (`Volume::write_out`).
    since_out: u64,
}

impl Held {
    // Let the handle of `file` go, to be dropped with the key's others.
    // Whether there was one.
    fn let_go(&mut self, file: u64) -> bool {
        let Some(open) = self.take(file) else {
            return false;
        };
        self.letting_go.push(open);
        true
    }

    // The handle of `file`, no longer the key's.
    fn take(&mut self, file: u64) -> Option<Open> {
        let open = self.open.remove(&file)?;
        if open.closed {
            self.unclose(file);
        }
        Some(open)
    }

    // A write, truncate or close of a kept closed file makes it an open
    // file again: it leaves the closed files, so the bound never lets go of
    // a file in use, and a close puts it back at the newest end. Whether
    // the key holds `file` open.
    fn in_use(&mut self, file: u64) -> bool {
        let Some(open) = self.open.get_mut(&file) else {
            return false;
        };
        if open.closed {
            open.closed = false;
            self.unclose(file);
        }
        true
    }

    // `file` leaves the closed files: it is in use again, or let go.
    fn unclose(&mut self, file: u64) {
        if let Some(at) = self.closed.iter().position(|closed| *closed == file) {
            self.closed.remove(at);
        }
    }

    // A close ended `file`: its handle is kept, and past `KEPT_CLOSED` the
    // oldest kept is let go. Whether one was.
    fn keep_closed(&mut self, file: u64) -> bool {
        let Some(open) = self.open.get_mut(&file) else {
            return false;
        };
        // A close's own open took it out of the closed files.
        debug_assert!(!open.closed);
        open.closed = true;
        self.closed.push_back(file);
        if self.closed.len() <= KEPT_CLOSED {
            return false;
        }
        let oldest = self.closed.pop_front().expect("past the bound");
        self.let_go(oldest)
    }

    // The open handle of `file`, opened through `create` when it is not
    // open. `None` for a file whose name was refused; an error for any other
    // file with no handle and no way to open one.
    fn opened(
        &mut self,
        file: u64,
        create: Option<&Create>,
        creating: &mut bool,
        counts: &WriterCounts,
    ) -> io::Result<Option<&mut Open>> {
        if !self.in_use(file) {
            // The writes of a file whose name was refused are dropped: the
            // refusal has said so.
            if self.refused.contains(&file) {
                return Ok(None);
            }
            // Any other write to a file that is not open must bring the way
            // to open it. One that does not would be lost without a word.
            let Some(create) = create else {
                return Err(io::Error::other(
                    "a write reached the volume's writer for a file that is not open, with no way to open it",
                ));
            };
            *creating = true;
            // Timed, failed or not: an open on some systems stands behind
            // what the file system is writing out.
            let began = now();
            let opened = (create.open)(&create.path, create.length, create.within.as_deref());
            let took = nanos_since(began);
            counts.opened(took);
            break_span();
            let opened = opened?;
            let [made, marked, sized] =
                [opened.steps.create, opened.steps.mark, opened.steps.size].map(OpenSteps::nanos);
            counts.open_steps(made, marked, sized);
            if took > SLOW_OPEN.as_nanos() as u64 {
                let micros = |step: Option<u64>| step.map(|nanos| nanos / 1_000);
                tracing::warn!(
                    path = %create.path.display(),
                    declared_length = ?create.length,
                    length = ?opened.length,
                    open_us = took / 1_000,
                    create_us = ?micros(made),
                    mark_us = ?micros(marked),
                    size_us = ?micros(sized),
                    "the volume's writer took over 100 ms to open a file"
                );
            }
            *creating = false;
            self.open.insert(
                file,
                Open {
                    file: opened.file,
                    path: create.path.clone(),
                    end: None,
                    length: opened.length,
                    closed: false,
                    since_out: 0,
                },
            );
        }
        Ok(self.open.get_mut(&file))
    }

    // Drop the handles let go, and `also`, timed for the volume's counts:
    // on some systems the close of a large file just written is slow.
    fn drop_let_go(&mut self, also: Option<Open>, counts: &WriterCounts) {
        let handles = self.letting_go.len() as u64 + u64::from(also.is_some());
        if handles == 0 {
            return;
        }
        let began = now();
        drop(also);
        self.letting_go.clear();
        counts.closed(handles, nanos_since(began));
        break_span();
    }
}

// Set the length of `open`, timed for the volume's counts, and keep what it
// left. A length set on some systems stands behind what the file system is
// writing out of the file.
fn set_length(open: &mut Open, length: u64, counts: &WriterCounts) -> io::Result<()> {
    let began = now();
    let set = open.file.set_len(length);
    counts.length_set(nanos_since(began));
    break_span();
    open.length = set.is_ok().then_some(length);
    set
}

fn nanos_since(began: Instant) -> u64 {
    now().saturating_duration_since(began).as_nanos() as u64
}

// The bytes written to one file between two write-outs of it, on a volume
// that starts them. Each write-out starts the write-out of what was written
// since the last and waits for what that one started, so the OS holds at
// most about two strides of a file unwritten, and the writer stands still
// often and briefly instead of for minutes at a close.
pub(crate) const WRITE_OUT_STRIDE: u64 = 32 * 1024 * 1024;

// `wrote` more bytes went to `open` on `volume`: on a volume that starts
// write-outs, start one once a stride of them has gone since the last. On
// the writer's thread, timed for the volume's counts and not as write time.
// A write-out that fails stops them on the volume; the write stands.
fn write_out_due(volume: &Volume, open: &mut Open, wrote: u64) {
    let Some(write_out) = volume.write_out(&open.file) else {
        return;
    };
    open.since_out = open.since_out.saturating_add(wrote);
    if open.since_out < WRITE_OUT_STRIDE {
        return;
    }
    open.since_out = 0;
    let began = now();
    let started = write_out(&open.path, &open.file);
    volume.writer.wrote_out(nanos_since(began));
    break_span();
    if let Err(error) = started {
        volume.write_out_failed(&open.path, &error);
    }
}

// Start the write-out of every page of `file` written and not yet sent,
// after waiting for the write-out already under way for it: one
// `sync_file_range` over the whole file with `WAIT_BEFORE | WRITE`. It
// waits for no write-out it starts, and no metadata is written: it is no
// sync, and no byte is on disk because it returned.
#[cfg(target_os = "linux")]
pub(super) fn start_write_out(_: &Path, file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: the descriptor is open for as long as `file` is borrowed; an
    // offset and length of zero name the whole file.
    let started = unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            0,
            0,
            libc::SYNC_FILE_RANGE_WAIT_BEFORE | libc::SYNC_FILE_RANGE_WRITE,
        )
    };
    if started == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

// What a batch has come to so far.
#[derive(Default)]
struct Ran {
    // Each with the owner of the key that failed.
    failures: Vec<(Owner, Failure)>,
    tally: Tally,
    // The bytes of the write being made, still counted as waiting.
    in_hand: u64,
    // When the write call before this operation returned, while nothing
    // else ran since: the next write's time is taken from it, so a run of
    // writes reads the clock once for each call. `None` at the start of a
    // batch and after any other operation, whose time is not a write's; a
    // write that opens its file first reads the clock again after the open.
    wrote_at: Option<Instant>,
    // The operation just run let a handle go (`Held::letting_go`). Its key
    // is told the operations from it on are placed as they run, and that
    // they returned only once its handles are dropped: a reader does not
    // wait behind the drop, and a settle, a checkpoint or a delivery still
    // does.
    let_go: bool,
    // The operation just run was a caller's code, run with its key's
    // handles dropped.
    dropped: bool,
}

// The writer's clock, for the time inside write calls. A test steps it from
// inside an open (`step_clock`); every read of a stepped clock moves it on
// by a millisecond.
fn now() -> Instant {
    #[cfg(test)]
    if let Some(at) = STEPPED.get() {
        let next = at + Duration::from_millis(1);
        STEPPED.set(Some(next));
        return next;
    }
    Instant::now()
}

#[cfg(test)]
thread_local! {
    static STEPPED: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
}

thread_local! {
    // Set when the writer did more between two write calls than a few
    // loads and stores: see `break_span`.
    static SPAN_BROKEN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// Work that can take microseconds ran on this thread: a wake of waiters, a
// caller's code, a report, or a buffer freed. A write chained from the
// last one's return (`Ran::wrote_at`) reads the clock again instead, so
// that work is not counted as write time. Between two calls without it
// sit a few atomic loads and stores and one uncontended lock, tens of
// nanoseconds against a write call of tens of microseconds or more.
fn break_span() {
    SPAN_BROKEN.set(true);
}

// Steps this thread's writer clock on by `by`, starting it when it is not
// stepped yet.
#[cfg(test)]
fn step_clock(by: Duration) {
    STEPPED.set(Some(STEPPED.get().unwrap_or_else(Instant::now) + by));
}

// What one batch adds to its volume's counters. Its write calls are
// counted on the volume as each returns; `calls` only says the batch made
// one.
#[derive(Default)]
struct Tally {
    calls: u64,
    operations: u64,
    abandoned: u64,
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

// One operation of the batch being run.
struct Entry {
    seq: u64,
    key: Arc<Key>,
    // Taken as the operation runs.
    op: Option<Op>,
    done: bool,
    // The key's next operation in the batch.
    next: u32,
}

struct Thread {
    state: Arc<LaneState>,
    rates: Option<Arc<RateSignal>>,
    registry: Weak<Registry>,
    batch: Vec<Entry>,
    order: Vec<Place>,
    // Each file of the batch, by its key and the key's number for it.
    files: HashMap<(usize, u64), (u32, u32, u32)>,
    ran: Ran,
    // The batch entries at which a key first let a handle go whose handles
    // are not dropped yet: the settle goes no further than the first.
    held: BTreeSet<u32>,
    // Operations that reached the thread, and batches it began.
    arrived: u64,
    turn: u64,
    // A batch that wrote, made, moved or removed something ran since the
    // thread last found nothing waiting. A batch of nothing but a caller's
    // bookkeeping, a handle closed or a key forgotten, is not work: it says
    // nothing of whether the caller's writes have come.
    worked: bool,
    // The volume's own work, kept for when nothing is queued.
    idle: Vec<Idle>,
}

impl Thread {
    fn new(
        state: Arc<LaneState>,
        rates: Option<Arc<RateSignal>>,
        registry: Weak<Registry>,
    ) -> Self {
        Self {
            state,
            rates,
            registry,
            batch: Vec::new(),
            order: Vec::new(),
            files: HashMap::new(),
            ran: Ran::default(),
            held: BTreeSet::new(),
            arrived: 0,
            turn: 0,
            worked: false,
            idle: Vec::new(),
        }
    }

    fn run(mut self, queued: mpsc::Receiver<Mail>) {
        self.state
            .volume
            .writer
            .threads
            .fetch_add(1, Ordering::Relaxed);
        loop {
            let mail = match queued.try_recv() {
                Ok(mail) => mail,
                Err(mpsc::TryRecvError::Disconnected) => return,
                Err(mpsc::TryRecvError::Empty) => {
                    // The volume's own work first, then a look at what came
                    // meanwhile.
                    if self.run_idle() {
                        continue;
                    }
                    match self.end(&queued) {
                        Ok(()) => return,
                        Err(Some(mail)) => mail,
                        Err(None) => {
                            #[cfg(any(test, feature = "test-support"))]
                            self.park(true);
                            let mail = queued.recv();
                            #[cfg(any(test, feature = "test-support"))]
                            self.park(false);
                            match mail {
                                Ok(mail) => {
                                    // The thread stood with nothing queued
                                    // and was woken: a send that found it
                                    // parked.
                                    self.state
                                        .volume
                                        .writer
                                        .woken
                                        .fetch_add(1, Ordering::Relaxed);
                                    mail
                                }
                                Err(_) => return,
                            }
                        }
                    }
                }
            };
            match mail {
                Mail::Op(first) => {
                    if self.batch(first, &queued) {
                        self.worked = true;
                    }
                }
                Mail::Idle(run) => self.idle.push(run),
                Mail::Left => {}
            }
        }
    }

    // Nothing is queued. Count the turn when the thread worked since the
    // last one, and run one piece of the volume's own work when the writer
    // has just run out of work or no caller holds the volume: a volume a
    // caller holds and has not written to yet keeps it, so the caller's
    // first writes find the thread free. Leaving the work here is itself followed
    // by a key forgotten, which is no work and so does not start it.
    // Whether a piece ran.
    fn run_idle(&mut self) -> bool {
        let worked = std::mem::take(&mut self.worked);
        if worked {
            self.state.volume.idle_turns.fetch_add(1, Ordering::Relaxed);
        }
        if self.idle.is_empty() {
            return false;
        }
        if self.state.stopped.load(Ordering::SeqCst) {
            self.idle.clear();
            return false;
        }
        if !worked && self.state.users.load(Ordering::SeqCst) > 0 {
            return false;
        }
        let run = self.idle.swap_remove(0);
        // A panic in it must not end the volume's thread.
        let _ = catch_unwind(AssertUnwindSafe(run));
        true
    }

    // End the thread when no caller holds the volume and nothing is queued:
    // the volume leaves the registry as the thread's last act. Decided under
    // the registry's lock, where a caller takes the volume, so none takes it
    // as the thread goes. `Err` when the thread stays, with what was queued
    // in the meantime.
    fn end(&self, queued: &mpsc::Receiver<Mail>) -> Result<(), Option<Mail>> {
        if self.state.users.load(Ordering::SeqCst) > 0 {
            return Err(None);
        }
        let registry = self.registry.upgrade();
        let mut lanes = registry.as_ref().map(|registry| registry.lock());
        if self.state.users.load(Ordering::SeqCst) > 0 {
            return Err(None);
        }
        if let Ok(mail) = queued.try_recv() {
            return Err(Some(mail));
        }
        if let Some(lanes) = &mut lanes {
            lanes.retain(|(_, lane)| !Arc::ptr_eq(&lane.state, &self.state));
        }
        #[cfg(any(test, feature = "test-support"))]
        if let Some(registry) = &registry {
            registry.ended.notify_all();
        }
        Ok(())
    }

    // Run `first` and everything queued behind it as one batch. Whether
    // the batch was work: it held more than a caller's bookkeeping.
    fn batch(&mut self, first: Queued, queued: &mpsc::Receiver<Mail>) -> bool {
        let mut bytes = first.op.write_len();
        self.batch.push(Entry::new(first));
        // A batch takes what is queued up to `BATCH_BYTES` of writes. The
        // ordering inside a batch can run one caller's operation behind
        // every other write of it, so an uncapped batch on a slow disk had
        // a reader wait for hundreds of megabytes it did not need.
        while bytes < BATCH_BYTES
            && let Ok(more) = queued.try_recv()
        {
            match more {
                Mail::Op(more) => {
                    bytes += more.op.write_len();
                    self.batch.push(Entry::new(more));
                }
                Mail::Idle(run) => self.idle.push(run),
                Mail::Left => {}
            }
        }
        let work = self
            .batch
            .iter()
            .any(|entry| !matches!(entry.op, Some(Op::Forget | Op::Release { .. })));
        let base = self.arrived;
        self.arrived += self.batch.len() as u64;
        self.turn += 1;
        // A panic in one operation must not leave the volume's callers
        // waiting for a thread that is gone.
        if catch_unwind(AssertUnwindSafe(|| self.run_batch(base))).is_err() {
            self.fail_batch();
        }
        self.batch.clear();
        let Ran {
            failures, tally, ..
        } = std::mem::take(&mut self.ran);
        let volume = &self.state.volume;
        let counts = &volume.writer;
        counts.batches.fetch_add(1, Ordering::Relaxed);
        counts
            .operations
            .fetch_add(tally.operations, Ordering::Relaxed);
        if tally.abandoned > 0 {
            counts
                .abandoned_operations
                .fetch_add(tally.abandoned, Ordering::Relaxed);
        }
        if !failures.is_empty() {
            counts
                .failed_operations
                .fetch_add(failures.len() as u64, Ordering::Relaxed);
        }
        if tally.calls > 0 {
            // The volume's write counters moved as each call returned
            // (`perform`); a sampler this wakes finds them moved.
            if let Some(rates) = &self.rates {
                rates.wake();
            }
        }
        if !failures.is_empty() {
            tell(failures);
        }
        // After the failures are told: a settle that waited for this batch finds its
        // failures already with the caller.
        self.state.returned(self.arrived);
        work
    }

    // The thread stands with nothing queued, or goes on.
    #[cfg(any(test, feature = "test-support"))]
    fn park(&self, parked: bool) {
        let mut gate = self
            .state
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        gate.parked = parked;
        self.state.gate_moved.notify_all();
    }

    fn run_batch(&mut self, base: u64) {
        self.arrange();
        let mut through = 0;
        let mut settled = 0;
        for index in 0..self.order.len() {
            let at = self.order[index].at as usize;
            #[cfg(any(test, feature = "test-support"))]
            let forced = self.pass_gate();
            #[cfg(not(any(test, feature = "test-support")))]
            let forced = None;
            let op = self.batch[at].op.take().expect("each operation runs once");
            perform(&self.state, &self.batch[at].key, op, forced, &mut self.ran);
            self.batch[at].done = true;
            self.state.done.fetch_add(1, Ordering::Relaxed);
            let key = &self.batch[at].key;
            // The first handle the key lets go in the batch holds back what
            // it returns from that operation on, and the settle from that
            // entry on, until the key's handles are dropped.
            if std::mem::take(&mut self.ran.let_go)
                && key.letting.load(Ordering::Relaxed) != self.turn
            {
                key.letting.store(self.turn, Ordering::Relaxed);
                key.held_entry.store(at as u32, Ordering::Relaxed);
                self.held.insert(at as u32);
            }
            // A caller's code ran with the key's handles dropped.
            if std::mem::take(&mut self.ran.dropped)
                && key.letting.load(Ordering::Relaxed) == self.turn
            {
                key.letting.store(0, Ordering::Relaxed);
                self.held.remove(&key.held_entry.load(Ordering::Relaxed));
            }
            // The key hears of its own operations in the order it submitted
            // them, whatever order the batch ran them in.
            let mut head = key.head.load(Ordering::Relaxed);
            if head as usize == at {
                let mut last = 0;
                while head != NONE && self.batch[head as usize].done {
                    last = self.batch[head as usize].seq;
                    head = self.batch[head as usize].next;
                }
                key.head.store(head, Ordering::Relaxed);
                if key.letting.load(Ordering::Relaxed) != self.turn {
                    key.returned(last);
                } else if head == NONE {
                    // The batch has nothing more of the key: its handles
                    // are dropped now, after a reader was told its bytes
                    // are placed.
                    key.placed(last);
                    let entry = key.held_entry.load(Ordering::Relaxed);
                    self.let_go(entry);
                    self.batch[at].key.returned(last);
                } else {
                    key.placed(last);
                    let first = self.batch[key.held_entry.load(Ordering::Relaxed) as usize].seq;
                    if first > 1 && !key.has_returned(last.min(first - 1)) {
                        key.returned(last.min(first - 1));
                    }
                }
            }
            while through < self.batch.len() && self.batch[through].done {
                through += 1;
            }
            // A settle is told as the batch goes up to the first entry held
            // back for a handle, and only while nothing of the batch failed:
            // from a failure on it is told after the report.
            let reach = self
                .held
                .first()
                .map_or(through, |held| through.min(*held as usize));
            if reach > settled && self.ran.failures.is_empty() {
                settled = reach;
                self.state.returned(base + reach as u64);
            }
        }
        // The last of a key's operations to run is the head of its chain,
        // so every key's handles were dropped as it ran.
        debug_assert!(self.held.is_empty(), "a key's handles outlived its batch");
    }

    // Drop the handles the key of batch entry `entry` let go, the first of
    // which that entry let go.
    fn let_go(&mut self, entry: u32) {
        #[cfg(any(test, feature = "test-support"))]
        self.pass_drop_gate();
        let key = &self.batch[entry as usize].key;
        key.held().drop_let_go(None, &self.state.volume.writer);
        key.letting.store(0, Ordering::Relaxed);
        self.held.remove(&entry);
    }

    // The batch panicked: every key with an operation in it has failed.
    // What was not run is not run, all of it counts as returned, and its
    // bytes stop waiting.
    fn fail_batch(&mut self) {
        unbuffer(&self.state, std::mem::take(&mut self.ran.in_hand));
        self.held.clear();
        for entry in &mut self.batch {
            if let Some(Op::Write { bytes, .. }) = entry.op.take() {
                unbuffer(&self.state, bytes.len() as u64);
            }
            if entry.key.letting.swap(0, Ordering::Relaxed) != 0 {
                entry.key.held().letting_go.clear();
            }
            if !std::mem::replace(&mut entry.done, true) {
                self.state.done.fetch_add(1, Ordering::Relaxed);
            }
            let key = &entry.key;
            if !key.failed.swap(true, Ordering::SeqCst) && !key.abandoned.load(Ordering::Acquire) {
                self.ran.failures.push((
                    key.owner.clone(),
                    Failure {
                        key: key.id,
                        file: None,
                        path: PathBuf::new(),
                        error: io::Error::other("a file operation panicked on the volume's writer"),
                        creating: false,
                    },
                ));
            }
            key.returned(entry.seq);
        }
        self.state.look_below();
    }

    // Order the batch: see `Place`. Overlapping writes of one file between
    // two of its other operations keep the order they were submitted in.
    // Each key's operations are chained in the order they came.
    fn arrange(&mut self) {
        self.order.clear();
        self.files.clear();
        let mut part = 0u32;
        let mut rank = 0u32;
        // The files of the batch a close ends, and those its first
        // operation in their part must open, by their rank.
        let mut closing = Vec::new();
        let mut opening = Vec::new();
        for index in 0..self.batch.len() {
            let at = index as u32;
            let key = &self.batch[index].key;
            let before = if key.turn.load(Ordering::Relaxed) == self.turn {
                Some(key.tail.swap(at, Ordering::Relaxed))
            } else {
                key.turn.store(self.turn, Ordering::Relaxed);
                key.head.store(at, Ordering::Relaxed);
                key.tail.store(at, Ordering::Relaxed);
                None
            };
            if let Some(before) = before {
                self.batch[before as usize].next = at;
            }
            let queued = &self.batch[index];
            let op = queued.op.as_ref().expect("a batch not yet run");
            let Some(file) = op.file() else {
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
            let of = (Arc::as_ptr(&queued.key) as usize, file);
            let entry = self.files.entry(of).or_insert((u32::MAX, 0, 0));
            if entry.0 != part {
                rank += 1;
                *entry = (part, rank, 0);
                // A write that brings the way to open its file, a truncate or
                // a close opens one the writer does not hold. A kept closed
                // file the batch uses is in use from here: a close placed
                // ahead of its use must not let it go at the bound.
                let opens = match op {
                    Op::Write { create, .. } => {
                        let held = queued.key.held().in_use(file);
                        create.is_some() && !held
                    }
                    Op::Truncate { .. } | Op::Close { .. } => !queued.key.held().in_use(file),
                    _ => false,
                };
                if opens {
                    opening.push(rank);
                }
            }
            // A write that opens the file runs before the writes after it,
            // which carry no way to open it.
            let alone = !matches!(op, Op::Write { create: None, .. });
            if matches!(op, Op::Close { .. }) {
                closing.push(entry.1);
            }
            let offset = match op {
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
        // Between two operations that may touch more than one file, the files
        // a close ends run first, then the files already open, then those
        // that must be opened, each in the order it first came: a reader of
        // a closed file, a check or a repair, is told its bytes and length
        // are placed without standing behind another file's open or writes,
        // and a file already open is not held behind another's open, which
        // on some systems stands behind what the file system writes out.
        // The files of one part touch nothing of each other, so any order of
        // them is as sound as the one they came in. A key still hears of its
        // operations in the order it sent them (`run_batch`).
        if !closing.is_empty() || !opening.is_empty() {
            const OPEN: u32 = 1 << 30;
            const OPENING: u32 = 1 << 31;
            let mut tier = vec![OPEN; rank as usize + 1];
            for rank in opening {
                tier[rank as usize] = OPENING;
            }
            for rank in closing {
                tier[rank as usize] = 0;
            }
            for place in &mut self.order {
                if place.file != 0 {
                    place.file |= tier[place.file as usize];
                }
            }
        }
        self.order.sort_unstable();
        let mut start = 0;
        while start < self.order.len() {
            let head = self.order[start];
            let mut end = start + 1;
            while end < self.order.len()
                && (
                    self.order[end].part,
                    self.order[end].file,
                    self.order[end].run,
                ) == (head.part, head.file, head.run)
            {
                end += 1;
            }
            let mut reached = 0u64;
            let mut overlaps = false;
            for place in &self.order[start..end] {
                if let Some(Op::Write { offset, bytes, .. }) = &self.batch[place.at as usize].op {
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

    #[cfg(any(test, feature = "test-support"))]
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

    // A test holds the thread before it drops the handles a batch let go.
    #[cfg(any(test, feature = "test-support"))]
    fn pass_drop_gate(&self) {
        let mut gate = self
            .state
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while gate.drops_held {
            gate.dropping = true;
            self.state.gate_moved.notify_all();
            gate = self
                .state
                .gate_moved
                .wait(gate)
                .unwrap_or_else(PoisonError::into_inner);
        }
        gate.dropping = false;
    }
}

impl Entry {
    fn new(queued: Queued) -> Self {
        Self {
            seq: queued.seq,
            key: queued.key,
            op: Some(queued.op),
            done: false,
            next: NONE,
        }
    }
}

// Run one operation of `key` to its end.
fn perform(state: &LaneState, key: &Key, op: Op, forced: Option<io::Error>, ran: &mut Ran) {
    let Ran {
        failures,
        tally,
        in_hand,
        wrote_at,
        let_go,
        dropped,
    } = ran;
    // Only a write call's return marks the clock for the next one.
    let since = wrote_at.take();
    *in_hand = match &op {
        Op::Write { bytes, .. } => bytes.len() as u64,
        _ => 0,
    };
    let skip = key.stopped();
    let file = op.file();
    match &op {
        Op::Forget | Op::Release { .. } => {}
        _ if skip => tally.abandoned += 1,
        _ => tally.operations += 1,
    }
    let mut creating = false;
    let mut failed_at = PathBuf::new();
    // A caller's own code runs with the key's files let go.
    let mut after = None;
    let mut held = key.held();
    let mut result = match op {
        Op::Forget => {
            let open = std::mem::take(&mut held.open);
            let mut letting_go = std::mem::take(&mut held.letting_go);
            *let_go = !open.is_empty();
            letting_go.extend(open.into_values());
            *held = Held {
                letting_go,
                ..Held::default()
            };
            Ok(())
        }
        Op::Release { file } => {
            *let_go = held.let_go(file);
            Ok(())
        }
        // A caller's code not run is dropped as if it had run: whatever it
        // would have moved or removed, its caller may now move or remove
        // on a thread of its own, so the key's handles are dropped first.
        Op::Run { file, .. } if skip => {
            let open = file.and_then(|file| held.take(file));
            held.drop_let_go(open, &state.volume.writer);
            *dropped = true;
            Ok(())
        }
        _ if skip => {
            if let Some(file) = file {
                *let_go = held.let_go(file);
            }
            Ok(())
        }
        // A test's forced failure names the file it stood before: by the
        // path the operation carries, or the one its file was opened at.
        _ if forced.is_some() => {
            failed_at = op.path();
            if failed_at.as_os_str().is_empty()
                && let Some(open) = file.and_then(|file| held.open.get(&file))
            {
                failed_at.clone_from(&open.path);
            }
            Err(forced.expect("checked above"))
        }
        Op::Write {
            file,
            offset,
            bytes,
            create,
        } => {
            if let Some(create) = &create {
                failed_at.clone_from(&create.path);
            }
            // An open made for this write is not write time: the span
            // starts again after it.
            let was_open = held.open.contains_key(&file);
            match held.opened(file, create.as_deref(), &mut creating, &state.volume.writer) {
                Ok(Some(open)) => {
                    tally.calls += 1;
                    let seek = open.end.is_some_and(|end| end != offset);
                    // A span chained from the last write's return holds
                    // the few loads and stores between two calls; work past
                    // that since (`break_span`) restarts it here.
                    let broken = SPAN_BROKEN.replace(false);
                    let started = since.filter(|_| was_open && !broken).unwrap_or_else(now);
                    let wrote = write_all_at(&open.file, offset, &bytes);
                    let returned = now();
                    if wrote.is_err() {
                        failed_at.clone_from(&open.path);
                    }
                    // Counted as the call returns, not when the batch ends:
                    // a batch can take seconds on a slow disk, and the rate
                    // read from these must see each write in the span it
                    // was made in. The time first and the bytes last,
                    // released: whoever reads the bytes first never sees
                    // bytes without their time.
                    let volume = &state.volume;
                    volume.write_nanos.fetch_add(
                        returned.duration_since(started).as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                    volume.write_calls.fetch_add(1, Ordering::Relaxed);
                    if seek {
                        volume.write_seeks.fetch_add(1, Ordering::Relaxed);
                    }
                    if wrote.is_ok() {
                        volume
                            .written_bytes
                            .fetch_add(bytes.len() as u64, Ordering::Release);
                    }
                    *wrote_at = Some(returned);
                    let end = offset + bytes.len() as u64;
                    open.end = Some(end);
                    open.length = open
                        .length
                        .filter(|_| wrote.is_ok())
                        .map(|length| length.max(end));
                    if wrote.is_ok() {
                        write_out_due(volume, open, bytes.len() as u64);
                    }
                    // Its count fell as the call returned (`in_hand`). The
                    // last share of the buffer is freed here, as soon as it
                    // is written: the memory goes back while it is still
                    // warm, and the next buffer is filled from it.
                    if bytes.is_unique() {
                        break_span();
                    }
                    drop(bytes);
                    wrote
                }
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            }
        }
        Op::Truncate { file, create } => {
            failed_at.clone_from(&create.path);
            if !held.open.contains_key(&file) && !create.path.try_exists().unwrap_or(true) {
                Ok(())
            } else {
                let counts = &state.volume.writer;
                held.opened(file, Some(&create), &mut creating, counts)
                    .and_then(|open| open.map_or(Ok(()), |open| set_length(open, 0, counts)))
            }
        }
        // A file already at its final length, sized when it was made and
        // written to its end or grown by its writes to it, is not set again:
        // that is no change, and on some systems the call stands behind
        // what the file system is writing out of the file.
        Op::Close {
            file,
            length,
            create,
        } => {
            failed_at.clone_from(&create.path);
            let counts = &state.volume.writer;
            let closed = held
                .opened(file, Some(&create), &mut creating, counts)
                .and_then(|open| match open {
                    None => Ok(()),
                    Some(open) if open.length == Some(length) => {
                        counts.length_sets_skipped.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    }
                    Some(open) => set_length(open, length, counts),
                });
            // The handle stays until something needs it gone: a release, a
            // move or removal of the file, the key's end, or the bound on
            // the closed files it keeps. A reader opens a handle of its own.
            *let_go = if closed.is_ok() {
                held.keep_closed(file)
            } else {
                held.let_go(file)
            };
            closed
        }
        Op::Run {
            file, path, run, ..
        } => {
            // Its code may move or remove what a handle the key let go
            // still holds open.
            let open = file.and_then(|file| held.take(file));
            held.drop_let_go(open, &state.volume.writer);
            *dropped = true;
            failed_at = path;
            after = Some(run);
            Ok(())
        }
    };
    drop(held);
    if let Some(run) = after {
        result = run();
    }
    unbuffer(state, std::mem::take(in_hand));
    if let Err(error) = result {
        // A name the file system will not take is a refusal of that one
        // file: the caller decides what follows. Anything else fails
        // the key, and what is queued behind it under the key is not
        // run.
        let refusal = creating && error.kind() == io::ErrorKind::InvalidFilename;
        if refusal {
            if let Some(file) = file {
                key.held().refused.insert(file);
            }
            key.unheard.fetch_add(1, Ordering::AcqRel);
        } else {
            key.failed.store(true, Ordering::SeqCst);
            state.look_below();
        }
        failures.push((
            key.owner.clone(),
            Failure {
                key: key.id,
                file,
                path: failed_at,
                error,
                creating,
            },
        ));
    }
}

// Tell each owner its keys' failures from a batch, in the order they came,
// then `room` once to each owner told, in the order first told.
#[cold]
fn tell(failures: Vec<(Owner, Failure)>) {
    let mut told: Vec<Owner> = Vec::new();
    for (owner, failure) in failures {
        owner.owner.failed(failure);
        if !told.iter().any(|seen| seen.bit == owner.bit) {
            told.push(owner);
        }
    }
    for owner in told {
        owner.owner.room();
    }
}

#[cfg(test)]
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

#[cfg(any(test, feature = "test-support"))]
#[derive(Default)]
struct Gate {
    held: bool,
    permits: usize,
    // The thread stands before an operation with no permit for it.
    waiting: bool,
    // The thread stands with nothing queued, its own work looked at.
    parked: bool,
    fail_next: Option<io::Error>,
    // The thread may not drop the handles a batch let go, and stands
    // before doing so.
    drops_held: bool,
    dropping: bool,
}

// A test's hold on a volume's thread.
#[cfg(any(test, feature = "test-support"))]
pub struct Hold(Arc<Lane>);

#[cfg(any(test, feature = "test-support"))]
impl Hold {
    fn gate(&self) -> std::sync::MutexGuard<'_, Gate> {
        self.0
            .state
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    // Let the next `count` operations through.
    pub fn release(&self, count: usize) {
        self.gate().permits += count;
        self.0.state.gate_moved.notify_all();
    }

    // Let everything through from now on.
    pub fn open(&self) {
        self.gate().held = false;
        self.0.state.gate_moved.notify_all();
    }

    // The next operation let through fails with `error` instead of running.
    pub fn fail_next(&self, error: io::Error) {
        self.gate().fail_next = Some(error);
    }

    // Hold the thread before it drops the handles a batch let go, or let
    // it drop them.
    pub(crate) fn hold_drops(&self, held: bool) {
        self.gate().drops_held = held;
        self.0.state.gate_moved.notify_all();
    }

    // Wait until the thread stands before dropping handles, or before an
    // operation it may not run yet. Whether it stands before the drop.
    #[cfg(test)]
    pub(crate) fn wait_held_or_dropping(&self) -> bool {
        let mut gate = self.gate();
        while !gate.dropping && !(gate.waiting && gate.permits == 0) {
            gate = self
                .0
                .state
                .gate_moved
                .wait(gate)
                .unwrap_or_else(PoisonError::into_inner);
        }
        gate.dropping
    }

    // Wait until the thread stands before dropping handles.
    #[cfg(test)]
    pub(crate) fn wait_dropping(&self) {
        let mut gate = self.gate();
        while !gate.dropping {
            gate = self
                .0
                .state
                .gate_moved
                .wait(gate)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    // Wait until `count` threads stand in `Submitter::wait_below` on the
    // volume.
    pub fn wait_below_waiters(&self, count: usize) {
        let state = &self.0.state;
        let mut held = state
            .volume
            .below_waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while state.volume.below_waiters.load(Ordering::SeqCst) < count {
            held = state
                .volume
                .below
                .wait(held)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    // Wait until a settle stands still for the volume.
    pub fn wait_settling(&self) {
        let state = &self.0.state;
        let mut held = state.waiting.lock().unwrap_or_else(PoisonError::into_inner);
        while state.waiters.load(Ordering::SeqCst) == 0 {
            held = state
                .woke
                .wait(held)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    // Wait until everything submitted to the volume so far has returned
    // and the thread stands with nothing queued: it has looked at the
    // volume's own work since, and either ran it or kept it.
    #[cfg(test)]
    pub(crate) fn wait_parked(&self) {
        let state = &self.0.state;
        let sent = state.sent.load(Ordering::SeqCst);
        let mut gate = self.gate();
        while !(gate.parked && state.returned.load(Ordering::SeqCst) >= sent) {
            gate = state
                .gate_moved
                .wait(gate)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    // Wait until the thread stands before an operation it may not run yet.
    pub fn wait_held(&self) {
        let mut gate = self.gate();
        while !(gate.waiting && gate.permits == 0) {
            gate = self
                .0
                .state
                .gate_moved
                .wait(gate)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for Hold {
    fn drop(&mut self) {
        self.hold_drops(false);
        self.0.leave();
    }
}

#[cfg(test)]
mod tests;
