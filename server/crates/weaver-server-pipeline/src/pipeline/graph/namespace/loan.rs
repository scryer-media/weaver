// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A recovery set's repair, lent off the loop. The set's object leaves its
// node, and the access it repairs through takes along what its reads reach:
// for every member of the namespaces below the set, as far down as the tree
// goes, its coverage, the file it is written at and where the bytes still
// held for it are, read there as the repair asks for them; for every data
// object there, what rebuilds its posted bytes; and the
// envelope shelves those objects keep, read as they stand. Nothing the loop
// changes after the loan reaches it, and what the loop moved that it read is
// known on its return: a shelf whose generation moved, or a file no longer
// where it was. The repaired ranges, the ranges found bad and the object come
// back together, and the namespace applies them as one repair. What the
// repair rebuilt waits on disk until then, never in memory.

pub mod replay;
pub mod rewalk;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use weaver_server_disk::operations::disk::volume::LoopSite;

use bytes::Bytes;
use weaver_graph::holds::Location;
use weaver_graph::recovery::{MemberAccess, RepairDiagnostic, RepairOutcome};
use weaver_graph::{
    ByteRange, Coverage, MemberId, NodeId, Object, Placement, Posting, Provenance, Tree,
    VolumeIndex,
};

use weaver_server_disk::operations::disk::reader::{Reading, TurnStop};
use weaver_server_disk::operations::disk::volume::Volume;

use super::super::envelope::Shelf;
use super::super::hardware::Shutdown;
use super::super::leaf::{Leaf, LeafError, read_exact_at};
use super::{ALL, NamespaceWriter, READ_BACK, SetState, Source, held_volume, index, read_member};

// A set's repair, ready to run anywhere.
pub struct Loan {
    pub(crate) set: NodeId,
    // The namespace the set covers, where the repair writes.
    pub(crate) namespace: NodeId,
    // What the set said of itself as it left: what it says while it is out.
    pub(crate) state: Option<SetState>,
    object: Box<dyn Object>,
    access: Lent,
}

// What a repair came back with.
pub struct Returned {
    pub set: NodeId,
    pub(crate) namespace: NodeId,
    pub(crate) object: Box<dyn Object>,
    pub outcome: Option<RepairOutcome>,
    pub(crate) reads: u64,
    pub(crate) repaired: Repaired,
    // Ranges the object proved bad and could not rebuild.
    pub(crate) found: Vec<(MemberId, ByteRange)>,
    pub diagnostics: Vec<RepairDiagnostic>,
    read: Read,
    inputs: Vec<Input>,
}

struct Input {
    node: NodeId,
    member: MemberId,
    length: Option<u64>,
    coverage: Coverage,
    bound: Option<(NodeId, VolumeIndex)>,
    // The ranges the repair read of the member: the only bytes of it its
    // ruling stands on.
    read: Coverage,
    held_basis: Vec<(ByteRange, u64)>,
    // The member's file, at the path and revision the repair found it.
    leaf: Option<(PathBuf, (u64, u64))>,
}

// The held revisions of a member within `range`, as `Holds::revisions`
// answers them. `held` is in offset order, the holds never overlapping, as
// `Holds::revisions` gives them, so only those `range` reaches are visited.
fn clipped(held: &[(ByteRange, u64)], range: ByteRange) -> Vec<(ByteRange, u64)> {
    let from = held.partition_point(|(held, _)| held.end <= range.start);
    held[from..]
        .iter()
        .take_while(|(held, _)| held.start < range.end)
        .filter_map(|(held, age)| Some((held.intersect(range)?, *age)))
        .collect()
}

// Whether two coverages say the same of `range`: the same bytes, with the
// same provenance.
fn same_runs(now: &Coverage, then: &Coverage, range: ByteRange) -> bool {
    let within = |coverage: &Coverage| {
        coverage
            .overlapping(range)
            .filter_map(|run| Some((run.range.intersect(range)?, run.provenance)))
            .collect::<Vec<_>>()
    };
    within(now) == within(then)
}

impl std::fmt::Debug for Returned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Returned")
            .field("set", &self.set)
            .field("namespace", &self.namespace)
            .field("outcome", &self.outcome)
            .field("reads", &self.reads)
            .field("repaired", &self.repaired.runs.len())
            .field("found", &self.found)
            .finish_non_exhaustive()
    }
}

// What the repair read that the loop may move: every shelf with the
// generation it stood at, and whether the pipeline began shutting down
// before the repair was done, so that what it read may have been cut short.
struct Read {
    shelves: Vec<(Arc<Shelf>, u64)>,
    stopped: bool,
}

impl Returned {
    // The shutdown stopped the repair: it is not repaired, and nothing it
    // came to stands.
    pub fn stopped(&self) -> bool {
        self.read.stopped
    }
}

impl Loan {
    // The loan's stop, which its job raises to end it.
    pub(crate) fn stop(&self) -> Arc<TurnStop> {
        self.access.stop()
    }

    // The members the repair is to write nothing into: the disk has no room
    // for them.
    pub(super) fn withhold(&mut self, members: BTreeSet<MemberId>) {
        self.access.withheld = members;
    }

    // Run the repair, where it is not in anyone's way, within `budget`. A
    // repair given after `shutdown` started, or after the job's working
    // directory went, reads nothing, and one going when either happens
    // reads nothing more: each comes back stopped.
    pub fn run(mut self, budget: weaver_graph::Budget, shutdown: &Shutdown) -> Returned {
        let (_running, running) = Running::enter();
        self.access.begin(shutdown);
        let given = !self.access.stopped();
        let outcome = self
            .object
            .recovery_mut()
            .filter(|_| given)
            .map(|recovery| recovery.repair(&mut self.access, budget));
        self.access.let_go();
        self.access.report_end(running);
        let stopped = self.access.stopped();
        let access = self.access;
        if let Some(dir) = &access.scratch {
            super::remove_scratch(dir);
        }
        let mut usage = access.usage.into_inner();
        let inputs = access
            .members
            .into_iter()
            .filter_map(|((node, member), taken)| {
                Some(Input {
                    node,
                    member,
                    length: taken.length,
                    coverage: taken.coverage,
                    bound: taken.bound,
                    read: usage.members.remove(&(node, member))?,
                    held_basis: taken.held_basis,
                    leaf: taken.path.zip(taken.revision),
                })
            })
            .collect();
        Returned {
            set: self.set,
            namespace: self.namespace,
            object: self.object,
            outcome,
            reads: access.reads,
            repaired: access.repaired,
            found: access.found,
            diagnostics: access.diagnostics,
            inputs,
            read: Read {
                shelves: access
                    .shelves
                    .into_iter()
                    .filter(|(node, _)| usage.shelves.contains(node))
                    .map(|(_, shelf)| shelf)
                    .collect(),
                stopped,
            },
        }
    }
}

// One member as the repair found it.
pub struct Taken {
    coverage: Coverage,
    length: Option<u64>,
    path: Option<PathBuf>,
    revision: Option<(u64, u64)>,
    scratch_revision: Option<(u64, u64)>,
    held_basis: Vec<(ByteRange, u64)>,
    // Where the bytes held for it are, in offset order.
    held: Vec<(ByteRange, Location)>,
    // Its spill file, for the bytes held past the budgets, and the revision
    // that file stood at when lent.
    spill: Option<File>,
    // The spill file to open once the loan's thread has waited for it.
    spill_later: Option<PathBuf>,
    spill_revision: Option<(u64, u64)>,
    bound: Option<(NodeId, VolumeIndex)>,
    // The volume the member's file lies on.
    volume: Option<Arc<Volume>>,
}

#[derive(Default)]
pub struct Usage {
    // Each member the repair asked of, and the ranges of it it read: none
    // for one it only asked the length or binding of.
    members: BTreeMap<(NodeId, MemberId), Coverage>,
    shelves: BTreeSet<NodeId>,
}

impl Lent {
    fn note(&self, at: (NodeId, MemberId), read: Option<ByteRange>) {
        if self.defer_holds {
            return;
        }
        let mut usage = self.usage.borrow_mut();
        let ranges = usage.members.entry(at).or_default();
        if let Some(range) = read {
            ranges.write(range, Provenance::Fresh);
        }
    }

    // A cached ruling still depends on its covered inputs even if the
    // worker need not read their bytes again: on their length and where
    // they are bound. Bytes said of them while it is out reach the set
    // through `feed` when it is back.
    fn require(&self, at: (NodeId, MemberId)) {
        self.note(at, None);
    }

    // The pipeline began shutting down, or the job's working directory was
    // released: the repair reads and writes nothing more.
    pub fn stopped(&self) -> bool {
        self.stop.raised() || self.shutdown.started() || self.unwritten || !self.internal.is_dir()
    }

    // The loan's thread begins: it waits for what was still queued for the
    // job's files when the loan was made, and opens what the loop left for
    // it to open.
    fn begin(&mut self, shutdown: &Shutdown) {
        self.shutdown = shutdown.clone();
        // The repair's timing is gathered only for its debug line.
        let on = tracing::enabled!(tracing::Level::DEBUG);
        let timing = self.timing.get_mut();
        timing.on = on;
        if on {
            timing.ran = Some(Instant::now());
        }
        let Some(settled) = self.settled.take() else {
            return;
        };
        let began = on.then(Instant::now);
        settled.wait_aside();
        if let Some(began) = began {
            self.timing.get_mut().settled = began.elapsed();
        }
        // A write of the job's failed: its files are not as the loop said
        // them, so the loan reads nothing. The failure fails the job.
        if settled.unwritten() {
            self.unwritten = true;
            return;
        }
        let began = on.then(Instant::now);
        for (node, path) in std::mem::take(&mut self.scratch_later) {
            if let Ok(file) = File::open(path) {
                self.held_scratch.insert(node, file);
            }
        }
        for taken in self.members.values_mut() {
            if let Some(path) = taken.spill_later.take() {
                taken.spill = File::open(path).ok();
            }
        }
        if let Some(began) = began {
            self.timing.get_mut().opened_at_begin = began.elapsed();
        }
    }
}

// The owned access a lent repair reads and writes through.
pub struct Lent {
    pub namespace: NodeId,
    pub set: NodeId,
    pub recovery: Option<NodeId>,
    // Replay validates every node's revisions on return, so it notes
    // nothing of what it reads.
    pub defer_holds: bool,
    // Bytes held on scratch are read in place, through the loan's own
    // handle on the namespace's scratch file, as the work asks for them. A
    // span is written again only once every hold on scratch has left, which
    // the hold revisions show. A repair's return checks the revisions of
    // what it read. A rewalk's pieces are heard as read, so it notes here
    // the held ranges each piece found on scratch, and the loop checks
    // their revisions as it hears the piece (`scratch_stands`).
    pub scratch_read: Option<RefCell<Vec<(NodeId, MemberId, ByteRange)>>>,
    // Each namespace's scratch file, opened when lent for a member that
    // holds bytes there.
    pub held_scratch: BTreeMap<NodeId, File>,
    // Everything the job had submitted for its files when the loan was
    // made, when some of it had yet to return, and the scratch files to
    // open once it has. The loan waits for it on its own thread as it
    // begins, never the loop: the files then lie as the loop said them.
    pub settled: Option<crate::pipeline::graph::disk::JobBarrier>,
    pub scratch_later: BTreeMap<NodeId, PathBuf>,
    // What the loan waited for did not all reach the disk: it reads and
    // writes nothing, as one stopped.
    pub unwritten: bool,
    // The directory the repair's scratch and output lie in, made when the
    // repair is lent. The repair never makes it again: once it is gone, the
    // job's working directory was released under the repair.
    pub internal: PathBuf,
    pub members: BTreeMap<(NodeId, MemberId), Taken>,
    // Members lifted out of their namespace, and where each lives now.
    pub homes: BTreeMap<(NodeId, MemberId), (NodeId, MemberId)>,
    pub usage: RefCell<Usage>,
    pub postings: BTreeMap<NodeId, Box<dyn Posting>>,
    pub shelves: BTreeMap<NodeId, (Arc<Shelf>, u64)>,
    // The set's own volumes: the member each is, in the namespace it was
    // posted in, and the range of it a window is.
    pub volumes: BTreeMap<VolumeIndex, (NodeId, MemberId)>,
    pub window: Option<ByteRange>,
    // The last file read, kept open: a repair reads a member slice after
    // slice.
    pub open: RefCell<Option<((NodeId, MemberId), File)>>,
    pub reads: u64,
    pub repaired: Repaired,
    pub found: Vec<(MemberId, ByteRange)>,
    pub diagnostics: Vec<RepairDiagnostic>,
    pub scratch: Option<PathBuf>,
    // The pipeline's shutdown: once it starts, no read is answered.
    pub shutdown: Shutdown,
    // The members the disk has no room for the repair to rebuild.
    pub withheld: BTreeSet<MemberId>,
    // The volume of the job's working directory, where its scratch and
    // spill files lie, and the read turn the loan holds (`Reading`): every
    // read of a file is made in a turn of the file's volume.
    pub working: Option<Arc<Volume>>,
    pub reading: RefCell<Reading>,
    // Raised when the job stops the loan (`Loans::request_stop`): a wait for
    // a read turn ends, and the loan reads nothing more and comes back
    // stopped, as it does once the shutdown starts.
    pub stop: Arc<TurnStop>,
    // What the repair's steps cost, for its one debug line as it ends.
    pub timing: RefCell<Timing>,
}

// What a repair's loan cost, gathered only while its debug line is on and
// said once, as the repair ends: the time from the loan's making to its
// thread, the wait for the job's writes, the files opened as it began, the
// engine's own steps to its plan, each file it opened and read, its held
// reads, and every read and repaired write it asked, before its plan and
// after, with where the process stood at the plan and at the end.
pub struct Timing {
    on: bool,
    lent: Instant,
    ran: Option<Instant>,
    settled: Duration,
    opened_at_begin: Duration,
    // The engine's steps to its plan, by name, as it told them.
    steps: Vec<(&'static str, Duration)>,
    // Each file the loan opened and read, in the order first opened, found
    // by the member it is; the one read last is at hand without a lookup.
    files: Vec<FileTiming>,
    file_at: BTreeMap<(NodeId, MemberId), usize>,
    current: Option<((NodeId, MemberId), usize)>,
    held: Duration,
    held_bytes: u64,
    asked: Asked,
    // Where the process stood at the repair's plan: what the repair cost
    // after it is the difference from there to its end.
    plan: Option<Mark>,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            on: false,
            lent: Instant::now(),
            ran: None,
            settled: Duration::ZERO,
            opened_at_begin: Duration::ZERO,
            steps: Vec::new(),
            files: Vec::new(),
            file_at: BTreeMap::new(),
            current: None,
            held: Duration::ZERO,
            held_bytes: 0,
            asked: Asked::default(),
            plan: None,
        }
    }
}

impl Timing {
    // The timing of the file the member is, the one read last at hand.
    fn file(&mut self, at: (NodeId, MemberId), path: &std::path::Path) -> &mut FileTiming {
        let index = match self.current {
            Some((held, index)) if held == at => index,
            _ => {
                let files = &mut self.files;
                let index = *self.file_at.entry(at).or_insert_with(|| {
                    files.push(FileTiming::named(path));
                    files.len() - 1
                });
                self.current = Some((at, index));
                index
            }
        };
        &mut self.files[index]
    }
}

// One file the loan opened and read: how often, how long, how much.
struct FileTiming {
    name: String,
    opens: u32,
    open: Duration,
    reads: u64,
    bytes: u64,
    read: Duration,
}

impl FileTiming {
    fn named(path: &std::path::Path) -> Self {
        Self {
            name: path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
            opens: 0,
            open: Duration::ZERO,
            reads: 0,
            bytes: 0,
            read: Duration::ZERO,
        }
    }
}

// Every read the repair asked and every repaired write it made, so far,
// with how long each took on this thread, and the files its reads opened.
#[derive(Clone, Copy, Default)]
struct Asked {
    reads: u64,
    read_bytes: u64,
    read: Duration,
    writes: u64,
    write_bytes: u64,
    write: Duration,
    opens: u64,
    open: Duration,
}

impl Asked {
    fn since(self, earlier: Self) -> Self {
        Self {
            reads: self.reads.saturating_sub(earlier.reads),
            read_bytes: self.read_bytes.saturating_sub(earlier.read_bytes),
            read: self.read.saturating_sub(earlier.read),
            writes: self.writes.saturating_sub(earlier.writes),
            write_bytes: self.write_bytes.saturating_sub(earlier.write_bytes),
            write: self.write.saturating_sub(earlier.write),
            opens: self.opens.saturating_sub(earlier.opens),
            open: self.open.saturating_sub(earlier.open),
        }
    }

    fn said(self) -> String {
        format!(
            "reads={} read_mb={} read_ms={:.1} opens={} open_ms={:.1} writes={} write_mb={} write_ms={:.1}",
            self.reads,
            self.read_bytes / MIB,
            ms(self.read),
            self.opens,
            ms(self.open),
            self.writes,
            self.write_bytes / MIB,
            ms(self.write),
        )
    }
}

const MIB: u64 = 1024 * 1024;

// The process at one moment of a repair: its CPU, user and kernel apart,
// the page faults it has taken, the memory the system has free, and what
// the repair had asked by then. Taken twice per repair, at its plan and at
// its end, on the repair's own thread.
#[derive(Clone, Copy)]
struct Mark {
    at: Instant,
    counters: Option<weaver_server_base::runtime::perf_probe::ProcessCounters>,
    available: Option<u64>,
    asked: Asked,
}

impl Mark {
    fn now(asked: Asked) -> Self {
        Self {
            at: Instant::now(),
            counters: weaver_server_base::runtime::perf_probe::process_counters(),
            available: weaver_server_disk::runtime::system_probe::available_memory_bytes(),
            asked,
        }
    }
}

// Repairs running in this process now, this one among them.
static RUNNING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Running;

impl Running {
    fn enter() -> (Self, u64) {
        let now = RUNNING.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        (Self, now)
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        RUNNING.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

// The files the line names, the slowest first: no more than this many.
const FILES_SAID: usize = 8;

// What a read the loan was stopped before gives.
fn stopped_read() -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "the loan was stopped",
    ))
}

impl Lent {
    pub fn stop(&self) -> Arc<TurnStop> {
        Arc::clone(&self.stop)
    }

    // `read` in a turn of `volume`, when the file read is written through
    // one, or `stopped` when the loan is stopped while it waits for the turn.
    pub fn in_turn<R>(
        &self,
        volume: Option<&Arc<Volume>>,
        stopped: impl FnOnce() -> R,
        read: impl FnOnce() -> R,
    ) -> R {
        match volume {
            Some(volume) => self
                .reading
                .borrow_mut()
                .read_unless(volume.read_turns(), &self.stop, read)
                .unwrap_or_else(stopped),
            None => read(),
        }
    }

    // The loan waits on something other than the disk, or is done reading:
    // its turn goes back.
    pub(super) fn let_go(&self) {
        self.reading.borrow_mut().let_go();
    }
}

// What a repair rewrote: runs of members in the order written, each one's
// bytes on the repair's output file, so a repair never holds what it
// rebuilt in memory however much that is. The file lies beside the
// repair's scratch directory, whose names are the object's own, and goes
// once the namespace has applied the runs, or turned them away. A write
// the file refuses fails the repair: nothing it rebuilt is applied.
pub struct Repaired {
    // The member, where in it the run goes, and where on the output it is.
    runs: Vec<(MemberId, u64, ByteRange)>,
    pub output: Leaf,
    // How far the output file reaches: the next run is written there.
    written: u64,
    // The first write the output refused. Nothing is written after it.
    failed: Option<LeafError>,
    // What the repair said it writes in all, here and in its scratch
    // directory, when it runs.
    writes: u64,
    // What a repair that failed for want of room needs free to run again:
    // all it said it writes, since it is run again from its start with its
    // output and its scratch directory gone. Never what it had written.
    gave_back: u64,
}

impl std::fmt::Debug for Repaired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Repaired")
            .field("runs", &self.runs.len())
            .field("written", &self.written)
            .finish_non_exhaustive()
    }
}

impl Repaired {
    fn new(
        working_dir: &std::path::Path,
        namespace: NodeId,
        set: NodeId,
    ) -> Result<Self, LeafError> {
        let name = format!(
            "{}/graph-recovery-{}-{}.repaired",
            weaver_graph::names::INTERNAL_DIR,
            namespace.0,
            set.0
        );
        Ok(Self {
            runs: Vec::new(),
            output: Leaf::at(working_dir, &name),
            written: 0,
            failed: None,
            writes: 0,
            gave_back: 0,
        })
    }

    // The repair says how much it writes in all when it runs.
    pub fn will_write(&mut self, bytes: u64) {
        self.writes = self.writes.saturating_add(bytes);
    }

    // What an object asked for again, read back and staged here until the
    // tree has uncovered it all, so a whole member said again is never in
    // memory at once. Each request gets its own file: one the replay
    // raises while an earlier one is being said again is staged apart.
    pub fn again(working_dir: &std::path::Path, node: NodeId) -> Result<Self, LeafError> {
        let internal = working_dir.join(weaver_graph::names::INTERNAL_DIR);
        std::fs::create_dir_all(&internal).map_err(|source| LeafError::Io {
            device: crate::pipeline::graph::limits::Device::Working,
            path: internal.clone(),
            source,
        })?;
        static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let serial = SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let name = format!(
            "{}/graph-again-{}-{serial}.repaired",
            weaver_graph::names::INTERNAL_DIR,
            node.0,
        );
        Ok(Self {
            runs: Vec::new(),
            output: Leaf::at(working_dir, &name),
            written: 0,
            failed: None,
            writes: 0,
            gave_back: 0,
        })
    }

    pub fn write(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        if self.failed.is_some() {
            return;
        }
        let at = ByteRange {
            start: self.written,
            end: self.written + bytes.len() as u64,
        };
        let written = self
            .output
            .create_within()
            .and_then(|()| self.output.write(LoopSite::Repair, at.start, bytes, None));
        match written {
            Ok(()) => {
                self.written = at.end;
                self.runs.push((member, offset, at));
            }
            // Nothing staged is applied once a write is refused, so what
            // the output holds goes now, not when the return is heard. A
            // file that will not go is tried again then.
            Err(error) => {
                self.output.release_handle();
                let _ = std::fs::remove_file(self.output.path());
                self.gave_back = self.writes;
                self.failed = Some(error);
            }
        }
    }

    // The repair's scratch directory, which lies beside the output on the
    // working disk, refused a write for want of room. The repair failed with
    // it as it does when the output refuses one: nothing staged is applied.
    pub fn refuse(&mut self, path: &std::path::Path, source: std::io::Error) {
        if self.failed.is_some() {
            return;
        }
        self.gave_back = self.writes;
        self.output.release_handle();
        let _ = std::fs::remove_file(self.output.path());
        self.failed = Some(LeafError::Io {
            device: crate::pipeline::graph::limits::Device::Working,
            path: path.to_path_buf(),
            source,
        });
    }

    // The write the output refused, if one did: the repair failed with it.
    pub fn failed(&mut self) -> Option<LeafError> {
        self.failed.take()
    }

    // What a repair that failed needs free to run again; nothing when it
    // did not say what it writes, and the job then waits for more room than
    // the disk showed.
    pub fn gave_back(&self) -> u64 {
        self.gave_back
    }

    // What the repair said it writes in all.
    pub(crate) fn writes(&self) -> u64 {
        self.writes
    }

    // Each run's member and how many bytes it holds, in the order written.
    pub(crate) fn sizes(&self) -> impl Iterator<Item = (MemberId, u64)> + '_ {
        self.runs.iter().map(|(member, _, at)| (*member, at.len()))
    }

    pub(crate) fn members(&self) -> impl Iterator<Item = MemberId> + '_ {
        self.runs.iter().map(|(member, _, _)| *member)
    }

    // Every run in the order written, a slice at a time, so no more than a
    // slice of it is in memory at once.
    pub(crate) fn slices(
        &self,
    ) -> impl Iterator<Item = (MemberId, u64, Result<Bytes, LeafError>)> + '_ {
        self.runs.iter().flat_map(move |(member, offset, at)| {
            (0..at.len().div_ceil(READ_BACK)).map(move |slice| {
                let from = slice * READ_BACK;
                let to = at.len().min(from + READ_BACK);
                let mut bytes = Vec::new();
                let read = self
                    .output
                    .read(
                        LoopSite::Repair,
                        at.start + from,
                        index(to - from),
                        &mut bytes,
                    )
                    .map(|()| Bytes::from(bytes));
                (*member, offset + from, read)
            })
        })
    }

    // The output goes with the runs.
    pub(crate) fn remove(self) -> Result<(), LeafError> {
        self.output.remove()
    }

    // As `remove`, for a staging nothing may have been written to: a
    // request whose ranges read back nothing has no file.
    pub(crate) fn discard(self) -> Result<(), LeafError> {
        if !self.output.path().exists() {
            return Ok(());
        }
        self.output.remove()
    }
}

impl NamespaceWriter {
    // Lend the set's object out of the tree with everything its repair
    // reads. `None` when the node holds no recovery object.
    pub fn lend(
        &self,
        tree: &mut Tree,
        namespace: NodeId,
        set: NodeId,
    ) -> Result<Option<Loan>, LeafError> {
        // The repair opens the job's files where they lie, once everything
        // submitted for them has returned: it waits for that as it begins.
        let mut access = Lent {
            namespace,
            set,
            recovery: Some(set),
            defer_holds: false,
            scratch_read: None,
            held_scratch: BTreeMap::new(),
            settled: self.unsettled(),
            scratch_later: BTreeMap::new(),
            unwritten: false,
            internal: self.working_dir.join(weaver_graph::names::INTERNAL_DIR),
            members: BTreeMap::new(),
            homes: BTreeMap::new(),
            usage: RefCell::default(),
            postings: BTreeMap::new(),
            shelves: BTreeMap::new(),
            volumes: BTreeMap::new(),
            window: tree.window(set),
            open: RefCell::new(None),
            reads: 0,
            repaired: Repaired::new(&self.working_dir, namespace, set)?,
            found: Vec::new(),
            diagnostics: Vec::new(),
            scratch: None,
            shutdown: Shutdown::default(),
            withheld: BTreeSet::new(),
            working: self.working_volume(),
            reading: RefCell::default(),
            stop: Arc::default(),
            timing: RefCell::default(),
        };
        let mut seen = BTreeSet::new();
        self.take_namespace(tree, namespace, &mut access, &mut seen);
        if let Some((parent, volumes)) = tree.volumes(set) {
            let volumes: Vec<(VolumeIndex, MemberId)> = volumes
                .iter()
                .map(|(volume, member)| (*volume, *member))
                .collect();
            for (volume, member) in volumes {
                access.volumes.insert(volume, (parent, member));
                self.take_member(tree, parent, member, &mut access, &mut seen);
            }
        }
        for &at in access.volumes.values() {
            access.require(at);
        }
        if let Some(recovery) = tree.recovery(set) {
            for member in tree
                .namespace(namespace)
                .into_iter()
                .flat_map(|namespace| namespace.members())
            {
                if recovery.covers(member.id) {
                    access.require((namespace, member.id));
                }
            }
        }
        let state = self.state(tree, namespace, set);
        match std::fs::create_dir(&access.internal) {
            Err(error) if error.kind() != std::io::ErrorKind::AlreadyExists => {
                return Err(LeafError::Io {
                    device: crate::pipeline::graph::limits::Device::Working,
                    path: access.internal,
                    source: error,
                });
            }
            _ => {}
        }
        let Some(object) = tree.lend(set) else {
            return Ok(None);
        };
        Ok(Some(Loan {
            set,
            namespace,
            state,
            object,
            access,
        }))
    }

    // Whether the loop moved anything the repair read: a member's length or
    // binding, a byte of a range it read held, covered or written otherwise
    // than it found it, a file that is no longer where it was, or a shelf
    // whose generation moved on, which it does when what it holds is
    // replaced or goes away. Bytes past what it read leave it standing.
    pub fn moved_since(&self, tree: &Tree, returned: &Returned) -> bool {
        let changed = returned.inputs.iter().any(|input| {
            let files = self.files.get(&input.node);
            let bound = tree
                .child_of(input.node, input.member)
                .filter(|(child, _)| !tree.recovers(*child));
            let Some(record) = tree
                .namespace(input.node)
                .and_then(|namespace| namespace.get(input.member))
            else {
                return true;
            };
            bound != input.bound
                || record.length != input.length
                || input.read.runs().any(|run| {
                    let range = run.range;
                    let held = files
                        .map(|files| files.holds.revisions(held_volume(input.member), range))
                        .unwrap_or_default();
                    held != clipped(&input.held_basis, range)
                        || !same_runs(&record.coverage, &input.coverage, range)
                        || input.leaf.as_ref().is_some_and(|(path, seen)| {
                            files
                                .and_then(|files| files.leaves.get(&input.member))
                                .is_none_or(|leaf| {
                                    leaf.path() != path
                                        || leaf.changed_since(*seen, range.start, range.end)
                                })
                        })
                })
        });
        let shelves = returned
            .read
            .shelves
            .iter()
            .any(|(shelf, generation)| shelf.generation() != *generation);
        changed || shelves
    }

    fn take_namespace(
        &self,
        tree: &Tree,
        node: NodeId,
        access: &mut Lent,
        seen: &mut BTreeSet<NodeId>,
    ) {
        if !seen.insert(node) {
            return;
        }
        let members: Vec<MemberId> = tree
            .namespace(node)
            .map(|namespace| namespace.members().map(|member| member.id).collect())
            .unwrap_or_default();
        for member in members {
            self.take_member(tree, node, member, access, seen);
        }
    }

    fn take_member(
        &self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        access: &mut Lent,
        seen: &mut BTreeSet<NodeId>,
    ) {
        // A member lives where `Tree::home` says: the loan takes it there,
        // and answers reads of the member it was with it.
        let home = tree.member_home((node, member));
        if home != (node, member) {
            access.homes.insert((node, member), home);
            return self.take_member(tree, home.0, home.1, access, seen);
        }
        if access.members.contains_key(&(node, member)) {
            return;
        }
        let Some(record) = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
        else {
            return;
        };
        let files = self.files.get(&node);
        let held = match files {
            Some(files) => {
                let held: Vec<(ByteRange, Location)> = files
                    .holds
                    .peek(held_volume(member), ALL)
                    .into_iter()
                    .map(|piece| (piece.range, piece.location))
                    .collect();
                let on_scratch = held
                    .iter()
                    .any(|(_, location)| matches!(location, Location::Scratch { .. }));
                if on_scratch
                    && !access.held_scratch.contains_key(&node)
                    && !access.scratch_later.contains_key(&node)
                    && let Some(leaf) = files.scratch.as_ref()
                {
                    if access.settled.is_some() {
                        access.scratch_later.insert(node, leaf.path().to_path_buf());
                    } else if let Ok(file) = File::open(leaf.path()) {
                        access.held_scratch.insert(node, file);
                    }
                }
                held
            }
            None => Vec::new(),
        };
        let bound = tree
            .child_of(node, member)
            .filter(|(child, _)| !tree.recovers(*child));
        access.members.insert(
            (node, member),
            Taken {
                coverage: record.coverage.clone(),
                length: record.length,
                path: files
                    .and_then(|files| files.leaves.get(&member))
                    .map(|leaf| leaf.path().to_path_buf()),
                revision: files
                    .and_then(|files| files.leaves.get(&member))
                    .map(Leaf::revision),
                scratch_revision: files
                    .and_then(|files| files.scratch.as_ref())
                    .map(Leaf::revision),
                held_basis: files
                    .map(|files| files.holds.revisions(held_volume(member), ALL))
                    .unwrap_or_default(),
                held,
                spill_revision: files
                    .and_then(|files| files.spills.get(&member))
                    .map(Leaf::revision),
                // Read in place by every loan, never copied: bytes on file
                // change only as the member's holds on file change, which
                // the return sees, a recovery's in the hold revisions of
                // the ranges it read, a replay's in the file's revision.
                spill: files
                    .and_then(|files| files.spills.get(&member))
                    .filter(|_| access.settled.is_none())
                    .and_then(|leaf| File::open(leaf.path()).ok()),
                spill_later: files
                    .and_then(|files| files.spills.get(&member))
                    .filter(|_| access.settled.is_some())
                    .map(|leaf| leaf.path().to_path_buf()),
                bound,
                volume: files
                    .and_then(|files| files.leaves.get(&member))
                    .and_then(Leaf::volume)
                    .cloned(),
            },
        );
        let Some((child, _)) = bound else {
            return;
        };
        if let Some(posting) = tree.posting(child) {
            access.postings.insert(child, posting);
        }
        if let Some(shelf) = self.envelopes.shelf_of(child) {
            let generation = shelf.generation();
            access.shelves.insert(child, (shelf, generation));
        }
        self.take_namespace(tree, child, access, seen);
    }

    // Whether every held range a rewalk's piece read on scratch is held
    // as it was when lent: the same holds, at the same revisions, so the
    // span the piece read was not written again under it.
    pub fn scratch_stands(&self, read: &ScratchRead) -> bool {
        read.0.iter().all(|(node, member, range, basis)| {
            self.files
                .get(node)
                .is_some_and(|files| files.holds.revisions(held_volume(*member), *range) == *basis)
        })
    }
}

// The held ranges one piece of a rewalk read on scratch, each with the
// revisions its holds stood at when lent.
#[derive(Default)]
pub struct ScratchRead(Vec<(NodeId, MemberId, ByteRange, Revisions)>);

// Held ranges with the revision each stood at, as `Holds::revisions` gives
// them.
type Revisions = Vec<(ByteRange, u64)>;

impl Lent {
    // What the reads since the last call found on scratch, with the
    // revisions lent for it.
    fn scratch_read(&self) -> ScratchRead {
        let Some(read) = &self.scratch_read else {
            return ScratchRead::default();
        };
        ScratchRead(
            read.borrow_mut()
                .drain(..)
                .filter_map(|(node, member, range)| {
                    let taken = self.members.get(&(node, member))?;
                    Some((node, member, range, clipped(&taken.held_basis, range)))
                })
                .collect(),
        )
    }
}

impl Lent {
    fn read_file(
        &self,
        at: (NodeId, MemberId),
        path: &std::path::Path,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        let volume = self
            .members
            .get(&at)
            .and_then(|taken| taken.volume.as_ref());
        self.in_turn(
            volume,
            || false,
            || self.read_file_here(at, path, range, into),
        )
    }

    fn read_file_here(
        &self,
        at: (NodeId, MemberId),
        path: &std::path::Path,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        let mut open = self.open.borrow_mut();
        let mut timing = self.timing.borrow_mut();
        let on = timing.on;
        if open.as_ref().is_none_or(|(held, _)| *held != at) {
            let began = on.then(Instant::now);
            let opened = File::open(path);
            if let Some(began) = began {
                let took = began.elapsed();
                timing.asked.opens += 1;
                timing.asked.open += took;
                let file = timing.file(at, path);
                file.opens += 1;
                file.open += took;
            }
            let Ok(file) = opened else {
                *open = None;
                return false;
            };
            *open = Some((at, file));
        }
        let Some((_, file)) = open.as_ref() else {
            return false;
        };
        let start = into.len();
        into.resize(start + index(range.len()), 0);
        let began = on.then(Instant::now);
        let read = read_exact_at(file, range.start, &mut into[start..]).is_ok();
        if let Some(began) = began {
            let file = timing.file(at, path);
            file.reads += 1;
            file.bytes += range.len();
            file.read += began.elapsed();
        }
        if !read {
            into.truncate(start);
        }
        read
    }

    // A read of bytes held on scratch or spilled, for the timing line.
    fn held_read(&self, began: Option<Instant>, len: u64) {
        let Some(began) = began else {
            return;
        };
        let mut timing = self.timing.borrow_mut();
        timing.held += began.elapsed();
        timing.held_bytes += len;
    }

    fn timing_on(&self) -> bool {
        self.timing.borrow().on
    }

    // The repair has its plan: the engine's steps to it, and where the
    // process stood then.
    fn report_planned(&mut self, steps: &[(&'static str, Duration)]) {
        let timing = self.timing.get_mut();
        if !timing.on || timing.plan.is_some() {
            return;
        }
        timing.steps = steps.to_vec();
        timing.plan = Some(Mark::now(timing.asked));
    }

    // Said once, as a repair that came to a plan ends: the time from the
    // loan's making to the plan, step by step, and from the plan to the
    // end; what the repair read and wrote before its plan and after it;
    // the CPU, user and kernel apart, and the page faults the process took
    // after the plan; and the memory the system had free at the plan and
    // at the end. Repair workers spinning for input burn user time while
    // the reads here take long; workers faulting in memory burn kernel time
    // with the faults. The repairs running and the pools built say what
    // earlier work left.
    fn report_end(&self, running: u64) {
        use std::fmt::Write as _;
        let timing = self.timing.borrow();
        if !timing.on {
            return;
        }
        let Some(plan) = timing.plan else {
            return;
        };
        let end = Mark::now(timing.asked);
        let ran = timing.ran.unwrap_or(timing.lent);
        let mut engine = String::new();
        for (name, took) in &timing.steps {
            let _ = write!(engine, "{name}={:.1} ", ms(*took));
        }
        let mut slowest: Vec<&FileTiming> = timing.files.iter().collect();
        slowest.sort_unstable_by_key(|file| std::cmp::Reverse(file.open + file.read));
        let mut files = String::new();
        for file in slowest.into_iter().take(FILES_SAID) {
            let _ = write!(
                files,
                "[{} opens={} open_ms={:.1} reads={} bytes={} read_ms={:.1}] ",
                file.name,
                file.opens,
                ms(file.open),
                file.reads,
                file.bytes,
                ms(file.read),
            );
        }
        let after = plan
            .counters
            .zip(end.counters)
            .map(|(from, to)| (to.cpu.saturating_sub(from.cpu), from.faults.zip(to.faults)));
        let cpu_after_plan = after.map_or_else(
            || "-".to_string(),
            |(cpu, _)| format!("user={:.0} kernel={:.0}", ms(cpu.user()), ms(cpu.system())),
        );
        let faults_after_plan = after
            .and_then(|(_, faults)| faults)
            .map(|(from, to)| to.saturating_sub(from));
        let mb = |bytes: Option<u64>| bytes.map(|bytes| bytes / MIB);
        tracing::debug!(
            namespace = self.namespace.0,
            set = self.set.0,
            since_lent_ms = format_args!("{:.1}", ms(timing.lent.elapsed())),
            to_thread_ms = format_args!("{:.1}", ms(ran.duration_since(timing.lent))),
            settle_wait_ms = format_args!("{:.1}", ms(timing.settled)),
            begin_open_ms = format_args!("{:.1}", ms(timing.opened_at_begin)),
            to_plan_ms = format_args!("{:.1}", ms(plan.at.duration_since(ran))),
            after_plan_ms = format_args!("{:.1}", ms(end.at.duration_since(plan.at))),
            engine_ms = engine.trim_end(),
            asked_to_plan = plan.asked.said(),
            asked_after_plan = end.asked.since(plan.asked).said(),
            held_read_ms = format_args!("{:.1}", ms(timing.held)),
            held_bytes = timing.held_bytes,
            files_read = timing.files.len(),
            files = files.trim_end(),
            cpu_after_plan_ms = cpu_after_plan,
            faults_after_plan,
            free_mb_plan = mb(plan.available),
            free_mb_end = mb(end.available),
            repairs_running = running,
            pools_built = weaver_server_base::runtime::postprocess_pool::pools_built(),
            pool_threads = rayon::current_num_threads(),
            pool_thread = std::thread::current().name().unwrap_or("-"),
            "graph repair timing"
        );
    }
}

impl Source for Lent {
    fn bound(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
        self.note((node, member), None);
        self.members.get(&(node, member))?.bound
    }

    fn member_home(&self, at: (NodeId, MemberId)) -> (NodeId, MemberId) {
        self.homes.get(&at).copied().unwrap_or(at)
    }

    // The one recovery object a repair takes along is its set.
    fn recovers(&self, child: NodeId) -> bool {
        self.recovery == Some(child)
    }

    // The one binding a repair takes along is its set's.
    fn in_member(&self, child: NodeId, range: ByteRange) -> Option<ByteRange> {
        if self.recovery != Some(child) {
            return None;
        }
        match self.window {
            Some(window) => range.in_window(window),
            None => Some(range),
        }
    }

    fn posted(
        &self,
        child: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        if !self.defer_holds {
            self.usage.borrow_mut().shelves.insert(child);
        }
        self.postings
            .get(&child)
            .is_some_and(|posting| posting.posted(volume, range, read, into))
    }

    fn envelope(&self, child: NodeId, envelope: u32, range: ByteRange, into: &mut Vec<u8>) -> bool {
        if !self.defer_holds {
            self.usage.borrow_mut().shelves.insert(child);
        }
        match self.shelves.get(&child) {
            // The turn is taken before the shelf's lock, which the read
            // holds: nothing waits for a turn under it.
            Some((shelf, _)) => self.in_turn(
                shelf.volume(),
                || false,
                || shelf.read_aside(envelope, range, into),
            ),
            None => range.is_empty(),
        }
    }

    fn leaf(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.note((node, member), Some(range));
        let Some(taken) = self.members.get(&(node, member)) else {
            return range.is_empty();
        };
        let Some(path) = &taken.path else {
            return range.is_empty();
        };
        // The file is read where the lent coverage tiles the range with runs
        // of any provenance, lost included: a lost run does not stop it.
        // What is held for the member is not in its file, whatever length
        // the file has: the hold gives those bytes, never the file.
        let from = taken
            .held
            .partition_point(|(held, _)| held.end <= range.start);
        let held = taken
            .held
            .get(from)
            .is_some_and(|(held, _)| held.start < range.end);
        taken.coverage.covered(range) && !held && self.read_file((node, member), path, range, into)
    }

    fn held(&self, node: NodeId, member: MemberId, range: ByteRange) -> Vec<(ByteRange, Location)> {
        self.note((node, member), Some(range));
        let Some(taken) = self.members.get(&(node, member)) else {
            return Vec::new();
        };
        let from = taken
            .held
            .partition_point(|(held, _)| held.end <= range.start);
        taken.held[from..]
            .iter()
            .take_while(|(held, _)| held.start < range.end)
            .filter_map(|(held, location)| {
                let inside = held.intersect(range)?;
                let from = inside.start - held.start;
                let location = match location {
                    Location::Ram(bytes) => {
                        Location::Ram(bytes.slice(index(from)..index(from + inside.len())))
                    }
                    Location::Scratch { offset, .. } => {
                        if let Some(read) = &self.scratch_read {
                            read.borrow_mut().push((node, member, inside));
                        }
                        Location::Scratch {
                            offset: offset + from,
                            len: inside.len(),
                        }
                    }
                    Location::Spilled => Location::Spilled,
                };
                Some((inside, location))
            })
            .collect()
    }

    fn scratch(
        &self,
        node: NodeId,
        member: MemberId,
        offset: u64,
        len: u64,
        into: &mut Vec<u8>,
    ) -> bool {
        let Some(file) = self
            .held_scratch
            .get(&node)
            .filter(|_| self.members.contains_key(&(node, member)))
        else {
            return false;
        };
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        super::hooks::SCRATCH_READ_MOST.with(|most| most.set(most.get().max(len)));
        let start = into.len();
        into.resize(start + index(len), 0);
        let began = self.timing_on().then(Instant::now);
        let read = self.in_turn(self.working.as_ref(), stopped_read, || {
            read_exact_at(file, offset, &mut into[start..])
        });
        self.held_read(began, len);
        if read.is_err() {
            into.truncate(start);
            return false;
        }
        true
    }

    fn spilled(
        &self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        let Some(file) = self
            .members
            .get(&(node, member))
            .and_then(|taken| taken.spill.as_ref())
        else {
            return false;
        };
        let start = into.len();
        into.resize(start + index(range.len()), 0);
        let began = self.timing_on().then(Instant::now);
        let read = self.in_turn(self.working.as_ref(), stopped_read, || {
            read_exact_at(file, range.start, &mut into[start..])
        });
        self.held_read(began, range.len());
        if read.is_err() {
            into.truncate(start);
            return false;
        }
        true
    }
}

impl MemberAccess for Lent {
    fn repair_error(&mut self, operation: &str, context: &str, error: &dyn std::error::Error) {
        self.diagnostics
            .push(RepairDiagnostic::capture(operation, context, error));
    }

    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        if self.stopped() {
            return false;
        }
        self.reads += 1;
        let start = into.len();
        let began = self.timing.get_mut().on.then(Instant::now);
        let read = read_member(self, self.namespace, member, range, into);
        self.asked_read(began, range.len());
        if !read {
            into.truncate(start);
        }
        read
    }

    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        if self.stopped() {
            return;
        }
        let began = self.timing.get_mut().on.then(Instant::now);
        self.repaired.write(member, offset, bytes);
        if let Some(began) = began {
            let asked = &mut self.timing.get_mut().asked;
            asked.writes += 1;
            asked.write_bytes += bytes.len() as u64;
            asked.write += began.elapsed();
        }
    }

    fn planned(&mut self, steps: &[(&'static str, Duration)]) {
        self.report_planned(steps);
    }

    fn found_damaged(&mut self, member: MemberId, range: ByteRange) {
        self.found.push((member, range));
    }

    fn length(&self, member: MemberId) -> Option<u64> {
        self.note((self.namespace, member), None);
        self.members.get(&(self.namespace, member))?.length
    }

    fn withheld(&self, member: MemberId) -> bool {
        self.withheld.contains(&member)
    }

    fn arrived(&self, member: MemberId, range: ByteRange, out: &mut Vec<ByteRange>) {
        self.note((self.namespace, member), Some(range));
        let Some(taken) = self.members.get(&(self.namespace, member)) else {
            return;
        };
        out.extend(
            taken
                .coverage
                .overlapping(range)
                .filter_map(|run| run.range.intersect(range)),
        );
    }

    // Under the directory finalize never delivers, one per recovery object
    // and namespace, so two repairs never share one.
    fn scratch(&mut self) -> Option<PathBuf> {
        let dir = self.internal.join(format!(
            "graph-recovery-{}-{}",
            self.namespace.0, self.set.0
        ));
        super::remove_scratch(&dir);
        if let Err(error) = std::fs::create_dir(&dir) {
            self.scratch_failed(error);
            return None;
        }
        self.scratch = Some(dir.clone());
        Some(dir)
    }

    // Only a disk with no room is the namespace's to wait on. Any other
    // failure stays the object's, which rules the set as it did before.
    fn scratch_failed(&mut self, error: std::io::Error) {
        if crate::operations::is_out_of_space(&error) {
            let dir = self
                .scratch
                .clone()
                .unwrap_or_else(|| self.internal.clone());
            self.repaired.refuse(&dir, error);
        }
    }

    fn will_write(&mut self, bytes: u64) {
        self.repaired.will_write(bytes);
    }

    // The set's volume is a member of the namespace it was posted in, or a
    // window on one, read wherever the member's bytes are: its own file, or
    // back through the inverse map of the object that owns it.
    fn read_volume(&mut self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        if self.stopped() {
            return false;
        }
        let Some(&(parent, member)) = self.volumes.get(&volume) else {
            return false;
        };
        let Some(within) = self
            .in_member(self.set, range)
            .filter(|within| within.len() == range.len())
        else {
            return false;
        };
        let start = into.len();
        let began = self.timing.get_mut().on.then(Instant::now);
        let read = read_member(self, parent, member, within, into);
        self.asked_read(began, within.len());
        if !read {
            into.truncate(start);
        }
        read
    }
}

impl Lent {
    // A read the repair asked, for the timing line.
    fn asked_read(&mut self, began: Option<Instant>, len: u64) {
        let Some(began) = began else {
            return;
        };
        let asked = &mut self.timing.get_mut().asked;
        asked.reads += 1;
        asked.read_bytes += len;
        asked.read += began.elapsed();
    }
}
