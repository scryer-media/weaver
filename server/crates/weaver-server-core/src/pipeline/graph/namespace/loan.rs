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

pub(super) mod replay;
pub(super) mod rewalk;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::holds::Location;
use weaver_graph::recovery::{MemberAccess, RepairDiagnostic, RepairOutcome};
use weaver_graph::{
    ByteRange, Coverage, MemberId, NodeId, Object, Placement, Posting, Provenance, Tree,
    VolumeIndex,
};

use super::super::envelope::Shelf;
use super::super::hardware::Shutdown;
use super::super::leaf::{Leaf, LeafError, read_exact_at};
use super::{ALL, NamespaceWriter, READ_BACK, SetState, Source, held_volume, index, read_member};

// A set's repair, ready to run anywhere.
pub(crate) struct Loan {
    pub(crate) set: NodeId,
    // The namespace the set covers, where the repair writes.
    pub(crate) namespace: NodeId,
    // What the set said of itself as it left: what it says while it is out.
    pub(crate) state: Option<SetState>,
    object: Box<dyn Object>,
    access: Lent,
}

// What a repair came back with.
pub(crate) struct Returned {
    pub(crate) set: NodeId,
    pub(crate) namespace: NodeId,
    pub(crate) object: Box<dyn Object>,
    pub(crate) outcome: Option<RepairOutcome>,
    pub(crate) reads: u64,
    pub(crate) repaired: Repaired,
    // Ranges the object proved bad and could not rebuild.
    pub(crate) found: Vec<(MemberId, ByteRange)>,
    pub(crate) diagnostics: Vec<RepairDiagnostic>,
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
    pub(crate) fn stopped(&self) -> bool {
        self.read.stopped
    }
}

impl Loan {
    // The members the repair is to write nothing into: the disk has no room
    // for them.
    pub(super) fn withhold(&mut self, members: BTreeSet<MemberId>) {
        self.access.withheld = members;
    }

    // Run the repair, where it is not in anyone's way, within `budget`. A
    // repair given after `shutdown` started, or after the job's working
    // directory went, reads nothing, and one going when either happens
    // reads nothing more: each comes back stopped.
    pub(crate) fn run(mut self, budget: weaver_graph::Budget, shutdown: &Shutdown) -> Returned {
        self.access.shutdown = shutdown.clone();
        let given = !self.access.stopped();
        let outcome = self
            .object
            .recovery_mut()
            .filter(|_| given)
            .map(|recovery| recovery.repair(&mut self.access, budget));
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
struct Taken {
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
    spill_revision: Option<(u64, u64)>,
    bound: Option<(NodeId, VolumeIndex)>,
}

#[derive(Default)]
struct Usage {
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
    fn stopped(&self) -> bool {
        self.shutdown.started() || !self.internal.is_dir()
    }
}

// The owned access a lent repair reads and writes through.
struct Lent {
    namespace: NodeId,
    set: NodeId,
    recovery: Option<NodeId>,
    // Replay validates every node's revisions on return, so it notes
    // nothing of what it reads.
    defer_holds: bool,
    // Bytes held on scratch are read in place, through the loan's own
    // handle on the namespace's scratch file, as the work asks for them. A
    // span is written again only once every hold on scratch has left, which
    // the hold revisions show. A repair's return checks the revisions of
    // what it read. A rewalk's pieces are heard as read, so it notes here
    // the held ranges each piece found on scratch, and the loop checks
    // their revisions as it hears the piece (`scratch_stands`).
    scratch_read: Option<RefCell<Vec<(NodeId, MemberId, ByteRange)>>>,
    // Each namespace's scratch file, opened when lent for a member that
    // holds bytes there.
    held_scratch: BTreeMap<NodeId, File>,
    // The directory the repair's scratch and output lie in, made when the
    // repair is lent. The repair never makes it again: once it is gone, the
    // job's working directory was released under the repair.
    internal: PathBuf,
    members: BTreeMap<(NodeId, MemberId), Taken>,
    usage: RefCell<Usage>,
    postings: BTreeMap<NodeId, Box<dyn Posting>>,
    shelves: BTreeMap<NodeId, (Arc<Shelf>, u64)>,
    // The set's own volumes: the member each is, in the namespace it was
    // posted in, and the range of it a window is.
    volumes: BTreeMap<VolumeIndex, (NodeId, MemberId)>,
    window: Option<ByteRange>,
    // The last file read, kept open: a repair reads a member slice after
    // slice.
    open: RefCell<Option<((NodeId, MemberId), File)>>,
    reads: u64,
    repaired: Repaired,
    found: Vec<(MemberId, ByteRange)>,
    diagnostics: Vec<RepairDiagnostic>,
    scratch: Option<PathBuf>,
    // The pipeline's shutdown: once it starts, no read is answered.
    shutdown: Shutdown,
    // The members the disk has no room for the repair to rebuild.
    withheld: BTreeSet<MemberId>,
}

// What a repair rewrote: runs of members in the order written, each one's
// bytes on the repair's output file, so a repair never holds what it
// rebuilt in memory however much that is. The file lies beside the
// repair's scratch directory, whose names are the object's own, and goes
// once the namespace has applied the runs, or turned them away. A write
// the file refuses fails the repair: nothing it rebuilt is applied.
pub(crate) struct Repaired {
    // The member, where in it the run goes, and where on the output it is.
    runs: Vec<(MemberId, u64, ByteRange)>,
    output: Leaf,
    // How far the output file reaches: the next run is written there.
    written: u64,
    // The first write the output refused. Nothing is written after it.
    failed: Option<LeafError>,
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
        })
    }

    // What an object asked for again, read back and staged here until the
    // tree has uncovered it all, so a whole member said again is never in
    // memory at once. Each request gets its own file: one the replay
    // raises while an earlier one is being said again is staged apart.
    pub(crate) fn again(working_dir: &std::path::Path, node: NodeId) -> Result<Self, LeafError> {
        let internal = working_dir.join(weaver_graph::names::INTERNAL_DIR);
        std::fs::create_dir_all(&internal).map_err(|source| LeafError::Io {
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
        })
    }

    pub(crate) fn write(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
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
            .and_then(|()| self.output.write(at.start, bytes, None));
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
                self.failed = Some(error);
            }
        }
    }

    // The write the output refused, if one did: the repair failed with it.
    pub(crate) fn failed(&mut self) -> Option<LeafError> {
        self.failed.take()
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
                    .read(at.start + from, index(to - from), &mut bytes)
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
    pub(crate) fn lend(
        &self,
        tree: &mut Tree,
        namespace: NodeId,
        set: NodeId,
    ) -> Result<Option<Loan>, LeafError> {
        let mut access = Lent {
            namespace,
            set,
            recovery: Some(set),
            defer_holds: false,
            scratch_read: None,
            held_scratch: BTreeMap::new(),
            internal: self.working_dir.join(weaver_graph::names::INTERNAL_DIR),
            members: BTreeMap::new(),
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
    pub(crate) fn moved_since(&self, tree: &Tree, returned: &Returned) -> bool {
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
                    && let Some(file) = files
                        .scratch
                        .as_ref()
                        .and_then(|leaf| File::open(leaf.path()).ok())
                {
                    access.held_scratch.insert(node, file);
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
                    .and_then(|leaf| File::open(leaf.path()).ok()),
                bound,
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
    pub(crate) fn scratch_stands(&self, read: &ScratchRead) -> bool {
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
pub(crate) struct ScratchRead(Vec<(NodeId, MemberId, ByteRange, Revisions)>);

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
        let mut open = self.open.borrow_mut();
        if open.as_ref().is_none_or(|(held, _)| *held != at) {
            let Ok(file) = File::open(path) else {
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
        let read = read_exact_at(file, range.start, &mut into[start..]).is_ok();
        if !read {
            into.truncate(start);
        }
        read
    }
}

impl Source for Lent {
    fn bound(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
        self.note((node, member), None);
        self.members.get(&(node, member))?.bound
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
            Some((shelf, _)) => shelf.read(envelope, range, into),
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
        taken.coverage.covered(range) && self.read_file((node, member), path, range, into)
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
        #[cfg(test)]
        super::tests::SCRATCH_READ_MOST.with(|most| most.set(most.get().max(len)));
        let start = into.len();
        into.resize(start + index(len), 0);
        if read_exact_at(file, offset, &mut into[start..]).is_err() {
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
        if read_exact_at(file, range.start, &mut into[start..]).is_err() {
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
        let read = read_member(self, self.namespace, member, range, into);
        if !read {
            into.truncate(start);
        }
        read
    }

    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        if self.stopped() {
            return;
        }
        self.repaired.write(member, offset, bytes);
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
        std::fs::create_dir(&dir).ok()?;
        self.scratch = Some(dir.clone());
        Some(dir)
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
        let read = read_member(self, parent, member, within, into);
        if !read {
            into.truncate(start);
        }
        read
    }
}
