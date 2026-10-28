// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A recovery set's repair, lent off the loop. The set's object leaves its
// node, and the access it repairs through takes along what its reads reach:
// for every member of the namespaces below the set, as far down as the tree
// goes, its coverage, the file it is written at and the bytes still held for
// it; for every data object there, what rebuilds its posted bytes; and the
// envelope shelves those objects keep, read as they stand. Nothing the loop
// changes after the loan reaches it, and what the loop moved that it read is
// known on its return: a shelf whose generation moved, or a file no longer
// where it was. The repaired ranges, the ranges found bad and the object come
// back together, and the namespace applies them as one repair. What the
// repair rebuilt waits on disk until then, never in memory.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::holds::Location;
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    ByteRange, Coverage, MemberId, NodeId, Object, Placement, Posting, Tree, VolumeIndex,
};

use super::super::envelope::Shelf;
use super::super::hardware::Shutdown;
use super::super::leaf::{Leaf, LeafError, read_exact_at};
use super::{
    ALL, NamespaceWriter, READ_BACK, SetState, Source, held_volume, index, read_member, read_pieces,
};

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
    read: Read,
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
// generation it stood at, and every file at the path it had, and whether
// the pipeline began shutting down before the repair was done, so that what
// it read may have been cut short.
struct Read {
    shelves: Vec<(Arc<Shelf>, u64)>,
    leaves: Vec<(NodeId, MemberId, PathBuf)>,
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
        let leaves = access
            .members
            .iter()
            .filter_map(|((node, member), taken)| Some((*node, *member, taken.path.clone()?)))
            .collect();
        Returned {
            set: self.set,
            namespace: self.namespace,
            object: self.object,
            outcome,
            reads: access.reads,
            repaired: access.repaired,
            found: access.found,
            read: Read {
                shelves: access.shelves.into_values().collect(),
                leaves,
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
    // The bytes held for it, in offset order.
    held: Vec<(ByteRange, Bytes)>,
    bound: Option<(NodeId, VolumeIndex)>,
}

impl Lent {
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
    // The directory the repair's scratch and output lie in, made when the
    // repair is lent. The repair never makes it again: once it is gone, the
    // job's working directory was released under the repair.
    internal: PathBuf,
    members: BTreeMap<(NodeId, MemberId), Taken>,
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
    scratch: Option<PathBuf>,
    // The pipeline's shutdown: once it starts, no read is answered.
    shutdown: Shutdown,
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

    fn write(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
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
            Err(error) => self.failed = Some(error),
        }
    }

    // The write the output refused, if one did: the repair failed with it.
    pub(crate) fn failed(&mut self) -> Option<LeafError> {
        self.failed.take()
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
            internal: self.working_dir.join(weaver_graph::names::INTERNAL_DIR),
            members: BTreeMap::new(),
            postings: BTreeMap::new(),
            shelves: BTreeMap::new(),
            volumes: BTreeMap::new(),
            window: tree.window(set),
            open: RefCell::new(None),
            reads: 0,
            repaired: Repaired::new(&self.working_dir, namespace, set)?,
            found: Vec::new(),
            scratch: None,
            shutdown: Shutdown::default(),
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

    // Whether the loop moved anything the repair read: a shelf whose
    // generation moved on, which it does when what it holds is replaced or
    // goes away, or a file that is no longer where it was.
    pub(crate) fn moved_since(&self, returned: &Returned) -> bool {
        let shelves = returned
            .read
            .shelves
            .iter()
            .any(|(shelf, generation)| shelf.generation() != *generation);
        shelves
            || returned.read.leaves.iter().any(|(node, member, path)| {
                self.files
                    .get(node)
                    .and_then(|files| files.leaves.get(member))
                    .is_none_or(|leaf| leaf.path() != path)
            })
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
        let held = files
            .map(|files| self.held_pieces(files, member))
            .unwrap_or_default();
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
                held,
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

    // What is held for the member, in memory: bytes paged to scratch are
    // copied out, since the scratch span is reused once its hold leaves.
    fn held_pieces(&self, files: &super::Files, member: MemberId) -> Vec<(ByteRange, Bytes)> {
        files
            .holds
            .peek(held_volume(member), ALL)
            .into_iter()
            .filter_map(|piece| {
                let bytes = match piece.location {
                    Location::Ram(bytes) => bytes,
                    Location::Scratch { offset, len } => {
                        let mut bytes = Vec::new();
                        files
                            .scratch
                            .as_ref()?
                            .read(offset, index(len), &mut bytes)
                            .ok()?;
                        Bytes::from(bytes)
                    }
                };
                Some((piece.range, bytes))
            })
            .collect()
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
        self.members.get(&(node, member))?.bound
    }

    fn posted(
        &self,
        child: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        self.postings
            .get(&child)
            .is_some_and(|posting| posting.posted(volume, range, read, into))
    }

    fn envelope(&self, child: NodeId, envelope: u32, range: ByteRange, into: &mut Vec<u8>) -> bool {
        match self.shelves.get(&child) {
            Some((shelf, _)) => shelf.read(envelope, range, into),
            None => range.is_empty(),
        }
    }

    fn leaf(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        let Some(taken) = self.members.get(&(node, member)) else {
            return range.is_empty();
        };
        let Some(path) = &taken.path else {
            return range.is_empty();
        };
        taken.coverage.covered(range) && self.read_file((node, member), path, range, into)
    }

    fn held(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        let Some(taken) = self.members.get(&(node, member)) else {
            return range.is_empty();
        };
        let pieces: Vec<(ByteRange, Location)> = taken
            .held
            .iter()
            .filter_map(|(held, bytes)| {
                let inside = held.intersect(range)?;
                let from = index(inside.start - held.start);
                let slice = bytes.slice(from..from + index(inside.len()));
                Some((inside, Location::Ram(slice)))
            })
            .collect();
        // Nothing it holds is on scratch: that was copied out when it left.
        read_pieces(
            pieces.iter().map(|(piece, location)| (*piece, location)),
            range,
            into,
            |_, _, _| false,
        )
    }
}

impl MemberAccess for Lent {
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
        self.members.get(&(self.namespace, member))?.length
    }

    fn arrived(&self, member: MemberId, range: ByteRange, out: &mut Vec<ByteRange>) {
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
        let within = match self.window {
            Some(window) => range.in_window(window),
            None => Some(range),
        };
        let Some(within) = within.filter(|within| within.len() == range.len()) else {
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
