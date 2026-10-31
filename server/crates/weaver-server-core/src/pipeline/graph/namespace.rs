// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The namespace write path: the one place bytes delivered to a namespace are
// placed, held or refused. Every delivery to every namespace of a job comes
// through here, so the holds budgets, the damage marks and the recovery
// evidence are enforced once, for the root and for every namespace below it,
// and so are the job's extraction caps: what an object writes, the members
// it announces, the names they take and how deep an object is bound.
//
// What becomes of a delivery depends only on what its member is:
//
//   undecided   its first bytes have not said what it is; the bytes are held,
//               and replayed once it is decided. A breach of the holds
//               budgets decides it a leaf on the spot, and nothing pauses.
//   a leaf      the bytes are written to its file at their offset, damaged
//               or not; the namespace's coverage is the record of which.
//   bound       the bytes go down the tree to the object that owns it, and
//               whatever that object says comes back as effects, applied
//               here the same way.
//
// The tree records coverage and routes; this file is where bytes touch
// disk. It takes a node everywhere and never assumes the root.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::Posted;
use weaver_graph::holds::{HoldLedger, HoldsAccountant, HoldsNamespace, Location, Placed};
use weaver_graph::member::FIRST_BYTES;
use weaver_graph::{
    Algorithm, Builder, ByteRange, Coverage, Effect, Emit, Evidenced, Kind, Marked, MemberId,
    NodeId, Object, ObjectId, Placement, Provenance, Segment, Tree, VolumeIndex, Why,
};

use super::checkpoint::why_name;
use super::envelope::{Envelopes, Shelf, Shelved};
use super::leaf::{Leaf, LeafError};
use super::limits::Caps;
use crate::operations::disk::CapacityReader;
use crate::pipeline::extraction::ExtractionFailure;

// The block-aligned segment checksums of a delivered range, which the source
// computed while it had the bytes. Asked in the namespace's own coordinates,
// where member M is addressed as volume M: exactly the root's numbering, and
// a numbering every namespace has. A source answers for the namespace it
// delivered into and leaves every other one without.
pub(crate) trait Segments {
    fn segments_for(
        &self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        out: &mut Vec<Segment>,
    );
}

// Bytes for a member of a namespace.
#[derive(Clone, Debug)]
pub(crate) struct Arrival {
    pub(crate) node: NodeId,
    pub(crate) member: MemberId,
    pub(crate) offset: u64,
    // With the evidence of them that came with them: a member's check and
    // the sets over it fold it rather than hash the bytes again.
    pub(crate) bytes: Evidenced,
    pub(crate) provenance: Provenance,
}

impl Arrival {
    fn range(&self) -> ByteRange {
        ByteRange::at(self.offset, self.bytes.len() as u64)
    }
}

// What a write places, carried with it down the tree. New bytes reach the
// sets over every member that takes them, at any depth, once; bytes placed
// again from where they went reach none twice.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Write {
    // Bytes new to the job, posted as `posting` of the namespace at
    // `entered`: the source's checksums of them are the posting's.
    New { entered: NodeId, posting: MemberId },
    Again,
}

impl Write {
    // Bytes new to the job, entering where `arrival` puts them.
    fn new(arrival: &Arrival) -> Self {
        Self::New {
            entered: arrival.node,
            posting: arrival.member,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WriteError {
    #[error(transparent)]
    Leaf(#[from] LeafError),
    #[error("member {member:?} of node {node:?} is not in its namespace")]
    UnknownMember { node: NodeId, member: MemberId },
    #[error("recovery {set:?} changed inputs and its original snapshot could not be restored")]
    StaleRecovery { set: NodeId },
    // A set proved or judged bytes the namespace holds, and they are not
    // there to deliver again under what it said of them.
    #[error("member {member:?} of node {node:?} lost proven bytes {range:?}")]
    ProofUnreadable {
        node: NodeId,
        member: MemberId,
        range: ByteRange,
    },
    // The job's extraction budget refused it; nothing more of the job is
    // written.
    #[error(transparent)]
    Refused(#[from] ExtractionFailure),
}

// What one namespace has on disk and in hand.
#[derive(Debug)]
// A member's file and why it is a leaf are what a checkpoint keeps of it,
// so every change to either marks the member, under the leaves' marks.
struct Files {
    leaves: Marked<MemberId, Leaf>,
    holds: HoldLedger,
    // Where held bytes go when RAM runs short; opened on the first page-out.
    scratch: Option<Leaf>,
    // Why a member is a leaf when something refused to own it. Changed only
    // through `refuse` and `unrefuse`, which mark the member.
    refused: BTreeMap<MemberId, Why>,
}

impl Files {
    fn new(namespace: HoldsNamespace, accountant: Arc<HoldsAccountant>) -> Self {
        Self {
            leaves: Marked::default(),
            holds: HoldLedger::new(namespace, accountant),
            scratch: None,
            refused: BTreeMap::new(),
        }
    }

    fn refuse(&mut self, member: MemberId, why: Why) -> Option<Why> {
        self.leaves.mark(member);
        self.refused.insert(member, why)
    }

    fn unrefuse(&mut self, member: MemberId) {
        if self.refused.remove(&member).is_some() {
            self.leaves.mark(member);
        }
    }
}

// A repair's return being applied. A gap one slice fills can close a file
// before a later slice of the same batch replaces other bytes in it, so the
// members closed meanwhile are judged once the whole batch is down.
struct Acknowledging {
    set: NodeId,
    closed: BTreeSet<(NodeId, MemberId)>,
}

pub(crate) struct NamespaceWriter {
    // The job's process-wide id, which with a node id names a namespace to
    // the holds accountant that every job shares.
    scope: u64,
    working_dir: PathBuf,
    accountant: Arc<HoldsAccountant>,
    free_disk: CapacityReader,
    caps: Caps,
    // Each namespace's files, marked as each changes: a checkpoint reads
    // again only the namespaces marked since it read last.
    files: Marked<NodeId, Files>,
    envelopes: Envelopes,
    // Member reads the recovery objects made while repairing.
    read_backs: u64,
    // Rejected members waiting for their input dependencies to be staged
    // off the loop, before coverage or inverse mappings can be changed.
    rechecks: BTreeMap<NodeId, Vec<(VolumeIndex, ByteRange)>>,
    // Sparse durable obligations, retained until a current verification answers.
    verifications: BTreeSet<(NodeId, MemberId)>,
    // Whether the job's holds have been paged out for a stop. Only then do
    // the holds on scratch go in the checkpoint: scratch space is reused
    // once nothing lives on it, so a row written mid-run could name bytes a
    // later hold replaced before the next checkpoint reached the database.
    stopped: bool,
    renamed: Vec<MemberId>,
    // Members whose files wait to move to the names the ledger gave them,
    // in the order it gave them.
    moves: Vec<moves::Waiting>,
    // The names files left, by the ledger's key, each with the checkpoint
    // that records it left once one is taken.
    left: BTreeMap<String, Option<u64>>,
    // The latest checkpoint the database has committed.
    committed: u64,
    segments: Vec<Segment>,
    // Room for the effects of one routed delivery, empty between deliveries
    // and kept so an article's effects need no list of their own.
    effects: Vec<Effect>,
    // The set whose repair is being acknowledged: only its certified writes
    // retain its settled ruling.
    acknowledging: Option<Acknowledging>,
    // Members refused while a repair's return was applied, each read as the
    // first piece of a file once the whole return is down: a later slice of
    // the same return may still rewrite the front of a piece beside it.
    refused_in_batch: BTreeSet<(NodeId, MemberId)>,
    #[cfg(test)]
    diagnostics: Vec<weaver_graph::recovery::RepairDiagnostic>,
    placements: Vec<Placement>,
    // Where the time a member write takes on disk is published, when the
    // job runs in a pipeline.
    metrics: Option<Arc<crate::operations::metrics::PipelineMetrics>>,
}

impl NamespaceWriter {
    pub(crate) fn new(
        scope: u64,
        working_dir: PathBuf,
        accountant: Arc<HoldsAccountant>,
        free_disk: CapacityReader,
    ) -> Self {
        Self {
            scope,
            envelopes: Envelopes::new(working_dir.clone()),
            working_dir,
            accountant,
            free_disk,
            caps: Caps::unbounded(),
            files: Marked::default(),
            read_backs: 0,
            rechecks: BTreeMap::new(),
            verifications: BTreeSet::new(),
            stopped: false,
            renamed: Vec::new(),
            moves: Vec::new(),
            left: BTreeMap::new(),
            committed: 0,
            segments: Vec::new(),
            effects: Vec::new(),
            acknowledging: None,
            refused_in_batch: BTreeSet::new(),
            #[cfg(test)]
            diagnostics: Vec::new(),
            placements: Vec::new(),
            metrics: None,
        }
    }

    // The job's extraction budget, which every write, announcement, name
    // and binding here is held to.
    pub(crate) fn capped(mut self, caps: Caps) -> Self {
        self.caps = caps;
        self
    }

    // Publish the time each member write takes on disk.
    pub(crate) fn metered(
        mut self,
        metrics: Option<Arc<crate::operations::metrics::PipelineMetrics>>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    // The breach of the job's extraction budget, once one stands.
    pub(crate) fn refusal(&self) -> Option<&ExtractionFailure> {
        self.caps.refusal()
    }

    // How many member reads every repair so far needed: the evidence fed as
    // bytes passed settles the rest without one.
    pub(crate) fn read_backs(&self) -> u64 {
        self.read_backs
    }

    // Every member of the namespace with a file of its own, and its path.
    pub(crate) fn files(&self, node: NodeId) -> impl Iterator<Item = (MemberId, &Path)> + '_ {
        self.files
            .get(&node)
            .into_iter()
            .flat_map(|files| files.leaves.iter())
            .map(|(member, leaf)| (*member, leaf.path()))
    }

    // The file an object at `node` keeps of its volume, at the volume's own
    // offsets, if it kept any of it.
    pub(crate) fn kept_file(&self, node: NodeId, volume: VolumeIndex) -> Option<PathBuf> {
        let shelf = self.envelopes.shelf_of(node)?;
        let state = shelf.lock();
        state
            .kept
            .get(&volume.0)
            .map(|kept| kept.path().to_path_buf())
    }

    // Why the member is a leaf, when an object or the holds budgets refused
    // to own it.
    pub(crate) fn refused(&self, node: NodeId, member: MemberId) -> Option<Why> {
        self.files.get(&node)?.refused.get(&member).copied()
    }

    // A repair's return is down. Each member refused while it was applied,
    // and refused still with no object holding it, may be the first piece
    // of a file, read on the bytes the return left every piece with.
    pub(crate) fn refused_after_return(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        for (node, member) in std::mem::take(&mut self.refused_in_batch) {
            if self.refused(node, member).is_none() || tree.child_of(node, member).is_some() {
                continue;
            }
            let mut effects = Vec::new();
            tree.refused_whole(node, member, &mut self.placing(builder), &mut effects);
            self.prune(tree)?;
            self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn held_bytes(&self, node: NodeId) -> u64 {
        self.files
            .get(&node)
            .map_or(0, |files| files.holds.total_held())
    }

    // Bytes for a member of a namespace. Every effect goes to `out` once
    // this writer has done its part, except writing a leaf, which is wholly
    // its own.
    pub(crate) fn deliver(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let write = Write::new(&arrival);
        self.arrive(tree, builder, segments, write, arrival, out)?;
        self.release_held(tree, builder, segments, out)
    }

    // Bytes a restore reads back from where they went before the restart.
    // They are not new to the job: every set that took them then ruled on
    // them or reads them back, so none hears them again.
    pub(crate) fn deliver_again(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.arrive(tree, builder, segments, Write::Again, arrival, out)?;
        self.release_held(tree, builder, segments, out)
    }

    // Bytes for a member, by the one write path, inside a call that
    // releases the holds once it is done.
    fn arrive(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        write: Write,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.place(tree, builder, segments, arrival, write, out)
    }

    // A member was announced. A directory exists at once, and so does a
    // member that will never have a byte; neither has bytes to sniff.
    pub(crate) fn announced(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.announce(tree, builder, segments, node, member, out)?;
        self.release_held(tree, builder, segments, out)
    }

    fn announce(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        let (output_name, length) = (record.output_name.clone(), record.length);
        self.caps.admit_name(&output_name)?;
        self.caps.admit_member(tree, node, member)?;
        if Leaf::is_directory(&output_name) {
            self.settle(tree, builder, segments, node, member, out)?;
            Leaf::at(&self.working_dir, &output_name).create_directory()?;
        } else if length == Some(0) {
            return self.shut(tree, builder, segments, node, member, out);
        }
        self.bind_recoveries(tree, builder, segments, node, out)
    }

    // Nothing more will arrive for the member. The tree closes it and says
    // so, and the owning object hears it as a closed volume.
    pub(crate) fn close(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.shut(tree, builder, segments, node, member, out)?;
        self.release_held(tree, builder, segments, out)
    }

    fn shut(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut effects = Vec::new();
        tree.close(node, member, &mut self.placing(builder), &mut effects);
        self.apply_each(tree, builder, segments, effects, Write::Again, out)
    }

    // The member's length became known. Recovery objects match by length,
    // so they bind again.
    pub(crate) fn length_learned(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member, length): (NodeId, MemberId, u64),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace_mut(node)
            .and_then(|namespace| namespace.get_mut(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        if record.length.is_some() {
            return Ok(());
        }
        record.length = Some(length);
        self.bind_recoveries(tree, builder, segments, node, out)
    }

    // Effects the tree raised outside a delivery: an answered seek, a range
    // marked damaged. The ones left for the owner go to `out`.
    pub(crate) fn apply_all(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        effects: Vec<Effect>,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
        self.release_held(tree, builder, segments, out)
    }

    // `write` is what the bytes the effects carry are: what a write of new
    // bytes raises carries them on, and anything else carries none.
    fn apply_each(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        mut effects: Vec<Effect>,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.apply_drain(tree, builder, segments, &mut effects, write, out)
    }

    // [`Self::apply_each`], draining a list the caller keeps: on an error
    // the effects not yet applied are dropped, as they always were.
    fn apply_drain(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        effects: &mut Vec<Effect>,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        // What a member received before it was bound reaches its object
        // once every effect raised with the bind is applied: the tree said
        // them in order, and bytes it gave the owner before the bind, for
        // this member or for one the new object's parent has not decided
        // yet, are never overtaken by what the object makes of the rest.
        // A member folded into another posting of it is handed over the
        // same way, as that member's bytes.
        let bound: Vec<(NodeId, MemberId, NodeId)> = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Bound {
                    node,
                    member,
                    child,
                    ..
                } => Some((*node, *member, *child)),
                _ => None,
            })
            .collect();
        let folded: Vec<(NodeId, MemberId, Coverage)> = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Folded {
                    node, copy, held, ..
                } => Some((*node, *copy, held.clone())),
                _ => None,
            })
            .collect();
        for effect in effects.drain(..) {
            self.apply(tree, builder, segments, effect, write, out)?;
        }
        for (node, member, child) in bound {
            self.hand_over(tree, builder, segments, (node, member), child, out)?;
        }
        for (node, copy, held) in folded {
            self.fold_over(tree, builder, segments, (node, copy), &held, out)?;
        }
        Ok(())
    }

    // What a posting received before it folded into the member it repeats:
    // held, or in its own file when it was a leaf first. It goes down as
    // the member's bytes, each run with the provenance `held` says it
    // arrived with, since the posting's own record now keeps only what the
    // member takes of it, and the posting's file goes: it is never a file
    // of its own.
    fn fold_over(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, copy): (NodeId, MemberId),
        held: &Coverage,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.replay(tree, builder, segments, node, copy, ALL, Some(held), out)?;
        self.replay_file(tree, builder, segments, node, copy, Some(held), out)?;
        let files = self.files_mut(node);
        files.unrefuse(copy);
        if let Some(leaf) = files.leaves.remove(&copy) {
            self.discard(leaf)?;
        }
        self.caps.forget(tree, node, copy);
        Ok(())
    }

    // What the member received before it was bound: held, or in its file
    // when it was a leaf first. A member that closed before then, or since
    // in this pass, has its object hear that last, from the tree. An object that gave the member up in the
    // delivery that bound it was handed none of what was held: the holds
    // stay the record of that, for the refusal to carry on.
    fn hand_over(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        child: NodeId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        if tree.child_of(node, member).map(|(bound, _)| bound) == Some(child) {
            self.replay(tree, builder, segments, node, member, ALL, None, out)?;
        }
        self.replay_file(tree, builder, segments, node, member, None, out)?;
        let mut effects = Vec::new();
        tree.handed_over(node, member, &mut self.placing(builder), &mut effects);
        self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
        if tree.recovers(child) {
            self.bind_recoveries(tree, builder, segments, node, out)?;
        }
        Ok(())
    }

    // What an engine's run said at `node`. A run is its namespace's source,
    // as the articles are the root's, so the bytes it produces are a
    // delivery into that namespace and take the one write path: what an
    // object below keeps is captured, and a recovery volume is written to
    // its file, at any depth. Anything else it said is the object's word.
    pub(crate) fn said(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        emit: Emit,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let Emit::Bytes {
            member,
            offset,
            bytes,
            provenance,
        } = emit
        else {
            let mut effects = Vec::new();
            tree.emitted(node, vec![emit], &mut self.placing(builder), &mut effects);
            return self.apply_all(tree, builder, segments, effects, out);
        };
        let arrival = Arrival {
            node,
            member,
            offset,
            bytes,
            provenance,
        };
        self.deliver(tree, builder, segments, arrival, out)
    }

    // A recovery object recognised the member: the tree's ledger moves its
    // name, and every file whose name changed follows.
    #[cfg(test)]
    pub(crate) fn rename(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        node: NodeId,
        member: MemberId,
        name: &str,
    ) -> Result<String, WriteError> {
        let mut effects = Vec::new();
        let output_name = tree
            .rename(node, member, name, &mut self.placing(builder), &mut effects)
            .ok_or(WriteError::UnknownMember { node, member })?;
        self.moved(tree, &effects)?;
        Ok(output_name)
    }

    // Output names the ledger changed: each file written under its old name
    // wants its new one, in the order the ledger gave, and moves there once
    // the checkpoint that records it has committed. A member never written
    // has nothing to move.
    pub(crate) fn moved(&mut self, tree: &mut Tree, effects: &[Effect]) -> Result<(), WriteError> {
        let mut freed = Vec::new();
        for effect in effects {
            let Effect::Renamed {
                node,
                member,
                old,
                new,
            } = effect
            else {
                continue;
            };
            self.caps.admit_name(new)?;
            self.intend(*node, *member, new);
            // A name no file of the member sits at is free once the ledger
            // moved the member off it.
            if !self.sits_at(*node, *member, old) {
                tree.vacated(old, &mut freed);
            }
        }
        if freed.is_empty() {
            return Ok(());
        }
        self.moved(tree, &freed)
    }

    // The builder, with this writer behind it: what an object taking a
    // delivery reads back of its placements is read where the writer put it.
    pub(crate) fn placing<'a>(&'a self, builder: &'a mut dyn Builder) -> Placing<'a> {
        Placing {
            builder,
            writer: self,
        }
    }

    // The namespace's face toward its members, which reads them wherever
    // their bytes are.
    pub(crate) fn access<'a>(&'a self, tree: &'a Tree) -> Access<'a> {
        Access {
            writer: self,
            tree,
            held: true,
        }
    }

    // Bytes placed again from where they went: a replay.
    fn route(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.place(tree, builder, segments, arrival, Write::Again, out)
    }

    fn place(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let (node, member) = (arrival.node, arrival.member);
        // Every article routes through here, so its effects go in the room
        // the writer keeps. A delivery that routes again while this one
        // applies finds the room taken and uses a list of its own.
        let mut effects = std::mem::take(&mut self.effects);
        tree.deliver(
            node,
            member,
            Posted {
                offset: arrival.offset,
                bytes: arrival.bytes.clone(),
                provenance: arrival.provenance,
            },
            &mut self.placing(builder),
            &mut effects,
        );
        let recovery_volume = tree
            .child_of(node, member)
            .is_some_and(|(child, _)| tree.recovers(child));
        let applied = self.apply_drain(tree, builder, segments, &mut effects, write, out);
        self.effects = effects;
        applied?;
        // The front decides content matches, a finished member has the
        // length matches need, and a recovery volume's bytes may describe
        // more of the members.
        let front = arrival.offset < FIRST_BYTES as u64;
        let complete = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|record| record.is_complete());
        if recovery_volume || front || complete {
            self.bind_recoveries(tree, builder, segments, node, out)?;
        }
        Ok(())
    }

    // The bytes a bound member's object kept for no member of its own: its
    // envelopes, and what it could not place yet, which the namespace holds
    // for the member as it holds any bytes whose place is not known.
    fn capture(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        (child, volume): (NodeId, VolumeIndex),
        arrival: &Arrival,
        effects: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut placements = std::mem::take(&mut self.placements);
        placements.clear();
        if tree.child_of(arrival.node, arrival.member) == Some((child, volume)) {
            tree.locate(child, volume, arrival.range(), &mut placements);
        }
        // An object that gave the volume back in this delivery places none
        // of it. What no piece covers is held for the member, as any bytes
        // whose place is not known, and goes to its own file with the rest.
        let placed: u64 = placements.iter().map(Placement::len).sum();
        if placed < arrival.range().len() {
            placements.push(Placement::Held {
                range: ByteRange::new(arrival.offset + placed, arrival.range().end),
            });
        }
        let kept = self
            .envelopes
            .capture(child, &placements, &arrival.bytes, arrival.provenance);
        // What arrives is the member's bytes now, whatever was held of them
        // before: a repair rewrites what was held, and the held copy never
        // goes down after it.
        if let Some(files) = self.files.get_mut(&arrival.node) {
            files
                .holds
                .take(held_volume(arrival.member), arrival.range());
        }
        let held: Vec<(u64, Bytes)> = placements
            .iter()
            .filter_map(|placement| match placement {
                Placement::Held { range } => range.intersect(arrival.range()),
                _ => None,
            })
            .map(|piece| {
                let bytes = arrival
                    .bytes
                    .bytes()
                    .slice(index(piece.start - arrival.offset)..index(piece.end - arrival.offset));
                (piece.start, bytes)
            })
            .collect();
        self.placements = placements;
        kept?;
        self.hold_for(
            tree,
            builder,
            child,
            (arrival.node, arrival.member),
            held,
            effects,
        )
    }

    // What the object at `child` kept in its envelope of its volume, the
    // member, and now places elsewhere: read back from where it was kept,
    // and held for the member until the object places it again.
    fn unkept(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        (node, member, child): (NodeId, MemberId, NodeId),
        (range, envelope, at): (ByteRange, u32, u64),
        effects: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut bytes = Vec::new();
        if !self
            .envelopes
            .read(child, envelope, ByteRange::at(at, range.len()), &mut bytes)
        {
            return Ok(());
        }
        let held = vec![(range.start, Bytes::from(bytes))];
        self.hold_for(tree, builder, child, (node, member), held, effects)
    }

    // Bytes of a bound member, held until its object places them. A breach
    // of the holds budgets refuses the object, as it would refuse an
    // undecided member, and the bytes that breached go to the member's own
    // file, which it is now.
    fn hold_for(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        child: NodeId,
        (node, member): (NodeId, MemberId),
        held: Vec<(u64, Bytes)>,
        effects: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut breached = Vec::new();
        for (offset, bytes) in held {
            if self.keep_held(node, member, offset, bytes.clone())? {
                breached.push((offset, bytes));
            }
        }
        if breached.is_empty() {
            return Ok(());
        }
        tree.emitted(
            child,
            vec![weaver_graph::Emit::Refuse(Why::HoldsBreached)],
            &mut self.placing(builder),
            effects,
        );
        for (offset, bytes) in breached {
            self.write_file(tree, node, member, offset, &bytes.into())?;
        }
        Ok(())
    }

    // The node's envelopes, shared with the run of an engine that reads
    // them as its volumes.
    pub(crate) fn shelf(&mut self, node: NodeId) -> Arc<Shelf> {
        self.envelopes.shelf(node)
    }

    // Change the node's envelopes as `change` says it did: what the shelf
    // keeps of them is a checkpoint's, so a change goes through here.
    pub(crate) fn update_shelf<E>(
        &mut self,
        node: NodeId,
        change: impl FnOnce(&Shelf, &mut Shelved) -> Result<bool, E>,
    ) -> Result<(), E> {
        self.envelopes.update_if(node, change)
    }

    fn apply(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        effect: Effect,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        // Every delivery the tree takes for a member ends in a write or a
        // reach, after the namespace kept what it keeps of the bytes: the
        // member's front and the runs of what arrived. A rename changes the
        // names its record keeps. Here what changed is charged.
        if let Effect::WriteLeaf { node, member, .. }
        | Effect::Reached { node, member, .. }
        | Effect::Renamed { node, member, .. } = &effect
        {
            self.caps.admit_member(tree, *node, *member)?;
        }
        match &effect {
            Effect::WriteLeaf {
                node,
                member,
                offset,
                bytes,
                provenance,
            } => {
                let arrival = Arrival {
                    node: *node,
                    member: *member,
                    offset: *offset,
                    bytes: bytes.clone(),
                    provenance: *provenance,
                };
                self.taken(tree, segments, &arrival, write);
                return self.write_leaf(tree, builder, segments, arrival, write, out);
            }
            Effect::Reached {
                node,
                member,
                child,
                volume,
                offset,
                bytes,
                provenance,
            } => {
                let arrival = Arrival {
                    node: *node,
                    member: *member,
                    offset: *offset,
                    bytes: bytes.clone(),
                    provenance: *provenance,
                };
                self.taken(tree, segments, &arrival, write);
                // A recovery object's volume stays in its member's own
                // file, where a repair reads its blocks back; any other
                // object's keeps are captured for it.
                if tree.recovers(*child) {
                    return self.write_file(tree, *node, *member, *offset, bytes);
                }
                let mut effects = Vec::new();
                self.capture(tree, builder, (*child, *volume), &arrival, &mut effects)?;
                return self.apply_each(tree, builder, segments, effects, write, out);
            }
            Effect::Announced { node, member } => {
                self.announce(tree, builder, segments, *node, *member, out)?;
            }
            Effect::Closed { node, member } => {
                // A member that ended before its front could say what it is
                // is a leaf, whoever closed it: the root's source or an
                // object's emit.
                if tree.is_undecided(*node, *member) {
                    self.settle(tree, builder, segments, *node, *member, out)?;
                }
                // A leaf and a recovery volume live in their own files; a
                // member another object owns has no file to close.
                if in_own_file(tree, *node, *member) {
                    self.close_leaf(tree, *node, *member)?;
                    match &mut self.acknowledging {
                        Some(batch) => {
                            batch.closed.insert((*node, *member));
                        }
                        None => self.judge(tree, builder, segments, (*node, *member), out)?,
                    }
                }
                self.bind_recoveries(tree, builder, segments, *node, out)?;
            }
            Effect::Bound {
                node,
                member,
                kind,
                child,
            } => {
                // Handed what it received before, by `apply_each`.
                self.bound(tree, *node, *member, *kind, *child);
            }
            // Handed over as the member's, by `apply_each`.
            Effect::Folded { .. } => {}
            Effect::Refused { node, member, why } | Effect::NeedsEngine { node, member, why } => {
                self.refuse(tree, *node, *member, *why);
                // The engine the refusal handed the member to took it over.
                if matches!(effect, Effect::NeedsEngine { .. })
                    && let Some((engine, _)) = tree.child_of(*node, *member)
                    && let Some(kind) = tree.kind(engine)
                {
                    self.bound(tree, *node, *member, kind, engine);
                }
                self.materialise(tree, builder, segments, *node, *member, out)?;
                let mut effects = Vec::new();
                tree.bury(*node, *member, &mut effects);
                // A refused member its recovery set proves whole may be the
                // first piece of a file, now in its own file. One refused
                // while a repair's return is applied is read once all of it
                // is down.
                if matches!(effect, Effect::Refused { .. }) {
                    if self.acknowledging.is_some() {
                        self.refused_in_batch.insert((*node, *member));
                    } else {
                        tree.refused_whole(
                            *node,
                            *member,
                            &mut self.placing(builder),
                            &mut effects,
                        );
                    }
                }
                self.prune(tree)?;
                self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
            }
            // Read back into its own file as a refused member is, nothing
            // refused, and then claimed by what its own bytes sign: its
            // object's claim on it was the name its bytes refuted.
            Effect::Disclaimed { node, member } => {
                self.materialise(tree, builder, segments, *node, *member, out)?;
                let mut effects = Vec::new();
                tree.claim_by_bytes(*node, *member, &mut self.placing(builder), &mut effects);
                self.prune(tree)?;
                self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
            }
            Effect::GivenBack {
                node,
                member,
                from,
                volume,
            } => self.given_back(tree, (*node, *member), (*from, *volume))?,
            Effect::Unkept {
                node,
                member,
                child,
                range,
                envelope,
                at,
                ..
            } => {
                let mut effects = Vec::new();
                self.unkept(
                    tree,
                    builder,
                    (*node, *member, *child),
                    (*range, *envelope, *at),
                    &mut effects,
                )?;
                self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
            }
            Effect::Retracted { node, member } => {
                self.retract(tree, *node, *member)?;
                self.prune(tree)?;
            }
            Effect::RecoveryAt {
                node,
                volume,
                range,
                kind,
                handed,
            } => {
                self.recovery_at(
                    tree,
                    builder,
                    segments,
                    (*node, *volume, *range),
                    (*kind, handed),
                    out,
                )?;
            }
            Effect::Damaged {
                node,
                member,
                range,
            } => {
                // A member whose front stops at bytes that will never come
                // cannot say what it is from them: it is a leaf, as one that
                // ended first is. A repair there lets it say again.
                if tree.is_undecided(*node, *member) && !tree.front_to_come(*node, *member) {
                    self.settle(tree, builder, segments, *node, *member, out)?;
                }
                let mut effects = Vec::new();
                tree.damage_reached(
                    *node,
                    *member,
                    *range,
                    &mut self.placing(builder),
                    &mut effects,
                );
                self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
            }
            Effect::Renamed { .. } => self.moved(tree, std::slice::from_ref(&effect))?,
            Effect::NeedsPassword { .. } => {}
            // Every effect before it is applied, so what the object reads
            // back of its members is on disk. Each run's effects are
            // applied before the next run goes, as each delivery's are.
            Effect::Again { node, ranges } => {
                let runs = tree.again_read(*node, ranges, &self.placing(builder));
                for (volume, posted) in runs {
                    let mut effects = Vec::new();
                    tree.replay_piece(
                        *node,
                        volume,
                        posted,
                        &mut self.placing(builder),
                        &mut effects,
                    );
                    self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
                }
            }
        }
        out.push(effect);
        Ok(())
    }

    // A member its object gave back is a leaf, and its file is what the
    // object kept of it, its envelope, which was always at the volume's own
    // offsets. Usually that file moves to the member's name. During a
    // reentrant handover a source leaf may still await replay: keep its
    // bytes and merge the returned envelope into it before releasing either.
    // An envelope nothing was written to is no file, as a member
    // none of whose bytes were written has none. What the object never
    // placed is still held for the member and goes into that file as held
    // bytes go into any leaf's. The member retains only the coverage backed
    // by that file and its holds. Until an object
    // claims it, the tree holds its parse beside it, which is what withholds
    // it; the claim that takes it reads the file in place, as it reads any
    // leaf's.
    fn given_back(
        &mut self,
        tree: &mut Tree,
        (node, member): (NodeId, MemberId),
        (from, volume): (NodeId, VolumeIndex),
    ) -> Result<(), WriteError> {
        let output_name = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| record.output_name.clone())
            .ok_or(WriteError::UnknownMember { node, member })?;
        if let Some(kept) = self.envelopes.take(from, volume.0) {
            if kept.coverage.bytes_covered() == 0 {
                self.discard(kept.into_leaf())?;
            } else {
                let mut retained = kept.coverage.clone();
                let envelope = kept.into_leaf();
                if let Some(leaf) = self.files_mut(node).leaves.get_mut(&member) {
                    let length = leaf.len()?;
                    // A leaf awaiting replay owns the member's recorded
                    // ranges, including after restore without a CRC fold.
                    // Its allocated length never supplies new coverage.
                    let prior: Vec<_> = tree
                        .namespace(node)
                        .and_then(|namespace| namespace.get(member))
                        .into_iter()
                        .flat_map(|record| record.coverage.runs())
                        .filter_map(|run| run.range.intersect(ByteRange::new(0, length)))
                        .collect();
                    // Only this transfer reads the returned envelope. The
                    // unreplayed source stays in place, including its tail.
                    let check = tree
                        .namespace(node)
                        .and_then(|namespace| namespace.get(member))
                        .and_then(|record| record.check());
                    for run in retained.runs() {
                        let mut at = run.range.start;
                        while at < run.range.end {
                            let take = (run.range.end - at).min(READ_BACK);
                            let mut bytes = Vec::new();
                            envelope.read(at, index(take), &mut bytes)?;
                            match check {
                                Some(algorithm) => {
                                    let bytes = Evidenced::from(bytes);
                                    leaf.write_folding(at, &bytes, algorithm, None)?;
                                }
                                None => leaf.write(at, &bytes, None)?,
                            }
                            at += take;
                        }
                    }
                    for range in prior {
                        retained.write(range, Provenance::Fresh);
                    }
                    self.discard(envelope)?;
                } else {
                    self.files_mut(node).leaves.insert(member, envelope);
                }
                self.uncover_unkept(tree, (node, member), &retained);
                self.intend(node, member, &output_name);
            }
        }
        self.release_to_leaf(tree, node, member)?;
        Ok(())
    }

    // A byte the member's coverage says arrived that neither the envelope
    // given back as its file nor its holds have is not in hand, as a
    // restart's lost range is not, and comes again. Only a restore leaves
    // one: an envelope comes back as far as its record and its file reach,
    // the member's coverage as far as what arrived, which counts bytes that
    // were only held in memory. A range held as damage may never have
    // arrived at all, and stays.
    fn uncover_unkept(&self, tree: &mut Tree, (node, member): (NodeId, MemberId), kept: &Coverage) {
        let Some(record) = tree.namespace(node).and_then(|found| found.get(member)) else {
            return;
        };
        let mut unkept = Coverage::new();
        for run in record.coverage.runs() {
            if !run.provenance.is_damaged() {
                unkept.write(run.range, run.provenance);
            }
        }
        for run in kept.runs() {
            unkept.erase(run.range);
        }
        if let Some(files) = self.files.get(&node) {
            for range in files.holds.held_ranges(held_volume(member)) {
                unkept.erase(range);
            }
        }
        let unkept: Vec<ByteRange> = unkept.runs().map(|run| run.range).collect();
        if let Some(namespace) = tree.namespace_mut(node) {
            for range in unkept {
                namespace.uncover(member, range);
            }
        }
    }

    // New bytes a member took, at any depth, reach the sets over it as they
    // pass, with the evidence they came with, so a set has no slice of them
    // to read back. In the namespace they entered they are the posting's,
    // whose source's checksums go with them.
    fn taken(&mut self, tree: &mut Tree, segments: &dyn Segments, arrival: &Arrival, write: Write) {
        if let Write::New { entered, posting } = write {
            let posting = if arrival.node == entered {
                posting
            } else {
                arrival.member
            };
            self.feed(tree, segments, arrival, posting);
        }
    }

    fn write_leaf(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let (node, member) = (arrival.node, arrival.member);
        if tree.is_undecided(node, member) {
            return self.hold(tree, builder, segments, arrival, write, out);
        }
        self.release_to_leaf(tree, node, member)?;
        self.write_file(tree, node, member, arrival.offset, &arrival.bytes)
    }

    // Hold bytes whose member is not yet decided. A breach settles it: the
    // object that takes it is handed everything held and the new bytes; a
    // leaf has them go to its file.
    fn hold(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let (node, member) = (arrival.node, arrival.member);
        if self.keep_held(node, member, arrival.offset, arrival.bytes.bytes().clone())? {
            if let Some(owner) = self.settle(tree, builder, segments, node, member, out)? {
                let mut effects = Vec::new();
                self.capture(tree, builder, owner, &arrival, &mut effects)?;
                return self.apply_each(tree, builder, segments, effects, write, out);
            }
            self.refuse(tree, node, member, Why::HoldsBreached);
            self.write_file(tree, node, member, arrival.offset, &arrival.bytes)?;
            out.push(Effect::Refused {
                node,
                member,
                why: Why::HoldsBreached,
            });
        }
        Ok(())
    }

    // Hold bytes of the member in its namespace's ledger, paging to scratch
    // as the ledger says. True when they breached the budgets and were not
    // held.
    fn keep_held(
        &mut self,
        node: NodeId,
        member: MemberId,
        offset: u64,
        bytes: Bytes,
    ) -> Result<bool, WriteError> {
        let free_disk = self.free_disk_bytes();
        let outcome =
            self.files_mut(node)
                .holds
                .hold(held_volume(member), offset, bytes, free_disk);
        for page_out in &outcome.page_outs {
            self.scratch(node)?
                .write(page_out.scratch_offset, &page_out.bytes, None)?;
        }
        Ok(matches!(outcome.placed, Placed::Breached(_)))
    }

    // A member whose front will say no more is settled by the tree: an
    // object beside it may take it, and is handed what was held of it;
    // unclaimed, it is a leaf, and what was held goes to its file. The
    // object that took it, as its volume.
    fn settle(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<Option<(NodeId, VolumeIndex)>, WriteError> {
        let mut effects = Vec::new();
        tree.settle_leaf(node, member, &mut effects);
        self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
        let owner = tree.child_of(node, member);
        if owner.is_none() {
            self.release_to_leaf(tree, node, member)?;
        }
        Ok(owner)
    }

    // The member was decided as a leaf: what was held is written first, so
    // the bytes that arrived after it win where they overlap.
    fn release_to_leaf(
        &mut self,
        tree: &mut Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), WriteError> {
        for (offset, bytes) in self.take_held(node, member, ALL)? {
            self.write_file(tree, node, member, offset, &bytes.into())?;
        }
        Ok(())
    }

    // What was held of `range` of the member goes down to its object, each
    // run with the provenance the coverage recorded when it arrived: all of
    // it once the member is bound, and what an object asks for again. The
    // coverage is `said` when given, a folded posting's record from before
    // it folded, else the member's record as it stands. One hold leaves the
    // ledger at a time, as it goes down: the ledger is the record of what
    // the object has not been handed, so whatever a piece makes of the
    // member, a refusal reading the object back among it, sees the rest
    // still held.
    #[allow(clippy::too_many_arguments)]
    fn replay(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        said: Option<&Coverage>,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let held: Vec<ByteRange> = self
            .files
            .get(&node)
            .map(|files| files.holds.held_ranges(held_volume(member)))
            .unwrap_or_default()
            .into_iter()
            .filter_map(|hold| hold.intersect(range))
            .collect();
        for hold in held {
            for piece in self.take_held(node, member, hold)? {
                self.replay_piece(tree, builder, segments, (node, member), piece, said, out)?;
            }
        }
        Ok(())
    }

    // One piece of what was held goes down, run by run of its coverage.
    #[allow(clippy::too_many_arguments)]
    fn replay_piece(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        (offset, bytes): (u64, Bytes),
        said: Option<&Coverage>,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let range = ByteRange::at(offset, bytes.len() as u64);
        let runs: Vec<(ByteRange, Provenance)> = said
            .or_else(|| {
                tree.namespace(node)
                    .and_then(|namespace| namespace.get(member))
                    .map(|record| &record.coverage)
            })
            .map(|coverage| {
                coverage
                    .overlapping(range)
                    .filter_map(|run| Some((run.range.intersect(range)?, run.provenance)))
                    .collect()
            })
            .unwrap_or_default();
        for (piece, provenance) in runs {
            let start = index(piece.start - offset);
            let end = index(piece.end - offset);
            let arrival = Arrival {
                node,
                member,
                offset: piece.start,
                bytes: bytes.slice(start..end).into(),
                provenance,
            };
            self.route(tree, builder, segments, arrival, out)?;
        }
        Ok(())
    }

    // Held bytes go down again once the object their member is a volume of
    // places them, at whatever depth: an object's map grows only as it
    // hears its volumes or announces a member, and either may come from an
    // object above it. What just arrived may be the headers that link a
    // volume whose data came first. What its object still cannot place
    // stays held. This runs once a call into the writer has done everything
    // else, and never inside one: held bytes never overtake the effects of
    // the bytes before them, and what one release makes placeable goes in
    // the next pass, not from inside this one.
    fn release_held(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        while self.release_placed(tree, builder, segments, out)? {}
        Ok(())
    }

    // One pass over every hold, each held volume once in volume order and
    // nothing else of its namespace: true when any went down.
    fn release_placed(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<bool, WriteError> {
        let mut released = false;
        let parents: Vec<NodeId> = self
            .files
            .iter()
            .filter(|(_, files)| files.holds.total_held() > 0)
            .map(|(node, _)| *node)
            .collect();
        let mut placements = Vec::new();
        for parent in parents {
            let mut after = None;
            while let Some(held) = self
                .files
                .get(&parent)
                .and_then(|files| files.holds.next_held_volume(after))
            {
                after = Some(held);
                let member = held_member(held);
                // A further posting's bytes are its member's: only the
                // namespace's own members are asked.
                let listed = tree.namespace(parent).is_some_and(|namespace| {
                    namespace.get(member).is_some() && namespace.copy_of(member).is_none()
                });
                if !listed {
                    continue;
                }
                #[cfg(test)]
                tests::RELEASE_ASKED.with(|asked| asked.set(asked.get() + 1));
                let Some((child, volume)) = tree.child_of(parent, member) else {
                    continue;
                };
                let held = self
                    .files
                    .get(&parent)
                    .map(|files| files.holds.held_ranges(held))
                    .unwrap_or_default();
                for range in held {
                    placements.clear();
                    tree.locate(child, volume, range, &mut placements);
                    if placements
                        .iter()
                        .any(|placement| !matches!(placement, Placement::Held { .. }))
                    {
                        self.replay(tree, builder, segments, parent, member, range, None, out)?;
                        released = true;
                    }
                }
            }
        }
        Ok(released)
    }

    // What is held of `range` of the member, in offset order, read back
    // from scratch where it was paged.
    fn take_held(
        &mut self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
    ) -> Result<Vec<(u64, Bytes)>, WriteError> {
        let Some(files) = self.files.get_mut(&node) else {
            return Ok(Vec::new());
        };
        let taken = files.holds.take(held_volume(member), range);
        let mut held = Vec::with_capacity(taken.len());
        for piece in taken {
            let bytes = match piece.location {
                Location::Ram(bytes) => bytes,
                Location::Scratch { offset, len } => {
                    let mut buffer = Vec::new();
                    self.scratch(node)?.read(offset, index(len), &mut buffer)?;
                    Bytes::from(buffer)
                }
            };
            held.push((piece.range.start, bytes));
        }
        Ok(held)
    }

    fn write_file(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        offset: u64,
        bytes: &Evidenced,
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        // The leaf takes the member's length when it opens, so the file
        // reaches that far from its first byte.
        let end = offset
            .saturating_add(bytes.len() as u64)
            .max(record.length.unwrap_or(0));
        self.admit_file(tree, node, member, end)?;
        let leaf = self.leaf(node, member, &record.output_name)?;
        let started = std::time::Instant::now();
        // The leaf folds the digest the member's stated check needs, from
        // the evidence the bytes came with, hashing only bytes no level above
        // proved. A member that states no check, or not yet, as a split RAR
        // member before its last header, folds only what its bytes came
        // proven with: nothing is hashed for a check that may never come, and
        // one stated later finds the fold without reading the file back.
        let proven = || {
            let algorithm = bytes.evidence().first()?.digest.algorithm;
            bytes.proven(algorithm).then_some(algorithm)
        };
        match record.check().or_else(proven) {
            Some(algorithm) => leaf.write_folding(offset, bytes, algorithm, record.length)?,
            None => leaf.write(offset, bytes, record.length)?,
        }
        let took = started.elapsed();
        if let Some(length) = record.length.filter(|_| record.is_complete()) {
            leaf.close(length)?;
        }
        if let Some(metrics) = &self.metrics {
            metrics.disk_write_latency_us.store(
                u64::try_from(took.as_micros()).unwrap_or(u64::MAX),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        Ok(())
    }

    // The member is closed: its file takes the length the tree gave it. A
    // member none of whose bytes were written has no file: a name is a file
    // only once something of it was written, so a file whose every article
    // was lost is none, though its coverage holds that loss as damage, and
    // a file a recovery object describes is one only when its repair
    // writes it.
    fn close_leaf(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        let length = record.length.unwrap_or(0);
        let written = self
            .files
            .get(&node)
            .is_some_and(|files| files.leaves.contains_key(&member));
        if length > 0 && !written {
            return Ok(());
        }
        let output_name = record.output_name.clone();
        self.admit_file(tree, node, member, length)?;
        self.leaf(node, member, &output_name)?.close(length)?;
        Ok(())
    }

    // A file whose bytes fail the checksum its object states for them is
    // damaged, whatever each byte's own check said: the graph hears it as
    // damage on the whole member.
    fn judge(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        if !self.fails_its_checksum(tree, node, member)? {
            return Ok(());
        }
        let mut effects = Vec::new();
        tree.checksum_failed(node, member, &mut effects);
        self.apply_each(tree, builder, segments, effects, Write::Again, out)
    }

    // Whether the member's file, every byte of it in and none damaged,
    // fails the CRC32 its object's headers state for it. Every such member
    // is checked, from the CRC the leaf folded as its writes landed, which
    // a rewrite, a cut and a restore keep. A missing fold queues a worker
    // and keeps a sparse durable obligation; nothing is read back here. A
    // member with no stated checksum, or one already short or damaged, has
    // nothing more for the check to say.
    fn fails_its_checksum(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<bool, WriteError> {
        let Some(record) = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
        else {
            return Ok(false);
        };
        let (Some(stated), Some(length)) = (record.crc32, record.length) else {
            return Ok(false);
        };
        let whole = ByteRange::new(0, length);
        if !record.coverage.covered(whole) || record.coverage.damaged(whole) {
            // Nothing to check yet, but an arrival may still clean it: the
            // obligation waits for a verdict or for the namespace's end.
            self.verifications.insert((node, member));
            return Ok(false);
        }
        let Some(leaf) = self
            .files
            .get(&node)
            .and_then(|files| files.leaves.get(&member))
        else {
            return Ok(false);
        };
        match leaf.folded(length, Algorithm::Crc32) {
            Some(actual) => {
                self.verifications.remove(&(node, member));
                Ok(actual != u64::from(stated))
            }
            None => {
                self.verifications.insert((node, member));
                Ok(false)
            }
        }
    }

    // The member's file reaches `end`, by a write or by taking its length.
    fn admit_file(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        end: u64,
    ) -> Result<(), WriteError> {
        self.caps
            .admit_write(tree, (node, member), end, self.free_disk.current())?;
        Ok(())
    }

    fn retract(&mut self, tree: &Tree, node: NodeId, member: MemberId) -> Result<(), WriteError> {
        self.take_held(node, member, ALL)?;
        if let Some(leaf) = self
            .files
            .get_mut(&node)
            .and_then(|files| files.leaves.remove(&member))
        {
            self.discard(leaf)?;
        }
        self.caps.forget(tree, node, member);
        Ok(())
    }

    // Forget what the tree dropped: the envelopes of every node nothing
    // reads back through, whether or not it ever wrote a file, and the file
    // of every member no namespace holds, with the namespace's own files
    // once the namespace is gone too. A namespace retired or gone holds no
    // entries and keeps nothing resident.
    fn prune(&mut self, tree: &Tree) -> Result<(), WriteError> {
        self.caps.release_gone(tree);
        let shelved: Vec<NodeId> = self.envelopes.nodes().collect();
        for node in shelved {
            if tree.buried(node) {
                self.envelopes.forget(node)?;
            }
        }
        let nodes: Vec<NodeId> = self.files.keys().copied().collect();
        for node in nodes {
            let Some(namespace) = tree.namespace(node) else {
                // Dropping the namespace's holds gives their count back.
                if let Some(files) = self.files.remove(&node) {
                    for member in files.leaves.keys() {
                        self.caps.forget(tree, node, *member);
                    }
                    for leaf in files.leaves.into_map().into_values().chain(files.scratch) {
                        self.discard(leaf)?;
                    }
                }
                continue;
            };
            let gone: Vec<MemberId> = self.files[&node]
                .leaves
                .keys()
                .copied()
                .filter(|member| {
                    namespace.get(*member).is_none() || tree.buried_member(node, *member)
                })
                .collect();
            for member in gone {
                self.take_held(node, member, ALL)?;
                let files = self.files_mut(node);
                files.unrefuse(member);
                if let Some(leaf) = files.leaves.remove(&member) {
                    self.discard(leaf)?;
                }
                self.caps.forget(tree, node, member);
            }
        }
        Ok(())
    }

    // An object took the member over: the one place a binding is known.
    fn bound(&self, tree: &Tree, node: NodeId, member: MemberId, kind: Kind, child: NodeId) {
        tracing::info!(
            job_id = self.scope,
            node = child.0,
            kind = %kind,
            over = %member_name(tree, node, member),
            depth = tree.depth(node),
            "object bound"
        );
    }

    // The member is a leaf because something gave it back or never took
    // it. Said once, when the reason is new.
    fn refuse(&mut self, tree: &Tree, node: NodeId, member: MemberId, why: Why) {
        if self.files_mut(node).refuse(member, why) == Some(why) {
            return;
        }
        // The object that gave the member back, if one had it.
        let kind = tree
            .refused_by(node, member)
            .and_then(|object| tree.kind(object))
            .map(|kind| kind.to_string())
            .unwrap_or_default();
        tracing::info!(
            job_id = self.scope,
            node = node.0,
            kind = %kind,
            member = %member_name(tree, node, member),
            why = %why_name(why),
            "object refused"
        );
    }

    fn files_mut(&mut self, node: NodeId) -> &mut Files {
        let namespace = HoldsNamespace {
            scope: self.scope,
            node,
        };
        let accountant = &self.accountant;
        self.files
            .get_or_insert_with(node, || Files::new(namespace, Arc::clone(accountant)))
    }

    fn leaf(
        &mut self,
        node: NodeId,
        member: MemberId,
        output_name: &str,
    ) -> Result<&mut Leaf, WriteError> {
        let opened = self
            .files
            .get(&node)
            .is_some_and(|files| files.leaves.contains_key(&member));
        if !opened {
            let leaf = self.new_leaf(node, member, output_name);
            self.files_mut(node).leaves.insert(member, leaf);
        }
        Ok(self
            .files_mut(node)
            .leaves
            .get_mut(&member)
            .expect("opened above"))
    }

    // The free space the holds budgets judge a page-out against.
    fn free_disk_bytes(&self) -> u64 {
        self.free_disk
            .current()
            .best_available_bytes()
            .unwrap_or(u64::MAX)
    }

    // The node's holds scratch, created empty the first time anything pages,
    // unless a restore adopted it as it was left. It lives under the
    // directory finalize never delivers.
    fn scratch(&mut self, node: NodeId) -> Result<&mut Leaf, WriteError> {
        let dir = self.working_dir.clone();
        let files = self.files_mut(node);
        if files.scratch.is_none() {
            let mut scratch = Leaf::at(&dir, &scratch_name(node));
            scratch.truncate()?;
            files.scratch = Some(scratch);
        }
        Ok(files.scratch.as_mut().expect("created above"))
    }
}

pub(crate) struct Placing<'a> {
    builder: &'a mut dyn Builder,
    writer: &'a NamespaceWriter,
}

impl Builder for Placing<'_> {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        self.builder.build(kind, id)
    }

    fn read(
        &self,
        tree: &Tree,
        node: NodeId,
        volume: VolumeIndex,
        placement: &Placement,
        into: &mut Vec<u8>,
    ) -> bool {
        let Some((parent, member)) = tree
            .volumes(node)
            .and_then(|(parent, volumes)| Some((parent, *volumes.get(&volume)?)))
        else {
            return false;
        };
        let access = self.writer.access(tree);
        let start = into.len();
        let read = read_placed(&access, (parent, member), node, placement, into);
        if !read {
            into.truncate(start);
        }
        read
    }
}

// Where a read of a namespace's members finds their bytes. A read follows
// the member to wherever they are: its leaf, the bytes still held for it,
// or through the inverse map of the object that owns it, into that object's
// members and envelopes, as deep as the tree goes. The live tree answers on
// the loop; a repair lent off it answers from what it took along.
pub(crate) trait Source {
    // The data object the member is bound to, and the volume it is of it.
    // A recovery volume is in its own file, so it is bound to none.
    fn bound(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)>;
    // The bytes posted for a range of the object's volume, rebuilt as the
    // object says from what `read` hands back per placement.
    fn posted(
        &self,
        child: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool;
    fn envelope(&self, child: NodeId, envelope: u32, range: ByteRange, into: &mut Vec<u8>) -> bool;
    // Whether the object at `child` is a recovery object, whose volume stays
    // in its member's own file.
    fn recovers(&self, child: NodeId) -> bool;
    // Where a range of the volume of the object at `child` lies in the
    // member it is bound to; `None` past the window it is bound to.
    fn in_member(&self, child: NodeId, range: ByteRange) -> Option<ByteRange>;
    // The member's own file, for a range its coverage says was written.
    fn leaf(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool;
    // The parts of a range still held for the member, in offset order and
    // left held, and where each is kept: a member whose object has not
    // placed them yet, or one whose map became known before they went to
    // its placements.
    fn held(&self, node: NodeId, member: MemberId, range: ByteRange) -> Vec<(ByteRange, Location)>;
    // Bytes of the member's held parts kept on scratch.
    fn scratch(
        &self,
        node: NodeId,
        member: MemberId,
        offset: u64,
        len: u64,
        into: &mut Vec<u8>,
    ) -> bool;
}

pub(crate) fn read_member(
    source: &dyn Source,
    node: NodeId,
    member: MemberId,
    range: ByteRange,
    into: &mut Vec<u8>,
) -> bool {
    let Some((child, volume)) = source.bound(node, member) else {
        return read_own(source, node, member, range, into);
    };
    // The object rebuilds its volume's bytes from its placements: one that
    // deciphers puts the cipher back.
    let mut read = |placement: &Placement, into: &mut Vec<u8>| {
        read_placed(source, (node, member), child, placement, into)
    };
    // What the object cannot rebuild is read from the member's own file
    // where that has it: a volume given back stays in its member's file,
    // and an object that takes it again reads it there in place.
    let mut posted = |range: ByteRange, into: &mut Vec<u8>| {
        let start = into.len();
        source.posted(child, volume, range, &mut read, into) || {
            into.truncate(start);
            source.leaf(node, member, range, into)
        }
    };
    let mut at = range.start;
    let pieces = source.held(node, member, range);
    for group in pieces.chunk_by(|(a, _), (b, _)| a.end == b.start) {
        let held = ByteRange::new(group[0].0.start, group[group.len() - 1].0.end);
        if at < held.start && !posted(ByteRange::new(at, held.start), into) {
            return false;
        }
        // Held custody is current authority, even if its backing cannot be
        // read. An older derived copy must not substitute for that failure.
        if !read_held(source, (node, member), group, held, into) {
            return false;
        }
        at = held.end;
    }
    at == range.end || posted(ByteRange::new(at, range.end), into)
}

// What one placement of the object at `child`, bound to `member` of `node`,
// holds.
fn read_placed(
    source: &dyn Source,
    (node, member): (NodeId, MemberId),
    child: NodeId,
    placement: &Placement,
    into: &mut Vec<u8>,
) -> bool {
    match placement {
        Placement::Member { member, range } => read_member(source, child, *member, *range, into),
        // A recovery object's volume stays in its member's own file: its
        // envelope is that file, at the volume's offsets in the member.
        Placement::Envelope { range, .. } if source.recovers(child) => source
            .in_member(child, *range)
            .is_some_and(|within| read_own(source, node, member, within, into)),
        Placement::Envelope { envelope, range } => source.envelope(child, *envelope, *range, into),
        // What the object could not place was held for the member, and once
        // released it is in the member's own file.
        Placement::Held { range } => read_own(source, node, member, *range, into),
    }
}

// A member no object reads: its bytes are in its own file, or still held
// while its front has not said what it is.
fn read_own(
    source: &dyn Source,
    node: NodeId,
    member: MemberId,
    range: ByteRange,
    into: &mut Vec<u8>,
) -> bool {
    let start = into.len();
    if source.leaf(node, member, range, into) {
        return true;
    }
    into.truncate(start);
    let pieces = source.held(node, member, range);
    read_held(source, (node, member), &pieces, range, into)
}

// The held pieces of a range, read where each is kept.
fn read_held(
    source: &dyn Source,
    (node, member): (NodeId, MemberId),
    pieces: &[(ByteRange, Location)],
    range: ByteRange,
    into: &mut Vec<u8>,
) -> bool {
    read_pieces(
        pieces.iter().map(|(piece, location)| (*piece, location)),
        range,
        into,
        |offset, len, into| source.scratch(node, member, offset, len, into),
    )
}

// The held pieces of a range from its start, as long as they run on, read
// with `piece`; whole, or nothing and `false`.
fn read_pieces<'p>(
    pieces: impl IntoIterator<Item = (ByteRange, &'p Location)>,
    range: ByteRange,
    into: &mut Vec<u8>,
    mut scratch: impl FnMut(u64, u64, &mut Vec<u8>) -> bool,
) -> bool {
    let start = into.len();
    let mut at = range.start;
    for (piece, location) in pieces {
        if piece.start != at {
            break;
        }
        let read = match location {
            Location::Ram(bytes) => {
                into.extend_from_slice(bytes);
                true
            }
            Location::Scratch { offset, len } => scratch(*offset, *len, into),
        };
        if !read {
            break;
        }
        at = piece.end;
    }
    let whole = at == range.end;
    if !whole {
        into.truncate(start);
    }
    whole
}

// The namespace's own face toward its members, on the loop.
pub(crate) struct Access<'a> {
    writer: &'a NamespaceWriter,
    tree: &'a Tree,
    // Whether bytes still held count as read. A repair reads them; what is
    // on disk, which a restart reads back, does not hold them.
    held: bool,
}

// A repair's scratch directory goes with the repair. One left behind by a
// crash is cleared before the next is lent, and lives under the directory
// finalize never delivers.
fn remove_scratch(dir: &Path) {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::warn!(dir = %dir.display(), %error, "recovery scratch left behind"),
    }
}

impl Access<'_> {
    // The range of the member appended to `into`, or nothing and `false`.
    pub(crate) fn read_at(
        &self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        let start = into.len();
        let read = read_member(self, node, member, range, into);
        if !read {
            into.truncate(start);
        }
        read
    }
}

impl Source for Access<'_> {
    fn bound(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
        self.tree
            .child_of(node, member)
            .filter(|(child, _)| !self.tree.recovers(*child))
    }

    fn recovers(&self, child: NodeId) -> bool {
        self.tree.recovers(child)
    }

    fn in_member(&self, child: NodeId, range: ByteRange) -> Option<ByteRange> {
        self.tree.in_member(child, range)
    }

    fn posted(
        &self,
        child: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        self.tree.posted(child, volume, range, read, into)
    }

    fn envelope(&self, child: NodeId, envelope: u32, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.writer.envelopes.read(child, envelope, range, into)
    }

    fn leaf(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        let written = self
            .tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|record| record.coverage.covered(range));
        let Some(leaf) = self
            .writer
            .files
            .get(&node)
            .and_then(|files| files.leaves.get(&member))
        else {
            return range.is_empty();
        };
        written && leaf.read(range.start, index(range.len()), into).is_ok()
    }

    fn held(&self, node: NodeId, member: MemberId, range: ByteRange) -> Vec<(ByteRange, Location)> {
        if !self.held {
            return Vec::new();
        }
        self.writer
            .files
            .get(&node)
            .map(|files| {
                files
                    .holds
                    .peek(held_volume(member), range)
                    .into_iter()
                    .map(|piece| (piece.range, piece.location))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn scratch(
        &self,
        node: NodeId,
        _: MemberId,
        offset: u64,
        len: u64,
        into: &mut Vec<u8>,
    ) -> bool {
        self.writer
            .files
            .get(&node)
            .and_then(|files| files.scratch.as_ref())
            .is_some_and(|scratch| scratch.read(offset, index(len), into).is_ok())
    }
}

// Holds are kept per namespace, keyed by member, which the ledger calls a
// volume: the member is the volume of whatever will own it.
// Every byte of a member.
const ALL: ByteRange = ByteRange {
    start: 0,
    end: u64::MAX,
};

// How much of a member is read back at once, so a large one never sits in
// memory whole.
const READ_BACK: u64 = 1 << 20;

// The name a member of the namespace carries now.
fn member_name(tree: &Tree, node: NodeId, member: MemberId) -> String {
    tree.namespace(node)
        .and_then(|namespace| namespace.get(member))
        .map(|record| record.name.clone())
        .unwrap_or_default()
}

// Whether the member lives in its own file: a leaf, or a recovery volume.
fn in_own_file(tree: &Tree, node: NodeId, member: MemberId) -> bool {
    tree.child_of(node, member)
        .is_none_or(|(child, _)| tree.recovers(child))
}

// The node's holds scratch file, under the working directory.
fn scratch_name(node: NodeId) -> String {
    format!(
        "{}/graph-holds-{}.scratch",
        weaver_graph::names::INTERNAL_DIR,
        node.0
    )
}

fn held_volume(member: MemberId) -> VolumeIndex {
    VolumeIndex(member.0)
}

// The member whose bytes a held volume holds.
fn held_member(volume: VolumeIndex) -> MemberId {
    MemberId(volume.0)
}

#[cfg(test)]
fn crc32(bytes: &[u8]) -> u32 {
    crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32
}

fn index(offset: u64) -> usize {
    usize::try_from(offset).expect("an offset fits the address space")
}

mod claimed;
mod durable;
mod loan;
mod moves;
mod recovery;
mod refusal;
mod verification;

pub(crate) use verification::{Verification, Verified};

pub(crate) use loan::replay::{Replay, Replayed};
pub(crate) use loan::{Loan, Returned};
pub(crate) use recovery::{Coming, Lender, SetState};
#[cfg(test)]
mod tests;
