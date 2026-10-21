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

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::Posted;
use weaver_graph::holds::{HoldLedger, HoldsAccountant, HoldsNamespace, Location, Placed};
use weaver_graph::member::FIRST_BYTES;
use weaver_graph::{
    Builder, ByteRange, Effect, Emit, Kind, MemberId, NodeId, Object, ObjectId, Placement,
    Provenance, Segment, Tree, VolumeIndex, Why,
};

use super::checkpoint::why_name;
use super::envelope::{Envelopes, Shelf};
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
    pub(crate) bytes: Bytes,
    pub(crate) provenance: Provenance,
}

impl Arrival {
    fn range(&self) -> ByteRange {
        ByteRange::at(self.offset, self.bytes.len() as u64)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WriteError {
    #[error(transparent)]
    Leaf(#[from] LeafError),
    #[error("member {member:?} of node {node:?} is not in its namespace")]
    UnknownMember { node: NodeId, member: MemberId },
    // The job's extraction budget refused it; nothing more of the job is
    // written.
    #[error(transparent)]
    Refused(#[from] ExtractionFailure),
}

// What one namespace has on disk and in hand.
#[derive(Debug)]
struct Files {
    leaves: BTreeMap<MemberId, Leaf>,
    holds: HoldLedger,
    // Where held bytes go when RAM runs short; opened on the first page-out.
    scratch: Option<Leaf>,
    // Why a member is a leaf when something refused to own it.
    refused: BTreeMap<MemberId, Why>,
}

impl Files {
    fn new(namespace: HoldsNamespace, accountant: Arc<HoldsAccountant>) -> Self {
        Self {
            leaves: BTreeMap::new(),
            holds: HoldLedger::new(namespace, accountant),
            scratch: None,
            refused: BTreeMap::new(),
        }
    }
}

// A run of a volume an object placed as its own: the volume range, and
// where in which envelope its bytes were kept.
#[derive(Clone, Copy)]
struct KeptRun {
    range: ByteRange,
    envelope: u32,
    at: u64,
}

pub(crate) struct NamespaceWriter {
    // The job's process-wide id, which with a node id names a namespace to
    // the holds accountant that every job shares.
    scope: u64,
    working_dir: PathBuf,
    accountant: Arc<HoldsAccountant>,
    free_disk: CapacityReader,
    caps: Caps,
    files: BTreeMap<NodeId, Files>,
    envelopes: Envelopes,
    // Where each object's own bytes were kept, by object and volume.
    kept: BTreeMap<(NodeId, VolumeIndex), Vec<KeptRun>>,
    // Member reads the recovery objects made while repairing.
    read_backs: u64,
    renamed: Vec<MemberId>,
    segments: Vec<Segment>,
    placements: Vec<Placement>,
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
            kept: BTreeMap::new(),
            working_dir,
            accountant,
            free_disk,
            caps: Caps::unbounded(),
            files: BTreeMap::new(),
            read_backs: 0,
            renamed: Vec::new(),
            segments: Vec::new(),
            placements: Vec::new(),
        }
    }

    // The job's extraction budget, which every write, announcement, name
    // and binding here is held to.
    pub(crate) fn capped(mut self, caps: Caps) -> Self {
        self.caps = caps;
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

    // Why the member is a leaf, when an object or the holds budgets refused
    // to own it.
    pub(crate) fn refused(&self, node: NodeId, member: MemberId) -> Option<Why> {
        self.files.get(&node)?.refused.get(&member).copied()
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
        self.arrive(tree, builder, segments, arrival, out)?;
        self.release_held(tree, builder, segments, out)
    }

    // Bytes for a member, by the one write path, inside a call that
    // releases the holds once it is done.
    fn arrive(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.feed(tree, segments, &arrival);
        self.route(tree, builder, segments, arrival, out)
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
            Leaf::at(&self.working_dir, &output_name)?.create_directory()?;
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
        self.apply_each(tree, builder, segments, effects, out)
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
        self.apply_each(tree, builder, segments, effects, out)?;
        self.release_held(tree, builder, segments, out)
    }

    fn apply_each(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        effects: Vec<Effect>,
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
        let folded: Vec<(NodeId, MemberId)> = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Folded { node, copy, .. } => Some((*node, *copy)),
                _ => None,
            })
            .collect();
        for effect in effects {
            self.apply(tree, builder, segments, effect, out)?;
        }
        for (node, member, child) in bound {
            self.hand_over(tree, builder, segments, (node, member), child, out)?;
        }
        for (node, copy) in folded {
            self.fold_over(tree, builder, segments, (node, copy), out)?;
        }
        Ok(())
    }

    // What a posting received before it folded into the member it repeats:
    // held, or in its own file when it was a leaf first. It goes down as
    // the member's bytes, each run with the provenance it arrived with, and
    // the posting's file goes: it is never a file of its own.
    fn fold_over(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, copy): (NodeId, MemberId),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.replay(tree, builder, segments, node, copy, ALL, out)?;
        self.replay_file(tree, builder, segments, node, copy, out)?;
        let files = self.files_mut(node);
        files.refused.remove(&copy);
        if let Some(leaf) = files.leaves.remove(&copy) {
            leaf.remove()?;
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
            self.replay(tree, builder, segments, node, member, ALL, out)?;
        }
        self.replay_file(tree, builder, segments, node, member, out)?;
        let mut effects = Vec::new();
        tree.handed_over(node, member, &mut self.placing(builder), &mut effects);
        self.apply_each(tree, builder, segments, effects, out)?;
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
        self.moved(&effects)?;
        Ok(output_name)
    }

    // Output names the ledger changed: each file written under its old name
    // moves to its new one, in the order the ledger gave, so no file lands
    // on one still in place. A member never written has nothing to move.
    fn moved(&mut self, effects: &[Effect]) -> Result<(), WriteError> {
        for effect in effects {
            let Effect::Renamed {
                node, member, new, ..
            } = effect
            else {
                continue;
            };
            self.caps.admit_name(new)?;
            if let Some(leaf) = self
                .files
                .get_mut(node)
                .and_then(|files| files.leaves.get_mut(member))
            {
                leaf.rename(&self.working_dir, new)?;
            }
        }
        Ok(())
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

    fn route(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let (node, member) = (arrival.node, arrival.member);
        let mut effects = Vec::new();
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
        self.apply_each(tree, builder, segments, effects, out)?;
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
    // for the member as it holds any bytes whose place is not known. A
    // breach of the holds budgets refuses the object, as it would refuse an
    // undecided member, and the bytes that breached go to the member's own
    // file, which it is now.
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
        tree.locate(child, volume, arrival.range(), &mut placements);
        // An object that gave the volume back in this delivery places none
        // of it. What no piece covers is held for the member, as any bytes
        // whose place is not known, and goes to its own file with the rest.
        let placed: u64 = placements.iter().map(Placement::len).sum();
        if placed < arrival.range().len() {
            placements.push(Placement::Held {
                range: ByteRange::new(arrival.offset + placed, arrival.range().end),
            });
        }
        let moved = self.unkept(tree, (child, volume), arrival.range());
        self.keep_runs((child, volume), arrival.offset, &placements);
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
        let mut breached = Vec::new();
        for (offset, bytes) in moved {
            if self.keep_held(arrival.node, arrival.member, offset, bytes.clone())? {
                breached.push((offset, bytes));
            }
        }
        for placement in &placements {
            let Placement::Held { range } = placement else {
                continue;
            };
            let Some(piece) = range.intersect(arrival.range()) else {
                continue;
            };
            let bytes = arrival
                .bytes
                .slice(index(piece.start - arrival.offset)..index(piece.end - arrival.offset));
            if self.keep_held(arrival.node, arrival.member, piece.start, bytes.clone())? {
                breached.push((piece.start, bytes));
            }
        }
        self.placements = placements;
        kept?;
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
            self.write_file(tree, arrival.node, arrival.member, offset, &bytes)?;
        }
        Ok(())
    }

    // The bytes of `volume` the object at `child` kept as its own that its
    // map now places elsewhere, read back from where they were kept, past
    // `arrival`, which is placed afresh. A header that came again, repaired,
    // says whose the bytes behind it are: they are held for the member again,
    // as any bytes whose place the namespace does not have, and go down where
    // the map now puts them.
    fn unkept(
        &mut self,
        tree: &Tree,
        (child, volume): (NodeId, VolumeIndex),
        arrival: ByteRange,
    ) -> Vec<(u64, Bytes)> {
        let Some(runs) = self.kept.get_mut(&(child, volume)) else {
            return Vec::new();
        };
        let mut moved = Vec::new();
        let mut stays = Vec::with_capacity(runs.len());
        let mut placements = Vec::new();
        for run in runs.drain(..) {
            for range in outside(run.range, arrival) {
                let at = run.at + (range.start - run.range.start);
                placements.clear();
                tree.locate(child, volume, range, &mut placements);
                let mut offset = range.start;
                for placement in &placements {
                    let piece = ByteRange::at(offset, placement.len());
                    let kept_at = at + (piece.start - range.start);
                    offset = piece.end;
                    let same = matches!(placement, Placement::Envelope { envelope, range }
                        if *envelope == run.envelope && range.start == kept_at);
                    if same {
                        stays.push(KeptRun {
                            range: piece,
                            envelope: run.envelope,
                            at: kept_at,
                        });
                        continue;
                    }
                    let mut bytes = Vec::new();
                    let read = self.envelopes.read(
                        child,
                        run.envelope,
                        ByteRange::at(kept_at, piece.len()),
                        &mut bytes,
                    );
                    if read {
                        moved.push((piece.start, Bytes::from(bytes)));
                    }
                }
            }
        }
        *runs = stays;
        moved
    }

    // The runs of a delivery at `offset` the object placed as its own.
    fn keep_runs(
        &mut self,
        (child, volume): (NodeId, VolumeIndex),
        offset: u64,
        placements: &[Placement],
    ) {
        let runs = self.kept.entry((child, volume)).or_default();
        let mut cursor = offset;
        for placement in placements {
            let range = ByteRange::at(cursor, placement.len());
            cursor = range.end;
            if let Placement::Envelope {
                envelope,
                range: kept,
            } = placement
            {
                runs.push(KeptRun {
                    range,
                    envelope: *envelope,
                    at: kept.start,
                });
            }
        }
        runs.sort_by_key(|run| run.range.start);
        runs.dedup_by(|next, run| {
            let joins = run.envelope == next.envelope
                && run.range.end == next.range.start
                && run.at + run.range.len() == next.at;
            if joins {
                run.range = ByteRange::new(run.range.start, next.range.end);
            }
            joins
        });
    }

    // The node's envelopes, shared with the run of an engine that reads
    // them as its volumes.
    pub(crate) fn shelf(&mut self, node: NodeId) -> Arc<Shelf> {
        self.envelopes.shelf(node)
    }

    fn apply(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        effect: Effect,
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
                return self.write_leaf(tree, builder, segments, arrival, out);
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
                // A recovery object's volume stays in its member's own
                // file, where a repair reads its blocks back; any other
                // object's keeps are captured for it.
                if tree.recovers(*child) {
                    return self.write_file(tree, *node, *member, *offset, bytes);
                }
                let arrival = Arrival {
                    node: *node,
                    member: *member,
                    offset: *offset,
                    bytes: bytes.clone(),
                    provenance: *provenance,
                };
                let mut effects = Vec::new();
                self.capture(tree, builder, (*child, *volume), &arrival, &mut effects)?;
                return self.apply_each(tree, builder, segments, effects, out);
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
                let in_own_file = tree
                    .child_of(*node, *member)
                    .is_none_or(|(child, _)| tree.recovers(child));
                if in_own_file {
                    self.close_leaf(tree, *node, *member)?;
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
                tree.bury(*node, *member);
                self.prune(tree)?;
            }
            Effect::GivenBack {
                node,
                member,
                from,
                volume,
            } => self.given_back(tree, (*node, *member), (*from, *volume))?,
            Effect::Retracted { node, member } => {
                self.retract(tree, *node, *member)?;
                self.prune(tree)?;
            }
            Effect::RecoveryAt {
                node,
                volume,
                range,
                kind,
            } => {
                self.recovery_at(
                    tree,
                    builder,
                    segments,
                    (*node, *volume, *range),
                    *kind,
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
                if tree.is_undecided(*node, *member) && tree.front_lost(*node, *member, *range) {
                    self.settle(tree, builder, segments, *node, *member, out)?;
                }
                let mut effects = Vec::new();
                tree.damaged_under_windows(
                    *node,
                    *member,
                    *range,
                    &mut self.placing(builder),
                    &mut effects,
                );
                self.apply_each(tree, builder, segments, effects, out)?;
            }
            Effect::Renamed { .. } => self.moved(std::slice::from_ref(&effect))?,
            Effect::NeedsPassword { .. } => {}
        }
        out.push(effect);
        Ok(())
    }

    // A member its object gave back is a leaf, and its file is what the
    // object kept of it, its envelope, which was always at the volume's own
    // offsets: the envelope's file is moved to the member's name, never
    // copied. What the object never placed is still held for the member and
    // goes into that file as held bytes go into any leaf's. Until an object
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
        if let Some(mut leaf) = self.envelopes.take(from, volume.0) {
            leaf.rename(&self.working_dir, &output_name)?;
            if let Some(stale) = self.files_mut(node).leaves.insert(member, leaf) {
                stale.remove()?;
            }
        }
        self.release_to_leaf(tree, node, member)?;
        Ok(())
    }

    fn write_leaf(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let (node, member) = (arrival.node, arrival.member);
        if tree.is_undecided(node, member) {
            return self.hold(tree, builder, segments, arrival, out);
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
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let (node, member) = (arrival.node, arrival.member);
        if self.keep_held(node, member, arrival.offset, arrival.bytes.clone())? {
            if let Some(owner) = self.settle(tree, builder, segments, node, member, out)? {
                let mut effects = Vec::new();
                self.capture(tree, builder, owner, &arrival, &mut effects)?;
                return self.apply_each(tree, builder, segments, effects, out);
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
        let free_disk = self
            .free_disk
            .current()
            .best_available_bytes()
            .unwrap_or(u64::MAX);
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
        self.apply_each(tree, builder, segments, effects, out)?;
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
            self.write_file(tree, node, member, offset, &bytes)?;
        }
        Ok(())
    }

    // What was held of `range` of the member goes down to its object, each
    // run with the provenance the coverage recorded when it arrived: all of
    // it once the member is bound, and what an object asks for again. One
    // hold leaves the ledger at a time, as it goes down: the ledger is the
    // record of what the object has not been handed, so whatever a piece
    // makes of the member, a refusal reading the object back among it, sees
    // the rest still held.
    #[allow(clippy::too_many_arguments)]
    fn replay(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
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
                self.replay_piece(tree, builder, segments, (node, member), piece, out)?;
            }
        }
        Ok(())
    }

    // One piece of what was held goes down, run by run of its coverage.
    fn replay_piece(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        (offset, bytes): (u64, Bytes),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let range = ByteRange::at(offset, bytes.len() as u64);
        let runs: Vec<(ByteRange, Provenance)> = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| {
                record
                    .coverage
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
                bytes: bytes.slice(start..end),
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

    // One pass over every hold: true when any went down.
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
            let members: Vec<MemberId> = tree
                .namespace(parent)
                .map(|namespace| namespace.members().map(|member| member.id).collect())
                .unwrap_or_default();
            for member in members {
                let Some((child, volume)) = tree.child_of(parent, member) else {
                    continue;
                };
                let held = self
                    .files
                    .get(&parent)
                    .map(|files| files.holds.held_ranges(held_volume(member)))
                    .unwrap_or_default();
                for range in held {
                    placements.clear();
                    tree.locate(child, volume, range, &mut placements);
                    if placements
                        .iter()
                        .any(|placement| !matches!(placement, Placement::Held { .. }))
                    {
                        self.replay(tree, builder, segments, parent, member, range, out)?;
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
        bytes: &[u8],
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        let end = offset.saturating_add(bytes.len() as u64);
        self.caps
            .admit_write(tree, (node, member), end, self.free_disk.current())?;
        let leaf = self.leaf(node, member, &record.output_name)?;
        leaf.write(offset, bytes, record.length)?;
        if let Some(length) = record.length.filter(|_| record.is_complete()) {
            leaf.close(length)?;
        }
        Ok(())
    }

    // The member is closed: its file takes the length the tree gave it. A
    // member none of whose bytes arrived has no file: a name is a file only
    // once something of it was written, so a file a recovery object
    // describes is one only when its repair writes it.
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
        if length > 0 && record.coverage.bytes_covered() == 0 {
            return Ok(());
        }
        let output_name = record.output_name.clone();
        self.leaf(node, member, &output_name)?.close(length)?;
        Ok(())
    }

    fn retract(&mut self, tree: &Tree, node: NodeId, member: MemberId) -> Result<(), WriteError> {
        self.take_held(node, member, ALL)?;
        if let Some(leaf) = self
            .files
            .get_mut(&node)
            .and_then(|files| files.leaves.get_mut(&member))
        {
            leaf.truncate()?;
        }
        self.caps.forget(tree, node, member);
        Ok(())
    }

    // Forget what the tree dropped: the envelopes of every node nothing
    // reads back through, and the file of every member no namespace holds,
    // with the namespace's own files once the namespace is gone too. A
    // namespace retired or gone holds no entries and keeps nothing resident.
    fn prune(&mut self, tree: &Tree) -> Result<(), WriteError> {
        self.caps.release_gone(tree);
        self.kept.retain(|(object, _), _| !tree.buried(*object));
        let nodes: Vec<NodeId> = self.files.keys().copied().collect();
        for node in nodes {
            if tree.buried(node) {
                self.envelopes.forget(node)?;
            }
            let Some(namespace) = tree.namespace(node) else {
                // Dropping the namespace's holds gives their count back.
                if let Some(files) = self.files.remove(&node) {
                    for member in files.leaves.keys() {
                        self.caps.forget(tree, node, *member);
                    }
                    for leaf in files.leaves.into_values().chain(files.scratch) {
                        leaf.remove()?;
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
                files.refused.remove(&member);
                if let Some(leaf) = files.leaves.remove(&member) {
                    leaf.remove()?;
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
        if self.files_mut(node).refused.insert(member, why) == Some(why) {
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
            .entry(node)
            .or_insert_with(|| Files::new(namespace, Arc::clone(accountant)))
    }

    fn leaf(
        &mut self,
        node: NodeId,
        member: MemberId,
        output_name: &str,
    ) -> Result<&mut Leaf, WriteError> {
        let dir = self.working_dir.clone();
        let leaves = &mut self.files_mut(node).leaves;
        match leaves.entry(member) {
            Entry::Occupied(leaf) => Ok(leaf.into_mut()),
            Entry::Vacant(free) => Ok(free.insert(Leaf::at(&dir, output_name)?)),
        }
    }

    // The node's holds scratch, created empty the first time anything pages.
    // It lives under the directory finalize never delivers.
    fn scratch(&mut self, node: NodeId) -> Result<&mut Leaf, WriteError> {
        let dir = self.working_dir.clone();
        let files = self.files_mut(node);
        if files.scratch.is_none() {
            let name = format!(".weaver-chunks/graph-holds-{}.scratch", node.0);
            let mut scratch = Leaf::at(&dir, &name)?;
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
    // The member's own file, for a range its coverage says was written.
    fn leaf(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool;
    // Bytes still held for the member, read where they are kept and left
    // held, for a member whose object has not placed them yet.
    fn held(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool;
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
    source.posted(child, volume, range, &mut read, into)
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
    source.held(node, member, range, into)
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

    fn held(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        if !self.held {
            return false;
        }
        let Some(files) = self.writer.files.get(&node) else {
            return range.is_empty();
        };
        let pieces = files.holds.peek(held_volume(member), range);
        read_pieces(
            pieces.iter().map(|piece| (piece.range, &piece.location)),
            range,
            into,
            |offset, len, into| {
                files
                    .scratch
                    .as_ref()
                    .is_some_and(|scratch| scratch.read(offset, index(len), into).is_ok())
            },
        )
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

fn held_volume(member: MemberId) -> VolumeIndex {
    VolumeIndex(member.0)
}

fn crc32(bytes: &[u8]) -> u32 {
    crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32
}

fn index(offset: u64) -> usize {
    usize::try_from(offset).expect("an offset fits the address space")
}

mod claimed;
mod durable;
mod loan;
mod recovery;
mod refusal;

pub(crate) use loan::{Loan, Returned};
pub(crate) use recovery::{Lender, SetState};
#[cfg(test)]
mod tests;

// The parts of `range` outside `cut`, in order.
fn outside(range: ByteRange, cut: ByteRange) -> impl Iterator<Item = ByteRange> {
    let before = ByteRange::new(range.start, range.end.min(cut.start).max(range.start));
    let after = ByteRange::new(range.start.max(cut.end).min(range.end), range.end);
    [before, after].into_iter().filter(|part| !part.is_empty())
}
