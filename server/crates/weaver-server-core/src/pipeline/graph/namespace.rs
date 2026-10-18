// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The namespace write path: the one place bytes delivered to a namespace are
// placed, held or refused. Every delivery to every namespace of a job comes
// through here, so the holds budgets, the damage marks and the recovery
// evidence are enforced once, for the root and for every namespace below it.
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
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::holds::{HoldLedger, HoldsAccountant, HoldsNamespace, Location, Placed};
use weaver_graph::member::FIRST_BYTES;
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    Builder, ByteRange, Effect, MemberId, NodeId, Placement, Provenance, RecoveryKind, Segment,
    Tree, VolumeIndex, Why,
};

use super::envelope::{Envelopes, Shelf};
use super::leaf::{Leaf, LeafError};
use crate::operations::disk::CapacityReader;

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

pub(crate) struct NamespaceWriter {
    // The job's process-wide id, which with a node id names a namespace to
    // the holds accountant that every job shares.
    scope: u64,
    working_dir: PathBuf,
    accountant: Arc<HoldsAccountant>,
    free_disk: CapacityReader,
    files: BTreeMap<NodeId, Files>,
    envelopes: Envelopes,
    // Member reads the recovery objects made while repairing.
    read_backs: u64,
    // Members carrying recovery data no object here reads, by namespace.
    unusable: Vec<(NodeId, MemberId)>,
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
            working_dir,
            accountant,
            free_disk,
            files: BTreeMap::new(),
            read_backs: 0,
            unusable: Vec::new(),
            renamed: Vec::new(),
            segments: Vec::new(),
            placements: Vec::new(),
        }
    }

    // How many member reads every repair so far needed: the evidence fed as
    // bytes passed settles the rest without one.
    pub(crate) fn read_backs(&self) -> u64 {
        self.read_backs
    }

    // Members whose object said they carry recovery data that no object
    // here can read: health says it is there, and no repair counts on it.
    pub(crate) fn unusable_recovery(&self) -> &[(NodeId, MemberId)] {
        &self.unusable
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

    pub(crate) fn leaf_path(&self, node: NodeId, member: MemberId) -> Option<&Path> {
        Some(self.files.get(&node)?.leaves.get(&member)?.path())
    }

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
        let record = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        let (output_name, length) = (record.output_name.clone(), record.length);
        if Leaf::is_directory(&output_name) {
            tree.settle_leaf(node, member);
            Leaf::at(&self.working_dir, &output_name)?.create_directory()?;
        } else if length == Some(0) {
            return self.close(tree, builder, segments, node, member, out);
        }
        self.bind_recoveries(tree, segments, node)
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
        let mut effects = Vec::new();
        tree.close(node, member, builder, &mut effects);
        self.apply_all(tree, builder, segments, effects, out)
    }

    // The member's length became known. Recovery objects match by length,
    // so they bind again.
    pub(crate) fn length_learned(
        &mut self,
        tree: &mut Tree,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        length: u64,
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace_mut(node)
            .and_then(|namespace| namespace.get_mut(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        if record.length.is_some() {
            return Ok(());
        }
        record.length = Some(length);
        self.bind_recoveries(tree, segments, node)
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
        for effect in effects {
            self.apply(tree, builder, segments, effect, out)?;
        }
        Ok(())
    }

    // A recovery object recognised the member: the tree's ledger moves its
    // name, and every file whose name changed follows.
    pub(crate) fn rename(
        &mut self,
        tree: &mut Tree,
        node: NodeId,
        member: MemberId,
        name: &str,
    ) -> Result<String, WriteError> {
        let mut effects = Vec::new();
        let output_name = tree
            .rename(node, member, name, &mut effects)
            .ok_or(WriteError::UnknownMember { node, member })?;
        self.moved(&effects)?;
        Ok(output_name)
    }

    // Output names the ledger changed: each file written under its old name
    // moves to its new one, in the order the ledger gave, so no file lands
    // on one still in place. A member never written has nothing to move.
    fn moved(&mut self, effects: &[Effect]) -> Result<(), WriteError> {
        for effect in effects {
            if let Effect::Renamed {
                node, member, new, ..
            } = effect
                && let Some(leaf) = self
                    .files
                    .get_mut(node)
                    .and_then(|files| files.leaves.get_mut(member))
            {
                leaf.rename(&self.working_dir, new)?;
            }
        }
        Ok(())
    }

    // Run every recovery object of the namespace. Each is lent out of the
    // tree for the call, so it reads the members and its own volumes
    // through the tree it lives in. The ranges it repaired come back through
    // the one write path as repaired bytes, so a leaf is overwritten and a
    // bound member's object takes them as any delivery.
    pub(crate) fn repair(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        out: &mut Vec<Effect>,
    ) -> Result<Vec<(NodeId, RepairOutcome)>, WriteError> {
        let mut outcomes = Vec::new();
        for recovery_node in tree.recovery_nodes(node) {
            let Some(mut object) = tree.take_object(recovery_node) else {
                continue;
            };
            let mut access = self.access(tree, node);
            access.recovery = Some(recovery_node);
            let outcome = object
                .recovery_mut()
                .map(|recovery| recovery.repair(&mut access));
            let (reads, repaired) = (access.reads, access.repaired);
            tree.put_object(recovery_node, object);
            self.read_backs += reads;
            outcomes.extend(outcome.map(|outcome| (recovery_node, outcome)));
            for (member, offset, bytes) in repaired {
                let arrival = Arrival {
                    node,
                    member,
                    offset,
                    bytes,
                    provenance: Provenance::Repaired,
                };
                self.deliver(tree, builder, segments, arrival, out)?;
            }
        }
        Ok(outcomes)
    }

    // The namespace's face toward its recovery objects.
    pub(crate) fn access<'a>(&'a self, tree: &'a Tree, node: NodeId) -> Access<'a> {
        Access {
            writer: self,
            tree,
            node,
            recovery: None,
            reads: 0,
            repaired: Vec::new(),
        }
    }

    // The evidence the grid folds: the segment checksums of the range, to
    // every recovery object that covers the member. Fresh bytes carry the
    // source's checksums as they came. Any other bytes are not the source's,
    // and nor are bytes the source has no checksums for, as in a namespace
    // below the root: they carry checksums of what is in hand, and are
    // evidence unless they are damage.
    fn feed(&mut self, tree: &mut Tree, segments: &dyn Segments, arrival: &Arrival) {
        let member = arrival.member;
        let covering: Vec<NodeId> = tree
            .recovery_nodes(arrival.node)
            .into_iter()
            .filter(|node| {
                tree.recovery(*node)
                    .is_some_and(|recovery| recovery.covers(member))
            })
            .collect();
        if covering.is_empty() {
            return;
        }
        let range = arrival.range();
        self.segments.clear();
        segments.segments_for(
            arrival.node,
            VolumeIndex(member.0),
            range,
            &mut self.segments,
        );
        let sourced = !self.segments.is_empty();
        if !sourced {
            // One checksum of the whole range, filled in below.
            self.segments.push(Segment {
                range,
                crc32: 0,
                verified: false,
            });
        }
        if arrival.provenance != Provenance::Fresh || !sourced {
            let verified = arrival.provenance != Provenance::Damaged;
            self.segments.retain_mut(|segment| {
                let Some(piece) = segment.range.intersect(range) else {
                    return false;
                };
                let start = index(piece.start - range.start);
                let end = index(piece.end - range.start);
                *segment = Segment {
                    range: piece,
                    crc32: crc32(&arrival.bytes[start..end]),
                    verified,
                };
                true
            });
        }
        let evidence = &self.segments;
        for node in covering {
            tree.with_recovery(node, |recovery, _| recovery.feed(member, evidence));
        }
    }

    // What arrived for the member before any recovery object covered it, as
    // the source's checksums of each run the coverage holds. Fresh runs are
    // evidence; a damaged run's checksums are of the bytes the source
    // delivered, which are the bytes written, so they mark its slices. A
    // repaired run's bytes are not the source's, so it gives nothing here
    // and its slices are read back if a repair needs them; so does every
    // run of a namespace the source has no checksums for.
    fn early_evidence(
        &mut self,
        tree: &Tree,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
    ) {
        self.segments.clear();
        let Some(record) = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
        else {
            return;
        };
        let mut found = Vec::new();
        for run in record.coverage.runs() {
            let verified = match run.provenance {
                Provenance::Fresh => true,
                Provenance::Damaged => false,
                Provenance::Repaired => continue,
            };
            found.clear();
            segments.segments_for(node, VolumeIndex(member.0), run.range, &mut found);
            self.segments.extend(
                found
                    .iter()
                    .filter(|segment| run.range.covers(segment.range))
                    .map(|segment| Segment {
                        verified,
                        ..*segment
                    }),
            );
        }
    }

    // Let every recovery object of the namespace match its members again,
    // after the members, their lengths or their first bytes changed. A
    // member a recovery object covers for the first time is fed everything
    // that arrived for it before, so early segments are not lost. One
    // recognised by content was renamed in the namespace, and its file
    // follows the new name.
    fn bind_recoveries(
        &mut self,
        tree: &mut Tree,
        segments: &dyn Segments,
        node: NodeId,
    ) -> Result<(), WriteError> {
        let recovery_nodes = tree.recovery_nodes(node);
        if recovery_nodes.is_empty() {
            return Ok(());
        }
        let members: Vec<MemberId> = tree
            .namespace(node)
            .map(|namespace| namespace.members().map(|member| member.id).collect())
            .unwrap_or_default();
        let mut renamed = std::mem::take(&mut self.renamed);
        renamed.clear();
        for recovery_node in recovery_nodes {
            let newly = tree
                .with_recovery(recovery_node, |recovery, namespace| {
                    let before: Vec<bool> = members
                        .iter()
                        .map(|member| recovery.covers(*member))
                        .collect();
                    recovery.bind(namespace, &mut renamed);
                    members
                        .iter()
                        .zip(before)
                        .filter(|(member, was)| !was && recovery.covers(**member))
                        .map(|(member, _)| *member)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            for member in newly {
                self.early_evidence(tree, segments, node, member);
                let evidence = &self.segments;
                if !evidence.is_empty() {
                    tree.with_recovery(recovery_node, |recovery, _| {
                        recovery.feed(member, evidence)
                    });
                }
            }
        }
        // A member the recovery renamed in its namespace wants that name
        // now; the tree's ledger places it.
        let mut effects = Vec::new();
        for member in &renamed {
            let wanted = tree
                .namespace(node)
                .and_then(|namespace| namespace.get(*member))
                .map(|record| record.name.clone());
            if let Some(wanted) = wanted {
                tree.rename(node, *member, &wanted, &mut effects);
            }
        }
        self.renamed = renamed;
        self.moved(&effects)
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
        // The object the bytes reach keeps what it placed even when they
        // make it give up: its refusal reads them back from it. That is the
        // object the member was bound to, or the one this delivery bound it
        // to, which may have given it up before the delivery returned.
        let reached = tree.child_of(node, member);
        let mut effects = Vec::new();
        tree.deliver(
            node,
            member,
            arrival.offset,
            arrival.bytes.clone(),
            arrival.provenance,
            builder,
            &mut effects,
        );
        let recovery_volume = tree
            .child_of(node, member)
            .is_some_and(|(child, _)| tree.recovers(child));
        if recovery_volume {
            // A recovery object's volume stays in its member's own file,
            // where a repair reads its blocks back.
            self.write_file(tree, node, member, arrival.offset, &arrival.bytes)?;
        } else if let Some(owner) = reached
            .or_else(|| tree.retired(node, member))
            .or_else(|| tree.child_of(node, member))
        {
            self.capture_below(tree, builder, owner, &arrival, &mut effects)?;
        }
        for effect in effects {
            self.apply(tree, builder, segments, effect, out)?;
        }
        self.release_placed(tree, builder, segments, out)?;
        // The front decides content matches, a finished member has the
        // length matches need, and a recovery volume's bytes may describe
        // more of the members.
        let front = arrival.offset < FIRST_BYTES as u64;
        let complete = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|record| record.is_complete());
        if recovery_volume || front || complete {
            self.bind_recoveries(tree, segments, node)?;
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
        let kept = self
            .envelopes
            .capture(child, &placements, &arrival.bytes, arrival.provenance);
        let mut breached = Vec::new();
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
            builder,
            effects,
        );
        for (offset, bytes) in breached {
            self.write_file(tree, arrival.node, arrival.member, offset, &bytes)?;
        }
        Ok(())
    }

    // What the object kept of the bytes, and at every depth below it. The
    // pieces it placed in a member that is another object's volume reached
    // that object as its emits, never through here, so they are followed
    // down as `route` takes the root's: what the object below kept or could
    // not place yet is captured for it, and a recovery object's volume is
    // written to its member's own file, where a repair reads its blocks.
    fn capture_below(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        (child, volume): (NodeId, VolumeIndex),
        arrival: &Arrival,
        effects: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.capture(tree, builder, (child, volume), arrival, effects)?;
        let mut placements = Vec::new();
        tree.locate(child, volume, arrival.range(), &mut placements);
        let mut at = 0;
        for placement in placements {
            let piece = at..at + index(placement.len());
            at = piece.end;
            let Placement::Member { member, range } = placement else {
                continue;
            };
            let Some(owner) = tree.child_of(child, member) else {
                continue;
            };
            let Some(bytes) = arrival
                .bytes
                .get(piece.clone())
                .map(|_| arrival.bytes.slice(piece))
            else {
                continue;
            };
            if tree.recovers(owner.0) {
                self.write_file(tree, child, member, range.start, &bytes)?;
                continue;
            }
            let below = Arrival {
                node: child,
                member,
                offset: range.start,
                bytes,
                provenance: arrival.provenance,
            };
            self.capture_below(tree, builder, owner, &below, effects)?;
        }
        Ok(())
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
                return self.write_leaf(tree, arrival, out);
            }
            Effect::Announced { node, member } => {
                self.announced(tree, builder, segments, *node, *member, out)?;
                self.release_placed(tree, builder, segments, out)?;
            }
            Effect::Closed { node, member } => {
                // A member that ended before its front could say what it is
                // is a leaf, whoever closed it: the root's source or an
                // object's emit.
                if tree.is_undecided(*node, *member) {
                    tree.settle_leaf(*node, *member);
                    self.release_to_leaf(tree, *node, *member)?;
                }
                // A leaf and a recovery volume live in their own files; a
                // member another object owns has no file to close.
                let in_own_file = tree
                    .child_of(*node, *member)
                    .is_none_or(|(child, _)| tree.recovers(child));
                if in_own_file {
                    self.close_leaf(tree, *node, *member)?;
                }
                self.bind_recoveries(tree, segments, *node)?;
            }
            Effect::Bound {
                node,
                member,
                child,
                ..
            } => {
                self.replay(tree, builder, segments, *node, *member, ALL, out)?;
                if tree.recovers(*child) {
                    self.bind_recoveries(tree, segments, *node)?;
                }
            }
            Effect::Refused { node, member, why } | Effect::NeedsEngine { node, member, why } => {
                self.files_mut(*node).refused.insert(*member, *why);
                self.materialise(tree, builder, segments, *node, *member, out)?;
                tree.bury(*node, *member);
                self.prune(tree)?;
            }
            Effect::Retracted { node, member } => {
                self.retract(*node, *member)?;
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
                let mut effects = Vec::new();
                tree.damaged_under_windows(*node, *member, *range, builder, &mut effects);
                self.apply_all(tree, builder, segments, effects, out)?;
            }
            Effect::Renamed { .. } => self.moved(std::slice::from_ref(&effect))?,
            Effect::NeedsPassword { .. } => {}
        }
        out.push(effect);
        Ok(())
    }

    // An object found its own recovery data at a range of its volume. The
    // recovery object for it is built over that range of the member the
    // volume is, as a window beside the object, so it lives among the
    // members it covers. What of the range has passed is read back through
    // the inverse map and handed to it; the rest reaches it as it passes.
    // Data no object here reads is only recorded.
    fn recovery_at(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, volume, range): (NodeId, VolumeIndex, ByteRange),
        kind: RecoveryKind,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let Some((parent, member)) = tree
            .volumes(node)
            .and_then(|(parent, volumes)| Some((parent, *volumes.get(&volume)?)))
        else {
            return Ok(());
        };
        let Some(object) = kind.object() else {
            if !self.unusable.contains(&(parent, member)) {
                self.unusable.push((parent, member));
            }
            return Ok(());
        };
        let runs: Vec<(ByteRange, Provenance)> = tree
            .namespace(parent)
            .and_then(|namespace| namespace.get(member))
            .map(|record| {
                record
                    .coverage
                    .overlapping(range)
                    .filter_map(|run| Some((run.range.intersect(range)?, run.provenance)))
                    .collect()
            })
            .unwrap_or_default();
        let access = self.access(tree, parent);
        let mut passed = Vec::new();
        for (piece, provenance) in runs {
            let mut bytes = Vec::new();
            if access.read_at(parent, member, piece, &mut bytes) {
                passed.push((piece.start, Bytes::from(bytes), provenance));
            }
        }
        let mut effects = Vec::new();
        let window = tree.attach_window(
            parent,
            (member, range),
            object,
            passed,
            builder,
            &mut effects,
        );
        self.apply_all(tree, builder, segments, effects, out)?;
        if window.is_some() {
            self.bind_recoveries(tree, segments, parent)?;
        }
        Ok(())
    }

    fn write_leaf(
        &mut self,
        tree: &mut Tree,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let (node, member) = (arrival.node, arrival.member);
        if tree.is_undecided(node, member) {
            return self.hold(tree, arrival, out);
        }
        self.release_to_leaf(tree, node, member)?;
        self.write_file(tree, node, member, arrival.offset, &arrival.bytes)
    }

    // Hold bytes whose member is not yet decided. A breach decides it a
    // leaf: everything held and the new bytes go to its file.
    fn hold(
        &mut self,
        tree: &mut Tree,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let (node, member) = (arrival.node, arrival.member);
        if self.keep_held(node, member, arrival.offset, arrival.bytes.clone())? {
            tree.settle_leaf(node, member);
            self.files_mut(node)
                .refused
                .insert(member, Why::HoldsBreached);
            self.release_to_leaf(tree, node, member)?;
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
    // it once the member is bound, and what an object asks for again.
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
        for (offset, bytes) in self.take_held(node, member, range)? {
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
        }
        Ok(())
    }

    // Held bytes go down again once the object their member is a volume of
    // places them, at whatever depth: an object's map grows only as it
    // hears its volumes or announces a member, and either may come from an
    // object above it, so this runs over every hold after each. What just
    // arrived may be the headers that link a volume whose data came first.
    // What its object still cannot place stays held.
    fn release_placed(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
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
                    }
                }
            }
        }
        Ok(())
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
        let leaf = self.leaf(node, member, &record.output_name)?;
        leaf.write(offset, bytes, record.length)?;
        if let Some(length) = record.length.filter(|_| record.is_complete()) {
            leaf.close(length)?;
        }
        Ok(())
    }

    // The member is closed: its file takes the length the tree gave it.
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
        let output_name = record.output_name.clone();
        self.leaf(node, member, &output_name)?.close(length)?;
        Ok(())
    }

    fn retract(&mut self, node: NodeId, member: MemberId) -> Result<(), WriteError> {
        self.take_held(node, member, ALL)?;
        if let Some(leaf) = self
            .files
            .get_mut(&node)
            .and_then(|files| files.leaves.get_mut(&member))
        {
            leaf.truncate()?;
        }
        Ok(())
    }

    // Forget every namespace the tree dropped, with the files it wrote.
    fn prune(&mut self, tree: &Tree) -> Result<(), WriteError> {
        let gone: Vec<NodeId> = self
            .files
            .keys()
            .copied()
            .filter(|node| tree.namespace(*node).is_none())
            .collect();
        for node in gone {
            self.envelopes.forget(node)?;
            // Dropping the namespace's holds gives their count back.
            if let Some(files) = self.files.remove(&node) {
                for leaf in files.leaves.into_values().chain(files.scratch) {
                    leaf.remove()?;
                }
            }
        }
        Ok(())
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
        if !leaves.contains_key(&member) {
            leaves.insert(member, Leaf::at(&dir, output_name)?);
        }
        Ok(leaves.get_mut(&member).expect("inserted above"))
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

// A namespace's recovery objects read members and write repaired ranges
// through this. A read follows the member to wherever its bytes are: its
// leaf, or through the inverse map of the object that owns it, into that
// object's members and envelopes, as deep as the tree goes. Repaired ranges
// are collected and delivered when the repair returns, so they reach the
// file and the owning object by the same path as any other bytes.
pub(crate) struct Access<'a> {
    writer: &'a NamespaceWriter,
    tree: &'a Tree,
    node: NodeId,
    // The recovery object this access was lent to, whose volumes it reads.
    recovery: Option<NodeId>,
    reads: u64,
    repaired: Vec<(MemberId, u64, Bytes)>,
}

impl Access<'_> {
    fn read_at(
        &self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        // A recovery volume is in its own file; the one lent this access is
        // out of the tree, so the tree cannot say it recovers.
        let Some((child, volume)) = self
            .tree
            .child_of(node, member)
            .filter(|(child, _)| self.recovery != Some(*child) && !self.tree.recovers(*child))
        else {
            return self.read_leaf(node, member, range, into);
        };
        // The object rebuilds its volume's bytes from its placements: one
        // that deciphers puts the cipher back.
        let mut read = |placement: &Placement, into: &mut Vec<u8>| match placement {
            Placement::Member { member, range } => self.read_at(child, *member, *range, into),
            Placement::Envelope { envelope, range } => {
                self.writer.envelopes.read(child, *envelope, *range, into)
            }
            // What the object could not place was held for the member, and
            // once released it is in the member's own file.
            Placement::Held { range } => self.read_leaf(node, member, *range, into),
        };
        self.tree.posted(child, volume, range, &mut read, into)
    }

    fn read_leaf(
        &self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
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
}

impl MemberAccess for Access<'_> {
    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.reads += 1;
        let start = into.len();
        let read = self.read_at(self.node, member, range, into);
        if !read {
            into.truncate(start);
        }
        read
    }

    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        self.repaired
            .push((member, offset, Bytes::copy_from_slice(bytes)));
    }

    fn length(&self, member: MemberId) -> Option<u64> {
        self.tree.namespace(self.node)?.get(member)?.length
    }

    // The recovery object's volume is a member of this namespace, or a
    // window on one, read wherever the member's bytes are: its own file,
    // or back through the inverse map of the object that owns it.
    fn read_volume(&mut self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        let member = self.recovery.and_then(|recovery| {
            let (parent, volumes) = self.tree.volumes(recovery)?;
            let member = volumes.get(&volume).copied()?;
            let within = self.tree.in_member(recovery, range)?;
            (parent == self.node && within.len() == range.len()).then_some((member, within))
        });
        let Some((member, range)) = member else {
            return false;
        };
        let start = into.len();
        let read = self.read_at(self.node, member, range, into);
        if !read {
            into.truncate(start);
        }
        read
    }
}

// Holds are kept per namespace, keyed by member, which the ledger calls a
// volume: the member is the volume of whatever will own it.
// Every byte of a member.
const ALL: ByteRange = ByteRange {
    start: 0,
    end: u64::MAX,
};

fn held_volume(member: MemberId) -> VolumeIndex {
    VolumeIndex(member.0)
}

fn crc32(bytes: &[u8]) -> u32 {
    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32
}

fn index(offset: u64) -> usize {
    usize::try_from(offset).expect("an offset fits the address space")
}

mod durable;
mod refusal;
#[cfg(test)]
mod tests;
