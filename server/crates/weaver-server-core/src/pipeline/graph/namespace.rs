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
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use weaver_graph::holds::{HoldLedger, HoldsAccountant, HoldsNamespace, Location, Placed};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    Builder, ByteRange, Effect, MemberId, NodeId, Placement, Provenance, Recovery, Segment, Tree,
    VolumeIndex, Why,
};

use super::envelope::Envelopes;
use super::leaf::{Leaf, LeafError};
use crate::operations::disk::CapacityReader;

// The block-aligned segment checksums of a delivered range, which the source
// computed while it had the bytes. Asked in the namespace's own coordinates,
// where member M is addressed as volume M: exactly the root's numbering, and
// a numbering every namespace has.
pub(crate) trait Segments {
    fn segments_for(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Segment>);
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

// Holds are counted per namespace across the whole process, and graph node
// ids repeat from job to job, so every namespace takes a number from here.
static NEXT_HOLDS_NAMESPACE: AtomicU64 = AtomicU64::new(1);

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
    fn new() -> Self {
        let id = NEXT_HOLDS_NAMESPACE.fetch_add(1, Ordering::Relaxed);
        Self {
            leaves: BTreeMap::new(),
            holds: HoldLedger::new(HoldsNamespace(id)),
            scratch: None,
            refused: BTreeMap::new(),
        }
    }
}

pub(crate) struct NamespaceWriter {
    working_dir: PathBuf,
    accountant: Arc<HoldsAccountant>,
    free_disk: CapacityReader,
    files: BTreeMap<NodeId, Files>,
    envelopes: Envelopes,
    // The recovery objects of each namespace, which cover members beside
    // them and are fed as their bytes pass.
    recoveries: BTreeMap<NodeId, Vec<Box<dyn Recovery + Send>>>,
    // The recovery objects the tree holds as nodes, bound to a member of the
    // namespace they cover, noted when they bind so a delivery need not scan
    // the namespace for them.
    recovery_nodes: BTreeMap<NodeId, Vec<NodeId>>,
    renamed: Vec<MemberId>,
    segments: Vec<Segment>,
    placements: Vec<Placement>,
}

impl NamespaceWriter {
    pub(crate) fn new(
        working_dir: PathBuf,
        accountant: Arc<HoldsAccountant>,
        free_disk: CapacityReader,
    ) -> Self {
        Self {
            working_dir,
            accountant,
            free_disk,
            files: BTreeMap::new(),
            envelopes: Envelopes::default(),
            recoveries: BTreeMap::new(),
            recovery_nodes: BTreeMap::new(),
            renamed: Vec::new(),
            segments: Vec::new(),
            placements: Vec::new(),
        }
    }

    pub(crate) fn add_recovery(&mut self, node: NodeId, recovery: Box<dyn Recovery + Send>) {
        self.recoveries.entry(node).or_default().push(recovery);
    }

    pub(crate) fn recoveries(&self, node: NodeId) -> &[Box<dyn Recovery + Send>] {
        self.recoveries.get(&node).map_or(&[], Vec::as_slice)
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
        self.route(tree, builder, arrival, out)
    }

    // A member was announced. A directory exists at once, and so does a
    // member that will never have a byte; neither has bytes to sniff.
    pub(crate) fn announced(
        &mut self,
        tree: &mut Tree,
        node: NodeId,
        member: MemberId,
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
            tree.settle_leaf(node, member);
            self.close_leaf(tree, node, member)?;
        }
        self.bind_recoveries(tree, node)
    }

    // Nothing more will arrive for the member. One still undecided is a
    // leaf now, and a leaf takes the length it reached.
    pub(crate) fn close(
        &mut self,
        tree: &mut Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), WriteError> {
        if tree.is_undecided(node, member) {
            tree.settle_leaf(node, member);
            self.release_to_leaf(tree, node, member)?;
        }
        if tree.child_of(node, member).is_none() {
            self.close_leaf(tree, node, member)?;
        }
        Ok(())
    }

    // A recovery object recognised the member: the name allocation moves to
    // the new name, and the file follows.
    pub(crate) fn rename(
        &mut self,
        tree: &mut Tree,
        node: NodeId,
        member: MemberId,
        name: &str,
    ) -> Result<String, WriteError> {
        let output_name = tree
            .namespace_mut(node)
            .and_then(|namespace| namespace.rename(member, name))
            .ok_or(WriteError::UnknownMember { node, member })?;
        if let Some(leaf) = self
            .files
            .get_mut(&node)
            .and_then(|files| files.leaves.get_mut(&member))
        {
            leaf.rename(&self.working_dir, &output_name)?;
        }
        Ok(output_name)
    }

    // Run every recovery object of the namespace. The ranges they repaired
    // come back through the one write path as repaired bytes, so a leaf is
    // overwritten and a bound member's object takes them as any delivery.
    pub(crate) fn repair(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        out: &mut Vec<Effect>,
    ) -> Result<Vec<RepairOutcome>, WriteError> {
        let Some(mut recoveries) = self.recoveries.remove(&node) else {
            return Ok(Vec::new());
        };
        let mut access = self.access(tree, node);
        let outcomes = recoveries
            .iter_mut()
            .map(|recovery| recovery.repair(&mut access))
            .collect();
        let repaired = access.into_repaired();
        self.recoveries.insert(node, recoveries);
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
        Ok(outcomes)
    }

    // The namespace's face toward its recovery objects.
    pub(crate) fn access<'a>(&'a self, tree: &'a Tree, node: NodeId) -> Access<'a> {
        Access {
            writer: self,
            tree,
            node,
            repaired: Vec::new(),
        }
    }

    // The evidence the grid folds: the segment checksums of the range, to
    // every recovery object that covers the member. Only fresh bytes are
    // evidence. Bytes that are not fresh carry checksums of what is in hand,
    // since the source's were for bytes that are no longer there.
    fn feed(&mut self, tree: &mut Tree, segments: &dyn Segments, arrival: &Arrival) {
        let member = arrival.member;
        let beside = self
            .recoveries
            .get(&arrival.node)
            .is_some_and(|recoveries| recoveries.iter().any(|recovery| recovery.covers(member)));
        let nodes = self
            .recovery_nodes
            .get(&arrival.node)
            .map_or(&[][..], Vec::as_slice);
        let in_tree = nodes.iter().any(|node| {
            tree.with_recovery(*node, |recovery, _| recovery.covers(member))
                .unwrap_or(false)
        });
        if !beside && !in_tree {
            return;
        }
        let range = arrival.range();
        self.segments.clear();
        segments.segments_for(VolumeIndex(arrival.member.0), range, &mut self.segments);
        let fresh = arrival.provenance == Provenance::Fresh;
        self.segments.retain_mut(|segment| {
            segment.verified = fresh;
            if fresh {
                return true;
            }
            let Some(piece) = segment.range.intersect(range) else {
                return false;
            };
            let start = index(piece.start - range.start);
            let end = index(piece.end - range.start);
            segment.range = piece;
            segment.crc32 = crc32(&arrival.bytes[start..end]);
            true
        });
        let evidence = &self.segments;
        for recovery in self
            .recoveries
            .get_mut(&arrival.node)
            .into_iter()
            .flatten()
            .filter(|recovery| recovery.covers(member))
        {
            recovery.feed(member, evidence);
        }
        for node in nodes {
            tree.with_recovery(*node, |recovery, _| {
                if recovery.covers(member) {
                    recovery.feed(member, evidence);
                }
            });
        }
    }

    // Let every recovery object of the namespace match its members again,
    // after the members or their lengths changed. One recognised by content
    // was renamed in the namespace, and its file follows the new name.
    fn bind_recoveries(&mut self, tree: &mut Tree, node: NodeId) -> Result<(), WriteError> {
        let mut renamed = std::mem::take(&mut self.renamed);
        renamed.clear();
        if let (Some(recoveries), Some(namespace)) =
            (self.recoveries.get_mut(&node), tree.namespace_mut(node))
        {
            for recovery in recoveries {
                recovery.bind(namespace, &mut renamed);
            }
        }
        for recovery_node in self.recovery_nodes.get(&node).into_iter().flatten() {
            tree.with_recovery(*recovery_node, |recovery, namespace| {
                recovery.bind(namespace, &mut renamed)
            });
        }
        let moved = renamed.iter().try_for_each(|member| {
            let output_name = tree
                .namespace(node)
                .and_then(|namespace| namespace.get(*member))
                .map(|record| record.output_name.clone());
            let leaf = self
                .files
                .get_mut(&node)
                .and_then(|files| files.leaves.get_mut(member));
            match (output_name, leaf) {
                (Some(output_name), Some(leaf)) => leaf.rename(&self.working_dir, &output_name),
                _ => Ok(()),
            }
        });
        self.renamed = renamed;
        Ok(moved?)
    }

    fn route(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut effects = Vec::new();
        tree.deliver(
            arrival.node,
            arrival.member,
            arrival.offset,
            arrival.bytes.clone(),
            arrival.provenance,
            builder,
            &mut effects,
        );
        self.capture_envelopes(tree, &arrival);
        for effect in effects {
            self.apply(tree, builder, effect, out)?;
        }
        Ok(())
    }

    // The bytes a bound member's object kept for no member of its own.
    fn capture_envelopes(&mut self, tree: &Tree, arrival: &Arrival) {
        let Some((child, volume)) = tree.child_of(arrival.node, arrival.member) else {
            return;
        };
        self.placements.clear();
        tree.locate(child, volume, arrival.range(), &mut self.placements);
        self.envelopes
            .capture(child, &self.placements, &arrival.bytes);
    }

    fn apply(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
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
            Effect::Announced { node, member } => self.announced(tree, *node, *member)?,
            Effect::Closed { node, member } => {
                self.close_leaf(tree, *node, *member)?;
                self.bind_recoveries(tree, *node)?;
            }
            Effect::Bound {
                node,
                member,
                child,
                ..
            } => {
                let recovers = tree.with_recovery(*child, |_, _| ()).is_some();
                let known = self.recovery_nodes.entry(*node).or_default();
                if recovers && !known.contains(child) {
                    known.push(*child);
                }
                self.replay(tree, builder, *node, *member, out)?;
                if recovers {
                    self.bind_recoveries(tree, *node)?;
                }
            }
            Effect::Refused { node, member, why } | Effect::NeedsEngine { node, member, why } => {
                self.files_mut(*node).refused.insert(*member, *why);
                self.prune(tree)?;
            }
            Effect::Retracted { node, member } => {
                self.retract(*node, *member)?;
                self.prune(tree)?;
            }
            Effect::NeedsPassword { .. } | Effect::RecoveryAt { .. } | Effect::Damaged { .. } => {}
        }
        out.push(effect);
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
        let free_disk = self
            .free_disk
            .current()
            .best_available_bytes()
            .unwrap_or(u64::MAX);
        let outcome = {
            let accountant = Arc::clone(&self.accountant);
            let files = self.files_mut(node);
            files.holds.hold(
                held_volume(member),
                arrival.offset,
                arrival.bytes.clone(),
                &accountant,
                free_disk,
            )
        };
        for page_out in &outcome.page_outs {
            self.scratch(node)?
                .write(page_out.scratch_offset, &page_out.bytes, None)?;
        }
        if let Placed::Breached(_) = outcome.placed {
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

    // The member was decided as a leaf: what was held is written first, so
    // the bytes that arrived after it win where they overlap.
    fn release_to_leaf(
        &mut self,
        tree: &mut Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), WriteError> {
        for (offset, bytes) in self.take_held(node, member)? {
            self.write_file(tree, node, member, offset, &bytes)?;
        }
        Ok(())
    }

    // The member was bound: what was held goes down to its object, each
    // run with the provenance the coverage recorded when it arrived.
    fn replay(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        for (offset, bytes) in self.take_held(node, member)? {
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
                self.route(tree, builder, arrival, out)?;
            }
        }
        Ok(())
    }

    // Everything held for the member, in offset order, read back from
    // scratch where it was paged.
    fn take_held(
        &mut self,
        node: NodeId,
        member: MemberId,
    ) -> Result<Vec<(u64, Bytes)>, WriteError> {
        let Some(files) = self.files.get_mut(&node) else {
            return Ok(Vec::new());
        };
        let taken = files.holds.take(
            held_volume(member),
            ByteRange::new(0, u64::MAX),
            &self.accountant,
        );
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

    // The member is complete: its file takes its length, which for a member
    // that never declared one is how far its bytes reached.
    fn close_leaf(
        &mut self,
        tree: &mut Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace_mut(node)
            .and_then(|namespace| namespace.get_mut(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        let reached = record.coverage.runs().last().map_or(0, |run| run.range.end);
        let length = *record.length.get_or_insert(reached);
        let output_name = record.output_name.clone();
        self.leaf(node, member, &output_name)?.close(length)?;
        Ok(())
    }

    fn retract(&mut self, node: NodeId, member: MemberId) -> Result<(), WriteError> {
        self.take_held(node, member)?;
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
            .chain(self.recoveries.keys())
            .chain(self.recovery_nodes.keys())
            .copied()
            .filter(|node| tree.namespace(*node).is_none())
            .collect();
        for nodes in self.recovery_nodes.values_mut() {
            nodes.retain(|node| tree.namespace(*node).is_some());
        }
        for node in gone {
            self.recovery_nodes.remove(&node);
            self.envelopes.forget(node);
            self.recoveries.remove(&node);
            if let Some(mut files) = self.files.remove(&node) {
                files.holds.discard(&self.accountant);
                for leaf in files.leaves.into_values().chain(files.scratch) {
                    leaf.remove()?;
                }
            }
        }
        Ok(())
    }

    fn files_mut(&mut self, node: NodeId) -> &mut Files {
        self.files.entry(node).or_insert_with(Files::new)
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

impl Drop for NamespaceWriter {
    // Held bytes are counted process-wide; a job that goes away gives its
    // count back even when it never placed them.
    fn drop(&mut self) {
        for files in self.files.values_mut() {
            files.holds.discard(&self.accountant);
        }
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
    repaired: Vec<(MemberId, u64, Bytes)>,
}

impl Access<'_> {
    pub(crate) fn into_repaired(self) -> Vec<(MemberId, u64, Bytes)> {
        self.repaired
    }

    fn read_at(
        &self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        let Some((child, volume)) = self.tree.child_of(node, member) else {
            return self.read_leaf(node, member, range, into);
        };
        let mut placements = Vec::new();
        self.tree.locate(child, volume, range, &mut placements);
        if placements.iter().map(Placement::len).sum::<u64>() != range.len() {
            return false;
        }
        placements.iter().all(|placement| match placement {
            Placement::Member { member, range } => self.read_at(child, *member, *range, into),
            Placement::Envelope { envelope, range } => {
                self.writer.envelopes.read(child, *envelope, *range, into)
            }
            Placement::Held { .. } => false,
        })
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
}

// Holds are kept per namespace, keyed by member, which the ledger calls a
// volume: the member is the volume of whatever will own it.
fn held_volume(member: MemberId) -> VolumeIndex {
    VolumeIndex(member.0)
}

fn crc32(bytes: &[u8]) -> u32 {
    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32
}

fn index(offset: u64) -> usize {
    usize::try_from(offset).expect("an offset fits the address space")
}

#[cfg(test)]
mod tests;
