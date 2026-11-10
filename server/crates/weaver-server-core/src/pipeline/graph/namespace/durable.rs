// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a checkpoint says of the writer, and the writer a restore puts back:
// which members have a file of their own and why a member is a leaf, the
// kept envelopes, the holds paged to scratch, how many member reads the
// repairs made, and the read of a root member back from wherever its bytes
// went. A hold in memory is not here: it is fetched again, so a graceful stop
// pages what it can to scratch first.

use std::collections::BTreeMap;

use weaver_graph::holds::{Reserve, ScratchHold};
use weaver_graph::{
    Builder, ByteRange, Coverage, Digest, Effect, MemberId, NodeId, Provenance, Segment, Tree,
    VolumeIndex, Why,
};

use super::{NamespaceWriter, Segments, WriteError, scratch_name};
use crate::pipeline::graph::checkpoint::{FileState, HoldRecord};
use crate::pipeline::graph::envelope::durable::KeptRecord;
use crate::pipeline::graph::leaf::Leaf;

impl NamespaceWriter {
    pub(crate) fn file_state(&self, node: NodeId, member: MemberId) -> FileState {
        let files = self.files.get(&node);
        let leaf = files.and_then(|files| files.leaves.get(&member));
        FileState {
            has_file: leaf.is_some(),
            on_disk: leaf
                .filter(|leaf| leaf.wants().is_some())
                .map(|leaf| leaf.name().to_owned()),
            refused: files.and_then(|files| files.refused.get(&member).copied()),
        }
    }

    // The generation of the writer's namespaces: a change to what a
    // namespace's members hold marks it under a later one. Its holds are
    // among what it holds: every hold, take and page-out reaches the ledger
    // through the marked map, so it marks the namespace too.
    pub(crate) fn files_generation(&self) -> u64 {
        self.files.marks().at()
    }

    // The namespaces whose members' states changed since generation `at`.
    pub(crate) fn files_changed_since(&self, at: u64) -> impl Iterator<Item = NodeId> + '_ {
        self.files.marks().since(at)
    }

    // The generation of the files of the node's members: a change to a
    // member's file, or to why it is a leaf, marks the member under a later
    // one.
    pub(crate) fn member_files_generation(&self, node: NodeId) -> u64 {
        self.files
            .get(&node)
            .map_or(0, |files| files.leaves.marks().at())
    }

    // The node's members whose files changed since generation `at`.
    pub(crate) fn member_files_changed_since(
        &self,
        node: NodeId,
        at: u64,
    ) -> impl Iterator<Item = MemberId> + '_ {
        self.files
            .get(&node)
            .into_iter()
            .flat_map(move |files| files.leaves.marks().since(at))
    }

    // The holds the node's namespace has on scratch and in its members'
    // spill files, each with whether it is in a spill file, once the job has
    // paged them out for a stop; none before, so no row names bytes a later
    // hold may replace. A spilled hold lies at its own offset, so its
    // scratch offset is that offset.
    pub(crate) fn hold_records(&self, node: NodeId) -> Vec<(ScratchHold, bool)> {
        if !self.stopped {
            return Vec::new();
        }
        let Some(files) = self.files.get(&node) else {
            return Vec::new();
        };
        let spilled = files
            .holds
            .spilled_holds()
            .into_iter()
            .map(|(volume, range)| {
                let held = ScratchHold {
                    volume,
                    range,
                    scratch_offset: range.start,
                };
                (held, true)
            });
        files
            .holds
            .scratch_holds()
            .into_iter()
            .map(|held| (held, false))
            .chain(spilled)
            .collect()
    }

    // A graceful stop: every namespace pages what it holds in RAM to
    // scratch, under the same budgets as any page-out, and what scratch
    // cannot take goes to its member's spill file as far as the disk
    // reserve leaves room, so the checkpoint after it records those holds
    // and a restart reads each back. The stop keeps the reserve for the
    // checkpoint and the database: what neither scratch nor the reserve
    // takes stays in RAM, unrecorded, and is fetched again after the
    // restart.
    pub(crate) fn page_out_holds(&mut self) -> Result<(), WriteError> {
        self.stopped = true;
        let nodes: Vec<NodeId> = self
            .files
            .iter()
            // A namespace whose holds are already on scratch or in a spill
            // file has nothing to page out, but the stop is what makes a
            // checkpoint record them, so it is marked with the rest.
            .filter(|(_, files)| {
                files.holds.resident_bytes() > 0
                    || !files.holds.scratch_holds().is_empty()
                    || !files.holds.spilled_holds().is_empty()
            })
            .map(|(node, _)| *node)
            .collect();
        let mut free_disk = self.free_disk_bytes();
        for node in nodes {
            let (page_outs, spills) = self.files_mut(node).holds.page_out_all(free_disk);
            for page_out in &page_outs {
                free_disk = free_disk.saturating_sub(page_out.range.len());
                self.scratch(node)?
                    .write(page_out.scratch_offset, &page_out.bytes, None)?;
            }
            for spill in &spills {
                free_disk = free_disk.saturating_sub(spill.range.len());
                self.spill_write(
                    node,
                    super::held_member(spill.volume),
                    spill.range.start,
                    &spill.bytes,
                )?;
            }
        }
        Ok(())
    }

    // The holds a checkpoint recorded, adopted where they lie: the node's
    // scratch file and its members' spill files are opened as they were
    // left, never emptied, and each hold goes back into its ledger. A hold
    // on scratch the budgets now refuse, under a lower ceiling say, moves to
    // its member's spill file when the disk reserve has room for the copy.
    // A hold that cannot be kept, one the reserve has no room for, one
    // whose move fails, one of a namespace or member the tree no longer
    // has, or one past the end of its file, is not adopted, so its article
    // reads as missing and is fetched again: a restore never fails for a
    // hold. Bytes the rows do not describe are never read.
    pub(crate) fn restore_holds(
        &mut self,
        tree: &Tree,
        records: &[HoldRecord],
    ) -> Result<(), WriteError> {
        // The space a move to a spill file may take: what is free now, as
        // the copy adds to the scratch file it leaves on disk.
        let mut spill_free = self.free_disk_bytes();
        // Every scratch byte a row names, adopted or not, is named by the
        // checkpoint the database holds: the ledger allocates past the end
        // of the last until one taken after this restore commits, as
        // `scratch_rewinding` says, and the file is opened as left.
        let mut by_node: BTreeMap<NodeId, (u64, Vec<(ScratchHold, bool)>)> = BTreeMap::new();
        for record in records {
            let Some(namespace) = tree.namespace(record.node) else {
                continue;
            };
            let (named, holds) = by_node.entry(record.node).or_default();
            if !record.on_file {
                let end = record
                    .held
                    .scratch_offset
                    .saturating_add(record.held.range.len());
                *named = (*named).max(end);
            }
            if namespace.get(MemberId(record.held.volume.0)).is_some() {
                holds.push((record.held, record.on_file));
            }
        }
        for (node, (named, holds)) in by_node {
            if named > 0 {
                self.files_mut(node).holds.keep_scratch_to(named);
            }
            let scratch = Leaf::found(&self.working_dir, &scratch_name(node))?;
            let written = scratch.len().unwrap_or(0);
            // The file's bytes are already on disk, so the disk reserve
            // judges them against the space they would leave if absent.
            let mut free_disk = self.free_disk_bytes().saturating_add(written);
            let mut adopted = false;
            for (held, on_file) in holds {
                let member = super::held_member(held.volume);
                if on_file {
                    self.adopt_spill(node, member)?;
                    let files = self.files_mut(node);
                    let reaches = files
                        .spills
                        .get(&member)
                        .and_then(|spill| spill.len().ok())
                        .is_some_and(|len| held.range.end <= len);
                    if reaches {
                        files.holds.restore_spilled(held.volume, held.range);
                    }
                    continue;
                }
                let within = held
                    .scratch_offset
                    .checked_add(held.range.len())
                    .is_some_and(|end| end <= written);
                if !within {
                    continue;
                }
                if self.files_mut(node).holds.restore(held, free_disk) == Reserve::Granted {
                    free_disk = free_disk.saturating_sub(held.range.len());
                    adopted = true;
                    continue;
                }
                match self.move_to_spill(&scratch, node, held, spill_free) {
                    Ok(true) => {
                        spill_free = spill_free.saturating_sub(held.range.len());
                        self.files_mut(node)
                            .holds
                            .restore_spilled(held.volume, held.range);
                    }
                    // Not adopted: its article reads as missing and is
                    // fetched again.
                    Ok(false) => {}
                    Err(error) => tracing::warn!(
                        job_id = self.scope,
                        node = node.0,
                        member = member.0,
                        error = %error,
                        "a restored hold could not move to its spill file; fetching it again"
                    ),
                }
            }
            // A file with bytes is kept as left even when no hold came back,
            // so the first page-out never empties what a row names.
            if adopted || written > 0 {
                self.files_mut(node).scratch = Some(scratch);
            }
        }
        Ok(())
    }

    // A restored hold on scratch moved to its member's spill file, a
    // `READ_BACK` at a time, when the disk reserve leaves room for it beside
    // `free_disk`. False when it has no room. The member's spill file may
    // hold other rows' bytes: it is opened as left, never emptied, and a
    // move cut short leaves bytes in it that no row names and nothing
    // reads.
    fn move_to_spill(
        &mut self,
        scratch: &Leaf,
        node: NodeId,
        held: ScratchHold,
        free_disk: u64,
    ) -> Result<bool, WriteError> {
        let len = held.range.len();
        let reserve = self.accountant.budgets().disk_reserve;
        if reserve != 0
            && reserve
                .checked_add(len)
                .is_none_or(|needed| needed > free_disk)
        {
            return Ok(false);
        }
        let member = super::held_member(held.volume);
        self.adopt_spill(node, member)?;
        let mut done = 0;
        let mut bytes = Vec::new();
        while done < len {
            let take = (len - done).min(super::READ_BACK);
            bytes.clear();
            scratch.read(held.scratch_offset + done, super::index(take), &mut bytes)?;
            self.spill_write(node, member, held.range.start + done, &bytes)?;
            done += take;
        }
        Ok(true)
    }

    // The member's spill file as a checkpoint left it, opened where it lies
    // and never emptied.
    fn adopt_spill(&mut self, node: NodeId, member: MemberId) -> Result<(), WriteError> {
        if self
            .files
            .get(&node)
            .is_some_and(|files| files.spills.contains_key(&member))
        {
            return Ok(());
        }
        let spill = Leaf::found(&self.working_dir, &super::spill_name(node, member))?;
        self.files_mut(node).spills.insert(member, spill);
        Ok(())
    }

    // The node's kept envelopes.
    pub(crate) fn kept_records(&self, node: NodeId) -> Vec<KeptRecord> {
        self.envelopes.records(node)
    }

    // The generation of the kept envelopes: a change to a node's marks it
    // under a later one.
    pub(crate) fn kept_generation(&self) -> u64 {
        self.envelopes.generation()
    }

    // The nodes whose kept envelopes changed since generation `at`.
    pub(crate) fn kept_changed_since(&self, at: u64) -> impl Iterator<Item = NodeId> + '_ {
        self.envelopes.changed_since(at)
    }

    // A member's file as it was left, where the checkpoint says it sits.
    // The file opens on the next write or read, and counts against the
    // job's budget as far as the tree's coverage of it reaches. A member
    // with a stated CRC takes back its fold, so its check at close reads
    // nothing: one closed whole and undamaged passed that check as it
    // closed, unless verification was still pending. Pending checks keep
    // no assumed fold and resume through the worker. One still open folds
    // what the source's checksums vouch for of what it holds, and nothing
    // is read: what they do not vouch for leaves the fold short, and its
    // check at close reads the file through the worker, as a pending one.
    pub(crate) fn restore_file(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        state: &FileState,
        segments: &dyn Segments,
    ) -> Result<(), WriteError> {
        let Some(record) = tree.namespace(node).and_then(|found| found.get(member)) else {
            return Ok(());
        };
        if state.has_file {
            let root = self.root_for(tree, node, member).to_path_buf();
            let mut leaf = self.restored_leaf(
                (node, member),
                &record.output_name,
                state.on_disk.as_deref(),
                &root,
            )?;
            if let Some(stated) = record.crc32 {
                let pending = self.verifications.contains(&(node, member));
                match (pending, record.length) {
                    // The worker reads it back, off the pipeline thread.
                    (true, _) => {}
                    (false, Some(length))
                        if record.closed
                            && record.coverage.covered(ByteRange::new(0, length))
                            && !record.coverage.damaged(ByteRange::new(0, length)) =>
                    {
                        leaf.fold_passed(length, Digest::crc32(stated));
                    }
                    (false, _) => {
                        let mut runs = vouched(segments, node, member, &record.coverage);
                        runs.extend(produced(tree, node, member, &record.coverage));
                        leaf.fold_vouched(runs)
                    }
                }
            }
            self.files_mut(node).leaves.insert(member, leaf);
            self.caps.restore(tree, node, member);
        }
        if let Some(why) = state.refused {
            self.files_mut(node).refuse(member, why);
        }
        Ok(())
    }

    // A checkpoint from a build that refused a member when its holds ran
    // past their budgets keeps that refusal, which was never a ruling on
    // the member's bytes: every byte went to the member's file. The tree
    // takes each such refusal back, the object that gave the member up goes
    // whole, and whatever owns its volumes now reads them from their files,
    // as after a repair proves a member.
    pub(crate) fn lift_hold_refusals(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let refused: Vec<(NodeId, MemberId)> = self
            .files
            .iter()
            .flat_map(|(node, files)| {
                files
                    .refused
                    .iter()
                    .filter(|(_, why)| **why == Why::HoldsBreached)
                    .map(|(member, _)| (*node, *member))
            })
            .collect();
        for refused in refused {
            self.reopen(tree, builder, segments, refused, out)?;
        }
        Ok(())
    }

    // No checkpoint keeps which member a set bound to which file: a set
    // binds from the names, lengths and fronts of the members beside it,
    // and a restored set has bound none. Each namespace's sets bind over it
    // as it was restored, parents first, before any of them rules: a set
    // asked what it describes that no member carries answers from what it
    // bound.
    pub(crate) fn bind_restored(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut bound = None;
        while let Some(node) = tree.next_recovery_home(bound) {
            bound = Some(node);
            if tree.is_live(node) {
                self.bind_recoveries(tree, builder, segments, node, out)?;
            }
        }
        Ok(())
    }

    // The restored tree's members, counted and charged against the job's
    // budget as the tree holds them.
    pub(crate) fn restore_members(&mut self, tree: &Tree) {
        self.caps.restore_members(tree);
    }

    // A member whose front a restore read again keeps it in memory: the
    // job's limits judge it as they judge a member that grew.
    pub(crate) fn admit_front(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), WriteError> {
        Ok(self.caps.admit_member(tree, node, member)?)
    }

    // The member reads the job's repairs made.
    pub(crate) fn restore_read_backs(&mut self, read_backs: u64) {
        self.read_backs = read_backs;
    }

    pub(crate) fn restore_kept(&mut self, record: &KeptRecord) -> Result<(), WriteError> {
        Ok(self.envelopes.restore(record)?)
    }

    // The posted bytes of a root member's range, read back through the
    // inverse map of whatever owns it, as a repair reads them, from what is
    // on disk only: what a restart now would find.
    #[cfg(test)]
    pub(crate) fn read_posted(
        &self,
        tree: &Tree,
        member: MemberId,
        range: weaver_graph::ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        let mut access = self.access(tree);
        access.held = false;
        access.read_at(NodeId::ROOT, member, range, into)
    }
}

// The runs of a member's file the object that produced it checksummed as
// they came, each with its CRC: those wholly inside bytes the tree holds
// covered and undamaged.
fn produced(
    tree: &Tree,
    node: NodeId,
    member: MemberId,
    coverage: &Coverage,
) -> impl Iterator<Item = (ByteRange, Digest)> {
    tree.vouched(node, member)
        .into_iter()
        .filter(|(range, _)| coverage.covered(*range) && !coverage.damaged(*range))
        .map(|(range, crc)| (range, Digest::crc32(crc)))
}

// The runs of a member's file the source's checksums vouch for, each with
// its CRC: as far as they lie end to end, every one passed, wholly inside
// bytes that came fresh and stand as they came.
fn vouched(
    segments: &dyn Segments,
    node: NodeId,
    member: MemberId,
    coverage: &Coverage,
) -> Vec<(ByteRange, Digest)> {
    let mut out = Vec::new();
    let mut found = Vec::new();
    for run in coverage
        .runs()
        .filter(|run| run.provenance == Provenance::Fresh)
    {
        found.clear();
        segments.segments_for(node, VolumeIndex(member.0), run.range, &mut found);
        found.retain(|segment: &Segment| {
            segment.verified
                && segment.range.start >= run.range.start
                && segment.range.end <= run.range.end
        });
        found.sort_by_key(|segment| segment.range.start);
        out.extend(
            found
                .iter()
                .map(|segment| (segment.range, Digest::crc32(segment.crc32))),
        );
    }
    out
}
