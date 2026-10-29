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
use weaver_graph::{ByteRange, MemberId, NodeId, Tree};

use super::{NamespaceWriter, WriteError, scratch_name};
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

    // The holds the node's namespace has on scratch, once the job has paged
    // them out for a stop; none before, so no row names bytes a later hold
    // may replace.
    pub(crate) fn hold_records(&self, node: NodeId) -> Vec<ScratchHold> {
        if !self.stopped {
            return Vec::new();
        }
        self.files
            .get(&node)
            .map(|files| files.holds.scratch_holds())
            .unwrap_or_default()
    }

    // A graceful stop: every namespace pages what it holds in RAM to
    // scratch, under the same budgets as any page-out, so the checkpoint
    // after it records them and a restart reads them back. What scratch
    // cannot take stays in RAM and is fetched again.
    pub(crate) fn page_out_holds(&mut self) -> Result<(), WriteError> {
        self.stopped = true;
        let nodes: Vec<NodeId> = self
            .files
            .iter()
            .filter(|(_, files)| files.holds.resident_bytes() > 0)
            .map(|(node, _)| *node)
            .collect();
        let mut free_disk = self.free_disk_bytes();
        for node in nodes {
            let page_outs = self.files_mut(node).holds.page_out_all(free_disk);
            for page_out in &page_outs {
                free_disk = free_disk.saturating_sub(page_out.range.len());
                self.scratch(node)?
                    .write(page_out.scratch_offset, &page_out.bytes, None)?;
            }
        }
        Ok(())
    }

    // The holds a checkpoint recorded on scratch, adopted where they lie:
    // the node's scratch file is opened as it was left, never emptied, and
    // each hold goes back into its ledger. A hold of a namespace or member
    // the tree no longer has, one past the end of the file, or one the
    // budgets refuse is not adopted, so its article reads as missing and is
    // fetched again. Bytes the rows do not describe are never read.
    pub(crate) fn restore_holds(
        &mut self,
        tree: &Tree,
        records: &[HoldRecord],
    ) -> Result<(), WriteError> {
        let mut by_node: BTreeMap<NodeId, Vec<ScratchHold>> = BTreeMap::new();
        for record in records {
            let member = MemberId(record.held.volume.0);
            if tree
                .namespace(record.node)
                .is_some_and(|namespace| namespace.get(member).is_some())
            {
                by_node.entry(record.node).or_default().push(record.held);
            }
        }
        for (node, holds) in by_node {
            let scratch = Leaf::at(&self.working_dir, &scratch_name(node));
            let written = scratch.len().unwrap_or(0);
            // The file's bytes are already on disk, so the disk reserve
            // judges them against the space they would leave if absent.
            let mut free_disk = self.free_disk_bytes().saturating_add(written);
            let files = self.files_mut(node);
            let mut adopted = false;
            for held in holds {
                let within = held
                    .scratch_offset
                    .checked_add(held.range.len())
                    .is_some_and(|end| end <= written);
                if within && files.holds.restore(held, free_disk) == Reserve::Granted {
                    free_disk = free_disk.saturating_sub(held.range.len());
                    adopted = true;
                }
            }
            if adopted {
                files.scratch = Some(scratch);
            }
        }
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
    // no assumed fold and resume through the worker. One still open reads
    // what it holds back here, off the pipeline thread, as a restore reads.
    pub(crate) fn restore_file(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        state: &FileState,
    ) -> Result<(), WriteError> {
        let Some(record) = tree.namespace(node).and_then(|found| found.get(member)) else {
            return Ok(());
        };
        if state.has_file {
            let mut leaf =
                self.restored_leaf(node, member, &record.output_name, state.on_disk.as_deref());
            if let Some(stated) = record.crc32 {
                match record.length {
                    Some(length)
                        if !self.verifications.contains(&(node, member))
                            && record.closed
                            && record.coverage.covered(ByteRange::new(0, length))
                            && !record.coverage.damaged(ByteRange::new(0, length)) =>
                    {
                        leaf.fold_passed(length, stated);
                    }
                    _ if self.verifications.contains(&(node, member)) => {}
                    _ => leaf.fold_back(
                        record
                            .coverage
                            .runs()
                            .map(|run| (run.range.start, run.range.end)),
                    ),
                }
            }
            self.files_mut(node).leaves.insert(member, leaf);
            self.caps.restore(tree, node, member);
        }
        if let Some(why) = state.refused {
            self.files_mut(node).refused.insert(member, why);
        }
        Ok(())
    }

    // The restored tree's members, counted and charged against the job's
    // budget as the tree holds them.
    pub(crate) fn restore_members(&mut self, tree: &Tree) {
        self.caps.restore_members(tree);
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
