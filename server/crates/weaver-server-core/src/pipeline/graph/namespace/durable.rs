// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a checkpoint says of the writer, and the writer a restore puts back:
// which members have a file of their own and why a member is a leaf, the
// kept envelopes, how many member reads the repairs made, and the read of a
// root member back from wherever its bytes went. Holds are not here: a hold is fetched again.

use weaver_graph::{MemberId, NodeId, Tree};

use super::{NamespaceWriter, WriteError};
use crate::pipeline::graph::checkpoint::FileState;
use crate::pipeline::graph::envelope::durable::KeptRecord;

impl NamespaceWriter {
    pub(crate) fn file_state(&self, node: NodeId, member: MemberId) -> FileState {
        let files = self.files.get(&node);
        FileState {
            has_file: files.is_some_and(|files| files.leaves.contains_key(&member)),
            refused: files.and_then(|files| files.refused.get(&member).copied()),
        }
    }

    pub(crate) fn kept_records(&self) -> Vec<KeptRecord> {
        self.envelopes.records()
    }

    // A member's file as it was left. Nothing on disk is touched: the file
    // opens on the next write or read, and counts against the job's budget
    // as far as the tree's coverage of it reaches.
    pub(crate) fn restore_file(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        state: FileState,
    ) -> Result<(), WriteError> {
        let Some(record) = tree.namespace(node).and_then(|found| found.get(member)) else {
            return Ok(());
        };
        if state.has_file {
            self.leaf(node, member, &record.output_name)?;
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
