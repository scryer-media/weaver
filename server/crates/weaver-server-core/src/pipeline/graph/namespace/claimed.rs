// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A member an object claimed after it had been decided a leaf: a volume that
// came before the one that built its object, or one whose front only said
// what it is once a repair rewrote it. Its bytes went to its own file, so
// they go down to its object again from there, each run with the
// provenance it arrived with, by the one write path, exactly as the bytes
// held for an undecided member go down once it is bound. The file is no
// delivery of its own after that, unless the object is a recovery object,
// whose volumes live in their files.

use bytes::Bytes;
use weaver_graph::{ByteRange, Effect, MemberId, NodeId, Provenance, Tree};

use super::{Arrival, NamespaceWriter, READ_BACK, Segments, WriteError};

impl NamespaceWriter {
    pub(super) fn replay_file(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn weaver_graph::Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        // A member with no file has nothing in one: what it received is in
        // its holds. A member with a file has every byte that arrived for it
        // in the file, so a run the file does not give back ends the write
        // with the file's error, a file gone with it included. Each chunk
        // goes by the coverage and the file as they are then: routing a
        // run can name the member and settle its length, which moves and
        // trims the file and the coverage behind it. A range held as damage
        // may never have arrived at all: the file ends before it, and what
        // lies past the end is not read. The file goes to the owner the
        // member has now: a run that has that owner give the member up hands
        // it on, and the hand-off carries the whole file to whatever holds
        // the member next, so this replay is done.
        if !self
            .files
            .get(&node)
            .is_some_and(|files| files.leaves.contains_key(&member))
        {
            return self.release_owned_elsewhere(tree, node, member);
        }
        let owner = tree.child_of(node, member);
        let mut at = 0;
        while tree.child_of(node, member) == owner
            && let Some(run) = tree
                .namespace(node)
                .and_then(|namespace| namespace.get(member))
                .and_then(|record| {
                    record
                        .coverage
                        .overlapping(ByteRange::new(at, u64::MAX))
                        .next()
                        .copied()
                })
        {
            let leaf = self
                .files
                .get(&node)
                .and_then(|files| files.leaves.get(&member))
                .ok_or(WriteError::UnknownMember { node, member })?;
            let end = match run.provenance {
                Provenance::Damaged => run.range.end.min(leaf.len()?),
                _ => run.range.end,
            };
            let start = run.range.start.max(at);
            if start >= end {
                at = run.range.end;
                continue;
            }
            let chunk = ByteRange::new(start, end.min(start.saturating_add(READ_BACK)));
            at = chunk.end;
            let mut bytes = Vec::new();
            leaf.read(chunk.start, super::index(chunk.len()), &mut bytes)?;
            let arrival = Arrival {
                node,
                member,
                offset: chunk.start,
                bytes: Bytes::from(bytes),
                provenance: run.provenance,
            };
            self.route(tree, builder, segments, arrival, out)?;
        }
        self.release_owned_elsewhere(tree, node, member)
    }

    // The file is no delivery of its own once a non-recovery object owns the
    // member.
    fn release_owned_elsewhere(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), WriteError> {
        let owned_elsewhere = tree
            .child_of(node, member)
            .is_some_and(|(child, _)| !tree.recovers(child));
        if owned_elsewhere {
            let files = self.files_mut(node);
            files.refused.remove(&member);
            if let Some(leaf) = files.leaves.remove(&member) {
                leaf.remove()?;
            }
        }
        Ok(())
    }
}
