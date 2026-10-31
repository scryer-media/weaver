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
use weaver_graph::{ByteRange, Coverage, Effect, MemberId, NodeId, Tree};

use super::{Arrival, NamespaceWriter, READ_BACK, Segments, WriteError};

impl NamespaceWriter {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn replay_file(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn weaver_graph::Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        said: Option<&Coverage>,
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
        // the member next, so this replay is done. A folded posting's runs
        // are `said`, its record from before it folded, which no route
        // moves.
        if !self
            .files
            .get(&node)
            .is_some_and(|files| files.leaves.contains_key(&member))
        {
            return self.release_owned_elsewhere(tree, node, member);
        }
        // A member can have both: a file a refusal read it back into, and
        // holds for what came after an object took it again. What the holds
        // carry never reached the file, so it goes on from the holds, and
        // the file is read only around them.
        let held = self
            .files
            .get(&node)
            .map(|files| files.holds.held_ranges(super::held_volume(member)))
            .unwrap_or_default();
        let owner = tree.child_of(node, member);
        let mut at = 0;
        while tree.child_of(node, member) == owner
            && let Some(run) = said
                .or_else(|| {
                    tree.namespace(node)
                        .and_then(|namespace| namespace.get(member))
                        .map(|record| &record.coverage)
                })
                .and_then(|coverage| {
                    coverage
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
            let end = if run.provenance.is_damaged() {
                run.range.end.min(leaf.len()?)
            } else {
                run.range.end
            };
            let start = run.range.start.max(at);
            if start >= end {
                at = run.range.end;
                continue;
            }
            if let Some(hold) = held.iter().find(|hold| hold.contains(start)) {
                at = hold.end;
                continue;
            }
            let end = held
                .iter()
                .map(|hold| hold.start)
                .filter(|from| *from > start)
                .fold(end, u64::min);
            let chunk = ByteRange::new(start, end.min(start.saturating_add(READ_BACK)));
            at = chunk.end;
            let mut bytes = Vec::new();
            leaf.read(chunk.start, super::index(chunk.len()), &mut bytes)?;
            let arrival = Arrival {
                node,
                member,
                offset: chunk.start,
                bytes: Bytes::from(bytes).into(),
                provenance: run.provenance,
            };
            self.route_as_brought(tree, builder, segments, arrival, out)?;
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
            files.unrefuse(member);
            if let Some(leaf) = files.leaves.remove(&member) {
                self.discard(leaf)?;
            }
        }
        Ok(())
    }
}
