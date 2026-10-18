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
use weaver_graph::{ByteRange, Effect, MemberId, NodeId, Tree};

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
        let runs: Vec<_> = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| record.coverage.runs().copied().collect())
            .unwrap_or_default();
        for run in runs {
            let mut at = run.range.start;
            while at < run.range.end {
                let chunk = ByteRange::new(at, run.range.end.min(at.saturating_add(READ_BACK)));
                at = chunk.end;
                let mut bytes = Vec::new();
                let read = self
                    .files
                    .get(&node)
                    .and_then(|files| files.leaves.get(&member))
                    .is_some_and(|leaf| {
                        leaf.read(chunk.start, super::index(chunk.len()), &mut bytes)
                            .is_ok()
                    });
                if !read {
                    continue;
                }
                let arrival = Arrival {
                    node,
                    member,
                    offset: chunk.start,
                    bytes: Bytes::from(bytes),
                    provenance: run.provenance,
                };
                self.route(tree, builder, segments, arrival, out)?;
            }
        }
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
