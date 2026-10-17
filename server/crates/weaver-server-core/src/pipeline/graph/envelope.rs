// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Envelopes: the bytes of a bound member that belong to no member of the
// object that owns it, such as headers and the padding between members.
// They are the inverse map's small remainder. The archive itself is never
// written, so these are kept, per node and envelope, as the bytes pass, in
// a file of their own under the directory finalize never delivers, and a
// read of the posted volume puts them back between the members' bytes.

use std::collections::BTreeMap;
use std::path::PathBuf;

use weaver_graph::{ByteRange, Coverage, NodeId, Placement, Provenance};

use super::leaf::{Leaf, LeafError};

#[derive(Debug)]
pub(crate) struct Envelopes {
    working_dir: PathBuf,
    // Each envelope's file and which of its bytes are in it.
    kept: BTreeMap<(NodeId, u32), (Leaf, Coverage)>,
}

impl Envelopes {
    pub(crate) fn new(working_dir: PathBuf) -> Self {
        Self {
            working_dir,
            kept: BTreeMap::new(),
        }
    }

    // Keep the envelope pieces among `placements`, which the object at
    // `node` gave for the volume range that `bytes` is: pieces in volume
    // order, so each one's bytes are the next run of `bytes`.
    pub(crate) fn capture(
        &mut self,
        node: NodeId,
        placements: &[Placement],
        bytes: &[u8],
    ) -> Result<(), LeafError> {
        let mut cursor = 0usize;
        for placement in placements {
            let len = usize::try_from(placement.len()).expect("a piece fits the address space");
            let Some(end) = cursor.checked_add(len).filter(|&end| end <= bytes.len()) else {
                return Ok(());
            };
            if let Placement::Envelope { envelope, range } = placement {
                self.keep(node, *envelope, range.start, &bytes[cursor..end])?;
            }
            cursor = end;
        }
        Ok(())
    }

    // Keep `bytes` at `offset` of an envelope, replacing what they overlap:
    // a repaired header replaces the damaged one.
    pub(crate) fn keep(
        &mut self,
        node: NodeId,
        envelope: u32,
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), LeafError> {
        if bytes.is_empty() {
            return Ok(());
        }
        let (leaf, coverage) = match self.kept.get_mut(&(node, envelope)) {
            Some(kept) => kept,
            None => {
                let name = format!(".weaver-chunks/graph-envelope-{}-{envelope}", node.0);
                let mut leaf = Leaf::at(&self.working_dir, &name)?;
                leaf.truncate()?;
                self.kept
                    .entry((node, envelope))
                    .or_insert((leaf, Coverage::new()))
            }
        };
        leaf.write(offset, bytes, None)?;
        coverage.write(ByteRange::at(offset, bytes.len() as u64), Provenance::Fresh);
        Ok(())
    }

    // Append the bytes of `range` of an envelope to `into`, or nothing and
    // `false` when any of them were never kept.
    pub(crate) fn read(
        &self,
        node: NodeId,
        envelope: u32,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        let Some((leaf, coverage)) = self.kept.get(&(node, envelope)) else {
            return range.is_empty();
        };
        let len = usize::try_from(range.len()).expect("an envelope range fits the address space");
        coverage.covered(range) && leaf.read(range.start, len, into).is_ok()
    }

    // The node is gone, and its envelopes with it.
    pub(crate) fn forget(&mut self, node: NodeId) -> Result<(), LeafError> {
        let gone: Vec<(NodeId, u32)> = self
            .kept
            .keys()
            .filter(|(owner, _)| *owner == node)
            .copied()
            .collect();
        for key in gone {
            if let Some((leaf, _)) = self.kept.remove(&key) {
                leaf.remove()?;
            }
        }
        Ok(())
    }
}
