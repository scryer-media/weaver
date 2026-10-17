// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Envelopes: the bytes of a bound member that belong to no member of the
// object that owns it, such as headers and the padding between members.
// They are the inverse map's small remainder. The archive itself is never
// written, so these are kept, per node and envelope, as the bytes pass, and
// a read of the posted volume puts them back between the members' bytes.

use std::collections::BTreeMap;

use bytes::Bytes;
use weaver_graph::{ByteRange, NodeId, Placement};

#[derive(Debug, Default)]
pub(crate) struct Envelopes {
    // Disjoint pieces by start offset in envelope coordinates.
    kept: BTreeMap<(NodeId, u32), BTreeMap<u64, Bytes>>,
}

impl Envelopes {
    // Keep the envelope pieces among `placements`, which the object at
    // `node` gave for the volume range that `bytes` is: pieces in volume
    // order, so each one's bytes are the next run of `bytes`.
    pub(crate) fn capture(&mut self, node: NodeId, placements: &[Placement], bytes: &Bytes) {
        let mut cursor = 0usize;
        for placement in placements {
            let len = usize::try_from(placement.len()).expect("a piece fits the address space");
            let Some(end) = cursor.checked_add(len).filter(|&end| end <= bytes.len()) else {
                return;
            };
            if let Placement::Envelope { envelope, range } = placement {
                self.keep(node, *envelope, range.start, bytes.slice(cursor..end));
            }
            cursor = end;
        }
    }

    // Keep `bytes` at `offset` of an envelope, replacing what they overlap:
    // a repaired header replaces the damaged one.
    pub(crate) fn keep(&mut self, node: NodeId, envelope: u32, offset: u64, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        let range = ByteRange::at(offset, bytes.len() as u64);
        let pieces = self.kept.entry((node, envelope)).or_default();
        let touched: Vec<u64> = pieces
            .range(..range.end)
            .filter(|(start, piece)| **start + piece.len() as u64 > range.start)
            .map(|(start, _)| *start)
            .collect();
        for start in touched {
            let Some(piece) = pieces.remove(&start) else {
                continue;
            };
            let end = start + piece.len() as u64;
            if start < range.start {
                pieces.insert(start, piece.slice(..index(range.start - start)));
            }
            if end > range.end {
                pieces.insert(range.end, piece.slice(index(range.end - start)..));
            }
        }
        pieces.insert(offset, bytes);
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
        let Some(pieces) = self.kept.get(&(node, envelope)) else {
            return range.is_empty();
        };
        let start = into.len();
        let mut cursor = range.start;
        for (&offset, piece) in pieces.range(..range.end) {
            let end = offset + piece.len() as u64;
            if end <= cursor {
                continue;
            }
            if offset > cursor {
                break;
            }
            let stop = end.min(range.end);
            into.extend_from_slice(&piece[index(cursor - offset)..index(stop - offset)]);
            cursor = stop;
        }
        if cursor < range.end {
            into.truncate(start);
            return false;
        }
        true
    }

    // The node is gone, and its envelopes with it.
    pub(crate) fn forget(&mut self, node: NodeId) {
        self.kept.retain(|(owner, _), _| *owner != node);
    }
}

fn index(offset: u64) -> usize {
    usize::try_from(offset).expect("an envelope offset fits the address space")
}
