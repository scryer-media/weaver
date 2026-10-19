// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A member its object gave up. It is a leaf now, or a volume of the engine
// the tree bound in the object's place, and either way the bytes it already
// received are delivered to it again: read back through the retired object's
// inverse map, in volume order, each piece with the provenance it arrived
// with. What went to the object's own members is read from wherever they put
// it, as deep as its subtree goes, and what it kept as envelope from the
// envelopes; what it could not place was held for the member and goes where
// held bytes always go once a member's fate is known. A leaf takes the bytes
// into its file and an engine into its envelopes, by the one write path, so
// refusing and handing to an engine differ only in what the tree bound.
//
// A range the map cannot reproduce is delivered as damage. A recovery object
// that covers the member repairs it from there; with none, the member
// completes with that damage tolerated, as a damaged article does.

use bytes::Bytes;
use weaver_graph::{Builder, ByteRange, Effect, MemberId, NodeId, Placement, Provenance, Tree};

use super::{Arrival, NamespaceWriter, READ_BACK, Segments, WriteError};

impl NamespaceWriter {
    pub(super) fn materialise(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let Some((retired, volume)) = tree.retired(node, member) else {
            return Ok(());
        };
        // What the member's holds carry never reached the object, whatever
        // its map has since come to say of those bytes: they go on from the
        // holds, and the object is asked only for the rest.
        let held = self
            .files
            .get(&node)
            .map(|files| files.holds.held_ranges(super::held_volume(member)))
            .unwrap_or_default();
        if tree.child_of(node, member).is_some() {
            self.replay(tree, builder, segments, node, member, super::ALL, out)?;
        } else {
            self.release_to_leaf(tree, node, member)?;
        }
        self.release_subtree(tree, retired)?;
        let record = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        let runs: Vec<_> = record
            .coverage
            .runs()
            .flat_map(|run| outside(run.range, &held).map(move |range| (range, run.provenance)))
            .collect();
        for (range, provenance) in runs {
            let mut at = range.start;
            while at < range.end {
                let chunk = ByteRange::new(at, range.end.min(at.saturating_add(READ_BACK)));
                at = chunk.end;
                for (piece, bytes) in self.read_back(tree, retired, volume, chunk) {
                    let arrival = Arrival {
                        node,
                        member,
                        offset: piece.start,
                        provenance: bytes.as_ref().map_or(Provenance::Damaged, |_| provenance),
                        bytes: bytes
                            .unwrap_or_else(|| Bytes::from(vec![0; super::index(piece.len())])),
                    };
                    // Reproduced bytes were evidence when they first came;
                    // damage is new, and the recovery objects hear it.
                    if arrival.provenance == provenance {
                        self.route(tree, builder, segments, arrival, out)?;
                    } else {
                        self.arrive(tree, builder, segments, arrival, out)?;
                    }
                }
            }
        }
        let closed_leaf = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|record| record.closed && record.is_leaf());
        if closed_leaf {
            self.close_leaf(tree, node, member)?;
        }
        Ok(())
    }

    // `range` of the retired node's volume, piece by piece in volume order,
    // each with the bytes that were posted there, or `None` where nothing
    // can reproduce them. The object rebuilds each piece through its own
    // `posted`, so one whose members are not its volume's bytes, one that
    // deciphers, gives back what was posted rather than what it made of it.
    // Held pieces are left out: they went with the member's holds.
    fn read_back(
        &self,
        tree: &mut Tree,
        retired: NodeId,
        volume: weaver_graph::VolumeIndex,
        range: ByteRange,
    ) -> Vec<(ByteRange, Option<Bytes>)> {
        let mut placements = Vec::new();
        tree.locate(retired, volume, range, &mut placements);
        let Some(object) = tree
            .take_object(retired)
            .filter(|_| placements.iter().map(Placement::len).sum::<u64>() == range.len())
        else {
            return vec![(range, None)];
        };
        let access = self.access(tree);
        let mut read = |placement: &Placement, into: &mut Vec<u8>| match placement {
            Placement::Member { member, range } => access.read_at(retired, *member, *range, into),
            Placement::Envelope { envelope, range } => {
                self.envelopes.read(retired, *envelope, *range, into)
            }
            Placement::Held { .. } => false,
        };
        let mut pieces = Vec::with_capacity(placements.len());
        let mut at = range.start;
        for placement in &placements {
            let piece = ByteRange::at(at, placement.len());
            at = piece.end;
            if matches!(placement, Placement::Held { .. }) {
                continue;
            }
            let mut into = Vec::new();
            let posted = object.posted(volume, piece, &mut read, &mut into)
                && into.len() as u64 == piece.len();
            pieces.push((piece, posted.then(|| Bytes::from(into))));
        }
        tree.put_object(retired, object);
        pieces
    }

    // Every byte held anywhere under the retired node goes to its member's
    // own file, where the inverse map reads it back.
    fn release_subtree(&mut self, tree: &mut Tree, retired: NodeId) -> Result<(), WriteError> {
        let mut pending = vec![retired];
        let mut seen = std::collections::BTreeSet::new();
        while let Some(at) = pending.pop() {
            if !seen.insert(at) {
                continue;
            }
            let members: Vec<MemberId> = tree
                .namespace(at)
                .map(|namespace| namespace.members().map(|member| member.id).collect())
                .unwrap_or_default();
            for member in members {
                self.release_to_leaf(tree, at, member)?;
                pending.extend(tree.child_of(at, member).map(|(child, _)| child));
            }
        }
        Ok(())
    }
}

// The parts of `range` no held range covers, in order.
fn outside(range: ByteRange, held: &[ByteRange]) -> impl Iterator<Item = ByteRange> + '_ {
    let mut cuts: Vec<ByteRange> = held
        .iter()
        .filter_map(|hold| hold.intersect(range))
        .collect();
    cuts.sort_by_key(|cut| cut.start);
    let mut at = range.start;
    cuts.into_iter()
        .map(Some)
        .chain(std::iter::once(None))
        .filter_map(move |cut| {
            let end = cut.map_or(range.end, |cut| cut.start);
            let part = ByteRange::new(at, end.max(at));
            at = at.max(cut.map_or(range.end, |cut| cut.end));
            (!part.is_empty()).then_some(part)
        })
}
