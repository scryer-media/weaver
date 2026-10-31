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
// A member still in its own file was never handed to the object whole: the
// file holds every byte it arrived with, so those go on from the file and
// the object is asked for nothing.
//
// A range the map cannot reproduce is delivered as damage. A recovery object
// that covers the member repairs it from there; with none, the member
// completes with that damage tolerated, as a damaged article does.

use bytes::Bytes;
use weaver_graph::{
    Builder, ByteRange, Coverage, Effect, MemberId, NodeId, Placement, Provenance, Tree,
};

use super::{Arrival, NamespaceWriter, READ_BACK, Segments, Write, WriteError};
use crate::pipeline::graph::leaf::Leaf;

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
        if self
            .files
            .get(&node)
            .is_some_and(|files| files.leaves.contains_key(&member))
        {
            // What the object held came from the file, which still has it.
            self.take_held(node, member, super::ALL)?;
            if tree.child_of(node, member).is_some() {
                self.replay_file(tree, builder, segments, node, member, None, out)?;
            }
            self.release_subtree(tree, retired)?;
            return self.close_if_leaf(tree, node, member);
        }
        // What the member's holds carry never reached the object, whatever
        // its map has since come to say of those bytes: they go on from the
        // holds, and the object is asked only for the rest.
        let held = self
            .files
            .get(&node)
            .map(|files| files.holds.held_ranges(super::held_volume(member)))
            .unwrap_or_default();
        let record = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        let output_name = record.output_name.clone();
        let runs: Vec<_> = record
            .coverage
            .runs()
            .flat_map(|run| outside(run.range, &held).map(move |range| (range, run.provenance)))
            .collect();
        let ranges: Vec<ByteRange> = runs.iter().map(|(range, _)| *range).collect();
        let shared = match kept_as_volume(tree, (retired, volume), &ranges) {
            Some(envelope) => {
                let taken = ranges.iter().map(ByteRange::len).sum();
                self.share_envelope(
                    tree,
                    (node, member),
                    (retired, envelope),
                    taken,
                    &output_name,
                )?
            }
            None => None,
        };
        if tree.child_of(node, member).is_some() {
            self.replay(tree, builder, segments, node, member, super::ALL, None, out)?;
        } else {
            self.release_to_leaf(tree, node, member)?;
        }
        self.release_subtree(tree, retired)?;
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
                            .unwrap_or_else(|| Bytes::from(vec![0; super::index(piece.len())]))
                            .into(),
                    };
                    // Reproduced bytes were evidence when they first came;
                    // damage is new, and the recovery objects hear it.
                    if arrival.provenance == provenance {
                        self.route_as_brought(tree, builder, segments, arrival, out)?;
                    } else {
                        self.arrive(tree, builder, segments, Write::new(&arrival), arrival, out)?;
                    }
                }
            }
        }
        if let Some(envelope) = shared {
            // The file is the member's now, or its engine's envelope: the
            // retired object no longer keeps it, so it is not removed with
            // the object's other envelopes.
            self.envelopes.take(retired, envelope);
        }
        self.close_if_leaf(tree, node, member)
    }

    // The retired object's envelope holds every byte the member took, at
    // the member's own offsets, and nothing else, so the bytes go back into
    // that same file rather than into a second one: the member's own file,
    // which moves to its name once the checkpoint that records it there has
    // committed, as any file waiting to move does, or the envelope of the
    // engine bound in the object's place, which keeps it where it is. Until
    // then the checkpoint the database holds names it as the retired
    // object's envelope, so the bytes are on disk once, where both
    // checkpoints look for them. The envelope, when it is shared; `None`,
    // with nothing done, when it keeps more than the member took, or the
    // bytes go to an object that is not an engine, which keeps only some of
    // a volume as envelope, or to an engine whose envelope already started.
    fn share_envelope(
        &mut self,
        tree: &Tree,
        (node, member): (NodeId, MemberId),
        (retired, envelope): (NodeId, u32),
        taken: u64,
        output_name: &str,
    ) -> Result<Option<u32>, WriteError> {
        let Some((file, kept)) = self.envelopes.kept_file(retired, envelope) else {
            return Ok(None);
        };
        if kept != taken {
            return Ok(None);
        }
        let shared = match tree.child_of(node, member) {
            Some((child, at)) if tree.engine(child).is_some() => {
                self.envelopes.start_in(child, at.0, &file)
            }
            Some(_) => false,
            None => {
                let leaf = Leaf::at(&self.working_dir, &file);
                self.files_mut(node).leaves.insert(member, leaf);
                self.intend(node, member, output_name);
                true
            }
        };
        Ok(shared.then_some(envelope))
    }

    // Bytes a member already took, placed again from where they went, each
    // part by the posting that brought it: the first copy stands against
    // them as it stood when they came, so all of them go down again.
    pub(super) fn route_as_brought(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let range = arrival.range();
        let parts = tree.namespace(arrival.node).map_or_else(
            || vec![(range, arrival.member)],
            |namespace| namespace.brought(arrival.member, range),
        );
        for (part, posting) in parts {
            let from = super::index(part.start - range.start);
            let to = super::index(part.end - range.start);
            let piece = Arrival {
                member: posting,
                offset: part.start,
                bytes: arrival.bytes.slice(from..to),
                ..arrival.clone()
            };
            self.route(tree, builder, segments, piece, out)?;
        }
        Ok(())
    }

    fn close_if_leaf(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), WriteError> {
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
            // Each part the placement's source holds is posted on its own,
            // so a gap in the source costs that gap, not the piece around it.
            let mut skip = 0;
            for kept in self.kept(tree, retired, placement) {
                if kept.start > skip {
                    pieces.push((
                        ByteRange::new(piece.start + skip, piece.start + kept.start),
                        None,
                    ));
                }
                let part = kept.shift(piece.start as i64);
                let mut into = Vec::new();
                let posted = object.posted(volume, part, &mut read, &mut into)
                    && into.len() as u64 == part.len();
                pieces.push((part, posted.then(|| Bytes::from(into))));
                skip = kept.end;
            }
            if skip < piece.len() {
                pieces.push((ByteRange::new(piece.start + skip, piece.end), None));
            }
        }
        tree.put_object(retired, object);
        pieces
    }

    // The parts of a placement its source holds, as offsets into the
    // placement, in order: what the envelope kept, or what reached the
    // member. Only those can be posted again.
    fn kept(&self, tree: &Tree, retired: NodeId, placement: &Placement) -> Vec<ByteRange> {
        let held = |coverage: &Coverage, range: ByteRange| {
            let mut parts: Vec<ByteRange> = Vec::new();
            for run in coverage.overlapping(range) {
                let Some(part) = run.range.intersect(range) else {
                    continue;
                };
                let part = part.shift(-(range.start as i64));
                match parts.last_mut() {
                    Some(last) if last.end == part.start => last.end = part.end,
                    _ => parts.push(part),
                }
            }
            parts
        };
        match placement {
            Placement::Member { member, range } => tree
                .namespace(retired)
                .and_then(|namespace| namespace.get(*member))
                .map(|record| held(&record.coverage, *range))
                .unwrap_or_default(),
            Placement::Envelope { envelope, range } => self
                .envelopes
                .shelf_of(retired)
                .and_then(|shelf| {
                    let state = shelf.lock();
                    state
                        .kept
                        .get(envelope)
                        .map(|kept| held(&kept.coverage, *range))
                })
                .unwrap_or_default(),
            Placement::Held { .. } => Vec::new(),
        }
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

// The envelope in which the object at `retired` placed every byte of
// `ranges` of its volume, at the volume's own offsets, if one did: that
// envelope's file holds those bytes as the volume does.
fn kept_as_volume(
    tree: &Tree,
    (retired, volume): (NodeId, weaver_graph::VolumeIndex),
    ranges: &[ByteRange],
) -> Option<u32> {
    let mut placements = Vec::new();
    let mut kept_in = None;
    for range in ranges {
        placements.clear();
        tree.locate(retired, volume, *range, &mut placements);
        let mut at = range.start;
        for placement in &placements {
            let Placement::Envelope {
                envelope,
                range: kept,
            } = placement
            else {
                return None;
            };
            if kept.start != at || *kept_in.get_or_insert(*envelope) != *envelope {
                return None;
            }
            at = kept.end;
        }
        if at != range.end {
            return None;
        }
    }
    kept_in
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
