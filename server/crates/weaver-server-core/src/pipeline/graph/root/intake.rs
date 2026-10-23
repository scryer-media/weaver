// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The deliver-down side of the root: a decoded article becomes bytes of a
// root member. Its own checksum says how far they can be trusted; damaged
// bytes are delivered all the same, because a mostly right file is worth
// having when nothing can repair it, and the coverage they leave says which
// bytes they were.

mod durable;

use std::collections::BTreeMap;

use bytes::Bytes;
use weaver_graph::{ByteRange, Marks, MemberId, NodeId, Provenance, Segment, VolumeIndex};

use crate::pipeline::graph::namespace::{Arrival, Segments};

// A decoded article, as the decoder placed it.
#[derive(Clone, Debug)]
pub(crate) struct Decoded {
    pub(crate) volume: VolumeIndex,
    pub(crate) ordinal: u32,
    pub(crate) offset: u64,
    pub(crate) bytes: Bytes,
    // The article's own checksum passed and nothing suggests it was cut short.
    pub(crate) intact: bool,
    // The encoding carries the article's position; uuencode does not, so its
    // offset is only where the articles before it ended.
    pub(crate) positioned: bool,
    // The file's decoded length, when the article declares it.
    pub(crate) length: Option<u64>,
    // The checksums of the article's bytes, cut at the recovery set's block
    // boundaries when the decoder knew them.
    pub(crate) segments: Vec<Segment>,
}

impl Decoded {
    pub(crate) fn range(&self) -> ByteRange {
        ByteRange::at(self.offset, self.bytes.len() as u64)
    }

    pub(crate) fn provenance(&self) -> Provenance {
        if self.intact {
            Provenance::Fresh
        } else {
            Provenance::Damaged
        }
    }

    pub(crate) fn into_arrival(self) -> Arrival {
        Arrival {
            node: NodeId::ROOT,
            member: MemberId(self.volume.0),
            offset: self.offset,
            provenance: self.provenance(),
            bytes: self.bytes,
        }
    }
}

// The checksums the decoder computed, kept for as long as the job runs: the
// namespace asks for them whenever bytes of a member reach a recovery object,
// which for bytes held until their member was decided is later than the
// delivery that brought them.
#[derive(Debug, Default)]
pub(crate) struct Checksums {
    volumes: BTreeMap<VolumeIndex, BTreeMap<u64, Segment>>,
    // The starts of the segments each change put in or took out.
    marks: BTreeMap<VolumeIndex, Marks<u64>>,
}

impl Checksums {
    // The checksums kept for a range are those of the bytes last delivered
    // there: an article delivered again, cut at other boundaries, replaces
    // every checksum its range overlaps.
    pub(crate) fn record(&mut self, volume: VolumeIndex, segments: &[Segment]) {
        let kept = self.volumes.entry(volume).or_default();
        let marks = self.marks.entry(volume).or_default();
        for segment in segments {
            let stale: Vec<u64> = kept
                .range(..segment.range.end)
                .rev()
                .take_while(|(_, kept)| kept.range.end > segment.range.start)
                .map(|(start, _)| *start)
                .collect();
            for start in stale {
                kept.remove(&start);
                marks.mark(start);
            }
            kept.insert(segment.range.start, *segment);
            marks.mark(segment.range.start);
        }
    }
}

// The root's face toward the namespace write path: the block-aligned
// checksums of the bytes it delivered, in root coordinates, where member M
// is volume M. A segment is returned when it overlaps the asked range; the
// namespace trims and re-checks whatever it does not take whole. They are
// checksums of what the source delivered, so a namespace below the root
// gets none, and checks the bytes it has in hand.
impl Segments for Checksums {
    fn segments_for(
        &self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        out: &mut Vec<Segment>,
    ) {
        if node != NodeId::ROOT {
            return;
        }
        let Some(kept) = self.volumes.get(&volume) else {
            return;
        };
        // A segment starting before the range may still reach into it.
        let first = kept
            .range(..range.start)
            .next_back()
            .map_or(range.start, |(start, _)| *start);
        out.extend(
            kept.range(first..range.end)
                .map(|(_, segment)| *segment)
                .filter(|segment| segment.range.intersect(range).is_some()),
        );
    }
}
