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
use weaver_graph::{
    ByteRange, Digest, Evidence, Evidenced, Marks, MemberId, NodeId, Provenance, Segment,
    VolumeIndex,
};

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
    // Where the article's part ends, as its header declares it: where the
    // next article begins, whatever its own bytes decoded to.
    pub(crate) declared_end: Option<u64>,
    // The checksums of the article's bytes, cut at the recovery set's block
    // boundaries when the decoder knew them.
    pub(crate) segments: Vec<Segment>,
}

impl Decoded {
    // The article as its header declares it. Bytes that decoded past the
    // declared end are not its own: they are cut off, with the checksums
    // that reach them, and the article is damaged. An intact article ends
    // where it declares, so this cuts nothing of one.
    pub(crate) fn declared(mut self) -> Self {
        let Some(end) = self.declared_end else {
            return self;
        };
        let own = end.saturating_sub(self.offset);
        if self.bytes.len() as u64 <= own {
            return self;
        }
        self.bytes.truncate(own as usize);
        self.intact = false;
        self.segments.retain(|segment| segment.range.end <= end);
        for segment in &mut self.segments {
            segment.verified = false;
        }
        self
    }

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

    // The article's bytes go down with the checksums the decoder computed
    // of them, damaged or not: each is the CRC32 of the bytes as decoded, so
    // every level below folds it rather than hash the bytes again.
    pub(crate) fn into_arrival(self) -> Arrival {
        let range = self.range();
        let evidence = self
            .segments
            .iter()
            .filter(|segment| range.covers(segment.range))
            .map(|segment| Evidence {
                range: segment.range.shift(-(self.offset as i64)),
                digest: Digest::crc32(segment.crc32),
            });
        Arrival {
            node: NodeId::ROOT,
            member: MemberId(self.volume.0),
            offset: self.offset,
            provenance: self.provenance(),
            bytes: Evidenced::with(self.bytes, evidence),
        }
    }
}

// The checksums the decoder computed, kept until their member is proven
// whole: the namespace asks for them whenever bytes of a member reach a
// recovery object, which for bytes held until their member was decided is
// later than the delivery that brought them. A member a set proved has no
// bytes left for a set to check, and its volume is sealed.
#[derive(Debug, Default)]
pub(crate) struct Checksums {
    volumes: BTreeMap<VolumeIndex, BTreeMap<u64, Segment>>,
    // The starts of the segments each change put in or took out.
    marks: BTreeMap<VolumeIndex, Marks<u64>>,
    // How many segments are kept across every volume, and whether the
    // guard below has said they passed it.
    kept: usize,
    warned: bool,
}

// Past this many kept segments the job holds checksums for more than any
// post needs at once: something proven is not being sealed. A guard that
// says so, not a limit; nothing is dropped for it.
const KEPT_GUARD: usize = 1 << 22;

impl Checksums {
    // The checksums kept for a range are those of the bytes last delivered
    // there: an article delivered again, cut at other boundaries, replaces
    // every checksum its range overlaps.
    pub(crate) fn record(&mut self, volume: VolumeIndex, segments: &[Segment]) {
        let kept = self.volumes.entry(volume).or_default();
        let marks = self.marks.entry(volume).or_default();
        for segment in segments {
            // Overlapped checksums go nearest first, straight out of the
            // map, with no list of them gathered on the way.
            while let Some((&start, overlapped)) = kept.range(..segment.range.end).next_back()
                && overlapped.range.end > segment.range.start
            {
                kept.remove(&start);
                marks.mark(start);
                self.kept -= 1;
            }
            kept.insert(segment.range.start, *segment);
            marks.mark(segment.range.start);
            self.kept += 1;
        }
        if self.kept > KEPT_GUARD && !self.warned {
            self.warned = true;
            tracing::warn!(
                kept = self.kept,
                "the job keeps more decoder checksums than any post needs"
            );
        }
    }

    // The volume's member is proven whole: no set reads the source's
    // checksums of its bytes again. Each segment dropped is marked, so the
    // facts drop it too.
    pub(crate) fn seal(&mut self, volume: VolumeIndex) {
        let Some(kept) = self.volumes.remove(&volume) else {
            return;
        };
        self.kept -= kept.len();
        let marks = self.marks.entry(volume).or_default();
        for start in kept.keys() {
            marks.mark(*start);
        }
    }

    // Whether any checksum of the volume is kept.
    #[cfg(test)]
    pub(crate) fn holds(&self, volume: VolumeIndex) -> bool {
        self.volumes
            .get(&volume)
            .is_some_and(|kept| !kept.is_empty())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(start: u64, end: u64, crc32: u32) -> Segment {
        Segment {
            range: ByteRange::new(start, end),
            crc32,
            verified: true,
        }
    }

    // An article delivered again replaces the checksums its range overlaps,
    // straight out of the kept map: the same checksums kept and the same
    // starts marked, and nothing allocated to find them.
    #[test]
    fn a_redelivery_replaces_what_it_overlaps_without_allocating() {
        let volume = VolumeIndex(3);
        let mut checksums = Checksums::default();
        checksums.record(
            volume,
            &[
                segment(0, 10, 1),
                segment(10, 20, 2),
                segment(20, 30, 3),
                segment(30, 40, 4),
            ],
        );
        let marked = checksums.marks[&volume].at();
        let redelivery = [segment(0, 25, 9)];

        let before = crate::alloc_probe::allocations();
        checksums.record(volume, &redelivery);
        assert_eq!(
            crate::alloc_probe::allocations(),
            before,
            "replacing overlapped checksums allocates nothing"
        );

        assert_eq!(checksums.kept, 2);
        let mut held = Vec::new();
        checksums.segments_for(NodeId::ROOT, volume, ByteRange::new(0, 40), &mut held);
        assert_eq!(held, vec![segment(0, 25, 9), segment(30, 40, 4)]);
        assert_eq!(
            checksums.marks[&volume].since(marked).collect::<Vec<_>>(),
            vec![20, 10, 0],
            "each overlapped start is marked, nearest first; the new segment \
             marks its start again last"
        );
    }
}
