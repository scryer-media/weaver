// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The decoder's checksums out as facts and back, and the check a restore
// makes of bytes it reads back against them.

use weaver_graph::{ByteRange, Segment, VolumeIndex};

use super::Checksums;
use crate::pipeline::graph::root::facts::VolumeFacts;

impl Checksums {
    pub(crate) fn facts(&self, volumes: &mut [VolumeFacts]) {
        for (volume, kept) in &self.volumes {
            let Some(facts) = volumes.get_mut(volume.0 as usize) else {
                continue;
            };
            facts.segments = kept
                .values()
                .map(|segment| {
                    (
                        segment.range.start,
                        segment.range.end,
                        segment.crc32,
                        segment.verified,
                    )
                })
                .collect();
        }
    }

    pub(crate) fn restore(&mut self, volumes: &[VolumeFacts]) {
        for (index, facts) in volumes.iter().enumerate() {
            let segments: Vec<Segment> = facts
                .segments
                .iter()
                .filter(|(start, end, _, _)| start < end)
                .map(|(start, end, crc32, verified)| Segment {
                    range: ByteRange::new(*start, *end),
                    crc32: *crc32,
                    verified: *verified,
                })
                .collect();
            if !segments.is_empty() {
                self.record(VolumeIndex(index as u32), &segments);
            }
        }
    }

    // Whether bytes read back for `range` match every checksum the decoder
    // took wholly inside it. A range with none is taken as read.
    pub(crate) fn matches(&self, volume: VolumeIndex, range: ByteRange, bytes: &[u8]) -> bool {
        let Some(kept) = self.volumes.get(&volume) else {
            return true;
        };
        kept.range(range.start..range.end)
            .map(|(_, segment)| segment)
            .filter(|segment| segment.range.end <= range.end)
            .all(|segment| {
                let from = (segment.range.start - range.start) as usize;
                let to = (segment.range.end - range.start) as usize;
                bytes
                    .get(from..to)
                    .is_some_and(|piece| crc32(piece) == segment.crc32)
            })
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32
}
