// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Member bytes out. A delivery is routed through the map: a stored entry's
// data is its member's bytes. Each member closes once its whole extent is
// covered, and its CRC32 is folded as its bytes pass, so a checksum that
// fails marks the member's extent damaged for the recovery beside it. Data
// that arrived before its header placed it, in a range the object kept to
// read the directory, is said from what was kept.

use bytes::Bytes;
use weaver_graph::{ByteRange, Emit, MemberId, Provenance, Segment, SliceVerdict};

use super::Zip;
use crate::grid::{self, WHOLE, crc32};

impl Zip {
    pub(super) fn route(
        &mut self,
        range: ByteRange,
        bytes: &[u8],
        provenance: Provenance,
        out: &mut Vec<Emit>,
    ) {
        let Some(map) = &self.map else {
            return;
        };
        let parts: Vec<(MemberId, ByteRange, u64)> = map
            .from(range.start)
            .take_while(|(_, entry)| entry.header < range.end)
            .filter_map(|(_, entry)| {
                let span = entry.span()?;
                let part = range.intersect(span)?;
                Some((entry.member?, part, span.start))
            })
            .collect();
        for (member, part, start) in parts {
            let slice =
                &bytes[(part.start - range.start) as usize..(part.end - range.start) as usize];
            self.record(member, part.start - start, slice, provenance, out);
        }
    }

    // What the object kept of a member's data, said once the data is placed.
    pub(super) fn emit_kept(&mut self, member: MemberId, out: &mut Vec<Emit>) {
        let Some(span) = self
            .map
            .as_ref()
            .and_then(|map| map.entry(member))
            .and_then(|entry| entry.span())
        else {
            return;
        };
        let mut runs = Vec::new();
        for wanted in self.kept.ranges_in(span) {
            let mut cursor = wanted.start;
            while cursor < wanted.end {
                let run = self.kept.from(cursor);
                if run.is_empty() {
                    match self.kept.next_in_hand(cursor) {
                        Some(next) if next < wanted.end => cursor = next,
                        _ => break,
                    }
                    continue;
                }
                let len = (run.len() as u64).min(wanted.end - cursor);
                let part = ByteRange::at(cursor, len);
                let provenance = if self.kept.verified(part) {
                    Provenance::Fresh
                } else {
                    Provenance::Damaged
                };
                runs.push((part, run[..len as usize].to_vec(), provenance));
                cursor = part.end;
            }
        }
        for (part, bytes, provenance) in runs {
            self.record(member, part.start - span.start, &bytes, provenance, out);
        }
    }

    fn record(
        &mut self,
        member: MemberId,
        offset: u64,
        bytes: &[u8],
        provenance: Provenance,
        out: &mut Vec<Emit>,
    ) {
        let Some((length, expected)) = self
            .map
            .as_ref()
            .and_then(|map| map.entry(member))
            .map(|entry| (entry.length, entry.crc))
        else {
            return;
        };
        let range = ByteRange::at(offset, bytes.len() as u64);
        let Some(said) = self.members.get(member.0 as usize) else {
            return;
        };
        // Bytes said already, as they were, are not said again.
        if said.coverage.covered(range)
            && said
                .coverage
                .overlapping(range)
                .all(|run| run.provenance == provenance)
        {
            return;
        }
        out.push(Emit::Bytes {
            member,
            offset,
            bytes: Bytes::copy_from_slice(bytes),
            provenance,
        });
        self.grid.offer(
            member,
            WHOLE,
            &Segment {
                range,
                crc32: crc32(bytes),
                verified: provenance.is_verified(),
            },
        );
        let whole = ByteRange::new(0, length);
        let folded = self.grid.fold(member, WHOLE, 0, whole);
        let verdict = grid::verdict(folded, length, 0, expected);
        let Some(said) = self.members.get_mut(member.0 as usize) else {
            return;
        };
        said.coverage.write(range, provenance);
        if !said.closed && said.coverage.covered(whole) {
            said.closed = true;
            out.push(Emit::MemberClosed(member));
        }
        match verdict {
            SliceVerdict::Damaged if !said.damaged => {
                said.damaged = true;
                let mut extents = Vec::new();
                self.member_extent(member, whole, &mut extents);
                out.extend(extents.into_iter().map(|extent| Emit::Damaged {
                    volume: extent.volume,
                    range: extent.range,
                }));
            }
            SliceVerdict::Intact => said.damaged = false,
            _ => {}
        }
    }
}
