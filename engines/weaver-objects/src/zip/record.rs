// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Member bytes out. A delivery is routed through the map: a stored entry's
// data is its member's bytes. Each member closes once its whole extent is
// covered, and its CRC32 is folded as its bytes pass, so a checksum that
// fails marks the member's extent damaged for the recovery beside it. Data
// that arrived before its header placed it, in a range the object kept to
// read the directory, is said from what was kept.

use weaver_graph::{
    Algorithm, ByteRange, Emit, Evidenced, MemberId, Placement, Provenance, Segment, SliceVerdict,
};

use super::Zip;
use crate::grid::{self, WHOLE};

impl Zip {
    pub(super) fn route(
        &mut self,
        range: ByteRange,
        bytes: &Evidenced,
        provenance: Provenance,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
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
                bytes.slice((part.start - range.start) as usize..(part.end - range.start) as usize);
            self.decrypt_record(member, part.start - start, slice, provenance, read, out);
        }
        self.emit_kept_encrypted(read, out);
    }

    // What the object kept of a member's data, said once the data is placed.
    pub(super) fn emit_kept(&mut self, member: MemberId, out: &mut Vec<Emit>) {
        if self
            .map
            .as_ref()
            .and_then(|map| map.entry(member))
            .is_some_and(|entry| entry.cipher.is_some())
        {
            return;
        }
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
                runs.push((
                    part,
                    Evidenced::from(run[..len as usize].to_vec()),
                    provenance,
                ));
                cursor = part.end;
            }
        }
        for (part, bytes, provenance) in runs {
            self.record(member, part.start - span.start, bytes, provenance, out);
        }
    }

    pub(super) fn record(
        &mut self,
        member: MemberId,
        offset: u64,
        bytes: Evidenced,
        provenance: Provenance,
        out: &mut Vec<Emit>,
    ) {
        let Some((length, expected, crc_known)) = self
            .map
            .as_ref()
            .and_then(|map| map.entry(member))
            .map(|entry| (entry.length, entry.crc, entry.crc_known))
        else {
            return;
        };
        let range = ByteRange::at(offset, bytes.len() as u64);
        let Some(said) = self.members.get(member.0 as usize) else {
            return;
        };
        // Bytes said already, as they were, are not said again.
        if !bytes.is_empty()
            && said.coverage.covered(range)
            && said
                .coverage
                .overlapping(range)
                .all(|run| run.provenance == provenance)
        {
            return;
        }
        // What no level above proved is hashed here once and handed down
        // with the bytes.
        let mut bytes = bytes;
        bytes.fill(Algorithm::Crc32);
        let segment = Segment {
            range,
            crc32: bytes.digest(Algorithm::Crc32).value as u32,
            verified: provenance.is_verified(),
        };
        self.grid.offer(member, WHOLE, &segment);
        out.push(Emit::Bytes {
            member,
            offset,
            bytes,
            provenance,
        });
        let whole = ByteRange::new(0, length);
        let folded = self.grid.fold(member, WHOLE, 0, whole);
        let verdict = if crc_known {
            grid::verdict(folded, length, 0, expected)
        } else {
            SliceVerdict::Unknown
        };
        let mut retry = false;
        let mut encrypted = false;
        let mut doubted = false;
        if let Some(map) = &mut self.map {
            let index = map.members[member.0 as usize];
            if let Some(cipher) = &mut map.entries[index].cipher {
                encrypted = true;
                if verdict == SliceVerdict::Intact {
                    cipher.proven = true;
                } else if verdict == SliceVerdict::Damaged
                    && !cipher.proven
                    && folded.is_some_and(|folded| folded.verified)
                {
                    // Another candidate to try: this one may be a false
                    // positive. The only one passing the check byte stays,
                    // and the bytes are said damaged for a repair; a later
                    // password can still be tried against them.
                    if cipher.alternative(&self.candidates) {
                        cipher.retry = true;
                        retry = true;
                    } else {
                        doubted = true;
                    }
                }
            }
        }
        if doubted && !self.asked {
            self.asked = true;
            out.push(Emit::NeedsPassword);
        }
        let Some(said) = self.members.get_mut(member.0 as usize) else {
            return;
        };
        said.coverage.write(range, provenance);
        if !retry
            && (!encrypted || verdict == SliceVerdict::Intact)
            && !said.closed
            && said.coverage.covered(whole)
        {
            said.closed = true;
            out.push(Emit::MemberClosed(member));
        }
        match verdict {
            SliceVerdict::Damaged if !said.damaged && !retry => {
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
        if retry
            || (encrypted
                && crc_known
                && folded.is_none()
                && self.members[member.0 as usize].coverage.covered(whole))
        {
            let mut extents = Vec::new();
            self.member_extent(member, whole, &mut extents);
            out.extend(extents.into_iter().map(|extent| Emit::Again {
                volume: extent.volume,
                range: extent.range,
            }));
            if retry {
                self.asked = false;
            }
        }
    }

    // A member whose whole CRC fails over verified bytes under a candidate
    // nothing proved, kept for want of another: a new password retries it.
    pub(super) fn retry_doubted(&mut self, out: &mut Vec<Emit>) {
        let Some(map) = &mut self.map else {
            return;
        };
        let mut again = Vec::new();
        for entry in &mut map.entries {
            let (Some(member), Some(cipher)) = (entry.member, &mut entry.cipher) else {
                continue;
            };
            if cipher.open.is_none()
                || cipher.proven
                || cipher.retry
                || !entry.crc_known
                || !cipher.alternative(&self.candidates)
            {
                continue;
            }
            let whole = ByteRange::new(0, entry.length);
            let folded = self.grid.fold(member, WHOLE, 0, whole);
            if grid::verdict(folded, entry.length, 0, entry.crc) != SliceVerdict::Damaged
                || !folded.is_some_and(|folded| folded.verified)
            {
                continue;
            }
            cipher.retry = true;
            again.push((member, whole));
        }
        for (member, whole) in again {
            let mut extents = Vec::new();
            self.member_extent(member, whole, &mut extents);
            out.extend(extents.into_iter().map(|extent| Emit::Again {
                volume: extent.volume,
                range: extent.range,
            }));
        }
    }
}
