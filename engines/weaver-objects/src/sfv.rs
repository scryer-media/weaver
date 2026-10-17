// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The SFV object: a list of names and CRC32s. It checks the members it
// names, from the whole-member CRC32 the grid folds as bytes pass, or by
// reading a member back when the feed did not cover it. It never repairs.
// An entry that names no member of the namespace fails nothing: an SFV is
// often written over files the post does not carry.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    ByteRange, Delivery, Durable, Emit, Extent, Facts, Kind, MemberId, Namespace, Object, ObjectId,
    Placement, Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex,
};

use crate::grid::{self, Grid, WHOLE};

// An SFV is a line per file; past this it is not one.
const TEXT_CAP: usize = 4 * 1024 * 1024;
// A readback is checked in pieces of this size.
const READ_CHUNK: u64 = 4 * 1024 * 1024;

// Its facts are the whole of it: the text, the checksums bound and what
// the last check came to.
#[derive(Serialize, Deserialize)]
pub struct Sfv {
    id: ObjectId,
    // The text as far as it arrived contiguously.
    text: Vec<u8>,
    // Bound members and the CRC32 their entry gives.
    bound: BTreeMap<MemberId, u32>,
    // Each bound member's length once all of it arrived.
    lengths: BTreeMap<MemberId, u64>,
    grid: Grid,
    // What the last check came to, for the members bound then.
    outcome: Option<RepairOutcome>,
}

impl Durable for Sfv {
    const VERSION: u16 = 1;

    fn impossible(&self) -> Option<&'static str> {
        let sized = self
            .lengths
            .keys()
            .all(|member| self.bound.contains_key(member));
        (self.text.len() > TEXT_CAP || !sized).then_some("an overlong text or an unbound length")
    }
}

impl Sfv {
    pub fn new(id: ObjectId) -> Self {
        Self {
            id,
            text: Vec::new(),
            bound: BTreeMap::new(),
            lengths: BTreeMap::new(),
            grid: Grid::default(),
            outcome: None,
        }
    }

    fn verdict(&self, member: MemberId, expected: u32) -> SliceVerdict {
        let Some(length) = self.lengths.get(&member).copied() else {
            return SliceVerdict::Unknown;
        };
        if length == 0 {
            return if expected == grid::crc32(&[]) {
                SliceVerdict::Intact
            } else {
                SliceVerdict::Damaged
            };
        }
        let folded = self.grid.fold(member, WHOLE, 0, ByteRange::new(0, length));
        grid::verdict(folded, length, 0, expected)
    }
}

// Entries of the text: a name, whitespace, eight hex digits. Lines starting
// with `;` are comments, and a line that is not an entry is skipped.
fn entries(text: &[u8]) -> Vec<(String, u32)> {
    String::from_utf8_lossy(text)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with(';'))
        .filter_map(|line| {
            let (name, crc) = line.rsplit_once(char::is_whitespace)?;
            let name = name.trim();
            (crc.len() == 8 && !name.is_empty())
                .then(|| u32::from_str_radix(crc, 16).ok())
                .flatten()
                .map(|crc| (name.to_owned(), crc))
        })
        .collect()
}

// The CRC32 of a member, read back in pieces.
fn read_crc32(member: MemberId, length: u64, access: &mut dyn MemberAccess) -> Option<u32> {
    let mut crc = grid::crc32(&[]);
    let mut buffer = Vec::new();
    let mut offset = 0;
    while offset < length {
        let end = (offset + READ_CHUNK).min(length);
        buffer.clear();
        if !access.read(member, ByteRange::new(offset, end), &mut buffer)
            || buffer.len() as u64 != end - offset
        {
            return None;
        }
        crc = grid::crc32_combine(crc, grid::crc32(&buffer), end - offset);
        offset = end;
    }
    Some(crc)
}

impl Object for Sfv {
    fn kind(&self) -> Kind {
        Kind::Sfv
    }

    fn deliver(&mut self, delivery: Delivery<'_>, _out: &mut Vec<Emit>) {
        let Ok(offset) = usize::try_from(delivery.offset) else {
            return;
        };
        if delivery.volume != VolumeIndex(0) || offset > self.text.len() || offset >= TEXT_CAP {
            return;
        }
        let end = offset.saturating_add(delivery.bytes.len()).min(TEXT_CAP);
        if end > self.text.len() {
            self.text.resize(end, 0);
        }
        self.text[offset..end].copy_from_slice(&delivery.bytes[..end - offset]);
    }

    fn volume_closed(&mut self, _volume: VolumeIndex, _out: &mut Vec<Emit>) {}

    fn volume_lost(&mut self, _volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {}

    fn wants(&mut self, _out: &mut Vec<Seek>) {}

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    // The text stays where the namespace put it, in the .sfv member's file.
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) {}

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }

    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }
    fn facts(&self) -> Facts {
        Facts::of(Kind::Sfv, self)
    }
}

impl Recovery for Sfv {
    // By name, exactly or, failing that, by one unique case-folded match.
    fn bind(&mut self, namespace: &mut Namespace, _renamed: &mut Vec<MemberId>) {
        let candidates: Vec<_> = namespace
            .members()
            .filter(|member| member.bound_to != Some(self.id))
            .collect();
        let mut bound = BTreeMap::new();
        for (name, crc) in entries(&self.text) {
            let exact: Vec<_> = candidates
                .iter()
                .filter(|member| member.name == name)
                .collect();
            let found = if exact.is_empty() {
                candidates
                    .iter()
                    .filter(|member| member.name.eq_ignore_ascii_case(&name))
                    .collect()
            } else {
                exact
            };
            if let [member] = found.as_slice() {
                bound.insert(member.id, crc);
            }
        }
        self.lengths = bound
            .keys()
            .filter_map(|member| {
                let record = namespace.get(*member)?;
                record
                    .length
                    .filter(|_| record.is_complete())
                    .map(|length| (*member, length))
            })
            .collect();
        if bound != self.bound {
            self.outcome = None;
        }
        self.bound = bound;
    }

    fn covers(&self, member: MemberId) -> bool {
        self.bound.contains_key(&member)
    }

    fn feed(&mut self, member: MemberId, segments: &[Segment]) {
        for segment in segments {
            self.grid.offer(member, WHOLE, segment);
        }
    }

    // One verdict for the whole member: an SFV has no slices.
    fn verdicts(&self, member: MemberId, out: &mut Vec<SliceVerdict>) {
        if let Some(expected) = self.bound.get(&member) {
            out.push(self.verdict(member, *expected));
        }
    }

    fn wants(&mut self, _out: &mut Vec<Seek>) {}

    fn repair(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome {
        let mut damaged = 0;
        for (&member, &expected) in &self.bound {
            let intact = match self.verdict(member, expected) {
                SliceVerdict::Intact => true,
                SliceVerdict::Damaged => false,
                SliceVerdict::Unknown => {
                    access
                        .length(member)
                        .and_then(|length| read_crc32(member, length, access))
                        == Some(expected)
                }
            };
            if !intact {
                damaged += 1;
            }
        }
        let outcome = if damaged == 0 {
            RepairOutcome::Clean
        } else {
            RepairOutcome::Impossible {
                damaged,
                reachable: 0,
            }
        };
        self.outcome = Some(outcome);
        outcome
    }

    fn outcome(&self) -> Option<RepairOutcome> {
        self.outcome
    }

    fn blocks_reachable(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests;
