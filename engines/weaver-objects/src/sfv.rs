// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The SFV object: a list of names and CRC32s. It checks the members it
// names, from the whole-member CRC32 the grid folds as bytes pass, or by
// reading a member back when the feed did not cover it. It never repairs.
// An entry that names no member of the namespace fails nothing: an SFV is
// often written over files the post does not carry.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    ByteRange, Delivery, Durable, Emit, Extent, Facts, Give, Kind, Located, MemberId, Namespace,
    Object, ObjectId, Placement, Pull, Recovery, Seek, Segment, SliceVerdict, Take, VolumeIndex,
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
    // Pieces that arrived past the end of the text, by offset, until the
    // text reaches them. A restart delivers every article again, so they
    // are not facts.
    #[serde(skip)]
    held: BTreeMap<usize, Vec<u8>>,
    // Bound members and the CRC32 their entry gives.
    bound: BTreeMap<MemberId, u32>,
    // Each bound member's length once all of it arrived.
    lengths: BTreeMap<MemberId, u64>,
    grid: Grid,
    // What the last check came to, for the members bound then.
    outcome: Option<RepairOutcome>,
}

impl Durable for Sfv {
    // 2: the grid shelved by chunk of blocks, kept apart from the bytes.
    // 3: a sealed value encodes as nothing, numbered by its owner's walk.
    const VERSION: u16 = 3;

    fn parts(&self, take: &mut Take) -> Result<(), String> {
        self.grid.parts(take)
    }

    fn restore_parts(&mut self, give: &mut Give) -> Result<(), String> {
        self.grid.restore_parts(give)
    }

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
            held: BTreeMap::new(),
            bound: BTreeMap::new(),
            lengths: BTreeMap::new(),
            grid: Grid::default(),
            outcome: None,
        }
    }

    // Bytes of the text at `offset`, which the text reaches.
    fn write(&mut self, offset: usize, bytes: &[u8]) {
        let end = offset + bytes.len();
        if end > self.text.len() {
            self.text.resize(end, 0);
        }
        self.text[offset..end].copy_from_slice(bytes);
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

// The entries of the text, keyed by lower-cased basename.
//
// Basenames because a listing is written relative to wherever the release was
// packed, often with DOS separators, while the members are named flat;
// lower-cased because the same listing is routinely produced on a
// case-insensitive filesystem and the case it records is not evidence of
// anything. A basename listed with disagreeing CRCs is withdrawn: the
// listing contradicts itself, so neither value is evidence.
fn entries(text: &[u8]) -> BTreeMap<String, u32> {
    let text = String::from_utf8_lossy(text);
    // A listing written on Windows commonly carries a UTF-8 BOM, which
    // would otherwise become part of the first entry's name.
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    let mut entries = BTreeMap::new();
    let mut conflicting = BTreeSet::new();
    // `str::lines` splits on `\n` and drops a trailing `\r`, so CRLF
    // listings need no separate handling.
    for raw_line in text.lines() {
        let line = raw_line.trim();
        // `;` opens a comment line, the header most generators write.
        if line.is_empty() || line.starts_with(';') {
            continue;
        }
        let Some((name, checksum)) = line.rsplit_once(char::is_whitespace) else {
            continue;
        };
        let name = name.trim_end();
        // Up to eight hex digits: canonical listings pad to eight, and a
        // few generators drop leading zeros. Anything else, a size column,
        // a stray word, an md5, is not a CRC32 and is refused rather than
        // truncated into one.
        if name.is_empty()
            || checksum.is_empty()
            || checksum.len() > 8
            || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            continue;
        }
        let Ok(crc32) = u32::from_str_radix(checksum, 16) else {
            continue;
        };
        let basename = sfv_basename(name);
        if basename.is_empty() {
            continue;
        }
        insert_entry(&mut entries, &mut conflicting, basename, crc32);
    }
    entries
}

fn insert_entry(
    entries: &mut BTreeMap<String, u32>,
    conflicting: &mut BTreeSet<String>,
    basename: String,
    crc32: u32,
) {
    if conflicting.contains(&basename) {
        return;
    }
    match entries.get(&basename) {
        // A listing repeated verbatim is not a contradiction.
        Some(existing) if *existing == crc32 => {}
        Some(_) => {
            entries.remove(&basename);
            conflicting.insert(basename);
        }
        None => {
            entries.insert(basename, crc32);
        }
    }
}

// The last path component, lower-cased. Both separators are honoured because
// a listing packed on Windows records `subdir\file.rar` and the same release
// unpacked elsewhere records `subdir/file.rar`.
fn sfv_basename(name: &str) -> String {
    name.rsplit(['/', '\\'])
        .next()
        .unwrap_or(name)
        .trim()
        .to_ascii_lowercase()
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

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _out: &mut Vec<Emit>,
    ) {
        let Ok(offset) = usize::try_from(delivery.offset) else {
            return;
        };
        if delivery.volume != VolumeIndex(0) || offset >= TEXT_CAP {
            return;
        }
        let end = offset.saturating_add(delivery.bytes.len()).min(TEXT_CAP);
        let bytes = &delivery.bytes[..end - offset];
        if offset > self.text.len() {
            self.held.insert(offset, bytes.to_vec());
            return;
        }
        self.write(offset, bytes);
        while let Some(piece) = self.held.first_entry() {
            if *piece.key() > self.text.len() {
                break;
            }
            let (at, bytes) = piece.remove_entry();
            self.write(at, &bytes);
        }
    }

    fn volume_closed(&mut self, _volume: VolumeIndex, _out: &mut Vec<Emit>) {}

    fn volume_lost(&mut self, _volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {}

    fn wants(&self, _out: &mut Vec<Seek>) {}

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    // The text stays where the namespace put it, in the .sfv member's file.
    // The text it copies out is in its facts, which a restart keeps: it says
    // nothing of the posted bytes.
    fn unposted(&mut self, _volume: VolumeIndex, _range: ByteRange) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) -> Located {
        Located::Never
    }

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
    // By lower-cased basename. Two members whose names differ only by case
    // cannot be told apart by a listing that records neither reliably, so
    // neither binds.
    fn bind(&mut self, namespace: &mut Namespace, _renamed: &mut Vec<MemberId>) {
        let mut by_basename: BTreeMap<String, Option<MemberId>> = BTreeMap::new();
        for member in namespace
            .members()
            .filter(|member| member.bound_to != Some(self.id))
        {
            by_basename
                .entry(sfv_basename(&member.name))
                .and_modify(|found| *found = None)
                .or_insert(Some(member.id));
        }
        let entries = entries(&self.text);
        let bound: BTreeMap<MemberId, u32> = by_basename
            .into_iter()
            .filter_map(|(basename, member)| Some((member?, *entries.get(&basename)?)))
            .collect();
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

    // A listing ships beside what it lists.
    fn released(&self) -> bool {
        true
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

    fn wants(&self, _out: &mut Vec<Seek>) {}

    fn repair(&mut self, access: &mut dyn MemberAccess, _: weaver_graph::Budget) -> RepairOutcome {
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
                // One checksum over the member: all of it is what failed.
                if let Some(length) = access.length(member) {
                    access.found_damaged(member, ByteRange::new(0, length));
                }
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
}

#[cfg(test)]
mod tests;
