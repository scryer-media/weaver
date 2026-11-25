// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The SFV object: a list of names and CRC32s. It binds as a recovery set
// and always stands aside: it is never fed, never repairs, never rules,
// vouches or proves, and never holds a job up. The job ends as it would with
// no listing. An SFV is often written over files the post does not carry,
// or over files as they were before they were packed again.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    ByteRange, Delivery, Durable, Emit, Extent, Facts, Give, Kind, Located, MemberId, Object,
    ObjectId, Placement, Pull, Recovery, Seek, Segment, SliceVerdict, Take, VolumeIndex,
};

// An SFV is a line per file; past this it is not one.
const TEXT_CAP: usize = 4 * 1024 * 1024;

// Its facts are the whole of it: the text.
#[derive(Serialize, Deserialize)]
pub struct Sfv {
    id: ObjectId,
    // The text as far as it arrived contiguously.
    text: Vec<u8>,
    // Pieces that arrived past the end of the text, by offset, until the
    // text reaches them. They are not facts: a restored SFV asks again for
    // whatever lies past its text.
    #[serde(skip)]
    held: BTreeMap<usize, Vec<u8>>,
}

impl Durable for Sfv {
    // 2: the grid shelved by chunk of blocks, kept apart from the bytes.
    // 3: a sealed value encodes as nothing, numbered by its owner's walk.
    // 4: a listing stands aside: it binds and checks nothing.
    const VERSION: u16 = 4;

    fn parts(&self, _take: &mut Take) -> Result<(), String> {
        Ok(())
    }

    fn restore_parts(&mut self, _give: &mut Give) -> Result<(), String> {
        Ok(())
    }

    fn impossible(&self) -> Option<&'static str> {
        (self.text.len() > TEXT_CAP).then_some("an overlong text")
    }
}

impl Sfv {
    pub fn new(id: ObjectId) -> Self {
        Self {
            id,
            text: Vec::new(),
            held: BTreeMap::new(),
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

    // What lies past the text, held then or never heard while the object was
    // out for a check, is asked for again.
    fn restored(&mut self, out: &mut Vec<Emit>) {
        if self.text.len() < TEXT_CAP {
            out.push(Emit::Again {
                volume: VolumeIndex(0),
                range: ByteRange::new(self.text.len() as u64, TEXT_CAP as u64),
            });
        }
    }

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

// A listing stands aside (a set that does not rebuild): the namespace
// never feeds it as a cover, never asks it to rule and never reads a
// verdict of it. What it must answer as a recovery object it answers as
// covering nothing.
impl Recovery for Sfv {
    fn rebuilds(&self) -> bool {
        false
    }

    fn carried_inside(&self) -> bool {
        false
    }

    fn outgrows_recovery(&self) -> bool {
        false
    }

    fn covers(&self, _member: MemberId) -> bool {
        false
    }

    fn covers_some(&self) -> bool {
        false
    }

    // A listing ships beside what it lists.
    fn released(&self) -> bool {
        true
    }

    fn feed(&mut self, _member: MemberId, _segments: &[Segment]) {}

    fn verdicts(&self, _member: MemberId, _out: &mut Vec<SliceVerdict>) {}

    fn wants(&self, _out: &mut Vec<Seek>) {}

    fn repair(&mut self, _access: &mut dyn MemberAccess, _: weaver_graph::Budget) -> RepairOutcome {
        RepairOutcome::Clean
    }

    fn outcome(&self) -> Option<RepairOutcome> {
        None
    }
}

#[cfg(test)]
mod tests;
