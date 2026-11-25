// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The SFV object: a list of names and CRC32s. It binds as a recovery set
// and always stands aside: it is never fed, never repairs, never rules,
// vouches or proves, and never holds a job up. The job ends as it would with
// no listing. An SFV is often written over files the post does not carry,
// or over files as they were before they were packed again.
//
// What it does do it does from the source's checksums of whole members,
// asked once a run: it notes a file it names whose CRC disagrees, and it
// names a file no entry names whose CRC only one entry states. It never
// reads a file back, so a member the source kept no checksums of, one
// below the root or one a set sealed, is neither noted nor named.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    ByteRange, Delivery, Durable, Emit, Extent, Facts, Give, Kind, Located, MemberId, Namespace,
    Object, ObjectId, Placement, Pull, Recovery, Seek, Segment, SliceVerdict, Take, VolumeIndex,
};

use crate::grid::{crc32, crc32_combine};

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
    // The entries of the text, parsed when the text changes. A restored
    // listing parses its text again where it reads them.
    #[serde(skip)]
    listed: Option<Listing>,
    // What the source's checksums said of each member asked for, by
    // member id, under the name it had then. Kept with the text, so a
    // listing restored, or back from being out of its node, asks no
    // member again. Bounded by the members asked for, at most the members
    // of its node, each once: one fixed record of a name, a length and a
    // CRC32.
    checked: BTreeMap<u32, Checked>,
}

// Entries by lower-cased basename: the basename in the listing's own case
// and the CRC32 it states.
type Listing = BTreeMap<String, Entry>;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    name: String,
    crc32: u32,
}

// A member's CRC32, folded from the source's checksums of all of its
// bytes, or `None` when the source no longer kept every one of them.
#[derive(Serialize, Deserialize)]
struct Checked {
    name: String,
    length: u64,
    crc32: Option<u32>,
}

impl Durable for Sfv {
    // 2: the grid shelved by chunk of blocks, kept apart from the bytes.
    // 3: a sealed value encodes as nothing, numbered by its owner's walk.
    // 4: a listing stands aside: it binds, rules nothing, and keeps the
    //    CRC32 the source's checksums gave each member it asked for.
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
            listed: None,
            checked: BTreeMap::new(),
        }
    }

    // The entries, parsed now when the text was restored and not read since.
    fn with_listing<R>(&self, read: impl FnOnce(&Listing) -> R) -> R {
        match &self.listed {
            Some(listed) => read(listed),
            None => read(&entries(&self.text)),
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
fn entries(text: &[u8]) -> Listing {
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
        let basename = last_component(name);
        if basename.is_empty() {
            continue;
        }
        insert_entry(&mut entries, &mut conflicting, basename, crc32);
    }
    entries
}

fn insert_entry(entries: &mut Listing, conflicting: &mut BTreeSet<String>, name: &str, crc32: u32) {
    let key = name.to_ascii_lowercase();
    if conflicting.contains(&key) {
        return;
    }
    match entries.get(&key) {
        // A listing repeated verbatim is not a contradiction; the first
        // spelling of the name stands.
        Some(existing) if existing.crc32 == crc32 => {}
        Some(_) => {
            entries.remove(&key);
            conflicting.insert(key);
        }
        None => {
            let name = name.to_owned();
            entries.insert(key, Entry { name, crc32 });
        }
    }
}

// The last path component, in its own case. Both separators are honoured
// because a listing packed on Windows records `subdir\file.rar` and the same
// release unpacked elsewhere records `subdir/file.rar`.
fn last_component(name: &str) -> &str {
    name.rsplit(['/', '\\']).next().unwrap_or(name).trim()
}

// The key a name answers an entry under: its last component, lower-cased.
fn sfv_basename(name: &str) -> String {
    last_component(name).to_ascii_lowercase()
}

// The CRC32 of a member `length` long from the source's checksums of its
// bytes: verified, in order, each starting where the last ended, from the
// first byte to the last. Anything short of that says nothing.
fn folded(segments: &[Segment], length: u64) -> Option<u32> {
    let mut sorted: Vec<&Segment> = segments.iter().collect();
    sorted.sort_by_key(|segment| segment.range.start);
    let mut at = 0;
    let mut crc = crc32(&[]);
    for segment in sorted {
        if !segment.verified || segment.range.start != at {
            return None;
        }
        crc = crc32_combine(crc, segment.crc32, segment.range.len());
        at = segment.range.end;
    }
    (at == length).then_some(crc)
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
        self.listed = Some(entries(&self.text));
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

    // The whole members no set that stands covers: those it names, for a
    // note, and while an entry names no member, those no entry names, for
    // a name. Each once.
    fn evidence(
        &self,
        namespace: &Namespace,
        covered: &dyn Fn(MemberId) -> bool,
        out: &mut Vec<MemberId>,
    ) {
        self.with_listing(|listed| {
            if listed.is_empty() {
                return;
            }
            let mut answered = BTreeSet::new();
            let mut unnamed = Vec::new();
            for member in namespace.members() {
                let key = sfv_basename(&member.name);
                let named = listed.contains_key(&key);
                if named {
                    answered.insert(key);
                }
                if !member.is_whole()
                    || self.checked.contains_key(&member.id.0)
                    || covered(member.id)
                {
                    continue;
                }
                if named {
                    out.push(member.id);
                } else {
                    unnamed.push(member.id);
                }
            }
            if answered.len() < listed.len() {
                out.extend(unnamed);
            }
        });
    }

    // An entry no member answers names the one member its CRC32 is the
    // CRC32 of, when no other entry no member answers states that CRC32
    // and no other such member has it. A member is such when its CRC32
    // was asked for under the name it still has, while no entry named it.
    // The name is the entry's, in the listing's own case. A set that names
    // the member after renames it again, and the listing never takes it
    // back: its name is no longer the one it was asked under.
    fn bind(&mut self, namespace: &mut Namespace, renamed: &mut Vec<MemberId>) {
        if self.listed.is_none() {
            self.listed = Some(entries(&self.text));
        }
        let Some(listed) = &self.listed else {
            return;
        };
        let answered: BTreeSet<String> = namespace
            .members()
            .map(|member| sfv_basename(&member.name))
            .filter(|key| listed.contains_key(key))
            .collect();
        if answered.len() == listed.len() {
            return;
        }
        // Each CRC32 the entries no member answers state: the entry, or
        // `None` when two of them state it.
        let mut wanted: BTreeMap<u32, Option<&Entry>> = BTreeMap::new();
        for (key, entry) in listed {
            if answered.contains(key) {
                continue;
            }
            wanted
                .entry(entry.crc32)
                .and_modify(|one| *one = None)
                .or_insert(Some(entry));
        }
        // Each wanted CRC32 the candidates have: the member, or `None` when
        // two of them have it.
        let mut found: BTreeMap<u32, Option<MemberId>> = BTreeMap::new();
        for (member, checked) in &self.checked {
            let Some(crc32) = checked.crc32 else {
                continue;
            };
            if !wanted.contains_key(&crc32) {
                continue;
            }
            let member = MemberId(*member);
            let candidate = namespace.get(member).is_some_and(|record| {
                record.name == checked.name
                    && record.length == Some(checked.length)
                    && !listed.contains_key(&sfv_basename(&record.name))
            });
            if candidate {
                found
                    .entry(crc32)
                    .and_modify(|one| *one = None)
                    .or_insert(Some(member));
            }
        }
        for (crc32, member) in found {
            let (Some(member), Some(Some(entry))) = (member, wanted.get(&crc32)) else {
                continue;
            };
            if namespace.rename(member, &entry.name).is_some() {
                renamed.push(member);
            }
        }
    }

    fn evidenced(&mut self, namespace: &Namespace, member: MemberId, segments: &[Segment]) {
        let Some(record) = namespace.get(member) else {
            return;
        };
        let Some(length) = record.length else {
            return;
        };
        self.checked.insert(
            member.0,
            Checked {
                name: record.name.clone(),
                length,
                crc32: folded(segments, length),
            },
        );
    }

    // Each member it names whose bytes, as the source checked them at the
    // length the member has, have another CRC32 than the entry states.
    fn disputed(&self, namespace: &Namespace, out: &mut Vec<MemberId>) {
        self.with_listing(|listed| {
            for (member, checked) in &self.checked {
                let member = MemberId(*member);
                let Some(record) = namespace.get(member) else {
                    continue;
                };
                let Some(crc32) = checked.crc32 else {
                    continue;
                };
                let Some(entry) = listed.get(&sfv_basename(&record.name)) else {
                    continue;
                };
                if record.length == Some(checked.length) && entry.crc32 != crc32 {
                    out.push(member);
                }
            }
        });
    }

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
