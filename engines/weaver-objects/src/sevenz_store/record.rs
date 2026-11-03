// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Member bytes out. A delivery is routed through the map: a stored folder's
// bytes are its files' bytes, an open AES folder's go through its cipher,
// and a locked one's stay held. Each member closes once its whole extent is
// covered, and its CRC32 is folded as its bytes pass, so a checksum that
// fails marks the member's extent damaged for the recovery beside it, unless
// the bytes are verified and the key that read them is on trial: then it is
// the key that is wrong.

use std::sync::Arc;

use weaver_graph::{
    Algorithm, ByteRange, Emit, Evidenced, MemberId, Placement, Provenance, Segment, SliceVerdict,
};

use super::map::Map;
use super::place::read_folder;
use super::{Lock, Out, SevenZipStore};
use crate::crypt::{Placed, worse};
use crate::grid::{self, WHOLE};

// What of an AES folder's plaintext its members have placed: the members'
// coverage at their offsets in the folder. Bytes no member holds were
// placed nowhere, so the cipher keeps their blocks.
pub(super) struct FolderPlaced<'a> {
    pub(super) map: &'a Map,
    pub(super) folder: usize,
    pub(super) members: &'a [Out],
}

impl Placed for FolderPlaced<'_> {
    fn placed(&self, range: ByteRange) -> Option<Provenance> {
        let mut worst = Provenance::Fresh;
        let mut cursor = range.start;
        for (member, entry) in self.map.entries_in(self.folder, range) {
            let Some(part) = range.intersect(entry.span()) else {
                continue;
            };
            if part.start > cursor {
                return None;
            }
            let said = self.members.get(member.0 as usize)?;
            let placed = said.coverage.placed(part.shift(-(entry.offset as i64)))?;
            worst = worse(worst, placed);
            cursor = part.end;
        }
        (cursor >= range.end).then_some(worst)
    }
}

impl SevenZipStore {
    pub(super) fn route(
        &mut self,
        range: ByteRange,
        bytes: &Evidenced,
        provenance: Provenance,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        let Some(map) = self.map.take() else {
            return;
        };
        let mut plains = Vec::new();
        for (index, folder) in map.folders_in(range) {
            let Some(part) = range.intersect(folder.pack) else {
                continue;
            };
            let slice =
                bytes.slice((part.start - range.start) as usize..(part.end - range.start) as usize);
            let local = part.shift(-(folder.pack.start as i64));
            match &mut self.folders[index] {
                Lock::Plain => {}
                Lock::Locked | Lock::Sealed(..) => continue,
                Lock::Open(cbc, padding) => {
                    let (cbc, padding) = (Arc::make_mut(cbc), Arc::make_mut(padding));
                    let placed = FolderPlaced {
                        map: &map,
                        folder: index,
                        members: &self.members,
                    };
                    let mut plain = |wanted: ByteRange, buffer: &mut Vec<u8>| {
                        read_folder(&map, index, wanted, read, buffer)
                    };
                    let arrival = (local.start, &slice[..], provenance);
                    cbc.deliver(&placed, padding, arrival, &mut plain, &mut plains);
                    // Deciphered bytes carry no evidence: nothing hashed them.
                    for plain in std::mem::take(&mut plains) {
                        let span = ByteRange::at(plain.offset, plain.bytes.len() as u64);
                        let bytes = Evidenced::from(plain.bytes);
                        self.emit_plain(&map, index, span, &bytes, plain.provenance, out);
                    }
                    continue;
                }
            }
            self.emit_plain(&map, index, local, &slice, provenance, out);
        }
        self.map = Some(map);
    }

    // Plaintext of a folder, at folder offsets, to the members it is.
    fn emit_plain(
        &mut self,
        map: &Map,
        folder: usize,
        range: ByteRange,
        bytes: &Evidenced,
        provenance: Provenance,
        out: &mut Vec<Emit>,
    ) {
        for (member, entry) in map.entries_in(folder, range) {
            let Some(part) = range.intersect(entry.span()) else {
                continue;
            };
            let slice =
                bytes.slice((part.start - range.start) as usize..(part.end - range.start) as usize);
            let offset = part.start - entry.offset;
            self.record(map, member, offset, slice, provenance, out);
        }
    }

    fn record(
        &mut self,
        map: &Map,
        member: MemberId,
        offset: u64,
        mut bytes: Evidenced,
        provenance: Provenance,
        out: &mut Vec<Emit>,
    ) {
        let Some(entry) = map.entry(member) else {
            return;
        };
        let range = ByteRange::at(offset, bytes.len() as u64);
        // A member that states no CRC has nothing to fold its bytes against.
        // One that does hashes here once what no level above proved, and
        // hands the digest down with the bytes.
        if entry.crc.is_some() {
            bytes.fill(Algorithm::Crc32);
            let segment = Segment {
                range,
                crc32: bytes.digest(Algorithm::Crc32).value as u32,
                verified: provenance.is_verified(),
            };
            self.grid.offer(member, WHOLE, &segment);
        }
        out.push(Emit::Bytes {
            member,
            offset,
            bytes,
            provenance,
        });
        let whole = ByteRange::new(0, entry.length);
        let folded = self.grid.fold(member, WHOLE, 0, whole);
        let verdict = entry
            .crc
            .map(|expected| grid::verdict(folded, entry.length, 0, expected));
        // Verified bytes that fail under a key nothing proved condemn the
        // key, not the bytes.
        let enciphered = entry
            .folder
            .is_some_and(|folder| map.folders[folder].aes.is_some());
        if verdict == Some(SliceVerdict::Damaged)
            && enciphered
            && self.on_trial()
            && folded.is_some_and(|folded| folded.verified)
        {
            self.wrong_key = true;
        }
        let Some(said) = Arc::make_mut(&mut self.members).get_mut(member.0 as usize) else {
            return;
        };
        said.coverage.write(range, provenance);
        if !said.closed && said.coverage.covered(whole) {
            said.closed = true;
            out.push(Emit::MemberClosed(member));
        }
        match verdict {
            Some(SliceVerdict::Damaged) if !said.damaged && !self.wrong_key => {
                said.damaged = true;
                let mut extents = Vec::new();
                self.extent_in(map, member, whole, &mut extents);
                out.extend(extents.into_iter().map(|extent| Emit::Damaged {
                    volume: extent.volume,
                    range: extent.range,
                }));
            }
            Some(SliceVerdict::Intact) => said.damaged = false,
            _ => {}
        }
    }
}
