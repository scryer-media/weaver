// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Member bytes out. A delivery's file data goes to the entry its block was
// linked to: a stored file's bytes are its member's, an open AES file's go
// through its cipher, a locked one's stay held. Two checksums are folded as
// the bytes pass: every part but the last carries the CRC of its own packed
// bytes, and the last carries the whole file's, keyed when the header says.
// A part that fails its own is damaged at its range; a file that fails as a
// whole is damaged at every part.

use bytes::Bytes;
use unrar_rs::convert_crc32_to_mac;
use weaver_graph::{ByteRange, Emit, MemberId, Provenance, Segment, VolumeIndex};

use super::{Lock, RarStore};
use crate::grid::{Folded, WHOLE, crc32};

// The composed CRC32, folded with the hash key when the header keys it;
// `None` while the key it needs is not known.
fn fold_member_crc(composed: u32, uses_mac: bool, hash_key: Option<&[u8; 32]>) -> Option<u32> {
    match (uses_mac, hash_key) {
        (true, Some(hash_key)) => Some(convert_crc32_to_mac(composed, hash_key)),
        (false, _) => Some(composed),
        (true, None) => None,
    }
}

// A fold against the CRC a header wrote: `None` while unfolded or unkeyed,
// else the folded CRC and whether it holds.
fn holds(
    folded: Option<Folded>,
    uses_mac: bool,
    hash_key: Option<&[u8; 32]>,
    expected: u32,
) -> Option<(u32, bool)> {
    let folded = folded?;
    let crc = fold_member_crc(folded.crc32, uses_mac, hash_key)?;
    Some((folded.crc32, crc == expected))
}

impl RarStore {
    pub(super) fn route(
        &mut self,
        volume: VolumeIndex,
        range: ByteRange,
        bytes: &[u8],
        provenance: Provenance,
        out: &mut Vec<Emit>,
    ) {
        for (id, part, stream) in self.linked(volume, range) {
            // The layout cuts a member's runs at its parts, so one run lies
            // in one piece; a part the entry has not taken yet routes nothing.
            let Some(pieces) = self.entries.get(id).map(|entry| &entry.pieces) else {
                continue;
            };
            let piece = pieces.partition_point(|piece| piece.at <= stream.start);
            let Some(piece) = piece.checked_sub(1) else {
                continue;
            };
            if stream.end > pieces[piece].at + pieces[piece].data.len() {
                continue;
            }
            let slice =
                &bytes[(part.start - range.start) as usize..(part.end - range.start) as usize];
            self.check_part(id, piece, part, slice, provenance, out);
            let entry = &mut self.entries[id];
            let mut plains = Vec::new();
            match &mut entry.lock {
                Lock::Locked => continue,
                Lock::Plain => {}
                Lock::Open(cbc) => {
                    cbc.deliver(stream.start, slice, provenance, &mut plains);
                    for plain in plains {
                        self.record(id, plain.offset, &plain.bytes, plain.provenance, out);
                    }
                    continue;
                }
            }
            self.record(id, stream.start, slice, provenance, out);
        }
    }

    // A part's packed bytes, against the part's own CRC.
    fn check_part(
        &mut self,
        id: usize,
        piece: usize,
        part: ByteRange,
        bytes: &[u8],
        provenance: Provenance,
        out: &mut Vec<Emit>,
    ) {
        let entry = &mut self.entries[id];
        let found = &entry.pieces[piece];
        let (Some(expected), data, volume, uses_mac) =
            (found.crc, found.data, found.volume, found.uses_mac)
        else {
            return;
        };
        let key = MemberId(piece as u32);
        entry.parts.offer(
            key,
            WHOLE,
            &Segment {
                range: part.shift(-(data.start as i64)),
                crc32: crc32(bytes),
                verified: provenance.is_verified(),
            },
        );
        let folded = entry
            .parts
            .fold(key, WHOLE, 0, ByteRange::new(0, data.len()));
        if let Some((crc, false)) = holds(folded, uses_mac, entry.hash_key.as_deref(), expected) {
            self.say_damaged(volume, data, crc, out);
        }
    }

    fn record(
        &mut self,
        id: usize,
        offset: u64,
        bytes: &[u8],
        provenance: Provenance,
        out: &mut Vec<Emit>,
    ) {
        let member = MemberId(id as u32);
        let entry = &mut self.entries[id];
        let range = ByteRange::at(offset, bytes.len() as u64);
        out.push(Emit::Bytes {
            member,
            offset,
            bytes: Bytes::copy_from_slice(bytes),
            provenance,
        });
        entry.grid.offer(
            member,
            WHOLE,
            &Segment {
                range,
                crc32: crc32(bytes),
                verified: provenance.is_verified(),
            },
        );
        entry.out.coverage.write(range, provenance);
        let Some(length) = entry.plain_len() else {
            return;
        };
        let whole = ByteRange::new(0, length);
        if !entry.out.closed && entry.out.coverage.covered(whole) {
            entry.out.closed = true;
            out.push(Emit::MemberClosed(member));
        }
        let verdict = entry.crc.and_then(|expected| {
            let folded = entry.grid.fold(member, WHOLE, 0, whole);
            holds(folded, entry.uses_mac, entry.hash_key.as_deref(), expected)
        });
        let Some((crc, false)) = verdict else {
            return;
        };
        let parts: Vec<(VolumeIndex, ByteRange)> = entry
            .pieces
            .iter()
            .map(|piece| (piece.volume, piece.data))
            .collect();
        for (volume, data) in parts {
            self.say_damaged(volume, data, crc, out);
        }
    }
}
