// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Member bytes out. A delivery's file data goes to the entry its block was
// linked to: a stored file's bytes are its member's, an open AES file's go
// through its cipher, a locked one's stay held. Two checksums are folded as
// the bytes pass: every part but the last carries the CRC of its own packed
// bytes, and the last carries the whole file's. A part that fails its own is
// damaged at its range; a file that fails as a whole is damaged at the parts
// that failed, or, when none did, at the parts nothing proved.

use bytes::Bytes;
use weaver_graph::{ByteRange, Emit, MemberId, Provenance, Segment, VolumeIndex};

use super::kdf::keyed_crc;
use super::{Entry, Lock, Proof, RarStore};
use crate::grid::{Folded, WHOLE, crc32};

// A fold against the CRC a header wrote, keyed when the file's checksums
// are: `None` while unfolded or while the key is not known.
fn holds(entry: &Entry, folded: Option<Folded>, expected: u32) -> Option<bool> {
    let folded = folded?;
    let crc = match (entry.keyed(), &entry.hash_key) {
        (false, _) => folded.crc32,
        (true, Some(key)) => keyed_crc(key, folded.crc32),
        (true, None) => return None,
    };
    Some(crc == expected)
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
        let Some(found) = self.volumes.get(&volume) else {
            return;
        };
        let linked: Vec<(usize, usize, ByteRange)> = found
            .walk
            .blocks
            .iter()
            .enumerate()
            .filter_map(|(index, block)| {
                let part = block.data.intersect(range)?;
                let (entry, piece) = self.links.get(&(volume, index))?;
                Some((*entry, *piece, part))
            })
            .collect();
        for (id, piece, part) in linked {
            let slice =
                &bytes[(part.start - range.start) as usize..(part.end - range.start) as usize];
            self.check_part(id, piece, part, slice, provenance, out);
            let entry = &mut self.entries[id];
            let data = entry.pieces[piece].data;
            let stream = entry.pieces[piece].at + (part.start - data.start);
            let mut plains = Vec::new();
            match &mut entry.lock {
                Lock::Locked => continue,
                Lock::Plain => {}
                Lock::Open(cbc) => {
                    cbc.deliver(stream, slice, provenance, &mut plains);
                    for plain in plains {
                        self.record(id, plain.offset, &plain.bytes, plain.provenance, out);
                    }
                    continue;
                }
            }
            self.record(id, stream, slice, provenance, out);
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
        let Some(expected) = entry.pieces[piece].crc else {
            return;
        };
        let data = entry.pieces[piece].data;
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
        let proof = match (holds(entry, folded, expected), folded) {
            (Some(false), _) => Proof::Damaged,
            (Some(true), Some(folded)) if folded.verified => Proof::Intact,
            _ => Proof::Unknown,
        };
        let was = std::mem::replace(&mut entry.pieces[piece].proof, proof);
        if proof == Proof::Damaged && was != Proof::Damaged {
            out.push(Emit::Damaged {
                volume: entry.pieces[piece].volume,
                range: data,
            });
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
            holds(entry, folded, expected)
        });
        match verdict {
            Some(false) if !entry.out.damaged => {
                entry.out.damaged = true;
                let failed: Vec<usize> = (0..entry.pieces.len())
                    .filter(|piece| entry.pieces[*piece].proof == Proof::Damaged)
                    .collect();
                let suspects: Vec<usize> = match failed.is_empty() {
                    true => (0..entry.pieces.len())
                        .filter(|piece| entry.pieces[*piece].proof != Proof::Intact)
                        .collect(),
                    false => failed,
                };
                for piece in suspects {
                    let piece = &entry.pieces[piece];
                    out.push(Emit::Damaged {
                        volume: piece.volume,
                        range: piece.data,
                    });
                }
            }
            Some(true) => entry.out.damaged = false,
            _ => {}
        }
    }
}
