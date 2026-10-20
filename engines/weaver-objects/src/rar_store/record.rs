// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Member bytes out. A delivery's file data goes to the entry its block was
// linked to: a stored file's bytes are its member's, an open AES file's go
// through its cipher, a locked one's stay held. Two checksums are folded as
// the bytes pass: every part but the last carries the CRC of its own packed
// bytes, and the last carries the whole file's, keyed when the header says.
// A part that fails its own is damaged at its range; a file that fails as a
// whole is damaged at every part. Bytes that came damaged are judged by
// neither: they are already marked where they lie, the runs give no value
// over them, and the part or file is judged when their repair arrives, so
// no mark is left on a part whose bytes were never bad.

use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::{ByteRange, Emit, MemberId, Placement, Provenance, SliceVerdict, VolumeIndex};

use super::{Lock, RarStore};
use crate::grid::crc32;
use crate::runs::gate;

impl RarStore {
    pub(super) fn route(
        &mut self,
        volume: VolumeIndex,
        range: ByteRange,
        bytes: &[u8],
        provenance: Provenance,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
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
                Lock::Locked | Lock::Sealed(_) => continue,
                Lock::Plain => {}
                Lock::Open(cbc) => {
                    let (cbc, said) = (Arc::make_mut(cbc), Arc::make_mut(&mut entry.out));
                    let member = MemberId(id as u32);
                    let mut plain = |range: ByteRange, buffer: &mut Vec<u8>| {
                        read(&Placement::Member { member, range }, buffer)
                    };
                    let (coverage, padding) = (&said.coverage, &mut said.padding);
                    let arrival = (stream.start, slice, provenance);
                    cbc.deliver(coverage, padding, arrival, &mut plain, &mut plains);
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
        let hash_key = entry.hash_key.as_deref();
        let runs = entry.parts.entry(piece).or_default();
        runs.feed(part.shift(-(data.start as i64)), crc32(bytes), provenance);
        if let (SliceVerdict::Damaged, crc) = gate(runs, data.len(), expected, uses_mac, hash_key) {
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
        entry.plain_runs.feed(range, crc32(bytes), provenance);
        let plain_len = entry.plain_len();
        let said = Arc::make_mut(&mut entry.out);
        said.coverage.write(range, provenance);
        let Some(length) = plain_len else {
            return;
        };
        let whole = ByteRange::new(0, length);
        if !said.closed && said.coverage.covered(whole) {
            said.closed = true;
            out.push(Emit::MemberClosed(member));
        }
        let Some(expected) = entry.crc else {
            return;
        };
        let hash_key = entry.hash_key.as_deref();
        let (SliceVerdict::Damaged, crc) = gate(
            &entry.plain_runs,
            length,
            expected,
            entry.uses_mac,
            hash_key,
        ) else {
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
