// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Member bytes out. A delivery's file data goes to the entry its block was
// linked to: a stored file's bytes are its member's, an open AES file's go
// through its cipher, a locked one's stay held. Two checksums are folded as
// the bytes pass: every part but the last carries the CRC of its own packed
// bytes, and the last carries the whole file's, keyed when the header says.
// A part that fails its own is damaged at its range; a file that fails as a
// whole is damaged at the parts no check of their own vouches for, unless a
// part's own failure accounts for it. Bytes that came damaged are judged by
// neither: they are already marked where they lie, the runs give no value
// over them, and the part or file is judged when their repair arrives, so
// no mark is left on a part whose bytes were never bad.

use std::sync::Arc;

use weaver_graph::{
    Algorithm, ByteRange, Coverage, Emit, Evidenced, MemberId, Placement, Provenance, SliceVerdict,
    VolumeIndex,
};

use super::{Lock, RarStore};
use crate::crypt::block_floor;
use crate::grid::crc32;
use crate::runs::{CrcRuns, gate};

impl RarStore {
    pub(super) fn route(
        &mut self,
        volume: VolumeIndex,
        range: ByteRange,
        bytes: &Evidenced,
        provenance: Provenance,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        for (id, part, stream) in self.linked(volume, range) {
            // A condemned file's part in this volume is posted again: it is
            // placed afresh from here, and once every part is, nothing reads
            // them through the cipher they stood under.
            if let Some(condemned) = self.condemned.get_mut(&id) {
                condemned.parts.remove(&volume);
                if condemned.parts.is_empty() {
                    self.condemned.remove(&id);
                }
            }
            // The layout cuts a member's runs at its parts, so one run lies
            // in one piece; a part the entry has not taken yet routes nothing.
            let Some(piece) = self
                .entries
                .get(id)
                .and_then(|entry| entry.piece_of(stream))
            else {
                continue;
            };
            let mut slice =
                bytes.slice((part.start - range.start) as usize..(part.end - range.start) as usize);
            // A stored file's packed bytes are its member's: the part's CRC
            // and the file's fold the same digests, hashed here at most once.
            if matches!(self.entries[id].lock, Lock::Plain) {
                slice.fill(Algorithm::Crc32);
            }
            self.check_part(id, piece, (part, stream), &slice, provenance, read, out);
            let entry = &mut self.entries[id];
            let judges = entry.judges_its_checksum();
            let mut plains = Vec::new();
            match &mut entry.lock {
                Lock::Locked | Lock::Sealed(_) => continue,
                Lock::Plain => {}
                Lock::Open(cbc) => {
                    let member = MemberId(id as u32);
                    // Nothing keeps the runs across a restart, and an open
                    // cipher says nothing new of bytes that came out before
                    // it: a checksum the store judges reads back what the
                    // member holds, once, at the first bytes after it.
                    if judges && entry.plain_runs.held() == 0 {
                        let whole = vec![ByteRange::new(0, u64::MAX)];
                        let said = (member, 0, &entry.out.coverage);
                        refeed(&mut entry.plain_runs, whole, said, read);
                    }
                    let (cbc, said) = (Arc::make_mut(cbc), Arc::make_mut(&mut entry.out));
                    let mut plain = |range: ByteRange, buffer: &mut Vec<u8>| {
                        read(&Placement::Member { member, range }, buffer)
                    };
                    let (coverage, padding) = (&said.coverage, &mut said.padding);
                    let arrival = (stream.start, &slice[..], provenance);
                    cbc.deliver(coverage, padding, arrival, &mut plain, &mut plains);
                    // Deciphered bytes carry no evidence: nothing hashed them.
                    for plain in plains {
                        let bytes = Evidenced::from(plain.bytes);
                        self.record(id, (plain.offset, bytes, plain.provenance), read, out);
                    }
                    continue;
                }
            }
            self.record(id, (stream.start, slice, provenance), read, out);
        }
    }

    // The bytes posted at `range` of the volume come again: each member
    // they reached forgets what came out of them, its padding with it, and
    // is open until it comes out again. A locked member's bytes were held,
    // and nothing came out of them; a sealed one's came out before the
    // restart, through the cipher its chain was kept from. A cipher block
    // comes out whole, so an enciphered span is un-said from the start of
    // the block it starts in.
    pub(super) fn unsay(&mut self, volume: VolumeIndex, range: ByteRange) {
        for (id, part, stream) in self.linked(volume, range) {
            let Some(entry) = self.entries.get_mut(id) else {
                continue;
            };
            // What the checksums folded of these bytes proves nothing of the
            // copy that comes again.
            if let Some(piece) = entry.piece_of(stream)
                && let Some(runs) = entry.parts.get_mut(&piece)
            {
                runs.doubt(part.shift(-(entry.pieces[piece].data.start as i64)));
            }
            let plain_len = entry.plain_len();
            let said = Arc::make_mut(&mut entry.out);
            let stream = match &entry.lock {
                Lock::Locked => continue,
                Lock::Plain => stream,
                Lock::Open(_) | Lock::Sealed(_) => {
                    let stream = ByteRange::new(block_floor(stream.start), stream.end);
                    if let Some(length) = plain_len {
                        said.padding.forget(length, stream);
                    }
                    stream
                }
            };
            let payload = plain_len.map_or(stream, |length| {
                ByteRange::new(stream.start.min(length), stream.end.min(length))
            });
            if !payload.is_empty() {
                said.coverage.erase(payload);
                said.closed = false;
                entry.plain_runs.doubt(payload);
            }
        }
    }

    // A part's packed bytes, against the part's own CRC. A stored file's
    // packed bytes are its member's, so what a repair cut from the runs is
    // read back from the member; another's are not, and stay unfed.
    #[allow(clippy::too_many_arguments)]
    fn check_part(
        &mut self,
        id: usize,
        piece: usize,
        (part, stream): (ByteRange, ByteRange),
        bytes: &Evidenced,
        provenance: Provenance,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
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
        let local = part.shift(-(data.start as i64));
        let gaps = runs.feed(local, crc(bytes), provenance);
        if matches!(entry.lock, Lock::Plain) {
            let at = stream.start - local.start;
            let member = (MemberId(id as u32), at, &entry.out.coverage);
            refeed(runs, gaps, member, read);
        }
        if let (SliceVerdict::Damaged, crc) = gate(runs, data.len(), expected, uses_mac, hash_key) {
            self.say_damaged(volume, data, crc, out);
        }
    }

    fn record(
        &mut self,
        id: usize,
        (offset, mut bytes, provenance): (u64, Evidenced, Provenance),
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        let member = MemberId(id as u32);
        let entry = &mut self.entries[id];
        let range = ByteRange::at(offset, bytes.len() as u64);
        // What no level above proved, deciphered bytes among them, is
        // hashed here once and handed down with the bytes.
        bytes.fill(Algorithm::Crc32);
        let digest = crc(&bytes);
        out.push(Emit::Bytes {
            member,
            offset,
            bytes,
            provenance,
        });
        let gaps = entry.plain_runs.feed(range, digest, provenance);
        let said = (member, 0, &entry.out.coverage);
        refeed(&mut entry.plain_runs, gaps, said, read);
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
        let (SliceVerdict::Damaged, composed) = gate(
            &entry.plain_runs,
            length,
            expected,
            entry.uses_mac,
            hash_key,
        ) else {
            return;
        };
        // The gate composes verified runs only, so a failure here is on
        // verified bytes: under a key on trial it condemns the key, not the
        // bytes. Under a proved key, a checksum the store judges damages the
        // file where no part's own check vouches for its bytes, unless a
        // part failing its own accounts for the failure, as damage already
        // marked does.
        if let Some(candidate) = entry.trial {
            self.wrong_key = Some(candidate);
        } else if entry.judges_its_checksum() {
            let hash_key = entry.hash_key.as_deref();
            let (mut unvouched, mut accounted) = (Vec::new(), false);
            for (piece, found) in entry.pieces.iter().enumerate() {
                let verdict = match (found.crc, entry.parts.get(&piece)) {
                    (Some(crc), Some(runs)) => {
                        gate(runs, found.data.len(), crc, found.uses_mac, hash_key).0
                    }
                    _ => SliceVerdict::Unknown,
                };
                match verdict {
                    SliceVerdict::Intact => {}
                    SliceVerdict::Damaged => accounted = true,
                    _ => unvouched.push((found.volume, found.data)),
                }
            }
            for (volume, data) in unvouched.into_iter().filter(|_| !accounted) {
                self.say_damaged(volume, data, composed, out);
            }
        }
    }
}

// What a rewrite cut from a member's runs and did not cover came out of the
// member already: it is read back from the member's own bytes, each piece
// as it came, and fed again, so the runs tile again and nothing is fetched
// twice. `at` is where the runs' offset zero lies in the member. It is read
// a bounded piece at a time, each piece its own run.
fn refeed(
    runs: &mut CrcRuns,
    gaps: Vec<ByteRange>,
    (member, at, coverage): (MemberId, u64, &Coverage),
    read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
) {
    const READ_BACK: u64 = 1 << 20;
    let mut buffer = Vec::new();
    for gap in gaps {
        let gap = gap.shift(at as i64);
        for run in coverage.overlapping(gap) {
            let Some(range) = run.range.intersect(gap) else {
                continue;
            };
            let mut start = range.start;
            while start < range.end {
                let range = ByteRange::new(start, range.end.min(start + READ_BACK));
                start = range.end;
                buffer.clear();
                if read(&Placement::Member { member, range }, &mut buffer) {
                    let local = range.shift(-(at as i64));
                    let cut = runs.feed(local, crc32(&buffer), run.provenance);
                    debug_assert!(cut.is_empty(), "a gap holds no run to cut");
                }
            }
        }
    }
}

// The CRC32 the runs fold, from the evidence the bytes came with, hashing
// only what no level above proved.
fn crc(bytes: &Evidenced) -> u32 {
    bytes.digest(Algorithm::Crc32).value as u32
}
