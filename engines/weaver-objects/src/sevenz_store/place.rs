// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where bytes are: the inverse map from a volume range to members, the
// envelope and held bytes; the forward map from a member to its volumes;
// and the posted bytes rebuilt, re-enciphering an AES folder's plaintext.
//
// Before the map, the signature, the end header, what follows it and the
// header ranges the parse asked for are the envelope, and everything else is
// held: only the map can say whose it is. A locked AES folder stays held
// until a password opens it.

use std::sync::Arc;

use weaver_graph::{ByteRange, Extent, MemberId, Placement, Placements, Posting, VolumeIndex};

use super::header::SIGNATURE;
use super::map::Map;
use super::record::FolderPlaced;
use super::{ENVELOPE, Lock, Out, SevenZipStore};
use crate::crypt::{BLOCK, Cbc, Padding};

// A piece of the container: the envelope and held bytes in container
// coordinates, a member's at its own offsets.
#[derive(Clone, Copy, Debug)]
enum Piece {
    Envelope(ByteRange),
    Held(ByteRange),
    Member(MemberId, ByteRange),
}

fn push(out: &mut Vec<Placement>, placement: Placement) {
    if placement.is_empty() {
        return;
    }
    let merged = match (out.last_mut(), &placement) {
        (Some(Placement::Held { range: last }), Placement::Held { range })
            if last.end == range.start =>
        {
            last.end = range.end;
            true
        }
        (
            Some(Placement::Envelope {
                envelope: last_envelope,
                range: last,
            }),
            Placement::Envelope { envelope, range },
        ) if last_envelope == envelope && last.end == range.start => {
            last.end = range.end;
            true
        }
        (
            Some(Placement::Member {
                member: last_member,
                range: last,
            }),
            Placement::Member { member, range },
        ) if last_member == member && last.end == range.start => {
            last.end = range.end;
            true
        }
        _ => false,
    };
    if !merged {
        out.push(placement);
    }
}

impl SevenZipStore {
    pub(super) fn place(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        // Bytes past what the geometry gives a volume belong to nothing.
        let overrun = Placement::Envelope {
            envelope: ENVELOPE + 1 + volume.0,
            range,
        };
        let Some(container) = self.container(volume, range) else {
            match self.geometry {
                Some(_) => push(out, overrun),
                None => push(out, Placement::Held { range }),
            }
            return;
        };
        let base = self.base(volume);
        let mut pieces = Vec::new();
        self.classify(container, &mut pieces);
        for piece in pieces {
            let placement = match piece {
                Piece::Envelope(range) => Placement::Envelope {
                    envelope: ENVELOPE,
                    range,
                },
                Piece::Held(range) => Placement::Held {
                    range: range.shift(-(base as i64)),
                },
                Piece::Member(member, range) => Placement::Member { member, range },
            };
            push(out, placement);
        }
        let reached = container.end - base;
        if reached < range.end {
            push(
                out,
                Placement::Envelope {
                    envelope: ENVELOPE + 1 + volume.0,
                    range: ByteRange::new(reached.max(range.start), range.end),
                },
            );
        }
    }

    fn classify(&self, range: ByteRange, out: &mut Vec<Piece>) {
        let mut cursor = range.start;
        if cursor < SIGNATURE.end {
            let end = SIGNATURE.end.min(range.end);
            out.push(Piece::Envelope(ByteRange::new(cursor, end)));
            cursor = end;
        }
        if cursor >= range.end {
            return;
        }
        let rest = ByteRange::new(cursor, range.end);
        match &self.map {
            Some(map) => self.classify_mapped(map, rest, out),
            None => self.classify_unmapped(rest, out),
        }
    }

    fn classify_unmapped(&self, range: ByteRange, out: &mut Vec<Piece>) {
        let Some(start) = self.start else {
            out.push(Piece::Held(range));
            return;
        };
        let tail = start.end_header.start.max(range.start).min(range.end);
        self.held_or_kept(ByteRange::new(range.start, tail), out);
        if tail < range.end {
            out.push(Piece::Envelope(ByteRange::new(tail, range.end)));
        }
    }

    // Bytes no map places yet: the object's own where it keeps them, held
    // everywhere else.
    fn held_or_kept(&self, range: ByteRange, out: &mut Vec<Piece>) {
        let mut cursor = range.start;
        for wanted in self.kept.ranges_in(range) {
            if wanted.start > cursor {
                out.push(Piece::Held(ByteRange::new(cursor, wanted.start)));
            }
            out.push(Piece::Envelope(wanted));
            cursor = wanted.end;
        }
        if cursor < range.end {
            out.push(Piece::Held(ByteRange::new(cursor, range.end)));
        }
    }

    fn classify_mapped(&self, map: &Map, range: ByteRange, out: &mut Vec<Piece>) {
        let mut cursor = range.start;
        for (index, folder) in map.folders_in(range) {
            if folder.pack.start > cursor {
                out.push(Piece::Envelope(ByteRange::new(cursor, folder.pack.start)));
                cursor = folder.pack.start;
            }
            let part = ByteRange::new(cursor, folder.pack.end.min(range.end));
            let local = part.shift(-(folder.pack.start as i64));
            match &self.folders[index] {
                // The proof file is kept to try passwords against.
                Lock::Locked | Lock::Sealed(..) => self.held_or_kept(part, out),
                Lock::Plain => classify_plain(map, index, local, folder.pack.start, out),
                Lock::Open(..) => {
                    let payload = ByteRange::new(
                        local.start.min(folder.plain_len),
                        local.end.min(folder.plain_len),
                    );
                    classify_plain(map, index, payload, folder.pack.start, out);
                    if local.end > folder.plain_len {
                        let padding = ByteRange::new(local.start.max(folder.plain_len), local.end);
                        out.push(Piece::Envelope(padding.shift(folder.pack.start as i64)));
                    }
                }
            }
            cursor = part.end;
        }
        if cursor < range.end {
            out.push(Piece::Envelope(ByteRange::new(cursor, range.end)));
        }
    }

    pub(super) fn member_extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) {
        if let Some(map) = &self.map {
            self.extent_in(map, member, range, out);
        }
    }

    // On a map in hand, for a caller that has taken it out to route by it.
    pub(super) fn extent_in(
        &self,
        map: &Map,
        member: MemberId,
        range: ByteRange,
        out: &mut Vec<Extent>,
    ) {
        let Some((index, container)) = map.container(member, range) else {
            return;
        };
        let folder = &map.folders[index];
        // A cipher block reads with the one before it as its seed.
        let container = if folder.aes.is_some() {
            let local = container.shift(-(folder.pack.start as i64));
            let first = local.start - local.start % BLOCK;
            let start = first.saturating_sub(BLOCK);
            let end = (local.end.div_ceil(BLOCK) * BLOCK).min(folder.pack.len());
            ByteRange::new(folder.pack.start + start, folder.pack.start + end)
        } else {
            container
        };
        let mut pieces = Vec::new();
        self.to_volumes(container, &mut pieces);
        out.extend(
            pieces
                .into_iter()
                .map(|(volume, range)| Extent { volume, range }),
        );
    }

    // The posted bytes of these ranges of the store's volumes, as it stands
    // now: their inverse map, and every open folder's cipher with the map
    // and what its members said, shared, so the cipher can be put back over
    // the plaintext away from the store.
    pub(super) fn posted_over(&self, ranges: &[(VolumeIndex, ByteRange)]) -> Posted {
        let placed = Placements::take(ranges, |volume, range, out| self.place(volume, range, out));
        let mut sealed = Vec::new();
        for &(volume, range) in ranges {
            let (Some(container), Some(map)) = (self.container(volume, range), &self.map) else {
                continue;
            };
            for (index, folder) in map.folders_in(container) {
                let (Lock::Open(cbc, padding), Some(part)) =
                    (&self.folders[index], container.intersect(folder.pack))
                else {
                    continue;
                };
                sealed.push(Sealed {
                    volume,
                    part,
                    base: self.base(volume),
                    folder: index,
                    pack: folder.pack.start,
                    cbc: Arc::clone(cbc),
                    padding: Arc::clone(padding),
                });
            }
        }
        Posted {
            placed,
            map: self.map.clone(),
            members: Arc::clone(&self.members),
            sealed,
        }
    }
}

// An open folder's part of a volume, in container coordinates, with where
// the volume and the folder start there, and the folder's cipher and its
// padding as they stood.
struct Sealed {
    volume: VolumeIndex,
    part: ByteRange,
    base: u64,
    folder: usize,
    pack: u64,
    cbc: Arc<Cbc>,
    padding: Arc<Padding>,
}

// The store's posted bytes, apart from the store: a plain stretch is read
// through its placements, an open folder's part is sealed again from its
// members' plaintext.
pub(super) struct Posted {
    placed: Placements,
    map: Option<Arc<Map>>,
    members: Arc<Vec<Out>>,
    sealed: Vec<Sealed>,
}

impl Posting for Posted {
    fn posted(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        let mut cursor = range.start;
        for sealed in self.sealed.iter().filter(|sealed| sealed.volume == volume) {
            let Some(part) = sealed.part.intersect(range.shift(sealed.base as i64)) else {
                continue;
            };
            let Some(map) = &self.map else {
                return false;
            };
            let start = part.start - sealed.base;
            if start > cursor
                && !self
                    .placed
                    .posted(volume, ByteRange::new(cursor, start), read, into)
            {
                return false;
            }
            let folder = sealed.folder;
            let mut plain = |wanted: ByteRange, buffer: &mut Vec<u8>| {
                read_folder(map, folder, wanted, read, buffer)
            };
            let placed = FolderPlaced {
                map,
                folder,
                members: &self.members,
            };
            let local = part.shift(-(sealed.pack as i64));
            if !sealed
                .cbc
                .seal(&placed, &sealed.padding, local, &mut plain, into)
            {
                return false;
            }
            cursor = part.end - sealed.base;
        }
        cursor >= range.end
            || self
                .placed
                .posted(volume, ByteRange::new(cursor, range.end), read, into)
    }
}

// The plaintext of `wanted`, a stretch of an AES folder, read back member
// by member. `false` at a byte no member holds or `read` cannot give.
pub(super) fn read_folder(
    map: &Map,
    folder: usize,
    wanted: ByteRange,
    read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
    buffer: &mut Vec<u8>,
) -> bool {
    let mut at = wanted.start;
    for (member, entry) in map.entries_in(folder, wanted) {
        let Some(piece) = wanted.intersect(entry.span()) else {
            continue;
        };
        if piece.start != at {
            return false;
        }
        let placement = Placement::Member {
            member,
            range: piece.shift(-(entry.offset as i64)),
        };
        if !read(&placement, buffer) {
            return false;
        }
        at = piece.end;
    }
    at == wanted.end
}

// A stored stretch of a folder's plaintext, to its members.
fn classify_plain(map: &Map, folder: usize, local: ByteRange, pack: u64, out: &mut Vec<Piece>) {
    let mut cursor = local.start;
    for (member, entry) in map.entries_in(folder, local) {
        let Some(part) = local.intersect(entry.span()) else {
            continue;
        };
        if part.start > cursor {
            out.push(Piece::Envelope(
                ByteRange::new(cursor, part.start).shift(pack as i64),
            ));
        }
        out.push(Piece::Member(member, part.shift(-(entry.offset as i64))));
        cursor = part.end;
    }
    if cursor < local.end {
        out.push(Piece::Envelope(
            ByteRange::new(cursor, local.end).shift(pack as i64),
        ));
    }
}
