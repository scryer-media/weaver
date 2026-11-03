// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where bytes are: the inverse map from a range of the file to members, the
// envelope and held bytes; the forward map from a member to its ranges of
// the file; and the posted bytes rebuilt, re-enciphering an AES folder's
// plaintext.
//
// Before the map, the signature, the end header, what follows it and the
// header ranges the parse asked for are the envelope, and everything else is
// held: only the map can say whose it is. A locked AES folder stays held
// until a password opens it.

use std::sync::Arc;

use weaver_graph::{
    ByteRange, Extent, Located, MemberId, Placement, Placements, Posting, VolumeIndex,
};

use super::header::SIGNATURE;
use super::map::Map;
use super::record::FolderPlaced;
use super::{ENVELOPE, FILE, Lock, Out, SevenZipStore};
use crate::crypt::{BLOCK, Cbc, Padding, block_floor};

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
    pub(super) fn place(&self, range: ByteRange, out: &mut Vec<Placement>) {
        let mut pieces = Vec::new();
        self.classify(range, &mut pieces);
        for piece in pieces {
            let placement = match piece {
                Piece::Envelope(range) => Placement::Envelope {
                    envelope: ENVELOPE,
                    range,
                },
                Piece::Held(range) => Placement::Held { range },
                Piece::Member(member, range) => Placement::Member { member, range },
            };
            push(out, placement);
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

    // The bytes posted at `range` of the file come again: each member
    // they reached forgets what was said of them and is open until it is
    // said again, and a cipher that was ever open forgets the padding they
    // came out to. A locked folder's bytes were held, and nothing came out
    // of them. A cipher block comes out whole, so an enciphered span is
    // un-said from the start of the block it starts in.
    pub(super) fn unsay(&mut self, range: ByteRange) {
        let Some(map) = &self.map else {
            return;
        };
        let mut pieces = Vec::new();
        for (index, folder) in map.folders_in(range) {
            let Some(part) = range.intersect(folder.pack) else {
                continue;
            };
            let local = part.shift(-(folder.pack.start as i64));
            let lock = &mut self.folders[index];
            // A folder a condemned key opened said nothing true of any of
            // its bytes: all of it is said again, under the next key.
            let local = match self.condemned.remove(&index) {
                true => {
                    self.relock.insert(index);
                    self.stale = true;
                    ByteRange::new(0, folder.pack.len())
                }
                false => local,
            };
            let (local, said) = match lock {
                Lock::Locked => continue,
                Lock::Plain => (local, local),
                Lock::Open(..) | Lock::Sealed(..) => {
                    let local = ByteRange::new(block_floor(local.start), local.end);
                    let said = ByteRange::new(
                        local.start.min(folder.plain_len),
                        local.end.min(folder.plain_len),
                    );
                    (local, said)
                }
            };
            classify_plain(map, index, said, folder.pack.start, &mut pieces);
            if let Some(padding) = lock.padding_mut() {
                padding.forget(folder.plain_len, local);
            }
        }
        let members = Arc::make_mut(&mut self.members);
        for piece in pieces {
            if let Piece::Member(member, range) = piece
                && !range.is_empty()
                && let Some(said) = members.get_mut(member.0 as usize)
            {
                said.coverage.erase(range);
                said.closed = false;
                said.damaged = false;
            }
        }
    }

    // Every member is a run of a stored folder's raw bytes, so every range
    // of one lies somewhere once the end header is read: until then it is
    // not located yet.
    pub(super) fn member_extent(
        &self,
        member: MemberId,
        range: ByteRange,
        out: &mut Vec<Extent>,
    ) -> Located {
        match &self.map {
            Some(map) => self.extent_in(map, member, range, out),
            None => Located::NotYet,
        }
    }

    // On a map in hand, for a caller that has taken it out to route by it.
    // A range the map gives no folder bytes is never addressed.
    pub(super) fn extent_in(
        &self,
        map: &Map,
        member: MemberId,
        range: ByteRange,
        out: &mut Vec<Extent>,
    ) -> Located {
        let Some((index, container)) = map.container(member, range) else {
            return Located::Never;
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
        out.push(Extent {
            volume: FILE,
            range: container,
        });
        Located::Whole
    }

    // The posted bytes of these ranges of the file, as it stands now: their
    // inverse map, and every open folder's cipher with the map and what its
    // members said, shared, so the cipher can be put back over the
    // plaintext away from the store.
    pub(super) fn posted_over(&self, ranges: &[(VolumeIndex, ByteRange)]) -> Posted {
        let placed = Placements::take(ranges, |_, range, out| self.place(range, out));
        let mut sealed = Vec::new();
        for &(_, range) in ranges {
            let Some(map) = &self.map else {
                continue;
            };
            for (index, folder) in map.folders_in(range) {
                let (Lock::Open(cbc, padding), Some(part)) =
                    (&self.folders[index], range.intersect(folder.pack))
                else {
                    continue;
                };
                sealed.push(Sealed {
                    part,
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

// An open folder's part of the file, with where the folder starts, and the
// folder's cipher and its padding as they stood.
struct Sealed {
    part: ByteRange,
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
        for sealed in &self.sealed {
            let Some(part) = sealed.part.intersect(range) else {
                continue;
            };
            let Some(map) = &self.map else {
                return false;
            };
            let start = part.start;
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
            cursor = part.end;
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
