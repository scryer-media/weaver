// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The map: every entry by where its local header starts in the container,
// and what each stretch of the container is. An entry's stretch runs from
// its header to the next one's: header, data, and whatever follows the
// data. A stored entry's data is its member's bytes once its header says
// where the data starts; everything else in the stretch is the envelope.
// The directory and the records after it are the envelope too.

use weaver_graph::{ByteRange, Coverage, MemberId};

use super::records::Header;

// What has been said for one member.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(super) struct Out {
    pub(super) coverage: Coverage,
    pub(super) closed: bool,
    pub(super) damaged: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(super) struct Entry {
    // Where the local header starts in the container.
    pub(super) header: u64,
    // `None` for an entry that is no member: a link or a device.
    pub(super) member: Option<MemberId>,
    // The name its member is said under, and the checksum it is said to
    // have.
    pub(super) name: String,
    pub(super) checksum: Option<u32>,
    pub(super) length: u64,
    pub(super) crc: u32,
    // Where the data starts in the container, once the local header says,
    // for an entry whose bytes stream into its member.
    pub(super) data: Option<u64>,
    pub(super) streams: bool,
    // What the record read first says of the entry, until the other says
    // the same: the directory's, until the local header is read, or the
    // local header's, until the walk's directory is.
    pub(super) expected: Option<Header>,
}

impl Entry {
    // The member's bytes in the container, once the header has placed them.
    pub(super) fn span(&self) -> Option<ByteRange> {
        self.data
            .filter(|_| self.streams)
            .map(|data| ByteRange::at(data, self.length))
    }
}

// A piece of the container: the envelope and held bytes in container
// coordinates, a member's at its own offsets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Piece {
    Envelope(ByteRange),
    // No map places these yet; the object keeps them only where it reads
    // them, which `Envelope` says.
    Unplaced(ByteRange),
    Member(MemberId, ByteRange),
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(super) struct Map {
    // By header.
    pub(super) entries: Vec<Entry>,
    // The entry each member is, by member id.
    pub(super) members: Vec<usize>,
    // Where the entries stop and the directory starts, once known.
    pub(super) end: Option<u64>,
    // Where the walk has read to: everything before it is placed.
    pub(super) frontier: u64,
    // The entries were held against the directory: built from it, or
    // walked and then found in it.
    pub(super) checked: bool,
}

impl Map {
    pub(super) fn entry(&self, member: MemberId) -> Option<&Entry> {
        self.entries.get(*self.members.get(member.0 as usize)?)
    }

    // Where an entry's stretch ends: the next entry's header, or where the
    // entries stop.
    pub(super) fn stretch_end(&self, index: usize) -> u64 {
        self.entries
            .get(index + 1)
            .map_or_else(|| self.end.unwrap_or(self.frontier), |next| next.header)
    }

    // Where entries start: what comes before the first is the envelope.
    fn start(&self) -> u64 {
        self.entries
            .first()
            .map_or_else(|| self.end.unwrap_or(self.frontier), |first| first.header)
    }

    // The entries whose stretch may meet a range, from the first that does.
    pub(super) fn from(&self, offset: u64) -> impl Iterator<Item = (usize, &Entry)> {
        let first = self
            .entries
            .partition_point(|entry| entry.header <= offset)
            .saturating_sub(1);
        self.entries.iter().enumerate().skip(first)
    }

    // What a container range is, piece by piece, in order.
    pub(super) fn classify(&self, range: ByteRange, out: &mut Vec<Piece>) {
        let mut cursor = range.start;
        let mut push = |piece: Piece, cursor: &mut u64| {
            let (Piece::Envelope(part) | Piece::Unplaced(part)) = piece else {
                out.push(piece);
                return;
            };
            if let Some(part) = part.intersect(range) {
                *cursor = part.end;
                out.push(match piece {
                    Piece::Envelope(_) => Piece::Envelope(part),
                    _ => Piece::Unplaced(part),
                });
            }
        };
        push(Piece::Envelope(between(0, self.start())), &mut cursor);
        for (index, entry) in self.from(range.start) {
            if entry.header >= range.end {
                break;
            }
            let stretch = ByteRange::new(entry.header, self.stretch_end(index));
            match (entry.streams, entry.span()) {
                (true, None) => push(Piece::Unplaced(stretch), &mut cursor),
                (true, Some(span)) => {
                    push(
                        Piece::Envelope(between(stretch.start, span.start)),
                        &mut cursor,
                    );
                    if let (Some(member), Some(part)) = (entry.member, span.intersect(range)) {
                        cursor = part.end;
                        push(
                            Piece::Member(member, part.shift(-(span.start as i64))),
                            &mut cursor,
                        );
                    }
                    push(Piece::Envelope(between(span.end, stretch.end)), &mut cursor);
                }
                (false, _) => push(Piece::Envelope(stretch), &mut cursor),
            }
        }
        let after = match self.end {
            Some(end) => Piece::Envelope(between(end.max(cursor), range.end)),
            None => Piece::Unplaced(between(self.frontier.max(cursor), range.end)),
        };
        push(after, &mut cursor);
    }
}

// A range that is empty when its ends cross, as they do where a stretch
// has nothing after its data.
fn between(start: u64, end: u64) -> ByteRange {
    ByteRange::new(start.min(end), end)
}
