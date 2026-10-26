// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where bytes are: the inverse map from a volume range to members, the
// envelope and held bytes, and the forward map from a member to its
// volumes. Before the map, and past where the walk has read, the bytes the
// object reads itself are the envelope and everything else is held: only
// the map can say whose it is. A volume with no place in the container yet
// is held whole.

use weaver_graph::{ByteRange, Extent, Located, MemberId, Placement, VolumeIndex};

use super::map::Piece;
use super::{ENVELOPE, Zip};

fn push(out: &mut Vec<Placement>, placement: Placement) {
    if placement.is_empty() {
        return;
    }
    let merged = match (out.last_mut(), &placement) {
        (Some(Placement::Held { range: last }), Placement::Held { range })
        | (Some(Placement::Envelope { range: last, .. }), Placement::Envelope { range, .. })
            if last.end == range.start =>
        {
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

impl Zip {
    pub(super) fn place(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let Some(global) = self.global(volume, range) else {
            return push(out, Placement::Held { range });
        };
        let base = global.start - range.start;
        let mut pieces = Vec::new();
        match &self.map {
            Some(map) => map.classify(global, &mut pieces),
            None => pieces.push(Piece::Unplaced(global)),
        }
        for piece in pieces {
            match piece {
                Piece::Envelope(range) => push(
                    out,
                    Placement::Envelope {
                        envelope: ENVELOPE,
                        range,
                    },
                ),
                Piece::Member(member, range) => push(out, Placement::Member { member, range }),
                Piece::Unplaced(range) => self.held_or_kept(range, base, out),
            }
        }
    }

    // Bytes no map places yet: the object's own where it keeps them, held,
    // in the volume's coordinates, everywhere else.
    fn held_or_kept(&self, range: ByteRange, base: u64, out: &mut Vec<Placement>) {
        let held = |range: ByteRange| Placement::Held {
            range: range.shift(-(base as i64)),
        };
        let mut cursor = range.start;
        for wanted in self.kept.ranges_in(range) {
            if wanted.start > cursor {
                push(out, held(ByteRange::new(cursor, wanted.start)));
            }
            push(
                out,
                Placement::Envelope {
                    envelope: ENVELOPE,
                    range: wanted,
                },
            );
            cursor = wanted.end;
        }
        if cursor < range.end {
            push(out, held(ByteRange::new(cursor, range.end)));
        }
    }

    // Every member is a stored entry's raw bytes, so every range of one lies
    // somewhere: not located yet until the map is read and the entry's local
    // header says where its data starts and the lengths of the volumes it
    // reaches are known, and never for a range the map gives it no bytes at.
    pub(super) fn member_extent(
        &self,
        member: MemberId,
        range: ByteRange,
        out: &mut Vec<Extent>,
    ) -> Located {
        let Some(map) = &self.map else {
            return Located::NotYet;
        };
        let Some(entry) = map.entry(member).filter(|entry| entry.streams) else {
            return Located::Never;
        };
        let Some(span) = entry.span() else {
            return Located::NotYet;
        };
        let Some(part) = range.shift(span.start as i64).intersect(span) else {
            return Located::Never;
        };
        let mut pieces = Vec::new();
        self.to_volumes(part, &mut pieces);
        let placed: u64 = pieces.iter().map(|(_, range)| range.len()).sum();
        out.extend(
            pieces
                .into_iter()
                .map(|(volume, range)| Extent { volume, range }),
        );
        match placed == part.len() {
            true => Located::Whole,
            false => Located::NotYet,
        }
    }
}
