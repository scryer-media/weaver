// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file posted in numbered pieces, `name.001`, `name.002`, …: a store whose
// one member is the file the pieces were cut from. Each piece's bytes are
// placed in that member where the piece starts, which is the sum of the
// lengths of the pieces before it, so a clean set is written once and never
// joined. A piece's length is the one its articles declare, told ahead of
// its bytes; bytes whose place is not known yet, behind a piece that has not
// declared, or past the length their own piece declared, are held, and
// placed once it is. A piece that closes at another length than it declared
// moves every piece after it: their bytes are asked for again, read back
// from where they were placed, and placed where they go now.
//
// The set counts from `.001`, and any piece founds it. A set missing a
// number, or its first piece, is no file of its own: its pieces go back as
// they came.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Delivery, Durable, Emit, Extent, Facts, Kind, Located, MemberId, Object, Placement,
    Pull, Seek, Standing, VolumeIndex, Weight, Why,
};

use crate::split;

// The one member: the file the pieces were cut from.
const MEMBER: MemberId = MemberId(0);

// The number a set's first piece carries.
const FIRST: u32 = 1;

#[derive(Default, Serialize, Deserialize)]
pub struct SplitStore {
    // The member's name: the founding piece's name with its number taken
    // off, as it was spelled, and the first piece's once that is bound.
    name: String,
    // The set's first piece, which every piece's place is counted from.
    first: Option<split::Piece>,
    // The pieces bound, by place.
    pieces: BTreeMap<u32, Piece>,
    // Pieces whose bytes come to me again, as the geometry changed under
    // them: until a delivery reaches `until`, the member is not whole. Kept:
    // a restart between the ask and the last of them still reads back
    // where they sat, and closes the member only once they came.
    awaiting: BTreeMap<u32, Awaited>,
    // The length the member was last announced with, once it was.
    #[serde(skip)]
    said: Option<Option<u64>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Piece {
    // The piece's length: what its articles declare, and what it reached
    // once it closed. A piece is never empty, so none is said as none.
    declared: Option<u64>,
    // How far its bytes and its losses reached before it closed.
    reached: u64,
    // How far the bytes it was delivered reached.
    delivered: u64,
    // How far the bytes it placed in the member reach, from its start.
    placed: u64,
    closed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Awaited {
    // Where its placed bytes still sit in the member, until they come again.
    from: Option<u64>,
    // The end of the last of its bytes that come again.
    until: u64,
}

impl SplitStore {
    pub fn new() -> Self {
        Self::default()
    }

    // The place a piece's name numbers in my set, counted from its first
    // piece: `None` for a piece of another set.
    fn place(&self, name: &str) -> Option<VolumeIndex> {
        let piece = split::parse(name)?;
        let first = self.first.clone().unwrap_or_else(|| piece.numbered(FIRST));
        piece.index_from(&first).map(VolumeIndex)
    }

    // Where a piece starts in the member: the sum of every piece before it,
    // once each of them is bound and declared. A set missing a number has
    // no places at all.
    fn offset(&self, index: u32) -> Option<u64> {
        if !self.contiguous() {
            return None;
        }
        (0..index).try_fold(0u64, |at, before| {
            Some(at + self.pieces.get(&before)?.declared?)
        })
    }

    // How far a piece's bytes can be placed: to its declared length, or
    // anywhere once its start is known and it declared none. Nothing while
    // its start is unknown.
    fn placeable(&self, index: u32) -> u64 {
        match (self.offset(index), self.pieces.get(&index)) {
            (Some(_), Some(piece)) => piece.declared.unwrap_or(u64::MAX),
            _ => 0,
        }
    }

    // The member's length: every piece from the first, each declared.
    fn length(&self) -> Option<u64> {
        let count = u32::try_from(self.pieces.len()).ok()?;
        (self.pieces.keys().next_back() == count.checked_sub(1).as_ref())
            .then(|| self.offset(count))
            .flatten()
    }

    // Whether the places held run from the first with no number missing.
    fn contiguous(&self) -> bool {
        self.pieces
            .keys()
            .copied()
            .zip(0u32..)
            .all(|(index, expected)| index == expected)
    }

    // The member, announced or announced again when its length changed. A
    // set missing a number is no file, so nothing reads it as one.
    fn say_member(&mut self, out: &mut Vec<Emit>) {
        let length = self.length();
        if !self.contiguous() || self.said == Some(length) {
            return;
        }
        self.said = Some(length);
        out.push(Emit::Member {
            member: MEMBER,
            name: self.name.clone(),
            length,
        });
    }

    // The member is whole once every piece from the first closed, each at
    // its length, and nothing is still to come again.
    fn close_member(&mut self, out: &mut Vec<Emit>) {
        if self.awaiting.is_empty()
            && !self.pieces.is_empty()
            && self.contiguous()
            && self.pieces.values().all(|piece| piece.closed)
            && self.length().is_some()
        {
            self.say_member(out);
            out.push(Emit::MemberClosed(MEMBER));
        }
    }

    // The one transition of the geometry: a piece's length changes. Every
    // piece whose placed bytes now start elsewhere asks for them again,
    // highest first, read back from where they sit; every piece whose held
    // bytes can now be placed will have them handed again by the owner. The
    // member is whole only once all of them came.
    fn resize(&mut self, index: u32, length: u64, out: &mut Vec<Emit>) {
        let Some(piece) = self.pieces.get(&index) else {
            return;
        };
        if length == 0 || piece.declared == Some(length) {
            return;
        }
        let before: Vec<(u32, Option<u64>, u64)> = self
            .pieces
            .keys()
            .map(|&at| (at, self.offset(at), self.placeable(at)))
            .collect();
        if let Some(piece) = self.pieces.get_mut(&index) {
            piece.declared = Some(length);
        }
        for (at, start, placeable) in before.into_iter().rev() {
            let now = self.offset(at);
            let placeable_now = self.placeable(at);
            let Some(piece) = self.pieces.get_mut(&at) else {
                continue;
            };
            let moved = piece.placed > 0 && start != now;
            let released = piece.delivered > placeable && placeable_now > placeable;
            if !(moved || released) {
                continue;
            }
            let until = piece.delivered.min(placeable_now);
            if moved {
                out.push(Emit::Again {
                    volume: VolumeIndex(at),
                    range: ByteRange::new(0, piece.placed),
                });
                piece.placed = 0;
            }
            self.awaiting.insert(
                at,
                Awaited {
                    from: start.filter(|_| moved),
                    until,
                },
            );
        }
        self.say_member(out);
    }
}

impl Durable for SplitStore {
    // 4: a store of the pieces' places, where the stream that joined them
    // kept 3.
    // 5: the pieces whose bytes come again, and where they sat.
    const VERSION: u16 = 5;
}

impl Object for SplitStore {
    fn kind(&self) -> Kind {
        Kind::Split
    }

    // Bytes of a piece go where the piece starts in the member, as far as
    // its declared length; the rest is held until its place is known.
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        let index = delivery.volume.0;
        let range = delivery.range();
        let Some(piece) = self.pieces.get_mut(&index) else {
            return;
        };
        piece.delivered = piece.delivered.max(range.end);
        if !piece.closed {
            piece.reached = piece.reached.max(range.end);
        }
        self.say_member(out);
        let placeable = self.placeable(index);
        let start = self.offset(index);
        let end = range.end.min(placeable);
        match start {
            Some(start) if end > range.start => {
                let len = (end - range.start) as usize;
                out.push(Emit::Bytes {
                    member: MEMBER,
                    offset: start + range.start,
                    bytes: delivery.bytes.slice(..len),
                    provenance: delivery.provenance,
                });
                if let Some(piece) = self.pieces.get_mut(&index) {
                    piece.placed = piece.placed.max(end);
                }
            }
            _ => out.push(Emit::Pending),
        }
        // A piece's bytes coming again stand where they go now once the
        // first of them came; the member waits for the last.
        if let Some(awaited) = self.awaiting.get_mut(&index) {
            awaited.from = None;
            if range.end >= awaited.until {
                self.awaiting.remove(&index);
                self.close_member(out);
            }
        }
    }

    // A piece that closes at another length than it declared is that
    // length: what it reached, its bytes and its losses alike.
    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        let Some(piece) = self.pieces.get_mut(&volume.0) else {
            return;
        };
        if !piece.closed {
            piece.closed = true;
            let reached = piece.reached;
            self.resize(volume.0, reached, out);
        }
        self.close_member(out);
    }

    // The member is marked through the piece's place, as any store's is.
    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, _out: &mut Vec<Emit>) {
        if let Some(piece) = self.pieces.get_mut(&volume.0)
            && !piece.closed
        {
            piece.reached = piece.reached.max(range.end);
        }
    }

    // A set missing a number, or its first piece, is no file of its own:
    // what was placed of it is forgotten and its pieces go back. A member
    // never placed whole goes back as any store's does.
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        if !self.contiguous() {
            out.push(Emit::Retract(MEMBER));
            out.push(Emit::Refuse(Why::Unrepairable));
            return;
        }
        out.extend(crate::quiet::unplaced(standing).map(Emit::Refuse));
    }

    // One article of each piece that has not declared its length: any
    // article declares it, and every place after it waits on it.
    fn wants(&self, out: &mut Vec<Seek>) {
        for (index, piece) in &self.pieces {
            if piece.declared.is_none() && !piece.closed {
                out.push(Seek::data(
                    VolumeIndex(*index),
                    ByteRange::at(0, 1),
                    Weight(1),
                ));
            }
        }
    }

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    // Its bytes come again and go where they went.
    fn unposted(&mut self, _volume: VolumeIndex, _range: ByteRange) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let index = volume.0;
        if let Some(Awaited {
            from: Some(start), ..
        }) = self.awaiting.get(&index)
        {
            out.push(Placement::Member {
                member: MEMBER,
                range: range.shift(*start as i64),
            });
            return;
        }
        let Some(start) = self.offset(index) else {
            out.push(Placement::Held { range });
            return;
        };
        let end = range.end.min(self.placeable(index)).max(range.start);
        if end > range.start {
            out.push(Placement::Member {
                member: MEMBER,
                range: ByteRange::new(start + range.start, start + end),
            });
        }
        if end < range.end {
            out.push(Placement::Held {
                range: ByteRange::new(end, range.end),
            });
        }
    }

    // The pieces a range of the member spans, as far as every piece before
    // it declared.
    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) -> Located {
        if member != MEMBER {
            return Located::Never;
        }
        let mut at = 0u64;
        for index in 0u32.. {
            if at >= range.end {
                return Located::Whole;
            }
            let Some(length) = self.pieces.get(&index).and_then(|piece| piece.declared) else {
                return Located::NotYet;
            };
            let span = ByteRange::at(at, length);
            if let Some(part) = span.intersect(range).filter(|part| !part.is_empty()) {
                out.push(Extent {
                    volume: VolumeIndex(index),
                    range: part.shift(-(at as i64)),
                });
            }
            at += length;
        }
        Located::NotYet
    }

    fn sized(&mut self, volume: VolumeIndex, length: u64, out: &mut Vec<Emit>) {
        if self
            .pieces
            .get(&volume.0)
            .is_some_and(|piece| !piece.closed)
        {
            self.resize(volume.0, length, out);
        }
    }

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8], length: Option<u64>) {
        let Some(piece) = split::parse(name) else {
            return;
        };
        if self.first.is_none() {
            self.first = Some(piece.numbered(FIRST));
        }
        if volume == VolumeIndex(0) || self.name.is_empty() {
            split::whole(name)
                .unwrap_or(name)
                .clone_into(&mut self.name);
        }
        let held = self.pieces.entry(volume.0).or_default();
        if held.declared.is_none() {
            held.declared = length.filter(|length| *length > 0);
        }
    }

    // The piece a recovery set named again names the member again when it
    // is the first.
    fn renamed(&mut self, volume: VolumeIndex, name: &str, out: &mut Vec<Emit>) {
        if volume != VolumeIndex(0) {
            return;
        }
        let whole = split::whole(name).unwrap_or(name);
        if whole != self.name {
            whole.clone_into(&mut self.name);
            if self.said.is_some() {
                self.said = None;
                self.say_member(out);
            }
        }
    }

    // A piece of my set by its name, wherever its number places it: what
    // its bytes open is the joined file's to say.
    fn claim(&mut self, name: &str, _first_bytes: &[u8], _length: Option<u64>) -> Vec<VolumeIndex> {
        self.place(name).into_iter().collect()
    }

    // The object its bytes founded holds it now.
    fn taken(&mut self, volume: VolumeIndex) {
        self.pieces.remove(&volume.0);
        self.awaiting.remove(&volume.0);
    }

    fn facts(&self) -> Facts {
        Facts::of(Kind::Split, self)
    }
}
