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
// they came. A set is one file cut in pieces, so only its first piece starts
// with a head: a later piece whose front opens on a signature that stands
// only at a file's head refutes the set, whichever piece came first, and its
// pieces go back as they came to be claimed by their own bytes, nothing
// failed. A signature that may recur inside a file refutes nothing.
//
// A set posted under names that number nothing is placed as the tree bound
// it, each piece at the place its founding gave it: no name of such a set
// says a place, so I claim none.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Claim, Delivery, Durable, Emit, Extent, Facts, HEAD_VERDICT_BYTES, Kind, Located,
    MemberId, Object, Placement, Pull, Seek, Sniff, Standing, VolumeIndex, Weight, Why,
    signs_a_head,
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
    // Whether my pieces were bound at their places with no name that
    // numbers them: then no name places a piece in my set.
    stated: bool,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Piece {
    // The piece's length: what its articles declare, what it reached once
    // it closed, and what its member is given anew after. A piece is never
    // empty, so none is said as none.
    declared: Option<u64>,
    // How far its bytes and its losses reached before it closed.
    reached: u64,
    // The bytes it was handed and could not place, which its owner holds
    // and hands again once they can be: in order, none touching another.
    held: Vec<ByteRange>,
    // How far the bytes it placed in the member reach, from its start.
    placed: u64,
    closed: bool,
    // What its front said of the set (`refutes`). Kept: a restart between
    // two short articles of the front still judges both.
    front: Front,
}

// A later piece's front, judged once whether it opens a file of its own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Front {
    // Not judged yet: its good bytes from offset zero that came so far, as
    // many as a head-only signature reads at most (`HEAD_VERDICT_BYTES`).
    // Damaged bytes stop it where they start, until a repair brings them.
    Open(Vec<u8>),
    // Nothing to judge: it joined the set or refuted it, or it is the
    // first piece, whose head opens the joined file.
    Judged,
}

impl Default for Front {
    fn default() -> Self {
        Self::Open(Vec::new())
    }
}

impl Piece {
    // Closed, with nothing it was handed within its length still held.
    fn whole(&self) -> bool {
        let length = self.declared.unwrap_or(0);
        self.closed && self.held.iter().all(|range| range.start >= length)
    }

    // Bytes it was handed: those in `placed` went to the member and are no
    // longer held, those in `held` are.
    fn handed(&mut self, placed: ByteRange, held: ByteRange) {
        if !placed.is_empty() && !self.held.is_empty() {
            let mut tail = None;
            self.held.retain_mut(|run| {
                if run.end <= placed.start || run.start >= placed.end {
                    return true;
                }
                if run.end > placed.end {
                    if run.start < placed.start {
                        tail = Some(ByteRange::new(placed.end, run.end));
                        run.end = placed.start;
                    } else {
                        run.start = placed.end;
                    }
                    return true;
                }
                run.end = placed.start;
                run.start < placed.start
            });
            self.held.extend(tail);
        }
        if !held.is_empty() {
            self.held.push(held);
        }
        self.held.sort_unstable_by_key(|run| run.start);
        self.held.dedup_by(|next, kept| {
            let touches = next.start <= kept.end;
            if touches {
                kept.end = kept.end.max(next.end);
            }
            touches
        });
        if !placed.is_empty() {
            self.placed = self.placed.max(placed.end);
        }
    }
}

#[derive(Clone, Copy)]
struct Before {
    whole: bool,
    awaiting: bool,
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

    // Whether a piece's own bytes refute the claim its name made: a set is
    // one file cut in pieces, so only its first starts with a head, and a
    // later piece whose front opens on a signature that stands only at a
    // file's head is a file of its own, and so is the set. Judged once, on
    // the front its deliveries gather from offset zero, however its
    // articles cut it and whichever piece came first. Damaged bytes say
    // nothing of the front: it waits for the bytes a repair brings over
    // them, and is judged on those. The first piece is never judged
    // (`bound`).
    fn refutes(&mut self, index: u32, delivery: &Delivery<'_>) -> bool {
        if delivery.provenance.is_damaged() {
            return false;
        }
        let Some(piece) = self.pieces.get_mut(&index) else {
            return false;
        };
        let Front::Open(front) = &mut piece.front else {
            return false;
        };
        let Some(at) = usize::try_from(delivery.offset)
            .ok()
            .filter(|at| *at <= front.len() && *at < HEAD_VERDICT_BYTES)
        else {
            return false;
        };
        let bytes = delivery.bytes.as_ref();
        let end = at.saturating_add(bytes.len()).min(HEAD_VERDICT_BYTES);
        if end > front.len() {
            front.reserve_exact(HEAD_VERDICT_BYTES - front.len());
            front.resize(end, 0);
        }
        front[at..end].copy_from_slice(&bytes[..end - at]);
        match signs_a_head(front, piece.declared) {
            Sniff::Pending => false,
            verdict => {
                piece.front = Front::Judged;
                matches!(verdict, Sniff::Object(_))
            }
        }
    }

    // The place a piece's name numbers in my set, counted from one (`FIRST`):
    // `None` for a piece of another set. A set whose lowest number is above
    // one has no first piece here, so a front of any of its pieces that
    // opens on a head refutes it (`refutes`).
    fn place(&self, name: &str) -> Option<VolumeIndex> {
        if self.stated {
            return None;
        }
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
    // anywhere once its start is known and it declared none, but never
    // into a place any piece's bytes still sit in until they come again,
    // its own among them, wherever that place starts: they are read back
    // from there. Nothing while its start is unknown.
    fn placeable(&self, index: u32) -> u64 {
        let (Some(start), Some(piece)) = (self.offset(index), self.pieces.get(&index)) else {
            return 0;
        };
        let declared = piece.declared.unwrap_or(u64::MAX);
        let end = start.saturating_add(declared);
        self.awaiting
            .values()
            .filter_map(|awaited| Some(ByteRange::at(awaited.from?, awaited.until)))
            .filter(|sitting| sitting.start < end && sitting.end > start)
            .map(|sitting| sitting.start.saturating_sub(start))
            .fold(declared, u64::min)
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
    // its length with every byte it was handed within it placed, and
    // nothing is still to come again.
    fn close_member(&mut self, out: &mut Vec<Emit>) {
        if self.awaiting.is_empty()
            && !self.pieces.is_empty()
            && self.contiguous()
            && self.pieces.values().all(Piece::whole)
            && self.length().is_some()
        {
            self.say_member(out);
            out.push(Emit::MemberClosed(MEMBER));
        }
    }

    // What the member's close turns on, as a piece stands before a change
    // to it: whether it was whole, and whether bytes were still to come
    // again.
    fn before(&self, index: u32) -> Option<Before> {
        let piece = self.pieces.get(&index)?;
        Some(Before {
            whole: piece.whole(),
            awaiting: !self.awaiting.is_empty(),
        })
    }

    // The member can close only on what a change to a piece changed: the
    // piece became whole, or the last bytes coming again came.
    fn settle(&mut self, index: u32, before: Before, out: &mut Vec<Emit>) {
        let whole = self.pieces.get(&index).is_some_and(Piece::whole);
        if whole && (!before.whole || (before.awaiting && self.awaiting.is_empty())) {
            self.close_member(out);
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
        let before: Vec<(u32, Option<u64>)> = self
            .pieces
            .keys()
            .map(|&at| (at, self.offset(at)))
            .collect();
        if let Some(piece) = self.pieces.get_mut(&index) {
            piece.declared = Some(length);
        }
        for (at, start) in before.into_iter().rev() {
            let now = self.offset(at);
            let Some(piece) = self.pieces.get_mut(&at) else {
                continue;
            };
            if piece.placed == 0 || start == now {
                continue;
            }
            let until = piece.placed;
            out.push(Emit::Again {
                volume: VolumeIndex(at),
                range: ByteRange::new(0, until),
            });
            piece.placed = 0;
            self.awaiting.insert(at, Awaited { from: start, until });
        }
        self.say_member(out);
    }
}

impl Durable for SplitStore {
    // 4: a store of the pieces' places, where the stream that joined them
    // kept 3.
    // 5: the pieces whose bytes come again, and where they sat.
    // 6: the length each piece's member was last given.
    // 7: whether the set was bound with no name that numbers it.
    // 8: what each later piece's front said of the set, or as much of it as
    // came.
    // 9: that length gone again: a piece's length is the one it holds; and
    // what each piece holds unplaced, where it kept how far it was handed.
    const VERSION: u16 = 9;
}

impl Object for SplitStore {
    fn kind(&self) -> Kind {
        Kind::Split
    }

    // Bytes of a piece go where the piece starts in the member, as far as
    // its declared length; the rest is held until its place is known. A
    // piece whose own front refutes the set is placed as any is, so every
    // piece sits where my map says it does and goes back as it was posted,
    // and then the set is refuted.
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        let index = delivery.volume.0;
        let refuted = self.refutes(index, &delivery);
        let range = delivery.range();
        let Some(before) = self.before(index) else {
            return;
        };
        // Bytes reaching into what a piece placed before it moved are those
        // bytes coming again: read back already, they stand where they go
        // now. Bytes past them are new, and its old place still waits.
        if let Some(awaited) = self.awaiting.get_mut(&index)
            && range.start < awaited.until
        {
            awaited.from = None;
        }
        let Some(piece) = self.pieces.get_mut(&index) else {
            return;
        };
        if !piece.closed {
            piece.reached = piece.reached.max(range.end);
        }
        self.say_member(out);
        let placeable = self.placeable(index);
        let start = self.offset(index);
        let end = range.end.min(placeable).max(range.start);
        match start {
            Some(start) if end > range.start => {
                let len = (end - range.start) as usize;
                out.push(Emit::Bytes {
                    member: MEMBER,
                    offset: start + range.start,
                    bytes: delivery.bytes.slice(..len),
                    provenance: delivery.provenance,
                });
            }
            _ => out.push(Emit::Pending),
        }
        let end = if start.is_some() { end } else { range.start };
        if let Some(piece) = self.pieces.get_mut(&index) {
            piece.handed(
                ByteRange::new(range.start, end),
                ByteRange::new(end, range.end),
            );
        }
        // The member waits for the last of a piece's bytes coming again.
        if self
            .awaiting
            .get(&index)
            .is_some_and(|awaited| awaited.from.is_none() && range.end >= awaited.until)
        {
            self.awaiting.remove(&index);
        }
        self.settle(index, before, out);
        if refuted {
            out.push(Emit::Refuted);
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

    // A length the piece's member is given is the piece's, closed or not:
    // the member closes at what arrived of it, and a recovery set that
    // proves another length outranks that, as its bytes outrank what the
    // piece held. The length the piece holds said again changes nothing.
    fn sized(&mut self, volume: VolumeIndex, length: u64, out: &mut Vec<Emit>) {
        let Some(before) = self.before(volume.0) else {
            return;
        };
        self.resize(volume.0, length, out);
        self.settle(volume.0, before, out);
    }

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8], length: Option<u64>) {
        match split::parse(name) {
            Some(piece) if !self.stated => {
                if self.first.is_none() {
                    self.first = Some(piece.numbered(FIRST));
                }
            }
            // A piece no name numbers is where it was bound, and so is
            // every piece of its set.
            _ if self.first.is_none() => self.stated = true,
            _ => return,
        }
        if volume == VolumeIndex(0) || self.name.is_empty() {
            split::whole(name)
                .unwrap_or(name)
                .clone_into(&mut self.name);
        }
        let held = self.pieces.entry(volume.0).or_insert_with(|| Piece {
            front: if volume == VolumeIndex(0) {
                Front::Judged
            } else {
                Front::default()
            },
            ..Piece::default()
        });
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
    // the first piece's bytes open is the joined file's to say, and a later
    // piece's front refutes the set when it reaches me (`refutes`).
    fn claim(&mut self, name: &str, _first_bytes: &[u8], _length: Option<u64>) -> Claim {
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
