// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The piece root: the root of a job whose bytes come as a torrent's pieces.
// Its volumes are the torrent's files in metainfo order, so root volume N is
// root member N. A piece is the unit it fetches: one piece can carry the end
// of one file and the start of the next, so each (volume, piece) pair the
// piece covers is a unit of that volume, numbered by the piece. A piece's
// bytes count only once it verified against the metainfo's hash; each file's
// share of it is then delivered in that file's own coordinates.
//
// A unit is lost only when the torrent is abandoned while it was never in
// hand. A piece no peer has now is waiting, not lost: a peer may yet come.
// Every volume range maps onto whole pieces, so a seek is never unseekable.
// The root has no block checksums for the namespace: a piece's hash covers a
// piece, which straddles files and never lines up with a recovery block, so
// the recovery objects read what they need back, as for any bytes no source
// vouched for.
//
//   layout    the torrent's files over its pieces: ranges to pieces and back
//   facts     the root out as facts and back

mod facts;
mod layout;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use bitvec::prelude::{BitVec, Msb0};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use weaver_graph::{ByteRange, MemberId, NodeId, Provenance, Pull, Segment, Tree, VolumeIndex};

pub(crate) use self::facts::PieceFacts;
pub(crate) use self::layout::{LayoutError, PieceFile, PieceLayout};
use super::window::Window;
use super::{Ask, Asked, Root, round, sample};
use crate::pipeline::graph::namespace::{Arrival, Segments};

// How soon a piece is fetched, soonest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum Class {
    // Something cannot progress until these bytes land.
    Critical,
    // Recovery blocks a repair is waiting on.
    Recovery,
    // The first piece of a file, which says what the file is.
    Head,
    // A volume an in-order reader will reach next, by draw order.
    Window(u32),
    Ordinary,
}

// A change of class the fetch has to apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Move {
    pub(crate) piece: u32,
    pub(crate) class: Class,
}

// One file's share of a verified piece, in the file's coordinates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Verified {
    pub(crate) volume: VolumeIndex,
    pub(crate) piece: u32,
    pub(crate) offset: u64,
    pub(crate) bytes: Bytes,
}

// Where one unit stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Unit {
    Waiting,
    // In hand, with the SHA-1 of the share as it was handed over; `None`
    // when an earlier run fetched it and this one never saw the bytes.
    Arrived(Option<[u8; 20]>),
    // The torrent was abandoned without it. `damaged` once its range went
    // to the tree.
    Lost { damaged: bool },
}

#[derive(Debug)]
pub(crate) struct PieceRoot {
    layout: Arc<PieceLayout>,
    // Per volume, one unit per piece carrying its bytes, from its first.
    units: Vec<Vec<Unit>>,
    // Per piece.
    classes: Vec<Class>,
    window: Window,
    moves: Vec<Move>,
}

impl PieceRoot {
    pub(crate) fn new(layout: Arc<PieceLayout>) -> Self {
        let units = (0..layout.volume_count())
            .map(|volume| {
                let pieces = layout.volume_pieces(VolumeIndex(volume as u32));
                vec![Unit::Waiting; pieces.len()]
            })
            .collect();
        Self {
            classes: vec![Class::Ordinary; layout.piece_count() as usize],
            layout,
            units,
            window: Window::default(),
            moves: Vec::new(),
        }
    }

    fn unit(&self, volume: VolumeIndex, piece: u32) -> Option<&Unit> {
        let first = self.layout.volume_pieces(volume).start;
        self.units
            .get(volume.0 as usize)?
            .get(piece.checked_sub(first)? as usize)
    }

    fn unit_mut(&mut self, volume: VolumeIndex, piece: u32) -> Option<&mut Unit> {
        let first = self.layout.volume_pieces(volume).start;
        self.units
            .get_mut(volume.0 as usize)?
            .get_mut(piece.checked_sub(first)? as usize)
    }

    // Every unit with its volume and piece.
    fn each(&self) -> impl Iterator<Item = (VolumeIndex, u32, Unit)> + '_ {
        self.units
            .iter()
            .enumerate()
            .flat_map(move |(index, units)| {
                let volume = VolumeIndex(index as u32);
                let first = self.layout.volume_pieces(volume).start;
                units
                    .iter()
                    .enumerate()
                    .map(move |(at, unit)| (volume, first + at as u32, *unit))
            })
    }

    // Whether the volume's share of the piece is still to come.
    pub(crate) fn waiting(&self, volume: VolumeIndex, piece: u32) -> bool {
        self.unit(volume, piece) == Some(&Unit::Waiting)
    }

    // A verified piece's share: record what it was, and hand back the
    // delivery for the namespace.
    pub(crate) fn receive(&mut self, verified: Verified) -> Arrival {
        let digest: [u8; 20] = Sha1::digest(&verified.bytes).into();
        if let Some(unit) = self.unit_mut(verified.volume, verified.piece) {
            *unit = Unit::Arrived(Some(digest));
        }
        Arrival {
            node: NodeId::ROOT,
            member: MemberId(verified.volume.0),
            offset: verified.offset,
            bytes: verified.bytes,
            provenance: Provenance::Fresh,
        }
    }

    // The pieces in hand, one bit per piece, most significant first: a
    // piece is in hand when every file's share of it is.
    pub(crate) fn have(&self) -> BitVec<u8, Msb0> {
        let mut have = BitVec::repeat(true, self.layout.piece_count() as usize);
        for (_, piece, unit) in self.each() {
            if !matches!(unit, Unit::Arrived(_)) {
                have.set(piece as usize, false);
            }
        }
        have
    }

    // Each file's bytes in hand, by its index in the torrent.
    pub(crate) fn file_have(&self) -> Vec<u64> {
        let mut have = vec![0; self.layout.files().count()];
        for (volume, piece, unit) in self.each() {
            if let (Unit::Arrived(_), Some(span)) = (unit, self.layout.span(volume, piece)) {
                have[self.layout.volume_file(volume)] += span.len();
            }
        }
        have
    }

    // The files to fetch now, by their index in the torrent: the ones
    // carrying a waiting piece raised above the ordinary class, or every
    // file still waiting when none is raised. A selection of files is all
    // the fetch can be told, so the order among the raised is the fetch's.
    pub(crate) fn selection(&self) -> Vec<usize> {
        let waiting: Vec<(VolumeIndex, u32)> = self
            .each()
            .filter(|(_, _, unit)| *unit == Unit::Waiting)
            .map(|(volume, piece, _)| (volume, piece))
            .collect();
        let raised = waiting
            .iter()
            .any(|(_, piece)| self.classes[*piece as usize] < Class::Ordinary);
        let mut files: Vec<usize> = waiting
            .into_iter()
            .filter(|(_, piece)| !raised || self.classes[*piece as usize] < Class::Ordinary)
            .map(|(volume, _)| self.layout.volume_file(volume))
            .collect();
        files.dedup();
        files
    }

    // Answer a seek, and raise the pieces that carry it to `class` when one
    // is given. A lost unit loses the seek, but every piece of the range
    // that can still come is raised all the same.
    fn seek(&mut self, volume: VolumeIndex, range: ByteRange, class: Option<Class>) -> Pull {
        if volume.0 as usize >= self.units.len() {
            return Pull::Lost;
        }
        let pieces = self.layout.pieces(volume, range);
        let lost = pieces
            .clone()
            .any(|piece| matches!(self.unit(volume, piece), Some(Unit::Lost { .. })));
        let mut moved = 0;
        for piece in pieces {
            if let Some(class) = class.filter(|class| *class < self.classes[piece as usize])
                && self.waiting(volume, piece)
            {
                self.classes[piece as usize] = class;
                self.moves.push(Move { piece, class });
                moved += 1;
            }
        }
        if lost { Pull::Lost } else { Pull::Moved(moved) }
    }

    fn volume_waiting(&self, volume: VolumeIndex) -> bool {
        waiting_in(&self.units, volume)
    }
}

fn waiting_in(units: &[Vec<Unit>], volume: VolumeIndex) -> bool {
    units
        .get(volume.0 as usize)
        .is_some_and(|units| units.contains(&Unit::Waiting))
}

impl Root for PieceRoot {
    type Intake = Verified;
    type Class = Class;
    type Move = Move;
    type Facts = PieceFacts;
    type Kept = ();
    const NAME: &'static str = "pieces";
    const FACTS_VERSION: u16 = PieceFacts::VERSION;

    fn facts(&self) -> PieceFacts {
        PieceRoot::facts(self)
    }

    fn arrived_before(&mut self, mut queued: impl FnMut(VolumeIndex, u32) -> bool) {
        let fetched: Vec<(VolumeIndex, u32)> = self
            .each()
            .filter(|(volume, piece, unit)| *unit == Unit::Waiting && !queued(*volume, *piece))
            .map(|(volume, piece, _)| (volume, piece))
            .collect();
        for (volume, piece) in fetched {
            if let Some(unit) = self.unit_mut(volume, piece) {
                *unit = Unit::Arrived(None);
            }
        }
    }

    fn any_head_waiting(&self) -> bool {
        self.units
            .iter()
            .any(|units| units.first() == Some(&Unit::Waiting))
    }

    fn class(&self, volume: VolumeIndex, unit: u32) -> Option<Class> {
        self.unit(volume, unit)?;
        self.classes.get(unit as usize).copied()
    }

    fn sample(&mut self) {
        for index in 0..self.units.len() {
            let volume = VolumeIndex(index as u32);
            let first = self.layout.volume_pieces(volume).start;
            if self.waiting(volume, first) && Class::Head < self.classes[first as usize] {
                self.classes[first as usize] = Class::Head;
                self.moves.push(Move {
                    piece: first,
                    class: Class::Head,
                });
            }
        }
    }

    fn take_moves(&mut self) -> Vec<Move> {
        let mut moves = std::mem::take(&mut self.moves);
        // A later raise of the same piece supersedes an earlier one.
        let mut latest: HashMap<u32, Class> = HashMap::new();
        for found in &moves {
            latest.insert(found.piece, found.class);
        }
        moves.retain(|found| latest.get(&found.piece) == Some(&found.class));
        moves.sort_by_key(|found| (found.class, found.piece));
        moves.dedup();
        moves
    }

    fn receive(&mut self, intake: Verified) -> (Arrival, Option<u64>) {
        // Every file's length is in the metainfo, announced with the file.
        (PieceRoot::receive(self, intake), None)
    }

    fn lost(&mut self, volume: VolumeIndex, unit: u32) {
        if let Some(found) = self.unit_mut(volume, unit)
            && *found == Unit::Waiting
        {
            *found = Unit::Lost { damaged: false };
        }
    }

    fn volume_waiting(&self, volume: VolumeIndex) -> bool {
        PieceRoot::volume_waiting(self, volume)
    }

    fn bootstrap(&mut self, tree: &Tree) {
        let units = &self.units;
        sample::bootstrap(tree, |volume| waiting_in(units, volume), &mut self.window);
    }

    fn answer(&mut self, asks: &[Ask], queued: impl FnMut(VolumeIndex) -> bool) -> Vec<Pull> {
        let (classes, drawn) = round(&mut self.window, asks, queued);
        let answers = asks
            .iter()
            .zip(classes)
            .map(|(ask, asked)| {
                let class = asked.map(|asked| match asked {
                    Asked::Critical => Class::Critical,
                    Asked::Recovery => Class::Recovery,
                    Asked::Window(rank) => Class::Window(rank),
                });
                self.seek(ask.seek.volume, ask.seek.range, class)
            })
            .collect();
        for (bid, rank) in drawn {
            self.seek(bid.volume, bid.range, Some(Class::Window(rank)));
        }
        answers
    }

    // Each lost share's exact range, runs of them merged.
    fn damage(&mut self) -> Vec<(VolumeIndex, ByteRange)> {
        let fresh: Vec<(VolumeIndex, u32)> = self
            .each()
            .filter(|(_, _, unit)| *unit == Unit::Lost { damaged: false })
            .map(|(volume, piece, _)| (volume, piece))
            .collect();
        let mut out: Vec<(VolumeIndex, ByteRange)> = Vec::new();
        for (volume, piece) in fresh {
            if let Some(unit) = self.unit_mut(volume, piece) {
                *unit = Unit::Lost { damaged: true };
            }
            let Some(range) = self.layout.span(volume, piece) else {
                continue;
            };
            match out.last_mut() {
                Some((last, run)) if *last == volume && run.end == range.start => {
                    run.end = range.end;
                }
                _ => out.push((volume, range)),
            }
        }
        out
    }

    fn volume_count(&self) -> usize {
        self.layout.volume_count()
    }

    fn arrived_ranges(&self, volume: VolumeIndex) -> Vec<(u32, ByteRange)> {
        self.each()
            .filter(|(found, _, unit)| *found == volume && matches!(unit, Unit::Arrived(_)))
            .filter_map(|(_, piece, _)| Some((piece, self.layout.span(volume, piece)?)))
            .collect()
    }

    // A unit's place is the layout's, so none is ever in hand unplaced.
    fn unseen(&self) -> Vec<(VolumeIndex, u32)> {
        Vec::new()
    }

    fn unarrive(&mut self, volume: VolumeIndex, unit: u32) {
        if let Some(found) = self.unit_mut(volume, unit) {
            *found = Unit::Waiting;
        }
    }

    fn settled(&self) -> Vec<(VolumeIndex, u32)> {
        self.each()
            .filter(|(_, _, unit)| *unit != Unit::Waiting)
            .map(|(volume, piece, _)| (volume, piece))
            .collect()
    }

    // Each share wholly inside the range must hash as it did when handed
    // over; one this run never saw, or only partly in the range, is taken
    // as read.
    fn matches(&self, volume: VolumeIndex, range: ByteRange, bytes: &[u8]) -> bool {
        self.layout
            .pieces(volume, range)
            .filter_map(|piece| match self.unit(volume, piece) {
                Some(Unit::Arrived(Some(digest))) => {
                    Some((self.layout.span(volume, piece)?, *digest))
                }
                _ => None,
            })
            .filter(|(span, _)| range.covers(*span))
            .all(|(span, digest)| {
                let from = (span.start - range.start) as usize;
                bytes
                    .get(from..from + span.len() as usize)
                    .is_some_and(|share| <[u8; 20]>::from(Sha1::digest(share)) == digest)
            })
    }
}

// A piece's hash vouches for no block the namespace's recovery objects
// fold, so the root gives none.
impl Segments for PieceRoot {
    fn segments_for(&self, _: NodeId, _: VolumeIndex, _: ByteRange, _: &mut Vec<Segment>) {}
}
