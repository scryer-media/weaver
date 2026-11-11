// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a restored recovery object asked for again, read off its volumes'
// files away from the loop. A recovery object's volumes stay in their
// members' own files, and it places nothing in members below it, so only
// the reading is slow: the object stays in its node and answers for the job
// meanwhile, and each piece read comes back to it as a late arrival does,
// forgotten first and then delivered. Nothing is staged and nothing is
// written: the bytes are on their file already. The close its volumes were
// told is told again once the last piece came.

use std::collections::BTreeSet;

use super::{Arrived, Builder, Effect, MemberId, NodeId, Tree};
use crate::deliver::{Posted, Provenance};
use crate::evidence::Evidenced;
use crate::object::{Placement, Posting};
use crate::volume::{ByteRange, VolumeIndex};

// One object's restart asks, ready to be read anywhere: what arrived of
// them in pieces of at most `AGAIN_PIECE` bytes, each with the provenance
// the volume's member keeps for it, and the map of where they lie, taken as
// the object stood.
pub struct Rewalk {
    pub node: NodeId,
    pieces: Vec<(VolumeIndex, ByteRange, Provenance)>,
    posting: Box<dyn Posting>,
}

impl Rewalk {
    pub fn len(&self) -> usize {
        self.pieces.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pieces.is_empty()
    }

    // Piece `index` read through the map. `read` answers a placement of a
    // volume. `Err` with the piece's range when it does not read back
    // whole: what arrived there is no longer where it was put, and the
    // range is fetched again. `None` past the last piece.
    pub fn read(
        &self,
        index: usize,
        read: &mut dyn FnMut(VolumeIndex, &Placement, &mut Vec<u8>) -> bool,
    ) -> Option<(VolumeIndex, Result<Posted, ByteRange>)> {
        let &(volume, piece, provenance) = self.pieces.get(index)?;
        let mut bytes = Vec::new();
        let mut placed = |placement: &Placement, into: &mut Vec<u8>| read(volume, placement, into);
        if !self.posting.posted(volume, piece, &mut placed, &mut bytes)
            || bytes.len() as u64 != piece.len()
        {
            return Some((volume, Err(piece)));
        }
        Some((
            volume,
            Ok(Posted {
                offset: piece.start,
                bytes: Evidenced::from(bytes),
                provenance,
            }),
        ))
    }
}

impl Tree {
    // The nodes whose restored recovery object asked for something to be
    // read again, in node order.
    pub fn rewalks(&self) -> Vec<NodeId> {
        self.rewalks.keys().copied().collect()
    }

    // Whether the object at `node` is still to hear what arrived of its
    // volumes again, asked or being read.
    pub(super) fn rewalk_owed(&self, node: NodeId) -> bool {
        self.rewalks.contains_key(&node) || self.rewalking.contains(&node)
    }

    // Whether any restored recovery object is still to hear what arrived
    // of its volumes again: until none is, the job fetches nothing.
    pub fn rewalks_owed(&self) -> bool {
        !self.rewalks.is_empty() || !self.rewalking.is_empty()
    }

    // What the object at `node` asked again at its restore, taken to be
    // read. `None` when it asked nothing, is no longer live, or is not home
    // to say where its volumes lie. A retired node's asks are dropped, as it
    // hears nothing; one not home keeps them, and is asked again.
    pub fn take_rewalk(&mut self, node: NodeId) -> Option<Rewalk> {
        if !self.is_live(node) {
            self.rewalks.remove(&node);
            return None;
        }
        let ranges = self.rewalks.get(&node)?;
        let pieces: Vec<(VolumeIndex, ByteRange, Provenance)> = self
            .forgets(node, ranges, false)
            .into_iter()
            .flat_map(|(volume, range)| {
                self.again_pieces(node, volume, range)
                    .into_iter()
                    .map(move |(piece, provenance)| (volume, piece, provenance))
            })
            .collect();
        let taken: Vec<(VolumeIndex, ByteRange)> = pieces
            .iter()
            .map(|&(volume, piece, _)| (volume, piece))
            .collect();
        let posting = self.object(node)?.posting(&taken);
        self.rewalks.remove(&node);
        self.rewalking.insert(node);
        Some(Rewalk {
            node,
            pieces,
            posting,
        })
    }

    // A piece read again comes to the object as a late arrival: forgotten,
    // then delivered, at the length the volume's member has now.
    pub fn rewalked(
        &mut self,
        node: NodeId,
        volume: VolumeIndex,
        posted: Posted,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        if !self.is_live(node) {
            return;
        }
        let Some(length) = self
            .volumes(node)
            .and_then(|(parent, volumes)| Some((parent, *volumes.get(&volume)?)))
            .map(|(parent, member)| {
                self.namespace(parent)
                    .and_then(|namespace| namespace.get(member))
                    .and_then(|record| record.length)
            })
        else {
            return;
        };
        let piece = ByteRange::at(posted.offset, posted.bytes.len() as u64);
        self.uncover(node, volume, piece);
        let arrived = Arrived {
            volume,
            offset: posted.offset,
            bytes: posted.bytes,
            provenance: posted.provenance,
        };
        self.deliver_to_object(node, &arrived, length, builder, effects);
    }

    // A rewalk that went down with its run is owed no more: the job it
    // belonged to has failed, and nothing is to hear its close.
    pub fn rewalk_dropped(&mut self, node: NodeId) {
        self.rewalking.remove(&node);
    }

    // The last piece came, or the reading ended short: each volume whose
    // member is closed hears its close, held from it while it read.
    pub fn rewalk_done(
        &mut self,
        node: NodeId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        if !self.rewalking.remove(&node) || !self.is_live(node) {
            return;
        }
        let members: BTreeSet<MemberId> = self
            .volumes(node)
            .map(|(_, volumes)| volumes.values().copied().collect())
            .unwrap_or_default();
        self.closed_again(node, members, builder, effects);
    }
}
