// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file cut in pieces and posted under names that number nothing, which a
// recovery set covers by those same names. No name says the pieces are one
// file, and the set lists its files in no posting order, so the pieces say
// it themselves: the first is the one whose front signs a kind, and the
// object that front founded refused it though the set proves every byte of
// it, so what the object reads runs past the piece. With one piece besides
// the first, that piece is the last whatever its length. With more, the last
// is the one shorter than the first and any between are as long as it.
// Such a set founds the split store, whose joined member's own front says
// what it is.
// What each piece is, the tree never asks.

use std::collections::BTreeMap;

use super::{Builder, Effect, NodeId, Tree};
use crate::member::MemberId;
use crate::object::Kind;
use crate::recovery::RepairOutcome;
use crate::sniff::{Sniff, signed};
use crate::volume::{ByteRange, VolumeIndex};

// The pieces a set can have between its first and its last and still be
// ordered: those between are as long as each other, so two or more of them
// could go in any order, and such a set stays as it was posted.
const BETWEEN: usize = 1;

// The pieces of one file, in order, and the further postings of each.
struct Pieces {
    order: Vec<MemberId>,
    copies: Vec<(MemberId, MemberId)>,
}

impl Tree {
    // A member its object refused, after the owner has its bytes back in its
    // own file. When the recovery set that covers it proves every byte of
    // it, it was refused for what lies past it, and when it is the first
    // piece of a file the recovery set beside it covers, the split store is
    // founded over the pieces, in their order, and each further posting of
    // a piece folds into it. Otherwise nothing changes: the member stays
    // refused, as posted.
    pub fn refused_whole(
        &mut self,
        node: NodeId,
        member: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(Pieces { order, copies }) = self.pieces(node, member) else {
            return;
        };
        for piece in &order {
            self.nodes.unretire(&(node, *piece));
            if let Some(mut members) = self.nodes.members_mut(node) {
                members.decide(*piece);
            }
        }
        let volumes: BTreeMap<VolumeIndex, MemberId> = order
            .iter()
            .enumerate()
            .map(|(index, piece)| (VolumeIndex(index as u32), *piece))
            .collect();
        let Ok(Some(child)) = self.attach(node, Kind::Split, |_, _| Some(volumes), builder) else {
            return;
        };
        for piece in order {
            self.bound(node, piece, Kind::Split, child, effects);
        }
        for (copy, into) in copies {
            self.fold(node, copy, into, effects);
        }
    }

    // The pieces `first` leads, when the recovery set that covers it covers
    // them too: members no object holds, whose fronts sign nothing, each
    // with its length. The further postings of a piece fold into it.
    fn pieces(&self, node: NodeId, first: MemberId) -> Option<Pieces> {
        let namespace = self.namespace(node)?;
        let record = namespace.get(first)?;
        let length = record.length?;
        if !matches!(
            signed(namespace.first_bytes(first), record.length),
            Sniff::Object(_)
        ) {
            return None;
        }
        let set = self
            .recovery_nodes(node)
            .into_iter()
            .find(|set| self.recovery(*set).is_some_and(|set| set.covers(first)))?;
        let recovery = self.recovery(set)?;
        let ruled = matches!(
            recovery.outcome(),
            Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
        );
        if !ruled || recovery.proves(first, length) != ByteRange::new(0, length) {
            return None;
        }
        let covered = |member: MemberId| self.recovery(set).is_some_and(|set| set.covers(member));
        let mut postings: Vec<(MemberId, u64)> = Vec::new();
        let mut copies = Vec::new();
        for other in namespace.members() {
            let piece = other.id;
            if piece == first
                || namespace.copy_of(piece).is_some()
                || self.child_of(node, piece).is_some()
                || !other.fronted
                || !covered(piece)
                || matches!(
                    signed(namespace.first_bytes(piece), other.length),
                    Sniff::Object(_)
                )
            {
                continue;
            }
            let Some(own) = other.length else {
                continue;
            };
            match postings
                .iter()
                .find(|(posted, _)| namespace.folds(piece, *posted))
            {
                Some((posted, _)) => copies.push((piece, *posted)),
                None => postings.push((piece, own)),
            }
        }
        let order = ordered(first, length, &postings)?;
        copies.retain(|(_, into)| order.contains(into));
        Some(Pieces { order, copies })
    }
}

// The pieces in order, from the first and its length and the postings
// besides it. One other is the last, whatever its length. Of several, the
// last is the one shorter than the first and the rest are between, each as
// long as the first; with no single shorter one, or more between than can be
// ordered, there is no order.
fn ordered(first: MemberId, length: u64, postings: &[(MemberId, u64)]) -> Option<Vec<MemberId>> {
    if let [(last, _)] = postings {
        return Some(vec![first, *last]);
    }
    let between: Vec<MemberId> = postings
        .iter()
        .filter(|(_, own)| *own == length)
        .map(|(piece, _)| *piece)
        .collect();
    let last: Vec<MemberId> = postings
        .iter()
        .filter(|(_, own)| *own < length)
        .map(|(piece, _)| *piece)
        .collect();
    let [last] = last[..] else {
        return None;
    };
    if between.len() > BETWEEN {
        return None;
    }
    Some(
        std::iter::once(first)
            .chain(between)
            .chain(std::iter::once(last))
            .collect(),
    )
}
