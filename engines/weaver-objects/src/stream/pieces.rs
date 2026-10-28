// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file posted in numbered pieces, `name.001`, `name.002`, …, including
// pieces whose first bytes identify a compressed stream. The identity join
// produces one member, `name`, which may open its own nested decoder. The
// set counts from `.001`, and any piece founds it. A missing ordinal has no
// known length: its neighbors cannot safely be joined across that gap.

use std::collections::BTreeSet;

use weaver_graph::{Emit, Kind, Standing, Why};

use super::Decoder;
use crate::split;

pub struct Pieces;

// How much of the joined bytes a run holds at once.
const WINDOW: u64 = 64 << 10;

impl Decoder for Pieces {
    const KIND: Kind = Kind::Split;
    const FIRST_PIECE: Option<u32> = Some(1);
    // The member is the pieces' bytes joined, nothing done to them.
    const IDENTITY: bool = true;

    fn window(_first: &[u8], _budget: &weaver_graph::Budget) -> u64 {
        WINDOW
    }

    // What the pieces were cut from.
    fn member(volume: &str) -> String {
        split::whole(volume).unwrap_or(volume).to_owned()
    }

    // The member is the pieces' bytes with nothing done to them, so damage
    // in it is theirs as they stand: it is delivered as the member, and the
    // pieces would carry the same damage under their own names. An absent
    // middle ordinal has no offset or length, so even a closed join (or a
    // successfully decoded descendant) is unsound. Retract that member and
    // its descendants before refusing the object and returning its volumes.
    // A member never closed, including a set whose first piece never came,
    // also gives back its pieces through the existing unplaced rule.
    fn quiet(standing: &dyn Standing, pieces: &BTreeSet<u32>, out: &mut Vec<Emit>) {
        let gap = pieces
            .iter()
            .zip(pieces.iter().skip(1))
            .any(|(before, after)| before.checked_add(1) != Some(*after));
        if gap {
            out.push(Emit::Retract(super::run::MEMBER));
            out.push(Emit::Refuse(Why::Unrepairable));
        } else {
            out.extend(crate::quiet::unplaced(standing).map(Emit::Refuse));
        }
    }
}
