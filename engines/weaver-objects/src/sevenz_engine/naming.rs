// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a 7z set numbers its volumes in their names: as pieces of a split
// file, `name.7z.001`, `name.7z.002`, …, counted from the first volume's
// own number, so a set renamed to `hash.10`, `hash.11`, … chains the same
// way. The first volume is the one with the signature; no other carries
// one. A lone `name.7z` is a set of one and names no siblings.

use std::collections::BTreeMap;

use crate::engine::SetName;
use crate::split::{self, Piece};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VolumeName {
    piece: Piece,
}

impl VolumeName {
    // Where this volume falls in the set whose first volume is `first`.
    pub(crate) fn index_from(&self, first: &VolumeName) -> Option<u32> {
        self.piece.index_from(&first.piece)
    }
}

impl SetName for VolumeName {
    // The volume a conventional name is, numbered from `.001`; the set's
    // first volume, when it is bound, says otherwise.
    fn index(&self) -> u32 {
        self.piece.number().saturating_sub(1)
    }

    fn same_set(&self, other: &VolumeName) -> bool {
        self.piece.same_set(&other.piece)
    }

    fn slot(&self, bound: &BTreeMap<u32, Self>) -> Option<u32> {
        let index = match bound.get(&0) {
            Some(first) => self.index_from(first)?,
            None => self.index(),
        };
        (!bound.contains_key(&index)).then_some(index)
    }
}

pub(super) fn parse(name: &str) -> Option<VolumeName> {
    split::parse(name).map(|piece| VolumeName { piece })
}
