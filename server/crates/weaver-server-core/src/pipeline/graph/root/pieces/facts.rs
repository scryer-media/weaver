// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The piece root's facts: the torrent's piece length and files, to prove a
// restore is over the same torrent, where each unit stands with the hash of
// what was handed over, and the waiting pieces raised above the ordinary
// class, since a repair's ask is made once.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{Class, PieceLayout, PieceRoot, Unit};
use crate::pipeline::graph::root::facts::RootFactsError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PieceFacts {
    pub(crate) piece_length: u64,
    // Each file's name, length and whether it is padding, in torrent order.
    pub(crate) files: Vec<(String, u64, bool)>,
    // Per volume, one per piece carrying its bytes.
    pub(crate) units: Vec<Vec<Unit>>,
    pub(crate) raised: Vec<(u32, Class)>,
}

impl PieceFacts {
    pub(crate) const VERSION: u16 = 1;
}

fn files(layout: &PieceLayout) -> Vec<(String, u64, bool)> {
    layout
        .files()
        .map(|file| (file.name.clone(), file.length, file.padding))
        .collect()
}

impl PieceRoot {
    pub(crate) fn facts(&self) -> PieceFacts {
        let raised = self
            .classes
            .iter()
            .enumerate()
            .filter(|(_, class)| **class < Class::Ordinary)
            .map(|(piece, class)| (piece as u32, *class))
            .filter(|(piece, _)| {
                self.layout.spans(*piece).iter().any(|span| {
                    span.volume
                        .is_some_and(|volume| self.waiting(volume, *piece))
                })
            })
            .collect();
        PieceFacts {
            piece_length: self.layout.piece_length(),
            files: files(&self.layout),
            units: self.units.clone(),
            raised,
        }
    }

    // The root at the state its facts describe, over the torrent's layout,
    // which must be the one they were taken from.
    pub(crate) fn restore(
        layout: Arc<PieceLayout>,
        facts: &PieceFacts,
    ) -> Result<Self, RootFactsError> {
        if layout.piece_length() != facts.piece_length {
            return Err(RootFactsError::Mismatch("another piece length"));
        }
        if files(&layout) != facts.files {
            return Err(RootFactsError::Mismatch("another file"));
        }
        let mut root = Self::new(layout);
        let shaped = root.units.len() == facts.units.len()
            && root
                .units
                .iter()
                .zip(&facts.units)
                .all(|(ours, theirs)| ours.len() == theirs.len());
        if !shaped {
            return Err(RootFactsError::Mismatch("another number of pieces"));
        }
        root.units = facts.units.clone();
        // Raised again through the moves, so the fetch is steered as the
        // one that went down was.
        for (piece, class) in &facts.raised {
            let Some(found) = root.classes.get_mut(*piece as usize) else {
                return Err(RootFactsError::Mismatch(
                    "a raised piece not in the torrent",
                ));
            };
            *found = *class;
            root.moves.push(super::Move {
                piece: *piece,
                class: *class,
            });
        }
        Ok(root)
    }
}
