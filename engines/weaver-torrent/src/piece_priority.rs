// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::core::lengths::{Lengths, ValidPieceIndex};
use anyhow::Context;
use serde::{Deserialize, Serialize};

/// How eagerly a piece is fetched.
///
/// Each peer is asked for the highest-priority missing piece it has: pieces of
/// one level are all requested before any piece of a lower level, but only
/// among the pieces that peer can serve, so a piece no peer has never holds
/// the others up. Within a level the rarest piece among the connected peers
/// goes first.
///
/// `Skip` means the piece is not wanted: it is neither requested nor counted
/// as needed, like the pieces of a file left out of `only_files`. A piece a
/// selected file needs is still `Skip` if set so; one only a deselected file
/// needs is not wanted at any priority.
///
/// Every piece starts at `Normal`.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(u8)]
pub enum PiecePriority {
    Skip = 0,
    Low = 1,
    #[default]
    Normal = 2,
    High = 3,
    Critical = 4,
}

impl PiecePriority {
    pub(crate) const COUNT: usize = 5;

    /// The levels that are fetched at all, in the order they are fetched.
    pub(crate) const WANTED_DESCENDING: [PiecePriority; 4] = [
        PiecePriority::Critical,
        PiecePriority::High,
        PiecePriority::Normal,
        PiecePriority::Low,
    ];
}

/// Check every index names a piece of the torrent.
pub(crate) fn validate_piece_priorities(
    lengths: &Lengths,
    priorities: &[(u32, PiecePriority)],
) -> anyhow::Result<Vec<(ValidPieceIndex, PiecePriority)>> {
    priorities
        .iter()
        .map(|(index, priority)| {
            let valid = lengths.validate_piece_index(*index).with_context(|| {
                format!(
                    "piece {index} is out of range: the torrent has {} pieces",
                    lengths.total_pieces()
                )
            })?;
            Ok((valid, *priority))
        })
        .collect()
}
