// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::core::lengths::{Lengths, ValidPieceIndex};
use crate::type_aliases::FileInfos;
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
/// as needed, like the pieces only `Ignored` files hold. A piece a wanted
/// file needs is still `Skip` if set so; one only `Ignored` files hold is not
/// wanted at any priority.
///
/// Each piece takes the level its files give it ([`FilePriority`],
/// [`file_to_piece_priorities`]), then any set piece by piece. `Low` is no
/// file's level: only a piece set to it is `Low`.
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

/// How eagerly a file is fetched: qBittorrent's four download priorities
/// (`downloadpriority.h`: Ignored 0, Normal 1, High 6, Maximum 7). A file at
/// `Ignored` is not wanted; padding is always `Ignored`. Each file's pieces
/// take its level ([`file_to_piece_priorities`]).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FilePriority {
    Ignored,
    #[default]
    Normal,
    High,
    Maximum,
}

impl FilePriority {
    /// The level of the pieces the file alone holds. `Low` is no file's: it
    /// is reached only piece by piece.
    pub const fn piece_priority(self) -> PiecePriority {
        match self {
            Self::Ignored => PiecePriority::Skip,
            Self::Normal => PiecePriority::Normal,
            Self::High => PiecePriority::High,
            Self::Maximum => PiecePriority::Critical,
        }
    }

    /// Whether the file is fetched at all.
    pub const fn wanted(self) -> bool {
        !matches!(self, Self::Ignored)
    }

    /// The level a file holds when asked for this one: padding holds
    /// `Ignored`, whatever is asked (libtorrent `torrent::file_priority`).
    pub const fn held_by(self, padding: bool) -> Self {
        if padding { Self::Ignored } else { self }
    }
}

/// Each piece's priority from the files' levels (libtorrent
/// `file_to_piece_prio`): a piece takes the highest level of the files
/// that share it; padding and empty files hold none, and a piece no wanted
/// file holds is `Skip`. A file past the end of `files` is `Normal`. With
/// `first_last` on, the first and last ceil(1% of the file / piece length)
/// pieces of each wanted file are at the top level and the rest at the
/// file's (qBittorrent `TorrentImpl::applyFirstLastPiecePriority`).
pub(crate) fn file_to_piece_priorities(
    lengths: &Lengths,
    file_infos: &FileInfos,
    files: &[FilePriority],
    first_last: bool,
) -> Vec<PiecePriority> {
    let mut pieces = vec![PiecePriority::Skip; lengths.total_pieces() as usize];
    let edge_span = 100 * u64::from(lengths.default_piece_length());
    for (index, file) in file_infos.iter().enumerate() {
        let level = file_priority(file_infos, files, index);
        if file.len == 0 || !level.wanted() {
            continue;
        }
        let edge = if first_last {
            file.len.div_ceil(edge_span)
        } else {
            0
        };
        let range = file.piece_range.clone();
        for piece in range.clone() {
            let from_start = u64::from(piece - range.start);
            let from_end = u64::from(range.end - 1 - piece);
            let priority = if from_start < edge || from_end < edge {
                PiecePriority::Critical
            } else {
                level.piece_priority()
            };
            if let Some(slot) = pieces.get_mut(piece as usize) {
                *slot = (*slot).max(priority);
            }
        }
    }
    pieces
}

/// The level of file `index`: its own, `Normal` past the end of `files`,
/// and `Ignored` for padding whatever is asked (libtorrent
/// `torrent::file_priority`).
pub(crate) fn file_priority(
    file_infos: &FileInfos,
    files: &[FilePriority],
    index: usize,
) -> FilePriority {
    let padding = file_infos.get(index).is_some_and(|file| file.attrs.padding);
    files
        .get(index)
        .copied()
        .unwrap_or_default()
        .held_by(padding)
}

/// Every file's level, padding at `Ignored`.
pub(crate) fn file_priorities(file_infos: &FileInfos, files: &[FilePriority]) -> Vec<FilePriority> {
    (0..file_infos.len())
        .map(|index| file_priority(file_infos, files, index))
        .collect()
}

/// The files fetched at all, by index: a view of the levels.
pub(crate) fn wanted_files(files: &[FilePriority]) -> Vec<usize> {
    files
        .iter()
        .enumerate()
        .filter(|(_, level)| level.wanted())
        .map(|(index, _)| index)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::constants::CHUNK_SIZE;
    use crate::file_info::FileInfo;

    const PIECE: u64 = CHUNK_SIZE as u64 * 4;

    /// Files of the given lengths laid end to end; `pad` names the padding.
    fn layout(lens: &[u64], pad: &[usize]) -> (Lengths, FileInfos) {
        let total: u64 = lens.iter().sum();
        let lengths = Lengths::new(total, u32::try_from(PIECE).unwrap()).unwrap();
        let mut offset = 0;
        let files = lens
            .iter()
            .enumerate()
            .map(|(index, &len)| {
                let start = u32::try_from(offset / PIECE).unwrap();
                let end = u32::try_from((offset + len).div_ceil(PIECE)).unwrap();
                let file = FileInfo {
                    relative_filename: format!("f{index}").into(),
                    offset_in_torrent: offset,
                    piece_range: start..end.max(start),
                    attrs: crate::core::torrent_metainfo::FileDetailsAttrs {
                        padding: pad.contains(&index),
                        ..Default::default()
                    },
                    len,
                };
                offset += len;
                file
            })
            .collect();
        (lengths, files)
    }

    #[test]
    fn a_piece_two_files_share_takes_the_higher_level() {
        use FilePriority::*;
        use PiecePriority as P;
        // Piece 1 holds the tail of file 0 and the head of file 1.
        let (lengths, files) = layout(&[PIECE * 3 / 2, PIECE * 3 / 2], &[]);
        let pieces =
            |levels: &[FilePriority]| file_to_piece_priorities(&lengths, &files, levels, false);
        assert_eq!(pieces(&[Normal, High]), [P::Normal, P::High, P::High]);
        assert_eq!(
            pieces(&[Maximum, Normal]),
            [P::Critical, P::Critical, P::Normal]
        );
        // An Ignored file gives up the shared piece to the wanted one.
        assert_eq!(pieces(&[Ignored, Normal]), [P::Skip, P::Normal, P::Normal]);
        assert_eq!(pieces(&[High, Ignored]), [P::High, P::High, P::Skip]);
        assert_eq!(pieces(&[Ignored, Ignored]), [P::Skip, P::Skip, P::Skip]);
    }

    #[test]
    fn padding_and_empty_files_are_ignored_whatever_is_asked() {
        use FilePriority::*;
        use PiecePriority as P;
        // File 0 fills half of piece 0, the padding the rest; file 2 is empty
        // at the start of piece 1.
        let (lengths, files) = layout(&[PIECE / 2, PIECE / 2, 0, PIECE], &[1]);
        assert_eq!(
            file_priorities(&files, &[Normal, Maximum, Maximum, Normal]),
            [Normal, Ignored, Maximum, Normal]
        );
        assert_eq!(
            file_to_piece_priorities(
                &lengths,
                &files,
                &[Ignored, Maximum, Maximum, Normal],
                false
            ),
            [P::Skip, P::Normal]
        );
        assert_eq!(wanted_files(&file_priorities(&files, &[Ignored])), [2, 3]);
    }

    #[test]
    fn the_first_and_last_percent_of_each_wanted_file_go_to_the_top() {
        use FilePriority::*;
        use PiecePriority as P;
        // 300 pieces: 1% is 3 pieces at each end.
        let (lengths, files) = layout(&[PIECE * 300], &[]);
        let on = file_to_piece_priorities(&lengths, &files, &[High], true);
        let top: Vec<usize> = (0..on.len()).filter(|&p| on[p] == P::Critical).collect();
        assert_eq!(top, [0, 1, 2, 297, 298, 299]);
        assert!(on[3..297].iter().all(|&p| p == P::High));
        // Off, every piece is back at the file's level.
        let off = file_to_piece_priorities(&lengths, &files, &[High], false);
        assert!(off.iter().all(|&p| p == P::High));
        // An Ignored file has no head or tail.
        let ignored = file_to_piece_priorities(&lengths, &files, &[Ignored], true);
        assert!(ignored.iter().all(|&p| p == P::Skip));
    }
}
