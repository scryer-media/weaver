// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's files laid end to end over its pieces. Piece P covers the
// torrent's bytes `[P * piece_length, (P + 1) * piece_length)`, the last one
// cut at the end. A file covers its own run of those bytes, so a range of a
// file falls on a run of pieces, and a piece falls on the files it crosses.
// A padding file only fills a piece out to a boundary: it is laid out like
// any other, but nothing of it is written or read from disk.

use std::ops::Range;

use weaver_graph::ByteRange;
use weaver_torrent::ValidatedTorrentMetaV1Info;

// A file as the metainfo lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PieceFile {
    // Its path inside the torrent, components joined by `/`.
    pub(crate) name: String,
    pub(crate) length: u64,
    pub(crate) padding: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Placed {
    file: PieceFile,
    // Where its bytes start in the torrent.
    start: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PieceLayout {
    piece_length: u64,
    total: u64,
    files: Vec<Placed>,
}

// One file's share of a piece.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Span {
    // The file's index in the torrent.
    pub(crate) file: usize,
    // Where the share starts inside the piece.
    pub(crate) at: u64,
    // The share, in the file's coordinates.
    pub(crate) range: ByteRange,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LayoutError {
    #[error("a torrent's pieces must have a length")]
    NoPieceLength,
    #[error("the torrent has more pieces than a piece index can number")]
    TooManyPieces,
    #[error("the torrent's files add up past the end of the address space")]
    TooLong,
}

impl PieceLayout {
    pub(crate) fn new(piece_length: u64, files: Vec<PieceFile>) -> Result<Self, LayoutError> {
        if piece_length == 0 {
            return Err(LayoutError::NoPieceLength);
        }
        let mut start = 0u64;
        let mut placed = Vec::with_capacity(files.len());
        for file in files {
            let length = file.length;
            placed.push(Placed { file, start });
            start = start.checked_add(length).ok_or(LayoutError::TooLong)?;
        }
        if u32::try_from(start.div_ceil(piece_length)).is_err() {
            return Err(LayoutError::TooManyPieces);
        }
        Ok(Self {
            piece_length,
            total: start,
            files: placed,
        })
    }

    pub(crate) fn piece_length(&self) -> u64 {
        self.piece_length
    }

    pub(crate) fn piece_count(&self) -> u32 {
        self.total.div_ceil(self.piece_length) as u32
    }

    // The piece's bytes in the torrent's coordinates.
    pub(crate) fn piece_range(&self, piece: u32) -> ByteRange {
        let start = (u64::from(piece) * self.piece_length).min(self.total);
        ByteRange::new(start, (start + self.piece_length).min(self.total))
    }

    pub(crate) fn files(&self) -> impl Iterator<Item = &PieceFile> + '_ {
        self.files.iter().map(|placed| &placed.file)
    }

    pub(crate) fn file(&self, file: usize) -> Option<&PieceFile> {
        self.files.get(file).map(|placed| &placed.file)
    }

    pub(crate) fn file_count(&self) -> usize {
        self.files.len()
    }

    // Every piece carrying a byte of the file; none for an empty file.
    pub(crate) fn file_pieces(&self, file: usize) -> Range<u32> {
        let Some(placed) = self.files.get(file) else {
            return 0..0;
        };
        if placed.file.length == 0 {
            return 0..0;
        }
        let first = placed.start / self.piece_length;
        let last = (placed.start + placed.file.length - 1) / self.piece_length;
        first as u32..last as u32 + 1
    }

    // The file's share of the piece, in the file's coordinates.
    pub(crate) fn share(&self, file: usize, piece: u32) -> Option<ByteRange> {
        let placed = self.files.get(file)?;
        let own = ByteRange::at(placed.start, placed.file.length);
        Some(
            own.intersect(self.piece_range(piece))?
                .shift(-(placed.start as i64)),
        )
    }

    // Every file's share of the piece, in torrent order.
    pub(crate) fn spans(&self, piece: u32) -> Vec<Span> {
        let range = self.piece_range(piece);
        let from = self
            .files
            .partition_point(|placed| placed.start + placed.file.length <= range.start);
        self.files[from..]
            .iter()
            .enumerate()
            .take_while(|(_, placed)| placed.start < range.end)
            .filter_map(|(offset, placed)| {
                let own = ByteRange::at(placed.start, placed.file.length);
                let share = own.intersect(range)?;
                Some(Span {
                    file: from + offset,
                    at: share.start - range.start,
                    range: share.shift(-(placed.start as i64)),
                })
            })
            .collect()
    }

    // Where a file's byte lies in the torrent.
    pub(crate) fn torrent_offset(&self, file: usize, offset: u64) -> Option<u64> {
        Some(self.files.get(file)?.start + offset)
    }
}

// The layout of a torrent's files over its pieces, from its metainfo.
pub(crate) fn layout_of<B: AsRef<[u8]>>(
    info: &ValidatedTorrentMetaV1Info<B>,
) -> Result<PieceLayout, LayoutError> {
    let files = info
        .iter_file_details()
        .map(|details| PieceFile {
            name: details.filename.to_vec().join("/"),
            length: details.len,
            padding: details.attrs().padding,
        })
        .collect();
    PieceLayout::new(u64::from(info.lengths().default_piece_length()), files)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, length: u64) -> PieceFile {
        PieceFile {
            name: name.to_owned(),
            length,
            padding: false,
        }
    }

    fn layout() -> PieceLayout {
        PieceLayout::new(
            1024,
            vec![file("a.bin", 1000), file("b.bin", 2500), file("c.bin", 700)],
        )
        .unwrap()
    }

    // A file's range maps onto the pieces it crosses, a piece onto every
    // file's share of it in that file's own coordinates.
    #[test]
    fn a_files_range_maps_onto_the_pieces_across_file_boundaries() {
        let layout = layout();
        assert_eq!(layout.piece_count(), 5);
        assert_eq!(layout.file_pieces(0), 0..1);
        assert_eq!(layout.file_pieces(1), 0..4);
        assert_eq!(layout.file_pieces(2), 3..5);
        assert_eq!(
            layout.spans(0),
            [
                Span {
                    file: 0,
                    at: 0,
                    range: ByteRange::new(0, 1000),
                },
                Span {
                    file: 1,
                    at: 1000,
                    range: ByteRange::new(0, 24),
                },
            ]
        );
        assert_eq!(
            layout.spans(3),
            [
                Span {
                    file: 1,
                    at: 0,
                    range: ByteRange::new(2072, 2500),
                },
                Span {
                    file: 2,
                    at: 428,
                    range: ByteRange::new(0, 596),
                },
            ]
        );
        assert_eq!(layout.share(2, 4), Some(ByteRange::new(596, 700)));
        assert_eq!(layout.share(0, 1), None);
        assert_eq!(layout.piece_range(4), ByteRange::new(4096, 4200));
        assert_eq!(layout.torrent_offset(2, 10), Some(3510));
    }

    // A padding file fills a piece out: it is laid out as any other file,
    // and an empty file carries no piece.
    #[test]
    fn padding_is_laid_out_and_an_empty_file_has_no_piece() {
        let layout = PieceLayout::new(
            1024,
            vec![
                file("a.bin", 1000),
                PieceFile {
                    name: ".pad/24".to_owned(),
                    length: 24,
                    padding: true,
                },
                file("empty", 0),
                file("b.bin", 100),
            ],
        )
        .unwrap();
        assert!(layout.file_pieces(2).is_empty());
        assert_eq!(layout.file_pieces(3), 1..2);
        let spans = layout.spans(0);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[1].file, 1);
        assert_eq!(spans[1].range, ByteRange::new(0, 24));
    }

    #[test]
    fn a_layout_refuses_no_piece_length() {
        assert_eq!(
            PieceLayout::new(0, vec![file("a", 1)]),
            Err(LayoutError::NoPieceLength)
        );
    }
}
