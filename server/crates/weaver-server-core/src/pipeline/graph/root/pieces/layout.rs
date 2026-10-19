// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's files laid end to end over its pieces. Piece P covers the
// torrent's bytes `[P * piece_length, (P + 1) * piece_length)`, the last one
// cut at the end. A file covers its own run of those bytes, so a range of a
// file falls on a run of pieces, and a piece falls on the files it crosses.
// A padding file only fills a piece out to a boundary: it is laid out like
// any other, but no volume is made of it and none of its bytes is fetched.

use std::ops::Range;

use weaver_graph::{ByteRange, VolumeIndex};

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
    volume: Option<VolumeIndex>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PieceLayout {
    piece_length: u64,
    total: u64,
    files: Vec<Placed>,
    // Volume N is the file at `volumes[N]`.
    volumes: Vec<usize>,
}

// One file's share of a piece.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Span {
    // The file's index in the torrent.
    pub(crate) file: usize,
    // `None` for a padding file.
    pub(crate) volume: Option<VolumeIndex>,
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
        let mut volumes = Vec::new();
        let mut placed = Vec::with_capacity(files.len());
        for (index, file) in files.into_iter().enumerate() {
            let volume = (!file.padding).then(|| {
                volumes.push(index);
                VolumeIndex(volumes.len() as u32 - 1)
            });
            let length = file.length;
            placed.push(Placed {
                file,
                start,
                volume,
            });
            start = start.checked_add(length).ok_or(LayoutError::TooLong)?;
        }
        if u32::try_from(start.div_ceil(piece_length)).is_err() {
            return Err(LayoutError::TooManyPieces);
        }
        Ok(Self {
            piece_length,
            total: start,
            files: placed,
            volumes,
        })
    }

    pub(crate) fn piece_length(&self) -> u64 {
        self.piece_length
    }

    pub(crate) fn total(&self) -> u64 {
        self.total
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

    pub(crate) fn volume_count(&self) -> usize {
        self.volumes.len()
    }

    // The torrent's index of the volume's file.
    pub(crate) fn volume_file(&self, volume: VolumeIndex) -> usize {
        self.volumes[volume.0 as usize]
    }

    pub(crate) fn file_volume(&self, file: usize) -> Option<VolumeIndex> {
        self.files.get(file)?.volume
    }

    pub(crate) fn volume(&self, volume: VolumeIndex) -> Option<&PieceFile> {
        let file = *self.volumes.get(volume.0 as usize)?;
        Some(&self.files[file].file)
    }

    // The pieces carrying any byte of the volume's range, cut at the
    // volume's end.
    pub(crate) fn pieces(&self, volume: VolumeIndex, range: ByteRange) -> Range<u32> {
        let Some(placed) = self
            .volumes
            .get(volume.0 as usize)
            .map(|file| &self.files[*file])
        else {
            return 0..0;
        };
        let end = range.end.min(placed.file.length);
        let start = range.start.min(end);
        if start == end {
            return 0..0;
        }
        let first = (placed.start + start) / self.piece_length;
        let last = (placed.start + end - 1) / self.piece_length;
        first as u32..last as u32 + 1
    }

    // Every piece carrying the volume's bytes; none for an empty file.
    pub(crate) fn volume_pieces(&self, volume: VolumeIndex) -> Range<u32> {
        self.pieces(volume, ByteRange::new(0, u64::MAX))
    }

    // The volume's share of the piece, in the volume's coordinates.
    pub(crate) fn span(&self, volume: VolumeIndex, piece: u32) -> Option<ByteRange> {
        let file = *self.volumes.get(volume.0 as usize)?;
        let placed = &self.files[file];
        let piece = self.piece_range(piece);
        let own = ByteRange::at(placed.start, placed.file.length);
        Some(own.intersect(piece)?.shift(-(placed.start as i64)))
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
                    volume: placed.volume,
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
