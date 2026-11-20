// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::layout::PieceLayout;
use bitvec::order::Msb0;
use bitvec::vec::BitVec;

// The pieces as they are kept between runs: one bit per piece had, most
// significant first, and whether a check of every piece had yet to find
// some, in which case the next run checks every piece again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kept {
    pub bits: Vec<u8>,
    pub checking: bool,
}

impl Kept {
    // How many pieces it has, and each file's bytes in them.
    pub fn had(&self, layout: &PieceLayout) -> (u32, Vec<u64>) {
        let have = Have::kept(layout, Some(self));
        (have.count, have.file_have)
    }
}

// Whether `file_have`, each file's bytes in the pieces had, holds every file
// `wanted` names whole. Padding is on no disk and is never wanted; a file
// past the end of `file_have` has none of its bytes.
pub fn has_files(layout: &PieceLayout, file_have: &[u64], wanted: impl Fn(usize) -> bool) -> bool {
    layout
        .files()
        .enumerate()
        .filter(|(index, file)| !file.padding && wanted(*index))
        .all(|(index, file)| file_have.get(index).copied().unwrap_or(0) >= file.length)
}

pub struct Have {
    pub pieces: BitVec<u8, Msb0>,
    // The pieces a check of every piece has neither found nor failed yet:
    // every piece when it begins, none once it is over. None of them is had
    // until it finds it.
    pub unchecked: BitVec<u8, Msb0>,
    // Each file's bytes in the pieces had.
    pub file_have: Vec<u64>,
    pub count: u32,
    // A move of placed files is between the write that set its row aside
    // and the write that ends it (`TorrentStore::mid_move`): the pieces
    // kept meanwhile are to be checked, as that row says.
    pub mid_move: bool,
    // The files written since their time was last taken for the database,
    // one bit per file: the next checkpoint takes it again.
    pub stale: BitVec<u8, Msb0>,
    // The pieces kept from the last run in a file whose time changed since
    // then: had no longer, and checked again as the session starts, each
    // had again once the check finds it. Kept for the next run until then.
    pub recheck: BitVec<u8, Msb0>,
    // Each file's pieces in `recheck`: its time is not taken while any is.
    pub rechecking: Vec<u32>,
}

impl Have {
    fn new(layout: &PieceLayout) -> Self {
        let count = layout.piece_count() as usize;
        Self {
            pieces: BitVec::repeat(false, count),
            unchecked: BitVec::repeat(false, count),
            file_have: vec![0; layout.file_count()],
            count: 0,
            mid_move: false,
            stale: BitVec::repeat(false, layout.file_count()),
            recheck: BitVec::repeat(false, count),
            rechecking: vec![0; layout.file_count()],
        }
    }

    pub fn kept(layout: &PieceLayout, kept: Option<&Kept>) -> Self {
        let mut have = Self::new(layout);
        if let Some(kept) = kept {
            let bits = BitVec::<u8, Msb0>::from_slice(&kept.bits);
            let count = have.pieces.len();
            for piece in bits.iter_ones().take_while(|piece| *piece < count) {
                have.set(layout, piece as u32, true);
            }
            // A check that was not over checks every piece again.
            if kept.checking {
                have.unchecked.fill(true);
            }
        }
        // Read back, not written: their times are as the database has them.
        have.stale.fill(false);
        have
    }

    // Had now; true when it was not before. A piece come to be had was
    // written, or found, in its files: their times are taken again.
    pub fn set(&mut self, layout: &PieceLayout, piece: u32, had: bool) -> bool {
        let Some(mut bit) = self.pieces.get_mut(piece as usize) else {
            return false;
        };
        if *bit == had {
            return false;
        }
        bit.set(had);
        drop(bit);
        for span in layout.spans(piece) {
            let have = &mut self.file_have[span.file];
            if had {
                *have += span.range.len();
                self.stale.set(span.file, true);
            } else {
                *have -= span.range.len();
            }
        }
        if had {
            self.count += 1;
        } else {
            self.count -= 1;
        }
        true
    }

    // The piece, kept from the last run, is checked again before it is had:
    // had no longer, and in `recheck` until the check says.
    pub fn hold_for_check(&mut self, layout: &PieceLayout, piece: u32) -> bool {
        if !self.set(layout, piece, false) {
            return false;
        }
        self.recheck.set(piece as usize, true);
        for span in layout.spans(piece) {
            self.rechecking[span.file] += 1;
        }
        true
    }

    // The check of a piece in `recheck` said, whatever it said: its files'
    // times are taken again, now that they vouch for what they hold.
    pub fn rechecked(&mut self, layout: &PieceLayout, piece: u32) {
        let Some(mut bit) = self.recheck.get_mut(piece as usize) else {
            return;
        };
        if !*bit {
            return;
        }
        bit.set(false);
        drop(bit);
        for span in layout.spans(piece) {
            self.rechecking[span.file] -= 1;
            self.stale.set(span.file, true);
        }
    }

    // The files were written outside a piece coming to be had: their times
    // are taken again.
    pub fn wrote(&mut self, files: impl IntoIterator<Item = usize>) {
        for file in files {
            if let Some(mut bit) = self.stale.get_mut(file) {
                bit.set(true);
            }
        }
    }

    // Every file it has bytes of: their times are taken again.
    pub fn all_written(&mut self) {
        let held: Vec<usize> = (0..self.file_have.len())
            .filter(|file| self.file_have[*file] > 0)
            .collect();
        self.wrote(held);
    }

    // No piece waits on a check of its own: a check of every piece begins.
    pub fn recheck_none(&mut self) {
        self.recheck.fill(false);
        self.rechecking.fill(0);
    }
}
