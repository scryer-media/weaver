// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent added in seed mode (libtorrent `torrent_flags::seed_mode`): its
// files are taken as complete without a check, so it seeds at once, and each
// piece is checked the first time a peer asks for it. A piece that fails
// ends seed mode and the torrent checks every piece again; once every piece
// passed, seed mode ends with nothing more to check (libtorrent
// `torrent::leave_seed_mode`).

use bitvec::vec::BitVec;
use parking_lot::Mutex;

use crate::core::lengths::ValidPieceIndex;
use crate::type_aliases::BF;

#[derive(Debug, Default)]
pub(crate) struct SeedMode {
    // The pieces no peer's request has checked yet, while in seed mode.
    unchecked: Mutex<Option<BitVec>>,
}

impl SeedMode {
    // Seed mode over `pieces` pieces, those `verified` sets already checked
    // (libtorrent's `verified_pieces` in resume data, restored on a seed-mode
    // add). With every piece checked there is nothing left to check, and seed
    // mode is over.
    pub(crate) fn over(pieces: u32, verified: Option<&BF>) -> Self {
        let mut unchecked = BitVec::repeat(true, pieces as usize);
        if let Some(verified) = verified {
            for piece in verified
                .iter_ones()
                .take_while(|&piece| piece < pieces as usize)
            {
                unchecked.set(piece, false);
            }
        }
        Self {
            unchecked: Mutex::new(unchecked.any().then_some(unchecked)),
        }
    }

    // The pieces that passed their check, while seed mode lasts.
    pub(crate) fn verified(&self) -> Option<BF> {
        let unchecked = self.unchecked.lock();
        let unchecked = unchecked.as_ref()?;
        let mut verified =
            BF::from_boxed_slice(vec![0u8; unchecked.len().div_ceil(8)].into_boxed_slice());
        for piece in unchecked.iter_zeros() {
            verified.set(piece, true);
        }
        Some(verified)
    }

    pub(crate) fn on(&self) -> bool {
        self.unchecked.lock().is_some()
    }

    // Whether a peer asking for `piece` must wait for its check.
    pub(crate) fn unchecked(&self, piece: ValidPieceIndex) -> bool {
        self.unchecked
            .lock()
            .as_ref()
            .is_some_and(|unchecked| unchecked.get(piece.get() as usize).is_some_and(|bit| *bit))
    }

    // The piece passed its check. True when it was the last, and seed mode
    // ended with it.
    pub(crate) fn checked(&self, piece: ValidPieceIndex) -> bool {
        let mut unchecked = self.unchecked.lock();
        let Some(bits) = unchecked.as_mut() else {
            return false;
        };
        bits.set(piece.get() as usize, false);
        if bits.any() {
            return false;
        }
        *unchecked = None;
        true
    }

    // Seed mode ends. True when it was on.
    pub(crate) fn leave(&self) -> bool {
        self.unchecked.lock().take().is_some()
    }

    // `f` while seed mode lasts, which it lasts through; None once it
    // ended. Every request of a peer waits on `f`, so it does no disk work.
    pub(crate) fn while_on<T>(&self, f: impl FnOnce() -> T) -> Option<T> {
        let unchecked = self.unchecked.lock();
        unchecked.is_some().then(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::lengths::Lengths;

    fn bits(pieces: usize, set: &[usize]) -> BF {
        let mut bits = BF::from_boxed_slice(vec![0u8; pieces.div_ceil(8)].into_boxed_slice());
        for &piece in set {
            bits.set(piece, true);
        }
        bits
    }

    // The pieces that passed before a restart are served without a second
    // check, as libtorrent's restored `verified_pieces` are; the rest wait
    // for theirs, and the set given back is the one restored and those since.
    #[test]
    fn pieces_that_passed_before_are_not_checked_again() {
        let lengths = Lengths::new(8 * 1024, 1024).unwrap();
        let piece = |index| lengths.validate_piece_index(index).unwrap();
        let seed = SeedMode::over(8, Some(&bits(8, &[0, 3])));
        assert!(seed.on());
        assert!(!seed.unchecked(piece(0)));
        assert!(!seed.unchecked(piece(3)));
        assert!(seed.unchecked(piece(1)));
        assert_eq!(seed.verified(), Some(bits(8, &[0, 3])));

        assert!(!seed.checked(piece(1)));
        assert_eq!(seed.verified(), Some(bits(8, &[0, 1, 3])));
        assert!(seed.leave());
        assert_eq!(seed.verified(), None);
    }

    // A set naming every piece leaves nothing to check: seed mode is over
    // from the start, as it ends once the last piece passed.
    #[test]
    fn a_set_naming_every_piece_leaves_seed_mode_over() {
        let seed = SeedMode::over(9, Some(&bits(9, &(0..9).collect::<Vec<_>>())));
        assert!(!seed.on());
        assert_eq!(seed.verified(), None);
        let fresh = SeedMode::over(9, None);
        assert_eq!(fresh.verified(), Some(bits(9, &[])));
    }
}
