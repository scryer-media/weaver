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

#[derive(Debug, Default)]
pub(crate) struct SeedMode {
    // The pieces no peer's request has checked yet, while in seed mode.
    unchecked: Mutex<Option<BitVec>>,
}

impl SeedMode {
    // Seed mode over `pieces` pieces, none checked.
    pub(crate) fn over(pieces: u32) -> Self {
        Self {
            unchecked: Mutex::new(Some(BitVec::repeat(true, pieces as usize))),
        }
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
