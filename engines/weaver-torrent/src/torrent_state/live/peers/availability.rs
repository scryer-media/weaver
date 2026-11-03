// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! How many connected peers have each piece.

use std::sync::atomic::AtomicU32;

use parking_lot::Mutex;

use crate::{
    chunk_tracker::Moved,
    core::lengths::ValidPieceIndex,
    torrent_state::utils::{atomic_dec, atomic_inc},
    type_aliases::BS,
};

/// One count per piece: the number of live peers whose bitfield holds it. A
/// peer's bitfield is added when it is counted in and taken away once when it
/// is counted out, so a count never outlives the connection that made it.
///
/// Each change is also logged for the piece picker, which moves the pieces
/// it names in its order at its next pick (libtorrent `inc_refcount` and
/// `dec_refcount`). The log lock is a leaf: it is taken under a peer's entry
/// and under the torrent's state lock, and nothing is taken under it.
pub(crate) struct Availability {
    counts: Box<[AtomicU32]>,
    moved: Mutex<Moved>,
}

impl Availability {
    pub fn new(total_pieces: u32) -> Self {
        Self {
            counts: (0..total_pieces).map(|_| AtomicU32::new(0)).collect(),
            moved: Mutex::default(),
        }
    }

    pub fn get(&self, piece: ValidPieceIndex) -> u32 {
        self.counts[piece.get_usize()].load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Every piece's count, in piece order.
    #[cfg(test)]
    pub fn counts(&self) -> Vec<u32> {
        self.counts
            .iter()
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .collect()
    }

    /// The pieces whose count moved since the last call.
    pub fn take_moved(&self) -> Moved {
        std::mem::take(&mut *self.moved.lock())
    }

    pub(super) fn peer_has(&self, piece: usize) {
        atomic_inc(&self.counts[piece]);
        self.log(1, std::iter::once(piece));
    }

    pub(super) fn peer_lost(&self, piece: usize) {
        atomic_dec(&self.counts[piece]);
        self.log(1, std::iter::once(piece));
    }

    pub(super) fn add(&self, bitfield: &BS) {
        for piece in bitfield.iter_ones() {
            atomic_inc(&self.counts[piece]);
        }
        self.log(bitfield.count_ones(), bitfield.iter_ones());
    }

    pub(super) fn remove(&self, bitfield: &BS) {
        for piece in bitfield.iter_ones() {
            atomic_dec(&self.counts[piece]);
        }
        self.log(bitfield.count_ones(), bitfield.iter_ones());
    }

    // Logs the `count` pieces `pieces` names, after their counts moved, so a
    // pick that takes the log reads the moved counts. A log that would name
    // more than half the torrent's pieces names them all instead: the picker
    // lays its order out anew, which costs no more than moving that many
    // pieces one by one (libtorrent's bitfield `inc_refcount` marks the
    // picker dirty past `min(50, num_pieces / 2)` pieces; weaver's log
    // gathers changes across messages until a pick takes it, so the bound is
    // on the log, and half the pieces bounds it whether or not a pick ever
    // comes).
    fn log(&self, count: usize, pieces: impl Iterator<Item = usize>) {
        let mut moved = self.moved.lock();
        if moved.all {
            return;
        }
        if moved.pieces.len() + count > self.counts.len() / 2 {
            *moved = Moved {
                pieces: Vec::new(),
                all: true,
            };
            return;
        }
        moved
            .pieces
            .extend(pieces.map(|piece| u32::try_from(piece).expect("piece indexes fit in u32")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_aliases::BF;

    fn bitfield(total: u32, pieces: &[usize]) -> BF {
        let mut bf = BF::from_boxed_slice(vec![0u8; total.div_ceil(8) as usize].into_boxed_slice());
        for piece in pieces {
            bf.set(*piece, true);
        }
        bf
    }

    #[test]
    fn each_count_that_moves_is_logged_until_the_log_would_name_half_the_pieces() {
        let availability = Availability::new(8);
        let moved = |pieces: &[u32]| Moved {
            pieces: pieces.to_vec(),
            all: false,
        };
        assert_eq!(availability.take_moved(), Moved::default());

        // A HAVE and a small bitfield, counted in and out: each piece logged
        // after its count moved; taking the log empties it.
        availability.peer_has(5);
        availability.add(&bitfield(8, &[1, 2]));
        assert_eq!(availability.take_moved(), moved(&[5, 1, 2]));
        availability.remove(&bitfield(8, &[1, 2]));
        assert_eq!(availability.take_moved(), moved(&[1, 2]));
        assert_eq!(availability.counts(), [0, 0, 0, 0, 0, 1, 0, 0]);

        // Past half the pieces the log names them all, and stays so until
        // taken.
        availability.add(&bitfield(8, &[0, 1, 2]));
        availability.peer_has(3);
        availability.peer_has(4);
        let all = Moved {
            pieces: Vec::new(),
            all: true,
        };
        assert_eq!(availability.take_moved(), all);
        availability.add(&bitfield(8, &[0, 1, 2, 3, 4, 5, 6, 7]));
        availability.peer_has(6);
        assert_eq!(availability.take_moved(), all);
        assert_eq!(availability.take_moved(), Moved::default());
    }
}
