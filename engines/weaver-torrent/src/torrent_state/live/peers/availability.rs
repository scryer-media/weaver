// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! How many connected peers have each piece.

use std::sync::atomic::{AtomicU32, Ordering};

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
/// A bitfield that holds every piece is counted once, in `seeds`, not piece
/// by piece: a seed joins and leaves at the cost of one count, and moves no
/// piece in the picker's order (libtorrent `inc_refcount_all`,
/// `dec_refcount_all`, `break_one_seed`).
///
/// Each change is also logged for the piece picker, which moves the pieces
/// it names in its order at its next pick (libtorrent `inc_refcount` and
/// `dec_refcount`). The log lock is a leaf: it is taken under a peer's entry
/// and under the torrent's state lock, and nothing is taken under it.
pub(crate) struct Availability {
    counts: Box<[AtomicU32]>,
    seeds: AtomicU32,
    moved: Mutex<Moved>,
}

impl Availability {
    pub fn new(total_pieces: u32) -> Self {
        Self {
            counts: (0..total_pieces).map(|_| AtomicU32::new(0)).collect(),
            seeds: AtomicU32::new(0),
            moved: Mutex::default(),
        }
    }

    /// How many live peers have `piece`, seeds among them.
    pub fn get(&self, piece: ValidPieceIndex) -> u32 {
        self.counted(piece) + self.seeds()
    }

    /// The count the picker orders `piece` by: the peers that are no seed
    /// and have it, and one for any seeds. More seeds add the same to every
    /// piece and leave the order as it is (libtorrent `piece_pos::priority`
    /// ranks by `peer_count + 1` and drops a piece only when `peer_count +
    /// m_seeds` is 0).
    pub fn ranked(&self, piece: ValidPieceIndex) -> u32 {
        self.counted(piece) + u32::from(self.seeds() > 0)
    }

    fn counted(&self, piece: ValidPieceIndex) -> u32 {
        self.counts[piece.get_usize()].load(Ordering::Relaxed)
    }

    fn seeds(&self) -> u32 {
        self.seeds.load(Ordering::Relaxed)
    }

    /// Every piece's count, seeds among them, in piece order.
    #[cfg(test)]
    pub fn counts(&self) -> Vec<u32> {
        let seeds = self.seeds();
        self.counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed) + seeds)
            .collect()
    }

    /// The pieces whose count moved since the last call.
    pub fn take_moved(&self) -> Moved {
        std::mem::take(&mut *self.moved.lock())
    }

    /// A peer gained `piece`. `whole` is its bitfield when that now holds
    /// every piece: the pieces it had leave the counts, and it counts as a
    /// seed instead.
    pub(super) fn peer_has(&self, piece: usize, whole: Option<&BS>) {
        match whole {
            None => {
                atomic_inc(&self.counts[piece]);
                self.log(1, std::iter::once(piece));
            }
            Some(bitfield) => {
                let others = || bitfield.iter_ones().filter(move |p| *p != piece);
                for p in others() {
                    atomic_dec(&self.counts[p]);
                }
                self.seed_joins();
                self.log(bitfield.count_ones() - 1, others());
            }
        }
    }

    /// A peer lost `piece`. `was_whole` is its bitfield, `piece` gone, when
    /// that held every piece before: the peer is a seed no longer, and the
    /// pieces it keeps are counted one by one.
    pub(super) fn peer_lost(&self, piece: usize, was_whole: Option<&BS>) {
        match was_whole {
            None => {
                atomic_dec(&self.counts[piece]);
                self.log(1, std::iter::once(piece));
            }
            Some(bitfield) => {
                self.seed_leaves();
                self.count(bitfield, atomic_inc);
            }
        }
    }

    pub(super) fn add(&self, bitfield: &BS) {
        if self.whole(bitfield) {
            self.seed_joins();
        } else {
            self.count(bitfield, atomic_inc);
        }
    }

    pub(super) fn remove(&self, bitfield: &BS) {
        if self.whole(bitfield) {
            self.seed_leaves();
        } else {
            self.count(bitfield, atomic_dec);
        }
    }

    // The first seed adds one to every piece's rank and the last takes it
    // away: the picker lays its order out anew (libtorrent `m_dirty` when
    // `m_seeds` moves between 0 and 1).
    fn seed_joins(&self) {
        if atomic_inc(&self.seeds) == 0 {
            self.log_all();
        }
    }

    fn seed_leaves(&self) {
        if atomic_dec(&self.seeds) == 1 {
            self.log_all();
        }
    }

    fn log_all(&self) {
        *self.moved.lock() = Moved {
            pieces: Vec::new(),
            all: true,
        };
    }

    // Whether `bitfield` holds every piece (`LivePeerState::has_full_torrent`).
    fn whole(&self, bitfield: &BS) -> bool {
        bitfield.get(..self.counts.len()).is_some_and(BS::all)
    }

    // Moves the count of each piece `bitfield` holds by `step`, and logs them.
    fn count(&self, bitfield: &BS, step: fn(&AtomicU32) -> u32) {
        for piece in bitfield.iter_ones() {
            step(&self.counts[piece]);
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
        availability.peer_has(5, None);
        availability.add(&bitfield(8, &[1, 2]));
        assert_eq!(availability.take_moved(), moved(&[5, 1, 2]));
        availability.remove(&bitfield(8, &[1, 2]));
        assert_eq!(availability.take_moved(), moved(&[1, 2]));
        assert_eq!(availability.counts(), [0, 0, 0, 0, 0, 1, 0, 0]);

        // Past half the pieces the log names them all, and stays so until
        // taken.
        availability.add(&bitfield(8, &[0, 1, 2]));
        availability.peer_has(3, None);
        availability.peer_has(4, None);
        let all = Moved {
            pieces: Vec::new(),
            all: true,
        };
        assert_eq!(availability.take_moved(), all);
        availability.add(&bitfield(8, &[0, 1, 2, 3, 4, 5, 6, 7]));
        availability.peer_has(6, None);
        assert_eq!(availability.take_moved(), all);
        assert_eq!(availability.take_moved(), Moved::default());
    }
}
