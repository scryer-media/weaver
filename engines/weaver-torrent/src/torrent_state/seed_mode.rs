// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent added in seed mode (libtorrent `torrent_flags::seed_mode`): its
// files are taken as complete without a check, so it seeds at once, and each
// piece is checked the first time a peer asks for it. A piece that fails
// ends seed mode and the torrent checks every piece again; once every piece
// passed, seed mode ends with nothing more to check (libtorrent
// `torrent::leave_seed_mode`).

use std::collections::{HashMap, hash_map::Entry};
use std::sync::Arc;

use bitvec::vec::BitVec;
use parking_lot::Mutex;
use tokio::sync::broadcast;

use crate::core::lengths::ValidPieceIndex;
use crate::type_aliases::BF;

#[derive(Debug, Default)]
pub(crate) struct SeedMode {
    unchecked: Mutex<Option<Unchecked>>,
    // The pieces a check is reading now, and where each check tells what
    // it found (libtorrent `peer_connection::fill_send_buffer` skips a
    // piece that is `verifying_piece`, peer_connection.cpp:5316, v2.0.15,
    // and every peer's request for it is answered from the one hash job).
    checking: Arc<Mutex<Verdicts>>,
}

type Verdicts = HashMap<u32, broadcast::Sender<bool>>;

// The pieces no peer's request has checked yet, while in seed mode, and
// how many they are.
#[derive(Debug)]
struct Unchecked {
    bits: BitVec,
    left: usize,
}

// Who checks a piece: the one that asked, or a check already running.
pub(crate) enum Claim {
    Check(Checking),
    // What the running check found is sent here: whether the piece is
    // served. A check that ends in an error sends nothing and closes it.
    Wait(broadcast::Receiver<bool>),
}

// The one check of a piece that runs. Dropped, the piece can be claimed
// again.
pub(crate) struct Checking {
    checking: Arc<Mutex<Verdicts>>,
    piece: u32,
    found: broadcast::Sender<bool>,
}

impl Checking {
    // The check ended: those that waited on it hear whether the piece is
    // served.
    pub(crate) fn found(self, served: bool) {
        self.release();
        let _ = self.found.send(served);
    }

    fn release(&self) {
        if let Entry::Occupied(running) = self.checking.lock().entry(self.piece)
            && running.get().same_channel(&self.found)
        {
            running.remove();
        }
    }
}

impl Drop for Checking {
    fn drop(&mut self) {
        self.release();
    }
}

// One read turn of `turns`, the torrent's volume's, waited for on the
// calling thread: for a check of a seed-mode piece that no peer's request
// runs, which is called off the async runtime. The checks of seed-mode
// pieces that read at once, whoever asked for them, are as many as the
// volume has turns, shared with every other read of it.
pub(crate) fn read_turn(
    spawner: &crate::spawn_utils::BlockingSpawner,
    turns: &crate::storage::ReadTurns,
) -> tokio::sync::OwnedSemaphorePermit {
    crate::spawn_utils::no_block_on_utp_thread();
    futures::executor::block_on(turns.owned_turn(spawner.stats()))
}

// The read of a seed-mode check, `read`, of a piece of `bytes`, counted
// once whichever way the check came in, a peer's request or a call of a
// running or a paused torrent: one seed-mode check started for the
// process and one `seed_check` disk call for the session.
pub(crate) fn counted_read<R>(
    spawner: &crate::spawn_utils::BlockingSpawner,
    bytes: u64,
    read: impl FnOnce() -> R,
) -> R {
    crate::session_stats::check::CHECK_STATS.seed_checks_started(1);
    let call = spawner
        .stats()
        .call(crate::session_stats::disk::DiskOp::SeedCheck);
    call.bytes(bytes);
    read()
}

impl SeedMode {
    // Seed mode over `pieces` pieces, those `verified` sets already checked
    // (libtorrent's `verified_pieces` in resume data, restored on a seed-mode
    // add). With every piece checked there is nothing left to check, and seed
    // mode is over.
    pub(crate) fn over(pieces: u32, verified: Option<&BF>) -> Self {
        let mut bits = BitVec::repeat(true, pieces as usize);
        if let Some(verified) = verified {
            for piece in verified
                .iter_ones()
                .take_while(|&piece| piece < pieces as usize)
            {
                bits.set(piece, false);
            }
        }
        let left = bits.count_ones();
        Self {
            unchecked: Mutex::new((left > 0).then_some(Unchecked { bits, left })),
            checking: Arc::default(),
        }
    }

    // The pieces that passed their check, while seed mode lasts.
    pub(crate) fn verified(&self) -> Option<BF> {
        let unchecked = self.unchecked.lock();
        let unchecked = &unchecked.as_ref()?.bits;
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
        self.unchecked.lock().as_ref().is_some_and(|unchecked| {
            unchecked
                .bits
                .get(piece.get() as usize)
                .is_some_and(|bit| *bit)
        })
    }

    // The check of `piece` falls to the caller when none runs, and is
    // waited on when one does: a piece is read and hashed once, however
    // many ask for it meanwhile.
    pub(crate) fn claim(&self, piece: ValidPieceIndex) -> Claim {
        match self.checking.lock().entry(piece.get()) {
            Entry::Occupied(running) => Claim::Wait(running.get().subscribe()),
            Entry::Vacant(none) => {
                let (found, _) = broadcast::channel(1);
                none.insert(found.clone());
                Claim::Check(Checking {
                    checking: Arc::clone(&self.checking),
                    piece: piece.get(),
                    found,
                })
            }
        }
    }

    // Whether a check of `piece` is claimed.
    #[cfg(test)]
    pub(crate) fn claimed(&self, piece: ValidPieceIndex) -> bool {
        self.checking.lock().contains_key(&piece.get())
    }

    // How many wait on the check of `piece` that runs.
    #[cfg(test)]
    pub(crate) fn waiting(&self, piece: ValidPieceIndex) -> usize {
        self.checking
            .lock()
            .get(&piece.get())
            .map_or(0, broadcast::Sender::receiver_count)
    }

    // The piece passed its check. True when it was the last, and seed mode
    // ended with it.
    pub(crate) fn checked(&self, piece: ValidPieceIndex) -> bool {
        let mut unchecked = self.unchecked.lock();
        let Some(Unchecked { bits, left }) = unchecked.as_mut() else {
            return false;
        };
        if bits.replace(piece.get() as usize, false) {
            *left -= 1;
        }
        if *left > 0 {
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

    // Seed mode ends exactly as the last unchecked piece passes, in
    // whatever order they pass, and a piece that passes twice counts once.
    #[test]
    fn seed_mode_ends_exactly_when_the_last_piece_passes() {
        let lengths = Lengths::new(70 * 1024, 1024).unwrap();
        let piece = |index| lengths.validate_piece_index(index).unwrap();
        let seed = SeedMode::over(70, Some(&bits(70, &[0, 64, 69])));
        let order: Vec<u32> = (1..64).rev().chain(65..69).collect();
        let (last, rest) = order.split_last().unwrap();
        for &index in rest {
            assert!(!seed.checked(piece(index)), "{index}");
            assert!(!seed.checked(piece(index)), "{index} again");
            assert!(seed.on());
            assert!(!seed.unchecked(piece(index)));
        }
        assert!(seed.unchecked(piece(*last)));
        assert!(seed.checked(piece(*last)));
        assert!(!seed.on());
        assert!(!seed.checked(piece(*last)));
    }

    // One check of a piece runs at a time: who asks meanwhile waits and
    // hears what it found. Once it ended the piece can be claimed again,
    // and a check dropped with nothing found closes the wait.
    #[test]
    fn a_piece_is_claimed_by_one_check_and_the_rest_hear_what_it_found() {
        let lengths = Lengths::new(8 * 1024, 1024).unwrap();
        let piece = |index| lengths.validate_piece_index(index).unwrap();
        let seed = SeedMode::over(8, None);
        let Claim::Check(first) = seed.claim(piece(2)) else {
            panic!("a check of the piece already runs");
        };
        let Claim::Wait(mut a) = seed.claim(piece(2)) else {
            panic!("the piece was claimed twice");
        };
        let Claim::Wait(mut b) = seed.claim(piece(2)) else {
            panic!("the piece was claimed twice");
        };
        assert_eq!(seed.waiting(piece(2)), 2);
        // Another piece is not held by it.
        let Claim::Check(other) = seed.claim(piece(3)) else {
            panic!("a check of another piece held this one");
        };
        first.found(true);
        assert_eq!(a.blocking_recv().ok(), Some(true));
        assert_eq!(b.blocking_recv().ok(), Some(true));

        let Claim::Check(again) = seed.claim(piece(2)) else {
            panic!("an ended check still holds the piece");
        };
        let Claim::Wait(mut c) = seed.claim(piece(2)) else {
            panic!("the piece was claimed twice");
        };
        drop(again);
        assert!(c.blocking_recv().is_err());
        assert!(matches!(seed.claim(piece(2)), Claim::Check(_)));
        // The other piece's check was not let go of by any of that.
        assert!(matches!(seed.claim(piece(3)), Claim::Wait(_)));
        drop(other);
    }
}
