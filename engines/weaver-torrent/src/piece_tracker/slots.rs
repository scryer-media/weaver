// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a piece in flight holds of its blocks, under the piece's own lock,
//! so a block of it is admitted and marked without the torrent's lock.
//!
//! A piece holds a slot from the grant that puts it in flight until it
//! leaves flight, and none otherwise. A slot is made and taken away only
//! under the torrent's write lock and the piece's lock both, and so are its
//! owner and the piece's grant: under either lock they hold still. The
//! blocks being written, the blocks doubled, the room taken and the senders
//! change under the piece's lock alone, as do the piece's received bits
//! while it holds a slot.
//!
//! Locks are taken in one order: a peer's shard, the torrent's, a piece's,
//! then the storage's room, a leaf. A thread holds a second piece's lock
//! only under the torrent's write lock, so two piece locks are never waited
//! for in opposite orders: every other holder of a piece lock holds that one
//! alone and waits for nothing but the storage's room. Debug and test
//! builds check both rules on every take.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

use crate::chunk_tracker::Received;
use crate::core::lengths::{ChunkInfo, Lengths, ValidPieceIndex};
use crate::type_aliases::PeerHandle;

use super::{Arrival, Senders};

/// What a piece in flight holds of its blocks.
#[derive(Debug)]
pub(crate) struct PieceSlot {
    // The peer that owns the piece, as the tracker's flight record has it.
    pub(super) owner: PeerHandle,
    // The blocks of the piece asked of another peer as well, in the end
    // game, one such peer a block: the block index and the peer.
    pub(super) duplicates: Vec<(u32, PeerHandle)>,
    // The storage holds room for the piece: a block of it was admitted.
    pub(super) room: bool,
    // The blocks a copy of which arrived and is being written, by index in
    // the piece: any other copy is redundant (libtorrent
    // `piece_picker::mark_as_writing`).
    pub(super) writing: Vec<u32>,
    // Who sent each received block, once one was.
    pub(super) senders: Option<Senders>,
}

/// The slot of every piece, by index: Some while the piece is in flight.
pub(crate) struct PieceSlots {
    slots: Box<[Mutex<Option<Box<PieceSlot>>>]>,
    received: Arc<Received>,
    lengths: Lengths,
}

/// A piece's lock, held.
pub(crate) struct Slot<'a> {
    g: MutexGuard<'a, Option<Box<PieceSlot>>>,
    _held: order::Piece,
}

impl Deref for Slot<'_> {
    type Target = Option<Box<PieceSlot>>;

    fn deref(&self) -> &Self::Target {
        &self.g
    }
}

impl DerefMut for Slot<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.g
    }
}

impl PieceSlots {
    pub(super) fn new(lengths: Lengths, received: Arc<Received>) -> Self {
        Self {
            slots: (0..lengths.total_pieces())
                .map(|_| Mutex::new(None))
                .collect(),
            received,
            lengths,
        }
    }

    /// The lock of `piece`, no other piece's held on this thread.
    pub(crate) fn lock(&self, piece: ValidPieceIndex) -> Slot<'_> {
        let held = order::Piece::one();
        Slot {
            g: self.slots[piece.get_usize()].lock(),
            _held: held,
        }
    }

    // The lock of `piece`, others perhaps held: only under the torrent's
    // write lock, which `&mut PieceTracker` stands for.
    pub(super) fn lock_also(&self, piece: ValidPieceIndex) -> Slot<'_> {
        let held = order::Piece::also();
        Slot {
            g: self.slots[piece.get_usize()].lock(),
            _held: held,
        }
    }

    pub(super) fn received(&self) -> &Received {
        &self.received
    }

    /// `peer`'s copy of `chunk` arrived, its piece in flight: what becomes
    /// of it, as [`super::PieceTracker::admitted_block_arrived`] would
    /// rule, under the piece's lock alone. None when the piece holds no
    /// slot: the tracker rules, under the torrent's lock.
    pub(crate) fn arrive(
        &self,
        chunk: &ChunkInfo,
        peer: PeerHandle,
        admit: impl FnOnce(ValidPieceIndex, bool) -> bool,
    ) -> Option<Arrival> {
        let mut slot = self.lock(chunk.piece_index);
        let slot = slot.as_deref_mut()?;
        Some(arrive_in(slot, &self.received, chunk, peer, admit))
    }

    /// The block `chunk`, written, is marked received from `sender` under
    /// its piece's lock alone: true. False when the tracker is to mark it,
    /// under the torrent's lock: its piece holds no slot, or the block is
    /// its last, whose mark takes the piece out of flight for its check, or
    /// one already received.
    pub(crate) fn mark(&self, chunk: &ChunkInfo, sender: PeerHandle) -> bool {
        let mut slot = self.lock(chunk.piece_index);
        let Some(slot) = slot.as_deref_mut() else {
            return false;
        };
        let blocks = self.lengths.chunk_range(chunk.piece_index);
        let at = chunk.absolute_index as usize;
        let count = blocks.len();
        if self.received.get(at) || self.received.count(blocks) + 1 >= count {
            return false;
        }
        slot.writing.retain(|block| *block != chunk.chunk_index);
        self.received.set(at, true);
        record(&mut slot.senders, count, chunk.chunk_index, sender);
        true
    }

    /// The write of `chunk` failed, its piece in flight: it is no longer
    /// being written, and a later copy is taken. False when the piece holds
    /// no slot: the tracker is told, under the torrent's lock.
    pub(crate) fn write_failed(&self, chunk: &ChunkInfo) -> bool {
        let mut slot = self.lock(chunk.piece_index);
        let Some(slot) = slot.as_deref_mut() else {
            return false;
        };
        slot.writing.retain(|block| *block != chunk.chunk_index);
        true
    }

    /// How many pieces hold a slot. Reads every lock in turn.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.lock().is_some())
            .count()
    }
}

// What becomes of `peer`'s copy of `chunk`, of the piece in flight that
// holds `slot` (libtorrent `peer_connection::incoming_piece`). A piece in
// flight is not had: it leaves flight before its check.
pub(super) fn arrive_in(
    slot: &mut PieceSlot,
    received: &Received,
    chunk: &ChunkInfo,
    peer: PeerHandle,
    admit: impl FnOnce(ValidPieceIndex, bool) -> bool,
) -> Arrival {
    if received.get(chunk.absolute_index as usize) || slot.writing.contains(&chunk.chunk_index) {
        return Arrival::Discard;
    }
    if !slot.room {
        if !admit(chunk.piece_index, slot.owner == peer) {
            return Arrival::NoRoom;
        }
        slot.room = true;
    }
    slot.writing.push(chunk.chunk_index);
    let mut cancel = Vec::new();
    if let Some(at) = slot
        .duplicates
        .iter()
        .position(|(block, _)| *block == chunk.chunk_index)
    {
        cancel.push(slot.duplicates.swap_remove(at).1);
    }
    cancel.push(slot.owner);
    cancel.retain(|other| *other != peer);
    Arrival::Write { cancel }
}

// `sender` sent block `block` of a piece of `blocks` blocks.
pub(super) fn record(senders: &mut Option<Senders>, blocks: usize, block: u32, sender: PeerHandle) {
    let record = senders.get_or_insert_with(|| Senders(vec![None; blocks]));
    if let Some(slot) = record.0.get_mut(block as usize) {
        *slot = Some(sender);
    }
}

/// Checks, in debug and test builds, that the torrent's lock is not taken under a
/// piece's.
pub(crate) fn torrent_lock_taken() {
    order::torrent();
}

/// How many times this thread took the torrent's lock.
#[cfg(test)]
pub(crate) fn torrent_takes() -> u64 {
    order::takes()
}

#[cfg(any(debug_assertions, test))]
mod order {
    use std::cell::Cell;

    thread_local! {
        static PIECES: Cell<u32> = const { Cell::new(0) };
        #[cfg(test)]
        static TAKES: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) struct Piece(());

    impl Piece {
        pub(super) fn one() -> Self {
            assert_eq!(
                PIECES.get(),
                0,
                "a second piece lock is taken only under the torrent's write lock"
            );
            Self::also()
        }

        pub(super) fn also() -> Self {
            PIECES.set(PIECES.get() + 1);
            Self(())
        }
    }

    impl Drop for Piece {
        fn drop(&mut self) {
            PIECES.set(PIECES.get() - 1);
        }
    }

    pub(super) fn torrent() {
        #[cfg(test)]
        TAKES.set(TAKES.get() + 1);
        assert_eq!(
            PIECES.get(),
            0,
            "the torrent's lock is taken before a piece's, never under one"
        );
    }

    #[cfg(test)]
    pub(super) fn takes() -> u64 {
        TAKES.get()
    }
}

#[cfg(not(any(debug_assertions, test)))]
mod order {
    pub(super) struct Piece;

    impl Piece {
        #[inline(always)]
        pub(super) fn one() -> Self {
            Self
        }

        #[inline(always)]
        pub(super) fn also() -> Self {
            Self
        }
    }

    #[inline(always)]
    pub(super) fn torrent() {}
}
