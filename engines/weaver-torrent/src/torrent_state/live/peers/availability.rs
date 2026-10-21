// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! How many connected peers have each piece.

use std::sync::atomic::AtomicU32;

use crate::{
    torrent_state::utils::{atomic_dec, atomic_inc},
    type_aliases::BS,
};

/// One count per piece: the number of live peers whose bitfield holds it. A
/// peer's bitfield is added when it is counted in and taken away once when it
/// is counted out, so a count never outlives the connection that made it.
pub(crate) struct Availability(Box<[AtomicU32]>);

impl Availability {
    pub fn new(total_pieces: u32) -> Self {
        Self((0..total_pieces).map(|_| AtomicU32::new(0)).collect())
    }

    /// Every piece's count, in piece order.
    #[cfg(test)]
    pub fn counts(&self) -> Vec<u32> {
        self.0
            .iter()
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .collect()
    }

    pub(super) fn peer_has(&self, piece: usize) {
        atomic_inc(&self.0[piece]);
    }

    pub(super) fn add(&self, bitfield: &BS) {
        for piece in bitfield.iter_ones() {
            atomic_inc(&self.0[piece]);
        }
    }

    pub(super) fn remove(&self, bitfield: &BS) {
        for piece in bitfield.iter_ones() {
            atomic_dec(&self.0[piece]);
        }
    }
}
