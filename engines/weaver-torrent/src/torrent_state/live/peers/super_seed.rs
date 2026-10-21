// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Super-seeding (BEP 16): a seed tells each peer of one piece only, and
//! hands it the next once the piece is seen to have spread.

use std::collections::HashSet;

use crate::{
    core::lengths::{Lengths, ValidPieceIndex},
    peer_connection::WriterRequest,
    peer_protocol::Message,
    type_aliases::PeerHandle,
};

use super::PeerStates;

impl PeerStates {
    /// The pieces of the peer `handle` are known afresh, from its bitfield
    /// or from its first message without one. Unless it holds a piece it
    /// lacks, it is handed one to super-seed and told with HAVE
    /// (libtorrent `bt_peer_connection::write_bitfield` under super-seeding).
    /// The piece is one it lacks, held by the fewest live peers, and handed
    /// to no other peer when one such is left (libtorrent
    /// `torrent::get_piece_to_super_seed`); the lowest index among equals.
    pub fn super_seed(&self, handle: PeerHandle, lengths: &Lengths) {
        let _held = self.super_seeding.lock();
        let holds_one_it_lacks = self.with_live(handle, |live| {
            live.super_seeded
                .is_some_and(|piece| !live.bitfield.get(piece.get_usize()).is_some_and(|b| *b))
        });
        if holds_one_it_lacks == Some(false) {
            self.hand_super_seed(handle, lengths);
        }
    }

    /// The peer `from` reported `piece`. When it was not the peer handed
    /// it, or it is our only peer, the piece has spread: every peer handed
    /// it that has it is handed the next (libtorrent
    /// `peer_connection::incoming_have` under `strict_super_seeding`).
    pub fn super_seed_spread(&self, from: PeerHandle, piece: ValidPieceIndex, lengths: &Lengths) {
        let _held = self.super_seeding.lock();
        let handed_to_from = self.with_live(from, |live| live.super_seeded == Some(piece));
        let alone = self
            .states
            .iter()
            .filter(|p| p.get_live().is_some())
            .count()
            == 1;
        if handed_to_from == Some(true) && !alone {
            return;
        }
        let spread: Vec<PeerHandle> = self
            .states
            .iter()
            .filter(|p| {
                p.get_live().is_some_and(|live| {
                    live.super_seeded == Some(piece)
                        && live.bitfield.get(piece.get_usize()).is_some_and(|b| *b)
                })
            })
            .map(|p| *p.key())
            .collect();
        for handle in spread {
            self.hand_super_seed(handle, lengths);
        }
    }

    /// Whether the peer `handle` may ask for `piece`: only the piece it was
    /// handed (libtorrent `peer_connection::incoming_request`).
    pub fn super_seeded(&self, handle: PeerHandle, piece: ValidPieceIndex) -> bool {
        self.with_live(handle, |live| live.super_seeded == Some(piece)) == Some(true)
    }

    // Replaces the piece `handle` holds with the next, under the lock
    // `super_seeding`. A peer that lacks no piece holds none.
    fn hand_super_seed(&self, handle: PeerHandle, lengths: &Lengths) {
        let handed: HashSet<ValidPieceIndex> = self
            .states
            .iter()
            .filter(|p| *p.key() != handle)
            .filter_map(|p| p.get_live()?.super_seeded)
            .collect();
        let Some(has) = self.with_live(handle, |live| live.bitfield.clone()) else {
            return;
        };
        let next = (0..lengths.total_pieces())
            .filter_map(|i| lengths.validate_piece_index(i))
            .filter(|i| !has.get(i.get_usize()).is_some_and(|b| *b))
            .min_by_key(|i| (handed.contains(i), self.availability.get(*i), i.get()));
        self.with_live_mut(handle, "super_seed", |live| {
            live.super_seeded = next;
            if let Some(piece) = next {
                let _ = live
                    .tx
                    .send(WriterRequest::Message(Message::Have(piece.get())));
            }
        });
    }
}
