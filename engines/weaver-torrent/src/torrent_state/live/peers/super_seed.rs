// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Super-seeding (BEP 16): a seed tells each peer of two pieces only, and
//! hands it another once it reports one of them, or stays uninterested.

use std::{collections::HashSet, time::Duration};

use rand::RngExt;
use tokio::time::Instant;

use crate::{
    core::lengths::{Lengths, ValidPieceIndex},
    type_aliases::PeerHandle,
};

use super::PeerStates;

/// How long a peer stays uninterested before it is handed another piece
/// (libtorrent `peer_connection::second_tick`).
const SUPER_SEED_UNINTERESTED: Duration = Duration::from_secs(10);

impl PeerStates {
    /// The pieces of the peer `handle` are known, from its first message:
    /// it is handed two (libtorrent `bt_peer_connection::write_bitfield`
    /// under super-seeding).
    pub fn super_seed(&self, handle: PeerHandle, lengths: &Lengths) {
        let _held = self.super_seeding.lock();
        for _ in 0..2 {
            self.hand_super_seed(handle, None, lengths);
        }
    }

    /// The peer `from` reported `piece`: when it was handed that piece, it
    /// is handed another in its place (libtorrent
    /// `peer_connection::incoming_have`, without `strict_super_seeding`).
    pub fn super_seed_have(&self, from: PeerHandle, piece: ValidPieceIndex, lengths: &Lengths) {
        let _held = self.super_seeding.lock();
        if self.super_seeded(from, piece) {
            self.hand_super_seed(from, Some(piece), lengths);
        }
    }

    /// At `now`, the peer `handle` is handed another piece if it has not
    /// been interested for longer than `SUPER_SEED_UNINTERESTED`: it may
    /// want that one (libtorrent `peer_connection::second_tick`).
    pub fn super_seed_tick(&self, handle: PeerHandle, now: Instant, lengths: &Lengths) {
        let _held = self.super_seeding.lock();
        let uninterested = self.with_live(handle, |live| {
            live.uninterested_since()
                .is_some_and(|since| now.saturating_duration_since(since) > SUPER_SEED_UNINTERESTED)
        });
        if uninterested == Some(true) {
            self.hand_super_seed(handle, None, lengths);
        }
    }

    /// Whether the peer `handle` may ask for `piece`: only a piece it was
    /// handed (libtorrent `peer_connection::incoming_request`).
    pub fn super_seeded(&self, handle: PeerHandle, piece: ValidPieceIndex) -> bool {
        self.with_live(handle, |live| live.is_super_seeded(piece)) == Some(true)
    }

    // Hands `handle`, under the lock `super_seeding`, a piece it lacks and
    // as few live peers have as any; a piece handed to a peer, this one
    // too, counts as more than any peer count while another is left; a
    // random one among equals (libtorrent `torrent::get_piece_to_super_seed`).
    // It replaces `replace`, or the older piece the peer holds; a peer that
    // lacks no piece holds none.
    fn hand_super_seed(
        &self,
        handle: PeerHandle,
        replace: Option<ValidPieceIndex>,
        lengths: &Lengths,
    ) {
        let handed: HashSet<ValidPieceIndex> = self
            .states
            .iter()
            .filter_map(|p| Some(p.get_live()?.super_seeded_pieces().collect::<Vec<_>>()))
            .flatten()
            .collect();
        let Some(has) = self.with_live(handle, |live| live.bitfield.clone()) else {
            return;
        };
        let rarity = |piece: &ValidPieceIndex| {
            if handed.contains(piece) {
                u32::MAX
            } else {
                self.availability.get(*piece)
            }
        };
        let lacks: Vec<ValidPieceIndex> = (0..lengths.total_pieces())
            .filter_map(|i| lengths.validate_piece_index(i))
            .filter(|i| !has.get(i.get_usize()).is_some_and(|b| *b))
            .collect();
        let rarest = lacks.iter().map(rarity).min();
        let rarest: Vec<ValidPieceIndex> = lacks
            .into_iter()
            .filter(|piece| Some(rarity(piece)) == rarest)
            .collect();
        let new = (!rarest.is_empty()).then(|| rarest[rand::rng().random_range(0..rarest.len())]);
        self.with_live_mut(handle, "super_seed", |live| {
            live.hand_super_seed(replace, new)
        });
    }
}
