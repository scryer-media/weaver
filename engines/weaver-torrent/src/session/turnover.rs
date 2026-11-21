// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{sync::Arc, time::Duration};

use crate::torrent_state::TorrentStateLive;
use crate::torrent_state::live::choker::{OPTIMISTIC_UNCHOKE_INTERVAL, UNCHOKE_INTERVAL};
use crate::type_aliases::PeerHandle;
use anyhow::Context;
use tracing::debug_span;

use super::Session;

// How often the session turns peers over (libtorrent
// `peer_turnover_interval`), and when it first does after it starts
// (`session_impl::m_disconnect_time_scaler`).
const PEER_TURNOVER_INTERVAL: Duration = Duration::from_secs(300);
const FIRST_PEER_TURNOVER: Duration = Duration::from_secs(90);
// The share of a connection cap, in percent, at which peers are turned over
// (libtorrent `peer_turnover_cutoff`).
const PEER_TURNOVER_CUTOFF: usize = 90;

// Where a torrent stands when a connection past the session's cap must
// replace one of another torrent's; the lowest gives one up: a torrent with
// connections, then a finished one, so the downloading ones keep theirs,
// then the most connections (libtorrent `compare_disconnect_torrent`).
fn disconnect_candidate_rank(torrent: &TorrentStateLive) -> (bool, bool, std::cmp::Reverse<usize>) {
    let (held, _) = torrent.connections();
    (held == 0, !torrent.is_finished(), std::cmp::Reverse(held))
}

impl Session {
    pub(super) fn start_peer_turnover(self: &Arc<Self>) {
        let session = Arc::downgrade(self);
        self.spawn(
            debug_span!(parent: self.rs(), "peer_turnover"),
            "peer_turnover",
            async move {
                let mut tick = tokio::time::interval_at(
                    tokio::time::Instant::now() + FIRST_PEER_TURNOVER,
                    PEER_TURNOVER_INTERVAL,
                );
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    session
                        .upgrade()
                        .context("session is dead")?
                        .turn_peers_over();
                }
            },
        )
    }

    // Hands the upload slots out every UNCHOKE_INTERVAL and passes the
    // optimistic ones on every OPTIMISTIC_UNCHOKE_INTERVAL, each at once when
    // the choker asks for it sooner, over every live torrent's peers at once
    // (libtorrent `session_impl::second_tick`). When both fall due the
    // optimistic slots pass first, so the round sees their new holders.
    pub(super) fn start_choker(self: &Arc<Self>) {
        let session = Arc::downgrade(self);
        let choker = self.choker.clone();
        self.spawn(
            debug_span!(parent: self.rs(), "choker"),
            "choker",
            async move {
                let first = tokio::time::Instant::now() + UNCHOKE_INTERVAL;
                let mut rounds = tokio::time::interval_at(first, UNCHOKE_INTERVAL);
                let mut rotations = tokio::time::interval_at(first, OPTIMISTIC_UNCHOKE_INTERVAL);
                loop {
                    let rotate = tokio::select! {
                        biased;
                        _ = rotations.tick() => true,
                        _ = choker.rotation_requested() => {
                            rotations.reset();
                            true
                        }
                        _ = choker.reshuffle_requested() => {
                            rounds.reset();
                            false
                        }
                        _ = rounds.tick() => false,
                    };
                    let torrents = session
                        .upgrade()
                        .context("session is dead")?
                        .live_torrents();
                    let now = tokio::time::Instant::now();
                    if rotate {
                        choker.rotate_optimistic(&torrents, now);
                    } else {
                        choker.recalculate(&torrents, now);
                    }
                }
            },
        )
    }

    // The peer at `addr` of `torrent` said it is interested: it takes one of
    // the session's upload slots when one is free.
    pub(crate) fn peer_interested(&self, torrent: &TorrentStateLive, addr: PeerHandle) {
        self.choker.peer_interested(torrent, addr);
    }

    // The torrents that are live, so whose peers the session uploads to and
    // turns over.
    pub(crate) fn live_torrents(&self) -> Vec<Arc<TorrentStateLive>> {
        self.with_torrents(|torrents| torrents.filter_map(|(_, t)| t.live()).collect())
    }

    // Makes room for connect candidates when connections run short
    // (libtorrent `session_impl::second_tick`). When the session holds
    // PEER_TURNOVER_CUTOFF percent of its cap, the torrent with the most
    // connections turns some over; otherwise each torrent holding that share
    // of its own cap does. Under a cap of 6 it would be too disruptive, and
    // nothing is turned over.
    pub(crate) fn turn_peers_over(&self) {
        let limit = self.connection_slots.limit();
        if limit < 6 {
            return;
        }
        let torrents = self.live_torrents();
        if self.connection_slots.in_use() >= limit * PEER_TURNOVER_CUTOFF / 100 {
            if let Some(most) = torrents.iter().max_by_key(|t| t.connections().0) {
                most.turn_peers_over();
            }
            return;
        }
        for torrent in &torrents {
            let (held, max) = torrent.connections();
            if max >= 6 && held >= max * PEER_TURNOVER_CUTOFF / 100 {
                torrent.turn_peers_over();
            }
        }
    }

    /// Closes up to `peers` TCP peer connections so the tunnel they ride
    /// gets their bytes back, and says how many. The torrents give them up in
    /// the order a connection past the session's cap takes one
    /// (`disconnect_candidate_rank`), and each its connections worth least
    /// first (`disconnect_lowest`). Only peers at an address `among` takes
    /// are closed: those whose socket rides the tunnel that asks. The close
    /// is no failure: the peers stay connect candidates, unmarked.
    pub fn give_back_tcp_peers(
        &self,
        peers: usize,
        among: &(dyn Fn(std::net::SocketAddr) -> bool + Send + Sync),
    ) -> usize {
        let mut torrents = self.live_torrents();
        torrents.sort_by_key(|torrent| disconnect_candidate_rank(torrent));
        let now = tokio::time::Instant::now();
        let mut closed = 0;
        for torrent in torrents {
            if closed == peers {
                break;
            }
            closed += torrent.give_back_tcp_peers(peers - closed, now, among);
        }
        closed
    }

    // An incoming connection at `addr` of `live` that the slack let in past
    // the session's cap takes a place under it: a free one, or that of the
    // lowest-ranked connection of the torrent that holds the most, when it
    // holds more than `live` does; else it is closed (libtorrent
    // `session_impl::incoming_connection` marks it
    // `peer_connection::peer_exceeds_limit`, and attaching it settles that:
    // `peer_connection::attach_to_torrent`, `find_disconnect_candidate_torrent`,
    // `torrent::find_lowest_ranking_peer`). One that displaced a connection
    // of its own torrent took that one's place, and is under the cap.
    pub(crate) fn settle_over_limit(&self, live: &TorrentStateLive, addr: PeerHandle) {
        if !live.exceeds_limit(addr, &self.connection_slots) {
            return;
        }
        let slot = self.connection_slots.try_open().or_else(|| {
            let fullest = self
                .live_torrents()
                .into_iter()
                .min_by_key(|t| disconnect_candidate_rank(t))?;
            (fullest.connections().0 > live.connections().0)
                .then(|| fullest.give_up_lowest())
                .flatten()
        });
        live.settle_slot(addr, slot);
    }
}
