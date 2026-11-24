// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{sync::atomic::Ordering, time::Duration};

use crate::session_stats::swarm::{self};

use super::{
    peer::{
        info::PeerInfo,
        stats::snapshot::{PeerStatsFilter, PeerStatsSnapshot},
    },
    stats::snapshot::StatsSnapshot,
};

use super::TorrentStateLive;

impl TorrentStateLive {
    pub fn get_uploaded_bytes(&self) -> u64 {
        self.stats.uploaded_bytes.load(Ordering::Relaxed)
    }

    pub fn get_downloaded_bytes(&self) -> u64 {
        self.stats
            .downloaded_and_checked_bytes
            .load(Ordering::Acquire)
    }

    /// Bytes of the pieces that failed their hash check this run.
    pub fn get_failed_bytes(&self) -> u64 {
        self.stats.failed_bytes.load(Ordering::Relaxed)
    }

    /// Bytes of the blocks received this run and not used.
    pub fn get_redundant_bytes(&self) -> u64 {
        self.stats.redundant_bytes.load(Ordering::Relaxed)
    }

    /// `len` bytes of a block arrived that are not used: the block was never
    /// asked of the peer, we had it, another copy came first, or the
    /// connection ended before the rest of it arrived (libtorrent
    /// `torrent::add_redundant_bytes`, from `peer_connection::incoming_piece`).
    pub(super) fn add_redundant_bytes(&self, len: u64) {
        self.stats.redundant_bytes.fetch_add(len, Ordering::Relaxed);
    }

    /// `len` bytes of a block a closing connection was receiving are not
    /// used. A connection closes after the run ended when the run's end
    /// closed it: its bytes count then in what the torrent moved, as
    /// libtorrent's `torrent::pause` closes each connection, and counts what
    /// it drops, before the torrent stops (`peer_connection::disconnect`).
    pub(super) fn add_closing_redundant_bytes(&self, len: u64) {
        self.session_stats.swarm.wasted(swarm::Waste::CutShort, len);
        let mut moved = self.shared.moved.lock();
        if self.run_counted.load(Ordering::Relaxed) {
            moved.redundant_bytes += len;
            moved.cut_short_bytes += len;
        } else {
            self.add_redundant_bytes(len);
            self.stats.cut_short_bytes.fetch_add(len, Ordering::Relaxed);
        }
    }

    pub fn get_approx_have_bytes(&self) -> u64 {
        self.stats.have_bytes.load(Ordering::Relaxed)
    }

    /// How long ago we last saw a complete copy of the torrent, none if we
    /// never did; no time at all while we have every piece (libtorrent
    /// `torrent::time_since_complete`). The `complete_ago` we send.
    pub fn complete_ago(&self) -> Option<Duration> {
        let now = tokio::time::Instant::now();
        self.last_seen_complete(now)
            .map(|last| now.saturating_duration_since(last))
    }

    /// How long ago anyone connected to us saw a complete copy: we, or a
    /// peer by its `complete_ago`, the latest sighting standing; none if
    /// none did (libtorrent `torrent::second_tick`'s
    /// `m_swarm_last_seen_complete`). For status only, never sent.
    pub fn swarm_complete_ago(&self) -> Option<Duration> {
        let now = tokio::time::Instant::now();
        let mut theirs = None;
        self.peers.for_each_live(|_, _, live| {
            theirs = theirs.max(live.last_seen_complete);
        });
        self.last_seen_complete(now)
            .max(theirs)
            .map(|last| now.saturating_duration_since(last))
    }

    // Our last sighting of a complete copy, which is now while we have
    // every piece.
    fn last_seen_complete(&self, now: tokio::time::Instant) -> Option<tokio::time::Instant> {
        if self.get_approx_have_bytes() == self.lengths.total_length() {
            self.seen_complete(now);
        }
        *self.last_seen_complete.lock()
    }

    /// A complete copy of the torrent was seen at `at`; the latest sighting
    /// stands (libtorrent `torrent::seen_complete`).
    pub(super) fn seen_complete(&self, at: tokio::time::Instant) {
        let mut last = self.last_seen_complete.lock();
        if last.is_none_or(|last| last < at) {
            *last = Some(at);
        }
    }

    pub fn stats_snapshot(&self) -> StatsSnapshot {
        use Ordering::*;
        let downloaded_bytes = self.stats.downloaded_and_checked_bytes.load(Relaxed);
        StatsSnapshot {
            downloaded_and_checked_bytes: downloaded_bytes,
            downloaded_and_checked_pieces: self.stats.downloaded_and_checked_pieces.load(Relaxed),
            fetched_bytes: self.stats.fetched_bytes.load(Relaxed),
            uploaded_bytes: self.stats.uploaded_bytes.load(Relaxed),
            fetched_protocol_bytes: self.stats.fetched_protocol_bytes.load(Relaxed),
            uploaded_protocol_bytes: self.stats.uploaded_protocol_bytes.load(Relaxed),
            failed_bytes: self.get_failed_bytes(),
            redundant_bytes: self.get_redundant_bytes(),
            cut_short_bytes: self.stats.cut_short_bytes.load(Relaxed),
            total_piece_download_ms: self.stats.total_piece_download_ms.load(Relaxed),
            peer_stats: self.peers.stats(),
        }
    }

    pub fn per_peer_stats_snapshot(&self, filter: PeerStatsFilter) -> PeerStatsSnapshot {
        PeerStatsSnapshot {
            peers: self
                .peers
                .states
                .iter()
                .filter(|e| filter.state.matches(e.value().get_state()))
                .map(|e| (e.key().to_string(), e.value().into()))
                .collect(),
        }
    }

    /// The torrent's connections to peers, by address (libtorrent
    /// `torrent::get_peer_info`). A web seed is no peer of the swarm, and
    /// is not listed ([`PeerInfo::of`]).
    pub fn peer_infos(&self) -> Vec<PeerInfo> {
        // The choker's lock goes before the peer table's.
        let optimistic = self
            .shared
            .session
            .upgrade()
            .map(|session| session.choker.optimistic(self.shared.id))
            .unwrap_or_default();
        let total_pieces = self.lengths.total_pieces();
        let mut infos: Vec<PeerInfo> = self
            .peers
            .states
            .iter()
            .filter_map(|e| PeerInfo::of(e.value(), total_pieces, optimistic.contains(e.key())))
            .collect();
        infos.sort_by_key(|info| info.addr);
        infos
    }
}
