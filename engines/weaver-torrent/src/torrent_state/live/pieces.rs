// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::atomic::Ordering;

use crate::core::lengths::ValidPieceIndex;
use crate::peer_protocol::Message;
use tracing::{debug, info};

use crate::{
    peer_connection::WriterRequest,
    piece_priority::{FilePriority, PiecePriority},
    type_aliases::PeerHandle,
};

use super::peer::LivePeerState;

use super::{FLUSH_BITV_EVERY_BYTES, TorrentStateLive, files_in_piece};

impl TorrentStateLive {
    pub(super) fn transmit_haves(&self, index: ValidPieceIndex) {
        if self.shared.options.disable_upload() {
            return;
        }
        self.peers.announce_have(index);
    }

    /// Our interest in the peer `live` is whether it has a piece we want and
    /// lack: a selected one we do not have, or one a stream reads next; none
    /// in upload mode. The peer is told when that changes. Returns it
    /// (libtorrent `peer_connection::do_update_interest`, not interested in
    /// a torrent that only uploads).
    pub(super) fn update_interest(&self, live: &mut LivePeerState) -> bool {
        let interesting = !self.upload_mode() && {
            let g = self.lock_read("update_interest");
            g.get_chunks().is_ok_and(|chunks| {
                let has = |p: ValidPieceIndex| live.bitfield.get(p.get_usize()).is_some_and(|b| *b);
                chunks.wants_any_of(&live.bitfield)
                    || self
                        .streams
                        .iter_next_pieces(&self.lengths)
                        .any(|p| has(p) && !chunks.is_piece_have(p))
            })
        };
        if live.interest(interesting, tokio::time::Instant::now()) {
            let message = if interesting {
                Message::Interested
            } else {
                Message::NotInterested
            };
            let _ = live.tx.send(WriterRequest::Message(message));
        }
        interesting
    }

    /// We have `piece` now: a peer we want it of may have nothing else we
    /// want (libtorrent `torrent::piece_passed` updates the interest of each
    /// interesting peer that has the piece).
    ///
    /// Every seeder has the same pieces, so the answer for one is the answer
    /// for all: once a seeder is found still interesting the rest are left
    /// as they are, and the torrent's lock is taken once for them, not once
    /// each for every piece that passes. Leaving a peer alone writes
    /// nothing, so it cannot undo what a later piece's pass tells it.
    pub(super) fn update_interest_of_holders(&self, piece: ValidPieceIndex) {
        let mut seeders_interesting = false;
        self.peers.for_each_live_mut(|_, peer| {
            if let Some(live) = peer.get_live_mut()
                && live.is_interesting()
                && live.bitfield.get(piece.get_usize()).is_some_and(|b| *b)
            {
                if live.seeder && seeders_interesting {
                    return;
                }
                let interesting = self.update_interest(live);
                if live.seeder {
                    seeders_interesting = interesting;
                }
            }
        });
    }

    /// The files' levels changed; see [`crate::chunk_tracker::ChunkTracker::prioritize`].
    pub(crate) fn prioritize(
        &self,
        files: &[FilePriority],
        pieces: &[PiecePriority],
    ) -> anyhow::Result<()> {
        let hns = {
            let mut g = self.lock_write("prioritize");
            let hns = g
                .get_pieces_mut()?
                .prioritize(&self.metadata.file_infos, files, pieces);
            self.settle_finish(g)?;
            hns
        };
        // The peers' requesters look again at what they may ask for, and
        // whether we want anything at all (libtorrent
        // `torrent::update_piece_priorities` calls `update_interest` on
        // every peer).
        self.new_pieces_notify.notify_waiters();
        // With the state lock released. A dying peer holds its shard of the peer table
        // while it asks whether the torrent is finished (on_peer_died), so touching the
        // table under the state lock is the reverse order, and the two deadlock.
        self.update_upload_only();
        if !hns.finished() {
            self.reconnect_all_not_needed_peers();
        }
        Ok(())
    }

    pub(crate) fn set_piece_priorities(
        &self,
        priorities: impl IntoIterator<Item = (ValidPieceIndex, PiecePriority)>,
    ) -> anyhow::Result<()> {
        let hns = {
            let mut g = self.lock_write("set_piece_priorities");
            let hns = g.get_pieces_mut()?.set_piece_priorities(priorities);
            self.settle_finish(g)?;
            hns
        };
        // As in prioritize().
        self.new_pieces_notify.notify_waiters();
        // Wanting a skipped piece again may make peers we let go useful. With the
        // state lock released, for the reason given in prioritize().
        self.update_upload_only();
        if !hns.finished() {
            self.reconnect_all_not_needed_peers();
        }
        Ok(())
    }

    // Peer `peer` is known to have sent bad data of `piece`, which failed
    // its hash check: whether it may be banned for that (libtorrent
    // `peer_connection::received_invalid_data`). A web seed serves none of
    // what the piece lies in from then on and has its pieces told again,
    // and may be banned only when it is left nothing to serve
    // (`web_peer_connection::received_invalid_data`); any other peer may.
    fn received_invalid_data(&self, peer: PeerHandle, piece: ValidPieceIndex) -> bool {
        let Some(have) = self
            .shared
            .web_seeds
            .piece_failed(peer, piece, &self.web_layout)
        else {
            return true;
        };
        let nothing_left = !have.any();
        self.peers.update_bitfield(peer, have);
        nothing_left
    }

    /// `peers` sent bad data of `piece`: each loses points and goes on
    /// parole; one at the floor, or `known_bad`, the bad data known to be
    /// its, when its connection lets it go for that, is banned (libtorrent
    /// `torrent::penalize_peers`).
    pub(super) fn penalize_peers(
        &self,
        peers: impl IntoIterator<Item = PeerHandle>,
        piece: ValidPieceIndex,
        known_bad: bool,
    ) {
        for peer in peers {
            let bannable = known_bad && self.received_invalid_data(peer, piece);
            let to_ban = self
                .peers
                .with_peer_mut(peer, "piece_failed", |p| {
                    p.piece_failed(bannable, &self.peers)
                })
                .unwrap_or(bannable);
            if to_ban {
                self.ban_for_bad_data(peer);
            }
        }
    }

    /// The storage is told of every piece queued again since it was last
    /// told: a piece that took room gives it back, and its blocks are
    /// written block by block from then on.
    pub(super) fn spill_requeued(&self) {
        let requeued = match self.lock_write("requeued").get_pieces_mut() {
            Ok(pieces) => pieces.take_requeued(),
            Err(_) => return,
        };
        for piece in requeued {
            self.files.on_piece_requeued(piece);
        }
    }

    /// The piece did not pass its hash check, or the storage refused it: it
    /// is downloaded again.
    pub(super) fn requeue_piece(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        // Before it is queued again, so the storage has let go of the failed
        // bytes by the time another peer is handed the piece.
        if let Err(e) = self.files.on_piece_hash_failed(piece) {
            debug!(
                ?piece,
                "file storage errored in on_piece_hash_failed(): {e:#}"
            );
        }
        self.lock_write("mark_piece_broken")
            .get_pieces_mut()?
            .mark_piece_hash_failed(piece);
        self.new_pieces_notify.notify_waiters();
        Ok(())
    }

    /// The checked piece, which the storage took, is had (libtorrent
    /// `torrent::we_have` and `file_completed_alert`). It is marked, counted
    /// for the run and into its files, and the finish settled, in one hold
    /// of the lock: whoever sees the torrent finished sees every had piece
    /// counted, in the totals and in its files. Each file it made whole is
    /// told to the storage once the lock is let go, so after the finish
    /// rings when the piece is the last.
    pub(super) fn on_piece_completed(&self, id: ValidPieceIndex) -> anyhow::Result<()> {
        let whole = self.count_piece_had(id)?;
        for file in whole {
            self.files.on_file_completed(file);
        }
        Ok(())
    }

    /// The piece marked had and counted, under the write lock, and what
    /// follows from it. Returns the files it made whole, padding left out;
    /// empty, and unallocated, when it made none.
    fn count_piece_had(&self, id: ValidPieceIndex) -> anyhow::Result<Vec<usize>> {
        let mut whole = Vec::new();
        let mut g = self.lock_write("on_piece_completed");
        let locked = &mut **g;
        let pieces = locked.get_pieces_mut()?;
        pieces.mark_piece_hash_ok(id);
        let piece_len = u64::from(self.lengths.piece_length(id));
        self.stats
            .downloaded_and_checked_bytes
            .fetch_add(piece_len, Ordering::Release);
        self.stats
            .downloaded_and_checked_pieces
            .fetch_add(1, Ordering::Release);
        self.stats
            .have_bytes
            .fetch_add(piece_len, Ordering::Relaxed);
        for (idx, file_info) in files_in_piece(&self.metadata.file_infos, &self.lengths, id) {
            if pieces.update_file_have_on_piece_completed(id, idx, file_info)
                && !file_info.attrs.padding
            {
                whole.push(idx);
            }
        }

        self.streams
            .wake_streams_on_piece_completed(id, self.metadata.lengths());

        locked.unflushed_bitv_bytes += piece_len;
        if locked.unflushed_bitv_bytes >= FLUSH_BITV_EVERY_BYTES {
            locked.try_flush_bitv(&self.shared, true)
        }

        if locked.get_chunks()?.get_selected_pieces()[id.get_usize()]
            && locked.get_chunks()?.is_finished()
            && !locked.finished
        {
            info!(id=self.shared.id, info_hash=?self.shared.info_hash, "torrent finished downloading");
        }
        self.settle_finish(g)?;
        Ok(whole)
    }
}
