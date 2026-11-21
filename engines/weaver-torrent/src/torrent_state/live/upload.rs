// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use tracing::debug;

use crate::type_aliases::PeerHandle;

use super::{TorrentStateLive, UploadMode, UploadWatch};

impl UploadWatch {
    /// The block bytes sent this run.
    pub fn uploaded(&self) -> u64 {
        self.live.stats.uploaded_bytes.load(Ordering::SeqCst)
    }

    /// Resolves once the bytes sent are no longer `seen`.
    pub async fn changed_from(&self, seen: u64) {
        loop {
            // Taken before the total is read: a send after the read rings it.
            let rung = self.live.uploaded_notify.notified();
            if self.uploaded() != seen {
                return;
            }
            rung.await;
        }
    }

    /// When the last write that carried block bytes was handed to its
    /// connection's writer: no older than the bytes [`Self::uploaded`] read
    /// before it counts.
    pub fn last_uploaded_at(&self) -> Option<tokio::time::Instant> {
        match self.live.last_upload.load(Ordering::Relaxed) {
            0 => None,
            at => Some(self.live.upload_origin + Duration::from_nanos(at - 1)),
        }
    }
}

impl Drop for UploadWatch {
    fn drop(&mut self) {
        self.live.upload_watches.fetch_sub(1, Ordering::SeqCst);
    }
}

impl TorrentStateLive {
    pub(super) async fn task_upload_scheduler(
        self: Arc<Self>,
        mut turns: tokio::sync::mpsc::UnboundedReceiver<PeerHandle>,
    ) -> crate::Result<()> {
        // The peers take turns in the order they became ready, and a turn
        // fills the peer's send buffer to its watermark. A peer is ready
        // again once its writer sends enough to fall below it, and then
        // waits behind every peer that became ready meanwhile: however many
        // blocks one peer asked for, the next peer waits at most one send
        // buffer of it (libtorrent `peer_connection::fill_send_buffer`, one
        // send buffer per peer, each held to its watermark). The writer pays
        // for each block as it sends it.
        while let Some(addr) = turns.recv().await {
            self.peers
                .with_live_mut(addr, "hand_upload", |live| live.hand_upload());
        }
        Ok(())
    }

    /// `addr` became ready to be handed a block: it takes its turn behind
    /// those that became ready before it.
    pub(super) fn upload_turn(&self, addr: PeerHandle) {
        let _ = self.upload_turns.send(addr);
    }

    /// A listener on the block bytes this run sends its peers.
    pub fn upload_watch(self: &Arc<Self>) -> UploadWatch {
        self.upload_watches.fetch_add(1, Ordering::SeqCst);
        UploadWatch {
            live: Arc::clone(self),
        }
    }

    // Tells the peers that take upload_only when whether we only upload
    // changed: we finished, or want pieces again (libtorrent
    // `torrent::send_upload_only`, from `finished` and `resume_download`).
    pub(super) fn update_upload_only(&self) {
        let upload_only = self.is_upload_only();
        if self.told_upload_only.swap(upload_only, Ordering::Relaxed) != upload_only {
            self.peers.send_upload_only(upload_only);
        }
    }

    // We only upload the torrent: it is in upload mode, or has every piece
    // it wants and no stream wants more; a super-seed never says so, as a
    // peer may take it to have one piece and leave (libtorrent
    // `torrent::is_upload_only`, `bt_peer_connection::write_extensions`).
    pub(super) fn is_upload_only(&self) -> bool {
        !self.super_seeding && (self.upload_mode() || self.is_finished_and_no_active_streams())
    }

    /// Whether the torrent is in upload mode: a write found no room, so it
    /// fetches nothing and keeps seeding (libtorrent `torrent::upload_mode`).
    pub fn upload_mode(&self) -> bool {
        self.upload_mode.borrow().is_some()
    }

    /// Resolves once the torrent is in upload mode, when `on` holds, or out
    /// of it.
    pub async fn wait_for_upload_mode(&self, on: bool) {
        let mut mode = self.upload_mode.subscribe();
        let _ = mode.wait_for(|at| at.is_some() == on).await;
    }

    /// Resolves to true once the torrent is in upload mode, or to false
    /// when its run ends first. It holds nothing of the torrent while it
    /// waits.
    pub fn upload_mode_entered(&self) -> impl std::future::Future<Output = bool> + Send + 'static {
        let mut mode = self.upload_mode.subscribe();
        async move { mode.wait_for(Option::is_some).await.is_ok() }
    }

    /// Leaves upload mode, as its owner does once room was made for a
    /// torrent added with
    /// [`AddTorrentOptions::owner_wakes_full_disk`](crate::AddTorrentOptions::owner_wakes_full_disk):
    /// the requesters ask again, and the peers are told we download. Nothing
    /// happens out of upload mode.
    pub fn leave_upload_mode(&self) {
        self.set_upload_mode(false);
    }

    /// Enters upload mode at its owner's word, with no write refused: the
    /// torrent fetches nothing and keeps seeding until its owner leaves it
    /// by [`Self::leave_upload_mode`], as a mode a refused write entered
    /// for a torrent added with
    /// [`AddTorrentOptions::owner_wakes_full_disk`](crate::AddTorrentOptions::owner_wakes_full_disk)
    /// does. Entered already, it goes on waiting for its owner.
    pub fn hold_download(&self) {
        self.enter_upload_mode(false, self.upload_epoch());
    }

    /// How many times upload mode was left: taken before a write, it is
    /// what that write's refusal is read against.
    pub(crate) fn upload_epoch(&self) -> u64 {
        self.upload_epoch.load(Ordering::Acquire)
    }

    /// Enters upload mode for a write made at `epoch`, to end by itself
    /// once it lasted OPTIMISTIC_DISK_RETRY when `retry` holds, else only
    /// when its owner says. A write made before upload mode was last left
    /// enters nothing: the leave came after it. Entered already, it keeps
    /// its time, and once it waits for its owner it goes on waiting.
    pub(super) fn enter_upload_mode(&self, retry: bool, epoch: u64) {
        let now = tokio::time::Instant::now();
        let mut entered = false;
        self.upload_mode.send_if_modified(|mode| match mode {
            _ if self.upload_epoch.load(Ordering::Acquire) != epoch => false,
            Some(mode) if mode.retry && !retry => {
                mode.retry = false;
                true
            }
            Some(_) => false,
            None => {
                *mode = Some(UploadMode { at: now, retry });
                entered = true;
                true
            }
        });
        if entered {
            self.upload_mode_changed(true);
        }
    }

    /// Enters or leaves upload mode (libtorrent `torrent::set_upload_mode`),
    /// a mode entered here ending by itself.
    pub(super) fn set_upload_mode(&self, on: bool) {
        if on {
            self.enter_upload_mode(true, self.upload_epoch());
            return;
        }
        if self.upload_mode.send_if_modified(|mode| {
            self.upload_epoch.fetch_add(1, Ordering::AcqRel);
            mode.take().is_some()
        }) {
            self.upload_mode_changed(false);
        }
    }

    /// Upload mode was entered or left. Entering, every request outstanding
    /// is cancelled and every piece in flight goes back to the queue, each
    /// under its peer's lock, under which a requester picks and asks;
    /// leaving, the requesters ask again and the peers with no connection
    /// are due to be connected to at once. Either way each requester looks
    /// at its interest again, and the peers are told whether we only
    /// upload.
    fn upload_mode_changed(&self, on: bool) {
        debug!(id = self.shared.id, on, "upload mode");
        if on {
            self.peers.for_each_live_mut(|addr, peer| {
                if let Some(live) = peer.get_live_mut() {
                    if let Ok(pieces) = self.lock_write("set_upload_mode").get_pieces_mut() {
                        pieces.release_pieces_owned_by(addr);
                    }
                    live.cancel_all_requests();
                }
            });
            self.spill_requeued();
        } else {
            // The seeds let go while we only uploaded are due again now,
            // not a reconnect time after they were let go.
            self.peers.reconnect_after_upload_mode();
        }
        self.new_pieces_notify.notify_waiters();
        self.update_upload_only();
    }
}
