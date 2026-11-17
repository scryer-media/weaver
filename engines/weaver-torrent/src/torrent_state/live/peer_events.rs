// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::peers;
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
};

use crate::buffers::ByteBuf;
use crate::peer_protocol::{
    Message, Request,
    extended::{
        ExtendedMessage,
        ut_holepunch::{self, UtHolepunch},
        ut_metadata::UtMetadataData,
        ut_pex::{MAX_PEX_MESSAGE_LEN, UtPex},
    },
};
use tracing::{debug, trace, warn};

use crate::{
    peer_connection::{Answer, WriterRequest},
    session_stats::{
        network::HolepunchEvent,
        swarm::{self, Direction},
    },
};

use super::{
    choker::CHOKED_REQUEST_GRACE,
    peer::{PeerState, PeerTx, Refused, UploadLedger, empty_bitfield, ingest_bitfield},
    peers::holepunch::is_local,
};

use crate::torrent_state::utils::TimedExistence;

use super::PeerHandler;

impl PeerHandler {
    pub(super) fn swarm(&self) -> &swarm::SwarmStats {
        &self.state.session_stats.swarm
    }

    /// What carries the connection, once its handshake is done.
    pub(super) fn transport(&self) -> Option<swarm::Transport> {
        let transport = self.transport.load(Ordering::Relaxed);
        swarm::Transport::ALL
            .get(usize::from(transport).wrapping_sub(1))
            .copied()
    }

    /// Bytes of the connection that crossed after its handshake.
    #[inline]
    pub(super) fn count_bytes(&self, direction: Direction, kind: swarm::ByteKind, bytes: u64) {
        if let Some(transport) = self.transport() {
            self.swarm().bytes(transport, direction, kind, bytes);
        }
    }

    // The peer's send buffer, once the connection is live.
    pub(super) fn upload_ledger(&self) -> Option<&Arc<UploadLedger>> {
        self.upload.get()
    }

    /// Queues `answer` for the writer, counted until it is written. False
    /// once the writer is gone.
    pub(super) fn answer(&self, answer: Answer) -> bool {
        self.answers.queued(&answer);
        self.tx.send(WriterRequest::Answer(answer)).is_ok()
    }

    /// The DHT a peer of this torrent hears of: none for a private torrent,
    /// which is found through its trackers only. So a private torrent's
    /// handshake offers no DHT, it sends no PORT, and a PORT from its peer
    /// adds no node.
    fn dht(&self) -> Option<crate::dht::Dht> {
        if self.state.metadata.info.info().private {
            return None;
        }
        self.state.shared.session.upgrade()?.get_dht().cloned()
    }

    /// The metadata this torrent hands its peers: none for a private
    /// torrent, whose metadata leaves only in the torrent file, so its
    /// extended handshake names no ut_metadata and no size, and a request
    /// for a piece of it is not answered.
    pub(super) fn shared_metadata(&self) -> Option<&bytes::Bytes> {
        let info = &self.state.metadata.info_bytes;
        (!info.is_empty() && !self.state.metadata.info.info().private).then_some(info)
    }

    /// The DHT port this peer is told, over its family (see
    /// `Session::dht_port_told`).
    pub(super) fn told_dht_port(&self) -> Option<u16> {
        let dht = self.dht()?;
        let ipv6 = self.addr.ip()?.to_canonical().is_ipv6();
        let session = self.state.shared.session.upgrade()?;
        Some(session.dht_port_told(&dht, ipv6))
    }

    /// Sends our DHT port, once per connection, with the port as it is
    /// then: a mapping that changes later is told to new connections only
    /// (libtorrent `bt_peer_connection::write_dht_port`, sent at the
    /// handshake only, bt_peer_connection.cpp:329, 1418 and 3628, v2.0.15).
    /// The writer sends the bitfield before anything queued, so a port
    /// queued at the handshake follows it.
    pub(super) fn send_dht_port(&self) {
        let Some(port) = self.told_dht_port() else {
            return;
        };
        if !self.sent_dht_port.swap(true, Ordering::Relaxed) {
            let _ = self.tx.send(WriterRequest::Message(Message::Port(port)));
        }
    }

    /// A peer named its DHT node: the DHT pings it and adds it on reply
    /// (libtorrent `peer_connection::incoming_dht_port`), once for each
    /// port the connection names, so a repeated PORT is no new ping. A peer
    /// that sends PORT runs a DHT node even without the handshake bit, so
    /// it gets ours (`bt_peer_connection::on_dht_port`).
    pub(super) fn on_dht_port(&self, port: u16) {
        let news = self
            .state
            .peers
            .with_live_mut(self.addr, "dht_port", |live| live.dht_node_named(port))
            .unwrap_or(false);
        if news
            && let Some(dht) = self.dht()
            && let Some(ip) = self.addr.ip()
        {
            dht.add_node(SocketAddr::new(ip, port));
        }
        self.send_dht_port();
    }

    /// A holepunch message (BEP 55), from a peer that advertised the
    /// extension; from one that did not it is ignored (libtorrent
    /// `bt_peer_connection::on_holepunch`).
    pub(super) fn on_holepunch(&self, msg: UtHolepunch) {
        // Only a swarm peer, at an address, speaks the extension.
        debug_assert!(self.addr.addr().is_some(), "a web seed sent a holepunch");
        let Some(me) = self.addr.addr() else {
            return;
        };
        let supported = self
            .state
            .peers
            .with_live(self.addr, |live| live.supports_holepunch)
            .unwrap_or(false);
        if !supported {
            debug!(
                ?msg,
                "holepunch message from a peer that did not advertise it, ignoring"
            );
            self.count_holepunch(HolepunchEvent::Unoffered);
            return;
        }
        // A private torrent takes no peer from any source but its trackers
        // (AGENTS.md, "Private trackers"): it neither dials the address a
        // Connect names nor introduces two peers to each other. A Connect is
        // ignored, and a Rendezvous is refused "no support", the BEP 55 code
        // that says no holepunch can happen through us whatever we are
        // connected to (`on_rendezvous`). This departs from libtorrent by the
        // operator's ruling of 2026-11-16: `bt_peer_connection::on_holepunch`
        // adds a Connect's address and introduces with no private check
        // (src/bt_peer_connection.cpp:1593-1629, v2.0.15). The extension is
        // still advertised in the handshake.
        let private = self.state.metadata.info.info().private;
        match msg {
            UtHolepunch::Rendezvous(target) => self.on_rendezvous(me, target, private),
            // A local address is taken only from a local peer.
            UtHolepunch::Connect(addr) => {
                if !private && (!is_local(addr.ip()) || is_local(me.ip())) {
                    self.count_holepunch(HolepunchEvent::Connect);
                    self.state.holepunch(addr);
                } else {
                    self.count_holepunch(HolepunchEvent::ConnectIgnored);
                }
            }
            UtHolepunch::Error(addr, code) => {
                self.count_holepunch(HolepunchEvent::Failed);
                debug!(?addr, code, "the peer could not introduce us");
            }
        }
    }

    fn count_holepunch(&self, event: HolepunchEvent) {
        self.state.session_stats.network.holepunch(event);
    }

    /// The peer asks us to introduce it to the peer at `target`: when we are
    /// connected to that peer and it speaks the extension, each is told to
    /// connect to the other; else the peer is told why not. A local peer and
    /// one that is not are never introduced (libtorrent
    /// `bt_peer_connection::on_holepunch`, `rendezvous`). A private torrent
    /// introduces no one: the peer is told "no support".
    fn on_rendezvous(&self, me: SocketAddr, target: SocketAddr, private: bool) {
        let send = |tx: &PeerTx, msg: UtHolepunch| {
            let _ = tx.send(WriterRequest::Message(Message::Extended(
                ExtendedMessage::UtHolepunch(msg),
            )));
        };
        let found = self.state.peers.with_live(target.into(), |live| {
            (live.supports_holepunch, live.tx.clone())
        });
        let refused = match &found {
            _ if private => Some(ut_holepunch::error::NO_SUPPORT),
            None => Some(ut_holepunch::error::NOT_CONNECTED),
            Some((false, _)) => Some(ut_holepunch::error::NO_SUPPORT),
            Some(_) if target == me => Some(ut_holepunch::error::NO_SELF),
            Some(_) => None,
        };
        if let Some(code) = refused {
            self.count_holepunch(HolepunchEvent::Refused);
            send(&self.tx, UtHolepunch::Error(target, code));
            return;
        }
        if is_local(target.ip()) != is_local(me.ip()) {
            self.count_holepunch(HolepunchEvent::Refused);
            debug!(?target, "not introducing a local peer to one that is not");
            return;
        }
        if let Some((_, target_tx)) = found {
            self.count_holepunch(HolepunchEvent::Introduced);
            send(&self.tx, UtHolepunch::Connect(target));
            send(&target_tx, UtHolepunch::Connect(me));
        }
    }
    pub(super) fn on_peer_died(self, error: Option<crate::Error>) -> crate::Result<()> {
        let peers = &self.state.peers;
        let handle = self.addr;
        let mut pe = match peers.states.get_mut(&handle) {
            Some(peer) => TimedExistence::new(peer, "on_peer_died"),
            None => {
                warn!(
                    id = self.state.shared.id,
                    info_hash = ?self.state.shared.info_hash,
                    addr=?handle,
                    "bug: peer not found in table. Forgetting it forever"
                );
                return Ok(());
            }
        };
        // Whether the connection failed as libtorrent's would, above normal
        // severity (`peer_connection::failed`; see `Error::is_failure`): one
        // the peer closed or left silent once open did not, so an encrypted
        // handshake it closed leaves no failure for the plaintext connection
        // that follows to wait behind. A connection to ourselves did not
        // either; it is banned below instead.
        let failed = error.as_ref().is_some_and(crate::Error::is_failure);
        // A connect to a peer that speaks the holepunch extension failed,
        // and no holepunch asked for it: a peer that can introduce us is
        // asked to (libtorrent `peer_connection::connect_failed`). A
        // connection that opened and failed its handshake reached the peer,
        // so no introduction is asked for.
        let rendezvous = matches!(pe.value().get_state(), PeerState::Connecting(_))
            && error.as_ref().is_some_and(crate::Error::is_dial_failure)
            && pe.value().supports_holepunch
            && !pe.value().holepunched();
        let needed = pe.value_mut().connection_closing(
            tokio::time::Instant::now(),
            failed,
            self.state.is_finished_and_no_active_streams(),
            peers,
        );
        let prev = pe.value_mut().take_state(peers);
        self.swarm().closed(
            match prev {
                PeerState::Live(_) => swarm::ClosePhase::Live,
                _ => swarm::ClosePhase::Connecting,
            },
            swarm::CloseReason::of(error.as_ref()),
        );
        self.state.layer_fetch.forget(handle);

        // Whether the connection held an upload slot, when it was live. The
        // choker hears of it once the peer table is let go: it takes its own
        // lock before the table's.
        let mut held_slot = None;
        match prev {
            PeerState::Connecting(_) => {}
            PeerState::Live(live) => {
                held_slot = Some(!live.is_choked());
                let mut g = self.state.lock_write("mark_chunk_requests_canceled");

                // Release all pieces owned by this peer (fixes the bug where pieces
                // could be in both queue_pieces AND inflight_pieces after peer death).
                // A paused torrent has no pieces in flight: pausing queued
                // them all again.
                let released = g
                    .pieces
                    .as_mut()
                    .map_or(0, |pieces| pieces.release_pieces_owned_by(self.addr));
                if released > 0 {
                    trace!(
                        "peer dead, released {} in-flight pieces back to queue",
                        released
                    );
                }

                // Also handle any active chunk-level inflight requests.
                let mut had_inflight = false;
                for req in live.inflight_requests() {
                    had_inflight = true;
                    trace!(
                        "peer dead, marking chunk request cancelled, index={}, chunk={}",
                        req.piece_index.get(),
                        req.chunk_index
                    );
                }

                if released > 0 || had_inflight {
                    self.state.new_pieces_notify.notify_waiters();
                }
            }
            s @ (PeerState::Queued | PeerState::Dead | PeerState::NotNeeded) => {
                warn!(
                    id = self.state.shared.id,
                    info_hash = ?self.state.shared.info_hash,
                    addr = ?handle,
                    "bug: peer was in a wrong state {s:?}, ignoring it forever"
                );
                // Prevent deadlocks.
                drop(pe);
                self.state.peers.drop_peer(handle);
                return Ok(());
            }
        };

        // A connection that ended in an error counts against the peer; one we
        // closed ourselves does not. Either way the peer stays a connect
        // candidate unless it only uploads and so do we (libtorrent
        // `peer_list::connection_closed`, `is_connect_candidate`).
        if failed {
            self.counters.errors.fetch_add(1, Ordering::Relaxed);
        }
        // Let go while we only uploaded, and upload mode was left before the
        // close came: the leave found the peer still connected, so the
        // close is due again at once, as the leave makes every other peer.
        let let_go = self.let_go_in_upload_mode.load(Ordering::Relaxed);
        if !failed && let_go != 0 && let_go - 1 != self.state.upload_epoch() {
            pe.value_mut().forget_last_close();
        }
        peers.settle_closed(pe.value_mut(), needed);
        trace!(state = %pe.value().get_state(), "peer settled after its connection closed");
        drop(pe);
        // A connection to ourselves bans the address on this torrent, so it
        // is not dialed again (libtorrent `peer_connection::disconnect`:
        // `self_connection` bans the peer's `torrent_peer`, one per address
        // with `allow_multiple_connections_per_ip` off).
        if matches!(error, Some(crate::Error::ConnectingToOurselves))
            && let Some(ip) = handle.ip()
        {
            debug!(addr = %handle, "the peer is ourselves");
            peers.ban(ip);
        }
        // A peer the IP filter came to refuse while it was connected leaves
        // the list now its connection is gone.
        peers.refuse_at(handle, |addr| self.state.refuses(addr));
        // So does a private torrent's peer no tracker named, asked to close
        // once a tracker answered.
        peers.untracked_closed(handle);
        if rendezvous
            && let Some(at) = handle.addr()
            && let Some(introducer) = peers.introducer(at)
        {
            self.count_holepunch(HolepunchEvent::Asked);
            let _ = introducer.send(WriterRequest::Message(Message::Extended(
                ExtendedMessage::UtHolepunch(UtHolepunch::Rendezvous(at)),
            )));
        }
        if let Some(held_slot) = held_slot
            && let Some(session) = self.state.shared.session.upgrade()
        {
            session
                .choker
                .peer_gone(self.state.shared.id, handle, held_slot);
        }
        Ok(())
    }

    pub(super) fn on_download_request(&self, request: Request) -> anyhow::Result<()> {
        if self.state.torrent().options.disable_upload() {
            anyhow::bail!("upload disabled, but peer requested a piece")
        }

        // A request for a block that is not one, or of a piece we do not
        // have, is refused; while we super-seed, so is one for a piece other
        // than the ones the peer was handed (libtorrent
        // `peer_connection::incoming_request`, which answers each with
        // `write_reject_request`).
        let chunk_info = match self.requested_chunk(&request) {
            Some(chunk)
                if !self.state.super_seeding
                    || self.state.peers.super_seeded(self.addr, chunk.piece_index) =>
            {
                self.state
                    .lock_read("is_chunk_ready_to_upload")
                    .get_chunks()?
                    .is_chunk_ready_to_upload(&chunk)
                    .then_some(chunk)
            }
            _ => None,
        };
        let Some(chunk_info) = chunk_info else {
            return self.on_invalid_request(request);
        };

        // A peer that asks without having said it is interested is taken to
        // have said so.
        let interested = self
            .state
            .peers
            .with_live(self.addr, |live| live.peer_interested);
        if interested == Some(false) {
            self.tell_choker_interested();
        }

        let chunks_per_piece = self.state.lengths.default_chunks_per_piece();
        let Some(queued) = self
            .state
            .peers
            .with_live_mut(self.addr, "queue_upload", |live| {
                live.queue_upload(chunk_info, chunks_per_piece)
            })
        else {
            return Ok(());
        };
        match queued {
            Ok(ready) => {
                if ready {
                    self.state.upload_turn(self.addr);
                }
                Ok(())
            }
            Err(Refused::Choked(choked_at)) => {
                let since = tokio::time::Instant::now().saturating_duration_since(choked_at);
                if since > CHOKED_REQUEST_GRACE {
                    anyhow::bail!("received {request:?} {since:?} after we choked the peer");
                }
                self.swarm().request_event(swarm::RequestEvent::WhileChoked);
                trace!(?request, "ignoring a request from a peer we choke");
                Ok(())
            }
            Err(Refused::AllowedFastSpent) => anyhow::bail!(
                "received {request:?} while choked, past three times the blocks of the piece"
            ),
            Err(Refused::QueueFull) => {
                self.swarm().request_event(swarm::RequestEvent::QueueFull);
                trace!(?request, "ignoring a request past the peer's request queue");
                Ok(())
            }
            Err(Refused::Queued) => Ok(()),
        }
    }

    /// The peer asked for what we cannot serve. The request is dropped, with
    /// a Reject on a fast connection; a plain one has no word for it
    /// (libtorrent `peer_connection::incoming_request`).
    fn on_invalid_request(&self, request: Request) -> anyhow::Result<()> {
        self.swarm().request_event(swarm::RequestEvent::Invalid);
        let peer_chokes_us = self.choked_by_peer();
        let keep = self
            .state
            .peers
            .with_live_mut(self.addr, "invalid_request", |live| {
                live.invalid_request(request, peer_chokes_us, |request| {
                    self.answer(Answer::Reject(request));
                })
            });
        if keep == Some(false) {
            anyhow::bail!("too many invalid requests while choked, the last {request:?}");
        }
        trace!(?request, "refusing a request we cannot serve");
        Ok(())
    }

    /// The peer no longer wants the block `request` names: it is not sent
    /// unless it already was.
    pub(super) fn on_upload_cancelled(&self, request: Request) {
        let Some(chunk) = self.requested_chunk(&request) else {
            trace!(?request, "cancel names no block of the torrent");
            return;
        };
        self.state
            .peers
            .with_live_mut(self.addr, "cancel_upload", |live| {
                live.cancel_upload(&chunk)
            });
    }

    /// The peer says whether it only uploads (libtorrent
    /// `peer_connection::set_upload_only`). Whether the connection is still
    /// of use is decided where interest is: the requester looks again.
    pub(super) fn on_upload_only(&self, upload_only: bool) {
        self.state
            .peers
            .with_live_mut(self.addr, "upload_only", |live| {
                live.set_upload_only(upload_only)
            });
        self.on_bitfield_notify.notify_waiters();
    }

    /// The peer said it is interested. A fast peer is first sent the pieces it
    /// may fetch while we choke it, unless we upload nothing.
    pub(super) fn on_peer_interested(&self) {
        if !self.state.torrent().options.disable_upload()
            && let Some(ip) = self.addr.ip()
        {
            let info_hash = self
                .state
                .shared
                .info_hashes
                .for_peer(self.speaks_v2.load(Ordering::Relaxed));
            self.state
                .peers
                .with_live_mut(self.addr, "send_allowed_fast", |live| {
                    live.send_allowed_fast(
                        ip,
                        info_hash,
                        &self.state.lengths,
                        self.state.super_seeding,
                    )
                });
        }
        self.tell_choker_interested();
    }

    // The session's choker hears that the peer is interested.
    fn tell_choker_interested(&self) {
        if let Some(session) = self.state.shared.session.upgrade() {
            session.peer_interested(&self.state, self.addr);
        }
    }

    /// The peer lets us fetch `index` while it chokes us (libtorrent
    /// `peer_connection::incoming_allowed_fast`). An index past the last
    /// piece, or a piece we have, is ignored.
    pub(super) fn on_allowed_fast(&self, index: u32) -> anyhow::Result<()> {
        let Some(piece) = self.state.lengths.validate_piece_index(index) else {
            trace!(index, "allowed fast names no piece of the torrent");
            return Ok(());
        };
        if self
            .state
            .lock_read("on_allowed_fast")
            .get_chunks()?
            .is_piece_have(piece)
        {
            return Ok(());
        }
        self.state
            .peers
            .with_live_mut(self.addr, "on_allowed_fast", |live| {
                if !live.allowed_fast_from_peer.contains(&piece) {
                    live.allowed_fast_from_peer.push(piece);
                }
            });
        self.notify_request_slots_changed();
        Ok(())
    }

    /// The peer suggests we fetch `index` (libtorrent
    /// `peer_connection::incoming_suggest`). An index past the last piece,
    /// or a piece we have, is ignored.
    pub(super) fn on_suggest(&self, index: u32) -> anyhow::Result<()> {
        let Some(piece) = self.state.lengths.validate_piece_index(index) else {
            trace!(index, "suggest names no piece of the torrent");
            return Ok(());
        };
        if self
            .state
            .lock_read("on_suggest")
            .get_chunks()?
            .is_piece_have(piece)
        {
            return Ok(());
        }
        self.state
            .peers
            .with_live_mut(self.addr, "on_suggest", |live| live.suggest(piece));
        Ok(())
    }

    /// A HAVE names a piece the peer now has; one at or past the piece count
    /// ends the connection.
    pub(super) fn on_have(&self, have: u32) -> anyhow::Result<()> {
        let Some(piece) = self.state.lengths.validate_piece_index(have) else {
            anyhow::bail!(
                "have {have} is past the last of {} pieces",
                self.state.lengths.total_pieces()
            );
        };
        trace!(have, "peer has piece");
        let have = self.state.peers.peer_has(self.addr, piece);
        if self.state.super_seeding {
            self.state
                .peers
                .super_seed_have(self.addr, piece, &self.state.lengths);
        }
        // Only a piece we lack, or the peer now holding every piece, can
        // change what we ask of it; a HAVE for a piece we hold wakes no one.
        let changed = match have {
            None | Some(peers::Have::Known) => false,
            Some(peers::Have::Completed) => true,
            Some(peers::Have::Gained) => self
                .state
                .lock_read("on_have")
                .get_chunks()
                .map_or(true, |chunks| !chunks.is_piece_have(piece)),
        };
        if changed {
            self.on_bitfield_notify.notify_waiters();
        }
        Ok(())
    }

    /// A DONT_HAVE names a piece the peer no longer has (libtorrent
    /// `peer_connection::incoming_dont_have`); one past the last piece is
    /// ignored.
    pub(super) fn on_dont_have(&self, index: u32) {
        let Some(piece) = self.state.lengths.validate_piece_index(index) else {
            trace!(index, "dont have names no piece of the torrent");
            return;
        };
        trace!(index, "peer no longer has piece");
        self.state.peers.peer_lost(self.addr, piece);
        self.on_bitfield_notify.notify_waiters();
    }

    pub(super) fn on_bitfield(&self, bitfield: &[u8]) -> anyhow::Result<()> {
        let bf = ingest_bitfield(bitfield, self.state.lengths.total_pieces())?;
        self.state.peers.update_bitfield(self.addr, bf);
        self.on_bitfield_notify.notify_waiters();
        Ok(())
    }

    /// Have All and Have None stand for a bitfield of every bit set or none
    /// (libtorrent `peer_connection::incoming_have_all`,
    /// `incoming_have_none`), and replace any the peer sent before.
    pub(super) fn on_have_all_or_none(&self, byte: u8) -> anyhow::Result<()> {
        let total_pieces = self.state.lengths.total_pieces();
        let len = empty_bitfield(total_pieces).as_raw_slice().len();
        self.on_bitfield(&vec![byte; len])
    }

    /// Whether the connection speaks the fast extension.
    pub(super) fn speaks_fast(&self) -> bool {
        self.state
            .peers
            .with_live(self.addr, |live| live.fast)
            .unwrap_or(false)
    }

    /// The peer's pieces are known, from its first message: while we
    /// super-seed, it is handed two.
    pub(super) fn super_seed(&self) {
        if self.state.super_seeding {
            self.state.peers.super_seed(self.addr, &self.state.lengths);
        }
    }

    pub(super) fn send_metadata_piece(&self, piece_id: u32) -> anyhow::Result<()> {
        let Some(info) = self.shared_metadata() else {
            return Ok(());
        };
        let data = UtMetadataData::piece_of(info, piece_id)?;
        anyhow::ensure!(
            self.answer(Answer::metadata(data)),
            "error sending UtMetadata: channel closed"
        );
        Ok(())
    }

    /// A PEX message from the peer (libtorrent
    /// `ut_pex_peer_plugin::on_extended`). One past [`MAX_PEX_MESSAGE_LEN`],
    /// or past the peer's rate, ends the connection. A peer on a local
    /// network it names is taken only from a peer on one. Each added peer
    /// it had not named before is news, a candidate for our peer list, up
    /// to [`peer::MAX_PEX_PEERS`] of each family; a dropped one is only
    /// forgotten as named, as the sender lost it, not we.
    pub(super) fn on_pex_message(&self, msg: UtPex<ByteBuf<'_>>) -> anyhow::Result<()> {
        let len = msg.lists_len();
        if len > MAX_PEX_MESSAGE_LEN {
            anyhow::bail!("PEX message of {len} bytes, past {MAX_PEX_MESSAGE_LEN}");
        }
        let from_local = self.addr.ip().is_some_and(is_local);
        let news = self
            .state
            .peers
            .with_live_mut(self.addr, "pex_named", |live| {
                if !live.pex_arrived(tokio::time::Instant::now()) {
                    anyhow::bail!("PEX messages too frequent");
                }
                Ok(live.pex_named(
                    msg.added_peers()
                        .filter(|p| from_local || !is_local(p.addr.ip())),
                    msg.dropped_peers().map(|p| p.addr),
                ))
            })
            .transpose()?
            .unwrap_or_default();
        for peer in news {
            self.state.add_pex_peer(peer.addr, peer.flags);
        }
        Ok(())
    }
}
