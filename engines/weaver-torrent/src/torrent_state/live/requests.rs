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

use crate::core::lengths::{ChunkInfo, ValidPieceIndex};
use crate::peer_protocol::{Message, Request};
use parking_lot::MutexGuard;
use tokio::sync::Notify;
use tracing::{debug, trace};

use crate::{
    Error,
    peer_connection::WriterRequest,
    piece_tracker::{AcquireRequest, AcquireResult, PickOptions},
    session_stats::swarm::{self},
    type_aliases::PeerHandle,
};

use super::peer::{InflightRequest, RemoveInflightRequestResult};

use crate::torrent_state::utils::{TimedExistence, timeit};

use super::{PeerFlowControl, PeerHandler, Pick, REPICK_INTERVAL, SNUB_INTERVAL};

impl PeerHandler {
    /// Acquire a piece for this peer: reserve → in the end game, take over a piece held 3x the peer's piece time, or double a block.
    pub(super) fn acquire_next_piece(&self) -> crate::Result<Pick> {
        // While the peer chokes us we may fetch only the pieces it allows
        // (libtorrent `request_a_block` masks the peer's bitfield with them).
        let choked = self.choked_by_peer();

        // Steal info to process after releasing the peer lock
        let mut steal_info: Option<(PeerHandle, ValidPieceIndex)> = None;
        // Whether this pick started the last queued piece, so the end game
        // began: peers with nothing to ask may now double blocks.
        let mut end_game_began = false;

        // A peer on parole downloads whole pieces alone until one passes
        // (libtorrent `peer_connection::picker_options`).
        let on_parole = self
            .state
            .peers
            .with_peer(self.addr, |peer| peer.trust().on_parole())
            .unwrap_or_default();

        let result = self
            .state
            .peers
            .with_live_mut(self.addr, "acquire_next_piece", |live| {
                let mut g = self.state.lock_write("acquire_next_piece");

                // Nothing is asked in upload mode (libtorrent
                // `peer_connection::send_block_requests`, `add_request`).
                // Read under the peer's lock, which set_upload_mode takes to
                // give back the peer's pieces: a piece is picked before and
                // given back there, or not picked.
                if self.state.upload_mode() {
                    return Ok(Pick::Nothing);
                }
                let pieces = g.pieces.as_mut().ok_or(Error::ChunkTrackerEmpty)?;
                // A piece we have is no longer the peer's to offer (libtorrent
                // `peer_connection::received_piece`, `announce_piece`).
                live.allowed_fast_from_peer
                    .retain(|p| !pieces.chunks().is_piece_have(*p));
                live.suggested
                    .retain(|p| !pieces.chunks().is_piece_have(*p));
                // A queued piece whose every missing block is asked of the
                // peer already holds nothing to ask it: the peer gave it back
                // when it was snubbed, for other peers to fetch, and is not
                // handed it again while those requests stand (libtorrent
                // `request_a_block` passes over a block in the peer's queues,
                // request_blocks.cpp:238-256, and `snub_peer` asks for a new
                // block before it times one out so as not to pick that block
                // again, peer_connection.cpp:5217-5233).
                let mut asked_whole: Vec<ValidPieceIndex> = Vec::new();
                for request in live.inflight_requests() {
                    let piece = request.piece_index;
                    if !pieces.is_inflight(piece)
                        && !asked_whole.contains(&piece)
                        && pieces
                            .missing_chunks(piece)
                            .all(|chunk| live.has_inflight_request(&chunk))
                    {
                        asked_whole.push(piece);
                    }
                }
                let bf = &live.bitfield;
                let allowed_fast = &live.allowed_fast_from_peer;
                let was_end_game = pieces.chunks().every_queued_piece_started();
                // A piece a storage that writes block by block has no room
                // for waits on room_notify, which the storage rings when it
                // has room again.
                let result = pieces.acquire_admitted_piece(
                    AcquireRequest {
                        peer: self.addr,
                        peer_avg_time: if live.is_snubbed() {
                            None
                        } else {
                            self.counters.average_piece_download_time()
                        },
                        priority_pieces: self.state.streams.iter_next_pieces(&self.state.lengths),
                        // Only a piece that can be checked once it arrives.
                        peer_has_piece: |p| {
                            bf.get(p.get() as usize).map(|v| *v) == Some(true)
                                && self.state.metadata.can_verify(p)
                                && (!choked || allowed_fast.contains(&p))
                                && !asked_whole.contains(&p)
                        },
                        availability: |p| self.state.peers.availability.ranked(p),
                        moved: self.state.peers.availability.take_moved(),
                        num_peers: self.state.peers.stats().live as usize,
                        outstanding_requests: live.requested_inflight_count(),
                        options: PickOptions {
                            reverse: live.is_snubbed(),
                            on_parole,
                            suggested: &live.suggested,
                            sequential: self
                                .state
                                .shared
                                .options
                                .sequential
                                .load(Ordering::Relaxed),
                        },
                        can_steal: |p| {
                            self.state.per_piece_locks[p.get_usize()]
                                .try_write()
                                .is_some()
                        },
                    },
                    // A storage that holds pieces in memory takes room as
                    // a piece's first block arrives, not here.
                    |p| {
                        self.state.files.writes_to_memory()
                            || self.state.files.reserve_piece(p, &self.state.room_notify)
                    },
                );
                end_game_began = !was_end_game && pieces.chunks().every_queued_piece_started();

                match result {
                    AcquireResult::Reserved(piece) => {
                        self.swarm().piece_pick(swarm::PiecePick::Reserved);
                        trace!("reserved piece {}", piece);
                        Ok(Pick::Piece(piece))
                    }
                    AcquireResult::Stolen { piece, from_peer } => {
                        self.swarm().piece_pick(swarm::PiecePick::Stolen);
                        debug!("stole piece {} from {}", piece, from_peer);
                        // Store steal info to process after releasing peer lock to avoid deadlock
                        steal_info = Some((from_peer, piece));
                        Ok(Pick::Piece(piece))
                    }
                    AcquireResult::EndGame { piece, chunk } => {
                        self.swarm().piece_pick(swarm::PiecePick::EndGame);
                        debug!("end game: asking for {chunk:?}, asked of another peer too");
                        Ok(Pick::Piece(piece))
                    }
                    AcquireResult::NoneAvailable => {
                        self.swarm().piece_pick(swarm::PiecePick::Nothing);
                        Ok(Pick::Nothing)
                    }
                    AcquireResult::NoRoom => {
                        self.swarm().piece_pick(swarm::PiecePick::NoRoom);
                        Ok(Pick::NoRoom)
                    }
                }
            })
            .transpose()
            .map(|r| r.unwrap_or(Pick::Nothing));

        // Process steal notification outside the peer lock to avoid deadlock
        if let Some((from_peer, piece)) = steal_info {
            self.state.peers.on_steal(from_peer, self.addr, piece);
        }
        if end_game_began {
            self.state.new_pieces_notify.notify_waiters();
        }

        result
    }

    /// The block of the torrent `request` names, if it names one a peer may
    /// ask for.
    pub(super) fn requested_chunk(&self, request: &Request) -> Option<ChunkInfo> {
        let piece_index = self.state.lengths.validate_piece_index(request.index)?;
        self.state
            .lengths
            .requested_block(piece_index, request.begin, request.length)
    }

    async fn wait_for_any_notify(&self, notify: &Notify, check: impl Fn() -> bool) {
        loop {
            // To remove possibility of races, we first grab a token, then check
            // if we need it, and only if so, await.
            let notified = notify.notified();
            if check() {
                return;
            }
            notified.await;
        }
    }

    /// Resolves once the peer's pieces are known: its first message was
    /// read, and its bitfield sized to the torrent.
    pub(super) async fn wait_for_bitfield(&self) {
        self.wait_for_any_notify(&self.on_bitfield_notify, || {
            self.state
                .peers
                .with_live(self.addr, |live| !live.bitfield.is_empty())
                .unwrap_or_default()
        })
        .await;
    }

    /// Resolves once what we want of the peer may have changed: pieces
    /// were queued again or the wanted ones changed (new_pieces_notify), the
    /// torrent finished, a stream started or ended, or the peer's pieces
    /// changed. Each wait is
    /// taken when this is called, not when it is first polled.
    fn wants_changed(&self) -> impl Future<Output = ()> + '_ {
        let pieces = self.state.new_pieces_notify.notified();
        let finished = self.state.finished_notify.notified();
        let streams = self.state.streams.changed.notified();
        let peer_pieces = self.on_bitfield_notify.notified();
        async move {
            tokio::select! {
                () = pieces => {}
                () = finished => {}
                () = streams => {}
                () = peer_pieces => {}
            }
        }
    }

    async fn wait_for_request_slot(&self) {
        loop {
            let Some(notify) = self.request_slots_changed() else {
                return;
            };
            let notified = notify.notified();
            if self.can_send_request() {
                return;
            }
            notified.await;
        }
    }

    // The job of this is to request chunks, time out the requests the peer
    // does not answer, fetch the piece layers a v2 torrent lacks, and also
    // to keep peer alive.
    // The moment this ends, the peer is disconnected.
    pub(super) async fn task_peer_chunk_requester(&self) -> crate::Result<()> {
        tokio::select! {
            r = self.request_chunks() => r,
            r = self.time_out_requests() => r,
            r = self.fetch_layers() => r,
            r = self.time_out_idle() => r,
        }
    }

    /// Returns, ending the connection with no fault of the peer's, once it
    /// is idle long enough (LivePeerState::timed_out), looked at each second
    /// from the peer's first message on (libtorrent
    /// `peer_connection::second_tick`). Connections run short within five of
    /// the torrent's limit or the session's.
    pub(super) async fn time_out_idle(&self) -> crate::Result<()> {
        self.wait_for_bitfield().await;
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let upload_only = self.state.is_finished_and_no_active_streams();
            let full = self.state.peer_semaphore.available_permits() <= 5
                || self
                    .state
                    .shared
                    .session
                    .upgrade()
                    .is_some_and(|session| session.connection_slots.free() <= 5);
            let now = tokio::time::Instant::now();
            if self.state.super_seeding {
                self.state
                    .peers
                    .super_seed_tick(self.addr, now, &self.state.lengths);
            }
            let Some(timed_out) = self
                .state
                .peers
                .with_live(self.addr, |live| live.timed_out(now, upload_only, full))
            else {
                return Ok(());
            };
            if let Some(why) = timed_out {
                debug!(?why, "idle, disconnecting peer");
                return Ok(());
            }
        }
    }

    /// Snubs this peer when it goes its request timeout
    /// (LivePeerState::request_deadline) without delivering a block, and
    /// again every [`SNUB_INTERVAL`] while its requests stay overdue.
    pub(super) async fn time_out_requests(&self) -> crate::Result<()> {
        self.wait_for_bitfield().await;
        let mut next_snub = None;
        loop {
            let Some(notify) = self.request_slots_changed() else {
                return Ok(());
            };
            let changed = notify.notified();
            let Some(deadline) = self
                .state
                .peers
                .with_live(self.addr, |live| live.request_deadline())
            else {
                return Ok(());
            };
            let Some(deadline) = deadline else {
                changed.await;
                continue;
            };
            let deadline = next_snub.map_or(deadline, |at: tokio::time::Instant| deadline.max(at));
            tokio::select! {
                // A block that arrived moved the deadline.
                biased;
                _ = changed => {}
                _ = tokio::time::sleep_until(deadline) => {
                    self.on_requests_timed_out()?;
                    next_snub = Some(tokio::time::Instant::now() + SNUB_INTERVAL);
                }
            }
        }
    }

    /// The peer is snubbed, and the newest piece it owns that waits on it
    /// alone goes back to the queue for other peers to fetch: one a snub, so
    /// a peer that stays overdue gives its pieces back one at a time
    /// (libtorrent `peer_connection::snub_peer` times out one block each
    /// `second_tick`). Its requests stay outstanding, so a block it still
    /// sends is kept. A peer on parole is left its pieces: it was handed
    /// only pieces no one else had started (`snub_peer` returns early on
    /// parole).
    pub(super) fn on_requests_timed_out(&self) -> crate::Result<()> {
        self.swarm().request_event(swarm::RequestEvent::TimedOut);
        let on_parole = self
            .state
            .peers
            .with_peer(self.addr, |peer| peer.trust().on_parole())
            .unwrap_or(false);
        let released = self
            .state
            .peers
            .with_live_mut(self.addr, "snub", |live| {
                live.snub();
                if on_parole {
                    return Ok(None);
                }
                Ok::<_, Error>(
                    self.state
                        .lock_write("snub")
                        .get_pieces_mut()?
                        .release_piece_blocked_by(self.addr, |chunk| {
                            live.has_inflight_request(chunk)
                        }),
                )
            })
            .transpose()?
            .flatten();
        debug!(?released, on_parole, "requests timed out");
        if released.is_some() {
            self.state.spill_requeued();
            self.state.new_pieces_notify.notify_waiters();
        }
        Ok(())
    }

    pub(super) async fn request_chunks(&self) -> crate::Result<()> {
        let handle = self.addr;
        self.wait_for_bitfield().await;

        loop {
            // Taken before what they guard is read, so no change in between
            // is missed.
            let mut wants_changed = std::pin::pin!(self.wants_changed());
            let finished = self.state.is_finished_and_no_active_streams();
            let Some((interesting, redundant)) =
                self.state
                    .peers
                    .with_live_mut(self.addr, "update_interest", |live| {
                        let interesting = self.state.update_interest(live);
                        (interesting, live.is_redundant(finished))
                    })
            else {
                return Ok(());
            };
            // The peer only uploads, and we only upload or want nothing it
            // has: the connection is of no use (libtorrent
            // `peer_connection::disconnect_if_redundant`, from
            // `do_update_interest`, `set_upload_only` and `torrent::finished`).
            if redundant {
                debug!("the peer only uploads and we want nothing of it, disconnecting peer");
                // The count is read before the mode, so a leave between the
                // two moves it past what is kept.
                let epoch = self.state.upload_epoch();
                if self.state.upload_mode() {
                    self.let_go_in_upload_mode
                        .store(epoch + 1, Ordering::Relaxed);
                }
                self.tx
                    .send(WriterRequest::Disconnect(Ok(())))
                    .ok()
                    .ok_or(Error::PeerTaskDead)?;
                // The writer sends what was queued before, NotInterested
                // among it, then closes the connection, which ends this
                // task.
                return std::future::pending().await;
            }
            // The peer has nothing we want: we wait until it, or what we
            // want, changes.
            if !interesting {
                aframe!(wants_changed).await;
                continue;
            }

            // What we want of the peer may change while we wait to ask it:
            // we look again.
            tokio::select! {
                () = aframe!(self.wait_for_request_slot()) => {}
                () = wants_changed.as_mut() => continue,
            }

            // Acquire a piece using the strategy: reserve → in the end game, take over a piece held 3x the peer's piece time, or double a block.
            // What the peer lets us request changes too: it unchokes us, or
            // lets us fetch another piece while it chokes us.
            let slots_changed = self.request_slots_changed();
            let slot_notify = slots_changed.as_deref().map(Notify::notified);
            let room = self.state.room_notify.notified();
            let side = self.request_side();
            let next = match self.acquire_next_piece()? {
                Pick::Piece(next) => next,
                pick @ (Pick::Nothing | Pick::NoRoom) => {
                    debug!(?pick, "no pieces to request");
                    // The request slots change with every block the peer
                    // delivers and every request sent; the pick reads only
                    // the peer's side of them, so only a change of that
                    // side wakes it.
                    let slot_changed = async {
                        let (Some(notify), Some(notified)) =
                            (slots_changed.as_deref(), slot_notify)
                        else {
                            return std::future::pending().await;
                        };
                        let mut notified = std::pin::pin!(notified);
                        loop {
                            notified.as_mut().await;
                            notified.set(notify.notified());
                            if self.request_side() != side {
                                return;
                            }
                        }
                    };
                    // Room the storage gives back matters only to a peer it
                    // was refused: it is given back with every piece the
                    // storage takes.
                    let room = async {
                        if pick == Pick::NoRoom {
                            room.await;
                        } else {
                            std::future::pending().await
                        }
                    };
                    // Nothing changes while a piece another peer holds grows
                    // old enough to take over, or blocks asked of others
                    // become ones to double: the peer picks again after
                    // REPICK_INTERVAL (libtorrent `peer_connection::second_tick`
                    // picks again 5 s after the last request in the end game).
                    tokio::select! {
                        () = wants_changed => debug!("woken up, new pieces might be available"),
                        () = slot_changed => debug!("woken up, request slots changed"),
                        () = room => debug!("woken up, the storage has room"),
                        () = tokio::time::sleep(REPICK_INTERVAL) => debug!("picking again"),
                    }
                    continue;
                }
            };

            // Only the chunks not yet received: a stolen piece keeps the
            // chunks its previous owner delivered. In the end game, the one
            // block this peer doubles.
            let missing = self
                .state
                .lock_read("missing chunks")
                .get_pieces()?
                .chunks_to_request(next, self.addr)
                .collect::<Vec<_>>();

            for chunk in missing {
                let request = Request {
                    index: next.get(),
                    begin: chunk.offset,
                    length: chunk.size,
                };

                // The request goes out at once: the limits charge the block
                // as it crosses the socket, not before it is asked for
                // (libtorrent `peer_connection::send_block_requests` asks no
                // bandwidth; `setup_receive` reads within the quota).
                if !aframe!(self.wait_to_request(next)).await? {
                    break;
                }

                match self
                    .state
                    .peers
                    .with_live_mut(handle, "add chunk request", |live| {
                        // Only the piece's owner requests its chunks. The check
                        // and the request are one step under this peer's lock,
                        // so a steal either comes after and cancels the request,
                        // or comes first and this peer stops requesting.
                        let owned = self
                            .state
                            .lock_read("add chunk request")
                            .get_pieces()
                            .is_ok_and(|pieces| pieces.may_request(next, self.addr));
                        owned.then(|| live.add_inflight_request(chunk))
                    }) {
                    Some(Some(true)) => {}
                    // Stolen or released: the piece is no longer ours to request.
                    Some(None) => break,
                    Some(Some(false)) => {
                        // The block is asked of the peer already, and is not
                        // asked again: the peer doubled it in the end game
                        // before the piece came back to the queue and to it
                        // (libtorrent `request_a_block` passes over a block
                        // in the peer's queues, request_blocks.cpp:238-256).
                        self.state
                            .stats
                            .asked_already
                            .fetch_add(1, Ordering::Relaxed);
                        self.swarm()
                            .request_event(swarm::RequestEvent::AskedAlready);
                        debug!(?chunk, "asked of the peer already");
                        continue;
                    }
                    // peer died
                    None => return Ok(()),
                };

                if self
                    .tx
                    .send(WriterRequest::Message(Message::Request(request)))
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
    }

    pub(super) fn on_i_am_choked(&self) -> crate::Result<()> {
        self.state
            .peers
            .with_live_mut(self.addr, "choked by the peer", |live| {
                live.choked_by_peer = true;
            });
        // A plain peer that chokes us drops every request we sent it (BEP 3).
        // A fast one answers each with its block or a Reject (BEP 6), so the
        // requests stay until it does (libtorrent
        // `bt_peer_connection::on_choke` takes them as rejected only without
        // the fast extension).
        if !self.speaks_fast() {
            self.return_requests()?;
        }
        self.notify_request_slots_changed();
        Ok(())
    }

    /// The peer will not send the block `request` names (BEP 6). The peer
    /// gives back every piece it owns and every request, as when its
    /// requests time out: a piece has one owner, which must be able to
    /// deliver all of it (libtorrent `peer_connection::incoming_reject_request`
    /// gives back only the block). A Reject for a request we never sent ends
    /// the connection (BEP 6: the peer should close it).
    pub(super) fn on_rejected(&self, request: Request) -> anyhow::Result<()> {
        let Some(chunk) = self.requested_chunk(&request) else {
            anyhow::bail!("peer rejected {request:?}, which names no block of the torrent");
        };
        // A piece the peer rejects is no longer one it lets us fetch while
        // it chokes us, or, unchoked, one it suggests (libtorrent
        // `incoming_reject_request`).
        let choked = self.choked_by_peer();
        let Some(answer) =
            self.state
                .peers
                .with_live_mut(self.addr, "reject_inflight_request", |live| {
                    let offered = if choked {
                        &mut live.allowed_fast_from_peer
                    } else {
                        &mut live.suggested
                    };
                    offered.retain(|p| *p != chunk.piece_index);
                    live.reject_inflight_request(&chunk)
                })
        else {
            return Ok(());
        };
        match answer {
            RemoveInflightRequestResult::Expected => {
                self.return_requests()?;
            }
            RemoveInflightRequestResult::LateCanceled => {
                trace!(
                    ?request,
                    "peer rejected a request we had stopped waiting for"
                );
            }
            RemoveInflightRequestResult::Unexpected => {
                anyhow::bail!("peer rejected {request:?}, which we never asked it for")
            }
        }
        Ok(())
    }

    /// Every request outstanding to this peer stops being in flight, and every
    /// piece it owns goes back to the queue with the chunks it already
    /// delivered, for any peer to take. One step under this peer's lock, as a
    /// request is checked and added: the requester either finds the piece gone
    /// or has its request taken here. Returns the requests taken.
    fn return_requests(&self) -> crate::Result<Vec<InflightRequest>> {
        let returned = self
            .state
            .peers
            .with_live_mut(self.addr, "return_requests", |live| {
                let released = self
                    .state
                    .lock_write("return_requests")
                    .get_pieces_mut()?
                    .release_pieces_owned_by(self.addr);
                Ok::<_, Error>((released, live.take_inflight_requests()))
            })
            .transpose()?;
        let Some((released, taken)) = returned else {
            return Ok(Vec::new());
        };
        if released > 0 {
            self.state.spill_requeued();
            self.state.new_pieces_notify.notify_waiters();
        }
        Ok(taken)
    }

    pub(super) fn on_i_am_unchoked(&self) {
        trace!("we are unchoked");
        self.state
            .peers
            .with_live_mut(self.addr, "unchoked by the peer", |live| {
                live.choked_by_peer = false;
            });
        self.notify_request_slots_changed();
    }

    // Whether the peer chokes us; it does until it says otherwise.
    pub(super) fn choked_by_peer(&self) -> bool {
        self.state
            .peers
            .with_live(self.addr, |live| live.choked_by_peer)
            .unwrap_or(true)
    }

    /// Whether we may send the peer another request: it has room for one,
    /// and it does not choke us or lets us fetch some piece while it does
    /// (BEP 6).
    pub(super) fn can_send_request(&self) -> bool {
        let request_window = self.lock_flow_control("can_send_request").request_window;

        self.state
            .peers
            .with_live(self.addr, |live| {
                (!live.choked_by_peer || !live.allowed_fast_from_peer.is_empty())
                    && live.can_hold_another_request(request_window)
            })
            .unwrap_or(false)
    }

    /// Waits until the peer has room for a request for a block of `piece`.
    /// False when the peer chokes us and does not let us fetch `piece`: like
    /// the requests libtorrent has not sent when it is choked
    /// (`peer_connection::clear_request_queue`), the rest of the piece is not
    /// asked for, so the piece goes back, and with it all the peer holds,
    /// which go back only together.
    async fn wait_to_request(&self, piece: ValidPieceIndex) -> crate::Result<bool> {
        loop {
            let Some(notify) = self.request_slots_changed() else {
                return Ok(true);
            };
            let notified = notify.notified();
            let request_window = self.lock_flow_control("wait_to_request").request_window;
            let slot = self.state.peers.with_live(self.addr, |live| {
                (
                    !live.choked_by_peer || live.allowed_fast_from_peer.contains(&piece),
                    live.can_hold_another_request(request_window),
                )
            });
            match slot {
                None | Some((true, true)) => return Ok(true),
                Some((false, _)) => {
                    self.return_requests()?;
                    return Ok(false);
                }
                Some((true, false)) => notified.await,
            }
        }
    }

    pub(super) fn lock_flow_control(
        &self,
        reason: &'static str,
    ) -> TimedExistence<MutexGuard<'_, PeerFlowControl>> {
        TimedExistence::new(timeit(reason, || self.flow_control.lock()), reason)
    }

    fn request_slots_changed(&self) -> Option<Arc<Notify>> {
        self.state
            .peers
            .with_live(self.addr, |live| live.request_slots_changed())
    }

    /// What a pick reads of the peer's side of the requests: whether it
    /// chokes us, the pieces it lets us fetch while it does, and whether a
    /// request to it is outstanding, which keeps the end game from it.
    fn request_side(&self) -> Option<(bool, usize, bool)> {
        self.state.peers.with_live(self.addr, |live| {
            (
                live.choked_by_peer,
                live.allowed_fast_from_peer.len(),
                live.requested_inflight_count() == 0,
            )
        })
    }

    pub(super) fn notify_request_slots_changed(&self) {
        if let Some(notify) = self.request_slots_changed() {
            notify.notify_waiters();
        }
    }
}
