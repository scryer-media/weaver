// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

pub mod stats;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

use crate::core::hash_id::Id20;
use crate::core::lengths::{ChunkInfo, ValidPieceIndex};
use crate::peer_protocol::{Message, Request};

use tokio::sync::{
    Notify,
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tokio::time::Instant;

use crate::peer_connection::WriterRequest;
use crate::stream_connect::ConnectionKind;
use crate::type_aliases::BF;

use super::PeerStates;

pub(crate) type InflightRequest = ChunkInfo;
pub(crate) type PeerRx = UnboundedReceiver<WriterRequest>;
pub(crate) type PeerTx = UnboundedSender<WriterRequest>;

// libtorrent `min_reconnect_time`: the wait unit between connection attempts to one peer.
pub(crate) const MIN_RECONNECT_TIME: Duration = Duration::from_secs(60);
// libtorrent `max_failcount`: failures in a row that end a peer's candidacy.
pub(crate) const MAX_FAILCOUNT: u32 = 3;

#[derive(Debug)]
pub(crate) struct Peer {
    pub addr: SocketAddr,
    state: PeerState,
    pub stats: stats::atomic::PeerStats,
    // Where the peer listens, when we know. We can connect to the peer when it
    // listens on the address it is listed under.
    pub outgoing_address: Option<SocketAddr>,
    // Connections that failed in a row; one that delivers data clears it.
    failcount: u32,
    // When the last of those failures happened.
    last_failed: Option<Instant>,
}

impl Peer {
    /// A peer that connected to us. The peer list counts it on admission.
    pub fn new_live_for_incoming_connection(
        addr: SocketAddr,
        peer_id: Id20,
        tx: PeerTx,
        connection_kind: ConnectionKind,
    ) -> Self {
        Self {
            addr,
            state: PeerState::Live(LivePeerState::new(peer_id, tx, true, connection_kind)),
            stats: Default::default(),
            outgoing_address: None,
            failcount: 0,
            last_failed: None,
        }
    }

    /// A peer a tracker, DHT or PEX named, listening where it was named. The
    /// peer list counts it on admission.
    pub fn new_with_outgoing_address(addr: SocketAddr) -> Self {
        Self {
            addr,
            outgoing_address: Some(addr),
            stats: Default::default(),
            state: Default::default(),
            failcount: 0,
            last_failed: None,
        }
    }

    // The state of a peer we want and are not connected to.
    fn idle_state(&self) -> PeerState {
        if self.outgoing_address == Some(self.addr) && self.failcount < MAX_FAILCOUNT {
            PeerState::Queued
        } else {
            PeerState::Dead
        }
    }

    pub(crate) fn is_connect_candidate(&self) -> bool {
        matches!(self.state, PeerState::Queued)
    }

    /// The earliest time we may connect to the peer: `(failcount + 1)`
    /// reconnect times after its last failure. None when it never failed.
    pub(crate) fn reconnect_at(&self) -> Option<Instant> {
        let wait = MIN_RECONNECT_TIME.saturating_mul(self.failcount.saturating_add(1));
        self.last_failed.map(|at| at + wait)
    }

    /// Where the peer stands among connect candidates due together; the lowest
    /// goes first: the fewest failures in a row, then the oldest failure.
    pub(crate) fn connect_rank(&self) -> (u32, Option<Instant>) {
        (self.failcount, self.last_failed)
    }

    /// Whether the peer is a connect candidate we may connect to at `now`.
    pub(crate) fn is_due(&self, now: Instant) -> bool {
        self.is_connect_candidate() && self.reconnect_at().is_none_or(|at| at <= now)
    }

    /// The connection completed its handshake and carried a message, so the
    /// failures before it no longer count.
    pub(crate) fn delivered(&mut self) {
        self.failcount = 0;
    }

    /// Settles the peer after its connection closed. `failed_at` is set when
    /// the connection ended in an error; `needed` says whether we still want
    /// the peer.
    pub(crate) fn connection_closed(
        &mut self,
        failed_at: Option<Instant>,
        needed: bool,
        counters: &PeerStates,
    ) {
        if let Some(at) = failed_at {
            self.failcount = self.failcount.saturating_add(1);
            self.last_failed = Some(at);
        }
        let next = if needed {
            self.idle_state()
        } else {
            PeerState::NotNeeded
        };
        self.set_state(next, counters);
    }

    /// We want a peer we let go again.
    pub(crate) fn needed_again(&mut self, counters: &PeerStates) {
        if matches!(self.state, PeerState::NotNeeded) {
            self.set_state(self.idle_state(), counters);
        }
    }
}

#[derive(Debug, Default)]
pub(crate) enum PeerState {
    #[default]
    // A connect candidate: connected once a slot is free and its reconnect
    // time has come.
    Queued,
    Connecting(PeerTx),
    Live(LivePeerState),
    // Not a connect candidate: it failed MAX_FAILCOUNT times in a row, or we do
    // not know where it listens. Never connected to again.
    Dead,
    // We don't need to do anything with the peer any longer.
    // The peer has the full torrent, and we have the full torrent, so no need
    // to keep talking to it.
    NotNeeded,
}

impl std::fmt::Display for PeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl PeerState {
    pub fn name(&self) -> &'static str {
        match self {
            PeerState::Queued => "queued",
            PeerState::Connecting(_) => "connecting",
            PeerState::Live(_) => "live",
            PeerState::Dead => "dead",
            PeerState::NotNeeded => "not needed",
        }
    }

    pub fn take_live_no_counters(self) -> Option<LivePeerState> {
        match self {
            PeerState::Live(l) => Some(l),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IncomingConnectionResult {
    #[error("peer already active")]
    AlreadyActive,
}

impl Peer {
    pub fn get_state(&self) -> &PeerState {
        &self.state
    }

    pub fn take_state(&mut self, counters: &PeerStates) -> PeerState {
        self.set_state(Default::default(), counters)
    }

    pub fn destroy(self, counters: &PeerStates) {
        for counter in [&counters.session_stats, &counters.stats] {
            counter.dec(&self.state);
        }
        if let (Some(addr), PeerState::Live(..)) = (self.outgoing_address, &self.state) {
            counters.live_outgoing_peers.write().remove(&addr);
        }
    }

    pub fn set_state(&mut self, new: PeerState, counters: &PeerStates) -> PeerState {
        for counter in [&counters.session_stats, &counters.stats] {
            counter.incdec(&self.state, &new);
        }
        if let Some(addr) = self.outgoing_address {
            if matches!(&self.state, PeerState::Live(..)) {
                counters.live_outgoing_peers.write().remove(&addr);
            }
            if matches!(&new, PeerState::Live(..))
                && self
                    .stats
                    .counters
                    .outgoing_connections
                    .load(Ordering::Relaxed)
                    > 0
            {
                counters.live_outgoing_peers.write().insert(addr);
            }
        }

        std::mem::replace(&mut self.state, new)
    }

    pub fn get_live(&self) -> Option<&LivePeerState> {
        match &self.state {
            PeerState::Live(l) => Some(l),
            _ => None,
        }
    }

    pub fn get_live_mut(&mut self) -> Option<&mut LivePeerState> {
        match &mut self.state {
            PeerState::Live(l) => Some(l),
            _ => None,
        }
    }

    /// Starts connecting to a connect candidate whose reconnect time has come.
    pub fn connect_if_due(
        &mut self,
        now: Instant,
        counters: &PeerStates,
    ) -> Option<(PeerRx, PeerTx)> {
        if !self.is_due(now) {
            return None;
        }
        let (tx, rx) = unbounded_channel();
        let tx_2 = tx.clone();
        self.set_state(PeerState::Connecting(tx), counters);
        Some((rx, tx_2))
    }

    pub fn incoming_connection(
        &mut self,
        peer_id: Id20,
        tx: PeerTx,
        counters: &PeerStates,
        connection_kind: ConnectionKind,
    ) -> Result<(), IncomingConnectionResult> {
        if matches!(&self.state, PeerState::Connecting(..) | PeerState::Live(..)) {
            return Err(IncomingConnectionResult::AlreadyActive);
        }
        match self.take_state(counters) {
            PeerState::Queued | PeerState::Dead | PeerState::NotNeeded => {
                self.set_state(
                    PeerState::Live(LivePeerState::new(peer_id, tx, true, connection_kind)),
                    counters,
                );
            }
            PeerState::Connecting(..) | PeerState::Live(..) => unreachable!(),
        }
        Ok(())
    }

    pub fn connecting_to_live(
        &mut self,
        peer_id: Id20,
        counters: &PeerStates,
        conn_kind: ConnectionKind,
    ) -> Option<&mut LivePeerState> {
        if let PeerState::Connecting(_) = &self.state {
            let tx = match self.take_state(counters) {
                PeerState::Connecting(tx) => tx,
                _ => unreachable!(),
            };
            self.set_state(
                PeerState::Live(LivePeerState::new(peer_id, tx, false, conn_kind)),
                counters,
            );
            self.get_live_mut()
        } else {
            None
        }
    }

    pub fn set_not_needed(&mut self, counters: &PeerStates) -> PeerState {
        self.set_state(PeerState::NotNeeded, counters)
    }
}

#[derive(Debug)]
pub(crate) struct LivePeerState {
    #[allow(dead_code)]
    peer_id: Id20,

    pub client_name: Option<String>,

    pub peer_interested: bool,

    // This is used to track the pieces the peer has.
    pub bitfield: BF,

    // Whether the bitfield above covers the whole torrent, i.e. the peer is a seeder.
    // Cached so that the aggregate seeder counter can be maintained on transitions
    // instead of scanning every peer's bitfield on each stats call. Kept in sync
    // with the bitfield by PeerStates::update_seeder_flag.
    pub seeder: bool,

    // When the peer sends us data this is used to track if we asked for it.
    inflight_requests: HashSet<InflightRequest>,

    // Bounded tolerance for chunks that arrive after we cancel requests.
    // This is intentionally approximate: we track a count instead of storing every
    // canceled chunk, so a peer is not disconnected for racing our cancel messages.
    late_cancelled_request_tolerance: u32,

    request_slots_changed: Arc<Notify>,

    // The main channel to send requests to peer.
    pub tx: PeerTx,

    pub connection_kind: ConnectionKind,
}

impl LivePeerState {
    pub fn new(
        peer_id: Id20,
        tx: PeerTx,
        initial_interested: bool,
        connection_kind: ConnectionKind,
    ) -> Self {
        LivePeerState {
            peer_id,
            client_name: None,
            peer_interested: initial_interested,
            bitfield: BF::default(),
            seeder: false,
            inflight_requests: Default::default(),
            late_cancelled_request_tolerance: 0,
            request_slots_changed: Default::default(),
            tx,
            connection_kind,
        }
    }

    pub fn has_full_torrent(&self, total_pieces: usize) -> bool {
        self.bitfield.get(0..total_pieces).is_some_and(|s| s.all())
    }

    pub fn request_slots_changed(&self) -> Arc<Notify> {
        self.request_slots_changed.clone()
    }

    pub fn requested_inflight_count(&self) -> usize {
        self.inflight_requests.len()
    }

    pub fn add_inflight_request(&mut self, chunk: ChunkInfo) -> bool {
        self.inflight_requests.insert(chunk)
    }

    pub fn remove_inflight_request(&mut self, chunk: &ChunkInfo) -> RemoveInflightRequestResult {
        if self.inflight_requests.remove(chunk) {
            self.request_slots_changed.notify_waiters();
            return RemoveInflightRequestResult::Expected;
        }

        if self.late_cancelled_request_tolerance > 0 {
            // This may be any unexpected chunk, not necessarily the exact canceled
            // one, but the tolerance is bounded by cancels we sent.
            self.late_cancelled_request_tolerance -= 1;
            RemoveInflightRequestResult::LateCanceled
        } else {
            RemoveInflightRequestResult::Unexpected
        }
    }

    pub fn cancel_inflight_requests_for_piece(&mut self, piece: ValidPieceIndex) {
        let tx = &self.tx;
        let late_cancelled_request_tolerance = &mut self.late_cancelled_request_tolerance;
        let before = self.inflight_requests.len();
        self.inflight_requests.retain(|req| {
            if req.piece_index == piece {
                let _ = tx.send(WriterRequest::Message(Message::Cancel(Request {
                    index: piece.get(),
                    begin: req.offset,
                    length: req.size,
                })));
                *late_cancelled_request_tolerance += 1;
                false
            } else {
                true
            }
        });

        if self.inflight_requests.len() != before {
            self.request_slots_changed.notify_waiters();
        }
    }

    pub fn inflight_requests(&self) -> impl Iterator<Item = &InflightRequest> {
        self.inflight_requests.iter()
    }

    pub fn inflight_requests_debug(&self) -> &HashSet<InflightRequest> {
        &self.inflight_requests
    }
}

pub(crate) enum RemoveInflightRequestResult {
    Expected,
    LateCanceled,
    Unexpected,
}
