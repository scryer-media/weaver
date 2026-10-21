// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

pub mod stats;

use std::cmp::Reverse;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::core::hash_id::Id20;
use crate::core::lengths::{ChunkInfo, Lengths, ValidPieceIndex};
use crate::peer_protocol::{Message, Request};
use crate::sha1w::{ISha1, Sha1};

use tokio::sync::{
    Notify,
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tokio::time::Instant;

use crate::peer_connection::WriterRequest;
use crate::peer_protocol::extended::ut_pex::{PEX_HOLEPUNCH, PEX_SEED, PEX_UTP, PexPeerInfo};
use crate::stream_connect::{ConnectionKind, Transports};
use crate::type_aliases::BF;

use super::PeerStates;
use super::choker::Transfer;
use stats::atomic::PeerCountersAtomic;

pub(crate) type InflightRequest = ChunkInfo;
pub(crate) type PeerRx = UnboundedReceiver<WriterRequest>;
pub(crate) type PeerTx = UnboundedSender<WriterRequest>;

// libtorrent `min_reconnect_time`: the wait unit between connection attempts to one peer.
pub(crate) const MIN_RECONNECT_TIME: Duration = Duration::from_secs(60);
// libtorrent `max_failcount`: failures in a row that end a peer's candidacy.
pub(crate) const MAX_FAILCOUNT: u32 = 3;
// libtorrent `settings_pack::max_pex_peers`: the most peers of each address
// family a connection's PEX messages are remembered to have named, and so
// the most it adds to our peer list.
pub(crate) const MAX_PEX_PEERS: usize = 50;
// libtorrent `ut_pex_peer_plugin::m_last_pex`: a peer may send this many PEX
// messages in PEX_FLOOD_WINDOW, and the next ends the connection.
const PEX_FLOOD_MESSAGES: usize = 6;
const PEX_FLOOD_WINDOW: Duration = Duration::from_secs(60);

// libtorrent `settings_pack::allowed_fast_set_size`: how many pieces a peer
// may fetch while we choke it.
pub(crate) const ALLOWED_FAST_SET_SIZE: u32 = 5;

// libtorrent `settings_pack::max_suggest_pieces`: how many of a peer's
// suggestions are kept.
const MAX_SUGGEST_PIECES: usize = 16;

/// libtorrent `settings_pack::max_allowed_in_request_queue`: the most
/// blocks a peer may have asked us for and not been sent. Our extended
/// handshake advertises it as `reqq`.
pub(crate) const MAX_ALLOWED_IN_REQUEST_QUEUE: u32 = 2000;

// libtorrent `settings_pack::request_queue_time`: the requests outstanding to
// a peer cover this long of its delivery.
const REQUEST_QUEUE_TIME: Duration = Duration::from_secs(3);
// libtorrent `min_request_queue`: the fewest requests outstanding to a peer
// that is not snubbed.
const MIN_REQUEST_QUEUE: usize = 2;
// libtorrent `peer_connection::m_desired_queue_size`'s first value: the
// requests outstanding to a peer before any block has arrived from it.
const INITIAL_REQUEST_QUEUE: usize = 4;

// libtorrent `peer_connection::second_tick`: how long a seed waits on an
// interested peer it unchoked to ask for a block, or to be sent the last one
// it asked for, before it ends the connection.
pub(crate) const NO_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

// libtorrent `inactivity_timeout`: how long two peers that want nothing of
// each other stay connected while connections run short.
pub(crate) const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(600);

/// Why a connection has gone idle long enough to end
/// (libtorrent `peer_connection::second_tick`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimedOut {
    /// We seed, the peer is interested and unchoked, and it has asked for
    /// nothing and been sent nothing for [`NO_REQUEST_TIMEOUT`]
    /// (`timed_out_no_request`).
    NoRequest,
    /// Neither of us has wanted anything of the other for
    /// [`INACTIVITY_TIMEOUT`], and connections run short
    /// (`timed_out_no_interest`).
    NoInterest,
}

/// BEP 6's allowed fast set of `k` of `num_pieces` pieces for the peer at
/// `ip` on the torrent `info_hash`, in the order it is generated; every piece
/// when `k` is not less than their count (libtorrent
/// `peer_connection::send_allowed_set`).
///
/// An IPv4 address counts only its /24, as BEP 6 says, so the peers of one
/// network get one set; libtorrent hashes the whole address. An IPv6 address
/// counts whole, as libtorrent hashes it.
pub(crate) fn allowed_fast_set(ip: IpAddr, info_hash: Id20, num_pieces: u32, k: u32) -> Vec<u32> {
    if k >= num_pieces {
        return (0..num_pieces).collect();
    }
    let mut x = Vec::with_capacity(16 + 20);
    match ip.to_canonical() {
        IpAddr::V4(v4) => x.extend_from_slice(&(u32::from(v4) & 0xffff_ff00).to_be_bytes()),
        IpAddr::V6(v6) => x.extend_from_slice(&v6.octets()),
    }
    x.extend_from_slice(&info_hash.0);
    let sha1 = |data: &[u8]| {
        let mut h = Sha1::new();
        h.update(data);
        h.finish()
    };
    let mut hash = sha1(&x);
    let mut set = Vec::with_capacity(k as usize);
    // libtorrent's safety net: the loop ends even when the hashes keep
    // naming pieces already in the set.
    let mut words = 0;
    loop {
        for word in hash.chunks_exact(4) {
            let piece = u32::from_be_bytes(word.try_into().unwrap()) % num_pieces;
            if !set.contains(&piece) {
                set.push(piece);
                if set.len() == k as usize {
                    return set;
                }
            }
            words += 1;
            if words > 500 {
                return set;
            }
        }
        hash = sha1(&hash);
    }
}

/// What the pieces a peer took part in say of it, kept across its
/// connections (libtorrent `torrent_peer::trust_points`, `hashfails`,
/// `on_parole`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Trust {
    points: i8,
    hashfails: u8,
    on_parole: bool,
}

impl Trust {
    // libtorrent `torrent_peer::trust_points` is a 4-bit field: [-7, 8].
    pub(crate) const MIN_POINTS: i8 = -7;
    pub(crate) const MAX_POINTS: i8 = 8;

    pub(crate) fn points(&self) -> i8 {
        self.points
    }

    pub(crate) fn hashfails(&self) -> u8 {
        self.hashfails
    }

    pub(crate) fn on_parole(&self) -> bool {
        self.on_parole
    }

    /// A piece the peer sent a block of passed: a point gained, up to the
    /// most, and parole ends (libtorrent `torrent::piece_passed`).
    fn piece_passed(&mut self) {
        self.on_parole = false;
        self.points = (self.points + 1).min(Self::MAX_POINTS);
    }

    /// A piece the peer sent a block of failed: two points lost, down to the
    /// least, as failing costs more than passing earns; one more hash failure;
    /// and parole (libtorrent `torrent::penalize_peers`, with
    /// `use_parole_mode` at its default, on). Returns whether the peer is to
    /// be banned: it has lost every point it can, or it sent the whole piece.
    fn piece_failed(&mut self, sole_sender: bool) -> bool {
        self.on_parole = true;
        self.points = self.points.saturating_sub(2).max(Self::MIN_POINTS);
        self.hashfails = self.hashfails.saturating_add(1);
        self.points <= Self::MIN_POINTS || sole_sender
    }
}

/// Where a peer that connected to us from `addr` listens, when its transport
/// says: uTP runs over the peer's listening UDP socket, while TCP connects from
/// an ephemeral port.
pub(crate) fn listen_address(addr: SocketAddr, kind: ConnectionKind) -> Option<SocketAddr> {
    match kind {
        ConnectionKind::Utp => Some(addr),
        ConnectionKind::Tcp => None,
    }
}

/// A peer bitfield holding no piece, shaped as BITFIELD carries one: a bit
/// per piece, padded to whole bytes.
pub(crate) fn empty_bitfield(total_pieces: u32) -> BF {
    BF::from_boxed_slice(vec![0; total_pieces.div_ceil(8) as usize].into_boxed_slice())
}

/// The pieces a BITFIELD message says the peer has (BEP 3): one bit per
/// piece, padded to whole bytes. A payload of any other length ends the
/// connection, and the padding bits are cleared here, so no reader of a peer
/// bitfield sees a piece past the torrent's last.
pub(crate) fn ingest_bitfield(payload: &[u8], total_pieces: u32) -> anyhow::Result<BF> {
    let mut bitfield = empty_bitfield(total_pieces);
    if payload.len() != bitfield.as_raw_slice().len() {
        anyhow::bail!(
            "bitfield of {} bytes, expected {} for {total_pieces} pieces",
            payload.len(),
            bitfield.as_raw_slice().len(),
        );
    }
    bitfield.as_raw_mut_slice().copy_from_slice(payload);
    bitfield[total_pieces as usize..].fill(false);
    Ok(bitfield)
}

/// Where a peer stands when a full peer list must forget one; the lowest goes
/// first. Peers we would not connect to go before connect candidates, then the
/// most failures in a row, then the oldest failure.
pub(crate) type EraseRank = (bool, Reverse<u32>, Option<Instant>);

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
    // When the choker last gave the peer the optimistic slot, on any
    // connection to it.
    pub last_optimistic_unchoke: Option<Instant>,
    // What the pieces it took part in say of it.
    trust: Trust,
    // A PEX message said the peer speaks the holepunch extension
    // (libtorrent `torrent_peer::supports_holepunch`).
    pub supports_holepunch: bool,
    // A holepunch introduced us: we connect to the peer now, whatever its
    // failures, until the connection closes (libtorrent
    // `peer_connection::set_holepunch_mode`, `torrent::connect_to_peer`).
    holepunch: bool,
}

impl Peer {
    /// A peer that connected to us. The peer list counts it on admission.
    pub fn new_live_for_incoming_connection(
        addr: SocketAddr,
        peer_id: Id20,
        tx: PeerTx,
        connection_kind: ConnectionKind,
    ) -> Self {
        let stats = stats::atomic::PeerStats::default();
        Self {
            addr,
            state: PeerState::Live(LivePeerState::new(
                peer_id,
                tx,
                connection_kind,
                &stats.counters,
            )),
            stats,
            outgoing_address: listen_address(addr, connection_kind),
            failcount: 0,
            last_failed: None,
            last_optimistic_unchoke: None,
            trust: Trust::default(),
            supports_holepunch: false,
            holepunch: false,
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
            last_optimistic_unchoke: None,
            trust: Trust::default(),
            supports_holepunch: false,
            holepunch: false,
        }
    }

    /// A peer a holepunch introduced us to, at `addr`. The peer list counts
    /// it on admission.
    pub fn new_holepunched(addr: SocketAddr) -> Self {
        Self {
            holepunch: true,
            ..Self::new_with_outgoing_address(addr)
        }
    }

    // The state of a peer we want and are not connected to. A banned peer is
    // no connect candidate (libtorrent `peer_list::is_connect_candidate`); a
    // holepunched one is, whatever its failures.
    fn idle_state(&self, peers: &PeerStates) -> PeerState {
        if self.outgoing_address == Some(self.addr)
            && (self.failcount < MAX_FAILCOUNT || self.holepunch)
            && !peers.is_banned(self.addr.ip())
        {
            PeerState::Queued
        } else {
            PeerState::Dead
        }
    }

    pub(crate) fn is_connect_candidate(&self) -> bool {
        matches!(self.state, PeerState::Queued)
    }

    /// The earliest time we may connect to the peer: `(failcount + 1)`
    /// reconnect times after its last failure. None when it never failed,
    /// or a holepunch introduced it.
    pub(crate) fn reconnect_at(&self) -> Option<Instant> {
        let wait = MIN_RECONNECT_TIME.saturating_mul(self.failcount.saturating_add(1));
        self.last_failed
            .filter(|_| !self.holepunch)
            .map(|at| at + wait)
    }

    /// Where the peer stands among connect candidates due together; the lowest
    /// goes first: a holepunched peer, then the fewest failures in a row,
    /// then the oldest failure.
    pub(crate) fn connect_rank(&self) -> (bool, u32, Option<Instant>) {
        (!self.holepunch, self.failcount, self.last_failed)
    }

    /// Whether a holepunch introduced the peer and the connection it asked
    /// for has not closed.
    pub(crate) fn holepunched(&self) -> bool {
        self.holepunch
    }

    /// What a connection to the peer may go over: uTP alone while a
    /// holepunch introduced it (libtorrent `bt_peer_connection::on_holepunch`
    /// marks it `supports_utp`, so `torrent::connect_to_peer` dials it
    /// through the uTP socket manager).
    pub(crate) fn transports(&self) -> Transports {
        if self.holepunch {
            Transports::Utp
        } else {
            Transports::Any
        }
    }

    /// A holepunch introduced the peer: unless we are connected or
    /// connecting to it, we connect to it at `addr` next, whatever its
    /// failures (libtorrent `bt_peer_connection::on_holepunch`, `connect`).
    /// Returns whether it is a connect candidate now.
    pub(crate) fn holepunch(&mut self, peers: &PeerStates) -> bool {
        if matches!(self.state, PeerState::Connecting(_) | PeerState::Live(_)) {
            return false;
        }
        self.holepunch = true;
        self.outgoing_address = Some(self.addr);
        self.set_state(self.idle_state(peers), peers);
        self.is_connect_candidate()
    }

    /// Whether the peer is a connect candidate we may connect to at `now`.
    pub(crate) fn is_due(&self, now: Instant) -> bool {
        self.is_connect_candidate() && self.reconnect_at().is_none_or(|at| at <= now)
    }

    /// Someone named the peer again: a failure is forgiven, which revives a
    /// peer that had run out of them.
    pub(crate) fn rediscovered(&mut self, counters: &PeerStates) {
        self.failcount = self.failcount.saturating_sub(1);
        if matches!(self.state, PeerState::Dead) {
            self.set_state(self.idle_state(counters), counters);
        }
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
        self.holepunch = false;
        let next = if needed {
            self.idle_state(counters)
        } else {
            PeerState::NotNeeded
        };
        self.set_state(next, counters);
    }

    /// We want a peer we let go again.
    pub(crate) fn needed_again(&mut self, counters: &PeerStates) {
        if matches!(self.state, PeerState::NotNeeded) {
            self.set_state(self.idle_state(counters), counters);
        }
    }

    /// The peer's address was banned: a connection to it is closed, which
    /// leaves it Dead, and a connect candidate is one no longer
    /// (libtorrent `peer_list::ban_peer`, and the disconnect in
    /// `torrent::penalize_peers`).
    pub(crate) fn ban(&mut self, peers: &PeerStates) {
        let tx = match &self.state {
            PeerState::Connecting(tx) => tx,
            PeerState::Live(live) => &live.tx,
            PeerState::Queued => {
                self.set_state(self.idle_state(peers), peers);
                return;
            }
            PeerState::Dead | PeerState::NotNeeded => return,
        };
        let _ = tx.send(WriterRequest::Disconnect(Err(anyhow::anyhow!(
            "the peer is banned"
        ))));
    }

    pub(crate) fn trust(&self) -> Trust {
        self.trust
    }

    /// A piece the peer sent a block of passed its hash check.
    pub(crate) fn piece_passed(&mut self) {
        self.trust.piece_passed();
    }

    /// A piece the peer sent a block of failed its hash check, and the peer
    /// sent all of it when `sole_sender` holds. Returns whether the peer is
    /// to be banned.
    pub(crate) fn piece_failed(&mut self, sole_sender: bool) -> bool {
        self.trust.piece_failed(sole_sender)
    }

    /// The peer as a pause leaves it on the list: where it listens, its
    /// failures, trust and holepunch support, without its connection or
    /// this run's counters. Holepunch mode ends with the connection
    /// (libtorrent `torrent::do_pause` disconnects every peer, and its
    /// `torrent_peer` stays on `torrent::m_peer_list`).
    pub(crate) fn idle(&self) -> Peer {
        Peer {
            addr: self.addr,
            state: PeerState::Dead,
            stats: Default::default(),
            outgoing_address: self.outgoing_address,
            failcount: self.failcount,
            last_failed: self.last_failed,
            last_optimistic_unchoke: self.last_optimistic_unchoke,
            trust: self.trust,
            supports_holepunch: self.supports_holepunch,
            holepunch: false,
        }
    }

    /// An idle peer joins the list of a new run: a connect candidate or
    /// not, as the list says now.
    pub(crate) fn rejoin(&mut self, peers: &PeerStates) {
        peers.count_in(&self.state);
        self.set_state(self.idle_state(peers), peers);
    }

    /// Where the peer stands when a full list must forget one; None for a
    /// connected peer, which is never forgotten.
    pub(crate) fn erase_rank(&self) -> Option<EraseRank> {
        let candidate = match self.state {
            PeerState::Connecting(_) | PeerState::Live(_) => return None,
            PeerState::Queued => true,
            PeerState::Dead | PeerState::NotNeeded => false,
        };
        Some((candidate, Reverse(self.failcount), self.last_failed))
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
    // not know where it listens. Kept until someone names it again or the list
    // needs the room.
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
        counters.count_out(&self.state);
    }

    pub fn set_state(&mut self, new: PeerState, counters: &PeerStates) -> PeerState {
        counters.count_out(&self.state);
        counters.count_in(&new);
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
                let live = LivePeerState::new(peer_id, tx, connection_kind, &self.stats.counters);
                self.set_state(PeerState::Live(live), counters);
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
            let live = LivePeerState::new(peer_id, tx, conn_kind, &self.stats.counters);
            self.set_state(PeerState::Live(live), counters);
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

    // Whether the peer wants to download from us. Like every connection it
    // starts not interested (BEP 3); the choker records the peer's word.
    pub peer_interested: bool,
    // When the peer last said it is no longer interested; the time the
    // connection went live until it does (libtorrent
    // `m_became_uninterested`).
    became_uninterested: Instant,
    // Whether we want to download from the peer, as we last told it.
    interesting: bool,
    // When we last told the peer we are no longer interested; the time the
    // connection went live until we do (libtorrent
    // `m_became_uninteresting`).
    became_uninteresting: Instant,
    // When we last queued a block the peer asked for, or took one off its
    // queue to send; the time the connection went live until we do
    // (libtorrent `m_last_incoming_request`, `m_last_sent_payload`).
    last_upload: Instant,

    // When we choked the peer; None while we do not. It starts choked when
    // the connection goes live (BEP 3), and only the choker unchokes it.
    choked_at: Option<Instant>,
    // When we last unchoked the peer, the time the connection went live
    // until we do, and what we had uploaded to it by then.
    last_unchoke: Instant,
    uploaded_at_unchoke: u64,
    // What the peer had sent us and we had sent it when the choker's current
    // round began.
    round_start: Transfer,

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

    // When we began waiting for the peer's next block: the request that found
    // none outstanding, or the last requested block to arrive. Read only while
    // requests are outstanding.
    waiting_since: Instant,

    // How long the peer takes to deliver a block, which sets its request timeout.
    block_time: BlockTime,

    // The peer went its request timeout without delivering a block. Until a
    // block it was asked for arrives, it may hold one request and takes no
    // piece from another peer.
    snubbed: bool,

    request_slots_changed: Arc<Notify>,

    // The blocks the peer asked us for that we have not sent yet. The writer
    // sends a block only by taking it from here, so a Cancel or a Choke that
    // removes it first keeps it from going out.
    upload_queue: HashSet<ChunkInfo>,

    // The requests the peer sent that we could not serve since we last
    // choked it (libtorrent `peer_connection::m_num_invalid_requests`).
    invalid_requests: u16,

    // While we super-seed, the one piece we told the peer we have
    // (libtorrent `peer_connection::m_superseed_piece`).
    pub super_seeded: Option<ValidPieceIndex>,

    // The peer's extended handshake names ut_holepunch (libtorrent
    // `bt_peer_connection::m_holepunch_id`).
    pub supports_holepunch: bool,

    // The peers its PEX messages named and have not dropped since, at most
    // MAX_PEX_PEERS of each address family (libtorrent `ut_pex_plugin`'s
    // `m_peers`, `m_peers6`).
    pex_named: HashSet<SocketAddr>,

    // When its last PEX_FLOOD_MESSAGES PEX messages arrived, the oldest
    // first (libtorrent `ut_pex_peer_plugin::m_last_pex`).
    pex_arrivals: [Option<Instant>; PEX_FLOOD_MESSAGES],

    // The port of the DHT node the peer named on this connection (BEP 5
    // PORT).
    dht_port: Option<u16>,

    // The main channel to send requests to peer.
    pub tx: PeerTx,

    pub connection_kind: ConnectionKind,

    // The peer set the fast bit in its handshake, as we always do, so the
    // connection speaks the fast extension (BEP 6; libtorrent
    // `bt_peer_connection::m_supports_fast`).
    pub fast: bool,

    // The pieces we let the peer fetch while we choke it, each with how many
    // blocks of it the peer asked for (libtorrent `m_accept_fast`,
    // `m_accept_fast_piece_cnt`). None until we send them, at its first
    // Interested.
    allowed_fast_to_peer: Option<Vec<(ValidPieceIndex, u32)>>,

    // The pieces the peer lets us fetch while it chokes us (libtorrent
    // `m_allowed_fast`).
    pub allowed_fast_from_peer: Vec<ValidPieceIndex>,

    // The pieces the peer suggested, the latest first (libtorrent
    // `m_suggested_pieces`).
    pub suggested: Vec<ValidPieceIndex>,
}

/// Why a request the peer sent us is not queued.
pub(crate) enum Refused {
    /// We choke the peer, since this instant, and it may not fetch the piece.
    Choked(Instant),
    /// We choke the peer, and it asked for the blocks of a piece it may fetch
    /// three times over.
    AllowedFastSpent,
    /// The peer has [`MAX_ALLOWED_IN_REQUEST_QUEUE`] blocks queued already.
    QueueFull,
    /// The block is queued already, and goes out once.
    Queued,
}

impl LivePeerState {
    /// A connection that just went live, to a peer with `counters`.
    pub fn new(
        peer_id: Id20,
        tx: PeerTx,
        connection_kind: ConnectionKind,
        counters: &PeerCountersAtomic,
    ) -> Self {
        let now = Instant::now();
        LivePeerState {
            peer_id,
            client_name: None,
            peer_interested: false,
            became_uninterested: now,
            interesting: false,
            became_uninteresting: now,
            last_upload: now,
            choked_at: Some(now),
            last_unchoke: now,
            uploaded_at_unchoke: 0,
            round_start: Transfer::of(counters),
            bitfield: BF::default(),
            seeder: false,
            inflight_requests: Default::default(),
            late_cancelled_request_tolerance: 0,
            waiting_since: Instant::now(),
            block_time: BlockTime::default(),
            snubbed: false,
            request_slots_changed: Default::default(),
            upload_queue: Default::default(),
            invalid_requests: 0,
            super_seeded: None,
            supports_holepunch: false,
            pex_named: HashSet::new(),
            pex_arrivals: [None; PEX_FLOOD_MESSAGES],
            dht_port: None,
            tx,
            connection_kind,
            fast: false,
            allowed_fast_to_peer: None,
            allowed_fast_from_peer: Vec::new(),
            suggested: Vec::new(),
        }
    }

    /// The peer named its DHT node at `port` (BEP 5 PORT). Whether that is
    /// news: the port it named last names nothing new, another replaces it.
    pub fn dht_node_named(&mut self, port: u16) -> bool {
        self.dht_port.replace(port) != Some(port)
    }

    /// A PEX message from the peer arrived at `now`. False when it is one
    /// more than [`PEX_FLOOD_MESSAGES`] in [`PEX_FLOOD_WINDOW`]: the peer
    /// floods us (libtorrent `ut_pex_peer_plugin::on_extended`,
    /// `too_frequent_pex`).
    pub fn pex_arrived(&mut self, now: Instant) -> bool {
        if self.pex_arrivals[0].is_some_and(|oldest| now - oldest < PEX_FLOOD_WINDOW) {
            return false;
        }
        self.pex_arrivals.rotate_left(1);
        self.pex_arrivals[PEX_FLOOD_MESSAGES - 1] = Some(now);
        true
    }

    /// A PEX message from the peer named `added` and dropped `dropped`
    /// (libtorrent `ut_pex_peer_plugin::on_extended`): the dropped go, then
    /// the added come while their family has room. Returns the added it had
    /// not named before, the ones that are news.
    pub fn pex_named(
        &mut self,
        added: impl Iterator<Item = PexPeerInfo>,
        dropped: impl Iterator<Item = SocketAddr>,
    ) -> Vec<PexPeerInfo> {
        for addr in dropped {
            self.pex_named.remove(&addr);
        }
        let mut v4 = self.pex_named.iter().filter(|a| a.is_ipv4()).count();
        let mut v6 = self.pex_named.len() - v4;
        added
            .filter(|peer| {
                let family = if peer.addr.is_ipv4() {
                    &mut v4
                } else {
                    &mut v6
                };
                let new = *family < MAX_PEX_PEERS && self.pex_named.insert(peer.addr);
                *family += usize::from(new);
                new
            })
            .collect()
    }

    /// What our PEX messages say of the peer: a seed, connected over uTP,
    /// speaking the holepunch extension (libtorrent `ut_pex_plugin::tick`).
    /// Encryption is not flagged: the peer's state does not hold whether its
    /// connection is encrypted.
    pub fn advertised_pex_flags(&self) -> u8 {
        let mut flags = 0;
        if self.seeder {
            flags |= PEX_SEED;
        }
        if matches!(self.connection_kind, ConnectionKind::Utp) {
            flags |= PEX_UTP;
        }
        if self.supports_holepunch {
            flags |= PEX_HOLEPUNCH;
        }
        flags
    }

    /// Whether the peer's PEX messages named `addr` and have not dropped it
    /// (libtorrent `ut_pex_peer_store::was_introduced_by`).
    pub fn was_introduced(&self, addr: SocketAddr) -> bool {
        self.pex_named.contains(&addr)
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

    pub fn is_snubbed(&self) -> bool {
        self.snubbed
    }

    /// The peer went its request timeout without delivering a block.
    pub fn snub(&mut self) {
        self.snubbed = true;
    }

    /// How many requests to keep outstanding to the peer, at most `max`
    /// (libtorrent `peer_connection::update_desired_queue_size`): one while
    /// it is snubbed; else the blocks it delivers in [`REQUEST_QUEUE_TIME`]
    /// at the pace it has delivered them, at least [`MIN_REQUEST_QUEUE`];
    /// [`INITIAL_REQUEST_QUEUE`] until a block has arrived.
    pub fn desired_queue_size(&self, max: usize) -> usize {
        if self.snubbed {
            return 1;
        }
        self.block_time
            .blocks_in(REQUEST_QUEUE_TIME)
            .unwrap_or(INITIAL_REQUEST_QUEUE)
            .max(MIN_REQUEST_QUEUE)
            .min(max)
    }

    /// Whether the peer may hold another request, at most `max` in all.
    pub fn can_hold_another_request(&self, max: usize) -> bool {
        self.requested_inflight_count() < self.desired_queue_size(max)
    }

    pub fn add_inflight_request(&mut self, chunk: ChunkInfo) -> bool {
        let none_outstanding = self.inflight_requests.is_empty();
        let added = self.inflight_requests.insert(chunk);
        if added && none_outstanding {
            // The wait for a block starts now, and the request timeout with it.
            self.waiting_since = Instant::now();
            self.request_slots_changed.notify_waiters();
        }
        added
    }

    /// How long the peer may take to answer a request.
    pub fn request_timeout(&self) -> Duration {
        self.block_time.request_timeout()
    }

    /// When the requests outstanding to the peer time out: the peer has gone
    /// its request timeout without delivering a block. None while none are
    /// outstanding.
    pub fn request_deadline(&self) -> Option<Instant> {
        (!self.inflight_requests.is_empty())
            .then(|| self.waiting_since + self.block_time.request_timeout())
    }

    pub fn remove_inflight_request(&mut self, chunk: &ChunkInfo) -> RemoveInflightRequestResult {
        if self.inflight_requests.remove(chunk) {
            let now = Instant::now();
            self.block_time.add(now - self.waiting_since);
            self.waiting_since = now;
            self.snubbed = false;
            self.request_slots_changed.notify_waiters();
            return RemoveInflightRequestResult::Expected;
        }
        self.answer_to_no_request()
    }

    /// The peer rejected our request for `chunk` (BEP 6). That answers the
    /// request as a block would, but says nothing of how fast the peer
    /// delivers.
    pub fn reject_inflight_request(&mut self, chunk: &ChunkInfo) -> RemoveInflightRequestResult {
        if self.inflight_requests.remove(chunk) {
            self.request_slots_changed.notify_waiters();
            return RemoveInflightRequestResult::Expected;
        }
        self.answer_to_no_request()
    }

    /// The peer answered a request that is not outstanding: late when we had
    /// stopped waiting for it, unexpected when we never sent it.
    fn answer_to_no_request(&mut self) -> RemoveInflightRequestResult {
        if self.late_cancelled_request_tolerance > 0 {
            // This may be any unexpected chunk, not necessarily the exact canceled
            // one, but the tolerance is bounded by cancels we sent.
            self.late_cancelled_request_tolerance -= 1;
            RemoveInflightRequestResult::LateCanceled
        } else {
            RemoveInflightRequestResult::Unexpected
        }
    }

    /// Stops waiting for `chunk`, which arrived from another peer it was
    /// asked of in the end game. The peer is told; a copy already on its
    /// way is let through as late (libtorrent `peer_connection::cancel_request`).
    pub fn cancel_inflight_request(&mut self, chunk: &ChunkInfo) {
        if self.inflight_requests.remove(chunk) {
            let _ = self
                .tx
                .send(WriterRequest::Message(Message::Cancel(wire_request(chunk))));
            self.late_cancelled_request_tolerance += 1;
            self.request_slots_changed.notify_waiters();
        }
    }

    pub fn cancel_inflight_requests_for_piece(&mut self, piece: ValidPieceIndex) {
        let tx = &self.tx;
        let late_cancelled_request_tolerance = &mut self.late_cancelled_request_tolerance;
        let before = self.inflight_requests.len();
        self.inflight_requests.retain(|req| {
            if req.piece_index == piece {
                let _ = tx.send(WriterRequest::Message(Message::Cancel(wire_request(req))));
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

    /// Stops waiting for every request outstanding to the peer and returns
    /// them. A block the peer already had on its way is let through as late.
    pub fn take_inflight_requests(&mut self) -> Vec<InflightRequest> {
        let taken: Vec<_> = self.inflight_requests.drain().collect();
        self.late_cancelled_request_tolerance = self
            .late_cancelled_request_tolerance
            .saturating_add(u32::try_from(taken.len()).unwrap_or(u32::MAX));
        if !taken.is_empty() {
            self.request_slots_changed.notify_waiters();
        }
        taken
    }

    pub fn inflight_requests(&self) -> impl Iterator<Item = &InflightRequest> {
        self.inflight_requests.iter()
    }

    pub fn inflight_requests_debug(&self) -> &HashSet<InflightRequest> {
        &self.inflight_requests
    }

    /// The peer asked us for `chunk`, a block of a piece of `chunks_per_piece`
    /// blocks (libtorrent `peer_connection::incoming_request`). A block
    /// queued already stays queued once. Past [`MAX_ALLOWED_IN_REQUEST_QUEUE`]
    /// queued blocks, any other is refused. While we choke the peer only a
    /// block of a piece it may fetch is queued, until it has asked for three
    /// times the piece's blocks; any other is refused. A refused block gets a
    /// Reject on a fast connection.
    pub fn queue_upload(&mut self, chunk: ChunkInfo, chunks_per_piece: u32) -> Result<(), Refused> {
        if self.upload_queue.contains(&chunk) {
            return Err(Refused::Queued);
        }
        if self.upload_queue.len() >= MAX_ALLOWED_IN_REQUEST_QUEUE as usize {
            self.reject(&chunk);
            return Err(Refused::QueueFull);
        }
        let allowed = self
            .allowed_fast_to_peer
            .iter_mut()
            .flatten()
            .find(|(piece, _)| *piece == chunk.piece_index)
            .map(|(_, asked)| asked);
        match (self.choked_at, allowed) {
            (Some(_), Some(asked)) if *asked >= 3 * chunks_per_piece => {
                return Err(Refused::AllowedFastSpent);
            }
            (Some(choked_at), None) => {
                self.reject(&chunk);
                return Err(Refused::Choked(choked_at));
            }
            (_, Some(asked)) => *asked += 1,
            (None, None) => {}
        }
        self.upload_queue.insert(chunk);
        self.last_upload = Instant::now();
        Ok(())
    }

    /// The peer sent a request we cannot serve, which is dropped; with
    /// `peer_chokes_us` when it chokes us. Every tenth while it is not
    /// interested and we choke it, it is told again that we do; past 300 so,
    /// while it does not choke us, it is to be disconnected: false then
    /// (libtorrent `peer_connection::incoming_request`).
    pub fn invalid_request(&mut self, peer_chokes_us: bool) -> bool {
        self.invalid_requests = self.invalid_requests.saturating_add(1);
        if self.peer_interested || !self.is_choked() || self.invalid_requests % 10 != 0 {
            return true;
        }
        if self.invalid_requests > 300 && !peer_chokes_us {
            return false;
        }
        let _ = self.tx.send(WriterRequest::Message(Message::Choke));
        true
    }

    /// The peer suggests `piece`. The latest suggestion goes first, and the
    /// oldest is dropped past [`MAX_SUGGEST_PIECES`] (libtorrent
    /// `peer_connection::incoming_suggest`). A piece suggested again moves
    /// to the front.
    pub fn suggest(&mut self, piece: ValidPieceIndex) {
        self.suggested.retain(|p| *p != piece);
        self.suggested.truncate(MAX_SUGGEST_PIECES - 1);
        self.suggested.insert(0, piece);
    }

    /// Whether the peer may fetch `piece` while we choke it.
    fn allows_fast(&self, piece: ValidPieceIndex) -> bool {
        self.allowed_fast_to_peer
            .iter()
            .flatten()
            .any(|(allowed, _)| *allowed == piece)
    }

    /// The peer said it is interested. On a fast connection it is sent, the
    /// first time, the pieces it may fetch while we choke it: the allowed
    /// fast set less the pieces it has (libtorrent
    /// `bt_peer_connection::on_interested`,
    /// `peer_connection::send_allowed_set`). The peer at `ip` downloads the
    /// torrent `info_hash` of `lengths`.
    pub fn send_allowed_fast(&mut self, ip: IpAddr, info_hash: Id20, lengths: &Lengths) {
        if !self.fast || self.allowed_fast_to_peer.is_some() {
            return;
        }
        let set: Vec<ValidPieceIndex> =
            allowed_fast_set(ip, info_hash, lengths.total_pieces(), ALLOWED_FAST_SET_SIZE)
                .into_iter()
                .filter_map(|piece| lengths.validate_piece_index(piece))
                .filter(|piece| self.bitfield.get(piece.get_usize()).is_none_or(|has| !*has))
                .collect();
        for piece in &set {
            let _ = self
                .tx
                .send(WriterRequest::Message(Message::AllowedFast(piece.get())));
        }
        self.allowed_fast_to_peer = Some(set.into_iter().map(|piece| (piece, 0)).collect());
    }

    /// Whether we choke the peer.
    pub fn is_choked(&self) -> bool {
        self.choked_at.is_some()
    }

    /// When we last unchoked the peer; when the connection went live if we
    /// never did.
    pub fn last_unchoke(&self) -> Instant {
        self.last_unchoke
    }

    /// What we have uploaded to the peer since we last unchoked it, given
    /// that we have uploaded `uploaded` to it in all.
    pub fn uploaded_since_unchoke(&self, uploaded: u64) -> u64 {
        uploaded.saturating_sub(self.uploaded_at_unchoke)
    }

    /// Chokes the peer at `now`: a Choke goes out and every block it asked
    /// for and was not sent is dropped (BEP 3), each with a Reject on a fast
    /// connection, but for the blocks of the pieces it may fetch while choked
    /// (libtorrent `peer_connection::send_choke`). False when it was choked
    /// already.
    pub(crate) fn choke(&mut self, now: Instant) -> bool {
        if self.is_choked() {
            return false;
        }
        self.choked_at = Some(now);
        self.invalid_requests = 0;
        let _ = self.tx.send(WriterRequest::Message(Message::Choke));
        let (kept, dropped): (HashSet<_>, HashSet<_>) = std::mem::take(&mut self.upload_queue)
            .into_iter()
            .partition(|chunk| self.allows_fast(chunk.piece_index));
        self.upload_queue = kept;
        for chunk in dropped {
            self.reject(&chunk);
        }
        true
    }

    /// Tells a fast peer we will not send the block `chunk`. A plain
    /// connection has no word for it (libtorrent
    /// `bt_peer_connection::write_reject_request`).
    fn reject(&self, chunk: &ChunkInfo) {
        if self.fast {
            let _ = self
                .tx
                .send(WriterRequest::Message(Message::Reject(wire_request(chunk))));
        }
    }

    /// Unchokes the peer at `now`, when we have uploaded `uploaded` to it in
    /// all. False when it was unchoked already.
    pub(crate) fn unchoke(&mut self, now: Instant, uploaded: u64) -> bool {
        if !self.is_choked() {
            return false;
        }
        self.choked_at = None;
        self.last_unchoke = now;
        self.uploaded_at_unchoke = uploaded;
        let _ = self.tx.send(WriterRequest::Message(Message::Unchoke));
        true
    }

    /// Tells the peer again that we do not choke it.
    pub(crate) fn repeat_unchoke(&self) {
        let _ = self.tx.send(WriterRequest::Message(Message::Unchoke));
    }

    /// Ends the choker's round for the peer, whose totals are now `totals`,
    /// and starts the next. Returns what moved during the round that ended.
    pub(crate) fn end_round(&mut self, totals: Transfer) -> Transfer {
        let round = totals.since(self.round_start);
        self.round_start = totals;
        round
    }

    /// The peer cancelled its request for `chunk`. A block already sent is
    /// not in the queue, and stays sent; one still queued is dropped, with a
    /// Reject on a fast connection (libtorrent
    /// `peer_connection::incoming_cancel`).
    pub fn cancel_upload(&mut self, chunk: &ChunkInfo) {
        if self.upload_queue.remove(chunk) {
            self.reject(chunk);
        }
    }

    /// Whether the peer still wants `chunk` and has not been sent it.
    pub fn is_upload_queued(&self, chunk: &ChunkInfo) -> bool {
        self.upload_queue.contains(chunk)
    }

    /// Takes `chunk` off the queue to send it. False when it is no longer
    /// there, and it is not sent.
    pub fn claim_upload(&mut self, chunk: &ChunkInfo) -> bool {
        let claimed = self.upload_queue.remove(chunk);
        if claimed {
            self.last_upload = Instant::now();
        }
        claimed
    }

    /// The peer said at `now` whether it is interested
    /// (libtorrent `peer_connection::incoming_interested`,
    /// `incoming_not_interested`).
    pub(crate) fn peer_interest(&mut self, interested: bool, now: Instant) {
        if self.peer_interested && !interested {
            self.became_uninterested = now;
        }
        self.peer_interested = interested;
    }

    /// We want to download from the peer, or no longer do, at `now`. True
    /// when that changed, and the peer is to be told
    /// (libtorrent `peer_connection::send_interested`,
    /// `send_not_interested`).
    pub(crate) fn interest(&mut self, interesting: bool, now: Instant) -> bool {
        if self.interesting == interesting {
            return false;
        }
        if !interesting {
            self.became_uninteresting = now;
        }
        self.interesting = interesting;
        true
    }

    /// Whether the connection is idle enough at `now` to end, with
    /// `upload_only` when we only seed the torrent and `full` when
    /// connections run short (libtorrent `peer_connection::second_tick`).
    pub(crate) fn timed_out(
        &self,
        now: Instant,
        upload_only: bool,
        full: bool,
    ) -> Option<TimedOut> {
        let served = self.last_unchoke.max(self.last_upload);
        if upload_only
            && self.peer_interested
            && !self.is_choked()
            && self.upload_queue.is_empty()
            && now.saturating_duration_since(served) > NO_REQUEST_TIMEOUT
        {
            return Some(TimedOut::NoRequest);
        }
        let idle = |since: Instant| now.saturating_duration_since(since) > INACTIVITY_TIMEOUT;
        if full
            && !self.interesting
            && !self.peer_interested
            && idle(self.became_uninterested)
            && idle(self.became_uninteresting)
        {
            return Some(TimedOut::NoInterest);
        }
        None
    }
}

/// The request for `chunk` as it goes on the wire.
fn wire_request(chunk: &ChunkInfo) -> Request {
    Request {
        index: chunk.piece_index.get(),
        begin: chunk.offset,
        length: chunk.size,
    }
}

pub(crate) enum RemoveInflightRequestResult {
    Expected,
    LateCanceled,
    Unexpected,
}

/// How long a peer takes to deliver a block we wait on: a running mean, and
/// the mean distance of samples from it, over about the last
/// [`Self::SAMPLES`] blocks.
#[derive(Debug, Default)]
struct BlockTime {
    samples: u32,
    mean_secs: f64,
    deviation_secs: f64,
}

impl BlockTime {
    const SAMPLES: u32 = 20;
    /// The timeout before a peer's first block, and the longest one after.
    const MAX_TIMEOUT: Duration = Duration::from_secs(60);
    /// No timeout is shorter: below this, ordinary jitter would trip it.
    const MIN_TIMEOUT: Duration = Duration::from_secs(2);

    fn add(&mut self, sample: Duration) {
        let sample = sample.as_secs_f64();
        let deviation = (self.mean_secs - sample).abs();
        self.samples = (self.samples + 1).min(Self::SAMPLES);
        self.mean_secs += (sample - self.mean_secs) / f64::from(self.samples);
        if self.samples > 1 {
            self.deviation_secs += (deviation - self.deviation_secs) / f64::from(self.samples - 1);
        }
    }

    /// How long the peer may go without delivering a block it was asked for:
    /// 60 s until it has delivered one; then its mean block time with a fifth
    /// added after one block, and with four mean deviations added after more;
    /// never under 2 s or over 60 s.
    fn request_timeout(&self) -> Duration {
        let secs = match self.samples {
            0 => return Self::MAX_TIMEOUT,
            1 => self.mean_secs + self.mean_secs / 5.0,
            _ => self.mean_secs + 4.0 * self.deviation_secs,
        };
        Duration::from_secs_f64(secs).clamp(Self::MIN_TIMEOUT, Self::MAX_TIMEOUT)
    }

    /// How many blocks the peer delivers in `window` at its mean block time:
    /// its download rate over `window`, in blocks. None until it has
    /// delivered one.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn blocks_in(&self, window: Duration) -> Option<usize> {
        // A mean of zero is a rate past any bound: the cast saturates.
        (self.samples > 0).then(|| (window.as_secs_f64() / self.mean_secs) as usize)
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use std::time::Duration;

    use tokio::time::Instant;

    use super::{
        INACTIVITY_TIMEOUT, LivePeerState, NO_REQUEST_TIMEOUT, TimedOut, Trust, allowed_fast_set,
        stats, unbounded_channel,
    };
    use crate::{
        core::{hash_id::Id20, lengths::Lengths},
        stream_connect::ConnectionKind,
    };

    #[test]
    fn the_latest_suggestion_goes_first_and_the_oldest_past_sixteen_is_dropped() {
        let lengths = Lengths::new(20 * 16384, 16384).unwrap();
        let piece = |i| lengths.validate_piece_index(i).unwrap();
        let (tx, _rx) = unbounded_channel();
        let counters = stats::atomic::PeerStats::default().counters;
        let mut live = LivePeerState::new(Id20::default(), tx, ConnectionKind::Tcp, &counters);

        for i in 0..17 {
            live.suggest(piece(i));
        }
        let kept: Vec<_> = (1..17).rev().map(piece).collect();
        assert_eq!(live.suggested, kept);

        // Suggested again, a piece moves to the front, and none is dropped.
        live.suggest(piece(5));
        let mut moved = vec![piece(5)];
        moved.extend(kept.iter().copied().filter(|p| *p != piece(5)));
        assert_eq!(live.suggested, moved);
    }

    #[tokio::test(start_paused = true)]
    async fn an_unchoked_peer_of_a_seed_times_out_a_minute_after_it_was_last_served() {
        let lengths = Lengths::new(2 * 16384, 16384).unwrap();
        let chunk = lengths
            .chunk_info_from_received_data(lengths.validate_piece_index(0).unwrap(), 0, 16384)
            .unwrap();
        let (tx, _rx) = unbounded_channel();
        let counters = stats::atomic::PeerStats::default().counters;
        let mut live = LivePeerState::new(Id20::default(), tx, ConnectionKind::Tcp, &counters);
        let start = Instant::now();
        let minute = start + NO_REQUEST_TIMEOUT;
        let past = |t: Instant| t + Duration::from_secs(1);

        // Not interested, or choked: never.
        assert_eq!(live.timed_out(past(minute), true, false), None);
        live.peer_interest(true, start);
        assert_eq!(live.timed_out(past(minute), true, false), None);

        // Unchoked a minute ago and asking for nothing, it times out while we
        // seed, and not while we still download.
        live.unchoke(start, 0);
        assert_eq!(live.timed_out(minute, true, false), None);
        let timed_out = Some(TimedOut::NoRequest);
        assert_eq!(live.timed_out(past(minute), true, false), timed_out);
        assert_eq!(live.timed_out(past(minute), false, false), None);

        // A block it asked for holds the connection until it is sent, and
        // the minute runs from then.
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(live.queue_upload(chunk, 1).is_ok());
        assert_eq!(live.timed_out(past(past(minute)), true, false), None);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(live.claim_upload(&chunk));
        let sent = Instant::now();
        assert_eq!(live.timed_out(sent + NO_REQUEST_TIMEOUT, true, false), None);
        assert_eq!(
            live.timed_out(past(sent + NO_REQUEST_TIMEOUT), true, false),
            timed_out
        );
    }

    #[tokio::test(start_paused = true)]
    async fn mutually_uninterested_peers_time_out_after_ten_minutes_when_slots_run_short() {
        let (tx, _rx) = unbounded_channel();
        let counters = stats::atomic::PeerStats::default().counters;
        let mut live = LivePeerState::new(Id20::default(), tx, ConnectionKind::Tcp, &counters);
        let start = Instant::now();
        let secs = |s| start + Duration::from_secs(s);
        let ten = INACTIVITY_TIMEOUT.as_secs();
        let timed_out = Some(TimedOut::NoInterest);

        // From the start neither wants anything; ten minutes on, the
        // connection times out only while connections run short.
        assert_eq!(live.timed_out(secs(ten), false, true), None);
        assert_eq!(live.timed_out(secs(ten + 1), false, true), timed_out);
        assert_eq!(live.timed_out(secs(ten + 1), false, false), None);

        // We want something of the peer from minute one to minute two: the
        // ten minutes run from when we no longer do, and only a change is
        // told to the peer.
        assert!(live.interest(true, secs(60)));
        assert!(!live.interest(true, secs(90)));
        assert_eq!(live.timed_out(secs(ten + 1), false, true), None);
        assert!(live.interest(false, secs(120)));
        assert!(!live.interest(false, secs(150)));
        assert_eq!(live.timed_out(secs(120 + ten), false, true), None);
        assert_eq!(live.timed_out(secs(121 + ten), false, true), timed_out);

        // The peer wants something of us until minute three, and the ten
        // minutes run from when it no longer does.
        live.peer_interest(true, secs(150));
        assert_eq!(live.timed_out(secs(1000), false, true), None);
        live.peer_interest(false, secs(180));
        assert_eq!(live.timed_out(secs(180 + ten), false, true), None);
        assert_eq!(live.timed_out(secs(181 + ten), false, true), timed_out);
    }

    #[test]
    fn the_allowed_fast_set_is_bep_6s() {
        // BEP 6's own example.
        let ip: IpAddr = "80.4.4.200".parse().unwrap();
        let info_hash = Id20::new([0xaa; 20]);
        let seven = [1059, 431, 808, 1217, 287, 376, 1188];
        assert_eq!(allowed_fast_set(ip, info_hash, 1313, 7), seven);
        let mut nine = seven.to_vec();
        nine.extend([353, 508]);
        assert_eq!(allowed_fast_set(ip, info_hash, 1313, 9), nine);

        // The peers of one /24 get one set, and an IPv4-mapped address is
        // its IPv4 one.
        for same in ["80.4.4.1", "::ffff:80.4.4.200"] {
            let same: IpAddr = same.parse().unwrap();
            assert_eq!(allowed_fast_set(same, info_hash, 1313, 7), seven);
        }
        let other: IpAddr = "80.4.5.200".parse().unwrap();
        assert_ne!(allowed_fast_set(other, info_hash, 1313, 7), seven);

        // No fewer pieces than the set: every piece.
        assert_eq!(allowed_fast_set(ip, info_hash, 5, 5), [0, 1, 2, 3, 4]);
    }

    #[test]
    fn a_failed_piece_costs_two_points_and_a_passed_one_earns_one_within_bounds() {
        let mut trust = Trust::default();
        for _ in 0..20 {
            trust.piece_passed();
        }
        assert_eq!(trust.points(), Trust::MAX_POINTS);

        // From the ceiling, eight failures reach the floor and ban the peer
        // each time from then on.
        let mut banned = Vec::new();
        for _ in 0..9 {
            banned.push(trust.piece_failed(false));
        }
        assert_eq!(
            banned,
            [false, false, false, false, false, false, false, true, true]
        );
        assert_eq!(trust.points(), Trust::MIN_POINTS);
        assert_eq!(trust.hashfails(), 9);
    }

    #[test]
    fn a_new_peer_is_banned_at_its_fourth_failure_or_at_once_when_it_sent_the_whole_piece() {
        let mut trust = Trust::default();
        let banned: Vec<_> = (0..4).map(|_| trust.piece_failed(false)).collect();
        assert_eq!(banned, [false, false, false, true]);

        assert!(Trust::default().piece_failed(true));
    }

    #[test]
    fn a_failure_puts_the_peer_on_parole_until_a_piece_passes() {
        let mut trust = Trust::default();
        assert!(!trust.on_parole());
        trust.piece_failed(false);
        assert!(trust.on_parole());
        trust.piece_passed();
        assert!(!trust.on_parole());
        assert_eq!(trust.points(), -1);
    }

    #[test]
    fn hash_failures_stop_counting_at_255() {
        let mut trust = Trust::default();
        for _ in 0..300 {
            trust.piece_failed(false);
        }
        assert_eq!(trust.hashfails(), u8::MAX);
    }
}
