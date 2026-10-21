// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use crate::core::{hash_id::Id20, lengths::ValidPieceIndex};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use tokio::{sync::Notify, time::Instant};
use tracing::warn;

use crate::{
    peer_connection::WriterRequest,
    peer_protocol::Message,
    stream_connect::ConnectionKind,
    torrent_state::utils::{TimedExistence, atomic_inc},
    type_aliases::{BF, PeerHandle},
};

use self::{
    availability::Availability,
    stats::{AggregatePeerStats, AggregatePeerStatsAtomic},
};

use super::peer::{
    EraseRank, IncomingConnectionResult, LivePeerState, Peer, PeerRx, PeerState, PeerTx,
    empty_bitfield, listen_address, stats::atomic::PeerCountersAtomic,
};

pub mod availability;
pub mod stats;
mod super_seed;

// libtorrent `max_peerlist_size`: the most peers one torrent's peer list holds.
pub(crate) const MAX_PEERLIST_SIZE: usize = 3000;

pub(crate) struct PeerStates {
    pub session_stats: Arc<AggregatePeerStatsAtomic>,

    // This keeps track of live addresses we connected to, for PEX.
    pub live_outgoing_peers: RwLock<HashSet<PeerHandle>>,
    pub stats: AggregatePeerStatsAtomic,
    pub states: DashMap<PeerHandle, Peer>,

    // How many live peers have each piece, kept with the bitfields above.
    pub availability: Availability,
    total_pieces: u32,

    // Held while a peer joins, so the list never grows past MAX_PEERLIST_SIZE,
    // and while an address is banned, so no peer at it joins meanwhile.
    admission: Mutex<()>,
    // The addresses banned for sending bad data. One host is one address, as
    // libtorrent's peer list keeps one peer per address
    // (`allow_multiple_connections_per_ip` off): a ban covers every port.
    // Taken last, after any other lock.
    banned: RwLock<HashSet<IpAddr>>,
    // Wakes the connect loop when a connect candidate appears or may come due
    // sooner.
    candidates_changed: Notify,
    // Held while a piece is handed to a peer to super-seed, so two peers
    // are not handed one piece at once.
    super_seeding: Mutex<()>,
}

/// What a newcomer may push out of a full peer list.
#[derive(Clone, Copy)]
enum Displace {
    /// Only peers we would not connect to: a peer someone named is worth no
    /// more than a connect candidate we hold.
    NonCandidates,
    /// Any peer we are not connected to: a connected peer outranks them.
    AnyIdle,
}

impl Displace {
    fn allows(self, (candidate, ..): &EraseRank) -> bool {
        match self {
            Displace::NonCandidates => !candidate,
            Displace::AnyIdle => true,
        }
    }
}

enum Admitted<R> {
    Known(R),
    New(Arc<PeerCountersAtomic>),
    Refused,
    Banned,
}

/// When the connect loop may next connect to someone.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NextCandidate {
    Due,
    At(Instant),
    None,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IncomingRefused {
    AlreadyActive,
    ListFull,
    Banned,
}

impl Drop for PeerStates {
    fn drop(&mut self) {
        for (_, p) in std::mem::take(&mut self.states).into_iter() {
            p.destroy(self);
        }
    }
}

impl PeerStates {
    pub fn new(session_stats: Arc<AggregatePeerStatsAtomic>, total_pieces: u32) -> Self {
        Self {
            session_stats,
            live_outgoing_peers: Default::default(),
            stats: Default::default(),
            states: Default::default(),
            availability: Availability::new(total_pieces),
            total_pieces,
            admission: Mutex::new(()),
            banned: Default::default(),
            candidates_changed: Notify::new(),
            super_seeding: Mutex::new(()),
        }
    }

    /// Whether peers at `ip` are banned.
    pub fn is_banned(&self, ip: IpAddr) -> bool {
        self.banned.read().contains(&ip)
    }

    /// Bans the address of `addr`: no peer at it joins the list again, and
    /// every one there now has its connection closed and is no connect
    /// candidate (libtorrent `torrent::ban_peer`, `peer_list::ban_peer`).
    pub fn ban(&self, addr: SocketAddr) {
        let _admission = self.admission.lock();
        if !self.banned.write().insert(addr.ip()) {
            return;
        }
        warn!(?addr, "banning the peer for sending bad data");
        for mut peer in self.states.iter_mut() {
            if peer.key().ip() == addr.ip() {
                peer.value_mut().ban(self);
            }
        }
    }

    pub fn stats(&self) -> AggregatePeerStats {
        self.stats.snapshot()
    }

    /// A tracker, DHT or PEX named `addr`: a known peer is named again, a new
    /// one joins as a connect candidate.
    pub fn add_peer(&self, addr: SocketAddr) {
        self.admit(
            addr,
            Displace::NonCandidates,
            |peer| peer.rediscovered(self),
            || Some(Peer::new_with_outgoing_address(addr)),
        );
        self.candidates_changed.notify_one();
    }

    /// A peer connected to us while every slot was taken. The connection goes,
    /// the peer stays: a known one is named again, and one we can connect back
    /// to joins as a connect candidate.
    pub fn add_refused_incoming(&self, addr: SocketAddr, kind: ConnectionKind) {
        self.admit(
            addr,
            Displace::NonCandidates,
            |peer| {
                atomic_inc(&peer.stats.counters.incoming_connections);
                peer.rediscovered(self);
            },
            || {
                listen_address(addr, kind).map(|listen| {
                    let peer = Peer::new_with_outgoing_address(listen);
                    atomic_inc(&peer.stats.counters.incoming_connections);
                    peer
                })
            },
        );
        self.candidates_changed.notify_one();
    }

    /// A peer connected to us and holds a slot. Returns its counters.
    pub fn add_incoming(
        &self,
        addr: SocketAddr,
        peer_id: Id20,
        tx: PeerTx,
        kind: ConnectionKind,
    ) -> Result<Arc<PeerCountersAtomic>, IncomingRefused> {
        let admitted = self.admit(
            addr,
            Displace::AnyIdle,
            |peer| {
                peer.incoming_connection(peer_id, tx.clone(), self, kind)
                    .map(|()| peer.stats.counters.clone())
            },
            || {
                Some(Peer::new_live_for_incoming_connection(
                    addr,
                    peer_id,
                    tx.clone(),
                    kind,
                ))
            },
        );
        match admitted {
            Admitted::Known(Ok(counters)) | Admitted::New(counters) => Ok(counters),
            Admitted::Known(Err(IncomingConnectionResult::AlreadyActive)) => {
                Err(IncomingRefused::AlreadyActive)
            }
            Admitted::Refused => Err(IncomingRefused::ListFull),
            Admitted::Banned => Err(IncomingRefused::Banned),
        }
    }

    // The one way into the list. A peer at a banned address is turned away
    // (libtorrent `peer_list::new_connection`, `peer_list::add_peer`). A known
    // peer is handed to `known`. A newcomer from `newcomer` joins, first
    // pushing out the peer that ranks lowest when the list is full; it is
    // refused when nobody `displace` allows is there.
    fn admit<R>(
        &self,
        addr: SocketAddr,
        displace: Displace,
        known: impl FnOnce(&mut Peer) -> R,
        newcomer: impl FnOnce() -> Option<Peer>,
    ) -> Admitted<R> {
        let _admission = self.admission.lock();
        if self.is_banned(addr.ip()) {
            return Admitted::Banned;
        }
        if let Some(r) = self.with_peer_mut(addr, "admit", known) {
            return Admitted::Known(r);
        }
        let Some(peer) = newcomer() else {
            return Admitted::Refused;
        };
        if self.states.len() >= MAX_PEERLIST_SIZE && !self.erase_one(displace) {
            return Admitted::Refused;
        }
        self.count_in(peer.get_state());
        for counter in [&*self.session_stats, &self.stats] {
            atomic_inc(&counter.seen);
        }
        let counters = peer.stats.counters.clone();
        self.states.insert(addr, peer);
        Admitted::New(counters)
    }

    // Forgets the lowest-ranked peer `displace` allows. Connected peers are
    // never forgotten.
    fn erase_one(&self, displace: Displace) -> bool {
        let victim = self
            .states
            .iter()
            .filter_map(|e| Some((e.value().erase_rank()?, *e.key())))
            .filter(|(rank, _)| displace.allows(rank))
            .min();
        let Some((_, addr)) = victim else {
            return false;
        };
        match self
            .states
            .remove_if(&addr, |_, peer| peer.erase_rank().is_some())
        {
            Some((_, peer)) => {
                peer.destroy(self);
                true
            }
            None => false,
        }
    }

    /// When the connect loop may next connect to someone.
    pub fn next_candidate(&self, now: Instant) -> NextCandidate {
        let mut next = NextCandidate::None;
        for e in self.states.iter() {
            let peer = e.value();
            if !peer.is_connect_candidate() {
                continue;
            }
            match peer.reconnect_at() {
                Some(at) if at > now => {
                    if !matches!(next, NextCandidate::At(sooner) if sooner <= at) {
                        next = NextCandidate::At(at);
                    }
                }
                _ => return NextCandidate::Due,
            }
        }
        next
    }

    /// Returns once a connect candidate is due.
    pub async fn wait_for_candidate(&self) {
        loop {
            match self.next_candidate(Instant::now()) {
                NextCandidate::Due => return,
                NextCandidate::At(at) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(at) => {}
                        _ = self.candidates_changed.notified() => {}
                    }
                }
                NextCandidate::None => self.candidates_changed.notified().await,
            }
        }
    }

    /// Returns once the connect candidates may have changed.
    pub async fn candidates_changed(&self) {
        self.candidates_changed.notified().await
    }

    /// Starts connecting to the connect candidate due at `now` that failed
    /// least, then longest ago.
    pub fn connect_next(&self, now: Instant) -> Option<(PeerHandle, PeerRx, PeerTx)> {
        let (_, addr) = self
            .states
            .iter()
            .filter(|e| e.value().is_due(now))
            .map(|e| (e.value().connect_rank(), *e.key()))
            .min()?;
        let (rx, tx) = self
            .with_peer_mut(addr, "connect_next", |peer| peer.connect_if_due(now, self))
            .flatten()?;
        Some((addr, rx, tx))
    }

    /// Settles a peer whose connection closed; see [`Peer::connection_closed`].
    pub fn connection_closed(&self, peer: &mut Peer, failed_at: Option<Instant>, needed: bool) {
        peer.connection_closed(failed_at, needed, self);
        if peer.is_connect_candidate() {
            self.candidates_changed.notify_one();
        }
    }

    /// The peer's connection completed its handshake and carried a message.
    pub fn connection_delivered(&self, addr: PeerHandle) {
        self.with_peer_mut(addr, "connection_delivered", Peer::delivered);
    }

    /// We want the peers we let go again.
    pub fn requeue_not_needed(&self) {
        for mut peer in self.states.iter_mut() {
            peer.value_mut().needed_again(self);
        }
        self.candidates_changed.notify_one();
    }

    pub fn with_peer<R>(&self, addr: PeerHandle, f: impl FnOnce(&Peer) -> R) -> Option<R> {
        self.states.get(&addr).map(|e| f(e.value()))
    }

    pub fn with_peer_mut<R>(
        &self,
        addr: PeerHandle,
        reason: &'static str,
        f: impl FnOnce(&mut Peer) -> R,
    ) -> Option<R> {
        use crate::torrent_state::utils::timeit;
        timeit(reason, || self.states.get_mut(&addr))
            .map(|e| f(TimedExistence::new(e, reason).value_mut()))
    }

    pub fn with_live<R>(&self, addr: PeerHandle, f: impl FnOnce(&LivePeerState) -> R) -> Option<R> {
        self.with_peer(addr, |peer| peer.get_live().map(f))
            .flatten()
    }

    pub fn with_live_mut<R>(
        &self,
        addr: PeerHandle,
        reason: &'static str,
        f: impl FnOnce(&mut LivePeerState) -> R,
    ) -> Option<R> {
        self.with_peer_mut(addr, reason, |peer| peer.get_live_mut().map(f))
            .flatten()
    }

    pub fn drop_peer(&self, handle: PeerHandle) -> Option<Peer> {
        let p = self.states.remove(&handle).map(|r| r.1)?;
        self.count_out(p.get_state());
        Some(p)
    }

    /// Counts a peer in `state` into the torrent and session peer stats and,
    /// when it is live, its bitfield into the piece availability.
    pub(super) fn count_in(&self, state: &PeerState) {
        for counter in [&*self.session_stats, &self.stats] {
            counter.inc(state);
        }
        if let PeerState::Live(live) = state {
            self.availability.add(&live.bitfield);
        }
    }

    /// Takes back what [`Self::count_in`] counted for a peer leaving `state`.
    /// Every way a connection ends passes here once, through
    /// [`Peer::set_state`] or the peer leaving the list.
    pub(super) fn count_out(&self, state: &PeerState) {
        for counter in [&*self.session_stats, &self.stats] {
            counter.dec(state);
        }
        if let PeerState::Live(live) = state {
            self.availability.remove(&live.bitfield);
        }
    }

    pub fn is_peer_not_interested_and_has_full_torrent(
        &self,
        handle: PeerHandle,
        total_pieces: usize,
    ) -> bool {
        self.with_live(handle, |live| {
            !live.peer_interested && live.has_full_torrent(total_pieces)
        })
        .unwrap_or(false)
    }

    /// The peer sent its BITFIELD. A second one replaces the first: the old
    /// pieces leave the availability before the new ones join it.
    pub fn update_bitfield(&self, handle: PeerHandle, bitfield: BF) -> Option<()> {
        self.with_live_mut(handle, "update_bitfield", |live| {
            self.availability.remove(&live.bitfield);
            self.availability.add(&bitfield);
            live.bitfield = bitfield;
            self.update_seeder_flag(live);
        })
    }

    /// The peer sent HAVE for `piece`. A peer that sent no bitfield had
    /// nothing until now. A HAVE for a piece the peer already has changes
    /// nothing.
    pub fn peer_has(&self, handle: PeerHandle, piece: ValidPieceIndex) -> Option<()> {
        self.with_live_mut(handle, "peer_has", |live| {
            if live.bitfield.is_empty() {
                live.bitfield = empty_bitfield(self.total_pieces);
            }
            if live.bitfield.replace(piece.get_usize(), true) {
                return;
            }
            self.availability.peer_has(piece.get_usize());
            self.update_seeder_flag(live);
        })
    }

    // Recompute the cached "this peer has the full torrent" flag after its bitfield
    // changed, keeping the aggregate seeder counters in sync. Must be called on every
    // bitfield change of a live peer.
    fn update_seeder_flag(&self, live: &mut LivePeerState) {
        let seeder = live.has_full_torrent(self.total_pieces as usize);
        if seeder == live.seeder {
            return;
        }
        live.seeder = seeder;
        self.stats.seeder_flag_changed(seeder);
        self.session_stats.seeder_flag_changed(seeder);
    }

    pub(crate) fn on_steal(
        &self,
        from_peer: SocketAddr,
        to_peer: SocketAddr,
        stolen_idx: ValidPieceIndex,
    ) {
        self.with_peer(to_peer, |p| {
            atomic_inc(&p.stats.counters.times_i_stole);
        });
        self.with_peer(from_peer, |p| {
            atomic_inc(&p.stats.counters.times_stolen_from_me);
        });
        self.stats.inc_steals();
        self.session_stats.inc_steals();

        self.with_live_mut(from_peer, "send_cancellations", |live| {
            live.cancel_inflight_requests_for_piece(stolen_idx);
        });
    }

    /// Tells every live peer that lacks `piece` that we have it now. A peer
    /// that sent no bitfield has nothing, so it lacks every piece. The HAVE
    /// goes on the peer's own unbounded writer queue, so a slow connection
    /// receives it late but never loses it. A peer that goes live after this
    /// walk reads our have-bitfield afterwards, which holds the piece.
    pub(crate) fn announce_have(&self, piece: ValidPieceIndex) {
        for peer in self.states.iter() {
            if let PeerState::Live(live) = peer.value().get_state()
                && !live.bitfield.get(piece.get_usize()).is_some_and(|has| *has)
            {
                let _ = live
                    .tx
                    .send(WriterRequest::Message(Message::Have(piece.get())));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::torrent_state::live::peer::{MAX_FAILCOUNT, MIN_RECONNECT_TIME};

    const TOTAL_PIECES: usize = 8;

    fn full_bitfield() -> BF {
        BF::from_boxed_slice(vec![0xffu8; 1].into_boxed_slice())
    }

    fn empty_bitfield() -> BF {
        BF::from_boxed_slice(vec![0u8; 1].into_boxed_slice())
    }

    fn make_peer_states() -> PeerStates {
        PeerStates::new(Default::default(), TOTAL_PIECES as u32)
    }

    // Returns the peer handle, keeping the receiving end of the peer channel alive.
    fn add_live_peer(peers: &PeerStates, addr: &str) -> (PeerHandle, PeerRx) {
        let addr: SocketAddr = addr.parse().unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(addr, Id20::default(), tx, ConnectionKind::Tcp)
            .unwrap();
        (addr, rx)
    }

    fn live_seeders(peers: &PeerStates) -> (u32, u32) {
        (
            peers.stats().live_seeders,
            peers.session_stats.snapshot().live_seeders,
        )
    }

    #[test]
    fn full_bitfield_counts_as_seeder_and_is_uncounted_on_disconnect() {
        let peers = make_peer_states();
        let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
        assert_eq!(live_seeders(&peers), (0, 0));

        peers.update_bitfield(addr, empty_bitfield()).unwrap();
        assert_eq!(live_seeders(&peers), (0, 0));

        peers.update_bitfield(addr, full_bitfield()).unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        // Repeated updates must not double-count.
        peers.update_bitfield(addr, full_bitfield()).unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        peers.drop_peer(addr).unwrap();
        assert_eq!(live_seeders(&peers), (0, 0));
    }

    #[test]
    fn bitfield_completed_piece_by_piece_counts_as_seeder() {
        let peers = make_peer_states();
        let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
        peers.update_bitfield(addr, empty_bitfield()).unwrap();

        for piece in 0..TOTAL_PIECES {
            peers
                .peer_has(addr, self::piece(TOTAL_PIECES as u32, piece as u32))
                .unwrap();
            let expected = u32::from(piece == TOTAL_PIECES - 1);
            assert_eq!(live_seeders(&peers), (expected, expected));
        }
    }

    #[test]
    fn seeder_is_uncounted_when_peer_leaves_live_state() {
        let peers = make_peer_states();
        let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
        peers.update_bitfield(addr, full_bitfield()).unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        peers
            .with_peer_mut(addr, "test", |peer| peer.set_not_needed(&peers))
            .unwrap();
        assert_eq!(live_seeders(&peers), (0, 0));
    }

    #[test]
    fn seeders_are_counted_per_peer() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.1:1235");

        peers.update_bitfield(a, full_bitfield()).unwrap();
        peers.update_bitfield(b, empty_bitfield()).unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        peers.update_bitfield(b, full_bitfield()).unwrap();
        assert_eq!(live_seeders(&peers), (2, 2));

        peers.drop_peer(a).unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));
    }

    #[test]
    fn a_steal_empties_the_victims_queue_of_the_stolen_piece() {
        use crate::core::constants::CHUNK_SIZE;
        use crate::core::lengths::Lengths;
        use crate::peer_protocol::Request;

        let lengths = Lengths::new(CHUNK_SIZE as u64 * 8, CHUNK_SIZE * 4).unwrap();
        let stolen = lengths.validate_piece_index(0).unwrap();
        let kept = lengths.validate_piece_index(1).unwrap();

        let peers = make_peer_states();
        let (victim, mut victim_rx) = add_live_peer(&peers, "127.0.0.1:1234");
        let (thief, _thief_rx) = add_live_peer(&peers, "127.0.0.1:1235");

        peers
            .with_live_mut(victim, "test", |live| {
                for chunk in lengths.iter_chunk_infos(stolen).take(2) {
                    assert!(live.add_inflight_request(chunk));
                }
                for chunk in lengths.iter_chunk_infos(kept).take(1) {
                    assert!(live.add_inflight_request(chunk));
                }
            })
            .unwrap();

        peers.on_steal(victim, thief, stolen);

        let queued = peers
            .with_live(victim, |live| {
                live.inflight_requests().copied().collect::<Vec<_>>()
            })
            .unwrap();
        assert!(
            queued.iter().all(|req| req.piece_index == kept),
            "{queued:?}"
        );
        assert_eq!(queued.len(), 1);

        let mut cancelled = Vec::new();
        while let Ok(msg) = victim_rx.try_recv() {
            match msg {
                WriterRequest::Message(Message::Cancel(Request { index, begin, .. })) => {
                    cancelled.push((index, begin))
                }
                _ => panic!("unexpected writer request"),
            }
        }
        cancelled.sort();
        assert_eq!(cancelled, vec![(0, 0), (0, CHUNK_SIZE)]);
    }

    fn bitfield_of(pieces: &[usize]) -> BF {
        let mut bf = empty_bitfield();
        for &p in pieces {
            bf.set(p, true);
        }
        bf
    }

    fn availability(peers: &PeerStates) -> Vec<u32> {
        peers.availability.counts()
    }

    #[test]
    fn availability_counts_every_live_peers_bitfield() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.1:1235");
        assert_eq!(availability(&peers), [0; TOTAL_PIECES]);

        peers.update_bitfield(a, full_bitfield()).unwrap();
        peers.update_bitfield(b, bitfield_of(&[0, 3])).unwrap();
        assert_eq!(availability(&peers), [2, 1, 1, 2, 1, 1, 1, 1]);
    }

    #[test]
    fn a_have_adds_the_piece_once() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        peers.update_bitfield(a, bitfield_of(&[0])).unwrap();

        peers.peer_has(a, piece(TOTAL_PIECES as u32, 5)).unwrap();
        assert_eq!(availability(&peers), [1, 0, 0, 0, 0, 1, 0, 0]);

        // libtorrent incoming_have: a HAVE for a piece the peer has is ignored.
        peers.peer_has(a, piece(TOTAL_PIECES as u32, 5)).unwrap();
        peers.peer_has(a, piece(TOTAL_PIECES as u32, 0)).unwrap();
        assert_eq!(availability(&peers), [1, 0, 0, 0, 0, 1, 0, 0]);
    }

    #[test]
    fn a_have_before_any_bitfield_counts() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");

        peers.peer_has(a, piece(TOTAL_PIECES as u32, 2)).unwrap();
        assert_eq!(availability(&peers), [0, 0, 1, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn a_second_bitfield_replaces_the_first() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        peers.update_bitfield(a, bitfield_of(&[0, 1])).unwrap();

        peers.update_bitfield(a, bitfield_of(&[1, 2])).unwrap();
        assert_eq!(availability(&peers), [0, 1, 1, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn a_closed_connection_takes_its_bitfield_back_once() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.1:1235");
        peers.update_bitfield(a, bitfield_of(&[0, 1])).unwrap();
        peers.update_bitfield(b, bitfield_of(&[1])).unwrap();
        peers.peer_has(a, piece(TOTAL_PIECES as u32, 7)).unwrap();

        // As the peer's task does when the connection ends: take the live
        // state, then settle the peer.
        peers
            .with_peer_mut(a, "test", |peer| {
                peer.take_state(&peers);
                peers.connection_closed(peer, None, true);
            })
            .unwrap();
        assert_eq!(availability(&peers), [0, 1, 0, 0, 0, 0, 0, 0]);

        peers
            .with_peer_mut(a, "test", |peer| peers.connection_closed(peer, None, true))
            .unwrap();
        assert_eq!(availability(&peers), [0, 1, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn a_peer_let_go_or_dropped_takes_its_bitfield_back() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.1:1235");
        peers.update_bitfield(a, full_bitfield()).unwrap();
        peers.update_bitfield(b, bitfield_of(&[4])).unwrap();

        peers
            .with_peer_mut(a, "test", |peer| peer.set_not_needed(&peers))
            .unwrap();
        assert_eq!(availability(&peers), [0, 0, 0, 0, 1, 0, 0, 0]);

        peers.drop_peer(b).unwrap();
        assert_eq!(availability(&peers), [0; TOTAL_PIECES]);
    }

    fn haves_queued(rx: &mut PeerRx) -> Vec<u32> {
        let mut haves = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            match msg {
                WriterRequest::Message(Message::Have(piece)) => haves.push(piece),
                _ => panic!("unexpected writer request"),
            }
        }
        haves
    }

    fn piece(total_pieces: u32, index: u32) -> ValidPieceIndex {
        crate::core::lengths::Lengths::new(total_pieces as u64, 1)
            .unwrap()
            .validate_piece_index(index)
            .unwrap()
    }

    #[test]
    fn a_completed_piece_is_announced_to_every_live_peer_that_lacks_it() {
        let peers = make_peer_states();
        let (_silent, mut silent_rx) = add_live_peer(&peers, "127.0.0.1:1234");
        let (seeder, mut seeder_rx) = add_live_peer(&peers, "127.0.0.1:1235");
        let (leecher, mut leecher_rx) = add_live_peer(&peers, "127.0.0.1:1236");
        peers.update_bitfield(seeder, full_bitfield()).unwrap();
        peers.update_bitfield(leecher, empty_bitfield()).unwrap();

        peers.announce_have(piece(TOTAL_PIECES as u32, 3));

        // The peer that sent no bitfield has nothing, so it is told.
        assert_eq!(haves_queued(&mut silent_rx), vec![3]);
        assert_eq!(haves_queued(&mut leecher_rx), vec![3]);
        assert_eq!(haves_queued(&mut seeder_rx), Vec::<u32>::new());
    }

    #[test]
    fn a_peer_that_falls_behind_still_learns_every_completed_piece() {
        // Far more pieces than a 128-entry bounded channel holds, all completed
        // before the peer's writer reads a single one.
        const PIECES: u32 = 1000;
        let peers = make_peer_states();
        let (_slow, mut slow_rx) = add_live_peer(&peers, "127.0.0.1:1234");

        for index in 0..PIECES {
            peers.announce_have(piece(PIECES, index));
        }

        assert_eq!(haves_queued(&mut slow_rx), (0..PIECES).collect::<Vec<_>>());
    }

    fn peer_addr(i: usize) -> SocketAddr {
        SocketAddr::from(([10, 0, (i >> 8) as u8, i as u8], 6881))
    }

    fn state_of(peers: &PeerStates, addr: SocketAddr) -> Option<&'static str> {
        peers.with_peer(addr, |peer| peer.get_state().name())
    }

    // Connects to `addr`, which must be due at `at`, and fails the connection
    // at `at`.
    fn fail_at(peers: &PeerStates, addr: SocketAddr, at: Instant) {
        peers
            .with_peer_mut(addr, "test", |peer| {
                peer.connect_if_due(at, peers).expect("a due candidate");
                peers.connection_closed(peer, Some(at), true);
            })
            .unwrap();
    }

    fn reconnect_wait(failcount: u32) -> Duration {
        MIN_RECONNECT_TIME * (failcount + 1)
    }

    // Fails `addr` MAX_FAILCOUNT times in a row from `t0`, each attempt as soon
    // as it is due; returns the time of the last failure.
    fn fail_out(peers: &PeerStates, addr: SocketAddr, t0: Instant) -> Instant {
        let mut at = t0;
        for failcount in 0..MAX_FAILCOUNT {
            if failcount > 0 {
                at += reconnect_wait(failcount);
            }
            fail_at(peers, addr, at);
        }
        at
    }

    #[test]
    fn a_failed_peer_waits_failcount_plus_one_reconnect_times() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        let t0 = Instant::now();
        peers.add_peer(a);
        assert_eq!(peers.next_candidate(t0), NextCandidate::Due);

        let mut failed = t0;
        for failcount in 1..MAX_FAILCOUNT {
            fail_at(&peers, a, failed);
            let due = failed + reconnect_wait(failcount);
            assert_eq!(state_of(&peers, a), Some("queued"));
            assert_eq!(peers.next_candidate(failed), NextCandidate::At(due));
            assert!(peers.connect_next(due - Duration::from_millis(1)).is_none());
            failed = due;
        }
        assert_eq!(peers.connect_next(failed).map(|(addr, ..)| addr), Some(a));
    }

    #[tokio::test(start_paused = true)]
    async fn the_connect_loop_wakes_when_the_reconnect_time_comes() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a);
        fail_at(&peers, a, Instant::now());

        let due = peers.wait_for_candidate();
        tokio::pin!(due);
        assert!(futures::poll!(&mut due).is_pending());
        tokio::time::advance(reconnect_wait(1) - Duration::from_secs(1)).await;
        assert!(futures::poll!(&mut due).is_pending());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(futures::poll!(&mut due).is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn the_connect_loop_wakes_when_a_peer_is_named() {
        let peers = make_peer_states();
        let due = peers.wait_for_candidate();
        tokio::pin!(due);
        assert!(futures::poll!(&mut due).is_pending());
        peers.add_peer(peer_addr(1));
        assert!(futures::poll!(&mut due).is_ready());
    }

    #[test]
    fn a_peer_that_failed_max_failcount_times_is_dropped_as_a_candidate() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a);
        let last = fail_out(&peers, a, Instant::now());

        assert_eq!(state_of(&peers, a), Some("dead"));
        assert_eq!(peers.next_candidate(last), NextCandidate::None);
        let much_later = last + Duration::from_secs(365 * 24 * 3600);
        assert!(peers.connect_next(much_later).is_none());
    }

    #[test]
    fn a_dropped_peer_is_revived_when_named_again() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a);
        let last = fail_out(&peers, a, Instant::now());
        assert_eq!(state_of(&peers, a), Some("dead"));

        // A tracker, DHT or PEX names it: one failure is forgiven.
        peers.add_peer(a);
        assert_eq!(state_of(&peers, a), Some("queued"));
        assert_eq!(
            peers.next_candidate(last),
            NextCandidate::At(last + reconnect_wait(MAX_FAILCOUNT - 1))
        );
    }

    #[test]
    fn a_dropped_peer_is_revived_when_it_connects_to_us() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a);
        fail_out(&peers, a, Instant::now());

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(a, Id20::default(), tx, ConnectionKind::Tcp)
            .unwrap();
        assert_eq!(state_of(&peers, a), Some("live"));
    }

    #[test]
    fn failures_are_forgotten_once_a_connection_delivers() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a);
        let t0 = Instant::now();
        fail_at(&peers, a, t0);
        let t1 = t0 + reconnect_wait(1);
        fail_at(&peers, a, t1);

        // The next attempt completes its handshake and carries a message.
        let t2 = t1 + reconnect_wait(2);
        peers
            .with_peer_mut(a, "test", |peer| peer.connect_if_due(t2, &peers))
            .unwrap()
            .unwrap();
        peers.connection_delivered(a);

        // Its later failure is the first in a row, not the third.
        let t3 = t2 + Duration::from_secs(600);
        peers
            .with_peer_mut(a, "test", |peer| {
                peers.connection_closed(peer, Some(t3), true)
            })
            .unwrap();
        assert_eq!(state_of(&peers, a), Some("queued"));
        assert_eq!(
            peers.next_candidate(t3),
            NextCandidate::At(t3 + reconnect_wait(1))
        );
    }

    #[test]
    fn a_connection_we_close_is_not_a_failure() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a);
        let t0 = Instant::now();
        peers
            .with_peer_mut(a, "test", |peer| {
                peer.connect_if_due(t0, &peers).unwrap();
                peers.connection_closed(peer, None, false);
            })
            .unwrap();
        assert_eq!(state_of(&peers, a), Some("not needed"));
        assert_eq!(peers.next_candidate(t0), NextCandidate::None);

        peers.requeue_not_needed();
        assert_eq!(state_of(&peers, a), Some("queued"));
        assert_eq!(peers.next_candidate(t0), NextCandidate::Due);
    }

    #[test]
    fn a_full_list_forgets_peers_we_would_not_connect_to_most_failed_and_oldest_first() {
        let peers = make_peer_states();
        let t0 = Instant::now();

        let (older, newer, not_needed, live) =
            (peer_addr(0), peer_addr(1), peer_addr(2), peer_addr(3));
        for addr in [older, newer, not_needed] {
            peers.add_peer(addr);
        }
        fail_out(&peers, older, t0);
        fail_out(&peers, newer, t0 + Duration::from_secs(1));
        peers
            .with_peer_mut(not_needed, "test", |peer| peer.set_not_needed(&peers))
            .unwrap();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(live, Id20::default(), tx, ConnectionKind::Tcp)
            .unwrap();
        for i in 4..MAX_PEERLIST_SIZE {
            peers.add_peer(peer_addr(i));
        }
        assert_eq!(peers.states.len(), MAX_PEERLIST_SIZE);

        let newcomer = |i| peer_addr(MAX_PEERLIST_SIZE + i);
        peers.add_peer(newcomer(0));
        assert_eq!(state_of(&peers, older), None);
        assert_eq!(state_of(&peers, newer), Some("dead"));
        peers.add_peer(newcomer(1));
        assert_eq!(state_of(&peers, newer), None);
        assert_eq!(state_of(&peers, not_needed), Some("not needed"));
        peers.add_peer(newcomer(2));
        assert_eq!(state_of(&peers, not_needed), None);

        // Only connect candidates and a connected peer are left: a named
        // peer does not push out a candidate we hold.
        peers.add_peer(newcomer(3));
        assert_eq!(state_of(&peers, newcomer(3)), None);
        assert_eq!(peers.states.len(), MAX_PEERLIST_SIZE);
        assert_eq!(state_of(&peers, live), Some("live"));
        assert_eq!(peers.stats().seen as usize, MAX_PEERLIST_SIZE + 3);
        assert_eq!(peers.stats().queued as usize, MAX_PEERLIST_SIZE - 1);
    }

    #[test]
    fn a_connected_peer_is_never_forgotten() {
        let peers = make_peer_states();
        let mut channels = Vec::new();
        for i in 0..MAX_PEERLIST_SIZE - 1 {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            peers
                .add_incoming(peer_addr(i), Id20::default(), tx, ConnectionKind::Tcp)
                .unwrap();
            channels.push(rx);
        }
        let candidate = peer_addr(MAX_PEERLIST_SIZE - 1);
        peers.add_peer(candidate);

        // An incoming peer holding a slot pushes out the one idle peer.
        let first = peer_addr(MAX_PEERLIST_SIZE);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(first, Id20::default(), tx, ConnectionKind::Tcp)
            .unwrap();
        assert_eq!(state_of(&peers, candidate), None);
        assert_eq!(state_of(&peers, first), Some("live"));

        // Then the list holds connected peers only, and nobody gets in.
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let second = peer_addr(MAX_PEERLIST_SIZE + 1);
        assert_eq!(
            peers
                .add_incoming(second, Id20::default(), tx, ConnectionKind::Tcp)
                .unwrap_err(),
            IncomingRefused::ListFull
        );
        peers.add_peer(second);
        assert_eq!(state_of(&peers, second), None);
        assert_eq!(peers.states.len(), MAX_PEERLIST_SIZE);
        assert_eq!(peers.stats().live as usize, MAX_PEERLIST_SIZE);
    }

    fn at_port(addr: SocketAddr, port: u16) -> SocketAddr {
        SocketAddr::new(addr.ip(), port)
    }

    #[test]
    fn a_banned_address_is_disconnected_and_never_admitted_again_on_any_port() {
        let peers = make_peer_states();
        let t0 = Instant::now();
        let banned = peer_addr(1);
        let (live, mut live_rx) = add_live_peer(&peers, &at_port(banned, 50000).to_string());
        peers.add_peer(banned);
        let other = peer_addr(2);
        peers.add_peer(other);

        peers.ban(live);

        // The connection is closed; the candidate at the address is one no
        // longer; a peer at another address is untouched.
        assert!(matches!(
            live_rx.try_recv(),
            Ok(WriterRequest::Disconnect(Err(_)))
        ));
        assert_eq!(state_of(&peers, banned), Some("dead"));
        assert_eq!(state_of(&peers, other), Some("queued"));

        // The closed connection leaves the peer Dead, needed or not.
        peers
            .with_peer_mut(live, "test", |peer| {
                peer.take_state(&peers);
                peers.connection_closed(peer, Some(t0), true);
            })
            .unwrap();
        assert_eq!(state_of(&peers, live), Some("dead"));

        // Named again or connecting from any port, the address is turned away.
        peers.add_peer(banned);
        peers.add_peer(at_port(banned, 6882));
        peers.add_refused_incoming(at_port(banned, 6883), ConnectionKind::Utp);
        assert_eq!(state_of(&peers, banned), Some("dead"));
        assert_eq!(state_of(&peers, at_port(banned, 6882)), None);
        assert_eq!(state_of(&peers, at_port(banned, 6883)), None);
        for addr in [live, at_port(banned, 50001)] {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            assert_eq!(
                peers
                    .add_incoming(addr, Id20::default(), tx, ConnectionKind::Tcp)
                    .unwrap_err(),
                IncomingRefused::Banned
            );
        }
        peers.requeue_not_needed();
        let much_later = t0 + Duration::from_secs(365 * 24 * 3600);
        assert_eq!(
            peers.connect_next(much_later).map(|(addr, ..)| addr),
            Some(other)
        );
        assert!(peers.connect_next(much_later).is_none());
    }

    #[test]
    fn an_incoming_peer_refused_a_slot_is_kept() {
        let peers = make_peer_states();
        let t0 = Instant::now();

        // A uTP peer connects from where it listens: it is queued to be
        // connected back.
        let utp = peer_addr(1);
        peers.add_refused_incoming(utp, ConnectionKind::Utp);
        assert_eq!(state_of(&peers, utp), Some("queued"));
        assert_eq!(peers.next_candidate(t0), NextCandidate::Due);
        let incoming = peers.with_peer(utp, |peer| {
            peer.stats
                .counters
                .incoming_connections
                .load(std::sync::atomic::Ordering::Relaxed)
        });
        assert_eq!(incoming, Some(1));

        // A known peer that connects is named again, which revives it.
        let known = peer_addr(2);
        peers.add_peer(known);
        fail_out(&peers, known, t0);
        assert_eq!(state_of(&peers, known), Some("dead"));
        peers.add_refused_incoming(known, ConnectionKind::Tcp);
        assert_eq!(state_of(&peers, known), Some("queued"));

        // An unknown TCP peer connects from an ephemeral port: there is no
        // address to connect back to.
        let tcp = peer_addr(3);
        peers.add_refused_incoming(tcp, ConnectionKind::Tcp);
        assert_eq!(state_of(&peers, tcp), None);
    }
}
