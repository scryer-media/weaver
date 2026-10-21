// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use crate::core::lengths::ValidPieceIndex;
use dashmap::DashMap;
use parking_lot::RwLock;

use crate::{
    Error,
    peer_connection::WriterRequest,
    peer_protocol::Message,
    torrent_state::utils::{TimedExistence, atomic_inc},
    type_aliases::{BF, PeerHandle},
};

use self::stats::{AggregatePeerStats, AggregatePeerStatsAtomic};

use super::peer::{LivePeerState, Peer, PeerRx, PeerState, PeerTx};

pub mod stats;

pub(crate) struct PeerStates {
    pub session_stats: Arc<AggregatePeerStatsAtomic>,

    // This keeps track of live addresses we connected to, for PEX.
    pub live_outgoing_peers: RwLock<HashSet<PeerHandle>>,
    pub stats: AggregatePeerStatsAtomic,
    pub states: DashMap<PeerHandle, Peer>,
}

impl Drop for PeerStates {
    fn drop(&mut self) {
        for (_, p) in std::mem::take(&mut self.states).into_iter() {
            p.destroy(self);
        }
    }
}

impl PeerStates {
    pub fn stats(&self) -> AggregatePeerStats {
        self.stats.snapshot()
    }

    pub fn add_if_not_seen(&self, addr: SocketAddr) -> Option<PeerHandle> {
        use dashmap::mapref::entry::Entry;
        match self.states.entry(addr) {
            Entry::Occupied(_) => None,
            Entry::Vacant(vac) => {
                vac.insert(Peer::new_with_outgoing_address(addr));
                atomic_inc(&self.stats.queued);
                atomic_inc(&self.session_stats.queued);

                atomic_inc(&self.stats.seen);
                atomic_inc(&self.session_stats.seen);
                Some(addr)
            }
        }
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
        let s = p.get_state();
        self.stats.dec(s);
        self.session_stats.dec(s);

        Some(p)
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

    pub fn mark_peer_interested(&self, handle: PeerHandle, is_interested: bool) -> Option<bool> {
        self.with_live_mut(handle, "mark_peer_interested", |live| {
            let prev = live.peer_interested;
            live.peer_interested = is_interested;
            prev
        })
    }

    pub fn update_bitfield(
        &self,
        handle: PeerHandle,
        bitfield: BF,
        total_pieces: usize,
    ) -> Option<()> {
        self.with_live_mut(handle, "update_bitfield", |live| {
            live.bitfield = bitfield;
            self.update_seeder_flag(live, total_pieces);
        })
    }

    // Recompute the cached "this peer has the full torrent" flag after its bitfield
    // changed, keeping the aggregate seeder counters in sync. Must be called on every
    // bitfield change of a live peer.
    pub fn update_seeder_flag(&self, live: &mut LivePeerState, total_pieces: usize) {
        let seeder = live.has_full_torrent(total_pieces);
        if seeder == live.seeder {
            return;
        }
        live.seeder = seeder;
        self.stats.seeder_flag_changed(seeder);
        self.session_stats.seeder_flag_changed(seeder);
    }

    pub fn mark_peer_connecting(&self, h: PeerHandle) -> crate::Result<(PeerRx, PeerTx)> {
        let rx = self
            .with_peer_mut(h, "mark_peer_connecting", |peer| {
                peer.idle_to_connecting(self)
                    .ok_or(Error::BugInvalidPeerState)
            })
            .ok_or(Error::BugPeerNotFound)??;
        Ok(rx)
    }

    pub fn reset_peer_backoff(&self, handle: PeerHandle) {
        self.with_peer_mut(handle, "reset_peer_backoff", |p| {
            p.stats.reset_backoff();
        });
    }

    pub fn mark_peer_not_needed(&self, handle: PeerHandle) -> Option<PeerState> {
        let prev = self.with_peer_mut(handle, "mark_peer_not_needed", |peer| {
            peer.set_not_needed(self)
        })?;
        Some(prev)
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
    use crate::core::hash_id::Id20;

    use super::*;
    use crate::stream_connect::ConnectionKind;

    const TOTAL_PIECES: usize = 8;

    fn full_bitfield() -> BF {
        BF::from_boxed_slice(vec![0xffu8; 1].into_boxed_slice())
    }

    fn empty_bitfield() -> BF {
        BF::from_boxed_slice(vec![0u8; 1].into_boxed_slice())
    }

    fn make_peer_states() -> PeerStates {
        PeerStates {
            session_stats: Default::default(),
            live_outgoing_peers: Default::default(),
            stats: Default::default(),
            states: Default::default(),
        }
    }

    // Returns the peer handle, keeping the receiving end of the peer channel alive.
    fn add_live_peer(peers: &PeerStates, addr: &str) -> (PeerHandle, PeerRx) {
        let addr: SocketAddr = addr.parse().unwrap();
        peers.add_if_not_seen(addr).unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .with_peer_mut(addr, "test", |peer| {
                peer.incoming_connection(Id20::default(), tx, peers, ConnectionKind::Tcp)
            })
            .unwrap()
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

        peers
            .update_bitfield(addr, empty_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (0, 0));

        peers
            .update_bitfield(addr, full_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        // Repeated updates must not double-count.
        peers
            .update_bitfield(addr, full_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        peers.drop_peer(addr).unwrap();
        assert_eq!(live_seeders(&peers), (0, 0));
    }

    #[test]
    fn bitfield_completed_piece_by_piece_counts_as_seeder() {
        let peers = make_peer_states();
        let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
        peers
            .update_bitfield(addr, empty_bitfield(), TOTAL_PIECES)
            .unwrap();

        for piece in 0..TOTAL_PIECES {
            peers
                .with_live_mut(addr, "test", |live| {
                    *live.bitfield.get_mut(piece).unwrap() = true;
                    peers.update_seeder_flag(live, TOTAL_PIECES);
                })
                .unwrap();
            let expected = u32::from(piece == TOTAL_PIECES - 1);
            assert_eq!(live_seeders(&peers), (expected, expected));
        }
    }

    #[test]
    fn seeder_is_uncounted_when_peer_leaves_live_state() {
        let peers = make_peer_states();
        let (addr, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
        peers
            .update_bitfield(addr, full_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        peers.mark_peer_not_needed(addr).unwrap();
        assert_eq!(live_seeders(&peers), (0, 0));
    }

    #[test]
    fn seeders_are_counted_per_peer() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.1:1235");

        peers
            .update_bitfield(a, full_bitfield(), TOTAL_PIECES)
            .unwrap();
        peers
            .update_bitfield(b, empty_bitfield(), TOTAL_PIECES)
            .unwrap();
        assert_eq!(live_seeders(&peers), (1, 1));

        peers
            .update_bitfield(b, full_bitfield(), TOTAL_PIECES)
            .unwrap();
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
        peers
            .update_bitfield(seeder, full_bitfield(), TOTAL_PIECES)
            .unwrap();
        peers
            .update_bitfield(leecher, empty_bitfield(), TOTAL_PIECES)
            .unwrap();

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
}
