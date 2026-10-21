// SPDX-License-Identifier: GPL-3.0-or-later

//! Who a torrent uploads to. The torrent has a fixed number of upload slots,
//! and the choker alone hands them out: a peer reports its interest and is
//! sent the Choke or Unchoke the choker decides. Each round the interested
//! peers are ranked, those that send us the most first; while we seed, the
//! peers we upload to fastest, rotated round-robin once they have had their
//! share. This is the shape of libtorrent's fixed-slots choker with its
//! round-robin seed choker.

use std::{cmp::Reverse, net::SocketAddr, sync::atomic::Ordering, time::Duration};

use parking_lot::Mutex;
use tokio::{sync::Notify, time::Instant};

use super::{
    peer::{LivePeerState, stats::atomic::PeerCountersAtomic},
    peers::PeerStates,
};

// libtorrent `unchoke_slots_limit`: the peers a torrent uploads to at once.
pub(crate) const DEFAULT_UNCHOKE_SLOTS_LIMIT: usize = 8;
// libtorrent `unchoke_interval`: how often the slots are handed out again.
pub(crate) const UNCHOKE_INTERVAL: Duration = Duration::from_secs(15);
// libtorrent `seeding_piece_quota`: the pieces a peer is sent before it
// yields its slot to a peer that has waited.
const SEEDING_PIECE_QUOTA: u64 = 20;
// libtorrent's round-robin seed choker: the least time a peer holds its slot
// before using up its quota can end it.
const MIN_SLOT_TIME: Duration = Duration::from_secs(60);

/// What a peer sent us and what we sent it, in payload bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Transfer {
    pub fetched: u64,
    pub uploaded: u64,
}

impl Transfer {
    /// A peer's totals over every connection to it.
    pub fn of(counters: &PeerCountersAtomic) -> Self {
        Self {
            fetched: counters.fetched_bytes.load(Ordering::Relaxed),
            uploaded: counters.uploaded_bytes.load(Ordering::Relaxed),
        }
    }

    /// What moved since the totals were `start`.
    pub fn since(self, start: Transfer) -> Transfer {
        Transfer {
            fetched: self.fetched.saturating_sub(start.fetched),
            uploaded: self.uploaded.saturating_sub(start.uploaded),
        }
    }
}

/// Where an interested peer ranks for a slot; the lowest ranks first.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Rank {
    // The most the peer sent us in the round first.
    fetched: Reverse<u64>,
    // A peer that has had its share of a slot goes after one that has not.
    share_used: bool,
    // The most we uploaded to the peer in the round first. A choked peer
    // counts nothing: what still arrived after its Choke is no claim.
    uploaded: Reverse<u64>,
    // The peer that has waited longest since we last unchoked it, one we
    // never unchoked first.
    last_unchoke: Option<Instant>,
}

pub(crate) struct Choker {
    slots: usize,
    piece_length: u64,
    // Every slot decision is made under this, so peers turning interested
    // on several connections at once never take more than `slots`.
    decisions: Mutex<()>,
    // Rings for a new round before the interval ends: a peer holding a slot
    // gave it up.
    reshuffle: Notify,
}

impl Choker {
    /// A choker of `slots` upload slots for a torrent of `piece_length`
    /// pieces.
    pub fn new(slots: usize, piece_length: u64) -> Self {
        Self {
            slots,
            piece_length,
            decisions: Mutex::new(()),
            reshuffle: Notify::new(),
        }
    }

    /// The peer at `addr` said it is interested. It takes a free slot at
    /// once; with none free it waits for a round to rank it.
    pub fn peer_interested(&self, peers: &PeerStates, addr: SocketAddr) {
        let _decisions = self.decisions.lock();
        let free = self.slots.saturating_sub(unchoked(peers)) > 0;
        let now = Instant::now();
        peers.with_peer_mut(addr, "peer_interested", |peer| {
            let uploaded = Transfer::of(&peer.stats.counters).uploaded;
            let Some(live) = peer.get_live_mut() else {
                return;
            };
            live.peer_interested = true;
            if free {
                live.unchoke(now, uploaded);
            }
        });
    }

    /// A peer that held a slot is gone: the slots are handed out again now.
    pub fn slot_freed(&self) {
        self.reshuffle.notify_one();
    }

    /// Returns once the slots are to be handed out before the interval ends.
    pub async fn reshuffle_requested(&self) {
        self.reshuffle.notified().await
    }

    /// A round at `now`: the interested peers that rank highest hold the
    /// slots, and every other peer is choked.
    pub fn recalculate(&self, peers: &PeerStates, now: Instant) {
        let _decisions = self.decisions.lock();
        let mut ranked = Vec::new();
        for mut entry in peers.states.iter_mut() {
            let addr = *entry.key();
            let peer = entry.value_mut();
            let totals = Transfer::of(&peer.stats.counters);
            let Some(live) = peer.get_live_mut() else {
                continue;
            };
            let round = live.end_round(totals);
            if live.peer_interested {
                ranked.push((self.rank(live, round, totals.uploaded, now), addr));
            } else {
                live.choke();
            }
        }
        ranked.sort_unstable();

        for (place, (_, addr)) in ranked.into_iter().enumerate() {
            peers.with_peer_mut(addr, "recalculate", |peer| {
                let uploaded = Transfer::of(&peer.stats.counters).uploaded;
                let Some(live) = peer.get_live_mut() else {
                    return;
                };
                if place < self.slots {
                    live.unchoke(now, uploaded);
                } else {
                    live.choke();
                }
            });
        }
    }

    // `round` is what moved in the round that ended; `uploaded` is all we
    // uploaded to the peer.
    fn rank(&self, live: &LivePeerState, round: Transfer, uploaded: u64, now: Instant) -> Rank {
        let choked = live.is_choked();
        let share_used = !choked
            && live.uploaded_since_unchoke(uploaded) > self.piece_length * SEEDING_PIECE_QUOTA
            && live
                .last_unchoke()
                .is_some_and(|at| now.saturating_duration_since(at) > MIN_SLOT_TIME);
        Rank {
            fetched: Reverse(round.fetched),
            share_used,
            uploaded: Reverse(if choked { 0 } else { round.uploaded }),
            last_unchoke: live.last_unchoke(),
        }
    }
}

// The live peers we do not choke.
fn unchoked(peers: &PeerStates) -> usize {
    peers
        .states
        .iter()
        .filter(|entry| entry.value().get_live().is_some_and(|l| !l.is_choked()))
        .count()
}
