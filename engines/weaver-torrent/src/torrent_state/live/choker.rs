// SPDX-License-Identifier: GPL-3.0-or-later

//! Who a torrent uploads to. The torrent has a fixed number of upload slots,
//! and the choker alone hands them out: a peer reports its interest and is
//! sent the Choke or Unchoke the choker decides. Each round the interested
//! peers are ranked, those that send us the most first; while we seed, the
//! peers we upload to fastest, rotated round-robin once they have had their
//! share. Some slots are optimistic: they pass in turn to the peers that
//! have waited longest for one, whatever they sent us, so a new peer gets
//! the chance to show what it can do. This follows libtorrent's
//! fixed-slots choker with its round-robin seed choker.

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
// libtorrent `optimistic_unchoke_interval`: how often the optimistic slots
// pass to other peers.
pub(crate) const OPTIMISTIC_UNCHOKE_INTERVAL: Duration = Duration::from_secs(30);
// libtorrent `seeding_piece_quota`: the pieces a peer is sent before it
// yields its slot to a peer that has waited.
const SEEDING_PIECE_QUOTA: u64 = 20;
// libtorrent `unchoke_compare_rr`: the least time a peer holds its slot
// before using up its quota can end it.
const MIN_SLOT_TIME: Duration = Duration::from_secs(60);
// libtorrent `peer_connection::incoming_request`: how long after its Choke a
// peer's requests are taken as already on their way; a request later than
// that ends the connection.
pub(crate) const CHOKED_REQUEST_GRACE: Duration = Duration::from_secs(2);

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
    // The peer that has waited longest since we last unchoked it first; one
    // we never unchoked has waited since its connection went live.
    last_unchoke: Instant,
}

/// Where a peer ranks for an optimistic slot; the lowest ranks first.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct OptimisticRank {
    // The peer that has waited longest for one, one that never held one
    // first.
    last_optimistic_unchoke: Option<Instant>,
    // Then any of those alike, at random.
    draw: u64,
}

pub(crate) struct Choker {
    slots: usize,
    piece_length: u64,
    // The peers holding the optimistic slots. Every slot decision is made
    // under this lock, so peers turning interested on several connections at
    // once never take more than `slots`.
    optimistic: Mutex<Vec<SocketAddr>>,
    // Rings for a round before the interval ends: a peer holding a slot gave
    // it up, or more peers are unchoked than there are slots.
    reshuffle: Notify,
    // Rings for the optimistic slots to pass on before their interval ends:
    // a holder gave its slot up, or took a regular one.
    rotation: Notify,
}

impl Choker {
    /// A choker of `slots` upload slots for a torrent of `piece_length`
    /// pieces.
    pub fn new(slots: usize, piece_length: u64) -> Self {
        Self {
            slots,
            piece_length,
            optimistic: Mutex::new(Vec::new()),
            reshuffle: Notify::new(),
            rotation: Notify::new(),
        }
    }

    // libtorrent `num_optimistic_unchoke_slots` at its default: one slot in
    // five and at least one, of a torrent that has any.
    fn optimistic_slots(&self) -> usize {
        match self.slots {
            0 => 0,
            slots => (slots / 5).max(1),
        }
    }

    /// The peer at `addr` said it is interested. It takes a free slot at
    /// once; with none free it waits for a round to rank it. A peer we
    /// already unchoked is told so again.
    pub fn peer_interested(&self, peers: &PeerStates, addr: SocketAddr) {
        let _optimistic = self.optimistic.lock();
        let free = self.slots.saturating_sub(unchoked(peers)) > 0;
        let now = Instant::now();
        peers.with_peer_mut(addr, "peer_interested", |peer| {
            let uploaded = Transfer::of(&peer.stats.counters).uploaded;
            let Some(live) = peer.get_live_mut() else {
                return;
            };
            live.peer_interested = true;
            if !live.is_choked() {
                live.repeat_unchoke();
            } else if free {
                live.unchoke(now, uploaded);
            }
        });
    }

    /// The peer at `addr` said it is not interested. It is choked at once,
    /// and a slot it held is handed out again now.
    pub fn peer_not_interested(&self, peers: &PeerStates, addr: SocketAddr) {
        let mut optimistic = self.optimistic.lock();
        let now = Instant::now();
        let held_slot = peers
            .with_live_mut(addr, "peer_not_interested", |live| {
                live.peer_interested = false;
                live.choke(now)
            })
            .unwrap_or(false);
        self.give_up_slot(&mut optimistic, addr, held_slot);
    }

    /// The connection to the peer at `addr` is gone; `held_slot` when we did
    /// not choke it. Its slot is handed out again now.
    pub fn peer_gone(&self, addr: SocketAddr, held_slot: bool) {
        let mut optimistic = self.optimistic.lock();
        self.give_up_slot(&mut optimistic, addr, held_slot);
    }

    // The peer at `addr` no longer holds the slot it held: an optimistic
    // slot passes on, and the regular ones are handed out again.
    fn give_up_slot(&self, optimistic: &mut Vec<SocketAddr>, addr: SocketAddr, held: bool) {
        if let Some(at) = optimistic.iter().position(|a| *a == addr) {
            optimistic.swap_remove(at);
            self.rotation.notify_one();
        }
        if held {
            self.reshuffle.notify_one();
        }
    }

    /// Returns once the slots are to be handed out before the interval ends.
    pub async fn reshuffle_requested(&self) {
        self.reshuffle.notified().await
    }

    /// Returns once the optimistic slots are to pass on before their
    /// interval ends.
    pub async fn rotation_requested(&self) {
        self.rotation.notified().await
    }

    /// A round at `now`: the interested peers that rank highest hold the
    /// regular slots, the optimistic slots keep their holders, and every
    /// other interested peer is choked.
    pub fn recalculate(&self, peers: &PeerStates, now: Instant) {
        let mut optimistic = self.optimistic.lock();
        let mut ranked = Vec::new();
        for mut entry in peers.states.iter_mut() {
            let addr = *entry.key();
            let peer = entry.value_mut();
            let totals = Transfer::of(&peer.stats.counters);
            let Some(live) = peer.get_live_mut() else {
                continue;
            };
            let round = live.end_round(totals);
            // A peer that is not interested is choked already: it was
            // choked when it said so, and only an interested one is unchoked.
            if live.peer_interested {
                ranked.push((self.rank(live, round, totals.uploaded, now), addr));
            }
        }
        ranked.sort_unstable();

        // The optimistic slots held, or all of them while none is.
        let reserved = match optimistic.len() {
            0 => self.optimistic_slots(),
            held => held,
        };
        let regular = self.slots.saturating_sub(reserved);
        for (place, (_, addr)) in ranked.into_iter().enumerate() {
            let holds_optimistic = optimistic.contains(&addr);
            if place < regular && holds_optimistic {
                // Promoted to a regular slot: its optimistic one passes on.
                optimistic.retain(|a| *a != addr);
                self.rotation.notify_one();
            }
            peers.with_peer_mut(addr, "recalculate", |peer| {
                let uploaded = Transfer::of(&peer.stats.counters).uploaded;
                let Some(live) = peer.get_live_mut() else {
                    return;
                };
                if place < regular {
                    live.unchoke(now, uploaded);
                } else if !holds_optimistic {
                    live.choke(now);
                }
            });
        }
    }

    /// The optimistic slots pass at `now` to the interested peers that have
    /// waited longest for one, choked or holding one already.
    pub fn rotate_optimistic(&self, peers: &PeerStates, now: Instant) {
        let mut optimistic = self.optimistic.lock();
        let previous = std::mem::take(&mut *optimistic);
        let mut candidates: Vec<_> = peers
            .states
            .iter()
            .filter_map(|entry| {
                let addr = *entry.key();
                let peer = entry.value();
                let live = peer.get_live()?;
                let candidate =
                    live.peer_interested && (live.is_choked() || previous.contains(&addr));
                candidate.then(|| {
                    let rank = OptimisticRank {
                        last_optimistic_unchoke: peer.last_optimistic_unchoke,
                        draw: rand::random(),
                    };
                    (rank, addr)
                })
            })
            .collect();
        candidates.sort_unstable();
        let chosen: Vec<SocketAddr> = candidates
            .into_iter()
            .take(self.optimistic_slots())
            .map(|(_, addr)| addr)
            .collect();

        for addr in chosen.iter().filter(|a| !previous.contains(a)) {
            peers.with_peer_mut(*addr, "rotate_optimistic", |peer| {
                let uploaded = Transfer::of(&peer.stats.counters).uploaded;
                let Some(live) = peer.get_live_mut() else {
                    return;
                };
                live.unchoke(now, uploaded);
                peer.last_optimistic_unchoke = Some(now);
            });
        }
        for addr in previous.iter().filter(|a| !chosen.contains(a)) {
            peers.with_live_mut(*addr, "rotate_optimistic", |live| live.choke(now));
        }
        *optimistic = chosen;

        if unchoked(peers) > self.slots {
            self.reshuffle.notify_one();
        }
    }

    // `round` is what moved in the round that ended; `uploaded` is all we
    // uploaded to the peer.
    fn rank(&self, live: &LivePeerState, round: Transfer, uploaded: u64, now: Instant) -> Rank {
        let choked = live.is_choked();
        let share_used = !choked
            && live.uploaded_since_unchoke(uploaded) > self.piece_length * SEEDING_PIECE_QUOTA
            && now.saturating_duration_since(live.last_unchoke()) > MIN_SLOT_TIME;
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
