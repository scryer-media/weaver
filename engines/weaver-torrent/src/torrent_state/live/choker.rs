// SPDX-License-Identifier: GPL-3.0-or-later

//! Who the session uploads to. The session has a fixed number of upload
//! slots, shared by all its torrents, and the choker alone hands them out: a
//! peer reports its interest and is sent the Choke or Unchoke the choker
//! decides. Each round the interested peers of every live torrent are ranked
//! together, those that send us the most first; while we seed, the peers we
//! upload to fastest, rotated round-robin once they have had their share.
//! Some slots are optimistic: they pass in turn to the peers, of any
//! torrent, that have waited longest for one, whatever they sent us, so a
//! new peer gets the chance to show what it can do. This follows
//! libtorrent's fixed-slots choker with its round-robin seed choker
//! (`session_impl::recalculate_unchoke_slots`,
//! `recalculate_optimistic_unchoke_slots`).

use std::{
    cmp::Reverse,
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use parking_lot::Mutex;
use tokio::{sync::Notify, time::Instant};

use super::{
    TorrentStateLive,
    peer::{LivePeerState, stats::atomic::PeerCountersAtomic},
};
use crate::session::TorrentId;

// libtorrent `unchoke_slots_limit`: the peers the session uploads to at once.
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

// A connection the choker deals with: its torrent, and the peer's address.
type Key = (TorrentId, SocketAddr);

pub(crate) struct Choker {
    slots: usize,
    // The connections holding the optimistic slots. Every slot decision is
    // made under this lock, so peers turning interested on several
    // connections at once never take more than `slots`.
    optimistic: Mutex<Vec<Key>>,
    // Rings for a round before the interval ends: a peer holding a slot gave
    // it up, or more peers are unchoked than there are slots.
    reshuffle: Notify,
    // Rings for the optimistic slots to pass on before their interval ends:
    // a holder gave its slot up, or took a regular one.
    rotation: Notify,
}

impl Choker {
    /// A choker of `slots` upload slots.
    pub fn new(slots: usize) -> Self {
        Self {
            slots,
            optimistic: Mutex::new(Vec::new()),
            reshuffle: Notify::new(),
            rotation: Notify::new(),
        }
    }

    // libtorrent `num_optimistic_unchoke_slots` at its default: one slot in
    // five and at least one, of a session that has any.
    fn optimistic_slots(&self) -> usize {
        match self.slots {
            0 => 0,
            slots => (slots / 5).max(1),
        }
    }

    /// The peer at `addr` of `torrent` said it is interested. It takes a
    /// slot at once when one of the session's is free, `torrents` being the
    /// session's live torrents; with none free it waits for a round to rank
    /// it (libtorrent `peer_connection::maybe_unchoke_this_peer`,
    /// `session_impl::preemptive_unchoke`). A peer we already unchoked is
    /// told so again.
    pub fn peer_interested(
        &self,
        torrents: &[Arc<TorrentStateLive>],
        torrent: &TorrentStateLive,
        addr: SocketAddr,
    ) {
        let _optimistic = self.optimistic.lock();
        let free = self.slots.saturating_sub(unchoked(torrents)) > 0;
        let now = Instant::now();
        torrent
            .peers
            .with_peer_mut(addr, "peer_interested", |peer| {
                let uploaded = Transfer::of(&peer.stats.counters).uploaded;
                let Some(live) = peer.get_live_mut() else {
                    return;
                };
                live.peer_interest(true, now);
                if !live.is_choked() {
                    live.repeat_unchoke();
                } else if free {
                    live.unchoke(now, uploaded);
                }
            });
    }

    /// The peer at `addr` of `torrent` said it is not interested. It is
    /// choked at once, and a slot it held is handed out again now.
    pub fn peer_not_interested(&self, torrent: &TorrentStateLive, addr: SocketAddr) {
        let mut optimistic = self.optimistic.lock();
        let now = Instant::now();
        let held_slot = torrent
            .peers
            .with_live_mut(addr, "peer_not_interested", |live| {
                live.peer_interest(false, now);
                live.choke(now)
            })
            .unwrap_or(false);
        self.give_up_slot(&mut optimistic, (torrent.shared.id, addr), held_slot);
    }

    /// The connection to the peer at `addr` of the torrent `torrent` is
    /// gone; `held_slot` when we did not choke it. Its slot is handed out
    /// again now.
    pub fn peer_gone(&self, torrent: TorrentId, addr: SocketAddr, held_slot: bool) {
        let mut optimistic = self.optimistic.lock();
        self.give_up_slot(&mut optimistic, (torrent, addr), held_slot);
    }

    // The connection `key` no longer holds the slot it held: an optimistic
    // slot passes on, and the regular ones are handed out again.
    fn give_up_slot(&self, optimistic: &mut Vec<Key>, key: Key, held: bool) {
        if let Some(at) = optimistic.iter().position(|k| *k == key) {
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

    /// A round at `now` over `torrents`, the session's live torrents: the
    /// interested peers that rank highest of all of them hold the regular
    /// slots, the optimistic slots keep their holders, and every other
    /// interested peer is choked.
    pub fn recalculate(&self, torrents: &[Arc<TorrentStateLive>], now: Instant) {
        let mut optimistic = self.optimistic.lock();
        let mut ranked = Vec::new();
        let mut held_optimistic = Vec::new();
        for (at, torrent) in torrents.iter().enumerate() {
            let piece_length = u64::from(torrent.lengths.default_piece_length());
            for mut entry in torrent.peers.states.iter_mut() {
                let key = (torrent.shared.id, *entry.key());
                let peer = entry.value_mut();
                let totals = Transfer::of(&peer.stats.counters);
                let Some(live) = peer.get_live_mut() else {
                    continue;
                };
                let round = live.end_round(totals);
                if !live.is_choked() && optimistic.contains(&key) {
                    held_optimistic.push(key);
                }
                // A peer that is not interested is choked already: it was
                // choked when it said so, and only an interested one is
                // unchoked.
                if live.peer_interested {
                    let rank = rank(live, round, totals.uploaded, piece_length, now);
                    ranked.push((rank, key, at));
                }
            }
        }
        ranked.sort_unstable();
        // An optimistic slot is held by a connection of a live torrent: one
        // whose torrent stopped, so whose peers went without a word, holds
        // none.
        *optimistic = held_optimistic;

        // The optimistic slots held, or all of them while none is.
        let reserved = match optimistic.len() {
            0 => self.optimistic_slots(),
            held => held,
        };
        let regular = self.slots.saturating_sub(reserved);
        for (place, (_, key, at)) in ranked.into_iter().enumerate() {
            let holds_optimistic = optimistic.contains(&key);
            if place < regular && holds_optimistic {
                // Promoted to a regular slot: its optimistic one passes on.
                optimistic.retain(|k| *k != key);
                self.rotation.notify_one();
            }
            torrents[at]
                .peers
                .with_peer_mut(key.1, "recalculate", |peer| {
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

    /// The optimistic slots pass at `now` to the interested peers of
    /// `torrents`, the session's live torrents, that have waited longest for
    /// one, choked or holding one already.
    pub fn rotate_optimistic(&self, torrents: &[Arc<TorrentStateLive>], now: Instant) {
        let mut optimistic = self.optimistic.lock();
        let previous = std::mem::take(&mut *optimistic);
        let mut candidates = Vec::new();
        for (at, torrent) in torrents.iter().enumerate() {
            for entry in torrent.peers.states.iter() {
                let key = (torrent.shared.id, *entry.key());
                let peer = entry.value();
                let Some(live) = peer.get_live() else {
                    continue;
                };
                if live.peer_interested && (live.is_choked() || previous.contains(&key)) {
                    let rank = OptimisticRank {
                        last_optimistic_unchoke: peer.last_optimistic_unchoke,
                        draw: rand::random(),
                    };
                    candidates.push((rank, key, at));
                }
            }
        }
        candidates.sort_unstable();
        candidates.truncate(self.optimistic_slots());
        let chosen: Vec<Key> = candidates.iter().map(|(_, key, _)| *key).collect();

        for (_, key, at) in candidates.iter().filter(|(_, k, _)| !previous.contains(k)) {
            torrents[*at]
                .peers
                .with_peer_mut(key.1, "rotate_optimistic", |peer| {
                    let uploaded = Transfer::of(&peer.stats.counters).uploaded;
                    let Some(live) = peer.get_live_mut() else {
                        return;
                    };
                    live.unchoke(now, uploaded);
                    peer.last_optimistic_unchoke = Some(now);
                });
        }
        // A previous holder that lost its slot is choked; one whose torrent
        // stopped is gone with it.
        for (id, addr) in previous.iter().filter(|k| !chosen.contains(k)) {
            if let Some(torrent) = torrents.iter().find(|t| t.shared.id == *id) {
                torrent
                    .peers
                    .with_live_mut(*addr, "rotate_optimistic", |live| live.choke(now));
            }
        }
        *optimistic = chosen;

        if unchoked(torrents) > self.slots {
            self.reshuffle.notify_one();
        }
    }
}

// Where `live` ranks at `now` for a regular slot (libtorrent `choker.cpp`
// `unchoke_compare_rr`). `round` is what moved in the round that ended;
// `uploaded` is all we uploaded to the peer; `piece_length` is its torrent's.
fn rank(
    live: &LivePeerState,
    round: Transfer,
    uploaded: u64,
    piece_length: u64,
    now: Instant,
) -> Rank {
    let choked = live.is_choked();
    let share_used = !choked
        && live.uploaded_since_unchoke(uploaded) > piece_length * SEEDING_PIECE_QUOTA
        && now.saturating_duration_since(live.last_unchoke()) > MIN_SLOT_TIME;
    Rank {
        fetched: Reverse(round.fetched),
        share_used,
        uploaded: Reverse(if choked { 0 } else { round.uploaded }),
        last_unchoke: live.last_unchoke(),
    }
}

// The live peers of `torrents` we do not choke.
fn unchoked(torrents: &[Arc<TorrentStateLive>]) -> usize {
    torrents
        .iter()
        .flat_map(|torrent| torrent.peers.states.iter())
        .filter(|entry| entry.value().get_live().is_some_and(|l| !l.is_choked()))
        .count()
}
