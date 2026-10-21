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
//!
//! Only the rounds walk the peers. A peer's word, or its going, touches
//! that peer alone, and the choker keeps beside the slots the connections
//! holding one, as libtorrent keeps `num_peers_up_unchoked`.

use std::{
    cmp::Reverse,
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use parking_lot::Mutex;
use tokio::{sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;

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

// Who holds the slots.
#[derive(Default)]
struct Held {
    // The connections we do not choke, each with its torrent's run. A
    // connection of a run that stopped holds no slot: its peers went
    // without a word.
    unchoked: HashMap<Key, CancellationToken>,
    // The connections holding the optimistic slots.
    optimistic: Vec<Key>,
}

impl Held {
    // The slots held: the unchoked connections of the runs still going.
    fn count(&mut self) -> usize {
        self.unchoked.retain(|_, run| !run.is_cancelled());
        self.unchoked.len()
    }

    // The connection `key` of `torrent` is unchoked when `unchoked`, and
    // holds a slot so long.
    fn record(&mut self, key: Key, torrent: &TorrentStateLive, unchoked: bool) {
        if unchoked {
            self.unchoked
                .insert(key, torrent.cancellation_token.clone());
        } else {
            self.unchoked.remove(&key);
        }
    }
}

pub(crate) struct Choker {
    slots: usize,
    // Every slot decision is made under this lock, so peers turning
    // interested on several connections at once never take more than
    // `slots`.
    held: Mutex<Held>,
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
            held: Mutex::new(Held::default()),
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
    /// slot at once when one of the session's is free; with none free it
    /// waits for a round to rank it (libtorrent
    /// `peer_connection::maybe_unchoke_this_peer`,
    /// `session_impl::preemptive_unchoke`). A peer we already unchoked is
    /// told so again. Only this peer is touched.
    pub fn peer_interested(&self, torrent: &TorrentStateLive, addr: SocketAddr) {
        let mut held = self.held.lock();
        let free = self.slots > held.count();
        let now = Instant::now();
        let unchoked = torrent
            .peers
            .with_peer_mut(addr, "peer_interested", |peer| {
                let uploaded = Transfer::of(&peer.stats.counters).uploaded;
                let live = peer.get_live_mut()?;
                live.peer_interest(true, now);
                if !live.is_choked() {
                    live.repeat_unchoke();
                } else if free {
                    live.unchoke(now, uploaded);
                }
                Some(!live.is_choked())
            })
            .flatten();
        if let Some(unchoked) = unchoked {
            held.record((torrent.shared.id, addr), torrent, unchoked);
        }
    }

    /// The peer at `addr` of `torrent` said it is not interested. It is
    /// choked at once, and a slot it held is handed out again now.
    pub fn peer_not_interested(&self, torrent: &TorrentStateLive, addr: SocketAddr) {
        let mut held = self.held.lock();
        let now = Instant::now();
        let held_slot = torrent
            .peers
            .with_live_mut(addr, "peer_not_interested", |live| {
                live.peer_interest(false, now);
                live.choke(now)
            })
            .unwrap_or(false);
        self.give_up_slot(&mut held, (torrent.shared.id, addr), held_slot);
    }

    /// The connection to the peer at `addr` of the torrent `torrent` is
    /// gone; `held_slot` when we did not choke it. Its slot is handed out
    /// again now.
    pub fn peer_gone(&self, torrent: TorrentId, addr: SocketAddr, held_slot: bool) {
        let mut held = self.held.lock();
        self.give_up_slot(&mut held, (torrent, addr), held_slot);
    }

    // The connection `key` no longer holds the slot it held: an optimistic
    // slot passes on, and the regular ones are handed out again (libtorrent
    // `peer_connection::choke_this_peer` and `torrent::remove_peer` call
    // `trigger_unchoke`).
    fn give_up_slot(&self, held: &mut Held, key: Key, held_slot: bool) {
        held.unchoked.remove(&key);
        if let Some(at) = held.optimistic.iter().position(|k| *k == key) {
            held.optimistic.swap_remove(at);
            self.rotation.notify_one();
        }
        if held_slot {
            self.reshuffle.notify_one();
        }
    }

    /// The peers of `torrent` holding the optimistic slots.
    pub fn optimistic(&self, torrent: TorrentId) -> Vec<SocketAddr> {
        let held = self.held.lock();
        held.optimistic
            .iter()
            .filter(|(id, _)| *id == torrent)
            .map(|(_, addr)| *addr)
            .collect()
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
    /// interested peer is choked. The round counts the slots held afresh.
    pub fn recalculate(&self, torrents: &[Arc<TorrentStateLive>], now: Instant) {
        let mut held = self.held.lock();
        let mut ranked = Vec::new();
        let mut held_optimistic = Vec::new();
        let mut unchoked = Held::default();
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
                unchoked.record(key, torrent, !live.is_choked());
                if !live.is_choked() && held.optimistic.contains(&key) {
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
        // A slot is held by a connection of a live torrent: one whose
        // torrent stopped, so whose peers went without a word, holds none.
        unchoked.optimistic = held_optimistic;
        *held = unchoked;

        // The optimistic slots held, or all of them while none is.
        let reserved = match held.optimistic.len() {
            0 => self.optimistic_slots(),
            optimistic => optimistic,
        };
        let regular = self.slots.saturating_sub(reserved);
        for (place, (_, key, at)) in ranked.into_iter().enumerate() {
            let holds_optimistic = held.optimistic.contains(&key);
            if place < regular && holds_optimistic {
                // Promoted to a regular slot: its optimistic one passes on.
                held.optimistic.retain(|k| *k != key);
                self.rotation.notify_one();
            }
            let torrent = &torrents[at];
            let unchoked = torrent
                .peers
                .with_peer_mut(key.1, "recalculate", |peer| {
                    let uploaded = Transfer::of(&peer.stats.counters).uploaded;
                    let live = peer.get_live_mut()?;
                    if place < regular {
                        live.unchoke(now, uploaded);
                    } else if !holds_optimistic {
                        live.choke(now);
                    }
                    Some(!live.is_choked())
                })
                .flatten();
            held.record(key, torrent, unchoked.unwrap_or(false));
        }
    }

    /// The optimistic slots pass at `now` to the interested peers of
    /// `torrents`, the session's live torrents, that have waited longest for
    /// one, choked or holding one already.
    pub fn rotate_optimistic(&self, torrents: &[Arc<TorrentStateLive>], now: Instant) {
        let mut held = self.held.lock();
        let previous = std::mem::take(&mut held.optimistic);
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
            let torrent = &torrents[*at];
            let unchoked = torrent
                .peers
                .with_peer_mut(key.1, "rotate_optimistic", |peer| {
                    let uploaded = Transfer::of(&peer.stats.counters).uploaded;
                    let live = peer.get_live_mut()?;
                    live.unchoke(now, uploaded);
                    peer.last_optimistic_unchoke = Some(now);
                    Some(true)
                })
                .flatten();
            held.record(*key, torrent, unchoked.unwrap_or(false));
        }
        // A previous holder that lost its slot is choked; one whose torrent
        // stopped is gone with it.
        for key in previous.iter().filter(|k| !chosen.contains(k)) {
            if let Some(torrent) = torrents.iter().find(|t| t.shared.id == key.0) {
                torrent
                    .peers
                    .with_live_mut(key.1, "rotate_optimistic", |live| live.choke(now));
            }
            held.unchoked.remove(key);
        }
        held.optimistic = chosen;

        if held.count() > self.slots {
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
