// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    cmp::Reverse,
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use crate::core::{hash_id::Id20, lengths::ValidPieceIndex};
use dashmap::DashMap;
use indexmap::IndexSet;
use parking_lot::{Mutex, RwLock};
use tokio::{
    sync::{Notify, mpsc::unbounded_channel},
    time::Instant,
};
use tracing::warn;

use crate::{
    mse::Encryption,
    peer_connection::WriterRequest,
    peer_protocol::{Message, extended::ExtendedMessage},
    stream_connect::ConnectionKind,
    torrent_state::utils::{TimedExistence, atomic_inc},
    type_aliases::{BF, PeerHandle, PeerSourceFlag, PeerSources},
};

use self::{
    availability::Availability,
    connect_order::ConnectOrder,
    index::{Basis, ConnectKey, Indexed, PeerIndex},
    stats::{AggregatePeerStats, AggregatePeerStatsAtomic},
};

use super::smart_ban::{SentBlock, SmartBan};

use super::peer::{
    IncomingConnectionResult, LivePeerState, Peer, PeerRx, PeerState, PeerTx, listen_address,
    stats::atomic::PeerCountersAtomic,
};

pub mod availability;
pub(crate) mod connect_order;
pub(crate) mod holepunch;
pub(crate) mod index;
pub mod stats;
mod super_seed;

// The most peers one torrent's peer list holds (libtorrent
// `max_peerlist_size`). libtorrent's default is 3000; Weaver holds 5000.
pub(crate) const MAX_PEERLIST_SIZE: usize = 5000;
// libtorrent `peer_list::erase_peers`'s low watermark: once the list holds
// this many, choosing whom to connect to weeds it to fewer
// (`peer_list::find_connect_candidates`).
const PEERLIST_LOW_WATERMARK: usize = MAX_PEERLIST_SIZE * 95 / 100;
// libtorrent `max_paused_peerlist_size`: the most peers a paused torrent's
// peer list holds.
pub(crate) const MAX_PAUSED_PEERLIST_SIZE: usize = 1000;

// The most addresses the session keeps banned for bad data: about a
// megabyte at most.
pub(crate) const MAX_SESSION_BANS: usize = 16_384;

/// The addresses a torrent of the session banned for sending bad data,
/// banned on every torrent of the session for as long as it runs, and kept
/// nowhere. One host is one address, so a ban covers every port, as each
/// torrent's own ban does. Past [`MAX_SESSION_BANS`] the oldest is
/// forgotten here; the torrent that banned it keeps its own ban.
#[derive(Default)]
pub(crate) struct SessionBans(RwLock<SessionBanList>);

#[derive(Default)]
struct SessionBanList {
    banned: HashSet<IpAddr>,
    // The order they were banned in, oldest first.
    order: VecDeque<IpAddr>,
}

impl SessionBans {
    /// Whether `ip` is banned.
    pub(crate) fn contains(&self, ip: IpAddr) -> bool {
        self.0.read().banned.contains(&ip)
    }

    /// Whether no address is banned.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.read().banned.is_empty()
    }

    /// Bans `ip`; true when it was not banned before.
    pub(crate) fn insert(&self, ip: IpAddr) -> bool {
        let mut list = self.0.write();
        if !list.banned.insert(ip) {
            return false;
        }
        list.order.push_back(ip);
        if list.order.len() > MAX_SESSION_BANS
            && let Some(oldest) = list.order.pop_front()
        {
            list.banned.remove(&oldest);
        }
        true
    }
}

/// Where a peer stands when the list must forget one; the lowest goes first
/// (libtorrent `peer_list::is_erase_candidate`, then `compare_peer_erase`):
/// an erase candidate, a peer with no connection that is no connect
/// candidate and has failed or was named by resume data alone; then the
/// most failures in a row; then one resume data alone named; then one we
/// cannot connect to; then the least trusted (peer_list.cpp:88-108,
/// 354-364, v2.0.15).
type EraseRank = (bool, Reverse<u32>, bool, bool, i8);

fn erase_rank(peer: &Peer) -> EraseRank {
    let failcount = peer.failcount();
    let resume_data_only = peer.sources() == PeerSourceFlag::ResumeData.into();
    let erase_candidate = peer.connected_out().is_none()
        && !peer.is_connect_candidate()
        && (failcount > 0 || resume_data_only);
    (
        !erase_candidate,
        Reverse(failcount),
        !resume_data_only,
        peer.listens_where_listed(),
        peer.trust().points(),
    )
}

pub(crate) struct PeerStates {
    pub session_stats: Arc<AggregatePeerStatsAtomic>,

    // The run's own counters, which its torrent's gauges read too.
    pub stats: Arc<AggregatePeerStatsAtomic>,
    pub states: DashMap<PeerHandle, Peer>,

    // How many live peers have each piece, kept with the bitfields above.
    pub availability: Availability,
    total_pieces: u32,

    // Held while a peer joins, so the list never grows past MAX_PEERLIST_SIZE,
    // and while an address is banned, so no peer at it joins meanwhile.
    admission: Mutex<()>,
    // The banned addresses: for sending bad data, or by the operator. One
    // host is one address, as
    // libtorrent's peer list keeps one peer per address
    // (`allow_multiple_connections_per_ip` off): a ban covers every port.
    // Taken last, after any other lock.
    banned: RwLock<HashSet<IpAddr>>,
    // The addresses the session bans for bad data sent to any of its
    // torrents. Taken after `banned`.
    session_bans: Arc<SessionBans>,
    // Wakes the connect loop when a connect candidate appears or may come due
    // sooner.
    candidates_changed: Notify,
    // Wakes the run's rate-tick join each time a peer goes live, so a run
    // with no connection costs the session's rate tick nothing.
    pub(crate) went_live: Arc<Notify>,
    // Held while a piece is handed to a peer to super-seed, so two peers
    // are not handed one piece at once.
    super_seeding: Mutex<()>,
    // How many of `states` are web seeds, which are not on the peer list:
    // libtorrent keeps a web seed's peer in its `web_seed_t`, so it counts
    // against no `max_peerlist_size`.
    web_seeds: AtomicUsize,
    // Whether peers at one address may each have a connection (libtorrent
    // `allow_multiple_connections_per_ip`, off by default). The session's
    // setting, read at each judgment as libtorrent reads it into each
    // `torrent::get_peer_list_state`, so a change holds at once.
    allow_multiple_connections_per_ip: Arc<AtomicBool>,
    // Whether we were finished when the list last looked: a peer known to
    // only upload is then no connect candidate, however it comes to be
    // idle (libtorrent `peer_list::m_finished`, which
    // `is_connect_candidate` reads).
    finished: AtomicBool,
    // What the peers sent of the pieces that failed. A record names a peer
    // on the list: it leaves with the peer (libtorrent `torrent::peers_erased`
    // tells `smart_ban_plugin::on_erase_peers`), goes with the list
    // (`on_clear_peers`), and crosses a pause with it in [`PeerList`]. Never
    // taken while an entry of `states` is held.
    smart_ban: Mutex<SmartBan>,
    // The connect candidates in the order they are connected to and by when
    // they are due, the peers with a connection by host, and the peers in
    // the order the list forgets them (see [`index`]): kept as each peer is
    // counted in and out and as anything its keys read changes
    // ([`Self::reindex`]), so a connect judgment or an eviction reads a few
    // entries, not the list. Taken while an entry of `states` is held, never
    // the other way: a judgment lets it go before it takes the entry of the
    // peer it chose.
    index: Mutex<PeerIndex>,
    // The peers in `states` that are live, web seeds too: kept as each peer
    // is counted in and out, so the work done for every connection each
    // tick, piece or round visits the connections alone, as libtorrent's
    // tick walks `torrent::m_connections`, not its peer list
    // (`torrent::second_tick`). Taken while an entry of `states` is held,
    // never the other way.
    live: Mutex<IndexSet<PeerHandle>>,
    // The connection that holds each peer id a handshake sent, so one
    // client reached at two addresses keeps one connection (libtorrent
    // `allow_multiple_connections_per_pid` off): claimed at the handshake
    // ([`Self::claim_peer_id`]) and let go as that connection leaves the
    // live state, so a handshake looks up one id, never the list. Taken
    // while an entry of `states` is held, never the other way.
    peer_ids: Mutex<HashMap<Id20, PeerHandle>>,
    // Bumped by every change that can alter what [`Self::pex_listed`]
    // returns: a peer going live or leaving it, and a live peer's listen
    // address or advertised PEX flags changing. Each bump comes while the
    // changed entry of `states` is held, or after the change.
    pex_version: AtomicU64,
    // The peers our PEX messages list, built at the version it holds, and
    // shared by every connection's message until the version moves on.
    // Never taken while an entry of `states` is held.
    pex_cache: Mutex<Option<(u64, PexListed)>>,
    // Whether the peers neither a tracker nor the operator named are still
    // to leave the list once a tracker answers; see
    // [`PeerList::taking_trackers_alone`].
    untracked_owed: AtomicBool,
    // The peers asked to close because neither a tracker nor the operator
    // named them: each leaves the list when its connection has closed.
    // Held for no longer than a look or a change, never while an entry of
    // `states` is held.
    untracked_leaving: Mutex<HashSet<PeerHandle>>,
    // What connect candidates are ordered by beyond the peers themselves;
    // see [`Self::connect_next`].
    connect_order: ConnectOrder,
}

// Whether a peer named by `sources` stays on a private torrent's list once
// its trackers answer: a tracker named it, or the operator did (the peers
// the torrent was added with, which come back as its resume data).
fn tracker_or_operator_named(sources: PeerSources) -> bool {
    sources.contains(PeerSourceFlag::Tracker) || sources.contains(PeerSourceFlag::ResumeData)
}

/// The peers our PEX messages list, each where it listens, with what the
/// message says of it; see [`PeerStates::pex_listed`].
pub(crate) type PexListed = Arc<HashMap<SocketAddr, u8>>;

/// The peer list as a pause leaves it: every peer on it, idle, the banned
/// addresses, and the smart-ban records of its peers. The next run's list
/// is founded on it, so bans, trust, failures and records outlive a pause,
/// as libtorrent keeps `torrent::m_peer_list` across one, and the smart-ban
/// plugin's `m_block_hashes` with the torrent (`torrent::do_pause` only
/// disconnects).
#[derive(Debug, Default)]
pub(crate) struct PeerList {
    peers: Vec<Peer>,
    banned: HashSet<IpAddr>,
    smart_ban: SmartBan,
    // See [`Self::taking_trackers_alone`].
    untracked_owed: bool,
}

impl PeerList {
    /// The list a torrent is added with: a connect candidate at each of
    /// `peers` not at a banned address, and `banned` banned (libtorrent
    /// `torrent::read_resume_data`, which adds each of `peers` with
    /// `peer_list::add_peer` and bans each of `banned_peers` with
    /// `peer_list::ban_peer`). The peers are on the list before the torrent
    /// first runs, so a restart saves them again until it does.
    pub fn resumed(
        peers: impl IntoIterator<Item = SocketAddr>,
        banned: impl IntoIterator<Item = IpAddr>,
    ) -> Self {
        let banned: HashSet<IpAddr> = banned.into_iter().collect();
        let mut seen = HashSet::new();
        let peers = peers
            .into_iter()
            .filter(|addr| !banned.contains(&addr.ip()) && seen.insert(*addr))
            .map(|addr| Peer::new_with_outgoing_address(addr, PeerSourceFlag::ResumeData.into()))
            .collect();
        Self {
            peers,
            banned,
            ..Default::default()
        }
    }

    /// The peers and bans a restart is to begin with; see [`ResumePeers`].
    pub fn resume_peers(&self) -> ResumePeers {
        ResumePeers::of(self.peers.iter().filter_map(resume_entry), &self.banned)
    }

    /// Bans `ip` on a list no run holds: a peer at it rejoins the next run
    /// as no connect candidate, and is saved as its ban (libtorrent
    /// `peer_list::ban_peer`).
    pub fn ban(&mut self, ip: IpAddr) {
        self.banned.insert(ip);
    }

    /// The list of a private torrent whose metadata a magnet's fetch found:
    /// its peers that neither a tracker nor the operator named (the DHT's,
    /// local discovery's, incoming ones) leave it once a tracker has
    /// answered an announce of the torrent, at once when one already has,
    /// else when the first does. Until then they stay, so a torrent whose
    /// trackers never answer still downloads from them.
    pub fn taking_trackers_alone(mut self, answered: bool) -> Self {
        if !answered {
            self.untracked_owed = true;
            return self;
        }
        let smart_ban = &mut self.smart_ban;
        self.peers.retain(|peer| {
            let kept = tracker_or_operator_named(peer.sources());
            if !kept {
                smart_ban.forget(peer.addr);
            }
            kept
        });
        self
    }

    /// Whether the peers no tracker named are still to leave once a tracker
    /// answers; see [`Self::taking_trackers_alone`].
    #[cfg(test)]
    pub(crate) fn owes_untracked(&self) -> bool {
        self.untracked_owed
    }

    /// The addresses of the peers on the list.
    #[cfg(test)]
    pub(crate) fn addrs(&self) -> Vec<SocketAddr> {
        self.peers
            .iter()
            .filter_map(|peer| peer.addr.addr())
            .collect()
    }
}

// libtorrent `torrent::write_resume_data`: the peers we were never connected
// to fill the saved list up to this many.
pub(crate) const RESUME_PEERS: usize = 100;

/// The peers and bans a restart begins with (libtorrent
/// `add_torrent_params::peers` and `banned_peers`, which
/// `torrent::write_resume_data` writes). The peers come back as the
/// torrent's initial peers, the bans as its banned addresses
/// (`AddTorrentOptions::initial_peers` and `banned_peers`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResumePeers {
    /// Where to connect to each peer, those we were connected to first.
    pub peers: Vec<SocketAddr>,
    /// The banned addresses, in order.
    pub banned: Vec<IpAddr>,
}

// What a saved list needs of a peer: its address, where we can connect to
// it when we can, and whether we were ever connected to it, or None when it
// is not to be saved: we cannot connect to it (not `connectable`), its last
// connection failed (`failcount`), or it sent bad data (`trust_points`
// below zero). None for a peer at no address, which is on no list.
fn resume_entry(peer: &Peer) -> Option<(IpAddr, Option<(SocketAddr, bool)>)> {
    let failcount = peer.failcount();
    let saved = peer
        .outgoing_address
        .filter(|_| failcount == 0 && peer.trust().points() >= 0)
        .map(|addr| (addr, peer.ever_closed() || peer.connected_out().is_some()));
    Some((peer.addr.ip()?, saved))
}

impl ResumePeers {
    /// Bans `ip` on a saved list: its peers leave it, and its ban joins the
    /// rest, in order (libtorrent `torrent::write_resume_data` saves a
    /// peer at a banned address as its ban).
    pub fn ban(&mut self, ip: IpAddr) {
        self.peers.retain(|addr| addr.ip() != ip);
        if let Err(at) = self.banned.binary_search(&ip) {
            self.banned.insert(at, ip);
        }
    }

    // libtorrent `torrent::write_resume_data`: a peer at a banned address is
    // saved as its ban; every other peer to be saved that we were connected
    // to is; and of those we never were, the first [`RESUME_PEERS`], at
    // random, until that many are saved in all.
    fn of(
        entries: impl Iterator<Item = (IpAddr, Option<(SocketAddr, bool)>)>,
        banned: &HashSet<IpAddr>,
    ) -> Self {
        let mut peers = Vec::new();
        let mut never = Vec::new();
        for (ip, saved) in entries {
            match saved {
                _ if banned.contains(&ip) => {}
                Some((addr, true)) => peers.push(addr),
                Some((addr, false)) if never.len() < RESUME_PEERS => never.push(addr),
                Some(_) | None => {}
            }
        }
        rand::seq::SliceRandom::shuffle(never.as_mut_slice(), &mut rand::rng());
        let room = RESUME_PEERS.saturating_sub(peers.len());
        peers.extend(never.into_iter().take(room));
        let mut banned: Vec<IpAddr> = banned.iter().copied().collect();
        banned.sort_unstable();
        Self { peers, banned }
    }
}

/// Whom the list may forget to make room (libtorrent `peer_list::erase_peers`
/// and its `force_erase` flag). A connected peer is never forgotten.
#[derive(Clone, Copy)]
enum Displace {
    /// Only erase candidates: a peer someone named is worth no more than one
    /// we hold that never failed.
    EraseCandidates,
    /// Any peer we are not connected to, erase candidates first: a peer that
    /// connected to us outranks them.
    AnyIdle,
}

impl Displace {
    fn allows(self, (kept, ..): &EraseRank) -> bool {
        match self {
            Displace::EraseCandidates => !kept,
            Displace::AnyIdle => true,
        }
    }
}

enum Admitted<R> {
    Known(R),
    New(Arc<PeerCountersAtomic>),
    Refused,
    Banned,
    Duplicate,
}

/// When the connect loop may next connect to someone.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NextCandidate {
    Due,
    At(Instant),
    None,
}

/// What a HAVE told us that we did not know.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Have {
    /// The peer already had the piece.
    Known,
    /// The peer has a piece it did not have.
    Gained,
    /// The peer has a piece it did not have, and now has them all.
    Completed,
}

/// What a connection's handshake leaves of it when another connection holds
/// the peer id it sent; see [`PeerStates::claim_peer_id`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PeerIdClaim {
    /// The connection holds the id, and no other did.
    Held,
    /// The connection holds the id; the one at this address held it, and
    /// is to close.
    TakenFrom(PeerHandle),
    /// Another connection holds the id: this one is to close.
    Refused,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IncomingRefused {
    AlreadyActive,
    ListFull,
    Banned,
}

// The `n` lowest of `ranked`, in no order: the first `n` a full sort would
// put first. Each entry holds a peer's address, so no two are equal and the
// `n` lowest are one set, whichever way ties of rank fall.
#[cfg(test)]
fn lowest<T: Ord>(mut ranked: Vec<T>, n: usize) -> Vec<T> {
    if n == 0 {
        ranked.clear();
    } else if n < ranked.len() {
        ranked.select_nth_unstable(n - 1);
        ranked.truncate(n);
    }
    ranked
}

// The address a peer at `handle` in `state` counts at among the connected
// addresses: it has a connection, open or opening, and is at an address
// (a web seed is at none).
fn connected_addr(handle: PeerHandle, state: &PeerState) -> Option<SocketAddr> {
    handle
        .addr()
        .filter(|_| matches!(state, PeerState::Connecting(_) | PeerState::Live(_)))
}

impl Drop for PeerStates {
    fn drop(&mut self) {
        for (_, p) in std::mem::take(&mut self.states).into_iter() {
            p.destroy(self);
        }
    }
}

impl PeerStates {
    /// The list of a run, founded on the `list` the torrent was added with,
    /// a metadata fetch wrote into, or a pause left.
    pub fn founded_on(
        list: PeerList,
        session_stats: Arc<AggregatePeerStatsAtomic>,
        total_pieces: u32,
    ) -> Self {
        let connect_order = ConnectOrder::default();
        let basis = Basis {
            ranking: connect_order.now(),
            finished: false,
            allow_multiple: false,
        };
        let peers = Self {
            session_stats,
            stats: Default::default(),
            states: Default::default(),
            availability: Availability::new(total_pieces),
            total_pieces,
            admission: Mutex::new(()),
            banned: RwLock::new(list.banned),
            session_bans: Default::default(),
            candidates_changed: Notify::new(),
            went_live: Arc::new(Notify::new()),
            super_seeding: Mutex::new(()),
            web_seeds: AtomicUsize::new(0),
            allow_multiple_connections_per_ip: Default::default(),
            finished: AtomicBool::new(false),
            smart_ban: Mutex::new(list.smart_ban),
            index: Mutex::new(PeerIndex::new(basis)),
            live: Default::default(),
            peer_ids: Default::default(),
            pex_version: AtomicU64::new(0),
            pex_cache: Mutex::new(None),
            untracked_owed: AtomicBool::new(list.untracked_owed),
            untracked_leaving: Mutex::default(),
            connect_order,
        };
        for mut peer in list.peers {
            peer.rejoin(&peers);
            peers.states.insert(peer.addr, peer);
        }
        peers
    }

    /// The list, letting peers at one address each have a connection while
    /// `allow` holds: a setting it reads at each judgment, so a change to it
    /// holds from the next.
    pub fn allowing_multiple_connections_per_ip(mut self, allow: Arc<AtomicBool>) -> Self {
        self.allow_multiple_connections_per_ip = allow;
        self
    }

    /// The list, ordering its connect candidates by `order`.
    pub(crate) fn ordering_connects_by(mut self, order: ConnectOrder) -> Self {
        self.connect_order = order;
        self
    }

    /// The list, refusing the addresses `bans` holds as it refuses its own
    /// banned ones.
    pub fn sharing_session_bans(mut self, bans: Arc<SessionBans>) -> Self {
        self.session_bans = bans;
        self
    }

    /// Whether we are `finished` now. When it changed, every peer is looked
    /// at again: one known to only upload is a connect candidate only while
    /// we are not (libtorrent `peer_list::recalculate_connect_candidates`,
    /// which `find_connect_candidates` runs when `is_finished` changed).
    pub fn set_finished(&self, finished: bool) {
        if self.finished.swap(finished, Ordering::Relaxed) == finished {
            return;
        }
        // The rank of a peer a PEX message flagged a seed reads whether we
        // are finished, so every candidate is filed again in the walk the
        // change needs anyway. It flips when a download completes, or files
        // or streams are chosen anew: a handful of times a run.
        self.index.lock().basis.finished = finished;
        for mut peer in self.states.iter_mut() {
            let peer = peer.value_mut();
            peer.finished(finished, self);
            self.reindex(peer);
        }
        self.candidates_changed.notify_one();
    }

    /// Whether we were finished when the list last looked.
    pub(crate) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
    }

    /// Whether the peers a connection at their address held back may be
    /// dialed changed: the connect loop looks again.
    pub fn connections_per_ip_changed(&self) {
        self.candidates_changed.notify_one();
    }

    /// Every connection on the list closes at `closed_at` as the run ends
    /// ([`Peer::connection_let_go`]): what it showed of its peer is kept,
    /// and the close starts no wait to reconnect, so the next run dials the
    /// peer at once unless failures before it still hold it back. A peer
    /// already idle keeps the wait it had.
    pub fn close_all(&self, closed_at: Instant) {
        for mut e in self.states.iter_mut() {
            if e.value().connected_out().is_some() {
                e.value_mut().connection_let_go(closed_at, self);
            }
        }
    }

    /// The list as the next run takes it: the [`MAX_PAUSED_PEERLIST_SIZE`]
    /// peers on it that rank highest to keep, as they stand before their
    /// connections end, each without its connection; the banned addresses;
    /// and the records of the peers kept. A peer cut, and a web seed, which
    /// is on no list, leaves its records behind, as it does when it leaves
    /// the list during a run. Taking the list closes no connection; see
    /// [`Peer::idle`].
    pub fn idle_list(&self) -> PeerList {
        let mut smart_ban = self.smart_ban.lock().clone();
        let leaving = self.untracked_leaving.lock().clone();
        let mut ranked = Vec::new();
        for e in self.states.iter() {
            if !e.key().is_listed() || self.leaves_untracked(&leaving, *e.key(), e.value()) {
                smart_ban.forget(*e.key());
            } else {
                ranked.push(((erase_rank(e.value()), *e.key()), e.value().idle()));
            }
        }
        ranked.sort_unstable_by_key(|(rank, _)| *rank);
        let cut = ranked.len().saturating_sub(MAX_PAUSED_PEERLIST_SIZE);
        for ((_, handle), _) in ranked.drain(..cut) {
            smart_ban.forget(handle);
        }
        PeerList {
            peers: ranked.into_iter().map(|(_, peer)| peer).collect(),
            banned: self.banned.read().clone(),
            smart_ban,
            untracked_owed: self.untracked_owed.load(Ordering::Relaxed),
        }
    }

    // Whether `peer`, at `handle`, was asked to close because neither a
    // tracker nor the operator named it, and none has since: it leaves the
    // list once its connection has closed, so no later list keeps it.
    fn leaves_untracked(
        &self,
        leaving: &HashSet<PeerHandle>,
        handle: PeerHandle,
        peer: &Peer,
    ) -> bool {
        leaving.contains(&handle) && !tracker_or_operator_named(peer.sources())
    }

    /// Whether the peers neither a tracker nor the operator named are still
    /// to leave once a tracker answers; see [`PeerList::taking_trackers_alone`].
    pub fn owes_untracked(&self) -> bool {
        self.untracked_owed.load(Ordering::Relaxed)
    }

    /// A tracker answered an announce of the torrent: when
    /// [`Self::owes_untracked`], every peer on the list that neither a
    /// tracker nor the operator named leaves it, one with no connection at
    /// once, one with a connection when its connection, asked to close, has
    /// closed ([`Self::untracked_closed`]). Runs once.
    pub fn drop_untracked(&self) {
        if !self.untracked_owed.swap(false, Ordering::Relaxed) {
            return;
        }
        let _admission = self.admission.lock();
        let named: Vec<PeerHandle> = self
            .states
            .iter()
            .filter(|e| e.key().is_listed() && !tracker_or_operator_named(e.value().sources()))
            .map(|e| *e.key())
            .collect();
        for handle in named {
            match self
                .states
                .remove_if(&handle, |_, peer| peer.connected_out().is_none())
            {
                Some((_, peer)) => self.erased(handle, &peer),
                None => {
                    self.untracked_leaving.lock().insert(handle);
                    self.with_peer(handle, Peer::disconnect);
                }
            }
        }
    }

    /// The connection of the peer at `handle` has closed: one asked to close
    /// by [`Self::drop_untracked`] leaves the list now, unless a tracker or
    /// the operator named it since.
    pub fn untracked_closed(&self, handle: PeerHandle) {
        if !self.untracked_leaving.lock().remove(&handle) {
            return;
        }
        let _admission = self.admission.lock();
        if let Some((_, peer)) = self.states.remove_if(&handle, |_, peer| {
            peer.connected_out().is_none() && !tracker_or_operator_named(peer.sources())
        }) {
            self.erased(handle, &peer);
        }
    }

    /// The peers and bans a restart is to begin with; see [`ResumePeers`].
    /// A web seed is on no list, and is not saved.
    pub fn resume_peers(&self) -> ResumePeers {
        let leaving = self.untracked_leaving.lock().clone();
        let entries: Vec<_> = self
            .states
            .iter()
            .filter(|e| {
                e.key().is_listed() && !self.leaves_untracked(&leaving, *e.key(), e.value())
            })
            .filter_map(|e| resume_entry(e.value()))
            .collect();
        ResumePeers::of(entries.into_iter(), &self.banned.read())
    }

    /// Whether peers at `ip` are banned: by this torrent, or by the session
    /// for bad data sent to another.
    pub fn is_banned(&self, ip: IpAddr) -> bool {
        self.banned.read().contains(&ip) || self.session_bans.contains(ip)
    }

    /// Bans `ip` for sending bad data: here, as [`Self::ban`] does, and on
    /// every torrent of the session for as long as it runs. True when the
    /// session had not banned it before.
    pub fn ban_for_bad_data(&self, ip: IpAddr) -> bool {
        self.ban(ip);
        self.session_bans.insert(ip)
    }

    /// The session banned `ip` for bad data another torrent was sent: every
    /// peer at it here has its connection closed and is no connect
    /// candidate, as [`Self::ban`] leaves it, while this torrent's own bans,
    /// which a restart keeps, stay as they are.
    pub fn close_session_banned(&self, ip: IpAddr) {
        let _admission = self.admission.lock();
        for mut peer in self.states.iter_mut() {
            if peer.key().ip() == Some(ip) {
                peer.value_mut().ban(self);
            }
        }
    }

    /// Bans `ip`: no peer at it joins the list again, and every one there
    /// now has its connection closed and is no connect candidate
    /// (libtorrent `torrent::ban_peer`, `peer_list::ban_peer`).
    pub fn ban(&self, ip: IpAddr) {
        let _admission = self.admission.lock();
        if !self.banned.write().insert(ip) {
            return;
        }
        warn!(%ip, "banning the peer's address");
        for mut peer in self.states.iter_mut() {
            if peer.key().ip() == Some(ip) {
                peer.value_mut().ban(self);
            }
        }
    }

    /// Every peer on the list at an address `refused` names leaves it: one
    /// with no connection now, one with a connection once it has closed,
    /// asked to here, and [`Self::refuse_at`] runs for it (libtorrent
    /// `peer_list::apply_ip_filter`, `apply_port_filter`). A web seed is on
    /// no list.
    pub fn refuse(&self, refused: impl Fn(SocketAddr) -> bool) {
        let _admission = self.admission.lock();
        let named: Vec<PeerHandle> = self
            .states
            .iter()
            .map(|e| *e.key())
            .filter(|handle| handle.addr().is_some_and(&refused))
            .collect();
        for handle in named {
            self.leave(handle);
        }
    }

    /// [`Self::refuse`] for the one peer at `handle`, whose connection
    /// closed.
    pub fn refuse_at(&self, handle: PeerHandle, refused: impl Fn(SocketAddr) -> bool) {
        let _admission = self.admission.lock();
        if handle.addr().is_some_and(refused) {
            self.leave(handle);
        }
    }

    // The peer at `handle` leaves the list, or its connection is asked to
    // close first.
    fn leave(&self, handle: PeerHandle) {
        match self
            .states
            .remove_if(&handle, |_, peer| peer.connected_out().is_none())
        {
            Some((_, peer)) => self.erased(handle, &peer),
            None => {
                self.with_peer(handle, Peer::disconnect);
            }
        }
    }

    pub fn stats(&self) -> AggregatePeerStats {
        self.stats.snapshot()
    }

    /// `source` named `addr`: a known peer is named again, a new one joins
    /// as a connect candidate (libtorrent `peer_list::add_peer`).
    pub fn add_peer(&self, addr: SocketAddr, source: PeerSourceFlag) {
        self.admit(
            addr,
            Displace::EraseCandidates,
            None,
            |peer| peer.rediscovered(source, self),
            || Some(Peer::new_with_outgoing_address(addr, source.into())),
        );
        self.candidates_changed.notify_one();
    }

    /// A PEX message named the peer at `addr` with `flags`; see
    /// [`Peer::pex_flags`].
    pub fn pex_flags(&self, addr: SocketAddr, flags: u8) {
        self.with_peer_mut(addr.into(), "pex_flags", |peer| peer.pex_flags(flags, self));
    }

    /// Whether the connection being made to the peer at `addr` tries the
    /// encrypted handshake; see [`Peer::encrypt_outgoing`]. Every connection
    /// to the peer asks here, whether it fetches metadata or pieces.
    pub fn encrypt_outgoing(&self, addr: PeerHandle) -> bool {
        self.with_peer_mut(addr, "encrypt_outgoing", |peer| peer.encrypt_outgoing())
            .unwrap_or(true)
    }

    /// The connection of the peer at `addr` completed its handshake; see
    /// [`Peer::handshake_encryption`].
    pub fn handshake_encryption(&self, addr: PeerHandle, encryption: Encryption) {
        self.with_peer_mut(addr, "handshake_encryption", |peer| {
            peer.handshake_encryption(encryption, self)
        });
    }

    /// A peer connected to us while every slot was taken. The connection goes,
    /// the peer stays: a known one is named again, as incoming, which forgives
    /// it no failure (libtorrent `peer_list::new_connection` leaves the
    /// failcount), and one we can connect back to joins as a connect
    /// candidate.
    pub fn add_refused_incoming(&self, addr: SocketAddr, kind: ConnectionKind) {
        self.admit(
            addr,
            Displace::EraseCandidates,
            None,
            |peer| {
                atomic_inc(&peer.stats.counters.incoming_connections);
                peer.rediscovered(PeerSourceFlag::Incoming, self);
            },
            || {
                listen_address(addr, kind).map(|listen| {
                    let peer =
                        Peer::new_with_outgoing_address(listen, PeerSourceFlag::Incoming.into());
                    atomic_inc(&peer.stats.counters.incoming_connections);
                    peer
                })
            },
        );
        self.candidates_changed.notify_one();
    }

    /// A peer connected to us, on our listen port `our_port`, and holds a
    /// slot. Returns its counters.
    pub fn add_incoming(
        &self,
        addr: SocketAddr,
        peer_id: Id20,
        tx: PeerTx,
        kind: ConnectionKind,
        our_port: u16,
    ) -> Result<Arc<PeerCountersAtomic>, IncomingRefused> {
        let admitted = self.admit(
            addr,
            Displace::AnyIdle,
            Some(our_port),
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
            Admitted::Known(Err(IncomingConnectionResult::AlreadyActive)) | Admitted::Duplicate => {
                Err(IncomingRefused::AlreadyActive)
            }
            Admitted::Refused => Err(IncomingRefused::ListFull),
            Admitted::Banned => Err(IncomingRefused::Banned),
        }
    }

    // The one way into the list. A peer at a banned address is turned away
    // (libtorrent `peer_list::new_connection`, `peer_list::add_peer`). A
    // connection that came to `our_port` first settles who keeps the one
    // connection to its address. A known peer is handed to `known`. A
    // newcomer from `newcomer` joins, first pushing out the peer that ranks
    // lowest to keep when the list is full (libtorrent `peer_list::insert_peer`,
    // `new_connection`); it is refused when nobody `displace` allows is there.
    fn admit<R>(
        &self,
        addr: SocketAddr,
        displace: Displace,
        our_port: Option<u16>,
        known: impl FnOnce(&mut Peer) -> R,
        newcomer: impl FnOnce() -> Option<Peer>,
    ) -> Admitted<R> {
        let _admission = self.admission.lock();
        if self.is_banned(addr.ip()) {
            return Admitted::Banned;
        }
        let handle = PeerHandle::from(addr);
        if let Some(our_port) = our_port
            && !self.keeps_incoming(addr, our_port)
        {
            return Admitted::Duplicate;
        }
        if let Some(r) = self.with_peer_mut(handle, "admit", known) {
            return Admitted::Known(r);
        }
        let Some(mut peer) = newcomer() else {
            return Admitted::Refused;
        };
        if !self.erase_to(displace, MAX_PEERLIST_SIZE) {
            return Admitted::Refused;
        }
        self.count_in(handle, peer.get_state());
        self.reindex(&mut peer);
        for counter in [&*self.session_stats, &*self.stats] {
            atomic_inc(&counter.seen);
        }
        let counters = peer.stats.counters.clone();
        self.states.insert(handle, peer);
        Admitted::New(counters)
    }

    // One connection per address (libtorrent `allow_multiple_connections_per_ip`
    // off, `peer_list::new_connection`). Whether a connection that came from
    // `addr` to `our_port` stays, against one to another port at its address.
    // When it reached us both times, the first stays. When we made the other
    // one, both ends keep the connection made by the end with the lower
    // listen port, so both drop the same one; a tie is settled by chance.
    // The connection that loses is closed, which is no failure.
    fn keeps_incoming(&self, addr: SocketAddr, our_port: u16) -> bool {
        if self
            .allow_multiple_connections_per_ip
            .load(Ordering::Relaxed)
        {
            return true;
        }
        let others = self
            .index
            .lock()
            .connected_at(addr.ip())
            .iter()
            .copied()
            .filter(|other| *other != addr)
            .collect::<Vec<_>>();
        let Some((other, outgoing)) = others
            .into_iter()
            .find_map(|other| Some((other, self.with_peer(other.into(), Peer::connected_out)??)))
        else {
            return true;
        };
        let keep = outgoing
            && match our_port.cmp(&other.port()) {
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => rand::random(),
            };
        if keep {
            self.with_peer(other.into(), Peer::disconnect);
        }
        keep
    }

    // The addresses of the peers on the list with a connection, open or
    // opening, web seeds aside, read off the whole list; the test that the
    // index keeps them compares the two. None when peers at one address may
    // each have one: no connection then stands in another's way. No other
    // peer at one of them is connected to (libtorrent's peer list holds one
    // entry per address, and an entry with a connection is no connect
    // candidate).
    #[cfg(test)]
    pub(crate) fn connected_ips_walked(&self) -> HashSet<IpAddr> {
        if self
            .allow_multiple_connections_per_ip
            .load(Ordering::Relaxed)
        {
            return HashSet::new();
        }
        self.states
            .iter()
            .filter_map(|e| e.value().connected_out().and(e.key().ip()))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn connected_ips_counted(&self) -> HashSet<IpAddr> {
        if self
            .allow_multiple_connections_per_ip
            .load(Ordering::Relaxed)
        {
            return HashSet::new();
        }
        self.index.lock().connected_ips()
    }

    /// How many peers we could connect to, due or not, at an address no
    /// connection holds back (libtorrent `peer_list::num_connect_candidates`).
    pub fn connect_candidates(&self) -> usize {
        self.index_blocking_now().candidates()
    }

    /// [`Self::connect_candidates`] counted off the whole list.
    #[cfg(test)]
    pub(crate) fn connect_candidates_walked(&self) -> usize {
        let connected = self.connected_ips_walked();
        self.states
            .iter()
            .filter(|e| {
                e.value().is_connect_candidate()
                    && e.key().ip().is_some_and(|ip| !connected.contains(&ip))
            })
            .count()
    }

    // The index, its connections holding back the candidates at their
    // address as the setting says now. Only an operator's change to the
    // session setting moves it, and the move visits the addresses with a
    // connection alone.
    fn index_blocking_now(&self) -> parking_lot::MutexGuard<'_, PeerIndex> {
        let mut index = self.index.lock();
        index.allow_multiple(
            self.allow_multiple_connections_per_ip
                .load(Ordering::Relaxed),
        );
        index
    }

    // Brings the index to what candidates are ranked by now: when the
    // endpoints peers see us at moved, or whether we are finished did
    // without [`Self::set_finished`] filing them again, every candidate is
    // ranked anew in one walk. The endpoints move when the vote on our
    // external address changes its winner or a port mapping changes: a
    // handful of times a run. Not called while an entry of `states` is held.
    fn rebase(&self) {
        let ranking = self.connect_order.now();
        let finished = self.is_finished();
        {
            let mut index = self.index_blocking_now();
            if index.basis.ranking == ranking && index.basis.finished == finished {
                return;
            }
            index.basis.ranking = ranking;
            index.basis.finished = finished;
        }
        for mut e in self.states.iter_mut() {
            self.reindex(e.value_mut());
        }
    }

    /// Files `peer` again wherever what its keys read changed: its connect
    /// candidacy, rank and due time, and its place in the erase order. Every
    /// change to a peer that can move one calls it, with the peer's entry
    /// held or the peer not yet on the list. A change that moves nothing
    /// takes no lock unless the peer is a connect candidate.
    pub(super) fn reindex(&self, peer: &mut Peer) {
        // The index holds the peers on the list, by address.
        let Some(addr) = peer.addr.addr().filter(|_| peer.addr.is_listed()) else {
            return;
        };
        let erase = peer.connected_out().is_none().then(|| erase_rank(peer));
        let candidate = peer.is_connect_candidate();
        let filed = peer.indexed();
        if !candidate && !filed.candidate && erase == filed.erase {
            return;
        }
        let mut index = self.index.lock();
        if erase != filed.erase {
            index.file_erase(addr, filed.erase, erase);
        }
        if candidate || filed.candidate {
            let key = candidate.then(|| ConnectKey {
                rank: peer.connect_rank(&index.basis.ranking, index.basis.finished),
                due: peer.reconnect_at(),
            });
            index.file(addr, key);
        }
        peer.set_indexed(Indexed { candidate, erase });
    }

    // [`Self::reindex`] from nothing: the peer's connect key is filed anew
    // even when the index holds one it thinks current.
    fn refile(&self, peer: &mut Peer) {
        let filed = peer.indexed();
        if filed.candidate
            && let Some(addr) = peer.addr.addr()
        {
            self.index.lock().file(addr, None);
            peer.set_indexed(Indexed {
                candidate: false,
                ..filed
            });
        }
        self.reindex(peer);
    }

    // `peer`, at `handle`, left the table: it leaves the index.
    fn unindex(&self, handle: PeerHandle, peer: &Peer) {
        let filed = peer.indexed();
        // Only a peer at an address is filed.
        let Some(addr) = handle.addr() else {
            return;
        };
        if !filed.candidate && filed.erase.is_none() {
            return;
        }
        let mut index = self.index.lock();
        index.file_erase(addr, filed.erase, None);
        if filed.candidate {
            index.file(addr, None);
        }
    }

    /// Closes the connections of the `n` peers that rank lowest to keep
    /// (libtorrent `torrent::disconnect_peers`); see [`Peer::disconnect_rank`].
    pub fn disconnect_lowest(&self, n: usize, now: Instant) {
        let mut ranked = self
            .states
            .iter()
            .filter_map(|e| Some((e.value().disconnect_rank(now)?, *e.key())))
            .collect::<Vec<_>>();
        ranked.sort_unstable();
        for (_, addr) in ranked.into_iter().take(n) {
            self.with_peer(addr, Peer::disconnect);
        }
    }

    // The peers on the peer list: every one but the web seeds.
    fn listed(&self) -> usize {
        self.states.len() - self.web_seeds.load(Ordering::Relaxed)
    }

    /// A connection to web seed `handle` begins: the seed joins the table,
    /// off the peer list, and is connecting. None while a connection to it
    /// is open. A seed banned for bad data is banned in [`WebSeeds`], and
    /// is not connected to again.
    ///
    /// [`WebSeeds`]: super::web_seed::WebSeeds
    pub fn admit_web_seed(&self, handle: PeerHandle) -> Option<(PeerRx, PeerTx)> {
        let _admission = self.admission.lock();
        let (tx, rx) = unbounded_channel();
        let connecting = PeerState::Connecting(tx.clone());
        if let Some(known) = self.with_peer_mut(handle, "admit_web_seed", |peer| {
            if matches!(
                peer.get_state(),
                PeerState::Connecting(_) | PeerState::Live(_)
            ) {
                return false;
            }
            peer.set_state(connecting, self);
            true
        }) {
            return known.then_some((rx, tx));
        }
        let mut peer = Peer::new_web_seed(handle);
        self.count_in(handle, peer.get_state());
        peer.set_state(PeerState::Connecting(tx.clone()), self);
        self.web_seeds.fetch_add(1, Ordering::Relaxed);
        self.states.insert(handle, peer);
        Some((rx, tx))
    }

    // Forgets the peers `displace` allows, lowest-ranked first, until the
    // list holds fewer than `fewer_than`; returns whether it does. Connected
    // peers and web seeds, which are on no peer list, are never forgotten.
    // The index holds the rest in the order they go, so this reads as many
    // as go. Under the admission lock.
    fn erase_to(&self, displace: Displace, fewer_than: usize) -> bool {
        let excess = (self.listed() + 1).saturating_sub(fewer_than);
        if excess == 0 {
            return true;
        }
        let victims = self.index.lock().erase_victims(displace, excess);
        for addr in victims.into_iter().map(PeerHandle::from) {
            if let Some((_, peer)) = self
                .states
                .remove_if(&addr, |_, peer| peer.connected_out().is_none())
            {
                self.erased(addr, &peer);
            }
        }
        self.listed() < fewer_than
    }

    /// The `n` peers [`Self::erase_to`] forgets first of those `displace`
    /// allows, as the index holds them.
    #[cfg(test)]
    fn erase_victims(&self, displace: Displace, n: usize) -> BTreeSet<SocketAddr> {
        self.index
            .lock()
            .erase_victims(displace, n)
            .into_iter()
            .collect()
    }

    /// [`Self::erase_victims`] read off the whole list.
    #[cfg(test)]
    fn erase_victims_walked(&self, displace: Displace, n: usize) -> BTreeSet<SocketAddr> {
        let ranked = self
            .states
            .iter()
            .filter(|e| e.key().is_listed() && e.value().connected_out().is_none())
            .filter_map(|e| Some((erase_rank(e.value()), e.key().addr()?)))
            .filter(|(rank, _)| displace.allows(rank))
            .collect::<Vec<_>>();
        lowest(ranked, n)
            .into_iter()
            .map(|(_, addr)| addr)
            .collect()
    }

    // `peer`, at `handle`, left the table: what was counted of it, its
    // place in the index, and the records of what it sent, go with it.
    fn erased(&self, handle: PeerHandle, peer: &Peer) {
        if !handle.is_listed() {
            self.web_seeds.fetch_sub(1, Ordering::Relaxed);
        }
        self.count_out(handle, peer.get_state());
        self.unindex(handle, peer);
        self.smart_ban.lock().forget(handle);
    }

    /// Piece `piece` failed its hash check and `blocks` are its blocks as
    /// read back: those of senders on the list are remembered. Returns the
    /// senders that had sent a block of it before with other bytes; see
    /// [`SmartBan::piece_failed`]. A sender no longer on the list blames
    /// nobody (libtorrent `smart_ban_plugin::on_read_failed_block`: "there
    /// is no peer with this address anymore").
    pub fn piece_failed(
        &self,
        piece: ValidPieceIndex,
        blocks: Vec<SentBlock>,
    ) -> BTreeSet<PeerHandle> {
        let mut smart_ban = self.smart_ban.lock();
        let known = blocks
            .into_iter()
            .filter(|b| self.states.contains_key(&b.sender()));
        smart_ban.piece_failed(piece, known)
    }

    /// Piece `piece` passed its hash check: the records of its failures,
    /// which leave with it; see [`SmartBan::piece_passed`].
    pub fn piece_passed(&self, piece: ValidPieceIndex) -> Vec<SentBlock> {
        self.smart_ban.lock().piece_passed(piece)
    }

    /// When the connect loop may next connect to someone: a candidate at an
    /// address no connection holds back is due at `now`, or will be at a
    /// time, or none is there.
    pub fn next_candidate(&self, now: Instant) -> NextCandidate {
        self.index_blocking_now().next_candidate(now)
    }

    /// [`Self::next_candidate`] read off the whole list.
    #[cfg(test)]
    pub(crate) fn next_candidate_walked(&self, now: Instant) -> NextCandidate {
        let connected = self.connected_ips_walked();
        let mut next = NextCandidate::None;
        for e in self.states.iter() {
            let peer = e.value();
            if !peer.is_connect_candidate() || e.key().ip().is_none_or(|ip| connected.contains(&ip))
            {
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

    /// Starts connecting to the connect candidate due at `now` that ranks
    /// first ([`Peer::connect_rank`]), at an address no connection is at.
    /// Under the admission lock, so none comes from there meanwhile. A list
    /// grown to its low watermark first forgets erase candidates,
    /// lowest-ranked first, until it is under it (libtorrent
    /// `peer_list::find_connect_candidates`).
    pub fn connect_next(&self, now: Instant) -> Option<(SocketAddr, PeerRx, PeerTx)> {
        let _admission = self.admission.lock();
        self.erase_to(Displace::EraseCandidates, PEERLIST_LOW_WATERMARK);
        loop {
            let addr = self.connect_pick(now)?;
            // The index is let go before the entry is taken. A peer it holds
            // that is not due after all, or is gone, is filed again from
            // what it is, so the next pick does not choose it again.
            match self.with_peer_mut(addr.into(), "connect_next", |peer| {
                let connecting = peer.connect_if_due(now, self);
                if connecting.is_none() {
                    self.refile(peer);
                }
                connecting
            }) {
                Some(Some((rx, tx))) => return Some((addr, rx, tx)),
                Some(None) => {}
                None => self.index.lock().file(addr, None),
            }
        }
    }

    // The candidate [`Self::connect_next`] connects to at `now`, as the
    // index holds it.
    fn connect_pick(&self, now: Instant) -> Option<SocketAddr> {
        self.rebase();
        self.index.lock().first_due(now)
    }

    /// The candidate [`Self::connect_next`] connects to at `now`, without
    /// connecting.
    #[cfg(test)]
    pub(crate) fn connect_pick_indexed(&self, now: Instant) -> Option<SocketAddr> {
        self.connect_pick(now)
    }

    /// [`Self::connect_pick_indexed`] read off the whole list: the due
    /// candidate at an address no connection is at that ranks first.
    #[cfg(test)]
    pub(crate) fn connect_pick_walked(&self, now: Instant) -> Option<SocketAddr> {
        let connected = self.connected_ips_walked();
        let finished = self.is_finished();
        let order = self.connect_order.now();
        self.states
            .iter()
            .filter_map(|e| {
                let addr = e.key().addr()?;
                (e.value().is_due(now) && !connected.contains(&addr.ip()))
                    .then(|| (e.value().connect_rank(&order, finished), addr))
            })
            .min()
            .map(|(_, addr)| addr)
    }

    /// Settles a peer whose connection closed; see [`Peer::connection_closed`].
    /// The peer, or another at its address, may be a connect candidate now.
    pub fn connection_closed(
        &self,
        peer: &mut Peer,
        closed_at: Instant,
        failed: bool,
        finished: bool,
    ) {
        peer.connection_closed(closed_at, failed, finished, self);
        self.candidates_changed.notify_one();
    }

    /// `peer`, whose connection closed (see [`Peer::connection_closing`]),
    /// settles: a connect candidate again when `needed`.
    pub fn settle_closed(&self, peer: &mut Peer, needed: bool) {
        peer.settle_closed(needed, self);
        self.candidates_changed.notify_one();
    }

    /// The peer's connection completed its handshake and carried a message.
    pub fn connection_delivered(&self, addr: PeerHandle) {
        self.with_peer_mut(addr, "connection_delivered", |peer| peer.delivered(self));
    }

    /// Upload mode was left: every peer with no connection that is not
    /// banned may be connected to at once, its failures in a row kept, and
    /// the connect loop is woken (libtorrent `torrent::set_upload_mode`
    /// clears every `torrent_peer::last_connected` to force a fast
    /// reconnect after leaving upload mode). One walk of the list per leave.
    pub fn reconnect_after_upload_mode(&self) {
        for mut e in self.states.iter_mut() {
            let banned = e.key().ip().is_some_and(|ip| self.is_banned(ip));
            let peer = e.value_mut();
            if !banned && peer.forget_last_close() {
                self.reindex(peer);
            }
        }
        self.candidates_changed.notify_one();
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

    // The live peers as they stand now. The set's lock is let go before any
    // entry of `states` is taken.
    fn live_handles(&self) -> Vec<PeerHandle> {
        self.live.lock().iter().copied().collect()
    }

    /// Runs `f` on every live peer, web seeds too, one entry held at a
    /// time. Visits the live peers alone, not the whole list.
    pub(crate) fn for_each_live(&self, mut f: impl FnMut(PeerHandle, &Peer, &LivePeerState)) {
        for handle in self.live_handles() {
            if let Some(e) = self.states.get(&handle)
                && let Some(live) = e.value().get_live()
            {
                f(handle, e.value(), live);
            }
        }
    }

    /// [`Self::for_each_live`], with the peer to change.
    pub(crate) fn for_each_live_mut(&self, mut f: impl FnMut(PeerHandle, &mut Peer)) {
        for handle in self.live_handles() {
            if let Some(mut e) = self.states.get_mut(&handle)
                && e.value().get_live().is_some()
            {
                f(handle, e.value_mut());
            }
        }
    }

    // The live peers read off the whole list, as before the running set;
    // the test that the set keeps them compares the two.
    #[cfg(test)]
    pub(crate) fn live_walked(&self) -> HashSet<PeerHandle> {
        self.states
            .iter()
            .filter(|e| e.value().get_live().is_some())
            .map(|e| *e.key())
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn live_counted(&self) -> HashSet<PeerHandle> {
        self.live.lock().iter().copied().collect()
    }

    /// The peers our PEX messages list, each where it listens, with what
    /// the message says of it (libtorrent `ut_pex_plugin::tick`,
    /// `send_peer`): every connected peer whose listening address we know,
    /// as we connected to it there or it told us; never a web seed. Built
    /// once and shared by every connection's message until a change that
    /// can alter it ([`Self::pex_changed`]).
    pub fn pex_listed(&self) -> PexListed {
        let mut cache = self.pex_cache.lock();
        // Read before the build: a change during it moves the version on,
        // so the next call builds again.
        let version = self.pex_version.load(Ordering::Acquire);
        if let Some((built_at, listed)) = &*cache
            && *built_at == version
        {
            return listed.clone();
        }
        let listed = Arc::new(self.pex_listed_now());
        *cache = Some((version, listed.clone()));
        listed
    }

    /// Something [`Self::pex_listed`] returns may have changed: a peer went
    /// live or left it, or a live peer's listen address or advertised PEX
    /// flags changed. Called while the changed entry is held, or after the
    /// change.
    pub(crate) fn pex_changed(&self) {
        self.pex_version.fetch_add(1, Ordering::Release);
    }

    // [`Self::pex_listed`] built now, from the live peers.
    fn pex_listed_now(&self) -> HashMap<SocketAddr, u8> {
        let mut listed = HashMap::new();
        self.for_each_live(|_, peer, live| {
            if let Some(addr) = peer.outgoing_address {
                listed.insert(addr, live.advertised_pex_flags());
            }
        });
        listed
    }

    // [`Self::pex_listed`] read off the whole list.
    #[cfg(test)]
    pub(crate) fn pex_listed_walked(&self) -> HashMap<SocketAddr, u8> {
        self.states
            .iter()
            .filter_map(|e| {
                let peer = e.value();
                let flags = peer.get_live()?.advertised_pex_flags();
                Some((peer.outgoing_address?, flags))
            })
            .collect()
    }

    pub fn drop_peer(&self, handle: PeerHandle) -> Option<Peer> {
        let mut p = self.states.remove(&handle).map(|r| r.1)?;
        self.erased(handle, &p);
        p.set_indexed(Indexed::default());
        Some(p)
    }

    /// Counts a peer at `addr` in `state` into the torrent and session peer
    /// stats, its address into the connected ones when it has a connection,
    /// and, when it is live, its bitfield into the piece availability.
    pub(super) fn count_in(&self, addr: PeerHandle, state: &PeerState) {
        for counter in [&*self.session_stats, &*self.stats] {
            counter.inc(state);
        }
        if let Some(at) = connected_addr(addr, state) {
            self.index.lock().connection_opened(at);
        }
        if let PeerState::Live(live) = state {
            self.live.lock().insert(addr);
            self.pex_changed();
            self.availability.add(&live.bitfield);
            self.went_live.notify_one();
        }
    }

    /// Takes back what [`Self::count_in`] counted for a peer leaving `state`.
    /// Every way a connection ends passes here once, through
    /// [`Peer::set_state`] or the peer leaving the list.
    pub(super) fn count_out(&self, addr: PeerHandle, state: &PeerState) {
        for counter in [&*self.session_stats, &*self.stats] {
            counter.dec(state);
        }
        if let Some(at) = connected_addr(addr, state) {
            self.index.lock().connection_closed(at);
        }
        if let PeerState::Live(live) = state {
            self.live.lock().swap_remove(&addr);
            self.pex_changed();
            self.availability.remove(&live.bitfield);
            let mut ids = self.peer_ids.lock();
            if ids.get(&live.peer_id()) == Some(&addr) {
                ids.remove(&live.peer_id());
            }
        }
    }

    /// The live connection at `handle`, which we made when `outgoing`, sent
    /// `peer_id` in its handshake, and `ours` is the id we sent. One
    /// connection holds each id. When another holds it, the end with the
    /// greater id keeps the connection it made, so both ends close the
    /// same one: this connection takes the id when it is outgoing and the
    /// peer's id is below ours, or incoming and above it; else it is
    /// refused (libtorrent `bt_peer_connection::on_receive`, "read_peer_id",
    /// with `allow_multiple_connections_per_pid` off). Our own id is the
    /// self connection, which ends on its own rule, and is held by none.
    pub(crate) fn claim_peer_id(
        &self,
        handle: PeerHandle,
        peer_id: Id20,
        outgoing: bool,
        ours: Id20,
    ) -> PeerIdClaim {
        if peer_id == ours {
            return PeerIdClaim::Held;
        }
        match self.peer_ids.lock().entry(peer_id) {
            std::collections::hash_map::Entry::Vacant(at) => {
                at.insert(handle);
                PeerIdClaim::Held
            }
            std::collections::hash_map::Entry::Occupied(at) if *at.get() == handle => {
                PeerIdClaim::Held
            }
            std::collections::hash_map::Entry::Occupied(mut at) => {
                if (peer_id < ours) == outgoing {
                    PeerIdClaim::TakenFrom(at.insert(handle))
                } else {
                    PeerIdClaim::Refused
                }
            }
        }
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
    pub(crate) fn peer_has(&self, handle: PeerHandle, piece: ValidPieceIndex) -> Option<Have> {
        self.with_live_mut(handle, "peer_has", |live| {
            if live.bitfield.replace(piece.get_usize(), true) {
                return Have::Known;
            }
            let seeder = live.seeder;
            self.update_seeder_flag(live);
            self.availability
                .peer_has(piece.get_usize(), live.seeder.then_some(&*live.bitfield));
            if live.seeder != seeder {
                Have::Completed
            } else {
                Have::Gained
            }
        })
    }

    /// The peer sent DONT_HAVE for `piece`: it leaves the peer's pieces and
    /// the availability, and a seeder is one no longer (libtorrent
    /// `peer_connection::incoming_dont_have`, `torrent::peer_lost`). One
    /// for a piece the peer does not have changes nothing.
    pub fn peer_lost(&self, handle: PeerHandle, piece: ValidPieceIndex) -> Option<()> {
        self.with_live_mut(handle, "peer_lost", |live| {
            if !live.bitfield.replace(piece.get_usize(), false) {
                return;
            }
            let seeder = live.seeder;
            self.update_seeder_flag(live);
            self.availability
                .peer_lost(piece.get_usize(), seeder.then_some(&*live.bitfield));
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
        self.pex_changed();
        self.stats.seeder_flag_changed(seeder);
        self.session_stats.seeder_flag_changed(seeder);
    }

    pub(crate) fn on_steal(
        &self,
        from_peer: PeerHandle,
        to_peer: PeerHandle,
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
        self.for_each_live(|_, _, live| {
            if !live.bitfield.get(piece.get_usize()).is_some_and(|has| *has) {
                let _ = live
                    .tx
                    .send(WriterRequest::Message(Message::Have(piece.get())));
            }
        });
    }

    /// Tells every live peer that takes upload_only whether we only upload
    /// now (libtorrent `torrent::send_upload_only`).
    pub(crate) fn send_upload_only(&self, enabled: bool) {
        self.for_each_live(|_, _, live| {
            if live.supports_upload_only {
                let _ = live.tx.send(WriterRequest::Message(Message::Extended(
                    ExtendedMessage::UploadOnly(enabled),
                )));
            }
        });
    }
}

#[cfg(test)]
mod tests;
