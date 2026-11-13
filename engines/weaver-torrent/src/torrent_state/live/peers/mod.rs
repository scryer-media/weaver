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
    stats::{AggregatePeerStats, AggregatePeerStatsAtomic},
};

use super::smart_ban::{SentBlock, SmartBan};
use super::web_seed::is_web_seed;

use super::peer::{
    IncomingConnectionResult, LivePeerState, Peer, PeerRx, PeerState, PeerTx, listen_address,
    stats::atomic::PeerCountersAtomic,
};

pub mod availability;
pub(crate) mod holepunch;
pub mod stats;
mod super_seed;

// libtorrent `max_peerlist_size`: the most peers one torrent's peer list holds.
pub(crate) const MAX_PEERLIST_SIZE: usize = 3000;
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
/// candidate and has failed; then the most failures in a row; then one we
/// cannot connect to; then the least trusted.
type EraseRank = (bool, Reverse<u32>, bool, i8);

fn erase_rank(peer: &Peer) -> EraseRank {
    let (_, failcount, _) = peer.connect_rank();
    let erase_candidate =
        peer.connected_out().is_none() && !peer.is_connect_candidate() && failcount > 0;
    (
        !erase_candidate,
        Reverse(failcount),
        peer.outgoing_address == Some(peer.addr),
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
    // The peers on the list with a connection, open or opening, web seeds
    // aside, by host: kept as each peer is counted in and out, so a connect
    // judgment does not walk the list for them. Taken while an entry of
    // `states` is held, never the other way.
    connected: Mutex<HashMap<IpAddr, Vec<SocketAddr>>>,
    // The peers in `states` that are live, web seeds too: kept as each peer
    // is counted in and out, so the work done for every connection each
    // tick, piece or round visits the connections alone, as libtorrent's
    // tick walks `torrent::m_connections`, not its peer list
    // (`torrent::second_tick`). Taken while an entry of `states` is held,
    // never the other way.
    live: Mutex<IndexSet<PeerHandle>>,
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
        ResumePeers::of(self.peers.iter().map(resume_entry), &self.banned)
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
        self.peers.iter().map(|peer| peer.addr).collect()
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
// below zero).
fn resume_entry(peer: &Peer) -> (IpAddr, Option<(SocketAddr, bool)>) {
    let (_, failcount, (_, last_connected)) = peer.connect_rank();
    let saved = peer
        .outgoing_address
        .filter(|_| failcount == 0 && peer.trust().points() >= 0)
        .map(|addr| {
            (
                addr,
                last_connected.is_some() || peer.connected_out().is_some(),
            )
        });
    (peer.addr.ip(), saved)
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

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IncomingRefused {
    AlreadyActive,
    ListFull,
    Banned,
}

// The `n` lowest of `ranked`, in no order: the first `n` a full sort would
// put first. Each entry holds a peer's address, so no two are equal and the
// `n` lowest are one set, whichever way ties of rank fall.
fn lowest<T: Ord>(mut ranked: Vec<T>, n: usize) -> Vec<T> {
    if n == 0 {
        ranked.clear();
    } else if n < ranked.len() {
        ranked.select_nth_unstable(n - 1);
        ranked.truncate(n);
    }
    ranked
}

// Whether a peer at `addr` in `state` counts among the connected
// addresses: it has a connection, open or opening, and is no web seed.
fn has_connection(addr: PeerHandle, state: &PeerState) -> bool {
    !is_web_seed(addr) && matches!(state, PeerState::Connecting(_) | PeerState::Live(_))
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
            connected: Default::default(),
            live: Default::default(),
            pex_version: AtomicU64::new(0),
            pex_cache: Mutex::new(None),
            untracked_owed: AtomicBool::new(list.untracked_owed),
            untracked_leaving: Mutex::default(),
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
        for mut peer in self.states.iter_mut() {
            peer.value_mut().finished(finished, self);
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
                e.value_mut().connection_let_go(closed_at);
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
            if is_web_seed(*e.key()) || self.leaves_untracked(&leaving, *e.key(), e.value()) {
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
            .filter(|e| !is_web_seed(*e.key()) && !tracker_or_operator_named(e.value().sources()))
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
                !is_web_seed(*e.key()) && !self.leaves_untracked(&leaving, *e.key(), e.value())
            })
            .map(|e| resume_entry(e.value()))
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
            if peer.key().ip() == ip {
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
            if peer.key().ip() == ip {
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
            .filter(|handle| !is_web_seed(*handle) && refused(*handle))
            .collect();
        for handle in named {
            self.leave(handle);
        }
    }

    /// [`Self::refuse`] for the one peer at `handle`, whose connection
    /// closed.
    pub fn refuse_at(&self, handle: PeerHandle, refused: impl Fn(SocketAddr) -> bool) {
        let _admission = self.admission.lock();
        if !is_web_seed(handle) && refused(handle) {
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
        self.with_peer_mut(addr, "pex_flags", |peer| peer.pex_flags(flags));
    }

    /// Whether the connection being made to the peer at `addr` tries the
    /// encrypted handshake; see [`Peer::encrypt_outgoing`]. Every connection
    /// to the peer asks here, whether it fetches metadata or pieces.
    pub fn encrypt_outgoing(&self, addr: SocketAddr) -> bool {
        self.with_peer_mut(addr, "encrypt_outgoing", |peer| peer.encrypt_outgoing())
            .unwrap_or(true)
    }

    /// The connection of the peer at `addr` completed its handshake; see
    /// [`Peer::handshake_encryption`].
    pub fn handshake_encryption(&self, addr: SocketAddr, encryption: Encryption) {
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
        if let Some(our_port) = our_port
            && !self.keeps_incoming(addr, our_port)
        {
            return Admitted::Duplicate;
        }
        if let Some(r) = self.with_peer_mut(addr, "admit", known) {
            return Admitted::Known(r);
        }
        let Some(peer) = newcomer() else {
            return Admitted::Refused;
        };
        if !self.erase_to(displace, MAX_PEERLIST_SIZE) {
            return Admitted::Refused;
        }
        self.count_in(addr, peer.get_state());
        for counter in [&*self.session_stats, &*self.stats] {
            atomic_inc(&counter.seen);
        }
        let counters = peer.stats.counters.clone();
        self.states.insert(addr, peer);
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
            .connected
            .lock()
            .get(&addr.ip())
            .map(|at| {
                at.iter()
                    .copied()
                    .filter(|other| *other != addr)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let Some((other, outgoing)) = others
            .into_iter()
            .find_map(|other| Some((other, self.with_peer(other, Peer::connected_out)??)))
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
            self.with_peer(other, Peer::disconnect);
        }
        keep
    }

    // The addresses a peer on the list has a connection, open or opening,
    // at. No other peer there is connected to (libtorrent's peer list holds
    // one entry per address, and an entry with a connection is no connect
    // candidate).
    // The addresses of the peers on the list with a connection, open or
    // opening, web seeds aside. None when peers at one address may each have
    // one: no connection then stands in another's way.
    fn connected_ips(&self) -> HashSet<IpAddr> {
        if self
            .allow_multiple_connections_per_ip
            .load(Ordering::Relaxed)
        {
            return HashSet::new();
        }
        self.connected.lock().keys().copied().collect()
    }

    // `connected_ips` read off the whole list, as it was before the running
    // count; the test that the count keeps it compares the two.
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
            .filter(|e| !is_web_seed(*e.key()))
            .filter_map(|e| e.value().connected_out().map(|_| e.key().ip()))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn connected_ips_counted(&self) -> HashSet<IpAddr> {
        self.connected_ips()
    }

    /// How many peers we could connect to (libtorrent
    /// `peer_list::num_connect_candidates`).
    pub fn connect_candidates(&self) -> usize {
        let connected = self.connected_ips();
        self.states
            .iter()
            .filter(|e| e.value().is_connect_candidate() && !connected.contains(&e.key().ip()))
            .count()
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
    /// off the peer list, and is connecting. None while its address is
    /// banned or a connection to it is open.
    pub fn admit_web_seed(&self, handle: PeerHandle) -> Option<(PeerRx, PeerTx)> {
        let _admission = self.admission.lock();
        if self.is_banned(handle.ip()) {
            return None;
        }
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
        // Not where it listens: a web seed is never a connect candidate.
        let mut peer = Peer::new_with_outgoing_address(handle, Default::default());
        peer.outgoing_address = None;
        self.count_in(handle, peer.get_state());
        peer.set_state(PeerState::Connecting(tx.clone()), self);
        self.web_seeds.fetch_add(1, Ordering::Relaxed);
        self.states.insert(handle, peer);
        Some((rx, tx))
    }

    // Forgets the peers `displace` allows, lowest-ranked first, until the
    // list holds fewer than `fewer_than`; returns whether it does. Connected
    // peers and web seeds, which are on no peer list, are never forgotten.
    // Under the admission lock.
    fn erase_to(&self, displace: Displace, fewer_than: usize) -> bool {
        let excess = (self.listed() + 1).saturating_sub(fewer_than);
        if excess == 0 {
            return true;
        }
        let ranked = self
            .states
            .iter()
            .filter(|e| !is_web_seed(*e.key()) && e.value().connected_out().is_none())
            .map(|e| (erase_rank(e.value()), *e.key()))
            .filter(|(rank, _)| displace.allows(rank))
            .collect::<Vec<_>>();
        for (_, addr) in lowest(ranked, excess) {
            if let Some((_, peer)) = self
                .states
                .remove_if(&addr, |_, peer| peer.connected_out().is_none())
            {
                self.erased(addr, &peer);
            }
        }
        self.listed() < fewer_than
    }

    // `peer`, at `handle`, left the table: what was counted of it, and the
    // records of what it sent, go with it.
    fn erased(&self, handle: PeerHandle, peer: &Peer) {
        if is_web_seed(handle) {
            self.web_seeds.fetch_sub(1, Ordering::Relaxed);
        }
        self.count_out(handle, peer.get_state());
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

    /// When the connect loop may next connect to someone.
    pub fn next_candidate(&self, now: Instant) -> NextCandidate {
        let connected = self.connected_ips();
        let mut next = NextCandidate::None;
        for e in self.states.iter() {
            let peer = e.value();
            if !peer.is_connect_candidate() || connected.contains(&e.key().ip()) {
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
    /// least, then longest ago, at an address no connection is at. Under the
    /// admission lock, so none comes from there meanwhile. A list grown to
    /// its low watermark first forgets erase candidates, lowest-ranked
    /// first, until it is under it (libtorrent
    /// `peer_list::find_connect_candidates`).
    pub fn connect_next(&self, now: Instant) -> Option<(PeerHandle, PeerRx, PeerTx)> {
        let _admission = self.admission.lock();
        self.erase_to(Displace::EraseCandidates, PEERLIST_LOW_WATERMARK);
        let connected = self.connected_ips();
        let (_, addr) = self
            .states
            .iter()
            .filter(|e| e.value().is_due(now) && !connected.contains(&e.key().ip()))
            .map(|e| (e.value().connect_rank(), *e.key()))
            .min()?;
        let (rx, tx) = self
            .with_peer_mut(addr, "connect_next", |peer| peer.connect_if_due(now, self))
            .flatten()?;
        Some((addr, rx, tx))
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
        let p = self.states.remove(&handle).map(|r| r.1)?;
        self.erased(handle, &p);
        Some(p)
    }

    /// Counts a peer at `addr` in `state` into the torrent and session peer
    /// stats, its address into the connected ones when it has a connection,
    /// and, when it is live, its bitfield into the piece availability.
    pub(super) fn count_in(&self, addr: PeerHandle, state: &PeerState) {
        for counter in [&*self.session_stats, &*self.stats] {
            counter.inc(state);
        }
        if has_connection(addr, state) {
            self.connected
                .lock()
                .entry(addr.ip())
                .or_default()
                .push(addr);
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
        if has_connection(addr, state) {
            let mut connected = self.connected.lock();
            if let std::collections::hash_map::Entry::Occupied(mut at) = connected.entry(addr.ip())
            {
                if let Some(i) = at.get().iter().position(|held| *held == addr) {
                    at.get_mut().swap_remove(i);
                }
                if at.get().is_empty() {
                    at.remove();
                }
            }
        }
        if let PeerState::Live(live) = state {
            self.live.lock().swap_remove(&addr);
            self.pex_changed();
            self.availability.remove(&live.bitfield);
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
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::peer_protocol::extended::ut_pex::PexPeerInfo;
    use crate::torrent_state::live::peer::{MAX_FAILCOUNT, MAX_PEX_PEERS, MIN_RECONNECT_TIME};

    const TOTAL_PIECES: usize = 8;

    fn full_bitfield() -> BF {
        BF::from_boxed_slice(vec![0xffu8; 1].into_boxed_slice())
    }

    fn empty_bitfield() -> BF {
        BF::from_boxed_slice(vec![0u8; 1].into_boxed_slice())
    }

    // The port the tests' incoming connections come to.
    const OUR_PORT: u16 = 6881;

    fn make_peer_states() -> PeerStates {
        PeerStates::founded_on(
            PeerList::default(),
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        )
    }

    // Returns the peer handle, keeping the receiving end of the peer channel alive.
    fn add_live_peer(peers: &PeerStates, addr: &str) -> (PeerHandle, PeerRx) {
        let addr: SocketAddr = addr.parse().unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(addr, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
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
                .peer_has(
                    addr,
                    self::piece(
                        u32::try_from(TOTAL_PIECES).unwrap(),
                        u32::try_from(piece).unwrap(),
                    ),
                )
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
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");

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
        let (thief, _thief_rx) = add_live_peer(&peers, "127.0.0.2:1235");

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
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");
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

        peers
            .peer_has(a, piece(u32::try_from(TOTAL_PIECES).unwrap(), 5))
            .unwrap();
        assert_eq!(availability(&peers), [1, 0, 0, 0, 0, 1, 0, 0]);

        // libtorrent incoming_have: a HAVE for a piece the peer has is ignored.
        peers
            .peer_has(a, piece(u32::try_from(TOTAL_PIECES).unwrap(), 5))
            .unwrap();
        peers
            .peer_has(a, piece(u32::try_from(TOTAL_PIECES).unwrap(), 0))
            .unwrap();
        assert_eq!(availability(&peers), [1, 0, 0, 0, 0, 1, 0, 0]);
    }

    #[test]
    fn a_have_says_whether_it_told_us_anything() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        let all_but_last: Vec<usize> = (0..TOTAL_PIECES - 1).collect();
        peers
            .update_bitfield(a, bitfield_of(&all_but_last))
            .unwrap();

        let have = |index: usize| {
            peers.peer_has(
                a,
                piece(
                    u32::try_from(TOTAL_PIECES).unwrap(),
                    u32::try_from(index).unwrap(),
                ),
            )
        };
        assert_eq!(have(0), Some(Have::Known));
        assert_eq!(have(TOTAL_PIECES - 1), Some(Have::Completed));
        assert_eq!(have(TOTAL_PIECES - 1), Some(Have::Known));

        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");
        peers.update_bitfield(b, bitfield_of(&[])).unwrap();
        assert_eq!(
            peers.peer_has(b, piece(u32::try_from(TOTAL_PIECES).unwrap(), 3)),
            Some(Have::Gained)
        );
    }

    #[test]
    fn a_seed_counts_once_and_a_dont_have_breaks_it_into_pieces() {
        let peers = make_peer_states();
        let total = u32::try_from(TOTAL_PIECES).unwrap();
        let ranked = |peers: &PeerStates| {
            (0..total)
                .map(|i| peers.availability.ranked(piece(total, i)))
                .collect::<Vec<_>>()
        };
        let all = crate::chunk_tracker::Moved {
            pieces: Vec::new(),
            all: true,
        };
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");
        let (c, _rx_c) = add_live_peer(&peers, "127.0.0.3:1236");
        let (d, _rx_d) = add_live_peer(&peers, "127.0.0.4:1237");
        peers.update_bitfield(c, bitfield_of(&[2])).unwrap();
        let all_but_last: Vec<usize> = (0..TOTAL_PIECES - 1).collect();
        peers
            .update_bitfield(b, bitfield_of(&all_but_last))
            .unwrap();
        peers.availability.take_moved();

        // The first seed adds one to every piece's rank: the picker lays its
        // order out anew.
        peers.update_bitfield(a, full_bitfield()).unwrap();
        assert_eq!(peers.availability.take_moved(), all);
        assert_eq!(availability(&peers), [2, 2, 3, 2, 2, 2, 2, 1]);
        assert_eq!(ranked(&peers), [2, 2, 3, 2, 2, 2, 2, 1]);

        // A peer that gains its last piece leaves the counts for the seed.
        assert_eq!(
            peers.peer_has(b, piece(total, total - 1)),
            Some(Have::Completed)
        );
        assert_eq!(availability(&peers), [2, 2, 3, 2, 2, 2, 2, 2]);
        assert_eq!(ranked(&peers), [1, 1, 2, 1, 1, 1, 1, 1]);
        peers.availability.take_moved();

        // Another seed moves no piece.
        peers.update_bitfield(d, full_bitfield()).unwrap();
        assert_eq!(peers.availability.take_moved(), Default::default());
        assert_eq!(availability(&peers), [3, 3, 4, 3, 3, 3, 3, 3]);

        // A seed's DONT_HAVE counts the pieces it keeps one by one.
        peers.peer_lost(a, piece(total, 3)).unwrap();
        assert_eq!(availability(&peers), [3, 3, 4, 2, 3, 3, 3, 3]);
        assert_eq!(ranked(&peers), [2, 2, 3, 1, 2, 2, 2, 2]);
        peers.availability.take_moved();

        // Only the last seed to leave moves the order.
        peers.drop_peer(b).unwrap();
        assert_eq!(peers.availability.take_moved(), Default::default());
        peers.drop_peer(d).unwrap();
        assert_eq!(peers.availability.take_moved(), all);
        assert_eq!(availability(&peers), [1, 1, 2, 0, 1, 1, 1, 1]);
        assert_eq!(ranked(&peers), [1, 1, 2, 0, 1, 1, 1, 1]);
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
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");
        peers.update_bitfield(a, bitfield_of(&[0, 1])).unwrap();
        peers.update_bitfield(b, bitfield_of(&[1])).unwrap();
        peers
            .peer_has(a, piece(u32::try_from(TOTAL_PIECES).unwrap(), 7))
            .unwrap();

        // As the peer's task does when the connection ends: take the live
        // state, then settle the peer.
        peers
            .with_peer_mut(a, "test", |peer| {
                peer.take_state(&peers);
                peers.connection_closed(peer, Instant::now(), false, false);
            })
            .unwrap();
        assert_eq!(availability(&peers), [0, 1, 0, 0, 0, 0, 0, 0]);

        peers
            .with_peer_mut(a, "test", |peer| {
                peers.connection_closed(peer, Instant::now(), false, false)
            })
            .unwrap();
        assert_eq!(availability(&peers), [0, 1, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn a_peer_let_go_or_dropped_takes_its_bitfield_back() {
        let peers = make_peer_states();
        let (a, _rx_a) = add_live_peer(&peers, "127.0.0.1:1234");
        let (b, _rx_b) = add_live_peer(&peers, "127.0.0.2:1235");
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
        let (seeder, mut seeder_rx) = add_live_peer(&peers, "127.0.0.2:1235");
        let (leecher, mut leecher_rx) = add_live_peer(&peers, "127.0.0.3:1236");
        peers.update_bitfield(seeder, full_bitfield()).unwrap();
        peers.update_bitfield(leecher, empty_bitfield()).unwrap();

        peers.announce_have(piece(u32::try_from(TOTAL_PIECES).unwrap(), 3));

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
        SocketAddr::from((
            [
                10,
                0,
                u8::try_from((i >> 8) & 0xff).unwrap(),
                u8::try_from(i & 0xff).unwrap(),
            ],
            6881,
        ))
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
                peers.connection_closed(peer, at, true, false);
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

    // The connected addresses kept as peers are counted in and out are the
    // ones a walk of the whole list finds, after every step of a seeded run
    // of names, connects, incoming connections, closes, failures, peers let
    // go and dropped, and web seeds, some peers sharing a host; and so is
    // what connect_next picks, with peers at one host each allowed a
    // connection or not.
    #[test]
    fn the_connected_addresses_kept_are_those_a_walk_of_the_list_finds() {
        for allow in [false, true] {
            let allowed = Arc::new(AtomicBool::new(allow));
            let peers = make_peer_states().allowing_multiple_connections_per_ip(allowed.clone());
            // Four ports on each of 16 hosts.
            let addr = |i: u64| {
                SocketAddr::from((
                    [10, 1, 0, u8::try_from(i % 16).unwrap()],
                    6881 + (i / 16 % 4) as u16,
                ))
            };
            let web_seed = |i: u64| {
                SocketAddr::from((
                    std::net::Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 0, u16::try_from(i % 3).unwrap()),
                    0,
                ))
            };
            let mut rng = 0x9e37_79b9_7f4a_7c15u64;
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            let mut keep = Vec::new();
            let mut at = Instant::now();
            let mut pex_listed = 0;
            for step in 0..4000 {
                let r = next();
                let a = addr(r >> 8);
                match r % 9 {
                    0 | 1 => peers.add_peer(a, PeerSourceFlag::Tracker),
                    2 => {
                        if let Some((handle, rx, tx)) = peers.connect_next(at) {
                            keep.push((handle, rx, tx));
                        }
                    }
                    3 => {
                        if let Ok(rx) = connected_to_us(&peers, a) {
                            keep.push((a, rx, unbounded_channel().0));
                            // Half tell us the port they listen on.
                            if r & 2 == 0 {
                                peers.with_peer_mut(a, "test", |peer| {
                                    peer.listens_at(a, &peers);
                                });
                            }
                        }
                    }
                    4 | 5 => {
                        peers.with_peer_mut(a, "test", |peer| {
                            if peer.connected_out().is_some() {
                                peers.connection_closed(peer, at, r & 1 == 0, false);
                            }
                        });
                    }
                    6 => {
                        peers.with_peer_mut(a, "test", |peer| peer.set_not_needed(&peers));
                    }
                    7 => {
                        if r & 3 == 0 {
                            peers.drop_peer(a);
                        } else if let Some((rx, tx)) = peers.admit_web_seed(web_seed(r >> 8)) {
                            keep.push((web_seed(r >> 8), rx, tx));
                        }
                    }
                    _ => {
                        let seed = web_seed(r >> 8);
                        peers.with_peer_mut(seed, "test", |peer| {
                            if peer.connected_out().is_some() {
                                peers.connection_closed(peer, at, false, false);
                            }
                        });
                        at += MIN_RECONNECT_TIME;
                    }
                }
                assert_eq!(
                    peers.connected_ips_counted(),
                    peers.connected_ips_walked(),
                    "step {step}"
                );
                assert_eq!(peers.live_counted(), peers.live_walked(), "step {step}");
                let listed = peers.pex_listed();
                assert_eq!(*listed, peers.pex_listed_walked(), "step {step}");
                pex_listed = pex_listed.max(listed.len());
            }
            assert!(pex_listed > 0, "the run listed no peer for PEX");
            assert!(
                allow || !peers.connected_ips_counted().is_empty(),
                "the run left no connection to compare"
            );
            // Every connection closed, the count is empty again.
            for (handle, ..) in &keep {
                peers.with_peer_mut(*handle, "test", |peer| {
                    if peer.connected_out().is_some() {
                        peers.connection_closed(peer, at, false, false);
                    }
                });
            }
            assert!(peers.connected.lock().is_empty());
            assert!(peers.live.lock().is_empty());
        }
    }

    #[test]
    fn a_failed_peer_waits_failcount_plus_one_reconnect_times() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        let t0 = Instant::now();
        peers.add_peer(a, PeerSourceFlag::Tracker);
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
        peers.add_peer(a, PeerSourceFlag::Tracker);
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
        peers.add_peer(peer_addr(1), PeerSourceFlag::Tracker);
        assert!(futures::poll!(&mut due).is_ready());
    }

    #[test]
    fn a_peer_that_failed_max_failcount_times_is_dropped_as_a_candidate() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a, PeerSourceFlag::Tracker);
        let last = fail_out(&peers, a, Instant::now());

        assert_eq!(state_of(&peers, a), Some("dead"));
        assert_eq!(peers.next_candidate(last), NextCandidate::None);
        let much_later = last + Duration::from_secs(365 * 24 * 3600);
        assert!(peers.connect_next(much_later).is_none());
    }

    // Every source but the tracker's, each naming `addr` once.
    fn name_by_every_other_source(peers: &PeerStates, addr: SocketAddr) {
        for source in [
            PeerSourceFlag::Dht,
            PeerSourceFlag::Pex,
            PeerSourceFlag::Lsd,
            PeerSourceFlag::ResumeData,
        ] {
            peers.add_peer(addr, source);
        }
        peers.add_refused_incoming(addr, ConnectionKind::Utp);
    }

    // libtorrent `peer_list::update_peer`: "only trust this if it comes from
    // the tracker". A tracker's naming takes a failure off the peer, which
    // shortens its wait to reconnect; any other source's leaves both.
    #[test]
    fn only_a_trackers_naming_forgives_a_failed_peer_a_failure() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a, PeerSourceFlag::Dht);
        let t0 = Instant::now();
        fail_at(&peers, a, t0);
        let t1 = t0 + reconnect_wait(1);
        fail_at(&peers, a, t1);
        assert_eq!(
            peers.next_candidate(t1),
            NextCandidate::At(t1 + reconnect_wait(2))
        );

        name_by_every_other_source(&peers, a);
        assert_eq!(state_of(&peers, a), Some("queued"));
        assert_eq!(
            peers.next_candidate(t1),
            NextCandidate::At(t1 + reconnect_wait(2))
        );

        peers.add_peer(a, PeerSourceFlag::Tracker);
        assert_eq!(
            peers.next_candidate(t1),
            NextCandidate::At(t1 + reconnect_wait(1))
        );
    }

    // libtorrent `peer_list::update_peer` and `is_connect_candidate`: a peer
    // at `max_failcount` is no candidate until a tracker's naming takes it
    // under; the DHT, PEX or any other source naming it leaves it so.
    #[test]
    fn a_dropped_peer_is_revived_only_when_a_tracker_names_it_again() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a, PeerSourceFlag::Tracker);
        let last = fail_out(&peers, a, Instant::now());
        assert_eq!(state_of(&peers, a), Some("dead"));

        name_by_every_other_source(&peers, a);
        assert_eq!(state_of(&peers, a), Some("dead"));
        assert_eq!(peers.next_candidate(last), NextCandidate::None);

        peers.add_peer(a, PeerSourceFlag::Tracker);
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
        peers.add_peer(a, PeerSourceFlag::Tracker);
        fail_out(&peers, a, Instant::now());

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(a, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
            .unwrap();
        assert_eq!(state_of(&peers, a), Some("live"));
    }

    #[test]
    fn failures_are_forgotten_once_a_connection_delivers() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a, PeerSourceFlag::Tracker);
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
                peers.connection_closed(peer, t3, true, false)
            })
            .unwrap();
        assert_eq!(state_of(&peers, a), Some("queued"));
        assert_eq!(
            peers.next_candidate(t3),
            NextCandidate::At(t3 + reconnect_wait(1))
        );
    }

    // A seed we let go while we only upload is no failed peer: once we want
    // pieces again it is due at once. A seed whose connection failed then
    // waits as its failure has it wait.
    #[test]
    fn a_seed_we_let_go_is_due_at_once_when_wanted_again() {
        let peers = make_peer_states();
        let let_go = peer_addr(1);
        let failed = peer_addr(2);
        let t0 = Instant::now();
        for (addr, fails) in [(let_go, false), (failed, true)] {
            peers.add_peer(addr, PeerSourceFlag::Tracker);
            peers
                .with_peer_mut(addr, "test", |peer| {
                    peer.connect_if_due(t0, &peers).unwrap();
                    assert!(
                        peer.connecting_to_live(Id20::default(), &peers, ConnectionKind::Tcp)
                            .is_some()
                    );
                })
                .unwrap();
            peers.update_bitfield(addr, full_bitfield()).unwrap();
            peers
                .with_peer_mut(addr, "test", |peer| {
                    peers.connection_closed(peer, t0, fails, true);
                })
                .unwrap();
            assert_eq!(state_of(&peers, addr), Some("not needed"));
        }
        assert_eq!(peers.next_candidate(t0), NextCandidate::None);

        peers.requeue_not_needed();
        assert_eq!(peers.connect_next(t0).map(|(addr, ..)| addr), Some(let_go));
        assert!(peers.connect_next(t0).is_none());
        assert_eq!(
            peers.next_candidate(t0),
            NextCandidate::At(t0 + reconnect_wait(1))
        );
    }

    // A connection we close while we want the peer is no failure, and the
    // peer waits one reconnect time from the close.
    #[test]
    fn a_connection_we_close_is_not_a_failure() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a, PeerSourceFlag::Tracker);
        let t0 = Instant::now();
        peers
            .with_peer_mut(a, "test", |peer| {
                peer.connect_if_due(t0, &peers).unwrap();
                peers.connection_closed(peer, t0, false, false);
            })
            .unwrap();
        assert_eq!(state_of(&peers, a), Some("queued"));
        assert_eq!(
            peers.next_candidate(t0),
            NextCandidate::At(t0 + reconnect_wait(0))
        );
    }

    // Names `addr`, fails a connection to it at `at`, and lets it go: an
    // erase candidate that failed once.
    fn failed_once(peers: &PeerStates, addr: SocketAddr, at: Instant) {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
        fail_at(peers, addr, at);
        peers
            .with_peer_mut(addr, "test", |peer| peer.set_not_needed(peers))
            .unwrap();
    }

    // Names `addr` and lets it go before any connection: no connect
    // candidate, and no erase candidate either, as it never failed.
    fn never_failed(peers: &PeerStates, addr: SocketAddr) {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
        peers
            .with_peer_mut(addr, "test", |peer| peer.set_not_needed(peers))
            .unwrap();
    }

    fn connected_to_us(peers: &PeerStates, addr: SocketAddr) -> Result<PeerRx, IncomingRefused> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(addr, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
            .map(|_| rx)
    }

    // libtorrent `peer_list::insert_peer` and `new_connection` at a full
    // list: `erase_peers` forgets the erase candidate `compare_peer_erase`
    // ranks lowest, the most failures first, then one we cannot connect to,
    // then the least trusted; a named peer is refused when there is none,
    // and a peer that connected to us (`force_erase`) pushes out an idle one.
    #[tokio::test(start_paused = true)]
    async fn an_insert_at_full_displaces_by_the_erase_rank() {
        let peers = make_peer_states();
        let t0 = Instant::now();
        let (most_failed, unconnectable, distrusted, trusted, kept) = (
            peer_addr(0),
            peer_addr(1),
            peer_addr(2),
            peer_addr(3),
            peer_addr(4),
        );
        peers.add_peer(most_failed, PeerSourceFlag::Tracker);
        fail_out(&peers, most_failed, t0);
        // It connected over TCP, from no port it listens on, and failed.
        let _closed_rx = connected_to_us(&peers, unconnectable).unwrap();
        peers
            .with_peer_mut(unconnectable, "test", |peer| {
                peer.take_state(&peers);
                peers.connection_closed(peer, t0, true, false);
            })
            .unwrap();
        for addr in [distrusted, trusted] {
            failed_once(&peers, addr, t0);
        }
        assert!(
            !peers
                .with_peer_mut(distrusted, "test", |peer| peer.piece_failed(false))
                .unwrap()
        );
        never_failed(&peers, kept);
        let live = peer_addr(5);
        let _live_rx = connected_to_us(&peers, live).unwrap();
        for i in 6..MAX_PEERLIST_SIZE {
            peers.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
        }
        assert_eq!(peers.listed(), MAX_PEERLIST_SIZE);

        let newcomer = |i| peer_addr(MAX_PEERLIST_SIZE + i);
        for (i, erased) in [most_failed, unconnectable, distrusted, trusted]
            .into_iter()
            .enumerate()
        {
            assert!(state_of(&peers, erased).is_some());
            peers.add_peer(newcomer(i), PeerSourceFlag::Tracker);
            assert_eq!(state_of(&peers, erased), None);
            assert_eq!(state_of(&peers, newcomer(i)), Some("queued"));
        }

        // No erase candidate is left: a named peer pushes out neither a peer
        // that never failed nor a connect candidate.
        peers.add_peer(newcomer(4), PeerSourceFlag::Tracker);
        assert_eq!(state_of(&peers, newcomer(4)), None);
        assert_eq!(state_of(&peers, kept), Some("not needed"));
        assert_eq!(peers.listed(), MAX_PEERLIST_SIZE);

        // A peer that connected to us pushes out an idle one, and the
        // connected peer stays.
        let _rx = connected_to_us(&peers, newcomer(5)).unwrap();
        assert_eq!(state_of(&peers, newcomer(5)), Some("live"));
        assert_eq!(state_of(&peers, live), Some("live"));
        assert_eq!(peers.listed(), MAX_PEERLIST_SIZE);
    }

    // libtorrent `peer_list::is_erase_candidate`: a peer with no connection
    // that is no connect candidate goes only once it has failed.
    #[tokio::test(start_paused = true)]
    async fn weeding_forgets_a_failed_peer_and_keeps_one_that_never_failed() {
        let peers = make_peer_states();
        let t0 = Instant::now();
        let (failed, kept) = (peer_addr(0), peer_addr(1));
        failed_once(&peers, failed, t0);
        never_failed(&peers, kept);
        for i in 2..PEERLIST_LOW_WATERMARK + 5 {
            peers.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
        }

        assert!(peers.connect_next(t0).is_some());
        assert_eq!(state_of(&peers, failed), None);
        assert_eq!(state_of(&peers, kept), Some("not needed"));
        assert_eq!(peers.listed(), PEERLIST_LOW_WATERMARK + 4);
    }

    // libtorrent `peer_list::find_connect_candidates` weeds once the list
    // holds 95% of `max_peerlist_size`, and `erase_peers` stops under its
    // low watermark.
    #[tokio::test(start_paused = true)]
    async fn weeding_runs_past_95_percent_and_leaves_the_list_under_it() {
        let peers = make_peer_states();
        let t0 = Instant::now();
        let failed = (0..100).map(peer_addr).collect::<Vec<_>>();
        for &addr in &failed {
            failed_once(&peers, addr, t0);
        }
        let mut next = failed.len();
        let mut fill_to = |listed: usize| {
            while peers.listed() < listed {
                peers.add_peer(peer_addr(next), PeerSourceFlag::Tracker);
                next += 1;
            }
        };
        let failed_left = |peers: &PeerStates| {
            failed
                .iter()
                .filter(|&&addr| state_of(peers, addr).is_some())
                .count()
        };

        fill_to(PEERLIST_LOW_WATERMARK - 1);
        assert!(peers.connect_next(t0).is_some());
        assert_eq!(peers.listed(), PEERLIST_LOW_WATERMARK - 1);
        assert_eq!(failed_left(&peers), failed.len());

        fill_to(PEERLIST_LOW_WATERMARK + 50);
        assert!(peers.connect_next(t0).is_some());
        assert_eq!(peers.listed(), PEERLIST_LOW_WATERMARK - 1);
        assert_eq!(failed_left(&peers), failed.len() - 51);
    }

    // A xorshift stream from `seed`, for seeded runs.
    fn xorshift(seed: u64) -> impl FnMut() -> u64 {
        let mut rng = seed | 1;
        move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        }
    }

    // Whom weeding to `fewer_than` forgets by a full sort of every idle
    // peer `displace` allows on the list, by erase rank and address: the
    // rule the partial selection keeps.
    fn erased_by_full_sort(
        peers: &PeerStates,
        displace: Displace,
        fewer_than: usize,
    ) -> HashSet<SocketAddr> {
        let excess = (peers.listed() + 1).saturating_sub(fewer_than);
        let mut ranked = peers
            .states
            .iter()
            .filter(|e| !is_web_seed(*e.key()) && e.value().connected_out().is_none())
            .map(|e| (erase_rank(e.value()), *e.key()))
            .filter(|(rank, _)| displace.allows(rank))
            .collect::<Vec<_>>();
        ranked.sort();
        ranked
            .into_iter()
            .take(excess)
            .map(|(_, addr)| addr)
            .collect()
    }

    // A list of `size` peers from `next`: named ones, ones that failed one
    // to three times and were or were not let go, ones let go that never
    // failed, live ones, ones whose connection to us closed, failed or
    // not, and ones refused while the slots were full; some more trusted,
    // some less. Many share an erase rank, so the address settles who goes.
    // Returns the receiving ends of the live peers' channels.
    fn seeded_list(
        peers: &PeerStates,
        next: &mut impl FnMut() -> u64,
        size: usize,
        t0: Instant,
    ) -> Vec<PeerRx> {
        let spaced = reconnect_wait(MAX_FAILCOUNT);
        let mut keep = Vec::new();
        for i in 0..size {
            let r = next();
            let addr = peer_addr(i);
            match r % 6 {
                0 => peers.add_peer(addr, PeerSourceFlag::Tracker),
                1 => {
                    peers.add_peer(addr, PeerSourceFlag::Tracker);
                    let failures = 1 + u32::try_from((r >> 8) % u64::from(MAX_FAILCOUNT)).unwrap();
                    for k in 0..failures {
                        fail_at(peers, addr, t0 + spaced * (k + 1));
                    }
                    if r & 16 != 0 {
                        peers.with_peer_mut(addr, "test", |peer| peer.set_not_needed(peers));
                    }
                }
                2 => never_failed(peers, addr),
                3 => keep.push(connected_to_us(peers, addr).unwrap()),
                4 => {
                    let _rx = connected_to_us(peers, addr).unwrap();
                    peers.with_peer_mut(addr, "test", |peer| {
                        peers.connection_closed(peer, t0, r & 16 != 0, false);
                    });
                }
                _ => peers.add_refused_incoming(addr, ConnectionKind::Tcp),
            }
            peers.with_peer_mut(addr, "test", |peer| {
                if r & 32 != 0 {
                    peer.piece_passed();
                }
                if r & 64 != 0 {
                    let _banned = peer.piece_failed(false);
                }
            });
        }
        keep
    }

    // The partial selection picks the set a full sort puts first, over
    // seeded lists with many ties of rank, every count to take.
    #[test]
    fn the_lowest_are_the_first_a_full_sort_puts_first() {
        let mut next = xorshift(0x2545_f491_4f6c_dd1d);
        for _ in 0..500 {
            let len = usize::try_from(next() % 64).unwrap();
            // Few ranks, so most entries tie on rank and differ in address.
            let ranked: Vec<(u8, u16)> = (0..len)
                .map(|i| (u8::try_from(next() % 4).unwrap(), u16::try_from(i).unwrap()))
                .collect();
            let mut sorted = ranked.clone();
            sorted.sort();
            for n in 0..=len + 1 {
                let mut picked = lowest(ranked.clone(), n);
                picked.sort();
                assert_eq!(picked, sorted[..n.min(len)], "{ranked:?} {n}");
            }
        }
    }

    // Weeding forgets the peers a full sort of the list would, over seeded
    // lists of every kind of peer and a full list, for both kinds of room
    // made, and says the same of whether the list is under its mark.
    #[tokio::test(start_paused = true)]
    async fn weeding_forgets_the_peers_a_full_sort_would() {
        let t0 = Instant::now();
        let mut checked = 0;
        for seed in 0..40u64 {
            let mut next = xorshift(0x9e37_79b9_7f4a_7c15 ^ seed);
            let peers = make_peer_states();
            let size = if seed == 0 {
                MAX_PEERLIST_SIZE
            } else {
                64 + usize::try_from(next() % 400).unwrap()
            };
            let _keep = seeded_list(&peers, &mut next, size, t0);
            for round in 0..4 {
                let displace = if next() & 1 == 0 {
                    Displace::EraseCandidates
                } else {
                    Displace::AnyIdle
                };
                let listed = peers.listed();
                let fewer_than = if seed == 0 && round == 0 {
                    PEERLIST_LOW_WATERMARK
                } else {
                    1 + usize::try_from(next() % u64::try_from(listed + 1).unwrap()).unwrap()
                };
                let expected = erased_by_full_sort(&peers, displace, fewer_than);
                let before: HashSet<SocketAddr> = peers.states.iter().map(|e| *e.key()).collect();
                let under = peers.erase_to(displace, fewer_than);
                let after: HashSet<SocketAddr> = peers.states.iter().map(|e| *e.key()).collect();
                let erased: HashSet<SocketAddr> = before.difference(&after).copied().collect();
                assert_eq!(erased, expected, "seed {seed} round {round}");
                assert_eq!(under, listed - expected.len() < fewer_than);
                checked += expected.len();
            }
        }
        assert!(checked > 0, "no run forgot a peer");
    }

    // Each connection's PEX message from the one shared map is one the
    // map built afresh for it gives: over a seeded run of peers going live
    // (connecting to us, or we to them over TCP or uTP), telling us where
    // they listen, closing, leaving, their handshakes encrypted or not and
    // their pieces whole or not, the shared map equals a fresh walk at
    // every round and is shared by the round's connections; and every
    // connection, local or not, with its own `told`, gets the peers the
    // fresh map adds and drops for it, at most 50 of each, local peers
    // hidden from one that is not local. Where more than 50 are new, the
    // 50 sent are those the map's order gives first, as with a map built
    // afresh, so the test checks they are 50 of the right ones.
    #[tokio::test(start_paused = true)]
    async fn each_pex_message_from_the_shared_map_is_the_one_a_fresh_map_gives() {
        use crate::torrent_state::live::{MAX_SENT_PEX_PEERS, pex_diff};
        use holepunch::is_local;
        let host = |i: u64| -> SocketAddr {
            let i = u8::try_from(i % 120).unwrap();
            if i % 3 == 0 {
                SocketAddr::from(([10, 2, 0, i], 6881))
            } else {
                SocketAddr::from(([203, 0, 113, i], 6881))
            }
        };
        let to: Vec<SocketAddr> = vec![
            "192.168.1.1:6881".parse().unwrap(),
            "192.168.1.2:6881".parse().unwrap(),
            "10.9.0.1:6881".parse().unwrap(),
            "198.51.100.1:6881".parse().unwrap(),
            "198.51.100.2:6881".parse().unwrap(),
            "198.51.100.3:6881".parse().unwrap(),
        ];
        let (mut capped, mut uncapped, mut hidden, mut dropped_seen) = (0, 0, 0, 0);
        for seed in 0..8u64 {
            let mut next = xorshift(0xd1b5_4a32_d192_ed03 ^ seed);
            let peers = make_peer_states();
            let mut told: Vec<HashSet<SocketAddr>> = vec![HashSet::new(); to.len()];
            let mut keep_rx = Vec::new();
            let mut keep = Vec::new();
            let mut now = Instant::now();
            // A burst first, so the first round has more than 50 to add.
            for i in 0..90 {
                if let Ok(rx) = connected_to_us(&peers, host(i)) {
                    peers.with_peer_mut(host(i), "test", |peer| peer.listens_at(host(i), &peers));
                    keep_rx.push(rx);
                }
            }
            for step in 0..600 {
                let r = next();
                let a = host(r >> 8);
                match r % 8 {
                    0 => {
                        if let Ok(rx) = connected_to_us(&peers, a) {
                            keep_rx.push(rx);
                            if r & 16 != 0 {
                                peers.with_peer_mut(a, "test", |peer| peer.listens_at(a, &peers));
                            }
                        }
                    }
                    1 => {
                        peers.add_peer(a, PeerSourceFlag::Tracker);
                        if let Some((handle, rx, tx)) = peers.connect_next(now) {
                            let kind = if r & 16 != 0 {
                                ConnectionKind::Utp
                            } else {
                                ConnectionKind::Tcp
                            };
                            peers.with_peer_mut(handle, "test", |peer| {
                                peer.connecting_to_live(Id20::default(), &peers, kind);
                            });
                            keep.push((rx, tx));
                        }
                    }
                    2 => {
                        peers.with_peer_mut(a, "test", |peer| {
                            if peer.connected_out().is_some() {
                                peers.connection_closed(peer, now, r & 16 != 0, false);
                            }
                        });
                    }
                    3 => {
                        peers.drop_peer(a);
                    }
                    4 => peers.handshake_encryption(a, handshake(r & 16 != 0)),
                    5 => {
                        let bitfield = if r & 16 != 0 {
                            full_bitfield()
                        } else {
                            empty_bitfield()
                        };
                        peers.update_bitfield(a, bitfield);
                    }
                    _ => {
                        let mut shared: Option<PexListed> = None;
                        for (to, told) in to.iter().zip(&mut told) {
                            let listed = peers.pex_listed();
                            let fresh = peers.pex_listed_walked();
                            assert_eq!(*listed, fresh, "seed {seed} step {step}");
                            if let Some(shared) = &shared {
                                assert!(Arc::ptr_eq(shared, &listed), "seed {seed} step {step}");
                            }
                            shared = Some(listed.clone());

                            let before = told.clone();
                            let shown =
                                |addr: &SocketAddr| is_local(to.ip()) || !is_local(addr.ip());
                            let may_add: HashMap<SocketAddr, u8> = fresh
                                .iter()
                                .filter(|(addr, _)| shown(addr) && !before.contains(*addr))
                                .map(|(&addr, &flags)| (addr, flags))
                                .collect();
                            let may_drop: HashSet<SocketAddr> = before
                                .iter()
                                .filter(|addr| !fresh.contains_key(*addr))
                                .copied()
                                .collect();
                            hidden += fresh.keys().filter(|addr| !shown(addr)).count();
                            let (added, dropped) = pex_diff(&listed, *to, told).unwrap_or_default();
                            assert_eq!(added.len(), may_add.len().min(MAX_SENT_PEX_PEERS));
                            assert_eq!(dropped.len(), may_drop.len().min(MAX_SENT_PEX_PEERS));
                            let added_at: HashSet<SocketAddr> =
                                added.iter().map(|p| p.addr).collect();
                            assert_eq!(added_at.len(), added.len());
                            for p in &added {
                                assert_eq!(may_add.get(&p.addr), Some(&p.flags), "{seed} {step}");
                            }
                            for addr in &dropped {
                                assert!(may_drop.contains(addr), "seed {seed} step {step}");
                            }
                            let mut expected_told = before;
                            for addr in &dropped {
                                expected_told.remove(addr);
                            }
                            expected_told.extend(added_at);
                            assert_eq!(*told, expected_told, "seed {seed} step {step}");
                            if may_add.len() > MAX_SENT_PEX_PEERS {
                                capped += 1;
                            } else {
                                uncapped += 1;
                            }
                            dropped_seen += dropped.len();
                        }
                        now += MIN_RECONNECT_TIME;
                    }
                }
            }
        }
        assert!(capped > 0, "no round had more than 50 to add");
        assert!(uncapped > 0, "no round had 50 or fewer to add");
        assert!(hidden > 0, "no round hid a local peer");
        assert!(dropped_seen > 0, "no round dropped a peer");
    }

    // libtorrent `torrent::get_peer_list_state`: a paused torrent's list
    // holds `max_paused_peerlist_size`; those that go are the ones
    // `erase_peers` forgets first.
    #[tokio::test(start_paused = true)]
    async fn a_paused_list_keeps_the_peers_that_rank_highest() {
        let peers = make_peer_states();
        let t0 = Instant::now();
        let most_failed = (0..1950).map(peer_addr).collect::<Vec<_>>();
        let failed = (1950..2100).map(peer_addr).collect::<Vec<_>>();
        let queued = (2100..2999).map(peer_addr).collect::<Vec<_>>();
        let live = peer_addr(2999);
        for &addr in &most_failed {
            peers.add_peer(addr, PeerSourceFlag::Tracker);
            fail_out(&peers, addr, t0);
        }
        for &addr in &failed {
            failed_once(&peers, addr, t0);
        }
        for &addr in &queued {
            peers.add_peer(addr, PeerSourceFlag::Tracker);
        }
        let _live_rx = connected_to_us(&peers, live).unwrap();
        assert_eq!(peers.listed(), MAX_PEERLIST_SIZE);

        let list = peers.idle_list();
        assert_eq!(list.peers.len(), MAX_PAUSED_PEERLIST_SIZE);
        let kept = list.peers.iter().map(|p| p.addr).collect::<HashSet<_>>();
        assert!(queued.iter().all(|addr| kept.contains(addr)));
        assert!(kept.contains(&live));
        assert!(most_failed.iter().all(|addr| !kept.contains(addr)));
        assert_eq!(
            failed.iter().filter(|addr| kept.contains(addr)).count(),
            MAX_PAUSED_PEERLIST_SIZE - queued.len() - 1
        );

        let next = PeerStates::founded_on(
            list,
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        assert_eq!(next.listed(), MAX_PAUSED_PEERLIST_SIZE);
    }

    #[test]
    fn a_connected_peer_is_never_forgotten() {
        let peers = make_peer_states();
        let mut channels = Vec::new();
        for i in 0..MAX_PEERLIST_SIZE - 1 {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            peers
                .add_incoming(
                    peer_addr(i),
                    Id20::default(),
                    tx,
                    ConnectionKind::Tcp,
                    OUR_PORT,
                )
                .unwrap();
            channels.push(rx);
        }
        let candidate = peer_addr(MAX_PEERLIST_SIZE - 1);
        peers.add_peer(candidate, PeerSourceFlag::Tracker);

        // An incoming peer holding a slot pushes out the one idle peer.
        let first = peer_addr(MAX_PEERLIST_SIZE);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(first, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
            .unwrap();
        assert_eq!(state_of(&peers, candidate), None);
        assert_eq!(state_of(&peers, first), Some("live"));

        // Then the list holds connected peers only, and nobody gets in.
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let second = peer_addr(MAX_PEERLIST_SIZE + 1);
        assert_eq!(
            peers
                .add_incoming(second, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
                .unwrap_err(),
            IncomingRefused::ListFull
        );
        peers.add_peer(second, PeerSourceFlag::Tracker);
        assert_eq!(state_of(&peers, second), None);
        assert_eq!(peers.states.len(), MAX_PEERLIST_SIZE);
        assert_eq!(peers.stats().live as usize, MAX_PEERLIST_SIZE);
    }

    // libtorrent `torrent::peers_erased` tells `smart_ban_plugin::on_erase_peers`
    // when entries leave the peer list, not when a connection closes; and
    // `on_read_failed_block` records nothing of a peer the list does not
    // know.
    #[test]
    fn the_smart_ban_records_of_a_peer_leave_the_list_with_it_and_outlive_its_connection() {
        use crate::core::{
            constants::CHUNK_SIZE,
            lengths::{ChunkInfo, Lengths},
        };
        use crate::piece_tracker::Senders;
        use crate::torrent_state::live::smart_ban::read_back;

        let lengths = Lengths::new(u64::from(CHUNK_SIZE) * 4, CHUNK_SIZE * 4).unwrap();
        let piece = lengths.validate_piece_index(0).unwrap();
        let peers = make_peer_states();
        let t0 = Instant::now();
        let (closed, mut closed_rx) = add_live_peer(&peers, "10.1.0.1:6881");
        let (erased, dropped, stranger) = (peer_addr(1), peer_addr(2), peer_addr(3));
        for addr in [erased, dropped] {
            peers.add_peer(addr, PeerSourceFlag::Tracker);
        }
        let senders: Senders = [closed, erased, dropped, stranger]
            .map(Some)
            .into_iter()
            .collect();
        let zeros = |_, _: &ChunkInfo, buf: &mut [u8]| {
            buf.fill(0);
            Ok(())
        };
        let senders_recorded = |records: Vec<SentBlock>| {
            records
                .iter()
                .map(SentBlock::sender)
                .collect::<BTreeSet<_>>()
        };

        let failed = read_back(&lengths, piece, &senders, zeros);
        assert!(peers.piece_failed(piece, failed).is_empty());

        // The connection closes: the peer stays on the list.
        peers
            .with_peer_mut(closed, "test", |peer| {
                peer.take_state(&peers);
                peers.connection_closed(peer, t0, false, false);
            })
            .unwrap();
        while closed_rx.try_recv().is_ok() {}
        // One peer is dropped, another pushed out of the full list.
        peers.drop_peer(dropped).unwrap();
        fail_out(&peers, erased, t0);
        for i in 4.. {
            if peers.listed() == MAX_PEERLIST_SIZE {
                break;
            }
            peers.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
        }
        peers.add_peer(peer_addr(MAX_PEERLIST_SIZE + 4), PeerSourceFlag::Tracker);
        assert_eq!(state_of(&peers, erased), None);

        let failed = read_back(&lengths, piece, &senders, zeros);
        assert!(peers.piece_failed(piece, failed).is_empty());
        assert_eq!(
            senders_recorded(peers.piece_passed(piece)),
            BTreeSet::from([closed])
        );
    }

    // libtorrent's smart-ban plugin lives with the torrent: its
    // `m_block_hashes` cross `torrent::do_pause`, which only disconnects,
    // and the next failure of the piece names the sender that sent other
    // bytes before the pause.
    #[test]
    fn a_smart_ban_record_made_before_a_pause_names_its_sender_after_it() {
        use crate::core::{
            constants::CHUNK_SIZE,
            lengths::{ChunkInfo, Lengths},
        };
        use crate::piece_tracker::Senders;
        use crate::torrent_state::live::smart_ban::read_back;

        let lengths = Lengths::new(u64::from(CHUNK_SIZE) * 4, CHUNK_SIZE * 4).unwrap();
        let piece = lengths.validate_piece_index(0).unwrap();
        let peers = make_peer_states();
        let sender = peer_addr(1);
        peers.add_peer(sender, PeerSourceFlag::Tracker);
        let senders: Senders = [sender; 4].map(Some).into_iter().collect();
        let filled = |byte: u8| {
            move |_, _: &ChunkInfo, buf: &mut [u8]| {
                buf.fill(byte);
                Ok(())
            }
        };
        let failed = read_back(&lengths, piece, &senders, filled(0));
        assert!(peers.piece_failed(piece, failed).is_empty());

        let next = PeerStates::founded_on(
            peers.idle_list(),
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        let failed = read_back(&lengths, piece, &senders, filled(1));
        assert_eq!(next.piece_failed(piece, failed), BTreeSet::from([sender]));
    }

    // libtorrent `torrent::do_pause` disconnects every peer and keeps
    // `torrent::m_peer_list`: the next run's list knows the banned
    // addresses and each peer's trust and failures, and a peer we were
    // connecting to is a connect candidate again.
    #[test]
    fn the_next_run_is_founded_on_the_list_a_pause_leaves() {
        let peers = make_peer_states();
        let t0 = Instant::now();
        let banned = peer_addr(1);
        peers.ban(banned.ip());
        let failed = peer_addr(2);
        peers.add_peer(failed, PeerSourceFlag::Tracker);
        fail_at(&peers, failed, t0);
        peers
            .with_peer_mut(failed, "test", |p| p.piece_failed(false))
            .unwrap();
        let connecting = peer_addr(3);
        peers.add_peer(connecting, PeerSourceFlag::Tracker);
        assert_eq!(
            peers.connect_next(t0).map(|(addr, ..)| addr),
            Some(connecting)
        );

        let next = PeerStates::founded_on(
            peers.idle_list(),
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        assert!(next.is_banned(banned.ip()));
        next.add_peer(banned, PeerSourceFlag::Tracker);
        assert_eq!(state_of(&next, banned), None);
        let standing = |list: &PeerStates| {
            list.with_peer(failed, |p| (p.trust(), p.reconnect_at()))
                .unwrap()
        };
        assert_eq!(standing(&next), standing(&peers));
        assert!(next.with_peer(failed, |p| p.trust().on_parole()).unwrap());
        assert_eq!(state_of(&next, connecting), Some("queued"));
        assert_eq!(state_of(&peers, connecting), Some("connecting"));
        assert_eq!(next.stats().queued, 2);
        assert_eq!(next.stats().connecting, 0);
    }

    // A peer counts as connecting from the moment its dial begins until the
    // dial ends, in the run's counts and the session's alike: each dial
    // that fails leaves both, and a run that ends leaves none behind.
    #[test]
    fn a_dial_counts_as_connecting_until_it_ends() {
        let session: Arc<AggregatePeerStatsAtomic> = Default::default();
        let peers = PeerStates::founded_on(
            PeerList::default(),
            session.clone(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        let t0 = Instant::now();
        let dialled = [peer_addr(1), peer_addr(2)];
        for addr in dialled {
            peers.add_peer(addr, PeerSourceFlag::Tracker);
        }
        let connecting = || (peers.stats().connecting, session.snapshot().connecting);
        assert_eq!(connecting(), (0, 0));
        for _ in dialled {
            assert!(peers.connect_next(t0).is_some());
        }
        assert_eq!(connecting(), (2, 2));

        peers
            .with_peer_mut(dialled[0], "test", |peer| {
                peers.connection_closed(peer, t0, true, false);
            })
            .unwrap();
        assert_eq!(connecting(), (1, 1));

        drop(peers);
        assert_eq!(session.snapshot().connecting, 0);
    }

    // A pause ends the run, not the peer: a peer the pause disconnects
    // starts no wait to reconnect, so the next run dials it at once, and a
    // peer already idle keeps the wait it had.
    #[tokio::test(start_paused = true)]
    async fn a_pause_starts_no_reconnect_wait_for_the_peers_it_disconnects() {
        let peers = make_peer_states();
        let t0 = Instant::now();
        let idle = peer_addr(1);
        peers.add_peer(idle, PeerSourceFlag::Tracker);
        fail_at(&peers, idle, t0);
        let connecting = peer_addr(2);
        peers.add_peer(connecting, PeerSourceFlag::Tracker);
        assert_eq!(
            peers.connect_next(t0).map(|(addr, ..)| addr),
            Some(connecting)
        );
        let live = peer_addr(3);
        let _live_rx = connected_to_us(&peers, live).unwrap();
        let idle_wait = peers.with_peer(idle, |p| p.reconnect_at()).unwrap();

        let paused_at = t0 + Duration::from_secs(10);
        peers.close_all(paused_at);
        let next = PeerStates::founded_on(
            peers.idle_list(),
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        let reconnect_at = |addr| next.with_peer(addr, |p| p.reconnect_at()).unwrap();
        assert_eq!(reconnect_at(connecting), Some(paused_at));
        assert_eq!(reconnect_at(live), Some(paused_at));
        assert_eq!(reconnect_at(idle), idle_wait);
        assert_eq!(idle_wait, Some(t0 + MIN_RECONNECT_TIME * 2));
    }

    // A pause of a finished torrent lets every peer it disconnects go
    // without the wait to reconnect, seed and leecher alike. A run that
    // resumes finished does not connect to the seeds.
    #[tokio::test(start_paused = true)]
    async fn a_pause_of_a_finished_torrent_starts_no_wait_for_its_peers() {
        let peers = make_peer_states();
        let seed = peer_addr(1);
        let leecher = peer_addr(2);
        let t0 = Instant::now();
        for addr in [seed, leecher] {
            peers.add_peer(addr, PeerSourceFlag::Tracker);
            peers
                .with_peer_mut(addr, "test", |peer| {
                    peer.connect_if_due(t0, &peers).unwrap();
                    assert!(
                        peer.connecting_to_live(Id20::default(), &peers, ConnectionKind::Tcp)
                            .is_some()
                    );
                })
                .unwrap();
        }
        peers.update_bitfield(seed, full_bitfield()).unwrap();
        peers.update_bitfield(leecher, empty_bitfield()).unwrap();

        let paused_at = t0 + Duration::from_secs(10);
        peers.close_all(paused_at);
        let next = PeerStates::founded_on(
            peers.idle_list(),
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        let reconnect_at = |addr| next.with_peer(addr, |p| p.reconnect_at()).unwrap();
        assert_eq!(reconnect_at(seed), Some(paused_at));
        assert_eq!(reconnect_at(leecher), Some(paused_at));

        for mut peer in next.states.iter_mut() {
            peer.value_mut().finished(true, &next);
        }
        assert_eq!(state_of(&next, seed), Some("not needed"));
        assert_eq!(state_of(&next, leecher), Some("queued"));
    }

    // libtorrent `peer_list::set_seed`, as the pause closes a seed's
    // connection: the list the pause leaves knows the peer only uploads,
    // so a finished torrent does not connect to it again
    // (`is_connect_candidate`).
    #[tokio::test(start_paused = true)]
    async fn a_seed_live_at_the_pause_stays_upload_only() {
        let peers = make_peer_states();
        let (seed, _rx) = add_live_peer(&peers, "127.0.0.1:1234");
        peers.update_bitfield(seed, full_bitfield()).unwrap();
        assert_eq!(peers.with_peer(seed, |p| p.needed(true)), Some(true));

        peers.close_all(Instant::now());
        let next = PeerStates::founded_on(
            peers.idle_list(),
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        assert_eq!(next.with_peer(seed, |p| p.needed(true)), Some(false));
        assert_eq!(next.with_peer(seed, |p| p.needed(false)), Some(true));
    }

    // libtorrent `peer_list::is_connect_candidate`, `(p.seed ||
    // p.upload_only) && m_finished`: a complete torrent whose list holds only
    // seeds opens no connection, however a seed comes to be idle: brought
    // into a new run, named again, or let go and wanted again. Once we want
    // pieces again, the seeds are candidates again.
    #[tokio::test(start_paused = true)]
    async fn a_finished_torrent_with_only_seeds_on_its_list_opens_no_connection() {
        let peers = make_peer_states();
        let (one, two) = (peer_addr(1), peer_addr(2));
        let t0 = Instant::now();
        for seed in [one, two] {
            peers.add_peer(seed, PeerSourceFlag::Tracker);
            peers
                .with_peer_mut(seed, "test", |peer| {
                    peer.connect_if_due(t0, &peers).unwrap();
                    assert!(
                        peer.connecting_to_live(Id20::default(), &peers, ConnectionKind::Tcp)
                            .is_some()
                    );
                })
                .unwrap();
            peers.update_bitfield(seed, full_bitfield()).unwrap();
        }
        peers.close_all(t0);

        let next = PeerStates::founded_on(
            peers.idle_list(),
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        next.set_finished(true);
        let now = Instant::now();
        let quiet = |next: &PeerStates| {
            assert_eq!(state_of(next, one), Some("not needed"));
            assert_eq!(state_of(next, two), Some("not needed"));
            assert_eq!(next.next_candidate(now), NextCandidate::None);
            assert!(next.connect_next(now).is_none());
            assert_eq!(next.stats().connecting, 0);
        };
        quiet(&next);

        next.add_peer(one, PeerSourceFlag::Tracker);
        next.add_peer(two, PeerSourceFlag::Pex);
        quiet(&next);
        next.requeue_not_needed();
        quiet(&next);

        next.set_finished(false);
        assert_eq!(state_of(&next, one), Some("queued"));
        assert_eq!(state_of(&next, two), Some("queued"));
        assert!(next.connect_next(now).is_some());
    }

    fn at_port(addr: SocketAddr, port: u16) -> SocketAddr {
        SocketAddr::new(addr.ip(), port)
    }

    // libtorrent `peer_list::apply_ip_filter`: a refused peer with no
    // connection is erased and counted out; one with a connection is asked
    // to close and is erased once it has; a web seed is on no list.
    #[test]
    fn a_refused_peer_leaves_the_list_once_it_has_no_connection() {
        let peers = make_peer_states();
        let (idle, other) = (peer_addr(1), peer_addr(2));
        peers.add_peer(idle, PeerSourceFlag::Tracker);
        peers.add_peer(other, PeerSourceFlag::Tracker);
        let (live, mut live_rx) = add_live_peer(&peers, "10.0.0.3:50000");
        let seed = SocketAddr::new(
            std::net::Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 0, 1).into(),
            0,
        );
        peers.admit_web_seed(seed).unwrap();
        let refused = |addr: SocketAddr| addr == idle || addr == live || addr == seed;
        peers.refuse(refused);
        assert_eq!(state_of(&peers, idle), None);
        assert_eq!(state_of(&peers, other), Some("queued"));
        assert_eq!(state_of(&peers, live), Some("live"));
        assert!(matches!(
            live_rx.try_recv(),
            Ok(WriterRequest::Disconnect(Ok(())))
        ));
        assert_eq!(state_of(&peers, seed), Some("connecting"));

        // Its connection closed, the refused peer goes too.
        peers
            .with_peer_mut(live, "test", |peer| {
                peer.take_state(&peers);
                peers.connection_closed(peer, Instant::now(), false, false);
            })
            .unwrap();
        peers.refuse_at(live, refused);
        assert_eq!(state_of(&peers, live), None);
        peers.refuse_at(other, refused);
        assert_eq!(state_of(&peers, other), Some("queued"));
        assert_eq!(peers.stats().live, 0);
    }

    // A private torrent's list, once a tracker answers, keeps the peers a
    // tracker or the operator named, whoever else named them too; the
    // DHT's, local discovery's, PEX's and incoming ones leave, an idle one
    // at once, a connected one once its connection, asked to close, has
    // closed. It happens once.
    #[test]
    fn a_trackers_answer_drops_the_peers_no_tracker_or_operator_named() {
        let operator = peer_addr(1);
        let peers = PeerStates::founded_on(
            PeerList::resumed([operator], []).taking_trackers_alone(false),
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        let (tracker, dht, lsd, pex, both) = (
            peer_addr(2),
            peer_addr(3),
            peer_addr(4),
            peer_addr(5),
            peer_addr(6),
        );
        peers.add_peer(tracker, PeerSourceFlag::Tracker);
        peers.add_peer(dht, PeerSourceFlag::Dht);
        peers.add_peer(lsd, PeerSourceFlag::Lsd);
        peers.add_peer(pex, PeerSourceFlag::Pex);
        peers.add_peer(both, PeerSourceFlag::Dht);
        peers.add_peer(both, PeerSourceFlag::Tracker);
        let (incoming, mut incoming_rx) = add_live_peer(&peers, "10.0.1.1:50000");
        assert!(peers.owes_untracked());

        peers.drop_untracked();
        assert!(!peers.owes_untracked());
        for gone in [dht, lsd, pex] {
            assert_eq!(state_of(&peers, gone), None, "{gone}");
        }
        for kept in [operator, tracker, both] {
            assert_eq!(state_of(&peers, kept), Some("queued"), "{kept}");
        }
        assert_eq!(state_of(&peers, incoming), Some("live"));
        assert!(matches!(
            incoming_rx.try_recv(),
            Ok(WriterRequest::Disconnect(Ok(())))
        ));
        // Neither the next run's list nor a restart's keeps it meanwhile.
        assert!(!peers.idle_list().addrs().contains(&incoming));
        assert!(!peers.resume_peers().peers.contains(&incoming));

        // Its connection closed, the incoming peer goes too.
        peers
            .with_peer_mut(incoming, "test", |peer| {
                peer.take_state(&peers);
                peers.connection_closed(peer, Instant::now(), false, false);
            })
            .unwrap();
        peers.untracked_closed(incoming);
        assert_eq!(state_of(&peers, incoming), None);

        // Once only: a peer the DHT names later is not dropped by a second
        // answer.
        peers.add_peer(dht, PeerSourceFlag::Dht);
        peers.drop_untracked();
        assert_eq!(state_of(&peers, dht), Some("queued"));
        assert!(!peers.idle_list().owes_untracked());
    }

    // A connected peer asked to close because no tracker named it stays
    // when a tracker names it before the connection has closed.
    #[test]
    fn a_peer_a_tracker_names_while_it_closes_stays() {
        let peers = PeerStates::founded_on(
            PeerList::default().taking_trackers_alone(false),
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        let (incoming, _incoming_rx) = add_live_peer(&peers, "10.0.1.1:50000");
        peers.drop_untracked();
        peers.add_peer(incoming, PeerSourceFlag::Tracker);
        peers
            .with_peer_mut(incoming, "test", |peer| {
                peer.take_state(&peers);
                peers.connection_closed(peer, Instant::now(), false, false);
            })
            .unwrap();
        peers.untracked_closed(incoming);
        assert!(state_of(&peers, incoming).is_some());
    }

    // The list a private magnet's fetch leaves: when a tracker has answered
    // already, the peers neither a tracker nor the operator named leave it
    // now; when none has, all stay, owed to leave at the first answer. The
    // owed drop crosses a pause.
    #[test]
    fn a_fetched_list_takes_trackers_alone_now_or_at_the_first_answer() {
        let fetched = || {
            let peers = PeerStates::founded_on(
                PeerList::resumed([peer_addr(1)], []),
                Default::default(),
                0,
            );
            peers.add_peer(peer_addr(2), PeerSourceFlag::Tracker);
            peers.add_peer(peer_addr(3), PeerSourceFlag::Dht);
            peers.add_peer(peer_addr(4), PeerSourceFlag::Lsd);
            peers.idle_list()
        };
        let mut now = fetched().taking_trackers_alone(true).addrs();
        now.sort();
        assert_eq!(now, [peer_addr(1), peer_addr(2)]);
        assert!(!fetched().taking_trackers_alone(true).owes_untracked());

        let later = fetched().taking_trackers_alone(false);
        assert_eq!(later.addrs().len(), 4);
        let paused = PeerStates::founded_on(later, Default::default(), 0).idle_list();
        assert!(paused.owes_untracked());
        let next = PeerStates::founded_on(paused, Default::default(), 0);
        next.drop_untracked();
        assert_eq!(state_of(&next, peer_addr(3)), None);
        assert_eq!(state_of(&next, peer_addr(2)), Some("queued"));
    }

    #[test]
    fn a_banned_address_is_disconnected_and_never_admitted_again_on_any_port() {
        let peers = make_peer_states();
        let t0 = Instant::now();
        let banned = peer_addr(1);
        let (live, mut live_rx) = add_live_peer(&peers, &at_port(banned, 50000).to_string());
        peers.add_peer(banned, PeerSourceFlag::Tracker);
        let other = peer_addr(2);
        peers.add_peer(other, PeerSourceFlag::Tracker);

        peers.ban(live.ip());

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
                peers.connection_closed(peer, t0, true, false);
            })
            .unwrap();
        assert_eq!(state_of(&peers, live), Some("dead"));

        // Named again or connecting from any port, the address is turned away.
        peers.add_peer(banned, PeerSourceFlag::Tracker);
        peers.add_peer(at_port(banned, 6882), PeerSourceFlag::Tracker);
        peers.add_refused_incoming(at_port(banned, 6883), ConnectionKind::Utp);
        assert_eq!(state_of(&peers, banned), Some("dead"));
        assert_eq!(state_of(&peers, at_port(banned, 6882)), None);
        assert_eq!(state_of(&peers, at_port(banned, 6883)), None);
        for addr in [live, at_port(banned, 50001)] {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            assert_eq!(
                peers
                    .add_incoming(addr, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
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

        // A known peer that connects is named again, as incoming, which
        // forgives no failure: a dropped one stays dropped.
        let known = peer_addr(2);
        peers.add_peer(known, PeerSourceFlag::Tracker);
        fail_out(&peers, known, t0);
        assert_eq!(state_of(&peers, known), Some("dead"));
        peers.add_refused_incoming(known, ConnectionKind::Tcp);
        assert_eq!(state_of(&peers, known), Some("dead"));

        // An unknown TCP peer connects from an ephemeral port: there is no
        // address to connect back to.
        let tcp = peer_addr(3);
        peers.add_refused_incoming(tcp, ConnectionKind::Tcp);
        assert_eq!(state_of(&peers, tcp), None);
    }

    // libtorrent `bt_peer_connection::on_holepunch`, `connect`: the peer is
    // added or found and connected to at once (`torrent::connect_to_peer`),
    // over uTP, whatever its failures, unless it is connected or banned;
    // holepunch mode ends with the connection.
    #[test]
    fn a_holepunched_peer_is_connected_to_next_whatever_its_failures() {
        use crate::stream_connect::Transports;
        let peers = make_peer_states();
        let t0 = Instant::now();
        let fresh = peer_addr(1);
        let failed = peer_addr(2);
        peers.add_peer(fresh, PeerSourceFlag::Tracker);
        peers.add_peer(failed, PeerSourceFlag::Tracker);
        let last = fail_out(&peers, failed, t0);
        assert_eq!(state_of(&peers, failed), Some("dead"));

        peers.holepunch(failed);
        assert_eq!(state_of(&peers, failed), Some("queued"));
        assert_eq!(peers.next_candidate(last), NextCandidate::Due);
        assert_eq!(
            peers.connect_next(last).map(|(addr, ..)| addr),
            Some(failed)
        );
        assert_eq!(state_of(&peers, failed), Some("connecting"));

        // The holepunched connection goes over uTP alone, and fails too:
        // the peer is out again, and may be dialed over TCP once more.
        peers
            .with_peer_mut(failed, "test", |peer| {
                assert!(peer.holepunched());
                assert_eq!(peer.transports(), Transports::Utp);
                peers.connection_closed(peer, last, true, false);
                assert!(!peer.holepunched());
                assert_eq!(peer.transports(), Transports::Any);
            })
            .unwrap();
        assert_eq!(state_of(&peers, failed), Some("dead"));

        // A peer we do not know joins, due now.
        let unknown = peer_addr(3);
        peers.holepunch(unknown);
        assert_eq!(state_of(&peers, unknown), Some("queued"));
        assert_eq!(
            peers.connect_next(last).map(|(addr, ..)| addr),
            Some(unknown)
        );

        // A connected peer, or a banned one, is left as it is.
        let (live, _rx) = add_live_peer(&peers, "10.0.1.1:6881");
        peers.holepunch(live);
        assert_eq!(state_of(&peers, live), Some("live"));
        let banned = peer_addr(4);
        peers.ban(banned.ip());
        peers.holepunch(banned);
        assert_eq!(state_of(&peers, banned), None);
    }

    // libtorrent `ut_pex_plugin` keeps what a connection's PEX named, the
    // added less the dropped, at most `max_pex_peers` of each family;
    // `torrent::find_introducer` asks a connected holepunch peer whose PEX
    // named the address.
    #[test]
    fn the_introducer_is_a_holepunch_peer_whose_pex_named_the_address() {
        let named_by =
            |addrs: Vec<SocketAddr>| addrs.into_iter().map(|addr| PexPeerInfo { flags: 0, addr });
        let peers = make_peer_states();
        let target = peer_addr(1);
        let (named, _named_rx) = add_live_peer(&peers, "10.0.1.1:6881");
        let (speaks, _speaks_rx) = add_live_peer(&peers, "10.0.1.2:6881");
        peers.with_live_mut(named, "test", |live| {
            live.pex_named(named_by(vec![target]), std::iter::empty())
        });
        assert!(peers.introducer(target).is_none());

        peers.with_live_mut(speaks, "test", |live| {
            live.supports_holepunch = true;
            live.pex_named(named_by(vec![target]), std::iter::empty());
        });
        assert!(peers.introducer(target).is_some());
        peers.with_live_mut(speaks, "test", |live| {
            live.pex_named(std::iter::empty(), [target].into_iter())
        });
        assert!(peers.introducer(target).is_none());

        let v4 = (0..300).map(peer_addr);
        let v6 = (0..300).map(|i| SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, i], 6881)));
        peers.with_live_mut(speaks, "test", |live| {
            // Each family takes peers while it has room; the news is what it took.
            let news = live.pex_named(named_by(v4.chain(v6).collect()), std::iter::empty());
            assert_eq!(news.len(), 2 * MAX_PEX_PEERS);
            let again = live.pex_named(named_by(vec![peer_addr(0)]), std::iter::empty());
            assert!(again.is_empty());
            let kept = |v4| {
                (0..300)
                    .filter(|&i| {
                        let addr = if v4 {
                            peer_addr(i)
                        } else {
                            SocketAddr::from((
                                [
                                    0x2001,
                                    0xdb8,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    u16::try_from(i & 0xffff).unwrap(),
                                ],
                                6881,
                            ))
                        };
                        live.was_introduced(addr)
                    })
                    .count()
            };
            assert_eq!((kept(true), kept(false)), (MAX_PEX_PEERS, MAX_PEX_PEERS));
        });
    }

    fn incoming(
        peers: &PeerStates,
        addr: SocketAddr,
        our_port: u16,
    ) -> Result<PeerRx, IncomingRefused> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        peers
            .add_incoming(addr, Id20::default(), tx, ConnectionKind::Tcp, our_port)
            .map(|_| rx)
    }

    fn told_to_close(rx: &mut PeerRx) -> bool {
        matches!(rx.try_recv(), Ok(WriterRequest::Disconnect(Ok(()))))
    }

    // libtorrent `peer_list::new_connection`: a peer we are connecting to
    // connects to us too. Both ends keep the connection made by the end
    // with the lower listen port; the other is closed.
    #[test]
    fn a_connection_from_a_peer_we_dial_is_settled_by_listen_port() {
        let peers = make_peer_states();
        let dialed = peer_addr(1);
        peers.add_peer(dialed, PeerSourceFlag::Tracker);
        let (_, mut dialing, _) = peers.connect_next(Instant::now()).unwrap();

        // Our port is the lower: our connection stays, theirs is refused.
        let from = at_port(dialed, 50000);
        assert_eq!(
            incoming(&peers, from, 6000).unwrap_err(),
            IncomingRefused::AlreadyActive
        );
        assert!(dialing.try_recv().is_err());

        // Theirs is the lower: their connection stays, ours is closed.
        assert!(incoming(&peers, from, 7000).is_ok());
        assert!(told_to_close(&mut dialing));
    }

    // libtorrent `peer_list::new_connection`: when a peer connected to us
    // already, a second connection from its address is dropped.
    #[test]
    fn a_second_connection_from_an_address_that_reached_us_is_refused() {
        let peers = make_peer_states();
        let mut first = incoming(&peers, at_port(peer_addr(1), 50000), OUR_PORT).unwrap();
        assert_eq!(
            incoming(&peers, at_port(peer_addr(1), 50001), OUR_PORT).unwrap_err(),
            IncomingRefused::AlreadyActive
        );
        assert!(first.try_recv().is_err());
        assert!(incoming(&peers, at_port(peer_addr(2), 50001), OUR_PORT).is_ok());
    }

    // libtorrent's peer list holds one entry per address, and one with a
    // connection is no connect candidate: a peer at an address a connection
    // is at waits for it to close.
    #[test]
    fn a_peer_at_a_connected_address_is_dialed_once_the_connection_closes() {
        let peers = make_peer_states();
        let t0 = Instant::now();
        let listening = peer_addr(1);
        peers.add_peer(listening, PeerSourceFlag::Tracker);
        peers.add_peer(peer_addr(2), PeerSourceFlag::Tracker);
        let from = at_port(listening, 50000);
        let _rx = incoming(&peers, from, OUR_PORT).unwrap();
        assert_eq!(peers.connect_candidates(), 1);
        assert_eq!(
            peers.connect_next(t0).map(|(addr, ..)| addr),
            Some(peer_addr(2))
        );
        assert!(peers.connect_next(t0).is_none());
        assert_eq!(peers.next_candidate(t0), NextCandidate::None);

        peers
            .with_peer_mut(from, "test", |peer| {
                peer.take_state(&peers);
                peers.connection_closed(peer, t0, false, false);
            })
            .unwrap();
        assert_eq!(peers.connect_candidates(), 1);
        assert_eq!(
            peers.connect_next(t0).map(|(addr, ..)| addr),
            Some(listening)
        );
    }

    // libtorrent reads `allow_multiple_connections_per_ip` into each
    // `torrent::get_peer_list_state`, so turning it on lets a peer the gate
    // held back be dialed at once, the connection at its address still
    // open; the connect loop is woken to look again.
    #[test]
    fn turning_multiple_connections_per_ip_on_dials_the_held_peer_at_once() {
        use futures::FutureExt;

        let allow = Arc::new(AtomicBool::new(false));
        let peers = make_peer_states().allowing_multiple_connections_per_ip(Arc::clone(&allow));
        let t0 = Instant::now();
        let a = peer_addr(1);
        let held = at_port(a, 6882);
        peers.add_peer(a, PeerSourceFlag::Tracker);
        peers.add_peer(held, PeerSourceFlag::Tracker);
        let (first, ..) = peers.connect_next(t0).unwrap();
        assert!(first == a || first == held);
        assert!(peers.connect_next(t0).is_none());
        // Drain the wake the dial left.
        let _ = peers.candidates_changed().now_or_never();

        allow.store(true, Ordering::Relaxed);
        peers.connections_per_ip_changed();
        assert!(peers.candidates_changed().now_or_never().is_some());
        let (second, ..) = peers.connect_next(t0).unwrap();
        assert_ne!(second, first);
        assert_eq!(second.ip(), a.ip());
    }

    // libtorrent `allow_multiple_connections_per_ip`: when it is on, peers
    // at one address each have a connection.
    #[test]
    fn a_list_allowing_multiple_connections_per_ip_dials_each_peer() {
        let peers = make_peer_states()
            .allowing_multiple_connections_per_ip(Arc::new(AtomicBool::new(true)));
        let t0 = Instant::now();
        let a = peer_addr(1);
        peers.add_peer(a, PeerSourceFlag::Tracker);
        peers.add_peer(at_port(a, 6882), PeerSourceFlag::Tracker);
        let _rx = incoming(&peers, at_port(a, 50000), OUR_PORT).unwrap();
        assert!(incoming(&peers, at_port(a, 50001), OUR_PORT).is_ok());
        assert!(peers.connect_next(t0).is_some());
        assert!(peers.connect_next(t0).is_some());
    }

    // libtorrent `torrent::disconnect_peers` with `compare_disconnect_peer`:
    // a peer we are not interested in goes first, then one that is no seed,
    // then one on parole, then the one that sent least per second connected.
    #[test]
    fn turnover_closes_the_connections_worth_least_first() {
        let peers = make_peer_states();
        let now = Instant::now();
        let mut rxs = Vec::new();
        let mut addrs = Vec::new();
        for i in 1..=5 {
            let (addr, rx) = add_live_peer(&peers, &format!("10.0.0.{i}:6881"));
            addrs.push(addr);
            rxs.push(rx);
        }
        let [seed, uninteresting, fast, slow, on_parole] = addrs[..] else {
            unreachable!()
        };
        for addr in [seed, fast, slow, on_parole] {
            peers.with_live_mut(addr, "test", |live| live.interest(true, now));
        }
        peers.update_bitfield(seed, full_bitfield()).unwrap();
        for (addr, fetched) in [(fast, 1000), (slow, 10), (on_parole, 1000)] {
            peers.with_peer(addr, |peer| {
                peer.stats
                    .counters
                    .fetched_bytes
                    .fetch_add(fetched, Ordering::Relaxed)
            });
        }
        peers.with_peer_mut(on_parole, "test", |peer| peer.piece_failed(false));

        let order = [uninteresting, on_parole, slow, fast, seed];
        for n in 1..=order.len() {
            peers.disconnect_lowest(n, now);
            for (addr, rx) in addrs.iter().zip(rxs.iter_mut()) {
                assert_eq!(
                    told_to_close(rx),
                    order[..n].contains(addr),
                    "{addr} of {n}"
                );
            }
        }
    }

    /// How an outgoing connection in [`attempt_at`] ends.
    #[derive(Clone, Copy)]
    enum Ends {
        /// Its handshake completes, and we close it later.
        Completed,
        /// Its handshake does not complete: the peer refuses it.
        Failed,
        /// We close it before its handshake completes.
        Closed,
    }

    // The handshake a connection completed: RC4-encrypted when `encrypted`.
    fn handshake(encrypted: bool) -> Encryption {
        if encrypted {
            Encryption::Rc4
        } else {
            Encryption::None
        }
    }

    // Connects to `addr`, due at `at`, where encryption is preferred: asks
    // which handshake the connection tries, which ends as `ends` says, at
    // `at`. Returns whether it tried the encrypted handshake.
    fn attempt_at(peers: &PeerStates, addr: SocketAddr, at: Instant, ends: Ends) -> bool {
        peers
            .with_peer_mut(addr, "test", |peer| {
                peer.connect_if_due(at, peers).expect("a due candidate");
                let encrypted = peer.encrypt_outgoing();
                if let Ends::Completed = ends {
                    peer.connecting_to_live(Id20::default(), peers, ConnectionKind::Tcp);
                    peer.handshake_encryption(handshake(encrypted), peers);
                }
                peers.connection_closed(peer, at, matches!(ends, Ends::Failed), false);
                encrypted
            })
            .unwrap()
    }

    // When the peer is due again after a connection closed at `at`.
    fn due_after(peers: &PeerStates, at: Instant) -> Instant {
        match peers.next_candidate(at) {
            NextCandidate::Due => at,
            NextCandidate::At(due) => due,
            NextCandidate::None => panic!("no candidate"),
        }
    }

    // libtorrent `bt_peer_connection::on_connected` and
    // `peer_connection::fast_reconnect`: an encrypted handshake that fails
    // leaves `pe_support` pointing at plaintext, and the next connection
    // tries it at once, not held by the wait to reconnect. A plaintext one
    // that fails waits as any failure does, and points back at encryption.
    #[tokio::test(start_paused = true)]
    async fn a_failed_encrypted_handshake_is_followed_at_once_by_a_plaintext_one() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a, PeerSourceFlag::Tracker);
        let t0 = Instant::now();

        assert!(attempt_at(&peers, a, t0, Ends::Failed));
        assert_eq!(peers.next_candidate(t0), NextCandidate::Due);
        assert!(!attempt_at(&peers, a, t0, Ends::Failed));
        assert_eq!(
            peers.next_candidate(t0),
            NextCandidate::At(t0 + reconnect_wait(2))
        );
        assert!(attempt_at(
            &peers,
            a,
            t0 + reconnect_wait(2),
            Ends::Completed
        ));
    }

    // libtorrent `bt_peer_connection::on_receive_impl` and
    // `init_bt_handshake` set `pe_support` to the method that completed:
    // the next connection tries it again.
    #[tokio::test(start_paused = true)]
    async fn a_peer_is_tried_again_with_the_handshake_it_last_completed() {
        let peers = make_peer_states();
        let (plain, encrypted) = (peer_addr(1), peer_addr(2));
        let t0 = Instant::now();

        peers.add_peer(plain, PeerSourceFlag::Tracker);
        assert!(attempt_at(&peers, plain, t0, Ends::Failed));
        assert!(!attempt_at(&peers, plain, t0, Ends::Completed));
        let t1 = due_after(&peers, t0);
        assert!(t1 > t0);
        assert!(!attempt_at(&peers, plain, t1, Ends::Completed));

        peers.add_peer(encrypted, PeerSourceFlag::Tracker);
        assert!(attempt_at(&peers, encrypted, t1, Ends::Completed));
        assert!(attempt_at(
            &peers,
            encrypted,
            t1 + MIN_RECONNECT_TIME,
            Ends::Completed
        ));
    }

    // libtorrent `peer_connection::fast_reconnect` takes effect while
    // `torrent_peer::fast_reconnects` is under two: a third encrypted
    // handshake that does not complete waits like any other close.
    #[tokio::test(start_paused = true)]
    async fn only_two_connections_follow_an_encrypted_handshake_at_once() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a, PeerSourceFlag::Tracker);
        let mut at = Instant::now();
        for at_once in [true, true, false] {
            assert!(attempt_at(&peers, a, at, Ends::Closed));
            let due = due_after(&peers, at);
            assert_eq!(due == at, at_once, "{due:?} after {at:?}");
            at = due;
            // The plaintext one in between, closed too, waits.
            assert!(!attempt_at(&peers, a, at, Ends::Closed));
            assert_eq!(due_after(&peers, at), at + MIN_RECONNECT_TIME);
            at += MIN_RECONNECT_TIME;
        }
    }

    // libtorrent `bt_peer_connection::on_connected` reads and flips
    // `pe_support` under `pe_enabled` alone: where encryption is required
    // nothing asks, so a failed connection neither toggles the peer's
    // method nor is followed at once.
    #[tokio::test(start_paused = true)]
    async fn a_connection_that_asks_nothing_toggles_nothing() {
        let peers = make_peer_states();
        let a = peer_addr(1);
        peers.add_peer(a, PeerSourceFlag::Tracker);
        let t0 = Instant::now();
        fail_at(&peers, a, t0);
        fail_at(&peers, a, t0 + reconnect_wait(1));
        assert_eq!(
            peers.next_candidate(t0 + reconnect_wait(1)),
            NextCandidate::At(t0 + reconnect_wait(1) + reconnect_wait(2))
        );
        let tried = peers.with_peer_mut(a, "test", |peer| peer.encrypt_outgoing());
        assert_eq!(tried, Some(true));
    }

    // libtorrent `peer_list::insert_peer` sets `pe_support` on a PEX
    // `pex_encryption` flag, and never clears it on a naming without one:
    // a peer remembered as plaintext is tried encrypted again.
    #[tokio::test(start_paused = true)]
    async fn a_pex_naming_with_the_encryption_flag_points_a_peer_back_at_encryption() {
        use crate::peer_protocol::extended::ut_pex::{PEX_ENCRYPTION, PEX_SEED};
        let peers = make_peer_states();
        let (named, unflagged) = (peer_addr(1), peer_addr(2));
        let t0 = Instant::now();
        for addr in [named, unflagged] {
            peers.add_peer(addr, PeerSourceFlag::Tracker);
            assert!(attempt_at(&peers, addr, t0, Ends::Failed));
            assert!(!attempt_at(&peers, addr, t0, Ends::Completed));
        }

        peers.pex_flags(named, PEX_ENCRYPTION);
        peers.pex_flags(unflagged, PEX_SEED);
        let t1 = due_after(&peers, t0);
        assert!(attempt_at(&peers, named, t1, Ends::Completed));
        assert!(!attempt_at(&peers, unflagged, t1, Ends::Completed));
    }

    // libtorrent `ut_pex_plugin::tick` flags `pex_encryption` for a peer
    // whose connection is encrypted (`bt_peer_connection::supports_encryption`).
    #[tokio::test(start_paused = true)]
    async fn our_pex_flags_a_peer_whose_connection_is_encrypted() {
        use crate::peer_protocol::extended::ut_pex::PEX_ENCRYPTION;
        let peers = make_peer_states();
        let (encrypted, plain) = (peer_addr(1), peer_addr(2));
        let now = Instant::now();
        for (addr, encrypt) in [(encrypted, true), (plain, false)] {
            peers.add_peer(addr, PeerSourceFlag::Tracker);
            peers.with_peer_mut(addr, "test", |peer| {
                peer.connect_if_due(now, &peers).expect("a due candidate");
                peer.connecting_to_live(Id20::default(), &peers, ConnectionKind::Tcp);
                peer.handshake_encryption(handshake(encrypt), &peers);
            });
        }

        let listed = peers.pex_listed();
        assert_eq!(listed[&encrypted] & PEX_ENCRYPTION, PEX_ENCRYPTION);
        assert_eq!(listed[&plain] & PEX_ENCRYPTION, 0);
    }

    // libtorrent `bt_peer_connection::init_bt_handshake` marks a connection
    // encrypted whichever side opened it; both writes of `pe_support`
    // (`init_bt_handshake`, `on_receive_impl`) are `is_outgoing()`, so a
    // handshake the peer opened leaves which one ours try alone.
    #[tokio::test(start_paused = true)]
    async fn a_handshake_the_peer_opened_leaves_which_one_ours_try_alone() {
        use crate::peer_protocol::extended::ut_pex::PEX_ENCRYPTION;
        let peers = make_peer_states();
        let mut ends = Vec::new();
        for (addr, encrypted, flag) in [
            ("10.0.0.1:6881", true, PEX_ENCRYPTION),
            ("10.0.0.2:6881", false, 0),
        ] {
            let (addr, rx) = add_live_peer(&peers, addr);
            ends.push(rx);
            peers.handshake_encryption(addr, handshake(encrypted));
            let advertised = peers.with_live(addr, |live| live.advertised_pex_flags());
            assert_eq!(advertised.unwrap() & PEX_ENCRYPTION, flag);

            peers.with_peer_mut(addr, "test", |peer| {
                peers.connection_closed(peer, Instant::now(), false, false)
            });
            assert!(peers.encrypt_outgoing(addr), "{addr}");
        }
    }

    // libtorrent `torrent::write_resume_data`: the saved list holds the
    // peers we can connect to that did not fail their last connection and
    // sent no bad data, those we were connected to first; a peer at a
    // banned address is saved as its ban.
    #[test]
    fn the_saved_peers_are_the_connectable_trusted_ones_those_connected_to_first() {
        let peers = make_peer_states();
        let t0 = Instant::now();
        let connecting = peer_addr(1);
        peers.add_peer(connecting, PeerSourceFlag::Tracker);
        let (_, _connecting_rx, _connecting_tx) = peers.connect_next(t0).unwrap();
        let never = peer_addr(2);
        peers.add_peer(never, PeerSourceFlag::Tracker);
        let failed = peer_addr(3);
        peers.add_peer(failed, PeerSourceFlag::Tracker);
        fail_at(&peers, failed, t0);
        let distrusted = peer_addr(4);
        peers.add_peer(distrusted, PeerSourceFlag::Tracker);
        peers
            .with_peer_mut(distrusted, "test", |p| p.piece_failed(false))
            .unwrap();
        // Connected to us: we know no address to connect to it at.
        let (_, _incoming_rx) = add_live_peer(&peers, "10.0.0.5:50000");
        let banned = peer_addr(6);
        peers.add_peer(banned, PeerSourceFlag::Tracker);
        peers.ban(banned.ip());

        assert_eq!(
            peers.resume_peers(),
            ResumePeers {
                peers: vec![connecting, never],
                banned: vec![banned.ip()],
            }
        );
        // The list a pause leaves saves the same.
        assert_eq!(peers.idle_list().resume_peers().banned, vec![banned.ip()]);
    }

    // libtorrent `torrent::write_resume_data`: every peer we were connected
    // to is saved, and those we never were fill the list to a hundred.
    #[test]
    fn peers_never_connected_to_fill_the_saved_list_to_a_hundred() {
        let peers = make_peer_states();
        let connecting = peer_addr(0);
        peers.add_peer(connecting, PeerSourceFlag::Tracker);
        let (_, _rx, _tx) = peers.connect_next(Instant::now()).unwrap();
        for i in 1..=150 {
            peers.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
        }
        let saved = peers.resume_peers().peers;
        assert_eq!(saved.len(), RESUME_PEERS);
        assert_eq!(saved[0], connecting);
        let distinct: HashSet<_> = saved.iter().collect();
        assert_eq!(distinct.len(), RESUME_PEERS);
    }

    // libtorrent `torrent::read_resume_data` bans each saved address with
    // `peer_list::ban_peer`: the list a restart is founded on refuses a peer
    // there on any port, named or connecting to us.
    #[test]
    fn a_saved_ban_refuses_the_peer() {
        let banned = peer_addr(1);
        let peers = PeerStates::founded_on(
            PeerList::resumed([], [banned.ip()]),
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        );
        assert!(peers.is_banned(banned.ip()));
        peers.add_peer(banned, PeerSourceFlag::ResumeData);
        assert_eq!(state_of(&peers, banned), None);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        assert_eq!(
            peers
                .add_incoming(
                    at_port(banned, 50000),
                    Id20::default(),
                    tx,
                    ConnectionKind::Tcp,
                    OUR_PORT
                )
                .err(),
            Some(IncomingRefused::Banned)
        );
        assert_eq!(peers.resume_peers().banned, vec![banned.ip()]);
    }
}
