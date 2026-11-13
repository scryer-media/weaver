// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What the peer list keeps beside its peers so that choosing whom to
//! connect to next, when that will be, how many could be, and whom to forget
//! to make room read a few entries, not the whole list.
//!
//! The list's table of peers is what holds each peer. These sets hold copies
//! of the keys the peers rank by, filed again each time a peer changes in a
//! way that moves its key ([`super::PeerStates::reindex`]):
//!
//! - every connect candidate, by host, with its rank and the time it is due;
//! - of those at a host no connection holds back, the ones due by the last
//!   time asked, in rank order, and every one with a due time, in time
//!   order;
//! - the hosts with a connection, open or opening;
//! - every peer with no connection, web seeds aside, in the order the list
//!   forgets them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::ops::Bound::{Excluded, Included, Unbounded};

use tokio::time::Instant;

use super::connect_order::{ConnectRank, ConnectRanking};
use super::{Displace, EraseRank, NextCandidate};

// The lowest and highest addresses there are, so a range of the candidates
// keyed by `(host, address)` takes every address at one host.
const LOWEST_ADDR: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
const HIGHEST_ADDR: SocketAddr = SocketAddr::V6(SocketAddrV6::new(
    Ipv6Addr::new(
        0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
    ),
    u16::MAX,
    u32::MAX,
    u32::MAX,
));

/// What every peer's rank and place in the sets was worked out against:
/// the order's endpoints and start, whether we were finished, and whether
/// peers at one host may each have a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Basis {
    pub(super) ranking: ConnectRanking,
    pub(super) finished: bool,
    pub(super) allow_multiple: bool,
}

/// What a connect candidate is filed under: its rank (`Peer::connect_rank`)
/// and the time it may be connected to, None for at once
/// (`Peer::reconnect_at`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ConnectKey {
    pub(super) rank: ConnectRank,
    pub(super) due: Option<Instant>,
}

/// What the sets hold of one peer, kept on the peer so a change that moves
/// nothing takes no lock, and a peer leaving the list takes out the key its
/// erase order was filed under.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Indexed {
    pub(super) candidate: bool,
    pub(super) erase: Option<EraseRank>,
}

#[derive(Clone, Copy, Debug)]
struct Slot {
    key: ConnectKey,
    // Whether no connection at its host holds it back, so it is in `Free`.
    free: bool,
}

/// The connect candidates no connection at their host holds back.
#[derive(Debug, Default)]
struct Free {
    // Those due by `watermark`, a peer with no due time among them, in the
    // order they are connected to.
    ready: BTreeSet<(ConnectRank, SocketAddr)>,
    // Every one with a due time, by it, with its rank: those due by
    // `watermark` are in `ready` too.
    waiting: BTreeMap<(Instant, SocketAddr), ConnectRank>,
    // How many of `waiting` are in `ready`.
    promoted: usize,
    // The time `ready` was brought to; None before any.
    watermark: Option<Instant>,
}

impl Free {
    fn place(&mut self, addr: SocketAddr, key: ConnectKey) {
        match key.due {
            None => {
                self.ready.insert((key.rank, addr));
            }
            Some(due) => {
                self.waiting.insert((due, addr), key.rank);
                if self.watermark.is_some_and(|w| due <= w) {
                    self.ready.insert((key.rank, addr));
                    self.promoted += 1;
                }
            }
        }
    }

    fn displace(&mut self, addr: SocketAddr, key: ConnectKey) {
        match key.due {
            None => {
                self.ready.remove(&(key.rank, addr));
            }
            Some(due) => {
                self.waiting.remove(&(due, addr));
                if self.watermark.is_some_and(|w| due <= w) {
                    self.ready.remove(&(key.rank, addr));
                    self.promoted -= 1;
                }
            }
        }
    }

    // Brings `ready` to `now`: every candidate due by then joins it, and,
    // when `now` is before the last time asked, as two callers that read
    // the clock in one order and ask in the other can make it, those due
    // only after `now` leave it. Each candidate joins once per wait while
    // time goes forward.
    fn advance(&mut self, now: Instant) {
        let Self {
            ready,
            waiting,
            promoted,
            watermark,
        } = self;
        match *watermark {
            Some(w) if now == w => return,
            Some(w) if now < w => {
                for (&(_, addr), &rank) in
                    waiting.range((Excluded((now, HIGHEST_ADDR)), Included((w, HIGHEST_ADDR))))
                {
                    ready.remove(&(rank, addr));
                    *promoted -= 1;
                }
            }
            from => {
                let low = from.map_or(Unbounded, |w| Excluded((w, HIGHEST_ADDR)));
                for (&(_, addr), &rank) in waiting.range((low, Included((now, HIGHEST_ADDR)))) {
                    ready.insert((rank, addr));
                    *promoted += 1;
                }
            }
        }
        *watermark = Some(now);
    }

    // When the next candidate is due, `ready` brought to the time asked.
    // With none ready, none of `waiting` is due by then, so the first of
    // it is the soonest.
    fn next(&self) -> NextCandidate {
        if !self.ready.is_empty() {
            NextCandidate::Due
        } else if let Some(&(due, _)) = self.waiting.keys().next() {
            NextCandidate::At(due)
        } else {
            NextCandidate::None
        }
    }

    fn len(&self) -> usize {
        self.ready.len() - self.promoted + self.waiting.len()
    }
}

/// The peer list's sets; see the module's notes.
#[derive(Debug)]
pub(super) struct PeerIndex {
    pub(super) basis: Basis,
    candidates: BTreeMap<(IpAddr, SocketAddr), Slot>,
    free: Free,
    // The peers on the list with a connection, open or opening, web seeds
    // aside, by host.
    connected: HashMap<IpAddr, Vec<SocketAddr>>,
    erase: BTreeSet<(EraseRank, SocketAddr)>,
}

// The candidates at `ip` are held back by a connection there, or not when
// `free`.
fn set_free_at(
    candidates: &mut BTreeMap<(IpAddr, SocketAddr), Slot>,
    sets: &mut Free,
    ip: IpAddr,
    free: bool,
) {
    for (&(_, addr), slot) in candidates.range_mut((ip, LOWEST_ADDR)..=(ip, HIGHEST_ADDR)) {
        if slot.free != free {
            slot.free = free;
            if free {
                sets.place(addr, slot.key);
            } else {
                sets.displace(addr, slot.key);
            }
        }
    }
}

impl PeerIndex {
    pub(super) fn new(basis: Basis) -> Self {
        Self {
            basis,
            candidates: BTreeMap::new(),
            free: Free::default(),
            connected: HashMap::new(),
            erase: BTreeSet::new(),
        }
    }

    // Whether a connection at `ip` holds back the candidates there.
    fn blocked(&self, ip: IpAddr) -> bool {
        !self.basis.allow_multiple && self.connected.contains_key(&ip)
    }

    /// Files the peer at `addr` as a connect candidate under `key`, or as
    /// none.
    pub(super) fn file(&mut self, addr: SocketAddr, key: Option<ConnectKey>) {
        let at = (addr.ip(), addr);
        let free = !self.blocked(addr.ip());
        if let (Some(slot), Some(key)) = (self.candidates.get(&at), key)
            && slot.key == key
            && slot.free == free
        {
            return;
        }
        if let Some(old) = self.candidates.remove(&at)
            && old.free
        {
            self.free.displace(addr, old.key);
        }
        if let Some(key) = key {
            self.candidates.insert(at, Slot { key, free });
            if free {
                self.free.place(addr, key);
            }
        }
    }

    /// Files the peer at `addr` in the erase order under `new`, out of
    /// `old`.
    pub(super) fn file_erase(
        &mut self,
        addr: SocketAddr,
        old: Option<EraseRank>,
        new: Option<EraseRank>,
    ) {
        if let Some(old) = old {
            self.erase.remove(&(old, addr));
        }
        if let Some(new) = new {
            self.erase.insert((new, addr));
        }
    }

    /// A connection to the peer at `addr` opened, or began to: the first at
    /// its host holds back the candidates there.
    pub(super) fn connection_opened(&mut self, addr: SocketAddr) {
        let ip = addr.ip();
        let at = self.connected.entry(ip).or_default();
        at.push(addr);
        if at.len() == 1 && !self.basis.allow_multiple {
            set_free_at(&mut self.candidates, &mut self.free, ip, false);
        }
    }

    /// The connection to the peer at `addr` is gone: the last at its host
    /// holds back the candidates there no longer.
    pub(super) fn connection_closed(&mut self, addr: SocketAddr) {
        let ip = addr.ip();
        let std::collections::hash_map::Entry::Occupied(mut at) = self.connected.entry(ip) else {
            return;
        };
        if let Some(i) = at.get().iter().position(|held| *held == addr) {
            at.get_mut().swap_remove(i);
        }
        if at.get().is_empty() {
            at.remove();
            if !self.basis.allow_multiple {
                set_free_at(&mut self.candidates, &mut self.free, ip, true);
            }
        }
    }

    /// Whether peers at one host may each have a connection is `allow` now:
    /// the candidates at every host with a connection move. Only an
    /// operator's change to the session setting moves it.
    pub(super) fn allow_multiple(&mut self, allow: bool) {
        if self.basis.allow_multiple == allow {
            return;
        }
        self.basis.allow_multiple = allow;
        for ip in self.connected.keys() {
            set_free_at(&mut self.candidates, &mut self.free, *ip, allow);
        }
    }

    /// The peers with a connection at `ip`.
    pub(super) fn connected_at(&self, ip: IpAddr) -> &[SocketAddr] {
        self.connected.get(&ip).map_or(&[], Vec::as_slice)
    }

    /// The candidate that ranks first of those due at `now`, at a host no
    /// connection holds back.
    pub(super) fn first_due(&mut self, now: Instant) -> Option<SocketAddr> {
        self.free.advance(now);
        self.free.ready.first().map(|(_, addr)| *addr)
    }

    /// When a candidate at a host no connection holds back is next due,
    /// asked at `now`.
    pub(super) fn next_candidate(&mut self, now: Instant) -> NextCandidate {
        self.free.advance(now);
        self.free.next()
    }

    /// How many candidates are at a host no connection holds back, due or
    /// not.
    pub(super) fn candidates(&self) -> usize {
        self.free.len()
    }

    /// The `n` peers the list forgets first of those `displace` allows.
    /// Those it allows come first in the order, so they are its head.
    pub(super) fn erase_victims(&self, displace: Displace, n: usize) -> Vec<SocketAddr> {
        self.erase
            .iter()
            .take_while(|(rank, _)| displace.allows(rank))
            .take(n)
            .map(|(_, addr)| *addr)
            .collect()
    }

    #[cfg(test)]
    pub(super) fn connected_ips(&self) -> std::collections::HashSet<IpAddr> {
        self.connected.keys().copied().collect()
    }

    #[cfg(test)]
    pub(super) fn holds_nothing(&self) -> bool {
        self.candidates.is_empty()
            && self.free.ready.is_empty()
            && self.free.waiting.is_empty()
            && self.free.promoted == 0
            && self.connected.is_empty()
            && self.erase.is_empty()
    }

    // The memory one entry of each set takes, its key and value side by
    // side, before the B-tree's own node overhead.
    #[cfg(test)]
    pub(super) fn entry_sizes() -> [(&'static str, usize); 4] {
        use std::mem::size_of;
        [
            (
                "candidates",
                size_of::<(IpAddr, SocketAddr)>() + size_of::<Slot>(),
            ),
            ("ready", size_of::<(ConnectRank, SocketAddr)>()),
            (
                "waiting",
                size_of::<(Instant, SocketAddr)>() + size_of::<ConnectRank>(),
            ),
            ("erase", size_of::<(EraseRank, SocketAddr)>()),
        ]
    }
}
