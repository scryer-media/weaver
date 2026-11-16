// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The share of a WireGuard tunnel's sockets each kind of traffic may hold.
//
// A tunnel holds a fixed number of sockets. Usenet connections, torrent
// peers over TCP, the tunnel's own sockets (name lookups, datagram sockets,
// probes) and HTTP (trackers, web seeds, feeds) each get a share of them, so
// one kind can never take the sockets another needs. Every stream or socket the session hands out holds
// one permit of its share until it is dropped.
//
// A connection kept open for later (a naming's DNS connection between
// lookups) holds a permit too, but only of room nothing else wants: a claim
// the share would refuse closes kept connections of the classes that would
// make room first, and takes the room they free. So what is kept never makes
// a lookup, a server test or a carrier fail or wait for a socket it would
// have got with nothing kept; at most it costs the next lookup one dial.
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::{Keeper, Purpose};

// Which share a socket counts against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SocketClass {
    Nntp,
    // Torrent peers over TCP. uTP peers ride the session's datagram socket
    // and take nothing of this.
    Torrent,
    // Name lookups, datagram sockets (DHT and uTP), probes and the carrier
    // a proxy session shares.
    ReserveCore,
    // Trackers, web seeds, feeds and other outbound HTTP.
    ReserveHttp,
}

impl SocketClass {
    pub fn of(purpose: &Purpose) -> Self {
        match purpose {
            Purpose::Nntp { .. } => Self::Nntp,
            Purpose::Torrent => Self::Torrent,
            Purpose::ProxyEndpoint {
                carrying: Some(class),
                ..
            } => *class,
            // A probe may test a server before it is saved or turned on,
            // when its route counts no connection on the tunnel yet.
            Purpose::NntpProbe { .. }
            | Purpose::ProxyEndpoint { .. }
            | Purpose::Probe
            | Purpose::Dns
            | Purpose::Datagram => Self::ReserveCore,
            Purpose::TorrentHttp | Purpose::Feed { .. } | Purpose::Outbound => Self::ReserveHttp,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nntp => "usenet",
            Self::Torrent => "torrent TCP",
            Self::ReserveCore => "reserve",
            Self::ReserveHttp => "tracker and HTTP",
        }
    }
}

// How many sockets each class may hold at once.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SocketShares {
    pub nntp: usize,
    pub torrent: usize,
    pub core: usize,
    pub http: usize,
}

impl SocketShares {
    pub fn of(&self, class: SocketClass) -> usize {
        match class {
            SocketClass::Nntp => self.nntp,
            SocketClass::Torrent => self.torrent,
            SocketClass::ReserveCore => self.core,
            SocketClass::ReserveHttp => self.http,
        }
    }
}

const CLASSES: [SocketClass; 4] = [
    SocketClass::Nntp,
    SocketClass::Torrent,
    SocketClass::ReserveCore,
    SocketClass::ReserveHttp,
];

const fn index(class: SocketClass) -> usize {
    match class {
        SocketClass::Nntp => 0,
        SocketClass::Torrent => 1,
        SocketClass::ReserveCore => 2,
        SocketClass::ReserveHttp => 3,
    }
}

// Every share of a session, under one lock so a re-budget and the sockets
// ending during it are settled together.
#[derive(Default)]
struct Ledger {
    size: [usize; 4],
    held: [usize; 4],
    // How much of each share grew while another share still held sockets
    // above its new size. That growth waits for those sockets to end.
    growing: [usize; 4],
}

impl Ledger {
    // Sockets held above their share's size, by shares that shrank.
    fn overflow(&self) -> usize {
        self.held
            .iter()
            .zip(&self.size)
            .map(|(held, size)| held.saturating_sub(*size))
            .sum()
    }

    // How many sockets each share may hold now: its size, less the growth
    // still waiting on the sockets the shrunk shares hold above their size.
    // A share takes no more of that wait than it could still open, since a
    // wait on sockets it would not open anyway holds nothing back.
    fn limits(&self) -> [usize; 4] {
        let mut overflow = self.overflow();
        let mut limits = self.size;
        let shares = limits.iter_mut().zip(&self.held).zip(&self.growing);
        for ((limit, &held), &growing) in shares {
            let waiting = overflow.min(growing).min(limit.saturating_sub(held));
            overflow -= waiting;
            *limit -= waiting;
        }
        limits
    }

    fn settle(&mut self) {
        if self.overflow() == 0 {
            self.growing = [0; 4];
        }
    }
}

struct Shares {
    ledger: Mutex<Ledger>,
    freed: [Arc<Notify>; 4],
}

impl Shares {
    fn ledger(&self) -> std::sync::MutexGuard<'_, Ledger> {
        self.ledger.lock().expect("socket shares")
    }

    fn wake(&self) {
        for freed in &self.freed {
            freed.notify_waiters();
        }
    }
}

// The permit one socket holds of its share.
pub struct SocketPermit {
    shares: Arc<Shares>,
    class: usize,
}

impl Drop for SocketPermit {
    fn drop(&mut self) {
        let mut ledger = self.shares.ledger();
        ledger.held[self.class] -= 1;
        let overflowed = ledger.growing.iter().any(|&growing| growing > 0);
        ledger.settle();
        drop(ledger);
        if overflowed {
            self.shares.wake();
        } else {
            self.shares.freed[self.class].notify_waiters();
        }
    }
}

// A session's shares. A session without them, any but a WireGuard one,
// holds its sockets to no share at all. The session's keeper lives here, so
// the share can close what is kept when it runs short.
#[derive(Default)]
pub struct SessionSockets {
    shares: Option<Arc<Shares>>,
    keeper: Keeper,
}

impl SessionSockets {
    pub fn new(shares: SocketShares) -> Self {
        Self {
            shares: Some(Arc::new(Shares {
                ledger: Mutex::new(Ledger {
                    size: CLASSES.map(|class| shares.of(class)),
                    ..Ledger::default()
                }),
                freed: CLASSES.map(|_| Arc::new(Notify::new())),
            })),
            keeper: Keeper::default(),
        }
    }

    // What the session keeps open for later.
    pub fn keeper(&self) -> &Keeper {
        &self.keeper
    }

    // Which kept sockets would make room for a claim of `class` the share
    // refuses now: one of `class` itself, or of a share that holds more than
    // its size while another grows.
    fn reclaimable(inner: &Shares, class: SocketClass) -> impl Fn(SocketClass) -> bool {
        let over: Vec<SocketClass> = {
            let ledger = inner.ledger();
            CLASSES
                .into_iter()
                .filter(|other| ledger.held[index(*other)] > ledger.size[index(*other)])
                .collect()
        };
        move |kept| kept == class || over.contains(&kept)
    }

    // Closes one kept connection that would make room for a claim of
    // `class`. Says whether one closed; its permit is back in the ledger
    // when this returns.
    fn reclaim(&self, inner: &Shares, class: SocketClass) -> bool {
        self.keeper.reclaim(Self::reclaimable(inner, class))
    }

    // Resize every share at once. Sockets already open stay open. A share
    // that shrank below them takes no new one until enough have ended, and
    // a share that grew meanwhile grows only into the sockets left free
    // under the new total, the rest as the shrunk shares' sockets end.
    pub fn set_shares(&self, shares: SocketShares) {
        let Some(inner) = &self.shares else {
            return;
        };
        let mut ledger = inner.ledger();
        for class in CLASSES {
            let i = index(class);
            let size = shares.of(class);
            ledger.growing[i] = if size >= ledger.size[i] {
                ledger.growing[i] + (size - ledger.size[i])
            } else {
                ledger.growing[i].saturating_sub(ledger.size[i] - size)
            };
            ledger.size[i] = size;
        }
        ledger.settle();
        drop(ledger);
        inner.wake();
    }

    // The shares in force, when the session has them.
    pub fn shares(&self) -> Option<SocketShares> {
        let ledger = self.shares.as_ref()?.ledger();
        Some(SocketShares {
            nntp: ledger.size[0],
            torrent: ledger.size[1],
            core: ledger.size[2],
            http: ledger.size[3],
        })
    }

    // Whether `class` has a socket free now.
    pub fn has_room(&self, class: SocketClass) -> bool {
        self.room(class).is_ok()
    }

    // A socket free in `class`'s share now, counting one a kept connection
    // would free when `take` closes it, or the signal that one is freed.
    pub fn room(&self, class: SocketClass) -> Result<(), Arc<Notify>> {
        let Some(inner) = &self.shares else {
            return Ok(());
        };
        let i = index(class);
        let ledger = inner.ledger();
        if ledger.held[i] < ledger.limits()[i] {
            return Ok(());
        }
        drop(ledger);
        // Room a kept connection would give back counts, but nothing closes
        // here: a caller may ask while it holds a lock a closer takes. Only
        // `take` closes one.
        if self.keeper.could_reclaim(Self::reclaimable(inner, class)) {
            return Ok(());
        }
        Err(Arc::clone(&inner.freed[i]))
    }

    // A permit of `class`'s share, or the signal that one is freed when the
    // share is full. A session without shares hands out no permits.
    pub fn take(&self, class: SocketClass) -> Result<Option<SocketPermit>, Arc<Notify>> {
        let Some(inner) = &self.shares else {
            return Ok(None);
        };
        let i = index(class);
        loop {
            let mut ledger = inner.ledger();
            if ledger.held[i] < ledger.limits()[i] {
                ledger.held[i] += 1;
                return Ok(Some(SocketPermit {
                    shares: Arc::clone(inner),
                    class: i,
                }));
            }
            drop(ledger);
            if !self.reclaim(inner, class) {
                return Err(Arc::clone(&inner.freed[i]));
            }
        }
    }

    // Sockets of `class` open now.
    pub fn held(&self, class: SocketClass) -> usize {
        self.shares
            .as_ref()
            .map_or(0, |inner| inner.ledger().held[index(class)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe::KeptRegistration;
    use std::sync::atomic::{AtomicBool, Ordering};

    const SHARES: SocketShares = SocketShares {
        nntp: 2,
        torrent: 1,
        core: 1,
        http: 1,
    };

    #[test]
    fn each_class_holds_its_own_share() {
        let sockets = SessionSockets::new(SHARES);
        let nntp: Vec<_> = (0..2)
            .map(|_| sockets.take(SocketClass::Nntp).unwrap().unwrap())
            .collect();
        assert!(sockets.take(SocketClass::Nntp).is_err());
        let torrent = sockets.take(SocketClass::Torrent).unwrap().unwrap();
        assert!(!sockets.has_room(SocketClass::Torrent));
        assert!(sockets.has_room(SocketClass::ReserveCore));
        let tracker = sockets.take(SocketClass::ReserveHttp).unwrap().unwrap();
        assert!(sockets.take(SocketClass::ReserveHttp).is_err());
        assert!(sockets.has_room(SocketClass::ReserveCore));
        drop(tracker);
        drop(torrent);
        assert!(sockets.has_room(SocketClass::Torrent));
        drop(nntp);
        assert_eq!(sockets.held(SocketClass::Nntp), 0);
    }

    #[test]
    fn a_shrunk_share_takes_busy_permits_back_as_they_end() {
        let sockets = SessionSockets::new(SHARES);
        let busy: Vec<_> = (0..2)
            .map(|_| sockets.take(SocketClass::Nntp).unwrap().unwrap())
            .collect();
        sockets.set_shares(SocketShares { nntp: 1, ..SHARES });
        assert_eq!(sockets.held(SocketClass::Nntp), 2);
        let mut busy = busy.into_iter();
        drop(busy.next());
        assert!(!sockets.has_room(SocketClass::Nntp));
        drop(busy.next());
        assert_eq!(sockets.held(SocketClass::Nntp), 0);
        let one = sockets.take(SocketClass::Nntp).unwrap().unwrap();
        assert!(sockets.take(SocketClass::Nntp).is_err());
        drop(one);

        sockets.set_shares(SocketShares { nntp: 3, ..SHARES });
        let grown: Vec<_> = (0..3)
            .map(|_| sockets.take(SocketClass::Nntp).unwrap().unwrap())
            .collect();
        assert!(sockets.take(SocketClass::Nntp).is_err());
        drop(grown);
    }

    #[test]
    fn a_share_grown_while_permits_are_owed_cancels_them_first() {
        let sockets = SessionSockets::new(SHARES);
        let busy: Vec<_> = (0..2)
            .map(|_| sockets.take(SocketClass::Nntp).unwrap().unwrap())
            .collect();
        sockets.set_shares(SocketShares { nntp: 0, ..SHARES });
        sockets.set_shares(SocketShares { nntp: 2, ..SHARES });
        assert!(sockets.take(SocketClass::Nntp).is_err());
        drop(busy);
        assert_eq!(sockets.held(SocketClass::Nntp), 0);
        let _again: Vec<_> = (0..2)
            .map(|_| sockets.take(SocketClass::Nntp).unwrap().unwrap())
            .collect();
        assert!(sockets.take(SocketClass::Nntp).is_err());
    }

    // `count` kept connections of `class`, each holding a permit, and the
    // switch each one's closer turns.
    fn kept(
        sockets: &SessionSockets,
        class: SocketClass,
        count: usize,
    ) -> (Vec<KeptRegistration>, Vec<Arc<AtomicBool>>) {
        let handle = sockets.keeper().handle();
        let closed: Vec<_> = (0..count)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();
        let kept = closed
            .iter()
            .map(|closed| {
                let permit = sockets.take(class).unwrap().unwrap();
                let closed = closed.clone();
                handle
                    .hold(
                        class,
                        Box::new(move || {
                            drop(permit);
                            closed.store(true, Ordering::Relaxed);
                        }),
                    )
                    .unwrap()
            })
            .collect();
        (kept, closed)
    }

    // Kept connections fill the reserve share: a claim of it closes the
    // oldest kept one and takes its room, at once, and a claim no kept
    // connection would make room for is refused as before.
    #[test]
    fn a_claim_closes_a_kept_connection_before_the_share_refuses() {
        let sockets = SessionSockets::new(SocketShares { core: 2, ..SHARES });
        let (kept, closed) = kept(&sockets, SocketClass::ReserveCore, 2);
        assert_eq!(sockets.held(SocketClass::ReserveCore), 2);
        assert!(sockets.take(SocketClass::Nntp).unwrap().is_some());

        let cold = sockets.take(SocketClass::ReserveCore).unwrap().unwrap();
        let closed_now = |at: usize| closed[at].load(Ordering::Relaxed);
        assert!(closed_now(0));
        assert!(!closed_now(1));
        assert_eq!(sockets.keeper().holding(), 1);
        let second = sockets.take(SocketClass::ReserveCore).unwrap().unwrap();
        assert!(closed_now(1));
        assert_eq!(sockets.keeper().holding(), 0);
        assert!(sockets.take(SocketClass::ReserveCore).is_err());
        drop((kept, cold, second));
    }

    // Asking for room counts what a kept connection would give back, and
    // closes nothing: a caller may ask under a lock a closer takes.
    #[test]
    fn asking_for_room_closes_nothing() {
        let sockets = SessionSockets::new(SocketShares { core: 1, ..SHARES });
        let (kept, closed) = kept(&sockets, SocketClass::ReserveCore, 1);
        assert!(sockets.has_room(SocketClass::ReserveCore));
        assert!(sockets.room(SocketClass::ReserveCore).is_ok());
        assert!(!closed[0].load(Ordering::Relaxed));
        assert_eq!(sockets.keeper().holding(), 1);
        assert_eq!(sockets.held(SocketClass::ReserveCore), 1);
        // A share no kept connection would make room in is still full.
        let busy = sockets.take(SocketClass::Torrent).unwrap().unwrap();
        assert!(!sockets.has_room(SocketClass::Torrent));
        assert!(!closed[0].load(Ordering::Relaxed));
        drop((kept, busy));
    }

    // A share shrunk below what it keeps open holds the room another share
    // grew into: a claim of the grown share closes a kept connection of the
    // shrunk one, and takes the room it frees.
    #[test]
    fn a_claim_closes_a_kept_connection_of_a_share_over_its_size() {
        let sockets = SessionSockets::new(SocketShares { core: 2, ..SHARES });
        let (kept, closed) = kept(&sockets, SocketClass::ReserveCore, 2);
        sockets.set_shares(SocketShares {
            nntp: 4,
            core: 0,
            ..SHARES
        });
        let busy: Vec<_> = (0..2)
            .map(|_| sockets.take(SocketClass::Nntp).unwrap().unwrap())
            .collect();
        assert!(sockets.has_room(SocketClass::Nntp));
        assert!(!closed[0].load(Ordering::Relaxed));
        let third = sockets.take(SocketClass::Nntp).unwrap().unwrap();
        assert!(closed[0].load(Ordering::Relaxed));
        assert!(!closed[1].load(Ordering::Relaxed));
        assert_eq!(sockets.held(SocketClass::ReserveCore), 1);
        let fourth = sockets.take(SocketClass::Nntp).unwrap().unwrap();
        assert!(closed[1].load(Ordering::Relaxed));
        assert!(sockets.take(SocketClass::Nntp).is_err());
        drop((kept, busy, third, fourth));
    }

    // A keeper cut closes everything it keeps.
    #[test]
    fn a_cut_closes_what_is_kept() {
        let sockets = SessionSockets::new(SHARES);
        let permit = sockets.take(SocketClass::ReserveCore).unwrap().unwrap();
        let _held = sockets
            .keeper()
            .handle()
            .hold(SocketClass::ReserveCore, Box::new(move || drop(permit)))
            .unwrap();
        assert_eq!(sockets.held(SocketClass::ReserveCore), 1);
        sockets.keeper().cut();
        assert_eq!(sockets.held(SocketClass::ReserveCore), 0);
        assert_eq!(sockets.keeper().holding(), 0);
    }

    #[test]
    fn a_session_without_shares_takes_no_permit() {
        let sockets = SessionSockets::default();
        assert!(sockets.take(SocketClass::Torrent).unwrap().is_none());
        assert!(sockets.has_room(SocketClass::Torrent));
        assert_eq!(sockets.shares(), None);
    }

    // Usenet shrinks from 101 to 51 with all 101 open while torrent grows
    // from 123 to 173: torrent takes only what the open usenet sockets leave
    // under the total, and the rest as they close.
    #[test]
    fn a_rebudget_never_lets_the_shares_hold_more_than_the_total() {
        let before = SocketShares {
            nntp: 101,
            torrent: 123,
            core: 8,
            http: 8,
        };
        let sockets = SessionSockets::new(before);
        let mut usenet: Vec<_> = (0..101)
            .map(|_| sockets.take(SocketClass::Nntp).unwrap().unwrap())
            .collect();
        let mut torrent: Vec<_> = (0..100)
            .map(|_| sockets.take(SocketClass::Torrent).unwrap().unwrap())
            .collect();
        sockets.set_shares(SocketShares {
            nntp: 51,
            torrent: 173,
            core: 8,
            http: 8,
        });
        let total = 256 - 16;
        let take_all = |torrent: &mut Vec<SocketPermit>| {
            while let Ok(permit) = sockets.take(SocketClass::Torrent) {
                torrent.push(permit.unwrap());
            }
        };
        take_all(&mut torrent);
        assert_eq!(torrent.len(), total - 101 - 16);
        let reserve: Vec<_> = [SocketClass::ReserveCore, SocketClass::ReserveHttp]
            .into_iter()
            .flat_map(|class| std::iter::repeat_n(class, 8))
            .map(|class| sockets.take(class).unwrap().unwrap())
            .collect();
        while usenet.len() > 51 {
            usenet.pop();
            assert!(!sockets.has_room(SocketClass::Nntp));
            take_all(&mut torrent);
            assert_eq!(usenet.len() + torrent.len() + reserve.len(), total);
        }
        assert_eq!(torrent.len(), 173);
        usenet.pop();
        assert!(!sockets.has_room(SocketClass::Torrent));
        assert!(sockets.take(SocketClass::Nntp).unwrap().is_some());
    }

    // A share that moves twice before the shrunk share's sockets end still
    // never lets the shares hold more than they add up to.
    #[test]
    fn two_rebudgets_in_a_row_never_let_the_shares_hold_more_than_the_total() {
        let shares = |nntp, torrent| SocketShares {
            nntp,
            torrent,
            core: 8,
            http: 8,
        };
        let sockets = SessionSockets::new(shares(100, 100));
        let take_all = |class, held: &mut Vec<SocketPermit>| {
            while let Ok(permit) = sockets.take(class) {
                held.push(permit.unwrap());
            }
        };
        let mut usenet = Vec::new();
        let mut torrent = Vec::new();
        take_all(SocketClass::Nntp, &mut usenet);
        take_all(SocketClass::Torrent, &mut torrent);
        assert_eq!((usenet.len(), torrent.len()), (100, 100));

        sockets.set_shares(shares(50, 150));
        take_all(SocketClass::Torrent, &mut torrent);
        assert_eq!(torrent.len(), 100);

        sockets.set_shares(shares(70, 130));
        take_all(SocketClass::Torrent, &mut torrent);
        assert_eq!(torrent.len(), 100);
        while usenet.len() > 70 {
            usenet.pop();
            take_all(SocketClass::Nntp, &mut usenet);
            take_all(SocketClass::Torrent, &mut torrent);
            assert_eq!(usenet.len() + torrent.len(), 200);
        }
        assert_eq!((usenet.len(), torrent.len()), (70, 130));
        assert!(!sockets.has_room(SocketClass::Nntp));
        assert!(!sockets.has_room(SocketClass::Torrent));
    }

    // A share at exactly its size takes none of the wait another share's
    // growth needs, so the reserve shrinking beside it still holds the
    // shares to what they add up to.
    #[test]
    fn a_share_at_its_size_takes_none_of_the_wait_on_a_shrunk_reserve() {
        let shares = |nntp, torrent, core| SocketShares {
            nntp,
            torrent,
            core,
            http: 4,
        };
        let sockets = SessionSockets::new(shares(101, 117, 18));
        let take_all = |class, held: &mut Vec<SocketPermit>| {
            while let Ok(permit) = sockets.take(class) {
                held.push(permit.unwrap());
            }
        };
        let mut usenet = Vec::new();
        let mut torrent = Vec::new();
        let mut core = Vec::new();
        let mut http = Vec::new();
        take_all(SocketClass::Nntp, &mut usenet);
        take_all(SocketClass::Torrent, &mut torrent);
        take_all(SocketClass::ReserveCore, &mut core);
        take_all(SocketClass::ReserveHttp, &mut http);
        let total = |usenet: &Vec<_>, torrent: &Vec<_>, core: &Vec<_>| {
            usenet.len() + torrent.len() + core.len() + http.len()
        };
        assert_eq!(total(&usenet, &torrent, &core), 240);

        for step in [
            shares(51, 167, 18),
            shares(71, 147, 18),
            shares(71, 152, 13),
        ] {
            sockets.set_shares(step);
            take_all(SocketClass::Torrent, &mut torrent);
            assert_eq!(total(&usenet, &torrent, &core), 240);
        }
        usenet.truncate(71);
        take_all(SocketClass::Torrent, &mut torrent);
        assert_eq!(torrent.len(), 147);
        assert_eq!(total(&usenet, &torrent, &core), 240);
        core.truncate(13);
        take_all(SocketClass::Torrent, &mut torrent);
        assert_eq!(torrent.len(), 152);
        assert_eq!(total(&usenet, &torrent, &core), 240);
    }
}
