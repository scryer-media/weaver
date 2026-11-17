// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a stage holds open for later on behalf of the work dialled through
// it: the DNS connections a naming keeps between lookups.
//
// Each stage instance owns one `Keeper`. Whatever is kept after a dial
// through a stage is registered with the keeper of that stage and of every
// stage beneath it, and is closed:
//
// - when any of those stages retires or shuts down, or is dropped, so a
//   proxy edited, disabled or replaced takes what was kept through it with
//   it, at once;
// - when a session's bytes are short, as many as a claim needs, before the
//   share refuses it or leaves it waiting (`SessionSockets`). A claim then
//   refused only for their bytes waits for them (`SessionSockets::claim`),
//   so nothing kept ever makes other work fail, or wait for longer than its
//   bytes take to come back. Keeping one rings its class's slot, so a claim
//   waiting while it was in use looks again and closes it.
//
// A kept thing remembers the stage it was dialled through by its keeper, and
// is handed back only to a dial through that same live stage.
//
// A keeper costs nothing until something is first kept through it: its
// holders are made on the first `handle`, which only a DNS lookup or a DNS
// dial asks for, so a dial of any other traffic allocates nothing for it.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

use super::{Reclaimable, SocketClass};

// Closes a kept thing, and says whether it closed one: none when it was
// taken back for use first.
type Close = Box<dyn FnOnce() -> bool + Send>;

// The class and bytes of a socket held in a share.
pub(crate) type Held = (SocketClass, u64);

// The bytes a dial's socket reserved in one session's share: the keeper of
// that session's bytes, and the socket. A connection kept after the dial
// registers it with that keeper, so its share reclaims exactly what the
// socket holds.
#[derive(Clone)]
pub(crate) struct Reservation {
    pub(crate) keeper: KeeperRef,
    pub(crate) socket: Reclaimable,
}

#[derive(Default)]
struct Holders {
    next: u64,
    // By registration, oldest first, so the share reclaims the oldest. The
    // socket the kept thing holds in this stage's share, as reserved when it
    // was dialled, or none for a stage it only rides on.
    by_id: BTreeMap<u64, (Option<Reclaimable>, Close)>,
    // Set, under the lock, once the keeper is dropped: nothing is held
    // after, even by a holder that reached the keeper before it went.
    closed: bool,
    // Counts the cuts. A handle remembers the count it was taken at, and
    // nothing is held through one taken before a cut: a lookup in flight as
    // the stage retires or shuts down closes its connection rather than
    // keep it on the stage being let go. A lookup begun after keeps again.
    cuts: u64,
}

// What one stage instance keeps open for later. Dropping it closes all of
// it.
#[derive(Default)]
pub struct Keeper {
    holders: OnceLock<Arc<Mutex<Holders>>>,
}

impl Keeper {
    // The handle a kept thing remembers this keeper by.
    pub fn handle(&self) -> KeeperRef {
        let holders = self.holders.get_or_init(Arc::default);
        let cuts = lock(holders).cuts;
        KeeperRef(Arc::downgrade(holders), cuts)
    }

    // Closes everything kept through this stage. The stage may keep more
    // later.
    pub fn cut(&self) {
        self.cut_all(false);
    }

    fn cut_all(&self, for_good: bool) {
        let Some(holders) = self.holders.get() else {
            return;
        };
        let closing = {
            let mut held = lock(holders);
            held.closed |= for_good;
            held.cuts += 1;
            std::mem::take(&mut held.by_id)
        };
        // Closed outside the lock: closing takes the kept thing's own lock,
        // and its registrations come back to this one.
        for (_, close) in closing.into_values() {
            close();
        }
    }

    // The sockets things kept hold in this stage's share, oldest first, in
    // the order `reclaim` closes them. Closes nothing.
    pub(crate) fn held(&self) -> Vec<Held> {
        self.holders.get().map_or_else(Vec::new, |holders| {
            lock(holders)
                .by_id
                .values()
                .filter_map(|(socket, _)| socket.as_ref().map(Reclaimable::held))
                .collect()
        })
    }

    // Closes the oldest thing kept that holds a socket in this stage's
    // share, marks its bytes reclaimed, and says whether there was one. One
    // taken back for use as it was found closes nothing and is not counted:
    // the next is closed in its place.
    pub(crate) fn reclaim(&self) -> bool {
        let Some(holders) = self.holders.get() else {
            return false;
        };
        loop {
            let closing = {
                let mut holders = lock(holders);
                let found = holders
                    .by_id
                    .iter()
                    .find(|(_, (socket, _))| socket.is_some())
                    .map(|(id, _)| *id);
                found.and_then(|id| holders.by_id.remove(&id))
            };
            let Some((Some(socket), close)) = closing else {
                return false;
            };
            if close() {
                socket.reclaimed();
                return true;
            }
        }
    }

    // How many things are kept through this stage now.
    #[cfg(test)]
    pub(crate) fn holding(&self) -> usize {
        self.holders
            .get()
            .map_or(0, |holders| lock(holders).by_id.len())
    }

    // Whether anything ever asked for a handle, and so made its holders.
    #[cfg(test)]
    pub(crate) fn asked_for(&self) -> bool {
        self.holders.get().is_some()
    }
}

impl Drop for Keeper {
    fn drop(&mut self) {
        self.cut_all(true);
    }
}

// A keeper, held weakly: it names the stage instance something was kept
// through, and registers what is kept with it while it lives.
// With the count of cuts when it was taken.
#[derive(Clone)]
pub struct KeeperRef(Weak<Mutex<Holders>>, u64);

impl KeeperRef {
    // Whether this names the same stage instance as `other`, and it is
    // still alive.
    pub(crate) fn is(&self, other: &KeeperRef) -> bool {
        Weak::ptr_eq(&self.0, &other.0)
            && self
                .0
                .upgrade()
                .is_some_and(|holders| !lock(&holders).closed)
    }

    // Registers `close` to run when this keeper cuts what it keeps, or, when
    // the kept thing holds `socket` in this keeper's share, reclaims it.
    // None when the keeper is gone, or going, or cut since this handle was
    // taken: nothing may be kept through it, and the caller closes what it
    // meant to keep. One holding a socket rings its class's slot once
    // registered, outside the lock.
    pub(crate) fn hold(&self, socket: Option<Reclaimable>, close: Close) -> Option<Registration> {
        let holders = self.0.upgrade()?;
        let mut held = lock(&holders);
        if held.closed || held.cuts != self.1 {
            return None;
        }
        held.next += 1;
        let id = held.next;
        let ring = socket.clone();
        held.by_id.insert(id, (socket, close));
        drop(held);
        if let Some(socket) = ring {
            socket.kept();
        }
        Some(Registration {
            keeper: self.0.clone(),
            id,
        })
    }
}

// One registration with a keeper. Dropped, it is withdrawn: what was kept
// was taken back for use, or closed some other way.
pub(crate) struct Registration {
    keeper: Weak<Mutex<Holders>>,
    id: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(holders) = self.keeper.upgrade() {
            let withdrawn = lock(&holders).by_id.remove(&self.id);
            drop(withdrawn);
        }
    }
}

fn lock(holders: &Mutex<Holders>) -> MutexGuard<'_, Holders> {
    holders.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe::{SessionSockets, SocketPermit, dns_stream_bytes, reserve_of};
    use std::sync::atomic::{AtomicBool, Ordering};

    // A DNS stream's bytes in `sockets`' reserve, and what a connection kept
    // on it registers.
    fn socket(sockets: &SessionSockets) -> (SocketPermit, Option<Reclaimable>) {
        let mut permit = sockets
            .take(SocketClass::Reserve, dns_stream_bytes())
            .expect("room")
            .expect("a budget");
        let socket = permit.reclaimable(SocketClass::Reserve);
        (permit, Some(socket))
    }

    fn closer(closed: &Arc<AtomicBool>) -> Close {
        let closed = closed.clone();
        Box::new(move || {
            closed.store(true, Ordering::Relaxed);
            true
        })
    }

    // A holder that reached the keeper just before it was dropped, as one on
    // another thread can, registers nothing after the drop: it is told the
    // keeper is gone, and closes what it meant to keep.
    #[test]
    fn nothing_is_held_once_the_keeper_is_dropped() {
        let keeper = Keeper::default();
        let handle = keeper.handle();
        // The holder's upgrade, taken before the drop.
        let reached = handle.0.upgrade().unwrap();
        let before = Arc::new(AtomicBool::new(false));
        let _held = handle.hold(None, closer(&before));
        drop(keeper);
        assert!(before.load(Ordering::Relaxed));
        let after = Arc::new(AtomicBool::new(false));
        assert!(handle.hold(None, closer(&after)).is_none());
        assert!(!handle.is(&handle));
        assert!(lock(&reached).by_id.is_empty());
    }

    // A cut closes what is kept, and the stage may keep again after, through
    // a handle taken after the cut.
    #[test]
    fn a_cut_closes_what_is_kept_and_the_stage_keeps_on() {
        let keeper = Keeper::default();
        let handle = keeper.handle();
        let closed = Arc::new(AtomicBool::new(false));
        let _held = handle.hold(None, closer(&closed));
        keeper.cut();
        assert!(closed.load(Ordering::Relaxed));
        assert_eq!(keeper.holding(), 0);
        let again = Arc::new(AtomicBool::new(false));
        let after = keeper.handle();
        let _again = after.hold(None, closer(&again)).unwrap();
        assert_eq!(keeper.holding(), 1);
        assert!(handle.is(&after));
    }

    // A lookup in flight as its stage retires or shuts down, holding the
    // handle it took before the cut, keeps nothing: the keeper refuses it,
    // and the caller closes the connection it meant to keep. Its socket's
    // class is not rung.
    #[test]
    fn a_link_offered_to_a_cut_keeper_is_closed_not_kept() {
        let sockets = reserve_of(2);
        let keeper = sockets.keeper();
        let in_flight = keeper.handle();
        keeper.cut();
        let (_permit, held) = socket(&sockets);
        let offered = Arc::new(AtomicBool::new(false));
        assert!(in_flight.hold(held, closer(&offered)).is_none());
        assert_eq!(keeper.holding(), 0);
        assert!(keeper.held().is_empty());
        // The closer was handed back unrun, as the caller's to close.
        assert!(!offered.load(Ordering::Relaxed));
        // A lookup begun after the cut keeps again.
        let (_next, held) = socket(&sockets);
        let _kept = keeper.handle().hold(held, closer(&offered)).unwrap();
        assert_eq!(keeper.holding(), 1);
    }

    // A keeper nothing was kept through holds nothing, and dropping it does
    // nothing, outside a runtime too.
    #[test]
    fn a_keeper_never_asked_for_costs_nothing() {
        let keeper = Keeper::default();
        assert!(keeper.holders.get().is_none());
        assert!(!keeper.reclaim());
        assert!(keeper.held().is_empty());
        keeper.cut();
        assert!(keeper.holders.get().is_none());
        drop(keeper);
    }

    // The share reclaims, oldest first, only what holds a socket in it,
    // and reads each at the bytes it registered. What only rides the stage
    // is closed by a cut alone.
    #[test]
    fn a_reclaim_closes_the_oldest_socket_held_in_the_share() {
        let sockets = reserve_of(2);
        let keeper = sockets.keeper();
        let handle = keeper.handle();
        let [riding, older, newer] = [(); 3].map(|_| Arc::new(AtomicBool::new(false)));
        let (_first, first) = socket(&sockets);
        let (_second, second) = socket(&sockets);
        let _riding = handle.hold(None, closer(&riding)).unwrap();
        let _older = handle.hold(first, closer(&older)).unwrap();
        let _newer = handle.hold(second, closer(&newer)).unwrap();
        assert_eq!(
            keeper.held(),
            [(SocketClass::Reserve, dns_stream_bytes()); 2]
        );
        assert!(keeper.reclaim());
        assert!(older.load(Ordering::Relaxed));
        assert!(!riding.load(Ordering::Relaxed) && !newer.load(Ordering::Relaxed));
        assert!(keeper.reclaim());
        assert!(newer.load(Ordering::Relaxed));
        assert!(!keeper.reclaim());
        assert!(!riding.load(Ordering::Relaxed));
        keeper.cut();
        assert!(riding.load(Ordering::Relaxed));
    }

    // A kept thing taken back for use as the share reclaims it closes
    // nothing, and is not counted as room made: the next one is closed in
    // its place, and with none left the reclaim says it made no room.
    #[test]
    fn a_reclaim_of_one_taken_back_closes_the_next() {
        let sockets = reserve_of(2);
        let keeper = sockets.keeper();
        let handle = keeper.handle();
        let (first_permit, first) = socket(&sockets);
        let (second_permit, second) = socket(&sockets);
        let taken = Arc::new(AtomicBool::new(false));
        let ran = taken.clone();
        let _taken = handle
            .hold(
                first,
                Box::new(move || {
                    ran.store(true, Ordering::Relaxed);
                    false
                }),
            )
            .unwrap();
        let next = Arc::new(AtomicBool::new(false));
        let _next = handle.hold(second, closer(&next)).unwrap();
        assert!(keeper.reclaim());
        assert!(taken.load(Ordering::Relaxed) && next.load(Ordering::Relaxed));
        assert!(!keeper.reclaim());

        drop((first_permit, second_permit));
        let (_third, third) = socket(&sockets);
        let _only = handle.hold(third, Box::new(|| false)).unwrap();
        assert!(!keeper.reclaim());
        assert_eq!(keeper.holding(), 0);
    }
}
