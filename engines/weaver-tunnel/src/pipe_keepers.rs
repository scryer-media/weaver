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
// - when a session's socket share is short, before the share refuses or
//   queues a claim (`SessionSockets`), so nothing kept ever makes other
//   work fail or wait for a socket.
//
// A kept thing remembers the stage it was dialled through by its keeper, and
// is handed back only to a dial through that same live stage.
//
// A keeper costs nothing until something is first kept through it: its
// holders are made on the first `handle`, which only a DNS lookup asks for,
// so a dial of any other traffic allocates nothing for it.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

use super::SocketClass;

type Close = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct Holders {
    next: u64,
    // By registration, oldest first, so the share reclaims the oldest.
    by_id: BTreeMap<u64, (SocketClass, Close)>,
    // Set, under the lock, once the keeper is dropped: nothing is held
    // after, even by a holder that reached the keeper before it went.
    closed: bool,
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
        KeeperRef(Arc::downgrade(self.holders.get_or_init(Arc::default)))
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
            std::mem::take(&mut held.by_id)
        };
        // Closed outside the lock: closing takes the kept thing's own lock,
        // and its registrations come back to this one.
        for (_, close) in closing.into_values() {
            close();
        }
    }

    // Whether something kept holds a socket of a class `wanted` accepts,
    // which `reclaim` would close. Closes nothing.
    pub(crate) fn could_reclaim(&self, wanted: impl Fn(SocketClass) -> bool) -> bool {
        self.holders.get().is_some_and(|holders| {
            lock(holders)
                .by_id
                .values()
                .any(|(class, _)| wanted(*class))
        })
    }

    // Closes the oldest thing kept that holds a socket of a class `wanted`
    // accepts, and says whether there was one.
    pub(crate) fn reclaim(&self, wanted: impl Fn(SocketClass) -> bool) -> bool {
        let Some(holders) = self.holders.get() else {
            return false;
        };
        let closing = {
            let mut holders = lock(holders);
            let found = holders
                .by_id
                .iter()
                .find(|(_, (class, _))| wanted(*class))
                .map(|(id, _)| *id);
            found.and_then(|id| holders.by_id.remove(&id))
        };
        match closing {
            Some((_, close)) => {
                close();
                true
            }
            None => false,
        }
    }

    // How many things are kept through this stage now.
    #[cfg(test)]
    pub(crate) fn holding(&self) -> usize {
        self.holders
            .get()
            .map_or(0, |holders| lock(holders).by_id.len())
    }
}

impl Drop for Keeper {
    fn drop(&mut self) {
        self.cut_all(true);
    }
}

// A keeper, held weakly: it names the stage instance something was kept
// through, and registers what is kept with it while it lives.
#[derive(Clone)]
pub struct KeeperRef(Weak<Mutex<Holders>>);

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

    // Registers `close` to run when this keeper cuts what it keeps, or
    // reclaims the socket of `class` it holds. None when the keeper is gone,
    // or going: nothing may be kept through it, and the caller closes what
    // it meant to keep.
    pub(crate) fn hold(&self, class: SocketClass, close: Close) -> Option<Registration> {
        let holders = self.0.upgrade()?;
        let mut held = lock(&holders);
        if held.closed {
            return None;
        }
        held.next += 1;
        let id = held.next;
        held.by_id.insert(id, (class, close));
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
    use std::sync::atomic::{AtomicBool, Ordering};

    fn closer(closed: &Arc<AtomicBool>) -> Close {
        let closed = closed.clone();
        Box::new(move || closed.store(true, Ordering::Relaxed))
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
        let _held = handle.hold(SocketClass::ReserveCore, closer(&before));
        drop(keeper);
        assert!(before.load(Ordering::Relaxed));
        let after = Arc::new(AtomicBool::new(false));
        assert!(
            handle
                .hold(SocketClass::ReserveCore, closer(&after))
                .is_none()
        );
        assert!(!handle.is(&handle));
        assert!(lock(&reached).by_id.is_empty());
    }

    // A cut closes what is kept, and the stage may keep again after.
    #[test]
    fn a_cut_closes_what_is_kept_and_the_stage_keeps_on() {
        let keeper = Keeper::default();
        let handle = keeper.handle();
        let closed = Arc::new(AtomicBool::new(false));
        let _held = handle.hold(SocketClass::ReserveCore, closer(&closed));
        keeper.cut();
        assert!(closed.load(Ordering::Relaxed));
        assert_eq!(keeper.holding(), 0);
        let again = Arc::new(AtomicBool::new(false));
        let _again = handle
            .hold(SocketClass::ReserveCore, closer(&again))
            .unwrap();
        assert_eq!(keeper.holding(), 1);
        assert!(handle.is(&keeper.handle()));
    }

    // A keeper nothing was kept through holds nothing, and dropping it does
    // nothing, outside a runtime too.
    #[test]
    fn a_keeper_never_asked_for_costs_nothing() {
        let keeper = Keeper::default();
        assert!(keeper.holders.get().is_none());
        assert!(!keeper.reclaim(|_| true));
        assert!(!keeper.could_reclaim(|_| true));
        keeper.cut();
        assert!(keeper.holders.get().is_none());
        drop(keeper);
    }
}
