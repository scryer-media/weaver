// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The line of lanes waiting for room on one server's full network route.
//
// A route that is up but full refuses a dial with the slots that ring as its
// sockets free. A lane refused that way waits here instead of dialling
// again. A ring lets one lane go: the first in line. Its turn ends once the
// route hands it a socket (its TLS and login wait on the server, not the
// route), or when its dial ends without one. The route is then asked whether
// it has room: while it has, the next lane goes; once it is full, the line
// waits for the next ring. A lane refused again keeps the head of the line,
// and the next goes only on a ring. A full route is never redialled for the
// line, and a freed socket starts exactly one dial.
//
// The slots are heard before the route is asked, so a socket freed between
// the refusal and the wait is not missed. Nothing here polls on a timer.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

use crate::route_dialer::RouteDialer;

// A place in the line: waiting, let go to dial, turned away because the
// line closed, or left.
const WAITING: u8 = 0;
const GOING: u8 = 1;
const CLOSED: u8 = 2;
const LEFT: u8 = 3;

#[derive(Default)]
struct Place {
    state: AtomicU8,
    told: Notify,
}

#[derive(Default)]
struct Line {
    waiting: VecDeque<Arc<Place>>,
    // A lane was let go and its dial has not ended.
    going: bool,
    // The task hearing the route's slots for the line. It hears them while
    // a lane let go dials too, so a socket freed then is not missed.
    hearing: Option<Hearing>,
    closed: bool,
    // A slot rang while a lane let go was dialling.
    rang: bool,
    // The lane let go was refused again and is back at the head: its turn
    // ending lets the next lane go only on a ring, not on the route's word
    // that it has room, which the refusal just contradicted.
    refused: bool,
}

// The task hearing the line's slots, and the slots it hears.
struct Hearing {
    task: tokio::task::AbortHandle,
    slots: Vec<Arc<Notify>>,
}

impl Hearing {
    fn abort(self) {
        self.task.abort();
    }
}

impl Drop for Line {
    fn drop(&mut self) {
        if let Some(hearing) = self.hearing.take() {
            hearing.abort();
        }
    }
}

pub(crate) struct RouteLine {
    dialer: Arc<RouteDialer>,
    line: Mutex<Line>,
    // How many lanes wait, published as it changes.
    waiting: tokio::sync::watch::Sender<usize>,
}

impl RouteLine {
    pub(crate) fn new(dialer: Arc<RouteDialer>) -> Arc<Self> {
        Arc::new(Self {
            dialer,
            line: Mutex::default(),
            waiting: tokio::sync::watch::channel(0).0,
        })
    }

    fn line(&self) -> MutexGuard<'_, Line> {
        self.line
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn publish(&self, line: &Line) {
        let waiting = line.waiting.len();
        self.waiting.send_if_modified(|count| {
            let changed = *count != waiting;
            *count = waiting;
            changed
        });
    }

    // Join the line after a refusal that named `slots`. `first` puts the
    // lane at the head: it was let go, and its dial was refused again.
    // `None` when the refusal names no slot: nothing would ring for it.
    pub(crate) fn join(
        self: &Arc<Self>,
        slots: Vec<Arc<Notify>>,
        first: bool,
    ) -> Option<RouteTurn> {
        if slots.is_empty() {
            return None;
        }
        let place = Arc::new(Place::default());
        {
            let mut line = self.line();
            if line.closed {
                return None;
            }
            if first {
                line.waiting.push_front(Arc::clone(&place));
                line.refused = true;
            } else {
                line.waiting.push_back(Arc::clone(&place));
            }
            self.publish(&line);
        }
        self.hear(slots, true);
        Some(RouteTurn {
            line: Arc::clone(self),
            place,
        })
    }

    // Hear `slots`, and whatever else the route names, then ask the route
    // for room: room lets the next lane go when `trust_room`, and otherwise
    // a task is left hearing the slots for the line.
    //
    // The slots the line already hears are heard again with them. A slot it
    // did not hear may have rung before it was heard: if the route then has
    // room while a lane let go is dialling, that counts as a ring, kept for
    // the dial's end. A refusal on slots already heard is not, so a route
    // that says it has room and refuses again does not spin the line.
    fn hear(self: &Arc<Self>, mut slots: Vec<Arc<Notify>>, trust_room: bool) {
        let known = {
            let line = self.line();
            if line.waiting.is_empty() {
                return;
            }
            line.hearing
                .as_ref()
                .map(|hearing| hearing.slots.clone())
                .unwrap_or_default()
        };
        let mut unheard = false;
        slots.extend(known.iter().cloned());
        let mut heard: Vec<Pin<Box<OwnedNotified>>> = Vec::new();
        let mut kept: Vec<Arc<Notify>> = Vec::new();
        loop {
            for slot in slots.drain(..) {
                if kept.iter().any(|seen| Arc::ptr_eq(seen, &slot)) {
                    continue;
                }
                let mut notified = Box::pin(Arc::clone(&slot).notified_owned());
                notified.as_mut().enable();
                heard.push(notified);
                unheard |= !known.iter().any(|seen| Arc::ptr_eq(seen, &slot));
                kept.push(slot);
            }
            match self.dialer.room() {
                Ok(()) if trust_room && self.let_one_go() => return,
                Ok(()) => {
                    if unheard {
                        let mut line = self.line();
                        line.rang |= line.going;
                    }
                    break;
                }
                Err(more)
                    if more
                        .iter()
                        .all(|slot| kept.iter().any(|seen| Arc::ptr_eq(seen, slot))) =>
                {
                    break;
                }
                Err(more) => slots = more,
            }
        }
        if heard.is_empty() {
            // Nothing can ring for the line, so no wait is safe: the lanes
            // leave it and take the refusal.
            self.turn_away(&mut self.line());
            return;
        }
        let line = Arc::downgrade(self);
        let hearing_slots = kept.clone();
        let task = self.dialer.runtime.spawn(async move {
            loop {
                std::future::poll_fn(|cx| {
                    if heard
                        .iter_mut()
                        .any(|slot| slot.as_mut().poll(cx).is_ready())
                    {
                        std::task::Poll::Ready(())
                    } else {
                        std::task::Poll::Pending
                    }
                })
                .await;
                // Heard again before the ring is acted on, so the next one
                // is not missed.
                heard = kept
                    .iter()
                    .map(|slot| {
                        let mut notified = Box::pin(Arc::clone(slot).notified_owned());
                        notified.as_mut().enable();
                        notified
                    })
                    .collect();
                let Some(line) = line.upgrade() else {
                    return;
                };
                line.rung();
            }
        });
        let mut line = self.line();
        if line.closed {
            task.abort();
        } else if let Some(previous) = line.hearing.replace(Hearing {
            task: task.abort_handle(),
            slots: hearing_slots,
        }) {
            previous.abort();
        }
    }

    // A slot rang: the first lane still waiting goes, or, while one is
    // dialling, the ring is kept for its dial's end.
    fn rung(&self) {
        let going = {
            let mut line = self.line();
            line.rang |= line.going;
            line.going
        };
        if !going {
            self.let_one_go();
        }
    }

    // The first lane still waiting goes, unless one is already dialling,
    // whose dial's end asks again. Whether one went.
    fn let_one_go(&self) -> bool {
        let mut line = self.line();
        if line.going || line.closed {
            return false;
        }
        let mut went = false;
        while let Some(place) = line.waiting.pop_front() {
            if place
                .state
                .compare_exchange(WAITING, GOING, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                line.going = true;
                line.rang = false;
                line.refused = false;
                place.told.notify_one();
                went = true;
                break;
            }
        }
        self.publish(&line);
        went
    }

    // The dial of the lane let go ended. The next lane goes when the route
    // has room, unless the lane was refused again and no slot rang during
    // its dial: then the line waits for the next ring.
    fn dial_ended(self: &Arc<Self>) {
        let ask = {
            let mut line = self.line();
            line.going = false;
            let ask = line.rang || !line.refused;
            line.rang = false;
            line.refused = false;
            if line.waiting.is_empty()
                && let Some(hearing) = line.hearing.take()
            {
                hearing.abort();
            }
            ask
        };
        if ask {
            self.hear(Vec::new(), true);
        }
    }

    // The pool is going: every lane in line leaves it with its refusal, and
    // none joins again.
    pub(crate) fn close(&self) {
        let mut line = self.line();
        line.closed = true;
        self.turn_away(&mut line);
    }

    // Every lane in line leaves it with its refusal.
    fn turn_away(&self, line: &mut Line) {
        for place in line.waiting.drain(..) {
            if place
                .state
                .compare_exchange(WAITING, CLOSED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                place.told.notify_one();
            }
        }
        if let Some(hearing) = line.hearing.take() {
            hearing.abort();
        }
        self.publish(line);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<usize> {
        self.waiting.subscribe()
    }
}

// A lane's place in a route's line. Dropped, the lane leaves it: a lane
// that was let go ends its dial so, and the line asks the route again.
pub struct RouteTurn {
    line: Arc<RouteLine>,
    place: Arc<Place>,
}

impl RouteTurn {
    // Whether the lane may dial (`true`) or the line closed (`false`).
    async fn given(&self) -> bool {
        loop {
            match self.place.state.load(Ordering::Acquire) {
                GOING => return true,
                CLOSED | LEFT => return false,
                _ => self.place.told.notified().await,
            }
        }
    }

    // Wait on the lane's own thread until it may dial (`Ok(true)`), the
    // line closed (`Ok(false)`), or `other` is ready first (`Err`). Called
    // off the async runtime, never from a task on it.
    pub fn wait_blocking<T>(&self, other: impl Future<Output = T>) -> Result<bool, T> {
        self.line.dialer.runtime.block_on(async {
            tokio::select! {
                biased;
                given = self.given() => Ok(given),
                other = other => Err(other),
            }
        })
    }
}

thread_local! {
    // The turn a lane let go holds while it opens its connection on this
    // thread, given up once the route hands it a socket.
    static HELD: std::cell::RefCell<Option<RouteTurn>> = const { std::cell::RefCell::new(None) };
}

// Run `open` holding `turn` until the route hands this thread a socket
// (`routed`): the rest of opening the connection, its TLS and login, waits
// on the server, not the route, so the next lane need not wait for it.
// The turn comes back when no socket was handed over, for the lane to keep
// its place.
pub fn held_until_routed<T>(
    turn: Option<RouteTurn>,
    open: impl FnOnce() -> T,
) -> (T, Option<RouteTurn>) {
    if turn.is_none() {
        return (open(), None);
    }
    let previous = HELD.with(|held| held.replace(turn));
    let opened = open();
    let turn = HELD.with(|held| held.replace(previous));
    (opened, turn)
}

// The route handed this thread a socket: the turn it held is given up.
pub(crate) fn routed() {
    drop(HELD.with(|held| held.borrow_mut().take()));
}

impl Drop for RouteTurn {
    fn drop(&mut self) {
        match self.place.state.swap(LEFT, Ordering::AcqRel) {
            GOING => self.line.dial_ended(),
            WAITING => {
                let mut line = self.line.line();
                line.waiting
                    .retain(|place| !Arc::ptr_eq(place, &self.place));
                if line.waiting.is_empty()
                    && !line.going
                    && let Some(hearing) = line.hearing.take()
                {
                    hearing.abort();
                }
                self.line.publish(&line);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use weaver_tunnel::pipe::{DialError, Dialed, Dialer, Purpose, Target};

    // A route whose room the test sets: none, naming one slot, or room.
    struct Route {
        full_on: Mutex<Option<Arc<Notify>>>,
    }
    #[async_trait::async_trait]
    impl Dialer for Route {
        fn room(&self, _: &Purpose) -> Result<(), Vec<Arc<Notify>>> {
            match &*self.full_on.lock().unwrap() {
                Some(slot) => Err(vec![Arc::clone(slot)]),
                None => Ok(()),
            }
        }
        async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
            unreachable!("the line never dials")
        }
        fn budget(&self) -> Duration {
            Duration::from_secs(30)
        }
        fn describe(&self) -> String {
            "route fixture".into()
        }
    }

    fn line_over(route: Arc<Route>) -> Arc<RouteLine> {
        RouteLine::new(Arc::new(RouteDialer {
            inner: route,
            plan: None,
            runtime: tokio::runtime::Handle::current(),
            server: 1,
            egress_controls: Arc::new(crate::transfer::ServerTransferRegistry::new()),
        }))
    }

    fn state(turn: &RouteTurn) -> u8 {
        turn.place.state.load(Ordering::Acquire)
    }

    // A lane let go on one slot is refused again naming a slot the line did
    // not hear, which freed before it was heard. The free is not lost: the
    // lane's dial ending lets it go again. Refused again on that same slot
    // with the route still saying it has room, the line does not spin: the
    // next lane goes only on the slot's ring.
    #[tokio::test]
    async fn a_refusal_on_an_unheard_slot_that_already_freed_is_not_lost() {
        let socket = Arc::new(Notify::new());
        let budget = Arc::new(Notify::new());
        let route = Arc::new(Route {
            full_on: Mutex::new(Some(Arc::clone(&socket))),
        });
        let line = line_over(Arc::clone(&route));
        let first = line.join(vec![Arc::clone(&socket)], false).unwrap();
        let second = line.join(vec![Arc::clone(&socket)], false).unwrap();
        assert_eq!((state(&first), state(&second)), (WAITING, WAITING));

        // The socket frees: the first lane goes.
        *route.full_on.lock().unwrap() = None;
        socket.notify_waiters();
        assert!(first.given().await);
        assert_eq!(state(&second), WAITING);

        // Its dial is refused for the budget, whose permit is given back
        // before the line hears it: the route has room again.
        let again = line.join(vec![Arc::clone(&budget)], true).unwrap();
        drop(first);
        assert_eq!(state(&again), GOING, "the free before hearing is kept");
        assert_eq!(state(&second), WAITING);

        // Refused again on the budget, which the line now hears, with the
        // route still saying it has room: nobody goes until it rings.
        let third = line.join(vec![Arc::clone(&budget)], true).unwrap();
        drop(again);
        assert_eq!((state(&third), state(&second)), (WAITING, WAITING));
        budget.notify_waiters();
        assert!(third.given().await);
        assert_eq!(state(&second), WAITING);
    }
}
