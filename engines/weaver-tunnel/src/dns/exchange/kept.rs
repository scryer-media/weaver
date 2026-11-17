// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The DNS over TLS and DNS over HTTPS connections one naming keeps between
// its lookups: at most one per server, wire, TLS name and HTTPS path. A
// connection is handed back only to the naming that dialled it, and only for
// a lookup through the same live stage instance it was dialled through, so
// it carries only that naming's lookups, on the route it was dialled on. A
// stage built anew, by a proxy edited, disabled or replaced, never takes a
// connection its predecessor dialled. A connection closes when no lookup
// takes it for `IDLE`, with the naming, when a stage it rides retires or is
// dropped, and when a socket share it holds a permit of runs short. Only
// that last is a reset, so the share has its bytes back at once; every other
// close ends the connection gracefully.
use super::{Tls, doh::Connection};
use crate::dns::DnsServer;
use crate::pipe::{KeeperRef, KeptRegistration, Reservation};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;
use tokio::time::Instant;

// How long a kept connection waits for the next lookup before it is closed.
// unbound waits 30 seconds for a query on a TCP connection before closing it
// (`tcp-idle-timeout`, default 30000 ms), and keeps its own persistent TCP
// connections to servers open for 60 (`tcp-reuse-timeout`, default 60000 ms;
// unbound.conf). Kept connections ride tunnel sockets, which are budgeted, so
// the shorter of the two.
pub(in crate::dns) const IDLE: Duration = Duration::from_secs(30);

// A connection one lookup left healthy, for the next to take.
pub(super) enum Link {
    Dot(Box<Tls>),
    Doh(Box<Connection>),
}

impl Link {
    // The bytes the connection's sockets reserved, by share.
    fn reserved(&self) -> &[Reservation] {
        match self {
            Self::Dot(tls) => &tls.get_ref().0.reserved,
            Self::Doh(connection) => connection.reserved(),
        }
    }
}

// The connections one naming keeps, by server. Each closes once `IDLE`
// passes without a lookup taking it. One sweep task per naming closes them,
// running only while something is kept, and holds the naming's connections
// weakly, so the naming closes every one it keeps when it goes. A lookup
// taking or keeping a connection starts no task while the sweep runs.
#[derive(Default)]
pub(in crate::dns) struct Kept {
    links: Arc<Mutex<Links>>,
}

#[derive(Default)]
struct Links {
    by_server: HashMap<DnsServer, Held>,
    // Tells one keeping of a server's connection from the next, so a stage
    // closes only the connection it was set for.
    next: u64,
    // Whether the sweep task runs.
    sweeping: bool,
}

struct Held {
    link: Option<Link>,
    tag: u64,
    // The stage instance the connection was dialled through.
    dialled: KeeperRef,
    // When it closes, unless a lookup takes it first.
    closes: Instant,
    // Its registration with the keeper of each stage it rides, withdrawn
    // as it is taken or closed.
    _rides: Vec<KeptRegistration>,
}

impl Kept {
    // The connection kept for `server`, if one is, it was dialled through
    // the stage `through` names, and its close time has not come: it is no
    // longer kept. One dialled through another stage instance, one whose
    // stage is gone, or one past its time (its sweep went with a runtime
    // before closing it) is closed instead. Closed outside the lock.
    pub(super) fn take(&self, server: &DnsServer, through: Option<&KeeperRef>) -> Option<Link> {
        let mut held = lock(&self.links).by_server.remove(server)?;
        let ours =
            through.is_some_and(|through| held.dialled.is(through)) && held.closes > Instant::now();
        if ours { held.link.take() } else { None }
    }

    // Keeps `link` for the next lookup of `server` through the stage that
    // `rides` names first, in place of one kept before, which closes. The
    // connection registers with the keeper of every stage in `rides`, and
    // closes when any of them cuts it, or when one whose share its socket
    // reserved bytes in reclaims it, at the bytes it reserved there. With no stage to tie it
    // to, a stage already gone, or outside a runtime, where there is no
    // timer to close it, it closes now.
    //
    // The sweep is spawned after the lock is let go: on a runtime shutting
    // down, tokio drops a new task at once, and the sweep's drop takes the
    // lock.
    pub(super) fn keep(&self, server: &DnsServer, link: Link, rides: &[KeeperRef]) {
        let Some(dialled) = rides.first() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut links = lock(&self.links);
        links.next += 1;
        let tag = links.next;
        let mut registered = Vec::with_capacity(rides.len());
        for keeper in rides {
            let closer = Box::new(close(Arc::downgrade(&self.links), server.clone(), tag));
            let socket = link
                .reserved()
                .iter()
                .find(|reserved| reserved.keeper.is(keeper))
                .map(|reserved| reserved.socket.clone());
            match keeper.hold(socket, closer) {
                Some(registration) => registered.push(registration),
                None => {
                    drop(links);
                    return;
                }
            }
        }
        let start = !std::mem::replace(&mut links.sweeping, true);
        let replaced = links.by_server.insert(
            server.clone(),
            Held {
                link: Some(link),
                tag,
                dialled: dialled.clone(),
                closes: Instant::now() + IDLE,
                _rides: registered,
            },
        );
        drop(links);
        drop(replaced);
        if start {
            runtime.spawn(sweep(Sweeping(Some(Arc::downgrade(&self.links)))));
        }
    }

    // How many connections are kept now.
    #[cfg(test)]
    pub(in crate::dns) fn len(&self) -> usize {
        lock(&self.links).by_server.len()
    }
}

// Closes each kept connection once its time comes, waking only when the
// next one's does, and ends once nothing is kept or the naming is gone. A
// connection kept while it sleeps closes after every one kept before it, so
// it never needs waking early.
async fn sweep(mut sweeping: Sweeping) {
    loop {
        let next = {
            let Some(links) = sweeping.0.as_ref().and_then(Weak::upgrade) else {
                return;
            };
            let now = Instant::now();
            let mut held = lock(&links);
            let due: Vec<DnsServer> = held
                .by_server
                .iter()
                .filter(|(_, kept)| kept.closes <= now)
                .map(|(server, _)| server.clone())
                .collect();
            // Closed outside the lock.
            let closing: Vec<Held> = due
                .iter()
                .filter_map(|server| held.by_server.remove(server))
                .collect();
            let next = held.by_server.values().map(|kept| kept.closes).min();
            if next.is_none() {
                held.sweeping = false;
                sweeping.0 = None;
            }
            drop(held);
            drop(closing);
            next
        };
        let Some(next) = next else {
            return;
        };
        tokio::time::sleep_until(next).await;
    }
}

// The running sweep. Dropped before it ends on its own, as with its
// runtime, it lets the next keep start another.
struct Sweeping(Option<Weak<Mutex<Links>>>);

impl Drop for Sweeping {
    fn drop(&mut self) {
        if let Some(links) = self.0.take().and_then(|links| links.upgrade()) {
            lock(&links).sweeping = false;
        }
    }
}

// Closes the connection kept for `server` under `tag`, if it is still kept,
// and says whether it was: one a lookup took first is in use, and closes
// nothing. The connection is dropped outside the lock.
fn close(links: Weak<Mutex<Links>>, server: DnsServer, tag: u64) -> impl FnOnce() -> bool + Send {
    move || {
        let Some(links) = links.upgrade() else {
            return false;
        };
        let mut held = lock(&links);
        if held
            .by_server
            .get(&server)
            .is_some_and(|kept| kept.tag == tag)
        {
            let closed = held.by_server.remove(&server);
            drop(held);
            drop(closed);
            true
        } else {
            false
        }
    }
}

fn lock(links: &Mutex<Links>) -> MutexGuard<'_, Links> {
    links.lock().unwrap_or_else(PoisonError::into_inner)
}
