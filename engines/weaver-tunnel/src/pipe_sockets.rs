// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The share of a WireGuard tunnel's sockets each kind of traffic may hold.
//
// A tunnel holds a fixed number of sockets. Usenet connections, torrent
// peers over TCP and everything else (trackers, web seeds, feeds, probes,
// datagram sockets) each get a share of them, so one kind can never take the
// sockets another needs. Every stream or socket the session hands out holds
// one permit of its share until it is dropped.
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use super::Purpose;

// Which share a socket counts against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SocketClass {
    Nntp,
    // Torrent peers over TCP. uTP peers ride the session's datagram socket
    // and take nothing of this.
    Torrent,
    Reserve,
}

impl SocketClass {
    pub fn of(purpose: &Purpose) -> Self {
        match purpose {
            Purpose::Nntp { .. } => Self::Nntp,
            Purpose::Torrent => Self::Torrent,
            // A probe may test a server before it is saved or turned on,
            // when its route counts no connection on the tunnel yet.
            Purpose::NntpProbe { .. }
            | Purpose::TorrentHttp
            | Purpose::ProxyEndpoint { .. }
            | Purpose::Feed { .. }
            | Purpose::Outbound
            | Purpose::Probe
            | Purpose::Dns => Self::Reserve,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nntp => "usenet",
            Self::Torrent => "torrent TCP",
            Self::Reserve => "reserve",
        }
    }
}

// How many sockets each class may hold at once.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SocketShares {
    pub nntp: usize,
    pub torrent: usize,
    pub reserve: usize,
}

impl SocketShares {
    pub fn of(&self, class: SocketClass) -> usize {
        match class {
            SocketClass::Nntp => self.nntp,
            SocketClass::Torrent => self.torrent,
            SocketClass::Reserve => self.reserve,
        }
    }
}

struct Share {
    permits: Arc<Semaphore>,
    // The share's size and the permits still to be taken back from sockets
    // that were busy when it shrank.
    size: Mutex<(usize, usize)>,
    freed: Arc<Notify>,
}

impl Share {
    fn new(size: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(size)),
            size: Mutex::new((size, 0)),
            freed: Arc::new(Notify::new()),
        }
    }

    // Grow by handing out permits, first cancelling any still owed. Shrink
    // by taking back the free permits now and the busy ones as they end.
    fn resize(&self, size: usize) {
        let mut state = self.size.lock().expect("socket share");
        let (current, owed) = &mut *state;
        if size >= *current {
            let mut grow = size - *current;
            let cancelled = grow.min(*owed);
            *owed -= cancelled;
            grow -= cancelled;
            self.permits.add_permits(grow);
        } else {
            let shrink = *current - size;
            let taken = self.permits.forget_permits(shrink);
            *owed += shrink - taken;
        }
        *current = size;
        drop(state);
        self.freed.notify_waiters();
    }
}

// The permit one socket holds of its share.
pub struct SocketPermit {
    share: Arc<Share>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for SocketPermit {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        {
            let mut state = self.share.size.lock().expect("socket share");
            if state.1 > 0 {
                state.1 -= 1;
                permit.forget();
                return;
            }
        }
        drop(permit);
        self.share.freed.notify_waiters();
    }
}

// A session's shares. A session without them, any but a WireGuard one,
// holds its sockets to no share at all.
#[derive(Default)]
pub struct SessionSockets {
    shares: Option<[Arc<Share>; 3]>,
}

impl SessionSockets {
    pub fn new(shares: SocketShares) -> Self {
        Self {
            shares: Some([
                Arc::new(Share::new(shares.nntp)),
                Arc::new(Share::new(shares.torrent)),
                Arc::new(Share::new(shares.reserve)),
            ]),
        }
    }

    fn share(&self, class: SocketClass) -> Option<&Arc<Share>> {
        let shares = self.shares.as_ref()?;
        Some(match class {
            SocketClass::Nntp => &shares[0],
            SocketClass::Torrent => &shares[1],
            SocketClass::Reserve => &shares[2],
        })
    }

    // Resize every share. Sockets already open stay open; a share that
    // shrank below them takes no new one until enough have ended.
    pub fn set_shares(&self, shares: SocketShares) {
        for class in [
            SocketClass::Nntp,
            SocketClass::Torrent,
            SocketClass::Reserve,
        ] {
            if let Some(share) = self.share(class) {
                share.resize(shares.of(class));
            }
        }
    }

    // The shares in force, when the session has them.
    pub fn shares(&self) -> Option<SocketShares> {
        let size = |class| {
            self.share(class)
                .map_or(0, |share| share.size.lock().expect("socket share").0)
        };
        self.shares.as_ref()?;
        Some(SocketShares {
            nntp: size(SocketClass::Nntp),
            torrent: size(SocketClass::Torrent),
            reserve: size(SocketClass::Reserve),
        })
    }

    // Whether `class` has a socket free now.
    pub fn has_room(&self, class: SocketClass) -> bool {
        self.share(class)
            .is_none_or(|share| share.permits.available_permits() > 0)
    }

    // A permit of `class`'s share, or the signal that one is freed when the
    // share is full. A session without shares hands out no permits.
    pub fn take(&self, class: SocketClass) -> Result<Option<SocketPermit>, Arc<Notify>> {
        let Some(share) = self.share(class) else {
            return Ok(None);
        };
        match share.permits.clone().try_acquire_owned() {
            Ok(permit) => Ok(Some(SocketPermit {
                share: Arc::clone(share),
                permit: Some(permit),
            })),
            Err(_) => Err(Arc::clone(&share.freed)),
        }
    }

    // Sockets of `class` open now.
    pub fn held(&self, class: SocketClass) -> usize {
        self.share(class).map_or(0, |share| {
            let (size, owed) = *share.size.lock().expect("socket share");
            (size + owed).saturating_sub(share.permits.available_permits())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHARES: SocketShares = SocketShares {
        nntp: 2,
        torrent: 1,
        reserve: 1,
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
        assert!(sockets.has_room(SocketClass::Reserve));
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

    #[test]
    fn a_session_without_shares_takes_no_permit() {
        let sockets = SessionSockets::default();
        assert!(sockets.take(SocketClass::Torrent).unwrap().is_none());
        assert!(sockets.has_room(SocketClass::Torrent));
        assert_eq!(sockets.shares(), None);
    }
}
