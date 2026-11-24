// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A WireGuard tunnel's ask for a class of job to give back its sockets
//! there, and the classes that answer it, as the network runtime sees them.

use std::sync::Arc;

/// A WireGuard tunnel's wake for a class to give back its sockets there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TunnelGiveBack {
    /// The tunnel: its egress and the hops up to and including it, as a
    /// socket's `DialPath` names them.
    pub egress: u32,
    pub hops: Vec<u32>,
    /// The torrent sessions whose legs run over the tunnel.
    pub torrent_routes: Vec<super::session_key::RouteKey>,
    /// What the class owes the tunnel, read when it acts.
    pub owed: Owed,
}

impl TunnelGiveBack {
    /// Whether a socket dialled over `path` sits in the tunnel itself: on
    /// its egress, with the tunnel its last hop.
    pub fn holds(&self, path: &weaver_tunnel::pipe::DialPath) -> bool {
        self.rides(path.egress, &path.proxies)
    }

    /// Whether a socket on `egress` through `proxies` sits in the tunnel.
    pub fn rides(&self, egress: u32, proxies: &[u32]) -> bool {
        egress == self.egress && proxies == self.hops
    }
}

/// What a class owes a tunnel: the bytes it holds there past what it is
/// given while another class presses, read at the moment it is acted on.
/// A level, never an amount handed over once: an answer, or a connection
/// marked to close for one, reads it again when it acts.
#[derive(Clone)]
pub struct Owed {
    owed: Arc<dyn Fn() -> u64 + Send + Sync>,
    dry: Arc<dyn Fn() + Send + Sync>,
}

impl Owed {
    /// `owed` reads the level; `dry` tells the tunnel an answer found
    /// nothing it could give.
    pub fn new(
        owed: impl Fn() -> u64 + Send + Sync + 'static,
        dry: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            owed: Arc::new(owed),
            dry: Arc::new(dry),
        }
    }

    /// The bytes owed now.
    pub fn bytes(&self) -> u64 {
        (self.owed)()
    }

    /// Whether anything is owed now.
    pub fn stands(&self) -> bool {
        self.bytes() > 0
    }

    /// The answer found nothing it could give: the class is not woken again
    /// until its own holdings on the tunnel change.
    pub fn dry(&self) {
        (self.dry)()
    }

    /// Whether anything is owed, as a connection marked to close reads it.
    pub fn standing(&self) -> Arc<dyn Fn() -> bool + Send + Sync> {
        let owed = Arc::clone(&self.owed);
        Arc::new(move || owed() > 0)
    }
}

impl std::fmt::Debug for Owed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Owed")
    }
}

impl PartialEq for Owed {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.owed, &other.owed)
    }
}

impl Eq for Owed {}

/// The classes of job a tunnel's ask goes to, one per side of a session, as
/// the job layer seats them: the network runtime sits below the classes and
/// reaches them only through this.
pub trait GiveBackClasses: Send + Sync {
    /// Whether a class is seated on `side`.
    fn seated(&self, side: usize) -> bool;
    /// Hand `ask` to the class on `side`.
    fn give_back(&self, side: usize, ask: TunnelGiveBack);
}
