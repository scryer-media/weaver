// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Who a torrent's peer table holds an entry for: a peer of the swarm, at
//! its address, or a web seed, by its id. What the peer model makes of each
//! kind is one table, which the peer list, bans, the IP filter, PEX and the
//! handshake read instead of asking which kind a peer is.

use std::net::{IpAddr, SocketAddr};

/// An entry of a torrent's peer table.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum PeerHandle {
    /// A peer of the swarm, at the address its connection is to or from.
    Addr(SocketAddr),
    /// A web seed of the torrent, by its id there; reached by URL, it has no
    /// address of its own.
    WebSeed(u32),
}

// A handle is a key of the peer table and of every set of senders, copied
// on the per-block path: no larger than the address it replaced.
const _: () = assert!(size_of::<PeerHandle>() == size_of::<SocketAddr>());

/// How the peer model treats a peer of a kind, read in place of asking
/// which kind it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Standing {
    /// On the peer list (libtorrent `peer_list`): it counts against
    /// `max_peerlist_size`, is ranked to be forgotten, crosses a pause, may
    /// be saved for a restart, and leaves a private torrent's list as its
    /// trackers answer. libtorrent keeps a web seed's peer in its
    /// `web_seed_t`, on no list.
    pub listed: bool,
    /// Its handshake sends a peer id, which its connection claims so one
    /// client reached twice keeps one connection.
    pub sends_peer_id: bool,
}

const SWARM: Standing = Standing {
    listed: true,
    sends_peer_id: true,
};

const WEB_SEED: Standing = Standing {
    listed: false,
    sends_peer_id: false,
};

impl PeerHandle {
    /// How the peer model treats this peer.
    pub const fn standing(self) -> &'static Standing {
        match self {
            Self::Addr(_) => &SWARM,
            Self::WebSeed(_) => &WEB_SEED,
        }
    }

    /// Where the peer is in the swarm: the address a ban, the IP filter and
    /// the count of connections by host read, and that the torrent's peer
    /// infos list. None for a web seed, which is reached by URL.
    pub const fn addr(self) -> Option<SocketAddr> {
        match self {
            Self::Addr(addr) => Some(addr),
            Self::WebSeed(_) => None,
        }
    }

    /// The host of [`Self::addr`].
    pub fn ip(self) -> Option<IpAddr> {
        self.addr().map(|addr| addr.ip())
    }

    /// See [`Standing::listed`].
    pub const fn is_listed(self) -> bool {
        self.standing().listed
    }

    /// See [`Standing::sends_peer_id`].
    pub const fn sends_peer_id(self) -> bool {
        self.standing().sends_peer_id
    }
}

impl From<SocketAddr> for PeerHandle {
    fn from(addr: SocketAddr) -> Self {
        Self::Addr(addr)
    }
}

// A swarm peer reads as its address, as logs wrote it before the handle
// was its own type; a web seed as what it is.
impl std::fmt::Display for PeerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Addr(addr) => addr.fmt(f),
            Self::WebSeed(id) => write!(f, "web seed {id}"),
        }
    }
}

impl std::fmt::Debug for PeerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv6Addr, SocketAddr};

    use super::*;

    // A web seed and a swarm peer never stand for each other, whatever the
    // address: before the handle had kinds, a peer in the IPv6 discard
    // prefix at port 0 was taken for web seed 0.
    #[test]
    fn a_web_seed_and_a_swarm_peer_never_collide() {
        let lookalike = SocketAddr::new(Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 0, 0).into(), 0);
        let addrs = [
            lookalike,
            SocketAddr::new(Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 1, 7).into(), 0),
            "1.2.3.4:0".parse().unwrap(),
            "0.0.0.0:0".parse().unwrap(),
            "[::]:0".parse().unwrap(),
            "[2001:db8::1]:6881".parse().unwrap(),
        ];
        for addr in addrs {
            let peer = PeerHandle::from(addr);
            assert_eq!(peer.addr(), Some(addr));
            assert!(peer.is_listed());
            for id in [0, 1, 7, 65536 + 7, u32::MAX] {
                let seed = PeerHandle::WebSeed(id);
                assert_ne!(peer, seed);
                assert_eq!(seed.addr(), None);
                assert!(!seed.is_listed());
            }
        }
        let set: std::collections::HashSet<_> = (0..4)
            .map(PeerHandle::WebSeed)
            .chain(addrs.map(PeerHandle::from))
            .collect();
        assert_eq!(set.len(), 4 + addrs.len());
    }

    // Each kind's standing: a swarm peer is listed and claims the peer id
    // it sends; a web seed is on no list and sends none.
    #[test]
    fn each_kinds_standing() {
        let peer = PeerHandle::from("1.2.3.4:6881".parse::<SocketAddr>().unwrap());
        assert_eq!(
            [*peer.standing(), *PeerHandle::WebSeed(3).standing()],
            [
                Standing {
                    listed: true,
                    sends_peer_id: true,
                },
                Standing {
                    listed: false,
                    sends_peer_id: false,
                },
            ]
        );
        assert!(peer.sends_peer_id());
        assert!(!PeerHandle::WebSeed(3).sends_peer_id());
    }

    // A swarm peer's address and host are where it is; a web seed has none.
    #[test]
    fn a_swarm_peer_is_at_its_address_and_a_web_seed_at_none() {
        let addr: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
        assert_eq!(PeerHandle::from(addr).addr(), Some(addr));
        assert_eq!(PeerHandle::from(addr).ip(), Some(addr.ip()));
        assert_eq!(PeerHandle::WebSeed(0).ip(), None);
    }

    // A swarm peer reads as its address, as logs wrote it; a web seed as
    // one, not as an address.
    #[test]
    fn a_handle_reads_as_what_it_is() {
        let addr: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        assert_eq!(PeerHandle::from(addr).to_string(), addr.to_string());
        assert_eq!(format!("{:?}", PeerHandle::from(addr)), format!("{addr:?}"));
        assert_eq!(PeerHandle::WebSeed(2).to_string(), "web seed 2");
        assert_eq!(format!("{:?}", PeerHandle::WebSeed(2)), "web seed 2");
    }
}
