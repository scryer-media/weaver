// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    str::FromStr,
    time::Duration,
};

use crate::bencode::ByteBufOwned;
use crate::core::{compact_ip::CompactSocketAddr, hash_id::Id20};
use parking_lot::RwLock;
use rand::Rng;
use tokio::time::Instant;
use tracing::trace;

use crate::dht::bprotocol::{AnnouncePeer, Want};

// How long a peer announced to us is kept after its last announce. libtorrent's
// peers re-announce every 30 minutes and its store drops a peer one and a half
// of those intervals after its last announce.
pub(crate) const PEER_LIFETIME: Duration = Duration::from_secs(45 * 60);
// The peers kept for one info-hash: libtorrent's `dht_max_peers`.
pub(crate) const MAX_PEERS_PER_INFO_HASH: usize = 500;
// The info-hashes kept: libtorrent's `dht_max_torrents`.
pub(crate) const MAX_INFO_HASHES: usize = 2000;
// How often expired peers are swept out: libtorrent ticks its store every two
// minutes.
pub(crate) const PEER_PURGE_INTERVAL: Duration = Duration::from_secs(2 * 60);

struct StoredToken {
    token: [u8; 4],
    node_id: Id20,
    addr: SocketAddr,
}

struct StoredPeer {
    addr: SocketAddr,
    announced: Instant,
}

impl StoredPeer {
    fn is_live(&self, now: Instant) -> bool {
        now < self.announced + PEER_LIFETIME
    }
}

// The peers announced to us, by info-hash. Lives in memory only: every entry is
// gone within PEER_LIFETIME anyway.
pub struct PeerStore {
    pub(crate) self_id: Id20,
    max_remembered_tokens: u32,
    max_distance: Id20,
    tokens: RwLock<VecDeque<StoredToken>>,
    swarms: RwLock<HashMap<Id20, Vec<StoredPeer>>>,
}

impl PeerStore {
    pub fn new(self_id: Id20) -> Self {
        Self {
            self_id,
            max_remembered_tokens: 1000,
            max_distance: Id20::from_str("00000fffffffffffffffffffffffffffffffffff").unwrap(),
            tokens: RwLock::new(VecDeque::new()),
            swarms: RwLock::new(HashMap::new()),
        }
    }

    pub fn gen_token_for(&self, node_id: Id20, addr: SocketAddr) -> [u8; 4] {
        let mut token = [0u8; 4];
        rand::rng().fill_bytes(&mut token);
        let mut tokens = self.tokens.write();
        tokens.push_back(StoredToken {
            token,
            addr,
            node_id,
        });
        if tokens.len() > self.max_remembered_tokens as usize {
            tokens.pop_front();
        }
        token
    }

    pub fn store_peer(&self, announce: &AnnouncePeer<ByteBufOwned>, mut addr: SocketAddr) -> bool {
        // If the info_hash in announce is too far away from us, don't store it.
        // If the token doesn't match, don't store it.
        // Otherwise, store it; when that is over a cap, the oldest goes.
        if announce.info_hash.distance(&self.self_id) > self.max_distance {
            trace!("peer store: info_hash too far to store");
            return false;
        }
        if !self.tokens.read().iter().any(|t| {
            t.token[..] == announce.token.as_ref()[..] && t.addr == addr && t.node_id == announce.id
        }) {
            trace!("peer store: can't find this token / addr combination");
            return false;
        }

        if announce.implied_port == 0 {
            addr.set_port(announce.port);
        }

        let now = Instant::now();
        let mut swarms = self.swarms.write();
        if !swarms.contains_key(&announce.info_hash) && swarms.len() >= MAX_INFO_HASHES {
            // The info-hash announced to least recently makes room.
            let stalest = swarms
                .iter()
                .min_by_key(|(_, peers)| peers.iter().map(|p| p.announced).max())
                .map(|(info_hash, _)| *info_hash);
            if let Some(stalest) = stalest {
                trace!(
                    "peer store: dropping {stalest:?} for {:?}",
                    announce.info_hash
                );
                swarms.remove(&stalest);
            }
        }
        let peers = swarms.entry(announce.info_hash).or_default();
        peers.retain(|p| p.is_live(now));
        if let Some(p) = peers.iter_mut().find(|p| p.addr == addr) {
            p.announced = now;
            return true;
        }
        if peers.len() >= MAX_PEERS_PER_INFO_HASH {
            // The peer announced least recently makes room.
            if let Some(oldest) = (0..peers.len()).min_by_key(|&i| peers[i].announced) {
                peers.swap_remove(oldest);
            }
        }
        peers.push(StoredPeer {
            addr,
            announced: now,
        });
        true
    }

    pub fn get_for_info_hash(&self, info_hash: Id20, want: Want) -> Vec<CompactSocketAddr> {
        let now = Instant::now();
        let swarms = self.swarms.read();
        let Some(peers) = swarms.get(&info_hash) else {
            return Vec::new();
        };
        peers
            .iter()
            .filter(|p| p.is_live(now))
            .filter(|p| {
                matches!(
                    (p.addr, want),
                    (SocketAddr::V6(..), Want::V6 | Want::Both)
                        | (SocketAddr::V4(..), Want::V4 | Want::Both)
                )
            })
            .map(|p| p.addr.into())
            .collect()
    }

    // Drop every peer past its lifetime, and every info-hash left with none.
    pub fn garbage_collect_peers(&self) {
        let now = Instant::now();
        self.swarms.write().retain(|_, peers| {
            peers.retain(|p| p.is_live(now));
            !peers.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, net::SocketAddr, time::Duration};

    use super::{MAX_INFO_HASHES, MAX_PEERS_PER_INFO_HASH, PEER_LIFETIME, PeerStore};
    use crate::bencode::ByteBufOwned;
    use crate::core::hash_id::Id20;
    use crate::dht::bprotocol::{AnnouncePeer, Want};

    fn store() -> PeerStore {
        PeerStore::new(Id20::new([0x11; 20]))
    }

    // An info-hash close enough to our id to be stored.
    fn near_info_hash(n: u32) -> Id20 {
        let mut id = [0x11; 20];
        id[16..].copy_from_slice(&n.to_be_bytes());
        Id20::new(id)
    }

    fn source(n: u32) -> SocketAddr {
        let [_, a, b, c] = n.to_be_bytes();
        SocketAddr::from(([10, a, b, c], 6881))
    }

    // `from` announces itself for `info_hash` with a token the store gave it.
    fn announce(store: &PeerStore, info_hash: Id20, from: SocketAddr) -> bool {
        let node_id = Id20::new([0x33; 20]);
        let token = store.gen_token_for(node_id, from);
        store.store_peer(
            &AnnouncePeer {
                id: node_id,
                implied_port: 1,
                info_hash,
                port: 0,
                token: ByteBufOwned::from(&token[..]),
            },
            from,
        )
    }

    fn stored(store: &PeerStore, info_hash: Id20) -> HashSet<SocketAddr> {
        store
            .get_for_info_hash(info_hash, Want::V4)
            .into_iter()
            .map(|addr| addr.0)
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn test_an_announced_peer_is_gone_after_its_lifetime() {
        let store = store();
        let info_hash = near_info_hash(1);
        assert!(announce(&store, info_hash, source(1)));

        tokio::time::advance(PEER_LIFETIME - Duration::from_secs(1)).await;
        store.garbage_collect_peers();
        assert_eq!(stored(&store, info_hash), HashSet::from([source(1)]));

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(stored(&store, info_hash).is_empty());
        store.garbage_collect_peers();
        assert!(store.swarms.read().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn test_the_peer_store_caps_hold_under_a_flood() {
        let store = store();
        let tick = Duration::from_millis(1);

        // More peers for one info-hash than it keeps: the earliest announced go.
        let crowded = near_info_hash(0);
        let flood = MAX_PEERS_PER_INFO_HASH as u32 + 100;
        for n in 0..flood {
            assert!(announce(&store, crowded, source(n)));
            tokio::time::advance(tick).await;
        }
        assert_eq!(stored(&store, crowded), (100..flood).map(source).collect());

        // More info-hashes than the store keeps: those announced to least
        // recently go, `crowded` first.
        let flood = MAX_INFO_HASHES as u32 + 10;
        for n in 1..=flood {
            assert!(announce(&store, near_info_hash(n), source(n)));
            tokio::time::advance(tick).await;
        }
        let kept: HashSet<Id20> = store.swarms.read().keys().copied().collect();
        assert_eq!(kept, (11..=flood).map(near_info_hash).collect());
    }
}
