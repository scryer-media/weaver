// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    time::Duration,
};

use crate::bencode::ByteBufOwned;
use crate::core::{compact_ip::CompactSocketAddr, hash_id::Id20};
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use tokio::time::Instant;
use tracing::trace;

use crate::dht::bprotocol::{AnnouncePeer, Want};
use crate::sha1w::{ISha1, Sha1};

// How long a peer announced to us is kept after its last announce. libtorrent's
// peers re-announce every 30 minutes and its store drops a peer one and a half
// of those intervals after its last announce.
pub(crate) const PEER_LIFETIME: Duration = Duration::from_secs(45 * 60);
// The peers kept for one info-hash: libtorrent's `dht_max_peers`.
pub(crate) const MAX_PEERS_PER_INFO_HASH: usize = 500;
// The most IPv4 peers one get_peers reply carries: libtorrent's
// `dht_max_peers_reply` (settings_pack.cpp:389, v2.0.15).
pub(crate) const MAX_PEERS_REPLY: usize = 100;
// The most IPv6 peers one reply carries: a quarter, an IPv6 address being
// four times the size (dht_storage.cpp:254-259, v2.0.15).
pub(crate) const MAX_PEERS_REPLY_V6: usize = MAX_PEERS_REPLY / 4;
// The info-hashes kept: libtorrent's `dht_max_torrents`.
pub(crate) const MAX_INFO_HASHES: usize = 2000;
// How often expired peers are swept out: libtorrent ticks its store every two
// minutes.
pub(crate) const PEER_PURGE_INTERVAL: Duration = Duration::from_secs(2 * 60);
// How often the secret write tokens are derived from is replaced: libtorrent's
// `dht_tracker` key refresh, every five minutes.
pub(crate) const WRITE_KEY_INTERVAL: Duration = Duration::from_secs(5 * 60);

// The secrets write tokens are derived from: the current one, and the one it
// replaced, so a token lives one to two intervals (libtorrent `node::m_secret`,
// `node::new_write_key`).
struct WriteKeys {
    current: [u8; 4],
    previous: [u8; 4],
    replaced: Instant,
}

impl WriteKeys {
    fn new(now: Instant) -> Self {
        Self {
            current: random_key(),
            previous: random_key(),
            replaced: now,
        }
    }

    // The two secrets as of `now`: one replaced for each interval gone by
    // since the last time.
    fn at(&mut self, now: Instant) -> [[u8; 4]; 2] {
        if now >= self.replaced + 2 * WRITE_KEY_INTERVAL {
            // Both have lapsed, and with them every token derived from them.
            *self = Self::new(now);
        } else if now >= self.replaced + WRITE_KEY_INTERVAL {
            self.previous = self.current;
            self.current = random_key();
            self.replaced += WRITE_KEY_INTERVAL;
        }
        [self.current, self.previous]
    }
}

fn random_key() -> [u8; 4] {
    let mut key = [0u8; 4];
    rand::rng().fill_bytes(&mut key);
    key
}

// The token the requester at `ip` presents to announce for `info_hash`: the
// first four bytes of a hash of its address, the secret and the info-hash
// (libtorrent `node::generate_token`).
fn token(ip: IpAddr, key: &[u8; 4], info_hash: &Id20) -> [u8; 4] {
    let mut h = Sha1::new();
    match ip {
        IpAddr::V4(ip) => h.update(&ip.octets()),
        IpAddr::V6(ip) => h.update(&ip.octets()),
    }
    h.update(key);
    h.update(&info_hash.0);
    let hash = h.finish();
    [hash[0], hash[1], hash[2], hash[3]]
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
    max_distance: Id20,
    write_keys: Mutex<WriteKeys>,
    swarms: RwLock<HashMap<Id20, Vec<StoredPeer>>>,
}

impl PeerStore {
    pub fn new(self_id: Id20) -> Self {
        Self {
            self_id,
            max_distance: Id20::from_str("00000fffffffffffffffffffffffffffffffffff").unwrap(),
            write_keys: Mutex::new(WriteKeys::new(Instant::now())),
            swarms: RwLock::new(HashMap::new()),
        }
    }

    // The token the requester at `addr` presents to announce for
    // `info_hash`. Nothing is kept: it is derived again to check it.
    pub fn gen_token_for(&self, info_hash: Id20, addr: SocketAddr) -> [u8; 4] {
        let [current, _] = self.write_keys.lock().at(Instant::now());
        token(addr.ip(), &current, &info_hash)
    }

    // Whether `presented` is a token we gave the requester at `addr`'s IP for
    // `info_hash`, from either secret (libtorrent `node::verify_token`).
    pub(crate) fn verify_token(&self, presented: &[u8], info_hash: Id20, addr: SocketAddr) -> bool {
        let keys = self.write_keys.lock().at(Instant::now());
        keys.iter()
            .any(|key| token(addr.ip(), key, &info_hash)[..] == *presented)
    }

    pub fn store_peer(&self, announce: &AnnouncePeer<ByteBufOwned>, mut addr: SocketAddr) -> bool {
        // If the info_hash in announce is too far away from us, don't store it.
        // If the token doesn't match, don't store it.
        // Otherwise, store it; when that is over a cap, the oldest goes.
        if announce.info_hash.distance(&self.self_id) > self.max_distance {
            trace!("peer store: info_hash too far to store");
            return false;
        }
        if !self.verify_token(announce.token.as_ref(), announce.info_hash, addr) {
            trace!("peer store: not a token we gave this address for this info_hash");
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

    /// The peers a get_peers reply carries: of the stored live peers of
    /// each family `want` asks for, a uniform random sample of at most
    /// [`MAX_PEERS_REPLY`] IPv4 and [`MAX_PEERS_REPLY_V6`] IPv6 (libtorrent
    /// `dht_default_storage::get_peers`, dht_storage.cpp:223-283, v2.0.15).
    pub fn get_for_info_hash(&self, info_hash: Id20, want: Want) -> Vec<CompactSocketAddr> {
        use rand::seq::IteratorRandom;
        let now = Instant::now();
        let swarms = self.swarms.read();
        let Some(peers) = swarms.get(&info_hash) else {
            return Vec::new();
        };
        let mut rng = rand::rng();
        let mut family = |v6: bool, most: usize| {
            peers
                .iter()
                .filter(|p| p.is_live(now) && p.addr.is_ipv6() == v6)
                .map(|p| CompactSocketAddr::from(p.addr))
                .sample(&mut rng, most)
        };
        let mut out = Vec::new();
        if matches!(want, Want::V4 | Want::Both) {
            out.extend(family(false, MAX_PEERS_REPLY));
        }
        if matches!(want, Want::V6 | Want::Both) {
            out.extend(family(true, MAX_PEERS_REPLY_V6));
        }
        out
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
        let token = store.gen_token_for(info_hash, from);
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

    // Every live peer the store holds for `info_hash`.
    fn stored(store: &PeerStore, info_hash: Id20) -> HashSet<SocketAddr> {
        let now = tokio::time::Instant::now();
        store
            .swarms
            .read()
            .get(&info_hash)
            .into_iter()
            .flatten()
            .filter(|p| p.is_live(now))
            .map(|p| p.addr)
            .collect()
    }

    // libtorrent `dht_default_storage::get_peers`: a reply carries at most
    // 100 IPv4 and 25 IPv6 peers, each a random pick of the live ones.
    #[tokio::test(start_paused = true)]
    async fn test_a_reply_carries_a_random_sample_of_at_most_100_peers() {
        use super::{MAX_PEERS_REPLY, MAX_PEERS_REPLY_V6};
        let store = store();
        let info_hash = near_info_hash(7);
        for n in 0..300 {
            assert!(announce(&store, info_hash, source(n)));
            let v6 = SocketAddr::from(([0xfd00, 0, 0, 0, 0, 0, 0, u16::try_from(n).unwrap() + 1], 6881));
            assert!(announce(&store, info_hash, v6));
        }
        let all = stored(&store, info_hash);
        assert_eq!(all.len(), 500);
        let reply = |want| -> Vec<SocketAddr> {
            store
                .get_for_info_hash(info_hash, want)
                .into_iter()
                .map(|addr| addr.0)
                .collect()
        };
        let v4 = reply(Want::V4);
        assert_eq!(v4.len(), MAX_PEERS_REPLY);
        assert!(v4.iter().all(|a| a.is_ipv4() && all.contains(a)));
        assert_eq!(v4.iter().collect::<HashSet<_>>().len(), v4.len());
        let v6 = reply(Want::V6);
        assert_eq!(v6.len(), MAX_PEERS_REPLY_V6);
        assert!(v6.iter().all(|a| a.is_ipv6() && all.contains(a)));
        let both = reply(Want::Both);
        assert_eq!(both.len(), MAX_PEERS_REPLY + MAX_PEERS_REPLY_V6);
        // The sample is drawn anew each time: over many replies every
        // stored IPv4 peer is handed out, not the first hundred alone.
        let mut seen = HashSet::new();
        while seen.len() < all.iter().filter(|a| a.is_ipv4()).count() {
            seen.extend(reply(Want::V4));
        }
        // A swarm smaller than the cap is handed out whole.
        let small = near_info_hash(8);
        for n in 0..5 {
            assert!(announce(&store, small, source(n)));
        }
        assert_eq!(reply_len(&store, small), 5);
    }

    fn reply_len(store: &PeerStore, info_hash: Id20) -> usize {
        store.get_for_info_hash(info_hash, Want::V4).len()
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
        let flood = u32::try_from(MAX_PEERS_PER_INFO_HASH).unwrap() + 100;
        for n in 0..flood {
            assert!(announce(&store, crowded, source(n)));
            tokio::time::advance(tick).await;
        }
        assert_eq!(stored(&store, crowded), (100..flood).map(source).collect());

        // More info-hashes than the store keeps: those announced to least
        // recently go, `crowded` first.
        let flood = u32::try_from(MAX_INFO_HASHES).unwrap() + 10;
        for n in 1..=flood {
            assert!(announce(&store, near_info_hash(n), source(n)));
            tokio::time::advance(tick).await;
        }
        let kept: HashSet<Id20> = store.swarms.read().keys().copied().collect();
        assert_eq!(kept, (11..=flood).map(near_info_hash).collect());
    }

    // libtorrent `node::generate_token` / `verify_token` / `new_write_key`:
    // a token is good from the requester's IP for the info-hash it was given
    // for, under the secret it was made with or the one after it, and nothing
    // is kept to check it.
    #[tokio::test(start_paused = true)]
    async fn test_a_write_token_is_tied_to_the_ip_and_info_hash_and_lives_two_key_intervals() {
        use super::WRITE_KEY_INTERVAL;

        let store = store();
        let info_hash = near_info_hash(1);
        let from = source(1);
        let token = store.gen_token_for(info_hash, from);
        let good = |info_hash, addr| store.verify_token(&token, info_hash, addr);

        let other_port = SocketAddr::new(from.ip(), from.port() + 1);
        assert!(good(info_hash, from));
        assert!(good(info_hash, other_port));
        assert!(!good(info_hash, source(2)));
        assert!(!good(near_info_hash(2), from));

        // Any number of tokens handed out leaves the first one good.
        for n in 2..2000 {
            store.gen_token_for(info_hash, source(n));
        }
        assert!(good(info_hash, from));

        tokio::time::advance(WRITE_KEY_INTERVAL).await;
        assert!(good(info_hash, from));
        tokio::time::advance(WRITE_KEY_INTERVAL).await;
        assert!(!good(info_hash, from));
        assert!(!store.store_peer(
            &AnnouncePeer {
                id: Id20::new([0x33; 20]),
                implied_port: 1,
                info_hash,
                port: 0,
                token: ByteBufOwned::from(&token[..]),
            },
            from,
        ));
        // A fresh one is good again.
        assert!(announce(&store, info_hash, from));
    }
}
