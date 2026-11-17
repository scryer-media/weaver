// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

mod availability;
mod connections;
mod peer_list;

use std::time::Duration;

use super::*;
use crate::peer_protocol::extended::ut_pex::PexPeerInfo;
use crate::torrent_state::live::peer::{MAX_FAILCOUNT, MAX_PEX_PEERS, MIN_RECONNECT_TIME};

const TOTAL_PIECES: usize = 8;

fn full_bitfield() -> BF {
    BF::from_boxed_slice(vec![0xffu8; 1].into_boxed_slice())
}

fn empty_bitfield() -> BF {
    BF::from_boxed_slice(vec![0u8; 1].into_boxed_slice())
}

// The port the tests' incoming connections come to.
const OUR_PORT: u16 = 6881;

fn make_peer_states() -> PeerStates {
    PeerStates::founded_on(
        PeerList::default(),
        Default::default(),
        u32::try_from(TOTAL_PIECES).unwrap(),
    )
}

// Returns the peer address, keeping the receiving end of the peer channel alive.
fn add_live_peer(peers: &PeerStates, addr: &str) -> (SocketAddr, PeerRx) {
    let addr: SocketAddr = addr.parse().unwrap();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    peers
        .add_incoming(addr, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
        .unwrap();
    (addr, rx)
}

fn live_seeders(peers: &PeerStates) -> (u32, u32) {
    (
        peers.stats().live_seeders,
        peers.session_stats.snapshot().live_seeders,
    )
}

fn peer_addr(i: usize) -> SocketAddr {
    SocketAddr::from((
        [
            10,
            0,
            u8::try_from((i >> 8) & 0xff).unwrap(),
            u8::try_from(i & 0xff).unwrap(),
        ],
        6881,
    ))
}

fn state_of(peers: &PeerStates, addr: SocketAddr) -> Option<&'static str> {
    peers.with_peer(addr.into(), |peer| peer.get_state().name())
}

// Connects to `addr`, which must be due at `at`, and fails the connection
// at `at`.
fn fail_at(peers: &PeerStates, addr: SocketAddr, at: Instant) {
    peers
        .with_peer_mut(addr.into(), "test", |peer| {
            peer.connect_if_due(at, peers).expect("a due candidate");
            peers.connection_closed(peer, at, true, false);
        })
        .unwrap();
}

// `addrs`, highest BEP 40 rank first, the lowest address first among
// equal ranks, as candidates that tie on every earlier step are handed
// out.
fn by_peer_rank(peers: &PeerStates, mut addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    addrs.sort_by_key(|addr| (Reverse(peers.connect_order.now().peer_rank(*addr)), *addr));
    addrs
}

fn reconnect_wait(failcount: u32) -> Duration {
    MIN_RECONNECT_TIME * (failcount + 1)
}

// Fails `addr` MAX_FAILCOUNT times in a row from `t0`, each attempt as soon
// as it is due; returns the time of the last failure.
fn fail_out(peers: &PeerStates, addr: SocketAddr, t0: Instant) -> Instant {
    let mut at = t0;
    for failcount in 0..MAX_FAILCOUNT {
        if failcount > 0 {
            at += reconnect_wait(failcount);
        }
        fail_at(peers, addr, at);
    }
    at
}

// Names `addr`, fails a connection to it at `at`, and lets it go: an
// erase candidate that failed once.
fn failed_once(peers: &PeerStates, addr: SocketAddr, at: Instant) {
    peers.add_peer(addr, PeerSourceFlag::Tracker);
    fail_at(peers, addr, at);
    peers
        .with_peer_mut(addr.into(), "test", |peer| peer.set_not_needed(peers))
        .unwrap();
}

// Names `addr` and lets it go before any connection: no connect
// candidate, and no erase candidate either, as it never failed.
fn never_failed(peers: &PeerStates, addr: SocketAddr) {
    peers.add_peer(addr, PeerSourceFlag::Tracker);
    peers
        .with_peer_mut(addr.into(), "test", |peer| peer.set_not_needed(peers))
        .unwrap();
}

fn connected_to_us(peers: &PeerStates, addr: SocketAddr) -> Result<PeerRx, IncomingRefused> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    peers
        .add_incoming(addr, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
        .map(|_| rx)
}

// A xorshift stream from `seed`, for seeded runs.
fn xorshift(seed: u64) -> impl FnMut() -> u64 {
    let mut rng = seed | 1;
    move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    }
}

// A list whose session started at `start`, listening at 10.0.0.200:6881,
// the endpoint every test peer, all on 10.0.0.0/24, sees us at: BEP 40
// then hashes whole addresses, so no two of them tie.
fn peer_states_started_at(start: Instant) -> PeerStates {
    make_peer_states().ordering_connects_by(ConnectOrder::new(
        start,
        Arc::new(|| connect_order::OurEndpoints {
            addresses: [[IpAddr::from([10, 0, 0, 200]); 2]; 2],
            port: 6881,
        }),
    ))
}
