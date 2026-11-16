// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::*;

// The connected addresses kept as peers are counted in and out are the
// ones a walk of the whole list finds, after every step of a seeded run
// of names, connects, incoming connections, closes, failures, peers let
// go and dropped, and web seeds, some peers sharing a host; and so is
// what connect_next picks, with peers at one host each allowed a
// connection or not.
#[test]
fn the_connected_addresses_kept_are_those_a_walk_of_the_list_finds() {
    for allow in [false, true] {
        let allowed = Arc::new(AtomicBool::new(allow));
        let peers = make_peer_states().allowing_multiple_connections_per_ip(allowed.clone());
        // Four ports on each of 16 hosts.
        let addr = |i: u64| {
            SocketAddr::from((
                [10, 1, 0, u8::try_from(i % 16).unwrap()],
                6881 + (i / 16 % 4) as u16,
            ))
        };
        let web_seed = |i: u64| PeerHandle::WebSeed(u32::try_from(i % 3).unwrap());
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut keep = Vec::new();
        let mut at = Instant::now();
        let mut pex_listed = 0;
        for step in 0..4000 {
            let r = next();
            let a = addr(r >> 8);
            match r % 9 {
                0 | 1 => peers.add_peer(a, PeerSourceFlag::Tracker),
                2 => {
                    if let Some((handle, rx, tx)) = peers.connect_next(at) {
                        keep.push((handle.into(), rx, tx));
                    }
                }
                3 => {
                    if let Ok(rx) = connected_to_us(&peers, a) {
                        keep.push((a.into(), rx, unbounded_channel().0));
                        // Half tell us the port they listen on.
                        if r & 2 == 0 {
                            peers.with_peer_mut(a.into(), "test", |peer| {
                                peer.listens_at(a, &peers);
                            });
                        }
                    }
                }
                4 | 5 => {
                    peers.with_peer_mut(a.into(), "test", |peer| {
                        if peer.connected_out().is_some() {
                            peers.connection_closed(peer, at, r & 1 == 0, false);
                        }
                    });
                }
                6 => {
                    peers.with_peer_mut(a.into(), "test", |peer| peer.set_not_needed(&peers));
                }
                7 => {
                    if r & 3 == 0 {
                        peers.drop_peer(a.into());
                    } else if let Some((rx, tx)) = peers.admit_web_seed(web_seed(r >> 8)) {
                        keep.push((web_seed(r >> 8), rx, tx));
                    }
                }
                _ => {
                    let seed = web_seed(r >> 8);
                    peers.with_peer_mut(seed, "test", |peer| {
                        if peer.connected_out().is_some() {
                            peers.connection_closed(peer, at, false, false);
                        }
                    });
                    at += MIN_RECONNECT_TIME;
                }
            }
            assert_eq!(
                peers.connected_ips_counted(),
                peers.connected_ips_walked(),
                "step {step}"
            );
            assert_eq!(peers.live_counted(), peers.live_walked(), "step {step}");
            let listed = peers.pex_listed();
            assert_eq!(*listed, peers.pex_listed_walked(), "step {step}");
            pex_listed = pex_listed.max(listed.len());
        }
        assert!(pex_listed > 0, "the run listed no peer for PEX");
        assert!(
            allow || !peers.connected_ips_counted().is_empty(),
            "the run left no connection to compare"
        );
        // Every connection closed, the count is empty again.
        for (handle, ..) in &keep {
            peers.with_peer_mut(*handle, "test", |peer| {
                if peer.connected_out().is_some() {
                    peers.connection_closed(peer, at, false, false);
                }
            });
        }
        assert!(peers.index.lock().connected_ips().is_empty());
        assert!(peers.live.lock().is_empty());
    }
}

// What the index answers is what a walk of the whole list finds: the
// candidate connect_next picks, when the next one is due, how many
// there are, and whom the list forgets first. Checked after every step
// of seeded runs of every change a peer goes through.
#[tokio::test(start_paused = true)]
async fn the_index_answers_as_a_walk_of_the_list_does() {
    for seed in [
        0x9e37_79b9_7f4a_7c15u64,
        0x2545_f491_4f6c_dd1d,
        0xd1b5_4a32_d192_ed03,
        0x8cb9_2ba7_2f3d_8dd7,
        0x0123_4567_89ab_cdef,
    ] {
        index_matches_walk(seed);
    }
}

// The times a step's answers are read at: a little before `at`, at it,
// past one reconnect time and past several.
fn query_times(at: Instant) -> [Instant; 4] {
    [
        at - Duration::from_secs(45),
        at,
        at + Duration::from_secs(61),
        at + Duration::from_secs(250),
    ]
}

// One seeded run. Twelve hosts, local and public, IPv4 and IPv6, hold
// four ports each, none of them ours, so no tie between two connections
// with one peer is settled by chance. The session starts 50 minutes
// into the run, so closes come before it and soon after it: some rank
// as no close while a wait still holds them back.
fn index_matches_walk(seed: u64) {
    use crate::peer_protocol::extended::ut_pex::{PEX_ENCRYPTION, PEX_HOLEPUNCH, PEX_SEED};
    use connect_order::OurEndpoints;

    let hosts: [IpAddr; 12] = [
        [10, 0, 0, 1].into(),
        [10, 0, 0, 2].into(),
        [192, 168, 1, 7].into(),
        [127, 0, 0, 1].into(),
        [8, 8, 4, 4].into(),
        [1, 2, 3, 4].into(),
        [93, 184, 216, 34].into(),
        [203, 0, 113, 9].into(),
        "2001:db8::1".parse().unwrap(),
        "2001:db8::2".parse().unwrap(),
        "fe80::1".parse().unwrap(),
        "fd00::5".parse().unwrap(),
    ];
    let addr = |i: u64| {
        SocketAddr::new(
            hosts[usize::try_from(i % 12).unwrap()],
            6882 + u16::try_from(i / 12 % 4).unwrap(),
        )
    };
    let web_seed = |i: u64| PeerHandle::WebSeed(u32::try_from(i % 3).unwrap());
    let endpoints = [
        OurEndpoints::default(),
        OurEndpoints {
            addresses: [[IpAddr::from([10, 0, 0, 200]); 2]; 2],
            port: 6881,
        },
        OurEndpoints {
            addresses: [
                [[203, 0, 113, 1].into(), "2001:db8::99".parse().unwrap()],
                [[10, 0, 0, 9].into(), "fd00::9".parse().unwrap()],
            ],
            port: 51413,
        },
    ];
    let sources = [
        PeerSourceFlag::Tracker,
        PeerSourceFlag::Dht,
        PeerSourceFlag::Pex,
        PeerSourceFlag::Lsd,
        PeerSourceFlag::ResumeData,
        PeerSourceFlag::Incoming,
    ];
    let base = Instant::now() + Duration::from_secs(10_000);
    let start = base + Duration::from_secs(3000);
    let ours = Arc::new(Mutex::new(endpoints[1]));
    let order = {
        let ours = ours.clone();
        ConnectOrder::new(start, Arc::new(move || *ours.lock()))
    };
    let allowed = Arc::new(AtomicBool::new(false));
    let build = |list: PeerList| {
        PeerStates::founded_on(
            list,
            Default::default(),
            u32::try_from(TOTAL_PIECES).unwrap(),
        )
        .allowing_multiple_connections_per_ip(allowed.clone())
        .ordering_connects_by(order.clone())
    };
    let mut rng = seed;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let pick = |r: u64, n: usize| (n > 0).then(|| usize::try_from(r >> 24).unwrap() % n);

    let mut peers = build(PeerList::default());
    let mut conns: Vec<(PeerHandle, PeerRx, PeerTx)> = Vec::new();
    let mut at = base;
    let (mut picked, mut later, mut due, mut victims, mut erased) = (0, 0, 0, 0, 0);
    let (mut dated_back_waiting, mut held_back, mut pauses) = (0, 0, 0);
    for step in 0..3000 {
        let r = next();
        let a = addr(r >> 16);
        match r % 20 {
            0 | 1 => peers.add_peer(a, sources[usize::try_from((r >> 8) % 6).unwrap()]),
            2 => {
                let kind = if r & 0x100 == 0 {
                    ConnectionKind::Tcp
                } else {
                    ConnectionKind::Utp
                };
                peers.add_refused_incoming(a, kind);
            }
            3 => {
                let flags = u8::try_from((r >> 8) & 0xff).unwrap()
                    & (PEX_SEED | PEX_HOLEPUNCH | PEX_ENCRYPTION);
                peers.pex_flags(a, flags);
            }
            4 => {
                peers.with_peer_mut(a.into(), "test", |peer| peer.holepunch(&peers));
            }
            5 | 6 => {
                let q = query_times(at)[usize::try_from((r >> 8) % 4).unwrap()];
                let expected = peers.connect_pick_walked(q);
                let got = peers.connect_next(q);
                assert_eq!(
                    got.as_ref().map(|(handle, ..)| *handle),
                    expected,
                    "seed {seed:#x} step {step}: connect_next"
                );
                if let Some((addr, rx, tx)) = got {
                    picked += 1;
                    conns.push((addr.into(), rx, tx));
                }
            }
            7 => {
                if let Some(i) = pick(r, conns.len()) {
                    peers.with_peer_mut(conns[i].0, "test", |peer| {
                        peer.connecting_to_live(Id20::default(), &peers, ConnectionKind::Tcp);
                    });
                }
            }
            8 | 9 => {
                if let Some(i) = pick(r, conns.len()) {
                    let failed = r & 0x1000 == 0;
                    peers.with_peer_mut(conns[i].0, "test", |peer| {
                        if peer.connected_out().is_none() {
                            return;
                        }
                        match (r >> 8) % 5 {
                            0 => peers.connection_closed(peer, at, true, false),
                            1 => peers.connection_closed(peer, at, false, false),
                            // An encrypted handshake that did not
                            // complete: the retry is due at once.
                            2 => {
                                peer.encrypt_outgoing();
                                peers.connection_closed(peer, at, failed, false);
                            }
                            // A seed we no longer need, as we are done.
                            3 => {
                                if let Some(live) = peer.get_live_mut() {
                                    live.set_upload_only(true);
                                }
                                peers.connection_closed(peer, at, false, true);
                            }
                            _ => {
                                let needed = peer.connection_closing(
                                    at,
                                    failed,
                                    peers.is_finished(),
                                    &peers,
                                );
                                peers.settle_closed(peer, needed);
                            }
                        }
                    });
                    conns.swap_remove(i);
                }
            }
            10 => {
                if let Ok(rx) = connected_to_us(&peers, a) {
                    conns.push((a.into(), rx, unbounded_channel().0));
                    if r & 0x100 == 0 {
                        peers.with_peer_mut(a.into(), "test", |peer| {
                            peer.listens_at(SocketAddr::new(a.ip(), 7000), &peers);
                        });
                    }
                }
            }
            11 => match (r >> 8) % 64 {
                0 => peers.ban(a.ip()),
                1..=12 => {
                    peers.drop_peer(a.into());
                }
                13..=20 => peers.refuse(|handle| handle == a),
                _ => {
                    peers.with_peer_mut(a.into(), "test", |peer| peer.set_not_needed(&peers));
                }
            },
            12 => peers.set_finished(r & 0x100 == 0),
            13 => *ours.lock() = endpoints[usize::try_from((r >> 8) % 3).unwrap()],
            14 => {
                allowed.store(r & 0x100 == 0, Ordering::Relaxed);
                peers.connections_per_ip_changed();
            }
            15 | 16 => at += Duration::from_secs((r >> 8) % 150),
            17 => match (r >> 8) % 4 {
                0 => {
                    if let Some(i) = pick(r, conns.len()) {
                        peers.connection_delivered(conns[i].0);
                    }
                }
                1 => {
                    peers.with_peer_mut(a.into(), "test", |peer| peer.piece_passed(&peers));
                }
                2 => {
                    peers.with_peer_mut(a.into(), "test", |peer| {
                        peer.piece_failed(r & 0x1000 == 0, &peers)
                    });
                }
                _ => peers.requeue_not_needed(),
            },
            18 => match (r >> 8) % 16 {
                // The run pauses and resumes on the list it left.
                0 => {
                    peers.close_all(at);
                    let list = peers.idle_list();
                    conns.clear();
                    peers = build(list);
                    pauses += 1;
                }
                1..=8 => {
                    let seed = web_seed(r >> 16);
                    if let Some((rx, tx)) = peers.admit_web_seed(seed) {
                        conns.push((seed, rx, tx));
                    }
                }
                _ => {
                    let seed = web_seed(r >> 16);
                    peers.with_peer_mut(seed, "test", |peer| {
                        if peer.connected_out().is_some() {
                            peers.connection_closed(peer, at, r & 0x1000 == 0, false);
                        }
                    });
                }
            },
            // The list forgets one to three peers to make room.
            _ => {
                let displace = if r & 0x100 == 0 {
                    Displace::EraseCandidates
                } else {
                    Displace::AnyIdle
                };
                let listed = peers.listed();
                let fewer_than = listed
                    .saturating_sub(usize::try_from((r >> 9) % 3).unwrap())
                    .max(1);
                let excess = (listed + 1).saturating_sub(fewer_than);
                let expected: BTreeSet<PeerHandle> = peers
                    .erase_victims_walked(displace, excess)
                    .into_iter()
                    .map(PeerHandle::from)
                    .collect();
                let before: BTreeSet<PeerHandle> = peers.states.iter().map(|e| *e.key()).collect();
                peers.erase_to(displace, fewer_than);
                let after: BTreeSet<PeerHandle> = peers.states.iter().map(|e| *e.key()).collect();
                let gone: BTreeSet<PeerHandle> = before.difference(&after).copied().collect();
                assert_eq!(gone, expected, "seed {seed:#x} step {step}: erase_to");
                erased += gone.len();
            }
        }

        let context = format!("seed {seed:#x} step {step}");
        assert_eq!(
            peers.connected_ips_counted(),
            peers.connected_ips_walked(),
            "{context}: connected addresses"
        );
        let candidates = peers.connect_candidates();
        assert_eq!(
            candidates,
            peers.connect_candidates_walked(),
            "{context}: candidates"
        );
        for q in query_times(at) {
            assert_eq!(
                peers.next_candidate(q),
                peers.next_candidate_walked(q),
                "{context}: next candidate at {q:?}"
            );
        }
        let listed = peers.listed();
        for displace in [Displace::EraseCandidates, Displace::AnyIdle] {
            for n in [1, 2, 5, listed + 1] {
                assert_eq!(
                    peers.erase_victims(displace, n),
                    peers.erase_victims_walked(displace, n),
                    "{context}: erase victims, {n}"
                );
            }
        }
        for q in query_times(at) {
            assert_eq!(
                peers.connect_pick_indexed(q),
                peers.connect_pick_walked(q),
                "{context}: pick at {q:?}"
            );
        }

        match peers.next_candidate(at) {
            NextCandidate::At(_) => later += 1,
            NextCandidate::Due => due += 1,
            NextCandidate::None => {}
        }
        if !peers.erase_victims(Displace::EraseCandidates, 1).is_empty() {
            victims += 1;
        }
        let ranking = peers.connect_order.now();
        let finished = peers.is_finished();
        let mut listed_candidates = 0;
        for e in peers.states.iter() {
            let peer = e.value();
            if !peer.is_connect_candidate() {
                continue;
            }
            listed_candidates += 1;
            if peer.ever_closed()
                && peer.connect_rank(&ranking, finished).3 == 0
                && peer.reconnect_at().is_some_and(|due| due > at)
            {
                dated_back_waiting += 1;
            }
        }
        if candidates < listed_candidates {
            held_back += 1;
        }
    }
    let context = format!("seed {seed:#x}");
    assert!(picked > 0, "{context}: no connect_next picked a peer");
    assert!(later > 0, "{context}: no candidate was due later");
    assert!(due > 0, "{context}: no candidate was due");
    assert!(victims > 0, "{context}: no erase candidate");
    assert!(erased > 0, "{context}: no peer was forgotten");
    assert!(
        dated_back_waiting > 0,
        "{context}: no candidate ranked as never closed while it waited"
    );
    assert!(
        held_back > 0,
        "{context}: no connection held a candidate back"
    );
    assert!(pauses > 0, "{context}: the run never paused");
    // Every peer gone, the index is empty again.
    let handles: Vec<PeerHandle> = peers.states.iter().map(|e| *e.key()).collect();
    for handle in handles {
        peers.drop_peer(handle);
    }
    assert!(peers.index.lock().holds_nothing(), "{context}: index left");
}

// Times the connect pick, a run of connect_next and the eviction pick,
// off the index and off a walk of the list, at list sizes up to the
// cap, and prints what a listed peer holds. A run by hand with
// WEAVER_PEER_INDEX_TIMINGS set and an optimised build prints the
// table; any other run returns at once.
#[test]
fn peer_index_timings() {
    if std::env::var_os("WEAVER_PEER_INDEX_TIMINGS").is_none() {
        return;
    }
    println!("size_of::<Peer>() = {} bytes", size_of::<Peer>());
    for (entry, size) in PeerIndex::entry_sizes() {
        println!("index {entry} entry = {size} bytes");
    }
    // `n` peers named by a tracker, a third of them failed once at `t0`:
    // waiting candidates and erase candidates.
    let list = |n: usize, t0: Instant| {
        let peers = peer_states_started_at(t0);
        for i in 0..n {
            peers.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
        }
        for i in (0..n).step_by(3) {
            fail_at(&peers, peer_addr(i), t0);
        }
        peers
    };
    // The mean time of `reps` calls of `f`, in nanoseconds.
    fn per_call(reps: usize, mut f: impl FnMut()) -> u128 {
        let begun = std::time::Instant::now();
        for _ in 0..reps {
            f();
        }
        begun.elapsed().as_nanos() / u128::try_from(reps).unwrap()
    }
    println!(
        "n | pick walk ns | pick index ns | connect_next walk ns | connect_next index ns | victim walk ns | victim index ns | admit full walk ns | admit full index ns | first pick rebase ns"
    );
    for n in [10, 300, 3000, MAX_PEERLIST_SIZE] {
        let t0 = Instant::now();
        let now = t0 + Duration::from_secs(1);
        let reps = (300_000 / n).clamp(20, 20_000);
        let peers = list(n, t0);
        // The list orders its candidates by the session's endpoints,
        // set after it was founded: the first pick ranks them anew in
        // one walk, which is timed apart.
        let rebase = per_call(1, || {
            std::hint::black_box(peers.connect_pick_indexed(now));
        });
        let pick_walk = per_call(reps, || {
            std::hint::black_box(peers.connect_pick_walked(now));
        });
        let pick_index = per_call(reps, || {
            std::hint::black_box(peers.connect_pick_indexed(now));
        });
        let victim_walk = per_call(reps, || {
            std::hint::black_box(peers.erase_victims_walked(Displace::EraseCandidates, 1));
        });
        let victim_index = per_call(reps, || {
            std::hint::black_box(peers.erase_victims(Displace::EraseCandidates, 1));
        });
        // A run of connects, each as the connect loop makes it, on a
        // list of its own each.
        let connects = (n - n.div_ceil(3)).min(100);
        let indexed = list(n, t0);
        indexed.connect_pick_indexed(now);
        let connect_index = per_call(connects, || {
            std::hint::black_box(indexed.connect_next(now).expect("a due candidate"));
        });
        let walked = list(n, t0);
        let connect_walk = per_call(connects, || {
            std::hint::black_box(connect_next_walked(&walked, now).expect("a due candidate"));
        });
        // A peer named at a full list pushes out the lowest erase
        // candidate.
        let (admit_walk, admit_index) = if n == MAX_PEERLIST_SIZE {
            let added = connects.min(n / 3);
            let indexed = list(n, t0);
            indexed.connect_pick_indexed(now);
            let mut i = n;
            let admit_index = per_call(added, || {
                indexed.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
                i += 1;
            });
            let walked = list(n, t0);
            let mut i = n;
            let admit_walk = per_call(added, || {
                erase_walked(&walked, Displace::EraseCandidates, MAX_PEERLIST_SIZE);
                walked.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
                i += 1;
            });
            assert_eq!(indexed.listed(), n);
            assert_eq!(walked.listed(), n);
            (admit_walk.to_string(), admit_index.to_string())
        } else {
            ("-".to_owned(), "-".to_owned())
        };
        println!(
            "{n} | {pick_walk} | {pick_index} | {connect_walk} | {connect_index} | {victim_walk} | {victim_index} | {admit_walk} | {admit_index} | {rebase}"
        );
    }
}

// `PeerStates::erase_to` as it read the whole list for its victims.
fn erase_walked(peers: &PeerStates, displace: Displace, fewer_than: usize) {
    let excess = (peers.listed() + 1).saturating_sub(fewer_than);
    if excess == 0 {
        return;
    }
    for addr in peers.erase_victims_walked(displace, excess) {
        if let Some((_, peer)) = peers.states.remove_if(&PeerHandle::from(addr), |_, peer| {
            peer.connected_out().is_none()
        }) {
            peers.erased(addr.into(), &peer);
        }
    }
}

// `PeerStates::connect_next` as it walked the whole list for its pick
// and its victims.
fn connect_next_walked(peers: &PeerStates, now: Instant) -> Option<SocketAddr> {
    let _admission = peers.admission.lock();
    erase_walked(peers, Displace::EraseCandidates, PEERLIST_LOW_WATERMARK);
    let addr = peers.connect_pick_walked(now)?;
    peers
        .with_peer_mut(addr.into(), "test", |peer| peer.connect_if_due(now, peers))
        .flatten()
        .map(|_| addr)
}

#[test]
fn a_failed_peer_waits_failcount_plus_one_reconnect_times() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    let t0 = Instant::now();
    peers.add_peer(a, PeerSourceFlag::Tracker);
    assert_eq!(peers.next_candidate(t0), NextCandidate::Due);

    let mut failed = t0;
    for failcount in 1..MAX_FAILCOUNT {
        fail_at(&peers, a, failed);
        let due = failed + reconnect_wait(failcount);
        assert_eq!(state_of(&peers, a), Some("queued"));
        assert_eq!(peers.next_candidate(failed), NextCandidate::At(due));
        assert!(peers.connect_next(due - Duration::from_millis(1)).is_none());
        failed = due;
    }
    assert_eq!(peers.connect_next(failed).map(|(addr, ..)| addr), Some(a));
}

#[tokio::test(start_paused = true)]
async fn the_connect_loop_wakes_when_the_reconnect_time_comes() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    fail_at(&peers, a, Instant::now());

    let due = peers.wait_for_candidate();
    tokio::pin!(due);
    assert!(futures::poll!(&mut due).is_pending());
    tokio::time::advance(reconnect_wait(1) - Duration::from_secs(1)).await;
    assert!(futures::poll!(&mut due).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(futures::poll!(&mut due).is_ready());
}

#[tokio::test(start_paused = true)]
async fn the_connect_loop_wakes_when_a_peer_is_named() {
    let peers = make_peer_states();
    let due = peers.wait_for_candidate();
    tokio::pin!(due);
    assert!(futures::poll!(&mut due).is_pending());
    peers.add_peer(peer_addr(1), PeerSourceFlag::Tracker);
    assert!(futures::poll!(&mut due).is_ready());
}

#[test]
fn a_peer_that_failed_max_failcount_times_is_dropped_as_a_candidate() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    let last = fail_out(&peers, a, Instant::now());

    assert_eq!(state_of(&peers, a), Some("dead"));
    assert_eq!(peers.next_candidate(last), NextCandidate::None);
    let much_later = last + Duration::from_secs(365 * 24 * 3600);
    assert!(peers.connect_next(much_later).is_none());
}

// Every source but the tracker's, each naming `addr` once.
fn name_by_every_other_source(peers: &PeerStates, addr: SocketAddr) {
    for source in [
        PeerSourceFlag::Dht,
        PeerSourceFlag::Pex,
        PeerSourceFlag::Lsd,
        PeerSourceFlag::ResumeData,
    ] {
        peers.add_peer(addr, source);
    }
    peers.add_refused_incoming(addr, ConnectionKind::Utp);
}

// libtorrent `peer_list::update_peer`: "only trust this if it comes from
// the tracker". A tracker's naming takes a failure off the peer, which
// shortens its wait to reconnect; any other source's leaves both.
#[test]
fn only_a_trackers_naming_forgives_a_failed_peer_a_failure() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Dht);
    let t0 = Instant::now();
    fail_at(&peers, a, t0);
    let t1 = t0 + reconnect_wait(1);
    fail_at(&peers, a, t1);
    assert_eq!(
        peers.next_candidate(t1),
        NextCandidate::At(t1 + reconnect_wait(2))
    );

    name_by_every_other_source(&peers, a);
    assert_eq!(state_of(&peers, a), Some("queued"));
    assert_eq!(
        peers.next_candidate(t1),
        NextCandidate::At(t1 + reconnect_wait(2))
    );

    peers.add_peer(a, PeerSourceFlag::Tracker);
    assert_eq!(
        peers.next_candidate(t1),
        NextCandidate::At(t1 + reconnect_wait(1))
    );
}

// libtorrent `peer_list::update_peer` and `is_connect_candidate`: a peer
// at `max_failcount` is no candidate until a tracker's naming takes it
// under; the DHT, PEX or any other source naming it leaves it so.
#[test]
fn a_dropped_peer_is_revived_only_when_a_tracker_names_it_again() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    let last = fail_out(&peers, a, Instant::now());
    assert_eq!(state_of(&peers, a), Some("dead"));

    name_by_every_other_source(&peers, a);
    assert_eq!(state_of(&peers, a), Some("dead"));
    assert_eq!(peers.next_candidate(last), NextCandidate::None);

    peers.add_peer(a, PeerSourceFlag::Tracker);
    assert_eq!(state_of(&peers, a), Some("queued"));
    assert_eq!(
        peers.next_candidate(last),
        NextCandidate::At(last + reconnect_wait(MAX_FAILCOUNT - 1))
    );
}

#[test]
fn a_dropped_peer_is_revived_when_it_connects_to_us() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    fail_out(&peers, a, Instant::now());

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    peers
        .add_incoming(a, Id20::default(), tx, ConnectionKind::Tcp, OUR_PORT)
        .unwrap();
    assert_eq!(state_of(&peers, a), Some("live"));
}

#[test]
fn failures_are_forgotten_once_a_connection_delivers() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    let t0 = Instant::now();
    fail_at(&peers, a, t0);
    let t1 = t0 + reconnect_wait(1);
    fail_at(&peers, a, t1);

    // The next attempt completes its handshake and carries a message.
    let t2 = t1 + reconnect_wait(2);
    peers
        .with_peer_mut(a.into(), "test", |peer| peer.connect_if_due(t2, &peers))
        .unwrap()
        .unwrap();
    peers.connection_delivered(a.into());

    // Its later failure is the first in a row, not the third.
    let t3 = t2 + Duration::from_secs(600);
    peers
        .with_peer_mut(a.into(), "test", |peer| {
            peers.connection_closed(peer, t3, true, false)
        })
        .unwrap();
    assert_eq!(state_of(&peers, a), Some("queued"));
    assert_eq!(
        peers.next_candidate(t3),
        NextCandidate::At(t3 + reconnect_wait(1))
    );
}

// A seed we let go while we only upload is no failed peer: once we want
// pieces again it is due at once. A seed whose connection failed then
// waits as its failure has it wait.
#[test]
fn a_seed_we_let_go_is_due_at_once_when_wanted_again() {
    let peers = make_peer_states();
    let let_go = peer_addr(1);
    let failed = peer_addr(2);
    let t0 = Instant::now();
    for (addr, fails) in [(let_go, false), (failed, true)] {
        peers.add_peer(addr, PeerSourceFlag::Tracker);
        peers
            .with_peer_mut(addr.into(), "test", |peer| {
                peer.connect_if_due(t0, &peers).unwrap();
                assert!(
                    peer.connecting_to_live(Id20::default(), &peers, ConnectionKind::Tcp)
                        .is_some()
                );
            })
            .unwrap();
        peers.update_bitfield(addr.into(), full_bitfield()).unwrap();
        peers
            .with_peer_mut(addr.into(), "test", |peer| {
                peers.connection_closed(peer, t0, fails, true);
            })
            .unwrap();
        assert_eq!(state_of(&peers, addr), Some("not needed"));
    }
    assert_eq!(peers.next_candidate(t0), NextCandidate::None);

    peers.requeue_not_needed();
    assert_eq!(peers.connect_next(t0).map(|(addr, ..)| addr), Some(let_go));
    assert!(peers.connect_next(t0).is_none());
    assert_eq!(
        peers.next_candidate(t0),
        NextCandidate::At(t0 + reconnect_wait(1))
    );
}

// A connection we close while we want the peer is no failure, and the
// peer waits one reconnect time from the close.
#[test]
fn a_connection_we_close_is_not_a_failure() {
    let peers = make_peer_states();
    let a = peer_addr(1);
    peers.add_peer(a, PeerSourceFlag::Tracker);
    let t0 = Instant::now();
    peers
        .with_peer_mut(a.into(), "test", |peer| {
            peer.connect_if_due(t0, &peers).unwrap();
            peers.connection_closed(peer, t0, false, false);
        })
        .unwrap();
    assert_eq!(state_of(&peers, a), Some("queued"));
    assert_eq!(
        peers.next_candidate(t0),
        NextCandidate::At(t0 + reconnect_wait(0))
    );
}

// libtorrent `peer_list::insert_peer` and `new_connection` at a full
// list: `erase_peers` forgets the erase candidate `compare_peer_erase`
// ranks lowest, the most failures first, then one we cannot connect to,
// then the least trusted; a named peer is refused when there is none,
// and a peer that connected to us (`force_erase`) pushes out an idle one.
#[tokio::test(start_paused = true)]
async fn an_insert_at_full_displaces_by_the_erase_rank() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    let (most_failed, unconnectable, distrusted, trusted, kept) = (
        peer_addr(0),
        peer_addr(1),
        peer_addr(2),
        peer_addr(3),
        peer_addr(4),
    );
    peers.add_peer(most_failed, PeerSourceFlag::Tracker);
    fail_out(&peers, most_failed, t0);
    // It connected over TCP, from no port it listens on, and failed.
    let _closed_rx = connected_to_us(&peers, unconnectable).unwrap();
    peers
        .with_peer_mut(unconnectable.into(), "test", |peer| {
            peer.take_state(&peers);
            peers.connection_closed(peer, t0, true, false);
        })
        .unwrap();
    for addr in [distrusted, trusted] {
        failed_once(&peers, addr, t0);
    }
    assert!(
        !peers
            .with_peer_mut(distrusted.into(), "test", |peer| peer
                .piece_failed(false, &peers))
            .unwrap()
    );
    never_failed(&peers, kept);
    let live = peer_addr(5);
    let _live_rx = connected_to_us(&peers, live).unwrap();
    for i in 6..MAX_PEERLIST_SIZE {
        peers.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
    }
    assert_eq!(peers.listed(), MAX_PEERLIST_SIZE);

    let newcomer = |i| peer_addr(MAX_PEERLIST_SIZE + i);
    for (i, erased) in [most_failed, unconnectable, distrusted, trusted]
        .into_iter()
        .enumerate()
    {
        assert!(state_of(&peers, erased).is_some());
        peers.add_peer(newcomer(i), PeerSourceFlag::Tracker);
        assert_eq!(state_of(&peers, erased), None);
        assert_eq!(state_of(&peers, newcomer(i)), Some("queued"));
    }

    // No erase candidate is left: a named peer pushes out neither a peer
    // that never failed nor a connect candidate.
    peers.add_peer(newcomer(4), PeerSourceFlag::Tracker);
    assert_eq!(state_of(&peers, newcomer(4)), None);
    assert_eq!(state_of(&peers, kept), Some("not needed"));
    assert_eq!(peers.listed(), MAX_PEERLIST_SIZE);

    // A peer that connected to us pushes out an idle one, and the
    // connected peer stays.
    let _rx = connected_to_us(&peers, newcomer(5)).unwrap();
    assert_eq!(state_of(&peers, newcomer(5)), Some("live"));
    assert_eq!(state_of(&peers, live), Some("live"));
    assert_eq!(peers.listed(), MAX_PEERLIST_SIZE);
}

// libtorrent `peer_list::is_erase_candidate` and `compare_peer_erase`
// (peer_list.cpp:88-108, 354-364, v2.0.15): a peer that resume data
// alone named is an erase candidate once let go, failed or not, and at
// the same failure count it goes before a peer another source named.
#[tokio::test(start_paused = true)]
async fn a_peer_resume_data_alone_named_goes_first_at_equal_failures() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    let (resume_failed, tracker_failed, resume_idle, kept) =
        (peer_addr(0), peer_addr(1), peer_addr(2), peer_addr(3));
    peers.add_peer(resume_failed, PeerSourceFlag::ResumeData);
    fail_at(&peers, resume_failed, t0);
    peers
        .with_peer_mut(resume_failed.into(), "test", |peer| {
            peer.set_not_needed(&peers)
        })
        .unwrap();
    failed_once(&peers, tracker_failed, t0);
    peers.add_peer(resume_idle, PeerSourceFlag::ResumeData);
    peers
        .with_peer_mut(resume_idle.into(), "test", |peer| {
            peer.set_not_needed(&peers)
        })
        .unwrap();
    never_failed(&peers, kept);
    for i in 4..MAX_PEERLIST_SIZE {
        peers.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
    }
    assert_eq!(peers.listed(), MAX_PEERLIST_SIZE);

    let newcomer = |i| peer_addr(MAX_PEERLIST_SIZE + i);
    for (i, erased) in [resume_failed, tracker_failed, resume_idle]
        .into_iter()
        .enumerate()
    {
        assert!(state_of(&peers, erased).is_some());
        peers.add_peer(newcomer(i), PeerSourceFlag::Tracker);
        assert_eq!(state_of(&peers, erased), None);
        assert_eq!(state_of(&peers, newcomer(i)), Some("queued"));
    }
    peers.add_peer(newcomer(3), PeerSourceFlag::Tracker);
    assert_eq!(state_of(&peers, newcomer(3)), None);
    assert_eq!(state_of(&peers, kept), Some("not needed"));
}

// libtorrent `peer_list::is_erase_candidate`: a peer with no connection
// that is no connect candidate goes only once it has failed.
#[tokio::test(start_paused = true)]
async fn weeding_forgets_a_failed_peer_and_keeps_one_that_never_failed() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    let (failed, kept) = (peer_addr(0), peer_addr(1));
    failed_once(&peers, failed, t0);
    never_failed(&peers, kept);
    for i in 2..PEERLIST_LOW_WATERMARK + 5 {
        peers.add_peer(peer_addr(i), PeerSourceFlag::Tracker);
    }

    assert!(peers.connect_next(t0).is_some());
    assert_eq!(state_of(&peers, failed), None);
    assert_eq!(state_of(&peers, kept), Some("not needed"));
    assert_eq!(peers.listed(), PEERLIST_LOW_WATERMARK + 4);
}

// libtorrent `peer_list::find_connect_candidates` weeds once the list
// holds 95% of `max_peerlist_size`, and `erase_peers` stops under its
// low watermark.
#[tokio::test(start_paused = true)]
async fn weeding_runs_past_95_percent_and_leaves_the_list_under_it() {
    let peers = make_peer_states();
    let t0 = Instant::now();
    let failed = (0..100).map(peer_addr).collect::<Vec<_>>();
    for &addr in &failed {
        failed_once(&peers, addr, t0);
    }
    let mut next = failed.len();
    let mut fill_to = |listed: usize| {
        while peers.listed() < listed {
            peers.add_peer(peer_addr(next), PeerSourceFlag::Tracker);
            next += 1;
        }
    };
    let failed_left = |peers: &PeerStates| {
        failed
            .iter()
            .filter(|&&addr| state_of(peers, addr).is_some())
            .count()
    };

    fill_to(PEERLIST_LOW_WATERMARK - 1);
    assert!(peers.connect_next(t0).is_some());
    assert_eq!(peers.listed(), PEERLIST_LOW_WATERMARK - 1);
    assert_eq!(failed_left(&peers), failed.len());

    fill_to(PEERLIST_LOW_WATERMARK + 50);
    assert!(peers.connect_next(t0).is_some());
    assert_eq!(peers.listed(), PEERLIST_LOW_WATERMARK - 1);
    assert_eq!(failed_left(&peers), failed.len() - 51);
}

// Whom weeding to `fewer_than` forgets by a full sort of every idle
// peer `displace` allows on the list, by erase rank and address: the
// rule the partial selection keeps.
fn erased_by_full_sort(
    peers: &PeerStates,
    displace: Displace,
    fewer_than: usize,
) -> HashSet<PeerHandle> {
    let excess = (peers.listed() + 1).saturating_sub(fewer_than);
    let mut ranked = peers
        .states
        .iter()
        .filter(|e| e.key().is_listed() && e.value().connected_out().is_none())
        .map(|e| (erase_rank(e.value()), *e.key()))
        .filter(|(rank, _)| displace.allows(rank))
        .collect::<Vec<_>>();
    ranked.sort();
    ranked
        .into_iter()
        .take(excess)
        .map(|(_, addr)| addr)
        .collect()
}

// A list of `size` peers from `next`: named ones, ones that failed one
// to three times and were or were not let go, ones let go that never
// failed, live ones, ones whose connection to us closed, failed or
// not, and ones refused while the slots were full; some more trusted,
// some less. Many share an erase rank, so the address settles who goes.
// Returns the receiving ends of the live peers' channels.
fn seeded_list(
    peers: &PeerStates,
    next: &mut impl FnMut() -> u64,
    size: usize,
    t0: Instant,
) -> Vec<PeerRx> {
    let spaced = reconnect_wait(MAX_FAILCOUNT);
    let mut keep = Vec::new();
    for i in 0..size {
        let r = next();
        let addr = peer_addr(i);
        match r % 6 {
            0 => peers.add_peer(addr, PeerSourceFlag::Tracker),
            1 => {
                peers.add_peer(addr, PeerSourceFlag::Tracker);
                let failures = 1 + u32::try_from((r >> 8) % u64::from(MAX_FAILCOUNT)).unwrap();
                for k in 0..failures {
                    fail_at(peers, addr, t0 + spaced * (k + 1));
                }
                if r & 16 != 0 {
                    peers.with_peer_mut(addr.into(), "test", |peer| peer.set_not_needed(peers));
                }
            }
            2 => never_failed(peers, addr),
            3 => keep.push(connected_to_us(peers, addr).unwrap()),
            4 => {
                let _rx = connected_to_us(peers, addr).unwrap();
                peers.with_peer_mut(addr.into(), "test", |peer| {
                    peers.connection_closed(peer, t0, r & 16 != 0, false);
                });
            }
            _ => peers.add_refused_incoming(addr, ConnectionKind::Tcp),
        }
        peers.with_peer_mut(addr.into(), "test", |peer| {
            if r & 32 != 0 {
                peer.piece_passed(peers);
            }
            if r & 64 != 0 {
                let _banned = peer.piece_failed(false, peers);
            }
        });
    }
    keep
}

// The partial selection picks the set a full sort puts first, over
// seeded lists with many ties of rank, every count to take.
#[test]
fn the_lowest_are_the_first_a_full_sort_puts_first() {
    let mut next = xorshift(0x2545_f491_4f6c_dd1d);
    for _ in 0..500 {
        let len = usize::try_from(next() % 64).unwrap();
        // Few ranks, so most entries tie on rank and differ in address.
        let ranked: Vec<(u8, u16)> = (0..len)
            .map(|i| (u8::try_from(next() % 4).unwrap(), u16::try_from(i).unwrap()))
            .collect();
        let mut sorted = ranked.clone();
        sorted.sort();
        for n in 0..=len + 1 {
            let mut picked = lowest(ranked.clone(), n);
            picked.sort();
            assert_eq!(picked, sorted[..n.min(len)], "{ranked:?} {n}");
        }
    }
}

// Weeding forgets the peers a full sort of the list would, over seeded
// lists of every kind of peer and a full list, for both kinds of room
// made, and says the same of whether the list is under its mark.
#[tokio::test(start_paused = true)]
async fn weeding_forgets_the_peers_a_full_sort_would() {
    let t0 = Instant::now();
    let mut checked = 0;
    for seed in 0..40u64 {
        let mut next = xorshift(0x9e37_79b9_7f4a_7c15 ^ seed);
        let peers = make_peer_states();
        let size = if seed == 0 {
            MAX_PEERLIST_SIZE
        } else {
            64 + usize::try_from(next() % 400).unwrap()
        };
        let _keep = seeded_list(&peers, &mut next, size, t0);
        for round in 0..4 {
            let displace = if next() & 1 == 0 {
                Displace::EraseCandidates
            } else {
                Displace::AnyIdle
            };
            let listed = peers.listed();
            let fewer_than = if seed == 0 && round == 0 {
                PEERLIST_LOW_WATERMARK
            } else {
                1 + usize::try_from(next() % u64::try_from(listed + 1).unwrap()).unwrap()
            };
            let expected = erased_by_full_sort(&peers, displace, fewer_than);
            let before: HashSet<PeerHandle> = peers.states.iter().map(|e| *e.key()).collect();
            let under = peers.erase_to(displace, fewer_than);
            let after: HashSet<PeerHandle> = peers.states.iter().map(|e| *e.key()).collect();
            let erased: HashSet<PeerHandle> = before.difference(&after).copied().collect();
            assert_eq!(erased, expected, "seed {seed} round {round}");
            assert_eq!(under, listed - expected.len() < fewer_than);
            checked += expected.len();
        }
    }
    assert!(checked > 0, "no run forgot a peer");
}
