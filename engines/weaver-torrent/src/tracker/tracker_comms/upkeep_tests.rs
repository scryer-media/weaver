// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{net::SocketAddr, time::Duration};

use futures::StreamExt;

use tokio::time::Instant;

use super::tests::{
    ANSWERED, LEG, STARTED, answered, due, http, http_listener, list, live, next_announce, op,
    order, query_param, read_announce, respond, short_of_a_seed, start,
};
use super::{
    AnnounceErrorKind, ReannounceDelay, StoppedAnnounces, SupportedTracker, ToldPorts,
    TrackerAnnounce, TrackerCommsStats, TrackerList, TrackerTask,
};
use super::{Entry, RowIndex, unique, write_answered, write_rows};

// The list's upkeep as it walked the list for each tracker, before it
// built a set per pass: the reference the test below compares with.
fn listed_new(tiers: &[Vec<SupportedTracker>], now: Instant) -> TrackerList {
    let mut entries: Vec<Entry> = Vec::new();
    for (tier, trackers) in tiers.iter().enumerate() {
        for tracker in trackers {
            if entries.iter().any(|e| &e.tracker == tracker) {
                continue;
            }
            entries.push(Entry::new(tracker.clone(), tier, now));
        }
    }
    TrackerList {
        entries,
        completion: short_of_a_seed(),
        forced: Vec::new(),
        announcing: true,
        handed_over: false,
        shape: &crate::tracker::AnnounceShape::WEAVER,
        unpublished: false,
    }
}

fn listed_replace(
    list: &mut TrackerList,
    tiers: &[Vec<SupportedTracker>],
    now: Instant,
) -> Vec<(SupportedTracker, super::Legs)> {
    let mut old = std::mem::take(&mut list.entries);
    for (tier, trackers) in tiers.iter().enumerate() {
        for tracker in trackers {
            if list.position(tracker).is_some() {
                continue;
            }
            let entry = match old.iter().position(|e| &e.tracker == tracker) {
                Some(i) => Entry {
                    tracker: tracker.clone(),
                    tier,
                    ..old.remove(i)
                },
                None => Entry::new(tracker.clone(), tier, now),
            };
            list.entries.push(entry);
        }
    }
    let entries = &list.entries;
    list.forced
        .retain(|t| entries.iter().any(|e| &e.tracker == t));
    old.into_iter()
        .filter(|e| e.start_sent.any())
        .map(|e| (e.tracker, e.start_sent))
        .collect()
}

fn listed_start_due(
    list: &mut TrackerList,
    now: Instant,
    stats: &TrackerCommsStats,
) -> Vec<(SupportedTracker, super::Events)> {
    let seed = stats.completed_during_run || list.owes_completed(stats);
    let port = ToldPorts::default();
    let forced = std::mem::take(&mut list.forced)
        .into_iter()
        .filter_map(|t| list.position(&t))
        .filter(|&i| list.entries[i].can_announce(now, seed, port));
    let mut picked: Vec<usize> = forced.collect();
    for i in list.walk(|_| true, |e| e.can_announce(now, seed, port)) {
        if !picked.contains(&i) {
            picked.push(i);
        }
    }
    picked
        .into_iter()
        .map(|i| {
            let event = list.event(&list.entries[i], stats);
            let entry = &mut list.entries[i];
            entry.updating = true;
            entry.port = port;
            (entry.tracker.clone(), event)
        })
        .collect()
}

fn listed_unique<'a>(
    trackers: impl IntoIterator<Item = &'a SupportedTracker>,
) -> Vec<SupportedTracker> {
    let mut unique: Vec<SupportedTracker> = Vec::new();
    for tracker in trackers {
        if !unique.contains(tracker) {
            unique.push(tracker.clone());
        }
    }
    unique
}

// The list kept with a set per pass holds the trackers, tiers, state,
// forced trackers and order the list walked per tracker held, and
// returns the same gone trackers, announces and unique trackers, over
// seeded runs of replaces with repeats, forces, answers, failures and
// due announces.
#[test]
fn tracker_upkeep_by_sets_matches_the_list_walked_per_tracker() {
    let mut rng = 0x510e_527f_ade6_82d1u64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let names: Vec<String> = (0..14).map(|i| format!("t{i}")).collect();
    let tiers = |next: &mut dyn FnMut() -> u64| -> Vec<Vec<SupportedTracker>> {
        (0..1 + next() % 4)
            .map(|_| {
                (0..next() % 6)
                    .map(|_| http(&names[(next() % 14) as usize]))
                    .collect()
            })
            .collect()
    };
    let stats = live(0, 90, false);
    for run in 0..30 {
        let mut now = Instant::now();
        let first = tiers(&mut next);
        let mut sets = TrackerList::new(
            &first,
            now,
            &crate::tracker::AnnounceShape::WEAVER,
            short_of_a_seed(),
        );
        let mut walked = listed_new(&first, now);
        assert_eq!(format!("{sets:?}"), format!("{walked:?}"), "run {run}");
        for step in 0..300 {
            let r = next();
            let tracker = http(&names[(r >> 8) as usize % 14]);
            match r % 6 {
                0 => {
                    let to = tiers(&mut next);
                    assert_eq!(
                        sets.replace(&to, now),
                        listed_replace(&mut walked, &to, now),
                        "run {run} step {step}"
                    );
                    let flat: Vec<&SupportedTracker> = to.iter().flatten().collect();
                    assert_eq!(
                        unique(flat.iter().copied()),
                        listed_unique(flat.iter().copied())
                    );
                }
                1 => {
                    let ignore = r & 0x100 != 0;
                    let named = (r & 0x200 != 0).then_some(&tracker);
                    assert_eq!(
                        sets.force(now, named, ignore),
                        walked.force(now, named, ignore)
                    );
                }
                2 => {
                    sets.on_success(&tracker, answered(STARTED), now, &ANSWERED);
                    walked.on_success(&tracker, answered(STARTED), now, &ANSWERED);
                }
                3 => {
                    sets.on_failure(&tracker, now, Duration::from_secs(5));
                    walked.on_failure(&tracker, now, Duration::from_secs(5));
                }
                4 => {
                    assert_eq!(
                        sets.start_due(now, ToldPorts::default(), &stats),
                        listed_start_due(&mut walked, now, &stats),
                        "run {run} step {step}"
                    );
                }
                _ => now += Duration::from_secs(next() % 120),
            }
            assert_eq!(
                format!("{sets:?}"),
                format!("{walked:?}"),
                "run {run} step {step}"
            );
        }
    }
}

#[test]
fn replaced_trackers_keep_what_announcing_to_them_did() {
    // libtorrent tracker_list::replace orders by tier and drops a
    // repeated URL; a kept tracker keeps its state here.
    let s = Duration::from_secs;
    let t0 = Instant::now();
    let mut list = list(&[&["a", "b"]], t0);
    let stats = live(0, 90, false);
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &stats),
        [(http("a"), STARTED)]
    );
    list.on_success(&http("a"), answered(STARTED), t0, &ANSWERED);

    list.replace(
        &[vec![http("c"), http("a")], vec![http("a"), http("d")]],
        t0 + s(10),
    );
    assert_eq!(order(&list), [http("c"), http("a"), http("d")]);
    assert_eq!(
        list.entries.iter().map(|e| e.tier).collect::<Vec<_>>(),
        [0, 0, 1]
    );
    // a is not sent `started` again nor brought forward; c and d, new,
    // are due, each the first of its tier.
    let a = &list.entries[1];
    assert!(a.start_sent.has(LEG));
    assert_eq!(a.next_announce, t0 + ANSWERED.regular);
    assert_eq!(
        due(&mut list, t0 + s(10), ToldPorts::default(), &stats),
        [(http("c"), STARTED), (http("d"), STARTED)]
    );
}

#[test]
fn replaced_trackers_gone_that_accepted_started_are_returned() {
    let t0 = Instant::now();
    let mut list = list(&[&["a", "b"], &["c"]], t0);
    let stats = live(0, 90, false);
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &stats),
        [(http("a"), STARTED), (http("c"), STARTED)]
    );
    list.on_success(&http("a"), answered(STARTED), t0, &ANSWERED);

    // a heard `started` and is owed `stopped`; b never heard from us.
    let gone = list.replace(&[vec![http("c")]], t0);
    assert_eq!(gone, [(http("a"), [LEG].into_iter().collect())]);
    assert_eq!(order(&list), [http("c")]);
}

#[test]
fn a_forced_reannounce_is_due_now_and_never_doubles_one_in_flight() {
    // libtorrent trigger_announce and torrent::announce_tracker_now.
    let s = Duration::from_secs;
    let t0 = Instant::now();
    let mut list = list(&[&["a", "b"]], t0);
    let stats = live(0, 90, false);
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &stats),
        [(http("a"), STARTED)]
    );
    let delay = ReannounceDelay {
        floor: s(900),
        regular: s(1800),
    };
    list.on_success(&http("a"), answered(STARTED), t0, &delay);

    // Within a's min interval, a force that respects it waits for it.
    let t1 = t0 + s(60);
    assert!(list.force(t1, None, false));
    assert_eq!(due(&mut list, t1, ToldPorts::default(), &stats), []);
    assert_eq!(list.next_due(), Some(t0 + s(901)));
    // Lifting it, a is due now, and once in flight a second force sends
    // nothing more.
    assert!(list.force(t1, None, true));
    assert_eq!(
        due(&mut list, t1, ToldPorts::default(), &stats),
        [(http("a"), None)]
    );
    assert!(list.force(t1, None, true));
    assert!(list.force(t1, Some(&http("a")), true));
    assert_eq!(due(&mut list, t1, ToldPorts::default(), &stats), []);

    // Forced alone, b goes though a works and the walk would stop there.
    list.on_success(&http("a"), answered(None), t1, &delay);
    assert!(list.force(t1, Some(&http("b")), true));
    assert_eq!(
        due(&mut list, t1, ToldPorts::default(), &stats),
        [(http("b"), STARTED)]
    );
    assert!(!list.force(t1, Some(&http("x")), true));
}

/// libtorrent `trigger_announce` writes the entry's next announce with
/// no regard for an announce waiting for its answer, and
/// `tracker_response` then writes the answer's intervals over it: a
/// force made while an announce waits is dropped.
#[test]
fn a_force_while_an_announce_waits_is_overwritten_by_its_answer() {
    let s = Duration::from_secs;
    let t0 = Instant::now();
    let mut list = list(&[&["a"]], t0);
    let stats = live(0, 90, false);
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &stats),
        [(http("a"), STARTED)]
    );
    let delay = ReannounceDelay {
        floor: s(900),
        regular: s(1800),
    };

    // Forced while it waits, alone and with the rest: nothing goes.
    let t1 = t0 + s(5);
    assert!(list.force(t1, None, true));
    assert!(list.force(t1, Some(&http("a")), true));
    assert_eq!(due(&mut list, t1, ToldPorts::default(), &stats), []);
    assert_eq!(list.next_due(), None);

    // Answered: due at the answer's interval, the force gone.
    list.on_success(&http("a"), answered(STARTED), t1, &delay);
    assert_eq!(due(&mut list, t1, ToldPorts::default(), &stats), []);
    assert_eq!(list.next_due(), Some(t1 + s(1800)));
}

/// Waits, driving `peers`, until the table says `done`.
pub(super) async fn table_once(
    status: &super::TrackerStatus,
    peers: &mut (impl futures::Stream<Item = SocketAddr> + Unpin),
    done: impl Fn(&[TrackerAnnounce]) -> bool,
) -> Vec<TrackerAnnounce> {
    let mut rx = status.subscribe();
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent peers"),
        r = rx.wait_for(|a| done(a)) => { r.unwrap(); }
    };
    status.announces()
}

#[tokio::test]
async fn a_forced_reannounce_goes_out_now_and_the_status_follows_each_answer() {
    // libtorrent torrent::force_tracker_request with ignore_min_interval,
    // and torrent::tracker_response, tracker_warning and
    // tracker_request_error for the entry's message, counts and fails.
    let (listener, url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    macro_rules! driving {
        ($f:expr) => {
            tokio::select! {
                _ = peers.next() => panic!("the tracker sent peers"),
                r = $f => r,
            }
        };
    }

    // Answered with a warning: working, the warning its message.
    let (conn, q) = driving!(read_announce(&listener));
    assert_eq!(query_param(&q, "event"), Some("started"));
    respond(
        conn,
        "d8:completei4e10:downloadedi7e10:incompletei2e8:intervali1800e12:min intervali900e15:warning message4:slow5:peers0:e",
    )
    .await;
    let table = table_once(&status, &mut peers, |a| a[0].last.is_some()).await;
    let last = table[0].last.clone().unwrap();
    let ok = last.result.clone().unwrap();
    assert_eq!(
        (
            ok.seeders,
            ok.leechers,
            ok.downloaded,
            ok.warning.as_deref()
        ),
        (Some(4), Some(2), Some(7), Some("slow"))
    );
    assert_eq!(table[0].fails, 0);
    assert_eq!(
        table[0].next_announce,
        Some(last.at + Duration::from_secs(1800))
    );

    // Forced, it goes out now, inside the min interval, with no event;
    // forced again while it waits, nothing more goes out.
    assert!(status.reannounce(Duration::ZERO, None, true));
    let (conn, q) = driving!(read_announce(&listener));
    assert_eq!(query_param(&q, "event"), None);
    assert!(status.announces()[0].in_flight);
    assert!(status.reannounce(Duration::ZERO, Some(&url), true));

    // Refused: not working, the reason its message, one failure.
    respond(conn, "d14:failure reason4:nopee").await;
    let table = table_once(&status, &mut peers, |a| a[0].fails == 1 && !a[0].in_flight).await;
    let error = table[0].last.clone().unwrap().result.unwrap_err();
    assert_eq!(error.kind, AnnounceErrorKind::Refused);
    assert!(error.message.contains("nope"), "{error}");
    assert!(table[0].next_announce.is_some());

    // Forced alone, answered again: working, failures cleared.
    assert!(status.reannounce(Duration::ZERO, Some(&url), true));
    let (conn, _) = driving!(read_announce(&listener));
    respond(conn, "d8:intervali1800e5:peers0:e").await;
    let table = table_once(&status, &mut peers, |a| {
        a[0].fails == 0 && a[0].last.as_ref().is_some_and(|l| l.result.is_ok())
    })
    .await;
    assert_eq!(table[0].last.clone().unwrap().result.unwrap().warning, None);

    drop(peers);
    next_announce(&listener).await;
    stopped.wait_idle().await;
    token.cancel();
}

#[tokio::test]
async fn trackers_added_replaced_and_removed_change_what_is_announced() {
    // libtorrent torrent::replace_trackers and add_tracker announce at
    // once to the trackers the walk now picks.
    let (a, a_url) = http_listener().await;
    let (b, b_url) = http_listener().await;
    let (c, c_url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(Vec::new(), &stopped).await;
    let TrackerTask { mut peers, status } = task;
    macro_rules! driving {
        ($f:expr) => {
            tokio::select! {
                _ = peers.next() => panic!("the tracker sent peers"),
                r = $f => r,
            }
        };
    }
    assert_eq!(status.announces(), []);

    // Added to a task with none, a is announced to.
    status.replace(op(vec![vec![a_url.clone()]]));
    let (conn, q) = driving!(read_announce(&a));
    assert_eq!(query_param(&q, "event"), Some("started"));
    respond(conn, "d8:intervali1800e5:peers0:e").await;
    table_once(&status, &mut peers, |t| t[0].last.is_some()).await;

    // b put ahead of it is announced to at once; a, kept, keeps its row.
    status.replace(op(vec![vec![b_url.clone()], vec![a_url.clone()]]));
    let urls = |t: &[TrackerAnnounce]| t.iter().map(|a| a.url.clone()).collect::<Vec<_>>();
    assert_eq!(
        urls(&status.announces()),
        [b_url.to_string(), a_url.to_string()]
    );
    assert!(status.announces()[1].last.is_some());
    let (conn, q) = driving!(read_announce(&b));
    assert_eq!(query_param(&q, "event"), Some("started"));

    // c replaces b while b's announce waits: it is abandoned, and c,
    // new, is asked.
    status.replace(op(vec![vec![c_url.clone()], vec![a_url.clone()]]));
    let closed = async {
        let mut byte = [0u8; 1];
        loop {
            conn.readable().await.unwrap();
            match conn.try_read(&mut byte) {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                read => break read.unwrap_or(0),
            }
        }
    };
    assert_eq!(driving!(closed), 0, "b's announce ended");
    let (conn, _) = driving!(read_announce(&c));
    assert_eq!(
        urls(&status.announces()),
        [c_url.to_string(), a_url.to_string()]
    );
    respond(conn, "d8:intervali1800e5:peers0:e").await;
    table_once(&status, &mut peers, |t| t[0].last.is_some()).await;

    // c removed, a alone is left; forced, it hears no second `started`.
    status.replace(op(vec![vec![a_url.clone()]]));
    assert_eq!(urls(&status.announces()), [a_url.to_string()]);
    assert!(!status.reannounce(Duration::ZERO, Some(&c_url), true));
    assert!(status.reannounce(Duration::ZERO, None, true));
    let (conn, q) = driving!(read_announce(&a));
    assert_eq!(query_param(&q, "event"), None);
    respond(conn, "d8:intervali1800e5:peers0:e").await;
    // Its answer is taken in: a tracker still waiting is owed no
    // `stopped`.
    table_once(&status, &mut peers, |t| !t[0].in_flight).await;

    drop(peers);
    next_announce(&a).await;
    stopped.wait_idle().await;
    drop((b, c));
    token.cancel();
}

#[tokio::test]
async fn a_tier_added_while_the_torrent_runs_is_announced_to_at_once_and_on_schedule() {
    // libtorrent torrent::add_tracker announces at once while the
    // torrent announces, and with announce_to_all_tiers, as qBittorrent
    // sets it, torrent::announce_with_tracker and update_tracker_timer
    // take one tracker of every tier, not only the first that works.
    let (a, a_url) = http_listener().await;
    let (b, b_url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![a_url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    macro_rules! driving {
        ($f:expr) => {
            tokio::select! {
                _ = peers.next() => panic!("the tracker sent peers"),
                r = $f => r,
            }
        };
    }
    let answered = "d8:intervali1800e12:min intervali900e5:peers0:e";

    let (conn, q) = driving!(read_announce(&a));
    assert_eq!(query_param(&q, "event"), Some("started"));
    respond(conn, answered).await;
    let table = table_once(&status, &mut peers, |t| t[0].last.is_some()).await;
    // What a's row says of its announces; its next one is read off the
    // clock each time the table is.
    let heard = |row: &TrackerAnnounce| (row.in_flight, row.last.clone(), row.fails);
    let a_row = heard(&table[0]);

    // b, a tier of its own added after a heard `started`, is announced
    // to at once though a works; a is not asked again.
    status.replace(op(vec![vec![a_url.clone()], vec![b_url.clone()]]));
    let (conn, q) = driving!(read_announce(&b));
    assert_eq!(query_param(&q, "event"), Some("started"));
    let table = status.announces();
    assert_eq!(heard(&table[0]), a_row);
    assert!(table[1].in_flight);
    respond(conn, answered).await;

    // Answered, b is due again after its interval, as a is after its own.
    let table = table_once(&status, &mut peers, |t| {
        t[1].last.is_some() && !t[1].in_flight
    })
    .await;
    assert_eq!(heard(&table[0]), a_row);
    let b_last = table[1].last.clone().unwrap();
    assert!(b_last.result.is_ok());
    assert_eq!(
        table[1].next_announce,
        Some(b_last.at + Duration::from_secs(1800))
    );

    // Both heard `started`, so both hear `stopped`.
    drop(peers);
    let (stopped_a, stopped_b) = tokio::join!(next_announce(&a), next_announce(&b));
    assert_eq!(query_param(&stopped_a, "event"), Some("stopped"));
    assert_eq!(query_param(&stopped_b, "event"), Some("stopped"));
    stopped.wait_idle().await;
    token.cancel();
}

#[test]
fn a_tier_added_while_announcing_comes_due_at_once_and_after_its_interval() {
    // libtorrent torrent::add_tracker, then update_tracker_timer with
    // announce_to_all_tiers: the new tier keeps its own schedule beside
    // the first's.
    let s = Duration::from_secs;
    let t0 = Instant::now();
    let mut list = list(&[&["a"]], t0);
    let stats = live(0, 90, false);
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &stats),
        [(http("a"), STARTED)]
    );
    list.on_success(&http("a"), answered(STARTED), t0, &ANSWERED);
    assert_eq!(list.next_due(), Some(t0 + s(1800)));

    let t1 = t0 + s(60);
    list.replace(&[vec![http("a")], vec![http("b")]], t1);
    assert_eq!(list.next_due(), Some(t1));
    assert_eq!(
        due(&mut list, t1, ToldPorts::default(), &stats),
        [(http("b"), STARTED)]
    );
    list.on_success(&http("b"), answered(STARTED), t1, &ANSWERED);

    assert_eq!(list.next_due(), Some(t0 + s(1800)));
    assert_eq!(
        due(&mut list, t0 + s(1800), ToldPorts::default(), &stats),
        [(http("a"), None)]
    );
    list.on_success(&http("a"), answered(None), t0 + s(1800), &ANSWERED);
    assert_eq!(list.next_due(), Some(t1 + s(1800)));
    assert_eq!(
        due(&mut list, t1 + s(1800), ToldPorts::default(), &stats),
        [(http("b"), None)]
    );
}

// The table as replacing its trackers built it when it looked each one up
// in the old rows: the reference the tests below compare with.
fn rows_replaced(rows: &[TrackerAnnounce], trackers: &[SupportedTracker]) -> Vec<TrackerAnnounce> {
    trackers
        .iter()
        .map(|t| {
            let url = t.url().as_str();
            rows.iter()
                .find(|a| a.url == url)
                .cloned()
                .unwrap_or_else(|| TrackerAnnounce {
                    url: url.to_owned(),
                    ..Default::default()
                })
        })
        .collect()
}

// A run's rows written after each answer by its tracker's row alone, when
// the others already show its list, hold what writing every row after
// each answer held, and change when it did: over seeded runs of one or two
// hashes (a hybrid) with many trackers in separate tiers, answers,
// failures, announces in flight, forces, completion, replaces and stops.
#[test]
fn rows_written_per_answer_match_every_row_rewritten() {
    let mut rng = 0x9b05_688c_2b3e_6c1fu64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    const NAMES: u64 = 60;
    let names: Vec<String> = (0..NAMES).map(|i| format!("t{i}")).collect();
    let tiers = |next: &mut dyn FnMut() -> u64| -> Vec<Vec<SupportedTracker>> {
        (0..8 + next() % 30)
            .map(|_| {
                (0..1 + next() % 3)
                    .map(|_| http(&names[(next() % NAMES) as usize]))
                    .collect()
            })
            .collect()
    };
    let stats = live(0, 90, false);
    let base = Instant::now();
    let base_at = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
    let mut publishes = 0;
    let mut one_row = 0;
    for run in 0..20 {
        let mut now = base;
        let first = tiers(&mut next);
        let hashes = 1 + (run % 2) as usize;
        let mut lists: Vec<TrackerList> = (0..hashes)
            .map(|_| {
                TrackerList::new(
                    &first,
                    now,
                    &crate::tracker::AnnounceShape::WEAVER,
                    short_of_a_seed(),
                )
            })
            .collect();
        let start: Vec<TrackerAnnounce> = rows_replaced(&[], &unique(first.iter().flatten()));
        let mut fast = start.clone();
        let mut full = start;
        let mut index = RowIndex::of(&fast);
        for step in 0..400 {
            let r = next();
            let k = (r >> 40) as usize % hashes;
            let tracker = http(&names[((r >> 8) % NAMES) as usize]);
            let at = base_at + now.duration_since(base);
            // The list `k` publishes, after `answered` ended or not.
            let mut publish =
                |lists: &mut Vec<TrackerList>,
                 fast: &mut Vec<TrackerAnnounce>,
                 full: &mut Vec<TrackerAnnounce>,
                 index: &mut RowIndex,
                 k: usize,
                 answered: Option<(&SupportedTracker, usize)>| {
                    let id = k as u64 + 1;
                    let one = answered.is_some() && index.writer == id && !lists[k].unpublished;
                    let changed = write_answered(fast, index, id, &mut lists[k], answered, now, at);
                    let list = &lists[k];
                    let reference = write_rows(
                        full,
                        &list.entries,
                        |e| e.tracker.url().as_str(),
                        |e| list.row_value(e, now, at),
                    );
                    assert_eq!(changed, reference, "run {run} step {step}");
                    assert_eq!(fast, full, "run {run} step {step}");
                    publishes += 1;
                    one_row += usize::from(one);
                };
            match r % 9 {
                0 | 1 => {
                    let i = lists[k].on_success(&tracker, answered(STARTED), now, &ANSWERED);
                    publish(
                        &mut lists,
                        &mut fast,
                        &mut full,
                        &mut index,
                        k,
                        i.map(|i| (&tracker, i)),
                    );
                }
                2 => {
                    let i = lists[k].on_failure(&tracker, now, Duration::from_secs(5));
                    publish(
                        &mut lists,
                        &mut fast,
                        &mut full,
                        &mut index,
                        k,
                        i.map(|i| (&tracker, i)),
                    );
                }
                3 => {
                    lists[k].start_due(now, ToldPorts::default(), &stats);
                }
                4 => {
                    lists[k].force(now, (r & 0x100 != 0).then_some(&tracker), r & 0x200 != 0);
                    publish(&mut lists, &mut fast, &mut full, &mut index, k, None);
                }
                5 if r & 0x400 != 0 => {
                    lists[k].completed(now);
                }
                5 => {
                    lists[k].stop();
                    publish(&mut lists, &mut fast, &mut full, &mut index, k, None);
                }
                6 if r & 0x400 != 0 => {
                    let to = tiers(&mut next);
                    let trackers = unique(to.iter().flatten());
                    fast = rows_replaced(&fast, &trackers);
                    full = rows_replaced(&full, &trackers);
                    index = RowIndex::of(&fast);
                    for k in 0..hashes {
                        lists[k].replace(&to, now);
                        publish(&mut lists, &mut fast, &mut full, &mut index, k, None);
                    }
                }
                _ => now += Duration::from_secs(next() % 900),
            }
        }
    }
    // The single-row path is taken often enough to be what the test checks.
    assert!(
        one_row * 8 > publishes,
        "{one_row} of {publishes} wrote one row"
    );
}
