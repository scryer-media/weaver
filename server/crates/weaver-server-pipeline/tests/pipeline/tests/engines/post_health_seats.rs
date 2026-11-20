// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A health probe's STAT batches are lane work. Each batch waits for a free
// seat on a server it has not asked, the seat an article lease would take,
// and is asked on that lane between leases. Here the post is a release
// posted as its first articles alone, and its one server is a listener this
// test owns that holds exactly those: the batches go to it over the download
// lanes, and what they find rules the post dead.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::harness::{ARTICLE, Post, Scenario, payload};
use weaver_server_pipeline::pipeline::tests::engines::post_health_seats::{
    armed, bare, batch_unfound, dispatch, failed_post, hear_one, hear_round, held, idle_lanes,
    known_gone, nntp, probe_out, rounds, stat_server, unasked_batches, waiting_batches,
    waits_for_seat,
};

// Three files of `articles` articles each, every article but each file's
// first gone.
fn first_articles_alone(articles: u32) -> Scenario {
    let posts: Vec<Post> = (0..3u8)
        .map(|at| {
            Post::plain(
                &format!("lantern{at:02}.bin"),
                payload(articles as usize * ARTICLE, at),
            )
        })
        .collect();
    (0..3u32)
        .flat_map(|file| (1..articles).map(move |ordinal| (file, ordinal)))
        .fold(Scenario::new(posts), |scenario, (file, ordinal)| {
            scenario.lost(file, ordinal)
        })
}

// Every seat busy: the batch waits for one rather than giving up, and a lane
// on that server yields at its next refill. Once the seat frees, the batch
// runs on it and the post is ruled dead on what it found.
#[tokio::test]
async fn a_probe_waits_for_a_busy_seat_and_rules_the_post_dead_once_it_frees() {
    let server = stat_server();
    let scenario = first_articles_alone(40);
    let (_temp_dir, mut pipeline) = armed(&scenario, server.port()).await;

    // A lane mid-transfer holds the server's one connection. Lanes dial
    // off the runtime, so this one does too.
    let nntp = Arc::clone(nntp(&pipeline));
    let busy = std::thread::spawn(move || {
        nntp.try_acquire_blocking_body_lane_with_estimate(&[], weaver_nntp::ServerSet::EMPTY, 0)
            .expect("the server seats the busy lane")
    })
    .join()
    .unwrap();
    let waiting = waiting_batches(&pipeline);
    assert_eq!(waiting, 1, "ten articles sampled, one batch");
    for _ in 0..3 {
        dispatch(&mut pipeline);
    }
    assert_eq!(waiting_batches(&pipeline), waiting);
    assert!(
        unasked_batches(&pipeline) == waiting,
        "no batch is answered without a seat"
    );
    assert!(probe_out(&pipeline), "the round waits, unanswered");
    assert!(
        waits_for_seat(&pipeline, 0),
        "a lane on the busy server yields its seat at its next refill"
    );

    // The lane finishes its transfer and its seat is free.
    std::thread::spawn(move || busy.park()).join().unwrap();
    hear_round(&mut pipeline).await;

    let failure = failed_post(&pipeline).expect("the round rules the post dead");
    assert!(
        failure.starts_with("health probe: 9 of 10 samples missing; health "),
        "{failure}"
    );
    assert_eq!(
        server.asked.lock().unwrap().len(),
        10,
        "every sample asked once"
    );
}

// A server held back from new connections seats no batch, not even on the
// connection a lane already keeps there: that seat waits for the hold to
// lift, and no lane there is yielded for it.
#[tokio::test]
async fn a_probe_never_takes_a_seat_on_a_held_server() {
    let server = stat_server();
    // Six hundred and sixty articles sample to fifty-five, two batches.
    let scenario = first_articles_alone(220);
    let (_temp_dir, mut pipeline) = armed(&scenario, server.port()).await;
    assert_eq!(waiting_batches(&pipeline), 2);

    // The one seat takes the first batch; the second waits for it.
    dispatch(&mut pipeline);
    assert_eq!(waiting_batches(&pipeline), 1);
    hear_one(&mut pipeline).await;
    assert_eq!(
        idle_lanes(&pipeline, 0),
        Some(1),
        "the lane that answered keeps its connection, idle"
    );

    // The server is quarantined, waiting on its recovery probe.
    let recovery = nntp(&pipeline).pool().take_recovery_probe_for_test(0);
    let mut health = nntp(&pipeline).pool().server_health_changes();
    for _ in 0..3 {
        dispatch(&mut pipeline);
    }
    assert_eq!(
        unasked_batches(&pipeline),
        1,
        "the second batch is not seated on the held server's idle connection"
    );
    assert!(probe_out(&pipeline));
    assert!(!waits_for_seat(&pipeline, 0));
    assert_eq!(
        server.asked.lock().unwrap().len(),
        50,
        "only the first batch was asked"
    );

    recovery.answer();
    health.changed().await;
    assert_eq!(nntp(&pipeline).pool().connection_hold(0, true), None);
    hear_round(&mut pipeline).await;

    let failure = failed_post(&pipeline).expect("the round rules the post dead");
    assert!(
        failure.starts_with("health probe: 54 of 55 samples missing; health "),
        "{failure}"
    );
    assert_eq!(server.asked.lock().unwrap().len(), 55);
}

// A batch the server leaves unanswered costs the round that batch alone: it
// is not asked of that server again, and with no other server to go to it
// is folded in unsettled. The batch beside it answered, and the round rules
// on what that batch found.
#[tokio::test]
async fn a_batch_the_server_leaves_unanswered_costs_the_round_that_batch_alone() {
    let server = stat_server();
    // Six hundred and sixty articles sample to fifty-five, two batches.
    let scenario = first_articles_alone(220);
    let (_temp_dir, mut pipeline) = armed(&scenario, server.port()).await;
    assert_eq!(waiting_batches(&pipeline), 2);
    let unanswered: BTreeSet<String> = batch_unfound(&pipeline, 0)
        .iter()
        .map(|id| bare(id).to_string())
        .collect();
    let answered = batch_unfound(&pipeline, 1);
    server
        .silent
        .lock()
        .unwrap()
        .extend(unanswered.iter().cloned());

    hear_round(&mut pipeline).await;

    let missed = answered.iter().filter(|id| !held(id)).count();
    let failure =
        failed_post(&pipeline).expect("the round rules the post dead on the batch that answered");
    assert!(
        failure.starts_with(&format!(
            "health probe: {missed} of {} samples missing; health ",
            answered.len()
        )),
        "{failure}"
    );
    let asked = server.asked.lock().unwrap();
    assert_eq!(
        asked
            .iter()
            .filter(|id| unanswered.contains(bare(id)))
            .count(),
        0,
        "the unanswered batch is never answered, nor asked again"
    );
    assert_eq!(asked.len(), answered.len());
}

// A post nobody uploaded but each file's first article, whose first losses
// fall where the round samples: the round steps over what every server
// already said is gone and asks the next article of each window instead.
// The post is still found dead on its first round, on as many samples as
// before, and nothing known gone is asked again.
#[tokio::test]
async fn a_dead_post_whose_known_losses_fall_in_the_sample_is_dead_on_its_first_round() {
    let server = stat_server();
    let articles = 40u32;
    let posts: Vec<Post> = (0..3u8)
        .map(|at| {
            Post::plain(
                &format!("lantern{at:02}.bin"),
                payload(articles as usize * ARTICLE, at),
            )
        })
        .collect();
    // A hundred and twenty articles sample one in twelve: these come first.
    let picked = [(0, 12), (0, 24), (0, 36)];
    let rest = (0..3u32)
        .flat_map(|file| (1..articles).map(move |ordinal| (file, ordinal)))
        .filter(|lost| !picked.contains(lost));
    let scenario = picked
        .into_iter()
        .chain(rest)
        .fold(Scenario::new(posts), |scenario, (file, ordinal)| {
            scenario.lost(file, ordinal)
        });
    let (_temp_dir, mut pipeline) = armed(&scenario, server.port()).await;
    let known: BTreeSet<String> = scenario
        .lost
        .iter()
        .filter(|(file, ordinal)| known_gone(&pipeline, *file, *ordinal))
        .map(|(file, ordinal)| format!("lantern-{file}-{ordinal}@example.com"))
        .collect();
    assert!(known.contains("lantern-0-12@example.com"), "{known:?}");

    hear_round(&mut pipeline).await;

    let failure = failed_post(&pipeline).expect("the round rules the post dead");
    assert!(
        failure.starts_with("health probe: 9 of 10 samples missing; health "),
        "{failure}"
    );
    assert_eq!(rounds(&pipeline), 1);
    let asked = server.asked.lock().unwrap();
    assert_eq!(asked.len(), 10, "{asked:?}");
    assert!(
        asked.iter().all(|id| !known.contains(bare(id))),
        "{asked:?} {known:?}"
    );
}
