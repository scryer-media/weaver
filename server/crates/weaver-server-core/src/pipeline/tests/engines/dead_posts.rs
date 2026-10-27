// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Dead posts fail fast: the first-article sample rules a post dead once the
// first articles of most of its files are lost, before the rest of it is
// fetched, unless the recovery the job can still reach covers the files the
// sample lost. A recovery set that can no longer rebuild what the post lost
// fails it as soon as the losses outrun every block it can still reach. A
// post whose losses look like a release nobody uploaded is sampled by STAT,
// and fails on what the sample projects once that is past critical.

use super::harness::*;
use crate::history::HistoryFailureKind;

// Twelve files of four articles each.
fn files() -> Vec<Post> {
    (0..12u8)
        .map(|at| Post::plain(&format!("lantern{at:02}.bin"), payload(4 * ARTICLE, at)))
        .collect()
}

#[tokio::test]
async fn a_post_whose_first_articles_are_lost_fails_before_its_payload_is_fetched() {
    let scenario = (0..10).fold(Scenario::new(files()), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    assert_eq!(
        graph.failure.as_deref(),
        Some("aborted: 10 of 12 first articles are missing, the post cannot complete")
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Health));
    // The first articles lead, and the tenth loss ends the job: nothing past
    // them was asked for.
    let asked: Vec<(u32, u32)> = graph.requests.keys().copied().collect();
    assert_eq!(asked, (0..10).map(|file| (file, 0)).collect::<Vec<_>>());
}

#[tokio::test]
async fn the_sample_is_the_payloads_first_articles_and_not_its_recoverys() {
    // Eleven of twelve files lost at their first article, beside a recovery
    // set of four blocks in four volumes whose first articles all came, far
    // too small to rebuild them. The sample is the twelve files and the
    // index; the recovery volumes would have diluted it below the share.
    let payload_files: Vec<(String, Vec<u8>)> = (0..12u8)
        .map(|at| (format!("lantern{at:02}.bin"), payload(512 * ARTICLE, at)))
        .collect();
    let named: Vec<(&str, &[u8])> = payload_files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = payload_files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    let recovery = par2_volumes_sliced("lantern", &named, 16 * ARTICLE as u64, 4, 4);
    assert_eq!(recovery.len(), 5, "an index and four volumes");
    posts.extend(recovery);
    let scenario = (0..11).fold(Scenario::new(posts), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    assert_eq!(
        graph.failure.as_deref(),
        Some("aborted: 11 of 13 first articles are missing, the post cannot complete")
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Health));
}

#[tokio::test]
async fn a_sample_short_of_the_share_leaves_the_post_to_its_recovery() {
    let scenario = (0..9).fold(Scenario::new(files()), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    assert!(
        !graph
            .failure
            .as_deref()
            .is_some_and(|failure| failure.contains("first articles are missing")),
        "{:?}",
        graph.failure
    );
    assert!(graph.requests.contains_key(&(11, 3)));
}

#[tokio::test]
async fn first_articles_the_recovery_covers_leave_the_post_to_its_repair() {
    let payload_files: Vec<(String, Vec<u8>)> = (0..10u8)
        .map(|at| (format!("lantern{at:02}.bin"), payload(ARTICLE, at)))
        .collect();
    let named: Vec<(&str, &[u8])> = payload_files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = payload_files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_set("lantern", &named, 12));
    let scenario = (0..10).fold(Scenario::new(posts), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    for (name, _) in &payload_files {
        assert_eq!(graph.claims.get(name), Some(&Claim::Complete), "{name}");
    }
}

#[tokio::test]
async fn losses_past_every_reachable_recovery_block_fail_before_the_payload_is_fetched() {
    // Sixty slices of payload, fifteen recovery blocks, and twenty slices
    // lost from the second article on, under the scheduler's leases.
    let bytes = payload(60 * ARTICLE, 7);
    let mut posts = vec![Post::plain("lantern.bin", bytes.clone())];
    posts.extend(par2_set(
        "lantern",
        &[("lantern.bin", bytes.as_slice())],
        15,
    ));
    let scenario = (1..=20).fold(Scenario::new(posts).leased(), |scenario, ordinal| {
        scenario.lost(0, ordinal)
    });
    let graph = run(&scenario).await;
    // The set's own words. The post's health is still above its critical
    // line, a third of the payload lost against a recovery set a quarter
    // its size, so the failure says nothing of it.
    assert_eq!(
        graph.failure.as_deref(),
        Some(
            "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern.bin): not repairable: \
             20 damaged slices, only 15 recovery blocks reachable"
        )
    );
    // A set that cannot rebuild what it covers is a repair failure, as it
    // is when it rules once the post is in.
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    // The losses end the job while the payload's last thirty articles are
    // still to fetch, and none of them was asked for.
    let asked: Vec<(u32, u32)> = graph.requests.keys().copied().collect();
    assert!(
        asked
            .iter()
            .all(|&(file, ordinal)| file != 0 || ordinal < 30),
        "{asked:?}"
    );
}

#[tokio::test]
async fn a_post_whose_every_article_is_gone_fails_below_critical() {
    // Four payload files and a recovery set of fifteen blocks in four
    // volumes: too few files for the sample to rule, and every article
    // deleted, so no recovery block can be had. The payload outweighs its
    // recovery as a real post's does, so the health has a critical line.
    let payload_files: Vec<(String, Vec<u8>)> = (0..4u8)
        .map(|at| (format!("lantern{at:02}.bin"), payload(512 * ARTICLE, at)))
        .collect();
    let named: Vec<(&str, &[u8])> = payload_files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = payload_files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_volumes_sliced(
        "lantern",
        &named,
        16 * ARTICLE as u64,
        15,
        4,
    ));
    let articles: Vec<(u32, u32)> = posts
        .iter()
        .zip(0u32..)
        .flat_map(|(post, file)| {
            (0..post.bytes.len().div_ceil(ARTICLE) as u32).map(move |ordinal| (file, ordinal))
        })
        .collect();
    let scenario = articles.iter().fold(
        Scenario::new(posts).leased(),
        |scenario, &(file, ordinal)| scenario.lost(file, ordinal),
    );
    let graph = run(&scenario).await;
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(failure.starts_with("health "), "{failure}");
    assert!(failure.contains("below critical"), "{failure}");
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Health));
}

#[tokio::test]
async fn a_release_posted_as_its_first_articles_alone_fails_on_its_probe_before_the_rest_is_fetched()
 {
    // Three files of forty articles and no recovery, every article but each
    // file's first gone: the sample finds every head, so only a probe can
    // tell the post is not coming.
    let posts: Vec<Post> = (0..3u8)
        .map(|at| Post::plain(&format!("lantern{at:02}.bin"), payload(40 * ARTICLE, at)))
        .collect();
    let scenario = (0..3u32)
        .flat_map(|file| (1..40u32).map(move |ordinal| (file, ordinal)))
        .fold(Scenario::new(posts), |scenario, (file, ordinal)| {
            scenario.lost(file, ordinal)
        });
    let graph = run(&scenario).await;
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(
        failure.starts_with("health probe: 9 of 10 samples missing; health "),
        "{failure}"
    );
    assert!(failure.contains("below critical"), "{failure}");
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Health));
    // The probe asked by STAT alone: of the hundred and twenty articles, the
    // download asked for no more than it had when the probe was armed.
    assert!(graph.order.len() < 40, "{:?}", graph.order);
}

#[tokio::test]
async fn a_post_with_no_recovery_fails_at_critical_before_the_rest_is_fetched() {
    // One file of sixty articles and no recovery, every other article gone:
    // half of what is asked lands, so it does not look like a release nobody
    // uploaded and no probe is armed. With nothing that could rebuild the
    // file, its losses fail the post once its health is at or below
    // critical, long before its last article is asked for.
    let posts = vec![Post::plain("lantern.bin", payload(60 * ARTICLE, 7))];
    let scenario = (1..60)
        .step_by(2)
        .fold(Scenario::new(posts), |scenario, ordinal| {
            scenario.lost(0, ordinal)
        });
    let graph = run(&scenario).await;
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(failure.starts_with("health "), "{failure}");
    assert!(failure.contains("below critical 85.0%"), "{failure}");
    // The losses rule it, not a probe's projection.
    assert!(!failure.starts_with("health probe"), "{failure}");
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Health));
    assert!(
        !graph.requests.contains_key(&(0, 59)),
        "{failure} {:?}",
        graph.order
    );
}

#[tokio::test]
async fn a_post_whose_only_recovery_is_par3_defers_to_it_below_critical() {
    // Two files of ten articles beside a PAR3 set of twelve blocks, three
    // articles of each lost: a probe projects the post below the 85% a post
    // without PAR2 bytes is held to. The PAR3 set is recovery the post
    // declares and it reaches past the loss, so the verdict waits on it,
    // and it rebuilds both files.
    let payload_files: Vec<(String, Vec<u8>)> = (0..2u8)
        .map(|at| (format!("lantern{at:02}.bin"), payload(10 * ARTICLE, at)))
        .collect();
    let named: Vec<(&str, &[u8])> = payload_files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = payload_files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par3_set("lantern", &named, 12));
    let scenario = (0..2u32)
        .flat_map(|file| [2, 5, 8].map(|ordinal| (file, ordinal)))
        .fold(Scenario::new(posts), |scenario, (file, ordinal)| {
            scenario.lost(file, ordinal)
        });
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    for (name, _) in &payload_files {
        assert_eq!(graph.claims.get(name), Some(&Claim::Complete), "{name}");
    }
}
