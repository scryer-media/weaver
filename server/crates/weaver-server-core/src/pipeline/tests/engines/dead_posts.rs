// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Dead posts fail fast, in two vocabularies. The usenet feed rules from
// the NZB and its servers alone: the first-article sample, then the post's
// health against its critical line, with a STAT round's projection naming
// what took it there. The graph rules from what its feed refused: a
// recovery set that can no longer reach enough blocks for the ranges it was
// refused rules impossible, and the job ends on its words.

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
    // Twelve files and no recovery: the post's health crosses its critical
    // line before the sample reaches its share, and the feed gives the post
    // up on whichever crosses first.
    let scenario = (0..10).fold(Scenario::new(files()), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    // The feed abandons the files that lost an article and keeps fetching
    // the others, which arrive whole.
    assert_eq!(graph.status, Some(crate::jobs::model::JobStatus::Partial));
    for name in ["lantern10.bin", "lantern11.bin"] {
        assert_eq!(graph.claims.get(name), Some(&Claim::Complete), "{name}");
    }
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
    // The sample rules the post dead; the one file whose first article came
    // is still fetched and arrives whole, so the job is partial.
    assert_eq!(graph.status, Some(crate::jobs::model::JobStatus::Partial), "{:?}", graph.failure);
    assert_eq!(graph.claims.get("lantern11.bin"), Some(&Claim::Complete));
}

#[tokio::test]
async fn a_sample_short_of_the_share_at_or_above_critical_leaves_the_post_to_its_recovery() {
    // Nine of twelve files lost at their first article, short of the
    // sample's share, beside a recovery set that keeps the post's health at
    // or above its critical line: the feed rules nothing, and the set
    // rebuilds what was lost.
    let payload_files: Vec<(String, Vec<u8>)> = (0..12u8)
        .map(|at| (format!("lantern{at:02}.bin"), payload(4 * ARTICLE, at)))
        .collect();
    let named: Vec<(&str, &[u8])> = payload_files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = payload_files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_set("lantern", &named, 24));
    let scenario = (0..9).fold(Scenario::new(posts), |scenario, file| {
        scenario.lost(file, 0)
    });
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
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

// The feed's vocabulary: a post taken down whole but for each file's first
// article. The sample finds every head, a STAT round finds nine of ten
// samples missing, and the post's health falls below the line a post with
// no recovery is held to, before the payload is fetched.
#[tokio::test]
async fn a_post_taken_down_fails_on_the_feeds_health_before_the_graph_rules() {
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
    assert!(failure.contains("below critical 85.0%"), "{failure}");
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Health));
    // No recovery object ruled: the feed gave the post up.
    assert!(graph.graph().outcomes.is_empty(), "{:?}", graph.graph().outcomes);
}

// The feed's vocabulary yields to the graph's: a post alive above its
// critical line whose PAR2 set of forty blocks is refused forty-one ranges.
// The set rules impossible on the forty-first and the job fails in its
// words, before the payload's tail is fetched.
#[tokio::test]
async fn a_par2_set_refused_one_range_past_its_blocks_fails_in_its_own_words() {
    let bytes = payload(200 * ARTICLE, 7);
    let mut posts = vec![Post::plain("lantern.bin", bytes.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", bytes.as_slice())], 40));
    let scenario = (1..=41).fold(Scenario::new(posts).leased(), |scenario, ordinal| {
        scenario.lost(0, ordinal)
    });
    let graph = run(&scenario).await;
    assert_eq!(
        graph.failure.as_deref(),
        Some(
            "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern.bin): not repairable: \
             41 damaged slices, only 40 recovery blocks reachable"
        )
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert!(!graph.requests.contains_key(&(0, 199)), "{:?}", graph.order);
}

// The graph's vocabulary: a stored RAR5 set of four volumes, three of them
// refused an article each, beside one `.rev` volume. The `.rev` set can
// rebuild one volume, so it rules impossible and the job fails in its words.
#[tokio::test]
async fn a_rev_set_refused_three_volumes_with_one_rev_rules_impossible() {
    let body = super::rar_rev::body();
    let volumes =
        crate::pipeline::tests::direct_store::single_member_store_set("beacon.bin", &body, 4);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let mut posts = super::rar::posts(volumes.clone());
    posts.extend(super::rar_rev::rar5_revs("silver.horizon", &bytes, 1));
    let scenario = (1..4u32).fold(Scenario::new(posts), |scenario, volume| {
        scenario.lost(volume, 1)
    });
    let graph = run(&scenario).await;
    let failure = graph.failure.as_deref().unwrap_or_default();
    assert!(failure.contains("not repairable: 3 damaged slices, only 1 recovery blocks reachable"), "{failure}");
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
}

// No recovery at all, every payload article refused: nothing can rebuild
// the file and the job fails.
#[tokio::test]
async fn a_post_with_no_recovery_whose_every_article_is_refused_fails() {
    let posts = vec![Post::plain("lantern.bin", payload(4 * ARTICLE, 7))];
    let scenario = (0..4).fold(Scenario::new(posts), |scenario, ordinal| {
        scenario.lost(0, ordinal)
    });
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(crate::jobs::model::JobStatus::Failed { .. })),
        "{:?} {:?}",
        graph.status,
        graph.failure
    );
}

// A whole archive volume no object opened is no usable file: its `.rar`
// lost, the `.r01` beside it arrives whole, and the job fails, not partial.
#[tokio::test]
async fn a_whole_volume_no_object_opened_is_not_a_usable_file() {
    let posts = vec![
        Post::plain("lantern.rar", payload(4 * ARTICLE, 3)),
        Post::plain("lantern.r01", payload(4 * ARTICLE, 4)),
    ];
    let scenario = (0..4).fold(Scenario::new(posts), |scenario, ordinal| {
        scenario.lost(0, ordinal)
    });
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(crate::jobs::model::JobStatus::Failed { .. })),
        "{:?} {:?}",
        graph.status,
        graph.failure
    );
}
