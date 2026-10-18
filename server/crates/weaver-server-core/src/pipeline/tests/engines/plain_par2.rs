// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Plain files, alone or under a PAR2 set, clean, damaged and lost.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    ARTICLE, Claim, Post, Scenario, assert_same, one_block_article, par2_set, payload, run_both,
};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;

// Plain files followed by one PAR2 set over them, the index then the
// volume; the files are numbered as listed, the index and volume after.
fn covered(files: &[(&str, Vec<u8>)], blocks: u32) -> Vec<Post> {
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_set("lantern", &described, blocks));
    posts
}

fn claims_all(claims: &std::collections::BTreeMap<String, Claim>, expected: Claim) -> bool {
    !claims.is_empty() && claims.values().all(|claim| *claim == expected)
}

#[tokio::test]
async fn plain_files_clean() {
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", payload(3 * ARTICLE, 1)),
        Post::plain("beacon.dat", payload(2 * ARTICLE - 9, 2)),
    ]);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
}

#[tokio::test]
async fn one_damaged_article_under_par2_is_repaired() {
    let data = payload(4 * ARTICLE, 3);
    let scenario = Scenario::new(covered(&[("lantern.bin", data)], 2)).damaged(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}

#[tokio::test]
async fn two_damaged_articles_across_two_files_under_one_set_are_repaired() {
    let scenario = Scenario::new(covered(
        &[
            ("lantern.bin", payload(3 * ARTICLE, 4)),
            ("beacon.bin", payload(3 * ARTICLE, 5)),
        ],
        3,
    ))
    .damaged(0, 1)
    .damaged(1, 2);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 2 })]
    );
}

#[tokio::test]
async fn a_lost_article_under_par2_is_repaired_with_its_blocks_reported() {
    let scenario =
        Scenario::new(covered(&[("lantern.bin", payload(4 * ARTICLE, 6))], 2)).lost(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    let view = graph.graph();
    assert_eq!(view.blocks_reachable, 2);
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}

#[tokio::test]
async fn a_lost_recovery_article_lowers_the_blocks_reachable_and_the_repair_succeeds() {
    let posts = covered(&[("lantern.bin", payload(4 * ARTICLE, 7))], 4);
    let lost = one_block_article(&posts[2]);
    let scenario = Scenario::new(posts).damaged(0, 2).lost(2, lost);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    let view = graph.graph();
    assert_eq!(view.blocks_reachable, 3);
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}

// Divergence: the graph counts what the lost volume article cost and fails
// the repair as a PAR2 failure does; classic reports the job complete with
// the posted bytes. That classic outcome is recorded, not explained: losing
// the volume's article 1 or 1 and 2 leaves classic complete, losing article
// 9 fails it on one advertised block.
#[tokio::test]
async fn a_lost_recovery_article_short_of_blocks_fails_under_graph_and_completes_under_classic() {
    let posts = covered(&[("lantern.bin", payload(4 * ARTICLE, 8))], 2);
    let lost = one_block_article(&posts[2]);
    let scenario = Scenario::new(posts)
        .damaged(0, 1)
        .damaged(0, 2)
        .lost(2, lost);
    let (classic, graph) = run_both(&scenario).await;
    for outcome in [&classic, &graph] {
        assert!(outcome.refetched().is_empty(), "{}", outcome.engine);
    }
    assert!(classic.completed(), "{:?}", classic.failure);
    assert!(claims_all(&classic.claims, Claim::Complete));

    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    let view = graph.graph();
    assert_eq!(view.blocks_reachable, 1);
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 2,
            reachable: 1
        })]
    );
    assert_eq!(view.claims["lantern.bin"], Claim::Damaged, "a damaged leaf");
    assert!(
        graph
            .failure
            .as_deref()
            .is_some_and(|error| error.starts_with("PAR2 recovery failed")),
        "{:?}",
        graph.failure
    );
}

// A damaged article no recovery covers, in the job's only file. Divergence:
// the graph keeps the article and claims the file damaged, and since nothing
// is whole the job fails for the short file; classic fails the download.
#[tokio::test]
async fn a_damaged_article_without_par2_fails_with_the_file_claimed_damaged_under_graph() {
    let scenario =
        Scenario::new(vec![Post::plain("lantern.bin", payload(10 * ARTICLE, 9))]).damaged(0, 4);
    let (classic, graph) = run_both(&scenario).await;
    for outcome in [&classic, &graph] {
        assert!(outcome.refetched().is_empty(), "{}", outcome.engine);
    }
    assert!(
        classic
            .failure
            .as_deref()
            .is_some_and(|error| error.starts_with("download incomplete")),
        "{:?}",
        classic.failure
    );
    // Nothing is whole: the job fails, naming the short file.
    assert_eq!(
        graph.failure.as_deref(),
        Some("no file was delivered whole; 1 short: lantern.bin")
    );
    // A failed job ships nothing; the graph's own claim keeps the damage.
    assert_eq!(graph.claims["lantern.bin"], Claim::Missing);
    assert_eq!(graph.graph().claims["lantern.bin"], Claim::Damaged);
}

// Divergence: a damaged member no set covers. Classic refuses the job for
// it; the graph leaves it a damaged leaf and delivers the rest repaired.
#[tokio::test]
async fn a_damaged_member_no_set_covers_is_refused_under_classic_and_a_damaged_leaf_under_graph() {
    let covered_data = payload(3 * ARTICLE, 10);
    let mut posts = vec![
        Post::plain("lantern.bin", covered_data.clone()),
        Post::plain("beacon.bin", payload(3 * ARTICLE, 11)),
    ];
    posts.extend(par2_set("lantern", &[("lantern.bin", &covered_data)], 2));
    let scenario = Scenario::new(posts).damaged(1, 1);
    let (classic, graph) = run_both(&scenario).await;
    for outcome in [&classic, &graph] {
        assert!(outcome.refetched().is_empty(), "{}", outcome.engine);
    }
    assert!(
        classic
            .failure
            .as_deref()
            .is_some_and(|error| error.starts_with(
                "retained damaged article bytes require verification or repair: beacon.bin"
            )),
        "{:?}",
        classic.failure
    );
    // The damaged leaf beside the whole member makes the job Partial.
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(graph.claims["lantern.bin"], Claim::Complete);
    assert_eq!(graph.claims["beacon.bin"], Claim::Damaged);
    let view = graph.graph();
    assert_eq!(view.claims["lantern.bin"], Claim::Complete);
    assert_eq!(view.claims["beacon.bin"], Claim::Damaged, "a damaged leaf");
}

// The grid: every article of a clean PAR2 job arrived with a verified
// checksum, so the graph rules on it from what passed and reads nothing
// back. Classic's figure is what it is.
#[tokio::test]
async fn a_clean_par2_job_reads_none_of_its_files_back_under_graph() {
    let scenario = Scenario::new(covered(
        &[
            ("lantern.bin", payload(4 * ARTICLE, 12)),
            ("beacon.bin", payload(2 * ARTICLE + 5, 13)),
        ],
        2,
    ));
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.reads, 0, "the graph read a member back");
    assert_eq!(graph.volume_samples(&classic), vec![(3, 0)]);
    assert_eq!(graph.graph().outcomes, vec![Some(RepairOutcome::Clean)]);
}
