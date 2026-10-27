// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Plain files, alone or under a PAR2 set, clean, damaged and lost.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    ARTICLE, Claim, Post, Scenario, assert_fetched_once, one_block_article, par2_set, par2_volumes,
    payload, repair_and_terminal_events, run, try_run,
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
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
}

#[tokio::test]
async fn one_damaged_article_under_par2_is_repaired() {
    let data = payload(4 * ARTICLE, 3);
    let scenario = Scenario::new(covered(&[("lantern.bin", data)], 2)).damaged(0, 1);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
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
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
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
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
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
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    let view = graph.graph();
    assert_eq!(view.blocks_reachable, 3);
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}

// Two damaged slices against a set of two blocks, one of them lost with its
// volume article. The graph counts what the lost article cost, fails the
// repair as one block reachable, files it under repair and delivers
// nothing.
#[tokio::test]
async fn a_lost_recovery_article_short_of_blocks_fails_as_repair() {
    let posts = covered(&[("lantern.bin", payload(4 * ARTICLE, 8))], 2);
    let lost = one_block_article(&posts[2]);
    let scenario = Scenario::new(posts)
        .damaged(0, 1)
        .damaged(0, 2)
        .lost(2, lost);
    let graph = run(&scenario).await;
    assert!(graph.refetched().is_empty());
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());

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
    // The set's files are its members, never its own volumes.
    assert_eq!(
        graph.failure.as_deref(),
        Some(
            "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern.bin): not repairable: \
             2 damaged slices, only 1 recovery blocks reachable"
        )
    );
}

// A damaged article no recovery covers, in the job's only file: the graph
// keeps the article and claims the file damaged, and since nothing is whole
// the job fails for the short file.
#[tokio::test]
async fn a_damaged_article_without_par2_fails_with_the_file_claimed_damaged() {
    let scenario =
        Scenario::new(vec![Post::plain("lantern.bin", payload(10 * ARTICLE, 9))]).damaged(0, 4);
    let graph = run(&scenario).await;
    assert!(graph.refetched().is_empty());
    // Nothing is whole: the job fails, naming the short file.
    assert_eq!(
        graph.failure.as_deref(),
        Some("no file was delivered whole; 1 short: lantern.bin")
    );
    // A failed job ships nothing; the graph's own claim keeps the damage.
    assert_eq!(graph.claims["lantern.bin"], Claim::Missing);
    assert_eq!(graph.graph().claims["lantern.bin"], Claim::Damaged);
}

// A damaged member no set covers: the graph leaves it a damaged leaf and
// delivers the rest repaired.
#[tokio::test]
async fn a_damaged_member_no_set_covers_is_a_damaged_leaf() {
    let covered_data = payload(3 * ARTICLE, 10);
    let mut posts = vec![
        Post::plain("lantern.bin", covered_data.clone()),
        Post::plain("beacon.bin", payload(3 * ARTICLE, 11)),
    ];
    posts.extend(par2_set("lantern", &[("lantern.bin", &covered_data)], 2));
    let scenario = Scenario::new(posts).damaged(1, 1);
    let graph = run(&scenario).await;
    assert!(graph.refetched().is_empty());
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
// back.
#[tokio::test]
async fn a_clean_par2_job_reads_none_of_its_files_back() {
    let scenario = Scenario::new(covered(
        &[
            ("lantern.bin", payload(4 * ARTICLE, 12)),
            ("beacon.bin", payload(2 * ARTICLE + 5, 13)),
        ],
        2,
    ));
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.reads, 0, "the graph read a member back");
    assert_eq!(graph.graph().outcomes, vec![Some(RepairOutcome::Clean)]);
}

// A PAR2 set rebuilds the one damaged article: the graph announces the
// repair once, with the slice it rebuilt, before the job completes.
#[tokio::test]
async fn a_par2_repair_is_announced_once_before_the_job_completes() {
    let data = payload(4 * ARTICLE, 21);
    let scenario = Scenario::new(covered(&[("lantern.bin", data)], 2)).damaged(0, 1);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        repair_and_terminal_events(&graph),
        vec!["RepairComplete 1".to_string(), "JobCompleted".to_string()]
    );
}

// Three damaged slices against two recovery blocks: the repair did not
// hold, so nothing announces one, and the job fails.
#[tokio::test]
async fn a_par2_set_short_of_blocks_announces_no_repair_and_fails() {
    let data = payload(4 * ARTICLE, 22);
    let scenario = Scenario::new(covered(&[("lantern.bin", data)], 2))
        .damaged(0, 0)
        .damaged(0, 1)
        .damaged(0, 2);
    let graph = run(&scenario).await;
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 3,
            reachable: 2
        })]
    );
    assert_eq!(
        repair_and_terminal_events(&graph),
        vec!["JobFailed".to_string()]
    );
}

// Twelve slices of `lantern.bin` zeroed before posting, so every article
// arrives with a good checksum and only the set's slice checksums can see
// the damage, against one recovery block.
// The set rules the repair impossible and reports the slices it
// proved bad, so the member is a damaged leaf the failed set covers:
// unrepaired, not delivered, and the job fails as a repair failure.
#[tokio::test]
async fn zeroed_slices_in_clean_articles_short_of_par2_recovery_fail_as_repair() {
    let original = payload(40 * ARTICLE, 0);
    let mut posted = original.clone();
    posted[20 * ARTICLE..32 * ARTICLE].fill(0);
    let mut posts = vec![Post::plain("lantern.bin", posted)];
    posts.extend(par2_set("lantern", &[("lantern.bin", &original)], 1));
    let graph = run(&Scenario::new(posts)).await;
    assert!(graph.refetched().is_empty());
    assert_eq!(
        graph.failure_kind,
        Some(HistoryFailureKind::Repair),
        "{:?}",
        graph.failure
    );
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());

    let view = graph.graph();
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 12,
            reachable: 1
        })]
    );
    assert_eq!(
        graph.failure.as_deref(),
        Some(
            "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern.bin): not repairable: \
             12 damaged slices, only 1 recovery blocks reachable"
        )
    );
    assert_eq!(view.claims["lantern.bin"], Claim::Damaged, "a damaged leaf");
    assert_eq!(view.delivery.unrepaired, ["lantern.bin"]);
    assert!(view.delivery.whole.is_empty(), "{:?}", view.delivery);
}

// A damaged slice under one volume of four blocks, the volume read front to
// back after the payload. The set waits on its blocks, and rules the moment
// the first of them is in: the three behind it, and the volume closing, are
// not what it waited for. Its repair goes out in that step, and what it came
// to is heard before the next article is served.
#[tokio::test]
async fn a_waiting_set_rules_in_the_step_its_first_block_arrives() {
    use super::harness::{JOB, admit, recovery_blocks, serve};
    use crate::pipeline::tests::{hear_graph_mail, retire_pipeline_database};

    let posts = covered(&[("lantern.bin", payload(4 * ARTICLE, 9))], 4);
    let volume = &posts[2];
    let needed = *recovery_blocks(volume)[0].last().unwrap();
    let articles = |post: &Post| post.bytes.len().div_ceil(ARTICLE) as u32;
    let (payload_articles, index_articles, volume_articles) =
        (articles(&posts[0]), articles(&posts[1]), articles(volume));
    assert!(needed + 1 < volume_articles, "blocks follow the first");
    let scenario = Scenario::new(posts).damaged(0, 1);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let order = (0..index_articles)
        .map(|ordinal| (1, ordinal))
        .chain([(2, 0)])
        .chain((0..payload_articles).map(|ordinal| (0, ordinal)))
        .chain((1..volume_articles).map(|ordinal| (2, ordinal)));
    let mut ruled = None;
    for article in order {
        serve(&mut pipeline, &scenario, article).await;
        hear_graph_mail(&mut pipeline, JOB).await;
        let outcomes: Vec<_> = pipeline.graph_jobs[&JOB]
            .recovery_summary()
            .iter()
            .map(|set| set.outcome)
            .collect();
        if outcomes == [Some(RepairOutcome::Repaired { slices: 1 })] {
            ruled = Some(article);
            break;
        }
    }
    assert_eq!(ruled, Some((2, needed)));
    retire_pipeline_database(pipeline).await;
}

// Every article of the payload lost, under a set with a block for each of
// its slices: the set asks for its volume and rebuilds the whole file from
// nothing, exactly as it rebuilds a file only some of whose articles came.
#[tokio::test]
async fn an_entirely_lost_payload_under_par2_is_rebuilt() {
    let scenario = Scenario::new(covered(&[("lantern.bin", payload(2 * ARTICLE, 23))], 2))
        .lost(0, 0)
        .lost(0, 1);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 2 })]
    );
}

// Every article of the index damaged, a payload article lost, and two
// intact recovery volumes: each volume carries the set's file descriptions
// beside its blocks, so the set is founded from them and rebuilds the file.
#[tokio::test]
async fn a_damaged_index_leaves_the_set_to_its_volumes() {
    let data = payload(6 * ARTICLE, 41);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_volumes("lantern", &[("lantern.bin", &data)], 4, 2));
    let index = posts[1].bytes.len().div_ceil(ARTICLE) as u32;
    let mut scenario = Scenario::new(posts).lost(0, 2);
    for ordinal in 0..index {
        scenario = scenario.damaged(1, ordinal);
    }
    let graph = try_run(&scenario).await.expect("the job settles");
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(graph.files["lantern beacon/lantern.bin"] == data);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}

// The index damaged and each recovery volume lost past its first article:
// the set is known from the blocks at the volumes' heads, but no packet that
// says what it describes is left, so it checked nothing. Every payload byte
// arrived, so the job completes.
#[tokio::test]
async fn a_set_with_no_description_left_checks_nothing() {
    let data = payload(6 * ARTICLE, 47);
    let scenario = undescribed(data.clone(), false);
    let graph = try_run(&scenario).await.expect("the job settles");
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(graph.files["lantern beacon/lantern.bin"] == data);
}

// The same with a payload article lost: nothing can rebuild it, so the job
// fails for the short file rather than waiting on a description.
#[tokio::test]
async fn a_set_with_no_description_left_fails_the_short_file() {
    let scenario = undescribed(payload(6 * ARTICLE, 43), true);
    let graph = try_run(&scenario).await.expect("the job settles");
    assert_eq!(
        graph.failure.as_deref(),
        Some("no file was delivered whole; 1 short: lantern.bin")
    );
}

fn undescribed(data: Vec<u8>, lose_payload: bool) -> Scenario {
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_volumes("lantern", &[("lantern.bin", &data)], 4, 2));
    let articles: Vec<u32> = posts
        .iter()
        .map(|post| post.bytes.len().div_ceil(ARTICLE) as u32)
        .collect();
    let mut scenario = Scenario::new(posts);
    if lose_payload {
        scenario = scenario.lost(0, 2);
    }
    for ordinal in 0..articles[1] {
        scenario = scenario.damaged(1, ordinal);
    }
    for (file, count) in articles.iter().enumerate().skip(2) {
        for ordinal in 1..*count {
            scenario = scenario.lost(file as u32, ordinal);
        }
    }
    scenario
}
