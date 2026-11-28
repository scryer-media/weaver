// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// RAR recovery volumes beside a PAR2 set that covers them and the RAR
// volumes: the recovery volumes are fetched, so the PAR2 set can rule on
// every member it covers, and the job ends on that ruling, not by the stall
// path.

use super::harness::*;
use super::rar::{body, posts, rar5_revs};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::single_member_store_set;
use weaver_graph::recovery::RepairOutcome;

use weaver_server_pipeline::pipeline::tests::engines::stall_fixtures::{stall_lines, warnings};

// A stored four-volume RAR5 set, one `.rev` over it, then a PAR2 index and
// volume over the four volumes and the `.rev`.
fn covered() -> (Scenario, u32) {
    let volumes = single_member_store_set("beacon.bin", &body(), 4);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 1);
    let mut files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    files.extend(
        revs.iter()
            .map(|rev| (rev.name.as_str(), rev.bytes.as_slice())),
    );
    let par2 = par2_set("silver.horizon", &files, 2);
    let mut all = posts(volumes.clone());
    let rev = all.len() as u32;
    all.extend(revs);
    all.extend(par2);
    (Scenario::new(all), rev)
}

// Nothing damaged: the `.rev` is fetched, the PAR2 set rules clean over all
// five, and the job completes with no stall line.
#[tokio::test]
async fn a_rev_a_par2_set_covers_is_fetched_and_the_par2_set_rules() {
    let (scenario, rev) = covered();
    let (lines, guard) = warnings();
    let outcome = try_run(&scenario).await;
    drop(guard);
    let outcome = outcome
        .unwrap_or_else(|stall| panic!("job stalled at {:?}: {}", stall.status, stall.state));
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.failure
    );
    let articles = scenario.posts[rev as usize]
        .articles(scenario.article)
        .count() as u32;
    assert_eq!(
        fetched(&outcome, rev),
        articles,
        "{:?}",
        outcome.graph().rulings
    );
    assert!(stall_lines(&lines).is_empty(), "{:?}", stall_lines(&lines));
    assert!(
        outcome
            .graph()
            .rulings
            .iter()
            .any(|(kind, ruling)| kind == "par2"
                && matches!(
                    ruling,
                    Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
                )),
        "{:?}",
        outcome.graph().rulings
    );
}

// The PAR2 index asked for only after the `.rev` was claimed and the rest of
// it parked: once the index says the `.rev` is covered, the parked articles
// are asked for, and the job ends the same.
#[tokio::test]
async fn a_rev_parked_before_the_par2_index_arrives_is_fetched_once_it_does() {
    let (scenario, rev) = covered();
    let index = rev + 1;
    let scenario = scenario.batched(1);
    let (lines, guard) = warnings();
    let outcome = try_run(&scenario).await;
    drop(guard);
    let outcome = outcome
        .unwrap_or_else(|stall| panic!("job stalled at {:?}: {}", stall.status, stall.state));
    let at = |article: (u32, u32)| outcome.order.iter().position(|asked| *asked == article);
    let first_index = at((index, 0)).expect("the PAR2 index is asked for");
    assert!(
        at((rev, 0)).is_some_and(|front| front < first_index),
        "the `.rev` front before the index: {:?}",
        outcome.order
    );
    assert!(
        (1..fetched(&outcome, rev)).all(|ordinal| at((rev, ordinal)) > Some(first_index)),
        "the rest of the `.rev` after the index: {:?}",
        outcome.order
    );
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.failure
    );
    let articles = scenario.posts[rev as usize]
        .articles(scenario.article)
        .count() as u32;
    assert_eq!(
        fetched(&outcome, rev),
        articles,
        "{:?}",
        outcome.graph().rulings
    );
    assert!(stall_lines(&lines).is_empty(), "{:?}", stall_lines(&lines));
    assert!(par2_ruled(&outcome, |ruling| matches!(
        ruling,
        RepairOutcome::Clean | RepairOutcome::Repaired { .. }
    )));
}

// No PAR2 set over the `.rev`: nothing changes for it. It stays parked past
// its front, the RAR recovery set rules clean, and the job completes.
#[tokio::test]
async fn a_rev_no_set_covers_stays_parked() {
    let volumes = single_member_store_set("beacon.bin", &body(), 4);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 1);
    let mut all = posts(volumes.clone());
    let rev = all.len() as u32;
    all.extend(revs);
    let scenario = Scenario::new(all);
    let outcome = try_run(&scenario)
        .await
        .unwrap_or_else(|stall| panic!("job stalled at {:?}: {}", stall.status, stall.state));
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.failure
    );
    let articles = scenario.posts[rev as usize]
        .articles(scenario.article)
        .count() as u32;
    assert!(articles > 1, "the `.rev` spans {articles} articles");
    assert_eq!(fetched(&outcome, rev), 1, "{:?}", outcome.order);
}

// A damaged article of the `.rev`, with enough PAR2 blocks over it: PAR2
// rules and repairs it, and the job completes.
#[tokio::test]
async fn a_damaged_rev_article_par2_covers_is_repaired() {
    let (scenario, rev) = covered();
    let scenario = scenario.damaged(rev, 1);
    let outcome = try_run(&scenario)
        .await
        .unwrap_or_else(|stall| panic!("job stalled at {:?}: {}", stall.status, stall.state));
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.failure
    );
    assert!(
        par2_ruled(&outcome, |ruling| matches!(
            ruling,
            RepairOutcome::Repaired { .. }
        )),
        "{:?}",
        outcome.graph().rulings
    );
}

fn par2_ruled(outcome: &Outcome, ruled: impl Fn(&RepairOutcome) -> bool) -> bool {
    outcome
        .graph()
        .rulings
        .iter()
        .any(|(kind, ruling)| kind == "par2" && ruling.as_ref().is_some_and(&ruled))
}

// The articles of post `file` the job asked for.
fn fetched(outcome: &Outcome, file: u32) -> u32 {
    outcome
        .requests
        .keys()
        .filter(|(post, _)| *post == file)
        .count() as u32
}
