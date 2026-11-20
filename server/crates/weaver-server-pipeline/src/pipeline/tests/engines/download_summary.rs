// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The download summary a finished job's history row keeps, read back as
// stored: plain files, one RAR, a RAR in volumes, and a RAR repaired by the
// PAR2 beside it. The posts are the ones the RAR rows already run.

use super::harness::{ARTICLE, Outcome, Post, Scenario, payload, run};
use super::rar::{posts, with_par2};
use crate::jobs::download_summary::{DownloadSummary, Layout, NodeMode};
use crate::pipeline::tests::direct_store::single_member_store_set;

const MEMBER: &str = "beacon.bin";

fn body() -> Vec<u8> {
    payload(7 * ARTICLE + 13, 51)
}

pub(super) fn summary(outcome: &Outcome) -> DownloadSummary {
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    let text = outcome
        .download_summary
        .as_deref()
        .expect("the history row keeps a summary");
    DownloadSummary::from_storage_json(text).expect("a summary this build reads")
}

#[tokio::test]
async fn plain_files_keep_a_plain_summary_with_no_tree() {
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", payload(3 * ARTICLE, 1)),
        Post::plain("beacon.dat", payload(2 * ARTICLE - 9, 2)),
    ]);
    let summary = summary(&run(&scenario).await);
    assert_eq!(summary.layout, Some(Layout::Plain));
    assert!(summary.nodes.is_empty(), "{:?}", summary.nodes);
    assert_eq!((summary.node_count, summary.max_depth), (0, 0));
    assert!(summary.recovery.is_empty());
    assert_eq!(summary.post.files, 2);
    assert_eq!(summary.post.articles, 5);
    assert_eq!(
        (summary.post.missing_articles, summary.post.failed_articles),
        (0, 0)
    );
}

#[tokio::test]
async fn one_rar_keeps_one_streamed_node() {
    let scenario = Scenario::new(posts(single_member_store_set(MEMBER, &body(), 1)));
    let summary = summary(&run(&scenario).await);
    assert_eq!(summary.layout, Some(Layout::Archive));
    assert_eq!((summary.node_count, summary.max_depth), (1, 1));
    let node = &summary.nodes[0];
    assert_eq!(node.kind, "rar_store");
    assert_eq!(node.parent, None);
    assert_eq!((node.volumes, node.depth), (1, 1));
    assert_eq!(node.mode, NodeMode::Streamed);
    assert!(!node.given_up && !node.encrypted && !node.password_needed);
    assert_eq!(node.password_source, None);
}

#[tokio::test]
async fn a_rar_in_volumes_keeps_its_volume_count() {
    let scenario = Scenario::new(posts(single_member_store_set(MEMBER, &body(), 4)));
    let summary = summary(&run(&scenario).await);
    assert_eq!(summary.node_count, 1);
    assert_eq!(summary.nodes[0].volumes, 4);
    assert_eq!(summary.post.files, 4);
}

// One article of the first volume damaged: the PAR2 beside the set repairs
// it, and the summary says the set ruled repaired.
#[tokio::test]
async fn a_repair_by_the_par2_beside_the_set_is_kept() {
    let scenario =
        Scenario::new(with_par2(single_member_store_set(MEMBER, &body(), 2))).damaged(0, 2);
    let summary = summary(&run(&scenario).await);
    assert_eq!(
        summary.layout,
        Some(Layout::Archive),
        "a recovery file is no payload"
    );
    assert_eq!(summary.node_count, 1, "a recovery object is no tree node");
    assert_eq!(summary.recovery_count, 1);
    let par2 = &summary.recovery[0];
    assert_eq!(par2.kind, "par2");
    assert_eq!(par2.outcome.as_deref(), Some("repaired"), "{par2:?}");
    assert_eq!(par2.set_count, 1);
    let set = &par2.sets[0];
    assert_eq!(set.outcome.as_deref(), Some("repaired"));
    assert!(set.slices_repaired >= 1, "{set:?}");
    assert!(set.blocks_reachable >= set.slices_repaired, "{set:?}");
}
