// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(super) use weaver_server_pipeline::pipeline::tests::engines::sfv::{
    assert_twin, assert_twin_but_text, crc, listing,
};

// SFV listings: a listing that agrees, one that names a
// file the job does not carry, one that disagrees, one whose names differ
// from the members' only in case, one that leaves a file unnamed, two that
// contradict each other, and one naming a file the post lost. A listing
// never claims damage: one that disagrees, or names a file the post lost,
// leaves the job as the same posts without it.

use super::harness::{ARTICLE, Claim, Post, Scenario, assert_fetched_once, payload, run};
use crate::jobs::model::JobStatus;

fn claims_all(claims: &std::collections::BTreeMap<String, Claim>, expected: Claim) -> bool {
    !claims.is_empty() && claims.values().all(|claim| *claim == expected)
}

#[tokio::test]
async fn an_sfv_whose_every_entry_matches_verifies_and_the_job_completes() {
    let lantern = payload(3 * ARTICLE, 21);
    let beacon = payload(2 * ARTICLE - 5, 22);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
        listing(
            "lantern.sfv",
            &[("lantern.bin", crc(&lantern)), ("beacon.bin", crc(&beacon))],
        ),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
}

#[tokio::test]
async fn an_sfv_entry_naming_no_member_never_fails_the_job() {
    let lantern = payload(3 * ARTICLE, 23);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        listing(
            "lantern.sfv",
            &[("lantern.bin", crc(&lantern)), ("quill.bin", 0x1234_5678)],
        ),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
}

// The posts of the mismatch case, with the listing or without it.
fn mismatch(listed: bool) -> Scenario {
    let lantern = payload(3 * ARTICLE, 24);
    let beacon = payload(2 * ARTICLE, 25);
    let mut posts = vec![
        Post::plain("lantern.mkv", lantern.clone()),
        Post::plain("beacon.mkv", beacon.clone()),
    ];
    if listed {
        posts.push(listing(
            "lantern.sfv",
            &[
                ("lantern.mkv", crc(&lantern)),
                ("beacon.mkv", crc(&beacon) ^ 0xffff_ffff),
            ],
        ));
    }
    Scenario::new(posts)
}

// A listing that disagrees with a file whose bytes arrived as posted does
// not mark it: both files are delivered as posted and the job completes,
// as the same posts without the listing do.
#[tokio::test]
async fn an_sfv_mismatch_leaves_the_job_as_without_the_listing() {
    let graph = run(&mismatch(true)).await;
    assert!(graph.refetched().is_empty());
    assert_eq!(
        graph.status,
        Some(JobStatus::Complete),
        "{:?}",
        graph.failure
    );
    assert_eq!(graph.failure_kind, None);
    assert_eq!(graph.files.len(), 3, "{:?}", graph.files.keys());
    let view = graph.graph();
    assert_eq!(view.claims["lantern.mkv"], Claim::Complete);
    assert_eq!(view.claims["beacon.mkv"], Claim::Complete);
    assert_twin(&graph, &run(&mismatch(false)).await);
}

// An entry naming a member in another case binds it: the listing
// disagrees with the member, and the job still ends as the post without it.
#[tokio::test]
async fn an_sfv_naming_a_member_in_another_case_binds_and_verifies_it() {
    let lantern = payload(3 * ARTICLE, 26);
    let posts = |listed: bool| {
        let mut posts = vec![Post::plain("lantern.bin", lantern.clone())];
        if listed {
            posts.push(listing("lantern.sfv", &[("LANTERN.BIN", crc(&lantern) ^ 1)]));
        }
        Scenario::new(posts)
    };
    let graph = run(&posts(true)).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_twin(&graph, &run(&posts(false)).await);
}

// A payload file no listing names is left as posted, and the job completes.
#[tokio::test]
async fn a_file_the_listing_does_not_name_completes_unchecked() {
    let lantern = payload(3 * ARTICLE, 30);
    let beacon = payload(2 * ARTICLE, 31);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
        listing("lantern.sfv", &[("lantern.bin", crc(&lantern))]),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.graph().claims["beacon.bin"], Claim::Complete);
    assert_eq!(graph.files["lantern beacon/beacon.bin"], beacon);
}

// Two listings that disagree about one name are a contradiction, not
// evidence: that file is not failed on either, and the name they agree on
// is still checked.
#[tokio::test]
async fn listings_that_disagree_about_a_name_fail_neither_way() {
    let lantern = payload(3 * ARTICLE, 33);
    let beacon = payload(2 * ARTICLE, 34);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
        listing(
            "lantern.sfv",
            &[("lantern.bin", crc(&lantern)), ("beacon.bin", crc(&beacon))],
        ),
        listing("lantern.extra.sfv", &[("lantern.bin", 0x0bad_f00d)]),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.graph().claims["beacon.bin"], Claim::Complete);
}

// A file the listing names whose every article is lost is the one file not
// delivered whole: the job ends Partial, as the same posts without the
// listing do. The listing claims no damage of its own.
#[tokio::test]
async fn a_listed_file_whose_articles_are_all_lost_ends_partial_marking_it() {
    let lantern = payload(2 * ARTICLE, 35);
    let beacon = payload(2 * ARTICLE, 36);
    let posts = |listed: bool| {
        let mut posts = vec![
            Post::plain("lantern.mkv", lantern.clone()),
            Post::plain("beacon.mkv", beacon.clone()),
        ];
        if listed {
            posts.push(listing(
                "lantern.sfv",
                &[("lantern.mkv", crc(&lantern)), ("beacon.mkv", crc(&beacon))],
            ));
        }
        Scenario::new(posts).lost(1, 0).lost(1, 1)
    };
    let graph = run(&posts(true)).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    let view = graph.graph();
    assert_eq!(view.claims["lantern.mkv"], Claim::Complete);
    assert_ne!(view.claims["beacon.mkv"], Claim::Complete);
    assert_twin(&graph, &run(&posts(false)).await);
}
