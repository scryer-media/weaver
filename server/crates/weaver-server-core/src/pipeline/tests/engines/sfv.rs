// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// SFV listings: a listing that agrees, one that names a
// file the job does not carry, one that disagrees, one whose names differ
// from the members' only in case, one that leaves a file unnamed, two that
// contradict each other, and one naming a file the post lost.

use super::harness::{ARTICLE, Claim, Outcome, Post, Scenario, assert_fetched_once, payload, run};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;

// A listing: a comment, then `name XXXXXXXX` per entry.
pub(super) fn listing(name: &str, entries: &[(&str, u32)]) -> Post {
    let mut text = String::from("; generated listing\n");
    for (listed, crc) in entries {
        text.push_str(&format!("{listed} {crc:08X}\n"));
    }
    Post::plain(name, text.into_bytes())
}

pub(super) fn crc(bytes: &[u8]) -> u32 {
    par2_rs::checksum::crc32(bytes)
}

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

// A listing that disagrees with the only payload file fails the job: the
// graph names the listing as a recovery set that could not repair it, and
// the file it found damaged, as it does for a PAR2 set short of blocks.
fn assert_failed_as_repair(graph: &Outcome, member: &str, listed_in: &str) {
    assert_fetched_once(graph);
    let graph_failure = graph.failure.as_deref().unwrap_or_default();
    assert!(
        graph_failure.starts_with(&format!(
            "SFV recovery failed for 1 set(s): {listed_in} ({member}): "
        )),
        "{graph_failure:?}"
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
}

#[tokio::test]
async fn an_sfv_mismatch_ends_partial_as_repair() {
    let lantern = payload(3 * ARTICLE, 24);
    let beacon = payload(2 * ARTICLE, 25);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
        listing(
            "lantern.sfv",
            &[
                ("lantern.bin", crc(&lantern)),
                ("beacon.bin", crc(&beacon) ^ 0xffff_ffff),
            ],
        ),
    ]);
    let graph = run(&scenario).await;
    assert!(graph.refetched().is_empty());
    // The file the listing agrees with is delivered whole beside the one it
    // does not, so the job ends Partial, its failure the repair's.
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    // Both files are delivered as posted; the namespace marks the one the
    // listing found bad.
    assert_eq!(graph.files.len(), 3, "{:?}", graph.files.keys());
    let view = graph.graph();
    assert_eq!(view.claims["lantern.bin"], Claim::Complete);
    assert_eq!(view.claims["beacon.bin"], Claim::Damaged);
}

// The listing disagrees with the member, so only an entry bound to it across
// the case difference can fail the job.
#[tokio::test]
async fn an_sfv_naming_a_member_in_another_case_binds_and_verifies_it() {
    let lantern = payload(3 * ARTICLE, 26);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        listing("lantern.sfv", &[("LANTERN.BIN", crc(&lantern) ^ 1)]),
    ]);
    let graph = run(&scenario).await;
    assert_failed_as_repair(&graph, "lantern.bin", "lantern.sfv");
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
// delivered whole: the job ends Partial, the namespace marking that file.
#[tokio::test]
async fn a_listed_file_whose_articles_are_all_lost_ends_partial_marking_it() {
    let lantern = payload(2 * ARTICLE, 35);
    let beacon = payload(2 * ARTICLE, 36);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
        listing(
            "lantern.sfv",
            &[("lantern.bin", crc(&lantern)), ("beacon.bin", crc(&beacon))],
        ),
    ])
    .lost(1, 0)
    .lost(1, 1);
    let graph = run(&scenario).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    let view = graph.graph();
    assert_eq!(view.claims["lantern.bin"], Claim::Complete);
    assert_eq!(view.claims["beacon.bin"], Claim::Damaged);
}
