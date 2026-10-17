// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// SFV listings under both engines: a listing that agrees, one that names a
// file the job does not carry, one that disagrees, and one whose names differ
// from the members' only in case.

use super::harness::{ARTICLE, Claim, Outcome, Post, Scenario, assert_same, payload, run_both};
use crate::history::HistoryFailureKind;

// A listing: a comment, then `name XXXXXXXX` per entry.
fn listing(name: &str, entries: &[(&str, u32)]) -> Post {
    let mut text = String::from("; generated listing\n");
    for (listed, crc) in entries {
        text.push_str(&format!("{listed} {crc:08X}\n"));
    }
    Post::plain(name, text.into_bytes())
}

fn crc(bytes: &[u8]) -> u32 {
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
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
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
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
}

// A listing that disagrees fails the job under both engines alike, at the
// same stage, with the same files and claims; each words it its own way.
// Classic names the file, the graph names the listing as a recovery set
// that could not repair it, as it does for a PAR2 set short of blocks.
fn assert_failed_each_its_way(classic: &Outcome, graph: &Outcome, member: &str, listed_in: &str) {
    for outcome in [classic, graph] {
        assert!(outcome.refetched().is_empty(), "{}", outcome.engine);
    }
    assert_eq!(
        classic.failure.as_deref(),
        Some(format!("sfv mismatch: {member}").as_str())
    );
    let graph_failure = graph.failure.as_deref().unwrap_or_default();
    assert!(
        graph_failure.starts_with(&format!("SFV recovery failed for 1 set(s): {listed_in}: ")),
        "{graph_failure:?}"
    );
    assert_eq!(classic.failure_kind, Some(HistoryFailureKind::Health));
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert_eq!(classic.files, graph.files, "delivered files");
    assert_eq!(classic.claims, graph.claims, "claims");
    assert_eq!(classic.requests, graph.requests, "article requests");
}

#[tokio::test]
async fn an_sfv_mismatch_fails_the_job_as_health_under_classic_and_as_repair_under_graph() {
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
    let (classic, graph) = run_both(&scenario).await;
    assert_failed_each_its_way(&classic, &graph, "beacon.bin", "lantern.sfv");
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
    let (classic, graph) = run_both(&scenario).await;
    assert_failed_each_its_way(&classic, &graph, "lantern.bin", "lantern.sfv");
}
