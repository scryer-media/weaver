// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Several PAR2 sets in one job, a member two sets describe, a name two
// descriptions answer to, and obfuscated names a set's descriptions undo.

use std::collections::BTreeMap;

use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_same, par2_set, payload, run_both,
};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;

// PAR2 names a file by the MD5 of at most this much of its front; damage
// past it leaves an obfuscated member recognisable by its front.
const HASH_WINDOW: usize = 16 * 1024;

// Every job's files land in a folder named for the job.
const FOLDER: &str = "lantern beacon";

fn claims_all(claims: &BTreeMap<String, Claim>, expected: Claim) -> bool {
    !claims.is_empty() && claims.values().all(|claim| *claim == expected)
}

fn delivered<'a>(outcome: &'a Outcome, name: &str) -> Option<&'a [u8]> {
    outcome
        .files
        .get(&format!("{FOLDER}/{name}"))
        .map(Vec::as_slice)
}

fn failed_with(outcome: &Outcome, prefix: &str) -> bool {
    outcome
        .failure
        .as_deref()
        .is_some_and(|error| error.starts_with(prefix))
}

// `lantern.bin` under the lantern set and `beacon.bin` under the beacon
// set, the payload files first, then each set's index and volume.
#[tokio::test]
async fn two_sets_in_one_job_each_repair_their_own_damage() {
    let lantern = payload(3 * ARTICLE, 21);
    let beacon = payload(3 * ARTICLE, 22);
    let mut posts = vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
    ];
    posts.extend(par2_set("lantern", &[("lantern.bin", &lantern)], 2));
    posts.extend(par2_set("beacon", &[("beacon.bin", &beacon)], 2));
    let scenario = Scenario::new(posts).damaged(0, 1).damaged(1, 2);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 2 })]
    );
}

// `ember.bin` is described by both sets; the lantern set, posted first,
// carries one recovery block, the beacon set two.
fn shared_member() -> Vec<Post> {
    let lantern = payload(3 * ARTICLE, 23);
    let ember = payload(3 * ARTICLE, 24);
    let beacon = payload(3 * ARTICLE, 25);
    let mut posts = vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("ember.bin", ember.clone()),
        Post::plain("beacon.bin", beacon.clone()),
    ];
    posts.extend(par2_set(
        "lantern",
        &[("lantern.bin", &lantern), ("ember.bin", &ember)],
        1,
    ));
    posts.extend(par2_set(
        "beacon",
        &[("beacon.bin", &beacon), ("ember.bin", &ember)],
        2,
    ));
    posts
}

#[tokio::test]
async fn a_member_two_sets_describe_is_repaired_once() {
    let scenario = Scenario::new(shared_member()).damaged(1, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}

// The member's binding answers to the first set that describes it under
// both engines: its shortfall is the repair's, though the second set alone
// holds blocks enough. Classic fails the job. Under the graph the members
// it never reached are delivered whole beside it, so the job ends Partial
// (the partial-extraction ruling of 2026-10-18), its failure the repair's.
#[tokio::test]
async fn a_member_two_sets_describe_fails_on_the_first_sets_shortfall() {
    let scenario = Scenario::new(shared_member()).damaged(1, 0).damaged(1, 2);
    let (classic, graph) = run_both(&scenario).await;
    for outcome in [&classic, &graph] {
        assert!(outcome.refetched().is_empty(), "{}", outcome.engine);
        assert_eq!(outcome.failure_kind, Some(HistoryFailureKind::Repair));
    }
    assert!(
        failed_with(&classic, "PAR2 recovery failed for 1 set(s): lantern.par2"),
        "{:?}",
        classic.failure
    );
    assert!(classic.files.is_empty());
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(graph.claims["lantern.bin"], Claim::Complete);
    assert_eq!(graph.claims["beacon.bin"], Claim::Complete);
    assert!(failed_with(
        &classic,
        "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern.bin, ember.bin): \
         not repairable: 2 damaged slices, only 1 recovery blocks advertised"
    ));
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 2,
            reachable: 1
        })]
    );
}

// Divergence: `quill.bin` is described by both sets with different bytes,
// the posted bytes being the lantern set's. A name two sets answer
// differently binds nothing by name; the posted bytes compute to the
// lantern set's file id, so that set binds the member by id, finds it
// clean, and the graph delivers it as posted. Classic completes too, but
// delivers the beacon set's description of it, every byte rewritten, after
// fetching the beacon volume whole; that classic outcome is recorded, not
// explained.
#[tokio::test]
async fn a_name_two_sets_describe_differently_is_bound_by_id_under_graph_and_rewritten_under_classic()
 {
    let quill = payload(3 * ARTICLE, 26);
    let other_quill = payload(3 * ARTICLE, 27);
    let lantern = payload(3 * ARTICLE, 28);
    let beacon = payload(3 * ARTICLE, 29);
    let mut posts = vec![
        Post::plain("quill.bin", quill.clone()),
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
    ];
    posts.extend(par2_set(
        "lantern",
        &[("lantern.bin", &lantern), ("quill.bin", &quill)],
        2,
    ));
    posts.extend(par2_set(
        "beacon",
        &[("beacon.bin", &beacon), ("quill.bin", &other_quill)],
        2,
    ));
    let scenario = Scenario::new(posts);
    let (classic, graph) = run_both(&scenario).await;
    for outcome in [&classic, &graph] {
        assert!(outcome.refetched().is_empty(), "{}", outcome.engine);
    }

    assert!(classic.completed(), "{:?}", classic.failure);
    assert_eq!(
        delivered(&classic, "quill.bin"),
        Some(other_quill.as_slice())
    );
    assert_eq!(classic.claims["quill.bin"], Claim::Damaged);
    assert_eq!(classic.claims["lantern.bin"], Claim::Complete);
    assert_eq!(classic.claims["beacon.bin"], Claim::Complete);

    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert_eq!(delivered(&graph, "quill.bin"), Some(quill.as_slice()));
    let view = graph.graph();
    assert_eq!(
        view.claims["quill.bin"],
        Claim::Complete,
        "bound by id, clean"
    );
    assert_eq!(view.outcomes, vec![Some(RepairOutcome::Clean)]);
}

// Hex names in place of the posted ones, the set named by a hex string
// too; the set's descriptions carry the real names.
fn obfuscated(files: &[(&str, Vec<u8>)], blocks: u32) -> Vec<Post> {
    const HEX: [&str; 2] = ["3f9a0c71e25b84d6", "a07d5e19c4b26f83"];
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .zip(HEX)
        .map(|((_, bytes), hex)| Post::plain(hex, bytes.clone()))
        .collect();
    posts.extend(par2_set("e81b6d3f0a4c9725", &described, blocks));
    posts
}

// Under the real names with the posted bytes, and nothing else: the posted
// names and the set's own files are gone from the output.
fn delivered_as_described(outcome: &Outcome, files: &[(&str, Vec<u8>)]) {
    let expected: BTreeMap<String, &[u8]> = files
        .iter()
        .map(|(name, bytes)| (format!("{FOLDER}/{name}"), bytes.as_slice()))
        .collect();
    let got: BTreeMap<String, &[u8]> = outcome
        .files
        .iter()
        .map(|(name, bytes)| (name.clone(), bytes.as_slice()))
        .collect();
    assert!(got == expected, "{}: {:?}", outcome.engine, got.keys());
}

#[tokio::test]
async fn obfuscated_names_are_renamed_by_the_set_under_both_engines() {
    let files = [
        ("lantern.bin", payload(3 * ARTICLE, 30)),
        ("beacon.bin", payload(2 * ARTICLE + 7, 31)),
    ];
    let scenario = Scenario::new(obfuscated(&files, 2));
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    delivered_as_described(&classic, &files);
    delivered_as_described(&graph, &files);
    let view = graph.graph();
    assert_eq!(view.outcomes, vec![Some(RepairOutcome::Clean)]);
    assert_eq!(
        view.claims.keys().collect::<Vec<_>>(),
        ["beacon.bin", "lantern.bin"],
        "the namespace renamed both members"
    );
    assert!(claims_all(&view.claims, Claim::Complete));
}

#[tokio::test]
async fn a_damaged_obfuscated_member_is_repaired_and_renamed_under_both_engines() {
    let files = [
        ("lantern.bin", payload(HASH_WINDOW + 4 * ARTICLE, 32)),
        ("beacon.bin", payload(2 * ARTICLE + 7, 33)),
    ];
    let past_window = (HASH_WINDOW / ARTICLE) as u32 + 1;
    let scenario = Scenario::new(obfuscated(&files, 2)).damaged(0, past_window);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    delivered_as_described(&classic, &files);
    delivered_as_described(&graph, &files);
    let view = graph.graph();
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(claims_all(&view.claims, Claim::Complete));
}
