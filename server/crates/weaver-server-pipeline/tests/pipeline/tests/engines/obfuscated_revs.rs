// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// RAR5 recovery volumes posted under obfuscated names beside a named
// volume set. Nothing in their names ties them to a set: their own header,
// signed and checksummed, states each data volume's size and CRC32, so they
// join each other by that table and bind the set whose volumes it
// describes, proved by a volume whose CRC32 is the one the table gives. A
// recovery volume of another set rebuilds nothing.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use super::rar::{delivered, posts, rar5_revs};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::single_member_store_set;

const MEMBER: &str = "beacon.bin";
const OTHER: &str = "harbor.bin";

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().outcomes,
        graph.files.keys().collect::<Vec<_>>()
    )
}

// The recovery volumes, each under a name of 32 hex digits from `seed`.
fn obfuscated(revs: Vec<Post>, seed: u128) -> Vec<Post> {
    revs.into_iter()
        .enumerate()
        .map(|(index, rev)| Post {
            least_article: rev.least_article,
            ..Post::plain(&format!("{:032x}", seed + index as u128), rev.bytes)
        })
        .collect()
}

// A set of four volumes named `<base>.partN.rar` over `body` stored as
// `member`, without the volumes at `absent`, and `revs` recovery volumes
// over it under obfuscated names.
fn set(
    base: &str,
    member: &str,
    body: &[u8],
    absent: &[usize],
    revs: usize,
    seed: u128,
) -> Vec<Post> {
    let volumes: Vec<(String, Vec<u8>)> = single_member_store_set(member, body, 4)
        .into_iter()
        .map(|(name, bytes)| {
            let index = name.rsplit_once(".part").map_or(name.as_str(), |(_, n)| n);
            (format!("{base}.part{index}"), bytes)
        })
        .collect();
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = obfuscated(rar5_revs(base, &bytes, revs), seed);
    let kept = volumes
        .into_iter()
        .enumerate()
        .filter(|(index, _)| !absent.contains(index))
        .map(|(_, volume)| volume)
        .collect();
    let mut out = posts(kept);
    out.extend(revs);
    out
}

fn assert_delivered(graph: &Outcome, wanted: &[(&str, &[u8])]) {
    assert!(graph.completed(), "{}", summary(graph));
    let mut names: Vec<String> = wanted.iter().map(|(name, _)| delivered(name)).collect();
    names.sort();
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        names,
        "{}",
        summary(graph)
    );
    for (name, body) in wanted {
        assert!(graph.files[&delivered(name)] == *body, "{name} bytes");
    }
}

// One volume absent, one recovery volume under an obfuscated name: its
// table binds the set, and it rebuilds the volume.
#[tokio::test]
async fn an_obfuscated_rar5_recovery_volume_rebuilds_an_absent_volume_of_its_set() {
    let body = payload(11 * ARTICLE + 29, 61);
    let graph = run(&Scenario::new(set(
        "silver.horizon",
        MEMBER,
        &body,
        &[1],
        1,
        0xa5e0_0000,
    )))
    .await;
    assert_delivered(&graph, &[(MEMBER, &body)]);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })],
        "{}",
        summary(&graph)
    );
}

// Two volumes absent, two recovery volumes under obfuscated names: the
// second joins the first by the table both carry, and together they
// rebuild both volumes.
#[tokio::test]
async fn obfuscated_rar5_recovery_volumes_join_by_their_table() {
    let body = payload(11 * ARTICLE + 29, 61);
    let graph = run(&Scenario::new(set(
        "silver.horizon",
        MEMBER,
        &body,
        &[1, 2],
        2,
        0xa5e0_0000,
    )))
    .await;
    assert_delivered(&graph, &[(MEMBER, &body)]);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 2 })],
        "{}",
        summary(&graph)
    );
}

// Two sets in one job, of different sizes, each short of one volume and
// each with one recovery volume under an obfuscated name: each recovery
// volume rebuilds its own set's volume, and neither takes the other's.
#[tokio::test]
async fn two_sets_keep_their_own_obfuscated_rar5_recovery_volumes() {
    let first = payload(11 * ARTICLE + 29, 61);
    let second = payload(17 * ARTICLE + 8, 67);
    let mut posts = set("silver.horizon", MEMBER, &first, &[1], 1, 0xa5e0_0000);
    posts.extend(set("amber.coast", OTHER, &second, &[2], 1, 0xb7f0_0000));
    let graph = run(&Scenario::new(posts)).await;
    assert_delivered(&graph, &[(MEMBER, &first), (OTHER, &second)]);
    let outcomes = &graph.graph().outcomes;
    assert_eq!(
        outcomes
            .iter()
            .filter(|said| **said == Some(RepairOutcome::Repaired { slices: 1 }))
            .count(),
        2,
        "{}",
        summary(&graph)
    );
}

// The volume sizes of `set`'s volumes over `body` stored as `member`.
fn sizes(member: &str, body: &[u8]) -> Vec<usize> {
    single_member_store_set(member, body, 4)
        .iter()
        .map(|(_, bytes)| bytes.len())
        .collect()
}

// Two sets in one job whose volumes are the same sizes, each short of one
// volume and each with one recovery volume under an obfuscated name. Both
// tables fit both sets by size; their CRC32s say which set is whose, so
// each recovery volume rebuilds its own set's volume.
#[tokio::test]
async fn two_sets_of_the_same_sizes_keep_their_own_obfuscated_rar5_recovery_volumes() {
    let first = payload(11 * ARTICLE + 29, 61);
    let second = payload(11 * ARTICLE + 29, 67);
    assert_eq!(sizes(MEMBER, &first), sizes(OTHER, &second), "alike sizes");
    let mut posts = set("silver.horizon", MEMBER, &first, &[1], 1, 0xa5e0_0000);
    posts.extend(set("amber.coast", OTHER, &second, &[2], 1, 0xb7f0_0000));
    let graph = run(&Scenario::new(posts)).await;
    assert_delivered(&graph, &[(MEMBER, &first), (OTHER, &second)]);
    let outcomes = &graph.graph().outcomes;
    assert_eq!(
        outcomes
            .iter()
            .filter(|said| **said == Some(RepairOutcome::Repaired { slices: 1 }))
            .count(),
        2,
        "{}",
        summary(&graph)
    );
}

// The same, with the set the first table fits ahead of the other in the
// order the sets are tried: the recovery volume of the later set is
// refuted by the earlier one's CRC32s before it binds its own.
#[tokio::test]
async fn an_obfuscated_rar5_recovery_volume_passes_over_a_set_of_the_same_sizes() {
    let first = payload(11 * ARTICLE + 29, 61);
    let second = payload(11 * ARTICLE + 29, 67);
    let mut posts = set("amber.coast", OTHER, &second, &[], 0, 0);
    posts.extend(set("silver.horizon", MEMBER, &first, &[2], 1, 0xa5e0_0000));
    let graph = run(&Scenario::new(posts)).await;
    assert_delivered(&graph, &[(MEMBER, &first), (OTHER, &second)]);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })],
        "{}",
        summary(&graph)
    );
}

// The same, with every volume of the recovery volume's own set that came
// damaged by a lost article, and a PAR2 set over them that mends them. The
// first ruling over that set finds no volume whole and proves nothing, so it
// passes over the set only for the volumes whole then; once the PAR2 set
// mends them, they prove it, and the recovery volume rebuilds the absent
// volume.
#[tokio::test]
async fn an_obfuscated_rar5_recovery_volume_proves_its_set_once_another_recovery_mends_it() {
    let first = payload(11 * ARTICLE + 29, 61);
    let second = payload(11 * ARTICLE + 29, 67);
    let mut posts = set("amber.coast", OTHER, &second, &[], 0, 0);
    posts.extend(set("silver.horizon", MEMBER, &first, &[1], 1, 0xa5e0_0000));
    let present: Vec<(String, Vec<u8>)> = single_member_store_set(MEMBER, &first, 4)
        .into_iter()
        .enumerate()
        .filter(|(index, _)| *index != 1)
        .map(|(_, (name, bytes))| {
            let index = name.rsplit_once(".part").map_or(name.as_str(), |(_, n)| n);
            (format!("silver.horizon.part{index}"), bytes)
        })
        .collect();
    let files: Vec<(&str, &[u8])> = present
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    posts.extend(par2_set("silver.horizon", &files, 4));
    // Posts 4, 5 and 6 are the set's present volumes: each loses its second
    // article.
    let scenario = Scenario::new(posts)
        .damaged(4, 1)
        .damaged(5, 1)
        .damaged(6, 1);
    let graph = run(&scenario).await;
    assert_delivered(&graph, &[(MEMBER, &first), (OTHER, &second)]);
    let rulings = &graph.graph().rulings;
    for kind in ["par2", "rar_recovery"] {
        assert!(
            rulings.iter().any(|(engine, result)| engine == kind
                && matches!(result, Some(RepairOutcome::Repaired { .. }))),
            "{kind} did not repair: {rulings:?} {}",
            summary(&graph)
        );
    }
}

// Two whole sets of the same sizes beside a recovery volume of neither:
// its table fits both by size and its CRC32s refute both, so it binds
// none and the job completes rather than waiting on it.
#[tokio::test]
async fn an_obfuscated_rar5_recovery_volume_that_fits_two_sets_by_size_alone_does_not_stall() {
    let first = payload(11 * ARTICLE + 29, 61);
    let second = payload(11 * ARTICLE + 29, 67);
    let mut posts = set("silver.horizon", MEMBER, &first, &[], 0, 0);
    posts.extend(set("amber.coast", OTHER, &second, &[], 0, 0));
    posts.extend(set(
        "silver.horizon",
        MEMBER,
        &payload(11 * ARTICLE + 29, 71),
        &[0, 1, 2, 3],
        1,
        0xc3d0_0000,
    ));
    let graph = run(&Scenario::new(posts)).await;
    assert_delivered(&graph, &[(MEMBER, &first), (OTHER, &second)]);
    assert!(
        !graph
            .graph()
            .outcomes
            .iter()
            .any(|said| matches!(said, Some(RepairOutcome::Repaired { .. }))),
        "{}",
        summary(&graph)
    );
}

// A set short of one volume, beside a recovery volume of another set under
// an obfuscated name: it rebuilds nothing, and the job fails with nothing
// delivered.
async fn assert_foreign_rev_unused(other: &[u8]) {
    let body = payload(11 * ARTICLE + 29, 61);
    let mut posts = set("silver.horizon", MEMBER, &body, &[1], 0, 0);
    posts.extend(set(
        "silver.horizon",
        MEMBER,
        other,
        &[0, 1, 2, 3],
        1,
        0xc3d0_0000,
    ));
    let graph = run(&Scenario::new(posts)).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{}",
        summary(&graph)
    );
    assert!(graph.files.is_empty(), "{}", summary(&graph));
    assert!(
        !graph
            .graph()
            .outcomes
            .iter()
            .any(|said| matches!(said, Some(RepairOutcome::Repaired { .. }))),
        "{}",
        summary(&graph)
    );
}

// The other set's volumes are the same sizes, but not the same bytes: no
// volume here has the CRC32 its table gives, so it binds nothing it can
// prove.
#[tokio::test]
async fn an_obfuscated_rar5_recovery_volume_of_a_set_of_the_same_sizes_is_not_used() {
    assert_foreign_rev_unused(&payload(11 * ARTICLE + 29, 71)).await;
}

// The other set's volumes are of other sizes: its table describes no set
// here.
#[tokio::test]
async fn an_obfuscated_rar5_recovery_volume_of_a_set_of_other_sizes_is_not_used() {
    assert_foreign_rev_unused(&payload(17 * ARTICLE + 8, 67)).await;
}

// A recovery volume named for a set the job does not hold binds nothing.
// It rules once every other member is in, on nothing, and the set short of
// a volume fails as it would without it, where before the job waited on
// the recovery volume for good.
#[tokio::test]
async fn a_recovery_volume_of_a_set_not_in_the_job_rules_on_nothing() {
    let body = payload(11 * ARTICLE + 29, 61);
    let mut posts = set("silver.horizon", MEMBER, &body, &[1], 0, 0);
    let foreign = set(
        "silver.horizon",
        MEMBER,
        &payload(17 * ARTICLE + 8, 67),
        &[0, 1, 2, 3],
        1,
        0,
    );
    posts.extend(foreign.into_iter().map(|rev| Post {
        least_article: rev.least_article,
        ..Post::plain("driftwood.part5.rev", rev.bytes)
    }));
    let graph = run(&Scenario::new(posts)).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{}",
        summary(&graph)
    );
    assert!(graph.files.is_empty(), "{}", summary(&graph));
}
