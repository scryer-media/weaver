// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A recovery object learns everything that arrived for the members it
// covers, whenever it binds: the order in which a set's index, its recovery
// volumes and the members it covers arrive never changes the outcome, the
// repair count, the bytes delivered or what is fetched. At the root each
// row serves the posts whole, one after another, in two orders; two deep,
// inside `level.rar` in `lantern.7z`, the order is the order of the entries.

use std::collections::BTreeSet;

use weaver_graph::recovery::RepairOutcome;

use super::deep::{articles_inside, delivered, two_deep};
use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use super::rar::posts as rar_posts;
use super::rar_rev::{body as rev_body, rar5_revs};
use super::restart::{Down, Restarted, run_restarting_at};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::single_member_store_set;

fn ember() -> Vec<u8> {
    payload(6 * ARTICLE + 5, 121)
}

// `ember.bin`, then the PAR2 index over it, then its volume.
fn ember_posts() -> Vec<Post> {
    let ember = ember();
    let mut posts = vec![Post::plain("ember.bin", ember.clone())];
    posts.extend(par2_set("ember", &[("ember.bin", ember.as_slice())], 2));
    posts
}

// `scenario` served a post at a time, every article of each in turn, the
// posts in `arrival` order; a post the queue does not hold yet comes when
// it does. Nothing goes down.
async fn served_in(scenario: Scenario, arrival: &[u32]) -> Restarted {
    let article = scenario.article;
    let order: Vec<(u32, u32)> = arrival
        .iter()
        .flat_map(|file| {
            let count = scenario.posts[*file as usize].bytes.len().div_ceil(article) as u32;
            (0..count).map(move |ordinal| (*file, ordinal))
        })
        .collect();
    let served = run_restarting_at(&scenario.first(&order), &mut |_| {}, Down::Crash, &|_| {
        false
    })
    .await;
    assert_eq!(served.restarts, 0);
    served
}

// Every article of `before` was asked for ahead of every article of `after`.
fn came_before(served: &Restarted, before: u32, after: u32) -> bool {
    let last = served.order.iter().rposition(|(file, _)| *file == before);
    let first = served.order.iter().position(|(file, _)| *file == after);
    matches!((last, first), (Some(last), Some(first)) if last < first)
}

fn asked(served: &Restarted) -> BTreeSet<(u32, u32)> {
    served.order.iter().copied().collect()
}

// Two arrivals of one job end alike: the status, the files and their bytes,
// the repairs and the blocks reachable, and neither asks for an article
// twice.
fn assert_ends_alike(one: &Restarted, other: &Restarted) {
    assert_eq!(
        one.status,
        Some(JobStatus::Complete),
        "{:?}",
        one.graph.delivery
    );
    assert_eq!(other.status, one.status, "{:?}", other.graph.delivery);
    assert!(other.files == one.files, "the files and their bytes");
    assert_eq!(other.graph.outcomes, one.graph.outcomes, "the repairs");
    assert_eq!(other.graph.blocks_reachable, one.graph.blocks_reachable);
    for served in [one, other] {
        assert_eq!(
            asked(served).len(),
            served.order.len(),
            "an article asked twice"
        );
    }
}

// As `assert_ends_alike`, and the same articles asked for.
fn assert_alike(one: &Restarted, other: &Restarted) {
    assert_ends_alike(one, other);
    assert_eq!(asked(other), asked(one), "the articles asked for");
}

// The same two deep: the status, the files and their bytes, the repairs and
// the blocks reachable; nothing asked for twice.
fn assert_alike_two_deep(one: &Outcome, other: &Outcome) {
    assert!(one.completed(), "{:?} {:?}", one.status, one.failure);
    assert_eq!(other.status, one.status, "{:?}", other.failure);
    assert!(other.files == one.files, "the files and their bytes");
    assert_eq!(other.graph().outcomes, one.graph().outcomes, "the repairs");
    assert_eq!(other.graph().blocks_reachable, one.graph().blocks_reachable);
    for outcome in [one, other] {
        assert!(outcome.refetched().is_empty(), "{:?}", outcome.refetched());
    }
}

// `posts` as the entries of `level.rar` in `lantern.7z`, in `arrival`
// order, the first article lying wholly inside `damaged` served damaged,
// each article posted as `shape` says.
async fn two_deep_in(
    posts: &[Post],
    arrival: &[usize],
    damaged: &[u8],
    shape: fn(Scenario) -> Scenario,
) -> Outcome {
    let entries: Vec<(&str, Vec<u8>)> = arrival
        .iter()
        .map(|at| (posts[*at].name.as_str(), posts[*at].bytes.clone()))
        .collect();
    let outer = two_deep(&entries);
    let ordinal = articles_inside(&outer, damaged)[0];
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]).damaged(0, ordinal);
    run(&shape(scenario)).await
}

// `ember.bin` with its second article damaged, served whole before its
// index: the set binds over a member that has all arrived, and repairs it
// as when it bound first.
#[tokio::test]
async fn a_par2_index_arriving_after_its_damaged_member_repairs_as_when_it_arrives_first() {
    let scenario = || Scenario::new(ember_posts()).damaged(0, 1);
    let first = served_in(scenario(), &[1, 0, 2]).await;
    let last = served_in(scenario(), &[0, 1, 2]).await;
    assert!(came_before(&first, 1, 0) && came_before(&last, 0, 1));
    assert_eq!(
        last.graph.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(last.files[&delivered("ember.bin")] == ember());
    assert_alike(&first, &last);
}

#[tokio::test]
async fn a_par2_index_two_deep_after_its_damaged_member_repairs_as_when_it_comes_first() {
    let posts = ember_posts();
    let first = two_deep_in(&posts, &[1, 2, 0], &posts[0].bytes, |s| s).await;
    let last = two_deep_in(&posts, &[0, 1, 2], &posts[0].bytes, |s| s).await;
    assert_eq!(
        last.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(last.files[&delivered("ember.bin")] == ember());
    assert_alike_two_deep(&first, &last);
}

// The recovery volume's head first, the queue holding no more of it until
// a set asks, then the damaged member, then the index: the set repairs as
// when the index came first, from the same member and index articles. The
// volume itself is not yet held to that: a volume whose head arrived before
// the damage was known is fetched whole, where index first fetches only the
// blocks the repair needs, so this row asks only that it fetch no less.
#[tokio::test]
async fn recovery_volumes_first_and_the_index_last_repair_as_when_the_index_arrives_first() {
    let scenario = || Scenario::new(ember_posts()).damaged(0, 1);
    let first = served_in(scenario(), &[1, 0, 2]).await;
    let volume_first = served_in(scenario(), &[2, 0, 1]).await;
    assert_eq!(volume_first.order[0], (2, 0));
    assert!(came_before(&volume_first, 0, 1));
    assert_eq!(
        volume_first.graph.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_ends_alike(&first, &volume_first);
    let beyond = |served: &Restarted| {
        asked(served)
            .into_iter()
            .filter(|(file, _)| *file != 2)
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(beyond(&volume_first), beyond(&first));
    assert!(asked(&first).is_subset(&asked(&volume_first)));
}

#[tokio::test]
async fn recovery_volumes_two_deep_first_and_the_index_last_repair_as_when_the_index_comes_first() {
    let posts = ember_posts();
    let first = two_deep_in(&posts, &[1, 2, 0], &posts[0].bytes, |s| s).await;
    let volume_first = two_deep_in(&posts, &[2, 0, 1], &posts[0].bytes, |s| s).await;
    assert_eq!(
        volume_first.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_alike_two_deep(&first, &volume_first);
}

// Every article uuencoded: none carries a checksum of its own, so a damaged
// one says only that it is damaged. The member, its second article
// damaged, arrives whole before the set binds, and the set repairs it as
// when it bound first.
#[tokio::test]
async fn a_uuencoded_member_damaged_before_its_set_binds_repairs_as_when_the_set_binds_first() {
    let scenario = || Scenario::new(ember_posts()).damaged(0, 1).uuencoded();
    let first = served_in(scenario(), &[1, 0, 2]).await;
    let last = served_in(scenario(), &[0, 1, 2]).await;
    assert!(came_before(&last, 0, 1));
    assert_eq!(
        last.graph.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_alike(&first, &last);
}

#[tokio::test]
async fn a_uuencoded_member_two_deep_damaged_before_its_set_binds_repairs_as_when_the_set_binds_first()
 {
    let posts = ember_posts();
    let first = two_deep_in(&posts, &[1, 2, 0], &posts[0].bytes, Scenario::uuencoded).await;
    let last = two_deep_in(&posts, &[0, 1, 2], &posts[0].bytes, Scenario::uuencoded).await;
    assert_eq!(
        last.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_alike_two_deep(&first, &last);
}

// A stored RAR5 set of four volumes, then the `.rev` over them, one article
// in the second volume damaged.
fn rev_posts() -> Vec<Post> {
    let volumes = single_member_store_set("beacon.bin", &rev_body(), 4);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 1);
    let mut posts = rar_posts(volumes);
    posts.extend(revs);
    posts
}

// The `.rev` binds after every volume arrived, one of them damaged, and
// rebuilds that volume as when it bound first. Bound first, only its head
// comes ahead of the volumes: the rest waits until the set comes up short,
// behind every volume.
#[tokio::test]
async fn a_rev_bound_after_its_volumes_arrived_one_damaged_rebuilds_as_when_it_binds_first() {
    let scenario = || Scenario::new(rev_posts()).damaged(1, 1);
    let first = served_in(scenario(), &[4, 0, 1, 2, 3]).await;
    let last = served_in(scenario(), &[0, 1, 2, 3, 4]).await;
    let rest_behind_the_volumes = |served: &Restarted| {
        let volumes = served.order.iter().rposition(|(file, _)| *file != 4);
        let rest = served.order.iter().position(|article| *article == (4, 1));
        matches!((volumes, rest), (Some(volumes), Some(rest)) if volumes < rest)
    };
    assert_eq!(first.order[0], (4, 0));
    assert!(rest_behind_the_volumes(&first) && came_before(&last, 3, 4));
    assert!(last.files[&delivered("beacon.bin")] == rev_body());
    assert_alike(&first, &last);
}

#[tokio::test]
async fn a_rev_two_deep_bound_after_its_volumes_arrived_one_damaged_rebuilds_as_when_it_binds_first()
 {
    let posts = rev_posts();
    let first = two_deep_in(&posts, &[4, 0, 1, 2, 3], &posts[1].bytes, |s| s).await;
    let last = two_deep_in(&posts, &[0, 1, 2, 3, 4], &posts[1].bytes, |s| s).await;
    assert!(last.files[&delivered("beacon.bin")] == rev_body());
    assert_alike_two_deep(&first, &last);
}
