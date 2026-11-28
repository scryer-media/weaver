// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Several PAR2 sets in one job, a member two sets describe, a name two
// descriptions answer to, and obfuscated names a set's descriptions undo.

use super::archive_scenario_fixtures::sets::{delivered, delivered_as_described};
use super::harness::{
    ARTICLE, Claim, Post, Scenario, assert_fetched_once, par2_set, par2_volumes_sliced, payload,
    run,
};
use std::collections::BTreeMap;
use weaver_graph::recovery::RepairOutcome;
use weaver_server_pipeline::pipeline::tests::engines::archive_scenario_fixtures::sets::namespace_move_counts;

// PAR2 names a file by the MD5 of at most this much of its front; damage
// past it leaves an obfuscated member recognisable by its front.
const HASH_WINDOW: usize = 16 * 1024;

fn claims_all(claims: &BTreeMap<String, Claim>, expected: Claim) -> bool {
    !claims.is_empty() && claims.values().all(|claim| *claim == expected)
}

// `lantern.bin` under the lantern set and `beacon.bin` under the beacon
// set, the payload files first, then each set's index and volume.
#[tokio::test]
async fn two_sets_in_one_job_each_repair_their_own_damage() {
    let lantern = payload(3 * ARTICLE, 21);
    let beacon = payload(3 * ARTICLE, 22);
    let mut posts = vec![
        Post::plain("lantern.mkv", lantern.clone()),
        Post::plain("beacon.mkv", beacon.clone()),
    ];
    posts.extend(par2_set("lantern", &[("lantern.mkv", &lantern)], 2));
    posts.extend(par2_set("beacon", &[("beacon.mkv", &beacon)], 2));
    let scenario = Scenario::new(posts).damaged(0, 1).damaged(1, 2);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 2 })]
    );
}

// Forty sets in one job, each of its own slice size and its own file, with
// articles no slice size divides, leased as a lane leases them. Each file's
// articles are cut at its own set's slices alone, so forty sizes cost an
// article one, and every set is vouched for without a byte read back.
#[tokio::test]
async fn sets_of_many_slice_sizes_are_each_vouched_for_without_a_read() {
    const SETS: usize = 40;
    let files: Vec<(String, Vec<u8>, u64)> = (0..SETS)
        .map(|at| {
            let slice = 64 + 4 * at as u64;
            (
                format!("file{at:02}.bin"),
                payload(4 * slice as usize + 10, 50 + at as u8),
                slice,
            )
        })
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .map(|(name, bytes, _)| Post::plain(name, bytes.clone()))
        .collect();
    for (at, (name, bytes, slice)) in files.iter().enumerate() {
        posts.extend(par2_volumes_sliced(
            &format!("set{at:02}"),
            &[(name.as_str(), bytes.as_slice())],
            *slice,
            2,
            1,
        ));
    }
    let graph = run(&Scenario::new(posts).framed(90).leased()).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert_eq!(graph.reads, 0);
    for (name, bytes, _) in &files {
        assert_eq!(delivered(&graph, name), Some(bytes.as_slice()));
    }
}

// The first set that describes `ember.bin` cannot rebuild it and gives it
// to a set of other slices. What arrived was cut and recorded on both
// sets' slices, so the second rebuilds the member having read back no more
// than it reads when it is the only set.
#[tokio::test]
async fn a_member_given_to_a_set_of_other_slices_costs_no_read() {
    let lantern = payload(4 * ARTICLE, 31);
    let ember = payload(4 * ARTICLE, 32);
    let beacon =
        || par2_volumes_sliced("beacon", &[("ember.mkv", &ember)], 2 * ARTICLE as u64, 2, 1);
    let first = || {
        let mut posts = vec![
            Post::plain("lantern.mkv", lantern.clone()),
            Post::plain("ember.mkv", ember.clone()),
        ];
        posts.extend(par2_volumes_sliced(
            "lantern",
            &[("lantern.mkv", &lantern), ("ember.mkv", &ember)],
            ARTICLE as u64,
            1,
            1,
        ));
        posts
    };
    let mut posts = first();
    posts.extend(beacon());
    let moved = run(&Scenario::new(posts).damaged(1, 0).damaged(1, 1)).await;
    assert!(moved.completed(), "{:?}", moved.failure);
    assert!(claims_all(&moved.claims, Claim::Complete));
    assert_eq!(delivered(&moved, "ember.mkv"), Some(ember.as_slice()));
    // Each set alone under the same damage: the lantern set reads what it
    // must to rule itself short, the beacon set what it must to rebuild.
    // Together they read that and no more: the move cost no read.
    let short = run(&Scenario::new(first()).damaged(1, 0).damaged(1, 1)).await;
    assert!(!short.completed());
    let mut alone = vec![Post::plain("ember.mkv", ember.clone())];
    alone.extend(beacon());
    let alone = run(&Scenario::new(alone).damaged(0, 0).damaged(0, 1)).await;
    assert!(alone.completed(), "{:?}", alone.failure);
    assert_eq!(moved.reads, short.reads + alone.reads);
}

// `ember.bin` is described by both sets; the lantern set, posted first,
// carries one recovery block, the beacon set two.
fn shared_member() -> Vec<Post> {
    let lantern = payload(3 * ARTICLE, 23);
    let ember = payload(3 * ARTICLE, 24);
    let beacon = payload(3 * ARTICLE, 25);
    let mut posts = vec![
        Post::plain("lantern.mkv", lantern.clone()),
        Post::plain("ember.mkv", ember.clone()),
        Post::plain("beacon.mkv", beacon.clone()),
    ];
    posts.extend(par2_set(
        "lantern",
        &[("lantern.mkv", &lantern), ("ember.mkv", &ember)],
        1,
    ));
    posts.extend(par2_set(
        "beacon",
        &[("beacon.mkv", &beacon), ("ember.mkv", &ember)],
        2,
    ));
    posts
}

#[tokio::test]
async fn a_member_two_sets_describe_is_repaired_once() {
    let scenario = Scenario::new(shared_member()).damaged(1, 1);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}

// The first set that describes the member cannot rebuild it: two of its
// slices are damaged and the lantern set carries one block. It gives the
// member up, the beacon set, which also describes it and holds two blocks,
// rebuilds it, and the job completes with every file whole.
#[tokio::test]
async fn a_member_its_first_set_cannot_rebuild_is_repaired_by_the_second() {
    let scenario = Scenario::new(shared_member()).damaged(1, 0).damaged(1, 2);
    let graph = run(&scenario).await;
    assert!(graph.refetched().is_empty());
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.failure_kind, None);
    assert!(claims_all(&graph.claims, Claim::Complete));
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    let ember = payload(3 * ARTICLE, 24);
    assert!(
        graph.files.values().any(|bytes| *bytes == ember),
        "{:?}",
        graph.files.keys()
    );
}

// `quill.bin` is described by both sets with different bytes,
// the posted bytes being the lantern set's. A name two sets answer
// differently binds nothing by name; the posted bytes compute to the
// lantern set's file id, so that set binds the member by id, finds it
// clean, and the graph delivers it as posted, reading nothing back.
#[tokio::test]
async fn a_name_two_sets_describe_differently_is_bound_by_id() {
    let quill = payload(3 * ARTICLE, 26);
    let other_quill = payload(3 * ARTICLE, 27);
    let lantern = payload(3 * ARTICLE, 28);
    let beacon = payload(3 * ARTICLE, 29);
    let mut posts = vec![
        Post::plain("quill.bin", quill.clone()),
        Post::plain("lantern.mkv", lantern.clone()),
        Post::plain("beacon.mkv", beacon.clone()),
    ];
    posts.extend(par2_set(
        "lantern",
        &[("lantern.mkv", &lantern), ("quill.bin", &quill)],
        2,
    ));
    posts.extend(par2_set(
        "beacon",
        &[("beacon.mkv", &beacon), ("quill.bin", &other_quill)],
        2,
    ));
    let scenario = Scenario::new(posts);
    let graph = run(&scenario).await;
    assert!(graph.refetched().is_empty());

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
    assert_eq!(
        graph.reads, 0,
        "the segment checksums vouch for every slice"
    );
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

#[tokio::test]
async fn obfuscated_names_are_renamed_by_the_set() {
    let files = [
        ("lantern.mkv", payload(3 * ARTICLE, 30)),
        ("beacon.mkv", payload(2 * ARTICLE + 7, 31)),
    ];
    let scenario = Scenario::new(obfuscated(&files, 2));
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    delivered_as_described(&graph, &files);
    let view = graph.graph();
    assert_eq!(view.outcomes, vec![Some(RepairOutcome::Clean)]);
    assert_eq!(
        view.claims.keys().collect::<Vec<_>>(),
        ["beacon.mkv", "lantern.mkv"],
        "the namespace renamed both members"
    );
    assert!(claims_all(&view.claims, Claim::Complete));
}

#[tokio::test]
async fn a_damaged_obfuscated_member_is_repaired_and_renamed() {
    let files = [
        ("lantern.mkv", payload(HASH_WINDOW + 4 * ARTICLE, 32)),
        ("beacon.mkv", payload(2 * ARTICLE + 7, 33)),
    ];
    let past_window = (HASH_WINDOW / ARTICLE) as u32 + 1;
    let scenario = Scenario::new(obfuscated(&files, 2)).damaged(0, past_window);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    delivered_as_described(&graph, &files);
    let view = graph.graph();
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(claims_all(&view.claims, Claim::Complete));
}

// Two files posted under each other's names, the set describing them under
// their own: each member's front says which file it is, so the set binds
// it by content over the name it was posted as, finds both clean, and the
// namespace puts both names back. The index comes first, so each member is
// bound by its name until its front is in, and the slices checked against
// the wrong file in that while ask for the recovery volume ahead.
#[tokio::test]
async fn swapped_names_are_put_back_by_the_set() {
    let files = [
        ("lantern.mkv", payload(3 * ARTICLE, 34)),
        ("beacon.mkv", payload(2 * ARTICLE + 7, 35)),
    ];
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let mut posts = vec![
        Post::plain("beacon.mkv", files[0].1.clone()),
        Post::plain("lantern.mkv", files[1].1.clone()),
    ];
    posts.extend(par2_set("lantern", &described, 2));
    let scenario = Scenario::new(posts);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    delivered_as_described(&graph, &files);
    let view = graph.graph();
    assert_eq!(view.outcomes, vec![Some(RepairOutcome::Clean)]);
    assert!(claims_all(&view.claims, Claim::Complete));
    assert_eq!(graph.reads, 0, "nothing was read back");
    assert!(graph.refetched().is_empty());
}

// The same swap with every article of the recovery volume gone: the set
// has its index and no block, finds both members clean once each is bound
// by its content, and the namespace still puts both names back.
#[tokio::test]
async fn swapped_names_are_put_back_by_a_set_whose_recovery_volume_is_gone() {
    let files = [
        ("lantern.mkv", payload(3 * ARTICLE, 34)),
        ("beacon.mkv", payload(2 * ARTICLE + 7, 35)),
    ];
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let mut posts = vec![
        Post::plain("beacon.mkv", files[0].1.clone()),
        Post::plain("lantern.mkv", files[1].1.clone()),
    ];
    posts.extend(par2_set("lantern", &described, 2));
    let articles = posts[3].bytes.len().div_ceil(ARTICLE) as u32;
    let scenario = (0..articles).fold(Scenario::new(posts), |scenario, ordinal| {
        scenario.lost(3, ordinal)
    });
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    delivered_as_described(&graph, &files);
    let view = graph.graph();
    assert_eq!(view.outcomes, vec![Some(RepairOutcome::Clean)]);
    assert!(claims_all(&view.claims, Claim::Complete));
}

// The set first, then one member posted under a hex name and three under
// the names the set gives them, the last of them lost whole. The set
// renames the hex member, and the lost one is rebuilt while that move waits
// for a checkpoint. The renamed member moves, once; every other file,
// the rebuilt one too, opens at its name.
#[tokio::test]
async fn one_renamed_member_moves_and_the_rest_open_at_their_names() {
    let files = [
        ("notes.nfo", payload(ARTICLE - 9, 40)),
        ("lantern.mkv", payload(3 * ARTICLE, 41)),
        ("beacon.mkv", payload(2 * ARTICLE + 7, 42)),
        ("quill.srt", payload(ARTICLE + 3, 43)),
    ];
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let mut posts = par2_set("lantern", &described, 3);
    posts.push(Post::plain("3f9a0c71e25b84d6", files[0].1.clone()));
    posts.extend(
        files[1..]
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone())),
    );
    let graph = run(&Scenario::new(posts).lost(5, 0).lost(5, 1)).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    delivered_as_described(&graph, &files);
    assert_eq!(namespace_move_counts().0, 1);
    assert_eq!(namespace_move_counts().1, 0);
}

// Every file posted under the name the set gives it: nothing moves.
#[tokio::test]
async fn a_post_whose_names_all_agree_moves_nothing() {
    let files = [
        ("lantern.mkv", payload(3 * ARTICLE, 44)),
        ("beacon.mkv", payload(2 * ARTICLE + 7, 45)),
    ];
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let mut posts = par2_set("lantern", &described, 2);
    posts.extend(
        files
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone())),
    );
    let graph = run(&Scenario::new(posts)).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    delivered_as_described(&graph, &files);
    assert_eq!(namespace_move_counts().0, 0);
    assert_eq!(namespace_move_counts().1, 0);
}

// The set spells a name the post composed in decomposed form: one name,
// so the member keeps the one it was posted under and nothing moves.
#[tokio::test]
async fn a_name_the_set_spells_another_way_moves_nothing() {
    let bytes = payload(2 * ARTICLE + 7, 46);
    let described = [("Cafe\u{301}.mkv", bytes.as_slice())];
    let mut posts = par2_set("lantern", &described, 2);
    posts.push(Post::plain("Caf\u{e9}.mkv", bytes.clone()));
    let graph = run(&Scenario::new(posts)).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(delivered(&graph, "Caf\u{e9}.mkv"), Some(bytes.as_slice()));
    assert_eq!(graph.files.len(), 1, "{:?}", graph.files.keys());
    assert_eq!(namespace_move_counts().0, 0);
}
