// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Several PAR2 sets in one job, a member two sets describe, a name two
// descriptions answer to, and obfuscated names a set's descriptions undo.

use super::archive_scenario_fixtures::sets::{delivered, delivered_as_described};
use super::harness::{ARTICLE, Post, Scenario, assert_fetched_once, par2_set, payload, run};

// How many files opened under a waiting name and how many moved, over the
// whole run.
fn tally() -> crate::pipeline::graph::namespace::Tally {
    crate::pipeline::graph::namespace::TALLY.with(std::cell::Cell::get)
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
    assert_eq!(tally().moves, 1);
    assert_eq!(tally().waiting_opens, 0);
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
    assert_eq!(tally().moves, 0);
    assert_eq!(tally().waiting_opens, 0);
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
    assert_eq!(tally().moves, 0);
}
