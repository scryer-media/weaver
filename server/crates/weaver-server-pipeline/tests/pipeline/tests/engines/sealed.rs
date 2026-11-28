// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The decoder's checksums of a member are kept while a set may still read
// them, and dropped once the sets proved the member whole.

use super::harness::{ARTICLE, Post, Scenario, look_afresh, par2_set, payload, run};

// A member whose article was lost keeps its checksums while its set waits
// for recovery and repairs it, and has none once the set repaired it.
#[tokio::test]
async fn a_member_its_set_proved_whole_keeps_no_checksums() {
    let data = payload(4 * ARTICLE, 47);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let graph = run(&Scenario::new(posts).lost(0, 1)).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    let looks: Vec<(bool, bool)> = graph
        .graph()
        .sealing
        .iter()
        .filter_map(|look| {
            look.iter()
                .find(|(name, ..)| name == "lantern.bin")
                .map(|(_, proven, held)| (*proven, *held))
        })
        .collect();
    let first_held = looks
        .iter()
        .position(|(_, held)| *held)
        .expect("the member's checksums were kept");
    assert!(
        looks[first_held..]
            .iter()
            .all(|(proven, held)| *proven || *held),
        "checksums dropped while the set still ruled: {looks:?}"
    );
    assert_eq!(
        looks.last(),
        Some(&(true, false)),
        "the proven member still keeps checksums: {looks:?}"
    );
}

// The same job with every look reading its tree afresh beside what it kept:
// each look the tree did not change since the one before keeps the same
// `sealing` entry, names, kinds and claims as reading them again gives.
#[tokio::test]
async fn a_look_keeps_only_what_its_tree_did_not_change() {
    let data = payload(4 * ARTICLE, 47);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    look_afresh(true);
    let graph = run(&Scenario::new(posts).lost(0, 1)).await;
    look_afresh(false);
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
}
