// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The decoder's checksums of a member are kept while a set may still read
// them, and dropped once the sets proved the member whole.

use super::harness::{ARTICLE, Post, Scenario, par2_set, payload, run};
use crate::pipeline::graph::Engine;

// A member whose article was lost keeps its checksums while its set waits
// for recovery and repairs it, and has none once the set repaired it.
#[tokio::test]
async fn a_member_its_set_proved_whole_keeps_no_checksums_under_graph() {
    let data = payload(4 * ARTICLE, 47);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let graph = run(&Scenario::new(posts).lost(0, 1), Engine::Graph).await;
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
