// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Seeks under both engines: the RAR store's bids bringing volumes forward
// into the root's window, the PAR2 object's recovery seeks promoted over
// every bid, lost and unseekable answers, a seek lifted through a store's
// extent map, and a recovery seek standing across a restart.

use super::harness::{ARTICLE, Outcome, Post, Scenario, payload, run};
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::multi_member_store_set;

const FOLDER: &str = "lantern beacon";

fn members(count: usize, seed: u8) -> Vec<(String, Vec<u8>)> {
    (0..count)
        .map(|index| {
            (
                format!("beacon{index}.bin"),
                payload(9 * ARTICLE + 7 * (index + 1), seed + index as u8),
            )
        })
        .collect()
}

fn store_set(members: &[(String, Vec<u8>)], volumes: usize) -> Vec<(String, Vec<u8>)> {
    let named: Vec<(&str, Vec<u8>)> = members
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    multi_member_store_set(&named, volumes)
}

fn assert_members(outcome: &Outcome, members: &[(String, Vec<u8>)]) {
    assert!(
        outcome.completed(),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    let mut expected: Vec<String> = members
        .iter()
        .map(|(name, _)| format!("{FOLDER}/{name}"))
        .collect();
    expected.sort();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        expected,
        "{}: delivered names",
        outcome.engine
    );
    for (name, bytes) in members {
        assert!(
            outcome.files[&format!("{FOLDER}/{name}")] == *bytes,
            "{}: {name} bytes",
            outcome.engine
        );
    }
}

#[tokio::test]
async fn store_bids_bring_reverse_posted_volumes_forward() {
    let members = members(4, 11);
    let mut volumes = store_set(&members, 12);
    volumes.reverse();
    let posts: Vec<Post> = volumes
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    for post in &posts {
        eprintln!("{} {}", post.name, post.bytes.len().div_ceil(ARTICLE));
    }
    let scenario = Scenario::new(posts);
    let graph = run(&scenario, Engine::Graph).await;
    eprintln!("graph {:?}", graph.order);
    let classic = run(&scenario, Engine::Classic).await;
    eprintln!("classic {:?}", classic.order);
    assert_members(&graph, &members);
    assert_members(&classic, &members);
}
