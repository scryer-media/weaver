// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A PAR2 set whose index never came, every article of it lost: the recovery
// volumes carry the same description packets, so damage is still repaired
// from them.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_volumes, payload, run};

fn assert_whole(outcome: &Outcome, name: &str, body: &[u8]) {
    assert!(
        outcome.completed(),
        "{:?} {:?} {:?}",
        outcome.status,
        outcome.failure,
        outcome.files.keys().collect::<Vec<_>>()
    );
    assert!(
        outcome.files[&format!("lantern beacon/{name}")] == body,
        "{name} bytes"
    );
}

fn without_index(file: Vec<u8>, blocks: u32, volumes: u32) -> (Scenario, u32) {
    let mut posts = vec![Post::plain("lantern.mkv", file.clone())];
    let set = par2_volumes("lantern", &[("lantern.mkv", &file)], blocks, volumes);
    let index_len = set[0].bytes.len();
    posts.extend(set);
    let mut scenario = Scenario::new(posts);
    for ordinal in 0..index_len.div_ceil(ARTICLE) as u32 {
        scenario = scenario.lost(1, ordinal);
    }
    (scenario, file.len().div_ceil(ARTICLE) as u32)
}

#[tokio::test]
async fn a_damaged_file_whose_index_never_came_is_repaired_from_its_volume() {
    let file = payload(6 * ARTICLE + 5, 61);
    let (scenario, _) = without_index(file.clone(), 2, 1);
    assert_whole(&run(&scenario.damaged(0, 2)).await, "lantern.mkv", &file);
}

#[tokio::test]
async fn a_file_missing_an_article_whose_index_never_came_is_repaired_from_two_volumes() {
    let file = payload(6 * ARTICLE + 5, 62);
    let (scenario, articles) = without_index(file.clone(), 3, 2);
    assert_whole(
        &run(&scenario.lost(0, articles - 1)).await,
        "lantern.mkv",
        &file,
    );
}
