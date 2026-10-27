// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a graph job's checkpoints ask the database to write. A checkpoint is
// taken after every step, so one that wrote the whole tree each time would
// cost a job the square of its size; one that writes what its step changed
// costs each step the same whatever the job's size.

use super::harness::{ARTICLE, Post, Scenario, par2_set, payload, run};
use crate::pipeline::graph::checkpoint::Tally;

// One plain file of `articles` articles under a PAR2 set sliced at an
// article.
fn covered(articles: usize) -> Scenario {
    let data = payload(articles * ARTICLE, 11);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    Scenario::new(posts)
}

async fn tally(articles: usize) -> Tally {
    let outcome = run(&covered(articles)).await;
    assert!(outcome.completed(), "{:?}", outcome.failure);
    outcome.graph().checkpoints
}

#[tokio::test]
async fn a_checkpoint_writes_what_its_step_changed() {
    let small = tally(20).await;
    let large = tally(80).await;
    assert!(small.checkpoints >= 20, "a checkpoint per step: {small:?}");
    assert!(large.checkpoints >= 80, "a checkpoint per step: {large:?}");
    // Per checkpoint, a job four times the size writes no more than a step
    // of the smaller one does, give or take what a step touches.
    let per = |of: usize, tally: Tally| of as f64 / tally.checkpoints as f64;
    assert!(
        per(large.facts, large) <= 1.5 * per(small.facts, small),
        "facts bytes per checkpoint grow with the job: {small:?} {large:?}"
    );
    assert!(
        per(large.rows, large) <= 1.5 * per(small.rows, small),
        "rows per checkpoint grow with the job: {small:?} {large:?}"
    );
}
