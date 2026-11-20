// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a graph job's checkpoints ask the database to write. A step takes a
// checkpoint only when the write it joins is due, so the count of them
// follows the job's bytes and age, not its steps; and one that writes what
// changed since the last costs a job no more than its size.

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
    // Neither job reaches the byte or age threshold of a write, so each
    // takes the same checkpoints whatever its steps.
    assert_eq!(
        small.checkpoints, large.checkpoints,
        "checkpoints follow the job's steps: {small:?} {large:?}"
    );
    assert!(
        large.checkpoints < 20,
        "a checkpoint per step: {small:?} {large:?}"
    );
    // A job four times the size writes no more than four times the rows
    // and facts, give or take what a step touches.
    assert!(
        large.facts as f64 <= 6.0 * small.facts as f64,
        "facts bytes grow faster than the job: {small:?} {large:?}"
    );
    assert!(
        large.rows as f64 <= 6.0 * small.rows as f64,
        "rows grow faster than the job: {small:?} {large:?}"
    );
}
