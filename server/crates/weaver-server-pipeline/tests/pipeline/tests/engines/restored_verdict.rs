// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A post whose health verdict is held on a first article still to come when
// the pipeline goes down: the restored job weighs its post again on the
// losses a run before it booked, so the verdict is held again, and the first
// article the hold waited on gives it when it comes.

use std::collections::BTreeSet;

use super::harness::{ARTICLE, Post, Scenario, admit, payload, serve};
use weaver_server_pipeline::pipeline::tests::engines::feed_fixtures::{
    files_with_losses, lose_article, verdict_held,
};
use weaver_server_pipeline::pipeline::tests::engines::post_health_seats::failed_post;
use weaver_server_pipeline::pipeline::tests::engines::restart_regressions::restart;
use weaver_server_pipeline::pipeline::tests::retire_pipeline_database;

// Ten named one-article payload files and one file under a name that
// numbers nothing and declares no kind, eleven articles long: were that
// file a recovery volume it could carry the ten, so while its first article
// is still to come a verdict on the ten lost first articles is held. No real
// post is named: it stands for an obfuscated release whose small named files
// are gone and whose large unnamed file has not arrived yet.
fn post() -> Scenario {
    let mut posts: Vec<Post> = (0..10u8)
        .map(|at| Post::plain(&format!("lantern-{at:02}.mkv"), payload(ARTICLE, at)))
        .collect();
    posts.push(Post::plain("5f0c3a9e81b24d7c", payload(11 * ARTICLE, 10)));
    Scenario::new(posts)
}

const UNNAMED: u32 = 10;

#[tokio::test]
async fn a_verdict_held_at_a_restart_is_given_when_the_first_article_it_waits_on_comes() {
    let scenario = post();
    let (temp_dir, mut pipeline, _) = admit(&scenario).await;
    for file in 0..UNNAMED {
        lose_article(&mut pipeline, file, 0);
    }
    assert!(
        verdict_held(&pipeline),
        "the unnamed file's first article holds the verdict"
    );
    assert_eq!(failed_post(&pipeline), None, "held, not given");

    let mut in_memory = BTreeSet::new();
    let mut pipeline = restart(&temp_dir, pipeline, &scenario, &mut in_memory).await;
    assert_eq!(
        files_with_losses(&pipeline),
        (0..UNNAMED).collect::<Vec<_>>(),
        "the restored job holds the losses a run before it booked"
    );
    assert!(verdict_held(&pipeline), "the verdict is held again");
    assert_eq!(failed_post(&pipeline), None, "held, not given");

    serve(&mut pipeline, &scenario, (UNNAMED, 0)).await;
    let given = failed_post(&pipeline).map(str::to_owned);
    assert!(
        given
            .as_deref()
            .is_some_and(|error| error.contains("10 of 11 first articles are missing")),
        "the first article the hold waited on gives the verdict: {given:?}"
    );
    assert!(!verdict_held(&pipeline));
    retire_pipeline_database(pipeline).await;
}
