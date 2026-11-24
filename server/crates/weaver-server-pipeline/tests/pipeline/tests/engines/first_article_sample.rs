// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The first-article sample the feed keeps as first articles settle or are
// lost. Whatever happens to the post's first articles, the verdict it gives
// is the one a walk of every file gives, and a lost article costs the head
// of its own file at most, never a walk of the post.

use std::collections::BTreeSet;

use super::harness::{ARTICLE, Post, Scenario, admit, par2_set, payload};
use weaver_server_pipeline::pipeline::tests::engines::feed_fixtures::{
    heads_read, kept_verdict, lose_article, walk, walked_verdict,
};
use weaver_server_pipeline::pipeline::tests::retire_pipeline_database;

// `files` payload files of `articles` articles each, and a PAR2 set of
// `blocks` blocks over them when `blocks` is not zero.
fn post(files: usize, articles: usize, blocks: u32) -> Scenario {
    let payloads: Vec<(String, Vec<u8>)> = (0..files)
        .map(|index| {
            (
                format!("lantern-{index:04}.mkv"),
                payload(articles * ARTICLE, index as u8),
            )
        })
        .collect();
    let mut posts: Vec<Post> = payloads
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    if blocks > 0 {
        let named: Vec<(&str, &[u8])> = payloads
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        posts.extend(par2_set("lantern", &named, blocks));
    }
    Scenario::new(posts)
}

// What the verdicts a run compared were, so the run is known to have
// reached each of them.
#[derive(Default)]
struct Seen {
    gates: BTreeSet<&'static str>,
    verdicts: BTreeSet<&'static str>,
}

fn verdict_name(verdict: &Option<String>) -> &'static str {
    match verdict {
        None => "none",
        Some(error) if error.contains("first articles are missing") => "first articles",
        Some(_) => "health",
    }
}

#[tokio::test]
async fn the_kept_sample_gives_the_verdict_a_walk_of_every_file_gives() {
    let mut seen = Seen::default();
    for (scenario, seeds) in [
        (post(14, 3, 0), 0..4u64),
        (post(14, 3, 6), 4..8),
        (post(12, 2, 40), 8..12),
    ] {
        for seed in seeds {
            walk(&scenario, seed, 120, |step, result| {
                assert_eq!(result.kept, result.walked, "seed {seed} step {step}");
                if let Some(matches_read) = result.matches_read {
                    assert!(matches_read, "seed {seed} step {step}");
                }
                if let Some(gate) = result.gate {
                    seen.gates.insert(gate);
                }
                seen.verdicts.insert(verdict_name(&result.walked));
            })
            .await;
        }
    }
    assert_eq!(
        seen.gates,
        BTreeSet::from(["dead", "open", "undecided"]),
        "every gate reached"
    );
    assert_eq!(
        seen.verdicts,
        BTreeSet::from(["first articles", "health", "none"]),
        "every verdict reached"
    );
}

// Once the sample is kept, a lost article reads the head of its own file
// when it is that file's first and none otherwise, where a walk of the
// post reads every file's.
#[tokio::test]
async fn a_lost_article_reads_one_head_not_every_files() {
    let files = 1_000;
    let (_temp_dir, mut pipeline, _) = admit(&post(files, 2, 0)).await;
    lose_article(&mut pipeline, 0, 0);
    let before = heads_read();
    lose_article(&mut pipeline, 1, 1);
    assert_eq!(heads_read() - before, 0, "a later article");
    let before = heads_read();
    lose_article(&mut pipeline, 2, 0);
    assert_eq!(heads_read() - before, 1, "a first article");
    let before = heads_read();
    let walked = walked_verdict(&pipeline);
    assert_eq!(heads_read() - before, files, "the walk");
    assert_eq!(kept_verdict(&mut pipeline), walked);
    retire_pipeline_database(pipeline).await;
}
