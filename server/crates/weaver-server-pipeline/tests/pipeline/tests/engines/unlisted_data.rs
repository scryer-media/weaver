// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A data file under a PAR2 set that loses some of its bytes: a listed
// article every server lost, or articles the NZB does not list, at the
// file's start, in its middle, or at the start of a file posted under a
// name the set does not describe. Each slice a hole touches needs a block,
// so the set counts it damaged, and counts only those. The health figure
// weighs what the NZB lists: a listed article lost counts against it, an
// article the NZB never listed counts on neither side.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};

// One data file of `articles` articles, posted as `posted_as`, under a set
// that describes it as `lantern.bin` with one block: too few for two holes,
// so the set rules on what it counted.
fn covered(posted_as: &str, articles: usize, seed: u8) -> Vec<Post> {
    let data = payload(articles * ARTICLE, seed);
    let mut posts = vec![Post::plain(posted_as, data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 1));
    posts
}

// The set ruled impossible on `damaged` slices against its one block, and
// the summary the history keeps says the same.
fn ruled(graph: &Outcome, damaged: u64, case: &str) {
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged,
            reachable: 1
        })],
        "{case}"
    );
    assert_eq!(graph.graph().damaged_slices, damaged, "{case}");
}

// The history's failed bytes are `failed`, and its health is what is left
// of the listed bytes.
fn health(graph: &Outcome, failed: u64, case: &str) {
    let row = graph.history.as_ref().expect("a history row");
    assert_eq!(row.failed_bytes, failed, "{case}");
    assert_eq!(
        u64::from(row.health),
        (row.total_bytes - failed) * 1000 / row.total_bytes,
        "{case}"
    );
}

// (a) Two listed articles every server lost: two damaged slices, and both
// articles' bytes against the health.
#[tokio::test]
async fn listed_articles_lost_count_as_damage_and_against_health() {
    let scenario = Scenario::new(covered("lantern.bin", 8, 81))
        .lost(0, 2)
        .lost(0, 5);
    let graph = run(&scenario).await;
    ruled(&graph, 2, "listed lost");
    health(&graph, 2 * ARTICLE as u64, "listed lost");
}

// (b) The file's first two articles are not in the NZB: the two slices
// they held are damaged, and the health weighs only what is listed.
#[tokio::test]
async fn an_unlisted_start_counts_as_damage_and_not_against_health() {
    let scenario = Scenario::new(covered("lantern.bin", 8, 81))
        .unlisted(0, 0)
        .unlisted(0, 1);
    let listed = scenario.spec().total_bytes;
    let graph = run(&scenario).await;
    ruled(&graph, 2, "unlisted start");
    health(&graph, 0, "unlisted start");
    assert_eq!(graph.history.as_ref().unwrap().total_bytes, listed);
}

// (c) Two articles in the middle are not in the NZB, in either order the
// articles land: two damaged slices, health untouched.
#[tokio::test]
async fn an_unlisted_middle_counts_as_damage_in_any_order() {
    for backwards in [false, true] {
        let mut scenario = Scenario::new(covered("lantern.bin", 8, 81))
            .unlisted(0, 3)
            .unlisted(0, 5);
        if backwards {
            scenario = scenario.backwards();
        }
        let case = format!("unlisted middle, backwards={backwards}");
        let graph = run(&scenario).await;
        ruled(&graph, 2, &case);
        health(&graph, 0, &case);
    }
}

// (d) A file posted under a name the set does not describe, without its
// first article, so its first bytes cannot say which file it is: the set
// counts only the slices its holes touch, not the whole file. Past 16 KiB
// too, where the bytes that identify it are all in the holes.
#[tokio::test]
async fn a_file_without_its_first_article_counts_only_its_holes() {
    let cases: [(&str, usize, Vec<u32>, u64); 3] = [
        ("small", 8, vec![0, 5], 2),
        ("past 16 KiB", 300, vec![0, 150], 2),
        (
            "past 16 KiB, first 260 unlisted",
            300,
            (0..260).collect(),
            260,
        ),
    ];
    for (case, articles, unlisted, damaged) in cases {
        let mut scenario = Scenario::new(covered("x7q.bin", articles, 82));
        for posted in unlisted {
            scenario = scenario.unlisted(0, posted);
        }
        let graph = run(&scenario).await;
        ruled(&graph, damaged, case);
        health(&graph, 0, case);
    }
}
