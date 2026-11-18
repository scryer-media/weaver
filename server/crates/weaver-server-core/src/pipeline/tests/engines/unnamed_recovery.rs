// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// PAR2 volumes posted under names that say nothing of what they are. The
// graph binds each to its recovery set by its bytes, and a file a recovery
// set holds is recovery, whatever its posted name: never a payload file the
// job is short of, and never failed bytes.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_volumes, payload, run};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::lines::Lines;

const MEMBER: &str = "ember.mkv";

// The payload, then an index and three volumes posted under random names.
fn posts() -> (Vec<u8>, Vec<Post>) {
    let data = payload(12 * ARTICLE + 17, 41);
    let mut posts = vec![Post::plain(MEMBER, data.clone())];
    let set = par2_volumes("ember", &[(MEMBER, &data)], 6, 3);
    let names = ["a3f9c1e07d", "b71e02c4aa", "5d0e9f3b12", "e62c8a7f40"];
    assert_eq!(set.len(), names.len(), "an index and three volumes");
    posts.extend(
        set.into_iter()
            .zip(names)
            .map(|(post, name)| Post::plain(name, post.bytes)),
    );
    (data, posts)
}

// A run, and that its census read no member short: a volume the set did
// not need is fetched only as far as its first article, and is no payload
// member that never fully arrived.
async fn run_census(scenario: &Scenario) -> Outcome {
    let (lines, guard) = Lines::capture_at(
        "weaver_server_core::pipeline::graph::status",
        tracing::Level::WARN,
    );
    let graph = run(scenario).await;
    drop(guard);
    assert_eq!(
        lines.of("delivering a member that never fully arrived"),
        Vec::<String>::new()
    );
    graph
}

fn assert_whole(graph: &Outcome, data: &[u8]) {
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(graph.status, Some(JobStatus::Complete));
    assert_eq!(
        graph.files.get(&format!("lantern beacon/{MEMBER}")),
        Some(&data.to_vec()),
        "{:?}",
        graph.files.keys().collect::<Vec<_>>()
    );
    let history = graph.history.as_ref().expect("a history row");
    assert_eq!(history.failed_bytes, 0, "{history:?}");
    assert_eq!(history.health, 1000, "{history:?}");
}

#[tokio::test]
async fn par2_volumes_under_random_names_beside_a_whole_payload_complete() {
    let (data, posts) = posts();
    let graph = run_census(&Scenario::new(posts)).await;
    assert_whole(&graph, &data);
}

// A volume posted twice, each copy under a random name: the set needs one
// of them, and the other is recovery too, never a payload file the job is
// short of.
#[tokio::test]
async fn a_par2_volume_posted_twice_under_random_names_completes() {
    let (data, mut posts) = posts();
    let again = Post::plain("0c4d7b9e21", posts.last().unwrap().bytes.clone());
    posts.insert(1, again);
    let graph = run_census(&Scenario::new(posts)).await;
    assert_whole(&graph, &data);
}

// A volume under a random name that lost an article beside a whole
// payload: the set it belongs to holds it, so its lost bytes are recovery
// the job did not need, not payload the job is short of.
#[tokio::test]
async fn a_par2_volume_under_a_random_name_that_lost_an_article_costs_nothing() {
    let (data, posts) = posts();
    let volume = posts.len() as u32 - 1;
    let graph = run_census(&Scenario::new(posts).lost(volume, 1)).await;
    assert_whole(&graph, &data);
}

#[tokio::test]
async fn par2_volumes_under_random_names_repair_a_damaged_payload() {
    let (data, posts) = posts();
    let graph = run_census(&Scenario::new(posts).damaged(0, 2).damaged(0, 7)).await;
    assert_whole(&graph, &data);
    assert!(
        matches!(
            graph.graph().outcomes.as_slice(),
            [Some(RepairOutcome::Repaired { .. })]
        ),
        "{:?}",
        graph.graph().outcomes
    );
}
