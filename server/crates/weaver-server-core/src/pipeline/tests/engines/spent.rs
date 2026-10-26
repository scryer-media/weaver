// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a graph job spends is left out of what it hands over: recovery
// volumes, bound or only named so, and the pieces a join consumed.

use super::harness::{ARTICLE, Post, Scenario, par2_set, payload, run};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::Engine;

// A PAR2 volume whose first article is lost never says by its bytes which
// set it is, but the post names it a volume: it is spent all the same, and
// the job hands over the data member alone.
#[tokio::test]
async fn a_recovery_volume_whose_head_was_lost_is_spent_under_graph() {
    let data = payload(4 * ARTICLE, 41);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let graph = run(&Scenario::new(posts).lost(2, 0), Engine::Graph).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/lantern.bin".to_string()],
    );
    assert!(graph.files["lantern beacon/lantern.bin"] == data);
}

// While the root fetches recovery volumes for a lost article the job shows
// it is fetching repair data, not repairing; once the repair run is out it
// shows repairing, and is no longer fetching.
#[tokio::test]
async fn a_graph_job_fetches_repair_data_before_it_repairs() {
    let data = payload(4 * ARTICLE, 43);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let graph = run(&Scenario::new(posts).lost(0, 1), Engine::Graph).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert!(graph.files["lantern beacon/lantern.bin"] == data);
    let shown = &graph.graph().shown;
    assert!(
        shown.contains(&(JobStatus::Downloading, true)),
        "never shown fetching: {shown:?}"
    );
    assert!(
        shown.contains(&(JobStatus::Repairing, false)),
        "never shown repairing: {shown:?}"
    );
    assert!(
        !shown
            .iter()
            .any(|(status, fetching)| *fetching && *status != JobStatus::Downloading),
        "fetching shown outside the download: {shown:?}"
    );
}
