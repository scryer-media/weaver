// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a graph job spends is left out of what it hands over: recovery
// volumes, bound or only named so, and the pieces a join consumed.

use super::harness::{ARTICLE, Post, Scenario, par2_set, payload, run};
use crate::jobs::model::JobStatus;

// A PAR2 volume whose first article is lost never says by its bytes which
// set it is, but the post names it a volume: it is spent all the same, and
// the job hands over the data member alone.
#[tokio::test]
async fn a_recovery_volume_whose_head_was_lost_is_spent() {
    let data = payload(4 * ARTICLE, 41);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let graph = run(&Scenario::new(posts).lost(2, 0)).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/lantern.bin".to_string()],
    );
    assert!(graph.files["lantern beacon/lantern.bin"] == data);
}

// A consumed input that cannot be removed, held open by a scanner or
// refused by a share, costs the delivery nothing: the job completes with
// its payload delivered, and the input stays in the working directory.
// The input is made a directory with an entry in it, which no file
// removal takes away on any host or under any user.
#[tokio::test]
async fn a_consumed_input_that_cannot_be_removed_stays_and_the_job_completes() {
    let data = payload(4 * ARTICLE, 47);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let (pinned_tx, pinned_rx) = std::sync::mpsc::channel();
    *crate::pipeline::completion::finalize::BEFORE_SPENT_REMOVAL
        .lock()
        .unwrap() = Some(Box::new(move |spent: &[std::path::PathBuf]| {
        let pinned = spent
            .iter()
            .find(|path| path.is_file())
            .expect("a recovery set is spent before delivery")
            .clone();
        std::fs::remove_file(&pinned).unwrap();
        std::fs::create_dir(&pinned).unwrap();
        std::fs::write(pinned.join("held"), b"held").unwrap();
        let _ = pinned_tx.send(pinned.file_name().unwrap().to_owned());
    }));
    let graph = run(&Scenario::new(posts)).await;
    let pinned = pinned_rx
        .try_recv()
        .expect("the delivery removed its consumed inputs");
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/lantern.bin".to_string()],
    );
    assert!(graph.files["lantern beacon/lantern.bin"] == data);
    let left = format!("/{}/held", pinned.to_string_lossy());
    assert!(
        graph
            .retained_files
            .keys()
            .any(|path| path.ends_with(&left)),
        "the consumed input left the working directory: {:?}",
        graph.retained_files.keys().collect::<Vec<_>>()
    );
}

// While the root fetches recovery volumes for a lost article the job shows
// it is fetching repair data, not repairing; once the repair run is out it
// shows repairing, and is no longer fetching.
#[tokio::test]
async fn a_graph_job_fetches_repair_data_before_it_repairs() {
    let data = payload(4 * ARTICLE, 43);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let graph = run(&Scenario::new(posts).lost(0, 1)).await;
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
