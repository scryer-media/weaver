// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Dead posts fail fast, in two vocabularies. The usenet feed rules from
// the NZB and its servers alone: the first-article sample, then the post's
// health against its critical line, with a STAT round's projection naming
// what took it there. The graph rules from what its feed refused: a
// recovery set that can no longer reach enough blocks for the ranges it was
// refused rules impossible, and the job ends on its words.

use super::harness::*;
use super::recovery_fixtures::dead_posts::files;
use crate::jobs::model::JobStatus;

#[tokio::test]
async fn committed_feed_verdict_survives_restart_without_fetching_abandoned_files() {
    let first: Vec<_> = (0..10).map(|file| (file, 0)).collect();
    let scenario = (0..10).fold(Scenario::new(files()).first(&first), |scenario, file| {
        scenario.lost(file, 0)
    });
    let mut committed = false;
    let restarted = super::restart::run_restarting_at(
        &scenario,
        &mut |rows| {
            let verdict = rows
                .feed_verdict
                .as_ref()
                .expect("the crash follows a committed feed verdict");
            assert!(verdict.reason.contains("first articles are missing"));
            assert_eq!(verdict.abandoned_files, (0..10).collect());
            committed = true;
        },
        super::restart::Down::Crash,
        &|asked| asked == 10,
    )
    .await;
    assert!(committed);
    assert_eq!(restarted.restarts, 1);
    assert_eq!(restarted.status, Some(JobStatus::Partial));
    for file in 0..10 {
        let fetched: Vec<_> = restarted
            .order
            .iter()
            .copied()
            .filter(|(index, _)| *index == file)
            .collect();
        assert_eq!(fetched, vec![(file, 0)], "abandoned file {file} restarted");
    }
    for file in 10..12u8 {
        let suffix = format!("lantern{file:02}.mkv");
        let (_, delivered) = restarted
            .files
            .iter()
            .find(|(name, _)| name.ends_with(&suffix))
            .expect("healthy files still finish");
        assert_eq!(*delivered, payload(4 * ARTICLE, file));
    }
}

// A job whose root is done has nothing downloading for the feed's health
// probe to rule on, so the feed forgets its probe as the job leaves the root,
// while the job itself is still in the pipeline.
#[tokio::test]
async fn the_feed_forgets_its_probe_when_the_job_leaves_the_root() {
    use crate::pipeline::graph::root::phase::RootPhase;
    use crate::pipeline::tests::{hear_graph_mail, retire_pipeline_database};

    let post = Post::plain("lantern.mkv", payload(4 * ARTICLE, 7));
    let articles = post.bytes.len().div_ceil(ARTICLE) as u32;
    let scenario = Scenario::new(vec![post]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    pipeline.post_probes.states.insert(
        JOB,
        crate::pipeline::post_health::ProbeState {
            rounds: 1,
            ..Default::default()
        },
    );
    for ordinal in 0..articles {
        serve(&mut pipeline, &scenario, (0, ordinal)).await;
        hear_graph_mail(&mut pipeline, JOB).await;
    }
    assert_eq!(pipeline.graph_jobs[&JOB].phase(), RootPhase::Done);
    assert!(pipeline.jobs.contains_key(&JOB));
    assert!(!pipeline.post_probes.states.contains_key(&JOB));
    retire_pipeline_database(pipeline).await;
}
