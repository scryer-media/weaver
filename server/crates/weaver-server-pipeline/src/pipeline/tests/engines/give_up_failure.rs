// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job that fails while a store it gave up still has volumes to read back.

use super::super::{job_status_for_assert, pump_pipeline_runtime_queues, settle_failed_releases};
use super::harness::{ARTICLE, Post, Scenario, admit, par2_set, payload, serve};
use super::rar_fixtures::posts;
use super::state::JOB;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::scenarios::encrypted_store_set;
use weaver_server_base::jobs::ids::SegmentId;

const SECRET: &str = "moonlit-harbour";
// The post's recovery volume.
const RECOVERY: u32 = 5;

// What the job and its graph said at the failure and after it.
pub struct FailedMidGiveUp {
    // The give-up had volumes left as the job failed.
    pub giving_up_at_failure: bool,
    // The graph had nothing out, so the failure kept it unstopped.
    pub kept_unstopped: bool,
    // After the failure: the give-up has volumes left, and it holds the loop.
    pub giving_up_after: bool,
    pub held_after: bool,
    // Where the job ended once its mail and its release were heard.
    pub status: Option<JobStatus>,
}

// An encrypted stored RAR set over four volumes, the second never posted,
// beside PAR2 whose one recovery block is lost too. The loss of the last
// recovery article leaves the job quiet, and the store gives its volumes up
// a turn at a time. The job fails before the next turn is heard, as a ruling
// that leaves it short fails it.
pub async fn fail_mid_give_up() -> FailedMidGiveUp {
    let body = payload(24 * ARTICLE + 77, 31);
    let volumes = encrypted_store_set("beacon.bin", &body, 4, SECRET, Some(SECRET), false);
    let absent = volumes[1].1.len().div_ceil(ARTICLE) as u32;
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut all: Vec<Post> = posts(volumes.clone());
    all.extend(par2_set("silver.horizon", &files, 1));
    let mut scenario = Scenario::new(all).password(SECRET);
    for ordinal in 0..absent {
        scenario = scenario.lost(1, ordinal);
    }
    let (_dir, mut pipeline, _) = admit(&scenario).await;
    // Every article is served but the recovery volume's, which is held
    // until nothing else of the job moves.
    let mut held: Vec<SegmentId> = Vec::new();
    loop {
        let mut moved = false;
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            moved = true;
            let segment = work.segment_id;
            if segment.file_id.file_index == RECOVERY {
                held.push(segment);
                continue;
            }
            serve(
                &mut pipeline,
                &scenario,
                (segment.file_id.file_index, segment.segment_number),
            )
            .await;
        }
        pipeline.check_job_completion(JOB).await;
        while pipeline.graph_jobs[&JOB].awaits_mail() {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            moved = true;
        }
        if !moved {
            break;
        }
    }
    assert!(!held.is_empty(), "the job asked for its recovery volume");
    assert!(!pipeline.graph_jobs[&JOB].giving_up());
    // The recovery articles are lost; the last leaves the job quiet, and
    // the store's give-up reads back its first volume on that turn.
    for segment in held {
        pipeline.book_failed_segment(segment);
    }
    let graph = &pipeline.graph_jobs[&JOB];
    let giving_up_at_failure = graph.giving_up();
    let kept_unstopped = !graph.holds_retirement();
    pipeline.fail_job(JOB, "PAR2 recovery failed".to_string());
    let graph = pipeline.graph_jobs.get(&JOB);
    let giving_up_after = graph.is_some_and(|graph| graph.giving_up());
    let held_after = graph.is_some_and(|graph| graph.held_by_work());
    // The give-up's wake was rung before the failure: it is heard, with the
    // rest of the job's mail and its release. A give-up the failure left
    // holding the loop would never be heard out, so the pipeline is not
    // pumped for it and the caller is told.
    if !held_after {
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_failed_releases(&mut pipeline).await;
    }
    FailedMidGiveUp {
        giving_up_at_failure,
        kept_unstopped,
        giving_up_after,
        held_after,
        status: job_status_for_assert(&pipeline, JOB),
    }
}
