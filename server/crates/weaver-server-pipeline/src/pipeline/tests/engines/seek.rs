// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Seeks: the RAR store's bids bringing volumes forward
// into the root's window, the PAR2 object's recovery seeks promoted over
// every bid, lost answers, a store read out of a stream, a seek lifted
// through a store's extent map, a store nothing names, an open-ended
// recovery seek, and a recovery seek standing across a restart. Where the
// engines part, the row names how.

use super::harness::{JOB, Scenario, finished, serve};
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::tests::{
    insert_active_job, new_direct_pipeline_with_backlog, pump_pipeline_runtime_queues,
    retire_pipeline_database,
};
use std::collections::BTreeMap;

fn recovering(pipeline: &Pipeline) -> bool {
    pipeline.jobs.get(&JOB).is_some_and(|state| {
        state
            .articles
            .active()
            .unwrap()
            .download_queue
            .count_matching(|work| work.is_recovery)
            > 0
    })
}

// How a restart takes the pipeline down: drained, as a graceful stop does,
// or dropped where it stands, as a crash does.
#[derive(Clone, Copy)]
pub enum Stop {
    Drained,
    Crashed,
}

// A graph run restarted once, after the first article that leaves `due`
// true.
type SeekOutcome = (
    Option<JobStatus>,
    Vec<(u32, u32)>,
    BTreeMap<(u32, u32), usize>,
);

async fn run_restarting_once(
    scenario: &Scenario,
    due: impl Fn(&Pipeline) -> bool,
    stop: Stop,
) -> SeekOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(&temp_dir, backlog_base_bytes(), 1).await;
    // Admitted as the whole run's job is: its queues built as admission
    // builds a graph job's.
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    let mut order = Vec::new();
    let mut requests = BTreeMap::new();
    let mut restarted = false;
    loop {
        // A job whose restored recovery object is still to hear what of
        // its volumes arrived fetches nothing, as the scheduler holds it.
        while let Some(work) = pipeline
            .graph_jobs
            .get(&JOB)
            .is_none_or(|job| !job.rewalks_owed())
            .then(|| {
                pipeline
                    .jobs
                    .get_mut(&JOB)
                    .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
            })
            .flatten()
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            *requests.entry(article).or_insert(0usize) += 1;
            order.push(article);
            serve(&mut pipeline, scenario, article).await;
            if !restarted && due(&pipeline) {
                restarted = true;
                pipeline = restart(&temp_dir, pipeline, scenario, stop).await;
            }
        }
        if finished(&pipeline) {
            break;
        }
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if finished(&pipeline) {
            break;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        let queued = pipeline.jobs.get(&JOB).map_or(0, |state| {
            state.articles.active().unwrap().download_queue.len()
        });
        assert!(queued > 0, "the restarted job stalled");
    }
    assert!(restarted, "the run restarted");
    let status = crate::pipeline::tests::job_status_for_assert(&pipeline, JOB);
    retire_pipeline_database(pipeline).await;
    (status, order, requests)
}

fn backlog_base_bytes() -> usize {
    16 * 1024 * 1024
}

// The pipeline goes down as `stop` says and a new one restores the job from
// what reached the database, as `restart.rs` does after every article.
async fn restart(
    temp_dir: &tempfile::TempDir,
    mut pipeline: Pipeline,
    scenario: &Scenario,
    stop: Stop,
) -> Pipeline {
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    match stop {
        Stop::Drained => pipeline.drain().await,
        Stop::Crashed => pipeline.flush_graph_checkpoint(JOB),
    }
    let db = pipeline.db.clone();
    retire_pipeline_database(pipeline).await;
    drop(db);
    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(temp_dir, backlog_base_bytes(), 1).await;
    pipeline
        .restore_job(crate::RestoreJobRequest {
            job_id: JOB,
            job_hash: [0; 32],
            spec: scenario.spec(),
            detected_archives: Default::default(),
            file_identities: Default::default(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();
    assert!(pipeline.graph_jobs.contains_key(&JOB), "the job restored");
    pipeline
}

pub async fn restart_recovery_seek(scenario: &Scenario, stop: Stop) -> SeekOutcome {
    run_restarting_once(scenario, recovering, stop).await
}

pub async fn restart_after_announcement(scenario: &Scenario, posted: u32) -> SeekOutcome {
    let announced = |pipeline: &Pipeline| {
        pipeline.graph_jobs.get(&JOB).is_some_and(|job| {
            job.tree()
                .namespace(weaver_graph::NodeId::ROOT)
                .is_some_and(|root| root.records().any(|member| member.id.0 >= posted))
        })
    };
    run_restarting_once(scenario, announced, Stop::Drained).await
}
