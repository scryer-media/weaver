// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job's remaining and wanted figures at every step of a run, from the
// figure it was admitted with on: remaining never above wanted, wanted never
// above the post, and remaining rising only when recovery is asked for,
// which wanted then grows by.

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Look {
    pub step: usize,
    pub remaining: u64,
    pub wanted: u64,
    pub total: u64,
    pub progress: f64,
    pub status: String,
}

// Where the walk pauses the job and resumes it, and whether the daemon
// restarts while it is paused.
#[derive(Clone, Copy)]
pub enum Pause {
    After(usize),
    RestartedAfter(usize),
}

pub struct Walk {
    pub admission: u64,
    pub looks: Vec<Look>,
    pub status: Option<JobStatus>,
    // The articles asked for more than once.
    pub twice: Vec<(u32, u32)>,
    // Every article asked for, counting each time.
    pub asked: usize,
    // Every article asked for, by file.
    pub requested: BTreeSet<(u32, u32)>,
    // The paused job's remaining and wanted before and after the restart.
    pub across_restart: Option<((u64, u64), (u64, u64))>,
}

fn look(pipeline: &Pipeline, step: usize, looks: &mut Vec<Look>) {
    let Some(report) = pipeline.job_report(JOB) else {
        return;
    };
    let seen = Look {
        step,
        remaining: report.remaining_bytes,
        wanted: report.wanted_bytes,
        total: report.total_bytes,
        progress: report.progress,
        status: format!("{:?}", report.status),
    };
    assert!(
        seen.remaining <= seen.wanted && seen.wanted <= seen.total,
        "invariant broken: {seen:?}"
    );
    let same = looks.last().is_some_and(|last| {
        (last.remaining, last.wanted, &last.status) == (seen.remaining, seen.wanted, &seen.status)
    });
    if !same {
        looks.push(seen);
    }
}

fn figures(pipeline: &Pipeline) -> Option<(u64, u64)> {
    pipeline
        .job_report(JOB)
        .map(|report| (report.remaining_bytes, report.wanted_bytes))
}

// The daemon goes down with the job paused and comes back from what reached
// the database, the job restored paused.
async fn restart_paused(
    temp_dir: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
) -> Pipeline {
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    retire_pipeline_database(pipeline).await;
    let backlog_base_bytes = 16 * 1024 * 1024;
    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(temp_dir, backlog_base_bytes, 1).await;
    scenario.prepare(&mut pipeline);
    pipeline
        .restore_job(crate::RestoreJobRequest {
            job_id: JOB,
            job_hash: [0; 32],
            spec: scenario.spec(),
            detected_archives: Default::default(),
            file_identities: Default::default(),
            status: JobStatus::Paused,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: Some(JobStatus::Downloading),
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();
    // A paused job parks once its recovery heard what arrived of it again,
    // on the turn after, as every turn asks.
    crate::pipeline::tests::hear_rewalks(&mut pipeline, JOB).await;
    pipeline.park_drained_jobs();
    pipeline
}

pub async fn walk(scenario: &Scenario, pause: Option<Pause>) -> Walk {
    let admission = scenario.spec().wanted_bytes();
    let (temp_dir, mut pipeline, _complete_dir) = admit(scenario).await;
    let mut across_restart = None;
    let mut looks = Vec::new();
    let mut requests = BTreeMap::new();
    let mut step = 0usize;
    let mut paused = false;
    let mut stalled = None;
    look(&pipeline, step, &mut looks);
    loop {
        while let Some((batch, plan)) = handed_out(&mut pipeline, scenario) {
            for work in batch {
                let article = (
                    work.segment_id.file_id.file_index,
                    work.segment_id.segment_number,
                );
                *requests.entry(article).or_insert(0usize) += 1;
                serve_from(
                    &mut pipeline,
                    JOB,
                    scenario,
                    article,
                    work.exclude_servers,
                    plan.as_ref(),
                )
                .await;
                hear_probe(&mut pipeline).await;
                step += 1;
                look(&pipeline, step, &mut looks);
            }
            let pause_here = pause.filter(|pause| {
                let (Pause::After(at) | Pause::RestartedAfter(at)) = *pause;
                step >= at
            });
            if !paused && let Some(pause) = pause_here {
                paused = true;
                pipeline.pump_decode_queue();
                pipeline.pause_job_runtime(JOB).unwrap();
                look(&pipeline, step, &mut looks);
                assert!(
                    !pipeline.graph_jobs.contains_key(&JOB),
                    "the pause released the graph"
                );
                if let Pause::RestartedAfter(_) = pause {
                    let before = figures(&pipeline).expect("the paused job reports");
                    pipeline = restart_paused(&temp_dir, pipeline, scenario).await;
                    let after = figures(&pipeline).expect("the restored job reports");
                    across_restart = Some((before, after));
                    look(&pipeline, step, &mut looks);
                }
                pipeline.resume_job_runtime(JOB).unwrap();
                look(&pipeline, step, &mut looks);
                crate::pipeline::tests::activate_job(&mut pipeline, JOB).await;
                look(&pipeline, step, &mut looks);
            }
        }
        if finished(&pipeline) {
            break;
        }
        let before = fingerprint(&pipeline, &requests);
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        look(&pipeline, step, &mut looks);
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
            look(&pipeline, step, &mut looks);
            continue;
        }
        if take_retry(&mut pipeline).await {
            continue;
        }
        let after = fingerprint(&pipeline, &requests);
        if before == after && stalled.replace(after.clone()) == Some(after.clone()) {
            panic!("stalled: {after}");
        }
    }
    let status = job_status_for_assert(&pipeline, JOB);
    retire_pipeline_database(pipeline).await;
    let asked = requests.values().sum();
    let requested = requests.keys().copied().collect();
    let twice = requests
        .into_iter()
        .filter(|(_, asked)| *asked > 1)
        .map(|(article, _)| article)
        .collect();
    Walk {
        admission,
        looks,
        status,
        twice,
        asked,
        requested,
        across_restart,
    }
}
