// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The pipeline side of the stall rule's scenarios: what a test of a job that
// waits with nothing pending reads and drives inside the pipeline.

use tracing::subscriber::DefaultGuard;

use super::actor_drive::drive;
use super::harness::{Held, JOB, Scenario, admit, run_holding_where};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::lines::Lines;
use crate::pipeline::tests::{job_status_for_assert, nzb_zstd_for, segmented_job_spec};
use crate::{JobId, Pipeline};

pub const STALLED: &str =
    "graph job waits with nothing pending; its objects are told nothing more arrives";
pub const STALL_ENDED: &str = "graph job still waits with nothing pending after its objects heard nothing more arrives; it ends as delivered";
pub const INPUT_ENDS: &str =
    "graph job waits on nothing but runs no input reaches; their input ends";

pub fn warnings() -> (Lines, DefaultGuard) {
    Lines::capture_at(
        "weaver_server_pipeline::pipeline::graph",
        tracing::Level::WARN,
    )
}

pub fn stall_lines(lines: &Lines) -> Vec<String> {
    let mut heard = lines.of(STALLED);
    heard.extend(lines.of(STALL_ENDED));
    heard.extend(lines.of(INPUT_ENDS));
    heard
}

// Whether `status` is one a job shows before it ends.
pub fn in_progress(status: &JobStatus) -> bool {
    status.in_progress()
}

// One NZB admitted at a time, and a small job queued behind `JOB`.
async fn one_at_a_time_with_one_behind(pipeline: &mut Pipeline) -> JobId {
    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    let next = JobId(JOB.0 + 1);
    let spec = segmented_job_spec("Queued behind", "payload.bin", &[512; 4]);
    let nzb = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            next,
            spec,
            std::path::PathBuf::from("queued-behind.nzb"),
            nzb,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.nzb_admitted.contains(&next));
    next
}

// Queue turns, hearing every database answer, until no slot moves.
async fn settle_slots(pipeline: &mut Pipeline) {
    loop {
        pipeline.run_queue_intents().await;
        let mut moved = false;
        while pipeline.db_replies_out() {
            let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
            moved |= matches!(
                reply,
                crate::pipeline::db_reply::DbReply::Slot {
                    committed: true,
                    ..
                }
            );
            pipeline.handle_db_reply(reply).await;
        }
        if !moved && !pipeline.restores_waiting {
            return;
        }
    }
}

// The stalled job of `scenario` ends Failed with one NZB admitted at a
// time, and gives its admission to the job queued behind it once the
// failure is published.
pub async fn stalled_job_gives_its_admission_to_the_next(scenario: &Scenario) {
    let (_temp_dir, mut pipeline, _) = admit(scenario).await;
    let next = one_at_a_time_with_one_behind(&mut pipeline).await;
    let mut events = pipeline.event_tx.subscribe();
    drive(&mut pipeline, scenario, JOB, |_| false).await;
    assert!(
        matches!(
            job_status_for_assert(&pipeline, JOB),
            Some(JobStatus::Failed { .. })
        ),
        "{:?}",
        job_status_for_assert(&pipeline, JOB)
    );
    crate::pipeline::tests::settle_failed_releases(&mut pipeline).await;
    // The row leaves the queue once its history archive commits, and the
    // failure is published then.
    loop {
        match events.recv().await.unwrap() {
            crate::events::model::PipelineEvent::JobFailed { job_id, .. } if job_id == JOB => {
                break;
            }
            _ => {}
        }
    }
    settle_slots(&mut pipeline).await;
    assert!(!pipeline.nzb_admitted.contains(&JOB));
    assert!(pipeline.nzb_admitted.contains(&next));
}

// The job of `scenario` under paused post-processing: drained of its
// downloads and mail, two gate turns leave it in progress and hear no stall.
// Once post-processing resumes it runs to its end. Returns its status and
// the stall lines heard, before the resume and in all.
pub async fn under_paused_post_processing(
    scenario: &Scenario,
) -> (Vec<String>, Option<JobStatus>, Vec<String>) {
    let (_temp_dir, mut pipeline, _) = admit(scenario).await;
    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.pause_post_processing().await.unwrap();
    pipeline.apply_queue_intents().await;
    let (lines, guard) = warnings();
    let drained = |pipeline: &Pipeline| {
        !pipeline.job_has_pending_download_pipeline_work(JOB)
            && pipeline
                .graph_jobs
                .get(&JOB)
                .is_some_and(|job| !job.awaits_mail())
    };
    drive(&mut pipeline, scenario, JOB, drained).await;
    for _ in 0..2 {
        pipeline.check_job_completion(JOB).await;
    }
    let paused = stall_lines(&lines);
    assert!(
        job_status_for_assert(&pipeline, JOB).is_some_and(|status| status.in_progress()),
        "{:?}",
        job_status_for_assert(&pipeline, JOB)
    );
    handle.resume_post_processing().await.unwrap();
    pipeline.apply_queue_intents().await;
    drive(&mut pipeline, scenario, JOB, |_| false).await;
    drop(guard);
    (
        paused,
        job_status_for_assert(&pipeline, JOB),
        lines.of(STALLED),
    )
}

// The job of `scenario` driven until its repair is out on its thread: two
// gate turns then leave it in progress with the repair still out. Returns
// its status at the end and every stall line heard.
pub async fn with_its_repair_out(scenario: &Scenario) -> (Option<JobStatus>, Vec<String>) {
    let (_temp_dir, mut pipeline, _) = admit(scenario).await;
    let (lines, guard) = warnings();
    let repairing = |pipeline: &Pipeline| {
        pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.repair_out())
    };
    drive(&mut pipeline, scenario, JOB, repairing).await;
    assert!(repairing(&pipeline), "the repair never went out");
    for _ in 0..2 {
        pipeline.check_job_completion(JOB).await;
    }
    assert!(repairing(&pipeline));
    assert!(
        job_status_for_assert(&pipeline, JOB).is_some_and(|status| status.in_progress()),
        "{:?}",
        job_status_for_assert(&pipeline, JOB)
    );
    drive(&mut pipeline, scenario, JOB, |_| false).await;
    drop(guard);
    (job_status_for_assert(&pipeline, JOB), stall_lines(&lines))
}

// What a job whose run was blocked for good came to: its status, the stall
// lines heard, and the most the job asked to hear of a run beginning to
// wait against the notices it was sent.
pub struct BlockedForGood {
    pub status: Option<JobStatus>,
    pub lines: Vec<String>,
    pub input_ended: Vec<String>,
    pub asks: u64,
    pub told: u64,
}

// The job of `scenario`, with the seam that never takes what its volumes
// lost as lost for good, one NZB admitted at a time and a job queued
// behind: driven to its end, its completion published, and its admission
// handed on.
pub async fn blocked_for_good_and_handed_on(scenario: &Scenario) -> BlockedForGood {
    let (_temp_dir, mut pipeline, _) = admit(scenario).await;
    pipeline
        .graph_jobs
        .get_mut(&JOB)
        .expect("the job's graph is built at admission")
        .withhold_lost_for_good();
    let next = one_at_a_time_with_one_behind(&mut pipeline).await;
    let (lines, guard) = warnings();
    let mut events = pipeline.event_tx.subscribe();
    let counts = std::cell::Cell::new((0, 0));
    drive(&mut pipeline, scenario, JOB, |pipeline| {
        if let Some(job) = pipeline.graph_jobs.get(&JOB) {
            counts.set((job.watch().asks(), job.watch().told()));
        }
        false
    })
    .await;
    drop(guard);
    let status = job_status_for_assert(&pipeline, JOB);
    if status == Some(JobStatus::Complete) {
        loop {
            match events.recv().await.unwrap() {
                crate::events::model::PipelineEvent::JobCompleted { job_id, .. }
                    if job_id == JOB =>
                {
                    break;
                }
                _ => {}
            }
        }
        settle_slots(&mut pipeline).await;
        assert!(!pipeline.nzb_admitted.contains(&JOB));
        assert!(pipeline.nzb_admitted.contains(&next));
    }
    let (asks, told) = counts.get();
    BlockedForGood {
        status,
        lines: stall_lines(&lines),
        input_ended: lines.of(INPUT_ENDS),
        asks,
        told,
    }
}

// `scenario` with every article of post `held` out with the server: once
// nothing moves, the job's one run reads and waits on its shelf, and the
// job has asked nothing of the wait.
pub async fn waiting_on_articles_still_out(scenario: &Scenario, held: u32) -> Held {
    run_holding_where(
        scenario,
        |article| article.0 == held,
        |pipeline| {
            let job = &pipeline.graph_jobs[&JOB];
            assert!(job.reads(), "the archive's run reads");
            assert!(job.runs_blocked(), "the run waits on its shelf");
            assert_eq!((job.watch().asks(), job.watch().told()), (0, 0));
        },
    )
    .await
}
