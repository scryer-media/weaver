// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's way out of the pipeline: what fails it, what holds it at its
// barriers, and the order in which its history row, its terminal event and
// its delivery land.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;

use super::super::restore_history::{hold_database_writer, wait_for_job_event};
use super::super::script_events::enable_queue_script;
use super::super::{
    age_job_days, insert_active_job, job_status_for_assert, new_direct_pipeline, sample_nzb_zstd,
    settle_inflight_moves, two_server_retention_client,
};
use super::harness::{
    ARTICLE, JOB, Post, Scenario, admit, admit_with, collect, payload, serve_job,
};
use crate::jobs::ids::JobId;
use crate::jobs::model::JobStatus;
use crate::pipeline::{
    DownloadBatchLease, DownloadLaneMode, DownloadResult, OwnedDownloadLaneEvent, Pipeline,
    PipelineEvent, TerminalPostProcessingEvent, is_terminal_status,
};
use crate::{SchedulerError, SchedulerHandle};

fn handle(pipeline: &Pipeline) -> SchedulerHandle {
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
}

fn ended(pipeline: &Pipeline, job: JobId) -> bool {
    matches!(
        job_status_for_assert(pipeline, job),
        Some(JobStatus::Complete | JobStatus::Partial | JobStatus::Failed { .. }) | None
    )
}

// Plays the server for `job` and runs its completion gate, a run's mail and
// the moves it starts, as the orchestrator does, until `until` holds or the
// job has ended. A round that changes nothing twice over is a stall.
async fn drive(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    job: JobId,
    until: impl Fn(&Pipeline) -> bool,
) {
    let look = |pipeline: &Pipeline| {
        let queued = pipeline.jobs.get(&job).map(|state| {
            (
                state.download_queue.len(),
                state.recovery_queue.len(),
                state.status.clone(),
            )
        });
        format!(
            "{queued:?} {} {}",
            pipeline.inflight_moves.contains(&job),
            pipeline.pending_completion_checks.len()
        )
    };
    let mut stalled = None;
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&job)
            .and_then(|state| state.download_queue.pop())
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            serve_job(pipeline, job, scenario, article).await;
        }
        if until(pipeline) || ended(pipeline, job) {
            return;
        }
        let before = look(pipeline);
        pipeline.check_job_completion(job).await;
        pipeline.pump_decode_queue();
        while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
            pipeline.pump_decode_queue();
        }
        if until(pipeline) || ended(pipeline, job) {
            return;
        }
        if pipeline.inflight_moves.contains(&job) {
            settle_inflight_moves(pipeline).await;
            continue;
        }
        if pipeline
            .graph_jobs
            .get(&job)
            .is_some_and(|graph| graph.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        let after = look(pipeline);
        if before == after && stalled.replace(after.clone()) == Some(after.clone()) {
            panic!("the job stalled: {after}");
        }
    }
}

// The job's history row, once every queued write has landed.
async fn history(pipeline: &Pipeline, job: JobId) -> crate::JobHistoryRow {
    pipeline.db.flush_write_queue().await.unwrap();
    pipeline
        .db
        .get_job_history(job.0)
        .unwrap()
        .expect("the job is archived")
}

// Admit the job the way a submission is admitted, so its durable candidate
// row records the terminal cause the archive writes.
fn admitted_job_id(pipeline: &Pipeline, spec: &crate::jobs::model::JobSpec) -> JobId {
    let request = crate::DuplicateAdmissionRequest {
        evidence: crate::FingerprintEvidence::from_validated_spec(spec, [60; 32]),
        mode: crate::DuplicateMode::Score,
        semantic: crate::SemanticDuplicate::from_source(&spec.name, 10),
        semantic_source: Some(crate::SemanticCandidateSource {
            nzb_zstd: sample_nzb_zstd(),
            filename: Some(format!("{}.nzb", spec.name)),
            password: None,
            category: None,
            metadata: vec![],
        }),
        origin: crate::SubmissionOrigin::Api,
        idempotency: None,
        policy: crate::DuplicatePolicy::default(),
    };
    let crate::DuplicateAdmission::Accepted { job_id, .. } =
        pipeline.db.admit_duplicate_submission(&request).unwrap()
    else {
        panic!("the job must be admitted");
    };
    job_id
}

// The last server that may serve the job refuses the pool's credentials: the
// articles only it could serve are booked missing, not retried, and the job
// fails for the server rather than for articles missing from Usenet.
#[tokio::test]
async fn the_last_server_refusing_credentials_fails_the_job() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 1))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    // Index 0 keeps five days of retention, index 1 everything; a job ten
    // days old is index 1's alone, and index 1 refuses the credentials.
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    age_job_days(&mut pipeline, job, 10);
    pipeline.clear_job_retention_excludes(job);
    assert_eq!(pipeline.job_retention_excludes(job).as_slice(), &[0]);
    pipeline
        .nntp
        .pool()
        .health()
        .lock()
        .await
        .record_failure(1, true);
    assert!(pipeline.nntp.pool().refuses_credentials(1));

    // The first queued article is leased and answered with what the pool
    // says for it, the way a lane task answers it.
    let work = pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    pipeline.hold_test_lane_works(vec![work.clone()], false);
    let effective_exclude_servers = pipeline.effective_exclude_servers(job, &work.exclude_servers);
    let Err(error) = pipeline
        .nntp
        .try_acquire_blocking_body_lane(&[], &effective_exclude_servers)
    else {
        panic!("a lane opened with no server that may serve the article");
    };
    let lease = DownloadBatchLease {
        lane_id: 0,
        job_id: job,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: Vec::new(),
        completion_critical: false,
        effective_exclude_servers,
        dial_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works: vec![work],
    };
    pipeline.active_download_connections = 1;
    pipeline.active_download_passes.insert(job);
    pipeline.active_download_connections_by_job.insert(job, 1);
    pipeline.note_download_lane_started(DownloadLaneMode::Sequential);
    let mut pending: VecDeque<DownloadResult> = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed { lease, error },
        &mut pending,
    );
    assert_eq!(pending.len(), 1, "the refused article takes the answer");
    assert_eq!(pending[0].source_server_idx, Some(1));
    for result in pending {
        pipeline.process_released_download_done(result).await;
    }
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        4,
        "every article is booked, none retried"
    );
    assert_eq!(pipeline.pending_retries_by_job.get(&job).copied(), None);
    assert_eq!(pipeline.infrastructure_retries.len(), 0);

    drive(&mut pipeline, &scenario, job, |_| false).await;

    assert!(
        matches!(
            job_status_for_assert(&pipeline, job),
            Some(JobStatus::Failed { .. })
        ),
        "{:?}",
        job_status_for_assert(&pipeline, job)
    );
    assert_eq!(history(&pipeline, job).await.status, "failed");
    assert_eq!(
        pipeline
            .db
            .semantic_candidate_snapshot(job)
            .unwrap()
            .and_then(|candidate| candidate.terminal_cause),
        Some(crate::jobs::SemanticTerminalCause::ServerAuthQuotaOrOutage)
    );
}

// A queue script subscribed to NZB_DOWNLOADED: the job's last decode brings
// it to the barrier, where it waits for the script instead of moving.
#[tokio::test]
async fn the_last_decode_holds_the_job_at_the_nzb_downloaded_barrier() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(3 * ARTICLE, 2))]);
    let (_temp_dir, mut pipeline, _) = admit_with(&scenario, |pipeline, root| {
        enable_queue_script(&pipeline.db, root);
    })
    .await;
    while let Some(work) = pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|state| state.download_queue.pop())
    {
        let article = (
            work.segment_id.file_id.file_index,
            work.segment_id.segment_number,
        );
        serve_job(&mut pipeline, JOB, &scenario, article).await;
        // The tail of every decode: the last one ends the download pass.
        if pipeline.job_decode_stage_drained(JOB) {
            pipeline.maybe_finish_download_pass(JOB);
        }
    }
    loop {
        if let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
            pipeline.pump_decode_queue();
            continue;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|graph| graph.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        break;
    }

    assert!(
        pipeline.queue_script_waiters.contains(&JOB),
        "the barrier is not held; status {:?}",
        job_status_for_assert(&pipeline, JOB)
    );
    assert!(pipeline.inflight_moves.is_empty());
    let TerminalPostProcessingEvent::QueueAdmitted(id) = pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    else {
        panic!("expected queue admission");
    };
    assert_eq!(id, JOB);
    pipeline.handle_queue_scripts_admitted(id);
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::AwaitingQueueScripts)
    );
}

// The job downloaded and settled, its final move started and not yet
// heard back: the moment just before it delivers.
async fn at_the_move(scenario: &Scenario) -> (tempfile::TempDir, Pipeline, std::path::PathBuf) {
    let (temp_dir, mut pipeline, complete_dir) = admit(scenario).await;
    drive(&mut pipeline, scenario, JOB, |pipeline| {
        pipeline.inflight_moves.contains(&JOB)
    })
    .await;
    assert!(
        pipeline.inflight_moves.contains(&JOB),
        "the move did not start; status {:?}",
        job_status_for_assert(&pipeline, JOB)
    );
    (temp_dir, pipeline, complete_dir)
}

fn delivered() -> Scenario {
    Scenario::new(vec![Post::plain("ember.bin", payload(3 * ARTICLE, 3))])
}

// The completion event is heard only once the history row is readable: a
// subscriber that reacts to it finds the row through the history reads, and
// the job no longer listed live.
#[tokio::test]
async fn history_is_archived_before_the_job_is_announced_complete() {
    let scenario = delivered();
    let (_temp_dir, mut pipeline, complete_dir) = at_the_move(&scenario).await;
    let mut events = pipeline.event_tx.subscribe();
    let release = hold_database_writer(&pipeline);
    settle_inflight_moves(&mut pipeline).await;

    let mut files = std::collections::BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    assert_eq!(files.len(), 1, "the job delivered: {:?}", files.keys());
    let mut heard = Vec::new();
    while let Ok(event) = events.try_recv() {
        heard.push(event);
    }
    assert!(
        !heard
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobCompleted { .. })),
        "completion must not be announced while the history archive is still queued"
    );
    assert!(pipeline.db.get_job_history(JOB.0).unwrap().is_none());

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == JOB),
    )
    .await;
    assert!(
        pipeline
            .shared_state
            .list_jobs()
            .iter()
            .all(|info| info.job_id != JOB || is_terminal_status(&info.status)),
        "the job is published as finished before its terminal event"
    );
    // No flush: the row is readable as soon as the event is heard.
    assert_eq!(
        pipeline
            .db
            .get_job_history(JOB.0)
            .unwrap()
            .map(|row| row.status),
        Some("complete".to_string())
    );
    let listed = pipeline
        .db
        .list_job_history(&crate::HistoryFilter::default())
        .unwrap();
    assert!(listed.iter().any(|row| row.job_id == JOB.0));
}

// A job retiring into history refuses every mutation until its archive is
// acknowledged, and none of them moves the queue's revision.
#[tokio::test]
async fn a_retiring_job_refuses_mutations_until_its_history_commits() {
    let scenario = delivered();
    let (_temp_dir, mut pipeline, _) = at_the_move(&scenario).await;
    let handle = handle(&pipeline);
    let mut events = handle.subscribe_events();
    let release = hold_database_writer(&pipeline);
    settle_inflight_moves(&mut pipeline).await;

    assert!(pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .any(|row| row.id == JOB && row.report.retiring)
    }));
    let watermark = handle.job_revision();
    assert!(matches!(
        handle.pause_job(JOB).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle.resume_job(JOB).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle
            .update_job(
                JOB,
                crate::JobUpdate {
                    category: crate::jobs::FieldUpdate::Set("changed-category".into()),
                    ..Default::default()
                }
            )
            .await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle.cancel_job(JOB).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle.cancel_semantic_bad(JOB).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert_eq!(handle.job_revision(), watermark);

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == JOB),
    )
    .await;
    assert_eq!(history(&pipeline, JOB).await.status, "complete");
}

// The final move cannot be made: the job fails with the move's reason, and
// nothing is delivered.
#[tokio::test]
async fn a_final_move_that_fails_fails_the_job() {
    let scenario = delivered();
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    // The complete directory is a file: nothing can be delivered under it.
    std::fs::remove_dir_all(&complete_dir).unwrap();
    std::fs::write(&complete_dir, b"").unwrap();
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    match job_status_for_assert(&pipeline, JOB) {
        Some(JobStatus::Failed { error }) => assert!(
            error.contains("failed to reserve a complete directory"),
            "unexpected failure: {error}"
        ),
        other => panic!("the job did not fail: {other:?}"),
    }
    assert!(complete_dir.is_file());
    assert_eq!(history(&pipeline, JOB).await.status, "failed");
}

// `weaver_files_missing_total` counts files, not articles: two articles lost
// from one file are one missing file carrying both.
#[tokio::test]
async fn a_file_that_lost_articles_counts_once_as_missing() {
    let scenario = Scenario::new(vec![
        Post::plain("ember.bin", payload(6 * ARTICLE, 4)),
        Post::plain("ember.idx", payload(2 * ARTICLE, 5)),
    ])
    .lost(0, 1)
    .lost(0, 4);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Partial)
    );
    let snapshot = pipeline.metrics.job_lifecycle.snapshot();
    assert_eq!(snapshot.files_missing_total, 1);
    assert_eq!(snapshot.missing_segments_total, 2);
}

// An unprotected file delivered short keeps its failure: the job is partial,
// and its record carries the bytes it never received and the honest health
// left.
#[tokio::test]
async fn a_short_unprotected_file_keeps_an_honest_health() {
    let scenario = Scenario::new(vec![
        Post::plain("ember.bin", payload(10 * ARTICLE, 6)),
        Post::plain("ember.idx", payload(10 * ARTICLE, 7)),
    ])
    .lost(0, 9);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Partial)
    );
    let row = history(&pipeline, JOB).await;
    assert_eq!(row.failed_bytes, ARTICLE as u64);
    assert_eq!(row.health, 950);
}

// A dead repost beside the file it reposts: the delivered file is not
// refused over it, and the hole stays on the record, since nothing claims it.
#[tokio::test]
async fn a_dead_repost_beside_a_delivered_file_does_not_refuse_the_job() {
    let bytes = payload(4 * ARTICLE, 8);
    let scenario = Scenario::new(vec![
        Post::plain("silver.mkv", bytes.clone()),
        Post::plain("silver.mkv.1", bytes),
    ])
    .lost(1, 0)
    .lost(1, 1)
    .lost(1, 2)
    .lost(1, 3);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    assert!(
        matches!(
            job_status_for_assert(&pipeline, JOB),
            Some(JobStatus::Complete | JobStatus::Partial)
        ),
        "{:?}",
        job_status_for_assert(&pipeline, JOB)
    );
    let mut files = std::collections::BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    assert!(
        files.keys().any(|name| name.ends_with("silver.mkv")),
        "{:?}",
        files.keys()
    );
    let row = history(&pipeline, JOB).await;
    assert_eq!(row.failed_bytes, 4 * ARTICLE as u64);
    assert_eq!(row.health, 500);
}

// The job's snapshot says, file by file, what of each the download has
// still to bring: an article served leaves its file short by its size, and
// a file nothing of which came is still to come whole.
#[tokio::test]
async fn the_job_snapshot_says_what_of_each_file_is_still_to_come() {
    let scenario = Scenario::new(vec![
        Post::plain("alpha.bin", payload(4 * ARTICLE, 71)),
        Post::plain("beta.bin", payload(ARTICLE, 72)),
    ]);
    let spec = scenario.spec();
    let sizes = |file: usize| -> Vec<u64> {
        spec.files[file]
            .segments
            .iter()
            .map(|segment| u64::from(segment.bytes))
            .collect()
    };
    let (alpha, beta) = (sizes(0), sizes(1));
    assert!(alpha.len() > 1, "alpha spans several articles");
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;

    let snapshot = |pipeline: &Pipeline| {
        pipeline
            .list_jobs()
            .into_iter()
            .find(|job| job.job_id == JOB)
            .expect("the job is listed")
            .file_remaining_bytes
    };
    assert_eq!(
        snapshot(&pipeline),
        vec![alpha.iter().sum::<u64>(), beta.iter().sum::<u64>()]
    );

    serve_job(&mut pipeline, JOB, &scenario, (0, 0)).await;
    assert_eq!(
        snapshot(&pipeline),
        vec![alpha[1..].iter().sum::<u64>(), beta.iter().sum::<u64>()]
    );
}
