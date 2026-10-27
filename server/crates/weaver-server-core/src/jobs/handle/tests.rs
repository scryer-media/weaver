// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;
use std::time::Duration;

use crate::events::model::PipelineEvent;
use crate::jobs::ids::JobId;

use super::*;
use crate::jobs::model::{FieldUpdate, JobSpec, JobStatus, JobUpdate};
use crate::operations::metrics::PipelineMetrics;

#[test]
fn shared_pipeline_metrics_snapshot_reads_do_not_resample_speed() {
    let metrics = PipelineMetrics::new();
    let shared_state = SharedPipelineState::new(metrics.clone(), vec![]);
    let now = std::time::Instant::now();
    shared_state.refresh_metrics_snapshot(now);

    shared_state.record_wire_bytes(256 * 1024);
    shared_state.refresh_metrics_snapshot(now + Duration::from_secs(1));

    let initial = shared_state.metrics_snapshot();
    assert!(initial.current_download_speed > 0);

    for _ in 0..24 {
        let cached = shared_state.metrics_snapshot();
        assert_eq!(
            cached.current_download_speed,
            initial.current_download_speed
        );
    }

    shared_state.refresh_metrics_snapshot(now + Duration::from_secs(3));
    let refreshed = shared_state.metrics_snapshot();
    assert!(refreshed.current_download_speed < initial.current_download_speed);
}

#[test]
fn raw_metrics_snapshot_reads_do_not_cool_shared_speed_tracker() {
    let metrics = PipelineMetrics::new();
    let shared_state = SharedPipelineState::new(metrics.clone(), vec![]);
    let now = std::time::Instant::now();
    shared_state.refresh_metrics_snapshot(now);

    shared_state.record_wire_bytes(256 * 1024);
    shared_state.refresh_metrics_snapshot(now + Duration::from_secs(1));

    let initial = shared_state.metrics_snapshot();
    assert!(initial.current_download_speed > 0);

    for _ in 0..24 {
        let live = shared_state.raw_metrics_snapshot();
        assert_eq!(live.bytes_downloaded, 256 * 1024);
    }

    let cached = shared_state.metrics_snapshot();
    assert_eq!(
        cached.current_download_speed,
        initial.current_download_speed
    );

    shared_state.refresh_metrics_snapshot(now + Duration::from_secs(3));
    let refreshed = shared_state.metrics_snapshot();
    assert!(refreshed.current_download_speed < initial.current_download_speed);
}

// Exercise the real admission and pipeline boundaries, including persistence.
async fn test_scheduler() -> (SchedulerHandle, tokio::task::JoinHandle<()>) {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let task = tokio::spawn(async move {
        let _temp = temp;
        pipeline.run().await;
    });
    (handle, task)
}

async fn wait_status(handle: &SchedulerHandle, id: JobId, status: JobStatus) -> JobInfo {
    let mut changes = handle.subscribe_job_changes();
    loop {
        let info = handle.get_job(id).unwrap();
        if info.status == status {
            return info;
        }
        changes.changed().await.unwrap();
    }
}

fn make_spec(name: &str) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        files: vec![crate::FileSpec {
            filename: "payload.bin".into(),
            role: weaver_model::files::FileRole::Standalone,
            groups: vec!["alt.binaries.test".into()],
            posted_at_epoch: None,
            segments: vec![crate::SegmentSpec {
                ordinal: 0,
                article_number: 1,
                bytes: 10,
                message_ids: crate::jobs::MessageIds::new(vec!["abc@test".into()]),
            }],
        }],
        total_bytes: 10,
        category: None,
        metadata: vec![],
    }
}

fn sample_nzb_zstd() -> Vec<u8> {
    crate::ingest::compress_nzb_bytes(
        br#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="poster" date="1700000000" subject="sample">
            <groups><group>alt.binaries.test</group></groups>
            <segments>
              <segment bytes="10" number="1">abc@test</segment>
            </segments>
          </file>
        </nzb>"#,
    )
    .unwrap()
}

#[tokio::test]
async fn update_random_read_iops_reaches_scheduler() {
    let (handle, task) = test_scheduler().await;

    handle.update_random_read_iops(1_500.0).await.unwrap();

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn add_and_list_jobs() {
    let (handle, task) = test_scheduler().await;

    handle
        .add_job(
            JobId(1),
            make_spec("Job 1"),
            PathBuf::from("test.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();
    handle
        .add_job(
            JobId(2),
            make_spec("Job 2"),
            PathBuf::from("test2.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();

    let jobs = handle.list_jobs();
    assert_eq!(jobs.len(), 2);

    let info = handle.get_job(JobId(1)).unwrap();
    assert_eq!(info.name, "Job 1");
    assert_eq!(info.status, JobStatus::Queued);

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn add_job_with_options_can_start_paused_and_resume_to_queued() {
    let (handle, task) = test_scheduler().await;

    handle
        .add_job_with_options(
            JobId(1),
            make_spec("Paused First"),
            PathBuf::from("paused.nzb"),
            sample_nzb_zstd(),
            AddJobOptions {
                initially_paused: true,
                semantic_materialization_generation: None,
                semantic_promotion_generation: None,
                supplied_password_candidates: Vec::new(),
            },
        )
        .await
        .unwrap();

    let info = handle.get_job(JobId(1)).unwrap();
    assert_eq!(info.status, JobStatus::Paused);
    assert_eq!(
        info.download_state,
        crate::jobs::model::DownloadState::Queued
    );
    assert_eq!(info.run_state, crate::jobs::model::RunState::Paused);

    handle.resume_job(JobId(1)).await.unwrap();
    let info = wait_status(&handle, JobId(1), JobStatus::Queued).await;
    assert_eq!(info.status, JobStatus::Queued);
    assert_eq!(
        info.download_state,
        crate::jobs::model::DownloadState::Queued
    );
    assert_eq!(info.run_state, crate::jobs::model::RunState::Active);

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn pause_resume() {
    let (handle, task) = test_scheduler().await;

    handle
        .add_job(
            JobId(1),
            make_spec("Test Job"),
            PathBuf::from("test.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();

    handle.pause_job(JobId(1)).await.unwrap();
    let info = wait_status(&handle, JobId(1), JobStatus::Paused).await;
    assert_eq!(info.status, JobStatus::Paused);

    handle.resume_job(JobId(1)).await.unwrap();
    let info = wait_status(&handle, JobId(1), JobStatus::Queued).await;
    assert_eq!(info.status, JobStatus::Queued);

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn cancel_job() {
    let (handle, task) = test_scheduler().await;

    handle
        .add_job(
            JobId(1),
            make_spec("To Cancel"),
            PathBuf::from("test.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();
    assert_eq!(handle.list_jobs().len(), 1);

    let mut events = handle.subscribe_events();
    handle.cancel_job(JobId(1)).await.unwrap();
    // The job leaves the list with its row, once its history row commits.
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == JobId(1))
    {
    }
    assert_eq!(handle.list_jobs().len(), 0);

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn redownload_failed_job() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline(&temp).await;
    pipeline
        .add_job(
            JobId(1),
            make_spec("Failed Job"),
            PathBuf::from("failed.nzb"),
            sample_nzb_zstd(),
            AddJobOptions::default(),
        )
        .await
        .unwrap();
    let state = pipeline.jobs.get_mut(&JobId(1)).unwrap();
    state.status = JobStatus::Failed {
        error: "fixture failure".into(),
    };
    state.refresh_runtime_lanes_from_status();
    pipeline.report_all_for_test();
    let mut changes = pipeline.shared_state.queue.subscribe();
    pipeline.record_job_history(JobId(1), None);
    while !pipeline
        .shared_state
        .queue
        .history()
        .iter()
        .any(|job| job.job_id == JobId(1))
    {
        changes.changed().await.unwrap();
    }
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let task = tokio::spawn(async move {
        let _temp = temp;
        pipeline.run().await;
    });

    handle.redownload_job(JobId(1)).await.unwrap();
    let info = handle.get_job(JobId(1)).unwrap();
    assert_eq!(info.status, JobStatus::Queued);

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn update_job_applies_explicit_patch() {
    let (handle, task) = test_scheduler().await;

    handle
        .add_job(
            JobId(1),
            make_spec("Patch Me"),
            PathBuf::from("test.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();

    handle
        .update_job(
            JobId(1),
            JobUpdate {
                category: FieldUpdate::Set("movies".to_string()),
                metadata: FieldUpdate::Set(vec![("priority".to_string(), "high".to_string())]),
                metadata_edits: Vec::new(),
                password: FieldUpdate::Unchanged,
                name: None,
            },
        )
        .await
        .unwrap();

    let info = handle.get_job(JobId(1)).unwrap();
    assert_eq!(info.category.as_deref(), Some("movies"));
    assert_eq!(
        info.metadata,
        vec![("priority".to_string(), "high".to_string())]
    );

    handle
        .update_job(
            JobId(1),
            JobUpdate {
                category: FieldUpdate::Clear,
                metadata: FieldUpdate::Clear,
                metadata_edits: Vec::new(),
                password: FieldUpdate::Unchanged,
                name: None,
            },
        )
        .await
        .unwrap();

    let cleared = handle.get_job(JobId(1)).unwrap();
    assert!(cleared.category.is_none());
    assert!(cleared.metadata.is_empty());

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn job_not_found() {
    let (handle, task) = test_scheduler().await;

    let err = handle.get_job(JobId(999)).unwrap_err();
    assert!(matches!(err, SchedulerError::JobNotFound(_)));

    let err = handle.pause_job(JobId(999)).await.unwrap_err();
    assert!(matches!(err, SchedulerError::JobNotFound(_)));

    let err = handle.cancel_job(JobId(999)).await.unwrap_err();
    assert!(matches!(err, SchedulerError::JobNotFound(_)));

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn event_subscription() {
    let (handle, task) = test_scheduler().await;

    let mut rx = handle.subscribe_events();

    handle
        .add_job(
            JobId(1),
            make_spec("Evented Job"),
            PathBuf::from("test.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();

    let event = rx.recv().await.unwrap();
    match event {
        PipelineEvent::JobCreated { job_id, name, .. } => {
            assert_eq!(job_id, JobId(1));
            assert_eq!(name, "Evented Job");
        }
        other => panic!("expected JobCreated, got {other:?}"),
    }

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn duplicate_job_rejected() {
    let (handle, task) = test_scheduler().await;

    handle
        .add_job(
            JobId(1),
            make_spec("First"),
            PathBuf::from("test.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();
    let err = handle
        .add_job(
            JobId(1),
            make_spec("Duplicate"),
            PathBuf::from("test.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, SchedulerError::JobExists(_)));

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn password_passthrough() {
    let (handle, task) = test_scheduler().await;

    let mut spec = make_spec("Protected");
    spec.password = Some("secret123".to_string());

    handle
        .add_job(JobId(1), spec, PathBuf::from("test.nzb"), sample_nzb_zstd())
        .await
        .unwrap();

    let info = handle.get_job(JobId(1)).unwrap();
    assert_eq!(info.password, Some("secret123".to_string()));

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn reorder_jobs_batch_succeeds_when_all_ids_known() {
    let (handle, task) = test_scheduler().await;

    for id in [1_u64, 2, 3] {
        handle
            .add_job(
                JobId(id),
                make_spec(&format!("Job {id}")),
                PathBuf::from(format!("test{id}.nzb")),
                sample_nzb_zstd(),
            )
            .await
            .unwrap();
    }

    handle
        .reorder_jobs(vec![
            (JobId(1), QueueMoveTarget::Bottom),
            (JobId(2), QueueMoveTarget::Top),
        ])
        .await
        .unwrap();

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn reorder_jobs_batch_rejects_unknown_id_without_partial_application() {
    let (handle, task) = test_scheduler().await;

    handle
        .add_job(
            JobId(1),
            make_spec("Known"),
            PathBuf::from("test.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();

    let err = handle
        .reorder_jobs(vec![
            (JobId(1), QueueMoveTarget::Top),
            (JobId(999), QueueMoveTarget::Bottom),
        ])
        .await
        .unwrap_err();
    assert!(matches!(err, SchedulerError::JobNotFound(JobId(999))));

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[test]
fn job_download_rates_report_only_transferring_download_phases() {
    use crate::jobs::phase_progress::{JobPhase, JobPhaseProgress};

    fn phase(phase: JobPhase, rate_bps: Option<u64>) -> JobPhaseProgress {
        JobPhaseProgress {
            phase,
            completed_bytes: 0,
            total_bytes: 100,
            progress_percent: 0.0,
            rate_bps,
            estimated_remaining_ms: None,
            started_at_epoch_ms: 0.0,
            updated_at_epoch_ms: 0.0,
        }
    }

    fn info(job_id: u64, phase_progress: Vec<JobPhaseProgress>) -> JobInfo {
        JobInfo {
            job_id: JobId(job_id),
            job_hash: None,
            name: format!("job-{job_id}"),
            status: JobStatus::Downloading,
            download_state: crate::jobs::model::DownloadState::Downloading,
            finalizing_download: false,
            fetching_repair_data: false,
            post_state: crate::jobs::model::PostState::Idle,
            run_state: crate::jobs::model::RunState::Active,
            progress: 0.0,
            total_bytes: 100,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            phase_progress,
            failed_bytes: 0,
            health: 1000,
            terminal_discards: Vec::new(),
            total_files: 0,
            completed_files: 0,
            remaining_par_files: 0,
            file_remaining_bytes: Vec::new(),
            password: None,
            category: None,
            metadata: Vec::new(),
            output_dir: None,
            error: None,
            download_wait_reason: None,
            download_retry_at_epoch_ms: None,
            server_attribution: Vec::new(),
            created_at_epoch_ms: 0.0,
        }
    }

    let jobs = vec![
        info(1, vec![phase(JobPhase::Downloading, Some(61_000_000))]),
        // Still inside the rate warm-up: no figure yet, so no entry.
        info(2, vec![phase(JobPhase::Downloading, None)]),
        // A repair rate is not a download rate.
        info(3, vec![phase(JobPhase::Repairing, Some(5_000_000))]),
        info(4, Vec::new()),
        info(
            5,
            vec![
                phase(JobPhase::Extracting, Some(9)),
                phase(JobPhase::Downloading, Some(1_000)),
            ],
        ),
    ];

    assert_eq!(
        job_download_rates(&jobs),
        vec![(JobId(1), 61_000_000), (JobId(5), 1_000)]
    );
}
