// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;
use std::time::Duration;

use weaver_server_base::events::model::PipelineEvent;
use weaver_server_base::jobs::ids::JobId;

use super::*;
use crate::jobs::model::{FieldUpdate, JobSpec, JobStatus, JobUpdate};
use weaver_server_base::operations::metrics::PipelineMetrics;

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
    assert_eq!(
        handle
            .state
            .queue
            .read(|inner| inner.globals.random_read_iops),
        Some(1_500.0)
    );

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn update_storage_classification_reaches_scheduler() {
    use weaver_server_base::runtime::system_profile::{FilesystemType, StorageClass};
    let (handle, task) = test_scheduler().await;

    handle.update_storage_classification(StorageClass::Hdd, FilesystemType::Ext4);
    assert_eq!(
        handle
            .state
            .queue
            .read(|inner| inner.globals.storage_classification.clone()),
        Some((StorageClass::Hdd, FilesystemType::Ext4))
    );

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
            protocol: crate::jobs::Protocol::Nzb,
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
            file_parked_bytes: Vec::new(),
            remaining_bytes: 0,
            wanted_bytes: 0,
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

// A class that records what the queue asked of it: each call, and every
// job's action in it.
struct RecordingClass {
    protocol: crate::jobs::Protocol,
    asked: std::sync::Mutex<Vec<(JobId, String)>>,
    calls: std::sync::atomic::AtomicUsize,
}

impl RecordingClass {
    fn new(protocol: crate::jobs::Protocol) -> Self {
        Self {
            protocol,
            asked: std::sync::Mutex::default(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl crate::jobs::JobClass for RecordingClass {
    fn protocol(&self) -> crate::jobs::Protocol {
        self.protocol
    }

    async fn act(
        &self,
        actions: Vec<(JobId, QueueAction)>,
    ) -> std::collections::BTreeMap<JobId, Result<(), SchedulerError>> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut asked = self.asked.lock().unwrap();
        actions
            .into_iter()
            .map(|(job_id, action)| {
                asked.push((job_id, format!("{action:?}")));
                (job_id, Ok(()))
            })
            .collect()
    }
}

fn owned_job(job_id: JobId, created_at_epoch_ms: f64) -> JobInfo {
    JobInfo {
        protocol: crate::jobs::Protocol::Torrent,
        file_remaining_bytes: Vec::new(),
        file_parked_bytes: Vec::new(),
        remaining_bytes: 0,
        wanted_bytes: 0,
        job_id,
        job_hash: None,
        name: format!("job {}", job_id.0),
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        status: JobStatus::Queued,
        download_state: crate::jobs::model::DownloadState::Queued,
        finalizing_download: false,
        fetching_repair_data: false,
        post_state: crate::jobs::model::PostState::Idle,
        run_state: crate::jobs::model::RunState::Active,
        progress: 0.0,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files: 0,
        completed_files: 0,
        remaining_par_files: 0,
        password: None,
        category: None,
        metadata: Vec::new(),
        output_dir: None,
        server_attribution: Vec::new(),
        created_at_epoch_ms,
    }
}

// Each job id has one owner: an action on a job another owner published goes
// to that owner, and an action on any other job goes to the pipeline.
#[tokio::test]
async fn a_queue_action_goes_to_the_jobs_owner() {
    let (handle, task) = test_scheduler().await;
    handle
        .add_job(
            JobId(1),
            make_spec("Pipeline Job"),
            PathBuf::from("test.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();
    let owner = Arc::new(RecordingClass::new(crate::jobs::Protocol::Torrent));
    handle.state().classes().seat(owner.clone());
    handle.publish_owned_jobs(
        crate::jobs::Protocol::Torrent,
        vec![owned_job(JobId(2), 0.0)],
    );

    handle.pause_job(JobId(2)).await.unwrap();
    handle.resume_job(JobId(2)).await.unwrap();
    handle.cancel_job_keeping_files(JobId(2)).await.unwrap();
    handle
        .update_job(
            JobId(2),
            JobUpdate {
                category: FieldUpdate::Set("tv".into()),
                ..JobUpdate::default()
            },
        )
        .await
        .unwrap();
    handle
        .reorder_jobs(vec![
            (JobId(1), QueueMoveTarget::Top),
            (JobId(2), QueueMoveTarget::Bottom),
        ])
        .await
        .unwrap();
    handle.pause_job(JobId(1)).await.unwrap();
    assert_eq!(handle.get_job(JobId(1)).unwrap().status, JobStatus::Paused);

    let asked = owner.asked.lock().unwrap().clone();
    let ids: Vec<JobId> = asked.iter().map(|(job_id, _)| *job_id).collect();
    assert_eq!(ids, vec![JobId(2); 5]);
    let actions: Vec<&str> = asked.iter().map(|(_, action)| action.as_str()).collect();
    assert_eq!(actions[0], "Pause");
    assert_eq!(actions[1], "Resume");
    assert_eq!(actions[2], "Cancel(UserKeepingFiles)");
    assert!(actions[3].starts_with("Update("), "{}", actions[3]);
    assert_eq!(actions[4], "Move(Bottom)");

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

// A batch over two classes is one call to each, and what it answers is what
// the jobs one at a time answer: each job its own class's word.
#[tokio::test]
async fn a_batch_over_two_classes_is_one_call_each_and_routes_as_one_at_a_time() {
    let (handle, task) = test_scheduler().await;
    handle
        .add_job(
            JobId(1),
            make_spec("Pipeline Job"),
            PathBuf::from("test.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();
    let torrents = Arc::new(RecordingClass::new(crate::jobs::Protocol::Torrent));
    handle.state().classes().seat(torrents.clone());
    handle.publish_owned_jobs(
        crate::jobs::Protocol::Torrent,
        vec![owned_job(JobId(2), 0.0), owned_job(JobId(3), 1.0)],
    );
    // Job 4 is no one's: the pipeline answers for it.
    let ids = [JobId(1), JobId(2), JobId(3), JobId(4)];

    let merged = handle.set_jobs_paused(ids, true).await;
    assert_eq!(torrents.calls(), 1, "one call for the torrent class");
    let asked: Vec<JobId> = torrents
        .asked
        .lock()
        .unwrap()
        .iter()
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(asked, vec![JobId(2), JobId(3)]);
    assert_eq!(merged.keys().copied().collect::<Vec<_>>(), ids.to_vec());

    handle.set_jobs_paused(ids, false).await;
    let calls = torrents.calls();
    let mut one_at_a_time = std::collections::BTreeMap::new();
    for id in ids {
        one_at_a_time.insert(id, format!("{:?}", handle.pause_job(id).await));
    }
    assert_eq!(
        torrents.calls(),
        calls + 2,
        "one call per torrent job alone"
    );
    let merged: std::collections::BTreeMap<_, _> = merged
        .into_iter()
        .map(|(id, outcome)| (id, format!("{outcome:?}")))
        .collect();
    assert_eq!(merged, one_at_a_time);
    assert!(one_at_a_time[&JobId(4)].contains("JobNotFound"));

    handle.shutdown().await.unwrap();
    task.await.unwrap();
}

// Each action over jobs of two classes and of no one is one call to the
// torrent class with only its jobs, in the order named, and one revision of
// the queue for the pipeline's part; each job is answered as written out
// here, apart from either path.
#[tokio::test]
async fn every_batch_action_over_two_classes_is_one_call_each_and_one_queue_revision() {
    use crate::jobs::Protocol;
    let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    let (events, _) = tokio::sync::broadcast::channel(16);
    let handle = SchedulerHandle::new(events, state);
    let mut pipeline_job = owned_job(JobId(1), 0.0);
    pipeline_job.protocol = Protocol::Nzb;
    handle.state().publish_jobs(vec![pipeline_job]);
    let torrents = Arc::new(RecordingClass::new(Protocol::Torrent));
    handle.state().classes().seat(torrents.clone());
    handle.publish_owned_jobs(
        Protocol::Torrent,
        vec![owned_job(JobId(2), 1.0), owned_job(JobId(3), 2.0)],
    );
    // Job 4 is no one's: the pipeline answers for it.
    let ids = [JobId(1), JobId(2), JobId(3), JobId(4)];
    let expected = |pipeline: &str| {
        std::collections::BTreeMap::from([
            (JobId(1), pipeline.to_owned()),
            (JobId(2), "Ok(())".to_owned()),
            (JobId(3), "Ok(())".to_owned()),
            (JobId(4), "Err(JobNotFound(JobId(4)))".to_owned()),
        ])
    };
    let shown = |outcome: std::collections::BTreeMap<JobId, Result<(), SchedulerError>>| {
        outcome
            .into_iter()
            .map(|(job_id, result)| (job_id, format!("{result:?}")))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    // What the torrent class was asked by the last call, and the queue's
    // revisions since `before`.
    let last_call = |calls: usize| {
        assert_eq!(
            torrents.calls(),
            calls + 1,
            "one call for the torrent class"
        );
        let asked = torrents.asked.lock().unwrap();
        asked[asked.len() - 2..].to_vec()
    };
    let pair = |action: &str| vec![(JobId(2), action.to_owned()), (JobId(3), action.to_owned())];

    let (calls, before) = (torrents.calls(), handle.job_revision());
    assert_eq!(
        shown(handle.set_jobs_paused(ids, true).await),
        expected("Ok(())")
    );
    assert_eq!(last_call(calls), pair("Pause"));
    assert_eq!(handle.job_revision(), before + 1);

    let (calls, before) = (torrents.calls(), handle.job_revision());
    assert_eq!(
        shown(handle.set_jobs_paused(ids, false).await),
        expected("Ok(())")
    );
    assert_eq!(last_call(calls), pair("Resume"));
    assert_eq!(handle.job_revision(), before + 1);

    let (calls, before) = (torrents.calls(), handle.job_revision());
    let update = JobUpdate {
        category: FieldUpdate::Set("tv".into()),
        ..JobUpdate::default()
    };
    let asked_update = format!("Update({update:?})");
    assert_eq!(
        shown(
            handle
                .update_jobs(ids.map(|job_id| (job_id, update.clone())))
                .await
        ),
        expected("Ok(())")
    );
    assert_eq!(last_call(calls), pair(&asked_update));
    assert_eq!(handle.job_revision(), before + 1);

    // The moves name no job of no one's: one unknown to the pipeline ends
    // them before any class is asked.
    let (calls, before) = (torrents.calls(), handle.job_revision());
    handle
        .reorder_jobs(
            [JobId(1), JobId(2), JobId(3)]
                .map(|job_id| (job_id, QueueMoveTarget::Top))
                .to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(last_call(calls), pair("Move(Top)"));
    assert_eq!(handle.job_revision(), before + 1);

    let (calls, before) = (torrents.calls(), handle.job_revision());
    let cancelled = handle
        .cancel_jobs(BTreeSet::from(ids), CancellationOrigin::User)
        .await
        .unwrap();
    assert_eq!(shown(cancelled), expected("Ok(())"));
    assert_eq!(last_call(calls), pair("Cancel(User)"));
    assert_eq!(handle.job_revision(), before + 1);
}

// A hold over one class stops that class alone; a hold over all stops every
// class. The targets are those product code asks about: the pipeline's jobs
// are the queue's own items, a held class's are its own, and a class with no
// hold of its own has no hold to name.
#[test]
fn a_hold_over_one_class_spares_the_others_and_one_over_all_stops_them_all() {
    use crate::jobs::{HeldClass, Protocol};
    let pipeline = HoldOwner::All;
    let torrents = HoldOwner::Class(crate::torrents::CLASS);
    assert_eq!(HeldClass::of(Protocol::Nzb), None);

    let set = hold_set(&[Hold::manual(torrents)]);
    for complete in [false, true] {
        assert!(set.stops(torrents, complete), "{complete}");
        assert!(!set.stops(pipeline, complete), "{complete}");
    }

    let set = hold_set(&[Hold::manual(HoldOwner::All)]);
    for owner in [pipeline, torrents] {
        assert!(set.stops(owner, false), "{owner:?}");
    }
}

// The kept holds are the queue's and each class's that holds apart, under
// the setting each is saved as.
#[test]
fn the_kept_holds_are_saved_under_each_class_s_setting() {
    let kept: Vec<_> = HoldOwner::kept().collect();
    assert_eq!(
        kept,
        vec![
            (HoldOwner::All, "global_paused"),
            (HoldOwner::Class(crate::torrents::CLASS), "torrent_paused"),
        ]
    );
}

// The queue lists every owner's jobs by when they were added, each owner's
// own order kept, then the finished jobs.
#[test]
fn the_queue_interleaves_owners_by_when_their_jobs_were_added() {
    let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    let live = vec![
        owned_job(JobId(1), 100.0),
        owned_job(JobId(2), 300.0),
        // Moved up by hand: the pipeline's order stands among its jobs.
        owned_job(JobId(3), 50.0),
    ];
    let finished = vec![owned_job(JobId(9), 10.0)];
    state.publish_jobs(live);
    state.load_history(finished);
    state.publish_class_jobs(
        crate::jobs::Protocol::Torrent,
        vec![owned_job(JobId(4), 200.0), owned_job(JobId(5), 400.0)],
    );
    let order: Vec<u64> = state.list_jobs().iter().map(|job| job.job_id.0).collect();
    assert_eq!(order, vec![1, 4, 2, 3, 5, 9]);
}

fn hold_set(holds: &[Hold]) -> HoldSet {
    let mut set = HoldSet::default();
    for hold in holds {
        set.hold(*hold);
    }
    set
}

const fn hold(origin: HoldOrigin, scope: HoldScope, owner: HoldOwner) -> Hold {
    Hold {
        origin,
        scope,
        owner,
    }
}

#[test]
fn a_hold_stops_what_it_covers_and_names_who_stops_it() {
    use HoldOrigin::{Manual, Scheduled, SpeedTest};
    use HoldOwner::All;
    const TORRENTS: HoldOwner = HoldOwner::Class(crate::torrents::CLASS);
    use HoldScope::{Downloads, Everything};
    // Usenet downloads, torrent downloads, seeding torrents.
    let items = [(All, false), (TORRENTS, false), (TORRENTS, true)];
    for (holds, expected) in [
        (vec![], [None, None, None]),
        (vec![Hold::manual(All)], [Some(Manual), Some(Manual), None]),
        (
            vec![hold(Scheduled, Everything, All)],
            [Some(Scheduled), Some(Scheduled), Some(Scheduled)],
        ),
        (
            vec![Hold::manual(TORRENTS)],
            [None, Some(Manual), Some(Manual)],
        ),
        (
            vec![hold(Scheduled, Everything, TORRENTS)],
            [None, Some(Scheduled), Some(Scheduled)],
        ),
        (
            vec![hold(SpeedTest, Everything, All)],
            [Some(SpeedTest), Some(SpeedTest), Some(SpeedTest)],
        ),
        (
            vec![hold(SpeedTest, Everything, All), Hold::manual(All)],
            [Some(Manual), Some(Manual), Some(SpeedTest)],
        ),
        (
            vec![
                hold(SpeedTest, Everything, All),
                hold(Scheduled, Downloads, All),
            ],
            [Some(Scheduled), Some(Scheduled), Some(SpeedTest)],
        ),
        (
            vec![Hold::manual(TORRENTS), hold(Scheduled, Downloads, All)],
            [Some(Scheduled), Some(Manual), Some(Manual)],
        ),
    ] {
        let set = hold_set(&holds);
        let reasons = items.map(|(owner, complete)| set.reason(owner, complete));
        assert_eq!(reasons, expected, "{holds:?}");
        for ((owner, complete), reason) in items.into_iter().zip(expected) {
            assert_eq!(set.stops(owner, complete), reason.is_some());
        }
    }
}

#[test]
fn a_hold_origin_releases_what_it_shares_and_nothing_else() {
    use HoldOrigin::{Manual, Scheduled, SpeedTest};
    use HoldOwner::All;
    const TORRENTS: HoldOwner = HoldOwner::Class(crate::torrents::CLASS);
    use HoldScope::{Downloads, Everything};
    let speed_test = hold(SpeedTest, Everything, All);

    // The operator and the schedule share one pause; each replaces and lifts
    // the other's.
    let mut set = hold_set(&[hold(Scheduled, Everything, All), Hold::manual(All)]);
    assert_eq!(set, hold_set(&[Hold::manual(All)]));
    set.hold(hold(Scheduled, Downloads, All));
    set.release(Manual, All);
    assert_eq!(set, HoldSet::default());

    // Neither touches a speed test's hold, and it touches neither of theirs.
    let mut set = hold_set(&[speed_test, Hold::manual(All)]);
    set.release(Manual, All);
    assert_eq!(set, hold_set(&[speed_test]));
    set.hold(hold(Scheduled, Downloads, All));
    set.release(SpeedTest, All);
    assert_eq!(set, hold_set(&[hold(Scheduled, Downloads, All)]));

    // A hold over the torrents stands apart from the queue's.
    let mut set = hold_set(&[Hold::manual(TORRENTS), Hold::manual(All)]);
    set.release(Manual, All);
    assert_eq!(set, hold_set(&[Hold::manual(TORRENTS)]));
    assert!(set.held_by(Manual, TORRENTS));
    assert!(!set.held_by(Manual, All));
    set.release(Scheduled, TORRENTS);
    assert_eq!(set, HoldSet::default());
}

#[test]
fn the_queue_is_paused_only_by_a_hold_over_it() {
    let metrics = PipelineMetrics::new();
    let state = SharedPipelineState::new(metrics, vec![]);
    let mut holds = state.subscribe_holds();
    state.hold(Hold::manual(HoldOwner::Class(crate::torrents::CLASS)));
    assert!(holds.has_changed().unwrap());
    holds.borrow_and_update();
    assert!(!state.is_paused());
    state.hold(Hold::manual(HoldOwner::Class(crate::torrents::CLASS)));
    assert!(
        !holds.has_changed().unwrap(),
        "an unchanged set is not sent"
    );
    state.hold(Hold::manual(HoldOwner::All));
    assert!(state.is_paused());
    state.release(HoldOrigin::Manual, HoldOwner::All);
    assert!(!state.is_paused());
}

// The archive password, and a password kept among the job's attributes,
// never reach a job's debug text.
#[test]
fn a_job_s_debug_text_never_shows_its_password() {
    const SECRET: &str = "hunter-secret-7731";
    let info = JobInfo {
        protocol: crate::jobs::Protocol::Nzb,
        job_id: JobId(9),
        job_hash: None,
        name: "job-9".into(),
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
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files: 0,
        completed_files: 0,
        remaining_par_files: 0,
        file_remaining_bytes: Vec::new(),
        file_parked_bytes: Vec::new(),
        remaining_bytes: 0,
        wanted_bytes: 0,
        password: Some(SECRET.into()),
        category: None,
        metadata: vec![
            (
                crate::history::attributes::VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY.into(),
                SECRET.into(),
            ),
            ("source".into(), "rss".into()),
        ],
        output_dir: None,
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        server_attribution: Vec::new(),
        created_at_epoch_ms: 0.0,
    };

    for text in [format!("{info:?}"), format!("{info:#?}")] {
        assert!(!text.contains(SECRET), "{text}");
        assert!(text.contains("[REDACTED]"));
        assert!(text.contains("rss"));
    }
}

// Each of the other owner's jobs is found at its place as they are added
// out of order, replaced and removed one at a time, each moving only the
// positions after it: every position is the one a fresh walk gives.
#[test]
fn the_owned_jobs_keep_their_positions_through_each_add_and_removal() {
    let mut rows = crate::jobs::OwnedRows::default();
    let held = |rows: &crate::jobs::OwnedRows, gone: &[JobId]| {
        let jobs = rows.rows();
        assert!(jobs.windows(2).all(|pair| {
            (pair[0].created_at_epoch_ms, pair[0].job_id)
                <= (pair[1].created_at_epoch_ms, pair[1].job_id)
        }));
        for (at, job) in jobs.iter().enumerate() {
            assert_eq!(rows.position(job.job_id), Some(at));
            assert_eq!(rows.get(job.job_id), Some(job));
        }
        for job_id in gone {
            assert_eq!(rows.position(*job_id), None);
        }
    };
    for id in 0..200_u64 {
        // Three at each instant, added out of the order they sit in.
        let at = ((id * 37) % 67 / 3) as f64;
        rows.insert(owned_job(JobId(id), at));
        held(&rows, &[]);
    }
    assert_eq!(rows.rows().len(), 200);
    let mut job = owned_job(JobId(5), rows.get(JobId(5)).unwrap().created_at_epoch_ms);
    job.name = "renamed".to_owned();
    let at = rows.position(JobId(5)).unwrap();
    rows.set(at, job);
    assert_eq!(rows.get(JobId(5)).unwrap().name, "renamed");
    let mut gone = Vec::new();
    for id in (0..200_u64).step_by(7).chain([199, 0]) {
        let removed = rows.remove(JobId(id));
        assert_eq!(removed.is_some(), !gone.contains(&JobId(id)));
        gone.push(JobId(id));
        held(&rows, &gone);
    }
    assert_eq!(rows.rows().len(), 200 - 200_usize.div_ceil(7) - 1);
    let replaced: Vec<JobInfo> = (300..303).map(|id| owned_job(JobId(id), 1.0)).collect();
    rows.replace(replaced);
    held(&rows, &(0..200).map(JobId).collect::<Vec<_>>());
}

// Publishing the other owner's jobs as they already stand changes nothing:
// no reader is woken and no event goes out. A change to one wakes them once.
#[tokio::test]
async fn publishing_the_owned_jobs_as_they_stand_wakes_no_reader() {
    let (handle, _task) = test_scheduler().await;
    let mut events = handle.subscribe_events();
    let mut changes = handle.subscribe_job_changes();
    handle.publish_owned_jobs(
        crate::jobs::Protocol::Torrent,
        vec![owned_job(JobId(2), 0.0), owned_job(JobId(3), 1.0)],
    );
    assert!(changes.has_changed().unwrap());
    changes.borrow_and_update();
    while events.try_recv().is_ok() {}

    handle.publish_owned_jobs(
        crate::jobs::Protocol::Torrent,
        vec![owned_job(JobId(2), 0.0), owned_job(JobId(3), 1.0)],
    );
    handle.publish_owned_job(crate::jobs::Protocol::Torrent, owned_job(JobId(3), 1.0));
    assert!(
        !changes.has_changed().unwrap(),
        "an idle publish woke a reader"
    );
    assert!(events.try_recv().is_err(), "an idle publish sent an event");

    let mut paused = owned_job(JobId(3), 1.0);
    paused.status = JobStatus::Paused;
    handle.publish_owned_job(crate::jobs::Protocol::Torrent, paused);
    assert!(changes.has_changed().unwrap());
    assert!(matches!(
        events.try_recv(),
        Ok(PipelineEvent::JobPaused { job_id: JobId(3) })
    ));
    assert_eq!(
        handle
            .state()
            .read_queue(|inner| inner.owned_job(JobId(3)).map(|job| job.status.clone())),
        Some(JobStatus::Paused)
    );
}

// The queue holds no way back to itself: once its state and handles are
// gone, with the pipeline seated and its admission resources set, the
// queue is dropped.
#[test]
fn the_queue_is_dropped_with_the_last_handle_on_it() {
    let temp = tempfile::tempdir().unwrap();
    let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    let _ = state
        .queue
        .resources
        .set(crate::pipeline::test_support::admission_resources(
            crate::Database::open_in_memory().unwrap(),
            temp.path(),
        ));
    assert!(state.classes().get(crate::jobs::Protocol::Nzb).is_some());
    let (events, _) = tokio::sync::broadcast::channel(4);
    let handle = SchedulerHandle::new(events, state.clone());
    let queue = Arc::downgrade(&state.queue);
    drop(handle);
    drop(state);
    assert!(queue.upgrade().is_none());
}
