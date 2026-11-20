// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::broadcast;
use weaver_nntp::client::{BlockingBodyLaneAcquireError, NntpClient, NntpClientConfig};
use weaver_server_composition::JobId;
use weaver_server_composition::events::model::PipelineEvent;
use weaver_server_composition::proxies::Bandwidth;
use weaver_server_composition::{
    DownloadBlockKind, DownloadBlockState, DownloadPressureReason, DownloadPressureState,
    EgressQuotaBlock, JobPhase,
};
use weaver_server_composition::{
    DownloadState, JobSpec, JobStatus, NzbFileId, PostState, RestoreJobRequest, RunState, SegmentId,
};
use weaver_server_pipeline::DownloadQueue;
use weaver_server_pipeline::bandwidth;
use weaver_server_pipeline::jobs::{
    AddJobOptions,
    working_dir::{mark_weaver_owned_output_dir, stamp_working_dir},
};
use weaver_server_pipeline::jobs::{Hold, HoldOrigin, HoldOwner};
use weaver_server_pipeline::pipeline::tests::bandwidth_fixture::BandwidthFixture;
use weaver_server_pipeline::pipeline::tests::dispatch_fixture::dispatch_timing::RateCounters;
use weaver_server_pipeline::pipeline::tests::propagation_fixture::{
    PropagationFixture, now_epoch_secs,
};
use weaver_server_pipeline::pipeline::tests::segmented_job_spec;
use weaver_server_pipeline::pipeline::tests::{
    TestHarness,
    dispatch_fixture::{DispatchFixture, propagation::PROPAGATION_WAIT_REASON},
    history_row_with_output_dir, sample_nzb_zstd, standalone_job_spec,
};

#[tokio::test]
async fn download_block_refresh_preserves_scheduled_speed_limit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::new(&temp_dir, 5632 * 1024, 1).await;

    pipeline.publish_scheduled_limit(Bandwidth {
        download_bps: Some(131_072),
        upload_bps: None,
        split: None,
    });
    assert_eq!(
        pipeline.state().download_block().scheduled_speed_limit,
        Some(131_072)
    );

    pipeline.record_bandwidth(1_024);
    assert_eq!(
        pipeline.state().download_block().scheduled_speed_limit,
        Some(131_072)
    );

    pipeline.sample_bandwidth();
    assert_eq!(
        pipeline.state().download_block().scheduled_speed_limit,
        Some(131_072)
    );
}

fn global_speed(bytes_per_sec: Option<u64>) -> bandwidth::ScheduleAction {
    bandwidth::ScheduleAction::SpeedLimit {
        limits: vec![bandwidth::SpeedLimitChange {
            target: bandwidth::SpeedTarget::Global,
            bytes_per_sec,
            upload_bytes_per_sec: None,
        }],
    }
}

fn download_bandwidth(bytes_per_sec: u64) -> Bandwidth {
    Bandwidth {
        download_bps: Some(bytes_per_sec),
        upload_bps: None,
        split: None,
    }
}

#[tokio::test]
async fn a_scheduled_speed_limit_holds_over_pauses_and_reloaded_limits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::new(&temp_dir, 16 * 1024 * 1024, 0).await;

    pipeline
        .reported_handle()
        .set_bandwidth(download_bandwidth(512 * 1024))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(
        pipeline.state().download_block().speed_limit_in_force,
        Some(512 * 1024)
    );
    assert!(!pipeline.state().download_block().speed_limit_by_schedule);

    pipeline
        .reported_handle()
        .apply_schedule_action(global_speed(Some(128 * 1024)))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.rate(), Some(128 * 1024));

    for action in [
        bandwidth::ScheduleAction::Pause,
        bandwidth::ScheduleAction::Resume,
    ] {
        pipeline
            .reported_handle()
            .apply_schedule_action(action.clone())
            .await
            .unwrap();
        pipeline.apply_queue_intents().await;
        assert_eq!(
            pipeline.state().is_paused(),
            action == bandwidth::ScheduleAction::Pause
        );
        assert_eq!(pipeline.rate(), Some(128 * 1024));
        assert_eq!(
            pipeline.scheduled_bandwidth(),
            Some(download_bandwidth(128 * 1024))
        );
        assert_eq!(
            pipeline.state().download_block().scheduled_speed_limit,
            Some(128 * 1024)
        );
        assert_eq!(
            pipeline.state().download_block().speed_limit_in_force,
            Some(128 * 1024)
        );
        assert!(pipeline.state().download_block().speed_limit_by_schedule);
    }

    pipeline
        .reported_handle()
        .restore_bandwidth(download_bandwidth(768 * 1024))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    // A reload records the saved limit but leaves the scheduled one in force.
    assert_eq!(
        pipeline.configured_bandwidth(),
        download_bandwidth(768 * 1024)
    );
    assert_eq!(pipeline.rate(), Some(128 * 1024));

    pipeline
        .reported_handle()
        .apply_schedule_action(bandwidth::ScheduleAction::Pause)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    // A schedule without a limit leaves the saved one in force.
    pipeline
        .reported_handle()
        .apply_schedule_action(global_speed(None))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.rate(), Some(768 * 1024));
    assert!(pipeline.state().is_paused());
    // The published limit in force is the saved one, and not the schedule's.
    assert_eq!(
        pipeline.state().download_block().speed_limit_in_force,
        Some(768 * 1024)
    );
    assert!(!pipeline.state().download_block().speed_limit_by_schedule);

    // A schedule's zero lets nothing through.
    pipeline
        .reported_handle()
        .apply_schedule_action(global_speed(Some(0)))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.rate(), Some(0));
    assert_eq!(
        pipeline.state().download_block().speed_limit_in_force,
        Some(0)
    );
    assert!(pipeline.state().download_block().speed_limit_by_schedule);
}

#[tokio::test]
async fn speed_test_hold_preserves_later_schedule_pause() {
    use weaver_server_pipeline::jobs::{Hold, HoldOrigin, HoldOwner, HoldScope};
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::new(&temp_dir, 16 * 1024 * 1024, 0).await;
    let speed_test = Hold {
        origin: HoldOrigin::SpeedTest,
        scope: HoldScope::Everything,
        owner: HoldOwner::All,
    };
    pipeline.reported_handle().pause(speed_test).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.state().is_paused());
    assert_eq!(pipeline.global_pause(), Some(HoldOrigin::SpeedTest));
    assert_eq!(
        pipeline.persisted_pause(),
        None,
        "the speed test's hold is not persisted"
    );
    // A manual resume leaves the running test's hold in place.
    pipeline
        .reported_handle()
        .resume(HoldOrigin::Manual, HoldOwner::All)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.global_pause(), Some(HoldOrigin::SpeedTest));
    pipeline
        .reported_handle()
        .apply_schedule_action(bandwidth::ScheduleAction::Pause)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    pipeline
        .reported_handle()
        .resume(HoldOrigin::SpeedTest, HoldOwner::All)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.state().is_paused());
    assert_eq!(pipeline.global_pause(), Some(HoldOrigin::Scheduled));
    pipeline
        .reported_handle()
        .apply_schedule_action(bandwidth::ScheduleAction::Resume)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.state().is_paused());
}

#[tokio::test]
async fn an_operator_speed_limit_replaces_the_scheduled_one_until_the_next_rule_fires() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::new(&temp_dir, 16 * 1024 * 1024, 0).await;
    let handle = pipeline.handle();
    handle
        .apply_schedule_action(global_speed(Some(128 * 1024)))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.rate(), Some(128 * 1024));

    // The operator sets a limit: it is in force at once.
    pipeline
        .reported_handle()
        .set_bandwidth(download_bandwidth(640 * 1024))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.scheduled_bandwidth(), None);
    assert_eq!(
        pipeline.configured_bandwidth(),
        download_bandwidth(640 * 1024)
    );
    assert_eq!(pipeline.rate(), Some(640 * 1024));
    assert_eq!(
        pipeline.state().download_block().scheduled_speed_limit,
        None
    );

    // The next scheduled speed rule takes over again.
    handle
        .apply_schedule_action(global_speed(Some(256 * 1024)))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(
        pipeline.scheduled_bandwidth(),
        Some(download_bandwidth(256 * 1024))
    );
    assert_eq!(pipeline.rate(), Some(256 * 1024));
    assert_eq!(
        pipeline.configured_bandwidth(),
        download_bandwidth(640 * 1024)
    );
}

#[tokio::test]
async fn the_deferral_boundary_is_posted_at_plus_the_delay() {
    // Pin both sides of the boundary with enough margin that a wall-clock
    // second ticking during fixture setup cannot change the expected side.
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = PropagationFixture::new(&temp_dir, Duration::from_secs(3600)).await;
    let now = now_epoch_secs();

    let young = JobId(20201);
    pipeline
        .add_post(
            young,
            "Silver Horizon Young",
            &[("queued.bin", Some(now - 3540))],
        )
        .await;
    assert!(
        pipeline.propagation_hold_until(young).is_some(),
        "a post inside the delay is still deferred"
    );

    let old = JobId(20202);
    pipeline
        .add_post(
            old,
            "Silver Horizon Old",
            &[("queued.bin", Some(now - 3660))],
        )
        .await;
    assert!(
        pipeline.propagation_hold_until(old).is_none(),
        "a post older than the delay starts immediately"
    );
    assert!(
        !pipeline.has_cached_hold(old),
        "and an eligible job leaves no deferral state behind"
    );
}

#[tokio::test]
async fn a_job_with_no_parseable_dates_is_never_deferred() {
    // No date is not the same question as "posted just now". An NZB that
    // carries no usable dates has no anchor to defer against, so it starts
    // immediately — the same stance retention takes for the same reason.
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = PropagationFixture::new(&temp_dir, Duration::from_secs(3600)).await;
    let job_id = JobId(20204);
    pipeline
        .add_post(job_id, "Silver Horizon Undated", &[("queued.bin", None)])
        .await;

    assert!(pipeline.propagation_hold_until(job_id).is_none());
    assert!(pipeline.next_propagation_delay().is_none());
}

#[tokio::test]
async fn the_newest_date_in_the_nzb_is_the_anchor() {
    // Files with no date contribute nothing, and the anchor is the newest date
    // the NZB does carry — that is the article most likely to still be in
    // flight, and deferring to it is what makes the wait cover the whole post.
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = PropagationFixture::new(&temp_dir, Duration::from_secs(3600)).await;
    let now = now_epoch_secs();
    let job_id = JobId(20205);
    pipeline
        .add_post(
            job_id,
            "Silver Horizon Mixed Dates",
            &[("old.bin", Some(now - 86_400)), ("new.bin", None)],
        )
        .await;
    assert!(
        pipeline.propagation_hold_until(job_id).is_none(),
        "a day-old anchor beside an undated file is eligible"
    );

    let fresh = JobId(20206);
    pipeline
        .add_post(
            fresh,
            "Silver Horizon Fresh Tail",
            &[("old.bin", Some(now - 86_400)), ("new.bin", Some(now))],
        )
        .await;
    assert!(
        pipeline.propagation_hold_until(fresh).is_some(),
        "one fresh file is enough to hold the job — its articles are the ones \
         still propagating"
    );
}

#[tokio::test]
async fn a_zero_delay_disables_deferral_entirely() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = PropagationFixture::new(&temp_dir, Duration::ZERO).await;
    let job_id = JobId(20207);
    pipeline
        .add_post(
            job_id,
            "Silver Horizon Disabled",
            &[("queued.bin", Some(now_epoch_secs()))],
        )
        .await;

    assert!(
        pipeline.propagation_hold_until(job_id).is_none(),
        "WEAVER_PROPAGATION_DELAY_SECS=0 is the off switch"
    );
    assert_eq!(pipeline.cached_hold_count(), 0);
}

fn queued_articles(pipeline: &DispatchFixture, id: JobId) -> usize {
    pipeline
        .job(id)
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}

fn posted_job_spec(name: &str, posted_at: Option<u64>) -> JobSpec {
    let mut spec = standalone_job_spec(name, &[("queued.bin".to_string(), 512)]);
    for file in &mut spec.files {
        file.posted_at_epoch = posted_at;
    }
    spec
}

#[tokio::test]
async fn add_job_with_options_initially_paused_publishes_queued_lane_from_real_pipeline() {
    let harness = TestHarness::new().await;
    let job_id = JobId(30037);

    harness
        .handle
        .add_job_with_options(
            job_id,
            standalone_job_spec("Paused From Start", &[("episode.mkv".to_string(), 123)]),
            PathBuf::from(format!("job-{}.nzb", job_id.0)),
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

    let info = harness.handle.get_job(job_id).unwrap();
    assert_eq!(info.status, JobStatus::Paused);
    assert_eq!(info.download_state, DownloadState::Queued);
    assert_eq!(info.run_state, RunState::Paused);

    let mut revisions = harness.handle.subscribe_job_changes();
    harness.handle.resume_job(job_id).await.unwrap();
    loop {
        revisions.mark_unchanged();
        if harness
            .handle
            .get_job(job_id)
            .is_ok_and(|job| job.run_state == RunState::Active)
        {
            break;
        }
        revisions.changed().await.unwrap();
    }
    let info = harness.handle.get_job(job_id).unwrap();
    assert_eq!(info.status, JobStatus::Queued);
    assert_eq!(info.download_state, DownloadState::Queued);
    assert_eq!(info.run_state, RunState::Active);

    harness.shutdown().await;
}

#[tokio::test]
async fn redownload_job_rebuilds_failed_history_as_queued_download() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = DispatchFixture::with_roots(&temp_dir).await;
    let job_id = JobId(30038);
    let working_dir = intermediate_dir.join("failed-redownload-job");
    let staging_dir = pipeline.staging_dir(job_id);

    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    // Made and marked as a job's staging directory is.
    DispatchFixture::make_job_staging_dir(&staging_dir, job_id).unwrap();
    tokio::fs::write(working_dir.join("partial.mkv"), b"partial")
        .await
        .unwrap();
    tokio::fs::write(staging_dir.join("partial.srt"), b"partial")
        .await
        .unwrap();
    let mut row =
        history_row_with_output_dir(job_id, "Failed History Job", "failed", working_dir.clone());
    stamp_working_dir(&intermediate_dir, &working_dir, job_id)
        .await
        .unwrap();
    row.category = Some("tv".to_string());
    row.metadata = Some(serde_json::to_string(&vec![("source", "history")]).unwrap());
    pipeline.insert_history_with_nzb(&row, &sample_nzb_zstd());

    pipeline.redownload_job(job_id).await.unwrap();

    let state = pipeline.job(job_id);
    assert_eq!(state.status, JobStatus::Queued);
    assert_eq!(state.articles.queued_download_len(), 1);
    assert_eq!(state.downloaded_bytes, 0);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert!(!working_dir.exists());
    assert!(!staging_dir.exists());
    assert!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
            .all(|job| job.job_id != job_id)
    );
    assert!(!pipeline.in_history(job_id));
}

#[tokio::test]
async fn redownload_job_rebuilds_complete_history_as_queued_download() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = DispatchFixture::with_roots(&temp_dir).await;
    let job_id = JobId(30040);
    let working_dir = complete_dir.join("complete-redownload-job");
    let staging_dir = pipeline.staging_dir(job_id);

    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    // Made and marked as a job's staging directory is.
    DispatchFixture::make_job_staging_dir(&staging_dir, job_id).unwrap();
    tokio::fs::create_dir_all(working_dir.join("subs"))
        .await
        .unwrap();
    tokio::fs::write(working_dir.join("episode.mkv"), b"complete")
        .await
        .unwrap();
    tokio::fs::write(working_dir.join("subs").join("episode.srt"), b"complete")
        .await
        .unwrap();
    tokio::fs::write(staging_dir.join("partial.srt"), b"partial")
        .await
        .unwrap();
    let mut row = history_row_with_output_dir(
        job_id,
        "Complete History Job",
        "complete",
        working_dir.clone(),
    );
    mark_weaver_owned_output_dir(&working_dir, job_id).unwrap();
    row.category = Some("tv".to_string());
    row.metadata = Some(serde_json::to_string(&vec![("source", "history")]).unwrap());
    pipeline.insert_history_with_nzb(&row, &sample_nzb_zstd());

    pipeline.redownload_job(job_id).await.unwrap();

    let state = pipeline.job(job_id);
    assert_eq!(state.status, JobStatus::Queued);
    assert_eq!(state.articles.queued_download_len(), 1);
    assert_eq!(state.downloaded_bytes, 0);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert!(!working_dir.exists());
    assert!(!staging_dir.exists());
    assert!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
            .all(|job| job.job_id != job_id)
    );
    assert!(!pipeline.in_history(job_id));
}

#[tokio::test]
async fn redownload_job_uses_history_blob_when_history_path_is_stale() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = DispatchFixture::with_roots(&temp_dir).await;
    let job_id = JobId(30041);
    let stale_nzb_path = temp_dir.path().join("missing-history.nzb");
    let working_dir = intermediate_dir.join("stale-redownload-job");

    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    let mut row =
        history_row_with_output_dir(job_id, "Stale History Job", "failed", working_dir.clone());
    stamp_working_dir(&intermediate_dir, &working_dir, job_id)
        .await
        .unwrap();
    row.category = Some("tv".to_string());
    row.nzb_path = Some(stale_nzb_path.display().to_string());
    row.metadata = Some(serde_json::to_string(&vec![("source", "history")]).unwrap());
    pipeline.insert_history_with_nzb(&row, &sample_nzb_zstd());

    pipeline.redownload_job(job_id).await.unwrap();

    let state = pipeline.job(job_id);
    assert_eq!(state.status, JobStatus::Queued);
    assert_eq!(state.articles.queued_download_len(), 1);
    assert_eq!(state.downloaded_bytes, 0);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert!(!stale_nzb_path.exists());
    assert!(!pipeline.in_history(job_id));
}

#[test]
fn runtime_transition_failpoint_mapping_uses_lane_transitions() {
    assert_eq!(
        DispatchFixture::status_failpoint(
            PostState::Idle,
            RunState::Active,
            PostState::Verifying,
            RunState::Active,
        ),
        Some(weaver_server_base::e2e_failpoint::STATUS_ENTER_VERIFYING)
    );
    assert_eq!(
        DispatchFixture::status_failpoint(
            PostState::QueuedRepair,
            RunState::Active,
            PostState::Repairing,
            RunState::Active,
        ),
        Some(weaver_server_base::e2e_failpoint::STATUS_ENTER_REPAIRING)
    );
    assert_eq!(
        DispatchFixture::status_failpoint(
            PostState::Idle,
            RunState::Active,
            PostState::Idle,
            RunState::Paused,
        ),
        Some(weaver_server_base::e2e_failpoint::STATUS_ENTER_PAUSED)
    );
    assert_eq!(
        DispatchFixture::status_failpoint(
            PostState::Extracting,
            RunState::Active,
            PostState::WaitingForVolumes,
            RunState::Active,
        ),
        None
    );
}

#[tokio::test]
async fn reconcile_job_progress_does_not_rewrite_status_from_lanes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = DispatchFixture::with_roots(&temp_dir).await;
    let job_id = JobId(30088);
    let spec = standalone_job_spec(
        "Restored Checking",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    pipeline.add_job(job_id, spec).await;

    {
        let state = pipeline.job_mut(job_id);
        state.download_state = DownloadState::Checking;
        state.refresh_legacy_status();
    }

    pipeline.reconcile_job_progress(job_id).await;

    let state = pipeline.job(job_id);
    assert!(matches!(state.download_state, DownloadState::Checking));
    assert!(matches!(state.status, JobStatus::Checking));
}

#[tokio::test]
async fn restore_paused_postprocessing_target_normalizes_to_downloading() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = DispatchFixture::with_roots(&temp_dir).await;
    let job_id = JobId(31206);
    let spec = standalone_job_spec("Restore paused", &[("sample.bin".to_string(), 100)]);
    let working_dir = temp_dir.path().join("restore-paused");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            status: JobStatus::Paused,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: Some(84_000.0),
            paused_resume_status: Some(JobStatus::QueuedExtract),
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .map(|state| state.status.clone()),
        Some(JobStatus::Paused)
    );
    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .and_then(|state| state.paused_resume_status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .and_then(|state| state.queued_extract_at_epoch_ms),
        Some(84_000.0)
    );

    pipeline.resume_job_runtime(job_id).unwrap();

    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn active_rate_limit_does_not_prelease_multiple_bodies() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 8 * 1024 * 1024, 4).await;
    let job_id = JobId(31232);
    let files = (0..4)
        .map(|index| (format!("rate-limit-{index}.bin"), 512 * 1024))
        .collect::<Vec<_>>();
    let spec = standalone_job_spec("Rate Limited Lease", &files);
    pipeline.add_job(job_id, spec).await;

    pipeline.set_rate(Some(128 * 1024));
    pipeline.dispatch();

    assert_eq!(pipeline.articles_in_flight(), 1);
    assert_eq!(queued_articles(&pipeline, job_id), 3);
}

#[tokio::test]
async fn download_done_refunds_rate_limit_estimate_to_actual_raw_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = DispatchFixture::with_roots(&temp_dir).await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: JobId(31233),
            file_index: 0,
        },
        segment_number: 0,
    };

    // A bucket of 4000 B/s holds a quarter second of it: 1000.
    pipeline.set_rate(Some(4_000));
    pipeline.refund_rate(1_000);
    pipeline.charge_rate(1_500);
    pipeline.reserve_rate(segment_id, 1_500);
    assert!(pipeline.rate_should_wait());

    pipeline.finish_rate_limited_body(segment_id, 500).await;

    assert!(!pipeline.rate_reservations().contains_key(&segment_id));
    assert!(!pipeline.rate_should_wait());
}

#[tokio::test]
async fn download_done_charges_rate_limit_for_raw_bytes_above_estimate() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = DispatchFixture::with_roots(&temp_dir).await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: JobId(31234),
            file_index: 0,
        },
        segment_number: 0,
    };

    // A bucket of 4000 B/s holds a quarter second of it: 1000.
    pipeline.set_rate(Some(4_000));
    pipeline.refund_rate(1_000);
    pipeline.charge_rate(500);
    pipeline.reserve_rate(segment_id, 500);
    assert!(!pipeline.rate_should_wait());

    pipeline.finish_rate_limited_body(segment_id, 1_600).await;

    assert!(!pipeline.rate_reservations().contains_key(&segment_id));
    assert!(pipeline.rate_should_wait());
}

#[tokio::test]
async fn a_freshly_posted_job_defers_until_its_articles_have_propagated() {
    // A post is not on the server the instant it is made. Fetching it now
    // produces not-founds that look exactly like missing articles: they burn
    // retries, spend the article's server budget and mark servers unhealthy —
    // and on a par2-less job they can fail a download that would have worked
    // minutes later.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = DispatchFixture::with_roots(&temp_dir).await;
    pipeline.force_propagation_delay(Duration::from_secs(3600));
    let job_id = JobId(20200);
    pipeline
        .add_job(
            job_id,
            posted_job_spec("Silver Horizon Fresh", Some(now_epoch_secs())),
        )
        .await;

    let hold = pipeline
        .propagation_hold_until(job_id)
        .expect("a post made now must be held");
    assert!(hold > Instant::now());

    // The call site itself is pinned by
    // `a_deferred_job_issues_nothing_until_it_is_eligible`, which drives
    // `dispatch_downloads` on a pipeline built with a connection to spend. This
    // test pins the decision function alone: at zero connections
    // `dispatch_downloads` never reaches per-job dispatch, so a status or queue
    // assertion here would hold whether or not the gate exists.

    let snapshot = pipeline
        .list_jobs()
        .into_iter()
        .find(|job| job.job_id == job_id)
        .unwrap();
    assert_eq!(
        snapshot.download_wait_reason.as_deref(),
        Some(PROPAGATION_WAIT_REASON)
    );
    assert!(snapshot.download_retry_at_epoch_ms.is_some());

    // The run loop is told when to wake, rather than rediscovering this by
    // polling.
    let wake = pipeline
        .next_propagation_delay()
        .expect("a deferred job must expose its wakeup");
    assert!(wake <= Duration::from_secs(3600));
    assert!(wake > Duration::from_secs(3500));
}

#[tokio::test]
async fn changing_propagation_delay_updates_the_hold_and_zero_dispatches_immediately() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 1).await;
    pipeline.set_local_propagation_delay(Duration::from_secs(600));
    let mut events = pipeline.events();
    let job_id = JobId(20220);
    pipeline
        .add_job(
            job_id,
            posted_job_spec("Propagation setting", Some(now_epoch_secs() + 60)),
        )
        .await;
    pipeline.dispatch();
    pipeline.publish_jobs();
    let held = pipeline.shared_state().get_job(job_id).unwrap();
    assert_eq!(held.download_state, DownloadState::Queued);
    assert_eq!(
        held.download_wait_reason.as_deref(),
        Some(PROPAGATION_WAIT_REASON)
    );
    let initial_deadline = held.download_retry_at_epoch_ms.unwrap();
    let initial_hold = pipeline.cached_propagation(job_id).unwrap();
    pipeline.apply_propagation_setting(600).await;
    assert_eq!(
        pipeline.cached_propagation(job_id).unwrap(),
        initial_hold,
        "saving unchanged settings must preserve the timer"
    );
    assert_eq!(pipeline.articles_in_flight(), 0);
    let drain_wait_updates = |events: &mut broadcast::Receiver<PipelineEvent>| {
        let mut count = 0;
        while let Ok(event) = events.try_recv() {
            if matches!(event, PipelineEvent::PhaseProgressUpdated { job_id: id } if id == job_id) {
                count += 1;
            }
        }
        count
    };
    assert_eq!(drain_wait_updates(&mut events), 1);
    pipeline.publish_jobs();
    assert_eq!(drain_wait_updates(&mut events), 0);
    assert_eq!(
        pipeline
            .shared_state()
            .get_job(job_id)
            .unwrap()
            .download_retry_at_epoch_ms,
        Some(initial_deadline)
    );

    pipeline.apply_propagation_setting(1200).await;
    pipeline.dispatch();
    let held = pipeline.shared_state().get_job(job_id).unwrap();
    assert!(held.download_retry_at_epoch_ms.unwrap() > initial_deadline + 500_000.0);
    assert_eq!(pipeline.articles_in_flight(), 0);
    assert_eq!(drain_wait_updates(&mut events), 1);

    pipeline.set_runtime_status(job_id, JobStatus::Paused);
    let paused = pipeline
        .list_jobs()
        .into_iter()
        .find(|job| job.job_id == job_id)
        .unwrap();
    assert_eq!(paused.status, JobStatus::Paused);
    assert!(paused.download_wait_reason.is_none());
    pipeline.set_runtime_status(job_id, JobStatus::Downloading);
    assert!(
        pipeline
            .list_jobs()
            .iter()
            .any(|job| job.job_id == job_id && job.download_wait_reason.is_some())
    );

    pipeline.apply_propagation_setting(0).await;
    pipeline.dispatch();
    assert_eq!(pipeline.articles_in_flight(), 1);
    assert!(pipeline.no_cached_propagation());
    assert!(pipeline.next_propagation_delay().is_none());
    let released = pipeline.shared_state().get_job(job_id).unwrap();
    assert!(released.download_wait_reason.is_none());
    assert!(released.download_retry_at_epoch_ms.is_none());
    assert_eq!(released.download_state, DownloadState::Downloading);
    assert_eq!(drain_wait_updates(&mut events), 1);
}

#[tokio::test]
async fn a_deferred_job_issues_nothing_until_it_is_eligible() {
    // The gate's *call site*, driven end to end: `dispatch_downloads` picks the
    // job, reaches `try_dispatch_download_for_job`, and the first thing that
    // function does is ask whether the post has propagated.
    //
    // The fixture that makes this observable is one connection instead of zero.
    // Every other propagation test uses `new_direct_pipeline`, which is built
    // with `total_connections: 0`; the hot-dispatch loop is gated on
    // `active_download_connections < max`, so with no capacity it exits before
    // the per-job call and any assertion after it is vacuous. One connection is
    // the whole fixture — no fake pool and no production seam, and no ramp to
    // account for since dispatch reaches full configured capacity immediately.
    // The empty server list is fine: leasing, activation and the queue drain
    // are synchronous, and only the spawned fetch needs a server, exactly as
    // the surrounding hot-dispatch tests rely on.
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 1).await;
    pipeline.force_propagation_delay(Duration::from_millis(150));

    let job_id = JobId(20209);
    // Dated one minute in the future: the gate saturates a future post to age
    // zero, so the first dispatch is held no matter how many wall-clock seconds
    // fixture setup takes on a loaded machine. A post dated exactly `now` flips
    // to eligible the moment one second ticks before `dispatch_downloads`,
    // because ages are whole epoch seconds and the delay here is only 150ms.
    // The first consult caches a monotonic deadline, which the fixture ages
    // after observing the hold.
    pipeline
        .add_job(
            job_id,
            posted_job_spec(
                "Silver Horizon Deferred Dispatch",
                Some(now_epoch_secs() + 60),
            ),
        )
        .await;
    // `insert_active_job` admits at `Downloading`. A deferred job has never
    // dispatched a segment, so `Queued` is its real state — and staying there is
    // half of what the gate promises.
    pipeline.set_runtime_status(job_id, JobStatus::Queued);
    assert_eq!(
        queued_articles(&pipeline, job_id),
        1,
        "one file, one segment, waiting to be issued"
    );

    pipeline.dispatch();

    assert_eq!(
        pipeline.articles_in_flight(),
        0,
        "a deferred job must not put an article on the wire"
    );
    assert_eq!(
        queued_articles(&pipeline, job_id),
        1,
        "and its segment must still be queued, not leased"
    );
    assert_eq!(
        pipeline.status(job_id),
        Some(JobStatus::Queued),
        "with the job still queued: a deferral is not a status"
    );

    pipeline.elapse_cached_propagation(job_id, Duration::from_millis(300));

    pipeline.dispatch();

    assert_eq!(
        pipeline.articles_in_flight(),
        1,
        "once the post has propagated the same pass dispatches it"
    );
    assert_eq!(
        queued_articles(&pipeline, job_id),
        0,
        "and the segment leaves the queue"
    );
    assert_eq!(
        pipeline.status(job_id),
        Some(JobStatus::Downloading),
        "the first dispatched segment is what moves the job to Downloading"
    );
}

#[tokio::test]
async fn a_job_becomes_eligible_when_its_delay_elapses() {
    // Use a future date so setup cannot consume the delay before the first
    // consult caches the deadline. Age that deadline after observing the hold.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = DispatchFixture::with_roots(&temp_dir).await;
    pipeline.force_propagation_delay(Duration::from_millis(120));
    let job_id = JobId(20203);
    pipeline
        .add_job(
            job_id,
            posted_job_spec("Silver Horizon Soon", Some(now_epoch_secs() + 60)),
        )
        .await;
    assert!(
        pipeline.propagation_hold_until(job_id).is_some(),
        "held at first"
    );

    pipeline.elapse_cached_propagation(job_id, Duration::from_millis(200));

    assert!(
        pipeline.propagation_hold_until(job_id).is_none(),
        "and eligible once the delay has passed"
    );
    assert!(
        pipeline.next_propagation_delay().is_none(),
        "with nothing left for the run loop to wake for"
    );
}

#[tokio::test]
async fn deferral_survives_pause_resume_and_leaves_nothing_behind_on_delete() {
    // A deferral is invisible to the lifecycle: it holds no status, no queue
    // position and no lock, so pause and resume behave exactly as they always
    // did — and removing the job takes its deferral with it rather than
    // leaving a wakeup for a job that no longer exists.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = DispatchFixture::with_roots(&temp_dir).await;
    pipeline.force_propagation_delay(Duration::from_secs(3600));
    let job_id = JobId(20208);
    pipeline
        .add_job(
            job_id,
            posted_job_spec("Silver Horizon Lifecycle", Some(now_epoch_secs())),
        )
        .await;
    assert!(pipeline.propagation_hold_until(job_id).is_some());

    pipeline.set_runtime_status(job_id, JobStatus::Paused);
    assert_eq!(
        pipeline.status(job_id),
        Some(JobStatus::Paused),
        "pause works on a deferred job exactly as on any queued one"
    );
    pipeline.dispatch();
    pipeline.set_runtime_status(job_id, JobStatus::Queued);
    assert_eq!(
        pipeline.status(job_id),
        Some(JobStatus::Queued),
        "and resume returns it to the ordinary queued state"
    );
    assert!(
        pipeline.propagation_hold_until(job_id).is_some(),
        "still deferred afterwards — the clock did not restart and did not stop"
    );

    pipeline.purge_terminal_runtime(job_id);
    assert!(
        pipeline.cached_propagation(job_id).is_none(),
        "a removed job must not leave a wakeup behind"
    );
    assert!(pipeline.next_propagation_delay().is_none());
}

fn with_priority(mut spec: JobSpec, priority: &str) -> JobSpec {
    spec.metadata = vec![("priority".to_string(), priority.to_string())];
    spec
}

#[tokio::test]
async fn owned_download_lane_capacity_failure_requeues_without_failing_the_articles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(20026);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Owned Capacity Requeue", "owned-capacity.bin", &[1024]),
        )
        .await;
    let work = pipeline.pop_download_work(job_id);
    let segment_id = work.segment_id;
    let lease = pipeline.book_lane_zero(work);

    let resets_before = pipeline.lane_reset_calls();
    let pending = pipeline.refuse_acquire(lease, BlockingBodyLaneAcquireError::LocalCapacity);

    assert!(pending.is_empty());
    assert_eq!(pipeline.articles_in_flight(), 0);
    assert_eq!(pipeline.active_connections(), 0);
    assert_eq!(pipeline.tracked_in_flight(job_id), None);
    assert_eq!(pipeline.held_connections(job_id), None);
    assert_eq!(pipeline.held_for_file(segment_id.file_id), None);
    assert_eq!(pipeline.lane_reset_calls(), resets_before);
    let restored = pipeline.pop_download_work(job_id);
    assert_eq!(restored.segment_id, segment_id);
    assert_eq!(restored.retry_count, 0);
}

// Health-mutex contention is not a tiering verdict. Before it had its own
// variant it arrived as `NoEligibleServer`, which fails every leased article
// and resets the owned lane pool — churning a healthy cached TLS lane over a
// microsecond-long lock collision.
#[tokio::test]
async fn owned_download_lane_selection_contention_requeues_without_failing_the_articles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(20027);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Owned Contention Requeue", "owned-contention.bin", &[1024]),
        )
        .await;
    let work = pipeline.pop_download_work(job_id);
    let segment_id = work.segment_id;
    let lease = pipeline.book_lane_zero(work);

    let resets_before = pipeline.lane_reset_calls();
    let pending = pipeline.refuse_acquire(lease, BlockingBodyLaneAcquireError::SelectionContended);

    assert!(
        pending.is_empty(),
        "contention must not resolve any of the leased articles"
    );
    assert_eq!(pipeline.articles_in_flight(), 0);
    assert_eq!(pipeline.active_connections(), 0);
    assert_eq!(pipeline.tracked_in_flight(job_id), None);
    assert_eq!(pipeline.held_connections(job_id), None);
    assert_eq!(pipeline.held_for_file(segment_id.file_id), None);
    assert_eq!(
        pipeline.lane_reset_calls(),
        resets_before,
        "a contended selection must not tear down the owned lane pool"
    );
    let restored = pipeline.pop_download_work(job_id);
    assert_eq!(restored.segment_id, segment_id);
    assert_eq!(
        restored.retry_count, 0,
        "contention must not consume a download retry"
    );
}

#[tokio::test]
async fn dispatch_downloads_shares_a_small_hot_job_across_both_lanes_before_the_next_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 2).await;

    let hot_job_id = JobId(20023);
    let secondary_job_id = JobId(20024);

    pipeline
        .add_job(
            hot_job_id,
            with_priority(
                standalone_job_spec(
                    "Hot Normal",
                    &[
                        ("hot-0.bin".to_string(), 512u32),
                        ("hot-1.bin".to_string(), 512u32),
                        ("hot-2.bin".to_string(), 512u32),
                    ],
                ),
                "NORMAL",
            ),
        )
        .await;
    pipeline
        .add_job(
            secondary_job_id,
            with_priority(
                standalone_job_spec("Secondary Normal", &[("secondary.bin".to_string(), 512u32)]),
                "NORMAL",
            ),
        )
        .await;

    pipeline.dispatch();

    assert_eq!(pipeline.articles_in_flight(), 3);
    assert_eq!(pipeline.active_connections(), 2);
    // The hot job's three articles are two lanes' share, not one handout: the
    // second lane takes what the first left rather than opening the next
    // job beside a hot job it could still be fetching. The next job waits
    // for the first lane that finds the hot job drained.
    assert_eq!(queued_articles(&pipeline, hot_job_id), 0);
    assert_eq!(queued_articles(&pipeline, secondary_job_id), 1);
    assert_eq!(pipeline.tracked_in_flight(hot_job_id), Some(3));
    assert_eq!(pipeline.tracked_in_flight(secondary_job_id), None);
}

#[tokio::test]
async fn dispatch_downloads_throttles_hot_job_under_soft_byte_pressure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 2).await;
    pipeline.set_decode_budget(1000);
    pipeline
        .metrics()
        .decode_pending_bytes
        .store(700, Ordering::Relaxed);

    let hot_job_id = JobId(20029);
    let secondary_job_id = JobId(20030);

    pipeline
        .add_job(
            hot_job_id,
            with_priority(
                standalone_job_spec(
                    "Soft Pressure Hot Full",
                    &[
                        ("hot-0.bin".to_string(), 512u32),
                        ("hot-1.bin".to_string(), 512u32),
                        ("hot-2.bin".to_string(), 512u32),
                    ],
                ),
                "NORMAL",
            ),
        )
        .await;
    pipeline
        .add_job(
            secondary_job_id,
            with_priority(
                standalone_job_spec(
                    "Soft Pressure Secondary Parked",
                    &[("secondary.bin".to_string(), 512u32)],
                ),
                "NORMAL",
            ),
        )
        .await;

    let first_pass_started = Instant::now();
    pipeline.dispatch();

    assert_eq!(pipeline.articles_in_flight(), 1);
    assert_eq!(pipeline.active_connections(), 1);
    assert_eq!(pipeline.tracked_in_flight(hot_job_id), Some(1));
    assert!(pipeline.tracked_in_flight(secondary_job_id).is_none());
    assert_eq!(queued_articles(&pipeline, hot_job_id), 2);
    assert_eq!(queued_articles(&pipeline, secondary_job_id), 1);
    assert_eq!(
        pipeline
            .metrics()
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Soft.as_code()
    );
    assert!(
        pipeline
            .soft_dispatch_after()
            .is_some_and(|ready_at| ready_at > first_pass_started),
        "a soft-pressure pass must hold the next one back"
    );

    // The hold lasts a fraction of a second; a pass that arrives inside it is
    // the case under test, so the test puts the next pass inside it rather
    // than racing the clock to get there.
    pipeline.set_soft_dispatch_after(Instant::now() + Duration::from_secs(3600));
    pipeline.dispatch();
    assert_eq!(pipeline.articles_in_flight(), 1);
    assert_eq!(pipeline.active_connections(), 1);
    assert_eq!(queued_articles(&pipeline, hot_job_id), 2);

    pipeline.set_soft_dispatch_after(Instant::now() - Duration::from_millis(1));
    pipeline.dispatch();
    assert_eq!(pipeline.articles_in_flight(), 2);
    assert_eq!(pipeline.active_connections(), 2);
    assert_eq!(pipeline.tracked_in_flight(hot_job_id), Some(2));
    assert_eq!(queued_articles(&pipeline, hot_job_id), 1);
}

#[tokio::test]
async fn dispatch_downloads_suppresses_the_spill_under_soft_byte_pressure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 2).await;
    pipeline.set_decode_budget(1000);
    pipeline
        .metrics()
        .decode_pending_bytes
        .store(700, Ordering::Relaxed);

    let hot_job_id = JobId(20025);
    let secondary_job_id = JobId(20026);

    pipeline
        .add_job(
            hot_job_id,
            with_priority(
                standalone_job_spec("Soft Pressure Hot", &[("hot.bin".to_string(), 512u32)]),
                "NORMAL",
            ),
        )
        .await;
    pipeline
        .add_job(
            secondary_job_id,
            with_priority(
                standalone_job_spec(
                    "Soft Pressure Secondary",
                    &[("secondary.bin".to_string(), 512u32)],
                ),
                "NORMAL",
            ),
        )
        .await;

    pipeline.dispatch();

    assert_eq!(pipeline.articles_in_flight(), 1);
    assert_eq!(pipeline.tracked_in_flight(hot_job_id), Some(1));
    assert!(pipeline.tracked_in_flight(secondary_job_id).is_none());
    assert_eq!(queued_articles(&pipeline, secondary_job_id), 1);
    assert_eq!(
        pipeline
            .metrics()
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Soft.as_code()
    );
    assert_eq!(
        pipeline
            .metrics()
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::Decode.as_code()
    );
}

#[tokio::test]
async fn dispatch_downloads_reorders_after_priority_metadata_change() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 1).await;

    let leading_job_id = JobId(20021);
    let promoted_job_id = JobId(20022);
    let leading_files = (0..66)
        .map(|idx| (format!("lead-{idx}.bin"), 512u32))
        .collect::<Vec<_>>();

    pipeline
        .add_job(
            leading_job_id,
            with_priority(
                standalone_job_spec("Leading Low Priority", &leading_files),
                "LOW",
            ),
        )
        .await;
    pipeline
        .add_job(
            promoted_job_id,
            with_priority(
                standalone_job_spec("Promoted Priority", &[("promoted.bin".to_string(), 512u32)]),
                "LOW",
            ),
        )
        .await;

    {
        let state = pipeline.job_mut(leading_job_id);
        let _ = state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .pop()
            .expect(
                "leading low-priority job should have one completed segment removed from queue",
            );
        state
            .articles
            .active_mut()
            .unwrap()
            .assembly
            .file_mut(NzbFileId {
                job_id: leading_job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 512)
            .unwrap();
    }

    pipeline.dispatch();

    // 65 queued articles minus the one handout the single lane was given.
    let want = pipeline.server_refill_want(0);
    assert_eq!(queued_articles(&pipeline, leading_job_id), 65 - want);
    assert_eq!(queued_articles(&pipeline, promoted_job_id), 1);
    pipeline.settle_lane_dials().await;

    pipeline.finish_test_lanes();
    pipeline.clear_active_passes();
    pipeline.job_mut(promoted_job_id).spec.metadata =
        vec![("priority".to_string(), "NORMAL".to_string())];

    pipeline.dispatch();

    // The promoted job takes the lane; the leading job's queue is untouched.
    assert_eq!(queued_articles(&pipeline, leading_job_id), 65 - want);
    assert_eq!(queued_articles(&pipeline, promoted_job_id), 0);
}

#[tokio::test]
async fn list_jobs_projects_downloading_while_extracting_with_active_download_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 2).await;
    let job_id = JobId(20008);
    pipeline
        .add_job(
            job_id,
            standalone_job_spec(
                "Extracting Still Downloading",
                &[("queued.bin".to_string(), 512)],
            ),
        )
        .await;
    {
        let state = pipeline.job_mut(job_id);
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
        assert_eq!(state.download_state, DownloadState::Complete);
        assert_eq!(state.articles.active().unwrap().download_queue.len(), 1);
    }

    let info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");

    assert_eq!(info.status, JobStatus::Extracting);
    assert_eq!(info.download_state, DownloadState::Complete);
    assert_eq!(info.post_state, PostState::Extracting);

    pipeline.set_in_flight_articles(job_id, 1);
    let active_info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");
    assert_eq!(active_info.download_state, DownloadState::Downloading);
}

#[tokio::test]
async fn lane_less_download_stays_downloading_and_resumes_on_the_gauge_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 2).await;
    let job_id = JobId(20009);
    pipeline
        .add_job(
            job_id,
            standalone_job_spec("Inactive Download", &[("queued.bin".to_string(), 512)]),
        )
        .await;

    pipeline.publish_job(job_id);
    pipeline.begin_download_phase(job_id, 512);
    let counters = pipeline.download_counters(job_id);
    let now = std::time::Instant::now();
    pipeline.set_in_flight_articles(job_id, 1);
    pipeline.publish_job(job_id);
    pipeline.sample_counters(now);
    counters.set_downloaded(64);
    pipeline.record_wire(64);
    pipeline.sample_counters(now + Duration::from_secs(1));
    assert!(
        pipeline
            .shared_state()
            .get_job(job_id)
            .unwrap()
            .phase_progress[0]
            .rate_bps
            .is_some()
    );

    pipeline.clear_in_flight(job_id);
    pipeline.publish_job(job_id);
    pipeline.sample_counters(now + Duration::from_secs(2));
    let inactive = pipeline.shared_state().get_job(job_id).unwrap();
    // Started, so its status stays Downloading; no lane holds it, so its
    // download lane reads as waiting for one.
    assert_eq!(inactive.status, JobStatus::Downloading);
    assert_eq!(inactive.download_state, DownloadState::Queued);
    assert!(inactive.phase_progress.is_empty());

    pipeline.set_in_flight_articles(job_id, 1);
    pipeline.publish_job(job_id);
    pipeline.sample_counters(now + Duration::from_secs(3));
    let active = pipeline.shared_state().get_job(job_id).unwrap();
    assert_eq!(active.download_state, DownloadState::Downloading);
    assert_eq!(active.phase_progress.len(), 1);
    assert_eq!(active.phase_progress[0].phase, JobPhase::Downloading);
    // The rate kept its window while no lane held the job, so it resumes on
    // the gauge's figure rather than warming up a window of its own.
    assert_eq!(
        active.phase_progress[0].rate_bps,
        Some(
            pipeline
                .shared_state()
                .metrics_snapshot()
                .current_download_speed
        )
    );
}

#[tokio::test]
async fn a_started_download_with_only_queued_work_lists_as_downloading() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 2).await;
    let job_id = JobId(20010);
    pipeline
        .add_job(
            job_id,
            standalone_job_spec("Wedged Download", &[("queued.bin".to_string(), 512)]),
        )
        .await;
    {
        let state = pipeline.job(job_id);
        assert_eq!(state.status, JobStatus::Downloading);
        assert_eq!(state.articles.active().unwrap().download_queue.len(), 1);
    }
    assert!(pipeline.held_connections(job_id).is_none());
    assert!(pipeline.tracked_in_flight(job_id).is_none());
    assert!(!pipeline.job_has_decode(job_id));
    assert!(!pipeline.job_has_download_activity(job_id));

    let info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");

    // Started, so its status stays Downloading; no lane holds it, so its
    // download lane reads as waiting for one.
    assert_eq!(info.status, JobStatus::Downloading);
    assert_eq!(info.download_state, DownloadState::Queued);
}

#[tokio::test]
async fn download_phase_rate_matches_the_global_speed_gauge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 2).await;
    let job_id = JobId(20010);
    pipeline
        .add_job(
            job_id,
            standalone_job_spec(
                "Wire Rate",
                &[
                    ("payload-0.bin".to_string(), 512),
                    ("payload-1.bin".to_string(), 512),
                ],
            ),
        )
        .await;
    pipeline.set_in_flight_articles(job_id, 1);
    pipeline.publish_job(job_id);
    pipeline.begin_download_phase(job_id, 1_024);
    let counters = pipeline.download_counters(job_id);
    let now = std::time::Instant::now();
    pipeline.sample_counters(now);

    // Accepted payload advances progress without inventing wire traffic.
    pipeline.job_mut(job_id).downloaded_bytes = 512;
    pipeline.mark_graph_volume_arrived(job_id, 0);
    pipeline.publish_job(job_id);
    counters.set_decoded(512);
    pipeline.sample_counters(now + Duration::from_secs(1));
    let info = pipeline.shared_state().get_job(job_id).unwrap();
    assert_eq!(info.phase_progress[0].completed_bytes, 512);
    assert!(info.phase_progress[0].rate_bps.is_none());
    assert_eq!(
        pipeline
            .shared_state()
            .metrics_snapshot()
            .current_download_speed,
        0
    );

    // Both rate windows sample the exact same clock and wire-byte delta.
    counters.set_downloaded(30_000);
    pipeline.record_wire(30_000);
    pipeline.sample_counters(now + Duration::from_secs(2));
    let info = pipeline.shared_state().get_job(job_id).unwrap();
    let row = info.phase_progress[0]
        .rate_bps
        .expect("wire traffic produces a rate");
    let gauge = pipeline
        .shared_state()
        .metrics_snapshot()
        .current_download_speed;
    assert!(gauge > 0);
    assert_eq!(row, gauge);
    assert_eq!(info.phase_progress[0].completed_bytes, 512);

    // Replacement articles add decoder and wire throughput, but replace
    // payload that the report already counted.
    counters.set_decoded(1_536);
    counters.set_downloaded(60_000);
    pipeline.record_wire(30_000);
    pipeline.sample_counters(now + Duration::from_secs(3));
    let info = pipeline.shared_state().get_job(job_id).unwrap();
    assert_eq!(info.phase_progress[0].completed_bytes, 512);
    assert_eq!(info.phase_progress[0].progress_percent, 50.0);
    assert!(info.phase_progress[0].rate_bps.is_some());
    assert!(info.phase_progress[0].estimated_remaining_ms.is_some());
}

// A job whose lane lease starts downloading: the queue's counters linked to
// the transfer total exactly as a handout links them.
fn start_rate_sum_download(pipeline: &mut DispatchFixture, job_id: JobId) -> RateCounters {
    pipeline.set_in_flight_articles(job_id, 1);
    pipeline.publish_job(job_id);
    pipeline.begin_download_phase(job_id, 1 << 40);
    pipeline.link_download_counters(job_id)
}

// Every downloading row's rate and the gauge on one tick.
fn rate_sum_tick(pipeline: &DispatchFixture, at: std::time::Instant) -> (Vec<(JobId, u64)>, u64) {
    pipeline.sample_counters(at);
    (
        pipeline.shared_state().job_download_rates(),
        pipeline
            .shared_state()
            .metrics_snapshot()
            .current_download_speed,
    )
}

fn assert_rates_sum_to_gauge(rates: &[(JobId, u64)], gauge: u64, tick: u32) {
    let sum: u64 = rates.iter().map(|(_, rate)| rate).sum();
    assert!(gauge > 0, "tick {tick}: the gauge reads traffic");
    // Each row and the gauge truncate to whole bytes per second on their own.
    assert!(
        sum.abs_diff(gauge) <= rates.len() as u64,
        "tick {tick}: rows {rates:?} sum to {sum}, gauge reads {gauge}"
    );
}

#[tokio::test]
async fn downloading_rows_rates_sum_to_the_global_speed_gauge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 6 * 1024 * 1024, 2).await;
    let first = JobId(20040);
    let second = JobId(20041);
    pipeline
        .add_job(
            first,
            standalone_job_spec("Rate Sum First", &[("first.bin".to_string(), 1 << 30)]),
        )
        .await;
    let first_counters = start_rate_sum_download(&mut pipeline, first);
    let start = std::time::Instant::now();
    let at = |tick: u32| start + Duration::from_millis(250) * tick;

    // The first job alone, at a rate that moves from tick to tick.
    for tick in 0..12 {
        first_counters.credit_body(300_000 + 100_000 * (tick as usize % 4));
        let (rates, gauge) = rate_sum_tick(&pipeline, at(tick));
        if tick > 0 {
            assert_eq!(rates.len(), 1, "tick {tick}");
            assert_rates_sum_to_gauge(&rates, gauge, tick);
        }
    }

    // A second job is added and starts mid-window: its rate is measured on
    // the gauge's window, not on one of its own that begins now.
    pipeline
        .add_job(
            second,
            standalone_job_spec("Rate Sum Second", &[("second.bin".to_string(), 1 << 30)]),
        )
        .await;
    let (_, _) = rate_sum_tick(&pipeline, at(12));
    let second_counters = start_rate_sum_download(&mut pipeline, second);
    for tick in 13..40 {
        first_counters.credit_body(250_000);
        second_counters.credit_body(600_000 + 200_000 * (tick as usize % 3));
        let (rates, gauge) = rate_sum_tick(&pipeline, at(tick));
        assert_eq!(rates.len(), 2, "tick {tick}");
        assert_rates_sum_to_gauge(&rates, gauge, tick);
    }

    // Between lanes the second job shows no rate; once a lane holds it again
    // its rate resumes on the same window, with no warm-up of its own.
    pipeline.clear_in_flight(second);
    pipeline.publish_job(second);
    for tick in 40..44 {
        first_counters.credit_body(250_000);
        let (rates, _) = rate_sum_tick(&pipeline, at(tick));
        assert_eq!(
            rates.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![first]
        );
    }
    pipeline.set_in_flight_articles(second, 1);
    pipeline.publish_job(second);
    for tick in 44..60 {
        first_counters.credit_body(250_000);
        second_counters.credit_body(500_000);
        let (rates, gauge) = rate_sum_tick(&pipeline, at(tick));
        assert_eq!(rates.len(), 2, "tick {tick}");
        assert_rates_sum_to_gauge(&rates, gauge, tick);
    }
}

#[tokio::test]
async fn egress_quota_block_outranks_server_quota_and_yields_to_a_pause() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let block = EgressQuotaBlock {
        egress_id: 4,
        egress_name: "Metered uplink".to_string(),
        used_bytes: 900,
        limit_bytes: 1_000,
        remaining_bytes: 0,
        window_starts_at_epoch_ms: Some(1_000.0),
        window_ends_at_epoch_ms: Some(2_000.0),
        timezone_name: "UTC".to_string(),
    };

    pipeline.shared_state().set_server_quota_blocked(true);
    pipeline
        .shared_state()
        .set_egress_quota_block(Some(block.clone()));
    let state = pipeline.shared_state().download_block();
    assert_eq!(state.kind, DownloadBlockKind::EgressQuota);
    assert_eq!(state.egress_id, Some(4));
    assert_eq!(state.egress_name.as_deref(), Some("Metered uplink"));
    assert_eq!(state.used_bytes, 900);
    assert_eq!(state.limit_bytes, 1_000);
    assert_eq!(state.window_ends_at_epoch_ms, Some(2_000.0));

    // A refresh from the pause state keeps the egress block laid over it.
    pipeline.publish_download_block();
    assert_eq!(
        pipeline.shared_state().download_block().kind,
        DownloadBlockKind::EgressQuota
    );

    // A manual pause outranks it, and the egress details stay readable.
    pipeline.shared_state().hold(Hold::manual(HoldOwner::All));
    pipeline.apply_current_holds().await;
    let state = pipeline.shared_state().download_block();
    assert_eq!(state.kind, DownloadBlockKind::ManualPause);
    assert_eq!(state.egress_id, Some(4));
    pipeline
        .shared_state()
        .release(HoldOrigin::Manual, HoldOwner::All);
    pipeline.apply_current_holds().await;

    // Once the egress clears, the server quota underneath shows again.
    pipeline.shared_state().set_egress_quota_block(None);
    let state = pipeline.shared_state().download_block();
    assert_eq!(state.kind, DownloadBlockKind::ServerQuota);
    assert_eq!(state.egress_id, None);
    pipeline.shared_state().set_server_quota_blocked(false);
    assert_eq!(
        pipeline.shared_state().download_block().kind,
        DownloadBlockKind::None
    );
}

// The pipeline refreshes the quota presentation on every article it takes.
// With no egress block standing, clearing it again takes no lock; a real
// change still takes it and lays the block as before.
#[tokio::test]
async fn refreshing_the_quota_presentation_without_an_egress_block_takes_no_lock() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pipeline = DispatchFixture::new(&temp_dir).await;
    let locks = || pipeline.egress_quota_lock_count();

    let before = locks();
    for _ in 0..3 {
        pipeline.refresh_server_quota();
    }
    assert_eq!(locks(), before, "no egress lock per article");
    assert_eq!(pipeline.shared_state().egress_quota_block(), None);

    let block = EgressQuotaBlock {
        egress_id: 5,
        egress_name: "Egress A".to_string(),
        used_bytes: 1_000,
        limit_bytes: 1_000,
        remaining_bytes: 0,
        window_starts_at_epoch_ms: None,
        window_ends_at_epoch_ms: None,
        timezone_name: "UTC".to_string(),
    };
    pipeline
        .shared_state()
        .set_egress_quota_block(Some(block.clone()));
    assert_eq!(locks(), before + 1);
    assert_eq!(
        pipeline.shared_state().download_block().kind,
        DownloadBlockKind::EgressQuota
    );
    pipeline.refresh_server_quota();
    assert_eq!(
        locks(),
        before + 2,
        "a standing block is cleared under the lock"
    );
    assert_eq!(pipeline.shared_state().egress_quota_block(), None);
    assert_eq!(
        pipeline.shared_state().download_block().kind,
        DownloadBlockKind::None
    );
    pipeline.refresh_server_quota();
    assert_eq!(locks(), before + 2);
}

// Dispatch never sends work to a server that is out of quota, so queued work
// behind an exhausted pool never parks. The block still has to say why it
// cannot move, as soon as the queue holds it.
#[tokio::test]
async fn queued_work_behind_quota_blocked_fills_shows_the_server_quota_block() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let control = weaver_nntp::transfer::ServerTransferRegistry::new().configure(
        weaver_nntp::transfer::StableServerId(31),
        weaver_nntp::transfer::ServerTransferConfig {
            closed: false,
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 128,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    let _spent = control.try_reserve(128).unwrap();
    pipeline.set_client(NntpClient::new(NntpClientConfig {
        servers: vec![weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "spent-quota.example.com".to_string(),
                ..Default::default()
            },
            stable_id: weaver_nntp::transfer::StableServerId(31),
            transfer_control: Some(std::sync::Arc::clone(&control)),
            max_connections: 1,
            group: 0,
            backfill: false,
            retention_days: 0,
        }],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    }));
    assert!(pipeline.client().all_normal_fill_servers_quota_blocked());

    let job_id = JobId(20995);
    let spec = standalone_job_spec("Spent Quota", &[("queued.bin".to_string(), 512u32)]);
    pipeline.add_job(job_id, spec).await;
    pipeline.update_queue_metrics();

    assert!(pipeline.no_server_quota_parks());
    assert!(
        !pipeline
            .job(job_id)
            .articles
            .active()
            .unwrap()
            .download_queue
            .is_empty()
    );
    assert_eq!(
        pipeline.shared_state().download_block().kind,
        DownloadBlockKind::ServerQuota
    );

    // With nothing left queued, an exhausted pool holds nothing back.
    pipeline
        .job_mut(job_id)
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
    pipeline.update_queue_metrics();
    assert_eq!(
        pipeline.shared_state().download_block().kind,
        DownloadBlockKind::None
    );
}

#[tokio::test]
async fn server_quota_ui_state_does_not_globally_stop_unrelated_dispatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::with_backlog(&temp_dir, 5632 * 1024, 2).await;
    let job_id = JobId(20996);
    let spec = standalone_job_spec("Server Quota Gate", &[("queued.bin".to_string(), 512u32)]);
    pipeline.add_job(job_id, spec).await;
    pipeline.shared_state().set_server_quota_blocked(true);
    pipeline
        .shared_state()
        .set_download_block(DownloadBlockState {
            kind: DownloadBlockKind::Scheduled,
            scheduled_speed_limit: Some(1_024),
            ..Default::default()
        });
    assert_eq!(
        pipeline.shared_state().download_block().kind,
        DownloadBlockKind::ServerQuota
    );
    pipeline.shared_state().set_server_quota_blocked(false);
    assert_eq!(
        pipeline.shared_state().download_block().kind,
        DownloadBlockKind::Scheduled
    );
    pipeline.shared_state().set_server_quota_blocked(true);

    pipeline.dispatch();

    assert_eq!(pipeline.articles_in_flight(), 1);
    assert_eq!(queued_articles(&pipeline, job_id), 0);
    // The queue change re-derives the block from the pool, whose server has
    // quota, so the stale presentation gives way to the schedule beneath it.
    assert_eq!(
        pipeline.shared_state().download_block().kind,
        DownloadBlockKind::Scheduled
    );
}

#[tokio::test]
async fn auto_pause_stalled_download_releases_blocking_runtime() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(31232);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    pipeline.add_minimal_job(job_id, "stalled-job", temp_dir.path().join("stalled-job"));
    pipeline.set_total_in_flight(1);
    pipeline.open_pass(job_id);
    pipeline.set_in_flight_articles(job_id, 1);
    pipeline.reserve_rate(segment_id, 256);
    pipeline.install_stalled_lane(segment_id);
    pipeline.mark_idle_past_stall(job_id);

    pipeline.pause_stalled_downloads();

    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .map(|state| state.status.clone()),
        Some(JobStatus::Paused)
    );
    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .and_then(|state| state.paused_resume_status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(pipeline.articles_in_flight(), 0);
    assert!(!pipeline.pass_open(job_id));
    assert!(pipeline.tracked_in_flight(job_id).is_none());
    assert!(pipeline.rate_reservations().is_empty());
}

#[tokio::test]
async fn auto_pause_ignores_stale_jobs_without_inflight_downloads() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(31233);

    pipeline.add_minimal_job(job_id, "idle-job", temp_dir.path().join("idle-job"));
    pipeline.open_pass(job_id);
    pipeline.mark_idle_past_stall(job_id);

    pipeline.pause_stalled_downloads();

    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert!(pipeline.pass_open(job_id));
}

#[tokio::test]
async fn body_activity_keeps_only_its_current_job_alive_without_article_results() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let live = JobId(31234);
    let silent = JobId(31235);
    let stale = JobId(31236);
    let bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let old_bytes = Arc::new(std::sync::atomic::AtomicU64::new(1));
    for job in [live, silent, stale] {
        pipeline.add_minimal_job(job, "paced", temp_dir.path().join(job.0.to_string()));
        pipeline.set_in_flight_articles(job, 1);
    }
    pipeline.install_body_activity_lane(live, stale, &bytes, &old_bytes);
    for _ in 0..4 {
        for job in [live, silent, stale] {
            pipeline.mark_idle_past_stall(job);
        }
        bytes.fetch_add(128, std::sync::atomic::Ordering::Relaxed);
        pipeline.pause_stalled_downloads();
        assert_eq!(pipeline.job(live).status, JobStatus::Downloading);
        assert_eq!(pipeline.job(silent).status, JobStatus::Paused);
        assert_eq!(pipeline.job(stale).status, JobStatus::Paused);
    }
    pipeline.mark_idle_past_stall(live);
    pipeline.pause_stalled_downloads();
    assert_eq!(pipeline.job(live).status, JobStatus::Paused);

    // Resuming the same ID need not replace the pool. A retired lease can
    // still return bytes, but only leases admitted after retirement count.
    pipeline.resume_job_runtime(live).unwrap();
    pipeline.set_in_flight_articles(live, 1);
    pipeline.mark_idle_past_stall(live);
    bytes.fetch_add(128, std::sync::atomic::Ordering::Relaxed);
    pipeline.pause_stalled_downloads();
    assert_eq!(pipeline.job(live).status, JobStatus::Paused);
}

#[tokio::test]
async fn pause_allows_extracting_status_when_download_lane_is_active() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(31241);

    pipeline.add_extracting_job(
        job_id,
        "extracting-job",
        temp_dir.path().join("extracting-job"),
        None,
    );

    pipeline.pause_job_runtime(job_id).unwrap();

    let state = pipeline.job(job_id);
    assert_eq!(state.status, JobStatus::Paused);
    assert_eq!(state.paused_resume_status, Some(JobStatus::Extracting));
    assert_eq!(
        state.paused_resume_download_state,
        Some(DownloadState::Downloading)
    );
    assert_eq!(state.paused_resume_post_state, Some(PostState::Extracting));

    pipeline.resume_job_runtime(job_id).unwrap();

    let state = pipeline.job(job_id);
    assert_eq!(state.status, JobStatus::Extracting);
    assert_eq!(state.download_state, DownloadState::Downloading);
    assert_eq!(state.post_state, PostState::Extracting);
    assert_eq!(pipeline.completion_checks(), vec![job_id]);
}

#[tokio::test]
async fn pause_allows_extracting_status_with_runtime_download_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(31242);

    pipeline.add_extracting_job(
        job_id,
        "extracting-with-active-download",
        temp_dir.path().join("extracting-with-active-download"),
        Some(DownloadState::Complete),
    );
    pipeline.set_total_in_flight(1);
    pipeline.open_pass(job_id);
    pipeline.set_in_flight_articles(job_id, 1);

    pipeline.pause_job_runtime(job_id).unwrap();

    let state = pipeline.job(job_id);
    assert_eq!(state.status, JobStatus::Paused);
    assert_eq!(state.paused_resume_status, Some(JobStatus::Extracting));
    assert_eq!(
        state.paused_resume_download_state,
        Some(DownloadState::Complete)
    );
    assert_eq!(state.paused_resume_post_state, Some(PostState::Extracting));
}

#[tokio::test]
async fn pause_rejects_extracting_state_without_download_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(31243);

    pipeline.add_extracting_job(
        job_id,
        "extracting-only-job",
        temp_dir.path().join("extracting-only-job"),
        Some(DownloadState::Complete),
    );

    let error = pipeline.pause_job_runtime(job_id).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("pause is only supported in queued or downloading states")
    );
}
