use super::*;
use weaver_server_core::JobStatus;
use weaver_server_core::jobs::ids::JobId;

fn base_job(status: JobStatus) -> weaver_server_core::JobInfo {
    let (download_state, post_state, run_state, error) = match &status {
        JobStatus::Queued => (
            weaver_server_core::DownloadState::Queued,
            weaver_server_core::PostState::Idle,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::Downloading => (
            weaver_server_core::DownloadState::Downloading,
            weaver_server_core::PostState::Idle,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::Checking => (
            weaver_server_core::DownloadState::Checking,
            weaver_server_core::PostState::Idle,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::Verifying => (
            weaver_server_core::DownloadState::Complete,
            weaver_server_core::PostState::Verifying,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::QueuedRepair => (
            weaver_server_core::DownloadState::Complete,
            weaver_server_core::PostState::QueuedRepair,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::Repairing => (
            weaver_server_core::DownloadState::Complete,
            weaver_server_core::PostState::Repairing,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::QueuedExtract => (
            weaver_server_core::DownloadState::Complete,
            weaver_server_core::PostState::QueuedExtract,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::Extracting => (
            weaver_server_core::DownloadState::Downloading,
            weaver_server_core::PostState::Extracting,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::Moving => (
            weaver_server_core::DownloadState::Complete,
            weaver_server_core::PostState::Finalizing,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::QueuedPostProcessing => (
            weaver_server_core::DownloadState::Complete,
            weaver_server_core::PostState::QueuedPostProcessing,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::PostProcessing => (
            weaver_server_core::DownloadState::Complete,
            weaver_server_core::PostState::PostProcessing,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::Complete => (
            weaver_server_core::DownloadState::Complete,
            weaver_server_core::PostState::Completed,
            weaver_server_core::RunState::Active,
            None,
        ),
        JobStatus::Failed { error } => (
            weaver_server_core::DownloadState::Failed,
            weaver_server_core::PostState::Failed,
            weaver_server_core::RunState::Active,
            Some(error.clone()),
        ),
        JobStatus::Paused => (
            weaver_server_core::DownloadState::Downloading,
            weaver_server_core::PostState::Idle,
            weaver_server_core::RunState::Paused,
            None,
        ),
    };
    weaver_server_core::JobInfo {
        job_id: JobId(42),
        job_hash: None,
        name: "Facade.Test.Release".to_string(),
        status,
        download_state,
        finalizing_download: false,
        fetching_repair_data: false,
        post_state,
        run_state,
        progress: 0.5,
        total_bytes: 1_000,
        downloaded_bytes: 500,
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
        category: Some("movies".to_string()),
        metadata: vec![
            ("source".to_string(), "api".to_string()),
            (
                CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                "req-42".to_string(),
            ),
        ],
        output_dir: Some("/tmp/out".to_string()),
        error,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        created_at_epoch_ms: 1_700_000_000_000.0,
    }
}

#[test]
fn queue_item_collapses_internal_waiting_states() {
    let repair = queue_item_from_job(&base_job(JobStatus::QueuedRepair));
    assert_eq!(repair.state, QueueItemState::Queued);
    assert_eq!(repair.wait_reason, Some(QueueWaitReason::RepairCapacity));

    let extract = queue_item_from_job(&base_job(JobStatus::QueuedExtract));
    assert_eq!(extract.state, QueueItemState::Queued);
    assert_eq!(
        extract.wait_reason,
        Some(QueueWaitReason::ExtractionCapacity)
    );
}

#[test]
fn queue_item_maps_moving_to_finalizing() {
    let item = queue_item_from_job(&base_job(JobStatus::Moving));
    assert_eq!(item.state, QueueItemState::Finalizing);
    assert_eq!(item.client_request_id.as_deref(), Some("req-42"));
    assert_eq!(item.attributes.len(), 1);
    assert_eq!(item.attributes[0].key, "source");
}

#[test]
fn queue_item_maps_checking_to_verifying() {
    let item = queue_item_from_job(&base_job(JobStatus::Checking));
    assert_eq!(item.state, QueueItemState::Verifying);
    assert_eq!(item.download_state, QueueDownloadState::Checking);
    assert_eq!(item.post_state, QueuePostState::Idle);
}

#[test]
fn queue_item_keeps_active_download_primary_over_extracting() {
    let item = queue_item_from_job(&base_job(JobStatus::Extracting));
    assert_eq!(item.state, QueueItemState::Downloading);
    assert_eq!(item.download_state, QueueDownloadState::Downloading);
    assert_eq!(item.post_state, QueuePostState::Extracting);
}

#[test]
fn queue_item_uses_extracting_after_download_work_finishes() {
    let mut job = base_job(JobStatus::Extracting);
    job.download_state = weaver_server_core::DownloadState::Complete;

    let item = queue_item_from_job(&job);

    assert_eq!(item.state, QueueItemState::Extracting);
    assert_eq!(item.download_state, QueueDownloadState::Complete);
    assert_eq!(item.post_state, QueuePostState::Extracting);
}

#[test]
fn queue_item_uses_post_lane_over_stale_lifecycle_status() {
    let mut job = base_job(JobStatus::Extracting);
    job.download_state = weaver_server_core::DownloadState::Complete;
    job.post_state = weaver_server_core::PostState::Verifying;

    let item = queue_item_from_job(&job);

    assert_eq!(item.state, QueueItemState::Verifying);
    assert_eq!(item.download_state, QueueDownloadState::Complete);
    assert_eq!(item.post_state, QueuePostState::Verifying);
}

#[test]
fn queue_item_keeps_active_download_primary_over_verifying() {
    let mut job = base_job(JobStatus::Verifying);
    job.download_state = weaver_server_core::DownloadState::Downloading;

    let item = queue_item_from_job(&job);

    assert_eq!(item.state, QueueItemState::Downloading);
    assert_eq!(item.download_state, QueueDownloadState::Downloading);
    assert_eq!(item.post_state, QueuePostState::Verifying);
}

#[test]
fn queue_item_shows_finalizing_download_after_the_download_lane_finishes() {
    let mut job = base_job(JobStatus::Downloading);
    job.download_state = weaver_server_core::DownloadState::Complete;
    job.finalizing_download = true;

    let item = queue_item_from_job(&job);

    assert_eq!(item.state, QueueItemState::FinalizingDownload);
    assert_eq!(item.download_state, QueueDownloadState::Complete);
    assert_eq!(item.post_state, QueuePostState::Idle);
}

#[test]
fn queue_item_shows_completion_critical_repair_fetch_ahead_of_generic_finalizing() {
    let mut job = base_job(JobStatus::Downloading);
    job.download_state = weaver_server_core::DownloadState::Complete;
    job.finalizing_download = true;
    job.fetching_repair_data = true;

    let item = queue_item_from_job(&job);

    assert_eq!(item.state, QueueItemState::FetchingRepairData);
    assert_eq!(item.download_state, QueueDownloadState::Complete);
    assert_eq!(item.post_state, QueuePostState::Idle);
}

#[test]
fn queue_item_keeps_active_download_primary_over_repair_fetch() {
    let mut job = base_job(JobStatus::Downloading);
    job.finalizing_download = true;
    job.fetching_repair_data = true;

    let item = queue_item_from_job(&job);

    assert_eq!(item.state, QueueItemState::Downloading);
    assert_eq!(item.download_state, QueueDownloadState::Downloading);
    assert_eq!(item.post_state, QueuePostState::Idle);
}

#[test]
fn synthetic_download_flags_never_override_terminal_or_post_processing_states() {
    for (status, expected) in [
        (JobStatus::Paused, QueueItemState::Paused),
        (
            JobStatus::Failed {
                error: "failed".to_string(),
            },
            QueueItemState::Failed,
        ),
        (JobStatus::Complete, QueueItemState::Completed),
        (JobStatus::Verifying, QueueItemState::Verifying),
        (JobStatus::Repairing, QueueItemState::Repairing),
        (JobStatus::Extracting, QueueItemState::Extracting),
        (JobStatus::Moving, QueueItemState::Finalizing),
        (JobStatus::PostProcessing, QueueItemState::PostProcessing),
    ] {
        let mut job = base_job(status);
        job.download_state = weaver_server_core::DownloadState::Complete;
        job.finalizing_download = true;
        job.fetching_repair_data = true;

        assert_eq!(queue_item_from_job(&job).state, expected);
    }
}

#[test]
fn queue_item_keeps_active_download_primary_over_every_lifecycle_overlay() {
    for status in [
        JobStatus::Queued,
        JobStatus::Downloading,
        JobStatus::Checking,
        JobStatus::Verifying,
        JobStatus::QueuedRepair,
        JobStatus::Repairing,
        JobStatus::QueuedExtract,
        JobStatus::Extracting,
        JobStatus::Moving,
        JobStatus::QueuedPostProcessing,
        JobStatus::PostProcessing,
        JobStatus::Complete,
    ] {
        let mut job = base_job(status);
        job.download_state = weaver_server_core::DownloadState::Downloading;

        assert_eq!(queue_item_from_job(&job).state, QueueItemState::Downloading);
    }
}

#[test]
fn download_accounting_keeps_the_active_download_lane_primary() {
    let mut job = base_job(JobStatus::Extracting);
    job.finalizing_download = true;
    job.fetching_repair_data = true;

    assert_eq!(
        queue_item_state_from_job_info_for_download_accounting(&job),
        QueueItemState::Downloading
    );
}

#[test]
fn download_accounting_uses_control_and_terminal_lanes_over_stale_lifecycle_status() {
    let mut paused = base_job(JobStatus::Downloading);
    paused.run_state = weaver_server_core::RunState::Paused;
    assert_eq!(
        queue_item_state_from_job_info_for_download_accounting(&paused),
        QueueItemState::Paused
    );

    let mut failed = base_job(JobStatus::Downloading);
    failed.post_state = weaver_server_core::PostState::Failed;
    assert_eq!(
        queue_item_state_from_job_info_for_download_accounting(&failed),
        QueueItemState::Failed
    );

    let mut completed = base_job(JobStatus::Downloading);
    completed.download_state = weaver_server_core::DownloadState::Complete;
    completed.post_state = weaver_server_core::PostState::Completed;
    assert_eq!(
        queue_item_state_from_job_info_for_download_accounting(&completed),
        QueueItemState::Completed
    );
}

#[test]
fn inactive_download_projection_is_queued_in_queue_state_and_summary() {
    let mut inactive = base_job(JobStatus::Downloading);
    inactive.download_state = weaver_server_core::DownloadState::Queued;
    let active = base_job(JobStatus::Downloading);
    let items = vec![queue_item_from_job(&inactive), queue_item_from_job(&active)];

    assert_eq!(items[0].state, QueueItemState::Queued);
    assert_eq!(items[1].state, QueueItemState::Downloading);

    let metrics = weaver_server_core::PipelineMetrics::new().snapshot();
    let summary = queue_summary(&items, &metrics);
    assert_eq!(summary.queued_items, 1);
    assert_eq!(summary.active_items, 1);
}

#[test]
fn job_status_gql_maps_checking_to_verifying() {
    assert_eq!(
        JobStatusGql::from(&JobStatus::Checking),
        JobStatusGql::Verifying
    );
}

#[test]
fn queue_item_surfaces_unhealthy_attention() {
    let mut job = base_job(JobStatus::Downloading);
    job.failed_bytes = 1234;
    job.health = 875;
    let item = queue_item_from_job(&job);
    let attention = item.attention.expect("attention should exist");
    assert_eq!(attention.code, "UNHEALTHY_ARTICLES");
    assert!(attention.message.contains("1234"));
}

#[test]
fn queue_item_builds_when_release_parse_is_not_media() {
    let mut job = base_job(JobStatus::Downloading);
    job.name = "ubuntu-24.04.2-live-server-amd64".to_string();
    job.category = None;

    let item = queue_item_from_job(&job);

    assert_eq!(item.display_title, "ubuntu-24 04 2-live-server-amd64");
    assert_eq!(item.parsed_release, ParsedRelease::default());
}

#[test]
fn queue_item_exposes_waiting_post_state_attention() {
    let mut job = base_job(JobStatus::Downloading);
    job.post_state = weaver_server_core::PostState::WaitingForVolumes;
    let item = queue_item_from_job(&job);
    assert_eq!(item.state, QueueItemState::Downloading);
    assert_eq!(item.post_state, QueuePostState::WaitingForVolumes);
    let attention = item.attention.expect("attention should exist");
    assert_eq!(attention.code, "WAITING_FOR_VOLUMES");
}

#[test]
fn persisted_queue_event_replays_pre_lane_payloads() {
    let payload = r#"{
        "occurred_at_ms": 1700000000000,
        "kind": "ItemProgress",
        "item_id": 42,
        "item": {
            "id": 42,
            "name": "Facade.Test.Release",
            "display_title": "Facade Test Release",
            "original_title": "Facade.Test.Release",
            "parsed_release": {
                "normalized_title": "",
                "release_group": null,
                "languages_audio": [],
                "languages_subtitles": [],
                "year": null,
                "quality": null,
                "source": null,
                "video_codec": null,
                "video_encoding": null,
                "audio": null,
                "audio_codecs": [],
                "audio_channels": null,
                "is_dual_audio": false,
                "is_atmos": false,
                "is_dolby_vision": false,
                "detected_hdr": false,
                "is_hdr10plus": false,
                "is_hlg": false,
                "fps": null,
                "is_proper_upload": false,
                "is_repack": false,
                "is_remux": false,
                "is_bd_disk": false,
                "is_ai_enhanced": false,
                "is_hardcoded_subs": false,
                "streaming_service": null,
                "edition": null,
                "anime_version": null,
                "episode": null,
                "parse_confidence": 0.0
            },
            "state": "Downloading",
            "wait_reason": null,
            "error": null,
            "progress_percent": 12.5,
            "total_bytes": 1000,
            "downloaded_bytes": 125,
            "optional_recovery_bytes": 0,
            "optional_recovery_downloaded_bytes": 0,
            "failed_bytes": 0,
            "health": 1000,
            "has_password": false,
            "category": null,
            "attributes": [],
            "client_request_id": null,
            "output_dir": null,
            "created_at": "2023-11-14T22:13:20Z",
            "attention": null
        },
        "state": "Downloading",
        "previous_state": null,
        "attention": null,
        "global_state": null
    }"#;

    let event: PersistedQueueEvent =
        serde_json::from_str(payload).expect("old event payload should decode");
    let item = event.item.expect("item should decode");
    assert_eq!(item.download_state, QueueDownloadState::Downloading);
    assert_eq!(item.post_state, QueuePostState::Idle);
}

#[test]
fn queue_filter_supports_exact_attribute_matches() {
    let mut job = base_job(JobStatus::Downloading);
    job.metadata
        .push(("*scryer_title_id".to_string(), "title-42".to_string()));
    let item = queue_item_from_job(&job);

    let matches = QueueFilterInput {
        attribute_equals: Some(AttributeInput {
            key: "*scryer_title_id".to_string(),
            value: "title-42".to_string(),
        }),
        ..QueueFilterInput::default()
    };
    assert!(matches_queue_filter(&item, Some(&matches)));

    let misses = QueueFilterInput {
        attribute_equals: Some(AttributeInput {
            key: "*scryer_title_id".to_string(),
            value: "title-99".to_string(),
        }),
        ..QueueFilterInput::default()
    };
    assert!(!matches_queue_filter(&item, Some(&misses)));
}
