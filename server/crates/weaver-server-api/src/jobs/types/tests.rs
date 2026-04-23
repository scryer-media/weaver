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
        name: "Facade.Test.Release".to_string(),
        status,
        download_state,
        post_state,
        run_state,
        progress: 0.5,
        total_bytes: 1_000,
        downloaded_bytes: 500,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
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
