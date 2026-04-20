use super::*;
use weaver_server_core::JobStatus;
use weaver_server_core::jobs::ids::JobId;

fn base_job(status: JobStatus) -> weaver_server_core::JobInfo {
    weaver_server_core::JobInfo {
        job_id: JobId(42),
        name: "Facade.Test.Release".to_string(),
        status,
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
        error: None,
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
