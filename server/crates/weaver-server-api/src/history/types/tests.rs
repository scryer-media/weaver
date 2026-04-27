use super::*;
use crate::jobs::types::AttributeInput;
use weaver_server_core::JobHistoryRow;

#[test]
fn history_item_roundtrips_attributes_and_client_request_id() {
    let row = JobHistoryRow {
        job_id: 77,
        name: "History Test".to_string(),
        status: "failed".to_string(),
        error_message: Some("extract failed".to_string()),
        total_bytes: 2_000,
        downloaded_bytes: 1_500,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 500,
        health: 750,
        category: Some("tv".to_string()),
        output_dir: Some("/tmp/history".to_string()),
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_100,
        metadata: Some(
            serde_json::to_string(&vec![
                ("source".to_string(), "rss".to_string()),
                (
                    CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                    "req-history".to_string(),
                ),
            ])
            .unwrap(),
        ),
    };

    let item = history_item_from_row(&row);
    assert_eq!(item.state, QueueItemState::Failed);
    assert_eq!(item.client_request_id.as_deref(), Some("req-history"));
    assert_eq!(item.attributes.len(), 1);
    assert_eq!(item.attributes[0].key, "source");
    assert_eq!(item.attributes[0].value, "rss");
    assert_eq!(item.attention.unwrap().code, "JOB_FAILED");
    assert_eq!(item.created_at.timestamp(), 1_700_000_000);
    assert_eq!(item.completed_at.timestamp(), 1_700_000_100);
}

#[test]
fn history_item_builds_when_release_parse_is_not_media() {
    let row = JobHistoryRow {
        job_id: 78,
        name: "ubuntu-24.04.2-live-server-amd64".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 2_000,
        downloaded_bytes: 2_000,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: Some("/tmp/history".to_string()),
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_100,
        metadata: None,
    };

    let item = history_item_from_row(&row);

    assert_eq!(item.display_title, "ubuntu-24 04 2-live-server-amd64");
    assert_eq!(item.parsed_release, ParsedRelease::default());
}

#[test]
fn history_filter_supports_exact_attribute_matches() {
    let row = JobHistoryRow {
        job_id: 88,
        name: "History Match".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 2_000,
        downloaded_bytes: 2_000,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: Some("tv".to_string()),
        output_dir: Some("/tmp/history".to_string()),
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_100,
        metadata: Some(
            serde_json::to_string(&vec![(
                "*scryer_title_id".to_string(),
                "title-88".to_string(),
            )])
            .unwrap(),
        ),
    };

    let item = history_item_from_row(&row);
    let matches = QueueFilterInput {
        attribute_equals: Some(AttributeInput {
            key: "*scryer_title_id".to_string(),
            value: "title-88".to_string(),
        }),
        ..QueueFilterInput::default()
    };
    assert!(matches_history_filter(&item, Some(&matches)));

    let misses = QueueFilterInput {
        attribute_equals: Some(AttributeInput {
            key: "*scryer_title_id".to_string(),
            value: "title-99".to_string(),
        }),
        ..QueueFilterInput::default()
    };
    assert!(!matches_history_filter(&item, Some(&misses)));
}
