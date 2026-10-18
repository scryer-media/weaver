// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::types::AttributeInput;
use weaver_server_core::{CLIENT_REQUEST_ID_ATTRIBUTE_KEY, JobHistoryRow};

#[test]
fn history_item_roundtrips_attributes_and_client_request_id() {
    let row = JobHistoryRow {
        job_id: 77,
        job_hash: None,
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
        server_attribution: None,
    };

    let item = history_item_from_row(&row, None);
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
        job_hash: None,
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
        server_attribution: None,
    };

    let item = history_item_from_row(&row, None);

    assert_eq!(item.display_title, "ubuntu-24 04 2-live-server-amd64");
    assert_eq!(item.parsed_release, ParsedRelease::default());
}

#[test]
fn history_filter_supports_exact_attribute_matches() {
    let row = JobHistoryRow {
        job_id: 88,
        job_hash: None,
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
        server_attribution: None,
    };

    let item = history_item_from_row(&row, None);
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

// History reports a password for a job submitted with one as well as for one
// whose archive password was validated, and none otherwise.
#[test]
fn history_item_has_password_for_a_submitted_or_validated_password() {
    use weaver_server_core::history::attributes::{
        SUBMITTED_PASSWORD_ATTRIBUTE_KEY, VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY,
    };
    let row = |metadata: Vec<(String, String)>| JobHistoryRow {
        job_id: 79,
        job_hash: None,
        name: "Password History".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 2_000,
        downloaded_bytes: 2_000,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_100,
        metadata: Some(serde_json::to_string(&metadata).unwrap()),
        server_attribution: None,
    };
    let marked = |key: &str| vec![(key.to_string(), "1".to_string())];
    assert!(
        history_item_from_row(&row(marked(SUBMITTED_PASSWORD_ATTRIBUTE_KEY)), None).has_password
    );
    assert!(
        history_item_from_row(&row(marked(VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY)), None)
            .has_password
    );
    let item = history_item_from_row(&row(marked("source")), None);
    assert!(!item.has_password);
    let item = history_item_from_row(&row(marked(SUBMITTED_PASSWORD_ATTRIBUTE_KEY)), None);
    assert!(
        item.attributes.is_empty(),
        "the marker is not a public attribute"
    );
}

#[tokio::test]
async fn history_item_exposes_full_file_names_but_not_as_an_attribute() {
    let row = JobHistoryRow {
        job_id: 79,
        job_hash: None,
        name: "Posted Names".to_string(),
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
        metadata: Some(
            serde_json::to_string(&vec![(
                weaver_server_core::ingest::FULL_FILE_NAMES_METADATA_KEY.to_string(),
                r#"[{"name":"short.mkv","full_name":"a much longer full name.mkv"}]"#.to_string(),
            )])
            .unwrap(),
        ),
        server_attribution: None,
    };

    struct Query(HistoryItem);
    #[async_graphql::Object]
    impl Query {
        async fn item(&self) -> HistoryItem {
            self.0.clone()
        }
    }

    let item = history_item_from_row(&row, None);
    assert!(item.attributes.is_empty());
    let schema = async_graphql::Schema::new(
        Query(item.clone()),
        async_graphql::EmptyMutation,
        async_graphql::EmptySubscription,
    );
    let response = schema
        .execute("{ item { fullFileNames { name fullName } } }")
        .await;

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap(),
        serde_json::json!({
            "item": {
                "fullFileNames": [
                    {"name": "short.mkv", "fullName": "a much longer full name.mkv"}
                ]
            }
        })
    );
    let table_item = history_table_item_from_row(&row, None);
    assert_eq!(
        table_item.full_file_names_record,
        item.full_file_names_record
    );
}
