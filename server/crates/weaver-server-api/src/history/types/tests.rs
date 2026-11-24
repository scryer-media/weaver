// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::types::AttributeInput;
use weaver_server_core::{CLIENT_REQUEST_ID_ATTRIBUTE_KEY, JobHistoryRow};

#[test]
fn history_item_roundtrips_attributes_and_client_request_id() {
    let row = JobHistoryRow {
        protocol: weaver_server_core::Protocol::Nzb,
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
        archived_at: None,
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
fn history_item_builds_for_a_name_that_is_not_media() {
    let row = JobHistoryRow {
        protocol: weaver_server_core::Protocol::Nzb,
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
        archived_at: None,
    };

    let item = history_item_from_row(&row, None);

    assert_eq!(item.display_title, "ubuntu-24 04 2-live-server-amd64");
}

#[test]
fn history_filter_supports_exact_attribute_matches() {
    let row = JobHistoryRow {
        protocol: weaver_server_core::Protocol::Nzb,
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
        archived_at: None,
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
        protocol: weaver_server_core::Protocol::Nzb,
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
        archived_at: None,
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
        protocol: weaver_server_core::Protocol::Nzb,
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
        archived_at: None,
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
}

// A failed job whose failure recorded a decode window over the profile's
// ceiling is failed for memory, and offers a reprocess with no limit. An
// error that only reads like one, a failure over the machine, or a job that
// did not fail does not.
#[tokio::test]
async fn history_item_says_whether_it_failed_for_memory() {
    let row = |status: &str, error: Option<&str>, marked: bool| JobHistoryRow {
        job_id: 80,
        job_hash: None,
        name: "Wide".to_string(),
        status: status.to_string(),
        error_message: error.map(str::to_string),
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
        metadata: marked.then(|| {
            serde_json::to_string(&[(
                weaver_server_core::history::attributes::FAILED_FOR_MEMORY_ATTRIBUTE_KEY,
                "1",
            )])
            .unwrap()
        }),
        server_attribution: None,
        protocol: weaver_server_core::Protocol::Nzb,
        archived_at: None,
    };

    struct Query(HistoryItem);
    #[async_graphql::Object]
    impl Query {
        async fn item(&self) -> HistoryItem {
            self.0.clone()
        }
    }

    for (status, error, marked, expected) in [
        (
            "failed",
            Some("lantern.7z needs 4.0 GiB of decode memory, 512.0 MiB allowed"),
            true,
            true,
        ),
        (
            "failed",
            Some("lantern.7z needs 4.0 GiB of decode memory, 512.0 MiB allowed"),
            false,
            false,
        ),
        (
            "failed",
            Some("lantern.mkv.zst needs 2.0 TiB of decode memory, this machine has 8.0 GiB"),
            false,
            false,
        ),
        ("failed", Some("extract failed"), false, false),
        ("complete", None, true, false),
    ] {
        let schema = async_graphql::Schema::new(
            Query(history_item_from_row(&row(status, error, marked), None)),
            async_graphql::EmptyMutation,
            async_graphql::EmptySubscription,
        );
        let response = schema.execute("{ item { failedForMemory } }").await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(
            response.data.into_json().unwrap(),
            serde_json::json!({ "item": { "failedForMemory": expected } }),
            "{status} {error:?} {marked}"
        );
    }
}

// An RSS job's item link and id are answered, opened, to an admin only, on
// every type that carries a job's attributes: the queue's item, the job and
// the history's item. Any other caller, a diagnostics capture, and a
// request under a lost key read neither, and never the sealed text.
#[tokio::test]
async fn an_rss_link_and_id_are_answered_to_an_admin_only() {
    use crate::jobs::types::{HideRssLinks, Job, QueueItem, queue_item_from_job};
    use weaver_server_core::auth::CallerScope;
    use weaver_server_core::persistence::encryption::{EncryptionKey, encrypt_value};

    const LINK: &str = "https://tracker.invalid/dl/1?passkey=f00d";
    let key = EncryptionKey::generate();
    let mut db = weaver_server_core::Database::open_in_memory().unwrap();
    db.set_encryption_key(key.clone());
    let row = JobHistoryRow {
        job_id: 81,
        job_hash: None,
        name: "Linked".to_string(),
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
        metadata: Some(
            serde_json::to_string(&[
                ("rss.feed_name", "feed".to_string()),
                ("rss.item_id", encrypt_value(&key, LINK).unwrap()),
                ("rss.item_url", encrypt_value(&key, LINK).unwrap()),
            ])
            .unwrap(),
        ),
        server_attribution: None,
        protocol: weaver_server_core::Protocol::Nzb,
        archived_at: None,
    };

    struct Query(HistoryItem, QueueItem, Job);
    #[async_graphql::Object]
    impl Query {
        async fn history(&self) -> HistoryItem {
            self.0.clone()
        }
        async fn queue(&self) -> QueueItem {
            self.1.clone()
        }
        async fn job(&self) -> Job {
            self.2.clone()
        }
    }
    let info = crate::history::query::job_info_from_history_row(&row);
    let item = history_item_from_row(&row, None);
    // Nothing that is serialised whole holds the link, sealed or not.
    let serialised = serde_json::to_string(&item).unwrap();
    assert!(!serialised.contains("enc:v1:") && !serialised.contains("passkey"));
    let schema = async_graphql::Schema::new(
        Query(item, queue_item_from_job(&info), Job::from(&info)),
        async_graphql::EmptyMutation,
        async_graphql::EmptySubscription,
    );
    let query = "{ history { attributes { key value } } queue { attributes { key value } } \
                 job { metadata { key value } } }";
    let answer = |db: weaver_server_core::Database, scope: CallerScope, hide: bool| {
        let schema = schema.clone();
        async move {
            let mut request = async_graphql::Request::new(query).data(db).data(scope);
            if hide {
                request = request.data(HideRssLinks);
            }
            let response = schema.execute(request).await;
            assert!(response.errors.is_empty(), "{:?}", response.errors);
            response.data.into_json().unwrap()
        }
    };
    let entries = |pairs: &[(&str, &str)]| {
        serde_json::Value::Array(
            pairs
                .iter()
                .map(|(key, value)| serde_json::json!({ "key": key, "value": value }))
                .collect(),
        )
    };
    let shown = |pairs: &[(&str, &str)]| {
        serde_json::json!({
            "history": { "attributes": entries(pairs) },
            "queue": { "attributes": entries(pairs) },
            "job": { "metadata": entries(pairs) },
        })
    };
    let public = shown(&[("rss.feed_name", "feed")]);
    let opened = shown(&[
        ("rss.feed_name", "feed"),
        ("rss.item_id", LINK),
        ("rss.item_url", LINK),
    ]);

    assert_eq!(answer(db.clone(), CallerScope::Admin, false).await, opened);
    assert_eq!(answer(db.clone(), CallerScope::Local, false).await, opened);
    for scope in [
        CallerScope::Read,
        CallerScope::Control,
        CallerScope::ScriptRun,
    ] {
        assert_eq!(answer(db.clone(), scope, false).await, public, "{scope:?}");
    }
    assert_eq!(answer(db.clone(), CallerScope::Admin, true).await, public);
    db.set_encryption_key(EncryptionKey::generate());
    assert_eq!(answer(db, CallerScope::Admin, false).await, public);
}

// A client that selects `outcome` sees a partial delivery as its own value;
// `state` and `status` still read it as failed, for the clients that do not
// model it. The words of what it lacks ride on `error`.
#[tokio::test]
async fn a_partial_job_is_partial_in_outcome_and_failed_in_state() {
    use crate::jobs::types::{Job, QueueItem, queue_item_from_job};
    const SAID: &str = "delivered in part; damaged: foo_dr.txt";
    let row = JobHistoryRow {
        job_id: 82,
        job_hash: None,
        name: "Lantern".to_string(),
        status: "partial".to_string(),
        error_message: Some(SAID.to_string()),
        total_bytes: 2_000,
        downloaded_bytes: 2_000,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 10,
        health: 995,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_100,
        metadata: None,
        server_attribution: None,
        protocol: weaver_server_core::Protocol::Nzb,
        archived_at: None,
    };

    struct Query(HistoryItem, QueueItem, Job);
    #[async_graphql::Object]
    impl Query {
        async fn history(&self) -> HistoryItem {
            self.0.clone()
        }
        async fn queue(&self) -> QueueItem {
            self.1.clone()
        }
        async fn job(&self) -> Job {
            self.2.clone()
        }
    }
    let info = crate::history::query::job_info_from_history_row(&row);
    assert_eq!(info.status, weaver_server_core::JobStatus::Partial);
    let schema = async_graphql::Schema::new(
        Query(
            history_item_from_row(&row, None),
            queue_item_from_job(&info),
            Job::from(&info),
        ),
        async_graphql::EmptyMutation,
        async_graphql::EmptySubscription,
    );
    let response = schema
        .execute(
            "{ history { state outcome error } queue { state outcome } \
             job { status outcome } }",
        )
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap(),
        serde_json::json!({
            "history": { "state": "FAILED", "outcome": "PARTIAL", "error": SAID },
            "queue": { "state": "FAILED", "outcome": "PARTIAL" },
            "job": { "status": "FAILED", "outcome": "PARTIAL" },
        })
    );

    // A client that does not ask sees what it saw before.
    let response = schema.execute("{ history { state } job { status } }").await;
    assert_eq!(
        response.data.into_json().unwrap(),
        serde_json::json!({ "history": { "state": "FAILED" }, "job": { "status": "FAILED" } })
    );
}
