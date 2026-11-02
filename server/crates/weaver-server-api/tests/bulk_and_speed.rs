// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod common;

use async_graphql::Value;
use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};

#[tokio::test]
async fn set_speed_limit() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { setSpeedLimit(bytesPerSec: 1048576) }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["setSpeedLimit"].as_bool().unwrap());
}

#[tokio::test]
async fn set_speed_limit_becomes_the_configured_limit() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { setSpeedLimit(bytesPerSec: 2097161) }"#)
        .await;
    assert_no_errors(&resp);
    let resp = h.execute(r#"{ settings { maxDownloadSpeed } }"#).await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["settings"]["maxDownloadSpeed"],
        2097161
    );
    let resp = h
        .execute(r#"mutation { setSpeedLimit(bytesPerSec: 0) }"#)
        .await;
    assert_no_errors(&resp);
    let resp = h.execute(r#"{ settings { maxDownloadSpeed } }"#).await;
    assert_eq!(response_data(&resp)["settings"]["maxDownloadSpeed"], 0);
}

#[tokio::test]
async fn set_speed_limit_unlimited() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { setSpeedLimit(bytesPerSec: 0) }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["setSpeedLimit"].as_bool().unwrap());
}

#[tokio::test]
async fn set_speed_limit_very_high() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { setSpeedLimit(bytesPerSec: 999999999999) }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["setSpeedLimit"].as_bool().unwrap());
}

#[tokio::test]
async fn update_jobs_category() {
    let h = TestHarness::new().await;

    // Submit two jobs.
    let id1 = h.submit_test_nzb("bulk-cat-1").await;
    let id2 = h.submit_test_nzb("bulk-cat-2").await;

    // Update both to category "movies".
    let resp = h
        .execute(&format!(
            r#"mutation {{ updateJobs(ids: [{id1}, {id2}], category: "movies") }}"#
        ))
        .await;
    assert_no_errors(&resp);

    // Verify both jobs have the new category.
    let resp = h
        .execute(&format!(r#"{{ job(id: {id1}) {{ category }} }}"#))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["job"]["category"].as_str().unwrap(), "movies");

    let resp = h
        .execute(&format!(r#"{{ job(id: {id2}) {{ category }} }}"#))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["job"]["category"].as_str().unwrap(), "movies");
}

#[tokio::test]
async fn update_jobs_canonicalizes_aliases_and_rejects_unsafe_categories() {
    let h = TestHarness::new().await;
    let add_category = h
        .execute(
            r#"mutation {
                addCategory(input: { name: "Movies", aliases: "film*" }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&add_category);
    let id = h.submit_test_nzb("category-validation").await;

    let canonicalized = h
        .execute(&format!(
            r#"mutation {{ updateJobs(ids: [{id}], category: "FILM-4K") }}"#
        ))
        .await;
    assert_no_errors(&canonicalized);
    let job = h
        .execute(&format!(r#"{{ job(id: {id}) {{ category }} }}"#))
        .await;
    assert_eq!(
        response_data(&job)["job"]["category"].as_str(),
        Some("Movies")
    );

    let rejected = h
        .execute(&format!(
            r#"mutation {{ updateJobs(ids: [{id}], category: "../../outside") }}"#
        ))
        .await;
    assert_has_errors(&rejected);
    assert_eq!(
        rejected.errors[0]
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.get("code"))
            .and_then(|value| match value {
                Value::String(code) => Some(code.as_str()),
                _ => None,
            }),
        Some("INVALID_INPUT")
    );
}

#[tokio::test]
async fn update_jobs_clear_category() {
    let h = TestHarness::new().await;

    // Submit a job with a category.
    let id = h
        .submit_test_nzb_with_options("clear-cat", Some("tv"), None, &[])
        .await;

    // Clear the category by setting it to empty string.
    let resp = h
        .execute(&format!(
            r#"mutation {{ updateJobs(ids: [{id}], category: "") }}"#
        ))
        .await;
    assert_no_errors(&resp);

    // Verify category is cleared (null).
    let resp = h
        .execute(&format!(r#"{{ job(id: {id}) {{ category }} }}"#))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["job"]["category"].is_null());
}

#[tokio::test]
async fn update_jobs_priority_preserves_existing_attributes() {
    let h = TestHarness::new().await;

    let id = h
        .submit_test_nzb_with_options("bulk-priority-merge", None, None, &[("source", "api")])
        .await;

    let resp = h
        .execute(&format!(
            r#"mutation {{ updateJobs(ids: [{id}], priority: "high") }}"#
        ))
        .await;
    assert_no_errors(&resp);

    let resp = h
        .execute(&format!(
            r#"{{ job(id: {id}) {{ metadata {{ key value }} }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let attributes = data["job"]["metadata"].as_array().unwrap();

    assert!(
        attributes
            .iter()
            .any(|entry| entry["key"] == "source" && entry["value"] == "api")
    );
    assert!(
        attributes
            .iter()
            .any(|entry| entry["key"] == "priority" && entry["value"] == "HIGH")
    );
}

#[tokio::test]
async fn update_jobs_rejects_invalid_priority() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("invalid-priority").await;

    let resp = h
        .execute(&format!(
            r#"mutation {{ updateJobs(ids: [{id}], priority: "urgent") }}"#
        ))
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn update_jobs_nonexistent() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { updateJobs(ids: [999999], category: "x") }"#)
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn cancel_jobs_reports_each_id_in_one_batch() {
    let h = TestHarness::new().await;
    let id1 = h.submit_test_nzb("bulk-cancel-1").await;
    let id2 = h.submit_test_nzb("bulk-cancel-2").await;
    let missing = 999_999;

    let resp = h
        .execute(&format!(
            r#"mutation {{ cancelJobs(ids: [{id1}, {missing}, {id2}]) {{ id cancelled errorCode }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let rows = data["cancelJobs"].as_array().unwrap();
    let row = |id: u64| {
        rows.iter()
            .find(|row| row["id"].as_u64() == Some(id))
            .unwrap_or_else(|| panic!("no result for {id}"))
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(row(id1)["cancelled"], true);
    assert_eq!(row(id2)["cancelled"], true);
    assert_eq!(row(missing)["cancelled"], false);
    assert_eq!(row(missing)["errorCode"], "NOT_FOUND");
}

#[tokio::test]
async fn batch_by_ids_applies_found_jobs_and_refuses_missing_ones() {
    let h = TestHarness::new().await;
    let id1 = h.submit_test_nzb("batch-ids-1").await;
    let id2 = h.submit_test_nzb("batch-ids-2").await;
    let missing = id1.max(id2) + 1_000;

    let resp = h
        .execute(&format!(
            r#"mutation {{ pauseQueueItems(target: {{ ids: [{id1}, {id2}, {id1}, {missing}] }}) {{
                matched applied refused {{ id code }}
            }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let result = &data["pauseQueueItems"];
    assert_eq!(result["matched"], 3, "{result:?}");
    assert_eq!(result["applied"], 2, "{result:?}");
    assert_eq!(result["refused"][0]["id"], missing);
    assert_eq!(result["refused"][0]["code"], "NOT_FOUND");

    h.wait_for_job_status(id1, weaver_server_core::JobStatus::Paused)
        .await;
    h.wait_for_job_status(id2, weaver_server_core::JobStatus::Paused)
        .await;
}

#[tokio::test]
async fn batch_by_selection_reaches_every_matching_job_only() {
    let h = TestHarness::new().await;
    let tv1 = h.submit_test_nzb("batch-sel-tv-1").await;
    let tv2 = h.submit_test_nzb("batch-sel-tv-2").await;
    let other = h.submit_test_nzb("batch-sel-other").await;

    let resp = h
        .execute(&format!(
            r#"mutation {{ setQueueItemsCategory(target: {{ ids: [{tv1}, {tv2}] }}, category: "tv") {{
                matched applied
            }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    assert_eq!(response_data(&resp)["setQueueItemsCategory"]["applied"], 2);

    let resp = h
        .execute(
            r#"mutation { setQueueItemsPriority(
                target: { selection: { categories: ["tv"] }, expectedMatched: 2 }, priority: HIGH
            ) { matched applied } }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["setQueueItemsPriority"]["matched"], 2, "{data:?}");
    assert_eq!(data["setQueueItemsPriority"]["applied"], 2, "{data:?}");

    for (id, expected) in [(tv1, Some("HIGH")), (tv2, Some("HIGH")), (other, None)] {
        let resp = h
            .execute(&format!(
                r#"{{ queueItem(id: {id}) {{ attributes {{ key value }} }} }}"#
            ))
            .await;
        assert_no_errors(&resp);
        let data = response_data(&resp);
        let priority = data["queueItem"]["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|attribute| attribute["key"] == "priority")
            .and_then(|attribute| attribute["value"].as_str().map(str::to_owned));
        assert_eq!(priority.as_deref(), expected, "job {id}");
    }
}

#[tokio::test]
async fn batch_refuses_too_many_ids_and_an_ambiguous_target() {
    let h = TestHarness::new().await;
    let ids = (1..=1_001u64)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let resp = h
        .execute(&format!(
            r#"mutation {{ cancelQueueItems(target: {{ ids: [{ids}] }}) {{ matched }} }}"#
        ))
        .await;
    assert_has_errors(&resp);
    assert_eq!(
        resp.errors[0]
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.get("code"))
            .cloned(),
        Some(Value::from("BATCH_TOO_LARGE"))
    );

    let resp = h
        .execute(r#"mutation { resumeQueueItems(target: {}) { matched } }"#)
        .await;
    assert_has_errors(&resp);

    // Repeats are dropped before the cap: 1,001 ids naming 600 jobs fit.
    let repeated = (1..=1_001u64)
        .map(|id| (id % 600 + 1).to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let resp = h
        .execute(&format!(
            r#"mutation {{ cancelQueueItems(target: {{ ids: [{repeated}] }}) {{ matched }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    assert_eq!(response_data(&resp)["cancelQueueItems"]["matched"], 600);
}

fn error_extension(resp: &async_graphql::Response, key: &str) -> Option<Value> {
    resp.errors[0]
        .extensions
        .as_ref()
        .and_then(|extensions| extensions.get(key))
        .cloned()
}

#[tokio::test]
async fn a_selection_acts_only_on_the_count_the_caller_confirmed() {
    let h = TestHarness::new().await;
    let first = h.submit_test_nzb("confirm-sel-1").await;
    let second = h.submit_test_nzb("confirm-sel-2").await;

    // No filter at all names the whole queue only when it says so.
    let resp = h
        .execute(r#"mutation { pauseQueueItems(target: { selection: {}, expectedMatched: 2 }) { matched } }"#)
        .await;
    assert_has_errors(&resp);
    assert_eq!(
        error_extension(&resp, "code"),
        Some(Value::from("INVALID_INPUT"))
    );

    // The operator confirmed one job; a second has joined since, so nothing
    // is paused and the answer carries the count to confirm again.
    let resp = h
        .execute(r#"mutation { pauseQueueItems(target: { selection: { all: true }, expectedMatched: 1 }) { matched } }"#)
        .await;
    assert_has_errors(&resp);
    assert_eq!(
        error_extension(&resp, "code"),
        Some(Value::from("CONFLICT"))
    );
    assert_eq!(error_extension(&resp, "matched"), Some(Value::from(2)));
    for id in [first, second] {
        let resp = h
            .execute(&format!("{{ queueItem(id: {id}) {{ state }} }}"))
            .await;
        assert_no_errors(&resp);
        assert_ne!(
            response_data(&resp)["queueItem"]["state"],
            "PAUSED",
            "job {id}"
        );
    }

    // Confirmed again at the current count, the batch goes through.
    let resp = h
        .execute(r#"mutation { pauseQueueItems(target: { selection: { all: true }, expectedMatched: 2 }) { matched applied } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["pauseQueueItems"]["matched"], 2, "{data:?}");
    assert_eq!(data["pauseQueueItems"]["applied"], 2, "{data:?}");
}
