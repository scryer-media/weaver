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
async fn set_speed_limit_becomes_the_global_download_limit() {
    let h = TestHarness::new().await;
    let bandwidth = r#"{ networking { bandwidth { downloadBps uploadBps } } }"#;
    assert_no_errors(
        &h.execute(r#"mutation { setBandwidth(input: { uploadBps: 4096 }) { uploadBps } }"#)
            .await,
    );
    let resp = h
        .execute(r#"mutation { setSpeedLimit(bytesPerSec: 2097161) }"#)
        .await;
    assert_no_errors(&resp);
    let resp = h.execute(bandwidth).await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["networking"]["bandwidth"],
        serde_json::json!({ "downloadBps": 2097161, "uploadBps": 4096 })
    );
    assert_eq!(
        h.db.network_bandwidth().unwrap().download_bps,
        Some(2097161)
    );
    // The old call's 0 is no limit.
    let resp = h
        .execute(r#"mutation { setQueueSpeedLimit(bytesPerSec: 0) { globalState { speedLimitBytesPerSec } } }"#)
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["setQueueSpeedLimit"]["globalState"]["speedLimitBytesPerSec"],
        serde_json::Value::Null
    );
    let resp = h.execute(bandwidth).await;
    assert_eq!(
        response_data(&resp)["networking"]["bandwidth"],
        serde_json::json!({ "downloadBps": null, "uploadBps": 4096 })
    );
}

#[tokio::test]
async fn set_bandwidth_round_trips_and_keeps_a_true_zero() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { setBandwidth(input: { downloadBps: 0, uploadBps: 1024 }) { downloadBps uploadBps } }"#)
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["setBandwidth"],
        serde_json::json!({ "downloadBps": 0, "uploadBps": 1024 })
    );
    let resp = h
        .execute(r#"{ networking { bandwidth { downloadBps uploadBps } } globalQueueState { speedLimitBytesPerSec } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["networking"]["bandwidth"],
        serde_json::json!({ "downloadBps": 0, "uploadBps": 1024 })
    );
    assert_eq!(data["globalQueueState"]["speedLimitBytesPerSec"], 0);
    assert_eq!(
        h.db.network_bandwidth().unwrap(),
        weaver_server_core::proxies::Bandwidth {
            download_bps: Some(0),
            upload_bps: Some(1024),
        }
    );
    let resp = h
        .execute(r#"mutation { setBandwidth(input: {}) { downloadBps uploadBps } }"#)
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        h.db.network_bandwidth().unwrap(),
        weaver_server_core::proxies::Bandwidth::default()
    );
    assert_has_errors(
        &h.execute(r#"mutation { setBandwidth(input: { downloadBps: 9223372036854775808 }) { downloadBps } }"#)
            .await,
    );
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
