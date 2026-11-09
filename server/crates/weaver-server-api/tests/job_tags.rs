// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod common;

use common::{TestHarness, assert_no_errors, make_test_nzb, response_data};
use serde_json::Value;
use weaver_server_core::JobHistoryRow;

fn history_row(job_id: u64, name: &str) -> JobHistoryRow {
    JobHistoryRow {
        protocol: weaver_server_core::Protocol::Nzb,
        job_id,
        job_hash: None,
        name: name.to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 1_000,
        downloaded_bytes: 1_000,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_060,
        metadata: None,
        server_attribution: None,
    }
}

async fn submit_tagged(h: &TestHarness, name: &str, tags: &str) -> u64 {
    let nzb = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        make_test_nzb(name).as_bytes(),
    );
    let response = h
        .execute(&format!(
            r#"mutation {{ submitNzb(input: {{ nzbBase64: "{nzb}", filename: "{name}.nzb", tags: [{tags}] }}) {{ item {{ id tags }} }} }}"#
        ))
        .await;
    assert_no_errors(&response);
    response_data(&response)["submitNzb"]["item"]["id"]
        .as_u64()
        .expect("a queue item id")
}

async fn data(h: &TestHarness, query: &str) -> Value {
    let response = h.execute(query).await;
    assert_no_errors(&response);
    response_data(&response)
}

fn ids(items: &Value) -> Vec<u64> {
    items
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_u64().unwrap())
        .collect()
}

fn tag_counts(data: &Value, field: &str) -> Vec<(String, u64)> {
    data[field]
        .as_array()
        .unwrap()
        .iter()
        .map(|tag| {
            (
                tag["name"].as_str().unwrap().to_string(),
                tag["count"].as_u64().unwrap(),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_submitted_job_takes_its_tags_and_the_queue_lists_filter_by_them() {
    let h = TestHarness::new().await;
    let tagged = submit_tagged(&h, "tagged", r#"" tv ", "hd", "", "a,b""#).await;
    let plain = submit_tagged(&h, "plain", "").await;

    let data = data(
        &h,
        r#"{
          tv: jobs(tag: "tv") { id tags }
          none: jobs(tag: "") { id tags }
          tvCount: jobCount(tag: "tv")
          page: queuePage(input: { pageIndex: 0, pageSize: 50, tag: "hd" }) { totalCount items { id tags } }
          untagged: queuePage(input: { pageIndex: 0, pageSize: 50, tag: "" }) { totalCount items { id } }
          jobTags { name count }
        }"#,
    )
    .await;
    assert_eq!(ids(&data["tv"]), [tagged]);
    assert_eq!(data["tv"][0]["tags"], serde_json::json!(["hd", "tv"]));
    assert_eq!(ids(&data["none"]), [plain]);
    assert_eq!(data["tvCount"].as_u64(), Some(1));
    assert_eq!(data["page"]["totalCount"].as_u64(), Some(1));
    assert_eq!(ids(&data["page"]["items"]), [tagged]);
    assert_eq!(
        data["page"]["items"][0]["tags"],
        serde_json::json!(["hd", "tv"])
    );
    assert_eq!(ids(&data["untagged"]["items"]), [plain]);
    assert_eq!(
        tag_counts(&data, "jobTags"),
        [("hd".to_string(), 1), ("tv".to_string(), 1)]
    );
}

#[tokio::test]
async fn a_queue_batch_selects_by_tag() {
    let h = TestHarness::new().await;
    let tagged = submit_tagged(&h, "batch-tagged", r#""hold""#).await;
    submit_tagged(&h, "batch-plain", "").await;

    let data = data(
        &h,
        r#"mutation { pauseQueueItems(target: { selection: { tag: "hold" }, expectedMatched: 1 }) { matched applied } }"#,
    )
    .await;
    assert_eq!(data["pauseQueueItems"]["matched"].as_u64(), Some(1));
    assert_eq!(data["pauseQueueItems"]["applied"].as_u64(), Some(1));
    h.wait_for_job_status(tagged, weaver_server_core::JobStatus::Paused)
        .await;
}

#[tokio::test]
async fn the_tag_mutations_change_history_jobs_and_the_history_page_filters_by_tag() {
    let h = TestHarness::new().await;
    h.insert_history_row(&history_row(800_001, "first"));
    h.insert_history_row(&history_row(800_002, "second"));

    let made = data(
        &h,
        r#"mutation { createTags(tags: ["kept", " ", "bad,tag"]) { name count } }"#,
    )
    .await;
    assert_eq!(tag_counts(&made, "createTags"), [("kept".to_string(), 0)]);

    // Adding passes over an id of no job and makes the tag.
    let added = data(
        &h,
        r#"mutation { addJobTags(ids: [800001, 800002, 800009], tags: ["tv", "TV"]) { name count } }"#,
    )
    .await;
    assert_eq!(
        tag_counts(&added, "addJobTags"),
        [
            ("kept".to_string(), 0),
            ("TV".to_string(), 2),
            ("tv".to_string(), 2)
        ]
    );

    let set = data(
        &h,
        r#"mutation { setJobTags(ids: [800002], tags: ["hd"]) { name count } }"#,
    )
    .await;
    assert_eq!(
        tag_counts(&set, "setJobTags"),
        [
            ("hd".to_string(), 1),
            ("kept".to_string(), 0),
            ("TV".to_string(), 1),
            ("tv".to_string(), 1)
        ]
    );

    let page = data(
        &h,
        r#"{
          hd: historyPage(input: { pageIndex: 0, pageSize: 25, tag: "hd" }) { totalCount items { id tags } }
          tv: historyPage(input: { pageIndex: 0, pageSize: 25, tag: "tv" }) { totalCount items { id tags } }
          item: historyItem(id: 800001) { tags }
        }"#,
    )
    .await;
    assert_eq!(ids(&page["hd"]["items"]), [800_002]);
    assert_eq!(page["hd"]["items"][0]["tags"], serde_json::json!(["hd"]));
    assert_eq!(ids(&page["tv"]["items"]), [800_001]);
    assert_eq!(page["item"]["tags"], serde_json::json!(["TV", "tv"]));

    // Removing one tag leaves the rest; removing none clears the job.
    data(
        &h,
        r#"mutation { removeJobTags(ids: [800001], tags: ["TV"]) { name } }"#,
    )
    .await;
    let after = data(&h, r#"{ historyItem(id: 800001) { tags } }"#).await;
    assert_eq!(after["historyItem"]["tags"], serde_json::json!(["tv"]));
    data(&h, r#"mutation { removeJobTags(ids: [800001]) { name } }"#).await;
    let untagged = data(
        &h,
        r#"{ historyPage(input: { pageIndex: 0, pageSize: 25, tag: "" }) { items { id } } }"#,
    )
    .await;
    assert_eq!(ids(&untagged["historyPage"]["items"]), [800_001]);

    // Deleting a tag takes it off every job.
    let deleted = data(
        &h,
        r#"mutation { deleteTags(tags: ["hd", "TV"]) { name count } }"#,
    )
    .await;
    assert_eq!(
        tag_counts(&deleted, "deleteTags"),
        [("kept".to_string(), 0), ("tv".to_string(), 0)]
    );
    let gone = data(&h, r#"{ historyItem(id: 800002) { tags } }"#).await;
    assert_eq!(gone["historyItem"]["tags"], serde_json::json!([]));
}
