// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod common;

use chrono::{NaiveDate, TimeZone};
use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use weaver_server_core::operations::{UsageConsumerKind, UsageCounts, UsageDayRow, UsageFlush};

fn day(y: i32, m: u32, d: u32) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(y, m, d)
}

fn usage(date: Option<NaiveDate>, kind: UsageConsumerKind, id: &str, bytes: u64) -> UsageDayRow {
    UsageDayRow {
        day: date,
        kind: kind.as_str().to_string(),
        consumer_id: id.to_string(),
        counts: UsageCounts {
            bytes,
            articles_tried: bytes / 10,
            articles_succeeded: bytes / 20,
            down_seconds: 0,
        },
    }
}

fn seed(harness: &TestHarness) {
    harness
        .db
        .write_usage_flush(&UsageFlush {
            rows: vec![
                usage(day(2024, 5, 1), UsageConsumerKind::Server, "1", 100),
                usage(day(2024, 5, 3), UsageConsumerKind::Server, "1", 300),
                usage(day(2024, 5, 3), UsageConsumerKind::Server, "2", 40),
                usage(day(2024, 4, 30), UsageConsumerKind::Server, "2", 7),
                usage(day(2024, 5, 3), UsageConsumerKind::Category, "tv", 340),
            ],
            fold_before: None,
        })
        .unwrap();
}

const QUERY: &str = r#"
query Usage($range: UsageRange!, $kind: UsageConsumerKind) {
  usageHistory(range: $range, kind: $kind) {
    rangeStart
    rangeEnd
    total { kind range { bytes } allTime { bytes } days { day bytes } }
    consumers {
      kind
      id
      range { bytes articlesTried articlesSucceeded downSeconds }
      allTime { bytes }
      days { day bytes }
    }
    finishedInRange { complete partial failed }
  }
}
"#;

fn variables(value: serde_json::Value) -> async_graphql::Variables {
    async_graphql::Variables::from_json(value)
}

#[tokio::test]
async fn a_custom_range_sums_each_server_and_the_total() {
    let harness = TestHarness::new().await;
    seed(&harness);
    let response = harness
        .execute_with_variables(
            QUERY,
            variables(serde_json::json!({
                "range": { "period": "CUSTOM", "fromDay": "2024-05-01", "toDay": "2024-05-03" },
            })),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let usage = &data["usageHistory"];
    assert_eq!(usage["rangeStart"], "2024-05-01");
    assert_eq!(usage["rangeEnd"], "2024-05-03");
    assert_eq!(usage["total"]["kind"], "server");
    assert_eq!(usage["total"]["range"]["bytes"], 440);
    assert_eq!(usage["total"]["allTime"]["bytes"], 447);
    assert_eq!(
        usage["total"]["days"],
        serde_json::json!([
            { "day": "2024-05-01", "bytes": 100 },
            { "day": "2024-05-02", "bytes": 0 },
            { "day": "2024-05-03", "bytes": 340 },
        ])
    );
    // With no kind asked, every consumer comes back; the total stays servers.
    let all = usage["consumers"].as_array().unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all[0]["kind"], "category");
    let consumers: Vec<_> = all.iter().filter(|c| c["kind"] == "server").collect();
    assert_eq!(consumers.len(), 2);
    assert_eq!(consumers[0]["id"], "1");
    assert_eq!(
        consumers[0]["range"],
        serde_json::json!({
            "bytes": 400, "articlesTried": 40, "articlesSucceeded": 20, "downSeconds": 0
        })
    );
    assert_eq!(consumers[1]["id"], "2");
    assert_eq!(consumers[1]["range"]["bytes"], 40);
    assert_eq!(consumers[1]["allTime"]["bytes"], 47);
}

#[tokio::test]
async fn the_kind_picks_categories() {
    let harness = TestHarness::new().await;
    seed(&harness);
    let response = harness
        .execute_with_variables(
            QUERY,
            variables(serde_json::json!({
                "range": { "period": "CUSTOM", "fromDay": "2024-05-03", "toDay": "2024-05-03" },
                "kind": "CATEGORY",
            })),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let usage = &data["usageHistory"];
    assert_eq!(usage["total"]["kind"], "category");
    assert_eq!(usage["total"]["range"]["bytes"], 340);
    let consumers = usage["consumers"].as_array().unwrap();
    assert_eq!(consumers.len(), 1);
    assert_eq!(consumers[0]["kind"], "category");
    assert_eq!(consumers[0]["id"], "tv");
}

#[tokio::test]
async fn jobs_finished_in_the_range_count_by_how_they_ended() {
    let harness = TestHarness::new().await;
    let start = chrono::Local
        .from_local_datetime(&day(2024, 5, 2).unwrap().and_hms_opt(12, 0, 0).unwrap())
        .earliest()
        .unwrap()
        .timestamp();
    for (id, status, at) in [
        (1, "complete", start),
        (2, "partial", start + 60),
        (3, "failed", start + 120),
        (4, "complete", start - 3 * 86_400),
    ] {
        harness.insert_history_row(&finished_row(id, status, at));
    }
    let response = harness
        .execute_with_variables(
            QUERY,
            variables(serde_json::json!({
                "range": { "period": "CUSTOM", "fromDay": "2024-05-01", "toDay": "2024-05-03" },
            })),
        )
        .await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["usageHistory"]["finishedInRange"],
        serde_json::json!({ "complete": 1, "partial": 1, "failed": 1 })
    );
}

#[tokio::test]
async fn a_custom_range_needs_both_days() {
    let harness = TestHarness::new().await;
    let response = harness
        .execute_with_variables(
            QUERY,
            variables(serde_json::json!({
                "range": { "period": "CUSTOM", "fromDay": "2024-05-01" },
            })),
        )
        .await;
    assert_has_errors(&response);
    let response = harness
        .execute_with_variables(
            QUERY,
            variables(serde_json::json!({
                "range": { "period": "CUSTOM", "fromDay": "2020-01-01", "toDay": "2024-01-01" },
            })),
        )
        .await;
    assert_has_errors(&response);
}

#[tokio::test]
async fn an_empty_ledger_reads_as_zero() {
    let harness = TestHarness::new().await;
    let response = harness
        .execute_with_variables(
            QUERY,
            variables(serde_json::json!({ "range": { "period": "MONTH" } })),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let usage = &data["usageHistory"];
    assert_eq!(usage["total"]["allTime"]["bytes"], 0);
    assert_eq!(usage["consumers"], serde_json::json!([]));
    assert!(
        usage["total"]["days"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["bytes"] == 0)
    );
}

fn finished_row(id: u64, status: &str, at: i64) -> weaver_server_core::JobHistoryRow {
    weaver_server_core::JobHistoryRow {
        protocol: weaver_server_core::jobs::Protocol::Nzb,
        job_id: id,
        job_hash: None,
        name: format!("job-{id}"),
        status: status.to_string(),
        error_message: None,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: at - 10,
        completed_at: at,
        metadata: None,
        server_attribution: None,
    }
}

// The Overview's finished count links to History from the same start; the
// two must agree. History shows a cancelled job as failed, and a paused row
// has not finished.
#[tokio::test]
async fn the_finished_count_matches_the_history_it_links_to() {
    let harness = TestHarness::new().await;
    let date = day(2024, 5, 2).unwrap();
    let start = chrono::Local
        .from_local_datetime(&date.and_hms_opt(0, 0, 0).unwrap())
        .earliest()
        .unwrap()
        .timestamp();
    for (id, status) in [
        (1, "complete"),
        (2, "failed"),
        (3, "cancelled"),
        (4, "paused"),
    ] {
        harness.insert_history_row(&finished_row(id, status, start + 3_600 + id as i64));
    }
    let response = harness
        .execute_with_variables(
            r#"query($since: Int!) {
                usageHistory(range: { period: CUSTOM, fromDay: "2024-05-02", toDay: "2024-05-02" }) {
                    finishedInRange { complete partial failed }
                }
                historyPage(input: { pageIndex: 0, pageSize: 50, completedSinceEpochSec: $since }) {
                    totalCount
                }
            }"#,
            variables(serde_json::json!({ "since": start })),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let finished = &data["usageHistory"]["finishedInRange"];
    assert_eq!(finished["complete"], 1);
    assert_eq!(finished["failed"], 2);
    assert_eq!(data["historyPage"]["totalCount"], 3);
}
