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
            ..UsageCounts::default()
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
            expire_removed_at: None,
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

fn counted(
    date: Option<NaiveDate>,
    kind: UsageConsumerKind,
    id: &str,
    counts: UsageCounts,
) -> UsageDayRow {
    UsageDayRow {
        day: date,
        kind: kind.as_str().to_string(),
        consumer_id: id.to_string(),
        counts,
    }
}

const INTEGRITY: &str = r#"
query Integrity($range: UsageRange!) {
  integrityHistory(range: $range) {
    rangeStart
    rangeEnd
    finished { id range { events } days { day events } }
    repaired { id range { events } }
    stages { id range { events durationMs } }
    recovery { availableBytes fetchedBytes }
    failures { cause count }
  }
}
"#;

#[tokio::test]
async fn integrity_reads_the_job_ledger_and_the_history() {
    let harness = TestHarness::new().await;
    let events = |events| UsageCounts {
        events,
        ..UsageCounts::default()
    };
    harness
        .db
        .write_usage_flush(&UsageFlush {
            rows: vec![
                counted(
                    day(2024, 5, 1),
                    UsageConsumerKind::Job,
                    "finished",
                    events(3),
                ),
                counted(
                    day(2024, 5, 3),
                    UsageConsumerKind::Job,
                    "finished",
                    events(2),
                ),
                counted(
                    day(2024, 5, 3),
                    UsageConsumerKind::Job,
                    "repaired",
                    events(1),
                ),
                counted(
                    day(2024, 4, 1),
                    UsageConsumerKind::Job,
                    "repaired",
                    events(9),
                ),
                counted(
                    day(2024, 5, 2),
                    UsageConsumerKind::Stage,
                    "repair",
                    UsageCounts {
                        events: 2,
                        duration_ms: 4_000,
                        ..UsageCounts::default()
                    },
                ),
            ],
            fold_before: None,
            expire_removed_at: None,
        })
        .unwrap();
    let noon = chrono::Local
        .from_local_datetime(&day(2024, 5, 2).unwrap().and_hms_opt(12, 0, 0).unwrap())
        .earliest()
        .unwrap()
        .timestamp();
    for (id, status, at, available, fetched) in [
        (1, "complete", noon, 100, 0),
        (2, "failed", noon + 60, 40, 40),
        (3, "failed", noon - 30 * 86_400, 7, 7),
    ] {
        let mut row = finished_row(id, status, at);
        row.optional_recovery_bytes = available;
        row.optional_recovery_downloaded_bytes = fetched;
        harness.insert_history_row(&row);
    }
    harness
        .db
        .set_history_failure_kind(
            2,
            Some(weaver_server_core::history::HistoryFailureKind::Repair),
        )
        .unwrap();
    let response = harness
        .execute_with_variables(
            INTEGRITY,
            variables(serde_json::json!({
                "range": { "period": "CUSTOM", "fromDay": "2024-05-01", "toDay": "2024-05-03" },
            })),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let integrity = &data["integrityHistory"];
    assert_eq!(integrity["finished"]["id"], "finished");
    assert_eq!(integrity["finished"]["range"]["events"], 5);
    assert_eq!(
        integrity["finished"]["days"],
        serde_json::json!([
            { "day": "2024-05-01", "events": 3 },
            { "day": "2024-05-02", "events": 0 },
            { "day": "2024-05-03", "events": 2 },
        ])
    );
    assert_eq!(integrity["repaired"]["range"]["events"], 1);
    assert_eq!(
        integrity["stages"],
        serde_json::json!([{ "id": "repair", "range": { "events": 2, "durationMs": 4000 } }])
    );
    assert_eq!(
        integrity["recovery"],
        serde_json::json!({ "availableBytes": 140, "fetchedBytes": 40 })
    );
    assert_eq!(
        integrity["failures"],
        serde_json::json!([{ "cause": "repair", "count": 1 }])
    );
}

// Every figure of the Integrity page covers the days the ledger does: a
// job that finished before the ledger's first day is in the history but in
// no ledger count, so its failure and recovery are left out, for All and
// for any range that reaches back before that day. The range bar still
// shows the range asked for.
#[tokio::test]
async fn integrity_history_figures_start_where_the_ledger_does() {
    let harness = TestHarness::new().await;
    harness
        .db
        .write_usage_flush(&UsageFlush {
            rows: vec![counted(
                day(2024, 5, 2),
                UsageConsumerKind::Job,
                "finished",
                UsageCounts {
                    events: 1,
                    ..UsageCounts::default()
                },
            )],
            fold_before: None,
            expire_removed_at: None,
        })
        .unwrap();
    let noon = |date: NaiveDate| {
        chrono::Local
            .from_local_datetime(&date.and_hms_opt(12, 0, 0).unwrap())
            .earliest()
            .unwrap()
            .timestamp()
    };
    for (id, at, recovery) in [
        (1, noon(day(2024, 4, 10).unwrap()), 7),
        (2, noon(day(2024, 5, 1).unwrap()), 5),
        (3, noon(day(2024, 5, 2).unwrap()), 40),
    ] {
        let mut row = finished_row(id, "failed", at);
        row.optional_recovery_bytes = recovery;
        harness.insert_history_row(&row);
    }
    for (range, range_start) in [
        (serde_json::json!({ "period": "ALL" }), "2024-05-02"),
        (
            serde_json::json!({ "period": "CUSTOM", "fromDay": "2024-04-01", "toDay": "2024-05-03" }),
            "2024-04-01",
        ),
    ] {
        let response = harness
            .execute_with_variables(INTEGRITY, variables(serde_json::json!({ "range": range })))
            .await;
        assert_no_errors(&response);
        let data = response_data(&response);
        let integrity = &data["integrityHistory"];
        assert_eq!(integrity["rangeStart"], range_start, "{range}");
        assert_eq!(integrity["finished"]["range"]["events"], 1, "{range}");
        assert_eq!(
            integrity["failures"],
            serde_json::json!([{ "cause": "", "count": 1 }]),
            "{range}"
        );
        assert_eq!(
            integrity["recovery"],
            serde_json::json!({ "availableBytes": 40, "fetchedBytes": 0 }),
            "{range}"
        );
    }

    // Once days past retention fold together the ledger's first day is no
    // longer known, and All reads every history row.
    harness
        .db
        .write_usage_flush(&UsageFlush {
            rows: vec![counted(
                None,
                UsageConsumerKind::Job,
                "finished",
                UsageCounts {
                    events: 2,
                    ..UsageCounts::default()
                },
            )],
            fold_before: None,
            expire_removed_at: None,
        })
        .unwrap();
    let response = harness
        .execute_with_variables(
            INTEGRITY,
            variables(serde_json::json!({ "range": { "period": "ALL" } })),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let integrity = &data["integrityHistory"];
    assert_eq!(integrity["finished"]["range"]["events"], 3);
    assert_eq!(
        integrity["failures"],
        serde_json::json!([{ "cause": "", "count": 3 }])
    );
}

#[tokio::test]
async fn integrity_with_no_jobs_reads_as_zero() {
    let harness = TestHarness::new().await;
    let response = harness
        .execute_with_variables(
            INTEGRITY,
            variables(serde_json::json!({ "range": { "period": "WEEK" } })),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let integrity = &data["integrityHistory"];
    assert_eq!(integrity["finished"]["range"]["events"], 0);
    assert_eq!(integrity["repaired"]["id"], "repaired");
    assert_eq!(integrity["stages"], serde_json::json!([]));
    assert_eq!(integrity["failures"], serde_json::json!([]));
    assert_eq!(
        integrity["recovery"],
        serde_json::json!({ "availableBytes": 0, "fetchedBytes": 0 })
    );
}

const TORRENTS: &str = r#"
query Torrents($range: UsageRange!) {
  torrentHistory(range: $range) {
    total { kind range { bytes bytesUploaded durationMs } }
    trackers { id range { bytes bytesUploaded durationMs } }
    peers { torrents connected seeds }
  }
}
"#;

#[tokio::test]
async fn torrents_read_the_total_and_each_tracker() {
    let harness = TestHarness::new().await;
    let seeded = |bytes, uploaded, ms| UsageCounts {
        bytes,
        bytes_uploaded: uploaded,
        duration_ms: ms,
        ..UsageCounts::default()
    };
    harness
        .db
        .write_usage_flush(&UsageFlush {
            rows: vec![
                counted(
                    day(2024, 5, 2),
                    UsageConsumerKind::Torrent,
                    "",
                    seeded(30, 90, 5_000),
                ),
                counted(
                    day(2024, 5, 2),
                    UsageConsumerKind::Tracker,
                    "a.test",
                    seeded(20, 80, 4_000),
                ),
                counted(
                    day(2024, 5, 2),
                    UsageConsumerKind::Tracker,
                    "",
                    seeded(10, 10, 1_000),
                ),
            ],
            fold_before: None,
            expire_removed_at: None,
        })
        .unwrap();
    let response = harness
        .execute_with_variables(
            TORRENTS,
            variables(serde_json::json!({
                "range": { "period": "CUSTOM", "fromDay": "2024-05-01", "toDay": "2024-05-03" },
            })),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let torrents = &data["torrentHistory"];
    assert_eq!(torrents["total"]["kind"], "torrent");
    assert_eq!(
        torrents["total"]["range"],
        serde_json::json!({ "bytes": 30, "bytesUploaded": 90, "durationMs": 5000 })
    );
    assert_eq!(
        torrents["trackers"],
        serde_json::json!([
            { "id": "", "range": { "bytes": 10, "bytesUploaded": 10, "durationMs": 1000 } },
            { "id": "a.test", "range": { "bytes": 20, "bytesUploaded": 80, "durationMs": 4000 } },
        ])
    );
    // No torrent client in this server: no peers.
    assert_eq!(
        torrents["peers"],
        serde_json::json!({ "torrents": 0, "connected": 0, "seeds": 0 })
    );
}

fn quota_server(id: u32, host: &str) -> weaver_server_core::servers::ServerConfig {
    weaver_server_core::servers::ServerConfig {
        id,
        host: host.to_string(),
        port: 563,
        tls: true,
        username: None,
        password: None,
        connections: 4,
        active: true,
        supports_pipelining: false,
        pipelining_depth: None,
        tls_name_mismatch_certificate_der: None,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: weaver_server_core::servers::ServerDownloadQuotaConfig {
            enabled: true,
            limit_bytes: 1_000_000,
            period: weaver_server_core::servers::ServerDownloadQuotaPeriod::Monthly,
            monthly_reset_day: 1,
            ..weaver_server_core::servers::ServerDownloadQuotaConfig::default()
        },
        tls_ca_cert: None,
    }
}

#[tokio::test]
async fn a_removed_server_shows_under_its_old_name_apart_from_a_new_one_with_its_id() {
    let harness = TestHarness::new().await;
    seed(&harness);
    let db = harness.db.clone();
    let removed_at = chrono::Local
        .with_ymd_and_hms(2024, 5, 3, 12, 0, 0)
        .earliest()
        .unwrap();
    let kept = tokio::task::spawn_blocking(move || {
        db.insert_server(&quota_server(9, "news.eweka.nl")).unwrap();
        db.write_usage_flush(&UsageFlush {
            rows: vec![usage(day(2024, 5, 2), UsageConsumerKind::Server, "9", 70)],
            ..UsageFlush::default()
        })
        .unwrap();
        let kept = db
            .delete_server_at(9, removed_at)
            .unwrap()
            .kept_usage
            .unwrap();
        // A new server takes the id and starts from zero.
        db.insert_server(&quota_server(9, "news.other.example"))
            .unwrap();
        db.write_usage_flush(&UsageFlush {
            rows: vec![usage(day(2024, 5, 3), UsageConsumerKind::Server, "9", 5)],
            ..UsageFlush::default()
        })
        .unwrap();
        kept
    })
    .await
    .unwrap();

    let response = harness
        .execute_with_variables(
            r#"query Usage($range: UsageRange!) {
              usageHistory(range: $range, kind: SERVER) {
                total { range { bytes } }
                consumers { id removedHost range { bytes } }
              }
            }"#,
            variables(serde_json::json!({
                "range": { "period": "CUSTOM", "fromDay": "2024-05-01", "toDay": "2024-05-03" },
            })),
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let usage = &data["usageHistory"];
    // The removed server's bytes still count in the servers' total.
    assert_eq!(usage["total"]["range"]["bytes"], 440 + 70 + 5);
    let consumer = |id: &str| {
        usage["consumers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|consumer| consumer["id"] == id)
            .cloned()
            .unwrap_or_else(|| panic!("no consumer {id}"))
    };
    let removed = consumer(&kept);
    assert_eq!(removed["removedHost"], "news.eweka.nl");
    assert_eq!(removed["range"]["bytes"], 70);
    let reused = consumer("9");
    assert_eq!(reused["removedHost"], serde_json::Value::Null);
    assert_eq!(reused["range"]["bytes"], 5);
    assert_eq!(consumer("1")["removedHost"], serde_json::Value::Null);
}
