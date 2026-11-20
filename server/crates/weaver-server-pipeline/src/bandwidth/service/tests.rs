// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::bandwidth::queries::BandwidthQueries;

#[tokio::test]
async fn bandwidth_ledger_flushes_lazily() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthLedgerRuntime::default();

    let now_minute = weaver_server_base::e2e_clock::unix_seconds().div_euclid(60);
    let counters = BandwidthCounters::default();
    counters.record(512);
    runtime.sample(&db, &counters, false).unwrap();
    db.flush_write_queue().await.unwrap();
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        0
    );

    runtime.flush_pending_usage(&db).unwrap();
    db.flush_write_queue().await.unwrap();
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        512
    );
}

fn rename_usage_table(db: &crate::Database, from: &'static str, to: &'static str) {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        crate::persistence::sql_runtime::SqlRuntime::execute(
            datastore.write_exec(),
            &format!("ALTER TABLE {from} RENAME TO {to}"),
            &[],
        )
        .await?;
        Ok(())
    })
    .unwrap();
}

#[tokio::test]
async fn a_usage_batch_whose_write_fails_is_written_once_by_the_next_flush() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthLedgerRuntime::default();
    let now_minute = weaver_server_base::e2e_clock::unix_seconds().div_euclid(60);
    let counters = BandwidthCounters::default();
    counters.record(512);
    runtime.sample(&db, &counters, false).unwrap();

    rename_usage_table(&db, "bandwidth_usage_minute_buckets", "unavailable_usage");
    runtime.flush_pending_usage(&db).unwrap();
    db.flush_write_queue().await.unwrap();
    rename_usage_table(&db, "unavailable_usage", "bandwidth_usage_minute_buckets");
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        0
    );

    runtime.flush_pending_usage(&db).unwrap();
    db.flush_write_queue().await.unwrap();
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        512
    );
    // Committed now: a further flush has nothing left to write.
    runtime.flush_pending_usage(&db).unwrap();
    db.flush_write_queue().await.unwrap();
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        512
    );
    assert!(runtime.pending_usage_by_minute.is_empty());
    assert!(runtime.unacknowledged_usage.is_empty());
}

#[tokio::test]
async fn a_usage_batch_whose_write_commits_leaves_the_ledger() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthLedgerRuntime::default();
    let counters = BandwidthCounters::default();
    counters.record(512);
    runtime.sample(&db, &counters, false).unwrap();

    runtime.flush_pending_usage(&db).unwrap();
    assert!(runtime.pending_usage_by_minute.is_empty());
    assert_eq!(runtime.unacknowledged_usage.len(), 1);
    db.flush_write_queue().await.unwrap();
    runtime.flush_pending_usage(&db).unwrap();
    assert!(runtime.pending_usage_by_minute.is_empty());
    assert_eq!(runtime.pending_usage_bytes, 0);
    assert!(runtime.unacknowledged_usage.is_empty());
}

#[tokio::test]
async fn bandwidth_ledger_flushes_once_the_byte_threshold_is_reached() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthLedgerRuntime::default();

    let now_minute = weaver_server_base::e2e_clock::unix_seconds().div_euclid(60);
    let counters = BandwidthCounters::default();
    counters.record(BANDWIDTH_USAGE_FLUSH_BYTES - 1);
    runtime.sample(&db, &counters, false).unwrap();
    db.flush_write_queue().await.unwrap();
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        0
    );
    counters.record(1);
    runtime.sample(&db, &counters, false).unwrap();
    db.flush_write_queue().await.unwrap();
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        BANDWIDTH_USAGE_FLUSH_BYTES
    );
}

#[test]
fn global_pause_origin_selects_the_block_kind() {
    // The block follows who holds the queue: a schedule's hold presents as
    // Scheduled, the operator's as a pause and a speed test's as its own.
    assert_eq!(
        to_download_block_state(None).kind,
        crate::DownloadBlockKind::None
    );
    assert_eq!(
        to_download_block_state(Some(HoldOrigin::Manual)).kind,
        crate::DownloadBlockKind::ManualPause
    );
    assert_eq!(
        to_download_block_state(Some(HoldOrigin::SpeedTest)).kind,
        crate::DownloadBlockKind::SpeedTest
    );
    assert_eq!(
        to_download_block_state(Some(HoldOrigin::Scheduled)).kind,
        crate::DownloadBlockKind::Scheduled
    );
}

#[tokio::test]
async fn quota_metering_excludes_unmetered_ledger_rows_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quota.db");
    let db = crate::Database::open(&path).unwrap();
    let mut runtime = BandwidthLedgerRuntime::default();
    runtime.record_pending_usage(100, 200, true);
    runtime.record_pending_usage(100, 700, false);
    runtime.record_pending_usage(100, 100, true);
    runtime.flush_pending_usage(&db).unwrap();
    db.flush_write_queue().await.unwrap();
    assert_eq!(db.sum_bandwidth_usage_minutes(100, 101).unwrap(), 1000);
    assert_eq!(
        db.sum_metered_bandwidth_usage_minutes(100, 101).unwrap(),
        300
    );
    let reopened = crate::Database::open(&path).unwrap();
    assert_eq!(
        reopened.sum_bandwidth_usage_minutes(100, 101).unwrap(),
        1000
    );
    assert_eq!(
        reopened
            .sum_metered_bandwidth_usage_minutes(100, 101)
            .unwrap(),
        300
    );
}

#[tokio::test]
async fn independent_counter_sampling_preserves_metering_and_never_double_counts() {
    let db = crate::Database::open_in_memory().unwrap();
    let counters = BandwidthCounters::default();
    let mut ledger = BandwidthLedgerRuntime::default();
    counters.record(200);
    counters.set_metering_enabled(false);
    counters.record(700);
    counters.set_metering_enabled(true);
    counters.record(100);
    ledger.sample(&db, &counters, true).unwrap();
    ledger.sample(&db, &counters, true).unwrap();
    db.flush_write_queue().await.unwrap();
    assert_eq!(counters.total(), 1000);
    assert_eq!(db.sum_bandwidth_usage_minutes(0, i64::MAX).unwrap(), 1000);
    assert_eq!(
        db.sum_metered_bandwidth_usage_minutes(0, i64::MAX).unwrap(),
        300
    );
}
