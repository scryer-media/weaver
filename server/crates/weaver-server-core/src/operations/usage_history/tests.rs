// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use chrono::NaiveDate;

use super::*;
use crate::persistence::Database;
use crate::persistence::database_target::DatabaseTarget;

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

fn healthy() -> Option<ServerStateReading> {
    Some(ServerStateReading {
        state: "healthy",
        down: false,
    })
}

fn disabled() -> Option<ServerStateReading> {
    Some(ServerStateReading {
        state: "disabled",
        down: true,
    })
}

fn server(id: u32, bytes: u64, tried: u64, ok: u64) -> ServerUsageReading {
    ServerUsageReading {
        server_id: id,
        bytes: Some(bytes),
        articles_tried: tried,
        articles_succeeded: ok,
        state: healthy(),
    }
}

fn reading(servers: Vec<ServerUsageReading>, categories: &[(&str, u64)]) -> UsageReading {
    UsageReading {
        servers,
        categories: categories
            .iter()
            .map(|(name, bytes)| ((*name).to_string(), *bytes))
            .collect(),
    }
}

fn row(day: NaiveDate, kind: UsageConsumerKind, id: &str, bytes: u64) -> UsageDayRow {
    UsageDayRow {
        day: Some(day),
        kind: kind.as_str().to_string(),
        consumer_id: id.to_string(),
        counts: UsageCounts {
            bytes,
            ..UsageCounts::default()
        },
    }
}

fn rows_only(flush: UsageFlush) -> UsageFlush {
    UsageFlush {
        rows: flush.rows,
        fold_before: None,
    }
}

fn sorted(mut rows: Vec<UsageDayRow>) -> Vec<UsageDayRow> {
    rows.sort_by(|a, b| (a.day, &a.kind, &a.consumer_id).cmp(&(b.day, &b.kind, &b.consumer_id)));
    rows
}

#[test]
fn priming_counts_nothing_and_later_readings_count_the_difference() {
    let recorder = UsageRecorder::new();
    // A server whose kept lifetime is 10 GB: none of it is today's.
    recorder.prime(&reading(vec![server(1, 10_000, 0, 0)], &[]), 0);
    let today = date(2026, 3, 4);
    recorder.observe(
        &reading(vec![server(1, 10_500, 7, 6)], &[("tv", 400)]),
        today,
        5,
    );
    let flush = recorder.take(today);
    assert_eq!(
        sorted(flush.rows),
        vec![
            row(today, UsageConsumerKind::Category, "tv", 400),
            UsageDayRow {
                counts: UsageCounts {
                    bytes: 500,
                    articles_tried: 7,
                    articles_succeeded: 6,
                    down_seconds: 0,
                },
                ..row(today, UsageConsumerKind::Server, "1", 0)
            },
        ]
    );
}

#[test]
fn readings_between_flushes_add_into_one_row_per_day_and_consumer() {
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![server(1, 0, 0, 0)], &[]), 0);
    let today = date(2026, 3, 4);
    for tick in 1..=10u64 {
        recorder.observe(
            &reading(vec![server(1, tick * 100, tick * 2, tick)], &[]),
            today,
            tick as i64 * 5,
        );
    }
    let flush = recorder.take(today);
    assert_eq!(flush.rows.len(), 1);
    assert_eq!(
        flush.rows[0].counts,
        UsageCounts {
            bytes: 1_000,
            articles_tried: 20,
            articles_succeeded: 10,
            down_seconds: 0,
        }
    );
    // Nothing moved since: the next batch holds no rows.
    assert!(recorder.take(today).rows.is_empty());
}

#[test]
fn the_day_rolls_at_local_midnight() {
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![server(1, 0, 0, 0)], &[]), 0);
    let before = date(2026, 3, 4);
    let after = date(2026, 3, 5);
    recorder.observe(&reading(vec![server(1, 300, 0, 0)], &[]), before, 5);
    recorder.observe(&reading(vec![server(1, 450, 0, 0)], &[]), after, 10);
    let rows = sorted(recorder.take(after).rows);
    assert_eq!(
        rows,
        vec![
            row(before, UsageConsumerKind::Server, "1", 300),
            row(after, UsageConsumerKind::Server, "1", 150),
        ]
    );
}

#[test]
fn a_total_that_goes_backwards_restarts_without_counting() {
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![], &[("tv", 900)]), 0);
    let today = date(2026, 3, 4);
    recorder.observe(&reading(vec![], &[("tv", 100)]), today, 5);
    recorder.observe(&reading(vec![], &[("tv", 160)]), today, 10);
    assert_eq!(
        recorder.take(today).rows,
        vec![row(today, UsageConsumerKind::Category, "tv", 60)]
    );
}

#[test]
fn a_server_added_later_counts_articles_from_zero_and_bytes_from_now() {
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![], &[]), 0);
    let today = date(2026, 3, 4);
    recorder.observe(&reading(vec![server(9, 5_000, 3, 3)], &[]), today, 5);
    let rows = recorder.take(today).rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].counts,
        UsageCounts {
            bytes: 0,
            articles_tried: 3,
            articles_succeeded: 3,
            down_seconds: 0,
        }
    );
}

#[test]
fn a_reading_without_a_byte_count_keeps_the_last_one() {
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![server(1, 1_000, 0, 0)], &[]), 0);
    let today = date(2026, 3, 4);
    let unknown = ServerUsageReading {
        bytes: None,
        ..server(1, 0, 0, 0)
    };
    recorder.observe(&reading(vec![unknown], &[]), today, 5);
    recorder.observe(&reading(vec![server(1, 1_200, 0, 0)], &[]), today, 10);
    assert_eq!(
        recorder.take(today).rows,
        vec![row(today, UsageConsumerKind::Server, "1", 200)]
    );
}

#[test]
fn time_down_counts_between_two_down_readings_and_state_keeps_its_start() {
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![server(1, 0, 0, 0)], &[]), 100);
    let today = date(2026, 3, 4);
    let down = ServerUsageReading {
        state: disabled(),
        ..server(1, 0, 0, 0)
    };
    // Healthy until 105, seen down at 105: the time before is not down.
    recorder.observe(&reading(vec![down], &[]), today, 105);
    recorder.observe(&reading(vec![down], &[]), today, 110);
    recorder.observe(&reading(vec![down], &[]), today, 117);
    assert_eq!(recorder.server_state_since(1), Some(("disabled", 105)));
    recorder.observe(&reading(vec![server(1, 0, 0, 0)], &[]), today, 120);
    assert_eq!(recorder.server_state_since(1), Some(("healthy", 120)));
    let rows = recorder.take(today).rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].counts.down_seconds, 12);
}

#[test]
fn a_restored_batch_goes_out_with_the_next_one() {
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![], &[]), 0);
    let today = date(2026, 3, 4);
    recorder.observe(&reading(vec![], &[("tv", 100)]), today, 5);
    let failed = recorder.take(today);
    assert!(failed.fold_before.is_some());
    recorder.restore(failed);
    recorder.observe(&reading(vec![], &[("tv", 130)]), today, 10);
    let next = recorder.take(today);
    assert_eq!(
        next.rows,
        vec![row(today, UsageConsumerKind::Category, "tv", 130)]
    );
    // The fold that failed is due again.
    assert!(next.fold_before.is_some());
    // And once written, not again the same day.
    assert!(recorder.take(today).fold_before.is_none());
}

#[test]
fn the_fold_is_due_once_per_date() {
    let recorder = UsageRecorder::new();
    let today = date(2026, 3, 4);
    assert_eq!(recorder.take(today).fold_before, Some(date(2025, 1, 28)));
    assert_eq!(recorder.take(today).fold_before, None);
    assert!(recorder.take(date(2026, 3, 5)).fold_before.is_some());
}

#[test]
fn period_bounds_start_the_week_on_monday() {
    // 2026-03-04 is a Wednesday.
    let today = date(2026, 3, 4);
    assert_eq!(
        period_bounds(UsagePeriod::Week, today),
        (Some(date(2026, 3, 2)), today)
    );
    assert_eq!(
        period_bounds(UsagePeriod::Week, date(2026, 3, 2)),
        (Some(date(2026, 3, 2)), date(2026, 3, 2))
    );
    assert_eq!(
        period_bounds(UsagePeriod::Week, date(2026, 3, 8)),
        (Some(date(2026, 3, 2)), date(2026, 3, 8))
    );
    assert_eq!(
        period_bounds(UsagePeriod::Month, today),
        (Some(date(2026, 3, 1)), today)
    );
    assert_eq!(
        period_bounds(UsagePeriod::Year, today),
        (Some(date(2026, 1, 1)), today)
    );
    assert_eq!(period_bounds(UsagePeriod::All, today), (None, today));
    assert_eq!(
        period_bounds(
            UsagePeriod::Custom {
                from: date(2026, 2, 10),
                to: date(2026, 2, 1)
            },
            today
        ),
        (Some(date(2026, 2, 1)), date(2026, 2, 10))
    );
}

fn seeded_db() -> Database {
    let db = Database::open_in_memory().unwrap();
    seed(&db);
    db
}

fn seed(db: &Database) {
    db.write_usage_flush(&UsageFlush {
        rows: vec![
            row(date(2026, 2, 27), UsageConsumerKind::Server, "1", 1),
            row(date(2026, 3, 2), UsageConsumerKind::Server, "1", 10),
            row(date(2026, 3, 4), UsageConsumerKind::Server, "1", 100),
            row(date(2026, 3, 4), UsageConsumerKind::Server, "2", 1_000),
            row(date(2025, 12, 31), UsageConsumerKind::Server, "2", 10_000),
            row(date(2026, 3, 4), UsageConsumerKind::Category, "tv", 1_100),
        ],
        fold_before: None,
    })
    .unwrap();
}

fn assert_range_sums(db: &Database) {
    let today = date(2026, 3, 4);
    let period = UsagePeriod::Week;
    let from = summary_read_from(period, today);
    let days = db
        .read_usage_days(from, today, Some(UsageConsumerKind::Server))
        .unwrap();
    let totals = db
        .read_usage_totals(Some(UsageConsumerKind::Server))
        .unwrap();
    let summary = summarize_usage(&days, &totals, today, period, UsageConsumerKind::Server);
    assert_eq!(summary.range_start, date(2026, 3, 2));
    assert_eq!(summary.range_end, today);
    assert_eq!(summary.consumers.len(), 2);
    let one = &summary.consumers[0];
    assert_eq!(one.consumer_id, "1");
    assert_eq!(one.today.bytes, 100);
    assert_eq!(one.week.bytes, 110);
    assert_eq!(one.month.bytes, 110);
    assert_eq!(one.year.bytes, 111);
    assert_eq!(one.all_time.bytes, 111);
    assert_eq!(one.range.bytes, 110);
    assert_eq!(
        one.days.iter().map(|(_, c)| c.bytes).collect::<Vec<_>>(),
        vec![10, 0, 100]
    );
    let two = &summary.consumers[1];
    assert_eq!(two.year.bytes, 1_000);
    assert_eq!(two.all_time.bytes, 11_000);
    assert_eq!(summary.total.kind, "server");
    assert_eq!(summary.total.today.bytes, 1_100);
    assert_eq!(summary.total.all_time.bytes, 11_111);
    assert_eq!(
        summary
            .total
            .days
            .iter()
            .map(|(_, c)| c.bytes)
            .collect::<Vec<_>>(),
        vec![10, 0, 1_100]
    );
}

#[test]
fn range_sums_cover_each_span() {
    assert_range_sums(&seeded_db());
}

#[test]
fn reads_filter_by_kind() {
    let db = seeded_db();
    let today = date(2026, 3, 4);
    let categories = db
        .read_usage_days(None, today, Some(UsageConsumerKind::Category))
        .unwrap();
    assert_eq!(
        categories,
        vec![row(today, UsageConsumerKind::Category, "tv", 1_100)]
    );
    let all = db.read_usage_days(Some(today), today, None).unwrap();
    assert_eq!(all.len(), 3);
    let summary = summarize_usage(
        &all,
        &db.read_usage_totals(None).unwrap(),
        today,
        UsagePeriod::Today,
        UsageConsumerKind::Category,
    );
    // Servers and categories count the same bytes: the total is one kind.
    assert_eq!(summary.total.today.bytes, 1_100);
    assert_eq!(summary.consumers.len(), 3);
}

#[test]
fn a_write_adds_onto_the_day_already_kept() {
    let db = seeded_db();
    let today = date(2026, 3, 4);
    db.write_usage_flush(&UsageFlush {
        rows: vec![UsageDayRow {
            counts: UsageCounts {
                bytes: 5,
                articles_tried: 4,
                articles_succeeded: 3,
                down_seconds: 2,
            },
            ..row(today, UsageConsumerKind::Server, "1", 0)
        }],
        fold_before: None,
    })
    .unwrap();
    let rows = db
        .read_usage_days(Some(today), today, Some(UsageConsumerKind::Server))
        .unwrap();
    assert_eq!(
        rows[0].counts,
        UsageCounts {
            bytes: 105,
            articles_tried: 4,
            articles_succeeded: 3,
            down_seconds: 2,
        }
    );
}

#[test]
fn a_restart_keeps_the_previous_day() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("weaver.db");
    let yesterday = date(2026, 3, 3);
    let today = date(2026, 3, 4);
    {
        let db = Database::open(&path).unwrap();
        let recorder = UsageRecorder::new();
        recorder.prime(&reading(vec![server(1, 0, 0, 0)], &[]), 0);
        recorder.observe(&reading(vec![server(1, 700, 0, 0)], &[]), yesterday, 5);
        db.write_usage_flush(&rows_only(recorder.take(yesterday)))
            .unwrap();
    }
    let db = Database::open(&path).unwrap();
    // A new process: its counters start again, its server bytes carry on.
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![server(1, 700, 0, 0)], &[]), 100);
    recorder.observe(&reading(vec![server(1, 750, 0, 0)], &[]), today, 105);
    db.write_usage_flush(&rows_only(recorder.take(today)))
        .unwrap();
    assert_eq!(
        db.read_usage_days(None, today, None).unwrap(),
        vec![
            row(yesterday, UsageConsumerKind::Server, "1", 700),
            row(today, UsageConsumerKind::Server, "1", 50),
        ]
    );
}

fn assert_retention_fold(db: &Database) {
    let today = date(2026, 3, 4);
    let cutoff = retention_cutoff(today);
    db.write_usage_flush(&UsageFlush {
        rows: vec![
            row(
                cutoff.pred_opt().unwrap(),
                UsageConsumerKind::Server,
                "1",
                7,
            ),
            row(date(2020, 1, 1), UsageConsumerKind::Server, "1", 3),
            row(cutoff, UsageConsumerKind::Server, "1", 5),
        ],
        fold_before: None,
    })
    .unwrap();
    for _ in 0..2 {
        // Folding twice changes nothing the second time.
        db.write_usage_flush(&UsageFlush {
            rows: Vec::new(),
            fold_before: Some(cutoff),
        })
        .unwrap();
    }
    let kept = db
        .read_usage_days(None, today, Some(UsageConsumerKind::Server))
        .unwrap();
    let ones: Vec<_> = kept.iter().filter(|r| r.consumer_id == "1").collect();
    assert_eq!(ones[0].day, None);
    assert_eq!(ones[0].counts.bytes, 10);
    assert!(ones.iter().skip(1).all(|r| r.day.unwrap() >= cutoff));
    let totals = db
        .read_usage_totals(Some(UsageConsumerKind::Server))
        .unwrap();
    let one = totals.iter().find(|t| t.consumer_id == "1").unwrap();
    // 10 folded + 5 at the cutoff + 1 + 10 + 100 from the seed.
    assert_eq!(one.counts.bytes, 126);
    // A range with a start never reads the folded row.
    assert!(
        db.read_usage_days(Some(cutoff), today, None)
            .unwrap()
            .iter()
            .all(|r| r.day.is_some())
    );
}

#[test]
fn retention_folds_old_days_into_one_row_per_consumer() {
    assert_retention_fold(&seeded_db());
}

fn assert_finished_counts(db: &Database) {
    for (id, status, at) in [
        (1, "complete", 1_000),
        (2, "complete", 1_500),
        (3, "partial", 1_600),
        (4, "failed", 1_700),
        (5, "cancelled", 1_800),
        (6, "complete", 500),
        (7, "failed", 3_000),
    ] {
        db.insert_job_history(&crate::JobHistoryRow {
            protocol: crate::jobs::Protocol::Nzb,
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
        })
        .unwrap();
    }
    assert_eq!(
        db.count_finished_jobs(Some(1_000), 2_000).unwrap(),
        FinishedJobCounts {
            complete: 2,
            partial: 1,
            failed: 1,
        }
    );
    assert_eq!(
        db.count_finished_jobs(None, 10_000).unwrap(),
        FinishedJobCounts {
            complete: 3,
            partial: 1,
            failed: 2,
        }
    );
}

#[test]
fn finished_jobs_count_by_how_they_ended() {
    assert_finished_counts(&Database::open_in_memory().unwrap());
}

async fn open_postgres_usage_test_db(test_name: &str) -> Option<(Database, sqlx::PgPool, String)> {
    let Ok(base_url) = std::env::var("WEAVER_TEST_POSTGRES_URL") else {
        eprintln!("skipping {test_name}; WEAVER_TEST_POSTGRES_URL is not set");
        return None;
    };
    if base_url.trim().is_empty() {
        eprintln!("skipping {test_name}; WEAVER_TEST_POSTGRES_URL is empty");
        return None;
    }
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("weaver_usage_{test_name}_{}_{suffix}", std::process::id());
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let url = format!("{base_url}{separator}options=-csearch_path%3D{schema}");
    let db = Database::open_target(DatabaseTarget::PostgresUrl(url)).unwrap();
    Some((db, admin, schema))
}

async fn drop_postgres_usage_test_schema(admin: sqlx::PgPool, schema: String) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(&admin)
    .await
    .unwrap();
}

#[tokio::test]
async fn postgres_keeps_the_same_ledger() {
    let Some((db, admin, schema)) = open_postgres_usage_test_db("ledger").await else {
        return;
    };
    seed(&db);
    assert_range_sums(&db);
    assert_retention_fold(&db);
    assert_finished_counts(&db);
    drop(db);
    drop_postgres_usage_test_schema(admin, schema).await;
}
