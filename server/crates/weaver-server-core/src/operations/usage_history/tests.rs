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
        ..UsageReading::default()
    }
}

fn jobs(finished: u64, repaired: u64, stages: &[(&'static str, u64, u64)]) -> UsageReading {
    UsageReading {
        jobs_finished: finished,
        jobs_repaired: repaired,
        stages: stages
            .iter()
            .map(|(stage, runs, total_ms)| StageReading {
                stage,
                runs: *runs,
                total_ms: *total_ms,
            })
            .collect(),
        ..UsageReading::default()
    }
}

fn torrent(job_id: u64, trackers: &[&str], down: u64, up: u64, seeded: u64) -> TorrentUsageReading {
    TorrentUsageReading {
        job_id,
        trackers: trackers.iter().map(|host| (*host).to_string()).collect(),
        downloaded_bytes: down,
        uploaded_bytes: up,
        seeding_seconds: seeded,
    }
}

fn torrents(list: Vec<TorrentUsageReading>) -> UsageReading {
    UsageReading {
        torrents: list,
        ..UsageReading::default()
    }
}

fn counted(kind: UsageConsumerKind, id: &str, counts: UsageCounts, day: NaiveDate) -> UsageDayRow {
    UsageDayRow {
        day: Some(day),
        kind: kind.as_str().to_string(),
        consumer_id: id.to_string(),
        counts,
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
        expire_removed_at: None,
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
                    ..UsageCounts::default()
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
            ..UsageCounts::default()
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
            ..UsageCounts::default()
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
fn a_long_gap_between_readings_adds_at_most_one_step_of_down_time() {
    let recorder = UsageRecorder::new();
    let today = date(2026, 3, 4);
    let down = ServerUsageReading {
        state: disabled(),
        ..server(1, 0, 0, 0)
    };
    recorder.prime(&reading(vec![down], &[]), 1_000);
    // Three days asleep, or a clock stepped forward: one step, not the gap.
    recorder.observe(&reading(vec![down], &[]), today, 1_000 + 3 * 86_400);
    let rows = recorder.take(today).rows;
    assert_eq!(rows[0].counts.down_seconds, USAGE_MAX_DOWN_STEP_SECS as u64);
}

#[test]
fn a_day_never_holds_more_down_time_than_it_has_seconds() {
    let db = Database::open_in_memory().unwrap();
    let day = date(2026, 3, 4);
    let down = |seconds| UsageDayRow {
        counts: UsageCounts {
            down_seconds: seconds,
            ..UsageCounts::default()
        },
        ..row(day, UsageConsumerKind::Server, "1", 0)
    };
    for seconds in [80_000, 80_000] {
        db.write_usage_flush(&UsageFlush {
            rows: vec![down(seconds)],
            fold_before: None,
            expire_removed_at: None,
        })
        .unwrap();
    }
    let kept = db.read_usage_days(Some(day), day, None).unwrap();
    assert_eq!(kept[0].counts.down_seconds, seconds_in_day(day));
}

#[test]
fn removing_a_server_removes_its_data_used() {
    let db = Database::open_in_memory().unwrap();
    let day = date(2026, 3, 4);
    db.write_usage_flush(&UsageFlush {
        rows: vec![
            row(day, UsageConsumerKind::Server, "3", 10),
            row(day, UsageConsumerKind::Server, "4", 20),
            row(day, UsageConsumerKind::Category, "3", 30),
        ],
        fold_before: None,
        expire_removed_at: None,
    })
    .unwrap();
    db.delete_server(3).unwrap();
    let kept: Vec<_> = db
        .read_usage_totals(None)
        .unwrap()
        .into_iter()
        .map(|total| (total.kind, total.consumer_id))
        .collect();
    assert_eq!(
        kept,
        vec![
            ("category".to_string(), "3".to_string()),
            ("server".to_string(), "4".to_string()),
        ]
    );
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
        expire_removed_at: None,
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
                ..UsageCounts::default()
            },
            ..row(today, UsageConsumerKind::Server, "1", 0)
        }],
        fold_before: None,
        expire_removed_at: None,
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
            ..UsageCounts::default()
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
        expire_removed_at: None,
    })
    .unwrap();
    for _ in 0..2 {
        // Folding twice changes nothing the second time.
        db.write_usage_flush(&UsageFlush {
            rows: Vec::new(),
            fold_before: Some(cutoff),
            expire_removed_at: None,
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
    // All as a range reaches back to the folded days, so it equals all time.
    let rows = db
        .read_usage_days(summary_read_from(UsagePeriod::All, today), today, None)
        .unwrap();
    let all_time = db.read_usage_totals(None).unwrap();
    let summary = summarize_usage(
        &rows,
        &all_time,
        today,
        UsagePeriod::All,
        UsageConsumerKind::Server,
    );
    assert_eq!(summary.total.range, summary.total.all_time);
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
        (8, "paused", 1_900),
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
            failed: 2,
        }
    );
    assert_eq!(
        db.count_finished_jobs(None, 10_000).unwrap(),
        FinishedJobCounts {
            complete: 3,
            partial: 1,
            failed: 3,
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
    assert_new_columns_survive_flush_and_fold(&db);
    assert_recovery_and_failure_causes(&db);
    assert_removed_server_kept_until_reset(&db);
    drop(db);
    drop_postgres_usage_test_schema(admin, schema).await;
}

#[test]
fn jobs_and_stages_count_once_at_their_event() {
    let today = date(2026, 3, 4);
    let recorder = UsageRecorder::new();
    recorder.prime(&jobs(0, 0, &[("repair", 0, 0)]), 0);
    recorder.observe(&jobs(2, 1, &[("repair", 1, 1_500)]), today, 5);
    // The same totals read again add nothing.
    recorder.observe(&jobs(2, 1, &[("repair", 1, 1_500)]), today, 10);
    // A stage first seen after priming counts from zero.
    recorder.observe(
        &jobs(3, 1, &[("repair", 1, 1_500), ("extract", 2, 400)]),
        today,
        15,
    );
    let flush = recorder.take(today);
    assert_eq!(
        sorted(flush.rows),
        sorted(vec![
            counted(
                UsageConsumerKind::Job,
                JOBS_FINISHED,
                UsageCounts {
                    events: 3,
                    ..UsageCounts::default()
                },
                today,
            ),
            counted(
                UsageConsumerKind::Job,
                JOBS_REPAIRED,
                UsageCounts {
                    events: 1,
                    ..UsageCounts::default()
                },
                today,
            ),
            counted(
                UsageConsumerKind::Stage,
                "extract",
                UsageCounts {
                    events: 2,
                    duration_ms: 400,
                    ..UsageCounts::default()
                },
                today,
            ),
            counted(
                UsageConsumerKind::Stage,
                "repair",
                UsageCounts {
                    events: 1,
                    duration_ms: 1_500,
                    ..UsageCounts::default()
                },
                today,
            ),
        ])
    );
}

#[test]
fn a_torrent_counts_from_now_under_each_tracker_and_once_in_the_total() {
    let today = date(2026, 3, 4);
    let recorder = UsageRecorder::new();
    // The kept lifetime at priming is not today's.
    recorder.prime(
        &torrents(vec![torrent(1, &["a.test", "b.test"], 1_000, 500, 60)]),
        0,
    );
    recorder.observe(
        &torrents(vec![
            torrent(1, &["a.test", "b.test"], 1_100, 800, 70),
            // First seen after priming, with a kept lifetime: from now.
            torrent(2, &[], 9_000, 9_000, 9_000),
        ]),
        today,
        5,
    );
    recorder.observe(
        &torrents(vec![
            torrent(1, &["a.test", "b.test"], 1_100, 800, 70),
            torrent(2, &[], 9_010, 9_000, 9_000),
        ]),
        today,
        10,
    );
    let one = UsageCounts {
        bytes: 100,
        bytes_uploaded: 300,
        duration_ms: 10_000,
        ..UsageCounts::default()
    };
    let two = UsageCounts {
        bytes: 10,
        ..UsageCounts::default()
    };
    let mut whole = one;
    whole.add(&two);
    assert_eq!(
        sorted(recorder.take(today).rows),
        sorted(vec![
            counted(UsageConsumerKind::Torrent, "", whole, today),
            counted(UsageConsumerKind::Tracker, "a.test", one, today),
            counted(UsageConsumerKind::Tracker, "b.test", one, today),
            counted(UsageConsumerKind::Tracker, "", two, today),
        ])
    );
    // A torrent removed and its id seen again starts from its new totals.
    recorder.observe(&torrents(Vec::new()), today, 15);
    recorder.observe(&torrents(vec![torrent(1, &["a.test"], 5, 5, 5)]), today, 20);
    assert!(recorder.take(today).rows.is_empty());
}

fn assert_new_columns_survive_flush_and_fold(db: &Database) {
    let today = date(2026, 3, 4);
    let old = retention_cutoff(today).pred_opt().unwrap();
    let recorder = UsageRecorder::new();
    recorder.prime(&UsageReading::default(), 0);
    let mut reading = jobs(4, 1, &[("verify", 3, 900)]);
    reading.torrents = vec![torrent(9, &["t.test"], 0, 0, 0)];
    recorder.observe(&reading, old, 5);
    reading.torrents = vec![torrent(9, &["t.test"], 200, 600, 30)];
    recorder.observe(&reading, today, 10);
    db.write_usage_flush(&rows_only(recorder.take(today)))
        .unwrap();

    let read = |kind| {
        db.read_usage_totals(Some(kind))
            .unwrap()
            .into_iter()
            .map(|total| (total.consumer_id, total.counts))
            .collect::<Vec<_>>()
    };
    let check = || {
        assert_eq!(
            read(UsageConsumerKind::Job),
            vec![
                (
                    JOBS_FINISHED.to_string(),
                    UsageCounts {
                        events: 4,
                        ..UsageCounts::default()
                    }
                ),
                (
                    JOBS_REPAIRED.to_string(),
                    UsageCounts {
                        events: 1,
                        ..UsageCounts::default()
                    }
                ),
            ]
        );
        assert_eq!(
            read(UsageConsumerKind::Stage),
            vec![(
                "verify".to_string(),
                UsageCounts {
                    events: 3,
                    duration_ms: 900,
                    ..UsageCounts::default()
                }
            )]
        );
        let seeded = UsageCounts {
            bytes: 200,
            bytes_uploaded: 600,
            duration_ms: 30_000,
            ..UsageCounts::default()
        };
        assert_eq!(
            read(UsageConsumerKind::Torrent),
            vec![(String::new(), seeded)]
        );
        assert_eq!(
            read(UsageConsumerKind::Tracker),
            vec![("t.test".to_string(), seeded)]
        );
    };
    check();
    db.write_usage_flush(&UsageFlush {
        rows: Vec::new(),
        fold_before: Some(retention_cutoff(today)),
        expire_removed_at: None,
    })
    .unwrap();
    // Folding past retention moves the job rows, never their totals.
    check();
    let folded = db
        .read_usage_days(None, today, Some(UsageConsumerKind::Job))
        .unwrap();
    assert!(folded.iter().all(|row| row.day.is_none()));
}

#[test]
fn new_columns_survive_a_flush_and_a_fold() {
    assert_new_columns_survive_flush_and_fold(&Database::open_in_memory().unwrap());
}

fn assert_recovery_and_failure_causes(db: &Database) {
    use crate::history::HistoryFailureKind;
    // Apart from the other history tests' times, which share a Postgres
    // schema with this one.
    for (id, status, protocol, at, available, fetched, cause) in [
        (
            11,
            "complete",
            crate::jobs::Protocol::Nzb,
            101_000,
            100,
            0,
            None,
        ),
        (
            12,
            "partial",
            crate::jobs::Protocol::Nzb,
            101_100,
            50,
            50,
            None,
        ),
        (
            13,
            "failed",
            crate::jobs::Protocol::Nzb,
            101_200,
            10,
            10,
            Some(HistoryFailureKind::Repair),
        ),
        (
            14,
            "failed",
            crate::jobs::Protocol::Nzb,
            101_300,
            0,
            0,
            Some(HistoryFailureKind::Repair),
        ),
        (
            15,
            "failed",
            crate::jobs::Protocol::Nzb,
            101_400,
            0,
            0,
            None,
        ),
        (
            16,
            "cancelled",
            crate::jobs::Protocol::Nzb,
            101_500,
            70,
            70,
            None,
        ),
        (
            17,
            "failed",
            crate::jobs::Protocol::Torrent,
            101_600,
            0,
            0,
            None,
        ),
        (
            18,
            "failed",
            crate::jobs::Protocol::Nzb,
            105_000,
            1,
            1,
            None,
        ),
    ] {
        db.insert_job_history(&crate::JobHistoryRow {
            protocol,
            job_id: id,
            job_hash: None,
            name: format!("job-{id}"),
            status: status.to_string(),
            error_message: None,
            total_bytes: 0,
            downloaded_bytes: 0,
            optional_recovery_bytes: available,
            optional_recovery_downloaded_bytes: fetched,
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
        if cause.is_some() {
            assert!(db.set_history_failure_kind(id, cause).unwrap());
        }
    }
    assert_eq!(
        db.sum_recovery_bytes(Some(101_000), 102_000).unwrap(),
        RecoveryBytes {
            available: 160,
            fetched: 60,
        }
    );
    assert_eq!(
        db.count_failures_by_cause(Some(101_000), 102_000).unwrap(),
        vec![(String::new(), 1), ("repair".to_string(), 2)]
    );
    assert_eq!(
        db.count_failures_by_cause(Some(100_000), 110_000).unwrap(),
        vec![(String::new(), 2), ("repair".to_string(), 2)]
    );
}

#[test]
fn recovery_and_failure_causes_read_from_history() {
    assert_recovery_and_failure_causes(&Database::open_in_memory().unwrap());
}

// ---------------------------------------------------------------------------
// Removed servers
// ---------------------------------------------------------------------------

fn configured_server(
    id: u32,
    host: &str,
    enabled: bool,
    period: crate::servers::ServerDownloadQuotaPeriod,
) -> crate::servers::ServerConfig {
    crate::servers::ServerConfig {
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
        download_quota: crate::servers::ServerDownloadQuotaConfig {
            enabled,
            limit_bytes: if enabled { 1_000_000 } else { 0 },
            period,
            monthly_reset_day: 1,
            ..crate::servers::ServerDownloadQuotaConfig::default()
        },
        tls_ca_cert: None,
    }
}

fn local(y: i32, m: u32, d: u32, h: u32) -> chrono::DateTime<Local> {
    Local.with_ymd_and_hms(y, m, d, h, 0, 0).earliest().unwrap()
}

fn expire_at(now: i64) -> UsageFlush {
    UsageFlush {
        expire_removed_at: Some(now),
        ..UsageFlush::default()
    }
}

fn server_totals(db: &Database) -> Vec<(String, u64)> {
    db.read_usage_totals(Some(UsageConsumerKind::Server))
        .unwrap()
        .into_iter()
        .map(|total| (total.consumer_id, total.counts.bytes))
        .collect()
}

fn seed_servers(db: &Database, day: NaiveDate) {
    db.write_usage_flush(&UsageFlush {
        rows: vec![
            row(day, UsageConsumerKind::Server, "3", 10),
            row(day, UsageConsumerKind::Server, "4", 20),
        ],
        ..UsageFlush::default()
    })
    .unwrap();
}

#[test]
fn a_removed_server_with_a_monthly_reset_keeps_its_rows_until_the_reset() {
    assert_removed_server_kept_until_reset(&Database::open_in_memory().unwrap());
}

fn assert_removed_server_kept_until_reset(db: &Database) {
    use crate::servers::ServerDownloadQuotaPeriod::Monthly;
    db.insert_server(&configured_server(3, "news.eweka.nl", true, Monthly))
        .unwrap();
    seed_servers(db, date(2026, 11, 2));
    // Only the servers seeded here; a shared database may hold others.
    let server_totals = |db: &Database| -> Vec<(String, u64)> {
        server_totals(db)
            .into_iter()
            .filter(|(id, _)| id == "3" || id == "4" || id.starts_with(REMOVED_SERVER_ID_PREFIX))
            .collect()
    };

    let removal = db.delete_server_at(3, local(2026, 11, 9, 12)).unwrap();
    assert!(removal.removed);
    let kept = removal.kept_usage.expect("a monthly quota keeps the rows");
    assert!(kept.starts_with(REMOVED_SERVER_ID_PREFIX));
    assert_eq!(
        server_totals(db),
        vec![("4".to_string(), 20), (kept.clone(), 10)]
    );
    let reset = local(2026, 12, 1, 0).timestamp();
    assert_eq!(
        db.read_removed_servers().unwrap(),
        vec![RemovedServerUsage {
            consumer_id: kept.clone(),
            server_id: 3,
            host: "news.eweka.nl".to_string(),
            remove_at_epoch_sec: reset,
        }]
    );

    // Before the reset nothing goes; at it, the rows and their record go.
    db.write_usage_flush(&expire_at(reset - 1)).unwrap();
    assert_eq!(server_totals(db).len(), 2);
    db.write_usage_flush(&expire_at(reset)).unwrap();
    assert_eq!(server_totals(db), vec![("4".to_string(), 20)]);
    assert!(db.read_removed_servers().unwrap().is_empty());
}

#[test]
fn a_removed_server_with_no_reset_loses_its_rows_at_once() {
    use crate::servers::ServerDownloadQuotaPeriod::{Monthly, OneTime};
    // A one-time quota, and a monthly period with the quota off.
    for server in [
        configured_server(3, "one-time.example", true, OneTime),
        configured_server(3, "no-quota.example", false, Monthly),
    ] {
        let db = Database::open_in_memory().unwrap();
        db.insert_server(&server).unwrap();
        seed_servers(&db, date(2026, 11, 2));
        let removal = db.delete_server_at(3, local(2026, 11, 9, 12)).unwrap();
        assert!(removal.removed);
        assert_eq!(removal.kept_usage, None, "{}", server.host);
        assert_eq!(server_totals(&db), vec![("4".to_string(), 20)]);
        assert!(db.read_removed_servers().unwrap().is_empty());
    }
}

#[test]
fn a_new_server_given_the_id_starts_from_zero() {
    use crate::servers::ServerDownloadQuotaPeriod::Monthly;
    let db = Database::open_in_memory().unwrap();
    db.insert_server(&configured_server(3, "news.eweka.nl", true, Monthly))
        .unwrap();
    seed_servers(&db, date(2026, 11, 2));
    let kept = db
        .delete_server_at(3, local(2026, 11, 9, 12))
        .unwrap()
        .kept_usage
        .unwrap();

    db.insert_server(&configured_server(3, "news.other.example", true, Monthly))
        .unwrap();
    db.write_usage_flush(&UsageFlush {
        rows: vec![row(date(2026, 11, 10), UsageConsumerKind::Server, "3", 5)],
        ..UsageFlush::default()
    })
    .unwrap();
    assert_eq!(
        server_totals(&db),
        vec![
            ("3".to_string(), 5),
            ("4".to_string(), 20),
            (kept.clone(), 10)
        ]
    );

    // Removing the new one too keeps both apart, each under its own id.
    let again = db
        .delete_server_at(3, local(2026, 11, 9, 12))
        .unwrap()
        .kept_usage
        .unwrap();
    assert_ne!(again, kept);
    assert_eq!(
        server_totals(&db),
        vec![
            ("4".to_string(), 20),
            (kept.clone(), 10),
            (again.clone(), 5)
        ]
    );
}

#[test]
fn what_the_recorder_holds_of_a_removed_server_follows_its_rows() {
    let today = date(2026, 11, 9);
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![server(3, 1_000, 0, 0)], &[]), 0);
    recorder.observe(&reading(vec![server(3, 1_400, 4, 4)], &[]), today, 5);

    recorder.retire_server(3, Some("removed-3-1"));
    // A reading taken before the pool let the server go still shows it.
    recorder.observe(&reading(vec![server(3, 1_500, 5, 5)], &[]), today, 10);
    let rows = recorder.take(today).rows;
    assert_eq!(
        rows,
        vec![counted(
            UsageConsumerKind::Server,
            "removed-3-1",
            UsageCounts {
                bytes: 500,
                articles_tried: 5,
                articles_succeeded: 5,
                ..UsageCounts::default()
            },
            today,
        )]
    );

    // A new server given the id counts as itself from its own totals.
    recorder.reuse_server_id(3);
    recorder.observe(&reading(vec![server(3, 0, 0, 0)], &[]), today, 15);
    recorder.observe(&reading(vec![server(3, 60, 2, 1)], &[]), today, 20);
    assert_eq!(
        recorder.take(today).rows,
        vec![counted(
            UsageConsumerKind::Server,
            "3",
            UsageCounts {
                bytes: 60,
                articles_tried: 2,
                articles_succeeded: 1,
                ..UsageCounts::default()
            },
            today,
        )]
    );
}

// A new server on a reused id counts what it moved by its first reading:
// totals below the old server's are its own, from zero, and totals that
// carry on from the old server's count only what moved past them.
#[test]
fn a_new_server_on_a_reused_id_counts_its_first_reading() {
    let today = date(2026, 11, 9);
    let first = |old: ServerUsageReading, new: ServerUsageReading| {
        let recorder = UsageRecorder::new();
        recorder.prime(&reading(vec![server(3, 1_000, 0, 0)], &[]), 0);
        recorder.observe(&reading(vec![old], &[]), today, 5);
        recorder.retire_server(3, Some("removed-3-1"));
        let _ = recorder.take(today);
        recorder.reuse_server_id(3);
        recorder.observe(&reading(vec![new], &[]), today, 10);
        recorder.take(today).rows
    };
    let as_new = |bytes, tried, ok| {
        vec![counted(
            UsageConsumerKind::Server,
            "3",
            UsageCounts {
                bytes,
                articles_tried: tried,
                articles_succeeded: ok,
                ..UsageCounts::default()
            },
            today,
        )]
    };
    // Its own totals, started from zero.
    assert_eq!(
        first(server(3, 1_500, 5, 5), server(3, 60, 2, 1)),
        as_new(60, 2, 1)
    );
    // Totals that carry on from the old server's.
    assert_eq!(
        first(server(3, 1_500, 5, 5), server(3, 1_560, 7, 6)),
        as_new(60, 2, 1)
    );
    // A server the recorder never saw before the id was reused.
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![], &[]), 0);
    recorder.reuse_server_id(3);
    recorder.observe(&reading(vec![server(3, 60, 2, 1)], &[]), today, 5);
    assert_eq!(recorder.take(today).rows, as_new(60, 2, 1));
    // Bytes a reading does not show yet are judged when one does.
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![server(3, 1_000, 0, 0)], &[]), 0);
    recorder.reuse_server_id(3);
    let no_bytes = ServerUsageReading {
        bytes: None,
        ..server(3, 0, 2, 1)
    };
    recorder.observe(&reading(vec![no_bytes], &[]), today, 5);
    recorder.observe(&reading(vec![server(3, 60, 2, 1)], &[]), today, 10);
    assert_eq!(recorder.take(today).rows, as_new(60, 2, 1));
}

#[test]
fn what_the_recorder_holds_of_a_server_whose_rows_went_is_dropped() {
    let today = date(2026, 11, 9);
    let recorder = UsageRecorder::new();
    recorder.prime(
        &reading(vec![server(3, 0, 0, 0), server(4, 0, 0, 0)], &[]),
        0,
    );
    recorder.observe(
        &reading(vec![server(3, 100, 0, 0), server(4, 10, 0, 0)], &[]),
        today,
        5,
    );
    let failed = recorder.take(today);
    recorder.retire_server(3, None);
    // A batch that failed before the removal comes back without it.
    recorder.restore(failed);
    recorder.observe(
        &reading(vec![server(3, 150, 0, 0), server(4, 10, 0, 0)], &[]),
        today,
        10,
    );
    assert_eq!(
        recorder.take(today).rows,
        vec![row(today, UsageConsumerKind::Server, "4", 10)]
    );
}

#[test]
fn a_restored_batch_of_a_kept_server_goes_to_its_kept_rows() {
    let today = date(2026, 11, 9);
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![server(3, 0, 0, 0)], &[]), 0);
    recorder.observe(&reading(vec![server(3, 100, 0, 0)], &[]), today, 5);
    let failed = recorder.take(today);
    recorder.retire_server(3, Some("removed-3-1"));
    recorder.restore(failed);
    assert_eq!(
        recorder.take(today).rows,
        vec![row(today, UsageConsumerKind::Server, "removed-3-1", 100)]
    );
}

#[test]
fn expiry_is_asked_for_about_once_a_minute_of_readings() {
    let today = date(2026, 11, 9);
    let recorder = UsageRecorder::new();
    // No reading yet, so no time to expire against.
    assert_eq!(recorder.take(today).expire_removed_at, None);
    recorder.prime(&reading(vec![], &[]), 1_000);
    assert_eq!(recorder.take(today).expire_removed_at, Some(1_000));
    recorder.observe(&reading(vec![], &[]), today, 1_030);
    assert_eq!(recorder.take(today).expire_removed_at, None);
    recorder.observe(&reading(vec![], &[]), today, 1_060);
    let due = recorder.take(today);
    assert_eq!(due.expire_removed_at, Some(1_060));
    // A failed write asks again with the next batch.
    recorder.restore(due);
    recorder.observe(&reading(vec![], &[]), today, 1_065);
    assert_eq!(recorder.take(today).expire_removed_at, Some(1_065));
}

#[test]
fn kept_rows_and_their_removal_survive_a_restart() {
    use crate::servers::ServerDownloadQuotaPeriod::Daily;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("weaver.db");
    let today = date(2026, 11, 9);
    let removed_at = local(2026, 11, 9, 12);
    let kept = {
        let db = Database::open(&path).unwrap();
        db.insert_server(&configured_server(3, "news.eweka.nl", true, Daily))
            .unwrap();
        let recorder = UsageRecorder::new();
        recorder.prime(&reading(vec![server(3, 0, 0, 0)], &[]), 0);
        recorder.observe(&reading(vec![server(3, 300, 0, 0)], &[]), today, 5);
        let removal = db.delete_server_at(3, removed_at).unwrap();
        let kept = removal.kept_usage.unwrap();
        recorder.retire_server(3, Some(&kept));
        db.write_usage_flush(&rows_only(recorder.take(today)))
            .unwrap();
        kept
    };
    let db = Database::open(&path).unwrap();
    assert_eq!(server_totals(&db), vec![(kept.clone(), 300)]);
    let reset = local(2026, 11, 10, 0).timestamp();
    assert_eq!(
        db.read_removed_servers().unwrap()[0].remove_at_epoch_sec,
        reset
    );
    // A new process asks for expiry with its first batch.
    let recorder = UsageRecorder::new();
    recorder.prime(&reading(vec![], &[]), reset);
    db.write_usage_flush(&recorder.take(date(2026, 11, 10)))
        .unwrap();
    assert!(server_totals(&db).is_empty());
    assert!(db.read_removed_servers().unwrap().is_empty());
}
