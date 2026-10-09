use super::*;

fn quota(period: ServerDownloadQuotaPeriod) -> ServerDownloadQuotaConfig {
    ServerDownloadQuotaConfig {
        enabled: true,
        period,
        limit_bytes: 1_000,
        reset_time_minutes_local: 8 * 60,
        weekly_reset_weekday: QuotaWeekday::Mon,
        monthly_reset_day: 31,
    }
}

#[test]
fn one_time_quota_has_no_window() {
    let now = local_datetime(2026, 3, 12, 7 * 60 + 30);
    assert!(compute_window(now, &quota(ServerDownloadQuotaPeriod::OneTime)).is_none());
}

#[test]
fn daily_window_rolls_back_before_anchor() {
    let now = local_datetime(2026, 3, 12, 7 * 60 + 30);
    let window = compute_window(now, &quota(ServerDownloadQuotaPeriod::Daily)).unwrap();
    assert_eq!(
        window.starts_at().date_naive(),
        NaiveDate::from_ymd_opt(2026, 3, 11).unwrap()
    );
    assert_eq!(
        window.ends_at().date_naive(),
        NaiveDate::from_ymd_opt(2026, 3, 12).unwrap()
    );
}

#[test]
fn weekly_window_uses_anchor_weekday() {
    let now = local_datetime(2026, 3, 12, 10 * 60);
    let window = compute_window(now, &quota(ServerDownloadQuotaPeriod::Weekly)).unwrap();
    assert_eq!(window.starts_at().weekday(), chrono::Weekday::Mon);
    assert_eq!(window.ends_at().weekday(), chrono::Weekday::Mon);
    assert_eq!((window.ends_at() - window.starts_at()).num_days(), 7);
}

#[test]
fn monthly_window_clamps_short_months_before_anchor() {
    let now = local_datetime(2026, 2, 28, 7 * 60 + 30);
    let window = compute_window(now, &quota(ServerDownloadQuotaPeriod::Monthly)).unwrap();
    assert_eq!(
        window.starts_at().date_naive(),
        NaiveDate::from_ymd_opt(2026, 1, 31).unwrap()
    );
    assert_eq!(
        window.ends_at().date_naive(),
        NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()
    );
}

#[test]
fn monthly_window_rolls_forward_after_clamped_anchor() {
    let now = local_datetime(2026, 2, 28, 12 * 60);
    let window = compute_window(now, &quota(ServerDownloadQuotaPeriod::Monthly)).unwrap();
    assert_eq!(
        window.starts_at().date_naive(),
        NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()
    );
    assert_eq!(
        window.ends_at().date_naive(),
        NaiveDate::from_ymd_opt(2026, 3, 31).unwrap()
    );
}

#[test]
fn bandwidth_ledger_flushes_lazily() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthCapRuntime::default();

    let now_minute = crate::e2e_clock::unix_seconds().div_euclid(60);
    runtime.record_download_bytes(&db, 512).unwrap();
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        0
    );

    runtime.flush_pending_usage(&db).unwrap();
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        512
    );
}

#[test]
fn bandwidth_ledger_flushes_once_the_byte_threshold_is_reached() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthCapRuntime::default();

    let now_minute = crate::e2e_clock::unix_seconds().div_euclid(60);
    runtime
        .record_download_bytes(&db, BANDWIDTH_USAGE_FLUSH_BYTES - 1)
        .unwrap();
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        0
    );
    runtime.record_download_bytes(&db, 1).unwrap();
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        BANDWIDTH_USAGE_FLUSH_BYTES
    );
}

#[test]
fn global_pause_origin_selects_the_block_kind() {
    let runtime = BandwidthCapRuntime::default();

    // A schedule-driven pause must present as Scheduled even though it shares
    // the `global_paused` flag with a manual pause; a concurrent recomputation
    // must not collapse it to ManualPause.
    assert_eq!(
        runtime.to_download_block_state(GlobalPause::Running).kind,
        crate::DownloadBlockKind::None
    );
    assert_eq!(
        runtime.to_download_block_state(GlobalPause::Manual).kind,
        crate::DownloadBlockKind::ManualPause
    );
    assert_eq!(
        runtime.to_download_block_state(GlobalPause::Scheduled).kind,
        crate::DownloadBlockKind::Scheduled
    );
}

#[test]
fn quota_metering_excludes_unmetered_ledger_rows_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quota.db");
    let db = crate::Database::open(&path).unwrap();
    let mut runtime = BandwidthCapRuntime::default();
    runtime.record_pending_usage(100, 200);
    runtime.set_metering_enabled(false);
    runtime.record_pending_usage(100, 700);
    runtime.set_metering_enabled(true);
    runtime.record_pending_usage(100, 100);
    runtime.flush_pending_usage(&db).unwrap();
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
