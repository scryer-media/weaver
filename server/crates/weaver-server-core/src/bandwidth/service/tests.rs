use super::*;

fn cap(period: IspBandwidthCapPeriod) -> IspBandwidthCapConfig {
    IspBandwidthCapConfig {
        enabled: true,
        period,
        limit_bytes: 1_000,
        reset_time_minutes_local: 8 * 60,
        weekly_reset_weekday: IspBandwidthCapWeekday::Mon,
        monthly_reset_day: 31,
    }
}

#[test]
fn daily_window_rolls_back_before_anchor() {
    let now = local_datetime(2026, 3, 12, 7 * 60 + 30);
    let window = compute_window(now, &cap(IspBandwidthCapPeriod::Daily));
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
    let window = compute_window(now, &cap(IspBandwidthCapPeriod::Weekly));
    assert_eq!(window.starts_at().weekday(), chrono::Weekday::Mon);
    assert_eq!(window.ends_at().weekday(), chrono::Weekday::Mon);
    assert_eq!((window.ends_at() - window.starts_at()).num_days(), 7);
}

#[test]
fn monthly_window_clamps_short_months_before_anchor() {
    let now = local_datetime(2026, 2, 28, 7 * 60 + 30);
    let window = compute_window(now, &cap(IspBandwidthCapPeriod::Monthly));
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
    let window = compute_window(now, &cap(IspBandwidthCapPeriod::Monthly));
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
fn download_usage_is_counted_immediately_but_ledger_flushes_lazily() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthCapRuntime::default();
    runtime.update_for_now(&db).unwrap();

    let now_minute = Local::now().timestamp().div_euclid(60);
    runtime.record_download_bytes(&db, 512).unwrap();

    assert_eq!(runtime.used_bytes, 512);
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
fn display_only_bandwidth_usage_uses_coarser_flush_threshold() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthCapRuntime::default();
    runtime.update_for_now(&db).unwrap();

    let now_minute = Local::now().timestamp().div_euclid(60);
    runtime
        .record_download_bytes(&db, BANDWIDTH_CAP_USAGE_FLUSH_BYTES)
        .unwrap();

    assert_eq!(runtime.used_bytes, BANDWIDTH_CAP_USAGE_FLUSH_BYTES);
    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        0
    );
}

#[test]
fn enabled_cap_keeps_conservative_bandwidth_flush_threshold() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthCapRuntime::default();
    runtime.set_policy(Some(cap(IspBandwidthCapPeriod::Monthly)));
    runtime.update_for_now(&db).unwrap();

    let now_minute = Local::now().timestamp().div_euclid(60);
    runtime
        .record_download_bytes(&db, BANDWIDTH_CAP_USAGE_FLUSH_BYTES)
        .unwrap();

    assert_eq!(
        db.sum_bandwidth_usage_minutes(now_minute, now_minute + 1)
            .unwrap(),
        BANDWIDTH_CAP_USAGE_FLUSH_BYTES
    );
}

#[test]
fn parked_dispatch_presents_a_sticky_isp_cap_block_below_the_limit() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthCapRuntime::default();
    runtime.set_policy(Some(cap(IspBandwidthCapPeriod::Daily)));
    runtime.update_for_now(&db).unwrap();

    // Consume most of the allowance without reaching the limit.
    runtime.reserve(700);
    runtime.release(700);
    runtime.record_download_bytes(&db, 700).unwrap();
    assert!(runtime.remaining_bytes() > 0);
    assert_eq!(
        runtime.to_download_block_state(GlobalPause::Running).kind,
        crate::DownloadBlockKind::None
    );

    // The next article no longer fits: conservative pre-reservation parks the
    // work while `used` is still below the limit. The presentation must say
    // IspCap and stay there across later block-state recomputation.
    assert!(!runtime.can_reserve(400));
    runtime.record_reservation_rejected();
    assert_eq!(
        runtime.to_download_block_state(GlobalPause::Running).kind,
        crate::DownloadBlockKind::IspCap
    );
    assert_eq!(
        runtime.to_download_block_state(GlobalPause::Running).kind,
        crate::DownloadBlockKind::IspCap
    );

    // A successful reservation clears the parked presentation.
    assert!(runtime.can_reserve(100));
    runtime.reserve(100);
    assert_eq!(
        runtime.to_download_block_state(GlobalPause::Running).kind,
        crate::DownloadBlockKind::None
    );

    // A parked runtime recovers when the window rolls over.
    runtime.record_reservation_rejected();
    runtime.set_policy(Some(cap(IspBandwidthCapPeriod::Daily)));
    runtime.update_for_now(&db).unwrap();
    assert_eq!(
        runtime.to_download_block_state(GlobalPause::Running).kind,
        crate::DownloadBlockKind::None
    );
}

#[test]
fn global_pause_origin_selects_the_block_kind() {
    let db = crate::Database::open_in_memory().unwrap();
    let mut runtime = BandwidthCapRuntime::default();
    runtime.update_for_now(&db).unwrap();

    // With no ISP cap engaged, the reported kind is driven purely by the
    // origin of the global pause. A schedule-driven pause must present as
    // Scheduled even though it shares the `global_paused` flag with a manual
    // pause; a concurrent recomputation must not collapse it to ManualPause.
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
