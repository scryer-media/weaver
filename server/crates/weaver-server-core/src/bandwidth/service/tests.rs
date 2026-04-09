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
        window.starts_at.date_naive(),
        NaiveDate::from_ymd_opt(2026, 3, 11).unwrap()
    );
    assert_eq!(
        window.ends_at.date_naive(),
        NaiveDate::from_ymd_opt(2026, 3, 12).unwrap()
    );
}

#[test]
fn weekly_window_uses_anchor_weekday() {
    let now = local_datetime(2026, 3, 12, 10 * 60);
    let window = compute_window(now, &cap(IspBandwidthCapPeriod::Weekly));
    assert_eq!(window.starts_at.weekday(), chrono::Weekday::Mon);
    assert_eq!(window.ends_at.weekday(), chrono::Weekday::Mon);
    assert_eq!((window.ends_at - window.starts_at).num_days(), 7);
}

#[test]
fn monthly_window_clamps_short_months_before_anchor() {
    let now = local_datetime(2026, 2, 28, 7 * 60 + 30);
    let window = compute_window(now, &cap(IspBandwidthCapPeriod::Monthly));
    assert_eq!(
        window.starts_at.date_naive(),
        NaiveDate::from_ymd_opt(2026, 1, 31).unwrap()
    );
    assert_eq!(
        window.ends_at.date_naive(),
        NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()
    );
}

#[test]
fn monthly_window_rolls_forward_after_clamped_anchor() {
    let now = local_datetime(2026, 2, 28, 12 * 60);
    let window = compute_window(now, &cap(IspBandwidthCapPeriod::Monthly));
    assert_eq!(
        window.starts_at.date_naive(),
        NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()
    );
    assert_eq!(
        window.ends_at.date_naive(),
        NaiveDate::from_ymd_opt(2026, 3, 31).unwrap()
    );
}
