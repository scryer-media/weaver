// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use chrono::{
    DateTime, Datelike, Duration, Local, LocalResult, NaiveDate, NaiveDateTime, TimeZone,
};

use crate::bandwidth::QuotaWeekday;
use crate::servers::{ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod};
use std::range::Range;

// One quota period, in local time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaWindow {
    period: Range<DateTime<Local>>,
}

impl QuotaWindow {
    fn new(start: DateTime<Local>, end: DateTime<Local>) -> Self {
        Self {
            period: Range { start, end },
        }
    }

    pub fn starts_at(&self) -> DateTime<Local> {
        self.period.start
    }

    pub fn ends_at(&self) -> DateTime<Local> {
        self.period.end
    }
}

fn month_days(year: i32, month: u32) -> u32 {
    let first_of_month = NaiveDate::from_ymd_opt(year, month, 1).expect("valid month start");
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_of_next =
        NaiveDate::from_ymd_opt(next_year, next_month, 1).expect("valid next month start");
    (first_of_next - first_of_month).num_days() as u32
}

fn shift_month(year: i32, month: u32, delta: i32) -> (i32, u32) {
    let total_months = year * 12 + month as i32 - 1 + delta;
    let shifted_year = total_months.div_euclid(12);
    let shifted_month = (total_months.rem_euclid(12) + 1) as u32;
    (shifted_year, shifted_month)
}

fn clamp_day(year: i32, month: u32, day: u8) -> u32 {
    month_days(year, month).min(day as u32).max(1)
}

fn local_datetime(year: i32, month: u32, day: u32, minutes_local: u16) -> DateTime<Local> {
    let hour = u32::from(minutes_local / 60);
    let minute = u32::from(minutes_local % 60);
    let naive = NaiveDate::from_ymd_opt(year, month, day)
        .expect("valid local date")
        .and_hms_opt(hour, minute, 0)
        .expect("valid local time");
    resolve_local_datetime(naive)
}

fn resolve_local_datetime(mut naive: NaiveDateTime) -> DateTime<Local> {
    for _ in 0..180 {
        match Local.from_local_datetime(&naive) {
            LocalResult::Single(value) => return value,
            LocalResult::Ambiguous(first, _) => return first,
            LocalResult::None => {
                naive += Duration::minutes(1);
            }
        }
    }
    match Local.from_local_datetime(&naive) {
        LocalResult::Single(value) => value,
        LocalResult::Ambiguous(first, _) => first,
        LocalResult::None => Local
            .timestamp_opt(naive.and_utc().timestamp(), 0)
            .earliest()
            .expect("fallback local timestamp"),
    }
}

fn weekday_to_chrono(weekday: QuotaWeekday) -> chrono::Weekday {
    match weekday {
        QuotaWeekday::Mon => chrono::Weekday::Mon,
        QuotaWeekday::Tue => chrono::Weekday::Tue,
        QuotaWeekday::Wed => chrono::Weekday::Wed,
        QuotaWeekday::Thu => chrono::Weekday::Thu,
        QuotaWeekday::Fri => chrono::Weekday::Fri,
        QuotaWeekday::Sat => chrono::Weekday::Sat,
        QuotaWeekday::Sun => chrono::Weekday::Sun,
    }
}

fn compute_daily_window(now: DateTime<Local>, reset_minutes: u16) -> QuotaWindow {
    let today = now.date_naive();
    let today_anchor = local_datetime(today.year(), today.month(), today.day(), reset_minutes);
    let start = if now >= today_anchor {
        today_anchor
    } else {
        let previous = today
            .pred_opt()
            .expect("previous day exists for a daily quota window");
        local_datetime(
            previous.year(),
            previous.month(),
            previous.day(),
            reset_minutes,
        )
    };
    let next_day = start
        .date_naive()
        .succ_opt()
        .expect("next day exists for a daily quota window");
    let end = local_datetime(
        next_day.year(),
        next_day.month(),
        next_day.day(),
        reset_minutes,
    );
    QuotaWindow::new(start, end)
}

fn compute_weekly_window(
    now: DateTime<Local>,
    reset_weekday: QuotaWeekday,
    reset_minutes: u16,
) -> QuotaWindow {
    let now_date = now.date_naive();
    let target = weekday_to_chrono(reset_weekday).num_days_from_monday() as i64;
    let current = now.weekday().num_days_from_monday() as i64;
    let mut start_date = now_date - Duration::days((current - target).rem_euclid(7));
    let mut start = local_datetime(
        start_date.year(),
        start_date.month(),
        start_date.day(),
        reset_minutes,
    );
    if now < start {
        start_date -= Duration::days(7);
        start = local_datetime(
            start_date.year(),
            start_date.month(),
            start_date.day(),
            reset_minutes,
        );
    }
    let end_date = start_date + Duration::days(7);
    let end = local_datetime(
        end_date.year(),
        end_date.month(),
        end_date.day(),
        reset_minutes,
    );
    QuotaWindow::new(start, end)
}

fn compute_monthly_window(now: DateTime<Local>, reset_day: u8, reset_minutes: u16) -> QuotaWindow {
    let today = now.date_naive();
    let current_day = clamp_day(today.year(), today.month(), reset_day);
    let current_anchor = local_datetime(today.year(), today.month(), current_day, reset_minutes);
    let (start_year, start_month) = if now >= current_anchor {
        (today.year(), today.month())
    } else {
        shift_month(today.year(), today.month(), -1)
    };
    let start_day = clamp_day(start_year, start_month, reset_day);
    let start = local_datetime(start_year, start_month, start_day, reset_minutes);

    let (end_year, end_month) = shift_month(start_year, start_month, 1);
    let end_day = clamp_day(end_year, end_month, reset_day);
    let end = local_datetime(end_year, end_month, end_day, reset_minutes);
    QuotaWindow::new(start, end)
}

// When a quota's periods begin and end, whatever holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaCalendar {
    pub period: ServerDownloadQuotaPeriod,
    pub reset_time_minutes_local: u16,
    pub weekly_reset_weekday: QuotaWeekday,
    pub monthly_reset_day: u8,
}

impl From<&ServerDownloadQuotaConfig> for QuotaCalendar {
    fn from(quota: &ServerDownloadQuotaConfig) -> Self {
        Self {
            period: quota.period,
            reset_time_minutes_local: quota.reset_time_minutes_local,
            weekly_reset_weekday: quota.weekly_reset_weekday,
            monthly_reset_day: quota.monthly_reset_day,
        }
    }
}

// The quota period containing `now`, or `None` for a one-time quota.
pub fn compute_window(now: DateTime<Local>, quota: QuotaCalendar) -> Option<QuotaWindow> {
    Some(match quota.period {
        ServerDownloadQuotaPeriod::OneTime => return None,
        ServerDownloadQuotaPeriod::Daily => {
            compute_daily_window(now, quota.reset_time_minutes_local)
        }
        ServerDownloadQuotaPeriod::Weekly => compute_weekly_window(
            now,
            quota.weekly_reset_weekday,
            quota.reset_time_minutes_local,
        ),
        ServerDownloadQuotaPeriod::Monthly => {
            compute_monthly_window(now, quota.monthly_reset_day, quota.reset_time_minutes_local)
        }
    })
}

#[cfg(test)]
mod tests {
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
        assert!(compute_window(now, (&quota(ServerDownloadQuotaPeriod::OneTime)).into()).is_none());
    }

    #[test]
    fn daily_window_rolls_back_before_anchor() {
        let now = local_datetime(2026, 3, 12, 7 * 60 + 30);
        let window =
            compute_window(now, (&quota(ServerDownloadQuotaPeriod::Daily)).into()).unwrap();
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
        let window =
            compute_window(now, (&quota(ServerDownloadQuotaPeriod::Weekly)).into()).unwrap();
        assert_eq!(window.starts_at().weekday(), chrono::Weekday::Mon);
        assert_eq!(window.ends_at().weekday(), chrono::Weekday::Mon);
        assert_eq!((window.ends_at() - window.starts_at()).num_days(), 7);
    }

    #[test]
    fn monthly_window_clamps_short_months_before_anchor() {
        let now = local_datetime(2026, 2, 28, 7 * 60 + 30);
        let window =
            compute_window(now, (&quota(ServerDownloadQuotaPeriod::Monthly)).into()).unwrap();
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
        let window =
            compute_window(now, (&quota(ServerDownloadQuotaPeriod::Monthly)).into()).unwrap();
        assert_eq!(
            window.starts_at().date_naive(),
            NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()
        );
        assert_eq!(
            window.ends_at().date_naive(),
            NaiveDate::from_ymd_opt(2026, 3, 31).unwrap()
        );
    }
}
