use chrono::{
    DateTime, Datelike, Duration, Local, LocalResult, NaiveDate, NaiveDateTime, TimeZone,
};

use weaver_core::config::{IspBandwidthCapConfig, IspBandwidthCapPeriod, IspBandwidthCapWeekday};
use weaver_scheduler::SchedulerError;
use weaver_scheduler::handle::{DownloadBlockKind, DownloadBlockState};

use super::Pipeline;

const BANDWIDTH_LEDGER_RETENTION_DAYS: i64 = 90;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BandwidthCapWindow {
    pub(super) starts_at: DateTime<Local>,
    pub(super) ends_at: DateTime<Local>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct BandwidthCapRuntime {
    pub(super) policy: Option<IspBandwidthCapConfig>,
    pub(super) window: Option<BandwidthCapWindow>,
    pub(super) used_bytes: u64,
    pub(super) reserved_bytes: u64,
    pub(super) last_pruned_bucket_epoch_minute: Option<i64>,
}

impl BandwidthCapRuntime {
    pub(super) fn set_policy(&mut self, policy: Option<IspBandwidthCapConfig>) {
        self.policy = policy;
        self.window = None;
        self.used_bytes = 0;
        self.reserved_bytes = 0;
    }

    pub(super) fn cap_enabled(&self) -> bool {
        self.policy.as_ref().is_some_and(|policy| policy.enabled)
    }

    pub(super) fn limit_bytes(&self) -> u64 {
        self.policy.as_ref().map_or(0, |policy| policy.limit_bytes)
    }

    pub(super) fn remaining_bytes(&self) -> u64 {
        self.limit_bytes()
            .saturating_sub(self.used_bytes.saturating_add(self.reserved_bytes))
    }

    /// Default policy used solely for computing a display window when no
    /// real policy is configured.  Monthly from day 1 at midnight matches
    /// the frontend form defaults.
    fn default_display_policy() -> IspBandwidthCapConfig {
        IspBandwidthCapConfig {
            enabled: false,
            period: IspBandwidthCapPeriod::Monthly,
            limit_bytes: 0,
            reset_time_minutes_local: 0,
            weekly_reset_weekday: IspBandwidthCapWeekday::Mon,
            monthly_reset_day: 1,
        }
    }

    pub(super) fn update_for_now(
        &mut self,
        db: &weaver_state::Database,
    ) -> Result<(), weaver_state::StateError> {
        // Always compute a window so the UI can show real usage even before
        // the user configures a cap.  Fall back to a default monthly window.
        let default_policy = Self::default_display_policy();
        let policy = self.policy.as_ref().unwrap_or(&default_policy);

        let now = Local::now();
        let next_window = compute_window(now, policy);
        let should_reload = self.window.as_ref() != Some(&next_window);
        if should_reload {
            let start_minute = next_window.starts_at.timestamp().div_euclid(60);
            let end_minute = next_window.ends_at.timestamp().div_euclid(60);
            self.used_bytes = db.sum_bandwidth_usage_minutes(start_minute, end_minute)?;
            self.window = Some(next_window);
            self.reserved_bytes = 0;
        }

        let prune_cutoff =
            now.timestamp().div_euclid(60) - BANDWIDTH_LEDGER_RETENTION_DAYS * 24 * 60;
        if self.last_pruned_bucket_epoch_minute != Some(prune_cutoff) {
            db.prune_bandwidth_usage_before(prune_cutoff)?;
            self.last_pruned_bucket_epoch_minute = Some(prune_cutoff);
        }

        Ok(())
    }

    pub(super) fn can_reserve(&self, bytes: u64) -> bool {
        if !self.cap_enabled() {
            return true;
        }
        self.used_bytes
            .saturating_add(self.reserved_bytes)
            .saturating_add(bytes)
            <= self.limit_bytes()
    }

    pub(super) fn reserve(&mut self, bytes: u64) {
        self.reserved_bytes = self.reserved_bytes.saturating_add(bytes);
    }

    pub(super) fn release(&mut self, bytes: u64) {
        self.reserved_bytes = self.reserved_bytes.saturating_sub(bytes);
    }

    pub(super) fn record_download_bytes(
        &mut self,
        db: &weaver_state::Database,
        payload_bytes: u64,
    ) -> Result<(), weaver_state::StateError> {
        let now = Local::now();
        db.add_bandwidth_usage_minute(now.timestamp().div_euclid(60), payload_bytes)?;
        if let Some(window) = &self.window
            && now >= window.starts_at
            && now < window.ends_at
        {
            self.used_bytes = self.used_bytes.saturating_add(payload_bytes);
        } else {
            self.update_for_now(db)?;
        }
        Ok(())
    }

    pub(super) fn to_download_block_state(&self, global_paused: bool) -> DownloadBlockState {
        let timezone_name = {
            let label = Local::now().format("%Z").to_string();
            if label.is_empty() {
                Local::now().offset().to_string()
            } else {
                label
            }
        };

        let kind = if global_paused {
            DownloadBlockKind::ManualPause
        } else if self.cap_enabled() && self.remaining_bytes() == 0 {
            DownloadBlockKind::IspCap
        } else {
            DownloadBlockKind::None
        };

        DownloadBlockState {
            kind,
            cap_enabled: self.cap_enabled(),
            period: self.policy.as_ref().map(|policy| policy.period),
            used_bytes: self.used_bytes,
            limit_bytes: self.limit_bytes(),
            remaining_bytes: self.remaining_bytes(),
            reserved_bytes: self.reserved_bytes,
            window_starts_at_epoch_ms: self
                .window
                .as_ref()
                .map(|window| window.starts_at.timestamp_millis() as f64),
            window_ends_at_epoch_ms: self
                .window
                .as_ref()
                .map(|window| window.ends_at.timestamp_millis() as f64),
            timezone_name,
        }
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

fn weekday_to_chrono(weekday: IspBandwidthCapWeekday) -> chrono::Weekday {
    match weekday {
        IspBandwidthCapWeekday::Mon => chrono::Weekday::Mon,
        IspBandwidthCapWeekday::Tue => chrono::Weekday::Tue,
        IspBandwidthCapWeekday::Wed => chrono::Weekday::Wed,
        IspBandwidthCapWeekday::Thu => chrono::Weekday::Thu,
        IspBandwidthCapWeekday::Fri => chrono::Weekday::Fri,
        IspBandwidthCapWeekday::Sat => chrono::Weekday::Sat,
        IspBandwidthCapWeekday::Sun => chrono::Weekday::Sun,
    }
}

fn compute_daily_window(now: DateTime<Local>, reset_minutes: u16) -> BandwidthCapWindow {
    let today = now.date_naive();
    let today_anchor = local_datetime(today.year(), today.month(), today.day(), reset_minutes);
    let start = if now >= today_anchor {
        today_anchor
    } else {
        let previous = today
            .pred_opt()
            .expect("previous day exists for daily bandwidth cap");
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
        .expect("next day exists for daily bandwidth cap");
    let end = local_datetime(
        next_day.year(),
        next_day.month(),
        next_day.day(),
        reset_minutes,
    );
    BandwidthCapWindow {
        starts_at: start,
        ends_at: end,
    }
}

fn compute_weekly_window(
    now: DateTime<Local>,
    reset_weekday: IspBandwidthCapWeekday,
    reset_minutes: u16,
) -> BandwidthCapWindow {
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
    BandwidthCapWindow {
        starts_at: start,
        ends_at: end,
    }
}

fn compute_monthly_window(
    now: DateTime<Local>,
    reset_day: u8,
    reset_minutes: u16,
) -> BandwidthCapWindow {
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
    BandwidthCapWindow {
        starts_at: start,
        ends_at: end,
    }
}

fn compute_window(now: DateTime<Local>, policy: &IspBandwidthCapConfig) -> BandwidthCapWindow {
    match policy.period {
        IspBandwidthCapPeriod::Daily => compute_daily_window(now, policy.reset_time_minutes_local),
        IspBandwidthCapPeriod::Weekly => compute_weekly_window(
            now,
            policy.weekly_reset_weekday,
            policy.reset_time_minutes_local,
        ),
        IspBandwidthCapPeriod::Monthly => compute_monthly_window(
            now,
            policy.monthly_reset_day,
            policy.reset_time_minutes_local,
        ),
    }
}

impl Pipeline {
    pub(super) fn refresh_bandwidth_cap_window(&mut self) -> Result<(), SchedulerError> {
        self.bandwidth_cap.update_for_now(&self.db)?;
        self.shared_state.set_download_block(
            self.bandwidth_cap
                .to_download_block_state(self.global_paused),
        );
        Ok(())
    }

    pub(super) fn apply_bandwidth_cap_policy(
        &mut self,
        policy: Option<IspBandwidthCapConfig>,
    ) -> Result<(), SchedulerError> {
        self.bandwidth_cap.set_policy(policy);
        self.refresh_bandwidth_cap_window()?;
        Ok(())
    }

    pub(super) fn reserve_bandwidth_for_dispatch(
        &mut self,
        segment_id: weaver_core::id::SegmentId,
        estimate_bytes: u64,
    ) -> Result<bool, SchedulerError> {
        self.refresh_bandwidth_cap_window()?;
        if !self.bandwidth_cap.can_reserve(estimate_bytes) {
            return Ok(false);
        }
        self.bandwidth_cap.reserve(estimate_bytes);
        self.bandwidth_reservations
            .insert(segment_id, estimate_bytes);
        self.shared_state.set_download_block(
            self.bandwidth_cap
                .to_download_block_state(self.global_paused),
        );
        Ok(true)
    }

    pub(super) fn release_bandwidth_reservation(
        &mut self,
        segment_id: weaver_core::id::SegmentId,
    ) -> Result<(), SchedulerError> {
        if let Some(reserved) = self.bandwidth_reservations.remove(&segment_id) {
            self.bandwidth_cap.release(reserved);
            self.refresh_bandwidth_cap_window()?;
        }
        Ok(())
    }

    pub(super) fn record_download_bandwidth_usage(
        &mut self,
        payload_bytes: u64,
    ) -> Result<(), SchedulerError> {
        self.bandwidth_cap
            .record_download_bytes(&self.db, payload_bytes)?;
        self.shared_state.set_download_block(
            self.bandwidth_cap
                .to_download_block_state(self.global_paused),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
}
