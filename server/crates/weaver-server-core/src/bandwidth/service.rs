use std::collections::BTreeMap;
use std::range::Range;
use std::time::{Duration as StdDuration, Instant};

use chrono::{
    DateTime, Datelike, Duration, Local, LocalResult, NaiveDate, NaiveDateTime, TimeZone,
};

use crate::SchedulerError;
use crate::bandwidth::{IspBandwidthCapConfig, IspBandwidthCapPeriod, IspBandwidthCapWeekday};
use crate::jobs::handle::{DownloadBlockKind, DownloadBlockState};
use crate::pipeline::Pipeline;

/// Origin of a global download pause, so the download-block presentation can
/// distinguish a schedule-driven pause from a manual one across every refresh.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GlobalPause {
    /// Not globally paused; the cap/quota may still block.
    Running,
    /// Operator pressed pause.
    Manual,
    /// A bandwidth schedule paused downloads.
    Scheduled,
}

const BANDWIDTH_LEDGER_RETENTION_DAYS: i64 = 90;
const BANDWIDTH_CAP_USAGE_FLUSH_BYTES: u64 = 64 * 1024 * 1024;
const BANDWIDTH_CAP_USAGE_FLUSH_INTERVAL: StdDuration = StdDuration::from_secs(10);
const BANDWIDTH_DISPLAY_USAGE_FLUSH_BYTES: u64 = 1024 * 1024 * 1024;
const BANDWIDTH_DISPLAY_USAGE_FLUSH_INTERVAL: StdDuration = StdDuration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BandwidthCapWindow {
    period: Range<DateTime<Local>>,
}

impl BandwidthCapWindow {
    fn new(start: DateTime<Local>, end: DateTime<Local>) -> Self {
        Self {
            period: Range { start, end },
        }
    }

    pub(crate) fn starts_at(&self) -> DateTime<Local> {
        self.period.start
    }

    pub(crate) fn ends_at(&self) -> DateTime<Local> {
        self.period.end
    }

    fn contains(&self, now: &DateTime<Local>) -> bool {
        self.period.contains(now)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BandwidthCapRuntime {
    policy: Option<IspBandwidthCapConfig>,
    window: Option<BandwidthCapWindow>,
    used_bytes: u64,
    reserved_bytes: u64,
    /// Dispatch was refused because the next reservation no longer fits the
    /// allowance. Conservative pre-reservation parks work before `used`
    /// reaches the limit, so the blocked presentation must not wait for
    /// `remaining_bytes() == 0`. Sticky until a reservation succeeds, the
    /// window rolls over, or the policy changes.
    parked_on_cap: bool,
    last_pruned_bucket_epoch_minute: Option<i64>,
    pending_usage_by_minute: BTreeMap<i64, u64>,
    pending_usage_bytes: u64,
    pending_usage_started_at: Option<Instant>,
}

impl BandwidthCapRuntime {
    pub(crate) fn set_policy(&mut self, policy: Option<IspBandwidthCapConfig>) {
        self.policy = policy;
        self.window = None;
        self.used_bytes = 0;
        self.reserved_bytes = 0;
        self.parked_on_cap = false;
    }

    pub(crate) fn cap_enabled(&self) -> bool {
        self.policy.as_ref().is_some_and(|policy| policy.enabled)
    }

    pub(crate) fn limit_bytes(&self) -> u64 {
        self.policy.as_ref().map_or(0, |policy| policy.limit_bytes)
    }

    pub(crate) fn remaining_bytes(&self) -> u64 {
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

    pub(crate) fn update_for_now(&mut self, db: &crate::Database) -> Result<(), crate::StateError> {
        // Always compute a window so the UI can show real usage even before
        // the user configures a cap.  Fall back to a default monthly window.
        let default_policy = Self::default_display_policy();
        let policy = self.policy.as_ref().unwrap_or(&default_policy);

        let now = crate::e2e_clock::local_now();
        let next_window = compute_window(now, policy);
        let should_reload = self.window.as_ref() != Some(&next_window);
        if should_reload {
            self.flush_pending_usage(db)?;
            let start_minute = next_window.starts_at().timestamp().div_euclid(60);
            let end_minute = next_window.ends_at().timestamp().div_euclid(60);
            self.used_bytes = db.sum_bandwidth_usage_minutes(start_minute, end_minute)?;
            self.window = Some(next_window);
            self.reserved_bytes = 0;
            self.parked_on_cap = false;
        }

        let prune_cutoff =
            now.timestamp().div_euclid(60) - BANDWIDTH_LEDGER_RETENTION_DAYS * 24 * 60;
        if self.last_pruned_bucket_epoch_minute != Some(prune_cutoff) {
            db.prune_bandwidth_usage_before(prune_cutoff)?;
            self.last_pruned_bucket_epoch_minute = Some(prune_cutoff);
        }

        Ok(())
    }

    pub(crate) fn can_reserve(&self, bytes: u64) -> bool {
        if !self.cap_enabled() {
            return true;
        }
        self.used_bytes
            .saturating_add(self.reserved_bytes)
            .saturating_add(bytes)
            <= self.limit_bytes()
    }

    pub(crate) fn reserve(&mut self, bytes: u64) {
        self.reserved_bytes = self.reserved_bytes.saturating_add(bytes);
        self.parked_on_cap = false;
    }

    pub(crate) fn record_reservation_rejected(&mut self) {
        self.parked_on_cap = true;
    }

    pub(crate) fn release(&mut self, bytes: u64) {
        self.reserved_bytes = self.reserved_bytes.saturating_sub(bytes);
    }

    pub(crate) fn record_download_bytes(
        &mut self,
        db: &crate::Database,
        payload_bytes: u64,
    ) -> Result<(), crate::StateError> {
        let now = crate::e2e_clock::local_now();
        let bucket_epoch_minute = now.timestamp().div_euclid(60);
        self.record_pending_usage(bucket_epoch_minute, payload_bytes);
        if let Some(window) = &self.window
            && window.contains(&now)
        {
            self.used_bytes = self.used_bytes.saturating_add(payload_bytes);
        } else {
            self.flush_pending_usage(db)?;
            self.update_for_now(db)?;
        }
        if self.should_flush_pending_usage() {
            self.flush_pending_usage(db)?;
        }
        Ok(())
    }

    pub(crate) fn flush_pending_usage(
        &mut self,
        db: &crate::Database,
    ) -> Result<(), crate::StateError> {
        if self.pending_usage_by_minute.is_empty() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let entries = self
            .pending_usage_by_minute
            .iter()
            .map(|(bucket, bytes)| (*bucket, *bytes))
            .collect::<Vec<_>>();
        db.add_bandwidth_usage_minutes(&entries)?;
        self.pending_usage_by_minute.clear();
        self.pending_usage_bytes = 0;
        self.pending_usage_started_at = None;
        crate::runtime::perf_probe::record("bandwidth.flush_pending_usage", started.elapsed());
        Ok(())
    }

    fn record_pending_usage(&mut self, bucket_epoch_minute: i64, payload_bytes: u64) {
        if self.pending_usage_by_minute.is_empty() {
            self.pending_usage_started_at = Some(Instant::now());
        }
        self.pending_usage_by_minute
            .entry(bucket_epoch_minute)
            .and_modify(|bytes| *bytes = bytes.saturating_add(payload_bytes))
            .or_insert(payload_bytes);
        self.pending_usage_bytes = self.pending_usage_bytes.saturating_add(payload_bytes);
    }

    fn should_flush_pending_usage(&self) -> bool {
        let (bytes, interval) = if self.cap_enabled() {
            (
                BANDWIDTH_CAP_USAGE_FLUSH_BYTES,
                BANDWIDTH_CAP_USAGE_FLUSH_INTERVAL,
            )
        } else {
            (
                BANDWIDTH_DISPLAY_USAGE_FLUSH_BYTES,
                BANDWIDTH_DISPLAY_USAGE_FLUSH_INTERVAL,
            )
        };
        self.pending_usage_bytes >= bytes
            || self
                .pending_usage_started_at
                .is_some_and(|started| started.elapsed() >= interval)
    }

    pub(crate) fn to_download_block_state(&self, pause: GlobalPause) -> DownloadBlockState {
        let timezone_name = {
            let now = crate::e2e_clock::local_now();
            let label = now.format("%Z").to_string();
            if label.is_empty() {
                now.offset().to_string()
            } else {
                label
            }
        };

        // A global pause outranks the cap. The origin of the pause is carried
        // explicitly so every block-state refresh reports the same kind:
        // deriving it from a single `global_paused` bool let a scheduled pause
        // be silently reclassified as manual by any later refresh.
        let kind = match pause {
            GlobalPause::Manual => DownloadBlockKind::ManualPause,
            GlobalPause::Scheduled => DownloadBlockKind::Scheduled,
            GlobalPause::Running => {
                if self.cap_enabled() && (self.remaining_bytes() == 0 || self.parked_on_cap) {
                    DownloadBlockKind::IspCap
                } else {
                    DownloadBlockKind::None
                }
            }
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
                .map(|window| window.starts_at().timestamp_millis() as f64),
            window_ends_at_epoch_ms: self
                .window
                .as_ref()
                .map(|window| window.ends_at().timestamp_millis() as f64),
            timezone_name,
            scheduled_speed_limit: 0,
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
    BandwidthCapWindow::new(start, end)
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
    BandwidthCapWindow::new(start, end)
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
    BandwidthCapWindow::new(start, end)
}

pub(crate) fn compute_window(
    now: DateTime<Local>,
    policy: &IspBandwidthCapConfig,
) -> BandwidthCapWindow {
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
    /// The current global-pause origin, so every download-block refresh reports
    /// a consistent Scheduled vs ManualPause kind.
    pub(crate) fn global_pause(&self) -> GlobalPause {
        if !self.global_paused {
            GlobalPause::Running
        } else if self.scheduled_pause {
            GlobalPause::Scheduled
        } else {
            GlobalPause::Manual
        }
    }

    pub(crate) fn refresh_bandwidth_cap_window(&mut self) -> Result<(), SchedulerError> {
        self.bandwidth_cap.update_for_now(&self.db)?;
        let mut block = self.bandwidth_cap.to_download_block_state(self.global_pause());
        block.scheduled_speed_limit = self.scheduled_rate_limit.unwrap_or(0);
        self.shared_state.set_download_block(block);
        Ok(())
    }

    pub(crate) fn apply_bandwidth_cap_policy(
        &mut self,
        policy: Option<IspBandwidthCapConfig>,
    ) -> Result<(), SchedulerError> {
        self.bandwidth_cap.set_policy(policy);
        self.refresh_bandwidth_cap_window()?;
        Ok(())
    }

    pub(crate) fn reserve_bandwidth_for_dispatch(
        &mut self,
        segment_id: crate::jobs::ids::SegmentId,
        estimate_bytes: u64,
    ) -> Result<bool, SchedulerError> {
        self.refresh_bandwidth_cap_window()?;
        if !self.bandwidth_cap.can_reserve(estimate_bytes) {
            self.bandwidth_cap.record_reservation_rejected();
            self.shared_state.set_download_block(
                self.bandwidth_cap
                    .to_download_block_state(self.global_pause()),
            );
            return Ok(false);
        }
        self.bandwidth_cap.reserve(estimate_bytes);
        self.bandwidth_reservations
            .insert(segment_id, estimate_bytes);
        self.shared_state.set_download_block(
            self.bandwidth_cap
                .to_download_block_state(self.global_pause()),
        );
        Ok(true)
    }

    pub(crate) fn release_bandwidth_reservation(
        &mut self,
        segment_id: crate::jobs::ids::SegmentId,
    ) -> Result<(), SchedulerError> {
        if let Some(reserved) = self.bandwidth_reservations.remove(&segment_id) {
            self.bandwidth_cap.release(reserved);
            self.refresh_bandwidth_cap_window()?;
        }
        Ok(())
    }

    pub(crate) fn record_download_bandwidth_usage(
        &mut self,
        payload_bytes: u64,
    ) -> Result<(), SchedulerError> {
        self.bandwidth_cap
            .record_download_bytes(&self.db, payload_bytes)?;
        let mut block = self
            .bandwidth_cap
            .to_download_block_state(self.global_pause());
        block.scheduled_speed_limit = self.scheduled_rate_limit.unwrap_or(0);
        self.shared_state.set_download_block(block);
        Ok(())
    }

    pub(crate) fn flush_download_bandwidth_usage(&mut self) -> Result<(), SchedulerError> {
        self.bandwidth_cap.flush_pending_usage(&self.db)?;
        let mut block = self
            .bandwidth_cap
            .to_download_block_state(self.global_pause());
        block.scheduled_speed_limit = self.scheduled_rate_limit.unwrap_or(0);
        self.shared_state.set_download_block(block);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
