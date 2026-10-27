// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;
use std::range::Range;
use std::time::{Duration as StdDuration, Instant};

use chrono::{
    DateTime, Datelike, Duration, Local, LocalResult, NaiveDate, NaiveDateTime, TimeZone,
};

use crate::bandwidth::QuotaWeekday;
use crate::jobs::handle::{DownloadBlockKind, DownloadBlockState};
use crate::pipeline::Pipeline;
use crate::servers::{ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::jobs::{HoldOrigin, HoldOwner, HoldSet};

/// Written by transport lanes, sampled independently of pipeline completions.
#[derive(Debug)]
pub(crate) struct BandwidthCounters {
    metered: AtomicU64,
    unmetered: AtomicU64,
    enabled: AtomicBool,
}

impl Default for BandwidthCounters {
    fn default() -> Self {
        Self {
            metered: AtomicU64::new(0),
            unmetered: AtomicU64::new(0),
            enabled: AtomicBool::new(true),
        }
    }
}

impl BandwidthCounters {
    pub(crate) fn record(&self, bytes: u64) {
        let target = if self.enabled.load(Ordering::Relaxed) {
            &self.metered
        } else {
            &self.unmetered
        };
        target.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn total(&self) -> u64 {
        self.metered
            .load(Ordering::Relaxed)
            .saturating_add(self.unmetered.load(Ordering::Relaxed))
    }

    pub(crate) fn set_metering_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }
}

const BANDWIDTH_LEDGER_RETENTION_DAYS: i64 = 90;
const BANDWIDTH_USAGE_FLUSH_BYTES: u64 = 1024 * 1024 * 1024;
const BANDWIDTH_USAGE_FLUSH_INTERVAL: StdDuration = StdDuration::from_secs(60);

// One quota period, in local time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuotaWindow {
    period: Range<DateTime<Local>>,
}

impl QuotaWindow {
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
}

// The per-minute download ledger behind the bandwidth graph.
//
// It records every BODY byte once, tagged with whether quota metering was on
// when it arrived, and flushes in batches. Download quotas themselves live on
// each egress and server; nothing here refuses work.
#[derive(Debug, Default)]
pub(crate) struct BandwidthLedgerRuntime {
    sampled: [u64; 2],
    last_pruned_bucket_epoch_minute: Option<i64>,
    pending_usage_by_minute: BTreeMap<(i64, bool), u64>,
    pending_usage_bytes: u64,
    pending_usage_started_at: Option<Instant>,
    // Batches handed to the writer whose write has not answered yet. A batch
    // stays here until its write commits; a failed or dropped write puts its
    // bytes back in the pending ledger.
    unacknowledged_usage: Vec<UnacknowledgedUsage>,
}

#[derive(Debug)]
struct UnacknowledgedUsage {
    entries: Vec<(i64, bool, u64)>,
    committed: tokio::sync::oneshot::Receiver<bool>,
}

impl BandwidthLedgerRuntime {
    pub(crate) fn sample(
        &mut self,
        db: &crate::Database,
        counters: &BandwidthCounters,
        flush: bool,
    ) -> Result<(), crate::StateError> {
        for (index, total) in [
            counters.unmetered.load(Ordering::Relaxed),
            counters.metered.load(Ordering::Relaxed),
        ]
        .into_iter()
        .enumerate()
        {
            let bytes = total.saturating_sub(self.sampled[index]);
            if bytes > 0 {
                self.record_pending_usage(
                    crate::e2e_clock::unix_seconds().div_euclid(60),
                    bytes,
                    index == 1,
                );
                self.sampled[index] = total;
            }
        }
        if flush || self.should_flush_pending_usage() {
            self.flush_pending_usage(db)?;
        }
        self.prune_if_due(db)
    }

    // Queues the delete of buckets past retention, once per minute. It goes
    // on the ordered writer queue rather than running here: a prune lost to a
    // crash is redone by the next one, and nothing reads the ledger back
    // before the writer has run it.
    pub(crate) fn prune_if_due(&mut self, db: &crate::Database) -> Result<(), crate::StateError> {
        let now_secs = crate::e2e_clock::unix_seconds();
        let prune_cutoff = now_secs.div_euclid(60) - BANDWIDTH_LEDGER_RETENTION_DAYS * 24 * 60;
        if self.last_pruned_bucket_epoch_minute != Some(prune_cutoff) {
            db.try_queue_write("bandwidth_usage_prune", move |db| {
                db.prune_bandwidth_usage_before(prune_cutoff).map(|_| ())
            })?;
            self.last_pruned_bucket_epoch_minute = Some(prune_cutoff);
        }
        Ok(())
    }

    // Hands the pending minutes to the ordered writer queue. The caller is
    // independent counter ticker, so the write itself never runs here; a
    // batch lost to a crash only shortens the usage graph, and the queue's
    // order keeps the shutdown flush behind every batch handed over before it.
    // A handed-over batch leaves the ledger only once its write commits: a
    // write that fails returns its bytes to the next flush.
    pub(crate) fn flush_pending_usage(
        &mut self,
        db: &crate::Database,
    ) -> Result<(), crate::StateError> {
        self.settle_unacknowledged_usage();
        if self.pending_usage_by_minute.is_empty() {
            return Ok(());
        }
        let entries = self
            .pending_usage_by_minute
            .iter()
            .map(|((bucket, metered), bytes)| (*bucket, *metered, *bytes))
            .collect::<Vec<_>>();
        let (committed_tx, committed) = tokio::sync::oneshot::channel();
        let written = entries.clone();
        db.try_queue_write("bandwidth_usage_flush", move |db| {
            let started = std::time::Instant::now();
            let result = db.add_metered_bandwidth_usage_minutes(&written);
            crate::runtime::perf_probe::record("bandwidth.flush_pending_usage", started.elapsed());
            let _ = committed_tx.send(result.is_ok());
            result
        })?;
        self.unacknowledged_usage
            .push(UnacknowledgedUsage { entries, committed });
        self.pending_usage_by_minute.clear();
        self.pending_usage_bytes = 0;
        self.pending_usage_started_at = None;
        Ok(())
    }

    // Drops every batch whose write committed and merges back the bytes of
    // every batch whose write failed or was dropped unrun. A batch whose write
    // has not run yet stays.
    fn settle_unacknowledged_usage(&mut self) {
        let mut returned = Vec::new();
        self.unacknowledged_usage
            .retain_mut(|batch| match batch.committed.try_recv() {
                Ok(true) => false,
                Ok(false) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    returned.append(&mut batch.entries);
                    false
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => true,
            });
        for (bucket_epoch_minute, metered, bytes) in returned {
            self.record_pending_usage(bucket_epoch_minute, bytes, metered);
        }
    }

    fn record_pending_usage(
        &mut self,
        bucket_epoch_minute: i64,
        payload_bytes: u64,
        metered: bool,
    ) {
        if self.pending_usage_by_minute.is_empty() {
            self.pending_usage_started_at = Some(Instant::now());
        }
        self.pending_usage_by_minute
            .entry((bucket_epoch_minute, metered))
            .and_modify(|bytes| *bytes = bytes.saturating_add(payload_bytes))
            .or_insert(payload_bytes);
        self.pending_usage_bytes = self.pending_usage_bytes.saturating_add(payload_bytes);
    }

    fn should_flush_pending_usage(&self) -> bool {
        self.pending_usage_bytes >= BANDWIDTH_USAGE_FLUSH_BYTES
            || self
                .pending_usage_started_at
                .is_some_and(|started| started.elapsed() >= BANDWIDTH_USAGE_FLUSH_INTERVAL)
    }
}

// The block a hold puts on the queue, by who holds it. Quota blocks are laid
// over this by the shared state.
pub(crate) fn to_download_block_state(held: Option<HoldOrigin>) -> DownloadBlockState {
    let kind = match held {
        Some(HoldOrigin::Manual) => DownloadBlockKind::ManualPause,
        Some(HoldOrigin::SpeedTest) => DownloadBlockKind::SpeedTest,
        Some(HoldOrigin::Scheduled) => DownloadBlockKind::Scheduled,
        None => DownloadBlockKind::None,
    };
    DownloadBlockState {
        kind,
        ..DownloadBlockState::default()
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
pub(crate) fn compute_window(now: DateTime<Local>, quota: QuotaCalendar) -> Option<QuotaWindow> {
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

impl Pipeline {
    // The rate one direction of the whole instance may move at: the
    // operator's global limit under any scheduled one.
    pub(crate) fn global_rate(&self, direction: crate::bandwidth::Direction) -> Option<u64> {
        crate::bandwidth::effective_rate(crate::bandwidth::RateLimits {
            global: self.configured_bandwidth.limit(direction),
            schedule: self
                .scheduled_bandwidth
                .and_then(|scheduled| scheduled.limit(direction)),
            ..crate::bandwidth::RateLimits::default()
        })
    }

    // The usenet/torrent split in force: a scheduled one over the
    // operator's.
    pub(crate) fn split_in_force(&self) -> Option<crate::bandwidth::BandwidthSplit> {
        self.scheduled_split.or(self.configured_bandwidth.split)
    }

    // The split's tick: once `SPLIT_TICK` has passed since it last
    // measured, measure what usenet and torrents each moved since, from
    // their byte counters, against the lowest rate each was held to in
    // between. The shares follow the measurement until the next. Usenet's
    // counter is the servers' registry's, which counts BODY bytes read by
    // read as they come off the wire, so a tick sees an article in flight
    // and not only the ones finished in it.
    //
    // The pipeline ticks every `IDLE_SNAPSHOT_INTERVAL` while idle, so the
    // tick is taken once `SPLIT_TICK` less half an interval has passed: a
    // pipeline tick a moment early does not put the split's off a whole
    // interval.
    pub(crate) fn measure_split(&mut self) {
        let now = tokio::time::Instant::now();
        let usenet = self
            .shared_state
            .server_transfer_policy()
            .map_or(0, |policy| policy.transfer_registry().received_bytes());
        let torrents = self.shared_state.torrent_rate();
        let Some((then, before)) = self.split_sampled else {
            self.split_sampled = Some((now, usenet));
            if let Some(torrents) = torrents {
                torrents.sample();
            }
            return;
        };
        let elapsed = now - then;
        let early = Self::metrics_snapshot_period(self.pipeline_is_live()) / 2;
        if elapsed + early < crate::bandwidth::SPLIT_TICK {
            return;
        }
        self.split_tick = crate::bandwidth::SplitTick {
            given: std::mem::replace(&mut self.split_held, self.split_applied),
            used: crate::bandwidth::SplitUse {
                usenet: crate::bandwidth::per_second(usenet.saturating_sub(before), elapsed),
                torrent: torrents.map_or(0, |torrents| torrents.sample()),
            },
        };
        self.split_sampled = Some((now, usenet));
    }

    // Put the global rates in force: the download rate shared between
    // usenet and torrents by the split in force from what each moved at over
    // the split's last tick, and the upload rate, all of it the torrents'
    // since usenet uploads nothing. Run on every change to a limit or the
    // split and on the bandwidth tick; between two of the split's ticks it
    // puts the same shares in force.
    pub(crate) fn apply_global_rate(&mut self) {
        let cap = self.global_rate(crate::bandwidth::Direction::Download);
        let split = self.split_in_force();
        let torrents = self.shared_state.torrent_rate();
        let rates = match &torrents {
            Some(_) => crate::bandwidth::split_rates(
                cap,
                split,
                self.split_tick.given,
                self.split_tick.used,
            ),
            // With no torrent side there is nothing to share: usenet is held
            // to the limit itself.
            None => crate::bandwidth::SplitRates {
                usenet: cap,
                torrent: None,
            },
        };
        self.split_applied = rates;
        self.split_held = self.split_held.lower(rates);
        if self.rate_limiter.rate() != rates.usenet {
            self.rate_limiter.set_rate(rates.usenet);
        }
        if let Some(torrents) = torrents {
            torrents.set_rates(crate::bandwidth::TorrentRates {
                download: rates.torrent,
                upload: self.global_rate(crate::bandwidth::Direction::Upload),
            });
        }
        self.shared_state
            .set_bandwidth_split(crate::bandwidth::SplitState {
                stored: self.configured_bandwidth.split,
                in_force: split,
                cap,
                rates,
            });
    }

    // Who holds the queue as a whole, while anyone does.
    pub(crate) fn global_pause(&self) -> Option<HoldOrigin> {
        self.applied_holds.reason(HoldOwner::All, false)
    }

    // Put in force the holds the queue records. Every pause and resume, the
    // operator's, the schedule's or a speed test's, writes its hold to the
    // queue, and the pipeline follows it here.
    pub(crate) async fn apply_holds(&mut self, holds: HoldSet) {
        if holds == self.applied_holds {
            return;
        }
        let before = std::mem::replace(&mut self.applied_holds, holds);
        let was_paused = before.stops(HoldOwner::All, false);
        let paused = self.applied_holds.stops(HoldOwner::All, false);
        if paused != was_paused {
            self.download_dispatch_stall = None;
        }
        if paused {
            if !was_paused {
                self.demand_direct_store_barriers_for_pause(None).await;
            }
            self.download_restart_durable_lead_retry_after.clear();
        }
        // The operator's holds outlive a restart; the others are put back by
        // whoever holds them.
        for owner in [HoldOwner::All, HoldOwner::Torrents] {
            let held = self.applied_holds.held_by(HoldOrigin::Manual, owner);
            if held != before.held_by(HoldOrigin::Manual, owner)
                && let Err(error) = self.db.try_queue_write(owner.setting(), move |db| {
                    db.set_setting(owner.setting(), &held.to_string())
                })
            {
                tracing::error!(%error, ?owner, "db write failed for the operator's hold");
            }
        }
        let held_over_all = |holds: &HoldSet| {
            (
                holds.reason(HoldOwner::All, false),
                holds.reason(HoldOwner::All, true),
            )
        };
        if held_over_all(&before) != held_over_all(&self.applied_holds) {
            self.emit_job_event(if paused {
                crate::events::model::PipelineEvent::GlobalPaused
            } else {
                crate::events::model::PipelineEvent::GlobalResumed
            });
        }
        self.publish_download_block();
    }

    pub(crate) fn publish_download_block(&mut self) {
        let mut block = to_download_block_state(self.global_pause());
        block.scheduled_speed_limit = self
            .scheduled_bandwidth
            .and_then(|scheduled| scheduled.download_bps);
        self.shared_state.set_download_block(block);
    }
}

#[cfg(test)]
mod tests;
