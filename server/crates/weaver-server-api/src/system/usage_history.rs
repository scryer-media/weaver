// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Data used per day, read back for the monitoring dashboards.
//!
//! Every date is a local date on this host, and a week starts on Monday.

use async_graphql::{Enum, InputObject, SimpleObject};
use chrono::{Days, Local, NaiveDate, TimeZone};
use serde::{Deserialize, Serialize};

use weaver_server_core::operations::{
    FinishedJobCounts, REMOVED_SERVER_ID_PREFIX, RecoveryBytes, UsageConsumerKind as CoreKind,
    UsageConsumerSummary, UsageCounts, UsagePeriod, UsageStart, UsageSummary, period_bounds,
    summarize_usage,
    summary_read_from,
};
use weaver_server_core::torrents::metrics::Published;
use weaver_server_core::{Database, StateError};

/// The longest custom range one request may ask for, in days.
pub const MAX_USAGE_RANGE_DAYS: i64 = 800;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
#[graphql(name = "UsagePeriod")]
pub enum UsagePeriodGql {
    Today,
    /// Monday to today.
    Week,
    Month,
    Year,
    All,
    /// `fromDay` to `toDay`, both included.
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
#[graphql(name = "UsageConsumerKind")]
pub enum UsageConsumerKindGql {
    Server,
    Category,
    /// NZB jobs finished and repaired.
    Job,
    /// Job stages, by name.
    Stage,
    /// Every torrent together.
    Torrent,
    /// Torrents by the tracker hosts they list.
    Tracker,
}

impl From<UsageConsumerKindGql> for CoreKind {
    fn from(value: UsageConsumerKindGql) -> Self {
        match value {
            UsageConsumerKindGql::Server => Self::Server,
            UsageConsumerKindGql::Category => Self::Category,
            UsageConsumerKindGql::Job => Self::Job,
            UsageConsumerKindGql::Stage => Self::Stage,
            UsageConsumerKindGql::Torrent => Self::Torrent,
            UsageConsumerKindGql::Tracker => Self::Tracker,
        }
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct UsageRange {
    pub period: UsagePeriodGql,
    /// `YYYY-MM-DD`, for a custom range.
    pub from_day: Option<String>,
    /// `YYYY-MM-DD`, for a custom range.
    pub to_day: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, SimpleObject)]
pub struct UsageTotals {
    pub bytes: u64,
    pub articles_tried: u64,
    pub articles_succeeded: u64,
    pub down_seconds: u64,
    /// Sent to peers, for a torrent or tracker.
    pub bytes_uploaded: u64,
    /// Jobs for a job row, timed runs for a stage.
    pub events: u64,
    /// Stage time, or seeding time, in milliseconds.
    pub duration_ms: u64,
}

impl From<UsageCounts> for UsageTotals {
    fn from(value: UsageCounts) -> Self {
        Self {
            bytes: value.bytes,
            articles_tried: value.articles_tried,
            articles_succeeded: value.articles_succeeded,
            down_seconds: value.down_seconds,
            bytes_uploaded: value.bytes_uploaded,
            events: value.events,
            duration_ms: value.duration_ms,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct UsageDay {
    /// `YYYY-MM-DD`.
    pub day: String,
    pub bytes: u64,
    pub articles_tried: u64,
    pub articles_succeeded: u64,
    pub down_seconds: u64,
    pub bytes_uploaded: u64,
    pub events: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct UsageConsumer {
    pub kind: String,
    /// A server's saved id, or a category's name; empty for no category and
    /// for the queue-wide total.
    pub id: String,
    pub today: UsageTotals,
    pub week: UsageTotals,
    pub month: UsageTotals,
    pub year: UsageTotals,
    pub all_time: UsageTotals,
    pub range: UsageTotals,
    /// Every date of the range in order, days with nothing at zero.
    pub days: Vec<UsageDay>,
    /// For a removed server whose rows are kept until its quota resets: the
    /// server's name when it was removed. Absent for every other consumer.
    pub removed_host: Option<String>,
}

impl From<UsageConsumerSummary> for UsageConsumer {
    fn from(value: UsageConsumerSummary) -> Self {
        Self {
            kind: value.kind,
            id: value.consumer_id,
            today: value.today.into(),
            week: value.week.into(),
            month: value.month.into(),
            year: value.year.into(),
            all_time: value.all_time.into(),
            range: value.range.into(),
            days: value
                .days
                .into_iter()
                .map(|(day, counts)| UsageDay {
                    day: day.format("%Y-%m-%d").to_string(),
                    bytes: counts.bytes,
                    articles_tried: counts.articles_tried,
                    articles_succeeded: counts.articles_succeeded,
                    down_seconds: counts.down_seconds,
                    bytes_uploaded: counts.bytes_uploaded,
                    events: counts.events,
                    duration_ms: counts.duration_ms,
                })
                .collect(),
            removed_host: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, SimpleObject)]
pub struct FinishedJobs {
    pub complete: u32,
    pub partial: u32,
    pub failed: u32,
}

impl From<FinishedJobCounts> for FinishedJobs {
    fn from(value: FinishedJobCounts) -> Self {
        Self {
            complete: value.complete,
            partial: value.partial,
            failed: value.failed,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct UsageHistory {
    /// This host's local date, `YYYY-MM-DD`.
    pub today: String,
    pub range_start: String,
    pub range_end: String,
    /// The start of today on this host, in epoch seconds.
    pub today_start_epoch_sec: i64,
    /// The sum over every consumer of the asked kind, servers when none is
    /// asked. Servers and categories count the same bytes, so the two are
    /// never added together.
    pub total: UsageConsumer,
    pub consumers: Vec<UsageConsumer>,
    /// Jobs that finished today, by how they ended.
    pub finished_today: FinishedJobs,
    /// Jobs that finished inside the range, by how they ended.
    pub finished_in_range: FinishedJobs,
}

/// Why a range was refused.
#[derive(Debug, PartialEq, Eq)]
pub enum UsageRangeError {
    MissingDay,
    BadDay(String),
    TooLong,
}

impl std::fmt::Display for UsageRangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingDay => write!(f, "a custom range needs fromDay and toDay"),
            Self::BadDay(day) => write!(f, "{day:?} is not a YYYY-MM-DD date"),
            Self::TooLong => write!(
                f,
                "a custom range may cover at most {MAX_USAGE_RANGE_DAYS} days"
            ),
        }
    }
}

pub fn usage_period(range: &UsageRange) -> Result<UsagePeriod, UsageRangeError> {
    let parse = |value: &Option<String>| -> Result<NaiveDate, UsageRangeError> {
        let value = value.as_deref().ok_or(UsageRangeError::MissingDay)?.trim();
        NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map_err(|_| UsageRangeError::BadDay(value.to_string()))
    };
    Ok(match range.period {
        UsagePeriodGql::Today => UsagePeriod::Today,
        UsagePeriodGql::Week => UsagePeriod::Week,
        UsagePeriodGql::Month => UsagePeriod::Month,
        UsagePeriodGql::Year => UsagePeriod::Year,
        UsagePeriodGql::All => UsagePeriod::All,
        UsagePeriodGql::Custom => {
            let from = parse(&range.from_day)?;
            let to = parse(&range.to_day)?;
            if (to - from).num_days().abs() >= MAX_USAGE_RANGE_DAYS {
                return Err(UsageRangeError::TooLong);
            }
            UsagePeriod::Custom { from, to }
        }
    })
}

/// The epoch second the local date starts at. A date whose midnight a
/// clock change skips starts at its first real moment.
fn local_day_start(day: NaiveDate) -> i64 {
    let midnight = day.and_hms_opt(0, 0, 0).unwrap_or_default();
    match Local.from_local_datetime(&midnight).earliest() {
        Some(start) => start.timestamp(),
        None => (1..=4)
            .find_map(|hour| {
                Local
                    .from_local_datetime(&(midnight + chrono::Duration::hours(hour)))
                    .earliest()
            })
            .map_or_else(|| midnight.and_utc().timestamp(), |start| start.timestamp()),
    }
}

fn next_day(day: NaiveDate) -> NaiveDate {
    day.checked_add_days(Days::new(1)).unwrap_or(day)
}

/// Read and sum the ledger for one request, against `today`.
pub fn load_usage_history(
    db: &Database,
    today: NaiveDate,
    period: UsagePeriod,
    kind: Option<CoreKind>,
) -> Result<UsageHistory, StateError> {
    let (start, _) = period_bounds(period, today);
    let summary = read_summary(db, today, period, kind)?;
    let today_start = local_day_start(today);
    let tomorrow_start = local_day_start(next_day(today));
    let finished_today = db.count_finished_jobs(Some(today_start), tomorrow_start)?;
    let finished_in_range = db.count_finished_jobs(
        start.map(local_day_start),
        local_day_start(next_day(summary.range_end)),
    )?;
    let day = |date: NaiveDate| date.format("%Y-%m-%d").to_string();
    let mut consumers: Vec<UsageConsumer> = summary.consumers.into_iter().map(Into::into).collect();
    if kind.is_none_or(|kind| kind == CoreKind::Server) {
        name_removed_servers(db, &mut consumers)?;
    }
    Ok(UsageHistory {
        today: day(summary.today),
        range_start: day(summary.range_start),
        range_end: day(summary.range_end),
        today_start_epoch_sec: today_start,
        total: summary.total.into(),
        consumers,
        finished_today: finished_today.into(),
        finished_in_range: finished_in_range.into(),
    })
}

/// Give each removed server's kept rows the name the server had.
fn name_removed_servers(db: &Database, consumers: &mut [UsageConsumer]) -> Result<(), StateError> {
    let server = CoreKind::Server.as_str();
    if !consumers.iter().any(|consumer| {
        consumer.kind == server && consumer.id.starts_with(REMOVED_SERVER_ID_PREFIX)
    }) {
        return Ok(());
    }
    let removed = db.read_removed_servers()?;
    for consumer in consumers
        .iter_mut()
        .filter(|consumer| consumer.kind == server)
    {
        consumer.removed_host = removed
            .iter()
            .find(|entry| entry.consumer_id == consumer.id)
            .map(|entry| entry.host.clone());
    }
    Ok(())
}

/// This host's local date now.
pub fn local_today() -> NaiveDate {
    Local::now().date_naive()
}

/// The ledger read and summed for one kind, or every kind with servers as
/// the total.
fn read_summary(
    db: &Database,
    today: NaiveDate,
    period: UsagePeriod,
    kind: Option<CoreKind>,
) -> Result<UsageSummary, StateError> {
    let (_, end) = period_bounds(period, today);
    let rows = db.read_usage_days(summary_read_from(period, today), end.max(today), kind)?;
    let totals = db.read_usage_totals(kind)?;
    Ok(summarize_usage(
        &rows,
        &totals,
        today,
        period,
        kind.unwrap_or(CoreKind::Server),
    ))
}

fn day_text(date: NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

#[derive(Debug, Clone, Copy, Default, SimpleObject)]
pub struct RecoveryTotals {
    /// Recovery the jobs posted.
    pub available_bytes: u64,
    /// Of that, what they fetched.
    pub fetched_bytes: u64,
}

impl From<RecoveryBytes> for RecoveryTotals {
    fn from(value: RecoveryBytes) -> Self {
        Self {
            available_bytes: value.available,
            fetched_bytes: value.fetched,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct FailureCount {
    /// `repair`, `password`, `disk_full`, `unpack`, `move`, `health` or
    /// `duplicate`; empty when the job's row keeps no cause.
    pub cause: String,
    pub count: u64,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct IntegrityHistory {
    /// This host's local date, `YYYY-MM-DD`.
    pub today: String,
    pub range_start: String,
    pub range_end: String,
    /// NZB jobs finished, counted in `events`, under the id `finished`.
    pub finished: UsageConsumer,
    /// Of those, the ones a repair was applied to, under the id `repaired`.
    pub repaired: UsageConsumer,
    /// Each job stage timed: its runs in `events`, its time in `durationMs`.
    pub stages: Vec<UsageConsumer>,
    /// From the history rows of the NZB jobs that finished in the range.
    pub recovery: RecoveryTotals,
    /// From the history rows of the NZB jobs that failed in the range.
    pub failures: Vec<FailureCount>,
}

/// The first day the history figures of a range count from, so they cover
/// the days the ledger's do: the range's own start, or the ledger's first
/// day when it starts later. `None` is every day, for a range with no
/// start over a ledger whose first days are folded together, which is
/// every day it counted.
fn history_from(start: Option<NaiveDate>, ledger: UsageStart, today: NaiveDate) -> Option<NaiveDate> {
    let ledger = match ledger {
        UsageStart::Folded => return start,
        // Nothing counted yet: the ledger, like the range it shows, starts
        // today.
        UsageStart::Empty => today,
        UsageStart::Day(day) => day,
    };
    Some(start.map_or(ledger, |start| start.max(ledger)))
}

/// Read the integrity ledger and history for one request, against `today`.
pub fn load_integrity_history(
    db: &Database,
    today: NaiveDate,
    period: UsagePeriod,
) -> Result<IntegrityHistory, StateError> {
    use weaver_server_core::operations::{JOBS_FINISHED, JOBS_REPAIRED};
    let (start, _) = period_bounds(period, today);
    let jobs = read_summary(db, today, period, Some(CoreKind::Job))?;
    let stages = read_summary(db, today, period, Some(CoreKind::Stage))?;
    let since = history_from(start, db.read_usage_start(CoreKind::Job)?, today)
        .map(local_day_start);
    let until = local_day_start(next_day(jobs.range_end));
    let recovery = db.sum_recovery_bytes(since, until)?;
    let failures = db.count_failures_by_cause(since, until)?;
    let job = |id: &str| -> UsageConsumer {
        let mut row = jobs
            .consumers
            .iter()
            .find(|consumer| consumer.consumer_id == id)
            .cloned()
            .unwrap_or_else(|| UsageConsumerSummary {
                kind: String::new(),
                consumer_id: id.to_string(),
                today: UsageCounts::default(),
                week: UsageCounts::default(),
                month: UsageCounts::default(),
                year: UsageCounts::default(),
                all_time: UsageCounts::default(),
                range: UsageCounts::default(),
                days: jobs
                    .total
                    .days
                    .iter()
                    .map(|(day, _)| (*day, UsageCounts::default()))
                    .collect(),
            });
        row.kind = CoreKind::Job.as_str().to_string();
        row.into()
    };
    Ok(IntegrityHistory {
        today: day_text(jobs.today),
        range_start: day_text(jobs.range_start),
        range_end: day_text(jobs.range_end),
        finished: job(JOBS_FINISHED),
        repaired: job(JOBS_REPAIRED),
        stages: stages.consumers.into_iter().map(Into::into).collect(),
        recovery: recovery.into(),
        failures: failures
            .into_iter()
            .map(|(cause, count)| FailureCount { cause, count })
            .collect(),
    })
}

#[derive(Debug, Clone, Copy, Default, SimpleObject)]
pub struct TorrentPeers {
    /// Torrents the client holds now.
    pub torrents: u32,
    /// Peers connected now, across every torrent.
    pub connected: u32,
    /// Of those, the ones that have every piece.
    pub seeds: u32,
}

impl TorrentPeers {
    pub fn of(published: &Published) -> Self {
        published
            .torrents
            .iter()
            .fold(Self::default(), |sum, torrent| Self {
                torrents: sum.torrents.saturating_add(1),
                connected: sum.connected.saturating_add(torrent.peers_established),
                seeds: sum.seeds.saturating_add(torrent.peer_seeders),
            })
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TorrentHistory {
    /// This host's local date, `YYYY-MM-DD`.
    pub today: String,
    pub range_start: String,
    pub range_end: String,
    /// Every torrent together: from peers in `bytes`, to peers in
    /// `bytesUploaded`, seeding time in `durationMs`.
    pub total: UsageConsumer,
    /// Each tracker host, by the torrents that list it; the empty id is
    /// the torrents that list none. A torrent counts under each host it
    /// lists, so trackers are never added together.
    pub trackers: Vec<UsageConsumer>,
    /// Now, as the torrent client last published its torrents.
    pub peers: TorrentPeers,
}

/// Read the torrent ledger for one request, against `today`.
pub fn load_torrent_history(
    db: &Database,
    today: NaiveDate,
    period: UsagePeriod,
    peers: TorrentPeers,
) -> Result<TorrentHistory, StateError> {
    let total = read_summary(db, today, period, Some(CoreKind::Torrent))?;
    let trackers = read_summary(db, today, period, Some(CoreKind::Tracker))?;
    Ok(TorrentHistory {
        today: day_text(total.today),
        range_start: day_text(total.range_start),
        range_end: day_text(total.range_end),
        total: total.total.into(),
        trackers: trackers.consumers.into_iter().map(Into::into).collect(),
        peers,
    })
}
