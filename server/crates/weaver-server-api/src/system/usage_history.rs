// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Data used per day, read back for the monitoring dashboards.
//!
//! Every date is a local date on this host, and a week starts on Monday.

use async_graphql::{Enum, InputObject, SimpleObject};
use chrono::{Days, Local, NaiveDate, TimeZone};
use serde::{Deserialize, Serialize};

use weaver_server_core::operations::{
    FinishedJobCounts, UsageConsumerKind as CoreKind, UsageConsumerSummary, UsageCounts,
    UsagePeriod, UsageSummary, period_bounds, summarize_usage, summary_read_from,
};
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
}

impl From<UsageConsumerKindGql> for CoreKind {
    fn from(value: UsageConsumerKindGql) -> Self {
        match value {
            UsageConsumerKindGql::Server => Self::Server,
            UsageConsumerKindGql::Category => Self::Category,
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
}

impl From<UsageCounts> for UsageTotals {
    fn from(value: UsageCounts) -> Self {
        Self {
            bytes: value.bytes,
            articles_tried: value.articles_tried,
            articles_succeeded: value.articles_succeeded,
            down_seconds: value.down_seconds,
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
                })
                .collect(),
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
    let (start, end) = period_bounds(period, today);
    let rows = db.read_usage_days(summary_read_from(period, today), end.max(today), kind)?;
    let totals = db.read_usage_totals(kind)?;
    let summary: UsageSummary = summarize_usage(
        &rows,
        &totals,
        today,
        period,
        kind.unwrap_or(CoreKind::Server),
    );
    let today_start = local_day_start(today);
    let tomorrow_start = local_day_start(next_day(today));
    let finished_today = db.count_finished_jobs(Some(today_start), tomorrow_start)?;
    let finished_in_range = db.count_finished_jobs(
        start.map(local_day_start),
        local_day_start(next_day(summary.range_end)),
    )?;
    let day = |date: NaiveDate| date.format("%Y-%m-%d").to_string();
    Ok(UsageHistory {
        today: day(summary.today),
        range_start: day(summary.range_start),
        range_end: day(summary.range_end),
        today_start_epoch_sec: today_start,
        total: summary.total.into(),
        consumers: summary.consumers.into_iter().map(Into::into).collect(),
        finished_today: finished_today.into(),
        finished_in_range: finished_in_range.into(),
    })
}

/// This host's local date now.
pub fn local_today() -> NaiveDate {
    Local::now().date_naive()
}
