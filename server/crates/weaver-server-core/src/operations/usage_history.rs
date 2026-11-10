// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Data used per day, per consumer.
//!
//! The download path already counts what each server fetched and what each
//! category received, in relaxed atomics. This ledger never touches that
//! path: a recorder reads those running totals every few seconds, keeps the
//! difference since its last reading against the host's local date, and a
//! flush adds the differences to one row per day and consumer. A lost last
//! batch costs a few seconds of counts, never a double count.
//!
//! The same reading carries the job counters the exporter already keeps
//! (jobs finished and repaired, each stage's timed runs) and each torrent's
//! seeding ledger as the torrent service last published it, so those too
//! are kept per day.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use chrono::{Datelike, Days, Local, NaiveDate, TimeZone};

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime};

#[cfg(test)]
mod tests;

/// How often the recorder reads the counters and writes what moved.
pub const USAGE_FLUSH_INTERVAL_SECS: u64 = 5;
/// Daily rows older than this many days fold into one row per consumer.
pub const USAGE_RETENTION_DAYS: u64 = 400;
/// The most down time one reading may add. A host that slept, or a clock
/// stepped forward, would otherwise land the whole gap on one day.
pub const USAGE_MAX_DOWN_STEP_SECS: i64 = 60;
/// The day a consumer's folded older rows are kept under. It sorts before
/// every real date, so a range with a start never reads it.
const EARLIER_DAY: &str = "0000-00-00";
const DAY_FORMAT: &str = "%Y-%m-%d";

/// Who used the data a row counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum UsageConsumerKind {
    /// An NNTP server, by its id.
    Server,
    /// A category, by its name; the empty name is no category.
    Category,
    /// NZB jobs: `finished` counts each one that ended complete, partial or
    /// failed, `repaired` each of those that had a repair applied.
    Job,
    /// A job stage, by its name: the runs timed and their time.
    Stage,
    /// Every torrent together, under the empty id.
    Torrent,
    /// The torrents that list a tracker, by the tracker's host; the empty
    /// host is the torrents that list none. A torrent counts under each host
    /// it lists, so trackers are never added together.
    Tracker,
}

impl UsageConsumerKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Category => "category",
            Self::Job => "job",
            Self::Stage => "stage",
            Self::Torrent => "torrent",
            Self::Tracker => "tracker",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "server" => Some(Self::Server),
            "category" => Some(Self::Category),
            "job" => Some(Self::Job),
            "stage" => Some(Self::Stage),
            "torrent" => Some(Self::Torrent),
            "tracker" => Some(Self::Tracker),
            _ => None,
        }
    }
}

/// The ids a [`UsageConsumerKind::Job`] row is kept under.
pub const JOBS_FINISHED: &str = "finished";
pub const JOBS_REPAIRED: &str = "repaired";

/// What one consumer used over some span.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageCounts {
    /// Downloaded: body bytes for a server or category, bytes from peers for
    /// a torrent or tracker.
    pub bytes: u64,
    pub articles_tried: u64,
    pub articles_succeeded: u64,
    pub down_seconds: u64,
    /// Sent to peers, for a torrent or tracker.
    pub bytes_uploaded: u64,
    /// Jobs for a job row, timed runs for a stage.
    pub events: u64,
    /// A stage's time, or a torrent's or tracker's seeding time, in
    /// milliseconds; torrents seeding at once each add their own.
    pub duration_ms: u64,
}

impl UsageCounts {
    pub fn add(&mut self, other: &Self) {
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.articles_tried = self.articles_tried.saturating_add(other.articles_tried);
        self.articles_succeeded = self
            .articles_succeeded
            .saturating_add(other.articles_succeeded);
        self.down_seconds = self.down_seconds.saturating_add(other.down_seconds);
        self.bytes_uploaded = self.bytes_uploaded.saturating_add(other.bytes_uploaded);
        self.events = self.events.saturating_add(other.events);
        self.duration_ms = self.duration_ms.saturating_add(other.duration_ms);
    }

    pub const fn is_zero(&self) -> bool {
        self.bytes == 0
            && self.articles_tried == 0
            && self.articles_succeeded == 0
            && self.down_seconds == 0
            && self.bytes_uploaded == 0
            && self.events == 0
            && self.duration_ms == 0
    }
}

/// One consumer's use on one local date.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageDayRow {
    /// `None` for the folded row of days past retention.
    pub day: Option<NaiveDate>,
    pub kind: String,
    pub consumer_id: String,
    pub counts: UsageCounts,
}

/// A consumer's whole kept history, summed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageConsumerTotal {
    pub kind: String,
    pub consumer_id: String,
    pub counts: UsageCounts,
}

/// Finished jobs by how they ended.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FinishedJobCounts {
    pub complete: u32,
    pub partial: u32,
    pub failed: u32,
}

// ---------------------------------------------------------------------------
// Ranges
// ---------------------------------------------------------------------------

/// A span of local dates a reader asks about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsagePeriod {
    Today,
    /// Monday to today.
    Week,
    Month,
    Year,
    All,
    /// Both ends included.
    Custom {
        from: NaiveDate,
        to: NaiveDate,
    },
}

/// The first and last date of `period`, both included. `None` as the first
/// date is every kept day.
pub fn period_bounds(period: UsagePeriod, today: NaiveDate) -> (Option<NaiveDate>, NaiveDate) {
    match period {
        UsagePeriod::Today => (Some(today), today),
        UsagePeriod::Week => (Some(week_start(today)), today),
        UsagePeriod::Month => (today.with_day(1), today),
        UsagePeriod::Year => (today.with_ordinal(1), today),
        UsagePeriod::All => (None, today),
        UsagePeriod::Custom { from, to } => {
            if from <= to {
                (Some(from), to)
            } else {
                (Some(to), from)
            }
        }
    }
}

pub fn week_start(today: NaiveDate) -> NaiveDate {
    today
        .checked_sub_days(Days::new(u64::from(today.weekday().num_days_from_monday())))
        .unwrap_or(today)
}

/// The seconds a local date really has: 86,400, or an hour either way on
/// the days the clocks change.
pub fn seconds_in_day(day: NaiveDate) -> u64 {
    let start = |date: NaiveDate| {
        date.and_hms_opt(0, 0, 0)
            .and_then(|midnight| Local.from_local_datetime(&midnight).earliest())
            .map(|moment| moment.timestamp())
    };
    match (start(day), day.succ_opt().and_then(start)) {
        (Some(from), Some(to)) if to > from => u64::try_from(to - from).unwrap_or(86_400),
        _ => 86_400,
    }
}

fn retention_cutoff(today: NaiveDate) -> NaiveDate {
    today
        .checked_sub_days(Days::new(USAGE_RETENTION_DAYS))
        .unwrap_or(today)
}

/// One consumer's use over each standard span and the asked range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageConsumerSummary {
    pub kind: String,
    pub consumer_id: String,
    pub today: UsageCounts,
    pub week: UsageCounts,
    pub month: UsageCounts,
    pub year: UsageCounts,
    pub all_time: UsageCounts,
    pub range: UsageCounts,
    /// Every date of the range in order, days with nothing at zero.
    pub days: Vec<(NaiveDate, UsageCounts)>,
}

impl UsageConsumerSummary {
    fn empty(kind: &str, consumer_id: &str) -> Self {
        Self {
            kind: kind.to_string(),
            consumer_id: consumer_id.to_string(),
            today: UsageCounts::default(),
            week: UsageCounts::default(),
            month: UsageCounts::default(),
            year: UsageCounts::default(),
            all_time: UsageCounts::default(),
            range: UsageCounts::default(),
            days: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSummary {
    pub today: NaiveDate,
    pub range_start: NaiveDate,
    pub range_end: NaiveDate,
    /// The sum over the consumers of `total_kind`.
    pub total: UsageConsumerSummary,
    pub consumers: Vec<UsageConsumerSummary>,
}

/// What a range read needs from the database: daily rows from this date on
/// (`None` is every kept row), and the totals over everything kept.
pub fn summary_read_from(period: UsagePeriod, today: NaiveDate) -> Option<NaiveDate> {
    let (start, _) = period_bounds(period, today);
    let start = start?;
    let year = today.with_ordinal(1).unwrap_or(today);
    Some(start.min(year).min(week_start(today)))
}

/// Sum daily rows into each consumer's standard spans and the asked range.
///
/// `rows` must cover every date from [`summary_read_from`] on; `all_time` is
/// every kept row summed per consumer. The total sums the consumers of
/// `total_kind`, so servers and categories, which count the same bytes
/// twice over, are never added together.
pub fn summarize_usage(
    rows: &[UsageDayRow],
    all_time: &[UsageConsumerTotal],
    today: NaiveDate,
    period: UsagePeriod,
    total_kind: UsageConsumerKind,
) -> UsageSummary {
    let (start, end) = period_bounds(period, today);
    let earliest = rows.iter().filter_map(|row| row.day).min();
    let range_start = start.or(earliest).unwrap_or(today).min(end);
    let week = week_start(today);
    let month = today.with_day(1).unwrap_or(today);
    let year = today.with_ordinal(1).unwrap_or(today);

    let mut consumers: BTreeMap<(String, String), UsageConsumerSummary> = BTreeMap::new();
    let mut by_day: BTreeMap<(String, String), BTreeMap<NaiveDate, UsageCounts>> = BTreeMap::new();
    for total in all_time {
        consumers
            .entry((total.kind.clone(), total.consumer_id.clone()))
            .or_insert_with(|| UsageConsumerSummary::empty(&total.kind, &total.consumer_id))
            .all_time
            .add(&total.counts);
    }
    for row in rows {
        let Some(day) = row.day else {
            // Folded days sit before every date: only a range with no start
            // reaches back to them.
            if start.is_none() {
                consumers
                    .entry((row.kind.clone(), row.consumer_id.clone()))
                    .or_insert_with(|| UsageConsumerSummary::empty(&row.kind, &row.consumer_id))
                    .range
                    .add(&row.counts);
            }
            continue;
        };
        let key = (row.kind.clone(), row.consumer_id.clone());
        let summary = consumers
            .entry(key.clone())
            .or_insert_with(|| UsageConsumerSummary::empty(&row.kind, &row.consumer_id));
        if day == today {
            summary.today.add(&row.counts);
        }
        if day >= week && day <= today {
            summary.week.add(&row.counts);
        }
        if day >= month && day <= today {
            summary.month.add(&row.counts);
        }
        if day >= year && day <= today {
            summary.year.add(&row.counts);
        }
        if day >= range_start && day <= end {
            summary.range.add(&row.counts);
            by_day
                .entry(key)
                .or_default()
                .entry(day)
                .or_default()
                .add(&row.counts);
        }
    }

    let dates: Vec<NaiveDate> = range_start
        .iter_days()
        .take_while(|day| *day <= end)
        .collect();
    let mut total = UsageConsumerSummary::empty(total_kind.as_str(), "");
    total.days = dates
        .iter()
        .map(|day| (*day, UsageCounts::default()))
        .collect();
    let consumers = consumers
        .into_iter()
        .map(|(key, mut summary)| {
            let days = by_day.remove(&key).unwrap_or_default();
            summary.days = dates
                .iter()
                .map(|day| (*day, days.get(day).copied().unwrap_or_default()))
                .collect();
            if summary.kind == total_kind.as_str() {
                total.today.add(&summary.today);
                total.week.add(&summary.week);
                total.month.add(&summary.month);
                total.year.add(&summary.year);
                total.all_time.add(&summary.all_time);
                total.range.add(&summary.range);
                for ((_, sum), (_, counts)) in total.days.iter_mut().zip(&summary.days) {
                    sum.add(counts);
                }
            }
            summary
        })
        .collect();

    UsageSummary {
        today,
        range_start,
        range_end: end,
        total,
        consumers,
    }
}

// ---------------------------------------------------------------------------
// Recorder
// ---------------------------------------------------------------------------

/// What a server is doing, as the recorder sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerStateReading {
    /// `healthy`, `degraded`, `cooling_down` or `disabled`.
    pub state: &'static str,
    /// Whether it is out of use: disabled, or cooling down after a transport
    /// fault. A short hold for capacity is not down.
    pub down: bool,
}

/// One server's running totals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerUsageReading {
    pub server_id: u32,
    /// Body bytes fetched over the server's whole kept life, when the
    /// server has a byte count to read. A missing count leaves the server's
    /// bytes where they were.
    pub bytes: Option<u64>,
    /// Article attempts that reached the server since the process started.
    pub articles_tried: u64,
    pub articles_succeeded: u64,
    pub state: Option<ServerStateReading>,
}

/// One stage's timed runs since the process started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StageReading {
    pub stage: &'static str,
    pub runs: u64,
    pub total_ms: u64,
}

/// One torrent's seeding ledger, as the torrent service last published it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TorrentUsageReading {
    pub job_id: u64,
    /// The tracker hosts it lists, each once.
    pub trackers: Vec<String>,
    /// Over the torrent's whole kept life.
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub seeding_seconds: u64,
}

/// Every running total the ledger reads, at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageReading {
    pub servers: Vec<ServerUsageReading>,
    /// Bytes per category since the process started.
    pub categories: Vec<(String, u64)>,
    /// NZB jobs finished, and of those repaired, since the process started.
    pub jobs_finished: u64,
    pub jobs_repaired: u64,
    pub stages: Vec<StageReading>,
    pub torrents: Vec<TorrentUsageReading>,
}

#[derive(Debug, Clone, Copy)]
struct ServerBaseline {
    bytes: Option<u64>,
    articles_tried: u64,
    articles_succeeded: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct TorrentBaseline {
    downloaded_bytes: u64,
    uploaded_bytes: u64,
    seeding_seconds: u64,
}

impl TorrentBaseline {
    fn of(torrent: &TorrentUsageReading) -> Self {
        Self {
            downloaded_bytes: torrent.downloaded_bytes,
            uploaded_bytes: torrent.uploaded_bytes,
            seeding_seconds: torrent.seeding_seconds,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct StateMark {
    state: &'static str,
    down: bool,
    since_epoch_sec: i64,
}

type PendingKey = (NaiveDate, UsageConsumerKind, String);

#[derive(Debug, Default)]
struct RecorderState {
    primed: bool,
    servers: HashMap<u32, ServerBaseline>,
    categories: HashMap<String, u64>,
    jobs_finished: u64,
    jobs_repaired: u64,
    stages: HashMap<&'static str, (u64, u64)>,
    torrents: HashMap<u64, TorrentBaseline>,
    states: HashMap<u32, StateMark>,
    last_tick_epoch_sec: Option<i64>,
    pending: HashMap<PendingKey, UsageCounts>,
    pruned_through: Option<NaiveDate>,
}

impl RecorderState {
    fn add_pending(
        &mut self,
        day: NaiveDate,
        kind: UsageConsumerKind,
        id: &str,
        counts: UsageCounts,
    ) {
        self.pending
            .entry((day, kind, id.to_string()))
            .or_default()
            .add(&counts);
    }
}

/// A batch ready to write: the rows to add, and the cutoff to fold older
/// days at when the date has moved since the last batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageFlush {
    pub rows: Vec<UsageDayRow>,
    pub fold_before: Option<NaiveDate>,
}

impl UsageFlush {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.fold_before.is_none()
    }
}

/// Turns running totals into per-day differences.
///
/// Nothing here runs on the download path: the owner calls [`observe`]
/// every few seconds with a fresh reading.
///
/// [`observe`]: UsageRecorder::observe
#[derive(Debug, Default)]
pub struct UsageRecorder {
    state: Mutex<RecorderState>,
}

impl UsageRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RecorderState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Take `reading` as the starting point: nothing in it is counted.
    ///
    /// Called once before downloads start, so a server's kept lifetime bytes
    /// are not read as one day's download.
    pub fn prime(&self, reading: &UsageReading, now_epoch_sec: i64) {
        let mut state = self.lock();
        for server in &reading.servers {
            state.servers.insert(
                server.server_id,
                ServerBaseline {
                    bytes: server.bytes,
                    articles_tried: server.articles_tried,
                    articles_succeeded: server.articles_succeeded,
                },
            );
            if let Some(reading) = server.state {
                state.states.insert(
                    server.server_id,
                    StateMark {
                        state: reading.state,
                        down: reading.down,
                        since_epoch_sec: now_epoch_sec,
                    },
                );
            }
        }
        for (category, bytes) in &reading.categories {
            state.categories.insert(category.clone(), *bytes);
        }
        state.jobs_finished = reading.jobs_finished;
        state.jobs_repaired = reading.jobs_repaired;
        for stage in &reading.stages {
            state
                .stages
                .insert(stage.stage, (stage.runs, stage.total_ms));
        }
        state.torrents = reading
            .torrents
            .iter()
            .map(|torrent| (torrent.job_id, TorrentBaseline::of(torrent)))
            .collect();
        state.last_tick_epoch_sec = Some(now_epoch_sec);
        state.primed = true;
    }

    /// Count what moved since the last reading against `today`.
    ///
    /// A total that went backwards restarts from where it is now. A server
    /// first seen after priming counts its articles from zero, since those
    /// counters start with the process, but its bytes from now, since those
    /// carry its kept lifetime. A server down at this reading and the last
    /// counts the time between them as down.
    pub fn observe(&self, reading: &UsageReading, today: NaiveDate, now_epoch_sec: i64) {
        let mut state = self.lock();
        if !state.primed {
            drop(state);
            self.prime(reading, now_epoch_sec);
            return;
        }
        let elapsed = state.last_tick_epoch_sec.map_or(0, |last| {
            now_epoch_sec
                .saturating_sub(last)
                .clamp(0, USAGE_MAX_DOWN_STEP_SECS)
        });
        let day_seconds = seconds_in_day(today);
        state.last_tick_epoch_sec = Some(now_epoch_sec);

        for server in &reading.servers {
            let baseline =
                state
                    .servers
                    .get(&server.server_id)
                    .copied()
                    .unwrap_or(ServerBaseline {
                        bytes: server.bytes,
                        articles_tried: 0,
                        articles_succeeded: 0,
                    });
            let mut counts = UsageCounts {
                bytes: match (baseline.bytes, server.bytes) {
                    (Some(before), Some(now)) => forward(before, now),
                    _ => 0,
                },
                articles_tried: forward(baseline.articles_tried, server.articles_tried),
                articles_succeeded: forward(baseline.articles_succeeded, server.articles_succeeded),
                ..UsageCounts::default()
            };
            state.servers.insert(
                server.server_id,
                ServerBaseline {
                    bytes: server.bytes.or(baseline.bytes),
                    articles_tried: server.articles_tried,
                    articles_succeeded: server.articles_succeeded,
                },
            );
            if let Some(now) = server.state {
                let previous = state.states.get(&server.server_id).copied();
                if now.down && previous.is_some_and(|mark| mark.down) {
                    counts.down_seconds = elapsed as u64;
                }
                let since = match previous {
                    Some(mark) if mark.state == now.state => mark.since_epoch_sec,
                    _ => now_epoch_sec,
                };
                state.states.insert(
                    server.server_id,
                    StateMark {
                        state: now.state,
                        down: now.down,
                        since_epoch_sec: since,
                    },
                );
            }
            if !counts.is_zero() {
                let pending = state
                    .pending
                    .entry((
                        today,
                        UsageConsumerKind::Server,
                        server.server_id.to_string(),
                    ))
                    .or_default();
                pending.add(&counts);
                pending.down_seconds = pending.down_seconds.min(day_seconds);
            }
        }

        for (category, bytes) in &reading.categories {
            let baseline = state.categories.get(category).copied().unwrap_or(0);
            state.categories.insert(category.clone(), *bytes);
            let moved = forward(baseline, *bytes);
            if moved > 0 {
                state
                    .pending
                    .entry((today, UsageConsumerKind::Category, category.clone()))
                    .or_default()
                    .add(&UsageCounts {
                        bytes: moved,
                        ..UsageCounts::default()
                    });
            }
        }

        // Job and stage counters start with the process, so a stage first
        // seen after priming counts from zero.
        let jobs = [
            (
                JOBS_FINISHED,
                forward(state.jobs_finished, reading.jobs_finished),
            ),
            (
                JOBS_REPAIRED,
                forward(state.jobs_repaired, reading.jobs_repaired),
            ),
        ];
        state.jobs_finished = reading.jobs_finished;
        state.jobs_repaired = reading.jobs_repaired;
        for (id, moved) in jobs {
            if moved > 0 {
                state.add_pending(
                    today,
                    UsageConsumerKind::Job,
                    id,
                    UsageCounts {
                        events: moved,
                        ..UsageCounts::default()
                    },
                );
            }
        }
        for stage in &reading.stages {
            let (runs, total_ms) = state.stages.get(stage.stage).copied().unwrap_or((0, 0));
            state
                .stages
                .insert(stage.stage, (stage.runs, stage.total_ms));
            let counts = UsageCounts {
                events: forward(runs, stage.runs),
                duration_ms: forward(total_ms, stage.total_ms),
                ..UsageCounts::default()
            };
            if !counts.is_zero() {
                state.add_pending(today, UsageConsumerKind::Stage, stage.stage, counts);
            }
        }

        // A torrent's ledger carries its kept lifetime, so a torrent first
        // seen counts from now. A torrent gone from the reading is dropped.
        let mut torrents = HashMap::with_capacity(reading.torrents.len());
        let mut whole = UsageCounts::default();
        for torrent in &reading.torrents {
            let now = TorrentBaseline::of(torrent);
            let before = state.torrents.get(&torrent.job_id).copied().unwrap_or(now);
            torrents.insert(torrent.job_id, now);
            let counts = UsageCounts {
                bytes: forward(before.downloaded_bytes, now.downloaded_bytes),
                bytes_uploaded: forward(before.uploaded_bytes, now.uploaded_bytes),
                duration_ms: forward(before.seeding_seconds, now.seeding_seconds)
                    .saturating_mul(1000),
                ..UsageCounts::default()
            };
            if counts.is_zero() {
                continue;
            }
            whole.add(&counts);
            if torrent.trackers.is_empty() {
                state.add_pending(today, UsageConsumerKind::Tracker, "", counts);
            }
            for host in &torrent.trackers {
                state.add_pending(today, UsageConsumerKind::Tracker, host, counts);
            }
        }
        state.torrents = torrents;
        if !whole.is_zero() {
            state.add_pending(today, UsageConsumerKind::Torrent, "", whole);
        }
    }

    /// The counts waiting to be written, taken out, and the fold due on a
    /// new date. Hand the batch back with [`restore`] if the write fails.
    ///
    /// [`restore`]: UsageRecorder::restore
    pub fn take(&self, today: NaiveDate) -> UsageFlush {
        let mut state = self.lock();
        let rows = state
            .pending
            .drain()
            .map(|((day, kind, consumer_id), counts)| UsageDayRow {
                day: Some(day),
                kind: kind.as_str().to_string(),
                consumer_id,
                counts,
            })
            .collect();
        let fold_before = if state.pruned_through == Some(today) {
            None
        } else {
            state.pruned_through = Some(today);
            Some(retention_cutoff(today))
        };
        UsageFlush { rows, fold_before }
    }

    /// Put back a batch whose write failed, to go with the next one.
    pub fn restore(&self, flush: UsageFlush) {
        let mut state = self.lock();
        for row in flush.rows {
            let (Some(day), Some(kind)) = (row.day, UsageConsumerKind::parse(&row.kind)) else {
                continue;
            };
            state
                .pending
                .entry((day, kind, row.consumer_id))
                .or_default()
                .add(&row.counts);
        }
        if flush.fold_before.is_some() {
            state.pruned_through = None;
        }
    }

    /// A server's state as last read and when it was first read so.
    pub fn server_state_since(&self, server_id: u32) -> Option<(&'static str, i64)> {
        self.lock()
            .states
            .get(&server_id)
            .map(|mark| (mark.state, mark.since_epoch_sec))
    }
}

const fn forward(baseline: u64, now: u64) -> u64 {
    now.saturating_sub(baseline)
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

fn day_text(day: Option<NaiveDate>) -> String {
    day.map_or_else(
        || EARLIER_DAY.to_string(),
        |day| day.format(DAY_FORMAT).to_string(),
    )
}

fn parse_day(text: &str) -> Result<Option<NaiveDate>, StateError> {
    if text == EARLIER_DAY {
        return Ok(None);
    }
    NaiveDate::parse_from_str(text, DAY_FORMAT)
        .map(Some)
        .map_err(|error| StateError::Database(format!("bad usage day {text:?}: {error}")))
}

fn to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn counts_from_row(row: &SqlRow) -> Result<UsageCounts, StateError> {
    Ok(UsageCounts {
        bytes: to_u64(row.i64("bytes_downloaded")?),
        articles_tried: to_u64(row.i64("articles_tried")?),
        articles_succeeded: to_u64(row.i64("articles_succeeded")?),
        down_seconds: to_u64(row.i64("down_seconds")?),
        bytes_uploaded: to_u64(row.i64("bytes_uploaded")?),
        events: to_u64(row.i64("events")?),
        duration_ms: to_u64(row.i64("duration_ms")?),
    })
}

const ADD_USAGE_DAY: &str = "INSERT INTO usage_daily
        (day, consumer_kind, consumer_id, bytes_downloaded,
         articles_tried, articles_succeeded, down_seconds,
         bytes_uploaded, events, duration_ms)
     VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})
     ON CONFLICT(day, consumer_kind, consumer_id) DO UPDATE SET
        bytes_downloaded = usage_daily.bytes_downloaded + excluded.bytes_downloaded,
        articles_tried = usage_daily.articles_tried + excluded.articles_tried,
        articles_succeeded = usage_daily.articles_succeeded + excluded.articles_succeeded,
        bytes_uploaded = usage_daily.bytes_uploaded + excluded.bytes_uploaded,
        events = usage_daily.events + excluded.events,
        duration_ms = usage_daily.duration_ms + excluded.duration_ms,
        down_seconds = CASE
            WHEN usage_daily.down_seconds + excluded.down_seconds > {}
            THEN {}
            ELSE usage_daily.down_seconds + excluded.down_seconds
        END";

// Rows past retention add into the consumer's earlier row, then go.
const FOLD_OLD_DAYS: &str = "INSERT INTO usage_daily
        (day, consumer_kind, consumer_id, bytes_downloaded,
         articles_tried, articles_succeeded, down_seconds,
         bytes_uploaded, events, duration_ms)
     SELECT {}, consumer_kind, consumer_id,
            CAST(SUM(bytes_downloaded) AS BIGINT),
            CAST(SUM(articles_tried) AS BIGINT),
            CAST(SUM(articles_succeeded) AS BIGINT),
            CAST(SUM(down_seconds) AS BIGINT),
            CAST(SUM(bytes_uploaded) AS BIGINT),
            CAST(SUM(events) AS BIGINT),
            CAST(SUM(duration_ms) AS BIGINT)
       FROM usage_daily
      WHERE day < {} AND day <> {}
      GROUP BY consumer_kind, consumer_id
     ON CONFLICT(day, consumer_kind, consumer_id) DO UPDATE SET
        bytes_downloaded = usage_daily.bytes_downloaded + excluded.bytes_downloaded,
        articles_tried = usage_daily.articles_tried + excluded.articles_tried,
        articles_succeeded = usage_daily.articles_succeeded + excluded.articles_succeeded,
        down_seconds = usage_daily.down_seconds + excluded.down_seconds,
        bytes_uploaded = usage_daily.bytes_uploaded + excluded.bytes_uploaded,
        events = usage_daily.events + excluded.events,
        duration_ms = usage_daily.duration_ms + excluded.duration_ms";

const DELETE_OLD_DAYS: &str = "DELETE FROM usage_daily WHERE day < {} AND day <> {}";

impl Database {
    /// Write a batch: add each row onto its day, and fold days older than
    /// the batch's cutoff. One transaction whatever the row count.
    pub fn write_usage_flush(&self, flush: &UsageFlush) -> Result<(), StateError> {
        if flush.is_empty() {
            return Ok(());
        }
        let mut statements: Vec<(&'static str, Vec<SqlArg>)> = flush
            .rows
            .iter()
            .map(|row| {
                // A day never holds more down time than it has seconds.
                let day_cap = SqlArg::I64(to_i64(row.day.map_or(u64::MAX, seconds_in_day)));
                (
                    ADD_USAGE_DAY,
                    vec![
                        SqlArg::Text(day_text(row.day)),
                        SqlArg::Text(row.kind.clone()),
                        SqlArg::Text(row.consumer_id.clone()),
                        SqlArg::I64(to_i64(row.counts.bytes)),
                        SqlArg::I64(to_i64(row.counts.articles_tried)),
                        SqlArg::I64(to_i64(row.counts.articles_succeeded)),
                        SqlArg::I64(to_i64(row.counts.down_seconds)),
                        SqlArg::I64(to_i64(row.counts.bytes_uploaded)),
                        SqlArg::I64(to_i64(row.counts.events)),
                        SqlArg::I64(to_i64(row.counts.duration_ms)),
                        day_cap.clone(),
                        day_cap,
                    ],
                )
            })
            .collect();
        if let Some(cutoff) = flush.fold_before {
            let cutoff = day_text(Some(cutoff));
            statements.push((
                FOLD_OLD_DAYS,
                vec![
                    SqlArg::Text(EARLIER_DAY.to_string()),
                    SqlArg::Text(cutoff.clone()),
                    SqlArg::Text(EARLIER_DAY.to_string()),
                ],
            ));
            statements.push((
                DELETE_OLD_DAYS,
                vec![SqlArg::Text(cutoff), SqlArg::Text(EARLIER_DAY.to_string())],
            ));
        }
        let statements = std::sync::Arc::new(statements);
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "write_usage_flush", |tx| {
                let statements = statements.clone();
                Box::pin(async move {
                    for (statement, args) in statements.iter() {
                        tx.execute(statement, args).await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    /// Daily rows from `from` (every kept row when `None`) through `to`,
    /// for one kind or all.
    pub fn read_usage_days(
        &self,
        from: Option<NaiveDate>,
        to: NaiveDate,
        kind: Option<UsageConsumerKind>,
    ) -> Result<Vec<UsageDayRow>, StateError> {
        let mut sql = String::from(
            "SELECT day, consumer_kind, consumer_id, bytes_downloaded,
                    articles_tried, articles_succeeded, down_seconds,
                    bytes_uploaded, events, duration_ms
               FROM usage_daily
              WHERE day <= {}",
        );
        let mut args = vec![SqlArg::Text(day_text(Some(to)))];
        if let Some(from) = from {
            sql.push_str(" AND day >= {}");
            args.push(SqlArg::Text(day_text(Some(from))));
        }
        if let Some(kind) = kind {
            sql.push_str(" AND consumer_kind = {}");
            args.push(SqlArg::Text(kind.as_str().to_string()));
        }
        sql.push_str(" ORDER BY day, consumer_kind, consumer_id");
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args)
                .await?
                .iter()
                .map(|row| {
                    Ok(UsageDayRow {
                        day: parse_day(&row.text("day")?)?,
                        kind: row.text("consumer_kind")?,
                        consumer_id: row.text("consumer_id")?,
                        counts: counts_from_row(row)?,
                    })
                })
                .collect()
        })
    }

    /// Every kept row summed per consumer, for one kind or all.
    pub fn read_usage_totals(
        &self,
        kind: Option<UsageConsumerKind>,
    ) -> Result<Vec<UsageConsumerTotal>, StateError> {
        let mut sql = String::from(
            "SELECT consumer_kind, consumer_id,
                    CAST(SUM(bytes_downloaded) AS BIGINT) AS bytes_downloaded,
                    CAST(SUM(articles_tried) AS BIGINT) AS articles_tried,
                    CAST(SUM(articles_succeeded) AS BIGINT) AS articles_succeeded,
                    CAST(SUM(down_seconds) AS BIGINT) AS down_seconds,
                    CAST(SUM(bytes_uploaded) AS BIGINT) AS bytes_uploaded,
                    CAST(SUM(events) AS BIGINT) AS events,
                    CAST(SUM(duration_ms) AS BIGINT) AS duration_ms
               FROM usage_daily",
        );
        let mut args = Vec::new();
        if let Some(kind) = kind {
            sql.push_str(" WHERE consumer_kind = {}");
            args.push(SqlArg::Text(kind.as_str().to_string()));
        }
        sql.push_str(" GROUP BY consumer_kind, consumer_id ORDER BY consumer_kind, consumer_id");
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args)
                .await?
                .iter()
                .map(|row| {
                    Ok(UsageConsumerTotal {
                        kind: row.text("consumer_kind")?,
                        consumer_id: row.text("consumer_id")?,
                        counts: counts_from_row(row)?,
                    })
                })
                .collect()
        })
    }

    /// Jobs that finished in `[since, until)`, in epoch seconds, by how they
    /// ended. History shows a cancelled job as failed, so it counts as one; a
    /// paused row has not finished and is not counted.
    pub fn count_finished_jobs(
        &self,
        since_epoch_sec: Option<i64>,
        until_epoch_sec: i64,
    ) -> Result<FinishedJobCounts, StateError> {
        let mut sql = String::from(
            "SELECT LOWER(status) AS status, COUNT(*) AS count
               FROM job_history
              WHERE completed_at < {}",
        );
        let mut args = vec![SqlArg::I64(until_epoch_sec)];
        if let Some(since) = since_epoch_sec {
            sql.push_str(" AND completed_at >= {}");
            args.push(SqlArg::I64(since));
        }
        sql.push_str(" GROUP BY LOWER(status)");
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut counts = FinishedJobCounts::default();
            for row in SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await? {
                let count = u32::try_from(row.i64("count")?).unwrap_or(u32::MAX);
                match row.text("status")?.as_str() {
                    "complete" => counts.complete = counts.complete.saturating_add(count),
                    "partial" => counts.partial = counts.partial.saturating_add(count),
                    "paused" => {}
                    _ => counts.failed = counts.failed.saturating_add(count),
                }
            }
            Ok(counts)
        })
    }

    /// The recovery the NZB jobs that finished in `[since, until)` posted,
    /// and how much of it they fetched, from their history rows.
    pub fn sum_recovery_bytes(
        &self,
        since_epoch_sec: Option<i64>,
        until_epoch_sec: i64,
    ) -> Result<RecoveryBytes, StateError> {
        let (sql, args) = finished_nzb_jobs(
            "SELECT CAST(COALESCE(SUM(optional_recovery_bytes), 0) AS BIGINT) AS available,
                    CAST(COALESCE(SUM(optional_recovery_downloaded_bytes), 0) AS BIGINT) AS fetched
               FROM job_history",
            since_epoch_sec,
            until_epoch_sec,
        );
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            let Some(row) = rows.first() else {
                return Ok(RecoveryBytes::default());
            };
            Ok(RecoveryBytes {
                available: to_u64(row.i64("available")?),
                fetched: to_u64(row.i64("fetched")?),
            })
        })
    }

    /// The NZB jobs that failed in `[since, until)`, by the cause their
    /// history row keeps; the empty cause is a row that keeps none.
    pub fn count_failures_by_cause(
        &self,
        since_epoch_sec: Option<i64>,
        until_epoch_sec: i64,
    ) -> Result<Vec<(String, u64)>, StateError> {
        let (mut sql, args) = finished_nzb_jobs(
            "SELECT COALESCE(failure_kind, '') AS cause, COUNT(*) AS count
               FROM job_history",
            since_epoch_sec,
            until_epoch_sec,
        );
        sql.push_str(
            " AND LOWER(status) = 'failed' GROUP BY COALESCE(failure_kind, '') ORDER BY cause",
        );
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args)
                .await?
                .iter()
                .map(|row| Ok((row.text("cause")?, to_u64(row.i64("count")?))))
                .collect()
        })
    }
}

/// What a span's finished jobs posted as recovery and fetched of it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryBytes {
    pub available: u64,
    pub fetched: u64,
}

// `select` over the NZB history rows that finished complete, partial or
// failed in `[since, until)`; a cancelled job never finished.
fn finished_nzb_jobs(
    select: &str,
    since_epoch_sec: Option<i64>,
    until_epoch_sec: i64,
) -> (String, Vec<SqlArg>) {
    let mut sql = format!(
        "{select}
          WHERE protocol = 'nzb'
            AND LOWER(status) IN ('complete', 'partial', 'failed')
            AND completed_at < {{}}"
    );
    let mut args = vec![SqlArg::I64(until_epoch_sec)];
    if let Some(since) = since_epoch_sec {
        sql.push_str(" AND completed_at >= {}");
        args.push(SqlArg::I64(since));
    }
    (sql, args)
}
