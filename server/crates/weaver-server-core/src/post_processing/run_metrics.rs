// Process-local counters for every script run, whatever started it.
//
// Recording a run costs relaxed atomic adds. Kinds, statuses, adapters and
// lanes are closed sets, held in fixed arrays indexed by their codes. The
// per-script figures sit in a registry keyed by the name the operator gave
// the script: the first run of a script adds its slots under a write lock,
// and every later run takes a read lock to find them. The name itself is
// only read again at scrape time.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use super::model::{
    PostProcessingSummary, ScriptAdapter, ScriptEventLabel, ScriptResult, ScriptStatus,
};
use crate::operations::{AtomicHistogram, HistogramSnapshot};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};
use crate::persistence::{Database, StateError};

// What started a run. A run nothing waits for during a job's
// post-processing is `Background`; every other run is named by its event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RunKind {
    PostProcessing,
    Background,
    Queue,
    Scan,
    Scheduler,
    Feed,
}

impl RunKind {
    pub const ALL: [Self; 6] = [
        Self::PostProcessing,
        Self::Background,
        Self::Queue,
        Self::Scan,
        Self::Scheduler,
        Self::Feed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PostProcessing => "post_processing",
            Self::Background => "background",
            Self::Queue => "queue",
            Self::Scan => "scan",
            Self::Scheduler => "scheduler",
            Self::Feed => "feed",
        }
    }

    pub fn of(event: &ScriptEventLabel, background: bool) -> Self {
        match event {
            ScriptEventLabel::PostProcessing if background => Self::Background,
            ScriptEventLabel::PostProcessing => Self::PostProcessing,
            ScriptEventLabel::Queue(_) => Self::Queue,
            ScriptEventLabel::Scan => Self::Scan,
            ScriptEventLabel::Scheduler(_) => Self::Scheduler,
            ScriptEventLabel::Feed(_) => Self::Feed,
        }
    }
}

// How a run ended: every script status, plus a run that had its slot but
// could not be launched and a run cut off when weaver stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RunStatus {
    Succeeded,
    Skipped,
    Warning,
    Failed,
    TimedOut,
    Cancelled,
    NotStarted,
    Interrupted,
}

impl RunStatus {
    pub const ALL: [Self; 8] = [
        Self::Succeeded,
        Self::Skipped,
        Self::Warning,
        Self::Failed,
        Self::TimedOut,
        Self::Cancelled,
        Self::NotStarted,
        Self::Interrupted,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Skipped => "skipped",
            Self::Warning => "warning",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::NotStarted => "not_started",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn of(status: ScriptStatus, started: bool) -> Self {
        if !started {
            return Self::NotStarted;
        }
        match status {
            ScriptStatus::Succeeded => Self::Succeeded,
            ScriptStatus::Skipped => Self::Skipped,
            ScriptStatus::Warning => Self::Warning,
            ScriptStatus::Failed => Self::Failed,
            ScriptStatus::TimedOut => Self::TimedOut,
            ScriptStatus::Cancelled => Self::Cancelled,
        }
    }
}

pub const ADAPTERS: [ScriptAdapter; 2] = [ScriptAdapter::Sabnzbd, ScriptAdapter::Nzbget];

fn adapter_index(adapter: ScriptAdapter) -> usize {
    match adapter {
        ScriptAdapter::Sabnzbd => 0,
        ScriptAdapter::Nzbget => 1,
    }
}

pub const SUMMARIES: [PostProcessingSummary; 7] = [
    PostProcessingSummary::NotRun,
    PostProcessingSummary::Running,
    PostProcessingSummary::Succeeded,
    PostProcessingSummary::Warning,
    PostProcessingSummary::Failed,
    PostProcessingSummary::Cancelled,
    PostProcessingSummary::Interrupted,
];

fn summary_index(summary: PostProcessingSummary) -> usize {
    match summary {
        PostProcessingSummary::NotRun => 0,
        PostProcessingSummary::Running => 1,
        PostProcessingSummary::Succeeded => 2,
        PostProcessingSummary::Warning => 3,
        PostProcessingSummary::Failed => 4,
        PostProcessingSummary::Cancelled => 5,
        PostProcessingSummary::Interrupted => 6,
    }
}

// What retention did with a run's record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionAction {
    // The run was recorded.
    Kept,
    // The run was not recorded because its job, feed or instance was gone.
    Discarded,
    // An older run was deleted by the retention limits.
    Pruned,
}

impl RetentionAction {
    pub const ALL: [Self; 3] = [Self::Kept, Self::Discarded, Self::Pruned];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Kept => "kept",
            Self::Discarded => "discarded",
            Self::Pruned => "pruned",
        }
    }
}

// Script run duration, seconds: from a hook that returns at once to a long
// library scan or repair.
pub const SCRIPT_RUN_DURATION_BOUNDS: &[f64] = &[
    0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0,
];

const KINDS: usize = RunKind::ALL.len();
const STATUSES: usize = RunStatus::ALL.len();
const WAITED: usize = 2;

static STARTED: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static RUNNING: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static WAITING: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static SLOT_WAITS: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static REFUSALS: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static JOB_SUMMARIES: [AtomicU64; SUMMARIES.len()] = [const { AtomicU64::new(0) }; SUMMARIES.len()];
static INTERRUPTED_RECOVERED: AtomicU64 = AtomicU64::new(0);
static RETAINED: [AtomicU64; RetentionAction::ALL.len()] =
    [const { AtomicU64::new(0) }; RetentionAction::ALL.len()];

const NO_EXIT_CODE: i64 = i64::MIN;
const NO_STATUS: u8 = u8::MAX;

// Everything kept for one script.
struct ScriptSlots {
    runs: [[[[AtomicU64; STATUSES]; WAITED]; ADAPTERS.len()]; KINDS],
    // One bit per kind, adapter and waited combination the script has run
    // under, so each of those renders every status.
    seen: AtomicU32,
    duration: [[[AtomicHistogram; STATUSES]; WAITED]; KINDS],
    nonzero_exits: AtomicU64,
    last_exit_code: AtomicI64,
    last_duration_ms: AtomicU64,
    last_finished_ms: AtomicI64,
    last_status: AtomicU8,
    output_bytes: AtomicU64,
    stored_bytes: AtomicU64,
    truncations: AtomicU64,
    pruned: AtomicU64,
}

impl ScriptSlots {
    fn new() -> Self {
        Self {
            runs: [const {
                [const { [const { [const { AtomicU64::new(0) }; STATUSES] }; WAITED] };
                    ADAPTERS.len()]
            }; KINDS],
            seen: AtomicU32::new(0),
            duration: [const {
                [const { [const { AtomicHistogram::new(SCRIPT_RUN_DURATION_BOUNDS) }; STATUSES] };
                    WAITED]
            }; KINDS],
            nonzero_exits: AtomicU64::new(0),
            last_exit_code: AtomicI64::new(NO_EXIT_CODE),
            last_duration_ms: AtomicU64::new(0),
            last_finished_ms: AtomicI64::new(0),
            last_status: AtomicU8::new(NO_STATUS),
            output_bytes: AtomicU64::new(0),
            stored_bytes: AtomicU64::new(0),
            truncations: AtomicU64::new(0),
            pruned: AtomicU64::new(0),
        }
    }
}

static SCRIPTS: LazyLock<RwLock<HashMap<Arc<str>, Arc<ScriptSlots>>>> =
    LazyLock::new(Default::default);

// The slots for `script`, added on its first run.
fn slots(script: &str) -> Arc<ScriptSlots> {
    if let Some(slots) = SCRIPTS
        .read()
        .unwrap_or_else(|error| error.into_inner())
        .get(script)
    {
        return slots.clone();
    }
    SCRIPTS
        .write()
        .unwrap_or_else(|error| error.into_inner())
        .entry(Arc::from(script))
        .or_insert_with(|| Arc::new(ScriptSlots::new()))
        .clone()
}

// Counts a run as started and running until it is dropped.
pub(crate) struct RunningGuard(RunKind);

impl RunningGuard {
    pub(crate) fn enter(kind: RunKind) -> Self {
        STARTED[kind as usize].fetch_add(1, Ordering::Relaxed);
        RUNNING[kind as usize].fetch_add(1, Ordering::Relaxed);
        Self(kind)
    }
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        RUNNING[self.0 as usize].fetch_sub(1, Ordering::Relaxed);
    }
}

// Counts a run as waiting for a slot until it is dropped.
pub(crate) struct WaitingGuard(RunKind);

impl WaitingGuard {
    pub(crate) fn enter(kind: RunKind) -> Self {
        SLOT_WAITS[kind as usize].fetch_add(1, Ordering::Relaxed);
        WAITING[kind as usize].fetch_add(1, Ordering::Relaxed);
        Self(kind)
    }
}

impl Drop for WaitingGuard {
    fn drop(&mut self) {
        WAITING[self.0 as usize].fetch_sub(1, Ordering::Relaxed);
    }
}

// Await `slot`, counting a run of `kind` as waiting for it meanwhile.
pub(crate) async fn waiting<T>(kind: RunKind, slot: impl Future<Output = T>) -> T {
    let _waiting = WaitingGuard::enter(kind);
    slot.await
}

// Record a finished run under its script's name. `started` is false for a
// run that had its slot but could not be launched.
pub(crate) fn record_finished(result: &ScriptResult, started: bool) {
    record(result, RunStatus::of(result.status, started));
}

// Record a run that weaver stopped while it ran.
pub fn record_interrupted(result: &ScriptResult) {
    record(result, RunStatus::Interrupted);
}

fn record(result: &ScriptResult, status: RunStatus) {
    let slots = slots(result.label());
    let kind = RunKind::of(&result.event, result.background) as usize;
    let adapter = adapter_index(result.adapter);
    let waited = usize::from(!result.background);
    slots.runs[kind][adapter][waited][status as usize].fetch_add(1, Ordering::Relaxed);
    slots.seen.fetch_or(
        1 << ((kind * ADAPTERS.len() + adapter) * WAITED + waited),
        Ordering::Relaxed,
    );
    slots.duration[kind][waited][status as usize]
        .observe(Duration::from_millis(result.duration_ms));
    if let Some(code) = result.exit_code {
        slots
            .last_exit_code
            .store(i64::from(code), Ordering::Relaxed);
        if code != 0 {
            slots.nonzero_exits.fetch_add(1, Ordering::Relaxed);
        }
    }
    if result.output_truncated {
        slots.truncations.fetch_add(1, Ordering::Relaxed);
    }
    slots
        .last_duration_ms
        .store(result.duration_ms, Ordering::Relaxed);
    slots
        .last_finished_ms
        .store(result.finished_at_epoch_ms, Ordering::Relaxed);
    slots.last_status.store(status as u8, Ordering::Relaxed);
}

// What a script wrote, and what of it was kept after compression.
pub(crate) fn record_output(script: &str, written: u64, stored: u64) {
    let slots = slots(script);
    slots.output_bytes.fetch_add(written, Ordering::Relaxed);
    slots.stored_bytes.fetch_add(stored, Ordering::Relaxed);
}

pub(crate) fn record_refusal(kind: RunKind) {
    REFUSALS[kind as usize].fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_job_summary(summary: PostProcessingSummary) {
    record_job_summaries(summary, 1);
}

pub(crate) fn record_job_summaries(summary: PostProcessingSummary, jobs: u64) {
    JOB_SUMMARIES[summary_index(summary)].fetch_add(jobs, Ordering::Relaxed);
}

// A job whose pass weaver stopped in the middle of, run again for the
// scripts that had not started.
pub fn record_interrupted_recovered() {
    INTERRUPTED_RECOVERED.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_retention(action: RetentionAction, runs: u64) {
    RETAINED[action as usize].fetch_add(runs, Ordering::Relaxed);
}

// What one retention transaction did. Each attempt of the transaction
// starts it afresh, so a retried transaction is counted once, after it
// commits.
#[derive(Clone, Default)]
pub(crate) struct RetentionTally(Arc<RetentionCounts>);

#[derive(Default)]
pub(crate) struct RetentionCounts {
    actions: [AtomicU64; 3],
    pruned_scripts: std::sync::Mutex<Vec<String>>,
}

impl RetentionTally {
    pub(crate) fn reset(&self) {
        for action in RetentionAction::ALL {
            self.set(action, 0);
        }
        self.pruned_scripts().clear();
    }

    pub(crate) fn set(&self, action: RetentionAction, runs: u64) {
        self.0.actions[action as usize].store(runs, Ordering::Relaxed);
    }

    // The scripts whose runs the transaction pruned, one entry per run.
    pub(crate) fn pruned(&self, scripts: Vec<String>) {
        self.set(RetentionAction::Pruned, scripts.len() as u64);
        *self.pruned_scripts() = scripts;
    }

    fn pruned_scripts(&self) -> std::sync::MutexGuard<'_, Vec<String>> {
        self.0
            .pruned_scripts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn commit(&self) {
        for action in RetentionAction::ALL {
            record_retention(
                action,
                self.0.actions[action as usize].load(Ordering::Relaxed),
            );
        }
        for script in self.pruned_scripts().iter() {
            slots(script).pruned.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// The script each of `ids` was a run of, read before retention deletes them.
pub(crate) async fn pruned_scripts(
    tx: &mut SqlTx<'_>,
    ids: &[String],
) -> Result<Vec<String>, StateError> {
    let mut scripts = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(256) {
        let sql = format!(
            "SELECT result_json FROM script_outputs WHERE id IN ({})",
            vec!["{}"; chunk.len()].join(", ")
        );
        let args = chunk.iter().cloned().map(SqlArg::Text).collect::<Vec<_>>();
        for row in tx.fetch_all(&sql, &args).await? {
            if let Ok(result) = serde_json::from_str::<ScriptResult>(&row.text("result_json")?) {
                scripts.push(result.label().to_string());
            }
        }
    }
    Ok(scripts)
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScriptRunCount {
    pub kind: RunKind,
    pub adapter: ScriptAdapter,
    pub waited: bool,
    pub status: RunStatus,
    pub runs: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScriptRunDuration {
    pub kind: RunKind,
    pub waited: bool,
    pub status: RunStatus,
    pub duration: HistogramSnapshot,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScriptMetrics {
    pub script: Arc<str>,
    // Every status for each kind, adapter and waited combination the script
    // has run under.
    pub runs: Vec<ScriptRunCount>,
    // Only the combinations that have a run.
    pub durations: Vec<ScriptRunDuration>,
    pub nonzero_exits: u64,
    pub last_exit_code: Option<i64>,
    pub last_duration_ms: Option<u64>,
    pub last_finished_epoch_ms: Option<i64>,
    pub last_status: Option<RunStatus>,
    pub output_bytes: u64,
    pub stored_bytes: u64,
    pub truncations: u64,
    pub pruned: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScriptRunMetricsSnapshot {
    pub started: Vec<(RunKind, u64)>,
    pub running: Vec<(RunKind, u64)>,
    pub waiting: Vec<(RunKind, u64)>,
    pub slot_waits: Vec<(RunKind, u64)>,
    pub refusals: Vec<(RunKind, u64)>,
    pub job_summaries: Vec<(PostProcessingSummary, u64)>,
    pub interrupted_recovered: u64,
    pub retained: Vec<(RetentionAction, u64)>,
    // Read from the database at scrape time, so a changed limit shows at
    // once; `None` when the read failed.
    pub concurrency_limit: Option<u64>,
    pub queue_event_backlog: Option<u64>,
    // Sorted by script name.
    pub scripts: Vec<ScriptMetrics>,
}

fn script_metrics(script: &Arc<str>, slots: &ScriptSlots) -> ScriptMetrics {
    let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    let seen = slots.seen.load(Ordering::Relaxed);
    let mut runs = Vec::new();
    let mut durations = Vec::new();
    for kind in RunKind::ALL {
        for waited in [true, false] {
            let w = usize::from(waited);
            for (a, adapter) in ADAPTERS.into_iter().enumerate() {
                if seen & (1 << ((kind as usize * ADAPTERS.len() + a) * WAITED + w)) == 0 {
                    continue;
                }
                for status in RunStatus::ALL {
                    runs.push(ScriptRunCount {
                        kind,
                        adapter,
                        waited,
                        status,
                        runs: load(&slots.runs[kind as usize][a][w][status as usize]),
                    });
                }
            }
            for status in RunStatus::ALL {
                let duration = slots.duration[kind as usize][w][status as usize].snapshot();
                if duration.count > 0 {
                    durations.push(ScriptRunDuration {
                        kind,
                        waited,
                        status,
                        duration,
                    });
                }
            }
        }
    }
    let last_status = RunStatus::ALL
        .into_iter()
        .find(|status| *status as u8 == slots.last_status.load(Ordering::Relaxed));
    ScriptMetrics {
        script: script.clone(),
        runs,
        durations,
        nonzero_exits: load(&slots.nonzero_exits),
        last_exit_code: Some(slots.last_exit_code.load(Ordering::Relaxed))
            .filter(|code| *code != NO_EXIT_CODE),
        last_duration_ms: last_status.map(|_| load(&slots.last_duration_ms)),
        last_finished_epoch_ms: last_status.map(|_| slots.last_finished_ms.load(Ordering::Relaxed)),
        last_status,
        output_bytes: load(&slots.output_bytes),
        stored_bytes: load(&slots.stored_bytes),
        truncations: load(&slots.truncations),
        pruned: load(&slots.pruned),
    }
}

// The counters alone, without reading the database.
pub fn counters_snapshot() -> ScriptRunMetricsSnapshot {
    let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    let by_kind = |counters: &[AtomicU64; KINDS]| {
        RunKind::ALL
            .into_iter()
            .map(|kind| (kind, load(&counters[kind as usize])))
            .collect::<Vec<_>>()
    };
    let mut scripts: Vec<_> = SCRIPTS
        .read()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .map(|(script, slots)| (script.clone(), slots.clone()))
        .collect();
    scripts.sort_by(|a, b| a.0.cmp(&b.0));
    ScriptRunMetricsSnapshot {
        started: by_kind(&STARTED),
        running: by_kind(&RUNNING),
        waiting: by_kind(&WAITING),
        slot_waits: by_kind(&SLOT_WAITS),
        refusals: by_kind(&REFUSALS),
        job_summaries: SUMMARIES
            .into_iter()
            .map(|summary| (summary, load(&JOB_SUMMARIES[summary_index(summary)])))
            .collect(),
        interrupted_recovered: load(&INTERRUPTED_RECOVERED),
        retained: RetentionAction::ALL
            .into_iter()
            .map(|action| (action, load(&RETAINED[action as usize])))
            .collect(),
        concurrency_limit: None,
        queue_event_backlog: None,
        scripts: scripts
            .iter()
            .map(|(script, slots)| script_metrics(script, slots))
            .collect(),
    }
}

// The counters plus what the database says now: the concurrency setting and
// the queue events not yet started. A failed read leaves those series out.
pub fn snapshot(db: &Database) -> ScriptRunMetricsSnapshot {
    let mut snapshot = counters_snapshot();
    snapshot.concurrency_limit = db
        .post_processing_settings()
        .ok()
        .map(|settings| u64::from(settings.concurrency));
    snapshot.queue_event_backlog = db.script_event_backlog().ok();
    snapshot
}

impl Database {
    // Queue events recorded but not yet started.
    pub fn script_event_backlog(&self) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            Ok(SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT COUNT(*) AS count FROM script_event_queue WHERE state = 'queued'",
                &[],
            )
            .await?
            .map(|row| row.i64("count"))
            .transpose()?
            .unwrap_or(0) as u64)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A name no other test records under, so the shared registry cannot
    // leak another test's runs into these counts.
    fn result(name: &str, status: &str, exit: i32) -> ScriptResult {
        let mut result: ScriptResult = serde_json::from_str(&format!(
            r#"{{"script":"notify.sh","adapter":"nzbget","status":"{status}","exitCode":{exit},"durationMs":1500,"finishedAtEpochMs":1700000000000}}"#
        ))
        .unwrap();
        result.instance_name = Some(name.into());
        result
    }

    fn script(name: &str) -> ScriptMetrics {
        counters_snapshot()
            .scripts
            .into_iter()
            .find(|s| &*s.script == name)
            .expect("the script was recorded")
    }

    #[test]
    fn a_finished_run_fills_every_status_and_the_last_run_gauges() {
        let name = "run-metrics-finished";
        record_finished(&result(name, "failed", 3), true);
        let metrics = script(name);

        let statuses: Vec<_> = metrics.runs.iter().map(|r| (r.status, r.runs)).collect();
        assert_eq!(statuses.len(), RunStatus::ALL.len());
        for (status, runs) in statuses {
            assert_eq!(runs, u64::from(status == RunStatus::Failed), "{status:?}");
        }
        assert!(metrics.runs.iter().all(|r| r.kind == RunKind::PostProcessing
            && r.adapter == ScriptAdapter::Nzbget
            && r.waited));
        assert_eq!(metrics.durations.len(), 1);
        assert_eq!(metrics.durations[0].status, RunStatus::Failed);
        assert_eq!(metrics.durations[0].duration.count, 1);
        assert_eq!(metrics.nonzero_exits, 1);
        assert_eq!(metrics.last_exit_code, Some(3));
        assert_eq!(metrics.last_duration_ms, Some(1500));
        assert_eq!(metrics.last_finished_epoch_ms, Some(1_700_000_000_000));
        assert_eq!(metrics.last_status, Some(RunStatus::Failed));
    }

    #[test]
    fn a_run_that_never_launched_and_an_interrupted_run_have_their_own_statuses() {
        let name = "run-metrics-not-started";
        record_finished(&result(name, "failed", 0), false);
        assert_eq!(script(name).last_status, Some(RunStatus::NotStarted));
        record_interrupted(&result(name, "failed", 0));
        let metrics = script(name);
        assert_eq!(metrics.last_status, Some(RunStatus::Interrupted));
        let count = |status| {
            metrics
                .runs
                .iter()
                .find(|r| r.status == status)
                .map_or(0, |r| r.runs)
        };
        assert_eq!(count(RunStatus::NotStarted), 1);
        assert_eq!(count(RunStatus::Interrupted), 1);
        assert_eq!(count(RunStatus::Failed), 0);
        assert_eq!(metrics.nonzero_exits, 0);
    }
}
