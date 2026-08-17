//! Observability collection primitives shared by the pipeline, the database
//! runtime and the HTTP surface.
//!
//! Everything here obeys one rule: **instrumentation must never compromise the
//! hot path.** Concretely that means the types used on per-segment, per-article
//! and per-byte paths ([`AtomicHistogram`], [`ServerCounters`]) only ever
//! perform `Relaxed` atomic loads/stores/`fetch_add`s. They allocate nothing,
//! format nothing, take no lock and read no clock. Anything that needs a map, a
//! lock or an allocation is confined to low-frequency events (a job submitted,
//! a verification finished, an NNTP runtime activation) or to scrape time.
//!
//! The 100 ms [`crate::MetricsSnapshot`] tick is deliberately untouched: none of
//! these values are copied into it. Readers pull them on demand through the
//! snapshot accessors at the bottom of this module, which is where the `Vec`
//! allocations live.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use serde::Serialize;

// ---------------------------------------------------------------------------
// Histograms
// ---------------------------------------------------------------------------

/// Maximum number of finite bucket bounds an [`AtomicHistogram`] accepts. The
/// bucket array is one longer to hold the `+Inf` overflow bucket. Keeping this
/// a small constant lets `observe` be a fixed-cost linear scan with no
/// allocation and no indirection beyond the `&'static [f64]` bounds slice.
pub const HISTOGRAM_MAX_BUCKETS: usize = 16;

/// Lock-free, allocation-free duration histogram with `const` bucket bounds.
///
/// Hot-path contract: [`AtomicHistogram::observe`] does a linear scan over at
/// most [`HISTOGRAM_MAX_BUCKETS`] `f64` comparisons followed by exactly three
/// `Relaxed` `fetch_add`s. It never allocates, never locks and never reads a
/// clock — the caller supplies a `Duration` that some existing measurement on
/// that path already produced.
///
/// The running sum is kept in **nanoseconds** in a `u64`, which saturates after
/// ~584 years of accumulated observed time; a process that reaches that has
/// bigger problems than a skewed average. No float is ever stored in an atomic:
/// the conversion to seconds happens once, at snapshot time.
#[derive(Debug)]
pub struct AtomicHistogram {
    bounds: &'static [f64],
    counts: [AtomicU64; HISTOGRAM_MAX_BUCKETS + 1],
    sum_nanos: AtomicU64,
    count: AtomicU64,
}

impl AtomicHistogram {
    /// Build a histogram over `bounds` (upper-inclusive, in seconds, ascending).
    ///
    /// # Panics
    /// Panics if `bounds` is longer than [`HISTOGRAM_MAX_BUCKETS`]. Bounds are
    /// `&'static` compile-time constants, so this can only fire in development.
    pub const fn new(bounds: &'static [f64]) -> Self {
        assert!(
            bounds.len() <= HISTOGRAM_MAX_BUCKETS,
            "histogram bounds exceed HISTOGRAM_MAX_BUCKETS"
        );
        Self {
            bounds,
            counts: [const { AtomicU64::new(0) }; HISTOGRAM_MAX_BUCKETS + 1],
            sum_nanos: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// The finite upper bounds, in seconds.
    pub const fn bounds(&self) -> &'static [f64] {
        self.bounds
    }

    /// Record one observation.
    ///
    /// Hot-path safe: bounded float compares plus three `Relaxed` `fetch_add`s.
    /// No allocation, no lock, no clock read.
    pub fn observe(&self, duration: Duration) {
        let seconds = duration.as_secs_f64();
        let mut index = self.bounds.len();
        for (candidate, bound) in self.bounds.iter().enumerate() {
            if seconds <= *bound {
                index = candidate;
                break;
            }
        }
        self.counts[index].fetch_add(1, Ordering::Relaxed);
        self.sum_nanos.fetch_add(
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Copy the current state out. Allocates one `Vec`; called at scrape time
    /// only, never on a per-segment path.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let counts = (0..=self.bounds.len())
            .map(|index| self.counts[index].load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        HistogramSnapshot {
            bounds: self.bounds,
            counts,
            sum: self.sum_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of an [`AtomicHistogram`].
///
/// `counts` holds **per-bucket** (not cumulative) counts and is always
/// `bounds.len() + 1` long; the final entry is the `+Inf` overflow bucket.
/// Renderers that need Prometheus' cumulative `le` buckets accumulate as they
/// go. `sum` is in seconds.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HistogramSnapshot {
    pub bounds: &'static [f64],
    pub counts: Vec<u64>,
    pub sum: f64,
    pub count: u64,
}

impl HistogramSnapshot {
    /// An all-zero snapshot over `bounds`, for series that must pre-exist.
    pub fn empty(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            counts: vec![0; bounds.len() + 1],
            sum: 0.0,
            count: 0,
        }
    }

    /// Cumulative (`le`) counts, one per finite bound plus a final `+Inf` entry.
    /// Convenience for exporters; the stored `counts` stay per-bucket.
    pub fn cumulative_counts(&self) -> Vec<u64> {
        let mut running = 0u64;
        self.counts
            .iter()
            .map(|count| {
                running = running.saturating_add(*count);
                running
            })
            .collect()
    }
}

/// Article round-trip latency, seconds. Spans a fast local feed to a stalled
/// remote connection just short of the fetch timeout.
pub const ARTICLE_LATENCY_BOUNDS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Disk write batch duration, seconds.
pub const DISK_WRITE_DURATION_BOUNDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
];

/// Decode task duration, seconds.
pub const DECODE_TASK_DURATION_BOUNDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
];

/// Archive member extraction duration, seconds.
pub const EXTRACT_MEMBER_DURATION_BOUNDS: &[f64] =
    &[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0, 900.0];

/// Whole-job wall duration from submission to terminal state, seconds.
pub const JOB_DURATION_BOUNDS: &[f64] = &[
    1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0, 21600.0, 43200.0, 86400.0,
];

/// Per-stage wall duration, seconds.
pub const STAGE_DURATION_BOUNDS: &[f64] = &[
    0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0,
];

/// Database operation duration, seconds.
pub const DB_OP_DURATION_BOUNDS: &[f64] = &[
    0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0,
];

/// HTTP request duration, seconds.
pub const HTTP_REQUEST_DURATION_BOUNDS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

// ---------------------------------------------------------------------------
// Per-server article attempts
// ---------------------------------------------------------------------------

/// How one article fetch attempt against one server ended.
///
/// Distinct from [`crate::events::model::ServerAttemptOutcome`]: that type is
/// part of the event stream's public shape, this one is the metric label set
/// and additionally carries `QuotaBlocked`, which the event stream drops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerAttemptOutcomeKind {
    Success,
    NotFound,
    AuthFailure,
    TransientFailure,
    PermanentFailure,
    QuotaBlocked,
}

impl ServerAttemptOutcomeKind {
    /// Every outcome, in label order. Exporters emit a row per
    /// `(outcome, recovery)` pair so the series pre-exist at zero.
    pub const ALL: [Self; 6] = [
        Self::Success,
        Self::NotFound,
        Self::AuthFailure,
        Self::TransientFailure,
        Self::PermanentFailure,
        Self::QuotaBlocked,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::NotFound => "not_found",
            Self::AuthFailure => "auth_failure",
            Self::TransientFailure => "transient_failure",
            Self::PermanentFailure => "permanent_failure",
            Self::QuotaBlocked => "quota_blocked",
        }
    }

    pub const fn index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::NotFound => 1,
            Self::AuthFailure => 2,
            Self::TransientFailure => 3,
            Self::PermanentFailure => 4,
            Self::QuotaBlocked => 5,
        }
    }

    /// Whether this outcome represents a completed network round-trip whose
    /// elapsed time is meaningful. Quota blocks never reached the wire.
    pub const fn has_round_trip(self) -> bool {
        !matches!(self, Self::QuotaBlocked)
    }
}

/// Lifetime counters for one news server, addressed by the runtime `server_idx`
/// of the active NNTP generation and identified by its durable stable id.
///
/// Hot-path contract: [`ServerCounters::note_attempt`] is a single `Relaxed`
/// `fetch_add` into a fixed `[[AtomicU64; 2]; 6]` array, and
/// [`ServerCounters::observe_latency`] delegates to [`AtomicHistogram::observe`].
/// The orchestrator holds these as a plain `Vec<Arc<ServerCounters>>` with no
/// lock; the exporter reaches the same `Arc`s through [`ServerMetricsRegistry`],
/// whose `RwLock` is only ever written at NNTP runtime activation.
#[derive(Debug)]
pub struct ServerCounters {
    stable_server_id: u32,
    /// `[outcome][recovery as usize]`.
    attempts: [[AtomicU64; 2]; 6],
    article_latency: AtomicHistogram,
}

impl ServerCounters {
    pub fn new(stable_server_id: u32) -> Self {
        Self {
            stable_server_id,
            attempts: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            article_latency: AtomicHistogram::new(ARTICLE_LATENCY_BOUNDS),
        }
    }

    pub const fn stable_server_id(&self) -> u32 {
        self.stable_server_id
    }

    /// Count one attempt. Hot-path safe: one `Relaxed` `fetch_add`.
    pub fn note_attempt(&self, outcome: ServerAttemptOutcomeKind, recovery: bool) {
        self.attempts[outcome.index()][usize::from(recovery)].fetch_add(1, Ordering::Relaxed);
    }

    /// Record the round-trip time of one attempt. Hot-path safe: see
    /// [`AtomicHistogram::observe`]. Callers must skip outcomes for which
    /// [`ServerAttemptOutcomeKind::has_round_trip`] is false.
    pub fn observe_latency(&self, elapsed: Duration) {
        self.article_latency.observe(elapsed);
    }

    /// Copy this server's counters out. Scrape-time only.
    pub fn snapshot(&self, server_idx: usize) -> ServerMetricsSnapshot {
        let mut attempts = Vec::with_capacity(ServerAttemptOutcomeKind::ALL.len() * 2);
        for outcome in ServerAttemptOutcomeKind::ALL {
            for recovery in [false, true] {
                attempts.push(ServerAttemptCount {
                    outcome: outcome.as_str(),
                    recovery,
                    count: self.attempts[outcome.index()][usize::from(recovery)]
                        .load(Ordering::Relaxed),
                });
            }
        }
        ServerMetricsSnapshot {
            stable_server_id: self.stable_server_id,
            server_idx,
            attempts,
            article_latency: self.article_latency.snapshot(),
        }
    }
}

/// One `(outcome, recovery)` attempt cell for a server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServerAttemptCount {
    /// One of `success`, `not_found`, `auth_failure`, `transient_failure`,
    /// `permanent_failure`, `quota_blocked`.
    pub outcome: &'static str,
    /// Whether the attempt came from the recovery (PAR2 top-up) queue.
    pub recovery: bool,
    pub count: u64,
}

/// Per-server article attempt counters and latency, for one server of the
/// currently active NNTP generation.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ServerMetricsSnapshot {
    /// Durable database identity; survives reorder and client rebuilds.
    pub stable_server_id: u32,
    /// Position in the active NNTP generation. Changes across rebuilds.
    pub server_idx: usize,
    /// Every `(outcome, recovery)` combination, including zeroes.
    pub attempts: Vec<ServerAttemptCount>,
    pub article_latency: HistogramSnapshot,
}

/// Owns the `Arc<ServerCounters>` for every server the process has ever seen.
///
/// The orchestrator takes a plain `Vec<Arc<ServerCounters>>` from
/// [`ServerMetricsRegistry::activate`] and indexes it directly by
/// `server_idx` — no lock is touched on the completion path. The exporter reads
/// through the `RwLock`, which is only ever write-locked at NNTP runtime
/// activation (a config reload, not a per-article event). Counters are keyed by
/// stable id and re-used across activations, so lifetime totals survive a
/// server reorder or a client rebuild.
#[derive(Debug, Default)]
pub struct ServerMetricsRegistry {
    active: RwLock<Vec<Arc<ServerCounters>>>,
    lifetime: Mutex<HashMap<u32, Arc<ServerCounters>>>,
}

impl ServerMetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild the active index from the stable ids of a freshly activated NNTP
    /// generation, re-using the existing counters for any stable id already
    /// seen. Returns the same `Vec` the caller should store for hot-path use.
    ///
    /// Called only when a new NNTP runtime generation is activated.
    pub fn activate(&self, stable_ids: &[u32]) -> Vec<Arc<ServerCounters>> {
        let counters = {
            let mut lifetime = self
                .lifetime
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            stable_ids
                .iter()
                .map(|stable_id| {
                    Arc::clone(
                        lifetime
                            .entry(*stable_id)
                            .or_insert_with(|| Arc::new(ServerCounters::new(*stable_id))),
                    )
                })
                .collect::<Vec<_>>()
        };
        *self
            .active
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = counters.clone();
        counters
    }

    /// Snapshot every server of the active generation. Scrape-time only.
    pub fn snapshot(&self) -> Vec<ServerMetricsSnapshot> {
        self.active
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .enumerate()
            .map(|(server_idx, counters)| counters.snapshot(server_idx))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Job lifecycle
// ---------------------------------------------------------------------------

/// Terminal disposition of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobResultKind {
    Complete,
    Failed,
    Cancelled,
}

impl JobResultKind {
    pub const ALL: [Self; 3] = [Self::Complete, Self::Failed, Self::Cancelled];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn index(self) -> usize {
        match self {
            Self::Complete => 0,
            Self::Failed => 1,
            Self::Cancelled => 2,
        }
    }
}

/// A user-visible job stage whose wall duration is timed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobStageKind {
    Download,
    Verify,
    Repair,
    Extract,
    Move,
    PostProcess,
}

impl JobStageKind {
    pub const ALL: [Self; 6] = [
        Self::Download,
        Self::Verify,
        Self::Repair,
        Self::Extract,
        Self::Move,
        Self::PostProcess,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Download => "download",
            Self::Verify => "verify",
            Self::Repair => "repair",
            Self::Extract => "extract",
            Self::Move => "move",
            Self::PostProcess => "post_process",
        }
    }

    pub const fn index(self) -> usize {
        match self {
            Self::Download => 0,
            Self::Verify => 1,
            Self::Repair => 2,
            Self::Extract => 3,
            Self::Move => 4,
            Self::PostProcess => 5,
        }
    }
}

/// Verdict of one PAR2 verification pass over a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationOutcomeKind {
    Intact,
    Damaged,
    Missing,
    Unverifiable,
}

impl VerificationOutcomeKind {
    pub const ALL: [Self; 4] = [
        Self::Intact,
        Self::Damaged,
        Self::Missing,
        Self::Unverifiable,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Intact => "intact",
            Self::Damaged => "damaged",
            Self::Missing => "missing",
            Self::Unverifiable => "unverifiable",
        }
    }

    pub const fn index(self) -> usize {
        match self {
            Self::Intact => 0,
            Self::Damaged => 1,
            Self::Missing => 2,
            Self::Unverifiable => 3,
        }
    }
}

/// Outcome of a repair or an extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageOutcomeKind {
    Complete,
    Failed,
}

impl StageOutcomeKind {
    pub const ALL: [Self; 2] = [Self::Complete, Self::Failed];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    pub const fn index(self) -> usize {
        match self {
            Self::Complete => 0,
            Self::Failed => 1,
        }
    }
}

/// A `(origin, category)` submission tally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobSubmissionCount {
    /// [`crate::SubmissionOrigin::as_str`], e.g. `api`, `rss`, `nzbget`.
    pub origin: &'static str,
    /// The job's category, or `""` when it has none.
    pub category: String,
    pub count: u64,
}

/// A `(result, category)` terminal tally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobFinishCount {
    /// `complete`, `failed` or `cancelled`.
    pub result: &'static str,
    /// The job's category, or `""` when it has none.
    pub category: String,
    pub count: u64,
}

/// Everything the exporter needs about job lifecycle, pulled on demand.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JobLifecycleMetricsSnapshot {
    pub submitted: Vec<JobSubmissionCount>,
    pub finished: Vec<JobFinishCount>,
    /// Keyed by [`JobResultKind::as_str`].
    pub job_duration: Vec<(&'static str, HistogramSnapshot)>,
    /// Keyed by [`JobStageKind::as_str`].
    pub stage_duration: Vec<(&'static str, HistogramSnapshot)>,
    /// Keyed by [`VerificationOutcomeKind::as_str`].
    pub verifications: Vec<(&'static str, u64)>,
    /// Keyed by [`StageOutcomeKind::as_str`].
    pub repairs: Vec<(&'static str, u64)>,
    pub repair_slices_repaired_total: u64,
    /// Keyed by [`StageOutcomeKind::as_str`].
    pub extractions: Vec<(&'static str, u64)>,
    pub files_missing_total: u64,
    pub missing_segments_total: u64,
    /// Decoded bytes attributed to each category (`""` when none).
    pub bytes_by_category: Vec<(String, u64)>,
}

/// Job lifecycle collection state.
///
/// Every counter here is driven by a **low-frequency** event: a job created, a
/// job reaching a terminal status, a verification/repair/extraction result, a
/// missing file. Those are per-job or per-archive-member, never per segment, so
/// the `Mutex<HashMap<..>>` label maps are safe (the same shape
/// `jobs::duplicate` already uses for admission metrics).
///
/// The one exception is `bytes_by_category`, which *is* fed from a per-segment
/// path. That path never touches the map: the category's `Arc<AtomicU64>` is
/// resolved once when the job is added and stashed in the job's runtime state,
/// so the per-segment site is a single `Relaxed` `fetch_add` through an already
/// resolved pointer.
#[derive(Debug)]
pub struct JobLifecycleMetrics {
    submitted: Mutex<HashMap<(&'static str, String), u64>>,
    finished: Mutex<HashMap<(&'static str, String), u64>>,
    job_duration: [AtomicHistogram; 3],
    stage_duration: [AtomicHistogram; 6],
    verifications: [AtomicU64; 4],
    repairs: [AtomicU64; 2],
    repair_slices_repaired_total: AtomicU64,
    extractions: [AtomicU64; 2],
    files_missing_total: AtomicU64,
    missing_segments_total: AtomicU64,
    bytes_by_category: Mutex<HashMap<String, Arc<AtomicU64>>>,
}

impl Default for JobLifecycleMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl JobLifecycleMetrics {
    pub fn new() -> Self {
        Self {
            submitted: Mutex::new(HashMap::new()),
            finished: Mutex::new(HashMap::new()),
            job_duration: std::array::from_fn(|_| AtomicHistogram::new(JOB_DURATION_BOUNDS)),
            stage_duration: std::array::from_fn(|_| AtomicHistogram::new(STAGE_DURATION_BOUNDS)),
            verifications: std::array::from_fn(|_| AtomicU64::new(0)),
            repairs: std::array::from_fn(|_| AtomicU64::new(0)),
            repair_slices_repaired_total: AtomicU64::new(0),
            extractions: std::array::from_fn(|_| AtomicU64::new(0)),
            files_missing_total: AtomicU64::new(0),
            missing_segments_total: AtomicU64::new(0),
            bytes_by_category: Mutex::new(HashMap::new()),
        }
    }

    /// One job accepted for download. Low-frequency: per submission.
    pub fn note_submitted(&self, origin: &'static str, category: &str) {
        let mut submitted = self
            .submitted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = submitted.get_mut(&(origin, category.to_string())) {
            *count += 1;
        } else {
            submitted.insert((origin, category.to_string()), 1);
        }
    }

    /// One job reached a terminal status. Low-frequency: per job.
    pub fn note_finished(&self, result: JobResultKind, category: &str, elapsed: Option<Duration>) {
        {
            let mut finished = self
                .finished
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let key = (result.as_str(), category.to_string());
            if let Some(count) = finished.get_mut(&key) {
                *count += 1;
            } else {
                finished.insert(key, 1);
            }
        }
        if let Some(elapsed) = elapsed {
            self.job_duration[result.index()].observe(elapsed);
        }
    }

    /// One stage of one job finished. Low-frequency: per job per stage.
    pub fn note_stage_duration(&self, stage: JobStageKind, elapsed: Duration) {
        self.stage_duration[stage.index()].observe(elapsed);
    }

    /// Low-frequency: one per PAR2 verification pass.
    pub fn note_verification(&self, outcome: VerificationOutcomeKind) {
        self.verifications[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Low-frequency: one per PAR2 repair attempt.
    pub fn note_repair(&self, outcome: StageOutcomeKind, slices_repaired: u64) {
        self.repairs[outcome.index()].fetch_add(1, Ordering::Relaxed);
        if slices_repaired > 0 {
            self.repair_slices_repaired_total
                .fetch_add(slices_repaired, Ordering::Relaxed);
        }
    }

    /// Low-frequency: one per archive extraction.
    pub fn note_extraction(&self, outcome: StageOutcomeKind) {
        self.extractions[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Low-frequency: one per file that could not be assembled.
    pub fn note_file_missing(&self, missing_segments: u64) {
        self.files_missing_total.fetch_add(1, Ordering::Relaxed);
        if missing_segments > 0 {
            self.missing_segments_total
                .fetch_add(missing_segments, Ordering::Relaxed);
        }
    }

    /// Resolve (creating if needed) the byte counter for `category`.
    ///
    /// Called **once per job**, when the job is added. The returned `Arc` is
    /// stashed in the job's runtime state so the per-segment byte accounting
    /// site never looks the map up.
    pub fn category_bytes_counter(&self, category: Option<&str>) -> Arc<AtomicU64> {
        let key = category.unwrap_or("");
        let mut map = self
            .bytes_by_category
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(counter) = map.get(key) {
            return Arc::clone(counter);
        }
        let counter = Arc::new(AtomicU64::new(0));
        map.insert(key.to_string(), Arc::clone(&counter));
        counter
    }

    /// Copy everything out. Scrape-time only; allocates.
    pub fn snapshot(&self) -> JobLifecycleMetricsSnapshot {
        let submitted = {
            let submitted = self
                .submitted
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut rows = submitted
                .iter()
                .map(|((origin, category), count)| JobSubmissionCount {
                    origin,
                    category: category.clone(),
                    count: *count,
                })
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| {
                left.origin
                    .cmp(right.origin)
                    .then_with(|| left.category.cmp(&right.category))
            });
            rows
        };
        let finished = {
            let finished = self
                .finished
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut rows = finished
                .iter()
                .map(|((result, category), count)| JobFinishCount {
                    result,
                    category: category.clone(),
                    count: *count,
                })
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| {
                left.result
                    .cmp(right.result)
                    .then_with(|| left.category.cmp(&right.category))
            });
            rows
        };
        let bytes_by_category = {
            let map = self
                .bytes_by_category
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut rows = map
                .iter()
                .map(|(category, counter)| (category.clone(), counter.load(Ordering::Relaxed)))
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| left.0.cmp(&right.0));
            rows
        };

        JobLifecycleMetricsSnapshot {
            submitted,
            finished,
            job_duration: JobResultKind::ALL
                .iter()
                .map(|result| {
                    (
                        result.as_str(),
                        self.job_duration[result.index()].snapshot(),
                    )
                })
                .collect(),
            stage_duration: JobStageKind::ALL
                .iter()
                .map(|stage| {
                    (
                        stage.as_str(),
                        self.stage_duration[stage.index()].snapshot(),
                    )
                })
                .collect(),
            verifications: VerificationOutcomeKind::ALL
                .iter()
                .map(|outcome| {
                    (
                        outcome.as_str(),
                        self.verifications[outcome.index()].load(Ordering::Relaxed),
                    )
                })
                .collect(),
            repairs: StageOutcomeKind::ALL
                .iter()
                .map(|outcome| {
                    (
                        outcome.as_str(),
                        self.repairs[outcome.index()].load(Ordering::Relaxed),
                    )
                })
                .collect(),
            repair_slices_repaired_total: self.repair_slices_repaired_total.load(Ordering::Relaxed),
            extractions: StageOutcomeKind::ALL
                .iter()
                .map(|outcome| {
                    (
                        outcome.as_str(),
                        self.extractions[outcome.index()].load(Ordering::Relaxed),
                    )
                })
                .collect(),
            files_missing_total: self.files_missing_total.load(Ordering::Relaxed),
            missing_segments_total: self.missing_segments_total.load(Ordering::Relaxed),
            bytes_by_category,
        }
    }
}

// ---------------------------------------------------------------------------
// Pipeline stage histograms
// ---------------------------------------------------------------------------

/// Wall-clock histograms for pipeline stages that already measure themselves.
///
/// Hot-path contract: every `observe` here is fed by a `Duration` that the
/// surrounding code already computed for its own purposes. Nothing in this
/// struct starts a clock.
#[derive(Debug)]
pub struct PipelineHistograms {
    pub disk_write_duration: AtomicHistogram,
    pub decode_task_duration: AtomicHistogram,
    pub extract_member_duration: AtomicHistogram,
    /// Set once the corresponding histogram has a real feeding site. Until
    /// then the snapshot reports `None` rather than an all-zero series that
    /// would read as "nothing is slow".
    decode_task_observed: AtomicU64,
    extract_member_observed: AtomicU64,
}

impl Default for PipelineHistograms {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineHistograms {
    pub const fn new() -> Self {
        Self {
            disk_write_duration: AtomicHistogram::new(DISK_WRITE_DURATION_BOUNDS),
            decode_task_duration: AtomicHistogram::new(DECODE_TASK_DURATION_BOUNDS),
            extract_member_duration: AtomicHistogram::new(EXTRACT_MEMBER_DURATION_BOUNDS),
            decode_task_observed: AtomicU64::new(0),
            extract_member_observed: AtomicU64::new(0),
        }
    }

    /// Hot-path safe: called from the decode worker's disk-write path with the
    /// `Duration` that path already measured for `disk_write_latency_us`.
    pub fn observe_disk_write(&self, elapsed: Duration) {
        self.disk_write_duration.observe(elapsed);
    }

    /// Hot-path safe when the caller already holds a wall-clock measurement.
    pub fn observe_decode_task(&self, elapsed: Duration) {
        self.decode_task_observed.store(1, Ordering::Relaxed);
        self.decode_task_duration.observe(elapsed);
    }

    /// Low-frequency: one per extracted archive member.
    pub fn observe_extract_member(&self, elapsed: Duration) {
        self.extract_member_observed.store(1, Ordering::Relaxed);
        self.extract_member_duration.observe(elapsed);
    }

    pub fn snapshot(&self) -> PipelineHistogramsSnapshot {
        PipelineHistogramsSnapshot {
            disk_write_duration: self.disk_write_duration.snapshot(),
            decode_task_duration: (self.decode_task_observed.load(Ordering::Relaxed) != 0)
                .then(|| self.decode_task_duration.snapshot()),
            extract_member_duration: (self.extract_member_observed.load(Ordering::Relaxed) != 0)
                .then(|| self.extract_member_duration.snapshot()),
        }
    }
}

/// Pipeline stage duration histograms.
///
/// The two `Option`s are `Some` only once their stage has actually been timed;
/// a stage that has no zero-cost measurement available on its path stays `None`
/// rather than reporting a fabricated empty series.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PipelineHistogramsSnapshot {
    pub disk_write_duration: HistogramSnapshot,
    pub decode_task_duration: Option<HistogramSnapshot>,
    pub extract_member_duration: Option<HistogramSnapshot>,
}

// ---------------------------------------------------------------------------
// Database runtime
// ---------------------------------------------------------------------------

/// Database executor saturation and operation latency.
///
/// Not a hot-path type: every site that feeds it is already blocking on a
/// channel round-trip to the database runtime thread and already reads a clock
/// for the existing `perf_probe` record.
#[derive(Debug)]
pub struct DbRuntimeMetrics {
    engine: &'static str,
    concurrency: u64,
    in_flight: AtomicU64,
    blocked_submissions_total: AtomicU64,
    op_duration: AtomicHistogram,
}

impl DbRuntimeMetrics {
    pub fn new(engine: &'static str, concurrency: u64) -> Self {
        Self {
            engine,
            concurrency,
            in_flight: AtomicU64::new(0),
            blocked_submissions_total: AtomicU64::new(0),
            op_duration: AtomicHistogram::new(DB_OP_DURATION_BOUNDS),
        }
    }

    pub const fn engine(&self) -> &'static str {
        self.engine
    }

    /// Mark one operation as submitted. The returned guard decrements the
    /// in-flight gauge when it is dropped, so an operation that errors out
    /// early (worker stopped, worker panicked) cannot leave the gauge stuck
    /// high.
    #[must_use = "the in-flight count is decremented when this guard drops"]
    pub fn note_submission_started(&self) -> DbInFlightGuard<'_> {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        DbInFlightGuard { metrics: self }
    }

    /// Record how long one submitted operation took.
    pub fn note_submission_finished(&self, elapsed: Duration) {
        self.op_duration.observe(elapsed);
    }

    pub fn note_submission_blocked(&self) {
        self.blocked_submissions_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> DbRuntimeMetricsSnapshot {
        DbRuntimeMetricsSnapshot {
            engine: self.engine,
            concurrency: self.concurrency,
            in_flight: self.in_flight.load(Ordering::Relaxed),
            blocked_submissions_total: self.blocked_submissions_total.load(Ordering::Relaxed),
            op_duration: self.op_duration.snapshot(),
        }
    }
}

/// Keeps the database in-flight gauge honest across early returns: dropping
/// it is the "operation is no longer in flight" event.
#[derive(Debug)]
pub struct DbInFlightGuard<'a> {
    metrics: &'a DbRuntimeMetrics,
}

impl Drop for DbInFlightGuard<'_> {
    fn drop(&mut self) {
        let _ =
            self.metrics
                .in_flight
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(1))
                });
    }
}

/// Database executor state at scrape time.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DbRuntimeMetricsSnapshot {
    /// `sqlite` or `postgres`.
    pub engine: &'static str,
    /// Configured executor concurrency (always 1 for the serialized sqlite worker).
    pub concurrency: u64,
    /// Operations currently submitted and not yet answered.
    pub in_flight: u64,
    /// Lifetime count of submissions that had to block on a full executor queue.
    pub blocked_submissions_total: u64,
    pub op_duration: HistogramSnapshot,
}

// ---------------------------------------------------------------------------
// HTTP request metrics
// ---------------------------------------------------------------------------

/// One `(route, method, status)` request tally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HttpRequestCount {
    /// A route **template**, never a raw path.
    pub route: &'static str,
    pub method: &'static str,
    pub status: u16,
    pub count: u64,
}

/// HTTP surface counters, keyed by route template to bound cardinality.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HttpMetricsSnapshot {
    pub requests: Vec<HttpRequestCount>,
    /// Keyed by route template.
    pub duration: Vec<(&'static str, HistogramSnapshot)>,
}

// ---------------------------------------------------------------------------
// Process and filesystem sampling
// ---------------------------------------------------------------------------

/// Process-level resource usage, sampled at scrape time.
///
/// Every field is optional: platforms differ in what they can answer cheaply,
/// and a metric that cannot be read is better absent than zero.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ProcessMetricsSnapshot {
    pub cpu_seconds_total: Option<f64>,
    pub resident_memory_bytes: Option<u64>,
    pub virtual_memory_bytes: Option<u64>,
    pub open_fds: Option<u64>,
    pub max_fds: Option<u64>,
    pub threads: Option<u64>,
    pub start_time_seconds: Option<f64>,
}

/// Free/total capacity for one configured directory role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiskSpaceSnapshot {
    /// `data`, `intermediate` or `complete`.
    pub role: &'static str,
    pub path: String,
    pub total_bytes: u64,
    pub available_bytes: u64,
}

#[cfg(test)]
mod tests;
