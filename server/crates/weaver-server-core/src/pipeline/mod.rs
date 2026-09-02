pub mod archive;
#[cfg(test)]
pub(crate) use archive::rar_state;
mod capacity;
mod completion;
mod decode;
mod direct_store;
pub mod direct_unpack;
pub mod download;
mod extraction;
mod health;
mod infrastructure_retry;
pub(crate) mod integrity;
mod orchestrator;
mod progress;
mod repair;

pub(crate) use orchestrator::check_disk_space;
pub(crate) use orchestrator::{close_cached_write_handles_under, release_cached_write_handle};
#[cfg(test)]
use orchestrator::{compute_decode_backlog_budget_bytes, compute_write_backlog_budget_bytes};
use orchestrator::{is_terminal_status, write_segment_to_disk, write_segments_to_disk};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::ActiveFileProgress;
#[cfg(test)]
use crate::RestoreJobRequest;
use crate::bandwidth::service::BandwidthCapRuntime;
use crate::events::model::PipelineEvent;
use crate::jobs::assembly::ExtractionReadiness;
#[cfg(test)]
use crate::jobs::assembly::JobAssembly;
use crate::jobs::assembly::write_buffer::{BufferedChunk, WriteReorderBuffer};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::{ArchivePasswordCandidate, ArchivePasswordSource};
use crate::jobs::{JobPhase, JobPhaseProgress, PhaseAttemptCounters, PhaseCounters};
use crate::post_processing::model::PostProcessingSettings;
use crate::runtime::buffers::{BufferHandle, BufferPool};
use crate::runtime::system_profile::SystemProfile;
use crate::{
    DispatchShareMode, DownloadPressureReason, DownloadPressureState, DownloadQueue, DownloadWork,
    JobInfo, JobSpec, JobState, JobStatus, NntpRuntimeActivation, PipelineMetrics, RuntimeTuner,
    SchedulerCommand, SchedulerError, SharedPipelineState, SpilloverDecision, TokenBucket,
};
#[cfg(test)]
use par2_rs::checksum;
use par2_rs::par2_set::Par2FileSet;
use weaver_nntp::NntpClient;

use self::archive::rar_state::{RarDerivedPlan, RarSetState};
use self::download::{
    DownloadLaneMode, DownloadLaneRuntimeState, JobTransportProfile, LaneParkReason,
};
use self::extraction::{
    ExtractionLimits, ExtractionRoot, JobExtractionBudget, ProcessMemoryBudget,
};

/// Maximum number of retries for a single segment before giving up.
const MAX_SEGMENT_RETRIES: u32 = 3;
const DOWNLOAD_RESTART_CHECKPOINT_BYTES: u64 = 256 * 1024 * 1024;
const DOWNLOAD_RESTART_MAX_DURABLE_LEAD_MULTIPLIER: u64 = 4;
const STALLED_DOWNLOAD_CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);
const STALLED_DOWNLOAD_IDLE_THRESHOLD: Duration = Duration::from_secs(5 * 60);
pub(in crate::pipeline) const RAR_CAPACITY_RETRY_DELAY: Duration = Duration::from_millis(500);

fn download_restart_checkpoint_bytes() -> u64 {
    static CHECKPOINT_BYTES: OnceLock<u64> = OnceLock::new();
    *CHECKPOINT_BYTES.get_or_init(|| {
        std::env::var("WEAVER_E2E_DOWNLOAD_RESTART_CHECKPOINT_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|bytes| *bytes > 0)
            .unwrap_or(DOWNLOAD_RESTART_CHECKPOINT_BYTES)
    })
}

/// How long after a post's own date its articles are left alone before weaver
/// will fetch them.
///
/// # Why there is a delay at all
///
/// A binary post does not appear on a server the instant it is made: it
/// propagates, article by article, and a reader that starts pulling immediately
/// meets articles that simply have not arrived yet. Every one of those reads is
/// a not-found that looks exactly like a missing article — it burns a retry, it
/// spends the article's server budget, it marks servers unhealthy, and on a
/// par2-less job it can fail a download that would have succeeded ten minutes
/// later. Waiting costs a few minutes; not waiting costs accuracy in the one
/// signal weaver uses to decide an article is gone.
///
/// # Why it is not a setting
///
/// Deliberately not user-facing: no settings row, no schema column, no UI, no
/// API surface. Both major clients expose this knob and the community guidance
/// that has grown up around it is a range — roughly five to fifteen minutes —
/// rather than a value anyone tunes per job. A knob whose right answer is "the
/// conservative end, always" is not a choice worth asking a user to make; it is
/// a default worth getting right. The environment variable exists so an
/// operator can disable the behaviour or shorten it for a test, not as a
/// supported configuration surface.
///
/// `WEAVER_PROPAGATION_DELAY_SECS`: unset takes the conservative end of that
/// range, `0` disables deferral entirely, and any other value is a delay in
/// seconds. Read once, like every other environment gate here.
fn propagation_delay() -> Duration {
    const DEFAULT_PROPAGATION_DELAY_SECS: u64 = 300;
    static DELAY: OnceLock<Duration> = OnceLock::new();
    *DELAY.get_or_init(|| {
        let secs = std::env::var("WEAVER_PROPAGATION_DELAY_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_PROPAGATION_DELAY_SECS);
        Duration::from_secs(secs)
    })
}

fn health_milli(total: u64, failed_bytes: u64) -> u32 {
    total
        .saturating_sub(failed_bytes)
        .saturating_mul(1000)
        .checked_div(total)
        .unwrap_or(1000) as u32
}

impl Pipeline {
    pub(super) fn archive_password_candidates_for_job(
        &self,
        job_id: JobId,
    ) -> Vec<ArchivePasswordCandidate> {
        self.harvest_archive_password_candidates(job_id).0
    }

    /// [`Self::archive_password_candidates_for_job`] plus whether the job's
    /// persisted NZB was actually **read**.
    ///
    /// The harvest's two halves fail differently. `spec.password` is already in
    /// memory and cannot fail; the NZB half is a database read followed by a
    /// parse, and both of those warn-and-continue with an empty list. So an
    /// empty result is two different facts — *"this job carries no password
    /// anywhere"*, which is permanent, and *"the read failed this once"*, which
    /// is not — and any caller that **memoizes** the harvest has to tell them
    /// apart. `false` here means the second: nothing about the job was learned,
    /// so nothing about it may be remembered.
    fn harvest_archive_password_candidates(
        &self,
        job_id: JobId,
    ) -> (Vec<ArchivePasswordCandidate>, bool) {
        let spec_password = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.spec.password.as_deref());
        let mut harvested = true;
        let mut candidates = match self.db.load_active_job_persisted_nzb(job_id) {
            Ok(Some((nzb_path, Some(nzb_zstd)))) => {
                match crate::ingest::parse_persisted_nzb_bytes(&nzb_zstd) {
                    Ok(nzb) => crate::ingest::nzb_password_candidates(&nzb, &nzb_path, None),
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            error = %error,
                            "failed to parse persisted NZB for password candidates"
                        );
                        harvested = false;
                        Vec::new()
                    }
                }
            }
            Ok(_) => Vec::new(),
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load persisted NZB for password candidates"
                );
                harvested = false;
                Vec::new()
            }
        };

        if let Some(value) = crate::ingest::normalize_archive_password_candidate(spec_password)
            && !candidates
                .iter()
                .any(|candidate| candidate.value() == value.as_str())
        {
            candidates.insert(
                0,
                ArchivePasswordCandidate::new(ArchivePasswordSource::Explicit, value),
            );
        }

        (candidates, harvested)
    }

    pub(super) fn primary_archive_password_for_job(&self, job_id: JobId) -> Option<String> {
        self.archive_password_candidates_for_job(job_id)
            .into_iter()
            .next()
            .map(|candidate| candidate.value().to_string())
    }

    pub(super) fn archive_password_candidates_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Vec<ArchivePasswordCandidate> {
        let candidates = self.archive_password_candidates_for_job(job_id);
        let Some(winner) = self
            .archive_password_winners
            .get(&(job_id, set_name.to_string()))
            .cloned()
        else {
            return candidates;
        };

        Self::password_candidates_with_selected_first(candidates, &winner)
    }

    pub(super) fn password_candidates_with_selected_first(
        mut candidates: Vec<ArchivePasswordCandidate>,
        selected: &ArchivePasswordCandidate,
    ) -> Vec<ArchivePasswordCandidate> {
        if let Some(position) = candidates
            .iter()
            .position(|candidate| candidate.value() == selected.value())
        {
            candidates.remove(position);
        }
        candidates.insert(0, selected.clone());
        candidates
    }

    pub(super) fn remember_archive_password_winner(
        &mut self,
        job_id: JobId,
        set_name: &str,
        selected_password: Option<&str>,
        candidates: &[ArchivePasswordCandidate],
    ) {
        let Some(selected_password) = selected_password else {
            return;
        };
        let Some(candidate) = candidates
            .iter()
            .find(|candidate| candidate.value() == selected_password)
            .cloned()
        else {
            return;
        };

        self.archive_password_winners
            .insert((job_id, set_name.to_string()), candidate);
    }
}

#[derive(Debug, Clone)]
pub(super) struct DownloadBatchCompatibility {
    pub(super) priority: u32,
    pub(super) is_recovery: bool,
    pub(super) completion_critical: bool,
    pub(super) groups: std::sync::Arc<[String]>,
    pub(super) exclude_servers: Vec<usize>,
    /// Transport-rotation hint carried from [`DownloadWork::avoid_server`].
    /// Batched works share one effective exclude set, so works with different
    /// avoid hints must not share a lease.
    pub(super) avoid_server: Option<usize>,
}

impl DownloadBatchCompatibility {
    fn from_work(work: &DownloadWork) -> Self {
        Self {
            priority: work.priority,
            is_recovery: work.is_recovery,
            completion_critical: work.completion_critical,
            groups: work.groups.clone(),
            exclude_servers: work.exclude_servers.clone(),
            avoid_server: work.avoid_server,
        }
    }

    fn matches(&self, work: &DownloadWork) -> bool {
        work.priority == self.priority
            && work.is_recovery == self.is_recovery
            && work.completion_critical == self.completion_critical
            && (std::sync::Arc::ptr_eq(&work.groups, &self.groups) || work.groups == self.groups)
            && work.exclude_servers == self.exclude_servers
            && work.avoid_server == self.avoid_server
    }
}

pub(super) struct DownloadBatchLease {
    pub(super) job_id: JobId,
    pub(super) runtime_generation: u64,
    pub(super) lane_mode: DownloadLaneMode,
    pub(super) spillover_loan_kind: Option<SpilloverLoanKind>,
    pub(super) server_modes: Vec<(usize, DownloadLaneMode)>,
    pub(super) compatibility: DownloadBatchCompatibility,
    /// Compatibility excludes plus the job's retention exclusions — the set
    /// server ordering and lane acquisition use. Results keep reporting the
    /// compatibility (failure-only) excludes; retention stays job-derived.
    pub(super) effective_exclude_servers: Vec<usize>,
    /// Immutable common-refinement geometry captured when this batch was
    /// leased. Each response carries this same snapshot through durable commit
    /// so grids admitted later cannot reinterpret old decoder output.
    pub(super) checkpoint_plan: weaver_yenc::CheckpointPlan,
    pub(super) works: Vec<DownloadWork>,
}

pub(super) struct DownloadLaneRefillRequest {
    pub(super) job_id: JobId,
    pub(super) runtime_generation: u64,
    pub(super) server_idx: usize,
    pub(super) remote_ip: IpAddr,
    pub(super) supports_pipelining: bool,
    pub(super) current_mode: DownloadLaneMode,
    pub(super) spillover_loan_kind: Option<SpilloverLoanKind>,
    pub(super) compatibility: DownloadBatchCompatibility,
    pub(super) response_tx: oneshot::Sender<DownloadLaneRefillResponse>,
}

pub(super) struct DownloadLaneRefillResponse {
    pub(super) lease: Option<DownloadBatchLease>,
    pub(super) park_reason: LaneParkReason,
}

const HOT_THROUGHPUT_WINDOW: Duration = Duration::from_secs(2);
const HOT_THROUGHPUT_BUCKET_WIDTH: Duration = Duration::from_millis(200);
const HOT_THROUGHPUT_BUCKETS: usize = 10;
const HOT_EXPANSION_AFTER_WINDOW: Duration = Duration::from_secs(2);
const HOT_EXPANSION_LOOKBACK: Duration = Duration::from_secs(4);

#[derive(Debug, Default)]
pub(super) struct HotJobThroughputWindow {
    buckets: VecDeque<HotThroughputBucket>,
}

#[derive(Debug)]
struct HotThroughputBucket {
    started_at: Instant,
    bytes: u64,
}

impl HotJobThroughputWindow {
    pub(super) fn clear(&mut self) {
        self.buckets.clear();
    }

    pub(super) fn record(&mut self, now: Instant, bytes: u64) {
        self.advance_to(now);
        if let Some(bucket) = self.buckets.back_mut() {
            bucket.bytes = bucket.bytes.saturating_add(bytes);
        }
    }

    pub(super) fn bps(&mut self, now: Instant) -> u64 {
        self.advance_to(now);
        let bytes = self.buckets.iter().map(|bucket| bucket.bytes).sum::<u64>();
        (bytes as f64 / HOT_THROUGHPUT_WINDOW.as_secs_f64()).round() as u64
    }

    fn advance_to(&mut self, now: Instant) {
        if self.buckets.back().is_some_and(|bucket| {
            now.saturating_duration_since(bucket.started_at) >= HOT_THROUGHPUT_WINDOW
        }) {
            self.buckets.clear();
        }

        if self.buckets.is_empty() {
            self.buckets.push_back(HotThroughputBucket {
                started_at: now,
                bytes: 0,
            });
            return;
        }

        while self.buckets.back().is_some_and(|bucket| {
            now.saturating_duration_since(bucket.started_at) >= HOT_THROUGHPUT_BUCKET_WIDTH
        }) {
            let next_started_at = self
                .buckets
                .back()
                .map(|bucket| bucket.started_at + HOT_THROUGHPUT_BUCKET_WIDTH)
                .unwrap_or(now);
            self.buckets.push_back(HotThroughputBucket {
                started_at: next_started_at,
                bytes: 0,
            });
            while self.buckets.len() > HOT_THROUGHPUT_BUCKETS {
                self.buckets.pop_front();
            }
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct HotExclusiveWindow {
    peak_bps: u64,
}

impl HotExclusiveWindow {
    pub(super) fn clear(&mut self) {
        self.peak_bps = 0;
    }

    pub(super) fn record(&mut self, bps: u64) {
        self.peak_bps = self.peak_bps.max(bps);
    }

    pub(super) fn peak_bps(&self) -> u64 {
        self.peak_bps
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HotExpansionKind {
    LaneStart,
    PipelinePromotion,
}

impl HotExpansionKind {
    pub(super) fn as_code(self) -> usize {
        match self {
            Self::LaneStart => 1,
            Self::PipelinePromotion => 2,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct HotExpansionEvent {
    pub(super) at: Instant,
    pub(super) kind: HotExpansionKind,
    pub(super) before_bps: u64,
    pub(super) after_bps: Option<u64>,
}

#[derive(Debug, Default)]
pub(super) struct HotExpansionWindow {
    events: VecDeque<HotExpansionEvent>,
}

impl HotExpansionWindow {
    pub(super) fn clear(&mut self) {
        self.events.clear();
    }

    pub(super) fn record(&mut self, now: Instant, kind: HotExpansionKind, before_bps: u64) {
        self.events.push_back(HotExpansionEvent {
            at: now,
            kind,
            before_bps,
            after_bps: None,
        });
        self.prune(now);
    }

    pub(super) fn refresh(&mut self, now: Instant, current_bps: u64) {
        for event in &mut self.events {
            if event.after_bps.is_none()
                && now.saturating_duration_since(event.at) >= HOT_EXPANSION_AFTER_WINDOW
            {
                event.after_bps = Some(current_bps);
            }
        }
        self.prune(now);
    }

    pub(super) fn recent_improvement_pct(&mut self, now: Instant) -> u64 {
        self.prune(now);
        self.events
            .iter()
            .filter_map(|event| {
                let after = event.after_bps?;
                if event.before_bps == 0 || after <= event.before_bps {
                    return Some(0);
                }
                Some(((after - event.before_bps) * 100) / event.before_bps)
            })
            .max()
            .unwrap_or(0)
    }

    pub(super) fn last_event(&self) -> Option<HotExpansionEvent> {
        self.events.back().copied()
    }

    fn prune(&mut self, now: Instant) {
        while self
            .events
            .front()
            .is_some_and(|event| now.saturating_duration_since(event.at) > HOT_EXPANSION_LOOKBACK)
        {
            self.events.pop_front();
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum SpilloverReclaimReason {
    SpeedHarm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SpilloverLoanKind {
    MeasuredUnderfill,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SpilloverLoanState {
    pub(super) measured_lent_connections: usize,
    pub(super) measured_post_lend_bps: Option<u64>,
    pub(super) measured_reclaim_reason: Option<SpilloverReclaimReason>,
}

#[derive(Debug, Default)]
pub(super) struct SpilloverLoanBook {
    loans: HashMap<JobId, SpilloverLoanState>,
    aggregate_pre_lend_bps: Option<u64>,
    aggregate_lent_at: Option<Instant>,
    aggregate_post_lend_bps: Option<u64>,
}

impl SpilloverLoanBook {
    pub(super) fn clear(&mut self) {
        self.loans.clear();
        self.aggregate_pre_lend_bps = None;
        self.aggregate_lent_at = None;
        self.aggregate_post_lend_bps = None;
    }

    pub(super) fn start_or_extend(
        &mut self,
        job_id: JobId,
        now: Instant,
        hot_speed_bps: u64,
        kind: SpilloverLoanKind,
    ) {
        if self.measured_lent_connections() == 0 {
            self.aggregate_pre_lend_bps = Some(hot_speed_bps);
            self.aggregate_lent_at = Some(now);
            self.aggregate_post_lend_bps = None;
        }
        self.loans
            .entry(job_id)
            .and_modify(|loan| {
                loan.increment(kind);
            })
            .or_insert(SpilloverLoanState {
                measured_lent_connections: 1,
                measured_post_lend_bps: None,
                measured_reclaim_reason: None,
            });
    }

    pub(super) fn release_one(&mut self, job_id: JobId, kind: SpilloverLoanKind) {
        let Some(loan) = self.loans.get_mut(&job_id) else {
            return;
        };
        loan.decrement(kind);
        if loan.total_lent_connections() == 0 {
            self.loans.remove(&job_id);
        }
        if self.measured_lent_connections() == 0 {
            self.aggregate_pre_lend_bps = None;
            self.aggregate_lent_at = None;
            self.aggregate_post_lend_bps = None;
        }
    }

    #[cfg(test)]
    pub(super) fn mark_reclaim_for_test(
        &mut self,
        job_id: JobId,
        post_lend_bps: Option<u64>,
        reason: SpilloverReclaimReason,
    ) {
        if let Some(loan) = self.loans.get_mut(&job_id) {
            loan.measured_post_lend_bps = post_lend_bps;
            loan.measured_reclaim_reason = Some(reason);
        }
    }

    pub(super) fn reclaim_pending_for(&self, job_id: JobId) -> bool {
        self.loans
            .get(&job_id)
            .is_some_and(|loan| loan.measured_reclaim_reason.is_some())
    }

    pub(super) fn active_lent_connections(&self) -> usize {
        self.loans
            .values()
            .map(SpilloverLoanState::total_lent_connections)
            .sum()
    }

    fn measured_lent_connections(&self) -> usize {
        self.loans
            .values()
            .map(|loan| loan.measured_lent_connections)
            .sum()
    }

    pub(super) fn active_loan_count(&self) -> usize {
        self.loans.len()
    }

    /// Number of distinct jobs currently holding a spillover loan. Same
    /// value as `active_loan_count` (loans are keyed one-per-job); named for
    /// the dispatch-side cap check against `HOT_DISPATCH_SPILLOVER_MAX_JOBS`,
    /// so that call site reads as what it is instead of what it happens to
    /// share a value with.
    pub(super) fn distinct_loan_jobs(&self) -> usize {
        self.active_loan_count()
    }

    /// Whether `job_id` already holds a spillover loan — used to prefer
    /// concentrating new lanes onto jobs already spilling to, rather than
    /// admitting a fresh job while under the distinct-job cap.
    pub(super) fn holds_loan(&self, job_id: JobId) -> bool {
        self.loans.contains_key(&job_id)
    }

    pub(super) fn speed_snapshot(&self) -> (u64, u64, usize) {
        let pre = self.aggregate_pre_lend_bps.unwrap_or(0);
        let post = self.aggregate_post_lend_bps.unwrap_or(0);
        (pre, post, self.active_loan_count())
    }

    pub(super) fn update_speed_harm(
        &mut self,
        now: Instant,
        hot_speed_bps: u64,
        harm_percent: u64,
    ) -> bool {
        if self.measured_lent_connections() == 0 || hot_speed_bps == 0 {
            return false;
        }

        let Some(lent_at) = self.aggregate_lent_at else {
            return false;
        };
        let Some(pre_lend_bps) = self.aggregate_pre_lend_bps else {
            return false;
        };
        if now.saturating_duration_since(lent_at) < Duration::from_secs(2) || pre_lend_bps == 0 {
            return false;
        }

        self.aggregate_post_lend_bps = Some(hot_speed_bps);
        for loan in self.loans.values_mut() {
            if loan.measured_lent_connections == 0 {
                continue;
            }
            loan.measured_post_lend_bps = Some(hot_speed_bps);
        }

        let harm_threshold = pre_lend_bps.saturating_mul(100 - harm_percent) / 100;
        if hot_speed_bps >= harm_threshold {
            return false;
        }

        let mut newly_reclaimed = false;
        for loan in self.loans.values_mut() {
            if loan.measured_lent_connections == 0 {
                continue;
            }
            if loan.measured_reclaim_reason.is_none() {
                loan.measured_reclaim_reason = Some(SpilloverReclaimReason::SpeedHarm);
                newly_reclaimed = true;
            }
        }
        newly_reclaimed
    }
}

impl SpilloverLoanState {
    fn increment(&mut self, kind: SpilloverLoanKind) {
        match kind {
            SpilloverLoanKind::MeasuredUnderfill => {
                self.measured_lent_connections = self.measured_lent_connections.saturating_add(1)
            }
        }
    }

    fn decrement(&mut self, kind: SpilloverLoanKind) {
        match kind {
            SpilloverLoanKind::MeasuredUnderfill => {
                self.measured_lent_connections = self.measured_lent_connections.saturating_sub(1)
            }
        }
    }

    fn total_lent_connections(&self) -> usize {
        self.measured_lent_connections
    }
}

/// Cooperative signal asking every non-critical lane to return its
/// unrequested tail so the dispatcher can hand the freed connection to
/// completion-critical work on the next pass. A plain flag rather than a
/// targeted job id: once completion-critical demand goes unmet, ANY lane
/// running regular bytes — the hot job's own included — is fair game to
/// yield, not just one designated job's.
#[derive(Debug, Default)]
pub(super) struct HotShareYieldSignal {
    requested: AtomicBool,
}

impl HotShareYieldSignal {
    pub(super) fn request(&self) {
        self.requested.store(true, Ordering::Relaxed);
    }

    pub(super) fn clear(&self) {
        self.requested.store(false, Ordering::Relaxed);
    }

    pub(super) fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HotBestModeBlockReason {
    None,
    HotHasQueuedPrimary,
    LaneCapacityAvailable,
}

impl HotBestModeBlockReason {
    pub(super) fn as_code(self) -> usize {
        match self {
            Self::None => 0,
            Self::HotHasQueuedPrimary => 1,
            Self::LaneCapacityAvailable => 2,
        }
    }
}

/// Labels for the `hot_dispatch_best_mode_block_reason` snapshot code, in code
/// order. The metrics snapshot carries the raw integer; exposing the mapping
/// lets the Prometheus exporter render a state-set rather than an opaque gauge.
pub const HOT_BEST_MODE_BLOCK_REASON_LABELS: [&str; 3] =
    ["none", "hot_has_queued_primary", "lane_capacity_available"];

/// Labels for the `hot_dispatch_last_expansion_kind` snapshot code, in code
/// order. Code 0 means "no expansion has been recorded yet".
pub const HOT_EXPANSION_KIND_LABELS: [&str; 3] = ["none", "lane_start", "pipeline_promotion"];

/// Resolve a `hot_dispatch_best_mode_block_reason` code to its label, falling
/// back to `unknown` so an unmapped code stays visible instead of panicking a
/// scrape.
pub fn hot_best_mode_block_reason_label(code: usize) -> &'static str {
    HOT_BEST_MODE_BLOCK_REASON_LABELS
        .get(code)
        .copied()
        .unwrap_or("unknown")
}

/// Resolve a `hot_dispatch_last_expansion_kind` code to its label.
pub fn hot_expansion_kind_label(code: usize) -> &'static str {
    HOT_EXPANSION_KIND_LABELS
        .get(code)
        .copied()
        .unwrap_or("unknown")
}

#[cfg(test)]
mod hot_dispatch_label_tests {
    use super::*;

    #[test]
    fn labels_line_up_with_snapshot_codes() {
        for reason in [
            HotBestModeBlockReason::None,
            HotBestModeBlockReason::HotHasQueuedPrimary,
            HotBestModeBlockReason::LaneCapacityAvailable,
        ] {
            assert_ne!(
                hot_best_mode_block_reason_label(reason.as_code()),
                "unknown"
            );
        }
        assert_eq!(hot_best_mode_block_reason_label(99), "unknown");

        for kind in [
            HotExpansionKind::LaneStart,
            HotExpansionKind::PipelinePromotion,
        ] {
            assert_ne!(hot_expansion_kind_label(kind.as_code()), "unknown");
        }
        assert_eq!(hot_expansion_kind_label(0), "none");
        assert_eq!(hot_expansion_kind_label(99), "unknown");
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct IpReplacementCandidate {
    pub(super) old_key: ServerIpKey,
    pub(super) old_ewma_ms: f64,
    pub(super) baseline_ms: f64,
}

pub(super) enum IpReplacementTrialEvent {
    CandidateAcquired {
        job_id: JobId,
        candidate: IpReplacementCandidate,
        candidate_ip: IpAddr,
        lane: Box<weaver_nntp::BodyLaneLease>,
    },
    AcquireFailed,
    SameIpRejected,
    CandidateRejected,
    CandidateAccepted {
        old_key: ServerIpKey,
        samples: Vec<weaver_nntp::client::FetchAttemptTrace>,
    },
}

pub(super) struct DownloadLaneParked {
    pub(super) job_id: JobId,
    pub(super) mode: DownloadLaneMode,
    pub(super) spillover_loan_kind: Option<SpilloverLoanKind>,
    pub(super) completion_critical: bool,
    pub(super) reason: LaneParkReason,
    pub(super) release_connection_slot: bool,
    pub(super) release_ip_replacement_burst: bool,
}

pub(super) enum OwnedDownloadLaneEvent {
    AcquireFailed {
        lease: DownloadBatchLease,
        error: weaver_nntp::client::BlockingBodyLaneAcquireError,
    },
    BatchComplete {
        results: Vec<DownloadResult>,
        unrequested_works: Vec<DownloadWork>,
        stats: weaver_nntp::blocking::BlockingLaneStats,
        ack: std::sync::mpsc::SyncSender<()>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DownloadResultOrigin {
    NormalPrimary,
    Recovery,
    CompletionCriticalPrimary,
    CompletionCriticalRecovery,
    IpReplacementTrial,
}

impl DownloadResultOrigin {
    pub(super) fn from_work(is_recovery: bool, completion_critical: bool) -> Self {
        match (is_recovery, completion_critical) {
            (false, false) => Self::NormalPrimary,
            (true, false) => Self::Recovery,
            (false, true) => Self::CompletionCriticalPrimary,
            (true, true) => Self::CompletionCriticalRecovery,
        }
    }

    pub(super) fn is_recovery(self) -> bool {
        matches!(self, Self::Recovery | Self::CompletionCriticalRecovery)
    }

    pub(super) fn is_completion_critical(self) -> bool {
        matches!(
            self,
            Self::CompletionCriticalPrimary | Self::CompletionCriticalRecovery
        )
    }

    pub(super) fn counts_for_hot_primary(self) -> bool {
        matches!(self, Self::NormalPrimary)
    }
}

/// Result of a download task.
pub(super) struct DownloadResult {
    pub(super) segment_id: SegmentId,
    pub(super) runtime_generation: u64,
    pub(super) data: std::result::Result<DownloadPayload, DownloadError>,
    pub(super) attempts: Vec<weaver_nntp::client::FetchAttemptTrace>,
    pub(super) lane_observation: Option<DownloadLaneObservation>,
    /// Server that successfully served this payload, if known.
    pub(super) source_server_idx: Option<usize>,
    /// Scheduler attribution for metrics, warmup, and retry semantics.
    pub(super) origin: DownloadResultOrigin,
    /// How many times this segment has been retried so far.
    pub(super) retry_count: u32,
    /// Servers intentionally excluded for this fetch attempt.
    pub(super) exclude_servers: Vec<usize>,
    /// Whether this result releases one NNTP connection dispatch slot.
    pub(super) release_connection_slot: bool,
}

/// A delayed retry re-entering the download queue, tagged with the NNTP pool
/// generation it was scheduled under so the orchestrator can drop stale
/// `exclude_servers` indices after a `RebuildNntp` reshaped the pool.
pub(in crate::pipeline) struct RetryWork {
    pub(in crate::pipeline) scheduled_pool_generation: u64,
    pub(in crate::pipeline) infrastructure_retry: bool,
    pub(in crate::pipeline) work: DownloadWork,
}

#[derive(Debug, Clone)]
pub(super) struct DownloadWaitStatus {
    pub(super) reason: &'static str,
    pub(super) retry_at_epoch_ms: Option<f64>,
    pub(super) pending_count: usize,
}

#[derive(Debug, Clone)]
pub(super) struct DownloadLaneObservation {
    pub(super) server_idx: Option<usize>,
    pub(super) mode: DownloadLaneMode,
    pub(super) supports_pipelining: bool,
    pub(super) rtt: Option<Duration>,
    pub(super) batch_complete: bool,
    pub(super) batch_clean: bool,
    pub(super) batch_response_count: u64,
    pub(super) unresolved_count: u64,
    pub(super) connection_discarded: bool,
}

pub(super) enum DownloadPayload {
    #[allow(dead_code)]
    Raw(Bytes),
    Decoded(DecodeResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DownloadFailureKind {
    ArticleNotFound,
    CapacityUnavailable,
    ServerQuota,
    LaneUnavailable,
    Unrequested,
    ConnectionEstablishment,
    EstablishedTransport,
    Auth,
    ContentOrProtocol,
}

impl DownloadFailureKind {
    fn preserves_article_retry_budget(&self) -> bool {
        matches!(
            self,
            Self::CapacityUnavailable
                | Self::ServerQuota
                | Self::LaneUnavailable
                | Self::Unrequested
                | Self::ConnectionEstablishment
                | Self::Auth
        )
    }

    fn infrastructure_wait_reason(&self) -> Option<&'static str> {
        match self {
            Self::CapacityUnavailable => Some("provider connection capacity unavailable"),
            Self::ServerQuota => Some("NNTP server quota blocked"),
            Self::LaneUnavailable => Some("no eligible NNTP server"),
            Self::Unrequested => Some("NNTP lane replaced"),
            Self::ConnectionEstablishment => Some("NNTP connection unavailable"),
            Self::EstablishedTransport => Some("NNTP BODY transport unavailable"),
            Self::Auth => Some("NNTP authentication blocked"),
            Self::ArticleNotFound | Self::ContentOrProtocol => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct DownloadFailure {
    pub(super) kind: DownloadFailureKind,
    pub(super) message: String,
    pub(super) retry_after: Option<Duration>,
    pub(super) quota_rejection: Option<weaver_nntp::transfer::QuotaRejection>,
}

impl DownloadFailure {
    pub(super) fn new(kind: DownloadFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_after: None,
            quota_rejection: None,
        }
    }

    pub(super) fn server_quota(
        message: impl Into<String>,
        rejection: weaver_nntp::transfer::QuotaRejection,
    ) -> Self {
        let retry_after = rejection
            .retry_at
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));
        Self {
            kind: DownloadFailureKind::ServerQuota,
            message: message.into(),
            retry_after,
            quota_rejection: Some(rejection),
        }
    }

    fn infrastructure_kind(
        error: &weaver_nntp::NntpError,
        transport_kind: DownloadFailureKind,
    ) -> Option<DownloadFailureKind> {
        use weaver_nntp::NntpError;

        match error {
            NntpError::PoolExhausted | NntpError::PoolShutdown | NntpError::TooManyConnections => {
                Some(DownloadFailureKind::CapacityUnavailable)
            }
            NntpError::AuthenticationFailed
            | NntpError::AuthenticationRejected
            | NntpError::AuthenticationRequired
            | NntpError::AccessDenied => Some(DownloadFailureKind::Auth),
            NntpError::ServiceUnavailable
            | NntpError::Timeout
            | NntpError::SoftTimeout(_)
            | NntpError::ConnectionClosed
            | NntpError::ServerDisconnectedMidBody
            | NntpError::TruncatedMultilineBody
            | NntpError::MalformedMultilineTerminator
            | NntpError::Io(_) => Some(transport_kind),
            _ => None,
        }
    }

    pub(super) fn from_lane_acquire_failure(error: Option<&weaver_nntp::NntpError>) -> Self {
        use weaver_nntp::NntpError;

        let Some(error) = error else {
            return Self::new(
                DownloadFailureKind::LaneUnavailable,
                "failed to acquire BODY lane",
            );
        };
        if let NntpError::QuotaBlocked(rejection) = error {
            return Self::server_quota(error.to_string(), rejection.as_ref().clone());
        }

        let kind = Self::infrastructure_kind(error, DownloadFailureKind::ConnectionEstablishment)
            .unwrap_or(match error {
                NntpError::NoSuchGroup
                | NntpError::NoGroupSelected
                | NntpError::CommandNotRecognized
                | NntpError::TlsRequired
                | NntpError::UnexpectedResponse { .. }
                | NntpError::MalformedResponse(_) => DownloadFailureKind::ContentOrProtocol,
                _ => DownloadFailureKind::LaneUnavailable,
            });

        Self::new(kind, error.to_string())
    }

    pub(super) fn from_nntp(error: weaver_nntp::NntpError) -> Self {
        use weaver_nntp::NntpError;

        if let NntpError::QuotaBlocked(rejection) = &error {
            return Self::server_quota(error.to_string(), rejection.as_ref().clone());
        }
        if matches!(&error, NntpError::BodyNotRequestedDueToQuota { .. }) {
            return Self::new(DownloadFailureKind::Unrequested, error.to_string());
        }

        let kind = match &error {
            NntpError::ArticleNotFound
            | NntpError::NoSuchArticle { .. }
            | NntpError::NoArticleWithNumber => DownloadFailureKind::ArticleNotFound,
            error => Self::infrastructure_kind(error, DownloadFailureKind::EstablishedTransport)
                .unwrap_or(DownloadFailureKind::ContentOrProtocol),
        };

        Self::new(kind, error.to_string())
    }
}

#[derive(Debug, Clone)]
pub(super) enum DownloadError {
    Fetch(DownloadFailure),
    Decode {
        raw_size: u64,
        error: String,
        crc_mismatch: bool,
    },
}

impl DownloadError {
    #[cfg(test)]
    pub(super) fn fetch(kind: DownloadFailureKind, message: impl Into<String>) -> Self {
        Self::Fetch(DownloadFailure::new(kind, message))
    }

    pub(super) fn from_nntp(error: weaver_nntp::NntpError) -> Self {
        Self::Fetch(DownloadFailure::from_nntp(error))
    }

    /// Whether this failure left the NNTP connection in a known-good state:
    /// the server's response was read to completion and the socket can carry
    /// the next BODY.
    ///
    /// `ArticleNotFound` (430) is a complete, bodyless server answer — it says
    /// nothing about the connection. Treating it as a transport fault used to
    /// QUIT the TLS session, abandon the rest of the leased batch, and block
    /// the server's pipelining proof, all because one article lives on another
    /// provider. `ServerQuota` / `Unrequested` are local policy outcomes where
    /// no BODY was ever issued, so they are clean for the same reason (they
    /// still park the lane, just without discarding it).
    ///
    /// Decode failures stay dirty: the yEnc decoder can fail on a body the
    /// transport never finished delivering.
    pub(super) fn leaves_connection_clean(&self) -> bool {
        match self {
            Self::Fetch(failure) => matches!(
                failure.kind,
                DownloadFailureKind::ArticleNotFound
                    | DownloadFailureKind::ServerQuota
                    | DownloadFailureKind::Unrequested
            ),
            Self::Decode { .. } => false,
        }
    }
}

/// Whether a download outcome leaves the lane's connection reusable.
///
/// See [`DownloadError::leaves_connection_clean`].
pub(super) fn download_outcome_keeps_connection(
    data: &std::result::Result<DownloadPayload, DownloadError>,
) -> bool {
    match data {
        Ok(_) => true,
        Err(error) => error.leaves_connection_clean(),
    }
}

/// Successful download payload waiting for decode scheduling.
pub(super) struct PendingDecodeWork {
    pub(super) segment_id: SegmentId,
    pub(super) raw: Bytes,
    pub(super) source_server_idx: Option<usize>,
    pub(super) exclude_servers: Vec<usize>,
}

/// Progress update from a health probe task.
pub(super) struct ProbeUpdate {
    pub(super) job_id: JobId,
    /// Total probes attempted so far.
    pub(super) total: usize,
    /// Number of missing articles found so far.
    pub(super) missed: usize,
    /// True when the probe is complete (final update).
    pub(super) done: bool,
    /// True when probe confirmation hit a non-authoritative transport/protocol
    /// failure and the round should be discarded.
    pub(super) inconclusive: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CapacityProbeCompletion {
    pub(super) generation: u64,
    pub(super) outcome: weaver_nntp::CapacityProbeOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum RefreshReason {
    CoverageExpansion,
    PostExtraction,
    IdentityRebind,
    ValidationFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RarRefreshRequest {
    pub(super) target_completed_volume: u32,
    pub(super) reason: RefreshReason,
}

impl RarRefreshRequest {
    fn merge(&mut self, other: Self) {
        self.target_completed_volume = self
            .target_completed_volume
            .max(other.target_completed_volume);
        self.reason = self.reason.max(other.reason);
    }
}

#[derive(Debug, Clone)]
pub(super) enum RarRefreshError {
    CapacityPressure(String),
    Other(String),
}

impl RarRefreshError {
    fn from_message(message: String) -> Self {
        if capacity::is_fd_capacity_error_message(&message) {
            Self::CapacityPressure(message)
        } else {
            Self::Other(message)
        }
    }

    fn is_capacity_pressure(&self) -> bool {
        matches!(self, Self::CapacityPressure(_))
    }

    fn message(&self) -> &str {
        match self {
            Self::CapacityPressure(message) | Self::Other(message) => message,
        }
    }
}

impl std::fmt::Display for RarRefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct RarRefreshState {
    pub(super) in_flight: Option<RarRefreshRequest>,
    pub(super) queued: Option<RarRefreshRequest>,
    pub(super) latest_completed_volume: u32,
    pub(super) refreshed_volumes: BTreeSet<u32>,
    pub(super) structure_dirty: bool,
    pub(super) last_error: Option<RarRefreshError>,
    /// Fingerprint of (facts generation, fact volumes, plan volumes, plan
    /// waits) at the last successful refresh completion. A coverage gap —
    /// facts the plan has not absorbed — normally spawns a follow-up refresh,
    /// but when a completed refresh lands on the same fingerprint as the one
    /// before it, the follow-up would recompute the identical answer from
    /// identical inputs: a gap the plan CANNOT close (a missing chain link,
    /// say) would otherwise respawn itself forever at actor speed. Matching
    /// fingerprints park the gap instead; any real change — a new fact, a
    /// changed fact, plan progress — changes the fingerprint and re-arms it.
    pub(super) last_completion_fingerprint: Option<u64>,
}

pub(super) struct ComputedRarSetState {
    pub(super) plan: RarDerivedPlan,
    pub(super) headers: Vec<u8>,
    pub(super) rebuild_source: archive::topology::RarTopologyRebuildSource,
    /// The volumes this refresh actually integrated into the header view.
    ///
    /// Not the plan's `complete_volumes`: that set is derived from the facts
    /// ledger, and a restored job can hold volumes on disk that the ledger
    /// never recorded. Refresh coverage has to measure what the refresh saw,
    /// or a coverage demand it can never satisfy re-issues itself after every
    /// completion.
    pub(super) integrated_volumes: BTreeSet<u32>,
}

pub(super) struct RarRefreshDone {
    pub(super) job_id: JobId,
    pub(super) set_name: String,
    pub(super) request: RarRefreshRequest,
    pub(super) extraction_generation: u64,
    pub(super) result: Result<ComputedRarSetState, RarRefreshError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::pipeline) enum RarCapacityRetryKind {
    Refresh,
    Extraction,
    FullSetExtraction,
}

pub(in crate::pipeline) struct RarCapacityRetry {
    pub(super) job_id: JobId,
    pub(super) set_name: String,
    pub(super) kind: RarCapacityRetryKind,
}

pub(crate) enum RarPasswordAttemptError {
    Rar(unrar_rs::RarError),
    Fatal(String),
}

pub(crate) struct ArchivePasswordSelection<T> {
    pub(crate) value: T,
    pub(crate) selected_password: Option<String>,
}

impl<T> ArchivePasswordSelection<T> {
    pub(crate) fn new(value: T, selected_password: Option<String>) -> Self {
        Self {
            value,
            selected_password,
        }
    }
}

impl std::fmt::Display for RarPasswordAttemptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rar(error) => write!(f, "{error}"),
            Self::Fatal(error) => f.write_str(error),
        }
    }
}

impl From<unrar_rs::RarError> for RarPasswordAttemptError {
    fn from(value: unrar_rs::RarError) -> Self {
        Self::Rar(value)
    }
}

/// Result of a background extraction task.
pub(super) struct BatchExtractionOutcome {
    pub(super) extracted: Vec<String>,
    pub(super) failed: Vec<(String, String)>,
    pub(super) selected_password: Option<String>,
    pub(super) phase_completed_bytes: u64,
}

pub(super) struct FullSetExtractionOutcome {
    pub(super) extracted: Vec<String>,
    pub(super) failed: Vec<(String, String)>,
    pub(super) selected_password: Option<String>,
}

/// Bounded metadata-discovery progress for one PAR2 candidate.
///
/// This is deliberately separate from `promoted`: probing an indexless
/// volume queues only its leading article, while promotion means the whole
/// volume is eligible to move out of the recovery queue.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) enum Par2DiscoveryState {
    #[default]
    Unseen,
    PrefixProbeQueued,
    PrefixProbed {
        set_ids: Vec<par2_rs::RecoverySetId>,
    },
    ProbeInconclusive,
    MetadataCarrierQueued {
        target_set_id: Option<par2_rs::RecoverySetId>,
        set_ids: Vec<par2_rs::RecoverySetId>,
    },
    Parsed {
        set_ids: Vec<par2_rs::RecoverySetId>,
    },
    Exhausted {
        set_ids: Vec<par2_rs::RecoverySetId>,
    },
}

impl Par2DiscoveryState {
    pub(super) fn observed_set_ids(&self) -> &[par2_rs::RecoverySetId] {
        match self {
            Self::PrefixProbed { set_ids }
            | Self::MetadataCarrierQueued { set_ids, .. }
            | Self::Parsed { set_ids }
            | Self::Exhausted { set_ids } => set_ids,
            _ => &[],
        }
    }

    pub(super) fn work_is_queued(&self) -> bool {
        matches!(
            self,
            Self::PrefixProbeQueued | Self::MetadataCarrierQueued { .. }
        )
    }

    pub(super) fn candidate_probe_is_terminal(&self) -> bool {
        matches!(
            self,
            Self::PrefixProbed { .. } | Self::Parsed { .. } | Self::Exhausted { .. }
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Par2FileRuntime {
    pub(super) filename: String,
    /// How many recovery blocks this file *claims* to carry: the count spelled
    /// out in a `volNN+CC` name, or an estimate derived from its encoded size.
    ///
    /// An advertisement, never evidence. A volume that lost an article
    /// advertises exactly what an intact one does, so this may decide what is
    /// worth fetching and may never decide whether a repair can go ahead.
    pub(super) recovery_blocks: u32,
    /// How many recovery blocks this file has *proven* it carries: packets that
    /// were read and whose own MD5 checked out, whether on completion or by
    /// reading back past a hole.
    ///
    /// Kept apart from the advertised count because the two disagree for
    /// exactly the volumes where it matters — a volume that stranded holding
    /// three of its twenty-four blocks. Crediting it with the other twenty-one
    /// leaves the arithmetic believing a repair is affordable while nothing is
    /// left to download, which is a job that waits forever.
    pub(super) validated_recovery_blocks: u32,
    /// A completed parse or read-back reached a final answer for this file's
    /// recovery capacity.  Once set, even zero is authoritative: a malformed
    /// or metadata-only carrier must never fall back to its filename claim.
    pub(super) recovery_capacity_accounted: bool,
    pub(super) promoted: bool,
    /// Recovery packets were read back off a volume that can no longer
    /// complete, and `validated_recovery_blocks` is how many of them validated.
    /// Set only when at least one block was recovered, because it is also what
    /// makes the file count toward the recovery available to a repair.
    pub(super) salvaged: bool,
    /// The file's `received_bytes()` when it was last read back, or `None` if it
    /// never was.
    ///
    /// One read-back per generation of bytes, not one ever: nothing about a
    /// volume changes between completion-gate entries while it sits still, and
    /// the gate is entered many times per job — but a volume that strands, is
    /// read back short, and then takes more articles before stranding again has
    /// more on disk than the first read saw.
    pub(super) salvaged_at_received_bytes: Option<u64>,
    /// How many validated recovery blocks this file contributed to each set it
    /// carries packets for.
    ///
    /// A file whose packets all answer to one set is described by
    /// `validated_recovery_blocks` alone. One carrying several sets' packets has
    /// no single answer, and its blocks are nonetheless merged into each of
    /// those sets — so the arithmetic that decides whether a repair is possible
    /// has to be able to see the same blocks the repairer already holds.
    pub(super) recovery_blocks_by_set: HashMap<par2_rs::RecoverySetId, u32>,
    /// Which recovery set this PAR2 file speaks for, once its packets have
    /// actually been read. A file carrying packets for more than one set stays
    /// `None`, because no single set owns it.
    pub(super) recovery_set_id: Option<par2_rs::RecoverySetId>,
    /// Whether packets have been read from this file. A packet-read file with
    /// no single set ID is deliberately not grouped by filename.
    pub(super) recovery_set_packets_read: bool,
    /// Explicit progress through index/indexless metadata discovery.
    pub(super) discovery: Par2DiscoveryState,
    /// This file was admitted as a PAR2 candidate from a structurally valid
    /// header despite its NZB filename not being PAR2-shaped. Full packet
    /// parsing remains required before it contributes metadata or recovery.
    pub(super) signature_candidate: bool,
    /// `MetadataCarrierQueued` is also used by the ordinary explicit-index
    /// bootstrap. Keep its provenance separate so only completion-driven
    /// metadata work receives completion-critical scheduling and UI state.
    pub(super) metadata_carrier_completion_critical: bool,
    /// Set-specific full-file metadata attempts. This prevents a completed
    /// carrier from being selected repeatedly when it contains valid packets
    /// but not enough critical metadata to construct that set.
    pub(super) metadata_targets_attempted: HashSet<par2_rs::RecoverySetId>,
    /// Article ordinals already used for bounded prefix probing. Full-carrier
    /// escalation skips them because their decoded bytes are already retained.
    pub(super) discovery_probe_ordinals: HashSet<u32>,
}

/// What a job knows about one recovery set it has encountered.
///
/// A posting may carry several independent recovery sets, each describing its
/// own files and sharing no bytes with the others. Only one of them is served,
/// so the rest have to be remembered rather than forgotten: their volumes must
/// not be mistaken for the served set's capacity, and the files they describe
/// must not be reported as if nothing ever protected them.
#[derive(Debug, Clone, Default)]
pub(super) struct Par2SetSummary {
    /// Whether an index of this set was actually parsed.
    ///
    /// A set first met through a foreign packet inside somebody else's volume
    /// is *known* but has no descriptions at all, so it can never be served —
    /// it exists here to be named in the warning and to attribute that volume.
    pub(super) describes: bool,
    /// The file whose packets described this set, and its position in the
    /// posting. The position orders independent verification passes regardless
    /// of arrival order.
    pub(super) index_filename: String,
    pub(super) index_file_index: u32,
    /// The index name with its `.par2` and any `.volNNN+CCC` part removed —
    /// what groups a never-parsed volume onto this set by name alone.
    pub(super) base_name: Option<String>,
    /// Sanitized names of the files this set protects.
    pub(super) described_filenames: Vec<String>,
    /// How much payload this set protects. Retained for diagnostics and for
    /// compatibility selection before the completion gate takes over.
    pub(super) described_bytes: u64,
    /// Files whose packets were observed to belong to this set.
    pub(super) volume_file_indices: HashSet<u32>,
}

#[derive(Default)]
pub(super) struct Par2SetRuntime {
    /// The parsed recovery set. `None` until an index of this set was parsed.
    pub(super) set: Option<Arc<Par2FileSet>>,
    /// The completion gate has reached a final answer for this recovery set.
    ///
    /// A later index can add a different set and reopen the job aggregate, but
    /// it must not make this set read the same bytes again.  Its own verdict
    /// and reconciliation latch therefore live with the set rather than with
    /// the job.
    pub(super) settled: bool,
    /// A final answer that could not verify or repair this set.  The gate keeps
    /// processing later sets before turning these failures into the job result.
    pub(super) failure: Option<String>,
    /// Damage observed while deciding this set.  The aggregate reports one
    /// job-level verification metric after every servable set has settled.
    pub(super) missing_blocks: u32,
    /// Whether any pass for this set required repair.  A clean post-repair pass
    /// does not erase that fact from the aggregate verification result.
    pub(super) needed_repair: bool,
    /// What this set describes and which volumes spoke for it.
    pub(super) summary: Par2SetSummary,
    /// Stateful assessment/repair engine. It intentionally owns no open file
    /// handles, and is invalidated before payload paths are rewritten.
    pub(super) session: Option<par2_rs::Par2RepairSession>,
    /// Last time the retained session was taken or restored, for global LRU
    /// eviction when the shared retained-state budget is exceeded.
    pub(super) session_last_used: Option<Instant>,
    /// Scan state carried between repairer passes over this set: the carry the
    /// last completed `Par2Repairer` pass returned, or one built from this
    /// module's own authoritative verification. Seeded into the next repairer
    /// run's options so an analysis or repair does not re-read bytes a
    /// previous pass already hashed. par2-rs validates a consumed carry
    /// against per-file stat fingerprints and re-checks bytes before any
    /// mutating request, so a stale stash costs nothing but the seed.
    pub(super) scan_carry: Option<std::sync::Arc<par2_rs::ScanCarry>>,
    /// Completed files whose current identity/checksum evidence was admitted
    /// to the retained session.
    pub(super) session_evidence_file_ids: HashSet<NzbFileId>,
    /// Completion-gate entries that found protected files still incomplete
    /// *after* the job's PAR2 verdict was settled — a state only a
    /// reconciliation defect of ours can produce. One retry is allowed; the
    /// second entry fails the job with a named bug report rather than
    /// re-reading the whole recovery set on every lap forever. Reset wherever
    /// a verdict is taken or reopened.
    pub(super) post_verdict_reconcile_attempts: u32,
}

/// A positive authoritative binding whose PAR2 slice CRCs make streamed MD5
/// unnecessary. It is rebuilt only after metadata or identity changes.
#[derive(Clone, Copy, Debug)]
pub(super) struct Par2Md5SubstitutionBinding {
    pub(super) recovery_set_id: par2_rs::RecoverySetId,
    pub(super) par2_file_id: par2_rs::FileId,
}

/// One direct-store post-repair read-back owned by the post-processing lane.
///
/// The PAR grid and direct provider are snapshotted before submission. The
/// pipeline actor retains only this generation fence and applies the terminal
/// verdict after the worker returns.
pub(super) struct DirectPostRepairWork {
    pub(super) work_id: u64,
    pub(super) recovery_set_id: par2_rs::RecoverySetId,
    /// When this ticket was handed to the detached task, so the completion
    /// handler can log how long the read-back actually took. The gap between
    /// submission and completion is exactly the window that once produced an
    /// unexplained multi-second stall with nothing in the logs to explain it.
    pub(super) submitted_at: std::time::Instant,
}

pub(super) struct DirectPostRepairWorkDone {
    pub(super) job_id: JobId,
    pub(super) work_id: u64,
    pub(super) recovery_set_id: par2_rs::RecoverySetId,
    pub(super) result: Result<par2_rs::VerificationResult, String>,
}

/// The pre-repair verdict and the repair's own write set, carried across a
/// direct-store repair so the post-repair read-back can be selective instead
/// of re-reading the whole recovery set.
///
/// This is the direct-store mirror of what
/// [`Pipeline::verify_repaired_par2_files_with_placement`] does for a
/// conventional set: that function is handed `pre_repair` by its caller,
/// which still has the verification in a local variable a few lines above.
/// The direct-store gate has no such luxury — the pre-repair verdict is
/// computed in [`Pipeline::resolve_direct_sets_before_par2_repairer_for_set`],
/// the repair runs, and the job re-enters the gate on a **later** completion
/// check to read the result back, by which point the local variable is long
/// gone. This struct is what stands in for it across that gap.
///
/// Keyed by job rather than by recovery set: a job serves one recovery set
/// through this gate at a time (see [`Pipeline::direct_sets_repaired_in_place`]),
/// so one carry is all a job ever needs, and `recovery_set_id` is kept
/// alongside it so a consumer can tell a fresh carry from a stale one instead
/// of trusting the map key alone.
pub(super) struct DirectPostRepairCarry {
    pub(super) recovery_set_id: par2_rs::RecoverySetId,
    pub(super) pre_repair: par2_rs::VerificationResult,
    pub(super) write_set: Vec<par2_rs::FileId>,
}

#[derive(Default)]
pub(super) struct Par2RuntimeState {
    /// Every recovery set this job has met. Each parsed, described entry gets
    /// its own completion-gate pass; entries without an index remain only for
    /// attribution and an operator warning.
    pub(super) sets: HashMap<par2_rs::RecoverySetId, Par2SetRuntime>,
    /// The set currently exposed through the compatibility helpers while its
    /// own gate pass is running.
    pub(super) served: Option<par2_rs::RecoverySetId>,
    pub(super) files: HashMap<u32, Par2FileRuntime>,
    /// Completion-time checksums retained only long enough to seed a session
    /// opened after a payload file finished downloading.
    pub(super) completed_checksums: HashMap<NzbFileId, CompletedFileChecksum>,
    /// Positive bindings only: an absent entry keeps streaming MD5 without
    /// retrying a recovery-set scan for every decoded article.
    pub(super) md5_substitution_bindings: HashMap<NzbFileId, Par2Md5SubstitutionBinding>,
    /// Monotonic parsed-grid admission and its immutable lease snapshot.
    /// Rebuilt only when parsed metadata changes, never while leasing work.
    pub(super) admitted_checkpoint_sizes: BTreeSet<u64>,
    pub(super) checkpoint_plan: Option<weaver_yenc::CheckpointPlan>,
    /// Monotonic lease gate: every declared explicit index is parsed or
    /// exhausted. Indexless discovery remains completion-bounded.
    pub(super) explicit_index_bootstrap_closed: bool,
    /// Whether the job has already named its indexless recovery sets. Cleared
    /// whenever a set is newly met so a changed picture is reported once.
    pub(super) unserved_sets_warned: bool,
    /// Whether the job has already reported that every PAR2 metadata candidate
    /// it could promote is finished. The completion gate asks for metadata on
    /// every entry, and the answer stops changing once the last candidate has
    /// settled, so the operator hears it once rather than on every lap.
    pub(super) metadata_exhausted_warned: bool,
}

impl Par2RuntimeState {
    pub(super) fn served_set_id(&self) -> Option<par2_rs::RecoverySetId> {
        self.served
    }

    pub(super) fn served(&self) -> Option<&Par2SetRuntime> {
        self.served.and_then(|set_id| self.set_runtime(set_id))
    }

    pub(super) fn served_mut(&mut self) -> Option<&mut Par2SetRuntime> {
        self.served.and_then(|set_id| self.set_runtime_mut(set_id))
    }

    pub(super) fn set_runtime(&self, set_id: par2_rs::RecoverySetId) -> Option<&Par2SetRuntime> {
        self.sets.get(&set_id)
    }

    pub(super) fn set_runtime_mut(
        &mut self,
        set_id: par2_rs::RecoverySetId,
    ) -> Option<&mut Par2SetRuntime> {
        self.sets.get_mut(&set_id)
    }

    pub(super) fn ensure_set_runtime(
        &mut self,
        set_id: par2_rs::RecoverySetId,
    ) -> &mut Par2SetRuntime {
        self.sets.entry(set_id).or_default()
    }

    /// Returns recovery set IDs in the deterministic order later per-set
    /// iteration relies on.
    pub(super) fn ordered_set_ids(&self) -> Vec<par2_rs::RecoverySetId> {
        let mut set_ids = self.sets.keys().copied().collect::<Vec<_>>();
        set_ids.sort_by_key(|set_id| {
            (
                self.sets
                    .get(set_id)
                    .map(|set_runtime| set_runtime.summary.index_file_index)
                    .unwrap_or_default(),
                *set_id.as_bytes(),
            )
        });
        set_ids
    }
}

pub(super) enum ExtractionDone {
    /// Batch extraction of specific members completed.
    Batch {
        job_id: JobId,
        set_name: String,
        attempted: Vec<String>,
        result: Result<BatchExtractionOutcome, String>,
    },
    /// Full set extraction completed (all volumes present).
    FullSet {
        job_id: JobId,
        set_name: String,
        result: Result<FullSetExtractionOutcome, String>,
    },
}

#[derive(Debug)]
pub(super) struct MoveToCompleteResult {
    pub(super) moved_entries: u32,
    /// Delivered files the deobfuscation pass renamed on the way out.
    pub(super) renamed_members: u32,
}

/// A final-move refusal that must never enter terminal post-processing versus
/// an ordinary move failure, whose legacy script handling remains intact.
#[derive(Debug)]
pub(super) enum MoveToCompleteFailure {
    Security(String),
    Operational(String),
}

impl std::fmt::Display for MoveToCompleteFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Security(message) | Self::Operational(message) => formatter.write_str(message),
        }
    }
}

pub(super) struct MoveToCompleteDone {
    pub(super) job_id: JobId,
    pub(super) dest: PathBuf,
    pub(super) result: Result<MoveToCompleteResult, MoveToCompleteFailure>,
}

pub(super) enum TerminalPostProcessingEvent {
    Started(JobId),
    Done(TerminalPostProcessingDone),
}

pub(super) struct TerminalPostProcessingDone {
    pub(super) job_id: JobId,
    pub(super) primary_failure: Option<String>,
    pub(super) result: Result<
        crate::post_processing::executor::JobPostProcessingReport,
        crate::post_processing::executor::PostProcessingExecutorError,
    >,
}

/// Map a yEnc CRC outcome onto the pipeline's `crc_valid` flag.
///
/// `crc_valid` here means "not known bad", which is what the CRC-error metric
/// and the segment event are counting. An article whose `=yend` carried no
/// usable `crc32=`/`pcrc32=` is *unverifiable*, not corrupt, and must not be
/// reported as a CRC failure — use [`weaver_yenc::CrcVerification::Verified`]
/// directly wherever real verification is the question.
pub(super) fn crc_not_mismatched(status: weaver_yenc::CrcVerification) -> bool {
    status != weaver_yenc::CrcVerification::Mismatch
}

/// How a segment was encoded on the wire, and therefore what evidence it
/// carries into the pipeline.
///
/// This is not cosmetic. A yEnc article declares where its bytes belong and
/// what they check to, and the whole zero-I/O verification story is built on
/// that. A uuencode article declares neither, so several stages that are
/// correct for yEnc are unsound for uuencode and gate on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SegmentEncoding {
    /// yEnc: per-part offsets and CRC, block-aligned CRC segments.
    Yenc,
    /// uuencode: decoded bytes and a segment index, nothing more.
    Uu(UuSegmentFacts),
}

/// The only things a uuencode article establishes beyond its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct UuSegmentFacts {
    /// A line in this part failed to decode. Its bytes are kept regardless —
    /// PAR2 adjudicates, not the decoder — but the file is no longer clean.
    pub(super) damaged: bool,
    /// This part carried the uuencode `end` marker, so it ends the file.
    pub(super) ended: bool,
}

impl SegmentEncoding {
    /// uuencode segments cannot be placed from the article itself, so they are
    /// assembled sequentially rather than by declared offset.
    pub(super) fn is_uu(self) -> bool {
        matches!(self, Self::Uu(_))
    }

    pub(super) fn uu_facts(self) -> Option<UuSegmentFacts> {
        match self {
            Self::Uu(facts) => Some(facts),
            Self::Yenc => None,
        }
    }
}

/// Storage for a uuencode part waiting for its prefix.
///
/// Disk-backed entries retain their trusted decoded length alongside the
/// temporary path. The spool file's metadata is never used for allocation or
/// validation when the part is released.
enum UuParkedEntry {
    Memory(DecodedChunk),
    Spilled {
        path: tempfile::TempPath,
        decoded_bytes: usize,
    },
}

impl UuParkedEntry {
    fn decoded_bytes(&self) -> usize {
        match self {
            Self::Memory(chunk) => chunk.len_bytes(),
            Self::Spilled { decoded_bytes, .. } => *decoded_bytes,
        }
    }

    fn is_spilled(&self) -> bool {
        matches!(self, Self::Spilled { .. })
    }
}

/// Remove every stale child of the transient UU spool root without following
/// links outside it. UU assembly checkpoints are not restored after restart.
pub(super) fn clear_stale_uu_park_root(root: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(std::io::Error::other(format!(
                "UU spool root is not a directory: {}",
                root.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(root)?;
        }
        Err(error) => return Err(error),
    }

    for child in std::fs::read_dir(root)? {
        let child = child?;
        let path = child.path();
        let file_type = child.file_type()?;
        if file_type.is_dir() {
            std::fs::remove_dir_all(path)?;
        } else {
            // This removes a symlink itself rather than its target.
            std::fs::remove_file(path)?;
        }
    }
    Ok(())
}

/// Remove a job's spool directory once its final transient file is gone.
///
/// An occupied directory belongs to another still-parked part, and a missing
/// one means a prior cleanup already won the race; neither is an error.
pub(super) fn remove_empty_uu_park_dir(root: &Path, job_id: JobId) {
    let path = root.join(job_id.0.to_string());
    if let Err(error) = std::fs::remove_dir(&path)
        && error.kind() != std::io::ErrorKind::NotFound
        && error.kind() != std::io::ErrorKind::DirectoryNotEmpty
    {
        warn!(
            job_id = job_id.0,
            path = %path.display(),
            error = %error,
            "failed to remove empty UU spool directory"
        );
    }
}

/// Sequential-assembly state for one uuencode file.
///
/// A uuencode part's position is the cumulative *decoded* length of every part
/// before it, which is only knowable once that whole prefix has arrived. So
/// assembly is strictly sequential: a part that arrives early has to wait, and
/// it has to wait until its prefix provides an offset.
/// That is the structural difference from yEnc, whose out-of-order parts can be
/// persisted immediately at the offset their own header declares.
#[derive(Default)]
pub(super) struct UuFileAssembly {
    /// The next segment ordinal the cursor will place.
    pub(super) next_index: u32,
    /// The decoded byte offset that ordinal will be placed at.
    pub(super) next_offset: u64,
    /// Parts that arrived ahead of the cursor, keyed by ordinal.
    ///
    /// Bounded by the same per-file limit the write reorder buffer uses; see
    /// the admission check at the placement seam for what happens on overflow.
    parked: BTreeMap<u32, UuParkedEntry>,
    /// A part decoded with damage, or a gap was closed by shifting later parts
    /// down over a part that never arrived.
    pub(super) damaged: bool,
    /// The uuencode `end` marker was seen on some part.
    pub(super) saw_end: bool,
    /// The name from the uuencode `begin` header, from whichever part carried
    /// it. First non-empty wins.
    ///
    /// yEnc repeats `name=` on every article, so the article that completes a
    /// file always carries the name to the identity seam. uuencode states it
    /// once, on the part that opens the body — which is normally the first
    /// part, and is emphatically not the last. Both reference decoders apply
    /// the name whichever part it arrives on, so it is retained here rather
    /// than read off the completing article.
    pub(super) filename: Option<String>,
    /// The file completed and this entry is a tombstone: `parked` has been
    /// released and the completion warning has already been issued.
    ///
    /// The entry outlives completion on purpose. It is what
    /// [`Pipeline::note_file_progress_floor`] reads to keep a restart
    /// checkpoint from ever being written for a uuencode file, and that
    /// suppression has to hold for the final write of the file as much as for
    /// every write before it.
    pub(super) finished: bool,
}

impl UuFileAssembly {
    /// Resident bytes held for parts waiting on their prefix.
    pub(super) fn parked_memory_bytes(&self) -> usize {
        self.parked
            .values()
            .filter(|entry| !entry.is_spilled())
            .map(UuParkedEntry::decoded_bytes)
            .sum()
    }

    /// Disk-backed bytes held for parts waiting on their prefix.
    pub(super) fn parked_spooled_bytes(&self) -> usize {
        self.parked
            .values()
            .filter(|entry| entry.is_spilled())
            .map(UuParkedEntry::decoded_bytes)
            .sum()
    }

    pub(super) fn parked_spooled_segments(&self) -> usize {
        self.parked
            .values()
            .filter(|entry| entry.is_spilled())
            .count()
    }
}

/// The window a PAR2 file description's `hash_16k` covers.
///
/// SPEC TRAP, and it is the whole reason this constant is a `min` rather than a
/// length: a description shorter than this hashes its **whole file**, with no
/// zero padding to 16 KiB. par2-rs writes it that way
/// (`&file_data[..file_data.len().min(16384)]`), so a matcher that padded — or
/// that skipped short descriptions — would silently refuse to bind exactly the
/// small files an obfuscated set is most likely to open with.
pub(super) const PAR2_HASH_16K_BYTES: usize = 16 * 1024;

/// Result of a decode task.
pub(super) struct DecodeResult {
    pub(super) segment_id: SegmentId,
    pub(super) raw_size: u64,
    /// How the article was encoded. Everything below that reads like a
    /// declared placement or a checksum is meaningful only for
    /// [`SegmentEncoding::Yenc`].
    pub(super) encoding: SegmentEncoding,
    pub(super) yenc_layout: YencLayoutAssertions,
    pub(super) crc_valid: bool,
    pub(super) part_crc_verified: bool,
    pub(super) part_crc: u32,
    pub(super) expected_file_crc: Option<u32>,
    pub(super) data: DecodedChunk,
    /// Original filename from the yEnc header (for swap detection observability).
    pub(super) yenc_name: String,
    /// Geometry actually applied by the decoder for this response.
    pub(super) checkpoint_plan: weaver_yenc::CheckpointPlan,
    /// The decode pass's CRC32 segments, cut at PAR2 block boundaries when the
    /// recovery set's block size was known to the decoder. [`Self::part_crc`] is
    /// their fold, so they add evidence without changing any verdict.
    pub(super) segments: Vec<weaver_yenc::Segment>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct YencLayoutAssertions {
    pub(super) file_size: u64,
    pub(super) part: Option<u32>,
    pub(super) total: Option<u32>,
    pub(super) begin: Option<u64>,
    pub(super) end: Option<u64>,
}

/// Whether a filename is a numeric split fragment of the described filename.
///
/// A fragment begins with the complete described name, so its first 16 KiB can
/// match the joined file even though the fragment cannot stand in for it.
pub(in crate::pipeline) fn is_split_fragment_of(
    candidate_name: &str,
    described_name: &str,
) -> bool {
    let described_name = weaver_model::files::sanitize_download_filename(described_name);
    candidate_name
        .strip_prefix(&described_name)
        .and_then(|suffix| suffix.strip_prefix('.'))
        .is_some_and(|extension| {
            !extension.is_empty() && extension.as_bytes().iter().all(u8::is_ascii_digit)
        })
}

#[derive(Debug)]
pub(super) struct SegmentSource {
    pub(super) source_server_idx: Option<usize>,
    pub(super) exclude_servers: Vec<usize>,
}

#[derive(Debug)]
pub(super) struct FileCrcRecoveryState {
    pub(super) pending_segments: HashSet<SegmentId>,
    pub(super) expected_crc: u32,
    pub(super) last_actual_crc: u32,
}

/// Completion of a decode task, including explicit failures so backlog
/// accounting is always drained.
pub(super) enum DecodeDone {
    Success {
        result: DecodeResult,
        source: SegmentSource,
    },
    Failed {
        segment_id: SegmentId,
        raw_size: u64,
        error: String,
        source_server_idx: Option<usize>,
        exclude_servers: Vec<usize>,
    },
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CompletedFileChecksum {
    pub(super) md5: Option<[u8; 16]>,
    pub(super) crc32: u32,
    pub(super) all_parts_crc_verified: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct StreamedCompletedFileChecksum {
    pub(super) md5: Option<[u8; 16]>,
    pub(super) crc32: u32,
    pub(super) all_parts_crc_verified: bool,
}

pub(super) struct CompletedFileChecksumState {
    md5: Option<par2_rs::checksum::FileHashState>,
    crc32: u32,
    crc32_combine_op: Option<(u64, weaver_yenc::Crc32Combine)>,
    bytes_fed: u64,
    all_parts_crc_verified: bool,
}

impl CompletedFileChecksumState {
    pub(super) fn new() -> Self {
        Self {
            md5: Some(par2_rs::checksum::FileHashState::new()),
            crc32: 0,
            crc32_combine_op: None,
            bytes_fed: 0,
            all_parts_crc_verified: true,
        }
    }

    pub(super) fn update(
        &mut self,
        data: &[u8],
        part_crc: u32,
        part_crc_verified: bool,
        track_md5: bool,
    ) {
        if !part_crc_verified {
            self.all_parts_crc_verified = false;
        }
        if track_md5 {
            let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.file_hash.update.md5");
            if let Some(md5) = self.md5.as_mut() {
                md5.update(data);
            }
        } else if self.md5.take().is_some() {
            crate::runtime::perf_probe::record(
                "download.file_hash.update.md5.disabled",
                Duration::from_nanos(1),
            );
        }
        self.update_crc32(data.len() as u64, part_crc, part_crc_verified);
    }

    pub(super) fn update_decoded_chunk(
        &mut self,
        data: &DecodedChunk,
        part_crc: u32,
        part_crc_verified: bool,
        track_md5: bool,
    ) {
        let total_len = data.len_bytes();
        if total_len == 0 {
            return;
        }
        if track_md5 {
            let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.file_hash.update.md5");
            if let Some(md5) = self.md5.as_mut() {
                data.for_each_slice(|slice| md5.update(slice));
            }
        } else if self.md5.take().is_some() {
            crate::runtime::perf_probe::record(
                "download.file_hash.update.md5.disabled",
                Duration::from_nanos(1),
            );
        }
        self.update_crc32(total_len as u64, part_crc, part_crc_verified);
    }

    pub(super) fn update_crc_metadata(&mut self, len: u64, part_crc: u32, part_crc_verified: bool) {
        if len == 0 {
            return;
        }
        if self.md5.take().is_some() {
            crate::runtime::perf_probe::record(
                "download.file_hash.update.md5.disabled",
                Duration::from_nanos(1),
            );
        }
        self.update_crc32(len, part_crc, part_crc_verified);
    }

    fn update_crc32(&mut self, len: u64, part_crc: u32, part_crc_verified: bool) {
        if !part_crc_verified {
            self.all_parts_crc_verified = false;
        }
        let _cpu_scope =
            crate::runtime::perf_probe::cpu_scope("download.file_hash.update.crc32_combine");
        if !matches!(self.crc32_combine_op.as_ref(), Some((cached_len, _)) if *cached_len == len) {
            self.crc32_combine_op = Some((len, weaver_yenc::Crc32Combine::new(len)));
        }
        let op = &self
            .crc32_combine_op
            .as_ref()
            .expect("crc32 combine op initialized")
            .1;
        self.crc32 = op.combine(self.crc32, part_crc);
        self.bytes_fed += len;
    }

    pub(super) fn bytes_fed(&self) -> u64 {
        self.bytes_fed
    }

    pub(super) fn crc32(&self) -> u32 {
        self.crc32
    }

    pub(super) fn all_parts_crc_verified(&self) -> bool {
        self.all_parts_crc_verified
    }

    pub(super) fn tracks_md5(&self) -> bool {
        self.md5.is_some()
    }

    pub(super) fn finalize(self) -> StreamedCompletedFileChecksum {
        StreamedCompletedFileChecksum {
            md5: self.md5.map(par2_rs::checksum::FileHashState::finalize),
            crc32: self.crc32,
            all_parts_crc_verified: self.all_parts_crc_verified,
        }
    }
}

impl Default for CompletedFileChecksumState {
    fn default() -> Self {
        Self::new()
    }
}

pub(super) enum DecodedChunk {
    Contiguous(Box<[u8]>),
    Batches { chunks: Vec<Box<[u8]>>, len: usize },
}

impl DecodedChunk {
    pub(super) fn len_bytes(&self) -> usize {
        match self {
            Self::Contiguous(bytes) => bytes.len(),
            Self::Batches { len, .. } => *len,
        }
    }

    pub(super) fn for_each_slice<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        match self {
            Self::Contiguous(bytes) => f(bytes),
            Self::Batches { chunks, .. } => {
                for chunk in chunks {
                    f(chunk.as_ref());
                }
            }
        }
    }

    pub(super) fn write_to<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        match self {
            Self::Contiguous(bytes) => writer.write_all(bytes),
            Self::Batches { chunks, .. } => {
                for chunk in chunks {
                    writer.write_all(chunk.as_ref())?;
                }
                Ok(())
            }
        }
    }

    /// Appends this chunk's slices, in order, for a vectored write that
    /// covers several contiguous chunks with one syscall.
    pub(super) fn push_io_slices<'a>(&'a self, out: &mut Vec<std::io::IoSlice<'a>>) {
        match self {
            Self::Contiguous(bytes) => out.push(std::io::IoSlice::new(bytes)),
            Self::Batches { chunks, .. } => {
                out.extend(chunks.iter().map(|chunk| std::io::IoSlice::new(chunk)));
            }
        }
    }
}

impl Pipeline {
    /// Publishes one per-article event on the segment stream rather than the
    /// job-level broadcast, so the always-on job-level subscribers are not
    /// woken several times per article to discard it. The event is built only
    /// when someone is listening.
    pub(crate) fn send_segment_event(&self, event: impl FnOnce() -> PipelineEvent) {
        self.shared_state.publish_segment_event(event);
    }
}

impl From<Vec<u8>> for DecodedChunk {
    fn from(value: Vec<u8>) -> Self {
        Self::Contiguous(value.into_boxed_slice())
    }
}

impl From<Vec<Box<[u8]>>> for DecodedChunk {
    fn from(mut chunks: Vec<Box<[u8]>>) -> Self {
        chunks.retain(|chunk| !chunk.is_empty());
        match chunks.len() {
            0 => Self::Contiguous(Vec::new().into_boxed_slice()),
            1 => Self::Contiguous(chunks.pop().expect("single chunk")),
            _ => {
                let len = chunks.iter().map(|chunk| chunk.len()).sum();
                Self::Batches { chunks, len }
            }
        }
    }
}

pub(super) struct BufferedDecodedSegment {
    pub(super) segment_id: SegmentId,
    pub(super) decoded_size: u32,
    /// Carried from the decoder so the durability seam can tell whether this
    /// segment is allowed to feed the dual-CRC grid.
    pub(super) encoding: SegmentEncoding,
    /// Immutable geometry snapshot captured before this article was decoded.
    /// Durable commit must never reinterpret its segments against grids that
    /// were admitted only after the response was already in flight.
    pub(super) checkpoint_plan: weaver_yenc::CheckpointPlan,
    pub(super) data: DecodedChunk,
    pub(super) part_crc: u32,
    pub(super) part_crc_verified: bool,
    pub(super) yenc_name: String,
    /// Block-aligned CRC32 segments carried from the decoder to the evidence
    /// collector, which runs after the bytes are durable.
    pub(super) segments: Vec<weaver_yenc::Segment>,
}

impl BufferedChunk for BufferedDecodedSegment {
    fn len_bytes(&self) -> usize {
        self.data.len_bytes()
    }
}

pub(super) struct DeferredFileHashChunk {
    pub(super) data: DecodedChunk,
    pub(super) part_crc: u32,
    pub(super) part_crc_verified: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum DeferredFileHashRangeSource {
    DecodedData,
    CrcMetadata,
}

impl DeferredFileHashRangeSource {
    pub(super) fn read_fallback_bucket(self) -> &'static str {
        match self {
            Self::DecodedData => {
                "download.file_hash.deferred_range_read.source.decoded_data_missing"
            }
            Self::CrcMetadata => "download.file_hash.deferred_range_read.source.crc_metadata",
        }
    }

    pub(super) fn read_fallback_bytes_bucket(self) -> &'static str {
        match self {
            Self::DecodedData => {
                "download.file_hash.deferred_range_read.source.decoded_data_missing.bytes"
            }
            Self::CrcMetadata => "download.file_hash.deferred_range_read.source.crc_metadata.bytes",
        }
    }

    pub(super) fn metadata_replay_bucket(self) -> &'static str {
        match self {
            Self::DecodedData => "download.file_hash.deferred_crc_metadata_replayed.decoded_data",
            Self::CrcMetadata => "download.file_hash.deferred_crc_metadata_replayed.crc_metadata",
        }
    }

    pub(super) fn metadata_replay_bytes_bucket(self) -> &'static str {
        match self {
            Self::DecodedData => {
                "download.file_hash.deferred_crc_metadata_replayed.decoded_data.bytes"
            }
            Self::CrcMetadata => {
                "download.file_hash.deferred_crc_metadata_replayed.crc_metadata.bytes"
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct DeferredFileHashRange {
    pub(super) len: usize,
    pub(super) part_crc: u32,
    pub(super) part_crc_verified: bool,
    pub(super) source: DeferredFileHashRangeSource,
}

impl DeferredFileHashChunk {
    pub(super) fn len_bytes(&self) -> usize {
        self.data.len_bytes()
    }
}

const MAX_IP_RTT_EWMA_ENTRIES: usize = 64;
const IP_RTT_EWMA_ALPHA: f64 = 0.20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ServerIpKey {
    pub(super) server_idx: usize,
    pub(super) ip: IpAddr,
}

#[derive(Debug, Clone)]
pub(super) struct IpRttEwma {
    pub(super) ewma_ms: f64,
    pub(super) samples: u16,
    pub(super) first_seen: Instant,
    pub(super) last_seen: Instant,
}

impl IpRttEwma {
    pub(super) fn new(now: Instant, elapsed: Duration) -> Self {
        Self {
            ewma_ms: elapsed.as_secs_f64() * 1000.0,
            samples: 1,
            first_seen: now,
            last_seen: now,
        }
    }

    pub(super) fn observe(&mut self, now: Instant, elapsed: Duration) {
        let next_ms = elapsed.as_secs_f64() * 1000.0;
        self.ewma_ms = (IP_RTT_EWMA_ALPHA * next_ms) + ((1.0 - IP_RTT_EWMA_ALPHA) * self.ewma_ms);
        self.samples = self.samples.saturating_add(1);
        self.last_seen = now;
    }
}

/// The one way a segment can stop being outstanding without arriving.
///
/// A segment reaches exactly one of these, exactly once, and the job's
/// `failed_bytes` is the sum of the *declared* sizes of the segments that hold
/// one. Delivery is the fourth terminal state and is recorded where it already
/// was — the assembly bitmap — so a delivered segment never appears here at
/// all.
///
/// The distinction between the variants is diagnostic; every one of them
/// contributes the same declared bytes. What matters is that there is one
/// place a segment can acquire a state and no place at all where bytes are
/// added without one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum SegmentTerminalState {
    /// Every configured server refused the article.
    Missing,
    /// The download retry budget ran out without a usable body.
    RetriesExhausted,
    /// Bodies arrived but no attempt decoded into the declared placement.
    DecodeExhausted,
    /// Retired without a wire outcome: the servers are serving a different
    /// file under these message ids, so the declared bytes cannot arrive.
    ForeignLayout,
}

/// What the settlement concluded a delivered job actually delivered.
///
/// Built once, from the claim census, at the last gate before the payload
/// leaves the working directory — while every settlement fact that decided the
/// job is still in hand. The terminal record is written from this rather than
/// from the live wire counters, which know only what the download layer saw and
/// nothing about what repair, verification or a discard did with it afterwards.
#[derive(Debug, Clone, Default)]
pub(in crate::pipeline) struct TerminalReconciliation {
    /// Declared bytes of the delivered files that really are short.
    pub(in crate::pipeline) failed_bytes: u64,
    /// Health over the delivered files alone, 0-1000.
    pub(in crate::pipeline) health: u32,
    /// Files that left the accounting, and why.
    pub(in crate::pipeline) discards: Vec<crate::jobs::model::TerminalDiscard>,
}

/// The layout a refused article says its bytes belong to.
///
/// Two fields, and only the first is evidence. `=ypart total=` is part
/// geometry — the NZB is authoritative for a file's part count, so an article
/// that names a different one is describing a different file. `=ybegin size=`
/// is a header real posters misstate all the time, so it corroborates a
/// geometry disagreement and never triggers one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) struct ForeignYencGeometry {
    pub(in crate::pipeline) served_total: Option<u32>,
    pub(in crate::pipeline) served_file_size: u64,
}

/// Per-file evidence that the servers hold a different file under this file's
/// message ids.
///
/// One consistent foreign geometry across many distinct segments is not
/// damage: a corrupt article disagrees with the declared layout in a way that
/// varies article by article, while a message-id collision with a repost
/// disagrees the *same* way every time, because every article really does
/// belong to one other, coherent file. Varying geometries therefore keep the
/// file fetching; agreeing ones retire it.
#[derive(Debug)]
pub(in crate::pipeline) struct ForeignLayoutWatch {
    /// The geometry the current run of refusals agrees on. Replaced — and the
    /// segment run restarted — the moment a refusal disagrees with it.
    pub(in crate::pipeline) geometry: ForeignYencGeometry,
    /// Distinct segment ordinals that refused with `geometry`.
    pub(in crate::pipeline) segments: HashSet<u32>,
    /// At least one refusal in the current run disagreed on *part* geometry
    /// rather than only on the `=ybegin size=` header. Real posts misstate that
    /// header, so a run made purely of size disagreements corroborates nothing
    /// and must never retire a file on its own.
    pub(in crate::pipeline) geometry_disagreed: bool,
    /// A segment of this file decoded into the declared layout. Permanent:
    /// the declared file demonstrably exists on the wire, so no amount of
    /// later foreign evidence may retire it.
    pub(in crate::pipeline) disarmed: bool,
    /// The breaker already fired for this file.
    pub(in crate::pipeline) tripped: bool,
}

/// The pipeline engine. Owns the scheduler loop and drives work through
/// download → decode → commit → verify → repair → extract stages.
pub struct Pipeline {
    /// Receives commands from SchedulerHandle.
    pub(super) cmd_rx: mpsc::Receiver<SchedulerCommand>,
    /// Broadcasts pipeline events to subscribers (API, journal, etc).
    pub(super) event_tx: broadcast::Sender<PipelineEvent>,
    /// NNTP client for fetching articles.
    pub(super) nntp: Arc<NntpClient>,
    /// Buffer pool used as decode scratch space only.
    pub(super) buffers: Arc<BufferPool>,
    /// Runtime tuner for adaptive concurrency.
    pub(super) tuner: RuntimeTuner,
    /// Shared atomic metrics.
    pub(super) metrics: Arc<PipelineMetrics>,
    /// Per-job state.
    pub(super) jobs: HashMap<JobId, JobState>,
    /// Typed terminal provenance retained until the ordered history archive has
    /// durably updated duplicate-promotion eligibility.
    pub(super) semantic_terminal_causes: HashMap<JobId, crate::jobs::SemanticTerminalCause>,
    /// Winning archive password candidate per job/RAR set, kept process-local and redacted.
    pub(super) archive_password_winners: HashMap<(JobId, String), ArchivePasswordCandidate>,
    /// Job dispatch order (FIFO by submission). First Downloading job is active.
    pub(super) job_order: Vec<JobId>,
    /// Number of in-flight article downloads (primary + recovery).
    pub(super) active_downloads: usize,
    /// Number of NNTP connection tasks currently fetching articles.
    pub(super) active_download_connections: usize,
    /// Active connection lanes carrying completion-critical PAR2 work.
    pub(super) active_completion_critical_connections: usize,
    /// Number of in-flight recovery downloads (subset of active_downloads).
    pub(super) active_recovery: usize,
    /// Current hot job receiving exclusive article-dispatch preference.
    pub(super) hot_dispatch_job: Option<JobId>,
    /// When the current hot-dispatch ownership period began.
    pub(super) hot_dispatch_started_at: Option<Instant>,
    /// Best observed speed while the current hot job was exclusive.
    pub(super) hot_dispatch_exclusive_peak_bps: u64,
    /// Last time dispatch lent a reclaimable connection to spillover work.
    pub(super) hot_dispatch_last_lend_at: Option<Instant>,
    /// Current scheduler share mode.
    pub(super) hot_dispatch_mode: DispatchShareMode,
    /// Start of the current unused-capacity underfill window.
    pub(super) hot_dispatch_underfill_since: Option<Instant>,
    /// Most recent spillover decision, for tick logging.
    pub(super) hot_dispatch_last_spillover_decision: SpilloverDecision,
    /// Two-second measured throughput for successful hot-job primary BODY results.
    pub(super) hot_dispatch_throughput_window: HotJobThroughputWindow,
    /// Peak measured hot-job speed while no spillover lanes are active.
    pub(super) hot_dispatch_exclusive_window: HotExclusiveWindow,
    /// Recent lane expansion and pipeline promotion outcomes.
    pub(super) hot_dispatch_expansion_window: HotExpansionWindow,
    /// Active reclaimable spillover loans keyed by lent job.
    pub(super) hot_dispatch_spillover_loans: SpilloverLoanBook,
    /// Cooperative signal asking owned hot lanes to return their unrequested tail.
    pub(super) hot_share_yield_signal: Arc<HotShareYieldSignal>,
    /// Runtime-only article transport classification per active job.
    pub(super) job_transport_profiles: HashMap<JobId, JobTransportProfile>,
    /// Runtime-only lane/proof state for BODY dispatch.
    pub(super) download_lane_runtime: DownloadLaneRuntimeState,
    /// Lane refill requests held under hard download pressure, answered as the
    /// backlog drains so lanes resume without a park/redispatch round-trip.
    pub(super) deferred_lane_refills: VecDeque<DownloadLaneRefillRequest>,
    /// User-enabled over-max burst budget for latent-IP replacement trials.
    pub(super) ip_replacement_trial_extra_connections: u8,
    /// Bounded per-server/per-IP BODY RTT EWMA state.
    pub(super) ip_rtt_ewma: HashMap<ServerIpKey, IpRttEwma>,
    /// Old server/IP identities accepted for replacement; active lanes park at clean refill.
    pub(super) ip_replacement_retired_ips: HashSet<ServerIpKey>,
    /// Whether the single global over-max replacement burst is currently occupied.
    pub(super) ip_replacement_burst_active: bool,
    /// Jobs currently inside an active article download pass.
    pub(super) active_download_passes: HashSet<JobId>,
    /// Jobs that still have decode/write pipeline work after network downloads finished.
    pub(super) jobs_finalizing_download: HashSet<JobId>,
    /// Released download results waiting to be committed into decode/write state.
    pub(super) pending_released_download_results_by_job: HashMap<JobId, usize>,
    /// Estimated decoded/raw bytes held by released results that are not committed yet.
    pub(super) pending_released_download_result_bytes_by_job: HashMap<JobId, u64>,
    /// In-flight article download count per job.
    pub(super) active_downloads_by_job: HashMap<JobId, usize>,
    /// In-flight NNTP connection task count per job.
    pub(super) active_download_connections_by_job: HashMap<JobId, usize>,
    /// Completion-critical connection lanes per job. Kept separately from
    /// article counts because pipelined lanes can carry several articles.
    pub(super) active_completion_critical_connections_by_job: HashMap<JobId, usize>,
    /// In-flight article download count per file.
    pub(super) active_downloads_by_file: HashMap<NzbFileId, usize>,
    /// In-flight decode task count per job.
    pub(super) active_decodes_by_job: HashMap<JobId, usize>,
    /// In-flight decode task count per file.
    pub(super) active_decodes_by_file: HashMap<NzbFileId, usize>,
    /// Last time a job made observable progress in the download stage.
    pub(super) job_last_download_activity: HashMap<JobId, Instant>,
    /// Delayed retry tasks that have been scheduled but not yet re-queued.
    pub(super) pending_retries_by_job: HashMap<JobId, usize>,
    /// Delayed retry tasks by exact segment.
    pub(super) pending_retries_by_segment: HashMap<SegmentId, usize>,
    pub(super) download_wait_by_job: HashMap<JobId, DownloadWaitStatus>,
    /// The one terminal state each segment reached, and the only thing the
    /// per-job failed-byte ledger is derived from.
    pub(in crate::pipeline) segment_terminal_states: HashMap<SegmentId, SegmentTerminalState>,
    /// Per-file watch on articles that decode against a layout the NZB never
    /// declared. Empty for every ordinary job: an entry appears only once a
    /// file has refused an article on part geometry.
    pub(in crate::pipeline) foreign_layout_watches: HashMap<NzbFileId, ForeignLayoutWatch>,
    /// Stands in for the `WEAVER_FOREIGN_LAYOUT_BREAKER` escape hatch, which is
    /// read once per process and so cannot be exercised both ways in one test
    /// binary.
    #[cfg(test)]
    pub(in crate::pipeline) foreign_layout_breaker_override: Option<bool>,
    /// What the claim census concluded for a job on its way out, keyed until
    /// the terminal record has been written from it.
    pub(in crate::pipeline) terminal_reconciliations: HashMap<JobId, TerminalReconciliation>,
    /// Files already counted into `weaver_files_missing_total`. A completion
    /// check re-enters many times per job; this keeps the counter per-file
    /// rather than per-check. Per-file, so the set is bounded by the job's
    /// file count and never touched from a per-segment path.
    pub(in crate::pipeline) files_counted_missing: HashSet<NzbFileId>,
    /// Work parked specifically on per-server quota capacity or policy changes.
    pub(super) server_quota_parked: HashSet<SegmentId>,
    /// Directory for active downloads (per-job subdirectories).
    pub(super) intermediate_dir: PathBuf,
    /// Directory for completed downloads (category subdirectories).
    pub(super) complete_dir: PathBuf,
    /// Legacy logical NZB path base retained for compatibility with existing rows and tests.
    pub(super) nzb_dir: PathBuf,
    /// Per-file contiguous write floors awaiting persistence.
    pub(super) pending_file_progress: HashMap<NzbFileId, u64>,
    /// Last queued/persisted contiguous write floor per file.
    pub(super) persisted_file_progress: HashMap<NzbFileId, u64>,
    /// Streaming checksum state for files whose decoded bytes have been observed in order.
    pub(super) file_hash_states: HashMap<NzbFileId, CompletedFileChecksumState>,
    /// In-stream PAR2 block CRC32s assembled from the decode pass's segments.
    /// See [`crate::pipeline::integrity`] for the verification policy.
    pub(super) block_crcs: crate::pipeline::integrity::BlockCrcCollector,
    /// Decoded bytes for out-of-order persisted ranges waiting to be replayed into the streaming checksum.
    pub(super) deferred_file_hash_data: HashMap<NzbFileId, BTreeMap<u64, DeferredFileHashChunk>>,
    pub(super) deferred_file_hash_data_bytes: usize,
    /// Out-of-order persisted ranges waiting to be replayed into the streaming checksum.
    pub(super) deferred_file_hash_ranges: HashMap<NzbFileId, BTreeMap<u64, DeferredFileHashRange>>,
    /// Expected whole-file yEnc CRC32 values observed from multipart `=yend crc32`.
    pub(super) expected_file_crcs: HashMap<NzbFileId, u32>,
    /// Files that need a one-time disk reread because out-of-order persistence broke the stream.
    pub(super) file_hash_reread_required: HashSet<NzbFileId>,
    #[cfg(test)]
    pub(super) try_update_archive_topology_calls: usize,
    #[cfg(test)]
    pub(super) par2_lower_bound_preflight_calls: usize,
    #[cfg(test)]
    pub(super) par2_authoritative_verify_calls: usize,
    /// Post-repair passes, which read only the files the repair rewrote.
    /// Counted apart from the whole-set passes above so a test can still say
    /// "no full verification happened here" now that every repair path ends
    /// in a selective re-read of what it installed.
    #[cfg(test)]
    pub(super) par2_selective_verify_calls: usize,
    /// Passes that concluded from evidence already in hand, reading nothing.
    /// The counters above can only say a whole-set read did *not* happen; this
    /// is what lets a test say the quick pass is what answered instead.
    #[cfg(test)]
    pub(super) par2_quick_verify_calls: usize,
    #[cfg(test)]
    pub(super) par2_quick_partial_verify_calls: usize,
    /// Repairer runs that were seeded with a stashed scan carry — a previous
    /// pass's returned carry or a host-verification one.
    #[cfg(test)]
    pub(super) par2_scan_carry_seeded_calls: usize,
    /// Repairer runs whose returned scan carry was stashed for the next pass.
    #[cfg(test)]
    pub(super) par2_scan_carry_stashed_calls: usize,
    /// Host-verification carries built from a damaged authoritative pass.
    #[cfg(test)]
    pub(super) par2_host_carry_builds: usize,
    /// Forces the PAR2 ignore-extension list for a test, so the "override
    /// disables it" case can be exercised without mutating a process-global
    /// environment variable while other tests are running.
    #[cfg(test)]
    pub(super) par2_ignore_extensions_override: Option<Vec<String>>,
    /// Read-backs of a recovery volume that can no longer complete. The
    /// one-shot latch is what keeps this off the gate's hot path, so a test can
    /// pin it.
    #[cfg(test)]
    pub(super) par2_recovery_salvage_scans: usize,
    /// Times a job announced that it carries recovery sets it does not serve.
    /// The announcement is latched, so a test can pin that a gate entered many
    /// times says it once.
    #[cfg(test)]
    pub(super) par2_unserved_set_warnings: usize,
    #[cfg(test)]
    pub(super) par2_repairer_analyze_calls: usize,
    #[cfg(test)]
    pub(super) par2_repairer_execute_calls: usize,
    /// Bytes each damaged-job authoritative analysis actually read, in order.
    /// The shipped build reports this through the pass's own outcome log line
    /// and a perf probe; a test needs it as a number it can bound.
    #[cfg(test)]
    pub(super) par2_authoritative_bytes_read: Vec<u64>,
    /// Forces the retained-session gate on or off for a test, so a
    /// differential can run both arms without mutating a process-global
    /// environment variable while other tests are running.
    #[cfg(test)]
    pub(super) stateful_par2_session_forced: Option<bool>,
    /// Times the retained session, rather than the read-and-verify pass,
    /// produced a direct set's verdict. A differential needs this to tell
    /// "the session agreed" from "the session refused and fell back".
    #[cfg(test)]
    pub(super) direct_session_pass_calls: usize,
    /// `(files stood in for, files read)` for every direct read-and-verify
    /// pass, in the order they ran. The whole point of feeding the grid from
    /// the direct seam is that the second number shrinks, and only a per-pass
    /// record can show it: a repair retires the job's block state and
    /// re-verifies, so a cumulative count would blend a pass that stood in for
    /// two volumes with a later one that could stand in for none.
    #[cfg(test)]
    pub(super) direct_verify_read_splits: Vec<(usize, usize)>,
    /// `(files carried, files read)` for every post-repair PAR2 pass, in the
    /// order they ran. Same shape and same purpose as
    /// `direct_verify_read_splits`: the selective post-repair pass exists so the
    /// second number is only what the repair rewrote, and only a per-pass record
    /// can show that — a job can repair more than once.
    #[cfg(test)]
    pub(super) par2_post_repair_read_splits: Vec<(usize, usize)>,
    /// `(files composed from wire CRCs, files read from disk)` for every SFV
    /// verification pass, in the order they ran. Same shape and same purpose as
    /// the two above: the zero-I/O arm exists so the second number is only the
    /// files the wire could not vouch for, and only a per-pass record can show
    /// which arm actually answered.
    #[cfg(test)]
    pub(super) sfv_verify_read_splits: Vec<(usize, usize)>,
    /// `(files claimed, files read)` for every quiet direct pass that ran
    /// **after** a repair-while-direct, in order.
    ///
    /// Separate from `direct_verify_read_splits`, which records every quiet
    /// pass: the post-repair pass has a rule of its own — it may claim nothing
    /// and must read everything — and only a record that says which passes were
    /// post-repair can pin it.
    #[cfg(test)]
    pub(super) direct_post_repair_read_splits: Vec<(usize, usize)>,
    /// The verdict of the most recent quiet direct pass. The pipeline runs
    /// that pass itself, mid-assembly, while live state is still intact — a
    /// test that calls the pass afterwards observes a different situation
    /// entirely, so the differential reads what actually happened.
    #[cfg(test)]
    pub(super) last_direct_verdict: Option<par2_rs::VerificationResult>,
    /// Downloaded article bodies waiting for decode scheduling.
    pub(super) pending_decode: VecDeque<PendingDecodeWork>,
    /// Jobs that should re-enter completion/post-processing on the next loop pass.
    pub(super) pending_completion_checks: VecDeque<JobId>,
    /// Channels for pipeline stage results.
    pub(super) download_done_tx: mpsc::Sender<DownloadResult>,
    pub(super) download_done_rx: mpsc::Receiver<DownloadResult>,
    pub(super) download_refill_tx: mpsc::Sender<DownloadLaneRefillRequest>,
    pub(super) download_refill_rx: mpsc::Receiver<DownloadLaneRefillRequest>,
    pub(super) download_lane_parked_tx: mpsc::Sender<DownloadLaneParked>,
    pub(super) download_lane_parked_rx: mpsc::Receiver<DownloadLaneParked>,
    pub(super) owned_download_lane_event_tx: mpsc::Sender<OwnedDownloadLaneEvent>,
    pub(super) owned_download_lane_event_rx: mpsc::Receiver<OwnedDownloadLaneEvent>,
    pub(super) owned_download_lane_pool: download::owned_lane::OwnedDownloadLanePool,
    pub(super) ip_replacement_trial_tx: mpsc::Sender<IpReplacementTrialEvent>,
    pub(super) ip_replacement_trial_rx: mpsc::Receiver<IpReplacementTrialEvent>,
    pub(super) decode_done_tx: mpsc::Sender<DecodeDone>,
    pub(super) decode_done_rx: mpsc::Receiver<DecodeDone>,
    /// Channel through which due retries re-enter the pipeline loop.
    pub(in crate::pipeline) retry_tx: mpsc::Sender<RetryWork>,
    pub(in crate::pipeline) retry_rx: mpsc::Receiver<RetryWork>,
    /// Pipeline-owned infrastructure retries, drained in deadline batches.
    pub(in crate::pipeline) infrastructure_retries:
        infrastructure_retry::InfrastructureRetryQueue<RetryWork>,
    /// Monotonic NNTP pool generation, bumped on every `RebuildNntp`. Server
    /// indices in `DownloadWork::exclude_servers` are only meaningful within
    /// the generation they were computed under; delayed retries carry the
    /// generation they were scheduled under so stale indices can be dropped
    /// when they re-enter after a rebuild.
    pub(super) pool_generation: u64,
    /// Per-server article attempt counters for the active NNTP generation,
    /// indexed by runtime `server_idx`.
    ///
    /// Held as a plain `Vec` with no lock: the completion path indexes it
    /// directly and only ever does `Relaxed` `fetch_add`s through the `Arc`s.
    /// It is rebuilt exactly when a new NNTP generation is activated, from
    /// `metrics.server_metrics`, which re-uses the counters of any stable
    /// server id it has already seen so lifetime totals survive a config
    /// reload. A stale generation may hand us an out-of-range index; the
    /// completion path bounds-checks and skips.
    pub(super) server_counters: Vec<Arc<crate::operations::instrumentation::ServerCounters>>,
    /// Wall-clock start of each in-flight job stage, keyed by `(job, stage)`.
    ///
    /// Per-job, not per-segment: a job passes through each stage at most a
    /// handful of times, so a `HashMap` here costs nothing measurable and never
    /// appears on an article path.
    pub(super) job_stage_started_at:
        HashMap<(JobId, crate::operations::instrumentation::JobStageKind), Instant>,
    /// Bounded completion path for dedicated adaptive-capacity probes.
    pub(super) capacity_probe_result_tx: mpsc::Sender<CapacityProbeCompletion>,
    pub(super) capacity_probe_result_rx: mpsc::Receiver<CapacityProbeCompletion>,
    /// Channel for health probe results: (job_id, total_probes, missed_count).
    pub(super) probe_result_tx: mpsc::Sender<ProbeUpdate>,
    pub(super) probe_result_rx: mpsc::Receiver<ProbeUpdate>,
    /// Channel for background extraction results.
    pub(super) extract_done_tx: mpsc::Sender<ExtractionDone>,
    pub(super) extract_done_rx: mpsc::Receiver<ExtractionDone>,
    /// Channel for background RAR topology refresh results.
    pub(super) rar_refresh_done_tx: mpsc::Sender<RarRefreshDone>,
    pub(super) rar_refresh_done_rx: mpsc::Receiver<RarRefreshDone>,
    /// Channel for delayed RAR capacity-pressure refresh/extraction wakeups.
    pub(in crate::pipeline) rar_capacity_retry_tx: mpsc::Sender<RarCapacityRetry>,
    pub(in crate::pipeline) rar_capacity_retry_rx: mpsc::Receiver<RarCapacityRetry>,
    /// Channel for background final-move results.
    pub(super) move_done_tx: mpsc::Sender<MoveToCompleteDone>,
    pub(super) move_done_rx: mpsc::Receiver<MoveToCompleteDone>,
    pub(super) terminal_post_processing_done_tx: mpsc::Sender<TerminalPostProcessingEvent>,
    pub(super) terminal_post_processing_done_rx: mpsc::Receiver<TerminalPostProcessingEvent>,
    pub(super) terminal_post_processing_executor:
        crate::post_processing::executor::PostProcessingExecutor,
    pub(super) inflight_terminal_post_processing: HashSet<JobId>,
    pub(super) terminal_post_processing_cancellations:
        HashMap<JobId, tokio::sync::watch::Sender<bool>>,
    /// Cooperative cancellation tokens for PAR2 verification and repair work.
    pub(super) par2_cancellations: HashMap<JobId, par2_rs::CancellationToken>,
    /// Monotonic fence for direct post-repair tickets detached from the actor.
    pub(super) next_direct_post_repair_work_id: u64,
    /// At most one direct post-repair read-back runs for a job.
    pub(super) direct_post_repair_in_flight: HashMap<JobId, DirectPostRepairWork>,
    /// Terminal verdicts awaiting the completion gate that submitted them.
    pub(super) direct_post_repair_results: HashMap<
        JobId,
        (
            par2_rs::RecoverySetId,
            Result<par2_rs::VerificationResult, String>,
        ),
    >,
    /// The bounded lane reports only terminal post-repair verdicts.
    pub(super) direct_post_repair_done_tx: mpsc::Sender<DirectPostRepairWorkDone>,
    pub(super) direct_post_repair_done_rx: mpsc::Receiver<DirectPostRepairWorkDone>,
    /// The pre-repair verdict a direct-store repair leaves behind for the
    /// post-repair pass to read selectively instead of re-reading the whole
    /// recovery set. Cleared once consumed, on demotion, and by
    /// [`Pipeline::clear_par2_runtime_state`] — see [`DirectPostRepairCarry`].
    pub(super) direct_post_repair_carry: HashMap<JobId, DirectPostRepairCarry>,
    /// Whether all downloads are globally paused.
    pub(super) global_paused: bool,
    /// Whether the active global pause came from a bandwidth schedule rather
    /// than an operator action. Only meaningful while `global_paused` is true;
    /// it selects the Scheduled vs ManualPause download-block presentation.
    pub(super) scheduled_pause: bool,
    /// ISP bandwidth cap runtime state.
    pub(crate) bandwidth_cap: BandwidthCapRuntime,
    /// Conservative byte reservations for in-flight downloads used to enforce the
    /// ISP bandwidth cap before actual payload bytes are known.
    pub(crate) bandwidth_reservations: HashMap<SegmentId, u64>,
    /// Estimated bytes charged to the speed limiter for in-flight downloads.
    pub(crate) rate_limit_reservations: HashMap<SegmentId, u64>,
    /// Persisted/general speed limit restored when no schedule speed action is active.
    pub(super) configured_rate_limit: u64,
    /// Active schedule speed action, if any.
    pub(super) scheduled_rate_limit: Option<u64>,
    /// Effective bandwidth rate limiter.
    pub(super) rate_limiter: TokenBucket,
    /// Max pending segments per write reorder buffer (memory-adaptive).
    pub(super) write_buf_max_pending: usize,
    /// Max in-memory raw article bytes queued or active for decode.
    pub(super) decode_backlog_budget_bytes: usize,
    /// Max in-memory decoded backlog before degrading to direct offset writes.
    pub(super) write_backlog_budget_bytes: usize,
    /// Whether raw decode backlog is in a hard-pressure drain cycle.
    pub(super) download_decode_hard_pressure_latched: bool,
    /// Whether decoded write backlog is in a hard-pressure drain cycle.
    pub(super) download_write_hard_pressure_latched: bool,
    /// Start time of the current hard pressure stall, if downloads are blocked.
    pub(super) download_pressure_hard_stall_started_at: Option<Instant>,
    /// Next time soft byte pressure may issue a replacement article.
    pub(super) download_pressure_soft_dispatch_after: Option<Instant>,
    /// When the job snapshot was last rebuilt and published.
    pub(super) snapshot_published_at: Option<Instant>,
    /// Whether a debounced snapshot publish is owed once the window reopens.
    pub(super) snapshot_publish_pending: bool,
    /// Per-job delay after restart-durable-lead throttling parks primary work.
    pub(super) download_restart_durable_lead_retry_after: HashMap<JobId, Instant>,
    /// When each deferred job's articles become old enough to fetch.
    ///
    /// Absent means the question has not been asked yet or was answered
    /// "eligible" — see [`Pipeline::propagation_hold_until`], which is where the
    /// answer is computed and cached. Same shape and same role as
    /// `download_restart_durable_lead_retry_after` above: a per-job "not
    /// before", read at the dispatch gate and surfaced to the run loop's sleep
    /// so the wake happens at eligibility rather than by polling.
    pub(super) propagation_ready_at: HashMap<JobId, Instant>,
    /// Test-only override of [`propagation_delay`], mirroring
    /// `stateful_par2_session_forced`. The env gate is read once per process,
    /// so a test that needs a different delay cannot get one by setting the
    /// variable.
    pub(super) propagation_delay_forced: Option<Duration>,
    /// Last time we logged a queued/no-active-download liveness stall.
    pub(super) last_download_dispatch_stall_log_at: Option<Instant>,
    /// Current in-memory decoded backlog retained for sequential write ordering.
    pub(super) write_buffered_bytes: usize,
    /// Current in-memory decoded segment count retained for sequential write ordering.
    pub(super) write_buffered_segments: usize,
    /// Disk-backed uuencode parts waiting for their missing prefix.
    pub(super) uu_spooled_bytes: usize,
    /// Disk-backed uuencode segment count waiting for their missing prefix.
    pub(super) uu_spooled_segments: usize,
    /// Every UU part held ahead of its cursor, regardless of storage form.
    ///
    /// This bounds map and temporary-path overhead even when decoded bytes are
    /// tiny.
    pub(super) uu_parked_segments: usize,
    /// Reserved transient spool root below the configured intermediate directory.
    pub(super) uu_spool_root: PathBuf,
    /// Aggregate disk-backed UU byte admission limit.
    pub(super) uu_spool_max_bytes: usize,
    /// Aggregate ahead-of-cursor UU entry admission limit.
    pub(super) uu_spool_max_segments: usize,
    /// Free space preserved on the intermediate filesystem while spilling UU.
    pub(super) uu_spool_min_free_bytes: u64,
    /// Most recent free-space sample for the spool filesystem.
    pub(super) uu_spool_last_free_space_check: Option<Instant>,
    /// Cached available bytes for the spool filesystem; `None` means unknown.
    pub(super) uu_spool_available_bytes: Option<u64>,
    #[cfg(test)]
    /// Test-only free-space result; `Some(None)` exercises a failed probe.
    pub(super) uu_spool_available_bytes_for_test: Option<Option<u64>>,
    /// Per-file write reorder buffers for decoded segments waiting on write order.
    pub(super) write_buffers: HashMap<NzbFileId, WriteReorderBuffer<BufferedDecodedSegment>>,
    /// The first [`PAR2_HASH_16K_BYTES`] decoded bytes of each file, anchored at
    /// offset 0, for binding an **obfuscated** file to its PAR2 description by
    /// content when its name matches nothing.
    ///
    /// Bounded twice over: 16 KiB per file, and only files whose first article
    /// has landed have an entry at all. Dropped with the rest of the job's
    /// per-file runtime.
    pub(super) file_prefix_16k: HashMap<NzbFileId, Vec<u8>>,
    /// First non-zero decoded size declared by a yEnc header for each file.
    ///
    /// This is independent evidence about the file the poster intended to
    /// send. A later article cannot revise an earlier declaration.
    pub(super) file_declared_size: HashMap<NzbFileId, u64>,
    /// Sequential-assembly state for uuencode files, created on the first
    /// uuencode part of a file and dropped with that file's write buffer.
    pub(super) uu_files: HashMap<NzbFileId, UuFileAssembly>,
    /// How often each uuencode segment has been displaced by park pressure and
    /// requeued. Purely a livelock bound — deliberately NOT the decode-failure
    /// counter, because park pressure is an ordering condition and must never
    /// spend a segment's retry budget.
    pub(super) uu_park_requeues: HashMap<SegmentId, u32>,
    /// Authoritative PAR2 runtime state per job.
    pub(super) par2_runtime: HashMap<JobId, Par2RuntimeState>,
    #[cfg(test)]
    pub(super) par2_binding_resolver_calls: std::sync::atomic::AtomicU64,
    /// Direct-store routing state: admitted archive sets, their routers and
    /// their coverage barriers. Inert while the gate is off.
    pub(super) direct_store: direct_store::wiring::DirectStoreRuntime,
    /// Direct-unpack state: 7z sets being decoded while they download. Inert
    /// while the gate is off.
    pub(super) direct_unpack: direct_unpack::wiring::DirectUnpackRuntime,
    /// RAR members already extracted per job (for incremental RAR extraction).
    pub(super) extracted_members: HashMap<JobId, HashSet<String>>,
    /// Archives whose extraction has completed successfully (by archive name).
    /// For RAR this is the set name; for 7z/zip/tar/gz it's the archive name.
    pub(super) extracted_archives: HashMap<JobId, HashSet<String>>,
    /// Archive sets a missing-volumes failure has already been dispatched for.
    /// Such a set has live claimants, no runtime state and nothing on disk, so
    /// extraction can only report the same failure again; the first report is
    /// the one that fails the job, and this stops a completion check that
    /// re-runs before it lands from queueing a second doomed extraction.
    pub(super) missing_volume_archive_sets: HashMap<JobId, HashSet<String>>,
    /// Tracks decode failure retries per segment. When yEnc decode fails (CRC/size
    /// mismatch), the segment is re-downloaded. After `MAX_SEGMENT_RETRIES` decode
    /// failures, the segment is marked permanently failed.
    pub(super) decode_retries: HashMap<SegmentId, u32>,
    /// Successfully decoded segments whose yEnc part CRC was absent. These are
    /// targeted for replacement only if the completed file CRC proves corruption.
    pub(super) unverified_segments: HashMap<NzbFileId, HashMap<u32, SegmentSource>>,
    /// Completed files currently replacing unverified segments after a whole-file
    /// CRC mismatch. Final verification waits for the entire batch.
    pub(super) file_crc_recoveries: HashMap<NzbFileId, FileCrcRecoveryState>,
    /// Archives with in-flight extraction tasks (spawned but not yet completed).
    /// Prevents duplicate spawns and ensures cleanup waits for extraction to finish.
    pub(super) inflight_extractions: HashMap<JobId, HashSet<String>>,
    /// Transient byte-progress state for active user-visible phases.
    pub(super) phase_progress: HashMap<(JobId, JobPhase), progress::JobPhaseRuntime>,
    /// Last sampled phase-progress snapshots projected into JobInfo.
    pub(super) phase_progress_snapshots: HashMap<JobId, Vec<JobPhaseProgress>>,
    /// Per-job queue-event coalescing state for sampled phase progress.
    pub(super) phase_publish_state: HashMap<JobId, progress::PhasePublishState>,
    /// Cached per-job retention exclusions (pool server indices whose
    /// retention window is older than the job). TTL'd; cleared on NNTP
    /// client rebuilds and job removal.
    pub(super) job_retention_exclude_cache: HashMap<JobId, (Instant, Arc<Vec<usize>>)>,
    /// Rate limiter for the "no eligible news server" warning.
    pub(super) last_no_eligible_server_warn: Option<Instant>,
    /// Rate limiter for representative NNTP BODY fetch failures at info level.
    pub(super) last_body_fetch_failure_log_at: Option<Instant>,
    /// Jobs currently performing their final move into the complete directory.
    pub(super) inflight_moves: HashSet<JobId>,
    /// Complete destinations reserved for in-flight moves so concurrent jobs do not collide.
    pub(super) reserved_complete_destinations: HashMap<JobId, PathBuf>,
    /// Members whose incremental extraction failed (corrupt volume, CRC error, etc).
    /// Prevents immediate retry during download; cleared after PAR2 repair so
    /// the post-repair extraction path can re-extract them.
    pub(super) failed_extractions: HashMap<JobId, HashSet<String>>,
    /// Filenames eagerly deleted per job after CRC-verified extraction.
    /// Used to distinguish truly-missing files from intentionally-deleted ones
    /// during PAR2 verification.
    pub(super) eagerly_deleted: HashMap<JobId, HashSet<String>>,
    /// Pipeline-owned RAR scheduling state derived from immutable completed-volume facts.
    rar_sets: HashMap<(JobId, String), RarSetState>,
    /// Runtime-only coalescing state for background RAR topology refreshes.
    rar_refresh_state: HashMap<(JobId, String), RarRefreshState>,
    /// Jobs whose queued RAR volume work needs unlock-priority recomputation.
    rar_unlock_priority_dirty_jobs: HashSet<JobId>,
    /// Files currently boosted for opportunistic RAR member unlock scheduling.
    rar_unlock_boosted_files: HashMap<JobId, HashSet<NzbFileId>>,
    /// Runtime-only coalescing state for delayed RAR capacity-pressure retries.
    pub(in crate::pipeline) pending_rar_capacity_retries:
        HashSet<(JobId, String, RarCapacityRetryKind)>,
    /// Members currently blocked on future RAR volumes. Used to emit stable
    /// waiting-started / waiting-finished events without relying on log text.
    pub(super) rar_waiting_members: HashMap<(JobId, String, String), usize>,
    /// Jobs that have already attempted normalization retry (one-shot guard).
    pub(super) normalization_retried: HashSet<JobId>,
    /// Members where extraction CRC passed and chunks are in the DB, but the
    /// output file isn't fully concatenated yet.  Separate from `extracted_members`
    /// to prevent `try_delete_volumes` from treating them as fully extracted.
    pub(super) pending_concat: HashMap<JobId, HashSet<String>>,
    /// Jobs where all archive members extracted with CRC pass — PAR2
    /// verification/repair is unnecessary.
    pub(super) par2_bypassed: HashSet<JobId>,
    /// Jobs whose PAR2 set has already validated the current payload bytes.
    pub(super) par2_verified: HashSet<JobId>,
    /// Split sets a recovery set has already answered for, keyed by set name,
    /// with the posted parts that join into it.
    ///
    /// A posting of `<name>.001/.002/.003` whose recovery data is computed over
    /// `<name>` describes a file nothing in the posting is called. The recovery
    /// pass reads the parts as one file and installs `<name>` itself, so the
    /// join has already happened: the split topology that would run it again
    /// is retired here, and the parts it names become consumed inputs rather
    /// than payload the job is still short of.
    pub(super) par2_joined_split_sets: HashMap<JobId, HashMap<String, HashSet<String>>>,
    /// Working-directory entry names as they stood immediately before a repair
    /// ran, per job.
    ///
    /// A repair leaves artefacts behind — par2-rs renames the damaged original
    /// aside before installing the repaired file, and only purges it when asked
    /// — and finalization relocates the whole directory, so anything still
    /// sitting there ships. `Par2RepairOutcome` does not report those paths, so
    /// the only way to name them without guessing at a suffix convention is to
    /// know what was there first. Consumed once the repair is accepted; dropped
    /// unread when it fails, which is what leaves the evidence in place.
    pub(super) par2_pre_repair_dir_entries: HashMap<JobId, HashSet<String>>,
    /// Jobs the SFV fallback has already ruled on (one-shot guard). The
    /// completion gate is re-entered many times per job and the fallback's
    /// disk arm reads the whole payload, so it runs once.
    pub(super) sfv_checked: HashSet<JobId>,
    /// Jobs that have already contributed a row to `weaver_verifications_total`.
    /// Claimed by the first verdict a job produces, so the `unverifiable`
    /// fallback recorded when a job ends with no PAR2 set can never
    /// double-count a job an actual pass already ruled on.
    pub(in crate::pipeline) jobs_with_verification_outcome: HashSet<JobId>,
    /// Promoted PAR2 recovery segments that can no longer be fetched or decoded.
    pub(super) unavailable_promoted_recovery_segments: HashSet<SegmentId>,
    /// Finished jobs (Complete/Failed) from recovery — surfaced in list/get queries.
    pub(super) finished_jobs: Vec<JobInfo>,
    /// Shared state for control plane reads (API handlers read without channel round-trip).
    pub(super) shared_state: SharedPipelineState,
    /// SQLite database for durable history.
    pub(super) db: crate::Database,
    /// Tracks detached `db_fire_and_forget` writes so shutdown `drain` can join
    /// them (bounded) instead of dropping in-flight writes. `db_fire_and_forget`
    /// only has `&self`, so the JoinSet lives behind a shared mutex; the lock is
    /// only ever held to spawn/drain, never across an await.
    pub(super) fire_and_forget_tasks: Arc<std::sync::Mutex<tokio::task::JoinSet<()>>>,
    /// Shared config for runtime category lookups (dest_dir overrides).
    pub(super) config: crate::settings::SharedConfig,
    /// Dedicated low-priority rayon thread pool for post-processing
    /// (extraction, PAR2 verify/repair). Niced on Unix so the OS scheduler
    /// prefers download/decode threads when CPU is contended.
    pub(super) pp_pool: Arc<rayon::ThreadPool>,
    /// Environment-derived, always-on extraction ceilings.
    pub(super) extraction_limits: Arc<ExtractionLimits>,
    /// One decoder-memory allowance shared by every extraction job.
    pub(super) process_memory_budget: Arc<ProcessMemoryBudget>,
    /// The post-processing pool again, for direct-unpack chases only.
    ///
    /// A chase runs its decode inside `install`, which occupies one worker
    /// thread for as long as the closure runs — and a chase's closure parks,
    /// sometimes for the whole of a download and a repair. Sharing `pp_pool`
    /// meant enough parked chases exhausted the post-processing pool and no
    /// extraction of any kind could start. Chases contend only with each other
    /// here.
    pub(super) chase_pool: Arc<rayon::ThreadPool>,
    /// The same allowance again, for direct-unpack chases only.
    ///
    /// A chase takes its permit before it opens the archive and holds it until
    /// it returns — across every park the gated reader does waiting on the
    /// download it is chasing. Drawing that from the shared pool meant one
    /// parked chase stopped every other 7z extraction in the process, including
    /// the conventional extractions that were the job's actual critical path.
    ///
    /// Chases share this pool with each other, so at most one chase holds
    /// decoder memory at a time no matter how many are armed. The cost is that
    /// worst-case decoder memory is now two allowances rather than one: one
    /// speculative chase plus one real extraction.
    pub(super) direct_unpack_process_memory: Arc<ProcessMemoryBudget>,
    /// One shared output budget per job, retained across nested extraction layers.
    pub(super) extraction_budgets: HashMap<JobId, Arc<JobExtractionBudget>>,
    /// A job's normalized unacceptable-extension policy is fixed at its first
    /// archive extraction and reused by every subsequent RAR set and retry.
    pub(super) unacceptable_extension_policies: HashMap<JobId, Arc<PostProcessingSettings>>,
}

#[cfg(test)]
mod tests;
