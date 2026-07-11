pub mod archive;
#[cfg(test)]
pub(crate) use archive::rar_state;
mod capacity;
mod completion;
mod decode;
pub mod download;
mod extraction;
mod health;
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
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
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
use crate::runtime::buffers::{BufferHandle, BufferPool};
use crate::runtime::system_profile::SystemProfile;
use crate::{
    DispatchShareMode, DownloadPressureReason, DownloadPressureState, DownloadQueue, DownloadWork,
    JobInfo, JobSpec, JobState, JobStatus, PipelineMetrics, RuntimeTuner, SchedulerCommand,
    SchedulerError, SharedPipelineState, SpilloverDecision, TokenBucket,
};
use weaver_nntp::NntpClient;
#[cfg(test)]
use weaver_par2::checksum;
use weaver_par2::par2_set::Par2FileSet;

use self::archive::rar_state::{RarDerivedPlan, RarSetState};
use self::download::{
    DownloadLaneMode, DownloadLaneRuntimeState, JobTransportProfile, LaneParkReason,
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
        let spec_password = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.spec.password.as_deref());
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

        candidates
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
    pub(super) groups: Vec<String>,
    pub(super) exclude_servers: Vec<usize>,
}

impl DownloadBatchCompatibility {
    fn from_work(work: &DownloadWork) -> Self {
        Self {
            priority: work.priority,
            is_recovery: work.is_recovery,
            groups: work.groups.clone(),
            exclude_servers: work.exclude_servers.clone(),
        }
    }

    fn matches(&self, work: &DownloadWork) -> bool {
        work.priority == self.priority
            && work.is_recovery == self.is_recovery
            && work.groups == self.groups
            && work.exclude_servers == self.exclude_servers
    }
}

pub(super) struct DownloadBatchLease {
    pub(super) job_id: JobId,
    pub(super) lane_mode: DownloadLaneMode,
    pub(super) spillover_loan_kind: Option<SpilloverLoanKind>,
    pub(super) server_modes: Vec<(usize, DownloadLaneMode)>,
    pub(super) compatibility: DownloadBatchCompatibility,
    /// Compatibility excludes plus the job's retention exclusions — the set
    /// server ordering and lane acquisition use. Results keep reporting the
    /// compatibility (failure-only) excludes; retention stays job-derived.
    pub(super) effective_exclude_servers: Vec<usize>,
    pub(super) works: Vec<DownloadWork>,
}

pub(super) struct DownloadLaneRefillRequest {
    pub(super) job_id: JobId,
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
    BoundedSameBand,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SpilloverLoanState {
    pub(super) measured_lent_connections: usize,
    pub(super) bounded_lent_connections: usize,
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
        if kind == SpilloverLoanKind::MeasuredUnderfill && self.measured_lent_connections() == 0 {
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
                measured_lent_connections: usize::from(
                    kind == SpilloverLoanKind::MeasuredUnderfill,
                ),
                bounded_lent_connections: usize::from(kind == SpilloverLoanKind::BoundedSameBand),
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

    pub(super) fn bounded_lent_connections(&self) -> usize {
        self.loans
            .values()
            .map(|loan| loan.bounded_lent_connections)
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
            SpilloverLoanKind::BoundedSameBand => {
                self.bounded_lent_connections = self.bounded_lent_connections.saturating_add(1)
            }
        }
    }

    fn decrement(&mut self, kind: SpilloverLoanKind) {
        match kind {
            SpilloverLoanKind::MeasuredUnderfill => {
                self.measured_lent_connections = self.measured_lent_connections.saturating_sub(1)
            }
            SpilloverLoanKind::BoundedSameBand => {
                self.bounded_lent_connections = self.bounded_lent_connections.saturating_sub(1)
            }
        }
    }

    fn total_lent_connections(&self) -> usize {
        self.measured_lent_connections
            .saturating_add(self.bounded_lent_connections)
    }
}

#[derive(Debug, Default)]
pub(super) struct HotShareYieldSignal {
    requested_hot_job_id: AtomicU64,
}

impl HotShareYieldSignal {
    pub(super) fn request(&self, job_id: JobId) {
        self.requested_hot_job_id.store(job_id.0, Ordering::Relaxed);
    }

    pub(super) fn clear(&self) {
        self.requested_hot_job_id.store(0, Ordering::Relaxed);
    }

    pub(super) fn is_requested_for(&self, job_id: JobId) -> bool {
        self.requested_hot_job_id.load(Ordering::Relaxed) == job_id.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HotBestModeBlockReason {
    None,
    HotHasQueuedPrimary,
    LaneCapacityAvailable,
    PipelinePromotionPending,
    RecentExpansionHelped,
}

impl HotBestModeBlockReason {
    pub(super) fn as_code(self) -> usize {
        match self {
            Self::None => 0,
            Self::HotHasQueuedPrimary => 1,
            Self::LaneCapacityAvailable => 2,
            Self::PipelinePromotionPending => 3,
            Self::RecentExpansionHelped => 4,
        }
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
    pub(super) reason: LaneParkReason,
    pub(super) release_connection_slot: bool,
    pub(super) release_ip_replacement_burst: bool,
}

pub(super) enum OwnedDownloadLaneEvent {
    AcquireFailed {
        lease: DownloadBatchLease,
        error: String,
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
    IpReplacementTrial,
}

impl DownloadResultOrigin {
    pub(super) fn from_recovery(is_recovery: bool) -> Self {
        if is_recovery {
            Self::Recovery
        } else {
            Self::NormalPrimary
        }
    }

    pub(super) fn is_recovery(self) -> bool {
        matches!(self, Self::Recovery)
    }

    pub(super) fn counts_for_hot_primary(self) -> bool {
        matches!(self, Self::NormalPrimary)
    }
}

/// Result of a download task.
pub(super) struct DownloadResult {
    pub(super) segment_id: SegmentId,
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
    pub(in crate::pipeline) work: DownloadWork,
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
    Transient,
    Auth,
    Permanent,
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

        let kind = match error {
            NntpError::AuthenticationFailed
            | NntpError::AuthenticationRejected
            | NntpError::AuthenticationRequired
            | NntpError::AccessDenied => DownloadFailureKind::Auth,
            NntpError::NoSuchGroup
            | NntpError::NoGroupSelected
            | NntpError::CommandNotRecognized
            | NntpError::TlsRequired
            | NntpError::UnexpectedResponse { .. }
            | NntpError::MalformedResponse(_) => DownloadFailureKind::Permanent,
            _ => DownloadFailureKind::LaneUnavailable,
        };

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
            NntpError::PoolExhausted | NntpError::PoolShutdown | NntpError::TooManyConnections => {
                DownloadFailureKind::CapacityUnavailable
            }
            NntpError::AuthenticationFailed
            | NntpError::AuthenticationRejected
            | NntpError::AuthenticationRequired
            | NntpError::AccessDenied => DownloadFailureKind::Auth,
            NntpError::ServiceUnavailable
            | NntpError::Timeout
            | NntpError::SoftTimeout(_)
            | NntpError::ConnectionClosed
            | NntpError::ServerDisconnectedMidBody
            | NntpError::TruncatedMultilineBody
            | NntpError::MalformedMultilineTerminator
            | NntpError::Io(_) => DownloadFailureKind::Transient,
            _ => DownloadFailureKind::Permanent,
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
}

pub(super) struct ComputedRarSetState {
    pub(super) plan: RarDerivedPlan,
    pub(super) headers: Vec<u8>,
    pub(super) rebuild_source: archive::topology::RarTopologyRebuildSource,
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

pub(super) struct VerifiedSuspectPersistDone {
    pub(super) job_id: JobId,
    pub(super) set_name: String,
    pub(super) version: u64,
    pub(super) result: Result<(), String>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct VerifiedSuspectPersistState {
    pub(super) desired: HashSet<u32>,
    pub(super) in_flight_version: Option<u64>,
    pub(super) next_version: u64,
    pub(super) queued: bool,
}

pub(crate) enum RarPasswordAttemptError {
    Rar(weaver_unrar::RarError),
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

impl From<weaver_unrar::RarError> for RarPasswordAttemptError {
    fn from(value: weaver_unrar::RarError) -> Self {
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Par2FileRuntime {
    pub(super) filename: String,
    pub(super) recovery_blocks: u32,
    pub(super) promoted: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct Par2RuntimeState {
    pub(super) set: Option<Arc<Par2FileSet>>,
    pub(super) files: HashMap<u32, Par2FileRuntime>,
    /// Scan state from the most recent analyze pass, reused by the execute
    /// pass so a repair does not re-scan sources the analysis just hashed.
    /// Self-invalidating: the engine re-stats every observed file and falls
    /// back to a full scan on any drift.
    pub(super) scan_carry: Option<Arc<weaver_par2::ScanCarry>>,
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

pub(super) struct MoveToCompleteResult {
    pub(super) moved_entries: u32,
}

pub(super) struct MoveToCompleteDone {
    pub(super) job_id: JobId,
    pub(super) dest: PathBuf,
    pub(super) result: Result<MoveToCompleteResult, String>,
}

/// Result of a decode task.
pub(super) struct DecodeResult {
    pub(super) segment_id: SegmentId,
    pub(super) raw_size: u64,
    /// Present only when this successful decode lacked an independently
    /// verified yEnc part CRC. Keeps provenance off the verified hot path.
    pub(super) unverified_provenance: Option<Box<UnverifiedSegmentProvenance>>,
    pub(super) file_offset: u64,
    pub(super) decoded_size: u32,
    pub(super) crc_valid: bool,
    pub(super) part_crc_verified: bool,
    pub(super) part_crc: u32,
    pub(super) expected_file_crc: Option<u32>,
    pub(super) data: DecodedChunk,
    /// Original filename from the yEnc header (for swap detection observability).
    pub(super) yenc_name: String,
}

#[derive(Debug)]
pub(super) struct UnverifiedSegmentProvenance {
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
    Success(DecodeResult),
    Failed {
        segment_id: SegmentId,
        raw_size: u64,
        error: String,
        source_server_idx: Option<usize>,
        exclude_servers: Vec<usize>,
    },
}

pub(super) struct CompletedFileChecksum {
    pub(super) md5: Option<[u8; 16]>,
    pub(super) crc32: u32,
}

pub(super) struct StreamedCompletedFileChecksum {
    pub(super) md5: Option<[u8; 16]>,
    pub(super) crc32: u32,
    pub(super) all_parts_crc_verified: bool,
}

pub(super) struct CompletedFileChecksumState {
    md5: Option<weaver_par2::checksum::FileHashState>,
    crc32: u32,
    crc32_combine_op: Option<(u64, weaver_par2::checksum::Crc32CombineOp)>,
    bytes_fed: u64,
    all_parts_crc_verified: bool,
}

impl CompletedFileChecksumState {
    pub(super) fn new() -> Self {
        Self {
            md5: Some(weaver_par2::checksum::FileHashState::new()),
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
            self.crc32_combine_op = Some((len, weaver_par2::checksum::Crc32CombineOp::new(len)));
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
            md5: self.md5.map(weaver_par2::checksum::FileHashState::finalize),
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
    pub(super) data: DecodedChunk,
    pub(super) part_crc: u32,
    pub(super) part_crc_verified: bool,
    pub(super) yenc_name: String,
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
    /// Number of in-flight recovery downloads (subset of active_downloads).
    pub(super) active_recovery: usize,
    /// Current hot job receiving exclusive article-dispatch preference.
    pub(super) hot_dispatch_job: Option<JobId>,
    /// When the current hot-dispatch ownership period began.
    pub(super) hot_dispatch_started_at: Option<Instant>,
    /// Successful primary BODY responses observed during the current hot period.
    pub(super) hot_dispatch_successes: u64,
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
    #[cfg(test)]
    pub(super) par2_repairer_analyze_calls: usize,
    #[cfg(test)]
    pub(super) par2_repairer_execute_calls: usize,
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
    /// Channel for delayed retries — segments sleep then come back here.
    pub(in crate::pipeline) retry_tx: mpsc::Sender<RetryWork>,
    pub(in crate::pipeline) retry_rx: mpsc::Receiver<RetryWork>,
    /// Monotonic NNTP pool generation, bumped on every `RebuildNntp`. Server
    /// indices in `DownloadWork::exclude_servers` are only meaningful within
    /// the generation they were computed under; delayed retries carry the
    /// generation they were scheduled under so stale indices can be dropped
    /// when they re-enter after a rebuild.
    pub(super) pool_generation: u64,
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
    /// Channel for serialized verified-suspect persistence completions.
    pub(super) verified_suspect_persist_done_tx: mpsc::Sender<VerifiedSuspectPersistDone>,
    pub(super) verified_suspect_persist_done_rx: mpsc::Receiver<VerifiedSuspectPersistDone>,
    /// Channel for background final-move results.
    pub(super) move_done_tx: mpsc::Sender<MoveToCompleteDone>,
    pub(super) move_done_rx: mpsc::Receiver<MoveToCompleteDone>,
    /// Whether all downloads are globally paused.
    pub(super) global_paused: bool,
    /// ISP bandwidth cap runtime state.
    pub(crate) bandwidth_cap: BandwidthCapRuntime,
    /// Conservative byte reservations for in-flight downloads used to enforce the
    /// ISP bandwidth cap before actual payload bytes are known.
    pub(crate) bandwidth_reservations: HashMap<SegmentId, u64>,
    /// Estimated bytes charged to the speed limiter for in-flight downloads.
    pub(crate) rate_limit_reservations: HashMap<SegmentId, u64>,
    /// Bandwidth rate limiter.
    pub(super) rate_limiter: TokenBucket,
    /// Gradual connection ramp-up limit (increases each tick).
    pub(super) connection_ramp: usize,
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
    /// Per-job delay after restart-durable-lead throttling parks primary work.
    pub(super) download_restart_durable_lead_retry_after: HashMap<JobId, Instant>,
    /// Last time we logged a queued/no-active-download liveness stall.
    pub(super) last_download_dispatch_stall_log_at: Option<Instant>,
    /// Current in-memory decoded backlog retained for sequential write ordering.
    pub(super) write_buffered_bytes: usize,
    /// Current in-memory decoded segment count retained for sequential write ordering.
    pub(super) write_buffered_segments: usize,
    /// Per-file write reorder buffers for decoded segments waiting on write order.
    pub(super) write_buffers: HashMap<NzbFileId, WriteReorderBuffer<BufferedDecodedSegment>>,
    /// Authoritative PAR2 runtime state per job.
    pub(super) par2_runtime: HashMap<JobId, Par2RuntimeState>,
    /// RAR members already extracted per job (for incremental RAR extraction).
    pub(super) extracted_members: HashMap<JobId, HashSet<String>>,
    /// Archives whose extraction has completed successfully (by archive name).
    /// For RAR this is the set name; for 7z/zip/tar/gz it's the archive name.
    pub(super) extracted_archives: HashMap<JobId, HashSet<String>>,
    /// Tracks decode failure retries per segment. When yEnc decode fails (CRC/size
    /// mismatch), the segment is re-downloaded. After `MAX_SEGMENT_RETRIES` decode
    /// failures, the segment is marked permanently failed.
    pub(super) decode_retries: HashMap<SegmentId, u32>,
    /// Successfully decoded segments whose yEnc part CRC was absent. These are
    /// targeted for replacement only if the completed file CRC proves corruption.
    pub(super) unverified_segments: HashMap<NzbFileId, HashMap<u32, UnverifiedSegmentProvenance>>,
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
    /// Runtime-only serialized persistence state for verified suspect volumes.
    verified_suspect_persist_state: HashMap<(JobId, String), VerifiedSuspectPersistState>,
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
}

#[cfg(test)]
mod tests;
