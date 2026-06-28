pub mod archive;
#[cfg(test)]
pub(crate) use archive::rar_state;
mod completion;
mod decode;
pub mod download;
mod extraction;
mod health;
mod orchestrator;
mod repair;

pub(crate) use orchestrator::check_disk_space;
#[cfg(test)]
use orchestrator::compute_write_backlog_budget_bytes;
use orchestrator::{is_terminal_status, write_segment_to_disk};

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use std::collections::HashSet;

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
use crate::runtime::buffers::{BufferHandle, BufferPool};
use crate::runtime::system_profile::SystemProfile;
use crate::{
    DownloadQueue, DownloadWork, JobInfo, JobSpec, JobState, JobStatus, PipelineMetrics,
    RuntimeTuner, SchedulerCommand, SchedulerError, SharedPipelineState, TokenBucket,
};
use weaver_nntp::NntpClient;
#[cfg(test)]
use weaver_par2::checksum;
use weaver_par2::par2_set::Par2FileSet;

use self::archive::rar_state::{RarDerivedPlan, RarSetState};

/// Maximum number of retries for a single segment before giving up.
const MAX_SEGMENT_RETRIES: u32 = 3;
const DOWNLOAD_RESTART_CHECKPOINT_BYTES: u64 = 256 * 1024 * 1024;
const STALLED_DOWNLOAD_CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);
const STALLED_DOWNLOAD_IDLE_THRESHOLD: Duration = Duration::from_secs(5 * 60);

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

/// Result of a download task.
pub(super) struct DownloadResult {
    pub(super) segment_id: SegmentId,
    pub(super) data: std::result::Result<DownloadPayload, DownloadError>,
    pub(super) attempts: Vec<weaver_nntp::client::FetchAttemptTrace>,
    /// Server that successfully served this payload, if known.
    pub(super) source_server_idx: Option<usize>,
    /// Whether this was a speculative recovery download.
    pub(super) is_recovery: bool,
    /// How many times this segment has been retried so far.
    pub(super) retry_count: u32,
    /// Servers intentionally excluded for this fetch attempt.
    pub(super) exclude_servers: Vec<usize>,
}

pub(super) enum DownloadPayload {
    Raw(Bytes),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DownloadFailureKind {
    ArticleNotFound,
    CapacityUnavailable,
    Transient,
    Auth,
    Permanent,
}

#[derive(Debug, Clone)]
pub(super) struct DownloadFailure {
    pub(super) kind: DownloadFailureKind,
    pub(super) message: String,
}

impl DownloadFailure {
    pub(super) fn new(kind: DownloadFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(super) fn from_nntp(error: weaver_nntp::NntpError) -> Self {
        use weaver_nntp::NntpError;

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

#[derive(Debug, Clone, Default)]
pub(super) struct RarRefreshState {
    pub(super) in_flight: Option<RarRefreshRequest>,
    pub(super) queued: Option<RarRefreshRequest>,
    pub(super) latest_completed_volume: u32,
    pub(super) refreshed_volumes: BTreeSet<u32>,
    pub(super) structure_dirty: bool,
    pub(super) last_error: Option<String>,
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
    pub(super) result: Result<ComputedRarSetState, String>,
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
}

pub(super) struct FullSetExtractionOutcome {
    pub(super) extracted: Vec<String>,
    pub(super) failed: Vec<String>,
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
    pub(super) file_offset: u64,
    pub(super) decoded_size: u32,
    pub(super) crc_valid: bool,
    pub(super) expected_file_crc: Option<u32>,
    pub(super) data: DecodedChunk,
    /// Original filename from the yEnc header (for swap detection observability).
    pub(super) yenc_name: String,
}

/// Completion of a decode task, including explicit failures so backlog
/// accounting is always drained.
pub(super) enum DecodeDone {
    Success(DecodeResult),
    Failed {
        segment_id: SegmentId,
        error: String,
        source_server_idx: Option<usize>,
        exclude_servers: Vec<usize>,
    },
}

pub(super) struct CompletedFileChecksum {
    pub(super) md5: [u8; 16],
    pub(super) crc32: u32,
}

pub(super) struct CompletedFileChecksumState {
    md5: weaver_par2::checksum::FileHashState,
    crc32: crc32fast::Hasher,
}

impl CompletedFileChecksumState {
    pub(super) fn new() -> Self {
        Self {
            md5: weaver_par2::checksum::FileHashState::new(),
            crc32: crc32fast::Hasher::new(),
        }
    }

    pub(super) fn update(&mut self, data: &[u8]) {
        self.md5.update(data);
        self.crc32.update(data);
    }

    pub(super) fn bytes_fed(&self) -> u64 {
        self.md5.bytes_fed()
    }

    pub(super) fn finalize(self) -> CompletedFileChecksum {
        CompletedFileChecksum {
            md5: self.md5.finalize(),
            crc32: self.crc32.finalize(),
        }
    }
}

impl Default for CompletedFileChecksumState {
    fn default() -> Self {
        Self::new()
    }
}

pub(super) struct DecodedChunk(Box<[u8]>);

impl DecodedChunk {
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub(super) fn len_bytes(&self) -> usize {
        self.0.len()
    }
}

impl From<Vec<u8>> for DecodedChunk {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into_boxed_slice())
    }
}

pub(super) struct BufferedDecodedSegment {
    pub(super) segment_id: SegmentId,
    pub(super) decoded_size: u32,
    pub(super) data: DecodedChunk,
    pub(super) yenc_name: String,
}

impl BufferedChunk for BufferedDecodedSegment {
    fn len_bytes(&self) -> usize {
        self.data.len_bytes()
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
    /// Winning archive password candidate per job/RAR set, kept process-local and redacted.
    pub(super) archive_password_winners: HashMap<(JobId, String), ArchivePasswordCandidate>,
    /// Job dispatch order (FIFO by submission). First Downloading job is active.
    pub(super) job_order: Vec<JobId>,
    /// Number of in-flight downloads (primary + recovery).
    pub(super) active_downloads: usize,
    /// Number of in-flight recovery downloads (subset of active_downloads).
    pub(super) active_recovery: usize,
    /// Jobs currently inside an active article download pass.
    pub(super) active_download_passes: HashSet<JobId>,
    /// Jobs that still have decode/write pipeline work after network downloads finished.
    pub(super) jobs_finalizing_download: HashSet<JobId>,
    /// In-flight article download count per job.
    pub(super) active_downloads_by_job: HashMap<JobId, usize>,
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
    pub(super) decode_done_tx: mpsc::Sender<DecodeDone>,
    pub(super) decode_done_rx: mpsc::Receiver<DecodeDone>,
    /// Channel for delayed retries — segments sleep then come back here.
    pub(super) retry_tx: mpsc::Sender<DownloadWork>,
    pub(super) retry_rx: mpsc::Receiver<DownloadWork>,
    /// Channel for health probe results: (job_id, total_probes, missed_count).
    pub(super) probe_result_tx: mpsc::Sender<ProbeUpdate>,
    pub(super) probe_result_rx: mpsc::Receiver<ProbeUpdate>,
    /// Channel for background extraction results.
    pub(super) extract_done_tx: mpsc::Sender<ExtractionDone>,
    pub(super) extract_done_rx: mpsc::Receiver<ExtractionDone>,
    /// Channel for background RAR topology refresh results.
    pub(super) rar_refresh_done_tx: mpsc::Sender<RarRefreshDone>,
    pub(super) rar_refresh_done_rx: mpsc::Receiver<RarRefreshDone>,
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
    /// Max in-memory decoded backlog before degrading to direct offset writes.
    pub(super) write_backlog_budget_bytes: usize,
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
    /// Archives with in-flight extraction tasks (spawned but not yet completed).
    /// Prevents duplicate spawns and ensures cleanup waits for extraction to finish.
    pub(super) inflight_extractions: HashMap<JobId, HashSet<String>>,
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
    /// Shared config for runtime category lookups (dest_dir overrides).
    pub(super) config: crate::settings::SharedConfig,
    /// Dedicated low-priority rayon thread pool for post-processing
    /// (extraction, PAR2 verify/repair). Niced on Unix so the OS scheduler
    /// prefers download/decode threads when CPU is contended.
    pub(super) pp_pool: Arc<rayon::ThreadPool>,
}

#[cfg(test)]
mod tests;
