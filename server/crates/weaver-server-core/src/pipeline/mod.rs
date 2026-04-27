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
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use std::collections::HashSet;

#[cfg(test)]
use crate::RestoreJobRequest;
use crate::bandwidth::service::BandwidthCapRuntime;
use crate::events::model::PipelineEvent;
use crate::jobs::assembly::ExtractionReadiness;
#[cfg(test)]
use crate::jobs::assembly::JobAssembly;
use crate::jobs::assembly::write_buffer::{BufferedChunk, WriteReorderBuffer};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::runtime::buffers::{BufferHandle, BufferPool};
use crate::runtime::system_profile::SystemProfile;
use crate::{ActiveFileProgress, CommittedSegment};
use crate::{
    DownloadQueue, DownloadWork, JobInfo, JobSpec, JobState, JobStatus, PipelineMetrics,
    RuntimeTuner, SchedulerCommand, SchedulerError, SharedPipelineState, TokenBucket,
};
use weaver_nntp::NntpClient;
#[cfg(test)]
use weaver_par2::checksum;
use weaver_par2::par2_set::Par2FileSet;

use self::archive::rar_state::RarSetState;

/// Maximum number of retries for a single segment before giving up.
const MAX_SEGMENT_RETRIES: u32 = 3;
const FILE_PROGRESS_FLUSH_DELTA_BYTES: u64 = 4 * 1024 * 1024;
const STALLED_DOWNLOAD_CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);
const STALLED_DOWNLOAD_IDLE_THRESHOLD: Duration = Duration::from_secs(5 * 60);

/// Result of a download task.
pub(super) struct DownloadResult {
    pub(super) segment_id: SegmentId,
    pub(super) data: std::result::Result<DownloadPayload, DownloadError>,
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

pub(super) enum DownloadError {
    Fetch(String),
}

/// Successful download payload waiting for decode scheduling.
pub(super) struct PendingDecodeWork {
    pub(super) segment_id: SegmentId,
    pub(super) raw: Bytes,
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
}

/// Result of a background extraction task.
pub(super) struct BatchExtractionOutcome {
    pub(super) extracted: Vec<String>,
    pub(super) failed: Vec<(String, String)>,
}

pub(super) struct FullSetExtractionOutcome {
    pub(super) extracted: Vec<String>,
    pub(super) failed: Vec<String>,
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

/// Result of a decode task.
pub(super) struct DecodeResult {
    pub(super) segment_id: SegmentId,
    pub(super) file_offset: u64,
    pub(super) decoded_size: u32,
    pub(super) crc_valid: bool,
    pub(super) crc32: u32,
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
    },
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
    pub(super) crc32: u32,
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
    /// Job dispatch order (FIFO by submission). First Downloading job is active.
    pub(super) job_order: Vec<JobId>,
    /// Number of in-flight downloads (primary + recovery).
    pub(super) active_downloads: usize,
    /// Number of in-flight recovery downloads (subset of active_downloads).
    pub(super) active_recovery: usize,
    /// Jobs currently inside an active article download pass.
    pub(super) active_download_passes: HashSet<JobId>,
    /// In-flight article download count per job.
    pub(super) active_downloads_by_job: HashMap<JobId, usize>,
    /// In-flight decode task count per job.
    pub(super) active_decodes_by_job: HashMap<JobId, usize>,
    /// Last time a job made observable progress in the download stage.
    pub(super) job_last_download_activity: HashMap<JobId, Instant>,
    /// Delayed retry tasks that have been scheduled but not yet re-queued.
    pub(super) pending_retries_by_job: HashMap<JobId, usize>,
    /// Directory for active downloads (per-job subdirectories).
    pub(super) intermediate_dir: PathBuf,
    /// Directory for completed downloads (category subdirectories).
    pub(super) complete_dir: PathBuf,
    /// Directory where persisted NZB files live (for reprocessing after restart).
    pub(super) nzb_dir: PathBuf,
    /// Pending segment commits (flushed to SQLite in batches).
    pub(super) segment_batch: Vec<CommittedSegment>,
    /// Per-file contiguous write floors awaiting persistence.
    pub(super) pending_file_progress: HashMap<NzbFileId, u64>,
    /// Last queued/persisted contiguous write floor per file.
    pub(super) persisted_file_progress: HashMap<NzbFileId, u64>,
    /// Streaming MD5 state for files whose decoded bytes have been observed in order.
    pub(super) file_hash_states: HashMap<NzbFileId, weaver_par2::checksum::FileHashState>,
    /// Files that need a one-time disk reread because out-of-order persistence broke the stream.
    pub(super) file_hash_reread_required: HashSet<NzbFileId>,
    #[cfg(test)]
    pub(super) try_update_archive_topology_calls: usize,
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
    /// Whether all downloads are globally paused.
    pub(super) global_paused: bool,
    /// ISP bandwidth cap runtime state.
    pub(crate) bandwidth_cap: BandwidthCapRuntime,
    /// Conservative byte reservations for in-flight downloads used to enforce the
    /// ISP bandwidth cap before actual payload bytes are known.
    pub(crate) bandwidth_reservations: HashMap<SegmentId, u64>,
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
