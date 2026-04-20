use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::jobs::assembly::{DetectedArchiveIdentity, JobAssembly};
use crate::jobs::ids::JobId;
use crate::jobs::record::ActiveFileIdentity;
use crate::pipeline::download::queue::{DownloadQueue, DownloadWork};
use weaver_model::files::FileRole;

/// A job definition submitted to the scheduler.
/// Contains everything needed to create and run a download job.
#[derive(Clone)]
pub struct JobSpec {
    /// Human-readable name for the job.
    pub name: String,
    /// Password for encrypted archives (from NZB meta or filename {{password}} convention).
    pub password: Option<String>,
    /// Files to download, each with their segments.
    pub files: Vec<FileSpec>,
    /// Total expected bytes across all files.
    pub total_bytes: u64,
    /// Optional category (e.g. "tv", "movies") for organizing downloads.
    pub category: Option<String>,
    /// Arbitrary key-value metadata attached by the submitting client.
    pub metadata: Vec<(String, String)>,
}

impl JobSpec {
    /// Total bytes of PAR2 recovery files in this spec.
    pub fn par2_bytes(&self) -> u64 {
        self.files
            .iter()
            .filter(|file| matches!(file.role, FileRole::Par2 { .. }))
            .flat_map(|file| file.segments.iter())
            .map(|segment| segment.bytes as u64)
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FieldUpdate<T> {
    #[default]
    Unchanged,
    Clear,
    Set(T),
}

impl<T> FieldUpdate<T> {
    pub fn is_unchanged(&self) -> bool {
        matches!(self, Self::Unchanged)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JobUpdate {
    pub category: FieldUpdate<String>,
    pub metadata: FieldUpdate<Vec<(String, String)>>,
}

impl JobUpdate {
    pub fn is_empty(&self) -> bool {
        self.category.is_unchanged() && self.metadata.is_unchanged()
    }

    pub fn apply_to_spec(&self, spec: &mut JobSpec) {
        match &self.category {
            FieldUpdate::Unchanged => {}
            FieldUpdate::Clear => spec.category = None,
            FieldUpdate::Set(category) => spec.category = Some(category.clone()),
        }

        match &self.metadata {
            FieldUpdate::Unchanged => {}
            FieldUpdate::Clear => spec.metadata.clear(),
            FieldUpdate::Set(metadata) => spec.metadata = metadata.clone(),
        }
    }
}

/// Specification for a single file within a job.
#[derive(Clone)]
pub struct FileSpec {
    /// Filename (extracted from NZB subject).
    pub filename: String,
    /// Inferred file role.
    pub role: FileRole,
    /// Newsgroups to try.
    pub groups: Vec<String>,
    /// Segments (articles) that make up this file.
    pub segments: Vec<SegmentSpec>,
}

/// Specification for a single segment to download.
#[derive(Clone)]
pub struct SegmentSpec {
    /// 0-indexed segment number.
    pub number: u32,
    /// Expected decoded size in bytes.
    pub bytes: u32,
    /// NNTP message-id (without angle brackets).
    pub message_id: String,
}

/// Status of a job in the scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    Queued,
    Downloading,
    /// Health probe in progress — checking article availability.
    Checking,
    Verifying,
    QueuedRepair,
    Repairing,
    QueuedExtract,
    Extracting,
    /// Moving extracted files to the final destination directory.
    Moving,
    Complete,
    Failed {
        error: String,
    },
    Paused,
}

/// Internal state for a running job.
pub struct JobState {
    pub job_id: JobId,
    pub spec: JobSpec,
    pub status: JobStatus,
    pub assembly: JobAssembly,
    /// Number of nested archive extraction passes already performed.
    pub extraction_depth: u32,
    pub created_at: std::time::Instant,
    /// Wall-clock creation time (Unix epoch milliseconds).
    pub created_at_epoch_ms: f64,
    /// When this job most recently entered the bounded repair queue.
    pub queued_repair_at_epoch_ms: Option<f64>,
    /// When this job most recently entered the bounded extraction queue.
    pub queued_extract_at_epoch_ms: Option<f64>,
    /// Safe status to restore when a paused job is resumed.
    pub paused_resume_status: Option<JobStatus>,
    /// Per-job working directory (subdirectory under intermediate_dir).
    /// All download/decode/verify/repair/extract I/O happens here.
    pub working_dir: PathBuf,
    /// Bytes downloaded for this specific job.
    pub downloaded_bytes: u64,
    /// Conservative restored progress floor from persisted file-write checkpoints.
    /// This is only used for reporting after restart and must not affect scheduling.
    pub restored_download_floor_bytes: u64,
    /// Bytes from segments that are permanently lost (430 / max retries exhausted).
    pub failed_bytes: u64,
    /// Total bytes of PAR2 recovery files, cached from spec at job creation.
    pub par2_bytes: u64,
    /// Whether health probes have been dispatched for this job.
    pub health_probing: bool,
    /// Highest failed-byte watermark that has already been health-probed.
    pub last_health_probe_failed_bytes: u64,
    /// Persisted probe facts used for pre-extraction classification.
    pub detected_archives: std::collections::HashMap<u32, DetectedArchiveIdentity>,
    /// Mutable file identity used by runtime/archive logic after rename/classification.
    pub file_identities: std::collections::HashMap<u32, ActiveFileIdentity>,
    /// Segments pulled from queues while health probe runs. Restored on
    /// probe pass, dropped on probe fail.
    pub held_segments: Vec<DownloadWork>,
    /// Primary download queue (data files, PAR2 index, RAR volumes).
    pub download_queue: DownloadQueue,
    /// Recovery download queue (PAR2 repair blocks, spare bandwidth).
    pub recovery_queue: DownloadQueue,
    /// Staging directory for extraction output (under complete_dir).
    /// Extraction writes chunks and assembled files here instead of
    /// working_dir, keeping local storage usage low for NFS setups.
    pub staging_dir: Option<PathBuf>,
}

/// Current wall-clock time as Unix epoch milliseconds.
pub fn epoch_ms_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}
