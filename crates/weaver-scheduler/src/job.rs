use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use weaver_assembly::JobAssembly;
use weaver_core::classify::FileRole;
use weaver_core::id::JobId;

use crate::download_queue::{DownloadQueue, DownloadWork};

/// Status of a job in the scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    Queued,
    Downloading,
    /// Health probe in progress — checking article availability.
    Checking,
    Verifying,
    Repairing,
    Extracting,
    Complete,
    Failed { error: String },
    Paused,
}

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

/// Internal state for a running job.
pub struct JobState {
    pub job_id: JobId,
    pub spec: JobSpec,
    pub status: JobStatus,
    pub assembly: JobAssembly,
    pub created_at: std::time::Instant,
    /// Wall-clock creation time (Unix epoch milliseconds).
    pub created_at_epoch_ms: f64,
    /// Per-job working directory (subdirectory under intermediate_dir).
    /// All download/decode/verify/repair/extract I/O happens here.
    pub working_dir: PathBuf,
    /// Bytes downloaded for this specific job.
    pub downloaded_bytes: u64,
    /// Bytes from segments that are permanently lost (430 / max retries exhausted).
    pub failed_bytes: u64,
    /// Whether health probes have been dispatched for this job.
    pub health_probing: bool,
    /// Segments pulled from queues while health probe runs. Restored on
    /// probe pass, dropped on probe fail.
    pub held_segments: Vec<DownloadWork>,
    /// Primary download queue (data files, PAR2 index, RAR volumes).
    pub download_queue: DownloadQueue,
    /// Recovery download queue (PAR2 repair blocks, spare bandwidth).
    pub recovery_queue: DownloadQueue,
}

/// Current wall-clock time as Unix epoch milliseconds.
pub fn epoch_ms_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}
