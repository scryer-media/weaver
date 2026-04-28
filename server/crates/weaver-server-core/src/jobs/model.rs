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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadState {
    Queued,
    Downloading,
    /// Health probe in progress — checking article availability.
    Checking,
    Complete,
    Failed,
}

impl DownloadState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Checking => "checking",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "downloading" => Some(Self::Downloading),
            "checking" => Some(Self::Checking),
            "complete" => Some(Self::Complete),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostState {
    Idle,
    QueuedRepair,
    Repairing,
    QueuedExtract,
    Extracting,
    WaitingForVolumes,
    AwaitingRepair,
    Verifying,
    Finalizing,
    Completed,
    Failed,
}

impl PostState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::QueuedRepair => "queued_repair",
            Self::Repairing => "repairing",
            Self::QueuedExtract => "queued_extract",
            Self::Extracting => "extracting",
            Self::WaitingForVolumes => "waiting_for_volumes",
            Self::AwaitingRepair => "awaiting_repair",
            Self::Verifying => "verifying",
            Self::Finalizing => "finalizing",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "idle" => Some(Self::Idle),
            "queued_repair" => Some(Self::QueuedRepair),
            "repairing" => Some(Self::Repairing),
            "queued_extract" => Some(Self::QueuedExtract),
            "extracting" => Some(Self::Extracting),
            "waiting_for_volumes" => Some(Self::WaitingForVolumes),
            "awaiting_repair" => Some(Self::AwaitingRepair),
            "verifying" => Some(Self::Verifying),
            "finalizing" => Some(Self::Finalizing),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunState {
    Active,
    Paused,
}

impl RunState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "paused" => Some(Self::Paused),
            _ => None,
        }
    }
}

pub fn derive_legacy_job_status(
    download_state: DownloadState,
    post_state: PostState,
    run_state: RunState,
    failure_error: Option<&str>,
) -> JobStatus {
    if matches!(run_state, RunState::Paused) {
        return JobStatus::Paused;
    }

    if matches!(download_state, DownloadState::Failed) || matches!(post_state, PostState::Failed) {
        return JobStatus::Failed {
            error: failure_error.unwrap_or("unknown error").to_string(),
        };
    }

    if matches!(post_state, PostState::Completed)
        && matches!(download_state, DownloadState::Complete)
    {
        return JobStatus::Complete;
    }

    match post_state {
        PostState::Finalizing => JobStatus::Moving,
        PostState::Repairing => JobStatus::Repairing,
        PostState::QueuedRepair => JobStatus::QueuedRepair,
        PostState::Verifying | PostState::AwaitingRepair => JobStatus::Verifying,
        PostState::Extracting => JobStatus::Extracting,
        PostState::QueuedExtract => JobStatus::QueuedExtract,
        PostState::Idle
        | PostState::WaitingForVolumes
        | PostState::Completed
        | PostState::Failed => match download_state {
            DownloadState::Checking => JobStatus::Checking,
            DownloadState::Queued => JobStatus::Queued,
            DownloadState::Downloading | DownloadState::Complete | DownloadState::Failed => {
                JobStatus::Downloading
            }
        },
    }
}

pub fn job_status_from_persisted_str(status: &str, error: Option<&str>) -> JobStatus {
    match status {
        "queued" => JobStatus::Queued,
        "downloading" => JobStatus::Downloading,
        "checking" => JobStatus::Checking,
        "verifying" => JobStatus::Verifying,
        "queued_repair" => JobStatus::QueuedRepair,
        "repairing" => JobStatus::Repairing,
        "queued_extract" => JobStatus::QueuedExtract,
        "extracting" => JobStatus::Extracting,
        "moving" => JobStatus::Moving,
        "complete" => JobStatus::Complete,
        "failed" => JobStatus::Failed {
            error: error.unwrap_or("unknown error").to_string(),
        },
        "paused" => JobStatus::Paused,
        "cancelled" => JobStatus::Failed {
            error: "cancelled".to_string(),
        },
        other => JobStatus::Failed {
            error: format!("unknown status: {other}"),
        },
    }
}

pub fn runtime_lanes_from_status_snapshot(
    status: &JobStatus,
) -> (DownloadState, PostState, RunState) {
    match status {
        JobStatus::Queued => (DownloadState::Queued, PostState::Idle, RunState::Active),
        JobStatus::Downloading => (
            DownloadState::Downloading,
            PostState::Idle,
            RunState::Active,
        ),
        JobStatus::Checking => (DownloadState::Checking, PostState::Idle, RunState::Active),
        JobStatus::Verifying => (
            DownloadState::Complete,
            PostState::Verifying,
            RunState::Active,
        ),
        JobStatus::QueuedRepair => (
            DownloadState::Complete,
            PostState::QueuedRepair,
            RunState::Active,
        ),
        JobStatus::Repairing => (
            DownloadState::Complete,
            PostState::Repairing,
            RunState::Active,
        ),
        JobStatus::QueuedExtract => (
            DownloadState::Complete,
            PostState::QueuedExtract,
            RunState::Active,
        ),
        JobStatus::Extracting => (
            DownloadState::Complete,
            PostState::Extracting,
            RunState::Active,
        ),
        JobStatus::Moving => (
            DownloadState::Complete,
            PostState::Finalizing,
            RunState::Active,
        ),
        JobStatus::Complete => (
            DownloadState::Complete,
            PostState::Completed,
            RunState::Active,
        ),
        JobStatus::Failed { .. } => (DownloadState::Failed, PostState::Failed, RunState::Active),
        JobStatus::Paused => (
            DownloadState::Downloading,
            PostState::Idle,
            RunState::Paused,
        ),
    }
}

/// Internal state for a running job.
pub struct JobState {
    pub job_id: JobId,
    pub spec: JobSpec,
    /// Runtime status used by the pipeline choreography. The lane fields below
    /// are compatibility projections and must not drive core lifecycle logic.
    pub status: JobStatus,
    pub download_state: DownloadState,
    pub post_state: PostState,
    pub run_state: RunState,
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
    /// Compatibility lane targets retained for v0.2.9-era persisted rows.
    pub paused_resume_download_state: Option<DownloadState>,
    pub paused_resume_post_state: Option<PostState>,
    /// Last terminal error associated with this job.
    pub failure_error: Option<String>,
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
    /// Probe activation counter used to rotate sampled segments across rounds.
    pub health_probe_round: u32,
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

impl JobState {
    pub fn refresh_runtime_lanes_from_status(&mut self) {
        let (download_state, post_state, run_state) =
            runtime_lanes_from_status_snapshot(&self.status);
        self.download_state = download_state;
        self.post_state = post_state;
        self.run_state = run_state;
    }

    pub fn refresh_legacy_status(&mut self) {
        self.status = derive_legacy_job_status(
            self.download_state,
            self.post_state,
            self.run_state,
            self.failure_error.as_deref(),
        );
    }

    pub fn set_failure(&mut self, error: impl Into<String>) {
        let error = error.into();
        self.failure_error = Some(error.clone());
        self.status = JobStatus::Failed { error };
        self.refresh_runtime_lanes_from_status();
    }
}

/// Current wall-clock time as Unix epoch milliseconds.
pub fn epoch_ms_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_status_parser_supports_moving() {
        assert_eq!(
            job_status_from_persisted_str("moving", None),
            JobStatus::Moving
        );
    }

    #[test]
    fn legacy_status_derivation_keeps_waiting_jobs_as_downloading() {
        assert_eq!(
            derive_legacy_job_status(
                DownloadState::Downloading,
                PostState::WaitingForVolumes,
                RunState::Active,
                None,
            ),
            JobStatus::Downloading
        );
    }

    #[test]
    fn runtime_lane_snapshot_maps_repairing_and_moving() {
        assert_eq!(
            runtime_lanes_from_status_snapshot(&JobStatus::Repairing),
            (
                DownloadState::Complete,
                PostState::Repairing,
                RunState::Active,
            )
        );
        assert_eq!(
            runtime_lanes_from_status_snapshot(&JobStatus::Moving),
            (
                DownloadState::Complete,
                PostState::Finalizing,
                RunState::Active,
            )
        );
    }
}
