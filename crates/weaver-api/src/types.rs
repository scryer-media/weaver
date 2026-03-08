use async_graphql::{Enum, InputObject, SimpleObject};

/// GraphQL representation of a configured NNTP server.
#[derive(Debug, Clone, SimpleObject)]
pub struct Server {
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    // NOTE: password intentionally omitted from output type
    pub connections: u16,
    pub active: bool,
    pub supports_pipelining: bool,
}

impl From<&weaver_core::config::ServerConfig> for Server {
    fn from(s: &weaver_core::config::ServerConfig) -> Self {
        Server {
            id: s.id,
            host: s.host.clone(),
            port: s.port,
            tls: s.tls,
            username: s.username.clone(),
            connections: s.connections,
            active: s.active,
            supports_pipelining: s.supports_pipelining,
        }
    }
}

/// Input for creating or updating a server.
#[derive(Debug, InputObject)]
pub struct ServerInput {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connections: u16,
    #[graphql(default = true)]
    pub active: bool,
}

/// Result of testing a server connection.
#[derive(Debug, Clone, SimpleObject)]
pub struct TestConnectionResult {
    pub success: bool,
    pub message: String,
    pub latency_ms: Option<u64>,
    pub supports_pipelining: bool,
}

/// GraphQL representation of general settings.
#[derive(Debug, Clone, SimpleObject)]
pub struct GeneralSettings {
    pub data_dir: String,
    pub intermediate_dir: String,
    pub complete_dir: String,
    pub cleanup_after_extract: bool,
    pub max_download_speed: u64,
    pub max_retries: u32,
}

/// Input for updating general settings.
#[derive(Debug, InputObject)]
pub struct GeneralSettingsInput {
    pub intermediate_dir: Option<String>,
    pub complete_dir: Option<String>,
    pub cleanup_after_extract: Option<bool>,
    pub max_download_speed: Option<u64>,
    pub max_retries: Option<u32>,
}

/// A key-value metadata entry attached to a job.
#[derive(Debug, Clone, SimpleObject)]
pub struct MetadataEntry {
    pub key: String,
    pub value: String,
}

/// Input for attaching metadata to a job submission.
#[derive(Debug, InputObject)]
pub struct MetadataInput {
    pub key: String,
    pub value: String,
}

/// GraphQL representation of a job.
#[derive(Debug, Clone, SimpleObject)]
pub struct Job {
    pub id: u64,
    pub name: String,
    pub status: JobStatusGql,
    pub progress: f64,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    /// Bytes from segments that are permanently lost (430 / max retries).
    pub failed_bytes: u64,
    /// Job health 0-1000 (1000 = perfect). Drops as articles fail.
    pub health: u32,
    pub has_password: bool,
    pub category: Option<String>,
    pub metadata: Vec<MetadataEntry>,
    pub output_dir: Option<String>,
}

/// GraphQL-friendly job status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum JobStatusGql {
    Queued,
    Downloading,
    Checking,
    Verifying,
    Repairing,
    Extracting,
    Complete,
    Failed,
    Paused,
}

/// GraphQL representation of pipeline metrics.
#[derive(Debug, Clone, Default, SimpleObject)]
pub struct Metrics {
    pub bytes_downloaded: u64,
    pub bytes_decoded: u64,
    pub bytes_committed: u64,
    pub download_queue_depth: u32,
    pub decode_pending: u32,
    pub commit_pending: u32,
    pub segments_downloaded: u64,
    pub segments_decoded: u64,
    pub segments_committed: u64,
    pub articles_not_found: u64,
    pub decode_errors: u64,
    pub verify_active: u32,
    pub repair_active: u32,
    pub extract_active: u32,
    pub segments_retried: u64,
    pub segments_failed_permanent: u64,
    pub current_download_speed: u64,
    pub crc_errors: u64,
    pub recovery_queue_depth: u32,
    pub articles_per_sec: f64,
    pub decode_rate_mbps: f64,
}

/// GraphQL representation of a pipeline event (real-time subscription).
#[derive(Debug, Clone, SimpleObject)]
pub struct PipelineEventGql {
    pub kind: EventKind,
    pub job_id: Option<u64>,
    pub file_id: Option<String>,
    pub message: String,
}

/// GraphQL representation of a persisted job event (historical query).
#[derive(Debug, Clone, SimpleObject)]
pub struct JobEvent {
    pub kind: EventKind,
    pub job_id: u64,
    pub file_id: Option<String>,
    pub message: String,
    /// Unix timestamp in milliseconds.
    pub timestamp: f64,
}

/// Event categories for subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum EventKind {
    JobCreated,
    JobPaused,
    JobResumed,
    JobCompleted,
    JobFailed,
    ArticleDownloaded,
    ArticleNotFound,
    SegmentDecoded,
    SegmentCommitted,
    FileComplete,
    FileMissing,
    VerificationStarted,
    VerificationComplete,
    RepairStarted,
    RepairComplete,
    RepairFailed,
    ExtractionReady,
    ExtractionProgress,
    ExtractionComplete,
    ExtractionFailed,
    SegmentRetryScheduled,
    SegmentFailedPermanent,
    GlobalPaused,
    GlobalResumed,
}

impl std::str::FromStr for EventKind {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "JobCreated" => Ok(Self::JobCreated),
            "JobPaused" => Ok(Self::JobPaused),
            "JobResumed" => Ok(Self::JobResumed),
            "JobCompleted" => Ok(Self::JobCompleted),
            "JobFailed" => Ok(Self::JobFailed),
            "ArticleDownloaded" => Ok(Self::ArticleDownloaded),
            "ArticleNotFound" => Ok(Self::ArticleNotFound),
            "SegmentDecoded" => Ok(Self::SegmentDecoded),
            "SegmentCommitted" => Ok(Self::SegmentCommitted),
            "FileComplete" => Ok(Self::FileComplete),
            "FileMissing" => Ok(Self::FileMissing),
            "VerificationStarted" => Ok(Self::VerificationStarted),
            "VerificationComplete" => Ok(Self::VerificationComplete),
            "RepairStarted" => Ok(Self::RepairStarted),
            "RepairComplete" => Ok(Self::RepairComplete),
            "RepairFailed" => Ok(Self::RepairFailed),
            "ExtractionReady" => Ok(Self::ExtractionReady),
            "ExtractionProgress" => Ok(Self::ExtractionProgress),
            "ExtractionComplete" => Ok(Self::ExtractionComplete),
            "ExtractionFailed" => Ok(Self::ExtractionFailed),
            "SegmentRetryScheduled" => Ok(Self::SegmentRetryScheduled),
            "SegmentFailedPermanent" => Ok(Self::SegmentFailedPermanent),
            "GlobalPaused" => Ok(Self::GlobalPaused),
            "GlobalResumed" => Ok(Self::GlobalResumed),
            _ => Err(()),
        }
    }
}

// --- Conversion from domain types ---

impl From<&weaver_scheduler::JobInfo> for Job {
    fn from(info: &weaver_scheduler::JobInfo) -> Self {
        Job {
            id: info.job_id.0,
            name: info.name.clone(),
            status: JobStatusGql::from(&info.status),
            progress: info.progress,
            total_bytes: info.total_bytes,
            downloaded_bytes: info.downloaded_bytes,
            failed_bytes: info.failed_bytes,
            health: info.health,
            has_password: info.password.is_some(),
            category: info.category.clone(),
            metadata: info
                .metadata
                .iter()
                .map(|(k, v)| MetadataEntry {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
            output_dir: info.output_dir.clone(),
        }
    }
}

impl From<&weaver_scheduler::JobStatus> for JobStatusGql {
    fn from(status: &weaver_scheduler::JobStatus) -> Self {
        match status {
            weaver_scheduler::JobStatus::Queued => JobStatusGql::Queued,
            weaver_scheduler::JobStatus::Downloading => JobStatusGql::Downloading,
            weaver_scheduler::JobStatus::Checking => JobStatusGql::Checking,
            weaver_scheduler::JobStatus::Verifying => JobStatusGql::Verifying,
            weaver_scheduler::JobStatus::Repairing => JobStatusGql::Repairing,
            weaver_scheduler::JobStatus::Extracting => JobStatusGql::Extracting,
            weaver_scheduler::JobStatus::Complete => JobStatusGql::Complete,
            weaver_scheduler::JobStatus::Failed { .. } => JobStatusGql::Failed,
            weaver_scheduler::JobStatus::Paused => JobStatusGql::Paused,
        }
    }
}

impl From<&weaver_scheduler::MetricsSnapshot> for Metrics {
    fn from(m: &weaver_scheduler::MetricsSnapshot) -> Self {
        Metrics {
            bytes_downloaded: m.bytes_downloaded,
            bytes_decoded: m.bytes_decoded,
            bytes_committed: m.bytes_committed,
            download_queue_depth: m.download_queue_depth as u32,
            decode_pending: m.decode_pending as u32,
            commit_pending: m.commit_pending as u32,
            segments_downloaded: m.segments_downloaded,
            segments_decoded: m.segments_decoded,
            segments_committed: m.segments_committed,
            articles_not_found: m.articles_not_found,
            decode_errors: m.decode_errors,
            verify_active: m.verify_active as u32,
            repair_active: m.repair_active as u32,
            extract_active: m.extract_active as u32,
            segments_retried: m.segments_retried,
            segments_failed_permanent: m.segments_failed_permanent,
            current_download_speed: m.current_download_speed,
            crc_errors: m.crc_errors,
            recovery_queue_depth: m.recovery_queue_depth as u32,
            articles_per_sec: m.articles_per_sec,
            decode_rate_mbps: m.decode_rate_mbps,
        }
    }
}

impl From<&weaver_core::event::PipelineEvent> for PipelineEventGql {
    fn from(event: &weaver_core::event::PipelineEvent) -> Self {
        use weaver_core::event::PipelineEvent;

        match event {
            PipelineEvent::JobCreated { job_id, name, total_files, total_bytes } => PipelineEventGql {
                kind: EventKind::JobCreated,
                job_id: Some(job_id.0),
                file_id: None,
                message: format!("{name}: {total_files} files, {total_bytes} bytes"),
            },
            PipelineEvent::JobPaused { job_id } => PipelineEventGql {
                kind: EventKind::JobPaused,
                job_id: Some(job_id.0),
                file_id: None,
                message: "paused".into(),
            },
            PipelineEvent::JobResumed { job_id } => PipelineEventGql {
                kind: EventKind::JobResumed,
                job_id: Some(job_id.0),
                file_id: None,
                message: "resumed".into(),
            },
            PipelineEvent::JobCompleted { job_id } => PipelineEventGql {
                kind: EventKind::JobCompleted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "completed".into(),
            },
            PipelineEvent::JobFailed { job_id, error } => PipelineEventGql {
                kind: EventKind::JobFailed,
                job_id: Some(job_id.0),
                file_id: None,
                message: error.clone(),
            },
            PipelineEvent::ArticleDownloaded { segment_id, raw_size } => PipelineEventGql {
                kind: EventKind::ArticleDownloaded,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(format!("{}", segment_id.file_id)),
                message: format!("{raw_size} bytes"),
            },
            PipelineEvent::ArticleNotFound { segment_id } => PipelineEventGql {
                kind: EventKind::ArticleNotFound,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(format!("{}", segment_id.file_id)),
                message: format!("segment {} not found", segment_id.segment_number),
            },
            PipelineEvent::SegmentDecoded { segment_id, decoded_size, crc_valid, .. } => PipelineEventGql {
                kind: EventKind::SegmentDecoded,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(format!("{}", segment_id.file_id)),
                message: format!("{decoded_size} bytes, crc_valid={crc_valid}"),
            },
            PipelineEvent::SegmentCommitted { segment_id } => PipelineEventGql {
                kind: EventKind::SegmentCommitted,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(format!("{}", segment_id.file_id)),
                message: "committed".into(),
            },
            PipelineEvent::FileComplete { file_id, filename, total_bytes } => PipelineEventGql {
                kind: EventKind::FileComplete,
                job_id: Some(file_id.job_id.0),
                file_id: Some(format!("{file_id}")),
                message: format!("{filename}: {total_bytes} bytes"),
            },
            PipelineEvent::FileMissing { file_id, filename, missing_segments } => PipelineEventGql {
                kind: EventKind::FileMissing,
                job_id: Some(file_id.job_id.0),
                file_id: Some(format!("{file_id}")),
                message: format!("{filename}: {missing_segments} segments missing"),
            },
            PipelineEvent::VerificationStarted { file_id } => PipelineEventGql {
                kind: EventKind::VerificationStarted,
                job_id: Some(file_id.job_id.0),
                file_id: Some(format!("{file_id}")),
                message: "verification started".into(),
            },
            PipelineEvent::VerificationComplete { file_id, status } => PipelineEventGql {
                kind: EventKind::VerificationComplete,
                job_id: Some(file_id.job_id.0),
                file_id: Some(format!("{file_id}")),
                message: format!("{status:?}"),
            },
            PipelineEvent::RepairStarted { job_id } => PipelineEventGql {
                kind: EventKind::RepairStarted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "repair started".into(),
            },
            PipelineEvent::RepairComplete { job_id, slices_repaired } => PipelineEventGql {
                kind: EventKind::RepairComplete,
                job_id: Some(job_id.0),
                file_id: None,
                message: format!("{slices_repaired} slices repaired"),
            },
            PipelineEvent::RepairFailed { job_id, error } => PipelineEventGql {
                kind: EventKind::RepairFailed,
                job_id: Some(job_id.0),
                file_id: None,
                message: error.clone(),
            },
            PipelineEvent::ExtractionReady { job_id } => PipelineEventGql {
                kind: EventKind::ExtractionReady,
                job_id: Some(job_id.0),
                file_id: None,
                message: "extraction ready".into(),
            },
            PipelineEvent::ExtractionProgress { job_id, member, bytes_written, total_bytes } => PipelineEventGql {
                kind: EventKind::ExtractionProgress,
                job_id: Some(job_id.0),
                file_id: None,
                message: format!("{member}: {bytes_written}/{total_bytes}"),
            },
            PipelineEvent::ExtractionComplete { job_id } => PipelineEventGql {
                kind: EventKind::ExtractionComplete,
                job_id: Some(job_id.0),
                file_id: None,
                message: "extraction complete".into(),
            },
            PipelineEvent::ExtractionFailed { job_id, error } => PipelineEventGql {
                kind: EventKind::ExtractionFailed,
                job_id: Some(job_id.0),
                file_id: None,
                message: error.clone(),
            },
            PipelineEvent::SegmentRetryScheduled { segment_id, attempt, delay_secs } => PipelineEventGql {
                kind: EventKind::SegmentRetryScheduled,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(format!("{}", segment_id.file_id)),
                message: format!("retry attempt {attempt}, backoff {delay_secs:.1}s"),
            },
            PipelineEvent::SegmentFailedPermanent { segment_id, error } => PipelineEventGql {
                kind: EventKind::SegmentFailedPermanent,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(format!("{}", segment_id.file_id)),
                message: error.clone(),
            },
            PipelineEvent::GlobalPaused => PipelineEventGql {
                kind: EventKind::GlobalPaused,
                job_id: None,
                file_id: None,
                message: "all downloads paused".into(),
            },
            PipelineEvent::GlobalResumed => PipelineEventGql {
                kind: EventKind::GlobalResumed,
                job_id: None,
                file_id: None,
                message: "all downloads resumed".into(),
            },
            // Events not directly surfaced to GraphQL get a generic mapping.
            _ => PipelineEventGql {
                kind: EventKind::SegmentCommitted,
                job_id: None,
                file_id: None,
                message: format!("{event:?}"),
            },
        }
    }
}
