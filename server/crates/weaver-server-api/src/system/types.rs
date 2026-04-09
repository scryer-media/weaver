use async_graphql::{Enum, SimpleObject};
use serde::{Deserialize, Serialize};
use weaver_server_core::jobs::handle::{DownloadBlockKind, DownloadBlockState};

use crate::history::types::{EventKind, encode_timeline_member_subject};
use crate::jobs::types::{GlobalQueueState, QueueSummary};
use crate::settings::types::IspBandwidthCapPeriodGql;

#[derive(Debug, Clone, SimpleObject)]
pub struct DirectoryBrowseEntry {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct DirectoryBrowseResult {
    pub current_path: String,
    pub parent_path: Option<String>,
    pub entries: Vec<DirectoryBrowseEntry>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ServiceLogsPayload {
    pub lines: Vec<String>,
    pub count: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum DownloadBlockKindGql {
    None,
    ManualPause,
    Scheduled,
    IspCap,
}

impl From<DownloadBlockKind> for DownloadBlockKindGql {
    fn from(value: DownloadBlockKind) -> Self {
        match value {
            DownloadBlockKind::None => Self::None,
            DownloadBlockKind::ManualPause => Self::ManualPause,
            DownloadBlockKind::Scheduled => Self::Scheduled,
            DownloadBlockKind::IspCap => Self::IspCap,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SimpleObject)]
pub struct DownloadBlock {
    pub kind: DownloadBlockKindGql,
    pub cap_enabled: bool,
    pub period: Option<IspBandwidthCapPeriodGql>,
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub remaining_bytes: u64,
    pub reserved_bytes: u64,
    pub window_starts_at_epoch_ms: Option<f64>,
    pub window_ends_at_epoch_ms: Option<f64>,
    pub timezone_name: String,
    pub scheduled_speed_limit: u64,
}

impl From<&DownloadBlockState> for DownloadBlock {
    fn from(value: &DownloadBlockState) -> Self {
        Self {
            kind: value.kind.into(),
            cap_enabled: value.cap_enabled,
            period: value.period.map(Into::into),
            used_bytes: value.used_bytes,
            limit_bytes: value.limit_bytes,
            remaining_bytes: value.remaining_bytes,
            reserved_bytes: value.reserved_bytes,
            window_starts_at_epoch_ms: value.window_starts_at_epoch_ms,
            window_ends_at_epoch_ms: value.window_ends_at_epoch_ms,
            timezone_name: value.timezone_name.clone(),
            scheduled_speed_limit: value.scheduled_speed_limit,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct Metrics {
    pub bytes_downloaded: u64,
    pub bytes_decoded: u64,
    pub bytes_committed: u64,
    pub download_queue_depth: u32,
    pub decode_pending: u32,
    pub commit_pending: u32,
    pub write_buffered_bytes: u64,
    pub write_buffered_segments: u32,
    pub direct_write_evictions: u64,
    pub segments_downloaded: u64,
    pub segments_decoded: u64,
    pub segments_committed: u64,
    pub articles_not_found: u64,
    pub decode_errors: u64,
    pub verify_active: u32,
    pub repair_active: u32,
    pub extract_active: u32,
    pub disk_write_latency_us: u64,
    pub segments_retried: u64,
    pub segments_failed_permanent: u64,
    pub current_download_speed: u64,
    pub crc_errors: u64,
    pub recovery_queue_depth: u32,
    pub articles_per_sec: f64,
    pub decode_rate_mbps: f64,
}

impl From<&weaver_server_core::MetricsSnapshot> for Metrics {
    fn from(m: &weaver_server_core::MetricsSnapshot) -> Self {
        Self {
            bytes_downloaded: m.bytes_downloaded,
            bytes_decoded: m.bytes_decoded,
            bytes_committed: m.bytes_committed,
            download_queue_depth: m.download_queue_depth as u32,
            decode_pending: m.decode_pending as u32,
            commit_pending: m.commit_pending as u32,
            write_buffered_bytes: m.write_buffered_bytes,
            write_buffered_segments: m.write_buffered_segments as u32,
            direct_write_evictions: m.direct_write_evictions,
            segments_downloaded: m.segments_downloaded,
            segments_decoded: m.segments_decoded,
            segments_committed: m.segments_committed,
            articles_not_found: m.articles_not_found,
            decode_errors: m.decode_errors,
            verify_active: m.verify_active as u32,
            repair_active: m.repair_active as u32,
            extract_active: m.extract_active as u32,
            disk_write_latency_us: m.disk_write_latency_us,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, SimpleObject)]
pub struct MetricLabel {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct MetricSeries {
    pub metric: String,
    pub labels: Vec<MetricLabel>,
    pub values: Vec<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct MetricsHistoryResult {
    pub timestamps: Vec<f64>,
    pub series: Vec<MetricSeries>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PipelineEventGql {
    pub kind: EventKind,
    pub job_id: Option<u64>,
    pub file_id: Option<String>,
    pub message: String,
}

impl From<&weaver_server_core::events::model::PipelineEvent> for PipelineEventGql {
    fn from(event: &weaver_server_core::events::model::PipelineEvent) -> Self {
        use weaver_server_core::events::model::PipelineEvent;

        match event {
            PipelineEvent::JobCreated {
                job_id,
                name,
                total_files,
                total_bytes,
            } => Self {
                kind: EventKind::JobCreated,
                job_id: Some(job_id.0),
                file_id: None,
                message: format!("{name}: {total_files} files, {total_bytes} bytes"),
            },
            PipelineEvent::JobPaused { job_id } => Self {
                kind: EventKind::JobPaused,
                job_id: Some(job_id.0),
                file_id: None,
                message: "paused".into(),
            },
            PipelineEvent::JobResumed { job_id } => Self {
                kind: EventKind::JobResumed,
                job_id: Some(job_id.0),
                file_id: None,
                message: "resumed".into(),
            },
            PipelineEvent::JobCancelled { job_id } => Self {
                kind: EventKind::JobCancelled,
                job_id: Some(job_id.0),
                file_id: None,
                message: "cancelled".into(),
            },
            PipelineEvent::JobCompleted { job_id } => Self {
                kind: EventKind::JobCompleted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "completed".into(),
            },
            PipelineEvent::JobFailed { job_id, error } => Self {
                kind: EventKind::JobFailed,
                job_id: Some(job_id.0),
                file_id: None,
                message: error.clone(),
            },
            PipelineEvent::DownloadStarted { job_id } => Self {
                kind: EventKind::DownloadStarted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "download started".into(),
            },
            PipelineEvent::DownloadFinished { job_id } => Self {
                kind: EventKind::DownloadFinished,
                job_id: Some(job_id.0),
                file_id: None,
                message: "download finished".into(),
            },
            PipelineEvent::ArticleDownloaded {
                segment_id,
                raw_size,
            } => Self {
                kind: EventKind::ArticleDownloaded,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(segment_id.file_id.to_string()),
                message: format!("{raw_size} bytes"),
            },
            PipelineEvent::ArticleNotFound { segment_id } => Self {
                kind: EventKind::ArticleNotFound,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(segment_id.file_id.to_string()),
                message: format!("segment {} not found", segment_id.segment_number),
            },
            PipelineEvent::SegmentDecoded {
                segment_id,
                decoded_size,
                crc_valid,
                ..
            } => Self {
                kind: EventKind::SegmentDecoded,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(segment_id.file_id.to_string()),
                message: format!("{decoded_size} bytes, crc_valid={crc_valid}"),
            },
            PipelineEvent::SegmentCommitted { segment_id } => Self {
                kind: EventKind::SegmentCommitted,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(segment_id.file_id.to_string()),
                message: "committed".into(),
            },
            PipelineEvent::FileComplete {
                file_id,
                filename,
                total_bytes,
            } => Self {
                kind: EventKind::FileComplete,
                job_id: Some(file_id.job_id.0),
                file_id: Some(file_id.to_string()),
                message: format!("{filename}: {total_bytes} bytes"),
            },
            PipelineEvent::FileMissing {
                file_id,
                filename,
                missing_segments,
            } => Self {
                kind: EventKind::FileMissing,
                job_id: Some(file_id.job_id.0),
                file_id: Some(file_id.to_string()),
                message: format!("{filename}: {missing_segments} segments missing"),
            },
            PipelineEvent::VerificationStarted { file_id } => Self {
                kind: EventKind::VerificationStarted,
                job_id: Some(file_id.job_id.0),
                file_id: Some(file_id.to_string()),
                message: "verification started".into(),
            },
            PipelineEvent::VerificationComplete { file_id, status } => Self {
                kind: EventKind::VerificationComplete,
                job_id: Some(file_id.job_id.0),
                file_id: Some(file_id.to_string()),
                message: format!("{status:?}"),
            },
            PipelineEvent::JobVerificationStarted { job_id } => Self {
                kind: EventKind::JobVerificationStarted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "verification started".into(),
            },
            PipelineEvent::JobVerificationComplete { job_id, passed } => Self {
                kind: EventKind::JobVerificationComplete,
                job_id: Some(job_id.0),
                file_id: None,
                message: if *passed {
                    "verification passed".into()
                } else {
                    "verification found damage".into()
                },
            },
            PipelineEvent::RepairStarted { job_id } => Self {
                kind: EventKind::RepairStarted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "repair started".into(),
            },
            PipelineEvent::RepairComplete {
                job_id,
                slices_repaired,
            } => Self {
                kind: EventKind::RepairComplete,
                job_id: Some(job_id.0),
                file_id: None,
                message: format!("{slices_repaired} slices repaired"),
            },
            PipelineEvent::RepairFailed { job_id, error } => Self {
                kind: EventKind::RepairFailed,
                job_id: Some(job_id.0),
                file_id: None,
                message: error.clone(),
            },
            PipelineEvent::ExtractionReady { job_id } => Self {
                kind: EventKind::ExtractionReady,
                job_id: Some(job_id.0),
                file_id: None,
                message: "extraction ready".into(),
            },
            PipelineEvent::ExtractionMemberStarted {
                job_id,
                set_name,
                member,
            } => Self {
                kind: EventKind::ExtractionMemberStarted,
                job_id: Some(job_id.0),
                file_id: encode_timeline_member_subject(set_name, member, None),
                message: format!("{set_name}: {member}"),
            },
            PipelineEvent::ExtractionMemberWaitingStarted {
                job_id,
                set_name,
                member,
                volume_index,
            } => Self {
                kind: EventKind::ExtractionMemberWaitingStarted,
                job_id: Some(job_id.0),
                file_id: encode_timeline_member_subject(set_name, member, Some(*volume_index)),
                message: format!("{set_name}: {member} waiting for volume {volume_index}"),
            },
            PipelineEvent::ExtractionMemberWaitingFinished {
                job_id,
                set_name,
                member,
                volume_index,
            } => Self {
                kind: EventKind::ExtractionMemberWaitingFinished,
                job_id: Some(job_id.0),
                file_id: encode_timeline_member_subject(set_name, member, Some(*volume_index)),
                message: format!("{set_name}: {member} resumed with volume {volume_index}"),
            },
            PipelineEvent::ExtractionMemberAppendStarted {
                job_id,
                set_name,
                member,
            } => Self {
                kind: EventKind::ExtractionMemberAppendStarted,
                job_id: Some(job_id.0),
                file_id: encode_timeline_member_subject(set_name, member, None),
                message: format!("{set_name}: {member}"),
            },
            PipelineEvent::ExtractionMemberAppendFinished {
                job_id,
                set_name,
                member,
            } => Self {
                kind: EventKind::ExtractionMemberAppendFinished,
                job_id: Some(job_id.0),
                file_id: encode_timeline_member_subject(set_name, member, None),
                message: format!("{set_name}: {member}"),
            },
            PipelineEvent::ExtractionProgress {
                job_id,
                member,
                bytes_written,
                total_bytes,
            } => Self {
                kind: EventKind::ExtractionProgress,
                job_id: Some(job_id.0),
                file_id: None,
                message: format!("{member}: {bytes_written}/{total_bytes}"),
            },
            PipelineEvent::ExtractionComplete { job_id } => Self {
                kind: EventKind::ExtractionComplete,
                job_id: Some(job_id.0),
                file_id: None,
                message: "extraction complete".into(),
            },
            PipelineEvent::ExtractionMemberFinished {
                job_id,
                set_name,
                member,
            } => Self {
                kind: EventKind::ExtractionMemberFinished,
                job_id: Some(job_id.0),
                file_id: encode_timeline_member_subject(set_name, member, None),
                message: format!("{set_name}: {member}"),
            },
            PipelineEvent::ExtractionMemberFailed {
                job_id,
                set_name,
                member,
                error,
            } => Self {
                kind: EventKind::ExtractionMemberFailed,
                job_id: Some(job_id.0),
                file_id: encode_timeline_member_subject(set_name, member, None),
                message: format!("{set_name}: {member} - {error}"),
            },
            PipelineEvent::ExtractionFailed { job_id, error } => Self {
                kind: EventKind::ExtractionFailed,
                job_id: Some(job_id.0),
                file_id: None,
                message: error.clone(),
            },
            PipelineEvent::MoveToCompleteStarted { job_id } => Self {
                kind: EventKind::MoveToCompleteStarted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "move to complete started".into(),
            },
            PipelineEvent::MoveToCompleteFinished { job_id } => Self {
                kind: EventKind::MoveToCompleteFinished,
                job_id: Some(job_id.0),
                file_id: None,
                message: "move to complete finished".into(),
            },
            PipelineEvent::SegmentRetryScheduled {
                segment_id,
                attempt,
                delay_secs,
            } => Self {
                kind: EventKind::SegmentRetryScheduled,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(segment_id.file_id.to_string()),
                message: format!("retry attempt {attempt}, backoff {delay_secs:.1}s"),
            },
            PipelineEvent::SegmentFailedPermanent { segment_id, error } => Self {
                kind: EventKind::SegmentFailedPermanent,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(segment_id.file_id.to_string()),
                message: error.clone(),
            },
            PipelineEvent::GlobalPaused => Self {
                kind: EventKind::GlobalPaused,
                job_id: None,
                file_id: None,
                message: "all downloads paused".into(),
            },
            PipelineEvent::GlobalResumed => Self {
                kind: EventKind::GlobalResumed,
                job_id: None,
                file_id: None,
                message: "all downloads resumed".into(),
            },
            _ => Self {
                kind: EventKind::SegmentCommitted,
                job_id: None,
                file_id: None,
                message: format!("{event:?}"),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct SystemStatus {
    pub version: String,
    pub global_state: GlobalQueueState,
    pub summary: QueueSummary,
}
