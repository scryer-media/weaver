use async_graphql::{Enum, SimpleObject};
use serde::{Deserialize, Serialize};
use weaver_server_core::jobs::handle::{DownloadBlockKind, DownloadBlockState};
use weaver_server_core::operations::{
    DirectoryBrowseEntry as CoreDirectoryBrowseEntry,
    DirectoryBrowseListing as CoreDirectoryBrowseListing,
};

use crate::history::types::{
    DOWNLOAD_FINALIZATION_MARKER, EventKind, encode_timeline_member_subject,
};
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

impl From<CoreDirectoryBrowseEntry> for DirectoryBrowseEntry {
    fn from(value: CoreDirectoryBrowseEntry) -> Self {
        Self {
            name: value.name,
            path: value.path,
        }
    }
}

impl From<CoreDirectoryBrowseListing> for DirectoryBrowseResult {
    fn from(value: CoreDirectoryBrowseListing) -> Self {
        Self {
            current_path: value.current_path,
            parent_path: value.parent_path,
            entries: value.entries.into_iter().map(Into::into).collect(),
        }
    }
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
    ServerQuota,
}

impl From<DownloadBlockKind> for DownloadBlockKindGql {
    fn from(value: DownloadBlockKind) -> Self {
        match value {
            DownloadBlockKind::None => Self::None,
            DownloadBlockKind::ManualPause => Self::ManualPause,
            DownloadBlockKind::Scheduled => Self::Scheduled,
            DownloadBlockKind::IspCap => Self::IspCap,
            DownloadBlockKind::ServerQuota => Self::ServerQuota,
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
    pub active_downloads: u32,
    pub active_decodes: u32,
    pub decode_pending: u32,
    pub decode_pending_bytes: u64,
    pub decode_active_bytes: u64,
    pub commit_pending: u32,
    pub write_buffered_bytes: u64,
    pub write_buffered_segments: u32,
    pub direct_write_evictions: u64,
    pub decode_pressure_soft_limit_bytes: u64,
    pub decode_pressure_hard_limit_bytes: u64,
    pub write_pressure_soft_limit_bytes: u64,
    pub write_pressure_hard_limit_bytes: u64,
    pub download_pressure_state: DownloadPressureStateGql,
    pub download_pressure_reason: DownloadPressureReasonGql,
    pub download_pressure_stalls_total: u64,
    pub download_pressure_stall_duration_ms: u64,
    pub download_pressure_current_stall_ms: u64,
    pub hot_dispatch_job_id: u64,
    pub hot_dispatch_mode: String,
    pub hot_dispatch_underfill_ms: u64,
    pub hot_dispatch_lent_connections: u32,
    pub hot_dispatch_warmup_complete: bool,
    pub hot_dispatch_last_spillover_decision: String,
    pub hot_dispatch_spillover_blocked_warmup_total: u64,
    pub hot_dispatch_spillover_blocked_pressure_total: u64,
    pub hot_dispatch_spillover_blocked_near_cap_total: u64,
    pub hot_dispatch_spillover_blocked_hot_can_use_capacity_total: u64,
    pub hot_dispatch_spillover_blocked_best_mode_pending_total: u64,
    pub hot_dispatch_spillover_blocked_recent_expansion_helped_total: u64,
    pub hot_dispatch_spillover_blocked_cap_speed_total: u64,
    pub hot_dispatch_spillover_allowed_underfill_total: u64,
    pub hot_dispatch_spillover_allowed_measured_underfill_total: u64,
    pub hot_dispatch_spillover_reclaimed_total: u64,
    pub hot_dispatch_hot_speed_bps: u64,
    pub hot_dispatch_exclusive_peak_bps: u64,
    pub hot_dispatch_spillover_pre_speed_bps: u64,
    pub hot_dispatch_spillover_post_speed_bps: u64,
    pub hot_dispatch_spillover_active_loans: u32,
    pub hot_dispatch_spillover_reclaimed_speed_harm_total: u64,
    pub hot_dispatch_recent_expansion_improvement_pct: u64,
    pub hot_dispatch_best_mode_block_reason: u32,
    pub hot_dispatch_last_expansion_kind: u32,
    pub hot_dispatch_last_expansion_before_bps: u64,
    pub hot_dispatch_last_expansion_after_bps: u64,
    pub download_lanes_active: u32,
    pub download_lanes_sequential_active: u32,
    pub download_lanes_depth2_active: u32,
    pub download_lanes_depth4_active: u32,
    pub download_lanes_idle_active: u32,
    pub download_lanes_awaiting_work_active: u32,
    pub download_lanes_binding_server_active: u32,
    pub download_lanes_acquired_active: u32,
    pub download_lanes_issuing_active: u32,
    pub download_lanes_draining_active: u32,
    pub download_lanes_yield_after_batch_active: u32,
    pub download_lanes_parking_active: u32,
    pub download_lanes_recovering_active: u32,
    pub download_lane_parks_no_work_total: u64,
    pub download_lane_parks_spillover_speed_harm_total: u64,
    pub download_lane_parks_ip_replacement_retired_total: u64,
    pub download_lane_parks_error_total: u64,
    pub download_lane_lease_items_total: u64,
    pub download_lane_refill_granted_total: u64,
    pub download_lane_refill_parked_total: u64,
    pub download_pipeline_trial_success_total: u64,
    pub download_pipeline_trial_failure_total: u64,
    pub download_pipeline_proof_pass_total: u64,
    pub download_pipeline_cooldown_total: u64,
    pub download_pipeline_replay_items_total: u64,
    pub ip_replacement_trial_extra_connections: u32,
    pub ip_replacement_burst_active: bool,
    pub ip_replacement_over_max_connections: u32,
    pub ip_rtt_ewma_entries: u32,
    pub ip_rtt_ewma_slowest_ms: u64,
    pub ip_replacement_trials_started_total: u64,
    pub ip_replacement_trials_rejected_total: u64,
    pub ip_replacement_trials_accepted_total: u64,
    pub ip_replacement_trials_blocked_total: u64,
    pub ip_replacement_trials_acquire_failed_total: u64,
    pub ip_replacement_trials_same_ip_rejected_total: u64,
    pub ip_replacement_old_connections_retired_total: u64,
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
    pub download_failures_article_not_found: u64,
    pub download_failures_capacity_unavailable: u64,
    pub download_failures_transient: u64,
    pub download_failures_auth: u64,
    pub download_failures_permanent: u64,
    pub current_download_speed: u64,
    pub crc_errors: u64,
    pub recovery_queue_depth: u32,
    pub articles_per_sec: f64,
    pub decode_rate_mbps: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum DownloadPressureStateGql {
    #[default]
    Clear,
    Soft,
    Hard,
}

impl From<weaver_server_core::DownloadPressureState> for DownloadPressureStateGql {
    fn from(value: weaver_server_core::DownloadPressureState) -> Self {
        match value {
            weaver_server_core::DownloadPressureState::Clear => Self::Clear,
            weaver_server_core::DownloadPressureState::Soft => Self::Soft,
            weaver_server_core::DownloadPressureState::Hard => Self::Hard,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum DownloadPressureReasonGql {
    #[default]
    None,
    Decode,
    Write,
    DecodeAndWrite,
}

impl From<weaver_server_core::DownloadPressureReason> for DownloadPressureReasonGql {
    fn from(value: weaver_server_core::DownloadPressureReason) -> Self {
        match value {
            weaver_server_core::DownloadPressureReason::None => Self::None,
            weaver_server_core::DownloadPressureReason::Decode => Self::Decode,
            weaver_server_core::DownloadPressureReason::Write => Self::Write,
            weaver_server_core::DownloadPressureReason::DecodeAndWrite => Self::DecodeAndWrite,
        }
    }
}

impl From<&weaver_server_core::MetricsSnapshot> for Metrics {
    fn from(m: &weaver_server_core::MetricsSnapshot) -> Self {
        Self {
            bytes_downloaded: m.bytes_downloaded,
            bytes_decoded: m.bytes_decoded,
            bytes_committed: m.bytes_committed,
            download_queue_depth: m.download_queue_depth as u32,
            active_downloads: m.active_downloads as u32,
            active_decodes: m.active_decodes as u32,
            decode_pending: m.decode_pending as u32,
            decode_pending_bytes: m.decode_pending_bytes,
            decode_active_bytes: m.decode_active_bytes,
            commit_pending: m.commit_pending as u32,
            write_buffered_bytes: m.write_buffered_bytes,
            write_buffered_segments: m.write_buffered_segments as u32,
            direct_write_evictions: m.direct_write_evictions,
            decode_pressure_soft_limit_bytes: m.decode_pressure_soft_limit_bytes,
            decode_pressure_hard_limit_bytes: m.decode_pressure_hard_limit_bytes,
            write_pressure_soft_limit_bytes: m.write_pressure_soft_limit_bytes,
            write_pressure_hard_limit_bytes: m.write_pressure_hard_limit_bytes,
            download_pressure_state: m.download_pressure_state.into(),
            download_pressure_reason: m.download_pressure_reason.into(),
            download_pressure_stalls_total: m.download_pressure_stalls_total,
            download_pressure_stall_duration_ms: m.download_pressure_stall_duration_ms,
            download_pressure_current_stall_ms: m.download_pressure_current_stall_ms,
            hot_dispatch_job_id: m.hot_dispatch_job_id,
            hot_dispatch_mode: m.hot_dispatch_mode.as_str().to_string(),
            hot_dispatch_underfill_ms: m.hot_dispatch_underfill_ms,
            hot_dispatch_lent_connections: m.hot_dispatch_lent_connections as u32,
            hot_dispatch_warmup_complete: m.hot_dispatch_warmup_complete,
            hot_dispatch_last_spillover_decision: m
                .hot_dispatch_last_spillover_decision
                .as_str()
                .to_string(),
            hot_dispatch_spillover_blocked_warmup_total: m
                .hot_dispatch_spillover_blocked_warmup_total,
            hot_dispatch_spillover_blocked_pressure_total: m
                .hot_dispatch_spillover_blocked_pressure_total,
            hot_dispatch_spillover_blocked_near_cap_total: m
                .hot_dispatch_spillover_blocked_near_cap_total,
            hot_dispatch_spillover_blocked_hot_can_use_capacity_total: m
                .hot_dispatch_spillover_blocked_hot_can_use_capacity_total,
            hot_dispatch_spillover_blocked_best_mode_pending_total: m
                .hot_dispatch_spillover_blocked_best_mode_pending_total,
            hot_dispatch_spillover_blocked_recent_expansion_helped_total: m
                .hot_dispatch_spillover_blocked_recent_expansion_helped_total,
            hot_dispatch_spillover_blocked_cap_speed_total: m
                .hot_dispatch_spillover_blocked_cap_speed_total,
            hot_dispatch_spillover_allowed_underfill_total: m
                .hot_dispatch_spillover_allowed_underfill_total,
            hot_dispatch_spillover_allowed_measured_underfill_total: m
                .hot_dispatch_spillover_allowed_measured_underfill_total,
            hot_dispatch_spillover_reclaimed_total: m.hot_dispatch_spillover_reclaimed_total,
            hot_dispatch_hot_speed_bps: m.hot_dispatch_hot_speed_bps,
            hot_dispatch_exclusive_peak_bps: m.hot_dispatch_exclusive_peak_bps,
            hot_dispatch_spillover_pre_speed_bps: m.hot_dispatch_spillover_pre_speed_bps,
            hot_dispatch_spillover_post_speed_bps: m.hot_dispatch_spillover_post_speed_bps,
            hot_dispatch_spillover_active_loans: m.hot_dispatch_spillover_active_loans as u32,
            hot_dispatch_spillover_reclaimed_speed_harm_total: m
                .hot_dispatch_spillover_reclaimed_speed_harm_total,
            hot_dispatch_recent_expansion_improvement_pct: m
                .hot_dispatch_recent_expansion_improvement_pct,
            hot_dispatch_best_mode_block_reason: m.hot_dispatch_best_mode_block_reason as u32,
            hot_dispatch_last_expansion_kind: m.hot_dispatch_last_expansion_kind as u32,
            hot_dispatch_last_expansion_before_bps: m.hot_dispatch_last_expansion_before_bps,
            hot_dispatch_last_expansion_after_bps: m.hot_dispatch_last_expansion_after_bps,
            download_lanes_active: m.download_lanes_active as u32,
            download_lanes_sequential_active: m.download_lanes_sequential_active as u32,
            download_lanes_depth2_active: m.download_lanes_depth2_active as u32,
            download_lanes_depth4_active: m.download_lanes_depth4_active as u32,
            download_lanes_idle_active: m.download_lanes_idle_active as u32,
            download_lanes_awaiting_work_active: m.download_lanes_awaiting_work_active as u32,
            download_lanes_binding_server_active: m.download_lanes_binding_server_active as u32,
            download_lanes_acquired_active: m.download_lanes_acquired_active as u32,
            download_lanes_issuing_active: m.download_lanes_issuing_active as u32,
            download_lanes_draining_active: m.download_lanes_draining_active as u32,
            download_lanes_yield_after_batch_active: m.download_lanes_yield_after_batch_active
                as u32,
            download_lanes_parking_active: m.download_lanes_parking_active as u32,
            download_lanes_recovering_active: m.download_lanes_recovering_active as u32,
            download_lane_parks_no_work_total: m.download_lane_parks_no_work_total,
            download_lane_parks_spillover_speed_harm_total: m
                .download_lane_parks_spillover_speed_harm_total,
            download_lane_parks_ip_replacement_retired_total: m
                .download_lane_parks_ip_replacement_retired_total,
            download_lane_parks_error_total: m.download_lane_parks_error_total,
            download_lane_lease_items_total: m.download_lane_lease_items_total,
            download_lane_refill_granted_total: m.download_lane_refill_granted_total,
            download_lane_refill_parked_total: m.download_lane_refill_parked_total,
            download_pipeline_trial_success_total: m.download_pipeline_trial_success_total,
            download_pipeline_trial_failure_total: m.download_pipeline_trial_failure_total,
            download_pipeline_proof_pass_total: m.download_pipeline_proof_pass_total,
            download_pipeline_cooldown_total: m.download_pipeline_cooldown_total,
            download_pipeline_replay_items_total: m.download_pipeline_replay_items_total,
            ip_replacement_trial_extra_connections: m.ip_replacement_trial_extra_connections as u32,
            ip_replacement_burst_active: m.ip_replacement_burst_active,
            ip_replacement_over_max_connections: m.ip_replacement_over_max_connections as u32,
            ip_rtt_ewma_entries: m.ip_rtt_ewma_entries as u32,
            ip_rtt_ewma_slowest_ms: m.ip_rtt_ewma_slowest_ms,
            ip_replacement_trials_started_total: m.ip_replacement_trials_started_total,
            ip_replacement_trials_rejected_total: m.ip_replacement_trials_rejected_total,
            ip_replacement_trials_accepted_total: m.ip_replacement_trials_accepted_total,
            ip_replacement_trials_blocked_total: m.ip_replacement_trials_blocked_total,
            ip_replacement_trials_acquire_failed_total: m
                .ip_replacement_trials_acquire_failed_total,
            ip_replacement_trials_same_ip_rejected_total: m
                .ip_replacement_trials_same_ip_rejected_total,
            ip_replacement_old_connections_retired_total: m
                .ip_replacement_old_connections_retired_total,
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
            download_failures_article_not_found: m.download_failures_article_not_found,
            download_failures_capacity_unavailable: m.download_failures_capacity_unavailable,
            download_failures_transient: m.download_failures_transient,
            download_failures_auth: m.download_failures_auth,
            download_failures_permanent: m.download_failures_permanent,
            current_download_speed: m.current_download_speed,
            crc_errors: m.crc_errors,
            recovery_queue_depth: m.recovery_queue_depth as u32,
            articles_per_sec: m.articles_per_sec,
            decode_rate_mbps: m.decode_rate_mbps,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct SystemMetricsSnapshot {
    pub metrics: Metrics,
    pub global_state: GlobalQueueState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum MetricsHistoryRangeGql {
    TenMinutes,
    OneHour,
    SixHours,
    TwentyFourHours,
    SevenDays,
    ThirtyDays,
}

impl MetricsHistoryRangeGql {
    pub fn window_sec(self) -> i64 {
        match self {
            Self::TenMinutes => 10 * 60,
            Self::OneHour => 60 * 60,
            Self::SixHours => 6 * 60 * 60,
            Self::TwentyFourHours => 24 * 60 * 60,
            Self::SevenDays => 7 * 24 * 60 * 60,
            Self::ThirtyDays => 30 * 24 * 60 * 60,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum MetricSeriesVariant {
    Actual,
    Avg,
    Peak,
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
    pub variant: MetricSeriesVariant,
    pub values: Vec<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct MetricsHistoryResult {
    pub timestamps: Vec<f64>,
    pub resolution_sec: i32,
    pub series: Vec<MetricSeries>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, SimpleObject)]
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
            PipelineEvent::DownloadFinished {
                job_id,
                finalization_pending,
            } => Self {
                kind: EventKind::DownloadFinished,
                job_id: Some(job_id.0),
                file_id: finalization_pending.then(|| DOWNLOAD_FINALIZATION_MARKER.to_string()),
                message: "download finished".into(),
            },
            PipelineEvent::DownloadPipelineDrained { job_id } => Self {
                kind: EventKind::DownloadPipelineDrained,
                job_id: Some(job_id.0),
                file_id: None,
                message: "download pipeline drained".into(),
            },
            PipelineEvent::ServerAttempt {
                segment_id,
                server_id,
                attempt,
                retry_count,
                latency_ms,
                outcome,
                error,
                is_recovery,
            } => Self {
                kind: EventKind::ServerAttempt,
                job_id: Some(segment_id.file_id.job_id.0),
                file_id: Some(segment_id.file_id.to_string()),
                message: format!(
                    "server-{} attempt {} retry {} {} in {}ms{}{}",
                    u32::from(server_id.0) + 1,
                    attempt,
                    retry_count,
                    format!("{outcome:?}").to_ascii_lowercase(),
                    latency_ms,
                    if *is_recovery { " recovery" } else { "" },
                    error
                        .as_ref()
                        .map(|value| format!(": {value}"))
                        .unwrap_or_default()
                ),
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
            PipelineEvent::PhaseProgressUpdated { job_id } => Self {
                kind: EventKind::PhaseProgressUpdated,
                job_id: Some(job_id.0),
                file_id: None,
                message: "phase progress updated".into(),
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

/// Filesystem capacity for a configured storage directory (data / intermediate / complete).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct DiskUsage {
    /// Human label for the directory role (e.g. "Data", "Complete library").
    pub label: String,
    /// Configured directory path.
    pub path: String,
    /// Total capacity of the backing filesystem, in bytes.
    pub total_bytes: u64,
    /// Used capacity of the backing filesystem, in bytes.
    pub used_bytes: u64,
    /// Available (non-reserved) capacity, in bytes.
    pub free_bytes: u64,
}

/// Live health for one configured news server, derived from the NNTP connection pool.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct ServerHealth {
    pub host: String,
    pub port: u16,
    /// `host:port` label.
    pub label: String,
    /// "PRIMARY" for the highest-priority server, "BACKUP" otherwise.
    pub tier: String,
    /// One of "healthy", "degraded", "cooling_down", "disabled".
    pub state: String,
    /// Currently in-use connections (max - available permits).
    pub connections_active: u32,
    /// Runtime effective maximum connections (legacy field).
    pub connections_max: u32,
    /// Saved operator-configured maximum connections.
    pub connections_configured: u32,
    /// Runtime maximum after provider capacity adaptation.
    pub connections_effective: u32,
    /// End of the current provider-capacity penalty, if one is active.
    pub capacity_penalty_until_epoch_ms: Option<u64>,
    /// Active NNTP runtime generation.
    pub runtime_generation: u64,
    /// EWMA request latency in milliseconds.
    pub latency_ms: f64,
    pub success_count: u64,
    pub failure_count: u64,
    pub consecutive_failures: u32,
    /// Connections that died before reaching a healthy age, recently.
    pub premature_deaths: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_server_core::events::model::PipelineEvent;
    use weaver_server_core::jobs::JobId;

    #[test]
    fn phase_progress_event_is_not_reported_as_segment_committed() {
        let event =
            PipelineEventGql::from(&PipelineEvent::PhaseProgressUpdated { job_id: JobId(42) });

        assert_eq!(event.kind, EventKind::PhaseProgressUpdated);
        assert_eq!(event.job_id, Some(42));
        assert_eq!(event.file_id, None);
    }
}
