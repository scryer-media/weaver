use async_graphql::{Enum, InputObject, SimpleObject};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use weaver_server_core::history::timeline::JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER;
use weaver_server_core::{
    AsyncOperationState, AsyncOperationTargetState,
    DIAGNOSTIC_INCLUDE_SERVER_HOSTNAMES_ATTRIBUTE_KEY, DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY,
    DiagnosticRunRow, DiagnosticRunStage as CoreDiagnosticRunStage,
    HistoryDeleteOperationSummary as CoreHistoryDeleteOperationSummary,
    HistoryDeleteRowState as CoreHistoryDeleteRowState, JobHistoryRow,
};

use crate::jobs::release_display::{ReleaseDisplayInput, release_display_info};
use crate::jobs::types::{
    Attribute, CLIENT_REQUEST_ID_ATTRIBUTE_KEY, JobStatusGql, ParsedRelease, QueueAttention,
    QueueFilterInput, QueueItemState, matches_attribute_filter,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct HistoryItem {
    pub id: u64,
    pub name: String,
    pub display_title: String,
    pub original_title: String,
    pub parsed_release: ParsedRelease,
    pub state: QueueItemState,
    pub error: Option<String>,
    pub progress_percent: f64,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    pub failed_bytes: u64,
    pub health: u32,
    pub has_password: bool,
    pub category: Option<String>,
    pub attributes: Vec<Attribute>,
    pub client_request_id: Option<String>,
    pub output_dir: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub last_diagnostic_id: Option<String>,
    pub last_diagnostic_uploaded_at: Option<DateTime<Utc>>,
    pub diagnostic_run: Option<HistoryDiagnosticRun>,
    pub attention: Option<QueueAttention>,
    pub delete_operation: Option<HistoryDeleteRowState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum HistoryDiagnosticStage {
    Queued,
    Running,
    Collecting,
    Uploading,
    Complete,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, SimpleObject, Serialize, Deserialize)]
pub struct HistoryDiagnosticRun {
    pub source_job_id: u64,
    pub diagnostic_job_id: u64,
    pub smg_diagnostic_id: Option<String>,
    pub stage: HistoryDiagnosticStage,
    pub include_server_hostnames: bool,
    pub rerun_succeeded: Option<bool>,
    pub error_message: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, SimpleObject)]
pub struct DiagnosticRedownloadAcceptance {
    pub source_job_id: u64,
    pub diagnostic_job_id: u64,
    pub stage: HistoryDiagnosticStage,
    pub include_server_hostnames: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum AcceptHistoryDeleteMode {
    Ids,
    AllHistory,
}

#[derive(Debug, Clone, InputObject)]
pub struct AcceptHistoryDeleteInput {
    pub mode: AcceptHistoryDeleteMode,
    #[graphql(default)]
    pub ids: Vec<u64>,
    pub delete_files: bool,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct HistoryDeleteAcceptance {
    pub operation_id: u64,
    pub accepted_ids: Vec<u64>,
    pub total_targets: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum HistoryDeleteRowStateKind {
    Queued,
    Running,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum HistoryDeleteOperationState {
    Queued,
    Running,
    Completed,
    CompletedWithErrors,
}

#[derive(Debug, Clone, PartialEq, Eq, SimpleObject, Serialize, Deserialize)]
pub struct HistoryDeleteRowState {
    pub operation_id: u64,
    pub state: HistoryDeleteRowStateKind,
    pub locked: bool,
    pub delete_files: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, SimpleObject)]
pub struct HistoryDeleteOperation {
    pub id: u64,
    pub state: HistoryDeleteOperationState,
    pub delete_files: bool,
    pub total_targets: u32,
    pub queued_targets: u32,
    pub running_targets: u32,
    pub completed_targets: u32,
    pub failed_targets: u32,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum HistoryStatusFilter {
    All,
    Success,
    Failure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum HistorySortField {
    CompletedAt,
    Name,
    State,
    Health,
    Size,
    Category,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum HistorySortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, InputObject)]
pub struct HistoryPageInput {
    pub page_index: u32,
    pub page_size: u32,
    pub search: Option<String>,
    pub status: Option<HistoryStatusFilter>,
    pub sort_field: Option<HistorySortField>,
    pub sort_direction: Option<HistorySortDirection>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct HistoryPageCounts {
    pub all: u32,
    pub success: u32,
    pub failure: u32,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct HistoryPage {
    pub items: Vec<HistoryItem>,
    pub total_count: u32,
    pub counts: HistoryPageCounts,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, SimpleObject)]
pub struct HistoryCommandResult {
    pub success: bool,
    pub removed_ids: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct JobEvent {
    pub kind: EventKind,
    pub job_id: u64,
    pub file_id: Option<String>,
    pub message: String,
    pub timestamp: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct JobDetailSnapshot {
    pub queue_item: Option<crate::jobs::types::QueueItem>,
    pub history_item: Option<HistoryItem>,
    pub job_timeline: Option<JobTimeline>,
    pub job_events: Vec<JobEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TimelineMemberSubject {
    pub set_name: String,
    pub member: String,
    #[serde(default)]
    pub volume_index: Option<usize>,
}

pub(crate) fn encode_timeline_member_subject(
    set_name: &str,
    member: &str,
    volume_index: Option<usize>,
) -> Option<String> {
    serde_json::to_string(&TimelineMemberSubject {
        set_name: set_name.to_string(),
        member: member.to_string(),
        volume_index,
    })
    .ok()
}

pub(crate) fn decode_timeline_member_subject(value: Option<&str>) -> Option<TimelineMemberSubject> {
    serde_json::from_str(value?).ok()
}

pub(crate) const DOWNLOAD_FINALIZATION_MARKER: &str = JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER;

pub(crate) fn marks_download_finalization(value: Option<&str>) -> bool {
    value == Some(DOWNLOAD_FINALIZATION_MARKER)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum EventKind {
    JobCreated,
    JobPaused,
    JobResumed,
    JobCancelled,
    JobCompleted,
    JobFailed,
    DownloadStarted,
    DownloadFinished,
    DownloadPipelineDrained,
    ArticleDownloaded,
    ArticleNotFound,
    SegmentDecoded,
    SegmentCommitted,
    FileComplete,
    FileMissing,
    VerificationStarted,
    VerificationComplete,
    JobVerificationStarted,
    JobVerificationComplete,
    RepairStarted,
    RepairComplete,
    RepairFailed,
    ExtractionReady,
    ExtractionMemberStarted,
    ExtractionMemberWaitingStarted,
    ExtractionMemberWaitingFinished,
    ExtractionMemberAppendStarted,
    ExtractionMemberAppendFinished,
    ExtractionMemberFinished,
    ExtractionMemberFailed,
    ExtractionProgress,
    ExtractionComplete,
    ExtractionFailed,
    MoveToCompleteStarted,
    MoveToCompleteFinished,
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
            "JobCancelled" => Ok(Self::JobCancelled),
            "JobCompleted" => Ok(Self::JobCompleted),
            "JobFailed" => Ok(Self::JobFailed),
            "DownloadStarted" => Ok(Self::DownloadStarted),
            "DownloadFinished" => Ok(Self::DownloadFinished),
            "DownloadPipelineDrained" => Ok(Self::DownloadPipelineDrained),
            "ArticleDownloaded" => Ok(Self::ArticleDownloaded),
            "ArticleNotFound" => Ok(Self::ArticleNotFound),
            "SegmentDecoded" => Ok(Self::SegmentDecoded),
            "SegmentCommitted" => Ok(Self::SegmentCommitted),
            "FileComplete" => Ok(Self::FileComplete),
            "FileMissing" => Ok(Self::FileMissing),
            "VerificationStarted" => Ok(Self::VerificationStarted),
            "VerificationComplete" => Ok(Self::VerificationComplete),
            "JobVerificationStarted" => Ok(Self::JobVerificationStarted),
            "JobVerificationComplete" => Ok(Self::JobVerificationComplete),
            "RepairStarted" => Ok(Self::RepairStarted),
            "RepairComplete" => Ok(Self::RepairComplete),
            "RepairFailed" => Ok(Self::RepairFailed),
            "ExtractionReady" => Ok(Self::ExtractionReady),
            "ExtractionMemberStarted" => Ok(Self::ExtractionMemberStarted),
            "ExtractionMemberWaitingStarted" => Ok(Self::ExtractionMemberWaitingStarted),
            "ExtractionMemberWaitingFinished" => Ok(Self::ExtractionMemberWaitingFinished),
            "ExtractionMemberAppendStarted" => Ok(Self::ExtractionMemberAppendStarted),
            "ExtractionMemberAppendFinished" => Ok(Self::ExtractionMemberAppendFinished),
            "ExtractionMemberFinished" => Ok(Self::ExtractionMemberFinished),
            "ExtractionMemberFailed" => Ok(Self::ExtractionMemberFailed),
            "ExtractionProgress" => Ok(Self::ExtractionProgress),
            "ExtractionComplete" => Ok(Self::ExtractionComplete),
            "ExtractionFailed" => Ok(Self::ExtractionFailed),
            "MoveToCompleteStarted" => Ok(Self::MoveToCompleteStarted),
            "MoveToCompleteFinished" => Ok(Self::MoveToCompleteFinished),
            "SegmentRetryScheduled" => Ok(Self::SegmentRetryScheduled),
            "SegmentFailedPermanent" => Ok(Self::SegmentFailedPermanent),
            "GlobalPaused" => Ok(Self::GlobalPaused),
            "GlobalResumed" => Ok(Self::GlobalResumed),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum TimelineStage {
    PendingDownload,
    Downloading,
    FinalizingDownload,
    Paused,
    Verifying,
    Repairing,
    Extracting,
    Interrupted,
    FinalMove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum TimelineSpanState {
    Running,
    Complete,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum ExtractionMemberState {
    Running,
    Interrupted,
    Complete,
    AwaitingRepair,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum ExtractionMemberSpanKind {
    Extracting,
    WaitingForVolume,
    Appending,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct JobTimelineSpan {
    pub started_at: f64,
    pub ended_at: Option<f64>,
    pub state: TimelineSpanState,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct JobTimelineLane {
    pub stage: TimelineStage,
    pub spans: Vec<JobTimelineSpan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct ExtractionMemberTimelineSpan {
    pub kind: ExtractionMemberSpanKind,
    pub started_at: f64,
    pub ended_at: Option<f64>,
    pub state: TimelineSpanState,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct ExtractionMemberTimeline {
    pub member: String,
    pub state: ExtractionMemberState,
    pub error: Option<String>,
    pub spans: Vec<ExtractionMemberTimelineSpan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct ExtractionTimelineGroup {
    pub set_name: String,
    pub members: Vec<ExtractionMemberTimeline>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct JobTimeline {
    pub started_at: f64,
    pub ended_at: Option<f64>,
    pub outcome: JobStatusGql,
    pub lanes: Vec<JobTimelineLane>,
    pub extraction_groups: Vec<ExtractionTimelineGroup>,
}

pub fn history_item_from_row(
    row: &JobHistoryRow,
    diagnostic_run: Option<HistoryDiagnosticRun>,
    delete_operation: Option<HistoryDeleteRowState>,
) -> HistoryItem {
    let metadata_pairs = row
        .metadata
        .as_deref()
        .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
        .unwrap_or_default();
    let (client_request_id, attributes) = split_attributes(&metadata_pairs);
    let state = history_state_from_row(row);
    let display = release_display_info(ReleaseDisplayInput {
        job_name: &row.name,
        metadata: &metadata_pairs,
        category: row.category.as_deref(),
    });
    let progress_percent = if row.total_bytes == 0 {
        0.0
    } else {
        (row.downloaded_bytes as f64 / row.total_bytes as f64 * 100.0).clamp(0.0, 100.0)
    };
    HistoryItem {
        id: row.job_id,
        name: row.name.clone(),
        display_title: display.display_title,
        original_title: display.original_title,
        parsed_release: display.parsed_release,
        state,
        error: row.error_message.clone(),
        progress_percent,
        total_bytes: row.total_bytes,
        downloaded_bytes: row.downloaded_bytes,
        optional_recovery_bytes: row.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: row.optional_recovery_downloaded_bytes,
        failed_bytes: row.failed_bytes,
        health: row.health,
        has_password: false,
        category: row.category.clone(),
        attributes,
        client_request_id,
        output_dir: row.output_dir.clone(),
        created_at: secs_to_datetime(row.created_at),
        completed_at: secs_to_datetime(row.completed_at),
        last_diagnostic_id: row.last_diagnostic_id.clone(),
        last_diagnostic_uploaded_at: row.last_diagnostic_uploaded_at_epoch_ms.map(ms_to_datetime),
        diagnostic_run,
        attention: attention_for_history_row(row, state),
        delete_operation,
    }
}

pub fn history_diagnostic_run_from_core(row: DiagnosticRunRow) -> HistoryDiagnosticRun {
    HistoryDiagnosticRun {
        source_job_id: row.source_job_id,
        diagnostic_job_id: row.diagnostic_job_id,
        smg_diagnostic_id: row.smg_diagnostic_id,
        stage: history_diagnostic_stage_from_core(row.stage),
        include_server_hostnames: row.include_server_hostnames,
        rerun_succeeded: row.rerun_succeeded,
        error_message: row.error_message,
        updated_at: ms_to_datetime(row.updated_at_epoch_ms),
    }
}

pub fn history_diagnostic_stage_from_core(stage: CoreDiagnosticRunStage) -> HistoryDiagnosticStage {
    match stage {
        CoreDiagnosticRunStage::Queued => HistoryDiagnosticStage::Queued,
        CoreDiagnosticRunStage::Running => HistoryDiagnosticStage::Running,
        CoreDiagnosticRunStage::Collecting => HistoryDiagnosticStage::Collecting,
        CoreDiagnosticRunStage::Uploading => HistoryDiagnosticStage::Uploading,
        CoreDiagnosticRunStage::Complete => HistoryDiagnosticStage::Complete,
        CoreDiagnosticRunStage::Failed => HistoryDiagnosticStage::Failed,
    }
}

pub fn history_delete_row_state_from_core(row: CoreHistoryDeleteRowState) -> HistoryDeleteRowState {
    HistoryDeleteRowState {
        operation_id: row.operation_id,
        state: match row.state {
            AsyncOperationTargetState::Queued => HistoryDeleteRowStateKind::Queued,
            AsyncOperationTargetState::Running => HistoryDeleteRowStateKind::Running,
            AsyncOperationTargetState::Failed => HistoryDeleteRowStateKind::Failed,
            AsyncOperationTargetState::Completed => {
                unreachable!("completed delete rows should not be exposed on history items")
            }
        },
        locked: row.locked,
        delete_files: row.delete_files,
        error_message: row.error_message,
    }
}

pub fn history_delete_operation_from_core(
    summary: CoreHistoryDeleteOperationSummary,
) -> HistoryDeleteOperation {
    HistoryDeleteOperation {
        id: summary.id,
        state: history_delete_operation_state_from_core(summary.state),
        delete_files: summary.delete_files,
        total_targets: summary.total_targets,
        queued_targets: summary.queued_targets,
        running_targets: summary.running_targets,
        completed_targets: summary.completed_targets,
        failed_targets: summary.failed_targets,
        requested_at: DateTime::from_timestamp_millis(summary.requested_at_epoch_ms)
            .unwrap_or_else(Utc::now),
    }
}

pub fn history_delete_operation_state_from_core(
    state: AsyncOperationState,
) -> HistoryDeleteOperationState {
    match state {
        AsyncOperationState::Queued => HistoryDeleteOperationState::Queued,
        AsyncOperationState::Running => HistoryDeleteOperationState::Running,
        AsyncOperationState::Completed => HistoryDeleteOperationState::Completed,
        AsyncOperationState::CompletedWithErrors => {
            HistoryDeleteOperationState::CompletedWithErrors
        }
    }
}

pub fn matches_history_filter(item: &HistoryItem, filter: Option<&QueueFilterInput>) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if let Some(item_ids) = &filter.item_ids
        && !item_ids.contains(&item.id)
    {
        return false;
    }
    if let Some(states) = &filter.states
        && !states.contains(&item.state)
    {
        return false;
    }
    if let Some(category) = &filter.category
        && item.category.as_ref() != Some(category)
    {
        return false;
    }
    if !matches_attribute_filter(&item.attributes, filter) {
        return false;
    }
    true
}

fn split_attributes(metadata: &[(String, String)]) -> (Option<String>, Vec<Attribute>) {
    let mut client_request_id = None;
    let mut attributes = Vec::new();
    for (key, value) in metadata {
        if key == CLIENT_REQUEST_ID_ATTRIBUTE_KEY {
            client_request_id = Some(value.clone());
        } else if key == DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY
            || key == DIAGNOSTIC_INCLUDE_SERVER_HOSTNAMES_ATTRIBUTE_KEY
        {
            continue;
        } else {
            attributes.push(Attribute {
                key: key.clone(),
                value: value.clone(),
            });
        }
    }
    (client_request_id, attributes)
}

fn secs_to_datetime(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

fn ms_to_datetime(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

fn attention_for_history_row(row: &JobHistoryRow, state: QueueItemState) -> Option<QueueAttention> {
    if row.status.eq_ignore_ascii_case("cancelled") {
        return Some(QueueAttention {
            code: "CANCELLED".to_string(),
            message: "item was cancelled".to_string(),
        });
    }

    row.error_message.as_ref().map(|message| QueueAttention {
        code: if state == QueueItemState::Failed {
            "JOB_FAILED".to_string()
        } else {
            "ATTENTION".to_string()
        },
        message: message.clone(),
    })
}

fn history_state_from_row(row: &JobHistoryRow) -> QueueItemState {
    match row.status.to_ascii_lowercase().as_str() {
        "complete" => QueueItemState::Completed,
        "cancelled" => QueueItemState::Failed,
        "failed" => QueueItemState::Failed,
        "paused" => QueueItemState::Paused,
        _ => QueueItemState::Failed,
    }
}

#[cfg(test)]
mod tests;
