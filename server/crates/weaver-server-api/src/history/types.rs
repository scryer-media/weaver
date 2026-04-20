use async_graphql::{Enum, SimpleObject};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use weaver_server_core::JobHistoryRow;

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
    pub attention: Option<QueueAttention>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, SimpleObject)]
pub struct HistoryCommandResult {
    pub success: bool,
    pub removed_ids: Vec<u64>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct JobEvent {
    pub kind: EventKind,
    pub job_id: u64,
    pub file_id: Option<String>,
    pub message: String,
    pub timestamp: f64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum EventKind {
    JobCreated,
    JobPaused,
    JobResumed,
    JobCancelled,
    JobCompleted,
    JobFailed,
    DownloadStarted,
    DownloadFinished,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum TimelineStage {
    PendingDownload,
    Downloading,
    Paused,
    Verifying,
    Repairing,
    Extracting,
    Interrupted,
    FinalMove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum TimelineSpanState {
    Running,
    Complete,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum ExtractionMemberState {
    Running,
    Interrupted,
    Complete,
    AwaitingRepair,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum ExtractionMemberSpanKind {
    Extracting,
    WaitingForVolume,
    Appending,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct JobTimelineSpan {
    pub started_at: f64,
    pub ended_at: Option<f64>,
    pub state: TimelineSpanState,
    pub label: Option<String>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct JobTimelineLane {
    pub stage: TimelineStage,
    pub spans: Vec<JobTimelineSpan>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ExtractionMemberTimelineSpan {
    pub kind: ExtractionMemberSpanKind,
    pub started_at: f64,
    pub ended_at: Option<f64>,
    pub state: TimelineSpanState,
    pub label: Option<String>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ExtractionMemberTimeline {
    pub member: String,
    pub state: ExtractionMemberState,
    pub error: Option<String>,
    pub spans: Vec<ExtractionMemberTimelineSpan>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ExtractionTimelineGroup {
    pub set_name: String,
    pub members: Vec<ExtractionMemberTimeline>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct JobTimeline {
    pub started_at: f64,
    pub ended_at: Option<f64>,
    pub outcome: JobStatusGql,
    pub lanes: Vec<JobTimelineLane>,
    pub extraction_groups: Vec<ExtractionTimelineGroup>,
}

pub fn history_item_from_row(row: &JobHistoryRow) -> HistoryItem {
    let metadata_pairs = row
        .metadata
        .as_deref()
        .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
        .unwrap_or_default();
    let (client_request_id, attributes) = split_attributes(&metadata_pairs);
    let state = history_state_from_row(row);
    let original_title =
        weaver_server_core::ingest::original_release_title(&row.name, &metadata_pairs);
    let display_title =
        weaver_server_core::ingest::derive_release_name(Some(&original_title), Some(&row.name));
    let parsed_release = ParsedRelease::from(weaver_server_core::ingest::parse_job_release(
        &row.name,
        &metadata_pairs,
    ));
    let progress_percent = if row.total_bytes == 0 {
        0.0
    } else {
        (row.downloaded_bytes as f64 / row.total_bytes as f64 * 100.0).clamp(0.0, 100.0)
    };
    HistoryItem {
        id: row.job_id,
        name: row.name.clone(),
        display_title,
        original_title,
        parsed_release,
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
        attention: attention_for_history_row(row, state),
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
