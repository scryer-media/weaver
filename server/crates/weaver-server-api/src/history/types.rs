// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{ComplexObject, Enum, InputObject, SimpleObject};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use weaver_server_core::history::timeline::JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER;
use weaver_server_core::{
    AsyncOperationState, AsyncOperationTargetState,
    HistoryDeleteOperationSummary as CoreHistoryDeleteOperationSummary,
    HistoryDeleteRowState as CoreHistoryDeleteRowState, JobHistoryRow, parse_history_metadata,
    split_history_metadata,
};

use crate::jobs::release_display::{
    ReleaseDisplayCache, ReleaseDisplayInput, release_display_info,
};
use crate::jobs::types::{
    Attribute, DuplicateSummaryInfo, JobStatusGql, ParsedRelease, PreparedQueueFilter,
    QueueAttention, QueueFilterInput, QueueItemState, matches_attribute_filter_prepared,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
#[graphql(complex)]
pub struct HistoryItem {
    pub id: u64,
    pub job_hash: Option<String>,
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
    pub delete_operation: Option<HistoryDeleteRowState>,
    #[serde(default)]
    pub duplicate_summary: Option<DuplicateSummaryInfo>,
    /// When the entry's files were removed and the entry kept; null while
    /// they are still there.
    #[serde(default)]
    pub files_removed_at: Option<DateTime<Utc>>,
    // The job's stored record of full file names, read only when a query
    // selects `fullFileNames`.
    #[graphql(skip)]
    #[serde(default)]
    pub full_file_names_record: Option<String>,
    // The job's failure recorded that a decode window was over the memory
    // profile's ceiling, so a reprocess with no limit may finish it.
    #[graphql(skip)]
    #[serde(default)]
    pub failed_over_ceiling: bool,
}

#[ComplexObject]
impl HistoryItem {
    /// The files Weaver gave a name other than their full one, each with its
    /// full name. Empty when every file carries its full name.
    async fn full_file_names(&self) -> Vec<FullFileName> {
        self.full_file_names_record
            .as_deref()
            .map(weaver_server_core::ingest::parse_full_file_names)
            .unwrap_or_default()
            .into_iter()
            .map(|file| FullFileName {
                name: file.name,
                full_name: file.full_name,
            })
            .collect()
    }

    /// Whether the job failed because a decode window was over the memory
    /// profile's ceiling, so `reprocessJob(noMemoryLimit: true)` may finish it.
    async fn failed_for_memory(&self) -> bool {
        self.state == QueueItemState::Failed && self.failed_over_ceiling
    }
}

/// A file Weaver named other than its full name, because the full name was
/// too long for a filesystem, used characters one refuses, or clashed with
/// another file in the job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, SimpleObject)]
pub struct FullFileName {
    /// The name Weaver gave the file last, inside `outputDir`. A file the job
    /// deleted after extracting it keeps the name it had then.
    pub name: String,
    /// The file's full name: the name it was posted with, or the one PAR2 or
    /// the release lookup gave it.
    pub full_name: String,
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

/// What a history operation does with its entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum HistoryOperationAction {
    /// Deletes the entry, with or without its files.
    Delete,
    /// Removes the entry's files and keeps the entry.
    RemoveFiles,
}

/// The history operation an entry is waiting on, or the last one that
/// failed for it. A files removal that succeeded shows here no more; the
/// entry's `filesRemovedAt` says so instead.
#[derive(Debug, Clone, PartialEq, Eq, SimpleObject, Serialize, Deserialize)]
pub struct HistoryDeleteRowState {
    pub operation_id: u64,
    pub state: HistoryDeleteRowStateKind,
    pub locked: bool,
    pub action: HistoryOperationAction,
    pub delete_files: bool,
    pub error_message: Option<String>,
    /// How a failed files removal ended; null for a delete, and while the
    /// removal waits or runs.
    pub files_removal_outcome: Option<HistoryFilesRemovalOutcome>,
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

/// What removing one history entry's files did. The entry stays either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum, Serialize, Deserialize)]
pub enum HistoryFilesRemovalOutcome {
    /// Folders Weaver manages were removed.
    Removed,
    /// Nothing was on disk, or the files were already removed.
    AlreadyGone,
    /// A folder of the job is not one Weaver manages and was left as it is.
    NotManaged,
    /// The job is in the queue or another history operation holds it.
    Busy,
    /// There is no history entry with this id.
    NotFound,
    /// Removing stopped on an error. Nothing is stamped and a retry is safe.
    Failed,
}

impl From<weaver_server_core::HistoryFilesRemovalOutcome> for HistoryFilesRemovalOutcome {
    fn from(outcome: weaver_server_core::HistoryFilesRemovalOutcome) -> Self {
        use weaver_server_core::HistoryFilesRemovalOutcome as Core;
        match outcome {
            Core::Removed => Self::Removed,
            Core::AlreadyGone => Self::AlreadyGone,
            Core::NotManaged => Self::NotManaged,
            Core::Busy => Self::Busy,
            Core::NotFound => Self::NotFound,
            Core::Failed => Self::Failed,
        }
    }
}

/// What a files removal request answered for one id, before any disk work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum HistoryFilesRemovalAcceptOutcome {
    /// Queued: the files are removed in the background.
    Accepted,
    /// There is no history entry and no job with this id.
    NotFound,
    /// Another history operation holds the entry, or the job is in the
    /// queue again.
    Busy,
    /// The job is in the queue and has not finished.
    NotFinished,
    /// The entry already says its files were removed.
    AlreadyRemoved,
}

impl From<weaver_server_core::HistoryFilesRemovalAcceptOutcome>
    for HistoryFilesRemovalAcceptOutcome
{
    fn from(outcome: weaver_server_core::HistoryFilesRemovalAcceptOutcome) -> Self {
        use weaver_server_core::HistoryFilesRemovalAcceptOutcome as Core;
        match outcome {
            Core::Accepted => Self::Accepted,
            Core::NotFound => Self::NotFound,
            Core::Busy => Self::Busy,
            Core::NotFinished => Self::NotFinished,
            Core::AlreadyRemoved => Self::AlreadyRemoved,
        }
    }
}

/// One id's answer to a files removal request.
#[derive(Debug, Clone, PartialEq, Eq, SimpleObject)]
pub struct HistoryFilesRemovalAcceptResult {
    pub id: u64,
    pub outcome: HistoryFilesRemovalAcceptOutcome,
}

/// The answer to a files removal request. The accepted ids are removed in
/// the background; each entry then shows `filesRemovedAt` once its files
/// are gone, or a failed `deleteOperation` with `action: REMOVE_FILES`.
#[derive(Debug, Clone, PartialEq, Eq, SimpleObject)]
pub struct HistoryFilesRemovalAcceptance {
    /// The operation holding the accepted ids; null when none was accepted.
    pub operation_id: Option<u64>,
    pub accepted_ids: Vec<u64>,
    pub total_targets: u32,
    /// One result per distinct id, in the order given.
    pub results: Vec<HistoryFilesRemovalAcceptResult>,
}

impl HistoryFilesRemovalAcceptance {
    pub(crate) fn from_core(acceptance: weaver_server_core::HistoryFilesRemovalAcceptance) -> Self {
        let results = acceptance
            .results
            .into_iter()
            .map(|(id, outcome)| HistoryFilesRemovalAcceptResult {
                id,
                outcome: outcome.into(),
            })
            .collect::<Vec<_>>();
        let accepted_ids = results
            .iter()
            .filter(|result| result.outcome == HistoryFilesRemovalAcceptOutcome::Accepted)
            .map(|result| result.id)
            .collect::<Vec<_>>();
        Self {
            operation_id: acceptance.operation_id,
            total_targets: accepted_ids.len() as u32,
            accepted_ids,
            results,
        }
    }
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
    /// Keep only rows whose category is one of these; absent or empty keeps
    /// every row.
    ///
    /// The list unions rather than intersects, so a caller offering categories
    /// as facets widens the result with each one. The empty string selects rows
    /// with no category at all, which is how "uncategorised" is asked for
    /// without a second field.
    pub categories: Option<Vec<String>>,
    pub sort_field: Option<HistorySortField>,
    pub sort_direction: Option<HistorySortDirection>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct HistoryPageCounts {
    pub all: u32,
    pub success: u32,
    pub failure: u32,
}

/// How many history rows carry one category.
#[derive(Debug, Clone, PartialEq, Eq, SimpleObject)]
pub struct HistoryCategoryCount {
    /// The category, or the empty string for rows with none — the same spelling
    /// `HistoryPageInput.categories` takes back.
    pub category: String,
    pub count: u32,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct HistoryPage {
    pub items: Vec<HistoryItem>,
    pub total_count: u32,
    pub counts: HistoryPageCounts,
    /// Every category present in history, with its row count.
    ///
    /// Counted with the search applied but *before* the category and status
    /// filters, so this answers "how many rows would that facet show me" for
    /// each one — including the facets the current selection is hiding. A
    /// caller that built its facet list out of `items` instead would watch the
    /// other categories vanish the moment one was picked.
    pub category_counts: Vec<HistoryCategoryCount>,
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
    /// Pass as `jobEvents(before:)` to read the events older than this one.
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject)]
#[graphql(complex)]
pub struct JobDetailSnapshot {
    pub queue_item: Option<crate::jobs::types::QueueItem>,
    pub history_item: Option<HistoryItem>,
    pub job_timeline: Option<JobTimeline>,
    /// The job's newest events, oldest first: at most 100.
    pub job_events: Vec<JobEvent>,
    /// Which servers served this job's articles, highest count first.
    ///
    /// Counted as articles land, so it covers only articles whose serving
    /// server could be named: it understates rather than misattributes, and
    /// the totals need not add up to the job's article count. Empty for jobs
    /// that finished before the counters existed.
    #[serde(default)]
    pub server_attribution: Vec<crate::jobs::types::JobServerContribution>,
    #[graphql(skip)]
    #[serde(skip)]
    pub(crate) job_id: u64,
}

#[async_graphql::ComplexObject]
impl JobDetailSnapshot {
    /// How many events the job has in all, however many `jobEvents` holds.
    async fn job_event_count(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<u64> {
        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        let job_id = self.job_id;
        tokio::task::spawn_blocking(move || db.count_job_events(job_id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))
    }

    /// How the download went: its archive tree, recovery and post. Null
    /// while the job is queued, and for an entry that finished before
    /// Weaver kept it.
    async fn download_summary(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<Option<crate::history::download_summary::DownloadSummary>> {
        if self.history_item.is_none() {
            return Ok(None);
        }
        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        let job_id = weaver_server_core::JobId(self.job_id);
        let summary = tokio::task::spawn_blocking(move || db.history_download_summary(job_id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(summary.map(Into::into))
    }
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
    ServerAttempt,
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
    // An archive given up on as an archive for one of its members.
    ArchiveDemoted,
    MoveToCompleteStarted,
    MoveToCompleteFinished,
    SegmentRetryScheduled,
    SegmentFailedPermanent,
    GlobalPaused,
    GlobalResumed,
    PhaseProgressUpdated,
    // One post-processing script run on the job, and what a script printed
    // at each severity.
    PostProcessingScript,
    ScriptError,
    ScriptWarning,
    ScriptOutput,
    ScriptDetail,
    ScriptDebug,
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
            "ServerAttempt" => Ok(Self::ServerAttempt),
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
            "ArchiveDemoted" => Ok(Self::ArchiveDemoted),
            "MoveToCompleteStarted" => Ok(Self::MoveToCompleteStarted),
            "MoveToCompleteFinished" => Ok(Self::MoveToCompleteFinished),
            "SegmentRetryScheduled" => Ok(Self::SegmentRetryScheduled),
            "SegmentFailedPermanent" => Ok(Self::SegmentFailedPermanent),
            "GlobalPaused" => Ok(Self::GlobalPaused),
            "GlobalResumed" => Ok(Self::GlobalResumed),
            "PhaseProgressUpdated" => Ok(Self::PhaseProgressUpdated),
            "PostProcessingScript" => Ok(Self::PostProcessingScript),
            "ScriptError" => Ok(Self::ScriptError),
            "ScriptWarning" => Ok(Self::ScriptWarning),
            "ScriptOutput" => Ok(Self::ScriptOutput),
            "ScriptDetail" => Ok(Self::ScriptDetail),
            "ScriptDebug" => Ok(Self::ScriptDebug),
            _ => Err(()),
        }
    }
}

// A stored job event as the API reports it. A kind the API does not name is
// left out with a warning rather than reported as some other kind.
pub(crate) fn job_event_from_core(event: weaver_server_core::JobEvent) -> Option<JobEvent> {
    let Ok(kind) = event.kind.parse::<EventKind>() else {
        tracing::warn!(
            job_id = event.job_id,
            kind = %event.kind,
            "job event of a kind the API does not name left out"
        );
        return None;
    };
    Some(JobEvent {
        kind,
        job_id: event.job_id,
        file_id: event.file_id,
        message: event.message,
        timestamp: event.timestamp as f64,
        cursor: None,
    })
}

// A stored event, with the cursor that pages back from it.
pub(crate) fn job_event_from_record(
    record: weaver_server_core::history::timeline::JobEventRecord,
) -> Option<JobEvent> {
    let id = record.id;
    let mut event = job_event_from_core(record.event)?;
    event.cursor = Some(crate::jobs::types::encode_event_cursor(id as i64));
    Some(event)
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
    delete_operation: Option<HistoryDeleteRowState>,
) -> HistoryItem {
    let metadata_pairs = parse_history_metadata(row.metadata.as_deref());
    let (client_request_id, attributes) = split_attributes(&metadata_pairs);
    let state = history_state_from_row(row);
    let display = release_display_info(ReleaseDisplayInput {
        job_name: &row.name,
        metadata: &metadata_pairs,
        category: row.category.as_deref(),
        cache: ReleaseDisplayCache::History(row.job_id),
    });
    history_item_from_display(
        row,
        &metadata_pairs,
        delete_operation,
        client_request_id,
        attributes,
        state,
        display.original_title,
        display.display_title,
        display.parsed_release,
    )
}

// Projects only the fields rendered by the paginated History table. Full
// release parsing remains available to detail and compatibility facades.
pub(crate) fn history_table_item_from_row(
    row: &JobHistoryRow,
    delete_operation: Option<HistoryDeleteRowState>,
) -> HistoryItem {
    let metadata_pairs = parse_history_metadata(row.metadata.as_deref());
    let (client_request_id, attributes) = split_attributes(&metadata_pairs);
    let state = history_state_from_row(row);
    let (original_title, display_title) = history_display_titles(row, &metadata_pairs);
    history_item_from_display(
        row,
        &metadata_pairs,
        delete_operation,
        client_request_id,
        attributes,
        state,
        original_title,
        display_title,
        ParsedRelease::default(),
    )
}

fn history_display_titles(row: &JobHistoryRow, metadata: &[(String, String)]) -> (String, String) {
    let original_title = weaver_server_core::ingest::original_release_title(&row.name, metadata);
    let display_title =
        weaver_server_core::ingest::derive_release_name(Some(&original_title), Some(&row.name));
    (original_title, display_title)
}

#[allow(clippy::too_many_arguments)]
fn history_item_from_display(
    row: &JobHistoryRow,
    metadata: &[(String, String)],
    delete_operation: Option<HistoryDeleteRowState>,
    client_request_id: Option<String>,
    attributes: Vec<Attribute>,
    state: QueueItemState,
    original_title: String,
    display_title: String,
    parsed_release: ParsedRelease,
) -> HistoryItem {
    let progress_percent = if row.total_bytes == 0 {
        0.0
    } else {
        (row.downloaded_bytes as f64 / row.total_bytes as f64 * 100.0).clamp(0.0, 100.0)
    };
    HistoryItem {
        id: row.job_id,
        job_hash: row.job_hash.as_ref().map(hex::encode),
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
        has_password: weaver_server_core::history::attributes::history_metadata_has_password(
            metadata,
        ),
        category: row.category.clone(),
        attributes,
        client_request_id,
        output_dir: row.output_dir.clone(),
        created_at: secs_to_datetime(row.created_at),
        completed_at: secs_to_datetime(row.completed_at),
        attention: attention_for_history_row(row, state),
        delete_operation,
        duplicate_summary: None,
        files_removed_at: row.files_removed_at.map(secs_to_datetime),
        full_file_names_record: weaver_server_core::ingest::full_file_names_record(metadata)
            .map(str::to_string),
        failed_over_ceiling: weaver_server_core::history::attributes::failed_for_memory(metadata),
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
        action: match row.action {
            weaver_server_core::HistoryOperationAction::Delete => HistoryOperationAction::Delete,
            weaver_server_core::HistoryOperationAction::RemoveFiles => {
                HistoryOperationAction::RemoveFiles
            }
        },
        delete_files: row.delete_files,
        error_message: row.error_message,
        files_removal_outcome: row.files_removal_outcome.map(Into::into),
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
    let prepared = PreparedQueueFilter::new(filter);
    matches_history_filter_prepared(item, prepared.as_ref())
}

pub(crate) fn matches_history_filter_prepared(
    item: &HistoryItem,
    filter: Option<&PreparedQueueFilter>,
) -> bool {
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
    if !matches_attribute_filter_prepared(&item.attributes, filter) {
        return false;
    }
    true
}

fn split_attributes(metadata: &[(String, String)]) -> (Option<String>, Vec<Attribute>) {
    let (client_request_id, attributes) = split_history_metadata(metadata);
    (
        client_request_id,
        attributes
            .into_iter()
            .map(|(key, value)| Attribute { key, value })
            .collect(),
    )
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
    history_state_from_status(&row.status)
}

// The public state of a history row with persisted `status`.
pub(crate) fn history_state_from_status(status: &str) -> QueueItemState {
    match status.to_ascii_lowercase().as_str() {
        "complete" => QueueItemState::Completed,
        "cancelled" | "failed" | "partial" => QueueItemState::Failed,
        "paused" => QueueItemState::Paused,
        _ => QueueItemState::Failed,
    }
}

#[cfg(test)]
mod tests;
