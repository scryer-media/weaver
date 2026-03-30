use async_graphql::{Enum, InputObject, SimpleObject, Upload};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use weaver_core::release_name::{derive_release_name, original_release_title, parse_job_release};
use weaver_scheduler::handle::DownloadBlockState;
use weaver_scheduler::job::JobStatus;
use weaver_scheduler::metrics::MetricsSnapshot;
use weaver_state::JobHistoryRow;

use crate::types::{DownloadBlock, Metrics, ParsedRelease};

pub const CLIENT_REQUEST_ID_ATTRIBUTE_KEY: &str = "__weaver_client_request_id";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, SimpleObject)]
pub struct Attribute {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, InputObject)]
pub struct AttributeInput {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Enum)]
pub enum QueueItemState {
    Queued,
    Downloading,
    Checking,
    Verifying,
    Repairing,
    Extracting,
    Finalizing,
    Completed,
    Failed,
    Paused,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Enum)]
pub enum QueueWaitReason {
    RepairCapacity,
    ExtractionCapacity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, SimpleObject)]
pub struct QueueAttention {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct QueueItem {
    pub id: u64,
    pub name: String,
    pub display_title: String,
    pub original_title: String,
    pub parsed_release: ParsedRelease,
    pub state: QueueItemState,
    pub wait_reason: Option<QueueWaitReason>,
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
    pub attention: Option<QueueAttention>,
}

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
pub struct QueueSummary {
    pub total_items: u32,
    pub queued_items: u32,
    pub active_items: u32,
    pub paused_items: u32,
    pub failed_items: u32,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub current_download_speed: u64,
    pub verifying_items: u32,
    pub repairing_items: u32,
    pub extracting_items: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct GlobalQueueState {
    pub is_paused: bool,
    pub speed_limit_bytes_per_sec: u64,
    pub download_block: DownloadBlock,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct QueueSnapshot {
    pub items: Vec<QueueItem>,
    pub summary: QueueSummary,
    pub metrics: Metrics,
    pub global_state: GlobalQueueState,
    pub latest_cursor: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Enum)]
pub enum QueueEventKind {
    ItemCreated,
    ItemStateChanged,
    ItemProgress,
    ItemAttention,
    ItemCompleted,
    ItemRemoved,
    GlobalStateChanged,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct QueueEvent {
    pub cursor: String,
    pub occurred_at: DateTime<Utc>,
    pub kind: QueueEventKind,
    pub item_id: Option<u64>,
    pub item: Option<QueueItem>,
    pub state: Option<QueueItemState>,
    pub previous_state: Option<QueueItemState>,
    pub attention: Option<QueueAttention>,
    pub global_state: Option<GlobalQueueState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct SubmissionResult {
    pub accepted: bool,
    pub client_request_id: Option<String>,
    pub item: QueueItem,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct QueueCommandResult {
    pub success: bool,
    pub message: Option<String>,
    pub item: Option<QueueItem>,
    pub global_state: Option<GlobalQueueState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, SimpleObject)]
pub struct HistoryCommandResult {
    pub success: bool,
    pub removed_ids: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct SystemStatus {
    pub version: String,
    pub global_state: GlobalQueueState,
    pub summary: QueueSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, InputObject, Default)]
pub struct QueueFilterInput {
    pub item_ids: Option<Vec<u64>>,
    pub states: Option<Vec<QueueItemState>>,
    pub category: Option<String>,
    pub has_attribute_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, InputObject, Default)]
pub struct SubmitNzbInput {
    pub url: Option<String>,
    pub nzb_base64: Option<String>,
    pub nzb_upload: Option<Upload>,
    pub filename: Option<String>,
    pub password: Option<String>,
    pub category: Option<String>,
    pub attributes: Option<Vec<AttributeInput>>,
    pub client_request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedQueueEvent {
    pub occurred_at_ms: i64,
    pub kind: QueueEventKind,
    pub item_id: Option<u64>,
    pub item: Option<QueueItem>,
    pub state: Option<QueueItemState>,
    pub previous_state: Option<QueueItemState>,
    pub attention: Option<QueueAttention>,
    pub global_state: Option<GlobalQueueState>,
}

/// Stable public cursor format for replayable queue events.
///
/// The payload is the URL-safe base64 encoding of the ASCII string `evt:<id>`.
/// Keep the `evt:` prefix stable so future cursor formats can be versioned by
/// prefix without breaking older consumers.
pub fn decode_event_cursor(cursor: Option<&str>) -> Result<Option<i64>, String> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| "cursor is not valid base64".to_string())?;
    let decoded =
        String::from_utf8(decoded).map_err(|_| "cursor is not valid UTF-8".to_string())?;
    let Some(id) = decoded.strip_prefix("evt:") else {
        return Err("cursor has an invalid prefix".to_string());
    };
    id.parse::<i64>()
        .map(Some)
        .map_err(|_| "cursor does not contain a valid event id".to_string())
}

pub fn encode_event_cursor(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("evt:{id}"))
}

pub fn decode_offset_cursor(cursor: Option<&str>) -> Result<usize, String> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| "cursor is not valid base64".to_string())?;
    let decoded =
        String::from_utf8(decoded).map_err(|_| "cursor is not valid UTF-8".to_string())?;
    let Some(offset) = decoded.strip_prefix("off:") else {
        return Err("cursor has an invalid prefix".to_string());
    };
    offset
        .parse::<usize>()
        .map_err(|_| "cursor does not contain a valid offset".to_string())
}

pub fn encode_offset_cursor(offset: usize) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("off:{offset}"))
}

pub fn submit_metadata(
    attributes: Option<Vec<AttributeInput>>,
    client_request_id: Option<String>,
) -> Vec<(String, String)> {
    let mut metadata: Vec<(String, String)> = attributes
        .unwrap_or_default()
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect();
    if let Some(client_request_id) = client_request_id.filter(|value| !value.trim().is_empty()) {
        metadata.push((
            CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
            client_request_id.trim().to_string(),
        ));
    }
    metadata
}

pub fn queue_item_from_job(info: &weaver_scheduler::JobInfo) -> QueueItem {
    let (client_request_id, attributes) = split_attributes(&info.metadata);
    let state = QueueItemState::from(&info.status);
    let original_title = original_release_title(&info.name, &info.metadata);
    let display_title = derive_release_name(Some(&original_title), Some(&info.name));
    let parsed_release = ParsedRelease::from(parse_job_release(&info.name, &info.metadata));
    QueueItem {
        id: info.job_id.0,
        name: info.name.clone(),
        display_title,
        original_title,
        parsed_release,
        state,
        wait_reason: wait_reason_for_status(&info.status),
        error: info.error.clone(),
        progress_percent: normalize_progress_percent(info.progress),
        total_bytes: info.total_bytes,
        downloaded_bytes: info.downloaded_bytes,
        optional_recovery_bytes: info.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: info.optional_recovery_downloaded_bytes,
        failed_bytes: info.failed_bytes,
        health: info.health,
        has_password: info.password.is_some(),
        category: info.category.clone(),
        attributes,
        client_request_id,
        output_dir: info.output_dir.clone(),
        created_at: ms_to_datetime(info.created_at_epoch_ms as i64),
        attention: attention_for_live_job(info, state),
    }
}

pub fn queue_item_from_submission(submitted: &crate::submit::SubmittedJob) -> QueueItem {
    let (client_request_id, attributes) = split_attributes(&submitted.spec.metadata);
    let original_title = original_release_title(&submitted.spec.name, &submitted.spec.metadata);
    let display_title = derive_release_name(Some(&original_title), Some(&submitted.spec.name));
    let parsed_release = ParsedRelease::from(parse_job_release(
        &submitted.spec.name,
        &submitted.spec.metadata,
    ));
    QueueItem {
        id: submitted.job_id.0,
        name: submitted.spec.name.clone(),
        display_title,
        original_title,
        parsed_release,
        state: QueueItemState::Queued,
        wait_reason: None,
        error: None,
        progress_percent: 0.0,
        total_bytes: submitted.spec.total_bytes,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        has_password: submitted.spec.password.is_some(),
        category: submitted.spec.category.clone(),
        attributes,
        client_request_id,
        output_dir: None,
        created_at: ms_to_datetime(submitted.created_at_epoch_ms as i64),
        attention: None,
    }
}

pub fn history_item_from_row(row: &JobHistoryRow) -> HistoryItem {
    let metadata_pairs = row
        .metadata
        .as_deref()
        .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
        .unwrap_or_default();
    let (client_request_id, attributes) = split_attributes(&metadata_pairs);
    let state = history_state_from_row(row);
    let original_title = original_release_title(&row.name, &metadata_pairs);
    let display_title = derive_release_name(Some(&original_title), Some(&row.name));
    let parsed_release = ParsedRelease::from(parse_job_release(&row.name, &metadata_pairs));
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
        created_at: ms_to_datetime(row.created_at),
        completed_at: ms_to_datetime(row.completed_at),
        attention: attention_for_history_row(row, state),
    }
}

fn matches_item_filter(
    id: u64,
    state: QueueItemState,
    item_category: Option<&String>,
    attributes: &[Attribute],
    filter: Option<&QueueFilterInput>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if let Some(item_ids) = &filter.item_ids
        && !item_ids.contains(&id)
    {
        return false;
    }
    if let Some(states) = &filter.states
        && !states.contains(&state)
    {
        return false;
    }
    if let Some(category) = &filter.category
        && item_category != Some(category)
    {
        return false;
    }
    if let Some(attribute_key) = &filter.has_attribute_key
        && !attributes.iter().any(|entry| entry.key == *attribute_key)
    {
        return false;
    }
    true
}

pub fn matches_queue_filter(item: &QueueItem, filter: Option<&QueueFilterInput>) -> bool {
    matches_item_filter(
        item.id,
        item.state,
        item.category.as_ref(),
        &item.attributes,
        filter,
    )
}

pub fn matches_history_filter(item: &HistoryItem, filter: Option<&QueueFilterInput>) -> bool {
    matches_item_filter(
        item.id,
        item.state,
        item.category.as_ref(),
        &item.attributes,
        filter,
    )
}

pub fn matches_queue_event_filter(event: &QueueEvent, filter: Option<&QueueFilterInput>) -> bool {
    let Some(filter) = filter else {
        return true;
    };

    if let Some(item_ids) = &filter.item_ids {
        let Some(item_id) = event.item_id else {
            return false;
        };
        if !item_ids.contains(&item_id) {
            return false;
        }
    }

    if let Some(states) = &filter.states {
        let Some(state) = event
            .state
            .or_else(|| event.item.as_ref().map(|item| item.state))
        else {
            return false;
        };
        if !states.contains(&state) {
            return false;
        }
    }

    if let Some(category) = &filter.category {
        let Some(item) = &event.item else {
            return false;
        };
        if item.category.as_ref() != Some(category) {
            return false;
        }
    }

    if let Some(attribute_key) = &filter.has_attribute_key {
        let Some(item) = &event.item else {
            return false;
        };
        if !item
            .attributes
            .iter()
            .any(|entry| entry.key == *attribute_key)
        {
            return false;
        }
    }

    true
}

pub fn queue_summary(items: &[QueueItem], metrics: &MetricsSnapshot) -> QueueSummary {
    let mut summary = QueueSummary {
        total_items: items.len() as u32,
        queued_items: 0,
        active_items: 0,
        paused_items: 0,
        failed_items: 0,
        total_bytes: items.iter().map(|item| item.total_bytes).sum(),
        downloaded_bytes: items.iter().map(|item| item.downloaded_bytes).sum(),
        current_download_speed: metrics.current_download_speed,
        verifying_items: 0,
        repairing_items: 0,
        extracting_items: 0,
    };

    for item in items {
        match item.state {
            QueueItemState::Queued => summary.queued_items += 1,
            QueueItemState::Paused => summary.paused_items += 1,
            QueueItemState::Failed => summary.failed_items += 1,
            QueueItemState::Verifying => {
                summary.active_items += 1;
                summary.verifying_items += 1;
            }
            QueueItemState::Repairing => {
                summary.active_items += 1;
                summary.repairing_items += 1;
            }
            QueueItemState::Extracting => {
                summary.active_items += 1;
                summary.extracting_items += 1;
            }
            QueueItemState::Downloading | QueueItemState::Checking | QueueItemState::Finalizing => {
                summary.active_items += 1;
            }
            QueueItemState::Completed => {}
        }
    }

    summary
}

pub fn global_queue_state(
    is_paused: bool,
    download_block: &DownloadBlockState,
    speed_limit_bytes_per_sec: u64,
) -> GlobalQueueState {
    GlobalQueueState {
        is_paused,
        speed_limit_bytes_per_sec,
        download_block: DownloadBlock::from(download_block),
    }
}

pub fn queue_event_from_record(id: i64, record: PersistedQueueEvent) -> QueueEvent {
    QueueEvent {
        cursor: encode_event_cursor(id),
        occurred_at: ms_to_datetime(record.occurred_at_ms),
        kind: record.kind,
        item_id: record.item_id,
        item: record.item,
        state: record.state,
        previous_state: record.previous_state,
        attention: record.attention,
        global_state: record.global_state,
    }
}

pub fn metrics_from_snapshot(snapshot: &MetricsSnapshot) -> Metrics {
    Metrics::from(snapshot)
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

fn normalize_progress_percent(progress_ratio: f64) -> f64 {
    (progress_ratio * 100.0).clamp(0.0, 100.0)
}

fn ms_to_datetime(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

fn wait_reason_for_status(status: &JobStatus) -> Option<QueueWaitReason> {
    match status {
        JobStatus::QueuedRepair => Some(QueueWaitReason::RepairCapacity),
        JobStatus::QueuedExtract => Some(QueueWaitReason::ExtractionCapacity),
        _ => None,
    }
}

fn attention_for_live_job(
    info: &weaver_scheduler::JobInfo,
    state: QueueItemState,
) -> Option<QueueAttention> {
    if let Some(error) = &info.error {
        return Some(QueueAttention {
            code: match state {
                QueueItemState::Failed => "JOB_FAILED".to_string(),
                _ => "ATTENTION".to_string(),
            },
            message: error.clone(),
        });
    }

    if info.failed_bytes > 0 && state != QueueItemState::Completed {
        return Some(QueueAttention {
            code: "UNHEALTHY_ARTICLES".to_string(),
            message: format!(
                "{} bytes are currently missing or unhealthy for this item",
                info.failed_bytes
            ),
        });
    }

    None
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

impl From<&JobStatus> for QueueItemState {
    fn from(value: &JobStatus) -> Self {
        match value {
            JobStatus::Queued | JobStatus::QueuedRepair | JobStatus::QueuedExtract => Self::Queued,
            JobStatus::Downloading => Self::Downloading,
            JobStatus::Checking => Self::Checking,
            JobStatus::Verifying => Self::Verifying,
            JobStatus::Repairing => Self::Repairing,
            JobStatus::Extracting => Self::Extracting,
            JobStatus::Moving => Self::Finalizing,
            JobStatus::Complete => Self::Completed,
            JobStatus::Failed { .. } => Self::Failed,
            JobStatus::Paused => Self::Paused,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_core::id::JobId;
    use weaver_state::JobHistoryRow;

    fn base_job(status: JobStatus) -> weaver_scheduler::JobInfo {
        weaver_scheduler::JobInfo {
            job_id: JobId(42),
            name: "Facade.Test.Release".to_string(),
            status,
            progress: 0.5,
            total_bytes: 1_000,
            downloaded_bytes: 500,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            password: None,
            category: Some("movies".to_string()),
            metadata: vec![
                ("source".to_string(), "api".to_string()),
                (
                    CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                    "req-42".to_string(),
                ),
            ],
            output_dir: Some("/tmp/out".to_string()),
            error: None,
            created_at_epoch_ms: 1_700_000_000_000.0,
        }
    }

    #[test]
    fn queue_item_collapses_internal_waiting_states() {
        let repair = queue_item_from_job(&base_job(JobStatus::QueuedRepair));
        assert_eq!(repair.state, QueueItemState::Queued);
        assert_eq!(repair.wait_reason, Some(QueueWaitReason::RepairCapacity));

        let extract = queue_item_from_job(&base_job(JobStatus::QueuedExtract));
        assert_eq!(extract.state, QueueItemState::Queued);
        assert_eq!(
            extract.wait_reason,
            Some(QueueWaitReason::ExtractionCapacity)
        );
    }

    #[test]
    fn queue_item_maps_moving_to_finalizing() {
        let item = queue_item_from_job(&base_job(JobStatus::Moving));
        assert_eq!(item.state, QueueItemState::Finalizing);
        assert_eq!(item.client_request_id.as_deref(), Some("req-42"));
        assert_eq!(item.attributes.len(), 1);
        assert_eq!(item.attributes[0].key, "source");
    }

    #[test]
    fn queue_item_surfaces_unhealthy_attention() {
        let mut job = base_job(JobStatus::Downloading);
        job.failed_bytes = 1234;
        job.health = 875;
        let item = queue_item_from_job(&job);
        let attention = item.attention.expect("attention should exist");
        assert_eq!(attention.code, "UNHEALTHY_ARTICLES");
        assert!(attention.message.contains("1234"));
    }

    #[test]
    fn history_item_roundtrips_attributes_and_client_request_id() {
        let row = JobHistoryRow {
            job_id: 77,
            name: "History Test".to_string(),
            status: "failed".to_string(),
            error_message: Some("extract failed".to_string()),
            total_bytes: 2_000,
            downloaded_bytes: 1_500,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 500,
            health: 750,
            category: Some("tv".to_string()),
            output_dir: Some("/tmp/history".to_string()),
            nzb_path: None,
            created_at: 1_700_000_000_000,
            completed_at: 1_700_000_100_000,
            metadata: Some(
                serde_json::to_string(&vec![
                    ("source".to_string(), "rss".to_string()),
                    (
                        CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                        "req-history".to_string(),
                    ),
                ])
                .unwrap(),
            ),
        };

        let item = history_item_from_row(&row);
        assert_eq!(item.state, QueueItemState::Failed);
        assert_eq!(item.client_request_id.as_deref(), Some("req-history"));
        assert_eq!(item.attributes.len(), 1);
        assert_eq!(item.attributes[0].key, "source");
        assert_eq!(item.attributes[0].value, "rss");
        assert_eq!(item.attention.unwrap().code, "JOB_FAILED");
    }
}
