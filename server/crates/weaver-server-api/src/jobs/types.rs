use async_graphql::{Enum, InputObject, SimpleObject, Upload};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use weaver_server_core::jobs::handle::DownloadBlockState;
use weaver_server_core::operations::metrics::MetricsSnapshot;

use super::release_display::{ReleaseDisplayInput, release_display_info};
use crate::system::types::{DownloadBlock, Metrics};

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

/// Public queue states collapse the internal `Checking` phase into
/// `Verifying` so clients see one post-download verification step before
/// extraction begins.
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

impl From<&weaver_server_core::JobStatus> for QueueItemState {
    fn from(value: &weaver_server_core::JobStatus) -> Self {
        match value {
            weaver_server_core::JobStatus::Queued
            | weaver_server_core::JobStatus::QueuedRepair
            | weaver_server_core::JobStatus::QueuedExtract => Self::Queued,
            weaver_server_core::JobStatus::Downloading => Self::Downloading,
            weaver_server_core::JobStatus::Checking => Self::Verifying,
            weaver_server_core::JobStatus::Verifying => Self::Verifying,
            weaver_server_core::JobStatus::Repairing => Self::Repairing,
            weaver_server_core::JobStatus::Extracting => Self::Extracting,
            weaver_server_core::JobStatus::Moving => Self::Finalizing,
            weaver_server_core::JobStatus::Complete => Self::Completed,
            weaver_server_core::JobStatus::Failed { .. } => Self::Failed,
            weaver_server_core::JobStatus::Paused => Self::Paused,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Enum)]
pub enum QueueDownloadState {
    Queued,
    Downloading,
    Checking,
    Complete,
    Failed,
}

impl From<weaver_server_core::DownloadState> for QueueDownloadState {
    fn from(value: weaver_server_core::DownloadState) -> Self {
        match value {
            weaver_server_core::DownloadState::Queued => Self::Queued,
            weaver_server_core::DownloadState::Downloading => Self::Downloading,
            weaver_server_core::DownloadState::Checking => Self::Checking,
            weaver_server_core::DownloadState::Complete => Self::Complete,
            weaver_server_core::DownloadState::Failed => Self::Failed,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Enum)]
pub enum QueuePostState {
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

impl From<weaver_server_core::PostState> for QueuePostState {
    fn from(value: weaver_server_core::PostState) -> Self {
        match value {
            weaver_server_core::PostState::Idle => Self::Idle,
            weaver_server_core::PostState::QueuedRepair => Self::QueuedRepair,
            weaver_server_core::PostState::Repairing => Self::Repairing,
            weaver_server_core::PostState::QueuedExtract => Self::QueuedExtract,
            weaver_server_core::PostState::Extracting => Self::Extracting,
            weaver_server_core::PostState::WaitingForVolumes => Self::WaitingForVolumes,
            weaver_server_core::PostState::AwaitingRepair => Self::AwaitingRepair,
            weaver_server_core::PostState::Verifying => Self::Verifying,
            weaver_server_core::PostState::Finalizing => Self::Finalizing,
            weaver_server_core::PostState::Completed => Self::Completed,
            weaver_server_core::PostState::Failed => Self::Failed,
        }
    }
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
    #[serde(default = "default_queue_download_state")]
    pub download_state: QueueDownloadState,
    #[serde(default = "default_queue_post_state")]
    pub post_state: QueuePostState,
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

fn default_queue_download_state() -> QueueDownloadState {
    QueueDownloadState::Downloading
}

fn default_queue_post_state() -> QueuePostState {
    QueuePostState::Idle
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, InputObject, Default)]
pub struct QueueFilterInput {
    pub item_ids: Option<Vec<u64>>,
    pub states: Option<Vec<QueueItemState>>,
    pub category: Option<String>,
    pub has_attribute_key: Option<String>,
    pub attribute_equals: Option<AttributeInput>,
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

#[derive(Debug, Clone, SimpleObject)]
pub struct JobOutputFile {
    pub name: String,
    pub path: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct JobOutputResult {
    pub output_dir: String,
    pub files: Vec<JobOutputFile>,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct MetadataEntry {
    pub key: String,
    pub value: String,
}

#[derive(Debug, InputObject)]
pub struct MetadataInput {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct ParsedEpisode {
    pub season: Option<u32>,
    pub episode_numbers: Vec<u32>,
    pub absolute_episode: Option<u32>,
    pub raw: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, SimpleObject)]
pub struct ParsedRelease {
    pub normalized_title: String,
    pub release_group: Option<String>,
    pub languages_audio: Vec<String>,
    pub languages_subtitles: Vec<String>,
    pub year: Option<u32>,
    pub quality: Option<String>,
    pub source: Option<String>,
    pub video_codec: Option<String>,
    pub video_encoding: Option<String>,
    pub audio: Option<String>,
    pub audio_codecs: Vec<String>,
    pub audio_channels: Option<String>,
    pub is_dual_audio: bool,
    pub is_atmos: bool,
    pub is_dolby_vision: bool,
    pub detected_hdr: bool,
    pub is_hdr10plus: bool,
    pub is_hlg: bool,
    pub fps: Option<f32>,
    pub is_proper_upload: bool,
    pub is_repack: bool,
    pub is_remux: bool,
    pub is_bd_disk: bool,
    pub is_ai_enhanced: bool,
    pub is_hardcoded_subs: bool,
    pub streaming_service: Option<String>,
    pub edition: Option<String>,
    pub anime_version: Option<u32>,
    pub episode: Option<ParsedEpisode>,
    pub parse_confidence: f32,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct Job {
    pub id: u64,
    pub name: String,
    pub display_title: String,
    pub original_title: String,
    pub parsed_release: ParsedRelease,
    pub status: JobStatusGql,
    pub error: Option<String>,
    pub progress: f64,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    pub failed_bytes: u64,
    pub health: u32,
    pub has_password: bool,
    pub category: Option<String>,
    pub metadata: Vec<MetadataEntry>,
    pub output_dir: Option<String>,
    pub created_at: Option<f64>,
}

impl From<&weaver_server_core::JobInfo> for Job {
    fn from(info: &weaver_server_core::JobInfo) -> Self {
        let display = release_display_info(ReleaseDisplayInput {
            job_name: &info.name,
            metadata: &info.metadata,
            category: info.category.as_deref(),
        });

        Self {
            id: info.job_id.0,
            name: info.name.clone(),
            display_title: display.display_title,
            original_title: display.original_title,
            parsed_release: display.parsed_release,
            status: JobStatusGql::from(&info.status),
            error: info.error.clone(),
            progress: info.progress,
            total_bytes: info.total_bytes,
            downloaded_bytes: info.downloaded_bytes,
            optional_recovery_bytes: info.optional_recovery_bytes,
            optional_recovery_downloaded_bytes: info.optional_recovery_downloaded_bytes,
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
            created_at: Some(info.created_at_epoch_ms),
        }
    }
}

/// GraphQL job statuses collapse the internal `Checking` phase into
/// `Verifying` so clients see one post-download verification step before
/// extraction begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum JobStatusGql {
    Queued,
    Downloading,
    Checking,
    Verifying,
    QueuedRepair,
    Repairing,
    QueuedExtract,
    Extracting,
    Moving,
    Complete,
    Failed,
    Paused,
}

impl From<&weaver_server_core::JobStatus> for JobStatusGql {
    fn from(status: &weaver_server_core::JobStatus) -> Self {
        match status {
            weaver_server_core::JobStatus::Queued => Self::Queued,
            weaver_server_core::JobStatus::Downloading => Self::Downloading,
            weaver_server_core::JobStatus::Checking => Self::Verifying,
            weaver_server_core::JobStatus::Verifying => Self::Verifying,
            weaver_server_core::JobStatus::QueuedRepair => Self::QueuedRepair,
            weaver_server_core::JobStatus::Repairing => Self::Repairing,
            weaver_server_core::JobStatus::QueuedExtract => Self::QueuedExtract,
            weaver_server_core::JobStatus::Extracting => Self::Extracting,
            weaver_server_core::JobStatus::Moving => Self::Moving,
            weaver_server_core::JobStatus::Complete => Self::Complete,
            weaver_server_core::JobStatus::Failed { .. } => Self::Failed,
            weaver_server_core::JobStatus::Paused => Self::Paused,
        }
    }
}

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

pub const PRIORITY_ATTRIBUTE_KEY: &str = "priority";

pub fn normalize_priority_value(value: &str) -> std::result::Result<String, String> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("high") {
        return Ok("HIGH".to_string());
    }
    if trimmed.eq_ignore_ascii_case("normal") {
        return Ok("NORMAL".to_string());
    }
    if trimmed.eq_ignore_ascii_case("low") {
        return Ok("LOW".to_string());
    }

    Err(format!(
        "invalid priority '{trimmed}'; expected HIGH, NORMAL, or LOW"
    ))
}

pub fn submit_metadata(
    attributes: Option<Vec<AttributeInput>>,
    client_request_id: Option<String>,
) -> std::result::Result<Vec<(String, String)>, String> {
    let mut metadata: Vec<(String, String)> = Vec::new();
    for entry in attributes.unwrap_or_default() {
        if entry.key.eq_ignore_ascii_case(PRIORITY_ATTRIBUTE_KEY) {
            metadata.retain(|(key, _)| !key.eq_ignore_ascii_case(PRIORITY_ATTRIBUTE_KEY));
            metadata.push((
                PRIORITY_ATTRIBUTE_KEY.to_string(),
                normalize_priority_value(&entry.value)?,
            ));
        } else {
            metadata.push((entry.key, entry.value));
        }
    }
    if let Some(client_request_id) = client_request_id.filter(|value| !value.trim().is_empty()) {
        metadata.push((
            CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
            client_request_id.trim().to_string(),
        ));
    }
    Ok(metadata)
}

pub fn queue_item_from_job(info: &weaver_server_core::JobInfo) -> QueueItem {
    let (client_request_id, attributes) = split_attributes(&info.metadata);
    let state = queue_item_state_from_job_info(info);
    let display = release_display_info(ReleaseDisplayInput {
        job_name: &info.name,
        metadata: &info.metadata,
        category: info.category.as_deref(),
    });
    QueueItem {
        id: info.job_id.0,
        name: info.name.clone(),
        display_title: display.display_title,
        original_title: display.original_title,
        parsed_release: display.parsed_release,
        state,
        download_state: QueueDownloadState::from(info.download_state),
        post_state: QueuePostState::from(info.post_state),
        wait_reason: wait_reason_for_post_state(info.post_state),
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

pub fn queue_item_from_submission(
    submitted: &weaver_server_core::ingest::SubmittedJob,
) -> QueueItem {
    let (client_request_id, attributes) = split_attributes(&submitted.spec.metadata);
    let display = release_display_info(ReleaseDisplayInput {
        job_name: &submitted.spec.name,
        metadata: &submitted.spec.metadata,
        category: submitted.spec.category.as_deref(),
    });
    QueueItem {
        id: submitted.job_id.0,
        name: submitted.spec.name.clone(),
        display_title: display.display_title,
        original_title: display.original_title,
        parsed_release: display.parsed_release,
        state: QueueItemState::Queued,
        download_state: QueueDownloadState::Queued,
        post_state: QueuePostState::Idle,
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

pub fn matches_queue_filter(item: &QueueItem, filter: Option<&QueueFilterInput>) -> bool {
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

    if filter.has_attribute_key.is_some() || filter.attribute_equals.is_some() {
        let Some(item) = &event.item else {
            return false;
        };
        if !matches_attribute_filter(&item.attributes, filter) {
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
            QueueItemState::Downloading | QueueItemState::Finalizing => {
                summary.active_items += 1;
            }
            QueueItemState::Checking => {
                summary.active_items += 1;
                summary.verifying_items += 1;
            }
            QueueItemState::Completed => {}
        }
    }

    summary
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

pub fn metrics_from_snapshot(snapshot: &MetricsSnapshot) -> Metrics {
    Metrics::from(snapshot)
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
    if !matches_attribute_filter(attributes, filter) {
        return false;
    }
    true
}

pub(crate) fn matches_attribute_filter(
    attributes: &[Attribute],
    filter: &QueueFilterInput,
) -> bool {
    if let Some(attribute_key) = &filter.has_attribute_key
        && !attributes.iter().any(|entry| entry.key == *attribute_key)
    {
        return false;
    }

    if let Some(attribute) = &filter.attribute_equals
        && !attributes
            .iter()
            .any(|entry| entry.key == attribute.key && entry.value == attribute.value)
    {
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

fn normalize_progress_percent(progress_ratio: f64) -> f64 {
    (progress_ratio * 100.0).clamp(0.0, 100.0)
}

fn ms_to_datetime(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

fn wait_reason_for_post_state(
    post_state: weaver_server_core::PostState,
) -> Option<QueueWaitReason> {
    match post_state {
        weaver_server_core::PostState::QueuedRepair => Some(QueueWaitReason::RepairCapacity),
        weaver_server_core::PostState::QueuedExtract => Some(QueueWaitReason::ExtractionCapacity),
        _ => None,
    }
}

fn queue_item_state_from_job_info(info: &weaver_server_core::JobInfo) -> QueueItemState {
    match &info.status {
        weaver_server_core::JobStatus::Paused => return QueueItemState::Paused,
        weaver_server_core::JobStatus::Failed { .. } => return QueueItemState::Failed,
        weaver_server_core::JobStatus::Complete => return QueueItemState::Completed,
        weaver_server_core::JobStatus::Moving => return QueueItemState::Finalizing,
        _ => {}
    }

    match info.download_state {
        weaver_server_core::DownloadState::Downloading => return QueueItemState::Downloading,
        weaver_server_core::DownloadState::Checking => return QueueItemState::Verifying,
        weaver_server_core::DownloadState::Queued => return QueueItemState::Queued,
        weaver_server_core::DownloadState::Failed => return QueueItemState::Failed,
        weaver_server_core::DownloadState::Complete => {}
    }

    match info.post_state {
        weaver_server_core::PostState::Verifying
        | weaver_server_core::PostState::AwaitingRepair => QueueItemState::Verifying,
        weaver_server_core::PostState::Repairing => QueueItemState::Repairing,
        weaver_server_core::PostState::Extracting => QueueItemState::Extracting,
        weaver_server_core::PostState::Finalizing => QueueItemState::Finalizing,
        weaver_server_core::PostState::Completed => QueueItemState::Completed,
        weaver_server_core::PostState::Failed => QueueItemState::Failed,
        weaver_server_core::PostState::QueuedRepair
        | weaver_server_core::PostState::QueuedExtract => QueueItemState::Queued,
        weaver_server_core::PostState::Idle | weaver_server_core::PostState::WaitingForVolumes => {
            QueueItemState::from(&info.status)
        }
    }
}

fn attention_for_live_job(
    info: &weaver_server_core::JobInfo,
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

    match info.post_state {
        weaver_server_core::PostState::WaitingForVolumes => {
            return Some(QueueAttention {
                code: "WAITING_FOR_VOLUMES".to_string(),
                message: "Waiting for additional archive volumes before extraction can continue"
                    .to_string(),
            });
        }
        weaver_server_core::PostState::AwaitingRepair => {
            return Some(QueueAttention {
                code: "AWAITING_REPAIR".to_string(),
                message: "Waiting for verification or repair before extraction can resume"
                    .to_string(),
            });
        }
        _ => {}
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

#[cfg(test)]
mod tests;
