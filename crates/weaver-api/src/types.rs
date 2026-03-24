use async_graphql::{Enum, InputObject, OneofObject, SimpleObject};
use serde::{Deserialize, Serialize};
use weaver_core::config::{IspBandwidthCapConfig, IspBandwidthCapPeriod, IspBandwidthCapWeekday};
use weaver_core::release_name::{derive_release_name, original_release_title, parse_job_release};
use weaver_scheduler::handle::{DownloadBlockKind, DownloadBlockState};

// --- API Key types ---

/// Scope level for API keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum ApiKeyScope {
    Integration,
    Admin,
}

/// An API key entry (never includes the raw key).
#[derive(Debug, Clone, SimpleObject)]
pub struct ApiKey {
    pub id: i64,
    pub name: String,
    pub scope: ApiKeyScope,
    pub created_at: f64,
    pub last_used_at: Option<f64>,
}

/// Result of creating an API key — includes the raw key once.
#[derive(Debug, Clone, SimpleObject)]
pub struct CreateApiKeyResult {
    pub key: ApiKey,
    /// The raw API key. Only shown once at creation time.
    pub raw_key: String,
}

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
    pub priority: u32,
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
            priority: s.priority,
        }
    }
}

impl From<weaver_state::RssRuleAction> for RssRuleActionGql {
    fn from(value: weaver_state::RssRuleAction) -> Self {
        match value {
            weaver_state::RssRuleAction::Accept => Self::Accept,
            weaver_state::RssRuleAction::Reject => Self::Reject,
        }
    }
}

impl From<RssRuleActionGql> for weaver_state::RssRuleAction {
    fn from(value: RssRuleActionGql) -> Self {
        match value {
            RssRuleActionGql::Accept => Self::Accept,
            RssRuleActionGql::Reject => Self::Reject,
        }
    }
}

impl RssRule {
    pub fn from_row(rule: &weaver_state::RssRuleRow) -> Self {
        Self {
            id: rule.id,
            feed_id: rule.feed_id,
            sort_order: rule.sort_order,
            enabled: rule.enabled,
            action: rule.action.into(),
            title_regex: rule.title_regex.clone(),
            item_categories: rule.item_categories.clone(),
            min_size_bytes: rule.min_size_bytes,
            max_size_bytes: rule.max_size_bytes,
            category_override: rule.category_override.clone(),
            metadata: rule
                .metadata
                .iter()
                .map(|(key, value)| MetadataEntry {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }
}

impl RssFeed {
    pub fn from_row(feed: &weaver_state::RssFeedRow, rules: Vec<RssRule>) -> Self {
        Self {
            id: feed.id,
            name: feed.name.clone(),
            url: feed.url.clone(),
            enabled: feed.enabled,
            poll_interval_secs: feed.poll_interval_secs,
            username: feed.username.clone(),
            has_password: feed.password.is_some(),
            default_category: feed.default_category.clone(),
            default_metadata: feed
                .default_metadata
                .iter()
                .map(|(key, value)| MetadataEntry {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
            etag: feed.etag.clone(),
            last_modified: feed.last_modified.clone(),
            last_polled_at: feed.last_polled_at.map(|value| value as f64 * 1000.0),
            last_success_at: feed.last_success_at.map(|value| value as f64 * 1000.0),
            last_error: feed.last_error.clone(),
            consecutive_failures: feed.consecutive_failures,
            rules,
        }
    }
}

impl RssSeenItem {
    pub fn from_row(item: &weaver_state::RssSeenItemRow) -> Self {
        Self {
            feed_id: item.feed_id,
            item_id: item.item_id.clone(),
            item_title: item.item_title.clone(),
            published_at: item.published_at.map(|value| value as f64 * 1000.0),
            size_bytes: item.size_bytes,
            decision: item.decision.clone(),
            seen_at: item.seen_at as f64 * 1000.0,
            job_id: item.job_id,
            item_url: item.item_url.clone(),
            error: item.error.clone(),
        }
    }
}

impl RssSyncReport {
    pub fn from_domain(report: &crate::rss::RssSyncReport) -> Self {
        Self {
            feeds_polled: report.feeds_polled,
            items_fetched: report.items_fetched,
            items_new: report.items_new,
            items_accepted: report.items_accepted,
            items_submitted: report.items_submitted,
            items_ignored: report.items_ignored,
            errors: report.errors.clone(),
            feed_results: report
                .feed_results
                .iter()
                .map(|feed| RssFeedSyncResult {
                    feed_id: feed.feed_id,
                    feed_name: feed.feed_name.clone(),
                    items_fetched: feed.items_fetched,
                    items_new: feed.items_new,
                    items_accepted: feed.items_accepted,
                    items_submitted: feed.items_submitted,
                    items_ignored: feed.items_ignored,
                    errors: feed.errors.clone(),
                })
                .collect(),
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
    /// Priority group (0 = primary, 1+ = backfill). Lower values tried first.
    #[graphql(default = 0)]
    pub priority: u16,
}

/// GraphQL representation of a configured category.
#[derive(Debug, Clone, SimpleObject)]
pub struct Category {
    pub id: u32,
    pub name: String,
    pub dest_dir: Option<String>,
    pub aliases: String,
}

impl From<&weaver_core::config::CategoryConfig> for Category {
    fn from(c: &weaver_core::config::CategoryConfig) -> Self {
        Category {
            id: c.id,
            name: c.name.clone(),
            dest_dir: c.dest_dir.clone(),
            aliases: c.aliases.clone(),
        }
    }
}

/// Input for creating or updating a category.
#[derive(Debug, InputObject)]
pub struct CategoryInput {
    pub name: String,
    pub dest_dir: Option<String>,
    #[graphql(default)]
    pub aliases: String,
}

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
pub struct ServiceLogsPayload {
    pub lines: Vec<String>,
    pub count: i32,
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
    pub isp_bandwidth_cap: Option<IspBandwidthCapSettings>,
}

/// Input for updating general settings.
#[derive(Debug, InputObject)]
pub struct GeneralSettingsInput {
    pub intermediate_dir: Option<String>,
    pub complete_dir: Option<String>,
    pub cleanup_after_extract: Option<bool>,
    pub max_download_speed: Option<u64>,
    pub max_retries: Option<u32>,
    pub isp_bandwidth_cap: Option<IspBandwidthCapSettingsInput>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum IspBandwidthCapPeriodGql {
    Daily,
    Weekly,
    Monthly,
}

impl From<IspBandwidthCapPeriod> for IspBandwidthCapPeriodGql {
    fn from(value: IspBandwidthCapPeriod) -> Self {
        match value {
            IspBandwidthCapPeriod::Daily => Self::Daily,
            IspBandwidthCapPeriod::Weekly => Self::Weekly,
            IspBandwidthCapPeriod::Monthly => Self::Monthly,
        }
    }
}

impl From<IspBandwidthCapPeriodGql> for IspBandwidthCapPeriod {
    fn from(value: IspBandwidthCapPeriodGql) -> Self {
        match value {
            IspBandwidthCapPeriodGql::Daily => Self::Daily,
            IspBandwidthCapPeriodGql::Weekly => Self::Weekly,
            IspBandwidthCapPeriodGql::Monthly => Self::Monthly,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum IspBandwidthCapWeekdayGql {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

impl From<IspBandwidthCapWeekday> for IspBandwidthCapWeekdayGql {
    fn from(value: IspBandwidthCapWeekday) -> Self {
        match value {
            IspBandwidthCapWeekday::Mon => Self::Mon,
            IspBandwidthCapWeekday::Tue => Self::Tue,
            IspBandwidthCapWeekday::Wed => Self::Wed,
            IspBandwidthCapWeekday::Thu => Self::Thu,
            IspBandwidthCapWeekday::Fri => Self::Fri,
            IspBandwidthCapWeekday::Sat => Self::Sat,
            IspBandwidthCapWeekday::Sun => Self::Sun,
        }
    }
}

impl From<IspBandwidthCapWeekdayGql> for IspBandwidthCapWeekday {
    fn from(value: IspBandwidthCapWeekdayGql) -> Self {
        match value {
            IspBandwidthCapWeekdayGql::Mon => Self::Mon,
            IspBandwidthCapWeekdayGql::Tue => Self::Tue,
            IspBandwidthCapWeekdayGql::Wed => Self::Wed,
            IspBandwidthCapWeekdayGql::Thu => Self::Thu,
            IspBandwidthCapWeekdayGql::Fri => Self::Fri,
            IspBandwidthCapWeekdayGql::Sat => Self::Sat,
            IspBandwidthCapWeekdayGql::Sun => Self::Sun,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct IspBandwidthCapSettings {
    pub enabled: bool,
    pub period: IspBandwidthCapPeriodGql,
    pub limit_bytes: u64,
    pub reset_time_minutes_local: u16,
    pub weekly_reset_weekday: IspBandwidthCapWeekdayGql,
    pub monthly_reset_day: u8,
}

impl From<&IspBandwidthCapConfig> for IspBandwidthCapSettings {
    fn from(value: &IspBandwidthCapConfig) -> Self {
        Self {
            enabled: value.enabled,
            period: value.period.into(),
            limit_bytes: value.limit_bytes,
            reset_time_minutes_local: value.reset_time_minutes_local,
            weekly_reset_weekday: value.weekly_reset_weekday.into(),
            monthly_reset_day: value.monthly_reset_day,
        }
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct IspBandwidthCapSettingsInput {
    pub enabled: bool,
    pub period: IspBandwidthCapPeriodGql,
    pub limit_bytes: u64,
    pub reset_time_minutes_local: u16,
    pub weekly_reset_weekday: IspBandwidthCapWeekdayGql,
    pub monthly_reset_day: u8,
}

impl From<IspBandwidthCapSettingsInput> for IspBandwidthCapConfig {
    fn from(value: IspBandwidthCapSettingsInput) -> Self {
        Self {
            enabled: value.enabled,
            period: value.period.into(),
            limit_bytes: value.limit_bytes,
            reset_time_minutes_local: value.reset_time_minutes_local,
            weekly_reset_weekday: value.weekly_reset_weekday.into(),
            monthly_reset_day: value.monthly_reset_day,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
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

#[derive(Debug, Clone, SimpleObject)]
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
    /// Speed limit imposed by the active schedule (0 = no scheduled limit).
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum RssRuleActionGql {
    Accept,
    Reject,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RssRule {
    pub id: u32,
    pub feed_id: u32,
    pub sort_order: i32,
    pub enabled: bool,
    pub action: RssRuleActionGql,
    pub title_regex: Option<String>,
    pub item_categories: Vec<String>,
    pub min_size_bytes: Option<u64>,
    pub max_size_bytes: Option<u64>,
    pub category_override: Option<String>,
    pub metadata: Vec<MetadataEntry>,
}

#[derive(Debug, InputObject)]
pub struct RssRuleInput {
    #[graphql(default = true)]
    pub enabled: bool,
    pub sort_order: i32,
    pub action: RssRuleActionGql,
    pub title_regex: Option<String>,
    pub item_categories: Option<Vec<String>>,
    pub min_size_bytes: Option<u64>,
    pub max_size_bytes: Option<u64>,
    pub category_override: Option<String>,
    pub metadata: Option<Vec<MetadataInput>>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RssFeed {
    pub id: u32,
    pub name: String,
    pub url: String,
    pub enabled: bool,
    pub poll_interval_secs: u32,
    pub username: Option<String>,
    pub has_password: bool,
    pub default_category: Option<String>,
    pub default_metadata: Vec<MetadataEntry>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub last_polled_at: Option<f64>,
    pub last_success_at: Option<f64>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    pub rules: Vec<RssRule>,
}

#[derive(Debug, InputObject)]
pub struct RssFeedInput {
    pub name: String,
    pub url: String,
    #[graphql(default = true)]
    pub enabled: bool,
    pub poll_interval_secs: Option<u32>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub default_category: Option<String>,
    pub default_metadata: Option<Vec<MetadataInput>>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RssFeedSyncResult {
    pub feed_id: u32,
    pub feed_name: String,
    pub items_fetched: u32,
    pub items_new: u32,
    pub items_accepted: u32,
    pub items_submitted: u32,
    pub items_ignored: u32,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RssSyncReport {
    pub feeds_polled: u32,
    pub items_fetched: u32,
    pub items_new: u32,
    pub items_accepted: u32,
    pub items_submitted: u32,
    pub items_ignored: u32,
    pub errors: Vec<String>,
    pub feed_results: Vec<RssFeedSyncResult>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RssSeenItem {
    pub feed_id: u32,
    pub item_id: String,
    pub item_title: String,
    pub published_at: Option<f64>,
    pub size_bytes: Option<u64>,
    pub decision: String,
    pub seen_at: f64,
    pub job_id: Option<u64>,
    pub item_url: Option<String>,
    pub error: Option<String>,
}

/// A key-value metadata entry attached to a job.
#[derive(Debug, Clone, SimpleObject)]
pub struct MetadataEntry {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ParsedEpisode {
    pub season: Option<u32>,
    pub episode_numbers: Vec<u32>,
    pub absolute_episode: Option<u32>,
    pub raw: Option<String>,
}

#[derive(Debug, Clone, SimpleObject)]
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

/// Input for attaching metadata to a job submission.
#[derive(Debug, InputObject)]
pub struct MetadataInput {
    pub key: String,
    pub value: String,
}

/// NZB source — either inline base64 content or a URL to fetch from.
#[derive(Debug, OneofObject)]
pub enum NzbSourceInput {
    /// Base64-encoded NZB file content.
    NzbBase64(String),
    /// URL to fetch the NZB from.
    Url(String),
}

/// GraphQL representation of a job.
#[derive(Debug, Clone, SimpleObject)]
pub struct Job {
    pub id: u64,
    pub name: String,
    pub display_title: String,
    pub original_title: String,
    pub parsed_release: ParsedRelease,
    pub status: JobStatusGql,
    /// Error message (only set when status is FAILED).
    pub error: Option<String>,
    pub progress: f64,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    /// Bytes from segments that are permanently lost (430 / max retries).
    pub failed_bytes: u64,
    /// Job health 0-1000 (1000 = perfect). Drops as articles fail.
    pub health: u32,
    pub has_password: bool,
    pub category: Option<String>,
    pub metadata: Vec<MetadataEntry>,
    pub output_dir: Option<String>,
    /// Wall-clock creation time (Unix epoch milliseconds).
    pub created_at: Option<f64>,
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
    Moving,
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

/// Event categories for subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum EventKind {
    JobCreated,
    JobPaused,
    JobResumed,
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

// --- Conversion from domain types ---

impl From<&weaver_scheduler::JobInfo> for Job {
    fn from(info: &weaver_scheduler::JobInfo) -> Self {
        let original_title = original_release_title(&info.name, &info.metadata);
        let display_title = derive_release_name(Some(&original_title), Some(&info.name));
        let parsed_release = ParsedRelease::from(parse_job_release(&info.name, &info.metadata));

        Job {
            id: info.job_id.0,
            name: info.name.clone(),
            display_title,
            original_title,
            parsed_release,
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

impl From<scryer_release_parser::ParsedEpisodeMetadata> for ParsedEpisode {
    fn from(value: scryer_release_parser::ParsedEpisodeMetadata) -> Self {
        Self {
            season: value.season,
            episode_numbers: value.episode_numbers,
            absolute_episode: value.absolute_episode,
            raw: value.raw,
        }
    }
}

impl From<scryer_release_parser::ParsedReleaseMetadata> for ParsedRelease {
    fn from(value: scryer_release_parser::ParsedReleaseMetadata) -> Self {
        Self {
            normalized_title: value.normalized_title,
            release_group: value.release_group,
            languages_audio: value.languages_audio,
            languages_subtitles: value.languages_subtitles,
            year: value.year,
            quality: value.quality,
            source: value.source,
            video_codec: value.video_codec,
            video_encoding: value.video_encoding,
            audio: value.audio,
            audio_codecs: value.audio_codecs,
            audio_channels: value.audio_channels,
            is_dual_audio: value.is_dual_audio,
            is_atmos: value.is_atmos,
            is_dolby_vision: value.is_dolby_vision,
            detected_hdr: value.detected_hdr,
            is_hdr10plus: value.is_hdr10plus,
            is_hlg: value.is_hlg,
            fps: value.fps,
            is_proper_upload: value.is_proper_upload,
            is_repack: value.is_repack,
            is_remux: value.is_remux,
            is_bd_disk: value.is_bd_disk,
            is_ai_enhanced: value.is_ai_enhanced,
            is_hardcoded_subs: value.is_hardcoded_subs,
            streaming_service: value.streaming_service,
            edition: value.edition,
            anime_version: value.anime_version,
            episode: value.episode.map(ParsedEpisode::from),
            parse_confidence: value.parse_confidence,
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
            weaver_scheduler::JobStatus::Moving => JobStatusGql::Moving,
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

impl From<&weaver_core::event::PipelineEvent> for PipelineEventGql {
    fn from(event: &weaver_core::event::PipelineEvent) -> Self {
        use weaver_core::event::PipelineEvent;

        match event {
            PipelineEvent::JobCreated {
                job_id,
                name,
                total_files,
                total_bytes,
            } => PipelineEventGql {
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
            PipelineEvent::DownloadStarted { job_id } => PipelineEventGql {
                kind: EventKind::DownloadStarted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "download started".into(),
            },
            PipelineEvent::DownloadFinished { job_id } => PipelineEventGql {
                kind: EventKind::DownloadFinished,
                job_id: Some(job_id.0),
                file_id: None,
                message: "download finished".into(),
            },
            PipelineEvent::ArticleDownloaded {
                segment_id,
                raw_size,
            } => PipelineEventGql {
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
            PipelineEvent::SegmentDecoded {
                segment_id,
                decoded_size,
                crc_valid,
                ..
            } => PipelineEventGql {
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
            PipelineEvent::FileComplete {
                file_id,
                filename,
                total_bytes,
            } => PipelineEventGql {
                kind: EventKind::FileComplete,
                job_id: Some(file_id.job_id.0),
                file_id: Some(format!("{file_id}")),
                message: format!("{filename}: {total_bytes} bytes"),
            },
            PipelineEvent::FileMissing {
                file_id,
                filename,
                missing_segments,
            } => PipelineEventGql {
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
            PipelineEvent::JobVerificationStarted { job_id } => PipelineEventGql {
                kind: EventKind::JobVerificationStarted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "verification started".into(),
            },
            PipelineEvent::JobVerificationComplete { job_id, passed } => PipelineEventGql {
                kind: EventKind::JobVerificationComplete,
                job_id: Some(job_id.0),
                file_id: None,
                message: if *passed {
                    "verification passed".into()
                } else {
                    "verification found damage".into()
                },
            },
            PipelineEvent::RepairStarted { job_id } => PipelineEventGql {
                kind: EventKind::RepairStarted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "repair started".into(),
            },
            PipelineEvent::RepairComplete {
                job_id,
                slices_repaired,
            } => PipelineEventGql {
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
            PipelineEvent::ExtractionMemberStarted {
                job_id,
                set_name,
                member,
            } => PipelineEventGql {
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
            } => PipelineEventGql {
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
            } => PipelineEventGql {
                kind: EventKind::ExtractionMemberWaitingFinished,
                job_id: Some(job_id.0),
                file_id: encode_timeline_member_subject(set_name, member, Some(*volume_index)),
                message: format!("{set_name}: {member} resumed with volume {volume_index}"),
            },
            PipelineEvent::ExtractionMemberAppendStarted {
                job_id,
                set_name,
                member,
            } => PipelineEventGql {
                kind: EventKind::ExtractionMemberAppendStarted,
                job_id: Some(job_id.0),
                file_id: encode_timeline_member_subject(set_name, member, None),
                message: format!("{set_name}: {member}"),
            },
            PipelineEvent::ExtractionMemberAppendFinished {
                job_id,
                set_name,
                member,
            } => PipelineEventGql {
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
            } => PipelineEventGql {
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
            PipelineEvent::ExtractionMemberFinished {
                job_id,
                set_name,
                member,
            } => PipelineEventGql {
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
            } => PipelineEventGql {
                kind: EventKind::ExtractionMemberFailed,
                job_id: Some(job_id.0),
                file_id: encode_timeline_member_subject(set_name, member, None),
                message: format!("{set_name}: {member} — {error}"),
            },
            PipelineEvent::ExtractionFailed { job_id, error } => PipelineEventGql {
                kind: EventKind::ExtractionFailed,
                job_id: Some(job_id.0),
                file_id: None,
                message: error.clone(),
            },
            PipelineEvent::MoveToCompleteStarted { job_id } => PipelineEventGql {
                kind: EventKind::MoveToCompleteStarted,
                job_id: Some(job_id.0),
                file_id: None,
                message: "move to complete started".into(),
            },
            PipelineEvent::MoveToCompleteFinished { job_id } => PipelineEventGql {
                kind: EventKind::MoveToCompleteFinished,
                job_id: Some(job_id.0),
                file_id: None,
                message: "move to complete finished".into(),
            },
            PipelineEvent::SegmentRetryScheduled {
                segment_id,
                attempt,
                delay_secs,
            } => PipelineEventGql {
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

// ── Schedules ───────────────────────────────────────────────────────────────

/// A schedule entry as returned by GraphQL queries.
#[derive(SimpleObject)]
pub struct Schedule {
    pub id: String,
    pub enabled: bool,
    pub label: String,
    pub days: Vec<String>,
    pub time: String,
    pub action_type: String,
    pub speed_limit_bytes: Option<u64>,
}

impl From<weaver_core::config::ScheduleEntry> for Schedule {
    fn from(e: weaver_core::config::ScheduleEntry) -> Self {
        let (action_type, speed_limit_bytes) = match &e.action {
            weaver_core::config::ScheduleAction::Pause => ("pause".into(), None),
            weaver_core::config::ScheduleAction::Resume => ("resume".into(), None),
            weaver_core::config::ScheduleAction::SpeedLimit { bytes_per_sec } => {
                ("speed_limit".into(), Some(*bytes_per_sec))
            }
        };
        Schedule {
            id: e.id,
            enabled: e.enabled,
            label: e.label,
            days: e
                .days
                .iter()
                .map(|d| format!("{d:?}").to_lowercase())
                .collect(),
            time: e.time,
            action_type,
            speed_limit_bytes,
        }
    }
}

/// Input type for creating/updating a schedule entry.
#[derive(InputObject)]
pub struct ScheduleInput {
    pub enabled: Option<bool>,
    pub label: Option<String>,
    pub days: Option<Vec<String>>,
    pub time: String,
    pub action_type: String,
    pub speed_limit_bytes: Option<u64>,
}

impl ScheduleInput {
    pub fn into_entry(self) -> weaver_core::config::ScheduleEntry {
        use weaver_core::config::{ScheduleAction, Weekday};
        let action = match self.action_type.as_str() {
            "pause" => ScheduleAction::Pause,
            "resume" => ScheduleAction::Resume,
            "speed_limit" => ScheduleAction::SpeedLimit {
                bytes_per_sec: self.speed_limit_bytes.unwrap_or(0),
            },
            _ => ScheduleAction::Resume,
        };
        let days: Vec<Weekday> = self
            .days
            .unwrap_or_default()
            .iter()
            .filter_map(|d| match d.to_lowercase().as_str() {
                "mon" => Some(Weekday::Mon),
                "tue" => Some(Weekday::Tue),
                "wed" => Some(Weekday::Wed),
                "thu" => Some(Weekday::Thu),
                "fri" => Some(Weekday::Fri),
                "sat" => Some(Weekday::Sat),
                "sun" => Some(Weekday::Sun),
                _ => None,
            })
            .collect();
        weaver_core::config::ScheduleEntry {
            id: format!(
                "sched-{:x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            ),
            enabled: self.enabled.unwrap_or(true),
            label: self.label.unwrap_or_default(),
            days,
            time: self.time,
            action,
        }
    }
}
