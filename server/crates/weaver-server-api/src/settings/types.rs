use async_graphql::{Enum, InputObject, MaybeUndefined, SimpleObject};
use serde::{Deserialize, Serialize};
use weaver_server_core::bandwidth::{
    IspBandwidthCapConfig, IspBandwidthCapPeriod, IspBandwidthCapWeekday,
};

#[derive(Debug, Clone, SimpleObject)]
pub struct GeneralSettings {
    pub data_dir: String,
    pub intermediate_dir: String,
    pub complete_dir: String,
    pub cleanup_after_extract: bool,
    pub max_download_speed: u64,
    pub max_retries: u32,
    pub ip_replacement_trial_extra_connections: u8,
    pub isp_bandwidth_cap: Option<IspBandwidthCapSettings>,
    pub watch_folder: WatchFolderSettings,
}

#[derive(Debug, InputObject)]
pub struct GeneralSettingsInput {
    pub intermediate_dir: MaybeUndefined<String>,
    pub complete_dir: MaybeUndefined<String>,
    pub cleanup_after_extract: Option<bool>,
    pub max_download_speed: Option<u64>,
    pub max_retries: Option<u32>,
    pub ip_replacement_trial_extra_connections: Option<u8>,
    pub isp_bandwidth_cap: Option<IspBandwidthCapSettingsInput>,
    pub watch_folder: Option<WatchFolderSettingsInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, SimpleObject)]
pub struct WatchFolderSettings {
    pub mode: String,
    pub path: Option<String>,
    pub poll_interval_secs: u64,
    pub stability_secs: u64,
    pub category_from_subfolders: bool,
    pub scanning_paused: bool,
}

impl From<&weaver_server_core::watch_folder::WatchFolderConfig> for WatchFolderSettings {
    fn from(value: &weaver_server_core::watch_folder::WatchFolderConfig) -> Self {
        Self {
            mode: value.mode.as_str().to_string(),
            path: value.path.clone(),
            poll_interval_secs: value.poll_interval_secs,
            stability_secs: value.stability_secs,
            category_from_subfolders: value.category_from_subfolders,
            scanning_paused: value.scanning_paused,
        }
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct WatchFolderSettingsInput {
    pub mode: Option<String>,
    pub path: MaybeUndefined<String>,
    pub poll_interval_secs: Option<u64>,
    pub stability_secs: Option<u64>,
    pub category_from_subfolders: Option<bool>,
    pub scanning_paused: Option<bool>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct WatchFolderScanIssue {
    pub path: String,
    pub reason: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct WatchFolderMarkerRename {
    pub from: String,
    pub to: String,
    pub marker: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct WatchFolderScanReport {
    pub discovered_files: Vec<String>,
    pub queued_nzbs: Vec<String>,
    pub skipped_inputs: Vec<WatchFolderScanIssue>,
    pub permanent_errors: Vec<WatchFolderScanIssue>,
    pub transient_errors: Vec<WatchFolderScanIssue>,
    pub marker_renamed_sources: Vec<WatchFolderMarkerRename>,
}

impl From<weaver_server_core::watch_folder::WatchFolderScanReport> for WatchFolderScanReport {
    fn from(value: weaver_server_core::watch_folder::WatchFolderScanReport) -> Self {
        Self {
            discovered_files: value.discovered_files,
            queued_nzbs: value.queued_nzbs,
            skipped_inputs: value
                .skipped_inputs
                .into_iter()
                .map(WatchFolderScanIssue::from)
                .collect(),
            permanent_errors: value
                .permanent_errors
                .into_iter()
                .map(WatchFolderScanIssue::from)
                .collect(),
            transient_errors: value
                .transient_errors
                .into_iter()
                .map(WatchFolderScanIssue::from)
                .collect(),
            marker_renamed_sources: value
                .marker_renamed_sources
                .into_iter()
                .map(
                    <WatchFolderMarkerRename as From<
                        weaver_server_core::watch_folder::MarkerRename,
                    >>::from,
                )
                .collect(),
        }
    }
}

impl From<weaver_server_core::watch_folder::ScanIssue> for WatchFolderScanIssue {
    fn from(value: weaver_server_core::watch_folder::ScanIssue) -> Self {
        Self {
            path: value.path,
            reason: value.reason,
        }
    }
}

impl From<weaver_server_core::watch_folder::MarkerRename> for WatchFolderMarkerRename {
    fn from(value: weaver_server_core::watch_folder::MarkerRename) -> Self {
        Self {
            from: value.from,
            to: value.to,
            marker: value.marker,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SimpleObject)]
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

impl From<weaver_server_core::bandwidth::ScheduleEntry> for Schedule {
    fn from(e: weaver_server_core::bandwidth::ScheduleEntry) -> Self {
        let (action_type, speed_limit_bytes) = match &e.action {
            weaver_server_core::bandwidth::ScheduleAction::Pause => ("pause".into(), None),
            weaver_server_core::bandwidth::ScheduleAction::Resume => ("resume".into(), None),
            weaver_server_core::bandwidth::ScheduleAction::PauseWatchFolderScanning => {
                ("pause_watch_folder_scanning".into(), None)
            }
            weaver_server_core::bandwidth::ScheduleAction::ResumeWatchFolderScanning => {
                ("resume_watch_folder_scanning".into(), None)
            }
            weaver_server_core::bandwidth::ScheduleAction::SpeedLimit { bytes_per_sec } => {
                ("speed_limit".into(), Some(*bytes_per_sec))
            }
        };
        Self {
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
    pub fn into_entry(self) -> weaver_server_core::bandwidth::ScheduleEntry {
        use weaver_server_core::bandwidth::{ScheduleAction, Weekday};

        let action = match self.action_type.as_str() {
            "pause" => ScheduleAction::Pause,
            "resume" => ScheduleAction::Resume,
            "pause_watch_folder_scanning" => ScheduleAction::PauseWatchFolderScanning,
            "resume_watch_folder_scanning" => ScheduleAction::ResumeWatchFolderScanning,
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
        weaver_server_core::bandwidth::ScheduleEntry {
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
