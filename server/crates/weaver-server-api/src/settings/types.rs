use async_graphql::{Enum, InputObject, MaybeUndefined, SimpleObject};
use serde::{Deserialize, Serialize};
use weaver_server_core::bandwidth::{
    IspBandwidthCapConfig, IspBandwidthCapPeriod, IspBandwidthCapWeekday,
};
use weaver_server_core::jobs::DuplicatePolicy;
use weaver_server_core::runtime::HardwareProfile;
use weaver_server_core::runtime::system_profile::SystemProfile;

use crate::jobs::types::DuplicateActionGql;

/// How hard Weaver leans on the machine it runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum HardwareProfileGql {
    Efficient,
    Balanced,
    Performance,
}

impl From<HardwareProfile> for HardwareProfileGql {
    fn from(value: HardwareProfile) -> Self {
        match value {
            HardwareProfile::Efficient => Self::Efficient,
            HardwareProfile::Balanced => Self::Balanced,
            HardwareProfile::Performance => Self::Performance,
        }
    }
}

impl From<HardwareProfileGql> for HardwareProfile {
    fn from(value: HardwareProfileGql) -> Self {
        match value {
            HardwareProfileGql::Efficient => Self::Efficient,
            HardwareProfileGql::Balanced => Self::Balanced,
            HardwareProfileGql::Performance => Self::Performance,
        }
    }
}

/// What the machine this Weaver runs on can actually use, after any container
/// limit. The numbers the profile requirements are judged against.
#[derive(Debug, Clone, Copy, SimpleObject)]
pub struct DetectedHardware {
    pub memory_bytes: u64,
    pub cores: u32,
}

/// What one profile would do on this machine, so an interface can say what a
/// card means without keeping its own copy of the numbers.
#[derive(Debug, Clone, Copy, SimpleObject)]
pub struct HardwareProfileOption {
    pub profile: HardwareProfileGql,
    /// Memory one 7z extraction may hold while decoding.
    pub sevenz_decode_memory_bytes: u64,
    /// Articles decoded at once.
    pub decode_threads: u32,
    /// Threads shared by extraction, repair and post-processing.
    pub extract_threads: u32,
    /// Downloads in flight at once, or null when the configured connection
    /// count is the only limit.
    pub max_concurrent_downloads: Option<u32>,
}

impl HardwareProfileOption {
    fn resolve(profile: HardwareProfile, probe: &SystemProfile) -> Self {
        let tuning = profile.tuning(probe);
        Self {
            profile: profile.into(),
            sevenz_decode_memory_bytes: tuning.sevenz_decode_memory_bytes,
            decode_threads: tuning.decode_threads as u32,
            extract_threads: tuning.extract_threads as u32,
            max_concurrent_downloads: tuning.max_concurrent_downloads_cap.map(|cap| cap as u32),
        }
    }
}

/// The hardware-profile choice, and everything needed to present it: an
/// interface with one available profile has nothing to ask and hides the
/// question entirely.
#[derive(Debug, Clone, SimpleObject)]
pub struct HardwareProfileSettings {
    /// The operator's choice, or null when they have never made one and the
    /// recommendation is standing in.
    pub selected: Option<HardwareProfileGql>,
    /// The most capable profile this machine can honour.
    pub recommended: HardwareProfileGql,
    /// Every profile this machine can honour, least demanding first.
    pub available: Vec<HardwareProfileGql>,
    /// What each available profile would do here, in the same order.
    pub options: Vec<HardwareProfileOption>,
    pub detected: DetectedHardware,
    /// The profile whose limits are in force now: the scheduled one while a
    /// schedule rule has one in force, the choice or the recommendation
    /// otherwise.
    pub active: HardwareProfileGql,
    /// The profile a schedule rule has in force over the choice, or null when
    /// no rule does and the choice applies.
    pub scheduled: Option<HardwareProfileGql>,
}

impl HardwareProfileSettings {
    /// The whole answer, resolved against one probe of the machine.
    pub(crate) fn resolve(selected: Option<HardwareProfile>, probe: &SystemProfile) -> Self {
        let available = HardwareProfile::available(probe);
        Self {
            selected: selected.map(Into::into),
            recommended: HardwareProfile::recommended(probe).into(),
            options: available
                .iter()
                .map(|profile| HardwareProfileOption::resolve(*profile, probe))
                .collect(),
            available: available.into_iter().map(Into::into).collect(),
            detected: DetectedHardware {
                memory_bytes: HardwareProfile::effective_memory_bytes(probe),
                cores: HardwareProfile::effective_cores(probe) as u32,
            },
            active: selected
                .filter(|profile| profile.unmet_requirement(probe).is_none())
                .unwrap_or_else(|| HardwareProfile::recommended(probe))
                .into(),
            scheduled: None,
        }
    }

    /// The same answer with the profile the pipeline reports in force. Without
    /// a report, the choice or the recommendation is what is in force.
    pub(crate) fn with_in_force(
        mut self,
        in_force: Option<weaver_server_core::HardwareProfileInForce>,
    ) -> Self {
        if let Some(in_force) = in_force {
            self.active = in_force.active.into();
            self.scheduled = in_force.scheduled.map(Into::into);
        }
        self
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct GeneralSettings {
    pub data_dir: String,
    pub intermediate_dir: String,
    pub complete_dir: String,
    pub cleanup_after_extract: bool,
    pub max_download_speed: u64,
    pub max_retries: u32,
    pub propagation_delay_secs: u32,
    pub enable_srrdb_lookup: bool,
    pub isp_bandwidth_cap: Option<IspBandwidthCapSettings>,
    pub watch_folder: WatchFolderSettings,
    pub duplicate_policy: DuplicatePolicySettings,
}

#[derive(Debug, InputObject)]
pub struct GeneralSettingsInput {
    pub intermediate_dir: MaybeUndefined<String>,
    pub complete_dir: MaybeUndefined<String>,
    pub cleanup_after_extract: Option<bool>,
    pub max_download_speed: Option<u64>,
    pub max_retries: Option<u32>,
    pub propagation_delay_secs: Option<u32>,
    pub enable_srrdb_lookup: Option<bool>,
    pub isp_bandwidth_cap: Option<IspBandwidthCapSettingsInput>,
    pub watch_folder: Option<WatchFolderSettingsInput>,
    pub duplicate_policy: Option<DuplicatePolicySettingsInput>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SimpleObject)]
pub struct DuplicatePolicySettings {
    pub strict_active_or_success: DuplicateActionGql,
    pub strict_failed_or_cancelled: DuplicateActionGql,
    pub article_layout_active_or_success: DuplicateActionGql,
    pub article_layout_failed_or_cancelled: DuplicateActionGql,
    pub article_set: DuplicateActionGql,
    pub normalized_name: DuplicateActionGql,
}

impl From<DuplicatePolicy> for DuplicatePolicySettings {
    fn from(value: DuplicatePolicy) -> Self {
        Self {
            strict_active_or_success: value.strict_active_or_success.into(),
            strict_failed_or_cancelled: value.strict_failed_or_cancelled.into(),
            article_layout_active_or_success: value.article_layout_active_or_success.into(),
            article_layout_failed_or_cancelled: value.article_layout_failed_or_cancelled.into(),
            article_set: value.article_set.into(),
            normalized_name: value.normalized_name.into(),
        }
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct DuplicatePolicySettingsInput {
    pub strict_active_or_success: Option<DuplicateActionGql>,
    pub strict_failed_or_cancelled: Option<DuplicateActionGql>,
    pub article_layout_active_or_success: Option<DuplicateActionGql>,
    pub article_layout_failed_or_cancelled: Option<DuplicateActionGql>,
    pub article_set: Option<DuplicateActionGql>,
    pub normalized_name: Option<DuplicateActionGql>,
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
    /// The profile a `hardware_profile` rule puts in force; null for every
    /// other action.
    pub hardware_profile: Option<HardwareProfileGql>,
}

impl From<weaver_server_core::bandwidth::ScheduleEntry> for Schedule {
    fn from(e: weaver_server_core::bandwidth::ScheduleEntry) -> Self {
        let mut hardware_profile = None;
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
            weaver_server_core::bandwidth::ScheduleAction::HardwareProfile { profile } => {
                hardware_profile = Some((*profile).into());
                ("hardware_profile".into(), None)
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
            hardware_profile,
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
    /// Required when the action is `hardware_profile`, ignored otherwise.
    pub hardware_profile: Option<HardwareProfileGql>,
}

impl ScheduleInput {
    /// Refuse a rule [`Self::into_entry`] would not build as asked. A
    /// `hardware_profile` rule needs a profile, and one this machine can
    /// honour: a rule that could never apply is refused by name here rather
    /// than saved and skipped every time it fires.
    pub fn validate(&self, probe: &SystemProfile) -> Result<(), String> {
        if self.action_type != "hardware_profile" {
            return Ok(());
        }
        let Some(profile) = self.hardware_profile else {
            return Err("a hardware_profile schedule needs a hardwareProfile".to_string());
        };
        match HardwareProfile::from(profile).unmet_requirement(probe) {
            Some(requirement) => Err(requirement),
            None => Ok(()),
        }
    }

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
            "hardware_profile" => match self.hardware_profile {
                Some(profile) => ScheduleAction::HardwareProfile {
                    profile: profile.into(),
                },
                // Refused by `validate` before an entry is built.
                None => ScheduleAction::Resume,
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
