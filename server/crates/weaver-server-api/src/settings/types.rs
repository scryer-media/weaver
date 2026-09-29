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
    /// The operator's choice, or null when the recommendation is standing in:
    /// they have never made one, or this machine can no longer honour it.
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
            // A saved choice the machine can no longer honour is not what
            // runs: the pipeline falls back to the recommendation at startup.
            selected: selected
                .filter(|chosen| available.contains(chosen))
                .map(Into::into),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
#[graphql(name = "ScheduleTrack")]
pub enum ScheduleTrackGql {
    Downloads,
    PostProcessing,
    WatchFolder,
    Rss,
    Speed,
    Profile,
    Quota,
    Server,
    OneShot,
}

#[derive(Clone, Copy, SimpleObject, InputObject)]
#[graphql(input_name = "SchedulePruneFilesInput")]
pub struct SchedulePruneFiles {
    pub delete_files: bool,
}

impl From<weaver_server_core::bandwidth::PruneFiles> for SchedulePruneFiles {
    fn from(value: weaver_server_core::bandwidth::PruneFiles) -> Self {
        Self {
            delete_files: value.delete_files,
        }
    }
}

impl From<SchedulePruneFiles> for weaver_server_core::bandwidth::PruneFiles {
    fn from(value: SchedulePruneFiles) -> Self {
        Self {
            delete_files: value.delete_files,
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
    pub times: Vec<String>,
    pub every_hour_at_minute: Option<u8>,
    pub server_id: Option<u32>,
    pub server_active: Option<bool>,
    pub feed_id: Option<u32>,
    pub quota_metering_enabled: Option<bool>,
    pub prune_failed: Option<SchedulePruneFiles>,
    pub prune_completed: Option<SchedulePruneFiles>,
    pub prune_cancelled: Option<SchedulePruneFiles>,
    pub action_type: String,
    /// Rules on this track hold independently of every other track.
    pub track: ScheduleTrackGql,
    pub speed_limit_bytes: Option<u64>,
    /// The profile a `hardware_profile` rule puts in force; null for every
    /// other action.
    pub hardware_profile: Option<HardwareProfileGql>,
}

impl From<weaver_server_core::bandwidth::ScheduleEntry> for Schedule {
    fn from(e: weaver_server_core::bandwidth::ScheduleEntry) -> Self {
        use weaver_server_core::bandwidth::ScheduleTrack;
        let track = match e.action.track() {
            Some(ScheduleTrack::Downloads) => ScheduleTrackGql::Downloads,
            Some(ScheduleTrack::PostProcessing) => ScheduleTrackGql::PostProcessing,
            Some(ScheduleTrack::WatchFolder) => ScheduleTrackGql::WatchFolder,
            Some(ScheduleTrack::Rss) => ScheduleTrackGql::Rss,
            Some(ScheduleTrack::Speed) => ScheduleTrackGql::Speed,
            Some(ScheduleTrack::Profile) => ScheduleTrackGql::Profile,
            Some(ScheduleTrack::Quota) => ScheduleTrackGql::Quota,
            Some(ScheduleTrack::Server(_)) => ScheduleTrackGql::Server,
            None => ScheduleTrackGql::OneShot,
        };
        use weaver_server_core::bandwidth::ScheduleAction;
        let (server_id, server_active) = match e.action {
            ScheduleAction::SetServerActive { server_id, active } => {
                (Some(server_id), Some(active))
            }
            _ => (None, None),
        };
        let feed_id = match e.action {
            ScheduleAction::FetchRss { feed_id } => feed_id,
            _ => None,
        };
        let quota_metering_enabled = match e.action {
            ScheduleAction::SetQuotaMetering { enabled } => Some(enabled),
            _ => None,
        };
        let (prune_failed, prune_completed, prune_cancelled) = match e.action {
            ScheduleAction::PruneHistory {
                failed,
                completed,
                cancelled,
            } => (
                failed.map(Into::into),
                completed.map(Into::into),
                cancelled.map(Into::into),
            ),
            _ => (None, None, None),
        };
        let mut hardware_profile = None;
        let (action_type, speed_limit_bytes) = match &e.action {
            weaver_server_core::bandwidth::ScheduleAction::Pause => ("pause".into(), None),
            weaver_server_core::bandwidth::ScheduleAction::Resume => ("resume".into(), None),
            ScheduleAction::PauseAll => ("pause_all".into(), None),
            ScheduleAction::PausePostProcessing => ("pause_post_processing".into(), None),
            ScheduleAction::ResumePostProcessing => ("resume_post_processing".into(), None),
            ScheduleAction::SetServerActive { .. } => ("set_server_active".into(), None),
            ScheduleAction::SetQuotaMetering { .. } => ("set_quota_metering".into(), None),
            ScheduleAction::ScanWatchFolder => ("scan_watch_folder".into(), None),
            ScheduleAction::FetchRss { .. } => ("fetch_rss".into(), None),
            ScheduleAction::PruneHistory { .. } => ("prune_history".into(), None),
            weaver_server_core::bandwidth::ScheduleAction::PauseWatchFolderScanning => {
                ("pause_watch_folder_scanning".into(), None)
            }
            weaver_server_core::bandwidth::ScheduleAction::ResumeWatchFolderScanning => {
                ("resume_watch_folder_scanning".into(), None)
            }
            weaver_server_core::bandwidth::ScheduleAction::SpeedLimit { bytes_per_sec } => {
                ("speed_limit".into(), Some(*bytes_per_sec))
            }
            weaver_server_core::bandwidth::ScheduleAction::ConfiguredSpeedLimit => {
                ("configured_speed_limit".into(), None)
            }
            weaver_server_core::bandwidth::ScheduleAction::HardwareProfile { profile } => {
                hardware_profile = Some((*profile).into());
                ("hardware_profile".into(), None)
            }
        };
        Self {
            id: e.id,
            track,
            enabled: e.enabled,
            label: e.label,
            days: e
                .days
                .iter()
                .map(|d| format!("{d:?}").to_lowercase())
                .collect(),
            time: e.time,
            times: e.times,
            every_hour_at_minute: e.every_hour_at_minute,
            server_id,
            server_active,
            feed_id,
            quota_metering_enabled,
            prune_failed,
            prune_completed,
            prune_cancelled,
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
    pub times: Option<Vec<String>>,
    pub every_hour_at_minute: Option<u8>,
    pub server_id: Option<u32>,
    pub server_active: Option<bool>,
    pub feed_id: Option<u32>,
    pub quota_metering_enabled: Option<bool>,
    pub prune_failed: Option<SchedulePruneFiles>,
    pub prune_completed: Option<SchedulePruneFiles>,
    pub prune_cancelled: Option<SchedulePruneFiles>,
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
        if !matches!(
            self.action_type.as_str(),
            "pause"
                | "resume"
                | "speed_limit"
                | "configured_speed_limit"
                | "pause_watch_folder_scanning"
                | "resume_watch_folder_scanning"
                | "hardware_profile"
                | "pause_all"
                | "pause_post_processing"
                | "resume_post_processing"
                | "set_server_active"
                | "set_quota_metering"
                | "scan_watch_folder"
                | "fetch_rss"
                | "prune_history"
        ) {
            return Err(format!("unknown schedule actionType: {}", self.action_type));
        }
        let one_shot = matches!(
            self.action_type.as_str(),
            "scan_watch_folder" | "fetch_rss" | "prune_history"
        );
        if let Some(minute) = self.every_hour_at_minute {
            if !one_shot {
                return Err("hourly schedules are only supported for one-shot actions".into());
            }
            if minute > 59 {
                return Err("hourly minute must be between 0 and 59".into());
            }
            if self.times.as_ref().is_some_and(|times| !times.is_empty()) {
                return Err("choose multiple times or hourly, not both".into());
            }
        }
        if weaver_server_core::bandwidth::schedule::parse_time(&self.time).is_none()
            || self.times.as_ref().is_some_and(|times| {
                times.len() > 24
                    || times.iter().any(|time| {
                        weaver_server_core::bandwidth::schedule::parse_time(time).is_none()
                    })
            })
        {
            return Err("schedule times must be HH:MM, with at most 24 times per rule".into());
        }
        if self.action_type == "set_server_active"
            && (self.server_id.is_none() || self.server_active.is_none())
        {
            return Err("set_server_active requires serverId and serverActive".into());
        }
        if self.action_type == "set_quota_metering" && self.quota_metering_enabled.is_none() {
            return Err("set_quota_metering requires quotaMeteringEnabled".into());
        }
        if self.action_type == "prune_history"
            && self.prune_failed.is_none()
            && self.prune_completed.is_none()
            && self.prune_cancelled.is_none()
        {
            return Err("prune_history requires at least one status".into());
        }
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

    pub fn into_entry(self) -> Result<weaver_server_core::bandwidth::ScheduleEntry, String> {
        use weaver_server_core::bandwidth::{ScheduleAction, Weekday};

        let action = match self.action_type.as_str() {
            "pause" => ScheduleAction::Pause,
            "resume" => ScheduleAction::Resume,
            "pause_all" => ScheduleAction::PauseAll,
            "pause_post_processing" => ScheduleAction::PausePostProcessing,
            "resume_post_processing" => ScheduleAction::ResumePostProcessing,
            "set_server_active" => ScheduleAction::SetServerActive {
                server_id: self
                    .server_id
                    .ok_or("set_server_active requires serverId")?,
                active: self
                    .server_active
                    .ok_or("set_server_active requires serverActive")?,
            },
            "set_quota_metering" => ScheduleAction::SetQuotaMetering {
                enabled: self
                    .quota_metering_enabled
                    .ok_or("set_quota_metering requires quotaMeteringEnabled")?,
            },
            "scan_watch_folder" => ScheduleAction::ScanWatchFolder,
            "fetch_rss" => ScheduleAction::FetchRss {
                feed_id: self.feed_id,
            },
            "prune_history" => ScheduleAction::PruneHistory {
                failed: self.prune_failed.map(Into::into),
                completed: self.prune_completed.map(Into::into),
                cancelled: self.prune_cancelled.map(Into::into),
            },
            "pause_watch_folder_scanning" => ScheduleAction::PauseWatchFolderScanning,
            "resume_watch_folder_scanning" => ScheduleAction::ResumeWatchFolderScanning,
            "speed_limit" => ScheduleAction::SpeedLimit {
                bytes_per_sec: self.speed_limit_bytes.unwrap_or(0),
            },
            "configured_speed_limit" => ScheduleAction::ConfiguredSpeedLimit,
            "hardware_profile" => match self.hardware_profile {
                Some(profile) => ScheduleAction::HardwareProfile {
                    profile: profile.into(),
                },
                None => return Err("a hardware_profile schedule needs a hardwareProfile".into()),
            },
            _ => return Err(format!("unknown schedule actionType: {}", self.action_type)),
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
        Ok(weaver_server_core::bandwidth::ScheduleEntry {
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
            time: self
                .times
                .as_ref()
                .and_then(|times| times.first())
                .cloned()
                .unwrap_or(self.time),
            times: self.times.unwrap_or_default(),
            every_hour_at_minute: self.every_hour_at_minute,
            action,
        })
    }
}
