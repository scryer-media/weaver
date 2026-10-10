use async_graphql::{Enum, InputObject, MaybeUndefined, SimpleObject};
use serde::{Deserialize, Serialize};
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

/// What one speed-limit change in a rule applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum ScheduleTargetKind {
    Global,
    Egress,
    Server,
}

/// One target of a `speed_limit` rule and the rate it sets. 0 removes the
/// limit at that level.
#[derive(Debug, Clone, Copy, SimpleObject, InputObject)]
#[graphql(input_name = "ScheduleSpeedLimitInput")]
pub struct ScheduleSpeedLimit {
    pub kind: ScheduleTargetKind,
    /// The egress or provider id; null for the global limit.
    pub id: Option<u32>,
    pub bytes_per_sec: u64,
}

impl From<weaver_server_core::bandwidth::SpeedLimitChange> for ScheduleSpeedLimit {
    fn from(change: weaver_server_core::bandwidth::SpeedLimitChange) -> Self {
        use weaver_server_core::bandwidth::SpeedTarget;
        let (kind, id) = match change.target {
            SpeedTarget::Global => (ScheduleTargetKind::Global, None),
            SpeedTarget::Egress(id) => (ScheduleTargetKind::Egress, Some(id)),
            SpeedTarget::Server(id) => (ScheduleTargetKind::Server, Some(id)),
        };
        Self {
            kind,
            id,
            bytes_per_sec: change.bytes_per_sec,
        }
    }
}

impl TryFrom<ScheduleSpeedLimit> for weaver_server_core::bandwidth::SpeedLimitChange {
    type Error = String;

    fn try_from(limit: ScheduleSpeedLimit) -> Result<Self, String> {
        use weaver_server_core::bandwidth::SpeedTarget;
        let target = match (limit.kind, limit.id) {
            (ScheduleTargetKind::Global, _) => SpeedTarget::Global,
            (ScheduleTargetKind::Egress, Some(id)) => SpeedTarget::Egress(id),
            (ScheduleTargetKind::Server, Some(id)) => SpeedTarget::Server(id),
            (_, None) => return Err("an egress or provider speed limit needs its id".into()),
        };
        Ok(Self {
            target,
            bytes_per_sec: limit.bytes_per_sec,
        })
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
    pub quota_metering_enabled: Option<bool>,
    /// The egress a `set_quota_metering` rule applies to; null for every
    /// egress.
    pub quota_egress_id: Option<u32>,
    pub prune_failed: Option<SchedulePruneFiles>,
    pub prune_completed: Option<SchedulePruneFiles>,
    pub prune_cancelled: Option<SchedulePruneFiles>,
    pub action_type: String,
    /// Rules on this track hold independently of every other track. A rule
    /// touching several tracks reports the first.
    pub track: ScheduleTrackGql,
    /// The global rate a `speed_limit` rule sets, if it sets one.
    #[graphql(deprecation = "Use speedLimits.")]
    pub speed_limit_bytes: Option<u64>,
    /// Every target a `speed_limit` rule sets, global first, then each
    /// egress, then each provider. Empty for every other action.
    pub speed_limits: Vec<ScheduleSpeedLimit>,
    /// The profile a `hardware_profile` rule puts in force; null for every
    /// other action.
    pub hardware_profile: Option<HardwareProfileGql>,
}

impl From<weaver_server_core::bandwidth::ScheduleEntry> for Schedule {
    fn from(e: weaver_server_core::bandwidth::ScheduleEntry) -> Self {
        use weaver_server_core::bandwidth::{
            QuotaTarget, ScheduleAction, ScheduleTrack, SpeedTarget,
        };
        let track = match e.action.tracks().first() {
            Some(ScheduleTrack::Downloads) => ScheduleTrackGql::Downloads,
            Some(ScheduleTrack::PostProcessing) => ScheduleTrackGql::PostProcessing,
            Some(ScheduleTrack::WatchFolder) => ScheduleTrackGql::WatchFolder,
            Some(ScheduleTrack::Rss) => ScheduleTrackGql::Rss,
            Some(ScheduleTrack::Speed(_)) => ScheduleTrackGql::Speed,
            Some(ScheduleTrack::Profile) => ScheduleTrackGql::Profile,
            Some(ScheduleTrack::Quota(_)) => ScheduleTrackGql::Quota,
            Some(ScheduleTrack::Server(_)) => ScheduleTrackGql::Server,
            None => ScheduleTrackGql::OneShot,
        };
        let (server_id, server_active) = match e.action {
            ScheduleAction::SetServerActive { server_id, active } => {
                (Some(server_id), Some(active))
            }
            _ => (None, None),
        };
        let (quota_metering_enabled, quota_egress_id) = match e.action {
            ScheduleAction::SetQuotaMetering { enabled, target } => (
                Some(enabled),
                match target {
                    QuotaTarget::AllEgresses => None,
                    QuotaTarget::Egress(id) => Some(id),
                },
            ),
            _ => (None, None),
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
        let mut speed_limits = Vec::new();
        let mut speed_limit_bytes = None;
        let action_type = match &e.action {
            ScheduleAction::Pause => "pause",
            ScheduleAction::Resume => "resume",
            ScheduleAction::PauseAll => "pause_all",
            ScheduleAction::PausePostProcessing => "pause_post_processing",
            ScheduleAction::ResumePostProcessing => "resume_post_processing",
            ScheduleAction::PauseWatchFolderScanning => "pause_watch_folder_scanning",
            ScheduleAction::ResumeWatchFolderScanning => "resume_watch_folder_scanning",
            ScheduleAction::PauseRss => "pause_rss",
            ScheduleAction::ResumeRss => "resume_rss",
            ScheduleAction::SetServerActive { .. } => "set_server_active",
            ScheduleAction::SetQuotaMetering { .. } => "set_quota_metering",
            ScheduleAction::PruneHistory { .. } => "prune_history",
            ScheduleAction::SpeedLimit { limits } => {
                speed_limit_bytes = limits
                    .iter()
                    .find(|change| change.target == SpeedTarget::Global)
                    .map(|change| change.bytes_per_sec);
                speed_limits = limits.iter().copied().map(Into::into).collect();
                "speed_limit"
            }
            ScheduleAction::HardwareProfile { profile } => {
                hardware_profile = Some((*profile).into());
                "hardware_profile"
            }
        };
        Self {
            id: e.id,
            track,
            enabled: e.enabled,
            label: e.label,
            days: e.days.iter().map(|d| d.as_str().to_string()).collect(),
            time: e.time,
            times: e.times,
            every_hour_at_minute: e.every_hour_at_minute,
            server_id,
            server_active,
            quota_metering_enabled,
            quota_egress_id,
            prune_failed,
            prune_completed,
            prune_cancelled,
            action_type: action_type.to_string(),
            speed_limit_bytes,
            speed_limits,
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
    pub quota_metering_enabled: Option<bool>,
    /// The egress a `set_quota_metering` rule applies to; omit for every
    /// egress.
    pub quota_egress_id: Option<u32>,
    pub prune_failed: Option<SchedulePruneFiles>,
    pub prune_completed: Option<SchedulePruneFiles>,
    pub prune_cancelled: Option<SchedulePruneFiles>,
    pub action_type: String,
    /// A global rate for a `speed_limit` rule, read only when `speedLimits`
    /// is omitted.
    #[graphql(deprecation = "Use speedLimits.")]
    pub speed_limit_bytes: Option<u64>,
    /// Every target a `speed_limit` rule sets. A target left out is left as
    /// it is; 0 removes the limit at that level.
    pub speed_limits: Option<Vec<ScheduleSpeedLimit>>,
    /// Required when the action is `hardware_profile`, ignored otherwise.
    pub hardware_profile: Option<HardwareProfileGql>,
}

impl ScheduleInput {
    /// The changes a `speed_limit` rule makes, global first, then each egress,
    /// then each provider. A target named twice keeps its last value.
    fn speed_limit_changes(
        &self,
    ) -> Result<Vec<weaver_server_core::bandwidth::SpeedLimitChange>, String> {
        use weaver_server_core::bandwidth::{SpeedLimitChange, SpeedTarget};
        let mut changes: Vec<SpeedLimitChange> = match &self.speed_limits {
            Some(limits) => limits
                .iter()
                .copied()
                .map(SpeedLimitChange::try_from)
                .collect::<Result<_, _>>()?,
            None => self
                .speed_limit_bytes
                .map(|bytes_per_sec| SpeedLimitChange {
                    target: SpeedTarget::Global,
                    bytes_per_sec,
                })
                .into_iter()
                .collect(),
        };
        // Egress and provider limits are stored as signed 64-bit values.
        if changes
            .iter()
            .any(|change| i64::try_from(change.bytes_per_sec).is_err())
        {
            return Err("speed limit is too large".into());
        }
        let mut seen = std::collections::HashSet::new();
        changes.reverse();
        changes.retain(|change| seen.insert(change.target));
        changes.sort_by_key(|change| match change.target {
            SpeedTarget::Global => (0, 0),
            SpeedTarget::Egress(id) => (1, id),
            SpeedTarget::Server(id) => (2, id),
        });
        Ok(changes)
    }

    /// Refuse a rule [`Self::into_entry`] would not build as asked. A
    /// `hardware_profile` rule needs a profile, and one this machine can
    /// honour: a rule that could never apply is refused by name here rather
    /// than saved and skipped every time it fires.
    pub fn validate(&self, probe: &SystemProfile) -> Result<(), String> {
        if !matches!(
            self.action_type.as_str(),
            "pause"
                | "resume"
                | "pause_all"
                | "pause_post_processing"
                | "resume_post_processing"
                | "pause_watch_folder_scanning"
                | "resume_watch_folder_scanning"
                | "pause_rss"
                | "resume_rss"
                | "speed_limit"
                | "hardware_profile"
                | "set_server_active"
                | "set_quota_metering"
                | "prune_history"
        ) {
            return Err(format!("unknown schedule actionType: {}", self.action_type));
        }
        let one_shot = self.action_type == "prune_history";
        if let Some(minute) = self.every_hour_at_minute {
            if !one_shot {
                return Err("hourly schedules are only supported for pruning history".into());
            }
            if minute > 59 {
                return Err("hourly minute must be between 0 and 59".into());
            }
            if self.times.as_ref().is_some_and(|times| !times.is_empty()) {
                return Err("choose multiple times or hourly, not both".into());
            }
        }
        if self.action_type == "speed_limit"
            && self.times.as_ref().is_some_and(|times| times.len() > 1)
        {
            return Err("a speed_limit schedule takes one time of day".into());
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
        if self.action_type == "speed_limit" && self.speed_limit_changes()?.is_empty() {
            return Err("a speed_limit schedule needs at least one limit".into());
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
        use weaver_server_core::bandwidth::{QuotaTarget, ScheduleAction, Weekday};

        let action = match self.action_type.as_str() {
            "pause" => ScheduleAction::Pause,
            "resume" => ScheduleAction::Resume,
            "pause_all" => ScheduleAction::PauseAll,
            "pause_post_processing" => ScheduleAction::PausePostProcessing,
            "resume_post_processing" => ScheduleAction::ResumePostProcessing,
            "pause_watch_folder_scanning" => ScheduleAction::PauseWatchFolderScanning,
            "resume_watch_folder_scanning" => ScheduleAction::ResumeWatchFolderScanning,
            "pause_rss" => ScheduleAction::PauseRss,
            "resume_rss" => ScheduleAction::ResumeRss,
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
                target: self
                    .quota_egress_id
                    .map_or(QuotaTarget::AllEgresses, QuotaTarget::Egress),
            },
            "prune_history" => ScheduleAction::PruneHistory {
                failed: self.prune_failed.map(Into::into),
                completed: self.prune_completed.map(Into::into),
                cancelled: self.prune_cancelled.map(Into::into),
            },
            "speed_limit" => {
                let limits = self.speed_limit_changes()?;
                if limits.is_empty() {
                    return Err("a speed_limit schedule needs at least one limit".into());
                }
                ScheduleAction::SpeedLimit { limits }
            }
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
            .filter_map(|d| Weekday::parse(&d.to_lowercase()))
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

#[cfg(test)]
mod schedule_input_tests {
    use super::*;
    use weaver_server_core::bandwidth::{
        QuotaTarget, ScheduleAction, SpeedLimitChange, SpeedTarget,
    };

    fn probe() -> SystemProfile {
        use weaver_server_core::runtime::system_profile::{
            CpuProfile, DiskProfile, FilesystemType, MemoryProfile, StorageClass,
        };
        SystemProfile {
            cpu: CpuProfile {
                physical_cores: 2,
                logical_cores: 2,
                simd: Default::default(),
                cgroup_limit: None,
            },
            memory: MemoryProfile {
                total_bytes: 1024 * 1024 * 1024,
                available_bytes: 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: DiskProfile {
                storage_class: StorageClass::Unknown,
                filesystem: FilesystemType::Unknown("test".into()),
                sequential_write_mbps: 0.0,
                random_read_iops: 0.0,
                same_filesystem: true,
            },
        }
    }

    fn input(action_type: &str) -> ScheduleInput {
        ScheduleInput {
            enabled: Some(true),
            label: None,
            days: None,
            time: "01:00".into(),
            times: None,
            every_hour_at_minute: None,
            server_id: None,
            server_active: None,
            quota_metering_enabled: None,
            quota_egress_id: None,
            prune_failed: None,
            prune_completed: None,
            prune_cancelled: None,
            action_type: action_type.into(),
            speed_limit_bytes: None,
            speed_limits: None,
            hardware_profile: None,
        }
    }

    fn limit(kind: ScheduleTargetKind, id: Option<u32>, bytes_per_sec: u64) -> ScheduleSpeedLimit {
        ScheduleSpeedLimit {
            kind,
            id,
            bytes_per_sec,
        }
    }

    #[test]
    fn a_speed_rule_sets_each_target_it_names_in_global_egress_provider_order() {
        let mut rule = input("speed_limit");
        rule.speed_limits = Some(vec![
            limit(ScheduleTargetKind::Server, Some(4), 0),
            limit(ScheduleTargetKind::Egress, Some(2), 2_000_000),
            limit(ScheduleTargetKind::Global, None, 5_000_000),
        ]);
        rule.validate(&probe()).unwrap();
        let entry = rule.into_entry().unwrap();
        assert_eq!(
            entry.action,
            ScheduleAction::SpeedLimit {
                limits: vec![
                    SpeedLimitChange {
                        target: SpeedTarget::Global,
                        bytes_per_sec: 5_000_000,
                    },
                    SpeedLimitChange {
                        target: SpeedTarget::Egress(2),
                        bytes_per_sec: 2_000_000,
                    },
                    SpeedLimitChange {
                        target: SpeedTarget::Server(4),
                        bytes_per_sec: 0,
                    },
                ],
            }
        );
        let shown = Schedule::from(entry);
        assert_eq!(shown.speed_limit_bytes, Some(5_000_000));
        assert_eq!(shown.speed_limits.len(), 3);
        assert_eq!(shown.track, ScheduleTrackGql::Speed);
    }

    #[test]
    fn the_old_single_rate_field_still_saves_a_global_limit() {
        let mut rule = input("speed_limit");
        rule.speed_limit_bytes = Some(1024);
        let entry = rule.into_entry().unwrap();
        assert_eq!(
            entry.action,
            ScheduleAction::SpeedLimit {
                limits: vec![SpeedLimitChange {
                    target: SpeedTarget::Global,
                    bytes_per_sec: 1024,
                }],
            }
        );
    }

    #[test]
    fn a_speed_rule_with_no_target_or_an_unnamed_egress_is_refused() {
        assert!(input("speed_limit").validate(&probe()).is_err());
        let mut rule = input("speed_limit");
        rule.speed_limits = Some(vec![limit(ScheduleTargetKind::Egress, None, 1)]);
        assert!(rule.validate(&probe()).is_err());
        let mut rule = input("speed_limit");
        rule.speed_limits = Some(vec![limit(ScheduleTargetKind::Global, None, 1)]);
        rule.times = Some(vec!["01:00".into(), "02:00".into()]);
        assert!(rule.validate(&probe()).is_err(), "one time of day per rule");
    }

    #[test]
    fn a_speed_rate_above_the_stored_range_is_refused() {
        let largest = i64::MAX as u64;
        for kind in [
            ScheduleTargetKind::Global,
            ScheduleTargetKind::Egress,
            ScheduleTargetKind::Server,
        ] {
            let id = (kind != ScheduleTargetKind::Global).then_some(1);
            let mut rule = input("speed_limit");
            rule.speed_limits = Some(vec![limit(kind, id, largest + 1)]);
            assert_eq!(
                rule.validate(&probe()).unwrap_err(),
                "speed limit is too large"
            );
            rule.speed_limits = Some(vec![limit(kind, id, largest)]);
            assert!(rule.validate(&probe()).is_ok());
        }
        let mut rule = input("speed_limit");
        rule.speed_limit_bytes = Some(u64::MAX);
        assert!(rule.validate(&probe()).is_err());
        assert!(rule.into_entry().is_err());
    }

    #[test]
    fn quota_metering_applies_to_every_egress_unless_one_is_named() {
        let mut rule = input("set_quota_metering");
        rule.quota_metering_enabled = Some(false);
        let entry = rule.into_entry().unwrap();
        assert_eq!(
            entry.action,
            ScheduleAction::SetQuotaMetering {
                enabled: false,
                target: QuotaTarget::AllEgresses,
            }
        );
        let mut rule = input("set_quota_metering");
        rule.quota_metering_enabled = Some(true);
        rule.quota_egress_id = Some(3);
        let shown = Schedule::from(rule.into_entry().unwrap());
        assert_eq!(shown.quota_egress_id, Some(3));
        assert_eq!(shown.quota_metering_enabled, Some(true));
    }

    #[test]
    fn rss_pair_is_offered_and_removed_actions_are_refused() {
        for action in ["pause_rss", "resume_rss"] {
            input(action).validate(&probe()).unwrap();
        }
        assert_eq!(
            input("pause_rss").into_entry().unwrap().action,
            ScheduleAction::PauseRss
        );
        for action in [
            "run_script",
            "scan_watch_folder",
            "fetch_rss",
            "configured_speed_limit",
        ] {
            assert!(input(action).validate(&probe()).is_err(), "{action}");
        }
    }

    #[test]
    fn only_pruning_runs_hourly() {
        let mut rule = input("pause");
        rule.every_hour_at_minute = Some(5);
        assert!(rule.validate(&probe()).is_err());
        let mut rule = input("prune_history");
        rule.every_hour_at_minute = Some(5);
        rule.prune_failed = Some(SchedulePruneFiles {
            delete_files: false,
        });
        rule.validate(&probe()).unwrap();
    }
}
