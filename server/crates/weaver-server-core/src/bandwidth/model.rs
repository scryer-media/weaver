use serde::{Deserialize, Serialize};

/// A time-based rule that pauses, resumes, changes the speed limit, or puts a
/// hardware profile in force.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleEntry {
    pub id: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub label: String,
    /// Days this schedule applies. Empty = every day.
    #[serde(default)]
    pub days: Vec<Weekday>,
    /// Time of day (HH:MM, 24-hour, local time).
    pub time: String,
    #[serde(default)]
    pub times: Vec<String>,
    #[serde(default)]
    pub every_hour_at_minute: Option<u8>,
    pub action: ScheduleAction,
}

/// What a schedule entry does when it fires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleAction {
    RunScript {
        script: String,
        #[serde(default)]
        run_at_startup: bool,
    },
    Pause,
    Resume,
    PauseAll,
    PausePostProcessing,
    ResumePostProcessing,
    PauseWatchFolderScanning,
    ResumeWatchFolderScanning,
    SpeedLimit {
        /// Bytes per second. 0 = unlimited.
        bytes_per_sec: u64,
    },
    /// End the scheduled override and follow the operator's configured limit.
    ConfiguredSpeedLimit,
    /// Put a hardware profile in force until the next profile rule fires.
    /// Profile rules are evaluated apart from every other action: one never
    /// ends a scheduled pause or speed limit, and neither of those ends it.
    HardwareProfile {
        profile: crate::runtime::HardwareProfile,
    },
    SetServerActive {
        server_id: u32,
        active: bool,
    },
    SetQuotaMetering {
        enabled: bool,
    },
    ScanWatchFolder,
    FetchRss {
        feed_id: Option<u32>,
    },
    PruneHistory {
        failed: Option<PruneFiles>,
        completed: Option<PruneFiles>,
        cancelled: Option<PruneFiles>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneFiles {
    pub delete_files: bool,
}

/// Independent held state. Removing its last rule leaves the last applied state
/// in place until the operator or another rule changes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScheduleTrack {
    Downloads,
    PostProcessing,
    WatchFolder,
    Rss,
    Speed,
    Profile,
    Quota,
    Server(u32),
}

impl ScheduleAction {
    pub const fn track(&self) -> Option<ScheduleTrack> {
        Some(match self {
            Self::Pause | Self::Resume | Self::PauseAll => ScheduleTrack::Downloads,
            Self::PausePostProcessing | Self::ResumePostProcessing => ScheduleTrack::PostProcessing,
            Self::PauseWatchFolderScanning | Self::ResumeWatchFolderScanning => {
                ScheduleTrack::WatchFolder
            }
            Self::SpeedLimit { .. } | Self::ConfiguredSpeedLimit => ScheduleTrack::Speed,
            Self::HardwareProfile { .. } => ScheduleTrack::Profile,
            Self::SetQuotaMetering { .. } => ScheduleTrack::Quota,
            Self::SetServerActive { server_id, .. } => ScheduleTrack::Server(*server_id),
            Self::ScanWatchFolder
            | Self::FetchRss { .. }
            | Self::PruneHistory { .. }
            | Self::RunScript { .. } => {
                return None;
            }
        })
    }

    pub fn tracks(&self) -> Vec<ScheduleTrack> {
        if matches!(self, Self::PauseAll | Self::Resume) {
            vec![
                ScheduleTrack::Downloads,
                ScheduleTrack::WatchFolder,
                ScheduleTrack::Rss,
            ]
        } else {
            self.track().into_iter().collect()
        }
    }

    /// A script rule is a one-shot with its own time syntax (`*:MM`,
    /// `startup`). The script evaluator fires it; the schedule evaluator
    /// neither holds nor dispatches it.
    pub const fn is_script(&self) -> bool {
        matches!(self, Self::RunScript { .. })
    }

    pub const fn is_hardware_profile(&self) -> bool {
        matches!(self, Self::HardwareProfile { .. })
    }
}

/// Day of week for schedule entries. Reuses the same serialization as
/// [`IspBandwidthCapWeekday`] but is a separate type to avoid coupling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Weekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

impl Weekday {
    /// The day before this one.
    pub const fn previous(self) -> Self {
        match self {
            Self::Mon => Self::Sun,
            Self::Tue => Self::Mon,
            Self::Wed => Self::Tue,
            Self::Thu => Self::Wed,
            Self::Fri => Self::Thu,
            Self::Sat => Self::Fri,
            Self::Sun => Self::Sat,
        }
    }

    /// Convert from `chrono::Weekday`.
    pub fn from_chrono(w: chrono::Weekday) -> Self {
        match w {
            chrono::Weekday::Mon => Self::Mon,
            chrono::Weekday::Tue => Self::Tue,
            chrono::Weekday::Wed => Self::Wed,
            chrono::Weekday::Thu => Self::Thu,
            chrono::Weekday::Fri => Self::Fri,
            chrono::Weekday::Sat => Self::Sat,
            chrono::Weekday::Sun => Self::Sun,
        }
    }
}

fn default_true() -> bool {
    true
}
