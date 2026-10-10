use serde::{Deserialize, Serialize};

/// A time-based rule that pauses, resumes, changes speed limits, or puts a
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
    Pause,
    Resume,
    PauseAll,
    PausePostProcessing,
    ResumePostProcessing,
    PauseWatchFolderScanning,
    ResumeWatchFolderScanning,
    PauseRss,
    ResumeRss,
    /// Change any number of speed limits at once. Each target is held on its
    /// own track, so a rule for one target never ends another's.
    SpeedLimit {
        limits: Vec<SpeedLimitChange>,
    },
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
        #[serde(default)]
        target: QuotaTarget,
    },
    PruneHistory {
        failed: Option<PruneFiles>,
        completed: Option<PruneFiles>,
        cancelled: Option<PruneFiles>,
    },
}

/// One limit a speed rule sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpeedLimitChange {
    pub target: SpeedTarget,
    /// Bytes per second. 0 = no limit at this level.
    pub bytes_per_sec: u64,
}

/// What a speed limit applies to. The effective rate of a download is the
/// lowest of the global limit, its egress's and its provider's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum SpeedTarget {
    Global,
    Egress(u32),
    Server(u32),
}

/// Which egresses a quota-metering rule turns counting on or off for.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum QuotaTarget {
    /// Every egress without a rule of its own.
    #[default]
    AllEgresses,
    Egress(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneFiles {
    pub delete_files: bool,
}

/// Independent held state. Removing its last rule leaves the last applied state
/// in place until the operator or another rule changes it.
///
/// Tracks are applied in this order, so a single egress's quota rule is
/// applied after the rule for every egress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScheduleTrack {
    Downloads,
    PostProcessing,
    WatchFolder,
    Rss,
    Speed(SpeedTarget),
    Profile,
    Quota(QuotaTarget),
    Server(u32),
}

impl ScheduleAction {
    /// The tracks this action holds. Empty for a one-shot.
    pub fn tracks(&self) -> Vec<ScheduleTrack> {
        match self {
            Self::PauseAll | Self::Resume => vec![
                ScheduleTrack::Downloads,
                ScheduleTrack::WatchFolder,
                ScheduleTrack::Rss,
            ],
            Self::Pause => vec![ScheduleTrack::Downloads],
            Self::PausePostProcessing | Self::ResumePostProcessing => {
                vec![ScheduleTrack::PostProcessing]
            }
            Self::PauseWatchFolderScanning | Self::ResumeWatchFolderScanning => {
                vec![ScheduleTrack::WatchFolder]
            }
            Self::PauseRss | Self::ResumeRss => vec![ScheduleTrack::Rss],
            Self::SpeedLimit { limits } => {
                let mut tracks: Vec<_> = limits
                    .iter()
                    .map(|limit| ScheduleTrack::Speed(limit.target))
                    .collect();
                tracks.sort_unstable();
                tracks.dedup();
                tracks
            }
            Self::HardwareProfile { .. } => vec![ScheduleTrack::Profile],
            Self::SetQuotaMetering { target, .. } => vec![ScheduleTrack::Quota(*target)],
            Self::SetServerActive { server_id, .. } => vec![ScheduleTrack::Server(*server_id)],
            Self::PruneHistory { .. } => Vec::new(),
        }
    }

    /// Whether this action fires once rather than holding a track.
    pub fn is_one_shot(&self) -> bool {
        matches!(self, Self::PruneHistory { .. })
    }

    /// The part of this action that concerns `track`: a speed rule touching
    /// several targets is held, compared and applied one target at a time.
    pub fn for_track(&self, track: ScheduleTrack) -> Self {
        match (self, track) {
            (Self::SpeedLimit { limits }, ScheduleTrack::Speed(target)) => Self::SpeedLimit {
                // The last value given for a target wins.
                limits: limits
                    .iter()
                    .rev()
                    .find(|limit| limit.target == target)
                    .copied()
                    .into_iter()
                    .collect(),
            },
            _ => self.clone(),
        }
    }

    /// Whether failing to apply this action must keep new downloads from
    /// starting: a pause that did not take, or a server that should have gone
    /// offline. Any other failure leaves admission as it was.
    pub const fn holds_admission(&self) -> bool {
        matches!(
            self,
            Self::Pause | Self::PauseAll | Self::SetServerActive { active: false, .. }
        )
    }

    pub const fn is_hardware_profile(&self) -> bool {
        matches!(self, Self::HardwareProfile { .. })
    }
}

/// Day of week for schedule entries. Reuses the same serialization as
/// [`QuotaWeekday`] but is a separate type to avoid coupling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

    pub const ALL: [Self; 7] = [
        Self::Mon,
        Self::Tue,
        Self::Wed,
        Self::Thu,
        Self::Fri,
        Self::Sat,
        Self::Sun,
    ];

    /// The name it is saved under.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mon => "mon",
            Self::Tue => "tue",
            Self::Wed => "wed",
            Self::Thu => "thu",
            Self::Fri => "fri",
            Self::Sat => "sat",
            Self::Sun => "sun",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|day| day.as_str().eq_ignore_ascii_case(value.trim()))
    }
}

fn default_true() -> bool {
    true
}
