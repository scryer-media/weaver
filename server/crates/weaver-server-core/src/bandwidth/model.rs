use serde::{Deserialize, Serialize};

/// A time-based rule that pauses, resumes, or changes the speed limit.
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
    pub action: ScheduleAction,
}

/// What a schedule entry does when it fires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleAction {
    Pause,
    Resume,
    SpeedLimit {
        /// Bytes per second. 0 = unlimited.
        bytes_per_sec: u64,
    },
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
