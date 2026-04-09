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
    pub isp_bandwidth_cap: Option<IspBandwidthCapSettings>,
}

#[derive(Debug, InputObject)]
pub struct GeneralSettingsInput {
    pub intermediate_dir: MaybeUndefined<String>,
    pub complete_dir: MaybeUndefined<String>,
    pub cleanup_after_extract: Option<bool>,
    pub max_download_speed: Option<u64>,
    pub max_retries: Option<u32>,
    pub isp_bandwidth_cap: Option<IspBandwidthCapSettingsInput>,
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
