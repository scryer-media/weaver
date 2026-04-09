use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IspBandwidthCapPeriod {
    Daily,
    Weekly,
    Monthly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IspBandwidthCapWeekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IspBandwidthCapConfig {
    #[serde(default)]
    pub enabled: bool,
    pub period: IspBandwidthCapPeriod,
    pub limit_bytes: u64,
    pub reset_time_minutes_local: u16,
    pub weekly_reset_weekday: IspBandwidthCapWeekday,
    pub monthly_reset_day: u8,
}
