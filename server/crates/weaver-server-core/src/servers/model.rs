use serde::{Deserialize, Serialize};

use crate::bandwidth::IspBandwidthCapWeekday;

/// Largest byte count accepted by persistence-backed per-server policies.
pub const MAX_PERSISTED_SERVER_DOWNLOAD_BYTES: u64 = i64::MAX as u64;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerDownloadQuotaPeriod {
    #[default]
    OneTime,
    Daily,
    Weekly,
    Monthly,
}

impl ServerDownloadQuotaPeriod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OneTime => "one_time",
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "one_time" => Some(Self::OneTime),
            "daily" => Some(Self::Daily),
            "weekly" => Some(Self::Weekly),
            "monthly" => Some(Self::Monthly),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerDownloadQuotaConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub limit_bytes: u64,
    #[serde(default)]
    pub period: ServerDownloadQuotaPeriod,
    #[serde(default)]
    pub reset_time_minutes_local: u16,
    #[serde(default = "default_quota_weekday")]
    pub weekly_reset_weekday: IspBandwidthCapWeekday,
    #[serde(default = "default_monthly_reset_day")]
    pub monthly_reset_day: u8,
}

impl Default for ServerDownloadQuotaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            limit_bytes: 0,
            period: ServerDownloadQuotaPeriod::OneTime,
            reset_time_minutes_local: 0,
            weekly_reset_weekday: default_quota_weekday(),
            monthly_reset_day: default_monthly_reset_day(),
        }
    }
}

impl ServerDownloadQuotaConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.limit_bytes > MAX_PERSISTED_SERVER_DOWNLOAD_BYTES {
            return Err("server download quota limit exceeds database range".to_string());
        }
        if self.enabled && self.limit_bytes == 0 {
            return Err(
                "enabled server download quota limit must be greater than zero".to_string(),
            );
        }
        if self.reset_time_minutes_local >= 24 * 60 {
            return Err("server download quota reset time must be between 0 and 1439".to_string());
        }
        if !(1..=31).contains(&self.monthly_reset_day) {
            return Err(
                "server download quota monthly reset day must be between 1 and 31".to_string(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Stable identifier for CRUD operations.
    #[serde(default)]
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connections: u16,
    /// Whether this server is enabled. Defaults to true.
    #[serde(default = "default_true")]
    pub active: bool,
    /// Whether the server supports NNTP command pipelining (RFC 4644).
    /// Auto-detected when the server is added or tested.
    #[serde(default)]
    pub supports_pipelining: bool,
    /// Priority group. Lower values tried first within the fill tier.
    #[serde(default)]
    pub priority: u32,
    /// Backfill servers only serve articles the fill tier could not:
    /// missing/corrupt there, or outside a fill server's retention window.
    /// Non-backfill ("fill") servers download ordinary work ordered by
    /// priority.
    #[serde(default)]
    pub backfill: bool,
    /// Days of retention this server is expected to hold. Articles older
    /// than this skip the server without a network attempt. 0 = unlimited.
    #[serde(default)]
    pub retention_days: u32,
    /// Aggregate download rate across this server's connections in bytes/sec.
    /// Zero means unlimited.
    #[serde(default)]
    pub max_download_speed: u64,
    /// Hard raw BODY-byte quota policy for this server.
    #[serde(default)]
    pub download_quota: ServerDownloadQuotaConfig,
    /// Optional path to a PEM-encoded CA certificate to trust for TLS
    /// connections to this server (e.g. self-signed or internal CAs).
    #[serde(default)]
    pub tls_ca_cert: Option<std::path::PathBuf>,
}

impl ServerConfig {
    pub fn validate_download_limits(&self) -> Result<(), String> {
        if self.max_download_speed > MAX_PERSISTED_SERVER_DOWNLOAD_BYTES {
            return Err("server max download speed exceeds database range".to_string());
        }
        self.download_quota.validate()
    }
}

fn default_true() -> bool {
    true
}

fn default_quota_weekday() -> IspBandwidthCapWeekday {
    IspBandwidthCapWeekday::Mon
}

fn default_monthly_reset_day() -> u8 {
    1
}
