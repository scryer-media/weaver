use async_graphql::{Enum, InputObject, SimpleObject};
use chrono::Local;

use weaver_server_core::bandwidth::IspBandwidthCapWeekday;
use weaver_server_core::servers::{
    ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod,
    transfer_policy::ServerDownloadQuotaSnapshot,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Enum)]
pub enum ServerDownloadQuotaPeriodGql {
    #[default]
    OneTime,
    Daily,
    Weekly,
    Monthly,
}

impl From<ServerDownloadQuotaPeriod> for ServerDownloadQuotaPeriodGql {
    fn from(value: ServerDownloadQuotaPeriod) -> Self {
        match value {
            ServerDownloadQuotaPeriod::OneTime => Self::OneTime,
            ServerDownloadQuotaPeriod::Daily => Self::Daily,
            ServerDownloadQuotaPeriod::Weekly => Self::Weekly,
            ServerDownloadQuotaPeriod::Monthly => Self::Monthly,
        }
    }
}

impl From<ServerDownloadQuotaPeriodGql> for ServerDownloadQuotaPeriod {
    fn from(value: ServerDownloadQuotaPeriodGql) -> Self {
        match value {
            ServerDownloadQuotaPeriodGql::OneTime => Self::OneTime,
            ServerDownloadQuotaPeriodGql::Daily => Self::Daily,
            ServerDownloadQuotaPeriodGql::Weekly => Self::Weekly,
            ServerDownloadQuotaPeriodGql::Monthly => Self::Monthly,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Enum)]
pub enum ServerDownloadQuotaWeekdayGql {
    #[default]
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

impl From<IspBandwidthCapWeekday> for ServerDownloadQuotaWeekdayGql {
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

impl From<ServerDownloadQuotaWeekdayGql> for IspBandwidthCapWeekday {
    fn from(value: ServerDownloadQuotaWeekdayGql) -> Self {
        match value {
            ServerDownloadQuotaWeekdayGql::Mon => Self::Mon,
            ServerDownloadQuotaWeekdayGql::Tue => Self::Tue,
            ServerDownloadQuotaWeekdayGql::Wed => Self::Wed,
            ServerDownloadQuotaWeekdayGql::Thu => Self::Thu,
            ServerDownloadQuotaWeekdayGql::Fri => Self::Fri,
            ServerDownloadQuotaWeekdayGql::Sat => Self::Sat,
            ServerDownloadQuotaWeekdayGql::Sun => Self::Sun,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ServerDownloadQuota {
    pub enabled: bool,
    pub limit_bytes: u64,
    pub period: ServerDownloadQuotaPeriodGql,
    pub reset_time_minutes_local: u16,
    pub weekly_reset_weekday: ServerDownloadQuotaWeekdayGql,
    pub monthly_reset_day: u8,
    pub lifetime_bytes: u64,
    pub used_bytes: u64,
    pub reserved_bytes: u64,
    pub remaining_bytes: u64,
    pub blocked: bool,
    pub window_starts_at_epoch_ms: Option<f64>,
    pub window_ends_at_epoch_ms: Option<f64>,
    pub timezone_name: String,
}

impl ServerDownloadQuota {
    fn from_config(
        config: &ServerDownloadQuotaConfig,
        snapshot: Option<&ServerDownloadQuotaSnapshot>,
    ) -> Self {
        let fallback_timezone =
            std::env::var("TZ").unwrap_or_else(|_| Local::now().offset().to_string());
        Self {
            enabled: config.enabled,
            limit_bytes: config.limit_bytes,
            period: config.period.into(),
            reset_time_minutes_local: config.reset_time_minutes_local,
            weekly_reset_weekday: config.weekly_reset_weekday.into(),
            monthly_reset_day: config.monthly_reset_day,
            lifetime_bytes: snapshot.map_or(0, |value| value.lifetime_bytes),
            used_bytes: snapshot.map_or(0, |value| value.used_bytes),
            reserved_bytes: snapshot.map_or(0, |value| value.reserved_bytes),
            remaining_bytes: snapshot.and_then(|value| value.remaining_bytes).unwrap_or(
                if config.enabled {
                    config.limit_bytes
                } else {
                    0
                },
            ),
            blocked: snapshot.is_some_and(|value| value.blocked),
            window_starts_at_epoch_ms: snapshot
                .and_then(|value| value.window_start)
                .map(|value| value.timestamp_millis() as f64),
            window_ends_at_epoch_ms: snapshot
                .and_then(|value| value.window_end)
                .map(|value| value.timestamp_millis() as f64),
            timezone_name: snapshot
                .map(|value| value.timezone.clone())
                .unwrap_or(fallback_timezone),
        }
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct ServerDownloadQuotaInput {
    #[graphql(default = false)]
    pub enabled: bool,
    #[graphql(default = 0)]
    pub limit_bytes: u64,
    #[graphql(default)]
    pub period: ServerDownloadQuotaPeriodGql,
    #[graphql(default = 0)]
    pub reset_time_minutes_local: u16,
    #[graphql(default)]
    pub weekly_reset_weekday: ServerDownloadQuotaWeekdayGql,
    #[graphql(default = 1)]
    pub monthly_reset_day: u8,
}

impl TryFrom<ServerDownloadQuotaInput> for ServerDownloadQuotaConfig {
    type Error = String;

    fn try_from(value: ServerDownloadQuotaInput) -> Result<Self, Self::Error> {
        let config = Self {
            enabled: value.enabled,
            limit_bytes: value.limit_bytes,
            period: value.period.into(),
            reset_time_minutes_local: value.reset_time_minutes_local,
            weekly_reset_weekday: value.weekly_reset_weekday.into(),
            monthly_reset_day: value.monthly_reset_day,
        };
        config.validate()?;
        Ok(config)
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct Server {
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub connections: u16,
    pub active: bool,
    pub supports_pipelining: bool,
    pub priority: u32,
    pub backfill: bool,
    pub retention_days: u32,
    pub max_download_speed: u64,
    pub download_quota: ServerDownloadQuota,
    pub tls_ca_cert: Option<String>,
}

impl Server {
    pub fn from_config(
        s: &weaver_server_core::servers::ServerConfig,
        snapshot: Option<&ServerDownloadQuotaSnapshot>,
    ) -> Self {
        Self {
            id: s.id,
            host: s.host.clone(),
            port: s.port,
            tls: s.tls,
            connections: s.connections,
            active: s.active,
            supports_pipelining: s.supports_pipelining,
            priority: s.priority,
            backfill: s.backfill,
            retention_days: s.retention_days,
            max_download_speed: s.max_download_speed,
            download_quota: ServerDownloadQuota::from_config(&s.download_quota, snapshot),
            tls_ca_cert: s.tls_ca_cert.as_ref().map(|p| p.display().to_string()),
        }
    }
}

impl From<&weaver_server_core::servers::ServerConfig> for Server {
    fn from(s: &weaver_server_core::servers::ServerConfig) -> Self {
        Self::from_config(s, None)
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ServerDetails {
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub connections: u16,
    pub active: bool,
    pub supports_pipelining: bool,
    pub priority: u32,
    pub backfill: bool,
    pub retention_days: u32,
    pub max_download_speed: u64,
    pub download_quota: ServerDownloadQuota,
    pub tls_ca_cert: Option<String>,
}

impl ServerDetails {
    pub fn from_config(
        s: &weaver_server_core::servers::ServerConfig,
        snapshot: Option<&ServerDownloadQuotaSnapshot>,
    ) -> Self {
        Self {
            id: s.id,
            host: s.host.clone(),
            port: s.port,
            tls: s.tls,
            username: s.username.clone(),
            connections: s.connections,
            active: s.active,
            supports_pipelining: s.supports_pipelining,
            priority: s.priority,
            backfill: s.backfill,
            retention_days: s.retention_days,
            max_download_speed: s.max_download_speed,
            download_quota: ServerDownloadQuota::from_config(&s.download_quota, snapshot),
            tls_ca_cert: s.tls_ca_cert.as_ref().map(|p| p.display().to_string()),
        }
    }
}

impl From<&weaver_server_core::servers::ServerConfig> for ServerDetails {
    fn from(s: &weaver_server_core::servers::ServerConfig) -> Self {
        Self::from_config(s, None)
    }
}

#[derive(Debug, InputObject)]
pub struct ServerInput {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connections: u16,
    #[graphql(default = true)]
    pub active: bool,
    #[graphql(default = 0)]
    pub priority: u16,
    /// Backfill servers only serve articles the fill tier is missing.
    #[graphql(default = false)]
    pub backfill: bool,
    /// Days of retention this server holds (0 = unlimited).
    #[graphql(default = 0)]
    pub retention_days: u32,
    /// Aggregate bytes/second for this server. Omitted updates preserve the value.
    pub max_download_speed: Option<u64>,
    /// Hard BODY-byte quota. Omitted updates preserve the current policy.
    pub download_quota: Option<ServerDownloadQuotaInput>,
    pub tls_ca_cert: Option<String>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TestConnectionResult {
    pub success: bool,
    pub message: String,
    pub latency_ms: Option<u64>,
    pub supports_pipelining: bool,
}

impl From<weaver_server_core::servers::ServerConnectivityResult> for TestConnectionResult {
    fn from(result: weaver_server_core::servers::ServerConnectivityResult) -> Self {
        Self {
            success: result.success,
            message: result.message,
            latency_ms: result.latency_ms,
            supports_pipelining: result.supports_pipelining,
        }
    }
}
