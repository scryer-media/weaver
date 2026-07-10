use crate::bandwidth::IspBandwidthCapWeekday;
use crate::servers::{ServerConfig, ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServerRecord {
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connections: u16,
    pub active: bool,
    pub supports_pipelining: bool,
    pub priority: u32,
    pub backfill: bool,
    pub retention_days: u32,
    pub max_download_speed: u64,
    pub download_quota_enabled: bool,
    pub download_quota_limit_bytes: u64,
    pub download_quota_period: ServerDownloadQuotaPeriod,
    pub download_quota_reset_time_minutes_local: u16,
    pub download_quota_weekly_reset_weekday: IspBandwidthCapWeekday,
    pub download_quota_monthly_reset_day: u8,
    pub tls_ca_cert: Option<String>,
}

impl ServerRecord {
    pub(crate) fn into_config(self) -> ServerConfig {
        ServerConfig {
            id: self.id,
            host: self.host,
            port: self.port,
            tls: self.tls,
            username: self.username,
            password: self.password,
            connections: self.connections,
            active: self.active,
            supports_pipelining: self.supports_pipelining,
            priority: self.priority,
            backfill: self.backfill,
            retention_days: self.retention_days,
            max_download_speed: self.max_download_speed,
            download_quota: ServerDownloadQuotaConfig {
                enabled: self.download_quota_enabled,
                limit_bytes: self.download_quota_limit_bytes,
                period: self.download_quota_period,
                reset_time_minutes_local: self.download_quota_reset_time_minutes_local,
                weekly_reset_weekday: self.download_quota_weekly_reset_weekday,
                monthly_reset_day: self.download_quota_monthly_reset_day,
            },
            tls_ca_cert: self.tls_ca_cert.map(std::path::PathBuf::from),
        }
    }

    pub(crate) fn from_config(server: &ServerConfig) -> Self {
        Self {
            id: server.id,
            host: server.host.clone(),
            port: server.port,
            tls: server.tls,
            username: server.username.clone(),
            password: server.password.clone(),
            connections: server.connections,
            active: server.active,
            supports_pipelining: server.supports_pipelining,
            priority: server.priority,
            backfill: server.backfill,
            retention_days: server.retention_days,
            max_download_speed: server.max_download_speed,
            download_quota_enabled: server.download_quota.enabled,
            download_quota_limit_bytes: server.download_quota.limit_bytes,
            download_quota_period: server.download_quota.period,
            download_quota_reset_time_minutes_local: server.download_quota.reset_time_minutes_local,
            download_quota_weekly_reset_weekday: server.download_quota.weekly_reset_weekday,
            download_quota_monthly_reset_day: server.download_quota.monthly_reset_day,
            tls_ca_cert: server
                .tls_ca_cert
                .as_ref()
                .map(|path| path.display().to_string()),
        }
    }
}

pub(crate) fn quota_weekday_str(value: IspBandwidthCapWeekday) -> &'static str {
    match value {
        IspBandwidthCapWeekday::Mon => "mon",
        IspBandwidthCapWeekday::Tue => "tue",
        IspBandwidthCapWeekday::Wed => "wed",
        IspBandwidthCapWeekday::Thu => "thu",
        IspBandwidthCapWeekday::Fri => "fri",
        IspBandwidthCapWeekday::Sat => "sat",
        IspBandwidthCapWeekday::Sun => "sun",
    }
}

pub(crate) fn parse_quota_weekday(value: &str) -> Option<IspBandwidthCapWeekday> {
    match value {
        "mon" => Some(IspBandwidthCapWeekday::Mon),
        "tue" => Some(IspBandwidthCapWeekday::Tue),
        "wed" => Some(IspBandwidthCapWeekday::Wed),
        "thu" => Some(IspBandwidthCapWeekday::Thu),
        "fri" => Some(IspBandwidthCapWeekday::Fri),
        "sat" => Some(IspBandwidthCapWeekday::Sat),
        "sun" => Some(IspBandwidthCapWeekday::Sun),
        _ => None,
    }
}
