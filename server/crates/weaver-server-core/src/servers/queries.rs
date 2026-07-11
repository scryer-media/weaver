use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::SqlRuntime;
use crate::servers::{ServerConfig, ServerDownloadQuotaPeriod, record::ServerRecord};

impl Database {
    pub fn list_servers(&self) -> Result<Vec<ServerConfig>, StateError> {
        use crate::persistence::encryption::maybe_decrypt;

        let datastore = self.datastore();
        let encryption_key = self.encryption_key().cloned();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, host, port, tls, username, password, connections, active, supports_pipelining, priority, backfill, retention_days, max_download_speed, download_quota_enabled, download_quota_limit_bytes, download_quota_period, download_quota_reset_time_minutes_local, download_quota_weekly_reset_weekday, download_quota_monthly_reset_day, tls_ca_cert
                   FROM servers ORDER BY priority, id",
                &[],
            )
            .await?;

            rows.into_iter()
                .map(|row| {
                    let mut record = ServerRecord {
                        id: u32_from_i64(row.i64("id")?, "server id")?,
                        host: row.text("host")?,
                        port: u16_from_i64(row.i64("port")?, "server port")?,
                        tls: row.bool("tls")?,
                        username: row.opt_text("username")?,
                        password: row.opt_text("password")?,
                        connections: u16_from_i64(
                            row.i64("connections")?,
                            "server connections",
                        )?,
                        active: row.bool("active")?,
                        supports_pipelining: row.bool("supports_pipelining")?,
                        priority: u32_from_i64(row.i64("priority")?, "server priority")?,
                        backfill: row.bool("backfill")?,
                        retention_days: u32_from_i64(
                            row.i64("retention_days")?,
                            "server retention days",
                        )?,
                        max_download_speed: u64_from_i64(
                            row.i64("max_download_speed")?,
                            "server max download speed",
                        )?,
                        download_quota_enabled: row.bool("download_quota_enabled")?,
                        download_quota_limit_bytes: u64_from_i64(
                            row.i64("download_quota_limit_bytes")?,
                            "server download quota limit",
                        )?,
                        download_quota_period: {
                            let value = row.text("download_quota_period")?;
                            ServerDownloadQuotaPeriod::parse(&value).ok_or_else(|| {
                                StateError::Database(format!(
                                    "invalid server download quota period '{value}'"
                                ))
                            })?
                        },
                        download_quota_reset_time_minutes_local: u16_from_i64(
                            row.i64("download_quota_reset_time_minutes_local")?,
                            "server download quota reset time",
                        )?,
                        download_quota_weekly_reset_weekday: {
                            let value = row.text("download_quota_weekly_reset_weekday")?;
                            crate::servers::record::parse_quota_weekday(&value).ok_or_else(|| {
                                StateError::Database(format!(
                                    "invalid server download quota weekday '{value}'"
                                ))
                            })?
                        },
                        download_quota_monthly_reset_day: u8_from_i64(
                            row.i64("download_quota_monthly_reset_day")?,
                            "server download quota monthly reset day",
                        )?,
                        tls_ca_cert: row.opt_text("tls_ca_cert")?,
                    };
                    record.password = maybe_decrypt(encryption_key.as_ref(), record.password);
                    Ok(record.into_config())
                })
                .collect()
        })
    }

    pub fn next_server_id(&self) -> Result<u32, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(id) AS id FROM servers",
                &[],
            )
            .await?;
            let max = row
                .map(|row| row.opt_i64("id"))
                .transpose()?
                .flatten()
                .unwrap_or(0);
            let max = u32_from_i64(max, "server id")?;
            max.checked_add(1).ok_or_else(|| {
                StateError::Database("no server IDs remain in the database range".to_string())
            })
        })
    }
}

fn u64_from_i64(value: i64, field: &str) -> Result<u64, StateError> {
    u64::try_from(value).map_err(|_| StateError::Database(format!("{field} must not be negative")))
}

fn u32_from_i64(value: i64, field: &str) -> Result<u32, StateError> {
    u32::try_from(value)
        .map_err(|_| StateError::Database(format!("{field} is outside the supported range")))
}

fn u16_from_i64(value: i64, field: &str) -> Result<u16, StateError> {
    u16::try_from(value)
        .map_err(|_| StateError::Database(format!("{field} is outside the supported range")))
}

fn u8_from_i64(value: i64, field: &str) -> Result<u8, StateError> {
    u8::try_from(value)
        .map_err(|_| StateError::Database(format!("{field} is outside the supported range")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_server_integer_conversions_reject_narrowing() {
        assert!(u64_from_i64(-1, "bytes").is_err());
        assert!(u32_from_i64(-1, "id").is_err());
        assert!(u32_from_i64(i64::from(u32::MAX) + 1, "id").is_err());
        assert!(u16_from_i64(i64::from(u16::MAX) + 1, "port").is_err());
        assert!(u8_from_i64(i64::from(u8::MAX) + 1, "day").is_err());
    }
}
