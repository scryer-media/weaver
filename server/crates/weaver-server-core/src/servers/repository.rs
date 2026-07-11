use chrono::{DateTime, TimeZone, Utc};

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime};
use crate::servers::ServerDownloadUsage;

impl Database {
    pub fn server_download_usage(
        &self,
        server_id: u32,
    ) -> Result<Option<ServerDownloadUsage>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT server_id, lifetime_bytes, quota_baseline_bytes,
                        window_start_epoch_seconds, window_end_epoch_seconds,
                        updated_at_epoch_seconds
                   FROM server_download_usage
                  WHERE server_id = {}",
                &[SqlArg::I64(i64::from(server_id))],
            )
            .await?
            .map(server_download_usage_from_row)
            .transpose()
        })
    }

    pub fn list_server_download_usage(&self) -> Result<Vec<ServerDownloadUsage>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT server_id, lifetime_bytes, quota_baseline_bytes,
                        window_start_epoch_seconds, window_end_epoch_seconds,
                        updated_at_epoch_seconds
                   FROM server_download_usage
                  ORDER BY server_id",
                &[],
            )
            .await?
            .into_iter()
            .map(server_download_usage_from_row)
            .collect()
        })
    }

    pub fn upsert_server_download_usage(
        &self,
        usage: &ServerDownloadUsage,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = server_download_usage_args(usage)?;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO server_download_usage
                    (server_id, lifetime_bytes, quota_baseline_bytes,
                     window_start_epoch_seconds, window_end_epoch_seconds,
                     updated_at_epoch_seconds)
                 VALUES ({}, {}, {}, {}, {}, {})
                 ON CONFLICT(server_id) DO UPDATE SET
                    lifetime_bytes = excluded.lifetime_bytes,
                    quota_baseline_bytes = excluded.quota_baseline_bytes,
                    window_start_epoch_seconds = excluded.window_start_epoch_seconds,
                    window_end_epoch_seconds = excluded.window_end_epoch_seconds,
                    updated_at_epoch_seconds = excluded.updated_at_epoch_seconds",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn reset_server_download_usage(
        &self,
        server_id: u32,
    ) -> Result<ServerDownloadUsage, StateError> {
        let datastore = self.datastore();
        let updated_at = Utc::now().timestamp();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO server_download_usage
                    (server_id, lifetime_bytes, quota_baseline_bytes,
                     window_start_epoch_seconds, window_end_epoch_seconds,
                     updated_at_epoch_seconds)
                 VALUES ({}, 0, 0, NULL, NULL, {})
                 ON CONFLICT(server_id) DO UPDATE SET
                    quota_baseline_bytes = server_download_usage.lifetime_bytes,
                    window_start_epoch_seconds = NULL,
                    window_end_epoch_seconds = NULL,
                    updated_at_epoch_seconds = excluded.updated_at_epoch_seconds",
                &[SqlArg::I64(i64::from(server_id)), SqlArg::I64(updated_at)],
            )
            .await?;
            Ok(())
        })?;
        self.server_download_usage(server_id)?.ok_or_else(|| {
            StateError::Database(format!(
                "server download usage reset did not create state for server {server_id}"
            ))
        })
    }
}

fn server_download_usage_from_row(row: SqlRow) -> Result<ServerDownloadUsage, StateError> {
    Ok(ServerDownloadUsage {
        server_id: u32_from_i64(row.i64("server_id")?, "server id")?,
        lifetime_bytes: u64_from_i64(row.i64("lifetime_bytes")?, "lifetime bytes")?,
        quota_baseline_bytes: u64_from_i64(
            row.i64("quota_baseline_bytes")?,
            "quota baseline bytes",
        )?,
        window_start: optional_timestamp(
            row.opt_i64("window_start_epoch_seconds")?,
            "window start",
        )?,
        window_end: optional_timestamp(row.opt_i64("window_end_epoch_seconds")?, "window end")?,
        updated_at: timestamp(row.i64("updated_at_epoch_seconds")?, "updated at")?,
    })
}

fn server_download_usage_args(usage: &ServerDownloadUsage) -> Result<Vec<SqlArg>, StateError> {
    Ok(vec![
        SqlArg::I64(i64::from(usage.server_id)),
        SqlArg::I64(i64_from_u64(usage.lifetime_bytes, "lifetime bytes")?),
        SqlArg::I64(i64_from_u64(
            usage.quota_baseline_bytes,
            "quota baseline bytes",
        )?),
        SqlArg::OptI64(usage.window_start.map(|value| value.timestamp())),
        SqlArg::OptI64(usage.window_end.map(|value| value.timestamp())),
        SqlArg::I64(usage.updated_at.timestamp()),
    ])
}

fn i64_from_u64(value: u64, field: &str) -> Result<i64, StateError> {
    i64::try_from(value)
        .map_err(|_| StateError::Database(format!("server download usage {field} is too large")))
}

fn u64_from_i64(value: i64, field: &str) -> Result<u64, StateError> {
    u64::try_from(value).map_err(|_| {
        StateError::Database(format!(
            "server download usage {field} must not be negative"
        ))
    })
}

fn u32_from_i64(value: i64, field: &str) -> Result<u32, StateError> {
    u32::try_from(value)
        .map_err(|_| StateError::Database(format!("server download usage {field} is invalid")))
}

fn optional_timestamp(
    value: Option<i64>,
    field: &str,
) -> Result<Option<DateTime<Utc>>, StateError> {
    value.map(|value| timestamp(value, field)).transpose()
}

fn timestamp(value: i64, field: &str) -> Result<DateTime<Utc>, StateError> {
    Utc.timestamp_opt(value, 0)
        .single()
        .ok_or_else(|| StateError::Database(format!("server download usage {field} is invalid")))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::servers::{ServerConfig, ServerDownloadQuotaConfig};

    fn server(id: u32) -> ServerConfig {
        ServerConfig {
            id,
            host: format!("news-{id}.example.com"),
            port: 563,
            tls: true,
            username: None,
            password: None,
            connections: 4,
            active: true,
            supports_pipelining: false,
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: 0,
            download_quota: ServerDownloadQuotaConfig::default(),
            tls_ca_cert: None::<PathBuf>,
        }
    }

    #[test]
    fn usage_roundtrip_reset_and_delete_cascade() {
        let db = Database::open_in_memory().unwrap();
        db.insert_server(&server(7)).unwrap();
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let end = DateTime::from_timestamp(1_700_086_400, 0).unwrap();
        let updated_at = DateTime::from_timestamp(1_700_000_123, 0).unwrap();
        let usage = ServerDownloadUsage {
            server_id: 7,
            lifetime_bytes: 5_000,
            quota_baseline_bytes: 1_000,
            window_start: Some(start),
            window_end: Some(end),
            updated_at,
        };

        db.upsert_server_download_usage(&usage).unwrap();
        assert_eq!(db.server_download_usage(7).unwrap(), Some(usage.clone()));
        let mut updated_server = server(7);
        updated_server.connections = 8;
        db.replace_servers(&[updated_server]).unwrap();
        assert_eq!(db.server_download_usage(7).unwrap(), Some(usage.clone()));
        assert_eq!(db.list_server_download_usage().unwrap(), vec![usage]);

        let reset = db.reset_server_download_usage(7).unwrap();
        assert_eq!(reset.lifetime_bytes, 5_000);
        assert_eq!(reset.quota_baseline_bytes, 5_000);
        assert_eq!(reset.used_bytes(), 0);
        assert_eq!(reset.window_start, None);
        assert_eq!(reset.window_end, None);

        assert!(db.delete_server(7).unwrap());
        assert_eq!(db.server_download_usage(7).unwrap(), None);
    }
}
