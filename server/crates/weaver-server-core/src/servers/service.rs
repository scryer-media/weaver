use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::servers::ServerConfig;

impl Database {
    pub(crate) fn replace_servers(&self, servers: &[ServerConfig]) -> Result<(), StateError> {
        use crate::persistence::encryption::encrypt_secret_for_write;

        let datastore = self.datastore();
        let encryption_key = self.encryption_key().cloned();
        self.run_sql_blocking({
            let servers = servers.to_vec();
            async move {
                SqlRuntime::run_in_transaction(&datastore, "replace_servers", |tx| {
                    let servers = servers.clone();
                    let encryption_key = encryption_key.clone();
                    Box::pin(async move {
                        let existing_ids = tx
                            .fetch_all("SELECT id FROM servers", &[])
                            .await?
                            .into_iter()
                            .map(|row| {
                                let id = row.i64("id")?;
                                u32::try_from(id).map_err(|_| {
                                    StateError::Database(
                                        "server id is outside the supported range".to_string(),
                                    )
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        let desired_ids = servers
                            .iter()
                            .map(|server| server.id)
                            .collect::<std::collections::HashSet<_>>();
                        for server in servers {
                            let record = crate::servers::record::ServerRecord::from_config(&server);
                            let encrypted_password =
                                encrypt_secret_for_write(encryption_key.as_ref(), &record.password)
                                    .map_err(StateError::Database)?;
                            let args = crate::servers::persistence::server_args(
                                record,
                                encrypted_password,
                            )?;
                            tx.execute(
                                "INSERT INTO servers
                                    (id, host, port, tls, username, password, connections, active, supports_pipelining, priority, backfill, retention_days, max_download_speed, download_quota_enabled, download_quota_limit_bytes, download_quota_period, download_quota_reset_time_minutes_local, download_quota_weekly_reset_weekday, download_quota_monthly_reset_day, tls_ca_cert)
                                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                                 ON CONFLICT(id) DO UPDATE SET
                                    host = excluded.host,
                                    port = excluded.port,
                                    tls = excluded.tls,
                                    username = excluded.username,
                                    password = excluded.password,
                                    connections = excluded.connections,
                                    active = excluded.active,
                                    supports_pipelining = excluded.supports_pipelining,
                                    priority = excluded.priority,
                                    backfill = excluded.backfill,
                                    retention_days = excluded.retention_days,
                                    max_download_speed = excluded.max_download_speed,
                                    download_quota_enabled = excluded.download_quota_enabled,
                                    download_quota_limit_bytes = excluded.download_quota_limit_bytes,
                                    download_quota_period = excluded.download_quota_period,
                                    download_quota_reset_time_minutes_local = excluded.download_quota_reset_time_minutes_local,
                                    download_quota_weekly_reset_weekday = excluded.download_quota_weekly_reset_weekday,
                                    download_quota_monthly_reset_day = excluded.download_quota_monthly_reset_day,
                                    tls_ca_cert = excluded.tls_ca_cert",
                                &args,
                            )
                            .await?;
                        }
                        for id in existing_ids {
                            if !desired_ids.contains(&id) {
                                tx.execute(
                                    "DELETE FROM servers WHERE id = {}",
                                    &[SqlArg::I64(i64::from(id))],
                                )
                                .await?;
                            }
                        }
                        Ok(())
                    })
                })
                .await
            }
        })
    }
}
