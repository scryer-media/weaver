// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The server writes the servers service makes, on the plain database, so the
// settings below the scheduler can make them too.
use crate::servers::ServerConfig;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::connection::Database as SqlDatabase;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRuntime};

pub trait ServersServiceQueries {
    fn set_server_max_download_speed(
        &self,
        server_id: u32,
        bytes_per_sec: u64,
    ) -> Result<(), StateError>;
    fn set_server_active(&self, server_id: u32, active: bool) -> Result<(), StateError>;
}

impl ServersServiceQueries for SqlDatabase {
    fn set_server_max_download_speed(
        &self,
        server_id: u32,
        bytes_per_sec: u64,
    ) -> Result<(), StateError> {
        let stored = i64::try_from(bytes_per_sec)
            .map_err(|_| StateError::Database("speed limit is out of range".into()))?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE servers SET max_download_speed = {} WHERE id = {}",
                &[SqlArg::I64(stored), SqlArg::I64(i64::from(server_id))],
            )
            .await?;
            if changed == 0 {
                return Err(StateError::Database(format!(
                    "server {server_id} not found"
                )));
            }
            Ok(())
        })
    }

    // Turn a server on or off. An activated server's connections are never
    // refused here: the tunnels its route reaches hold usenet to their bytes
    // as its sockets open.
    fn set_server_active(&self, server_id: u32, active: bool) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_server_active", |tx| {
                Box::pin(async move {
                    crate::proxies::lock_network(tx).await?;
                    let changed = tx
                        .execute(
                            "UPDATE servers SET active = {} WHERE id = {}",
                            &[SqlArg::Bool(active), SqlArg::I64(i64::from(server_id))],
                        )
                        .await?;
                    if changed == 0 {
                        return Err(StateError::Database(format!(
                            "server {server_id} not found"
                        )));
                    }
                    Ok(())
                })
            })
            .await
        })
    }
}

pub trait ServersServiceInternalQueries {
    fn replace_servers(&self, servers: &[ServerConfig]) -> Result<(), StateError>;
}

impl ServersServiceInternalQueries for SqlDatabase {
    fn replace_servers(&self, servers: &[ServerConfig]) -> Result<(), StateError> {
        use weaver_server_db::persistence::encryption::encrypt_secret_for_write;

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
                                    (id, host, port, tls, username, password, connections, active, supports_pipelining, pipelining_depth, priority, backfill, retention_days, max_download_speed, download_quota_enabled, download_quota_limit_bytes, download_quota_period, download_quota_reset_time_minutes_local, download_quota_weekly_reset_weekday, download_quota_monthly_reset_day, tls_ca_cert, tls_name_mismatch_certificate_der)
                                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                                 ON CONFLICT(id) DO UPDATE SET
                                    host = excluded.host,
                                    port = excluded.port,
                                    tls = excluded.tls,
                                    username = excluded.username,
                                    password = excluded.password,
                                    connections = excluded.connections,
                                    active = excluded.active,
                                    supports_pipelining = excluded.supports_pipelining,
                                    pipelining_depth = excluded.pipelining_depth,
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
                                    tls_ca_cert = excluded.tls_ca_cert,
                                    tls_name_mismatch_certificate_der = excluded.tls_name_mismatch_certificate_der",
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
