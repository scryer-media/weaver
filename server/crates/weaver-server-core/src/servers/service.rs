use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::servers::ServerConfig;

/// Shared serialization for server mutations and whole-generation reloads.
pub static SERVER_MUTATION_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone)]
pub struct ServersService {
    db: Database,
    config: crate::settings::SharedConfig,
    handle: crate::SchedulerHandle,
}

impl ServersService {
    pub fn new(
        db: Database,
        config: crate::settings::SharedConfig,
        handle: crate::SchedulerHandle,
    ) -> Self {
        Self { db, config, handle }
    }

    /// Persist operator intent even when the provider is offline. Scheduled
    /// activation deliberately performs no connection probe.
    pub async fn set_active(&self, server_id: u32, active: bool) -> Result<(), String> {
        let _guard = SERVER_MUTATION_GUARD.lock().await;
        let previous_active = self
            .config
            .read()
            .await
            .servers
            .iter()
            .find(|server| server.id == server_id)
            .ok_or_else(|| format!("server {server_id} is missing from the runtime configuration"))?
            .active;
        if previous_active == active {
            return Ok(());
        }
        let proxy_runtime = self.handle.proxy_runtime();
        let _proxy_guard = match &proxy_runtime {
            Some(runtime) => Some(runtime.mutations.lock().await),
            None => None,
        };
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.set_server_active(server_id, active))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        {
            let mut config = self.config.write().await;
            let server = config
                .servers
                .iter_mut()
                .find(|server| server.id == server_id)
                .ok_or_else(|| {
                    format!("server {server_id} is missing from the runtime configuration")
                })?;
            server.active = active;
        }
        if let Err(error) =
            crate::runtime::reload::rebuild_nntp_from_config(&self.config, &self.handle).await
        {
            // Failed activations must leave persistence and runtime in agreement,
            // including when the next attempt happens after a process restart.
            if let Some(server) = self
                .config
                .write()
                .await
                .servers
                .iter_mut()
                .find(|server| server.id == server_id)
            {
                server.active = previous_active;
            }
            let db = self.db.clone();
            let rollback = tokio::task::spawn_blocking(move || {
                db.set_server_active(server_id, previous_active)
            })
            .await;
            if !matches!(rollback, Ok(Ok(()))) {
                return Err(format!(
                    "{error}; restoring server activation failed: {rollback:?}"
                ));
            }
            return Err(error.to_string());
        }
        Ok(())
    }
}

impl Database {
    pub fn set_server_active(&self, server_id: u32, active: bool) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
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
    }

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
