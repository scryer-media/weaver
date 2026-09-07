use crate::StateError;
use crate::bandwidth::ScheduleEntry;
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, StoreDatastore};

const NEXT_JOB_ID_SETTING_KEY: &str = "next_job_id";

/// The bind-address portion of an authenticated network-policy update.
/// Keeping `Unchanged` distinct from `Clear` lets the combined settings form
/// leave a saved listener alone while still offering an explicit reset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkBindAddressUpdate {
    Unchanged,
    Clear,
    Set(String),
}

/// A complete atomic authenticated browser-network policy write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedNetworkAccessUpdate {
    pub trusted_proxies_json: Option<String>,
    /// `None` preserves an environment-pinned trusted-network setting.
    pub trusted_networks_json: Option<String>,
    /// `None` preserves an environment-pinned bind setting.
    pub bind_address: NetworkBindAddressUpdate,
}

impl Database {
    /// Persist the authenticated browser policy in one transaction. The live
    /// runtime snapshot is intentionally updated by the caller only after this
    /// returns successfully.
    pub fn update_authenticated_network_access(
        &self,
        update: &AuthenticatedNetworkAccessUpdate,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let update = update.clone();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_authenticated_network_access", |tx| {
                let update = update.clone();
                Box::pin(async move {
                    if let Some(networks) = update.trusted_networks_json {
                        tx.execute(
                            "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                            &[
                                SqlArg::Text(crate::security::SETTING_TRUSTED_NETWORKS.to_string()),
                                SqlArg::Text(networks),
                            ],
                        ).await?;
                    }
                    if let Some(proxies) = update.trusted_proxies_json {
                        tx.execute(
                            "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                            &[
                                SqlArg::Text(crate::security::SETTING_TRUSTED_PROXIES.to_string()),
                                SqlArg::Text(proxies),
                            ],
                        ).await?;
                    }
                    match update.bind_address {
                        NetworkBindAddressUpdate::Unchanged => {}
                        NetworkBindAddressUpdate::Clear => {
                            tx.execute(
                                "DELETE FROM settings WHERE key = {}",
                                &[SqlArg::Text(crate::security::SETTING_HTTP_BIND_ADDRESS.to_string())],
                            ).await?;
                        }
                        NetworkBindAddressUpdate::Set(address) => {
                            tx.execute(
                                "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                                &[
                                    SqlArg::Text(crate::security::SETTING_HTTP_BIND_ADDRESS.to_string()),
                                    SqlArg::Text(address),
                                ],
                            ).await?;
                        }
                    }
                    tx.execute(
                        "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                        &[
                            SqlArg::Text(crate::security::SETTING_SECURITY_POLICY_REVISION.to_string()),
                            SqlArg::Text(crate::security::AUTHENTICATED_POLICY_REVISION.to_string()),
                        ],
                    ).await?;
                    Ok(())
                })
            }).await
        })
    }

    pub fn set_setting(&self, key: &str, value: &str) -> Result<(), StateError> {
        let datastore = self.datastore();
        let key = key.to_string();
        let value = value.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO settings (key, value) VALUES ({}, {})
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                &[SqlArg::Text(key), SqlArg::Text(value)],
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_setting(&self, key: &str) -> Result<(), StateError> {
        let datastore = self.datastore();
        let key = key.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM settings WHERE key = {}",
                &[SqlArg::Text(key)],
            )
            .await?;
            Ok(())
        })
    }

    /// Delete `key` only if its stored value still equals `expected`, atomically
    /// in one statement. Closes the read-then-delete TOCTOU a two-round-trip
    /// value-compare would leave open (a concurrent writer replacing the value
    /// between the read and the delete).
    pub fn delete_setting_if_value(&self, key: &str, expected: &str) -> Result<(), StateError> {
        let datastore = self.datastore();
        let key = key.to_string();
        let expected = expected.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM settings WHERE key = {} AND value = {}",
                &[SqlArg::Text(key), SqlArg::Text(expected)],
            )
            .await?;
            Ok(())
        })
    }

    pub fn save_schedules(&self, entries: &[ScheduleEntry]) -> Result<(), StateError> {
        let json =
            serde_json::to_string(entries).map_err(|e| StateError::Database(e.to_string()))?;
        self.set_setting("schedules", &json)
    }

    pub fn initialize_next_job_id_counter(&self) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let next_job_id = next_job_id_floor_target(&datastore).await?;
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO settings (key, value) VALUES ({}, {})
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                &[
                    SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string()),
                    SqlArg::Text(next_job_id.to_string()),
                ],
            )
            .await?;
            Ok(next_job_id)
        })
    }

    pub fn reserve_next_job_id(&self) -> Result<JobId, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "reserve_next_job_id", |tx| {
                Box::pin(async move {
                    let next_job_id = reserve_next_job_id_tx(tx).await?;
                    Ok(JobId(next_job_id))
                })
            })
            .await
        })
    }
}

async fn next_job_id_floor_target(datastore: &StoreDatastore) -> Result<u64, StateError> {
    let persisted = SqlRuntime::fetch_optional(
        datastore.read_exec(),
        "SELECT value FROM settings WHERE key = {}",
        &[SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string())],
    )
    .await?
    .and_then(|row| row.text("value").ok())
    .and_then(|value| value.parse::<u64>().ok())
    .unwrap_or(10_000);

    let row = SqlRuntime::fetch_optional(
        datastore.read_exec(),
        "SELECT MAX(id) AS id FROM (
             SELECT MAX(job_id) AS id FROM active_jobs
             UNION ALL
             SELECT MAX(job_id) AS id FROM job_history
             UNION ALL
             SELECT MAX(job_id) AS id FROM duplicate_job_snapshots
             UNION ALL
             SELECT MAX(job_id) AS id FROM forgotten_duplicate_identities
         ) ids",
        &[],
    )
    .await?;
    let max_seen = row
        .map(|row| row.opt_i64("id"))
        .transpose()?
        .flatten()
        .unwrap_or(0);

    Ok(persisted.max(max_seen as u64 + 1).max(10_000))
}

async fn max_seen_job_id_floor_tx(tx: &mut SqlTx<'_>) -> Result<u64, StateError> {
    let row = tx
        .fetch_optional(
            "SELECT MAX(id) AS id FROM (
                 SELECT MAX(job_id) AS id FROM active_jobs
                 UNION ALL
                 SELECT MAX(job_id) AS id FROM job_history
                 UNION ALL
                 SELECT MAX(job_id) AS id FROM duplicate_job_snapshots
                 UNION ALL
                 SELECT MAX(job_id) AS id FROM forgotten_duplicate_identities
             ) ids",
            &[],
        )
        .await?;
    let max_seen = row
        .map(|row| row.opt_i64("id"))
        .transpose()?
        .flatten()
        .unwrap_or(0);

    Ok((max_seen as u64 + 1).max(10_000))
}

pub(crate) async fn reserve_next_job_id_tx(tx: &mut SqlTx<'_>) -> Result<u64, StateError> {
    let floor = max_seen_job_id_floor_tx(tx).await?;
    tx.execute(
        "INSERT INTO settings (key, value) VALUES ({}, {})
         ON CONFLICT(key) DO NOTHING",
        &[
            SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string()),
            SqlArg::Text(floor.to_string()),
        ],
    )
    .await?;

    let select_sql = match tx {
        SqlTx::Postgres(_) => "SELECT value FROM settings WHERE key = {} FOR UPDATE",
        SqlTx::Sqlite(_) => "SELECT value FROM settings WHERE key = {}",
    };
    let persisted = tx
        .fetch_optional(
            select_sql,
            &[SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string())],
        )
        .await?
        .and_then(|row| row.text("value").ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(floor);
    let next_job_id = persisted.max(floor).max(10_000);
    tx.execute(
        "UPDATE settings SET value = {} WHERE key = {}",
        &[
            SqlArg::Text((next_job_id + 1).to_string()),
            SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string()),
        ],
    )
    .await?;

    Ok(next_job_id)
}
