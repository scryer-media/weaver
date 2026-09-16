use super::{Consumer, ProxyProfile, RoutingPolicy};
use crate::persistence::{
    encryption::{decrypt_value, encrypt_value},
    sql_runtime::{SqlArg, SqlRuntime},
};
use crate::{Database, StateError};

fn error(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

pub(crate) async fn write_routing(
    tx: &mut crate::persistence::sql_runtime::SqlTx<'_>,
    consumer: Consumer,
    policy: Option<&RoutingPolicy>,
) -> Result<(), StateError> {
    let Some(policy) = policy else {
        return Ok(());
    };
    policy.validate().map_err(error)?;
    for id in &policy.proxy_ids {
        if tx
            .fetch_optional(
                "SELECT id FROM proxy_profiles WHERE id = {}",
                &[SqlArg::I64(i64::from(*id))],
            )
            .await?
            .is_none()
        {
            return Err(error("routing policy references a missing proxy"));
        }
    }
    tx.execute("INSERT INTO proxy_routes (consumer, policy) VALUES ({}, {}) ON CONFLICT(consumer) DO UPDATE SET policy = excluded.policy", &[SqlArg::Text(consumer.key()), SqlArg::Text(serde_json::to_string(policy).map_err(error)?)]).await?;
    Ok(())
}

impl Database {
    pub fn list_proxy_profiles(&self) -> Result<Vec<ProxyProfile>, StateError> {
        let store = self.datastore();
        let key = self.encryption_key().cloned();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT id, config, password FROM proxy_profiles ORDER BY id",
                &[],
            )
            .await?;
            rows.into_iter()
                .map(|row| {
                    let mut profile: ProxyProfile =
                        serde_json::from_str(&row.text("config")?).map_err(error)?;
                    profile.id = u32::try_from(row.i64("id")?).map_err(error)?;
                    if let Some(encrypted) = row.opt_text("password")? {
                        let key = key.as_ref().ok_or_else(|| {
                            error("encryption key required for proxy credentials")
                        })?;
                        let plaintext = decrypt_value(key, &encrypted)
                            .map_err(|_| error("cannot decrypt proxy credentials"))?;
                        profile.secrets = serde_json::from_str(&plaintext)
                            .map_err(|_| error("invalid stored proxy credentials"))?;
                    }
                    Ok(profile)
                })
                .collect()
        })
    }

    pub fn save_proxy_profile(&self, profile: &ProxyProfile) -> Result<ProxyProfile, StateError> {
        self.write_proxy_profile(profile, false)
    }

    pub fn reset_proxy_host_key(&self, profile: &ProxyProfile) -> Result<ProxyProfile, StateError> {
        if profile.kind != super::ProxyKind::Ssh {
            return Err(error("only SSH profiles have host-key trust"));
        }
        self.write_proxy_profile(profile, true)
    }

    fn write_proxy_profile(
        &self,
        profile: &ProxyProfile,
        reset_trust: bool,
    ) -> Result<ProxyProfile, StateError> {
        profile.validate().map_err(error)?;
        let key = self
            .encryption_key()
            .ok_or_else(|| error("encryption key required for proxy credentials"))?;
        let password = encrypt_value(
            key,
            &serde_json::to_string(&profile.secrets).map_err(error)?,
        )
        .map_err(error)?;
        let profile = profile.clone();
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "save_proxy_profile", |tx| {
                let mut profile = profile.clone();
                let password = password.clone();
                Box::pin(async move {
                    let id = [SqlArg::I64(i64::from(profile.id))];
                    // A write before the read serializes saves and TOFU pins on
                    // both SQLite and PostgreSQL, including READ COMMITTED.
                    tx.execute("UPDATE proxy_profiles SET id = id WHERE id = {}", &id).await?;
                    if let Some(row) = tx.fetch_optional("SELECT config FROM proxy_profiles WHERE id = {}", &id).await? {
                        let existing: ProxyProfile = serde_json::from_str(&row.text("config")?).map_err(error)?;
                        if profile.revision < existing.revision || (reset_trust && profile.revision == existing.revision) {
                            return Err(error("proxy revision changed"));
                        }
                        profile.host_key_fingerprint = if reset_trust { None } else { existing.host_key_fingerprint.clone() };
                        // Key rotation rewrites encrypted secrets without changing
                        // the public profile or its runtime revision.
                        if profile.revision == existing.revision && serde_json::to_value(&profile).map_err(error)? != serde_json::to_value(&existing).map_err(error)? {
                            return Err(error("proxy revision changed"));
                        }
                    } else if reset_trust {
                        return Err(error("proxy no longer exists"));
                    }
                    tx.execute("INSERT INTO proxy_profiles (id, config, password) VALUES ({}, {}, {}) ON CONFLICT(id) DO UPDATE SET config = excluded.config, password = excluded.password", &[
                        SqlArg::I64(i64::from(profile.id)),
                        SqlArg::Text(serde_json::to_string(&profile).map_err(error)?),
                        SqlArg::Text(password),
                    ]).await?;
                    Ok(profile)
                })
            }).await
        })
    }

    pub fn delete_proxy_profile(&self, id: u32) -> Result<(), StateError> {
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "delete_proxy", |tx| {
                Box::pin(async move {
                    for row in tx.fetch_all("SELECT policy FROM proxy_routes", &[]).await? {
                        let policy: RoutingPolicy =
                            serde_json::from_str(&row.text("policy")?).map_err(error)?;
                        if policy.proxy_ids.contains(&id) {
                            return Err(error("proxy is referenced by a server or RSS feed"));
                        }
                    }
                    tx.execute(
                        "DELETE FROM proxy_profiles WHERE id = {}",
                        &[SqlArg::I64(i64::from(id))],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn list_proxy_routes(&self) -> Result<Vec<(String, RoutingPolicy)>, StateError> {
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT consumer, policy FROM proxy_routes",
                &[],
            )
            .await?
            .into_iter()
            .map(|r| {
                Ok((
                    r.text("consumer")?,
                    serde_json::from_str(&r.text("policy")?).map_err(error)?,
                ))
            })
            .collect()
        })
    }

    pub fn proxy_routing_policy(&self, consumer: Consumer) -> Result<RoutingPolicy, StateError> {
        let store = self.datastore();
        let key = consumer.key();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                store.read_exec(),
                "SELECT policy FROM proxy_routes WHERE consumer = {}",
                &[SqlArg::Text(key)],
            )
            .await?
            .map(|r| serde_json::from_str(&r.text("policy")?).map_err(error))
            .transpose()
            .map(|v| v.unwrap_or_default())
        })
    }

    pub fn save_proxy_routing_policy(
        &self,
        consumer: Consumer,
        policy: &RoutingPolicy,
    ) -> Result<(), StateError> {
        policy.validate().map_err(error)?;
        let key = consumer.key();
        let policy = policy.clone();
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "save_proxy_route", |tx| {
                let key = key.clone(); let policy = policy.clone();
                Box::pin(async move {
                    for id in &policy.proxy_ids {
                        if tx.fetch_optional("SELECT id FROM proxy_profiles WHERE id = {}", &[SqlArg::I64(i64::from(*id))]).await?.is_none() { return Err(error("routing policy references a missing proxy")); }
                    }
                    tx.execute("INSERT INTO proxy_routes (consumer, policy) VALUES ({}, {}) ON CONFLICT(consumer) DO UPDATE SET policy = excluded.policy", &[SqlArg::Text(key), SqlArg::Text(serde_json::to_string(&policy).map_err(error)?)]).await?;
                    Ok(())
                })
            }).await
        })
    }

    pub fn proxy_host_key(&self, id: u32, revision: u64) -> Result<Option<String>, StateError> {
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                store.read_exec(),
                "SELECT config FROM proxy_profiles WHERE id = {}",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?
            .ok_or_else(|| error("proxy no longer exists"))?;
            let profile: ProxyProfile =
                serde_json::from_str(&row.text("config")?).map_err(error)?;
            if profile.revision != revision {
                return Err(error("proxy revision changed"));
            }
            Ok(profile.host_key_fingerprint)
        })
    }

    /// Pin only the revision that authenticated; stale handshakes cannot undo an edit.
    pub fn pin_proxy_host_key(
        &self,
        id: u32,
        revision: u64,
        fingerprint: &str,
    ) -> Result<(), StateError> {
        let store = self.datastore();
        let fingerprint = fingerprint.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "pin_proxy_host_key", |tx| {
                let fingerprint = fingerprint.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE proxy_profiles SET id = id WHERE id = {}",
                        &[SqlArg::I64(i64::from(id))],
                    )
                    .await?;
                    let Some(row) = tx
                        .fetch_optional(
                            "SELECT config FROM proxy_profiles WHERE id = {}",
                            &[SqlArg::I64(i64::from(id))],
                        )
                        .await?
                    else {
                        return Err(error("proxy no longer exists"));
                    };
                    let mut profile: ProxyProfile =
                        serde_json::from_str(&row.text("config")?).map_err(error)?;
                    if profile.revision != revision {
                        return Err(error("proxy revision changed"));
                    }
                    if profile
                        .host_key_fingerprint
                        .as_ref()
                        .is_some_and(|pin| pin != &fingerprint)
                    {
                        return Err(error("SSH host key does not match persisted trust"));
                    }
                    if profile.host_key_fingerprint.is_none() {
                        profile.host_key_fingerprint = Some(fingerprint);
                        tx.execute(
                            "UPDATE proxy_profiles SET config = {} WHERE id = {}",
                            &[
                                SqlArg::Text(serde_json::to_string(&profile).map_err(error)?),
                                SqlArg::I64(i64::from(id)),
                            ],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }
}
