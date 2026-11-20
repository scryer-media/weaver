// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{Consumer, NetworkRefusal, ProxyProfile, RoutingPolicy};
use crate::persistence::{
    encryption::{decrypt_value, encrypt_value},
    sql_runtime::{SqlArg, SqlRuntime},
};
use crate::{Database, StateError};

// A storage fault. Input the operator can correct is a `NetworkRefusal`.
fn error(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

// Write `consumer`'s route after its own row, in a transaction that holds
// the network lock. A server's connection count is never refused here: the
// tunnels hold usenet to their bytes as its sockets open.
pub(crate) async fn write_routing(
    tx: &mut crate::persistence::sql_runtime::SqlTx<'_>,
    consumer: Consumer,
    policy: Option<&RoutingPolicy>,
) -> Result<(), StateError> {
    let Some(policy) = policy else {
        return Ok(());
    };
    let policy = super::network_persistence::validate_policy(tx, consumer, policy).await?;
    insert_route(tx, consumer.key(), &policy).await?;
    super::network_persistence::validate_stored_network(
        tx,
        super::dns_persistence::Touched::Route(consumer),
    )
    .await?;
    Ok(())
}

// Write a route row. A policy that names no DNS profile keeps the stored
// one, or takes the System profile for a new route.
async fn insert_route(
    tx: &mut crate::persistence::sql_runtime::SqlTx<'_>,
    key: String,
    policy: &RoutingPolicy,
) -> Result<(), StateError> {
    let profile = SqlArg::OptI64(policy.dns_profile_id.map(i64::from));
    tx.execute(
        "INSERT INTO proxy_routes (consumer, policy, dns_profile_id) VALUES ({}, {}, COALESCE(CAST({} AS BIGINT), 0)) ON CONFLICT(consumer) DO UPDATE SET policy = excluded.policy, dns_profile_id = COALESCE(CAST({} AS BIGINT), proxy_routes.dns_profile_id)",
        &[
            SqlArg::Text(key),
            SqlArg::Text(serde_json::to_string(policy).map_err(error)?),
            profile.clone(),
            profile,
        ],
    )
    .await?;
    Ok(())
}

// A stored route row's policy, with the DNS profile from its own column.
pub(super) fn route_policy(
    row: &crate::persistence::sql_runtime::SqlRow,
) -> Result<RoutingPolicy, StateError> {
    let mut policy: RoutingPolicy = serde_json::from_str(&row.text("policy")?).map_err(error)?;
    policy.dns_profile_id = Some(u32::try_from(row.i64("dns_profile_id")?).map_err(error)?);
    Ok(policy)
}

pub trait ProxiesPersistenceQueries {
    fn list_proxy_profiles(&self) -> Result<Vec<ProxyProfile>, StateError>;
    fn save_proxy_profile(&self, profile: &ProxyProfile) -> Result<ProxyProfile, StateError>;
    fn reset_proxy_host_key(&self, profile: &ProxyProfile) -> Result<ProxyProfile, StateError>;
    fn delete_proxy_profile(&self, id: u32) -> Result<(), StateError>;
    fn list_proxy_routes(&self) -> Result<Vec<(String, RoutingPolicy)>, StateError>;
    fn proxy_routing_policy(&self, consumer: Consumer) -> Result<RoutingPolicy, StateError>;
    fn save_proxy_routing_policy(
        &self,
        consumer: Consumer,
        policy: &RoutingPolicy,
    ) -> Result<(), StateError>;
    fn proxy_host_key(&self, id: u32, revision: u64) -> Result<Option<String>, StateError>;
    fn pin_proxy_host_key(
        &self,
        id: u32,
        revision: u64,
        fingerprint: &str,
    ) -> Result<(), StateError>;
}

impl ProxiesPersistenceQueries for Database {
    fn list_proxy_profiles(&self) -> Result<Vec<ProxyProfile>, StateError> {
        let store = self.datastore();
        let key = self.encryption_key().cloned();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT id, config, password, dns_profile_id FROM proxy_profiles ORDER BY id",
                &[],
            )
            .await?;
            rows.into_iter()
                .map(|row| {
                    let mut profile: ProxyProfile =
                        serde_json::from_str(&row.text("config")?).map_err(error)?;
                    profile.id = u32::try_from(row.i64("id")?).map_err(error)?;
                    profile.dns_profile_id =
                        u32::try_from(row.i64("dns_profile_id")?).map_err(error)?;
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

    fn save_proxy_profile(&self, profile: &ProxyProfile) -> Result<ProxyProfile, StateError> {
        self.write_proxy_profile(profile, false)
    }

    fn reset_proxy_host_key(&self, profile: &ProxyProfile) -> Result<ProxyProfile, StateError> {
        if profile.kind != super::ProxyKind::Ssh {
            return Err(NetworkRefusal::ProxyHostKeyNotSsh.into());
        }
        self.write_proxy_profile(profile, true)
    }

    fn delete_proxy_profile(&self, id: u32) -> Result<(), StateError> {
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "delete_proxy", |tx| {
                Box::pin(async move {
                    super::network_persistence::lock_network(tx).await?;
                    for row in tx
                        .fetch_all("SELECT id, member_ids FROM proxy_pools ORDER BY id", &[])
                        .await?
                    {
                        let members: Vec<u32> =
                            serde_json::from_str(&row.text("member_ids")?).map_err(error)?;
                        if members.contains(&id) {
                            let pool = u32::try_from(row.i64("id")?).map_err(error)?;
                            return Err(NetworkRefusal::ProxyInPool { pool }.into());
                        }
                    }
                    for (holder, route) in super::network_persistence::routes_by_holder(tx).await? {
                        if route.legs.iter().any(|leg| match &leg.path {
                            super::LegPath::Direct => false,
                            super::LegPath::Ladder { rungs, .. } => {
                                rungs.iter().any(|rung| match rung {
                                    super::Rung::Proxy { id: referenced } => *referenced == id,
                                    super::Rung::Chain { ids } => ids.contains(&id),
                                    super::Rung::Pool { .. } => false,
                                })
                            }
                        }) {
                            return Err(NetworkRefusal::ProxyInUse(holder).into());
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

    fn list_proxy_routes(&self) -> Result<Vec<(String, RoutingPolicy)>, StateError> {
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT consumer, policy, dns_profile_id FROM proxy_routes",
                &[],
            )
            .await?
            .into_iter()
            .map(|r| Ok((r.text("consumer")?, route_policy(&r)?)))
            .collect()
        })
    }

    fn proxy_routing_policy(&self, consumer: Consumer) -> Result<RoutingPolicy, StateError> {
        #[cfg(test)]
        self.proxy_route_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let store = self.datastore();
        let key = consumer.key();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                store.read_exec(),
                "SELECT policy, dns_profile_id FROM proxy_routes WHERE consumer = {}",
                &[SqlArg::Text(key)],
            )
            .await?
            .map(|r| route_policy(&r))
            .transpose()
            .map(|v| v.unwrap_or_default())
        })
    }

    fn save_proxy_routing_policy(
        &self,
        consumer: Consumer,
        policy: &RoutingPolicy,
    ) -> Result<(), StateError> {
        policy.validate()?;
        let key = consumer.key();
        let policy = policy.clone();
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "save_proxy_route", |tx| {
                let key = key.clone();
                let policy = policy.clone();
                Box::pin(async move {
                    super::network_persistence::lock_network(tx).await?;
                    let policy =
                        super::network_persistence::validate_policy(tx, consumer, &policy).await?;
                    insert_route(tx, key, &policy).await?;
                    super::network_persistence::validate_stored_network(
                        tx,
                        super::dns_persistence::Touched::Route(consumer),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    fn proxy_host_key(&self, id: u32, revision: u64) -> Result<Option<String>, StateError> {
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

    // Pin only the revision that authenticated; stale handshakes cannot undo an edit.
    fn pin_proxy_host_key(
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

trait ProxiesPersistenceLocalQueries {
    fn write_proxy_profile(
        &self,
        profile: &ProxyProfile,
        reset_trust: bool,
    ) -> Result<ProxyProfile, StateError>;
}

impl ProxiesPersistenceLocalQueries for Database {
    fn write_proxy_profile(
        &self,
        profile: &ProxyProfile,
        reset_trust: bool,
    ) -> Result<ProxyProfile, StateError> {
        let mut profile = profile.clone();
        // SSH authenticates with an Ed25519 key alone, so no password is kept for it.
        if profile.kind == super::ProxyKind::Ssh {
            profile.secrets.password = None;
        }
        profile.validate()?;
        let key = self
            .encryption_key()
            .ok_or_else(|| error("encryption key required for proxy credentials"))?;
        let password = encrypt_value(
            key,
            &serde_json::to_string(&profile.secrets).map_err(error)?,
        )
        .map_err(error)?;
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "save_proxy_profile", |tx| {
                let mut profile = profile.clone();
                let password = password.clone();
                Box::pin(async move {
                    super::network_persistence::lock_network(tx).await?;
                    for row in tx.fetch_all("SELECT kind, member_ids FROM proxy_pools", &[]).await? {
                        let members: Vec<u32> = serde_json::from_str(&row.text("member_ids")?).map_err(error)?;
                        let kind: super::ProxyKind = serde_json::from_str(&row.text("kind")?).map_err(error)?;
                        if members.contains(&profile.id) && kind != profile.kind {
                            return Err(NetworkRefusal::ProxyKindLockedByPool.into());
                        }
                    }
                    let id = [SqlArg::I64(i64::from(profile.id))];
                    // A write before the read serializes saves and TOFU pins on
                    // both SQLite and PostgreSQL, including READ COMMITTED.
                    tx.execute("UPDATE proxy_profiles SET id = id WHERE id = {}", &id).await?;
                    if let Some(row) = tx.fetch_optional("SELECT config FROM proxy_profiles WHERE id = {}", &id).await? {
                        let existing: ProxyProfile = serde_json::from_str(&row.text("config")?).map_err(error)?;
                        if profile.revision < existing.revision || (reset_trust && profile.revision == existing.revision) {
                            return Err(NetworkRefusal::ProxyRevisionChanged.into());
                        }
                        profile.host_key_fingerprint = if reset_trust { None } else { existing.host_key_fingerprint.clone() };
                        // Key rotation rewrites encrypted secrets without changing
                        // the public profile or its runtime revision.
                        if profile.revision == existing.revision && serde_json::to_value(&profile).map_err(error)? != serde_json::to_value(&existing).map_err(error)? {
                            return Err(NetworkRefusal::ProxyRevisionChanged.into());
                        }
                    } else if reset_trust {
                        return Err(NetworkRefusal::ProxyGone.into());
                    }
                    tx.execute("INSERT INTO proxy_profiles (id, config, password, dns_profile_id) VALUES ({}, {}, {}, {}) ON CONFLICT(id) DO UPDATE SET config = excluded.config, password = excluded.password, dns_profile_id = excluded.dns_profile_id", &[
                        SqlArg::I64(i64::from(profile.id)),
                        SqlArg::Text(serde_json::to_string(&profile).map_err(error)?),
                        SqlArg::Text(password),
                        SqlArg::I64(i64::from(profile.dns_profile_id)),
                    ]).await?;
                    super::network_persistence::validate_stored_network(tx, super::dns_persistence::Touched::Proxy(profile.id)).await?;
                    Ok(profile)
                })
            }).await
        })
    }
}
