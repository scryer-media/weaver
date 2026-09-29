use std::collections::HashMap;

use super::{
    EgressBinding, EgressInterface, LegPath, ProxyPool, ProxyProfile, Route, RoutingPolicy, Rung,
};
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx};
use crate::{Database, StateError};

fn error(value: impl std::fmt::Display) -> StateError {
    StateError::Database(value.to_string())
}

fn egress_row(row: SqlRow) -> Result<EgressInterface, StateError> {
    let value = row.opt_text("binding_value")?;
    let binding = match row.text("binding_kind")?.as_str() {
        "system" => EgressBinding::System,
        "interface" => EgressBinding::Interface {
            name: value.ok_or_else(|| error("missing interface binding"))?,
        },
        "sourceAddress" => EgressBinding::SourceAddress {
            address: value
                .ok_or_else(|| error("missing source address"))?
                .parse()
                .map_err(error)?,
        },
        _ => return Err(error("unknown stored egress binding")),
    };
    let egress = EgressInterface {
        id: u32::try_from(row.i64("id")?).map_err(error)?,
        name: row.text("name")?,
        binding,
        enabled: row.i64("enabled")? != 0,
        max_download_speed: u64::try_from(row.i64("max_download_speed")?).map_err(error)?,
    };
    egress.validate().map_err(error)?;
    Ok(egress)
}

fn pool_row(row: SqlRow) -> Result<ProxyPool, StateError> {
    Ok(ProxyPool {
        id: u32::try_from(row.i64("id")?).map_err(error)?,
        name: row.text("name")?,
        kind: serde_json::from_str(&row.text("kind")?).map_err(error)?,
        member_ids: serde_json::from_str(&row.text("member_ids")?).map_err(error)?,
        enabled: row.i64("enabled")? != 0,
    })
}

pub(super) fn stored_route(json: &str) -> Result<Route, StateError> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(error)?;
    if value
        .get("legs")
        .is_some_and(|legs| !matches!(legs, serde_json::Value::Array(legs) if legs.is_empty()))
    {
        serde_json::from_value(value).map_err(error)
    } else {
        let legacy: RoutingPolicy = serde_json::from_value(value).map_err(error)?;
        Ok(Route::from_legacy(&legacy))
    }
}

async fn profiles(tx: &mut SqlTx<'_>) -> Result<HashMap<u32, ProxyProfile>, StateError> {
    tx.fetch_all("SELECT id, config FROM proxy_profiles", &[])
        .await?
        .into_iter()
        .map(|row| {
            let mut profile: ProxyProfile =
                serde_json::from_str(&row.text("config")?).map_err(error)?;
            profile.id = u32::try_from(row.i64("id")?).map_err(error)?;
            Ok((profile.id, profile))
        })
        .collect()
}

async fn routes(tx: &mut SqlTx<'_>) -> Result<Vec<Route>, StateError> {
    tx.fetch_all("SELECT policy FROM proxy_routes", &[])
        .await?
        .into_iter()
        .map(|row| stored_route(&row.text("policy")?))
        .collect()
}

/// Serialize networking resource edits on a row that cannot be removed.
pub(super) async fn lock_network(tx: &mut SqlTx<'_>) -> Result<(), StateError> {
    tx.execute("UPDATE egress_interfaces SET id = id WHERE id = 0", &[])
        .await?;
    Ok(())
}

pub(super) async fn validate_policy(
    tx: &mut SqlTx<'_>,
    consumer: super::Consumer,
    policy: &RoutingPolicy,
) -> Result<RoutingPolicy, StateError> {
    policy.validate().map_err(error)?;
    if policy.legs.is_empty() {
        if let Some(row) = tx
            .fetch_optional(
                "SELECT policy FROM proxy_routes WHERE consumer = {}",
                &[SqlArg::Text(consumer.key())],
            )
            .await?
            && stored_route(&row.text("policy")?)?
                .legacy_policy()
                .is_none()
        {
            return Err(error(
                "this route has advanced legs; edit it in Networking instead of replacing its legacy projection",
            ));
        }
        let profiles = profiles(tx).await?;
        if policy.proxy_ids.iter().any(|id| !profiles.contains_key(id)) {
            return Err(error("routing policy references a missing proxy"));
        }
        return Ok(policy.clone());
    }
    let egresses = tx
        .fetch_all("SELECT * FROM egress_interfaces", &[])
        .await?
        .into_iter()
        .map(egress_row)
        .map(|r| r.map(|e| (e.id, e)))
        .collect::<Result<HashMap<_, _>, _>>()?;
    let pools = tx
        .fetch_all("SELECT * FROM proxy_pools", &[])
        .await?
        .into_iter()
        .map(pool_row)
        .map(|r| r.map(|p| (p.id, p)))
        .collect::<Result<HashMap<_, _>, _>>()?;
    let profiles = profiles(tx).await?;
    let route = policy.route();
    route
        .validate_references(
            &egresses,
            &profiles,
            &pools,
            matches!(consumer, super::Consumer::Rss(_)),
        )
        .map_err(error)?;
    Ok(RoutingPolicy::from_route(route, &pools))
}

pub(super) async fn validate_stored_network(tx: &mut SqlTx<'_>) -> Result<(), StateError> {
    let profiles = profiles(tx).await?;
    let pools = tx
        .fetch_all("SELECT * FROM proxy_pools", &[])
        .await?
        .into_iter()
        .map(pool_row)
        .map(|r| r.map(|pool| (pool.id, pool)))
        .collect::<Result<HashMap<_, _>, _>>()?;
    let egresses = tx
        .fetch_all("SELECT * FROM egress_interfaces", &[])
        .await?
        .into_iter()
        .map(egress_row)
        .map(|r| {
            r.map(|mut egress| {
                // Existing routes may intentionally remain attached to a disabled link.
                egress.enabled = true;
                (egress.id, egress)
            })
        })
        .collect::<Result<HashMap<_, _>, _>>()?;
    let mut routes = Vec::new();
    for row in tx
        .fetch_all("SELECT consumer, policy FROM proxy_routes", &[])
        .await?
    {
        let route = stored_route(&row.text("policy")?)?;
        // Missing restored egresses remain disabled in the runtime. Validate the other
        // references against placeholders without changing their persisted identity.
        let mut restored_egresses = egresses.clone();
        for leg in &route.legs {
            restored_egresses
                .entry(leg.egress_id)
                .or_insert_with(|| EgressInterface {
                    id: leg.egress_id,
                    name: "Missing restored egress".into(),
                    binding: EgressBinding::Interface {
                        name: "missing-restored-egress".into(),
                    },
                    enabled: true,
                    max_download_speed: 0,
                });
        }
        if route.legacy_policy().is_none() {
            route
                .validate_references(
                    &restored_egresses,
                    &profiles,
                    &pools,
                    row.text("consumer")?.starts_with("rss:"),
                )
                .map_err(error)?;
        }
        routes.push(route);
    }
    super::validate_instance_budget(&routes, &profiles, &pools, super::max_wireguard_instances())
        .map_err(error)
}

impl Database {
    pub fn list_egress_interfaces(&self) -> Result<Vec<EgressInterface>, StateError> {
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT * FROM egress_interfaces ORDER BY id",
                &[],
            )
            .await?
            .into_iter()
            .map(egress_row)
            .collect()
        })
    }

    pub fn create_egress_interface(
        &self,
        egress: &EgressInterface,
    ) -> Result<EgressInterface, StateError> {
        self.write_egress_interface(egress, true)
    }

    pub fn update_egress_interface(
        &self,
        egress: &EgressInterface,
    ) -> Result<EgressInterface, StateError> {
        self.write_egress_interface(egress, false)
    }

    fn write_egress_interface(
        &self,
        egress: &EgressInterface,
        create: bool,
    ) -> Result<EgressInterface, StateError> {
        let egress = egress.clone();
        let speed = i64::try_from(egress.max_download_speed)
            .map_err(|_| error("egress speed limit is too large"))?;
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "save_egress", |tx| {
                let mut egress = egress.clone();
                Box::pin(async move {
                    lock_network(tx).await?;
                    if create {
                        let row = tx.fetch_optional("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM egress_interfaces", &[]).await?.ok_or_else(|| error("cannot allocate egress id"))?;
                        egress.id = u32::try_from(row.i64("next_id")?).map_err(error)?;
                    } else if tx.fetch_optional("SELECT id FROM egress_interfaces WHERE id = {}", &[SqlArg::I64(i64::from(egress.id))]).await?.is_none() {
                        return Err(error("egress no longer exists"));
                    }
                    egress.validate().map_err(error)?;
                    let (kind, value) = match &egress.binding {
                        EgressBinding::System => ("system", None),
                        EgressBinding::Interface { name } => ("interface", Some(name.clone())),
                        EgressBinding::SourceAddress { address } => ("sourceAddress", Some(address.to_string())),
                    };
                    let now = chrono::Utc::now().timestamp();
                    tx.execute("INSERT INTO egress_interfaces (id, name, binding_kind, binding_value, enabled, max_download_speed, created_at, updated_at) VALUES ({}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT(id) DO UPDATE SET name = excluded.name, binding_kind = excluded.binding_kind, binding_value = excluded.binding_value, enabled = excluded.enabled, max_download_speed = excluded.max_download_speed, updated_at = excluded.updated_at", &[
                        SqlArg::I64(i64::from(egress.id)), SqlArg::Text(egress.name.clone()), SqlArg::Text(kind.into()), SqlArg::OptText(value),
                        SqlArg::I64(i64::from(egress.enabled)), SqlArg::I64(speed), SqlArg::I64(now), SqlArg::I64(now),
                    ]).await?;
                    Ok(egress)
                })
            }).await
        })
    }

    pub fn delete_egress_interface(&self, id: u32) -> Result<(), StateError> {
        if id == 0 {
            return Err(error("the System egress cannot be deleted"));
        }
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "delete_egress", |tx| {
                Box::pin(async move {
                    lock_network(tx).await?;
                    if routes(tx)
                        .await?
                        .iter()
                        .any(|route| route.legs.iter().any(|leg| leg.egress_id == id))
                    {
                        return Err(error("egress is referenced by a server or RSS feed"));
                    }
                    tx.execute(
                        "DELETE FROM egress_interfaces WHERE id = {}",
                        &[SqlArg::I64(i64::from(id))],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn list_proxy_pools(&self) -> Result<Vec<ProxyPool>, StateError> {
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT * FROM proxy_pools ORDER BY id",
                &[],
            )
            .await?
            .into_iter()
            .map(pool_row)
            .collect()
        })
    }

    pub fn create_proxy_pool(&self, pool: &ProxyPool) -> Result<ProxyPool, StateError> {
        self.write_proxy_pool(pool, true)
    }

    pub fn update_proxy_pool(&self, pool: &ProxyPool) -> Result<ProxyPool, StateError> {
        self.write_proxy_pool(pool, false)
    }

    fn write_proxy_pool(&self, pool: &ProxyPool, create: bool) -> Result<ProxyPool, StateError> {
        let pool = pool.clone();
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "save_proxy_pool", |tx| {
                let mut pool = pool.clone();
                Box::pin(async move {
                    lock_network(tx).await?;
                    if create {
                        let row = tx.fetch_optional("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM proxy_pools", &[]).await?.ok_or_else(|| error("cannot allocate pool id"))?;
                        pool.id = u32::try_from(row.i64("next_id")?).map_err(error)?;
                    } else if tx.fetch_optional("SELECT id FROM proxy_pools WHERE id = {}", &[SqlArg::I64(i64::from(pool.id))]).await?.is_none() {
                        return Err(error("proxy pool no longer exists"));
                    }
                    let profiles = profiles(tx).await?;
                    pool.validate(&profiles).map_err(error)?;
                    let mut pools: HashMap<_, _> = tx.fetch_all("SELECT * FROM proxy_pools", &[]).await?.into_iter().map(pool_row).collect::<Result<Vec<_>, _>>()?.into_iter().map(|pool| (pool.id, pool)).collect();
                    pools.insert(pool.id, pool.clone());
                    // Revalidate all consumers that use this pool before changing its members.
                    // Disabled egresses are runtime state, so only path references are checked here.
                    for row in tx.fetch_all("SELECT consumer, policy FROM proxy_routes", &[]).await? {
                        let require_dns = row.text("consumer")?.starts_with("rss:");
                        let route = stored_route(&row.text("policy")?)?;
                        for leg in route.legs {
                            let LegPath::Ladder { rungs, .. } = leg.path else { continue };
                            if !rungs.iter().any(|rung| matches!(rung, Rung::Pool { id } if *id == pool.id)) { continue; }
                            let mut seen = std::collections::HashSet::new();
                            for rung in rungs {
                                let ids = match rung {
                                    Rung::Proxy { id } => vec![id], Rung::Chain { ids } => ids,
                                    Rung::Pool { id } => pools.get(&id).ok_or_else(|| error("route references a missing pool"))?.member_ids.clone(),
                                };
                                for id in ids {
                                    if !seen.insert(id) { return Err(error("pool edit would repeat a proxy in an existing ladder")); }
                                    if require_dns && profiles.get(&id).is_none_or(|profile| profile.dns_servers.is_empty()) {
                                        return Err(error("every RSS proxy and pool member requires DNS servers"));
                                    }
                                }
                            }
                        }
                    }
                    let now = chrono::Utc::now().timestamp();
                    tx.execute("INSERT INTO proxy_pools (id, name, kind, member_ids, enabled, created_at, updated_at) VALUES ({}, {}, {}, {}, {}, {}, {}) ON CONFLICT(id) DO UPDATE SET name = excluded.name, kind = excluded.kind, member_ids = excluded.member_ids, enabled = excluded.enabled, updated_at = excluded.updated_at", &[
                        SqlArg::I64(i64::from(pool.id)), SqlArg::Text(pool.name.clone()), SqlArg::Text(serde_json::to_string(&pool.kind).map_err(error)?), SqlArg::Text(serde_json::to_string(&pool.member_ids).map_err(error)?), SqlArg::I64(i64::from(pool.enabled)), SqlArg::I64(now), SqlArg::I64(now),
                    ]).await?;
                    validate_stored_network(tx).await?;
                    Ok(pool)
                })
            }).await
        })
    }

    pub fn delete_proxy_pool(&self, id: u32) -> Result<(), StateError> {
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "delete_proxy_pool", |tx| Box::pin(async move {
                lock_network(tx).await?;
                for route in routes(tx).await? {
                    for leg in route.legs {
                        if let LegPath::Ladder { rungs, .. } = leg.path
                            && rungs.iter().any(|rung| matches!(rung, Rung::Pool { id: referenced } if *referenced == id)) {
                            return Err(error("pool is referenced by a server or RSS feed"));
                        }
                    }
                }
                tx.execute("DELETE FROM proxy_pools WHERE id = {}", &[SqlArg::I64(i64::from(id))]).await?;
                Ok(())
            })).await
        })
    }
}

#[cfg(test)]
#[path = "network_persistence_tests.rs"]
mod tests;
