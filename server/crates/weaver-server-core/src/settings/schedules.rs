use std::collections::BTreeSet;

use crate::StateError;
use crate::bandwidth::{QuotaTarget, ScheduleAction, ScheduleEntry, SpeedTarget};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};

/// The row every schedule read and write takes first, so saves and the
/// tidying a load does never interleave. The value is only a lock sentinel.
const LOCK_KEY: &str = "schedule_write_lock";

impl Database {
    /// The saved schedule rules. A rule that names an egress or provider that
    /// is gone loses that part, and is saved that way, with a log line.
    pub fn list_schedules(&self) -> Result<Vec<ScheduleEntry>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "list_schedules", |tx| {
                Box::pin(async move {
                    lock(tx).await?;
                    let mut entries = read_schedules(tx).await?;
                    let holders = Holders::load(tx).await?;
                    if holders.prune(&mut entries) {
                        write_schedules(tx, &entries).await?;
                    }
                    Ok(entries)
                })
            })
            .await
        })
    }

    pub fn save_schedules(&self, entries: &[ScheduleEntry]) -> Result<(), StateError> {
        let datastore = self.datastore();
        let entries = entries.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_schedule_tracks", |tx| {
                let entries = entries.clone();
                Box::pin(async move {
                    lock(tx).await?;
                    let holders = Holders::load(tx).await?;
                    for entry in &entries {
                        holders.check(&entry.action)?;
                    }
                    write_schedules(tx, &entries).await
                })
            })
            .await
        })
    }

    /// Drop every rule and every speed limit for a server being deleted.
    pub(crate) async fn remove_server_schedules(
        tx: &mut SqlTx<'_>,
        server_id: u32,
    ) -> Result<(), StateError> {
        lock(tx).await?;
        let mut entries = read_schedules(tx).await?;
        entries.retain(|entry| !matches!(entry.action, ScheduleAction::SetServerActive { server_id: id, .. } if id == server_id));
        for entry in &mut entries {
            if let ScheduleAction::SpeedLimit { limits } = &mut entry.action {
                limits.retain(|limit| limit.target != SpeedTarget::Server(server_id));
            }
        }
        write_schedules(tx, &entries).await
    }

    /// Drop every quota rule and every speed limit for an egress being
    /// deleted, so an egress later given the same id inherits none of them.
    pub(crate) async fn remove_egress_schedules(
        tx: &mut SqlTx<'_>,
        egress_id: u32,
    ) -> Result<(), StateError> {
        lock(tx).await?;
        let mut entries = read_schedules(tx).await?;
        entries.retain(|entry| {
            !matches!(
                entry.action,
                ScheduleAction::SetQuotaMetering {
                    target: QuotaTarget::Egress(id),
                    ..
                } if id == egress_id
            )
        });
        for entry in &mut entries {
            if let ScheduleAction::SpeedLimit { limits } = &mut entry.action {
                limits.retain(|limit| limit.target != SpeedTarget::Egress(egress_id));
            }
        }
        write_schedules(tx, &entries).await
    }
}

/// Each rule as saved. One that cannot be read is left out with a warning
/// rather than keeping every other rule from loading.
async fn read_schedules(tx: &mut SqlTx<'_>) -> Result<Vec<ScheduleEntry>, StateError> {
    let json = tx
        .fetch_optional(
            "SELECT value FROM settings WHERE key = {}",
            &[SqlArg::Text("schedules".into())],
        )
        .await?
        .map(|row| row.text("value"))
        .transpose()?
        .unwrap_or_else(|| "[]".into());
    decode(&json)
}

fn decode(json: &str) -> Result<Vec<ScheduleEntry>, StateError> {
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(json).map_err(|error| StateError::Database(error.to_string()))?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            serde_json::from_value::<ScheduleEntry>(row.clone())
                .inspect_err(|error| {
                    let label = row
                        .get("label")
                        .and_then(serde_json::Value::as_str)
                        .filter(|label| !label.is_empty())
                        .or_else(|| row.get("id").and_then(serde_json::Value::as_str))
                        .unwrap_or_default();
                    tracing::warn!(rule = %label, %error, "a schedule rule could not be read and was left out");
                })
                .ok()
        })
        .collect())
}

/// The egresses and providers that exist, which a rule may name.
struct Holders {
    egresses: BTreeSet<u32>,
    servers: BTreeSet<u32>,
}

impl Holders {
    async fn load(tx: &mut SqlTx<'_>) -> Result<Self, StateError> {
        let ids = |rows: Vec<crate::persistence::sql_runtime::SqlRow>| {
            rows.into_iter()
                .map(|row| {
                    let id = row.i64("id")?;
                    u32::try_from(id)
                        .map_err(|_| StateError::Database(format!("id {id} is out of range")))
                })
                .collect::<Result<BTreeSet<_>, _>>()
        };
        let egresses = ids(tx
            .fetch_all("SELECT id FROM egress_interfaces", &[])
            .await?)?;
        let servers = ids(tx.fetch_all("SELECT id FROM servers", &[]).await?)?;
        Ok(Self { egresses, servers })
    }

    fn has(&self, target: SpeedTarget) -> bool {
        match target {
            SpeedTarget::Global => true,
            SpeedTarget::Egress(id) => self.egresses.contains(&id),
            SpeedTarget::Server(id) => self.servers.contains(&id),
        }
    }

    /// Refuse a rule that names something that is not there.
    fn check(&self, action: &ScheduleAction) -> Result<(), StateError> {
        let missing =
            |what: &str, id: u32| Err(StateError::Database(format!("{what} {id} not found")));
        match action {
            ScheduleAction::SetServerActive { server_id, .. }
                if !self.servers.contains(server_id) =>
            {
                missing("server", *server_id)
            }
            ScheduleAction::SetQuotaMetering {
                target: QuotaTarget::Egress(id),
                ..
            } if !self.egresses.contains(id) => missing("egress", *id),
            ScheduleAction::SpeedLimit { limits } => {
                match limits.iter().find(|limit| !self.has(limit.target)) {
                    Some(limit) => match limit.target {
                        SpeedTarget::Egress(id) => missing("egress", id),
                        SpeedTarget::Server(id) => missing("server", id),
                        SpeedTarget::Global => Ok(()),
                    },
                    None => Ok(()),
                }
            }
            _ => Ok(()),
        }
    }

    /// Take out what names a deleted egress or provider. Returns whether
    /// anything changed.
    fn prune(&self, entries: &mut Vec<ScheduleEntry>) -> bool {
        let before = entries.clone();
        entries.retain(|entry| {
            let gone = match entry.action {
                ScheduleAction::SetQuotaMetering {
                    target: QuotaTarget::Egress(id),
                    ..
                } => (!self.egresses.contains(&id)).then_some(("egress", id)),
                ScheduleAction::SetServerActive { server_id, .. } => {
                    (!self.servers.contains(&server_id)).then_some(("server", server_id))
                }
                _ => None,
            };
            if let Some((what, id)) = gone {
                tracing::warn!(rule = %rule_name(entry), "{what} {id} is gone, so its schedule rule was removed");
            }
            gone.is_none()
        });
        for entry in entries.iter_mut() {
            let name = rule_name(entry).to_string();
            if let ScheduleAction::SpeedLimit { limits } = &mut entry.action {
                limits.retain(|limit| {
                    let keep = self.has(limit.target);
                    if !keep {
                        tracing::warn!(rule = %name, target = ?limit.target, "an egress or provider a speed rule named is gone, so the rule no longer sets its limit");
                    }
                    keep
                });
            }
        }
        *entries != before
    }
}

fn rule_name(entry: &ScheduleEntry) -> &str {
    if entry.label.is_empty() {
        &entry.id
    } else {
        &entry.label
    }
}

/// Serialize schedule reads and saves on both SQL backends, including the
/// first save on an empty installation.
async fn lock(tx: &mut SqlTx<'_>) -> Result<(), StateError> {
    tx.execute(
        "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO NOTHING",
        &[SqlArg::Text(LOCK_KEY.into()), SqlArg::Text("1".into())],
    )
    .await?;
    let sql = match tx {
        SqlTx::Postgres(_) => "SELECT value FROM settings WHERE key = {} FOR UPDATE",
        SqlTx::Sqlite(_) => "SELECT value FROM settings WHERE key = {}",
    };
    tx.fetch_optional(sql, &[SqlArg::Text(LOCK_KEY.into())])
        .await?
        .ok_or_else(|| StateError::Database("missing schedule lock row".into()))?;
    Ok(())
}

async fn write_schedules(tx: &mut SqlTx<'_>, entries: &[ScheduleEntry]) -> Result<(), StateError> {
    if entries
        .iter()
        .any(|entry| entry.enabled && matches!(entry.action, ScheduleAction::PauseAll))
    {
        tx.execute(
            "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO NOTHING",
            &[
                SqlArg::Text("schedule_pause_all_used".into()),
                SqlArg::Text("true".into()),
            ],
        )
        .await?;
    }
    let json =
        serde_json::to_string(entries).map_err(|error| StateError::Database(error.to_string()))?;
    tx.execute(
        "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        &[SqlArg::Text("schedules".into()), SqlArg::Text(json)],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
