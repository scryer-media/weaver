// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime};
use crate::settings::model::TrackerRule;

#[cfg(test)]
use crate::categories::CategoryConfig;
#[cfg(test)]
use crate::servers::ServerConfig;
#[cfg(test)]
use crate::settings::Config;

const TRACKER_RULE_COLUMNS: &str = "id, host_pattern, seeding, never_pause";

fn tracker_rule_from_row(row: &SqlRow) -> Result<TrackerRule, StateError> {
    let seeding = row
        .opt_text("seeding")?
        .map(|json| {
            serde_json::from_str(&json).map_err(|error| {
                StateError::Database(format!("tracker rule seeding policy is not valid: {error}"))
            })
        })
        .transpose()?;
    let id = row.i64("id")?;
    Ok(TrackerRule {
        id: u32::try_from(id)
            .map_err(|_| StateError::Database(format!("tracker rule id {id} is not valid")))?,
        host_pattern: row.text("host_pattern")?,
        seeding,
        never_pause: row.bool("never_pause")?,
    })
}

// The rule's columns after its id, in `TRACKER_RULE_COLUMNS` order.
fn tracker_rule_args(rule: &TrackerRule) -> Result<Vec<SqlArg>, StateError> {
    let seeding = rule
        .seeding
        .map(|policy| serde_json::to_string(&policy))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))?;
    Ok(vec![
        SqlArg::Text(rule.host_pattern.clone()),
        SqlArg::OptText(seeding),
        SqlArg::Bool(rule.never_pause),
    ])
}

impl Database {
    pub fn list_tracker_rules(&self) -> Result<Vec<TrackerRule>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!("SELECT {TRACKER_RULE_COLUMNS} FROM tracker_rules ORDER BY id"),
                &[],
            )
            .await?;
            rows.iter().map(tracker_rule_from_row).collect()
        })
    }

    // Add `rule` under the next free id, which it returns with.
    pub fn insert_tracker_rule(
        &self,
        rule: &TrackerRule,
        now_ms: i64,
    ) -> Result<TrackerRule, StateError> {
        let datastore = self.datastore();
        let args = tracker_rule_args(rule)?;
        let rule = rule.clone();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_tracker_rule", |tx| {
                let args = args.clone();
                let mut rule = rule.clone();
                Box::pin(async move {
                    let max = tx
                        .fetch_optional("SELECT MAX(id) AS id FROM tracker_rules", &[])
                        .await?
                        .map(|row| row.opt_i64("id"))
                        .transpose()?
                        .flatten()
                        .unwrap_or(0);
                    rule.id = u32::try_from(max + 1).map_err(|_| {
                        StateError::Database("no tracker rule id is left".to_string())
                    })?;
                    let mut values = vec![SqlArg::I64(i64::from(rule.id))];
                    values.extend(args);
                    values.extend([SqlArg::I64(now_ms), SqlArg::I64(now_ms)]);
                    tx.execute(
                        &format!(
                            "INSERT INTO tracker_rules ({TRACKER_RULE_COLUMNS}, created_at_ms, updated_at_ms)
                             VALUES ({{}}, {{}}, {{}}, {{}}, {{}}, {{}})"
                        ),
                        &values,
                    )
                    .await?;
                    Ok(rule)
                })
            })
            .await
        })
    }

    // Returns whether the rule exists.
    pub fn update_tracker_rule(&self, rule: &TrackerRule, now_ms: i64) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let mut args = tracker_rule_args(rule)?;
        args.extend([SqlArg::I64(now_ms), SqlArg::I64(i64::from(rule.id))]);
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE tracker_rules
                    SET host_pattern = {}, seeding = {}, never_pause = {}, updated_at_ms = {}
                  WHERE id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // Returns whether the rule existed. A torrent it matched keeps no rule
    // until its trackers are matched again.
    pub fn delete_tracker_rule(&self, id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM tracker_rules WHERE id = {}",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }
}

#[cfg(test)]
mod tests;
