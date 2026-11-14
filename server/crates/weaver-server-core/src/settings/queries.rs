// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::settings::record::SettingRecord;

impl Database {
    pub(crate) fn list_setting_records(&self) -> Result<Vec<SettingRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT key, value FROM settings ORDER BY key",
                &[],
            )
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(SettingRecord {
                        key: row.text("key")?,
                        value: row.text("value")?,
                    })
                })
                .collect()
        })
    }

    // As `get_setting`, for every key of `keys`: one statement per bind-sized
    // chunk. A key with no setting is absent from the map.
    pub(crate) fn get_settings(
        &self,
        keys: Vec<String>,
    ) -> Result<std::collections::HashMap<String, String>, StateError> {
        if keys.is_empty() {
            return Ok(Default::default());
        }
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut values = std::collections::HashMap::new();
            let chunk_len =
                crate::persistence::sql_runtime::max_rows_for_engine(datastore.engine(), 1);
            for chunk in keys.chunks(chunk_len) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args = chunk.iter().cloned().map(SqlArg::Text).collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!("SELECT key, value FROM settings WHERE key IN ({placeholders})"),
                    &args,
                )
                .await?;
                for row in rows {
                    values.insert(row.text("key")?, row.text("value")?);
                }
            }
            Ok(values)
        })
    }

    pub fn get_setting(&self, key: &str) -> Result<Option<String>, StateError> {
        let datastore = self.datastore();
        let key = key.to_string();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT value FROM settings WHERE key = {}",
                &[SqlArg::Text(key)],
            )
            .await?
            .map(|row| row.text("value"))
            .transpose()
        })
    }
}
