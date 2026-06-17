use crate::StateError;
use crate::bandwidth::ScheduleEntry;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::settings::record::SettingRecord;

impl Database {
    pub(crate) fn list_setting_records(&self) -> Result<Vec<SettingRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
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

    pub fn get_setting(&self, key: &str) -> Result<Option<String>, StateError> {
        let datastore = self.datastore();
        let key = key.to_string();
        self.run_sql_blocking(async move {
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

    pub fn list_schedules(&self) -> Result<Vec<ScheduleEntry>, StateError> {
        let json = self
            .get_setting("schedules")?
            .unwrap_or_else(|| "[]".into());
        serde_json::from_str(&json).map_err(|e| StateError::Database(e.to_string()))
    }
}
