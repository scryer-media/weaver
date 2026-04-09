use crate::StateError;
use crate::bandwidth::ScheduleEntry;
use crate::persistence::Database;
use crate::settings::record::SettingRecord;

impl Database {
    pub(crate) fn list_setting_records(&self) -> Result<Vec<SettingRecord>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare_cached("SELECT key, value FROM settings ORDER BY key")
            .map_err(|e| StateError::Database(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(SettingRecord {
                    key: row.get(0)?,
                    value: row.get(1)?,
                })
            })
            .map_err(|e| StateError::Database(e.to_string()))?;

        let mut settings = Vec::new();
        for row in rows {
            settings.push(row.map_err(|e| StateError::Database(e.to_string()))?);
        }
        Ok(settings)
    }

    pub fn get_setting(&self, key: &str) -> Result<Option<String>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare_cached("SELECT value FROM settings WHERE key = ?1")
            .map_err(|e| StateError::Database(e.to_string()))?;
        let result = stmt.query_row([key], |row| row.get(0)).ok();
        Ok(result)
    }

    pub fn list_schedules(&self) -> Result<Vec<ScheduleEntry>, StateError> {
        let json = self
            .get_setting("schedules")?
            .unwrap_or_else(|| "[]".into());
        serde_json::from_str(&json).map_err(|e| StateError::Database(e.to_string()))
    }
}
