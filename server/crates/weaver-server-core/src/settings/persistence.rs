use crate::StateError;
use crate::bandwidth::ScheduleEntry;
use crate::persistence::Database;

impl Database {
    pub fn set_setting(&self, key: &str, value: &str) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![key, value],
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn delete_setting(&self, key: &str) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute("DELETE FROM settings WHERE key = ?1", [key])
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn save_schedules(&self, entries: &[ScheduleEntry]) -> Result<(), StateError> {
        let json =
            serde_json::to_string(entries).map_err(|e| StateError::Database(e.to_string()))?;
        self.set_setting("schedules", &json)
    }
}
