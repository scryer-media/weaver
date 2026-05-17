use crate::StateError;
use crate::bandwidth::ScheduleEntry;
use crate::jobs::ids::JobId;
use crate::persistence::Database;

const NEXT_JOB_ID_SETTING_KEY: &str = "next_job_id";

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

    pub fn initialize_next_job_id_counter(&self) -> Result<u64, StateError> {
        let conn = self.conn();
        let next_job_id = next_job_id_floor(&conn)?;
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![NEXT_JOB_ID_SETTING_KEY, next_job_id.to_string()],
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(next_job_id)
    }

    pub fn reserve_next_job_id(&self) -> Result<JobId, StateError> {
        let conn = self.conn();
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| StateError::Database(e.to_string()))?;
        let next_job_id = next_job_id_floor(&tx)?;
        tx.execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![NEXT_JOB_ID_SETTING_KEY, (next_job_id + 1).to_string()],
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
        tx.commit()
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(JobId(next_job_id))
    }
}

fn next_job_id_floor(conn: &rusqlite::Connection) -> Result<u64, StateError> {
    let persisted = conn
        .query_row(
            "SELECT value FROM settings WHERE key = ?1",
            [NEXT_JOB_ID_SETTING_KEY],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10_000);

    let max_seen: Option<i64> = conn
        .query_row(
            "SELECT MAX(id) FROM (
                 SELECT MAX(job_id) AS id FROM active_jobs
                 UNION ALL
                 SELECT MAX(job_id) AS id FROM job_history
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|e| StateError::Database(e.to_string()))?;

    Ok(persisted.max(max_seen.unwrap_or(0) as u64 + 1).max(10_000))
}
