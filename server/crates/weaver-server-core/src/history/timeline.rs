use crate::StateError;
use crate::persistence::Database;

/// A persisted job event.
#[derive(Debug, Clone)]
pub struct JobEvent {
    pub job_id: u64,
    pub timestamp: i64,
    pub kind: String,
    pub message: String,
    pub file_id: Option<String>,
}

fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

impl Database {
    /// Insert a single job event.
    pub fn insert_job_event(
        &self,
        job_id: u64,
        timestamp: i64,
        kind: &str,
        message: &str,
        file_id: Option<&str>,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO job_events (job_id, timestamp, kind, message, file_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![job_id as i64, timestamp, kind, message, file_id],
        )
        .map_err(db_err)?;
        Ok(())
    }

    /// Batch-insert job events in a single transaction.
    pub fn insert_job_events(&self, events: &[JobEvent]) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }
        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO job_events (job_id, timestamp, kind, message, file_id)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )
                .map_err(db_err)?;
            for event in events {
                stmt.execute(rusqlite::params![
                    event.job_id as i64,
                    event.timestamp,
                    event.kind,
                    event.message,
                    event.file_id,
                ])
                .map_err(db_err)?;
            }
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    /// Load all events for a specific job, ordered by timestamp ascending.
    pub fn get_job_events(&self, job_id: u64) -> Result<Vec<JobEvent>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT job_id, timestamp, kind, message, file_id
                 FROM job_events
                 WHERE job_id = ?1
                 ORDER BY id ASC",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([job_id as i64], |row| {
                Ok(JobEvent {
                    job_id: row.get::<_, i64>(0)? as u64,
                    timestamp: row.get(1)?,
                    kind: row.get(2)?,
                    message: row.get(3)?,
                    file_id: row.get(4)?,
                })
            })
            .map_err(db_err)?;
        let mut events = Vec::new();
        for row in rows {
            events.push(row.map_err(db_err)?);
        }
        Ok(events)
    }

    /// Delete all events for a job.
    pub fn delete_job_events(&self, job_id: u64) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute("DELETE FROM job_events WHERE job_id = ?1", [job_id as i64])
            .map_err(db_err)?;
        Ok(())
    }

    /// Delete all job events across all jobs.
    pub fn delete_all_job_events(&self) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute("DELETE FROM job_events", []).map_err(db_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
