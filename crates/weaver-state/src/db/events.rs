use super::Database;
use crate::StateError;

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
        conn.execute(
            "DELETE FROM job_events WHERE job_id = ?1",
            [job_id as i64],
        )
        .map_err(db_err)?;
        Ok(())
    }

    /// Delete all job events across all jobs.
    pub fn delete_all_job_events(&self) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute("DELETE FROM job_events", [])
            .map_err(db_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_load_events() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_event(1, 1000, "JOB_CREATED", "test: 5 files", None)
            .unwrap();
        db.insert_job_event(1, 1001, "FILE_COMPLETE", "data.rar: 1000 bytes", Some("1:0"))
            .unwrap();
        db.insert_job_event(2, 1002, "JOB_CREATED", "other job", None)
            .unwrap();

        let events = db.get_job_events(1).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "JOB_CREATED");
        assert_eq!(events[1].kind, "FILE_COMPLETE");
        assert_eq!(events[1].file_id.as_deref(), Some("1:0"));

        // Job 2 has its own events.
        let events2 = db.get_job_events(2).unwrap();
        assert_eq!(events2.len(), 1);
    }

    #[test]
    fn batch_insert_events() {
        let db = Database::open_in_memory().unwrap();
        let events = vec![
            JobEvent {
                job_id: 1,
                timestamp: 1000,
                kind: "JOB_CREATED".to_string(),
                message: "created".to_string(),
                file_id: None,
            },
            JobEvent {
                job_id: 1,
                timestamp: 1001,
                kind: "FILE_COMPLETE".to_string(),
                message: "done".to_string(),
                file_id: Some("1:0".to_string()),
            },
        ];
        db.insert_job_events(&events).unwrap();
        assert_eq!(db.get_job_events(1).unwrap().len(), 2);
    }

    #[test]
    fn delete_events() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_event(1, 1000, "JOB_CREATED", "test", None)
            .unwrap();
        db.insert_job_event(1, 1001, "FILE_COMPLETE", "done", None)
            .unwrap();
        db.delete_job_events(1).unwrap();
        assert!(db.get_job_events(1).unwrap().is_empty());
    }
}
