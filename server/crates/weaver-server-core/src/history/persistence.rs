use crate::StateError;
use crate::history::record::{IntegrationEventRow, JobHistoryRow};
use crate::persistence::Database;

use super::repository::db_err;

impl Database {
    pub fn insert_job_history(&self, entry: &JobHistoryRow) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT OR REPLACE INTO job_history
             (job_id, name, status, error_message, total_bytes, downloaded_bytes,
              optional_recovery_bytes, optional_recovery_downloaded_bytes,
              failed_bytes, health, category, output_dir, nzb_path,
              created_at, completed_at, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            rusqlite::params![
                entry.job_id as i64,
                entry.name,
                entry.status,
                entry.error_message,
                entry.total_bytes as i64,
                entry.downloaded_bytes as i64,
                entry.optional_recovery_bytes as i64,
                entry.optional_recovery_downloaded_bytes as i64,
                entry.failed_bytes as i64,
                entry.health,
                entry.category,
                entry.output_dir,
                entry.nzb_path,
                entry.created_at,
                entry.completed_at,
                entry.metadata,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn delete_job_history(&self, job_id: u64) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute("DELETE FROM job_history WHERE job_id = ?1", [job_id as i64])
            .map_err(db_err)?;
        Ok(changed > 0)
    }

    pub fn delete_all_job_history(&self) -> Result<usize, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute("DELETE FROM job_history", [])
            .map_err(db_err)?;
        Ok(changed)
    }

    pub fn insert_integration_events(
        &self,
        events: &[IntegrationEventRow],
    ) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }

        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO integration_events (timestamp, kind, item_id, payload_json)
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .map_err(db_err)?;
            for event in events {
                stmt.execute(rusqlite::params![
                    event.timestamp,
                    event.kind,
                    event.item_id.map(|value| value as i64),
                    event.payload_json,
                ])
                .map_err(db_err)?;
            }
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn delete_all_integration_events(&self) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute("DELETE FROM integration_events", [])
            .map_err(db_err)?;
        Ok(())
    }
}
