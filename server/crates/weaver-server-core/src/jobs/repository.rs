use crate::StateError;
use crate::history;
use crate::jobs::ids::JobId;
use crate::persistence::Database;

pub(crate) fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

fn delete_active_job_rows(tx: &rusqlite::Transaction<'_>, id: i64) -> Result<(), StateError> {
    tx.execute("DELETE FROM active_segments WHERE job_id = ?1", [id])
        .map_err(db_err)?;
    tx.execute("DELETE FROM active_file_progress WHERE job_id = ?1", [id])
        .map_err(db_err)?;
    tx.execute("DELETE FROM active_files WHERE job_id = ?1", [id])
        .map_err(db_err)?;
    tx.execute("DELETE FROM active_par2 WHERE job_id = ?1", [id])
        .map_err(db_err)?;
    tx.execute("DELETE FROM active_par2_files WHERE job_id = ?1", [id])
        .map_err(db_err)?;
    tx.execute("DELETE FROM active_extracted WHERE job_id = ?1", [id])
        .map_err(db_err)?;
    tx.execute(
        "DELETE FROM active_failed_extractions WHERE job_id = ?1",
        [id],
    )
    .map_err(db_err)?;
    tx.execute(
        "DELETE FROM active_extraction_chunks WHERE job_id = ?1",
        [id],
    )
    .map_err(db_err)?;
    tx.execute("DELETE FROM active_archive_headers WHERE job_id = ?1", [id])
        .map_err(db_err)?;
    tx.execute(
        "DELETE FROM active_rar_volume_facts WHERE job_id = ?1",
        [id],
    )
    .map_err(db_err)?;
    tx.execute("DELETE FROM active_volume_status WHERE job_id = ?1", [id])
        .map_err(db_err)?;
    tx.execute(
        "DELETE FROM active_rar_verified_suspect WHERE job_id = ?1",
        [id],
    )
    .map_err(db_err)?;
    tx.execute("DELETE FROM active_jobs WHERE job_id = ?1", [id])
        .map_err(db_err)?;
    Ok(())
}

impl Database {
    pub fn archive_job(
        &self,
        job_id: JobId,
        history: &history::JobHistoryRow,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        tx.execute(
            "INSERT OR REPLACE INTO job_history
             (job_id, name, status, error_message, total_bytes, downloaded_bytes,
              optional_recovery_bytes, optional_recovery_downloaded_bytes,
              failed_bytes, health, category, output_dir, nzb_path,
              created_at, completed_at, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            rusqlite::params![
                history.job_id as i64,
                history.name,
                history.status,
                history.error_message,
                history.total_bytes as i64,
                history.downloaded_bytes as i64,
                history.optional_recovery_bytes as i64,
                history.optional_recovery_downloaded_bytes as i64,
                history.failed_bytes as i64,
                history.health,
                history.category,
                history.output_dir,
                history.nzb_path,
                history.created_at,
                history.completed_at,
                history.metadata,
            ],
        )
        .map_err(db_err)?;
        delete_active_job_rows(&tx, job_id.0 as i64)?;
        tx.commit().map_err(db_err)?;
        conn.execute_batch("PRAGMA incremental_vacuum")
            .map_err(db_err)?;
        Ok(())
    }

    pub fn delete_active_job(&self, job_id: JobId) -> Result<(), StateError> {
        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        delete_active_job_rows(&tx, job_id.0 as i64)?;
        tx.commit().map_err(db_err)?;
        conn.execute_batch("PRAGMA incremental_vacuum")
            .map_err(db_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
