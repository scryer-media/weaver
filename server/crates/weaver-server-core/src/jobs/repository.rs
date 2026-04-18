use crate::StateError;
use crate::history;
use crate::jobs::ids::JobId;
use crate::persistence::Database;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OrphanActiveStateCounts {
    pub active_segments: usize,
    pub active_file_progress: usize,
    pub active_files: usize,
    pub active_par2: usize,
    pub active_par2_files: usize,
    pub active_extracted: usize,
    pub active_failed_extractions: usize,
    pub active_extraction_chunks: usize,
    pub active_archive_headers: usize,
    pub active_rar_volume_facts: usize,
    pub active_volume_status: usize,
    pub active_rar_verified_suspect: usize,
}

impl OrphanActiveStateCounts {
    pub fn total_removed(self) -> usize {
        self.active_segments
            + self.active_file_progress
            + self.active_files
            + self.active_par2
            + self.active_par2_files
            + self.active_extracted
            + self.active_failed_extractions
            + self.active_extraction_chunks
            + self.active_archive_headers
            + self.active_rar_volume_facts
            + self.active_volume_status
            + self.active_rar_verified_suspect
    }
}

pub(crate) fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

fn delete_orphan_rows(
    tx: &rusqlite::Transaction<'_>,
    table: &'static str,
) -> Result<usize, StateError> {
    let deleted = tx
        .execute(
            &format!(
                "DELETE FROM {table}
                 WHERE NOT EXISTS (
                     SELECT 1 FROM active_jobs WHERE active_jobs.job_id = {table}.job_id
                 )"
            ),
            [],
        )
        .map_err(db_err)?;
    Ok(deleted)
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

    pub fn prune_orphan_active_state(&self) -> Result<OrphanActiveStateCounts, StateError> {
        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        let counts = OrphanActiveStateCounts {
            active_segments: delete_orphan_rows(&tx, "active_segments")?,
            active_file_progress: delete_orphan_rows(&tx, "active_file_progress")?,
            active_files: delete_orphan_rows(&tx, "active_files")?,
            active_par2: delete_orphan_rows(&tx, "active_par2")?,
            active_par2_files: delete_orphan_rows(&tx, "active_par2_files")?,
            active_extracted: delete_orphan_rows(&tx, "active_extracted")?,
            active_failed_extractions: delete_orphan_rows(&tx, "active_failed_extractions")?,
            active_extraction_chunks: delete_orphan_rows(&tx, "active_extraction_chunks")?,
            active_archive_headers: delete_orphan_rows(&tx, "active_archive_headers")?,
            active_rar_volume_facts: delete_orphan_rows(&tx, "active_rar_volume_facts")?,
            active_volume_status: delete_orphan_rows(&tx, "active_volume_status")?,
            active_rar_verified_suspect: delete_orphan_rows(&tx, "active_rar_verified_suspect")?,
        };
        tx.commit().map_err(db_err)?;
        if counts.total_removed() > 0 {
            conn.execute_batch("PRAGMA incremental_vacuum")
                .map_err(db_err)?;
        }
        Ok(counts)
    }
}

#[cfg(test)]
mod tests;
