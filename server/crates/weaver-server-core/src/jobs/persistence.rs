use std::collections::HashSet;
use std::path::Path;

use rusqlite::OptionalExtension;

use crate::StateError;
use crate::jobs::assembly::DetectedArchiveIdentity;
use crate::jobs::ids::JobId;
use crate::jobs::model::{FieldUpdate, JobUpdate};
use crate::jobs::record::{
    ActiveExtractionChunk, ActiveFileProgress, ActiveJob, CommittedSegment, ExtractionChunk,
};
use crate::persistence::Database;

use super::repository::db_err;

fn active_job_exists(conn: &rusqlite::Connection, job_id: JobId) -> Result<bool, StateError> {
    let exists = conn
        .query_row(
            "SELECT 1 FROM active_jobs WHERE job_id = ?1 LIMIT 1",
            [job_id.0 as i64],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(db_err)?
        .is_some();
    Ok(exists)
}

fn active_job_exists_tx(tx: &rusqlite::Transaction<'_>, job_id: JobId) -> Result<bool, StateError> {
    let exists = tx
        .query_row(
            "SELECT 1 FROM active_jobs WHERE job_id = ?1 LIMIT 1",
            [job_id.0 as i64],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(db_err)?
        .is_some();
    Ok(exists)
}

impl Database {
    pub fn create_active_job(&self, job: &ActiveJob) -> Result<(), StateError> {
        let conn = self.conn();
        let metadata_json = if job.metadata.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&job.metadata).map_err(db_err)?)
        };
        conn.execute(
            "INSERT INTO active_jobs
             (job_id, nzb_hash, nzb_path, output_dir, status, created_at, category, metadata)
             VALUES (?1, ?2, ?3, ?4, 'queued', ?5, ?6, ?7)",
            rusqlite::params![
                job.job_id.0 as i64,
                job.nzb_hash.as_slice(),
                job.nzb_path.to_str().unwrap_or(""),
                job.output_dir.to_str().unwrap_or(""),
                job.created_at as i64,
                job.category,
                metadata_json,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn update_active_job(&self, job_id: JobId, update: &JobUpdate) -> Result<(), StateError> {
        let conn = self.conn();
        let id = job_id.0 as i64;
        match &update.category {
            FieldUpdate::Unchanged => {}
            FieldUpdate::Clear => {
                conn.execute(
                    "UPDATE active_jobs SET category = NULL WHERE job_id = ?1",
                    rusqlite::params![id],
                )
                .map_err(db_err)?;
            }
            FieldUpdate::Set(category) => {
                conn.execute(
                    "UPDATE active_jobs SET category = ?1 WHERE job_id = ?2",
                    rusqlite::params![category, id],
                )
                .map_err(db_err)?;
            }
        }

        match &update.metadata {
            FieldUpdate::Unchanged => {}
            FieldUpdate::Clear => {
                conn.execute(
                    "UPDATE active_jobs SET metadata = NULL WHERE job_id = ?1",
                    rusqlite::params![id],
                )
                .map_err(db_err)?;
            }
            FieldUpdate::Set(metadata) => {
                let json = if metadata.is_empty() {
                    None
                } else {
                    Some(serde_json::to_string(metadata).map_err(db_err)?)
                };
                conn.execute(
                    "UPDATE active_jobs SET metadata = ?1 WHERE job_id = ?2",
                    rusqlite::params![json, id],
                )
                .map_err(db_err)?;
            }
        }
        Ok(())
    }

    pub fn set_active_job_status(
        &self,
        job_id: JobId,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), StateError> {
        self.set_active_job_runtime(job_id, status, error, None, None, None)
    }

    pub fn set_active_job_runtime(
        &self,
        job_id: JobId,
        status: &str,
        error: Option<&str>,
        queued_repair_at_epoch_ms: Option<f64>,
        queued_extract_at_epoch_ms: Option<f64>,
        paused_resume_status: Option<&str>,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE active_jobs
             SET status = ?1,
                 error = ?2,
                 queued_repair_at_epoch_ms = ?3,
                 queued_extract_at_epoch_ms = ?4,
                 paused_resume_status = ?5
             WHERE job_id = ?6",
            rusqlite::params![
                status,
                error,
                queued_repair_at_epoch_ms,
                queued_extract_at_epoch_ms,
                paused_resume_status,
                job_id.0 as i64
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn commit_segments(&self, segments: &[CommittedSegment]) -> Result<(), StateError> {
        if segments.is_empty() {
            return Ok(());
        }
        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        let mut known_active_jobs = HashSet::new();
        let mut missing_jobs = HashSet::new();
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT OR IGNORE INTO active_segments
                     (job_id, file_index, segment_number, file_offset, decoded_size, crc32)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )
                .map_err(db_err)?;
            for seg in segments {
                if missing_jobs.contains(&seg.job_id) {
                    continue;
                }
                if !known_active_jobs.contains(&seg.job_id) {
                    if active_job_exists_tx(&tx, seg.job_id)? {
                        known_active_jobs.insert(seg.job_id);
                    } else {
                        missing_jobs.insert(seg.job_id);
                        continue;
                    }
                }
                stmt.execute(rusqlite::params![
                    seg.job_id.0 as i64,
                    seg.file_index,
                    seg.segment_number,
                    seg.file_offset as i64,
                    seg.decoded_size,
                    seg.crc32,
                ])
                .map_err(db_err)?;
            }
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn upsert_file_progress_batch(
        &self,
        progress: &[ActiveFileProgress],
    ) -> Result<(), StateError> {
        if progress.is_empty() {
            return Ok(());
        }
        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        let mut known_active_jobs = HashSet::new();
        let mut missing_jobs = HashSet::new();
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO active_file_progress
                     (job_id, file_index, contiguous_bytes_written)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(job_id, file_index)
                     DO UPDATE SET contiguous_bytes_written = MAX(
                        active_file_progress.contiguous_bytes_written,
                        excluded.contiguous_bytes_written
                     )",
                )
                .map_err(db_err)?;
            for entry in progress {
                if missing_jobs.contains(&entry.job_id) {
                    continue;
                }
                if !known_active_jobs.contains(&entry.job_id) {
                    if active_job_exists_tx(&tx, entry.job_id)? {
                        known_active_jobs.insert(entry.job_id);
                    } else {
                        missing_jobs.insert(entry.job_id);
                        continue;
                    }
                }
                stmt.execute(rusqlite::params![
                    entry.job_id.0 as i64,
                    entry.file_index,
                    entry.contiguous_bytes_written as i64,
                ])
                .map_err(db_err)?;
            }
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn clear_file_progress(&self, job_id: JobId, file_index: u32) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_file_progress WHERE job_id = ?1 AND file_index = ?2",
            rusqlite::params![job_id.0 as i64, file_index],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn complete_file(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: &[u8; 16],
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if active_job_exists(&conn, job_id)? {
            conn.execute(
                "INSERT OR IGNORE INTO active_files (job_id, file_index, filename, md5)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![job_id.0 as i64, file_index, filename, md5.as_slice()],
            )
            .map_err(db_err)?;
        }
        conn.execute(
            "DELETE FROM active_file_progress WHERE job_id = ?1 AND file_index = ?2",
            rusqlite::params![job_id.0 as i64, file_index],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn mark_file_incomplete(&self, job_id: JobId, file_index: u32) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_segments WHERE job_id = ?1 AND file_index = ?2",
            rusqlite::params![job_id.0 as i64, file_index],
        )
        .map_err(db_err)?;
        conn.execute(
            "DELETE FROM active_file_progress WHERE job_id = ?1 AND file_index = ?2",
            rusqlite::params![job_id.0 as i64, file_index],
        )
        .map_err(db_err)?;
        conn.execute(
            "DELETE FROM active_files WHERE job_id = ?1 AND file_index = ?2",
            rusqlite::params![job_id.0 as i64, file_index],
        )
        .map_err(db_err)?;
        conn.execute(
            "DELETE FROM active_detected_archives WHERE job_id = ?1 AND file_index = ?2",
            rusqlite::params![job_id.0 as i64, file_index],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn set_par2_metadata(
        &self,
        job_id: JobId,
        slice_size: u64,
        recovery_block_count: u32,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        conn.execute(
            "INSERT OR REPLACE INTO active_par2 (job_id, slice_size, recovery_block_count)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![job_id.0 as i64, slice_size as i64, recovery_block_count],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn upsert_par2_file(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        recovery_block_count: u32,
        promoted: bool,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        conn.execute(
            "INSERT OR REPLACE INTO active_par2_files
             (job_id, file_index, filename, recovery_block_count, promoted)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                job_id.0 as i64,
                file_index,
                filename,
                recovery_block_count,
                i64::from(promoted),
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn set_par2_file_promotion(
        &self,
        job_id: JobId,
        file_index: u32,
        promoted: bool,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE active_par2_files SET promoted = ?1 WHERE job_id = ?2 AND file_index = ?3",
            rusqlite::params![i64::from(promoted), job_id.0 as i64, file_index],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn replace_failed_extractions(
        &self,
        job_id: JobId,
        members: &HashSet<String>,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        tx.execute(
            "DELETE FROM active_failed_extractions WHERE job_id = ?1",
            [job_id.0 as i64],
        )
        .map_err(db_err)?;
        if !members.is_empty() {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO active_failed_extractions (job_id, member_name)
                     VALUES (?1, ?2)",
                )
                .map_err(db_err)?;
            for member_name in members {
                stmt.execute(rusqlite::params![job_id.0 as i64, member_name])
                    .map_err(db_err)?;
            }
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn add_failed_extraction(
        &self,
        job_id: JobId,
        member_name: &str,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        conn.execute(
            "INSERT OR IGNORE INTO active_failed_extractions (job_id, member_name)
             VALUES (?1, ?2)",
            rusqlite::params![job_id.0 as i64, member_name],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn remove_failed_extraction(
        &self,
        job_id: JobId,
        member_name: &str,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_failed_extractions
             WHERE job_id = ?1 AND member_name = ?2",
            rusqlite::params![job_id.0 as i64, member_name],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn set_active_job_normalization_retried(
        &self,
        job_id: JobId,
        normalization_retried: bool,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE active_jobs SET normalization_retried = ?1 WHERE job_id = ?2",
            rusqlite::params![i64::from(normalization_retried), job_id.0 as i64],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn replace_verified_suspect_volumes(
        &self,
        job_id: JobId,
        set_name: &str,
        volumes: &HashSet<u32>,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        tx.execute(
            "DELETE FROM active_rar_verified_suspect
             WHERE job_id = ?1 AND set_name = ?2",
            rusqlite::params![job_id.0 as i64, set_name],
        )
        .map_err(db_err)?;
        if !volumes.is_empty() {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO active_rar_verified_suspect
                     (job_id, set_name, volume_index)
                     VALUES (?1, ?2, ?3)",
                )
                .map_err(db_err)?;
            for volume_index in volumes {
                stmt.execute(rusqlite::params![job_id.0 as i64, set_name, volume_index])
                    .map_err(db_err)?;
            }
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn clear_verified_suspect_volumes(&self, job_id: JobId) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_rar_verified_suspect WHERE job_id = ?1",
            [job_id.0 as i64],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn add_extracted_member(
        &self,
        job_id: JobId,
        member_name: &str,
        output_path: &Path,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        conn.execute(
            "INSERT OR IGNORE INTO active_extracted (job_id, member_name, output_path)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![
                job_id.0 as i64,
                member_name,
                output_path.to_str().unwrap_or(""),
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn clear_extracted_members(&self, job_id: JobId) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_extracted WHERE job_id = ?1",
            [job_id.0 as i64],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn insert_extraction_chunk(&self, chunk: &ActiveExtractionChunk) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, chunk.job_id)? {
            return Ok(());
        }
        conn.execute(
            "INSERT OR REPLACE INTO active_extraction_chunks
             (job_id, set_name, member_name, volume_index, bytes_written, temp_path,
              start_offset, end_offset)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                chunk.job_id.0 as i64,
                &chunk.set_name,
                &chunk.member_name,
                chunk.volume_index,
                chunk.bytes_written as i64,
                &chunk.temp_path,
                chunk.start_offset as i64,
                chunk.end_offset as i64,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn mark_chunk_verified(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
        volume_index: u32,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE active_extraction_chunks SET verified = 1
             WHERE job_id = ?1 AND set_name = ?2 AND member_name = ?3 AND volume_index = ?4",
            rusqlite::params![job_id.0 as i64, set_name, member_name, volume_index],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn replace_member_chunks(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
        chunks: &[ExtractionChunk],
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        conn.execute_batch("BEGIN").map_err(db_err)?;

        conn.execute(
            "DELETE FROM active_extraction_chunks
             WHERE job_id = ?1 AND set_name = ?2 AND member_name = ?3",
            rusqlite::params![job_id.0 as i64, set_name, member_name],
        )
        .map_err(|error| {
            let _ = conn.execute_batch("ROLLBACK");
            db_err(error)
        })?;

        for chunk in chunks {
            conn.execute(
                "INSERT INTO active_extraction_chunks
                 (job_id, set_name, member_name, volume_index, bytes_written, temp_path,
                  start_offset, end_offset, verified, appended)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    job_id.0 as i64,
                    set_name,
                    &chunk.member_name,
                    chunk.volume_index,
                    chunk.bytes_written as i64,
                    &chunk.temp_path,
                    chunk.start_offset as i64,
                    chunk.end_offset as i64,
                    chunk.verified as i64,
                    chunk.appended as i64,
                ],
            )
            .map_err(|error| {
                let _ = conn.execute_batch("ROLLBACK");
                db_err(error)
            })?;
        }

        conn.execute_batch("COMMIT").map_err(db_err)?;
        Ok(())
    }

    pub fn mark_chunk_appended(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
        volume_index: u32,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE active_extraction_chunks SET appended = 1
             WHERE job_id = ?1 AND set_name = ?2 AND member_name = ?3 AND volume_index = ?4",
            rusqlite::params![job_id.0 as i64, set_name, member_name, volume_index],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn clear_member_chunks(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_extraction_chunks
             WHERE job_id = ?1 AND set_name = ?2 AND member_name = ?3",
            rusqlite::params![job_id.0 as i64, set_name, member_name],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn save_archive_headers(
        &self,
        job_id: JobId,
        set_name: &str,
        headers: &[u8],
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        conn.execute(
            "INSERT OR REPLACE INTO active_archive_headers (job_id, set_name, headers)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![job_id.0 as i64, set_name, headers],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn delete_archive_headers(&self, job_id: JobId, set_name: &str) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_archive_headers WHERE job_id = ?1 AND set_name = ?2",
            rusqlite::params![job_id.0 as i64, set_name],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn save_rar_volume_facts(
        &self,
        job_id: JobId,
        set_name: &str,
        volume_index: u32,
        facts_blob: &[u8],
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        conn.execute(
            "INSERT OR REPLACE INTO active_rar_volume_facts
             (job_id, set_name, volume_index, facts_blob)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![job_id.0 as i64, set_name, volume_index, facts_blob],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn save_detected_archive_identity(
        &self,
        job_id: JobId,
        file_index: u32,
        detected: &DetectedArchiveIdentity,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        conn.execute(
            "INSERT OR REPLACE INTO active_detected_archives
             (job_id, file_index, kind, set_name, volume_index)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                job_id.0 as i64,
                file_index,
                detected.kind.as_str(),
                detected.set_name,
                detected.volume_index,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn delete_detected_archive_identity(
        &self,
        job_id: JobId,
        file_index: u32,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_detected_archives WHERE job_id = ?1 AND file_index = ?2",
            rusqlite::params![job_id.0 as i64, file_index],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn delete_all_rar_volume_facts(&self, job_id: JobId) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_rar_volume_facts WHERE job_id = ?1",
            [job_id.0 as i64],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn delete_rar_volume_facts_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_rar_volume_facts WHERE job_id = ?1 AND set_name = ?2",
            rusqlite::params![job_id.0 as i64, set_name],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn set_volume_status(
        &self,
        job_id: JobId,
        set_name: &str,
        volume_index: u32,
        extracted: bool,
        par2_clean: bool,
        deleted: bool,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        if !active_job_exists(&conn, job_id)? {
            return Ok(());
        }
        conn.execute(
            "INSERT OR REPLACE INTO active_volume_status
             (job_id, set_name, volume_index, extracted, par2_clean, deleted)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                job_id.0 as i64,
                set_name,
                volume_index,
                extracted as i64,
                par2_clean as i64,
                deleted as i64,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn clear_volume_status_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_volume_status WHERE job_id = ?1 AND set_name = ?2",
            rusqlite::params![job_id.0 as i64, set_name],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn clear_verified_suspect_volumes_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_rar_verified_suspect WHERE job_id = ?1 AND set_name = ?2",
            rusqlite::params![job_id.0 as i64, set_name],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn clear_extraction_chunks_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_extraction_chunks WHERE job_id = ?1 AND set_name = ?2",
            rusqlite::params![job_id.0 as i64, set_name],
        )
        .map_err(db_err)?;
        Ok(())
    }
}
