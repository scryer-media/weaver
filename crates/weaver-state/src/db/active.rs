use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use weaver_core::id::{JobId, NzbFileId, SegmentId};

use super::Database;
use crate::StateError;

/// Data for creating an active job.
#[derive(Debug, Clone)]
pub struct ActiveJob {
    pub job_id: JobId,
    pub nzb_hash: [u8; 32],
    pub nzb_path: PathBuf,
    pub output_dir: PathBuf,
    pub created_at: u64,
    pub category: Option<String>,
    pub metadata: Vec<(String, String)>,
}

/// A committed segment for batch insertion.
#[derive(Debug, Clone)]
pub struct CommittedSegment {
    pub job_id: JobId,
    pub file_index: u32,
    pub segment_number: u32,
    pub file_offset: u64,
    pub decoded_size: u32,
    pub crc32: u32,
}

/// A single job's recovered state (loaded from active tables on startup).
#[derive(Debug)]
pub struct RecoveredJob {
    pub job_id: JobId,
    pub nzb_path: PathBuf,
    pub output_dir: PathBuf,
    pub committed_segments: HashSet<SegmentId>,
    pub complete_files: HashSet<NzbFileId>,
    pub extracted_members: HashSet<String>,
    pub status: String,
    pub error: Option<String>,
    pub created_at: u64,
    pub category: Option<String>,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivePar2File {
    pub file_index: u32,
    pub filename: String,
    pub recovery_block_count: u32,
    pub promoted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveExtractionChunk {
    pub job_id: JobId,
    pub set_name: String,
    pub member_name: String,
    pub volume_index: u32,
    pub bytes_written: u64,
    pub temp_path: String,
    pub start_offset: u64,
    pub end_offset: u64,
}

pub type RarVolumeFactsBySet = HashMap<String, Vec<(u32, Vec<u8>)>>;

fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

impl Database {
    /// Insert a new active job.
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
             VALUES (?1, ?2, ?3, ?4, 'downloading', ?5, ?6, ?7)",
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

    /// Update category and/or metadata of an active job.
    pub fn update_active_job(
        &self,
        job_id: JobId,
        category: Option<Option<&str>>,
        metadata: Option<&[(String, String)]>,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        let id = job_id.0 as i64;
        if let Some(cat) = category {
            conn.execute(
                "UPDATE active_jobs SET category = ?1 WHERE job_id = ?2",
                rusqlite::params![cat, id],
            )
            .map_err(db_err)?;
        }
        if let Some(meta) = metadata {
            let json = if meta.is_empty() {
                None
            } else {
                Some(serde_json::to_string(meta).map_err(db_err)?)
            };
            conn.execute(
                "UPDATE active_jobs SET metadata = ?1 WHERE job_id = ?2",
                rusqlite::params![json, id],
            )
            .map_err(db_err)?;
        }
        Ok(())
    }

    /// Update the status of an active job.
    pub fn set_active_job_status(
        &self,
        job_id: JobId,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE active_jobs SET status = ?1, error = ?2 WHERE job_id = ?3",
            rusqlite::params![status, error, job_id.0 as i64],
        )
        .map_err(db_err)?;
        Ok(())
    }

    /// Batch-insert committed segments in a single transaction.
    pub fn commit_segments(&self, segments: &[CommittedSegment]) -> Result<(), StateError> {
        if segments.is_empty() {
            return Ok(());
        }
        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT OR IGNORE INTO active_segments
                     (job_id, file_index, segment_number, file_offset, decoded_size, crc32)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )
                .map_err(db_err)?;
            for seg in segments {
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

    /// Record a completed file.
    pub fn complete_file(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: &[u8; 16],
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT OR IGNORE INTO active_files (job_id, file_index, filename, md5)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![job_id.0 as i64, file_index, filename, md5.as_slice()],
        )
        .map_err(db_err)?;
        Ok(())
    }

    /// Store PAR2 metadata for a job.
    pub fn set_par2_metadata(
        &self,
        job_id: JobId,
        slice_size: u64,
        recovery_block_count: u32,
    ) -> Result<(), StateError> {
        let conn = self.conn();
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

    pub fn load_par2_files(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, ActivePar2File>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT file_index, filename, recovery_block_count, promoted
                 FROM active_par2_files
                 WHERE job_id = ?1",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([job_id.0 as i64], |row| {
                Ok(ActivePar2File {
                    file_index: row.get(0)?,
                    filename: row.get(1)?,
                    recovery_block_count: row.get(2)?,
                    promoted: row.get::<_, i64>(3)? != 0,
                })
            })
            .map_err(db_err)?;
        let mut files = HashMap::new();
        for row in rows {
            let file = row.map_err(db_err)?;
            files.insert(file.file_index, file);
        }
        Ok(files)
    }

    pub fn load_failed_extractions(&self, job_id: JobId) -> Result<HashSet<String>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT member_name FROM active_failed_extractions
                 WHERE job_id = ?1",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([job_id.0 as i64], |row| row.get::<_, String>(0))
            .map_err(db_err)?;
        rows.collect::<Result<HashSet<_>, _>>().map_err(db_err)
    }

    pub fn replace_failed_extractions(
        &self,
        job_id: JobId,
        members: &HashSet<String>,
    ) -> Result<(), StateError> {
        let conn = self.conn();
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

    pub fn load_active_job_normalization_retried(&self, job_id: JobId) -> Result<bool, StateError> {
        let conn = self.conn();
        let result = conn.query_row(
            "SELECT normalization_retried FROM active_jobs WHERE job_id = ?1",
            [job_id.0 as i64],
            |row| row.get::<_, i64>(0),
        );
        match result {
            Ok(value) => Ok(value != 0),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(error) => Err(db_err(error)),
        }
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

    pub fn load_verified_suspect_volumes(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<String, HashSet<u32>>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT set_name, volume_index
                 FROM active_rar_verified_suspect
                 WHERE job_id = ?1",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([job_id.0 as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
            })
            .map_err(db_err)?;
        let mut result = HashMap::<String, HashSet<u32>>::new();
        for row in rows {
            let (set_name, volume_index) = row.map_err(db_err)?;
            result.entry(set_name).or_default().insert(volume_index);
        }
        Ok(result)
    }

    pub fn replace_verified_suspect_volumes(
        &self,
        job_id: JobId,
        set_name: &str,
        volumes: &HashSet<u32>,
    ) -> Result<(), StateError> {
        let conn = self.conn();
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

    /// Record an extracted member.
    pub fn add_extracted_member(
        &self,
        job_id: JobId,
        member_name: &str,
        output_path: &Path,
    ) -> Result<(), StateError> {
        let conn = self.conn();
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

    /// Load all active jobs with their committed segments and complete files.
    /// Replaces journal replay on startup.
    pub fn load_active_jobs(&self) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        let conn = self.conn();
        let mut jobs = HashMap::new();

        // Load job rows.
        {
            let mut stmt = conn
                .prepare(
                    "SELECT job_id, nzb_path, output_dir, status, error,
                            created_at, category, metadata
                     FROM active_jobs",
                )
                .map_err(db_err)?;
            let rows = stmt
                .query_map([], |row| {
                    let job_id = JobId(row.get::<_, i64>(0)? as u64);
                    let nzb_path: String = row.get(1)?;
                    let output_dir: String = row.get(2)?;
                    let status: String = row.get(3)?;
                    let error: Option<String> = row.get(4)?;
                    let created_at = row.get::<_, i64>(5)? as u64;
                    let category: Option<String> = row.get(6)?;
                    let metadata_json: Option<String> = row.get(7)?;
                    let metadata: Vec<(String, String)> = metadata_json
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or_default();
                    Ok(RecoveredJob {
                        job_id,
                        nzb_path: PathBuf::from(nzb_path),
                        output_dir: PathBuf::from(output_dir),
                        committed_segments: HashSet::new(),
                        complete_files: HashSet::new(),
                        extracted_members: HashSet::new(),
                        status,
                        error,
                        created_at,
                        category,
                        metadata,
                    })
                })
                .map_err(db_err)?;
            for row in rows {
                let job = row.map_err(db_err)?;
                jobs.insert(job.job_id, job);
            }
        }

        // Load committed segments.
        {
            let mut stmt = conn
                .prepare("SELECT job_id, file_index, segment_number FROM active_segments")
                .map_err(db_err)?;
            let rows = stmt
                .query_map([], |row| {
                    let job_id = JobId(row.get::<_, i64>(0)? as u64);
                    let file_index: u32 = row.get(1)?;
                    let segment_number: u32 = row.get(2)?;
                    Ok((job_id, file_index, segment_number))
                })
                .map_err(db_err)?;
            for row in rows {
                let (job_id, file_index, segment_number) = row.map_err(db_err)?;
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.committed_segments.insert(SegmentId {
                        file_id: NzbFileId { job_id, file_index },
                        segment_number,
                    });
                }
            }
        }

        // Load complete files.
        {
            let mut stmt = conn
                .prepare("SELECT job_id, file_index FROM active_files")
                .map_err(db_err)?;
            let rows = stmt
                .query_map([], |row| {
                    let job_id = JobId(row.get::<_, i64>(0)? as u64);
                    let file_index: u32 = row.get(1)?;
                    Ok((job_id, file_index))
                })
                .map_err(db_err)?;
            for row in rows {
                let (job_id, file_index) = row.map_err(db_err)?;
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.complete_files.insert(NzbFileId { job_id, file_index });
                }
            }
        }

        // Load extracted members.
        {
            let mut stmt = conn
                .prepare("SELECT job_id, member_name FROM active_extracted")
                .map_err(db_err)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        JobId(row.get::<_, i64>(0)? as u64),
                        row.get::<_, String>(1)?,
                    ))
                })
                .map_err(db_err)?;
            for row in rows {
                let (job_id, member_name) = row.map_err(db_err)?;
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.extracted_members.insert(member_name);
                }
            }
        }

        Ok(jobs)
    }

    /// Return the max job_id across both active_jobs and job_history, or 0.
    pub fn max_job_id_all(&self) -> Result<u64, StateError> {
        let conn = self.conn();
        let max: Option<i64> = conn
            .query_row(
                "SELECT MAX(id) FROM (
                     SELECT MAX(job_id) AS id FROM active_jobs
                     UNION ALL
                     SELECT MAX(job_id) AS id FROM job_history
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(db_err)?;
        Ok(max.unwrap_or(0) as u64)
    }

    /// Archive an active job to job_history and delete all active state.
    /// Runs in a single transaction.
    pub fn archive_job(
        &self,
        job_id: JobId,
        history: &super::history::JobHistoryRow,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        // Insert into job_history.
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
        // Delete active state.
        let id = job_id.0 as i64;
        tx.execute("DELETE FROM active_segments WHERE job_id = ?1", [id])
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
        tx.commit().map_err(db_err)?;
        // Reclaim freed pages.
        conn.execute_batch("PRAGMA incremental_vacuum")
            .map_err(db_err)?;
        Ok(())
    }

    /// Delete all active state for a job (cancellation without archiving).
    pub fn delete_active_job(&self, job_id: JobId) -> Result<(), StateError> {
        let conn = self.conn();
        let id = job_id.0 as i64;
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        tx.execute("DELETE FROM active_segments WHERE job_id = ?1", [id])
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
        tx.commit().map_err(db_err)?;
        conn.execute_batch("PRAGMA incremental_vacuum")
            .map_err(db_err)?;
        Ok(())
    }

    // --- Extraction chunk tracking ---

    /// Record an extraction chunk (per-volume temp file).
    pub fn insert_extraction_chunk(&self, chunk: &ActiveExtractionChunk) -> Result<(), StateError> {
        let conn = self.conn();
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

    /// Mark a chunk as verified (CRC passed).
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

    /// Get all extraction chunks for a job+set.
    pub fn get_extraction_chunks(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<Vec<ExtractionChunk>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT member_name, volume_index, bytes_written, temp_path,
                        start_offset, end_offset, verified, appended
                 FROM active_extraction_chunks
                 WHERE job_id = ?1 AND set_name = ?2
                 ORDER BY member_name, volume_index",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map(rusqlite::params![job_id.0 as i64, set_name], |row| {
                Ok(ExtractionChunk {
                    member_name: row.get(0)?,
                    volume_index: row.get(1)?,
                    bytes_written: row.get::<_, i64>(2)? as u64,
                    temp_path: row.get(3)?,
                    start_offset: row.get::<_, i64>(4)? as u64,
                    end_offset: row.get::<_, i64>(5)? as u64,
                    verified: row.get::<_, i64>(6)? != 0,
                    appended: row.get::<_, i64>(7)? != 0,
                })
            })
            .map_err(db_err)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db_err)
    }

    /// Atomically replace all chunks for a (job, set, member) in a single transaction.
    /// On crash during insert, the transaction rolls back — no partial chunk sets visible.
    pub fn replace_member_chunks(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
        chunks: &[ExtractionChunk],
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute_batch("BEGIN").map_err(db_err)?;

        // Delete any existing chunks for this member.
        conn.execute(
            "DELETE FROM active_extraction_chunks
             WHERE job_id = ?1 AND set_name = ?2 AND member_name = ?3",
            rusqlite::params![job_id.0 as i64, set_name, member_name],
        )
        .map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            db_err(e)
        })?;

        // Insert all new chunks.
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
            .map_err(|e| {
                let _ = conn.execute_batch("ROLLBACK");
                db_err(e)
            })?;
        }

        conn.execute_batch("COMMIT").map_err(db_err)?;
        Ok(())
    }

    /// Mark a single chunk as appended to the output file.
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

    /// Delete all chunk rows for a (job, set, member).
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

    // --- Archive header caching ---

    /// Save serialized archive headers for a set.
    pub fn save_archive_headers(
        &self,
        job_id: JobId,
        set_name: &str,
        headers: &[u8],
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT OR REPLACE INTO active_archive_headers (job_id, set_name, headers)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![job_id.0 as i64, set_name, headers],
        )
        .map_err(db_err)?;
        Ok(())
    }

    /// Load cached archive headers for a set.
    pub fn load_archive_headers(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<Option<Vec<u8>>, StateError> {
        let conn = self.conn();
        let result = conn.query_row(
            "SELECT headers FROM active_archive_headers
             WHERE job_id = ?1 AND set_name = ?2",
            rusqlite::params![job_id.0 as i64, set_name],
            |row| row.get(0),
        );
        match result {
            Ok(data) => Ok(Some(data)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(db_err(e)),
        }
    }

    /// Load all cached archive headers for a job.
    pub fn load_all_archive_headers(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<String, Vec<u8>>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT set_name, headers FROM active_archive_headers
                 WHERE job_id = ?1",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map(rusqlite::params![job_id.0 as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(db_err)?;
        rows.collect::<Result<HashMap<_, _>, _>>().map_err(db_err)
    }

    /// Delete cached archive headers for a single set.
    pub fn delete_archive_headers(&self, job_id: JobId, set_name: &str) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_archive_headers WHERE job_id = ?1 AND set_name = ?2",
            rusqlite::params![job_id.0 as i64, set_name],
        )
        .map_err(db_err)?;
        Ok(())
    }

    /// Save immutable header facts for a single physical RAR volume.
    pub fn save_rar_volume_facts(
        &self,
        job_id: JobId,
        set_name: &str,
        volume_index: u32,
        facts_blob: &[u8],
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT OR REPLACE INTO active_rar_volume_facts
             (job_id, set_name, volume_index, facts_blob)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![job_id.0 as i64, set_name, volume_index, facts_blob],
        )
        .map_err(db_err)?;
        Ok(())
    }

    /// Load all persisted RAR volume facts for a job, grouped by set name.
    pub fn load_all_rar_volume_facts(
        &self,
        job_id: JobId,
    ) -> Result<RarVolumeFactsBySet, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT set_name, volume_index, facts_blob
                 FROM active_rar_volume_facts
                 WHERE job_id = ?1
                 ORDER BY set_name, volume_index",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map(rusqlite::params![job_id.0 as i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(db_err)?;
        let mut grouped: RarVolumeFactsBySet = HashMap::new();
        for row in rows {
            let (set_name, volume_index, facts_blob) = row.map_err(db_err)?;
            grouped
                .entry(set_name)
                .or_default()
                .push((volume_index, facts_blob));
        }
        Ok(grouped)
    }

    /// Delete persisted RAR volume facts for a whole job.
    pub fn delete_all_rar_volume_facts(&self, job_id: JobId) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM active_rar_volume_facts WHERE job_id = ?1",
            [job_id.0 as i64],
        )
        .map_err(db_err)?;
        Ok(())
    }

    // --- Volume status tracking ---

    /// Set volume extraction/verification/deletion status.
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

    /// Load all volumes that were already eagerly deleted for a job.
    pub fn load_deleted_volume_statuses(
        &self,
        job_id: JobId,
    ) -> Result<Vec<(String, u32)>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT set_name, volume_index
                 FROM active_volume_status
                 WHERE job_id = ?1 AND deleted = 1
                 ORDER BY set_name, volume_index",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([job_id.0 as i64], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(db_err)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db_err)
    }
}

/// A recorded extraction chunk.
#[derive(Debug, Clone)]
pub struct ExtractionChunk {
    pub member_name: String,
    pub volume_index: u32,
    pub bytes_written: u64,
    pub temp_path: String,
    pub start_offset: u64,
    pub end_offset: u64,
    pub verified: bool,
    pub appended: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job(id: u64) -> ActiveJob {
        ActiveJob {
            job_id: JobId(id),
            nzb_hash: [0xAA; 32],
            nzb_path: PathBuf::from(format!("/tmp/test_{id}.nzb")),
            output_dir: PathBuf::from(format!("/tmp/output_{id}")),
            created_at: 1700000000 + id,
            category: None,
            metadata: vec![],
        }
    }

    fn sample_segments(job_id: u64, count: u32) -> Vec<CommittedSegment> {
        (0..count)
            .map(|i| CommittedSegment {
                job_id: JobId(job_id),
                file_index: 0,
                segment_number: i,
                file_offset: i as u64 * 768000,
                decoded_size: 768000,
                crc32: 0xDEADBEEF,
            })
            .collect()
    }

    #[test]
    fn create_and_load_active_job() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs.len(), 1);
        let job = &jobs[&JobId(1)];
        assert_eq!(job.nzb_path, PathBuf::from("/tmp/test_1.nzb"));
        assert_eq!(job.status, "downloading");
        assert!(job.committed_segments.is_empty());
        assert!(job.complete_files.is_empty());
    }

    #[test]
    fn commit_and_load_segments() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();
        db.commit_segments(&sample_segments(1, 50)).unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs[&JobId(1)].committed_segments.len(), 50);
    }

    #[test]
    fn duplicate_segments_ignored() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();
        let segs = sample_segments(1, 3);
        db.commit_segments(&segs).unwrap();
        db.commit_segments(&segs).unwrap(); // duplicates

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs[&JobId(1)].committed_segments.len(), 3);
    }

    #[test]
    fn complete_file_and_load() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();
        db.complete_file(JobId(1), 0, "data.rar", &[0x11; 16])
            .unwrap();
        db.complete_file(JobId(1), 1, "data.r00", &[0x22; 16])
            .unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs[&JobId(1)].complete_files.len(), 2);
    }

    #[test]
    fn set_status() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();
        db.set_active_job_status(JobId(1), "verifying", None)
            .unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs[&JobId(1)].status, "verifying");
    }

    #[test]
    fn archive_job_moves_to_history() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();
        db.commit_segments(&sample_segments(1, 10)).unwrap();

        let history = super::super::history::JobHistoryRow {
            job_id: 1,
            name: "test.nzb".to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 1_000_000,
            downloaded_bytes: 1_000_000,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: Some("/tmp/output_1".to_string()),
            nzb_path: Some("/tmp/test_1.nzb".to_string()),
            created_at: 1700000001,
            completed_at: 1700001000,
            metadata: None,
        };
        db.archive_job(JobId(1), &history).unwrap();

        // Active tables should be empty.
        let jobs = db.load_active_jobs().unwrap();
        assert!(jobs.is_empty());

        // History should have the entry.
        let hist = db
            .list_job_history(&super::super::history::HistoryFilter::default())
            .unwrap();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].name, "test.nzb");
    }

    #[test]
    fn delete_active_job_cleans_all() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();
        db.commit_segments(&sample_segments(1, 5)).unwrap();
        db.complete_file(JobId(1), 0, "f.rar", &[0; 16]).unwrap();

        db.delete_active_job(JobId(1)).unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert!(jobs.is_empty());
    }

    #[test]
    fn max_job_id_all_spans_both_tables() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(5)).unwrap();

        let history = super::super::history::JobHistoryRow {
            job_id: 10,
            name: "old.nzb".to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 0,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 0,
            completed_at: 0,
            metadata: None,
        };
        db.insert_job_history(&history).unwrap();

        assert_eq!(db.max_job_id_all().unwrap(), 10);
    }

    #[test]
    fn multiple_jobs_isolated() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();
        db.create_active_job(&sample_job(2)).unwrap();
        db.commit_segments(&sample_segments(1, 3)).unwrap();
        db.commit_segments(&sample_segments(2, 5)).unwrap();

        db.delete_active_job(JobId(1)).unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[&JobId(2)].committed_segments.len(), 5);
    }

    #[test]
    fn metadata_roundtrip() {
        let db = Database::open_in_memory().unwrap();
        let mut job = sample_job(1);
        job.metadata = vec![
            ("title".to_string(), "My Movie".to_string()),
            ("year".to_string(), "2024".to_string()),
        ];
        job.category = Some("movies".to_string());
        db.create_active_job(&job).unwrap();

        let jobs = db.load_active_jobs().unwrap();
        let recovered = &jobs[&JobId(1)];
        assert_eq!(recovered.category, Some("movies".to_string()));
        assert_eq!(recovered.metadata.len(), 2);
        assert_eq!(
            recovered.metadata[0],
            ("title".to_string(), "My Movie".to_string())
        );
    }

    #[test]
    fn par2_metadata_roundtrip() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();
        db.set_par2_metadata(JobId(1), 384000, 8).unwrap();
        // Overwrite should work (INSERT OR REPLACE).
        db.set_par2_metadata(JobId(1), 768000, 16).unwrap();

        let conn = db.conn();
        let (slice, blocks): (i64, u32) = conn
            .query_row(
                "SELECT slice_size, recovery_block_count FROM active_par2 WHERE job_id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(slice, 768000);
        assert_eq!(blocks, 16);
    }

    #[test]
    fn par2_file_roundtrip() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();
        db.upsert_par2_file(JobId(1), 4, "repair.vol00+01.par2", 12, false)
            .unwrap();
        db.set_par2_file_promotion(JobId(1), 4, true).unwrap();

        let files = db.load_par2_files(JobId(1)).unwrap();
        assert_eq!(
            files.get(&4),
            Some(&ActivePar2File {
                file_index: 4,
                filename: "repair.vol00+01.par2".to_string(),
                recovery_block_count: 12,
                promoted: true,
            })
        );
    }

    #[test]
    fn restart_runtime_state_roundtrip() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();

        db.replace_failed_extractions(
            JobId(1),
            &HashSet::from(["E10.mkv".to_string(), "E15.mkv".to_string()]),
        )
        .unwrap();
        db.set_active_job_normalization_retried(JobId(1), true)
            .unwrap();
        db.replace_verified_suspect_volumes(JobId(1), "show", &HashSet::from([37u32, 38u32]))
            .unwrap();

        let failed = db.load_failed_extractions(JobId(1)).unwrap();
        assert_eq!(
            failed,
            HashSet::from(["E10.mkv".to_string(), "E15.mkv".to_string()])
        );
        assert!(db.load_active_job_normalization_retried(JobId(1)).unwrap());
        assert_eq!(
            db.load_verified_suspect_volumes(JobId(1)).unwrap(),
            HashMap::from([("show".to_string(), HashSet::from([37u32, 38u32]))])
        );
    }

    #[test]
    fn extracted_member_roundtrip() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();
        db.add_extracted_member(JobId(1), "movie.mkv", Path::new("/tmp/output_1/movie.mkv"))
            .unwrap();

        let conn = db.conn();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM active_extracted WHERE job_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn extraction_chunk_replace_append_and_clear_roundtrip() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();

        db.replace_member_chunks(
            JobId(1),
            "set",
            "movie.mkv",
            &[
                ExtractionChunk {
                    member_name: "movie.mkv".into(),
                    volume_index: 0,
                    bytes_written: 111,
                    temp_path: "/tmp/chunk0".into(),
                    start_offset: 0,
                    end_offset: 111,
                    verified: true,
                    appended: false,
                },
                ExtractionChunk {
                    member_name: "movie.mkv".into(),
                    volume_index: 1,
                    bytes_written: 222,
                    temp_path: "/tmp/chunk1".into(),
                    start_offset: 111,
                    end_offset: 333,
                    verified: true,
                    appended: false,
                },
            ],
        )
        .unwrap();

        db.mark_chunk_appended(JobId(1), "set", "movie.mkv", 0)
            .unwrap();

        let chunks = db.get_extraction_chunks(JobId(1), "set").unwrap();
        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().any(|c| c.volume_index == 0 && c.appended));
        assert!(
            chunks
                .iter()
                .any(|c| c.volume_index == 1 && c.verified && !c.appended)
        );

        db.clear_member_chunks(JobId(1), "set", "movie.mkv")
            .unwrap();
        assert!(
            db.get_extraction_chunks(JobId(1), "set")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn extraction_chunk_replace_only_overwrites_target_member() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();

        db.replace_member_chunks(
            JobId(1),
            "set",
            "movie_a.mkv",
            &[ExtractionChunk {
                member_name: "movie_a.mkv".into(),
                volume_index: 0,
                bytes_written: 100,
                temp_path: "/tmp/a0".into(),
                start_offset: 0,
                end_offset: 100,
                verified: true,
                appended: false,
            }],
        )
        .unwrap();
        db.replace_member_chunks(
            JobId(1),
            "set",
            "movie_b.mkv",
            &[ExtractionChunk {
                member_name: "movie_b.mkv".into(),
                volume_index: 0,
                bytes_written: 200,
                temp_path: "/tmp/b0".into(),
                start_offset: 0,
                end_offset: 200,
                verified: true,
                appended: false,
            }],
        )
        .unwrap();

        db.replace_member_chunks(
            JobId(1),
            "set",
            "movie_a.mkv",
            &[ExtractionChunk {
                member_name: "movie_a.mkv".into(),
                volume_index: 1,
                bytes_written: 300,
                temp_path: "/tmp/a1".into(),
                start_offset: 100,
                end_offset: 400,
                verified: true,
                appended: false,
            }],
        )
        .unwrap();

        let chunks = db.get_extraction_chunks(JobId(1), "set").unwrap();
        assert_eq!(chunks.len(), 2);
        assert!(
            chunks
                .iter()
                .any(|c| c.member_name == "movie_a.mkv" && c.volume_index == 1)
        );
        assert!(
            chunks
                .iter()
                .any(|c| c.member_name == "movie_b.mkv" && c.bytes_written == 200)
        );
        assert!(
            !chunks
                .iter()
                .any(|c| c.member_name == "movie_a.mkv" && c.volume_index == 0)
        );
    }

    #[test]
    fn archive_headers_roundtrip_and_delete() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();

        db.save_archive_headers(JobId(1), "set-a", &[1, 2, 3])
            .unwrap();
        db.save_archive_headers(JobId(1), "set-b", &[4, 5]).unwrap();

        assert_eq!(
            db.load_archive_headers(JobId(1), "set-a").unwrap(),
            Some(vec![1, 2, 3])
        );

        let all = db.load_all_archive_headers(JobId(1)).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all["set-b"], vec![4, 5]);

        db.delete_archive_headers(JobId(1), "set-a").unwrap();
        assert_eq!(db.load_archive_headers(JobId(1), "set-a").unwrap(), None);
        assert_eq!(db.load_all_archive_headers(JobId(1)).unwrap().len(), 1);
    }

    #[test]
    fn deleted_volume_statuses_roundtrip() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_job(1)).unwrap();

        db.set_volume_status(JobId(1), "set-a", 0, true, true, true)
            .unwrap();
        db.set_volume_status(JobId(1), "set-a", 1, true, true, false)
            .unwrap();
        db.set_volume_status(JobId(1), "set-b", 4, true, true, true)
            .unwrap();

        assert_eq!(
            db.load_deleted_volume_statuses(JobId(1)).unwrap(),
            vec![("set-a".to_string(), 0), ("set-b".to_string(), 4)]
        );
    }
}
