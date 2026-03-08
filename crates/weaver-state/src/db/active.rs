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
    pub status: String,
    pub error: Option<String>,
    pub created_at: u64,
    pub category: Option<String>,
    pub metadata: Vec<(String, String)>,
}

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
                        file_id: NzbFileId {
                            job_id,
                            file_index,
                        },
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
                    job.complete_files.insert(NzbFileId {
                        job_id,
                        file_index,
                    });
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
              failed_bytes, health, category, output_dir, nzb_path,
              created_at, completed_at, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            rusqlite::params![
                history.job_id as i64,
                history.name,
                history.status,
                history.error_message,
                history.total_bytes as i64,
                history.downloaded_bytes as i64,
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
        tx.execute("DELETE FROM active_extracted WHERE job_id = ?1", [id])
            .map_err(db_err)?;
        tx.execute("DELETE FROM active_jobs WHERE job_id = ?1", [id])
            .map_err(db_err)?;
        tx.commit().map_err(db_err)?;
        // Reclaim freed pages.
        conn.execute_batch("PRAGMA incremental_vacuum").map_err(db_err)?;
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
        tx.execute("DELETE FROM active_extracted WHERE job_id = ?1", [id])
            .map_err(db_err)?;
        tx.execute("DELETE FROM active_jobs WHERE job_id = ?1", [id])
            .map_err(db_err)?;
        tx.commit().map_err(db_err)?;
        conn.execute_batch("PRAGMA incremental_vacuum").map_err(db_err)?;
        Ok(())
    }
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
        assert_eq!(recovered.metadata[0], ("title".to_string(), "My Movie".to_string()));
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
}
