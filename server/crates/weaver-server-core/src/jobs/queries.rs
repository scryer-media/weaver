use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::StateError;
use crate::jobs::assembly::{DetectedArchiveIdentity, DetectedArchiveKind};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::record::{ActivePar2File, ExtractionChunk, RarVolumeFactsBySet, RecoveredJob};
use crate::persistence::Database;

use super::repository::db_err;

impl Database {
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

    pub fn load_active_jobs(&self) -> Result<HashMap<JobId, RecoveredJob>, StateError> {
        let conn = self.conn();
        let mut jobs = HashMap::new();

        {
            let mut stmt = conn
                .prepare(
                    "SELECT job_id, nzb_path, output_dir, status, error,
                            created_at, queued_repair_at_epoch_ms,
                            queued_extract_at_epoch_ms, paused_resume_status,
                            category, metadata
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
                    let queued_repair_at_epoch_ms: Option<f64> = row.get(6)?;
                    let queued_extract_at_epoch_ms: Option<f64> = row.get(7)?;
                    let paused_resume_status: Option<String> = row.get(8)?;
                    let category: Option<String> = row.get(9)?;
                    let metadata_json: Option<String> = row.get(10)?;
                    let metadata: Vec<(String, String)> = metadata_json
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or_default();
                    Ok(RecoveredJob {
                        job_id,
                        nzb_path: PathBuf::from(nzb_path),
                        output_dir: PathBuf::from(output_dir),
                        committed_segments: HashSet::new(),
                        file_progress: HashMap::new(),
                        detected_archives: HashMap::new(),
                        complete_files: HashSet::new(),
                        extracted_members: HashSet::new(),
                        status,
                        error,
                        created_at,
                        queued_repair_at_epoch_ms,
                        queued_extract_at_epoch_ms,
                        paused_resume_status,
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

        {
            let mut stmt = conn
                .prepare(
                    "SELECT job_id, file_index, contiguous_bytes_written
                     FROM active_file_progress",
                )
                .map_err(db_err)?;
            let rows = stmt
                .query_map([], |row| {
                    let job_id = JobId(row.get::<_, i64>(0)? as u64);
                    let file_index: u32 = row.get(1)?;
                    let contiguous_bytes_written = row.get::<_, i64>(2)? as u64;
                    Ok((job_id, file_index, contiguous_bytes_written))
                })
                .map_err(db_err)?;
            for row in rows {
                let (job_id, file_index, contiguous_bytes_written) = row.map_err(db_err)?;
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.file_progress
                        .insert(file_index, contiguous_bytes_written);
                }
            }
        }

        {
            let mut stmt = conn
                .prepare(
                    "SELECT job_id, file_index, kind, set_name, volume_index
                     FROM active_detected_archives",
                )
                .map_err(db_err)?;
            let rows = stmt
                .query_map([], |row| {
                    let job_id = JobId(row.get::<_, i64>(0)? as u64);
                    let file_index: u32 = row.get(1)?;
                    let kind = row.get::<_, String>(2)?;
                    let set_name = row.get::<_, String>(3)?;
                    let volume_index: Option<u32> = row.get(4)?;
                    Ok((job_id, file_index, kind, set_name, volume_index))
                })
                .map_err(db_err)?;
            for row in rows {
                let (job_id, file_index, kind, set_name, volume_index) = row.map_err(db_err)?;
                let Some(kind) = DetectedArchiveKind::parse(&kind) else {
                    continue;
                };
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.detected_archives.insert(
                        file_index,
                        DetectedArchiveIdentity {
                            kind,
                            set_name,
                            volume_index,
                        },
                    );
                }
            }
        }

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

    pub fn load_detected_archive_identities(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, DetectedArchiveIdentity>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT file_index, kind, set_name, volume_index
                 FROM active_detected_archives
                 WHERE job_id = ?1",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([job_id.0 as i64], |row| {
                Ok((
                    row.get::<_, u32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<u32>>(3)?,
                ))
            })
            .map_err(db_err)?;
        let mut detected = HashMap::new();
        for row in rows {
            let (file_index, kind, set_name, volume_index) = row.map_err(db_err)?;
            let Some(kind) = DetectedArchiveKind::parse(&kind) else {
                continue;
            };
            detected.insert(
                file_index,
                DetectedArchiveIdentity {
                    kind,
                    set_name,
                    volume_index,
                },
            );
        }
        Ok(detected)
    }

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
            Err(error) => Err(db_err(error)),
        }
    }

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
