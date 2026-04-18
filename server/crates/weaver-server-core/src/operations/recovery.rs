use std::collections::HashSet;
use std::path::{Path, PathBuf};

use tracing::{error, info, warn};

use crate::Database;
use crate::ingest;
use crate::jobs::working_dir::is_weaver_owned_working_dir;
use crate::runtime::load_global_pause_from_db;
use crate::{JobInfo, JobStatus, RestoreJobRequest};

pub struct RecoveredServerState {
    pub initial_history: Vec<JobInfo>,
    pub to_restore: Vec<RestoreCandidate>,
    pub initial_global_paused: bool,
}

pub struct RestoreCandidate {
    pub job_id: crate::jobs::ids::JobId,
    pub committed_count: usize,
    pub request: RestoreJobRequest,
}

pub async fn recover_server_state(
    db: &Database,
    data_dir: &Path,
    intermediate_dir: &Path,
) -> Result<RecoveredServerState, Box<dyn std::error::Error>> {
    // One-time migration: convert binary journal to SQLite if it exists.
    let journal_path = data_dir.join(".weaver-journal");
    match db.migrate_from_journal(&journal_path) {
        Ok(true) => info!("migrated journal to SQLite"),
        Ok(false) => {}
        Err(e) => error!(error = %e, "journal migration failed"),
    }

    // Recover active jobs from SQLite.
    let max_id = db.max_job_id_all().unwrap_or(0);
    if max_id > 0 {
        info!(max_id, "recovered max job ID");
    }
    crate::ingest::init_job_counter(max_id + 1);

    match db.prune_orphan_active_state() {
        Ok(counts) if counts.total_removed() > 0 => {
            info!(
                active_segments = counts.active_segments,
                active_file_progress = counts.active_file_progress,
                active_files = counts.active_files,
                active_par2 = counts.active_par2,
                active_par2_files = counts.active_par2_files,
                active_extracted = counts.active_extracted,
                active_failed_extractions = counts.active_failed_extractions,
                active_extraction_chunks = counts.active_extraction_chunks,
                active_archive_headers = counts.active_archive_headers,
                active_rar_volume_facts = counts.active_rar_volume_facts,
                active_volume_status = counts.active_volume_status,
                active_rar_verified_suspect = counts.active_rar_verified_suspect,
                "removed orphaned active-state rows before recovery"
            );
        }
        Ok(_) => {}
        Err(error) => {
            warn!(error = %error, "failed to cleanup orphaned active-state rows before recovery");
        }
    }

    let active_jobs = db.load_active_jobs().unwrap_or_default();
    let mut referenced_nzb_paths: HashSet<PathBuf> = active_jobs
        .values()
        .map(|recovered| recovered.nzb_path.clone())
        .collect();
    let mut referenced_intermediate_dirs: HashSet<PathBuf> = active_jobs
        .values()
        .filter(|recovered| recovered.output_dir.starts_with(intermediate_dir))
        .map(|recovered| recovered.output_dir.clone())
        .collect();

    // Split recovered jobs into finished (history) vs in-progress (to restore).
    let mut initial_history = Vec::new();
    let mut to_restore = Vec::new();

    for (job_id, recovered) in active_jobs {
        let is_finished = matches!(
            recovered.status.as_str(),
            "complete" | "failed" | "cancelled"
        );

        if is_finished {
            let name = recovered
                .nzb_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("Unknown")
                .to_string();
            let status = status_str_to_job_status(&recovered.status, recovered.error.as_deref());
            initial_history.push(JobInfo {
                job_id,
                name,
                error: if let JobStatus::Failed { error } = &status {
                    Some(error.clone())
                } else {
                    None
                },
                status,
                progress: 1.0,
                total_bytes: 0,
                downloaded_bytes: 0,
                optional_recovery_bytes: 0,
                optional_recovery_downloaded_bytes: 0,
                failed_bytes: 0,
                health: 1000,
                password: None,
                category: recovered.category,
                metadata: recovered.metadata,
                output_dir: Some(recovered.output_dir.display().to_string()),
                created_at_epoch_ms: recovered.created_at as f64 * 1000.0,
            });
        } else {
            // In-progress job - need to re-parse NZB and restore.
            if !recovered.nzb_path.exists() {
                warn!(
                    job_id = job_id.0,
                    nzb_path = %recovered.nzb_path.display(),
                    "NZB file missing for recovered job, marking as failed"
                );
                initial_history.push(JobInfo {
                    job_id,
                    name: recovered
                        .nzb_path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("Unknown")
                        .to_string(),
                    error: Some("NZB file missing after restart".to_string()),
                    status: JobStatus::Failed {
                        error: "NZB file missing after restart".to_string(),
                    },
                    progress: 0.0,
                    total_bytes: 0,
                    downloaded_bytes: 0,
                    optional_recovery_bytes: 0,
                    optional_recovery_downloaded_bytes: 0,
                    failed_bytes: 0,
                    health: 0,
                    password: None,
                    category: recovered.category,
                    metadata: recovered.metadata,
                    output_dir: Some(recovered.output_dir.display().to_string()),
                    created_at_epoch_ms: recovered.created_at as f64 * 1000.0,
                });
                continue;
            }

            match ingest::parse_persisted_nzb(&recovered.nzb_path) {
                Ok(nzb) => {
                    let spec = ingest::nzb_to_spec(
                        &nzb,
                        &recovered.nzb_path,
                        recovered.category,
                        recovered.metadata,
                    );
                    let status =
                        status_str_to_job_status(&recovered.status, recovered.error.as_deref());
                    let paused_resume_status = recovered
                        .paused_resume_status
                        .as_deref()
                        .map(|status| status_str_to_job_status(status, None));
                    to_restore.push(RestoreCandidate {
                        job_id,
                        committed_count: recovered.committed_segments.len(),
                        request: RestoreJobRequest {
                            job_id,
                            spec,
                            committed_segments: recovered.committed_segments,
                            file_progress: recovered.file_progress,
                            extracted_members: recovered.extracted_members,
                            status,
                            queued_repair_at_epoch_ms: recovered.queued_repair_at_epoch_ms,
                            queued_extract_at_epoch_ms: recovered.queued_extract_at_epoch_ms,
                            paused_resume_status,
                            working_dir: recovered.output_dir,
                        },
                    });
                }
                Err(ingest::PersistedNzbError::Parse(e)) => {
                    warn!(
                        job_id = job_id.0,
                        error = %e,
                        "failed to parse NZB for recovered job"
                    );
                }
                Err(ingest::PersistedNzbError::Io(e)) => {
                    warn!(
                        job_id = job_id.0,
                        error = %e,
                        "failed to read NZB for recovered job"
                    );
                }
            }
        }
    }

    // Also load archived job history from SQLite so the UI can show completed/failed jobs.
    match db.list_job_history(&crate::HistoryFilter::default()) {
        Ok(history_rows) => {
            for row in history_rows {
                if let Some(nzb_path) = row.nzb_path.as_ref() {
                    referenced_nzb_paths.insert(PathBuf::from(nzb_path));
                }
                if let Some(output_dir) = row.output_dir.as_ref() {
                    let output_dir = PathBuf::from(output_dir);
                    if output_dir.starts_with(intermediate_dir) {
                        referenced_intermediate_dirs.insert(output_dir);
                    }
                }
                let job_id = crate::jobs::ids::JobId(row.job_id);
                // Skip if we already have this job from active_jobs recovery.
                if initial_history.iter().any(|job| job.job_id == job_id) {
                    continue;
                }
                let status = status_str_to_job_status(&row.status, row.error_message.as_deref());
                initial_history.push(JobInfo {
                    job_id,
                    name: row.name,
                    error: if let JobStatus::Failed { error } = &status {
                        Some(error.clone())
                    } else {
                        None
                    },
                    status,
                    progress: 1.0,
                    total_bytes: row.total_bytes,
                    downloaded_bytes: row.downloaded_bytes,
                    optional_recovery_bytes: row.optional_recovery_bytes,
                    optional_recovery_downloaded_bytes: row.optional_recovery_downloaded_bytes,
                    failed_bytes: row.failed_bytes,
                    health: row.health,
                    password: None,
                    category: row.category,
                    metadata: row
                        .metadata
                        .and_then(|metadata| serde_json::from_str(&metadata).ok())
                        .unwrap_or_default(),
                    output_dir: row.output_dir,
                    created_at_epoch_ms: row.created_at as f64 * 1000.0,
                });
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to load job history from database");
        }
    }

    match cleanup_unreferenced_intermediate_dirs(intermediate_dir, &referenced_intermediate_dirs) {
        Ok(removed) if removed > 0 => {
            info!(
                removed,
                intermediate_dir = %intermediate_dir.display(),
                "removed unreferenced intermediate directories"
            );
        }
        Ok(_) => {}
        Err(error) => {
            warn!(
                error = %error,
                intermediate_dir = %intermediate_dir.display(),
                "failed to cleanup unreferenced intermediate directories"
            );
        }
    }

    let nzb_dir = data_dir.join(".weaver-nzbs");
    match ingest::cleanup_orphaned_persisted_nzbs(&nzb_dir, &referenced_nzb_paths) {
        Ok(removed) if removed > 0 => {
            info!(removed, nzb_dir = %nzb_dir.display(), "removed orphaned persisted nzbs");
        }
        Ok(_) => {}
        Err(error) => {
            warn!(
                error = %error,
                nzb_dir = %nzb_dir.display(),
                "failed to cleanup orphaned persisted nzbs"
            );
        }
    }

    if !initial_history.is_empty() {
        info!(
            count = initial_history.len(),
            "recovered finished jobs for history"
        );
    }
    if !to_restore.is_empty() {
        info!(count = to_restore.len(), "recovering in-progress jobs");
    }

    let initial_global_paused = load_global_pause_from_db(db).await?;

    Ok(RecoveredServerState {
        initial_history,
        to_restore,
        initial_global_paused,
    })
}

fn cleanup_unreferenced_intermediate_dirs(
    intermediate_dir: &Path,
    referenced_dirs: &HashSet<PathBuf>,
) -> Result<usize, std::io::Error> {
    if !intermediate_dir.exists() {
        return Ok(0);
    }

    let mut removed = 0usize;
    for entry in std::fs::read_dir(intermediate_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let path = entry.path();
        if !is_weaver_owned_working_dir(&path) {
            continue;
        }
        if referenced_dirs.contains(&path) {
            continue;
        }
        std::fs::remove_dir_all(&path)?;
        removed += 1;
    }

    Ok(removed)
}

fn status_str_to_job_status(status: &str, error: Option<&str>) -> JobStatus {
    match status {
        "queued" => JobStatus::Queued,
        "downloading" => JobStatus::Downloading,
        "checking" => JobStatus::Checking,
        "verifying" => JobStatus::Verifying,
        "queued_repair" => JobStatus::QueuedRepair,
        "repairing" => JobStatus::Repairing,
        "queued_extract" => JobStatus::QueuedExtract,
        "extracting" => JobStatus::Extracting,
        "complete" => JobStatus::Complete,
        "failed" => JobStatus::Failed {
            error: error.unwrap_or("unknown error").to_string(),
        },
        "paused" => JobStatus::Paused,
        "cancelled" => JobStatus::Failed {
            error: "cancelled".to_string(),
        },
        other => JobStatus::Failed {
            error: format!("unknown status: {other}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::recover_server_state;
    use std::path::PathBuf;

    use tempfile::TempDir;

    use crate::jobs::working_dir::working_dir_marker_path;
    use crate::{ActiveJob, CommittedSegment, Database, JobHistoryRow, JobId};

    fn sample_active_job(id: u64, nzb_path: PathBuf, output_dir: PathBuf) -> ActiveJob {
        ActiveJob {
            job_id: JobId(id),
            nzb_hash: [0xAA; 32],
            nzb_path,
            output_dir,
            created_at: 1_700_000_000 + id,
            category: None,
            metadata: vec![],
        }
    }

    fn sample_nzb_bytes() -> Vec<u8> {
        br#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="poster" date="1700000000" subject="sample">
            <groups><group>alt.binaries.test</group></groups>
            <segments>
              <segment bytes="10" number="1">abc@test</segment>
            </segments>
          </file>
        </nzb>"#
            .to_vec()
    }

    fn count_rows(db: &Database, table: &str, job_id: u64) -> i64 {
        let conn = db.conn();
        conn.query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE job_id = ?1"),
            [job_id as i64],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn recovery_cleans_orphan_active_rows_and_unreferenced_intermediate_dirs() {
        let temp = TempDir::new().unwrap();
        let data_dir = temp.path().join("data");
        let intermediate_dir = temp.path().join("intermediate");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&intermediate_dir).unwrap();

        let db = Database::open_in_memory().unwrap();
        let active_output_dir = intermediate_dir.join("active-job");
        let history_output_dir = intermediate_dir.join("failed-history-job");
        let orphan_output_dir = intermediate_dir.join("orphan-job");
        let unrelated_output_dir = intermediate_dir.join("not-weaver-owned");
        std::fs::create_dir_all(&active_output_dir).unwrap();
        std::fs::create_dir_all(&history_output_dir).unwrap();
        std::fs::create_dir_all(&orphan_output_dir).unwrap();
        std::fs::create_dir_all(&unrelated_output_dir).unwrap();
        std::fs::write(working_dir_marker_path(&active_output_dir), []).unwrap();
        std::fs::write(working_dir_marker_path(&history_output_dir), []).unwrap();
        std::fs::write(working_dir_marker_path(&orphan_output_dir), []).unwrap();

        let nzb_path = data_dir.join("active-job.nzb");
        std::fs::write(&nzb_path, sample_nzb_bytes()).unwrap();

        db.create_active_job(&sample_active_job(1, nzb_path, active_output_dir.clone()))
            .unwrap();
        db.commit_segments(&[CommittedSegment {
            job_id: JobId(1),
            file_index: 0,
            segment_number: 0,
            file_offset: 0,
            decoded_size: 10,
            crc32: 42,
        }])
        .unwrap();

        {
            let conn = db.conn();
            conn.execute(
                "INSERT INTO active_segments
                 (job_id, file_index, segment_number, file_offset, decoded_size, crc32)
                 VALUES (?1, 0, 0, 0, 10, 99)",
                [99i64],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO active_file_progress
                 (job_id, file_index, contiguous_bytes_written)
                 VALUES (?1, 0, 1024)",
                [100i64],
            )
            .unwrap();
        }

        db.insert_job_history(&JobHistoryRow {
            job_id: 2,
            name: "failed-history".to_string(),
            status: "failed".to_string(),
            error_message: Some("boom".to_string()),
            total_bytes: 10,
            downloaded_bytes: 5,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 5,
            health: 100,
            category: None,
            output_dir: Some(history_output_dir.display().to_string()),
            nzb_path: None,
            created_at: 1_700_000_000,
            completed_at: 1_700_000_100,
            metadata: None,
        })
        .unwrap();

        let recovered = recover_server_state(&db, &data_dir, &intermediate_dir)
            .await
            .unwrap();

        assert_eq!(count_rows(&db, "active_segments", 99), 0);
        assert_eq!(count_rows(&db, "active_file_progress", 100), 0);
        assert_eq!(count_rows(&db, "active_segments", 1), 1);
        assert!(active_output_dir.exists());
        assert!(history_output_dir.exists());
        assert!(!orphan_output_dir.exists());
        assert!(unrelated_output_dir.exists());
        assert_eq!(recovered.to_restore.len(), 1);
    }
}
