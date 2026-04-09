use std::collections::HashSet;
use std::path::{Path, PathBuf};

use tracing::{error, info, warn};

use crate::Database;
use crate::ingest;
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

    let active_jobs = db.load_active_jobs().unwrap_or_default();
    let mut referenced_nzb_paths: HashSet<PathBuf> = active_jobs
        .values()
        .map(|recovered| recovered.nzb_path.clone())
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
