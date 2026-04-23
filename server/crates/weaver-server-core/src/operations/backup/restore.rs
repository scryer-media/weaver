use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::manifest::{BackupServiceError, RestoreOptions};
use crate::settings::Config;
use crate::{
    Database, HistoryFilter, JobInfo, JobStatus, job_status_from_persisted_str,
    runtime_lanes_from_status_snapshot,
};

pub(crate) fn rewrite_backup_db_for_restore(
    backup_db_path: &Path,
    source_config: &Config,
    options: &RestoreOptions,
) -> Result<(), BackupServiceError> {
    let remap_map: BTreeMap<&str, &str> = options
        .category_remaps
        .iter()
        .map(|entry| (entry.category_name.trim(), entry.new_dest_dir.trim()))
        .collect();
    let old_complete = PathBuf::from(source_config.complete_dir());
    let new_complete = PathBuf::from(
        options
            .complete_dir
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("{}/complete", options.data_dir)),
    );

    let mut missing = Vec::new();
    let mut category_updates = Vec::new();
    for category in &source_config.categories {
        let Some(dest_dir) = &category.dest_dir else {
            continue;
        };
        let new_dest = if let Ok(relative) = Path::new(dest_dir).strip_prefix(&old_complete) {
            new_complete.join(relative).to_string_lossy().to_string()
        } else if let Some(mapped) = remap_map.get(category.name.as_str()) {
            (*mapped).to_string()
        } else {
            missing.push(category.name.clone());
            continue;
        };

        category_updates.push((category.id, new_dest));
    }

    if !missing.is_empty() {
        missing.sort();
        return Err(BackupServiceError::MissingCategoryRemaps(
            missing.join(", "),
        ));
    }

    let conn = rusqlite::Connection::open(backup_db_path)
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;

    set_or_insert_setting(&tx, "data_dir", &options.data_dir)?;

    match &options.intermediate_dir {
        Some(value) if !value.trim().is_empty() => {
            set_or_insert_setting(&tx, "intermediate_dir", value.trim())?
        }
        _ => {
            tx.execute("DELETE FROM settings WHERE key = ?1", ["intermediate_dir"])
                .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        }
    }

    match &options.complete_dir {
        Some(value) if !value.trim().is_empty() => {
            set_or_insert_setting(&tx, "complete_dir", value.trim())?
        }
        _ => {
            tx.execute("DELETE FROM settings WHERE key = ?1", ["complete_dir"])
                .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        }
    }

    for (category_id, new_dest) in category_updates {
        let updated = tx
            .execute(
                "UPDATE categories SET dest_dir = ?1 WHERE id = ?2",
                rusqlite::params![new_dest, category_id],
            )
            .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
        if updated == 0 {
            return Err(BackupServiceError::Validation(format!(
                "category id {category_id} not found in backup"
            )));
        }
    }

    tx.commit()
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;

    let backup_db = Database::open(backup_db_path)
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    let rewritten = backup_db
        .load_config()
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    rewritten
        .validate()
        .map_err(|errors| BackupServiceError::Validation(errors.join("; ")))?;

    Ok(())
}

fn set_or_insert_setting(
    conn: &rusqlite::Transaction<'_>,
    key: &str,
    value: &str,
) -> Result<(), BackupServiceError> {
    let updated = conn
        .execute(
            "UPDATE settings SET value = ?1 WHERE key = ?2",
            rusqlite::params![value, key],
        )
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    if updated == 0 {
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2)",
            rusqlite::params![key, value],
        )
        .map_err(|e| BackupServiceError::Validation(e.to_string()))?;
    }
    Ok(())
}

pub(crate) async fn load_history_snapshot(
    db: &Database,
) -> Result<Vec<JobInfo>, BackupServiceError> {
    let db = db.clone();
    tokio::task::spawn_blocking(move || {
        let rows = db.list_job_history(&HistoryFilter::default())?;
        Ok::<_, crate::StateError>(
            rows.into_iter()
                .map(job_info_from_history)
                .collect::<Vec<_>>(),
        )
    })
    .await
    .map_err(|e| BackupServiceError::Io(e.to_string()))?
    .map_err(|e| BackupServiceError::Io(e.to_string()))
}

fn job_info_from_history(row: crate::JobHistoryRow) -> JobInfo {
    let status = job_status_from_persisted_str(&row.status, row.error_message.as_deref());
    let (download_state, post_state, run_state) = runtime_lanes_from_status_snapshot(&status);

    JobInfo {
        job_id: crate::jobs::ids::JobId(row.job_id),
        name: row.name,
        status: status.clone(),
        download_state,
        post_state,
        run_state,
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
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default(),
        output_dir: row.output_dir,
        error: if let JobStatus::Failed { error } = &status {
            Some(error.clone())
        } else {
            None
        },
        created_at_epoch_ms: row.created_at as f64 * 1000.0,
    }
}

#[cfg(test)]
mod tests {
    use super::job_info_from_history;

    #[test]
    fn history_snapshot_preserves_repairing_and_moving_status_shapes() {
        let repairing = job_info_from_history(crate::JobHistoryRow {
            job_id: 1,
            name: "repairing".into(),
            status: "repairing".into(),
            error_message: None,
            total_bytes: 1,
            downloaded_bytes: 1,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
        });
        assert_eq!(repairing.status, crate::JobStatus::Repairing);
        assert_eq!(repairing.download_state, crate::DownloadState::Complete);
        assert_eq!(repairing.post_state, crate::PostState::Repairing);

        let moving = job_info_from_history(crate::JobHistoryRow {
            job_id: 2,
            name: "moving".into(),
            status: "moving".into(),
            error_message: None,
            total_bytes: 1,
            downloaded_bytes: 1,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
        });
        assert_eq!(moving.status, crate::JobStatus::Moving);
        assert_eq!(moving.download_state, crate::DownloadState::Complete);
        assert_eq!(moving.post_state, crate::PostState::Finalizing);
    }
}
