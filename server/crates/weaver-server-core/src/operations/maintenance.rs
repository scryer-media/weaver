use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::Database;
use crate::persistence::maintenance::DbMaintenanceOptions;
use crate::persistence::sql_runtime::SqlEngine;

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
const STALE_STAGING_TTL: Duration = Duration::from_secs(24 * 60 * 60);

pub fn spawn_maintenance_worker(
    db: Database,
    complete_dir: PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_maintenance_pass(db.clone(), complete_dir.clone()).await;

        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + MAINTENANCE_INTERVAL,
            MAINTENANCE_INTERVAL,
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            run_maintenance_pass(db.clone(), complete_dir.clone()).await;
        }
    })
}

async fn run_maintenance_pass(db: Database, complete_dir: PathBuf) {
    if db.datastore().engine() == SqlEngine::Sqlite {
        let sqlite_db = db.clone();
        match tokio::task::spawn_blocking(move || {
            sqlite_db.run_sqlite_maintenance_pass(DbMaintenanceOptions::default())
        })
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "sqlite maintenance pass failed");
            }
            Err(error) => {
                tracing::warn!(error = %error, "sqlite maintenance worker panicked");
            }
        }
    } else {
        tracing::debug!("skipping sqlite maintenance for non-sqlite datastore");
    }

    match tokio::task::spawn_blocking(move || run_staging_cleanup(&db, &complete_dir)).await {
        Ok(Ok(report)) => {
            tracing::info!(
                staging_dirs_removed = report.removed_count,
                staging_bytes_removed = report.removed_bytes,
                "stale staging cleanup complete"
            );
        }
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "stale staging cleanup failed");
        }
        Err(error) => {
            tracing::warn!(error = %error, "stale staging cleanup worker panicked");
        }
    }
}

fn run_staging_cleanup(db: &Database, complete_dir: &Path) -> Result<StagingCleanupReport, String> {
    let active_job_ids = db.active_job_ids().map_err(|error| error.to_string())?;
    cleanup_stale_staging_dirs(complete_dir, &active_job_ids, STALE_STAGING_TTL)
        .map_err(|error| error.to_string())
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct StagingCleanupReport {
    removed_count: u64,
    removed_bytes: u64,
}

fn cleanup_stale_staging_dirs(
    complete_dir: &Path,
    active_job_ids: &HashSet<u64>,
    ttl: Duration,
) -> io::Result<StagingCleanupReport> {
    cleanup_stale_staging_dirs_at(complete_dir, active_job_ids, ttl, SystemTime::now())
}

fn cleanup_stale_staging_dirs_at(
    complete_dir: &Path,
    active_job_ids: &HashSet<u64>,
    ttl: Duration,
    now: SystemTime,
) -> io::Result<StagingCleanupReport> {
    let staging_root = complete_dir.join(".weaver-staging");
    let mut report = StagingCleanupReport::default();
    let entries = match fs::read_dir(&staging_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(report),
        Err(error) => return Err(error),
    };

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let metadata = match entry.metadata() {
            Ok(metadata) if metadata.is_dir() => metadata,
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let Some(job_id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u64>().ok())
        else {
            continue;
        };
        if active_job_ids.contains(&job_id) {
            continue;
        }
        let age = match metadata.modified().and_then(|modified| {
            now.duration_since(modified)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
        }) {
            Ok(age) => age,
            Err(error) => {
                tracing::debug!(
                    path = %path.display(),
                    error = %error,
                    "skipping stale staging cleanup because directory mtime is uncertain"
                );
                continue;
            }
        };
        if age < ttl {
            continue;
        }

        let scan = match scan_staging_tree(&path, &metadata) {
            Ok(scan) => scan,
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %error,
                    "skipping stale staging cleanup because staging tree scan failed"
                );
                continue;
            }
        };
        let newest_age = match now.duration_since(scan.newest_modified) {
            Ok(age) => age,
            Err(error) => {
                tracing::debug!(
                    path = %path.display(),
                    error = %error,
                    "skipping stale staging cleanup because staging tree mtime is in the future"
                );
                continue;
            }
        };
        if newest_age < ttl {
            continue;
        }

        let removed_bytes = scan.bytes;
        fs::remove_dir_all(&path)?;
        report.removed_count += 1;
        report.removed_bytes = report.removed_bytes.saturating_add(removed_bytes);
    }

    Ok(report)
}

#[derive(Debug, Clone, Copy)]
struct StagingTreeScan {
    bytes: u64,
    newest_modified: SystemTime,
}

fn scan_staging_tree(path: &Path, root_metadata: &fs::Metadata) -> io::Result<StagingTreeScan> {
    let mut scan = StagingTreeScan {
        bytes: 0,
        newest_modified: root_metadata.modified()?,
    };
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            let modified = metadata.modified()?;
            if modified > scan.newest_modified {
                scan.newest_modified = modified;
            }
            if metadata.is_dir() {
                stack.push(entry.path());
            } else if metadata.is_file() {
                scan.bytes = scan.bytes.saturating_add(metadata.len());
            }
        }
    }
    Ok(scan)
}

#[cfg(test)]
mod tests;
