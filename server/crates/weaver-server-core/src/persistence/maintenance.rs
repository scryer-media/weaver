use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rusqlite::Connection;

use super::{Database, StateError};
use crate::jobs::repository::db_err;

pub(crate) const DB_MAINTENANCE_RECLAIM_THRESHOLD_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const DB_MAINTENANCE_FREELIST_RATIO_THRESHOLD: f64 = 0.25;
pub(crate) const DB_MAINTENANCE_INCREMENTAL_BATCH_PAGES: u64 = 4096;
pub(crate) const DB_MAINTENANCE_MAX_INCREMENTAL_PAGES: u64 = 65_536;
pub(crate) const DB_MAINTENANCE_MAX_VACUUM_DURATION: Duration = Duration::from_secs(5);
pub(crate) const DB_MAINTENANCE_WAL_TRUNCATE_THRESHOLD_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub(crate) struct DbMaintenanceOptions {
    pub reclaim_threshold_bytes: u64,
    pub freelist_ratio_threshold: f64,
    pub incremental_batch_pages: u64,
    pub max_incremental_pages: u64,
    pub max_vacuum_duration: Duration,
    pub wal_truncate_threshold_bytes: u64,
}

impl Default for DbMaintenanceOptions {
    fn default() -> Self {
        Self {
            reclaim_threshold_bytes: DB_MAINTENANCE_RECLAIM_THRESHOLD_BYTES,
            freelist_ratio_threshold: DB_MAINTENANCE_FREELIST_RATIO_THRESHOLD,
            incremental_batch_pages: DB_MAINTENANCE_INCREMENTAL_BATCH_PAGES,
            max_incremental_pages: DB_MAINTENANCE_MAX_INCREMENTAL_PAGES,
            max_vacuum_duration: DB_MAINTENANCE_MAX_VACUUM_DURATION,
            wal_truncate_threshold_bytes: DB_MAINTENANCE_WAL_TRUNCATE_THRESHOLD_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DbMaintenanceReport {
    pub before: DbMaintenanceSnapshot,
    pub after: DbMaintenanceSnapshot,
    pub active_job_count: u64,
    pub incremental_vacuum_ran: bool,
    pub vacuum_iterations: u64,
    pub reclaimed_pages_estimate: u64,
    pub reclaimed_bytes_estimate: u64,
    pub budget_limited: bool,
    pub wal_truncate_ran: bool,
    pub wal_truncate_result: Option<WalCheckpointResult>,
}

#[derive(Debug, Clone)]
pub(crate) struct DbMaintenanceSnapshot {
    pub page_size: u64,
    pub page_count: u64,
    pub freelist_count: u64,
    pub passive_checkpoint: WalCheckpointResult,
    pub db_size_bytes: Option<u64>,
    pub wal_size_bytes: Option<u64>,
}

impl DbMaintenanceSnapshot {
    fn read(conn: &Connection) -> Result<Self, StateError> {
        let page_stats = read_page_stats(conn)?;
        let passive_checkpoint = wal_checkpoint(conn, "PASSIVE")?;
        let db_path = main_database_path(conn)?;
        let wal_path = db_path.as_deref().map(wal_path_for);
        Ok(Self {
            page_size: page_stats.page_size,
            page_count: page_stats.page_count,
            freelist_count: page_stats.freelist_count,
            passive_checkpoint,
            db_size_bytes: file_size_bytes(db_path.as_deref()),
            wal_size_bytes: file_size_bytes(wal_path.as_deref()),
        })
    }

    pub(crate) fn reclaimable_bytes(&self) -> u64 {
        self.page_size.saturating_mul(self.freelist_count)
    }

    fn freelist_ratio(&self) -> f64 {
        if self.page_count == 0 {
            0.0
        } else {
            self.freelist_count as f64 / self.page_count as f64
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WalCheckpointResult {
    pub busy: i64,
    pub log_frames: i64,
    pub checkpointed_frames: i64,
}

#[derive(Debug, Clone, Copy)]
struct PageStats {
    page_size: u64,
    page_count: u64,
    freelist_count: u64,
}

impl PageStats {
    fn reclaimable_bytes(self) -> u64 {
        self.page_size.saturating_mul(self.freelist_count)
    }
}

impl Database {
    pub(crate) fn run_sqlite_maintenance_pass(
        &self,
        options: DbMaintenanceOptions,
    ) -> Result<DbMaintenanceReport, StateError> {
        let started = Instant::now();
        let conn = self.conn();
        let before = DbMaintenanceSnapshot::read(&conn)?;
        let active_job_count = active_job_count(&conn)?;

        let mut current = PageStats {
            page_size: before.page_size,
            page_count: before.page_count,
            freelist_count: before.freelist_count,
        };
        let mut incremental_vacuum_ran = false;
        let mut vacuum_iterations = 0u64;
        let mut budget_limited = false;
        let mut page_budget = options.max_incremental_pages;

        if before.reclaimable_bytes() > options.reclaim_threshold_bytes
            && before.freelist_ratio() > options.freelist_ratio_threshold
        {
            loop {
                if current.reclaimable_bytes() <= options.reclaim_threshold_bytes {
                    break;
                }
                if started.elapsed() >= options.max_vacuum_duration || page_budget == 0 {
                    budget_limited = true;
                    break;
                }

                let batch = options.incremental_batch_pages.max(1).min(page_budget);
                let previous_freelist = current.freelist_count;
                conn.execute_batch(&format!("PRAGMA incremental_vacuum({batch})"))
                    .map_err(db_err)?;
                incremental_vacuum_ran = true;
                vacuum_iterations += 1;
                page_budget = page_budget.saturating_sub(batch);
                current = read_page_stats(&conn)?;

                if current.freelist_count >= previous_freelist {
                    break;
                }
            }

            if current.reclaimable_bytes() > options.reclaim_threshold_bytes
                && (started.elapsed() >= options.max_vacuum_duration || page_budget == 0)
            {
                budget_limited = true;
            }
        }

        let wal_size_for_truncate = current_wal_size(&conn)?;
        let mut wal_truncate_ran = false;
        let mut wal_truncate_result = None;
        if active_job_count == 0 && wal_size_for_truncate > options.wal_truncate_threshold_bytes {
            match wal_checkpoint(&conn, "TRUNCATE") {
                Ok(result) => {
                    wal_truncate_ran = true;
                    wal_truncate_result = Some(result);
                }
                Err(error) => {
                    tracing::warn!(error = %error, "failed to truncate sqlite WAL during maintenance");
                }
            }
        }

        let after = DbMaintenanceSnapshot::read(&conn)?;
        let reclaimed_pages_estimate = before.page_count.saturating_sub(after.page_count);
        let reclaimed_bytes_estimate = before
            .reclaimable_bytes()
            .saturating_sub(after.reclaimable_bytes());

        let report = DbMaintenanceReport {
            before,
            after,
            active_job_count,
            incremental_vacuum_ran,
            vacuum_iterations,
            reclaimed_pages_estimate,
            reclaimed_bytes_estimate,
            budget_limited,
            wal_truncate_ran,
            wal_truncate_result,
        };
        log_report(&report);
        Ok(report)
    }

    pub(crate) fn active_job_ids(&self) -> Result<HashSet<u64>, StateError> {
        let conn = self.read_conn();
        let mut stmt = conn
            .prepare("SELECT job_id FROM active_jobs")
            .map_err(db_err)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(db_err)?;
        let mut ids = HashSet::new();
        for row in rows {
            let id = row.map_err(db_err)?;
            if let Ok(id) = u64::try_from(id) {
                ids.insert(id);
            }
        }
        Ok(ids)
    }
}

fn log_report(report: &DbMaintenanceReport) {
    tracing::info!(
        page_size = report.before.page_size,
        page_count_before = report.before.page_count,
        page_count_after = report.after.page_count,
        freelist_count_before = report.before.freelist_count,
        freelist_count_after = report.after.freelist_count,
        reclaimable_bytes_before = report.before.reclaimable_bytes(),
        reclaimable_bytes_after = report.after.reclaimable_bytes(),
        reclaimed_pages_estimate = report.reclaimed_pages_estimate,
        reclaimed_bytes_estimate = report.reclaimed_bytes_estimate,
        db_size_bytes_before = report.before.db_size_bytes,
        db_size_bytes_after = report.after.db_size_bytes,
        wal_size_bytes_before = report.before.wal_size_bytes,
        wal_size_bytes_after = report.after.wal_size_bytes,
        wal_passive_busy_before = report.before.passive_checkpoint.busy,
        wal_passive_log_frames_before = report.before.passive_checkpoint.log_frames,
        wal_passive_checkpointed_frames_before =
            report.before.passive_checkpoint.checkpointed_frames,
        wal_passive_busy_after = report.after.passive_checkpoint.busy,
        wal_passive_log_frames_after = report.after.passive_checkpoint.log_frames,
        wal_passive_checkpointed_frames_after = report.after.passive_checkpoint.checkpointed_frames,
        wal_truncate_ran = report.wal_truncate_ran,
        wal_truncate_busy = report.wal_truncate_result.map(|result| result.busy),
        wal_truncate_log_frames = report.wal_truncate_result.map(|result| result.log_frames),
        wal_truncate_checkpointed_frames = report
            .wal_truncate_result
            .map(|result| result.checkpointed_frames),
        active_job_count = report.active_job_count,
        incremental_vacuum_ran = report.incremental_vacuum_ran,
        vacuum_iterations = report.vacuum_iterations,
        budget_limited = report.budget_limited,
        "sqlite maintenance pass complete"
    );
}

fn read_page_stats(conn: &Connection) -> Result<PageStats, StateError> {
    Ok(PageStats {
        page_size: pragma_u64(conn, "page_size")?,
        page_count: pragma_u64(conn, "page_count")?,
        freelist_count: pragma_u64(conn, "freelist_count")?,
    })
}

fn pragma_u64(conn: &Connection, name: &'static str) -> Result<u64, StateError> {
    conn.query_row(&format!("PRAGMA {name}"), [], |row| row.get::<_, i64>(0))
        .map(|value| value.max(0) as u64)
        .map_err(db_err)
}

fn wal_checkpoint(
    conn: &Connection,
    mode: &'static str,
) -> Result<WalCheckpointResult, StateError> {
    conn.query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |row| {
        Ok(WalCheckpointResult {
            busy: row.get(0)?,
            log_frames: row.get(1)?,
            checkpointed_frames: row.get(2)?,
        })
    })
    .map_err(db_err)
}

fn active_job_count(conn: &Connection) -> Result<u64, StateError> {
    conn.query_row("SELECT COUNT(*) FROM active_jobs", [], |row| {
        row.get::<_, i64>(0)
    })
    .map(|value| value.max(0) as u64)
    .map_err(db_err)
}

fn current_wal_size(conn: &Connection) -> Result<u64, StateError> {
    let db_path = main_database_path(conn)?;
    Ok(file_size_bytes(db_path.as_deref().map(wal_path_for).as_deref()).unwrap_or(0))
}

fn main_database_path(conn: &Connection) -> Result<Option<PathBuf>, StateError> {
    let mut stmt = conn.prepare("PRAGMA database_list").map_err(db_err)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })
        .map_err(db_err)?;

    for row in rows {
        let (name, path) = row.map_err(db_err)?;
        if name == "main" && !path.is_empty() {
            return Ok(Some(PathBuf::from(path)));
        }
    }

    Ok(None)
}

fn wal_path_for(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push("-wal");
    PathBuf::from(value)
}

fn file_size_bytes(path: Option<&Path>) -> Option<u64> {
    path.and_then(|path| fs::metadata(path).ok())
        .map(|metadata| metadata.len())
}

#[cfg(test)]
mod tests;
