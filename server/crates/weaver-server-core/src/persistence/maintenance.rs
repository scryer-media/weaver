use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::{Database, StateError};
use crate::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRuntime, StoreDatastore};

pub(crate) const DB_MAINTENANCE_RECLAIM_THRESHOLD_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const DB_MAINTENANCE_FREELIST_RATIO_THRESHOLD: f64 = 0.25;
pub(crate) const DB_MAINTENANCE_INCREMENTAL_BATCH_PAGES: u64 = 4096;
pub(crate) const DB_MAINTENANCE_MAX_INCREMENTAL_PAGES: u64 = 65_536;
pub(crate) const DB_MAINTENANCE_MAX_VACUUM_DURATION: Duration = Duration::from_secs(5);
pub(crate) const DB_MAINTENANCE_WAL_TRUNCATE_THRESHOLD_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const DB_MAINTENANCE_FULL_VACUUM_MIN_INTERVAL: Duration =
    Duration::from_secs(24 * 60 * 60);

const INTERNAL_METADATA_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS weaver_internal_metadata (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);";
const LAST_FULL_VACUUM_KEY: &str = "sqlite_full_vacuum_success_epoch_secs";
const INCREMENTAL_NO_PROGRESS_LIMIT: u64 = 2;

#[derive(Debug, Clone, Copy)]
pub(crate) struct DbMaintenanceOptions {
    pub reclaim_threshold_bytes: u64,
    pub freelist_ratio_threshold: f64,
    pub incremental_batch_pages: u64,
    pub max_incremental_pages: u64,
    pub max_vacuum_duration: Duration,
    pub wal_truncate_threshold_bytes: u64,
    pub full_vacuum_min_interval: Duration,
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
            full_vacuum_min_interval: DB_MAINTENANCE_FULL_VACUUM_MIN_INTERVAL,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FullVacuumDecision {
    NotNeeded,
    Ran,
    SkippedActiveJobs,
    SkippedRecentSuccess,
    Failed,
}

impl FullVacuumDecision {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotNeeded => "not_needed",
            Self::Ran => "ran",
            Self::SkippedActiveJobs => "skipped_active_jobs",
            Self::SkippedRecentSuccess => "skipped_recent_success",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DbMaintenanceReport {
    pub before: DbMaintenanceSnapshot,
    pub after: DbMaintenanceSnapshot,
    pub active_job_count: u64,
    pub full_vacuum_ran: bool,
    pub full_vacuum_decision: FullVacuumDecision,
    pub full_vacuum_last_success_epoch_secs: Option<i64>,
    pub full_vacuum_error: Option<String>,
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
    async fn read(datastore: &StoreDatastore) -> Result<Self, StateError> {
        let page_stats = read_page_stats(datastore).await?;
        let passive_checkpoint = wal_checkpoint(datastore, "PASSIVE").await?;
        let db_path = main_database_path(datastore).await?;
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
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            if datastore.engine() != SqlEngine::Sqlite {
                return Err(StateError::Database(
                    "sqlite maintenance requires a sqlite datastore".to_string(),
                ));
            }
            run_sqlite_maintenance_pass(&datastore, options).await
        })
    }

    pub(crate) fn active_job_ids(&self) -> Result<HashSet<u64>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows =
                SqlRuntime::fetch_all(datastore.read_exec(), "SELECT job_id FROM active_jobs", &[])
                    .await?;
            let mut ids = HashSet::new();
            for row in rows {
                let id = row.i64("job_id")?;
                if let Ok(id) = u64::try_from(id) {
                    ids.insert(id);
                }
            }
            Ok(ids)
        })
    }
}

async fn run_sqlite_maintenance_pass(
    datastore: &StoreDatastore,
    options: DbMaintenanceOptions,
) -> Result<DbMaintenanceReport, StateError> {
    let started = Instant::now();
    let before = DbMaintenanceSnapshot::read(datastore).await?;
    let active_job_count = active_job_count(datastore).await?;

    let mut current = PageStats {
        page_size: before.page_size,
        page_count: before.page_count,
        freelist_count: before.freelist_count,
    };
    let mut incremental_vacuum_ran = false;
    let mut vacuum_iterations = 0u64;
    let mut budget_limited = false;
    let mut page_budget = options.max_incremental_pages;
    let mut full_vacuum_decision = FullVacuumDecision::NotNeeded;
    let mut full_vacuum_last_success_epoch_secs = None;
    let mut full_vacuum_error = None;

    if before.reclaimable_bytes() > options.reclaim_threshold_bytes
        && before.freelist_ratio() > options.freelist_ratio_threshold
    {
        if active_job_count == 0 {
            let now_epoch_secs = current_epoch_secs();
            full_vacuum_last_success_epoch_secs =
                read_last_full_vacuum_success_epoch_secs(datastore).await?;

            if full_vacuum_is_due(
                full_vacuum_last_success_epoch_secs,
                options.full_vacuum_min_interval,
                now_epoch_secs,
            ) {
                match SqlRuntime::execute(datastore.read_exec(), "VACUUM", &[]).await {
                    Ok(_) => {
                        write_last_full_vacuum_success_epoch_secs(datastore, now_epoch_secs)
                            .await?;
                        full_vacuum_last_success_epoch_secs = Some(now_epoch_secs);
                        full_vacuum_decision = FullVacuumDecision::Ran;
                    }
                    Err(error) => {
                        full_vacuum_decision = FullVacuumDecision::Failed;
                        full_vacuum_error = Some(error.to_string());
                        tracing::warn!(
                            error = %error,
                            "failed to run sqlite full vacuum during idle maintenance"
                        );
                    }
                }
            } else {
                full_vacuum_decision = FullVacuumDecision::SkippedRecentSuccess;
            }
        } else {
            full_vacuum_decision = FullVacuumDecision::SkippedActiveJobs;

            let mut no_progress_iterations = 0u64;
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
                SqlRuntime::execute(
                    datastore.read_exec(),
                    &format!("PRAGMA incremental_vacuum({batch})"),
                    &[],
                )
                .await?;
                incremental_vacuum_ran = true;
                vacuum_iterations += 1;
                current = read_page_stats(datastore).await?;

                let reclaimed_pages = previous_freelist.saturating_sub(current.freelist_count);
                if reclaimed_pages == 0 {
                    no_progress_iterations += 1;
                    if no_progress_iterations >= INCREMENTAL_NO_PROGRESS_LIMIT {
                        break;
                    }
                } else {
                    no_progress_iterations = 0;
                    page_budget = page_budget.saturating_sub(reclaimed_pages);
                }
            }

            if current.reclaimable_bytes() > options.reclaim_threshold_bytes
                && (started.elapsed() >= options.max_vacuum_duration || page_budget == 0)
            {
                budget_limited = true;
            }
        }
    }

    let wal_size_for_truncate = current_wal_size(datastore).await?;
    let mut wal_truncate_ran = false;
    let mut wal_truncate_result = None;
    if active_job_count == 0 && wal_size_for_truncate > options.wal_truncate_threshold_bytes {
        match wal_checkpoint(datastore, "TRUNCATE").await {
            Ok(result) => {
                wal_truncate_ran = true;
                wal_truncate_result = Some(result);
            }
            Err(error) => {
                tracing::warn!(error = %error, "failed to truncate sqlite WAL during maintenance");
            }
        }
    }

    let after = DbMaintenanceSnapshot::read(datastore).await?;
    let reclaimed_pages_estimate = before.page_count.saturating_sub(after.page_count);
    let reclaimed_bytes_estimate = before
        .reclaimable_bytes()
        .saturating_sub(after.reclaimable_bytes());

    let report = DbMaintenanceReport {
        before,
        after,
        active_job_count,
        full_vacuum_ran: matches!(full_vacuum_decision, FullVacuumDecision::Ran),
        full_vacuum_decision,
        full_vacuum_last_success_epoch_secs,
        full_vacuum_error,
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
        full_vacuum_ran = report.full_vacuum_ran,
        full_vacuum_decision = report.full_vacuum_decision.as_str(),
        full_vacuum_last_success_epoch_secs = report.full_vacuum_last_success_epoch_secs,
        full_vacuum_error = report.full_vacuum_error.as_deref(),
        incremental_vacuum_ran = report.incremental_vacuum_ran,
        vacuum_iterations = report.vacuum_iterations,
        budget_limited = report.budget_limited,
        "sqlite maintenance pass complete"
    );
}

async fn read_page_stats(datastore: &StoreDatastore) -> Result<PageStats, StateError> {
    Ok(PageStats {
        page_size: pragma_u64(datastore, "page_size").await?,
        page_count: pragma_u64(datastore, "page_count").await?,
        freelist_count: pragma_u64(datastore, "freelist_count").await?,
    })
}

async fn pragma_u64(datastore: &StoreDatastore, name: &'static str) -> Result<u64, StateError> {
    let row = SqlRuntime::fetch_optional(datastore.read_exec(), &format!("PRAGMA {name}"), &[])
        .await?
        .ok_or_else(|| StateError::Database(format!("PRAGMA {name} returned no rows")))?;
    Ok(row.i64_at(0)?.max(0) as u64)
}

async fn wal_checkpoint(
    datastore: &StoreDatastore,
    mode: &'static str,
) -> Result<WalCheckpointResult, StateError> {
    let row = SqlRuntime::fetch_optional(
        datastore.read_exec(),
        &format!("PRAGMA wal_checkpoint({mode})"),
        &[],
    )
    .await?
    .ok_or_else(|| StateError::Database(format!("wal_checkpoint({mode}) returned no rows")))?;
    Ok(WalCheckpointResult {
        busy: row.i64_at(0)?,
        log_frames: row.i64_at(1)?,
        checkpointed_frames: row.i64_at(2)?,
    })
}

async fn active_job_count(datastore: &StoreDatastore) -> Result<u64, StateError> {
    let row = SqlRuntime::fetch_optional(
        datastore.read_exec(),
        "SELECT COUNT(*) AS count FROM active_jobs",
        &[],
    )
    .await?;
    Ok(row
        .map(|row| row.i64("count"))
        .transpose()?
        .unwrap_or(0)
        .max(0) as u64)
}

fn current_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn full_vacuum_is_due(
    last_success_epoch_secs: Option<i64>,
    min_interval: Duration,
    now_epoch_secs: i64,
) -> bool {
    let Some(last_success_epoch_secs) = last_success_epoch_secs else {
        return true;
    };
    let min_interval_secs = i64::try_from(min_interval.as_secs()).unwrap_or(i64::MAX);
    now_epoch_secs.saturating_sub(last_success_epoch_secs) >= min_interval_secs
}

async fn ensure_internal_metadata_table(datastore: &StoreDatastore) -> Result<(), StateError> {
    SqlRuntime::execute(datastore.read_exec(), INTERNAL_METADATA_TABLE_SQL, &[]).await?;
    Ok(())
}

async fn read_last_full_vacuum_success_epoch_secs(
    datastore: &StoreDatastore,
) -> Result<Option<i64>, StateError> {
    ensure_internal_metadata_table(datastore).await?;
    let value = SqlRuntime::fetch_optional(
        datastore.read_exec(),
        "SELECT value FROM weaver_internal_metadata WHERE key = {}",
        &[SqlArg::Text(LAST_FULL_VACUUM_KEY.to_string())],
    )
    .await?
    .map(|row| row.text("value"))
    .transpose()?;

    Ok(value.and_then(|value| value.parse::<i64>().ok()))
}

async fn write_last_full_vacuum_success_epoch_secs(
    datastore: &StoreDatastore,
    epoch_secs: i64,
) -> Result<(), StateError> {
    ensure_internal_metadata_table(datastore).await?;
    SqlRuntime::execute(
        datastore.read_exec(),
        "INSERT INTO weaver_internal_metadata (key, value)
         VALUES ({}, {})
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        &[
            SqlArg::Text(LAST_FULL_VACUUM_KEY.to_string()),
            SqlArg::Text(epoch_secs.to_string()),
        ],
    )
    .await?;
    Ok(())
}

async fn current_wal_size(datastore: &StoreDatastore) -> Result<u64, StateError> {
    let db_path = main_database_path(datastore).await?;
    Ok(file_size_bytes(db_path.as_deref().map(wal_path_for).as_deref()).unwrap_or(0))
}

async fn main_database_path(datastore: &StoreDatastore) -> Result<Option<PathBuf>, StateError> {
    let rows = SqlRuntime::fetch_all(datastore.read_exec(), "PRAGMA database_list", &[]).await?;
    for row in rows {
        let name = row.text("name")?;
        let path = row.opt_text("file")?.unwrap_or_default();
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
