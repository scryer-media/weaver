use crate::StateError;
use crate::history;
use crate::history::persistence::replace_job_history_attributes_tx;
use crate::jobs::ids::JobId;
use crate::jobs::persistence::lock_active_job_for_delete_tx;
use crate::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRuntime, SqlTx, StoreDatastore};
use crate::persistence::{Database, DatabaseWriterExecutor};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OrphanActiveStateCounts {
    pub active_segments: usize,
    pub active_file_progress: usize,
    pub active_files: usize,
    pub active_file_identities: usize,
    pub active_par2: usize,
    pub active_par2_files: usize,
    pub active_extracted: usize,
    pub active_failed_extractions: usize,
    pub active_extraction_chunks: usize,
    pub active_archive_headers: usize,
    pub active_rar_volume_facts: usize,
    pub active_detected_archives: usize,
    pub active_volume_status: usize,
    pub active_rar_verified_suspect: usize,
}

impl OrphanActiveStateCounts {
    pub fn total_removed(self) -> usize {
        self.active_segments
            + self.active_file_progress
            + self.active_files
            + self.active_file_identities
            + self.active_par2
            + self.active_par2_files
            + self.active_extracted
            + self.active_failed_extractions
            + self.active_extraction_chunks
            + self.active_archive_headers
            + self.active_rar_volume_facts
            + self.active_detected_archives
            + self.active_volume_status
            + self.active_rar_verified_suspect
    }
}

const INLINE_INCREMENTAL_VACUUM_PAGES: u64 = 256;

const ACTIVE_JOB_CHILD_TABLES: [&str; 14] = [
    "active_segments",
    "active_file_progress",
    "active_files",
    "active_file_identities",
    "active_par2",
    "active_par2_files",
    "active_extracted",
    "active_failed_extractions",
    "active_extraction_chunks",
    "active_archive_headers",
    "active_rar_volume_facts",
    "active_detected_archives",
    "active_volume_status",
    "active_rar_verified_suspect",
];

async fn run_inline_incremental_vacuum(datastore: &StoreDatastore) -> Result<(), StateError> {
    if datastore.engine() != SqlEngine::Sqlite {
        return Ok(());
    }
    SqlRuntime::execute(
        datastore.read_exec(),
        &format!("PRAGMA incremental_vacuum({INLINE_INCREMENTAL_VACUUM_PAGES})"),
        &[],
    )
    .await?;
    Ok(())
}

async fn delete_orphan_rows(tx: &mut SqlTx<'_>, table: &'static str) -> Result<usize, StateError> {
    let deleted = tx
        .execute(
            &format!(
                "DELETE FROM {table}
                 WHERE NOT EXISTS (
                     SELECT 1 FROM active_jobs WHERE active_jobs.job_id = {table}.job_id
                 )"
            ),
            &[],
        )
        .await?;
    Ok(deleted as usize)
}

async fn delete_active_job_rows(tx: &mut SqlTx<'_>, id: i64) -> Result<(), StateError> {
    if matches!(&*tx, SqlTx::Postgres(_)) {
        let mut sql = String::from("WITH ");
        for (idx, table) in ACTIVE_JOB_CHILD_TABLES.iter().enumerate() {
            if idx > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "d{idx} AS (DELETE FROM {table} WHERE job_id = {{}})"
            ));
        }
        sql.push_str(", d_job AS (DELETE FROM active_jobs WHERE job_id = {}) SELECT 1");
        let args = vec![SqlArg::I64(id); ACTIVE_JOB_CHILD_TABLES.len() + 1];
        tx.execute(&sql, &args).await?;
        return Ok(());
    }

    for table in ACTIVE_JOB_CHILD_TABLES {
        tx.execute(
            &format!("DELETE FROM {table} WHERE job_id = {{}}"),
            &[SqlArg::I64(id)],
        )
        .await?;
    }
    tx.execute(
        "DELETE FROM active_jobs WHERE job_id = {}",
        &[SqlArg::I64(id)],
    )
    .await?;
    Ok(())
}

fn history_args(history: &history::JobHistoryRow, job_id: JobId) -> Vec<SqlArg> {
    vec![
        SqlArg::I64(job_id.0 as i64),
        SqlArg::OptBytes(history.job_hash.clone()),
        SqlArg::I64(job_id.0 as i64),
        SqlArg::Text(history.name.clone()),
        SqlArg::Text(history.status.clone()),
        SqlArg::OptText(history.error_message.clone()),
        SqlArg::I64(history.total_bytes as i64),
        SqlArg::I64(history.downloaded_bytes as i64),
        SqlArg::I64(history.optional_recovery_bytes as i64),
        SqlArg::I64(history.optional_recovery_downloaded_bytes as i64),
        SqlArg::I64(history.failed_bytes as i64),
        SqlArg::I64(history.health as i64),
        SqlArg::OptText(history.category.clone()),
        SqlArg::OptText(history.output_dir.clone()),
        SqlArg::OptText(history.nzb_path.clone()),
        SqlArg::I64(job_id.0 as i64),
        SqlArg::I64(job_id.0 as i64),
        SqlArg::I64(history.created_at),
        SqlArg::I64(history.completed_at),
        SqlArg::OptText(history.metadata.clone()),
        SqlArg::OptText(history.last_diagnostic_id.clone()),
        SqlArg::OptI64(history.last_diagnostic_uploaded_at_epoch_ms),
    ]
}

async fn archive_job_sql(
    datastore: StoreDatastore,
    job_id: JobId,
    args: Vec<SqlArg>,
) -> Result<Option<history::JobHistoryRow>, StateError> {
    let archived = SqlRuntime::run_in_transaction(&datastore, "archive_job", |tx| {
        let args = args.clone();
        Box::pin(async move {
            lock_active_job_for_delete_tx(tx, job_id).await?;
            let archived = tx
                .fetch_optional(
                "INSERT INTO job_history
                 (job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
                  optional_recovery_bytes, optional_recovery_downloaded_bytes,
                  failed_bytes, health, category, output_dir, nzb_path, nzb_zstd,
                  created_at, completed_at, metadata, last_diagnostic_id, last_diagnostic_uploaded_at_epoch_ms)
                 VALUES ({}, COALESCE({}, (SELECT nzb_hash FROM active_jobs WHERE job_id = {})),
                         {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                         COALESCE({}, (SELECT nzb_path FROM active_jobs WHERE job_id = {})),
                         (SELECT nzb_zstd FROM active_jobs WHERE job_id = {}),
                         {}, {}, {}, {}, {})
                 ON CONFLICT(job_id) DO UPDATE SET
                    job_hash = excluded.job_hash,
                    name = excluded.name,
                    status = excluded.status,
                    error_message = excluded.error_message,
                    total_bytes = excluded.total_bytes,
                    downloaded_bytes = excluded.downloaded_bytes,
                    optional_recovery_bytes = excluded.optional_recovery_bytes,
                    optional_recovery_downloaded_bytes = excluded.optional_recovery_downloaded_bytes,
                    failed_bytes = excluded.failed_bytes,
                    health = excluded.health,
                    category = excluded.category,
                    output_dir = excluded.output_dir,
                    nzb_path = excluded.nzb_path,
                    nzb_zstd = COALESCE(excluded.nzb_zstd, job_history.nzb_zstd),
                    created_at = excluded.created_at,
                    completed_at = excluded.completed_at,
                    metadata = excluded.metadata,
                    last_diagnostic_id = excluded.last_diagnostic_id,
                    last_diagnostic_uploaded_at_epoch_ms = excluded.last_diagnostic_uploaded_at_epoch_ms
                 RETURNING job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
                    optional_recovery_bytes, optional_recovery_downloaded_bytes,
                    failed_bytes, health, category, output_dir, nzb_path,
                    created_at, completed_at, metadata,
                    last_diagnostic_id, last_diagnostic_uploaded_at_epoch_ms",
                &args,
            )
            .await?
            .map(history::queries::job_history_row_from_sql)
            .transpose()?;
            if let Some(row) = &archived {
                replace_job_history_attributes_tx(tx, row).await?;
            }
            delete_active_job_rows(tx, job_id.0 as i64).await?;
            Ok(archived)
        })
    })
    .await?;
    run_inline_incremental_vacuum(&datastore).await?;
    Ok(archived)
}

impl Database {
    pub fn archive_job(
        &self,
        job_id: JobId,
        history: &history::JobHistoryRow,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = history_args(history, job_id);
        let result = self.run_sql_blocking(archive_job_sql(datastore, job_id, args));
        if let Ok(Some(row)) = &result {
            self.cache_job_history(row.clone());
        }
        result.map(|_| ())
    }

    pub fn delete_active_job(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_active_job", |tx| {
                Box::pin(async move {
                    lock_active_job_for_delete_tx(tx, job_id).await?;
                    delete_active_job_rows(tx, job_id.0 as i64).await?;
                    Ok(())
                })
            })
            .await?;
            run_inline_incremental_vacuum(&datastore).await?;
            Ok(())
        })
    }

    pub fn prune_orphan_active_state(&self) -> Result<OrphanActiveStateCounts, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let counts =
                SqlRuntime::run_in_transaction(&datastore, "prune_orphan_active_state", |tx| {
                    Box::pin(async move {
                        Ok(OrphanActiveStateCounts {
                            active_segments: delete_orphan_rows(tx, "active_segments").await?,
                            active_file_progress: delete_orphan_rows(tx, "active_file_progress")
                                .await?,
                            active_files: delete_orphan_rows(tx, "active_files").await?,
                            active_file_identities: delete_orphan_rows(
                                tx,
                                "active_file_identities",
                            )
                            .await?,
                            active_par2: delete_orphan_rows(tx, "active_par2").await?,
                            active_par2_files: delete_orphan_rows(tx, "active_par2_files").await?,
                            active_extracted: delete_orphan_rows(tx, "active_extracted").await?,
                            active_failed_extractions: delete_orphan_rows(
                                tx,
                                "active_failed_extractions",
                            )
                            .await?,
                            active_extraction_chunks: delete_orphan_rows(
                                tx,
                                "active_extraction_chunks",
                            )
                            .await?,
                            active_archive_headers: delete_orphan_rows(
                                tx,
                                "active_archive_headers",
                            )
                            .await?,
                            active_rar_volume_facts: delete_orphan_rows(
                                tx,
                                "active_rar_volume_facts",
                            )
                            .await?,
                            active_detected_archives: delete_orphan_rows(
                                tx,
                                "active_detected_archives",
                            )
                            .await?,
                            active_volume_status: delete_orphan_rows(tx, "active_volume_status")
                                .await?,
                            active_rar_verified_suspect: delete_orphan_rows(
                                tx,
                                "active_rar_verified_suspect",
                            )
                            .await?,
                        })
                    })
                })
                .await?;

            if counts.total_removed() > 0 {
                run_inline_incremental_vacuum(&datastore).await?;
            }
            Ok(counts)
        })
    }
}

impl DatabaseWriterExecutor {
    pub(crate) fn archive_job(
        &self,
        job_id: JobId,
        history: &history::JobHistoryRow,
    ) -> Result<Option<history::JobHistoryRow>, StateError> {
        let datastore = self.datastore();
        let args = history_args(history, job_id);
        self.run_sql_blocking(archive_job_sql(datastore, job_id, args))
    }
}

#[cfg(test)]
mod tests;
