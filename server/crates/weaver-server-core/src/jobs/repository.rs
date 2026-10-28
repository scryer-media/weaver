// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::history;
use crate::history::persistence::replace_job_history_attributes_tx;
use crate::jobs::ids::JobId;
use crate::jobs::persistence::lock_active_job_for_delete_tx;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, StoreDatastore};
use crate::persistence::{Database, DatabaseWriterExecutor};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OrphanActiveStateCounts {
    pub active_file_progress: usize,
    pub active_files: usize,
    pub active_file_identities: usize,
    pub active_extracted: usize,
    pub active_detected_archives: usize,
    // Every graph checkpoint table together.
    pub graph_checkpoint: usize,
}

impl OrphanActiveStateCounts {
    pub fn total_removed(self) -> usize {
        self.active_file_progress
            + self.active_files
            + self.active_file_identities
            + self.active_extracted
            + self.active_detected_archives
            + self.graph_checkpoint
    }
}

const ACTIVE_JOB_CHILD_TABLES: [&str; 13] = [
    "active_file_progress",
    "active_files",
    "active_file_identities",
    "active_extracted",
    "active_detected_archives",
    "graph_nodes",
    "graph_members",
    "graph_coverage",
    "graph_envelopes",
    "graph_crypt",
    "graph_recovery",
    "graph_holds",
    "graph_facts_parts",
];

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
        SqlArg::I64(job_id.0 as i64),
        SqlArg::I64(job_id.0 as i64),
        SqlArg::OptText(history.server_attribution.clone()),
        SqlArg::I64(job_id.0 as i64),
        SqlArg::I64(job_id.0 as i64),
    ]
}

fn mark_submitted_password(metadata: Option<&str>) -> Result<String, StateError> {
    let key = crate::history::attributes::SUBMITTED_PASSWORD_ATTRIBUTE_KEY;
    let mut parameters: Vec<(String, String)> = metadata
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))?
        .unwrap_or_default();
    parameters.retain(|(name, _)| name != key);
    parameters.push((key.to_string(), "1".to_string()));
    serde_json::to_string(&parameters).map_err(|error| StateError::Database(error.to_string()))
}

async fn archive_job_sql(
    datastore: StoreDatastore,
    job_id: JobId,
    history: history::JobHistoryRow,
    typed_terminal_cause: Option<crate::jobs::SemanticTerminalCause>,
    failure_kind: Option<history::HistoryFailureKind>,
) -> Result<Option<history::JobHistoryRow>, StateError> {
    let archived = SqlRuntime::run_in_transaction(&datastore, "archive_job", |tx| {
        let mut history = history.clone();
        Box::pin(async move {
            tx.execute("UPDATE script_output_state SET next_seq = next_seq WHERE singleton = 1", &[]).await?;
            lock_active_job_for_delete_tx(tx, job_id).await?;
            if let Some(row) = tx.fetch_optional("SELECT state FROM script_job_state WHERE job_id = {}", &[SqlArg::I64(job_id.0 as i64)]).await? {
                let effects: crate::post_processing::effects::JobScriptEffects = serde_json::from_str(&row.text("state")?).map_err(|error| StateError::Database(error.to_string()))?;
                let mut parameters: Vec<(String, String)> = history.metadata.as_deref().map(serde_json::from_str).transpose().map_err(|error| StateError::Database(error.to_string()))?.unwrap_or_default();
                effects.merge_parameters(&mut parameters);
                history.metadata = Some(serde_json::to_string(&parameters).map_err(|error| StateError::Database(error.to_string()))?);
                if history.status == "cancelled" && let Some(directory) = effects.directory {
                    history.output_dir = Some(directory.to_string_lossy().into_owned());
                }
            }
            // The password leaves with the active row; the history row keeps
            // only that the job was submitted with one.
            let submitted_password = tx
                .fetch_optional("SELECT password FROM active_jobs WHERE job_id = {}", &[SqlArg::I64(job_id.0 as i64)])
                .await?
                .map(|row| row.opt_text("password"))
                .transpose()?
                .flatten()
                .is_some_and(|password| !password.is_empty());
            if submitted_password {
                history.metadata = Some(mark_submitted_password(history.metadata.as_deref())?);
            }
            let args = history_args(&history, job_id);
            let archived = tx
                .fetch_optional(
                "INSERT INTO job_history
                 (job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
                  optional_recovery_bytes, optional_recovery_downloaded_bytes,
                  failed_bytes, health, category, output_dir, nzb_path, nzb_zstd,
                  created_at, completed_at, metadata,
                  post_processing_summary, script_results_json, server_attribution, support_facts)
                 VALUES ({}, COALESCE({}, (SELECT nzb_hash FROM active_jobs WHERE job_id = {})),
                         {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                         COALESCE({}, (SELECT nzb_path FROM active_jobs WHERE job_id = {})),
                         (SELECT nzb_zstd FROM active_jobs WHERE job_id = {}),
                         {}, {}, {},
                         COALESCE((SELECT post_processing_summary FROM active_jobs WHERE job_id = {}), 'not_run'),
                         (SELECT script_results_json FROM active_jobs WHERE job_id = {}),
                         COALESCE({}, (SELECT server_attribution FROM active_jobs WHERE job_id = {})),
                         (SELECT support_facts FROM active_jobs WHERE job_id = {}))
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
                    post_processing_summary = excluded.post_processing_summary,
                    script_results_json = excluded.script_results_json,
                    server_attribution =
                        COALESCE(excluded.server_attribution, job_history.server_attribution),
                    support_facts =
                        COALESCE(excluded.support_facts, job_history.support_facts)
                 RETURNING job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
                    optional_recovery_bytes, optional_recovery_downloaded_bytes,
                    failed_bytes, health, category, output_dir, nzb_path,
                    created_at, completed_at, metadata, server_attribution",
                &args,
            )
            .await?
            .map(history::queries::job_history_row_from_sql)
            .transpose()?;
            if let Some(row) = &archived {
                // Set on every archive so a re-archived job never keeps the
                // kind of an earlier failure.
                tx.execute(
                    "UPDATE job_history SET failure_kind = {} WHERE job_id = {}",
                    &[
                        SqlArg::OptText(failure_kind.map(|kind| kind.as_str().to_string())),
                        SqlArg::I64(job_id.0 as i64),
                    ],
                )
                .await?;
                replace_job_history_attributes_tx(tx, row).await?;
                crate::jobs::duplicate_persistence::transition_duplicate_snapshot_for_history_tx(
                    tx,
                    job_id,
                    &row.status,
                    row.error_message.as_deref(),
                    typed_terminal_cause,
                    row.health,
                    row.failed_bytes,
                    row.completed_at,
                )
                .await?;
            }
            delete_active_job_rows(tx, job_id.0 as i64).await?;
            Ok(archived)
        })
    })
    .await?;
    Ok(archived)
}

impl Database {
    pub fn archive_job(
        &self,
        job_id: JobId,
        history: &history::JobHistoryRow,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let history = history.clone();
        // Capture the cache generation before the archive read so a concurrent
        // history-delete that bumps the generation makes the re-cache a no-op
        // instead of resurrecting the just-deleted row.
        let observed_generation = self.job_history_cache_generation();
        let result = self.run_sql_blocking(archive_job_sql(datastore, job_id, history, None, None));
        if let Ok(Some(row)) = &result {
            self.cache_job_history_at(row.clone(), observed_generation);
        }
        result.map(|_| ())
    }

    pub fn delete_active_job(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        let db = self.clone();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_active_job", |tx| {
                Box::pin(async move {
                    crate::post_processing::output::delete_script_state_tx(tx, job_id.0 as i64)
                        .await?;
                    lock_active_job_for_delete_tx(tx, job_id).await?;
                    delete_active_job_rows(tx, job_id.0 as i64).await?;
                    Ok(())
                })
            })
            .await?;
            db.notify_script_events_changed();
            Ok(())
        });
        if result.is_ok() {
            self.forget_script_effects(job_id.0);
        }
        result
    }

    pub fn prune_orphan_active_state(&self) -> Result<OrphanActiveStateCounts, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "prune_orphan_active_state", |tx| {
                Box::pin(async move {
                    Ok(OrphanActiveStateCounts {
                        active_file_progress: delete_orphan_rows(tx, "active_file_progress")
                            .await?,
                        active_files: delete_orphan_rows(tx, "active_files").await?,
                        active_file_identities: delete_orphan_rows(tx, "active_file_identities")
                            .await?,
                        active_extracted: delete_orphan_rows(tx, "active_extracted").await?,
                        active_detected_archives: delete_orphan_rows(
                            tx,
                            "active_detected_archives",
                        )
                        .await?,
                        graph_checkpoint: {
                            let mut removed = 0;
                            for table in crate::jobs::graph_checkpoint::TABLES {
                                removed += delete_orphan_rows(tx, table).await?;
                            }
                            removed
                        },
                    })
                })
            })
            .await
        })
    }
}

impl DatabaseWriterExecutor {
    #[allow(dead_code)] // retained for callers without typed pipeline provenance.
    pub(crate) fn archive_job(
        &self,
        job_id: JobId,
        history: &history::JobHistoryRow,
    ) -> Result<Option<history::JobHistoryRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(archive_job_sql(
            datastore,
            job_id,
            history.clone(),
            None,
            None,
        ))
    }

    pub(crate) fn archive_job_with_terminal_cause(
        &self,
        job_id: JobId,
        history: &history::JobHistoryRow,
        typed_terminal_cause: Option<crate::jobs::SemanticTerminalCause>,
        failure_kind: Option<history::HistoryFailureKind>,
    ) -> Result<Option<history::JobHistoryRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(archive_job_sql(
            datastore,
            job_id,
            history.clone(),
            typed_terminal_cause,
            failure_kind,
        ))
    }
}

#[cfg(test)]
mod tests;
