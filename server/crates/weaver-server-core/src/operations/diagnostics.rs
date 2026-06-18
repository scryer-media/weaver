use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::history::{
    DIAGNOSTIC_INCLUDE_SERVER_HOSTNAMES_ATTRIBUTE_KEY, DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY,
};
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime};
use crate::{Database, StateError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagnosticRunStage {
    Queued,
    Running,
    Collecting,
    Uploading,
    Complete,
    Failed,
}

impl DiagnosticRunStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Collecting => "collecting",
            Self::Uploading => "uploading",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    pub fn is_active(self) -> bool {
        !matches!(self, Self::Complete | Self::Failed)
    }

    fn parse(value: &str) -> Result<Self, StateError> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "collecting" => Ok(Self::Collecting),
            "uploading" => Ok(Self::Uploading),
            "complete" => Ok(Self::Complete),
            "failed" => Ok(Self::Failed),
            other => Err(StateError::Database(format!(
                "invalid diagnostic run stage `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticRunRow {
    pub source_job_id: u64,
    pub diagnostic_job_id: u64,
    pub diagnostic_id: Option<String>,
    pub stage: DiagnosticRunStage,
    pub include_server_hostnames: bool,
    pub rerun_succeeded: Option<bool>,
    pub error_message: Option<String>,
    pub created_at_epoch_ms: i64,
    pub updated_at_epoch_ms: i64,
    pub last_activity_at_epoch_ms: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum DiagnosticRunInsertError {
    #[error("source history row {0} does not exist")]
    MissingSourceJob(u64),
    #[error("diagnostic run already active for source job {0}")]
    ActiveRunExists(u64),
    #[error(transparent)]
    State(#[from] StateError),
}

pub fn diagnostic_source_job_id(metadata: &[(String, String)]) -> Option<u64> {
    metadata
        .iter()
        .find(|(key, _)| key == DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY)
        .and_then(|(_, value)| value.parse::<u64>().ok())
}

pub fn diagnostic_include_server_hostnames(metadata: &[(String, String)]) -> bool {
    metadata
        .iter()
        .find(|(key, _)| key == DIAGNOSTIC_INCLUDE_SERVER_HOSTNAMES_ATTRIBUTE_KEY)
        .and_then(|(_, value)| value.parse::<bool>().ok())
        .unwrap_or(true)
}

pub fn with_diagnostic_metadata(
    mut metadata: Vec<(String, String)>,
    source_job_id: u64,
    include_server_hostnames: bool,
) -> Vec<(String, String)> {
    metadata.retain(|(key, _)| {
        key != DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY
            && key != DIAGNOSTIC_INCLUDE_SERVER_HOSTNAMES_ATTRIBUTE_KEY
    });
    metadata.push((
        DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY.to_string(),
        source_job_id.to_string(),
    ));
    metadata.push((
        DIAGNOSTIC_INCLUDE_SERVER_HOSTNAMES_ATTRIBUTE_KEY.to_string(),
        include_server_hostnames.to_string(),
    ));
    metadata
}

pub fn diagnostic_cleanup_cutoff_ms(now_epoch_ms: i64) -> i64 {
    now_epoch_ms - (24 * 60 * 60 * 1000)
}

impl Database {
    pub fn insert_diagnostic_run(
        &self,
        row: &DiagnosticRunRow,
    ) -> Result<(), DiagnosticRunInsertError> {
        let datastore = self.datastore();
        let row = row.clone();
        self.run_sql_blocking(async move {
            let source_exists = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 FROM job_history WHERE job_id = {} LIMIT 1",
                &[SqlArg::I64(row.source_job_id as i64)],
            )
            .await?
            .is_some();
            if !source_exists {
                return Ok(Err(DiagnosticRunInsertError::MissingSourceJob(
                    row.source_job_id,
                )));
            }

            let existing = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                DIAGNOSTIC_RUN_SELECT_BY_SOURCE,
                &[SqlArg::I64(row.source_job_id as i64)],
            )
            .await?
            .map(diagnostic_run_from_row)
            .transpose()?;
            if existing
                .as_ref()
                .is_some_and(|current| current.stage.is_active())
            {
                return Ok(Err(DiagnosticRunInsertError::ActiveRunExists(
                    row.source_job_id,
                )));
            }

            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO diagnostic_runs
                 (source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
                  rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                 ON CONFLICT(source_job_id) DO UPDATE SET
                   diagnostic_job_id = excluded.diagnostic_job_id,
                   smg_diagnostic_id = excluded.smg_diagnostic_id,
                   stage = excluded.stage,
                   include_server_hostnames = excluded.include_server_hostnames,
                   rerun_succeeded = excluded.rerun_succeeded,
                   error_message = excluded.error_message,
                   created_at_epoch_ms = excluded.created_at_epoch_ms,
                   updated_at_epoch_ms = excluded.updated_at_epoch_ms,
                   last_activity_at_epoch_ms = excluded.last_activity_at_epoch_ms",
                &diagnostic_args(&row),
            )
            .await?;
            Ok(Ok(()))
        })?
    }

    pub fn get_diagnostic_run_for_source(
        &self,
        source_job_id: u64,
    ) -> Result<Option<DiagnosticRunRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                DIAGNOSTIC_RUN_SELECT_BY_SOURCE,
                &[SqlArg::I64(source_job_id as i64)],
            )
            .await?
            .map(diagnostic_run_from_row)
            .transpose()
        })
    }

    pub fn get_diagnostic_run_by_job(
        &self,
        diagnostic_job_id: u64,
    ) -> Result<Option<DiagnosticRunRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                DIAGNOSTIC_RUN_SELECT_BY_JOB,
                &[SqlArg::I64(diagnostic_job_id as i64)],
            )
            .await?
            .map(diagnostic_run_from_row)
            .transpose()
        })
    }

    pub fn list_pending_diagnostic_runs_for_sources(
        &self,
        source_job_ids: &[u64],
    ) -> Result<Vec<DiagnosticRunRow>, StateError> {
        let mut seen = HashSet::with_capacity(source_job_ids.len());
        let mut ids = Vec::with_capacity(source_job_ids.len());
        for &id in source_job_ids {
            if seen.insert(id) {
                ids.push(id);
            }
        }
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let mut result = Vec::new();
            for chunk in ids.chunks(900) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let sql = format!(
                    "SELECT source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
                            rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms
                       FROM diagnostic_runs
                      WHERE source_job_id IN ({placeholders})
                        AND stage IN ('queued', 'running', 'collecting', 'uploading')
                      ORDER BY created_at_epoch_ms ASC"
                );
                let args = chunk
                    .iter()
                    .map(|id| SqlArg::I64(*id as i64))
                    .collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                result.extend(
                    rows.into_iter()
                        .map(diagnostic_run_from_row)
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            Ok(result)
        })
    }

    pub fn list_pending_diagnostic_runs(&self) -> Result<Vec<DiagnosticRunRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
                        rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms
                 FROM diagnostic_runs
                 WHERE stage IN ('queued', 'running', 'collecting', 'uploading')
                 ORDER BY created_at_epoch_ms ASC",
                &[],
            )
            .await?;
            rows.into_iter().map(diagnostic_run_from_row).collect()
        })
    }

    pub fn list_stale_diagnostic_runs(
        &self,
        cutoff_epoch_ms: i64,
    ) -> Result<Vec<DiagnosticRunRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
                        rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms
                 FROM diagnostic_runs
                 WHERE last_activity_at_epoch_ms < {}",
                &[SqlArg::I64(cutoff_epoch_ms)],
            )
            .await?;
            rows.into_iter().map(diagnostic_run_from_row).collect()
        })
    }

    pub fn update_diagnostic_run(&self, row: &DiagnosticRunRow) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let row = row.clone();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE diagnostic_runs
                 SET diagnostic_job_id = {},
                     smg_diagnostic_id = {},
                     stage = {},
                     include_server_hostnames = {},
                     rerun_succeeded = {},
                     error_message = {},
                     updated_at_epoch_ms = {},
                     last_activity_at_epoch_ms = {}
                 WHERE source_job_id = {}",
                &[
                    SqlArg::I64(row.diagnostic_job_id as i64),
                    SqlArg::OptText(row.diagnostic_id),
                    SqlArg::Text(row.stage.as_str().to_string()),
                    SqlArg::Bool(row.include_server_hostnames),
                    SqlArg::OptBool(row.rerun_succeeded),
                    SqlArg::OptText(row.error_message),
                    SqlArg::I64(row.updated_at_epoch_ms),
                    SqlArg::I64(row.last_activity_at_epoch_ms),
                    SqlArg::I64(row.source_job_id as i64),
                ],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    pub fn touch_diagnostic_run(
        &self,
        source_job_id: u64,
        updated_at_epoch_ms: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE diagnostic_runs
                 SET updated_at_epoch_ms = {},
                     last_activity_at_epoch_ms = {}
                 WHERE source_job_id = {}",
                &[
                    SqlArg::I64(updated_at_epoch_ms),
                    SqlArg::I64(updated_at_epoch_ms),
                    SqlArg::I64(source_job_id as i64),
                ],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    pub fn delete_diagnostic_run(&self, source_job_id: u64) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM diagnostic_runs WHERE source_job_id = {}",
                &[SqlArg::I64(source_job_id as i64)],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    pub fn persist_job_history_diagnostic_receipt(
        &self,
        source_job_id: u64,
        diagnostic_id: &str,
        uploaded_at_epoch_ms: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let diagnostic_id = diagnostic_id.to_string();
        let result = self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE job_history
                 SET last_diagnostic_id = {},
                     last_diagnostic_uploaded_at_epoch_ms = {}
                 WHERE job_id = {}",
                &[
                    SqlArg::Text(diagnostic_id),
                    SqlArg::I64(uploaded_at_epoch_ms),
                    SqlArg::I64(source_job_id as i64),
                ],
            )
            .await?;
            Ok(changed > 0)
        });
        if result.as_ref().is_ok_and(|changed| *changed) {
            self.invalidate_job_history_cache(source_job_id);
        }
        result
    }
}

const DIAGNOSTIC_RUN_SELECT_BY_SOURCE: &str = "SELECT source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
        rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms
 FROM diagnostic_runs
 WHERE source_job_id = {}";

const DIAGNOSTIC_RUN_SELECT_BY_JOB: &str = "SELECT source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
        rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms
 FROM diagnostic_runs
 WHERE diagnostic_job_id = {}";

fn diagnostic_args(row: &DiagnosticRunRow) -> Vec<SqlArg> {
    vec![
        SqlArg::I64(row.source_job_id as i64),
        SqlArg::I64(row.diagnostic_job_id as i64),
        SqlArg::OptText(row.diagnostic_id.clone()),
        SqlArg::Text(row.stage.as_str().to_string()),
        SqlArg::Bool(row.include_server_hostnames),
        SqlArg::OptBool(row.rerun_succeeded),
        SqlArg::OptText(row.error_message.clone()),
        SqlArg::I64(row.created_at_epoch_ms),
        SqlArg::I64(row.updated_at_epoch_ms),
        SqlArg::I64(row.last_activity_at_epoch_ms),
    ]
}

fn diagnostic_run_from_row(row: SqlRow) -> Result<DiagnosticRunRow, StateError> {
    Ok(DiagnosticRunRow {
        source_job_id: row.i64("source_job_id")? as u64,
        diagnostic_job_id: row.i64("diagnostic_job_id")? as u64,
        diagnostic_id: row.opt_text("smg_diagnostic_id")?,
        stage: DiagnosticRunStage::parse(&row.text("stage")?)?,
        include_server_hostnames: row.bool("include_server_hostnames")?,
        rerun_succeeded: row.opt_bool("rerun_succeeded")?,
        error_message: row.opt_text("error_message")?,
        created_at_epoch_ms: row.i64("created_at_epoch_ms")?,
        updated_at_epoch_ms: row.i64("updated_at_epoch_ms")?,
        last_activity_at_epoch_ms: row.i64("last_activity_at_epoch_ms")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobHistoryRow;

    fn history_row(job_id: u64) -> JobHistoryRow {
        JobHistoryRow {
            job_id,
            job_hash: None,
            name: format!("job-{job_id}.nzb"),
            status: "failed".to_string(),
            error_message: Some("simulated".to_string()),
            total_bytes: 1000,
            downloaded_bytes: 900,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 100,
            health: 900,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1000 + job_id as i64,
            completed_at: 2000 + job_id as i64,
            metadata: None,
            last_diagnostic_id: None,
            last_diagnostic_uploaded_at_epoch_ms: None,
        }
    }

    fn diagnostic_row(
        source_job_id: u64,
        diagnostic_job_id: u64,
        stage: DiagnosticRunStage,
    ) -> DiagnosticRunRow {
        DiagnosticRunRow {
            source_job_id,
            diagnostic_job_id,
            diagnostic_id: None,
            stage,
            include_server_hostnames: true,
            rerun_succeeded: None,
            error_message: None,
            created_at_epoch_ms: 10_000 + source_job_id as i64,
            updated_at_epoch_ms: 10_000 + source_job_id as i64,
            last_activity_at_epoch_ms: 10_000 + source_job_id as i64,
        }
    }

    #[test]
    fn list_pending_diagnostic_runs_for_sources_filters_requested_active_rows() {
        let db = Database::open_in_memory().unwrap();
        for job_id in 1..=3 {
            db.insert_job_history(&history_row(job_id)).unwrap();
        }
        db.insert_diagnostic_run(&diagnostic_row(1, 101, DiagnosticRunStage::Queued))
            .unwrap();
        db.insert_diagnostic_run(&diagnostic_row(2, 102, DiagnosticRunStage::Complete))
            .unwrap();
        db.insert_diagnostic_run(&diagnostic_row(3, 103, DiagnosticRunStage::Running))
            .unwrap();

        let rows = db
            .list_pending_diagnostic_runs_for_sources(&[3, 2, 3, 99])
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_job_id, 3);
        assert_eq!(rows[0].diagnostic_job_id, 103);
    }

    #[test]
    fn list_pending_diagnostic_runs_for_sources_returns_empty_for_empty_input() {
        let db = Database::open_in_memory().unwrap();

        assert!(
            db.list_pending_diagnostic_runs_for_sources(&[])
                .unwrap()
                .is_empty()
        );
    }
}
