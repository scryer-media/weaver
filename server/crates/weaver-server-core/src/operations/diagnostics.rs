use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::{Database, StateError};

pub const DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY: &str = "__weaver_diagnostic_source_job_id";
pub const DIAGNOSTIC_INCLUDE_SERVER_HOSTNAMES_ATTRIBUTE_KEY: &str =
    "__weaver_diagnostic_include_server_hostnames";

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

fn db_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
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
        let conn = self.conn();

        let source_exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM job_history WHERE job_id = ?1)",
                [row.source_job_id as i64],
                |result| result.get(0),
            )
            .map_err(db_err)?;
        if !source_exists {
            return Err(DiagnosticRunInsertError::MissingSourceJob(
                row.source_job_id,
            ));
        }

        let existing = conn
            .query_row(
                "SELECT source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
                        rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms
                 FROM diagnostic_runs
                 WHERE source_job_id = ?1",
                [row.source_job_id as i64],
                diagnostic_run_from_row,
            )
            .optional()
            .map_err(db_err)
            .map_err(DiagnosticRunInsertError::State)?;
        if existing
            .as_ref()
            .is_some_and(|current| current.stage.is_active())
        {
            return Err(DiagnosticRunInsertError::ActiveRunExists(row.source_job_id));
        }

        conn.execute(
            "INSERT INTO diagnostic_runs
             (source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
              rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
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
            params![
                row.source_job_id as i64,
                row.diagnostic_job_id as i64,
                row.diagnostic_id,
                row.stage.as_str(),
                row.include_server_hostnames,
                row.rerun_succeeded,
                row.error_message,
                row.created_at_epoch_ms,
                row.updated_at_epoch_ms,
                row.last_activity_at_epoch_ms,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn get_diagnostic_run_for_source(
        &self,
        source_job_id: u64,
    ) -> Result<Option<DiagnosticRunRow>, StateError> {
        let conn = self.read_conn();
        conn.query_row(
            "SELECT source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
                    rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms
             FROM diagnostic_runs
             WHERE source_job_id = ?1",
            [source_job_id as i64],
            diagnostic_run_from_row,
        )
        .optional()
        .map_err(db_err)
    }

    pub fn get_diagnostic_run_by_job(
        &self,
        diagnostic_job_id: u64,
    ) -> Result<Option<DiagnosticRunRow>, StateError> {
        let conn = self.read_conn();
        conn.query_row(
            "SELECT source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
                    rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms
             FROM diagnostic_runs
             WHERE diagnostic_job_id = ?1",
            [diagnostic_job_id as i64],
            diagnostic_run_from_row,
        )
        .optional()
        .map_err(db_err)
    }

    pub fn list_pending_diagnostic_runs(&self) -> Result<Vec<DiagnosticRunRow>, StateError> {
        let conn = self.read_conn();
        let mut stmt = conn
            .prepare(
                "SELECT source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
                        rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms
                 FROM diagnostic_runs
                 WHERE stage IN ('queued', 'running', 'collecting', 'uploading')
                 ORDER BY created_at_epoch_ms ASC",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([], diagnostic_run_from_row)
            .map_err(db_err)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(db_err)?);
        }
        Ok(out)
    }

    pub fn list_stale_diagnostic_runs(
        &self,
        cutoff_epoch_ms: i64,
    ) -> Result<Vec<DiagnosticRunRow>, StateError> {
        let conn = self.read_conn();
        let mut stmt = conn
            .prepare(
                "SELECT source_job_id, diagnostic_job_id, smg_diagnostic_id, stage, include_server_hostnames,
                        rerun_succeeded, error_message, created_at_epoch_ms, updated_at_epoch_ms, last_activity_at_epoch_ms
                 FROM diagnostic_runs
                 WHERE last_activity_at_epoch_ms < ?1",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([cutoff_epoch_ms], diagnostic_run_from_row)
            .map_err(db_err)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(db_err)?);
        }
        Ok(out)
    }

    pub fn update_diagnostic_run(&self, row: &DiagnosticRunRow) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute(
                "UPDATE diagnostic_runs
                 SET diagnostic_job_id = ?2,
                     smg_diagnostic_id = ?3,
                     stage = ?4,
                     include_server_hostnames = ?5,
                     rerun_succeeded = ?6,
                     error_message = ?7,
                     updated_at_epoch_ms = ?8,
                     last_activity_at_epoch_ms = ?9
                 WHERE source_job_id = ?1",
                params![
                    row.source_job_id as i64,
                    row.diagnostic_job_id as i64,
                    row.diagnostic_id,
                    row.stage.as_str(),
                    row.include_server_hostnames,
                    row.rerun_succeeded,
                    row.error_message,
                    row.updated_at_epoch_ms,
                    row.last_activity_at_epoch_ms,
                ],
            )
            .map_err(db_err)?;
        Ok(changed > 0)
    }

    pub fn touch_diagnostic_run(
        &self,
        source_job_id: u64,
        updated_at_epoch_ms: i64,
    ) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute(
                "UPDATE diagnostic_runs
                 SET updated_at_epoch_ms = ?2,
                     last_activity_at_epoch_ms = ?2
                 WHERE source_job_id = ?1",
                params![source_job_id as i64, updated_at_epoch_ms],
            )
            .map_err(db_err)?;
        Ok(changed > 0)
    }

    pub fn delete_diagnostic_run(&self, source_job_id: u64) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute(
                "DELETE FROM diagnostic_runs WHERE source_job_id = ?1",
                [source_job_id as i64],
            )
            .map_err(db_err)?;
        Ok(changed > 0)
    }

    pub fn persist_job_history_diagnostic_receipt(
        &self,
        source_job_id: u64,
        diagnostic_id: &str,
        uploaded_at_epoch_ms: i64,
    ) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute(
                "UPDATE job_history
                 SET last_diagnostic_id = ?2,
                     last_diagnostic_uploaded_at_epoch_ms = ?3
                 WHERE job_id = ?1",
                params![source_job_id as i64, diagnostic_id, uploaded_at_epoch_ms],
            )
            .map_err(db_err)?;
        Ok(changed > 0)
    }
}

fn diagnostic_run_from_row(row: &rusqlite::Row<'_>) -> Result<DiagnosticRunRow, rusqlite::Error> {
    let stage = row.get::<_, String>(3)?;
    let rerun_succeeded = row.get::<_, Option<bool>>(5)?;
    Ok(DiagnosticRunRow {
        source_job_id: row.get::<_, i64>(0)? as u64,
        diagnostic_job_id: row.get::<_, i64>(1)? as u64,
        diagnostic_id: row.get(2)?,
        stage: DiagnosticRunStage::parse(&stage).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        include_server_hostnames: row.get(4)?,
        rerun_succeeded,
        error_message: row.get(6)?,
        created_at_epoch_ms: row.get(7)?,
        updated_at_epoch_ms: row.get(8)?,
        last_activity_at_epoch_ms: row.get(9)?,
    })
}
