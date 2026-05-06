use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::{Database, StateError};

const HISTORY_DELETE_KIND: &str = "history_delete";
const HISTORY_JOB_TARGET_KIND: &str = "history_job";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AsyncOperationState {
    Queued,
    Running,
    Completed,
    CompletedWithErrors,
}

impl AsyncOperationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::CompletedWithErrors => "completed_with_errors",
        }
    }

    fn parse(value: &str) -> Result<Self, StateError> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "completed_with_errors" => Ok(Self::CompletedWithErrors),
            other => Err(StateError::Database(format!(
                "invalid async operation state `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AsyncOperationTargetState {
    Queued,
    Running,
    Completed,
    Failed,
}

impl AsyncOperationTargetState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self, StateError> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(StateError::Database(format!(
                "invalid async operation target state `{other}`"
            ))),
        }
    }

    pub fn locked(self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryDeleteOperationRow {
    pub id: u64,
    pub state: AsyncOperationState,
    pub delete_files: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryDeleteTargetWork {
    pub operation_id: u64,
    pub target_id: u64,
    pub state: AsyncOperationTargetState,
    pub delete_files: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryDeleteRowState {
    pub operation_id: u64,
    pub state: AsyncOperationTargetState,
    pub locked: bool,
    pub delete_files: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryDeleteOperationSummary {
    pub id: u64,
    pub state: AsyncOperationState,
    pub delete_files: bool,
    pub total_targets: u32,
    pub queued_targets: u32,
    pub running_targets: u32,
    pub completed_targets: u32,
    pub failed_targets: u32,
    pub requested_at_epoch_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryDeleteOperationPayload {
    pub delete_files: bool,
}

#[derive(Debug)]
pub enum HistoryDeleteOperationInsertError {
    EmptyTargets,
    MissingRows,
    LockedTargets,
    NoHistoryRows,
    State(StateError),
}

impl std::fmt::Display for HistoryDeleteOperationInsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyTargets => {
                write!(f, "cannot create history delete operation with no targets")
            }
            Self::MissingRows => write!(f, "one or more history rows no longer exist"),
            Self::LockedTargets => write!(f, "one or more history rows are already deleting"),
            Self::NoHistoryRows => write!(f, "there are no history rows to delete"),
            Self::State(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for HistoryDeleteOperationInsertError {}

impl From<StateError> for HistoryDeleteOperationInsertError {
    fn from(error: StateError) -> Self {
        Self::State(error)
    }
}

fn db_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

fn epoch_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn dedupe_ids(ids: &[u64]) -> Vec<u64> {
    let mut seen = HashSet::with_capacity(ids.len());
    let mut ordered = Vec::with_capacity(ids.len());
    for &id in ids {
        if seen.insert(id) {
            ordered.push(id);
        }
    }
    ordered
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(",")
}

fn count_existing_history_rows(tx: &Transaction<'_>, ids: &[u64]) -> Result<usize, StateError> {
    let sql = format!(
        "SELECT COUNT(*) FROM job_history WHERE job_id IN ({})",
        placeholders(ids.len())
    );
    let mut stmt = tx.prepare(&sql).map_err(db_err)?;
    let values = ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
    stmt.query_row(rusqlite::params_from_iter(values), |row| {
        row.get::<_, i64>(0)
    })
    .map(|count| count as usize)
    .map_err(db_err)
}

fn count_locked_history_delete_targets(
    tx: &Transaction<'_>,
    ids: &[u64],
) -> Result<usize, StateError> {
    let sql = format!(
        "SELECT COUNT(DISTINCT target_id)
         FROM async_operation_targets
         WHERE target_kind = ?1
           AND state IN ('queued', 'running')
           AND target_id IN ({})",
        placeholders(ids.len())
    );
    let mut stmt = tx.prepare(&sql).map_err(db_err)?;
    let mut values: Vec<rusqlite::types::Value> = Vec::with_capacity(ids.len() + 1);
    values.push(rusqlite::types::Value::Text(
        HISTORY_JOB_TARGET_KIND.to_string(),
    ));
    values.extend(ids.iter().map(|id| (*id as i64).into()));
    stmt.query_row(rusqlite::params_from_iter(values), |row| {
        row.get::<_, i64>(0)
    })
    .map(|count| count as usize)
    .map_err(db_err)
}

fn list_all_history_job_ids_tx(tx: &Transaction<'_>) -> Result<Vec<u64>, StateError> {
    let mut stmt = tx
        .prepare(
            "SELECT job_id
             FROM job_history
             ORDER BY completed_at DESC, job_id DESC",
        )
        .map_err(db_err)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(db_err)?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(db_err)? as u64);
    }
    Ok(out)
}

fn insert_history_delete_operation_tx(
    tx: &Transaction<'_>,
    ids: &[u64],
    delete_files: bool,
) -> Result<u64, StateError> {
    let payload_json =
        serde_json::to_string(&HistoryDeleteOperationPayload { delete_files }).map_err(db_err)?;
    let now = epoch_ms_now();

    tx.execute(
        "INSERT INTO async_operations
         (kind, state, payload_json, requested_at, started_at, finished_at)
         VALUES (?1, ?2, ?3, ?4, NULL, NULL)",
        params![
            HISTORY_DELETE_KIND,
            AsyncOperationState::Queued.as_str(),
            payload_json,
            now,
        ],
    )
    .map_err(db_err)?;
    let operation_id = tx.last_insert_rowid() as u64;

    let mut stmt = tx
        .prepare(
            "INSERT INTO async_operation_targets
             (operation_id, target_kind, target_id, state, error_message, sort_order)
             VALUES (?1, ?2, ?3, ?4, NULL, ?5)",
        )
        .map_err(db_err)?;
    for (index, id) in ids.iter().enumerate() {
        stmt.execute(params![
            operation_id as i64,
            HISTORY_JOB_TARGET_KIND,
            *id as i64,
            AsyncOperationTargetState::Queued.as_str(),
            index as i64,
        ])
        .map_err(db_err)?;
    }
    drop(stmt);

    Ok(operation_id)
}

impl Database {
    pub fn list_history_delete_locked_target_ids(
        &self,
        ids: &[u64],
    ) -> Result<HashSet<u64>, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(HashSet::new());
        }

        let conn = self.conn();
        let sql = format!(
            "SELECT DISTINCT target_id
             FROM async_operation_targets
             WHERE target_kind = ?1
               AND state IN ('queued', 'running')
               AND target_id IN ({})",
            placeholders(ids.len())
        );
        let mut stmt = conn.prepare(&sql).map_err(db_err)?;
        let mut values: Vec<rusqlite::types::Value> = Vec::with_capacity(ids.len() + 1);
        values.push(rusqlite::types::Value::Text(
            HISTORY_JOB_TARGET_KIND.to_string(),
        ));
        values.extend(ids.iter().map(|id| (*id as i64).into()));
        let rows = stmt
            .query_map(rusqlite::params_from_iter(values), |row| {
                row.get::<_, i64>(0)
            })
            .map_err(db_err)?;
        let mut out = HashSet::new();
        for row in rows {
            out.insert(row.map_err(db_err)? as u64);
        }
        Ok(out)
    }

    pub fn has_any_locked_history_delete_targets(&self) -> Result<bool, StateError> {
        let conn = self.conn();
        let exists = conn
            .query_row(
                "SELECT 1
                 FROM async_operation_targets
                 WHERE target_kind = ?1
                   AND state IN ('queued', 'running')
                 LIMIT 1",
                [HISTORY_JOB_TARGET_KIND],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(db_err)?
            .is_some();
        Ok(exists)
    }

    pub fn list_history_job_ids(&self, ids: &[u64]) -> Result<BTreeSet<u64>, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(BTreeSet::new());
        }

        let conn = self.conn();
        let sql = format!(
            "SELECT job_id FROM job_history WHERE job_id IN ({})",
            placeholders(ids.len())
        );
        let mut stmt = conn.prepare(&sql).map_err(db_err)?;
        let values = ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
        let rows = stmt
            .query_map(rusqlite::params_from_iter(values), |row| {
                row.get::<_, i64>(0)
            })
            .map_err(db_err)?;
        let mut out = BTreeSet::new();
        for row in rows {
            out.insert(row.map_err(db_err)? as u64);
        }
        Ok(out)
    }

    pub fn list_all_history_job_ids(&self) -> Result<Vec<u64>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT job_id
                 FROM job_history
                 ORDER BY completed_at DESC, job_id DESC",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(db_err)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(db_err)? as u64);
        }
        Ok(out)
    }

    pub fn insert_history_delete_operation(
        &self,
        ids: &[u64],
        delete_files: bool,
    ) -> Result<u64, HistoryDeleteOperationInsertError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Err(HistoryDeleteOperationInsertError::EmptyTargets);
        }

        let mut conn = self.conn();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_err)
            .map_err(HistoryDeleteOperationInsertError::from)?;
        let existing_count = count_existing_history_rows(&tx, &ids)
            .map_err(HistoryDeleteOperationInsertError::from)?;
        if existing_count != ids.len() {
            return Err(HistoryDeleteOperationInsertError::MissingRows);
        }
        let locked_count = count_locked_history_delete_targets(&tx, &ids)
            .map_err(HistoryDeleteOperationInsertError::from)?;
        if locked_count > 0 {
            return Err(HistoryDeleteOperationInsertError::LockedTargets);
        }

        let operation_id = insert_history_delete_operation_tx(&tx, &ids, delete_files)
            .map_err(HistoryDeleteOperationInsertError::from)?;
        tx.commit()
            .map_err(db_err)
            .map_err(HistoryDeleteOperationInsertError::from)?;
        Ok(operation_id)
    }

    pub fn insert_all_history_delete_operation(
        &self,
        delete_files: bool,
    ) -> Result<(u64, Vec<u64>), HistoryDeleteOperationInsertError> {
        let mut conn = self.conn();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_err)
            .map_err(HistoryDeleteOperationInsertError::from)?;
        let ids =
            list_all_history_job_ids_tx(&tx).map_err(HistoryDeleteOperationInsertError::from)?;
        if ids.is_empty() {
            return Err(HistoryDeleteOperationInsertError::NoHistoryRows);
        }

        let has_locked = tx
            .query_row(
                "SELECT 1
                 FROM async_operation_targets
                 WHERE target_kind = ?1
                   AND state IN ('queued', 'running')
                 LIMIT 1",
                [HISTORY_JOB_TARGET_KIND],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(db_err)
            .map_err(HistoryDeleteOperationInsertError::from)?
            .is_some();
        if has_locked {
            return Err(HistoryDeleteOperationInsertError::LockedTargets);
        }

        let operation_id = insert_history_delete_operation_tx(&tx, &ids, delete_files)
            .map_err(HistoryDeleteOperationInsertError::from)?;
        tx.commit()
            .map_err(db_err)
            .map_err(HistoryDeleteOperationInsertError::from)?;
        Ok((operation_id, ids))
    }

    pub fn recover_running_history_delete_operations(&self) -> Result<(), StateError> {
        let mut conn = self.conn();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_err)?;
        tx.execute(
            "UPDATE async_operation_targets
             SET state = 'queued',
                 error_message = NULL
             WHERE target_kind = ?1
               AND state = 'running'
               AND operation_id IN (
                 SELECT id
                 FROM async_operations
                 WHERE kind = ?2
                   AND state = 'running'
               )",
            params![HISTORY_JOB_TARGET_KIND, HISTORY_DELETE_KIND],
        )
        .map_err(db_err)?;
        tx.execute(
            "UPDATE async_operations
             SET state = 'queued',
                 finished_at = NULL
             WHERE kind = ?1
               AND state = 'running'",
            [HISTORY_DELETE_KIND],
        )
        .map_err(db_err)?;
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn requeue_history_delete_operation(&self, operation_id: u64) -> Result<(), StateError> {
        let mut conn = self.conn();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_err)?;
        tx.execute(
            "UPDATE async_operation_targets
             SET state = 'queued',
                 error_message = NULL
             WHERE operation_id = ?1
               AND target_kind = ?2
               AND state = 'running'",
            params![operation_id as i64, HISTORY_JOB_TARGET_KIND],
        )
        .map_err(db_err)?;
        tx.execute(
            "UPDATE async_operations
             SET state = 'queued',
                 finished_at = NULL
             WHERE id = ?1
               AND kind = ?2
               AND state = 'running'",
            params![operation_id as i64, HISTORY_DELETE_KIND],
        )
        .map_err(db_err)?;
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn next_history_delete_operation(
        &self,
    ) -> Result<Option<HistoryDeleteOperationRow>, StateError> {
        let mut conn = self.conn();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_err)?;
        let row = tx
            .query_row(
                "SELECT id, payload_json
                 FROM async_operations
                 WHERE kind = ?1
                   AND state = 'queued'
                 ORDER BY requested_at ASC, id ASC
                 LIMIT 1",
                [HISTORY_DELETE_KIND],
                |row| {
                    let id = row.get::<_, i64>(0)? as u64;
                    let payload = serde_json::from_str::<HistoryDeleteOperationPayload>(
                        &row.get::<_, String>(1)?,
                    )
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                    Ok((id, payload.delete_files))
                },
            )
            .optional()
            .map_err(db_err)?;
        let Some((operation_id, delete_files)) = row else {
            tx.commit().map_err(db_err)?;
            return Ok(None);
        };

        let updated = tx
            .execute(
                "UPDATE async_operations
                 SET state = ?1,
                     started_at = COALESCE(started_at, ?2),
                     finished_at = NULL
                 WHERE id = ?3
                   AND state = 'queued'",
                params![
                    AsyncOperationState::Running.as_str(),
                    epoch_ms_now(),
                    operation_id as i64,
                ],
            )
            .map_err(db_err)?;
        if updated != 1 {
            tx.commit().map_err(db_err)?;
            return Ok(None);
        }

        tx.commit().map_err(db_err)?;
        Ok(Some(HistoryDeleteOperationRow {
            id: operation_id,
            state: AsyncOperationState::Running,
            delete_files,
        }))
    }

    pub fn list_history_delete_operation_targets(
        &self,
        operation_id: u64,
        delete_files: bool,
    ) -> Result<Vec<HistoryDeleteTargetWork>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT target_id, state
                 FROM async_operation_targets
                 WHERE operation_id = ?1
                   AND target_kind = ?2
                   AND state IN ('queued', 'running')
                 ORDER BY sort_order ASC, target_id ASC",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map(
                params![operation_id as i64, HISTORY_JOB_TARGET_KIND],
                |row| {
                    let target_id = row.get::<_, i64>(0)? as u64;
                    let state = AsyncOperationTargetState::parse(&row.get::<_, String>(1)?)
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(std::io::Error::other(error.to_string())),
                            )
                        })?;
                    Ok(HistoryDeleteTargetWork {
                        operation_id,
                        target_id,
                        state,
                        delete_files,
                    })
                },
            )
            .map_err(db_err)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(db_err)?);
        }
        Ok(out)
    }

    pub fn mark_history_delete_target_state(
        &self,
        operation_id: u64,
        target_id: u64,
        state: AsyncOperationTargetState,
        error_message: Option<&str>,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE async_operation_targets
             SET state = ?1,
                 error_message = ?2
             WHERE operation_id = ?3
               AND target_kind = ?4
               AND target_id = ?5",
            params![
                state.as_str(),
                error_message,
                operation_id as i64,
                HISTORY_JOB_TARGET_KIND,
                target_id as i64,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn finalize_history_delete_operation(
        &self,
        operation_id: u64,
    ) -> Result<AsyncOperationState, StateError> {
        let conn = self.conn();
        let failed_count: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM async_operation_targets
                 WHERE operation_id = ?1
                   AND target_kind = ?2
                   AND state = 'failed'",
                params![operation_id as i64, HISTORY_JOB_TARGET_KIND],
                |row| row.get(0),
            )
            .map_err(db_err)?;
        let next_state = if failed_count > 0 {
            AsyncOperationState::CompletedWithErrors
        } else {
            AsyncOperationState::Completed
        };
        conn.execute(
            "UPDATE async_operations
             SET state = ?1,
                 finished_at = ?2
             WHERE id = ?3",
            params![next_state.as_str(), epoch_ms_now(), operation_id as i64],
        )
        .map_err(db_err)?;
        Ok(next_state)
    }

    pub fn list_history_delete_row_states(
        &self,
        ids: &[u64],
    ) -> Result<HashMap<u64, HistoryDeleteRowState>, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let conn = self.conn();
        let sql = format!(
            "SELECT t.target_id, t.operation_id, t.state, t.error_message, o.payload_json
             FROM async_operation_targets t
             INNER JOIN async_operations o ON o.id = t.operation_id
             WHERE o.kind = ?1
               AND t.target_kind = ?2
               AND t.state != 'completed'
               AND t.target_id IN ({})
               AND t.operation_id = (
                 SELECT t2.operation_id
                 FROM async_operation_targets t2
                 INNER JOIN async_operations o2 ON o2.id = t2.operation_id
                 WHERE o2.kind = ?1
                   AND t2.target_kind = ?2
                   AND t2.target_id = t.target_id
                   AND t2.state != 'completed'
                 ORDER BY t2.operation_id DESC
                 LIMIT 1
               )",
            placeholders(ids.len())
        );
        let mut stmt = conn.prepare(&sql).map_err(db_err)?;
        let mut params: Vec<rusqlite::types::Value> = Vec::with_capacity(ids.len() + 2);
        params.push(rusqlite::types::Value::Text(
            HISTORY_DELETE_KIND.to_string(),
        ));
        params.push(rusqlite::types::Value::Text(
            HISTORY_JOB_TARGET_KIND.to_string(),
        ));
        params.extend(ids.iter().map(|id| (*id as i64).into()));
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params), |row| {
                let target_id = row.get::<_, i64>(0)? as u64;
                let operation_id = row.get::<_, i64>(1)? as u64;
                let state = AsyncOperationTargetState::parse(&row.get::<_, String>(2)?).map_err(
                    |error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(std::io::Error::other(error.to_string())),
                        )
                    },
                )?;
                let error_message = row.get::<_, Option<String>>(3)?;
                let payload = serde_json::from_str::<HistoryDeleteOperationPayload>(
                    &row.get::<_, String>(4)?,
                )
                .map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
                Ok((
                    target_id,
                    HistoryDeleteRowState {
                        operation_id,
                        state,
                        locked: state.locked(),
                        delete_files: payload.delete_files,
                        error_message,
                    },
                ))
            })
            .map_err(db_err)?;
        let mut out = HashMap::new();
        for row in rows {
            let (target_id, state) = row.map_err(db_err)?;
            out.insert(target_id, state);
        }
        Ok(out)
    }

    pub fn list_history_delete_operations(
        &self,
        active_only: bool,
    ) -> Result<Vec<HistoryDeleteOperationSummary>, StateError> {
        let conn = self.conn();
        let mut sql = String::from(
            "SELECT o.id,
                    o.state,
                    o.payload_json,
                    o.requested_at,
                    COUNT(t.target_id) AS total_targets,
                    SUM(CASE WHEN t.state = 'queued' THEN 1 ELSE 0 END) AS queued_targets,
                    SUM(CASE WHEN t.state = 'running' THEN 1 ELSE 0 END) AS running_targets,
                    SUM(CASE WHEN t.state = 'completed' THEN 1 ELSE 0 END) AS completed_targets,
                    SUM(CASE WHEN t.state = 'failed' THEN 1 ELSE 0 END) AS failed_targets
             FROM async_operations o
             LEFT JOIN async_operation_targets t
               ON t.operation_id = o.id
              AND t.target_kind = ?1
             WHERE o.kind = ?2",
        );
        if active_only {
            sql.push_str(" AND o.state IN ('queued', 'running')");
        }
        sql.push_str(
            " GROUP BY o.id, o.state, o.payload_json, o.requested_at
              ORDER BY o.requested_at ASC, o.id ASC",
        );
        let mut stmt = conn.prepare(&sql).map_err(db_err)?;
        let rows = stmt
            .query_map(
                params![HISTORY_JOB_TARGET_KIND, HISTORY_DELETE_KIND],
                |row| {
                    let id = row.get::<_, i64>(0)? as u64;
                    let state =
                        AsyncOperationState::parse(&row.get::<_, String>(1)?).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(std::io::Error::other(error.to_string())),
                            )
                        })?;
                    let payload = serde_json::from_str::<HistoryDeleteOperationPayload>(
                        &row.get::<_, String>(2)?,
                    )
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                    Ok(HistoryDeleteOperationSummary {
                        id,
                        state,
                        delete_files: payload.delete_files,
                        requested_at_epoch_ms: row.get::<_, i64>(3)?,
                        total_targets: row.get::<_, i64>(4)? as u32,
                        queued_targets: row.get::<_, i64>(5)? as u32,
                        running_targets: row.get::<_, i64>(6)? as u32,
                        completed_targets: row.get::<_, i64>(7)? as u32,
                        failed_targets: row.get::<_, i64>(8)? as u32,
                    })
                },
            )
            .map_err(db_err)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(db_err)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobHistoryRow;

    fn history(job_id: u64, completed_at: i64) -> JobHistoryRow {
        JobHistoryRow {
            job_id,
            name: format!("job-{job_id}"),
            status: "complete".into(),
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
            created_at: completed_at - 10,
            completed_at,
            metadata: None,
            last_diagnostic_id: None,
            last_diagnostic_uploaded_at_epoch_ms: None,
        }
    }

    #[test]
    fn insert_history_delete_operation_creates_targets_and_locked_states() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        db.insert_job_history(&history(11, 101)).unwrap();

        let operation_id = db.insert_history_delete_operation(&[10, 11], true).unwrap();
        let states = db.list_history_delete_row_states(&[10, 11]).unwrap();
        assert_eq!(states.len(), 2);
        assert_eq!(states[&10].operation_id, operation_id);
        assert!(states[&10].locked);
        assert!(states[&11].delete_files);

        let summaries = db.list_history_delete_operations(true).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].total_targets, 2);
        assert_eq!(summaries[0].queued_targets, 2);
    }

    #[test]
    fn finalize_history_delete_operation_keeps_failed_rows_visible() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        db.insert_job_history(&history(11, 101)).unwrap();
        let operation_id = db
            .insert_history_delete_operation(&[10, 11], false)
            .unwrap();

        db.mark_history_delete_target_state(
            operation_id,
            10,
            AsyncOperationTargetState::Completed,
            None,
        )
        .unwrap();
        db.mark_history_delete_target_state(
            operation_id,
            11,
            AsyncOperationTargetState::Failed,
            Some("disk busy"),
        )
        .unwrap();

        let final_state = db.finalize_history_delete_operation(operation_id).unwrap();
        assert_eq!(final_state, AsyncOperationState::CompletedWithErrors);

        let states = db.list_history_delete_row_states(&[10, 11]).unwrap();
        assert!(!states.contains_key(&10));
        assert_eq!(states[&11].state, AsyncOperationTargetState::Failed);
        assert!(!states[&11].locked);
        assert_eq!(states[&11].error_message.as_deref(), Some("disk busy"));
    }

    #[test]
    fn locked_target_lookup_ignores_failed_operations() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        let operation_id = db.insert_history_delete_operation(&[10], false).unwrap();
        db.mark_history_delete_target_state(
            operation_id,
            10,
            AsyncOperationTargetState::Failed,
            Some("oops"),
        )
        .unwrap();
        db.finalize_history_delete_operation(operation_id).unwrap();

        let locked = db.list_history_delete_locked_target_ids(&[10]).unwrap();
        assert!(locked.is_empty());
    }

    #[test]
    fn insert_history_delete_operation_rejects_locked_targets() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        db.insert_history_delete_operation(&[10], false).unwrap();

        let error = db.insert_history_delete_operation(&[10], true).unwrap_err();
        assert!(matches!(
            error,
            HistoryDeleteOperationInsertError::LockedTargets
        ));
    }

    #[test]
    fn next_history_delete_operation_claims_only_once_until_recovered() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        let operation_id = db.insert_history_delete_operation(&[10], false).unwrap();

        let claimed = db.next_history_delete_operation().unwrap().unwrap();
        assert_eq!(claimed.id, operation_id);
        assert_eq!(claimed.state, AsyncOperationState::Running);
        assert!(db.next_history_delete_operation().unwrap().is_none());

        db.mark_history_delete_target_state(
            operation_id,
            10,
            AsyncOperationTargetState::Running,
            None,
        )
        .unwrap();
        db.recover_running_history_delete_operations().unwrap();

        let recovered = db.next_history_delete_operation().unwrap().unwrap();
        assert_eq!(recovered.id, operation_id);
        let states = db.list_history_delete_row_states(&[10]).unwrap();
        assert_eq!(states[&10].state, AsyncOperationTargetState::Queued);
    }

    #[test]
    fn requeue_history_delete_operation_retries_only_running_targets() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        db.insert_job_history(&history(11, 100)).unwrap();
        let operation_id = db
            .insert_history_delete_operation(&[10, 11], false)
            .unwrap();

        let claimed = db.next_history_delete_operation().unwrap().unwrap();
        assert_eq!(claimed.id, operation_id);
        db.mark_history_delete_target_state(
            operation_id,
            10,
            AsyncOperationTargetState::Completed,
            None,
        )
        .unwrap();
        db.mark_history_delete_target_state(
            operation_id,
            11,
            AsyncOperationTargetState::Running,
            None,
        )
        .unwrap();

        db.requeue_history_delete_operation(operation_id).unwrap();

        let reclaimed = db.next_history_delete_operation().unwrap().unwrap();
        assert_eq!(reclaimed.id, operation_id);
        let targets = db
            .list_history_delete_operation_targets(operation_id, false)
            .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].target_id, 11);
        assert_eq!(targets[0].state, AsyncOperationTargetState::Queued);
    }
}
