use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sqlx::{Postgres, QueryBuilder, Sqlite};

use crate::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRow, SqlRuntime, SqlTx};
use crate::{Database, StateError};

const HISTORY_DELETE_KIND: &str = "history_delete";
const HISTORY_JOB_TARGET_KIND: &str = "history_job";

// Per-dialect bind budgets for chunking large `IN (…)` id lists. A delete-all
// over a very large history table can produce far more ids than sqlite's
// variable limit (999), so id lists must be split into chunks that stay under
// the budget once the query's fixed (non-id) binds are subtracted.
const SQLITE_BATCH_BIND_LIMIT: usize = 900;
const POSTGRES_BATCH_BIND_LIMIT: usize = 16_000;

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
    std::iter::repeat_n("{}", count)
        .collect::<Vec<_>>()
        .join(",")
}

fn id_args(ids: &[u64]) -> Vec<SqlArg> {
    ids.iter().map(|id| SqlArg::I64(*id as i64)).collect()
}

fn bind_budget_for_engine(engine: SqlEngine) -> usize {
    match engine {
        SqlEngine::Sqlite => SQLITE_BATCH_BIND_LIMIT,
        SqlEngine::Postgres => POSTGRES_BATCH_BIND_LIMIT,
    }
}

fn bind_budget_for_tx(tx: &SqlTx<'_>) -> usize {
    match tx {
        SqlTx::Sqlite(_) => SQLITE_BATCH_BIND_LIMIT,
        SqlTx::Postgres(_) => POSTGRES_BATCH_BIND_LIMIT,
    }
}

/// Maximum number of id binds per `IN (…)` chunk, leaving room for `fixed_binds`
/// non-id parameters that every chunk carries (e.g. a `target_kind`).
fn id_chunk_size(bind_budget: usize, fixed_binds: usize) -> usize {
    bind_budget.saturating_sub(fixed_binds).max(1)
}

fn payload_json(delete_files: bool) -> Result<String, StateError> {
    serde_json::to_string(&HistoryDeleteOperationPayload { delete_files }).map_err(db_err)
}

fn parse_payload(raw: &str) -> Result<HistoryDeleteOperationPayload, StateError> {
    serde_json::from_str(raw).map_err(db_err)
}

async fn count_existing_history_rows(tx: &mut SqlTx<'_>, ids: &[u64]) -> Result<usize, StateError> {
    let chunk_size = id_chunk_size(bind_budget_for_tx(tx), 0);
    let mut total = 0i64;
    // `ids` is deduped by the caller, so chunks are disjoint and per-chunk counts
    // sum to the exact total.
    for chunk in ids.chunks(chunk_size) {
        let sql = format!(
            "SELECT COUNT(*) AS count FROM job_history WHERE job_id IN ({})",
            placeholders(chunk.len())
        );
        let row = tx.fetch_optional(&sql, &id_args(chunk)).await?;
        total += row.map(|row| row.i64("count")).transpose()?.unwrap_or(0);
    }
    Ok(total.max(0) as usize)
}

async fn count_locked_history_delete_targets(
    tx: &mut SqlTx<'_>,
    ids: &[u64],
) -> Result<usize, StateError> {
    // One fixed bind (`target_kind`) per chunk. `ids` is deduped and chunks are
    // disjoint, so `COUNT(DISTINCT target_id)` per chunk sums to the exact total
    // (no target_id can span two chunks).
    let chunk_size = id_chunk_size(bind_budget_for_tx(tx), 1);
    let mut total = 0i64;
    for chunk in ids.chunks(chunk_size) {
        let sql = format!(
            "SELECT COUNT(DISTINCT target_id) AS count
             FROM async_operation_targets
             WHERE target_kind = {{}}
               AND state IN ('queued', 'running')
               AND target_id IN ({})",
            placeholders(chunk.len())
        );
        let mut args = vec![SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string())];
        args.extend(id_args(chunk));
        let row = tx.fetch_optional(&sql, &args).await?;
        total += row.map(|row| row.i64("count")).transpose()?.unwrap_or(0);
    }
    Ok(total.max(0) as usize)
}

async fn list_all_history_job_ids_tx(tx: &mut SqlTx<'_>) -> Result<Vec<u64>, StateError> {
    let rows = tx
        .fetch_all(
            "SELECT job_id
             FROM job_history
             ORDER BY completed_at DESC, job_id DESC",
            &[],
        )
        .await?;
    rows.into_iter()
        .map(|row| Ok(row.i64("job_id")? as u64))
        .collect()
}

async fn has_locked_history_delete_targets_tx(tx: &mut SqlTx<'_>) -> Result<bool, StateError> {
    Ok(tx
        .fetch_optional(
            "SELECT 1
             FROM async_operation_targets
             WHERE target_kind = {}
               AND state IN ('queued', 'running')
             LIMIT 1",
            &[SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string())],
        )
        .await?
        .is_some())
}

async fn insert_history_delete_operation_tx(
    tx: &mut SqlTx<'_>,
    ids: &[u64],
    delete_files: bool,
) -> Result<u64, StateError> {
    let payload_json = payload_json(delete_files)?;
    let now = epoch_ms_now();

    let row = tx
        .fetch_optional(
            "INSERT INTO async_operations
             (kind, state, payload_json, requested_at, started_at, finished_at)
             VALUES ({}, {}, {}, {}, NULL, NULL)
             RETURNING id",
            &[
                SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                SqlArg::Text(AsyncOperationState::Queued.as_str().to_string()),
                SqlArg::Text(payload_json),
                SqlArg::I64(now),
            ],
        )
        .await?
        .ok_or_else(|| StateError::Database("failed to insert async operation".to_string()))?;
    let operation_id = row.i64("id")? as u64;

    bulk_insert_history_delete_targets_tx(tx, operation_id, ids).await?;

    Ok(operation_id)
}

/// Insert one `async_operation_targets` row per id using multi-row `VALUES`
/// chunks (instead of one `INSERT` statement per id).
///
/// `sort_order` is the id's index in `ids`, preserving the exact ordering
/// semantics of the previous per-statement loop. `error_message` is always
/// `NULL` for a freshly queued target. Rows bind 6 parameters each
/// (operation_id, target_kind, target_id, state, error_message, sort_order), so
/// chunks stay under the dialect bind budget.
async fn bulk_insert_history_delete_targets_tx(
    tx: &mut SqlTx<'_>,
    operation_id: u64,
    ids: &[u64],
) -> Result<(), StateError> {
    if ids.is_empty() {
        return Ok(());
    }

    const BINDS_PER_ROW: usize = 6;
    let chunk_size = (bind_budget_for_tx(tx) / BINDS_PER_ROW).max(1);
    let state = AsyncOperationTargetState::Queued.as_str();
    let prefix = "INSERT INTO async_operation_targets \
                  (operation_id, target_kind, target_id, state, error_message, sort_order) ";

    match tx {
        SqlTx::Sqlite(tx) => {
            for (chunk_index, chunk) in ids.chunks(chunk_size).enumerate() {
                let base = chunk_index * chunk_size;
                let mut builder = QueryBuilder::<Sqlite>::new(prefix);
                builder.push_values(chunk.iter().enumerate(), |mut row, (offset, id)| {
                    row.push_bind(operation_id as i64)
                        .push_bind(HISTORY_JOB_TARGET_KIND)
                        .push_bind(*id as i64)
                        .push_bind(state)
                        .push_bind(Option::<String>::None)
                        .push_bind((base + offset) as i64);
                });
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
        SqlTx::Postgres(tx) => {
            for (chunk_index, chunk) in ids.chunks(chunk_size).enumerate() {
                let base = chunk_index * chunk_size;
                let mut builder = QueryBuilder::<Postgres>::new(prefix);
                builder.push_values(chunk.iter().enumerate(), |mut row, (offset, id)| {
                    row.push_bind(operation_id as i64)
                        .push_bind(HISTORY_JOB_TARGET_KIND)
                        .push_bind(*id as i64)
                        .push_bind(state)
                        .push_bind(Option::<String>::None)
                        .push_bind((base + offset) as i64);
                });
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
    }

    Ok(())
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

        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let chunk_size = id_chunk_size(bind_budget_for_engine(datastore.engine()), 1);
            let mut locked = HashSet::new();
            for chunk in ids.chunks(chunk_size) {
                let sql = format!(
                    "SELECT DISTINCT target_id
                     FROM async_operation_targets
                     WHERE target_kind = {{}}
                       AND state IN ('queued', 'running')
                       AND target_id IN ({})",
                    placeholders(chunk.len())
                );
                let mut args = vec![SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string())];
                args.extend(id_args(chunk));
                let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                for row in rows {
                    locked.insert(row.i64("target_id")? as u64);
                }
            }
            Ok(locked)
        })
    }

    pub fn has_any_locked_history_delete_targets(&self) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            Ok(SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1
                 FROM async_operation_targets
                 WHERE target_kind = {}
                   AND state IN ('queued', 'running')
                 LIMIT 1",
                &[SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string())],
            )
            .await?
            .is_some())
        })
    }

    pub fn list_history_job_ids(&self, ids: &[u64]) -> Result<BTreeSet<u64>, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(BTreeSet::new());
        }

        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let chunk_size = id_chunk_size(bind_budget_for_engine(datastore.engine()), 0);
            let mut present = BTreeSet::new();
            for chunk in ids.chunks(chunk_size) {
                let sql = format!(
                    "SELECT job_id FROM job_history WHERE job_id IN ({})",
                    placeholders(chunk.len())
                );
                let rows =
                    SqlRuntime::fetch_all(datastore.read_exec(), &sql, &id_args(chunk)).await?;
                for row in rows {
                    present.insert(row.i64("job_id")? as u64);
                }
            }
            Ok(present)
        })
    }

    pub fn list_all_history_job_ids(&self) -> Result<Vec<u64>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id
                 FROM job_history
                 ORDER BY completed_at DESC, job_id DESC",
                &[],
            )
            .await?;
            rows.into_iter()
                .map(|row| Ok(row.i64("job_id")? as u64))
                .collect()
        })
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

        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_history_delete_operation", |tx| {
                let ids = ids.clone();
                Box::pin(async move {
                    let existing_count = count_existing_history_rows(tx, &ids).await?;
                    if existing_count != ids.len() {
                        return Ok(Err(HistoryDeleteOperationInsertError::MissingRows));
                    }
                    let locked_count = count_locked_history_delete_targets(tx, &ids).await?;
                    if locked_count > 0 {
                        return Ok(Err(HistoryDeleteOperationInsertError::LockedTargets));
                    }

                    let operation_id =
                        insert_history_delete_operation_tx(tx, &ids, delete_files).await?;
                    Ok(Ok(operation_id))
                })
            })
            .await
        })?
    }

    pub fn insert_all_history_delete_operation(
        &self,
        delete_files: bool,
    ) -> Result<(u64, Vec<u64>), HistoryDeleteOperationInsertError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "insert_all_history_delete_operation",
                |tx| {
                    Box::pin(async move {
                        let ids = list_all_history_job_ids_tx(tx).await?;
                        if ids.is_empty() {
                            return Ok(Err(HistoryDeleteOperationInsertError::NoHistoryRows));
                        }

                        if has_locked_history_delete_targets_tx(tx).await? {
                            return Ok(Err(HistoryDeleteOperationInsertError::LockedTargets));
                        }

                        let operation_id =
                            insert_history_delete_operation_tx(tx, &ids, delete_files).await?;
                        Ok(Ok((operation_id, ids)))
                    })
                },
            )
            .await
        })?
    }

    pub fn recover_running_history_delete_operations(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "recover_running_history_delete_operations",
                |tx| {
                    Box::pin(async move {
                        tx.execute(
                            "UPDATE async_operation_targets
                         SET state = 'queued',
                             error_message = NULL
                         WHERE target_kind = {}
                           AND state = 'running'
                           AND operation_id IN (
                             SELECT id
                             FROM async_operations
                             WHERE kind = {}
                               AND state = 'running'
                           )",
                            &[
                                SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
                                SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                            ],
                        )
                        .await?;
                        tx.execute(
                            "UPDATE async_operations
                         SET state = 'queued',
                             finished_at = NULL
                         WHERE kind = {}
                           AND state = 'running'",
                            &[SqlArg::Text(HISTORY_DELETE_KIND.to_string())],
                        )
                        .await?;
                        Ok(())
                    })
                },
            )
            .await
        })
    }

    pub fn requeue_history_delete_operation(&self, operation_id: u64) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "requeue_history_delete_operation", |tx| {
                Box::pin(async move {
                    tx.execute(
                        "UPDATE async_operation_targets
                         SET state = 'queued',
                             error_message = NULL
                         WHERE operation_id = {}
                           AND target_kind = {}
                           AND state = 'running'",
                        &[
                            SqlArg::I64(operation_id as i64),
                            SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
                        ],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE async_operations
                         SET state = 'queued',
                             finished_at = NULL
                         WHERE id = {}
                           AND kind = {}
                           AND state = 'running'",
                        &[
                            SqlArg::I64(operation_id as i64),
                            SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn next_history_delete_operation(
        &self,
    ) -> Result<Option<HistoryDeleteOperationRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "next_history_delete_operation", |tx| {
                Box::pin(async move {
                    let row = tx
                        .fetch_optional(
                            "SELECT id, payload_json
                             FROM async_operations
                             WHERE kind = {}
                               AND state = 'queued'
                             ORDER BY requested_at ASC, id ASC
                             LIMIT 1",
                            &[SqlArg::Text(HISTORY_DELETE_KIND.to_string())],
                        )
                        .await?;
                    let Some(row) = row else {
                        return Ok(None);
                    };
                    let operation_id = row.i64("id")? as u64;
                    let delete_files = parse_payload(&row.text("payload_json")?)?.delete_files;

                    let updated = tx
                        .execute(
                            "UPDATE async_operations
                             SET state = {},
                                 started_at = COALESCE(started_at, {}),
                                 finished_at = NULL
                             WHERE id = {}
                               AND state = 'queued'",
                            &[
                                SqlArg::Text(AsyncOperationState::Running.as_str().to_string()),
                                SqlArg::I64(epoch_ms_now()),
                                SqlArg::I64(operation_id as i64),
                            ],
                        )
                        .await?;
                    if updated != 1 {
                        return Ok(None);
                    }

                    Ok(Some(HistoryDeleteOperationRow {
                        id: operation_id,
                        state: AsyncOperationState::Running,
                        delete_files,
                    }))
                })
            })
            .await
        })
    }

    pub fn list_history_delete_operation_targets(
        &self,
        operation_id: u64,
        delete_files: bool,
    ) -> Result<Vec<HistoryDeleteTargetWork>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT target_id, state
                 FROM async_operation_targets
                 WHERE operation_id = {}
                   AND target_kind = {}
                   AND state IN ('queued', 'running')
                 ORDER BY sort_order ASC, target_id ASC",
                &[
                    SqlArg::I64(operation_id as i64),
                    SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
                ],
            )
            .await?;
            rows.into_iter()
                .map(|row| {
                    Ok(HistoryDeleteTargetWork {
                        operation_id,
                        target_id: row.i64("target_id")? as u64,
                        state: AsyncOperationTargetState::parse(&row.text("state")?)?,
                        delete_files,
                    })
                })
                .collect()
        })
    }

    pub fn mark_history_delete_target_state(
        &self,
        operation_id: u64,
        target_id: u64,
        state: AsyncOperationTargetState,
        error_message: Option<&str>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let error_message = error_message.map(str::to_string);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE async_operation_targets
                 SET state = {},
                     error_message = {}
                 WHERE operation_id = {}
                   AND target_kind = {}
                   AND target_id = {}",
                &[
                    SqlArg::Text(state.as_str().to_string()),
                    SqlArg::OptText(error_message),
                    SqlArg::I64(operation_id as i64),
                    SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
                    SqlArg::I64(target_id as i64),
                ],
            )
            .await?;
            Ok(())
        })
    }

    pub fn finalize_history_delete_operation(
        &self,
        operation_id: u64,
    ) -> Result<AsyncOperationState, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let failed_count = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT COUNT(*) AS count
                 FROM async_operation_targets
                 WHERE operation_id = {}
                   AND target_kind = {}
                   AND state = 'failed'",
                &[
                    SqlArg::I64(operation_id as i64),
                    SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
                ],
            )
            .await?
            .map(|row| row.i64("count"))
            .transpose()?
            .unwrap_or(0);
            let next_state = if failed_count > 0 {
                AsyncOperationState::CompletedWithErrors
            } else {
                AsyncOperationState::Completed
            };
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE async_operations
                 SET state = {},
                     finished_at = {}
                 WHERE id = {}",
                &[
                    SqlArg::Text(next_state.as_str().to_string()),
                    SqlArg::I64(epoch_ms_now()),
                    SqlArg::I64(operation_id as i64),
                ],
            )
            .await?;
            Ok(next_state)
        })
    }

    pub fn list_history_delete_row_states(
        &self,
        ids: &[u64],
    ) -> Result<HashMap<u64, HistoryDeleteRowState>, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let sql = format!(
                "SELECT t.target_id, t.operation_id, t.state, t.error_message, o.payload_json
                 FROM async_operation_targets t
                 INNER JOIN async_operations o ON o.id = t.operation_id
                 WHERE o.kind = {{}}
                   AND t.target_kind = {{}}
                   AND t.state != 'completed'
                   AND t.target_id IN ({})
                   AND t.operation_id = (
                     SELECT t2.operation_id
                     FROM async_operation_targets t2
                     INNER JOIN async_operations o2 ON o2.id = t2.operation_id
                     WHERE o2.kind = {{}}
                       AND t2.target_kind = {{}}
                       AND t2.target_id = t.target_id
                       AND t2.state != 'completed'
                     ORDER BY t2.operation_id DESC
                     LIMIT 1
                   )",
                placeholders(ids.len())
            );
            let mut args = vec![
                SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
            ];
            args.extend(id_args(&ids));
            args.push(SqlArg::Text(HISTORY_DELETE_KIND.to_string()));
            args.push(SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()));
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            let mut out = HashMap::new();
            for row in rows {
                let target_id = row.i64("target_id")? as u64;
                out.insert(target_id, history_delete_row_state_from_row(row)?);
            }
            Ok(out)
        })
    }

    pub fn list_history_delete_operations(
        &self,
        active_only: bool,
    ) -> Result<Vec<HistoryDeleteOperationSummary>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
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
                  AND t.target_kind = {}
                 WHERE o.kind = {}",
            );
            if active_only {
                sql.push_str(" AND o.state IN ('queued', 'running')");
            }
            sql.push_str(
                " GROUP BY o.id, o.state, o.payload_json, o.requested_at
                  ORDER BY o.requested_at ASC, o.id ASC",
            );
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &sql,
                &[
                    SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
                    SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                ],
            )
            .await?;
            rows.into_iter()
                .map(history_delete_operation_summary_from_row)
                .collect()
        })
    }
}

fn history_delete_row_state_from_row(row: SqlRow) -> Result<HistoryDeleteRowState, StateError> {
    let state = AsyncOperationTargetState::parse(&row.text("state")?)?;
    Ok(HistoryDeleteRowState {
        operation_id: row.i64("operation_id")? as u64,
        state,
        locked: state.locked(),
        delete_files: parse_payload(&row.text("payload_json")?)?.delete_files,
        error_message: row.opt_text("error_message")?,
    })
}

fn history_delete_operation_summary_from_row(
    row: SqlRow,
) -> Result<HistoryDeleteOperationSummary, StateError> {
    Ok(HistoryDeleteOperationSummary {
        id: row.i64("id")? as u64,
        state: AsyncOperationState::parse(&row.text("state")?)?,
        delete_files: parse_payload(&row.text("payload_json")?)?.delete_files,
        requested_at_epoch_ms: row.i64("requested_at")?,
        total_targets: row.i64("total_targets")? as u32,
        queued_targets: row.i64("queued_targets")? as u32,
        running_targets: row.i64("running_targets")? as u32,
        completed_targets: row.i64("completed_targets")? as u32,
        failed_targets: row.i64("failed_targets")? as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobHistoryRow;

    fn history(job_id: u64, completed_at: i64) -> JobHistoryRow {
        JobHistoryRow {
            job_id,
            job_hash: None,
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

    // Exceeds the sqlite bind budget (900) so the `IN (…)` builders and the
    // target bulk-insert are forced across multiple chunks.
    const CHUNKED_ID_COUNT: u64 = 2_500;

    #[test]
    fn insert_all_history_delete_operation_chunks_large_target_set() {
        let db = Database::open_in_memory().unwrap();
        for job_id in 1..=CHUNKED_ID_COUNT {
            db.insert_job_history(&history(job_id, 1_000 + job_id as i64))
                .unwrap();
        }

        let (operation_id, ids) = db.insert_all_history_delete_operation(true).unwrap();
        assert_eq!(ids.len() as u64, CHUNKED_ID_COUNT);

        // Every id became exactly one queued, locked target across all chunks.
        let summaries = db.list_history_delete_operations(true).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, operation_id);
        assert_eq!(u64::from(summaries[0].total_targets), CHUNKED_ID_COUNT);
        assert_eq!(u64::from(summaries[0].queued_targets), CHUNKED_ID_COUNT);

        // sort_order must be dense and unique 0..N across chunk boundaries so the
        // claim order matches the (completed_at DESC, job_id DESC) input order.
        let targets = db
            .list_history_delete_operation_targets(operation_id, true)
            .unwrap();
        assert_eq!(targets.len() as u64, CHUNKED_ID_COUNT);
        // `list_all_history_job_ids_tx` orders newest-first, so target index 0 is
        // the highest job_id.
        for (index, target) in targets.iter().enumerate() {
            let expected = CHUNKED_ID_COUNT - index as u64;
            assert_eq!(target.target_id, expected, "unexpected order at {index}");
        }
    }

    #[test]
    fn list_history_job_ids_chunks_large_input() {
        let db = Database::open_in_memory().unwrap();
        for job_id in 1..=CHUNKED_ID_COUNT {
            db.insert_job_history(&history(job_id, 1_000 + job_id as i64))
                .unwrap();
        }

        // Query for every present id plus a large block of absent ids; both the
        // present and absent ids together exceed a single sqlite bind chunk.
        let mut query_ids: Vec<u64> = (1..=CHUNKED_ID_COUNT).collect();
        query_ids.extend(1_000_000..1_000_000 + CHUNKED_ID_COUNT);
        let present = db.list_history_job_ids(&query_ids).unwrap();
        assert_eq!(present.len() as u64, CHUNKED_ID_COUNT);
        assert!(present.contains(&1));
        assert!(present.contains(&CHUNKED_ID_COUNT));
        assert!(!present.contains(&1_000_000));
    }

    #[test]
    fn locked_target_lookup_chunks_large_input() {
        let db = Database::open_in_memory().unwrap();
        for job_id in 1..=CHUNKED_ID_COUNT {
            db.insert_job_history(&history(job_id, 1_000 + job_id as i64))
                .unwrap();
        }
        db.insert_all_history_delete_operation(false).unwrap();

        let query_ids: Vec<u64> = (1..=CHUNKED_ID_COUNT).collect();
        let locked = db
            .list_history_delete_locked_target_ids(&query_ids)
            .unwrap();
        assert_eq!(locked.len() as u64, CHUNKED_ID_COUNT);
        assert!(locked.contains(&1));
        assert!(locked.contains(&CHUNKED_ID_COUNT));

        // A fresh insert over the same (now locked) rows must be rejected, which
        // exercises the chunked count_existing/count_locked paths in the tx.
        let error = db
            .insert_history_delete_operation(&query_ids, true)
            .unwrap_err();
        assert!(matches!(
            error,
            HistoryDeleteOperationInsertError::LockedTargets
        ));
    }
}
