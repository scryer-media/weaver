// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sqlx::{Postgres, QueryBuilder, Sqlite};

use crate::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRow, SqlRuntime, SqlTx};
use crate::{Database, StateError};

const HISTORY_DELETE_KIND: &str = "history_delete";
// Removing an entry's files and keeping the entry: the same queue, targets
// and worker as a delete, one operation kind more.
const HISTORY_FILES_REMOVAL_KIND: &str = "history_files_removal";
const HISTORY_JOB_TARGET_KIND: &str = "history_job";
// The outcome a queued files removal ends with when a delete of the same
// entry is accepted first.
const SUPERSEDED_OUTCOME: &str = "superseded";

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

// What a history operation does with its entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryOperationAction {
    // Deletes the entries, with or without their files.
    Delete,
    // Removes the entries' files and keeps the entries.
    RemoveFiles,
}

impl HistoryOperationAction {
    fn kind(self) -> &'static str {
        match self {
            Self::Delete => HISTORY_DELETE_KIND,
            Self::RemoveFiles => HISTORY_FILES_REMOVAL_KIND,
        }
    }

    fn from_kind(kind: &str) -> Result<Self, StateError> {
        match kind {
            HISTORY_DELETE_KIND => Ok(Self::Delete),
            HISTORY_FILES_REMOVAL_KIND => Ok(Self::RemoveFiles),
            other => Err(StateError::Database(format!(
                "invalid history operation kind `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryDeleteOperationRow {
    pub id: u64,
    pub state: AsyncOperationState,
    pub action: HistoryOperationAction,
    pub delete_files: bool,
    pub file_delete_authorized: bool,
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
    pub action: HistoryOperationAction,
    pub delete_files: bool,
    pub error_message: Option<String>,
    // How a files removal ended, once it has; `None` for a delete.
    pub files_removal_outcome: Option<crate::HistoryFilesRemovalOutcome>,
}

// What a files removal request answers for one id when it is accepted or
// refused, before any disk work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryFilesRemovalAcceptOutcome {
    // Queued: the worker removes the files later.
    Accepted,
    // There is no history entry and no job with this id.
    NotFound,
    // Another history operation holds the entry, or the job is in the queue
    // again.
    Busy,
    // The job is in the queue and has not finished.
    NotFinished,
    // The entry already says its files were removed.
    AlreadyRemoved,
}

// The answer to a files removal request: the operation that holds the
// accepted ids, when any was, and one outcome per distinct id in the order
// given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryFilesRemovalAcceptance {
    pub operation_id: Option<u64>,
    pub results: Vec<(u64, HistoryFilesRemovalAcceptOutcome)>,
}

// How the worker settles one target of a files removal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryFilesRemovalSettlement {
    pub target_id: u64,
    pub state: AsyncOperationTargetState,
    pub outcome: crate::HistoryFilesRemovalOutcome,
    pub message: Option<String>,
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
    #[serde(default)]
    pub file_delete_authorized: bool,
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

// Maximum number of id binds per `IN (…)` chunk, leaving room for `fixed_binds`
// non-id parameters that every chunk carries (e.g. a `target_kind`).
fn id_chunk_size(bind_budget: usize, fixed_binds: usize) -> usize {
    bind_budget.saturating_sub(fixed_binds).max(1)
}

// The history ids of `count` id binds, then a target kind bind, that no
// history operation holds. Served by the primary key and the target state
// index.
fn unclaimed_history_ids_sql(count: usize) -> String {
    format!(
        "SELECT job_id FROM job_history
          WHERE job_id IN ({}) AND NOT EXISTS (
              SELECT 1 FROM async_operation_targets
               WHERE target_kind = {{}} AND target_id = job_history.job_id
                 AND state IN ('queued', 'running'))",
        placeholders(count),
    )
}

// One delete operation for every id of `ids` that is still a history entry
// and no history operation holds; `None` when there is none.
async fn insert_unclaimed_history_delete_operation_tx(
    tx: &mut SqlTx<'_>,
    ids: &[u64],
    delete_files: bool,
) -> Result<Option<u64>, StateError> {
    let mut available = Vec::new();
    for chunk in ids.chunks(id_chunk_size(bind_budget_for_tx(tx), 1)) {
        let mut args = id_args(chunk);
        args.push(SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_owned()));
        let present: HashSet<_> = tx
            .fetch_all(&unclaimed_history_ids_sql(chunk.len()), &args)
            .await?
            .into_iter()
            .map(|row| row.i64("job_id").map(|id| id as u64))
            .collect::<Result<_, _>>()?;
        available.extend(chunk.iter().copied().filter(|id| present.contains(id)));
    }
    if available.is_empty() {
        return Ok(None);
    }
    insert_history_delete_operation_tx(tx, &available, delete_files, delete_files)
        .await
        .map(Some)
}

// What retention does with one due entry, by the latest operation that
// targeted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetentionStep {
    // An operation holds it: this pass leaves it.
    Held,
    // Its latest target is a finished delete of the same file policy that
    // refused it: that target is queued again.
    Rearm,
    // Nothing holds it: it joins a new delete operation.
    Queue,
}

// Per due entry, whether any operation holds it, and the state, kind,
// payload and operation state of its latest target. One bind for each
// target kind, then the ids. Served by the primary key and the target state
// index.
fn retention_probe_sql(count: usize) -> String {
    format!(
        "SELECT h.job_id,
                CAST(CASE WHEN EXISTS (
                    SELECT 1 FROM async_operation_targets l
                     WHERE l.target_kind = {{}} AND l.target_id = h.job_id
                       AND l.state IN ('queued', 'running'))
                     THEN 1 ELSE 0 END AS BIGINT) AS held,
                t.state AS target_state,
                o.kind,
                o.state AS operation_state,
                o.payload_json
           FROM job_history h
           LEFT JOIN async_operation_targets t
             ON t.target_kind = {{}} AND t.target_id = h.job_id
            AND t.operation_id = (
                SELECT MAX(t2.operation_id) FROM async_operation_targets t2
                 WHERE t2.target_kind = {{}} AND t2.target_id = h.job_id)
           LEFT JOIN async_operations o ON o.id = t.operation_id
          WHERE h.job_id IN ({})",
        placeholders(count)
    )
}

fn retention_step(row: &SqlRow, delete_files: bool) -> Result<RetentionStep, StateError> {
    if row.i64("held")? != 0 {
        return Ok(RetentionStep::Held);
    }
    let refused = row.opt_text("target_state")?.as_deref() == Some("failed")
        && row.opt_text("kind")?.as_deref() == Some(HISTORY_DELETE_KIND)
        && matches!(
            row.opt_text("operation_state")?.as_deref(),
            Some("completed" | "completed_with_errors")
        );
    if !refused {
        return Ok(RetentionStep::Queue);
    }
    let same_policy = row
        .opt_text("payload_json")?
        .map(|raw| parse_payload(&raw))
        .transpose()?
        .is_some_and(|payload| payload.delete_files == delete_files);
    Ok(if same_policy {
        RetentionStep::Rearm
    } else {
        RetentionStep::Queue
    })
}

// Queues the refused latest delete target of each of `ids` again, with the
// operation that holds it, when nothing holds the entry since and that
// operation has finished. Returns how many targets it queued.
async fn rearm_refused_history_deletes_tx(
    tx: &mut SqlTx<'_>,
    ids: &[u64],
) -> Result<usize, StateError> {
    let mut operations = BTreeSet::new();
    let mut rearmed = 0;
    for chunk in ids.chunks(id_chunk_size(bind_budget_for_tx(tx), 4)) {
        let sql = format!(
            "UPDATE async_operation_targets
                SET state = 'queued', error_message = NULL, outcome = NULL
              WHERE target_kind = {{}}
                AND state = 'failed'
                AND target_id IN ({})
                AND operation_id = (
                    SELECT MAX(t2.operation_id) FROM async_operation_targets t2
                     WHERE t2.target_kind = async_operation_targets.target_kind
                       AND t2.target_id = async_operation_targets.target_id)
                AND NOT EXISTS (
                    SELECT 1 FROM async_operation_targets l
                     WHERE l.target_kind = async_operation_targets.target_kind
                       AND l.target_id = async_operation_targets.target_id
                       AND l.state IN ('queued', 'running'))
                AND operation_id IN (
                    SELECT id FROM async_operations
                     WHERE id = async_operation_targets.operation_id
                       AND kind = {{}}
                       AND state IN ({{}}, {{}}))
              RETURNING operation_id",
            placeholders(chunk.len())
        );
        let mut args = vec![SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string())];
        args.extend(id_args(chunk));
        args.push(SqlArg::Text(HISTORY_DELETE_KIND.to_string()));
        args.push(SqlArg::Text(
            AsyncOperationState::Completed.as_str().to_string(),
        ));
        args.push(SqlArg::Text(
            AsyncOperationState::CompletedWithErrors
                .as_str()
                .to_string(),
        ));
        for row in tx.fetch_all(&sql, &args).await? {
            rearmed += 1;
            operations.insert(row.i64("operation_id")? as u64);
        }
    }
    let operations = operations.into_iter().collect::<Vec<_>>();
    for chunk in operations.chunks(id_chunk_size(bind_budget_for_tx(tx), 0)) {
        let sql = format!(
            "UPDATE async_operations
                SET state = 'queued', finished_at = NULL
              WHERE id IN ({})",
            placeholders(chunk.len())
        );
        tx.execute(&sql, &id_args(chunk)).await?;
    }
    Ok(rearmed)
}

fn payload_json(delete_files: bool, file_delete_authorized: bool) -> Result<String, StateError> {
    serde_json::to_string(&HistoryDeleteOperationPayload {
        delete_files,
        file_delete_authorized,
    })
    .map_err(db_err)
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

// How many of `ids` another delete holds. A files removal does not hold an
// entry against a delete: the delete supersedes it.
async fn count_locked_history_delete_targets(
    tx: &mut SqlTx<'_>,
    ids: &[u64],
) -> Result<usize, StateError> {
    // Two fixed binds (`target_kind`, `kind`) per chunk. `ids` is deduped and
    // chunks are disjoint, so `COUNT(DISTINCT target_id)` per chunk sums to the
    // exact total (no target_id can span two chunks).
    let chunk_size = id_chunk_size(bind_budget_for_tx(tx), 2);
    let mut total = 0i64;
    for chunk in ids.chunks(chunk_size) {
        let sql = format!(
            "SELECT COUNT(DISTINCT t.target_id) AS count
             FROM async_operation_targets t
             INNER JOIN async_operations o ON o.id = t.operation_id
             WHERE t.target_kind = {{}}
               AND t.state IN ('queued', 'running')
               AND t.target_id IN ({})
               AND o.kind = {{}}",
            placeholders(chunk.len())
        );
        let mut args = vec![SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string())];
        args.extend(id_args(chunk));
        args.push(SqlArg::Text(HISTORY_DELETE_KIND.to_string()));
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

// Whether another delete holds any entry. Served by the operation kind and
// state index, then each active operation's targets.
async fn has_locked_history_delete_targets_tx(tx: &mut SqlTx<'_>) -> Result<bool, StateError> {
    Ok(tx
        .fetch_optional(
            "SELECT 1
             FROM async_operations o
             INNER JOIN async_operation_targets t ON t.operation_id = o.id
             WHERE o.kind = {}
               AND o.state IN ('queued', 'running')
               AND t.target_kind = {}
               AND t.state IN ('queued', 'running')
             LIMIT 1",
            &[
                SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
            ],
        )
        .await?
        .is_some())
}

// A delete accepted for entries a files removal still waits on wins: the
// queued removal targets of those entries end, superseded, so the removal
// never runs for them. A removal already running finishes first, as the
// worker runs one operation at a time. `ids` `None` supersedes every queued
// removal target.
async fn supersede_queued_history_files_removals_tx(
    tx: &mut SqlTx<'_>,
    ids: Option<&[u64]>,
) -> Result<(), StateError> {
    let update = "UPDATE async_operation_targets
                     SET state = 'completed',
                         outcome = {},
                         error_message = NULL
                   WHERE target_kind = {}
                     AND state = 'queued'
                     AND operation_id IN (
                         SELECT id FROM async_operations
                          WHERE kind = {} AND state = 'queued')";
    let fixed = || {
        vec![
            SqlArg::Text(SUPERSEDED_OUTCOME.to_string()),
            SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
            SqlArg::Text(HISTORY_FILES_REMOVAL_KIND.to_string()),
        ]
    };
    match ids {
        None => {
            tx.execute(update, &fixed()).await?;
        }
        Some(ids) => {
            for chunk in ids.chunks(id_chunk_size(bind_budget_for_tx(tx), 3)) {
                let sql = format!("{update} AND target_id IN ({})", placeholders(chunk.len()));
                let mut args = fixed();
                args.extend(id_args(chunk));
                tx.execute(&sql, &args).await?;
            }
        }
    }
    Ok(())
}

async fn insert_history_delete_operation_tx(
    tx: &mut SqlTx<'_>,
    ids: &[u64],
    delete_files: bool,
    file_delete_authorized: bool,
) -> Result<u64, StateError> {
    insert_history_operation_tx(
        tx,
        HistoryOperationAction::Delete,
        ids,
        delete_files,
        file_delete_authorized,
    )
    .await
}

async fn insert_history_operation_tx(
    tx: &mut SqlTx<'_>,
    action: HistoryOperationAction,
    ids: &[u64],
    delete_files: bool,
    file_delete_authorized: bool,
) -> Result<u64, StateError> {
    let payload_json = payload_json(delete_files, file_delete_authorized)?;
    let now = epoch_ms_now();

    let row = tx
        .fetch_optional(
            "INSERT INTO async_operations
             (kind, state, payload_json, requested_at, started_at, finished_at)
             VALUES ({}, {}, {}, {}, NULL, NULL)
             RETURNING id",
            &[
                SqlArg::Text(action.kind().to_string()),
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

// Insert one `async_operation_targets` row per id using multi-row `VALUES`
// chunks (instead of one `INSERT` statement per id).
//
// `sort_order` is the id's index in `ids`, preserving the exact ordering
// semantics of the previous per-statement loop. `error_message` is always
// `NULL` for a freshly queued target. Rows bind 6 parameters each
// (operation_id, target_kind, target_id, state, error_message, sort_order), so
// chunks stay under the dialect bind budget.
#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: a batch on the transaction run_in_transaction opened under the gate; Postgres has no writer gate"
)]
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
        self.run_sql_blocking_read(async move {
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
        self.run_sql_blocking_read(async move {
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
        self.run_sql_blocking_read(async move {
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
        self.run_sql_blocking_read(async move {
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
        file_delete_authorized: bool,
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
                    supersede_queued_history_files_removals_tx(tx, Some(&ids)).await?;

                    let operation_id = insert_history_delete_operation_tx(
                        tx,
                        &ids,
                        delete_files,
                        file_delete_authorized,
                    )
                    .await?;
                    Ok(Ok(operation_id))
                })
            })
            .await
        })?
    }

    #[cfg(test)]
    pub(crate) fn insert_available_history_delete_operation(
        &self,
        ids: &[u64],
        delete_files: bool,
    ) -> Result<Option<u64>, StateError> {
        let ids = dedupe_ids(ids);
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "insert_available_history_delete_operation",
                |tx| {
                    let ids = ids.clone();
                    Box::pin(async move {
                        insert_unclaimed_history_delete_operation_tx(tx, &ids, delete_files).await
                    })
                },
            )
            .await
        })
    }

    // Retention's deletes for the due entries `ids`, with or without their
    // files. An entry another operation holds is left for a later pass. An
    // entry whose latest delete of the same file policy refused it is
    // retried: that target and its operation are queued again, so no new
    // operation is added for it pass after pass. Every other entry joins
    // one new operation. The entries are read first outside any
    // transaction: a pass with nothing to queue or retry writes nothing.
    // Returns whether anything was queued; the caller rings the worker.
    pub(crate) fn queue_retention_history_deletes(
        &self,
        ids: &[u64],
        delete_files: bool,
    ) -> Result<bool, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(false);
        }
        let datastore = self.datastore();
        let probe = ids.clone();
        let (rearm, queue) = self.run_sql_blocking_read(async move {
            let chunk_size = id_chunk_size(bind_budget_for_engine(datastore.engine()), 3);
            let mut rearm = Vec::new();
            let mut queue = Vec::new();
            for chunk in probe.chunks(chunk_size) {
                let mut args = vec![
                    SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_owned()),
                    SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_owned()),
                    SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_owned()),
                ];
                args.extend(id_args(chunk));
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &retention_probe_sql(chunk.len()),
                    &args,
                )
                .await?;
                let mut steps = HashMap::with_capacity(rows.len());
                for row in rows {
                    steps.insert(
                        row.i64("job_id")? as u64,
                        retention_step(&row, delete_files)?,
                    );
                }
                for id in chunk {
                    match steps.get(id) {
                        Some(RetentionStep::Rearm) => rearm.push(*id),
                        Some(RetentionStep::Queue) => queue.push(*id),
                        Some(RetentionStep::Held) | None => {}
                    }
                }
            }
            Ok::<_, StateError>((rearm, queue))
        })?;
        if rearm.is_empty() && queue.is_empty() {
            return Ok(false);
        }
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "queue_retention_history_deletes", |tx| {
                let rearm = rearm.clone();
                let queue = queue.clone();
                Box::pin(async move {
                    let rearmed = rearm_refused_history_deletes_tx(tx, &rearm).await?;
                    let inserted =
                        insert_unclaimed_history_delete_operation_tx(tx, &queue, delete_files)
                            .await?;
                    Ok(rearmed > 0 || inserted.is_some())
                })
            })
            .await
        })
    }

    // Accepts the removal of the files of `ids`, keeping the entries: one
    // transaction reads, per id, whether it is an entry, a job still in the
    // queue, held by another history operation, or already without its
    // files, then queues one operation for the accepted ids. Nothing is
    // written when none is accepted. The worker is rung; no disk is touched.
    pub fn accept_history_files_removal(
        &self,
        ids: &[u64],
    ) -> Result<HistoryFilesRemovalAcceptance, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(HistoryFilesRemovalAcceptance {
                operation_id: None,
                results: Vec::new(),
            });
        }
        let datastore = self.datastore();
        let acceptance = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "accept_history_files_removal", |tx| {
                let ids = ids.clone();
                Box::pin(async move {
                    let chunk_size = id_chunk_size(bind_budget_for_tx(tx), 1);
                    let mut entries = HashMap::new();
                    let mut active = HashSet::new();
                    for chunk in ids.chunks(chunk_size) {
                        let sql = format!(
                            "SELECT h.job_id, h.files_removed_at,
                                    CAST(CASE WHEN EXISTS (
                                        SELECT 1 FROM async_operation_targets t
                                         WHERE t.target_kind = {{}}
                                           AND t.target_id = h.job_id
                                           AND t.state IN ('queued', 'running'))
                                         THEN 1 ELSE 0 END AS BIGINT) AS held
                               FROM job_history h
                              WHERE h.job_id IN ({})",
                            placeholders(chunk.len())
                        );
                        let mut args = vec![SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string())];
                        args.extend(id_args(chunk));
                        for row in tx.fetch_all(&sql, &args).await? {
                            entries.insert(
                                row.i64("job_id")? as u64,
                                (
                                    row.opt_i64("files_removed_at")?.is_some(),
                                    row.i64("held")? != 0,
                                ),
                            );
                        }
                        let sql = format!(
                            "SELECT job_id FROM active_jobs WHERE job_id IN ({})",
                            placeholders(chunk.len())
                        );
                        for row in tx.fetch_all(&sql, &id_args(chunk)).await? {
                            active.insert(row.i64("job_id")? as u64);
                        }
                    }
                    let results = ids
                        .iter()
                        .map(|id| {
                            let outcome = match (entries.get(id), active.contains(id)) {
                                (None, true) => HistoryFilesRemovalAcceptOutcome::NotFinished,
                                (None, false) => HistoryFilesRemovalAcceptOutcome::NotFound,
                                (Some((true, _)), _) => {
                                    HistoryFilesRemovalAcceptOutcome::AlreadyRemoved
                                }
                                (Some((false, held)), in_queue) if *held || in_queue => {
                                    HistoryFilesRemovalAcceptOutcome::Busy
                                }
                                (Some(_), _) => HistoryFilesRemovalAcceptOutcome::Accepted,
                            };
                            (*id, outcome)
                        })
                        .collect::<Vec<_>>();
                    let accepted = results
                        .iter()
                        .filter(|(_, outcome)| {
                            *outcome == HistoryFilesRemovalAcceptOutcome::Accepted
                        })
                        .map(|(id, _)| *id)
                        .collect::<Vec<_>>();
                    let operation_id = if accepted.is_empty() {
                        None
                    } else {
                        Some(
                            insert_history_operation_tx(
                                tx,
                                HistoryOperationAction::RemoveFiles,
                                &accepted,
                                true,
                                false,
                            )
                            .await?,
                        )
                    };
                    Ok(HistoryFilesRemovalAcceptance {
                        operation_id,
                        results,
                    })
                })
            })
            .await
        })?;
        if acceptance.operation_id.is_some() {
            self.history_delete_wake.notify_one();
        }
        Ok(acceptance)
    }

    // Settles the targets of one files removal operation, each with its
    // state, outcome and message, in one transaction of one statement per
    // bind-sized chunk.
    pub fn settle_history_files_removal_targets(
        &self,
        operation_id: u64,
        settled: &[HistoryFilesRemovalSettlement],
    ) -> Result<(), StateError> {
        if settled.is_empty() {
            return Ok(());
        }
        let settled = settled.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "settle_history_files_removal_targets",
                |tx| {
                    let settled = settled.clone();
                    Box::pin(async move {
                        const BINDS_PER_ROW: usize = 4;
                        let rows = (bind_budget_for_tx(tx).saturating_sub(2) / BINDS_PER_ROW).max(1);
                        for chunk in settled.chunks(rows) {
                            let values = vec![
                                "(CAST({} AS BIGINT), CAST({} AS TEXT), CAST({} AS TEXT), CAST({} AS TEXT))";
                                chunk.len()
                            ]
                            .join(", ");
                            let mut args = Vec::with_capacity(chunk.len() * BINDS_PER_ROW + 2);
                            for settlement in chunk {
                                args.push(SqlArg::I64(settlement.target_id as i64));
                                args.push(SqlArg::Text(settlement.state.as_str().to_string()));
                                args.push(SqlArg::Text(settlement.outcome.as_str().to_string()));
                                args.push(SqlArg::OptText(settlement.message.clone()));
                            }
                            args.push(SqlArg::I64(operation_id as i64));
                            args.push(SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()));
                            tx.execute(
                                &format!(
                                    "WITH settled (settled_id, settled_state, settled_outcome, settled_message)
                                       AS (VALUES {values})
                                     UPDATE async_operation_targets
                                        SET state = settled.settled_state,
                                            outcome = settled.settled_outcome,
                                            error_message = settled.settled_message
                                       FROM settled
                                      WHERE async_operation_targets.operation_id = {{}}
                                        AND async_operation_targets.target_kind = {{}}
                                        AND async_operation_targets.target_id = settled.settled_id"
                                ),
                                &args,
                            )
                            .await?;
                        }
                        Ok(())
                    })
                },
            )
            .await
        })
    }

    pub fn insert_all_history_delete_operation(
        &self,
        delete_files: bool,
        file_delete_authorized: bool,
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
                        supersede_queued_history_files_removals_tx(tx, None).await?;

                        let operation_id = insert_history_delete_operation_tx(
                            tx,
                            &ids,
                            delete_files,
                            file_delete_authorized,
                        )
                        .await?;
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
                             WHERE kind IN ({}, {})
                               AND state = 'running'
                           )",
                            &[
                                SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
                                SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                                SqlArg::Text(HISTORY_FILES_REMOVAL_KIND.to_string()),
                            ],
                        )
                        .await?;
                        tx.execute(
                            "UPDATE async_operations
                         SET state = 'queued',
                             finished_at = NULL
                         WHERE kind IN ({}, {})
                           AND state = 'running'",
                            &[
                                SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                                SqlArg::Text(HISTORY_FILES_REMOVAL_KIND.to_string()),
                            ],
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
                           AND kind IN ({}, {})
                           AND state = 'running'",
                        &[
                            SqlArg::I64(operation_id as i64),
                            SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                            SqlArg::Text(HISTORY_FILES_REMOVAL_KIND.to_string()),
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
                            "SELECT id, kind, payload_json
                             FROM async_operations
                             WHERE kind IN ({}, {})
                               AND state = 'queued'
                             ORDER BY requested_at ASC, id ASC
                             LIMIT 1",
                            &[
                                SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                                SqlArg::Text(HISTORY_FILES_REMOVAL_KIND.to_string()),
                            ],
                        )
                        .await?;
                    let Some(row) = row else {
                        return Ok(None);
                    };
                    let operation_id = row.i64("id")? as u64;
                    let action = HistoryOperationAction::from_kind(&row.text("kind")?)?;
                    let payload = parse_payload(&row.text("payload_json")?)?;

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
                        action,
                        delete_files: payload.delete_files,
                        file_delete_authorized: payload.file_delete_authorized,
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
        self.run_sql_blocking_read(async move {
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
        self.mark_history_delete_targets_state(operation_id, &[target_id], state, error_message)
    }

    // Moves every listed target of one operation to `state` with one
    // `UPDATE ... IN (...)` per bind-sized chunk.
    pub fn mark_history_delete_targets_state(
        &self,
        operation_id: u64,
        target_ids: &[u64],
        state: AsyncOperationTargetState,
        error_message: Option<&str>,
    ) -> Result<(), StateError> {
        let target_ids = dedupe_ids(target_ids);
        if target_ids.is_empty() {
            return Ok(());
        }
        let datastore = self.datastore();
        let error_message = error_message.map(str::to_string);
        self.run_sql_blocking(async move {
            // Four fixed binds: state, error_message, operation_id, target_kind.
            let chunk_size = id_chunk_size(bind_budget_for_engine(datastore.engine()), 4);
            for chunk in target_ids.chunks(chunk_size) {
                let sql = format!(
                    "UPDATE async_operation_targets
                     SET state = {{}},
                         error_message = {{}}
                     WHERE operation_id = {{}}
                       AND target_kind = {{}}
                       AND target_id IN ({})",
                    placeholders(chunk.len())
                );
                let mut args = vec![
                    SqlArg::Text(state.as_str().to_string()),
                    SqlArg::OptText(error_message.clone()),
                    SqlArg::I64(operation_id as i64),
                    SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
                ];
                args.extend(id_args(chunk));
                SqlRuntime::execute(datastore.write_exec(), &sql, &args).await?;
            }
            Ok(())
        })
    }

    pub fn fail_pending_history_delete_operation_targets(
        &self,
        operation_id: u64,
        error_message: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let error_message = error_message.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE async_operation_targets
                 SET state = 'failed',
                     error_message = {}
                 WHERE operation_id = {}
                   AND target_kind = {}
                   AND state IN ('queued', 'running')",
                &[
                    SqlArg::Text(error_message),
                    SqlArg::I64(operation_id as i64),
                    SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
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
                datastore.write_exec(),
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
        self.run_sql_blocking_read(async move {
            // A delete's state is its latest target that has not completed;
            // a files removal's is its latest target, shown only while it has
            // not completed, so a removal that later succeeds clears an older
            // failure. Every target here belongs to one of the two kinds.
            let sql = format!(
                "SELECT t.target_id, t.operation_id, t.state, t.error_message, t.outcome,
                        o.kind, o.payload_json
                 FROM async_operation_targets t
                 INNER JOIN async_operations o ON o.id = t.operation_id
                 WHERE o.kind IN ({{}}, {{}})
                   AND t.target_kind = {{}}
                   AND t.state != 'completed'
                   AND t.target_id IN ({})
                   AND t.operation_id = (
                     SELECT t2.operation_id
                     FROM async_operation_targets t2
                     INNER JOIN async_operations o2 ON o2.id = t2.operation_id
                     WHERE t2.target_kind = {{}}
                       AND t2.target_id = t.target_id
                       AND ((o2.kind = {{}} AND t2.state != 'completed') OR o2.kind = {{}})
                     ORDER BY t2.operation_id DESC
                     LIMIT 1
                   )",
                placeholders(ids.len())
            );
            let mut args = vec![
                SqlArg::Text(HISTORY_DELETE_KIND.to_string()),
                SqlArg::Text(HISTORY_FILES_REMOVAL_KIND.to_string()),
                SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
            ];
            args.extend(id_args(&ids));
            args.push(SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()));
            args.push(SqlArg::Text(HISTORY_DELETE_KIND.to_string()));
            args.push(SqlArg::Text(HISTORY_FILES_REMOVAL_KIND.to_string()));
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
        self.run_sql_blocking_read(async move {
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
        action: HistoryOperationAction::from_kind(&row.text("kind")?)?,
        delete_files: parse_payload(&row.text("payload_json")?)?.delete_files,
        error_message: row.opt_text("error_message")?,
        files_removal_outcome: row
            .opt_text("outcome")?
            .as_deref()
            .and_then(crate::HistoryFilesRemovalOutcome::parse),
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
            server_attribution: None,
            files_removed_at: None,
        }
    }

    #[test]
    fn legacy_history_delete_payload_defaults_file_delete_authorization_to_false() {
        let payload: HistoryDeleteOperationPayload =
            serde_json::from_str(r#"{"delete_files":true}"#).unwrap();

        assert!(payload.delete_files);
        assert!(!payload.file_delete_authorized);
    }

    #[test]
    fn unproven_file_delete_fails_remaining_targets_without_removing_history() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        db.insert_job_history(&history(11, 101)).unwrap();
        let operation_id = db
            .insert_history_delete_operation(&[10, 11], true, false)
            .unwrap();

        let claimed = db.next_history_delete_operation().unwrap().unwrap();
        assert!(claimed.delete_files);
        assert!(!claimed.file_delete_authorized);
        db.mark_history_delete_target_state(
            operation_id,
            10,
            AsyncOperationTargetState::Completed,
            None,
        )
        .unwrap();
        db.fail_pending_history_delete_operation_targets(
            operation_id,
            "file deletion was not authorized; resubmit with an admin-scoped caller",
        )
        .unwrap();

        assert_eq!(
            db.finalize_history_delete_operation(operation_id).unwrap(),
            AsyncOperationState::CompletedWithErrors
        );
        let states = db.list_history_delete_row_states(&[10, 11]).unwrap();
        assert!(!states.contains_key(&10));
        assert_eq!(states[&11].state, AsyncOperationTargetState::Failed);
        assert_eq!(
            states[&11].error_message.as_deref(),
            Some("file deletion was not authorized; resubmit with an admin-scoped caller")
        );
        assert!(db.get_job_history(10).unwrap().is_some());
        assert!(db.get_job_history(11).unwrap().is_some());
    }

    #[test]
    fn authorized_file_delete_marker_survives_restart_recovery() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        db.insert_history_delete_operation(&[10], true, true)
            .unwrap();

        let claimed = db.next_history_delete_operation().unwrap().unwrap();
        assert!(claimed.file_delete_authorized);
        db.recover_running_history_delete_operations().unwrap();

        let recovered = db.next_history_delete_operation().unwrap().unwrap();
        assert!(recovered.delete_files);
        assert!(recovered.file_delete_authorized);
    }

    #[tokio::test]
    async fn scheduled_prune_skips_locked_rows_and_accepts_both_file_policies() {
        let db = Database::open_in_memory().unwrap();
        for (id, status) in [
            (10, "complete"),
            (11, "complete"),
            (12, "failed"),
            (13, "cancelled"),
        ] {
            let mut row = history(id, 100);
            row.status = status.into();
            db.insert_job_history(&row).unwrap();
        }
        let existing = db
            .insert_history_delete_operation(&[10], false, false)
            .unwrap();
        db.prune_history(
            Some(crate::bandwidth::PruneFiles { delete_files: true }),
            Some(crate::bandwidth::PruneFiles {
                delete_files: false,
            }),
            None,
        )
        .await
        .unwrap();
        let states = db
            .list_history_delete_row_states(&[10, 11, 12, 13])
            .unwrap();
        assert_eq!(states[&10].operation_id, existing);
        assert!(!states[&11].delete_files);
        assert!(states[&12].delete_files);
        assert!(!states.contains_key(&13));
    }

    #[test]
    fn scheduled_prune_batches_available_rows_and_skips_missing_targets() {
        let db = Database::open_in_memory().unwrap();
        for id in 10..110 {
            db.insert_job_history(&history(id, 100)).unwrap();
        }
        let locked = db
            .insert_history_delete_operation(&[10], false, false)
            .unwrap();
        let ids: Vec<_> = (9..111).collect();
        let operation = db
            .insert_available_history_delete_operation(&ids, true)
            .unwrap()
            .unwrap();
        let states = db.list_history_delete_row_states(&ids).unwrap();
        assert_eq!(states.len(), 100);
        assert_eq!(states[&10].operation_id, locked);
        for id in 11..110 {
            assert_eq!(states[&id].operation_id, operation);
            assert!(states[&id].delete_files);
        }
        assert_eq!(
            db.insert_available_history_delete_operation(&ids, true)
                .unwrap(),
            None
        );
    }

    #[test]
    fn insert_history_delete_operation_creates_targets_and_locked_states() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        db.insert_job_history(&history(11, 101)).unwrap();

        let operation_id = db
            .insert_history_delete_operation(&[10, 11], true, true)
            .unwrap();
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
            .insert_history_delete_operation(&[10, 11], false, false)
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
        let operation_id = db
            .insert_history_delete_operation(&[10], false, false)
            .unwrap();
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
        db.insert_history_delete_operation(&[10], false, false)
            .unwrap();

        let error = db
            .insert_history_delete_operation(&[10], true, true)
            .unwrap_err();
        assert!(matches!(
            error,
            HistoryDeleteOperationInsertError::LockedTargets
        ));
    }

    #[test]
    fn next_history_delete_operation_claims_only_once_until_recovered() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        let operation_id = db
            .insert_history_delete_operation(&[10], false, false)
            .unwrap();

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
            .insert_history_delete_operation(&[10, 11], false, false)
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

        let (operation_id, ids) = db.insert_all_history_delete_operation(true, true).unwrap();
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
        db.insert_all_history_delete_operation(false, false)
            .unwrap();

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
            .insert_history_delete_operation(&query_ids, true, true)
            .unwrap_err();
        assert!(matches!(
            error,
            HistoryDeleteOperationInsertError::LockedTargets
        ));
    }

    fn active(job_id: u64) -> crate::ActiveJob {
        crate::ActiveJob {
            job_id: crate::JobId(job_id),
            nzb_hash: [0xBC; 32],
            nzb_path: std::path::PathBuf::from(format!("/invented/job-{job_id}.nzb")),
            nzb_zstd: Vec::new(),
            output_dir: std::path::PathBuf::from(format!("/invented/job-{job_id}")),
            created_at: 1_700_000_000,
            category: None,
            metadata: Vec::new(),
            status: "queued",
            download_state: "queued",
            post_state: "idle",
            run_state: "active",
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            password_override: None,
            supplied_password_candidates: Vec::new(),
        }
    }

    fn settle(
        db: &Database,
        operation_id: u64,
        target_id: u64,
        outcome: crate::HistoryFilesRemovalOutcome,
        message: Option<&str>,
    ) {
        db.settle_history_files_removal_targets(
            operation_id,
            &[HistoryFilesRemovalSettlement {
                target_id,
                state: if outcome.settles() {
                    AsyncOperationTargetState::Completed
                } else {
                    AsyncOperationTargetState::Failed
                },
                outcome,
                message: message.map(str::to_string),
            }],
        )
        .unwrap();
    }

    // Each distinct id gets its answer in the order given, and only the
    // accepted ones are queued, as one files removal operation.
    #[test]
    fn a_files_removal_accept_answers_each_id_and_queues_the_accepted() {
        use HistoryFilesRemovalAcceptOutcome as Accept;
        let db = Database::open_in_memory().unwrap();
        for id in [10, 11, 12, 14] {
            db.insert_job_history(&history(id, 100 + id as i64))
                .unwrap();
        }
        crate::pipeline::test_support::stamp_files_removed(
            &db,
            crate::JobId(11),
            Default::default(),
            1_700_000_000,
        );
        db.insert_history_delete_operation(&[12], false, false)
            .unwrap();
        db.create_active_job(&active(13)).unwrap();
        db.create_active_job(&active(14)).unwrap();
        let delete = db.next_history_delete_operation().unwrap().unwrap();
        db.recover_running_history_delete_operations().unwrap();

        let acceptance = db
            .accept_history_files_removal(&[13, 10, 99, 11, 10, 12, 14])
            .unwrap();

        assert_eq!(
            acceptance.results,
            vec![
                (13, Accept::NotFinished),
                (10, Accept::Accepted),
                (99, Accept::NotFound),
                (11, Accept::AlreadyRemoved),
                (12, Accept::Busy),
                (14, Accept::Busy),
            ]
        );
        let operation_id = acceptance.operation_id.unwrap();
        assert_ne!(operation_id, delete.id);
        let targets = db
            .list_history_delete_operation_targets(operation_id, true)
            .unwrap();
        assert_eq!(
            targets.iter().map(|t| t.target_id).collect::<Vec<_>>(),
            [10]
        );
        let state = db.list_history_delete_row_states(&[10]).unwrap()[&10].clone();
        assert_eq!(state.action, HistoryOperationAction::RemoveFiles);
        assert_eq!(state.state, AsyncOperationTargetState::Queued);
        assert!(state.locked);
        assert_eq!(state.files_removal_outcome, None);

        // A request with nothing to accept writes nothing.
        let refused = db.accept_history_files_removal(&[10, 99]).unwrap();
        assert_eq!(refused.operation_id, None);
        assert_eq!(
            refused.results,
            vec![(10, Accept::Busy), (99, Accept::NotFound)]
        );
    }

    // A removal the worker could not finish shows on its entry with its
    // outcome and message, and holds nothing; a later removal that succeeds
    // clears it.
    #[test]
    fn a_failed_files_removal_shows_until_a_later_one_succeeds() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        let first = db.accept_history_files_removal(&[10]).unwrap();
        let first = first.operation_id.unwrap();
        let claimed = db.next_history_delete_operation().unwrap().unwrap();
        assert_eq!(
            (claimed.id, claimed.action),
            (first, HistoryOperationAction::RemoveFiles)
        );
        settle(
            &db,
            first,
            10,
            crate::HistoryFilesRemovalOutcome::NotManaged,
            Some("left /x: not managed by Weaver"),
        );
        assert_eq!(
            db.finalize_history_delete_operation(first).unwrap(),
            AsyncOperationState::CompletedWithErrors
        );

        let state = db.list_history_delete_row_states(&[10]).unwrap()[&10].clone();
        assert_eq!(state.operation_id, first);
        assert_eq!(state.state, AsyncOperationTargetState::Failed);
        assert!(!state.locked);
        assert_eq!(state.action, HistoryOperationAction::RemoveFiles);
        assert_eq!(
            state.files_removal_outcome,
            Some(crate::HistoryFilesRemovalOutcome::NotManaged)
        );
        assert_eq!(
            state.error_message.as_deref(),
            Some("left /x: not managed by Weaver")
        );

        let second = db.accept_history_files_removal(&[10]).unwrap();
        let second = second.operation_id.unwrap();
        db.next_history_delete_operation().unwrap().unwrap();
        settle(
            &db,
            second,
            10,
            crate::HistoryFilesRemovalOutcome::Removed,
            None,
        );
        db.finalize_history_delete_operation(second).unwrap();
        assert!(db.list_history_delete_row_states(&[10]).unwrap().is_empty());
    }

    // The delete operation list shows deletes only: a waiting and a failed
    // files removal are seen on their rows, not in the list.
    #[test]
    fn the_delete_operation_list_leaves_out_files_removals() {
        let db = Database::open_in_memory().unwrap();
        for id in [10, 11, 12] {
            db.insert_job_history(&history(id, 100 + id as i64))
                .unwrap();
        }
        let failed = db
            .accept_history_files_removal(&[10])
            .unwrap()
            .operation_id
            .unwrap();
        db.next_history_delete_operation().unwrap().unwrap();
        settle(
            &db,
            failed,
            10,
            crate::HistoryFilesRemovalOutcome::Failed,
            Some("disk unreachable"),
        );
        db.finalize_history_delete_operation(failed).unwrap();
        let pending = db
            .accept_history_files_removal(&[11])
            .unwrap()
            .operation_id
            .unwrap();
        let delete = db
            .insert_history_delete_operation(&[12], false, false)
            .unwrap();

        for active_only in [true, false] {
            let listed = db
                .list_history_delete_operations(active_only)
                .unwrap()
                .into_iter()
                .map(|summary| summary.id)
                .collect::<Vec<_>>();
            assert_eq!(listed, [delete], "active_only {active_only}");
        }
        let states = db.list_history_delete_row_states(&[10, 11, 12]).unwrap();
        assert_eq!(states[&10].operation_id, failed);
        assert_eq!(states[&10].state, AsyncOperationTargetState::Failed);
        assert_eq!(states[&11].operation_id, pending);
        assert_eq!(states[&11].action, HistoryOperationAction::RemoveFiles);
        assert_eq!(states[&12].action, HistoryOperationAction::Delete);
    }

    // A delete for an entry whose files removal is running is accepted. The
    // removal is not superseded: it finishes, then the delete runs.
    #[test]
    fn a_delete_is_accepted_while_a_files_removal_runs() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        let removal = db
            .accept_history_files_removal(&[10])
            .unwrap()
            .operation_id
            .unwrap();
        assert_eq!(
            db.next_history_delete_operation().unwrap().unwrap().id,
            removal
        );

        let delete = db
            .insert_history_delete_operation(&[10], false, false)
            .unwrap();

        let removal_targets = db
            .list_history_delete_operation_targets(removal, true)
            .unwrap();
        assert_eq!(removal_targets.len(), 1);
        assert_eq!(
            db.list_history_delete_row_states(&[10]).unwrap()[&10].operation_id,
            delete
        );
        settle(
            &db,
            removal,
            10,
            crate::HistoryFilesRemovalOutcome::Removed,
            None,
        );
        db.finalize_history_delete_operation(removal).unwrap();
        assert_eq!(
            db.next_history_delete_operation().unwrap().unwrap().id,
            delete
        );
    }

    // A files removal the worker had claimed when the process stopped is
    // queued again at start-up, with its targets, as a delete is.
    #[test]
    fn a_claimed_files_removal_is_queued_again_after_a_restart() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        let operation_id = db
            .accept_history_files_removal(&[10])
            .unwrap()
            .operation_id
            .unwrap();
        db.next_history_delete_operation().unwrap().unwrap();
        db.mark_history_delete_targets_state(
            operation_id,
            &[10],
            AsyncOperationTargetState::Running,
            None,
        )
        .unwrap();
        assert!(db.next_history_delete_operation().unwrap().is_none());

        db.recover_running_history_delete_operations().unwrap();

        let claimed = db.next_history_delete_operation().unwrap().unwrap();
        assert_eq!(claimed.id, operation_id);
        assert_eq!(claimed.action, HistoryOperationAction::RemoveFiles);
        let targets = db
            .list_history_delete_operation_targets(operation_id, true)
            .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].state, AsyncOperationTargetState::Queued);
    }

    // A delete accepted while a files removal of the same entry still waits
    // wins: the removal's target ends superseded and only the delete holds
    // the entry; the removal's other entries still wait for it.
    #[test]
    fn a_delete_accepted_after_a_waiting_files_removal_supersedes_it() {
        let db = Database::open_in_memory().unwrap();
        for id in [10, 11, 12] {
            db.insert_job_history(&history(id, 100 + id as i64))
                .unwrap();
        }
        let removal = db
            .accept_history_files_removal(&[10, 11])
            .unwrap()
            .operation_id
            .unwrap();

        let delete = db
            .insert_history_delete_operation(&[10], true, true)
            .unwrap();

        let state = db.list_history_delete_row_states(&[10, 11]).unwrap();
        assert_eq!(state[&10].operation_id, delete);
        assert_eq!(state[&10].action, HistoryOperationAction::Delete);
        assert_eq!(state[&11].operation_id, removal);
        let waiting = db
            .list_history_delete_operation_targets(removal, true)
            .unwrap();
        assert_eq!(
            waiting.iter().map(|t| t.target_id).collect::<Vec<_>>(),
            [11]
        );

        // A delete of all history supersedes every waiting removal.
        db.insert_job_history(&history(13, 113)).unwrap();
        let later = db
            .accept_history_files_removal(&[13])
            .unwrap()
            .operation_id
            .unwrap();
        // The earlier delete still holds 10, so a delete of all is refused
        // until it finishes; it then supersedes the rest.
        assert!(matches!(
            db.insert_all_history_delete_operation(false, false),
            Err(HistoryDeleteOperationInsertError::LockedTargets)
        ));
        db.mark_history_delete_target_state(delete, 10, AsyncOperationTargetState::Completed, None)
            .unwrap();
        db.finalize_history_delete_operation(delete).unwrap();
        db.insert_all_history_delete_operation(false, false)
            .unwrap();
        for operation in [removal, later] {
            assert!(
                db.list_history_delete_operation_targets(operation, true)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    // Retention deletes an entry whose files removal failed, in a new
    // operation, as the removal holds nothing once it has ended.
    #[test]
    fn retention_deletes_an_entry_whose_files_removal_failed() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(10, 100)).unwrap();
        let removal = db
            .accept_history_files_removal(&[10])
            .unwrap()
            .operation_id
            .unwrap();
        // Held while the removal waits.
        assert!(!db.queue_retention_history_deletes(&[10], false).unwrap());
        db.next_history_delete_operation().unwrap().unwrap();
        settle(
            &db,
            removal,
            10,
            crate::HistoryFilesRemovalOutcome::Failed,
            Some("disk error"),
        );
        db.finalize_history_delete_operation(removal).unwrap();

        assert!(db.queue_retention_history_deletes(&[10], false).unwrap());
        let state = db.list_history_delete_row_states(&[10]).unwrap()[&10].clone();
        assert_eq!(state.action, HistoryOperationAction::Delete);
        assert_eq!(state.state, AsyncOperationTargetState::Queued);
    }
}
