// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sqlx::{Postgres, QueryBuilder, Sqlite};

use super::database::HistoryDatabase;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRow, SqlRuntime, SqlTx};

const HISTORY_DELETE_KIND: &str = "history_delete";
// Archiving an entry, which removes its files and keeps the entry: the same
// queue, targets and worker as a delete, one operation kind more.
const ARCHIVE_HISTORY_KIND: &str = "history_archive";
const HISTORY_JOB_TARGET_KIND: &str = "history_job";
// The outcome a queued archive ends with when a delete of the same
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
    Archive,
}

impl HistoryOperationAction {
    fn kind(self) -> &'static str {
        match self {
            Self::Delete => HISTORY_DELETE_KIND,
            Self::Archive => ARCHIVE_HISTORY_KIND,
        }
    }

    fn from_kind(kind: &str) -> Result<Self, StateError> {
        match kind {
            HISTORY_DELETE_KIND => Ok(Self::Delete),
            ARCHIVE_HISTORY_KIND => Ok(Self::Archive),
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
    // How an archive ended, once it has; `None` for a delete.
    pub archive_outcome: Option<weaver_server_jobs::jobs::control::ArchiveHistoryOutcome>,
}

// What an archive request answers for one id when it is accepted or
// refused, before any disk work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveHistoryAcceptOutcome {
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
    AlreadyArchived,
}

// The answer to an archive request: the operation that holds the
// accepted ids, when any was, and one outcome per distinct id in the order
// given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveHistoryAcceptance {
    pub operation_id: Option<u64>,
    pub results: Vec<(u64, ArchiveHistoryAcceptOutcome)>,
}

// How the worker settles one target of an archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveHistorySettlement {
    pub target_id: u64,
    pub state: AsyncOperationTargetState,
    pub outcome: weaver_server_jobs::jobs::control::ArchiveHistoryOutcome,
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

// How many of `ids` another delete holds. An archive does not hold an
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

// A delete accepted for entries an archive still waits on wins: the
// queued removal targets of those entries end, superseded, so the removal
// never runs for them. A removal already running finishes first, as the
// worker runs one operation at a time. `ids` `None` supersedes every queued
// removal target.
async fn supersede_queued_archive_history_tx(
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
            SqlArg::Text(ARCHIVE_HISTORY_KIND.to_string()),
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

pub trait OperationsAsyncOpsQueries {
    fn list_history_delete_locked_target_ids(
        &self,
        ids: &[u64],
    ) -> Result<HashSet<u64>, StateError>;
    fn has_any_locked_history_delete_targets(&self) -> Result<bool, StateError>;
    fn list_history_job_ids(&self, ids: &[u64]) -> Result<BTreeSet<u64>, StateError>;
    fn list_all_history_job_ids(&self) -> Result<Vec<u64>, StateError>;
    fn insert_history_delete_operation(
        &self,
        ids: &[u64],
        delete_files: bool,
        file_delete_authorized: bool,
    ) -> Result<u64, HistoryDeleteOperationInsertError>;
    fn accept_archive_history(&self, ids: &[u64]) -> Result<ArchiveHistoryAcceptance, StateError>;
    fn settle_archive_history_targets(
        &self,
        operation_id: u64,
        settled: &[ArchiveHistorySettlement],
    ) -> Result<(), StateError>;
    fn insert_all_history_delete_operation(
        &self,
        delete_files: bool,
        file_delete_authorized: bool,
    ) -> Result<(u64, Vec<u64>), HistoryDeleteOperationInsertError>;
    fn recover_running_history_delete_operations(&self) -> Result<(), StateError>;
    fn requeue_history_delete_operation(&self, operation_id: u64) -> Result<(), StateError>;
    fn next_history_delete_operation(
        &self,
    ) -> Result<Option<HistoryDeleteOperationRow>, StateError>;
    fn list_history_delete_operation_targets(
        &self,
        operation_id: u64,
        delete_files: bool,
    ) -> Result<Vec<HistoryDeleteTargetWork>, StateError>;
    fn mark_history_delete_target_state(
        &self,
        operation_id: u64,
        target_id: u64,
        state: AsyncOperationTargetState,
        error_message: Option<&str>,
    ) -> Result<(), StateError>;
    fn mark_history_delete_targets_state(
        &self,
        operation_id: u64,
        target_ids: &[u64],
        state: AsyncOperationTargetState,
        error_message: Option<&str>,
    ) -> Result<(), StateError>;
    fn fail_pending_history_delete_operation_targets(
        &self,
        operation_id: u64,
        error_message: &str,
    ) -> Result<(), StateError>;
    fn finalize_history_delete_operation(
        &self,
        operation_id: u64,
    ) -> Result<AsyncOperationState, StateError>;
    fn list_history_delete_row_states(
        &self,
        ids: &[u64],
    ) -> Result<HashMap<u64, HistoryDeleteRowState>, StateError>;
    fn list_history_delete_operations(
        &self,
        active_only: bool,
    ) -> Result<Vec<HistoryDeleteOperationSummary>, StateError>;
}

impl<D: HistoryDatabase> OperationsAsyncOpsQueries for D {
    fn list_history_delete_locked_target_ids(
        &self,
        ids: &[u64],
    ) -> Result<HashSet<u64>, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(HashSet::new());
        }

        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
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

    fn has_any_locked_history_delete_targets(&self) -> Result<bool, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
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

    fn list_history_job_ids(&self, ids: &[u64]) -> Result<BTreeSet<u64>, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(BTreeSet::new());
        }

        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
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

    fn list_all_history_job_ids(&self) -> Result<Vec<u64>, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
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

    fn insert_history_delete_operation(
        &self,
        ids: &[u64],
        delete_files: bool,
        file_delete_authorized: bool,
    ) -> Result<u64, HistoryDeleteOperationInsertError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Err(HistoryDeleteOperationInsertError::EmptyTargets);
        }

        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
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
                    supersede_queued_archive_history_tx(tx, Some(&ids)).await?;

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

    // Accepts the removal of the files of `ids`, keeping the entries: one
    // transaction reads, per id, whether it is an entry, a job still in the
    // queue, held by another history operation, or already without its
    // files, then queues one operation for the accepted ids. Nothing is
    // written when none is accepted. The worker is rung; no disk is touched.
    fn accept_archive_history(&self, ids: &[u64]) -> Result<ArchiveHistoryAcceptance, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(ArchiveHistoryAcceptance {
                operation_id: None,
                results: Vec::new(),
            });
        }
        let datastore = self.borrow().datastore();
        let acceptance = self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "accept_archive_history", |tx| {
                let ids = ids.clone();
                Box::pin(async move {
                    let chunk_size = id_chunk_size(bind_budget_for_tx(tx), 1);
                    let mut entries = HashMap::new();
                    let mut active = HashSet::new();
                    for chunk in ids.chunks(chunk_size) {
                        let sql = format!(
                            "SELECT h.job_id, h.archived_at,
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
                                (row.opt_i64("archived_at")?.is_some(), row.i64("held")? != 0),
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
                                (None, true) => ArchiveHistoryAcceptOutcome::NotFinished,
                                (None, false) => ArchiveHistoryAcceptOutcome::NotFound,
                                (Some((true, _)), _) => {
                                    ArchiveHistoryAcceptOutcome::AlreadyArchived
                                }
                                (Some((false, held)), in_queue) if *held || in_queue => {
                                    ArchiveHistoryAcceptOutcome::Busy
                                }
                                (Some(_), _) => ArchiveHistoryAcceptOutcome::Accepted,
                            };
                            (*id, outcome)
                        })
                        .collect::<Vec<_>>();
                    let accepted = results
                        .iter()
                        .filter(|(_, outcome)| *outcome == ArchiveHistoryAcceptOutcome::Accepted)
                        .map(|(id, _)| *id)
                        .collect::<Vec<_>>();
                    let operation_id = if accepted.is_empty() {
                        None
                    } else {
                        Some(
                            insert_history_operation_tx(
                                tx,
                                HistoryOperationAction::Archive,
                                &accepted,
                                true,
                                false,
                            )
                            .await?,
                        )
                    };
                    Ok(ArchiveHistoryAcceptance {
                        operation_id,
                        results,
                    })
                })
            })
            .await
        })?;
        if acceptance.operation_id.is_some() {
            self.wake_history_delete();
        }
        Ok(acceptance)
    }

    // Settles the targets of one archive operation, each with its
    // state, outcome and message, in one transaction of one statement per
    // bind-sized chunk.
    fn settle_archive_history_targets(
        &self,
        operation_id: u64,
        settled: &[ArchiveHistorySettlement],
    ) -> Result<(), StateError> {
        if settled.is_empty() {
            return Ok(());
        }
        let settled = settled.to_vec();
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "settle_archive_history_targets",
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

    fn insert_all_history_delete_operation(
        &self,
        delete_files: bool,
        file_delete_authorized: bool,
    ) -> Result<(u64, Vec<u64>), HistoryDeleteOperationInsertError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
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
                        supersede_queued_archive_history_tx(tx, None).await?;

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

    fn recover_running_history_delete_operations(&self) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
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
                                SqlArg::Text(ARCHIVE_HISTORY_KIND.to_string()),
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
                                SqlArg::Text(ARCHIVE_HISTORY_KIND.to_string()),
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

    fn requeue_history_delete_operation(&self, operation_id: u64) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
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
                            SqlArg::Text(ARCHIVE_HISTORY_KIND.to_string()),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    fn next_history_delete_operation(
        &self,
    ) -> Result<Option<HistoryDeleteOperationRow>, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
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
                                SqlArg::Text(ARCHIVE_HISTORY_KIND.to_string()),
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

    fn list_history_delete_operation_targets(
        &self,
        operation_id: u64,
        delete_files: bool,
    ) -> Result<Vec<HistoryDeleteTargetWork>, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
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

    fn mark_history_delete_target_state(
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
    fn mark_history_delete_targets_state(
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
        let datastore = self.borrow().datastore();
        let error_message = error_message.map(str::to_string);
        self.borrow().run_sql_blocking(async move {
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

    fn fail_pending_history_delete_operation_targets(
        &self,
        operation_id: u64,
        error_message: &str,
    ) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        let error_message = error_message.to_string();
        self.borrow().run_sql_blocking(async move {
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

    fn finalize_history_delete_operation(
        &self,
        operation_id: u64,
    ) -> Result<AsyncOperationState, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
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

    fn list_history_delete_row_states(
        &self,
        ids: &[u64],
    ) -> Result<HashMap<u64, HistoryDeleteRowState>, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
            // A delete's state is its latest target that has not completed;
            // an archive's is its latest target, shown only while it has
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
                SqlArg::Text(ARCHIVE_HISTORY_KIND.to_string()),
                SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()),
            ];
            args.extend(id_args(&ids));
            args.push(SqlArg::Text(HISTORY_JOB_TARGET_KIND.to_string()));
            args.push(SqlArg::Text(HISTORY_DELETE_KIND.to_string()));
            args.push(SqlArg::Text(ARCHIVE_HISTORY_KIND.to_string()));
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            let mut out = HashMap::new();
            for row in rows {
                let target_id = row.i64("target_id")? as u64;
                out.insert(target_id, history_delete_row_state_from_row(row)?);
            }
            Ok(out)
        })
    }

    fn list_history_delete_operations(
        &self,
        active_only: bool,
    ) -> Result<Vec<HistoryDeleteOperationSummary>, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
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

pub trait OperationsAsyncOpsInternalQueries {
    #[cfg(any(test, feature = "test-support"))]
    fn insert_available_history_delete_operation(
        &self,
        ids: &[u64],
        delete_files: bool,
    ) -> Result<Option<u64>, StateError>;
    fn queue_retention_history_deletes(
        &self,
        ids: &[u64],
        delete_files: bool,
    ) -> Result<bool, StateError>;
}

impl<D: HistoryDatabase> OperationsAsyncOpsInternalQueries for D {
    #[cfg(any(test, feature = "test-support"))]
    fn insert_available_history_delete_operation(
        &self,
        ids: &[u64],
        delete_files: bool,
    ) -> Result<Option<u64>, StateError> {
        let ids = dedupe_ids(ids);
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
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
    fn queue_retention_history_deletes(
        &self,
        ids: &[u64],
        delete_files: bool,
    ) -> Result<bool, StateError> {
        let ids = dedupe_ids(ids);
        if ids.is_empty() {
            return Ok(false);
        }
        let datastore = self.borrow().datastore();
        let probe = ids.clone();
        let (rearm, queue) = self.borrow().run_sql_blocking_read(async move {
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
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking(async move {
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
        archive_outcome: row
            .opt_text("outcome")?
            .as_deref()
            .and_then(weaver_server_jobs::jobs::control::ArchiveHistoryOutcome::parse),
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
